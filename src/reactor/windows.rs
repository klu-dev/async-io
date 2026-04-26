// SPDX-License-Identifier: MIT OR Apache-2.0

//! Windows-specific [`Registration`] backend.
//!
//! Sockets and waitable handles use the readiness-style portion of
//! [`polling`] (`Poller::add` / `add_waitable`). Overlapped file
//! handles (regular files, named pipes, mailslots, …) use the
//! completion-style portion of [`polling`] introduced on the
//! `kail/windows_file` branch — see the polling design doc
//! `docs/named-pipe.design.md` in that crate for the rationale.
//!
//! The completion-style file API is **not** readiness: every
//! `RegisteredFile::submit_*` call hands a buffer to the kernel and
//! returns a [`polling::os::iocp::OpHandle`] that the reactor wakes
//! exactly once when the IOCP completion arrives. The reactor still
//! observes those completions as `Event { readable | writable, .. }`
//! and routes them to the wakers parked on the source's
//! [`crate::reactor::Source`], so the reactor itself does not need to
//! know about overlapped I/O.

use polling::os::iocp::{PollerIocpExt, RegisteredFile};
use polling::{Event, PollMode, Poller};
use std::fmt;
use std::io::Result;
use std::os::windows::io::{AsRawSocket, BorrowedHandle, BorrowedSocket, RawHandle, RawSocket};
use std::sync::Arc;

/// The raw registration into the reactor.
#[doc(hidden)]
pub enum Registration {
    /// Raw socket handle on Windows.
    ///
    /// # Invariant
    ///
    /// This describes a valid socket that has not been `close`d. It
    /// will not be closed while this object is alive.
    Socket(RawSocket),

    /// Waitable handle for Windows.
    ///
    /// # Invariant
    ///
    /// This describes a valid waitable handle that has not been
    /// `close`d. It will not be closed while this object is alive.
    Handle(RawHandle),

    /// Overlapped file handle bound to the IOCP via the completion
    /// API. The wrapping [`Arc<RegisteredFile>`] keeps the IOCP
    /// binding alive for as long as in-flight `OpHandle`s reference
    /// it, even if this `Registration` is dropped first.
    File(Arc<RegisteredFile>),
}

// `Arc<RegisteredFile>` is `Send + Sync`; the raw socket / handle
// arms are inert pointers whose lifetime is upheld by the caller's
// invariant comment above.
unsafe impl Send for Registration {}
unsafe impl Sync for Registration {}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(raw) => fmt::Debug::fmt(raw, f),
            Self::Handle(handle) => fmt::Debug::fmt(handle, f),
            Self::File(_) => f.debug_tuple("File").finish(),
        }
    }
}

impl Registration {
    /// Create a registration from a raw socket.
    ///
    /// # Safety
    ///
    /// The provided socket must be valid and not be closed while
    /// this object is alive.
    pub(crate) unsafe fn new(f: BorrowedSocket<'_>) -> Self {
        Self::Socket(f.as_raw_socket())
    }

    /// Create a new [`Registration`] around a waitable handle.
    ///
    /// # Safety
    ///
    /// The provided handle must be valid and not be closed while
    /// this object is alive.
    pub(crate) unsafe fn new_waitable(f: BorrowedHandle<'_>) -> Self {
        use std::os::windows::io::AsRawHandle;
        Self::Handle(f.as_raw_handle())
    }

    /// Wrap an already-attached [`RegisteredFile`] as a registration.
    ///
    /// The caller must have produced `file` via
    /// [`PollerIocpFileExt::register_file`] against the reactor's
    /// poller. We do not register here because `register_file`
    /// requires `unsafe` and yields the long-lived
    /// [`RegisteredFile`] handle the caller needs to submit ops.
    pub(crate) fn new_file(file: Arc<RegisteredFile>) -> Self {
        Self::File(file)
    }

    /// Returns the underlying [`RegisteredFile`] for [`Self::File`]
    /// registrations, or `None` for sockets / waitable handles.
    /// This is the only way to submit overlapped reads / writes /
    /// connects.
    #[inline]
    pub(crate) fn registered_file(&self) -> Option<&Arc<RegisteredFile>> {
        match self {
            Self::File(rf) => Some(rf),
            _ => None,
        }
    }

    /// Registers the object into the reactor.
    #[inline]
    pub(crate) fn add(&self, poller: &Poller, token: usize) -> Result<()> {
        // SAFETY: This object's existence validates the invariants
        // for socket / waitable registration. `RegisteredFile` is
        // already attached to the poller; we only rebind the user
        // key so the dispatcher tags every future completion with
        // the slab slot index reserved by the reactor.
        unsafe {
            match self {
                Self::Socket(raw) => poller.add(*raw, Event::none(token)),
                Self::Handle(handle) => {
                    poller.add_waitable(*handle, Event::none(token), PollMode::Oneshot)
                }
                Self::File(rf) => {
                    rf.set_user_key(token);
                    Ok(())
                }
            }
        }
    }

    /// Re-registers the object with the reactor.
    ///
    /// For [`Self::File`] this is a no-op: per-op interest
    /// (readable / writable) is supplied at `submit_*` time inside
    /// the polling crate, not at the source level.
    #[inline]
    pub(crate) fn modify(&self, poller: &Poller, interest: Event) -> Result<()> {
        match self {
            Self::Socket(raw) => {
                poller.modify(unsafe { BorrowedSocket::borrow_raw(*raw) }, interest)
            }
            Self::Handle(handle) => poller.modify_waitable(
                unsafe { BorrowedHandle::borrow_raw(*handle) },
                interest,
                PollMode::Oneshot,
            ),
            Self::File(_) => Ok(()),
        }
    }

    /// Deregisters the object from the reactor.
    ///
    /// For [`Self::File`] this calls
    /// [`RegisteredFile::deactivate`], which prevents new
    /// submissions but leaves any in-flight `OpHandle`s able to
    /// drain through the IOCP completion path. The user is
    /// responsible for awaiting / cancelling those before dropping
    /// the underlying `OwnedHandle`.
    #[inline]
    pub(crate) fn delete(&self, poller: &Poller) -> Result<()> {
        match self {
            Self::Socket(raw) => poller.delete(unsafe { BorrowedSocket::borrow_raw(*raw) }),
            Self::Handle(handle) => {
                poller.remove_waitable(unsafe { BorrowedHandle::borrow_raw(*handle) })
            }
            Self::File(rf) => {
                rf.deactivate();
                Ok(())
            }
        }
    }
}
