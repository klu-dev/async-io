// SPDX-License-Identifier: MIT OR Apache-2.0

//! Functionality that is only available on Windows.
//!
//! ## Architecture
//!
//! Sockets and waitable handles are still managed through `polling`'s
//! readiness-mode API (sockets via AFD, waitables via
//! `RegisterWaitForSingleObject`). Overlapped *file* handles such as
//! named pipes and mailslots are managed through `polling`'s new
//! completion-mode file API (`RegisteredFile` / `OpHandle` /
//! `Submission`). The reactor in this crate observes both kinds of
//! events through the same `polling::Poller`; per-source wakers live in
//! `crate::reactor::Source`. See `docs/named-pipe.design.md`.

use crate::reactor::{Reactor, Readable, Registration, Source};

use std::ffi::OsStr;
use std::fmt;
use std::future::Future;
use std::io::{self, Result};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{
    AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle, RawHandle,
};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Configuration types for Windows named-pipe endpoints
/// ([`PipeMode`], [`PipeAccess`], [`NamedPipeOpenOptions`],
/// [`NamedPipeConnectOptions`]).
pub mod named_pipe;
pub mod waitable;

/// `compio_io::AsyncRead`/`AsyncWrite` adapters for [`NamedPipeStream`].
///
/// Gated behind the `compio` cargo feature so consumers that do not
/// need them avoid pulling in the `compio-io` / `compio-buf` deps.
/// See `docs/named-pipe.design.md` §6.
#[cfg(feature = "compio")]
pub mod compio;
pub use named_pipe::{NamedPipeConnectOptions, NamedPipeOpenOptions, PipeAccess, PipeMode};
pub use waitable::Waitable;

/// Future for [`waitable::Waitable::ready`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Ready<'a, T>(Readable<'a, T>);

impl<T> Future for Ready<'_, T> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

use blocking::unblock;
use futures_io::{AsyncRead, AsyncWrite};
use futures_lite::ready;
use futures_lite::stream::{self, Stream};
use polling::os::iocp::{
    OpHandle, PollerIocpFileExt, RegisteredFile, StableBuf, StableBufMut, Submission,
};
use windows_sys::Win32::Foundation as wf;
use windows_sys::Win32::Security as wsec;
use windows_sys::Win32::Storage::FileSystem as wsf;
use windows_sys::Win32::System::Pipes as wsp;

// =====================================================================
// OpHandleGuard: best-effort cancel-on-drop wrapper
// =====================================================================

/// Wrapper around [`OpHandle`] that issues `CancelIoEx` when the
/// future holding it is dropped before completion.
///
/// The buffer can no longer be recovered after a drop (the kernel
/// retains its `Arc` strong reference until the eventual completion
/// drains it), but in-flight resources are bounded.
struct OpHandleGuard<B>(Option<OpHandle<B>>);

impl<B> OpHandleGuard<B> {
    fn new(op: OpHandle<B>) -> Self {
        Self(Some(op))
    }

    fn as_ref(&self) -> &OpHandle<B> {
        // Safety: only `take` consumes the inner; guard is otherwise live.
        self.0.as_ref().expect("OpHandleGuard taken")
    }

    fn take(mut self) -> OpHandle<B> {
        self.0.take().expect("OpHandleGuard taken")
    }
}

impl<B> Drop for OpHandleGuard<B> {
    fn drop(&mut self) {
        if let Some(op) = self.0.take() {
            // Best-effort. The IOCP completion will still arrive
            // (with `ERROR_OPERATION_ABORTED`) and the dispatcher
            // will reclaim the per-op packet.
            let _ = op.cancel();
        }
    }
}

// =====================================================================
// Internal helpers: wait for a Pending op to complete via the reactor
// =====================================================================

/// Wait for an in-flight `OpHandle` to complete, driving the reactor's
/// per-source waker registry along the way.
///
/// Ordering invariant: the caller MUST have submitted the op already
/// (this helper does not register interest before the submit). The
/// inherent `read`/`write` and `accept` paths satisfy this by calling
/// `submit_*` first; if the kernel completed the op synchronously we
/// would never reach `poll_op` at all.
///
/// With the completion-mode polling crate every IOCP packet wakes a
/// waker exactly once, so the loop drains stale `Ready(Ok)` returns
/// (each represents a previously-consumed event) until either:
///   * `is_complete()` reports true \u2014 the matching completion has
///     been published; or
///   * `poll_readable`/`poll_writable` returns `Pending` \u2014 our waker is
///     parked on the next delivery, which (because the op is already
///     in flight) is guaranteed to be ours.
///
/// `Source::poll_ready` is event-counter-based, so a delivery that
/// races between submit and the first poll bumps the counter and the
/// very next `poll_readable` reports `Ready` even though no waker had
/// been registered yet.
fn poll_op<B>(
    op: &OpHandle<B>,
    source: &Source,
    is_read: bool,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>> {
    loop {
        if op.is_complete() {
            return Poll::Ready(Ok(()));
        }
        let p = if is_read {
            source.poll_readable(cx)
        } else {
            source.poll_writable(cx)
        };
        match p {
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            // A delivery landed; loop and re-check `is_complete`.
            Poll::Ready(Ok(())) => continue,
            Poll::Pending => return Poll::Pending,
        }
    }
}

/// Pre-arm an edge-mode source by taking the per-direction `events`
/// snapshot **before** an op is submitted.
///
/// Required for inherent `read`/`write`/`accept` (and any other path
/// that calls `submit_*` outside of `poll_*`). Without this priming
/// step the snapshot is taken inside the first `poll_op` iteration,
/// which races against the kernel publishing the completion in the
/// window between `submit_*` and that first poll: `react()` bumps
/// `state.events` from N → N+1 with zero parked wakers, then our
/// snapshot captures `Some(N+1)` and we park forever waiting for an
/// `events` value that will never change again.
///
/// After this call the source has `captured_events = Some(N)` (where
/// N is the live counter at this instant), so any delivery that
/// happens after we return — including one racing the `submit_*` we
/// are about to issue — will be observed by the next `poll_op`
/// iteration as `events != N` and resolve `Ready`.
fn arm_source(source: &Source, is_read: bool, cx: &mut Context<'_>) {
    let p = if is_read {
        source.poll_readable(cx)
    } else {
        source.poll_writable(cx)
    };
    // Edge mode: the very first call after the snapshot is cleared
    // always returns `Pending` (it installs the waker and snapshots
    // `events`). A `Ready` here would mean a delivery happened with
    // no caller awaiting it, which is harmless — the next `poll_op`
    // iteration will simply re-arm.
    let _ = p;
}

// =====================================================================
// NamedPipeListener
// =====================================================================

/// A Windows named-pipe server endpoint.
///
/// Each call to [`accept`](Self::accept) creates a fresh pipe instance
/// bound to the same name; the very first `accept` additionally
/// requests `FILE_FLAG_FIRST_PIPE_INSTANCE` so two independent
/// processes hosting the same name fail loudly. Cloning a listener
/// shares the underlying name and the first-instance latch.
#[derive(Debug)]
pub struct NamedPipeListener {
    name: Arc<Vec<u16>>,
    pipe_mode: PipeMode,
    options: NamedPipeOpenOptions,
    /// `false` until the first `accept` runs. The first `accept`
    /// flips this to `true` (atomically) and passes `first = true` to
    /// `CreateNamedPipeW`, which requests
    /// `FILE_FLAG_FIRST_PIPE_INSTANCE` so concurrent listeners on the
    /// same name fail loudly.
    accepted: Arc<AtomicBool>,
}

impl Clone for NamedPipeListener {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            pipe_mode: self.pipe_mode,
            options: self.options.clone(),
            // Share the latch so a clone never re-asserts FIRST_PIPE_INSTANCE.
            accepted: self.accepted.clone(),
        }
    }
}

impl NamedPipeListener {
    /// Creates a listener bound to `addr` with the given pipe mode and
    /// (optional) open options.
    ///
    /// For new code prefer [`builder`](Self::builder), which avoids
    /// passing `None` / `Default::default()` for unused fields.
    pub fn bind<A: AsRef<OsStr>>(
        addr: A,
        pipe_mode: PipeMode,
        options: Option<NamedPipeOpenOptions>,
    ) -> io::Result<Self> {
        let name: Vec<u16> = addr.as_ref().encode_wide().chain(Some(0)).collect();
        Ok(Self {
            name: Arc::new(name),
            pipe_mode,
            options: options.unwrap_or_default(),
            accepted: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Returns a builder for constructing a [`NamedPipeListener`] with
    /// non-default options.
    ///
    /// ```ignore
    /// NamedPipeListener::builder(r"\\.\pipe\my-pipe")
    ///     .pipe_mode(PipeMode::Message)
    ///     .in_buffer_size(4096)
    ///     .bind()?;
    /// ```
    pub fn builder<A: AsRef<OsStr>>(addr: A) -> NamedPipeListenerBuilder {
        NamedPipeListenerBuilder::new(addr)
    }

    /// Waits for and accepts the next incoming client connection,
    /// returning a connected [`NamedPipeStream`].
    pub async fn accept(&self) -> io::Result<NamedPipeStream> {
        // `swap` returns the previous value: `false` on the very first
        // call, `true` thereafter.
        let first = !self.accepted.swap(true, Ordering::AcqRel);

        // Step 1: create a fresh pipe instance.
        //
        // Wrapped in a sync block so the stack-allocated
        // `SECURITY_ATTRIBUTES` (which contains a raw `*mut c_void`
        // and is therefore neither `Send` nor `Sync`) does not live
        // across the `.await` below — the `Incoming` stream requires
        // `Send + Sync`.
        let owned: OwnedHandle = {
            let mut flag = self.options.access.to_flag() | wsf::FILE_FLAG_OVERLAPPED;
            if first {
                flag |= wsf::FILE_FLAG_FIRST_PIPE_INSTANCE;
            }
            // Build a stack-allocated SECURITY_ATTRIBUTES if the caller
            // configured a custom SD or requested handle inheritance.
            let sa_storage: wsec::SECURITY_ATTRIBUTES;
            let sa_ptr: *const wsec::SECURITY_ATTRIBUTES =
                if self.options.security_descriptor.is_some() || self.options.inherit_handle {
                    sa_storage = wsec::SECURITY_ATTRIBUTES {
                        nLength: std::mem::size_of::<wsec::SECURITY_ATTRIBUTES>() as u32,
                        lpSecurityDescriptor: self
                            .options
                            .security_descriptor
                            .as_deref()
                            .map_or(std::ptr::null_mut(), |v| v.as_ptr() as *mut _),
                        bInheritHandle: self.options.inherit_handle as windows_sys::core::BOOL,
                    };
                    &sa_storage
                } else {
                    std::ptr::null()
                };
            // SAFETY: `name` is a NUL-terminated UTF-16 string; `sa_ptr`
            // is either null or points to `sa_storage` on this stack
            // frame, whose `lpSecurityDescriptor` (if non-null) borrows
            // the SD bytes owned by `self.options.security_descriptor`
            // for the duration of this call. All other arguments are
            // plain integers.
            let raw = unsafe {
                wsp::CreateNamedPipeW(
                    self.name.as_ptr(),
                    flag,
                    self.pipe_mode.to_flags(),
                    self.options.max_instances,
                    self.options.out_buffer_size,
                    self.options.in_buffer_size,
                    0,
                    sa_ptr as *mut _,
                )
            };
            if raw == wf::INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `raw` is a freshly created kernel handle we own.
            unsafe { OwnedHandle::from_raw_handle(raw as RawHandle) }
        };

        // Step 2: register the handle with the IOCP and the reactor.
        let stream = NamedPipeStream::from_overlapped_handle(owned)?;

        // Step 3: arm the source, then submit ConnectNamedPipe and
        // await its completion. The arm-before-submit ordering is
        // mandatory under the reactor's edge-mode wake semantics:
        // see [`arm_source`].
        let src = stream.source.clone();
        let file = stream.file.clone();
        let mut submitted: Option<OpHandleGuard<()>> = None;
        let mut sync_done = false;
        futures_lite::future::poll_fn(|cx| -> Poll<io::Result<()>> {
            if submitted.is_none() && !sync_done {
                arm_source(&src, true, cx);
                match file.submit_connect_named_pipe() {
                    Submission::Complete { .. } => {
                        sync_done = true;
                        return Poll::Ready(Ok(()));
                    }
                    Submission::Failed { error, .. } => return Poll::Ready(Err(error)),
                    Submission::Pending(op) => submitted = Some(OpHandleGuard::new(op)),
                }
            }
            if sync_done {
                return Poll::Ready(Ok(()));
            }
            let guard = submitted.as_ref().unwrap();
            match poll_op(guard.as_ref(), &src, true, cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let g = submitted.take().unwrap();
                    let (res, _) = g.take().take();
                    Poll::Ready(res.map(|_| ()))
                }
            }
        })
        .await?;

        Ok(stream)
    }

    /// Returns a [`Stream`] of incoming connections.
    ///
    /// Each item is the result of a single [`accept`](Self::accept)
    /// call. The stream never terminates on its own.
    pub fn incoming(&self) -> Incoming<'_> {
        Incoming {
            incoming: Box::pin(stream::unfold(self, |listener| async move {
                let res = listener.accept().await;
                Some((res, listener))
            })),
        }
    }
}

/// Builder for [`NamedPipeListener`] returned by
/// [`NamedPipeListener::builder`].
#[derive(Debug, Clone)]
pub struct NamedPipeListenerBuilder {
    name: std::ffi::OsString,
    pipe_mode: PipeMode,
    options: NamedPipeOpenOptions,
}

impl NamedPipeListenerBuilder {
    fn new<A: AsRef<OsStr>>(addr: A) -> Self {
        Self {
            name: addr.as_ref().to_owned(),
            pipe_mode: PipeMode::default(),
            options: NamedPipeOpenOptions::new(),
        }
    }

    /// Sets the wire-format and read-side mode (default `PipeMode::Byte`).
    pub fn pipe_mode(mut self, mode: PipeMode) -> Self {
        self.pipe_mode = mode;
        self
    }

    /// Sets the server-side data direction (default `PipeAccess::Duplex`).
    pub fn access(mut self, access: PipeAccess) -> Self {
        self.options = self.options.access(access);
        self
    }

    /// Sets the maximum number of pipe instances
    /// (default `PIPE_UNLIMITED_INSTANCES`).
    pub fn max_instances(mut self, instances: u32) -> Self {
        self.options = self.options.max_instances(instances);
        self
    }

    /// Sets the outbound buffer size hint in bytes (default 64 KiB).
    pub fn out_buffer_size(mut self, size: u32) -> Self {
        self.options = self.options.out_buffer_size(size);
        self
    }

    /// Sets the inbound buffer size hint in bytes (default 64 KiB).
    pub fn in_buffer_size(mut self, size: u32) -> Self {
        self.options = self.options.in_buffer_size(size);
        self
    }

    /// Sets a custom self-relative `SECURITY_DESCRIPTOR` for the pipe.
    /// See [`NamedPipeOpenOptions::security_descriptor`].
    pub fn security_descriptor(mut self, sd: impl Into<Box<[u8]>>) -> Self {
        self.options = self.options.security_descriptor(sd);
        self
    }

    /// Convenience: parse SDDL into a self-relative SD. See
    /// [`NamedPipeOpenOptions::security_descriptor_sddl`].
    pub fn security_descriptor_sddl(mut self, sddl: &str) -> io::Result<Self> {
        self.options = self.options.security_descriptor_sddl(sddl)?;
        Ok(self)
    }

    /// Sets `bInheritHandle` on the underlying `SECURITY_ATTRIBUTES`
    /// (default `false`). See
    /// [`NamedPipeOpenOptions::inherit_handle`].
    pub fn inherit_handle(mut self, on: bool) -> Self {
        self.options = self.options.inherit_handle(on);
        self
    }

    /// Replaces the underlying [`NamedPipeOpenOptions`] wholesale.
    pub fn options(mut self, options: NamedPipeOpenOptions) -> Self {
        self.options = options;
        self
    }

    /// Consumes the builder and creates the listener.
    pub fn bind(self) -> io::Result<NamedPipeListener> {
        NamedPipeListener::bind(&self.name, self.pipe_mode, Some(self.options))
    }
}

/// Stream of incoming named-pipe connections, returned by
/// [`NamedPipeListener::incoming`].
pub struct Incoming<'a> {
    incoming: Pin<Box<dyn Stream<Item = io::Result<NamedPipeStream>> + Send + Sync + 'a>>,
}

impl fmt::Debug for Incoming<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Incoming").finish_non_exhaustive()
    }
}

impl Stream for Incoming<'_> {
    type Item = io::Result<NamedPipeStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = ready!(Pin::new(&mut self.incoming).poll_next(cx));
        Poll::Ready(res)
    }
}

// =====================================================================
// NamedPipeStream
// =====================================================================

/// Per-direction state used by the [`AsyncRead`] / [`AsyncWrite`]
/// impls. The owned-buffer inherent API ([`NamedPipeStream::read`] /
/// [`NamedPipeStream::write`]) does *not* go through this state; it
/// owns its `OpHandle` directly inside the returned future.
enum DirState {
    Idle,
    Pending(OpHandleGuard<Vec<u8>>),
}

struct FuturesIoState {
    read: DirState,
    write: DirState,
    flush: Option<blocking::Task<io::Result<()>>>,
}

/// Inner shared state behind the cloneable [`NamedPipeStream`].
struct StreamInner {
    /// The underlying pipe handle. Held in an `Arc` (one level above)
    /// so `Clone` is cheap and all clones see the same kernel handle
    /// until the last `Arc` drops.
    handle: OwnedHandle,
    /// The IOCP-attached handle used to submit overlapped ops.
    file: Arc<RegisteredFile>,
    /// Per-source waker registry inside the reactor.
    source: Arc<Source>,
    /// Internal state for the futures-io poll-style traits.
    futures_io: Mutex<FuturesIoState>,
}

impl Drop for StreamInner {
    fn drop(&mut self) {
        // Deactivate first so no new submissions sneak in while the
        // reactor tears the registration down. `remove_io` then
        // forwards to `Registration::delete` which calls
        // `RegisteredFile::deactivate` again (idempotent) and
        // releases the slab slot.
        self.file.deactivate();
        let _ = Reactor::get().remove_io(&self.source);
    }
}

/// A bidirectional Windows named-pipe stream.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`] from `futures-io`. In
/// addition, the inherent [`read`](Self::read) / [`write`](Self::write)
/// methods operate on caller-owned buffers (`StableBuf` /
/// `StableBufMut`) and are the zero-extra-copy fast path; the
/// `futures-io` impls stage through internal `Vec<u8>` buffers because
/// the trait borrows the caller's slice for only the duration of the
/// `poll_*` call.
///
/// Cloning a `NamedPipeStream` shares the underlying pipe handle and
/// per-direction state via `Arc`.
#[derive(Clone)]
pub struct NamedPipeStream {
    inner: Arc<StreamInner>,
    /// Convenience copy of `inner.file` so the hot inherent
    /// `read`/`write` paths avoid one indirection.
    file: Arc<RegisteredFile>,
    /// Convenience copy of `inner.source` for the same reason.
    source: Arc<Source>,
}

impl NamedPipeStream {
    /// Wraps an `OwnedHandle` already opened with
    /// `FILE_FLAG_OVERLAPPED` and not yet attached to any IOCP. The
    /// caller is responsible for those preconditions; this method
    /// performs the IOCP attachment and reactor registration.
    fn from_overlapped_handle(handle: OwnedHandle) -> io::Result<Self> {
        // Step 1: bind the handle to our IOCP. The user_key starts at
        // 0; `Registration::add` rebinds it to the slab slot below.
        //
        // SAFETY: the caller of `from_overlapped_handle` guarantees
        // the handle was opened with `FILE_FLAG_OVERLAPPED` and is
        // not yet attached to another IOCP.
        let file = unsafe { Reactor::get().poller.register_file(&handle, 0)? };
        let file = Arc::new(file);

        // Step 2: insert into the reactor with edge-triggered semantics.
        // IOCP completions are delivered exactly once per op (the OS does
        // not redeliver a packet on the next `wait()` the way epoll/kqueue
        // re-asserts level readiness), so the reactor must use the
        // per-direction `events` counter rather than the global `tick` to
        // detect delivery. `insert_io` (level mode) would alias against
        // unrelated reactor activity in the same cycle and break the
        // race-free wakeup that the Idle-arm "snapshot before submit"
        // pattern below relies on.
        let source = Reactor::get().insert_io_edge(Registration::new_file(file.clone()))?;

        let inner = Arc::new(StreamInner {
            handle,
            file: file.clone(),
            source: source.clone(),
            futures_io: Mutex::new(FuturesIoState {
                read: DirState::Idle,
                write: DirState::Idle,
                flush: None,
            }),
        });
        Ok(Self {
            inner,
            file,
            source,
        })
    }

    /// Opens a client-side connection to the named pipe at `addr` with
    /// the default [`NamedPipeConnectOptions`].
    pub async fn connect<A: AsRef<OsStr>>(addr: A) -> io::Result<Self> {
        Self::connect_with_options(addr, NamedPipeConnectOptions::new()).await
    }

    /// Same as [`connect`](Self::connect) but uses the provided client
    /// options.
    pub async fn connect_with_options<A: AsRef<OsStr>>(
        addr: A,
        options: NamedPipeConnectOptions,
    ) -> io::Result<Self> {
        let name: Vec<u16> = addr.as_ref().encode_wide().chain(Some(0)).collect();
        // `CreateFileW` is blocking; run it on the global blocking pool.
        let owned = unblock(move || -> io::Result<OwnedHandle> {
            let access = match (options.read, options.write) {
                (true, true) => wsf::FILE_GENERIC_READ | wsf::FILE_GENERIC_WRITE,
                (true, false) => wsf::FILE_GENERIC_READ,
                (false, true) => wsf::FILE_GENERIC_WRITE,
                (false, false) => 0,
            };

            // Build a SECURITY_ATTRIBUTES on the stack only when the
            // caller requested handle inheritance. (CreateFileW with
            // OPEN_EXISTING ignores `lpSecurityDescriptor`, so the
            // client-side options struct does not expose an SD setter.)
            let sa_storage: wsec::SECURITY_ATTRIBUTES;
            let sa_ptr: *const wsec::SECURITY_ATTRIBUTES = if options.inherit_handle {
                sa_storage = wsec::SECURITY_ATTRIBUTES {
                    nLength: std::mem::size_of::<wsec::SECURITY_ATTRIBUTES>() as u32,
                    lpSecurityDescriptor: std::ptr::null_mut(),
                    bInheritHandle: 1,
                };
                &sa_storage
            } else {
                std::ptr::null()
            };

            // SAFETY: `name` is a NUL-terminated UTF-16 string; `sa_ptr`
            // is null or points to `sa_storage` on the current stack;
            // all other arguments are plain integers.
            let raw = unsafe {
                wsf::CreateFileW(
                    name.as_ptr(),
                    access,
                    0,
                    sa_ptr as *mut _,
                    wsf::OPEN_EXISTING,
                    options.custom_flags(),
                    std::ptr::null_mut(),
                )
            };
            if raw == wf::INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `raw` is a freshly-opened kernel handle we own.
            Ok(unsafe { OwnedHandle::from_raw_handle(raw as RawHandle) })
        })
        .await?;
        Self::from_overlapped_handle(owned)
    }

    /// Submits a single overlapped `ReadFile` against the pipe and
    /// awaits its completion, returning `(io::Result<bytes_read>, buf)`.
    ///
    /// The buffer is moved into the kernel for the duration of the
    /// op; on success its initialised length is set to `bytes_read`.
    /// A return of `Ok(0)` indicates EOF (peer closed the pipe). For
    /// `PipeMode::Message` pipes, `ERROR_MORE_DATA` is surfaced as a
    /// partial-read `Ok(n)` rather than an error.
    ///
    /// On error the caller's buffer is returned unchanged so the
    /// allocation can be reused. This matches
    /// [`compio_io::AsyncRead::read`]'s `BufResult` shape so the
    /// `compio` feature's trait impl is a thin re-wrap of this method.
    ///
    /// Multiple concurrent reads on the same `NamedPipeStream` are
    /// permitted — the reactor wakes every parked reader on each
    /// completion and the first whose `OpHandle::is_complete` reports
    /// true claims the result. This is correct but O(in-flight) per
    /// completion.
    ///
    /// # Cancellation
    ///
    /// Dropping the returned future before it resolves is safe: the
    /// in-flight `OpHandle` is held in an `OpHandleGuard` whose
    /// `Drop` issues `CancelIoEx` on the pending op (best-effort).
    /// The kernel still owns the original buffer until the (now
    /// aborted) completion drains, so the buffer is *not* returned to
    /// the caller on drop, but the stream itself remains usable —
    /// subsequent `read`/`write` calls will succeed normally. See the
    /// `read_future_drop_cancels_cleanly` integration test.
    pub async fn read<B: StableBufMut>(&self, buf: B) -> (io::Result<usize>, B) {
        // Arm-before-submit: the snapshot must be in place before the
        // kernel can publish the completion. See [`arm_source`].
        let src = self.source.clone();
        let file = self.file.clone();
        let mut buf_slot: Option<B> = Some(buf);
        let mut submitted: Option<OpHandleGuard<B>> = None;
        futures_lite::future::poll_fn(|cx| -> Poll<(io::Result<usize>, B)> {
            if submitted.is_none() {
                arm_source(&src, true, cx);
                let b = buf_slot.take().expect("read buf consumed twice");
                match file.submit_read(b) {
                    Submission::Complete { bytes, mut buf } => {
                        debug_assert!(
                            bytes <= buf.capacity(),
                            "polling crate violated postcondition: kernel wrote {} bytes but capacity is {}",
                            bytes,
                            buf.capacity()
                        );
                        // SAFETY: kernel wrote `bytes` initialised bytes;
                        // `bytes <= capacity` is enforced by the polling crate
                        // and guarded above in debug builds.
                        unsafe { buf.set_init(bytes) };
                        return Poll::Ready((Ok(bytes), buf));
                    }
                    Submission::Failed { error, buf } => {
                        return Poll::Ready((map_read_error(error), buf));
                    }
                    Submission::Pending(op) => submitted = Some(OpHandleGuard::new(op)),
                }
            }
            let guard = submitted.as_ref().unwrap();
            match poll_op(guard.as_ref(), &src, true, cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => {
                    // poll_readable error — the op is still in the
                    // guard; tear it down (best-effort cancel on drop)
                    // and we have no buffer to hand back.
                    let _ = submitted.take();
                    // SAFETY: this branch should not be reachable on
                    // the IOCP completion path; if it ever fires we
                    // genuinely lost the buffer to the kernel. Panic
                    // is preferable to fabricating a buffer.
                    unreachable!("poll_readable does not fail in edge mode: {e}")
                }
                Poll::Ready(Ok(())) => {
                    let g = submitted.take().unwrap();
                    let (res, mut v) = g.take().take();
                    let mapped = match res {
                        Ok(n) => Ok(n),
                        Err(e) => map_read_error(e),
                    };
                    if let Ok(n) = mapped {
                        debug_assert!(
                            n <= v.capacity(),
                            "polling crate violated postcondition: kernel wrote {} bytes but capacity is {}",
                            n,
                            v.capacity()
                        );
                        // SAFETY: kernel wrote `n` initialised bytes
                        // (or `n == 0` for the EOF-mapped path).
                        unsafe { v.set_init(n) };
                    }
                    Poll::Ready((mapped, v))
                }
            }
        })
        .await
    }

    /// Submits a single overlapped `WriteFile` against the pipe and
    /// awaits its completion, returning `(io::Result<bytes_written>, buf)`.
    ///
    /// As with [`read`](Self::read), the buffer is moved into the
    /// kernel for the duration of the op and returned to the caller
    /// on completion — success *and* error — so the allocation can
    /// be reused.
    ///
    /// # Cancellation
    ///
    /// Same semantics as [`read`](Self::read#cancellation): dropping
    /// the future cancels the in-flight op (`CancelIoEx`) and the
    /// stream remains usable, but the buffer is not handed back.
    pub async fn write<B: StableBuf>(&self, buf: B) -> (io::Result<usize>, B) {
        // Arm-before-submit: see [`arm_source`].
        let src = self.source.clone();
        let file = self.file.clone();
        let mut buf_slot: Option<B> = Some(buf);
        let mut submitted: Option<OpHandleGuard<B>> = None;
        futures_lite::future::poll_fn(|cx| -> Poll<(io::Result<usize>, B)> {
            if submitted.is_none() {
                arm_source(&src, false, cx);
                let b = buf_slot.take().expect("write buf consumed twice");
                match file.submit_write(b) {
                    Submission::Complete { bytes, buf } => {
                        return Poll::Ready((Ok(bytes), buf));
                    }
                    Submission::Failed { error, buf } => {
                        return Poll::Ready((Err(error), buf));
                    }
                    Submission::Pending(op) => submitted = Some(OpHandleGuard::new(op)),
                }
            }
            let guard = submitted.as_ref().unwrap();
            match poll_op(guard.as_ref(), &src, false, cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => {
                    let _ = submitted.take();
                    unreachable!("poll_writable does not fail in edge mode: {e}")
                }
                Poll::Ready(Ok(())) => {
                    let g = submitted.take().unwrap();
                    Poll::Ready(g.take().take())
                }
            }
        })
        .await
    }

    /// Flushes buffered writes to the named-pipe handle by invoking
    /// `FlushFileBuffers` on a blocking thread (the Win32 call may
    /// block until the peer drains its receive buffer).
    ///
    /// Used by both the futures-io [`AsyncWrite::poll_flush`] path
    /// (via `StreamInner`) and the `compio` feature's
    /// `AsyncWrite::flush`/`shutdown` impls.
    pub async fn flush(&self) -> io::Result<()> {
        let raw = self.inner.handle.as_raw_handle() as isize;
        unblock(move || {
            // SAFETY: the cloned `NamedPipeStream` (or its `Arc<StreamInner>`)
            // is held by the awaiting task, so the handle is live for the
            // duration of this blocking call.
            let ret = unsafe { wsf::FlushFileBuffers(raw as _) };
            if ret == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
        .await
    }

    /// Synchronously peeks at bytes available in the pipe without
    /// consuming them, using `PeekNamedPipe`.
    ///
    /// Returns the number of bytes copied into `buf`. May be `0` even
    /// when the peer is connected (no data buffered yet).
    pub fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut bytes_read: u32 = 0;
        // SAFETY: handle is valid for the lifetime of `self`; `buf`
        // is borrowed mutably for the duration of the call.
        let ret = unsafe {
            wsp::PeekNamedPipe(
                self.as_raw_handle() as _,
                buf.as_mut_ptr() as *mut _,
                buf.len() as u32,
                &mut bytes_read as *mut _,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if ret == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(bytes_read as usize)
        }
    }

    /// Fallible variant of [`FromRawHandle::from_raw_handle`].
    ///
    /// # Safety
    ///
    /// All of the following must hold for `handle`:
    /// 1. It is a valid Windows kernel handle to a named-pipe
    ///    instance that the caller is allowed to take ownership of.
    /// 2. It was opened with `FILE_FLAG_OVERLAPPED`.
    /// 3. It has *not* been associated with any other I/O completion
    ///    port.
    pub unsafe fn try_from_raw_handle(handle: RawHandle) -> io::Result<Self> {
        let owned = unsafe { OwnedHandle::from_raw_handle(handle) };
        Self::from_overlapped_handle(owned)
    }
}

impl fmt::Debug for NamedPipeStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedPipeStream")
            .field("handle", &self.inner.handle.as_raw_handle())
            .finish()
    }
}

impl AsRawHandle for NamedPipeStream {
    fn as_raw_handle(&self) -> RawHandle {
        self.inner.handle.as_raw_handle()
    }
}

impl AsHandle for NamedPipeStream {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.inner.handle.as_handle()
    }
}

impl TryFrom<OwnedHandle> for NamedPipeStream {
    type Error = io::Error;

    fn try_from(value: OwnedHandle) -> Result<Self> {
        Self::from_overlapped_handle(value)
    }
}

impl FromRawHandle for NamedPipeStream {
    /// Wraps a raw named-pipe `HANDLE` into a `NamedPipeStream`.
    ///
    /// # Safety
    ///
    /// See [`NamedPipeStream::try_from_raw_handle`].
    ///
    /// # Panics
    ///
    /// Panics on registration failure. Use
    /// [`try_from_raw_handle`](NamedPipeStream::try_from_raw_handle)
    /// for a fallible variant.
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        unsafe { Self::try_from_raw_handle(handle) }
            .expect("failed to register named pipe handle with IOCP")
    }
}

// =====================================================================
// futures-io: AsyncRead / AsyncWrite
// =====================================================================

impl AsyncRead for NamedPipeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut state = this.inner.futures_io.lock().unwrap();
        loop {
            match &mut state.read {
                DirState::Idle => {
                    // Step 1 — register IOCP read interest BEFORE submitting
                    // the op. This is the critical ordering: with the
                    // completion-mode polling crate, each IOCP packet wakes
                    // a waker exactly once, and that waker must already be
                    // installed when the kernel publishes the completion.
                    // If we submitted first and registered after, a fast
                    // completion racing ahead of registration could land
                    // before our waker existed and the wake event would be
                    // lost.
                    //
                    // We drain any stale `Ready(Ok)` returns in a loop:
                    // those represent prior consumed events; only a
                    // `Pending` return guarantees our waker is parked on
                    // the *next* delivery (the upcoming completion of the
                    // op we are about to submit).
                    loop {
                        match this.source.poll_readable(cx) {
                            Poll::Ready(Ok(())) => continue,
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => break,
                        }
                    }

                    // Step 2 — submit the read into an owned `Vec<u8>`. The
                    // owned buffer survives across futures-io's borrow
                    // boundary (the caller's `&mut [u8]` only lives for
                    // the duration of this `poll_read` call), and also
                    // across cancellation: if the caller drops this
                    // future, the kernel keeps writing into our buffer
                    // until the eventual completion, which the dispatcher
                    // then drains.
                    let v: Vec<u8> = Vec::with_capacity(buf.len().max(1));
                    match this.file.submit_read(v) {
                        // Sync completion — no IOCP packet was queued, so
                        // the waker we just armed will not fire from this
                        // op. That's fine: we have the result in hand.
                        Submission::Complete {
                            bytes,
                            buf: mut filled,
                        } => {
                            debug_assert!(
                                bytes <= filled.capacity(),
                                "polling crate violated postcondition: kernel wrote {} bytes but capacity is {}",
                                bytes,
                                filled.capacity()
                            );
                            // SAFETY: kernel wrote `bytes` initialised
                            // bytes into the buffer; `bytes <= capacity`
                            // is enforced by the polling crate.
                            unsafe { filled.set_init(bytes) };
                            let n = bytes.min(buf.len());
                            buf[..n].copy_from_slice(&filled[..n]);
                            return Poll::Ready(Ok(n));
                        }
                        // Sync error — again, no IOCP packet, no wake.
                        Submission::Failed { error, .. } => {
                            return Poll::Ready(map_read_error(error));
                        }
                        // Async — stash the in-flight handle and fall
                        // through to the Pending arm to consult it.
                        Submission::Pending(op) => {
                            state.read = DirState::Pending(OpHandleGuard::new(op));
                        }
                    }
                }
                DirState::Pending(guard) => {
                    if !guard.as_ref().is_complete() {
                        // Drain stale `Ready(Ok)` returns the same way as
                        // in the Idle arm. The completion event we are
                        // waiting for will surface as one of these
                        // `Ready(Ok)` returns (or already did, in which
                        // case `is_complete()` will be true on the next
                        // iteration); stopping at the first `Pending`
                        // guarantees our waker is parked.
                        loop {
                            match this.source.poll_readable(cx) {
                                Poll::Ready(Ok(())) => continue,
                                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                                Poll::Pending => break,
                            }
                        }
                        if !guard.as_ref().is_complete() {
                            return Poll::Pending;
                        }
                    }
                    // Take the result and copy into the caller's slice.
                    let DirState::Pending(g) = std::mem::replace(&mut state.read, DirState::Idle)
                    else {
                        unreachable!()
                    };
                    return Poll::Ready(match g.take().take() {
                        (Ok(n), mut v) => {
                            debug_assert!(
                                n <= v.capacity(),
                                "polling crate violated postcondition: kernel wrote {} bytes but capacity is {}",
                                n,
                                v.capacity()
                            );
                            // SAFETY: kernel wrote `n` initialised bytes;
                            // `n <= capacity`.
                            unsafe { v.set_len(n) };
                            let copy = n.min(buf.len());
                            buf[..copy].copy_from_slice(&v[..copy]);
                            Ok(copy)
                        }
                        (Err(e), _) => map_read_error(e),
                    });
                }
            }
        }
    }
}

impl AsyncWrite for NamedPipeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut state = this.inner.futures_io.lock().unwrap();
        loop {
            match &mut state.write {
                DirState::Idle => {
                    // Step 1 — register IOCP write interest BEFORE
                    // submitting. Same ordering rule as `poll_read`:
                    // the waker must exist before the kernel publishes
                    // the completion, otherwise the wake is lost. We
                    // drain stale `Ready(Ok)` returns (each represents
                    // a previously consumed completion) and only stop
                    // on `Pending`, which proves our waker is parked.
                    loop {
                        match this.source.poll_writable(cx) {
                            Poll::Ready(Ok(())) => continue,
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => break,
                        }
                    }

                    // Step 2 — submit the write from an owned copy of
                    // the caller's bytes. The owned buffer survives
                    // both the borrow boundary and a cancellation drop
                    // (the kernel will keep reading from it until the
                    // completion fires).
                    let v: Vec<u8> = buf.to_vec();
                    match this.file.submit_write(v) {
                        Submission::Complete { bytes, .. } => {
                            return Poll::Ready(Ok(bytes));
                        }
                        Submission::Failed { error, .. } => {
                            return Poll::Ready(Err(error));
                        }
                        Submission::Pending(op) => {
                            state.write = DirState::Pending(OpHandleGuard::new(op));
                        }
                    }
                }
                DirState::Pending(guard) => {
                    if !guard.as_ref().is_complete() {
                        loop {
                            match this.source.poll_writable(cx) {
                                Poll::Ready(Ok(())) => continue,
                                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                                Poll::Pending => break,
                            }
                        }
                        if !guard.as_ref().is_complete() {
                            return Poll::Pending;
                        }
                    }
                    let DirState::Pending(g) = std::mem::replace(&mut state.write, DirState::Idle)
                    else {
                        unreachable!()
                    };
                    let (res, _) = g.take().take();
                    return Poll::Ready(res);
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut state = this.inner.futures_io.lock().unwrap();
        // Spawn the blocking FlushFileBuffers on the first call;
        // subsequent polls drive the same task. When it completes we
        // clear `flush` AND reset `write` to `Idle` — mirroring the
        // previous implementation's fix for "Task waken frequently":
        // a stale `Pending` write left in `write` would otherwise keep
        // re-arming the waker on every poll.
        let task = match state.flush.as_mut() {
            Some(t) => t,
            None => {
                let raw = this.inner.handle.as_raw_handle() as isize;
                state.flush.insert(unblock(move || {
                    // SAFETY: we keep the `OwnedHandle` alive for as
                    // long as any clone of the stream exists, so the
                    // raw pointer remains valid here.
                    let ret = unsafe { wsf::FlushFileBuffers(raw as _) };
                    if ret == 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(())
                    }
                }))
            }
        };
        let res = ready!(Pin::new(task).poll(cx));
        state.flush = None;
        state.write = DirState::Idle;
        Poll::Ready(res)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        // Windows has no half-close primitive equivalent to
        // `shutdown(SHUT_WR)` for client-side pipes. Match the
        // conventional `AsyncWrite::poll_close` semantics by flushing
        // and deferring the actual close to `Drop`.
        self.poll_flush(cx)
    }
}

/// Map `ERROR_BROKEN_PIPE` (peer closed) and a few cancellation
/// codes to an EOF (`Ok(0)`) result for the futures-io read path.
///
/// Note: `ERROR_MORE_DATA` (message-mode partial read) is *not*
/// handled here — the polling crate translates it to `Ok(bytes)`
/// inside `OpHandle::take_inner` before we ever see it, so by the
/// time `map_read_error` runs the result is already `Ok(n)`. See
/// `polling::iocp::ERROR_MORE_DATA`.
fn map_read_error(e: io::Error) -> io::Result<usize> {
    match e.raw_os_error() {
        Some(code) if code == wf::ERROR_BROKEN_PIPE as i32 => Ok(0),
        Some(code) if code == wf::ERROR_OPERATION_ABORTED as i32 => Ok(0),
        Some(code) if code == wf::ERROR_HANDLE_EOF as i32 => Ok(0),
        _ => Err(e),
    }
}
