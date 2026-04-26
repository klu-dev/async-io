// SPDX-License-Identifier: MIT OR Apache-2.0

//! Configuration types for Windows named-pipe endpoints.
//!
//! The actual server / client endpoints live in
//! [`super::NamedPipeListener`] and [`super::NamedPipeStream`], which
//! drive overlapped I/O through `polling`'s completion-mode file API.
//! This module just exposes the value-style configuration that those
//! endpoints accept.

use std::io;

use windows_sys::Win32::Foundation as wf;
use windows_sys::Win32::Security::Authorization as wsa;
use windows_sys::Win32::Security::PSECURITY_DESCRIPTOR;
use windows_sys::Win32::Storage::FileSystem as wsf;
use windows_sys::Win32::System::Pipes as wsp;

/// Wire-format and read-side mode for a named pipe.
///
/// The Windows API exposes pipe write semantics (`PIPE_TYPE_*`) and
/// read semantics (`PIPE_READMODE_*`) as separate flags but rejects
/// nonsensical combinations (`PIPE_TYPE_BYTE | PIPE_READMODE_MESSAGE`
/// returns `ERROR_INVALID_PARAMETER`). This enum exposes only the
/// three valid combinations so invalid configurations cannot be
/// constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PipeMode {
    /// Byte stream in both directions
    /// (`PIPE_TYPE_BYTE | PIPE_READMODE_BYTE`). The default.
    #[default]
    Byte,
    /// Message-framed wire format, read as a byte stream
    /// (`PIPE_TYPE_MESSAGE | PIPE_READMODE_BYTE`). Useful when the
    /// server wants to preserve message boundaries for the client but
    /// read incoming data without caring about boundaries itself.
    MessageStreamRead,
    /// Message-framed wire format, read one message per `read` call
    /// (`PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE`).
    ///
    /// In overlapped non-blocking mode the read buffer must be large
    /// enough to hold the entire message; otherwise the read returns
    /// `ERROR_MORE_DATA` (and `ERROR_BROKEN_PIPE` if the peer closes
    /// mid-message).
    Message,
}

impl PipeMode {
    /// Returns the combined `PIPE_TYPE_* | PIPE_READMODE_*` flag bits
    /// passed to `CreateNamedPipeW`.
    pub(crate) fn to_flags(self) -> u32 {
        match self {
            PipeMode::Byte => wsp::PIPE_TYPE_BYTE | wsp::PIPE_READMODE_BYTE,
            PipeMode::MessageStreamRead => wsp::PIPE_TYPE_MESSAGE | wsp::PIPE_READMODE_BYTE,
            PipeMode::Message => wsp::PIPE_TYPE_MESSAGE | wsp::PIPE_READMODE_MESSAGE,
        }
    }
}

/// Direction of data flow through a named pipe, from the *server* side.
///
/// Maps to the `PIPE_ACCESS_*` flags of `CreateNamedPipeW`. The crate
/// always OR's `FILE_FLAG_OVERLAPPED` into the final open mode
/// internally; callers cannot disable it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PipeAccess {
    /// Server reads only (`PIPE_ACCESS_INBOUND`).
    Inbound,
    /// Server writes only (`PIPE_ACCESS_OUTBOUND`).
    Outbound,
    /// Both directions (`PIPE_ACCESS_DUPLEX`). The default.
    #[default]
    Duplex,
}

impl PipeAccess {
    pub(crate) fn to_flag(self) -> u32 {
        match self {
            PipeAccess::Inbound => wsf::PIPE_ACCESS_INBOUND,
            PipeAccess::Outbound => wsf::PIPE_ACCESS_OUTBOUND,
            PipeAccess::Duplex => wsf::PIPE_ACCESS_DUPLEX,
        }
    }
}

/// Server-side configuration passed to `CreateNamedPipeW`.
///
/// Holds the data direction, instance limit, per-direction buffer
/// size hints, an optional custom security descriptor, and the
/// `bInheritHandle` flag for the underlying `SECURITY_ATTRIBUTES`.
/// Construct via [`new`](Self::new) or [`Default`] and customise with
/// the chained setters; the crate always OR's `FILE_FLAG_OVERLAPPED`
/// into the final open mode internally.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NamedPipeOpenOptions {
    pub(crate) access: PipeAccess,
    pub(crate) max_instances: u32,
    pub(crate) out_buffer_size: u32,
    pub(crate) in_buffer_size: u32,
    /// Self-relative `SECURITY_DESCRIPTOR` bytes, or `None` to use
    /// the system default. See [`security_descriptor`](Self::security_descriptor).
    pub(crate) security_descriptor: Option<Box<[u8]>>,
    /// Sets the `bInheritHandle` field of `SECURITY_ATTRIBUTES`.
    pub(crate) inherit_handle: bool,
}

impl NamedPipeOpenOptions {
    /// Returns the default open options:
    /// `Duplex` access, unlimited instances, 64 KiB in/out buffers,
    /// no custom security descriptor, and non-inheritable handles.
    pub fn new() -> Self {
        Self {
            access: PipeAccess::Duplex,
            max_instances: wsp::PIPE_UNLIMITED_INSTANCES,
            out_buffer_size: 65536,
            in_buffer_size: 65536,
            security_descriptor: None,
            inherit_handle: false,
        }
    }

    /// Sets which directions the server side may use.
    pub fn access(mut self, access: PipeAccess) -> Self {
        self.access = access;
        self
    }

    /// Sets the maximum number of pipe instances
    /// (default `PIPE_UNLIMITED_INSTANCES`).
    pub fn max_instances(mut self, instances: u32) -> Self {
        self.max_instances = instances;
        self
    }

    /// Sets the outbound (server → client) buffer size hint in bytes
    /// (default 64 KiB).
    pub fn out_buffer_size(mut self, size: u32) -> Self {
        self.out_buffer_size = size;
        self
    }

    /// Sets the inbound (client → server) buffer size hint in bytes
    /// (default 64 KiB).
    pub fn in_buffer_size(mut self, size: u32) -> Self {
        self.in_buffer_size = size;
        self
    }

    /// Sets a custom security descriptor for the pipe.
    ///
    /// `sd` must contain the bytes of a **self-relative**
    /// `SECURITY_DESCRIPTOR` — the contiguous-buffer form, not the
    /// absolute form whose internal `Owner`/`Group`/`Sacl`/`Dacl`
    /// fields are pointers. Self-relative SDs are produced by
    /// `ConvertStringSecurityDescriptorToSecurityDescriptorW` (after
    /// copying the `LocalAlloc`'d output and freeing it) or by
    /// `MakeSelfRelativeSD`. See
    /// [`security_descriptor_sddl`](Self::security_descriptor_sddl)
    /// for a convenience that does this for you.
    ///
    /// # Format requirement
    ///
    /// Passing absolute-form SD bytes, truncated bytes, or arbitrary
    /// bytes is **not** undefined behaviour: the Windows kernel
    /// validates the buffer and either rejects it with
    /// `ERROR_INVALID_SECURITY_DESCR` (surfaced as an `io::Error`
    /// from `bind`/`accept`) or — in the unfortunate case where the
    /// bytes happen to look like a well-formed but semantically
    /// wrong SD — creates the pipe with an incorrect access-control
    /// policy. The kernel never dereferences pointers from the
    /// caller's address space.
    ///
    /// # Default
    ///
    /// If unset, the pipe receives the default security descriptor of
    /// the calling process's token. Per the
    /// [`CreateNamedPipeW` documentation][create-named-pipe], that
    /// default grants full control to the LocalSystem account, the
    /// Administrators group, and the pipe creator, and read access to
    /// the Everyone group and the anonymous account.
    ///
    /// [create-named-pipe]: https://learn.microsoft.com/windows/win32/api/winbase/nf-winbase-createnamedpipew
    pub fn security_descriptor(mut self, sd: impl Into<Box<[u8]>>) -> Self {
        self.security_descriptor = Some(sd.into());
        self
    }

    /// Convenience: parses an [SDDL] string into a self-relative
    /// `SECURITY_DESCRIPTOR` and stores it as if
    /// [`security_descriptor`](Self::security_descriptor) had been
    /// called with the resulting bytes.
    ///
    /// Wraps `ConvertStringSecurityDescriptorToSecurityDescriptorW`.
    /// Returns the underlying `io::Error` from
    /// `GetLastError` if the SDDL string is malformed.
    ///
    /// # Examples
    ///
    /// Grant full control to the Everyone group:
    ///
    /// ```ignore
    /// let opts = NamedPipeOpenOptions::new()
    ///     .security_descriptor_sddl("D:(A;;GA;;;WD)")?;
    /// ```
    ///
    /// [SDDL]: https://learn.microsoft.com/windows/win32/secauthz/security-descriptor-string-format
    pub fn security_descriptor_sddl(self, sddl: &str) -> io::Result<Self> {
        let bytes = sddl_to_self_relative_bytes(sddl)?;
        Ok(self.security_descriptor(bytes))
    }

    /// Sets the `bInheritHandle` field of the `SECURITY_ATTRIBUTES`
    /// passed to `CreateNamedPipeW` (default `false`).
    ///
    /// When `true`, child processes spawned with handle inheritance
    /// enabled may inherit the pipe instance handle.
    pub fn inherit_handle(mut self, on: bool) -> Self {
        self.inherit_handle = on;
        self
    }

    /// Returns the configured server-side data direction.
    pub fn get_access(&self) -> PipeAccess {
        self.access
    }

    /// Returns the configured maximum number of pipe instances.
    pub fn get_max_instances(&self) -> u32 {
        self.max_instances
    }

    /// Returns the configured outbound buffer size hint in bytes.
    pub fn get_out_buffer_size(&self) -> u32 {
        self.out_buffer_size
    }

    /// Returns the configured inbound buffer size hint in bytes.
    pub fn get_in_buffer_size(&self) -> u32 {
        self.in_buffer_size
    }

    /// Returns the configured custom security descriptor bytes, if any.
    pub fn get_security_descriptor(&self) -> Option<&[u8]> {
        self.security_descriptor.as_deref()
    }

    /// Returns whether `bInheritHandle` is enabled.
    pub fn get_inherit_handle(&self) -> bool {
        self.inherit_handle
    }
}

impl Default for NamedPipeOpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Options for opening the client side of a named pipe.
///
/// Mirrors the server-side [`NamedPipeOpenOptions`] for the client
/// connect path. The crate always sets `FILE_FLAG_OVERLAPPED`
/// internally; callers cannot disable it.
///
/// Note: a custom security descriptor cannot be set on the client
/// side because `CreateFileW` ignores `lpSecurityDescriptor` when
/// opening an existing object (only the server-side `CreateNamedPipeW`
/// honors it). Only [`inherit_handle`](Self::inherit_handle) from the
/// `SECURITY_ATTRIBUTES` is meaningful here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct NamedPipeConnectOptions {
    pub(crate) read: bool,
    pub(crate) write: bool,
    pub(crate) write_through: bool,
    pub(crate) inherit_handle: bool,
}

impl NamedPipeConnectOptions {
    /// Returns connect options with read + write access (the default).
    pub fn new() -> Self {
        Self {
            read: true,
            write: true,
            write_through: false,
            inherit_handle: false,
        }
    }

    /// Sets whether the client may read from the pipe (default `true`).
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Sets whether the client may write to the pipe (default `true`).
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Enables `FILE_FLAG_WRITE_THROUGH`, which causes writes to bypass
    /// the intermediate cache and not return until the data is
    /// transmitted to the remote pipe instance (default `false`).
    pub fn write_through(mut self, on: bool) -> Self {
        self.write_through = on;
        self
    }

    /// Sets the `bInheritHandle` field of the `SECURITY_ATTRIBUTES`
    /// passed to `CreateFileW` (default `false`).
    ///
    /// When `true`, child processes spawned with handle inheritance
    /// enabled may inherit the connected pipe handle.
    pub fn inherit_handle(mut self, on: bool) -> Self {
        self.inherit_handle = on;
        self
    }

    /// Returns the configured read flag.
    pub fn get_read(&self) -> bool {
        self.read
    }

    /// Returns the configured write flag.
    pub fn get_write(&self) -> bool {
        self.write
    }

    /// Returns whether `FILE_FLAG_WRITE_THROUGH` is enabled.
    pub fn get_write_through(&self) -> bool {
        self.write_through
    }

    /// Returns whether `bInheritHandle` is enabled.
    pub fn get_inherit_handle(&self) -> bool {
        self.inherit_handle
    }

    pub(crate) fn custom_flags(&self) -> u32 {
        let mut flags = wsf::FILE_FLAG_OVERLAPPED;
        if self.write_through {
            flags |= wsf::FILE_FLAG_WRITE_THROUGH;
        }
        flags
    }
}

impl Default for NamedPipeConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse an SDDL string into a freshly-allocated, Rust-owned
/// self-relative `SECURITY_DESCRIPTOR` byte buffer.
///
/// Calls `ConvertStringSecurityDescriptorToSecurityDescriptorW`,
/// copies the resulting `LocalAlloc`'d bytes into a `Box<[u8]>`, then
/// `LocalFree`'s the original allocation. The returned boxed slice is the
/// only remaining owner of the SD bytes.
fn sddl_to_self_relative_bytes(sddl: &str) -> io::Result<Box<[u8]>> {
    // Build a NUL-terminated wide string.
    let wide: Vec<u16> = sddl.encode_utf16().chain(std::iter::once(0)).collect();

    let mut psd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    let mut size: u32 = 0;
    // SAFETY: `wide` is a NUL-terminated UTF-16 string valid for the
    // call duration; `psd` and `size` are valid out-pointers.
    let ok = unsafe {
        wsa::ConvertStringSecurityDescriptorToSecurityDescriptorW(
            wide.as_ptr(),
            wsa::SDDL_REVISION_1,
            &mut psd,
            &mut size,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    debug_assert!(!psd.is_null());

    // SAFETY: on success, `psd` points to `size` bytes of an allocated
    // self-relative SD. We copy them out into a Rust-owned buffer.
    let bytes = unsafe { std::slice::from_raw_parts(psd as *const u8, size as usize) }
        .to_vec()
        .into_boxed_slice();

    // SAFETY: `psd` was allocated by
    // `ConvertStringSecurityDescriptorToSecurityDescriptorW`, which
    // documents `LocalFree` as the matching deallocator.
    unsafe {
        wf::LocalFree(psd as wf::HLOCAL);
    }

    Ok(bytes)
}
