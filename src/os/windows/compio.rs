//! `compio_io::AsyncRead` / `AsyncWrite` impls for [`NamedPipeStream`].
//!
//! See `docs/named-pipe.design.md` §6 for the design rationale.
//! The adapter is a thin re-wrap of the inherent
//! [`NamedPipeStream::read`] / [`NamedPipeStream::write`] /
//! [`NamedPipeStream::flush`] methods:
//!
//! 1. Wrap the caller's `B: IoBuf` / `B: IoBufMut` in
//!    [`ReadBufWrap`] / [`WriteBufWrap`] so the kernel sees a
//!    [`polling::os::iocp::StableBuf`]/`StableBufMut`.
//! 2. Submit through the inherent IOCP path.
//! 3. Unwrap on completion and repackage as a
//!    [`compio_buf::BufResult`].
//!
//! `flush`/`shutdown` both call [`NamedPipeStream::flush`]; Windows
//! has no half-close primitive for client-side pipes, so `shutdown`
//! is a flush. See §6.6.

use compio_buf::{BufResult, IoBuf, IoBufMut};
use compio_io::{AsyncRead, AsyncWrite};
use polling::os::iocp::{StableBuf, StableBufMut};

use super::NamedPipeStream;

// ---------------------------------------------------------------------
// Send precondition
// ---------------------------------------------------------------------
//
// `polling::os::iocp::StableBuf` requires `Send + 'static`: the IOCP
// completion runs on the `Poller::wait` thread, which may differ from
// the submitter, so the buffer crosses a thread boundary while the
// kernel owns it.
//
// `compio_buf::IoBuf{,Mut}` does **not** require `Send` because
// compio's runtime is single-threaded. To bridge the two worlds we
// `unsafe impl Send` the buffer wrappers below. This is **sound only
// for byte buffers without thread-local state** — every `IoBuf{,Mut}`
// impl shipped by compio-buf 0.8 (`Vec<u8>`, `Box<[u8]>`, `[u8; N]`,
// `BytesMut`, `Bytes`, `ArrayVec<u8, N>`, `SmallVec<[u8; N]>`) is
// `Send` and the wrapper is therefore safe for them.
//
// Callers passing a non-`Send` custom `IoBufMut` impl violate this
// precondition — there is no compile-time way to forbid it because
// the trait's method bound is `B: IoBufMut`, which we cannot narrow.
// See `docs/named-pipe.design.md` §6.5.

/// Adapter that exposes a `compio_buf::IoBuf` value as a
/// `polling::os::iocp::StableBuf` for the IOCP write path.
///
/// Boxed because polling's `OpInner` slot is dimensioned at 32
/// bytes; an inline `B` (e.g. `BytesMut` at 32 B + extra wrapper
/// state) would overflow it. The `Box` keeps the wrapper a single
/// pointer wide regardless of `B`'s size.
struct WriteBufWrap<B: IoBuf>(Box<B>);

// SAFETY: see the "Send precondition" comment above the imports —
// every byte buffer in compio-buf 0.8 is `Send`; the wrapper merely
// owns a `Box<B>` whose backing storage does not carry thread-affinity.
// We cannot narrow this to `B: Send` because compio's `AsyncRead::read<B>`
// signature does not require `B: Send`, and trait-method bounds cannot
// be tightened in an impl block.
unsafe impl<B: IoBuf> Send for WriteBufWrap<B> {}

// SAFETY: forwards `as_ptr` / `len` to `B`'s `IoBuf` impl, which
// returns a stable pointer/length pair for the initialised region.
unsafe impl<B: IoBuf + 'static> StableBuf for WriteBufWrap<B> {
    fn as_ptr(&self) -> *const u8 {
        self.0.buf_ptr()
    }
    fn len(&self) -> usize {
        self.0.buf_len()
    }
}

/// Adapter that exposes a `compio_buf::IoBufMut` value as a
/// `polling::os::iocp::StableBufMut` for the IOCP read path.
///
/// Boxed for the same reason as [`WriteBufWrap`]; additionally the
/// box gives us a stable address for `B` so we can serve
/// `StableBuf::capacity` (which takes `&self`) by calling
/// `IoBufMut::buf_capacity` (which takes `&mut self`) through an
/// unsafe re-borrow — sound because the `Box` is uniquely owned by
/// the wrapper for the duration of the kernel op.
struct ReadBufWrap<B: IoBufMut>(Box<B>);

// SAFETY: same precondition reasoning as `WriteBufWrap`.
unsafe impl<B: IoBufMut> Send for ReadBufWrap<B> {}

// SAFETY: forwards `as_ptr` / `len` to `B`'s `IoBuf` impl. `len`
// reports initialised bytes — for a fresh read buffer this is
// typically `0`, which is what the IOCP path expects.
unsafe impl<B: IoBufMut + 'static> StableBuf for ReadBufWrap<B> {
    fn as_ptr(&self) -> *const u8 {
        self.0.buf_ptr()
    }
    fn len(&self) -> usize {
        self.0.buf_len()
    }
}

// SAFETY: `set_init` forwards to `SetLen::set_len`, whose contract
// matches `StableBufMut::set_init` byte-for-byte. `as_mut_ptr` and
// `capacity` forward through the uniquely-owned `Box<B>`.
unsafe impl<B: IoBufMut + 'static> StableBufMut for ReadBufWrap<B> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.buf_mut_ptr() as *mut u8
    }
    fn capacity(&self) -> usize {
        // SAFETY: `IoBufMut::buf_capacity` requires `&mut self` but
        // is a pure accessor (it reads the length of the slice
        // returned by `as_uninit`, which itself only reads pointer
        // metadata stored in `B`). Re-borrowing the boxed `B` as
        // `&mut` for the duration of the call is sound because:
        //  - the wrapper uniquely owns the `Box<B>`,
        //  - polling holds at most one `&self` reference to the
        //    wrapper while the op is in flight (per its trait
        //    contract on `StableBuf`),
        //  - the call does not escape a `&mut B` to user code.
        let this = self as *const Self as *mut Self;
        unsafe { (*this).0.buf_capacity() }
    }
    unsafe fn set_init(&mut self, n: usize) {
        // SAFETY: caller (polling's IOCP completion path) verified
        // `n <= capacity()` and that the first `n` bytes are
        // initialised. `SetLen::set_len` has the identical contract.
        unsafe { self.0.set_len(n) }
    }
}

impl AsyncRead for &NamedPipeStream {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        let wrapped = ReadBufWrap(Box::new(buf));
        let (res, wrapped) = NamedPipeStream::read(self, wrapped).await;
        BufResult(res, *wrapped.0)
    }
}

impl AsyncRead for NamedPipeStream {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        AsyncRead::read(&mut &*self, buf).await
    }
}

impl AsyncWrite for &NamedPipeStream {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        let wrapped = WriteBufWrap(Box::new(buf));
        let (res, wrapped) = NamedPipeStream::write(self, wrapped).await;
        BufResult(res, *wrapped.0)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        NamedPipeStream::flush(self).await
    }

    /// Windows has no half-close primitive equivalent to
    /// `shutdown(SHUT_WR)` for client-side named pipes; defer the
    /// actual close to `Drop` and just flush here. See §6.6.
    async fn shutdown(&mut self) -> std::io::Result<()> {
        NamedPipeStream::flush(self).await
    }
}

impl AsyncWrite for NamedPipeStream {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        AsyncWrite::write(&mut &*self, buf).await
    }
    async fn flush(&mut self) -> std::io::Result<()> {
        NamedPipeStream::flush(self).await
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        NamedPipeStream::flush(self).await
    }
}
