# Windows Named Pipe — Design Notes

Scope: the IOCP-completion-mode named-pipe support in `async_io::os::windows`,
on branch `kail/windows_named_pipe`.

Files:

- [src/os/windows.rs](../src/os/windows.rs) — `NamedPipeListener`,
  `NamedPipeStream`, the IOCP submission/poll glue.
- [src/os/windows/named_pipe.rs](../src/os/windows/named_pipe.rs) — the
  configuration types (`PipeMode`, `PipeAccess`, `NamedPipeOpenOptions`,
  `NamedPipeConnectOptions`).
- [src/reactor.rs](../src/reactor.rs) — `WakeMode`, the per-direction
  `events` counter, and `Reactor::insert_io_edge`.
- [src/reactor/windows.rs](../src/reactor/windows.rs) — the
  `Registration::File(Arc<RegisteredFile>)` variant and its `add` /
  `modify` / `delete` handling.
- [tests/windows_named_pipe.rs](../tests/windows_named_pipe.rs) —
  integration tests.

The old design notes (pre-rewrite, when the implementation was layered
over `polling::os::iocp::{read_file_overlapped, write_file_overlapped,
AsFileHandle, FileOverlappedWrapper}`) are preserved in
[`named-pipe.design.md.bak`](./named-pipe.design.md.bak); §6 below
summarises which of those concerns are now resolved.

---

## 1. Architecture

`async-io` already supports two readiness-mode IOCP source kinds on
Windows:

- **Sockets** — registered via AFD (`Registration::Socket(RawSocket)`).
- **Waitable handles** — registered via
  `RegisterWaitForSingleObject` (`Registration::Handle(RawHandle)`).

This branch adds a third kind:

- **Overlapped file handles** (named pipes today, mailslots / device
  files later) — registered via `polling`'s **completion-mode** IOCP
  file API (`Registration::File(Arc<RegisteredFile>)`).

All three kinds share the same `polling::Poller` instance, the same
reactor loop, and the same per-source `Source { wakers, ... }` used
elsewhere in the crate. The only thing that varies per-kind is

1. how `polling` learns about the source (`Registration::add` /
   `modify` / `delete` in [src/reactor/windows.rs](../src/reactor/windows.rs)),
   and
2. how `Source::poll_readable` / `poll_writable` decide that a wakeup
   is real (the wake-mode discussion below).

### 1.1 Public API surface (`async_io::os::windows`)

| Item | Purpose |
| --- | --- |
| `PipeMode` | 3-variant enum: `Byte` (default), `MessageStreamRead`, `Message`. Maps to the `PIPE_TYPE_* \| PIPE_READMODE_*` server flags. Invalid combinations are unrepresentable. |
| `PipeAccess` | 3-variant enum: `Inbound`, `Outbound`, `Duplex` (default). Maps to `PIPE_ACCESS_*`. |
| `NamedPipeOpenOptions` | `#[non_exhaustive]` server-side configuration: access, max instances, in/out buffer sizes. Built directly or via the listener builder. |
| `NamedPipeConnectOptions` | `#[non_exhaustive]` client-side configuration: read, write, write-through. |
| `NamedPipeListener` | Cloneable server endpoint. `bind()` constructs from raw options; `builder()` returns a fluent builder. `accept()` returns a `NamedPipeStream`. |
| `NamedPipeStream` | Cloneable bidirectional pipe. Owned-buffer fast path via inherent `read` / `write` (`StableBuf` / `StableBufMut`); `futures-io` `AsyncRead` / `AsyncWrite` for ecosystem compatibility. |

Notes on the public surface:

- `FILE_FLAG_OVERLAPPED` is OR'd in internally on both server (`accept`)
  and client (`connect_with_options`) paths; callers cannot disable it.
- The `NamedPipeListener::accepted` latch is an `AtomicBool` shared
  across clones, so `FILE_FLAG_FIRST_PIPE_INSTANCE` is requested
  exactly once per listener (and its clones).
- `NamedPipeStream::peek` is a synchronous `PeekNamedPipe` that does
  not consume bytes from the kernel buffer.
- `poll_close` resolves to `Ok(())` after delegating to `poll_flush`
  (a `FlushFileBuffers` on the blocking pool); the actual handle close
  happens when the last `Arc<StreamInner>` drops.

### 1.2 IOCP completion API used from `polling`

This crate depends on a fork of `polling` that exposes a
completion-mode IOCP file API on the
`klu-dev/polling/kail/windows_file` branch:

```rust
pub trait PollerIocpFileExt {
    unsafe fn register_file(&self, h: &impl AsRawHandle, user_key: u64)
        -> io::Result<RegisteredFile>;
}

impl RegisteredFile {
    pub fn set_user_key(&self, key: u64);
    pub fn deactivate(&self);

    pub fn submit_read<B: StableBufMut>(&self, buf: B)  -> Submission<B>;
    pub fn submit_write<B: StableBuf>(&self, buf: B)    -> Submission<B>;
    pub fn submit_connect_named_pipe(&self)             -> Submission<()>;
}

pub enum Submission<B> {
    Complete { bytes: usize, buf: B },   // sync inline completion
    Pending(OpHandle<B>),                // async, completion en route
    Failed   { error: io::Error, buf: B },
}

impl<B> OpHandle<B> {
    pub fn is_complete(&self) -> bool;
    pub fn take(self) -> io::Result<(usize, B)>;
}
```

`StableBuf` / `StableBufMut` are blanket-implemented for `Vec<u8>`,
`Box<[u8]>`, `&'static [u8]`, and `()` (used by
`submit_connect_named_pipe`).

### 1.3 Reactor wake modes

`Source::poll_readable` and `Source::poll_writable` need to answer the
question "has a *new* event been delivered since I last asked?". The
answer differs by source kind, so each `Source` carries a `WakeMode`
chosen at registration time:

- **`WakeMode::Level`** — sockets and waitable handles. The reactor
  re-asserts readiness whenever the underlying kernel state remains
  ready. Implementation: compare against the global reactor `tick`
  (the existing scheme used everywhere on Unix and pre-existing on
  Windows).

- **`WakeMode::Edge`** — IOCP completion-mode files. Each completion
  packet is delivered exactly once by the kernel and the polling
  layer; if the caller misses it, it is gone. Implementation: a
  per-direction monotonic delivery counter `Direction::events: u64`
  is incremented exclusively by `ReactorLock::react()` on actual
  delivery. `Source::poll_ready` snapshots `events` into
  `captured_events` and reports `Ready` iff the counter has advanced
  past the snapshot. The global reactor `tick` is irrelevant here.

The `WakeMode::Edge` variant is `#[cfg(windows)]`; the `Level` arms
of `check_ready` / `capture` / `Ready::poll` are the only ones
compiled on non-Windows. `Source::wake_mode()` returns `Level`
unconditionally on non-Windows so cross-platform code stays linear.

Why edge mode cannot collapse back to a tick comparison: the global
`tick` is bumped every reactor cycle. A `(tick != snapshot) || (Edge && tick == snapshot)`
predicate returns spurious `Ready` whenever an *unrelated* `react()`
cycle bumps the ticker without delivering our event — the caller then
takes `Ready`, registers no waker, and nothing wakes the actual
completion. The `events` counter advances only on real delivery, so
the inequality is conclusive. See the regression test
`reactor::ready_tests::edge_no_spurious_ready_when_tick_equals_capture`.

### 1.4 Lifecycle of a single overlapped op

A read on `NamedPipeStream` (the inherent owned-buffer path) is the
canonical example:

```text
                                                     ┌── kernel ──┐
poll #1 (caller's first .await)                      │            │
  ┌──────────────────────────────────────────────┐   │            │
  │ arm_source(&src, /*read=*/true, cx)          │   │            │
  │   → Source::poll_readable                    │   │            │
  │   → captured_events = Some(N)                │   │            │
  │   → register cx.waker() in source.read.waker │   │            │
  │ file.submit_read(buf)                        │──►│ ReadFile   │
  │   → Submission::Pending(op)                  │   │ overlapped │
  │ submitted = Some(OpHandleGuard(op))          │   │            │
  │ poll_op(op, &src, true, cx)                  │   │            │
  │   → op.is_complete() = false                 │   │            │
  │   → poll_readable → Pending (events == N)    │   │            │
  │ return Poll::Pending                         │   │            │
  └──────────────────────────────────────────────┘   │            │
                                                     │   ...      │
reactor thread: react()                              │            │
  ┌──────────────────────────────────────────────┐   │            │
  │ poller.wait()                                │◄──│ IOCP packet│
  │ for each delivered op:                       │   │ for op X   │
  │   state[read].events += 1     // N → N+1     │   │            │
  │   state[read].waker.wake()                   │   │            │
  └──────────────────────────────────────────────┘   │            │
                                                     │            │
poll #2 (woken)                                      │            │
  ┌──────────────────────────────────────────────┐   │            │
  │ submitted.is_some() → skip arm + submit      │   │            │
  │ poll_op(op, &src, true, cx)                  │   │            │
  │   → op.is_complete() = true                  │   │            │
  │   → return Ready(Ok(()))                     │   │            │
  │ guard.take().take()                          │   │            │
  │   → (bytes, buf)                             │   │            │
  │ buf.set_init(bytes); return Ready(Ok((..)))  │   │            │
  └──────────────────────────────────────────────┘   └────────────┘
```

The same shape is used by `NamedPipeStream::write`,
`NamedPipeListener::accept` (`submit_connect_named_pipe`), and the
`futures-io` impls (`AsyncRead` / `AsyncWrite`) — only the buffer type
and the post-completion bookkeeping differ.

### 1.5 The arm-before-submit invariant

> **Any path that calls `submit_*` outside of `Source::poll_*` must
> first call `poll_readable` / `poll_writable` to pin a snapshot of
> the per-direction `events` counter into `captured_events`.**

This is the single load-bearing rule of the IOCP integration. It is
encapsulated in the `arm_source(&Source, is_read, cx)` helper in
[src/os/windows.rs](../src/os/windows.rs) and called by:

- `NamedPipeListener::accept` before `submit_connect_named_pipe`.
- `NamedPipeStream::read` before `submit_read`.
- `NamedPipeStream::write` before `submit_write`.

The `futures-io` `AsyncRead` / `AsyncWrite` impls do not need to call
`arm_source` explicitly: they take their `Idle → Pending` transition
*inside* the `poll_*` callback, and the first thing they do in that
arm is call `poll_readable` / `poll_writable` themselves — which is
exactly what `arm_source` does.

#### Why this matters

If `submit_*` runs without an active `captured_events` snapshot, the
following race is observable and was the source of the
`owned_round_trip_byte_mode` hang fixed earlier on this branch:

```text
T0: submit_read(buf)   → Submission::Pending(op)   // events == N
T1: kernel completes ReadFile
T2: react()            → state[read].events = N+1
                       → no waker parked, nothing to wake
T3: poll #1 runs       → arm_source                // captured_events = Some(N+1)
T4: poll_op            → events == captured_events // Pending
T∞: nothing wakes us; the task hangs forever.
```

Arming before the submit pins `captured_events = Some(N)`, so the
delivery at T2 wakes the reactor's own `events` increment past `N`
and the next poll's snapshot inequality resolves `Ready` correctly,
regardless of whether the delivery races T0–T3 or arrives later.

### 1.6 `futures-io` staging buffers

`futures_io::{AsyncRead, AsyncWrite}` borrow the caller's slice for
only the duration of the `poll_*` call. IOCP needs the buffer to live
across the suspend point until the completion is dequeued. The
staging strategy is therefore:

- `DirState::Idle → DirState::Pending(OpHandleGuard<Vec<u8>>)`:
  on the first poll of a new op, copy the caller's bytes into an owned
  internal `Vec<u8>` (write) or allocate a sized internal `Vec<u8>`
  (read), submit the owned buffer, stash the `OpHandleGuard` in the
  per-direction slot.
- Subsequent polls call `poll_op`; on completion they extract
  `(bytes, buf)`, copy into the caller's slice (read) or simply discard
  (write), and reset the slot to `Idle`.

This is one extra copy per direction per `poll_*` cycle. The inherent
owned-buffer `read` / `write` / `compio`-style API avoids it entirely
by handing the caller's buffer straight to the kernel.

`poll_flush` runs `FlushFileBuffers` on the global blocking pool via
`blocking::unblock` and stores the resulting `Task` in
`FuturesIoState::flush` so repeated polls join the same in-flight
flush rather than launching a new one. After completion the `flush`
slot is cleared *and* the `write` slot is reset to `Idle`.

### 1.7 EOF translation

Windows surfaces peer-close in three OS error codes. They are
translated to `Ok(0)` (futures-io `read` EOF) inside the read paths:

| Win32 error | Meaning | Translation |
| --- | --- | --- |
| `ERROR_BROKEN_PIPE` | Peer closed pipe | `Ok(0)` |
| `ERROR_OPERATION_ABORTED` | Local cancel/close | `Ok(0)` |
| `ERROR_HANDLE_EOF` | End of file marker | `Ok(0)` |
| `ERROR_MORE_DATA` | Message-mode partial read | `Ok(bytes_transferred)` (not an error) |

Inherent `read` (returning `(usize, B)`) uses the same translation so
clients of either API observe the conventional EOF shape.

### 1.8 Cancellation and Drop

`OpHandleGuard<B>` wraps `Option<OpHandle<B>>`. On drop it issues a
best-effort `OpHandle::cancel()` to the kernel; the buffer remains
owned by the kernel until the OS dequeues the cancel completion, then
is released by `polling`. This means dropping a `read` / `write`
future mid-flight is sound: the caller never touches the buffer again,
the kernel writes (or doesn't) into memory it owns, and the reactor
collects the completion in the background.

`StreamInner::drop` calls `RegisteredFile::deactivate()` *before*
`Reactor::remove_io`, so no further submissions can win the race
against teardown.

---

## 2. Repository layout of the change

```
src/reactor.rs
  + WakeMode { Level, Edge#[cfg(windows)] }
  + Direction { events: u64, captured_events: Option<u64>, .. }
  + Source::check_ready / Source::capture / Source::wake_mode
  + Reactor::insert_io_edge

src/reactor/windows.rs
  + Registration::File(Arc<RegisteredFile>)
  + Registration::add: file → set_user_key(token)
  + Registration::delete: file → deactivate()
  + Registration::modify: file → no-op

src/os/windows.rs
  + OpHandleGuard<B>            (cancel-on-drop)
  + poll_op<B>(op, source, is_read, cx)
  + arm_source(source, is_read, cx)
  + NamedPipeListener           (with AtomicBool first-instance latch)
  + NamedPipeStream             (Arc<StreamInner>; inherent + futures-io)
  + DirState / FuturesIoState
  + AsyncRead / AsyncWrite
  + translate_eof_error / map_read_error

src/os/windows/named_pipe.rs
  + PipeMode, PipeAccess
  + NamedPipeOpenOptions  (#[non_exhaustive] + getters)
  + NamedPipeConnectOptions (#[non_exhaustive] + getters)
  + NamedPipeListenerBuilder

Cargo.toml
  + polling = git { branch = "kail/windows_file" }   (fork)
  - bitflags                                         (no longer needed)
  - compio-io / compio-buf                           (removed; see §6)
```

---

## 3. Test inventory

[tests/windows_named_pipe.rs](../tests/windows_named_pipe.rs) — 12
integration tests, all run in a single thread (`--test-threads=1` is
not required but the suite completes under 0.5 s either way):

| # | Test | What it asserts |
| --- | --- | --- |
| 1 | `owned_round_trip_byte_mode` | Inherent `read` / `write` round-trip on a byte-mode pipe. The original hang regression. |
| 2 | `futures_io_round_trip` | `AsyncReadExt` / `AsyncWriteExt` round-trip including `flush`. |
| 3 | `peer_close_eof_futures_io` | Dropping the client surfaces `Ok(0)` EOF on the server's `AsyncRead::read`. |
| 4 | `peek_does_not_consume` | `PeekNamedPipe` returns ≥ 1 byte and a subsequent `read` still returns the full payload. |
| 5 | `try_from_raw_handle_wraps_client` | Foreign overlapped handles can be wrapped via `unsafe { try_from_raw_handle }`. |
| 6 | `try_from_owned_handle` | `TryFrom<OwnedHandle> for NamedPipeStream`. |
| 7 | `connect_with_options_inbound_server` | Server `PipeAccess::Outbound` + client `read=true, write=false` round-trip. |
| 8 | `as_raw_handle_is_valid` | Listener clone + drop smoke test. |
| 9 | `connect_fails_when_no_server` | `connect` to a name with no listener fails synchronously with `ERROR_FILE_NOT_FOUND`. |
| 10 | `first_pipe_instance_locks_name` | Two listeners on the same name; second `accept` fails `ERROR_ACCESS_DENIED` while the first instance is still alive. |
| 11 | `large_data_round_trip_eof_on_drop` | Client writes ~120 KB × 2 with `write_all + flush`, drops; server reads in a loop until `Ok(0)` and asserts the full byte total. Exercises multiple `submit_write` per direction and the `poll_flush` cleanup. |
| 12 | `message_mode_round_trip` | `PipeMode::Message` listener; two distinct messages arrive as two separate `read` results of the correct sizes. |

Naming uses `unique_name(stem)` — `pid + atomic counter` — so
parallel test invocations cannot collide on the pipe namespace.

`block_on` and `Timer` come from `async_io` itself; the brief
`Timer::after(Duration::from_millis(10))` before each client
`connect` makes the server-listening side of the race deterministic
without polling.

---

## 4. Known follow-ups

- Two `dead_code` warnings remain (`Source::registration`,
  `Registration::registered_file`). Both are accessors kept for
  symmetry with the socket / handle variants and intended for the
  next iteration that will expose mailslot / device-file IOCP
  sources. Decide whether to silence with `#[allow(dead_code)]` or
  delete and reintroduce when the second user lands.
- Vectored I/O (`AsyncRead::poll_read_vectored` /
  `AsyncWrite::poll_write_vectored`) falls back to the trait-default
  scalar implementations.
- `NamedPipeListener::incoming` (a `Stream` of accepted connections)
  is not exposed — callers must loop over `accept()` directly.

---

## 5. Cross-references

- `arm_source` doc-comment — the canonical statement of the
  arm-before-submit invariant
  ([src/os/windows.rs](../src/os/windows.rs)).
- `poll_op` doc-comment — explains the `is_complete → poll_readable`
  loop.
- `Source::check_ready` — the wake-mode predicate
  ([src/reactor.rs](../src/reactor.rs)).
- `reactor::ready_tests::edge_no_spurious_ready_when_tick_equals_capture`
  — regression for the `Edge`-mode aliasing bug that motivated the
  per-direction `events` counter.

---

## 6. Proposal: optional `compio` trait support

> **Status:** design only — not yet implemented. Pending review.

### 6.1 Motivation

The futures-io `AsyncRead` / `AsyncWrite` impls borrow the caller's
slice for the duration of `poll_*`. IOCP requires the buffer to live
across the suspend point until the completion is dequeued, so the
current impl stages every read/write through an internal `Vec<u8>`
(see §1.6). That is one extra heap copy per direction per op — a
real cost for high-throughput callers and the price the futures-io
trait family charges for being readiness-shaped.

The [`compio-io`](https://docs.rs/compio-io) trait family is designed
for completion-mode I/O. Its read/write methods take **owned**
buffers and hand them back in the result:

```rust
pub trait AsyncRead {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B>;
}
pub trait AsyncWrite {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B>;
    async fn flush(&mut self) -> io::Result<()>;
    async fn shutdown(&mut self) -> io::Result<()>;
}
pub struct BufResult<T, B>(pub io::Result<T>, pub B);
```

This matches the IOCP kernel contract natively: the caller's buffer
goes straight to `submit_read` / `submit_write` and comes back from
the completion with no intermediate allocation.

`NamedPipeStream` already exposes the same shape via the inherent
`read<B: StableBufMut>` / `write<B: StableBuf>` methods. The compio
impls are therefore a **thin trait adapter over existing inherent
methods**, not a new I/O path.

### 6.2 Cargo feature

```toml
[features]
default = []
# Enables the `compio_io::AsyncRead` / `AsyncWrite` impls on
# `NamedPipeStream` and re-exports `compio-io` / `compio-buf`.
compio = ["dep:compio-io", "dep:compio-buf"]

[dependencies]
compio-io  = { version = "0.x", optional = true }
compio-buf = { version = "0.x", optional = true }
```

The feature is **off by default** so consumers that do not need
named-pipe ownership-style I/O do not pull in the compio crates or
their transitive dependencies. Pin both to the minor version
compio-io is published with — they are released together.

We depend only on `compio-io` (the trait crate) and `compio-buf`
(the `IoBuf` / `IoBufMut` trait crate). We **do not** depend on
`compio-runtime`, `compio-driver`, or any other compio runtime
machinery — async-io's own reactor (`Reactor` + `polling::Poller`)
remains the sole I/O driver. This is what allows compio-style trait
calls to work inside a `block_on` from `async-io`, `smol`, or any
other futures-runtime; we are using compio's *trait vocabulary*, not
its runtime.

### 6.3 Module layout

```
src/os/windows.rs
  pub use compio::{AsyncRead, AsyncWrite};   // gated, see below

src/os/windows/compio.rs                     // new; #[cfg(feature = "compio")]
  - StableBufWrap<B>      (newtype adapter, see §7.4)
  - impl compio_io::AsyncRead  for &NamedPipeStream
  - impl compio_io::AsyncRead  for NamedPipeStream
  - impl compio_io::AsyncWrite for &NamedPipeStream
  - impl compio_io::AsyncWrite for NamedPipeStream

  pub use compio_io::{AsyncRead, AsyncWrite};
  pub use compio_buf::{IoBuf, IoBufMut, BufResult};
```

The whole submodule is `#[cfg(feature = "compio")]`. In
`src/os/windows.rs`:

```rust
#[cfg(feature = "compio")]
pub mod compio;   // re-exports compio_io / compio_buf for callers
```

Consumers enable the feature and use:

```rust
use async_io::os::windows::compio::{AsyncRead, AsyncWrite, BufResult};

let BufResult(res, buf) = AsyncRead::read(&stream, Vec::with_capacity(64)).await;
let n = res?;
```

### 6.4 The `IoBuf` ↔ `StableBuf` adapter

`polling::os::iocp::StableBuf` and `compio_buf::IoBuf` express the
same safety contract — "the pointer is stable across moves of the
wrapper, the kernel may write to it across the suspend point" — with
slightly different method names:

| polling `StableBuf` | compio `IoBuf` |
| --- | --- |
| `fn as_ptr(&self) -> *const u8` | `fn as_buf_ptr(&self) -> *const u8` |
| `fn len(&self) -> usize` | `fn buf_len(&self) -> usize` |
| (none) | `fn buf_capacity(&self) -> usize` |

`StableBufMut` adds `as_mut_ptr` / `capacity` / `set_init`, mirroring
`IoBufMut`'s `as_buf_mut_ptr` / `buf_capacity` / `set_buf_init`.

A single newtype bridges them:

```rust
struct StableBufWrap<B>(B);

unsafe impl<B: compio_buf::IoBuf + Send + 'static> polling::os::iocp::StableBuf
    for StableBufWrap<B>
{
    fn as_ptr(&self) -> *const u8 { self.0.as_buf_ptr() }
    fn len(&self) -> usize        { self.0.buf_len() }
}

unsafe impl<B: compio_buf::IoBufMut + Send + 'static> polling::os::iocp::StableBufMut
    for StableBufWrap<B>
{
    fn as_mut_ptr(&mut self) -> *mut u8 { self.0.as_buf_mut_ptr() }
    fn capacity(&self) -> usize         { self.0.buf_capacity() }
    unsafe fn set_init(&mut self, n: usize) { self.0.set_buf_init(n) }
}
```

Both trait families are `unsafe`-to-implement on the producer side
and document the same invariants, so the wrap is a no-op SAFETY-wise:
we are forwarding identical guarantees under different method names.

The adapter is private; callers only see compio buffer types.

### 6.5 Trait impl shape

```rust
impl<B: IoBufMut + Send + 'static> compio_io::AsyncRead for &NamedPipeStream {
    async fn read(&mut self, buf: B) -> BufResult<usize, B> {
        match NamedPipeStream::read(self, StableBufWrap(buf)).await {
            Ok((n, StableBufWrap(buf))) => BufResult(Ok(n), buf),
            Err(e)                      => BufResult(Err(e), /* recovered buf */),
        }
    }
}
```

There is one open contract question: compio requires the buffer to
be returned even on error. The current inherent `read` /
`write` signatures are `io::Result<(usize, B)>`, which loses the
buffer on error.

Two options, in order of preference:

1. **Change the inherent signature to `(io::Result<usize>, B)`.**
   This is the same shape compio already uses (`BufResult`) and
   matches what `polling::Submission::Failed { error, buf }` already
   produces internally. Inherent callers get a strictly more
   informative result and the trait impl is a one-liner. This is a
   breaking change to the inherent API — acceptable while the named
   pipe code is unreleased on this branch.

2. **Keep `io::Result<(usize, B)>` and `Drop` the buffer on error in
   the trait impl, returning `BufResult(Err(e), B::default())` if
   `B: Default`** — only works for buffer types with a sensible
   default (`Vec`, `Box<[u8]>`). Worse ergonomics, narrower trait
   bounds, and silently drops the user's allocation.

**Decision: option 1 — accepted.** Change inherent `read` / `write` to
`(io::Result<usize>, B)` *before* shipping the compio feature so the
buffer is always returned to the caller and the wire format never has
to change. This is a breaking change to the inherent API; acceptable
because the named-pipe code is unreleased on this branch.

After the change, `Submission::Failed { error, buf }` from `polling`
flows straight through: the inherent method returns `(Err(error), buf)`
and the compio impl is a one-line `BufResult(res, buf)` re-wrap.

### 6.6 `flush` and `shutdown`

`compio_io::AsyncWrite::flush` and `shutdown` are `async fn ... ->
io::Result<()>`. Mapping:

#### `flush`

Call the same `FlushFileBuffers`-on-the-blocking-pool path that
`futures_io::AsyncWrite::poll_flush` already drives. Factor the
staging out of `poll_flush` into an inherent `async fn flush(&self)
-> io::Result<()>` that both trait impls call. The inherent method
does not need the `FuturesIoState::flush` slot used by `poll_flush`
(an async fn keeps its `unblock` task alive in its own stack frame),
so it is a few lines:

```rust
impl NamedPipeStream {
    pub async fn flush(&self) -> io::Result<()> {
        let h = self.inner.handle.try_clone()?;   // or share via Arc
        unblock(move || flush_file_buffers(&h)).await
    }
}
```

#### `shutdown` — flush, do **not** deactivate

`shutdown` is implemented as:

```rust
async fn shutdown(&mut self) -> io::Result<()> {
    self.flush().await
}
```

It is intentionally **not** a no-op (we drain in-flight kernel
buffers so callers that `write_all` then `shutdown` get the same
ordering guarantee they would on a TCP socket), and it is
intentionally **not** a teardown:

- **No `RegisteredFile::deactivate()`.** `RegisteredFile` is shared
  across `NamedPipeStream` clones via `Arc<StreamInner>`. Calling
  `deactivate()` is a process-wide kill switch for that
  registration: it stops *all* future submissions on every clone
  and is not reversible. Doing it from `shutdown` would invalidate
  reads happening concurrently on a clone in another task. The
  correct place for `deactivate()` is `StreamInner::drop`, which is
  where it already lives — it runs exactly once, when the *last*
  `Arc<StreamInner>` is released.

- **No `DisconnectNamedPipe`.** The Win32 half-close-equivalent for
  named pipes is server-side and tears down the whole connection
  synchronously, invalidating both directions on every shared
  handle. This is the wrong granularity for an `AsyncWrite`
  trait method whose contract is "no more writes from this end".

- **No handle close.** The OS handle is owned by `OwnedHandle`
  inside `StreamInner` and is closed by its `Drop` when the last
  `Arc` is released. `shutdown` returning does *not* imply the
  handle is gone — a still-live clone keeps reading and writing
  fine, which matches what compio's contract requires ("no more
  writes from *this* writer").

In other words: `shutdown` is a "please drain" boundary, not a
"please tear down" boundary. The existing `Drop` chain on
`StreamInner` (deactivate → `Reactor::remove_io` → `OwnedHandle` close)
remains the single canonical teardown path. See §1.8.

### 6.7 Method-resolution shadowing

Inherent methods win over trait methods on dot-call. Calling
`stream.read(buf).await` will hit the inherent `read`, *not*
`compio_io::AsyncRead::read`, even with the trait imported.

Both methods produce the same buffer shape, so the practical
difference is small, but it is worth documenting:

```rust
// Inherent (always available):
let res = stream.read(buf).await;

// compio trait (with `compio` feature):
use async_io::os::windows::compio::AsyncRead;
let BufResult(res, buf) = AsyncRead::read(&stream, buf).await;
```

If we adopt §6.5 option 1 (inherent returns `(io::Result<usize>, B)`),
the inherent and trait return types become trivially convertible and
the shadowing is harmless.

### 6.8 Test plan

Add `tests/windows_named_pipe_compio.rs`, gated on
`#[cfg(all(windows, feature = "compio"))]`, mirroring the existing
suite where the data path differs:

| Test | Asserts |
| --- | --- |
| `compio_round_trip` | `Vec<u8>` round-trip via `AsyncRead::read` / `AsyncWrite::write`. |
| `compio_eof` | Peer drop surfaces `Ok(0)` from `AsyncRead::read`. |
| `compio_set_buf_init` | Reads of `Vec<u8>` with `with_capacity(n)` come back with `buf.len() == bytes_read` (validates the `set_buf_init` forwarding). |
| `compio_buffer_returned_on_error` | Disconnect mid-op; the returned `BufResult` carries back the caller's allocation rather than dropping it. |
| `compio_flush_shutdown` | `flush().await.is_ok()` and `shutdown().await.is_ok()` on a live stream. |

CI: add a Windows job entry running
`cargo test --target x86_64-pc-windows-msvc --features compio`. The
default-feature job continues to run without the feature so we
catch any cross-feature regressions in either direction.

### 6.9 Future Linux io_uring support

This is the strategic motivation for routing the compio impls
through the inherent owned-buffer methods rather than implementing
the traits directly against `polling::submit_read` / `submit_write`.

#### Today

| OS | Reactor backend | Mode | Source kinds compio could wrap |
| --- | --- | --- | --- |
| Windows | `polling::Poller` (IOCP) | Mixed: AFD readiness for sockets, completion for files (`RegisteredFile`) | `NamedPipeStream` (this proposal) |
| Linux | `polling::Poller` (epoll) | Readiness only | None — no completion-mode source |
| macOS / BSD | `polling::Poller` (kqueue) | Readiness only | None |

On non-Windows targets the `compio` feature has no impls to offer
because there is no completion-mode source kind. **Decision: make
the `compio` feature Windows-only.**

Concretely:

- The feature is declared with no platform gate in `Cargo.toml`
  (cargo features cannot be `cfg`-gated), but every item the
  feature touches — the `compio` module, the trait impls, the
  re-exports — is gated `#[cfg(all(windows, feature = "compio"))]`.
- Enabling `--features compio` on Linux/macOS compiles cleanly but
  produces no new public items. We do not re-export the compio
  crates on non-Windows; consumers that want compio traits on those
  platforms should depend on `compio-io` directly.
- The CI matrix runs `--features compio` only on the Windows job.

#### When `polling` eventually adds an io_uring completion API

**Decision: defer the platform-shared helper extraction until
io_uring actually lands.** `arm_source` and `poll_op` stay in
`src/os/windows.rs` for now. Moving them preemptively to a neutral
`src/iocp_like.rs` would commit to an API shape before we have a
second consumer to validate it against.

If/when `polling`'s Linux backend grows a completion-mode submission
API analogous to today's IOCP file API (the kail/windows_file branch
shape: `RegisteredFd` + `submit_*` returning `Submission<B>` +
`OpHandle<B>`), the porting work in this crate is essentially
mechanical:

1. Add `Registration::Fd(Arc<RegisteredFd>)` in
   `src/reactor/unix.rs`, mirroring
   `Registration::File(Arc<RegisteredFile>)` on Windows.
2. Reactor `react()` for io_uring CQEs bumps the same per-direction
   `Direction::events` counter — no new wake mode, no new state.
3. At that point lift `arm_source` / `poll_op` from
   `src/os/windows.rs` into a target-shared module (the helpers
   themselves are platform-agnostic; they only ever touch `Source`
   and `OpHandle`). The signatures stay identical.
4. Add a Linux equivalent of `NamedPipeStream` for whichever fds we
   want to expose (anonymous pipes, regular files, etc.) with the
   same inherent `read<B: StableBufMut>` / `write<B: StableBuf>`
   methods.
5. The compio `AsyncRead` / `AsyncWrite` impls in the new Linux
   module are the same one-line adapters as on Windows — they wrap
   the caller's `IoBuf` in `StableBufWrap` and call the inherent
   methods.

The buffer adapter (§6.4) is platform-agnostic. When the second
platform lands it moves out of `src/os/windows/compio.rs` into a
shared module used by both Windows and Linux trait impls; until
then it stays alongside its only user.

#### Design properties this preserves

- **One reactor.** The compio trait impls do not introduce a second
  driver; they ride the same `polling::Poller` already running. A
  user combining named-pipe I/O with TCP sockets and timers in the
  same task gets one IOCP/epoll/io_uring instance, not two.
- **Same wake semantics.** `WakeMode::Edge` and the `events`
  counter were designed for IOCP completions; io_uring CQEs are the
  same delivery shape ("one packet per op, no level-readiness
  re-assertion"), so the existing reactor primitive serves both.
- **Optional.** Users that do not care about io_uring or compio
  pay nothing — the feature is off by default and the platform
  gates avoid pulling the trait crates onto unsupported targets.

#### What this proposal explicitly does *not* commit to

- **Embedding `compio-driver` or `compio-runtime`.** Out of scope
  forever; that would mean async-io hosts two reactors, which
  defeats the point of the crate.
- **Implementing compio's `IoBuf` for `polling`'s buffer types** (or
  vice versa). Each crate keeps its own buffer trait; the adapter
  newtype bridges them at the call site.
- **Vectored I/O** (`read_vectored` / `write_vectored`). Compio's
  default trait impls fall back to scalar; we accept the fallback
  for Phase 1.

### 6.10 Implementation status (delivered)

Phase 1 landed on branch `kail/windows_named_pipe`:

- `polling` updated to commit `c7abfb98` on
  `klu-dev/polling#kail/windows_file`. `OpHandle::take` now returns
  `(io::Result<usize>, B)` so the buffer survives the async error
  path (previously `io::Result<(usize, B)>`, which dropped `B` on
  `Err`). The inherent `NamedPipeStream::read` / `write` collapse
  to a one-line repackage of that tuple.
- Inherent `read` applies `map_read_error` (BROKEN_PIPE /
  OPERATION_ABORTED / HANDLE_EOF → `Ok(0)`) so EOF semantics are
  identical across the inherent, futures-io, and compio surfaces.
- An inherent `pub async fn flush(&self) -> io::Result<()>`
  (implemented as `unblock(FlushFileBuffers)`) backs both the
  futures-io `poll_flush` path and the compio `AsyncWrite::flush` /
  `shutdown` impls.
- `src/os/windows/compio.rs` defines two private wrappers,
  `WriteBufWrap<B>(Box<B>)` and `ReadBufWrap<B>(Box<B>)`, which
  forward `IoBuf` / `IoBufMut` to `polling::os::iocp::StableBuf` /
  `StableBufMut`. The `Box` is required because polling's `OpInner`
  buffer slot is dimensioned at 32 bytes (`ErasedBuf = [u8; 32]`)
  and several compio buffer types (`BytesMut`, `SmallVec<[u8; N]>`)
  exceed that inline; the box keeps every wrapper a single pointer
  wide.
- `Send` is grafted onto the wrappers via `unsafe impl` because
  `IoBuf`/`IoBufMut` do not require it (compio's runtime is
  single-threaded). Sound for every byte-buffer impl shipped by
  compio-buf 0.8; documented in the module preamble.
- Trait impls live on `&NamedPipeStream` and `NamedPipeStream`. The
  `&mut` impl is provided automatically by compio's blanket
  `impl<A: AsyncRead + ?Sized> AsyncRead for &mut A` (and the
  matching `AsyncWrite` blanket); writing our own `&mut` impl
  conflicts with the blanket.
- All five tests in §6.8 pass on `x86_64-pc-windows-msvc`
  (`tests/windows_named_pipe_compio.rs`), alongside the original 12
  inherent / futures-io tests.

---

## 7. Historical context

The previous design (over `polling::os::iocp::{read_file_overlapped,
write_file_overlapped, AsFileHandle, FileOverlappedWrapper}`) and its
review history are preserved in
[`named-pipe.design.md.bak`](./named-pipe.design.md.bak). The
following items called out in that document are resolved by the
current implementation:

- `Source::wake_mode` field gated to `#[cfg(windows)]`; `WakeMode::Edge`
  variant gated; non-Windows reads go through `Source::wake_mode()`
  returning `Level`.
- `PipeMode` is a 3-variant enum; invalid `BYTE | READ_MODE_MESSAGE`
  combinations are unrepresentable; the `bitflags` dependency was
  dropped.
- `NamedPipeOpenOptions::open_mode(u32)` replaced with
  `access(PipeAccess)`. `FILE_FLAG_OVERLAPPED` is OR'd in internally
  on both server and client paths.
- `NamedPipeConnectOptions` exposes typed client-side options.
- Both options structs are `#[non_exhaustive]` with public getters.
- `NamedPipeListener::builder(addr).bind()` replaces the
  three-positional-argument `bind`.
- Edge-mode aliasing race fixed by replacing the `tick`-based
  predicate with the per-direction `events` counter.
- `ERROR_BROKEN_PIPE` / `ERROR_OPERATION_ABORTED` / `ERROR_HANDLE_EOF`
  surface as `Ok(0)` on the read path; `ERROR_MORE_DATA` surfaces as
  a partial-read `Ok(n)`.
- `poll_close` resolves to `Ok(())` after `poll_flush`.
- All `println!` debug noise was removed from the production paths.
- `NamedPipeListener::accepted` is an `AtomicBool` shared across
  clones; the `OnceLock<bool>` first-accept latch is gone.

The `compio-io` / `compio-buf` integration described in §1.6 of the
old doc was **removed** during the rewrite onto `polling`'s
completion-mode file API: the inherent `read<B: StableBufMut>` /
`write<B: StableBuf>` methods now provide the same zero-extra-copy
ownership-style API natively, without a separate trait family or a
cargo feature.
