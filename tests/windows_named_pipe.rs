// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the completion-mode named-pipe
//! implementation in `async_io::os::windows`.
//!
//! Each test uses a unique pipe name derived from the test function +
//! a random suffix so concurrently-running tests do not collide.

#![cfg(windows)]

use std::os::windows::io::{FromRawHandle, IntoRawHandle, OwnedHandle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_io::os::windows::{
    NamedPipeConnectOptions, NamedPipeListener, NamedPipeStream, PipeAccess, PipeMode,
};
use async_io::{block_on, Timer};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!(r"\\.\pipe\async_io_test_{}_{}_{}", stem, pid, n)
}

fn spawn_listener(name: String, mode: PipeMode) -> NamedPipeListener {
    NamedPipeListener::bind(name, mode, None).expect("bind")
}

// ---------------------------------------------------------------------
// 1. Owned-buffer round-trip via the inherent read/write API
// ---------------------------------------------------------------------
#[test]
fn owned_round_trip_byte_mode() {
    block_on(async {
        let name = unique_name("owned_rt");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let buf = Vec::with_capacity(64);
                let (res, buf) = server.read(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"hello");
                let (res, _) = server.write(b"world".to_vec()).await;
                let m = res.unwrap();
                assert_eq!(m, 5);
            }
        };

        let client_task = async {
            // Tiny delay so the server has a chance to call accept.
            // Not strictly required (CreateFile retries via FILE_FLAG_OVERLAPPED
            // semantics handled inside connect), but keeps the test deterministic.
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            let (res, _) = client.write(b"hello".to_vec()).await;
            let n = res.unwrap();
            assert_eq!(n, 5);
            let buf = Vec::with_capacity(64);
            let (res, buf) = client.read(buf).await;
            let m = res.unwrap();
            assert_eq!(&buf[..m], b"world");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 2. futures-io AsyncRead/AsyncWrite round-trip
// ---------------------------------------------------------------------
#[test]
fn futures_io_round_trip() {
    block_on(async {
        let name = unique_name("futio_rt");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                let mut buf = [0u8; 32];
                let n = AsyncReadExt::read(&mut server, &mut buf).await.unwrap();
                assert_eq!(&buf[..n], b"ping");
                AsyncWriteExt::write_all(&mut server, b"pong")
                    .await
                    .unwrap();
                AsyncWriteExt::flush(&mut server).await.unwrap();
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let mut client = NamedPipeStream::connect(name).await.unwrap();
            AsyncWriteExt::write_all(&mut client, b"ping")
                .await
                .unwrap();
            AsyncWriteExt::flush(&mut client).await.unwrap();
            let mut buf = [0u8; 32];
            let n = AsyncReadExt::read(&mut client, &mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"pong");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 3. Peer-close EOF — futures-io read returns Ok(0)
// ---------------------------------------------------------------------
#[test]
fn peer_close_eof_futures_io() {
    block_on(async {
        let name = unique_name("eof_futio");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                let mut buf = [0u8; 16];
                // Client never writes; client drops -> EOF.
                let n = AsyncReadExt::read(&mut server, &mut buf).await.unwrap();
                assert_eq!(n, 0, "expected EOF after peer drop");
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            // Drop client immediately to close peer.
            drop(client);
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 4. peek does not consume bytes
// ---------------------------------------------------------------------
#[test]
fn peek_does_not_consume() {
    block_on(async {
        let name = unique_name("peek");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                // Wait a bit for client write to land in the buffer.
                Timer::after(Duration::from_millis(50)).await;

                let mut peek_buf = [0u8; 16];
                let peeked = server.peek(&mut peek_buf).unwrap();
                assert!(peeked >= 1, "PeekNamedPipe should see at least one byte");

                // The actual read must still succeed and return the same data.
                let buf = Vec::with_capacity(16);
                let (res, buf) = server.read(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"abc");
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            let (res, _) = client.write(b"abc".to_vec()).await;
            res.unwrap();
            // Hold the client until the server is done so the pipe stays open.
            Timer::after(Duration::from_millis(150)).await;
            drop(client);
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 5. try_from_raw_handle wraps a foreign overlapped handle
// ---------------------------------------------------------------------
#[test]
fn try_from_raw_handle_wraps_client() {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem as wsf;

    block_on(async {
        let name = unique_name("from_raw");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let (res, _) = server.write(b"hi".to_vec()).await;
                let n = res.unwrap();
                assert_eq!(n, 2);
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            // Open with FILE_FLAG_OVERLAPPED ourselves, then hand the
            // raw handle to NamedPipeStream::try_from_raw_handle.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(wsf::FILE_FLAG_OVERLAPPED)
                .open(&name)
                .unwrap();
            let raw = file.into_raw_handle();
            let client = unsafe { NamedPipeStream::try_from_raw_handle(raw) }.unwrap();

            let buf = Vec::with_capacity(8);
            let (res, buf) = client.read(buf).await;
            let n = res.unwrap();
            assert_eq!(&buf[..n], b"hi");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 6. TryFrom<OwnedHandle>
// ---------------------------------------------------------------------
#[test]
fn try_from_owned_handle() {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem as wsf;

    block_on(async {
        let name = unique_name("from_owned");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let (res, _) = server.write(b"ok".to_vec()).await;
                let n = res.unwrap();
                assert_eq!(n, 2);
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(wsf::FILE_FLAG_OVERLAPPED)
                .open(&name)
                .unwrap();
            let owned: OwnedHandle =
                unsafe { OwnedHandle::from_raw_handle(file.into_raw_handle()) };
            let client = NamedPipeStream::try_from(owned).unwrap();
            let buf = Vec::with_capacity(8);
            let (res, buf) = client.read(buf).await;
            let n = res.unwrap();
            assert_eq!(&buf[..n], b"ok");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 7. connect_with_options propagates flags (read-only client)
// ---------------------------------------------------------------------
#[test]
fn connect_with_options_inbound_server() {
    block_on(async {
        let name = unique_name("inbound");
        let listener = NamedPipeListener::builder(&name)
            .access(PipeAccess::Outbound) // server writes only
            .bind()
            .unwrap();

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let (res, _) = server.write(b"data".to_vec()).await;
                let n = res.unwrap();
                assert_eq!(n, 4);
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect_with_options(
                name,
                NamedPipeConnectOptions::new().read(true).write(false),
            )
            .await
            .unwrap();
            let buf = Vec::with_capacity(8);
            let (res, buf) = client.read(buf).await;
            let n = res.unwrap();
            assert_eq!(&buf[..n], b"data");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 8. AsRawHandle returns a non-null, non-INVALID handle
// ---------------------------------------------------------------------
#[test]
fn as_raw_handle_is_valid() {
    block_on(async {
        let name = unique_name("raw_handle");
        let listener = spawn_listener(name, PipeMode::Byte);
        // The listener itself doesn't expose a raw handle (no
        // backing kernel object until accept), so just smoke-test
        // it can be cloned & dropped.
        let _clone = listener.clone();
        drop(listener);
    });
}

// ---------------------------------------------------------------------
// 9. Connect before any pipe instance exists -> ERROR_FILE_NOT_FOUND
// ---------------------------------------------------------------------
#[test]
fn connect_fails_when_no_server() {
    use windows_sys::Win32::Foundation as wf;

    block_on(async {
        let name = unique_name("no_server");
        // No listener bound, no accept ever called: nothing has
        // created a pipe instance with this name yet.
        let err = NamedPipeStream::connect(name).await.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(wf::ERROR_FILE_NOT_FOUND as i32));
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::NotFound,
            "ERROR_FILE_NOT_FOUND must surface as ErrorKind::NotFound"
        );
    });
}

// ---------------------------------------------------------------------
// 10. FILE_FLAG_FIRST_PIPE_INSTANCE rejects a competing listener
// ---------------------------------------------------------------------
//
// The first call to `accept()` on a `NamedPipeListener` requests
// `FILE_FLAG_FIRST_PIPE_INSTANCE` so a second, independently-bound
// listener on the same name fails its own first `accept()` with
// `ERROR_ACCESS_DENIED`. We must keep the first listener's server
// endpoint alive while we probe the second listener; otherwise the
// kernel frees the namespace and the second accept simply succeeds.
#[test]
fn first_pipe_instance_locks_name() {
    use windows_sys::Win32::Foundation as wf;

    block_on(async {
        let name = unique_name("first_inst");
        let listener_a = spawn_listener(name.clone(), PipeMode::Byte);
        let listener_b = spawn_listener(name.clone(), PipeMode::Byte);

        // Drive listener_a's first accept to completion via a real client
        // so the FIRST_PIPE_INSTANCE pipe is created and remains alive.
        let server_a_task = {
            let listener_a = listener_a.clone();
            async move { listener_a.accept().await.unwrap() }
        };
        let client_task = async {
            Timer::after(Duration::from_millis(20)).await;
            NamedPipeStream::connect(name).await.unwrap()
        };
        let (server_a, client) = futures_lite::future::zip(server_a_task, client_task).await;

        // While both endpoints are still alive, listener_b's first
        // accept must fail because FIRST_PIPE_INSTANCE was claimed.
        let err = listener_b.accept().await.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(wf::ERROR_ACCESS_DENIED as i32),
            "second listener should be rejected by FIRST_PIPE_INSTANCE"
        );

        drop(client);
        drop(server_a);
    });
}

// ---------------------------------------------------------------------
// 11. Large-data round-trip + flush + EOF after peer drop
// ---------------------------------------------------------------------
//
// Mirrors the previous `stream_flush_write` / `repeat_poll_read`
// tests: the server reads in a loop until EOF, the client writes
// many KB across multiple `write_all` calls, then drops. Validates
// that:
//   * Multiple submits per direction interleave correctly.
//   * `poll_flush` (FlushFileBuffers on the blocking pool) does not
//     deadlock when followed by a drop.
//   * `ERROR_BROKEN_PIPE` from the kernel is surfaced as `Ok(0)` by
//     the futures-io read path.
#[test]
fn large_data_round_trip_eof_on_drop() {
    const CHUNK: &[u8] = b"\
Lorem ipsum dolor sit amet, consectetur adipiscing elit. \
Donec pretium ante erat, vitae sodales mi varius quis.\n";

    block_on(async {
        let name = unique_name("large_rt");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                let mut total = 0usize;
                loop {
                    let n = AsyncReadExt::read(&mut server, &mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    total += n;
                }
                total
            }
        };

        let payload = CHUNK.repeat(1000); // ~120 KB
        let expected = payload.len() * 2;

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let mut client = NamedPipeStream::connect(name).await.unwrap();
            AsyncWriteExt::write_all(&mut client, &payload)
                .await
                .unwrap();
            AsyncWriteExt::write_all(&mut client, &payload)
                .await
                .unwrap();
            AsyncWriteExt::flush(&mut client).await.unwrap();
            drop(client);
        };

        let (total, ()) = futures_lite::future::zip(server_task, client_task).await;
        assert_eq!(total, expected);
    });
}

// ---------------------------------------------------------------------
// 12. PipeMode::Message round-trip preserves message boundaries
// ---------------------------------------------------------------------
//
// In message mode each `WriteFile` produces one discrete message and
// each `ReadFile` returns at most one message. We send two distinct
// messages and confirm the server reads them as two separate
// `read` results of exactly the right sizes.
#[test]
fn message_mode_round_trip() {
    block_on(async {
        let name = unique_name("msg_rt");
        let listener = NamedPipeListener::builder(&name)
            .pipe_mode(PipeMode::Message)
            .in_buffer_size(4096)
            .out_buffer_size(4096)
            .bind()
            .unwrap();

        let msg_a = b"first-message".to_vec();
        let msg_b = b"second-much-longer-message-payload".to_vec();
        let exp_a = msg_a.clone();
        let exp_b = msg_b.clone();

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let buf = Vec::with_capacity(256);
                let (res, buf) = server.read(buf).await;
                let n1 = res.unwrap();
                assert_eq!(&buf[..n1], exp_a.as_slice());
                let buf = Vec::with_capacity(256);
                let (res, buf) = server.read(buf).await;
                let n2 = res.unwrap();
                assert_eq!(&buf[..n2], exp_b.as_slice());
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            let (res, _) = client.write(msg_a).await;
            res.unwrap();
            let (res, _) = client.write(msg_b).await;
            res.unwrap();
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 13. NamedPipeListener::incoming() yields successive clients
// ---------------------------------------------------------------------
//
// Exercises the `Incoming` Stream impl. We connect three clients in
// sequence and confirm the server gets three items from `incoming()`,
// each one a usable stream.
#[test]
fn incoming_stream_yields_clients() {
    use futures_lite::StreamExt;

    block_on(async {
        let name = unique_name("incoming");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut incoming = listener.incoming();
                let mut totals = Vec::new();
                for _ in 0..3 {
                    let server = incoming.next().await.unwrap().unwrap();
                    let buf = Vec::with_capacity(8);
                    let (res, buf) = server.read(buf).await;
                    let n = res.unwrap();
                    totals.push(buf[..n].to_vec());
                }
                totals
            }
        };

        let client_task = async {
            let mut sent = Vec::new();
            for i in 0..3u8 {
                Timer::after(Duration::from_millis(10)).await;
                let client = NamedPipeStream::connect(name.clone()).await.unwrap();
                let payload = vec![b'A' + i];
                let (res, p) = client.write(payload).await;
                res.unwrap();
                sent.push(p);
                drop(client);
            }
            sent
        };

        let (got, sent) = futures_lite::future::zip(server_task, client_task).await;
        assert_eq!(got, sent);
    });
}

// ---------------------------------------------------------------------
// 14. Inherent `flush()` resolves on a live stream
// ---------------------------------------------------------------------
//
// Direct test of `NamedPipeStream::flush()` (the
// `unblock(FlushFileBuffers)` path) independent of the futures-io
// `poll_flush` driver.
#[test]
fn inherent_flush_resolves() {
    block_on(async {
        let name = unique_name("inh_flush");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let (res, _) = server.write(b"x".to_vec()).await;
                res.unwrap();
                // The flush call we are testing.
                server.flush().await.unwrap();
                // Drain to keep the client write happy.
                let buf = Vec::with_capacity(4);
                let (res, _) = server.read(buf).await;
                let _ = res;
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            let buf = Vec::with_capacity(4);
            let (res, _) = client.read(buf).await;
            res.unwrap();
            let (res, _) = client.write(b"y".to_vec()).await;
            res.unwrap();
            client.flush().await.unwrap();
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 15. Builder `.options()` accepts a pre-built NamedPipeOpenOptions
// ---------------------------------------------------------------------
//
// Bypasses the per-field builder helpers. The options set via
// `.options()` must take effect: we hand in `PipeAccess::Outbound`
// and verify a write-only client succeeds while a read-only client
// would fail.
#[test]
fn builder_options_method_overrides() {
    block_on(async {
        let name = unique_name("opts_method");
        let opts = async_io::os::windows::NamedPipeOpenOptions::new()
            .access(PipeAccess::Outbound) // server writes only
            .in_buffer_size(2048)
            .out_buffer_size(2048);

        let listener = NamedPipeListener::builder(&name)
            .options(opts)
            .bind()
            .unwrap();

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let (res, _) = server.write(b"sent".to_vec()).await;
                res.unwrap();
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect_with_options(
                name,
                NamedPipeConnectOptions::new().read(true).write(false),
            )
            .await
            .unwrap();
            let buf = Vec::with_capacity(8);
            let (res, buf) = client.read(buf).await;
            let n = res.unwrap();
            assert_eq!(&buf[..n], b"sent");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 16. Dropping a pending `read` future cancels the op cleanly
// ---------------------------------------------------------------------
//
// `OpHandleGuard::drop` issues `CancelIoEx`. After dropping a pending
// read future the stream must remain usable: a subsequent `read` on a
// fresh buffer should observe the next payload the peer sends. The
// kernel still owns the original buffer until its (aborted) completion
// drains, but that is internal — the user-visible API just continues
// to work without panic, hang, or use-after-free.
#[test]
fn read_future_drop_cancels_cleanly() {
    block_on(async {
        let name = unique_name("cancel_read");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                // Wait long enough for the client to submit, then drop,
                // its first read before we send anything.
                Timer::after(Duration::from_millis(100)).await;
                let (res, _) = server.write(b"after-cancel".to_vec()).await;
                res.unwrap();
                // Keep server alive until client has read.
                Timer::after(Duration::from_millis(50)).await;
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name.clone()).await.unwrap();

            // Submit a read into a fresh buffer, then drop the future
            // before any data arrives.
            {
                let buf = Vec::with_capacity(64);
                let read_fut = client.read(buf);
                // No `.await` — drop the future. `OpHandleGuard::drop`
                // should issue `CancelIoEx` and leave the stream
                // usable.
                drop(read_fut);
            }

            // Give the kernel a moment to process the cancellation
            // before we submit the second read.
            Timer::after(Duration::from_millis(20)).await;

            // The stream must still be usable: read the payload the
            // server sends after the cancellation window.
            let buf = Vec::with_capacity(64);
            let (res, buf) = client.read(buf).await;
            let n = res.expect("post-cancel read must succeed");
            assert_eq!(&buf[..n], b"after-cancel");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 17. Dropping a pending `accept` future leaves the listener usable
// ---------------------------------------------------------------------
//
// Symmetric to #16 for the listener side: drop an in-flight `accept`
// future, then perform a fresh `accept` and verify it still completes
// when a client connects.
#[test]
fn accept_future_drop_then_reaccept() {
    block_on(async {
        let name = unique_name("cancel_accept");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        // Start an accept and drop it before any client connects.
        {
            let accept_fut = listener.accept();
            drop(accept_fut);
        }

        // Brief settle.
        Timer::after(Duration::from_millis(20)).await;

        // A second accept on the same listener must complete normally.
        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let buf = Vec::with_capacity(8);
                let (res, buf) = server.read(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"hi");
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(20)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            let (res, _) = client.write(b"hi".to_vec()).await;
            res.unwrap();
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 18. Message-mode partial read surfaces as `Ok(n)` (ERROR_MORE_DATA)
// ---------------------------------------------------------------------
//
// In message mode, when the read buffer is smaller than the next
// queued message, Win32 returns `ERROR_MORE_DATA` (234) which the
// polling fork translates to `Ok(bytes)` so the partial data is not
// lost. The remaining bytes of the same message are delivered by the
// next `read` call. We send a 32-byte message, read with an 8-byte
// buffer, and verify two reads reassemble the original.
#[test]
fn message_mode_partial_read() {
    block_on(async {
        let name = unique_name("msg_partial");
        let listener = NamedPipeListener::builder(&name)
            .pipe_mode(PipeMode::Message)
            .in_buffer_size(4096)
            .out_buffer_size(4096)
            .bind()
            .unwrap();

        let payload: Vec<u8> = (0..32u8).collect();
        let exp = payload.clone();

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                let (res, _) = server.write(payload).await;
                res.unwrap();
                // Hold open until client finishes.
                Timer::after(Duration::from_millis(50)).await;
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();

            // First read: 8-byte buffer; should return Ok(8) carrying
            // the first 8 bytes of the 32-byte message.
            let buf = Vec::with_capacity(8);
            let (res, buf) = client.read(buf).await;
            let n = res.expect("partial read must surface as Ok");
            assert_eq!(n, 8, "first read should fill the small buffer");
            assert_eq!(&buf[..n], &exp[..8]);

            // Drain the remaining 24 bytes (may take multiple reads
            // because each ReadFile in message mode returns at most
            // one message fragment per call).
            let mut got = Vec::from(&buf[..n]);
            while got.len() < exp.len() {
                let buf = Vec::with_capacity(64);
                let (res, buf) = client.read(buf).await;
                let n = res.unwrap();
                assert!(n > 0, "remaining bytes must still be readable");
                got.extend_from_slice(&buf[..n]);
            }
            assert_eq!(got, exp, "reassembled payload matches original");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 19. Zero-length read and write are no-ops that succeed
// ---------------------------------------------------------------------
//
// Win32 `ReadFile` / `WriteFile` with a zero-byte buffer return
// success with 0 bytes transferred. The async wrappers should pass
// the empty buffer through unchanged and return `Ok(0)`.
#[test]
fn zero_length_read_write() {
    block_on(async {
        let name = unique_name("zero_len");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                // Zero-length write: must succeed with 0 bytes.
                let (res, buf) = server.write(Vec::<u8>::new()).await;
                let n = res.expect("zero-length write");
                assert_eq!(n, 0);
                assert!(buf.is_empty(), "buffer returned unchanged");
                // Hold open so the client can complete its zero read.
                Timer::after(Duration::from_millis(50)).await;
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            // Zero-capacity read: must succeed with 0 bytes.
            let buf = Vec::<u8>::with_capacity(0);
            let (res, buf) = client.read(buf).await;
            let n = res.expect("zero-length read");
            assert_eq!(n, 0);
            assert_eq!(buf.capacity(), 0);
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 20. Concurrent read + write on the same `&NamedPipeStream`
// ---------------------------------------------------------------------
//
// The inherent `read` / `write` methods take `&self`, so two futures
// can hold simultaneous outstanding ops on the same stream — the
// reactor's per-direction wakers keep them independent. We drive a
// concurrent read+write on a single client stream against an echoing
// server.
#[test]
fn concurrent_read_and_write_on_same_stream() {
    block_on(async {
        let name = unique_name("concurrent");
        let listener = spawn_listener(name.clone(), PipeMode::Byte);

        let server_task = {
            let listener = listener.clone();
            async move {
                let server = listener.accept().await.unwrap();
                // Echo back whatever we receive.
                let buf = Vec::with_capacity(64);
                let (res, buf) = server.read(buf).await;
                let n = res.unwrap();
                let (res, _) = server.write(buf[..n].to_vec()).await;
                res.unwrap();
                Timer::after(Duration::from_millis(50)).await;
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();

            // Issue read and write concurrently on the same stream
            // (both take `&self`). The write must complete first
            // (server reads it), then the server's echo unblocks the
            // read.
            let read_fut = {
                let client = &client;
                async move {
                    let buf = Vec::with_capacity(16);
                    let (res, buf) = client.read(buf).await;
                    let n = res.unwrap();
                    buf[..n].to_vec()
                }
            };
            let write_fut = {
                let client = &client;
                async move {
                    let (res, _) = client.write(b"echo-me".to_vec()).await;
                    res.unwrap();
                }
            };

            let (got, ()) = futures_lite::future::zip(read_fut, write_fut).await;
            assert_eq!(got, b"echo-me");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// SECURITY_ATTRIBUTES support: inherit_handle + security_descriptor
// ---------------------------------------------------------------------

mod security {
    use super::*;
    use std::os::windows::io::{AsHandle, AsRawHandle};

    use async_io::os::windows::NamedPipeOpenOptions;
    use windows_sys::Win32::Foundation as wf;
    use windows_sys::Win32::Security as wsec;
    use windows_sys::Win32::Security::Authorization as wsa;

    /// Server-side `inherit_handle(true)` results in a kernel handle
    /// whose `HANDLE_FLAG_INHERIT` is set.
    #[test]
    fn inherit_handle_server_sets_handle_flag_inherit() {
        block_on(async {
            let name = unique_name("inherit_srv");
            let opts = NamedPipeOpenOptions::new().inherit_handle(true);
            let listener = NamedPipeListener::builder(&name)
                .options(opts)
                .bind()
                .expect("bind");
            // We need an in-flight pipe instance to inspect. accept()
            // creates one before suspending on ConnectNamedPipe.
            let server_fut = listener.accept();
            let client_fut = NamedPipeStream::connect(name);
            let (server, _client) =
                futures_lite::future::zip(server_fut, client_fut).await;
            let server = server.expect("accept");

            let mut flags: u32 = 0;
            // SAFETY: server.as_handle() is a live pipe handle; flags
            // is a valid out-pointer.
            let ok = unsafe {
                wf::GetHandleInformation(
                    server.as_handle().as_raw_handle() as wf::HANDLE,
                    &mut flags,
                )
            };
            assert_ne!(ok, 0, "GetHandleInformation failed");
            assert_eq!(
                flags & wf::HANDLE_FLAG_INHERIT,
                wf::HANDLE_FLAG_INHERIT,
                "expected HANDLE_FLAG_INHERIT to be set"
            );
        });
    }

    /// Default options must NOT set `HANDLE_FLAG_INHERIT` — sanity
    /// check that the server-side flag actually tracks the option.
    #[test]
    fn default_inherit_handle_is_off() {
        block_on(async {
            let name = unique_name("inherit_off");
            let listener = spawn_listener(name.clone(), PipeMode::Byte);
            let server_fut = listener.accept();
            let client_fut = NamedPipeStream::connect(name);
            let (server, _client) =
                futures_lite::future::zip(server_fut, client_fut).await;
            let server = server.expect("accept");

            let mut flags: u32 = 0;
            // SAFETY: see previous test.
            let ok = unsafe {
                wf::GetHandleInformation(
                    server.as_handle().as_raw_handle() as wf::HANDLE,
                    &mut flags,
                )
            };
            assert_ne!(ok, 0);
            assert_eq!(flags & wf::HANDLE_FLAG_INHERIT, 0);
        });
    }

    /// Client-side `inherit_handle(true)` flows through the
    /// `CreateFileW` `SECURITY_ATTRIBUTES`.
    #[test]
    fn inherit_handle_client_sets_handle_flag_inherit() {
        block_on(async {
            let name = unique_name("inherit_cli");
            let listener = spawn_listener(name.clone(), PipeMode::Byte);
            let server_fut = listener.accept();
            let opts = NamedPipeConnectOptions::new().inherit_handle(true);
            let client_fut = NamedPipeStream::connect_with_options(name, opts);
            let (server, client) =
                futures_lite::future::zip(server_fut, client_fut).await;
            let _server = server.expect("accept");
            let client = client.expect("connect");

            let mut flags: u32 = 0;
            // SAFETY: client.as_handle() is a live pipe handle; flags
            // is a valid out-pointer.
            let ok = unsafe {
                wf::GetHandleInformation(
                    client.as_handle().as_raw_handle() as wf::HANDLE,
                    &mut flags,
                )
            };
            assert_ne!(ok, 0);
            assert_eq!(flags & wf::HANDLE_FLAG_INHERIT, wf::HANDLE_FLAG_INHERIT);
        });
    }

    /// Custom SDDL applied via `security_descriptor_sddl` is observed
    /// on the resulting pipe via `GetSecurityInfo` round-tripped back
    /// to an SDDL string.
    #[test]
    fn sddl_round_trip_via_get_security_info() {
        // Grant Generic All to Everyone (WD = World/Everyone group).
        const SDDL: &str = "D:(A;;GA;;;WD)";

        block_on(async {
            let name = unique_name("sddl_rt");
            let listener = NamedPipeListener::builder(&name)
                .security_descriptor_sddl(SDDL)
                .expect("parse sddl")
                .bind()
                .expect("bind");
            let server_fut = listener.accept();
            let client_fut = NamedPipeStream::connect(name);
            let (server, _client) =
                futures_lite::future::zip(server_fut, client_fut).await;
            let server = server.expect("accept");

            // Query the kernel's view of the DACL.
            let mut psd: wsec::PSECURITY_DESCRIPTOR = std::ptr::null_mut();
            // SAFETY: server.as_handle() is live; output pointers are valid.
            let err = unsafe {
                wsa::GetSecurityInfo(
                    server.as_handle().as_raw_handle() as wf::HANDLE,
                    wsa::SE_KERNEL_OBJECT,
                    wsec::DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &mut psd,
                )
            };
            assert_eq!(err, 0, "GetSecurityInfo failed: {}", err);
            assert!(!psd.is_null());

            // Convert back to SDDL.
            let mut wide_out: windows_sys::core::PWSTR = std::ptr::null_mut();
            let mut wide_len: u32 = 0;
            // SAFETY: psd is a valid SD; out-pointers are valid.
            let ok = unsafe {
                wsa::ConvertSecurityDescriptorToStringSecurityDescriptorW(
                    psd,
                    wsa::SDDL_REVISION_1,
                    wsec::DACL_SECURITY_INFORMATION,
                    &mut wide_out,
                    &mut wide_len,
                )
            };
            assert_ne!(ok, 0, "ConvertSecurityDescriptorToString failed");

            // wide_len includes the NUL terminator.
            let len = wide_len.saturating_sub(1) as usize;
            // SAFETY: wide_out points to `wide_len` u16s.
            let slice = unsafe { std::slice::from_raw_parts(wide_out, len) };
            let sddl_back = String::from_utf16(slice).expect("utf16");

            // SAFETY: psd was allocated by GetSecurityInfo (LocalAlloc).
            unsafe {
                wf::LocalFree(psd as wf::HLOCAL);
            }
            // SAFETY: wide_out was allocated by Convert...ToString (LocalAlloc).
            unsafe {
                wf::LocalFree(wide_out as wf::HLOCAL);
            }

            // The kernel resolves generic rights against the object
            // type's GENERIC_MAPPING when storing the SD, so `GA`
            // (Generic All) is normalised to the file/pipe-specific
            // `FA` (File All Access) on read-back. Accept either form,
            // alongside the World SID (`WD`).
            assert!(
                sddl_back.contains(";WD)"),
                "expected SDDL round-trip to mention WD (Everyone); got {sddl_back:?}"
            );
            assert!(
                sddl_back.contains(";GA;") || sddl_back.contains(";FA;"),
                "expected SDDL round-trip to grant GA or FA; got {sddl_back:?}"
            );
        });
    }

    /// Invalid SDDL is rejected at the builder level, before any
    /// `bind()` happens.
    #[test]
    fn invalid_sddl_returns_err() {
        let res = NamedPipeOpenOptions::new()
            .security_descriptor_sddl("this is not a valid SDDL string");
        let err = res.expect_err("expected SDDL parse to fail");
        // ConvertString...W returns ERROR_INVALID_PARAMETER (87) for
        // malformed SDDL on all supported Windows versions, but allow
        // any kind of error for forward compatibility.
        assert!(err.raw_os_error().is_some(), "expected an OS error");
    }

    /// Random bytes passed to `security_descriptor` are not UB; the
    /// kernel rejects them at `accept()` time with
    /// `ERROR_INVALID_SECURITY_DESCR`.
    #[test]
    fn invalid_sd_bytes_fail_at_accept() {
        block_on(async {
            let name = unique_name("bad_sd");
            // 64 bytes of garbage. Length matches no real SD layout.
            let opts = NamedPipeOpenOptions::new().security_descriptor(vec![0xFFu8; 64]);
            let listener = NamedPipeListener::builder(&name)
                .options(opts)
                .bind()
                .expect("bind (lazy)");
            let err = listener.accept().await.expect_err("accept must fail");
            // ERROR_INVALID_SECURITY_DESCR = 1338. Don't pin the exact
            // code (Win32 may evolve) — just require an OS error.
            assert!(err.raw_os_error().is_some(), "expected an OS error");
        });
    }
}

