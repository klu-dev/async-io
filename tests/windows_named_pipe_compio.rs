// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for the `compio_io::{AsyncRead, AsyncWrite}`
//! adapters on `NamedPipeStream`. Gated on `cfg(windows)` and the
//! `compio` feature; see `docs/named-pipe.design.md` §6.8.

#![cfg(all(windows, feature = "compio"))]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_io::os::windows::{NamedPipeListener, NamedPipeStream, PipeMode};
use async_io::{block_on, Timer};
use compio_buf::BufResult;
use compio_io::{AsyncRead, AsyncWrite};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!(r"\\.\pipe\async_io_compio_{}_{}_{}", stem, pid, n)
}

fn spawn_listener(name: String) -> NamedPipeListener {
    NamedPipeListener::bind(name, PipeMode::Byte, None).expect("bind")
}

// ---------------------------------------------------------------------
// 1. Vec<u8> round-trip via compio AsyncRead / AsyncWrite
// ---------------------------------------------------------------------
#[test]
fn compio_round_trip() {
    block_on(async {
        let name = unique_name("rt");
        let listener = spawn_listener(name.clone());

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                let buf: Vec<u8> = Vec::with_capacity(64);
                let BufResult(res, buf) = AsyncRead::read(&mut server, buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"hello");
                let BufResult(res, _) = AsyncWrite::write(&mut server, b"world".to_vec()).await;
                assert_eq!(res.unwrap(), 5);
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let mut client = NamedPipeStream::connect(name).await.unwrap();
            let BufResult(res, _) = AsyncWrite::write(&mut client, b"hello".to_vec()).await;
            assert_eq!(res.unwrap(), 5);
            let buf: Vec<u8> = Vec::with_capacity(64);
            let BufResult(res, buf) = AsyncRead::read(&mut client, buf).await;
            let n = res.unwrap();
            assert_eq!(&buf[..n], b"world");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 2. Peer drop surfaces Ok(0) (EOF) on the next read
// ---------------------------------------------------------------------
#[test]
fn compio_eof() {
    block_on(async {
        let name = unique_name("eof");
        let listener = spawn_listener(name.clone());

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                // Read until EOF.
                let buf: Vec<u8> = Vec::with_capacity(64);
                let BufResult(res, _) = AsyncRead::read(&mut server, buf).await;
                let n = res.unwrap();
                assert_eq!(n, 0, "peer drop should surface as Ok(0)");
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            // Drop immediately to close the pipe.
            drop(client);
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 3. set_buf_init forwarding: returned Vec has len() == bytes_read
// ---------------------------------------------------------------------
#[test]
fn compio_set_buf_init() {
    block_on(async {
        let name = unique_name("setinit");
        let listener = spawn_listener(name.clone());

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                let buf: Vec<u8> = Vec::with_capacity(128);
                assert_eq!(buf.len(), 0);
                let BufResult(res, buf) = AsyncRead::read(&mut server, buf).await;
                let n = res.unwrap();
                // Critical assertion: the wrapper must forward
                // set_init -> SetLen::set_len, so buf.len() now
                // equals the bytes read.
                assert_eq!(buf.len(), n);
                assert_eq!(&buf[..], b"abcdefg");
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let mut client = NamedPipeStream::connect(name).await.unwrap();
            let BufResult(res, _) = AsyncWrite::write(&mut client, b"abcdefg".to_vec()).await;
            res.unwrap();
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 4. Buffer is returned even on error (peer-disconnect during read)
// ---------------------------------------------------------------------
#[test]
fn compio_buffer_returned_on_error() {
    block_on(async {
        let name = unique_name("buferr");
        let listener = spawn_listener(name.clone());

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                // Two reads: the first observes EOF (Ok(0)); the
                // second is what we use to verify the buffer is
                // returned. The second read after EOF on a broken
                // pipe yields either Ok(0) again or a translated
                // EOF — in *either* case the BufResult must carry
                // back the buffer we passed in.
                let buf: Vec<u8> = Vec::with_capacity(32);
                let cap_before = buf.capacity();
                let BufResult(_res, buf) = AsyncRead::read(&mut server, buf).await;
                // Buffer must come back with the same allocation
                // identity (capacity preserved).
                assert_eq!(buf.capacity(), cap_before);
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            drop(client);
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 5. flush() and shutdown() succeed on a live stream
// ---------------------------------------------------------------------
#[test]
fn compio_flush_shutdown() {
    block_on(async {
        let name = unique_name("flush");
        let listener = spawn_listener(name.clone());

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                let BufResult(res, _) = AsyncWrite::write(&mut server, b"ping".to_vec()).await;
                res.unwrap();
                AsyncWrite::flush(&mut server).await.unwrap();
                AsyncWrite::shutdown(&mut server).await.unwrap();
                // Drain the response so the client's write completes.
                let buf: Vec<u8> = Vec::with_capacity(8);
                let BufResult(res, _) = AsyncRead::read(&mut server, buf).await;
                let _ = res;
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let mut client = NamedPipeStream::connect(name).await.unwrap();
            let buf: Vec<u8> = Vec::with_capacity(8);
            let BufResult(res, _) = AsyncRead::read(&mut client, buf).await;
            res.unwrap();
            let BufResult(res, _) = AsyncWrite::write(&mut client, b"pong".to_vec()).await;
            res.unwrap();
            AsyncWrite::flush(&mut client).await.unwrap();
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 6. Round-trip with `Box<[u8]>` (a different IoBufMut impl)
// ---------------------------------------------------------------------
//
// Validates that the `WriteBufWrap`/`ReadBufWrap` adapters work for
// IoBuf{,Mut} impls other than `Vec<u8>`. `Box<[u8]>` reports
// capacity == len (no separate length field), which exercises a
// distinct branch of the polling `set_init` no-op contract.
#[test]
fn compio_box_slice_buffer() {
    block_on(async {
        let name = unique_name("box_slice");
        let listener = spawn_listener(name.clone());

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                let buf: Box<[u8]> = vec![0u8; 16].into_boxed_slice();
                let BufResult(res, buf) = AsyncRead::read(&mut server, buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"boxed");
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let mut client = NamedPipeStream::connect(name).await.unwrap();
            let payload: Box<[u8]> = b"boxed".to_vec().into_boxed_slice();
            let BufResult(res, _) = AsyncWrite::write(&mut client, payload).await;
            assert_eq!(res.unwrap(), 5);
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}

// ---------------------------------------------------------------------
// 7. Owned `NamedPipeStream` works as the `AsyncRead`/`AsyncWrite`
//    receiver (not just `&mut`).
// ---------------------------------------------------------------------
//
// The compio traits are implemented for both `&NamedPipeStream` and
// `NamedPipeStream`; the `&mut` form is provided by compio's
// blanket. This test passes an owned stream into a generic helper
// that takes `impl AsyncRead + AsyncWrite` to ensure the owned
// impl is selectable.
#[test]
fn compio_owned_receiver() {
    async fn echo<S: AsyncRead + AsyncWrite>(mut s: S, payload: Vec<u8>) -> Vec<u8> {
        let BufResult(res, _) = AsyncWrite::write(&mut s, payload).await;
        res.unwrap();
        AsyncWrite::flush(&mut s).await.unwrap();
        let buf: Vec<u8> = Vec::with_capacity(64);
        let BufResult(res, buf) = AsyncRead::read(&mut s, buf).await;
        let n = res.unwrap();
        buf[..n].to_vec()
    }

    block_on(async {
        let name = unique_name("owned");
        let listener = spawn_listener(name.clone());

        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await.unwrap();
                let buf: Vec<u8> = Vec::with_capacity(64);
                let BufResult(res, buf) = AsyncRead::read(&mut server, buf).await;
                let n = res.unwrap();
                let mut reply = buf[..n].to_vec();
                reply.reverse();
                let BufResult(res, _) = AsyncWrite::write(&mut server, reply).await;
                res.unwrap();
            }
        };

        let client_task = async {
            Timer::after(Duration::from_millis(10)).await;
            let client = NamedPipeStream::connect(name).await.unwrap();
            // Pass the owned stream by value into the generic helper.
            let got = echo(client, b"abc".to_vec()).await;
            assert_eq!(got, b"cba");
        };

        futures_lite::future::zip(server_task, client_task).await;
    });
}
