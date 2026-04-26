//! Async named-pipe server + client on Windows using
//! [`async_io::os::windows::NamedPipeListener`] and
//! [`async_io::os::windows::NamedPipeStream`].
//!
//! Both endpoints run inside the same `block_on` so the example is
//! self-contained. The server accepts one client, echoes a single
//! line back upper-cased, then closes; the client connects, sends a
//! greeting, prints the response, and exits.
//!
//! For an accept loop that handles many clients, use
//! [`NamedPipeListener::incoming`] which yields a
//! [`futures_lite::Stream`] of accepted connections:
//!
//! ```ignore
//! use futures_lite::StreamExt;
//! let mut incoming = listener.incoming();
//! while let Some(stream) = incoming.next().await {
//!     let stream = stream?;
//!     // spawn a task per client, read/write, ...
//! }
//! ```
//!
//! With the `compio` cargo feature enabled, `NamedPipeStream` also
//! implements `compio_io::AsyncRead` / `AsyncWrite` for use from a
//! compio-based runtime (see `tests/windows_named_pipe_compio.rs`).
//!
//! Run with:
//!
//! ```text
//! cargo run --example windows-named-pipe
//! ```

#[cfg(windows)]
fn main() -> std::io::Result<()> {
    use std::time::Duration;

    use async_io::os::windows::{NamedPipeListener, NamedPipeStream, PipeMode};
    use async_io::{block_on, Timer};
    use futures_lite::{future, AsyncReadExt, AsyncWriteExt};

    // Pipe names live in the `\\.\pipe\` namespace and are
    // process-wide; embed the PID so concurrent runs of the example
    // do not collide.
    let pipe_name = format!(r"\\.\pipe\async-io-example-{}", std::process::id());

    // Build the listener up front so the client cannot race the
    // server to `CreateFile` before the first pipe instance exists.
    let listener = NamedPipeListener::builder(&pipe_name)
        .pipe_mode(PipeMode::Byte)
        .in_buffer_size(4096)
        .out_buffer_size(4096)
        .bind()?;

    block_on(async move {
        let server_task = {
            let listener = listener.clone();
            async move {
                let mut server = listener.accept().await?;
                println!("[server] client connected");

                // Read whatever the client sent (up to 64 bytes for
                // demo purposes).
                let mut buf = [0u8; 64];
                let n = AsyncReadExt::read(&mut server, &mut buf).await?;
                let request = std::str::from_utf8(&buf[..n]).unwrap_or("<not utf-8>");
                println!("[server] received: {:?}", request);

                // Echo back upper-cased.
                let reply = request.to_uppercase();
                AsyncWriteExt::write_all(&mut server, reply.as_bytes()).await?;
                AsyncWriteExt::flush(&mut server).await?;
                println!("[server] sent: {:?}", reply);
                Ok::<(), std::io::Error>(())
            }
        };

        let client_task = async {
            // Tiny delay so the print order is deterministic; the
            // actual `connect` would block-and-retry on its own.
            Timer::after(Duration::from_millis(50)).await;

            let mut client = NamedPipeStream::connect(pipe_name).await?;
            println!("[client] connected");

            AsyncWriteExt::write_all(&mut client, b"hello, named pipe").await?;
            AsyncWriteExt::flush(&mut client).await?;

            let mut buf = [0u8; 64];
            let n = AsyncReadExt::read(&mut client, &mut buf).await?;
            let reply = std::str::from_utf8(&buf[..n]).unwrap_or("<not utf-8>");
            println!("[client] received: {:?}", reply);
            Ok::<(), std::io::Error>(())
        };

        let (s, c) = future::zip(server_task, client_task).await;
        s.and(c)
    })
}

#[cfg(not(windows))]
fn main() {
    println!("This example is Windows-only.");
}
