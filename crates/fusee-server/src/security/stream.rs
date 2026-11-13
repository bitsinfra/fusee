//! An asynchronous TLS stream over a tokio TCP socket, split into a read half
//! for a task and a write half that any thread can use without a runtime.
//!
//! The write half has the same contract as the server's plain socket halves
//! ([`TryWrite`]): `try_write` takes what it can now, and `writable` waits
//! until it can take more. It encrypts under a mutex and writes the records
//! with the socket's non-blocking `try_write`, so no lock is ever held
//! across a wait. Records the socket did not take wait in an outbound
//! buffer; while some do, a flusher task writes them as the socket drains,
//! so output never depends on the caller writing again. Once the buffer
//! holds [`BACKLOG`] bytes, `try_write` takes nothing.
//!
//! The read half reads ciphertext straight into rustls with `try_read`,
//! and sends anything rustls answers (a key update, an alert) the same way.
use super::auth::EXPORTER_LABEL;
use crate::server::{Socket, TryWrite};
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Notify;

/// Encrypted bytes waiting for the socket before `try_write` refuses more.
const BACKLOG: usize = 256 << 10;
/// Plaintext encrypted per `try_write`.
const CHUNK: usize = 256 << 10;

struct State {
    conn: rustls::Connection,
    out: Vec<u8>,
    sent: usize,
    failed: Option<(io::ErrorKind, String)>,
}

impl State {
    fn backlog(&self) -> usize {
        self.out.len() - self.sent
    }
}

struct Shared {
    sock: TcpStream,
    state: Mutex<State>,
    wake: Arc<Wake>,
}

/// Outlives [`Shared`] in the flusher, which holds only a weak reference
/// while it waits.
#[derive(Default)]
struct Wake {
    kick: Notify,
    closed: AtomicBool,
}

/// Dropped with the last half: stops the flusher, which lets the socket close.
struct Guard(Arc<Wake>);

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.kick.notify_one();
    }
}

struct TryRead<'a>(&'a TcpStream);

impl Read for TryRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.try_read(buf)
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Moves rustls's records into the outbound buffer and writes what the
    /// socket takes now.
    fn flush(&self, st: &mut State) -> io::Result<()> {
        if let Some((kind, message)) = &st.failed {
            return Err(io::Error::new(*kind, message.clone()));
        }
        while st.conn.wants_write() {
            st.conn.write_tls(&mut st.out)?;
        }
        while st.sent < st.out.len() {
            match self.sock.try_write(&st.out[st.sent..]) {
                Ok(0) => return Err(Self::fail(st, io::ErrorKind::WriteZero.into())),
                Ok(n) => st.sent += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(Self::fail(st, e)),
            }
        }
        if st.sent == st.out.len() {
            st.out.clear();
            st.sent = 0;
        } else if st.sent >= 1 << 20 {
            st.out.drain(..st.sent);
            st.sent = 0;
        }
        Ok(())
    }

    fn fail(st: &mut State, e: io::Error) -> io::Error {
        st.failed = Some((e.kind(), e.to_string()));
        e
    }

    fn kick_if_waiting(&self, st: &State) {
        if st.backlog() > 0 {
            self.wake.kick.notify_one();
        }
    }
}

/// Writes records the socket did not take while the halves are idle.
async fn flusher(shared: Weak<Shared>, wake: Arc<Wake>) {
    loop {
        wake.kick.notified().await;
        loop {
            if wake.closed.load(Ordering::Acquire) {
                return;
            }
            let Some(shared) = shared.upgrade() else {
                return;
            };
            let waiting = {
                let mut st = shared.lock();
                if shared.flush(&mut st).is_err() {
                    return;
                }
                st.backlog() > 0
            };
            if !waiting {
                break;
            }
            tokio::select! {
                r = shared.sock.writable() => if r.is_err() { return },
                _ = wake.kick.notified() => {}
            }
        }
    }
}

/// A TLS connection after its handshake.
pub(crate) struct TlsStream {
    read: TlsRead,
    write: TlsWrite,
}

pub(crate) struct TlsRead {
    shared: Arc<Shared>,
    _guard: Arc<Guard>,
}

pub(crate) struct TlsWrite {
    shared: Arc<Shared>,
    _guard: Arc<Guard>,
}

impl TlsStream {
    /// Runs the handshake on `sock`, then wraps it. Must run on a runtime:
    /// the stream's flusher is spawned on it.
    pub(crate) async fn handshake(sock: TcpStream, conn: rustls::Connection) -> io::Result<Self> {
        let (sock, conn) = Self::handshake_only(sock, conn).await?;
        Ok(Self::wrap(sock, conn))
    }

    /// The handshake alone: the socket and the established connection, to
    /// wrap later, possibly on another runtime. Anything the peer sent after
    /// its last handshake message is already inside `conn`.
    pub(crate) async fn handshake_only(
        sock: TcpStream,
        mut conn: rustls::Connection,
    ) -> io::Result<(TcpStream, rustls::Connection)> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut sock = sock;
        let mut buf = vec![0u8; 16 << 10];
        let mut out = Vec::new();
        loop {
            while conn.wants_write() {
                conn.write_tls(&mut out)?;
                sock.write_all(&out).await?;
                out.clear();
            }
            if !conn.is_handshaking() {
                break;
            }
            let n = sock.read(&mut buf).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the connection closed during the TLS handshake",
                ));
            }
            let mut input = &buf[..n];
            while !input.is_empty() {
                conn.read_tls(&mut input)?;
                if let Err(e) = conn.process_new_packets() {
                    // Send the alert that says why, then give up.
                    while conn.wants_write() {
                        if conn.write_tls(&mut out).is_err() {
                            break;
                        }
                    }
                    let _ = sock.write_all(&out).await;
                    linger(&mut sock).await;
                    return Err(super::tls::error("TLS handshake", e));
                }
            }
        }
        conn.set_buffer_limit(None);
        Ok((sock, conn))
    }

    /// Wraps an established connection. Must run on the runtime that
    /// `sock` is registered with: the stream's flusher is spawned on it.
    pub(crate) fn wrap(sock: TcpStream, conn: rustls::Connection) -> Self {
        let wake = Arc::new(Wake::default());
        let shared = Arc::new(Shared {
            sock,
            state: Mutex::new(State {
                conn,
                out: Vec::new(),
                sent: 0,
                failed: None,
            }),
            wake: wake.clone(),
        });
        tokio::spawn(flusher(Arc::downgrade(&shared), wake.clone()));
        let guard = Arc::new(Guard(wake));
        Self {
            read: TlsRead {
                shared: shared.clone(),
                _guard: guard.clone(),
            },
            write: TlsWrite {
                shared,
                _guard: guard,
            },
        }
    }

    /// TLS exporter output that binds the token exchange to this session.
    pub(crate) fn binding(&self) -> io::Result<Vec<u8>> {
        self.read
            .shared
            .lock()
            .conn
            .export_keying_material(vec![0u8; 32], EXPORTER_LABEL, None)
            .map_err(|e| super::tls::error("TLS exporter", e))
    }

    pub(crate) fn into_split(self) -> (TlsRead, TlsWrite) {
        (self.read, self.write)
    }

    /// The other side's end-entity certificate, verified against the CA
    /// during the handshake; `None` when the listener did not ask for one.
    pub(crate) fn peer_certificate(&self) -> Option<rustls::pki_types::CertificateDer<'static>> {
        self.read
            .shared
            .lock()
            .conn
            .peer_certificates()
            .and_then(|chain| chain.first())
            .cloned()
    }
}

/// How long a refused connection waits for its peer to close first.
const LINGER: std::time::Duration = std::time::Duration::from_secs(1);

/// Discards what the peer still sends, until it closes or [`LINGER`]
/// passes. Closing a socket with unread input resets the connection, and a
/// reset can destroy the alert or refusal written just before it.
pub(super) async fn linger<S: AsyncRead + Unpin>(sock: &mut S) {
    use tokio::io::AsyncReadExt;
    let _ = tokio::time::timeout(LINGER, async {
        let mut buf = [0u8; 4096];
        while let Ok(1..) = sock.read(&mut buf).await {}
    })
    .await;
}

impl TlsRead {
    fn poll_read_plain(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut eof = false;
        loop {
            let mut st = self.shared.lock();
            match st.conn.reader().read(buf.initialize_unfilled()) {
                Ok(n) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                // Closed without close_notify. Frames are self-delimiting and
                // checksummed, so the protocol sees an ordinary EOF.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Poll::Ready(Ok(())),
                Err(e) => return Poll::Ready(Err(e)),
            }
            if eof {
                return Poll::Ready(Ok(()));
            }
            match st.conn.read_tls(&mut TryRead(&self.shared.sock)) {
                Ok(0) => eof = true,
                Ok(_) => {
                    if let Err(e) = st.conn.process_new_packets() {
                        let _ = self.shared.flush(&mut st);
                        return Poll::Ready(Err(super::tls::error("TLS", e)));
                    }
                    if st.conn.wants_write() {
                        let flushed = self.shared.flush(&mut st);
                        self.shared.kick_if_waiting(&st);
                        flushed?;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    drop(st);
                    ready!(self.shared.sock.poll_read_ready(cx))?;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl AsyncRead for TlsRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().poll_read_plain(cx, buf)
    }
}

impl TlsWrite {
    /// Encrypts up to [`CHUNK`] bytes of `buf` unless [`BACKLOG`] bytes of
    /// records are already waiting; `WouldBlock` when it takes nothing.
    pub(crate) fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        let mut st = self.shared.lock();
        self.shared.flush(&mut st)?;
        if st.backlog() >= BACKLOG {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = buf.len().min(CHUNK);
        st.conn.writer().write_all(&buf[..n])?;
        let flushed = self.shared.flush(&mut st);
        self.shared.kick_if_waiting(&st);
        flushed.map(|()| n)
    }

    /// Ready once [`try_write`](Self::try_write) can take more.
    fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            {
                let mut st = self.shared.lock();
                self.shared.flush(&mut st)?;
                if st.backlog() < BACKLOG {
                    return Poll::Ready(Ok(()));
                }
            }
            ready!(self.shared.sock.poll_write_ready(cx))?;
        }
    }

    /// Ready once every record has reached the socket.
    fn poll_flushed(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            {
                let mut st = self.shared.lock();
                self.shared.flush(&mut st)?;
                if st.backlog() == 0 {
                    return Poll::Ready(Ok(()));
                }
            }
            ready!(self.shared.sock.poll_write_ready(cx))?;
        }
    }

    pub(crate) async fn writable(&self) -> io::Result<()> {
        std::future::poll_fn(|cx| self.poll_writable(cx)).await
    }

    /// Queues a TLS 1.3 key update that also asks the peer to update.
    #[cfg(test)]
    pub(super) fn refresh_traffic_keys(&self) {
        self.shared
            .lock()
            .conn
            .refresh_traffic_keys()
            .expect("key update");
    }
}

impl AsyncWrite for TlsWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match self.try_write(buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => ready!(self.poll_writable(cx))?,
                result => return Poll::Ready(result),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flushed(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shared.lock().conn.send_close_notify();
        self.poll_flushed(cx)
    }
}

impl AsyncRead for TlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().read.poll_read_plain(cx, buf)
    }
}

impl AsyncWrite for TlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().write).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().write).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().write).poll_shutdown(cx)
    }
}

impl Socket for TlsStream {
    type Read = TlsRead;
    type Write = TlsWrite;
    fn split(self) -> (TlsRead, TlsWrite) {
        self.into_split()
    }
}

impl TryWrite for TlsWrite {
    fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        TlsWrite::try_write(self, buf)
    }
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send {
        TlsWrite::writable(self)
    }
}
