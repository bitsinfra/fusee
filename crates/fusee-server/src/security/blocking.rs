//! Blocking connections for workers and clients: one thread reads, others
//! write under the caller's lock.
//!
//! Over TLS both halves share the rustls connection behind a mutex that is
//! held only to encrypt or decrypt, never across a socket call. The reader
//! reads ciphertext without the lock and feeds it in; a writer encrypts
//! under the lock and writes the records after releasing it. Records the
//! reader's processing produces (a key-update answer) wait in rustls until
//! the next write sends them ahead of its own; TLS 1.3 lets that answer be
//! late. So neither side ever blocks on the socket while the other needs
//! the lock, and two peers that both write heavily cannot deadlock.
use super::auth::EXPORTER_LABEL;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A connected socket.
#[derive(Debug)]
pub(crate) enum Raw {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Raw {
    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        match self {
            Self::Tcp(s) => s.try_clone().map(Self::Tcp),
            Self::Unix(s) => s.try_clone().map(Self::Unix),
        }
    }

    /// The descriptor of a Unix socket, which can carry descriptors.
    pub(crate) fn unix_fd(&self) -> Option<std::os::fd::RawFd> {
        use std::os::fd::AsRawFd;
        match self {
            Self::Tcp(_) => None,
            Self::Unix(s) => Some(s.as_raw_fd()),
        }
    }

    /// Closes both directions; a socket already disconnected is closed.
    pub(crate) fn shutdown(&self) -> io::Result<()> {
        let result = match self {
            Self::Tcp(s) => s.shutdown(std::net::Shutdown::Both),
            Self::Unix(s) => s.shutdown(std::net::Shutdown::Both),
        };
        match result {
            Err(e) if e.kind() == io::ErrorKind::NotConnected => Ok(()),
            other => other,
        }
    }

    pub(crate) fn set_timeouts(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Self::Tcp(s) => {
                s.set_read_timeout(timeout)?;
                s.set_write_timeout(timeout)
            }
            Self::Unix(s) => {
                s.set_read_timeout(timeout)?;
                s.set_write_timeout(timeout)
            }
        }
    }
}

impl Read for Raw {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(s) => s.read(buf),
            Self::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Raw {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(s) => s.write(buf),
            Self::Unix(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(s) => s.flush(),
            Self::Unix(s) => s.flush(),
        }
    }
}

type Shared = Arc<Mutex<rustls::Connection>>;

fn lock(conn: &Shared) -> std::sync::MutexGuard<'_, rustls::Connection> {
    conn.lock().unwrap_or_else(|e| e.into_inner())
}

/// Plaintext written per call: bounds the records buffered for one write.
const CHUNK: usize = 256 << 10;

pub(crate) fn client_handshake(
    raw: &mut Raw,
    config: Arc<rustls::ClientConfig>,
    name: rustls::pki_types::ServerName<'static>,
) -> io::Result<rustls::Connection> {
    let mut conn = rustls::ClientConnection::new(config, name)
        .map_err(|e| super::tls::error("TLS setup", e))?;
    while conn.is_handshaking() {
        conn.complete_io(raw).map_err(unwrap_tls)?;
    }
    while conn.wants_write() {
        conn.write_tls(raw)?;
    }
    raw.flush()?;
    let mut conn = rustls::Connection::from(conn);
    conn.set_buffer_limit(None);
    Ok(conn)
}

/// rustls reports protocol failures as `InvalidData` around its own error.
pub(super) fn unwrap_tls(e: io::Error) -> io::Error {
    match e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    {
        Some(tls) => super::tls::error("TLS", tls.clone()),
        None => e,
    }
}

/// A connected, secured stream: plaintext in and out.
pub(crate) struct Conn {
    reader: ConnReader,
    writer: ConnWriter,
    raw: Raw,
}

impl Conn {
    pub(crate) fn plain(raw: Raw) -> io::Result<Self> {
        Ok(Self {
            reader: ConnReader {
                raw: raw.try_clone()?,
                tls: None,
            },
            writer: ConnWriter {
                raw: raw.try_clone()?,
                tls: None,
            },
            raw,
        })
    }

    pub(crate) fn tls(raw: Raw, conn: rustls::Connection) -> io::Result<Self> {
        let shared: Shared = Arc::new(Mutex::new(conn));
        Ok(Self {
            reader: ConnReader {
                raw: raw.try_clone()?,
                tls: Some(TlsIn {
                    conn: shared.clone(),
                    buf: vec![0; 64 << 10],
                    pos: 0,
                    len: 0,
                    eof: false,
                }),
            },
            writer: ConnWriter {
                raw: raw.try_clone()?,
                tls: Some(TlsOut {
                    conn: shared,
                    out: Vec::new(),
                }),
            },
            raw,
        })
    }

    pub(crate) fn raw(&self) -> &Raw {
        &self.raw
    }

    /// TLS exporter output that binds the token exchange to this session.
    pub(crate) fn binding(&self) -> io::Result<Vec<u8>> {
        match &self.writer.tls {
            None => Ok(Vec::new()),
            Some(t) => lock(&t.conn)
                .export_keying_material(vec![0u8; 32], EXPORTER_LABEL, None)
                .map_err(|e| super::tls::error("TLS exporter", e)),
        }
    }

    #[cfg(test)]
    pub(super) fn shared_tls(&self) -> Option<Shared> {
        self.writer.tls.as_ref().map(|t| t.conn.clone())
    }

    /// The reading half, the writing half, and the socket (to shut down).
    pub(crate) fn split(self) -> (ConnReader, ConnWriter, Raw) {
        (self.reader, self.writer, self.raw)
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

pub(crate) struct ConnReader {
    raw: Raw,
    tls: Option<TlsIn>,
}

struct TlsIn {
    conn: Shared,
    /// Ciphertext read from the socket and not yet fed to rustls.
    buf: Vec<u8>,
    pos: usize,
    len: usize,
    eof: bool,
}

impl Read for ConnReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let Some(t) = &mut self.tls else {
            return self.raw.read(out);
        };
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            {
                let mut c = lock(&t.conn);
                loop {
                    match c.reader().read(out) {
                        Ok(n) => return Ok(n),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        // The peer closed without close_notify: our frames
                        // are self-delimiting and checksummed, so this is EOF.
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
                        Err(e) => return Err(e),
                    }
                    if t.eof {
                        return Ok(0);
                    }
                    if t.pos == t.len {
                        break;
                    }
                    let n = c.read_tls(&mut &t.buf[t.pos..t.len])?;
                    t.pos += n;
                    c.process_new_packets()
                        .map_err(|e| super::tls::error("TLS", e))?;
                }
            }
            let n = self.raw.read(&mut t.buf)?;
            t.pos = 0;
            t.len = n;
            if n == 0 {
                t.eof = true;
                let mut c = lock(&t.conn);
                let _ = c.read_tls(&mut &[][..]);
            }
        }
    }
}

pub(crate) struct ConnWriter {
    raw: Raw,
    tls: Option<TlsOut>,
}

struct TlsOut {
    conn: Shared,
    out: Vec<u8>,
}

impl Write for ConnWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(t) = &mut self.tls else {
            return self.raw.write(buf);
        };
        let n = buf.len().min(CHUNK);
        {
            let mut c = lock(&t.conn);
            // Records the reader queued (a key-update answer) go first.
            c.writer().write_all(&buf[..n])?;
            while c.wants_write() {
                c.write_tls(&mut t.out)?;
            }
        }
        let result = self.raw.write_all(&t.out);
        t.out.clear();
        result.map(|()| n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.raw.flush()
    }
}
