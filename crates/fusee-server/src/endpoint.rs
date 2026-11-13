//! Server addresses for workers and clients, and how to connect to them.
//!
//! ```text
//! address = target [ "?" options ]
//! target  = host:port | unix:/path.sock | replicas://host:port,host:port,host:port
//! options = see Security::from_options (ca, cert, key, name, token-file),
//!           max-shards=N: a worker holds at most N shards at once,
//!           shm=1: same-host shared memory (Unix sockets; off by default), and
//!           invocation=ID: an invoked worker (see crate::invoke)
//! ```
//!
//! A `replicas://` address finds a primary that can still commit, even when
//! a deposed member is reachable.
use crate::proto;
use crate::security::{Conn, Connector, NO_SENDER, Purpose, Raw, Security};
use fusee_core::frame;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

pub(crate) const REPLICAS: &str = "replicas://";

enum Target {
    Tcp(String),
    Unix(String),
    Replicas(Vec<String>),
}

pub(crate) struct Endpoint {
    target: Target,
    connector: Connector,
    /// The most shards a worker holds at once; 0: all.
    max_shards: u32,
    /// A worker asks for same-host shared memory (Unix sockets only).
    shm: bool,
    /// An invoked worker: the invocation it answers.
    invocation: Option<u64>,
}

impl Endpoint {
    /// Parses an address with its connection options.
    pub(crate) fn parse(address: &str) -> io::Result<Self> {
        let (target, options) = match address.split_once('?') {
            Some((target, options)) => (target, options),
            None => (address, ""),
        };
        let mut max_shards = None;
        let mut shm = None;
        let mut invocation = None;
        let mut security = Vec::new();
        for pair in options.split('&').filter(|p| !p.is_empty()) {
            if let Some(v) = pair.strip_prefix("invocation=") {
                let id = crate::invoke::parse_id(v).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invocation takes the request's invocation ID, not '{v}'"),
                    )
                })?;
                if invocation.replace(id).is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "connection option 'invocation' is given twice",
                    ));
                }
                continue;
            }
            if let Some(v) = pair.strip_prefix("shm=") {
                let on = match v {
                    "1" => true,
                    "0" => false,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("shm takes 1 or 0, not '{v}'"),
                        ));
                    }
                };
                if shm.replace(on).is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "connection option 'shm' is given twice",
                    ));
                }
                continue;
            }
            match pair.strip_prefix("max-shards=") {
                Some(v) => {
                    let n = v.parse::<u32>().ok().filter(|n| *n > 0).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("max-shards needs a positive number, not '{v}'"),
                        )
                    })?;
                    if max_shards.replace(n).is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "connection option 'max-shards' is given twice",
                        ));
                    }
                }
                None => security.push(pair),
            }
        }
        let mut endpoint = Self::new(target, &Security::from_options(&security.join("&"))?)?;
        endpoint.max_shards = max_shards.unwrap_or(0);
        endpoint.shm = shm.unwrap_or(false);
        endpoint.invocation = invocation;
        if endpoint.shm && !matches!(endpoint.target, Target::Unix(_)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shm=1 needs a unix: address (shared memory is for workers on the server's host)",
            ));
        }
        Ok(endpoint)
    }

    pub(crate) fn max_shards(&self) -> u32 {
        self.max_shards
    }

    /// Whether a worker asks for same-host shared memory.
    pub(crate) fn shm(&self) -> bool {
        self.shm
    }

    /// The invocation an invoked worker answers.
    pub(crate) fn invocation(&self) -> Option<u64> {
        self.invocation
    }

    /// An address without options, secured with `security`.
    pub(crate) fn with_security(address: &str, security: &Security) -> io::Result<Self> {
        if address.contains('?') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "give connection options in the address or as Security, not both",
            ));
        }
        Self::new(address, security)
    }

    fn new(address: &str, security: &Security) -> io::Result<Self> {
        let target = if let Some(path) = address.strip_prefix("unix:") {
            if security.tls.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "TLS is not used over Unix sockets; drop ca=, cert= and key=",
                ));
            }
            Target::Unix(path.to_owned())
        } else if let Some(list) = address.strip_prefix(REPLICAS) {
            let names: Vec<String> = list.split(',').map(str::to_owned).collect();
            if !matches!(names.len(), 3 | 5) || names.iter().any(|name| name.is_empty()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected three or five replica worker addresses",
                ));
            }
            Target::Replicas(names)
        } else {
            Target::Tcp(address.to_owned())
        };
        Ok(Self {
            target,
            connector: Connector::new(security, Purpose::Server, NO_SENDER)?,
            max_shards: 0,
            shm: false,
            invocation: None,
        })
    }

    pub(crate) fn is_replicas(&self) -> bool {
        matches!(self.target, Target::Replicas(_))
    }

    pub(crate) fn connect(&self) -> io::Result<Conn> {
        match &self.target {
            Target::Unix(path) => {
                let raw = Raw::Unix(
                    std::os::unix::net::UnixStream::connect(path)
                        .map_err(|e| no_server(&format!("unix:{path}"), e))?,
                );
                self.secure(raw, path, crate::security::HANDSHAKE_TIMEOUT)
            }
            Target::Tcp(address) => {
                let tcp = TcpStream::connect(address).map_err(|e| no_server(address, e))?;
                tcp.set_nodelay(true)?;
                self.secure(Raw::Tcp(tcp), address, crate::security::HANDSHAKE_TIMEOUT)
            }
            Target::Replicas(names) => self.connect_replicas(names),
        }
    }

    /// TLS and the token exchange under `timeout`; nothing for an open endpoint.
    fn secure(&self, raw: Raw, address: &str, timeout: Duration) -> io::Result<Conn> {
        if self.connector.is_open() {
            return Conn::plain(raw);
        }
        raw.set_timeouts(Some(timeout))?;
        let conn = self.connector.secure(raw, address)?;
        conn.raw().set_timeouts(None)?;
        Ok(conn)
    }

    fn connect_replicas(&self, names: &[String]) -> io::Result<Conn> {
        let addrs = names
            .iter()
            .map(|name| {
                name.to_socket_addrs()
                    .map(|v| v.map(|a| (name.as_str(), a)).collect::<Vec<_>>())
            })
            .collect::<io::Result<Vec<_>>>()?;
        let deadline = Instant::now() + Duration::from_secs(15);
        let timeout = Duration::from_millis(750);
        loop {
            for (name, addr) in addrs.iter().flatten() {
                let error = match TcpStream::connect_timeout(addr, timeout)
                    .and_then(|tcp| {
                        tcp.set_nodelay(true)?;
                        // Each member presents a certificate for its own name.
                        self.secure(Raw::Tcp(tcp), name, timeout)
                    })
                    .and_then(|mut conn| probe(&mut conn, timeout).map(|()| conn))
                {
                    Ok(conn) => return Ok(conn),
                    // Every member shares the token and CA: do not retry a refusal.
                    Err(e) if e.kind() == io::ErrorKind::PermissionDenied => return Err(e),
                    Err(e) => e,
                };
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("no worker endpoint has a committed quorum (last error: {error})"),
                    ));
                }
            }
            if addrs.iter().all(Vec::is_empty) {
                return Err(io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    "replica worker addresses did not resolve",
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn probe(stream: &mut Conn, timeout: Duration) -> io::Result<()> {
    stream.raw().set_timeouts(Some(timeout))?;
    let mut out = Vec::new();
    frame::encode(proto::req::QUORUM_PROBE, &[], &mut out);
    stream.write_all(&out)?;
    let mut expected = Vec::new();
    frame::encode(proto::resp::QUORUM_READY, &[], &mut expected);
    let mut response = vec![0; expected.len()];
    stream.read_exact(&mut response)?;
    if response != expected {
        // A member that requires a token answers anything else with an
        // error frame that says so (`id u64, message`).
        if response[..4] == frame::MAGIC[..] && response[4] == proto::resp::ERR {
            let len = u32::from_le_bytes([response[5], response[6], response[7], response[8]]);
            let mut body = vec![0; len.min(4096) as usize];
            stream.read_exact(&mut body)?;
            let mut r = fusee_core::wire::Reader::new(&body);
            let _ = r.u64();
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "the server refused the connection: {}",
                    r.str().unwrap_or("")
                ),
            ));
        }
        return Err(io::Error::other("unexpected quorum probe response"));
    }
    stream.raw().set_timeouts(None)
}

/// A connection that found no server: the message names the address and
/// how to start one.
pub(crate) fn no_server(address: &str, e: io::Error) -> io::Error {
    let hint = match e.kind() {
        io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound => format!(
            ". Start one with: fusee server --dir <data dir> --listen {}",
            address.trim_start_matches("unix:")
        ),
        _ => String::new(),
    };
    io::Error::new(
        e.kind(),
        format!("no fusee server at {address} ({e}){hint}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_shards_is_a_connection_option_next_to_security() {
        assert_eq!(Endpoint::parse("127.0.0.1:1").unwrap().max_shards(), 0);
        assert_eq!(
            Endpoint::parse("127.0.0.1:1?max-shards=3")
                .unwrap()
                .max_shards(),
            3
        );
        for bad in [
            "127.0.0.1:1?max-shards=0",
            "127.0.0.1:1?max-shards=x",
            "127.0.0.1:1?max-shards=",
            "127.0.0.1:1?max-shards=1&max-shards=2",
            "127.0.0.1:1?max-shards=1&shards=2",
            "127.0.0.1:1?shm=1",
            "unix:/tmp/x.sock?shm=yes",
            "unix:/tmp/x.sock?shm=1&shm=0",
        ] {
            assert!(Endpoint::parse(bad).is_err(), "{bad}");
        }
        assert_eq!(
            Endpoint::parse("127.0.0.1:1?invocation=00000000000000ab")
                .unwrap()
                .invocation(),
            Some(0xab)
        );
        assert_eq!(Endpoint::parse("127.0.0.1:1").unwrap().invocation(), None);
        for bad in [
            "127.0.0.1:1?invocation=ab",
            "127.0.0.1:1?invocation=0000000000000000",
            "127.0.0.1:1?invocation=00000000000000ab&invocation=00000000000000ab",
        ] {
            assert!(Endpoint::parse(bad).is_err(), "{bad}");
        }
        assert!(Endpoint::parse("unix:/tmp/x.sock?shm=1").unwrap().shm());
        assert!(!Endpoint::parse("unix:/tmp/x.sock?shm=0").unwrap().shm());
        assert!(!Endpoint::parse("unix:/tmp/x.sock").unwrap().shm());
    }
}
