//! Authentication and TLS for every connection: workers and clients to a
//! server, and replicas to each other.
//!
//! Two independent layers wrap the socket before its first protocol frame:
//!
//! - **TLS 1.3** (rustls with the ring provider). A listener presents its
//!   certificate. A listener that also has a CA requires the connecting side
//!   to present a certificate signed by it (mutual TLS). A connecting side
//!   verifies the listener's certificate against its CA and the host name or
//!   IP address it dialled. Replicas always use mutual TLS.
//! - **A shared-secret token**, proven with an HMAC-SHA256 challenge and
//!   response over both sides' fresh nonces, in both directions. The secret
//!   never crosses the wire, and under TLS the proofs are bound to the
//!   session, so they cannot be relayed into another connection. A Unix
//!   socket or loopback deployment can authenticate without certificates.
//!
//! Without either, the socket carries the plain protocol. Both
//! layers are stream wrappers: the server and the replicas see a stream that
//! reads and writes plaintext, and never the handshake.
//!
//! A token without TLS authenticates the start of each connection only. An
//! attacker on the network path can still read the traffic and inject into
//! an authenticated TCP stream; use TLS wherever the network is not trusted.

mod auth;
mod blocking;
mod stream;
mod tls;

pub(crate) use auth::HELLO as AUTH_HELLO;
pub(crate) use blocking::{Conn, ConnWriter, Raw};
pub(crate) use stream::{TlsRead, TlsStream, TlsWrite};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// How long a connecting side has to finish TLS and the token exchange.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Security settings for one side of a connection.
///
/// For a listener, [`tls`](Self::tls) needs a certificate and key, and its CA
/// (if any) makes client certificates mandatory. For a connecting side, TLS
/// needs a CA to verify the listener, and a certificate and key are sent
/// when the listener asks for them. A replica uses one value for both roles
/// and needs all three. The default is neither TLS nor a token.
#[derive(Clone, Debug, Default)]
pub struct Security {
    pub tls: Option<Tls>,
    pub token: Option<Token>,
}

impl Security {
    /// Neither TLS nor a token: connections carry the plain protocol.
    pub fn is_open(&self) -> bool {
        self.tls.is_none() && self.token.is_none()
    }

    /// Parses connection options, the part of an address after `?`:
    /// `ca=PATH`, `cert=PATH`, `key=PATH`, `name=SERVER_NAME` and
    /// `token-file=PATH`, separated by `&`. A CA turns TLS on. Unknown keys
    /// are refused, so a misspelt option never silently drops security.
    pub fn from_options(options: &str) -> io::Result<Self> {
        let mut ca = None;
        let mut cert = None;
        let mut key = None;
        let mut name = None;
        let mut token = None;
        for pair in options.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            if v.is_empty() {
                return Err(invalid(format!("connection option '{k}' needs a value")));
            }
            let slot = match k {
                "ca" => &mut ca,
                "cert" => &mut cert,
                "key" => &mut key,
                "name" => &mut name,
                "token-file" => &mut token,
                _ => {
                    return Err(invalid(format!(
                        "unknown connection option '{k}' (expected ca, cert, key, name, token-file or max-shards)"
                    )));
                }
            };
            if slot.replace(v.to_owned()).is_some() {
                return Err(invalid(format!("connection option '{k}' is given twice")));
            }
        }
        let tls = match (&ca, &cert, &key) {
            (None, None, None) if name.is_some() => {
                return Err(invalid("name= applies to TLS, which needs ca="));
            }
            (None, None, None) => None,
            (None, _, _) => {
                return Err(invalid(
                    "TLS needs ca=PATH to verify the server; cert= and key= alone are not enough",
                ));
            }
            _ => {
                let tls = Tls::from_pem_files(
                    cert.as_deref().map(Path::new),
                    key.as_deref().map(Path::new),
                    ca.as_deref().map(Path::new),
                )?;
                Some(match name {
                    Some(n) => tls.with_server_name(n),
                    None => tls,
                })
            }
        };
        Ok(Self {
            tls,
            token: token.map(Token::from_file).transpose()?,
        })
    }
}

/// Certificates for TLS 1.3: this side's chain and key, and the CA that
/// signs the other side's certificate.
#[derive(Clone)]
pub struct Tls {
    certs: Vec<CertificateDer<'static>>,
    key: Option<Arc<PrivateKeyDer<'static>>>,
    roots: Option<Arc<rustls::RootCertStore>>,
    server_name: Option<String>,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tls")
            .field("certificates", &self.certs.len())
            .field("key", &self.key.is_some())
            .field("ca", &self.roots.as_ref().map(|r| r.len()))
            .field("server_name", &self.server_name)
            .finish()
    }
}

impl Tls {
    /// Loads PEM files: `cert` holds this side's chain (leaf first), `key`
    /// its private key (PKCS#8, PKCS#1 or SEC1), `ca` one or more trusted
    /// CA certificates.
    pub fn from_pem_files(
        cert: Option<&Path>,
        key: Option<&Path>,
        ca: Option<&Path>,
    ) -> io::Result<Self> {
        let read = |path: Option<&Path>| -> io::Result<Option<(PathBuf, Vec<u8>)>> {
            path.map(|p| {
                std::fs::read(p)
                    .map(|bytes| (p.to_owned(), bytes))
                    .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", p.display())))
            })
            .transpose()
        };
        let (cert, key, ca) = (read(cert)?, read(key)?, read(ca)?);
        tls::load(
            cert.as_ref().map(|(p, b)| (p.as_path(), b.as_slice())),
            key.as_ref().map(|(p, b)| (p.as_path(), b.as_slice())),
            ca.as_ref().map(|(p, b)| (p.as_path(), b.as_slice())),
        )
    }

    /// Like [`from_pem_files`](Self::from_pem_files), from PEM text.
    pub fn from_pem(
        cert: Option<&[u8]>,
        key: Option<&[u8]>,
        ca: Option<&[u8]>,
    ) -> io::Result<Self> {
        let unnamed = Path::new("PEM");
        tls::load(
            cert.map(|b| (unnamed, b)),
            key.map(|b| (unnamed, b)),
            ca.map(|b| (unnamed, b)),
        )
    }

    /// The name a connecting side verifies in the listener's certificate,
    /// instead of the host part of the address it dialled.
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    fn has_identity(&self) -> bool {
        self.key.is_some()
    }

    /// Whether this side presents a certificate.
    pub(crate) fn has_identity_pub(&self) -> bool {
        self.has_identity()
    }

    /// Whether a CA was given, to verify the other side.
    pub(crate) fn has_roots(&self) -> bool {
        self.roots.is_some()
    }

    /// This identity, verifying the other side against `other`'s CA.
    pub(crate) fn with_roots_of(mut self, other: &Tls) -> Self {
        self.roots = other.roots.clone();
        self
    }
}

/// rustls settings for an HTTPS client (the server invoking a worker's
/// endpoint): TLS 1.3, `http/1.1`, verifying against `tls`'s CA and
/// presenting its certificate, if any, when asked (mutual TLS).
pub(crate) fn https_client(tls: &Tls) -> io::Result<Arc<rustls::ClientConfig>> {
    let mut c = (*tls::client_config(tls)?).clone();
    c.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(c))
}

/// rustls settings for an HTTPS listener (a self-hosted worker endpoint):
/// TLS 1.3, `http/1.1`; with a CA in `tls`, clients must present a
/// certificate it signed.
pub(crate) fn https_server(tls: &Tls) -> io::Result<Arc<rustls::ServerConfig>> {
    let mut c = (*tls::server_config(tls)?).clone();
    c.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(c))
}

/// A shared secret both sides prove they hold. Its bytes are never printed.
#[derive(Clone)]
pub struct Token(Arc<ring::hmac::Key>);

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Token(redacted)")
    }
}

impl Token {
    /// The shortest secret accepted. Use 32 random bytes, for example
    /// `openssl rand -hex 32`.
    pub const MIN_LEN: usize = 16;

    pub fn new(secret: &[u8]) -> io::Result<Self> {
        if secret.len() < Self::MIN_LEN {
            return Err(invalid(format!(
                "a token needs at least {} bytes; use 32 random bytes",
                Self::MIN_LEN
            )));
        }
        Ok(Self(Arc::new(ring::hmac::Key::new(
            ring::hmac::HMAC_SHA256,
            secret,
        ))))
    }

    /// Reads a token file. Leading and trailing whitespace (a final newline)
    /// is not part of the secret. Warns when other users can read the file.
    pub fn from_file(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)
            .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(path)
                && meta.permissions().mode() & 0o077 != 0
            {
                eprintln!(
                    "fusee: warning: token file {} is readable by other users; chmod 600 it",
                    path.display()
                );
            }
        }
        Self::new(bytes.trim_ascii())
            .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))
    }

    fn key(&self) -> &ring::hmac::Key {
        &self.0
    }
}

/// Which protocol a connection carries. The token exchange is bound to it,
/// so a proof for one protocol is never accepted by the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Purpose {
    /// A worker or client connecting to a server.
    Server = 1,
    /// A replica connecting to another member of its group.
    Replica = 2,
}

/// What a server-protocol token may do. The listener's own token
/// ([`Security::token`]) is a worker's; others are added with
/// [`Server::set_security_scoped`](crate::Server::set_security_scoped).
/// A connecting side proves its token as with a single one: the listener
/// finds which one it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Holds shards and runs workflows, and makes any call.
    Worker,
    /// Makes calls (start, signal, query, list, ...), holds no shard.
    Client,
    /// Makes calls that only read: list, describe, query, result, streams,
    /// types, metrics, info, limits, export.
    Read,
}

/// The calls a [`Scope::Read`] token may make.
pub(crate) const READ_CALLS: &[&str] = &[
    "list",
    "describe",
    "query",
    "result",
    "streamRead",
    "types",
    "metrics",
    "info",
    "limits",
    "export",
    "invocations",
];

/// The connecting side's identity in the token exchange when it is not a
/// replica.
pub(crate) const NO_SENDER: u8 = u8::MAX;

/// The accepting side of a listener, built once from its [`Security`].
pub(crate) struct Acceptor {
    tls: Option<Arc<rustls::ServerConfig>>,
    token: Option<Token>,
    /// Tokens with less than a worker's reach (server listeners only).
    scoped: Vec<(Token, Scope)>,
    purpose: Purpose,
}

/// A connection after the listener's handshake.
pub(crate) enum Secured<S> {
    Plain(S),
    Tls(TlsStream),
}

/// A TCP connection after the listener's TLS handshake, if it has TLS, and
/// before its token exchange.
enum Established {
    Plain(tokio::net::TcpStream),
    Tls(tokio::net::TcpStream, Box<rustls::Connection>),
}

enum Detached {
    Plain(std::net::TcpStream),
    Tls(std::net::TcpStream, Box<rustls::Connection>),
}

/// What [`Acceptor::handshake_tls`] hands to [`Acceptor::finish`]: the
/// socket, detached from any runtime, with its TLS state and its deadline.
pub(crate) struct Handshaken {
    established: Detached,
    deadline: tokio::time::Instant,
}

impl Acceptor {
    pub(crate) fn new(security: &Security, purpose: Purpose) -> io::Result<Self> {
        let tls = security
            .tls
            .as_ref()
            .map(|t| {
                if purpose == Purpose::Replica && t.roots.is_none() {
                    return Err(invalid(
                        "replica TLS needs a CA that signs every member's certificate",
                    ));
                }
                tls::server_config(t)
            })
            .transpose()?;
        Ok(Self {
            tls,
            token: security.token.clone(),
            scoped: Vec::new(),
            purpose,
        })
    }

    /// Adds tokens with less reach: a client's or a reader's.
    pub(crate) fn with_scoped(mut self, scoped: Vec<(Token, Scope)>) -> io::Result<Self> {
        if !scoped.is_empty() && self.token.is_none() {
            return Err(invalid(
                "client and read tokens need the worker token too (--token-file): \
                 without it, every connection is a worker's",
            ));
        }
        self.scoped = scoped;
        Ok(self)
    }

    pub(crate) fn open(purpose: Purpose) -> Self {
        Self {
            tls: None,
            token: None,
            scoped: Vec::new(),
            purpose,
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.tls.is_none() && self.token.is_none()
    }

    pub(crate) fn has_tls(&self) -> bool {
        self.tls.is_some()
    }

    pub(crate) fn has_token(&self) -> bool {
        self.token.is_some()
    }

    /// TLS (if configured), then the token exchange (if configured). For a
    /// replica listener with a token, also returns the member ID the
    /// connecting side declared in its authenticated hello.
    pub(crate) async fn accept_tcp(
        &self,
        sock: tokio::net::TcpStream,
    ) -> io::Result<(Secured<tokio::net::TcpStream>, Option<u8>)> {
        let work = async {
            let established = self.establish(sock).await?;
            self.authenticate(established).await
        };
        tokio::time::timeout(HANDSHAKE_TIMEOUT, work)
            .await
            .map_err(|_| timed_out())?
    }

    /// [`accept_tcp`](Self::accept_tcp) in two halves, for a caller that runs
    /// the TLS handshake, the expensive half, on another runtime than the
    /// one that reads the stream. This half returns the socket detached from
    /// the runtime; [`finish`](Self::finish) attaches it to the current one.
    pub(crate) async fn handshake_tls(
        &self,
        sock: tokio::net::TcpStream,
    ) -> io::Result<Handshaken> {
        let deadline = tokio::time::Instant::now() + HANDSHAKE_TIMEOUT;
        let established = tokio::time::timeout_at(deadline, self.establish(sock))
            .await
            .map_err(|_| timed_out())??;
        let established = match established {
            Established::Plain(s) => Detached::Plain(s.into_std()?),
            Established::Tls(s, conn) => Detached::Tls(s.into_std()?, conn),
        };
        Ok(Handshaken {
            established,
            deadline,
        })
    }

    /// The token exchange after [`handshake_tls`](Self::handshake_tls), on
    /// the runtime that will read the stream, within the same deadline.
    pub(crate) async fn finish(
        &self,
        handshaken: Handshaken,
    ) -> io::Result<(Secured<tokio::net::TcpStream>, Option<u8>)> {
        let Handshaken {
            established,
            deadline,
        } = handshaken;
        let established = match established {
            Detached::Plain(s) => Established::Plain(tokio::net::TcpStream::from_std(s)?),
            Detached::Tls(s, conn) => Established::Tls(tokio::net::TcpStream::from_std(s)?, conn),
        };
        tokio::time::timeout_at(deadline, self.authenticate(established))
            .await
            .map_err(|_| timed_out())?
    }

    async fn establish(&self, sock: tokio::net::TcpStream) -> io::Result<Established> {
        let Some(config) = &self.tls else {
            return Ok(Established::Plain(sock));
        };
        let conn = rustls::ServerConnection::new(config.clone())
            .map_err(|e| tls::error("TLS setup", e))?;
        let (sock, conn) = TlsStream::handshake_only(sock, conn.into()).await?;
        Ok(Established::Tls(sock, Box::new(conn)))
    }

    async fn authenticate(
        &self,
        established: Established,
    ) -> io::Result<(Secured<tokio::net::TcpStream>, Option<u8>)> {
        match established {
            Established::Plain(mut sock) => {
                let sender = self.respond(&mut sock, &[]).await?;
                Ok((Secured::Plain(sock), sender))
            }
            Established::Tls(sock, conn) => {
                let mut stream = TlsStream::wrap(sock, *conn);
                let binding = stream.binding()?;
                let sender = self.respond(&mut stream, &binding).await?;
                Ok((Secured::Tls(stream), sender))
            }
        }
    }

    async fn respond<S>(&self, stream: &mut S, binding: &[u8]) -> io::Result<Option<u8>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        self.respond_scoped(stream, binding).await.map(|(s, _)| s)
    }

    /// The token exchange, and the scope of the token the other side holds.
    async fn respond_scoped<S>(
        &self,
        stream: &mut S,
        binding: &[u8],
    ) -> io::Result<(Option<u8>, Scope)>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let Some(token) = &self.token else {
            return Ok((None, Scope::Worker));
        };
        let mut tokens = vec![token];
        tokens.extend(self.scoped.iter().map(|(t, _)| t));
        let (sender, i) = auth::respond_any(stream, &tokens, self.purpose, binding).await?;
        let scope = i.checked_sub(1).map_or(Scope::Worker, |i| self.scoped[i].1);
        Ok(((self.purpose == Purpose::Replica).then_some(sender), scope))
    }

    /// [`accept_tcp`](Self::accept_tcp) for a server listener: the
    /// connection and its token's scope.
    pub(crate) async fn accept_tcp_scoped(
        &self,
        sock: tokio::net::TcpStream,
    ) -> io::Result<(Secured<tokio::net::TcpStream>, Scope)> {
        let work = async {
            match self.establish(sock).await? {
                Established::Plain(mut sock) => {
                    let (_, scope) = self.respond_scoped(&mut sock, &[]).await?;
                    Ok((Secured::Plain(sock), scope))
                }
                Established::Tls(sock, conn) => {
                    let mut stream = TlsStream::wrap(sock, *conn);
                    let binding = stream.binding()?;
                    let (_, scope) = self.respond_scoped(&mut stream, &binding).await?;
                    Ok((Secured::Tls(stream), scope))
                }
            }
        };
        tokio::time::timeout(HANDSHAKE_TIMEOUT, work)
            .await
            .map_err(|_| timed_out())?
    }

    /// The token exchange on a Unix socket, and the token's scope. TLS is
    /// never used there: the socket file's permissions decide who can
    /// connect.
    pub(crate) async fn accept_unix_scoped(
        &self,
        mut sock: tokio::net::UnixStream,
    ) -> io::Result<(tokio::net::UnixStream, Scope)> {
        let (_, scope) =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, self.respond_scoped(&mut sock, &[]))
                .await
                .map_err(|_| timed_out())??;
        Ok((sock, scope))
    }
}

/// The connecting side, built once from its [`Security`].
#[derive(Clone)]
pub(crate) struct Connector {
    tls: Option<Arc<rustls::ClientConfig>>,
    server_name: Option<String>,
    token: Option<Token>,
    purpose: Purpose,
    sender: u8,
}

impl std::fmt::Debug for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("tls", &self.tls.is_some())
            .field("token", &self.token.is_some())
            .finish()
    }
}

impl Connector {
    pub(crate) fn new(security: &Security, purpose: Purpose, sender: u8) -> io::Result<Self> {
        let tls = security
            .tls
            .as_ref()
            .map(|t| {
                if purpose == Purpose::Replica && !t.has_identity() {
                    return Err(invalid(
                        "replica TLS needs this member's certificate and key",
                    ));
                }
                tls::client_config(t)
            })
            .transpose()?;
        Ok(Self {
            tls,
            server_name: security.tls.as_ref().and_then(|t| t.server_name.clone()),
            token: security.token.clone(),
            purpose,
            sender,
        })
    }

    pub(crate) fn is_open(&self) -> bool {
        self.tls.is_none() && self.token.is_none()
    }

    fn name(&self, address: &str) -> io::Result<rustls::pki_types::ServerName<'static>> {
        let host = match &self.server_name {
            Some(name) => name.as_str(),
            None => host_of(address),
        };
        rustls::pki_types::ServerName::try_from(host.to_owned())
            .map_err(|_| invalid(format!("'{host}' is not a valid TLS server name")))
    }

    /// Blocking: TLS over TCP (if configured), then the token exchange (if
    /// configured). The caller sets socket timeouts.
    pub(crate) fn secure(&self, mut raw: Raw, address: &str) -> io::Result<Conn> {
        let mut conn = match (&self.tls, &raw) {
            (None, _) => Conn::plain(raw)?,
            (Some(_), Raw::Unix(_)) => {
                return Err(invalid(
                    "TLS is not used over Unix sockets; drop ca=, cert= and key=",
                ));
            }
            (Some(config), Raw::Tcp(_)) => {
                let conn =
                    blocking::client_handshake(&mut raw, config.clone(), self.name(address)?)
                        .map_err(|e| context(e, &format!("TLS with {address}")))?;
                Conn::tls(raw, conn)?
            }
        };
        if let Some(token) = &self.token {
            let binding = conn.binding()?;
            auth::initiate_blocking(&mut conn, token, self.purpose, self.sender, &binding)?;
        }
        Ok(conn)
    }

    /// Asynchronous, for replica peers: TLS, then the token exchange.
    pub(crate) async fn secure_tcp(
        &self,
        sock: tokio::net::TcpStream,
        address: &str,
    ) -> io::Result<Secured<tokio::net::TcpStream>> {
        let work = async {
            let mut secured = match &self.tls {
                None => Secured::Plain(sock),
                Some(config) => {
                    let conn = rustls::ClientConnection::new(config.clone(), self.name(address)?)
                        .map_err(|e| tls::error("TLS setup", e))?;
                    Secured::Tls(TlsStream::handshake(sock, conn.into()).await?)
                }
            };
            if let Some(token) = &self.token {
                match &mut secured {
                    Secured::Plain(s) => {
                        auth::initiate(s, token, self.purpose, self.sender, &[]).await?
                    }
                    Secured::Tls(s) => {
                        let binding = s.binding()?;
                        auth::initiate(s, token, self.purpose, self.sender, &binding).await?
                    }
                }
            }
            Ok(secured)
        };
        tokio::time::timeout(HANDSHAKE_TIMEOUT, work)
            .await
            .map_err(|_| timed_out())?
    }
}

/// The members (a bit per index into `peers`) whose host, the part of their
/// replication address before the port, `cert` is valid for. A member's
/// certificate must already name its own host, since every other member
/// verifies it under that name when it dials; checking the same name on
/// streams it opens ties each stream to the members its certificate names.
pub(crate) fn members_named(cert: &CertificateDer<'_>, peers: &[String]) -> u32 {
    let Ok(parsed) = rustls::server::ParsedCertificate::try_from(cert) else {
        return 0;
    };
    let mut mask = 0;
    for (i, peer) in peers.iter().enumerate().take(32) {
        let named = rustls::pki_types::ServerName::try_from(host_of(peer))
            .is_ok_and(|name| rustls::client::verify_server_name(&parsed, &name).is_ok());
        if named {
            mask |= 1 << i;
        }
    }
    mask
}

/// The host part of `host:port` (`[v6]:port` too), for TLS name checks.
pub(crate) fn host_of(address: &str) -> &str {
    let host = address.rsplit_once(':').map_or(address, |(h, _)| h);
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// Prints the warning for a listener anyone on the network can use.
pub(crate) fn warn_if_exposed(what: &str, addr: SocketAddr, tls: bool, token: bool) {
    if addr.ip().is_loopback() || tls {
        return;
    }
    if token {
        eprintln!(
            "fusee {what}: warning: {addr} authenticates connections with a token but has no TLS; traffic is readable and can be altered on the network path"
        );
    } else {
        eprintln!(
            "fusee {what}: warning: {addr} has neither TLS nor a token; anyone who can reach it can read and change workflow state"
        );
    }
}

/// Logs a failed handshake, at most once every ten seconds per process, so
/// a scanner or a misconfigured peer cannot flood the log.
pub(crate) fn refused(what: std::fmt::Arguments<'_>, e: &io::Error) {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = fusee::local::monotonic_us() / 1000;
    let last = LAST.load(Ordering::Relaxed);
    if (last == 0 || now.saturating_sub(last) >= 10_000)
        && LAST
            .compare_exchange(last, now.max(1), Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        eprintln!("fusee {what}: {e}");
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn timed_out() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "the connection did not finish TLS and authentication in time",
    )
}

fn context(e: io::Error, what: &str) -> io::Error {
    io::Error::new(e.kind(), format!("{what}: {e}"))
}

#[cfg(test)]
mod tests;
