//! Workers invoked over HTTP: suspend to zero.
//!
//! A server configured with [`Endpoint`]s ([`Server::set_invokers`]) sends
//! an HTTP `POST` to one when a shard that no worker holds has work due (a
//! client call, a message from another shard, a timer, or a holder that left
//! without saying what was next). The process that receives it (a function
//! on Lambda, Cloud Run or Vercel, or any HTTP server) hands the request to a
//! [`Handler`], which connects to the server as a worker for exactly that
//! invocation's shards, runs them until they are idle or the invocation's
//! budget ends, reports each shard's next due time and returns. The server
//! invokes again when more work comes; nothing stays up while workflows
//! sleep. Leases, epochs and fencing are those of every worker, so
//! exactly-once effects are unchanged. See `docs/server-mode.md`.
//!
//! [`Server::set_invokers`]: crate::Server::set_invokers

pub(crate) mod dispatch;
pub(crate) mod http;
pub(crate) mod worker;

use crate::security::Tls;
use fusee::Engine;
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;
use std::time::Duration;

pub use dispatch::{EndpointMetrics, InvokeMetrics};

/// The header that carries the request's signature.
pub const SIGNATURE_HEADER: &str = "fusee-signature";

/// How long a suspended run stays loaded in an invoked worker before it is
/// parked (written to the run store). Short: the worker leaves soon, and a
/// parked run costs the next invocation's open almost nothing.
pub const PARK_AFTER: Duration = Duration::from_millis(50);

/// How old a signature a handler accepts.
pub const SIGNATURE_TOLERANCE: Duration = Duration::from_secs(300);

/// Whether `FUSEE_INVOKE_TRACE` asks for a line per invocation event on
/// standard error.
pub(crate) fn tracing() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("FUSEE_INVOKE_TRACE").is_some_and(|v| v != "0"))
}

macro_rules! trace {
    ($($t:tt)*) => {
        if $crate::invoke::tracing() {
            eprintln!("fusee invoke: {}", format_args!($($t)*));
        }
    };
}
pub(crate) use trace;

/// An invocation ID as the request and the `invocation=` option carry it:
/// 16 lowercase hex digits.
pub fn format_id(id: u64) -> String {
    format!("{id:016x}")
}

/// Parses [`format_id`]'s output.
pub fn parse_id(s: &str) -> Option<u64> {
    (s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| u64::from_str_radix(s, 16).ok())
        .flatten()
        .filter(|id| *id != 0)
}

/// Where an invoked worker's invocation stands
/// ([`Remote::invocation_poll`](crate::remote::Remote::invocation_poll)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
    /// Work goes on: poll again.
    Running,
    /// Every held shard is idle and drained: close the engine and leave.
    Idle,
    /// The budget (less its margin) is over: close the engine and leave;
    /// shards with work left are handed back as due now.
    Expired,
    /// The connection to the server is gone: the lease ended with it.
    Lost,
}

impl Progress {
    pub fn as_str(self) -> &'static str {
        match self {
            Progress::Running => "running",
            Progress::Idle => "idle",
            Progress::Expired => "expired",
            Progress::Lost => "lost",
        }
    }
}

/// The body of an invocation request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub version: u32,
    /// The ID to connect with ([`format_id`]).
    pub invocation: String,
    /// The shards the invocation is for.
    pub shards: Vec<u32>,
    /// How long the invocation may take, from when the server sent it.
    pub budget_ms: u64,
    /// Why the shards are due: `call`, `message`, `timer`, `recovery`,
    /// `handback` or `code`.
    #[serde(default)]
    pub reasons: Vec<String>,
}

/// The `fusee-signature` value for `body` signed at `t_ms` (Unix ms):
/// `t=<t_ms>,v1=<hex HMAC-SHA256(secret, "<t_ms>." + body)>`.
pub fn sign(secret: &[u8], t_ms: u64, body: &[u8]) -> String {
    format!("t={t_ms},v1={}", hex(&mac(secret, t_ms, body)))
}

fn mac(secret: &[u8], t_ms: u64, body: &[u8]) -> Vec<u8> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
    let mut ctx = ring::hmac::Context::with_key(&key);
    ctx.update(t_ms.to_string().as_bytes());
    ctx.update(b".");
    ctx.update(body);
    ctx.sign().as_ref().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Checks a `fusee-signature` value against `body` at `now_ms`: the MAC
/// (in constant time) and the timestamp, within [`SIGNATURE_TOLERANCE`].
pub fn verify(secret: &[u8], header: &str, body: &[u8], now_ms: u64) -> Result<(), String> {
    let mut t = None;
    let mut sigs = Vec::new();
    for part in header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", v)) => t = v.parse::<u64>().ok(),
            Some(("v1", v)) => sigs.push(v),
            _ => {}
        }
    }
    let t = t.ok_or("the signature has no timestamp")?;
    let tolerance = SIGNATURE_TOLERANCE.as_millis() as u64;
    if t.abs_diff(now_ms) > tolerance {
        return Err("the signature is too old (or the clocks differ by over five minutes)".into());
    }
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
    let mut msg = Vec::with_capacity(body.len() + 21);
    msg.extend_from_slice(t.to_string().as_bytes());
    msg.push(b'.');
    msg.extend_from_slice(body);
    for s in sigs {
        if let Some(tag) = unhex(s)
            && ring::hmac::verify(&key, &msg, &tag).is_ok()
        {
            return Ok(());
        }
    }
    Err("the signature does not match".into())
}

/// A secret whose bytes are never printed.
#[derive(Clone)]
pub struct Secret(Arc<Vec<u8>>);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(redacted)")
    }
}

impl Secret {
    /// At least 16 bytes; use 32 random ones.
    pub fn new(bytes: impl AsRef<[u8]>) -> io::Result<Self> {
        let b = bytes.as_ref();
        if b.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "an invocation secret needs at least 16 bytes; use 32 random bytes",
            ));
        }
        Ok(Self(Arc::new(b.to_vec())))
    }

    /// Reads a secret file (surrounding whitespace is not part of it).
    pub fn from_file(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)
            .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
        Self::new(bytes.trim_ascii())
            .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))
    }

    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
}

/// An HTTP endpoint the server invokes workers at.
#[derive(Clone, Debug)]
pub struct Endpoint {
    /// `http://` or `https://`.
    pub url: String,
    /// Workflow types this endpoint has code for; empty: every type.
    pub types: Vec<String>,
    /// Signs each request ([`sign`]). `None` sends no signature: only for
    /// an endpoint that checks the server's client certificate instead.
    pub secret: Option<Secret>,
    /// Invocations in flight at once, at most.
    pub concurrency: u32,
    /// How long one invocation may take, at most the platform's limit.
    pub budget: Duration,
    /// Shards per invocation, at most; 0: no limit.
    pub max_shards: u32,
    /// How much longer than the budget the server waits for an answer
    /// before it fences the invocation. Default 30 s.
    pub grace: Duration,
    /// For `https://`: the CA that signs the endpoint's certificate (the
    /// system bundle if `None`), and the server's client certificate for
    /// mutual TLS.
    pub tls: Option<Tls>,
}

impl Endpoint {
    /// An endpoint with the defaults: every type, 16 invocations at once, a
    /// 60 s budget, no limit on shards per invocation.
    pub fn new(url: impl Into<String>, secret: Option<Secret>) -> Self {
        Self {
            url: url.into(),
            types: Vec::new(),
            secret,
            concurrency: 16,
            budget: Duration::from_secs(60),
            max_shards: 0,
            grace: Duration::from_secs(30),
            tls: None,
        }
    }

    pub fn types(mut self, types: &[&str]) -> Self {
        self.types = types.iter().map(|t| (*t).to_owned()).collect();
        self
    }

    pub fn concurrency(mut self, n: u32) -> Self {
        self.concurrency = n;
        self
    }

    pub fn budget(mut self, d: Duration) -> Self {
        self.budget = d;
        self
    }

    pub fn max_shards(mut self, n: u32) -> Self {
        self.max_shards = n;
        self
    }

    pub fn grace(mut self, d: Duration) -> Self {
        self.grace = d;
        self
    }

    pub fn tls(mut self, tls: Tls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// An endpoint from its configuration: `{"url": ..., "types": [...],
    /// "secretFile": PATH, "concurrency": N, "budgetMs": N, "shards": N,
    /// "graceMs": N, "ca": PATH, "cert": PATH, "key": PATH}`; only `url` is
    /// required. Unknown keys are refused, so a misspelt one never silently
    /// drops a setting.
    pub fn from_json(v: &serde_json::Value) -> io::Result<Self> {
        let bad = |m: String| io::Error::new(io::ErrorKind::InvalidInput, m);
        let obj = v
            .as_object()
            .ok_or_else(|| bad("an endpoint is a JSON object".into()))?;
        let known = [
            "url",
            "types",
            "secretFile",
            "concurrency",
            "budgetMs",
            "shards",
            "graceMs",
            "ca",
            "cert",
            "key",
        ];
        if let Some(k) = obj.keys().find(|k| !known.contains(&k.as_str())) {
            return Err(bad(format!(
                "unknown endpoint setting '{k}' (expected one of {})",
                known.join(", ")
            )));
        }
        let url = obj
            .get("url")
            .and_then(|u| u.as_str())
            .ok_or_else(|| bad("an endpoint needs a \"url\"".into()))?;
        let num = |k: &str| -> io::Result<Option<u64>> {
            obj.get(k)
                .map(|v| {
                    v.as_u64()
                        .ok_or_else(|| bad(format!("\"{k}\" takes a whole number")))
                })
                .transpose()
        };
        let path = |k: &str| -> io::Result<Option<std::path::PathBuf>> {
            obj.get(k)
                .map(|v| {
                    v.as_str()
                        .map(std::path::PathBuf::from)
                        .ok_or_else(|| bad(format!("\"{k}\" takes a file path")))
                })
                .transpose()
        };
        let secret = path("secretFile")?.map(Secret::from_file).transpose()?;
        let mut e = Self::new(url, secret);
        if let Some(t) = obj.get("types") {
            e.types = t
                .as_array()
                .and_then(|a| a.iter().map(|x| x.as_str().map(str::to_owned)).collect())
                .ok_or_else(|| bad("\"types\" takes a list of workflow types".into()))?;
        }
        if let Some(n) = num("concurrency")? {
            e.concurrency = u32::try_from(n).map_err(|_| bad("concurrency is too large".into()))?;
        }
        if let Some(n) = num("budgetMs")? {
            e.budget = Duration::from_millis(n);
        }
        if let Some(n) = num("shards")? {
            e.max_shards = u32::try_from(n).map_err(|_| bad("shards is too large".into()))?;
        }
        if let Some(n) = num("graceMs")? {
            e.grace = Duration::from_millis(n);
        }
        let (ca, cert, key) = (path("ca")?, path("cert")?, path("key")?);
        if ca.is_some() || cert.is_some() || key.is_some() {
            e.tls = Some(Tls::from_pem_files(
                cert.as_deref(),
                key.as_deref(),
                ca.as_deref(),
            )?);
        }
        if e.secret.is_none() && e.tls.as_ref().is_none_or(|t| !t.has_identity_pub()) {
            eprintln!(
                "fusee: warning: endpoint {url} has neither a secret nor a client certificate; \
                 anyone who can reach it can make it connect to the server"
            );
        }
        e.validate()?;
        Ok(e)
    }

    /// Endpoints from a configuration file: `{"endpoints": [...]}` (see
    /// [`from_json`](Self::from_json)).
    pub fn from_config_file(path: impl AsRef<std::path::Path>) -> io::Result<Vec<Self>> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
        let v: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: {e}", path.display()),
            )
        })?;
        v.get("endpoints")
            .and_then(|e| e.as_array())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: expected {{\"endpoints\": [...]}}", path.display()),
                )
            })?
            .iter()
            .map(Self::from_json)
            .collect()
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, m.to_owned());
        http::Url::parse(&self.url)?;
        if self.concurrency == 0 {
            return Err(bad("an endpoint's concurrency must be at least 1"));
        }
        if self.budget < Duration::from_secs(1) || self.budget > Duration::from_secs(24 * 3600) {
            return Err(bad("an endpoint's budget must be 1 s to 24 h"));
        }
        Ok(())
    }
}

/// System CA bundles tried for `https://` endpoints without a CA file.
const SYSTEM_CAS: &[&str] = &[
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/ssl/cert.pem",
    "/opt/homebrew/etc/openssl@3/cert.pem",
];

/// The TLS client settings for an endpoint.
pub(crate) fn client_tls(e: &Endpoint) -> io::Result<Option<Arc<rustls::ClientConfig>>> {
    if !http::Url::parse(&e.url)?.tls {
        return Ok(None);
    }
    let tls = match &e.tls {
        Some(t) if t.has_roots() => t.clone(),
        other => {
            let path = SYSTEM_CAS
                .iter()
                .find(|p| std::path::Path::new(p).exists())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "{}: no CA file given and no system CA bundle found; pass --invoke-ca",
                            e.url
                        ),
                    )
                })?;
            let roots = Tls::from_pem_files(None, None, Some(std::path::Path::new(path)))?;
            match other {
                Some(t) => t.clone().with_roots_of(&roots),
                None => roots,
            }
        }
    };
    crate::security::https_client(&tls).map(Some)
}

/// What a [`Handler`] answers: an HTTP status and a JSON body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Reply {
    fn json(status: u16, v: &serde_json::Value) -> Self {
        Self {
            status,
            body: v.to_string().into_bytes(),
        }
    }
}

type Register = Arc<dyn Fn(&Engine) + Send + Sync>;
type Configure = Arc<dyn Fn(&mut fusee::Config) + Send + Sync>;

/// The worker side of an invocation, for any HTTP framework: give it the
/// request's `fusee-signature` header and body, answer with its [`Reply`].
/// Each call opens a fresh engine and closes it before it returns; nothing
/// is kept between invocations.
#[derive(Clone)]
pub struct Handler {
    address: String,
    secret: Option<Secret>,
    register: Register,
    configure: Option<Configure>,
}

impl std::fmt::Debug for Handler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handler")
            .field("address", &self.address)
            .finish()
    }
}

impl Handler {
    /// `address`: the server, with this worker's connection options
    /// (`host:port?ca=...&token-file=...`). `secret`: the endpoint's signing
    /// secret. `register`: registers the workflows on each fresh engine.
    pub fn new(
        address: impl Into<String>,
        secret: impl AsRef<[u8]>,
        register: impl Fn(&Engine) + Send + Sync + 'static,
    ) -> io::Result<Self> {
        Ok(Self {
            address: address.into(),
            secret: Some(Secret::new(secret)?),
            register: Arc::new(register),
            configure: None,
        })
    }

    /// Accepts unsigned requests: only behind mutual TLS ([`serve`] with a
    /// CA) or on a network only the server reaches.
    pub fn unsigned(
        address: impl Into<String>,
        register: impl Fn(&Engine) + Send + Sync + 'static,
    ) -> Self {
        Self {
            address: address.into(),
            secret: None,
            register: Arc::new(register),
            configure: None,
        }
    }

    /// Adjusts each engine's configuration before it opens.
    pub fn configure(mut self, f: impl Fn(&mut fusee::Config) + Send + Sync + 'static) -> Self {
        self.configure = Some(Arc::new(f));
        self
    }

    /// Checks the signature and runs the invocation: 200 with a summary,
    /// 401 for a bad signature, 400 for a bad body, 503 when the server
    /// cannot be reached.
    pub async fn handle(&self, signature: Option<&str>, body: &[u8]) -> Reply {
        if let Some(secret) = &self.secret {
            let Some(sig) = signature else {
                return Reply::json(401, &serde_json::json!({"error": "unsigned request"}));
            };
            if let Err(e) = verify(secret.bytes(), sig, body, fusee::local::unix_ms()) {
                return Reply::json(401, &serde_json::json!({ "error": e }));
            }
        }
        let request: Request = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => {
                return Reply::json(400, &serde_json::json!({"error": e.to_string()}));
            }
        };
        match self.invoke(&request).await {
            Ok(summary) => Reply::json(200, &summary),
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => Reply::json(
                200,
                &serde_json::json!({"invocation": request.invocation, "skipped": e.to_string()}),
            ),
            Err(e) => Reply::json(503, &serde_json::json!({"error": e.to_string()})),
        }
    }

    /// Runs a verified invocation; the summary is the 200 reply's body.
    pub async fn invoke(&self, request: &Request) -> io::Result<serde_json::Value> {
        if parse_id(&request.invocation).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid invocation ID",
            ));
        }
        let started = std::time::Instant::now();
        let address = with_invocation(&self.address, &request.invocation);
        let (mut cfg, remote) = tokio::task::spawn_blocking(move || crate::remote_worker(&address))
            .await
            .map_err(io::Error::other)??;
        // Relayed messages wait for the code: the shards' threads run once
        // the engine opens, before `register` below.
        remote.hold_relay();
        // Runs park soon after they suspend, so the checkpoint at close
        // holds them parked and the next invocation's open stays cheap.
        cfg.park_after = PARK_AFTER;
        if let Some(c) = &self.configure {
            c(&mut cfg);
        }
        cfg.checkpoint_on_shutdown = true;
        let engine = match Engine::open(cfg).await {
            Ok(e) => e,
            Err(e) => {
                remote.finish_invocation();
                return Err(io::Error::other(e.to_string()));
            }
        };
        (self.register)(&engine);
        remote.release_relay();
        let open_ms = started.elapsed().as_secs_f64() * 1e3;
        let calls = tokio::spawn(crate::serve_calls(remote.clone(), engine.clone()));
        let progress = loop {
            match remote.invocation_poll(&[]) {
                Progress::Running => tokio::time::sleep(Duration::from_millis(5)).await,
                p => break p,
            }
        };
        let held = remote.held_shards();
        let steps = remote.inner().invoked.as_ref().map_or(0, |i| i.steps());
        let closed = engine.shutdown().await;
        remote.finish_invocation();
        calls.abort();
        let expired = remote.inner().invoked.as_ref().is_some_and(|i| i.expired());
        Ok(serde_json::json!({
            "invocation": request.invocation,
            "shards": held,
            "outcome": progress.as_str(),
            "expired": expired,
            "steps": steps,
            "openMs": open_ms,
            "ms": started.elapsed().as_secs_f64() * 1e3,
            "error": closed.err().map(|e| e.to_string()),
        }))
    }
}

/// `address` with `invocation=<id>` among its options.
pub fn with_invocation(address: &str, id: &str) -> String {
    let sep = if address.contains('?') { '&' } else { '?' };
    format!("{address}{sep}invocation={id}")
}

/// Serves `handler` over HTTP on `listener` until the task is cancelled:
/// a self-hosted endpoint. With `tls`, over HTTPS; a CA in `tls` requires
/// the server's client certificate (mutual TLS).
pub async fn serve(
    listener: tokio::net::TcpListener,
    handler: Handler,
    tls: Option<Tls>,
) -> io::Result<()> {
    serve_with(listener, tls, move |signature, body| {
        let handler = handler.clone();
        async move { handler.handle(signature.as_deref(), &body).await }
    })
    .await
}

/// Like [`serve`], answering each `POST` with `answer(signature header,
/// body)`: for a process that hands invocations on (to a child process, a
/// queue) instead of running them itself.
pub async fn serve_with<F, Fut>(
    listener: tokio::net::TcpListener,
    tls: Option<Tls>,
    answer: F,
) -> io::Result<()>
where
    F: Fn(Option<String>, Vec<u8>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Reply> + Send + 'static,
{
    let config = tls
        .as_ref()
        .map(crate::security::https_server)
        .transpose()?;
    loop {
        let (sock, _) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        let (answer, config) = (answer.clone(), config.clone());
        tokio::spawn(async move {
            let r = match config {
                Some(config) => match rustls::ServerConnection::new(config) {
                    Ok(conn) => {
                        match crate::security::TlsStream::handshake(sock, conn.into()).await {
                            Ok(mut s) => exchange(&mut s, &answer).await,
                            Err(e) => Err(e),
                        }
                    }
                    Err(e) => Err(io::Error::other(e.to_string())),
                },
                None => {
                    let mut sock = sock;
                    exchange(&mut sock, &answer).await
                }
            };
            let _ = r;
        });
    }
}

async fn exchange<S, F, Fut>(s: &mut S, answer: &F) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: Fn(Option<String>, Vec<u8>) -> Fut,
    Fut: Future<Output = Reply>,
{
    let req = match http::read_request(s).await {
        Ok(r) => r,
        Err(e) => {
            let body = serde_json::json!({"error": e.to_string()}).to_string();
            return http::write_response(s, 400, body.as_bytes()).await;
        }
    };
    if req.method != "POST" {
        return http::write_response(s, 405, br#"{"error":"POST only"}"#).await;
    }
    let signature = req.header(SIGNATURE_HEADER).map(str::to_owned);
    let reply = answer(signature, req.body).await;
    http::write_response(s, reply.status, &reply.body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signatures() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let body = br#"{"version":1}"#;
        let now = 1_800_000_000_000;
        let sig = sign(secret, now, body);
        assert!(sig.starts_with(&format!("t={now},v1=")));
        assert_eq!(verify(secret, &sig, body, now + 1000), Ok(()));
        // A tampered body, another secret, an old or future timestamp.
        assert!(verify(secret, &sig, br#"{"version":2}"#, now).is_err());
        assert!(verify(b"another secret of enough length", &sig, body, now).is_err());
        assert!(verify(secret, &sig, body, now + 301_000).is_err());
        assert!(verify(secret, &sig, body, now - 301_000).is_err());
        assert!(verify(secret, "v1=00", body, now).is_err());
        assert!(verify(secret, &format!("t={now},v1=zz"), body, now).is_err());
        // Known vector: HMAC-SHA256("key", "1.x").
        assert_eq!(
            hex(&mac(b"key", 1, b"x")),
            hex(ring::hmac::sign(
                &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"key"),
                b"1.x"
            )
            .as_ref())
        );
    }

    #[test]
    fn ids_and_addresses() {
        assert_eq!(parse_id(&format_id(0xabc)), Some(0xabc));
        assert_eq!(parse_id("0"), None);
        assert_eq!(parse_id("0000000000000000"), None);
        assert_eq!(parse_id("zzzzzzzzzzzzzzzz"), None);
        assert_eq!(
            with_invocation("h:1", "00000000000000ab"),
            "h:1?invocation=00000000000000ab"
        );
        assert_eq!(
            with_invocation("h:1?token-file=t", "00000000000000ab"),
            "h:1?token-file=t&invocation=00000000000000ab"
        );
    }
}
