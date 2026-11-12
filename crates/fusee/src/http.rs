//! Fusee's HTTP API: `/v1`, JSON, server-sent events for run changes and
//! streams, an OpenAPI document, Prometheus metrics, and the web UI.
//!
//! Every route turns into one of the protocol's [calls](calls), answered by
//! whatever [`Calls`] it serves: a Rust [`Engine`], a [`Relay`] to an SDK's
//! engine (`engine.serve()` in TypeScript and Python), or a server
//! (`fusee server --http`). It runs on its own task and reads what the
//! engine's existing entry points answer: nothing rides the step path.
//!
//! ```no_run
//! # async fn demo(engine: fusee::Engine) -> std::io::Result<()> {
//! let server = fusee::http::serve(engine, "127.0.0.1:7601", fusee::http::HttpOptions::default()).await?;
//! println!("UI at {}", server.url());
//! # Ok(()) }
//! ```
//!
//! | Route | Call |
//! | --- | --- |
//! | `GET /v1/runs?type=&status=&limit=&where=JSON` | `list` |
//! | `POST /v1/runs` `{type, id, input, idPolicy, timeout, version}` | `start` |
//! | `GET /v1/runs/{id}` | `describe` |
//! | `GET /v1/runs/{id}/result` | `result`, as `{status, value}` or `{status, error}` |
//! | `POST /v1/runs/{id}/signals/{name}` (body: the value) | `signal` |
//! | `POST /v1/runs/{id}/queries/{name}` (body: the argument) | `query` |
//! | `POST /v1/runs/{id}/updates/{name}?id=` (body: the argument) | `update` |
//! | `POST /v1/runs/{id}/cancel`, `/terminate` `{reason}` | `cancel`, `terminate` |
//! | `POST /v1/runs/{id}/reset` `{beforeStep, reapplySignals}` | `reset` |
//! | `POST /v1/runs/{id}/retry` | `reset` before the failed step |
//! | `POST /v1/runs/{id}/resume` | `resume`: a paused run's step runs again |
//! | `GET /v1/runs/{id}/streams/{name}?from=&wait=` | `streamRead`; `Accept: text/event-stream` follows it |
//! | `POST /v1/complete/{token}` (body: the value), `POST /v1/fail/{token}` (body: a failure) | `complete`, `fail` |
//! | `GET /v1/objects/{type}/{key}` | `describe` and the state query |
//! | `POST /v1/objects/{type}/{key}/calls/{handler}?id=` (body: the argument) | a call |
//! | `PUT`, `DELETE /v1/objects/{type}/{key}/state/{field}`; `PATCH` (a JSON Patch), `POST` (an edit), `DELETE /v1/objects/{type}/{key}/state`; `If-Match` | `editState` |
//! | `GET /v1/schedules`, `GET /v1/schedules/{name}` | `list`, the schedule's `describe` query |
//! | `POST /v1/schedules/{name}/pause`, `/resume`, `/trigger`; `DELETE /v1/schedules/{name}` | control signals |
//! | `GET /v1/types` | `types` |
//! | `GET /v1/limits` | `limits` |
//! | `GET /v1/metrics`, `GET /metrics` (Prometheus) | `metrics` |
//! | `GET /v1/info` | `info` |
//! | `GET /v1/events` | run changes, as server-sent events |
//! | `POST /v1/call/{method}` (body: the arguments) | any call, for clients without an SDK |
//! | `GET /v1/openapi.json` | this API |
//! | `GET /` and `/assets/...` | the web UI |
//!
//! **Security.** It listens on loopback unless told otherwise, and then
//! requires a token (`Authorization: Bearer ...`). A request that changes
//! something must say so in a way a cross-site page cannot forge: a JSON
//! body (`Content-Type: application/json`) or `X-Fusee-Action: 1`; and a
//! browser `Origin` must match the host. Without a token, a loopback API
//! answers only requests that name this machine (`localhost`, a loopback
//! address, or its public URL's host), against DNS rebinding.

pub mod calls;
mod client;
mod ui;

pub use calls::{Calls, Relay, RelayedCall, answer, decode_answer, error_json, run_call};
pub use client::HttpClient;

use crate::error::{Error, Result};
use serde_json::{Value, json};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How the API serves.
#[derive(Clone, Debug, Default)]
pub struct HttpOptions {
    /// Required as `Authorization: Bearer <token>` on every request. A
    /// listener off loopback needs one.
    pub token: Option<String>,
    /// Another full token: a client's (the server protocol's client
    /// scope). The HTTP API only makes calls, so it reaches all of it.
    pub client_token: Option<String>,
    /// A second token that only reads: it is accepted for `GET` requests
    /// (lists, runs, results, events, metrics) and refused for anything
    /// that changes something. For dashboards and on-call read access.
    pub read_token: Option<String>,
    /// Tokens that reach one namespace only, as `(token, namespace)`. A
    /// namespace is a prefix of the workflow ID: with `acme`'s token,
    /// calls name only IDs under `acme/`, lists show only those runs,
    /// and of the rest only info, types and limits answer.
    pub namespaces: Vec<(String, String)>,
    /// Listen off loopback without a token (only behind a proxy that
    /// authenticates).
    pub insecure: bool,
    /// The header in which a proxy in front names who signed in
    /// (`X-Forwarded-User`, `X-Auth-Request-Email`). Requests then need it
    /// or a token; it names the requester of signals, cancellations,
    /// terminations and resets in their records (audit), and the UI shows
    /// it. Trust it only behind that proxy.
    pub identity_header: Option<String>,
    /// Serve the web UI at `/` (on by default with the `ui` feature).
    pub no_ui: bool,
    /// The address callers outside see (for callback URLs), if not the
    /// listener's.
    pub public_url: Option<String>,
    /// Shown by `/v1/info` and the UI (`dev` under the dev profile).
    pub profile: Option<String>,
    /// The operator's codec endpoint (see [`serve_codec`]): the UI sends
    /// the payloads a codec encrypted there to show them decoded. The
    /// browser calls it directly, so this API never holds the keys or the
    /// decoded payloads.
    pub codec_url: Option<String>,
}

/// The origin (`scheme://host[:port]`) of an `http(s)` URL.
fn origin_of(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    if !matches!(scheme, "http" | "https") {
        return None;
    }
    let host = rest.split(['/', '?', '#']).next()?;
    (!host.is_empty()).then(|| format!("{scheme}://{host}"))
}

/// A running API server.
pub struct HttpServer {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
    opts: Option<Arc<std::sync::RwLock<Arc<HttpOptions>>>>,
}

impl HttpServer {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://host:port`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Stops serving.
    pub fn stop(self) {
        self.task.abort();
    }

    /// Replaces the tokens, namespaces and codec URL (on `SIGHUP`): new
    /// connections use them; open event streams keep theirs. The address
    /// and the UI do not change.
    pub fn reload(&self, opts: HttpOptions) -> io::Result<()> {
        check_options(&opts, self.addr)?;
        if let Some(shared) = &self.opts {
            *shared.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(opts);
        }
        Ok(())
    }
}

/// What makes options unusable on `addr`.
fn check_options(opts: &HttpOptions, addr: SocketAddr) -> io::Result<()> {
    if let Some(url) = &opts.codec_url
        && origin_of(url).is_none()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("the codec URL '{url}' is not an http(s) URL"),
        ));
    }
    if !addr.ip().is_loopback()
        && opts.token.is_none()
        && opts.namespaces.is_empty()
        && !opts.insecure
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "the HTTP API would listen on {addr}, off loopback, without a token: pass a token \
                 (--token-file), listen on 127.0.0.1, or (behind a proxy that authenticates) pass insecure"
            ),
        ));
    }
    Ok(())
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Binds `addr` and serves `calls` on a task of the current runtime.
pub async fn serve(calls: impl Calls, addr: &str, opts: HttpOptions) -> io::Result<HttpServer> {
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot listen on {addr} for the HTTP API: {e}"),
        )
    })?;
    serve_on(Arc::new(calls), listener, opts)
}

/// Serves on a listener the caller bound.
pub fn serve_on(
    calls: Arc<dyn Calls>,
    listener: TcpListener,
    opts: HttpOptions,
) -> io::Result<HttpServer> {
    let addr = listener.local_addr()?;
    check_options(&opts, addr)?;
    let shared = Arc::new(std::sync::RwLock::new(Arc::new(opts)));
    let current = shared.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            let calls = calls.clone();
            let opts = current.read().unwrap_or_else(|e| e.into_inner()).clone();
            tokio::spawn(async move {
                let _ = connection(calls, stream, &opts, addr).await;
            });
        }
    });
    Ok(HttpServer {
        addr,
        task,
        opts: Some(shared),
    })
}

/// Serves on a thread of its own, with its own runtime: for engines whose
/// host has no Tokio runtime (the SDKs).
pub fn serve_thread(calls: impl Calls, addr: &str, opts: HttpOptions) -> io::Result<SocketAddr> {
    let (tx, rx) = std::sync::mpsc::channel();
    let addr = addr.to_owned();
    std::thread::Builder::new()
        .name("fusee-http".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };
            rt.block_on(async move {
                match serve(calls, &addr, opts).await {
                    Ok(server) => {
                        let _ = tx.send(Ok(server.addr()));
                        // Serves until the process exits.
                        let _keep = server;
                        std::future::pending::<()>().await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            });
        })?;
    rx.recv()
        .map_err(|_| io::Error::other("the HTTP thread exited"))?
}

// ------------------------------------------------------------- codec server

/// How [`serve_codec`] serves.
#[derive(Clone, Debug, Default)]
pub struct CodecOptions {
    /// The UI's origin (`http://127.0.0.1:7601`): only its pages may call.
    pub allow_origin: String,
    /// Listen off loopback (only behind a proxy that signs people in: the
    /// endpoint decrypts for whoever reaches it).
    pub insecure: bool,
}

/// Serves a codec endpoint for the UI: `POST /decode` with
/// `{"payloads": [base64, ...]}` answers `{"payloads": [base64 | null, ...]}`,
/// each payload passed through `codec.decode` (null for one it cannot
/// decode). Point the API's [`HttpOptions::codec_url`] at it. The UI's
/// browser calls it directly, so the keys and the decoded payloads stay
/// with whoever runs it.
pub async fn serve_codec(
    codec: Arc<dyn crate::codec::Codec>,
    addr: &str,
    opts: CodecOptions,
) -> io::Result<HttpServer> {
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot listen on {addr} for the codec endpoint: {e}"),
        )
    })?;
    let addr = listener.local_addr()?;
    if !addr.ip().is_loopback() && !opts.insecure {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "the codec endpoint would listen on {addr}, off loopback: it decrypts for whoever \
                 reaches it, so listen on 127.0.0.1 or, behind a proxy that signs people in, pass insecure"
            ),
        ));
    }
    let Some(origin) = origin_of(&opts.allow_origin) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "the allowed origin '{}' is not an http(s) origin",
                opts.allow_origin
            ),
        ));
    };
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            let (codec, origin) = (codec.clone(), origin.clone());
            tokio::spawn(async move {
                let Ok(Some(req)) = read_request(&mut stream).await else {
                    return;
                };
                let mut resp = codec_answer(&*codec, &origin, &req);
                resp.extra.extend([
                    ("Access-Control-Allow-Origin", origin.clone()),
                    ("Access-Control-Allow-Methods", "POST".into()),
                    ("Access-Control-Allow-Headers", "content-type".into()),
                    ("Access-Control-Allow-Credentials", "true".into()),
                    ("Vary", "Origin".into()),
                ]);
                let _ = write_response(&mut stream, resp).await;
            });
        }
    });
    Ok(HttpServer {
        addr,
        task,
        opts: None,
    })
}

fn codec_answer(codec: &dyn crate::codec::Codec, origin: &str, req: &Request) -> Response {
    // Browsers send their page's origin on every cross-site call.
    if req.header("origin").is_some_and(|o| o != origin) {
        return Response::error(403, "Rejected", "a page from another origin is refused");
    }
    match (req.method.as_str(), req.path.trim_end_matches('/')) {
        ("OPTIONS", _) => Response {
            status: 204,
            content_type: "text/plain",
            body: Vec::new(),
            extra: Vec::new(),
        },
        ("POST", "/decode") => {
            let body = match req.json_body() {
                Ok(b) => b,
                Err(e) => return Response::of(&e),
            };
            let Some(payloads) = body["payloads"].as_array() else {
                return Response::error(400, "Rejected", "expected {\"payloads\": [base64, ...]}");
            };
            let out: Vec<Value> = payloads
                .iter()
                .map(|p| {
                    p.as_str()
                        .and_then(calls::unbase64)
                        .and_then(|b| codec.decode(&b).ok())
                        .map_or(Value::Null, |plain| json!(calls::base64(&plain)))
                })
                .collect();
            Response::json(200, &json!({ "payloads": out }))
        }
        _ => Response::error(404, "NotFound", "the codec endpoint answers POST /decode"),
    }
}

// ------------------------------------------------------------------ requests

pub(crate) struct Request {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn query(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    }

    fn json_body(&self) -> Result<Value> {
        if self.body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&self.body)
            .map_err(|e| Error::Rejected(format!("the request body is not JSON: {e}")))
    }
}

pub(crate) struct Response {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
    pub extra: Vec<(&'static str, String)>,
}

impl Response {
    pub fn json(status: u16, v: &Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: serde_json::to_vec(v).unwrap_or_default(),
            extra: Vec::new(),
        }
    }

    fn error(status: u16, code: &str, message: &str) -> Self {
        Self::json(
            status,
            &json!({"error": {"code": code, "message": message, "docs": crate::error::docs_link(code)}}),
        )
    }

    fn of(e: &Error) -> Self {
        let status = match e {
            Error::NotFound(_) => 404,
            Error::AlreadyExists(_) => 409,
            Error::Rejected(m) if m.starts_with(calls::OUTSIDE_NAMESPACE) => 403,
            Error::Rejected(_) | Error::Codec(_) | Error::UnknownWorkflow(_) => 400,
            Error::Busy => 429,
            Error::Closed | Error::Connection(_) => 503,
            Error::Cancelled(_) | Error::TimedOut | Error::Terminated | Error::Determinism(_) => {
                409
            }
            _ => 500,
        };
        let mut body =
            json!({"error": {"code": e.code(), "message": e.to_string(), "docs": e.docs()}});
        if e.failure().is_some() {
            body["error"]["failure"] = serde_json::to_value(e.to_failure()).unwrap_or(Value::Null);
        }
        Self::json(status, &body)
    }

    fn result(r: Result<Value>) -> Self {
        match r {
            Ok(v) => Self::json(200, &v),
            Err(e) => Self::of(&e),
        }
    }
}

const MAX_HEAD: usize = 64 << 10;
const MAX_BODY: usize = 8 << 20;

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Error",
    }
}

async fn connection(
    all: Arc<dyn Calls>,
    mut stream: TcpStream,
    opts: &HttpOptions,
    addr: SocketAddr,
) -> io::Result<()> {
    let _ = stream.set_nodelay(true);
    let Some(req) = read_request(&mut stream).await? else {
        return Ok(());
    };
    if let Some(refusal) = refuse(&req, opts, addr) {
        return write_response(&mut stream, refusal).await;
    }
    // A namespace's token reaches that namespace only: every route goes
    // through these calls.
    let scoped: Arc<dyn Calls> = match namespace_of(&req, opts) {
        Some(ns) => Arc::new(calls::Namespaced::new(all, &ns)),
        None => all,
    };
    // Who asked, on the records of the requests that change a run.
    let trace = req
        .header("traceparent")
        .filter(|t| calls::is_traceparent(t))
        .map(str::to_owned);
    let scoped: Arc<dyn Calls> =
        Arc::new(calls::Stamped::new(scoped, requester(&req, opts), trace));
    let calls: &dyn Calls = &*scoped;
    let decoded: Vec<String> = req.path.trim_matches('/').split('/').map(decode).collect();
    let parts: Vec<&str> = decoded.iter().map(String::as_str).collect();
    // Streams of events hold the connection.
    let sse = req
        .header("accept")
        .is_some_and(|a| a.contains("text/event-stream"));
    match (req.method.as_str(), parts.as_slice()) {
        ("GET", ["v1", "events"]) => return events(calls, &req, &mut stream).await,
        ("GET", ["v1", "runs", id, "streams", name]) if sse => {
            return stream_events(calls, &req, id, name, &mut stream).await;
        }
        _ => {}
    }
    let resp = route(calls, &req, &parts, opts, addr).await;
    write_response(&mut stream, resp).await
}

/// The token a request carries: `Authorization: Bearer`, or `?token=` for
/// event streams, which cannot send headers.
fn given_token(req: &Request) -> Option<&str> {
    req.header("authorization")
        .and_then(|h| h.strip_prefix("Bearer "))
        .or_else(|| req.query("token"))
}

/// Who is asking (audit): the proxy's identity header, else the kind of
/// token the request carries.
fn requester(req: &Request, opts: &HttpOptions) -> String {
    if let Some(h) = &opts.identity_header
        && let Some(who) = req.header(h).map(str::trim).filter(|v| !v.is_empty())
    {
        return who.chars().take(200).collect();
    }
    let given = given_token(req);
    let is = |t: &Option<String>| matches!((given, t), (Some(g), Some(t)) if constant_eq(g.as_bytes(), t.as_bytes()));
    if let Some(ns) = namespace_of(req, opts) {
        format!("token:namespace:{ns}")
    } else if is(&opts.token) {
        "token".into()
    } else if is(&opts.client_token) {
        "token:client".into()
    } else {
        "http".into()
    }
}

/// The namespace the request's token is limited to, if it is a namespace's.
fn namespace_of(req: &Request, opts: &HttpOptions) -> Option<String> {
    let given = given_token(req)?;
    opts.namespaces
        .iter()
        .find(|(t, _)| constant_eq(given.as_bytes(), t.as_bytes()))
        .map(|(_, ns)| ns.clone())
}

/// The host of a `Host` header: `name`, `name:port` or `[v6]:port`.
fn host_name(host: &str) -> &str {
    if let Some(rest) = host.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    match host.rsplit_once(':') {
        Some((h, port)) if port.bytes().all(|b| b.is_ascii_digit()) => h,
        _ => host,
    }
}

/// `localhost`, a name under it, or a loopback address.
fn is_loopback_name(name: &str) -> bool {
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    name == "localhost"
        || name.ends_with(".localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// An API on loopback that needs no token trusts whoever reaches it, which
/// a page on another site can do by pointing its own name at 127.0.0.1
/// (DNS rebinding): the browser then sends that name as `Host` and as the
/// `Origin`, which match. Such a request is refused unless it names this
/// machine, or the host of the API's public URL.
fn rebinding(req: &Request, opts: &HttpOptions, addr: SocketAddr) -> Option<Response> {
    let open = opts.token.is_none()
        && opts.client_token.is_none()
        && opts.read_token.is_none()
        && opts.namespaces.is_empty()
        && opts.identity_header.is_none();
    if !open || !addr.ip().is_loopback() {
        return None;
    }
    let name = host_name(req.header("host")?);
    let public = opts
        .public_url
        .as_deref()
        .and_then(origin_of)
        .and_then(|o| o.split_once("://").map(|(_, h)| host_name(h).to_owned()));
    if is_loopback_name(name) || public.is_some_and(|p| p.eq_ignore_ascii_case(name)) {
        return None;
    }
    Some(Response::error(
        403,
        "Rejected",
        &format!(
            "this API has no token and serves this machine only, but the request is for host              '{name}' (a page on another site can reach it that way): open it as              http://localhost:{port}, or give it a token or a public URL",
            port = addr.port()
        ),
    ))
}

/// Checks the token and that a change cannot come from another site.
fn refuse(req: &Request, opts: &HttpOptions, addr: SocketAddr) -> Option<Response> {
    if let Some(refusal) = rebinding(req, opts, addr) {
        return Some(refusal);
    }
    let changes = !matches!(req.method.as_str(), "GET" | "HEAD" | "OPTIONS");
    // Signed in at the proxy in front (see `identity_header`).
    let named = opts
        .identity_header
        .as_ref()
        .is_some_and(|h| req.header(h).is_some_and(|v| !v.trim().is_empty()));
    if opts.token.is_some()
        || opts.client_token.is_some()
        || opts.read_token.is_some()
        || !opts.namespaces.is_empty()
    {
        let given = given_token(req);
        let is = |t: &Option<String>| matches!((given, t), (Some(g), Some(t)) if constant_eq(g.as_bytes(), t.as_bytes()));
        let read = is(&opts.read_token);
        // A namespace's token is full within its namespace.
        let full =
            is(&opts.token) || is(&opts.client_token) || namespace_of(req, opts).is_some() || named;
        // The UI's page and its assets load without the token; their calls
        // carry it.
        let page = req.method == "GET" && (req.path == "/" || req.path.starts_with("/assets/"));
        if !full && !read && !page {
            return Some(Response::error(
                401,
                "Rejected",
                "this API needs its token: Authorization: Bearer <token>",
            ));
        }
        if read && !full && changes {
            return Some(Response::error(
                403,
                "Rejected",
                "this token only reads: changes need the full token",
            ));
        }
    }
    // Behind a proxy that signs people in, every call names who.
    if let Some(h) = &opts.identity_header {
        let page = req.method == "GET" && (req.path == "/" || req.path.starts_with("/assets/"));
        let tokened = given_token(req).is_some_and(|g| {
            [&opts.token, &opts.client_token, &opts.read_token]
                .iter()
                .any(|t| {
                    t.as_deref()
                        .is_some_and(|t| constant_eq(g.as_bytes(), t.as_bytes()))
                })
        }) || namespace_of(req, opts).is_some();
        if !page && !tokened && !named {
            return Some(Response::error(
                401,
                "Rejected",
                &format!(
                    "this API is behind a proxy that signs people in: the request has no {h} header"
                ),
            ));
        }
    }
    if let (Some(origin), Some(host)) = (req.header("origin"), req.header("host"))
        && origin.split("://").nth(1) != Some(host)
    {
        return Some(Response::error(
            403,
            "Rejected",
            "a request from another site's page is refused",
        ));
    }
    if changes {
        let json = req
            .header("content-type")
            .is_some_and(|c| c.starts_with("application/json"));
        if !json && req.header("x-fusee-action") != Some("1") {
            return Some(Response::error(
                403,
                "Rejected",
                "a request that changes something sends Content-Type: application/json (or X-Fusee-Action: 1)",
            ));
        }
    }
    None
}

fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn write_response(stream: &mut TcpStream, resp: Response) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n",
        resp.status,
        reason(resp.status),
        resp.content_type,
        resp.body.len()
    );
    for (k, v) in &resp.extra {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&resp.body).await?;
    stream.shutdown().await
}

async fn read_request(stream: &mut TcpStream) -> io::Result<Option<Request>> {
    let mut buf = Vec::with_capacity(4096);
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        if buf.len() > MAX_HEAD {
            return Ok(None);
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or_default().split(' ');
    let method = first.next().unwrap_or_default().to_owned();
    let target = first.next().unwrap_or_default();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target, ""),
    };
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        .collect();
    let len: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    if len > MAX_BODY {
        return Ok(None);
    }
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < len {
        let mut chunk = vec![0u8; (len - body.len()).min(64 << 10)];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(len);
    Ok(Some(Request {
        method,
        path: path.to_owned(),
        query: query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| match p.split_once('=') {
                Some((k, v)) => (decode(k), decode(v)),
                None => (decode(p), String::new()),
            })
            .collect(),
        headers,
        body,
    }))
}

/// Percent-decoding (and `+` as space).
pub(crate) fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && i + 3 <= bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(v) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(if b == b'+' { b' ' } else { b });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Percent-encoding of one path segment.
pub fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ------------------------------------------------------------------ routes

const SCHEDULE_TYPE: &str = "fusee.schedule";
const SCHEDULE_CONTROL: &str = "fusee.schedule.control";

fn list_filter(req: &Request) -> Result<Value> {
    let mut filter = match req.query("where").filter(|f| !f.is_empty()) {
        Some(f) => serde_json::from_str::<Value>(f)
            .map_err(|e| Error::Rejected(format!("where is not JSON: {e}")))?,
        None => json!({}),
    };
    for key in ["type", "status", "id"] {
        if let Some(v) = req.query(key).filter(|v| !v.is_empty()) {
            filter[key] = json!(v);
        }
    }
    Ok(filter)
}

async fn route(
    calls: &dyn Calls,
    req: &Request,
    parts: &[&str],
    opts: &HttpOptions,
    addr: SocketAddr,
) -> Response {
    let body = || req.json_body();
    let limit = || {
        req.query("limit")
            .and_then(|l| l.parse::<usize>().ok())
            .unwrap_or(200)
            .min(10_000)
    };
    let r: Result<Value> = match (req.method.as_str(), parts) {
        ("GET", [""] | ["index.html"]) if !opts.no_ui => return ui::page(opts),
        ("GET", ["assets", file]) if !opts.no_ui => return ui::asset(file),
        ("GET", ["metrics"]) => {
            return match calls.call("metrics", json!({})).await {
                Ok(m) => Response {
                    status: 200,
                    content_type: "text/plain; version=0.0.4",
                    body: prometheus_json(&m).into_bytes(),
                    extra: Vec::new(),
                },
                Err(e) => Response::of(&e),
            };
        }
        ("GET", ["v1", "openapi.json"]) => Ok(openapi()),
        ("GET", ["v1", "info"]) => calls.call("info", json!({})).await.map(|mut v| {
            v["http"] = json!({
                "url": opts.public_url.clone().unwrap_or_else(|| format!("http://{addr}")),
                "token": opts.token.is_some(),
            });
            if let Some(p) = &opts.profile {
                v["profile"] = json!(p);
            }
            if let Some(c) = &opts.codec_url {
                v["codecUrl"] = json!(c.trim_end_matches('/'));
            }
            v["viewer"] = json!(requester(req, opts));
            v
        }),
        ("GET", ["v1", "types"]) => calls.call("types", json!({})).await,
        ("GET", ["v1", "metrics"]) => calls.call("metrics", json!({})).await,
        ("GET", ["v1", "limits"]) => calls.call("limits", json!({})).await,
        ("POST", ["v1", "call", method]) => match body() {
            Ok(args) => calls.call(method, args).await,
            Err(e) => Err(e),
        },
        ("GET", ["v1", "runs"]) => match list_filter(req) {
            Ok(filter) => {
                let n = limit();
                calls
                    .call("list", json!({"where": filter, "limit": n}))
                    .await
                    .map(|mut rows| {
                        if let Value::Array(a) = &mut rows {
                            a.sort_by_key(|r| std::cmp::Reverse(r["startedMs"].as_u64().unwrap_or(0)));
                            a.truncate(n);
                        }
                        rows
                    })
            }
            Err(e) => Err(e),
        },
        ("POST", ["v1", "runs"]) => match body() {
            Ok(b) => {
                let id = b["id"].clone();
                let args = json!({
                    "type": b["type"], "id": id, "input": b["input"], "version": b["version"],
                    "options": {"timeout": b["timeout"], "idPolicy": b["idPolicy"]},
                });
                calls.call("start", args).await.map(|_| json!({ "id": id }))
            }
            Err(e) => Err(e),
        },
        ("GET", ["v1", "runs", id]) => calls.call("describe", json!({ "id": id })).await,
        ("GET", ["v1", "runs", id, "result"]) => {
            return match calls.call("result", json!({ "id": id })).await {
                Ok(v) => Response::json(200, &json!({"status": "ok", "value": v})),
                // How the run ended. Any other error is the request's (a
                // refused token, an engine closed, no worker for the shard),
                // not the run's, and answers as one.
                Err(e) if is_outcome(&e) => Response::json(
                    200,
                    &json!({"status": status_word(&e), "error": serde_json::to_value(e.to_failure()).unwrap_or(Value::Null)}),
                ),
                Err(e) => Response::of(&e),
            };
        }
        ("POST", ["v1", "runs", id, "signals", name]) => match body() {
            Ok(v) => calls
                .call("signal", json!({"id": id, "name": name, "value": v}))
                .await,
            Err(e) => Err(e),
        },
        ("POST", ["v1", "runs", id, "queries", name]) => match body() {
            Ok(v) => calls
                .call("query", json!({"id": id, "name": name, "arg": v}))
                .await,
            Err(e) => Err(e),
        },
        ("POST", ["v1", "runs", id, "updates", name]) => match body() {
            Ok(v) => {
                let uid = req.query("id").unwrap_or_default();
                calls
                    .call("update", json!({"id": id, "name": name, "arg": v, "updateId": uid}))
                    .await
            }
            Err(e) => Err(e),
        },
        ("POST", ["v1", "runs", id, action @ ("cancel" | "terminate")]) => match body() {
            Ok(b) => {
                let reason = b["reason"].as_str().unwrap_or(if *action == "cancel" {
                    "cancelled"
                } else {
                    "terminated"
                });
                calls
                    .call(action, json!({"id": id, "reason": reason}))
                    .await
            }
            Err(e) => Err(e),
        },
        ("POST", ["v1", "runs", id, "reset"]) => match body() {
            Ok(b) => calls
                .call(
                    "reset",
                    json!({"id": id, "beforeStep": b["beforeStep"], "reapplySignals": b["reapplySignals"].as_bool().unwrap_or(true)}),
                )
                .await,
            Err(e) => Err(e),
        },
        ("POST", ["v1", "runs", id, "retry"]) => retry(calls, id).await,
        ("POST", ["v1", "runs", id, "resume"]) => calls.call("resume", json!({ "id": id })).await,
        ("GET", ["v1", "runs", id, "export"]) => {
            return match calls.call("export", json!({ "id": id })).await {
                Ok(v) => match v["file"].as_str().and_then(calls::unbase64) {
                    Some(file) => Response {
                        status: 200,
                        content_type: "application/octet-stream",
                        body: file,
                        extra: vec![(
                            "Content-Disposition",
                            format!(
                                "attachment; filename=\"{}\"",
                                v["name"].as_str().unwrap_or("run.fjnl").replace('"', "")
                            ),
                        )],
                    },
                    None => Response::error(500, "Io", "the export was not base64"),
                },
                Err(e) => Response::of(&e),
            };
        }
        ("GET", ["v1", "runs", id, "streams", name]) => {
            let num = |k: &str| req.query(k).and_then(|v| v.parse::<u64>().ok());
            calls
                .call(
                    "streamRead",
                    json!({"id": id, "run": num("run"), "name": name, "from": num("from").unwrap_or(0),
                        "waitMs": num("wait").unwrap_or(0).min(25_000)}),
                )
                .await
        }
        ("POST", ["v1", "complete", token]) => match body() {
            Ok(v) => calls
                .call("complete", json!({"token": token, "value": v}))
                .await,
            Err(e) => Err(e),
        },
        ("POST", ["v1", "fail", token]) => match body() {
            Ok(v) => {
                let error = match v {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                calls
                    .call("fail", json!({"token": token, "error": error}))
                    .await
            }
            Err(e) => Err(e),
        },
        ("GET", ["v1", "objects", wtype, key @ ..]) if !key.is_empty() => {
            let id = format!("{wtype}/{}", key.join("/"));
            match calls.call("describe", json!({ "id": id })).await {
                Ok(mut d) => {
                    if let Ok(state) = calls
                        .call("query", json!({"id": id, "name": crate::object::STATE_QUERY, "arg": null}))
                        .await
                    {
                        d["state"] = state;
                    }
                    Ok(d)
                }
                Err(e) => Err(e),
            }
        }
        // Edits of a key's state: `.../state` (POST an edit, PATCH a JSON
        // Patch, DELETE all of it) and `.../state/{field}` (PUT its value,
        // DELETE it). `If-Match` is the version the edit expects.
        (method @ ("POST" | "PATCH" | "PUT" | "DELETE"), ["v1", "objects", wtype, rest @ ..])
            if state_route(method, rest).is_some() =>
        {
            let (key, field) = state_route(method, rest).unwrap_or_default();
            let edit = match (method, field) {
                ("POST", None) => body(),
                ("PATCH", None) => body().map(|ops| json!({ "patch": ops })),
                ("PUT", Some(f)) => body().map(|v| json!({ "set": { f: v } })),
                ("DELETE", Some(f)) => Ok(json!({ "clear": [f] })),
                ("DELETE", None) => Ok(json!({ "clearAll": true })),
                _ => Err(Error::Rejected("no such state route".into())),
            };
            let edit = edit.map(|mut e| {
                if let Some(v) = req.header("if-match") {
                    e["ifVersion"] = json!(v.trim_matches('"'));
                }
                e
            });
            let r = match edit {
                Ok(edit) => {
                    calls
                        .call(
                            "editState",
                            json!({"id": format!("{wtype}/{key}"), "edit": edit,
                                "updateId": req.query("id").unwrap_or_default()}),
                        )
                        .await
                }
                Err(e) => Err(e),
            };
            if let Err(e) = &r
                && e.to_string().contains(crate::object::edit::STALE)
            {
                return Response::error(412, "Rejected", &e.to_string());
            }
            r
        }
        ("POST", ["v1", "objects", wtype, rest @ ..]) if rest.len() >= 3 && rest[rest.len() - 2] == "calls" => {
            let key = rest[..rest.len() - 2].join("/");
            let handler = rest[rest.len() - 1];
            match body() {
                Ok(arg) => calls
                    .call(
                        "updateWithStart",
                        json!({"type": wtype, "id": format!("{wtype}/{key}"), "input": null,
                            "name": handler, "arg": arg, "updateId": req.query("id").unwrap_or_default(),
                            "version": null, "options": {"timeout": null,
                                "idPolicy": {"running": "use-existing", "finished": "allow"}}}),
                    )
                    .await,
                Err(e) => Err(e),
            }
        }
        ("GET", ["v1", "schedules"]) => calls
            .call("list", json!({"where": {"type": SCHEDULE_TYPE}, "limit": limit()}))
            .await
            .map(|rows| {
                Value::Array(
                    rows.as_array()
                        .into_iter()
                        .flatten()
                        .filter(|r| r["finishedMs"].is_null())
                        .map(|r| {
                            let id = r["id"].as_str().unwrap_or_default();
                            json!({"name": id.strip_prefix("fusee.schedule/").unwrap_or(id), "summary": r})
                        })
                        .collect(),
                )
            }),
        ("GET", ["v1", "schedules", name]) => {
            calls
                .call(
                    "query",
                    json!({"id": format!("fusee.schedule/{name}"), "name": "describe", "arg": null}),
                )
                .await
        }
        ("POST", ["v1", "schedules", name, op @ ("pause" | "resume" | "trigger")]) => calls
            .call(
                "signal",
                json!({"id": format!("fusee.schedule/{name}"), "name": SCHEDULE_CONTROL, "value": {"op": op}}),
            )
            .await,
        ("DELETE", ["v1", "schedules", name]) => calls
            .call(
                "terminate",
                json!({"id": format!("fusee.schedule/{name}"), "reason": "schedule deleted"}),
            )
            .await,
        ("GET" | "POST" | "DELETE", ["v1", ..]) => {
            return Response::error(404, "NotFound", "no such route; see /v1/openapi.json");
        }
        ("GET", _) if opts.no_ui => {
            return Response::error(404, "NotFound", "not found (the UI is not served)");
        }
        ("GET", _) => return ui::page(opts),
        _ => return Response::error(405, "Rejected", "method not allowed"),
    };
    Response::result(r)
}

/// Whether `result` failed because the run ended that way.
fn is_outcome(e: &Error) -> bool {
    matches!(e, Error::Cancelled(_) | Error::TimedOut | Error::Terminated) || e.failure().is_some()
}

fn status_word(e: &Error) -> &'static str {
    match e {
        Error::Cancelled(_) => "cancelled",
        Error::TimedOut => "timed_out",
        Error::Terminated => "terminated",
        _ => "failed",
    }
}

/// A reset before the run's failed step.
/// A state route's key and field: `[key.., "state"]` or, for `PUT` and
/// `DELETE`, `[key.., "state", field]`.
fn state_route<'a>(method: &str, rest: &[&'a str]) -> Option<(String, Option<&'a str>)> {
    let n = rest.len();
    if n >= 2 && rest[n - 1] == "state" && method != "PUT" {
        return Some((rest[..n - 1].join("/"), None));
    }
    if n >= 3 && rest[n - 2] == "state" && matches!(method, "PUT" | "DELETE") {
        return Some((rest[..n - 2].join("/"), Some(rest[n - 1])));
    }
    None
}

async fn retry(calls: &dyn Calls, id: &str) -> Result<Value> {
    let d = calls.call("describe", json!({ "id": id })).await?;
    let failed = d["journal"]
        .as_array()
        .into_iter()
        .flatten()
        .rev()
        .find(|r| r["kind"] == "step" && r["status"] == "failed")
        .and_then(|r| r["name"].as_str().map(str::to_owned));
    match failed {
        Some(step) => calls
            .call(
                "reset",
                json!({"id": id, "beforeStep": step, "reapplySignals": true}),
            )
            .await
            .map(|_| json!({ "resetBefore": step })),
        None => Err(Error::Rejected(format!(
            "workflow '{id}' has no failed step to retry (status {})",
            d["status"].as_str().unwrap_or("unknown")
        ))),
    }
}

// ------------------------------------------------------------------ events

async fn sse_head(stream: &mut TcpStream) -> io::Result<()> {
    stream
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nConnection: keep-alive\r\nX-Accel-Buffering: no\r\n\r\n")
        .await
}

async fn sse(stream: &mut TcpStream, event: &str, id: Option<u64>, data: &Value) -> io::Result<()> {
    let mut out = String::new();
    if let Some(id) = id {
        out.push_str(&format!("id: {id}\n"));
    }
    out.push_str(&format!("event: {event}\ndata: {data}\n\n"));
    stream.write_all(out.as_bytes()).await
}

/// Run changes: a listing every half second, sent as the summaries that
/// changed. It reads like any client; nothing is pushed from the step path.
async fn events(calls: &dyn Calls, req: &Request, stream: &mut TcpStream) -> io::Result<()> {
    sse_head(stream).await?;
    let filter = list_filter(req).unwrap_or_else(|_| json!({}));
    let mut seen: rustc_hash::FxHashMap<String, Value> = Default::default();
    let mut first = true;
    loop {
        match calls
            .call("list", json!({"where": filter, "limit": 1000}))
            .await
        {
            Ok(Value::Array(rows)) => {
                for r in rows {
                    let id = r["id"].as_str().unwrap_or_default().to_owned();
                    if seen.get(&id) != Some(&r) {
                        if !first {
                            sse(stream, "run", None, &r).await?;
                        }
                        seen.insert(id, r);
                    }
                }
                if first {
                    sse(stream, "ready", None, &json!({ "runs": seen.len() })).await?;
                    first = false;
                }
            }
            Ok(_) => {}
            Err(e) => sse(stream, "error", None, &error_json(&e)).await?,
        }
        // A comment keeps proxies from closing a quiet stream.
        stream.write_all(b": tick\n\n").await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// A run's stream as events: each chunk with its offset, then `end`.
async fn stream_events(
    calls: &dyn Calls,
    req: &Request,
    id: &str,
    name: &str,
    stream: &mut TcpStream,
) -> io::Result<()> {
    sse_head(stream).await?;
    let mut from = req
        .query("from")
        .or_else(|| req.header("last-event-id"))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let mut run: Option<u64> = None;
    loop {
        let b = match calls
            .call(
                "streamRead",
                json!({"id": id, "run": run, "name": name, "from": from, "waitMs": 20_000}),
            )
            .await
        {
            Ok(b) => b,
            Err(e) => return sse(stream, "error", None, &error_json(&e)).await,
        };
        run = b["run"].as_u64().or(run);
        let start = b["from"].as_u64().unwrap_or(from);
        for (i, c) in b["chunks"].as_array().into_iter().flatten().enumerate() {
            let offset = start + i as u64;
            sse(
                stream,
                "chunk",
                Some(offset + 1),
                &json!({"offset": offset, "value": c}),
            )
            .await?;
        }
        from = b["next"].as_u64().unwrap_or(from).max(from);
        if !b["end"].is_null() {
            return sse(stream, "end", None, &b["end"]).await;
        }
        stream.write_all(b": tick\n\n").await?;
    }
}

// ------------------------------------------------------------------ metrics

/// Counters that go down as well as up; the rest only grow.
const GAUGES: &[&str] = &[
    "parked_entries",
    "max_tick_us",
    "max_flush_us",
    "volatile_timers",
    "runs_stuck",
    "runs_paused",
    "runs_waiting_for_code",
];

/// Prometheus text from a `metrics` answer (`{ shards: [...] }`).
pub fn prometheus_json(m: &Value) -> String {
    let mut by_name: std::collections::BTreeMap<String, Vec<(usize, u64)>> = Default::default();
    for (i, shard) in m["shards"].as_array().into_iter().flatten().enumerate() {
        if let Value::Object(fields) = shard {
            for (k, v) in fields {
                if let Some(n) = v.as_u64() {
                    by_name.entry(k.clone()).or_default().push((i, n));
                }
            }
        }
    }
    let mut out = String::new();
    for (name, values) in by_name {
        let kind = if GAUGES.contains(&name.as_str()) {
            "gauge"
        } else {
            "counter"
        };
        out.push_str(&format!("# TYPE fusee_{name} {kind}\n"));
        for (shard, v) in values {
            out.push_str(&format!("fusee_{name}{{shard=\"{shard}\"}} {v}\n"));
        }
    }
    out
}

/// Prometheus text of per-shard counters.
pub fn prometheus(shards: &[crate::Metrics]) -> String {
    prometheus_json(&json!({
        "shards": shards.iter().map(|m| serde_json::to_value(m).unwrap_or(Value::Null)).collect::<Vec<_>>()
    }))
}

// ------------------------------------------------------------------ OpenAPI

fn openapi() -> Value {
    let op =
        |summary: &str| json!({"summary": summary, "responses": {"200": {"description": "OK"}}});
    let id = json!({"name": "id", "in": "path", "required": true, "schema": {"type": "string"}});
    let name =
        json!({"name": "name", "in": "path", "required": true, "schema": {"type": "string"}});
    let with = |mut o: Value, params: &[&Value]| {
        o["parameters"] = Value::Array(params.iter().map(|p| (*p).clone()).collect());
        o
    };
    json!({
        "openapi": "3.1.0",
        "info": {"title": "Fusee", "version": env!("CARGO_PKG_VERSION"),
            "description": "Fusee's HTTP API. Every route is one of the protocol's calls; POST /v1/call/{method} sends any of them."},
        "paths": {
            "/v1/runs": {"get": op("List runs (filters: type, status, id, where, limit)"), "post": op("Start a run")},
            "/v1/runs/{id}": {"get": with(op("A run and its journal"), &[&id])},
            "/v1/runs/{id}/result": {"get": with(op("Wait for a run's result"), &[&id])},
            "/v1/runs/{id}/signals/{name}": {"post": with(op("Signal a run"), &[&id, &name])},
            "/v1/runs/{id}/queries/{name}": {"post": with(op("Query a run"), &[&id, &name])},
            "/v1/runs/{id}/updates/{name}": {"post": with(op("Update a run"), &[&id, &name])},
            "/v1/runs/{id}/cancel": {"post": with(op("Cancel a run"), &[&id])},
            "/v1/runs/{id}/terminate": {"post": with(op("Terminate a run"), &[&id])},
            "/v1/runs/{id}/reset": {"post": with(op("Reset a run to before a step"), &[&id])},
            "/v1/runs/{id}/retry": {"post": with(op("Run a failed run again from its failed step"), &[&id])},
            "/v1/runs/{id}/resume": {"post": with(op("Resume a paused run: its step runs again with a fresh retry budget"), &[&id])},
            "/v1/runs/{id}/streams/{name}": {"get": with(op("Read a run's stream (text/event-stream follows it)"), &[&id, &name])},
            "/v1/complete/{token}": {"post": op("Complete an async completion")},
            "/v1/fail/{token}": {"post": op("Fail an async completion")},
            "/v1/objects/{type}/{key}": {"get": op("A key of an object type: its run and state")},
            "/v1/objects/{type}/{key}/calls/{handler}": {"post": op("Call an object's handler")},
            "/v1/objects/{type}/{key}/state": {"post": op("Edit a key's state (If-Match: its version)"), "patch": op("Apply a JSON Patch to a key's state"), "delete": op("Clear a key's state")},
            "/v1/objects/{type}/{key}/state/{field}": {"put": op("Set one field of a key's state"), "delete": op("Clear one field of a key's state")},
            "/v1/schedules": {"get": op("List schedules")},
            "/v1/schedules/{name}": {"get": op("Describe a schedule"), "delete": op("Delete a schedule")},
            "/v1/schedules/{name}/pause": {"post": op("Pause a schedule")},
            "/v1/schedules/{name}/resume": {"post": op("Resume a schedule")},
            "/v1/schedules/{name}/trigger": {"post": op("Start one scheduled run now")},
            "/v1/types": {"get": op("Registered workflow types and versions")},
            "/v1/limits": {"get": op("Defined limits and their permits now")},
            "/v1/metrics": {"get": op("Counters, total and per shard")},
            "/v1/info": {"get": op("What serves this API")},
            "/v1/events": {"get": op("Run changes as server-sent events")},
            "/v1/call/{method}": {"post": op("Any protocol call")},
            "/metrics": {"get": op("Prometheus metrics")},
        },
    })
}
