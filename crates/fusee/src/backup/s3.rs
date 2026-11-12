//! An S3-compatible backup target (feature `s3`): requests signed with
//! Signature Version 4, path-style or virtual-hosted addressing, HTTP/1.1
//! over TLS 1.3 (rustls, ring provider) or plain TCP for local endpoints.
//!
//! Objects are `<prefix>/<name>`; the manifest is `<prefix>/manifest.json`,
//! replaced by a single `PUT` (S3 makes a new object version visible
//! atomically, and a `PUT` returns once the object is durable). Listing
//! uses `ListObjectsV2`.

use super::target::{Target, check_name};
use sha2::{Digest, Sha256};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// Access key, secret and (for temporary credentials) session token.
#[derive(Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &self.access_key)
            .finish_non_exhaustive()
    }
}

impl Credentials {
    /// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optionally
    /// `AWS_SESSION_TOKEN`.
    pub fn from_env() -> io::Result<Self> {
        let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        match (get("AWS_ACCESS_KEY_ID"), get("AWS_SECRET_ACCESS_KEY")) {
            (Some(access_key), Some(secret_key)) => Ok(Self {
                access_key,
                secret_key,
                session_token: get("AWS_SESSION_TOKEN"),
            }),
            _ => Err(other(
                "S3 credentials: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
            )),
        }
    }
}

/// Where and how to reach the bucket.
#[derive(Clone, Debug)]
pub struct S3Options {
    pub bucket: String,
    /// Key prefix under which the backup lives (no leading or trailing `/`).
    pub prefix: String,
    pub region: String,
    /// `https://host[:port]` or `http://host[:port]`; `None`: AWS
    /// (`https://s3.<region>.amazonaws.com`).
    pub endpoint: Option<String>,
    /// `https://host/bucket/key` instead of `https://bucket.host/key`
    /// (MinIO and most other S3-compatible stores need it).
    pub path_style: bool,
    pub credentials: Credentials,
    /// PEM file of CA certificates; `None`: `SSL_CERT_FILE`, then the
    /// system's bundle.
    pub ca_file: Option<PathBuf>,
    pub timeout: Duration,
}

/// One HTTP request, before it is sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    pub https: bool,
    pub host: String,
    pub port: u16,
    /// URI-encoded path.
    pub path: String,
    /// Decoded query parameters.
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    /// The query as sent and signed: encoded, sorted.
    pub fn query_string(&self) -> String {
        let mut q: Vec<(String, String)> = self
            .query
            .iter()
            .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
            .collect();
        q.sort();
        q.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// The `Host` header: with the port unless it is the scheme's default.
    pub fn host_header(&self) -> String {
        let default = if self.https { 443 } else { 80 };
        if self.port == default {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// The request as it goes on the wire (body excluded).
    pub fn head(&self) -> String {
        let q = self.query_string();
        let mut out = format!(
            "{} {}{}{} HTTP/1.1\r\n",
            self.method,
            self.path,
            if q.is_empty() { "" } else { "?" },
            q
        );
        for (k, v) in &self.headers {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        out.push_str("\r\n");
        out
    }
}

#[derive(Clone, Debug, Default)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Sends requests (a network connection, or a recording in tests).
pub trait Transport: Send + Sync {
    fn send(&self, req: &Request) -> io::Result<Response>;
}

/// Percent-encodes everything but unreserved characters (and `/` unless
/// `slash`), as SigV4 requires.
pub fn uri_encode(s: &str, slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) || (b == b'/' && !slash) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// HMAC-SHA256 (RFC 2104).
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.map(|b| b ^ 0x36));
    inner.update(msg);
    let mut outer = Sha256::new();
    outer.update(k.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// The SigV4 canonical request and its signed-header list. Header names
/// are lowercased and sorted, values trimmed with inner runs of spaces
/// collapsed; the payload hash is `x-amz-content-sha256`'s value.
pub fn canonical_request(req: &Request) -> (String, String) {
    let mut headers: Vec<(String, String)> = req
        .headers
        .iter()
        .filter(|(k, _)| !k.eq_ignore_ascii_case("authorization"))
        .map(|(k, v)| {
            (
                k.to_ascii_lowercase(),
                v.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect();
    headers.sort();
    let signed = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let payload = headers
        .iter()
        .find(|(k, _)| k == "x-amz-content-sha256")
        .map_or_else(|| sha256_hex(&req.body), |(_, v)| v.clone());
    let mut c = format!("{}\n{}\n{}\n", req.method, req.path, req.query_string());
    for (k, v) in &headers {
        c.push_str(&format!("{k}:{v}\n"));
    }
    c.push_str(&format!("\n{signed}\n{payload}"));
    (c, signed)
}

/// Signs `req` (adding `host`, `x-amz-date`, `x-amz-content-sha256`, the
/// session token if any, and `authorization`). `amz_date` is
/// `YYYYMMDDTHHMMSSZ`.
pub fn sign(req: &mut Request, creds: &Credentials, region: &str, amz_date: &str) {
    let has = |req: &Request, name: &str| {
        req.headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(name))
    };
    if !has(req, "host") {
        let h = req.host_header();
        req.headers.push(("host".into(), h));
    }
    if !has(req, "x-amz-content-sha256") {
        let h = sha256_hex(&req.body);
        req.headers.push(("x-amz-content-sha256".into(), h));
    }
    req.headers.push(("x-amz-date".into(), amz_date.into()));
    if let Some(t) = &creds.session_token {
        req.headers.push(("x-amz-security-token".into(), t.clone()));
    }
    let (canonical, signed) = canonical_request(req);
    let date = &amz_date[..8];
    let scope = format!("{date}/{region}/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let mut key = hmac_sha256(
        format!("AWS4{}", creds.secret_key).as_bytes(),
        date.as_bytes(),
    );
    for part in [region, "s3", "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    let signature = hex(&hmac_sha256(&key, to_sign.as_bytes()));
    req.headers.push((
        "authorization".into(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
            creds.access_key
        ),
    ));
}

/// `YYYYMMDDTHHMMSSZ` for a time.
pub fn amz_date(t: SystemTime) -> String {
    let secs = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs()) as i64;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|d| d.format("%Y%m%dT%H%M%SZ").to_string())
        .unwrap_or_default()
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// The text of every `<tag>` element, in order.
fn xml_all<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(&after[..j]);
        rest = &after[j + close.len()..];
    }
    out
}

/// A backup target in an S3 bucket.
pub struct S3Target {
    opts: S3Options,
    https: bool,
    host: String,
    port: u16,
    transport: Box<dyn Transport>,
    clock: Box<dyn Fn() -> SystemTime + Send + Sync>,
}

impl S3Target {
    pub fn new(opts: S3Options) -> io::Result<Self> {
        let (https, _, _) = parse_endpoint(&endpoint_of(&opts))?;
        let transport = NetTransport::new(https, opts.ca_file.clone(), opts.timeout)?;
        Self::with_transport(opts, Box::new(transport), Box::new(SystemTime::now))
    }

    /// A target that sends through `transport` and signs with `clock`'s
    /// time (tests replay recorded requests with both fixed).
    pub fn with_transport(
        opts: S3Options,
        transport: Box<dyn Transport>,
        clock: Box<dyn Fn() -> SystemTime + Send + Sync>,
    ) -> io::Result<Self> {
        if opts.bucket.is_empty() {
            return Err(other("S3: no bucket"));
        }
        let (https, host, port) = parse_endpoint(&endpoint_of(&opts))?;
        let host = if opts.path_style {
            host
        } else {
            format!("{}.{host}", opts.bucket)
        };
        Ok(Self {
            opts: S3Options {
                prefix: opts.prefix.trim_matches('/').to_owned(),
                ..opts
            },
            https,
            host,
            port,
            transport,
            clock,
        })
    }

    /// `s3://bucket/prefix?region=us-east-1&endpoint=http://127.0.0.1:9000&path_style=1&ca=/path.pem`.
    /// The region defaults to `AWS_REGION` (or `AWS_DEFAULT_REGION`, else
    /// `us-east-1`); credentials come from the environment
    /// ([`Credentials::from_env`]).
    pub fn from_url(url: &str) -> io::Result<Self> {
        let rest = url
            .strip_prefix("s3://")
            .ok_or_else(|| other("an S3 target starts with s3://"))?;
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
        let mut region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".into());
        let (mut endpoint, mut path_style, mut ca_file) = (None, false, None);
        for kv in query.split('&').filter(|s| !s.is_empty()) {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            match k {
                "region" => region = v.into(),
                "endpoint" => endpoint = Some(v.to_owned()),
                "path_style" => path_style = v != "0" && v != "false",
                "ca" => ca_file = Some(PathBuf::from(v)),
                _ => return Err(other(format!("S3 target: unknown option {k:?}"))),
            }
        }
        Self::new(S3Options {
            bucket: bucket.into(),
            prefix: prefix.into(),
            region,
            path_style: path_style || endpoint.is_some(),
            endpoint,
            credentials: Credentials::from_env()?,
            ca_file,
            timeout: Duration::from_secs(30),
        })
    }

    fn key(&self, name: &str) -> String {
        if self.opts.prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{}/{name}", self.opts.prefix)
        }
    }

    fn base_path(&self) -> String {
        if self.opts.path_style {
            format!("/{}", uri_encode(&self.opts.bucket, true))
        } else {
            String::new()
        }
    }

    fn request(
        &self,
        method: &str,
        key: Option<&str>,
        query: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> io::Result<Response> {
        let path = match key {
            Some(k) => format!("{}/{}", self.base_path(), uri_encode(k, false)),
            None => format!("{}/", self.base_path()),
        };
        let mut req = Request {
            method: method.into(),
            https: self.https,
            host: self.host.clone(),
            port: self.port,
            path,
            query,
            headers: Vec::new(),
            body,
        };
        sign(
            &mut req,
            &self.opts.credentials,
            &self.opts.region,
            &amz_date((self.clock)()),
        );
        self.transport.send(&req)
    }

    fn fail(&self, what: &str, r: &Response) -> io::Error {
        let body = String::from_utf8_lossy(&r.body);
        let code = xml_all(&body, "Code").first().map(|s| xml_unescape(s));
        let msg = xml_all(&body, "Message").first().map(|s| xml_unescape(s));
        other(format!(
            "S3 {what}: HTTP {}{}{}",
            r.status,
            code.map(|c| format!(" {c}")).unwrap_or_default(),
            msg.map(|m| format!(": {m}")).unwrap_or_default()
        ))
    }

    fn put_key(&self, key: &str, bytes: &[u8]) -> io::Result<()> {
        let r = self.request("PUT", Some(key), Vec::new(), bytes.to_vec())?;
        if r.status / 100 == 2 {
            Ok(())
        } else {
            Err(self.fail(&format!("PUT {key}"), &r))
        }
    }

    fn get_key(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let r = self.request("GET", Some(key), Vec::new(), Vec::new())?;
        match r.status {
            200 => Ok(Some(r.body)),
            404 => Ok(None),
            _ => Err(self.fail(&format!("GET {key}"), &r)),
        }
    }
}

fn endpoint_of(opts: &S3Options) -> String {
    opts.endpoint
        .clone()
        .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", opts.region))
}

/// `(https, host, port)` of `scheme://host[:port]`.
fn parse_endpoint(endpoint: &str) -> io::Result<(bool, String, u16)> {
    let (https, rest) = if let Some(r) = endpoint.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = endpoint.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(other(format!(
            "S3 endpoint {endpoint:?} must start with https:// or http://"
        )));
    };
    let authority = rest.trim_end_matches('/');
    if authority.contains('/') || authority.is_empty() {
        return Err(other(format!(
            "S3 endpoint {endpoint:?} must be scheme://host[:port]"
        )));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || h.starts_with('[') => (
            h.to_owned(),
            p.parse()
                .map_err(|_| other(format!("bad port in S3 endpoint {endpoint:?}")))?,
        ),
        _ => (authority.to_owned(), if https { 443 } else { 80 }),
    };
    Ok((https, host, port))
}

impl Target for S3Target {
    fn put(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        check_name(name)?;
        self.put_key(&self.key(name), bytes)
    }

    fn get(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        check_name(name)?;
        self.get_key(&self.key(name))
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let base = self.key("");
        let full = format!("{base}{prefix}");
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![
                ("list-type".to_owned(), "2".to_owned()),
                ("prefix".to_owned(), full.clone()),
            ];
            if let Some(t) = &token {
                q.push(("continuation-token".to_owned(), t.clone()));
            }
            let r = self.request("GET", None, q, Vec::new())?;
            if r.status != 200 {
                return Err(self.fail(&format!("list {full}"), &r));
            }
            let body = String::from_utf8_lossy(&r.body).into_owned();
            for c in xml_all(&body, "Contents") {
                if let Some(k) = xml_all(c, "Key").first() {
                    let k = xml_unescape(k);
                    if let Some(name) = k.strip_prefix(&base)
                        && name != "manifest.json"
                    {
                        out.push(name.to_owned());
                    }
                }
            }
            let truncated = xml_all(&body, "IsTruncated").first() == Some(&"true");
            token = xml_all(&body, "NextContinuationToken")
                .first()
                .map(|t| xml_unescape(t));
            if !truncated || token.is_none() {
                break;
            }
        }
        out.sort();
        Ok(out)
    }

    fn delete(&self, name: &str) -> io::Result<()> {
        check_name(name)?;
        let key = self.key(name);
        let r = self.request("DELETE", Some(&key), Vec::new(), Vec::new())?;
        if r.status / 100 == 2 || r.status == 404 {
            Ok(())
        } else {
            Err(self.fail(&format!("DELETE {key}"), &r))
        }
    }

    fn put_manifest(&self, bytes: &[u8]) -> io::Result<()> {
        self.put_key(&self.key("manifest.json"), bytes)
    }

    fn get_manifest(&self) -> io::Result<Option<Vec<u8>>> {
        self.get_key(&self.key("manifest.json"))
    }

    fn describe(&self) -> String {
        format!("s3://{}/{}", self.opts.bucket, self.opts.prefix)
    }
}

trait Stream: Read + Write + Send {}
impl<T: Read + Write + Send> Stream for T {}

/// HTTP/1.1 over TCP, with TLS for `https`; keeps one connection open
/// between requests.
pub struct NetTransport {
    tls: Option<Arc<rustls::ClientConfig>>,
    timeout: Duration,
    conn: Mutex<Option<(String, u16, BufReader<Box<dyn Stream>>)>>,
}

const CA_BUNDLES: &[&str] = &[
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/ssl/cert.pem",
    "/usr/local/etc/openssl/cert.pem",
    "/opt/homebrew/etc/openssl@3/cert.pem",
];

impl NetTransport {
    pub fn new(https: bool, ca_file: Option<PathBuf>, timeout: Duration) -> io::Result<Self> {
        let tls = if https {
            use rustls::pki_types::CertificateDer;
            use rustls::pki_types::pem::PemObject;
            let path = ca_file
                .or_else(|| std::env::var_os("SSL_CERT_FILE").map(PathBuf::from))
                .or_else(|| CA_BUNDLES.iter().map(PathBuf::from).find(|p| p.exists()))
                .ok_or_else(|| {
                    other("S3 over HTTPS: no CA bundle found; set SSL_CERT_FILE or ca=PATH")
                })?;
            let pem = std::fs::read(&path)?;
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(&pem) {
                let cert = cert.map_err(|e| other(format!("{}: {e}", path.display())))?;
                // A bundle may hold certificates this provider cannot use.
                let _ = roots.add(cert);
            }
            if roots.is_empty() {
                return Err(other(format!("{}: no CA certificate", path.display())));
            }
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = rustls::ClientConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(other)?
                .with_root_certificates(roots)
                .with_no_client_auth();
            Some(Arc::new(config))
        } else {
            None
        };
        Ok(Self {
            tls,
            timeout,
            conn: Mutex::new(None),
        })
    }

    fn connect(&self, req: &Request) -> io::Result<BufReader<Box<dyn Stream>>> {
        use std::net::ToSocketAddrs;
        let addr = (req.host.as_str(), req.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| other(format!("{}: no address", req.host)))?;
        let tcp = TcpStream::connect_timeout(&addr, self.timeout)?;
        tcp.set_read_timeout(Some(self.timeout))?;
        tcp.set_write_timeout(Some(self.timeout))?;
        tcp.set_nodelay(true)?;
        let stream: Box<dyn Stream> = match (&self.tls, req.https) {
            (Some(tls), true) => {
                let name = rustls::pki_types::ServerName::try_from(req.host.clone())
                    .map_err(|e| other(format!("{}: {e}", req.host)))?;
                let conn = rustls::ClientConnection::new(tls.clone(), name).map_err(other)?;
                Box::new(rustls::StreamOwned::new(conn, tcp))
            }
            (None, true) => return Err(other("HTTPS without a TLS configuration")),
            _ => Box::new(tcp),
        };
        Ok(BufReader::new(stream))
    }

    fn exchange(
        stream: &mut BufReader<Box<dyn Stream>>,
        req: &Request,
    ) -> io::Result<(Response, bool)> {
        let mut head = req.head();
        head.insert_str(
            head.len() - 2,
            &format!("content-length: {}\r\n", req.body.len()),
        );
        let w = stream.get_mut();
        w.write_all(head.as_bytes())?;
        w.write_all(&req.body)?;
        w.flush()?;
        let mut line = String::new();
        if stream.read_line(&mut line)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "connection closed",
            ));
        }
        let status: u16 = line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| other(format!("bad HTTP status line {line:?}")))?;
        let mut headers = Vec::new();
        loop {
            let mut h = String::new();
            stream.read_line(&mut h)?;
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some((k, v)) = h.split_once(':') {
                headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
            }
        }
        let header = |n: &str| {
            headers
                .iter()
                .find(|(k, _)| k == n)
                .map(|(_, v)| v.as_str())
        };
        let close = header("connection").is_some_and(|v| v.eq_ignore_ascii_case("close"));
        let mut body = Vec::new();
        if req.method == "HEAD" || status == 204 || status == 304 {
        } else if header("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
            loop {
                let mut size = String::new();
                stream.read_line(&mut size)?;
                let n = usize::from_str_radix(size.trim().split(';').next().unwrap_or(""), 16)
                    .map_err(|_| other("bad chunk size"))?;
                if n == 0 {
                    // Trailers, then the blank line.
                    loop {
                        let mut t = String::new();
                        stream.read_line(&mut t)?;
                        if t.trim_end().is_empty() {
                            break;
                        }
                    }
                    break;
                }
                let at = body.len();
                body.resize(at + n, 0);
                stream.read_exact(&mut body[at..])?;
                let mut crlf = [0u8; 2];
                stream.read_exact(&mut crlf)?;
            }
        } else if let Some(n) = header("content-length").and_then(|v| v.parse::<usize>().ok()) {
            body.resize(n, 0);
            stream.read_exact(&mut body)?;
        } else {
            stream.read_to_end(&mut body)?;
            return Ok((
                Response {
                    status,
                    headers,
                    body,
                },
                false,
            ));
        }
        Ok((
            Response {
                status,
                headers,
                body,
            },
            !close,
        ))
    }
}

impl Transport for NetTransport {
    fn send(&self, req: &Request) -> io::Result<Response> {
        let mut slot = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        // A kept connection the server may have closed meanwhile: one retry
        // on a new one.
        // (Every request the backup makes is idempotent.)
        if let Some((host, port, mut stream)) = slot.take()
            && host == req.host
            && port == req.port
            && let Ok((r, keep)) = Self::exchange(&mut stream, req)
        {
            if keep {
                *slot = Some((host, port, stream));
            }
            return Ok(r);
        }
        let mut stream = self.connect(req)?;
        let (r, keep) = Self::exchange(&mut stream, req)?;
        if keep {
            *slot = Some((req.host.clone(), req.port, stream));
        }
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_creds() -> Credentials {
        // The keys of AWS's Signature Version 4 examples.
        Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        }
    }

    fn example(
        method: &str,
        path: &str,
        query: &[(&str, &str)],
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Request {
        Request {
            method: method.into(),
            https: true,
            host: "examplebucket.s3.amazonaws.com".into(),
            port: 443,
            path: path.into(),
            query: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: body.to_vec(),
        }
    }

    fn signature(req: &Request) -> String {
        let auth = &req
            .headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .unwrap()
            .1;
        auth.rsplit_once("Signature=").unwrap().1.to_owned()
    }

    const EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn hmac_matches_rfc_4231() {
        // RFC 4231, test case 2.
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Test case 6: a key longer than the block.
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    /// The four requests of the S3 documentation's Signature Version 4
    /// examples ("Signature Calculations for the Authorization Header").
    #[test]
    fn signatures_match_the_aws_examples() {
        let creds = example_creds();
        let date = "20130524T000000Z";
        let mut get = example("GET", "/test.txt", &[], &[("Range", "bytes=0-9")], b"");
        sign(&mut get, &creds, "us-east-1", date);
        assert_eq!(
            canonical_request(&get).0,
            format!(
                "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\n\
                 x-amz-content-sha256:{EMPTY}\nx-amz-date:20130524T000000Z\n\n\
                 host;range;x-amz-content-sha256;x-amz-date\n{EMPTY}"
            )
        );
        assert_eq!(
            signature(&get),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        let mut put = example(
            "PUT",
            &format!("/{}", uri_encode("test$file.text", false)),
            &[],
            &[
                ("Date", "Fri, 24 May 2013 00:00:00 GMT"),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ],
            b"Welcome to Amazon S3.",
        );
        sign(&mut put, &creds, "us-east-1", date);
        assert_eq!(put.path, "/test%24file.text");
        assert_eq!(
            signature(&put),
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
        let mut lifecycle = example("GET", "/", &[("lifecycle", "")], &[], b"");
        sign(&mut lifecycle, &creds, "us-east-1", date);
        assert_eq!(
            signature(&lifecycle),
            "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        );
        let mut list = example("GET", "/", &[("max-keys", "2"), ("prefix", "J")], &[], b"");
        sign(&mut list, &creds, "us-east-1", date);
        assert_eq!(list.query_string(), "max-keys=2&prefix=J");
        assert_eq!(
            signature(&list),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    /// Replays canned responses and records the requests.
    struct Recording {
        sent: Mutex<Vec<Request>>,
        replies: Mutex<std::collections::VecDeque<Response>>,
    }

    impl Transport for Arc<Recording> {
        fn send(&self, req: &Request) -> io::Result<Response> {
            self.sent.lock().unwrap().push(req.clone());
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| other("no reply recorded"))
        }
    }

    fn reply(status: u16, body: &str) -> Response {
        Response {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }
    }

    fn recorded(path_style: bool, replies: Vec<Response>) -> (S3Target, Arc<Recording>) {
        let rec = Arc::new(Recording {
            sent: Mutex::new(Vec::new()),
            replies: Mutex::new(replies.into()),
        });
        let t = S3Target::with_transport(
            S3Options {
                bucket: "backups".into(),
                prefix: "/fusee/prod/".into(),
                region: "eu-west-1".into(),
                endpoint: path_style.then(|| "http://127.0.0.1:9000".into()),
                path_style,
                credentials: Credentials {
                    session_token: Some("token+/=".into()),
                    ..example_creds()
                },
                ca_file: None,
                timeout: Duration::from_secs(1),
            },
            Box::new(rec.clone()),
            Box::new(|| SystemTime::UNIX_EPOCH + Duration::from_secs(1_762_689_600)),
        )
        .unwrap();
        (t, rec)
    }

    /// Requests a backup makes, as sent: method, path, query, every header
    /// and the signature, recorded once from this implementation and checked
    /// against the AWS rules above.
    #[test]
    fn requests_match_their_recording() {
        let (t, rec) = recorded(false, vec![reply(200, "")]);
        t.put(
            "t1/log/00000000000000000001/000000000000-000000016384.wal",
            b"bytes",
        )
        .unwrap();
        let sent = rec.sent.lock().unwrap().remove(0);
        assert_eq!(
            sent.head(),
            "PUT /fusee/prod/t1/log/00000000000000000001/000000000000-000000016384.wal HTTP/1.1\r\n\
             host: backups.s3.eu-west-1.amazonaws.com\r\n\
             x-amz-content-sha256: 277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9\r\n\
             x-amz-date: 20251109T120000Z\r\n\
             x-amz-security-token: token+/=\r\n\
             authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20251109/eu-west-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, \
             Signature=RECORDED\r\n\r\n"
                .replace(
                    "RECORDED",
                    &signature(&sent)
                )
        );
        assert_eq!(sent.body, b"bytes");
        assert_eq!(
            sha256_hex(b"bytes"),
            "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9"
        );
        // The signature itself, recorded.
        assert_eq!(
            signature(&sent),
            RECORDED_PUT_SIGNATURE,
            "a change to how requests are signed"
        );

        // Path style, a listing over two pages with an escaped key.
        let page1 = "<?xml version=\"1.0\"?><ListBucketResult><IsTruncated>true</IsTruncated>\
            <Contents><Key>fusee/prod/manifest.json</Key></Contents>\
            <Contents><Key>fusee/prod/state/1.json</Key><Size>3</Size></Contents>\
            <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>\
            </ListBucketResult>";
        let page2 = "<ListBucketResult><IsTruncated>false</IsTruncated>\
            <Contents><Key>fusee/prod/t1/a&amp;b.wal</Key></Contents></ListBucketResult>";
        let (t, rec) = recorded(true, vec![reply(200, page1), reply(200, page2)]);
        assert_eq!(t.list("").unwrap(), vec!["state/1.json", "t1/a&b.wal"]);
        let sent = rec.sent.lock().unwrap().clone();
        assert_eq!(sent[0].host_header(), "127.0.0.1:9000");
        assert_eq!(
            sent[0].head().lines().next().unwrap(),
            "GET /backups/?list-type=2&prefix=fusee%2Fprod%2F HTTP/1.1"
        );
        assert_eq!(
            sent[1].head().lines().next().unwrap(),
            "GET /backups/?continuation-token=1ueGcxLPRx1Tr%2FXYExHnhbYLgveDs2J%2Fwm36Hy4vbOwM%3D&list-type=2&prefix=fusee%2Fprod%2F HTTP/1.1"
        );

        // Missing objects, errors, deletes, the manifest.
        let error =
            "<Error><Code>AccessDenied</Code><Message>Access &amp; denied</Message></Error>";
        let (t, rec) = recorded(
            true,
            vec![
                reply(404, ""),
                reply(403, error),
                reply(204, ""),
                reply(404, ""),
                reply(200, ""),
                reply(200, "{\"format\":1}"),
            ],
        );
        assert!(t.get("state/9.json").unwrap().is_none());
        let err = t.get("state/9.json").unwrap_err().to_string();
        assert!(
            err.contains("HTTP 403 AccessDenied: Access & denied"),
            "{err}"
        );
        t.delete("old.wal").unwrap();
        t.delete("old.wal").unwrap();
        t.put_manifest(b"{}").unwrap();
        assert_eq!(t.get_manifest().unwrap().unwrap(), b"{\"format\":1}");
        let sent = rec.sent.lock().unwrap().clone();
        let lines: Vec<String> = sent
            .iter()
            .map(|r| r.head().lines().next().unwrap().to_owned())
            .collect();
        assert_eq!(
            lines,
            vec![
                "GET /backups/fusee/prod/state/9.json HTTP/1.1",
                "GET /backups/fusee/prod/state/9.json HTTP/1.1",
                "DELETE /backups/fusee/prod/old.wal HTTP/1.1",
                "DELETE /backups/fusee/prod/old.wal HTTP/1.1",
                "PUT /backups/fusee/prod/manifest.json HTTP/1.1",
                "GET /backups/fusee/prod/manifest.json HTTP/1.1",
            ]
        );
        // Names are checked before anything is sent.
        assert!(t.put("../escape", b"").is_err());
    }

    const RECORDED_PUT_SIGNATURE: &str =
        "1e1cf4d6f24b215eef1703f360e15fae99ef538f8b8bafea76bc924a0cf320f0";

    #[test]
    fn endpoints_and_urls_parse() {
        assert_eq!(
            parse_endpoint("https://s3.eu-west-1.amazonaws.com").unwrap(),
            (true, "s3.eu-west-1.amazonaws.com".into(), 443)
        );
        assert_eq!(
            parse_endpoint("http://minio.local:9000/").unwrap(),
            (false, "minio.local".into(), 9000)
        );
        assert!(parse_endpoint("ftp://x").is_err());
        assert!(parse_endpoint("https://host/path").is_err());
        assert_eq!(uri_encode("a b/c~d", false), "a%20b/c~d");
        assert_eq!(uri_encode("a b/c", true), "a%20b%2Fc");
        assert_eq!(
            amz_date(SystemTime::UNIX_EPOCH + Duration::from_secs(1_369_353_600)),
            "20130524T000000Z"
        );
    }

    /// The HTTP framing against a server on the loopback interface inside
    /// this test (nothing leaves the machine): content length, chunked
    /// replies, and a kept connection used again.
    #[test]
    fn http_framing_over_a_loopback_socket() {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();
            let mut r = BufReader::new(s.try_clone().unwrap());
            let mut w = s;
            let mut seen = Vec::new();
            for reply in [
                "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2;x=1\r\nde\r\n0\r\n\r\n",
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ] {
                let mut head = String::new();
                let mut len = 0;
                loop {
                    let mut l = String::new();
                    r.read_line(&mut l).unwrap();
                    if let Some(v) = l.strip_prefix("content-length: ") {
                        len = v.trim().parse().unwrap();
                    }
                    head.push_str(&l);
                    if l == "\r\n" {
                        break;
                    }
                }
                let mut body = vec![0; len];
                r.read_exact(&mut body).unwrap();
                seen.push((head, body));
                w.write_all(reply.as_bytes()).unwrap();
            }
            seen
        });
        let t = NetTransport::new(false, None, Duration::from_secs(5)).unwrap();
        let mut req = Request {
            method: "PUT".into(),
            https: false,
            host: "127.0.0.1".into(),
            port,
            path: "/b/k".into(),
            query: Vec::new(),
            headers: vec![("host".into(), format!("127.0.0.1:{port}"))],
            body: b"payload".to_vec(),
        };
        let r = t.send(&req).unwrap();
        assert_eq!((r.status, r.body.as_slice()), (200, &b"hello"[..]));
        req.method = "GET".into();
        req.body.clear();
        let r = t.send(&req).unwrap();
        assert_eq!((r.status, r.body.as_slice()), (200, &b"abcde"[..]));
        let r = t.send(&req).unwrap();
        assert_eq!(r.status, 404);
        let seen = server.join().unwrap();
        assert!(seen[0].0.starts_with("PUT /b/k HTTP/1.1\r\n"));
        assert!(seen[0].0.contains("content-length: 7\r\n"));
        assert_eq!(seen[0].1, b"payload");
        assert!(seen[1].0.starts_with("GET /b/k HTTP/1.1\r\n"));
    }
}
