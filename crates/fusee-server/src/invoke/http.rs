//! Just enough HTTP/1.1 for invocations: one `POST` per connection, with a
//! `Content-Length` body, over TCP or TLS 1.3. The server's invoker is the
//! client; [`super::serve`] is the server side for self-hosted workers and
//! tests.
use crate::security::TlsStream;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// The largest request or response body accepted.
pub(crate) const MAX_BODY: usize = 1 << 20;
/// The largest request or response head accepted.
const MAX_HEAD: usize = 16 << 10;

/// An `http://` or `https://` URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Url {
    pub(crate) tls: bool,
    pub(crate) host: String,
    pub(crate) port: u16,
    /// Path and query, starting with `/`.
    pub(crate) path: String,
}

impl Url {
    pub(crate) fn parse(url: &str) -> io::Result<Self> {
        let bad = |why: &str| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid endpoint URL '{url}': {why}"),
            )
        };
        let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = url.strip_prefix("http://") {
            (false, r)
        } else {
            return Err(bad("it must start with http:// or https://"));
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        if authority.contains('@') {
            return Err(bad("credentials in the URL are not supported"));
        }
        let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
            let (h, after) = v6
                .split_once(']')
                .ok_or_else(|| bad("unclosed IPv6 address"))?;
            let port = match after.strip_prefix(':') {
                Some(p) => p.parse().map_err(|_| bad("invalid port"))?,
                None if after.is_empty() => default_port(tls),
                None => return Err(bad("invalid authority")),
            };
            (h.to_owned(), port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h.to_owned(), p.parse().map_err(|_| bad("invalid port"))?),
                None => (authority.to_owned(), default_port(tls)),
            }
        };
        if host.is_empty() {
            return Err(bad("no host"));
        }
        Ok(Self {
            tls,
            host,
            port,
            path: path.to_owned(),
        })
    }

    fn host_header(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        if self.port == default_port(self.tls) {
            host
        } else {
            format!("{host}:{}", self.port)
        }
    }
}

fn default_port(tls: bool) -> u16 {
    if tls { 443 } else { 80 }
}

/// A response's status and body.
#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
}

/// Sends one `POST` and reads the response. `tls`: the client
/// configuration for an `https://` URL.
pub(crate) async fn post(
    url: &Url,
    tls: Option<&Arc<rustls::ClientConfig>>,
    headers: &[(&str, String)],
    body: &[u8],
) -> io::Result<Response> {
    let sock = TcpStream::connect((url.host.as_str(), url.port)).await?;
    let _ = sock.set_nodelay(true);
    let mut head = format!(
        "POST {} HTTP/1.1\r\nhost: {}\r\ncontent-length: {}\r\nconnection: close\r\nuser-agent: fusee/{}\r\n",
        url.path,
        url.host_header(),
        body.len(),
        env!("CARGO_PKG_VERSION"),
    );
    for (k, v) in headers {
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    if url.tls {
        let config =
            tls.ok_or_else(|| io::Error::other("an https:// endpoint needs TLS settings"))?;
        let name = rustls::pki_types::ServerName::try_from(url.host.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let conn = rustls::ClientConnection::new(config.clone(), name)
            .map_err(|e| io::Error::other(format!("TLS setup: {e}")))?;
        let mut stream = TlsStream::handshake(sock, conn.into()).await?;
        exchange(&mut stream, head.as_bytes(), body).await
    } else {
        let mut sock = sock;
        exchange(&mut sock, head.as_bytes(), body).await
    }
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    head: &[u8],
    body: &[u8],
) -> io::Result<Response> {
    let mut out = Vec::with_capacity(head.len() + body.len());
    out.extend_from_slice(head);
    out.extend_from_slice(body);
    s.write_all(&out).await?;
    s.flush().await?;
    let (start, headers, mut rest) = read_head(s).await?;
    let mut parts = start.splitn(3, ' ');
    let version = parts.next().unwrap_or_default();
    if !version.starts_with("HTTP/1.") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("not an HTTP/1 response: '{start}'"),
        ));
    }
    let status: u16 = parts
        .next()
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP status line"))?;
    let chunked = header(&headers, "transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    let body = if chunked {
        read_chunked(s, rest).await?
    } else if let Some(len) = header(&headers, "content-length") {
        let len: usize = len
            .trim()
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid content-length"))?;
        read_exact_body(s, &mut rest, len).await?;
        rest
    } else {
        // Until the server closes the connection.
        let mut buf = [0u8; 8192];
        loop {
            match s.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    rest.extend_from_slice(&buf[..n]);
                    if rest.len() > MAX_BODY {
                        return Err(too_large());
                    }
                }
                // A peer that closes without close_notify still sent its body.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        rest
    };
    Ok(Response { status, body })
}

fn too_large() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "HTTP body too large")
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Reads a message head: the start line, the headers (names lowercased),
/// and whatever followed the blank line.
async fn read_head<S: AsyncRead + Unpin>(
    s: &mut S,
) -> io::Result<(String, Vec<(String, String)>, Vec<u8>)> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];
    let end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        if buf.len() > MAX_HEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP head too large",
            ));
        }
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the connection closed before an HTTP head",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let text = std::str::from_utf8(&buf[..end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "HTTP head is not UTF-8"))?;
    let mut lines = text.split("\r\n");
    let start = lines.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    for line in lines {
        let (k, v) = line
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP header"))?;
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
    }
    let rest = buf[end + 4..].to_vec();
    Ok((start, headers, rest))
}

async fn read_exact_body<S: AsyncRead + Unpin>(
    s: &mut S,
    rest: &mut Vec<u8>,
    len: usize,
) -> io::Result<()> {
    if len > MAX_BODY {
        return Err(too_large());
    }
    let mut chunk = [0u8; 8192];
    while rest.len() < len {
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the connection closed inside an HTTP body",
            ));
        }
        rest.extend_from_slice(&chunk[..n]);
    }
    rest.truncate(len);
    Ok(())
}

async fn read_chunked<S: AsyncRead + Unpin>(s: &mut S, mut buf: Vec<u8>) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    let mut chunk = [0u8; 8192];
    let mut more = async |buf: &mut Vec<u8>, s: &mut S| -> io::Result<()> {
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the connection closed inside a chunked body",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        Ok(())
    };
    loop {
        let line_end = loop {
            if let Some(i) = buf.windows(2).position(|w| w == b"\r\n") {
                break i;
            }
            more(&mut buf, s).await?;
        };
        let size_text = std::str::from_utf8(&buf[..line_end]).unwrap_or("");
        let size = usize::from_str_radix(size_text.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk size"))?;
        buf.drain(..line_end + 2);
        if size == 0 {
            return Ok(body);
        }
        if body.len() + size > MAX_BODY {
            return Err(too_large());
        }
        while buf.len() < size + 2 {
            more(&mut buf, s).await?;
        }
        body.extend_from_slice(&buf[..size]);
        buf.drain(..size + 2);
    }
}

/// A request a worker's endpoint received.
#[derive(Debug)]
pub(crate) struct Request {
    pub(crate) method: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl Request {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }
}

/// Reads one request with a `Content-Length` body (or none).
pub(crate) async fn read_request<S: AsyncRead + Unpin>(s: &mut S) -> io::Result<Request> {
    let (start, headers, mut rest) = read_head(s).await?;
    let method = start.split(' ').next().unwrap_or_default().to_owned();
    if header(&headers, "transfer-encoding").is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunked requests are not supported: send content-length",
        ));
    }
    let len = match header(&headers, "content-length") {
        Some(v) => v
            .trim()
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid content-length"))?,
        None => 0,
    };
    read_exact_body(s, &mut rest, len).await?;
    Ok(Request {
        method,
        headers,
        body: rest,
    })
}

/// Writes a response with a JSON body and closes the exchange.
pub(crate) async fn write_response<S: AsyncWrite + Unpin>(
    s: &mut S,
    status: u16,
    body: &[u8],
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let mut out = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    s.write_all(&out).await?;
    s.flush().await?;
    let _ = s.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        let u = Url::parse("https://fn.example.com/a/b?code=1").unwrap();
        assert_eq!(
            u,
            Url {
                tls: true,
                host: "fn.example.com".into(),
                port: 443,
                path: "/a/b?code=1".into()
            }
        );
        let u = Url::parse("http://127.0.0.1:8080").unwrap();
        assert_eq!((u.port, u.path.as_str(), u.tls), (8080, "/", false));
        let u = Url::parse("http://[::1]:9/x").unwrap();
        assert_eq!((u.host.as_str(), u.port), ("::1", 9));
        assert_eq!(u.host_header(), "[::1]:9");
        for bad in ["ftp://x", "http://", "http://a:b/", "http://u:p@h/"] {
            assert!(Url::parse(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn a_request_and_a_chunked_response() {
        let (mut a, mut b) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move {
            let req = read_request(&mut b).await.unwrap();
            assert_eq!(req.method, "POST");
            assert_eq!(req.header("x-test"), Some("1"));
            assert_eq!(req.body, b"hello");
            b.write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n")
                .await
                .unwrap();
        });
        let r = exchange(
            &mut a,
            b"POST /x HTTP/1.1\r\nhost: h\r\ncontent-length: 5\r\nx-test: 1\r\n\r\n",
            b"hello",
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert_eq!((r.status, r.body.as_slice()), (200, &b"abcde"[..]));
    }
}
