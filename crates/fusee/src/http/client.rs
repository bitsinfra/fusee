//! A client of the HTTP API: protocol calls over `POST /v1/call/{method}`,
//! for tools and languages without an SDK (`fusee` the CLI uses it).

use super::calls::{CallFuture, Calls};
use crate::error::{Error, Result};
use crate::failure::Failure;
use serde_json::Value;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Calls an engine or server through its HTTP API.
#[derive(Clone, Debug)]
pub struct HttpClient {
    host: String,
    port: u16,
    token: Option<String>,
    timeout: Duration,
}

impl HttpClient {
    /// `url`: `http://host:port` (a path after it is ignored).
    pub fn new(url: &str, token: Option<String>) -> Result<Self> {
        let rest = url.strip_prefix("http://").ok_or_else(|| {
            Error::Rejected(format!(
                "'{url}' is not an http:// URL (the HTTP API is plain HTTP; put TLS in front of it)"
            ))
        })?;
        let authority = rest.split('/').next().unwrap_or(rest);
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (
                h.to_owned(),
                p.parse::<u16>()
                    .map_err(|_| Error::Rejected(format!("'{url}' has no valid port")))?,
            ),
            None => (authority.to_owned(), 80),
        };
        Ok(Self {
            host,
            port,
            token,
            timeout: Duration::from_secs(120),
        })
    }

    pub fn url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    /// One request; the body is JSON. Returns the status and the body.
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Vec<u8>)> {
        let addr = format!("{}:{}", self.host, self.port);
        let fut = async {
            let mut s = TcpStream::connect(&addr).await.map_err(|e| {
                Error::Connection(format!(
                    "no Fusee HTTP API at {} ({e}). Start one with `fusee dev`, `engine.serve()`, or \
                     `fusee server --http {addr}`",
                    self.url()
                ))
            })?;
            let body = body.map(|b| b.to_string()).unwrap_or_default();
            let mut head = format!(
                "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\n"
            );
            if !body.is_empty() || method != "GET" {
                head.push_str(&format!(
                    "Content-Type: application/json\r\nContent-Length: {}\r\n",
                    body.len()
                ));
            }
            if let Some(t) = &self.token {
                head.push_str(&format!("Authorization: Bearer {t}\r\n"));
            }
            head.push_str("\r\n");
            s.write_all(head.as_bytes()).await?;
            s.write_all(body.as_bytes()).await?;
            let mut out = Vec::new();
            s.read_to_end(&mut out).await?;
            let end = out
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .ok_or_else(|| Error::Io(format!("{} sent no HTTP response", self.url())))?;
            let status = std::str::from_utf8(&out[..end])
                .ok()
                .and_then(|h| h.split(' ').nth(1))
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);
            Ok((status, out[end + 4..].to_vec()))
        };
        tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| Error::Connection(format!("{} did not answer in time", self.url())))?
    }

    /// A call by protocol method name.
    pub async fn call_method(&self, method: &str, args: Value) -> Result<Value> {
        let (status, body) = self
            .request("POST", &format!("/v1/call/{method}"), Some(&args))
            .await?;
        let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if status == 200 {
            return Ok(v);
        }
        Err(error_from(&v, status))
    }
}

/// The error an HTTP answer carries (`{"error": {code, message, failure}}`).
pub fn error_from(v: &Value, status: u16) -> Error {
    let e = &v["error"];
    let message = e["message"].as_str().unwrap_or("").to_owned();
    if let Some(f) = e.get("failure").filter(|f| !f.is_null())
        && let Ok(f) = serde_json::from_value::<Failure>(f.clone())
    {
        return Error::from_failure(f);
    }
    match e["code"].as_str().unwrap_or("") {
        "NotFound" => Error::NotFound(message),
        "AlreadyExists" => Error::AlreadyExists(message),
        "UnknownWorkflow" => Error::UnknownWorkflow(message),
        "Rejected" => Error::Rejected(message),
        "Cancelled" => Error::Cancelled(message.trim_start_matches("cancelled: ").to_owned()),
        "TimedOut" => Error::TimedOut,
        "Terminated" => Error::Terminated,
        "Determinism" => Error::Determinism(message),
        "Codec" => Error::Codec(message),
        "Connection" => Error::Connection(message),
        "Busy" => Error::Busy,
        "Closed" => Error::Closed,
        "Io" => Error::Io(message),
        _ if message.is_empty() => Error::Io(format!("the HTTP API answered {status}: {v}")),
        _ => Error::failed(message),
    }
}

impl Calls for HttpClient {
    fn call<'a>(&'a self, method: &'a str, args: Value) -> CallFuture<'a> {
        Box::pin(async move { self.call_method(method, args).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn errors_come_back_as_their_codes() {
        let v = json!({"error": {"code": "NotFound", "message": "workflow 'x' not found"}});
        assert_eq!(
            error_from(&v, 404),
            Error::NotFound("workflow 'x' not found".into())
        );
        let f = Failure::fatal("declined").kind("CardDeclined");
        let step = Error::Step {
            name: "pay".into(),
            failure: f,
        };
        let v =
            json!({"error": {"code": "StepFailed", "message": "x", "failure": step.to_failure()}});
        assert_eq!(error_from(&v, 500), step);
    }

    #[test]
    fn urls() {
        let c = HttpClient::new("http://127.0.0.1:7601/", None).unwrap();
        assert_eq!(c.url(), "http://127.0.0.1:7601");
        assert!(HttpClient::new("127.0.0.1:7601", None).is_err());
    }
}
