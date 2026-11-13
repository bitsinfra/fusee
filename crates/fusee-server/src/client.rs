//! Calls from other machines in server mode.
//!
//! Workflows run in the worker that holds the server's lease. Any other
//! process (another service, a webhook handler, an operator's script)
//! connects as a [`Client`]; the server relays its calls to the worker,
//! which runs them against its engine and answers through the server. So a
//! completion token can be finished from anywhere, and updates, queries
//! and signals reach the shard that owns the workflow.
//!
//! The worker opts in with [`serve_calls`]. Arguments and results are JSON.

use crate::proto::{self, Head, req, resp};
use crate::remote::{RelayedCall, Remote};
use fusee::{Engine, Error, StartOptions};
use fusee_core::frame;
use fusee_core::wire::{Reader, put_bytes};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Replies = HashMap<u64, Sender<Result<Vec<u8>, String>>>;
type Pending = Arc<Mutex<Option<Replies>>>;

/// The write half and the socket, closed when the last client handle goes.
struct Writer {
    stream: Mutex<crate::security::ConnWriter>,
    raw: crate::security::Raw,
}
impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.raw.shutdown();
    }
}

/// A connection that sends calls to whichever worker holds the shards.
#[derive(Clone)]
pub struct Client {
    writer: Arc<Writer>,
    pending: Pending,
    /// Why the server closed the connection, when it said so.
    refused: Arc<std::sync::OnceLock<String>>,
    next: Arc<AtomicU64>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish()
    }
}

impl Client {
    /// Connects to `addr` (`host:port`, `unix:/path.sock` or
    /// `replicas://...`, with optional connection options such as
    /// `?ca=ca.pem&token-file=token`; see
    /// [`Security::from_options`](crate::security::Security::from_options)).
    pub fn connect(addr: &str) -> io::Result<Self> {
        Self::open(crate::endpoint::Endpoint::parse(addr)?.connect()?)
    }

    /// Like [`connect`](Self::connect), with security given separately.
    pub fn connect_with(addr: &str, security: &crate::security::Security) -> io::Result<Self> {
        Self::open(crate::endpoint::Endpoint::with_security(addr, security)?.connect()?)
    }

    /// A client of `server` in this process, over a socket pair: no TLS or
    /// token (nothing leaves the process), a client's scope (calls, no
    /// shards). The server's side runs on the current Tokio runtime.
    pub fn in_process(server: &Arc<crate::Server>) -> io::Result<Self> {
        let (mine, theirs) = std::os::unix::net::UnixStream::pair()?;
        theirs.set_nonblocking(true)?;
        let theirs = tokio::net::UnixStream::from_std(theirs)?;
        tokio::spawn(
            server
                .clone()
                .serve_conn_scoped(theirs, crate::security::Scope::Client),
        );
        Self::open(crate::security::Conn::plain(crate::security::Raw::Unix(
            mine,
        ))?)
    }

    /// Whether the connection is gone: the server closed it, or it broke.
    /// Calls on a closed client fail at once.
    pub fn is_closed(&self) -> bool {
        self.pending.lock().map_or(true, |p| p.is_none())
    }

    fn open(mut stream: crate::security::Conn) -> io::Result<Self> {
        let mut out = Vec::new();
        frame::encode(req::CLIENT_HELLO, &[], &mut out);
        stream.write_all(&out)?;
        let (mut reader, writer, raw) = stream.split();
        let pending: Pending = Arc::new(Mutex::new(Some(HashMap::new())));
        let p = pending.clone();
        let refused = Arc::new(std::sync::OnceLock::new());
        let refused_by_reader = refused.clone();
        std::thread::Builder::new()
            .name("fusee-client".into())
            .spawn(move || {
                let mut dec = frame::Decoder::new();
                let mut buf = vec![0u8; 1 << 16];
                // An error for call 0 (calls start at 1) is the server
                // refusing the connection, for example for a missing token.
                let mut refusal = None;
                'read: loop {
                    loop {
                        let (kind, body) = match dec.next() {
                            Ok(Some(frame)) => frame,
                            Ok(None) => break,
                            Err(_) => break 'read,
                        };
                        let mut r = Reader::new(&body);
                        let Ok(id) = r.u64() else { break 'read };
                        let v = match kind {
                            resp::OK => Ok(r.rest().to_vec()),
                            resp::ERR => Err(r.str().unwrap_or("error").to_owned()),
                            _ => break 'read,
                        };
                        if id == 0 {
                            refusal = v.err();
                        } else if let Some(tx) =
                            p.lock().ok().and_then(|mut m| m.as_mut()?.remove(&id))
                        {
                            let _ = tx.send(v);
                        }
                    }
                    match reader.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => dec.feed(&buf[..n]),
                    }
                }
                let reason = match refusal {
                    Some(r) => {
                        let reason = format!("the server refused this client: {r}");
                        let _ = refused_by_reader.set(reason.clone());
                        reason
                    }
                    None => "connection to the server was lost".into(),
                };
                if let Some(calls) = p.lock().unwrap_or_else(|e| e.into_inner()).take() {
                    for tx in calls.into_values() {
                        let _ = tx.send(Err(reason.clone()));
                    }
                }
            })?;
        Ok(Self {
            writer: Arc::new(Writer {
                stream: Mutex::new(writer),
                raw,
            }),
            pending,
            refused,
            next: Arc::new(AtomicU64::new(1)),
        })
    }

    /// The error for a call on a closed connection.
    fn closed(&self) -> Error {
        match self.refused.get() {
            Some(reason) => Error::Io(reason.clone()),
            None => Error::Closed,
        }
    }

    /// Closes the transport and wakes calls waiting for replies.
    pub fn close(&self) -> io::Result<()> {
        self.writer.raw.shutdown()
    }

    /// Sends a call and waits for the worker's answer.
    pub fn call(&self, method: &str, args: &Value) -> Result<Value, Error> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = channel();
        self.pending
            .lock()
            .map_err(|_| Error::Closed)?
            .as_mut()
            .ok_or_else(|| self.closed())?
            .insert(id, tx);
        let mut b = Vec::new();
        proto::head(
            &mut b,
            Head {
                id,
                shard: 0,
                epoch: 0,
            },
        );
        put_bytes(&mut b, method.as_bytes());
        put_bytes(&mut b, args.to_string().as_bytes());
        let mut out = Vec::new();
        frame::encode(req::CALL, &b, &mut out);
        let sent = self
            .writer
            .stream
            .lock()
            .map_err(|_| Error::Closed)
            .and_then(|mut sock| sock.write_all(&out).map_err(Error::from));
        if let Err(e) = sent {
            self.pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_mut()
                .and_then(|calls| calls.remove(&id));
            return Err(e);
        }
        let result = if method == "result" {
            rx.recv()
                .map_err(|_| std::sync::mpsc::RecvTimeoutError::Disconnected)
        } else {
            rx.recv_timeout(Duration::from_secs(120))
        };
        if result.is_err() {
            self.pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_mut()
                .and_then(|calls| calls.remove(&id));
        }
        match result {
            Ok(Ok(v)) => {
                let v: Value = serde_json::from_slice(&v)?;
                match v.get("error").and_then(Value::as_str) {
                    Some(e) => Err(decode_error(v.get("kind").and_then(Value::as_str), e)),
                    None => Ok(v.get("value").cloned().unwrap_or(Value::Null)),
                }
            }
            Ok(Err(m)) => Err(Error::Io(m)),
            Err(_) => Err(Error::Io("the server did not answer".into())),
        }
    }

    /// Finishes an async completion from its token.
    pub fn complete(&self, token: &str, value: &Value) -> Result<(), Error> {
        self.call("complete", &json!({"token": token, "value": value}))
            .map(|_| ())
    }

    pub fn fail(&self, token: &str, error: &str) -> Result<(), Error> {
        self.call("fail", &json!({"token": token, "error": error}))
            .map(|_| ())
    }

    pub fn start(&self, wtype: &str, id: &str, input: &Value) -> Result<(), Error> {
        self.call("start", &json!({"type": wtype, "id": id, "input": input}))
            .map(|_| ())
    }

    /// Starts or attaches to a workflow with the same options as an embedded worker.
    pub fn start_with(
        &self,
        wtype: &str,
        id: &str,
        input: &Value,
        options: &StartOptions,
    ) -> Result<(), Error> {
        self.call(
            "start",
            &json!({"type":wtype, "id":id, "input":input,
            "version": options.version, "options": wire_options(options)}),
        )
        .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_with_start(
        &self,
        wtype: &str,
        id: &str,
        input: &Value,
        options: &StartOptions,
        name: &str,
        update_id: &str,
        arg: &Value,
    ) -> Result<Value, Error> {
        self.call(
            "updateWithStart",
            &json!({"type":wtype,"id":id,"input":input,"version":options.version,
            "options":wire_options(options),"name":name,"updateId":update_id,"arg":arg}),
        )
    }

    /// Runs matching `filter`, up to `limit` per shard, from every worker
    /// holding shards, in shard order. Fails at once if a shard has no
    /// holder right now.
    pub fn list_where(&self, filter: &Value, limit: usize) -> Result<Value, Error> {
        self.call("list", &json!({"where":filter,"limit":limit}))
    }

    /// Runs, up to `limit` per shard, as [`list_where`](Self::list_where)
    /// without a filter.
    pub fn list(&self, limit: usize) -> Result<Value, Error> {
        self.call("list", &json!({"limit":limit}))
    }

    pub fn reset(&self, id: &str, before_step: &str, reapply_signals: bool) -> Result<(), Error> {
        self.call(
            "reset",
            &json!({"id":id,"beforeStep":before_step,"reapplySignals":reapply_signals}),
        )
        .map(|_| ())
    }

    pub fn signal(&self, id: &str, name: &str, value: &Value) -> Result<(), Error> {
        self.call("signal", &json!({"id": id, "name": name, "value": value}))
            .map(|_| ())
    }

    pub fn update(
        &self,
        id: &str,
        name: &str,
        update_id: &str,
        arg: &Value,
    ) -> Result<Value, Error> {
        self.call(
            "update",
            &json!({"id": id, "name": name, "updateId": update_id, "arg": arg}),
        )
    }

    pub fn query(&self, id: &str, name: &str, arg: &Value) -> Result<Value, Error> {
        self.call("query", &json!({"id": id, "name": name, "arg": arg}))
    }

    /// Calls handler `handler` of key `key` of object type `wtype` (see
    /// [`fusee::object`]) and waits for its durable result. Calls with the
    /// same `call_id` (empty: none) run once and get the recorded result.
    pub fn object_call(
        &self,
        wtype: &str,
        key: &str,
        handler: &str,
        arg: &Value,
        call_id: &str,
    ) -> Result<Value, Error> {
        self.update_with_start(
            wtype,
            &fusee::object::object_id(wtype, key),
            &Value::Null,
            &StartOptions::default().use_existing(),
            handler,
            call_id,
            arg,
        )
    }

    /// [`object_call`](Self::object_call) without waiting for the result:
    /// returns once the call is accepted.
    pub fn object_send(
        &self,
        wtype: &str,
        key: &str,
        handler: &str,
        arg: &Value,
        call_id: &str,
    ) -> Result<(), Error> {
        let id = format!("{}{call_id}", fusee::object::SEND_PREFIX);
        self.object_call(wtype, key, handler, arg, &id).map(|_| ())
    }

    /// The committed state of key `key` of object type `wtype` (`{}` for a
    /// key never called).
    pub fn object_state(&self, wtype: &str, key: &str) -> Result<Value, Error> {
        match self.query(
            &fusee::object::object_id(wtype, key),
            fusee::object::STATE_QUERY,
            &Value::Null,
        ) {
            Err(Error::NotFound(_)) => Ok(json!({})),
            r => r,
        }
    }

    pub fn result(&self, id: &str) -> Result<Value, Error> {
        self.call("result", &json!({"id": id}))
    }

    pub fn cancel(&self, id: &str, reason: &str) -> Result<(), Error> {
        self.call("cancel", &json!({"id": id, "reason": reason}))
            .map(|_| ())
    }

    /// Resumes a paused run: its step runs again with a fresh retry budget.
    pub fn resume(&self, id: &str) -> Result<(), Error> {
        self.call("resume", &json!({ "id": id })).map(|_| ())
    }

    pub fn terminate(&self, id: &str, reason: &str) -> Result<(), Error> {
        self.call("terminate", &json!({"id": id, "reason": reason}))
            .map(|_| ())
    }

    /// One read of stream `name` of workflow `id` from offset `from`: the
    /// durable chunks there are, else those appended within `wait_ms` (at
    /// most a minute), and the end once the read has every chunk. `run`
    /// keeps reading the run an earlier read named. The worker holding the
    /// ID's shard answers.
    pub fn stream_read(
        &self,
        id: &str,
        run: Option<u64>,
        name: &str,
        from: u64,
        wait_ms: u64,
    ) -> Result<fusee::StreamBatch, Error> {
        let v = self.call(
            "streamRead",
            &json!({"id": id, "run": run, "name": name, "from": from, "waitMs": wait_ms}),
        )?;
        fusee::StreamBatch::from_json(&v)
    }

    /// A reader of stream `name` of workflow `id` from offset `from`: an
    /// iterator of chunks that blocks until each is durable and ends once
    /// the stream is complete. After a lost connection, a new reader from
    /// [`ClientStream::offset`] continues exactly there.
    pub fn stream(&self, id: &str, name: &str, from: u64) -> ClientStream {
        ClientStream {
            client: self.clone(),
            id: id.to_owned(),
            name: name.to_owned(),
            run: None,
            next: from,
            buf: Default::default(),
            end: None,
        }
    }
}

/// A stream read through a server ([`Client::stream`]).
#[derive(Debug)]
pub struct ClientStream {
    client: Client,
    id: String,
    name: String,
    run: Option<u64>,
    next: u64,
    buf: std::collections::VecDeque<fusee::Chunk>,
    end: Option<fusee::StreamEnd>,
}

impl ClientStream {
    /// The offset of the next chunk.
    pub fn offset(&self) -> u64 {
        self.buf.front().map_or(self.next, |c| c.offset)
    }

    /// How the stream ended, once the iterator is done.
    pub fn end(&self) -> Option<&fusee::StreamEnd> {
        if self.buf.is_empty() {
            self.end.as_ref()
        } else {
            None
        }
    }
}

impl Iterator for ClientStream {
    type Item = Result<fusee::Chunk, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(c) = self.buf.pop_front() {
                return Some(Ok(c));
            }
            if self.end.is_some() {
                return None;
            }
            let b = match self.client.stream_read(
                &self.id,
                self.run,
                &self.name,
                self.next,
                fusee::stream::WAIT_MS,
            ) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            self.run = Some(b.run);
            self.next = self.next.max(b.next);
            self.buf.extend(b.chunks);
            self.end = b.end;
        }
    }
}

use fusee::http::calls::{decode_error, wire_options};

/// Serves calls other machines send through the server, until the worker's
/// connection closes. Spawn it next to the engine:
///
/// ```no_run
/// # async fn demo() -> std::io::Result<()> {
/// let (cfg, remote) = fusee_server::remote_worker("127.0.0.1:7600")?;
/// let engine = fusee::Engine::open(cfg).await.map_err(std::io::Error::other)?;
/// tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));
/// # Ok(()) }
/// ```
pub async fn serve_calls(remote: Remote, engine: Engine) {
    let mut calls = remote.take_calls();
    let inner = remote.inner();
    while let Some(RelayedCall {
        relay,
        method,
        args,
    }) = calls.recv().await
    {
        let (engine, inner) = (engine.clone(), inner.clone());
        tokio::spawn(async move {
            let a: Value = serde_json::from_slice(&args).unwrap_or(Value::Null);
            // A failure crosses as its envelope, so the caller revives it.
            let out = fusee::http::answer(fusee::http::run_call(&engine, &method, &a).await);
            let _ = inner.reply_call(relay, Ok(out.to_string().into_bytes()));
        });
    }
}

/// The protocol calls of a server, answered in its own process: the HTTP
/// API of `fusee server --http`. Calls go through an
/// [in-process](Client::in_process) client connection, opened again
/// whenever the server closed it (a replica closes its clients on every
/// view change, and every connection while it is not the ready primary).
/// A call that only reads is sent again once if the connection broke under
/// it; any other fails, since the server may have run it.
pub struct LocalCalls {
    server: Arc<crate::Server>,
    runtime: tokio::runtime::Handle,
    client: Mutex<Option<Client>>,
}

impl LocalCalls {
    /// Must be called within a Tokio runtime, which serves the connections.
    pub fn new(server: &Arc<crate::Server>) -> Self {
        Self {
            server: server.clone(),
            runtime: tokio::runtime::Handle::current(),
            client: Mutex::new(None),
        }
    }

    fn client(&self) -> Result<Client, Error> {
        if let Some(why) = self.server.not_serving() {
            return Err(Error::Connection(why));
        }
        let mut slot = self.client.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = slot.as_ref().filter(|c| !c.is_closed()) {
            return Ok(c.clone());
        }
        let _runtime = self.runtime.enter();
        let fresh = Client::in_process(&self.server)
            .map_err(|e| Error::Connection(format!("cannot reach this server: {e}")))?;
        *slot = Some(fresh.clone());
        Ok(fresh)
    }

    async fn once(
        &self,
        method: &str,
        args: &Value,
    ) -> Result<(Client, Result<Value, Error>), Error> {
        let client = self.client()?;
        let (c, method, args) = (client.clone(), method.to_owned(), args.clone());
        let r = tokio::task::spawn_blocking(move || c.call(&method, &args))
            .await
            .map_err(|e| Error::Io(e.to_string()))?;
        Ok((client, r))
    }
}

impl fusee::http::Calls for LocalCalls {
    fn call<'a>(&'a self, method: &'a str, args: Value) -> fusee::http::calls::CallFuture<'a> {
        Box::pin(async move {
            let (client, r) = self.once(method, &args).await?;
            if r.is_err() && client.is_closed() && crate::security::READ_CALLS.contains(&method) {
                return self.once(method, &args).await?.1;
            }
            r
        })
    }
}
