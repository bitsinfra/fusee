//! Durable streams: chunks a run appends to a named stream (tokens from a
//! model, progress events) and readers that follow them from an offset.
//!
//! ```no_run
//! use fusee::{Ctx, Engine, Error};
//!
//! # async fn demo(engine: Engine) -> Result<(), Error> {
//! engine.register("answer", |ctx: Ctx, prompt: String| async move {
//!     let c = ctx.clone();
//!     ctx.step("generate", move || {
//!         let c = c.clone();
//!         let prompt = prompt.clone();
//!         async move {
//!             let out = c.stream("tokens")?;
//!             // Chunks an earlier attempt recorded: continue after them.
//!             let mut text: String = out.recorded::<String>()?.concat();
//!             for token in [" a", " b"].into_iter().skip(text.len() / 2) {
//!                 out.append(token)?;
//!                 text.push_str(token);
//!             }
//!             let _ = prompt;
//!             Ok::<_, Error>(text)
//!         }
//!     })
//!     .await
//! });
//! let mut tokens = engine.run::<String>("q-1").stream("tokens");
//! while let Some(chunk) = tokens.next().await? {
//!     print!("{}", chunk.json::<String>()?);
//! }
//! # Ok(()) }
//! ```
//!
//! The rules (docs/features.md#streams):
//!
//! - Appending records a `Chunk` and returns at once; chunks go to the log
//!   with whatever else flushes next. A reader sees a chunk only once it is
//!   durable (committed, when replicated), so a crash never takes back a
//!   chunk a reader has seen.
//! - Inside a step, a chunk records the step's attempt. A step that runs
//!   again (a retry, or after a crash) keeps the chunks earlier attempts
//!   recorded and continues after them: [`StreamWriter::recorded`] returns
//!   them. The stream a step opened is completed by the step's final result.
//! - In workflow code, appends are journaled once: a replay of the code
//!   matches its appends to the recorded ones in order and appends nothing.
//!   `close()` completes the stream, as the run's end does.

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use fusee_core::record::{Payload, RunId, Seq};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::Arc;

/// Most payload bytes one read returns (at least one chunk, whatever its size).
pub const BATCH_BYTES: usize = 256 << 10;
/// Most chunks one read returns.
pub const BATCH_CHUNKS: usize = 4096;
/// How long a read waits for a chunk by default before it answers with
/// none (a long poll; the reader then asks again).
pub const WAIT_MS: u64 = 30_000;

/// A chunk's value: JSON, or bytes.
#[derive(Clone, Debug, PartialEq)]
pub enum ChunkValue {
    Json(Value),
    Bytes(Vec<u8>),
}

/// One chunk as a reader receives it.
#[derive(Clone, Debug, PartialEq)]
pub struct Chunk {
    /// Its place in the stream: 0, 1, 2, ...
    pub offset: u64,
    pub value: ChunkValue,
}

impl Chunk {
    /// The chunk decoded from JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        match &self.value {
            ChunkValue::Json(v) => Ok(T::deserialize(v)?),
            ChunkValue::Bytes(_) => Err(Error::Codec(format!(
                "chunk {} holds bytes, not JSON",
                self.offset
            ))),
        }
    }

    /// The chunk's bytes, if it was appended as bytes.
    pub fn bytes(&self) -> Option<&[u8]> {
        match &self.value {
            ChunkValue::Bytes(b) => Some(b),
            ChunkValue::Json(_) => None,
        }
    }
}

/// How a stream ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamEnd {
    /// `ok` for a stream its writer closed or whose step succeeded; the
    /// step's status (`failed`, `cancelled`) when its step ended otherwise;
    /// the run's status (`ok`, `failed`, `terminated`, `continued_as_new`
    /// ...) when the run ended with the stream open.
    pub status: String,
    /// A `StreamEnd` record completed it (rather than the run's end).
    pub closed: bool,
}

/// One read's answer: the durable chunks from `from` on, and the end once
/// the reader has every chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamBatch {
    /// The run the stream belongs to. A reader passes it back to keep
    /// reading that run when its workflow ID moves on to another.
    pub run: RunId,
    pub from: u64,
    /// The offset to read from next.
    pub next: u64,
    pub chunks: Vec<Chunk>,
    pub end: Option<StreamEnd>,
}

impl StreamBatch {
    /// Decodes a read's answer (JSON, as every SDK and the server get it).
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        Self::from_json(&serde_json::from_slice(bytes)?)
    }

    /// [`Self::parse`] from a JSON value.
    pub fn from_json(v: &Value) -> Result<Self> {
        let bad = || Error::Codec("malformed stream read".into());
        let run = v["run"].as_u64().ok_or_else(bad)?;
        let from = v["from"].as_u64().ok_or_else(bad)?;
        let next = v["next"].as_u64().ok_or_else(bad)?;
        let values = v["chunks"].as_array().ok_or_else(bad)?;
        let bytes: Vec<usize> = match v.get("bytes").and_then(Value::as_array) {
            Some(b) => b
                .iter()
                .map(|i| i.as_u64().map(|i| i as usize).ok_or_else(bad))
                .collect::<Result<_>>()?,
            None => Vec::new(),
        };
        let mut chunks = Vec::with_capacity(values.len());
        for (i, value) in values.iter().enumerate() {
            let value = if bytes.binary_search(&i).is_ok() {
                ChunkValue::Bytes(base64_decode(value.as_str().ok_or_else(bad)?).ok_or_else(bad)?)
            } else {
                ChunkValue::Json(value.clone())
            };
            chunks.push(Chunk {
                offset: from + i as u64,
                value,
            });
        }
        let end = match &v["end"] {
            Value::Null => None,
            e => Some(StreamEnd {
                status: e["status"].as_str().ok_or_else(bad)?.to_owned(),
                closed: e["closed"].as_bool().unwrap_or(false),
            }),
        };
        Ok(Self {
            run,
            from,
            next,
            chunks,
            end,
        })
    }
}

/// Encodes a read's answer. `chunks` are tagged payloads as recorded
/// (decrypted): raw bytes after a zero byte, JSON otherwise.
pub(crate) fn encode_batch(
    run: RunId,
    from: u64,
    chunks: &[Payload],
    end: Option<&StreamEnd>,
) -> Vec<u8> {
    use std::fmt::Write;
    let size: usize = chunks.iter().map(|c| c.len() + 2).sum();
    let mut s = String::with_capacity(96 + size + size / 3);
    let next = from + chunks.len() as u64;
    let _ = write!(
        s,
        "{{\"run\":{run},\"from\":{from},\"next\":{next},\"chunks\":["
    );
    let mut bytes = Vec::new();
    for (i, c) in chunks.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        match c.first() {
            None => s.push_str("null"),
            Some(0) => {
                bytes.push(i);
                s.push('"');
                base64_encode(&c[1..], &mut s);
                s.push('"');
            }
            Some(_) => match std::str::from_utf8(c) {
                Ok(text) => s.push_str(text),
                // Not JSON text: hand it out as bytes rather than break the
                // answer.
                Err(_) => {
                    bytes.push(i);
                    s.push('"');
                    base64_encode(c, &mut s);
                    s.push('"');
                }
            },
        }
    }
    s.push_str("],\"bytes\":[");
    for (k, i) in bytes.iter().enumerate() {
        if k > 0 {
            s.push(',');
        }
        let _ = write!(s, "{i}");
    }
    s.push_str("],\"end\":");
    match end {
        None => s.push_str("null"),
        Some(e) => {
            let _ = write!(s, "{{\"status\":\"{}\",\"closed\":{}}}", e.status, e.closed);
        }
    }
    s.push('}');
    s.into_bytes()
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding.
pub(crate) fn base64_encode(b: &[u8], out: &mut String) {
    for c in b.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(c.get(1).copied().unwrap_or(0)) << 8)
            | u32::from(c.get(2).copied().unwrap_or(0));
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
}

pub(crate) fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let val = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => return None,
        } as u32)
    };
    let s = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for c in s.chunks(4) {
        if c.len() == 1 {
            return None;
        }
        let mut n = 0u32;
        for (i, b) in c.iter().enumerate() {
            n |= val(*b)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if c.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if c.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

/// A writer of one of the run's streams ([`Ctx::stream`]). Opened in
/// workflow code, it writes the run's stream, which `close()` or the run's
/// end completes; opened inside a step's function, the step's stream,
/// which the step's final result completes.
#[derive(Clone)]
pub struct StreamWriter {
    ctx: Ctx,
    name: Arc<str>,
    step: Option<Seq>,
    recorded: u64,
    closed: bool,
}

impl std::fmt::Debug for StreamWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamWriter")
            .field("name", &self.name)
            .field("step", &self.step)
            .finish()
    }
}

/// A tagged payload of raw bytes: a zero byte, then the bytes.
fn tagged_bytes(b: &[u8]) -> Payload {
    let mut v = Vec::with_capacity(b.len() + 1);
    v.push(0);
    v.extend_from_slice(b);
    Arc::from(v)
}

impl StreamWriter {
    pub(crate) fn open(ctx: &Ctx, name: &str) -> Result<Self> {
        let step = ctx.running_step();
        let open = ctx.host_ref().stream_open(ctx.run_id(), name, step)?;
        Ok(Self {
            ctx: ctx.clone(),
            name: Arc::from(name),
            step,
            recorded: open.recorded,
            closed: open.closed,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Appends `value` as JSON. Never waits for the disk. Inside a step it
    /// records the running attempt; in workflow code a replay appends
    /// nothing.
    pub fn append<T: Serialize + ?Sized>(&self, value: &T) -> Result<()> {
        self.append_raw(crate::ctx::encode(value)?)
    }

    /// Appends raw bytes; readers get them back as bytes.
    pub fn append_bytes(&self, bytes: &[u8]) -> Result<()> {
        self.append_raw(tagged_bytes(bytes))
    }

    fn append_raw(&self, payload: Payload) -> Result<()> {
        self.ctx.host_ref().stream_append(
            self.ctx.run_id(),
            &self.name,
            self.ctx.running_step(),
            payload,
        )
    }

    /// Completes the stream: readers end once they have every chunk.
    pub fn close(&self) -> Result<()> {
        self.ctx
            .host_ref()
            .stream_close(self.ctx.run_id(), &self.name, self.ctx.running_step())
    }

    /// Inside a step: how many chunks the step's earlier attempts recorded
    /// (after a crash, the durable ones). 0 on a first attempt and in
    /// workflow code.
    pub fn recorded_len(&self) -> u64 {
        self.recorded
    }

    /// The chunks the step's earlier attempts recorded, decoded from JSON:
    /// a step that runs again continues after them (for a model, by passing
    /// them back as the start of its answer), so readers see no chunk twice.
    pub fn recorded<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.recorded_raw()?
            .iter()
            .map(|p| match p.first() {
                Some(0) => Err(Error::Codec("a recorded chunk holds bytes".into())),
                _ => crate::ctx::decode(p),
            })
            .collect()
    }

    /// [`Self::recorded`] as payloads: JSON, or bytes after a zero byte.
    pub fn recorded_raw(&self) -> Result<Vec<Payload>> {
        match self.step {
            Some(seq) if self.recorded > 0 => self.ctx.host_ref().stream_recorded(
                self.ctx.run_id(),
                &self.name,
                seq,
                self.recorded,
            ),
            _ => Ok(Vec::new()),
        }
    }

    /// The stream was complete when it was opened: the step completed it in
    /// an earlier attempt (its appends are dropped), or it was closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

/// A reader of a run's stream ([`crate::RunHandle::stream`]): yields each
/// chunk once it is durable, in order, and ends once the stream is
/// complete. [`Self::offset`] is where a reader that reconnects continues.
pub struct StreamReader {
    engine: crate::Engine,
    id: String,
    name: String,
    run: Option<RunId>,
    next: u64,
    wait: std::time::Duration,
    buf: std::collections::VecDeque<Chunk>,
    end: Option<StreamEnd>,
}

impl std::fmt::Debug for StreamReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamReader")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("offset", &self.offset())
            .finish()
    }
}

impl StreamReader {
    pub(crate) fn new(engine: crate::Engine, id: &str, name: &str) -> Self {
        Self {
            engine,
            id: id.to_owned(),
            name: name.to_owned(),
            run: None,
            next: 0,
            wait: std::time::Duration::from_millis(WAIT_MS),
            buf: Default::default(),
            end: None,
        }
    }

    /// Starts at chunk `offset` instead of the first.
    pub fn from(mut self, offset: u64) -> Self {
        self.next = offset;
        self
    }

    /// Reads the stream of `run` (an earlier reader's [`Self::run`]), not
    /// the workflow ID's newest run.
    pub fn of_run(mut self, run: RunId) -> Self {
        self.run = Some(run);
        self
    }

    /// How long one read waits for a new chunk before it asks again
    /// (default 30 s); it only bounds a request, not the reader.
    pub fn wait(mut self, wait: std::time::Duration) -> Self {
        self.wait = wait;
        self
    }

    /// The offset of the chunk [`Self::next`] returns next.
    pub fn offset(&self) -> u64 {
        self.buf.front().map_or(self.next, |c| c.offset)
    }

    /// The run being read, once the first read found it.
    pub fn run(&self) -> Option<RunId> {
        self.run
    }

    /// How the stream ended, once [`Self::next`] returned `None`.
    pub fn end(&self) -> Option<&StreamEnd> {
        if self.buf.is_empty() {
            self.end.as_ref()
        } else {
            None
        }
    }

    /// The next chunk, waiting until one is durable; `None` once the stream
    /// is complete.
    pub async fn next(&mut self) -> Result<Option<Chunk>> {
        loop {
            if let Some(c) = self.buf.pop_front() {
                return Ok(Some(c));
            }
            if self.end.is_some() {
                return Ok(None);
            }
            let b = self
                .engine
                .stream_read(&self.id, self.run, &self.name, self.next, self.wait)
                .await?;
            self.run = Some(b.run);
            self.next = self.next.max(b.next);
            self.buf.extend(b.chunks);
            self.end = b.end;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips() {
        for n in 0..40usize {
            let b: Vec<u8> = (0..n as u8).map(|i| i.wrapping_mul(37)).collect();
            let mut s = String::new();
            base64_encode(&b, &mut s);
            assert_eq!(base64_decode(&s).unwrap(), b, "{n}");
        }
        let mut s = String::new();
        base64_encode(b"hello", &mut s);
        assert_eq!(s, "aGVsbG8=");
    }

    #[test]
    fn a_batch_round_trips() {
        let chunks: Vec<Payload> = vec![
            Arc::from(&b"\"tok\""[..]),
            Arc::from(&b"\0\x01\x02"[..]),
            Arc::from(&b"{\"a\":1}"[..]),
            Arc::from(&b""[..]),
        ];
        let end = StreamEnd {
            status: "ok".into(),
            closed: true,
        };
        let b = StreamBatch::parse(&encode_batch(9, 5, &chunks, Some(&end))).unwrap();
        assert_eq!(b.run, 9);
        assert_eq!((b.from, b.next), (5, 9));
        assert_eq!(b.chunks[0].json::<String>().unwrap(), "tok");
        assert_eq!(b.chunks[1].bytes(), Some(&[1u8, 2][..]));
        assert_eq!(b.chunks[2].offset, 7);
        assert_eq!(b.chunks[3].value, ChunkValue::Json(Value::Null));
        assert_eq!(b.end, Some(end));
    }
}
