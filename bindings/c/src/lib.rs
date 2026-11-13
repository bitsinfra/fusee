//! C interface to the Fusee engine for SDKs whose workflow code runs in the
//! host language (Go and Java). `include/fusee.h` is the header and
//! documents every function; the rules in short:
//!
//! - An engine handle is not thread-safe, but it is not bound to a thread
//!   either: calls on one engine must not overlap, and each must happen
//!   after the previous one returned (one owning thread, or a lock, or a
//!   scheduler that hands the engine from one thread to the next with a
//!   synchronizing handoff). Engines are independent of each other.
//! - The notify callback runs on any thread, at most once between two
//!   ticks; it must only wake the host's loop, never call into the engine.
//! - Inputs are borrowed for the call. Outputs that point into the engine
//!   stay valid until the next call on that engine (tick outputs until the
//!   next tick). `fusee_buf` outputs are owned by the caller and freed with
//!   `fusee_buf_free`.
//! - Every function returns a status code; the message of the last failure
//!   on an engine is `fusee_engine_error`. No panic crosses the boundary: a
//!   panic is reported as `FUSEE_PANIC` and poisons the engine.
//!
//! Payloads cross in their recorded form: JSON text, or a zero byte
//! followed by raw bytes.
//!
//! Why the engine may move between threads: [`ForeignEngine`] is `!Send`
//! because it holds `Rc` and `Cell` values. Every one of them belongs to the
//! engine and is reached only through this handle, and the host contract
//! guarantees that no two threads touch the handle at once and that each
//! call happens after the previous one, so none of them is ever accessed
//! concurrently. The engine's thread-local state (the runtime's tick-local
//! ready list, encode scratch buffers) is set and cleared within a call.

#![allow(non_camel_case_types)]

use fusee::engine::Config;
use fusee::foreign::{EngineEvent, ForeignEngine};
use fusee::limits::{Acquire, LimitSpec};
use fusee::local::FlushMethod;
use fusee::runtime::{Delivery, ResultState, StartOutcome, StartRequest, UpdateRequest};
use fusee::visibility::ListFilter;
use fusee_core::record::{Payload, RunId, Status};
use fusee_core::run::{AwaitStart, StepStart};
use fusee_server::foreign::ForeignConnection;
use std::collections::HashSet;
use std::ffi::{CStr, c_char, c_void};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Bumped when a struct layout or a function signature changes, or when
/// functions or journal operations are added (a host that needs them checks
/// for at least the version that added them). 2: durable streams
/// (`FUSEE_OP_STREAM_*`, `fusee_engine_stream_read`). 3: keyed objects
/// (`fusee_engine_register_object`, `FUSEE_OP_OBJECT_*`,
/// `FUSEE_OP_BEGIN_CALL`; listings and `inspect` carry `object`), and
/// `fusee_engine_invocation_state` for invoked workers. 4: paused steps
/// (`fusee_engine_resume`, `FUSEE_OP_STEP_PAUSED`, a retry time of
/// `u64::MAX`) and state edits (`FUSEE_OP_OBJECT_EDIT`).
pub const FUSEE_ABI_VERSION: u32 = 4;

pub const FUSEE_OK: i32 = 0;
pub const FUSEE_FAILED: i32 = 1;
pub const FUSEE_BUSY: i32 = 2;
pub const FUSEE_NOT_FOUND: i32 = 3;
pub const FUSEE_ALREADY_EXISTS: i32 = 4;
pub const FUSEE_UNKNOWN_WORKFLOW: i32 = 5;
pub const FUSEE_DETERMINISM: i32 = 6;
pub const FUSEE_REJECTED: i32 = 7;
pub const FUSEE_IO: i32 = 8;
pub const FUSEE_CLOSED: i32 = 9;
pub const FUSEE_INVALID: i32 = 10;
pub const FUSEE_PANIC: i32 = 11;

pub const FUSEE_EVENT_STRIDE: usize = 6;
pub const FUSEE_EV_WAKE: u64 = 1;
pub const FUSEE_EV_ACK: u64 = 2;
pub const FUSEE_EV_REJECT: u64 = 3;
pub const FUSEE_EV_START: u64 = 4;
pub const FUSEE_EV_STUCK: u64 = 5;
pub const FUSEE_EV_ENDED: u64 = 6;
pub const FUSEE_EV_RESULT: u64 = 7;
pub const FUSEE_EV_REPLY: u64 = 8;
pub const FUSEE_EV_REFUSE: u64 = 9;

pub const FUSEE_DELIVERY_NONE: u32 = 0;
pub const FUSEE_DELIVERY_RESOLVE: u32 = 1;
pub const FUSEE_DELIVERY_HELD: u32 = 2;
pub const FUSEE_DELIVERY_CANCEL: u32 = 3;
pub const FUSEE_DELIVERY_UPDATE: u32 = 4;

pub const FUSEE_STEP_READ_ONLY: u32 = 1;
pub const FUSEE_STEP_INLINE: u32 = 2;
pub const FUSEE_DONE_OK: u32 = 1;
pub const FUSEE_DONE_KEEP: u32 = 2;

pub const FUSEE_OP_BEGIN_AWAIT: u32 = 1;
pub const FUSEE_OP_BEGIN_SLEEP: u32 = 2;
pub const FUSEE_OP_BEGIN_WAIT: u32 = 3;
pub const FUSEE_OP_BEGIN_CHILD: u32 = 4;
pub const FUSEE_OP_SEND: u32 = 5;
pub const FUSEE_OP_AWAITING: u32 = 6;
pub const FUSEE_OP_HEARTBEAT: u32 = 7;
pub const FUSEE_OP_PATCHED: u32 = 8;
pub const FUSEE_OP_SET_ATTRIBUTES: u32 = 9;
pub const FUSEE_OP_UPDATE_DONE: u32 = 10;
pub const FUSEE_OP_CONTINUE_AS_NEW: u32 = 11;
pub const FUSEE_OP_JOURNAL_SIZE: u32 = 12;
pub const FUSEE_OP_CANCEL_WORKFLOW: u32 = 13;
pub const FUSEE_OP_FORGET: u32 = 14;
pub const FUSEE_OP_LIMIT_TICKET: u32 = 15;
pub const FUSEE_OP_ACQUIRE: u32 = 16;
pub const FUSEE_OP_RELEASE: u32 = 17;
pub const FUSEE_OP_WAKE_AT: u32 = 18;
pub const FUSEE_OP_VALUE: u32 = 19;
pub const FUSEE_OP_KEY: u32 = 20;
pub const FUSEE_OP_TOKEN: u32 = 21;
pub const FUSEE_OP_PROGRESS: u32 = 22;
pub const FUSEE_OP_STREAM_OPEN: u32 = 23;
pub const FUSEE_OP_STREAM_APPEND: u32 = 24;
pub const FUSEE_OP_STREAM_CLOSE: u32 = 25;
pub const FUSEE_OP_STREAM_RECORDED: u32 = 26;
pub const FUSEE_OP_OBJECT_GET: u32 = 27;
pub const FUSEE_OP_OBJECT_SET: u32 = 28;
pub const FUSEE_OP_OBJECT_KEYS: u32 = 29;
pub const FUSEE_OP_OBJECT_STATE: u32 = 30;
pub const FUSEE_OP_BEGIN_CALL: u32 = 31;
pub const FUSEE_OP_STEP_PAUSED: u32 = 32;
pub const FUSEE_OP_OBJECT_EDIT: u32 = 33;

/// A stream operation's writer: `FUSEE_NO_SEQ` for workflow code, else the
/// journal position of the step whose function is running.
pub const FUSEE_NO_SEQ: u64 = 0xFFFF_FFFF;

/// Borrowed bytes: an input for the length of a call, or an output that
/// points into the engine.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct fusee_str {
    pub ptr: *const u8,
    pub len: usize,
}

const EMPTY: fusee_str = fusee_str {
    ptr: std::ptr::null(),
    len: 0,
};

impl fusee_str {
    fn of(b: &[u8]) -> Self {
        Self {
            ptr: b.as_ptr(),
            len: b.len(),
        }
    }
}

/// Bytes the caller owns; free with `fusee_buf_free`.
#[repr(C)]
pub struct fusee_buf {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

#[repr(C)]
pub struct fusee_key {
    pub id: *const c_char,
    /// 32 bytes.
    pub key: *const u8,
}

pub type fusee_notify_fn = Option<unsafe extern "C" fn(user: *mut c_void)>;

#[repr(C)]
pub struct fusee_options {
    /// `sizeof(fusee_options)`.
    pub size: u32,
    /// "local" (default), "memory", "server" or "replicated".
    pub durability: *const c_char,
    pub dir: *const c_char,
    pub address: *const c_char,
    /// Replicated mode: this member's index in `peers`; -1 otherwise.
    pub replica_id: i32,
    pub n_peers: u32,
    pub peers: *const *const c_char,
    /// 0: the default (1 for a new local or memory engine).
    pub shards: u32,
    pub keep_history: u32,
    pub archive_history: u32,
    /// -1: the default; 0 off; 1 on.
    pub inline_flush: i32,
    /// Milliseconds; -1 for the default.
    pub park_after_ms: i64,
    pub retention_ms: i64,
    pub checkpoint_interval_ms: i64,
    /// -1 for the default.
    pub segment_bytes: i64,
    /// "fsync", "fdatasync", "fullfsync", "dsync", or NULL.
    pub flush: *const c_char,
    pub n_codec_keys: u32,
    pub codec_keys: *const fusee_key,
    pub notify: fusee_notify_fn,
    pub notify_user: *mut c_void,
    /// A descriptor to write one byte to instead of (or as well as) the
    /// callback; -1 for none. Duplicated: the caller keeps its own.
    pub notify_fd: i32,
}

/// State the engine shares with the host, read between calls without one.
#[repr(C)]
pub struct fusee_io {
    /// A wake was raised on the calling thread during a call (and no
    /// notify was sent for it): tick soon. The host may clear it.
    pub owed: u32,
    pub shards: u32,
    /// Each shard's commit position: a step whose gate is at or below it
    /// (and whose not-before time passed) may run. Updated by ticks and
    /// gate checks.
    pub commits: *const u64,
}

#[repr(C)]
pub struct fusee_step {
    pub seq: u32,
    /// 1: the step's result is in the journal; it does not run.
    pub replay: u32,
    pub gate: u64,
    pub attempt: u32,
    /// 1: the gate is open, run the attempt now.
    pub open: u32,
    pub not_before_ms: u64,
}

#[repr(C)]
pub struct fusee_delivery {
    pub kind: u32,
    pub seq: u32,
    pub status: u32,
    pub n_seqs: u32,
    /// Resolve: the result; cancel: the reason; update: the argument.
    pub payload: fusee_str,
    /// Cancel: the operations cancelled together.
    pub seqs: *const u32,
    pub ordinal: u64,
    /// Update: -1 for a recorded update (start its handler), otherwise the
    /// request to validate (`fusee_engine_decide_update`).
    pub token: i64,
    pub name: fusee_str,
    pub id: fusee_str,
}

#[repr(C)]
pub struct fusee_tick {
    /// `FUSEE_EVENT_STRIDE` numbers per event: kind, then four numbers,
    /// then the index of its first extra.
    pub events: *const u64,
    pub n_events: usize,
    pub extras: *const fusee_str,
    pub n_extras: usize,
    /// Unix ms of the next tick wanted; -1 for none (0: at once).
    pub next_wake_ms: i64,
}

#[repr(C)]
pub struct fusee_start {
    pub id: fusee_str,
    pub wtype: fusee_str,
    pub input: fusee_str,
    pub version: u32,
    /// ID and close policy bits.
    pub policy: u32,
    /// Absolute Unix ms; 0 for none.
    pub deadline_ms: u64,
    /// Acknowledgment token; -1 for none.
    pub token: i64,
    /// 1: deliver an update in the start record (update-with-start).
    pub has_update: u32,
    pub _reserved: u32,
    pub update_name: fusee_str,
    pub update_id: fusee_str,
    pub update_arg: fusee_str,
    pub update_token: i64,
}

#[repr(C)]
pub struct fusee_result {
    /// 0 not found, 1 running, 2 done.
    pub state: u32,
    pub shard: u32,
    pub run: u64,
    pub status: u32,
    pub _reserved: u32,
    pub payload: fusee_str,
}

#[derive(Debug)]
struct Fail {
    code: i32,
    msg: String,
}

type R<T> = Result<T, Fail>;

fn code_of(e: &fusee::Error) -> i32 {
    match e {
        fusee::Error::Busy => FUSEE_BUSY,
        fusee::Error::NotFound(_) => FUSEE_NOT_FOUND,
        fusee::Error::AlreadyExists(_) => FUSEE_ALREADY_EXISTS,
        fusee::Error::UnknownWorkflow(_) => FUSEE_UNKNOWN_WORKFLOW,
        fusee::Error::Determinism(_) => FUSEE_DETERMINISM,
        fusee::Error::Rejected(_) => FUSEE_REJECTED,
        fusee::Error::Io(_) => FUSEE_IO,
        fusee::Error::Closed => FUSEE_CLOSED,
        _ => FUSEE_FAILED,
    }
}

impl From<fusee::Error> for Fail {
    fn from(e: fusee::Error) -> Self {
        Fail {
            code: code_of(&e),
            msg: e.to_string(),
        }
    }
}

impl From<std::io::Error> for Fail {
    fn from(e: std::io::Error) -> Self {
        Fail {
            code: FUSEE_IO,
            msg: e.to_string(),
        }
    }
}

fn invalid(msg: impl Into<String>) -> Fail {
    Fail {
        code: FUSEE_INVALID,
        msg: msg.into(),
    }
}

fn panic_text(p: &(dyn std::any::Any + Send)) -> String {
    let what = p
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".into());
    format!("panic inside the engine: {what}")
}

/// # Safety
/// `s` points to `len` readable bytes for the call (or `len` is 0).
unsafe fn bytes<'a>(s: fusee_str) -> R<&'a [u8]> {
    if s.len == 0 {
        return Ok(&[]);
    }
    if s.ptr.is_null() {
        return Err(invalid("a null pointer with a length"));
    }
    // SAFETY: the caller passes `len` readable bytes.
    Ok(unsafe { std::slice::from_raw_parts(s.ptr, s.len) })
}

/// # Safety
/// As [`bytes`].
unsafe fn text<'a>(s: fusee_str) -> R<&'a str> {
    std::str::from_utf8(unsafe { bytes(s)? }).map_err(|_| invalid("text is not UTF-8"))
}

/// # Safety
/// As [`bytes`].
unsafe fn payload(s: fusee_str) -> R<Payload> {
    Ok(Arc::from(unsafe { bytes(s)? }))
}

/// # Safety
/// `p` is NULL or a NUL-terminated string.
unsafe fn cstr<'a>(p: *const c_char) -> R<Option<&'a str>> {
    if p.is_null() {
        return Ok(None);
    }
    // SAFETY: the caller passes a NUL-terminated string.
    let s = unsafe { CStr::from_ptr(p) };
    s.to_str()
        .map(Some)
        .map_err(|_| invalid("option text is not UTF-8"))
}

/// # Safety
/// `p` is NULL or points to `n` values.
unsafe fn slice<'a, T>(p: *const T, n: usize) -> R<&'a [T]> {
    if n == 0 {
        return Ok(&[]);
    }
    if p.is_null() {
        return Err(invalid("a null array with a length"));
    }
    // SAFETY: the caller passes `n` values.
    Ok(unsafe { std::slice::from_raw_parts(p, n) })
}

fn out<'a, T>(p: *mut T) -> R<&'a mut T> {
    // SAFETY: a non-null output pointer is writable for the call.
    unsafe { p.as_mut() }.ok_or_else(|| invalid("a null output pointer"))
}

fn put_buf(b: *mut fusee_buf, data: Vec<u8>) {
    // SAFETY: a non-null buffer pointer is writable.
    if let Some(b) = unsafe { b.as_mut() } {
        let mut data = std::mem::ManuallyDrop::new(data);
        *b = fusee_buf {
            ptr: data.as_mut_ptr(),
            len: data.len(),
            cap: data.capacity(),
        };
    }
}

fn clear_buf(b: *mut fusee_buf) {
    // SAFETY: a non-null buffer pointer is writable.
    if let Some(b) = unsafe { b.as_mut() } {
        *b = fusee_buf {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        };
    }
}

/// Runs a call without an engine handle; its error goes to `error`.
fn guarded(error: *mut fusee_buf, f: impl FnOnce() -> R<()>) -> i32 {
    clear_buf(error);
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => FUSEE_OK,
        Ok(Err(fail)) => {
            put_buf(error, fail.msg.into_bytes());
            fail.code
        }
        Err(p) => {
            put_buf(error, panic_text(&*p).into_bytes());
            FUSEE_PANIC
        }
    }
}

thread_local! {
    static TOKEN: u8 = const { 0 };
}

/// A number naming the calling thread while it lives.
fn thread_token() -> usize {
    TOKEN.with(|t| t as *const u8 as usize)
}

/// The notify path: coalesced until the next tick, and suppressed (made
/// `owed`) for a wake raised on the thread inside a call.
struct Wake {
    callback: fusee_notify_fn,
    user: usize,
    fd: Option<std::fs::File>,
    pending: AtomicBool,
    owed: AtomicBool,
    /// The token of the thread inside a call on this engine, or 0.
    caller: AtomicUsize,
}

impl Wake {
    fn raise(&self) {
        if self.caller.load(Ordering::Acquire) == thread_token() {
            self.owed.store(true, Ordering::Release);
            return;
        }
        self.send();
    }

    fn send(&self) {
        if self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(f) = self.callback {
            // SAFETY: the host gave this callback and its argument for
            // calls from any thread while the engine is open.
            unsafe { f(self.user as *mut c_void) };
        }
        if let Some(mut fd) = self.fd.as_ref() {
            // A full pipe already holds a byte.
            let _ = fd.write(&[1]);
        }
    }
}

pub struct fusee_engine {
    engine: Option<ForeignEngine>,
    connection: Option<ForeignConnection>,
    /// The HTTP API's calls (`fusee_engine_serve`), answered through
    /// `fusee_engine_next_call` like a server's.
    relay: Option<fusee::http::Relay>,
    wake: Arc<Wake>,
    io: Box<fusee_io>,
    commits: Box<[u64]>,
    /// Step results the host recorded with `FUSEE_DONE_KEEP` and still
    /// holds: their delivery carries no payload.
    held: HashSet<(u32, RunId, u32)>,
    error: String,
    poisoned: bool,
    // Outputs, valid until the next call.
    payload: Option<Payload>,
    text: Vec<u8>,
    seqs: Vec<u32>,
    // Tick outputs, valid until the next tick.
    events: Vec<u64>,
    spans: Vec<(usize, usize)>,
    extras: Vec<fusee_str>,
    arena: Vec<u8>,
}

fn live<'a>(
    engine: &'a Option<ForeignEngine>,
    connection: &Option<ForeignConnection>,
) -> R<&'a ForeignEngine> {
    if let Some(c) = connection {
        c.check()?;
    }
    engine.as_ref().ok_or_else(|| fusee::Error::Closed.into())
}

fn note_commit(commits: &mut [u64], e: &ForeignEngine, shard: u32) {
    if let (Some(slot), Some(c)) = (commits.get_mut(shard as usize), e.commit(shard)) {
        *slot = c;
    }
}

fn drop_held(held: &mut HashSet<(u32, RunId, u32)>, shard: u32, run: RunId) {
    if !held.is_empty() {
        held.retain(|&(s, r, _)| s != shard || r != run);
    }
}

fn status_from(v: u32) -> Status {
    match v {
        0 => Status::Ok,
        2 => Status::Cancelled,
        3 => Status::TimedOut,
        4 => Status::Terminated,
        _ => Status::Failed,
    }
}

/// Runs a call on an engine: marks the calling thread (wakes it raises
/// become `owed`), keeps panics inside, records the error message.
fn enter(e: *mut fusee_engine, f: impl FnOnce(&mut fusee_engine) -> R<()>) -> i32 {
    // SAFETY: the host passes a live engine handle and no other call on it
    // runs meanwhile (see the module comment).
    let Some(e) = (unsafe { e.as_mut() }) else {
        return FUSEE_INVALID;
    };
    if e.poisoned {
        e.error = "the engine stopped after a panic; close it".into();
        return FUSEE_PANIC;
    }
    e.wake.caller.store(thread_token(), Ordering::Release);
    let r = catch_unwind(AssertUnwindSafe(|| f(&mut *e)));
    e.wake.caller.store(0, Ordering::Release);
    if e.wake.owed.swap(false, Ordering::AcqRel) {
        e.io.owed = 1;
    }
    match r {
        Ok(Ok(())) => FUSEE_OK,
        Ok(Err(fail)) => {
            e.error = fail.msg;
            fail.code
        }
        Err(p) => {
            e.poisoned = true;
            e.error = panic_text(&*p);
            FUSEE_PANIC
        }
    }
}

fn codec(keys: &[fusee_key]) -> R<Option<Arc<dyn fusee::Codec>>> {
    if keys.is_empty() {
        return Ok(None);
    }
    let mut list = Vec::new();
    for k in keys {
        // SAFETY: the options carry NUL-terminated IDs.
        let id = unsafe { cstr(k.id)? }.ok_or_else(|| invalid("a codec key needs an ID"))?;
        if k.key.is_null() {
            return Err(invalid("a codec key is 32 bytes"));
        }
        let mut key = [0u8; 32];
        // SAFETY: each key points to 32 bytes.
        key.copy_from_slice(unsafe { std::slice::from_raw_parts(k.key, 32) });
        list.push((id.to_owned(), key));
    }
    Ok(Some(fusee::aes_gcm(list)))
}

fn ms(v: i64) -> Option<Duration> {
    (v >= 0).then(|| Duration::from_millis(v as u64))
}

/// # Safety
/// `o` is a valid options struct.
unsafe fn open(o: &fusee_options) -> R<fusee_engine> {
    if o.size as usize != std::mem::size_of::<fusee_options>() {
        return Err(invalid(format!(
            "fusee_options is {} bytes here but {} in the caller: rebuild against this fusee.h",
            std::mem::size_of::<fusee_options>(),
            o.size
        )));
    }
    // SAFETY (all `cstr` and `slice` calls): the options' pointers are
    // NULL or valid for the call, as the header requires.
    let durability = unsafe { cstr(o.durability)? }.unwrap_or("local");
    let dir = unsafe { cstr(o.dir)? };
    let address = unsafe { cstr(o.address)? };
    let flush = unsafe { cstr(o.flush)? };
    let mut peers = Vec::new();
    for &p in unsafe { slice(o.peers, o.n_peers as usize)? } {
        peers.push(
            unsafe { cstr(p)? }
                .ok_or_else(|| invalid("a null peer address"))?
                .to_owned(),
        );
    }
    let keys = unsafe { slice(o.codec_keys, o.n_codec_keys as usize)? };
    let connection = match durability {
        "server" | "replicated" => Some(ForeignConnection::prepare_with(
            durability,
            address.map(str::to_owned),
            (o.replica_id >= 0)
                .then(|| u8::try_from(o.replica_id))
                .transpose()
                .map_err(|_| invalid("replica_id is a small number"))?,
            (!peers.is_empty()).then_some(peers),
            dir.map(str::to_owned),
            flush.map(str::to_owned),
            None,
        )?),
        _ => None,
    };
    let mut cfg = match durability {
        "memory" => Config::memory(),
        "local" => Config::local(dir.unwrap_or("./fusee-data")),
        "server" | "replicated" => connection
            .as_ref()
            .map(|c| c.config.clone())
            .ok_or_else(|| invalid("missing connection"))?,
        other => return Err(invalid(format!("unsupported durability '{other}'"))),
    };
    if connection.is_some() {
        if o.shards > 0 && Some(o.shards) != cfg.shards {
            return Err(invalid("shard count must match the remote configuration"));
        }
    } else {
        cfg.shards = Some(if o.shards > 0 { o.shards } else { 1 });
    }
    if let Some(d) = ms(o.park_after_ms) {
        cfg.park_after = d;
    }
    if let Some(d) = ms(o.retention_ms) {
        cfg.retention = d;
    }
    if let Some(d) = ms(o.checkpoint_interval_ms) {
        cfg.checkpoint_interval = d;
    }
    if o.segment_bytes >= 0 {
        cfg.local.segment_bytes = o.segment_bytes as u64;
    }
    cfg.keep_history = o.keep_history != 0;
    cfg.archive_history = o.archive_history != 0;
    cfg.codec = codec(keys)?;
    if o.inline_flush >= 0 {
        cfg.inline_flush = o.inline_flush != 0;
    }
    match flush {
        Some("fsync") => cfg.local.flush = FlushMethod::Fsync,
        Some("fullfsync") => cfg.local.flush = FlushMethod::FullFsync,
        Some("fdatasync") => cfg.local.flush = FlushMethod::Fdatasync,
        Some("dsync") => cfg.local.flush = FlushMethod::Dsync,
        None => {}
        Some(other) => return Err(invalid(format!("unknown flush method '{other}'"))),
    }
    let fd = if o.notify_fd >= 0 {
        use std::os::fd::FromRawFd;
        // SAFETY: fcntl duplicates the caller's descriptor; the copy is ours.
        let fd = unsafe { libc_dup(o.notify_fd) }?;
        // SAFETY: a fresh descriptor this engine owns.
        Some(unsafe { std::fs::File::from_raw_fd(fd) })
    } else {
        None
    };
    let wake = Arc::new(Wake {
        callback: o.notify,
        user: o.notify_user as usize,
        fd,
        pending: AtomicBool::new(false),
        owed: AtomicBool::new(false),
        caller: AtomicUsize::new(0),
    });
    let notify: fusee::env::Notify = {
        let wake = wake.clone();
        Arc::new(move || wake.raise())
    };
    if let Some(c) = &connection {
        c.set_notify(notify.clone());
    }
    let engine = ForeignEngine::open(&cfg, notify)?;
    let shards = engine.shards();
    let commits: Box<[u64]> = vec![0; shards as usize].into_boxed_slice();
    let io = Box::new(fusee_io {
        owed: 0,
        shards,
        commits: commits.as_ptr(),
    });
    Ok(fusee_engine {
        engine: Some(engine),
        connection,
        relay: None,
        wake,
        io,
        commits,
        held: HashSet::new(),
        error: String::new(),
        poisoned: false,
        payload: None,
        text: Vec::new(),
        seqs: Vec::new(),
        events: Vec::new(),
        spans: Vec::new(),
        extras: Vec::new(),
        arena: Vec::new(),
    })
}

/// # Safety
/// `fd` is a descriptor the caller holds.
unsafe fn libc_dup(fd: i32) -> R<i32> {
    // SAFETY: duplicating a descriptor does not take the caller's.
    let d = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if d < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(d)
}

// ------------------------------------------------------------------ lifecycle

#[unsafe(no_mangle)]
pub extern "C" fn fusee_abi_version() -> u32 {
    FUSEE_ABI_VERSION
}

/// The library's version, NUL-terminated.
#[unsafe(no_mangle)]
pub extern "C" fn fusee_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_buf_free(b: *mut fusee_buf) {
    // SAFETY: a buffer this library filled (or a zeroed one).
    let Some(b) = (unsafe { b.as_mut() }) else {
        return;
    };
    if !b.ptr.is_null() {
        // SAFETY: the parts of a Vec this library leaked in `put_buf`.
        drop(unsafe { Vec::from_raw_parts(b.ptr, b.len, b.cap) });
    }
    b.ptr = std::ptr::null_mut();
    b.len = 0;
    b.cap = 0;
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_open(
    opts: *const fusee_options,
    engine: *mut *mut fusee_engine,
    error: *mut fusee_buf,
) -> i32 {
    guarded(error, || {
        // SAFETY: a valid options struct, as the header requires.
        let o = unsafe { opts.as_ref() }.ok_or_else(|| invalid("no options"))?;
        let slot = out(engine)?;
        *slot = std::ptr::null_mut();
        // SAFETY: as above.
        let e = unsafe { open(o)? };
        *slot = Box::into_raw(Box::new(e));
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_close(e: *mut fusee_engine, error: *mut fusee_buf) -> i32 {
    if e.is_null() {
        clear_buf(error);
        return FUSEE_OK;
    }
    // SAFETY: the host hands back a handle from `fusee_engine_open`, once,
    // with no other call running.
    let mut boxed = unsafe { Box::from_raw(e) };
    guarded(error, move || {
        let poisoned = boxed.poisoned;
        let result = match boxed.engine.take() {
            Some(engine) if !poisoned => engine.close(),
            _ => Ok(()),
        };
        boxed.connection.take();
        if let Some(r) = boxed.relay.take() {
            r.close();
        }
        drop(boxed);
        result.map_err(Fail::from)
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_error(e: *const fusee_engine) -> fusee_str {
    // SAFETY: a live handle, between calls.
    match unsafe { e.as_ref() } {
        Some(e) => fusee_str::of(e.error.as_bytes()),
        None => EMPTY,
    }
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_io(e: *mut fusee_engine) -> *mut fusee_io {
    // SAFETY: a live handle, between calls.
    match unsafe { e.as_mut() } {
        Some(e) => &mut *e.io,
        None => std::ptr::null_mut(),
    }
}

// --------------------------------------------------------------------- tick

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_tick(e: *mut fusee_engine, tick: *mut fusee_tick) -> i32 {
    enter(e, |e| {
        let tick = out(tick)?;
        let engine = live(&e.engine, &e.connection)?;
        // Clear before polling: a completion racing this tick sends again.
        e.wake.pending.store(false, Ordering::Release);
        let (events, next) = engine.tick();
        engine.check_health()?;
        // A wake raised during the tick is covered by it, unless it left a
        // shard with work.
        if e.wake.owed.swap(false, Ordering::AcqRel) && engine.has_dirty() {
            e.io.owed = 1;
        }
        for s in 0..engine.shards() {
            note_commit(&mut e.commits, engine, s);
        }
        e.events.clear();
        e.spans.clear();
        e.arena.clear();
        let (arena, spans) = (&mut e.arena, &mut e.spans);
        let mut extra = |b: &[u8]| -> u64 {
            spans.push((arena.len(), b.len()));
            arena.extend_from_slice(b);
            (spans.len() - 1) as u64
        };
        for ev in events {
            let row: [u64; FUSEE_EVENT_STRIDE] = match ev {
                EngineEvent::Wake { shard, run } => [FUSEE_EV_WAKE, shard.into(), run, 0, 0, 0],
                EngineEvent::Ack { token } => [FUSEE_EV_ACK, token, 0, 0, 0, 0],
                EngineEvent::Reject { token } => [FUSEE_EV_REJECT, token, 0, 0, 0, 0],
                EngineEvent::Start { shard, run, info } => {
                    let x = extra(info.id.as_bytes());
                    extra(info.wtype.as_bytes());
                    extra(&info.input);
                    [
                        FUSEE_EV_START,
                        shard.into(),
                        run,
                        info.version.into(),
                        info.started_ms,
                        x,
                    ]
                }
                EngineEvent::Stuck {
                    shard,
                    run,
                    message,
                } => {
                    drop_held(&mut e.held, shard, run);
                    let x = extra(message.as_bytes());
                    [FUSEE_EV_STUCK, shard.into(), run, 0, 0, x]
                }
                EngineEvent::Ended { shard, run } => {
                    drop_held(&mut e.held, shard, run);
                    [FUSEE_EV_ENDED, shard.into(), run, 0, 0, 0]
                }
                EngineEvent::Result {
                    shard,
                    run,
                    status,
                    payload,
                } => {
                    let x = extra(&payload);
                    [FUSEE_EV_RESULT, shard.into(), run, status as u64, 0, x]
                }
                EngineEvent::Reply {
                    token,
                    status,
                    payload,
                } => {
                    let x = extra(&payload);
                    [FUSEE_EV_REPLY, token, status as u64, 0, 0, x]
                }
                EngineEvent::Refuse { token, message } => {
                    let x = extra(message.as_bytes());
                    [FUSEE_EV_REFUSE, token, 0, 0, 0, x]
                }
            };
            e.events.extend_from_slice(&row);
        }
        let base = e.arena.as_ptr();
        e.extras.clear();
        e.extras.extend(e.spans.iter().map(|&(at, len)| fusee_str {
            // SAFETY: each span lies inside the arena, which no longer grows.
            ptr: if len == 0 {
                std::ptr::null()
            } else {
                unsafe { base.add(at) }
            },
            len,
        }));
        *tick = fusee_tick {
            events: e.events.as_ptr(),
            n_events: e.events.len() / FUSEE_EVENT_STRIDE,
            extras: e.extras.as_ptr(),
            n_extras: e.extras.len(),
            next_wake_ms: next.map(|n| n as i64).unwrap_or(-1),
        };
        Ok(())
    })
}

// ----------------------------------------------------------------- requests

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_start(
    e: *mut fusee_engine,
    req: fusee_start,
    existed: *mut u32,
) -> i32 {
    enter(e, |e| {
        let q = &req;
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY (text/payload): the request's strings are valid for the call.
        let mut r = StartRequest::new(
            unsafe { text(q.id)? },
            unsafe { text(q.wtype)? },
            unsafe { payload(q.input)? },
            Default::default(),
        );
        r.version = Some(q.version.max(1));
        r.deadline_ms = q.deadline_ms;
        r.policy = q.policy as u8;
        r.token = (q.token >= 0).then_some(q.token as u64);
        if q.has_update != 0 {
            r.update = Some(UpdateRequest {
                name: unsafe { text(q.update_name)? }.to_owned(),
                id: unsafe { text(q.update_id)? }.to_owned(),
                payload: if q.update_arg.len == 0 {
                    Arc::from(&b"null"[..])
                } else {
                    unsafe { payload(q.update_arg)? }
                },
                token: (q.update_token >= 0).then_some(q.update_token as u64),
            });
        }
        let found = match engine.start(r)? {
            (_, StartOutcome::Created { .. }) => 0,
            (_, StartOutcome::Existing { .. }) => 1,
        };
        // SAFETY: NULL or writable.
        if let Some(x) = unsafe { existed.as_mut() } {
            *x = found;
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_signal(
    e: *mut fusee_engine,
    id: fusee_str,
    name: fusee_str,
    value: fusee_str,
    token: u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        unsafe {
            engine.signal(text(id)?, text(name)?, payload(value)?, 0, Some(token))?;
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_cancel(
    e: *mut fusee_engine,
    id: fusee_str,
    reason: fusee_str,
    terminate: u32,
    token: u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let (id, reason) = unsafe { (text(id)?, text(reason)?) };
        if terminate != 0 {
            engine.terminate(id, reason, Some(token))?;
        } else {
            engine.cancel(id, reason, Some(token))?;
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_update(
    e: *mut fusee_engine,
    id: fusee_str,
    name: fusee_str,
    update_id: fusee_str,
    arg: fusee_str,
    token: u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        unsafe {
            engine.update(
                text(id)?,
                text(name)?,
                text(update_id)?,
                payload(arg)?,
                Some(token),
            )?;
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_complete(
    e: *mut fusee_engine,
    completion: fusee_str,
    ok: u32,
    value: fusee_str,
    token: u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        let status = if ok != 0 { Status::Ok } else { Status::Failed };
        // SAFETY: inputs valid for the call.
        unsafe { engine.complete(text(completion)?, status, payload(value)?, Some(token))? };
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_reset(
    e: *mut fusee_engine,
    id: fusee_str,
    before_step: fusee_str,
    reapply_signals: u32,
    version: u32,
    token: u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        unsafe {
            engine.reset(
                text(id)?,
                text(before_step)?,
                reapply_signals != 0,
                (version > 0).then_some(version),
                Some(token),
            )?;
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_ack_at(
    e: *mut fusee_engine,
    shard: u32,
    lsn: u64,
    token: u64,
) -> i32 {
    enter(e, |e| {
        live(&e.engine, &e.connection)?.ack_at(shard, lsn, token)?;
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_query_native(
    e: *mut fusee_engine,
    id: fusee_str,
    name: fusee_str,
    arg: fusee_str,
    token: u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        unsafe { engine.query_native(text(id)?, text(name)?, payload(arg)?, token)? };
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_stream_read(
    e: *mut fusee_engine,
    id: fusee_str,
    run: i64,
    name: fusee_str,
    from: u64,
    wait_ms: u64,
    token: u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        let run = u64::try_from(run).ok();
        // SAFETY: inputs valid for the call.
        unsafe { engine.stream_read(text(id)?, run, text(name)?, from, wait_ms, token)? };
        Ok(())
    })
}

// -------------------------------------------------------------------- reads

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_result(
    e: *mut fusee_engine,
    id: fusee_str,
    result: *mut fusee_result,
) -> i32 {
    enter(e, |e| {
        let r = out(result)?;
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let (shard, state) = engine.result(unsafe { text(id)? })?;
        *r = fusee_result {
            state: 0,
            shard,
            run: 0,
            status: 0,
            _reserved: 0,
            payload: EMPTY,
        };
        match state {
            ResultState::NotFound => {}
            ResultState::Running(run) => {
                r.state = 1;
                r.run = run;
            }
            ResultState::Done {
                run,
                status,
                payload,
            } => {
                r.state = 2;
                r.run = run;
                r.status = status as u32;
                r.payload = fusee_str::of(&payload);
                e.payload = Some(payload);
            }
        }
        Ok(())
    })
}

fn summary_json(s: &fusee::RunSummary) -> serde_json::Value {
    let attrs: serde_json::Value =
        serde_json::from_str(&s.attributes).unwrap_or(serde_json::Value::Null);
    serde_json::json!({"id": s.id, "type": s.wtype, "version": s.version, "status": s.status,
        "startedMs": s.started_ms, "finishedMs": s.finished_ms, "error": s.error,
        "shard": s.shard, "run": s.run, "attributes": attrs, "object": s.object})
}

fn set_text(slot: &mut Vec<u8>, b: &[u8], to: *mut fusee_str) -> R<()> {
    slot.clear();
    slot.extend_from_slice(b);
    *out(to)? = fusee_str::of(slot);
    Ok(())
}

fn set_found(found: *mut u32, v: bool) {
    // SAFETY: NULL or writable.
    if let Some(f) = unsafe { found.as_mut() } {
        *f = u32::from(v);
    }
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_inspect(
    e: *mut fusee_engine,
    id: fusee_str,
    json: *mut fusee_str,
    found: *mut u32,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let d = engine.inspect(unsafe { text(id)? })?;
        set_found(found, d.is_some());
        let Some(d) = d else {
            return set_text(&mut e.text, b"", json);
        };
        let mut v = summary_json(&d.summary);
        v["journal"] = fusee::journal::normalize(&d.journal);
        // A key's state and pending calls, as the other SDKs show them.
        match (&d.object, v.as_object_mut()) {
            (Some(o), Some(m)) => {
                m.insert(
                    "object".into(),
                    serde_json::json!({"state": o.state, "version": o.version,
                        "running": o.running, "waiting": o.waiting}),
                );
            }
            (None, Some(m)) => {
                m.remove("object");
            }
            _ => {}
        }
        set_text(&mut e.text, v.to_string().as_bytes(), json)
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_list(
    e: *mut fusee_engine,
    filter: fusee_str,
    limit: u32,
    json: *mut fusee_str,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let f = unsafe { bytes(filter)? };
        let runs = if f.is_empty() {
            engine.list(limit as usize)
        } else {
            let v: serde_json::Value =
                serde_json::from_slice(f).map_err(|e| invalid(format!("filter: {e}")))?;
            engine.list_where(&ListFilter::parse(&v)?, limit as usize)
        };
        let v = serde_json::Value::Array(runs.iter().map(summary_json).collect());
        set_text(&mut e.text, v.to_string().as_bytes(), json)
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_export_journal(
    e: *mut fusee_engine,
    id: fusee_str,
    file: *mut fusee_str,
    found: *mut u32,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let d = engine.inspect(unsafe { text(id)? })?;
        set_found(found, d.is_some());
        let b = d
            .map(|d| fusee::export::ExportedJournal::from_detail(&d).encode())
            .unwrap_or_default();
        set_text(&mut e.text, &b, file)
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_shard_metrics(
    e: *mut fusee_engine,
    json: *mut fusee_str,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        let s = serde_json::to_string(&engine.shard_metrics()).map_err(fusee::Error::from)?;
        set_text(&mut e.text, s.as_bytes(), json)
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_query_point(
    e: *mut fusee_engine,
    id: fusee_str,
    shard: *mut u32,
    run: *mut u64,
    lsn: *mut u64,
    foreign: *mut u32,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let (s, r, l, f) = engine.query_route(unsafe { text(id)? })?;
        *out(shard)? = s;
        *out(run)? = r;
        *out(lsn)? = l;
        *out(foreign)? = u32::from(f);
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_code_state(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    state: *mut u32,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        *out(state)? = match engine.code_state(shard, run)? {
            None => 0,
            Some((replaying, live_work)) => {
                1 | (u32::from(replaying) << 1) | (u32::from(live_work) << 2)
            }
        };
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_limit(
    e: *mut fusee_engine,
    name: fusee_str,
    concurrent: u32,
    per_second: f64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        engine.limit(
            unsafe { text(name)? },
            LimitSpec {
                concurrent,
                per_second,
            },
        )?;
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_limit_usage(
    e: *mut fusee_engine,
    name: fusee_str,
    held: *mut u32,
    waiting: *mut u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let (h, w) = engine.limit_usage(unsafe { text(name)? });
        *out(held)? = h;
        *out(waiting)? = w as u64;
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_register_object(
    e: *mut fusee_engine,
    name: fusee_str,
    shared: *const fusee_str,
    n_shared: u32,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let names = unsafe { slice(shared, n_shared as usize)? }
            .iter()
            .map(|&s| unsafe { text(s) }.map(str::to_owned))
            .collect::<R<Vec<String>>>()?;
        // SAFETY: as above.
        engine.register_object(unsafe { text(name)? }, &names)?;
        Ok(())
    })
}

// ------------------------------------------------------------ workflow code

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_begin_step(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    name: fusee_str,
    flags: u32,
    step: *mut fusee_step,
) -> i32 {
    enter(e, |e| {
        let step = out(step)?;
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let name = unsafe { text(name)? };
        let (seq, start, gate) = engine.with_host(shard, |h| h.begin_step(run, name))??;
        *step = match start {
            StepStart::Replay => fusee_step {
                seq,
                replay: 1,
                gate,
                attempt: 0,
                open: 0,
                not_before_ms: 0,
            },
            StepStart::Run {
                attempt,
                not_before_ms,
            } => {
                // A read-only step waits for no earlier record.
                let g = if flags & FUSEE_STEP_READ_ONLY != 0 {
                    0
                } else {
                    gate
                };
                let open = engine.may_run_step(
                    shard,
                    run,
                    g,
                    not_before_ms,
                    flags & FUSEE_STEP_INLINE != 0,
                )?;
                note_commit(&mut e.commits, engine, shard);
                fusee_step {
                    seq,
                    replay: 0,
                    gate,
                    attempt,
                    open: u32::from(open),
                    not_before_ms,
                }
            }
        };
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_step_gate(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    gate: u64,
    not_before_ms: u64,
    inline: u32,
    open: *mut u32,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        let o = engine.may_run_step(shard, run, gate, not_before_ms, inline != 0)?;
        note_commit(&mut e.commits, engine, shard);
        *out(open)? = u32::from(o);
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_step_done(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    seq: u32,
    flags: u32,
    value: fusee_str,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        let ok = flags & FUSEE_DONE_OK != 0;
        let status = if ok { Status::Ok } else { Status::Failed };
        // SAFETY: inputs valid for the call.
        let p = unsafe { payload(value)? };
        engine.with_host(shard, |h| h.step_done(run, seq, status, p))?;
        if ok && flags & FUSEE_DONE_KEEP != 0 {
            e.held.insert((shard, run, seq));
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_resume(
    e: *mut fusee_engine,
    id: fusee_str,
    token: u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let id = unsafe { text(id)? };
        engine.resume(id, Some(token))?;
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_step_retry(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    seq: u32,
    retry_at_ms: u64,
    error: fusee_str,
    gate: *mut u64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        // SAFETY: inputs valid for the call.
        let p = unsafe { payload(error)? };
        let g = engine.with_host(shard, |h| h.step_retry(run, seq, retry_at_ms, p))??;
        *out(gate)? = g;
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_host(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    op: u32,
    n0: u64,
    n1: u64,
    n2: u64,
    s0: fusee_str,
    s1: fusee_str,
    s2: fusee_str,
    result: *mut u64,
    data: *mut fusee_str,
) -> i32 {
    enter(e, |e| {
        let nums = [n0, n1, n2];
        let strs = [s0, s1, s2];
        let n = |i: usize| nums[i];
        // SAFETY: the strings are valid for the call.
        let s = |i: usize| -> R<&str> { unsafe { text(strs[i]) } };
        // SAFETY: as above.
        let b = |i: usize| -> R<Payload> { unsafe { payload(strs[i]) } };
        let engine = live(&e.engine, &e.connection)?;
        let mut o = [0u64; 5];
        let mut produced: Option<Vec<u8>> = None;
        engine.with_host(shard, |h| -> R<()> {
            match op {
                FUSEE_OP_BEGIN_AWAIT => {
                    let (seq, start, gate) = h.begin_await(run, s(0)?)?;
                    o = match start {
                        AwaitStart::Run {
                            attempt,
                            not_before_ms,
                        } => [seq.into(), 0, gate, attempt.into(), not_before_ms],
                        AwaitStart::Replay => [seq.into(), 1, gate, 0, 0],
                        AwaitStart::Waiting { attempt } => [seq.into(), 2, gate, attempt.into(), 0],
                    };
                }
                FUSEE_OP_BEGIN_SLEEP => o[0] = h.begin_sleep(run, s(0)?, n(0))?.into(),
                FUSEE_OP_BEGIN_WAIT => o[0] = h.begin_wait(run, s(0)?, n(0))?.into(),
                FUSEE_OP_BEGIN_CHILD => {
                    match h.begin_child(run, s(0)?, s(1)?, n(0) as u8, n(1), b(2)?)? {
                        Some(seq) => o[..2].copy_from_slice(&[seq.into(), 1]),
                        None => o[1] = 0,
                    }
                }
                FUSEE_OP_SEND => h.send(run, s(0)?, s(1)?, b(2)?)?,
                FUSEE_OP_AWAITING => h.awaiting(run, n(0) as u32)?,
                FUSEE_OP_HEARTBEAT => h.heartbeat(run, n(0) as u32, b(0)?)?,
                FUSEE_OP_PATCHED => o[0] = u64::from(h.patched(run, s(0)?, n(0) != 0)?),
                FUSEE_OP_SET_ATTRIBUTES => h.set_attributes(run, b(0)?)?,
                FUSEE_OP_UPDATE_DONE => {
                    let st = status_from(n(1) as u32);
                    let outcome = if st == Status::Ok {
                        Ok(b(0)?)
                    } else {
                        Err((st, b(0)?))
                    };
                    h.update_done(run, n(0), outcome)?;
                }
                FUSEE_OP_CONTINUE_AS_NEW => {
                    let v = n(0);
                    h.continue_as_new(run, b(0)?, (v > 0).then_some(v as u32))?;
                }
                FUSEE_OP_JOURNAL_SIZE => o[0] = h.journal_size(run).into(),
                FUSEE_OP_CANCEL_WORKFLOW => h.cancel_workflow(run, s(0)?, s(1)?)?,
                FUSEE_OP_FORGET => h.forget(run, n(0) as u32),
                FUSEE_OP_STEP_PAUSED => o[0] = u64::from(h.step_paused(run, n(0) as u32)),
                FUSEE_OP_LIMIT_TICKET => o[0] = h.limit_ticket(),
                FUSEE_OP_ACQUIRE => {
                    let level = n(1) as i64;
                    let priority = fusee::Priority {
                        level: level.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32,
                        fairness_key: s(1)?.to_owned(),
                        fairness_weight: n(2).min(u64::from(u32::MAX)) as u32,
                    };
                    match h.acquire_in(run, s(0)?, n(0), &priority, None) {
                        Acquire::Granted => {}
                        Acquire::Wait {
                            retry_at_ms: Some(t),
                        } => o[..2].copy_from_slice(&[1, t]),
                        Acquire::Wait { retry_at_ms: None } => o[0] = 2,
                    }
                }
                FUSEE_OP_RELEASE => h.release(s(0)?, n(0)),
                FUSEE_OP_WAKE_AT => h.wake_run_at(run, n(0)),
                FUSEE_OP_VALUE => {
                    let c = b(0)?;
                    produced = Some(h.value(run, n(0) as u8, &mut |_, _| c.clone())?.to_vec());
                }
                FUSEE_OP_KEY => produced = Some(h.key(run, n(0) as u32).into_bytes()),
                FUSEE_OP_TOKEN => {
                    produced = Some(h.completion_token(run, n(0) as u32)?.into_bytes())
                }
                FUSEE_OP_PROGRESS => match h.progress(run, n(0) as u32) {
                    Some(p) => {
                        o[0] = 1;
                        produced = Some(p.to_vec());
                    }
                    None => produced = Some(Vec::new()),
                },
                FUSEE_OP_STREAM_OPEN => {
                    let open = h.stream_open(run, s(0)?, writer(n(0)))?;
                    o[..2].copy_from_slice(&[open.recorded, u64::from(open.closed)]);
                }
                FUSEE_OP_STREAM_APPEND => h.stream_append(run, s(0)?, writer(n(0)), b(1)?)?,
                FUSEE_OP_STREAM_CLOSE => h.stream_close(run, s(0)?, writer(n(0)))?,
                FUSEE_OP_STREAM_RECORDED => {
                    let chunks = match writer(n(0)) {
                        Some(seq) => h.stream_recorded(run, s(0)?, seq, n(1))?,
                        None => Vec::new(),
                    };
                    o[0] = chunks.len() as u64;
                    produced = Some(framed(&chunks));
                }
                FUSEE_OP_OBJECT_GET => match h.object_get(run, s(0)?, n(0) != 0)? {
                    Some(v) => {
                        o[0] = 1;
                        produced = Some(v.to_vec());
                    }
                    None => produced = Some(Vec::new()),
                },
                FUSEE_OP_OBJECT_SET => {
                    let value = (n(0) != 0).then(|| b(1)).transpose()?;
                    h.object_set(run, s(0)?, value)?;
                }
                FUSEE_OP_OBJECT_KEYS => {
                    let keys = h.object_keys(run, n(0) != 0)?;
                    o[0] = keys.len() as u64;
                    produced = Some(serde_json::to_vec(&keys).map_err(fusee::Error::from)?);
                }
                FUSEE_OP_OBJECT_STATE => produced = Some(h.object_state(run)?.to_vec()),
                FUSEE_OP_OBJECT_EDIT => produced = Some(h.object_edit(run, &b(0)?)?.to_vec()),
                FUSEE_OP_BEGIN_CALL => {
                    // s0 is the key's workflow ID: a type's name has no '/'.
                    let (wtype, key) = s(0)?
                        .split_once('/')
                        .ok_or_else(|| invalid("an object call names <type>/<key>"))?;
                    let arg = b(2)?;
                    let arg = if arg.is_empty() {
                        Arc::from(&b"null"[..])
                    } else {
                        arg
                    };
                    match h.begin_call(run, wtype, key, s(1)?, arg, n(0) != 0)? {
                        Some(seq) => o[..2].copy_from_slice(&[seq.into(), 1]),
                        None => o[1] = 0,
                    }
                }
                other => return Err(invalid(format!("no host operation {other}"))),
            }
            Ok(())
        })??;
        // SAFETY: NULL or room for five numbers.
        if let Some(r) = unsafe { result.as_mut() } {
            // SAFETY: as above.
            unsafe { std::ptr::copy_nonoverlapping(o.as_ptr(), r, o.len()) };
        }
        if let Some(p) = produced {
            e.text = p;
            if !data.is_null() {
                *out(data)? = fusee_str::of(&e.text);
            }
        }
        Ok(())
    })
}

/// The writer of a stream operation from its number (see `FUSEE_NO_SEQ`).
fn writer(seq: u64) -> Option<u32> {
    (seq != FUSEE_NO_SEQ).then_some(seq as u32)
}

/// Payloads in one buffer: each a 4-byte little-endian length, then it.
fn framed(payloads: &[Payload]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payloads.iter().map(|p| p.len() + 4).sum());
    for p in payloads {
        out.extend_from_slice(&(p.len() as u32).to_le_bytes());
        out.extend_from_slice(p);
    }
    out
}

fn no_delivery() -> fusee_delivery {
    fusee_delivery {
        kind: FUSEE_DELIVERY_NONE,
        seq: 0,
        status: 0,
        n_seqs: 0,
        payload: EMPTY,
        seqs: std::ptr::null(),
        ordinal: 0,
        token: -1,
        name: EMPTY,
        id: EMPTY,
    }
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_quiescent(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    delivery: *mut fusee_delivery,
) -> i32 {
    enter(e, |e| {
        let d = out(delivery)?;
        *d = no_delivery();
        let engine = live(&e.engine, &e.connection)?;
        match engine.quiescent(shard, run)? {
            None => {}
            Some(Delivery::Resolve { seq, outcome }) => {
                d.seq = seq;
                d.status = outcome.status as u32;
                let kept = !e.held.is_empty() && e.held.remove(&(shard, run, seq));
                if kept && outcome.status == Status::Ok {
                    d.kind = FUSEE_DELIVERY_HELD;
                } else {
                    d.kind = FUSEE_DELIVERY_RESOLVE;
                    d.payload = fusee_str::of(&outcome.payload);
                    e.payload = Some(outcome.payload);
                }
            }
            Some(Delivery::CancelAll { seqs, reason }) => {
                d.kind = FUSEE_DELIVERY_CANCEL;
                d.status = Status::Cancelled as u32;
                e.seqs = seqs;
                d.seqs = e.seqs.as_ptr();
                d.n_seqs = e.seqs.len() as u32;
                d.payload = fusee_str::of(&reason);
                e.payload = Some(reason);
            }
            Some(Delivery::Update {
                ordinal,
                name,
                id,
                payload,
                token,
            }) => {
                d.kind = FUSEE_DELIVERY_UPDATE;
                d.ordinal = ordinal;
                d.token = token.map(|t| t as i64).unwrap_or(-1);
                e.text.clear();
                e.text.extend_from_slice(name.as_bytes());
                e.text.extend_from_slice(id.as_bytes());
                let (n, i) = e.text.split_at(name.len());
                d.name = fusee_str::of(n);
                d.id = fusee_str::of(i);
                d.payload = fusee_str::of(&payload);
                e.payload = Some(payload);
            }
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_decide_update(
    e: *mut fusee_engine,
    shard: u32,
    token: u64,
    ok: u32,
    message: fusee_str,
    ordinal: *mut i64,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        let decision = if ok != 0 {
            Ok(())
        } else {
            // SAFETY: inputs valid for the call.
            Err(unsafe { text(message)? }.to_owned())
        };
        *out(ordinal)? = engine
            .decide_update(shard, token, decision)?
            .map(|o| o as i64)
            .unwrap_or(-1);
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_finish(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    status: u32,
    value: fusee_str,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        let s = status_from(status);
        // SAFETY: inputs valid for the call.
        let p = unsafe { payload(value)? };
        let outcome = if s == Status::Ok { Ok(p) } else { Err((s, p)) };
        drop_held(&mut e.held, shard, run);
        engine.finish(shard, run, outcome)?;
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_stuck(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    message: fusee_str,
) -> i32 {
    enter(e, |e| {
        let engine = live(&e.engine, &e.connection)?;
        drop_held(&mut e.held, shard, run);
        // SAFETY: inputs valid for the call.
        engine.stuck(shard, run, unsafe { text(message)? }.to_owned())?;
        Ok(())
    })
}

// ------------------------------------------------------------- server calls

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_next_call(
    e: *mut fusee_engine,
    json: *mut fusee_str,
    found: *mut u32,
) -> i32 {
    enter(e, |e| {
        set_found(found, false);
        if let Some(c) = e.relay.as_ref().and_then(|r| r.next()) {
            let call =
                serde_json::json!({"relay": c.id.to_string(), "method": c.method, "args": c.args});
            set_found(found, true);
            set_text(&mut e.text, call.to_string().as_bytes(), json)?;
            return Ok(());
        }
        let Some(c) = e.connection.as_mut() else {
            return Ok(());
        };
        if let Some(call) = c.next_call_json()? {
            set_found(found, true);
            set_text(&mut e.text, call.as_bytes(), json)?;
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_serve(
    e: *mut fusee_engine,
    addr: fusee_str,
    token: fusee_str,
    bound: *mut fusee_str,
) -> i32 {
    enter(e, |e| {
        let relay = match &e.relay {
            Some(r) => r.clone(),
            None => {
                let wake = e.wake.clone();
                let r = fusee::http::Relay::new(Arc::new(move || wake.raise()));
                e.relay = Some(r.clone());
                r
            }
        };
        // SAFETY: inputs valid for the call.
        let (addr, token) = unsafe { (text(addr)?.to_owned(), text(token)?.to_owned()) };
        let opts = fusee::http::HttpOptions {
            token: (!token.is_empty()).then_some(token),
            ..Default::default()
        };
        let at = fusee::http::serve_thread(relay, &addr, opts)
            .map_err(|err| Fail::from(fusee::Error::Io(err.to_string())))?;
        set_text(&mut e.text, format!("http://{at}").as_bytes(), bound)?;
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_invocation_state(
    e: *mut fusee_engine,
    awaiting: *const fusee_str,
    n_awaiting: u32,
    state: *mut fusee_str,
) -> i32 {
    enter(e, |e| {
        // SAFETY: inputs valid for the call.
        let types = unsafe { slice(awaiting, n_awaiting as usize)? }
            .iter()
            .map(|&s| unsafe { text(s) }.map(str::to_owned))
            .collect::<R<Vec<String>>>()?;
        let now = e
            .connection
            .as_ref()
            .and_then(|c| c.invocation_state(&types))
            .unwrap_or("");
        set_text(&mut e.text, now.as_bytes(), state)?;
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_engine_reply_call(
    e: *mut fusee_engine,
    relay: u64,
    response: fusee_str,
) -> i32 {
    enter(e, |e| {
        // SAFETY: inputs valid for the call.
        let response = unsafe { bytes(response)? };
        if fusee::http::Relay::owns(relay)
            && let Some(r) = &e.relay
        {
            r.reply(
                relay,
                serde_json::from_slice(response).unwrap_or(serde_json::Value::Null),
            );
            return Ok(());
        }
        let c = e
            .connection
            .as_ref()
            .ok_or_else(|| Fail::from(fusee::Error::Closed))?;
        c.reply(relay, response.to_vec())?;
        Ok(())
    })
}

// ----------------------------------------------------------- free functions

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_queue_request(
    kind: u32,
    workflow: fusee_str,
    key: fusee_str,
    event_id: fusee_str,
    input: fusee_str,
    a: f64,
    b: f64,
    c: f64,
    result: *mut fusee_buf,
    error: *mut fusee_buf,
) -> i32 {
    guarded(error, || {
        use fusee::queue::{Batch, Debounce, Event, Rule};
        let opt = |v: f64| (!v.is_nan()).then_some(v);
        let ms = |v: f64, default: u64| opt(v).map(|v| v.max(0.0) as u64).unwrap_or(default);
        let rule = match kind {
            0 => {
                let d = Debounce::default();
                Rule::Debounce(Debounce {
                    window_ms: ms(a, d.window_ms),
                    max_wait_ms: ms(b, d.max_wait_ms),
                    dedup_ms: ms(c, d.dedup_ms),
                })
            }
            1 => {
                let d = Batch::default();
                Rule::Batch(Batch {
                    max_events: opt(a)
                        .map(|v| v.clamp(0.0, f64::from(u32::MAX)) as u32)
                        .unwrap_or(d.max_events),
                    max_wait_ms: ms(b, d.max_wait_ms),
                    dedup_ms: ms(c, d.dedup_ms),
                })
            }
            other => return Err(invalid(format!("no queue rule {other}"))),
        };
        rule.validate()?;
        // SAFETY: inputs valid for the call.
        let (workflow, key, event_id, input) =
            unsafe { (text(workflow)?, text(key)?, text(event_id)?, bytes(input)?) };
        let event = Event {
            id: event_id.to_owned(),
            rule,
            input: serde_json::from_slice(input).map_err(fusee::Error::from)?,
        };
        let (wtype, id, state, arg) = fusee::queue::request(workflow, key, &event);
        let parts = [
            wtype.as_bytes().to_vec(),
            id.into_bytes(),
            state.to_string().into_bytes(),
            arg.to_string().into_bytes(),
        ];
        if result.is_null() {
            return Err(invalid("a null output pointer"));
        }
        for (i, p) in parts.into_iter().enumerate() {
            // SAFETY: room for four buffers, as the header requires.
            put_buf(unsafe { result.add(i) }, p);
        }
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_validate_schedule(spec: fusee_str, problem: *mut fusee_buf) -> i32 {
    guarded(problem, || {
        // SAFETY: inputs valid for the call.
        let s: fusee::ScheduleSpec =
            serde_json::from_slice(unsafe { bytes(spec)? }).map_err(|e| invalid(e.to_string()))?;
        s.validate().map_err(|e| invalid(e.to_string()))
    })
}

// ------------------------------------------------------------------- client

/// A connection to a Fusee server. Thread-safe: calls may overlap.
pub struct fusee_client {
    client: fusee_server::Client,
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_client_connect(
    address: fusee_str,
    client: *mut *mut fusee_client,
    error: *mut fusee_buf,
) -> i32 {
    guarded(error, || {
        let slot = out(client)?;
        *slot = std::ptr::null_mut();
        // SAFETY: inputs valid for the call.
        let c = fusee_server::Client::connect(unsafe { text(address)? })?;
        *slot = Box::into_raw(Box::new(fusee_client { client: c }));
        Ok(())
    })
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_client_call(
    c: *const fusee_client,
    method: fusee_str,
    args: fusee_str,
    result: *mut fusee_buf,
) -> i32 {
    clear_buf(result);
    let r = catch_unwind(AssertUnwindSafe(|| -> R<Vec<u8>> {
        // SAFETY: a live client handle.
        let c = unsafe { c.as_ref() }.ok_or_else(|| invalid("no client"))?;
        // SAFETY: inputs valid for the call.
        let (method, args) = unsafe { (text(method)?, bytes(args)?) };
        let params: serde_json::Value =
            serde_json::from_slice(args).map_err(|e| invalid(format!("arguments: {e}")))?;
        Ok(c.client.call(method, &params)?.to_string().into_bytes())
    }));
    match r {
        Ok(Ok(v)) => {
            put_buf(result, v);
            FUSEE_OK
        }
        Ok(Err(f)) => {
            put_buf(result, f.msg.into_bytes());
            f.code
        }
        Err(p) => {
            put_buf(result, panic_text(&*p).into_bytes());
            FUSEE_PANIC
        }
    }
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_client_close(c: *const fusee_client) -> i32 {
    // SAFETY: a live client handle.
    match unsafe { c.as_ref() } {
        Some(c) => match c.client.close() {
            Ok(()) => FUSEE_OK,
            Err(_) => FUSEE_IO,
        },
        None => FUSEE_OK,
    }
}

/// # Safety
/// See `fusee.h`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fusee_client_free(c: *mut fusee_client) {
    if !c.is_null() {
        // SAFETY: a handle from `fusee_client_connect`, once, with no call
        // running on it.
        drop(unsafe { Box::from_raw(c) });
    }
}

#[cfg(test)]
mod tests;
