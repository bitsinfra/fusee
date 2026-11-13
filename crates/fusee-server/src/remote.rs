//! The worker side of server mode: [`RemoteEnv`] is an [`Env`] whose log,
//! run store and checkpoints live on the server. A shard runtime driven by
//! it behaves exactly as in local mode; a flush is durable once the server
//! flushed it, one pipelined round trip later.

use crate::limits::{LimitWire, RemoteLimits};
use crate::proto::{self, Head, req, resp};
use crate::spin::{self, Signal};
use fusee::env::{Env, Notify};
use fusee_core::Lsn;
use fusee_core::frame;
use fusee_core::segment::Batch;
use fusee_core::store::Checkpoint;
use fusee_core::wire::{Reader, put_bytes, put_u32, put_u64};
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

type Reply = Result<Vec<u8>, String>;

#[derive(Default)]
struct Slot {
    flushed: Option<io::Result<Lsn>>,
    ckpt: Option<io::Result<Lsn>>,
    notify: Option<Notify>,
    /// Verified run-store appends awaiting the server's offset, in the
    /// order sent: `(request, offset this worker referenced)`.
    appends: VecDeque<(u64, u64)>,
    /// The shard's first failure. Every later result reports it, so no
    /// acknowledgment that follows a failed append can count.
    failed: Option<String>,
    /// Shared with the shard's [`RemoteEnv`].
    timing: Arc<FlushTiming>,
    /// The shard thread is waiting for its flush reply inside
    /// [`RemoteEnv::flush_inline`]; the reader hands the reply over directly.
    inline: bool,
    /// Log format 3: the shard's durable LSN, from `DURABLE` frames.
    durable: Lsn,
    /// Log format 3: the LSN the flush in flight waits for.
    want: Option<Lsn>,
    /// Log format 3: one more checkpoint of this shard lets the server
    /// retire a segment of its log.
    pins: bool,
}

/// One shard's flush round trips, measured by whichever side saw the reply.
#[derive(Default)]
struct FlushTiming {
    /// When the background flush in flight was sent, in monotonic µs.
    submitted_us: AtomicU64,
    /// Round trip of the most recent flush in µs, so the runtime waits
    /// inline only for a fast server.
    last_us: AtomicU64,
    /// Raised when the reply an inline wait expects has arrived.
    arrived: Signal,
    /// Shared memory: wakes an inline wait parked on the shard's watermark.
    shm_wake: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>>,
}

impl FlushTiming {
    /// Ends an inline wait: its reply, or a failure, is in the slot.
    fn hand_over(&self) {
        self.arrived.raise();
        if let Some(wake) = self.shm_wake.get() {
            wake();
        }
    }
}

/// A same-host worker's shared memory with the server ([`crate::shm`]).
pub(crate) struct WorkerShm {
    shared: crate::shm::Shared,
    bell: crate::shm::Doorbell,
    /// One producer at a time, with its scratch buffer.
    producer: Mutex<Vec<u8>>,
}

impl WorkerShm {
    /// Sends a section through the ring; `false` if it does not fit now.
    fn push_section(&self, shard: u32, epoch: u64, batch: &Batch) -> bool {
        let mut buf = self.producer.lock().unwrap_or_else(|e| e.into_inner());
        buf.clear();
        crate::server::ring_head(&mut buf, req::APPEND, shard, epoch, 0);
        proto::encode_section(&mut buf, batch);
        if !self.shared.push(&buf) {
            return false;
        }
        drop(buf);
        self.bell.ring();
        true
    }

    /// Wakes every inline wait (the connection is gone).
    fn wake_all(&self) {
        for shard in 0..self.shared.layout().shards {
            if let Some(line) = self.shared.line(shard) {
                line.seq().fetch_add(1, Ordering::Release);
                crate::shm::wake(line.seq());
            }
        }
    }
}

impl Slot {
    /// Stops the shard; its runtime sees the error on its next poll.
    fn stop(&mut self, message: &str) {
        let message = self
            .failed
            .get_or_insert_with(|| message.to_owned())
            .clone();
        self.flushed = Some(Err(other(message)));
        self.want = None;
        self.appends.clear();
        if self.inline {
            self.timing.hand_over();
        }
        if let Some(n) = &self.notify {
            n();
        }
    }
}

/// The longest an inline flush spins for its reply before parking: the
/// runtime waits inline only for flushes this fast.
const INLINE_SPIN_CAP: Duration = Duration::from_micros(200);

pub(crate) struct Inner {
    writer: Mutex<Box<dyn Write + Send>>,
    pending: Mutex<HashMap<u64, Sender<Reply>>>,
    slots: Mutex<HashMap<u32, Slot>>,
    next: AtomicU64,
    dead: AtomicBool,
    /// The epoch the lease named: connection-level requests carry it.
    epoch: u64,
    shards: u32,
    capabilities: u32,
    first_segment_bytes: u64,
    /// Per shard: its lease's epoch while this worker holds it, else 0.
    leases: Mutex<Vec<u64>>,
    /// Signalled when a shard is granted or the connection is lost.
    granted: std::sync::Condvar,
    limits: Mutex<Option<std::sync::Weak<RemoteLimits>>>,
    /// Log format 3: the server log's next batch and active segment, from
    /// `DURABLE` frames, for checkpoints.
    log_position: AtomicU64,
    log_segment: AtomicU64,
    /// Run-store reads that went to the server (the rest were read ahead).
    store_reads: AtomicU64,
    /// Calls other machines sent through the server.
    calls: Mutex<Option<tokio::sync::mpsc::Sender<RelayedCall>>>,
    /// An invoked worker's calls from the moment it holds its shards (the
    /// server sends the ones that waited for it right after the lease).
    early_calls: Mutex<Option<tokio::sync::mpsc::Receiver<RelayedCall>>>,
    call_notify: Mutex<Option<fusee::env::Notify>>,
    /// Messages relayed from shards other workers hold.
    relay: Mutex<RelayState>,
    /// Same-host shared memory, when the server granted it.
    shm: Option<Arc<WorkerShm>>,
    /// An invoked worker's state ([`crate::invoke`]).
    pub(crate) invoked: Option<crate::invoke::worker::Invoked>,
}

/// Relayed messages that arrived before their shard's thread attached are
/// kept, up to this many frames per shard; the sender resends the rest.
const EARLY_MESSAGES: usize = 4096;

#[derive(Default)]
struct RelayState {
    /// Each shard's thread, once it runs.
    inbound: HashMap<u32, Box<dyn fusee::env::RelayInbound>>,
    /// Messages for shards whose thread has not attached yet (granted to
    /// this worker and still recovering), or for every shard while `held`:
    /// `(from, epoch, entries)`.
    early: HashMap<u32, VecDeque<(u32, u64, Vec<u8>)>>,
    /// Keep relayed messages back even from attached shards
    /// ([`Remote::hold_relay`]): the code is not registered yet.
    held: bool,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("epoch", &self.epoch)
            .finish()
    }
}

/// A call from another machine, relayed by the server to this worker.
#[derive(Clone, Debug)]
pub struct RelayedCall {
    pub relay: u64,
    pub method: String,
    pub args: Vec<u8>,
}

impl Inner {
    fn write_frame(&self, kind: u8, body: &[u8]) -> io::Result<()> {
        let mut out = Vec::with_capacity(body.len() + frame::HEADER);
        frame::encode(kind, body, &mut out);
        let mut w = self.writer.lock().map_err(|_| other("poisoned"))?;
        w.write_all(&out)
    }

    /// Answers a relayed call.
    pub(crate) fn reply_call(&self, relay: u64, result: Result<Vec<u8>, String>) -> io::Result<()> {
        if let Some(inv) = &self.invoked {
            // Workers answer `{"value": ...}` or `{"error": ..., "kind": ...}`.
            let failed = match &result {
                Err(_) => true,
                Ok(v) => serde_json::from_slice::<serde_json::Value>(v)
                    .ok()
                    .is_some_and(|v| v.get("error").is_some()),
            };
            if !inv.answered(relay, failed) {
                return Ok(());
            }
        }
        let mut b = Vec::new();
        proto::head(
            &mut b,
            Head {
                id: relay,
                shard: 0,
                epoch: self.epoch,
            },
        );
        match result {
            Ok(v) => {
                b.push(1);
                put_bytes(&mut b, &v);
            }
            Err(e) => {
                b.push(0);
                put_bytes(&mut b, e.as_bytes());
            }
        }
        self.write_frame(req::CALL_REPLY, &b)
    }

    /// The epoch of `shard`'s lease while this worker holds it, else 0.
    fn lease_of(&self, shard: u32) -> u64 {
        self.leases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(shard as usize)
            .copied()
            .unwrap_or(0)
    }

    /// Relayed messages for `to`: to its thread, or kept until it attaches.
    fn relayed(&self, to: u32, from: u32, epoch: u64, entries: &[u8]) {
        let mut st = self.relay.lock().unwrap_or_else(|e| e.into_inner());
        if !st.held
            && let Some(inbound) = st.inbound.get(&to)
        {
            inbound.messages(from, epoch, entries);
            return;
        }
        let early = st.early.entry(to).or_default();
        if early.len() >= EARLY_MESSAGES {
            early.pop_front();
        }
        early.push_back((from, epoch, entries.to_vec()));
    }
}

/// Carries messages between this worker's shards and shards other
/// workers hold, through the server ([`proto::MESSAGE_RELAY`]).
struct RemoteRelay {
    inner: Arc<Inner>,
}

impl fusee::env::Relay for RemoteRelay {
    fn holds(&self, shard: u32) -> bool {
        self.inner.lease_of(shard) != 0
    }

    fn send(&self, from: u32, target: u32, entries: &[u8]) {
        let epoch = self.inner.lease_of(from);
        if epoch == 0 || self.inner.dead.load(Ordering::Acquire) {
            return;
        }
        let mut b = Vec::with_capacity(entries.len() + 32);
        proto::head(
            &mut b,
            Head {
                id: 0,
                shard: from,
                epoch,
            },
        );
        put_u32(&mut b, target);
        put_bytes(&mut b, entries);
        let _ = self.inner.write_frame(req::RELAY, &b);
    }

    fn ack(&self, from: u32, acks: &[(u32, u64, u64)]) {
        let epoch = self.inner.lease_of(from);
        if epoch == 0 || acks.is_empty() || self.inner.dead.load(Ordering::Acquire) {
            return;
        }
        let mut b = Vec::with_capacity(24 + acks.len() * 20);
        proto::head(
            &mut b,
            Head {
                id: 0,
                shard: from,
                epoch,
            },
        );
        put_u32(&mut b, acks.len() as u32);
        for &(shard, epoch, id) in acks {
            put_u32(&mut b, shard);
            put_u64(&mut b, epoch);
            put_u64(&mut b, id);
        }
        let _ = self.inner.write_frame(req::RELAY_ACKS, &b);
    }

    fn attach(&self, shard: u32, inbound: Box<dyn fusee::env::RelayInbound>) {
        let mut st = self.inner.relay.lock().unwrap_or_else(|e| e.into_inner());
        if !st.held {
            for (from, epoch, entries) in st.early.remove(&shard).unwrap_or_default() {
                inbound.messages(from, epoch, &entries);
            }
        }
        st.inbound.insert(shard, inbound);
    }
}

impl LimitWire for Inner {
    fn send_limit(
        &self,
        o: u8,
        name: &str,
        ticket: u64,
        a: u32,
        b: f64,
        priority: &fusee::limits::Priority,
    ) -> io::Result<()> {
        let mut body = Vec::new();
        proto::head(
            &mut body,
            Head {
                id: 0,
                shard: 0,
                epoch: self.epoch,
            },
        );
        body.push(o);
        put_bytes(&mut body, name.as_bytes());
        put_u64(&mut body, ticket);
        body.extend_from_slice(&a.to_le_bytes());
        put_u64(&mut body, b.to_bits());
        crate::limits::put_priority(&mut body, o, priority);
        self.write_frame(req::LIMIT, &body)
    }
}

/// Shuts the connection down when the last handle goes away, so the
/// server sees the worker leave and hands the lease to a standby.
struct Closer(Box<dyn Fn() + Send + Sync>);

impl Drop for Closer {
    fn drop(&mut self) {
        (self.0)();
    }
}

/// A worker's connection to a server, holding the shards' lease.
#[derive(Clone)]
pub struct Remote {
    inner: Arc<Inner>,
    closer: Arc<Closer>,
}

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

impl Remote {
    /// Keeps messages relayed from other workers' shards back until
    /// [`release_relay`](Self::release_relay), even from shards whose
    /// thread runs: call it before `Engine::open` when the code is
    /// registered after the engine opens. Otherwise a child start (or a
    /// signal) that waited for this worker can reach its shard before its
    /// type is registered, and fails as a type no code is registered for.
    pub fn hold_relay(&self) {
        self.inner
            .relay
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .held = true;
    }

    /// Delivers the messages [`hold_relay`](Self::hold_relay) kept back,
    /// in the order they came, and lets later ones through.
    pub fn release_relay(&self) {
        let mut st = self.inner.relay.lock().unwrap_or_else(|e| e.into_inner());
        st.held = false;
        let RelayState { inbound, early, .. } = &mut *st;
        for (shard, to) in inbound.iter() {
            for (from, epoch, entries) in early.remove(shard).unwrap_or_default() {
                to.messages(from, epoch, &entries);
            }
        }
    }

    /// Connects and waits (as a standby, if another worker holds the lease)
    /// until this worker holds every shard. `addr` is `host:port`,
    /// `unix:/path.sock` or `replicas://...`, optionally followed by
    /// connection options (`?ca=...&token-file=...`, see
    /// [`Security::from_options`](crate::security::Security::from_options)).
    pub fn connect(addr: &str) -> io::Result<Self> {
        Self::connect_endpoint(&crate::endpoint::Endpoint::parse(addr)?)
    }

    /// Like [`connect`](Self::connect), with security given separately
    /// (the address then has no options).
    pub fn connect_with(addr: &str, security: &crate::security::Security) -> io::Result<Self> {
        Self::connect_endpoint(&crate::endpoint::Endpoint::with_security(addr, security)?)
    }

    fn connect_endpoint(endpoint: &crate::endpoint::Endpoint) -> io::Result<Self> {
        let most = endpoint.max_shards();
        let shm = endpoint.shm();
        let inv = endpoint.invocation();
        if !endpoint.is_replicas() {
            return Self::with_conn(endpoint.connect()?, most, shm, inv);
        }
        // A standby can lose its primary before it receives its lease.
        // Re-discover once in that case. No workflow has been submitted yet.
        Self::with_conn(endpoint.connect()?, most, shm, inv)
            .or_else(|_| Self::with_conn(endpoint.connect()?, most, shm, inv))
    }

    fn with_conn(
        conn: crate::security::Conn,
        most: u32,
        shm: bool,
        invocation: Option<u64>,
    ) -> io::Result<Self> {
        let (reader, writer, raw) = conn.split();
        // Shared memory arrives as descriptors with the lease, on the
        // plain Unix socket.
        let fd = if shm && crate::shm::SUPPORTED {
            raw.unix_fd()
        } else {
            None
        };
        let closer = Closer(Box::new(move || {
            let _ = raw.shutdown();
        }));
        Self::with_stream(
            Box::new(reader),
            Box::new(writer),
            closer,
            most,
            fd,
            invocation,
        )
    }

    #[cfg(unix)]
    pub fn connect_unix(path: &str) -> io::Result<Self> {
        Self::connect(&format!("unix:{path}"))
    }

    /// `most`: the most shards to hold at once (0: all), offered to servers
    /// that lease shards one at a time. `shm_fd`: the Unix socket, to ask
    /// for same-host shared memory over it. `invocation`: the invocation an
    /// invoked worker answers.
    fn with_stream(
        mut reader: Box<dyn Read + Send>,
        mut writer: Box<dyn Write + Send>,
        closer: Closer,
        most: u32,
        shm_fd: Option<std::os::fd::RawFd>,
        invocation: Option<u64>,
    ) -> io::Result<Self> {
        let mut out = Vec::new();
        let mut body = Vec::new();
        put_u64(&mut body, 1);
        // Offered capabilities; servers predating the offer ignore it.
        let mut offer = proto::STORE_OFFSETS
            | proto::LEASE_LIVENESS
            | proto::SHARED_LOG
            | proto::SHARD_LEASES
            | proto::MESSAGE_RELAY;
        if shm_fd.is_some() {
            offer |= proto::SHARED_MEMORY;
        }
        if invocation.is_some() {
            offer |= proto::INVOKED;
        }
        put_u32(&mut body, offer);
        put_u32(&mut body, most);
        if let Some(id) = invocation {
            put_u64(&mut body, id);
        }
        frame::encode(req::HELLO, &body, &mut out);
        writer.write_all(&out)?;
        let mut dec = frame::Decoder::new();
        let mut buf = vec![0u8; 1 << 16];
        let mut fds = Vec::new();
        let (shards, epoch, capabilities, first_segment_bytes, ping, leases, ring, remaining) = loop {
            if let Some((kind, body)) = dec.next().map_err(other)? {
                if kind == resp::ERR {
                    let mut r = Reader::new(&body);
                    let _id = r.u64();
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("the server refused this worker: {}", r.str().unwrap_or("")),
                    ));
                }
                if kind != resp::LEASE {
                    return Err(other("expected a lease"));
                }
                let mut r = Reader::new(&body);
                let _id = r.u64().map_err(other)?;
                let shards = r.u32().map_err(other)?;
                let epoch = r.u64().map_err(other)?;
                let capabilities = if r.is_empty() {
                    0
                } else {
                    r.u32().map_err(other)?
                };
                let initial = if capabilities & proto::FIRST_SEGMENT_LIMIT != 0 {
                    r.u64().map_err(other)?
                } else {
                    u64::MAX
                };
                let ping = if capabilities & proto::LEASE_LIVENESS != 0 {
                    let interval = r.u32().map_err(other)?;
                    let _timeout = r.u32().map_err(other)?;
                    Some(Duration::from_millis(u64::from(interval.max(1))))
                } else {
                    None
                };
                // Shards leased one at a time: those granted now. Otherwise
                // every shard, under the lease's epoch.
                let mut leases = vec![0u64; shards as usize];
                if capabilities & proto::SHARD_LEASES != 0 {
                    read_grant(&mut r, &mut leases).map_err(other)?;
                } else {
                    leases.fill(epoch);
                }
                let ring = if capabilities & proto::SHARED_MEMORY != 0 {
                    Some(r.u64().map_err(other)?)
                } else {
                    None
                };
                let remaining = if capabilities & proto::INVOKED != 0 {
                    Some(Duration::from_millis(u64::from(r.u32().map_err(other)?)))
                } else {
                    None
                };
                if invocation.is_some() && remaining.is_none() {
                    return Err(other(
                        "the server does not invoke workers: drop invocation= from the address",
                    ));
                }
                break (
                    shards,
                    epoch,
                    capabilities,
                    initial,
                    ping,
                    leases,
                    ring,
                    remaining,
                );
            }
            let n = match shm_fd {
                Some(fd) => crate::shm::recv_with_fds(fd, &mut buf, &mut fds)?,
                None => reader.read(&mut buf)?,
            };
            if n == 0 {
                return Err(other("server closed the connection"));
            }
            dec.feed(&buf[..n]);
        };
        let shm = match ring {
            Some(ring) => {
                let layout =
                    crate::shm::Layout::new(shards, usize::try_from(ring).map_err(other)?)?;
                if fds.len() < 2 {
                    return Err(other("the server granted shared memory without passing it"));
                }
                let mut fds = fds.into_iter();
                let shared = crate::shm::Shared::attach(fds.next().expect("checked"), layout)?;
                let bell = crate::shm::Doorbell::attach(fds.next().expect("checked"))?;
                Some(Arc::new(WorkerShm {
                    shared,
                    bell,
                    producer: Mutex::new(Vec::new()),
                }))
            }
            None => None,
        };
        let early = invocation.map(|_| tokio::sync::mpsc::channel(1024));
        let inner = Arc::new(Inner {
            writer: Mutex::new(writer),
            pending: Mutex::new(HashMap::new()),
            slots: Mutex::new(HashMap::new()),
            next: AtomicU64::new(2),
            dead: AtomicBool::new(false),
            epoch,
            shards,
            capabilities,
            first_segment_bytes,
            leases: Mutex::new(leases),
            granted: std::sync::Condvar::new(),
            limits: Mutex::new(None),
            log_position: AtomicU64::new(0),
            log_segment: AtomicU64::new(0),
            store_reads: AtomicU64::new(0),
            calls: Mutex::new(early.as_ref().map(|(tx, _)| tx.clone())),
            early_calls: Mutex::new(early.map(|(_, rx)| rx)),
            call_notify: Mutex::new(None),
            relay: Mutex::new(RelayState::default()),
            shm,
            invoked: invocation
                .zip(remaining)
                .map(|(id, left)| crate::invoke::worker::Invoked::new(id, shards, left)),
        });
        let i2 = inner.clone();
        std::thread::Builder::new()
            .name("fusee-remote".into())
            .spawn(move || reader_loop(i2, reader, dec))?;
        if let Some(interval) = ping {
            let weak = Arc::downgrade(&inner);
            std::thread::Builder::new()
                .name("fusee-ping".into())
                .spawn(move || ping_loop(weak, interval))?;
        }
        Ok(Self {
            inner,
            closer: Arc::new(closer),
        })
    }

    /// Whether log sections and durable positions go through same-host
    /// shared memory instead of the socket.
    pub fn shared_memory(&self) -> bool {
        self.inner.shm.is_some()
    }

    /// The invocation this worker answers, if it is an invoked worker.
    pub fn invocation(&self) -> Option<u64> {
        self.inner.invoked.as_ref().map(|i| i.id)
    }

    /// An invoked worker: whether to keep running ([`crate::invoke`]).
    /// Asks every held shard what it has left to do, and drains once all
    /// are idle. `awaiting`: workflow types the host has runs of and no
    /// code for. Cheap; call it every few milliseconds until it says to
    /// stop, then close the engine and call
    /// [`finish_invocation`](Self::finish_invocation).
    pub fn invocation_poll(&self, awaiting: &[String]) -> crate::invoke::Progress {
        let Some(inv) = &self.inner.invoked else {
            return crate::invoke::Progress::Idle;
        };
        if self.inner.dead.load(Ordering::Acquire) {
            return crate::invoke::Progress::Lost;
        }
        let held = self.held_shards();
        let notify = |shard: u32| {
            let n = self
                .inner
                .slots
                .lock()
                .ok()
                .and_then(|s| s.get(&shard).and_then(|slot| slot.notify.clone()));
            if let Some(n) = n {
                n();
            }
        };
        let drain = |shards: &[u32]| -> io::Result<std::sync::mpsc::Receiver<Reply>> {
            let id = self.inner.next.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = channel();
            self.inner
                .pending
                .lock()
                .map_err(|_| other("poisoned"))?
                .insert(id, tx);
            let mut b = Vec::new();
            proto::head(
                &mut b,
                Head {
                    id,
                    shard: 0,
                    epoch: self.inner.epoch,
                },
            );
            put_u32(&mut b, shards.len() as u32);
            for &s in shards {
                put_u32(&mut b, s);
            }
            self.inner.write_frame(req::DRAIN, &b)?;
            Ok(rx)
        };
        inv.poll(&held, awaiting, &notify, &drain)
    }

    /// An invoked worker leaves: reports each held shard's next due time
    /// from its engine's last report at close (shards whose engine did not
    /// close cleanly are left out, so the server treats them as crashed),
    /// then closes the connection. Idempotent.
    pub fn finish_invocation(&self) {
        let Some(inv) = &self.inner.invoked else {
            return;
        };
        let held: Vec<(u32, u64)> = self
            .inner
            .leases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .enumerate()
            .filter(|(_, e)| **e != 0)
            .map(|(s, e)| (s as u32, *e))
            .collect();
        if let Some(body) = inv.finish(&held)
            && !self.inner.dead.load(Ordering::Acquire)
        {
            let _ = self.inner.write_frame(req::IDLE, &body);
        }
        (self.closer.0)();
    }

    pub(crate) fn healthy(&self) -> io::Result<()> {
        if self.inner.dead.load(Ordering::Acquire) {
            Err(other("connection to the server was lost"))
        } else {
            Ok(())
        }
    }

    pub fn shards(&self) -> u32 {
        self.inner.shards
    }

    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    /// The shards this worker holds now. A server that leases shards one at
    /// a time grants more as other workers' shards free up; the engine's
    /// other shards wait as standbys until then.
    pub fn held_shards(&self) -> Vec<u32> {
        self.inner
            .leases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .enumerate()
            .filter(|(_, e)| **e != 0)
            .map(|(s, _)| s as u32)
            .collect()
    }

    /// Run-store reads this connection sent to the server. Reads that
    /// continue a sequential scan (a mass wake of parked runs) are mostly
    /// served from what earlier ones read ahead.
    pub fn store_round_trips(&self) -> u64 {
        self.inner.store_reads.load(Ordering::Relaxed)
    }

    /// Step limits held on the server (permits are leases).
    pub fn limits(&self) -> Arc<RemoteLimits> {
        let mut slot = self.inner.limits.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(l) = slot.as_ref().and_then(|w| w.upgrade()) {
            return l;
        }
        let wire: Arc<dyn LimitWire> = self.inner.clone();
        let l = RemoteLimits::new(wire, self.inner.epoch);
        *slot = Some(Arc::downgrade(&l));
        l
    }

    /// Calls other machines send through the server, for
    /// [`crate::client::serve_calls`].
    pub fn take_calls(&self) -> tokio::sync::mpsc::Receiver<RelayedCall> {
        if let Some(rx) = self
            .inner
            .early_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            return rx;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        *self.inner.calls.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
        rx
    }

    pub(crate) fn set_call_notify(&self, notify: fusee::env::Notify) {
        *self
            .inner
            .call_notify
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(notify.clone());
        notify();
    }

    pub(crate) fn inner(&self) -> Arc<Inner> {
        self.inner.clone()
    }

    pub fn env(&self, shard: u32) -> RemoteEnv {
        let timing = self
            .inner
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(shard)
            .or_default()
            .timing
            .clone();
        if let Some(shm) = &self.inner.shm {
            let shm = shm.clone();
            let _ = timing.shm_wake.set(Box::new(move || {
                if let Some(line) = shm.shared.line(shard) {
                    line.seq().fetch_add(1, Ordering::Release);
                    crate::shm::wake(line.seq());
                }
            }));
        }
        RemoteEnv {
            remote: self.inner.clone(),
            _closer: self.closer.clone(),
            shared: self.inner.capabilities & proto::SHARED_LOG != 0,
            ahead: ReadAhead::default(),
            shard,
            store_len: 0,
            store_gen: 0,
            can_compact_store: false,
            can_archive_history: false,
            can_index: false,
            rng: fusee::local::unix_ms() ^ (u64::from(shard) << 40) ^ 0x9e37_79b9_7f4a_7c15,
            timing,
            round_trip_us: 0,
        }
    }
}

/// Keeps the lease: the server revokes it after its liveness timeout without
/// traffic. Runs on its own thread, so busy shard threads or a request
/// waiting on a slow server never delay it. Ends with the connection.
fn ping_loop(inner: Weak<Inner>, interval: Duration) {
    loop {
        std::thread::sleep(interval);
        let Some(inner) = inner.upgrade() else { return };
        if inner.dead.load(Ordering::Acquire) {
            return;
        }
        // With shared memory the heartbeat in the mapping keeps the lease.
        if let Some(shm) = &inner.shm {
            shm.shared.heartbeat().fetch_add(1, Ordering::Release);
            continue;
        }
        let mut body = Vec::with_capacity(20);
        proto::head(
            &mut body,
            Head {
                id: 0,
                shard: 0,
                epoch: inner.epoch,
            },
        );
        if inner.write_frame(req::PING, &body).is_err() {
            return;
        }
    }
}

/// Records granted shards: `count u32, then shard u32, epoch u64` each.
fn read_grant(r: &mut Reader<'_>, leases: &mut [u64]) -> fusee_core::wire::DecodeResult<()> {
    let n = r.u32()?;
    for _ in 0..n {
        let shard = r.u32()? as usize;
        let epoch = r.u64()?;
        if let Some(slot) = leases.get_mut(shard) {
            *slot = epoch;
        }
    }
    Ok(())
}

fn reader_loop(inner: Arc<Inner>, mut reader: Box<dyn Read + Send>, mut dec: frame::Decoder) {
    let mut buf = vec![0u8; 1 << 16];
    let fail_all = |inner: &Inner, msg: &str| {
        inner.dead.store(true, Ordering::Release);
        // Standby shards stop waiting for a lease.
        drop(inner.leases.lock());
        inner.granted.notify_all();
        inner.calls.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(notify) = inner
            .call_notify
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            notify();
        }
        if let Ok(mut p) = inner.pending.lock() {
            for (_, tx) in p.drain() {
                let _ = tx.send(Err(msg.into()));
            }
        }
        if let Ok(mut s) = inner.slots.lock() {
            for slot in s.values_mut() {
                slot.ckpt = Some(Err(other(msg)));
                slot.stop(msg);
            }
        }
        if let Some(shm) = &inner.shm {
            shm.wake_all();
        }
    };
    loop {
        loop {
            let next = match dec.next() {
                Ok(n) => n,
                Err(e) => {
                    fail_all(&inner, &e.to_string());
                    return;
                }
            };
            let Some((kind, body)) = next else { break };
            let mut r = Reader::new(&body);
            let Ok(id) = r.u64() else { continue };
            match kind {
                resp::OK | resp::ERR => {
                    let tx = inner.pending.lock().ok().and_then(|mut p| p.remove(&id));
                    if let Some(tx) = tx {
                        let v = if kind == resp::OK {
                            Ok(r.rest().to_vec())
                        } else {
                            Err(r.str().unwrap_or("error").to_owned())
                        };
                        let _ = tx.send(v);
                    }
                }
                resp::LIMITED => {
                    let (granted, absolute) = (r.u8().unwrap_or(0) == 1, r.u64().unwrap_or(0));
                    // New servers include a relative delay, independent of clock skew.
                    let retry = r
                        .u64()
                        .ok()
                        .filter(|d| *d > 0)
                        .map_or(absolute, |d| fusee::local::unix_ms().saturating_add(d));
                    let l = inner
                        .limits
                        .lock()
                        .ok()
                        .and_then(|l| l.as_ref().and_then(|w| w.upgrade()));
                    if let Some(l) = l {
                        l.answered(id, granted, retry);
                    }
                }
                resp::CALL => {
                    let method = r.str().unwrap_or_default().to_owned();
                    let args = r.bytes().unwrap_or_default().to_vec();
                    if let Some(inv) = &inner.invoked {
                        inv.called(id, &method);
                    }
                    let tx = inner.calls.lock().ok().and_then(|c| c.clone());
                    let call = RelayedCall {
                        relay: id,
                        method,
                        args,
                    };
                    match tx {
                        Some(tx) => {
                            if tx.try_send(call).is_err() {
                                let _ = inner.reply_call(
                                    id,
                                    Err("worker call queue is full or closed".into()),
                                );
                            } else if let Some(wake) = inner
                                .call_notify
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .clone()
                            {
                                wake();
                            }
                        }
                        None => {
                            let _ = inner.reply_call(
                                id,
                                Err("this worker does not accept calls from other machines".into()),
                            );
                        }
                    }
                }
                resp::FLUSHED | resp::CHECKPOINTED | resp::IO_FAILED => {
                    let Ok(shard) = r.u32() else { continue };
                    if let Ok(mut s) = inner.slots.lock() {
                        let slot = s.entry(shard).or_default();
                        if kind == resp::IO_FAILED {
                            // Either kind of background work failed: stop.
                            slot.stop(r.str().unwrap_or("server I/O failed"));
                        } else {
                            let res = match &slot.failed {
                                Some(m) => Err(other(m)),
                                None => r.u64().map_err(other),
                            };
                            if kind == resp::CHECKPOINTED {
                                slot.ckpt = Some(res);
                            } else {
                                slot.flushed = Some(res);
                                if slot.inline {
                                    // The shard thread is spinning or parked
                                    // for exactly this reply: hand it over
                                    // without waking its runtime.
                                    slot.timing.hand_over();
                                    continue;
                                }
                                let sent = slot.timing.submitted_us.load(Ordering::Relaxed);
                                if sent != 0 {
                                    slot.timing.last_us.store(
                                        fusee::local::monotonic_us().saturating_sub(sent),
                                        Ordering::Relaxed,
                                    );
                                }
                            }
                            if let Some(n) = &slot.notify {
                                n();
                            }
                        }
                    }
                }
                resp::GRANT => {
                    let _ = id;
                    let mut leases = inner.leases.lock().unwrap_or_else(|e| e.into_inner());
                    if read_grant(&mut r, &mut leases).is_ok() {
                        inner.granted.notify_all();
                    }
                }
                resp::MESSAGES => {
                    let (Ok(to), Ok(from), Ok(epoch), Ok(entries)) =
                        (r.u32(), r.u32(), r.u64(), r.bytes())
                    else {
                        continue;
                    };
                    inner.relayed(to, from, epoch, entries);
                }
                resp::MESSAGE_ACKS => {
                    let Ok(n) = r.u32() else { continue };
                    let st = inner.relay.lock().unwrap_or_else(|e| e.into_inner());
                    for _ in 0..n {
                        let (Ok(shard), Ok(epoch), Ok(outbox)) = (r.u32(), r.u64(), r.u64()) else {
                            break;
                        };
                        // Only the lease that sent the entry may take its
                        // acknowledgment: a later holder renumbered its outbox.
                        if epoch != 0
                            && inner.lease_of(shard) == epoch
                            && let Some(inbound) = st.inbound.get(&shard)
                        {
                            inbound.acked(outbox);
                        }
                    }
                }
                resp::DURABLE => {
                    // `id` held 0; positions are cumulative and may arrive
                    // out of order, so each only ever raises what is known.
                    let (Ok(position), Ok(segment), Ok(n)) = (r.u64(), r.u64(), r.u32()) else {
                        continue;
                    };
                    let _ = id;
                    inner.log_position.fetch_max(position, Ordering::AcqRel);
                    inner.log_segment.fetch_max(segment, Ordering::AcqRel);
                    let Ok(mut s) = inner.slots.lock() else {
                        continue;
                    };
                    for _ in 0..n {
                        let (Ok(shard), Ok(lsn), Ok(flags)) = (r.u32(), r.u64(), r.u8()) else {
                            break;
                        };
                        let slot = s.entry(shard).or_default();
                        slot.pins = flags & proto::DURABLE_PINS != 0;
                        slot.durable = slot.durable.max(lsn);
                        let Some(want) = slot.want else { continue };
                        if slot.durable < want || slot.failed.is_some() {
                            continue;
                        }
                        slot.want = None;
                        slot.flushed = Some(Ok(slot.durable));
                        if slot.inline {
                            slot.timing.hand_over();
                            continue;
                        }
                        let sent = slot.timing.submitted_us.load(Ordering::Relaxed);
                        if sent != 0 {
                            slot.timing.last_us.store(
                                fusee::local::monotonic_us().saturating_sub(sent),
                                Ordering::Relaxed,
                            );
                        }
                        if let Some(n) = &slot.notify {
                            n();
                        }
                    }
                }
                resp::STORE_APPENDED => {
                    let Ok(shard) = r.u32() else { continue };
                    let written = r.u64().ok();
                    if let Ok(mut s) = inner.slots.lock() {
                        let slot = s.entry(shard).or_default();
                        let problem = match (slot.appends.pop_front(), written) {
                            _ if slot.failed.is_some() => None,
                            (Some((request, referenced)), Some(written)) if request == id => {
                                (referenced != written).then(|| {
                                    format!(
                                        "run-store append offset mismatch on shard {shard}: this worker referenced offset {referenced}, the server wrote at {written}; the shard has stopped"
                                    )
                                })
                            }
                            _ => Some(format!(
                                "unexpected run-store append reply on shard {shard}; the shard has stopped"
                            )),
                        };
                        if let Some(problem) = problem {
                            slot.stop(&problem);
                        }
                    }
                }
                _ => {}
            }
        }
        match reader.read(&mut buf) {
            Ok(0) | Err(_) => {
                fail_all(&inner, "connection to the server was lost");
                return;
            }
            Ok(n) => dec.feed(&buf[..n]),
        }
    }
}

pub struct RemoteEnv {
    remote: Arc<Inner>,
    _closer: Arc<Closer>,
    /// Log format 3: the server keeps one log for every shard.
    shared: bool,
    ahead: ReadAhead,
    shard: u32,
    store_len: u64,
    store_gen: u64,
    can_compact_store: bool,
    can_archive_history: bool,
    can_index: bool,
    rng: u64,
    timing: Arc<FlushTiming>,
    /// Smoothed flush round trip in µs, for the inline spin budget.
    round_trip_us: u64,
}

/// Run-store reads that continue a sequential scan (a mass wake loading
/// runs parked one after another) read further ahead each time, so most of
/// them need no round trip. A read elsewhere reads only what it asked for.
#[derive(Default)]
struct ReadAhead {
    generation: u64,
    start: u64,
    bytes: Vec<u8>,
    /// Where the last read from the server ended.
    end: u64,
    window: u64,
}

/// The longest read-ahead of the run store.
const READ_AHEAD_MAX: u64 = 1 << 20;
/// The most run-store bytes one request appends or reads: well inside a
/// frame ([`fusee_core::frame::MAX_FRAME`]), so a store frame of any length
/// (up to the store's own 4 GiB) crosses the connection in pieces.
const STORE_CHUNK: usize = 32 << 20;
/// A read that starts at most this far past the last one continues a scan.
const READ_AHEAD_GAP: u64 = 64 << 10;

impl RemoteEnv {
    /// This shard's lease epoch (0 while another worker holds it).
    fn lease_epoch(&self) -> u64 {
        self.remote
            .leases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(self.shard as usize)
            .copied()
            .unwrap_or(0)
    }

    /// The flush in flight for this shard waits for `last` (log format 3);
    /// fails if the shard has stopped.
    fn expect_durable(&self, last: Lsn, inline: bool) -> io::Result<()> {
        let mut slots = self.remote.slots.lock().map_err(|_| other("poisoned"))?;
        let slot = slots.entry(self.shard).or_default();
        if let Some(m) = &slot.failed {
            return Err(other(m));
        }
        slot.flushed = None;
        slot.want = self.shared.then_some(last);
        slot.inline = inline;
        if inline {
            // Under the lock, so a reply or failure from now on is seen.
            self.timing.arrived.take();
        }
        Ok(())
    }

    /// Sends a section: through the shared-memory ring when there is one
    /// and no run-store append of this shard awaits its answer (the server
    /// must have written every frame a section may reference before it
    /// takes the section), else on the socket. `inline`: this thread waits
    /// on the shard's watermark itself, so the server sends no `DURABLE`
    /// frame for it.
    fn send_section(&self, id: u64, batch: &Batch, inline: bool) -> io::Result<()> {
        if let Some(shm) = &self.remote.shm
            && let Some(line) = shm.shared.line(self.shard)
        {
            let quiet = self
                .remote
                .slots
                .lock()
                .map_err(|_| other("poisoned"))?
                .get(&self.shard)
                .is_none_or(|s| s.appends.is_empty());
            // Before the section, so the server's publication sees it.
            line.notify()
                .store(u32::from(!(inline && quiet)), Ordering::Release);
            if quiet {
                if self.remote.dead.load(Ordering::Acquire) {
                    return Err(other("connection to the server was lost"));
                }
                if shm.push_section(self.shard, self.lease_epoch(), batch) {
                    return Ok(());
                }
                line.notify().store(1, Ordering::Release);
            }
        }
        let mut body = Vec::with_capacity(batch.section.len() + 32);
        proto::encode_section(&mut body, batch);
        self.send(req::APPEND, id, &body)
    }

    /// Shared memory: waits on this shard's watermark for `last`, spinning
    /// for about the recent round trip first. Ends early if the shard
    /// stops or the lease does.
    fn wait_watermark(&self, shm: &WorkerShm, last: Lsn) -> Option<Lsn> {
        let line = shm.shared.line(self.shard)?;
        let recent = match self.round_trip_us {
            0 => self.timing.last_us.load(Ordering::Relaxed),
            us => us,
        };
        let dead = &self.remote.dead;
        let arrived = &self.timing.arrived;
        let closed = shm.shared.closed();
        let done = || {
            line.durable().load(Ordering::Acquire) >= last
                || line.failed().load(Ordering::Acquire) != 0
                || arrived.is_raised()
                || closed.load(Ordering::Acquire) != 0
                || dead.load(Ordering::Acquire)
        };
        line.wait_until(
            spin::budget(Duration::from_micros(recent), INLINE_SPIN_CAP),
            Duration::from_secs(60),
            &done,
        );
        let durable = line.durable().load(Ordering::Acquire);
        (durable >= last).then_some(durable)
    }

    /// Ends an inline wait, returning the reply if it arrived.
    fn take_inline(&self) -> Option<io::Result<Lsn>> {
        let mut slots = self.remote.slots.lock().unwrap_or_else(|e| e.into_inner());
        let slot = slots.entry(self.shard).or_default();
        slot.inline = false;
        slot.flushed.take()
    }

    fn record_round_trip(&mut self, us: u64) {
        self.timing.last_us.store(us, Ordering::Relaxed);
        self.round_trip_us = if self.round_trip_us == 0 {
            us
        } else {
            (self.round_trip_us * 7 + us) / 8
        };
    }

    fn send(&self, kind: u8, id: u64, body: &[u8]) -> io::Result<()> {
        if self.remote.dead.load(Ordering::Acquire) {
            return Err(other("connection to the server was lost"));
        }
        let mut b = Vec::with_capacity(body.len() + 20);
        proto::head(
            &mut b,
            Head {
                id,
                shard: self.shard,
                epoch: self.lease_epoch(),
            },
        );
        b.extend_from_slice(body);
        let mut out = Vec::with_capacity(b.len() + frame::HEADER);
        frame::encode(kind, &b, &mut out);
        let mut w = self.remote.writer.lock().map_err(|_| other("poisoned"))?;
        w.write_all(&out)
    }

    fn call(&self, kind: u8, body: &[u8]) -> io::Result<Vec<u8>> {
        let id = self.remote.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = channel();
        {
            let mut requests = self.remote.pending.lock().map_err(|_| other("poisoned"))?;
            if self.remote.dead.load(Ordering::Acquire) {
                return Err(other("server disconnected"));
            }
            requests.insert(id, tx);
        }
        let result = self.send(kind, id, body).and_then(|()| {
            match rx.recv_timeout(Duration::from_secs(60)) {
                Ok(Ok(v)) => Ok(v),
                Ok(Err(m)) => Err(other(m)),
                Err(_) => Err(other("server did not answer")),
            }
        });
        if result.is_err() {
            self.remote
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
        }
        result
    }

    fn fire(&self, kind: u8, body: &[u8]) {
        let id = self.remote.next.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.send(kind, id, body) {
            self.stop(&e.to_string());
        }
    }

    fn stop(&self, message: &str) {
        if let Ok(mut s) = self.remote.slots.lock() {
            s.entry(self.shard).or_default().stop(message);
        }
    }

    /// Pipelined: this worker is the store's only writer, so it computes the
    /// offset itself. Under [`proto::STORE_OFFSETS`] the request carries that
    /// offset, the server writes nowhere else and answers with the offset it
    /// wrote at, which the reader thread checks. Any failure stops the shard
    /// before an acknowledgment that follows the append can count.
    fn append_store(
        &mut self,
        frame: &[u8],
        history: Option<(fusee_core::RunId, Lsn)>,
    ) -> io::Result<u64> {
        // The store is bytes at offsets to the server, and this worker is
        // its only writer: an append longer than a protocol frame carries
        // (a run with a long journal) goes as consecutive appends. A
        // history frame is archived whole, so it goes in one.
        if history.is_some() || frame.len() <= STORE_CHUNK {
            return self.append_store_piece(frame, history);
        }
        let off = self.store_len;
        for piece in frame.chunks(STORE_CHUNK) {
            self.append_store_piece(piece, None)?;
        }
        Ok(off)
    }

    fn append_store_piece(
        &mut self,
        frame: &[u8],
        history: Option<(fusee_core::RunId, Lsn)>,
    ) -> io::Result<u64> {
        let off = self.store_len;
        let verified = self.remote.capabilities & proto::STORE_OFFSETS != 0;
        let mut body = Vec::with_capacity(frame.len() + 32);
        if verified {
            put_u64(&mut body, off);
        }
        put_bytes(&mut body, frame);
        if let Some((run, lsn)) = history {
            put_u64(&mut body, run);
            put_u64(&mut body, lsn);
        }
        if verified {
            let id = self.remote.next.fetch_add(1, Ordering::Relaxed);
            {
                let mut slots = self.remote.slots.lock().map_err(|_| other("poisoned"))?;
                let slot = slots.entry(self.shard).or_default();
                if let Some(m) = &slot.failed {
                    return Err(other(m));
                }
                slot.appends.push_back((id, off));
            }
            if let Err(e) = self.send(req::STORE_APPEND_AT, id, &body) {
                self.stop(&e.to_string());
                return Err(e);
            }
        } else {
            self.fire(req::STORE_APPEND, &body);
        }
        self.store_len += frame.len() as u64;
        Ok(off)
    }
}

impl Env for RemoteEnv {
    fn monotonic_us(&self) -> u64 {
        fusee::local::monotonic_us()
    }
    fn now_ms(&self) -> u64 {
        fusee::local::unix_ms()
    }

    fn random(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn set_notify(&mut self, notify: Notify) {
        if let Ok(mut s) = self.remote.slots.lock() {
            s.entry(self.shard).or_default().notify = Some(notify);
        }
    }

    fn submit(&mut self, batch: Batch) {
        self.timing
            .submitted_us
            .store(fusee::local::monotonic_us(), Ordering::Relaxed);
        if self.shared {
            // Pipelined: the shard's position arrives in a `DURABLE` frame.
            let id = self.remote.next.fetch_add(1, Ordering::Relaxed);
            let sent = self
                .expect_durable(batch.last_lsn, false)
                .and_then(|()| self.send_section(id, &batch, false));
            if let Err(e) = sent {
                self.stop(&e.to_string());
            }
            return;
        }
        let mut body = Vec::with_capacity(batch.bytes as usize + 64);
        proto::encode_batch(&mut body, &batch);
        self.fire(req::SUBMIT, &body);
    }

    /// Waits on the calling shard thread for the server's reply: spins for
    /// about the recent round trip, then parks until the reader thread hands
    /// the reply over. The runtime takes this path only when it is idle and
    /// the last round trip was fast.
    fn supports_inline(&self) -> bool {
        true
    }

    fn flush_inline(&mut self, batch: Batch) -> io::Result<Lsn> {
        let id = self.remote.next.fetch_add(1, Ordering::Relaxed);
        let last = batch.last_lsn;
        self.expect_durable(last, true)?;
        let started = std::time::Instant::now();
        if self.shared
            && let Some(shm) = self.remote.shm.clone()
        {
            if let Err(e) = self.send_section(id, &batch, true) {
                self.stop(&e.to_string());
                self.take_inline();
                return Err(e);
            }
            drop(batch);
            let durable = self.wait_watermark(&shm, last);
            let elapsed = started.elapsed().as_micros() as u64;
            // A `DURABLE` frame (a section that went over the socket) may
            // have answered as well; either way the wait is over.
            let answered = {
                let mut slots = self.remote.slots.lock().unwrap_or_else(|e| e.into_inner());
                let slot = slots.entry(self.shard).or_default();
                slot.inline = false;
                slot.want = None;
                if let Some(d) = durable {
                    slot.durable = slot.durable.max(d);
                }
                match (&slot.failed, slot.flushed.take()) {
                    (Some(m), _) => Some(Err(other(m))),
                    (None, Some(Ok(d))) => Some(Ok(d.max(durable.unwrap_or(0)))),
                    (None, Some(Err(e))) => Some(Err(e)),
                    (None, None) => durable.map(Ok),
                }
            };
            return match answered {
                Some(Ok(d)) => {
                    self.record_round_trip(elapsed);
                    Ok(d)
                }
                Some(Err(e)) => Err(e),
                None => {
                    let message = if self.remote.dead.load(Ordering::Acquire)
                        || shm.shared.closed().load(Ordering::Acquire) != 0
                    {
                        "connection to the server was lost".to_owned()
                    } else if shm
                        .shared
                        .line(self.shard)
                        .is_some_and(|l| l.failed().load(Ordering::Acquire) != 0)
                    {
                        format!("the server refused shard {}'s records", self.shard)
                    } else {
                        "the server did not answer a flush within 60 s; the shard has stopped"
                            .to_owned()
                    };
                    self.stop(&message);
                    Err(other(message))
                }
            };
        }
        let sent = if self.shared {
            self.send_section(id, &batch, true)
        } else {
            let mut body = Vec::with_capacity(batch.bytes as usize + 64);
            proto::encode_batch(&mut body, &batch);
            self.send(req::SUBMIT, id, &body)
        };
        drop(batch);
        if let Err(e) = sent {
            self.stop(&e.to_string());
            self.take_inline();
            return Err(e);
        }
        let recent = match self.round_trip_us {
            0 => self.timing.last_us.load(Ordering::Relaxed),
            us => us,
        };
        let dead = &self.remote.dead;
        self.timing.arrived.wait(
            spin::budget(Duration::from_micros(recent), INLINE_SPIN_CAP),
            Duration::from_secs(60),
            Some(&|| dead.load(Ordering::Acquire)),
        );
        let elapsed = started.elapsed().as_micros() as u64;
        match self.take_inline() {
            Some(res) => {
                self.record_round_trip(elapsed);
                res
            }
            None => {
                let message = if dead.load(Ordering::Acquire) {
                    "connection to the server was lost"
                } else {
                    "the server did not answer a flush within 60 s; the shard has stopped"
                };
                self.stop(message);
                self.take_inline();
                Err(other(message))
            }
        }
    }

    fn last_flush_us(&self) -> u64 {
        self.timing.last_us.load(Ordering::Relaxed)
    }

    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>> {
        let mut slots = self.remote.slots.lock().ok()?;
        let slot = slots.get_mut(&self.shard)?;
        // Shared memory: the watermark may say so before the frame does.
        if let (Some(shm), Some(want), None, true) = (
            &self.remote.shm,
            slot.want,
            &slot.failed,
            slot.flushed.is_none(),
        ) && let Some(line) = shm.shared.line(self.shard)
        {
            let durable = line.durable().load(Ordering::Acquire);
            if durable >= want {
                slot.want = None;
                slot.durable = slot.durable.max(durable);
                return Some(Ok(durable));
            }
        }
        slot.flushed.take()
    }

    fn store_append(&mut self, frame_bytes: &[u8]) -> io::Result<u64> {
        self.append_store(frame_bytes, None)
    }

    fn store_append_history(
        &mut self,
        frame: &[u8],
        run: fusee_core::RunId,
        lsn: Lsn,
    ) -> io::Result<u64> {
        if !self.can_archive_history {
            return Err(other("server does not support history archives"));
        }
        self.append_store(frame, Some((run, lsn)))
    }
    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        let a = &mut self.ahead;
        let want_end = offset.saturating_add(u64::from(len));
        if a.generation == self.store_gen
            && offset >= a.start
            && want_end <= a.start + a.bytes.len() as u64
        {
            let from = (offset - a.start) as usize;
            return Ok(a.bytes[from..from + len as usize].to_vec());
        }
        // Continuing a scan: read further ahead. This worker is the store's
        // only writer, so everything below its length is on the server.
        let sequential =
            a.generation == self.store_gen && offset >= a.start && offset <= a.end + READ_AHEAD_GAP;
        a.window = if sequential {
            (a.window.max(READ_AHEAD_GAP / 2) * 2).min(READ_AHEAD_MAX)
        } else {
            0
        };
        let fetch = u64::from(len)
            .max(a.window.min(self.store_len.saturating_sub(offset)))
            .min(u64::from(u32::MAX));
        // One request for what a frame carries; a longer read (the frame of
        // a run with a long journal) takes several.
        let mut bytes = Vec::new();
        loop {
            let want = (fetch - bytes.len() as u64).min(STORE_CHUNK as u64);
            let mut body = Vec::with_capacity(12);
            put_u64(&mut body, offset + bytes.len() as u64);
            body.extend_from_slice(&(want as u32).to_le_bytes());
            self.remote.store_reads.fetch_add(1, Ordering::Relaxed);
            let v = self.call(req::STORE_READ, &body)?;
            let part = Reader::new(&v).blob().map_err(other)?;
            bytes.extend_from_slice(part);
            if bytes.len() as u64 >= fetch || (part.len() as u64) < want {
                break;
            }
        }
        if bytes.len() < len as usize {
            return Err(other("short run-store read"));
        }
        let out = bytes[..len as usize].to_vec();
        let a = &mut self.ahead;
        a.generation = self.store_gen;
        a.end = offset + bytes.len() as u64;
        a.start = offset;
        a.bytes = if fetch > u64::from(len) {
            bytes
        } else {
            Vec::new()
        };
        Ok(out)
    }

    fn store_len(&self) -> u64 {
        self.store_len
    }

    fn store_generation(&self) -> u64 {
        self.store_gen
    }

    fn compact_store(
        &mut self,
        live: &[(fusee_core::RunId, fusee_core::shard::BlobRef)],
    ) -> io::Result<bool> {
        if !self.can_compact_store {
            return Ok(false);
        }
        self.call(req::COMPACT_STORE, &[0])?;
        for page in live.chunks(4096) {
            let mut body = vec![1];
            for (run, blob) in page {
                put_u64(&mut body, *run);
                put_u64(&mut body, blob.offset);
                put_u32(&mut body, blob.len);
            }
            self.call(req::COMPACT_STORE, &body)?;
        }
        let result = self.call(req::COMPACT_STORE, &[2])?;
        let mut reader = Reader::new(&result);
        self.store_gen = reader.u64().map_err(other)?;
        self.store_len = reader.u64().map_err(other)?;
        Ok(true)
    }

    fn visibility_candidates(
        &mut self,
        filter: &fusee::visibility::ListFilter,
        limit: usize,
    ) -> Option<fusee::visibility::Candidates> {
        if !self.can_index {
            return None;
        }
        let mut body = Vec::new();
        put_bytes(&mut body, &serde_json::to_vec(filter).ok()?);
        put_u32(&mut body, limit.min(4096) as u32);
        serde_json::from_slice(&self.call(req::VISIBILITY, &body).ok()?).ok()?
    }
    fn checkpoint(&mut self, checkpoint: Checkpoint, delete_through_segment: Option<u64>) {
        let mut body = Vec::new();
        put_bytes(&mut body, &checkpoint.encode());
        put_u64(&mut body, delete_through_segment.unwrap_or(0));
        let id = self.remote.next.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.send(req::CHECKPOINT, id, &body)
            && let Ok(mut s) = self.remote.slots.lock()
        {
            s.entry(self.shard).or_default().ckpt = Some(Err(e));
        }
    }

    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>> {
        self.remote
            .slots
            .lock()
            .ok()?
            .get_mut(&self.shard)?
            .ckpt
            .take()
    }

    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        let v = self.call(req::LOAD_CHECKPOINT, &[])?;
        let mut r = Reader::new(&v);
        if r.u8().map_err(other)? == 0 {
            return Ok(None);
        }
        Checkpoint::decode(r.blob().map_err(other)?)
            .map(Some)
            .map_err(other)
    }

    fn load_store(&mut self, generation: u64, len: u64) -> io::Result<Vec<u8>> {
        let mut body = Vec::new();
        put_u64(&mut body, generation);
        put_u64(&mut body, len);
        let v = self.call(req::LOAD_STORE, &body)?;
        let mut r = Reader::new(&v);
        self.store_gen = r.u64().map_err(other)?;
        let data = r.blob().map_err(other)?.to_vec();
        self.store_len = data.len() as u64;
        Ok(data)
    }

    fn recover_store(
        &mut self,
        generation: u64,
        len: u64,
        visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry),
    ) -> io::Result<()> {
        let mut body = Vec::new();
        put_u64(&mut body, generation);
        put_u64(&mut body, len);
        body.push(1);
        let v = self.call(req::LOAD_STORE, &body)?;
        let mut r = Reader::new(&v);
        let tag = r.u64().map_err(other)?;
        if tag == u64::MAX {
            self.store_gen = r.u64().map_err(other)?;
            let length = r.u64().map_err(other)?;
            self.store_len = length;
            let capabilities = r.u8().unwrap_or(0);
            self.can_compact_store = capabilities & 1 != 0;
            self.can_archive_history = capabilities & 2 != 0;
            self.can_index = capabilities & 4 != 0;
            fusee::env::scan_store_pages(self, length, visit)
        } else {
            // Servers predating paged recovery return their original full prefix.
            self.store_gen = tag;
            let bytes = r.blob().map_err(other)?;
            self.store_len = bytes.len() as u64;
            fusee_core::store::scan_store(bytes, visit).map_err(other)
        }
    }
    fn segments(&mut self) -> io::Result<Vec<u64>> {
        if self.shared {
            return Ok(Vec::new());
        }
        let v = self.call(req::SEGMENTS, &[])?;
        let mut r = Reader::new(&v);
        let n = r.varint().map_err(other)?;
        (0..n).map(|_| r.varint().map_err(other)).collect()
    }

    fn read_segment(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        let mut body = Vec::new();
        put_u64(&mut body, segment);
        if self.remote.capabilities & proto::PAGED_RECOVERY == 0 {
            let response = self.call(req::READ_SEGMENT, &body)?;
            return Ok(Reader::new(&response).bytes().map_err(other)?.to_vec());
        }
        let extent = self.call(req::SEGMENT_EXTENT, &body)?;
        let mut r = Reader::new(&extent);
        let length = usize::try_from(r.u64().map_err(other)?).map_err(other)?;
        let prefix = usize::try_from(r.u64().map_err(other)?).map_err(other)?;
        if prefix > length || !r.is_empty() {
            return Err(other("invalid recovery segment extent"));
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(other)?;
        while bytes.len() < prefix {
            let size = (prefix - bytes.len()).min(proto::RECOVERY_PAGE);
            body.truncate(8);
            put_u64(&mut body, bytes.len() as u64);
            put_u32(&mut body, size as u32);
            let page = self.call(req::SEGMENT_RANGE, &body)?;
            if page.len() != size {
                return Err(other("short recovery segment page"));
            }
            bytes.extend_from_slice(&page);
        }
        bytes.resize(length, 0);
        Ok(bytes)
    }

    fn sync_recovered(&mut self) -> io::Result<()> {
        if self.shared {
            // The server's log syncs what it accepts when it opens, and
            // reads back only durable batches.
            return Ok(());
        }
        self.call(req::SYNC_RECOVERED, &[]).map(|_| ())
    }

    fn discard_segment(&mut self, segment: u64) -> io::Result<()> {
        if self.shared {
            return Ok(());
        }
        let mut body = Vec::new();
        put_u64(&mut body, segment);
        self.call(req::DISCARD, &body).map(|_| ())
    }

    fn exclusive_log(&self) -> bool {
        self.remote.capabilities & proto::EXCLUSIVE_LOG != 0
    }

    fn first_segment_bytes(&self, maximum: u64) -> u64 {
        maximum.min(self.remote.first_segment_bytes)
    }

    fn open_log(&mut self, segment: u64) -> io::Result<()> {
        if self.shared {
            return Ok(());
        }
        let mut body = Vec::new();
        put_u64(&mut body, segment);
        self.call(req::OPEN_LOG, &body).map(|_| ())
    }

    fn shared_log(&self) -> bool {
        self.shared
    }

    fn standby(&self) -> bool {
        self.lease_epoch() == 0
    }

    fn wait_lease(&mut self, timeout: Duration) -> io::Result<bool> {
        let leases = self.remote.leases.lock().unwrap_or_else(|e| e.into_inner());
        let (leases, _) = self
            .remote
            .granted
            .wait_timeout_while(leases, timeout, |l| {
                l.get(self.shard as usize).is_some_and(|e| *e == 0)
                    && !self.remote.dead.load(Ordering::Acquire)
            })
            .unwrap_or_else(|e| e.into_inner());
        if leases.get(self.shard as usize).is_some_and(|e| *e != 0) {
            return Ok(true);
        }
        if self.remote.dead.load(Ordering::Acquire) {
            return Err(other("connection to the server was lost"));
        }
        Ok(false)
    }

    /// The server reads its log and sends this shard's sections after
    /// `after`, in pages; the records are decoded here.
    fn recover_log(
        &mut self,
        after: Lsn,
    ) -> io::Result<(Vec<(Lsn, fusee_core::record::RecordBuf)>, Lsn)> {
        if !self.shared {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this server keeps a log per shard",
            ));
        }
        let mut sections = Vec::new();
        let mut last;
        loop {
            let mut body = Vec::with_capacity(12);
            put_u64(&mut body, after);
            put_u32(&mut body, sections.len() as u32);
            let page = self.call(req::RECOVER_LOG, &body)?;
            let mut r = Reader::new(&page);
            last = r.u64().map_err(other)?;
            let total = r.u32().map_err(other)? as usize;
            let count = r.u32().map_err(other)?;
            proto::decode_sections(&mut r, count, &mut sections).map_err(other)?;
            if sections.len() >= total {
                break;
            }
            if count == 0 {
                return Err(other("the server sent an empty log recovery page"));
            }
        }
        let (records, end) = fusee::wal::records_after(self.shard, &sections, after)?;
        let last = last.max(end).max(after);
        if let Ok(mut s) = self.remote.slots.lock() {
            let slot = s.entry(self.shard).or_default();
            slot.durable = slot.durable.max(last);
        }
        Ok((records, last))
    }

    fn log_position(&self) -> u64 {
        let shm = self.remote.shm.as_ref();
        self.remote
            .log_position
            .load(Ordering::Acquire)
            .max(shm.map_or(0, |s| s.shared.position().load(Ordering::Acquire)))
    }

    fn log_segment(&self) -> u64 {
        let shm = self.remote.shm.as_ref();
        self.remote
            .log_segment
            .load(Ordering::Acquire)
            .max(shm.map_or(0, |s| s.shared.segment().load(Ordering::Acquire)))
    }

    fn checkpoint_wanted(&self) -> bool {
        if let Some(line) = self
            .remote
            .shm
            .as_ref()
            .and_then(|s| s.shared.line(self.shard))
        {
            return self.shared && line.pins().load(Ordering::Acquire) != 0;
        }
        self.shared
            && self
                .remote
                .slots
                .lock()
                .ok()
                .and_then(|s| s.get(&self.shard).map(|slot| slot.pins))
                .unwrap_or(false)
    }

    fn due_wanted(&self) -> bool {
        self.remote
            .invoked
            .as_ref()
            .is_some_and(|i| i.wanted(self.shard))
    }

    fn report_due(&mut self, due: fusee::runtime::Due, closed: bool) {
        if let Some(i) = &self.remote.invoked {
            i.report(self.shard, due, closed);
        }
    }

    fn relay(&self) -> Option<Arc<dyn fusee::env::Relay>> {
        let relay: Arc<dyn fusee::env::Relay> = Arc::new(RemoteRelay {
            inner: self.remote.clone(),
        });
        (self.remote.capabilities & proto::MESSAGE_RELAY != 0).then_some(relay)
    }
}
