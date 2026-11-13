//! `replicated` durability: the log replicated to a quorum.
//!
//! Three (or five) processes each run a [`ReplicaNode`]. The replication
//! protocol is [`fusee_core::vsr`]; one protocol entry is one log-format-3
//! batch carrying a section from every shard of the primary's engine
//! ([`log3`]), so one group serves all of the primary's shards. The
//! primary's engine stores its log through [`ReplicatedEnv`]: a flush
//! completes when the entry commits, that is once any quorum holds it
//! durably, usually one round trip after it was sent; the primary's own
//! write runs in the background ([`writes`]). The engine of a backup that
//! becomes primary recovers from the committed entries themselves: they are
//! the only copy of the log on disk.
//!
//! One I/O thread per member reads the peer sockets, runs the protocol and
//! owns the protocol log. Shards hand it their sections and spin, then
//! park, until it publishes their committed position; it spins, then parks,
//! for the next section. Inbound frames are handled on the reading task
//! itself, and every round sends at most one write per peer.
//!
//! Each replica keeps:
//! - `vsr/log`: checksummed protocol entries (appended and flushed in
//!   groups; replaced atomically by write, flush, rename);
//! - `vsr/superblock`: view, last normal view, commit and applied hints,
//!   saved only after the log it vouches for is durable;
//! - `engine/`: the engine's run stores and checkpoints, one directory per
//!   shard.
//!
//! A dedicated server's group is a segment group: one shard, flush batches
//! in log format 2, materialized into `engine/shard-000/log`, which its
//! remote workers read.
//! Fixed membership. Durable checkpoint images anchor a trimmed protocol
//! log and let lagging replicas install a new baseline.

mod generation;
mod leases;
mod log3;
mod queue;
mod remote;
pub(crate) use remote::{ReplicaStorage, WorkerSession};
mod snapshot;
#[cfg(test)]
mod tests;
mod timings;
mod transfer;
mod transport;
mod writes;
pub use timings::{StageDistribution, stage_summary};

use crate::proto;
use crate::security::{Acceptor, Connector, Purpose, Secured};
use fusee::engine::EnvFactory;
use fusee::env::{Env, Notify};
use fusee::local::{
    BatchWriter, FlushMethod, LocalEnv, LocalOptions, Preallocate, flush_file, sync_dir,
};
use fusee::{Config, Durability};
use fusee_core::Lsn;
use fusee_core::frame;
use fusee_core::record::{Payload, RecordBuf};
use fusee_core::segment::Batch;
use fusee_core::store::Checkpoint;
use fusee_core::vsr::{
    Action, Entry, Msg, Op, Recovered, Replica, ReplicaId, Timing, decode_entry, decode_msg,
    encode_entry, encode_msg,
};
use fusee_core::wire::{Reader, put_u64};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone, Debug)]
pub struct ReplicaConfig {
    pub id: ReplicaId,
    /// Every replica's `host:port`, indexed by replica ID (an empty string
    /// for an ID that is not a member). For a new group, its members; once
    /// the membership has changed, the saved membership takes precedence
    /// and this only says where to find the group.
    pub peers: Vec<String>,
    /// A new member of a running group: it starts with an empty directory,
    /// asks the members in `peers` for the membership that includes it
    /// (see [`ReplicaNode::change_members`]), then catches up. Ignored once
    /// the directory holds a membership.
    pub join: bool,
    pub dir: PathBuf,
    pub local: LocalOptions,
    pub timing: Timing,
    /// Mutual TLS and the token between members. With TLS, every member
    /// presents its certificate in both directions and verifies its peers'
    /// against the CA; with a token, every stream proves it before its
    /// first frame. A member refuses frames on a stream that did neither.
    pub security: crate::security::Security,
}

/// A replica's role and progress. Status receivers are notified when `view`,
/// `primary`, `ready` or `stopped` changes; the progress fields are updated
/// in place on every commit without waking anyone, so read them by polling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeStatus {
    pub view: u64,
    pub primary: bool,
    /// Primary, with every entry committed and applied: the engine may open.
    pub ready: bool,
    /// The replica stopped after an unrecoverable storage error.
    pub stopped: bool,
    /// The membership epoch this replica is in (see [`ReplicaNode::membership`]).
    pub epoch: u64,
    /// A membership change removed this replica: it takes part in nothing
    /// and can be stopped once the new members have caught up.
    pub retired: bool,
    /// A new member that has not yet learned the membership including it.
    pub joining: bool,
    pub op: Op,
    /// Committed prefix covered by a durable portable checkpoint.
    pub checkpoint: Op,
    pub commit: Op,
    pub applied: Op,
}

#[derive(Default)]
struct Traffic {
    timings: timings::Timings,
    sent: AtomicU64,
    received: AtomicU64,
}

/// Observe protocol progress and successful TCP application bytes on this
/// member. Byte counts include frame headers, retransmissions and heartbeats.
#[derive(Clone)]
pub struct ReplicaObserver {
    status: watch::Receiver<NodeStatus>,
    traffic: Arc<Traffic>,
}

impl ReplicaObserver {
    /// Enable per-member measurements. The normal protocol does not read
    /// a clock on these paths until a consumer requests timings.
    pub fn enable_timings(&self) {
        self.traffic.timings.enable();
    }

    pub fn timings(&self) -> BTreeMap<&'static str, StageDistribution> {
        self.traffic.timings.snapshot()
    }

    /// Wait for the protocol to stop, for example after a disk error.
    pub async fn wait_stopped(&mut self) {
        let _ = self.status.wait_for(|s| s.stopped).await;
    }

    /// Waits until the role, view, readiness or stop state changes (the
    /// progress fields change without notice). False once the node is gone.
    pub async fn changed(&mut self) -> bool {
        self.status.changed().await.is_ok()
    }

    pub fn snapshot(&self) -> (NodeStatus, u64, u64) {
        (
            *self.status.borrow(),
            self.traffic.sent.load(Ordering::Relaxed),
            self.traffic.received.load(Ordering::Relaxed),
        )
    }
}

#[derive(Default)]
struct Slot {
    callback: Option<Box<dyn FnOnce(io::Result<Lsn>) + Send>>,
    result: Option<io::Result<Lsn>>,
    notify: Option<Notify>,
}

struct Proposal {
    queued: Option<Instant>,
    control: Option<leases::Request>,
    body: Payload,
    last_lsn: Lsn,
    epoch: u64,
    slot: Arc<Mutex<Slot>>,
}

enum Pending {
    /// A permit command or a segment-group flush batch.
    Slot {
        return_session: bool,
        slot: Arc<Mutex<Slot>>,
        last_lsn: Lsn,
        proposed: Option<Instant>,
    },
    /// A log batch: each shard's last LSN in it.
    Sections {
        shards: Vec<(Arc<log3::ShardSlot>, Lsn)>,
        /// When it was proposed (group commit reads it; always set).
        at: Instant,
        proposed: Option<Instant>,
        /// Written to the backups' sockets (timings only).
        sent: Option<Instant>,
    },
}

impl Barrier {
    fn refuse(self) {
        let e = || other("replica lost the primary role");
        match self {
            Self::Checkpoint { reply, .. } => {
                let _ = reply.send(Err(e()));
            }
            Self::Recovery { reply, .. } => {
                let _ = reply.send(Err(e()));
            }
        }
    }
}

impl Pending {
    fn fail(self, e: &str) {
        match self {
            Self::Slot { slot, .. } => finish(&slot, Err(other(e))),
            Self::Sections { shards, .. } => {
                for (slot, _) in shards {
                    slot.fail();
                }
            }
        }
    }
}

/// What a group's data entries are, decided by its first engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// No workflow data yet.
    Undecided,
    /// One shard, flush batches in log format 2, materialized into the
    /// engine's segment files (a segment group: dedicated servers, whose
    /// remote workers read those segments).
    Segments,
    /// Log-format-3 batches of an engine with this many shards.
    Log(u32),
}

impl Kind {
    fn encode(self) -> u64 {
        match self {
            Self::Undecided => 0,
            Self::Segments => 1,
            Self::Log(n) => 2 + u64::from(n),
        }
    }
    fn decode(v: u64) -> Self {
        match v {
            0 => Self::Undecided,
            1 => Self::Segments,
            n => Self::Log((n - 2) as u32),
        }
    }
}

/// The shard count an embedded engine of a new group uses.
fn default_shards() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get().min(8) as u32)
        .unwrap_or(1)
}

/// A shard's synchronous request to the I/O thread.
enum Barrier {
    /// A segment group's shard is about to drop engine segments: the applied hint
    /// must be durable first.
    Checkpoint {
        epoch: u64,
        reply: std::sync::mpsc::SyncSender<io::Result<()>>,
    },
    /// A log group's shard opens: the committed log batches it recovers
    /// from, as of now.
    Recovery {
        epoch: u64,
        reply: std::sync::mpsc::SyncSender<io::Result<Arc<log3::Recovery>>>,
    },
}

// Proposals waiting for the protocol thread. A shard has at most one flush
// in flight; the rest are permit commands. Producers wait for room.
const PROPOSAL_QUEUE: usize = 1024;
// One checkpoint barrier per shard is in flight at a time.
const BARRIER_QUEUE: usize = 16;

struct Shared {
    status: watch::Sender<NodeStatus>,
    proposals: queue::Sender<Proposal>,
    barriers: queue::Sender<Barrier>,
    checkpoints: mpsc::Sender<(u64, snapshot::Sources)>,
    /// Changes whenever this node stops (or starts) being a ready primary;
    /// storage handles from an older epoch refuse to write.
    epoch: AtomicU64,
    engine_dir: Mutex<PathBuf>,
    local: LocalOptions,
    next_ticket: AtomicU64,
    traffic: Arc<Traffic>,
    /// [`Kind::encode`] of the group, set when this replica becomes the
    /// ready primary and by the first engine configuration.
    kind: AtomicU64,
    /// Sections staged by shards of a log group.
    batcher: log3::Batcher,
    /// Wakes the I/O thread: sections staged, a log write finished.
    wake: Arc<tokio::sync::Notify>,
    /// Bumped when the I/O thread wants every shard to checkpoint, so it
    /// can build a checkpoint image and trim the protocol log.
    checkpoint_round: AtomicU64,
    /// One bit per shard: the next image waits for this shard's checkpoint.
    checkpoint_wanted: AtomicU64,
    /// While this replica is the ready primary, the time ([`now_ms`]) until
    /// which it holds a read lease; 0 otherwise. Cleared before anything of
    /// a view change leaves this node.
    lease_until: AtomicU64,
    /// The current membership and every known member's address (index:
    /// member ID; empty: unknown).
    members: std::sync::RwLock<(fusee_core::vsr::Membership, Vec<String>)>,
    /// Dialers started after a membership change (aborted on drop).
    dialers: Mutex<BTreeMap<ReplicaId, tokio::task::JoinHandle<()>>>,
    /// Membership change requests for the I/O thread.
    reconfigure: Mutex<Vec<Vec<(ReplicaId, String)>>>,
    /// The I/O thread's protocol state, for a lone shard that proposes its
    /// own section while the I/O thread waits (see [`Core::inline_round`]).
    core: std::sync::OnceLock<std::sync::Weak<Mutex<Core>>>,
    /// Standby shards wait here for this member to become the ready
    /// primary (notified whenever the role, readiness or stop state changes).
    role: (Mutex<()>, std::sync::Condvar),
    _writer: File,
}

impl Shared {
    fn role_changed(&self) {
        let _guard = self.role.0.lock().unwrap_or_else(|e| e.into_inner());
        self.role.1.notify_all();
    }

    fn kind(&self) -> Kind {
        Kind::decode(self.kind.load(Ordering::Acquire))
    }

    /// The group's kind, deciding it as `fallback` if no data says yet.
    fn decide(&self, fallback: Kind) -> Kind {
        match self.kind.compare_exchange(
            Kind::Undecided.encode(),
            fallback.encode(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => fallback,
            Err(v) => Kind::decode(v),
        }
    }
}

/// One replica of a shard group.
pub struct ReplicaNode {
    shared: Arc<Shared>,
    status: watch::Receiver<NodeStatus>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    stop: Option<oneshot::Sender<()>>,
    core_thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ReplicaNode {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        for t in &self.tasks {
            t.abort();
        }
        for t in self
            .shared
            .dialers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            t.abort();
        }
        if let Some(thread) = self.core_thread.take()
            && thread.thread().id() != std::thread::current().id()
        {
            let _ = thread.join();
        }
    }
}

/// `e` with what was being done and to which path, so a storage error that
/// stops a replica says where it came from.
pub(super) fn annotate(e: io::Error, what: &str, path: &Path) -> io::Error {
    io::Error::new(e.kind(), format!("{what} {}: {e}", path.display()))
}

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

// ---------------------------------------------------------------- disk

struct Superblock {
    view: u64,
    last_normal: u64,
    commit: Op,
    applied: Op,
}

fn read_superblock(path: &Path) -> io::Result<Superblock> {
    let empty = Superblock {
        view: 0,
        last_normal: 0,
        commit: 0,
        applied: 0,
    };
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let log = path.with_file_name("log");
            match log.metadata() {
                Ok(meta) if meta.len() > 0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "missing replica superblock for existing log",
                    ));
                }
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => return Ok(empty),
            }
        }
        Err(e) => return Err(e),
    };
    if bytes.len() != 36
        || crc32c::crc32c(&bytes[..32])
            != u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "damaged replica superblock",
        ));
    }
    let u = |i: usize| {
        let mut b = [0u8; 8];
        b.copy_from_slice(&bytes[i..i + 8]);
        u64::from_le_bytes(b)
    };
    if u(24) > u(16) || u(8) > u(0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "inconsistent replica superblock",
        ));
    }
    Ok(Superblock {
        view: u(0),
        last_normal: u(8),
        commit: u(16),
        applied: u(24),
    })
}

/// The commit hint file of a log directory: the commit position, known
/// committed in the group and durable in this log when written. Rewritten
/// in place in the background (12 bytes, checksummed: a torn write reads as
/// absent), so the I/O thread never syncs a superblock just to keep it.
/// Recovery takes the higher of it and the superblock's commit.
const COMMIT_HINT: &str = "commit";

fn read_commit_hint(dir: &Path) -> Op {
    match std::fs::read(dir.join(COMMIT_HINT)) {
        Ok(b) if b.len() >= 12 => {
            let commit = u64::from_le_bytes(b[..8].try_into().unwrap());
            let crc = u32::from_le_bytes(b[8..12].try_into().unwrap());
            if crc32c::crc32c(&b[..8]) == crc {
                commit
            } else {
                0
            }
        }
        _ => 0,
    }
}

/// The membership file, in `vsr/` (it outlives log generations): the
/// committed reconfiguration entry that began the epoch and the members
/// before it, checksummed. Absent for a group that never changed.
const MEMBERS: &str = "members";

fn read_membership(vsr: &Path) -> io::Result<Option<fusee_core::vsr::Membership>> {
    let bytes = match std::fs::read(vsr.join(MEMBERS)) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let bad = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "damaged replica membership file",
        )
    };
    if bytes.len() < 4 {
        return Err(bad());
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err(bad());
    }
    let mut r = Reader::new(body);
    let entry = decode_entry(&mut r).map_err(|_| bad())?;
    let previous = r.bytes().map_err(|_| bad())?.to_vec();
    let mut m = fusee_core::vsr::Membership::from_entry(&entry).ok_or_else(bad)?;
    m.previous = previous;
    Ok(Some(m))
}

fn write_membership(
    vsr: &Path,
    m: &fusee_core::vsr::Membership,
    method: FlushMethod,
) -> io::Result<()> {
    let Some(entry) = &m.entry else {
        return Ok(());
    };
    let mut b = Vec::new();
    encode_entry(entry, &mut b);
    fusee_core::wire::put_bytes(&mut b, &m.previous);
    let crc = crc32c::crc32c(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    let tmp = vsr.join("members.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&b)?;
        flush_file(&f, method)?;
    }
    std::fs::rename(&tmp, vsr.join(MEMBERS))?;
    sync_dir(vsr, method)
}

/// Parses a member list: `host:port,...` (IDs are positions) or
/// `id=host:port,...`. Returns addresses indexed by ID (empty: no member).
pub fn parse_members(list: &str) -> io::Result<Vec<String>> {
    let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, m.to_string());
    let items: Vec<&str> = list
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let mut out = vec![String::new(); fusee_core::vsr::MAX_MEMBERS];
    let mut max = 0;
    for (i, item) in items.iter().enumerate() {
        let (id, addr) = match item.split_once('=') {
            Some((id, addr)) => (
                id.trim()
                    .parse::<usize>()
                    .map_err(|_| bad("a member ID must be a number"))?,
                addr.trim(),
            ),
            None => (i, *item),
        };
        if id >= fusee_core::vsr::MAX_MEMBERS || addr.is_empty() || !out[id].is_empty() {
            return Err(bad(
                "member IDs must be distinct, below 32, each with an address",
            ));
        }
        out[id] = addr.to_string();
        max = max.max(id + 1);
    }
    out.truncate(max);
    Ok(out)
}

fn encode_commit_hint(commit: Op) -> Vec<u8> {
    let mut b = commit.to_le_bytes().to_vec();
    b.extend_from_slice(&crc32c::crc32c(&b).to_le_bytes());
    b
}

struct VsrLog {
    dir: PathBuf,
    file: Arc<File>,
    method: FlushMethod,
    dirty: bool,
    end: u64,
    capacity: u64,
    /// `LocalOptions::direct_io`: write through `O_DIRECT` where the
    /// filesystem allows it (Linux).
    want_direct: bool,
    /// The `O_DIRECT` descriptor of `log`.
    direct: Option<Arc<File>>,
    /// Set for good once the filesystem refused an `O_DIRECT` write.
    refused: Arc<std::sync::atomic::AtomicBool>,
    /// Every write covers whole 4 KiB blocks (with `direct`; tests also set
    /// it alone): it starts at the block `end` is in and rewrites `tail`.
    aligned: bool,
    /// The bytes of the last partial block, from `end` rounded down to a
    /// block to `end`. Only the previous write put them there, and it has
    /// completed before the next write shares its block
    /// ([`writes::Writes`]), so rewriting them is exact.
    tail: Vec<u8>,
    /// Tests: the filesystem refuses every `O_DIRECT` write.
    #[cfg(test)]
    reject_direct: bool,
}

const BLOCK: usize = fusee_core::wal::BLOCK as usize;

// Overwrite prepared extents on the normal commit path. Extending i_size
// for every entry otherwise makes fdatasync commit filesystem metadata too.
const VSR_EXTENT: u64 = 1 << 20;

fn reserve_vsr(file: &File, capacity: u64, needed: u64) -> io::Result<u64> {
    if needed <= capacity {
        return Ok(capacity);
    }
    let target = needed
        .checked_next_multiple_of(VSR_EXTENT)
        .ok_or_else(|| other("replica log capacity overflow"))?;
    let zeros = [0u8; 64 << 10];
    let mut pos = capacity;
    while pos < target {
        let size = (target - pos).min(zeros.len() as u64) as usize;
        file.write_all_at(&zeros[..size], pos)?;
        pos += size as u64;
    }
    Ok(target)
}

/// Zero-fills `from..to` (block-aligned) through an `O_DIRECT` descriptor.
fn zero_direct(file: &File, from: u64, to: u64) -> io::Result<()> {
    const CHUNK: usize = 64 << 10;
    let zeros = fusee::local::AlignedBuf::new(CHUNK);
    let mut pos = from;
    while pos < to {
        let size = (to - pos).min(CHUNK as u64) as usize;
        file.write_all_at(&zeros.as_slice()[..size], pos)?;
        pos += size as u64;
    }
    Ok(())
}

/// Drops a file's clean cached pages, so `O_DIRECT` writes over bytes that
/// were read or written buffered do not first invalidate them.
#[cfg(target_os = "linux")]
fn drop_cached(file: &File) {
    use std::os::fd::AsRawFd;
    // SAFETY: an advisory call on a descriptor `file` owns; no pointers.
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
}

#[cfg(not(target_os = "linux"))]
fn drop_cached(_file: &File) {}

impl VsrLog {
    fn empty(dir: &Path, method: FlushMethod) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(dir.join("log"))?;
        Ok(Self::with_file(dir, file, method, 0, 0))
    }

    fn with_file(dir: &Path, file: File, method: FlushMethod, end: u64, capacity: u64) -> Self {
        Self {
            dir: dir.to_owned(),
            file: Arc::new(file),
            method,
            dirty: false,
            end,
            capacity,
            want_direct: false,
            direct: None,
            refused: Arc::default(),
            aligned: false,
            tail: Vec::new(),
            #[cfg(test)]
            reject_direct: false,
        }
    }

    /// Writes go through `O_DIRECT` from now on if `want` (Linux, where the
    /// filesystem accepts it; else buffered, with one line on stderr).
    fn use_direct(&mut self, want: bool) -> io::Result<()> {
        self.want_direct = want;
        self.direct = None;
        self.aligned = false;
        self.tail.clear();
        if !want || !cfg!(target_os = "linux") || self.refused.load(Ordering::Acquire) {
            return Ok(());
        }
        let opened =
            fusee::local::with_direct(OpenOptions::new().write(true)).open(self.dir.join("log"));
        match opened {
            Ok(f) => self.direct = Some(Arc::new(f)),
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                if !self.refused.swap(true, Ordering::AcqRel) {
                    fusee::local::note_direct_refused(&self.dir);
                }
                return Ok(());
            }
            Err(e) => return Err(e),
        }
        self.align()
    }

    /// Every write from now on covers whole blocks: keeps the last partial
    /// block, read back from the file.
    fn align(&mut self) -> io::Result<()> {
        let start = self.end - self.end % BLOCK as u64;
        let mut tail = vec![0; (self.end - start) as usize];
        if !tail.is_empty() {
            File::open(self.dir.join("log"))?.read_exact_at(&mut tail, start)?;
        }
        self.tail = tail;
        self.aligned = true;
        drop_cached(&self.file);
        Ok(())
    }

    /// Where background writes go.
    fn target(&self) -> writes::LogFile {
        writes::LogFile {
            file: self.file.clone(),
            direct: self.direct.clone(),
            method: self.method,
            refused: self.refused.clone(),
            dir: self.dir.clone(),
            #[cfg(test)]
            reject_direct: self.reject_direct,
        }
    }

    /// Prepares the log through `needed`: zero-filled whole extents, so
    /// writes on the commit path overwrite allocated blocks and change no
    /// metadata. Through `O_DIRECT` the zeros are flushed at once (no flush
    /// follows a write that carries `RWF_DSYNC`).
    fn reserve(&mut self, needed: u64) -> io::Result<()> {
        if needed <= self.capacity {
            return Ok(());
        }
        if let Some(d) = self
            .direct
            .clone()
            .filter(|_| !self.refused.load(Ordering::Acquire))
        {
            let target = needed
                .checked_next_multiple_of(VSR_EXTENT)
                .ok_or_else(|| other("replica log capacity overflow"))?;
            // Past the end of the file, the rest of its last block reads as
            // zeros; the next aligned write rewrites that block whole.
            let from = self.capacity.next_multiple_of(BLOCK as u64);
            match zero_direct(&d, from, target) {
                Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                    if !self.refused.swap(true, Ordering::AcqRel) {
                        fusee::local::note_direct_refused(&self.dir);
                    }
                }
                Err(e) => return Err(e),
                Ok(()) => {
                    flush_file(&d, self.method)?;
                    self.capacity = target;
                    return Ok(());
                }
            }
        }
        self.capacity = reserve_vsr(&self.file, self.capacity, needed)?;
        Ok(())
    }
    fn frame(e: &Entry, out: &mut Vec<u8>) {
        let mut body = Vec::with_capacity(e.body.len() + 32);
        encode_entry(e, &mut body);
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
        out.extend_from_slice(&body);
    }

    /// Opens the log and reads it back: entries, and a damaged range if a
    /// record in the middle fails its checksum while later ones are intact.
    fn open(
        dir: &Path,
        method: FlushMethod,
        committed: Op,
        base: Op,
        base_checksum: u64,
    ) -> io::Result<(Self, Vec<Entry>, Option<(Op, Op)>)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("log");
        let mut bytes = Vec::new();
        match File::open(&path) {
            Ok(mut f) => {
                f.read_to_end(&mut bytes)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let mut entries = Vec::new();
        let mut pos = 0usize;
        let mut damaged = None;
        let mut valid_end = 0usize;
        while pos + 8 <= bytes.len() {
            let len =
                u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                    as usize;
            let crc = u32::from_le_bytes([
                bytes[pos + 4],
                bytes[pos + 5],
                bytes[pos + 6],
                bytes[pos + 7],
            ]);
            let body = bytes.get(pos + 8..pos + 8 + len);
            let ok = body.is_some_and(|b| crc32c::crc32c(b) == crc);
            if let (true, Some(b)) = (ok, body)
                && let Ok(e) = decode_entry(&mut Reader::new(b))
            {
                let (prior, hash) = entries
                    .last()
                    .map_or((base, base_checksum), |p: &Entry| (p.op, p.checksum));
                if e.op <= prior || e.op == Op::MAX {
                    return Err(other("out-of-order replica log entry"));
                }
                let expected = prior + 1;
                if e.op > expected {
                    damaged = Some((damaged.map_or(expected, |(first, _)| first), e.op - 1));
                } else if e.parent != hash {
                    // A checksummed frame from a different chain does not
                    // authenticate a prepared position. Keep the raw file
                    // until peer repair supplies the correct bytes.
                    damaged = Some((damaged.map_or(expected, |(first, _)| first), e.op));
                    pos += 8 + len;
                    valid_end = pos;
                    continue;
                }
                entries.push(e);
                pos += 8 + len;
                valid_end = pos;
                continue;
            }
            if len == 0 && bytes[pos..].iter().all(|b| *b == 0) {
                break; // the unused, zero-filled suffix
            }
            let block = pos.next_multiple_of(BLOCK);
            if block > pos && bytes[pos..block.min(bytes.len())].iter().all(|b| *b == 0) {
                // A write placed while another was in flight is padded
                // with zeros to the end of its block. Padding shorter than
                // a length field reads, with the next frame's first bytes,
                // as a nonzero length: no valid frame starts here (it would
                // have been taken above), so the zeros are padding.
                pos = block;
                continue;
            }
            // Invalid frame: look for a later valid one (damage) or stop (torn).
            let expected = entries.last().map(|e: &Entry| e.op + 1).unwrap_or(base + 1);
            let mut later = None;
            let mut p = pos + 1;
            while p + 8 <= bytes.len() {
                let l = u32::from_le_bytes([bytes[p], bytes[p + 1], bytes[p + 2], bytes[p + 3]])
                    as usize;
                let c =
                    u32::from_le_bytes([bytes[p + 4], bytes[p + 5], bytes[p + 6], bytes[p + 7]]);
                if let Some(b) = bytes.get(p + 8..p + 8 + l)
                    && l > 0
                    && crc32c::crc32c(b) == c
                    && let Ok(e) = decode_entry(&mut Reader::new(b))
                    && e.op > expected
                {
                    later = Some((p, e.op));
                    break;
                }
                p += 1;
            }
            match later {
                Some((p, op)) => {
                    damaged = Some((damaged.map_or(expected, |(start, _)| start), op - 1));
                    pos = p;
                }
                None => break,
            }
        }
        let last = entries.last().map_or(base, |e| e.op);
        if last < committed {
            // The superblock survived the flush for these entries. Keep the
            // evidence until repair installs a peer's committed contents.
            damaged = Some((damaged.map_or(last + 1, |(start, _)| start), committed));
        }
        if damaged.is_none() && bytes[valid_end..].iter().any(|b| *b != 0) {
            // A torn tail: never acknowledged.
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(valid_end as u64)?;
            flush_file(&f, method)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        let capacity = file.metadata()?.len();
        Ok((
            Self::with_file(dir, file, method, valid_end as u64, capacity),
            entries,
            damaged,
        ))
    }

    /// The entries after `after` (op, checksum) in the log file at `path`,
    /// read from its first `end` bytes: the chain from `after` as far as it
    /// is intact and continuous. Runs off the I/O thread (a staged
    /// generation's tail), so it reads what the file holds rather than
    /// cloning the replica's entries: a frame still being written, a torn
    /// one or a gap ends the tail, and the caller takes everything after
    /// it from memory.
    fn read_tail(path: &Path, after: (Op, u64), end: u64) -> io::Result<Vec<Entry>> {
        Self::read_tail_from(path, after, end, 8 << 20)
    }

    /// [`read_tail`](Self::read_tail), first reading the last `window` bytes.
    fn read_tail_from(
        path: &Path,
        after: (Op, u64),
        end: u64,
        mut window: u64,
    ) -> io::Result<Vec<Entry>> {
        // The tail is the end of the file: read a window there, widening it
        // until it starts at or before the entry after `after` (a log of a
        // gigabyte took the staging thread most of a second to read whole).
        let file = File::open(path)?;
        loop {
            let start = end.saturating_sub(window);
            let mut bytes = vec![0; (end - start) as usize];
            file.read_exact_at(&mut bytes, start)?;
            // Mid-file, the window starts inside some frame: resume at the
            // first checksummed one.
            let first = if start == 0 {
                Some(0)
            } else {
                Self::next_frame(&bytes, 0)
            };
            let (tail, began_after) = match first {
                Some(pos) => Self::scan_tail(&bytes, start, pos, after),
                None => (Vec::new(), false),
            };
            if start == 0 || began_after || !tail.is_empty() {
                return Ok(tail);
            }
            window = window.saturating_mul(4);
        }
    }

    /// The first position at or after `from` where a whole frame with a
    /// valid checksum and entry starts.
    fn next_frame(bytes: &[u8], from: usize) -> Option<usize> {
        let mut p = from;
        while p + 8 <= bytes.len() {
            let len = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            if len > 0 && p + 8 + len <= bytes.len() {
                let crc = u32::from_le_bytes(bytes[p + 4..p + 8].try_into().unwrap());
                let body = &bytes[p + 8..p + 8 + len];
                if crc32c::crc32c(body) == crc && decode_entry(&mut Reader::new(body)).is_ok() {
                    return Some(p);
                }
            }
            p += 1;
        }
        None
    }

    /// The chain after `after` in `bytes` (the file from offset `base`)
    /// from `pos`, and whether the scan met an entry at or before `after`
    /// (so the window held the tail's start).
    fn scan_tail(bytes: &[u8], base: u64, mut pos: usize, after: (Op, u64)) -> (Vec<Entry>, bool) {
        let (mut next, mut parent) = (after.0.saturating_add(1), after.1);
        let mut tail = Vec::new();
        let mut began = false;
        while pos + 8 <= bytes.len() {
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
            let body = bytes
                .get(pos + 8..pos + 8 + len)
                .filter(|b| len > 0 && crc32c::crc32c(b) == crc);
            let Some(body) = body else {
                // Zero padding to the end of a block of the file (a write
                // placed beside another), or the end of what is written.
                let at = base + pos as u64;
                let block = (at.next_multiple_of(BLOCK as u64) - base) as usize;
                if block > pos && bytes[pos..block.min(bytes.len())].iter().all(|b| *b == 0) {
                    pos = block;
                    continue;
                }
                break;
            };
            let e = match decode_entry(&mut Reader::new(body)) {
                Ok(e) => e,
                Err(_) => break,
            };
            pos += 8 + len;
            if e.op < next {
                began = true;
                if e.op == after.0 && e.checksum != after.1 {
                    // Not the chain the caller holds.
                    return (Vec::new(), true);
                }
                continue;
            }
            if e.op != next || e.parent != parent {
                break;
            }
            began = true;
            parent = e.checksum;
            next += 1;
            tail.push(e);
        }
        (tail, began)
    }

    /// Appends entries at the end on this thread (a staged generation's
    /// catch-up; the replica's own appends go through `writes`).
    fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        let mut out = Vec::new();
        for e in entries {
            Self::frame(e, &mut out);
        }
        self.write_frames(out)
    }

    /// Writes framed entries at the end on this thread: durable when it
    /// returns with `O_DIRECT` and `RWF_DSYNC`, else at the next sync.
    fn write_frames(&mut self, frames: Vec<u8>) -> io::Result<()> {
        let p = self.place(frames, false)?;
        if !self.target().write(p.bytes.as_slice(), p.offset)? {
            self.dirty = true;
        }
        Ok(())
    }

    /// Positions bytes at the end of the log, in prepared extents. `pad`:
    /// another write may be in flight with this one, so the bytes are
    /// padded to the end of their last block, as the shared log pads a
    /// pipelined batch ([`fusee_core::wal::batch_span`]). Aligned writes
    /// start at the block the end is in, with the kept tail before the
    /// bytes, and run to the end of their last block.
    fn place(&mut self, mut bytes: Vec<u8>, pad: bool) -> io::Result<writes::Placement> {
        if self.direct.is_some() && self.refused.load(Ordering::Acquire) {
            // The filesystem refused an O_DIRECT write: buffered from now on.
            self.direct = None;
            self.aligned = false;
            self.tail.clear();
        }
        let offset = self.end;
        let len = bytes.len();
        let span = if pad {
            fusee_core::wal::batch_span(offset, len, true)
        } else {
            len as u64
        };
        let next = offset
            .checked_add(span)
            .ok_or_else(|| other("replica log length overflow"))?;
        let padded = next.is_multiple_of(BLOCK as u64);
        if !self.aligned {
            bytes.resize(span as usize, 0);
            self.reserve(next)?;
            self.end = next;
            return Ok(writes::Placement {
                offset,
                bytes: writes::WriteBuf::Plain(bytes),
                padded,
            });
        }
        let start = offset - self.tail.len() as u64;
        let total = self.tail.len() + span as usize;
        let whole = total.next_multiple_of(BLOCK);
        self.reserve(start + whole as u64)?;
        self.end = next;
        let mut buf = fusee::local::AlignedBuf::new(whole);
        let s = buf.as_mut_slice();
        s[..self.tail.len()].copy_from_slice(&self.tail);
        s[self.tail.len()..self.tail.len() + len].copy_from_slice(&bytes);
        let keep = (next % BLOCK as u64) as usize;
        self.tail = s[total - keep..total].to_vec();
        Ok(writes::Placement {
            offset: start,
            bytes: writes::WriteBuf::Aligned { buf, len: whole },
            padded,
        })
    }

    fn sync(&mut self) -> io::Result<()> {
        if self.dirty {
            flush_file(&self.file, self.method)?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Atomically replaces the whole log.
    fn replace<'a>(&mut self, entries: impl Iterator<Item = &'a Entry>) -> io::Result<()> {
        let tmp = self.dir.join("log.tmp");
        let mut end = 0u64;
        let capacity = {
            let f = File::create(&tmp)?;
            let mut frame = Vec::new();
            {
                let mut stream = BufWriter::with_capacity(1 << 20, &f);
                for entry in entries {
                    frame.clear();
                    Self::frame(entry, &mut frame);
                    stream.write_all(&frame)?;
                    end = end
                        .checked_add(frame.len() as u64)
                        .ok_or_else(|| other("replica log length overflow"))?;
                }
                stream.flush()?;
            }
            let capacity = reserve_vsr(
                &f,
                end,
                end.max(1)
                    .checked_next_multiple_of(VSR_EXTENT)
                    .ok_or_else(|| other("replica log capacity overflow"))?,
            )?;
            flush_file(&f, self.method)?;
            capacity
        };
        std::fs::rename(&tmp, self.dir.join("log"))?;
        sync_dir(&self.dir, self.method)?;
        self.file = Arc::new(OpenOptions::new().write(true).open(self.dir.join("log"))?);
        self.end = end;
        self.capacity = capacity;
        self.dirty = false;
        let aligned = self.aligned && self.direct.is_none();
        self.use_direct(self.want_direct)?;
        if aligned {
            self.align()?;
        }
        Ok(())
    }

    fn save(&mut self, sb: &Superblock) -> io::Result<()> {
        // Barrier: the log this superblock vouches for is durable first.
        self.sync()?;
        let mut b = Vec::with_capacity(36);
        put_u64(&mut b, sb.view);
        put_u64(&mut b, sb.last_normal);
        put_u64(&mut b, sb.commit);
        put_u64(&mut b, sb.applied);
        let crc = crc32c::crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        let tmp = self.dir.join("superblock.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&b)?;
            flush_file(&f, self.method)?;
        }
        std::fs::rename(&tmp, self.dir.join("superblock"))?;
        sync_dir(&self.dir, self.method)
    }
}

// ---------------------------------------------------------------- node

/// The protocol's clock, in milliseconds since this process first read it.
/// Election and heartbeat intervals must survive wall-clock corrections, so
/// it is monotonic; read leases assume its rate is within
/// [`fusee_core::vsr::MAX_CLOCK_DRIFT_PPM`] of real time, so it also counts
/// time the host spends suspended: it advances with the monotonic clock,
/// plus however far, at most, `CLOCK_BOOTTIME` (Linux; it counts
/// suspension, as macOS's `CLOCK_MONOTONIC` already does) has run ahead of
/// it. A suspension is counted, and a boot clock that some tool moves back
/// (libfaketime fakes it) neither moves this clock back nor stops it.
fn now_ms() -> u64 {
    static START: std::sync::OnceLock<(u64, u64)> = std::sync::OnceLock::new();
    static AHEAD: AtomicU64 = AtomicU64::new(0);
    static LAST: AtomicU64 = AtomicU64::new(0);
    let (mono, boot) = (clock_ms(libc::CLOCK_MONOTONIC), boot_ms());
    let start = *START.get_or_init(|| (mono.unwrap_or(0), boot.unwrap_or(0)));
    let elapsed = mono.map_or(0, |m| m.saturating_sub(start.0));
    let ahead = boot.map_or(0, |b| b.saturating_sub(start.1).saturating_sub(elapsed));
    let now = elapsed + AHEAD.fetch_max(ahead, Ordering::Relaxed).max(ahead);
    // Never back, even if a read fails.
    LAST.fetch_max(now, Ordering::Relaxed).max(now)
}

#[cfg(target_os = "linux")]
fn boot_ms() -> Option<u64> {
    clock_ms(libc::CLOCK_BOOTTIME)
}

#[cfg(not(target_os = "linux"))]
fn boot_ms() -> Option<u64> {
    None
}

fn clock_ms(clock: libc::clockid_t) -> Option<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid timespec for the call to fill in.
    (unsafe { libc::clock_gettime(clock, &mut ts) } == 0)
        .then(|| ts.tv_sec as u64 * 1000 + ts.tv_nsec as u64 / 1_000_000)
}

impl ReplicaNode {
    /// Starts the replica: recovers its protocol log, re-applies committed
    /// batches to its engine directory, and joins the group.
    pub async fn start(cfg: ReplicaConfig) -> io::Result<Self> {
        use fusee_core::vsr::{MAX_GROUP, MAX_MEMBERS, Membership};
        let invalid = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, m.to_string());
        let id = usize::from(cfg.id);
        if id >= MAX_MEMBERS
            || cfg.peers.get(id).is_none_or(String::is_empty)
            || cfg.peers.len() > MAX_MEMBERS
        {
            return Err(invalid(
                "this replica's ID must be below 32 and have an address among the peers",
            ));
        }
        cfg.timing.validate().map_err(invalid)?;
        let vsr_dir = cfg.dir.join("vsr");
        let saved = read_membership(&vsr_dir)?;
        let named = cfg.peers.iter().filter(|p| !p.is_empty()).count();
        if saved.is_none() && !cfg.join && !(1..=MAX_GROUP).contains(&named) {
            return Err(invalid("a group has 1 to 9 replicas"));
        }
        if saved.is_none() && !cfg.join && cfg.peers.iter().take(named).any(String::is_empty) {
            return Err(invalid(
                "a new group's members have IDs 0, 1, 2, ...; give every one an address",
            ));
        }
        if saved.is_none()
            && cfg.join
            && std::fs::metadata(vsr_dir.join("log")).is_ok_and(|m| m.len() > 0)
        {
            return Err(invalid(
                "a joining replica starts with an empty directory; this one holds a replica log",
            ));
        }
        // Addresses by member ID: the configured ones, then the saved
        // membership's (which win once the membership has changed).
        let mut addresses = cfg.peers.clone();
        addresses.resize(MAX_MEMBERS, String::new());
        if let Some(m) = &saved {
            for (member, addr) in m.addresses() {
                addresses[usize::from(member)] = addr;
            }
        }
        let joining = saved.is_none() && cfg.join;
        let membership = match &saved {
            Some(m) => m.clone(),
            None if joining => Membership {
                epoch: 0,
                members: (0..MAX_MEMBERS as u8)
                    .filter(|m| !addresses[usize::from(*m)].is_empty())
                    .collect(),
                entry: None,
                previous: Vec::new(),
            },
            None => Membership::initial(named as u8),
        };
        let n = membership.members.len() as u8;
        let acceptor = Arc::new(Acceptor::new(&cfg.security, Purpose::Replica)?);
        let connector = Arc::new(Connector::new(&cfg.security, Purpose::Replica, cfg.id)?);
        let writer_lock = fusee::local::lock_directory(&cfg.dir.join("engine"))?;
        std::fs::create_dir_all(&vsr_dir)?;
        sync_dir(&cfg.dir, cfg.local.flush)?;
        let current = generation::Generation::read(&vsr_dir)?;
        let engine_dir = cfg
            .dir
            .join(current.as_ref().map_or("engine", |g| &g.engine));
        let log_dir = current
            .as_ref()
            .map_or_else(|| vsr_dir.clone(), |g| g.log_dir(&vsr_dir));
        let (base, base_checksum) = current
            .as_ref()
            .map_or((0, 0), |g| (g.image.op, g.image.checksum));
        let shard_dir = engine_dir.join("shard-000");
        std::fs::create_dir_all(&shard_dir)?;
        let mut local = cfg.local.clone();
        // Two writers touch the segment files (the applier and the engine's
        // recovery); neither may zero-fill over the other's data.
        local.preallocate = Preallocate::None;
        local.direct_io = false;
        let mut sb = read_superblock(&log_dir.join("superblock"))?;
        sb.commit = sb.commit.max(read_commit_hint(&log_dir));
        if sb.applied < base || sb.commit < base {
            return Err(other("replica superblock precedes checkpoint"));
        }
        let (mut log, entries, damaged) =
            VsrLog::open(&log_dir, local.flush, sb.commit, base, base_checksum)?;
        // The protocol log is written only by this I/O thread and its
        // helpers, so it takes `direct_io` as configured (the engine's own
        // files above do not).
        log.use_direct(cfg.local.direct_io)?;
        let mut quarantine = false;
        let ledger_bytes = if let Some(g) = &current {
            generation::retire_prefix_log(&vsr_dir, local.flush)?;
            let restore = || -> io::Result<Vec<u8>> {
                let ledger = g.ledger(&vsr_dir)?;
                snapshot::rebuild(
                    &vsr_dir.join(g.image.name()),
                    &g.image,
                    &engine_dir,
                    &ledger,
                    local.flush,
                )?;
                Ok(ledger)
            };
            match restore() {
                Ok(ledger) => ledger,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::NotFound
                            | io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::InvalidData
                            | io::ErrorKind::Other
                    ) =>
                {
                    // The saved view and committed watermark remain authoritative.
                    // Refuse to vote or apply anything until an eligible donor
                    // supplies a verified replacement covering this checkpoint.
                    eprintln!(
                        "Fusee replica {} is fetching its damaged checkpoint: {e}",
                        cfg.id
                    );
                    quarantine = true;
                    leases::Ledger::default().snapshot()?
                }
                Err(e) => return Err(e),
            }
        } else {
            leases::Ledger::default().snapshot()?
        };
        let base_ledger = leases::Ledger::restore(&ledger_bytes)?;
        let (base, base_checksum, damaged) = if quarantine {
            (0, 0, Some((1, base)))
        } else {
            (base, base_checksum, damaged)
        };
        let mut writer = BatchWriter::new(&shard_dir.join("log"), local.clone())?;
        // Re-apply committed batches the engine directory may have lost.
        let commit = sb.commit;
        let mut applied = if current.is_some() { base } else { sb.applied };
        {
            let from = applied;
            let through = damaged.map_or(commit, |(first, _)| commit.min(first.saturating_sub(1)));
            for e in entries.iter().filter(|e| e.op > from && e.op <= through) {
                if is_segment_batch(&e.body) {
                    let batch = proto::decode_batch(&mut Reader::new(&e.body)).map_err(other)?;
                    writer.write(&batch)?;
                }
                applied = e.op;
            }
        }
        let rec = Recovered {
            view: sb.view,
            last_normal: sb.last_normal,
            commit,
            entries,
            base_checksum,
            base,
            damaged,
            membership: Some(membership.clone()),
            joining,
        };
        let seed = fusee::local::unix_ms() ^ (u64::from(cfg.id) << 48);
        let replica = Replica::new(cfg.id, n, rec, cfg.timing.clone(), seed, now_ms());
        if !quarantine {
            log.save(&Superblock {
                view: replica.view(),
                last_normal: sb.last_normal,
                commit: replica.commit(),
                applied,
            })?;
        }
        if damaged.is_none() && log.capacity == 0 {
            // First install the empty superblock, so a crash during preparation
            // cannot leave an ambiguous nonempty log without its superblock.
            log.reserve(1)?;
            flush_file(&log.file, log.method)?;
        }
        let (status_tx, status_rx) = watch::channel(NodeStatus::default());
        let (ptx, prx) = queue::bounded(PROPOSAL_QUEUE);
        let (btx, brx) = queue::bounded(BARRIER_QUEUE);
        let (checkpoints, captures) = mpsc::channel(64);
        let wake = Arc::new(tokio::sync::Notify::new());
        let shared = Arc::new(Shared {
            status: status_tx,
            proposals: ptx,
            barriers: btx,
            checkpoints,
            epoch: AtomicU64::new(1),
            engine_dir: Mutex::new(engine_dir.clone()),
            local: local.clone(),
            next_ticket: AtomicU64::new(1),
            traffic: Arc::new(Traffic::default()),
            kind: AtomicU64::new(Kind::Undecided.encode()),
            batcher: log3::Batcher::default(),
            wake: wake.clone(),
            checkpoint_round: AtomicU64::new(0),
            checkpoint_wanted: AtomicU64::new(0),
            lease_until: AtomicU64::new(0),
            members: std::sync::RwLock::new((membership.clone(), addresses.clone())),
            dialers: Mutex::new(BTreeMap::new()),
            reconfigure: Mutex::new(Vec::new()),
            core: std::sync::OnceLock::new(),
            role: (Mutex::new(()), std::sync::Condvar::new()),
            _writer: writer_lock,
        });
        let log_io = writes::LogIo::start(wake.clone())?;
        // The I/O thread: one current-thread runtime that reads every peer
        // socket, runs the protocol and owns the log.
        let protocol_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?;
        let tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let listener = tokio::net::TcpListener::bind(&addresses[usize::from(cfg.id)]).await?;
        let listener = {
            let _owner = protocol_runtime.enter();
            tokio::net::TcpListener::from_std(listener.into_std()?)?
        };
        if let Ok(addr) = listener.local_addr() {
            crate::security::warn_if_exposed(
                "replica",
                addr,
                acceptor.has_tls(),
                acceptor.has_token(),
            );
        }
        let (admitted_tx, admitted) = mpsc::unbounded_channel();
        let inbox = Inbox {
            tx: admitted_tx,
            acceptor,
            shared: shared.clone(),
            me: cfg.id,
            handshakes: tokio::runtime::Handle::current(),
        };
        let dialing = Dialing {
            handle: tokio::runtime::Handle::current(),
            connector,
            me: cfg.id,
        };
        let mut senders = BTreeMap::new();
        dialing.dial(&shared, &mut senders);
        let core = Core {
            id: cfg.id,
            replica,
            log,
            writes: writes::Writes::new(log_io, local.emulated_flush),
            writer,
            applied,
            staged_since_hint: false,
            senders,
            outbox: vec![Vec::new(); MAX_MEMBERS],
            prepare_cache: None,
            queued: Vec::new(),
            pending: BTreeMap::new(),
            leases: base_ledger,
            lease_applied: base,
            checkpoints: transfer::Checkpoints::new(
                vsr_dir.clone(),
                current,
                ledger_bytes,
                quarantine,
                n,
            ),
            session: None,
            shared: shared.clone(),
            ready: false,
            last_hint: now_ms(),
            commit_hint: (0, None),
            recovery: None,
            released: false,
            round_ns: 100_000,
            last_entry_at: None,
            halted: None,
            dialing,
            vsr_dir: vsr_dir.clone(),
            read_at: None,
            unsent: Vec::new(),
            durable_at: None,
            ack_queued: false,
        };
        let core: CoreCell = Arc::new(Mutex::new(core));
        let _ = shared.core.set(Arc::downgrade(&core));
        let (stop, stopped) = oneshot::channel();
        let thread = match std::thread::Builder::new()
            .name(format!("fusee-replica-{}", cfg.id))
            .spawn(move || {
                queue::mark_consumer();
                let local = tokio::task::LocalSet::new();
                local.block_on(&protocol_runtime, async move {
                    tokio::task::spawn_local(listen(listener, inbox));
                    tokio::task::spawn_local(read_admitted(admitted, core.clone()));
                    drive(core, prx, brx, captures, stopped).await;
                })
            }) {
            Ok(thread) => thread,
            Err(e) => {
                for task in tasks {
                    task.abort();
                }
                return Err(e);
            }
        };
        Ok(Self {
            shared,
            status: status_rx,
            tasks,
            stop: Some(stop),
            core_thread: Some(thread),
        })
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<NodeStatus> {
        self.status.clone()
    }

    pub fn observer(&self) -> ReplicaObserver {
        ReplicaObserver {
            status: self.status.clone(),
            traffic: self.shared.traffic.clone(),
        }
    }

    pub fn status(&self) -> NodeStatus {
        *self.status.borrow()
    }

    /// Resolves once this replica is primary with everything committed and
    /// applied, so an engine opened now sees the whole committed log.
    pub async fn wait_ready(&mut self) {
        let _ = self.status.wait_for(|s| s.ready || s.stopped).await;
    }

    /// Resolves once this replica is no longer a ready primary.
    pub async fn wait_lost(&mut self) {
        let _ = self.status.wait_for(|s| !s.ready).await;
    }

    /// The engine configuration for this replica while it is primary. Call
    /// it after [`wait_ready`](Self::wait_ready): its storage refuses writes
    /// once this replica stops being the ready primary.
    ///
    /// The engine has the group's shard count: the one its log was written
    /// with, or for a new group one per core up to eight. Keep it: records
    /// of a shard the engine does not open would never be replayed.
    /// A hot-standby engine configuration: an engine opened with it on a
    /// backup (or at any time) opens at once with every shard standing by
    /// (threads, runtimes, open run stores, registered workflows), and its
    /// shards start the moment this replica becomes the ready primary: each
    /// recovers from the committed log batches the I/O thread holds in
    /// memory and the files it already has open, with no engine to open.
    /// Calls wait meanwhile. Like an engine from
    /// [`engine_config`](Self::engine_config) it stops when this replica
    /// loses the primary role: shut it down and open another standby
    /// engine. Log groups only (a segment group's shard refuses to stand by).
    ///
    /// ```text
    /// loop {
    ///     let engine = Engine::open(node.standby_config()).await?; // at once
    ///     register_workflows(&engine);
    ///     node.wait_ready().await;   // promoted: the shards are running
    ///     node.wait_lost().await;
    ///     let _ = engine.shutdown().await;
    /// }
    /// ```
    pub fn standby_config(&self) -> Config {
        let storage = ReplicaStorage::new(self);
        let shards = match self.shared.kind() {
            Kind::Log(n) => n,
            _ => default_shards(),
        };
        // The primary epoch the engine's shards are promoted in: one, for
        // every shard and the permit backend alike.
        let tenure = Arc::new(AtomicU64::new(0));
        let limits = leases::Backend::standby(self.shared.clone(), tenure.clone());
        let factory: EnvFactory = Arc::new(move |shard| {
            let standby = Standby {
                shards,
                tenure: tenure.clone(),
            };
            Ok(Box::new(storage.open_standby(shard, standby)?) as Box<dyn Env>)
        });
        let mut cfg = Config::memory();
        cfg.durability = Durability::Custom(factory);
        cfg.shards = Some(shards);
        cfg.local = self.shared.local.clone();
        cfg.limits = Some(Arc::new(limits));
        cfg
    }

    pub fn engine_config(&self) -> Config {
        let shared = self.shared.clone();
        let epoch = shared.epoch.load(Ordering::Acquire);
        let storage = ReplicaStorage::new(self);
        let kind = shared.decide(Kind::Log(default_shards()));
        let shards = match kind {
            Kind::Log(n) => n,
            _ => 1,
        };
        let factory: EnvFactory = Arc::new(move |shard| {
            let env = match kind {
                Kind::Log(n) => storage.open_log(shard, n, epoch)?,
                _ => storage.open(shard, epoch)?,
            };
            Ok(Box::new(env) as Box<dyn Env>)
        });
        let mut cfg = Config::memory();
        cfg.durability = Durability::Custom(factory);
        cfg.shards = Some(shards);
        cfg.local = self.shared.local.clone();
        cfg.limits = Some(Arc::new(leases::Backend::new(self.shared.clone(), epoch)));
        cfg
    }

    /// Whether this replica is the ready primary and holds a read lease now:
    /// a quorum answered its ping recently enough that no other member can
    /// lead a newer view yet, so reads of its engine (queries, results,
    /// inspect, list) see every write the group acknowledged. The lease is
    /// renewed with every heartbeat and lapses about
    /// [`Timing::lease_ms`] after the last quorum of answers.
    pub fn lease_valid(&self) -> bool {
        self.status.borrow().ready && now_ms() < self.shared.lease_until.load(Ordering::Acquire)
    }

    /// For a read that must not be stale: returns at once under a lease,
    /// else once a no-op commits through a quorum (one round trip). Fails
    /// if this replica is not the ready primary or no quorum answers; the
    /// read must then not be served from this replica.
    pub async fn confirm_reads(&self) -> io::Result<()> {
        ReplicaStorage::new(self).confirm_quorum().await
    }

    /// The membership epoch this replica is in, and its members with their
    /// addresses, in order (the first leads the epoch's first view).
    pub fn membership(&self) -> (u64, Vec<(ReplicaId, String)>) {
        let m = self
            .shared
            .members
            .read()
            .unwrap_or_else(|e| e.into_inner());
        (
            m.0.epoch,
            m.0.members
                .iter()
                .map(|id| (*id, m.1[usize::from(*id)].clone()))
                .collect(),
        )
    }

    /// Changes the group's membership to `members` (each ID with its
    /// address), from any member: the primary proposes it as the last entry
    /// of the current epoch; once it commits, the new members that hold the
    /// log change to the next epoch with a view change, members left out
    /// retire, and new members (started with [`ReplicaConfig::join`]) fetch
    /// the log. A quorum of the new members must be members now, so one
    /// change adds or removes members, or replaces a minority: replace a
    /// dead member by adding its successor in its place. Returns the new
    /// epoch once this replica has entered it (a removed replica enters it
    /// to retire). Like a failover, the change stops the primary's engine:
    /// it reopens on the first member of the new list (the current primary
    /// when it stays).
    pub async fn change_members(
        &self,
        members: Vec<(ReplicaId, String)>,
        timeout: Duration,
    ) -> io::Result<u64> {
        let (epoch, current) = {
            let m = self
                .shared
                .members
                .read()
                .unwrap_or_else(|e| e.into_inner());
            (m.0.epoch, m.0.clone())
        };
        // Refuse a change the primary would refuse, with its reason.
        current
            .next(ReplicaId::MAX, &members)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let mut status = self.status.clone();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            self.shared
                .reconfigure
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(members.clone());
            self.shared.wake.notify_one();
            // Ask again now and then: the request, or the entry, can be lost
            // with a primary.
            let retry = tokio::time::Instant::now() + Duration::from_millis(500);
            match tokio::time::timeout_at(
                retry.min(deadline),
                status.wait_for(|s| s.epoch > epoch || s.stopped),
            )
            .await
            {
                Ok(Ok(s)) if s.stopped => return Err(other("replica stopped")),
                Ok(Ok(s)) => return Ok(s.epoch),
                Ok(Err(_)) => return Err(other("replica stopped")),
                Err(_) if tokio::time::Instant::now() >= deadline => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "the membership change did not complete in time (is a quorum of the old and of the new members up?)",
                    ));
                }
                Err(_) => {}
            }
        }
    }

    /// A handle that answers [`Self::lease_valid`] and
    /// [`Self::confirm_reads`] from other tasks.
    pub fn reads(&self) -> ReadGuard {
        ReadGuard(ReplicaStorage::new(self))
    }

    /// Stops the replica at once, as a crash would (in-flight work is lost
    /// unless a quorum already holds it).
    pub fn kill(self) {
        drop(self);
    }
}

/// Checks that reads of a replicated engine are served under a lease (see
/// [`ReplicaNode::lease_valid`]).
#[derive(Clone)]
pub struct ReadGuard(ReplicaStorage);

impl ReadGuard {
    pub fn lease_valid(&self) -> bool {
        self.0.lease_valid()
    }

    pub async fn confirm_reads(&self) -> io::Result<()> {
        self.0.confirm_quorum().await
    }
}

/// Which member an inbound stream may carry frames from.
#[derive(Clone, Copy)]
enum Sender {
    /// No security: the frame's sender byte is trusted.
    Any,
    /// Mutual TLS without a token: the stream belongs to its first sender,
    /// which must be one of these members (a bit per member ID).
    First(u32),
    /// The member the token exchange authenticated.
    Only(ReplicaId),
}

/// What an inbound stream needs before it may reach the protocol.
struct Inbox {
    /// Admitted streams, to be read on the I/O thread.
    tx: mpsc::UnboundedSender<Admitted>,
    acceptor: Arc<Acceptor>,
    /// The members' addresses, which change with the membership.
    shared: Arc<Shared>,
    me: ReplicaId,
    /// Runs TLS handshakes, so that a peer (or anyone who can reach the
    /// port) never spends the I/O thread's time on key exchanges.
    handshakes: tokio::runtime::Handle,
}

/// An authenticated inbound stream and whom it may speak for.
struct Admitted {
    stream: Secured<tokio::net::TcpStream>,
    sender: Sender,
}

async fn listen(l: tokio::net::TcpListener, inbox: Inbox) {
    let inbox = Arc::new(inbox);
    let protocol = tokio::runtime::Handle::current();
    loop {
        let Ok((s, peer)) = l.accept().await else {
            continue;
        };
        let _ = s.set_nodelay(true);
        if inbox.acceptor.is_open() {
            let _ = inbox.tx.send(Admitted {
                stream: Secured::Plain(s),
                sender: Sender::Any,
            });
            continue;
        }
        // The handshake runs before a single frame is read: an unauthenticated
        // stream never reaches the protocol.
        let inbox = inbox.clone();
        if !inbox.acceptor.has_tls() {
            // A token exchange alone is a few hashes: run it here.
            tokio::spawn(async move {
                let accepted = inbox.acceptor.accept_tcp(s).await;
                inbox.admit(accepted, peer);
            });
            continue;
        }
        let Ok(s) = s.into_std() else { continue };
        let protocol = protocol.clone();
        inbox.handshakes.clone().spawn(async move {
            let handshaken = match tokio::net::TcpStream::from_std(s) {
                Ok(s) => inbox.acceptor.handshake_tls(s).await,
                Err(e) => Err(e),
            };
            match handshaken {
                Ok(h) => {
                    protocol.spawn(async move {
                        let accepted = inbox.acceptor.finish(h).await;
                        inbox.admit(accepted, peer);
                    });
                }
                Err(e) => inbox.refuse(peer, &e),
            }
        });
    }
}

impl Inbox {
    fn refuse(&self, peer: std::net::SocketAddr, e: &io::Error) {
        crate::security::refused(
            format_args!("replica {}: refused a connection from {peer}", self.me),
            e,
        );
    }

    /// Ties an accepted stream to the members it may speak for, then hands
    /// it to the I/O thread's readers.
    fn admit(
        &self,
        accepted: io::Result<(Secured<tokio::net::TcpStream>, Option<ReplicaId>)>,
        peer: std::net::SocketAddr,
    ) {
        let (stream, proven) = match accepted {
            Ok(accepted) => accepted,
            Err(e) => return self.refuse(peer, &e),
        };
        // Every replica this one knows the address of: the members, those
        // the last change removed, and (for a joining replica) those it
        // was told to ask.
        let peers = self
            .shared
            .members
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .1
            .clone();
        let others = peers
            .iter()
            .enumerate()
            .filter(|(i, p)| !p.is_empty() && *i != usize::from(self.me))
            .fold(0u32, |mask, (i, _)| mask | 1 << i);
        // Under mutual TLS, only the members whose host the certificate
        // names: a certificate the CA signed for anything else (a worker)
        // never speaks for a member.
        let allowed = others
            & match &stream {
                Secured::Tls(s) => s
                    .peer_certificate()
                    .map_or(0, |cert| crate::security::members_named(&cert, &peers)),
                Secured::Plain(_) => others,
            };
        let sender = match proven {
            Some(id) if member(allowed, id) => Sender::Only(id),
            Some(id) => {
                return self.refuse(
                    peer,
                    &other(format!(
                        "it authenticated as member {id}, which is not another member or which its certificate does not name"
                    )),
                );
            }
            None if allowed == 0 => {
                return self.refuse(peer, &other("its certificate names no other member's host"));
            }
            None => Sender::First(allowed),
        };
        let _ = self.tx.send(Admitted { stream, sender });
    }
}

fn member(mask: u32, id: ReplicaId) -> bool {
    u32::from(id) < 32 && mask & (1 << id) != 0
}

/// Starts a reader on the I/O thread for every admitted stream.
async fn read_admitted(mut admitted: mpsc::UnboundedReceiver<Admitted>, core: CoreCell) {
    while let Some(a) = admitted.recv().await {
        let core = core.clone();
        match a.stream {
            Secured::Plain(s) => tokio::task::spawn_local(receive(s, a.sender, core)),
            Secured::Tls(s) => tokio::task::spawn_local(receive(s, a.sender, core)),
        };
    }
}

/// Reads one peer's stream and handles each frame on the spot: no channel
/// and no other task between the socket and the protocol. Every read ends
/// with one round (one log write, one socket write per peer).
async fn receive<R: tokio::io::AsyncRead + Unpin>(mut s: R, mut sender: Sender, core: CoreCell) {
    let traffic = lock(&core).shared.traffic.clone();
    let mut dec = frame::Decoder::new();
    let mut buf = vec![0u8; 1 << 16];
    let mut out = Vec::new();
    // The member whose frames this stream carried.
    let mut carried = None;
    loop {
        let n = match s.read(&mut buf).await {
            Ok(0) | Err(_) => {
                // A crashed process's streams close at once: if this was
                // the primary's, its backups need not wait out a timeout.
                if let Some(id) = carried {
                    let mut c = lock(&core);
                    c.replica.peer_closed(id, now_ms());
                }
                return;
            }
            Ok(n) => n,
        };
        traffic.received.fetch_add(n as u64, Ordering::Relaxed);
        let read_at = traffic.timings.start();
        dec.feed(&buf[..n]);
        let mut c = lock(&core);
        if c.halted.is_some() {
            return;
        }
        c.read_at = read_at;
        let mut keep = true;
        let result = (|| -> io::Result<()> {
            loop {
                match dec.next() {
                    Ok(Some((from, body))) => {
                        match sender {
                            Sender::Any => {}
                            Sender::First(allowed) if member(allowed, from) => {
                                sender = Sender::Only(from);
                            }
                            Sender::Only(id) if id == from => {}
                            // A frame from a member this stream may not speak
                            // for: drop the stream and everything after it.
                            Sender::First(_) | Sender::Only(_) => {
                                keep = false;
                                break;
                            }
                        }
                        carried = Some(from);
                        c.receive(from, &body, &mut out)?;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        keep = false;
                        break;
                    }
                }
            }
            c.round(&mut out)
        })();
        c.read_at = None;
        if let Err(e) = result {
            c.halt(e);
            return;
        }
        // An acknowledgment can make the primary's own write the one a
        // quorum waits for: have that write wake the main task, and let it
        // look now if the write already finished.
        if c.released || c.arm_durable_wake() || !busy_poll().is_zero() {
            // Let the main task spin for the shards' next sections (or,
            // busy polling, for the next frame).
            c.shared.wake.notify_one();
        }
        if !keep {
            return;
        }
    }
}

/// The protocol state. The I/O thread holds it while it handles frames,
/// timers and proposals; a lone shard takes it, without waiting, to
/// propose its own section ([`Core::inline_round`]).
type CoreCell = Arc<Mutex<Core>>;

fn lock(core: &CoreCell) -> std::sync::MutexGuard<'_, Core> {
    core.lock().unwrap_or_else(|e| e.into_inner())
}

struct Core {
    id: ReplicaId,
    replica: Replica,
    log: VsrLog,
    /// The primary's background log writes.
    writes: writes::Writes,
    /// Segment groups: committed batches materialized into the engine's
    /// segment files.
    writer: BatchWriter,
    applied: Op,
    staged_since_hint: bool,
    senders: BTreeMap<ReplicaId, Arc<transport::Peer>>,
    /// This round's frames for each peer, written once at its end.
    outbox: Vec<Vec<u8>>,
    /// The last Prepare encoded this round, sent unchanged to every backup.
    prepare_cache: Option<((u64, Op, Op, u64, Op), Vec<u8>)>,
    /// Permit commands and segment-group batches taken off the proposal queue.
    queued: Vec<Proposal>,
    pending: BTreeMap<Op, Pending>,
    leases: leases::Ledger,
    lease_applied: Op,
    checkpoints: transfer::Checkpoints,
    // First control op, logical clock at election, local monotonic start.
    session: Option<(Op, u64, u64)>,
    shared: Arc<Shared>,
    ready: bool,
    last_hint: u64,
    /// This round released commits to their waiters.
    released: bool,
    /// Group commit: a round's usual time in ns, and when the last log batch
    /// was proposed (see `pipeline_full`).
    round_ns: u64,
    last_entry_at: Option<Instant>,
    /// The last recovery snapshot handed to opening shards, by commit.
    recovery: Option<(Op, Arc<log3::Recovery>)>,
    /// The commit hint last queued, and the file it goes to.
    commit_hint: (Op, Option<(PathBuf, Arc<File>)>),
    /// A storage error stopped the protocol.
    halted: Option<io::Error>,
    /// Starts connections to members a membership change adds.
    dialing: Dialing,
    vsr_dir: PathBuf,
    /// Timings only: when the inbound read this round handles returned.
    read_at: Option<Instant>,
    /// Timings only: log batches proposed this round, not yet sent.
    unsent: Vec<Op>,
    /// Timings only (backup): this round's write became durable, and an
    /// acknowledgment is queued.
    durable_at: Option<Instant>,
    ack_queued: bool,
}

/// Whether an entry body is a segment-group (format 2) flush batch: not a permit
/// command, a log batch or a membership change.
fn is_segment_batch(body: &[u8]) -> bool {
    !leases::is_control(body) && !log3::is_log_batch(body) && !fusee_core::vsr::is_reconfig(body)
}

/// Connects to every member (and every member the last change removed,
/// which still serve the log to members catching up).
struct Dialing {
    handle: tokio::runtime::Handle,
    connector: Arc<Connector>,
    me: ReplicaId,
}

impl Dialing {
    fn dial(&self, shared: &Arc<Shared>, senders: &mut BTreeMap<ReplicaId, Arc<transport::Peer>>) {
        let (membership, addresses) = shared
            .members
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let wanted: std::collections::BTreeSet<ReplicaId> = membership
            .members
            .iter()
            .chain(&membership.previous)
            .copied()
            .filter(|m| *m != self.me && !addresses[usize::from(*m)].is_empty())
            .collect();
        senders.retain(|id, _| wanted.contains(id));
        let mut dialers = shared.dialers.lock().unwrap_or_else(|e| e.into_inner());
        dialers.retain(|id, task| {
            let keep = wanted.contains(id);
            if !keep {
                task.abort();
            }
            keep
        });
        for id in wanted {
            if senders.contains_key(&id) {
                continue;
            }
            let peer = Arc::new(transport::Peer::default());
            senders.insert(id, peer.clone());
            let addr = addresses[usize::from(id)].clone();
            let traffic = shared.traffic.clone();
            let connector = self.connector.clone();
            dialers.insert(
                id,
                self.handle
                    .spawn(async move { peer.run(&addr, &traffic, &connector).await }),
            );
        }
    }
}

fn finish(slot: &Arc<Mutex<Slot>>, r: io::Result<Lsn>) {
    let mut state = slot.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(callback) = state.callback.take() {
        drop(state);
        callback(r);
    } else {
        state.result = Some(r);
        let notify = state.notify.clone();
        drop(state);
        if let Some(notify) = notify {
            notify();
        }
    }
}

/// The longest the I/O thread spins, after releasing a commit to shards,
/// for their next section before it parks. The budget follows how soon
/// sections came after recent releases ([`fusee::spin::Budget`]).
const RELEASE_SPIN_CAP: Duration = Duration::from_micros(50);

/// Log batches the primary keeps proposed and uncommitted at most before
/// staged sections wait to be merged into the next (group commit; see
/// `Core::pipeline_full`).
const IN_FLIGHT_ENTRIES: u64 = 4;

/// While an entry is uncommitted, another goes out only once the oldest has
/// waited this much longer than a round usually takes (in eighths), and no
/// sooner than [`ENTRY_GAP`] after the last: the I/O thread's cost is per
/// entry, so under load entries grow instead of multiplying.
const SLOW_ROUND_EIGHTHS: u64 = 12;
const ENTRY_GAP: Duration = Duration::from_micros(40);

/// Sections of one shard a replicated engine keeps proposed and
/// uncommitted at once: while an entry is uncommitted, a shard's next
/// records stage for the next entry instead of waiting for the commit.
const SECTIONS_IN_FLIGHT: usize = 4;

/// Busy-polled receive: `FUSEE_REPLICA_BUSY_POLL_US`, the time the I/O
/// thread keeps polling its sockets without blocking after any activity, on
/// any host. Off (0) by default; use it only where it shortens the round
/// trip measurably.
fn busy_poll() -> Duration {
    static POLL: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *POLL.get_or_init(|| {
        std::env::var("FUSEE_REPLICA_BUSY_POLL_US")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map_or(Duration::ZERO, |us| Duration::from_micros(us.min(10_000)))
    })
}

/// Spins on the I/O thread's runtime: each turn lets its readers run and
/// polls the reactor without blocking, so frames that arrive meanwhile are
/// handled too. Returns whether `ready()` became true.
async fn spin(budget: Duration, ready: impl Fn() -> bool) -> bool {
    if ready() {
        return true;
    }
    if budget.is_zero() {
        return false;
    }
    let until = Instant::now() + budget;
    loop {
        tokio::task::yield_now().await;
        if ready() {
            return true;
        }
        if Instant::now() >= until {
            return false;
        }
    }
}

/// The I/O thread's main task: shard proposals, checkpoint barriers and
/// captures, timers. Peer frames are handled by their readers ([`receive`]).
async fn drive(
    core: CoreCell,
    mut proposals: queue::Receiver<Proposal>,
    mut barriers: queue::Receiver<Barrier>,
    mut captures: mpsc::Receiver<(u64, snapshot::Sources)>,
    mut stopped: oneshot::Receiver<()>,
) {
    let wake = lock(&core).shared.wake.clone();
    let release_spin = fusee::spin::Budget::new(RELEASE_SPIN_CAP);
    let mut out = Vec::new();
    let mut next_tick = 0;
    // Ticks run at half the heartbeat interval (1 to 5 ms), so heartbeats
    // and timeouts are late by at most that.
    let tick_ms = (lock(&core).replica.timing().heartbeat_ms / 2).clamp(1, 5);
    let mut waiting_barriers: Vec<Barrier> = Vec::new();
    loop {
        if !matches!(stopped.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
            break;
        }
        if now_ms() >= next_tick && lock(&core).replica.election_due(now_ms()) {
            // About to suspect the primary. If this thread was not scheduled
            // for a while, its heartbeats may be sitting in the sockets:
            // let the readers run (a yield polls the reactor first) and
            // look again before starting an election.
            tokio::task::yield_now().await;
        }
        {
            let mut c = lock(&core);
            let held = c.shared.traffic.timings.start();
            if c.halted.is_some() {
                break;
            }
            let result = (|| -> io::Result<()> {
                while let Some(p) = proposals.try_recv() {
                    c.queued.push(p);
                }
                while let Ok((epoch, sources)) = captures.try_recv() {
                    c.capture_checkpoint(epoch, sources)?;
                }
                let now = now_ms();
                if now >= next_tick {
                    next_tick = now + tick_ms;
                    let ticked = c.shared.traffic.timings.start();
                    if !c.checkpoints.quarantine {
                        c.replica.on_tick(now, &mut out);
                        c.reconfigure(&mut out);
                    }
                    let part = c.shared.traffic.timings.start();
                    c.shared.traffic.timings.tick_protocol.observe(ticked);
                    c.process(&mut out)?;
                    c.shared.traffic.timings.tick_process.observe(part);
                    let part = c.shared.traffic.timings.start();
                    c.service_checkpoints(&mut out)?;
                    c.shared.traffic.timings.tick_checkpoints.observe(part);
                    c.shared.traffic.timings.io_tick.observe(ticked);
                }
                c.round(&mut out)?;
                while let Some(b) = barriers.try_recv() {
                    waiting_barriers.push(b);
                }
                for request in waiting_barriers.drain(..) {
                    c.answer(request)?;
                }
                Ok(())
            })();
            if let Err(e) = result {
                c.halt(e);
                break;
            }
            c.shared.traffic.timings.io_lock_hold.observe(held);
        }
        // Idle. Right after a release, a shard's next section is usually a
        // few microseconds away: spin for it (polling the sockets meanwhile)
        // rather than pay a wake-up, then park until a shard, a log write,
        // a peer or a timer wakes us. Waiting for acknowledgments parks at
        // once: the reactor wakes the reader.
        let idle_at = Instant::now();
        let after_release = std::mem::take(&mut lock(&core).released);
        let ready = || {
            lock(&core).may_propose()
                || !proposals.is_empty()
                || lock(&core).writes.has_done()
                || lock(&core).halted.is_some()
        };
        let budget = if after_release {
            release_spin.get()
        } else {
            Duration::ZERO
        }
        .max(busy_poll());
        // A write that finishes while this thread waits wakes it only if a
        // quorum waits for it (or more frames wait to be written).
        lock(&core).arm_durable_wake();
        if !spin(budget, ready).await {
            let tick = tokio::time::Instant::now()
                + Duration::from_millis(next_tick.saturating_sub(now_ms()).max(1));
            tokio::select! {
                _ = &mut stopped => break,
                () = wake.notified() => {}
                Some(p) = proposals.recv() => lock(&core).queued.push(p),
                Some(b) = barriers.recv() => waiting_barriers.push(b),
                Some((epoch, sources)) = captures.recv() => {
                    let result = lock(&core).capture_checkpoint(epoch, sources);
                    if let Err(e) = result {
                        lock(&core).halt(e);
                    }
                }
                () = tokio::time::sleep_until(tick) => {}
            }
        }
        if after_release {
            release_spin.record(idle_at.elapsed());
        }
    }
    {
        let mut c = lock(&core);
        if let Some(e) = c.halted.take() {
            // A failed write of the protocol log: stop, like a shard.
            eprintln!("Fusee replica {} stopped after a storage error: {e}", c.id);
        }
        c.halted = Some(other("replica stopped"));
        c.lose_primacy();
        c.shared.status.send_modify(|s| {
            s.ready = false;
            s.primary = false;
            s.stopped = true;
        });
        c.shared.role_changed();
    }
    proposals.close();
    barriers.close();
    for b in waiting_barriers {
        b.refuse();
    }
    while let Some(p) = proposals.recv().await {
        finish(&p.slot, Err(other("replica stopped before commit")));
    }
}

impl Core {
    fn answer(&mut self, request: Barrier) -> io::Result<()> {
        let epoch = self.shared.epoch.load(Ordering::Acquire);
        match request {
            Barrier::Checkpoint { epoch: e, reply } if self.ready && e == epoch => {
                self.save_hint()?;
                let _ = reply.send(Ok(()));
            }
            Barrier::Recovery { epoch: e, reply } if self.ready && e == epoch => {
                let commit = self.replica.commit();
                let cached = self
                    .recovery
                    .as_ref()
                    .filter(|(at, r)| *at == commit && r.epoch == epoch)
                    .map(|(_, r)| r.clone());
                let r = match cached {
                    Some(r) => r,
                    None => {
                        let r = Arc::new(log3::Recovery {
                            epoch,
                            entries: self
                                .replica
                                .entries()
                                .filter(|e| e.op <= commit && log3::is_log_batch(&e.body))
                                .cloned()
                                .collect(),
                        });
                        self.recovery = Some((commit, r.clone()));
                        r
                    }
                };
                let _ = reply.send(Ok(r));
            }
            other => other.refuse(),
        }
        Ok(())
    }

    /// Membership change requests: proposed here on the primary, else sent
    /// to the primary of this replica's view.
    fn reconfigure(&mut self, out: &mut Vec<Action>) {
        let requests = std::mem::take(
            &mut *self
                .shared
                .reconfigure
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        for members in requests {
            if self.replica.is_primary() {
                match self.replica.reconfigure(&members, out) {
                    Ok(_) | Err("a membership change is already in progress") => {}
                    Err(e) => {
                        eprintln!("Fusee replica {}: membership change refused: {e}", self.id)
                    }
                }
            } else {
                let primary = self.replica.primary_of(self.replica.view());
                let epoch = self.replica.membership().epoch + 1;
                if primary != self.id && usize::from(primary) < fusee_core::vsr::MAX_MEMBERS {
                    out.push(Action::Send {
                        to: primary,
                        msg: Msg::Reconfigure {
                            body: fusee_core::vsr::encode_reconfig(epoch, &members),
                        },
                    });
                }
            }
        }
    }

    /// Before waiting: arms the primary's own finished writes to wake the
    /// I/O thread only when its durability would commit entries (see
    /// [`writes::Writes::wake_when_done`]). Returns whether a write has
    /// finished meanwhile.
    fn arm_durable_wake(&self) -> bool {
        self.writes
            .wake_when_done(self.replica.commit_waits_on_own_durability());
        self.writes.has_done()
    }

    /// A round run by a shard thread for its own section, while the I/O
    /// thread waits: propose what is staged, send the Prepare, hand the
    /// primary's own write to its helper. The shard then waits for the
    /// commit; the I/O thread, woken by the acknowledgment,
    /// releases it. One thread hand-off fewer per sequential step (the
    /// shared log's inline write, for the replicated log).
    fn inline_round(&mut self) {
        if self.halted.is_some() {
            return;
        }
        let mut out = Vec::new();
        if let Err(e) = self.round(&mut out) {
            self.halt(e);
            return;
        }
        if self.arm_durable_wake() || !out.is_empty() {
            self.shared.wake.notify_one();
        }
    }

    /// Staged sections wait for a commit: adaptive group commit. With
    /// nothing uncommitted they go out at once; with an entry in flight they
    /// wait for it to commit and go out together, unless that round runs
    /// slow (then up to [`IN_FLIGHT_ENTRIES`], [`ENTRY_GAP`] apart).
    fn pipeline_full(&self) -> bool {
        let uncommitted = self.replica.op().saturating_sub(self.replica.commit());
        if uncommitted == 0 {
            return false;
        }
        if uncommitted >= IN_FLIGHT_ENTRIES {
            return true;
        }
        // The oldest log batch in flight; with permit commands only, two
        // uncommitted entries fill the pipeline.
        let Some(at) = self.pending.values().find_map(|p| match p {
            Pending::Sections { at, .. } => Some(*at),
            Pending::Slot { .. } => None,
        }) else {
            return uncommitted >= 2;
        };
        let slow = at.elapsed().as_nanos() as u64 > self.round_ns * SLOW_ROUND_EIGHTHS / 8;
        let spaced = self.last_entry_at.is_none_or(|t| t.elapsed() >= ENTRY_GAP);
        !(slow && spaced)
    }

    /// Staged sections may be proposed now (see [`Self::propose_staged`]).
    fn may_propose(&self) -> bool {
        self.shared.batcher.has_staged() && !(self.ready && self.pipeline_full())
    }

    fn halt(&mut self, e: io::Error) {
        if self.halted.is_none() {
            self.halted = Some(e);
        }
        self.shared.wake.notify_one();
    }

    /// One round: propose what shards staged, act on the protocol's output
    /// (one log write), then write each peer's frames once.
    fn round(&mut self, out: &mut Vec<Action>) -> io::Result<()> {
        self.propose_staged(out);
        self.process(out)?;
        // A commit in this round (an acknowledgment it read) let what staged
        // meanwhile go out: in the same round's frames.
        if self.may_propose() {
            self.propose_staged(out);
            self.process(out)?;
        }
        self.flush_outbox();
        Ok(())
    }

    fn flush_outbox(&mut self) {
        self.prepare_cache = None;
        for (to, bytes) in self.outbox.iter_mut().enumerate() {
            if bytes.is_empty() {
                continue;
            }
            let bytes = std::mem::take(bytes);
            if let Some(tx) = self.senders.get(&(to as ReplicaId)) {
                tx.send(bytes, &self.shared.traffic);
            }
        }
        let timings = &self.shared.traffic.timings;
        if !self.unsent.is_empty() {
            let now = timings.start();
            for op in self.unsent.drain(..) {
                if let Some(Pending::Sections { proposed, sent, .. }) = self.pending.get_mut(&op) {
                    *sent = now;
                    timings.take_to_sent.between(*proposed, now);
                }
            }
        }
        if std::mem::take(&mut self.ack_queued) {
            let now = timings.start();
            timings.durable_to_ack.between(self.durable_at.take(), now);
            timings.prepare_to_ack.between(self.read_at, now);
        }
    }

    /// Frames `msg` for `to` into this round's output. A Prepare is encoded
    /// once and the same bytes go to every backup.
    fn queue_send(&mut self, to: ReplicaId, msg: &Msg) {
        let Some(buf) = self.outbox.get_mut(usize::from(to)) else {
            return;
        };
        if matches!(msg, Msg::PrepareOk { .. }) {
            self.ack_queued = self.shared.traffic.timings.enabled();
        }
        if let Msg::Prepare {
            view,
            entries,
            commit,
        } = msg
            && let (Some(first), Some(last)) = (entries.first(), entries.last())
        {
            let key = (*view, first.op, last.op, last.checksum, *commit);
            if let Some((k, bytes)) = &self.prepare_cache
                && *k == key
            {
                buf.extend_from_slice(bytes);
                return;
            }
            let timings = &self.shared.traffic.timings;
            timings.count(&timings.entries_per_prepare, entries.len());
            let mut body = Vec::new();
            encode_msg(msg, &mut body);
            let mut f = Vec::with_capacity(body.len() + frame::HEADER);
            frame::encode(self.id, &body, &mut f);
            buf.extend_from_slice(&f);
            self.prepare_cache = Some((key, f));
            return;
        }
        let mut body = Vec::new();
        encode_msg(msg, &mut body);
        frame::encode(self.id, &body, buf);
    }

    /// Proposes permit commands, segment-group batches and every staged section
    /// (one log batch) as consecutive entries of one Prepare.
    fn propose_staged(&mut self, out: &mut Vec<Action>) {
        // A membership change ends this epoch: what shards stage waits for
        // it (and fails with the role, as in any failover).
        if self.ready && self.replica.sealed() {
            return;
        }
        // Group commit: while enough entries are uncommitted, staged
        // sections wait, and those of every shard that stages meanwhile go
        // out as one entry when one commits. A shard has one section in
        // flight, so it waits at most one commit; an idle group proposes at
        // once. Permit commands do not wait.
        let staged = if self.ready && self.pipeline_full() {
            Vec::new()
        } else {
            self.shared.batcher.take()
        };
        if staged.is_empty() && self.queued.is_empty() {
            return;
        }
        let queued = std::mem::take(&mut self.queued);
        let epoch = self.shared.epoch.load(Ordering::Acquire);
        if !self.ready {
            for p in queued {
                finish(&p.slot, Err(other("this replica is not the primary")));
            }
            for s in staged {
                s.slot.fail();
            }
            return;
        }
        let mut bodies = Vec::new();
        let mut pendings = Vec::new();
        for p in queued {
            if p.epoch != epoch {
                finish(&p.slot, Err(other("this replica is not the primary")));
                continue;
            }
            self.shared.traffic.timings.app_queue.observe(p.queued);
            let proposed = self.shared.traffic.timings.start();
            let claim = matches!(p.control, Some(leases::Request::Claim));
            // The op this body will get.
            let op = self.replica.op() + 1 + bodies.len() as Op;
            let body = if let Some(request) = p.control {
                let now = now_ms();
                let command = match request {
                    leases::Request::Embedded(command) => command,
                    // Tickets start at one. Releasing ticket zero changes no
                    // grants, and old replicas already understand this record.
                    leases::Request::Probe => {
                        leases::Command::new(crate::limits::op::RELEASE, String::new(), 0, 0, 0.0)
                    }
                    leases::Request::Claim => {
                        self.session = Some((op, self.leases.time, now));
                        leases::Command::renew()
                    }
                    leases::Request::Worker(session, command) => {
                        if self.session.map(|s| s.0) != Some(session) {
                            finish(&p.slot, Err(other("remote worker session is stale")));
                            continue;
                        }
                        command
                    }
                };
                let (session, base, start) =
                    *self.session.get_or_insert((op, self.leases.time, now));
                match leases::encode(
                    command,
                    session,
                    base.saturating_add(now.saturating_sub(start)),
                ) {
                    Ok(data) => data,
                    Err(e) => {
                        finish(&p.slot, Err(e));
                        continue;
                    }
                }
            } else {
                p.body
            };
            bodies.push(body);
            pendings.push(Pending::Slot {
                return_session: claim,
                slot: p.slot,
                last_lsn: p.last_lsn,
                proposed,
            });
        }
        let mut staged: Vec<log3::Staged> = staged
            .into_iter()
            .filter_map(|s| {
                if s.epoch == epoch {
                    Some(s)
                } else {
                    s.slot.fail();
                    None
                }
            })
            .collect();
        if !staged.is_empty() {
            let timings = &self.shared.traffic.timings;
            for s in &staged {
                timings.stage_to_take.since_ns(s.staged_ns);
            }
            // One section per shard, in shard order. A shard has one flush
            // in flight, but consecutive sections would merge here.
            staged.sort_by_key(|s| s.shard);
            let mut merged: Vec<log3::Staged> = Vec::with_capacity(staged.len());
            for s in staged {
                if let Some(prev) = merged.last_mut()
                    && prev.shard == s.shard
                    && s.batch.first_lsn == prev.batch.last_lsn + 1
                {
                    prev.batch.section.extend_from_slice(&s.batch.section);
                    prev.batch.last_lsn = s.batch.last_lsn;
                    prev.batch.records += s.batch.records;
                    prev.slot = s.slot;
                    continue;
                }
                merged.push(s);
            }
            let shards = match self.shared.kind() {
                Kind::Log(n) => n,
                _ => merged.iter().map(|s| s.shard + 1).max().unwrap_or(1),
            };
            let sections: Vec<fusee_core::wal::SectionRef<'_>> = merged
                .iter()
                .map(|s| fusee_core::wal::SectionRef {
                    shard: s.shard,
                    first_lsn: s.batch.first_lsn,
                    count: s.batch.records,
                    records: &s.batch.section,
                })
                .collect();
            bodies.push(Arc::from(log3::encode(shards, &sections)));
            let timings = &self.shared.traffic.timings;
            timings.count(&timings.sections_per_entry, merged.len());
            pendings.push(Pending::Sections {
                shards: merged
                    .into_iter()
                    .map(|s| (s.slot, s.batch.last_lsn))
                    .collect(),
                at: Instant::now(),
                proposed: timings.start(),
                sent: None,
            });
            self.last_entry_at = Some(Instant::now());
        }
        match self.replica.propose_many(bodies, out) {
            Some((first, _)) => {
                for (op, p) in (first..).zip(pendings) {
                    if matches!(
                        p,
                        Pending::Sections {
                            proposed: Some(_),
                            ..
                        }
                    ) {
                        self.unsent.push(op);
                    }
                    self.pending.insert(op, p);
                }
            }
            None => {
                for p in pendings {
                    p.fail("this replica is not the primary");
                }
            }
        }
    }

    fn lose_primacy(&mut self) {
        // Before any frame of the view change is written.
        self.shared.lease_until.store(0, Ordering::Release);
        self.session = None;
        self.ready = false;
        self.shared.epoch.fetch_add(1, Ordering::AcqRel);
        for pending in std::mem::take(&mut self.pending).into_values() {
            pending.fail("this replica lost the primary role");
        }
        for s in self.shared.batcher.take() {
            s.slot.fail();
        }
        for p in std::mem::take(&mut self.queued) {
            finish(&p.slot, Err(other("this replica lost the primary role")));
        }
        self.recovery = None;
    }

    /// Places frames for a background write (see [`writes`]).
    fn place<'a>(
        log: &'a mut VsrLog,
    ) -> impl FnMut(Vec<u8>, bool) -> io::Result<writes::Placement> + 'a {
        move |bytes, pad| log.place(bytes, pad)
    }

    /// Takes finished background writes.
    fn poll_writes(&mut self, out: &mut Vec<Action>) -> io::Result<()> {
        if !self.writes.has_done() {
            return Ok(());
        }
        let target = self.log.target();
        if let Some(op) = self.writes.poll(&target, &mut Self::place(&mut self.log))? {
            self.replica.on_durable(op, out);
        }
        Ok(())
    }

    /// Waits for every background write: a barrier.
    fn drain_writes(&mut self) -> io::Result<Option<Op>> {
        if self.writes.idle() {
            return Ok(None);
        }
        let target = self.log.target();
        self.writes.drain(&target, &mut Self::place(&mut self.log))
    }

    /// Writes this round's frames: on the primary in the background (an
    /// entry commits on any durable quorum, so its own flush is not on the
    /// commit path), on a backup inline, then reports durability.
    fn write_round(
        &mut self,
        frames: Vec<u8>,
        last: Op,
        sync: bool,
        out: &mut Vec<Action>,
    ) -> io::Result<()> {
        if frames.is_empty() {
            return Ok(());
        }
        let start = self.shared.traffic.timings.start();
        if !sync && self.replica.is_primary() {
            // The backups' Prepare first: one of their flushes, not this
            // one, is on the commit path, and handing the write to a helper
            // costs a wake-up.
            self.flush_outbox();
            let target = self.log.target();
            self.writes
                .submit(frames, last, &target, &mut Self::place(&mut self.log))?;
            self.shared.traffic.timings.protocol_append.observe(start);
            return Ok(());
        }
        if let Some(op) = self.drain_writes()? {
            self.replica.on_durable(op, out);
        }
        let timings = &self.shared.traffic.timings;
        timings.prepare_to_write.between(self.read_at, start);
        self.log.write_frames(frames)?;
        timings.protocol_append.observe(start);
        let start = timings.start();
        if !self.shared.local.emulated_flush.is_zero() {
            std::thread::sleep(self.shared.local.emulated_flush);
        }
        self.log.sync()?;
        timings.protocol_sync.observe(start);
        self.durable_at = timings.start();
        self.replica.on_durable(last, out);
        Ok(())
    }

    fn process(&mut self, out: &mut Vec<Action>) -> io::Result<()> {
        for _ in 0..1000 {
            self.poll_writes(out)?;
            if out.is_empty() {
                break;
            }
            let actions: Vec<Action> = std::mem::take(out);
            let mut frames = Vec::new();
            let mut last = 0;
            // A replacement writes the whole log as it now is in memory,
            // which already includes any entry persisted after it here.
            let mut replaced = false;
            for a in actions {
                match a {
                    Action::Send { to, msg } => self.queue_send(to, &msg),
                    Action::Persist { entries } => {
                        if replaced {
                            continue;
                        }
                        for e in &entries {
                            VsrLog::frame(e, &mut frames);
                        }
                        last = entries.last().map_or(last, |e| e.op);
                    }
                    Action::Replace { .. } => {
                        frames.clear();
                        // In-flight writes go to the file being replaced.
                        self.drain_writes()?;
                        self.log.replace(self.replica.entries())?;
                        replaced = true;
                    }
                    Action::Committed { .. } => {}
                    Action::SaveView {
                        view,
                        last_normal,
                        commit,
                    } => {
                        // A barrier: everything before it is durable first.
                        let frames = std::mem::take(&mut frames);
                        self.write_round(frames, last, true, out)?;
                        if let Some(op) = self.drain_writes()? {
                            self.replica.on_durable(op, out);
                        }
                        self.writer.sync_staged()?;
                        self.log.save(&Superblock {
                            view,
                            last_normal,
                            commit,
                            applied: self.applied,
                        })?;
                    }
                    Action::Role { primary, .. } => {
                        if !primary {
                            self.lose_primacy();
                        }
                    }
                    Action::SaveMembership { membership } => {
                        // A barrier too: durable before anything after it.
                        let frames = std::mem::take(&mut frames);
                        self.write_round(frames, last, true, out)?;
                        if let Some(op) = self.drain_writes()? {
                            self.replica.on_durable(op, out);
                        }
                        write_membership(&self.vsr_dir, &membership, self.log.method)?;
                        {
                            let mut m = self
                                .shared
                                .members
                                .write()
                                .unwrap_or_else(|e| e.into_inner());
                            for (id, addr) in membership.addresses() {
                                m.1[usize::from(id)] = addr;
                            }
                            m.0 = membership;
                        }
                        self.dialing.dial(&self.shared, &mut self.senders);
                    }
                }
            }
            if replaced {
                let op = self.replica.op();
                self.replica.on_durable(op, out);
            } else {
                self.write_round(frames, last, false, out)?;
            }
        }
        self.apply()?;
        self.publish();
        Ok(())
    }

    // A saved "applied" watermark allows recovery to skip these entries.
    // Only publish it after their materialized segments are themselves durable.
    fn save_hint(&mut self) -> io::Result<()> {
        let start = self.shared.traffic.timings.start();
        // The superblock vouches for the log: finish the background writes.
        if let Some(op) = self.drain_writes()? {
            // On the primary this only moves the commit position, which the
            // next round releases.
            self.replica.on_durable(op, &mut Vec::new());
        }
        self.writer.sync_staged()?;
        self.staged_since_hint = false;
        self.log.save(&Superblock {
            view: self.replica.view(),
            last_normal: self.replica.last_normal(),
            commit: self.replica.commit(),
            applied: self.applied,
        })?;
        self.shared.traffic.timings.hint.observe(start);
        self.last_hint = now_ms();
        Ok(())
    }

    /// Releases committed entries to their waiters, then materializes
    /// committed segment-group batches into the engine's segment files.
    ///
    /// Release happens at the commit position: a quorum holds those entries
    /// durably, whether or not this replica's own write has finished (the
    /// safety argument is in `docs/replication.md`). Materialization keeps
    /// to `min(commit, durable)`: recovery re-stages every committed entry
    /// above the saved applied hint, and the hint only moves after
    /// `sync_staged`. Both loops finish before this returns, so a
    /// checkpoint barrier, capture or view save, all handled after
    /// `process`, never sees a released segment-group batch that is not staged.
    fn apply(&mut self) -> io::Result<()> {
        if self.checkpoints.quarantine {
            return Ok(());
        }
        let commit = self.replica.commit();
        while self.lease_applied < commit {
            let op = self.lease_applied + 1;
            let Some(e) = self.replica.entry(op) else {
                break;
            };
            let answer = if leases::is_control(&e.body) {
                Some(self.leases.apply(&e.body)?)
            } else {
                None
            };
            self.lease_applied = op;
            match self.pending.remove(&op) {
                Some(Pending::Slot {
                    return_session,
                    slot,
                    last_lsn,
                    proposed,
                }) => {
                    self.shared.traffic.timings.quorum.observe(proposed);
                    self.released = true;
                    let lsn = match answer {
                        Some(_) if return_session => op,
                        Some(answer) => answer,
                        None => last_lsn,
                    };
                    finish(&slot, Ok(lsn));
                }
                Some(Pending::Sections {
                    shards,
                    at,
                    proposed,
                    sent,
                }) => {
                    // A round: proposal to commit (an average of the last
                    // eight), for group commit's slow-round rule.
                    let round = at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    self.round_ns = (self.round_ns * 7 + round) / 8;
                    let timings = &self.shared.traffic.timings;
                    timings.quorum.observe(proposed);
                    if proposed.is_some() {
                        // Only a commit that an acknowledgment brought (not
                        // one the primary's own write completed) has a read.
                        let now = timings.start();
                        if self.read_at.is_some() {
                            timings.sent_to_ack.between(sent, self.read_at);
                            timings.ack_to_release.between(self.read_at, now);
                        }
                    }
                    let released = timings.stamp();
                    self.released = true;
                    for (slot, lsn) in shards {
                        slot.committed.fetch_max(lsn, Ordering::AcqRel);
                        slot.released_ns.store(released, Ordering::Relaxed);
                        slot.wake();
                    }
                }
                None => {}
            }
        }
        let through = self.lease_applied.min(self.replica.durable());
        for op in self.applied + 1..=through {
            let e = self
                .replica
                .entry(op)
                .ok_or_else(|| other("released entry left the protocol log"))?;
            if is_segment_batch(&e.body) {
                let start = self.shared.traffic.timings.start();
                let batch = proto::decode_batch(&mut Reader::new(&e.body)).map_err(other)?;
                self.writer.stage(&batch)?;
                self.staged_since_hint = true;
                self.shared.traffic.timings.materialize.observe(start);
            }
            self.applied = op;
        }
        // Only materialized segments need the periodic hint and its sync.
        let now = now_ms();
        if now.saturating_sub(self.last_hint) >= 200 {
            if self.staged_since_hint {
                self.save_hint()?;
            } else {
                self.queue_commit_hint(now)?;
            }
        }
        Ok(())
    }

    /// Records the commit position in the background, so a replica that
    /// restarts with a damaged committed tail keeps it for repair instead
    /// of taking it for an unacknowledged one.
    fn queue_commit_hint(&mut self, now: u64) -> io::Result<()> {
        let commit = self.replica.commit().min(self.replica.durable());
        if commit <= self.commit_hint.0 {
            return Ok(());
        }
        self.last_hint = now;
        let file = match &self.commit_hint.1 {
            Some((dir, file)) if *dir == self.log.dir => file.clone(),
            _ => {
                let file = Arc::new(
                    OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(self.log.dir.join(COMMIT_HINT))?,
                );
                self.commit_hint.1 = Some((self.log.dir.clone(), file.clone()));
                file
            }
        };
        self.commit_hint.0 = commit;
        self.writes
            .hint(file, encode_commit_hint(commit), self.log.method);
        Ok(())
    }

    /// What the group's data says it is: the newest data entry, else the
    /// checkpoint image.
    fn observed_kind(&self) -> Kind {
        for e in self.replica.entries().rev() {
            if log3::is_log_batch(&e.body) {
                return log3::decode(&e.body).map_or(Kind::Undecided, |(n, _)| Kind::Log(n));
            }
            if is_segment_batch(&e.body) {
                return Kind::Segments;
            }
        }
        match self.checkpoints.image() {
            Some(image) if image.shards > 0 => Kind::Log(image.shards),
            Some(image) if image.lsn > 0 => Kind::Segments,
            _ => Kind::Undecided,
        }
    }

    fn publish(&mut self) {
        let primary = self.replica.is_primary();
        let caught_up = self.replica.commit() == self.replica.op()
            && self.applied == self.replica.commit()
            && self.lease_applied == self.replica.commit();
        let ready = primary && (self.ready || caught_up);
        if ready && !self.ready {
            self.shared.epoch.fetch_add(1, Ordering::AcqRel);
            // What the engine of this epoch opens as.
            let kind = self.observed_kind();
            self.shared.kind.store(kind.encode(), Ordering::Release);
        }
        if !ready && self.ready {
            self.lose_primacy();
        }
        self.ready = ready;
        let lease = if ready {
            self.replica.lease_until().unwrap_or(0)
        } else {
            0
        };
        self.shared.lease_until.store(lease, Ordering::Release);
        let s = NodeStatus {
            view: self.replica.view(),
            primary,
            ready,
            stopped: false,
            epoch: self.replica.membership().epoch,
            retired: self.replica.retired(),
            joining: self.replica.joining(),
            op: self.replica.op(),
            checkpoint: self.replica.base(),
            commit: self.replica.commit(),
            applied: self.applied,
        };
        // Progress fields change with every commit. Readers that poll see
        // them in place; waiters are woken only when the role, view,
        // readiness, stop state or membership changes, which is all any of
        // them waits on.
        let changed = self.shared.status.send_if_modified(|old| {
            let notify = old.view != s.view
                || old.primary != s.primary
                || old.ready != s.ready
                || old.stopped != s.stopped
                || old.epoch != s.epoch
                || old.retired != s.retired
                || old.joining != s.joining;
            *old = s;
            notify
        });
        if changed {
            self.shared.role_changed();
        }
    }
}

// ---------------------------------------------------------------- env

/// How a shard's flushes reach the group.
enum Mode {
    /// A segment group's only shard: each flush is one entry.
    Segments {
        slot: Arc<Mutex<Slot>>,
        inflight: bool,
    },
    /// A log group: the shard's section joins the next log batch.
    Log {
        index: u32,
        slot: Arc<log3::ShardSlot>,
        /// The last position submitted and not yet committed, and the
        /// committed position last reported to the runtime.
        want: Option<Lsn>,
        reported: Lsn,
        recovery: Option<Arc<log3::Recovery>>,
        /// Timings only: when the shard last saw its records committed.
        resumed_ns: u64,
    },
}

/// The primary engine's storage: the local engine directory for the run
/// store and checkpoints; the replication group for the log.
pub struct ReplicatedEnv {
    local: LocalEnv,
    shared: Arc<Shared>,
    epoch: u64,
    mode: Mode,
    checkpoint_error: Option<io::Error>,
    checkpoint_prefix: Option<snapshot::CheckpointPrefix>,
    shard: PathBuf,
    _worker_lock: File,
    /// A standby shard: it waits for this replica to become the ready
    /// primary ([`Env::wait_lease`]).
    standby: Option<Standby>,
}

/// What a standby shard needs to start when its replica is promoted.
struct Standby {
    /// The engine's shard count, which must be the group's.
    shards: u32,
    /// The primary epoch its engine runs in (0: not promoted yet).
    tenure: Arc<AtomicU64>,
}

impl ReplicatedEnv {
    fn new(
        local: LocalEnv,
        shared: Arc<Shared>,
        epoch: u64,
        mode: Mode,
        shard: PathBuf,
        lock: File,
    ) -> Self {
        Self {
            local,
            shared,
            epoch,
            mode,
            checkpoint_error: None,
            checkpoint_prefix: None,
            shard,
            _worker_lock: lock,
            standby: None,
        }
    }

    fn deposed(&self) -> bool {
        self.shared.proposals.is_closed() || self.shared.epoch.load(Ordering::Acquire) != self.epoch
    }
}

impl Env for ReplicatedEnv {
    fn monotonic_us(&self) -> u64 {
        fusee::local::monotonic_us()
    }
    fn now_ms(&self) -> u64 {
        self.local.now_ms()
    }
    fn random(&mut self) -> u64 {
        self.local.random()
    }
    fn set_notify(&mut self, notify: Notify) {
        match &self.mode {
            Mode::Segments { slot, .. } => {
                if let Ok(mut s) = slot.lock() {
                    s.notify = Some(notify.clone());
                }
            }
            Mode::Log { slot, .. } => {
                *slot.notify.lock().unwrap_or_else(|e| e.into_inner()) = Some(notify.clone());
            }
        }
        self.local.set_notify(notify);
    }
    fn submit(&mut self, batch: Batch) {
        match &mut self.mode {
            Mode::Segments { slot, inflight } => {
                *inflight = true;
                let mut body = Vec::with_capacity(batch.bytes as usize + 64);
                proto::encode_batch(&mut body, &batch);
                let p = Proposal {
                    queued: self.shared.traffic.timings.start(),
                    control: None,
                    body: Arc::from(body),
                    last_lsn: batch.last_lsn,
                    epoch: self.epoch,
                    slot: slot.clone(),
                };
                if self.shared.epoch.load(Ordering::Acquire) != self.epoch {
                    finish(slot, Err(other("this replica is no longer the primary")));
                    return;
                }
                if self.shared.proposals.send(p).is_err() {
                    finish(slot, Err(other("the replica has stopped")));
                }
            }
            Mode::Log {
                index,
                slot,
                want,
                resumed_ns,
                ..
            } => {
                *want = Some(batch.last_lsn);
                if batch.section.is_empty() && batch.records > 0 {
                    slot.fail();
                    return;
                }
                let timings = &self.shared.traffic.timings;
                timings.resume_to_stage.since_ns(std::mem::take(resumed_ns));
                // A lone shard (nothing else staged or queued, few shards
                // working) proposes its section itself if the I/O thread is
                // not busy, rather than wake it.
                let alone = !self.shared.batcher.has_staged()
                    && self.shared.proposals.is_empty()
                    && fusee::spin::few_busy();
                self.shared.batcher.stage(log3::Staged {
                    epoch: self.epoch,
                    shard: *index,
                    batch,
                    slot: slot.clone(),
                    staged_ns: timings.stamp(),
                });
                if alone
                    && let Some(core) = self.shared.core.get().and_then(std::sync::Weak::upgrade)
                    && let Ok(mut core) = core.try_lock()
                {
                    core.inline_round();
                    return;
                }
                // Free unless the I/O thread is parked: it spins on staged
                // sections before it parks.
                self.shared.wake.notify_one();
            }
        }
    }
    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>> {
        let deposed = self.deposed();
        match &mut self.mode {
            Mode::Segments { slot, inflight } => {
                let r = slot.lock().ok()?.result.take();
                if r.is_some() {
                    *inflight = false;
                    return r;
                }
                // The replica stopped, or this node lost the primary role:
                // the flush will never be confirmed here.
                if *inflight && deposed {
                    *inflight = false;
                    return Some(Err(other("this replica is no longer the primary")));
                }
                None
            }
            Mode::Log {
                slot,
                want,
                reported,
                resumed_ns,
                ..
            } => {
                let w = (*want)?;
                let committed = slot.committed.load(Ordering::Acquire);
                if committed >= w {
                    *want = None;
                    *reported = committed;
                    let timings = &self.shared.traffic.timings;
                    timings
                        .release_to_resume
                        .since_ns(slot.released_ns.swap(0, Ordering::Relaxed));
                    *resumed_ns = timings.stamp();
                    return Some(Ok(committed));
                }
                if slot.failed.load(Ordering::Acquire) || deposed {
                    *want = None;
                    return Some(Err(other("this replica is no longer the primary")));
                }
                // An earlier section of several in flight committed.
                if committed > *reported {
                    *reported = committed;
                    return Some(Ok(committed));
                }
                None
            }
        }
    }
    fn max_in_flight(&self) -> usize {
        match self.mode {
            Mode::Log { .. } => SECTIONS_IN_FLIGHT,
            Mode::Segments { .. } => 1,
        }
    }
    fn store_append(&mut self, frame: &[u8]) -> io::Result<u64> {
        self.local.store_append(frame)
    }
    fn store_append_history(
        &mut self,
        frame: &[u8],
        run: fusee_core::RunId,
        lsn: Lsn,
    ) -> io::Result<u64> {
        self.local.store_append_history(frame, run, lsn)
    }
    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        self.local.store_read(offset, len)
    }
    fn store_len(&self) -> u64 {
        self.local.store_len()
    }
    fn store_generation(&self) -> u64 {
        self.local.store_generation()
    }
    fn compact_store(
        &mut self,
        live: &[(fusee_core::RunId, fusee_core::shard::BlobRef)],
    ) -> io::Result<bool> {
        self.local.compact_store(live)
    }
    fn visibility_candidates(
        &mut self,
        filter: &fusee::visibility::ListFilter,
        limit: usize,
    ) -> Option<fusee::visibility::Candidates> {
        self.local.visibility_candidates(filter, limit)
    }
    fn checkpoint(&mut self, checkpoint: Checkpoint, delete_through_segment: Option<u64>) {
        let prefix = snapshot::CheckpointPrefix {
            lsn: checkpoint.lsn,
            generation: checkpoint.store_generation,
            len: checkpoint.store_len,
        };
        if let Mode::Log { .. } = self.mode {
            // No engine segments: the protocol log is the only copy, and
            // only a checkpoint image trims it.
            self.checkpoint_prefix = Some(prefix);
            self.local.checkpoint(checkpoint, None);
            return;
        }
        // Before dropping any engine segment, the replica must durably
        // record that replay will start after all entries the checkpoint covers.
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let result = self
            .shared
            .barriers
            .send(Barrier::Checkpoint {
                epoch: self.epoch,
                reply: tx,
            })
            .map_err(|_| other("replica stopped before checkpoint"))
            .and_then(|()| {
                rx.recv()
                    .map_err(|_| other("replica stopped during checkpoint"))?
            });
        match result {
            Ok(()) => {
                self.checkpoint_prefix = Some(prefix);
                self.local.checkpoint(checkpoint, delete_through_segment);
            }
            Err(e) => self.checkpoint_error = Some(e),
        }
    }
    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>> {
        let result = self
            .checkpoint_error
            .take()
            .map(Err)
            .or_else(|| self.local.poll_checkpoint())?;
        let index = match &self.mode {
            Mode::Log { index, .. } => Some(*index),
            Mode::Segments { .. } => None,
        };
        Some(result.and_then(|lsn| {
            if let Some(prefix) = self.checkpoint_prefix.take()
                && prefix.lsn == lsn
                && self.shared.epoch.load(Ordering::Acquire) == self.epoch
                && let Ok(slot) = self.shared.checkpoints.try_reserve()
            {
                let sources = snapshot::Sources::pin(&self.shard, prefix, index)?;
                slot.send((self.epoch, sources));
            }
            Ok(lsn)
        }))
    }
    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        self.local.load_checkpoint()
    }
    fn load_store(&mut self, generation: u64, len: u64) -> io::Result<Vec<u8>> {
        self.local.load_store(generation, len)
    }
    fn recover_store(
        &mut self,
        generation: u64,
        len: u64,
        visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry),
    ) -> io::Result<()> {
        self.local.recover_store(generation, len, visit)
    }
    fn segments(&mut self) -> io::Result<Vec<u64>> {
        match self.mode {
            Mode::Log { .. } => Ok(Vec::new()),
            Mode::Segments { .. } => self.local.segments(),
        }
    }
    fn read_segment(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        self.local.read_segment(segment)
    }
    fn sync_recovered(&mut self) -> io::Result<()> {
        match self.mode {
            Mode::Log { .. } => Ok(()),
            Mode::Segments { .. } => self.local.sync_recovered(),
        }
    }
    fn discard_segment(&mut self, segment: u64) -> io::Result<()> {
        match self.mode {
            Mode::Log { .. } => Ok(()),
            Mode::Segments { .. } => self.local.discard_segment(segment),
        }
    }
    fn open_log(&mut self, segment: u64) -> io::Result<()> {
        match self.mode {
            Mode::Log { .. } => Ok(()),
            Mode::Segments { .. } => self.local.open_log(segment),
        }
    }
    fn shared_log(&self) -> bool {
        matches!(self.mode, Mode::Log { .. })
    }
    fn recover_log(&mut self, after: Lsn) -> io::Result<(Vec<(Lsn, RecordBuf)>, Lsn)> {
        let Mode::Log {
            index,
            slot,
            recovery,
            ..
        } = &mut self.mode
        else {
            return Err(other(
                "a replicated segment group's shard has per-shard segments",
            ));
        };
        let r = recovery
            .take()
            .ok_or_else(|| other("this replica is no longer the primary"))?;
        let (records, last) = r.records(*index, after)?;
        slot.committed.fetch_max(last, Ordering::AcqRel);
        Ok((records, last))
    }
    fn log_segment(&self) -> u64 {
        // A new round asks every active shard for a checkpoint.
        self.shared.checkpoint_round.load(Ordering::Acquire)
    }
    fn checkpoint_wanted(&self) -> bool {
        match self.mode {
            Mode::Log { index, .. } => {
                index < 64
                    && self.shared.checkpoint_wanted.load(Ordering::Acquire) & (1 << index) != 0
            }
            Mode::Segments { .. } => false,
        }
    }
    fn standby(&self) -> bool {
        self.standby.is_some()
    }
    fn wait_lease(&mut self, timeout: Duration) -> io::Result<bool> {
        self.wait_promotion(timeout)
    }
}

#[cfg(test)]
mod materialization_tests {
    use super::*;
    use fusee_core::record::{Record, Status};
    use fusee_core::segment::LogWriter;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_replays_quorum_data_after_loss_of_staged_segment_bytes() {
        let dir = std::env::temp_dir().join(format!(
            "fusee-materialize-{}-{}",
            std::process::id(),
            fusee::local::unix_ms()
        ));
        let local = LocalOptions {
            flush: FlushMethod::Fsync,
            preallocate: Preallocate::None,
            ..Default::default()
        };
        let mut engine_log = LogWriter::new(0, 1, 1, 64 << 10, 1);
        let record: fusee_core::RecordRef<'_> = Record::Done {
            run: 1,
            status: Status::Ok,
            finished_ms: 10,
            payload: b"42",
            by: None,
        };
        engine_log.append(&record, 10).unwrap();
        let batch = engine_log.take().unwrap();
        let mut body = Vec::new();
        proto::encode_batch(&mut body, &batch);
        let vsr = dir.join("vsr");
        let (mut log, _, _) = VsrLog::open(&vsr, local.flush, 0, 0, 0).unwrap();
        log.append(&[Entry::new(1, 0, 0, Arc::from(body))]).unwrap();
        log.save(&Superblock {
            view: 0,
            last_normal: 0,
            commit: 1,
            applied: 0,
        })
        .unwrap();
        drop(log);
        let segment_dir = dir.join("engine/shard-000/log");
        let mut writer = BatchWriter::new(&segment_dir, local.clone()).unwrap();
        writer.stage(&batch).unwrap();
        let file = std::fs::read_dir(segment_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let expected = std::fs::read(&file).unwrap();
        drop(writer);
        // A power loss can leave the materialized file truncated; the VSR
        // entry and the earlier applied watermark are known durable.
        std::fs::write(&file, &expected[..12]).unwrap();
        let node = ReplicaNode::start(ReplicaConfig {
            id: 0,
            peers: vec!["127.0.0.1:0".into(); 3],
            join: false,
            dir: dir.clone(),
            local,
            timing: Default::default(),
            security: Default::default(),
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read(file).unwrap(), expected);
        assert_eq!(read_superblock(&vsr.join("superblock")).unwrap().applied, 1);
        node.kill();
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn chain(n: Op) -> Vec<Entry> {
        let mut parent = 0;
        (1..=n)
            .map(|op| {
                let e = Entry::new(op, 1, parent, Arc::from(vec![op as u8; 100 + op as usize]));
                parent = e.checksum;
                e
            })
            .collect()
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fusee-vsrlog-{name}-{}-{}",
            std::process::id(),
            fusee::local::unix_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// The writes a log would issue, applied to a disk image in memory.
    fn apply(image: &mut Vec<u8>, p: &writes::Placement) {
        let bytes = p.bytes.as_slice();
        let end = p.offset as usize + bytes.len();
        if image.len() < end {
            image.resize(end, 0);
        }
        image[p.offset as usize..end].copy_from_slice(bytes);
    }

    fn read_image(dir: &Path, image: &[u8], committed: Op) -> (Vec<Entry>, Option<(Op, Op)>) {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("log"), image).unwrap();
        let (_, back, damaged) = VsrLog::open(dir, FlushMethod::Fsync, committed, 0, 0).unwrap();
        (back, damaged)
    }

    /// A log written in whole aligned blocks (as through `O_DIRECT`).
    fn aligned_log(dir: &Path) -> VsrLog {
        let (mut log, _, _) = VsrLog::open(dir, FlushMethod::Fsync, 0, 0, 0).unwrap();
        log.align().unwrap();
        log
    }

    /// A staged generation's tail, read from the end of the log file: the
    /// entries after any position, across padded writes, whether the first
    /// window held the tail's start or had to widen; nothing for a position
    /// whose checksum is not the file's, or past its end.
    #[test]
    fn a_tail_is_read_from_the_end_of_the_log_file() {
        let dir = scratch("tail");
        let entries = chain(300);
        let mut log = VsrLog::open(&dir, FlushMethod::Fsync, 0, 0, 0).unwrap().0;
        for (i, e) in entries.iter().enumerate() {
            let mut frame = Vec::new();
            VsrLog::frame(e, &mut frame);
            let p = log.place(frame, i % 3 == 0).unwrap();
            log.file.write_all_at(p.bytes.as_slice(), p.offset).unwrap();
        }
        log.sync().unwrap();
        let (path, end) = (dir.join("log"), log.end);
        for window in [64, 4096, 1 << 20] {
            for after in [0usize, 1, 150, 299] {
                let (op, sum) = if after == 0 {
                    (0, 0)
                } else {
                    (entries[after - 1].op, entries[after - 1].checksum)
                };
                let tail = VsrLog::read_tail_from(&path, (op, sum), end, window).unwrap();
                assert_eq!(tail, entries[after..], "window {window}, after {after}");
            }
            let wrong = VsrLog::read_tail_from(&path, (150, 1), end, window).unwrap();
            assert!(wrong.is_empty());
            let last = entries.last().unwrap();
            let none =
                VsrLog::read_tail_from(&path, (last.op, last.checksum), end, window).unwrap();
            assert!(none.is_empty());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Writes placed while another is in flight are padded to the end of
    /// their block; the log reads back whole across the padding, and a lost
    /// padded write before a landed one is damage to repair, never a gap
    /// silently skipped. The same with whole-block writes, where a packed
    /// write's last block is written again, with the same bytes, by the next.
    #[test]
    fn padded_writes_read_back_and_a_lost_one_is_reported_as_damage() {
        for aligned in [false, true] {
            let dir = scratch(if aligned { "padded-aligned" } else { "padded" });
            let entries = chain(6);
            let mut log = if aligned {
                aligned_log(&dir)
            } else {
                VsrLog::open(&dir, FlushMethod::Fsync, 0, 0, 0).unwrap().0
            };
            let mut placed = Vec::new();
            for (i, e) in entries.iter().enumerate() {
                let mut frame = Vec::new();
                VsrLog::frame(e, &mut frame);
                let p = log.place(frame, i % 2 == 0).unwrap();
                let end = p.offset + p.bytes.as_slice().len() as u64;
                if i % 2 == 0 {
                    assert!(p.padded && end.is_multiple_of(4096));
                }
                if aligned {
                    assert!(p.offset.is_multiple_of(4096) && end.is_multiple_of(4096));
                }
                log.file.write_all_at(p.bytes.as_slice(), p.offset).unwrap();
                placed.push(p);
            }
            log.sync().unwrap();
            drop(log);
            let (_, back, damaged) = VsrLog::open(&dir, FlushMethod::Fsync, 6, 0, 0).unwrap();
            assert_eq!(back, entries);
            assert_eq!(damaged, None);
            // Entry 3's padded write never landed; entry 4's did.
            let mut image = Vec::new();
            for (i, p) in placed.iter().enumerate() {
                if i != 2 {
                    apply(&mut image, p);
                }
            }
            let (back, damaged) = read_image(&dir, &image, 2);
            assert_eq!(damaged, Some((3, 3)), "aligned {aligned}");
            assert_eq!(
                back.iter().map(|e| e.op).collect::<Vec<_>>(),
                [1, 2, 4, 5, 6]
            );
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    /// Crash states of the protocol log, up to two writes in flight as the
    /// primary's background writes run them, written packed or in whole
    /// blocks (as through `O_DIRECT`): every write that completed in order
    /// keeps every entry it carried, whatever happened to the writes in
    /// flight (landed, lost, or torn at any 512-byte sector, including one
    /// rewriting a completed write's last block), and nothing read back
    /// differs from what was written. (It found that padding shorter than a
    /// length field before a block boundary read as damage.)
    #[test]
    fn writes_keep_every_durable_entry_in_any_crash_state() {
        let dir = scratch("crash-states");
        let mut rng = 0x5eed_u64;
        let mut next = |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        let entries = chain(400);
        for aligned in [false, true] {
            let mut checked = 0;
            for trial in 0..300 {
                let log_dir = dir.join(format!("log-{aligned}-{trial}"));
                std::fs::create_dir_all(&log_dir).unwrap();
                let mut log = if aligned {
                    aligned_log(&log_dir)
                } else {
                    VsrLog::open(&log_dir, FlushMethod::Fsync, 0, 0, 0)
                        .unwrap()
                        .0
                };
                let mut image = Vec::new();
                // (placement, last op), oldest first; completion in order.
                let mut in_flight: std::collections::VecDeque<(writes::Placement, Op)> =
                    Default::default();
                let mut waiting: Vec<u8> = Vec::new();
                let mut waiting_last = 0;
                let mut busy = false;
                let mut durable = 0;
                let mut op = 0usize;
                let crash_after = 5 + next(120) as usize;
                for _ in 0..crash_after {
                    if op < entries.len() {
                        // A round: one to three entries.
                        for _ in 0..1 + next(3) {
                            if op < entries.len() {
                                VsrLog::frame(&entries[op], &mut waiting);
                                waiting_last = entries[op].op;
                                op += 1;
                            }
                        }
                        if !in_flight.is_empty() {
                            busy = true;
                        }
                    }
                    // Start what may start (the rule in `writes::Writes`).
                    if !waiting.is_empty()
                        && in_flight.len() < writes::IN_FLIGHT
                        && in_flight.back().is_none_or(|(p, _)| p.padded)
                    {
                        let pad = busy || !in_flight.is_empty();
                        let p = log.place(std::mem::take(&mut waiting), pad).unwrap();
                        in_flight.push_back((p, waiting_last));
                    }
                    // The oldest write completes now and then.
                    if !in_flight.is_empty() && next(3) == 0 {
                        let (p, last) = in_flight.pop_front().unwrap();
                        apply(&mut image, &p);
                        durable = last;
                        if in_flight.is_empty() && waiting.is_empty() {
                            busy = false;
                        }
                    }
                }
                // Crash: each write in flight landed, was lost, or tore.
                for (p, _) in &in_flight {
                    match next(3) {
                        0 => apply(&mut image, p),
                        1 => {}
                        _ => {
                            let bytes = p.bytes.as_slice();
                            let end = p.offset as usize + bytes.len();
                            if image.len() < end {
                                image.resize(end, 0);
                            }
                            for s in (0..bytes.len()).step_by(512) {
                                if next(2) == 0 {
                                    let e = (s + 512).min(bytes.len());
                                    let at = p.offset as usize + s;
                                    image[at..at + e - s].copy_from_slice(&bytes[s..e]);
                                }
                            }
                        }
                    }
                }
                let (back, _) = read_image(&log_dir, &image, durable);
                for e in &back {
                    assert_eq!(
                        *e,
                        entries[e.op as usize - 1],
                        "aligned {aligned}, trial {trial}: entry {} differs",
                        e.op
                    );
                }
                let held: std::collections::BTreeSet<Op> = back.iter().map(|e| e.op).collect();
                for o in 1..=durable {
                    assert!(
                        held.contains(&o),
                        "aligned {aligned}, trial {trial}: durable entry {o} lost"
                    );
                }
                checked += durable;
            }
            assert!(checked > 1000, "too few durable entries checked: {checked}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A filesystem that refuses an `O_DIRECT` write (`EINVAL`) moves the log
    /// to buffered writes for good: the refused write is written again
    /// buffered, later ones are packed, and everything reads back.
    #[test]
    fn a_refused_direct_write_falls_back_to_buffered_writes() {
        let dir = scratch("refused");
        let entries = chain(8);
        let mut log = aligned_log(&dir);
        // A descriptor standing in for O_DIRECT (macOS has none) whose
        // writes the "filesystem" refuses.
        log.direct = Some(log.file.clone());
        log.reject_direct = true;
        for e in &entries[..4] {
            let mut frame = Vec::new();
            VsrLog::frame(e, &mut frame);
            log.write_frames(frame).unwrap();
        }
        assert!(log.refused.load(Ordering::Acquire));
        assert!(
            log.direct.is_none() && !log.aligned,
            "buffered after the refusal"
        );
        for e in &entries[4..] {
            let mut frame = Vec::new();
            VsrLog::frame(e, &mut frame);
            log.write_frames(frame).unwrap();
        }
        log.sync().unwrap();
        drop(log);
        let (_, back, damaged) = VsrLog::open(&dir, FlushMethod::Fsync, 8, 0, 0).unwrap();
        assert_eq!(back, entries);
        assert_eq!(damaged, None);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Background writes complete in placement order, two at a time at
    /// most, and a drain waits for all of them; packed or in whole blocks.
    #[test]
    fn background_writes_report_durability_in_order() {
        for aligned in [false, true] {
            let dir = scratch(if aligned { "writes-aligned" } else { "writes" });
            let entries = chain(40);
            let mut log = if aligned {
                aligned_log(&dir)
            } else {
                VsrLog::open(&dir, FlushMethod::Fsync, 0, 0, 0).unwrap().0
            };
            let wake = Arc::new(tokio::sync::Notify::new());
            let mut writes =
                writes::Writes::new(writes::LogIo::start(wake).unwrap(), Duration::ZERO);
            let target = log.target();
            let mut durable = 0;
            for e in &entries {
                let mut frame = Vec::new();
                VsrLog::frame(e, &mut frame);
                writes
                    .submit(frame, e.op, &target, &mut |b, pad| log.place(b, pad))
                    .unwrap();
                if let Some(op) = writes
                    .poll(&target, &mut |b, pad| log.place(b, pad))
                    .unwrap()
                {
                    assert!(op > durable && op <= e.op);
                    durable = op;
                }
            }
            let last = writes
                .drain(&target, &mut |b, pad| log.place(b, pad))
                .unwrap();
            assert!(writes.idle());
            assert_eq!(last.unwrap_or(durable), 40);
            drop(writes);
            drop(log);
            let (_, back, damaged) = VsrLog::open(&dir, FlushMethod::Fsync, 40, 0, 0).unwrap();
            assert_eq!(back, entries);
            assert_eq!(damaged, None);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    /// On Linux with `direct_io`, the log really opens with `O_DIRECT` (on a
    /// filesystem that allows it; otherwise it says so and stays buffered),
    /// and appends, a replacement and a reopen keep every entry.
    #[test]
    fn the_protocol_log_writes_through_o_direct_where_the_filesystem_allows() {
        let dir = scratch("direct");
        let entries = chain(30);
        let (mut log, _, _) = VsrLog::open(&dir, FlushMethod::Dsync, 0, 0, 0).unwrap();
        log.use_direct(true).unwrap();
        let direct = log.direct.is_some();
        if cfg!(target_os = "linux") {
            assert!(direct || log.refused.load(Ordering::Acquire));
        } else {
            assert!(!direct && !log.aligned);
        }
        for e in &entries[..10] {
            let mut frame = Vec::new();
            VsrLog::frame(e, &mut frame);
            log.write_frames(frame).unwrap();
        }
        log.sync().unwrap();
        log.replace(entries[..20].iter()).unwrap();
        assert_eq!(
            log.direct.is_some(),
            direct,
            "a replaced log is written as before"
        );
        for e in &entries[20..] {
            let mut frame = Vec::new();
            VsrLog::frame(e, &mut frame);
            log.write_frames(frame).unwrap();
        }
        log.sync().unwrap();
        drop(log);
        let (mut log, back, damaged) = VsrLog::open(&dir, FlushMethod::Dsync, 30, 0, 0).unwrap();
        assert_eq!(back, entries);
        assert_eq!(damaged, None);
        // Reopened in place, the kept tail block continues the log.
        log.use_direct(true).unwrap();
        let more = {
            let mut parent = entries.last().unwrap().checksum;
            (31..=35)
                .map(|op| {
                    let e = Entry::new(op, 1, parent, Arc::from(vec![7u8; 333]));
                    parent = e.checksum;
                    e
                })
                .collect::<Vec<_>>()
        };
        for e in &more {
            let mut frame = Vec::new();
            VsrLog::frame(e, &mut frame);
            log.write_frames(frame).unwrap();
        }
        log.sync().unwrap();
        drop(log);
        let (_, back, damaged) = VsrLog::open(&dir, FlushMethod::Dsync, 35, 0, 0).unwrap();
        assert_eq!(back.len(), 35);
        assert_eq!(&back[30..], &more[..]);
        assert_eq!(damaged, None);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
