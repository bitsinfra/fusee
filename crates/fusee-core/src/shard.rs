//! Shard state.
//!
//! A shard owns a slice of the workflow ID space, one log and the state
//! derived from it. [`ShardCore::apply`] is the only state transition: it is
//! called for every record as it is appended, and again for every record read
//! back during recovery, so live state and recovered state cannot drift.
//!
//! The core does no I/O and reads no clock. It emits [`Event`]s for the host
//! (a timer fired, a signal matched a wait, a run started or finished) and
//! queues [`Output`]s that may leave the process only once the record that
//! produced them is durable: [`ShardCore::release`] hands them out as the
//! durable position advances.
//!
//! Runs are either **loaded** (full state and journal in memory) or **parked**
//! (a 32-byte entry in the [`RunTable`] pointing at its frame in the run
//! store, with its earliest timer). Suspended and finished runs are parked so
//! memory does not grow with idle work; the host loads a parked run back
//! whenever a record for it must be appended.
//!
//! **Timers.** The wheel holds only timers due within [`HORIZON_MS`]. A
//! later timer waits in its run's table entry, and each chunk of 4,096
//! entries is a time bucket that knows its earliest waiting timer. Once that
//! comes within the horizon, the sweep walks the chunk and moves what is due
//! within it into a wheel (loaded runs into the timer wheel, parked runs into
//! a wheel of loads). So a sleeping population costs its table entries and
//! nothing per timer, and no work until its timers approach.

use crate::record::{
    Lsn, Name, Origin, PAUSED, ParentRef, Payload, Record, RecordBuf, RunId, Seq, Status, ToName,
    ToPayload, WaitOutcome, decode_canonical_all, policy,
};
use crate::table::{self, Entry, RunTable, Slot, flag};
use crate::wheel::Wheel;
use crate::wire::{checksum64, hash_id};
use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TimerKind {
    Sleep = 0,
    Retry = 1,
    WaitTimeout = 2,
    Deadline = 3,
}

impl TimerKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Sleep,
            1 => Self::Retry,
            2 => Self::WaitTimeout,
            3 => Self::Deadline,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerKey {
    pub run: RunId,
    pub seq: Seq,
    pub kind: TimerKind,
}

/// Something the host must act on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A run was created and needs its code started.
    Started { run: RunId },
    /// A timer is due: a sleep fired, a retry is due, a wait timed out, or
    /// the workflow's deadline passed (`seq` is 0 for deadlines).
    Timer {
        run: RunId,
        seq: Seq,
        kind: TimerKind,
    },
    /// A buffered signal matches a pending wait.
    SignalReady {
        run: RunId,
        seq: Seq,
        ordinal: u64,
        payload: Payload,
    },
    /// A run's `Done` record was applied.
    Finished { run: RunId },
    /// A parked run has a due timer; load it so the timer can fire.
    NeedLoad { run: RunId },
}

/// Something that may leave the process once its record is durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    /// Acknowledge a client request (start, signal, cancel).
    Ack(u64),
    /// Refuse a client request, once the state the refusal is based on is
    /// durable (for example, a signal to a run whose `Done` was just
    /// appended: a crash could still take that `Done` back).
    Reject(u64),
    /// A run finished; release its result to callers.
    Result {
        run: RunId,
        status: Status,
        payload: Payload,
    },
    /// Start (or attach to) a child workflow.
    ChildStart {
        parent: ParentRef,
        wtype: Name,
        id: Name,
        flags: u8,
        deadline_ms: u64,
        input: Payload,
    },
    /// Report a child's result to its parent.
    ChildResult {
        parent: ParentRef,
        status: Status,
        payload: Payload,
    },
    /// Deliver a signal to another workflow.
    Signal {
        target: Name,
        name: Name,
        payload: Payload,
        dedup: u64,
    },
    /// An update's handler returned; release its result to callers.
    UpdateResult {
        run: RunId,
        ordinal: u64,
        id: Name,
        status: Status,
        payload: Payload,
    },
    /// Answer a client request with a value (an update result, a query
    /// answer, a refusal with its reason). Never produced by the core.
    Reply {
        token: u64,
        status: Status,
        payload: Payload,
    },
    /// Refuse a client request with a reason (an update's validator said
    /// no, a completion token is stale). Never produced by the core.
    Refuse { token: u64, message: Payload },
    /// Close another workflow: a child under its parent close policy
    /// (`parent` names the closing parent), or a cancel request sent by
    /// workflow code.
    Close {
        target: Name,
        /// The child's workflow type (parent close policy), so a child whose
        /// start has not arrived yet is recorded as closed.
        wtype: Name,
        parent: Option<ParentRef>,
        terminate: bool,
        reason: Payload,
    },
}

/// The reserved signal name `ctx.cancelWorkflow` sends.
pub const CANCEL_SIGNAL: &str = "fusee.cancel";

/// An accepted update of a loaded run.
#[derive(Clone, Debug)]
pub struct UpdateSlot {
    pub id: Name,
    /// The handler's name.
    pub name: Name,
    /// The workflow operation waiting for the result, if a workflow called.
    pub caller: Option<ParentRef>,
    /// Set once its handler returned, with the LSN of its `UpdateDone`.
    pub result: Option<(Status, Payload, Lsn)>,
}

/// Location of a blob in the run store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct BlobRef {
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Debug)]
struct SignalSlot {
    ordinal: u64,
    name: Name,
    payload: Payload,
    offered: Option<Seq>,
}

#[derive(Clone, Debug)]
struct WaitSlot {
    seq: Seq,
    name: Name,
    offered: Option<u64>,
}

/// A run held in memory.
#[derive(Clone, Debug)]
pub struct LoadedRun {
    pub id: Name,
    pub wtype: Name,
    pub version: u32,
    pub codec: u8,
    pub parent: Option<ParentRef>,
    pub started_ms: u64,
    pub deadline_ms: u64,
    pub last_lsn: Lsn,
    pub done: Option<(Status, u64)>,
    pub result: Option<Payload>,
    /// Journal prefix stored in the run store, if any: its newest frame.
    pub blob: Option<BlobRef>,
    /// Bytes of `blob` and the frames it extends (see
    /// [`crate::store::StoreEntry::Delta`]), and how many frames it extends.
    pub blob_chain: u64,
    pub blob_depth: u8,
    /// Canonical records after `blob`.
    pub journal: Vec<u8>,
    pub journal_records: u32,
    signal_count: u64,
    /// Buffered signals, oldest first. A deque: waits take them in arrival
    /// order, so draining a long buffer removes from the front each time.
    signals: VecDeque<SignalSlot>,
    waits: Vec<WaitSlot>,
    armed: Vec<(Seq, TimerKind, u64)>,
    dedup: FxHashSet<u64>,
    children: Vec<Seq>,
    pub policy: u8,
    pub from: Option<Origin>,
    /// Records still to come that are copies from `from`'s journal.
    copies_left: u32,
    /// Accepted updates by ordinal, and their IDs.
    pub updates: Vec<UpdateSlot>,
    update_ids: FxHashMap<Name, u64>,
    /// Async completions whose token is out: seq → attempt.
    pub awaiting: FxHashMap<Seq, u32>,
    /// Children not known to have finished, for the parent close policy:
    /// `(seq, workflow ID, workflow type, policy)`.
    kids: Vec<(Seq, Name, Name, u8)>,
    /// Search attribute objects in the order they were set.
    pub attrs: Vec<Payload>,
    /// The journal holds stream records (`Chunk`, `StreamEnd`), which a
    /// finished run's summary keeps.
    pub streams: bool,
    /// Timer and signal events for the run not taken by the host yet.
    queued: u32,
}

impl LoadedRun {
    pub fn is_done(&self) -> bool {
        self.done.is_some()
    }

    /// Whether an event for the run (a fired timer, an offered signal) is
    /// still queued for the host. Such a run is about to continue: parking
    /// it now would drop the fired timer, which only its event delivers.
    pub fn has_queued_events(&self) -> bool {
        self.queued > 0
    }

    /// Nothing can make progress without an outside event: every pending
    /// operation is a timer or a wait.
    pub fn armed_timers(&self) -> impl Iterator<Item = (Seq, TimerKind, u64)> + '_ {
        self.armed.iter().copied()
    }

    pub fn waiting_on(&self) -> impl Iterator<Item = (Seq, &str)> + '_ {
        self.waits.iter().map(|w| (w.seq, &*w.name))
    }

    pub fn pending_children(&self) -> &[Seq] {
        &self.children
    }

    pub fn buffered_signals(&self) -> usize {
        self.signals.len()
    }

    /// The accepted update with this ID, if any: its ordinal and slot.
    pub fn update(&self, id: &str) -> Option<(u64, &UpdateSlot)> {
        let o = *self.update_ids.get(id)?;
        Some((o, self.updates.get(o as usize)?))
    }
}

/// A parked run as its table entry describes it. Everything else (its last
/// LSN included) is in its frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParkedRun {
    offset: u64,
    len: u32,
    /// 0 for a suspended run, else 1 + the final status.
    done: u8,
}

impl ParkedRun {
    pub fn new(blob: BlobRef, done: Option<Status>) -> Self {
        Self {
            offset: blob.offset,
            len: blob.len,
            done: done.map(|s| 1 + s as u8).unwrap_or(0),
        }
    }

    fn of(e: &Entry) -> Self {
        Self::new(
            BlobRef {
                offset: e.offset(),
                len: e.len(),
            },
            e.done(),
        )
    }

    pub fn blob(&self) -> BlobRef {
        BlobRef {
            offset: self.offset,
            len: self.len,
        }
    }

    /// `Some` for finished runs, kept until retention ends.
    pub fn done(&self) -> Option<Status> {
        self.done
            .checked_sub(1)
            .and_then(|s| Status::from_u8(s).ok())
    }
}

/// What a run lookup by workflow ID found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Found {
    Loaded(RunId),
    /// The ID's hash matches a parked run; the host confirms the full ID from
    /// the blob when it matters.
    Parked(RunId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mode {
    /// Append the record to the run's in-memory journal.
    pub journal: bool,
    /// Queue outputs.
    pub outputs: bool,
    /// Emit events.
    pub events: bool,
}

impl Mode {
    pub const LIVE: Self = Self {
        journal: true,
        outputs: true,
        events: true,
    };
    /// Replaying the log tail at recovery: outputs are regenerated (they are
    /// idempotent at their receivers); the host restarts code itself.
    pub const RECOVER: Self = Self {
        journal: true,
        outputs: true,
        events: false,
    };
    /// Rebuilding a run from a stored blob.
    pub const BLOB: Self = Self {
        journal: false,
        outputs: false,
        events: false,
    };
    /// Rebuilding a run from journal bytes kept in a checkpoint.
    pub const SUFFIX: Self = Self {
        journal: true,
        outputs: false,
        events: false,
    };
}

pub struct ShardCore {
    pub shard: u32,
    pub shards: u32,
    pub retention_ms: u64,
    next_run: RunId,
    /// Boxed: a run's state is moved in and out of the map as it loads and
    /// parks, and a few hundred bytes moved each way (and on each rehash)
    /// were a tenth of a mass wake's copying.
    loaded: FxHashMap<RunId, Box<LoadedRun>>,
    /// Every run the shard knows, loaded or parked.
    runs: RunTable,
    /// Entries in a parked state.
    parked_count: usize,
    wheel: Wheel<TimerKey>,
    /// Parked runs whose earliest timer is within the horizon.
    near: Wheel<RunId>,
    /// The time buckets far timers wait in: per chunk of the table, the
    /// earliest time an entry in it waits for outside the wheels
    /// (`u64::MAX`: none). Lowered on every change, recomputed when the
    /// sweep walks the chunk, which it does once the time comes within the
    /// horizon. `far_min` is the earliest of them.
    far: Vec<u64>,
    far_min: u64,
    /// When the sweep last ran, and whether it must walk every chunk next
    /// time (after a compaction moved entries).
    sweep_at: u64,
    sweep_full: bool,
    /// Entries keep their slots: the host is writing an index base a chunk
    /// at a time and walks the slots in order (see [`hold_slots`](Self::hold_slots)).
    slots_held: bool,
    outputs: VecDeque<(Lsn, Output)>,
    events: VecDeque<Event>,
    retention: BinaryHeap<Reverse<(u64, RunId)>>,
    fired: Vec<TimerKey>,
    /// Scratch for the due timers of a run being loaded.
    due: Vec<TimerKey>,
    /// Runs that left memory, emptied, for the next ones (see
    /// [`LoadedRun::empty_for_reuse`]).
    // Boxes, not runs: each goes into `loaded` as it is, so a run's state
    // is never moved between the pool and the map.
    #[allow(clippy::vec_box)]
    spare: Vec<Box<LoadedRun>>,
    /// Due timers not fired yet, and the time they were found due.
    backlog: VecDeque<TimerKey>,
    backlog_at: u64,
    near_fired: Vec<RunId>,
    near_backlog: VecDeque<RunId>,
    near_backlog_at: u64,
    /// The persisted index: runs whose parked entry changed, and runs
    /// removed, since the index last recorded them.
    /// When either list would outgrow [`Self::index_list_cap`], they are
    /// dropped and the next index write is a whole new base instead.
    changed: Vec<RunId>,
    deleted: Vec<RunId>,
    index_rebase: bool,
    /// Bytes of loaded runs' in-memory journals (records after their
    /// stored prefix).
    journal_bytes: usize,
}

/// Timers due later than this wait in their run's table entry instead of a
/// wheel. A sweep walks the table once per half horizon.
pub const HORIZON_MS: u64 = 1 << 16;
/// While far timers wait, the host polls at least this often.
const SWEEP_MS: u64 = HORIZON_MS / 16;

/// At most this many due timers fire per [`ShardCore::poll_timers`]. The rest
/// stay due for the next call, so a mass wake is loaded, driven and flushed in
/// chunks instead of loading every run before the first one continues.
pub const TIMER_CHUNK: usize = 4096;
/// Slots per time bucket, as a power of two: a sweep walks the buckets
/// whose earliest far timer comes within the horizon.
const BUCKET_BITS: u32 = 12;

/// A shard's runs from the highest run number down: the newest start first,
/// since a shard numbers its runs in the order they start. (After a
/// reshard, runs that had to be renumbered count as newer than they are.)
///
/// Batches let the caller release the shard between them. The cursor
/// probes each number below the last one issued, so the newest `k` runs
/// cost about `k` lookups; once probing has cost two passes over the table
/// (old numbers mostly expired), it sorts the runs left instead. Runs
/// started after the cursor was made are not visited; a run removed
/// meanwhile may still be returned once.
#[derive(Debug)]
pub struct NewestFirst {
    /// Numbers at and above this were visited.
    below: RunId,
    probes: usize,
    /// Every run under `below`, ascending, once probing stopped paying.
    rest: Option<Vec<RunId>>,
}

impl NewestFirst {
    /// The next at most `n` runs.
    pub fn next_batch(&mut self, core: &ShardCore, n: usize) -> Vec<RunId> {
        let mut out = Vec::with_capacity(n.min(4096));
        let budget = core.runs.len().saturating_mul(2).saturating_add(1024);
        while out.len() < n {
            if let Some(rest) = &mut self.rest {
                match rest.pop() {
                    Some(r) => out.push(r),
                    None => break,
                }
                continue;
            }
            // Run numbers start at 1; 0 marks a free table slot.
            if self.below <= 1 {
                break;
            }
            if self.probes >= budget {
                let below = self.below;
                let mut rest: Vec<RunId> = core
                    .runs
                    .iter()
                    .map(|(_, e)| e.run())
                    .filter(|&r| r < below)
                    .collect();
                rest.sort_unstable();
                self.rest = Some(rest);
                continue;
            }
            self.below -= 1;
            self.probes += 1;
            if core.runs.has_run(self.below) {
                out.push(self.below);
            }
        }
        out
    }
}

/// The shard a workflow ID belongs to.
pub fn shard_of(id: &str, shards: u32) -> u32 {
    (hash_id(id) % u64::from(shards.max(1))) as u32
}

/// Dedup key for a signal sent by a workflow, stable across resends. Never
/// 0, which means "no key".
pub fn send_dedup(shard: u32, run: RunId, seq: Seq) -> u64 {
    let mut key = [0u8; 16];
    key[..4].copy_from_slice(&shard.to_le_bytes());
    key[4..12].copy_from_slice(&run.to_le_bytes());
    key[12..].copy_from_slice(&seq.to_le_bytes());
    checksum64(&[&key]) | 1
}

impl ShardCore {
    pub fn new(shard: u32, shards: u32, retention_ms: u64, now_ms: u64) -> Self {
        Self {
            shard,
            shards,
            retention_ms,
            next_run: 1,
            loaded: FxHashMap::default(),
            runs: RunTable::new(),
            parked_count: 0,
            wheel: Wheel::new(now_ms),
            near: Wheel::new(now_ms),
            far: Vec::new(),
            far_min: u64::MAX,
            // As if a sweep ran one interval ago: timers that came due while
            // the shard was closed fire at its first poll, not one interval
            // (4 s) later.
            sweep_at: now_ms.saturating_sub(SWEEP_MS),
            sweep_full: false,
            slots_held: false,
            outputs: VecDeque::new(),
            events: VecDeque::new(),
            retention: BinaryHeap::new(),
            fired: Vec::new(),
            due: Vec::new(),
            spare: Vec::new(),
            backlog: VecDeque::new(),
            backlog_at: 0,
            near_fired: Vec::new(),
            near_backlog: VecDeque::new(),
            near_backlog_at: 0,
            changed: Vec::new(),
            deleted: Vec::new(),
            index_rebase: false,
            journal_bytes: 0,
        }
    }

    /// The time an entry waits for outside the wheels, if any: a suspended
    /// run's timer that is not in a wheel yet.
    fn waits_far(e: &Entry) -> Option<u64> {
        let t = e.time();
        let waits = t != 0
            && !e.has(flag::NEAR)
            && !e.has(flag::PENDING)
            && (e.state() == table::LOADED || e.state() == table::PARKED);
        waits.then_some(t)
    }

    /// Keeps the parked count and the entry's time bucket.
    fn noted(&mut self, slot: Slot, was_parked: bool) {
        let e = self.runs.get(slot);
        let parked = e.is_parked();
        self.parked_count = self.parked_count + usize::from(parked) - usize::from(was_parked);
        if let Some(t) = Self::waits_far(e) {
            let c = (slot >> BUCKET_BITS) as usize;
            if self.far.len() <= c {
                self.far.resize(c + 1, u64::MAX);
            }
            self.far[c] = self.far[c].min(t);
            self.far_min = self.far_min.min(t);
        }
    }

    /// Changes an entry, keeping the parked count and its time bucket.
    fn edit(&mut self, slot: Slot, f: impl FnOnce(&mut Entry)) {
        let was = self.runs.get(slot).is_parked();
        f(self.runs.get_mut(slot));
        self.noted(slot, was);
    }

    fn table_insert(&mut self, e: Entry) -> Slot {
        let slot = self.runs.insert(e);
        self.noted(slot, false);
        slot
    }

    fn table_remove(&mut self, slot: Slot) {
        let e = self.runs.remove(slot);
        self.parked_count -= usize::from(e.is_parked());
        if !self.index_rebase {
            self.deleted.push(e.run());
            self.check_index_lists();
        }
    }

    fn mark_changed(&mut self, slot: Slot) {
        let e = self.runs.get_mut(slot);
        if !e.has(flag::DIRTY) {
            e.set(flag::DIRTY, true);
            let run = e.run();
            if !self.index_rebase {
                self.changed.push(run);
                self.check_index_lists();
            }
        }
    }

    /// Most runs the change lists hold before the next index write is a
    /// whole base: an eighth of the parked runs (a delta that large costs
    /// at least an eighth of a base anyway), and at least 64 Ki.
    fn index_list_cap(&self) -> usize {
        (self.parked_count / 8).max(1 << 16)
    }

    fn check_index_lists(&mut self) {
        if self.changed.len() + self.deleted.len() > self.index_list_cap() {
            self.index_rebase = true;
            self.changed = Vec::new();
            self.deleted = Vec::new();
        }
    }

    /// The run's entry, as a loaded run, found by ID hash unless a later
    /// run of the same ID holds the place.
    fn note_loaded(&mut self, run: RunId, id_hash: u64) {
        let slot = match self.runs.find_run(run) {
            Some(slot) => {
                self.edit(slot, |e| {
                    e.set_state(table::LOADED);
                    e.set_time(0);
                    e.set(flag::NEAR | flag::PENDING, false);
                });
                slot
            }
            None => self.table_insert(Entry::new(run, id_hash, table::LOADED)),
        };
        self.runs.index_id(slot);
    }

    pub fn next_run_id(&self) -> RunId {
        self.next_run
    }

    pub fn set_next_run_id(&mut self, next: RunId) {
        self.next_run = self.next_run.max(next);
    }

    /// Allocates a run ID for a `Start` record about to be appended.
    pub fn allocate_run(&mut self) -> RunId {
        let r = self.next_run;
        self.next_run += 1;
        r
    }

    pub fn find(&self, id: &str) -> Option<Found> {
        let e = self.runs.get(self.runs.find_id(hash_id(id))?);
        let run = e.run();
        if !e.is_parked() {
            let l = self.loaded.get(&run)?;
            return (*l.id == *id).then_some(Found::Loaded(run));
        }
        Some(Found::Parked(run))
    }

    pub fn get(&self, run: RunId) -> Option<&LoadedRun> {
        self.loaded.get(&run).map(|l| &**l)
    }

    fn parked_entry(&self, run: RunId) -> Option<&Entry> {
        let e = self.runs.get(self.runs.find_run(run)?);
        e.is_parked().then_some(e)
    }

    pub fn parked(&self, run: RunId) -> Option<ParkedRun> {
        self.parked_entry(run).map(ParkedRun::of)
    }

    pub fn is_parked(&self, run: RunId) -> bool {
        self.parked_entry(run).is_some()
    }

    /// Parked runs whose frame is a `Delta` (compaction rewrites them whole).
    pub fn chained_runs(&self) -> impl Iterator<Item = (RunId, BlobRef)> + '_ {
        self.runs
            .iter()
            .filter(|(_, e)| e.is_parked() && e.has(flag::CHAINED))
            .map(|(_, e)| {
                (
                    e.run(),
                    BlobRef {
                        offset: e.offset(),
                        len: e.len(),
                    },
                )
            })
    }

    /// A parked run's frame was rewritten whole at `blob` (before compaction).
    pub fn reparked(&mut self, run: RunId, blob: BlobRef) {
        if let Some(slot) = self.runs.find_run(run)
            && self.runs.get(slot).is_parked()
        {
            self.runs.get_mut(slot).set_location(blob.offset, blob.len);
            self.runs.get_mut(slot).set(flag::CHAINED, false);
            self.mark_changed(slot);
        }
    }

    pub fn loaded_runs(&self) -> impl Iterator<Item = (RunId, &LoadedRun)> {
        self.loaded.iter().map(|(k, v)| (*k, &**v))
    }

    pub fn parked_runs(&self) -> impl Iterator<Item = (RunId, ParkedRun)> + '_ {
        self.runs
            .iter()
            .filter(|(_, e)| e.is_parked())
            .map(|(_, e)| (e.run(), ParkedRun::of(e)))
    }

    /// A cursor over every run, loaded or parked, newest first (see
    /// [`NewestFirst`]).
    pub fn newest_first(&self) -> NewestFirst {
        NewestFirst {
            below: self.next_run,
            probes: 0,
            rest: None,
        }
    }

    pub fn loaded_len(&self) -> usize {
        self.loaded.len()
    }

    pub fn parked_len(&self) -> usize {
        self.parked_count
    }

    pub fn timers_len(&self) -> usize {
        self.wheel.len() + self.backlog.len() + self.near.len() + self.near_backlog.len()
    }

    /// Heap bytes of the run table, its indexes and the near wheel's items.
    pub fn table_bytes(&self) -> usize {
        self.runs.heap_bytes() + self.retention.capacity() * 16 + self.near.len() * 16
    }

    /// Whether a signal with this dedup key was already accepted.
    pub fn seen_signal(&self, run: RunId, dedup: u64) -> bool {
        dedup != 0
            && self
                .loaded
                .get(&run)
                .is_some_and(|l| l.dedup.contains(&dedup))
    }

    /// Queue an output for the record at `lsn` (the most recent append).
    pub fn add_output(&mut self, lsn: Lsn, output: Output) {
        self.outputs.push_back((lsn, output));
    }

    /// Outputs whose records are durable through `durable`.
    pub fn release(&mut self, durable: Lsn, out: &mut Vec<Output>) {
        while let Some((lsn, _)) = self.outputs.front() {
            if *lsn > durable {
                break;
            }
            if let Some((_, o)) = self.outputs.pop_front() {
                out.push(o);
            }
        }
    }

    pub fn pending_outputs(&self) -> usize {
        self.outputs.len()
    }

    /// Outputs not yet released (their records are not yet durable).
    pub fn unreleased(&self) -> impl Iterator<Item = &Output> {
        self.outputs.iter().map(|(_, o)| o)
    }

    pub fn take_event(&mut self) -> Option<Event> {
        let ev = self.events.pop_front()?;
        if let Event::Timer { run, .. } | Event::SignalReady { run, .. } = &ev
            && let Some(l) = self.loaded.get_mut(run)
        {
            l.queued = l.queued.saturating_sub(1);
        }
        Some(ev)
    }

    pub fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    /// When the host should next call [`poll_timers`](Self::poll_timers).
    pub fn next_timer(&self) -> Option<u64> {
        if !self.backlog.is_empty() {
            return Some(self.backlog_at);
        }
        if !self.near_backlog.is_empty() {
            return Some(self.near_backlog_at);
        }
        let sweep = if self.sweep_full {
            Some(self.sweep_at)
        } else {
            // The first time a sweep moves something: its horizon is
            // `[now, now + HORIZON_MS)`, so `far_min` enters it one
            // millisecond after `far_min - HORIZON_MS`.
            (self.far_min != u64::MAX).then(|| {
                self.far_min
                    .saturating_sub(HORIZON_MS - 1)
                    .max(self.sweep_at.saturating_add(SWEEP_MS))
            })
        };
        // An empty wheel (the usual case for runs that do not sleep) has
        // no next wake; asking would walk all of its levels.
        fn wake<K: Copy>(w: &Wheel<K>) -> Option<u64> {
            if w.is_empty() { None } else { w.next_wake() }
        }
        [wake(&self.wheel), wake(&self.near), sweep]
            .into_iter()
            .flatten()
            .min()
    }

    /// Moves timers that came within the horizon from table entries into
    /// the wheels: walks the chunks whose bucket time is within it (at most
    /// once per [`SWEEP_MS`], and only once something there comes due).
    fn sweep(&mut self, now_ms: u64) {
        if !self.sweep_full
            && (now_ms < self.sweep_at.saturating_add(SWEEP_MS)
                || self.far_min >= now_ms.saturating_add(HORIZON_MS))
        {
            return;
        }
        if self.sweep_full {
            // Entries moved: every chunk is walked.
            let chunks = (self.runs.used() as usize).div_ceil(1 << BUCKET_BITS);
            self.far = vec![0; chunks];
            self.sweep_full = false;
        }
        self.sweep_at = now_ms;
        let end = now_ms.saturating_add(HORIZON_MS);
        let used = self.runs.used();
        let mut far_min = u64::MAX;
        for c in 0..self.far.len() {
            if self.far[c] >= end {
                far_min = far_min.min(self.far[c]);
                continue;
            }
            let mut min = u64::MAX;
            let from = (c as u32) << BUCKET_BITS;
            let to = from.saturating_add(1 << BUCKET_BITS).min(used);
            for slot in from..to {
                let Some(e) = self.runs.live_at(slot) else {
                    continue;
                };
                let Some(t) = Self::waits_far(e) else {
                    continue;
                };
                if t >= end {
                    min = min.min(t);
                    continue;
                }
                let (run, state) = (e.run(), e.state());
                if state == table::PARKED {
                    self.near.insert(t, run);
                    self.runs.get_mut(slot).set(flag::NEAR, true);
                } else {
                    self.rearm(run, slot, end);
                    if let Some(t) = Self::waits_far(self.runs.get(slot)) {
                        min = min.min(t);
                    }
                }
            }
            self.far[c] = min;
            far_min = far_min.min(min);
        }
        self.far_min = far_min;
    }

    /// The end of the horizon, from the latest time the core has seen (the
    /// wheels advance only when polled).
    fn horizon_end(&self) -> u64 {
        self.wheel
            .elapsed()
            .max(self.near.elapsed())
            .max(self.sweep_at)
            .saturating_add(HORIZON_MS)
    }

    /// A loaded run's timers due before `end` go into the wheel; its entry
    /// keeps the earliest later one.
    fn rearm(&mut self, run_id: RunId, slot: Slot, end: u64) {
        let mut later = 0u64;
        if let Some(run) = self.loaded.get(&run_id) {
            for &(seq, kind, at) in &run.armed {
                if at < end {
                    self.wheel.insert(
                        at,
                        TimerKey {
                            run: run_id,
                            seq,
                            kind,
                        },
                    );
                } else if later == 0 || at < later {
                    later = at;
                }
            }
        }
        self.edit(slot, |e| e.set_time(later));
    }

    /// Fires due timers as events, at most [`TIMER_CHUNK`] per call.
    pub fn poll_timers(&mut self, now_ms: u64) {
        self.sweep(now_ms);
        if self.near_backlog.is_empty() {
            let mut fired = std::mem::take(&mut self.near_fired);
            fired.clear();
            self.near.poll(now_ms, &mut fired);
            self.near_backlog.extend(fired.drain(..));
            self.near_fired = fired;
            self.near_backlog_at = now_ms;
        }
        for _ in 0..TIMER_CHUNK {
            let Some(run) = self.near_backlog.pop_front() else {
                break;
            };
            self.fire_parked(run, now_ms, true);
        }
        if self.backlog.is_empty() {
            let mut fired = std::mem::take(&mut self.fired);
            fired.clear();
            self.wheel.poll(now_ms, &mut fired);
            self.backlog.extend(fired.drain(..));
            self.fired = fired;
            self.backlog_at = now_ms;
        }
        for _ in 0..TIMER_CHUNK {
            let Some(key) = self.backlog.pop_front() else {
                break;
            };
            self.fire(key, now_ms);
        }
    }

    /// The next event without taking it.
    pub fn peek_event(&self) -> Option<&Event> {
        self.events.front()
    }

    /// A timer of a parked run is due: ask for a load, once. `near`: the
    /// near wheel's item, which stands for the entry's time.
    fn fire_parked(&mut self, run: RunId, now_ms: u64, near: bool) {
        let Some(slot) = self.runs.find_run(run) else {
            return;
        };
        let e = self.runs.get_mut(slot);
        // A finished run's leftover timer, a load already asked for, or a
        // timer no longer armed (the entry has its earliest armed one).
        if e.state() != table::PARKED
            || e.has(flag::PENDING)
            || e.time() == 0
            || e.time() > now_ms
            || (near && !e.has(flag::NEAR))
        {
            return;
        }
        e.set(flag::NEAR, false);
        e.set(flag::PENDING, true);
        self.events.push_back(Event::NeedLoad { run });
    }

    fn fire(&mut self, key: TimerKey, now_ms: u64) {
        let Some(run) = self.loaded.get_mut(&key.run) else {
            self.fire_parked(key.run, now_ms, false);
            return;
        };
        if run.done.is_some() {
            return;
        }
        let Some(pos) = run
            .armed
            .iter()
            .position(|(s, k, at)| *s == key.seq && *k == key.kind && *at <= now_ms)
        else {
            return;
        };
        run.armed.swap_remove(pos);
        run.queued += 1;
        self.events.push_back(Event::Timer {
            run: key.run,
            seq: key.seq,
            kind: key.kind,
        });
    }

    fn arm(&mut self, run: RunId, seq: Seq, kind: TimerKind, at: u64) {
        if let Some(l) = self.loaded.get_mut(&run)
            && !l
                .armed
                .iter()
                .any(|(s, k, a)| *s == seq && *k == kind && *a == at)
        {
            l.armed.push((seq, kind, at));
        }
        self.schedule(run, seq, kind, at);
    }

    /// A loaded run's armed timer into the wheel if it is due within the
    /// horizon, else into its entry for the sweep.
    fn schedule(&mut self, run: RunId, seq: Seq, kind: TimerKind, at: u64) {
        if at < self.horizon_end() {
            self.wheel.insert(at, TimerKey { run, seq, kind });
        } else if let Some(slot) = self.runs.find_run(run) {
            let t = self.runs.get(slot).time();
            if !self.runs.get(slot).is_parked() && (t == 0 || at < t) {
                self.edit(slot, |e| e.set_time(at));
            }
        }
    }

    fn disarm(run: &mut LoadedRun, seq: Seq, kind: TimerKind) {
        run.armed.retain(|(s, k, _)| !(*s == seq && *k == kind));
    }

    fn offer(&mut self, run_id: RunId, events: bool) {
        let Some(run) = self.loaded.get_mut(&run_id) else {
            return;
        };
        for w in run.waits.iter_mut() {
            if w.offered.is_some() {
                continue;
            }
            if let Some(sig) = run
                .signals
                .iter_mut()
                .find(|s| s.offered.is_none() && s.name == w.name)
            {
                sig.offered = Some(w.seq);
                w.offered = Some(sig.ordinal);
                if events {
                    run.queued += 1;
                    self.events.push_back(Event::SignalReady {
                        run: run_id,
                        seq: w.seq,
                        ordinal: sig.ordinal,
                        payload: sig.payload.clone(),
                    });
                }
            }
        }
    }

    /// Re-offers buffered signals to pending waits after recovery or load.
    pub fn reoffer(&mut self, run_id: RunId) {
        if let Some(run) = self.loaded.get_mut(&run_id) {
            for w in run.waits.iter_mut() {
                w.offered = None;
            }
            for s in run.signals.iter_mut() {
                s.offered = None;
            }
        }
        self.offer(run_id, true);
    }

    /// Applies one record at `lsn`.
    pub fn apply<S: ToName, B: ToPayload>(&mut self, lsn: Lsn, rec: &Record<S, B>, mode: Mode) {
        self.apply_with(lsn, rec, mode, None);
    }

    /// [`Self::apply`] for a record whose canonical encoding
    /// ([`crate::record::encode_canonical`]) the host already has: the
    /// journal copies it instead of encoding the record again.
    pub fn apply_encoded<S: ToName, B: ToPayload>(
        &mut self,
        lsn: Lsn,
        rec: &Record<S, B>,
        mode: Mode,
        canonical: &[u8],
    ) {
        self.apply_with(lsn, rec, mode, Some(canonical));
    }

    fn apply_with<S: ToName, B: ToPayload>(
        &mut self,
        lsn: Lsn,
        rec: &Record<S, B>,
        mode: Mode,
        canonical: Option<&[u8]>,
    ) {
        let run_id = rec.run();
        if let Record::Start {
            run,
            id,
            wtype,
            version,
            codec,
            started_ms,
            deadline_ms,
            parent,
            policy,
            from,
            copies,
            update,
            ..
        } = rec
        {
            if self.loaded.contains_key(run) {
                // Already built: a handover `Done` created it, and recovery
                // meets its `Start` again in the run's own journal.
                return;
            }
            let id: Name = id.to_name();
            self.next_run = self.next_run.max(run + 1);
            // The latest run for an ID wins (loading an older run back, for
            // example to read it, must not hide a newer one).
            self.note_loaded(*run, hash_id(&id));
            // A pooled run's box and containers, emptied (see `recycle`).
            let mut l = self
                .spare
                .pop()
                .unwrap_or_else(|| Box::new(LoadedRun::empty()));
            if let Some(u) = update {
                let id = u.id.to_name();
                l.update_ids.insert(id.clone(), 0);
                l.updates.push(UpdateSlot {
                    id,
                    name: u.name.to_name(),
                    caller: None,
                    result: None,
                });
            }
            l.id = id;
            l.wtype = wtype.to_name();
            l.version = *version;
            l.codec = *codec;
            l.parent = *parent;
            l.started_ms = *started_ms;
            l.deadline_ms = *deadline_ms;
            l.last_lsn = lsn;
            l.done = None;
            l.result = None;
            l.blob = None;
            l.blob_chain = 0;
            l.blob_depth = 0;
            l.journal_records = 0;
            l.signal_count = 0;
            l.policy = *policy;
            l.from = *from;
            l.copies_left = *copies;
            l.streams = false;
            l.queued = 0;
            self.loaded.insert(*run, l);
            if *deadline_ms > 0 {
                self.arm(*run, 0, TimerKind::Deadline, *deadline_ms);
            }
            if mode.events {
                self.events.push_back(Event::Started { run: *run });
            }
        }
        let shard = self.shard;
        let Some(run) = self.loaded.get_mut(&run_id) else {
            return;
        };
        run.last_lsn = run.last_lsn.max(lsn);
        if mode.journal {
            let before = run.journal.len();
            match canonical {
                Some(bytes) => run.journal.extend_from_slice(bytes),
                None => crate::record::encode_canonical(rec, &mut run.journal),
            }
            self.journal_bytes += run.journal.len() - before;
            run.journal_records += 1;
        }
        // Records copied from a reset run's journal already sent their
        // messages once.
        let copied = !matches!(rec, Record::Start { .. }) && run.copies_left > 0;
        if copied {
            run.copies_left -= 1;
        }
        match rec {
            Record::Start { .. } => {}
            Record::Step { seq, .. } => Self::disarm(run, *seq, TimerKind::Retry),
            // A paused step waits for a resume, which wakes the run.
            Record::Attempt {
                seq,
                retry_at_ms: PAUSED,
                ..
            }
            | Record::Resume { seq, .. } => Self::disarm(run, *seq, TimerKind::Retry),
            Record::Attempt {
                seq, retry_at_ms, ..
            } => {
                let (seq, at) = (*seq, *retry_at_ms);
                self.arm(run_id, seq, TimerKind::Retry, at);
            }
            Record::Timer {
                seq, fire_at_ms, ..
            } => {
                let (seq, at) = (*seq, *fire_at_ms);
                self.arm(run_id, seq, TimerKind::Sleep, at);
            }
            Record::Fired { seq, .. } => Self::disarm(run, *seq, TimerKind::Sleep),
            Record::Signal {
                name,
                dedup,
                payload,
                ..
            } => {
                let ordinal = run.signal_count;
                run.signal_count += 1;
                if *dedup != 0 {
                    run.dedup.insert(*dedup);
                }
                // Shared with the record when it owns them (no copy).
                run.signals.push_back(SignalSlot {
                    ordinal,
                    name: name.to_name(),
                    payload: payload.to_payload(),
                    offered: None,
                });
                self.offer(run_id, mode.events);
            }
            Record::Wait {
                seq,
                name,
                deadline_ms,
                ..
            } => {
                run.waits.push(WaitSlot {
                    seq: *seq,
                    name: name.to_name(),
                    offered: None,
                });
                let (seq, at) = (*seq, *deadline_ms);
                if at > 0 {
                    self.arm(run_id, seq, TimerKind::WaitTimeout, at);
                }
                self.offer(run_id, mode.events);
            }
            Record::Waited { seq, outcome, .. } => {
                Self::disarm(run, *seq, TimerKind::WaitTimeout);
                let offered = run
                    .waits
                    .iter()
                    .position(|w| w.seq == *seq)
                    .map(|i| run.waits.remove(i).offered);
                match outcome {
                    WaitOutcome::Signal(ordinal) => {
                        // Ordinals are unique, and the one consumed is
                        // usually the oldest: found and removed at the front.
                        if let Some(i) = run.signals.iter().position(|s| s.ordinal == *ordinal) {
                            run.signals.remove(i);
                        }
                        // If this wait had been offered a different signal,
                        // that one is free again.
                        if let Some(Some(o)) = offered
                            && o != *ordinal
                            && let Some(s) = run.signals.iter_mut().find(|s| s.ordinal == o)
                        {
                            s.offered = None;
                        }
                    }
                    WaitOutcome::TimedOut | WaitOutcome::Abandoned => {
                        if let Some(Some(o)) = offered
                            && let Some(s) = run.signals.iter_mut().find(|s| s.ordinal == o)
                        {
                            s.offered = None;
                        }
                    }
                }
                self.offer(run_id, mode.events);
            }
            Record::Value { .. } => {}
            Record::Child {
                seq,
                wtype,
                id,
                flags,
                deadline_ms,
                input,
                ..
            } => {
                if flags & policy::DETACHED == 0 {
                    run.children.push(*seq);
                }
                if !run.kids.iter().any(|k| k.0 == *seq) {
                    run.kids.push((*seq, id.to_name(), wtype.to_name(), *flags));
                }
                if mode.outputs {
                    let parent = ParentRef {
                        shard,
                        run: run_id,
                        seq: *seq,
                    };
                    self.outputs.push_back((
                        lsn,
                        Output::ChildStart {
                            parent,
                            wtype: wtype.to_name(),
                            id: id.to_name(),
                            flags: *flags,
                            deadline_ms: *deadline_ms,
                            input: input.to_payload(),
                        },
                    ));
                }
            }
            Record::ChildDone { seq, .. } => {
                run.children.retain(|s| s != seq);
                run.kids.retain(|k| k.0 != *seq);
            }
            Record::Send {
                seq,
                target,
                name,
                payload,
                ..
            } => {
                if mode.outputs && !copied {
                    let target: Name = target.to_name();
                    let out = if name.as_ref() == CANCEL_SIGNAL {
                        Output::Close {
                            target,
                            wtype: Arc::from(""),
                            parent: None,
                            terminate: false,
                            reason: payload.to_payload(),
                        }
                    } else {
                        Output::Signal {
                            target,
                            name: name.to_name(),
                            payload: payload.to_payload(),
                            dedup: send_dedup(shard, run_id, *seq),
                        }
                    };
                    self.outputs.push_back((lsn, out));
                }
            }
            Record::Cancel { .. } => {}
            Record::Update {
                id, name, caller, ..
            } => {
                let ordinal = run.updates.len() as u64;
                let id: Name = id.to_name();
                run.update_ids.insert(id.clone(), ordinal);
                run.updates.push(UpdateSlot {
                    id,
                    name: name.to_name(),
                    caller: *caller,
                    result: None,
                });
            }
            Record::UpdateDone {
                ordinal,
                status,
                payload,
                ..
            } => {
                let payload: Payload = payload.to_payload();
                if let Some(slot) = run.updates.get_mut(*ordinal as usize) {
                    slot.result = Some((*status, payload.clone(), lsn));
                    if mode.outputs {
                        let id = slot.id.clone();
                        // A workflow that called an object gets the result
                        // as a child result, sent again after a crash from
                        // this record.
                        if let (Some(parent), false) = (slot.caller, copied) {
                            self.outputs.push_back((
                                lsn,
                                Output::ChildResult {
                                    parent,
                                    status: *status,
                                    payload: payload.clone(),
                                },
                            ));
                        }
                        self.outputs.push_back((
                            lsn,
                            Output::UpdateResult {
                                run: run_id,
                                ordinal: *ordinal,
                                id,
                                status: *status,
                                payload,
                            },
                        ));
                    }
                }
            }
            Record::Heartbeat { .. } | Record::Patch { .. } => {}
            Record::Awaiting { seq, attempt, .. } => {
                run.awaiting.insert(*seq, *attempt);
            }
            Record::Completed { seq, .. } => {
                run.awaiting.remove(seq);
            }
            Record::Attributes { payload, .. } => run.attrs.push(payload.to_payload()),
            Record::Chunk { .. } | Record::StreamEnd { .. } => run.streams = true,
            Record::Done {
                status,
                finished_ms,
                payload,
                ..
            } => {
                if run.done.is_some() {
                    return;
                }
                if status.is_handover() {
                    // The payload is the next run's `Start`: ending this run
                    // and starting that one is one record.
                    let next = decode_canonical_all(payload.as_ref())
                        .ok()
                        .and_then(|mut v| (v.len() == 1).then(|| v.remove(0)))
                        .filter(|r| matches!(r, Record::Start { .. }));
                    let (status, finished) = (*status, *finished_ms);
                    self.close_handover(lsn, run_id, status, finished, mode);
                    // Only a live append or the log tail creates the next
                    // run. Rebuilding this run from a stored journal or a
                    // checkpoint (or loading it back to deliver a late
                    // message) finds the next run in the store or the
                    // checkpoint already, perhaps finished since.
                    if let Some(next) = next
                        && mode.outputs
                        && self.runs.find_run(next.run()).is_none()
                    {
                        self.apply(lsn, &next, mode);
                    }
                    return;
                }
                run.done = Some((*status, *finished_ms));
                // Shared with the record when it owns one (no copy).
                let payload: Payload = payload.to_payload();
                run.result = Some(payload.clone());
                run.armed.clear();
                run.waits.clear();
                run.children.clear();
                run.awaiting.clear();
                let parent = run.parent;
                let detached = run.policy & policy::DETACHED != 0;
                let kids = std::mem::take(&mut run.kids);
                self.retention.push(Reverse((*finished_ms, run_id)));
                if mode.outputs {
                    self.outputs.push_back((
                        lsn,
                        Output::Result {
                            run: run_id,
                            status: *status,
                            payload: payload.clone(),
                        },
                    ));
                    if let (Some(parent), false) = (parent, detached) {
                        self.outputs.push_back((
                            lsn,
                            Output::ChildResult {
                                parent,
                                status: *status,
                                payload,
                            },
                        ));
                    }
                    self.close_children(lsn, run_id, kids);
                }
                if mode.events {
                    self.events.push_back(Event::Finished { run: run_id });
                }
            }
        }
    }

    /// Children follow their parent close policy when the parent closes.
    fn close_children(&mut self, lsn: Lsn, run: RunId, kids: Vec<(Seq, Name, Name, u8)>) {
        for (seq, id, wtype, flags) in kids {
            let terminate = match flags & policy::CLOSE_MASK {
                policy::CLOSE_ABANDON => continue,
                policy::CLOSE_CANCEL => false,
                _ => true,
            };
            self.outputs.push_back((
                lsn,
                Output::Close {
                    target: id,
                    wtype,
                    parent: Some(ParentRef {
                        shard: self.shard,
                        run,
                        seq,
                    }),
                    terminate,
                    reason: Arc::from(&b"parent closed"[..]),
                },
            ));
        }
    }

    /// Ends a run that handed over to a newer run of its workflow ID
    /// (continue-as-new or reset). The newer run inherits its parent, so no
    /// child result is sent; callers waiting on this run follow the ID.
    fn close_handover(&mut self, lsn: Lsn, run_id: RunId, status: Status, at: u64, mode: Mode) {
        let Some(run) = self.loaded.get_mut(&run_id) else {
            return;
        };
        if run.done.is_some() {
            return;
        }
        run.last_lsn = run.last_lsn.max(lsn);
        run.done = Some((status, at));
        run.result = Some(Arc::from(&[][..]));
        run.armed.clear();
        run.waits.clear();
        run.children.clear();
        run.awaiting.clear();
        let kids = std::mem::take(&mut run.kids);
        self.retention.push(Reverse((at, run_id)));
        if mode.outputs {
            self.outputs.push_back((
                lsn,
                Output::Result {
                    run: run_id,
                    status,
                    payload: Arc::from(&[][..]),
                },
            ));
            if status == Status::ContinuedAsNew {
                self.close_children(lsn, run_id, kids);
            }
        }
        if mode.events {
            self.events.push_back(Event::Finished { run: run_id });
        }
    }

    /// Moves a loaded run to the parked index. The host has written its
    /// journal to `blob` (`chained`: a `Delta` frame extending earlier ones).
    pub fn park(&mut self, run_id: RunId, blob: BlobRef, chained: bool) -> Option<ParkedRun> {
        let run = self.loaded.remove(&run_id)?;
        self.journal_bytes = self.journal_bytes.saturating_sub(run.journal.len());
        let (state, time) = match run.done {
            Some((status, finished)) => (table::DONE + status as u8, finished),
            None => (
                table::PARKED,
                run.armed.iter().map(|a| a.2).min().unwrap_or(0),
            ),
        };
        let slot = match self.runs.find_run(run_id) {
            Some(slot) => slot,
            None => {
                let slot = self.table_insert(Entry::new(run_id, hash_id(&run.id), state));
                self.runs.index_id(slot);
                slot
            }
        };
        // A timer within the horizon goes into the near wheel; a later one
        // waits in the entry's time bucket.
        let near = state == table::PARKED && time != 0 && time < self.horizon_end();
        self.edit(slot, |e| {
            e.set_state(state);
            e.set_location(blob.offset, blob.len);
            e.set_time(time);
            e.set(flag::PENDING, false);
            e.set(flag::NEAR, near);
            e.set(flag::CHAINED, chained);
        });
        self.mark_changed(slot);
        if near {
            self.near.insert(time, run_id);
        }
        let parked = ParkedRun::new(blob, run.done.map(|d| d.0));
        self.recycle(run);
        Some(parked)
    }

    /// Frees spare containers beyond `keep`: a shard that loads and starts
    /// nothing keeps few, so a large parked population costs no pool.
    /// Returns how many it freed.
    pub fn shrink_spare(&mut self, keep: usize) -> usize {
        let freed = self.spare.len().saturating_sub(keep);
        if freed > 0 {
            self.spare.truncate(keep);
            self.spare.shrink_to(keep);
        }
        freed
    }

    /// Keeps the containers of a run that left memory for the next run
    /// loaded or started (see [`LoadedRun::empty_for_reuse`]).
    fn recycle(&mut self, mut run: Box<LoadedRun>) {
        if self.spare.len() < SPARE_RUNS && run.empty_for_reuse() {
            self.spare.push(run);
        }
    }

    /// Restores a parked run (run store scan or persisted index at
    /// recovery). `time`: its earliest timer, or when it finished.
    pub fn insert_parked(
        &mut self,
        run: RunId,
        blob: BlobRef,
        done: Option<Status>,
        chained: bool,
        id_hash: u64,
        time: u64,
    ) {
        self.next_run = self.next_run.max(run + 1);
        if self.loaded.contains_key(&run) {
            return;
        }
        let state = match done {
            Some(s) => table::DONE + s as u8,
            None => table::PARKED,
        };
        let slot = match self.runs.find_run(run) {
            Some(slot) => slot,
            None => self.table_insert(Entry::new(run, id_hash, state)),
        };
        self.edit(slot, |e| {
            e.set_state(state);
            e.set_location(blob.offset, blob.len);
            e.set_time(time);
            e.set(flag::CHAINED, chained);
        });
        self.runs.index_id(slot);
        if done.is_some() {
            self.retention.push(Reverse((time, run)));
        }
    }

    /// Applies persisted index records (see [`table::Entry::record`]): each
    /// sets a parked run, or removes it. Recovery reads the index frames
    /// named by the checkpoint this way, oldest first.
    pub fn load_index(&mut self, records: &[u8]) -> Result<(), &'static str> {
        let (records, trailing) = records.as_chunks::<{ table::RECORD }>();
        if !trailing.is_empty() {
            return Err("index frame is not whole records");
        }
        for r in records {
            let e = Entry::from_record(r).ok_or("bad index record")?;
            let run = e.run();
            if e.state() == table::DELETED {
                if let Some(slot) = self.runs.find_run(run) {
                    self.table_remove(slot);
                }
                continue;
            }
            // Every run in the table is below `next_run`: a base, in slot
            // order, mostly lists runs above it and needs no lookup.
            let known = run < self.next_run;
            self.next_run = self.next_run.max(run + 1);
            let found = if known { self.runs.find_run(run) } else { None };
            let slot = match found {
                Some(slot) => {
                    self.edit(slot, |x| {
                        let indexed = x.has(flag::IN_ID);
                        *x = e;
                        x.set(flag::IN_ID, indexed);
                    });
                    slot
                }
                None => self.table_insert(e),
            };
            self.runs.index_id(slot);
        }
        self.deleted.clear();
        Ok(())
    }

    /// Makes room for `n` more runs (before loading an index).
    pub fn reserve_runs(&mut self, n: usize) {
        self.runs.reserve(n);
    }

    /// After [`load_index`](Self::load_index): rebuilds the retention queue.
    pub fn index_loaded(&mut self) {
        let done: Vec<(u64, RunId)> = self
            .runs
            .iter()
            .filter(|(_, e)| e.done().is_some())
            .map(|(_, e)| (e.time(), e.run()))
            .collect();
        self.retention.extend(done.into_iter().map(Reverse));
    }

    /// Starts a new base of the persisted index: clears the change lists.
    /// [`index_snapshot`](Self::index_snapshot) then lists every parked run.
    pub fn begin_index_snapshot(&mut self) {
        self.changed = Vec::new();
        self.deleted = Vec::new();
        self.index_rebase = false;
    }

    /// Index records for up to `max` parked runs from slot `from` on, for a
    /// new base. Returns the slot to continue from (`None`: done).
    pub fn index_snapshot(&mut self, from: Slot, max: usize, out: &mut Vec<u8>) -> Option<Slot> {
        let mut n = 0;
        for slot in from..self.runs.used() {
            if n == max {
                return Some(slot);
            }
            let e = self.runs.get_mut(slot);
            if e.run() == 0 {
                continue;
            }
            // Every entry is recorded as it is now (the change lists may
            // have been dropped with flags still set).
            e.set(flag::DIRTY, false);
            if e.is_parked() {
                out.extend_from_slice(&e.record());
                n += 1;
            }
        }
        None
    }

    /// While on, [`shrink`](Self::shrink) does not compact the run table,
    /// so every entry stays in its slot: an index base written over several
    /// calls of [`index_snapshot`](Self::index_snapshot) then visits every
    /// entry that was in the table when it began (entries added later land in
    /// a free or new slot and are in the change lists once they matter).
    pub fn hold_slots(&mut self, on: bool) {
        self.slots_held = on;
    }

    /// The change lists overflowed: the next index write must be a base.
    pub fn index_needs_base(&self) -> bool {
        self.index_rebase
    }

    /// Index records for what changed since the last snapshot or delta.
    pub fn index_delta(&mut self, out: &mut Vec<u8>) {
        for run in std::mem::take(&mut self.changed) {
            let Some(slot) = self.runs.find_run(run) else {
                continue;
            };
            let e = self.runs.get_mut(slot);
            e.set(flag::DIRTY, false);
            // A run loaded again is restored from the checkpoint's runs.
            if e.is_parked() {
                out.extend_from_slice(&e.record());
            }
        }
        for run in std::mem::take(&mut self.deleted) {
            out.extend_from_slice(&Entry::new(run, 0, table::DELETED).record());
        }
    }

    /// Records pending for the next index delta.
    pub fn index_pending(&self) -> usize {
        self.changed.len() + self.deleted.len()
    }

    /// Bytes of loaded runs' in-memory journals.
    pub fn journal_bytes(&self) -> usize {
        self.journal_bytes
    }

    /// Loads a parked run from its decoded journal (`last_lsn` from its
    /// frame). If a timer of the run came due while it was parked, its due
    /// timers fire now.
    pub fn unpark(
        &mut self,
        run_id: RunId,
        last_lsn: Lsn,
        blob_records: &[RecordBuf],
        now_ms: u64,
    ) -> bool {
        let Some(slot) = self.runs.find_run(run_id) else {
            return false;
        };
        let e = *self.runs.get(slot);
        if !e.is_parked() || !matches!(blob_records.first(), Some(Record::Start { .. })) {
            return false;
        }
        let pending = e.has(flag::PENDING);
        for rec in blob_records {
            self.apply(last_lsn, rec, Mode::BLOB);
        }
        let Some(l) = self.loaded.get_mut(&run_id) else {
            return false;
        };
        l.blob = Some(BlobRef {
            offset: e.offset(),
            len: e.len(),
        });
        l.blob_chain = u64::from(e.len());
        l.blob_depth = 0;
        self.journal_bytes = self.journal_bytes.saturating_sub(l.journal.len());
        l.journal.clear();
        l.journal_records = 0;
        l.last_lsn = last_lsn;
        // Rebuilding the run armed its timers again. Those that came due
        // while it was parked, and asked for this load, fire now.
        if pending {
            let mut due = std::mem::take(&mut self.due);
            due.clear();
            due.extend(
                l.armed
                    .iter()
                    .filter(|a| a.2 <= now_ms)
                    .map(|&(seq, kind, _)| TimerKey {
                        run: run_id,
                        seq,
                        kind,
                    }),
            );
            for &key in &due {
                self.fire(key, now_ms);
            }
            self.due = due;
        }
        true
    }

    /// Earliest time a completed run becomes eligible for removal, including after recovery.
    pub fn next_expiry(&self) -> Option<u64> {
        self.retention
            .peek()
            .map(|Reverse((at, _))| at.saturating_add(self.retention_ms))
    }

    /// Update the frame location without losing a loaded journal's suffix or timers.
    pub fn relocate_blob(&mut self, run: RunId, offset: u64) {
        if let Some(blob) = self.loaded.get_mut(&run).and_then(|l| l.blob.as_mut()) {
            blob.offset = offset;
        } else if let Some(slot) = self.runs.find_run(run)
            && self.runs.get(slot).is_parked()
        {
            let len = self.runs.get(slot).len();
            self.runs.get_mut(slot).set_location(offset, len);
            self.mark_changed(slot);
        }
    }

    /// Remove expired runs and return the number of store bytes they occupied.
    pub fn expire(&mut self, now_ms: u64, out: &mut Vec<RunId>) -> u64 {
        let mut released = 0;
        while let Some(&Reverse((finished, run))) = self.retention.peek() {
            if finished.saturating_add(self.retention_ms) > now_ms {
                break;
            }
            self.retention.pop();
            let Some(slot) = self.runs.find_run(run) else {
                continue;
            };
            let e = *self.runs.get(slot);
            if e.is_parked() {
                if e.done().is_none() {
                    continue;
                }
                released += u64::from(e.len());
            } else {
                match self.loaded.get(&run) {
                    Some(l) if l.done.is_some() => {
                        released += l.blob.map_or(0, |_| l.blob_chain);
                        self.forget_loaded(run);
                    }
                    _ => continue,
                }
            }
            self.table_remove(slot);
            out.push(run);
        }
        released
    }

    /// Forgets a run entirely (terminated before it was ever parked, or a
    /// determinism failure the host gave up on). Rarely needed.
    pub fn remove(&mut self, run: RunId) {
        self.forget_loaded(run);
        if let Some(slot) = self.runs.find_run(run) {
            self.table_remove(slot);
        }
    }

    /// Releases capacity left behind when many runs were parked or
    /// finished (hash maps never shrink on their own).
    pub fn shrink(&mut self) {
        fn fit<K: std::hash::Hash + Eq, V>(m: &mut FxHashMap<K, V>) {
            if m.capacity() > 1024 && m.capacity() > 4 * m.len() {
                m.shrink_to(m.len() * 2);
            }
        }
        fit(&mut self.loaded);
        if !self.slots_held && self.runs.compact() {
            // Slots moved: the next sweep walks every chunk.
            self.sweep_full = true;
        }
        if self.outputs.capacity() > 1024 && self.outputs.capacity() > 4 * self.outputs.len() {
            self.outputs.shrink_to(self.outputs.len() * 2);
        }
        if self.retention.capacity() > 1024 && self.retention.capacity() > 4 * self.retention.len()
        {
            self.retention.shrink_to(self.retention.len() * 2);
        }
    }

    /// Replaces the journal of a loaded run after the host stored a prefix
    /// of it in the run store: `blob` is the newest frame, `chain` the bytes
    /// of it and the frames it extends, `depth` how many it extends.
    pub fn spilled(&mut self, run: RunId, blob: BlobRef, chain: u64, depth: u8) {
        if let Some(l) = self.loaded.get_mut(&run) {
            l.blob = Some(blob);
            l.blob_chain = chain;
            l.blob_depth = depth;
            self.journal_bytes = self.journal_bytes.saturating_sub(l.journal.len());
            l.journal.clear();
            l.journal_records = 0;
        }
    }

    /// A loaded run's stored prefix moved to `blob` (its chain written
    /// whole), keeping the records after it in memory.
    pub fn set_blob(&mut self, run: RunId, blob: BlobRef, chain: u64, depth: u8) {
        if let Some(l) = self.loaded.get_mut(&run) {
            l.blob = Some(blob);
            l.blob_chain = chain;
            l.blob_depth = depth;
        }
    }

    fn forget_loaded(&mut self, run: RunId) {
        if let Some(l) = self.loaded.remove(&run) {
            self.journal_bytes = self.journal_bytes.saturating_sub(l.journal.len());
            self.recycle(l);
        }
    }
}

/// Emptied runs a shard keeps while runs come and go (a mass wake reuses
/// them); an idle shard keeps [`SPARE_RUNS_IDLE`] (see
/// [`ShardCore::shrink_spare`]).
const SPARE_RUNS: usize = 2048;
/// Emptied runs kept once a shard is idle.
pub const SPARE_RUNS_IDLE: usize = 64;

impl LoadedRun {
    /// A run's box with nothing in it, for a pool that is empty.
    fn empty() -> Self {
        let blank: Name = Name::from("");
        Self {
            id: blank.clone(),
            wtype: blank,
            version: 0,
            codec: 0,
            parent: None,
            started_ms: 0,
            deadline_ms: 0,
            last_lsn: 0,
            done: None,
            result: None,
            blob: None,
            blob_chain: 0,
            blob_depth: 0,
            journal: Vec::new(),
            journal_records: 0,
            signal_count: 0,
            signals: VecDeque::new(),
            waits: Vec::new(),
            armed: Vec::new(),
            dedup: FxHashSet::default(),
            children: Vec::new(),
            policy: 0,
            from: None,
            copies_left: 0,
            updates: Vec::new(),
            update_ids: FxHashMap::default(),
            awaiting: FxHashMap::default(),
            kids: Vec::new(),
            attrs: Vec::new(),
            streams: false,
            queued: 0,
        }
    }

    /// Empties a run that left memory, keeping its box and containers for
    /// the next run loaded or started (a mass wake loads and parks runs in
    /// turn without allocating their journals and timer lists each time);
    /// false if one container is large (emptying keeps its capacity, and a
    /// large one is better given back). Its names stay until the next run
    /// replaces them; its result goes now.
    fn empty_for_reuse(&mut self) -> bool {
        const MAX: usize = 64;
        if self.journal.capacity() > 4096
            || self.signals.capacity() > MAX
            || self.waits.capacity() > MAX
            || self.armed.capacity() > MAX
            || self.dedup.capacity() > MAX
            || self.children.capacity() > MAX
            || self.updates.capacity() > MAX
            || self.update_ids.capacity() > MAX
            || self.awaiting.capacity() > MAX
            || self.kids.capacity() > MAX
            || self.attrs.capacity() > MAX
        {
            return false;
        }
        self.result = None;
        self.journal.clear();
        self.signals.clear();
        self.waits.clear();
        self.armed.clear();
        self.dedup.clear();
        self.children.clear();
        self.updates.clear();
        self.update_ids.clear();
        self.awaiting.clear();
        self.kids.clear();
        self.attrs.clear();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordRef;

    fn start(run: RunId, id: &'static str, deadline: u64) -> RecordRef<'static> {
        Record::Start {
            run,
            id,
            wtype: "t",
            version: 1,
            codec: 0,
            started_ms: 0,
            deadline_ms: deadline,
            parent: None,
            policy: 0,
            from: None,
            copies: 0,
            update: None,
            input: b"",
            meta: None,
        }
    }

    /// Listings walk runs newest first, whether old run numbers are still
    /// there or mostly gone.
    #[test]
    fn runs_come_newest_first() {
        for gap in [false, true] {
            let mut core = ShardCore::new(0, 1, 1000, 0);
            for run in 1..=300u64 {
                let id: &'static str = Box::leak(format!("r{run}").into_boxed_str());
                core.apply(run, &start(run, id, 0), Mode::LIVE);
            }
            if gap {
                // Numbers issued since and gone: probing them costs more
                // than a pass over the table, so the cursor sorts instead.
                core.set_next_run_id(1_000_000);
            }
            let mut cursor = core.newest_first();
            assert_eq!(
                cursor.next_batch(&core, 50),
                (251..=300).rev().collect::<Vec<_>>()
            );
            let mut rest = Vec::new();
            loop {
                let batch = cursor.next_batch(&core, 64);
                if batch.is_empty() {
                    break;
                }
                rest.extend(batch);
            }
            assert_eq!(rest, (1..=250).rev().collect::<Vec<_>>(), "gap {gap}");
        }
    }

    #[test]
    fn continue_as_new_is_one_record() {
        let mut core = ShardCore::new(0, 1, 1000, 0);
        core.apply(1, &start(1, "a", 0), Mode::LIVE);
        let next = Record::Start {
            run: 2,
            id: Arc::from("a"),
            wtype: Arc::from("t"),
            version: 1,
            codec: 1,
            started_ms: 5,
            deadline_ms: 0,
            parent: None,
            policy: 0,
            from: Some(Origin {
                run: 1,
                cause: crate::record::origin::CONTINUED,
            }),
            copies: 0,
            update: None,
            input: Arc::from(&b"{}"[..]),
            meta: None,
        };
        let mut payload = Vec::new();
        crate::record::encode_canonical(&next, &mut payload);
        core.apply(
            2,
            &RecordRef::Done {
                run: 1,
                status: Status::ContinuedAsNew,
                finished_ms: 5,
                payload: &payload,
                by: None,
            },
            Mode::LIVE,
        );
        assert_eq!(core.get(1).unwrap().done, Some((Status::ContinuedAsNew, 5)));
        assert_eq!(core.find("a"), Some(Found::Loaded(2)));
        let j = crate::record::decode_canonical_all(&core.get(2).unwrap().journal).unwrap();
        assert!(matches!(j[0], Record::Start { run: 2, .. }));
        // Recovery meets the new run's own start again: no reset of state.
        core.apply(
            3,
            &RecordRef::Signal {
                run: 2,
                name: "s",
                dedup: 0,
                payload: b"",
                by: None,
            },
            Mode::LIVE,
        );
        core.apply(2, &next, Mode::RECOVER);
        assert_eq!(core.get(2).unwrap().buffered_signals(), 1);
    }

    #[test]
    fn loading_a_continued_run_back_leaves_its_successor_alone() {
        let mut core = ShardCore::new(0, 1, 1_000_000, 0);
        core.apply(1, &start(1, "a", 0), Mode::LIVE);
        let next = Record::Start {
            run: 2,
            id: Arc::from("a"),
            wtype: Arc::from("t"),
            version: 1,
            codec: 1,
            started_ms: 5,
            deadline_ms: 0,
            parent: None,
            policy: 0,
            from: Some(Origin {
                run: 1,
                cause: crate::record::origin::CONTINUED,
            }),
            copies: 0,
            update: None,
            input: Arc::from(&b"{}"[..]),
            meta: None,
        };
        let mut payload = Vec::new();
        crate::record::encode_canonical(&next, &mut payload);
        let done = RecordRef::Done {
            run: 1,
            status: Status::ContinuedAsNew,
            finished_ms: 5,
            payload: &payload,
            by: None,
        };
        core.apply(2, &done, Mode::LIVE);
        let history = vec![start(1, "a", 0).to_buf(), done.to_buf()];
        core.park(1, BlobRef { offset: 0, len: 1 }, false);
        core.apply(
            3,
            &RecordRef::Done {
                run: 2,
                status: Status::Ok,
                finished_ms: 6,
                payload: b"7",
                by: None,
            },
            Mode::LIVE,
        );
        core.park(2, BlobRef { offset: 9, len: 1 }, false);
        // A late message loads the first run back from its full history.
        assert!(core.unpark(1, 2, &history, 10));
        assert_eq!(core.parked(2).and_then(|p| p.done()), Some(Status::Ok));
        assert_eq!(core.find("a"), Some(Found::Parked(2)));
    }

    #[test]
    fn parent_close_policy_closes_children() {
        let mut core = ShardCore::new(0, 1, 1000, 0);
        core.apply(1, &start(1, "p", 0), Mode::LIVE);
        for (seq, id, flags) in [
            (0, "kill", policy::CLOSE_TERMINATE),
            (1, "ask", policy::CLOSE_CANCEL | policy::DETACHED),
            (2, "keep", policy::CLOSE_ABANDON),
        ] {
            core.apply(
                2 + u64::from(seq),
                &RecordRef::Child {
                    run: 1,
                    seq,
                    wtype: "t",
                    id,
                    flags,
                    deadline_ms: 0,
                    input: b"",
                },
                Mode::LIVE,
            );
        }
        core.apply(
            9,
            &RecordRef::Done {
                run: 1,
                status: Status::Terminated,
                finished_ms: 1,
                payload: b"",
                by: None,
            },
            Mode::LIVE,
        );
        let mut out = Vec::new();
        core.release(9, &mut out);
        let closes: Vec<(String, bool)> = out
            .iter()
            .filter_map(|o| match o {
                Output::Close {
                    target, terminate, ..
                } => Some((target.to_string(), *terminate)),
                _ => None,
            })
            .collect();
        assert_eq!(
            closes,
            vec![("kill".to_string(), true), ("ask".to_string(), false)]
        );
    }

    #[test]
    fn signals_match_waits_and_timeouts_release_offers() {
        let mut core = ShardCore::new(0, 1, 1000, 0);
        core.apply(1, &start(1, "a", 0), Mode::LIVE);
        assert_eq!(core.take_event(), Some(Event::Started { run: 1 }));
        core.apply(
            2,
            &RecordRef::Signal {
                run: 1,
                name: "go",
                dedup: 0,
                payload: b"x",
                by: None,
            },
            Mode::LIVE,
        );
        assert_eq!(core.take_event(), None);
        core.apply(
            3,
            &RecordRef::Wait {
                run: 1,
                seq: 0,
                name: "go",
                deadline_ms: 50,
            },
            Mode::LIVE,
        );
        let Some(Event::SignalReady {
            seq: 0, ordinal: 0, ..
        }) = core.take_event()
        else {
            panic!("expected the buffered signal to be offered");
        };
        // The wait times out instead (the timeout won the race).
        core.apply(
            4,
            &RecordRef::Waited {
                run: 1,
                seq: 0,
                outcome: WaitOutcome::TimedOut,
            },
            Mode::LIVE,
        );
        // A second wait gets the same signal.
        core.apply(
            5,
            &RecordRef::Wait {
                run: 1,
                seq: 1,
                name: "go",
                deadline_ms: 0,
            },
            Mode::LIVE,
        );
        assert!(matches!(
            core.take_event(),
            Some(Event::SignalReady {
                seq: 1,
                ordinal: 0,
                ..
            })
        ));
        // The stale timeout entry no longer fires.
        core.poll_timers(100);
        assert_eq!(core.take_event(), None);
    }

    #[test]
    fn outputs_wait_for_durability() {
        let mut core = ShardCore::new(0, 1, 1000, 0);
        core.apply(1, &start(1, "a", 0), Mode::LIVE);
        core.add_output(1, Output::Ack(7));
        core.apply(
            2,
            &RecordRef::Done {
                run: 1,
                status: Status::Ok,
                finished_ms: 5,
                payload: b"r",
                by: None,
            },
            Mode::LIVE,
        );
        let mut out = Vec::new();
        core.release(1, &mut out);
        assert_eq!(out, vec![Output::Ack(7)]);
        out.clear();
        core.release(2, &mut out);
        assert!(matches!(out[0], Output::Result { run: 1, .. }));
        let mut expired = Vec::new();
        core.expire(1004, &mut expired);
        assert!(expired.is_empty());
        core.expire(1005, &mut expired);
        assert_eq!(expired, vec![1]);
        assert_eq!(core.find("a"), None);
    }

    #[test]
    fn parked_timers_request_a_load() {
        let mut core = ShardCore::new(0, 1, 1000, 0);
        core.apply(1, &start(1, "a", 0), Mode::LIVE);
        core.apply(
            2,
            &RecordRef::Timer {
                run: 1,
                seq: 0,
                name: "nap",
                fire_at_ms: 10,
            },
            Mode::LIVE,
        );
        core.take_event();
        let journal = crate::record::decode_canonical_all(&core.get(1).unwrap().journal).unwrap();
        core.park(1, BlobRef { offset: 0, len: 1 }, false);
        assert_eq!(std::mem::size_of::<ParkedRun>(), 16);
        assert_eq!(core.find("a"), Some(Found::Parked(1)));
        core.poll_timers(10);
        assert_eq!(core.take_event(), Some(Event::NeedLoad { run: 1 }));
        assert!(core.unpark(1, 2, &journal, 10));
        assert_eq!(
            core.take_event(),
            Some(Event::Timer {
                run: 1,
                seq: 0,
                kind: TimerKind::Sleep
            })
        );
    }

    fn sleeper(core: &mut ShardCore, run: RunId, id: &'static str, at: u64) -> Vec<RecordBuf> {
        core.apply(run * 10, &start(run, id, 0), Mode::LIVE);
        core.apply(
            run * 10 + 1,
            &RecordRef::Timer {
                run,
                seq: 0,
                name: "nap",
                fire_at_ms: at,
            },
            Mode::LIVE,
        );
        while core.take_event().is_some() {}
        crate::record::decode_canonical_all(&core.get(run).unwrap().journal).unwrap()
    }

    #[test]
    fn far_timers_wait_in_their_bucket_until_it_approaches() {
        let mut core = ShardCore::new(0, 1, 1000, 0);
        let far = 10 * HORIZON_MS + 7;
        let mut journals = Vec::new();
        for run in 1..=3 {
            journals.push(sleeper(
                &mut core,
                run,
                ["a", "b", "c"][run as usize - 1],
                far,
            ));
        }
        // Loaded: the far timers are not in the wheel.
        assert_eq!(core.wheel.len(), 0);
        core.park(1, BlobRef { offset: 0, len: 1 }, false);
        core.park(2, BlobRef { offset: 9, len: 1 }, false);
        assert_eq!(core.near.len(), 0);
        // Nothing to do until the bucket comes within the horizon, which
        // ends just before `now + HORIZON_MS`: the time the core asks to be
        // polled at must move something, or a caller that polls exactly
        // then (a simulated clock) never gets past it.
        assert_eq!(core.next_timer(), Some(far - HORIZON_MS + 1));
        core.poll_timers(far - HORIZON_MS - 1);
        assert_eq!((core.near.len(), core.wheel.len()), (0, 0));
        core.poll_timers(far - HORIZON_MS);
        assert_eq!((core.near.len(), core.wheel.len()), (0, 0));
        assert!(core.next_timer().is_some_and(|t| t > far - HORIZON_MS));
        core.poll_timers(far - HORIZON_MS + 1);
        assert_eq!((core.near.len(), core.wheel.len()), (2, 1));
        assert_eq!(core.take_event(), None);
        core.poll_timers(far);
        let mut loads: Vec<_> = std::iter::from_fn(|| core.take_event()).collect();
        loads.sort_by_key(|e| format!("{e:?}"));
        assert_eq!(
            loads,
            vec![
                Event::NeedLoad { run: 1 },
                Event::NeedLoad { run: 2 },
                Event::Timer {
                    run: 3,
                    seq: 0,
                    kind: TimerKind::Sleep
                }
            ]
        );
        assert!(core.unpark(1, 11, &journals[0], far));
        assert_eq!(
            core.take_event(),
            Some(Event::Timer {
                run: 1,
                seq: 0,
                kind: TimerKind::Sleep
            })
        );
    }

    #[test]
    fn the_index_restores_parked_runs_and_their_buckets() {
        let mut core = ShardCore::new(0, 1, 1000, 0);
        let far = 5 * HORIZON_MS;
        for run in 1..=50 {
            let id: &'static str = Box::leak(format!("r{run}").into_boxed_str());
            sleeper(&mut core, run, id, far + run);
            core.park(
                run,
                BlobRef {
                    offset: run * 100,
                    len: 40,
                },
                run % 2 == 0,
            );
        }
        let mut base = Vec::new();
        core.begin_index_snapshot();
        let mut from = Some(0);
        while let Some(at) = from {
            from = core.index_snapshot(at, 7, &mut base);
        }
        assert_eq!(base.len(), 50 * table::RECORD);
        // Later changes go to a delta: one run moves, one is removed.
        core.relocate_blob(3, 9_999);
        core.remove(4);
        let mut delta = Vec::new();
        core.index_delta(&mut delta);
        assert_eq!(delta.len(), 2 * table::RECORD);

        let mut back = ShardCore::new(0, 1, 1000, 0);
        back.reserve_runs(52);
        back.load_index(&base).unwrap();
        back.load_index(&delta).unwrap();
        back.index_loaded();
        assert_eq!(back.parked_len(), 49);
        assert_eq!(back.next_run_id(), 51);
        assert_eq!(back.parked(3).unwrap().blob().offset, 9_999);
        assert!(back.parked(4).is_none());
        assert_eq!(back.find("r7"), Some(Found::Parked(7)));
        assert!(back.chained_runs().all(|(r, _)| r % 2 == 0));
        assert_eq!(back.chained_runs().count(), 24);
        // The earliest timer is `far + 1`; it enters the horizon a
        // millisecond after `far + 1 - HORIZON_MS`.
        assert_eq!(back.next_timer(), Some(far + 2 - HORIZON_MS));
        back.poll_timers(far + 50);
        let loads = std::iter::from_fn(|| back.take_event()).count();
        assert_eq!(loads, 49);
        assert!(back.load_index(&[1; 31]).is_err());
    }

    #[test]
    fn workflow_deadline_fires() {
        let mut core = ShardCore::new(0, 1, 1000, 0);
        core.apply(1, &start(1, "a", 30), Mode::LIVE);
        core.take_event();
        core.poll_timers(29);
        assert_eq!(core.take_event(), None);
        core.poll_timers(30);
        assert_eq!(
            core.take_event(),
            Some(Event::Timer {
                run: 1,
                seq: 0,
                kind: TimerKind::Deadline
            })
        );
    }
}
