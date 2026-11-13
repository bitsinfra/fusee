//! The per-run journal machine.
//!
//! Workflow code is deterministic between journal operations. Every operation
//! the code starts (`step`, `sleep`, `wait_for`, `child`, `now`, ...) takes the
//! next sequence number, so an operation's identity is the order in which the
//! code *called* it. Results come back as **deliveries**, and the journal
//! records deliveries in the order they were handed to the code. Replay hands
//! recorded results back in exactly that order, one at a time, letting the
//! code run until it is quiescent in between. Live completions that arrive
//! while replay is still in progress are held until the recorded ones are all
//! delivered. Live execution uses the same one-at-a-time rule, so a parallel
//! branch can never observe a different interleaving after a crash.
//!
//! Accepted updates are deliveries too: an update's handler starts at the
//! update's place in the journal, so its operations take the same sequence
//! numbers on every run.
//!
//! The machine does no I/O. It tells the host which records to append and
//! when a step function may run; the host (the embedded engine, a language
//! binding or a remote worker) does the rest.

use crate::record::{
    Lsn, Name, PAUSED, Payload, Record, RecordBuf, RunId, Seq, Status, WaitOutcome, stream,
    value_kind,
};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

/// The kinds of journal operations workflow code can start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    Step,
    Sleep,
    Wait,
    Child,
    Value,
    Send,
    /// A step completed from outside (`ctx.awaitCompletion`).
    Await,
    /// An update handler returned.
    UpdateDone,
    Attributes,
}

impl OpKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Step => "step",
            Self::Sleep => "sleep",
            Self::Wait => "wait",
            Self::Child => "child",
            Self::Value => "value",
            Self::Send => "send",
            Self::Await => "awaitCompletion",
            Self::UpdateDone => "update result",
            Self::Attributes => "setAttributes",
        }
    }
}

/// The code did something its journal says it did not do the first time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeterminismError {
    pub seq: Option<Seq>,
    pub message: String,
}

impl fmt::Display for DeterminismError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.seq {
            Some(seq) => write!(
                f,
                "determinism error at journal position {seq}: {}",
                self.message
            ),
            None => write!(f, "determinism error: {}", self.message),
        }
    }
}

impl std::error::Error for DeterminismError {}

/// What an operation resolves to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    pub status: Status,
    pub payload: Payload,
}

impl Outcome {
    pub fn ok(payload: Payload) -> Self {
        Self {
            status: Status::Ok,
            payload,
        }
    }

    pub fn empty(status: Status) -> Self {
        Self {
            status,
            payload: empty(),
        }
    }
}

/// The empty payload, shared on this thread (a clone, not an allocation).
pub fn empty() -> Payload {
    EMPTY.with(Payload::clone)
}

/// A live completion reported by the host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Completion {
    Step {
        status: Status,
        payload: Payload,
    },
    Fired,
    Signal {
        ordinal: u64,
        payload: Payload,
    },
    WaitTimedOut,
    Child {
        status: Status,
        payload: Payload,
    },
    /// An asynchronously completed step got its result for `attempt`.
    Completed {
        attempt: u32,
        status: Status,
        payload: Payload,
    },
}

/// What the host must do after the code starts a step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepStart {
    /// The result is recorded; wait for its delivery and do not run the
    /// function.
    Replay,
    /// Run the function once the run's journal is durable through `gate`
    /// and the clock has reached `not_before_ms`.
    Run { attempt: u32, not_before_ms: u64 },
}

/// What the host must do after the code starts an async completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AwaitStart {
    /// The result is recorded; wait for its delivery.
    Replay,
    /// The token for `attempt` was already handed out; wait.
    Waiting { attempt: u32 },
    /// Run the function that hands the token out, as for a step.
    Run { attempt: u32, not_before_ms: u64 },
}

/// What the host must do after the code starts a sleep, wait or child.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Arm {
    /// Recorded result; wait for its delivery.
    Replay,
    /// Already set up in an earlier life of this run; wait for completion.
    Armed,
    /// Append this record; the shard arms the timer, wait or child from it.
    Append(RecordBuf),
}

/// The next thing to hand to the code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next {
    /// Resolve operation `seq`. If `record` is set, append it first: this is
    /// a live delivery and the record is how the journal remembers it.
    Deliver {
        seq: Seq,
        outcome: Outcome,
        record: Option<RecordBuf>,
    },
    /// A live step's or async completion's result: append
    /// [`LiveRecord::record`] first, then resolve `seq` with `outcome`. The
    /// record borrows the outcome's payload and status, so the payload is
    /// never cloned for it.
    DeliverLive {
        seq: Seq,
        outcome: Outcome,
        record: LiveRecord,
    },
    /// Cancellation reached the code: resolve every listed operation with
    /// `Status::Cancelled` and `reason`.
    CancelAll {
        seqs: Vec<Seq>,
        reason: Payload,
        record: Option<RecordBuf>,
    },
    /// An update reached its turn: start its handler now.
    ///
    /// - `live: None`: the update is recorded (replay, or delivered with the
    ///   start). Start the handler; nothing is appended.
    /// - `live: Some(token)`: a new request. Run the validator now, then call
    ///   [`RunMachine::accept_update`] (and append its record) or drop it.
    Update {
        ordinal: u64,
        name: Name,
        id: Name,
        payload: Payload,
        live: Option<u64>,
    },
    /// Replay is waiting for the code to start the operation the journal
    /// delivers next. If the code is quiescent and nothing live is pending,
    /// this is a determinism error ([`RunMachine::stall_error`]).
    Stalled,
    /// Nothing to deliver.
    Idle,
}

/// The record of a [`Next::DeliverLive`], less what its outcome holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveRecord {
    Step { name: Name, attempt: u32 },
    Completed { attempt: u32 },
}

impl LiveRecord {
    /// The record to append, borrowing `outcome`'s payload. `time`: when
    /// the step ran, under the dev profile only (a `Completed` has none).
    pub fn record<'a>(
        &'a self,
        run: RunId,
        seq: Seq,
        outcome: &'a Outcome,
        time: Option<crate::record::StepTime>,
    ) -> crate::record::RecordRef<'a> {
        match self {
            Self::Step { name, attempt } => Record::Step {
                run,
                seq,
                name,
                attempt: *attempt,
                status: outcome.status,
                payload: &outcome.payload,
                time,
            },
            Self::Completed { attempt } => Record::Completed {
                run,
                seq,
                attempt: *attempt,
                status: outcome.status,
                payload: &outcome.payload,
            },
        }
    }
}

#[derive(Clone, Debug)]
struct Recorded {
    kind: OpKind,
    name: Name,
    /// Last failed attempt and when to retry (steps); the token attempt
    /// handed out (async completions); the value kind (values).
    attempt: u32,
    retry_at_ms: u64,
    /// Attempts made before the step's last resume: its retry budget
    /// counts from here.
    base: u32,
    /// A begin marker (timer, wait, child, awaiting) or value exists.
    marked: bool,
    value: Option<Payload>,
    /// A final delivery for this seq exists in the replay queue.
    delivered: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    Replay,
    Live,
}

#[derive(Clone, Debug)]
struct PendingOp {
    kind: OpKind,
    name: Name,
    attempt: u32,
    source: Source,
    /// Waiting on something outside the run (an async completion whose
    /// token was handed out): the run may be parked.
    waiting: bool,
    /// A live completion for it is queued (in `live`, not yet delivered).
    queued: bool,
    /// A step whose retries ran out, waiting for a resume: the run may be
    /// parked.
    paused: bool,
}

/// Operations by sequence number: up to `N` in a vector searched linearly
/// (a sequential step has one pending; a short journal has a few recorded),
/// a hash map beyond, so a step pays no hash insert and remove and a
/// fan-out of thousands no linear search. Iteration order is unspecified,
/// as a map's is.
struct SeqMap<V, const N: usize> {
    small: Vec<(Seq, V)>,
    /// Holds every entry while it is not empty.
    large: FxHashMap<Seq, V>,
}

impl<V, const N: usize> Default for SeqMap<V, N> {
    fn default() -> Self {
        Self {
            small: Vec::new(),
            large: FxHashMap::default(),
        }
    }
}

impl<V, const N: usize> SeqMap<V, N> {
    fn insert(&mut self, seq: Seq, v: V) {
        if self.large.is_empty() {
            if let Some(e) = self.small.iter_mut().find(|(s, _)| *s == seq) {
                e.1 = v;
                return;
            }
            if self.small.len() < N {
                self.small.push((seq, v));
                return;
            }
            self.large.extend(self.small.drain(..));
        }
        self.large.insert(seq, v);
    }

    fn get_or_insert_with(&mut self, seq: Seq, f: impl FnOnce() -> V) -> &mut V {
        if self.large.is_empty() {
            if let Some(i) = self.small.iter().position(|(s, _)| *s == seq) {
                return &mut self.small[i].1;
            }
            if self.small.len() < N {
                self.small.push((seq, f()));
                let last = self.small.len() - 1;
                return &mut self.small[last].1;
            }
            self.large.extend(self.small.drain(..));
        }
        self.large.entry(seq).or_insert_with(f)
    }

    fn get(&self, seq: &Seq) -> Option<&V> {
        if self.large.is_empty() {
            self.small.iter().find(|(s, _)| s == seq).map(|(_, v)| v)
        } else {
            self.large.get(seq)
        }
    }

    fn get_mut(&mut self, seq: &Seq) -> Option<&mut V> {
        if self.large.is_empty() {
            self.small
                .iter_mut()
                .find(|(s, _)| s == seq)
                .map(|(_, v)| v)
        } else {
            self.large.get_mut(seq)
        }
    }

    fn remove(&mut self, seq: &Seq) -> Option<V> {
        if self.large.is_empty() {
            let i = self.small.iter().position(|(s, _)| s == seq)?;
            Some(self.small.swap_remove(i).1)
        } else {
            self.large.remove(seq)
        }
    }

    fn clear(&mut self) {
        self.small.clear();
        self.large.clear();
    }

    fn len(&self) -> usize {
        self.small.len() + self.large.len()
    }

    fn is_empty(&self) -> bool {
        self.small.is_empty() && self.large.is_empty()
    }

    fn capacity(&self) -> usize {
        self.small.capacity().max(self.large.capacity())
    }

    fn values(&self) -> impl Iterator<Item = &V> {
        self.small.iter().map(|(_, v)| v).chain(self.large.values())
    }

    fn keys(&self) -> impl Iterator<Item = &Seq> {
        self.small.iter().map(|(s, _)| s).chain(self.large.keys())
    }

    fn iter(&self) -> impl Iterator<Item = (Seq, &V)> {
        self.small
            .iter()
            .map(|(s, v)| (*s, v))
            .chain(self.large.iter().map(|(s, v)| (*s, v)))
    }
}

#[derive(Clone, Debug)]
enum ReplayItem {
    Op {
        seq: Seq,
        outcome: Outcome,
    },
    Cancel {
        reason: Payload,
    },
    Update {
        ordinal: u64,
        name: Name,
        id: Name,
        payload: Payload,
    },
}

#[derive(Clone, Debug)]
enum LiveItem {
    Complete {
        seq: Seq,
        completion: Completion,
    },
    Cancel {
        reason: Payload,
        /// Who asked (audit).
        by: Option<Payload>,
    },
    Update {
        token: u64,
        name: Name,
        id: Name,
        payload: Payload,
    },
}

/// Who writes a stream in this life of the run. Not journaled: workflow
/// code opens its streams again as it replays, and a step when it runs
/// again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamOwner {
    None,
    Workflow,
    Step(Seq),
}

/// One of the run's streams, as its journal and this life's code made it.
#[derive(Clone, Debug)]
struct StreamState {
    name: Name,
    /// Chunks recorded or appended so far: the next chunk's offset.
    len: u64,
    /// Completed, and by whom ([`stream::WORKFLOW`] or a step's position).
    closed: Option<Seq>,
    /// Chunks of workflow code the journal holds that this life's code has
    /// not appended again yet: replay matches the code's appends to them in
    /// order and appends nothing.
    replay_left: u64,
    /// Chunks per writing step: `(position, count)`.
    by_step: Vec<(Seq, u64)>,
    owner: StreamOwner,
}

/// What [`RunMachine::stream_open`] tells the writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamOpen {
    /// Chunks earlier attempts of the opening step recorded (0 for
    /// workflow code): the prefix a step that runs again continues after.
    pub recorded: u64,
    /// The stream is complete; appends from the opening step are dropped
    /// (it completed the stream in an earlier attempt) or refused.
    pub closed: bool,
}

/// Identity and input of a run, from its `Start` record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunInfo {
    pub run: RunId,
    pub id: Name,
    pub wtype: Name,
    pub version: u32,
    pub codec: u8,
    pub started_ms: u64,
    pub deadline_ms: u64,
    pub policy: u8,
    pub input: Payload,
    /// The W3C `traceparent` the run was started under, if any.
    pub trace: Option<Payload>,
}

pub struct RunMachine {
    info: RunInfo,
    next_seq: Seq,
    recorded: SeqMap<Recorded, 16>,
    /// Highest sequence number the loaded journal mentions.
    max_recorded: Option<Seq>,
    replay: VecDeque<ReplayItem>,
    pending: SeqMap<PendingOp, 4>,
    live: VecDeque<LiveItem>,
    /// Completions that arrived before the code started their operation
    /// (a timer that fired, or a signal that matched, while replay had not
    /// yet reached it).
    early: FxHashMap<Seq, Completion>,
    signals: Vec<Payload>,
    /// Latest recorded heartbeat progress per step.
    progress: FxHashMap<Seq, Payload>,
    /// `(position, patch id)` markers from the journal.
    patches: FxHashSet<(Seq, Name)>,
    /// Updates accepted so far (the next one's ordinal).
    updates: u64,
    /// Waits the code dropped, as recorded.
    abandoned: FxHashSet<Seq>,
    cancel_queued: bool,
    last_cancel_position: Option<Seq>,
    done: Option<(Status, Payload)>,
    /// Operation names the code used, shared by its operations so that an
    /// operation does not allocate its name (at most [`INTERNED_NAMES`]).
    names: FxHashSet<Name>,
    /// The name interned last: a loop of one kind of step finds it without
    /// hashing.
    last_name: Option<Name>,
    /// The run's streams, in the order they first appeared.
    streams: Vec<StreamState>,
    /// LSN of the run's most recent record. A step function may run only once
    /// the log is durable through it.
    pub gate: Lsn,
}

/// Distinct operation names a run keeps. Code that makes up a new name for
/// each operation gets a new allocation for each beyond these.
const INTERNED_NAMES: usize = 256;

thread_local! {
    static BLANK: Name = Arc::from("");
    static EMPTY: Payload = Arc::from(&[][..]);
}

/// The empty name, shared on this thread (a record that names no
/// operation): loading a journal does not allocate one per record.
/// A journal whose first record is not its run's start.
fn not_started() -> DeterminismError {
    DeterminismError {
        seq: None,
        message: "journal does not begin with a start record".into(),
    }
}

fn blank_name() -> Name {
    BLANK.with(Name::clone)
}

fn mismatch(seq: Seq, rec: &Recorded, kind: OpKind, name: &str) -> DeterminismError {
    DeterminismError {
        seq: Some(seq),
        message: format!(
            "the journal recorded {} '{}' here but the code called {} '{}'",
            rec.kind.as_str(),
            rec.name,
            kind.as_str(),
            name
        ),
    }
}

/// A step's failed attempts are recorded before anything tells whether the
/// operation was a plain step or an async completion.
fn compatible(recorded: &Recorded, kind: OpKind) -> bool {
    recorded.kind == kind
        || (recorded.kind == OpKind::Step
            && kind == OpKind::Await
            && !recorded.delivered
            && !recorded.marked)
}

fn add_chunk(by_step: &mut Vec<(Seq, u64)>, seq: Seq) {
    match by_step.iter_mut().find(|(s, _)| *s == seq) {
        Some((_, n)) => *n += 1,
        None => by_step.push((seq, 1)),
    }
}

fn chunks_of(by_step: &[(Seq, u64)], seq: Seq) -> u64 {
    by_step
        .iter()
        .find(|(s, _)| *s == seq)
        .map_or(0, |(_, n)| *n)
}

impl RunMachine {
    /// Builds a machine from a run's journal (its records in log order, the
    /// first being `Start`). `gate` is the LSN of the last record.
    pub fn new(journal: &[RecordBuf], gate: Lsn) -> Result<Self, DeterminismError> {
        let Some(Record::Start {
            run,
            id,
            wtype,
            version,
            codec,
            started_ms,
            deadline_ms,
            policy,
            update,
            input,
            meta,
            ..
        }) = journal.first()
        else {
            return Err(DeterminismError {
                seq: None,
                message: "journal does not begin with a start record".into(),
            });
        };
        let mut m = Self {
            info: RunInfo {
                run: *run,
                id: id.clone(),
                wtype: wtype.clone(),
                version: *version,
                codec: *codec,
                started_ms: *started_ms,
                deadline_ms: *deadline_ms,
                policy: *policy,
                input: input.clone(),
                trace: meta.as_ref().and_then(|m| m.trace.clone()),
            },
            next_seq: 0,
            recorded: SeqMap::default(),
            max_recorded: None,
            replay: VecDeque::new(),
            pending: SeqMap::default(),
            live: VecDeque::new(),
            early: FxHashMap::default(),
            signals: Vec::new(),
            progress: FxHashMap::default(),
            patches: FxHashSet::default(),
            updates: 0,
            abandoned: FxHashSet::default(),
            cancel_queued: false,
            last_cancel_position: None,
            done: None,
            names: FxHashSet::default(),
            last_name: None,
            streams: Vec::new(),
            gate,
        };
        if let Some(u) = update {
            m.replay.push_back(ReplayItem::Update {
                ordinal: 0,
                name: u.name.clone(),
                id: u.id.clone(),
                payload: u.payload.clone(),
            });
            m.updates = 1;
        }
        for rec in &journal[1..] {
            m.load(rec)?;
        }
        Ok(m)
    }

    /// [`Self::new`] in place: the machine becomes `journal`'s run's,
    /// keeping its collections' memory and its cache of operation names
    /// (a shard reuses the machines of runs that ended or parked). After an
    /// error the machine holds part of `journal` and must be rebuilt again
    /// before use.
    pub fn rebuild(&mut self, journal: &[RecordBuf], gate: Lsn) -> Result<(), DeterminismError> {
        let Some(Record::Start {
            run,
            id,
            wtype,
            version,
            codec,
            started_ms,
            deadline_ms,
            policy,
            update,
            input,
            meta,
            ..
        }) = journal.first()
        else {
            return Err(not_started());
        };
        let info = RunInfo {
            run: *run,
            id: id.clone(),
            wtype: wtype.clone(),
            version: *version,
            codec: *codec,
            started_ms: *started_ms,
            deadline_ms: *deadline_ms,
            policy: *policy,
            input: input.clone(),
            trace: meta.as_ref().and_then(|m| m.trace.clone()),
        };
        let update = update
            .as_ref()
            .map(|u| (u.name.clone(), u.id.clone(), u.payload.clone()));
        self.reset(info, update, gate);
        for rec in &journal[1..] {
            self.load(rec)?;
        }
        Ok(())
    }

    /// [`Self::rebuild`] from a journal it may empty: the start record's
    /// names and payloads move into the run instead of being cloned (on a
    /// wake they were cloned, then dropped with the decoded records, an
    /// atomic each way on lines no cache held). `journal` is left empty,
    /// its memory kept.
    pub fn rebuild_drain(
        &mut self,
        journal: &mut Vec<RecordBuf>,
        gate: Lsn,
    ) -> Result<(), DeterminismError> {
        let mut records = journal.drain(..);
        let Some(Record::Start {
            run,
            id,
            wtype,
            version,
            codec,
            started_ms,
            deadline_ms,
            policy,
            update,
            input,
            meta,
            ..
        }) = records.next()
        else {
            return Err(not_started());
        };
        let info = RunInfo {
            run,
            id,
            wtype,
            version,
            codec,
            started_ms,
            deadline_ms,
            policy,
            input,
            trace: meta.and_then(|m| m.trace),
        };
        self.reset(info, update.map(|u| (u.name, u.id, u.payload)), gate);
        for rec in records {
            self.load(&rec)?;
        }
        Ok(())
    }

    /// Makes the machine a new run's, `info`'s, before its journal loads.
    fn reset(&mut self, new_info: RunInfo, update: Option<(Name, Name, Payload)>, gate: Lsn) {
        // Every field, so that a new one cannot be left over from the
        // previous run.
        let Self {
            info,
            next_seq,
            recorded,
            max_recorded,
            replay,
            pending,
            live,
            early,
            signals,
            progress,
            patches,
            updates,
            abandoned,
            cancel_queued,
            last_cancel_position,
            done,
            names: _,
            last_name: _,
            streams,
            gate: machine_gate,
        } = self;
        *info = new_info;
        *next_seq = 0;
        recorded.clear();
        *max_recorded = None;
        replay.clear();
        pending.clear();
        live.clear();
        early.clear();
        signals.clear();
        progress.clear();
        patches.clear();
        *updates = 0;
        abandoned.clear();
        *cancel_queued = false;
        *last_cancel_position = None;
        *done = None;
        streams.clear();
        *machine_gate = gate;
        if let Some((name, id, payload)) = update {
            self.replay.push_back(ReplayItem::Update {
                ordinal: 0,
                name,
                id,
                payload,
            });
            self.updates = 1;
        }
    }

    /// Lets go of the run: its collections are emptied (keeping their
    /// memory) and its payloads and names dropped, so a machine kept for
    /// [`Self::rebuild`] holds nothing of it.
    pub fn release(&mut self) {
        let blank = blank_name();
        self.info.id = blank.clone();
        self.info.wtype = blank;
        self.info.input = EMPTY.with(Payload::clone);
        self.recorded.clear();
        self.replay.clear();
        self.pending.clear();
        self.live.clear();
        self.early.clear();
        self.signals.clear();
        self.progress.clear();
        self.patches.clear();
        self.abandoned.clear();
        self.done = None;
        self.streams.clear();
    }

    /// Whether the machine is small enough to keep for [`Self::rebuild`]
    /// (clearing a collection costs its capacity, not its length).
    pub fn reusable(&self) -> bool {
        const MAX: usize = 64;
        self.recorded.capacity() <= MAX
            && self.replay.capacity() <= MAX
            && self.pending.capacity() <= MAX
            && self.live.capacity() <= MAX
            && self.early.capacity() <= MAX
            && self.signals.capacity() <= MAX
            && self.progress.capacity() <= MAX
            && self.patches.capacity() <= MAX
            && self.abandoned.capacity() <= MAX
            && self.streams.capacity() <= MAX
    }

    fn mark(
        &mut self,
        seq: Seq,
        kind: OpKind,
        name: &Name,
    ) -> Result<&mut Recorded, DeterminismError> {
        self.max_recorded = Some(self.max_recorded.map_or(seq, |m| m.max(seq)));
        let entry = self.recorded.get_or_insert_with(seq, || Recorded {
            kind,
            name: name.clone(),
            attempt: 0,
            retry_at_ms: 0,
            base: 0,
            marked: false,
            value: None,
            delivered: false,
        });
        if entry.kind == OpKind::Step && kind == OpKind::Await && !entry.delivered && !entry.marked
        {
            // Attempts came first; the operation turned out to be an async
            // completion.
            entry.kind = OpKind::Await;
        }
        if !compatible(entry, kind) || (!name.is_empty() && *entry.name != **name) {
            return Err(DeterminismError {
                seq: Some(seq),
                message: format!(
                    "journal is inconsistent: {} '{}' and {} '{}' share a position",
                    entry.kind.as_str(),
                    entry.name,
                    kind.as_str(),
                    name
                ),
            });
        }
        if !name.is_empty() && entry.name.is_empty() {
            entry.name = name.clone();
        }
        Ok(entry)
    }

    fn load(&mut self, rec: &RecordBuf) -> Result<(), DeterminismError> {
        match rec {
            Record::Step {
                seq,
                name,
                status,
                payload,
                ..
            } => {
                self.mark(*seq, OpKind::Step, name)?.delivered = true;
                self.replay.push_back(ReplayItem::Op {
                    seq: *seq,
                    outcome: Outcome {
                        status: *status,
                        payload: payload.clone(),
                    },
                });
            }
            Record::Attempt {
                seq,
                name,
                attempt,
                retry_at_ms,
                ..
            } => {
                let r = self.mark(*seq, OpKind::Step, name)?;
                r.attempt = *attempt;
                r.retry_at_ms = *retry_at_ms;
            }
            Record::Resume { seq, attempt, .. } => {
                let r = self.mark(*seq, OpKind::Step, &blank_name())?;
                r.retry_at_ms = 0;
                r.base = *attempt;
            }
            Record::Timer { seq, name, .. } => self.mark(*seq, OpKind::Sleep, name)?.marked = true,
            Record::Fired { seq, .. } => {
                self.mark(*seq, OpKind::Sleep, &blank_name())?.delivered = true;
                self.replay.push_back(ReplayItem::Op {
                    seq: *seq,
                    outcome: Outcome::empty(Status::Ok),
                });
            }
            Record::Signal { payload, .. } => self.signals.push(payload.clone()),
            Record::Wait { seq, name, .. } => self.mark(*seq, OpKind::Wait, name)?.marked = true,
            Record::Waited {
                seq,
                outcome: WaitOutcome::Abandoned,
                ..
            } => {
                self.mark(*seq, OpKind::Wait, &blank_name())?;
                self.abandoned.insert(*seq);
            }
            Record::Waited { seq, outcome, .. } => {
                self.mark(*seq, OpKind::Wait, &blank_name())?.delivered = true;
                let outcome = match outcome {
                    WaitOutcome::Signal(ordinal) => Outcome::ok(
                        usize::try_from(*ordinal)
                            .ok()
                            .and_then(|i| self.signals.get(i))
                            .cloned()
                            .ok_or_else(|| DeterminismError {
                                seq: Some(*seq),
                                message: "wait consumed a signal the journal does not hold".into(),
                            })?,
                    ),
                    WaitOutcome::TimedOut | WaitOutcome::Abandoned => {
                        Outcome::empty(Status::TimedOut)
                    }
                };
                self.replay.push_back(ReplayItem::Op { seq: *seq, outcome });
            }
            Record::Value {
                seq, kind, value, ..
            } => {
                let r = self.mark(*seq, OpKind::Value, &blank_name())?;
                r.marked = true;
                r.value = Some(value.clone());
                r.attempt = u32::from(*kind);
            }
            Record::Child { seq, wtype, .. } => {
                self.mark(*seq, OpKind::Child, wtype)?.marked = true
            }
            Record::ChildDone {
                seq,
                status,
                payload,
                ..
            } => {
                self.mark(*seq, OpKind::Child, &blank_name())?.delivered = true;
                self.replay.push_back(ReplayItem::Op {
                    seq: *seq,
                    outcome: Outcome {
                        status: *status,
                        payload: payload.clone(),
                    },
                });
            }
            Record::Send { seq, name, .. } => {
                let r = self.mark(*seq, OpKind::Send, name)?;
                r.marked = true;
                r.value = Some(empty());
            }
            Record::Cancel { reason, .. } => self.replay.push_back(ReplayItem::Cancel {
                reason: reason.clone(),
            }),
            Record::Done {
                status, payload, ..
            } => self.done = Some((*status, payload.clone())),
            Record::Update {
                name, id, payload, ..
            } => {
                let ordinal = self.updates;
                self.updates += 1;
                self.replay.push_back(ReplayItem::Update {
                    ordinal,
                    name: name.clone(),
                    id: id.clone(),
                    payload: payload.clone(),
                });
            }
            Record::UpdateDone { seq, ordinal, .. } => {
                let r = self.mark(*seq, OpKind::UpdateDone, &blank_name())?;
                r.marked = true;
                r.value = Some(Arc::from(&ordinal.to_le_bytes()[..]));
            }
            Record::Heartbeat { seq, payload, .. } => {
                self.progress.insert(*seq, payload.clone());
            }
            Record::Awaiting {
                seq, name, attempt, ..
            } => {
                let r = self.mark(*seq, OpKind::Await, name)?;
                r.marked = true;
                r.attempt = *attempt;
            }
            Record::Completed {
                seq,
                status,
                payload,
                ..
            } => {
                self.mark(*seq, OpKind::Await, &blank_name())?.delivered = true;
                self.replay.push_back(ReplayItem::Op {
                    seq: *seq,
                    outcome: Outcome {
                        status: *status,
                        payload: payload.clone(),
                    },
                });
            }
            Record::Patch { at, id, .. } => {
                self.patches.insert((*at, id.clone()));
            }
            Record::Attributes { seq, .. } => {
                let r = self.mark(*seq, OpKind::Attributes, &blank_name())?;
                r.marked = true;
                r.value = Some(empty());
            }
            Record::Chunk {
                seq,
                stream: name,
                offset,
                ..
            } => {
                let st = self.stream_state(name);
                st.len = st.len.max(offset.saturating_add(1));
                if *seq == stream::WORKFLOW {
                    st.replay_left += 1;
                } else {
                    add_chunk(&mut st.by_step, *seq);
                }
            }
            Record::StreamEnd {
                seq, stream: name, ..
            } => {
                let st = self.stream_state(name);
                st.closed.get_or_insert(*seq);
            }
            Record::Start { .. } => {
                return Err(DeterminismError {
                    seq: None,
                    message: "journal holds a second start record".into(),
                });
            }
        }
        Ok(())
    }

    /// A fresh run: its journal is just the `Start` record at `lsn`.
    pub fn fresh(start: &RecordBuf, lsn: Lsn) -> Result<Self, DeterminismError> {
        Self::new(std::slice::from_ref(start), lsn)
    }

    pub fn info(&self) -> &RunInfo {
        &self.info
    }

    pub fn run(&self) -> RunId {
        self.info.run
    }

    /// The recorded result, if the journal already holds `Done`.
    pub fn recorded_done(&self) -> Option<&(Status, Payload)> {
        self.done.as_ref()
    }

    pub fn is_replaying(&self) -> bool {
        !self.replay.is_empty()
    }

    /// Operations started and not yet resolved.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// True while some started operation will be resolved by live work.
    /// Completions, updates or cancellations wait to be delivered to code.
    pub fn has_queued_deliveries(&self) -> bool {
        !self.live.is_empty()
    }

    /// The next delivery is `seq`'s live result and nothing could come
    /// before or with it: nothing left to replay, no cancellation, no
    /// other completion or update queued, no other operation pending, and
    /// the step's result is what its completion queued.
    pub fn delivers_only(&self, seq: Seq) -> bool {
        self.replay.is_empty()
            && !self.cancel_queued
            && self.pending.len() == 1
            && self.live.len() == 1
            && matches!(
                self.live.front(),
                Some(LiveItem::Complete {
                    seq: s,
                    completion: Completion::Step { .. },
                }) if *s == seq
            )
            && self
                .pending
                .get(&seq)
                .is_some_and(|p| p.source == Source::Live && p.kind == OpKind::Step)
    }

    /// A live step's result taken at once: when
    /// it is the only delivery the run could take next (nothing left to
    /// replay, no cancellation, no other operation pending, nothing queued,
    /// no stream of the step to end), the step's pending entry goes as
    /// [`next`](Self::next) would remove it on delivering the completion,
    /// and the record to append comes back. `None` changes nothing: queue
    /// the completion with [`complete`](Self::complete) instead.
    /// A name [`take_live_step`](Self::take_live_step) handed out, back
    /// to the cache the next operation of that name takes it from.
    pub fn return_name(&mut self, name: Name) {
        if self.last_name.is_none() {
            self.last_name = Some(name);
        }
    }

    pub fn take_live_step(&mut self, seq: Seq) -> Option<LiveRecord> {
        if !self.replay.is_empty()
            || self.cancel_queued
            || self.pending.len() != 1
            || !self.live.is_empty()
            || self.owns_streams(seq)
        {
            return None;
        }
        match self.pending.get(&seq) {
            Some(p) if p.source == Source::Live && p.kind == OpKind::Step && !p.queued => {}
            _ => return None,
        }
        let p = self.pending.remove(&seq)?;
        Some(LiveRecord::Step {
            name: p.name,
            attempt: p.attempt,
        })
    }

    pub fn has_live_pending(&self) -> bool {
        self.pending.values().any(|p| p.source == Source::Live) || !self.live.is_empty()
    }

    /// Nothing is pending, queued or left to replay: the code has started
    /// no operation it waits for (an object between calls).
    pub fn is_idle(&self) -> bool {
        self.replay.is_empty() && self.live.is_empty() && self.pending.is_empty()
    }

    /// Every pending operation waits on time, a signal, a child or an
    /// outside completion, nothing is queued for delivery and replay is
    /// done: the run can be parked.
    pub fn is_suspended(&self) -> bool {
        self.replay.is_empty()
            && self.live.is_empty()
            && !self.pending.is_empty()
            && self.pending.values().all(|p| {
                matches!(p.kind, OpKind::Sleep | OpKind::Wait | OpKind::Child)
                    || (p.kind == OpKind::Await && p.waiting)
                    || p.paused
            })
    }

    pub fn next_seq(&self) -> Seq {
        self.next_seq
    }

    /// Journal operations the code has started so far. Deterministic, so
    /// code may decide on it (for example, when to continue as new).
    pub fn journal_size(&self) -> u32 {
        self.next_seq
    }

    /// The idempotency key for operation `seq`: stable across retries and
    /// recovery, distinct across runs that reuse a workflow ID (including a
    /// run that replaced this one by reset).
    pub fn key(&self, seq: Seq) -> String {
        format!("{}/{}/{}", self.info.id, self.info.run, seq)
    }

    /// The name of a started, unresolved operation (a running step), for
    /// logs.
    pub fn pending_name(&self, seq: Seq) -> Option<&str> {
        self.pending.get(&seq).map(|p| &*p.name)
    }

    fn take_seq(&mut self) -> Seq {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    fn check(
        &self,
        seq: Seq,
        kind: OpKind,
        name: &str,
    ) -> Result<Option<&Recorded>, DeterminismError> {
        // Live code (nothing recorded to replay) has nothing to check.
        if self.recorded.is_empty() {
            return Ok(None);
        }
        match self.recorded.get(&seq) {
            Some(r)
                if !compatible(r, kind)
                    || (!matches!(
                        kind,
                        OpKind::Value | OpKind::UpdateDone | OpKind::Attributes
                    ) && *r.name != *name) =>
            {
                Err(mismatch(seq, r, kind, name))
            }
            other => Ok(other),
        }
    }

    /// `name` as a shared string, interned for the run.
    fn intern(&mut self, name: &str) -> Name {
        // The cached name is moved out, not cloned: a step that completes
        // inline gives it back ([`return_name`](Self::return_name)), so a
        // chain of steps of one name moves no reference count.
        if self.last_name.as_deref() == Some(name) {
            return self.last_name.take().expect("checked");
        }
        let n = match self.names.get(name) {
            Some(n) => n.clone(),
            None => {
                let n: Name = Arc::from(name);
                if self.names.len() < INTERNED_NAMES {
                    self.names.insert(n.clone());
                }
                n
            }
        };
        self.last_name = Some(n.clone());
        n
    }

    fn add_pending(&mut self, seq: Seq, kind: OpKind, name: &str, attempt: u32, source: Source) {
        let name = self.intern(name);
        self.pending.insert(
            seq,
            PendingOp {
                kind,
                name,
                attempt,
                source,
                waiting: false,
                queued: false,
                paused: false,
            },
        );
    }

    pub fn begin_step(&mut self, name: &str) -> Result<(Seq, StepStart), DeterminismError> {
        let seq = self.take_seq();
        // The journal numbers a step's attempts across resumes; the code
        // counts them from its last resume, which its retry budget does.
        let (start, source, attempt) = match self.check(seq, OpKind::Step, name)? {
            Some(r) if r.delivered => (StepStart::Replay, Source::Replay, 0),
            Some(r) => (
                StepStart::Run {
                    attempt: r.attempt + 1 - r.base,
                    not_before_ms: r.retry_at_ms,
                },
                Source::Live,
                r.attempt + 1,
            ),
            None => (
                StepStart::Run {
                    attempt: 1,
                    not_before_ms: 0,
                },
                Source::Live,
                1,
            ),
        };
        self.add_pending(seq, OpKind::Step, name, attempt, source);
        if let StepStart::Run {
            not_before_ms: PAUSED,
            ..
        } = start
            && let Some(p) = self.pending.get_mut(&seq)
        {
            p.paused = true;
        }
        self.take_early(seq);
        Ok((seq, start))
    }

    /// Starts an async completion: like a step whose result comes from
    /// outside. The function that hands out the token runs as a step does.
    pub fn begin_await(&mut self, name: &str) -> Result<(Seq, AwaitStart), DeterminismError> {
        let seq = self.take_seq();
        let start = match self.check(seq, OpKind::Await, name)? {
            Some(r) if r.delivered => AwaitStart::Replay,
            Some(r) if r.marked && r.kind == OpKind::Await => {
                AwaitStart::Waiting { attempt: r.attempt }
            }
            Some(r) => AwaitStart::Run {
                attempt: r.attempt + 1,
                not_before_ms: r.retry_at_ms,
            },
            None => AwaitStart::Run {
                attempt: 1,
                not_before_ms: 0,
            },
        };
        let (attempt, source, waiting) = match start {
            AwaitStart::Replay => (0, Source::Replay, false),
            AwaitStart::Waiting { attempt } => (attempt, Source::Live, true),
            AwaitStart::Run { attempt, .. } => (attempt, Source::Live, false),
        };
        self.add_pending(seq, OpKind::Await, name, attempt, source);
        if let Some(p) = self.pending.get_mut(&seq) {
            p.waiting = waiting;
        }
        self.take_early(seq);
        Ok((seq, start))
    }

    /// The token function of async completion `seq` returned: the token for
    /// its current attempt is out. Returns the `Awaiting` record to append.
    pub fn awaiting(&mut self, seq: Seq) -> Option<RecordBuf> {
        let run = self.info.run;
        let p = self.pending.get_mut(&seq)?;
        if p.source != Source::Live || p.kind != OpKind::Await || p.waiting {
            return None;
        }
        p.waiting = true;
        Some(Record::Awaiting {
            run,
            seq,
            name: p.name.clone(),
            attempt: p.attempt,
        })
    }

    /// The attempt an async completion's current token names, if `seq` is
    /// waiting for one.
    pub fn await_attempt(&self, seq: Seq) -> Option<u32> {
        self.pending
            .get(&seq)
            .filter(|p| p.kind == OpKind::Await && p.source == Source::Live)
            .map(|p| p.attempt)
    }

    /// A step attempt failed and will be retried. Returns the `Attempt`
    /// record to append now; the shard arms the retry timer from it.
    pub fn step_retry(&mut self, seq: Seq, retry_at_ms: u64, error: Payload) -> Option<RecordBuf> {
        let p = self.pending.get_mut(&seq)?;
        if p.source != Source::Live {
            return None;
        }
        let rec = Record::Attempt {
            run: self.info.run,
            seq,
            name: p.name.clone(),
            attempt: p.attempt,
            retry_at_ms,
            error,
        };
        p.attempt += 1;
        p.waiting = false;
        p.paused = retry_at_ms == PAUSED;
        Some(rec)
    }

    /// Whether step `seq` is paused (its retries ran out and no resume
    /// has come).
    pub fn step_paused(&self, seq: Seq) -> bool {
        self.pending.get(&seq).is_some_and(|p| p.paused)
    }

    /// Whether step `seq` still waits for its code to run it (not resolved,
    /// for example by a cancellation).
    pub fn step_pending(&self, seq: Seq) -> bool {
        self.pending
            .get(&seq)
            .is_some_and(|p| p.source == Source::Live && !p.queued)
    }

    /// Whether any of the run's steps is paused, whether or not its code
    /// has reached it yet.
    pub fn is_paused(&self) -> bool {
        self.pending.values().any(|p| p.paused) || self.recorded_paused().next().is_some()
    }

    /// Recorded steps the code has not reached yet that are paused.
    fn recorded_paused(&self) -> impl Iterator<Item = (Seq, &Recorded)> {
        self.recorded.iter().filter(|(seq, r)| {
            r.kind == OpKind::Step
                && r.retry_at_ms == PAUSED
                && !r.delivered
                && self.pending.get(seq).is_none()
        })
    }

    /// Resumes every paused step: each runs its next attempt at once, with
    /// a fresh retry budget. Returns the `Resume` records to append, in
    /// step order (none if nothing was paused).
    pub fn resume(&mut self, by: Option<Payload>) -> Vec<RecordBuf> {
        let run = self.info.run;
        let mut seqs: Vec<(Seq, u32)> = self
            .pending
            .iter()
            .filter(|(_, p)| p.paused)
            .map(|(seq, p)| (seq, p.attempt - 1))
            .collect();
        let waiting: Vec<(Seq, u32)> = self
            .recorded_paused()
            .map(|(seq, r)| (seq, r.attempt))
            .collect();
        for (seq, _) in &seqs {
            if let Some(p) = self.pending.get_mut(seq) {
                p.paused = false;
            }
        }
        for &(seq, attempt) in &waiting {
            if let Some(r) = self.recorded.get_mut(&seq) {
                r.retry_at_ms = 0;
                r.base = attempt;
            }
        }
        seqs.extend(waiting);
        seqs.sort_unstable();
        seqs.into_iter()
            .map(|(seq, attempt)| Record::Resume {
                run,
                seq,
                attempt,
                by: by.clone(),
            })
            .collect()
    }

    /// The attempt number the next run of step `seq` will be.
    pub fn step_attempt(&self, seq: Seq) -> Option<u32> {
        self.pending.get(&seq).map(|p| p.attempt)
    }

    /// A running step reported progress. Returns the `Heartbeat` record (the
    /// host coalesces: only the latest per step is appended).
    pub fn heartbeat(&mut self, seq: Seq, payload: Payload) -> Option<RecordBuf> {
        let p = self.pending.get(&seq)?;
        if p.source != Source::Live || !matches!(p.kind, OpKind::Step | OpKind::Await) {
            return None;
        }
        let attempt = p.attempt;
        self.progress.insert(seq, payload.clone());
        Some(Record::Heartbeat {
            run: self.info.run,
            seq,
            attempt,
            payload,
        })
    }

    /// The latest progress step `seq` reported, in this life or a recorded
    /// earlier one.
    pub fn progress(&self, seq: Seq) -> Option<Payload> {
        self.progress.get(&seq).cloned()
    }

    /// The stream named `name`, created empty if the run has none yet.
    fn stream_state(&mut self, name: &str) -> &mut StreamState {
        match self.streams.iter().position(|s| &*s.name == name) {
            Some(i) => &mut self.streams[i],
            None => {
                let name = self.intern(name);
                self.streams.push(StreamState {
                    name,
                    len: 0,
                    closed: None,
                    replay_left: 0,
                    by_step: Vec::new(),
                    owner: StreamOwner::None,
                });
                let last = self.streams.len() - 1;
                &mut self.streams[last]
            }
        }
    }

    /// The attempt of the live step at `seq` (a step, or an async
    /// completion whose function is running), or an error naming `what`.
    fn stream_step(&self, seq: Seq, what: &str) -> Result<u32, String> {
        match self.pending.get(&seq) {
            Some(p)
                if p.source == Source::Live
                    && matches!(p.kind, OpKind::Step | OpKind::Await)
                    && !p.waiting =>
            {
                Ok(p.attempt)
            }
            _ => Err(format!(
                "{what} is called from a step that is no longer running"
            )),
        }
    }

    /// Workflow code (`step: None`) or the function of the step at `step`
    /// opens stream `name`. A stream belongs to whoever opens it first in
    /// this life: workflow code, whose steps may append to it as well, or
    /// one step, whose final result completes it.
    pub fn stream_open(&mut self, name: &str, step: Option<Seq>) -> Result<StreamOpen, String> {
        if let Some(s) = step {
            self.stream_step(s, "ctx.stream")?;
        }
        let st = self.stream_state(name);
        match (step, st.owner) {
            (None, StreamOwner::Step(t)) => {
                return Err(format!(
                    "stream '{name}' is written by the step at journal position {t}"
                ));
            }
            (None, _) => st.owner = StreamOwner::Workflow,
            (Some(s), StreamOwner::Step(t)) if t != s => {
                return Err(format!(
                    "stream '{name}' is written by the step at journal position {t}"
                ));
            }
            (Some(s), StreamOwner::None) if st.closed.is_none() => st.owner = StreamOwner::Step(s),
            _ => {}
        }
        let recorded = match step {
            Some(s) => chunks_of(&st.by_step, s),
            None => 0,
        };
        Ok(StreamOpen {
            recorded,
            closed: st.closed.is_some(),
        })
    }

    /// Appends a chunk to stream `name` from workflow code (`step: None`)
    /// or from the function of the step at `step`. Returns the `Chunk`
    /// record to append, or none when there is nothing to append: workflow
    /// code replaying an append its journal holds, or a step whose earlier
    /// attempt completed the stream.
    pub fn stream_append(
        &mut self,
        name: &str,
        step: Option<Seq>,
        payload: Payload,
    ) -> Result<Option<RecordBuf>, String> {
        let attempt = match step {
            Some(s) => self.stream_step(s, "stream append")?,
            None => 0,
        };
        let run = self.info.run;
        let st = self.stream_state(name);
        let seq = match step {
            None => {
                if st.replay_left > 0 {
                    st.replay_left -= 1;
                    return Ok(None);
                }
                if let StreamOwner::Step(t) = st.owner {
                    return Err(format!(
                        "stream '{name}' is written by the step at journal position {t}"
                    ));
                }
                stream::WORKFLOW
            }
            Some(s) => {
                if st.closed == Some(s) {
                    return Ok(None);
                }
                if let StreamOwner::Step(t) = st.owner
                    && t != s
                {
                    return Err(format!(
                        "stream '{name}' is written by the step at journal position {t}"
                    ));
                }
                s
            }
        };
        if st.closed.is_some() {
            return Err(format!("stream '{name}' is closed"));
        }
        match (seq, st.owner) {
            (stream::WORKFLOW, _) => st.owner = StreamOwner::Workflow,
            (s, StreamOwner::None) => st.owner = StreamOwner::Step(s),
            _ => {}
        }
        if seq != stream::WORKFLOW {
            add_chunk(&mut st.by_step, seq);
        }
        let offset = st.len;
        st.len += 1;
        Ok(Some(Record::Chunk {
            run,
            seq,
            attempt,
            stream: st.name.clone(),
            offset,
            payload,
        }))
    }

    /// Completes stream `name` from workflow code (`step: None`) or a step.
    /// Returns the `StreamEnd` record to append, or none if the stream is
    /// complete already (replay included).
    pub fn stream_close(
        &mut self,
        name: &str,
        step: Option<Seq>,
    ) -> Result<Option<RecordBuf>, String> {
        if let Some(s) = step {
            self.stream_step(s, "stream close")?;
        }
        let run = self.info.run;
        let st = self.stream_state(name);
        if st.closed.is_some() {
            return Ok(None);
        }
        let seq = step.unwrap_or(stream::WORKFLOW);
        match (step, st.owner) {
            (None, StreamOwner::Step(t)) => {
                return Err(format!(
                    "stream '{name}' is written by the step at journal position {t}"
                ));
            }
            (Some(s), StreamOwner::Step(t)) if t != s => {
                return Err(format!(
                    "stream '{name}' is written by the step at journal position {t}"
                ));
            }
            _ => {}
        }
        st.closed = Some(seq);
        if st.owner == StreamOwner::Step(seq) {
            st.owner = StreamOwner::None;
        }
        Ok(Some(Record::StreamEnd {
            run,
            seq,
            stream: st.name.clone(),
            chunks: st.len,
            status: Status::Ok,
        }))
    }

    /// The step at `seq` got its final result (`status`): the streams it
    /// opened and did not close are complete. Returns their `StreamEnd`
    /// records, to append just before the result's record.
    pub fn stream_ends(&mut self, seq: Seq, status: Status) -> Vec<RecordBuf> {
        let run = self.info.run;
        let mut out = Vec::new();
        for st in &mut self.streams {
            if st.owner != StreamOwner::Step(seq) {
                continue;
            }
            st.owner = StreamOwner::None;
            if st.closed.is_none() {
                st.closed = Some(seq);
                out.push(Record::StreamEnd {
                    run,
                    seq,
                    stream: st.name.clone(),
                    chunks: st.len,
                    status,
                });
            }
        }
        out
    }

    /// Whether some stream belongs to the step at `seq` in this life.
    pub fn owns_streams(&self, seq: Seq) -> bool {
        self.streams
            .iter()
            .any(|s| s.owner == StreamOwner::Step(seq))
    }

    /// Chunks of stream `name` the journal and this life hold so far.
    pub fn stream_len(&self, name: &str) -> Option<u64> {
        self.streams
            .iter()
            .find(|s| &*s.name == name)
            .map(|s| s.len)
    }

    pub fn begin_sleep(
        &mut self,
        name: &str,
        fire_at_ms: u64,
    ) -> Result<(Seq, Arm), DeterminismError> {
        let seq = self.take_seq();
        let arm = match self.check(seq, OpKind::Sleep, name)? {
            Some(r) if r.delivered => Arm::Replay,
            Some(r) if r.marked => Arm::Armed,
            _ => Arm::Append(Record::Timer {
                run: self.info.run,
                seq,
                name: self.intern(name),
                fire_at_ms,
            }),
        };
        self.register(seq, OpKind::Sleep, name, &arm);
        Ok((seq, arm))
    }

    pub fn begin_wait(
        &mut self,
        name: &str,
        deadline_ms: u64,
    ) -> Result<(Seq, Arm), DeterminismError> {
        let seq = self.take_seq();
        let arm = match self.check(seq, OpKind::Wait, name)? {
            Some(r) if r.delivered => Arm::Replay,
            Some(r) if r.marked => Arm::Armed,
            _ => Arm::Append(Record::Wait {
                run: self.info.run,
                seq,
                name: self.intern(name),
                deadline_ms,
            }),
        };
        self.register(seq, OpKind::Wait, name, &arm);
        Ok((seq, arm))
    }

    pub fn begin_child(
        &mut self,
        wtype: &str,
        id: &str,
        flags: u8,
        deadline_ms: u64,
        input: Payload,
    ) -> Result<(Seq, Arm), DeterminismError> {
        let seq = self.take_seq();
        let arm = match self.check(seq, OpKind::Child, wtype)? {
            Some(r) if r.delivered => Arm::Replay,
            Some(r) if r.marked => Arm::Armed,
            _ => Arm::Append(Record::Child {
                run: self.info.run,
                seq,
                wtype: Arc::from(wtype),
                id: Arc::from(id),
                flags,
                deadline_ms,
                input,
            }),
        };
        if flags & crate::record::policy::DETACHED != 0 {
            // A detached child resolves immediately with nothing.
            return Ok((seq, arm));
        }
        self.register(seq, OpKind::Child, wtype, &arm);
        Ok((seq, arm))
    }

    fn register(&mut self, seq: Seq, kind: OpKind, name: &str, arm: &Arm) {
        let source = if matches!(arm, Arm::Replay) {
            Source::Replay
        } else {
            Source::Live
        };
        self.add_pending(seq, kind, name, 0, source);
        self.take_early(seq);
    }

    fn take_early(&mut self, seq: Seq) {
        if self.early.is_empty() {
            return;
        }
        if let Some(c) = self.early.remove(&seq)
            && self.is_live_pending(seq)
        {
            if let Completion::Completed { attempt, .. } = &c
                && self.await_attempt(seq) != Some(*attempt)
            {
                return;
            }
            self.push_complete(seq, c);
        }
    }

    fn push_complete(&mut self, seq: Seq, completion: Completion) {
        if let Some(p) = self.pending.get_mut(&seq) {
            p.queued = true;
        }
        self.live.push_back(LiveItem::Complete { seq, completion });
    }

    /// A recorded nondeterministic value: the recorded one on replay, else
    /// `make()`'s, returned together with the `Value` record to append.
    pub fn value(
        &mut self,
        kind: u8,
        make: impl FnOnce() -> Payload,
    ) -> Result<(Seq, Payload, Option<RecordBuf>), DeterminismError> {
        let seq = self.take_seq();
        if let Some(r) = self.check(seq, OpKind::Value, "")? {
            if r.attempt != u32::from(kind) {
                return Err(DeterminismError {
                    seq: Some(seq),
                    message: format!(
                        "the journal recorded value kind {} here but the code asked for kind {kind}",
                        r.attempt
                    ),
                });
            }
            return Ok((seq, r.value.clone().unwrap_or_else(empty), None));
        }
        let value = make();
        let rec = Record::Value {
            run: self.info.run,
            seq,
            kind,
            value: value.clone(),
        };
        Ok((seq, value, Some(rec)))
    }

    /// Convenience for `ctx.now()`: Unix milliseconds as 8 little-endian bytes.
    pub fn now(&mut self, now_ms: u64) -> Result<(u64, Option<RecordBuf>), DeterminismError> {
        let (_, v, rec) = self.value(value_kind::NOW, || Arc::from(&now_ms.to_le_bytes()[..]))?;
        let mut b = [0u8; 8];
        if v.len() == 8 {
            b.copy_from_slice(&v);
        }
        Ok((u64::from_le_bytes(b), rec))
    }

    /// Sends a signal to another workflow once this record is durable.
    pub fn send(
        &mut self,
        target: &str,
        name: &str,
        payload: Payload,
    ) -> Result<(Seq, Option<RecordBuf>), DeterminismError> {
        let seq = self.take_seq();
        if self.check(seq, OpKind::Send, name)?.is_some() {
            return Ok((seq, None));
        }
        Ok((
            seq,
            Some(Record::Send {
                run: self.info.run,
                seq,
                target: Arc::from(target),
                name: Arc::from(name),
                payload,
            }),
        ))
    }

    /// Records search attributes (a JSON object).
    pub fn set_attributes(
        &mut self,
        payload: Payload,
    ) -> Result<(Seq, Option<RecordBuf>), DeterminismError> {
        let seq = self.take_seq();
        if self.check(seq, OpKind::Attributes, "")?.is_some() {
            return Ok((seq, None));
        }
        Ok((
            seq,
            Some(Record::Attributes {
                run: self.info.run,
                seq,
                payload,
            }),
        ))
    }

    /// Whether the code at this point is past everything its journal
    /// recorded (it is running new code, not replaying old decisions).
    fn beyond_journal(&self) -> bool {
        self.max_recorded.is_none_or(|m| m < self.next_seq)
    }

    /// `ctx.patched(id)`: true for new runs and for running ones the first
    /// time their code reaches this point after the patch was deployed;
    /// false while replaying a journal that passed this point before.
    /// Returns the `Patch` marker to append when the decision is new.
    pub fn patched(&mut self, id: &str) -> (bool, Option<RecordBuf>) {
        let at = self.next_seq;
        let key = (at, Name::from(id));
        if self.patches.contains(&key) {
            return (true, None);
        }
        if self.beyond_journal() {
            self.patches.insert(key);
            return (
                true,
                Some(Record::Patch {
                    run: self.info.run,
                    at,
                    id: Arc::from(id),
                }),
            );
        }
        (false, None)
    }

    /// The handler for update `ordinal` returned. Returns the `UpdateDone`
    /// record to append (none on replay).
    pub fn update_done(
        &mut self,
        ordinal: u64,
        status: Status,
        payload: Payload,
    ) -> Result<(Seq, Option<RecordBuf>), DeterminismError> {
        let seq = self.take_seq();
        if let Some(r) = self.check(seq, OpKind::UpdateDone, "")? {
            let recorded = r
                .value
                .as_ref()
                .and_then(|v| <[u8; 8]>::try_from(&v[..]).ok())
                .map(u64::from_le_bytes);
            if recorded != Some(ordinal) {
                return Err(DeterminismError {
                    seq: Some(seq),
                    message: format!(
                        "the journal recorded the result of update #{} here but the code returned update #{ordinal}",
                        recorded.unwrap_or(u64::MAX)
                    ),
                });
            }
            return Ok((seq, None));
        }
        Ok((
            seq,
            Some(Record::UpdateDone {
                run: self.info.run,
                seq,
                ordinal,
                status,
                payload,
                state: empty(),
            }),
        ))
    }

    /// Queues a new update request. It reaches the code at its turn, where
    /// the host runs its validator.
    pub fn offer_update(&mut self, token: u64, name: Name, id: Name, payload: Payload) {
        self.live.push_back(LiveItem::Update {
            token,
            name,
            id,
            payload,
        });
    }

    /// The validator accepted a live update delivered by [`next`](Self::next).
    /// Returns its `Update` record, to append before starting the handler.
    pub fn accept_update(&mut self, name: Name, id: Name, payload: Payload) -> (u64, RecordBuf) {
        let ordinal = self.updates;
        self.updates += 1;
        (
            ordinal,
            Record::Update {
                run: self.info.run,
                name,
                id,
                payload,
                caller: None,
            },
        )
    }

    /// Reports a live completion. Ignored for operations that are not
    /// pending live (dropped, already resolved or replayed). Returns whether
    /// the completion was taken now.
    pub fn complete(&mut self, seq: Seq, completion: Completion) -> bool {
        if seq >= self.next_seq {
            // The code has not started this operation yet (replay is
            // behind); hold the completion until it does.
            self.early.insert(seq, completion);
            return false;
        }
        match self.pending.get_mut(&seq) {
            Some(p) if p.source == Source::Live => {
                if let Completion::Completed { attempt, .. } = &completion
                    && (p.kind != OpKind::Await || p.attempt != *attempt)
                {
                    return false;
                }
                if p.queued {
                    return false;
                }
                p.queued = true;
                self.live.push_back(LiveItem::Complete { seq, completion });
                true
            }
            _ => false,
        }
    }

    /// Whether `seq` is pending a live result.
    pub fn is_live_pending(&self, seq: Seq) -> bool {
        self.pending
            .get(&seq)
            .is_some_and(|p| p.source == Source::Live)
    }

    pub fn pending_kind(&self, seq: Seq) -> Option<OpKind> {
        self.pending.get(&seq).map(|p| p.kind)
    }

    /// A repeated request at the same code position has no new operation to
    /// cancel. A compensation step takes another sequence number and can be
    /// cancelled by a later request.
    pub fn cancellation_covers_current_operations(&self) -> bool {
        self.pending.is_empty() && self.last_cancel_position == Some(self.next_seq)
    }

    /// Queues cancellation. It reaches the code at its turn, like any
    /// delivery, so replay cancels exactly the same operations.
    pub fn request_cancel(&mut self, reason: Payload) {
        self.request_cancel_by(reason, None);
    }

    /// [`request_cancel`](Self::request_cancel), recording who asked on the
    /// `Cancel` record (audit).
    pub fn request_cancel_by(&mut self, reason: Payload, by: Option<Payload>) {
        if !self.cancel_queued && !self.cancellation_covers_current_operations() {
            self.cancel_queued = true;
            self.live.push_back(LiveItem::Cancel { reason, by });
        }
    }

    /// The code dropped the operation's future (for example, the losing side
    /// of a race). Its result will never be delivered. For a wait, returns
    /// the `Waited { Abandoned }` record to append, so the shard stops
    /// offering signals to it (none on replay: it is recorded).
    pub fn forget(&mut self, seq: Seq) -> Option<RecordBuf> {
        let p = self.pending.get(&seq)?;
        if p.source != Source::Live {
            return None;
        }
        let kind = p.kind;
        self.pending.remove(&seq);
        if kind != OpKind::Wait || !self.abandoned.insert(seq) {
            return None;
        }
        Some(Record::Waited {
            run: self.info.run,
            seq,
            outcome: WaitOutcome::Abandoned,
        })
    }

    /// The next delivery, if one may be made now. Call only when the code is
    /// quiescent; resolve the result, let the code run, then call again.
    pub fn next(&mut self) -> Next {
        if let Some(front) = self.replay.front() {
            return match front {
                ReplayItem::Op { seq, .. } => {
                    let seq = *seq;
                    match self.pending.get(&seq) {
                        Some(p) if p.source == Source::Replay => {
                            self.pending.remove(&seq);
                            let Some(ReplayItem::Op { outcome, .. }) = self.replay.pop_front()
                            else {
                                return Next::Stalled;
                            };
                            Next::Deliver {
                                seq,
                                outcome,
                                record: None,
                            }
                        }
                        _ => Next::Stalled,
                    }
                }
                ReplayItem::Cancel { .. } => {
                    let Some(ReplayItem::Cancel { reason }) = self.replay.pop_front() else {
                        return Next::Stalled;
                    };
                    self.last_cancel_position = Some(self.next_seq);
                    let seqs = self.drain_pending();
                    Next::CancelAll {
                        seqs,
                        reason,
                        record: None,
                    }
                }
                ReplayItem::Update { .. } => {
                    let Some(ReplayItem::Update {
                        ordinal,
                        name,
                        id,
                        payload,
                    }) = self.replay.pop_front()
                    else {
                        return Next::Stalled;
                    };
                    Next::Update {
                        ordinal,
                        name,
                        id,
                        payload,
                        live: None,
                    }
                }
            };
        }
        while let Some(item) = self.live.pop_front() {
            match item {
                LiveItem::Complete { seq, completion } => {
                    if !self
                        .pending
                        .get(&seq)
                        .is_some_and(|p| p.source == Source::Live)
                    {
                        continue;
                    }
                    let Some(p) = self.pending.remove(&seq) else {
                        continue;
                    };
                    let run = self.info.run;
                    let (outcome, record) = match completion {
                        Completion::Step { status, payload } => {
                            let record = if p.kind == OpKind::Await {
                                LiveRecord::Completed { attempt: p.attempt }
                            } else {
                                LiveRecord::Step {
                                    name: p.name,
                                    attempt: p.attempt,
                                }
                            };
                            return Next::DeliverLive {
                                seq,
                                outcome: Outcome { status, payload },
                                record,
                            };
                        }
                        Completion::Fired => {
                            (Outcome::empty(Status::Ok), Record::Fired { run, seq })
                        }
                        Completion::Signal { ordinal, payload } => (
                            Outcome::ok(payload),
                            Record::Waited {
                                run,
                                seq,
                                outcome: WaitOutcome::Signal(ordinal),
                            },
                        ),
                        Completion::WaitTimedOut => (
                            Outcome::empty(Status::TimedOut),
                            Record::Waited {
                                run,
                                seq,
                                outcome: WaitOutcome::TimedOut,
                            },
                        ),
                        Completion::Child { status, payload } => (
                            Outcome {
                                status,
                                payload: payload.clone(),
                            },
                            Record::ChildDone {
                                run,
                                seq,
                                status,
                                payload,
                            },
                        ),
                        Completion::Completed {
                            attempt,
                            status,
                            payload,
                        } => {
                            return Next::DeliverLive {
                                seq,
                                outcome: Outcome { status, payload },
                                record: LiveRecord::Completed { attempt },
                            };
                        }
                    };
                    return Next::Deliver {
                        seq,
                        outcome,
                        record: Some(record),
                    };
                }
                LiveItem::Cancel { reason, by } => {
                    self.cancel_queued = false;
                    self.last_cancel_position = Some(self.next_seq);
                    let seqs = self.drain_pending();
                    return Next::CancelAll {
                        seqs,
                        record: Some(Record::Cancel {
                            run: self.info.run,
                            reason: reason.clone(),
                            by,
                        }),
                        reason,
                    };
                }
                LiveItem::Update {
                    token,
                    name,
                    id,
                    payload,
                } => {
                    return Next::Update {
                        ordinal: self.updates,
                        name,
                        id,
                        payload,
                        live: Some(token),
                    };
                }
            }
        }
        Next::Idle
    }

    /// Update requests still queued (their callers are told the run ended).
    pub fn take_offered_updates(&mut self) -> Vec<u64> {
        let mut out = Vec::new();
        self.live.retain(|l| match l {
            LiveItem::Update { token, .. } => {
                out.push(*token);
                false
            }
            _ => true,
        });
        out
    }

    fn drain_pending(&mut self) -> Vec<Seq> {
        let mut seqs: Vec<Seq> = self.pending.keys().copied().collect();
        seqs.sort_unstable();
        self.pending.clear();
        seqs
    }

    /// Explains a stall the host has confirmed: the code is quiescent, no
    /// live work is pending, and the journal still has deliveries.
    pub fn stall_error(&self) -> Option<DeterminismError> {
        if self.has_live_pending() {
            return None;
        }
        match self.replay.front() {
            Some(ReplayItem::Op { seq, .. }) => {
                let what = self
                    .recorded
                    .get(seq)
                    .map(|r| format!("{} '{}'", r.kind.as_str(), r.name))
                    .unwrap_or_else(|| "an operation".into());
                Some(DeterminismError {
                    seq: Some(*seq),
                    message: format!(
                        "the journal delivers {what} next, but the code is waiting without having started it"
                    ),
                })
            }
            _ => None,
        }
    }

    /// Checks that the code may end here: its journal has nothing left to
    /// deliver.
    pub fn check_end(&self) -> Result<(), DeterminismError> {
        // Older journals can contain repeated cancels between the delivery
        // that settled the workflow's last future and the return of its code.
        if self.cancellation_covers_current_operations()
            && self
                .replay
                .iter()
                .all(|r| matches!(r, ReplayItem::Cancel { .. }))
        {
            return Ok(());
        }
        if let Some(item) = self.replay.front() {
            let seq = match item {
                ReplayItem::Op { seq, .. } => Some(*seq),
                _ => None,
            };
            return Err(DeterminismError {
                seq,
                message: format!(
                    "the workflow returned but its journal holds {} more deliveries",
                    self.replay.len()
                ),
            });
        }
        Ok(())
    }

    /// The code returned. Produces the `Done` record.
    pub fn finish(
        &mut self,
        status: Status,
        payload: Payload,
        finished_ms: u64,
    ) -> Result<RecordBuf, DeterminismError> {
        self.check_end()?;
        Ok(Record::Done {
            run: self.info.run,
            status,
            finished_ms,
            payload,
            by: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Payload {
        Arc::from(s.as_bytes())
    }

    fn start() -> RecordBuf {
        crate::record::start_record(
            1,
            Arc::from("wf"),
            Arc::from("t"),
            1,
            10,
            0,
            None,
            0,
            p("in"),
        )
    }

    /// Delivers everything deliverable, appending live records to `journal`.
    fn drain(m: &mut RunMachine, journal: &mut Vec<RecordBuf>) -> Vec<(Seq, Outcome)> {
        let mut out = Vec::new();
        loop {
            match m.next() {
                Next::Deliver {
                    seq,
                    outcome,
                    record,
                } => {
                    if let Some(r) = record {
                        journal.push(r);
                    }
                    out.push((seq, outcome));
                }
                Next::DeliverLive {
                    seq,
                    outcome,
                    record,
                } => {
                    journal.push(record.record(m.info().run, seq, &outcome, None).to_buf());
                    out.push((seq, outcome));
                }
                Next::CancelAll {
                    seqs,
                    reason,
                    record,
                } => {
                    if let Some(r) = record {
                        journal.push(r);
                    }
                    for s in seqs {
                        out.push((
                            s,
                            Outcome {
                                status: Status::Cancelled,
                                payload: reason.clone(),
                            },
                        ));
                    }
                }
                Next::Update { .. } => {}
                Next::Stalled | Next::Idle => return out,
            }
        }
    }

    #[test]
    fn live_then_replay_in_journal_order() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (a, sa) = m.begin_step("a").unwrap();
        let (b, _) = m.begin_step("b").unwrap();
        assert_eq!(
            sa,
            StepStart::Run {
                attempt: 1,
                not_before_ms: 0
            }
        );
        // b finishes before a.
        m.complete(
            b,
            Completion::Step {
                status: Status::Ok,
                payload: p("B"),
            },
        );
        m.complete(
            a,
            Completion::Step {
                status: Status::Ok,
                payload: p("A"),
            },
        );
        let got = drain(&mut m, &mut journal);
        assert_eq!(got.iter().map(|g| g.0).collect::<Vec<_>>(), vec![b, a]);

        // Replay delivers b before a even though a is registered first.
        let mut r = RunMachine::new(&journal, 3).unwrap();
        assert_eq!(r.begin_step("a").unwrap().1, StepStart::Replay);
        assert_eq!(r.begin_step("b").unwrap().1, StepStart::Replay);
        let mut scratch = Vec::new();
        let got = drain(&mut r, &mut scratch);
        assert!(scratch.is_empty());
        assert_eq!(got[0], (b, Outcome::ok(p("B"))));
        assert_eq!(got[1], (a, Outcome::ok(p("A"))));
        assert!(!r.is_replaying());
    }

    #[test]
    fn mismatched_name_is_a_determinism_error() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (s, _) = m.begin_step("charge").unwrap();
        m.complete(
            s,
            Completion::Step {
                status: Status::Ok,
                payload: p("1"),
            },
        );
        drain(&mut m, &mut journal);
        let mut r = RunMachine::new(&journal, 2).unwrap();
        let err = r.begin_step("refund").unwrap_err();
        assert!(
            err.message.contains("'charge'") && err.message.contains("'refund'"),
            "{err}"
        );
    }

    #[test]
    fn live_completions_wait_for_replay() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (a, _) = m.begin_step("a").unwrap();
        let (_b, _) = m.begin_step("b").unwrap();
        m.complete(
            a,
            Completion::Step {
                status: Status::Ok,
                payload: p("A"),
            },
        );
        drain(&mut m, &mut journal);
        // Crash: b never finished. On replay, b runs live; its completion
        // must wait until a's recorded result has been delivered.
        let mut r = RunMachine::new(&journal, 2).unwrap();
        let (b2, sb) = {
            let (a2, sa) = r.begin_step("a").unwrap();
            assert_eq!((a2, sa), (a, StepStart::Replay));
            r.begin_step("b").unwrap()
        };
        assert!(matches!(sb, StepStart::Run { .. }));
        r.complete(
            b2,
            Completion::Step {
                status: Status::Ok,
                payload: p("B"),
            },
        );
        let mut more = Vec::new();
        let got = drain(&mut r, &mut more);
        assert_eq!(got[0].0, a);
        assert_eq!(got[1].0, b2);
        assert_eq!(more.len(), 1);
    }

    #[test]
    fn retries_resume_from_the_journal() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (s, _) = m.begin_step("flaky").unwrap();
        journal.push(m.step_retry(s, 500, p("e1")).unwrap());
        journal.push(m.step_retry(s, 900, p("e2")).unwrap());
        let mut r = RunMachine::new(&journal, 3).unwrap();
        assert_eq!(
            r.begin_step("flaky").unwrap().1,
            StepStart::Run {
                attempt: 3,
                not_before_ms: 900
            }
        );
    }

    #[test]
    fn a_paused_step_resumes_with_a_fresh_budget() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (s, _) = m.begin_step("charge").unwrap();
        journal.push(m.step_retry(s, 500, p("e1")).unwrap());
        assert!(!m.is_suspended());
        journal.push(m.step_retry(s, PAUSED, p("e2")).unwrap());
        assert!(m.step_paused(s) && m.is_paused() && m.is_suspended());
        // A crash while paused: the step is still paused.
        let mut r = RunMachine::new(&journal, 3).unwrap();
        assert!(r.is_paused());
        assert_eq!(
            r.begin_step("charge").unwrap().1,
            StepStart::Run {
                attempt: 3,
                not_before_ms: PAUSED
            }
        );
        assert!(r.step_paused(s) && r.is_suspended());
        // Live: the resume names the last attempt; the next is the 3rd in
        // the journal and the 1st of the new budget.
        let resumed = m.resume(Some(p("cli:ana")));
        assert_eq!(
            resumed,
            vec![Record::Resume {
                run: 1,
                seq: s,
                attempt: 2,
                by: Some(p("cli:ana")),
            }]
        );
        assert!(!m.step_paused(s) && !m.is_paused() && m.resume(None).is_empty());
        journal.extend(resumed);
        journal.push(m.step_retry(s, 900, p("e3")).unwrap());
        // Replay: attempts count from the resume; the journal's go on.
        let mut r = RunMachine::new(&journal, 4).unwrap();
        assert_eq!(
            r.begin_step("charge").unwrap().1,
            StepStart::Run {
                attempt: 2,
                not_before_ms: 900
            }
        );
        assert_eq!(
            r.step_retry(s, 950, p("e4")),
            Some(Record::Attempt {
                run: 1,
                seq: s,
                name: Arc::from("charge"),
                attempt: 4,
                retry_at_ms: 950,
                error: p("e4"),
            })
        );
    }

    #[test]
    fn a_resume_before_the_code_reaches_the_step() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (s, _) = m.begin_step("charge").unwrap();
        journal.push(m.step_retry(s, PAUSED, p("e1")).unwrap());
        // Loaded from the journal; the code has not run yet.
        let mut r = RunMachine::new(&journal, 2).unwrap();
        assert!(r.is_paused());
        let resumed = r.resume(None);
        assert_eq!(resumed.len(), 1);
        assert!(!r.is_paused());
        assert_eq!(
            r.begin_step("charge").unwrap().1,
            StepStart::Run {
                attempt: 1,
                not_before_ms: 0
            }
        );
        assert!(!r.step_paused(s));
    }

    #[test]
    fn cancellation_replays_at_the_same_point() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (a, _) = m.begin_step("a").unwrap();
        let (w, arm) = m.begin_wait("approve", 0).unwrap();
        if let Arm::Append(rec) = arm {
            journal.push(rec);
        }
        m.request_cancel(p("stop"));
        let got = drain(&mut m, &mut journal);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|g| g.1.status == Status::Cancelled));
        // Compensation after cancellation runs normally.
        let (c, _) = m.begin_step("undo").unwrap();
        m.complete(
            c,
            Completion::Step {
                status: Status::Ok,
                payload: p("u"),
            },
        );
        drain(&mut m, &mut journal);
        journal.push(m.finish(Status::Cancelled, p("stop"), 99).unwrap());

        let mut r = RunMachine::new(&journal, 9).unwrap();
        assert_eq!(
            r.begin_step("a").unwrap(),
            (
                a,
                StepStart::Run {
                    attempt: 1,
                    not_before_ms: 0
                }
            )
        );
        assert_eq!(r.begin_wait("approve", 0).unwrap(), (w, Arm::Armed));
        let mut none = Vec::new();
        let got = drain(&mut r, &mut none);
        assert_eq!(got.len(), 2);
        assert!(none.is_empty());
        assert_eq!(r.begin_step("undo").unwrap().1, StepStart::Replay);
        assert_eq!(drain(&mut r, &mut none)[0].1, Outcome::ok(p("u")));
        assert!(r.recorded_done().is_some());
    }

    #[test]
    fn repeated_terminal_cancel_replays_without_hiding_other_deliveries() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        m.begin_step("charge").unwrap();
        m.request_cancel(p("stop"));
        assert_eq!(drain(&mut m, &mut journal).len(), 1);
        m.request_cancel(p("retry"));
        assert!(drain(&mut m, &mut journal).is_empty());
        assert_eq!(journal.len(), 2);
        journal.push(Record::Cancel {
            run: 1,
            reason: p("retry"),
            by: None,
        });
        let mut replay = RunMachine::new(&journal, 3).unwrap();
        replay.begin_step("charge").unwrap();
        assert!(matches!(replay.next(), Next::CancelAll { .. }));
        replay.check_end().unwrap();
        // Compensation started after that cancellation. Its historical
        // cancellation must still be delivered before the workflow returns.
        replay.begin_step("undo").unwrap();
        assert!(replay.check_end().is_err());
        assert!(matches!(replay.next(), Next::CancelAll { .. }));
        replay.check_end().unwrap();
    }

    #[test]
    fn values_and_waits_replay() {
        let mut journal = vec![start()];
        journal.push(Record::Signal {
            run: 1,
            name: Arc::from("go"),
            dedup: 0,
            payload: p("yes"),
            by: None,
        });
        let mut m = RunMachine::new(&journal, 2).unwrap();
        let (_, now, rec) = m.value(value_kind::NOW, || p("12345678")).unwrap();
        journal.push(rec.unwrap());
        assert_eq!(&*now, b"12345678");
        let (w, arm) = m.begin_wait("go", 0).unwrap();
        let Arm::Append(rec) = arm else { panic!() };
        journal.push(rec);
        m.complete(
            w,
            Completion::Signal {
                ordinal: 0,
                payload: p("yes"),
            },
        );
        drain(&mut m, &mut journal);

        let mut r = RunMachine::new(&journal, 5).unwrap();
        let (_, v, rec) = r.value(value_kind::NOW, || p("other")).unwrap();
        assert_eq!((&*v, rec), (&b"12345678"[..], None));
        assert_eq!(r.begin_wait("go", 0).unwrap().1, Arm::Replay);
        assert_eq!(
            r.next(),
            Next::Deliver {
                seq: w,
                outcome: Outcome::ok(p("yes")),
                record: None
            }
        );
    }

    #[test]
    fn stall_is_reported_only_when_nothing_live_is_pending() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let _ = m.begin_step("a").unwrap();
        let (b, _) = m.begin_step("b").unwrap();
        m.complete(
            b,
            Completion::Step {
                status: Status::Ok,
                payload: p("B"),
            },
        );
        drain(&mut m, &mut journal);
        // On replay the code starts "a" (live: it never finished) but never
        // starts "b", whose result the journal delivers next.
        let mut r = RunMachine::new(&journal, 2).unwrap();
        let (a, _) = r.begin_step("a").unwrap();
        assert_eq!(r.next(), Next::Stalled);
        assert!(r.stall_error().is_none(), "step a is live work");
        r.forget(a);
        let err = r.stall_error().unwrap();
        assert!(err.message.contains("step 'b'"), "{err}");
    }

    #[test]
    fn updates_are_deliveries_in_journal_order() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (a, _) = m.begin_step("a").unwrap();
        m.offer_update(77, Arc::from("add"), Arc::from("u1"), p("5"));
        let Next::Update {
            ordinal,
            live: Some(77),
            name,
            id,
            payload,
        } = m.next()
        else {
            panic!("expected a live update");
        };
        assert_eq!(ordinal, 0);
        let (ord, rec) = m.accept_update(name, id, payload);
        journal.push(rec);
        // The handler runs a step and returns.
        let (h, _) = m.begin_step("handler-step").unwrap();
        m.complete(
            h,
            Completion::Step {
                status: Status::Ok,
                payload: p("H"),
            },
        );
        drain(&mut m, &mut journal);
        let (_, done) = m.update_done(ord, Status::Ok, p("6")).unwrap();
        journal.push(done.unwrap());
        m.complete(
            a,
            Completion::Step {
                status: Status::Ok,
                payload: p("A"),
            },
        );
        drain(&mut m, &mut journal);

        // Replay: the update is delivered at its place, the handler's step
        // and result are recorded, nothing is appended.
        let mut r = RunMachine::new(&journal, 9).unwrap();
        assert_eq!(r.begin_step("a").unwrap().1, StepStart::Replay);
        let Next::Update {
            ordinal: 0,
            live: None,
            ..
        } = r.next()
        else {
            panic!("expected the recorded update");
        };
        assert_eq!(r.begin_step("handler-step").unwrap().1, StepStart::Replay);
        assert!(matches!(r.next(), Next::Deliver { seq, .. } if seq == h));
        assert_eq!(r.update_done(0, Status::Ok, p("6")).unwrap().1, None);
        assert!(matches!(r.next(), Next::Deliver { seq, .. } if seq == a));
        assert!(!r.is_replaying());
    }

    #[test]
    fn async_completion_checks_the_attempt() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (s, st) = m.begin_await("approval").unwrap();
        assert_eq!(
            st,
            AwaitStart::Run {
                attempt: 1,
                not_before_ms: 0
            }
        );
        // The token function failed once and is retried.
        journal.push(m.step_retry(s, 5, p("smtp down")).unwrap());
        journal.push(m.awaiting(s).unwrap());
        assert!(m.is_suspended());
        // A token from the first attempt is stale.
        assert!(!m.complete(
            s,
            Completion::Completed {
                attempt: 1,
                status: Status::Ok,
                payload: p("x")
            }
        ));
        assert!(m.complete(
            s,
            Completion::Completed {
                attempt: 2,
                status: Status::Ok,
                payload: p("yes")
            }
        ));
        let got = drain(&mut m, &mut journal);
        assert_eq!(got, vec![(s, Outcome::ok(p("yes")))]);
        // Replay after the token went out waits instead of running again.
        let mut r = RunMachine::new(&journal[..3], 3).unwrap();
        assert_eq!(
            r.begin_await("approval").unwrap().1,
            AwaitStart::Waiting { attempt: 2 }
        );
        let mut r = RunMachine::new(&journal, 4).unwrap();
        assert_eq!(r.begin_await("approval").unwrap().1, AwaitStart::Replay);
    }

    #[test]
    fn patches_take_no_position() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (a, _) = m.begin_step("a").unwrap();
        m.complete(
            a,
            Completion::Step {
                status: Status::Ok,
                payload: p("A"),
            },
        );
        drain(&mut m, &mut journal);
        let (b, _) = m.begin_step("b").unwrap();
        m.complete(
            b,
            Completion::Step {
                status: Status::Ok,
                payload: p("B"),
            },
        );
        drain(&mut m, &mut journal);
        // Old code ran a then b. New code asks for a patch between them:
        // replaying, it must take the old branch and keep b at seq 1.
        let mut r = RunMachine::new(&journal, 3).unwrap();
        r.begin_step("a").unwrap();
        assert_eq!(r.patched("fix"), (false, None));
        assert_eq!(r.begin_step("b").unwrap(), (1, StepStart::Replay));
        // Past the journal the patch applies, and is remembered.
        let (on, rec) = r.patched("later");
        assert!(on);
        let mut j2 = journal.clone();
        j2.push(rec.unwrap());
        let mut r2 = RunMachine::new(&j2, 4).unwrap();
        r2.begin_step("a").unwrap();
        r2.begin_step("b").unwrap();
        assert_eq!(r2.patched("later"), (true, None));
    }

    #[test]
    fn heartbeat_progress_survives_into_the_retry() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let (s, _) = m.begin_step("export").unwrap();
        journal.push(m.heartbeat(s, p("page-7")).unwrap());
        let r = {
            let mut r = RunMachine::new(&journal, 2).unwrap();
            r.begin_step("export").unwrap();
            r
        };
        assert_eq!(r.progress(s), Some(p("page-7")));
    }

    #[test]
    fn a_queued_completion_is_taken_once_and_in_order() {
        let mut journal = vec![start()];
        let mut m = RunMachine::fresh(&journal[0], 1).unwrap();
        let n: Seq = 20_000;
        let seqs: Vec<Seq> = (0..n).map(|_| m.begin_step("s").unwrap().0).collect();
        let done = |i: Seq| Completion::Step {
            status: Status::Ok,
            payload: p(&i.to_string()),
        };
        // Completed back to front, each reported twice: only the first counts.
        for &s in seqs.iter().rev() {
            assert!(m.complete(s, done(s)));
            assert!(!m.complete(s, done(s + 1)));
        }
        let got = drain(&mut m, &mut journal);
        assert_eq!(
            got.iter().map(|g| g.0).collect::<Vec<_>>(),
            seqs.iter().rev().copied().collect::<Vec<_>>()
        );
        assert!(got.iter().all(|(s, o)| o.payload == p(&s.to_string())));
        // Delivered: the operation is no longer pending, so a late report is ignored.
        assert!(!m.complete(seqs[0], done(0)));
        assert!(matches!(m.next(), Next::Idle));
    }
}
