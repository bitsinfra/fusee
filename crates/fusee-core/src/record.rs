//! Journal records.
//!
//! A record is one fact about one workflow run. The same `Record` type is used
//! borrowed ([`RecordRef`], for encoding on the hot path) and owned
//! ([`RecordBuf`], after decoding). Records have two encodings:
//!
//! - **Segment**: inside a log segment. Workflow type and step names are
//!   interned per segment: the first record that uses a name defines it in a
//!   prefix, later records refer to it by a small integer.
//! - **Canonical**: names written inline. Used for journals held in memory,
//!   spilled run blobs, checkpoints, replication and the server protocol, so
//!   a record means the same thing wherever it travels.

use crate::wire::{DecodeError, DecodeResult, Reader, put_bytes, put_varint};
use rustc_hash::FxHashMap;
use std::sync::Arc;

pub type RunId = u64;
pub type Seq = u32;
pub type Lsn = u64;
pub type Name = Arc<str>;
pub type Payload = Arc<[u8]>;

/// How a step, child or workflow ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    Failed = 1,
    Cancelled = 2,
    TimedOut = 3,
    Terminated = 4,
    /// The run ended by starting a new run of the same workflow ID.
    ContinuedAsNew = 5,
    /// The run was replaced by a reset.
    Reset = 6,
}

impl Status {
    pub fn from_u8(v: u8) -> DecodeResult<Self> {
        Ok(match v {
            0 => Self::Ok,
            1 => Self::Failed,
            2 => Self::Cancelled,
            3 => Self::TimedOut,
            4 => Self::Terminated,
            5 => Self::ContinuedAsNew,
            6 => Self::Reset,
            _ => return Err(DecodeError("unknown status")),
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Terminated => "terminated",
            Self::ContinuedAsNew => "continued_as_new",
            Self::Reset => "reset",
        }
    }

    /// The run handed over to a newer run of the same workflow ID.
    pub fn is_handover(self) -> bool {
        matches!(self, Self::ContinuedAsNew | Self::Reset)
    }
}

/// The parent of a child workflow: the shard and run that started it and the
/// journal position that waits for its result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ParentRef {
    pub shard: u32,
    pub run: RunId,
    pub seq: Seq,
}

/// Where a run came from, when it is not a fresh start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Origin {
    pub run: RunId,
    pub cause: u8,
}

pub mod origin {
    /// `ctx.continueAsNew`: the `Start` record also ends the previous run.
    pub const CONTINUED: u8 = 1;
    /// `run.reset`: the previous run is ended separately; records copied from
    /// its journal follow the `Start`.
    pub const RESET: u8 = 2;
}

/// Recorded nondeterministic values (`ctx.now()`, `ctx.random()`, ...).
pub mod value_kind {
    pub const NOW: u8 = 0;
    pub const RANDOM: u8 = 1;
    pub const UUID: u8 = 2;
    pub const CUSTOM: u8 = 3;
}

/// The policy byte of a `Start` record (and the `flags` of a `Child` record,
/// which become the child's start policy). Recorded so replay and recovery
/// apply the same rules.
pub mod policy {
    /// Bits 0–1: starting an ID that is already running.
    pub const RUNNING_MASK: u8 = 0b11;
    pub const RUNNING_FAIL: u8 = 0;
    pub const RUNNING_USE_EXISTING: u8 = 1;
    pub const RUNNING_TERMINATE: u8 = 2;
    /// Bits 2–3: starting an ID whose last run finished within retention.
    pub const FINISHED_MASK: u8 = 0b11 << 2;
    pub const FINISHED_ALLOW: u8 = 0;
    pub const FINISHED_IF_FAILED: u8 = 1 << 2;
    pub const FINISHED_REFUSE: u8 = 2 << 2;
    pub const FINISHED_USE_EXISTING: u8 = 3 << 2;
    /// Bits 4–5: what happens to a child when its parent closes.
    pub const CLOSE_MASK: u8 = 0b11 << 4;
    pub const CLOSE_TERMINATE: u8 = 0;
    pub const CLOSE_CANCEL: u8 = 1 << 4;
    pub const CLOSE_ABANDON: u8 = 2 << 4;
    /// Bit 6: the parent does not wait for the child's result.
    pub const DETACHED: u8 = 1 << 6;
    /// Bit 7: on a `Start`, the run is a keyed object (its workflow ID is
    /// `<type>/<key>`, its handlers are updates and its input is a state
    /// snapshot); on a `Child`, the operation is a call to an object's
    /// handler rather than a child start (its input is the encoded call).
    pub const OBJECT: u8 = 1 << 7;
    /// The policy every start between workflows uses: attach to a run that
    /// exists, so a resent start never creates a second run.
    pub const ATTACH: u8 = RUNNING_USE_EXISTING | FINISHED_USE_EXISTING;
}

/// The outcome of a `Waited` record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The wait consumed the run's signal with this ordinal.
    Signal(u64),
    TimedOut,
    /// The code dropped the wait (it lost a race): a signal is no longer
    /// offered to it. Not a delivery.
    Abandoned,
}

/// When a step ran: recorded under the dev profile only (production steps
/// read no clock for it and write no byte), for the UI's timeline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StepTime {
    /// When the code asked for the step, Unix ms.
    pub began_ms: u64,
    /// From then until its result, ms (retries and their waits included).
    pub took_ms: u64,
}

/// The rare fields of a `Start` record, boxed so every record stays small.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartMeta<B> {
    /// Who asked (audit; see [`Record`]), for a reset's start.
    pub by: Option<B>,
    /// The W3C `traceparent` of the request that started the run, when the
    /// starter traces: the run's spans join that trace.
    pub trace: Option<B>,
}

impl<B> StartMeta<B> {
    /// `None` when both are absent, so a plain start carries nothing.
    pub fn of(by: Option<B>, trace: Option<B>) -> Option<Box<Self>> {
        (by.is_some() || trace.is_some()).then(|| Box::new(Self { by, trace }))
    }
}

/// An update carried by a `Start` record (update-with-start).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartUpdate<S, B> {
    pub name: S,
    pub id: S,
    pub payload: B,
}

/// A journal record.
///
/// Audit: the `by` of a `Signal`, `Cancel`, `Done` (terminate, reset) or a
/// reset's `Start` names who asked, as the API that took the request saw
/// them (a token's scope, a proxy's identity header, a CLI user). It is
/// written after the record's other fields only when present, so records
/// without it carry no extra bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record<S, B> {
    /// A run was created. Its acknowledgment is released once durable.
    Start {
        run: RunId,
        id: S,
        wtype: S,
        version: u32,
        codec: u8,
        started_ms: u64,
        /// Absolute workflow deadline in Unix milliseconds; 0 means none.
        deadline_ms: u64,
        parent: Option<ParentRef>,
        /// See [`policy`].
        policy: u8,
        /// The run this one continues or replaces.
        from: Option<Origin>,
        /// How many of the run's following records are copies from
        /// `from`'s journal (reset). Their messages are not sent again.
        copies: u32,
        /// An update delivered in the same record (update-with-start).
        /// Boxed: rare, and it keeps every record small.
        update: Option<Box<StartUpdate<S, B>>>,
        input: B,
        /// Who asked (a reset's start) and the trace context: rare, so
        /// boxed.
        meta: Option<Box<StartMeta<B>>>,
    },
    /// A step finished (or was cancelled). Delivered to code in journal order.
    Step {
        run: RunId,
        seq: Seq,
        name: S,
        attempt: u32,
        status: Status,
        payload: B,
        /// When it ran, under the dev profile (a `STEP_TIMED` record).
        time: Option<StepTime>,
    },
    /// A step attempt failed and will be retried at `retry_at_ms`.
    Attempt {
        run: RunId,
        seq: Seq,
        name: S,
        attempt: u32,
        retry_at_ms: u64,
        error: B,
    },
    /// A durable timer was set (`ctx.sleep`).
    Timer {
        run: RunId,
        seq: Seq,
        name: S,
        fire_at_ms: u64,
    },
    /// A durable timer fired and its sleep returned. Delivered.
    Fired { run: RunId, seq: Seq },
    /// A signal arrived. Its acknowledgment is released once durable.
    /// `dedup` (0 = none) suppresses repeated deliveries of the same message.
    Signal {
        run: RunId,
        name: S,
        dedup: u64,
        payload: B,
        /// Who sent it (audit; see [`Record`]).
        by: Option<B>,
    },
    /// The run began waiting for a signal; `deadline_ms` 0 means no timeout.
    Wait {
        run: RunId,
        seq: Seq,
        name: S,
        deadline_ms: u64,
    },
    /// A wait finished. Delivered.
    Waited {
        run: RunId,
        seq: Seq,
        outcome: WaitOutcome,
    },
    /// A recorded nondeterministic value.
    Value {
        run: RunId,
        seq: Seq,
        kind: u8,
        value: B,
    },
    /// The run started a child workflow. Sent once durable. `flags` is the
    /// child's start policy ([`policy`]).
    Child {
        run: RunId,
        seq: Seq,
        wtype: S,
        id: S,
        flags: u8,
        deadline_ms: u64,
        input: B,
    },
    /// A child workflow finished. Delivered.
    ChildDone {
        run: RunId,
        seq: Seq,
        status: Status,
        payload: B,
    },
    /// The run sent a signal to another workflow. Sent once durable.
    Send {
        run: RunId,
        seq: Seq,
        target: S,
        name: S,
        payload: B,
    },
    /// Cancellation was delivered: every operation pending at this point in
    /// the journal ends with `Status::Cancelled`. Delivered.
    Cancel {
        run: RunId,
        reason: B,
        /// Who asked (audit; see [`Record`]).
        by: Option<B>,
    },
    /// The run finished. Its result is released once durable.
    Done {
        run: RunId,
        status: Status,
        finished_ms: u64,
        payload: B,
        /// Who asked, when a request ended the run (terminate, reset);
        /// see [`Record`].
        by: Option<B>,
    },
    /// An update passed its validator and was accepted. Delivered: the
    /// handler starts at this point in the journal. Its ordinal is the
    /// number of updates the run accepted before it.
    Update {
        run: RunId,
        name: S,
        id: S,
        payload: B,
        /// The workflow operation waiting for the result (a call to an
        /// object from workflow code): the result is sent to it as a child
        /// result once the `UpdateDone` is durable.
        caller: Option<ParentRef>,
    },
    /// An update's handler returned. Its result is released once durable.
    UpdateDone {
        run: RunId,
        seq: Seq,
        ordinal: u64,
        status: Status,
        payload: B,
        /// An object's state changes the handler commits with its result
        /// (an encoded JSON object, sealed by the codec; empty for none).
        state: B,
    },
    /// A running step reported progress (only the latest is kept).
    Heartbeat {
        run: RunId,
        seq: Seq,
        attempt: u32,
        payload: B,
    },
    /// An asynchronously completed step handed out its token for `attempt`
    /// and now waits for `engine.complete` or `engine.fail`.
    Awaiting {
        run: RunId,
        seq: Seq,
        name: S,
        attempt: u32,
    },
    /// An asynchronously completed step got its result. Delivered.
    Completed {
        run: RunId,
        seq: Seq,
        attempt: u32,
        status: Status,
        payload: B,
    },
    /// `ctx.patched(id)` took the new branch at journal position `at`. Takes
    /// no position itself, so adding or removing patches never shifts the
    /// journal.
    Patch { run: RunId, at: Seq, id: S },
    /// Search attributes set by the code (a JSON object, never encrypted).
    Attributes { run: RunId, seq: Seq, payload: B },
    /// A chunk appended to one of the run's streams (`ctx.stream`). `seq`
    /// and `attempt` name the step attempt that wrote it, or are
    /// [`stream::WORKFLOW`] and 0 for workflow code; `offset` is its place
    /// in the stream (0, 1, 2, ...). Takes no journal position.
    Chunk {
        run: RunId,
        seq: Seq,
        attempt: u32,
        stream: S,
        offset: u64,
        payload: B,
    },
    /// A stream was completed after `chunks` chunks: closed by workflow
    /// code (`seq` [`stream::WORKFLOW`]) or by a step, or by the final
    /// result of the step that wrote it (`status` is that result's).
    StreamEnd {
        run: RunId,
        seq: Seq,
        stream: S,
        chunks: u64,
        status: Status,
    },
    /// A paused step (an `Attempt` whose `retry_at_ms` is [`PAUSED`]) was
    /// resumed: its next attempt runs at once with a fresh retry budget.
    /// `attempt` is the step's last attempt before it.
    Resume {
        run: RunId,
        seq: Seq,
        attempt: u32,
        /// Who asked (audit; see [`Record`]).
        by: Option<B>,
    },
}

/// The `retry_at_ms` of an `Attempt` whose step is paused: retried only
/// once a [`Record::Resume`] follows.
pub const PAUSED: u64 = u64::MAX;

/// Streams (`ctx.stream`).
pub mod stream {
    use super::Seq;
    /// The `seq` of chunks and ends written by workflow code rather than by
    /// a step.
    pub const WORKFLOW: Seq = Seq::MAX;
}

pub type RecordRef<'a> = Record<&'a str, &'a [u8]>;
pub type RecordBuf = Record<Name, Payload>;

/// A record's payload as a [`Payload`]: shared when it is one, copied
/// when borrowed.
pub trait ToPayload: AsRef<[u8]> {
    fn to_payload(&self) -> Payload;
}

impl ToPayload for Payload {
    #[inline]
    fn to_payload(&self) -> Payload {
        self.clone()
    }
}

impl ToPayload for &[u8] {
    #[inline]
    fn to_payload(&self) -> Payload {
        Arc::from(*self)
    }
}

impl ToPayload for Vec<u8> {
    #[inline]
    fn to_payload(&self) -> Payload {
        Arc::from(&self[..])
    }
}

/// A record's string field as a [`Name`]: shared when it is one already,
/// allocated when borrowed.
pub trait ToName: AsRef<str> {
    fn to_name(&self) -> Name;
}

impl ToName for Name {
    #[inline]
    fn to_name(&self) -> Name {
        self.clone()
    }
}

impl ToName for &str {
    #[inline]
    fn to_name(&self) -> Name {
        Arc::from(*self)
    }
}

pub mod kind {
    pub const START: u8 = 1;
    pub const STEP: u8 = 2;
    pub const ATTEMPT: u8 = 3;
    pub const TIMER: u8 = 4;
    pub const FIRED: u8 = 5;
    pub const SIGNAL: u8 = 6;
    pub const WAIT: u8 = 7;
    pub const WAITED: u8 = 8;
    pub const VALUE: u8 = 9;
    pub const CHILD: u8 = 10;
    pub const CHILD_DONE: u8 = 11;
    pub const SEND: u8 = 12;
    pub const CANCEL: u8 = 13;
    pub const DONE: u8 = 14;
    pub const UPDATE: u8 = 15;
    pub const UPDATE_DONE: u8 = 16;
    pub const HEARTBEAT: u8 = 17;
    pub const AWAITING: u8 = 18;
    pub const COMPLETED: u8 = 19;
    pub const PATCH: u8 = 20;
    pub const ATTRIBUTES: u8 = 21;
    pub const CHUNK: u8 = 22;
    pub const STREAM_END: u8 = 23;
    /// An `Update` that names its caller (a call to an object from
    /// workflow code). Its own kind, so a truncated record never decodes.
    pub const UPDATE_CALL: u8 = 24;
    /// An `UpdateDone` that carries an object's state changes.
    pub const UPDATE_DONE_STATE: u8 = 25;
    /// A `Step` with its times (the dev profile). Its own kind, so a plain
    /// step's bytes carry no times and a truncated record never decodes.
    pub const STEP_TIMED: u8 = 26;
    /// A paused step was resumed.
    pub const RESUME: u8 = 27;
    /// Set on the kind byte when interned name definitions follow it.
    pub const DEFINES: u8 = 0x80;
}

impl<S, B> Record<S, B> {
    /// The same record with its names and payloads converted.
    pub fn map<'a, S2, B2>(
        &'a self,
        n: impl Fn(&'a S) -> S2,
        p: impl Fn(&'a B) -> B2,
    ) -> Record<S2, B2> {
        match self {
            Self::Start {
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
                input,
                meta,
            } => Record::Start {
                run: *run,
                id: n(id),
                wtype: n(wtype),
                version: *version,
                codec: *codec,
                started_ms: *started_ms,
                deadline_ms: *deadline_ms,
                parent: *parent,
                policy: *policy,
                from: *from,
                copies: *copies,
                update: update.as_ref().map(|u| {
                    Box::new(StartUpdate {
                        name: n(&u.name),
                        id: n(&u.id),
                        payload: p(&u.payload),
                    })
                }),
                input: p(input),
                meta: meta.as_ref().map(|m| {
                    Box::new(StartMeta {
                        by: m.by.as_ref().map(&p),
                        trace: m.trace.as_ref().map(&p),
                    })
                }),
            },
            Self::Step {
                run,
                seq,
                name,
                attempt,
                status,
                payload,
                time,
            } => Record::Step {
                run: *run,
                seq: *seq,
                name: n(name),
                attempt: *attempt,
                status: *status,
                payload: p(payload),
                time: *time,
            },
            Self::Attempt {
                run,
                seq,
                name,
                attempt,
                retry_at_ms,
                error,
            } => Record::Attempt {
                run: *run,
                seq: *seq,
                name: n(name),
                attempt: *attempt,
                retry_at_ms: *retry_at_ms,
                error: p(error),
            },
            Self::Timer {
                run,
                seq,
                name,
                fire_at_ms,
            } => Record::Timer {
                run: *run,
                seq: *seq,
                name: n(name),
                fire_at_ms: *fire_at_ms,
            },
            Self::Fired { run, seq } => Record::Fired {
                run: *run,
                seq: *seq,
            },
            Self::Signal {
                run,
                name,
                dedup,
                payload,
                by,
            } => Record::Signal {
                run: *run,
                name: n(name),
                dedup: *dedup,
                payload: p(payload),
                by: by.as_ref().map(&p),
            },
            Self::Wait {
                run,
                seq,
                name,
                deadline_ms,
            } => Record::Wait {
                run: *run,
                seq: *seq,
                name: n(name),
                deadline_ms: *deadline_ms,
            },
            Self::Waited { run, seq, outcome } => Record::Waited {
                run: *run,
                seq: *seq,
                outcome: *outcome,
            },
            Self::Value {
                run,
                seq,
                kind,
                value,
            } => Record::Value {
                run: *run,
                seq: *seq,
                kind: *kind,
                value: p(value),
            },
            Self::Child {
                run,
                seq,
                wtype,
                id,
                flags,
                deadline_ms,
                input,
            } => Record::Child {
                run: *run,
                seq: *seq,
                wtype: n(wtype),
                id: n(id),
                flags: *flags,
                deadline_ms: *deadline_ms,
                input: p(input),
            },
            Self::ChildDone {
                run,
                seq,
                status,
                payload,
            } => Record::ChildDone {
                run: *run,
                seq: *seq,
                status: *status,
                payload: p(payload),
            },
            Self::Send {
                run,
                seq,
                target,
                name,
                payload,
            } => Record::Send {
                run: *run,
                seq: *seq,
                target: n(target),
                name: n(name),
                payload: p(payload),
            },
            Self::Cancel { run, reason, by } => Record::Cancel {
                run: *run,
                reason: p(reason),
                by: by.as_ref().map(&p),
            },
            Self::Done {
                run,
                status,
                finished_ms,
                payload,
                by,
            } => Record::Done {
                run: *run,
                status: *status,
                finished_ms: *finished_ms,
                payload: p(payload),
                by: by.as_ref().map(&p),
            },
            Self::Update {
                run,
                name,
                id,
                payload,
                caller,
            } => Record::Update {
                run: *run,
                name: n(name),
                id: n(id),
                payload: p(payload),
                caller: *caller,
            },
            Self::UpdateDone {
                run,
                seq,
                ordinal,
                status,
                payload,
                state,
            } => Record::UpdateDone {
                run: *run,
                seq: *seq,
                ordinal: *ordinal,
                status: *status,
                payload: p(payload),
                state: p(state),
            },
            Self::Heartbeat {
                run,
                seq,
                attempt,
                payload,
            } => Record::Heartbeat {
                run: *run,
                seq: *seq,
                attempt: *attempt,
                payload: p(payload),
            },
            Self::Awaiting {
                run,
                seq,
                name,
                attempt,
            } => Record::Awaiting {
                run: *run,
                seq: *seq,
                name: n(name),
                attempt: *attempt,
            },
            Self::Completed {
                run,
                seq,
                attempt,
                status,
                payload,
            } => Record::Completed {
                run: *run,
                seq: *seq,
                attempt: *attempt,
                status: *status,
                payload: p(payload),
            },
            Self::Patch { run, at, id } => Record::Patch {
                run: *run,
                at: *at,
                id: n(id),
            },
            Self::Attributes { run, seq, payload } => Record::Attributes {
                run: *run,
                seq: *seq,
                payload: p(payload),
            },
            Self::Chunk {
                run,
                seq,
                attempt,
                stream,
                offset,
                payload,
            } => Record::Chunk {
                run: *run,
                seq: *seq,
                attempt: *attempt,
                stream: n(stream),
                offset: *offset,
                payload: p(payload),
            },
            Self::StreamEnd {
                run,
                seq,
                stream,
                chunks,
                status,
            } => Record::StreamEnd {
                run: *run,
                seq: *seq,
                stream: n(stream),
                chunks: *chunks,
                status: *status,
            },
            Self::Resume {
                run,
                seq,
                attempt,
                by,
            } => Record::Resume {
                run: *run,
                seq: *seq,
                attempt: *attempt,
                by: by.as_ref().map(&p),
            },
        }
    }

    pub fn run(&self) -> RunId {
        match self {
            Self::Start { run, .. }
            | Self::Step { run, .. }
            | Self::Attempt { run, .. }
            | Self::Timer { run, .. }
            | Self::Fired { run, .. }
            | Self::Signal { run, .. }
            | Self::Wait { run, .. }
            | Self::Waited { run, .. }
            | Self::Value { run, .. }
            | Self::Child { run, .. }
            | Self::ChildDone { run, .. }
            | Self::Send { run, .. }
            | Self::Cancel { run, .. }
            | Self::Done { run, .. }
            | Self::Update { run, .. }
            | Self::UpdateDone { run, .. }
            | Self::Heartbeat { run, .. }
            | Self::Awaiting { run, .. }
            | Self::Completed { run, .. }
            | Self::Patch { run, .. }
            | Self::Attributes { run, .. }
            | Self::Chunk { run, .. }
            | Self::StreamEnd { run, .. }
            | Self::Resume { run, .. } => *run,
        }
    }

    /// The same record for another run (reset copies a journal).
    pub fn with_run(&self, new: RunId) -> Self
    where
        S: Clone,
        B: Clone,
    {
        let mut r = self.map(S::clone, B::clone);
        match &mut r {
            Self::Start { run, .. }
            | Self::Step { run, .. }
            | Self::Attempt { run, .. }
            | Self::Timer { run, .. }
            | Self::Fired { run, .. }
            | Self::Signal { run, .. }
            | Self::Wait { run, .. }
            | Self::Waited { run, .. }
            | Self::Value { run, .. }
            | Self::Child { run, .. }
            | Self::ChildDone { run, .. }
            | Self::Send { run, .. }
            | Self::Cancel { run, .. }
            | Self::Done { run, .. }
            | Self::Update { run, .. }
            | Self::UpdateDone { run, .. }
            | Self::Heartbeat { run, .. }
            | Self::Awaiting { run, .. }
            | Self::Completed { run, .. }
            | Self::Patch { run, .. }
            | Self::Attributes { run, .. }
            | Self::Chunk { run, .. }
            | Self::StreamEnd { run, .. }
            | Self::Resume { run, .. } => *run = new,
        }
        r
    }

    pub fn seq(&self) -> Option<Seq> {
        match self {
            Self::Step { seq, .. }
            | Self::Attempt { seq, .. }
            | Self::Timer { seq, .. }
            | Self::Fired { seq, .. }
            | Self::Wait { seq, .. }
            | Self::Waited { seq, .. }
            | Self::Value { seq, .. }
            | Self::Child { seq, .. }
            | Self::ChildDone { seq, .. }
            | Self::Send { seq, .. }
            | Self::UpdateDone { seq, .. }
            | Self::Heartbeat { seq, .. }
            | Self::Awaiting { seq, .. }
            | Self::Completed { seq, .. }
            | Self::Attributes { seq, .. }
            | Self::Chunk { seq, .. }
            | Self::StreamEnd { seq, .. }
            | Self::Resume { seq, .. } => Some(*seq),
            _ => None,
        }
    }

    pub fn kind(&self) -> u8 {
        match self {
            Self::Start { .. } => kind::START,
            Self::Step { .. } => kind::STEP,
            Self::Attempt { .. } => kind::ATTEMPT,
            Self::Timer { .. } => kind::TIMER,
            Self::Fired { .. } => kind::FIRED,
            Self::Signal { .. } => kind::SIGNAL,
            Self::Wait { .. } => kind::WAIT,
            Self::Waited { .. } => kind::WAITED,
            Self::Value { .. } => kind::VALUE,
            Self::Child { .. } => kind::CHILD,
            Self::ChildDone { .. } => kind::CHILD_DONE,
            Self::Send { .. } => kind::SEND,
            Self::Cancel { .. } => kind::CANCEL,
            Self::Done { .. } => kind::DONE,
            Self::Update {
                caller: Some(_), ..
            } => kind::UPDATE_CALL,
            Self::Update { .. } => kind::UPDATE,
            Self::UpdateDone { .. } => kind::UPDATE_DONE,
            Self::Heartbeat { .. } => kind::HEARTBEAT,
            Self::Awaiting { .. } => kind::AWAITING,
            Self::Completed { .. } => kind::COMPLETED,
            Self::Patch { .. } => kind::PATCH,
            Self::Attributes { .. } => kind::ATTRIBUTES,
            Self::Chunk { .. } => kind::CHUNK,
            Self::StreamEnd { .. } => kind::STREAM_END,
            Self::Resume { .. } => kind::RESUME,
        }
    }
}

impl<S: AsRef<str>, B: AsRef<[u8]>> Record<S, B> {
    /// Calls `f` for each interned name the record carries, in encoding order.
    pub fn for_each_name(&self, mut f: impl FnMut(&str)) {
        match self {
            Self::Start { wtype, update, .. } => {
                f(wtype.as_ref());
                if let Some(u) = update {
                    f(u.name.as_ref());
                }
            }
            Self::Step { name, .. }
            | Self::Attempt { name, .. }
            | Self::Timer { name, .. }
            | Self::Signal { name, .. }
            | Self::Wait { name, .. }
            | Self::Update { name, .. }
            | Self::Awaiting { name, .. } => f(name.as_ref()),
            Self::Child { wtype, .. } => f(wtype.as_ref()),
            Self::Send { name, .. } => f(name.as_ref()),
            Self::Chunk { stream, .. } | Self::StreamEnd { stream, .. } => f(stream.as_ref()),
            _ => {}
        }
    }

    pub fn as_ref(&self) -> RecordRef<'_> {
        self.map(|s| s.as_ref(), |b| b.as_ref())
    }

    pub fn to_buf(&self) -> RecordBuf {
        self.map(|s| Arc::from(s.as_ref()), |b| Arc::from(b.as_ref()))
    }
}

/// How names are written into a record body.
pub trait NameEncoder {
    fn put(&mut self, out: &mut Vec<u8>, name: &str);
}

/// How names are read from a record body.
pub trait NameDecoder {
    /// Called when the kind byte carries [`kind::DEFINES`].
    fn define(&mut self, r: &mut Reader<'_>) -> DecodeResult<()>;
    fn get(&mut self, r: &mut Reader<'_>) -> DecodeResult<Name>;
}

/// Canonical encoding: names inline.
pub struct Inline;

impl NameEncoder for Inline {
    #[inline]
    fn put(&mut self, out: &mut Vec<u8>, name: &str) {
        put_bytes(out, name.as_bytes());
    }
}

/// Canonical decoding. Repeated names share one allocation.
#[derive(Default)]
pub struct InlineDecoder {
    cache: FxHashMap<Box<str>, Name>,
}

impl NameDecoder for InlineDecoder {
    fn define(&mut self, _r: &mut Reader<'_>) -> DecodeResult<()> {
        Err(DecodeError("name definitions in canonical record"))
    }

    fn get(&mut self, r: &mut Reader<'_>) -> DecodeResult<Name> {
        let s = r.str()?;
        if let Some(n) = self.cache.get(s) {
            return Ok(n.clone());
        }
        let name: Name = Arc::from(s);
        if self.cache.len() < 4096 {
            self.cache.insert(Box::from(s), name.clone());
        }
        Ok(name)
    }
}

fn put_status(out: &mut Vec<u8>, s: Status) {
    out.push(s as u8);
}

/// Optional fields after a record's others, each as its tag and bytes, in
/// tag order, only when present and not empty, so a record without them
/// carries no extra bytes (see [`Record`]).
mod ext {
    /// Who asked (audit).
    pub const BY: u8 = 1;
    /// A `Start`'s trace context.
    pub const TRACE: u8 = 2;
}

fn put_ext<B: AsRef<[u8]>>(out: &mut Vec<u8>, tag: u8, v: &Option<B>) {
    if let Some(b) = v
        && !b.as_ref().is_empty()
    {
        out.push(tag);
        put_bytes(out, b.as_ref());
    }
}

/// The optional fields after a record's others: those with tags up to
/// `last`, each once, in order and not empty, so a stray byte still fails
/// the record.
fn exts(r: &mut Reader<'_>, last: u8) -> DecodeResult<[Option<Payload>; 2]> {
    let mut out = [None, None];
    let mut prev = 0u8;
    while !r.is_empty() {
        let tag = r.u8()?;
        if tag <= prev || tag > last {
            return Err(DecodeError("trailing bytes"));
        }
        let b = r.bytes()?;
        if b.is_empty() {
            return Err(DecodeError("trailing bytes"));
        }
        out[usize::from(tag - 1)] = Some(Arc::from(b));
        prev = tag;
    }
    Ok(out)
}

/// The `by` of a record that has no other optional field.
fn by(r: &mut Reader<'_>) -> DecodeResult<Option<Payload>> {
    let [by, _] = exts(r, ext::BY)?;
    Ok(by)
}

/// Encodes the record body: the kind byte, then fields. `defines` holds new
/// interned names to define in a prefix (segment encoding only).
pub fn encode_body<S: AsRef<str>, B: AsRef<[u8]>>(
    rec: &Record<S, B>,
    defines: &[&str],
    names: &mut impl NameEncoder,
    out: &mut Vec<u8>,
) {
    let k = match rec {
        Record::UpdateDone { state, .. } if !state.as_ref().is_empty() => kind::UPDATE_DONE_STATE,
        Record::Step { time: Some(_), .. } => kind::STEP_TIMED,
        _ => rec.kind(),
    };
    if defines.is_empty() {
        out.push(k);
    } else {
        out.push(k | kind::DEFINES);
        put_varint(out, defines.len() as u64);
        for d in defines {
            put_bytes(out, d.as_bytes());
        }
    }
    let seqv = |out: &mut Vec<u8>, s: &Seq| put_varint(out, u64::from(*s));
    match rec {
        Record::Start {
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
            input,
            meta,
        } => {
            put_varint(out, *run);
            put_bytes(out, id.as_ref().as_bytes());
            names.put(out, wtype.as_ref());
            put_varint(out, u64::from(*version));
            out.push(*codec);
            put_varint(out, *started_ms);
            put_varint(out, *deadline_ms);
            match parent {
                None => out.push(0),
                Some(p) => {
                    out.push(1);
                    put_varint(out, u64::from(p.shard));
                    put_varint(out, p.run);
                    put_varint(out, u64::from(p.seq));
                }
            }
            out.push(*policy);
            match from {
                None => out.push(0),
                Some(o) => {
                    out.push(1);
                    put_varint(out, o.run);
                    out.push(o.cause);
                }
            }
            put_varint(out, u64::from(*copies));
            match update {
                None => out.push(0),
                Some(u) => {
                    out.push(1);
                    names.put(out, u.name.as_ref());
                    put_bytes(out, u.id.as_ref().as_bytes());
                    put_bytes(out, u.payload.as_ref());
                }
            }
            put_bytes(out, input.as_ref());
            if let Some(m) = meta {
                put_ext(out, ext::BY, &m.by);
                put_ext(out, ext::TRACE, &m.trace);
            }
        }
        Record::Step {
            run,
            seq,
            name,
            attempt,
            status,
            payload,
            time,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            names.put(out, name.as_ref());
            put_varint(out, u64::from(*attempt));
            put_status(out, *status);
            put_bytes(out, payload.as_ref());
            // Only in a `STEP_TIMED` record.
            if let Some(t) = time {
                put_varint(out, t.began_ms);
                put_varint(out, t.took_ms);
            }
        }
        Record::Attempt {
            run,
            seq,
            name,
            attempt,
            retry_at_ms,
            error,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            names.put(out, name.as_ref());
            put_varint(out, u64::from(*attempt));
            put_varint(out, *retry_at_ms);
            put_bytes(out, error.as_ref());
        }
        Record::Timer {
            run,
            seq,
            name,
            fire_at_ms,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            names.put(out, name.as_ref());
            put_varint(out, *fire_at_ms);
        }
        Record::Fired { run, seq } => {
            put_varint(out, *run);
            seqv(out, seq);
        }
        Record::Signal {
            run,
            name,
            dedup,
            payload,
            by,
        } => {
            put_varint(out, *run);
            names.put(out, name.as_ref());
            put_varint(out, *dedup);
            put_bytes(out, payload.as_ref());
            put_ext(out, ext::BY, by);
        }
        Record::Wait {
            run,
            seq,
            name,
            deadline_ms,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            names.put(out, name.as_ref());
            put_varint(out, *deadline_ms);
        }
        Record::Waited { run, seq, outcome } => {
            put_varint(out, *run);
            seqv(out, seq);
            match outcome {
                WaitOutcome::Signal(ordinal) => {
                    out.push(0);
                    put_varint(out, *ordinal);
                }
                WaitOutcome::TimedOut => out.push(1),
                WaitOutcome::Abandoned => out.push(2),
            }
        }
        Record::Value {
            run,
            seq,
            kind,
            value,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            out.push(*kind);
            put_bytes(out, value.as_ref());
        }
        Record::Child {
            run,
            seq,
            wtype,
            id,
            flags,
            deadline_ms,
            input,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            names.put(out, wtype.as_ref());
            put_bytes(out, id.as_ref().as_bytes());
            out.push(*flags);
            put_varint(out, *deadline_ms);
            put_bytes(out, input.as_ref());
        }
        Record::ChildDone {
            run,
            seq,
            status,
            payload,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            put_status(out, *status);
            put_bytes(out, payload.as_ref());
        }
        Record::Send {
            run,
            seq,
            target,
            name,
            payload,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            put_bytes(out, target.as_ref().as_bytes());
            names.put(out, name.as_ref());
            put_bytes(out, payload.as_ref());
        }
        Record::Cancel { run, reason, by } => {
            put_varint(out, *run);
            put_bytes(out, reason.as_ref());
            put_ext(out, ext::BY, by);
        }
        Record::Done {
            run,
            status,
            finished_ms,
            payload,
            by,
        } => {
            put_varint(out, *run);
            put_status(out, *status);
            put_varint(out, *finished_ms);
            put_bytes(out, payload.as_ref());
            put_ext(out, ext::BY, by);
        }
        Record::Update {
            run,
            name,
            id,
            payload,
            caller,
        } => {
            put_varint(out, *run);
            names.put(out, name.as_ref());
            put_bytes(out, id.as_ref().as_bytes());
            put_bytes(out, payload.as_ref());
            // Only in an `UPDATE_CALL` record: an update without a caller
            // encodes as it always has.
            if let Some(p) = caller {
                put_varint(out, u64::from(p.shard));
                put_varint(out, p.run);
                put_varint(out, u64::from(p.seq));
            }
        }
        Record::UpdateDone {
            run,
            seq,
            ordinal,
            status,
            payload,
            state,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            put_varint(out, *ordinal);
            put_status(out, *status);
            put_bytes(out, payload.as_ref());
            // Only in an `UPDATE_DONE_STATE` record.
            if !state.as_ref().is_empty() {
                put_bytes(out, state.as_ref());
            }
        }
        Record::Heartbeat {
            run,
            seq,
            attempt,
            payload,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            put_varint(out, u64::from(*attempt));
            put_bytes(out, payload.as_ref());
        }
        Record::Awaiting {
            run,
            seq,
            name,
            attempt,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            names.put(out, name.as_ref());
            put_varint(out, u64::from(*attempt));
        }
        Record::Completed {
            run,
            seq,
            attempt,
            status,
            payload,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            put_varint(out, u64::from(*attempt));
            put_status(out, *status);
            put_bytes(out, payload.as_ref());
        }
        Record::Patch { run, at, id } => {
            put_varint(out, *run);
            seqv(out, at);
            put_bytes(out, id.as_ref().as_bytes());
        }
        Record::Attributes { run, seq, payload } => {
            put_varint(out, *run);
            seqv(out, seq);
            put_bytes(out, payload.as_ref());
        }
        Record::Chunk {
            run,
            seq,
            attempt,
            stream,
            offset,
            payload,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            put_varint(out, u64::from(*attempt));
            names.put(out, stream.as_ref());
            put_varint(out, *offset);
            put_bytes(out, payload.as_ref());
        }
        Record::StreamEnd {
            run,
            seq,
            stream,
            chunks,
            status,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            names.put(out, stream.as_ref());
            put_varint(out, *chunks);
            put_status(out, *status);
        }
        Record::Resume {
            run,
            seq,
            attempt,
            by,
        } => {
            put_varint(out, *run);
            seqv(out, seq);
            put_varint(out, u64::from(*attempt));
            put_ext(out, ext::BY, by);
        }
    }
}

fn payload(r: &mut Reader<'_>) -> DecodeResult<Payload> {
    Ok(Arc::from(r.bytes()?))
}

fn inline_name(r: &mut Reader<'_>) -> DecodeResult<Name> {
    Ok(Arc::from(r.str()?))
}

/// Decodes a record body produced by [`encode_body`]. Fails on trailing bytes.
pub fn decode_body(bytes: &[u8], names: &mut impl NameDecoder) -> DecodeResult<RecordBuf> {
    let mut r = Reader::new(bytes);
    let mut k = r.u8()?;
    if k & kind::DEFINES != 0 {
        k &= !kind::DEFINES;
        names.define(&mut r)?;
    }
    let rec = match k {
        kind::START => {
            let run = r.varint()?;
            let id = inline_name(&mut r)?;
            let wtype = names.get(&mut r)?;
            let version = r.varint_u32()?;
            let codec = r.u8()?;
            let started_ms = r.varint()?;
            let deadline_ms = r.varint()?;
            let parent = match r.u8()? {
                0 => None,
                1 => Some(ParentRef {
                    shard: r.varint_u32()?,
                    run: r.varint()?,
                    seq: r.varint_u32()?,
                }),
                _ => return Err(DecodeError("bad parent flag")),
            };
            let policy = r.u8()?;
            let from = match r.u8()? {
                0 => None,
                1 => Some(Origin {
                    run: r.varint()?,
                    cause: r.u8()?,
                }),
                _ => return Err(DecodeError("bad origin flag")),
            };
            let copies = r.varint_u32()?;
            let update = match r.u8()? {
                0 => None,
                1 => Some(Box::new(StartUpdate {
                    name: names.get(&mut r)?,
                    id: inline_name(&mut r)?,
                    payload: payload(&mut r)?,
                })),
                _ => return Err(DecodeError("bad update flag")),
            };
            Record::Start {
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
                input: payload(&mut r)?,
                meta: {
                    let [by, trace] = exts(&mut r, ext::TRACE)?;
                    StartMeta::of(by, trace)
                },
            }
        }
        kind::STEP | kind::STEP_TIMED => Record::Step {
            run: r.varint()?,
            seq: r.varint_u32()?,
            name: names.get(&mut r)?,
            attempt: r.varint_u32()?,
            status: Status::from_u8(r.u8()?)?,
            payload: payload(&mut r)?,
            time: if k == kind::STEP_TIMED {
                Some(StepTime {
                    began_ms: r.varint()?,
                    took_ms: r.varint()?,
                })
            } else {
                None
            },
        },
        kind::ATTEMPT => Record::Attempt {
            run: r.varint()?,
            seq: r.varint_u32()?,
            name: names.get(&mut r)?,
            attempt: r.varint_u32()?,
            retry_at_ms: r.varint()?,
            error: payload(&mut r)?,
        },
        kind::TIMER => Record::Timer {
            run: r.varint()?,
            seq: r.varint_u32()?,
            name: names.get(&mut r)?,
            fire_at_ms: r.varint()?,
        },
        kind::FIRED => Record::Fired {
            run: r.varint()?,
            seq: r.varint_u32()?,
        },
        kind::SIGNAL => Record::Signal {
            run: r.varint()?,
            name: names.get(&mut r)?,
            dedup: r.varint()?,
            payload: payload(&mut r)?,
            by: by(&mut r)?,
        },
        kind::WAIT => Record::Wait {
            run: r.varint()?,
            seq: r.varint_u32()?,
            name: names.get(&mut r)?,
            deadline_ms: r.varint()?,
        },
        kind::WAITED => {
            let run = r.varint()?;
            let seq = r.varint_u32()?;
            let outcome = match r.u8()? {
                0 => WaitOutcome::Signal(r.varint()?),
                1 => WaitOutcome::TimedOut,
                2 => WaitOutcome::Abandoned,
                _ => return Err(DecodeError("bad wait outcome")),
            };
            Record::Waited { run, seq, outcome }
        }
        kind::VALUE => Record::Value {
            run: r.varint()?,
            seq: r.varint_u32()?,
            kind: r.u8()?,
            value: payload(&mut r)?,
        },
        kind::CHILD => Record::Child {
            run: r.varint()?,
            seq: r.varint_u32()?,
            wtype: names.get(&mut r)?,
            id: inline_name(&mut r)?,
            flags: r.u8()?,
            deadline_ms: r.varint()?,
            input: payload(&mut r)?,
        },
        kind::CHILD_DONE => Record::ChildDone {
            run: r.varint()?,
            seq: r.varint_u32()?,
            status: Status::from_u8(r.u8()?)?,
            payload: payload(&mut r)?,
        },
        kind::SEND => Record::Send {
            run: r.varint()?,
            seq: r.varint_u32()?,
            target: inline_name(&mut r)?,
            name: names.get(&mut r)?,
            payload: payload(&mut r)?,
        },
        kind::CANCEL => Record::Cancel {
            run: r.varint()?,
            reason: payload(&mut r)?,
            by: by(&mut r)?,
        },
        kind::DONE => Record::Done {
            run: r.varint()?,
            status: Status::from_u8(r.u8()?)?,
            finished_ms: r.varint()?,
            payload: payload(&mut r)?,
            by: by(&mut r)?,
        },
        kind::UPDATE | kind::UPDATE_CALL => Record::Update {
            run: r.varint()?,
            name: names.get(&mut r)?,
            id: inline_name(&mut r)?,
            payload: payload(&mut r)?,
            caller: if k == kind::UPDATE {
                None
            } else {
                Some(ParentRef {
                    shard: r.varint_u32()?,
                    run: r.varint()?,
                    seq: r.varint_u32()?,
                })
            },
        },
        kind::UPDATE_DONE | kind::UPDATE_DONE_STATE => Record::UpdateDone {
            run: r.varint()?,
            seq: r.varint_u32()?,
            ordinal: r.varint()?,
            status: Status::from_u8(r.u8()?)?,
            payload: payload(&mut r)?,
            state: if k == kind::UPDATE_DONE {
                Arc::from(&[][..])
            } else {
                payload(&mut r)?
            },
        },
        kind::HEARTBEAT => Record::Heartbeat {
            run: r.varint()?,
            seq: r.varint_u32()?,
            attempt: r.varint_u32()?,
            payload: payload(&mut r)?,
        },
        kind::AWAITING => Record::Awaiting {
            run: r.varint()?,
            seq: r.varint_u32()?,
            name: names.get(&mut r)?,
            attempt: r.varint_u32()?,
        },
        kind::COMPLETED => Record::Completed {
            run: r.varint()?,
            seq: r.varint_u32()?,
            attempt: r.varint_u32()?,
            status: Status::from_u8(r.u8()?)?,
            payload: payload(&mut r)?,
        },
        kind::PATCH => Record::Patch {
            run: r.varint()?,
            at: r.varint_u32()?,
            id: inline_name(&mut r)?,
        },
        kind::ATTRIBUTES => Record::Attributes {
            run: r.varint()?,
            seq: r.varint_u32()?,
            payload: payload(&mut r)?,
        },
        kind::CHUNK => Record::Chunk {
            run: r.varint()?,
            seq: r.varint_u32()?,
            attempt: r.varint_u32()?,
            stream: names.get(&mut r)?,
            offset: r.varint()?,
            payload: payload(&mut r)?,
        },
        kind::STREAM_END => Record::StreamEnd {
            run: r.varint()?,
            seq: r.varint_u32()?,
            stream: names.get(&mut r)?,
            chunks: r.varint()?,
            status: Status::from_u8(r.u8()?)?,
        },
        kind::RESUME => Record::Resume {
            run: r.varint()?,
            seq: r.varint_u32()?,
            attempt: r.varint_u32()?,
            by: by(&mut r)?,
        },
        _ => return Err(DecodeError("unknown record kind")),
    };
    if !r.is_empty() {
        return Err(DecodeError("trailing bytes after record"));
    }
    Ok(rec)
}

/// Canonical encoding of one record, appended to `out` with a varint length
/// prefix so records can be concatenated into journals and blobs.
pub fn encode_canonical<S: AsRef<str>, B: AsRef<[u8]>>(rec: &Record<S, B>, out: &mut Vec<u8>) {
    // The body goes straight into `out` after a one-byte length, which
    // fits bodies under 128 bytes; a longer one moves over for its varint.
    let at = out.len();
    out.push(0);
    encode_body(rec, &[], &mut Inline, out);
    let len = out.len() - at - 1;
    if len < 0x80 {
        out[at] = len as u8;
    } else {
        // The varint on the stack: the body moves once, nothing is
        // allocated for the prefix.
        let mut prefix = [0u8; 10];
        let mut n = 0;
        let mut v = len as u64;
        while v >= 0x80 {
            prefix[n] = (v as u8) | 0x80;
            v >>= 7;
            n += 1;
        }
        prefix[n] = v as u8;
        out.splice(at..at + 1, prefix[..=n].iter().copied());
    }
}

/// Decodes a concatenation of [`encode_canonical`] records.
pub fn decode_canonical_all(bytes: &[u8]) -> DecodeResult<Vec<RecordBuf>> {
    let mut out = Vec::new();
    decode_canonical_into(bytes, &mut InlineDecoder::default(), &mut out)?;
    Ok(out)
}

/// [`decode_canonical_all`] into `out` (cleared first), with a name cache
/// the caller keeps: names repeated across journals (workflow types, step
/// names) are then decoded without allocating.
pub fn decode_canonical_into(
    bytes: &[u8],
    names: &mut InlineDecoder,
    out: &mut Vec<RecordBuf>,
) -> DecodeResult<()> {
    out.clear();
    let mut r = Reader::new(bytes);
    while !r.is_empty() {
        let body = r.bytes()?;
        out.push(decode_body(body, names)?);
    }
    Ok(())
}

/// A plain start record (no origin, copies or update).
#[allow(clippy::too_many_arguments)]
pub fn start_record(
    run: RunId,
    id: Name,
    wtype: Name,
    version: u32,
    started_ms: u64,
    deadline_ms: u64,
    parent: Option<ParentRef>,
    policy: u8,
    input: Payload,
) -> RecordBuf {
    Record::Start {
        run,
        id,
        wtype,
        version,
        codec: 1,
        started_ms,
        deadline_ms,
        parent,
        policy,
        from: None,
        copies: 0,
        update: None,
        input,
        meta: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn samples() -> Vec<RecordBuf> {
        let refs: Vec<RecordRef<'static>> = vec![
            Record::Start {
                run: 7,
                id: "order-123",
                wtype: "checkout",
                version: 2,
                codec: 1,
                started_ms: 1_700_000_000_000,
                deadline_ms: 0,
                parent: Some(ParentRef {
                    shard: 3,
                    run: 9,
                    seq: 4,
                }),
                policy: policy::RUNNING_USE_EXISTING | policy::CLOSE_ABANDON,
                from: Some(Origin {
                    run: 6,
                    cause: origin::CONTINUED,
                }),
                copies: 0,
                update: Some(Box::new(StartUpdate {
                    name: "addItem",
                    id: "u-1",
                    payload: b"{}",
                })),
                input: b"{\"a\":1}",
                meta: Some(Box::new(StartMeta {
                    by: Some(b"token:full"),
                    trace: Some(b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
                })),
            },
            Record::Step {
                run: 7,
                seq: 0,
                name: "quote",
                attempt: 1,
                status: Status::Ok,
                payload: b"12",
                time: None,
            },
            Record::Step {
                run: 7,
                seq: 2,
                name: "ship",
                attempt: 2,
                status: Status::Ok,
                payload: b"{}",
                time: Some(StepTime {
                    began_ms: 1_762_000_000_123,
                    took_ms: 1_500,
                }),
            },
            Record::Attempt {
                run: 7,
                seq: 1,
                name: "charge",
                attempt: 1,
                retry_at_ms: 5,
                error: b"boom",
            },
            Record::Timer {
                run: 7,
                seq: 2,
                name: "cool-off",
                fire_at_ms: 99,
            },
            Record::Fired { run: 7, seq: 2 },
            Record::Signal {
                run: 7,
                name: "review",
                dedup: 0,
                payload: b"true",
                by: None,
            },
            Record::Signal {
                run: 7,
                name: "review",
                dedup: 3,
                payload: b"false",
                by: Some(b"proxy:ana@example.com"),
            },
            Record::Wait {
                run: 7,
                seq: 3,
                name: "review",
                deadline_ms: 0,
            },
            Record::Waited {
                run: 7,
                seq: 3,
                outcome: WaitOutcome::Signal(0),
            },
            Record::Waited {
                run: 7,
                seq: 9,
                outcome: WaitOutcome::TimedOut,
            },
            Record::Waited {
                run: 7,
                seq: 8,
                outcome: WaitOutcome::Abandoned,
            },
            Record::Value {
                run: 7,
                seq: 4,
                kind: value_kind::RANDOM,
                value: &[1, 2, 3],
            },
            Record::Child {
                run: 7,
                seq: 5,
                wtype: "ship",
                id: "order-123/ship",
                flags: 0,
                deadline_ms: 0,
                input: b"{}",
            },
            Record::ChildDone {
                run: 7,
                seq: 5,
                status: Status::Failed,
                payload: b"x",
            },
            Record::Send {
                run: 7,
                seq: 6,
                target: "other",
                name: "ping",
                payload: b"",
            },
            Record::Cancel {
                run: 7,
                reason: b"user",
                by: Some(b"token:client"),
            },
            Record::Update {
                run: 7,
                name: "addItem",
                id: "u-2",
                payload: b"3",
                caller: None,
            },
            Record::Update {
                run: 7,
                name: "add",
                id: "call-1",
                payload: b"5",
                caller: Some(ParentRef {
                    shard: 3,
                    run: 12,
                    seq: 4,
                }),
            },
            Record::UpdateDone {
                run: 7,
                seq: 10,
                ordinal: 1,
                status: Status::Ok,
                payload: b"4",
                state: b"",
            },
            Record::UpdateDone {
                run: 7,
                seq: 11,
                ordinal: 2,
                status: Status::Ok,
                payload: b"6",
                state: b"{\"n\":1}",
            },
            Record::Heartbeat {
                run: 7,
                seq: 11,
                attempt: 2,
                payload: b"{\"page\":4}",
            },
            Record::Awaiting {
                run: 7,
                seq: 12,
                name: "approval",
                attempt: 1,
            },
            Record::Completed {
                run: 7,
                seq: 12,
                attempt: 1,
                status: Status::Ok,
                payload: b"true",
            },
            Record::Patch {
                run: 7,
                at: 13,
                id: "refund-rounding",
            },
            Record::Attributes {
                run: 7,
                seq: 13,
                payload: b"{\"status\":\"paid\"}",
            },
            Record::Chunk {
                run: 7,
                seq: 14,
                attempt: 2,
                stream: "tokens",
                offset: 3,
                payload: b"\"lo\"",
            },
            Record::Chunk {
                run: 7,
                seq: stream::WORKFLOW,
                attempt: 0,
                stream: "progress",
                offset: 0,
                payload: b"\0raw",
            },
            Record::StreamEnd {
                run: 7,
                seq: 14,
                stream: "tokens",
                chunks: 4,
                status: Status::Failed,
            },
            Record::Attempt {
                run: 7,
                seq: 15,
                name: "charge",
                attempt: 5,
                retry_at_ms: PAUSED,
                error: b"declined",
            },
            Record::Resume {
                run: 7,
                seq: 15,
                attempt: 5,
                by: Some(b"token"),
            },
            Record::Resume {
                run: 7,
                seq: 15,
                attempt: 6,
                by: None,
            },
            Record::Done {
                run: 7,
                status: Status::ContinuedAsNew,
                finished_ms: 1_700_000_000_500,
                payload: b"\"ch_1\"",
                by: None,
            },
        ];
        refs.iter().map(|r| r.to_buf()).collect()
    }

    #[test]
    fn canonical_round_trip() {
        let recs = samples();
        let mut out = Vec::new();
        for r in &recs {
            encode_canonical(r, &mut out);
        }
        assert_eq!(decode_canonical_all(&out).unwrap(), recs);
    }

    /// The record with its optional trailing fields dropped one at a time
    /// from the end: what a body cut just before each reads as.
    fn shorter(r: &RecordBuf) -> Vec<RecordBuf> {
        let mut out = Vec::new();
        let mut r = r.clone();
        loop {
            let dropped = match &mut r {
                Record::Start { meta, .. } => match meta.as_deref_mut() {
                    Some(m) if m.trace.is_some() => m.trace.take().is_some(),
                    Some(m) if m.by.is_some() => m.by.take().is_some(),
                    _ => false,
                },
                Record::Signal { by, .. }
                | Record::Cancel { by, .. }
                | Record::Done { by, .. }
                | Record::Resume { by, .. } => by.take().is_some(),
                _ => false,
            };
            if !dropped {
                return out;
            }
            if let Record::Start { meta, .. } = &mut r
                && meta
                    .as_ref()
                    .is_some_and(|m| m.by.is_none() && m.trace.is_none())
            {
                *meta = None;
            }
            out.push(r.clone());
        }
    }

    #[test]
    fn truncated_bodies_fail_cleanly() {
        for r in samples() {
            let mut body = Vec::new();
            encode_body(&r, &[], &mut Inline, &mut body);
            // Optional fields are last: a body cut just before one is the
            // record without it (the framing around bodies catches cuts).
            let prefixes: Vec<(usize, RecordBuf)> = shorter(&r)
                .into_iter()
                .map(|s| {
                    let mut b = Vec::new();
                    encode_body(&s, &[], &mut Inline, &mut b);
                    (b.len(), s)
                })
                .collect();
            for cut in 0..body.len() {
                let decoded = decode_body(&body[..cut], &mut InlineDecoder::default());
                match prefixes.iter().find(|(len, _)| *len == cut) {
                    Some((_, s)) => assert_eq!(&decoded.unwrap(), s),
                    None => assert!(decoded.is_err(), "{r:?} cut at {cut}"),
                }
            }
            let mut extra = body.clone();
            extra.push(0);
            assert!(decode_body(&extra, &mut InlineDecoder::default()).is_err());
        }
    }

    #[test]
    fn with_run_rewrites_every_kind() {
        for r in samples() {
            assert_eq!(r.with_run(42).run(), 42);
        }
    }

    /// The length prefix is the body's varint whatever its width: bodies
    /// around each width's edge, after other records in the same buffer.
    #[test]
    fn canonical_prefixes_match_the_body_length() {
        for size in [
            0usize, 100, 110, 127, 128, 200, 16_300, 16_384, 70_000, 2_100_000,
        ] {
            let rec: RecordBuf = Record::Value {
                run: 1,
                seq: 2,
                kind: value_kind::CUSTOM,
                value: Arc::from(vec![7u8; size]),
            };
            let mut body = Vec::new();
            encode_body(&rec, &[], &mut Inline, &mut body);
            let mut expected = vec![0xAA];
            put_bytes(&mut expected, &body);
            let mut out = vec![0xAA];
            encode_canonical(&rec, &mut out);
            assert_eq!(out, expected, "payload of {size} bytes");
            encode_canonical(&rec, &mut out);
            assert_eq!(
                decode_canonical_all(&out[1..]).unwrap(),
                vec![rec.clone(), rec]
            );
        }
    }
}
