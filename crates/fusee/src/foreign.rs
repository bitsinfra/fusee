//! An engine whose workflow code runs in a host language's event loop.
//!
//! Every shard runtime lives on the caller's thread; the caller (the Node.js
//! or Python binding, or the server) drives code itself through each shard's
//! [`Host`] and [`ShardRuntime::quiescent`], and calls [`ForeignEngine::tick`]
//! whenever it is notified (a flush finished) or a deadline passes. Messages
//! between shards are routed synchronously inside `tick`.
//!
//! A [`Group`] splits one engine's shards across several such threads (Node
//! worker threads): each opens the shards it owns with
//! [`ForeignEngine::open_part`], all of them append to the one shared log,
//! and a message for a shard another thread owns goes to that shard's inbox
//! and wakes its owner.

use crate::engine::{Config, Durability, open_directory, runtime_config};
use crate::env::{Env, Notify};
use crate::error::{Error, Result};
use crate::local::{LocalEnv, MemoryEnv};
use crate::runtime::{
    Delivery, ForeignEvent, Host, Message, ResultState, RunDetail, RunOutcome, RunSummary,
    Services, ShardRuntime, SharedRegistry, StartOutcome, StartRequest, builtin_registry,
};
use crate::visibility::ListFilter;
use fusee_core::record::{Lsn, Payload, RunId, Status};
use fusee_core::run::RunInfo;
use fusee_core::shard::{Output, shard_of};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

pub type DynRuntime = ShardRuntime<Box<dyn Env>>;

/// What happened since the last tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineEvent {
    /// Start (or resume, replaying its journal) this run's code.
    Start {
        shard: u32,
        run: RunId,
        info: RunInfo,
    },
    /// Re-check this run's step gates and pump its deliveries.
    Wake {
        shard: u32,
        run: RunId,
    },
    /// The run hit a determinism error and was stopped.
    Stuck {
        shard: u32,
        run: RunId,
        message: String,
    },
    /// The run ended from outside its code; drop its code.
    Ended {
        shard: u32,
        run: RunId,
    },
    Ack {
        token: u64,
    },
    Reject {
        token: u64,
    },
    /// An answer to `token` with a value (an update's result, a query).
    Reply {
        token: u64,
        status: Status,
        payload: Payload,
    },
    /// `token` was refused with a reason.
    Refuse {
        token: u64,
        message: String,
    },
    /// A run's result is durable.
    Result {
        shard: u32,
        run: RunId,
        status: Status,
        payload: Payload,
    },
}

struct ForeignShard {
    runtime: DynRuntime,
    dirty: Arc<AtomicBool>,
    /// A delivery since the last tick handed this shard's records to the
    /// log ([`Flush::Pipeline`]).
    handed: Cell<bool>,
    deadline: Cell<Option<u64>>,
    // An outside caller can retain &DynRuntime and drive it between ticks.
    poll_always: Cell<bool>,
    /// Server mode: what the server relays to this shard from shards
    /// other processes hold.
    relay_in: Option<Arc<RelayBox>>,
    /// Relayed messages the runtime has not acknowledged yet.
    relayed: std::cell::RefCell<Relayed>,
}

/// IDs the runtime sees for relayed messages start here, above any outbox
/// ID of this process, so their acknowledgments are told apart (as in the
/// engine's shard threads).
const RELAYED_BASE: u64 = 1 << 62;
/// Relayed messages awaiting the runtime's acknowledgment, at most: the
/// oldest go first (the sender resends what stays unacknowledged).
const RELAYED_MAX: usize = 1 << 16;
/// Messages relayed to one shard go in frames of about this many bytes.
const RELAY_CHUNK: usize = 1 << 20;

/// Relayed messages not yet acknowledged: the ID the runtime knows each by,
/// to the sender's `(shard, lease epoch, outbox ID)`.
#[derive(Default)]
struct Relayed {
    next: u64,
    pending: std::collections::BTreeMap<u64, (u32, u64, u64)>,
}

impl Relayed {
    fn insert(&mut self, from: (u32, u64, u64)) -> u64 {
        let id = RELAYED_BASE + self.next;
        self.next += 1;
        self.pending.insert(id, from);
        while self.pending.len() > RELAYED_MAX {
            self.pending.pop_first();
        }
        id
    }

    fn take(&mut self, id: u64) -> Option<(u32, u64, u64)> {
        self.pending.remove(&id)
    }
}

enum RelayMail {
    Messages {
        from: u32,
        epoch: u64,
        entries: Vec<crate::runtime::Outgoing>,
    },
    Acked(u64),
}

/// What the relay brought one shard, for the host's thread to apply.
#[derive(Default)]
struct RelayBox {
    mail: Mutex<Vec<RelayMail>>,
    pending: AtomicBool,
}

/// Hands what the relay brings (on its own thread) to the shard's box.
struct RelayInbox {
    boxed: Arc<RelayBox>,
    dirty: Arc<AtomicBool>,
    notify: Notify,
}

impl RelayInbox {
    fn put(&self, mail: RelayMail) {
        lock(&self.boxed.mail).push(mail);
        self.boxed.pending.store(true, Ordering::Release);
        self.dirty.store(true, Ordering::Release);
        (self.notify)();
    }
}

impl crate::env::RelayInbound for RelayInbox {
    fn messages(&self, from: u32, epoch: u64, entries: &[u8]) {
        if let Ok(entries) = crate::runtime::decode_outbox(entries) {
            self.put(RelayMail::Messages {
                from,
                epoch,
                entries,
            });
        }
    }

    fn acked(&self, id: u64) {
        self.put(RelayMail::Acked(id));
    }
}

/// Where a foreign host's flushes run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flush {
    /// On the host's thread when nothing else is pending (a lone
    /// workflow's gate, an idle tick); otherwise on the log's threads.
    Inline,
    /// Always on the shared log's threads, handed over as soon as a
    /// delivery appends a record, so the write overlaps the host's code up
    /// to the next gate. A gate whose records are in flight, or a tick with
    /// nothing else to do, waits for them by spinning briefly when nothing
    /// else waits for the host's thread: the flush runs off the
    /// interpreter thread. The default on the shared log
    /// (`FUSEE_SDK_FLUSH=inline` turns it off): faster for a chain, slower
    /// for a wide fan-out. Its waits spin only while a core is spare
    /// ([`crate::spin::spare_core`]), so worker threads on every core do
    /// not spin against each other. Only the first
    /// delivery of a host's batch hands its records over: the rest of a
    /// fan-out joins the next write instead of each starting one.
    Pipeline,
    /// Always on the log's threads, and the host's thread never waits for
    /// one (`inline_flush: false`).
    Background,
}

/// A tick hands the host at most about this many events: a mass wake then
/// reaches its code in batches, each run continuing before later ones load,
/// instead of every run loading before the first continues.
const EVENT_BATCH: usize = 4096;

/// How long a host's thread spins for a flush in flight at most.
const FLUSH_SPIN_CAP: Duration = Duration::from_micros(60);

pub struct ForeignEngine {
    /// Indexed by shard number; `None` for a shard another part owns.
    shards: Vec<Option<ForeignShard>>,
    /// The shard a tick visits first (see `tick_rounds`).
    first_shard: Cell<usize>,
    count: u32,
    limits: std::sync::Arc<dyn crate::limits::LimitBackend>,
    /// This engine is one part of a [`Group`].
    part: Option<Part>,
    /// What every shard's runtime knows of the code: built-in workflows
    /// and the object types the host registered.
    registry: SharedRegistry,
    flush: Flush,
    /// Server mode with per-shard leases: carries messages to and from
    /// shards other processes hold.
    relay: Option<Arc<dyn crate::env::Relay>>,
    _writer: Option<std::fs::File>,
}

/// The flush mode for `config`: [`Flush::Pipeline`] on the shared log of a
/// host with more than two cores, else [`Flush::Inline`]; background when
/// `inline_flush` is off. `FUSEE_SDK_FLUSH` (`inline`, `background`) says
/// otherwise.
fn flush_mode(config: &Config, shared_log: bool) -> Flush {
    if !config.inline_flush {
        return Flush::Background;
    }
    // Pipelined by default on the shared log: its waits spin only while a
    // core is spare (`crate::spin::spare_core`). `FUSEE_SDK_FLUSH=inline`
    // flushes inline instead.
    match std::env::var("FUSEE_SDK_FLUSH").as_deref() {
        Ok("background") => Flush::Background,
        Ok("inline") => Flush::Inline,
        _ if shared_log && crate::spin::may_spin() => Flush::Pipeline,
        _ => Flush::Inline,
    }
}

struct Part {
    group: Arc<Group>,
    worker: u32,
}

/// Opens one shard's storage and runtime.
fn open_shard(
    config: &Config,
    shard: u32,
    count: u32,
    wal: Option<&Arc<crate::wal::SharedWal>>,
    registry: &SharedRegistry,
    services: &Services,
    notify: &Notify,
    flush: Flush,
) -> Result<Option<ForeignShard>> {
    let env: Box<dyn Env> = match &config.durability {
        Durability::Memory => Box::new(MemoryEnv::default()),
        Durability::Custom(f) => {
            let env = f(shard)?;
            // Server mode: another process holds this shard's lease. This
            // engine opens without it (its runs are that process's), as a
            // part of a group opens without other parts' shards.
            if env.standby() {
                return Ok(None);
            }
            env
        }
        Durability::Local => match wal {
            Some(w) => Box::new(crate::wal::WalEnv::open(
                &config.dir.join(format!("shard-{shard:03}")),
                shard,
                config.local.clone(),
                w.clone(),
            )?),
            None => Box::new(LocalEnv::open(
                &config.dir.join(format!("shard-{shard:03}")),
                config.local.clone(),
            )?),
        },
    };
    let dirty = Arc::new(AtomicBool::new(true));
    let on_wake = {
        let dirty = dirty.clone();
        let notify = notify.clone();
        Arc::new(move || {
            dirty.store(true, Ordering::Release);
            notify();
        })
    };
    let mut rc = runtime_config(config, shard, count);
    // Pipelined: the runtime hands every flush to the log's threads, and
    // the engine decides when the host's thread waits for one.
    if flush == Flush::Pipeline {
        rc.inline_flush = false;
    }
    let runtime =
        ShardRuntime::open_with(env, rc, registry.clone(), true, services.clone(), on_wake)?;
    let relay_in = runtime.with(|i| i.env.relay()).map(|r| {
        let boxed = Arc::new(RelayBox::default());
        r.attach(
            shard,
            Box::new(RelayInbox {
                boxed: boxed.clone(),
                dirty: dirty.clone(),
                notify: notify.clone(),
            }),
        );
        boxed
    });
    Ok(Some(ForeignShard {
        runtime,
        dirty,
        handed: Cell::new(false),
        deadline: Cell::new(None),
        // Custom environments may complete work when polled.
        poll_always: Cell::new(matches!(config.durability, Durability::Custom(_))),
        relay_in,
        relayed: Default::default(),
    }))
}

/// The relay of the first shard that has one (every shard's is the same).
fn relay_of(shards: &[Option<ForeignShard>]) -> Option<Arc<dyn crate::env::Relay>> {
    shards
        .iter()
        .flatten()
        .find_map(|s| s.runtime.with(|i| i.env.relay()))
}

impl ForeignEngine {
    pub fn open(config: &Config, notify: Notify) -> Result<Self> {
        let (count, writer) = open_directory(config)?;
        let wal = crate::engine::open_shared_log(config, count)?;
        let registry = builtin_registry();
        let services = config.services();
        let limits = services.limits.clone();
        let flush = flush_mode(config, wal.is_some());
        if let (Flush::Pipeline, Some(w)) = (flush, &wal) {
            w.set_direct_submit(true);
        }
        let mut shards = Vec::new();
        for shard in 0..count {
            shards.push(open_shard(
                config,
                shard,
                count,
                wal.as_ref(),
                &registry,
                &services,
                &notify,
                flush,
            )?);
        }
        let relay = relay_of(&shards);
        Ok(Self {
            first_shard: Cell::new(0),
            shards,
            count,
            limits,
            part: None,
            registry,
            flush,
            relay,
            _writer: writer,
        })
    }

    /// Where this engine's flushes run (see [`Flush`]).
    pub fn flush_mode(&self) -> Flush {
        self.flush
    }

    /// Opens, on the calling thread, the shards of `group` that part
    /// `worker` owns (see [`Group::owner`]). `notify` is called (from any
    /// thread) when this part should tick: after its own flushes, and when
    /// another thread leaves it mail or a request.
    pub fn open_part(group: Arc<Group>, worker: u32, notify: Notify) -> Result<Self> {
        if worker >= group.workers {
            return Err(Error::Rejected(format!(
                "no worker {worker}: the group has {}",
                group.workers
            )));
        }
        let count = group.count;
        let registry = builtin_registry();
        let flush = flush_mode(&group.config, group.wal.is_some());
        if let (Flush::Pipeline, Some(w)) = (flush, &group.wal) {
            w.set_direct_submit(true);
        }
        let mut shards: Vec<Option<ForeignShard>> = (0..count).map(|_| None).collect();
        for shard in (0..count).filter(|&s| group.owner(s) == worker) {
            shards[shard as usize] = open_shard(
                &group.config,
                shard,
                count,
                group.wal.as_ref(),
                &registry,
                &group.services,
                &notify,
                flush,
            )?;
        }
        group.attach(worker, notify)?;
        let relay = relay_of(&shards);
        Ok(Self {
            first_shard: Cell::new(0),
            shards,
            count,
            limits: group.services.limits.clone(),
            part: Some(Part { group, worker }),
            registry,
            flush,
            relay,
            _writer: None,
        })
    }

    /// Marks `name` as an object type whose `shared` handlers are shared
    /// (see [`crate::object`]): starts of it create keys' runs, and their
    /// calls take turns. The host runs the type's code as for a workflow.
    pub fn register_object(&self, name: &str, shared: &[String]) -> Result<()> {
        crate::object::check_type(name)?;
        if let Ok(mut r) = self.registry.write() {
            r.register_object(name, crate::object::ObjectDef::new(shared.iter().cloned()));
        }
        Ok(())
    }

    /// Whether this engine holds `shard` (every shard, unless it is a part).
    pub fn owns(&self, shard: u32) -> bool {
        matches!(self.shards.get(shard as usize), Some(Some(_)))
    }

    fn owned(&self) -> impl Iterator<Item = (u32, &ForeignShard)> {
        self.shards
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|s| (i as u32, s)))
    }

    /// Defines (or resizes) a step limit shared by every shard.
    pub fn limit(&self, name: &str, spec: crate::limits::LimitSpec) -> Result<()> {
        spec.validate()?;
        self.limits.define(name, spec);
        Ok(())
    }

    /// `(permits held, steps waiting)` of a limit right now (waiting: the
    /// ones the backend knows of, as `Engine::limit_waiting`).
    pub fn limit_usage(&self, name: &str) -> (u32, usize) {
        (self.limits.in_use(name), self.limits.waiting(name))
    }

    pub fn update(
        &self,
        id: &str,
        name: &str,
        update_id: &str,
        payload: Payload,
        token: Option<u64>,
    ) -> Result<()> {
        self.access(self.shard_of(id))?
            .update(id, name, update_id, payload, token)
    }

    /// The validator's decision on a live update delivered to code.
    pub fn decide_update(
        &self,
        shard: u32,
        token: u64,
        decision: std::result::Result<(), String>,
    ) -> Result<Option<u64>> {
        self.access(shard)?.decide_update(token, decision)
    }

    /// For a query: `(shard, run, position the answer waits for)`.
    pub fn query_point(&self, id: &str) -> Result<(u32, RunId, Lsn)> {
        let shard = self.shard_of(id);
        let (run, lsn) = self.access(shard)?.query_point(id)?;
        Ok((shard, run, lsn))
    }

    /// Loading a parked foreign workflow queues its Start. The SDK must
    /// restore its handlers through replay before evaluating the query.
    pub fn query_route(&self, id: &str) -> Result<(u32, RunId, Lsn, bool)> {
        let (shard, run, lsn) = self.query_point(id)?;
        Ok((
            shard,
            run,
            lsn,
            self.shard(shard)?.runtime.is_foreign_slot(run),
        ))
    }

    pub fn ack_at(&self, shard: u32, lsn: Lsn, token: u64) -> Result<()> {
        self.access(shard)?.ack_at(lsn, token);
        Ok(())
    }

    pub fn complete(
        &self,
        token: &str,
        status: Status,
        payload: Payload,
        reply: Option<u64>,
    ) -> Result<()> {
        let Some((id, ..)) = crate::runtime::parse_token(token) else {
            return Err(Error::Rejected("not a completion token".into()));
        };
        self.access(self.shard_of(&id))?
            .complete(token, status, payload, reply)
    }

    pub fn reset(
        &self,
        id: &str,
        before_step: &str,
        reapply_signals: bool,
        version: Option<u32>,
        token: Option<u64>,
    ) -> Result<RunId> {
        self.access(self.shard_of(id))?
            .reset(id, before_step, reapply_signals, version, token)
    }

    /// Foreign code hit an error it cannot recover from (a recorded update
    /// with no handler): the run stops, as on a determinism error.
    pub fn stuck(&self, shard: u32, run: RunId, message: String) -> Result<()> {
        self.access(shard)?.foreign_stuck(run, message);
        Ok(())
    }

    /// The test kits: the earliest timer (a sleep, a deadline, a retry) of
    /// every shard here.
    pub fn next_timer_ms(&self) -> Option<u64> {
        self.owned()
            .filter_map(|(_, s)| s.runtime.next_timer_ms())
            .min()
    }

    /// The test kits' crash: loads an exported journal (`.fjnl`) as a run on
    /// its workflow ID's shard; its code then replays and continues. With
    /// `before_step` (0-based), the journal is cut just before that step's
    /// record: a crash after the step's function ran and before its result
    /// was recorded (`crash_everywhere`).
    pub fn import_journal(&self, file: &[u8], before_step: Option<u32>) -> Result<()> {
        let j = crate::export::ExportedJournal::decode(file)?;
        let mut records = j.records;
        if let Some(n) = before_step {
            let mut seen = 0;
            let cut = records
                .iter()
                .position(|r| {
                    let step = matches!(r, fusee_core::record::Record::Step { .. });
                    if step {
                        seen += 1;
                    }
                    step && seen == n + 1
                })
                .unwrap_or(records.len());
            records.truncate(cut);
        }
        let shard = self.shard_of(&j.workflow_id);
        self.access(shard)?.import(&records)?;
        Ok(())
    }

    /// `(replaying, live work pending)` of a run's journal machine.
    pub fn code_state(&self, shard: u32, run: RunId) -> Result<Option<(bool, bool)>> {
        Ok(self.access(shard)?.code_state(run))
    }

    /// A query against a run whose code is native (built-in workflows).
    pub fn query_native(&self, id: &str, name: &str, arg: Payload, token: u64) -> Result<()> {
        self.access(self.shard_of(id))?.query(id, name, arg, token)
    }

    pub fn list_where(&self, filter: &ListFilter, limit: usize) -> Vec<RunSummary> {
        self.owned()
            .flat_map(|(_, s)| s.runtime.list_where(filter, limit))
            .collect()
    }

    pub fn shards(&self) -> u32 {
        self.count
    }

    /// The shard's commit position: a step whose gate is at or below it,
    /// and whose not-before time passed, may run.
    pub fn commit(&self, shard: u32) -> Option<Lsn> {
        self.shards
            .get(shard as usize)?
            .as_ref()
            .map(|s| s.runtime.with(|i| i.commit()))
    }

    /// Some shard has work the last tick did not take (a wake arrived after
    /// that shard was polled, or another part left mail or a request).
    pub fn has_dirty(&self) -> bool {
        self.owned().any(|(_, s)| s.dirty.load(Ordering::Acquire))
            || self.part.as_ref().is_some_and(|p| {
                p.group.has_requests(p.worker) || self.owned().any(|(i, _)| p.group.has_mail(i))
            })
    }

    pub fn shard_of(&self, id: &str) -> u32 {
        shard_of(id, self.count)
    }

    fn shard(&self, shard: u32) -> Result<&ForeignShard> {
        match self.shards.get(shard as usize) {
            Some(Some(s)) => Ok(s),
            Some(None) => Err(Error::Rejected(format!(
                "shard {shard} belongs to another worker"
            ))),
            None => Err(Error::Rejected(format!("no shard {shard}"))),
        }
    }

    fn access(&self, shard: u32) -> Result<&DynRuntime> {
        let s = self.shard(shard)?;
        s.dirty.store(true, Ordering::Release);
        Ok(&s.runtime)
    }

    pub fn runtime(&self, shard: u32) -> Result<&DynRuntime> {
        let s = self.shard(shard)?;
        s.poll_always.set(true);
        Ok(&s.runtime)
    }

    pub fn host(&self, shard: u32) -> Result<Rc<dyn Host>> {
        Ok(self.runtime(shard)?.host())
    }

    /// A binding call borrows the host only for this operation.
    pub fn with_host<T>(&self, shard: u32, call: impl FnOnce(&dyn Host) -> T) -> Result<T> {
        Ok(call(self.access(shard)?.host().as_ref()))
    }

    /// Check an external step's durable and time dependencies. A host with
    /// one ready workflow can flush locally without another event-loop turn.
    pub fn may_run_step(
        &self,
        shard: u32,
        run: RunId,
        gate: Lsn,
        not_before: u64,
        inline: bool,
    ) -> Result<bool> {
        let s = self.shard(shard)?;
        s.dirty.store(true, Ordering::Release);
        crate::spin::mark_host_busy();
        let runtime = &s.runtime;
        let host = runtime.host();
        if self.flush == Flush::Pipeline {
            // The code reached a gate: the deliveries before it are over,
            // and the next one hands its records to the log again. Without
            // this a chain that never returns to a tick would hand over
            // only its first step's record; every later write would start
            // at the gate, after the code it was meant to overlap.
            s.handed.set(false);
            // Wait here, before `may_run` registers the gate for a wake.
            if inline && runtime.with(|i| i.commit()) < gate {
                self.wait_for_flush(shard, s, gate);
            }
            return Ok(host.may_run(run, gate, not_before, std::task::Waker::noop()));
        }
        if host.may_run(run, gate, not_before, std::task::Waker::noop()) {
            return Ok(true);
        }
        // Shards another part owns flush on that part's thread.
        if inline
            && self.flush == Flush::Inline
            && self.others_quiet(shard)
            && runtime.flush_solo_gate(run, gate, not_before)?
        {
            return Ok(host.may_run(run, gate, not_before, std::task::Waker::noop()));
        }
        Ok(false)
    }

    /// Nothing waits for the host's thread on any shard but `shard`, and
    /// no other part asked anything of it.
    fn others_quiet(&self, shard: u32) -> bool {
        self.owned()
            .all(|(index, s)| index == shard || s.runtime.quiet(true))
            && self.no_mail()
    }

    /// No other part left this one mail or a request.
    fn no_mail(&self) -> bool {
        self.part.as_ref().is_none_or(|p| {
            !p.group.has_requests(p.worker) && !self.owned().any(|(i, _)| p.group.has_mail(i))
        })
    }

    /// How long the host's thread may spin for a flush in flight: twice
    /// what the log's last write took (hand-off to completion) and a
    /// margin, while that stays within the cap; nothing on hosts without a
    /// spare core.
    fn spin_budget(&self, s: &ForeignShard) -> Duration {
        let last = Duration::from_micros(s.runtime.last_flush_us());
        if !crate::spin::spare_core() || last > FLUSH_SPIN_CAP {
            return Duration::ZERO;
        }
        (last * 2 + Duration::from_micros(10)).min(FLUSH_SPIN_CAP)
    }

    /// A gate at `gate` is shut: when the flush in flight covers it and
    /// nothing else waits for the host's thread, spin for it instead of
    /// waiting for a wake in another turn of the host's loop.
    fn wait_for_flush(&self, shard: u32, s: &ForeignShard, gate: Lsn) {
        match s.runtime.submit_flush() {
            Some(last) if last >= gate => {}
            _ => return,
        }
        if !s.runtime.quiet(false) || !self.others_quiet(shard) {
            return;
        }
        crate::spin::spin_until(self.spin_budget(s), || s.runtime.poll_flush());
    }

    pub fn start(&self, req: StartRequest) -> Result<(u32, StartOutcome)> {
        let shard = self.shard_of(&req.id);
        let out = self.access(shard)?.start(req)?;
        Ok((shard, out))
    }

    /// Runs `f`, a signal, cancel, terminate or reset of `id`, recording
    /// `by`, who asked, on the record it writes (audit; see
    /// [`crate::audit`]).
    pub fn audited<T>(
        &self,
        id: &str,
        by: Option<&str>,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        match by.filter(|b| !b.is_empty()) {
            None => f(),
            Some(b) => self
                .access(self.shard_of(id))?
                .with_by(Some(Arc::from(b.as_bytes())), f),
        }
    }

    pub fn signal(
        &self,
        id: &str,
        name: &str,
        payload: Payload,
        dedup: u64,
        token: Option<u64>,
    ) -> Result<()> {
        self.access(self.shard_of(id))?
            .signal(id, name, payload, dedup, token)
    }

    pub fn cancel(&self, id: &str, reason: &str, token: Option<u64>) -> Result<()> {
        self.access(self.shard_of(id))?.cancel(id, reason, token)
    }

    /// Resumes the run's paused steps (see [`DynRuntime::resume`]).
    pub fn resume(&self, id: &str, token: Option<u64>) -> Result<()> {
        self.access(self.shard_of(id))?.resume(id, token)
    }

    pub fn terminate(&self, id: &str, reason: &str, token: Option<u64>) -> Result<()> {
        self.access(self.shard_of(id))?.terminate(id, reason, token)
    }

    /// `(shard, state)`; `Running` until the result is durable.
    pub fn result(&self, id: &str) -> Result<(u32, ResultState)> {
        let shard = self.shard_of(id);
        Ok((shard, self.access(shard)?.result(id)?))
    }

    pub fn inspect(&self, id: &str) -> Result<Option<RunDetail>> {
        self.access(self.shard_of(id))?.inspect(id)
    }

    /// Reads stream `name` of workflow `id` (see [`ShardRuntime::stream_read`]);
    /// the answer comes as [`EngineEvent::Reply`] for `token`.
    pub fn stream_read(
        &self,
        id: &str,
        run: Option<RunId>,
        name: &str,
        from: u64,
        wait_ms: u64,
        token: u64,
    ) -> Result<()> {
        self.access(self.shard_of(id))?
            .stream_read(id, run, name, from, wait_ms, token)
    }

    pub fn list(&self, limit: usize) -> Vec<RunSummary> {
        self.owned()
            .flat_map(|(_, s)| s.runtime.list(limit))
            .collect()
    }

    pub fn quiescent(&self, shard: u32, run: RunId) -> Result<Option<Delivery>> {
        let rt = self.access(shard)?;
        let d = rt.quiescent(run)?;
        // The delivery appended its record and the code it resumes runs
        // to its next gate: write meanwhile. Later deliveries of the same
        // batch wait for that write or the gate's, together.
        if d.is_some() && self.flush == Flush::Pipeline {
            let s = self.shard(shard)?;
            if !s.handed.replace(true) {
                rt.submit_flush();
            }
        }
        Ok(d)
    }

    pub fn finish(&self, shard: u32, run: RunId, outcome: RunOutcome) -> Result<()> {
        let rt = self.access(shard)?;
        rt.foreign_finish(run, outcome);
        if self.flush == Flush::Pipeline {
            rt.submit_flush();
        }
        Ok(())
    }

    /// Advances every shard. Returns events and when to tick again.
    pub fn tick(&self) -> (Vec<EngineEvent>, Option<u64>) {
        self.tick_idle(false)
    }

    /// [`tick`](Self::tick) for a host with nothing else to do on its
    /// thread (`idle`): with [`Flush::Pipeline`], when the tick yields no
    /// event and a flush is in flight, it spins briefly for the flush and
    /// ticks again, rather than leaving the host to wait for a wake.
    pub fn tick_idle(&self, idle: bool) -> (Vec<EngineEvent>, Option<u64>) {
        let (events, next) = self.tick_once();
        if !idle || self.flush != Flush::Pipeline || !events.is_empty() || next == Some(0) {
            return (events, next);
        }
        let waiting: Vec<&ForeignShard> = self
            .owned()
            .filter(|(_, s)| s.runtime.submit_flush().is_some())
            .map(|(_, s)| s)
            .collect();
        let Some(budget) = waiting.iter().map(|s| self.spin_budget(s)).max() else {
            return (events, next);
        };
        // Only the writes wait: no delivery, event or start anywhere.
        if !self.owned().all(|(_, s)| s.runtime.quiet(false)) || !self.no_mail() {
            return (events, next);
        }
        let mut done = None;
        crate::spin::spin_until(budget, || {
            done = waiting.iter().find(|s| s.runtime.poll_flush());
            done.is_some()
        });
        match done {
            Some(s) => {
                s.dirty.store(true, Ordering::Release);
                self.tick_once()
            }
            None => (events, next),
        }
    }

    fn tick_once(&self) -> (Vec<EngineEvent>, Option<u64>) {
        crate::spin::mark_host_busy();
        let mut unfinished = false;
        for (_, s) in self.owned() {
            s.handed.set(false);
        }
        if let Some(p) = &self.part {
            self.answer(p.group.take_requests(p.worker));
        }
        // Several shards to tick: an idle shard flushes inline, so each
        // would write its own records in turn. All but the last hand theirs
        // to the log instead, and the last one's inline write (or the log)
        // takes them together.
        let held: Vec<&ForeignShard> = if self.flush == Flush::Inline {
            let dirty: Vec<&ForeignShard> = self
                .owned()
                .filter(|(_, s)| s.dirty.load(Ordering::Acquire))
                .map(|(_, s)| s)
                .collect();
            match dirty.split_last() {
                Some((_, rest)) if !rest.is_empty() => rest.to_vec(),
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        };
        for s in &held {
            s.runtime.set_inline_flush(false);
        }
        let events = self.tick_rounds(&mut unfinished);
        for s in &held {
            s.runtime.set_inline_flush(true);
        }
        let next = if unfinished {
            Some(0)
        } else {
            self.owned().filter_map(|(_, s)| s.deadline.get()).min()
        };
        (events, next)
    }

    fn tick_rounds(&self, unfinished: &mut bool) -> Vec<EngineEvent> {
        let mut events = Vec::new();
        for _ in 0..64 {
            let mut moved = false;
            if let Some(p) = &self.part {
                self.take_mail(&p.group);
            }
            // The shard that takes the batch's room first changes from one
            // tick to the next, so a mass wake reaches every shard's runs.
            let n = self.shards.len().max(1);
            let first = self.first_shard.get() % n;
            for i in (0..n).map(|k| (first + k) % n) {
                let Some(s) = self.shards.get(i).and_then(Option::as_ref) else {
                    continue;
                };
                let rt = &s.runtime;
                if !s.dirty.swap(false, Ordering::AcqRel)
                    && !s.poll_always.get()
                    && !rt.needs_foreign_poll(s.deadline.get())
                {
                    continue;
                }
                let shard = i as u32;
                self.take_relayed(s);
                // The runtime stops its rounds once a batch of events waits,
                // and loads no more runs than the batch has room for.
                let room = EVENT_BATCH.saturating_sub(events.len());
                rt.set_load_room(room);
                let t = rt.tick_with(&|| rt.foreign_len() >= room);
                rt.set_load_room(usize::MAX);
                s.deadline.set(t.next_wake_ms);
                for e in rt.take_foreign() {
                    events.push(match e {
                        ForeignEvent::Start { run, info } => {
                            EngineEvent::Start { shard, run, info }
                        }
                        ForeignEvent::Wake { run } => EngineEvent::Wake { shard, run },
                        ForeignEvent::Stuck { run, message } => EngineEvent::Stuck {
                            shard,
                            run,
                            message,
                        },
                        ForeignEvent::Ended { run } => EngineEvent::Ended { shard, run },
                    });
                }
                for o in rt.take_released() {
                    match o {
                        Output::Ack(token) => events.push(EngineEvent::Ack { token }),
                        Output::Reject(token) => events.push(EngineEvent::Reject { token }),
                        Output::Reply {
                            token,
                            status,
                            payload,
                        } => events.push(EngineEvent::Reply {
                            token,
                            status,
                            payload,
                        }),
                        Output::Refuse { token, message } => events.push(EngineEvent::Refuse {
                            token,
                            message: String::from_utf8_lossy(&message).into_owned(),
                        }),
                        Output::Result {
                            run,
                            status,
                            payload,
                        } => events.push(EngineEvent::Result {
                            shard,
                            run,
                            status,
                            payload,
                        }),
                        _ => {}
                    }
                }
                let mut remote: Vec<(u32, Vec<crate::runtime::Outgoing>)> = Vec::new();
                for o in rt.take_outgoing() {
                    match self.shards.get(o.target as usize) {
                        Some(Some(target)) => {
                            moved = true;
                            target.dirty.store(true, Ordering::Release);
                            let _ = target.runtime.message(o.msg, Some((shard, o.id)));
                        }
                        // Another part's shard. Unacknowledged messages are
                        // sent again, so one a closing part drops is not lost.
                        Some(None) => {
                            if let Some(p) = &self.part {
                                let from = (shard, o.id);
                                p.group.post(o.target, Mail::Message { msg: o.msg, from });
                            } else if self.relay.is_some() {
                                // Another process's shard: through the server.
                                match remote.iter_mut().find(|(t, _)| *t == o.target) {
                                    Some((_, v)) => v.push(o),
                                    None => remote.push((o.target, vec![o])),
                                }
                            }
                        }
                        None => {}
                    }
                }
                if let Some(r) = &self.relay {
                    for (target, entries) in remote {
                        send_relayed(r.as_ref(), shard, target, entries);
                    }
                }
                let mut relay_acks = Vec::new();
                for (to, id) in rt.take_message_acks() {
                    if id >= RELAYED_BASE {
                        // A relayed message: the ack goes back the way it came.
                        relay_acks.extend(s.relayed.borrow_mut().take(id));
                        continue;
                    }
                    match self.shards.get(to as usize) {
                        Some(Some(target)) => {
                            moved = true;
                            target.dirty.store(true, Ordering::Release);
                            target.runtime.outbox_ack(id);
                        }
                        Some(None) => {
                            if let Some(p) = &self.part {
                                p.group.post(to, Mail::Ack(id));
                            }
                        }
                        None => {}
                    }
                }
                if let Some(r) = self.relay.as_ref().filter(|_| !relay_acks.is_empty()) {
                    r.ack(shard, &relay_acks);
                }
                if t.busy {
                    s.dirty.store(true, Ordering::Release);
                    moved = true;
                }
            }
            *unfinished = moved;
            if !moved {
                break;
            }
            if events.len() >= EVENT_BATCH {
                self.first_shard.set(self.first_shard.get().wrapping_add(1));
                // The host handles these first; its next tick goes on.
                *unfinished = true;
                break;
            }
        }
        events
    }

    /// Applies what the server relayed to `s` from other processes' shards.
    fn take_relayed(&self, s: &ForeignShard) {
        let Some(b) = &s.relay_in else { return };
        if !b.pending.load(Ordering::Acquire) || !b.pending.swap(false, Ordering::AcqRel) {
            return;
        }
        for mail in std::mem::take(&mut *lock(&b.mail)) {
            match mail {
                RelayMail::Messages {
                    from,
                    epoch,
                    entries,
                } => {
                    for e in entries {
                        let local = s.relayed.borrow_mut().insert((from, epoch, e.id));
                        // Errors leave it unacknowledged; the sender resends.
                        if s.runtime.message(e.msg, Some((from, local))).is_err() {
                            s.relayed.borrow_mut().take(local);
                        }
                    }
                }
                RelayMail::Acked(id) => s.runtime.outbox_ack(id),
            }
        }
    }

    /// Hands this part's shards the mail other parts left them.
    fn take_mail(&self, group: &Group) {
        for (shard, s) in self.owned() {
            for mail in group.take_mail(shard) {
                s.dirty.store(true, Ordering::Release);
                match mail {
                    Mail::Message { msg, from } => {
                        let _ = s.runtime.message(msg, Some(from));
                    }
                    Mail::Ack(id) => s.runtime.outbox_ack(id),
                }
            }
        }
    }

    /// Answers reads another thread asked of this part's shards.
    fn answer(&self, requests: Vec<Request>) {
        for r in requests {
            let answer = match &r.ask {
                Ask::List { filter, limit } => Answer::List(
                    self.owned()
                        .map(|(i, s)| {
                            let runs = match filter {
                                Some(f) => s.runtime.list_where(f, *limit),
                                None => s.runtime.list(*limit),
                            };
                            (i, runs)
                        })
                        .collect(),
                ),
                Ask::Inspect(id) => Answer::Inspect(self.inspect(id)),
                Ask::Metrics => Answer::Metrics(
                    self.owned()
                        .map(|(i, s)| (i, s.runtime.metrics()))
                        .collect(),
                ),
            };
            r.reply.give(answer);
        }
    }

    /// Per-shard counters in shard-number order.
    pub fn shard_metrics(&self) -> Vec<crate::Metrics> {
        self.owned().map(|(_, s)| s.runtime.metrics()).collect()
    }

    /// Report storage failure to foreign event loops with outstanding callers.
    pub fn check_health(&self) -> Result<()> {
        for (_, s) in self.owned() {
            if let Some(error) = s.runtime.with(|i| i.failed().map(str::to_owned)) {
                return Err(Error::Io(error));
            }
        }
        Ok(())
    }

    /// Flushes everything pending.
    pub fn flush_all(&self) -> Result<()> {
        for (_, s) in self.owned() {
            s.runtime.flush_all()?;
        }
        Ok(())
    }

    /// Clean shutdown: flushes everything pending and checkpoints each shard
    /// so the next open replays nothing.
    pub fn close(&self) -> Result<()> {
        self.detach();
        for (_, s) in self.owned() {
            s.runtime.close()?;
        }
        Ok(())
    }

    /// A part stops taking requests, answering the ones already asked.
    fn detach(&self) {
        if let Some(p) = &self.part {
            self.answer(p.group.detach(p.worker));
        }
    }
}

/// Messages from `from` to `target` (another process's shard), in frames
/// of about [`RELAY_CHUNK`] bytes.
fn send_relayed(
    r: &dyn crate::env::Relay,
    from: u32,
    target: u32,
    entries: Vec<crate::runtime::Outgoing>,
) {
    let mut chunk = Vec::new();
    let mut bytes = 0;
    for o in entries {
        let n = relayed_len(&o.msg);
        if !chunk.is_empty() && bytes + n > RELAY_CHUNK {
            r.send(from, target, &crate::runtime::encode_outbox(&chunk));
            chunk.clear();
            bytes = 0;
        }
        bytes += n;
        chunk.push(o);
    }
    if !chunk.is_empty() {
        r.send(from, target, &crate::runtime::encode_outbox(&chunk));
    }
}

/// About how many bytes `msg` takes in a relay frame.
fn relayed_len(msg: &Message) -> usize {
    64 + match msg {
        Message::ChildStart {
            wtype, id, input, ..
        } => wtype.len() + id.len() + input.len(),
        Message::ChildResult { payload, .. } => payload.len(),
        Message::Signal {
            target,
            name,
            payload,
            ..
        } => target.len() + name.len() + payload.len(),
        Message::Close {
            target,
            wtype,
            reason,
            ..
        } => target.len() + wtype.len() + reason.len(),
    }
}

impl Drop for ForeignEngine {
    fn drop(&mut self) {
        self.detach();
    }
}

/// One engine's shards split across threads, each opening the shards it
/// owns with [`ForeignEngine::open_part`]. The group holds what the parts
/// share: the directory lock, the shared log, step limits and the codec,
/// and an inbox per shard for messages between parts. Its methods may be
/// called from any thread.
pub struct Group {
    config: Config,
    count: u32,
    workers: u32,
    wal: Option<Arc<crate::wal::SharedWal>>,
    services: Services,
    inbox: Vec<Inbox>,
    parts: Vec<PartSlot>,
    _writer: Option<std::fs::File>,
}

// Parts on different threads share the group.
const _: () = {
    const fn shared<T: Send + Sync>() {}
    shared::<Group>();
};

/// Each on its own cache lines: parts on different cores poll their own.
#[derive(Default)]
#[repr(align(128))]
struct Inbox {
    mail: Mutex<Vec<Mail>>,
    pending: AtomicBool,
}

enum Mail {
    Message { msg: Message, from: (u32, u64) },
    Ack(u64),
}

#[derive(Default)]
#[repr(align(128))]
struct PartSlot {
    wake: Mutex<Option<Notify>>,
    /// Open and taking requests; changes under `requests`' lock.
    open: AtomicBool,
    requests: Mutex<Vec<Request>>,
    pending: AtomicBool,
}

enum Ask {
    List {
        filter: Option<ListFilter>,
        limit: usize,
    },
    Inspect(String),
    Metrics,
}

enum Answer {
    List(Vec<(u32, Vec<RunSummary>)>),
    Inspect(Result<Option<RunDetail>>),
    Metrics(Vec<(u32, crate::Metrics)>),
}

struct Request {
    ask: Ask,
    reply: Arc<Reply>,
}

#[derive(Default)]
struct Reply {
    answer: Mutex<Option<Answer>>,
    ready: Condvar,
}

impl Reply {
    fn give(&self, answer: Answer) {
        *lock(&self.answer) = Some(answer);
        self.ready.notify_all();
    }

    fn wait(&self, until: Instant) -> Option<Answer> {
        let mut slot = lock(&self.answer);
        loop {
            if let Some(a) = slot.take() {
                return Some(a);
            }
            let left = until.checked_duration_since(Instant::now())?;
            slot = self
                .ready
                .wait_timeout(slot, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// How long a read across parts waits for a part to answer.
const ASK_TIMEOUT: Duration = Duration::from_secs(30);

impl Group {
    /// Takes the directory lock, fixes the shard count and opens (and
    /// recovers) the shared log, for `workers` parts. Without a shard count
    /// a new directory gets one shard per part. There are at most as many
    /// parts as shards; [`Group::workers`] is the number used.
    pub fn open(config: &Config, workers: u32) -> Result<Arc<Self>> {
        let mut config = config.clone();
        if config.shards.is_none() {
            let existing = match config.durability {
                Durability::Local => crate::engine::read_meta(&config.dir)?.map(|m| m.0),
                _ => None,
            };
            if existing.is_none() {
                config.shards = Some(workers.max(1));
            }
        }
        let (count, writer) = open_directory(&config)?;
        let wal = crate::engine::open_shared_log(&config, count)?;
        let services = config.services();
        let workers = workers.clamp(1, count.max(1));
        Ok(Arc::new(Self {
            config,
            count,
            workers,
            wal,
            services,
            inbox: (0..count).map(|_| Inbox::default()).collect(),
            parts: (0..workers).map(|_| PartSlot::default()).collect(),
            _writer: writer,
        }))
    }

    pub fn shards(&self) -> u32 {
        self.count
    }

    /// Parts (worker threads) the shards are split across.
    pub fn workers(&self) -> u32 {
        self.workers
    }

    /// The part that owns `shard`.
    pub fn owner(&self, shard: u32) -> u32 {
        shard % self.workers
    }

    pub fn shard_of(&self, id: &str) -> u32 {
        shard_of(id, self.count)
    }

    /// Defines (or resizes) a step limit shared by every part.
    pub fn limit(&self, name: &str, spec: crate::limits::LimitSpec) -> Result<()> {
        spec.validate()?;
        self.services.limits.define(name, spec);
        Ok(())
    }

    /// [`ForeignEngine::limit_usage`] across every part.
    pub fn limit_usage(&self, name: &str) -> (u32, usize) {
        let l = &self.services.limits;
        (l.in_use(name), l.waiting(name))
    }

    fn attach(&self, worker: u32, wake: Notify) -> Result<()> {
        let p = &self.parts[worker as usize];
        let _requests = lock(&p.requests);
        if p.open.swap(true, Ordering::AcqRel) {
            return Err(Error::Rejected(format!("worker {worker} is already open")));
        }
        *lock(&p.wake) = Some(wake);
        Ok(())
    }

    /// The part stops taking requests; returns the ones not yet answered.
    fn detach(&self, worker: u32) -> Vec<Request> {
        let p = &self.parts[worker as usize];
        let mut requests = lock(&p.requests);
        p.open.store(false, Ordering::Release);
        *lock(&p.wake) = None;
        p.pending.store(false, Ordering::Release);
        std::mem::take(&mut *requests)
    }

    fn wake(&self, worker: u32) {
        let wake = lock(&self.parts[worker as usize].wake).clone();
        if let Some(w) = wake {
            w();
        }
    }

    fn post(&self, shard: u32, mail: Mail) {
        let Some(inbox) = self.inbox.get(shard as usize) else {
            return;
        };
        lock(&inbox.mail).push(mail);
        inbox.pending.store(true, Ordering::Release);
        self.wake(self.owner(shard));
    }

    fn has_mail(&self, shard: u32) -> bool {
        self.inbox
            .get(shard as usize)
            .is_some_and(|i| i.pending.load(Ordering::Acquire))
    }

    fn take_mail(&self, shard: u32) -> Vec<Mail> {
        match self.inbox.get(shard as usize) {
            // Every tick looks: read first, so an empty inbox's line stays
            // shared instead of moving to this core.
            Some(i)
                if i.pending.load(Ordering::Acquire) && i.pending.swap(false, Ordering::AcqRel) =>
            {
                std::mem::take(&mut *lock(&i.mail))
            }
            _ => Vec::new(),
        }
    }

    fn has_requests(&self, worker: u32) -> bool {
        self.parts[worker as usize].pending.load(Ordering::Acquire)
    }

    fn take_requests(&self, worker: u32) -> Vec<Request> {
        let p = &self.parts[worker as usize];
        if !p.pending.load(Ordering::Acquire) || !p.pending.swap(false, Ordering::AcqRel) {
            return Vec::new();
        }
        std::mem::take(&mut *lock(&p.requests))
    }

    /// Asks each of `workers` and waits for their answers, in that order.
    /// It blocks until each of them ticks: never call it from a part's own
    /// thread.
    fn ask(
        &self,
        workers: impl Iterator<Item = u32>,
        ask: impl Fn() -> Ask,
    ) -> Result<Vec<Answer>> {
        let mut replies = Vec::new();
        for w in workers {
            let p = &self.parts[w as usize];
            let reply = Arc::new(Reply::default());
            {
                let mut requests = lock(&p.requests);
                if !p.open.load(Ordering::Acquire) {
                    return Err(Error::Rejected(format!("worker {w} is not running")));
                }
                requests.push(Request {
                    ask: ask(),
                    reply: reply.clone(),
                });
                p.pending.store(true, Ordering::Release);
            }
            self.wake(w);
            replies.push((w, reply));
        }
        let until = Instant::now() + ASK_TIMEOUT;
        replies
            .into_iter()
            .map(|(w, r)| {
                r.wait(until)
                    .ok_or_else(|| Error::Rejected(format!("worker {w} did not answer")))
            })
            .collect()
    }

    /// Up to `limit` runs of each shard (matching `filter`), in shard order:
    /// what one engine holding every shard lists.
    pub fn list_where(&self, filter: Option<&ListFilter>, limit: usize) -> Result<Vec<RunSummary>> {
        let answers = self.ask(0..self.workers, || Ask::List {
            filter: filter.cloned(),
            limit,
        })?;
        let mut shards: Vec<(u32, Vec<RunSummary>)> = answers
            .into_iter()
            .flat_map(|a| match a {
                Answer::List(l) => l,
                _ => Vec::new(),
            })
            .collect();
        shards.sort_by_key(|(shard, _)| *shard);
        Ok(shards.into_iter().flat_map(|(_, runs)| runs).collect())
    }

    /// A run's summary and journal, from the part that owns it.
    pub fn inspect(&self, id: &str) -> Result<Option<RunDetail>> {
        let owner = self.owner(self.shard_of(id));
        match self
            .ask(std::iter::once(owner), || Ask::Inspect(id.to_owned()))?
            .pop()
        {
            Some(Answer::Inspect(r)) => r,
            _ => Ok(None),
        }
    }

    /// Per-shard counters in shard-number order.
    pub fn shard_metrics(&self) -> Result<Vec<crate::Metrics>> {
        let answers = self.ask(0..self.workers, || Ask::Metrics)?;
        let mut all: Vec<(u32, crate::Metrics)> = answers
            .into_iter()
            .flat_map(|a| match a {
                Answer::Metrics(m) => m,
                _ => Vec::new(),
            })
            .collect();
        all.sort_by_key(|(shard, _)| *shard);
        Ok(all.into_iter().map(|(_, m)| m).collect())
    }
}
