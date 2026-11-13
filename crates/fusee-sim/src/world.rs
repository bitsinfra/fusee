//! The simulated world: shards on one machine, a client, an external
//! service, a seeded scheduler and fault injection.

use crate::disk::SimFile;
use crate::env::{Io, Machine, MachineRef, SimEnv, SimLog, SimLogRef};
use crate::rng::Rng;
use crate::scenario::{Check, Outcome, Setup, UserAction};
use fusee::codec::Codec;
use fusee::ctx::{ChildOptions, Ctx, RetryPolicy, StepOptions};
use fusee::error::Error;
use fusee::limits::{LimitBackend, LimitSpec, LocalLimits};
use fusee::queue::{BATCH_TYPE, ENDED, EVENT, Event, QueueState, Receipt, Rule};
use fusee::runtime::{
    IdPolicy, Message, OnFinished, OnRunning, ParentClose, ResultState, RunOutcome, RuntimeConfig,
    Services, ShardRuntime, SharedRegistry, StartRequest, UpdateRequest, WorkflowFn,
};
use fusee::schedule::{Overlap, SCHEDULE_TYPE, ScheduleSpec, ScheduleState, schedule_id};
use fusee_core::record::{Payload, Record, Status};
use fusee_core::shard::{Output, shard_of};
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// Knobs for one simulation run.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub shards: u32,
    pub workflows: u32,
    /// Simulated milliseconds of client activity with faults on.
    pub duration_ms: u64,
    /// Mean simulated milliseconds between crashes (0 = never).
    pub crash_every_ms: u64,
    pub tear: bool,
    pub flush_fail_per_mille: u32,
    pub max_io_ms: u64,
    pub max_msg_ms: u64,
    pub park_after_ms: u64,
    pub checkpoint_interval_ms: u64,
    pub segment_bytes: u64,
    /// Log format 3: the shards share one log ([`fusee::wal`]); otherwise
    /// each has its own segments.
    pub shared_log: bool,
    /// The shared log offloads records whose encoding is longer than this
    /// to its blob area (0: none). Low, so that ordinary records go there.
    pub offload_bytes: u64,
    /// Simulated milliseconds every acknowledged workflow gets to finish
    /// once faults stop.
    pub settle_ms: u64,
    /// A user scenario ([`crate::scenario`]) in place of the built-in
    /// workload.
    pub user: Option<crate::scenario::UserSetup>,
    #[doc(hidden)]
    pub sabotage: fusee::runtime::Sabotage,
    /// Sabotage: the shared log writes a batch as soon as the blobs it
    /// references are written, before they are durable.
    #[doc(hidden)]
    pub early_blobs: bool,
}

impl SimConfig {
    pub fn for_seed(seed: u64) -> Self {
        let mut r = Rng::new(seed);
        Self {
            seed,
            shards: 1 + r.below(3) as u32,
            workflows: 10 + r.below(40) as u32,
            duration_ms: 2_000,
            crash_every_ms: [0, 150, 400, 900][r.below(4) as usize],
            tear: true,
            flush_fail_per_mille: [0, 0, 2, 10][r.below(4) as usize],
            max_io_ms: 1 + r.below(4),
            max_msg_ms: r.below(3),
            park_after_ms: [0, 5, 50, u64::MAX][r.below(4) as usize],
            checkpoint_interval_ms: [20, 100, 500][r.below(3) as usize],
            segment_bytes: [16 << 10, 64 << 10, 1 << 20][r.below(3) as usize],
            // By parity, so the other knobs of every seed stay as they were.
            shared_log: seed % 2 == 1,
            // From bits of the seed the other knobs do not use.
            offload_bytes: [0, 16, 40, 120][(seed / 18 % 4) as usize],
            settle_ms: 60_000,
            user: None,
            sabotage: Default::default(),
            early_blobs: false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub seed: u64,
    pub crashes: u32,
    pub flush_failures: u32,
    pub workflows: u32,
    pub finished: u32,
    pub service_calls: u64,
    pub sim_ms: u64,
    pub records: u64,
    /// Feature coverage: update replies, query answers, completions
    /// accepted, scheduled runs, resets acknowledged, heartbeat progress
    /// reports.
    pub updates: u32,
    pub queries: u32,
    pub completions: u32,
    pub scheduled: u32,
    pub resets: u32,
    pub heartbeats: u32,
    /// Read-only calls that ran again with a different input after a crash
    /// took back what they had read from.
    pub reads_rerun: u32,
    /// Debounced or batched events acknowledged, and the window runs they
    /// joined.
    pub queued: u32,
    pub windows: u32,
    /// Stream chunks readers received, and readers that read a stream to
    /// its end.
    pub chunks_read: u64,
    pub streams_read: u32,
    /// The seed ran on the shared log (log format 3), and the batches the
    /// last process life ended with.
    pub shared_log: bool,
    pub log_batches: u64,
    /// Records the shared log offloaded to its blob area, over all lives.
    pub offloaded: u64,
}

#[derive(Clone, Debug)]
pub struct Failure {
    pub seed: u64,
    pub message: String,
    pub trace: Vec<String>,
    /// A user scenario's seed ([`crate::scenario::Scenario::run`]).
    pub scenario: bool,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "seed {} failed: {}", self.seed, self.message)?;
        if self.scenario {
            writeln!(f, "reproduce: scenario.run({})", self.seed)?;
        } else {
            writeln!(f, "reproduce: fusee-sim --seed {}", self.seed)?;
        }
        for l in self.trace.iter().rev().take(60).rev() {
            writeln!(f, "  {l}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Simulated time for step functions.

thread_local! {
    static NOW: Cell<u64> = const { Cell::new(0) };
    static TIMERS: RefCell<BinaryHeap<Reverse<(u64, u64)>>> = const { RefCell::new(BinaryHeap::new()) };
    static WAKERS: RefCell<BTreeMap<u64, Waker>> = const { RefCell::new(BTreeMap::new()) };
    static TIMER_SEQ: Cell<u64> = const { Cell::new(0) };
}

/// Sleeps in simulated time (for step functions that model slow services).
pub struct SimSleep {
    at: u64,
    armed: Option<u64>,
}

pub fn sim_sleep(ms: u64) -> SimSleep {
    SimSleep {
        at: NOW.with(Cell::get) + ms,
        armed: None,
    }
}

impl Future for SimSleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if NOW.with(Cell::get) >= self.at {
            return Poll::Ready(());
        }
        if self.armed.is_none() {
            let id = TIMER_SEQ.with(|s| {
                let v = s.get();
                s.set(v + 1);
                v
            });
            TIMERS.with(|t| t.borrow_mut().push(Reverse((self.at, id))));
            WAKERS.with(|w| w.borrow_mut().insert(id, cx.waker().clone()));
            self.armed = Some(id);
        } else if let Some(id) = self.armed {
            WAKERS.with(|w| w.borrow_mut().insert(id, cx.waker().clone()));
        }
        Poll::Pending
    }
}

fn sim_timers_next() -> Option<u64> {
    TIMERS.with(|t| t.borrow().peek().map(|Reverse((at, _))| *at))
}

fn sim_timers_fire(now: u64) -> bool {
    NOW.with(|n| n.set(now));
    let mut any = false;
    loop {
        let due = TIMERS.with(|t| {
            let mut t = t.borrow_mut();
            match t.peek() {
                Some(Reverse((at, id))) if *at <= now => {
                    let id = *id;
                    t.pop();
                    Some(id)
                }
                _ => None,
            }
        });
        let Some(id) = due else { break };
        if let Some(w) = WAKERS.with(|w| w.borrow_mut().remove(&id)) {
            w.wake();
            any = true;
        }
    }
    any
}

fn sim_timers_clear() {
    TIMERS.with(|t| t.borrow_mut().clear());
    WAKERS.with(|w| w.borrow_mut().clear());
}

// ---------------------------------------------------------------------------
// The external service.

/// A mock external service. It logs every call with its idempotency key and
/// input. Responses differ per call, so a step that runs again yields a
/// different value; if any later step had already run on the first value,
/// the log shows one key called with two different inputs.
#[derive(Default)]
pub struct Service {
    pub calls: BTreeMap<String, Vec<u64>>,
    pub violations: Vec<String>,
    pub total: u64,
    pub seed: u64,
    /// Progress each step key reported through heartbeats.
    pub progress: BTreeMap<String, BTreeSet<u32>>,
    pub stalled: BTreeSet<String>,
    /// Completion tokens handed out, in order: (workflow ID, token).
    pub tokens: Vec<(String, String)>,
    pub issued: BTreeMap<String, u32>,
    /// Runs seen per scheduled workflow ID.
    pub ticks: BTreeMap<String, BTreeSet<u64>>,
    /// Inputs of read-only calls per key. Reads may run on state a crash
    /// takes back, so a key read with different inputs is allowed; it is
    /// counted to show that read-only steps really run ahead of the disk.
    pub reads: BTreeMap<String, Vec<u64>>,
    pub reads_rerun: u32,
    /// Tokens a model step asked for per key, and the keys whose call
    /// failed once.
    pub talk_calls: BTreeMap<String, u64>,
    pub talk_failed: BTreeSet<String>,
}

impl Service {
    fn violation(&mut self, v: String) {
        if self.violations.len() < 16 {
            self.violations.push(v);
        }
    }

    /// A step resumed from heartbeat progress `start`: it must be progress
    /// the step reported (a crash may only take back newer progress).
    pub fn resume(&mut self, key: &str, start: u32) {
        if start > 0
            && !self
                .progress
                .get(key)
                .is_some_and(|p| p.contains(&(start - 1)))
        {
            self.violation(format!(
                "step {key} resumed from progress {start}, which it never reported"
            ));
        }
    }

    pub fn report_progress(&mut self, key: &str, p: u32) {
        self.progress.entry(key.to_owned()).or_default().insert(p);
    }

    /// True once per key: the step stalls without heartbeats.
    pub fn stall_once(&mut self, key: &str) -> bool {
        self.stalled.insert(key.to_owned())
    }

    /// Hands out a completion token. The first attempt per workflow fails.
    pub fn issue(&mut self, wf: &str, token: &str) -> Result<(), String> {
        let n = self.issued.entry(wf.to_owned()).or_insert(0);
        *n += 1;
        if *n == 1 {
            return Err("approval system unavailable".into());
        }
        self.tokens.push((wf.to_owned(), token.to_owned()));
        Ok(())
    }

    pub fn tick(&mut self, wf: &str, run: u64) {
        let runs = self.ticks.entry(wf.to_owned()).or_default();
        runs.insert(run);
        if runs.len() > 1 {
            let v = format!(
                "scheduled workflow {wf} was started as runs {runs:?}: one scheduled time fired twice"
            );
            self.violation(v);
        }
    }

    /// A read-only call: logged apart from calls with effects and never a
    /// violation. Answers differ per call like [`Service::call`].
    pub fn read(&mut self, key: &str, input: u64, fail_first: u32) -> Result<u64, String> {
        self.total += 1;
        let reads = self.reads.entry(key.to_owned()).or_default();
        if reads.first().is_some_and(|first| *first != input) && !reads.contains(&input) {
            self.reads_rerun += 1;
        }
        reads.push(input);
        let n = reads.len() as u64;
        if n <= u64::from(fail_first) {
            return Err(format!("transient read failure {n}"));
        }
        let mut h = self.seed ^ 0x8422_2325_cbf2_9ce4;
        for b in key.as_bytes() {
            h = (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3);
        }
        h ^= input.rotate_left(29) ^ n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        Ok(h % 1_000_000)
    }

    /// A model's next token: a new value on every call, as sampling gives
    /// (so a token generated again differs from the first). Fails once per
    /// even key at the third token.
    pub fn token(&mut self, key: &str, i: usize) -> Result<u64, String> {
        self.total += 1;
        let n = self.talk_calls.entry(key.to_owned()).or_insert(0);
        *n += 1;
        let mut h = self.seed ^ 0x7a1c_e5d1_4b3f_0e29;
        for b in key.as_bytes() {
            h = (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3);
        }
        if i == 2 && h.is_multiple_of(2) && self.talk_failed.insert(key.to_owned()) {
            return Err(format!("the model failed at token {i}"));
        }
        h ^= (i as u64).rotate_left(23) ^ n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        Ok(h % 1_000_000)
    }

    pub fn call(&mut self, key: &str, input: u64, fail_first: u32) -> Result<u64, String> {
        self.total += 1;
        let calls = self.calls.entry(key.to_owned()).or_default();
        if let Some(first) = calls.first()
            && *first != input
            && self.violations.len() < 16
        {
            self.violations.push(format!(
                "service key {key} was called with input {first} and later with {input}: \
                     a step ran on state a crash took back"
            ));
        }
        calls.push(input);
        let n = calls.len() as u64;
        if n <= u64::from(fail_first) {
            return Err(format!("transient failure {n}"));
        }
        let mut h = self.seed ^ 0xcbf2_9ce4_8422_2325;
        for b in key.as_bytes() {
            h = (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3);
        }
        h ^= input.rotate_left(17) ^ n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        Ok(h % 1_000_000)
    }
}

pub type ServiceRef = Arc<Mutex<Service>>;

// ---------------------------------------------------------------------------
// Workflows.

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Job {
    Order {
        n: u64,
    },
    Fanout {
        n: u32,
    },
    Pinger {
        target: String,
    },
    /// Updates and queries.
    Counter,
    /// Continue-as-new.
    Looper {
        left: u32,
        acc: u64,
    },
    /// Patching: new code is deployed at the first crash.
    Patchy {
        n: u64,
    },
    /// Heartbeats and heartbeat timeouts.
    Export,
    /// Async completion.
    Approval,
    /// Parent close policy.
    Parent {
        kids: u32,
    },
    /// Reset.
    Resettable {
        n: u64,
    },
    /// Read-only steps feeding a step with effects.
    Reader {
        n: u64,
    },
    /// Streams: a model call as a step streams its tokens (continuing after
    /// the recorded prefix when it runs again), and the workflow streams
    /// its progress.
    Talker {
        n: u32,
    },
}

/// What a [`Job::Talker`] returns: a hash of its tokens in order, which the
/// checker compares with what readers of its stream saw.
pub fn tokens_hash(tokens: &[u64]) -> u64 {
    tokens.iter().fold(0xcbf2_9ce4_8422_2325, |h, t| {
        (h ^ t).wrapping_mul(0x100_0000_01b3)
    })
}

/// The progress a [`Job::Talker`] streams.
pub const TALKER_PROGRESS: [&str; 3] = ["start", "generated", "end"];

/// A deterministic stand-in for encryption (the simulator must be
/// reproducible from its seed).
#[derive(Debug)]
pub struct SimCodec;

impl Codec for SimCodec {
    fn encode(&self, p: &[u8]) -> Result<Vec<u8>, String> {
        let mut v = b"SIMC".to_vec();
        v.extend(p.iter().rev().map(|b| b ^ 0x5a));
        Ok(v)
    }

    fn decode(&self, d: &[u8]) -> Result<Vec<u8>, String> {
        match d.strip_prefix(b"SIMC") {
            Some(r) => Ok(r.iter().rev().map(|b| b ^ 0x5a).collect()),
            None => Ok(d.to_vec()),
        }
    }
}

pub(crate) fn erase<I, O, F, Fut>(f: F) -> WorkflowFn
where
    I: Serialize + serde::de::DeserializeOwned + 'static,
    O: Serialize + serde::de::DeserializeOwned + 'static,
    F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, Error>> + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |ctx: Ctx| {
        let f = f.clone();
        let input = ctx.input_bytes(|bytes| serde_json::from_slice::<I>(bytes));
        Box::pin(async move {
            let map = |e: Error| -> (Status, Payload) {
                let (s, m) = match e {
                    Error::Cancelled(r) => (Status::Cancelled, r),
                    Error::TimedOut => (Status::TimedOut, "timed out".into()),
                    other => (Status::Failed, other.to_string()),
                };
                (s, Arc::from(m.into_bytes()))
            };
            let input: I = input.map_err(|e| map(Error::Codec(e.to_string())))?;
            let out = f(ctx, input).await.map_err(map)?;
            serde_json::to_vec(&out)
                .map(Arc::from)
                .map_err(|e| map(Error::Codec(e.to_string())))
        }) as Pin<Box<dyn Future<Output = RunOutcome>>>
    })
}

/// The client's request for a debounced start or a batched event.
fn enqueue_action(
    workflow: &str,
    key: &str,
    event_id: &str,
    input: serde_json::Value,
    rule: Rule,
    continue_at: u32,
) -> Action {
    let event = Event {
        id: event_id.into(),
        rule,
        input,
    };
    let (wtype, coordinator, state, event) = fusee::queue::request(workflow, key, &event);
    let mut state: QueueState = serde_json::from_value(state).unwrap_or_default();
    state.continue_at = continue_at;
    Action::Enqueue {
        coordinator,
        wtype,
        workflow: workflow.into(),
        state: serde_json::to_vec(&state).unwrap_or_default(),
        event_id: event_id.into(),
        event: serde_json::to_vec(&event).unwrap_or_default(),
    }
}

async fn call(
    ctx: &Ctx,
    svc: &ServiceRef,
    name: &str,
    input: u64,
    fail_first: u32,
) -> Result<u64, Error> {
    let mut opts = StepOptions::default().retry(RetryPolicy::exponential(
        fail_first + 2,
        std::time::Duration::from_millis(1),
        std::time::Duration::from_millis(4),
    ));
    if name == "charge" || name == "ship" {
        opts = opts.limit("svc");
        // Odd seeds queue for the permit by priority and tenant (neither
        // is journaled, so replay passes the same ones again).
        if svc.lock().is_ok_and(|s| s.seed % 2 == 1) {
            opts = opts.priority((input % 3) as i32 - 1);
            if input % 2 == 1 {
                opts = opts
                    .fairness(&format!("tenant-{}", input % 5))
                    .fairness_weight(1 + (input % 3) as u32);
            }
        }
    }
    ctx.step(name, || {
        let key = ctx.key();
        let svc = svc.clone();
        async move {
            let delay = {
                let s = svc.lock().map_err(|_| "poisoned".to_string())?;
                (s.total + input) % 3
            };
            sim_sleep(delay).await;

            svc.lock()
                .map_err(|_| "poisoned".to_string())?
                .call(&key, input, fail_first)
        }
    })
    .options(opts)
    .await
}

/// A read-only step: runs without waiting for the records before it.
async fn read(
    ctx: &Ctx,
    svc: &ServiceRef,
    name: &str,
    input: u64,
    fail_first: u32,
) -> Result<u64, Error> {
    let opts = StepOptions::default()
        .read_only()
        .retry(RetryPolicy::exponential(
            fail_first + 2,
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(4),
        ));
    ctx.step(name, || {
        let key = ctx.key();
        let svc = svc.clone();
        async move {
            let delay = {
                let s = svc.lock().map_err(|_| "poisoned".to_string())?;
                (s.total + input) % 2
            };
            sim_sleep(delay).await;
            svc.lock()
                .map_err(|_| "poisoned".to_string())?
                .read(&key, input, fail_first)
        }
    })
    .options(opts)
    .await
}

pub fn register(registry: &SharedRegistry, svc: &ServiceRef, deployed: Arc<AtomicBool>) {
    let mut r = registry.write().unwrap_or_else(|e| e.into_inner());
    let s = svc.clone();
    let deployed2 = deployed.clone();
    r.register(
        "job",
        1,
        erase(move |ctx: Ctx, job: Job| {
            let svc = s.clone();
            let deployed2 = deployed2.clone();
            async move {
                match job {
                    Job::Order { n } => {
                        let quote = call(&ctx, &svc, "quote", n, 0).await?;
                        let charge = call(&ctx, &svc, "charge", quote, 2).await?;
                        ctx.sleep("cool-off", std::time::Duration::from_millis(3))
                            .await?;
                        let approved: Option<u64> = match ctx
                            .wait_for("approve")
                            .timeout(std::time::Duration::from_millis(60))
                            .await
                        {
                            Ok(v) => v,
                            Err(Error::Cancelled(_)) => {
                                let r = call(&ctx, &svc, "refund", charge, 0).await?;
                                return Err(Error::Cancelled(format!("refunded {r}")));
                            }
                            Err(e) => return Err(e),
                        };
                        match approved {
                            Some(v) => {
                                let id = format!("{}/ship", ctx.workflow_id());
                                ctx.child::<u64, u64>("ship", &id, &(charge ^ v)).await
                            }
                            None => call(&ctx, &svc, "refund", charge, 0).await,
                        }
                    }
                    Job::Fanout { n } => {
                        let branches: Vec<_> = (0..n)
                            .map(|i| {
                                let ctx = ctx.clone();
                                let svc = svc.clone();
                                async move {
                                    call(&ctx, &svc, &format!("leg{i}"), u64::from(i), i % 2).await
                                }
                            })
                            .collect();
                        let mut sum = 0u64;
                        for r in ctx.all(branches).await {
                            sum = sum.wrapping_add(r?);
                        }
                        let t = ctx.now_ms()?;
                        let total = call(&ctx, &svc, "total", sum ^ (t % 7), 0).await?;
                        Ok(total)
                    }
                    Job::Pinger { target } => {
                        let v = call(&ctx, &svc, "prep", 1, 0).await?;
                        ctx.signal_workflow(&target, "approve", &v)?;
                        Ok(v)
                    }
                    Job::Counter => {
                        let total = Rc::new(Cell::new(0u64));
                        let t = total.clone();
                        ctx.on_query("total", move |_: serde_json::Value| t.get());
                        let (t, c, s2) = (total.clone(), ctx.clone(), svc.clone());
                        ctx.on_update(
                            "add",
                            |v: &u64| {
                                if *v >= 1000 {
                                    Err(format!("{v} is too large"))
                                } else {
                                    Ok(())
                                }
                            },
                            move |v: u64| {
                                let (t, c, svc) = (t.clone(), c.clone(), s2.clone());
                                async move {
                                    let r = call(&c, &svc, "apply", v, 0).await?;
                                    t.set(t.get() + r % 100);
                                    Ok(t.get())
                                }
                            },
                        );
                        let _ = ctx
                            .wait_for::<u64>("close")
                            .timeout(Duration::from_millis(150))
                            .await?;
                        Ok(total.get())
                    }
                    Job::Looper { left, acc } => {
                        let v =
                            call(&ctx, &svc, "loop", acc.wrapping_add(u64::from(left)), 0).await?;
                        if left == 0 {
                            return Ok(acc ^ v);
                        }
                        ctx.continue_as_new(&Job::Looper {
                            left: left - 1,
                            acc: acc ^ v,
                        })
                        .await
                    }
                    Job::Patchy { n } => {
                        let a = call(&ctx, &svc, "a", n, 0).await?;
                        ctx.sleep("gap", Duration::from_millis(5)).await?;
                        if deployed2.load(Ordering::Relaxed) && ctx.patched("fix")? {
                            call(&ctx, &svc, "new", a, 0).await
                        } else {
                            call(&ctx, &svc, "old", a, 0).await
                        }
                    }
                    Job::Export => {
                        let opts = StepOptions::default()
                            .retry(RetryPolicy::exponential(
                                5,
                                Duration::from_millis(1),
                                Duration::from_millis(2),
                            ))
                            .heartbeat_timeout(Duration::from_millis(20));
                        let (c, s2) = (ctx.clone(), svc.clone());
                        let pages: u32 = ctx
                            .step("export", move || {
                                let (c, svc) = (c.clone(), s2.clone());
                                async move {
                                    let key = c.key();
                                    let start: u32 = c
                                        .progress::<u32>()
                                        .map_err(|e| e.to_string())?
                                        .unwrap_or(0);
                                    svc.lock()
                                        .map_err(|_| "poisoned".to_string())?
                                        .resume(&key, start);
                                    for p in start..8 {
                                        let stall = p == 3
                                            && svc
                                                .lock()
                                                .map_err(|_| "poisoned".to_string())?
                                                .stall_once(&key);
                                        sim_sleep(if stall { 40 } else { 2 }).await;
                                        svc.lock()
                                            .map_err(|_| "poisoned".to_string())?
                                            .report_progress(&key, p);
                                        c.heartbeat(&(p + 1)).map_err(|e| e.to_string())?;
                                    }
                                    Ok::<u32, String>(8)
                                }
                            })
                            .options(opts)
                            .await?;
                        Ok(u64::from(pages))
                    }
                    Job::Approval => {
                        let (s2, id) = (svc.clone(), ctx.workflow_id().to_owned());
                        let v: u64 = ctx
                            .await_completion("approval", move |c: fusee::Completion| {
                                let token = c.token;
                                let (svc, id) = (s2.clone(), id.clone());
                                async move {
                                    svc.lock()
                                        .map_err(|_| "poisoned".to_string())?
                                        .issue(&id, &token)
                                }
                            })
                            .options(StepOptions::default().retry(RetryPolicy::exponential(
                                3,
                                Duration::from_millis(1),
                                Duration::from_millis(2),
                            )))
                            .await?;
                        call(&ctx, &svc, "after", v, 0).await
                    }
                    Job::Parent { kids } => {
                        for k in 0..kids {
                            let close = if k % 2 == 0 {
                                ParentClose::Terminate
                            } else {
                                ParentClose::Abandon
                            };
                            ctx.start_child(
                                "kid",
                                &format!("{}/kid{k}", ctx.workflow_id()),
                                &u64::from(k),
                            )
                            .options(ChildOptions {
                                close,
                                timeout: None,
                            })
                            .await?;
                        }
                        let v = call(&ctx, &svc, "parent", 1, 0).await?;
                        let _ = ctx
                            .wait_for::<u64>("done")
                            .timeout(Duration::from_millis(30))
                            .await?;
                        Ok(v)
                    }
                    Job::Reader { n } => {
                        // Each read's input comes from the read before it,
                        // which may not be durable yet. `store` has effects:
                        // its gate waits for every read before it.
                        let a = read(&ctx, &svc, "fetch", n, 0).await?;
                        let b = read(&ctx, &svc, "enrich", a, 1).await?;
                        let c = call(&ctx, &svc, "store", b, 1).await?;
                        let d = read(&ctx, &svc, "verify", c, 0).await?;
                        Ok(d)
                    }
                    Job::Talker { n } => {
                        let progress = ctx.stream("progress")?;
                        progress.append(&TALKER_PROGRESS[0])?;
                        let (c, s2) = (ctx.clone(), svc.clone());
                        let opts = StepOptions::default().retry(RetryPolicy::exponential(
                            4,
                            Duration::from_millis(1),
                            Duration::from_millis(2),
                        ));
                        let tokens: Vec<u64> = ctx
                            .step("talk", move || {
                                let (c, svc) = (c.clone(), s2.clone());
                                async move {
                                    let key = c.key();
                                    let out = c.stream("tokens").map_err(|e| e.to_string())?;
                                    // Continue after what was recorded.
                                    let mut toks: Vec<u64> =
                                        out.recorded().map_err(|e| e.to_string())?;
                                    for i in toks.len()..n as usize {
                                        sim_sleep(1).await;
                                        let t = svc
                                            .lock()
                                            .map_err(|_| "poisoned".to_string())?
                                            .token(&key, i)?;
                                        out.append(&t).map_err(|e| e.to_string())?;
                                        toks.push(t);
                                    }
                                    Ok::<_, String>(toks)
                                }
                            })
                            .options(opts)
                            .await?;
                        progress.append(&TALKER_PROGRESS[1])?;
                        ctx.sleep("rest", Duration::from_millis(4)).await?;
                        progress.append(&TALKER_PROGRESS[2])?;
                        progress.close()?;
                        Ok(tokens_hash(&tokens))
                    }
                    Job::Resettable { n } => {
                        let a = call(&ctx, &svc, "a", n, 0).await?;
                        let b = call(&ctx, &svc, "b", a, 0).await?;
                        let c = call(&ctx, &svc, "c", b, 0).await?;
                        let _ = ctx
                            .wait_for::<u64>("go")
                            .timeout(Duration::from_millis(80))
                            .await?;
                        Ok(a ^ b ^ c)
                    }
                }
            }
        }),
    );
    let s = svc.clone();
    r.register(
        "kid",
        1,
        erase(move |ctx: Ctx, k: u64| {
            let svc = s.clone();
            async move {
                let wait = if k.is_multiple_of(2) { 400 } else { 60 };
                let _ = ctx
                    .wait_for::<u64>("never")
                    .timeout(Duration::from_millis(wait))
                    .await?;
                call(&ctx, &svc, "kid", k, 0).await
            }
        }),
    );
    let s = svc.clone();
    r.register(
        "tick",
        1,
        erase(move |ctx: Ctx, slow: bool| {
            let svc = s.clone();
            async move {
                let (id, run) = (ctx.workflow_id().to_owned(), ctx.run_id());
                let v: u64 = ctx
                    .step("tick", || {
                        let svc = svc.clone();
                        let id = id.clone();
                        async move {
                            svc.lock()
                                .map_err(|_| "poisoned".to_string())?
                                .tick(&id, run);
                            Ok::<u64, String>(run)
                        }
                    })
                    .await?;
                if slow {
                    ctx.sleep("work", Duration::from_millis(35)).await?;
                }
                Ok(v)
            }
        }),
    );
    let s = svc.clone();
    r.register(
        "ship",
        1,
        erase(move |ctx: Ctx, x: u64| {
            let svc = s.clone();
            async move { call(&ctx, &svc, "ship", x, 1).await }
        }),
    );
    // What debounce and batch windows start: returns its input.
    r.register(
        "echo",
        1,
        erase(move |ctx: Ctx, v: serde_json::Value| async move {
            ctx.step("echo", || {
                let v = v.clone();
                async move { Ok::<_, Error>(v) }
            })
            .await
        }),
    );
}

// ---------------------------------------------------------------------------
// The client and checkers.

#[derive(Clone, Debug)]
enum Action {
    Start {
        id: String,
        job: Job,
    },
    /// A start of a user scenario's workflow type.
    StartAs {
        id: String,
        wtype: String,
        input: Vec<u8>,
    },
    Signal {
        id: String,
        name: String,
        payload: Vec<u8>,
        dedup: u64,
    },
    Cancel {
        id: String,
    },
    Terminate {
        id: String,
    },
    Update {
        id: String,
        update_id: String,
        value: u64,
    },
    Query {
        id: String,
    },
    Reset {
        id: String,
    },
    Schedule {
        name: String,
        spec: ScheduleSpec,
    },
    Complete {
        wf: String,
        token: String,
    },
    /// A call to a key of an object type ([`fusee::object`]): update-with-
    /// start on the key's run, retried with one call ID until answered.
    ObjectCall {
        id: String,
        wtype: String,
        handler: String,
        call_id: String,
        arg: Vec<u8>,
    },
    /// A debounced start or batched event: update-with-start on the key's
    /// coordinator, retried with one event ID until it has a receipt.
    Enqueue {
        coordinator: String,
        wtype: &'static str,
        /// The workflow type its window starts.
        workflow: String,
        state: Vec<u8>,
        event_id: String,
        event: Vec<u8>,
    },
}

#[derive(Clone, Debug)]
enum WorldEv {
    Client(usize),
    Deliver {
        to: u32,
        from: u32,
        id: u64,
        msg: Message,
        epoch: u64,
    },
    Ack {
        to: u32,
        id: u64,
        epoch: u64,
    },
    Crash,
    /// A supervisor restarts a process whose shard stopped (not a fault).
    Restart,
    /// Reader `n` reads its stream again.
    Read(usize),
}

/// A client reading one stream from start to end, reconnecting at its
/// offset after every crash.
#[derive(Clone, Debug)]
struct SimReader {
    id: String,
    name: String,
    run: Option<u64>,
    next: u64,
    done: bool,
}

/// What readers saw of one stream: every chunk by offset, and its end.
#[derive(Clone, Debug, Default)]
struct SeenStream {
    chunks: Vec<serde_json::Value>,
    end: Option<String>,
}

pub struct World {
    cfg: SimConfig,
    m: MachineRef,
    rng: Rng,
    shards: Vec<Option<ShardRuntime<SimEnv>>>,
    /// The machine's shared log, reopened with the process.
    log: Option<SimLogRef>,
    registry: SharedRegistry,
    svc: ServiceRef,
    /// New code (a patch) is deployed at the first crash.
    deployed: Arc<AtomicBool>,
    limits: Arc<LocalLimits>,
    max_permits: u32,
    codec: Option<Arc<dyn Codec>>,
    /// Replies per update ID (every call must get the same result).
    update_replies: BTreeMap<String, (Status, Vec<u8>)>,
    /// The latest query answer per workflow (must never go back).
    query_answers: BTreeMap<String, u64>,
    /// Every query answer per workflow.
    all_answers: BTreeMap<String, BTreeSet<u64>>,
    /// Completions acknowledged per workflow.
    completions: BTreeMap<String, Vec<String>>,
    tokens_seen: usize,
    /// Workflows with a reset in flight: their result may change.
    resetting: BTreeSet<String>,
    parents: Vec<(String, u32)>,
    events: BTreeMap<(u64, u64), WorldEv>,
    ev_seq: u64,
    actions: Vec<Action>,
    /// Action index -> acknowledged.
    acked: Vec<bool>,
    /// Debounced or batched event ID -> the run its receipt named.
    receipts: BTreeMap<String, String>,
    /// Object call ID -> its answer, when the client first sent it, and
    /// when the first answer came.
    calls: BTreeMap<String, crate::scenario::CallReply>,
    call_sent: BTreeMap<String, u64>,
    tokens: BTreeMap<u64, usize>,
    next_token: u64,
    results: BTreeMap<String, (Status, Vec<u8>)>,
    faults: bool,
    report: Report,
    trace: Vec<String>,
    /// A user scenario's requests (taken by `plan`) and checks.
    user_actions: Vec<(u64, UserAction)>,
    checks: Vec<Check>,
    /// Stream readers, their reads in flight, and what they saw by
    /// `(workflow ID, run, stream)`.
    readers: Vec<SimReader>,
    read_tokens: BTreeMap<u64, usize>,
    seen: BTreeMap<(String, u64, String), SeenStream>,
}

impl World {
    pub fn new(cfg: SimConfig) -> Self {
        let mut rng = Rng::new(cfg.seed);
        let m = Rc::new(RefCell::new(Machine {
            disk: Default::default(),
            now: 1_000_000,
            rng: rng.fork(),
            io: BTreeMap::new(),
            io_seq: 0,
            epoch: 0,
            flush_fail_per_mille: cfg.flush_fail_per_mille,
            max_io_ms: cfg.max_io_ms,
            log: Vec::new(),
        }));
        let svc: ServiceRef = Arc::new(Mutex::new(Service {
            seed: cfg.seed,
            ..Default::default()
        }));
        let registry: SharedRegistry = Arc::new(RwLock::new(Default::default()));
        let deployed = Arc::new(AtomicBool::new(false));
        let mut setup = None;
        match &cfg.user {
            Some(user) => {
                let mut s = Setup::new(cfg.seed, registry.clone());
                (user.0)(&mut s);
                setup = Some(s);
            }
            None => register(&registry, &svc, deployed.clone()),
        }
        if let Ok(mut r) = registry.write() {
            fusee::schedule::register_builtins(&mut r);
        }
        sim_timers_clear();
        NOW.with(|n| n.set(1_000_000));
        let mut w = Self {
            cfg: cfg.clone(),
            m,
            rng,
            shards: Vec::new(),
            log: None,
            registry,
            svc,
            deployed,
            limits: Arc::new(LocalLimits::new()),
            max_permits: 0,
            codec: if cfg.seed % 3 == 1 {
                Some(Arc::new(SimCodec))
            } else {
                None
            },
            update_replies: BTreeMap::new(),
            query_answers: BTreeMap::new(),
            all_answers: BTreeMap::new(),
            completions: BTreeMap::new(),
            tokens_seen: 0,
            resetting: BTreeSet::new(),
            parents: Vec::new(),
            events: BTreeMap::new(),
            ev_seq: 0,
            actions: Vec::new(),
            acked: Vec::new(),
            receipts: BTreeMap::new(),
            calls: BTreeMap::new(),
            call_sent: BTreeMap::new(),
            tokens: BTreeMap::new(),
            next_token: 1,
            results: BTreeMap::new(),
            faults: true,
            report: Report {
                seed: cfg.seed,
                workflows: cfg.workflows,
                ..Default::default()
            },
            trace: Vec::new(),
            user_actions: Vec::new(),
            checks: Vec::new(),
            readers: Vec::new(),
            read_tokens: BTreeMap::new(),
            seen: BTreeMap::new(),
        };
        if let Some(s) = setup {
            w.report.workflows = s
                .actions
                .iter()
                .filter(|a| matches!(a.1, UserAction::Start { .. }))
                .count() as u32;
            w.user_actions = s.actions;
            w.checks = s.checks;
            w.plan_user();
        } else {
            w.plan();
        }
        w
    }

    /// A user scenario's requests, then crashes as for the built-in
    /// workload.
    fn plan_user(&mut self) {
        let start = self.now();
        for (at, a) in std::mem::take(&mut self.user_actions) {
            let action = match a {
                UserAction::Start { id, wtype, input } => Action::StartAs { id, wtype, input },
                UserAction::Signal { id, name, payload } => Action::Signal {
                    id,
                    name,
                    payload,
                    dedup: 1 + self.rng.next() % (1 << 40),
                },
                UserAction::Cancel { id } => Action::Cancel { id },
                UserAction::Terminate { id } => Action::Terminate { id },
                UserAction::ReadStream { id, name } => {
                    let n = self.readers.len();
                    self.readers.push(SimReader {
                        id,
                        name,
                        run: None,
                        next: 0,
                        done: false,
                    });
                    self.at(start + at, WorldEv::Read(n));
                    continue;
                }
                UserAction::Call {
                    wtype,
                    key,
                    handler,
                    call_id,
                    arg,
                } => Action::ObjectCall {
                    id: fusee::object::object_id(&wtype, &key),
                    wtype,
                    handler,
                    call_id,
                    arg,
                },
                UserAction::Enqueue {
                    workflow,
                    key,
                    event_id,
                    input,
                    rule,
                } => {
                    // A third of the seeds make coordinators continue as new
                    // every few events, carrying windows and event IDs.
                    let continue_at = [0, 12, 30][(self.cfg.seed % 3) as usize];
                    enqueue_action(&workflow, &key, &event_id, input, rule, continue_at)
                }
            };
            let ix = self.actions.len();
            self.actions.push(action);
            self.at(start + at, WorldEv::Client(ix));
        }
        self.acked = vec![false; self.actions.len()];
        self.plan_crashes(start);
    }

    fn plan_crashes(&mut self, start: u64) {
        if self.cfg.crash_every_ms > 0 {
            let mut t = start;
            loop {
                t += 1 + self.rng.below(self.cfg.crash_every_ms * 2);
                if t > start + self.cfg.duration_ms {
                    break;
                }
                self.at(t, WorldEv::Crash);
            }
        }
    }

    fn now(&self) -> u64 {
        self.m.borrow().now
    }

    fn log(&mut self, s: String) {
        let t = self.now();
        self.trace.push(format!("[{t}] {s}"));
        if self.trace.len() > 400 {
            self.trace.drain(..100);
        }
    }

    fn at(&mut self, t: u64, ev: WorldEv) {
        let s = self.ev_seq;
        self.ev_seq += 1;
        self.events.insert((t, s), ev);
    }

    fn plan(&mut self) {
        let start = self.now();
        let n = self.cfg.workflows;
        let span = self.cfg.duration_ms / 2;
        let mut orders = Vec::new();
        for i in 0..n {
            let id = format!("wf-{i}");
            let job = match self.rng.below(20) {
                0..=7 | 18 => {
                    orders.push(id.clone());
                    Job::Order { n: u64::from(i) }
                }
                19 => Job::Reader { n: u64::from(i) },
                8..=9 => Job::Fanout {
                    n: 2 + self.rng.below(5) as u32,
                },
                10 => Job::Pinger {
                    target: orders.last().cloned().unwrap_or_else(|| "nobody".into()),
                },
                11 => Job::Counter,
                12 => Job::Looper {
                    left: 1 + self.rng.below(4) as u32,
                    acc: u64::from(i),
                },
                13 => Job::Patchy { n: u64::from(i) },
                14 => Job::Export,
                15 => Job::Approval,
                16 => {
                    let kids = 1 + self.rng.below(4) as u32;
                    self.parents.push((id.clone(), kids));
                    Job::Parent { kids }
                }
                _ => Job::Resettable { n: u64::from(i) },
            };
            let t = start + self.rng.below(span);
            let ix = self.actions.len();
            self.actions.push(Action::Start {
                id: id.clone(),
                job: job.clone(),
            });
            self.at(t, WorldEv::Client(ix));
            match &job {
                Job::Counter => {
                    for u in 0..1 + self.rng.below(4) {
                        let ix = self.actions.len();
                        self.actions.push(Action::Update {
                            id: id.clone(),
                            update_id: format!("{id}/u{u}"),
                            value: self.rng.below(1200),
                        });
                        let t2 = t + 1 + self.rng.below(120);
                        self.at(t2, WorldEv::Client(ix));
                        // A retried call with the same update ID.
                        if self.rng.chance(300) {
                            let t3 = t2 + self.rng.below(60);
                            self.at(t3, WorldEv::Client(ix));
                        }
                        // A query while the update's handler may be running.
                        let ix = self.actions.len();
                        self.actions.push(Action::Query { id: id.clone() });
                        let t4 = t2 + 1 + self.rng.below(5);
                        self.at(t4, WorldEv::Client(ix));
                    }
                    for _ in 0..2 + self.rng.below(6) {
                        let ix = self.actions.len();
                        self.actions.push(Action::Query { id: id.clone() });
                        let t2 = t + 1 + self.rng.below(150);
                        self.at(t2, WorldEv::Client(ix));
                    }
                }
                Job::Parent { .. } if self.rng.chance(400) => {
                    let ix = self.actions.len();
                    self.actions.push(Action::Terminate { id: id.clone() });
                    let t2 = t + self.rng.below(40);
                    self.at(t2, WorldEv::Client(ix));
                }
                Job::Resettable { .. } if self.rng.chance(600) => {
                    let ix = self.actions.len();
                    self.actions.push(Action::Reset { id: id.clone() });
                    let t2 = t + self.rng.below(80);
                    self.at(t2, WorldEv::Client(ix));
                }
                _ => {}
            }
            if matches!(job, Job::Order { .. }) {
                if self.rng.chance(500) {
                    let ix = self.actions.len();
                    let dedup = 1 + self.rng.next() % (1 << 40);
                    let value = self.rng.below(1000);
                    self.actions.push(Action::Signal {
                        id: id.clone(),
                        name: "approve".into(),
                        payload: serde_json::to_vec(&value).unwrap_or_default(),
                        dedup,
                    });
                    let t2 = t + 1 + self.rng.below(80);
                    self.at(t2, WorldEv::Client(ix));
                }
                if self.rng.chance(120) {
                    let ix = self.actions.len();
                    self.actions.push(Action::Cancel { id: id.clone() });
                    let t3 = t + self.rng.below(40);
                    self.at(t3, WorldEv::Client(ix));
                }
            }
        }
        if self.cfg.seed.is_multiple_of(2) {
            let overlap = [
                Overlap::Skip,
                Overlap::BufferOne,
                Overlap::AllowAll,
                Overlap::CancelOther,
            ][self.rng.below(4) as usize];
            let spec = ScheduleSpec {
                every_ms: Some([23, 41][self.rng.below(2) as usize]),
                overlap,
                jitter_ms: self.rng.below(5),
                workflow: "tick".into(),
                input: serde_json::json!(self.rng.chance(300)),
                ..Default::default()
            };
            let ix = self.actions.len();
            self.actions.push(Action::Schedule {
                name: "tick".into(),
                spec,
            });
            let t = start + self.rng.below(span);
            self.at(t, WorldEv::Client(ix));
            // The schedule is deleted when the client is done.
            let ix = self.actions.len();
            self.actions.push(Action::Terminate {
                id: schedule_id("tick"),
            });
            self.at(start + self.cfg.duration_ms, WorldEv::Client(ix));
        }
        if self.cfg.seed % 4 == 3 {
            self.plan_queues(start, span);
        }
        self.plan_streams(start, span);
        self.acked = vec![false; self.actions.len()];
        self.plan_crashes(start);
    }

    /// Model calls that stream their tokens, and readers of their streams
    /// that start before, during and after them. A random stream of its
    /// own, so the rest of the seed's plan stays as it was.
    fn plan_streams(&mut self, start: u64, span: u64) {
        let mut r = Rng::new(self.cfg.seed ^ 0x5717_ea35_c0de_0001);
        for k in 0..1 + r.below(3) {
            let id = format!("talk-{k}");
            let t = start + r.below(span.max(1));
            let ix = self.actions.len();
            self.actions.push(Action::Start {
                id: id.clone(),
                job: Job::Talker {
                    n: 3 + r.below(10) as u32,
                },
            });
            self.at(t, WorldEv::Client(ix));
            for name in ["tokens", "progress", "tokens"] {
                if name == "tokens" && r.chance(300) {
                    continue;
                }
                let n = self.readers.len();
                self.readers.push(SimReader {
                    id: id.clone(),
                    name: name.into(),
                    run: None,
                    next: 0,
                    done: false,
                });
                self.at(t.saturating_sub(5) + r.below(80), WorldEv::Read(n));
            }
        }
    }

    /// Debounced starts and batched events of `echo`, in bursts, each with
    /// its own event ID (the input is the ID, so checks can find it).
    fn plan_queues(&mut self, start: u64, span: u64) {
        let continue_at = [0, 12, 30][(self.cfg.seed / 4 % 3) as usize];
        for k in 0..4u64 {
            let rule = if k % 2 == 1 {
                Rule::Batch(fusee::Batch::new(
                    2 + self.rng.below(3) as u32,
                    Duration::from_millis(20 + self.rng.below(60)),
                ))
            } else {
                Rule::Debounce(
                    fusee::Debounce::new(Duration::from_millis(5 + self.rng.below(40)))
                        .max_wait(Duration::from_millis(30 + self.rng.below(80))),
                )
            };
            let mut t = start + self.rng.below(span);
            for i in 0..3 + self.rng.below(6) {
                t += if self.rng.chance(300) {
                    40 + self.rng.below(100)
                } else {
                    self.rng.below(15)
                };
                let id = format!("q{k}/e{i}");
                let ix = self.actions.len();
                self.actions.push(enqueue_action(
                    "echo",
                    &format!("key-{k}"),
                    &id,
                    serde_json::json!(id),
                    rule.clone(),
                    continue_at,
                ));
                self.at(t, WorldEv::Client(ix));
            }
        }
    }

    /// The built-in workload's windows: a batch run's input holds each of
    /// its events once, and no event is in two runs; a debounced run's
    /// input is one of its own window's starts.
    fn check_queue_contents(&self) -> Result<(), Failure> {
        for a in &self.actions {
            let Action::Enqueue {
                wtype, event_id, ..
            } = a
            else {
                continue;
            };
            let Some(run) = self.receipts.get(event_id) else {
                continue;
            };
            let got: serde_json::Value = match self.results.get(run) {
                Some((Status::Ok, p)) => serde_json::from_slice(p).unwrap_or_default(),
                other => {
                    return Err(self.fail(format!(
                        "window run {run} of event {event_id} ended with {other:?}"
                    )));
                }
            };
            // The events whose receipts name this run.
            let mates: Vec<&String> = self
                .receipts
                .iter()
                .filter(|(_, r)| *r == run)
                .map(|(e, _)| e)
                .collect();
            if *wtype == BATCH_TYPE {
                // Each event once in its own run, and nothing else there:
                // so no event is in two runs.
                let events = got.as_array().cloned().unwrap_or_default();
                let n = events
                    .iter()
                    .filter(|e| e.as_str() == Some(event_id))
                    .count();
                if n != 1 || events.len() != mates.len() {
                    return Err(self.fail(format!(
                        "batch run {run} holds event {event_id} {n} times and {} events \
                         in all, but {} receipts name it: {got}",
                        events.len(),
                        mates.len()
                    )));
                }
            } else if !mates.iter().any(|e| got.as_str() == Some(e.as_str())) {
                return Err(self.fail(format!(
                    "debounced run {run} took {got}, not a start of its window"
                )));
            }
        }
        Ok(())
    }

    fn runtime_config(&self, shard: u32) -> RuntimeConfig {
        RuntimeConfig {
            shard,
            shards: self.cfg.shards,
            retention_ms: 3_600_000,
            park_after_ms: self.cfg.park_after_ms,
            // A third of the seeds park every suspended run at each sweep
            // through the memory budget instead of waiting `park_after_ms`.
            memory_budget: if self.cfg.seed % 3 == 1 { 1 } else { 0 },
            checkpoint_interval_ms: self.cfg.checkpoint_interval_ms,
            max_loaded: 100_000,
            segment_bytes: self.cfg.segment_bytes,
            inline_flush: false,
            inline_max_us: 0,
            keep_history: self.cfg.seed.is_multiple_of(2),
            // From bits of the seed the other knobs do not use: index bases
            // written a few runs at a time between checkpoints, and often,
            // so that they interleave with parks, wakes and crashes.
            index_chunk_records: [1, 3, 64, fusee::runtime::INDEX_CHUNK_RECORDS]
                [(self.cfg.seed / 72 % 4) as usize],
            index_rebase_deltas: [1, 2, 8, 64][(self.cfg.seed / 288 % 4) as usize],
            sabotage: self.cfg.sabotage,
            ..RuntimeConfig::default()
        }
    }

    fn open_all(&mut self) -> Result<(), String> {
        self.shards.clear();
        self.log = None;
        if self.cfg.shared_log {
            self.log = Some(
                SimLog::open(
                    &self.m,
                    self.cfg.shards,
                    self.cfg.segment_bytes,
                    // From bits of the seed the other knobs do not use.
                    1 + (self.cfg.seed / 2 % 3) as usize,
                    (self.cfg.seed / 6 % 3) as usize,
                    crate::env::Offload {
                        threshold: self.cfg.offload_bytes,
                        early: self.cfg.early_blobs,
                    },
                )
                .map_err(|e| format!("the shared log failed to recover: {e}"))?,
            );
        }
        for shard in 0..self.cfg.shards {
            let env = match &self.log {
                Some(log) => SimEnv::with_log(shard, self.m.clone(), log.clone()),
                None => SimEnv::new(shard, self.m.clone()),
            };
            let notify: fusee::env::Notify = Arc::new(|| {});
            let services = Services {
                codec: self.codec.clone(),
                limits: self.limits.clone(),
            };
            self.limits.define(
                "svc",
                LimitSpec {
                    concurrent: 2,
                    per_second: 0.0,
                },
            );
            match ShardRuntime::open_with(
                env,
                self.runtime_config(shard),
                self.registry.clone(),
                false,
                services,
                notify,
            ) {
                Ok(rt) => self.shards.push(Some(rt)),
                Err(e) => return Err(format!("shard {shard} failed to recover: {e}")),
            }
        }
        Ok(())
    }

    fn shard_rt(&self, shard: u32) -> Option<&ShardRuntime<SimEnv>> {
        self.shards.get(shard as usize).and_then(|s| s.as_ref())
    }

    fn fail(&self, message: String) -> Failure {
        let mut trace = self.trace.clone();
        trace.extend(self.m.borrow().log.iter().rev().take(20).rev().cloned());
        Failure {
            seed: self.cfg.seed,
            message,
            trace,
            scenario: self.cfg.user.is_some(),
        }
    }

    fn do_action(&mut self, ix: usize) {
        if self.acked[ix] {
            return;
        }
        let token = self.next_token;
        self.next_token += 1;
        let action = self.actions[ix].clone();
        let shards = self.cfg.shards;
        let r = match &action {
            Action::Start { id, job } => {
                let shard = shard_of(id, shards);
                // A client that retries uses the existing run.
                let mut req = StartRequest::new(
                    id,
                    "job",
                    Arc::from(serde_json::to_vec(job).unwrap_or_default()),
                    IdPolicy::ATTACH,
                );
                req.token = Some(token);
                self.shard_rt(shard).map(|rt| rt.start(req).map(|_| ()))
            }
            Action::StartAs { id, wtype, input } => {
                let shard = shard_of(id, shards);
                let mut req = StartRequest::new(id, wtype, Arc::from(&input[..]), IdPolicy::ATTACH);
                req.token = Some(token);
                self.shard_rt(shard).map(|rt| rt.start(req).map(|_| ()))
            }
            Action::Schedule { name, spec } => {
                let id = schedule_id(name);
                let state = ScheduleState {
                    name: name.clone(),
                    spec: spec.clone(),
                    ..Default::default()
                };
                let mut req = StartRequest::new(
                    &id,
                    SCHEDULE_TYPE,
                    Arc::from(serde_json::to_vec(&state).unwrap_or_default()),
                    IdPolicy::ATTACH,
                );
                req.token = Some(token);
                self.shard_rt(shard_of(&id, shards))
                    .map(|rt| rt.start(req).map(|_| ()))
            }
            Action::Terminate { id } => self
                .shard_rt(shard_of(id, shards))
                .map(|rt| rt.terminate(id, "client terminated", Some(token))),
            Action::Update {
                id,
                update_id,
                value,
            } => {
                let payload: Payload = Arc::from(serde_json::to_vec(value).unwrap_or_default());
                self.shard_rt(shard_of(id, shards))
                    .map(|rt| rt.update(id, "add", update_id, payload, Some(token)))
            }
            Action::Query { id } => self
                .shard_rt(shard_of(id, shards))
                .map(|rt| rt.query(id, "total", Arc::from(&b"null"[..]), token)),
            Action::Reset { id } => {
                self.resetting.insert(id.clone());
                self.results.remove(id);
                self.shard_rt(shard_of(id, shards))
                    .map(|rt| rt.reset(id, "b", true, None, Some(token)).map(|_| ()))
            }
            Action::Complete { wf, token: t } => {
                let payload: Payload =
                    Arc::from(serde_json::to_vec(&(wf.len() as u64 * 7)).unwrap_or_default());
                self.shard_rt(shard_of(wf, shards))
                    .map(|rt| rt.complete(t, Status::Ok, payload, Some(token)))
            }
            Action::Signal {
                id,
                name,
                payload,
                dedup,
            } => {
                // Only signal workflows whose start was acknowledged.
                let started = self
                    .actions
                    .iter()
                    .zip(&self.acked)
                    .any(|(a, ok)| {
                        matches!(a, Action::Start { id: s, .. } | Action::StartAs { id: s, .. } if s == id)
                            && *ok
                    });
                if !started {
                    let t = self.now() + 5;
                    self.at(t, WorldEv::Client(ix));
                    return;
                }
                let shard = shard_of(id, shards);
                let payload: Payload = Arc::from(&payload[..]);
                self.shard_rt(shard)
                    .map(|rt| rt.signal(id, name, payload, *dedup, Some(token)))
            }
            Action::Cancel { id } => {
                let shard = shard_of(id, shards);
                self.shard_rt(shard)
                    .map(|rt| rt.cancel(id, "client cancelled", Some(token)))
            }
            Action::ObjectCall {
                id,
                wtype,
                handler,
                call_id,
                arg,
            } => {
                // As `ObjectRef::call_with_id` sends it: update-with-start.
                let now = self.now();
                self.call_sent.entry(call_id.clone()).or_insert(now);
                let mut req = StartRequest::new(
                    id,
                    wtype,
                    Arc::from(&b"null"[..]),
                    IdPolicy {
                        running: OnRunning::UseExisting,
                        finished: OnFinished::Allow,
                    },
                );
                req.update = Some(UpdateRequest {
                    name: handler.clone(),
                    id: call_id.clone(),
                    payload: Arc::from(&arg[..]),
                    token: Some(token),
                });
                let r = self
                    .shard_rt(shard_of(id, shards))
                    .map(|rt| rt.start(req).map(|_| ()));
                self.log(format!("call {call_id} to {id}.{handler}: {r:?}"));
                if !matches!(r, Some(Ok(()))) {
                    // Transient (busy, recovering): send it again.
                    let t = self.now() + 10;
                    self.at(t, WorldEv::Client(ix));
                    return;
                }
                r
            }
            Action::Enqueue {
                coordinator,
                wtype,
                state,
                event_id,
                event,
                ..
            } => {
                // As `Engine::debounce_raw` sends it: update-with-start.
                let mut req = StartRequest::new(
                    coordinator,
                    wtype,
                    Arc::from(&state[..]),
                    IdPolicy {
                        running: OnRunning::UseExisting,
                        finished: OnFinished::Allow,
                    },
                );
                req.update = Some(UpdateRequest {
                    name: EVENT.into(),
                    id: event_id.clone(),
                    payload: Arc::from(&event[..]),
                    token: Some(token),
                });
                let r = self
                    .shard_rt(shard_of(coordinator, shards))
                    .map(|rt| rt.start(req).map(|_| ()));
                self.log(format!("event {event_id} to {coordinator}: {r:?}"));
                if !matches!(r, Some(Ok(()))) {
                    // Every refusal here is transient (busy, recovering).
                    let t = self.now() + 10;
                    self.at(t, WorldEv::Client(ix));
                    return;
                }
                r
            }
        };
        if let Action::Update { id, update_id, .. } = &action {
            self.log(format!("update {update_id} to {id}: {r:?} (token {token})"));
        }
        match r {
            Some(Ok(())) => {
                self.tokens.insert(token, ix);
            }
            Some(Err(Error::NotFound(_) | Error::Failed(_) | Error::Rejected(_))) => {
                // Signal or cancel for a run that finished or was never
                // started: there is nothing to deliver.
                self.acked[ix] = true;
                if let Action::Reset { id } = &action {
                    self.resetting.remove(id);
                }
            }
            _ => {
                let t = self.now() + 10;
                self.at(t, WorldEv::Client(ix));
            }
        }
    }

    fn on_output(&mut self, shard: u32, out: Output) -> Result<(), Failure> {
        match out {
            Output::Ack(t) | Output::Reject(t) => {
                if let Some(ix) = self.tokens.remove(&t) {
                    self.acked[ix] = true;
                    match self.actions[ix].clone() {
                        Action::Reset { id } => {
                            let found = self
                                .shard_rt(shard_of(&id, self.cfg.shards))
                                .and_then(|rt| rt.find(&id));
                            self.log(format!(
                                "reset {id} acknowledged; the ID now names run {found:?}"
                            ));
                            self.report.resets += 1;
                            self.resetting.remove(&id);
                            self.results.remove(&id);
                        }
                        Action::Complete { wf, token } => {
                            let accepted = self.completions.entry(wf.clone()).or_default();
                            if !accepted.contains(&token) {
                                accepted.push(token);
                            }
                            if accepted.len() > 1 {
                                let all = accepted.clone();
                                return Err(self.fail(format!(
                                    "two completions were accepted for {wf}: {all:?}"
                                )));
                            }
                        }
                        _ => {}
                    }
                }
            }
            Output::Refuse { token, message } => {
                if let Some(ix) = self.tokens.remove(&token) {
                    if let Action::ObjectCall { call_id, .. } = &self.actions[ix] {
                        // The key could not take it now (its run was ending
                        // or not running code yet): the client tries again.
                        self.log(format!(
                            "call {call_id} refused: {}; again",
                            String::from_utf8_lossy(&message)
                        ));
                        let t = self.now() + 10;
                        self.at(t, WorldEv::Client(ix));
                        return Ok(());
                    }
                    if let Action::Enqueue { event_id, .. } = &self.actions[ix] {
                        return Err(self.fail(format!(
                            "event {event_id} was refused: {}",
                            String::from_utf8_lossy(&message)
                        )));
                    }
                    self.acked[ix] = true;
                }
            }
            Output::Reply {
                token,
                status,
                payload,
            } if self.read_tokens.contains_key(&token) => {
                if let Some(n) = self.read_tokens.remove(&token) {
                    self.on_read(n, status, &payload)?;
                }
            }
            Output::Reply {
                token,
                status,
                payload,
            } => {
                if let Some(ix) = self.tokens.remove(&token) {
                    if let Action::Enqueue { event_id, .. } = self.actions[ix].clone() {
                        return self.on_receipt(ix, event_id, status, &payload);
                    }
                    if let Action::ObjectCall { call_id, .. } = self.actions[ix].clone() {
                        return self.on_call_reply(ix, call_id, status, &payload);
                    }
                    self.acked[ix] = true;
                    match self.actions[ix].clone() {
                        Action::Update { update_id, .. } => {
                            let got = (status, payload.to_vec());
                            match self.update_replies.get(&update_id) {
                                Some(prev) if *prev != got => {
                                    return Err(self.fail(format!(
                                        "update {update_id} answered {:?} {:?} and later {:?} {:?}",
                                        prev.0,
                                        String::from_utf8_lossy(&prev.1),
                                        got.0,
                                        String::from_utf8_lossy(&got.1)
                                    )));
                                }
                                Some(_) => {}
                                None => {
                                    self.update_replies.insert(update_id, got);
                                }
                            }
                        }
                        Action::Query { id } => {
                            let v: u64 = serde_json::from_slice(&payload).unwrap_or(0);
                            let prev = self.query_answers.get(&id).copied().unwrap_or(0);
                            if v < prev {
                                return Err(self.fail(format!(
                                    "query of {id} answered {v} after answering {prev}: an answer \
                                     left before the state it read was durable"
                                )));
                            }
                            self.all_answers.entry(id.clone()).or_default().insert(v);
                            self.query_answers.insert(id, v);
                        }
                        _ => {}
                    }
                }
            }
            Output::Result { status, .. } if status.is_handover() => {}
            Output::Result {
                run,
                status,
                payload,
            } => {
                // Recovery regenerates the outputs of older runs too; only
                // the ID's current run speaks for the ID.
                let id = self
                    .shard_rt(shard)
                    .and_then(|rt| rt.summary(run).filter(|s| rt.find(&s.id) == Some(run)))
                    .map(|s| s.id);
                if let Some(id) = id {
                    self.log(format!("released result of {id} run {run}: {status:?}"));
                    self.record_result(&id, status, payload.to_vec())?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// A key answered a call. Every answer to one call ID must be the same:
    /// a call runs once, however often the client sent it.
    fn on_call_reply(
        &mut self,
        ix: usize,
        call_id: String,
        status: Status,
        payload: &[u8],
    ) -> Result<(), Failure> {
        let now = self.now();
        let status = status.as_str().to_owned();
        if let Some(prev) = self.calls.get(&call_id) {
            if prev.status != status || prev.payload != payload {
                return Err(self.fail(format!(
                    "call {call_id} was answered {} {:?} and later {status} {:?}: it ran twice",
                    prev.status,
                    String::from_utf8_lossy(&prev.payload),
                    String::from_utf8_lossy(payload)
                )));
            }
        } else {
            let sent_ms = self.call_sent.get(&call_id).copied().unwrap_or(now);
            self.log(format!(
                "call {call_id} answered {status} {}",
                String::from_utf8_lossy(payload)
            ));
            self.calls.insert(
                call_id,
                crate::scenario::CallReply {
                    status,
                    payload: payload.to_vec(),
                    sent_ms,
                    answered_ms: now,
                },
            );
        }
        self.acked[ix] = true;
        Ok(())
    }

    /// A coordinator answered an event: a receipt, or "ended first", which
    /// the client answers by sending the event again.
    fn on_receipt(
        &mut self,
        ix: usize,
        event_id: String,
        status: Status,
        payload: &[u8],
    ) -> Result<(), Failure> {
        match status {
            Status::Ok => {
                let r: Receipt = serde_json::from_slice(payload).map_err(|e| {
                    self.fail(format!("event {event_id} got an unreadable receipt: {e}"))
                })?;
                self.log(format!("event {event_id} joined {}", r.id));
                if let Some(prev) = self.receipts.get(&event_id)
                    && *prev != r.id
                {
                    return Err(self.fail(format!(
                        "event {event_id} joined run {prev} and later run {}",
                        r.id
                    )));
                }
                self.receipts.insert(event_id, r.id);
                self.acked[ix] = true;
            }
            Status::Failed if payload == ENDED => {
                self.log(format!("event {event_id}: its coordinator ended; again"));
                let t = self.now() + 1;
                self.at(t, WorldEv::Client(ix));
            }
            s => {
                return Err(self.fail(format!(
                    "event {event_id} failed: {s:?} {}",
                    String::from_utf8_lossy(payload)
                )));
            }
        }
        Ok(())
    }

    fn record_result(&mut self, id: &str, status: Status, payload: Vec<u8>) -> Result<(), Failure> {
        if self.resetting.contains(id) {
            return Ok(());
        }
        match self.results.get(id) {
            Some((s, p)) if *s != status || *p != payload => {
                return Err(self.fail(format!(
                    "workflow {id} reported result {:?} {:?} and later {:?} {:?}",
                    s,
                    String::from_utf8_lossy(p),
                    status,
                    String::from_utf8_lossy(&payload)
                )));
            }
            Some(_) => {}
            None => {
                self.log(format!("result {id} {status:?}"));
                self.results.insert(id.to_owned(), (status, payload));
            }
        }
        Ok(())
    }

    /// Ticks every shard until nothing moves.
    fn settle(&mut self) -> Result<(), Failure> {
        for _ in 0..100_000 {
            let mut progress = false;
            let now = self.now();
            progress |= sim_timers_fire(now);
            for shard in 0..self.cfg.shards {
                let Some(rt) = self.shard_rt(shard) else {
                    continue;
                };
                let tick = rt.tick();
                let released = rt.take_released();
                let outgoing = rt.take_outgoing();
                let acks = rt.take_message_acks();
                let failed = rt.with(|i| i.failed().map(str::to_owned));
                progress |=
                    tick.busy || !released.is_empty() || !outgoing.is_empty() || !acks.is_empty();
                for out in released {
                    self.on_output(shard, out)?;
                }
                let epoch = self.m.borrow().epoch;
                for o in outgoing {
                    let d = self.rng.below(self.cfg.max_msg_ms + 1);
                    self.at(
                        now + d,
                        WorldEv::Deliver {
                            to: o.target,
                            from: shard,
                            id: o.id,
                            msg: o.msg,
                            epoch,
                        },
                    );
                }
                for (to, id) in acks {
                    let d = self.rng.below(self.cfg.max_msg_ms + 1);
                    self.at(now + d, WorldEv::Ack { to, id, epoch });
                }
                if let Some(f) = failed
                    && !self
                        .events
                        .values()
                        .any(|e| matches!(e, WorldEv::Crash | WorldEv::Restart))
                {
                    self.log(format!(
                        "shard {shard} stopped: {f}; restarting the process"
                    ));
                    self.at(now + 1, WorldEv::Restart);
                }
            }
            progress |= self.new_tokens();
            if !progress {
                return Ok(());
            }
        }
        Err(self.fail("the shards never settled".into()))
    }

    /// The client completes every token the approval system handed out,
    /// stale ones included (those must be refused).
    fn new_tokens(&mut self) -> bool {
        let fresh: Vec<(String, String)> = {
            let s = match self.svc.lock() {
                Ok(s) => s,
                Err(_) => return false,
            };
            s.tokens[self.tokens_seen.min(s.tokens.len())..].to_vec()
        };
        if fresh.is_empty() {
            return false;
        }
        self.tokens_seen += fresh.len();
        let now = self.now();
        for (wf, token) in fresh {
            let ix = self.actions.len();
            self.actions.push(Action::Complete { wf, token });
            self.acked.push(false);
            let t = now + self.rng.below(20);
            self.at(t, WorldEv::Client(ix));
        }
        true
    }

    fn next_time(&self) -> Option<u64> {
        let now = self.now();
        let mut t: Option<u64> = None;
        let mut take = |x: Option<u64>| {
            if let Some(x) = x {
                t = Some(t.map_or(x, |y: u64| y.min(x)));
            }
        };
        take(self.events.keys().next().map(|k| k.0));
        take(self.m.borrow().io.keys().next().map(|k| k.0));
        take(sim_timers_next());
        for s in self.shards.iter().flatten() {
            if s.with(|i| i.failed().is_none()) {
                take(s.tick().next_wake_ms);
            }
        }
        t.map(|t| t.max(now))
    }

    fn crash(&mut self) -> Result<(), Failure> {
        self.report.crashes += 1;
        self.report.offloaded += self
            .log
            .as_ref()
            .map_or(0, |l| l.borrow().wal.blob_stats().records);
        self.log("crash: process dies; unflushed writes are lost or torn".into());
        self.check_limits()?;
        self.shards.clear();
        // The restarted process runs new code (a patch) and has fresh
        // limits: permits held by the dead process are gone with it.
        self.deployed.store(true, Ordering::Relaxed);
        self.limits = Arc::new(LocalLimits::new());
        sim_timers_clear();
        {
            let mut m = self.m.borrow_mut();
            let tear = self.cfg.tear;
            let mut rng = m.rng.fork();
            m.disk.crash(&mut rng, tear);
            m.io.clear();
            m.epoch += 1;
        }
        self.events
            .retain(|_, e| !matches!(e, WorldEv::Deliver { .. } | WorldEv::Ack { .. }));
        self.tokens.clear();
        self.read_tokens.clear();
        self.open_all().map_err(|e| self.fail(e))?;
        self.log(format!("recovered {} shards", self.cfg.shards));
        // Readers connect again at their offsets.
        let now = self.now();
        for n in 0..self.readers.len() {
            if !self.readers[n].done {
                self.at(now + 1 + n as u64 % 4, WorldEv::Read(n));
            }
        }
        // Client retries everything not acknowledged.
        let now = self.now();
        for ix in 0..self.actions.len() {
            if !self.acked[ix]
                && self
                    .events
                    .values()
                    .all(|e| !matches!(e, WorldEv::Client(i) if *i == ix))
            {
                let started_before = true;
                if started_before {
                    self.at(now + 1, WorldEv::Client(ix));
                }
            }
        }
        self.check_after_recovery()
    }

    fn check_after_recovery(&mut self) -> Result<(), Failure> {
        let shards = self.cfg.shards;
        for ix in 0..self.actions.len() {
            if !self.acked[ix] {
                continue;
            }
            match self.actions[ix].clone() {
                Action::Start { id, .. } | Action::StartAs { id, .. } => {
                    let shard = shard_of(&id, shards);
                    if self.shard_rt(shard).and_then(|rt| rt.find(&id)).is_none() {
                        return Err(self.fail(format!("acknowledged start of {id} was lost")));
                    }
                }
                Action::Signal { id, dedup, .. } => {
                    let shard = shard_of(&id, shards);
                    let Some(rt) = self.shard_rt(shard) else {
                        continue;
                    };
                    match rt.inspect(&id) {
                        Ok(Some(d)) if d.summary.finished_ms.is_none() => {
                            let found = d.journal.iter().any(
                                |r| matches!(r, Record::Signal { dedup: x, .. } if *x == dedup),
                            );
                            if !found {
                                return Err(self.fail(format!(
                                    "acknowledged signal {dedup} to {id} is missing from its journal: {:?} {:?}",
                                    d.summary, d.journal
                                )));
                            }
                        }
                        Ok(_) => {}
                        Err(e) => return Err(self.fail(format!("inspect {id}: {e}"))),
                    }
                }
                Action::Enqueue {
                    coordinator,
                    event_id,
                    ..
                } => {
                    // Its run has started, or the coordinator that holds
                    // its window (or sends the run's start) still exists.
                    let Some(run) = self.receipts.get(&event_id) else {
                        continue;
                    };
                    let exists = |id: &str| {
                        self.shard_rt(shard_of(id, shards))
                            .and_then(|rt| rt.find(id))
                            .is_some()
                    };
                    if !exists(run) && !exists(&coordinator) {
                        return Err(self.fail(format!(
                            "acknowledged event {event_id} was lost: neither its run {run} nor \
                             its coordinator {coordinator} exists"
                        )));
                    }
                }
                _ => {}
            }
        }
        self.check_answers()?;
        // Results seen before must not change.
        let known: Vec<String> = self.results.keys().cloned().collect();
        for id in known {
            let shard = shard_of(&id, shards);
            let r = self.shard_rt(shard).map(|rt| rt.result(&id));
            match r {
                Some(Ok(ResultState::Done {
                    status,
                    payload,
                    run,
                })) => {
                    self.log(format!("after recovery {id} is run {run}: {status:?}"));
                    self.record_result(&id, status, payload.to_vec())?
                }
                Some(Ok(ResultState::NotFound)) => {
                    return Err(
                        self.fail(format!("finished workflow {id} vanished after recovery"))
                    );
                }
                Some(Ok(ResultState::Running(_))) => {
                    let job = self.actions.iter().find_map(|a| match a {
                        Action::Start { id: x, job } if *x == id => Some(format!("{job:?}")),
                        Action::StartAs { id: x, wtype, .. } if *x == id => Some(wtype.clone()),
                        _ => None,
                    });
                    let detail = self
                        .shard_rt(shard)
                        .and_then(|rt| rt.inspect(&id).ok().flatten());
                    return Err(self.fail(format!(
                        "workflow {id} ({}) had reported a result but runs again after recovery: {detail:?}",
                        job.unwrap_or_default()
                    )));
                }
                Some(Err(e)) => return Err(self.fail(format!("result {id}: {e}"))),
                None => {}
            }
        }
        self.check_service()
    }

    /// Every query answer must be a state the journal recorded: the total
    /// after some update (each handler returns the total it set).
    fn check_answers(&mut self) -> Result<(), Failure> {
        let shards = self.cfg.shards;
        for (id, answers) in self.all_answers.clone() {
            let Some(rt) = self.shard_rt(shard_of(&id, shards)) else {
                continue;
            };
            let Ok(Some(d)) = rt.inspect(&id) else {
                continue;
            };
            let full = d
                .journal
                .iter()
                .any(|r| !matches!(r, Record::Start { .. } | Record::Done { .. }));
            if !full && d.summary.finished_ms.is_some() {
                continue;
            }
            let mut states: BTreeSet<u64> = [0].into();
            for r in &d.journal {
                if let Record::UpdateDone {
                    status: Status::Ok,
                    payload,
                    ..
                } = r
                {
                    let p = self
                        .codec
                        .as_ref()
                        .and_then(|c| c.decode(payload).ok())
                        .unwrap_or_else(|| payload.to_vec());
                    if let Ok(v) = serde_json::from_slice::<u64>(&p) {
                        states.insert(v);
                    }
                }
            }
            if let Some(a) = answers.iter().find(|a| !states.contains(a)) {
                return Err(self.fail(format!(
                    "query of {id} answered {a}, a state its journal never recorded ({states:?}): \
                     the answer left before that state was durable"
                )));
            }
        }
        Ok(())
    }

    fn check_limits(&mut self) -> Result<(), Failure> {
        self.max_permits = self.max_permits.max(self.limits.max_in_use("svc"));
        if self.max_permits > 2 {
            return Err(self.fail(format!(
                "limit 'svc' (2 permits) had {} permits out at once",
                self.max_permits
            )));
        }
        Ok(())
    }

    /// End-of-run checks: no run stopped on a determinism error (replaying
    /// patched, continued and reset runs reproduced their journals), every
    /// accepted update ran once, children followed their close policy.
    fn check_final(&mut self) -> Result<(), Failure> {
        self.check_limits()?;
        self.check_streams()?;
        if self.cfg.user.is_none() {
            self.check_queue_contents()?;
        }
        // Runs a debounce or batch window started: one per window, and only
        // for windows some acknowledged event joined.
        let mut targets = BTreeSet::new();
        let mut direct = BTreeSet::new();
        for a in &self.actions {
            match a {
                Action::Enqueue { workflow, .. } => {
                    targets.insert(workflow.clone());
                }
                Action::StartAs { id, .. } => {
                    direct.insert(id.clone());
                }
                _ => {}
            }
        }
        let windows: BTreeSet<&String> = self.receipts.values().collect();
        for shard in 0..self.cfg.shards {
            let Some(rt) = self.shard_rt(shard) else {
                continue;
            };
            for s in rt.list(1_000_000) {
                if s.status == "stuck" {
                    return Err(self.fail(format!(
                        "{} stopped on a determinism error: {}",
                        s.id,
                        s.error.unwrap_or_default()
                    )));
                }
                if targets.contains(&s.wtype) && !direct.contains(&s.id) && !windows.contains(&s.id)
                {
                    return Err(self.fail(format!(
                        "run {} of {} was started by a window no acknowledged event joined",
                        s.id, s.wtype
                    )));
                }
            }
        }
        let shards = self.cfg.shards;
        let counters: Vec<String> = self
            .actions
            .iter()
            .filter_map(|a| match a {
                Action::Start {
                    id,
                    job: Job::Counter,
                } => Some(id.clone()),
                _ => None,
            })
            .collect();
        for id in &counters {
            // A total only grows, so no answer may exceed the final one.
            if let (Some(answer), Some((Status::Ok, p))) =
                (self.query_answers.get(id), self.results.get(id))
            {
                let last: u64 = serde_json::from_slice(p).unwrap_or(u64::MAX);
                if *answer > last {
                    return Err(self.fail(format!(
                        "query of {id} answered {answer} but its final total is {last}: an \
                         answer reflected state a crash took back"
                    )));
                }
            }
        }
        for id in counters {
            let Some(rt) = self.shard_rt(shard_of(&id, shards)) else {
                continue;
            };
            if let Ok(Some(d)) = rt.inspect(&id) {
                let mut seen = BTreeSet::new();
                for r in &d.journal {
                    if let Record::Update { id: u, .. } = r
                        && !seen.insert(u.to_string())
                    {
                        return Err(self.fail(format!(
                            "update {u} was accepted twice by {id}: {:?}",
                            d.journal
                        )));
                    }
                }
            }
        }
        for (parent, kids) in self.parents.clone() {
            for k in 0..kids {
                let kid = format!("{parent}/kid{k}");
                let Some(rt) = self.shard_rt(shard_of(&kid, shards)) else {
                    continue;
                };
                let Some(s) = rt.find(&kid).and_then(|r| rt.summary(r)) else {
                    continue;
                };
                let ok = match k % 2 {
                    0 => matches!(s.status.as_str(), "terminated" | "ok"),
                    _ => s.status == "ok",
                };
                if !ok {
                    return Err(self.fail(format!(
                        "child {kid} ({}) ended '{}' against its parent close policy",
                        if k % 2 == 0 { "terminate" } else { "abandon" },
                        s.status
                    )));
                }
            }
        }
        Ok(())
    }

    fn check_service(&self) -> Result<(), Failure> {
        let v = self
            .svc
            .lock()
            .map(|s| s.violations.clone())
            .unwrap_or_default();
        if let Some(first) = v.first() {
            return Err(self.fail(first.clone()));
        }
        Ok(())
    }

    fn handle(&mut self, ev: WorldEv) -> Result<(), Failure> {
        let epoch = self.m.borrow().epoch;
        match ev {
            WorldEv::Client(ix) => self.do_action(ix),
            WorldEv::Deliver {
                to,
                from,
                id,
                msg,
                epoch: e,
            } => {
                if e == epoch
                    && let Some(rt) = self.shard_rt(to)
                {
                    let _ = rt.message(msg, Some((from, id)));
                }
            }
            WorldEv::Ack { to, id, epoch: e } => {
                if e == epoch
                    && let Some(rt) = self.shard_rt(to)
                {
                    rt.outbox_ack(id);
                }
            }
            WorldEv::Crash => {
                if self.faults {
                    self.crash()?;
                }
            }
            WorldEv::Restart => self.crash()?,
            WorldEv::Read(n) => self.do_read(n),
        }
        Ok(())
    }

    /// Reader `n` asks for its stream from its offset (a long poll of 20
    /// simulated ms).
    fn do_read(&mut self, n: usize) {
        let Some(r) = self.readers.get(n).cloned() else {
            return;
        };
        if r.done || self.read_tokens.values().any(|x| *x == n) {
            return;
        }
        let token = self.next_token;
        self.next_token += 1;
        let res = self
            .shard_rt(shard_of(&r.id, self.cfg.shards))
            .map(|rt| rt.stream_read(&r.id, r.run, &r.name, r.next, 20, token));
        match res {
            Some(Ok(())) => {
                self.read_tokens.insert(token, n);
            }
            // Not started yet, or the shard is down: ask again soon.
            _ => {
                let t = self.now() + 5;
                self.at(t, WorldEv::Read(n));
            }
        }
    }

    /// A read's answer: every chunk must be the one readers saw at its
    /// offset before (a chunk a reader saw never changes or goes away),
    /// with no offset skipped, and a stream ends once.
    fn on_read(&mut self, n: usize, status: Status, payload: &[u8]) -> Result<(), Failure> {
        let now = self.now();
        if status != Status::Ok {
            self.at(now + 5, WorldEv::Read(n));
            return Ok(());
        }
        let b = fusee::StreamBatch::parse(payload)
            .map_err(|e| self.fail(format!("a stream read answered something unreadable: {e}")))?;
        let r = self.readers[n].clone();
        if b.from != r.next || r.run.is_some_and(|run| run != b.run) {
            return Err(self.fail(format!(
                "reader {n} of {} {} asked for offset {} of run {:?} and got offset {} of run {}",
                r.id, r.name, r.next, r.run, b.from, b.run
            )));
        }
        let key = (r.id.clone(), b.run, r.name.clone());
        let mut seen = self.seen.remove(&key).unwrap_or_default();
        for c in &b.chunks {
            let v = match &c.value {
                fusee::ChunkValue::Json(v) => v.clone(),
                fusee::ChunkValue::Bytes(b) => serde_json::json!(b),
            };
            match seen.chunks.get(c.offset as usize) {
                Some(prev) if *prev != v => {
                    return Err(self.fail(format!(
                        "chunk {} of {} {} was {prev} for one reader and {v} for another: a reader saw a chunk a crash took back",
                        c.offset, r.id, r.name
                    )));
                }
                Some(_) => {}
                None if c.offset as usize == seen.chunks.len() => seen.chunks.push(v),
                None => {
                    return Err(self.fail(format!(
                        "a reader of {} {} got chunk {} while readers had seen {}",
                        r.id,
                        r.name,
                        c.offset,
                        seen.chunks.len()
                    )));
                }
            }
        }
        self.report.chunks_read += b.chunks.len() as u64;
        if let Some(end) = &b.end {
            if b.next < seen.chunks.len() as u64 {
                return Err(self.fail(format!(
                    "{} {} ended at {} chunks after readers saw {}",
                    r.id,
                    r.name,
                    b.next,
                    seen.chunks.len()
                )));
            }
            match &seen.end {
                Some(prev) if *prev != end.status => {
                    return Err(self.fail(format!(
                        "{} {} ended '{prev}' for one reader and '{}' for another",
                        r.id, r.name, end.status
                    )));
                }
                _ => seen.end = Some(end.status.clone()),
            }
            self.report.streams_read += 1;
        }
        self.seen.insert(key, seen);
        let reader = &mut self.readers[n];
        reader.run = Some(b.run);
        reader.next = b.next;
        if b.end.is_some() {
            reader.done = true;
        } else {
            self.at(now + (n as u64 % 3), WorldEv::Read(n));
        }
        Ok(())
    }

    /// Every finished stream holds exactly what its workflow made: the
    /// tokens its step returned (a step that ran again continued after the
    /// recorded prefix) and its progress, appended once across replays.
    fn check_streams(&self) -> Result<(), Failure> {
        for a in &self.actions {
            let Action::Start {
                id,
                job: Job::Talker { .. },
            } = a
            else {
                continue;
            };
            let Some((Status::Ok, p)) = self.results.get(id) else {
                continue;
            };
            let hash: u64 = serde_json::from_slice(p).unwrap_or(0);
            for ((sid, _, name), s) in &self.seen {
                if sid != id {
                    continue;
                }
                if s.end.as_deref() != Some("ok") {
                    return Err(self.fail(format!(
                        "{id} {name} ended {:?}, not ok: {:?}",
                        s.end, s.chunks
                    )));
                }
                match name.as_str() {
                    "tokens" => {
                        let toks: Vec<u64> = s
                            .chunks
                            .iter()
                            .map(|v| v.as_u64().unwrap_or(u64::MAX))
                            .collect();
                        if tokens_hash(&toks) != hash {
                            return Err(self.fail(format!(
                                "readers of {id} saw tokens {toks:?}, not the ones its step returned"
                            )));
                        }
                    }
                    _ => {
                        let want: Vec<serde_json::Value> = TALKER_PROGRESS
                            .iter()
                            .map(|s| serde_json::json!(s))
                            .collect();
                        if s.chunks != want {
                            return Err(self.fail(format!(
                                "readers of {id} saw progress {:?}: a replay appended again, or an append was lost",
                                s.chunks
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_io(&mut self, io: Io) {
        let epoch = self.m.borrow().epoch;
        match io {
            Io::Flush { shard, epoch: e } if e == epoch => {
                if let Some(rt) = self.shard_rt(shard) {
                    rt.with(|i| i.env.complete_flush());
                }
            }
            Io::Checkpoint { shard, epoch: e } if e == epoch => {
                if let Some(rt) = self.shard_rt(shard) {
                    rt.with(|i| i.env.complete_checkpoint());
                }
            }
            Io::LogFlush { epoch: e, seq } if e == epoch => {
                if let Some(log) = self.log.clone() {
                    SimLog::complete(&log, &self.m, seq);
                    // The narrow window of two writes in flight: one is
                    // durable, an earlier or later one is not. Crash in it
                    // now and then.
                    if self.faults
                        && self.cfg.crash_every_ms > 0
                        && log.borrow().in_flight() > 0
                        && self.rng.below(20) == 0
                    {
                        let now = self.now();
                        self.at(now, WorldEv::Crash);
                    }
                }
            }
            Io::BlobFlush { epoch: e, id } if e == epoch => {
                if let Some(log) = self.log.clone() {
                    SimLog::complete_blobs(&log, &self.m, id);
                    // Blobs durable, the batches that reference them not
                    // yet: crash in that window now and then.
                    if self.faults && self.cfg.crash_every_ms > 0 && self.rng.below(20) == 0 {
                        let now = self.now();
                        self.at(now, WorldEv::Crash);
                    }
                }
            }
            _ => {}
        }
    }

    /// Advances to the next moment anything happens.
    fn step(&mut self) -> Result<bool, Failure> {
        self.settle()?;
        let shared_block = self
            .log
            .as_ref()
            .and_then(|l| l.borrow().violation().map(str::to_owned));
        if let Some(v) = shared_block {
            return Err(self.fail(v));
        }
        let Some(t) = self.next_time() else {
            return Ok(false);
        };
        self.m.borrow_mut().now = t;
        NOW.with(|n| n.set(t));
        loop {
            let io = {
                let mut m = self.m.borrow_mut();
                match m.io.keys().next().copied() {
                    Some(k) if k.0 <= t => m.io.remove(&k),
                    _ => None,
                }
            };
            match io {
                Some(io) => self.handle_io(io),
                None => break,
            }
        }
        loop {
            let ev = match self.events.keys().next().copied() {
                Some(k) if k.0 <= t => self.events.remove(&k),
                _ => None,
            };
            match ev {
                Some(ev) => self.handle(ev)?,
                None => break,
            }
        }
        Ok(true)
    }

    pub fn run(mut self) -> Result<Report, Failure> {
        self.open_all().map_err(|e| self.fail(e))?;
        let start = self.now();
        let fault_end = start + self.cfg.duration_ms;
        while self.now() < fault_end {
            if !self.step()? {
                break;
            }
            self.check_service()?;
        }
        // Faults stop. Every acknowledged workflow must now finish.
        self.faults = false;
        self.m.borrow_mut().flush_fail_per_mille = 0;
        self.events.retain(|_, e| !matches!(e, WorldEv::Crash));
        if self
            .shards
            .iter()
            .flatten()
            .any(|s| s.with(|i| i.failed().is_some()))
        {
            self.faults = true;
            self.crash()?;
            self.faults = false;
        }
        let deadline = fault_end + self.cfg.settle_ms;
        loop {
            let all_done = self.all_finished();
            if all_done {
                break;
            }
            // No more crashes change the code: a stuck run stays stuck.
            if let Some((id, why)) = self.first_stuck() {
                return Err(self.fail(format!("{id} stopped on a determinism error: {why}")));
            }
            if self.now() > deadline {
                let pending = self.unfinished();
                return Err(self.fail(format!(
                    "{} workflows did not finish after faults stopped: {}",
                    pending.len(),
                    pending.join(", ")
                )));
            }
            if !self.step()? {
                // Nothing scheduled: give retransmission timers a chance.
                let t = self.now() + 500;
                self.m.borrow_mut().now = t;
                NOW.with(|n| n.set(t));
            }
        }
        self.check_after_recovery()?;
        self.check_final()?;
        self.run_user_checks()?;
        self.report.finished = self.results.len() as u32;
        self.report.updates = self.update_replies.len() as u32;
        self.report.queries = self.query_answers.len() as u32;
        self.report.completions = self.completions.values().map(|v| v.len() as u32).sum();
        self.report.queued = self.receipts.len() as u32;
        self.report.windows = self.receipts.values().collect::<BTreeSet<_>>().len() as u32;
        if let Ok(s) = self.svc.lock() {
            self.report.scheduled = s.ticks.len() as u32;
            self.report.heartbeats = s.progress.values().map(|p| p.len() as u32).sum();
            self.report.reads_rerun = s.reads_rerun;
            self.report.shared_log = self.log.is_some();
            self.report.log_batches = self
                .log
                .as_ref()
                .map_or(0, |l| l.borrow().wal.position() - 1);
        }
        self.report.offloaded += self
            .log
            .as_ref()
            .map_or(0, |l| l.borrow().wal.blob_stats().records);
        self.report.sim_ms = self.now() - start;
        self.report.service_calls = self.svc.lock().map(|s| s.total).unwrap_or(0);
        self.report.records = self
            .shards
            .iter()
            .flatten()
            .map(|s| s.metrics().records)
            .sum();
        self.report.flush_failures = self
            .m
            .borrow()
            .log
            .iter()
            .filter(|l| l.contains("flush failed"))
            .count() as u32;
        Ok(self.report)
    }

    fn run_user_checks(&mut self) -> Result<(), Failure> {
        if self.checks.is_empty() {
            return Ok(());
        }
        let mut streams = BTreeMap::new();
        // Keys sort by run, so each ID's newest run read comes last.
        for ((id, _, name), s) in &self.seen {
            streams.insert(
                (id.clone(), name.clone()),
                (s.chunks.clone(), s.end.clone()),
            );
        }
        let outcome = Outcome {
            seed: self.cfg.seed,
            crashes: self.report.crashes,
            results: self.results.clone(),
            receipts: self.receipts.clone(),
            streams,
            calls: self.calls.clone(),
        };
        for check in std::mem::take(&mut self.checks) {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(&outcome)));
            let message = match r {
                Ok(Ok(())) => continue,
                Ok(Err(m)) => m,
                Err(p) => p
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                    .unwrap_or_else(|| "the check panicked".into()),
            };
            return Err(self.fail(format!("check failed: {message}")));
        }
        Ok(())
    }

    fn first_stuck(&self) -> Option<(String, String)> {
        let shards = self.cfg.shards;
        self.started_ids()
            .into_iter()
            .filter(|id| !self.results.contains_key(id))
            .find_map(|id| {
                let rt = self.shard_rt(shard_of(&id, shards))?;
                let why = rt.stuck_reason(rt.find(&id)?)?;
                Some((id, why))
            })
    }

    fn started_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .actions
            .iter()
            .zip(&self.acked)
            .filter_map(|(a, ok)| match a {
                Action::Start { id, .. } | Action::StartAs { id, .. } if *ok => Some(id.clone()),
                _ => None,
            })
            .collect();
        // The run every acknowledged debounced or batched event joined must
        // start and finish too.
        let runs: BTreeSet<&String> = self.receipts.values().collect();
        ids.extend(runs.into_iter().cloned());
        ids
    }

    fn all_finished(&mut self) -> bool {
        if self.acked.iter().any(|a| !a) || self.readers.iter().any(|r| !r.done) {
            return false;
        }
        let shards = self.cfg.shards;
        // Children that exist must have ended too (an abandoned child may
        // start late, after its start message was resent).
        for (parent, kids) in self.parents.clone() {
            for k in 0..kids {
                let kid = format!("{parent}/kid{k}");
                let running = self.shard_rt(shard_of(&kid, shards)).is_some_and(|rt| {
                    rt.find(&kid)
                        .and_then(|r| rt.summary(r))
                        .is_some_and(|s| s.finished_ms.is_none())
                });
                if running {
                    return false;
                }
            }
        }
        for id in self.started_ids() {
            if self.results.contains_key(&id) {
                continue;
            }
            let r = self
                .shard_rt(shard_of(&id, shards))
                .and_then(|rt| rt.result(&id).ok());
            if let Some(ResultState::Done {
                status, payload, ..
            }) = r
            {
                if self.record_result(&id, status, payload.to_vec()).is_err() {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    fn unfinished(&self) -> Vec<String> {
        let shards = self.cfg.shards;
        let mut out = Vec::new();
        for (ix, a) in self.actions.iter().enumerate() {
            if !self.acked[ix] {
                out.push(format!("{a:?} (unacknowledged)"));
            }
        }
        for r in self.readers.iter().filter(|r| !r.done) {
            out.push(format!("reader {r:?} (not at the end of its stream)"));
        }
        for id in self.started_ids() {
            if !self.results.contains_key(&id) {
                let rt = self.shard_rt(shard_of(&id, shards));
                let s = rt.and_then(|rt| rt.find(&id).and_then(|r| rt.summary(r)));
                let j = rt
                    .and_then(|rt| rt.inspect(&id).ok().flatten())
                    .map(|d| d.journal);
                let timers = rt.map(|rt| {
                    rt.with(|i| {
                        (
                            i.core.timers_len(),
                            i.core.next_timer(),
                            fusee::env::Env::now_ms(&i.env),
                        )
                    })
                });
                let child = format!("{id}/ship");
                let crt = self.shard_rt(shard_of(&child, shards));
                let cs = crt.and_then(|rt| rt.find(&child).and_then(|r| rt.summary(r)));
                let outboxes: Vec<usize> = self
                    .shards
                    .iter()
                    .flatten()
                    .map(|s| s.outbox_len())
                    .collect();
                out.push(format!("{id}: {s:?}\n    timers {timers:?}\n    journal {j:?}\n    child {cs:?}\n    outboxes {outboxes:?}"));
            }
        }
        out
    }
}

/// Runs one seed.
pub fn run_seed(cfg: SimConfig) -> Result<Report, Failure> {
    let _ = SimFile::default();
    World::new(cfg).run()
}
