//! Your workflows under the simulator.
//!
//! A [`Scenario`] registers your workflow functions, the starts and signals
//! a client sends, and checks on the results. Each seed runs it on a
//! simulated machine: the real shard runtime and Rust SDK, a seeded
//! scheduler, a disk that tears unflushed writes and fails flushes, process
//! crashes at random moments, delayed messages between shards, and a clock
//! that skips ahead whenever nothing is runnable, so a workflow that sleeps
//! for a month finishes in milliseconds. The seed also picks the shard
//! count, park and checkpoint timing, segment size, log format and payload
//! encryption, so a range of seeds covers configurations as well as
//! interleavings.
//!
//! After every recovery and at the end the simulator checks what the engine
//! promises: an acknowledged start or signal is never lost, a reported
//! result never changes, no run stops on a determinism error when its
//! journal is replayed (nondeterministic workflow code shows up here), a
//! stream reader ([`Setup::read_stream`]) never sees a chunk change or go
//! away, and once faults stop every workflow finishes. Your checks then see the final
//! results and whatever state your step bodies kept.
//!
//! ```no_run
//! use fusee::{Ctx, Error};
//! use fusee_sim::scenario::Scenario;
//! use std::sync::{Arc, Mutex};
//! use std::time::Duration;
//!
//! let scenario = Scenario::new(|sim| {
//!     // State that outlives crashes, like an external system: the ledger.
//!     let ledger = Arc::new(Mutex::new(Vec::<(String, u64)>::new()));
//!     let l = ledger.clone();
//!     sim.register("pay", move |ctx: Ctx, amount: u64| {
//!         let l = l.clone();
//!         async move {
//!             ctx.sleep("a-day", Duration::from_secs(86_400)).await?;
//!             ctx.step("charge", || {
//!                 let l = l.clone();
//!                 let key = ctx.key();
//!                 async move {
//!                     fusee_sim::sleep(Duration::from_millis(3)).await;
//!                     let mut l = l.lock().unwrap();
//!                     if !l.iter().any(|(k, _)| *k == key) {
//!                         l.push((key, amount));
//!                     }
//!                     Ok::<_, Error>(amount)
//!                 }
//!             })
//!             .await
//!         }
//!     });
//!     for i in 0..20 {
//!         let at = Duration::from_millis(sim.random(500));
//!         sim.start(at, &format!("pay-{i}"), "pay", &(i * 10));
//!     }
//!     sim.check(move |out| {
//!         for i in 0..20u64 {
//!             assert_eq!(out.result::<u64>(&format!("pay-{i}")), Some(Ok(i * 10)));
//!         }
//!         assert_eq!(ledger.lock().unwrap().len(), 20);
//!         Ok(())
//!     });
//! });
//! scenario.run_seeds(0..200).unwrap();
//! ```
//!
//! Step bodies run on the simulator's single thread: wait with
//! [`crate::sleep`], not `tokio::time::sleep`, and stand in for networks
//! and databases with in-memory state like the ledger above. Workflow code
//! itself needs no change; the same `async fn` can be registered with
//! [`fusee::Engine::workflow`] and here.

use crate::rng::Rng;
use crate::world::{Failure, Report, SimConfig, World, erase};
use fusee::ctx::Ctx;
use fusee::error::Error;
use fusee::runtime::SharedRegistry;
use fusee_core::record::Status;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// A check on the outcome of one seed. An `Err` (or a panic) fails the seed.
pub type Check = Box<dyn FnOnce(&Outcome) -> Result<(), String>>;

pub(crate) enum UserAction {
    Start {
        id: String,
        wtype: String,
        input: Vec<u8>,
    },
    Signal {
        id: String,
        name: String,
        payload: Vec<u8>,
    },
    Cancel {
        id: String,
    },
    Terminate {
        id: String,
    },
    /// A client reads a stream to its end (see [`fusee::stream`]).
    ReadStream {
        id: String,
        name: String,
    },
    /// A call to a key of an object type (see [`fusee::object`]).
    Call {
        wtype: String,
        key: String,
        handler: String,
        call_id: String,
        arg: Vec<u8>,
    },
    /// A debounced start or a batched event (see [`fusee::queue`]).
    Enqueue {
        workflow: String,
        key: String,
        event_id: String,
        input: serde_json::Value,
        rule: fusee::queue::Rule,
    },
}

/// What a scenario's setup function builds for one seed: the workflows,
/// the client's requests and the checks.
pub struct Setup {
    seed: u64,
    rng: Rng,
    registry: SharedRegistry,
    pub(crate) actions: Vec<(u64, UserAction)>,
    pub(crate) checks: Vec<Check>,
}

impl Setup {
    pub(crate) fn new(seed: u64, registry: SharedRegistry) -> Self {
        Self {
            seed,
            rng: Rng::new(seed ^ 0x5ce7_a210_0f1e_5eed),
            registry,
            actions: Vec::new(),
            checks: Vec::new(),
        }
    }

    /// The seed being run.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// A number below `n` from the seed, to vary the scenario per seed.
    pub fn random(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.rng.below(n) }
    }

    /// Registers a workflow type's code (at the reference's version, else
    /// version 1). Inputs and results are JSON, as with
    /// [`fusee::Engine::register`].
    pub fn register<I, O, F, Fut>(
        &mut self,
        workflow: impl fusee::AsWorkflow<I, O>,
        f: F,
    ) -> &mut Self
    where
        I: Serialize + DeserializeOwned + 'static,
        O: Serialize + DeserializeOwned + 'static,
        F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, Error>> + 'static,
    {
        let mut r = self.registry.write().unwrap_or_else(|e| e.into_inner());
        r.register(workflow.name(), workflow.version().unwrap_or(1), erase(f));
        drop(r);
        self
    }

    /// Registers an object type with its handlers (see [`fusee::object`]).
    /// Its keys are called by the client ([`call`](Self::call)) and by
    /// workflow code (`ctx.object(type, key)`).
    pub fn object(&mut self, name: &str, handlers: fusee::object::ObjectHandlers) -> &mut Self {
        let (f, def) = handlers.into_code();
        let mut r = self.registry.write().unwrap_or_else(|e| e.into_inner());
        r.register(name, 1, f);
        r.register_object(name, def);
        drop(r);
        self
    }

    /// The client calls `handler` of key `key` of object type `wtype` at
    /// `at` ([`fusee::ObjectRef::call_with_id`]). Until it is answered the
    /// client sends it again, across crashes, with the same `call_id`; the
    /// simulator fails the seed if two answers to one call ID differ (the
    /// call ran twice). [`Outcome::call`] has the answer.
    pub fn call(
        &mut self,
        at: Duration,
        wtype: &str,
        key: &str,
        handler: &str,
        call_id: &str,
        arg: &impl Serialize,
    ) -> &mut Self {
        assert!(!call_id.is_empty(), "a simulated call needs an ID");
        self.actions.push((
            at.as_millis() as u64,
            UserAction::Call {
                wtype: wtype.into(),
                key: key.into(),
                handler: handler.into(),
                call_id: call_id.into(),
                arg: serde_json::to_vec(arg).expect("the argument encodes as JSON"),
            },
        ));
        self
    }

    /// The client starts `id` as a `workflow` at `at` after the
    /// simulation begins. Until it is acknowledged the client retries,
    /// across crashes, attaching to the existing run.
    pub fn start(
        &mut self,
        at: Duration,
        id: &str,
        workflow: &str,
        input: &impl Serialize,
    ) -> &mut Self {
        self.actions.push((
            at.as_millis() as u64,
            UserAction::Start {
                id: id.into(),
                wtype: workflow.into(),
                input: serde_json::to_vec(input).expect("the input encodes as JSON"),
            },
        ));
        self
    }

    /// The client signals `id` once its start was acknowledged; retries
    /// carry one deduplication key, so the run receives it once.
    pub fn signal(
        &mut self,
        at: Duration,
        id: &str,
        name: &str,
        payload: &impl Serialize,
    ) -> &mut Self {
        self.actions.push((
            at.as_millis() as u64,
            UserAction::Signal {
                id: id.into(),
                name: name.into(),
                payload: serde_json::to_vec(payload).expect("the payload encodes as JSON"),
            },
        ));
        self
    }

    /// A client reads stream `name` of `id` from `at` on, to its end,
    /// connecting again at its offset after every crash. The simulator
    /// checks that every reader of a stream sees the same chunk at each
    /// offset (none changes or goes away after a crash), with no offset
    /// skipped; [`Outcome::stream`] has what they saw.
    pub fn read_stream(&mut self, at: Duration, id: &str, name: &str) -> &mut Self {
        self.actions.push((
            at.as_millis() as u64,
            UserAction::ReadStream {
                id: id.into(),
                name: name.into(),
            },
        ));
        self
    }

    /// The client cancels `id`.
    pub fn cancel(&mut self, at: Duration, id: &str) -> &mut Self {
        self.actions
            .push((at.as_millis() as u64, UserAction::Cancel { id: id.into() }));
        self
    }

    /// The client terminates `id`.
    pub fn terminate(&mut self, at: Duration, id: &str) -> &mut Self {
        self.actions.push((
            at.as_millis() as u64,
            UserAction::Terminate { id: id.into() },
        ));
        self
    }

    /// The client debounces a start of `workflow` under `key` at `at`
    /// ([`fusee::Workflow::debounce_with_id`]). Until it is acknowledged the
    /// client sends it again, across crashes, with the same `event_id`; the
    /// run it joined is in [`Outcome::receipt`]. The simulator checks that
    /// every acknowledged event's run starts and finishes.
    pub fn debounce(
        &mut self,
        at: Duration,
        workflow: &str,
        key: &str,
        event_id: &str,
        input: &impl Serialize,
        rule: &fusee::Debounce,
    ) -> &mut Self {
        self.enqueue(
            at,
            workflow,
            key,
            event_id,
            input,
            fusee::queue::Rule::Debounce(rule.clone()),
        )
    }

    /// The client adds an event to `workflow`'s batch under `key` at `at`
    /// ([`fusee::Workflow::batch_with_id`]), retrying as for
    /// [`debounce`](Self::debounce).
    pub fn batch(
        &mut self,
        at: Duration,
        workflow: &str,
        key: &str,
        event_id: &str,
        event: &impl Serialize,
        rule: &fusee::Batch,
    ) -> &mut Self {
        self.enqueue(
            at,
            workflow,
            key,
            event_id,
            event,
            fusee::queue::Rule::Batch(rule.clone()),
        )
    }

    fn enqueue(
        &mut self,
        at: Duration,
        workflow: &str,
        key: &str,
        event_id: &str,
        input: &impl Serialize,
        rule: fusee::queue::Rule,
    ) -> &mut Self {
        assert!(!event_id.is_empty(), "a simulated event needs an ID");
        self.actions.push((
            at.as_millis() as u64,
            UserAction::Enqueue {
                workflow: workflow.into(),
                key: key.into(),
                event_id: event_id.into(),
                input: serde_json::to_value(input).expect("the input encodes as JSON"),
                rule,
            },
        ));
        self
    }

    /// Adds a check on the final results.
    pub fn check(&mut self, f: impl FnOnce(&Outcome) -> Result<(), String> + 'static) -> &mut Self {
        self.checks.push(Box::new(f));
        self
    }
}

/// The final results of one seed.
#[derive(Debug)]
pub struct Outcome {
    pub seed: u64,
    /// Process crashes during the run.
    pub crashes: u32,
    pub(crate) results: BTreeMap<String, (Status, Vec<u8>)>,
    /// Event ID -> the workflow ID of the run it joined.
    pub(crate) receipts: BTreeMap<String, String>,
    /// `(workflow ID, stream)` -> the chunks its readers saw (of the ID's
    /// newest run they read), and how it ended.
    pub(crate) streams: BTreeMap<(String, String), (Vec<serde_json::Value>, Option<String>)>,
    /// Object call ID -> its answer.
    pub(crate) calls: BTreeMap<String, CallReply>,
}

/// The answer to one object call ([`Setup::call`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallReply {
    /// `ok`, `failed`, ...
    pub status: String,
    pub payload: Vec<u8>,
    /// Simulated time the client first sent the call, and got its answer.
    pub sent_ms: u64,
    pub answered_ms: u64,
}

impl Outcome {
    /// The answer to object call `call_id`: the decoded value when the
    /// handler succeeded, the error message otherwise.
    pub fn call<T: DeserializeOwned>(&self, call_id: &str) -> Option<Result<T, String>> {
        let c = self.calls.get(call_id)?;
        Some(if c.status == "ok" {
            serde_json::from_slice(&c.payload).map_err(|e| e.to_string())
        } else {
            Err(String::from_utf8_lossy(&c.payload).into_owned())
        })
    }

    /// Every answered object call, by call ID.
    pub fn calls(&self) -> impl Iterator<Item = (&str, &CallReply)> {
        self.calls.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// The workflow ID of the run a debounced or batched event joined.
    pub fn receipt(&self, event_id: &str) -> Option<&str> {
        self.receipts.get(event_id).map(String::as_str)
    }

    /// Every acknowledged event and the run it joined, by event ID.
    pub fn receipts(&self) -> impl Iterator<Item = (&str, &str)> {
        self.receipts.iter().map(|(e, r)| (e.as_str(), r.as_str()))
    }

    /// The chunks readers of stream `name` of `id` saw, in order (bytes as
    /// a JSON array of numbers), if a [`Setup::read_stream`] read it.
    pub fn stream(&self, id: &str, name: &str) -> Option<&[serde_json::Value]> {
        self.streams
            .get(&(id.to_owned(), name.to_owned()))
            .map(|s| s.0.as_slice())
    }

    /// How stream `name` of `id` ended for its readers (`ok`, `failed`, the
    /// run's status).
    pub fn stream_end(&self, id: &str, name: &str) -> Option<&str> {
        self.streams
            .get(&(id.to_owned(), name.to_owned()))
            .and_then(|s| s.1.as_deref())
    }

    /// IDs with a result.
    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.results.keys().map(String::as_str)
    }

    /// `ok`, `failed`, `cancelled`, `timed_out` or `terminated`; `None` for
    /// an ID the client never started.
    pub fn status(&self, id: &str) -> Option<&'static str> {
        self.results.get(id).map(|r| r.0.as_str())
    }

    /// The result of `id`: the decoded value when it succeeded, the error
    /// message otherwise.
    pub fn result<T: DeserializeOwned>(&self, id: &str) -> Option<Result<T, String>> {
        let (status, payload) = self.results.get(id)?;
        Some(match status {
            Status::Ok => serde_json::from_slice(payload).map_err(|e| e.to_string()),
            _ => Err(String::from_utf8_lossy(payload).into_owned()),
        })
    }
}

type SetupFn = dyn Fn(&mut Setup) + Send + Sync;

/// A scenario's setup function, carried by [`SimConfig::user`].
#[derive(Clone)]
pub struct UserSetup(pub(crate) Arc<SetupFn>);

impl std::fmt::Debug for UserSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UserSetup")
    }
}

/// Your workflows and requests, run under seeds. See the module docs.
#[derive(Clone)]
pub struct Scenario {
    setup: UserSetup,
    shards: Option<u32>,
    fault_window_ms: u64,
    crash_every_ms: Option<u64>,
    flush_fail_per_mille: Option<u32>,
    tear: bool,
    settle_ms: u64,
}

/// Totals over the seeds [`Scenario::run_seeds`] ran.
#[derive(Clone, Debug, Default)]
pub struct Summary {
    pub seeds: u64,
    pub crashes: u64,
    pub workflows: u64,
    pub records: u64,
    pub flush_failures: u64,
    pub simulated_ms: u64,
}

impl Scenario {
    /// `setup` runs once per seed, on a fresh machine, and builds that
    /// seed's workflows, requests and checks.
    pub fn new(setup: impl Fn(&mut Setup) + Send + Sync + 'static) -> Self {
        Self {
            setup: UserSetup(Arc::new(setup)),
            shards: None,
            fault_window_ms: 2_000,
            crash_every_ms: None,
            flush_fail_per_mille: None,
            tear: true,
            settle_ms: 3_600_000,
        }
    }

    /// A fixed shard count (by default the seed picks 1 to 3).
    pub fn shards(mut self, n: u32) -> Self {
        self.shards = Some(n.max(1));
        self
    }

    /// How long faults are injected, from the start of the simulation
    /// (default 2 s of simulated time). Requests may come later.
    pub fn fault_window(mut self, d: Duration) -> Self {
        self.fault_window_ms = d.as_millis() as u64;
        self
    }

    /// Mean time between process crashes in the fault window; zero for
    /// none. By default the seed picks none, 150, 400 or 900 ms.
    pub fn crash_every(mut self, d: Duration) -> Self {
        self.crash_every_ms = Some(d.as_millis() as u64);
        self
    }

    /// Flushes that fail, per thousand, in the fault window. By default
    /// the seed picks 0, 2 or 10.
    pub fn flush_failures_per_mille(mut self, n: u32) -> Self {
        self.flush_fail_per_mille = Some(n);
        self
    }

    /// Whether a crash tears unflushed writes (default true) or only loses
    /// them.
    pub fn torn_writes(mut self, on: bool) -> Self {
        self.tear = on;
        self
    }

    /// No crashes and no failed flushes: the scheduler and the clock only.
    pub fn no_faults(self) -> Self {
        self.crash_every(Duration::ZERO).flush_failures_per_mille(0)
    }

    /// Simulated time every workflow gets to finish once faults stop
    /// (default one hour). Workflows that sleep longer need more.
    pub fn settle_within(mut self, d: Duration) -> Self {
        self.settle_ms = d.as_millis() as u64;
        self
    }

    /// The configuration seed `seed` runs with.
    pub fn config(&self, seed: u64) -> SimConfig {
        let mut cfg = SimConfig::for_seed(seed);
        if let Some(n) = self.shards {
            cfg.shards = n;
        }
        cfg.duration_ms = self.fault_window_ms;
        if let Some(c) = self.crash_every_ms {
            cfg.crash_every_ms = c;
        }
        if let Some(f) = self.flush_fail_per_mille {
            cfg.flush_fail_per_mille = f;
        }
        cfg.tear = self.tear;
        cfg.settle_ms = self.settle_ms;
        cfg.user = Some(self.setup.clone());
        cfg
    }

    /// Runs one seed. The same seed reproduces the same run, so a failing
    /// seed is a test case.
    pub fn run(&self, seed: u64) -> Result<Report, Failure> {
        World::new(self.config(seed)).run()
    }

    /// Runs every seed, stopping at the first failure.
    pub fn run_seeds(&self, seeds: impl IntoIterator<Item = u64>) -> Result<Summary, Failure> {
        let mut s = Summary::default();
        for seed in seeds {
            let r = self.run(seed)?;
            s.seeds += 1;
            s.crashes += u64::from(r.crashes);
            s.workflows += u64::from(r.workflows);
            s.records += r.records;
            s.flush_failures += u64::from(r.flush_failures);
            s.simulated_ms += r.sim_ms;
        }
        Ok(s)
    }
}
