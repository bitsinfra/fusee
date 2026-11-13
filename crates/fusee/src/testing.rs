//! The test kit (feature `testing`): a `memory` engine under the dev profile
//! with virtual time, step mocks, simulated crashes, and `crash_everywhere`.
//!
//! ```toml
//! [dev-dependencies]
//! fusee = { version = "…", features = ["testing"] }
//! ```
//!
//! ```ignore
//! let t = TestEngine::open().await?;
//! t.register(CHECKOUT, checkout);
//! t.mock_step(CHECKOUT, "charge", |_attempt| Ok(json!({"id": "ch_1"})));
//! t.engine().start(CHECKOUT, "order-1", &order).await?;
//! t.advance(Duration::from_secs(86_400)).await;   // or let result() skip time
//! t.crash().await?;                               // drop memory, recover, continue
//! let receipt: Receipt = t.result("order-1").await?;
//! assert_eq!(t.steps("order-1").await?, ["quote", "charge"]);
//! t.check().await?;                               // strict replay of every run
//! ```
//!
//! Builds without the feature carry none of these hooks: the step path is
//! unchanged.

use crate::ctx::encode;
use crate::defs::{AsWorkflow, WorkflowRef};
use crate::engine::{Config, Engine, StartOptions};
use crate::error::{Error, Result};
use crate::failure::Failure;
use crate::{Ctx, ReplayReport};
use fusee_core::record::{Payload, Record, RecordBuf};
use rustc_hash::FxHashMap;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

type MockFn = Arc<dyn Fn(u32) -> std::result::Result<Value, Failure> + Send + Sync>;

/// What a test engine shares with its shard threads.
#[doc(hidden)]
#[derive(Default)]
pub struct Hooks {
    /// Milliseconds the virtual clock is ahead of the wall clock.
    offset: Arc<AtomicU64>,
    mocks: Mutex<FxHashMap<(String, String), MockFn>>,
}

impl std::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hooks")
            .field("offset_ms", &self.offset_ms())
            .finish_non_exhaustive()
    }
}

impl Hooks {
    pub(crate) fn clock(&self) -> Arc<AtomicU64> {
        self.offset.clone()
    }

    pub(crate) fn offset_ms(&self) -> u64 {
        self.offset.load(Ordering::Acquire)
    }
}

thread_local! {
    static HOOKS: RefCell<Option<Arc<Hooks>>> = const { RefCell::new(None) };
}

/// Called on each shard thread as it starts.
pub(crate) fn install(h: Option<Arc<Hooks>>) {
    HOOKS.with(|x| *x.borrow_mut() = h);
}

/// A mocked step's outcome, if its workflow type and name have a mock.
pub(crate) fn mocked(
    wtype: &str,
    step: &str,
    attempt: u32,
) -> Option<std::result::Result<Payload, Failure>> {
    let m = HOOKS.with(|h| {
        let h = h.borrow();
        let h = h.as_ref()?;
        let mocks = h.mocks.lock().ok()?;
        mocks.get(&(wtype.to_owned(), step.to_owned())).cloned()
    })?;
    Some(
        m(attempt)
            .and_then(|v| encode(&v).map_err(|e| Failure::fatal(e.to_string()).kind("Codec"))),
    )
}

/// A workflow type, by reference or name.
pub trait TypeName {
    fn type_name(&self) -> String;
}

impl TypeName for &str {
    fn type_name(&self) -> String {
        (*self).to_owned()
    }
}

impl TypeName for String {
    fn type_name(&self) -> String {
        self.clone()
    }
}

impl<I, O> TypeName for WorkflowRef<I, O> {
    fn type_name(&self) -> String {
        self.name().to_owned()
    }
}

impl<I, O> TypeName for &WorkflowRef<I, O> {
    fn type_name(&self) -> String {
        self.name().to_owned()
    }
}

type Registration = Box<dyn Fn(&Engine) + Send + Sync>;

struct Inner {
    hooks: Arc<Hooks>,
    config: Config,
    engine: RwLock<Engine>,
    registrations: Mutex<Vec<Registration>>,
}

/// A `memory` engine for tests: dev profile (strict replay; a stuck run's
/// result fails), virtual time, mocks, and crashes. Cheap to clone.
#[derive(Clone)]
pub struct TestEngine {
    inner: Arc<Inner>,
}

impl TestEngine {
    /// One shard.
    pub async fn open() -> Result<Self> {
        Self::with_shards(1).await
    }

    pub async fn with_shards(shards: u32) -> Result<Self> {
        let hooks = Arc::new(Hooks::default());
        let mut config = Config::memory().shards(shards).dev().http("off");
        config.test_hooks = Some(hooks.clone());
        let engine = Engine::open(config.clone()).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                hooks,
                config,
                engine: RwLock::new(engine),
                registrations: Mutex::new(Vec::new()),
            }),
        })
    }

    /// The engine now (a new one after [`crash`](Self::crash)).
    pub fn engine(&self) -> Engine {
        self.inner
            .engine
            .read()
            .map(|e| e.clone())
            .unwrap_or_else(|e| e.into_inner().clone())
    }

    /// Registers a workflow type, as [`Engine::register`] does, and again
    /// on the engine that follows each crash.
    pub fn register<W, I, O, F, Fut>(&self, workflow: W, f: F) -> WorkflowRef<I, O>
    where
        W: AsWorkflow<I, O> + Clone + Send + Sync + 'static,
        I: Serialize + DeserializeOwned + 'static,
        O: Serialize + DeserializeOwned + 'static,
        F: Fn(Ctx, I) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + 'static,
    {
        let reference = self.engine().register(workflow.clone(), f.clone());
        if let Ok(mut r) = self.inner.registrations.lock() {
            r.push(Box::new(move |e: &Engine| {
                e.register(workflow.clone(), f.clone());
            }));
        }
        reference
    }

    /// Replaces a step's function: `f(attempt)` gives its result, or a
    /// failure (retried under the step's policy unless final).
    pub fn mock_step(
        &self,
        workflow: impl TypeName,
        step: &str,
        f: impl Fn(u32) -> std::result::Result<Value, Failure> + Send + Sync + 'static,
    ) {
        if let Ok(mut m) = self.inner.hooks.mocks.lock() {
            m.insert((workflow.type_name(), step.to_owned()), Arc::new(f));
        }
    }

    /// The virtual time, Unix ms.
    pub fn now_ms(&self) -> u64 {
        crate::local::unix_ms() + self.inner.hooks.offset_ms()
    }

    /// Moves the clock forward: timers that fall due fire.
    pub async fn advance(&self, by: Duration) {
        self.inner
            .hooks
            .offset
            .fetch_add(by.as_millis() as u64, Ordering::AcqRel);
        self.engine().kick_all();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    /// While every run waits on a timer (no step function waits on outside
    /// work), moves the clock to the next one.
    async fn skip(&self) -> Result<bool> {
        let Some(at) = self.engine().next_timer_ms().await? else {
            return Ok(false);
        };
        let now = self.now_ms();
        if at > now {
            self.advance(Duration::from_millis(at - now)).await;
        } else {
            self.engine().kick_all();
        }
        Ok(true)
    }

    /// The run's result. While every run waits on a timer, time skips to
    /// it, so a workflow that sleeps a day finishes at once.
    pub async fn result<O: DeserializeOwned>(&self, id: &str) -> Result<O> {
        let engine = self.engine();
        let run = engine.run::<O>(id);
        let fut = run.result();
        tokio::pin!(fut);
        loop {
            tokio::select! {
                r = &mut fut => return r,
                // Each look goes to the shards between two ticks, so no
                // run's code is between two calls.
                _ = tokio::time::sleep(Duration::from_millis(2)) => {
                    self.skip().await?;
                }
            }
        }
    }

    /// The run's journal.
    pub async fn journal(&self, id: &str) -> Result<Vec<RecordBuf>> {
        match self.engine().describe(id).await? {
            Some(d) => Ok(d.journal),
            None => Err(Error::NotFound(format!("workflow '{id}' not found"))),
        }
    }

    /// The names of the steps the run completed, in order.
    pub async fn steps(&self, id: &str) -> Result<Vec<String>> {
        Ok(self
            .journal(id)
            .await?
            .iter()
            .filter_map(|r| match r {
                Record::Step { name, .. } => Some(name.to_string()),
                _ => None,
            })
            .collect())
    }

    /// The run's journal as stable JSON, for snapshot tests (run IDs,
    /// times and keys normalized, as the conformance suite compares them).
    pub async fn snapshot(&self, id: &str) -> Result<Value> {
        Ok(crate::journal::normalize(&self.journal(id).await?))
    }

    /// Strict replay of every run against the registered code now: an error
    /// naming each run and step whose code differs from its journal.
    pub async fn check(&self) -> Result<()> {
        let found: Vec<ReplayReport> = self.engine().replay_check().await?;
        if found.is_empty() {
            return Ok(());
        }
        Err(Error::Determinism(
            found
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join("; "),
        ))
    }

    /// Drops the engine's memory as a crash would and recovers: a new
    /// engine loads every run from its journal, the code replays, and runs
    /// continue. Step results come from the journal; steps whose result was
    /// not recorded run again.
    pub async fn crash(&self) -> Result<()> {
        self.crash_with(FxHashMap::default()).await
    }

    /// [`crash`](Self::crash), with some runs recovered from other journals
    /// (`crash_everywhere` cuts one short).
    async fn crash_with(&self, mut journals: FxHashMap<String, Vec<RecordBuf>>) -> Result<()> {
        let old = self.engine();
        let mut runs = old.list(usize::MAX).await?;
        runs.retain(|r| !r.wtype.starts_with("fusee."));
        runs.sort_by_key(|r| (r.started_ms, r.shard, r.run));
        let mut saved = Vec::new();
        for r in &runs {
            let records = match journals.remove(&r.id) {
                Some(j) => j,
                None => match old.describe(&r.id).await? {
                    Some(d) => d.journal,
                    None => continue,
                },
            };
            saved.push((r.id.clone(), records));
        }
        old.shutdown().await?;
        drop(old);
        let engine = Engine::open(self.inner.config.clone()).await?;
        if let Ok(regs) = self.inner.registrations.lock() {
            for r in regs.iter() {
                r(&engine);
            }
        }
        for (id, records) in saved {
            engine.import_journal(&id, records).await?;
        }
        if let Ok(mut e) = self.inner.engine.write() {
            *e = engine;
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.engine().shutdown().await
    }

    async fn start_case(&self, case: &Case) -> Result<()> {
        self.engine()
            .start_raw(
                &case.workflow,
                &case.id,
                encode(&case.input)?,
                StartOptions::default(),
            )
            .await
    }
}

/// One run for [`crash_everywhere`]: its workflow type, ID and input, and how
/// to read the outside effects of its steps (a fake service's calls).
pub struct Case {
    pub workflow: String,
    pub id: String,
    pub input: Value,
    pub effects: Box<dyn Fn() -> Value + Send + Sync>,
}

/// Runs a workflow once per step boundary with a crash at each: the step's
/// function ran, its result was not recorded, the engine recovered. Each
/// time the result and the outside effects must equal those of a run
/// without a crash: a step that is not idempotent (charges twice when run
/// again) is found and named.
///
/// `build` gets a fresh engine, registers the workflows with fresh fakes,
/// and returns the [`Case`].
pub async fn crash_everywhere<F, Fut>(build: F) -> Result<()>
where
    F: Fn(TestEngine) -> Fut,
    Fut: Future<Output = Result<Case>>,
{
    let t = TestEngine::open().await?;
    let case = build(t.clone()).await?;
    t.start_case(&case).await?;
    let expected: Value = t.result(&case.id).await?;
    let effects = (case.effects)();
    let steps = t.steps(&case.id).await?;
    t.close().await?;
    for (k, step) in steps.iter().enumerate() {
        let t = TestEngine::open().await?;
        let case = build(t.clone()).await?;
        t.start_case(&case).await?;
        let _: Value = t.result(&case.id).await?;
        // Cut the journal just before the k-th step's record: that step
        // and the ones after it run again after the crash.
        let full = t.journal(&case.id).await?;
        let mut seen = 0;
        let cut = full
            .iter()
            .position(|r| {
                if matches!(r, Record::Step { .. }) {
                    seen += 1;
                    seen == k + 1
                } else {
                    false
                }
            })
            .unwrap_or(full.len());
        let mut journals = FxHashMap::default();
        journals.insert(case.id.clone(), full[..cut].to_vec());
        t.crash_with(journals).await?;
        let got: Result<Value> = t.result(&case.id).await;
        let now = (case.effects)();
        t.close().await?;
        let at = format!("a crash after step '{step}' ran and before its result was recorded");
        match got {
            Ok(v) if v != expected => {
                return Err(Error::failed(format!(
                    "{at} changed the result: {expected} without a crash, {v} with one"
                )));
            }
            Err(e) => return Err(Error::failed(format!("{at} failed the run: {e}"))),
            Ok(_) => {}
        }
        if now != effects {
            return Err(Error::failed(format!(
                "{at} changed the outside effects: {effects} without a crash, {now} with one. \
                 A step that runs again must not repeat its effect: pass ctx.key() to the \
                 service as an idempotency key"
            )));
        }
    }
    Ok(())
}
