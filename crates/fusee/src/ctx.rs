//! The workflow context: what workflow code uses to make durable progress.
//!
//! Code between `ctx` calls must be deterministic: it is re-run against the
//! journal after a crash. Anything nondeterministic (calling services, the
//! clock, randomness) goes through `ctx` so its result is recorded.

use crate::defs::{AsQuery, AsSignal, AsUpdate, AsWorkflow, Make, Op};
use crate::error::{Error, Result};
use crate::failure::{Failure, IntoFailure};
use crate::limits::{Acquire, Priority};
use crate::runtime::{Host, LocalFuture, ParentClose, RunOutcome};
use fusee_core::record::{Lsn, PAUSED, Payload, RunId, Seq, Status, policy, value_kind};
use fusee_core::run::{AwaitStart, Outcome, RunInfo, StepStart};
use rustc_hash::FxHashMap;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

/// Decides whether a failure may be retried, given the attempt that failed.
pub type RetryIf = Arc<dyn Fn(&Failure, u32) -> bool + Send + Sync>;

/// What a step does when its retries run out on a failure that could be
/// retried: its attempts, or `max_elapsed`, are used up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Exhausted {
    /// The step fails, and with it the run unless the code catches it.
    #[default]
    Fail,
    /// The run pauses: its parent and callers keep waiting until
    /// [`RunHandle::resume`](crate::RunHandle::resume) runs the step again
    /// with a fresh budget, on the code the process has then. A final
    /// failure ([`Failure::fatal`]) still fails at once.
    Pause,
}

/// How a failing step is retried. Attempts include the first one.
///
/// A failure marked final ([`Failure::fatal`], or `retryable: false`) is
/// never retried, whatever the policy says.
#[derive(Clone)]
pub struct RetryPolicy {
    pub attempts: u32,
    pub initial: Duration,
    pub factor: f64,
    pub max: Duration,
    /// Each delay is drawn between half of it and all of it, so retries of
    /// many runs that failed together spread out. On by default. The delay
    /// taken is recorded, so a replay waits for the same time.
    pub jitter: bool,
    /// No retry starts later than this after the step's first failure.
    pub max_elapsed: Option<Duration>,
    /// Failures of these types (or caused by them) are not retried.
    pub non_retryable: Vec<String>,
    /// Called for each failure that could be retried; `false` makes it final.
    pub retry_if: Option<RetryIf>,
    /// When retries run out; `None` takes the engine's
    /// ([`Config::on_exhausted`](crate::Config::on_exhausted), else fail).
    pub on_exhausted: Option<Exhausted>,
}

impl std::fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("attempts", &self.attempts)
            .field("initial", &self.initial)
            .field("factor", &self.factor)
            .field("max", &self.max)
            .field("jitter", &self.jitter)
            .field("max_elapsed", &self.max_elapsed)
            .field("non_retryable", &self.non_retryable)
            .field("retry_if", &self.retry_if.as_ref().map(|_| "fn"))
            .field("on_exhausted", &self.on_exhausted)
            .finish()
    }
}

impl PartialEq for RetryPolicy {
    fn eq(&self, o: &Self) -> bool {
        self.attempts == o.attempts
            && self.initial == o.initial
            && self.factor == o.factor
            && self.max == o.max
            && self.jitter == o.jitter
            && self.max_elapsed == o.max_elapsed
            && self.non_retryable == o.non_retryable
            && self.on_exhausted == o.on_exhausted
            && match (&self.retry_if, &o.retry_if) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
    }
}

impl RetryPolicy {
    /// Run once; a failure is final.
    pub fn none() -> Self {
        Self {
            attempts: 1,
            ..Self::default()
        }
    }

    /// `attempts` tries with exponential backoff from `initial`, capped at `max`.
    pub fn exponential(attempts: u32, initial: Duration, max: Duration) -> Self {
        Self {
            attempts,
            initial,
            max,
            ..Self::default()
        }
    }

    /// `attempts` tries with the default backoff.
    pub fn attempts(attempts: u32) -> Self {
        Self {
            attempts,
            ..Self::default()
        }
    }

    /// Without jitter: each delay is exactly the backoff.
    pub fn no_jitter(mut self) -> Self {
        self.jitter = false;
        self
    }

    /// No retry starts later than `limit` after the first failure.
    pub fn max_elapsed(mut self, limit: Duration) -> Self {
        self.max_elapsed = Some(limit);
        self
    }

    /// Failures of type `kind` (or caused by one) are final.
    pub fn non_retryable(mut self, kind: impl Into<String>) -> Self {
        self.non_retryable.push(kind.into());
        self
    }

    /// `f(failure, attempt)` decides whether each failure is retried.
    pub fn retry_if(mut self, f: impl Fn(&Failure, u32) -> bool + Send + Sync + 'static) -> Self {
        self.retry_if = Some(Arc::new(f));
        self
    }

    /// What the step does when its retries run out ([`Exhausted::Pause`]:
    /// the run waits for a resume).
    pub fn on_exhausted(mut self, then: Exhausted) -> Self {
        self.on_exhausted = Some(then);
        self
    }

    /// Whether `failure`, from attempt `attempt`, may be retried.
    pub fn allows(&self, failure: &Failure, attempt: u32) -> bool {
        attempt < self.attempts && self.retryable(failure, attempt)
    }

    /// Whether `failure` is one the policy retries at all (not final),
    /// whatever attempts remain.
    pub fn retryable(&self, failure: &Failure, attempt: u32) -> bool {
        failure.is_retryable()
            && !self.non_retryable.iter().any(|k| failure.has_kind(k))
            && self.retry_if.as_ref().is_none_or(|f| f(failure, attempt))
    }

    pub fn delay(&self, failed_attempt: u32) -> Duration {
        let exp = self.factor.powi(failed_attempt.saturating_sub(1) as i32);
        let d = self.initial.as_secs_f64() * exp;
        Duration::from_secs_f64(d.min(self.max.as_secs_f64()))
    }
}

impl Default for RetryPolicy {
    /// Three attempts, about 100 ms then 200 ms apart (with jitter).
    fn default() -> Self {
        Self {
            attempts: 3,
            initial: Duration::from_millis(100),
            factor: 2.0,
            max: Duration::from_secs(10),
            jitter: true,
            max_elapsed: None,
            non_retryable: Vec::new(),
            retry_if: None,
            on_exhausted: None,
        }
    }
}

/// What follows a failed attempt.
enum Next {
    /// Another attempt at this time.
    Retry(u64),
    /// Retries ran out on a failure that could be retried.
    Exhausted,
    /// A final failure.
    Final,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StepOptions {
    pub retry: RetryPolicy,
    /// Each attempt fails (and may be retried) if it runs longer than this.
    pub timeout: Option<Duration>,
    /// An attempt fails (and may be retried) if this long passes without a
    /// `ctx.heartbeat` from it.
    pub heartbeat_timeout: Option<Duration>,
    /// Take a permit of this limit before each attempt runs.
    pub limit: Option<String>,
    /// While waiting for the limit's permit: higher levels go first, and
    /// steps of one level take turns by fairness key (see
    /// [`crate::limits`]). Not journaled; the default is arrival order.
    pub priority: Priority,
    /// The step only reads: its function changes nothing outside the
    /// process, so it may run before the records ahead of it are durable
    /// (it skips its gate). A crash can then make it run again with
    /// different inputs, which is harmless for a read and wrong for anything
    /// else. Its result is recorded like any step's, and later steps and
    /// outputs still wait for it to be durable. Ignored by
    /// `await_completion`.
    pub read_only: bool,
    /// A live attempt's result is returned as the step function produced it
    /// instead of decoded from the JSON recorded for it (replay still
    /// decodes). Saves the decode, about 4% of a `memory` step, but code
    /// then sees, for a type whose JSON round trip loses something, what
    /// replay would not give it. Off by default.
    pub own_value: bool,
}

impl StepOptions {
    /// See [`own_value`](field@StepOptions::own_value).
    pub fn own_value(mut self) -> Self {
        self.own_value = true;
        self
    }

    /// See [`read_only`](field@StepOptions::read_only).
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }

    pub fn limit(mut self, name: &str) -> Self {
        self.limit = Some(name.to_owned());
        self
    }

    /// A limit per key: the permits of `name` for this `key` only (a
    /// tenant, an account), each key with the size `engine.limit(name, …)`
    /// gives, made on first use.
    pub fn limit_by(mut self, name: &str, key: &str) -> Self {
        self.limit = Some(format!("{name}/{key}"));
        self
    }

    /// Waits for the limit's permit ahead of steps with a lower level
    /// (default 0).
    pub fn priority(mut self, level: i32) -> Self {
        self.priority.level = level;
        self
    }

    /// Steps waiting for the limit's permit at one level take turns by this
    /// key (a tenant ID, say), so a key with a deep backlog cannot starve
    /// the others.
    pub fn fairness(mut self, key: &str) -> Self {
        self.priority.fairness_key = key.to_owned();
        self
    }

    /// Permits the fairness key takes per turn (default 1).
    pub fn fairness_weight(mut self, weight: u32) -> Self {
        self.priority.fairness_weight = weight;
        self
    }
}

/// How a signal is waited for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WaitOptions {
    /// The wait ends with `None` once this long passes.
    pub timeout: Option<Duration>,
}

impl<M: Make<StepOptions>> Op<StepOptions, M> {
    /// See [`StepOptions::own_value`].
    pub fn own_value(mut self) -> Self {
        self.options_mut().own_value = true;
        self
    }

    /// How failures are retried (default: [`RetryPolicy::default`]).
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.options_mut().retry = retry;
        self
    }

    /// Each attempt fails (and may be retried) if it runs longer than this.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options_mut().timeout = Some(timeout);
        self
    }

    /// An attempt fails if this long passes without a `ctx.heartbeat`.
    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.options_mut().heartbeat_timeout = Some(timeout);
        self
    }

    /// The step only reads: it may run before the records ahead of it are
    /// durable (see [`StepOptions::read_only`](field@StepOptions::read_only)).
    pub fn read_only(mut self) -> Self {
        self.options_mut().read_only = true;
        self
    }

    /// Takes a permit of this limit before each attempt.
    pub fn limit(mut self, name: &str) -> Self {
        self.options_mut().limit = Some(name.to_owned());
        self
    }

    /// Takes a permit of `name` for this `key` only: each key (a tenant, an
    /// account) has its own permits, of the size `engine.limit(name, …)`
    /// gives, made on first use.
    ///
    /// ```ignore
    /// ctx.step("sync", || api.sync(&tenant)).limit_by("tenant", &tenant).await?;
    /// ```
    pub fn limit_by(mut self, name: &str, key: &str) -> Self {
        self.options_mut().limit = Some(format!("{name}/{key}"));
        self
    }

    /// Waits for the limit's permit ahead of steps with a lower level.
    pub fn priority(mut self, level: i32) -> Self {
        self.options_mut().priority.level = level;
        self
    }

    /// Steps waiting at one level take turns by this key.
    pub fn fairness(mut self, key: &str) -> Self {
        self.options_mut().priority.fairness_key = key.to_owned();
        self
    }

    /// Permits the fairness key takes per turn (default 1).
    pub fn fairness_weight(mut self, weight: u32) -> Self {
        self.options_mut().priority.fairness_weight = weight;
        self
    }
}

impl<M: Make<WaitOptions>> Op<WaitOptions, M> {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options_mut().timeout = Some(timeout);
        self
    }
}

impl<M: Make<ChildOptions>> Op<ChildOptions, M> {
    /// What happens to the child when this workflow closes.
    pub fn parent_close(mut self, close: ParentClose) -> Self {
        self.options_mut().close = close;
        self
    }

    /// The child's own deadline.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options_mut().timeout = Some(timeout);
        self
    }
}

/// How a child workflow is started.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChildOptions {
    /// What happens to the child when this workflow closes.
    pub close: ParentClose,
    /// The child's own deadline.
    pub timeout: Option<Duration>,
}

pub type QueryFn = Rc<dyn Fn(Payload) -> std::result::Result<Payload, String>>;
pub type ValidateFn = Rc<dyn Fn(&Payload) -> std::result::Result<(), String>>;
pub type UpdateFn = Rc<dyn Fn(Payload) -> LocalFuture>;

/// Query and update handlers a run's code registered.
#[derive(Default)]
pub struct Handlers {
    queries: RefCell<FxHashMap<String, QueryFn>>,
    updates: RefCell<FxHashMap<String, (ValidateFn, UpdateFn)>>,
}

impl Handlers {
    pub fn query(&self, name: &str) -> Option<QueryFn> {
        self.queries.borrow().get(name).cloned()
    }

    pub fn update(&self, name: &str) -> Option<(ValidateFn, UpdateFn)> {
        self.updates.borrow().get(name).cloned()
    }

    /// Drops every handler. A handler usually holds the run's `Ctx`, which
    /// holds these handlers: the runtime calls this when it drops a run's
    /// code, or the cycle would keep the run (and its host) alive.
    pub(crate) fn clear(&self) {
        let queries = std::mem::take(&mut *self.queries.borrow_mut());
        let updates = std::mem::take(&mut *self.updates.borrow_mut());
        drop(queries);
        drop(updates);
    }
}

/// The context handed to workflow code.
#[derive(Clone)]
pub struct Ctx {
    host: Rc<dyn Host>,
    run: RunId,
    s: Rc<CtxState>,
    handlers: Rc<Handlers>,
    /// The shard's step functions that are pending.
    outside: Rc<Cell<u32>>,
}

/// What every clone of a run's context shares, in one allocation.
struct CtxState {
    info: RunInfo,
    /// The step whose function is being polled.
    current: Cell<Option<Seq>>,
    /// When each running step last reported progress (heartbeat timeouts).
    beats: RefCell<FxHashMap<Seq, u64>>,
    /// Steps to run, newest first, if the workflow fails ([`Ctx::compensate`]).
    compensations: RefCell<Vec<(String, Compensation)>>,
}

type Compensation = Rc<dyn Fn() -> Pin<Box<dyn Future<Output = std::result::Result<(), Failure>>>>>;

/// Encodes into a buffer kept per thread, so a payload costs one
/// allocation (its own) rather than a growing `Vec` and then a copy.
/// Encoded bytes as a payload: `null` (a run or step with no result, `()`
/// or `None`) shares one.
fn payload_of(bytes: &[u8]) -> Payload {
    thread_local! {
        static NULL: Payload = Arc::from(&b"null"[..]);
    }
    if *bytes == *b"null"
        && let Ok(null) = NULL.try_with(Arc::clone)
    {
        return null;
    }
    Arc::from(bytes)
}

/// A duration in milliseconds, saturating: `Duration::MAX` is "never", not
/// the low bits of a number that does not fit.
pub(crate) fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The Unix time `d` after `now_ms`, saturating. A sleep or a timeout of
/// `Duration::MAX` (the usual way to say "forever") is then as late as time
/// goes; added without the check it wrapped to a millisecond before `now`,
/// so the sleep returned and the timeout fired at once (and a debug build
/// panicked).
pub(crate) fn after(now_ms: u64, d: Duration) -> u64 {
    now_ms.saturating_add(millis(d))
}

pub(crate) fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Payload> {
    thread_local! {
        static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    }
    /// A buffer that grew past this is not kept.
    const KEEP: usize = 64 << 10;
    let kept = SCRATCH.try_with(|scratch| {
        let mut buf = scratch.try_borrow_mut().ok()?;
        buf.clear();
        let r = serde_json::to_writer(&mut *buf, value).map(|()| payload_of(&buf));
        if buf.capacity() > KEEP {
            *buf = Vec::new();
        }
        Some(r)
    });
    match kept {
        Ok(Some(r)) => Ok(r?),
        // A `Serialize` impl that encodes a payload of its own, or a thread
        // that is exiting.
        _ => Ok(Arc::from(serde_json::to_vec(value)?)),
    }
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() {
        return Ok(serde_json::from_slice(b"null")?);
    }
    Ok(serde_json::from_slice(bytes)?)
}

fn text(p: &[u8]) -> String {
    String::from_utf8_lossy(p).into_owned()
}

/// A step result that cannot be encoded: final, since every attempt would
/// fail the same way.
/// See [`Ctx::settle`].
enum Attempted<T> {
    Settled(Result<T>),
    Payload(Payload),
}

fn unencodable(e: Error) -> Failure {
    Failure::fatal(format!(
        "the step's result cannot be serialized as JSON ({e}); return a value serde_json can encode"
    ))
    .kind("Codec")
}

/// A number from 0 to 1 for spreading a retry's delay: a hash of the run,
/// the step, the attempt and the time, so a simulation stays reproducible.
fn jitter_fraction(run: RunId, seq: Seq, attempt: u32, now: u64) -> f64 {
    let mut x =
        run.rotate_left(32) ^ (u64::from(seq) << 8) ^ u64::from(attempt) ^ now.rotate_left(17);
    // splitmix64
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    (x >> 11) as f64 / (1u64 << 53) as f64
}

/// Resolves once the runtime delivered operation `seq`.
struct Delivery {
    host: Rc<dyn Host>,
    run: RunId,
    seq: Seq,
    done: bool,
}

impl Future for Delivery {
    type Output = Outcome;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome> {
        match self.host.take_resolution(self.run, self.seq, cx.waker()) {
            Some(o) => {
                self.done = true;
                Poll::Ready(o)
            }
            None => Poll::Pending,
        }
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if !self.done {
            self.host.forget(self.run, self.seq);
        }
    }
}

/// Resolves once a step gated on `gate` may run.
struct Gate {
    host: Rc<dyn Host>,
    run: RunId,
    seq: Seq,
    gate: Lsn,
    /// [`PAUSED`]: until the step is resumed.
    not_before: u64,
    /// The timer for `not_before`, cancelled when the gate is dropped.
    timer: Option<u64>,
}

impl Gate {
    fn new(host: Rc<dyn Host>, run: RunId, seq: Seq, gate: Lsn, not_before: u64) -> Self {
        Self {
            host,
            run,
            seq,
            gate,
            not_before,
            timer: None,
        }
    }
}

impl Future for Gate {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // A paused step waits for its resume, which wakes the run.
        if self.not_before == PAUSED {
            if self.host.step_paused(self.run, self.seq) {
                return Poll::Pending;
            }
            self.not_before = 0;
        }
        // A retry waits out its backoff on one timer, then for the disk.
        if self.not_before > 0 && self.host.now_ms() < self.not_before {
            if self.timer.is_none() {
                self.timer = Some(self.host.wake_at(self.not_before, cx.waker()));
            }
            return Poll::Pending;
        }
        if self
            .host
            .may_run(self.run, self.gate, self.not_before, cx.waker())
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        if let Some(id) = self.timer {
            self.host.cancel_timer(id);
        }
    }
}

/// Resolves at `at` (volatile, for step timeouts).
struct Until {
    host: Rc<dyn Host>,
    at: u64,
    /// Cancelled when the step finishes first.
    timer: Option<u64>,
}

impl Future for Until {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.host.now_ms() >= self.at {
            return Poll::Ready(());
        }
        if self.timer.is_none() {
            self.timer = Some(self.host.wake_at(self.at, cx.waker()));
        }
        Poll::Pending
    }
}

impl Drop for Until {
    fn drop(&mut self) {
        if let Some(id) = self.timer {
            self.host.cancel_timer(id);
        }
    }
}

/// What [`Ctx::await_completion`]'s `send` gets: the token that completes
/// this attempt, and the URL that does it over HTTP when the engine serves
/// its API (`POST` the value as JSON to `url`; `…/v1/fail/<token>` fails it).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    pub token: String,
    pub url: Option<String>,
}

thread_local! {
    /// The URL the engine's HTTP API is reached at, for completion URLs
    /// (set on each shard thread from the engine's config).
    pub(crate) static CALLBACK_BASE: RefCell<Option<Arc<std::sync::RwLock<Option<String>>>>> =
        const { RefCell::new(None) };
}

impl Completion {
    fn new(token: String) -> Self {
        let base = CALLBACK_BASE.with(|b| {
            b.borrow()
                .as_ref()
                .and_then(|b| b.read().ok().and_then(|u| u.clone()))
        });
        let url = base.map(|b| {
            format!(
                "{}/v1/complete/{}",
                b.trim_end_matches('/'),
                crate::http::encode_segment(&token)
            )
        });
        Self { token, url }
    }
}

/// [`Ctx::log`]: a workflow's log.
pub struct WorkflowLog<'a> {
    ctx: &'a Ctx,
}

impl WorkflowLog<'_> {
    pub fn debug(&self, message: impl std::fmt::Display) {
        self.emit(crate::log::Level::Debug, message);
    }

    pub fn info(&self, message: impl std::fmt::Display) {
        self.emit(crate::log::Level::Info, message);
    }

    pub fn warn(&self, message: impl std::fmt::Display) {
        self.emit(crate::log::Level::Warn, message);
    }

    pub fn error(&self, message: impl std::fmt::Display) {
        self.emit(crate::log::Level::Error, message);
    }

    fn emit(&self, level: crate::log::Level, message: impl std::fmt::Display) {
        let c = self.ctx;
        let seq = c.s.current.get();
        // A step's function runs only live; the workflow's own code runs
        // again on every replay.
        if seq.is_none() && c.host.replaying(c.run) {
            return;
        }
        let step = seq.and_then(|s| c.host.op_name(c.run, s));
        let attempt = seq.map(|s| c.host.step_attempt(c.run, s));
        let message = message.to_string();
        crate::log::emit(&crate::log::Record {
            level,
            source: "workflow",
            message: &message,
            workflow_id: Some(&c.s.info.id),
            workflow_type: Some(&c.s.info.wtype),
            run: Some(c.run),
            step: step.as_deref(),
            attempt,
        });
    }
}

/// Polls the step function, where it is, with `ctx.key()` pointing at its
/// step. While the function is pending it counts in `outside`: it waits on
/// something other than the runtime (the shard thread's reactor, another
/// thread).
struct Keyed<'a, F> {
    fut: Pin<&'a mut F>,
    current: &'a Cell<Option<Seq>>,
    outside: &'a Cell<u32>,
    seq: Seq,
    pending: bool,
}

impl<F: Future> Future for Keyed<'_, F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let prev = self.current.replace(Some(self.seq));
        let r = self.fut.as_mut().poll(cx);
        self.current.set(prev);
        let pending = r.is_pending();
        if pending != self.pending {
            self.pending = pending;
            let n = self.outside.get();
            self.outside
                .set(if pending { n + 1 } else { n.saturating_sub(1) });
        }
        r
    }
}

impl<F> Drop for Keyed<'_, F> {
    fn drop(&mut self) {
        if self.pending {
            self.outside.set(self.outside.get().saturating_sub(1));
        }
    }
}

/// Resolves once `seq` has gone `timeout_ms` without a heartbeat.
struct BeatWatch {
    host: Rc<dyn Host>,
    state: Rc<CtxState>,
    seq: Seq,
    timeout_ms: u64,
    /// The deadline the timer is armed for, and the timer.
    armed: Option<(u64, u64)>,
}

impl Future for BeatWatch {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let last = self
            .state
            .beats
            .borrow()
            .get(&self.seq)
            .copied()
            .unwrap_or(0);
        let at = last + self.timeout_ms;
        if self.host.now_ms() >= at {
            return Poll::Ready(());
        }
        if self.armed.is_none_or(|(armed_for, _)| armed_for != at) {
            if let Some((_, id)) = self.armed.take() {
                self.host.cancel_timer(id);
            }
            let id = self.host.wake_at(at, cx.waker());
            self.armed = Some((at, id));
        }
        Poll::Pending
    }
}

impl Drop for BeatWatch {
    fn drop(&mut self) {
        if let Some((_, id)) = self.armed {
            self.host.cancel_timer(id);
        }
    }
}

/// Holds a limit permit for one attempt; returns it when dropped.
struct Permit {
    host: Rc<dyn Host>,
    limit: String,
    ticket: u64,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.host.release(&self.limit, self.ticket);
    }
}

/// Resolves once the permit is granted.
struct AcquirePermit {
    permit: Option<Permit>,
    priority: Priority,
    run: RunId,
}

impl Future for AcquirePermit {
    type Output = Permit;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Permit> {
        let Some(p) = self.permit.as_ref() else {
            return Poll::Pending;
        };
        match p.host.acquire_in(
            self.run,
            &p.limit,
            p.ticket,
            &self.priority,
            Some(cx.waker()),
        ) {
            Acquire::Granted => match self.permit.take() {
                Some(p) => Poll::Ready(p),
                None => Poll::Pending,
            },
            Acquire::Wait { .. } => Poll::Pending,
        }
    }
}

impl Ctx {
    /// `outside` counts the shard's step functions that are pending (see
    /// [`ShardRuntime::waits_outside`](crate::runtime::ShardRuntime::waits_outside)).
    /// The run's input as recorded, for the type-erased code to decode
    /// ([`crate::runtime::WorkflowFn`]).
    #[doc(hidden)]
    pub fn input_bytes<T>(&self, f: impl FnOnce(&[u8]) -> T) -> T {
        f(&self.s.info.input)
    }

    pub(crate) fn new(
        host: Rc<dyn Host>,
        run: RunId,
        info: RunInfo,
        handlers: Rc<Handlers>,
        outside: Rc<Cell<u32>>,
    ) -> Self {
        Self {
            host,
            run,
            s: Rc::new(CtxState {
                info,
                current: Cell::new(None),
                beats: RefCell::new(FxHashMap::default()),
                compensations: RefCell::new(Vec::new()),
            }),
            handlers,
            outside,
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.s.info.id
    }

    pub fn workflow_type(&self) -> &str {
        &self.s.info.wtype
    }

    pub fn version(&self) -> u32 {
        self.s.info.version
    }

    pub fn run_id(&self) -> RunId {
        self.run
    }

    /// When the run started (Unix ms), as recorded.
    pub fn started_ms(&self) -> u64 {
        self.s.info.started_ms
    }

    /// The W3C `traceparent` the run was started under (and its children
    /// join), as recorded: the parent for the spans of the calls its steps
    /// make. `None` when the start was not traced.
    pub fn traceparent(&self) -> Option<&str> {
        self.s
            .info
            .trace
            .as_deref()
            .and_then(|t| std::str::from_utf8(t).ok())
    }

    /// A key stable across retries and recovery for the step whose function
    /// is running. Pass it to services that accept idempotency keys to make
    /// the step's effect exactly once.
    ///
    /// Call it inside a step's function. Outside one there is no step to
    /// key: the run stops (status `stuck`) with a message saying so.
    pub fn key(&self) -> String {
        let seq = self.s.current.get().unwrap_or(Seq::MAX);
        self.host.key(self.run, seq)
    }

    /// Registers a step that undoes what the workflow did so far: if the
    /// workflow fails or is cancelled, every registered compensation runs as
    /// a step named `name`, newest first, then the failure is returned.
    /// Register it after the step it undoes succeeded:
    ///
    /// ```ignore
    /// let hold = ctx.step("reserve", || inventory.reserve(sku)).await?;
    /// ctx.compensate("release", move || inventory.release(hold));
    /// ctx.step("charge", || payments.charge(cents)).await?; // fails: "release" runs
    /// ```
    pub fn compensate<F, Fut, E>(&self, name: &str, f: F)
    where
        F: Fn() -> Fut + 'static,
        Fut: Future<Output = std::result::Result<(), E>> + 'static,
        E: IntoFailure + 'static,
    {
        let f: Compensation = Rc::new(move || {
            let fut = f();
            Box::pin(async move { fut.await.map_err(IntoFailure::into_failure) })
        });
        self.s.compensations.borrow_mut().push((name.to_owned(), f));
    }

    /// Runs the registered compensations, newest first (the workflow
    /// failed). A compensation that fails after its retries is logged and
    /// the others still run.
    pub(crate) async fn run_compensations(&self) {
        loop {
            let next = self.s.compensations.borrow_mut().pop();
            let Some((name, f)) = next else { break };
            let r = self
                .step(&name, || {
                    let f = f.clone();
                    async move { f().await }
                })
                .await;
            if let Err(e) = r {
                self.log()
                    .error(format!("compensation '{name}' failed: {e}"));
            }
        }
    }

    pub(crate) fn has_compensations(&self) -> bool {
        !self.s.compensations.borrow().is_empty()
    }

    /// The workflow's log: records tagged with the workflow ID, run, and
    /// the step whose function is running, routed through [`crate::log`]
    /// (stderr by default). Silent while the run replays its journal, so a
    /// line is written once; not journaled.
    ///
    /// ```ignore
    /// ctx.log().info(format!("charging {cents}"));
    /// ```
    pub fn log(&self) -> WorkflowLog<'_> {
        WorkflowLog { ctx: self }
    }

    fn outcome_to<T: DeserializeOwned>(&self, name: &str, o: Outcome) -> Result<T> {
        match o.status {
            Status::Ok => decode(&o.payload),
            Status::Failed => Err(Error::Step {
                name: name.to_owned(),
                failure: Failure::from_payload(&o.payload),
            }),
            Status::Cancelled => Err(Error::Cancelled(text(&o.payload))),
            Status::TimedOut => Err(Error::TimedOut),
            Status::Terminated | Status::ContinuedAsNew | Status::Reset => Err(Error::Terminated),
        }
    }

    /// What follows `failure` from attempt `attempt`. `first` holds the
    /// time of the step's first failure. Only failures reach here: the
    /// successful path reads no clock.
    fn next_attempt(
        &self,
        policy: &RetryPolicy,
        seq: Seq,
        failure: &Failure,
        attempt: u32,
        first: &mut Option<u64>,
    ) -> Next {
        if !policy.retryable(failure, attempt) {
            return Next::Final;
        }
        match self.retry_at(policy, seq, attempt, first) {
            Some(at) => Next::Retry(at),
            None => Next::Exhausted,
        }
    }

    /// Whether a step whose retries ran out pauses its run.
    fn pauses(&self, policy: &RetryPolicy) -> bool {
        policy
            .on_exhausted
            .unwrap_or_else(|| self.host.on_exhausted())
            == Exhausted::Pause
    }

    /// When to retry a retryable failure from attempt `attempt`, or `None`
    /// once the attempts or `max_elapsed` are used up.
    fn retry_at(
        &self,
        policy: &RetryPolicy,
        seq: Seq,
        attempt: u32,
        first: &mut Option<u64>,
    ) -> Option<u64> {
        if attempt >= policy.attempts {
            return None;
        }
        let now = self.host.now_ms();
        let first = *first.get_or_insert(now);
        let mut delay = millis(policy.delay(attempt));
        if policy.jitter && delay > 1 {
            let half = delay / 2;
            delay = half
                + (jitter_fraction(self.run, seq, attempt, now) * (delay - half) as f64) as u64;
        }
        let at = now.saturating_add(delay);
        if policy
            .max_elapsed
            .is_some_and(|limit| at.saturating_sub(first) > millis(limit))
        {
            return None;
        }
        Some(at)
    }

    /// A successful attempt's value: recorded, and settled at once when its
    /// result is the run's only delivery (then the code reads it from the
    /// bytes just recorded, as a delivery of them would give it: no payload
    /// to allocate, no delivery to wait for); otherwise encoded, for
    /// `step_done`.
    fn settle<T: Serialize + DeserializeOwned>(
        &self,
        seq: Seq,
        opts: &StepOptions,
        v: T,
        kept: &Cell<Option<T>>,
    ) -> std::result::Result<Attempted<T>, Failure> {
        thread_local! {
            static BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }
        // Taken out of the thread's slot, so a `Serialize` impl that runs a
        // step of its own finds a fresh one.
        let mut buf = BUF
            .try_with(|b| {
                b.try_borrow_mut()
                    .map(|mut b| std::mem::take(&mut *b))
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        buf.clear();
        let r = match serde_json::to_writer(&mut buf, &v) {
            Err(e) => Err(unencodable(e.into())),
            Ok(()) if self.host.step_done_inline(self.run, seq, &buf) => Ok(Attempted::Settled(
                // The live result as the function produced it.
                if opts.own_value { Ok(v) } else { decode(&buf) },
            )),
            Ok(()) => {
                if opts.own_value {
                    kept.set(Some(v));
                }
                Ok(Attempted::Payload(payload_of(&buf)))
            }
        };
        if buf.capacity() <= 64 << 10 {
            let _ = BUF.try_with(|b| {
                if let Ok(mut b) = b.try_borrow_mut() {
                    *b = buf;
                }
            });
        }
        r
    }

    /// Runs one attempt of a step function: optional permit, `ctx.key()`
    /// pointing at the step, timeout and heartbeat timeout.
    async fn attempt<T, E, Fut>(
        &self,
        seq: Seq,
        opts: &StepOptions,
        fut: Fut,
        kept: &Cell<Option<T>>,
    ) -> std::result::Result<Attempted<T>, Failure>
    where
        T: Serialize + DeserializeOwned,
        E: IntoFailure,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        // The common step: no permit, timeout or heartbeat to race against.
        if opts.limit.is_none() && opts.timeout.is_none() && opts.heartbeat_timeout.is_none() {
            let fut = std::pin::pin!(fut);
            let keyed = Keyed {
                fut,
                current: &self.s.current,
                outside: &self.outside,
                seq,
                pending: false,
            };
            return match keyed.await {
                Ok(v) => self.settle(seq, opts, v, kept),
                Err(e) => Err(e.into_failure()),
            };
        }
        let _permit = match &opts.limit {
            Some(limit) => Some(
                AcquirePermit {
                    permit: Some(Permit {
                        host: self.host.clone(),
                        limit: limit.clone(),
                        ticket: self.host.limit_ticket(),
                    }),
                    priority: opts.priority.clone(),
                    run: self.run,
                }
                .await,
            ),
            None => None,
        };
        // Only a heartbeat timeout reads when the step last reported.
        let watched = opts.heartbeat_timeout.is_some();
        if watched {
            self.s.beats.borrow_mut().insert(seq, self.host.now_ms());
        }
        let fut = std::pin::pin!(fut);
        let keyed = Keyed {
            fut,
            current: &self.s.current,
            outside: &self.outside,
            seq,
            pending: false,
        };
        let deadline = opts.timeout.map(|t| after(self.host.now_ms(), t));
        let watch = opts.heartbeat_timeout.map(|t| BeatWatch {
            host: self.host.clone(),
            state: self.s.clone(),
            seq,
            timeout_ms: millis(t),
            armed: None,
        });
        let raced = Race3 {
            fut: keyed,
            until: deadline.map(|at| Until {
                host: self.host.clone(),
                at,
                timer: None,
            }),
            watch,
        }
        .await;
        if watched {
            self.s.beats.borrow_mut().remove(&seq);
        }
        match raced {
            Raced::Done(Ok(v)) => self.settle(seq, opts, v, kept),
            Raced::Done(Err(e)) => Err(e.into_failure()),
            Raced::TimedOut => Err(Failure::new(format!(
                "step timed out after {} ms",
                opts.timeout.map(|t| t.as_millis()).unwrap_or(0)
            ))
            .kind("TimedOut")),
            Raced::Silent => Err(Failure::new(format!(
                "step sent no heartbeat for {} ms",
                opts.heartbeat_timeout.map(|t| t.as_millis()).unwrap_or(0)
            ))
            .kind("TimedOut")),
        }
    }

    /// Runs `f` as a durable step; options by builder, then `.await`:
    ///
    /// ```ignore
    /// let charge: Charge = ctx
    ///     .step("charge", || payments::charge(&quote, ctx.key()))
    ///     .retry(RetryPolicy::attempts(5))
    ///     .timeout(Duration::from_secs(10))
    ///     .await?;
    /// ```
    ///
    /// `f` runs only once every earlier record of this run is durable, so a
    /// crash can never take back an input it saw (unless the step is
    /// [`read_only`](Op::read_only), which runs at once). Its result is
    /// recorded and returned without waiting for the disk. On replay the
    /// recorded result is returned and `f` does not run. Its error is
    /// anything [`IntoFailure`]: a [`Failure`] it chose, Fusee's [`Error`],
    /// or any error type.
    pub fn step<'a, T, E, F, Fut>(
        &'a self,
        name: &'a str,
        f: F,
    ) -> Op<
        StepOptions,
        impl Make<StepOptions, Output = Result<T>, Fut = impl Future<Output = Result<T>> + 'a> + 'a,
    >
    where
        T: Serialize + DeserializeOwned + 'a,
        E: IntoFailure + 'a,
        F: FnMut() -> Fut + 'a,
        Fut: Future<Output = std::result::Result<T, E>> + 'a,
    {
        Op::new(StepOptions::default(), move |opts| {
            self.run_step(name, opts, f)
        })
    }

    async fn run_step<T, E, F, Fut>(&self, name: &str, opts: StepOptions, mut f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        E: IntoFailure,
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        let (seq, start, mut gate) = self.host.begin_step(self.run, name)?;
        // The live result, kept with `own_value`.
        let kept: Cell<Option<T>> = Cell::new(None);
        // A read-only step waits for no earlier record (gate 0 is always durable).
        let gated = !opts.read_only;
        if !gated {
            gate = 0;
        }
        let mut delivery = Delivery {
            host: self.host.clone(),
            run: self.run,
            seq,
            done: false,
        };
        if let StepStart::Run {
            attempt,
            not_before_ms,
        } = start
        {
            let mut attempt = attempt;
            let mut not_before = not_before_ms;
            let mut first = None;
            loop {
                Gate::new(self.host.clone(), self.run, seq, gate, not_before).await;
                if not_before == PAUSED {
                    if !self.host.step_pending(self.run, seq) {
                        // Resolved while paused (cancelled): no attempt.
                        break;
                    }
                    // Resumed: a fresh budget, counted from here.
                    attempt = 1;
                    first = None;
                }
                // The test kit's mocks replace the function (builds with the
                // `testing` feature only).
                #[cfg(feature = "testing")]
                let mocked = crate::testing::mocked(&self.s.info.wtype, name, attempt);
                #[cfg(not(feature = "testing"))]
                let mocked: Option<std::result::Result<Payload, Failure>> = None;
                let result = match mocked {
                    Some(r) => r.map(Attempted::Payload),
                    None => self.attempt(seq, &opts, async { f().await }, &kept).await,
                };
                match result {
                    Ok(Attempted::Settled(r)) => {
                        // Recorded and settled: nothing left to deliver.
                        delivery.done = true;
                        return r;
                    }
                    Ok(Attempted::Payload(payload)) => {
                        self.host.step_done(self.run, seq, Status::Ok, payload);
                        break;
                    }
                    Err(failure) => {
                        let next =
                            self.next_attempt(&opts.retry, seq, &failure, attempt, &mut first);
                        let at = match next {
                            Next::Retry(at) => Some(at),
                            Next::Exhausted if self.pauses(&opts.retry) => Some(PAUSED),
                            _ => None,
                        };
                        match at {
                            Some(at) => {
                                let retry_gate = self.host.step_retry(
                                    self.run,
                                    seq,
                                    at,
                                    failure.to_payload(),
                                )?;
                                gate = if gated { retry_gate } else { 0 };
                                not_before = at;
                                attempt += 1;
                            }
                            None => {
                                self.host.step_done(
                                    self.run,
                                    seq,
                                    Status::Failed,
                                    failure.to_payload(),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }
        let outcome = (&mut delivery).await;
        // The live result as the function produced it.
        if outcome.status == Status::Ok
            && let Some(v) = kept.take()
        {
            return Ok(v);
        }
        self.outcome_to(name, outcome)
    }

    /// A step completed from outside the workflow. `send` receives a
    /// completion token, starts work elsewhere (an approval email, a job on
    /// another system) and returns; the workflow then waits until a process
    /// holding the token calls `engine.complete(token, result)` or
    /// `engine.fail(token, error)`. `send` is retried under the retry
    /// policy; each attempt gets a new token and older ones are refused.
    pub fn await_completion<'a, T, E, F, Fut>(
        &'a self,
        name: &'a str,
        send: F,
    ) -> Op<
        StepOptions,
        impl Make<StepOptions, Output = Result<T>, Fut = impl Future<Output = Result<T>> + 'a> + 'a,
    >
    where
        T: DeserializeOwned + 'a,
        E: IntoFailure + 'a,
        F: FnMut(Completion) -> Fut + 'a,
        Fut: Future<Output = std::result::Result<(), E>> + 'a,
    {
        Op::new(StepOptions::default(), move |opts| {
            self.run_await_completion(name, opts, send)
        })
    }

    async fn run_await_completion<T, E, F, Fut>(
        &self,
        name: &str,
        opts: StepOptions,
        mut send: F,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        E: IntoFailure,
        F: FnMut(Completion) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
    {
        let (seq, start, mut gate) = self.host.begin_await(self.run, name)?;
        let mut delivery = Delivery {
            host: self.host.clone(),
            run: self.run,
            seq,
            done: false,
        };
        if let AwaitStart::Run {
            attempt,
            not_before_ms,
        } = start
        {
            let mut attempt = attempt;
            let mut not_before = not_before_ms;
            let mut first = None;
            loop {
                Gate::new(self.host.clone(), self.run, seq, gate, not_before).await;
                let token = self.host.completion_token(self.run, seq)?;
                let completion = Completion::new(token);
                let result = self
                    .attempt(
                        seq,
                        &opts,
                        async { send(completion).await.map(|()| true) },
                        &Cell::new(None),
                    )
                    .await;
                match result {
                    Ok(_) => {
                        self.host.awaiting(self.run, seq)?;
                        break;
                    }
                    Err(failure) => {
                        let at = match self.next_attempt(
                            &opts.retry,
                            seq,
                            &failure,
                            attempt,
                            &mut first,
                        ) {
                            Next::Retry(at) => Some(at),
                            Next::Exhausted | Next::Final => None,
                        };
                        match at {
                            Some(at) => {
                                gate = self.host.step_retry(
                                    self.run,
                                    seq,
                                    at,
                                    failure.to_payload(),
                                )?;
                                not_before = at;
                                attempt += 1;
                            }
                            None => {
                                self.host.step_done(
                                    self.run,
                                    seq,
                                    Status::Failed,
                                    failure.to_payload(),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }
        let outcome = (&mut delivery).await;
        self.outcome_to(name, outcome)
    }

    /// Reports progress from inside a running step. Coalesced (only the
    /// latest is kept) and never waits for the disk. A step retried after a
    /// crash receives its last durable progress through [`progress`](Self::progress).
    pub fn heartbeat<T: Serialize>(&self, progress: &T) -> Result<()> {
        let Some(seq) = self.s.current.get() else {
            return Err(Error::Rejected(
                "ctx.heartbeat() was called outside a step's function: call it inside ctx.step(name, || ...)".into(),
            ));
        };
        // A step without a heartbeat timeout has no entry to refresh.
        if let Some(last) = self.s.beats.borrow_mut().get_mut(&seq) {
            *last = self.host.now_ms();
        }
        self.host.heartbeat(self.run, seq, encode(progress)?)
    }

    /// Opens the run's stream `name` for appending (see [`crate::stream`]).
    /// In workflow code it opens a stream of the run: its appends are
    /// recorded once (a replay appends nothing) and `close()` or the run's
    /// end completes it; steps may append to it too. Inside a step's
    /// function it opens a stream of the step, which the step's final
    /// result completes; an attempt that runs again finds what earlier
    /// attempts recorded in [`StreamWriter::recorded`] and continues after
    /// it. Readers follow a stream with [`crate::RunHandle::stream`].
    ///
    /// [`StreamWriter::recorded`]: crate::stream::StreamWriter::recorded
    pub fn stream(&self, name: &str) -> Result<crate::stream::StreamWriter> {
        crate::stream::StreamWriter::open(self, name)
    }

    pub(crate) fn host_ref(&self) -> &Rc<dyn Host> {
        &self.host
    }

    /// The step whose function is being polled, if any.
    pub(crate) fn running_step(&self) -> Option<Seq> {
        self.s.current.get()
    }

    /// The latest progress the running step reported, in this attempt or an
    /// earlier one (including one before a crash).
    pub fn progress<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        let Some(seq) = self.s.current.get() else {
            return Ok(None);
        };
        match self.host.progress(self.run, seq) {
            Some(p) => decode(&p).map(Some),
            None => Ok(None),
        }
    }

    /// Registers a read-only query handler: `run.query(name, arg)` calls it
    /// against the run's state. It must not change state or call `ctx`.
    pub fn on_query<A, R, F>(&self, query: impl AsQuery<A, R>, f: F)
    where
        A: DeserializeOwned,
        R: Serialize,
        F: Fn(A) -> R + 'static,
    {
        let name = query.name();
        let erased: QueryFn = Rc::new(move |arg: Payload| {
            let a: A = decode(&arg).map_err(|e| e.to_string())?;
            encode(&f(a)).map_err(|e| e.to_string())
        });
        self.handlers
            .queries
            .borrow_mut()
            .insert(name.to_owned(), erased);
    }

    /// Registers an update. `validate` runs first, against the current
    /// state; a rejection leaves no record. An accepted update is recorded
    /// with its ID and `handle` runs (it may run steps); the caller gets its
    /// result once it is durable.
    pub fn on_update<A, R, V, H, Fut>(&self, update: impl AsUpdate<A, R>, validate: V, handle: H)
    where
        A: DeserializeOwned + 'static,
        R: Serialize + 'static,
        V: Fn(&A) -> std::result::Result<(), String> + 'static,
        H: Fn(A) -> Fut + 'static,
        Fut: Future<Output = Result<R>> + 'static,
    {
        let name = update.name();
        let v: ValidateFn = Rc::new(move |arg: &Payload| {
            let a: A = decode(arg).map_err(|e| e.to_string())?;
            validate(&a)
        });
        let handle = Rc::new(handle);
        let h: UpdateFn = Rc::new(move |arg: Payload| {
            let handle = handle.clone();
            Box::pin(async move {
                let a: A = decode(&arg).map_err(crate::engine::outcome_of)?;
                let r = handle(a).await.map_err(crate::engine::outcome_of)?;
                encode(&r).map_err(crate::engine::outcome_of)
            }) as Pin<Box<dyn Future<Output = RunOutcome>>>
        });
        self.handlers
            .updates
            .borrow_mut()
            .insert(name.to_owned(), (v, h));
    }

    /// [`on_update`](Self::on_update) with the payloads as they are.
    pub(crate) fn on_update_raw(&self, name: &str, validate: ValidateFn, handle: UpdateFn) {
        self.handlers
            .updates
            .borrow_mut()
            .insert(name.to_owned(), (validate, handle));
    }

    /// [`on_query`](Self::on_query) with the payloads as they are.
    pub(crate) fn on_query_raw(&self, name: &str, f: QueryFn) {
        self.handlers
            .queries
            .borrow_mut()
            .insert(name.to_owned(), f);
    }

    /// Resolves once operation `seq` is delivered.
    pub(crate) fn delivery(&self, seq: Seq) -> impl Future<Output = Outcome> + '_ {
        Delivery {
            host: self.host.clone(),
            run: self.run,
            seq,
            done: false,
        }
    }

    /// Key `key` of object type `wtype`, to call from this workflow (or
    /// object handler): its calls are journal operations, sent through the
    /// durable outbox and run exactly once (see [`crate::object`]).
    pub fn object(&self, wtype: &str, key: &str) -> crate::object::ObjectCall<'_> {
        crate::object::ObjectCall {
            ctx: self,
            wtype: wtype.to_owned(),
            key: key.to_owned(),
        }
    }

    /// Ends this run and starts a new run of the same workflow ID with
    /// `input` and an empty journal, in one record. Signals and updates that
    /// arrive afterwards go to the new run. Never returns.
    pub async fn continue_as_new<I: Serialize, T>(&self, input: &I) -> Result<T> {
        self.host.continue_as_new(self.run, encode(input)?, None)?;
        std::future::pending().await
    }

    /// Journal operations this run has made so far. Deterministic, so a
    /// long-lived workflow can use it to decide when to continue as new.
    pub fn journal_size(&self) -> u32 {
        self.host.journal_size(self.run)
    }

    /// Lets running workflows pick up new code at this point: true for new
    /// runs and for running ones the first time they get here after the
    /// deploy; false while replaying a journal that passed this point before
    /// the patch existed. The decision is recorded.
    pub fn patched(&self, id: &str) -> Result<bool> {
        self.host.patched(self.run, id, false)
    }

    /// Marks a patch whose old branch has been removed: every running
    /// workflow has passed it. Safe to delete later.
    pub fn deprecate_patch(&self, id: &str) -> Result<()> {
        self.host.patched(self.run, id, true).map(|_| ())
    }

    /// Records search attributes (an object; `null` removes a key). They
    /// stay readable when payloads are encrypted, so do not put secrets in
    /// them. `engine.list_where` filters on them.
    pub fn set_attributes<T: Serialize>(&self, attributes: &T) -> Result<()> {
        let v = serde_json::to_value(attributes)?;
        if !v.is_object() {
            return Err(Error::Codec("search attributes are a JSON object".into()));
        }
        self.host
            .set_attributes(self.run, Arc::from(serde_json::to_vec(&v)?))
    }

    /// Asks another workflow to cancel, once this point is durable.
    pub fn cancel_workflow(&self, target: &str, reason: &str) -> Result<()> {
        self.host.cancel_workflow(self.run, target, reason)
    }

    /// Sleeps durably: the wake-up time is recorded, survives restarts, and
    /// the run is parked out of memory while it waits.
    pub async fn sleep(&self, name: &str, duration: Duration) -> Result<()> {
        let at = after(self.host.now_ms(), duration);
        self.sleep_until(name, at).await
    }

    /// Sleeps until the Unix time `at_ms`.
    pub async fn sleep_until(&self, name: &str, at_ms: u64) -> Result<()> {
        let seq = self.host.begin_sleep(self.run, name, at_ms)?;
        let o = Delivery {
            host: self.host.clone(),
            run: self.run,
            seq,
            done: false,
        }
        .await;
        match o.status {
            Status::Ok => Ok(()),
            Status::Cancelled => Err(Error::Cancelled(text(&o.payload))),
            _ => Err(Error::Failed(Failure::from_payload(&o.payload))),
        }
    }

    /// Waits for the next signal; `None` once `.timeout(d)` passes:
    /// `ctx.wait_for(REVIEW).timeout(DAY).await?`.
    pub fn wait_for<'a, T: DeserializeOwned + 'a>(
        &'a self,
        signal: impl AsSignal<T> + 'a,
    ) -> Op<
        WaitOptions,
        impl Make<
            WaitOptions,
            Output = Result<Option<T>>,
            Fut = impl Future<Output = Result<Option<T>>> + 'a,
        > + 'a,
    > {
        Op::new(WaitOptions::default(), move |o: WaitOptions| async move {
            self.run_wait_for(signal.name(), o.timeout).await
        })
    }

    async fn run_wait_for<T: DeserializeOwned>(
        &self,
        name: &str,
        timeout: Option<Duration>,
    ) -> Result<Option<T>> {
        let deadline = timeout.map(|t| after(self.host.now_ms(), t)).unwrap_or(0);
        let seq = self.host.begin_wait(self.run, name, deadline)?;
        let o = Delivery {
            host: self.host.clone(),
            run: self.run,
            seq,
            done: false,
        }
        .await;
        match o.status {
            Status::Ok => decode(&o.payload).map(Some),
            Status::TimedOut => Ok(None),
            Status::Cancelled => Err(Error::Cancelled(text(&o.payload))),
            _ => Err(Error::Failed(Failure::from_payload(&o.payload))),
        }
    }

    /// Starts a child workflow and waits for its result. The child's ID
    /// makes the start idempotent. Options by builder: `.timeout(d)`,
    /// `.parent_close(p)`.
    pub fn child<'a, I: Serialize + 'a, O: DeserializeOwned + 'a>(
        &'a self,
        workflow: impl AsWorkflow<I, O> + 'a,
        id: &'a str,
        input: &'a I,
    ) -> Op<
        ChildOptions,
        impl Make<ChildOptions, Output = Result<O>, Fut = impl Future<Output = Result<O>> + 'a> + 'a,
    > {
        Op::new(ChildOptions::default(), move |o| async move {
            self.run_child(workflow.name(), id, input, o).await
        })
    }

    pub(crate) async fn run_child<I: Serialize, O: DeserializeOwned>(
        &self,
        wtype: &str,
        id: &str,
        input: &I,
        opts: ChildOptions,
    ) -> Result<O> {
        let input = encode(input)?;
        let deadline = opts
            .timeout
            .map(|t| after(self.host.now_ms(), t))
            .unwrap_or(0);
        let Some(seq) =
            self.host
                .begin_child(self.run, wtype, id, opts.close.bits(), deadline, input)?
        else {
            return Err(Error::failed("child did not register"));
        };
        let o = Delivery {
            host: self.host.clone(),
            run: self.run,
            seq,
            done: false,
        }
        .await;
        match o.status {
            Status::Ok => decode(&o.payload),
            Status::Cancelled => Err(Error::Cancelled(text(&o.payload))),
            Status::TimedOut => Err(Error::TimedOut),
            Status::Terminated | Status::ContinuedAsNew | Status::Reset => Err(Error::Terminated),
            Status::Failed => Err(Error::Child {
                id: id.to_owned(),
                failure: Failure::from_payload(&o.payload),
            }),
        }
    }

    /// Starts a child workflow without waiting for its result (await the
    /// start). It is terminated when this workflow closes; with
    /// `.parent_close(ParentClose::Abandon)` it outlives the parent.
    pub fn start_child<'a, I: Serialize + 'a>(
        &'a self,
        workflow: impl crate::defs::NamesWorkflow<I> + 'a,
        id: &'a str,
        input: &'a I,
    ) -> Op<
        ChildOptions,
        impl Make<ChildOptions, Output = Result<()>, Fut = impl Future<Output = Result<()>> + 'a> + 'a,
    > {
        Op::new(ChildOptions::default(), move |o| async move {
            self.run_start_child(workflow.name(), id, input, o)
        })
    }

    pub(crate) fn run_start_child<I: Serialize>(
        &self,
        wtype: &str,
        id: &str,
        input: &I,
        opts: ChildOptions,
    ) -> Result<()> {
        let input = encode(input)?;
        let deadline = opts
            .timeout
            .map(|t| after(self.host.now_ms(), t))
            .unwrap_or(0);
        self.host.begin_child(
            self.run,
            wtype,
            id,
            opts.close.bits() | policy::DETACHED,
            deadline,
            input,
        )?;
        Ok(())
    }

    /// Sends a signal to another workflow once this run's journal is
    /// durable through this point.
    pub fn signal_workflow<T: Serialize>(
        &self,
        id: &str,
        signal: impl AsSignal<T>,
        value: &T,
    ) -> Result<()> {
        self.host.send(self.run, id, signal.name(), encode(value)?)
    }

    /// The current time, recorded: replay returns the same instant.
    pub fn now_ms(&self) -> Result<u64> {
        let v = self.host.value(self.run, value_kind::NOW, &mut |now, _| {
            Arc::from(&now.to_le_bytes()[..])
        })?;
        let mut b = [0u8; 8];
        if v.len() == 8 {
            b.copy_from_slice(&v);
        }
        Ok(u64::from_le_bytes(b))
    }

    /// A random value, recorded.
    pub fn random(&self) -> Result<u64> {
        let v = self.host.value(self.run, value_kind::RANDOM, &mut |_, r| {
            Arc::from(&r.to_le_bytes()[..])
        })?;
        let mut b = [0u8; 8];
        if v.len() == 8 {
            b.copy_from_slice(&v);
        }
        Ok(u64::from_le_bytes(b))
    }

    /// A random (version 4) UUID, recorded.
    pub fn uuid(&self) -> Result<String> {
        let v = self.host.value(self.run, value_kind::UUID, &mut |now, r| {
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&r.to_le_bytes());
            let mix = r.rotate_left(29) ^ now.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            b[8..].copy_from_slice(&mix.to_le_bytes());
            b[6] = (b[6] & 0x0f) | 0x40;
            b[8] = (b[8] & 0x3f) | 0x80;
            Arc::from(&b[..])
        })?;
        let h: String = v.iter().map(|b| format!("{b:02x}")).collect();
        if h.len() != 32 {
            return Err(Error::Codec("recorded uuid has the wrong length".into()));
        }
        Ok(format!(
            "{}-{}-{}-{}-{}",
            &h[0..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..32]
        ))
    }

    /// Runs futures concurrently and returns all results in argument order.
    ///
    /// A future is polled again only when its waker fires (or the run is
    /// woken as a whole), so a fan-out costs time linear in its width; a
    /// future that can make progress must wake its waker, as the `Future`
    /// contract requires.
    pub async fn all<F: Future>(&self, futures: impl IntoIterator<Item = F>) -> Vec<F::Output> {
        let items: Vec<_> = futures
            .into_iter()
            .map(|f| JoinItem::Pending(Box::pin(f)))
            .collect();
        JoinAll {
            pending: items.len(),
            branches: Branches::new(self.host.clone(), self.run, items.len()),
            items,
        }
        .await
    }

    /// Runs futures concurrently; returns the index and result of the first
    /// to finish and drops the rest. Which one wins is recorded by the order
    /// of deliveries in the journal, so replay picks the same winner.
    /// Futures are polled as in [`all`](Self::all).
    pub async fn race<F: Future>(
        &self,
        futures: impl IntoIterator<Item = F>,
    ) -> (usize, F::Output) {
        let items: Vec<_> = futures.into_iter().map(|f| Box::pin(f)).collect();
        Race {
            branches: Branches::new(self.host.clone(), self.run, items.len()),
            items,
        }
        .await
    }
}

enum Raced<T> {
    Done(T),
    TimedOut,
    Silent,
}

/// A step attempt raced against its timeout and its heartbeat timeout.
struct Race3<F> {
    fut: F,
    until: Option<Until>,
    watch: Option<BeatWatch>,
}

impl<F: Future + Unpin> Future for Race3<F> {
    type Output = Raced<F::Output>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(v) = Pin::new(&mut self.fut).poll(cx) {
            return Poll::Ready(Raced::Done(v));
        }
        if let Some(u) = self.until.as_mut()
            && Pin::new(u).poll(cx).is_ready()
        {
            return Poll::Ready(Raced::TimedOut);
        }
        if let Some(w) = self.watch.as_mut()
            && Pin::new(w).poll(cx).is_ready()
        {
            return Poll::Ready(Raced::Silent);
        }
        Poll::Pending
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Branches of a combinator whose wakers fired since it last polled them.
#[derive(Default)]
struct Woken {
    branches: Mutex<BTreeSet<usize>>,
    /// The combinator's own waker: a branch's wake is passed on to it.
    parent: Mutex<Option<Waker>>,
}

struct BranchWaker {
    woken: Arc<Woken>,
    index: usize,
}

impl Wake for BranchWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        lock(&self.woken.branches).insert(self.index);
        let parent = lock(&self.woken.parent).clone();
        if let Some(p) = parent {
            p.wake();
        }
    }
}

/// Which branches a combinator polls: all of them on its first poll and
/// after a wake that names no future ([`Host::broadcasts`]), otherwise only
/// those whose wakers fired (a delivery wakes the future it resolves).
/// Either way in index order, and a branch woken during a pass is polled in
/// that pass if the pass has not reached it yet, so code makes its calls in
/// the same order as when every branch is polled on every wake, while a
/// fan-out of n branches costs O(n) branch polls instead of O(n²).
struct Branches {
    host: Rc<dyn Host>,
    run: RunId,
    woken: Arc<Woken>,
    wakers: Vec<Waker>,
    /// The broadcast count at the last poll (`None` before the first).
    seen: Option<u64>,
}

impl Branches {
    fn new(host: Rc<dyn Host>, run: RunId, n: usize) -> Self {
        let woken = Arc::new(Woken::default());
        let wakers = (0..n)
            .map(|index| {
                Waker::from(Arc::new(BranchWaker {
                    woken: woken.clone(),
                    index,
                }))
            })
            .collect();
        Self {
            host,
            run,
            woken,
            wakers,
            seen: None,
        }
    }

    /// Starts a pass. Returns whether it polls every branch.
    fn begin(&mut self, cx: &Context<'_>) -> bool {
        {
            let mut parent = lock(&self.woken.parent);
            if !parent.as_ref().is_some_and(|p| p.will_wake(cx.waker())) {
                *parent = Some(cx.waker().clone());
            }
        }
        let now = self.host.broadcasts(self.run);
        let all = self.seen != Some(now);
        self.seen = Some(now);
        if all {
            lock(&self.woken.branches).clear();
        }
        all
    }

    /// The next branch from index `from` on that this pass polls.
    fn next(&self, all: bool, from: usize) -> Option<usize> {
        if all {
            return (from < self.wakers.len()).then_some(from);
        }
        let mut woken = lock(&self.woken.branches);
        let i = woken.range(from..).next().copied()?;
        woken.remove(&i);
        Some(i)
    }
}

enum JoinItem<F: Future> {
    Pending(Pin<Box<F>>),
    Done(Option<F::Output>),
}

struct JoinAll<F: Future> {
    items: Vec<JoinItem<F>>,
    branches: Branches,
    pending: usize,
}

// Each future is pinned in its own box; outputs are plain values.
impl<F: Future> Unpin for JoinAll<F> {}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let all = this.branches.begin(cx);
        // With several branches unfinished, their steps' results are
        // delivered after this pass, in order, not settled inside it: a
        // result settled here would be the run's newest record, and every
        // branch that starts its step after it in the pass would wait for
        // that record's flush (a fan-out's legs, one flush each, instead of
        // one flush for all of them).
        let held = (this.pending > 1).then(|| this.branches.host.hold_inline());
        let mut from = 0;
        while let Some(i) = this.branches.next(all, from) {
            from = i + 1;
            let item = &mut this.items[i];
            if let JoinItem::Pending(f) = item
                && let Poll::Ready(v) = f
                    .as_mut()
                    .poll(&mut Context::from_waker(&this.branches.wakers[i]))
            {
                *item = JoinItem::Done(Some(v));
                this.pending -= 1;
            }
        }
        if let Some(held) = held {
            this.branches.host.resume_inline(held);
        }
        if this.pending > 0 {
            return Poll::Pending;
        }
        Poll::Ready(
            this.items
                .iter_mut()
                .filter_map(|i| match i {
                    JoinItem::Done(v) => v.take(),
                    JoinItem::Pending(_) => None,
                })
                .collect(),
        )
    }
}

struct Race<F> {
    items: Vec<Pin<Box<F>>>,
    branches: Branches,
}

impl<F: Future> Future for Race<F> {
    type Output = (usize, F::Output);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let all = this.branches.begin(cx);
        let mut from = 0;
        while let Some(i) = this.branches.next(all, from) {
            from = i + 1;
            let mut branch = Context::from_waker(&this.branches.wakers[i]);
            if let Poll::Ready(v) = this.items[i].as_mut().poll(&mut branch) {
                return Poll::Ready((i, v));
            }
        }
        Poll::Pending
    }
}
