//! The server's invoker: which shards are due, which endpoint gets them,
//! and the HTTP requests, with their concurrency limits, backoff and
//! fencing.
use super::{Endpoint, Request, http, trace};
use crate::server::Server;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Why a shard is due, as the request's `reasons` names them.
const CALL: u8 = 1;
const MESSAGE: u8 = 2;
const TIMER: u8 = 4;
const RECOVERY: u8 = 8;
const HANDBACK: u8 = 16;
const CODE: u8 = 32;
const REASONS: [(u8, &str); 6] = [
    (CALL, "call"),
    (MESSAGE, "message"),
    (TIMER, "timer"),
    (RECOVERY, "recovery"),
    (HANDBACK, "handback"),
    (CODE, "code"),
];

/// After a 2xx, how long the invocation's connection may take to close by
/// itself before it is revoked.
const CLOSE_GRACE: Duration = Duration::from_secs(2);

/// `base`, doubling per failure in a row up to `max`, with a fifth of jitter.
fn backoff(failures: u32, base: Duration, max: Duration, rng: &mut u64) -> Duration {
    let exp = base.saturating_mul(1u32 << failures.saturating_sub(1).min(20));
    let d = exp.min(max);
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    let jitter = (*rng % 400) as f64 / 1000.0 - 0.2;
    d.mul_f64(1.0 + jitter)
}

/// What an `IDLE` frame said about one shard.
#[derive(Clone, Debug, Default)]
pub(crate) struct IdleEntry {
    pub(crate) shard: u32,
    pub(crate) epoch: u64,
    pub(crate) flags: u8,
    pub(crate) due_in_ms: u64,
    pub(crate) steps: u64,
    pub(crate) open_us: u64,
    pub(crate) types: Vec<String>,
}

#[derive(Default)]
struct ShardWake {
    /// When the shard has work by itself (Unix ms); `None`: only outside
    /// events wake it.
    due: Option<u64>,
    reasons: u8,
    /// Failures in a row (crashes, invocations without progress), and the
    /// earliest next invocation.
    failures: u32,
    not_before: u64,
    /// The invocation in flight for this shard.
    invoking: Option<u64>,
    last_endpoint: Option<usize>,
    /// Types the last holder had runs of and no code for.
    awaiting: Vec<String>,
    /// The last holder's report: `(connection, entry)`.
    report: Option<(u64, IdleEntry)>,
}

struct Invocation {
    endpoint: usize,
    shards: Vec<u32>,
    conn: Option<u64>,
    leased: bool,
    started: Instant,
    budget: Duration,
}

#[derive(Default)]
struct EndpointState {
    in_flight: u32,
    failures: u32,
    not_before: u64,
    m: EndpointMetrics,
    logged: Option<Instant>,
}

struct State {
    shards: Vec<ShardWake>,
    invocations: HashMap<u64, Invocation>,
    /// Connection to the invocation it answers and that invocation's
    /// endpoint (kept until the lease ends, which may be after the answer).
    conns: HashMap<u64, (u64, usize)>,
    endpoints: Vec<EndpointState>,
    rng: u64,
    handbacks: u64,
    crashes: u64,
    no_progress: u64,
}

/// One endpoint's counters.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointMetrics {
    pub url: String,
    pub types: Vec<String>,
    /// Invocations sent, and how they ended: a 2xx after which the
    /// invocation's shards were leased, an error or a status other than
    /// 2xx, no answer within the budget plus 30 s, or a 2xx without a lease.
    pub invocations: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub unleased: u64,
    pub in_flight: u32,
    pub failures_in_a_row: u32,
    /// How long until the endpoint is invoked again, while it backs off.
    pub backoff_ms: u64,
    /// Sent to answered, summed and at most.
    pub total_ms: u64,
    pub max_ms: u64,
    /// Shards invoked, steps they ran, and their engine opens (in µs).
    pub shards: u64,
    pub steps: u64,
    pub open_us_total: u64,
    pub opens: u64,
    pub last_error: Option<String>,
}

/// The invoker's counters ([`Server::invocation_metrics`]).
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvokeMetrics {
    pub endpoints: Vec<EndpointMetrics>,
    /// Shards due now and waiting for an invocation slot or backoff.
    pub due_shards: u32,
    /// Shards with an invocation in flight.
    pub invoking_shards: u32,
    /// Shards in backoff after failures.
    pub backoff_shards: u32,
    /// Shards handed back with work left when their budget ended, and
    /// leases that ended without a report (crashes, revocations).
    pub handbacks: u64,
    pub crashes: u64,
    /// Invocations that reached their budget without finishing a step.
    pub no_progress: u64,
    /// Client calls waiting at the server for a holder.
    pub parked_calls: usize,
}

pub(crate) struct Dispatch {
    endpoints: Vec<Endpoint>,
    urls: Vec<http::Url>,
    tls: Vec<Option<Arc<rustls::ClientConfig>>>,
    state: Mutex<State>,
    pub(crate) wake: Notify,
}

impl Dispatch {
    pub(crate) fn new(endpoints: Vec<Endpoint>, shards: u32) -> std::io::Result<Self> {
        if endpoints.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no invocation endpoints",
            ));
        }
        let mut urls = Vec::new();
        let mut tls = Vec::new();
        for e in &endpoints {
            e.validate()?;
            urls.push(http::Url::parse(&e.url)?);
            tls.push(super::client_tls(e)?);
        }
        let now = fusee::local::unix_ms();
        let state = State {
            // Nothing is known about any shard yet: each is due once.
            shards: (0..shards)
                .map(|_| ShardWake {
                    due: Some(now),
                    reasons: RECOVERY,
                    ..Default::default()
                })
                .collect(),
            invocations: HashMap::new(),
            conns: HashMap::new(),
            endpoints: endpoints
                .iter()
                .map(|e| EndpointState {
                    m: EndpointMetrics {
                        url: e.url.clone(),
                        types: e.types.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .collect(),
            rng: now | 1,
            handbacks: 0,
            crashes: 0,
            no_progress: 0,
        };
        Ok(Self {
            endpoints,
            urls,
            tls,
            state: Mutex::new(state),
            wake: Notify::new(),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A worker connected with `invocation=<id>`: the shards it may take
    /// and the budget it has left.
    pub(crate) fn bind(&self, id: u64, conn: u64) -> Result<(Vec<u32>, Duration), String> {
        let mut st = self.state();
        let Some(inv) = st.invocations.get_mut(&id) else {
            return Err("this invocation is unknown or already over".into());
        };
        if inv.conn.is_some() {
            return Err("another worker already answers this invocation".into());
        }
        inv.conn = Some(conn);
        let shards = inv.shards.clone();
        let left = inv.budget.saturating_sub(inv.started.elapsed());
        let endpoint = inv.endpoint;
        st.conns.insert(conn, (id, endpoint));
        Ok((shards, left))
    }

    /// The invocation's worker was granted `granted`.
    pub(crate) fn leased(&self, conn: u64, granted: &[(u32, u64)]) {
        let mut st = self.state();
        let Some((id, _)) = st.conns.get(&conn).copied() else {
            return;
        };
        if let Some(inv) = st.invocations.get_mut(&id) {
            inv.leased |= !granted.is_empty();
        }
        for &(s, _) in granted {
            if let Some(w) = st.shards.get_mut(s as usize) {
                w.due = None;
                w.reasons = 0;
                w.report = None;
            }
        }
    }

    pub(crate) fn is_invoked(&self, conn: u64) -> bool {
        self.state().conns.contains_key(&conn)
    }

    /// What a holder reported before leaving.
    pub(crate) fn report(&self, conn: u64, entries: Vec<IdleEntry>) {
        let mut st = self.state();
        for e in entries {
            if let Some(w) = st.shards.get_mut(e.shard as usize) {
                w.report = Some((conn, e));
            }
        }
    }

    /// `conn` released `held` (`(shard, epoch)` each): each shard's next
    /// due time is what its report said, or now if it left none.
    pub(crate) fn released(&self, conn: u64, held: &[(u32, u64)]) {
        let now = fusee::local::unix_ms();
        let mut guard = self.state();
        let st = &mut *guard;
        let invoked = st.conns.remove(&conn);
        let endpoint = invoked.map(|(_, e)| e);
        let mut log = Vec::new();
        for &(shard, epoch) in held {
            let Some(w) = st.shards.get_mut(shard as usize) else {
                continue;
            };
            let report = w
                .report
                .take()
                .filter(|(c, e)| *c == conn && e.epoch == epoch)
                .map(|(_, e)| e);
            let Some(r) = report else {
                // A crash, a revoked lease, or a worker that never reports.
                st.crashes += 1;
                w.due = Some(now);
                w.reasons |= RECOVERY;
                if invoked.is_some() {
                    w.failures += 1;
                    w.not_before = now
                        + backoff(
                            w.failures,
                            Duration::from_millis(100),
                            Duration::from_secs(30),
                            &mut st.rng,
                        )
                        .as_millis() as u64;
                }
                continue;
            };
            if let Some(e) = endpoint {
                let m = &mut st.endpoints[e].m;
                m.steps += r.steps;
                m.open_us_total += r.open_us;
                m.opens += 1;
            }
            w.awaiting = r.types.clone();
            if r.flags & crate::proto::IDLE_BUSY != 0 {
                st.handbacks += 1;
                w.due = Some(now);
                w.reasons |= HANDBACK;
                if r.flags & crate::proto::IDLE_EXPIRED != 0 && r.steps == 0 {
                    st.no_progress += 1;
                    w.failures += 1;
                    let d = backoff(
                        w.failures,
                        Duration::from_secs(1),
                        Duration::from_secs(300),
                        &mut st.rng,
                    );
                    w.not_before = now + d.as_millis() as u64;
                    log.push(format!(
                        "Fusee server: shard {shard} reached its invocation budget without finishing a step; \
                         a step may take longer than the budget (next try in {} ms)",
                        d.as_millis()
                    ));
                } else {
                    w.failures = 0;
                }
                continue;
            }
            w.failures = 0;
            w.due = (r.due_in_ms != u64::MAX).then(|| now.saturating_add(r.due_in_ms));
            w.reasons = if w.due.is_some() { TIMER } else { 0 };
            // Runs this worker had no code for go to an endpoint that has.
            let other = r.types.iter().any(|t| {
                self.endpoints
                    .iter()
                    .enumerate()
                    .any(|(i, e)| Some(i) != endpoint && e.types.contains(t))
            });
            if other {
                w.due = Some(now);
                w.reasons |= CODE;
            }
        }
        for &(shard, _) in held {
            if let Some(w) = guard.shards.get(shard as usize) {
                trace!(
                    "connection {conn} released shard {shard}: due in {:?} ms, reasons {:#x}, failures {}",
                    w.due.map(|d| d as i64 - now as i64),
                    w.reasons,
                    w.failures
                );
            }
        }
        drop(guard);
        for l in log {
            eprintln!("{l}");
        }
        self.wake.notify_one();
    }

    /// The endpoint for a shard's next invocation.
    fn choose(&self, w: &ShardWake, starts: &[String]) -> usize {
        let typed = |t: &String| self.endpoints.iter().position(|e| e.types.contains(t));
        if let Some(e) = starts.iter().find_map(typed) {
            return e;
        }
        if let Some(e) = w
            .awaiting
            .iter()
            .filter_map(typed)
            .find(|e| Some(*e) != w.last_endpoint)
        {
            return e;
        }
        if let Some(e) = w.last_endpoint.filter(|e| *e < self.endpoints.len()) {
            return e;
        }
        self.endpoints
            .iter()
            .position(|e| e.types.is_empty())
            .unwrap_or(0)
    }

    pub(crate) fn metrics(&self, parked: usize) -> InvokeMetrics {
        let now = fusee::local::unix_ms();
        let st = self.state();
        let mut m = InvokeMetrics {
            endpoints: Vec::new(),
            handbacks: st.handbacks,
            crashes: st.crashes,
            no_progress: st.no_progress,
            parked_calls: parked,
            ..Default::default()
        };
        for e in &st.endpoints {
            let mut em = e.m.clone();
            em.in_flight = e.in_flight;
            em.failures_in_a_row = e.failures;
            em.backoff_ms = e.not_before.saturating_sub(now);
            m.endpoints.push(em);
        }
        for w in &st.shards {
            if w.invoking.is_some() {
                m.invoking_shards += 1;
            } else if w.not_before > now {
                m.backoff_shards += 1;
            } else if w.due.is_some_and(|d| d <= now) {
                m.due_shards += 1;
            }
        }
        m
    }

    /// Starts the invocations that are due and have a slot; returns when
    /// to look again.
    fn dispatch(self: &Arc<Self>, server: &Arc<Server>) -> Option<u64> {
        let now = fusee::local::unix_ms();
        let holders = server.holders();
        let wanted = server.wanted();
        let mut st = self.state();
        let mut next: Option<u64> = None;
        let mut soon = |t: u64| next = Some(next.map_or(t, |n| n.min(t)));
        // Due shards by endpoint, with why.
        let mut due: Vec<Vec<(u32, u8)>> = vec![Vec::new(); self.endpoints.len()];
        for (s, w) in st.shards.iter().enumerate() {
            if holders.get(s).copied().flatten().is_some() || w.invoking.is_some() {
                continue;
            }
            let want = wanted.get(s);
            let mut reasons = w.reasons;
            if want.is_some_and(|w| w.calls) {
                reasons |= CALL;
            }
            if want.is_some_and(|w| w.messages) {
                reasons |= MESSAGE;
            }
            let triggered = reasons & (CALL | MESSAGE) != 0;
            let at = if triggered {
                now
            } else {
                match w.due {
                    Some(d) => d,
                    None => continue,
                }
            };
            let at = at.max(w.not_before);
            if at > now {
                soon(at);
                continue;
            }
            let starts = want.map(|w| w.starts.as_slice()).unwrap_or_default();
            due[self.choose(w, starts)].push((s as u32, reasons));
        }
        let mut start = Vec::new();
        for (e, list) in due.into_iter().enumerate() {
            if list.is_empty() {
                continue;
            }
            let es = &st.endpoints[e];
            if es.not_before > now {
                soon(es.not_before);
                continue;
            }
            let free = self.endpoints[e].concurrency.saturating_sub(es.in_flight) as usize;
            if free == 0 {
                continue;
            }
            let mut per = list.len().div_ceil(free).max(1);
            if self.endpoints[e].max_shards > 0 {
                per = per.min(self.endpoints[e].max_shards as usize);
            }
            for chunk in list.chunks(per).take(free) {
                let id = random_id();
                let shards: Vec<u32> = chunk.iter().map(|(s, _)| *s).collect();
                let reasons = chunk.iter().fold(0, |a, (_, r)| a | r);
                for &s in &shards {
                    let w = &mut st.shards[s as usize];
                    w.invoking = Some(id);
                    w.last_endpoint = Some(e);
                }
                let es = &mut st.endpoints[e];
                es.in_flight += 1;
                es.m.invocations += 1;
                es.m.shards += shards.len() as u64;
                st.invocations.insert(
                    id,
                    Invocation {
                        endpoint: e,
                        shards: shards.clone(),
                        conn: None,
                        leased: false,
                        started: Instant::now(),
                        budget: self.endpoints[e].budget,
                    },
                );
                start.push((id, e, shards, reasons));
            }
        }
        drop(st);
        for (id, e, shards, reasons) in start {
            trace!(
                "invocation {} to endpoint {e} for shards {shards:?}, reasons {reasons:#x}",
                super::format_id(id)
            );
            let me = self.clone();
            let server = Arc::downgrade(server);
            tokio::spawn(async move { me.call(id, e, shards, reasons, server).await });
        }
        next
    }

    async fn call(
        self: Arc<Self>,
        id: u64,
        e: usize,
        shards: Vec<u32>,
        reasons: u8,
        server: Weak<Server>,
    ) {
        let ep = &self.endpoints[e];
        let request = Request {
            version: 1,
            invocation: super::format_id(id),
            shards,
            budget_ms: ep.budget.as_millis() as u64,
            reasons: REASONS
                .iter()
                .filter(|(bit, _)| reasons & bit != 0)
                .map(|(_, n)| (*n).to_owned())
                .collect(),
        };
        let body = serde_json::to_vec(&request).unwrap_or_default();
        let mut headers = vec![("content-type", "application/json".to_owned())];
        if let Some(secret) = &ep.secret {
            headers.push((
                super::SIGNATURE_HEADER,
                super::sign(secret.bytes(), fusee::local::unix_ms(), &body),
            ));
        }
        let sent = Instant::now();
        let result = tokio::time::timeout(
            ep.budget + ep.grace,
            http::post(&self.urls[e], self.tls[e].as_ref(), &headers, &body),
        )
        .await;
        let outcome = match result {
            Err(_) => Outcome::TimedOut,
            Ok(Err(err)) => Outcome::Failed(err.to_string()),
            Ok(Ok(r)) if (200..300).contains(&r.status) => Outcome::Ok,
            Ok(Ok(r)) => Outcome::Failed(format!(
                "HTTP {}: {}",
                r.status,
                String::from_utf8_lossy(&r.body[..r.body.len().min(200)])
            )),
        };
        trace!(
            "invocation {} answered after {} ms: {}",
            super::format_id(id),
            sent.elapsed().as_millis(),
            match &outcome {
                Outcome::Ok => "ok".to_owned(),
                Outcome::Failed(e) => e.clone(),
                Outcome::TimedOut => "timed out".to_owned(),
            }
        );
        let conn = self.state().invocations.get(&id).and_then(|i| i.conn);
        if let (Outcome::Ok, Some(conn), Some(s)) = (&outcome, conn, server.upgrade()) {
            // Its report and close are on their way on the lease connection.
            let deadline = Instant::now() + CLOSE_GRACE;
            while s.holds(conn) && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        self.finish(id, outcome, sent.elapsed(), server.upgrade().as_ref());
    }

    fn finish(&self, id: u64, outcome: Outcome, took: Duration, server: Option<&Arc<Server>>) {
        let now = fusee::local::unix_ms();
        let Some(inv) = self.state().invocations.remove(&id) else {
            return;
        };
        // Fenced: a process still holding the lease loses it, and the ID
        // stops being valid.
        if let (Some(conn), Some(s)) = (inv.conn, server)
            && s.holds(conn)
        {
            s.revoke(conn);
        }
        let mut guard = self.state();
        let st = &mut *guard;
        for &s in &inv.shards {
            if let Some(w) = st.shards.get_mut(s as usize)
                && w.invoking == Some(id)
            {
                w.invoking = None;
            }
        }
        let es = &mut st.endpoints[inv.endpoint];
        es.in_flight = es.in_flight.saturating_sub(1);
        let ms = took.as_millis() as u64;
        es.m.total_ms += ms;
        es.m.max_ms = es.m.max_ms.max(ms);
        let error = match outcome {
            Outcome::Ok if inv.leased => {
                es.m.succeeded += 1;
                None
            }
            Outcome::Ok => {
                es.m.unleased += 1;
                Some("the endpoint answered 2xx without taking the lease".to_owned())
            }
            Outcome::Failed(e) => {
                es.m.failed += 1;
                Some(e)
            }
            Outcome::TimedOut => {
                es.m.timed_out += 1;
                Some(format!(
                    "no answer within the budget plus {} ms",
                    self.endpoints[inv.endpoint].grace.as_millis()
                ))
            }
        };
        match error {
            None => es.failures = 0,
            Some(e) => {
                es.failures += 1;
                let d = backoff(
                    es.failures,
                    Duration::from_millis(100),
                    Duration::from_secs(30),
                    &mut st.rng,
                );
                es.not_before = now + d.as_millis() as u64;
                if es
                    .logged
                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(10))
                {
                    es.logged = Some(Instant::now());
                    eprintln!(
                        "Fusee server: invoking {} failed ({e}); backing off {} ms",
                        self.endpoints[inv.endpoint].url,
                        d.as_millis()
                    );
                }
                es.m.last_error = Some(e);
            }
        }
        drop(guard);
        self.wake.notify_one();
    }

    /// Runs the invoker until the server goes away.
    pub(crate) async fn run(self: Arc<Self>, server: Weak<Server>) {
        loop {
            let Some(s) = server.upgrade() else { return };
            let next = self.dispatch(&s);
            let parked_next = s.expire_parked();
            drop(s);
            let now = fusee::local::unix_ms();
            let mut wait = Duration::from_secs(1);
            for t in [next, parked_next].into_iter().flatten() {
                wait = wait.min(Duration::from_millis(t.saturating_sub(now).max(1)));
            }
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }
}

enum Outcome {
    Ok,
    Failed(String),
    TimedOut,
}

/// A fresh, unpredictable, nonzero invocation ID.
fn random_id() -> u64 {
    use ring::rand::SecureRandom;
    let rng = ring::rand::SystemRandom::new();
    loop {
        let mut b = [0u8; 8];
        if rng.fill(&mut b).is_err() {
            // No system randomness: time and a counter are still unique.
            static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
            return fusee::local::monotonic_us() << 16
                | N.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 0xffff;
        }
        let id = u64::from_le_bytes(b);
        if id != 0 {
            return id;
        }
    }
}

/// Per shard: what waits at the server for a holder.
#[derive(Clone, Debug, Default)]
pub(crate) struct Wanted {
    pub(crate) calls: bool,
    pub(crate) messages: bool,
    /// Workflow types of waiting starts.
    pub(crate) starts: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_with_jitter_and_caps() {
        let mut rng = 12345;
        let b = Duration::from_millis(100);
        let m = Duration::from_secs(30);
        for (n, want) in [(1, 100), (2, 200), (3, 400), (10, 30_000), (40, 30_000)] {
            let d = backoff(n, b, m, &mut rng).as_millis() as u64;
            assert!(d >= want * 8 / 10 && d <= want * 12 / 10, "{n}: {d}");
        }
    }
}
