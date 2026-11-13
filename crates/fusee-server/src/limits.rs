//! Step limits in server mode.
//!
//! The limit lives on the server and every permit is a lease: the worker
//! holding it renews it while its step runs, and a permit whose worker
//! crashed or was cut off returns only when its lease expires. So a limit
//! holds across a failover too: a deposed worker whose steps may still be
//! running keeps its permits until they lapse, and the new worker waits.
//!
//! [`ServerLimits`] is the server's table; [`RemoteLimits`] is the worker's
//! [`LimitBackend`], which asks the server asynchronously and wakes waiting
//! steps when the server grants their permit.
//!
//! The server keeps every waiting ticket in a [`WaitQueue`] with the
//! priority and fairness key it was first asked with, so the order is the
//! same as in one process. A worker asks again only for its own best
//! waiting ticket of each limit; any ask from a worker lets the server
//! grant whichever ticket of that worker the queue serves next, and the
//! answer names that ticket.

use fusee::limits::{Acquire, LimitBackend, LimitSpec, Priority, WaitQueue};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Waker;
use std::time::{Duration, Instant};

/// How long a permit lease lasts without renewal.
pub const LEASE_MS: u64 = 3_000;

/// A worker's tickets count up from `worker_id << CLIENT_SHIFT`, so the
/// bits above name the client that asked.
pub const CLIENT_SHIFT: u32 = 40;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Limit {
    #[serde(with = "saved_spec")]
    spec: LimitSpec,
    #[serde(with = "saved_balance")]
    tokens: f64,
    refilled_ms: Option<u64>,
    /// (worker session, ticket) → lease expiry
    #[serde(with = "grants")]
    held: BTreeMap<(u64, u64), u64>,
    /// (worker session, ticket) waiting for a permit.
    queue: WaitQueue<(u64, u64)>,
    max_seen: u32,
}

/// The server's answer to an acquire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Grant {
    /// This ticket of the asking session holds a permit now: the asked
    /// ticket, or another one of the session's that the queue served first.
    Granted(u64),
    Queued,
    /// Ask again at this time (the rate limit refills then).
    RetryAt(u64),
}

// One floating-point ulp can move a token bucket across a grant boundary.
// Preserve both machine values bit-for-bit across checkpoint installation.
mod saved_balance {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(n: &f64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(n.to_bits())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        Ok(f64::from_bits(u64::deserialize(d)?))
    }
}
mod saved_spec {
    use super::LimitSpec;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S: Serializer>(n: &LimitSpec, s: S) -> Result<S::Ok, S::Error> {
        (n.concurrent, n.per_second.to_bits()).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<LimitSpec, D::Error> {
        let (concurrent, bits) = <(u32, u64)>::deserialize(d)?;
        Ok(LimitSpec {
            concurrent,
            per_second: f64::from_bits(bits),
        })
    }
}

// JSON object keys cannot represent (session, ticket) pairs. Persist sorted
// triples to retain the full identity and make the checkpoint deterministic.
mod grants {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        held: &BTreeMap<(u64, u64), u64>,
        writer: S,
    ) -> Result<S::Ok, S::Error> {
        writer.collect_seq(
            held.iter()
                .map(|(&(session, ticket), &until)| (session, ticket, until)),
        )
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        reader: D,
    ) -> Result<BTreeMap<(u64, u64), u64>, D::Error> {
        let mut held = BTreeMap::new();
        for (session, ticket, until) in Vec::<(u64, u64, u64)>::deserialize(reader)? {
            if held.insert((session, ticket), until).is_some() {
                return Err(serde::de::Error::custom("duplicate replicated permit"));
            }
        }
        Ok(held)
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SavedLimits(BTreeMap<String, Limit>);

/// The server's limit table.
#[derive(Debug, Default)]
pub struct ServerLimits {
    limits: Mutex<HashMap<String, Limit>>,
    disk: Option<(PathBuf, fusee::FlushMethod)>,
    restart: Option<Instant>,
    failure: Mutex<Option<String>>,
}

impl ServerLimits {
    pub(crate) fn snapshot(&self) -> io::Result<SavedLimits> {
        self.healthy()?;
        let limits = self
            .limits
            .lock()
            .map_err(|_| io::Error::other("poisoned limit table"))?;
        Ok(SavedLimits(
            limits.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        ))
    }

    pub(crate) fn from_snapshot(saved: SavedLimits) -> io::Result<Self> {
        for limit in saved.0.values() {
            limit.spec.validate().map_err(io::Error::other)?;
            if !limit.tokens.is_finite() || limit.tokens < 0.0 {
                return Err(io::Error::other("invalid replicated rate balance"));
            }
        }
        Ok(Self {
            limits: Mutex::new(saved.0.into_iter().collect()),
            ..Self::default()
        })
    }

    pub(crate) fn open(dir: &Path, method: fusee::FlushMethod) -> io::Result<Self> {
        let file = dir.join("limits.fsl");
        let specs = match std::fs::read(&file) {
            Ok(bytes) => {
                if bytes.len() < 8
                    || &bytes[..4] != b"FSL1"
                    || crc32c::crc32c(&bytes[8..]).to_le_bytes() != bytes[4..8]
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "damaged limit definitions",
                    ));
                }
                serde_json::from_slice::<BTreeMap<String, LimitSpec>>(&bytes[8..])?
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e),
        };
        let mut out = Self::default();
        for (name, spec) in specs {
            out.define(&name, spec);
        }
        // The former process may have issued permits immediately before it
        // stopped. No grant is reusable until its full lease and rate interval
        // could have elapsed on this process's monotonic clock.
        out.healthy()?;
        out.restart = dir.join("shard-000").exists().then(Instant::now);
        out.disk = Some((dir.to_owned(), method));
        Ok(out)
    }

    pub(crate) fn healthy(&self) -> io::Result<()> {
        if let Some(message) = self
            .failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            return Err(io::Error::other(message.clone()));
        }
        Ok(())
    }

    pub fn define(&self, name: &str, spec: LimitSpec) {
        if let Err(e) = spec.validate() {
            *self.failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(e.to_string());
            return;
        }
        let mut limits = self.limits.lock().unwrap_or_else(|e| e.into_inner());
        if limits.get(name).is_some_and(|l| l.spec == spec) {
            return;
        }
        limits
            .entry(name.to_owned())
            .or_insert_with(|| Limit {
                spec,
                tokens: 0.0,
                refilled_ms: None,
                held: BTreeMap::new(),
                queue: WaitQueue::default(),
                max_seen: 0,
            })
            .spec = spec;
        if let Some((dir, method)) = &self.disk {
            let save = || -> io::Result<()> {
                let bytes = serde_json::to_vec(
                    &limits
                        .iter()
                        .map(|(name, l)| (name, l.spec))
                        .collect::<BTreeMap<_, _>>(),
                )?;
                let tmp = dir.join("limits.fsl.tmp");
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(b"FSL1")?;
                f.write_all(&crc32c::crc32c(&bytes).to_le_bytes())?;
                f.write_all(&bytes)?;
                fusee::local::flush_file(&f, *method)?;
                std::fs::rename(tmp, dir.join("limits.fsl"))?;
                fusee::local::sync_dir(dir, *method)
            };
            if let Err(e) = save() {
                *self.failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(e.to_string());
            }
        }
    }

    /// Queues `ticket` of session `conn` at `priority` (the first time it is
    /// asked for), then grants the permit to the ticket the queue serves
    /// next if it is one of `conn`'s and the limit has room.
    pub fn acquire(
        &self,
        name: &str,
        conn: u64,
        ticket: u64,
        priority: &Priority,
        now: u64,
        lease_ms: u64,
    ) -> Grant {
        let Ok(mut all) = self.limits.lock() else {
            return Grant::Queued;
        };
        let Some(l) = all.get_mut(name) else {
            return Grant::Granted(ticket);
        };
        if let Some(restart) = self.restart {
            let refill = if l.spec.per_second > 0.0 {
                (1000.0 / l.spec.per_second).ceil() as u64
            } else {
                0
            };
            let until = Duration::from_millis(LEASE_MS.max(refill));
            if let Some(wait) = until.checked_sub(restart.elapsed()) {
                return Grant::RetryAt(now.saturating_add(wait.as_millis() as u64 + 1));
            }
        }
        l.held.retain(|_, exp| *exp > now);
        if let Some(h) = l.held.get_mut(&(conn, ticket)) {
            *h = now.saturating_add(lease_ms);
            return Grant::Granted(ticket);
        }
        l.queue.push((conn, ticket), priority);
        if l.spec.per_second > 0.0 {
            let burst = l.spec.per_second.max(1.0);
            if let Some(previous) = l.refilled_ms {
                let add = now.saturating_sub(previous) as f64 * l.spec.per_second / 1000.0;
                l.tokens = (l.tokens + add).min(burst);
                l.refilled_ms = Some(previous.max(now));
            } else {
                l.tokens = burst;
                l.refilled_ms = Some(now);
            }
        }
        // Any of a client's asks lets it take the permit the queue serves
        // next, so a worker need not know which of its tickets that is. A
        // ticket's top bits name its client (see `CLIENT_SHIFT`): one
        // replicated session can carry two allocators.
        let Some(next) = l
            .queue
            .next()
            .filter(|n| n.0 == conn && n.1 >> CLIENT_SHIFT == ticket >> CLIENT_SHIFT)
        else {
            return Grant::Queued;
        };
        let room = l.spec.concurrent == 0 || (l.held.len() as u32) < l.spec.concurrent;
        if !room {
            return Grant::Queued;
        }
        if l.spec.per_second > 0.0 && l.tokens < 1.0 {
            let wait = ((1.0 - l.tokens) * 1000.0 / l.spec.per_second).ceil() as u64;
            return Grant::RetryAt(now.saturating_add(wait.max(1)));
        }
        l.queue.pop_next();
        if l.spec.per_second > 0.0 {
            l.tokens -= 1.0;
        }
        l.held.insert(next, now.saturating_add(lease_ms));
        l.max_seen = l.max_seen.max(l.held.len() as u32);
        Grant::Granted(next.1)
    }

    pub fn release(&self, name: &str, conn: u64, ticket: u64) {
        if let Ok(mut all) = self.limits.lock()
            && let Some(l) = all.get_mut(name)
        {
            l.held.remove(&(conn, ticket));
            l.queue.remove(&(conn, ticket));
        }
    }

    /// Extends every lease `conn` holds.
    pub fn renew(&self, conn: u64, now: u64, lease_ms: u64) {
        if let Ok(mut all) = self.limits.lock() {
            for l in all.values_mut() {
                l.held.retain(|_, exp| *exp > now);
                for ((c, _), exp) in &mut l.held {
                    if *c == conn {
                        *exp = now.saturating_add(lease_ms);
                    }
                }
            }
        }
    }

    /// A connection went away: its queued tickets leave the queue. Its
    /// permits stay until their leases expire (its steps may still run).
    pub fn disconnected(&self, conn: u64) {
        if let Ok(mut all) = self.limits.lock() {
            for l in all.values_mut() {
                l.queue.retain(|q| q.0 != conn);
            }
        }
    }

    /// Tickets waiting for a permit of `name`.
    pub fn waiting(&self, name: &str) -> usize {
        if let Ok(all) = self.limits.lock() {
            all.get(name).map_or(0, |l| l.queue.len())
        } else {
            0
        }
    }

    pub fn in_use(&self, name: &str, now: u64) -> u32 {
        self.limits
            .lock()
            .ok()
            .and_then(|l| {
                l.get(name)
                    .map(|l| l.held.values().filter(|e| **e > now).count() as u32)
            })
            .unwrap_or(0)
    }

    pub fn max_in_use(&self, name: &str) -> u32 {
        self.limits
            .lock()
            .ok()
            .and_then(|l| l.get(name).map(|l| l.max_seen))
            .unwrap_or(0)
    }
}

/// What the worker knows about one ticket.
#[derive(Debug)]
enum TicketState {
    Asked,
    Granted,
    Retry(u64),
}

#[derive(Debug, Default)]
struct Tickets {
    state: HashMap<u64, (String, TicketState, Option<Waker>, Priority)>,
    // The worker's own waiting tickets per limit, in the server's order
    // (first-send order within a key, which need not be ticket order).
    waiting: HashMap<String, WaitQueue<u64>>,
}

impl Tickets {
    /// A ticket stops waiting: `granted`, or withdrawn.
    fn remove_waiter(&mut self, name: &str, ticket: u64, granted: bool) {
        if let Some(queue) = self.waiting.get_mut(name) {
            // A grant of the ticket served next moves the turn on, as it
            // did on the server; a withdrawal does not.
            if granted && queue.next() == Some(ticket) {
                queue.pop_next();
            } else {
                queue.remove(&ticket);
            }
            if queue.is_empty() {
                self.waiting.remove(name);
            }
        }
    }

    /// The ticket to ask for again, per limit: the worker's best waiting
    /// one. After a server restart it is the one the server hears of first.
    fn retries(&self, now: u64) -> Vec<(String, u64, Priority)> {
        self.waiting
            .iter()
            .filter_map(|(name, queue)| {
                let ticket = queue.next()?;
                let (_, state, _, priority) = self.state.get(&ticket)?;
                if matches!(state, TicketState::Asked)
                    || matches!(state,TicketState::Retry(at) if *at<=now)
                {
                    Some((name.clone(), ticket, priority.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Sends limit requests to the server. `priority` matters only for
/// [`op::ACQUIRE`].
pub trait LimitWire: Send + Sync + std::fmt::Debug {
    fn send_limit(
        &self,
        op: u8,
        name: &str,
        ticket: u64,
        a: u32,
        b: f64,
        priority: &Priority,
    ) -> std::io::Result<()>;
}

pub mod op {
    pub const DEFINE: u8 = 1;
    pub const ACQUIRE: u8 = 2;
    pub const RELEASE: u8 = 3;
    pub const RENEW: u8 = 4;
}

/// Appends an acquire's priority after a limit request's fixed fields:
/// level (i32), weight (u32), fairness key (bytes). Nothing for the default
/// priority, so arrival-order requests are the bytes they always were.
pub(crate) fn put_priority(out: &mut Vec<u8>, operation: u8, p: &Priority) {
    if operation == op::ACQUIRE && !p.is_default() {
        out.extend_from_slice(&p.level.to_le_bytes());
        out.extend_from_slice(&p.fairness_weight.to_le_bytes());
        fusee_core::wire::put_bytes(out, p.fairness_key.as_bytes());
    }
}

/// Reads what [`put_priority`] wrote: the default when nothing follows.
pub(crate) fn read_priority(
    r: &mut fusee_core::wire::Reader<'_>,
) -> fusee_core::wire::DecodeResult<Priority> {
    if r.is_empty() {
        return Ok(Priority::default());
    }
    Ok(Priority {
        level: r.u32()? as i32,
        fairness_weight: r.u32()?,
        fairness_key: r.str()?.to_owned(),
    })
}

/// The worker's [`LimitBackend`] in server mode.
#[derive(Debug)]
pub struct RemoteLimits {
    wire: Arc<dyn LimitWire>,
    wire_order: Mutex<()>,
    tickets: Mutex<Tickets>,
    maintenance: OnceLock<std::thread::Thread>,
    next: AtomicU64,
}

impl RemoteLimits {
    pub fn new(wire: Arc<dyn LimitWire>, worker_id: u64) -> Arc<Self> {
        let me = Arc::new(Self {
            wire,
            wire_order: Mutex::new(()),
            tickets: Mutex::new(Tickets::default()),
            maintenance: OnceLock::new(),
            // Tickets are unique across workers.
            next: AtomicU64::new(worker_id << CLIENT_SHIFT),
        });
        let weak = Arc::downgrade(&me);
        let _ = std::thread::Builder::new()
            .name("fusee-limits".into())
            .spawn(move || {
                // Renew held leases and ask again for waiting tickets.
                if let Some(me) = weak.upgrade() {
                    let _ = me.maintenance.set(std::thread::current());
                }
                let mut renewed = Instant::now();
                loop {
                    std::thread::park_timeout(Duration::from_millis(5));
                    let Some(me) = weak.upgrade() else { return };
                    let _order = me.wire_order.lock().unwrap_or_else(|e| e.into_inner());
                    if renewed.elapsed() >= Duration::from_millis(LEASE_MS / 3) {
                        renewed = Instant::now();
                        if me.tickets.lock().is_ok_and(|t| {
                            t.state
                                .values()
                                .any(|(_, s, _, _)| matches!(s, TicketState::Granted))
                        }) {
                            let _ = me.wire.send_limit(
                                op::RENEW,
                                "",
                                0,
                                LEASE_MS as u32,
                                0.0,
                                &Priority::default(),
                            );
                        }
                    }
                    let now = fusee::local::unix_ms();
                    let again = me
                        .tickets
                        .lock()
                        .map(|t| t.retries(now))
                        .unwrap_or_default();
                    for (name, t, priority) in again {
                        let _ = me.wire.send_limit(
                            op::ACQUIRE,
                            &name,
                            t,
                            LEASE_MS as u32,
                            0.0,
                            &priority,
                        );
                    }
                }
            });
        me
    }

    /// The server answered an acquire.
    pub fn answered(&self, ticket: u64, granted: bool, retry_at: u64) {
        let Ok(mut t) = self.tickets.lock() else {
            return;
        };
        let Some((name, state, waker, _)) = t.state.get_mut(&ticket) else {
            return;
        };
        if granted {
            *state = TicketState::Granted;
            let wake = waker.take();
            let name = name.clone();
            t.remove_waiter(&name, ticket, true);
            drop(t);
            if let Some(thread) = self.maintenance.get() {
                thread.unpark();
            }
            if let Some(w) = wake {
                w.wake();
            }
        } else if retry_at > 0 {
            *state = TicketState::Retry(retry_at);
        }
    }
}

impl LimitBackend for RemoteLimits {
    fn define(&self, name: &str, spec: LimitSpec) {
        let _ = self.wire.send_limit(
            op::DEFINE,
            name,
            0,
            spec.concurrent,
            spec.per_second,
            &Priority::default(),
        );
    }

    fn ticket(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn acquire_in(
        &self,
        name: &str,
        ticket: u64,
        priority: &Priority,
        _now_ms: u64,
        waker: Option<&Waker>,
    ) -> Acquire {
        let _order = self.wire_order.lock().unwrap_or_else(|e| e.into_inner());
        let Ok(mut t) = self.tickets.lock() else {
            return Acquire::Wait { retry_at_ms: None };
        };
        match t.state.get_mut(&ticket) {
            Some((_, TicketState::Granted, _, _)) => Acquire::Granted,
            Some((_, _, w, _)) => {
                *w = waker.cloned();
                Acquire::Wait { retry_at_ms: None }
            }
            None => {
                t.waiting
                    .entry(name.to_owned())
                    .or_default()
                    .push(ticket, priority);
                t.state.insert(
                    ticket,
                    (
                        name.to_owned(),
                        TicketState::Asked,
                        waker.cloned(),
                        priority.clone(),
                    ),
                );
                drop(t);
                let _ =
                    self.wire
                        .send_limit(op::ACQUIRE, name, ticket, LEASE_MS as u32, 0.0, priority);
                Acquire::Wait { retry_at_ms: None }
            }
        }
    }

    fn release(&self, name: &str, ticket: u64) {
        let _order = self.wire_order.lock().unwrap_or_else(|e| e.into_inner());
        if let Ok(mut t) = self.tickets.lock() {
            t.state.remove(&ticket);
            t.remove_waiter(name, ticket, false);
        }
        let _ = self
            .wire
            .send_limit(op::RELEASE, name, ticket, 0, 0.0, &Priority::default());
        if let Some(thread) = self.maintenance.get() {
            thread.unpark();
        }
    }

    fn in_use(&self, name: &str) -> u32 {
        self.tickets
            .lock()
            .map(|t| {
                t.state
                    .values()
                    .filter(|(n, s, _, _)| n == name && matches!(s, TicketState::Granted))
                    .count() as u32
            })
            .unwrap_or(0)
    }

    fn waiting(&self, name: &str) -> usize {
        self.tickets
            .lock()
            .map(|t| t.waiting.get(name).map_or(0, |q| q.len()))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(level: i32, key: &str) -> Priority {
        Priority {
            level,
            fairness_key: key.into(),
            fairness_weight: 1,
        }
    }

    fn one_permit() -> ServerLimits {
        let l = ServerLimits::default();
        l.define(
            "db",
            LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        );
        l
    }

    #[test]
    fn retries_only_the_best_ticket_of_each_limit() {
        let mut tickets = Tickets::default();
        let plain = Priority::default();
        for (name, id, p) in [
            ("a", 9, &plain),
            ("b", 5, &plain),
            ("a", 1, &plain),
            ("a", 3, &plain),
        ] {
            tickets.waiting.entry(name.into()).or_default().push(id, p);
            tickets
                .state
                .insert(id, (name.into(), TicketState::Asked, None, p.clone()));
        }
        let names = |r: Vec<(String, u64, Priority)>| {
            let mut r: Vec<(String, u64)> = r.into_iter().map(|(n, t, _)| (n, t)).collect();
            r.sort();
            r
        };
        assert_eq!(
            names(tickets.retries(5)),
            vec![("a".into(), 9), ("b".into(), 5)]
        );
        tickets.state.get_mut(&9).unwrap().1 = TicketState::Retry(20);
        assert_eq!(names(tickets.retries(19)), vec![("b".into(), 5)]);
        tickets.remove_waiter("a", 9, true);
        tickets.remove_waiter("a", 3, false);
        assert_eq!(
            names(tickets.retries(20)),
            vec![("a".into(), 1), ("b".into(), 5)]
        );
        // A later ticket with a higher level is asked for first.
        let urgent = level(4, "");
        tickets.waiting.get_mut("b").unwrap().push(7, &urgent);
        tickets
            .state
            .insert(7, ("b".into(), TicketState::Asked, None, urgent.clone()));
        assert_eq!(
            names(tickets.retries(20)),
            vec![("a".into(), 1), ("b".into(), 7)]
        );
    }

    #[test]
    fn server_serves_priority_then_fairness_across_sessions() {
        let l = one_permit();
        assert_eq!(
            l.acquire("db", 1, 100, &Priority::default(), 0, 1000),
            Grant::Granted(100)
        );
        // Session 1 queues a deep backlog for tenant "big"; session 2 queues
        // for tenants "a" and "b"; one urgent ticket arrives last.
        let mut asks = Vec::new();
        for t in 101..121 {
            asks.push((1, t, level(0, "big")));
        }
        for (t, key) in [(201, "a"), (202, "b"), (203, "a")] {
            asks.push((2, t, level(0, key)));
        }
        asks.push((2, 299, level(5, "")));
        for (conn, t, p) in &asks {
            assert_eq!(l.acquire("db", *conn, *t, p, 0, 1000), Grant::Queued);
        }
        assert_eq!(l.waiting("db"), asks.len());
        let mut held = (1, 100);
        let mut served = Vec::new();
        for _ in 0..6 {
            l.release("db", held.0, held.1);
            // Each session rings with one of its waiting tickets; the
            // permit goes to the ticket the queue serves next, which may
            // be another of the session's.
            let got = [(1, 120), (2, 203)]
                .iter()
                .find_map(|&(conn, ring)| {
                    match l.acquire("db", conn, ring, &level(0, "x"), 0, 1000) {
                        Grant::Granted(t) => Some((conn, t)),
                        _ => None,
                    }
                })
                .expect("one session takes the free permit");
            served.push(got.1);
            held = got;
        }
        // The urgent ticket, then tenants in turn: "big" does not hold "a"
        // and "b" back behind its twenty.
        assert_eq!(served, vec![299, 101, 201, 202, 102, 203]);
        assert_eq!(l.max_in_use("db"), 1);
    }

    #[test]
    fn default_priority_keeps_arrival_order_on_the_server() {
        // With default priorities the server keeps arrival order: a later
        // ticket of the same session waits for the earlier one.
        let l = one_permit();
        let plain = Priority::default();
        assert_eq!(l.acquire("db", 1, 1, &plain, 0, 1000), Grant::Granted(1));
        assert_eq!(l.acquire("db", 1, 2, &plain, 0, 1000), Grant::Queued);
        assert_eq!(l.acquire("db", 1, 3, &plain, 0, 1000), Grant::Queued);
        l.release("db", 1, 1);
        assert_eq!(l.acquire("db", 1, 3, &plain, 0, 1000), Grant::Granted(2));
        l.release("db", 1, 2);
        assert_eq!(l.acquire("db", 1, 3, &plain, 0, 1000), Grant::Granted(3));
    }

    #[test]
    fn persisted_definitions_and_session_scoped_tickets() {
        let dir = std::env::temp_dir().join(format!(
            "fusee-limit-persist-{}-{}",
            std::process::id(),
            fusee::local::unix_ms()
        ));
        std::fs::create_dir(&dir).unwrap();
        let method = fusee::FlushMethod::Fsync;
        let table = ServerLimits::open(&dir, method).unwrap();
        table.define(
            "one",
            LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        );
        assert_eq!(
            table.acquire("one", 11, 1, &Priority::default(), 0, 10),
            Grant::Granted(1)
        );
        table.release("one", 12, 1);
        assert_eq!(
            table.acquire("one", 12, 1, &Priority::default(), 1, 10),
            Grant::Queued
        );
        table.renew(11, 11, 10); // an expired slot cannot be revived
        assert_eq!(
            table.acquire("one", 12, 1, &Priority::default(), 11, 10),
            Grant::Granted(1)
        );
        drop(table);
        let table = ServerLimits::open(&dir, method).unwrap();
        assert_eq!(
            table.acquire("one", 11, 1, &Priority::default(), 20, 10),
            Grant::Granted(1)
        );
        assert_eq!(
            table.acquire("one", 12, 1, &Priority::default(), 21, 10),
            Grant::Queued
        );
        std::fs::create_dir(dir.join("shard-000")).unwrap();
        assert!(matches!(
            ServerLimits::open(&dir, method).unwrap().acquire(
                "one",
                1,
                1,
                &Priority::default(),
                0,
                10
            ),
            Grant::RetryAt(_)
        ));
        std::fs::write(dir.join("limits.fsl"), b"damaged").unwrap();
        assert!(ServerLimits::open(&dir, method).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn leases_outlive_their_holder() {
        let l = ServerLimits::default();
        l.define(
            "stripe",
            LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        );
        assert_eq!(
            l.acquire("stripe", 1, 10, &Priority::default(), 0, 100),
            Grant::Granted(10)
        );
        // Worker 1 is cut off; worker 2 must wait for the lease to lapse.
        l.disconnected(1);
        assert_eq!(
            l.acquire("stripe", 2, 20, &Priority::default(), 50, 100),
            Grant::Queued
        );
        assert_eq!(
            l.acquire("stripe", 2, 20, &Priority::default(), 99, 100),
            Grant::Queued
        );
        assert_eq!(
            l.acquire("stripe", 2, 20, &Priority::default(), 101, 100),
            Grant::Granted(20)
        );
        assert_eq!(l.max_in_use("stripe"), 1);
        // Renewal keeps a lease alive.
        l.renew(2, 150, 100);
        assert_eq!(
            l.acquire("stripe", 3, 30, &Priority::default(), 240, 100),
            Grant::Queued
        );
        l.release("stripe", 2, 20);
        assert_eq!(
            l.acquire("stripe", 3, 30, &Priority::default(), 241, 100),
            Grant::Granted(30)
        );
    }
}
