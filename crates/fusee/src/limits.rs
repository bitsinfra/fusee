//! Concurrency and rate limits for steps.
//!
//! `engine.limit("stripe", LimitSpec { concurrent: 10, per_second: 100.0 })`
//! defines a limit; a step run with `StepOptions::limit("stripe")` takes a
//! permit before its function runs and returns it when the attempt ends.
//! A step waiting for a permit holds nothing else (its gate is already
//! durable). After a crash a new attempt requests a fresh permit.
//!
//! Waiting steps form a [`WaitQueue`]: the highest [`Priority::level`] is
//! served first; within a level, steps are grouped by
//! [`Priority::fairness_key`] and the groups take turns, each taking up to
//! [`Priority::fairness_weight`] permits per turn, so one key with a deep
//! backlog cannot starve the others. Within a key, arrival order. With the
//! default priority every step is in one group at one level: plain arrival
//! order.
//!
//! The priority is not journaled. It comes from the step's options, which
//! the code passes again when it reaches the step after a crash, so the
//! request is made again with the same priority and key.
//!
//! In one process every shard shares one [`LocalLimits`]: a counting
//! semaphore and a token bucket per limit in front of one wait queue. In
//! server mode the limit lives on the server and permits are leases (see
//! `fusee_server::RemoteLimits`), so a crashed holder's permits come back
//! when its lease expires; the server keeps the wait queue and decides with
//! the same [`WaitQueue`].

use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Waker;

/// A limit's size.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LimitSpec {
    /// Permits held at once; 0 means no concurrency limit.
    pub concurrent: u32,
    /// Permits started per second, with a burst of one second's worth; 0
    /// means no rate limit.
    pub per_second: f64,
}

impl LimitSpec {
    pub fn validate(self) -> crate::Result<()> {
        if !self.per_second.is_finite() || self.per_second < 0.0 {
            return Err(crate::Error::Rejected(
                "limit rate must be finite and nonnegative".into(),
            ));
        }
        Ok(())
    }
}

/// Where a step waiting for a permit stands. The default (level 0, no
/// key, weight 1) is plain arrival order.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(default)]
pub struct Priority {
    /// Higher levels are served first. A lower level waits while any
    /// higher one has waiters.
    pub level: i32,
    /// Waiters at one level are grouped by this key (a tenant ID, say) and
    /// the groups take turns. Empty: the shared group of unkeyed waiters.
    pub fairness_key: String,
    /// Permits the key takes in each turn; 0 counts as 1. The latest waiter
    /// of a key sets its weight.
    pub fairness_weight: u32,
}

impl Priority {
    pub fn is_default(&self) -> bool {
        self.level == 0 && self.fairness_key.is_empty() && self.fairness_weight <= 1
    }

    fn weight(&self) -> u32 {
        self.fairness_weight.max(1)
    }
}

/// One fairness key's waiters at one level.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Lane<I> {
    waiting: VecDeque<I>,
    weight: u32,
    /// Permits left in the lane's current turn.
    left: u32,
}

/// One priority level: lanes in turn order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Level<I> {
    /// Every lane once; the front lane is served next.
    turns: VecDeque<String>,
    lanes: BTreeMap<String, Lane<I>>,
}

impl<I> Default for Level<I> {
    fn default() -> Self {
        Self {
            turns: VecDeque::new(),
            lanes: BTreeMap::new(),
        }
    }
}

/// Waiters for one limit's permits: highest level first, then the levels'
/// fairness keys in turn (weighted round-robin), then arrival order within
/// a key. Deterministic, so a replicated server can apply it as a state
/// machine. A queue of default-priority waiters only saves as the plain
/// arrival-order list queues always saved as; any other as its levels.
#[derive(Clone, Debug)]
pub struct WaitQueue<I: Copy + Eq + Hash> {
    levels: BTreeMap<i32, Level<I>>,
    index: FxHashMap<I, (i32, String)>,
}

impl<I: Copy + Eq + Hash + Serialize> Serialize for WaitQueue<I> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut levels = self.levels.iter();
        match (levels.next(), levels.next()) {
            (None, _) => s.collect_seq(std::iter::empty::<I>()),
            (Some((0, level)), None)
                if level.lanes.len() == 1
                    && level.lanes.get("").is_some_and(|lane| lane.weight == 1) =>
            {
                s.collect_seq(&level.lanes[""].waiting)
            }
            _ => {
                // (level, lanes) pairs, lowest first: integer map keys do
                // not survive serde's buffering of untagged formats.
                #[derive(Serialize)]
                struct Levels<'a, I> {
                    levels: Vec<(&'a i32, &'a Level<I>)>,
                }
                Levels {
                    levels: self.levels.iter().collect(),
                }
                .serialize(s)
            }
        }
    }
}

impl<I: Copy + Eq + Hash> Default for WaitQueue<I> {
    fn default() -> Self {
        Self {
            levels: BTreeMap::new(),
            index: FxHashMap::default(),
        }
    }
}

impl<I: Copy + Eq + Hash> PartialEq for WaitQueue<I> {
    fn eq(&self, other: &Self) -> bool {
        self.levels == other.levels
    }
}

impl<'de, I> Deserialize<'de> for WaitQueue<I>
where
    I: Copy + Eq + Hash + Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Default-priority waiters only (and every queue saved before
        // priorities): a plain arrival-order list.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Saved<I> {
            Levels { levels: Vec<(i32, Level<I>)> },
            Fifo(VecDeque<I>),
        }
        let mut q = match Saved::<I>::deserialize(d)? {
            Saved::Levels { levels } => {
                let n = levels.len();
                let levels: BTreeMap<i32, Level<I>> = levels.into_iter().collect();
                if levels.len() != n || levels.values().any(|l| l.lanes.is_empty()) {
                    return Err(serde::de::Error::custom("limit queue has a bad level"));
                }
                Self {
                    levels,
                    index: FxHashMap::default(),
                }
            }
            Saved::Fifo(ids) => {
                let mut q = Self::default();
                for id in ids {
                    if !q.push(id, &Priority::default()) {
                        return Err(serde::de::Error::custom("limit queue holds a waiter twice"));
                    }
                }
                q
            }
        };
        q.reindex().map_err(serde::de::Error::custom)?;
        Ok(q)
    }
}

impl<I: Copy + Eq + Hash> WaitQueue<I> {
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn contains(&self, id: &I) -> bool {
        self.index.contains_key(id)
    }

    /// Queues `id` behind the waiters of its key and level. A waiter that
    /// is already queued keeps its place and priority; returns whether it
    /// was added.
    pub fn push(&mut self, id: I, p: &Priority) -> bool {
        if self.index.contains_key(&id) {
            return false;
        }
        let level = self.levels.entry(p.level).or_default();
        let lane = match level.lanes.get_mut(&p.fairness_key) {
            Some(lane) => lane,
            None => {
                level.turns.push_back(p.fairness_key.clone());
                level.lanes.entry(p.fairness_key.clone()).or_insert(Lane {
                    waiting: VecDeque::new(),
                    weight: p.weight(),
                    left: p.weight(),
                })
            }
        };
        lane.weight = p.weight();
        lane.waiting.push_back(id);
        self.index.insert(id, (p.level, p.fairness_key.clone()));
        true
    }

    /// The waiter served next.
    pub fn next(&self) -> Option<I> {
        let (_, level) = self.levels.iter().next_back()?;
        let key = level.turns.front()?;
        level.lanes.get(key)?.waiting.front().copied()
    }

    /// Removes the waiter served next and moves the turn on: its key keeps
    /// the turn until it has taken its weight's worth of permits.
    pub fn pop_next(&mut self) -> Option<I> {
        let mut entry = self.levels.last_entry()?;
        let level = entry.get_mut();
        let key = level.turns.front()?.clone();
        let lane = level.lanes.get_mut(&key)?;
        let id = lane.waiting.pop_front()?;
        lane.left = lane.left.saturating_sub(1);
        if lane.waiting.is_empty() {
            level.lanes.remove(&key);
            level.turns.pop_front();
        } else if lane.left == 0 {
            lane.left = lane.weight;
            level.turns.rotate_left(1);
        }
        if level.lanes.is_empty() {
            entry.remove();
        }
        self.index.remove(&id);
        Some(id)
    }

    /// Withdraws a waiter; returns whether it was queued.
    pub fn remove(&mut self, id: &I) -> bool {
        let Some((p, key)) = self.index.remove(id) else {
            return false;
        };
        if let Some(level) = self.levels.get_mut(&p) {
            if let Some(lane) = level.lanes.get_mut(&key) {
                if lane.waiting.front() == Some(id) {
                    lane.waiting.pop_front();
                } else if let Some(at) = lane.waiting.iter().position(|w| w == id) {
                    lane.waiting.remove(at);
                }
                if lane.waiting.is_empty() {
                    level.lanes.remove(&key);
                    if let Some(at) = level.turns.iter().position(|k| *k == key) {
                        level.turns.remove(at);
                    }
                }
            }
            if level.lanes.is_empty() {
                self.levels.remove(&p);
            }
        }
        true
    }

    /// Withdraws every waiter `keep` refuses.
    pub fn retain(&mut self, mut keep: impl FnMut(&I) -> bool) {
        let gone: Vec<I> = self.index.keys().filter(|i| !keep(i)).copied().collect();
        for id in gone {
            self.remove(&id);
        }
    }

    /// Rebuilds the index from the levels (after deserializing); fails on
    /// a waiter queued twice or a malformed level.
    pub fn reindex(&mut self) -> Result<(), String> {
        self.index.clear();
        for (&p, level) in &self.levels {
            if level.turns.len() != level.lanes.len()
                || level.turns.iter().any(|k| !level.lanes.contains_key(k))
            {
                return Err("limit queue turns do not match its lanes".into());
            }
            for (key, lane) in &level.lanes {
                if lane.waiting.is_empty() || lane.weight == 0 {
                    return Err("limit queue has an empty or weightless lane".into());
                }
                for id in &lane.waiting {
                    if self.index.insert(*id, (p, key.clone())).is_some() {
                        return Err("limit queue holds a waiter twice".into());
                    }
                }
            }
        }
        Ok(())
    }
}

/// The answer to an acquire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Acquire {
    Granted,
    /// Queued. The waker is woken when it may be granted; if `retry_at_ms`
    /// is set, ask again at that time (the rate limit refills then).
    Wait {
        retry_at_ms: Option<u64>,
    },
}

/// Where limits live.
pub trait LimitBackend: Send + Sync + std::fmt::Debug {
    fn define(&self, name: &str, spec: LimitSpec);
    /// A new ticket for one attempt's permit.
    fn ticket(&self) -> u64;
    /// Asks for the permit of `ticket`, queued at `priority` the first time
    /// it is asked for. Idempotent: asking again for a granted ticket
    /// answers `Granted`.
    fn acquire_in(
        &self,
        name: &str,
        ticket: u64,
        priority: &Priority,
        now_ms: u64,
        waker: Option<&Waker>,
    ) -> Acquire;
    /// [`acquire_in`](Self::acquire_in) at the default priority.
    fn acquire(&self, name: &str, ticket: u64, now_ms: u64, waker: Option<&Waker>) -> Acquire {
        self.acquire_in(name, ticket, &Priority::default(), now_ms, waker)
    }
    /// Returns a granted permit, or withdraws a queued ticket.
    fn release(&self, name: &str, ticket: u64);
    /// Permits currently held (for checks and metrics).
    fn in_use(&self, name: &str) -> u32;
    /// Steps waiting for a permit here, where the backend knows (metrics).
    fn waiting(&self, _name: &str) -> usize {
        0
    }
    /// The limits defined here, by name, where the backend knows (`fusee
    /// limits`, the UI).
    fn list(&self) -> Vec<(String, LimitSpec)> {
        Vec::new()
    }
}

/// A limit and its permits right now ([`crate::Engine::limits`]).
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitInfo {
    pub name: String,
    /// Permits held at once; 0 means no concurrency limit.
    pub concurrent: u32,
    /// Permits started per second; 0 means no rate limit.
    pub per_second: f64,
    /// Permits held now.
    pub in_use: u32,
    /// Steps of this process waiting for a permit.
    pub waiting: usize,
}

impl Limit {
    fn new(spec: LimitSpec) -> Self {
        Self {
            spec,
            in_use: 0,
            tokens: 0.0,
            refilled_ms: 0,
            queue: WaitQueue::default(),
            wakers: FxHashMap::default(),
            held: FxHashSet::default(),
            max_seen: 0,
        }
    }
}

#[derive(Debug)]
struct Limit {
    spec: LimitSpec,
    in_use: u32,
    tokens: f64,
    refilled_ms: u64,
    queue: WaitQueue<u64>,
    wakers: FxHashMap<u64, Option<Waker>>,
    held: FxHashSet<u64>,
    max_seen: u32,
}

impl Limit {
    fn refill(&mut self, now: u64) {
        if self.spec.per_second <= 0.0 {
            return;
        }
        let burst = self.spec.per_second.max(1.0);
        if self.refilled_ms == 0 {
            self.tokens = burst;
        } else if now > self.refilled_ms {
            let add = (now - self.refilled_ms) as f64 * self.spec.per_second / 1000.0;
            self.tokens = (self.tokens + add).min(burst);
        }
        self.refilled_ms = now.max(self.refilled_ms);
    }

    fn room(&self) -> bool {
        self.spec.concurrent == 0 || self.in_use < self.spec.concurrent
    }

    fn wake_head(&mut self) {
        if let Some(next) = self.queue.next()
            && let Some(Some(w)) = self.wakers.get(&next)
        {
            w.wake_by_ref();
        }
    }
}

/// Limits shared by every shard of one process.
#[derive(Debug, Default)]
pub struct LocalLimits {
    limits: Mutex<FxHashMap<String, Limit>>,
    next: AtomicU64,
}

impl LocalLimits {
    pub fn new() -> Self {
        Self::default()
    }

    /// The most permits ever held at once (the simulator's checker).
    pub fn max_in_use(&self, name: &str) -> u32 {
        self.limits
            .lock()
            .ok()
            .and_then(|l| l.get(name).map(|l| l.max_seen))
            .unwrap_or(0)
    }
}

impl LimitBackend for LocalLimits {
    fn define(&self, name: &str, spec: LimitSpec) {
        if let Ok(mut l) = self.limits.lock() {
            let e = l.entry(name.to_owned()).or_insert_with(|| Limit::new(spec));
            e.spec = spec;
            e.wake_head();
        }
    }

    fn ticket(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn acquire_in(
        &self,
        name: &str,
        ticket: u64,
        priority: &Priority,
        now_ms: u64,
        waker: Option<&Waker>,
    ) -> Acquire {
        let Ok(mut all) = self.limits.lock() else {
            return Acquire::Wait { retry_at_ms: None };
        };
        // A limit by key, `tenant/acme`: made on first use with the size of
        // the limit `tenant` (one per key, each its own permits).
        if !all.contains_key(name)
            && let Some((base, _)) = name.split_once('/')
            && let Some(spec) = all.get(base).map(|l| l.spec)
        {
            all.insert(name.to_owned(), Limit::new(spec));
        }
        // An undefined limit does not hold anything back.
        let Some(l) = all.get_mut(name) else {
            return Acquire::Granted;
        };
        if l.spec.validate().is_err() {
            return Acquire::Wait { retry_at_ms: None };
        }
        if l.held.contains(&ticket) {
            return Acquire::Granted;
        }
        l.queue.push(ticket, priority);
        l.wakers.insert(ticket, waker.cloned());
        l.refill(now_ms);
        if l.queue.next() != Some(ticket) || !l.room() {
            return Acquire::Wait { retry_at_ms: None };
        }
        if l.spec.per_second > 0.0 && l.tokens < 1.0 {
            let wait = ((1.0 - l.tokens) * 1000.0 / l.spec.per_second).ceil() as u64;
            return Acquire::Wait {
                retry_at_ms: Some(now_ms.saturating_add(wait.max(1))),
            };
        }
        l.queue.pop_next();
        l.wakers.remove(&ticket);
        if l.spec.per_second > 0.0 {
            l.tokens -= 1.0;
        }
        l.in_use += 1;
        l.max_seen = l.max_seen.max(l.in_use);
        l.held.insert(ticket);
        // The next in line may fit too.
        l.wake_head();
        Acquire::Granted
    }

    fn release(&self, name: &str, ticket: u64) {
        let Ok(mut all) = self.limits.lock() else {
            return;
        };
        let Some(l) = all.get_mut(name) else {
            return;
        };
        if l.held.remove(&ticket) {
            l.in_use = l.in_use.saturating_sub(1);
        } else {
            let was_next = l.queue.next() == Some(ticket);
            l.queue.remove(&ticket);
            l.wakers.remove(&ticket);
            if !was_next {
                return;
            }
        }
        l.wake_head();
    }

    fn in_use(&self, name: &str) -> u32 {
        self.limits
            .lock()
            .ok()
            .and_then(|l| l.get(name).map(|l| l.in_use))
            .unwrap_or(0)
    }

    fn waiting(&self, name: &str) -> usize {
        self.limits
            .lock()
            .ok()
            .and_then(|l| l.get(name).map(|l| l.queue.len()))
            .unwrap_or(0)
    }

    fn list(&self) -> Vec<(String, LimitSpec)> {
        let Ok(l) = self.limits.lock() else {
            return Vec::new();
        };
        let mut out: Vec<_> = l.iter().map(|(n, l)| (n.clone(), l.spec)).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

/// The replicated limit ledger answers an acquire with one number: `0`
/// queued, `1` the asked ticket granted, `2 + delay` ask again after
/// `delay` ms (the rate limit refills then), or the top bit plus the
/// zigzag distance to another ticket of the same session that was granted
/// instead (the one the wait queue serves next).
pub mod answer {
    pub const QUEUED: u64 = 0;
    pub const GRANTED: u64 = 1;
    const OTHER: u64 = 1 << 63;
    const MAX_DELAY: u64 = (1 << 62) - 1;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Answer {
        Queued,
        Granted(u64),
        RetryAfter(u64),
    }

    pub fn retry_after(delay_ms: u64) -> u64 {
        delay_ms.min(MAX_DELAY) + 2
    }

    /// Exact while the two tickets are less than 2^62 apart, as tickets of
    /// one client always are (they come from one counter).
    pub fn granted(asked: u64, ticket: u64) -> u64 {
        if ticket == asked {
            return GRANTED;
        }
        let d = ticket.wrapping_sub(asked) as i64;
        OTHER | (((d << 1) ^ (d >> 63)) as u64 & !OTHER)
    }

    pub fn decode(asked: u64, a: u64) -> Answer {
        match a {
            QUEUED => Answer::Queued,
            GRANTED => Answer::Granted(asked),
            a if a & OTHER != 0 => {
                let z = a & !OTHER;
                let d = ((z >> 1) as i64) ^ -((z & 1) as i64);
                Answer::Granted(asked.wrapping_add(d as u64))
            }
            a => Answer::RetryAfter(a - 2),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(level: i32, key: &str) -> Priority {
        Priority {
            level,
            fairness_key: key.into(),
            fairness_weight: 1,
        }
    }

    fn one_at_a_time() -> LocalLimits {
        let l = LocalLimits::new();
        l.define(
            "db",
            LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        );
        l
    }

    /// Grants the held permit away and returns the ticket served next.
    fn serve(l: &LocalLimits, held: &mut u64, waiting: &mut Vec<(u64, Priority)>) -> u64 {
        l.release("db", *held);
        let at = waiting
            .iter()
            .position(|(t, p)| l.acquire_in("db", *t, p, 0, None) == Acquire::Granted)
            .expect("a waiter is granted once the permit is free");
        let (t, _) = waiting.remove(at);
        *held = t;
        t
    }

    #[test]
    fn concurrency_in_arrival_order() {
        let l = LocalLimits::new();
        l.define(
            "db",
            LimitSpec {
                concurrent: 2,
                per_second: 0.0,
            },
        );
        let (a, b, c, d) = (l.ticket(), l.ticket(), l.ticket(), l.ticket());
        assert_eq!(l.acquire("db", a, 0, None), Acquire::Granted);
        assert_eq!(l.acquire("db", b, 0, None), Acquire::Granted);
        assert!(matches!(l.acquire("db", c, 0, None), Acquire::Wait { .. }));
        assert!(matches!(l.acquire("db", d, 0, None), Acquire::Wait { .. }));
        l.release("db", a);
        // d arrived after c, so it still waits even though there is room.
        assert!(matches!(l.acquire("db", d, 0, None), Acquire::Wait { .. }));
        assert_eq!(l.acquire("db", c, 0, None), Acquire::Granted);
        assert_eq!(l.in_use("db"), 2);
        assert_eq!(l.max_in_use("db"), 2);
    }

    #[test]
    fn default_priority_is_arrival_order() {
        // Many waiters with the default priority are served exactly in the
        // order they first asked.
        let l = one_at_a_time();
        let mut held = l.ticket();
        assert_eq!(l.acquire("db", held, 0, None), Acquire::Granted);
        let mut waiting: Vec<(u64, Priority)> =
            (0..50).map(|_| (l.ticket(), Priority::default())).collect();
        for (t, p) in &waiting {
            assert!(matches!(
                l.acquire_in("db", *t, p, 0, None),
                Acquire::Wait { .. }
            ));
        }
        let expected: Vec<u64> = waiting.iter().map(|w| w.0).collect();
        let mut served = Vec::new();
        while !waiting.is_empty() {
            served.push(serve(&l, &mut held, &mut waiting));
        }
        assert_eq!(served, expected);
        assert_eq!(l.max_in_use("db"), 1);
    }

    #[test]
    fn higher_priority_first() {
        let l = one_at_a_time();
        let mut held = l.ticket();
        assert_eq!(l.acquire("db", held, 0, None), Acquire::Granted);
        let mut waiting = Vec::new();
        for level in [0, 5, -3, 5, 0, 9] {
            let t = l.ticket();
            let pr = p(level, "");
            assert!(matches!(
                l.acquire_in("db", t, &pr, 0, None),
                Acquire::Wait { .. }
            ));
            waiting.push((t, pr));
        }
        let ids: Vec<u64> = waiting.iter().map(|w| w.0).collect();
        let mut served = Vec::new();
        while !waiting.is_empty() {
            served.push(serve(&l, &mut held, &mut waiting));
        }
        // Level 9, then the two at 5 in arrival order, the two at 0, -3.
        assert_eq!(served, vec![ids[5], ids[1], ids[3], ids[0], ids[4], ids[2]]);
    }

    #[test]
    fn a_deep_key_does_not_starve_the_others() {
        let l = one_at_a_time();
        let mut held = l.ticket();
        assert_eq!(l.acquire("db", held, 0, None), Acquire::Granted);
        // Tenant "big" queues 100 steps before "a" and "b" queue 3 each.
        let mut waiting = Vec::new();
        for key in std::iter::repeat_n("big", 100).chain(["a", "b", "a", "b", "a", "b"]) {
            let t = l.ticket();
            let pr = p(0, key);
            assert!(matches!(
                l.acquire_in("db", t, &pr, 0, None),
                Acquire::Wait { .. }
            ));
            waiting.push((t, pr));
        }
        let key_of: FxHashMap<u64, String> = waiting
            .iter()
            .map(|(t, p)| (*t, p.fairness_key.clone()))
            .collect();
        let mut order = Vec::new();
        for _ in 0..9 {
            order.push(key_of[&serve(&l, &mut held, &mut waiting)].clone());
        }
        // Turns go big, a, b, big, a, b, ...: the small tenants are done
        // after nine permits, not after 106.
        assert_eq!(order, ["big", "a", "b", "big", "a", "b", "big", "a", "b"]);
        while !waiting.is_empty() {
            assert_eq!(key_of[&serve(&l, &mut held, &mut waiting)], "big");
        }
    }

    #[test]
    fn weights_give_a_key_more_turns() {
        let l = one_at_a_time();
        let mut held = l.ticket();
        assert_eq!(l.acquire("db", held, 0, None), Acquire::Granted);
        let mut waiting = Vec::new();
        for (key, weight) in
            std::iter::repeat_n(("gold", 3), 9).chain(std::iter::repeat_n(("free", 1), 9))
        {
            let t = l.ticket();
            let pr = Priority {
                level: 0,
                fairness_key: key.into(),
                fairness_weight: weight,
            };
            assert!(matches!(
                l.acquire_in("db", t, &pr, 0, None),
                Acquire::Wait { .. }
            ));
            waiting.push((t, pr));
        }
        let key_of: FxHashMap<u64, String> = waiting
            .iter()
            .map(|(t, p)| (*t, p.fairness_key.clone()))
            .collect();
        let order: Vec<String> = (0..8)
            .map(|_| key_of[&serve(&l, &mut held, &mut waiting)].clone())
            .collect();
        assert_eq!(
            order,
            [
                "gold", "gold", "gold", "free", "gold", "gold", "gold", "free"
            ]
        );
    }

    #[test]
    fn a_withdrawn_waiter_passes_the_turn_on() {
        let mut q = WaitQueue::<u64>::default();
        q.push(1, &p(0, "a"));
        q.push(2, &p(0, "b"));
        q.push(3, &p(0, "a"));
        q.push(4, &p(1, "c"));
        assert!(!q.push(1, &p(7, "z")), "a queued waiter keeps its place");
        assert_eq!(q.next(), Some(4));
        assert!(q.remove(&4));
        assert_eq!(q.next(), Some(1));
        assert!(q.remove(&1));
        // Withdrawing a waiter does not end its key's turn while the key
        // has others; an emptied key leaves the turns.
        assert_eq!(q.next(), Some(3));
        assert_eq!(q.pop_next(), Some(3));
        assert_eq!(q.pop_next(), Some(2));
        assert!(q.is_empty());
        assert_eq!(q.pop_next(), None);
    }

    #[test]
    fn queues_serialize_with_their_turns() {
        let mut q = WaitQueue::<(u64, u64)>::default();
        q.push((1, 1), &p(0, "a"));
        q.push((1, 2), &p(0, "a"));
        q.push((2, 1), &p(0, "b"));
        q.push((2, 2), &p(3, ""));
        assert_eq!(q.pop_next(), Some((2, 2)));
        assert_eq!(q.pop_next(), Some((1, 1)));
        let bytes = serde_json::to_vec(&q).unwrap();
        let mut back: WaitQueue<(u64, u64)> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, q);
        assert_eq!(back.len(), 2);
        assert_eq!(back.pop_next(), Some((2, 1)));
        assert_eq!(back.pop_next(), Some((1, 2)));
        // Default priorities only: a queue saves as a plain arrival-order
        // list, and such a list reads back.
        let old: WaitQueue<(u64, u64)> = serde_json::from_str("[[4,1],[3,9]]").unwrap();
        assert_eq!(old.next(), Some((4, 1)));
        assert_eq!(old.len(), 2);
        assert_eq!(serde_json::to_string(&old).unwrap(), "[[4,1],[3,9]]");
        assert_eq!(
            serde_json::to_string(&WaitQueue::<u64>::default()).unwrap(),
            "[]"
        );
        assert!(serde_json::from_str::<WaitQueue<u64>>("[1,1]").is_err());
    }

    #[test]
    fn answers_round_trip() {
        use answer::*;
        for (asked, t) in [
            (5u64, 5u64),
            (5, 9),
            (9, 5),
            (u64::MAX, 0),
            (1 << 40, (1 << 40) + 3),
            (7 << 40, (7 << 40) + (1 << 39)),
        ] {
            let a = granted(asked, t);
            assert!(a == GRANTED || a > retry_after(u64::MAX));
            assert_eq!(decode(asked, a), Answer::Granted(t));
        }
        assert_eq!(decode(3, QUEUED), Answer::Queued);
        assert_eq!(decode(3, retry_after(250)), Answer::RetryAfter(250));
        assert_eq!(
            decode(3, retry_after(u64::MAX)),
            Answer::RetryAfter((1 << 62) - 1)
        );
    }

    #[test]
    fn rate_limit_refills() {
        let l = LocalLimits::new();
        l.define(
            "api",
            LimitSpec {
                concurrent: 0,
                per_second: 2.0,
            },
        );
        let t: Vec<u64> = (0..3).map(|_| l.ticket()).collect();
        assert_eq!(l.acquire("api", t[0], 1000, None), Acquire::Granted);
        assert_eq!(l.acquire("api", t[1], 1000, None), Acquire::Granted);
        assert_eq!(
            l.acquire("api", t[2], 1000, None),
            Acquire::Wait {
                retry_at_ms: Some(1500)
            }
        );
        assert_eq!(l.acquire("api", t[2], 1500, None), Acquire::Granted);
    }

    #[test]
    fn rate_limit_serves_priority_first() {
        let l = LocalLimits::new();
        l.define(
            "api",
            LimitSpec {
                concurrent: 0,
                per_second: 1.0,
            },
        );
        let (a, low, high) = (l.ticket(), l.ticket(), l.ticket());
        assert_eq!(l.acquire("api", a, 1000, None), Acquire::Granted);
        assert!(matches!(
            l.acquire_in("api", low, &p(0, ""), 1000, None),
            Acquire::Wait {
                retry_at_ms: Some(_)
            }
        ));
        // A later, higher waiter takes the next token.
        assert!(matches!(
            l.acquire_in("api", high, &p(1, ""), 1000, None),
            Acquire::Wait { .. }
        ));
        assert!(matches!(
            l.acquire_in("api", low, &p(0, ""), 2000, None),
            Acquire::Wait { .. }
        ));
        assert_eq!(
            l.acquire_in("api", high, &p(1, ""), 2000, None),
            Acquire::Granted
        );
        assert_eq!(
            l.acquire_in("api", low, &p(0, ""), 3000, None),
            Acquire::Granted
        );
    }
}
