//! An invoked worker's side of an invocation: what each held shard has left
//! to do, the relayed calls it is answering, and the drain before it leaves.
use super::Progress;
use crate::proto;
use fusee::runtime::Due;
use fusee_core::wire::{put_bytes, put_u32, put_u64};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// A shard is idle only if its next due time is at least this far away:
/// waiting that long here is cheaper than another invocation.
pub(crate) const LINGER: Duration = Duration::from_millis(250);

/// How long a shard with messages another shard has not acknowledged
/// stays for the acknowledgments. One that leaves first loses them (they
/// name its lease) and resends from its next invocation.
pub(crate) const ACK_WAIT: Duration = Duration::from_secs(3);

/// How long every held shard stays idle before the worker leaves: work that
/// arrives one call at a time then shares an invocation instead of paying a
/// cold start each.
pub(crate) const QUIET_FOR: Duration = Duration::from_millis(100);

/// A report asked for and not given this long is asked for again.
const ASK_AGAIN: Duration = Duration::from_millis(20);

/// The share of the budget kept for closing the engine and answering.
fn margin(budget: Duration) -> Duration {
    (budget / 10).min(Duration::from_secs(5))
}

type Reply = Result<Vec<u8>, String>;

struct Report {
    due: Due,
    /// The engine closed cleanly after this report.
    closed: bool,
    at: Instant,
    /// Unix time of the report, for its due time.
    now_ms: u64,
    /// Since when the shard has had unacknowledged messages.
    unacked_since: Option<Instant>,
}

enum Drain {
    No,
    Sent(Receiver<Reply>),
    /// The server routes nothing more to this worker since `at`.
    Done(Instant),
}

struct State {
    reports: Vec<Option<Report>>,
    /// Relayed calls being answered: relay ID to whether the call only
    /// waits (for a result or stream chunks), which keeps no shard busy.
    calls: HashMap<u64, bool>,
    drain: Drain,
    /// Types the host had runs of and no code for, at its last poll.
    awaiting: Vec<String>,
    expired: bool,
    /// Per shard: when a report was last asked for.
    asked: Vec<Option<Instant>>,
    /// When the state was last traced.
    traced: Option<Instant>,
    /// Since when every held shard has been idle.
    quiet_since: Option<Instant>,
}

pub(crate) struct Invoked {
    pub(crate) id: u64,
    /// When the worker stops: its budget less the margin.
    stop_at: Instant,
    /// Per shard: a report is wanted at the end of its next tick.
    want: Vec<AtomicBool>,
    state: Mutex<State>,
    finished: AtomicBool,
    /// The invocation is ending: its engine closes.
    leaving: AtomicBool,
}

impl Invoked {
    pub(crate) fn new(id: u64, shards: u32, remaining: Duration) -> Self {
        Self {
            id,
            stop_at: Instant::now() + remaining.saturating_sub(margin(remaining)),
            want: (0..shards).map(|_| AtomicBool::new(false)).collect(),
            state: Mutex::new(State {
                reports: (0..shards).map(|_| None).collect(),
                calls: HashMap::new(),
                drain: Drain::No,
                awaiting: Vec::new(),
                expired: false,
                asked: (0..shards).map(|_| None).collect(),
                traced: None,
                quiet_since: None,
            }),
            finished: AtomicBool::new(false),
            leaving: AtomicBool::new(false),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn wanted(&self, shard: u32) -> bool {
        self.want
            .get(shard as usize)
            .is_some_and(|w| w.load(Ordering::Relaxed))
    }

    pub(crate) fn report(&self, shard: u32, due: Due, closed: bool) {
        if let Some(w) = self.want.get(shard as usize) {
            w.store(false, Ordering::Relaxed);
        }
        let mut st = self.state();
        if let Some(slot) = st.reports.get_mut(shard as usize) {
            let now = Instant::now();
            let unacked_since = match slot.as_ref().and_then(|r| r.unacked_since) {
                _ if due.unacked == 0 => None,
                Some(t) => Some(t),
                None => Some(now),
            };
            *slot = Some(Report {
                due,
                closed,
                at: now,
                now_ms: fusee::local::unix_ms(),
                unacked_since,
            });
        }
    }

    pub(crate) fn called(&self, relay: u64, method: &str) {
        let waits = matches!(method, "result" | "streamRead");
        self.state().calls.insert(relay, waits);
    }

    /// A relayed call is answered (`failed`: with an error): whether to
    /// send the answer. A wait (for a result or stream chunks) that fails
    /// because the invocation ends, its engine closing, is not answered: the
    /// server keeps it for the shard's next holder.
    pub(crate) fn answered(&self, relay: u64, failed: bool) -> bool {
        let waits = self.state().calls.remove(&relay);
        !(failed && waits == Some(true) && self.leaving.load(Ordering::Acquire))
    }

    /// Whether the shard can be left as it is now.
    fn idle(r: &Report, awaiting: &[String]) -> bool {
        let lacks_code = |t: &String| awaiting.contains(t) || r.due.awaiting.contains(t);
        !r.due.busy
            && r.unacked_since.is_none_or(|t| t.elapsed() >= ACK_WAIT)
            && r.due.running.iter().all(lacks_code)
            && r.due
                .at_ms
                .is_none_or(|at| at >= r.now_ms.saturating_add(LINGER.as_millis() as u64))
    }

    /// One look at the invocation (see [`crate::remote::Remote::invocation_poll`]).
    pub(crate) fn poll(
        &self,
        held: &[u32],
        awaiting: &[String],
        notify: &dyn Fn(u32),
        drain: &dyn Fn(&[u32]) -> std::io::Result<Receiver<Reply>>,
    ) -> Progress {
        let p = self.look(held, awaiting, notify, drain);
        if p != Progress::Running && !self.leaving.swap(true, Ordering::AcqRel) {
            // Out of time: route nothing more here while the engine closes.
            let undrained = matches!(self.state().drain, Drain::No);
            if p == Progress::Expired && undrained && !held.is_empty() {
                let _ = drain(held);
            }
        }
        p
    }

    fn look(
        &self,
        held: &[u32],
        awaiting: &[String],
        notify: &dyn Fn(u32),
        drain: &dyn Fn(&[u32]) -> std::io::Result<Receiver<Reply>>,
    ) -> Progress {
        if self.finished.load(Ordering::Acquire) {
            return Progress::Idle;
        }
        let now = Instant::now();
        let mut st = self.state();
        st.awaiting = awaiting.to_vec();
        if now >= self.stop_at {
            st.expired = true;
            return Progress::Expired;
        }
        if held.is_empty() {
            return Progress::Idle;
        }
        // Ask every held shard again; each answers after its next tick. A
        // wake can come before the shard listens for it: ask again.
        for &s in held {
            let Some(w) = self.want.get(s as usize) else {
                continue;
            };
            let stale = st.asked[s as usize].is_none_or(|t| t.elapsed() >= ASK_AGAIN);
            if !w.swap(true, Ordering::Relaxed) || stale {
                st.asked[s as usize] = Some(now);
                drop(st);
                notify(s);
                st = self.state();
            }
        }
        if super::tracing()
            && st
                .traced
                .is_none_or(|t| t.elapsed() >= Duration::from_secs(1))
        {
            st.traced = Some(now);
            for &s in held {
                if let Some(r) = st.reports.get(s as usize).and_then(Option::as_ref) {
                    super::trace!(
                        "invocation {:016x} shard {s}: busy {} running {:?} awaiting {:?} due in {:?} ms, unacked {}, report {} ms old, calls {:?}",
                        self.id,
                        r.due.busy,
                        r.due.running,
                        r.due.awaiting,
                        r.due
                            .at_ms
                            .map(|a| a as i64 - fusee::local::unix_ms() as i64),
                        r.due.unacked,
                        r.at.elapsed().as_millis(),
                        st.calls
                    );
                } else {
                    super::trace!("invocation {:016x} shard {s}: no report yet", self.id);
                }
            }
        }
        let since = match &st.drain {
            Drain::Done(at) => Some(*at),
            _ => None,
        };
        let quiet = st.calls.values().all(|waits| *waits)
            && held.iter().all(|&s| {
                st.reports
                    .get(s as usize)
                    .and_then(Option::as_ref)
                    .is_some_and(|r| since.is_none_or(|t| r.at > t) && Self::idle(r, awaiting))
            });
        if !quiet {
            st.quiet_since = None;
        } else if st.quiet_since.is_none() {
            st.quiet_since = Some(now);
        }
        let settled = st.quiet_since.is_some_and(|t| now - t >= QUIET_FOR);
        match &st.drain {
            Drain::No if settled => {
                // Not under the lock: the connection's reader takes it for
                // every relayed call, and a write may wait for the socket.
                drop(st);
                let sent = drain(held);
                st = self.state();
                match sent {
                    Ok(rx) => st.drain = Drain::Sent(rx),
                    Err(_) => return Progress::Lost,
                }
            }
            Drain::Sent(rx) => match rx.try_recv() {
                // An error answer comes from a server that cannot drain:
                // nothing more is routed here once this worker leaves anyway.
                Ok(_) => st.drain = Drain::Done(Instant::now()),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return Progress::Lost,
            },
            Drain::Done(_) if quiet => return Progress::Idle,
            _ => {}
        }
        Progress::Running
    }

    /// The `IDLE` body for the held shards `(shard, epoch)` whose engine
    /// closed cleanly; `None` once already finished.
    pub(crate) fn finish(&self, held: &[(u32, u64)]) -> Option<Vec<u8>> {
        if self.finished.swap(true, Ordering::AcqRel) {
            return None;
        }
        let st = self.state();
        let now_ms = fusee::local::unix_ms();
        let mut entries = Vec::new();
        for &(shard, epoch) in held {
            let Some(r) = st
                .reports
                .get(shard as usize)
                .and_then(Option::as_ref)
                .filter(|r| r.closed)
            else {
                continue;
            };
            // Runs of types this worker has code for and could not finish.
            let lacks_code = |t: &String| st.awaiting.contains(t) || r.due.awaiting.contains(t);
            let busy = r.due.busy || r.due.running.iter().any(|t| !lacks_code(t));
            let mut types: Vec<&String> = r.due.awaiting.iter().collect();
            for t in r.due.running.iter().chain(&st.awaiting) {
                if lacks_code(t) && r.due.running.contains(t) && !types.contains(&t) {
                    types.push(t);
                }
            }
            let mut flags = 0;
            if busy {
                flags |= proto::IDLE_BUSY;
            }
            if st.expired {
                flags |= proto::IDLE_EXPIRED;
            }
            let due_in = if busy {
                0
            } else {
                r.due.at_ms.map_or(u64::MAX, |at| at.saturating_sub(now_ms))
            };
            let mut e = Vec::new();
            put_u32(&mut e, shard);
            put_u64(&mut e, epoch);
            e.push(flags);
            put_u64(&mut e, due_in);
            put_u64(&mut e, r.due.steps);
            put_u64(&mut e, r.due.open_us);
            put_u32(&mut e, types.len() as u32);
            for t in types {
                put_bytes(&mut e, t.as_bytes());
            }
            entries.push(e);
        }
        let mut body = Vec::new();
        proto::head(
            &mut body,
            proto::Head {
                id: 0,
                shard: 0,
                epoch: 0,
            },
        );
        put_u32(&mut body, entries.len() as u32);
        for e in entries {
            body.extend_from_slice(&e);
        }
        Some(body)
    }

    /// Steps the held shards ran so far, from their latest reports.
    pub(crate) fn steps(&self) -> u64 {
        self.state()
            .reports
            .iter()
            .flatten()
            .map(|r| r.due.steps)
            .sum()
    }

    pub(crate) fn expired(&self) -> bool {
        self.state().expired
    }
}
