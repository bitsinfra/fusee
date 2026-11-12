//! Lease decisions are ordered with workflow batches in the same VSR log.
//! A session starts at its first control op. This gives it a unique persistent
//! identity even when the same process or member becomes primary twice.
use super::{Proposal, Shared, Slot, other};
use crate::limits::{Grant, LEASE_MS, LimitWire, RemoteLimits, ServerLimits, op};
use fusee::limits::{Acquire, LimitBackend, LimitSpec, Priority};
use fusee_core::record::Payload;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::Waker;

// The first varint of an engine batch is its positive first LSN.
const PREFIX: &[u8] = b"\0FSL\x01";

#[derive(Serialize, Deserialize)]
pub(super) struct Command {
    operation: u8,
    name: String,
    ticket: u64,
    concurrent: u32,
    per_second: f64,
    /// An acquire's place in the wait queue; absent for arrival order.
    #[serde(default, skip_serializing_if = "Priority::is_default")]
    priority: Priority,
}

pub(super) enum Request {
    Embedded(Command),
    Claim,
    Probe,
    Worker(u64, Command),
}

impl Command {
    pub(super) fn new(
        operation: u8,
        name: String,
        ticket: u64,
        concurrent: u32,
        per_second: f64,
    ) -> Self {
        Self {
            operation,
            name,
            ticket,
            concurrent,
            per_second,
            priority: Priority::default(),
        }
    }

    pub(super) fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub(super) fn renew() -> Self {
        Self::new(op::RENEW, String::new(), 0, 0, 0.0)
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct Record {
    session: u64,
    time: u64,
    command: Command,
}

pub(super) fn is_control(body: &[u8]) -> bool {
    body.first() == Some(&0)
}

pub(super) fn encode(command: Command, session: u64, time: u64) -> io::Result<Payload> {
    let mut bytes = PREFIX.to_vec();
    serde_json::to_writer(
        &mut bytes,
        &Record {
            session,
            time,
            command,
        },
    )?;
    Ok(Arc::from(bytes))
}

#[derive(Default)]
pub(super) struct Ledger {
    table: ServerLimits,
    session: u64,
    pub(super) time: u64,
}

impl Ledger {
    pub(super) fn snapshot(&self) -> io::Result<Vec<u8>> {
        Ok(serde_json::to_vec(&(
            self.session,
            self.time,
            self.table.snapshot()?,
        ))?)
    }

    pub(super) fn restore(bytes: &[u8]) -> io::Result<Self> {
        let (session, time, limits) = serde_json::from_slice(bytes)?;
        Ok(Self {
            session,
            time,
            table: ServerLimits::from_snapshot(limits)?,
        })
    }

    pub(super) fn apply(&mut self, body: &[u8]) -> io::Result<u64> {
        let record: Record = serde_json::from_slice(
            body.strip_prefix(PREFIX)
                .ok_or_else(|| other("unknown replicated control record"))?,
        )?;
        if record.session < self.session {
            return Err(other("lease session moved backwards"));
        }
        if record.session != self.session {
            self.table.disconnected(self.session);
            self.session = record.session;
        }
        // Logical time advances only by observed elapsed time on one primary.
        // A new primary starts from the last committed time. Old grants can
        // therefore live longer during failover, but can never expire early.
        self.time = self.time.max(record.time);
        let c = record.command;
        match c.operation {
            op::DEFINE => self.table.define(
                &c.name,
                LimitSpec {
                    concurrent: c.concurrent,
                    per_second: c.per_second,
                },
            ),
            op::ACQUIRE => {
                use fusee::limits::answer;
                let grant = self.table.acquire(
                    &c.name,
                    self.session,
                    c.ticket,
                    &c.priority,
                    self.time,
                    LEASE_MS,
                );
                return Ok(match grant {
                    // Another ticket of the same client: within 2^40.
                    Grant::Granted(t) => answer::granted(c.ticket, t),
                    Grant::Queued => answer::QUEUED,
                    Grant::RetryAt(at) => answer::retry_after(at.saturating_sub(self.time)),
                });
            }
            op::RELEASE => self.table.release(&c.name, self.session, c.ticket),
            op::RENEW => self.table.renew(self.session, self.time, LEASE_MS),
            _ => return Err(other("unknown replicated lease operation")),
        }
        Ok(0)
    }
}

struct Wire {
    shared: Arc<Shared>,
    /// The primary epoch the engine runs in (0 until a standby engine's
    /// shards are promoted).
    epoch: Arc<AtomicU64>,
    client: OnceLock<Weak<RemoteLimits>>,
    acquiring: Arc<Mutex<HashSet<u64>>>,
}

impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaLeaseWire")
            .field("epoch", &self.epoch)
            .finish()
    }
}

impl LimitWire for Wire {
    fn send_limit(
        &self,
        operation: u8,
        name: &str,
        ticket: u64,
        concurrent: u32,
        per_second: f64,
        priority: &Priority,
    ) -> io::Result<()> {
        let epoch = self.epoch.load(Ordering::Acquire);
        if epoch != self.shared.epoch.load(Ordering::Acquire) {
            return Err(other("worker lost its primary role"));
        }
        let mut slot = Slot::default();
        if operation == op::ACQUIRE {
            let acquiring = self.acquiring.clone();
            if !acquiring
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(ticket)
            {
                return Ok(());
            }
            let client = self.client.get().cloned().unwrap_or_default();
            slot.callback = Some(Box::new(move |result| {
                acquiring
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&ticket);
                if let Ok(answer) = result
                    && let Some(client) = client.upgrade()
                {
                    use fusee::limits::answer::{Answer, decode};
                    match decode(ticket, answer) {
                        Answer::Granted(t) => client.answered(t, true, 0),
                        Answer::RetryAfter(d) => client.answered(
                            ticket,
                            false,
                            fusee::local::unix_ms().saturating_add(d),
                        ),
                        Answer::Queued => client.answered(ticket, false, 0),
                    }
                }
            }));
        }
        let proposal = Proposal {
            queued: self.shared.traffic.timings.start(),
            body: Arc::from([]),
            last_lsn: 0,
            epoch,
            slot: Arc::new(Mutex::new(slot)),
            control: Some(Request::Embedded(
                Command::new(operation, name.into(), ticket, concurrent, per_second)
                    .with_priority(priority.clone()),
            )),
        };
        if self.shared.proposals.send(proposal).is_err() {
            self.acquiring
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&ticket);
            return Err(other("replica stopped"));
        }
        Ok(())
    }
}

/// Ordinary workflows allocate no lease worker and produce no control records.
pub(super) struct Backend {
    shared: Arc<Shared>,
    /// The primary epoch the engine runs in; 0 for a standby engine not yet
    /// promoted (its shards, or this backend, set it once the member is the
    /// ready primary).
    epoch: Arc<AtomicU64>,
    client: OnceLock<Arc<RemoteLimits>>,
    /// Limits a standby engine defined before its promotion, sent first
    /// once it runs.
    pending: Mutex<Vec<(String, LimitSpec)>>,
}

impl std::fmt::Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicatedLimits")
            .field("epoch", &self.epoch)
            .finish()
    }
}

impl Backend {
    pub(super) fn new(shared: Arc<Shared>, epoch: u64) -> Self {
        Self::standby(shared, Arc::new(AtomicU64::new(epoch)))
    }

    /// A standby engine's: its epoch is set when its shards are promoted.
    pub(super) fn standby(shared: Arc<Shared>, epoch: Arc<AtomicU64>) -> Self {
        Self {
            shared,
            epoch,
            client: OnceLock::new(),
            pending: Mutex::new(Vec::new()),
        }
    }

    /// Whether the engine's epoch is known: a standby engine's is set here
    /// if this member is now the ready primary (the shards agree on it, or
    /// fail). Then sends the limits defined while it stood by.
    fn running(&self) -> bool {
        if self.epoch.load(Ordering::Acquire) == 0 {
            if !self.shared.status.borrow().ready {
                return false;
            }
            let now = self.shared.epoch.load(Ordering::Acquire);
            let _ = self
                .epoch
                .compare_exchange(0, now, Ordering::AcqRel, Ordering::Acquire);
        }
        // Held while sending, so no acquire overtakes a definition.
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        for (name, spec) in pending.drain(..) {
            self.client().define(&name, spec);
        }
        true
    }
    fn client(&self) -> &Arc<RemoteLimits> {
        self.client.get_or_init(|| {
            let wire = Arc::new(Wire {
                shared: self.shared.clone(),
                epoch: self.epoch.clone(),
                client: OnceLock::new(),
                acquiring: Arc::default(),
            });
            let client = RemoteLimits::new(wire.clone(), 0);
            let _ = wire.client.set(Arc::downgrade(&client));
            client
        })
    }
}
impl LimitBackend for Backend {
    fn define(&self, name: &str, spec: LimitSpec) {
        if !self.running() {
            self.pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((name.to_owned(), spec));
            return;
        }
        self.client().define(name, spec);
    }
    fn ticket(&self) -> u64 {
        self.shared.next_ticket.fetch_add(1, Ordering::Relaxed)
    }
    fn acquire_in(
        &self,
        name: &str,
        ticket: u64,
        priority: &Priority,
        now: u64,
        waker: Option<&Waker>,
    ) -> Acquire {
        if !self.running()
            || self.epoch.load(Ordering::Acquire) != self.shared.epoch.load(Ordering::Acquire)
        {
            return Acquire::Wait { retry_at_ms: None };
        }
        self.client().acquire_in(name, ticket, priority, now, waker)
    }
    fn release(&self, name: &str, ticket: u64) {
        if let Some(client) = self.client.get() {
            client.release(name, ticket);
        }
    }
    fn in_use(&self, name: &str) -> u32 {
        self.client.get().map_or(0, |c| c.in_use(name))
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    #[test]
    fn checkpoint_preserves_lease_sessions_rate_balances_and_waiter_order() {
        let mut ledger = Ledger::default();
        let command = |op, ticket, session, time| {
            encode(
                Command::new(op, "api".into(), ticket, 1, 4.0),
                session,
                time,
            )
            .unwrap()
        };
        ledger.apply(&command(op::DEFINE, 0, 4, 100)).unwrap();
        assert_eq!(ledger.apply(&command(op::ACQUIRE, 2, 4, 100)).unwrap(), 1);
        assert_eq!(ledger.apply(&command(op::ACQUIRE, 3, 4, 150)).unwrap(), 0);
        assert_eq!(ledger.apply(&command(op::ACQUIRE, 4, 4, 165)).unwrap(), 0);
        let saved = ledger.snapshot().unwrap();
        let mut copy = Ledger::restore(&saved).unwrap();
        assert_eq!(saved, copy.snapshot().unwrap());
        for msg in [
            command(op::RELEASE, 2, 4, 170),
            command(op::ACQUIRE, 4, 4, 200),
            command(op::ACQUIRE, 3, 4, 200),
            command(op::RENEW, 0, 4, 1000),
            command(op::ACQUIRE, 1, 9, 1000),
            command(op::ACQUIRE, 1, 9, 4001),
        ] {
            assert_eq!(ledger.apply(&msg).unwrap(), copy.apply(&msg).unwrap());
        }
        assert_eq!(ledger.snapshot().unwrap(), copy.snapshot().unwrap());
    }

    #[test]
    fn priorities_replicate_and_grants_name_the_ticket_served() {
        use fusee::limits::answer::{Answer, decode};
        let mut ledger = Ledger::default();
        let at = |level| Priority {
            level,
            ..Priority::default()
        };
        let command = |op, ticket, p: Priority, time| {
            encode(
                Command::new(op, "one".into(), ticket, 1, 0.0).with_priority(p),
                4,
                time,
            )
            .unwrap()
        };
        ledger
            .apply(&command(op::DEFINE, 0, Priority::default(), 1))
            .unwrap();
        assert_eq!(
            ledger
                .apply(&command(op::ACQUIRE, 1, Priority::default(), 1))
                .unwrap(),
            1
        );
        assert_eq!(ledger.apply(&command(op::ACQUIRE, 2, at(0), 2)).unwrap(), 0);
        assert_eq!(ledger.apply(&command(op::ACQUIRE, 3, at(9), 3)).unwrap(), 0);
        // The checkpoint carries the queue with its levels.
        let saved = ledger.snapshot().unwrap();
        let mut copy = Ledger::restore(&saved).unwrap();
        assert_eq!(saved, copy.snapshot().unwrap());
        let mut answers = Vec::new();
        for msg in [
            command(op::RELEASE, 1, Priority::default(), 4),
            // Ticket 2 rings; ticket 3, queued later at a higher level, is
            // the one granted, and the answer names it.
            command(op::ACQUIRE, 2, at(0), 5),
            command(op::RELEASE, 3, Priority::default(), 6),
            command(op::ACQUIRE, 2, at(0), 7),
        ] {
            let a = ledger.apply(&msg).unwrap();
            assert_eq!(a, copy.apply(&msg).unwrap());
            answers.push(a);
        }
        assert_eq!(decode(2, answers[1]), Answer::Granted(3));
        assert_eq!(decode(2, answers[3]), Answer::Granted(2));
        assert_eq!(ledger.snapshot().unwrap(), copy.snapshot().unwrap());
    }
}
