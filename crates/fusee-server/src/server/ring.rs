//! The server's end of the same-host shared-memory transport
//! ([`crate::shm`]): one thread polls every worker's ring, stages the
//! sections it finds with the same epoch and LSN checks as the socket path,
//! writes them as one batch, and the workers' watermarks are stored by the
//! sink of each shard's lease.

use super::SharedLog;
use super::log::{Mark, Sink};
use super::shard::{Inbox, Outbox};
use crate::proto::{self, req, resp};
use crate::shm::{self, Doorbell, Shared};
use fusee_core::frame;
use fusee_core::wire::{Reader, put_bytes, put_u32, put_u64};
use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// A ring frame's payload: `kind u8, shard u32, epoch u64, flags u8`, then
/// the request body (for `APPEND`, [`proto::encode_section`]).
pub(crate) fn ring_head(out: &mut Vec<u8>, kind: u8, shard: u32, epoch: u64, flags: u8) {
    out.push(kind);
    put_u32(out, shard);
    put_u64(out, epoch);
    out.push(flags);
}

/// The longest the log thread spins for the next frame before it sleeps.
/// It spins for twice the longer of the recent flush and the workers'
/// recent turnaround (from publishing a shard's position to its next
/// section): the next frame is about that far away.
const SPIN_CAP: Duration = Duration::from_micros(100);

/// One worker connection using shared memory.
pub(crate) struct ShmConn {
    pub(crate) conn: u64,
    pub(crate) shared: Shared,
    /// The consumer's position; only the log thread moves it.
    head: AtomicU64,
    outbox: Arc<Outbox>,
    /// The shard threads of the lease, for their stopped flags.
    inboxes: Mutex<Vec<Option<Arc<Inbox>>>>,
    /// Shards whose ring frames were refused: later ones are too.
    refused: Mutex<HashSet<u32>>,
    closed: AtomicBool,
    /// Set when the worker broke the ring protocol; the connection task
    /// revokes the lease.
    pub(crate) corrupt: Mutex<Option<&'static str>>,
    pub(crate) revoke: tokio::sync::Notify,
}

impl ShmConn {
    pub(crate) fn new(conn: u64, shared: Shared, outbox: Arc<Outbox>, shards: u32) -> Self {
        Self {
            conn,
            head: AtomicU64::new(shared.consumed()),
            shared,
            outbox,
            inboxes: Mutex::new(vec![None; shards as usize]),
            refused: Mutex::new(HashSet::new()),
            closed: AtomicBool::new(false),
            corrupt: Mutex::new(None),
            revoke: tokio::sync::Notify::new(),
        }
    }

    /// `shard` was granted to this connection under `epoch`.
    pub(crate) fn granted(&self, shard: u32, epoch: u64, inbox: Arc<Inbox>) {
        if let Some(slot) = self
            .inboxes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(shard as usize)
        {
            *slot = Some(inbox);
        }
        self.refused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&shard);
        if let Some(line) = self.shared.line(shard) {
            line.failed().store(0, Ordering::Relaxed);
            line.epoch().store(epoch, Ordering::Release);
        }
    }

    /// The lease ended: the worker's waiters stop waiting, and no frame of
    /// its ring is staged any more.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.shared.closed().store(1, Ordering::Release);
        for shard in 0..self.shared.layout().shards {
            if let Some(line) = self.shared.line(shard) {
                line.epoch().store(0, Ordering::Release);
                line.publish();
            }
        }
    }

    /// Refuses `shard`'s frames from now on and tells the worker.
    fn refuse(&self, shard: u32, message: &str) {
        self.refused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(shard);
        // The line first: whoever hears of it on the socket sees it there.
        if let Some(line) = self.shared.line(shard) {
            line.failed().store(1, Ordering::Release);
            line.publish();
        }
        let mut b = Vec::new();
        put_u64(&mut b, 0);
        put_u32(&mut b, shard);
        put_bytes(&mut b, message.as_bytes());
        let mut f = Vec::new();
        frame::encode(resp::IO_FAILED, &b, &mut f);
        self.outbox.send(&f);
    }

    /// Stages one ring frame. `Err` only when the frame breaks the protocol.
    fn take(&self, log: &SharedLog, payload: &[u8]) -> Result<(), &'static str> {
        let mut r = Reader::new(payload);
        let (Ok(kind), Ok(shard), Ok(epoch), Ok(_flags)) = (r.u8(), r.u32(), r.u64(), r.u8())
        else {
            return Err("ring frame too short");
        };
        if kind != req::APPEND {
            return Err("unknown ring frame");
        }
        if shard >= self.shared.layout().shards {
            return Err("ring frame for an unknown shard");
        }
        if self
            .refused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&shard)
        {
            return Ok(());
        }
        // A shard whose thread stopped it (a failed run-store append)
        // accepts no more records in this lease.
        let stopped = self
            .inboxes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(shard as usize)
            .and_then(Option::as_ref)
            .is_none_or(|i| i.is_stopped());
        if stopped {
            self.refuse(
                shard,
                &format!("shard {shard}: this worker does not hold it or it stopped"),
            );
            return Ok(());
        }
        let res = proto::decode_section(&mut r)
            .map_err(|e| io::Error::other(e.to_string()))
            .and_then(|batch| log.stage(shard, epoch, batch));
        if let Err(e) = res {
            self.refuse(shard, &format!("shard {shard}: {e}"));
        }
        Ok(())
    }
}

/// Durable positions for a shared-memory connection: stored in the
/// worker's watermark lines, and sent as `DURABLE` frames only for shards
/// whose worker waits on the socket (`notify`).
pub(crate) struct ShmSink {
    pub(crate) conn: Arc<ShmConn>,
}

impl Sink for ShmSink {
    fn durable(&self, marks: &[Mark], position: u64, segment: u64) {
        let s = &self.conn.shared;
        s.position().fetch_max(position, Ordering::AcqRel);
        s.segment().fetch_max(segment, Ordering::AcqRel);
        let mut socket = Vec::new();
        for m in marks {
            let Some(line) = s.line(m.shard) else {
                continue;
            };
            line.pins().store(u32::from(m.pins), Ordering::Relaxed);
            // After the position and pins it covers.
            line.durable().fetch_max(m.lsn, Ordering::AcqRel);
            line.publish();
            if line.notify().load(Ordering::Acquire) != 0 {
                socket.push(*m);
            }
        }
        if !socket.is_empty() {
            self.conn
                .outbox
                .send(&super::durable_frame(&socket, position, segment));
        }
    }

    fn failed(&self, shards: &[u32], message: &str) {
        for &shard in shards {
            self.conn.refuse(shard, message);
        }
    }
}

/// Every shared-memory connection of a server, and the thread polling
/// their rings.
pub(crate) struct Hub {
    pub(crate) bell: Doorbell,
    conns: Mutex<Vec<Arc<ShmConn>>>,
    generation: AtomicU64,
    log: Arc<SharedLog>,
    stop: AtomicBool,
}

impl Hub {
    pub(crate) fn start(log: Arc<SharedLog>) -> io::Result<Arc<Self>> {
        let hub = Arc::new(Self {
            bell: Doorbell::create()?,
            conns: Mutex::new(Vec::new()),
            generation: AtomicU64::new(0),
            log,
            stop: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&hub);
        std::thread::Builder::new()
            .name("fusee-server-shm".into())
            .spawn(move || poll(weak))?;
        Ok(hub)
    }

    pub(crate) fn add(&self, conn: Arc<ShmConn>) {
        self.conns
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(conn);
        self.generation.fetch_add(1, Ordering::AcqRel);
        // The thread may be asleep with an old list.
        self.bell.ring_always();
    }

    pub(crate) fn remove(&self, conn: u64) {
        self.conns
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|c| c.conn != conn);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

/// The log thread: drains every ring, writes what it staged, and when no
/// frame comes for twice the recent flush time, sleeps on the doorbell.
fn poll(hub: Weak<Hub>) {
    let mut conns: Vec<Arc<ShmConn>> = Vec::new();
    let mut seen = u64::MAX;
    let mut buf = Vec::new();
    loop {
        let Some(h) = hub.upgrade() else { return };
        if h.stop.load(Ordering::Acquire) {
            return;
        }
        let generation = h.generation.load(Ordering::Acquire);
        if generation != seen {
            conns = h.conns.lock().unwrap_or_else(|e| e.into_inner()).clone();
            seen = generation;
        }
        let mut staged = false;
        for c in &conns {
            if c.closed.load(Ordering::Acquire) {
                continue;
            }
            let mut head = c.head.load(Ordering::Relaxed);
            loop {
                match c.shared.pop(&mut head, &mut buf) {
                    Ok(true) => match c.take(&h.log, &buf) {
                        Ok(()) => staged = true,
                        Err(why) => {
                            corrupt(c, why);
                            break;
                        }
                    },
                    Ok(false) => break,
                    Err(shm::Corrupt(why)) => {
                        corrupt(c, why);
                        break;
                    }
                }
            }
            c.head.store(head, Ordering::Relaxed);
            c.shared.release(head);
        }
        if staged {
            // Failures reach every shard's sink.
            let _ = h.log.write_staged(true);
            continue;
        }
        let pending = |conns: &[Arc<ShmConn>]| {
            conns.iter().any(|c| {
                !c.closed.load(Ordering::Acquire)
                    && c.shared.pending(c.head.load(Ordering::Relaxed))
            })
        };
        // The next frame comes a worker's turnaround after the positions it
        // waited for were published, or a flush after the section before.
        let expect = h.log.last_flush_us().max(h.log.turnaround_us()).max(1);
        let budget = crate::spin::budget(Duration::from_micros(expect), SPIN_CAP);
        if crate::spin::spin_until(budget, &|| pending(&conns)) {
            continue;
        }
        let bell = &h.bell;
        let gen_now = &h.generation;
        bell.sleep(
            &|| pending(&conns) || gen_now.load(Ordering::Acquire) != seen,
            Duration::from_millis(100),
        );
    }
}

fn corrupt(c: &ShmConn, why: &'static str) {
    c.closed.store(true, Ordering::Release);
    *c.corrupt.lock().unwrap_or_else(|e| e.into_inner()) = Some(why);
    c.revoke.notify_one();
}

#[cfg(test)]
mod tests;
