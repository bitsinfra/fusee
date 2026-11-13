//! The server's shared log (log format 3): one write-ahead log for every
//! shard the server holds, whichever worker connection runs the shard.
//!
//! **Appends.** A worker sends each shard's records as one section
//! (`APPEND`); the server stages it and does not answer it on its own.
//! Whoever writes next takes every staged section, from every connection,
//! and writes them as one batch with one write (and, without `RWF_DSYNC`,
//! one sync). When a batch is durable, each connection holding one of its
//! shards gets one `DURABLE` frame with the cumulative durable LSN of each
//! such shard. Appends are therefore pipelined: a worker never waits for a
//! reply before it sends the next section of another shard.
//!
//! **Who writes.** When nothing is in flight and the disk is fast, the
//! thread that appends writes the batch itself (the server's shard thread
//! for that shard), outside the log lock. Otherwise the section stays
//! staged and an I/O thread writes it as soon as fewer than
//! [`WalOptions::in_flight`] writes are in flight; batches of overlapping
//! writes are padded to the end of their last block
//! ([`fusee_core::wal::batch_span`]), so two writes in flight never share a
//! block under `O_DIRECT`, and positions are published in batch order.
//!
//! **Leases.** Each shard has the epoch of the lease that may append to it.
//! Granting a shard to another lease drops whatever the previous holder had
//! staged (none of it was acknowledged) and remembers the last batch sealed
//! so far: the new holder's recovery waits until that batch is durable, so
//! every record of the previous holder that can still reach the disk is in
//! what recovery reads, and none can follow the new holder's records. The
//! new holder's appends must then continue its shard's LSNs exactly.
//!
//! **Recovery.** At open the log is read once and each shard's sections are
//! kept until its first lease recovers it. A shard granted again later is
//! read back from the disk: one read of the chain serves every shard
//! granted since the last read. Segment retirement waits while a read runs.
//!
//! [`ServerWal`] is the deterministic part; [`LogService`] adds threads.

use fusee::wal::{InFlight, LogDevice, RawSection, Wal, WalOptions, read_sections};
use fusee_core::Lsn;
use fusee_core::segment::Batch;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// A shard's durable position after a write, for its holder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Mark {
    pub shard: u32,
    pub lsn: Lsn,
    /// The shard alone holds back retiring the oldest segment: one more
    /// checkpoint of it lets the log reuse that segment.
    pub pins: bool,
}

/// Where a shard's durable positions go: the connection holding its lease.
pub(crate) trait Sink: Send + Sync {
    /// Positions that advanced, all of shards this sink holds.
    fn durable(&self, marks: &[Mark], position: u64, segment: u64);
    /// The log failed; these shards can no longer be written.
    fn failed(&self, shards: &[u32], message: &str);
}

/// What recovery of a shard needs next.
#[derive(Debug)]
pub(crate) enum Recovery {
    /// The shard's sections, oldest first.
    Ready(Vec<RawSection>),
    /// Read these shards back from the disk, through the snapshot's last
    /// durable batch, then call [`ServerWal::scanned`].
    Scan(fusee::wal::Chain, Vec<u32>),
    /// Another thread is reading the shard back; wait for it.
    Wait,
}

#[derive(Debug)]
enum State {
    /// Still holds the sections read when the log opened.
    Fresh,
    /// Granted again: must be read back from the disk.
    Stale,
    /// Being read back.
    Scanning,
    Cached(Vec<RawSection>),
    /// Recovered by its current lease.
    Recovered,
}

struct ShardState {
    epoch: u64,
    state: State,
    /// Recovery of the current lease waits until this batch is durable:
    /// the last one sealed when the shard was granted.
    fence: u64,
    /// The LSN the next section must start with, once recovered.
    next: Option<Lsn>,
    /// Retention reported while a read-back ran, applied after it.
    deferred: Option<Lsn>,
}

/// The deterministic part of the server log: lease epochs, staging,
/// recovery state and retention over a [`Wal`]. Not thread-safe.
pub(crate) struct ServerWal<D: LogDevice> {
    wal: Wal<D>,
    shards: Vec<ShardState>,
    scanning: usize,
    failed: Option<String>,
    /// Shards a write made durable since the last seal that have staged
    /// nothing since: they usually send their next sections together, so
    /// the next batch waits briefly for them ([`LogService`]).
    expected: Vec<bool>,
    /// When each shard's position was last published (µs, 0: not since
    /// its last section), to measure how soon its next section follows.
    released: Vec<u64>,
}

impl<D: LogDevice> ServerWal<D> {
    pub fn open(dev: D, shards: u32, opts: WalOptions, life: u64, now_ms: u64) -> io::Result<Self> {
        let wal = Wal::open(dev, shards, opts, life, now_ms)?;
        Ok(Self {
            wal,
            shards: (0..shards)
                .map(|_| ShardState {
                    epoch: 0,
                    state: State::Fresh,
                    fence: 0,
                    next: None,
                    deferred: None,
                })
                .collect(),
            scanning: 0,
            failed: None,
            expected: vec![false; shards as usize],
            released: vec![0; shards as usize],
        })
    }

    pub fn wal(&self) -> &Wal<D> {
        &self.wal
    }

    #[cfg(test)]
    pub fn wal_mut(&mut self) -> &mut Wal<D> {
        &mut self.wal
    }

    pub fn failed(&self) -> Option<&str> {
        self.failed.as_deref()
    }

    pub fn fail(&mut self, message: String) {
        self.failed.get_or_insert(message);
    }

    fn shard(&mut self, shard: u32) -> io::Result<&mut ShardState> {
        self.shards
            .get_mut(shard as usize)
            .ok_or_else(|| other(format!("no shard {shard}")))
    }

    /// Hands `shard` to the lease with `epoch`: drops what the previous
    /// holder staged, and makes its recovery wait for every batch sealed
    /// so far.
    pub fn grant(&mut self, shard: u32, epoch: u64) -> io::Result<()> {
        let sealed = self.wal.position().saturating_sub(1);
        self.wal.discard_staged(shard);
        let s = self.shard(shard)?;
        s.epoch = epoch;
        s.fence = sealed;
        s.next = None;
        if !matches!(s.state, State::Fresh) {
            s.state = State::Stale;
        }
        Ok(())
    }

    /// The lease with `epoch` ended: what it staged is dropped. The shard
    /// keeps its epoch, so later appends of that lease are refused.
    pub fn release(&mut self, shard: u32, epoch: u64) {
        if let Some(s) = self.shards.get_mut(shard as usize)
            && s.epoch == epoch
        {
            s.next = None;
            if matches!(s.state, State::Recovered | State::Cached(_)) {
                s.state = State::Stale;
            }
            self.wal.discard_staged(shard);
        }
    }

    fn check(&self, shard: u32, epoch: u64) -> io::Result<&ShardState> {
        if let Some(m) = &self.failed {
            return Err(other(m));
        }
        let s = self
            .shards
            .get(shard as usize)
            .ok_or_else(|| other(format!("no shard {shard}")))?;
        if s.epoch != epoch || epoch == 0 {
            return Err(other(
                "lease epoch is stale: another worker holds the shard",
            ));
        }
        Ok(s)
    }

    /// The batch recovery of `shard` must see durable first.
    pub fn fence(&self, shard: u32) -> u64 {
        self.shards.get(shard as usize).map_or(0, |s| s.fence)
    }

    /// What recovering `shard` for the lease with `epoch` needs next. The
    /// fence must be durable (`acked() >= fence(shard)`).
    pub fn recovery(&mut self, shard: u32, epoch: u64) -> io::Result<Recovery> {
        self.check(shard, epoch)?;
        if self.wal.acked() < self.fence(shard) {
            return Ok(Recovery::Wait);
        }
        let state = std::mem::replace(&mut self.shard(shard)?.state, State::Recovered);
        match state {
            State::Fresh => Ok(Recovery::Ready(self.wal.take_recovered_sections(shard))),
            State::Cached(sections) => Ok(Recovery::Ready(sections)),
            State::Recovered => Err(other(format!(
                "shard {shard} was already recovered by this lease"
            ))),
            State::Scanning => {
                self.shard(shard)?.state = State::Scanning;
                Ok(Recovery::Wait)
            }
            State::Stale => {
                // One read serves every shard waiting to be read back whose
                // previous holder's writes are all durable.
                let acked = self.wal.acked();
                let mut wanted = Vec::new();
                for (i, s) in self.shards.iter_mut().enumerate() {
                    if i as u32 == shard || (matches!(s.state, State::Stale) && s.fence <= acked) {
                        s.state = State::Scanning;
                        wanted.push(i as u32);
                    }
                }
                self.scanning += 1;
                Ok(Recovery::Scan(self.wal.chain(), wanted))
            }
        }
    }

    /// Stores what a read-back found, and applies retention reported
    /// meanwhile once no read-back runs.
    pub fn scanned(&mut self, shards: &[u32], found: io::Result<Vec<Vec<RawSection>>>) {
        self.scanning = self.scanning.saturating_sub(1);
        match found {
            Ok(found) => {
                for (&shard, sections) in shards.iter().zip(found) {
                    if let Some(s) = self.shards.get_mut(shard as usize)
                        && matches!(s.state, State::Scanning)
                    {
                        s.state = State::Cached(sections);
                    }
                }
            }
            Err(_) => {
                for &shard in shards {
                    if let Some(s) = self.shards.get_mut(shard as usize)
                        && matches!(s.state, State::Scanning)
                    {
                        s.state = State::Stale;
                    }
                }
            }
        }
        if self.scanning == 0 {
            for shard in 0..self.shards.len() as u32 {
                if let Some(lsn) = self.shards[shard as usize].deferred.take()
                    && let Err(e) = self.wal.set_retain(shard, lsn)
                {
                    self.fail(format!("log segment retirement failed: {e}"));
                }
            }
        }
    }

    /// Recovery of `shard` finished at `last`: its next section starts
    /// right after.
    pub fn recovered(&mut self, shard: u32, epoch: u64, last: Lsn) -> io::Result<()> {
        self.check(shard, epoch)?;
        self.shard(shard)?.next = Some(last + 1);
        Ok(())
    }

    /// Stages a section of `shard` from the lease with `epoch`. Refused if
    /// the lease is stale, the shard was not recovered by it, or the
    /// section does not continue the shard's records exactly.
    pub fn append(&mut self, shard: u32, epoch: u64, batch: Batch) -> io::Result<()> {
        let next = self.check(shard, epoch)?.next;
        match next {
            None => {
                return Err(other(format!(
                    "shard {shard} must be recovered before it is appended to"
                )));
            }
            Some(n) if n != batch.first_lsn || batch.last_lsn < batch.first_lsn => {
                return Err(other(format!(
                    "shard {shard} section {}..={} does not follow LSN {}",
                    batch.first_lsn,
                    batch.last_lsn,
                    n - 1
                )));
            }
            Some(_) => {}
        }
        let last = batch.last_lsn;
        self.wal.stage(shard, batch)?;
        self.shard(shard)?.next = Some(last + 1);
        if let Some(e) = self.expected.get_mut(shard as usize) {
            *e = false;
        }
        Ok(())
    }

    /// How long after its position was published `shard` staged its next
    /// section, once per publication.
    pub fn turnaround(&mut self, shard: u32, now_us: u64) -> Option<u64> {
        let at = std::mem::take(self.released.get_mut(shard as usize)?);
        (at != 0).then(|| now_us.saturating_sub(at))
    }

    /// Whether a shard made durable since the last seal has not sent its
    /// next section yet.
    pub fn awaits_cohort(&self) -> bool {
        self.expected.iter().any(|e| *e)
    }

    /// Seals what is staged; shards still expected are no longer waited for.
    pub fn seal(&mut self, now_ms: u64, overlap: bool) -> io::Result<Option<InFlight>> {
        let w = self.wal.seal(now_ms, overlap)?;
        if w.is_some() {
            self.expected.fill(false);
        }
        Ok(w)
    }

    /// Records that a write is durable; the positions it advanced.
    pub fn complete(&mut self, w: InFlight, now_us: u64) -> Vec<Mark> {
        let shards = self.wal.complete(w);
        for &shard in &shards {
            if let Some(e) = self.expected.get_mut(shard as usize) {
                *e = true;
            }
            if let Some(r) = self.released.get_mut(shard as usize) {
                *r = now_us.max(1);
            }
        }
        shards
            .into_iter()
            .map(|shard| Mark {
                shard,
                lsn: self.wal.durable(shard),
                pins: false,
            })
            .collect()
    }

    /// `lsn`: what the shard's oldest retained checkpoint covers.
    pub fn set_retain(&mut self, shard: u32, lsn: Lsn) -> io::Result<()> {
        if self.scanning > 0 {
            if let Some(s) = self.shards.get_mut(shard as usize) {
                s.deferred = Some(lsn);
            }
            return Ok(());
        }
        self.wal.set_retain(shard, lsn)
    }

    pub fn durable(&self, shard: u32) -> Lsn {
        self.wal.durable(shard)
    }
}

/// [`ServerWal`] shared by threads: shard threads append and recover, I/O
/// threads write and sync, and durable positions go to each shard's
/// [`Sink`].
pub(crate) struct LogService<D: LogDevice + Clone + Send + Sync + 'static> {
    dev: D,
    state: Mutex<ServerWal<D>>,
    sinks: Mutex<Vec<Option<(u64, Arc<dyn Sink>)>>>,
    /// Signalled whenever a write completes, a read-back ends, or the log
    /// fails.
    published: Condvar,
    jobs: Mutex<VecDeque<(InFlight, Instant)>>,
    job_ready: Condvar,
    queued: AtomicU64,
    stop: AtomicBool,
    spin_cap_us: u64,
    last_flush_us: AtomicU64,
    position: AtomicU64,
    segment: AtomicU64,
    /// Monotonic µs since which the next batch waits for the shards a write
    /// just made durable; 0 while it does not.
    gathering_since: AtomicU64,
    /// Smoothed time from publishing a shard's position to its next
    /// section, in µs.
    turnaround_us: AtomicU64,
}

/// An inline write only on a disk whose last write took at most this.
const INLINE_MAX_US: u64 = 200;

/// How long the next batch waits for the shards the last write made
/// durable: three times their usual turnaround (from publishing a shard's
/// position to its next section), at least this, and at most a quarter of
/// a flush. Their next sections arrive within about a round trip of each
/// other; sealing at the first would split them over several writes, and
/// once more groups of shards take turns than writes may overlap, every
/// step waits for writes of the others. The batch is sealed as soon as all
/// of them arrived, so a lone shard never waits.
const GATHER_MIN_US: u64 = 20;
/// A turnaround longer than this and than a flush says nothing about how
/// soon shards come back (the shard was idle); it is not counted.
const TURNAROUND_MAX_US: u64 = 200;

impl<D: LogDevice + Clone + Send + Sync + 'static> LogService<D> {
    pub fn open(dev: D, shards: u32, opts: WalOptions) -> io::Result<Arc<Self>> {
        let wal = ServerWal::open(
            dev.clone(),
            shards,
            opts,
            fusee::wal::new_life(),
            fusee::local::unix_ms(),
        )?;
        let position = wal.wal().position();
        let segment = wal.wal().active_segment();
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        let service = Arc::new(Self {
            dev,
            state: Mutex::new(wal),
            sinks: Mutex::new((0..shards).map(|_| None).collect()),
            published: Condvar::new(),
            jobs: Mutex::new(VecDeque::new()),
            job_ready: Condvar::new(),
            queued: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            spin_cap_us: if cores > 2 { 50 } else { 0 },
            last_flush_us: AtomicU64::new(0),
            position: AtomicU64::new(position),
            segment: AtomicU64::new(segment),
            gathering_since: AtomicU64::new(0),
            turnaround_us: AtomicU64::new(0),
        });
        for i in 0..opts.in_flight.max(1) {
            let weak = Arc::downgrade(&service);
            std::thread::Builder::new()
                .name(format!("fusee-server-log-{i}"))
                .spawn(move || io_thread(weak))?;
        }
        Ok(service)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ServerWal<D>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The sequence of the next batch (a checkpoint taken now covers every
    /// earlier one) and the active segment.
    pub fn position(&self) -> (u64, u64) {
        (
            self.position.load(Ordering::Acquire),
            self.segment.load(Ordering::Acquire),
        )
    }

    pub fn last_flush_us(&self) -> u64 {
        self.last_flush_us.load(Ordering::Relaxed)
    }

    /// Smoothed time from publishing a shard's position to its next
    /// section, in µs (0 until measured).
    pub fn turnaround_us(&self) -> u64 {
        self.turnaround_us.load(Ordering::Relaxed)
    }

    pub fn pins(&self, shard: u32) -> bool {
        self.lock().wal().pins(shard)
    }

    pub fn durable(&self, shard: u32) -> Lsn {
        self.lock().durable(shard)
    }

    /// Hands `shard` to the lease with `epoch`, whose durable positions go
    /// to `sink`.
    pub fn grant(&self, shard: u32, epoch: u64, sink: Arc<dyn Sink>) -> io::Result<()> {
        self.lock().grant(shard, epoch)?;
        if let Some(slot) = self
            .sinks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(shard as usize)
        {
            *slot = Some((epoch, sink));
        }
        Ok(())
    }

    /// The lease with `epoch` ended.
    pub fn release(&self, shard: u32, epoch: u64) {
        self.lock().release(shard, epoch);
        if let Some(slot) = self
            .sinks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(shard as usize)
            && slot.as_ref().is_some_and(|(e, _)| *e == epoch)
        {
            *slot = None;
        }
    }

    /// `shard`'s sections whose records reach past `after`, oldest first,
    /// and its last LSN in the log. From now on its sections must start
    /// right after that LSN.
    pub fn recover(
        &self,
        shard: u32,
        epoch: u64,
        after: Lsn,
    ) -> io::Result<(Vec<RawSection>, Lsn)> {
        let mut st = self.lock();
        let sections = loop {
            match st.recovery(shard, epoch)? {
                Recovery::Ready(sections) => break sections,
                Recovery::Wait => {
                    if let Some(m) = st.failed() {
                        return Err(other(m));
                    }
                    st = self
                        .published
                        .wait_timeout(st, Duration::from_millis(10))
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                }
                Recovery::Scan(chain, wanted) => {
                    drop(st);
                    let found = read_sections(&self.dev, &chain, &wanted);
                    st = self.lock();
                    let error = found.as_ref().err().map(|e| e.to_string());
                    st.scanned(&wanted, found);
                    self.published.notify_all();
                    if let Some(e) = error {
                        return Err(other(format!("reading the log back failed: {e}")));
                    }
                }
            }
        };
        let mut last = after;
        let mut kept = Vec::with_capacity(sections.len());
        for (first, count, bytes) in sections {
            let end = first + u64::from(count) - 1;
            last = last.max(end);
            if end > after {
                kept.push((first, count, bytes));
            }
        }
        st.recovered(shard, epoch, last)?;
        Ok((kept, last))
    }

    /// Reports retention: what the shard's oldest retained checkpoint
    /// covers.
    pub fn set_retain(&self, shard: u32, lsn: Lsn) -> io::Result<()> {
        let mut st = self.lock();
        st.set_retain(shard, lsn)?;
        self.position.store(st.wal().position(), Ordering::Release);
        Ok(())
    }

    /// Stages a section of `shard`. On a fast disk with nothing in flight
    /// and `inline`, writes it on this thread; otherwise an I/O thread
    /// writes it with whatever else is staged. Its durable position goes
    /// to the shard's sink either way.
    pub fn append(&self, shard: u32, epoch: u64, batch: Batch, inline: bool) -> io::Result<()> {
        let mut st = self.lock();
        self.stage_locked(&mut st, shard, epoch, batch)?;
        self.write_locked(st, inline)
    }

    /// Stages a section without writing it; [`write_staged`](Self::write_staged)
    /// writes what is staged. For a thread that takes several sections at
    /// once, so they share a write.
    pub fn stage(&self, shard: u32, epoch: u64, batch: Batch) -> io::Result<()> {
        self.stage_locked(&mut self.lock(), shard, epoch, batch)
    }

    /// Writes what is staged: on this thread with `inline` on a fast disk
    /// with nothing in flight, otherwise on the I/O threads.
    pub fn write_staged(&self, inline: bool) -> io::Result<()> {
        self.write_locked(self.lock(), inline)
    }

    fn stage_locked(
        &self,
        st: &mut ServerWal<D>,
        shard: u32,
        epoch: u64,
        batch: Batch,
    ) -> io::Result<()> {
        st.append(shard, epoch, batch)?;
        if let Some(us) = st.turnaround(shard, fusee::local::monotonic_us()) {
            self.record_turnaround(us);
        }
        Ok(())
    }

    fn write_locked(
        &self,
        mut st: std::sync::MutexGuard<'_, ServerWal<D>>,
        inline: bool,
    ) -> io::Result<()> {
        if !st.wal().has_staged() {
            return Ok(());
        }
        let fast = self.last_flush_us() <= INLINE_MAX_US;
        if !(inline && fast && st.wal().in_flight() == 0) {
            if let Some(m) = self.start_writes(&mut st) {
                drop(st);
                self.fail(m.clone());
                return Err(other(m));
            }
            return Ok(());
        }
        let sealed = st.seal(fusee::local::unix_ms(), false);
        let mut w = match sealed {
            Ok(Some(w)) => w,
            Ok(None) => return Ok(()),
            Err(e) => {
                let m = format!("log write failed: {e}");
                drop(st);
                self.fail(m.clone());
                return Err(other(m));
            }
        };
        drop(st);
        let started = Instant::now();
        let written = Wal::perform(&self.dev, &mut w);
        self.last_flush_us
            .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.finish(w, written);
        Ok(())
    }

    /// Completes a write: publishes positions, or fails the log.
    fn finish(&self, w: InFlight, written: io::Result<()>) {
        let mut st = self.lock();
        match written {
            Ok(()) => {
                let mut marks = st.complete(w, fusee::local::monotonic_us());
                for m in &mut marks {
                    m.pins = st.wal().pins(m.shard);
                }
                let position = st.wal().position();
                let segment = st.wal().active_segment();
                self.position.store(position, Ordering::Release);
                self.segment.store(segment, Ordering::Release);
                // Whatever was staged meanwhile goes out now.
                let failed = self.start_writes(&mut st);
                drop(st);
                self.published.notify_all();
                self.publish(&marks, position, segment);
                if let Some(m) = failed {
                    self.fail(m);
                }
            }
            Err(e) => {
                drop(st);
                self.fail(format!("log write failed: {e}"));
            }
        }
    }

    /// Sends each sink the positions of its shards.
    fn publish(&self, marks: &[Mark], position: u64, segment: u64) {
        if marks.is_empty() {
            return;
        }
        let sinks = self.sinks.lock().unwrap_or_else(|e| e.into_inner());
        let mut done = vec![false; marks.len()];
        for i in 0..marks.len() {
            if done[i] {
                continue;
            }
            let Some(Some((epoch, sink))) = sinks.get(marks[i].shard as usize) else {
                continue;
            };
            let mut group = Vec::new();
            for j in i..marks.len() {
                if !done[j]
                    && let Some(Some((e, s))) = sinks.get(marks[j].shard as usize)
                    && *e == *epoch
                    && Arc::ptr_eq(s, sink)
                {
                    done[j] = true;
                    group.push(marks[j]);
                }
            }
            sink.durable(&group, position, segment);
        }
    }

    /// The log's state on disk is uncertain: every shard stops.
    fn fail(&self, message: String) {
        self.lock().fail(message.clone());
        self.published.notify_all();
        let sinks: Vec<(u32, Arc<dyn Sink>)> = self
            .sinks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|(_, s)| (i as u32, s.clone())))
            .collect();
        let mut done = vec![false; sinks.len()];
        for i in 0..sinks.len() {
            if done[i] {
                continue;
            }
            let shards: Vec<u32> = (i..sinks.len())
                .filter(|&j| Arc::ptr_eq(&sinks[j].1, &sinks[i].1))
                .inspect(|&j| done[j] = true)
                .map(|j| sinks[j].0)
                .collect();
            sinks[i].1.failed(&shards, &message);
        }
    }

    /// Seals what is staged while another write may start; the I/O threads
    /// write them. Called with the log locked; a failure is returned for
    /// the caller to report once the lock is released ([`fail`](Self::fail)).
    #[must_use]
    fn start_writes(&self, st: &mut ServerWal<D>) -> Option<String> {
        if st.failed().is_some() {
            return None;
        }
        while st.wal().has_staged() && st.wal().can_begin() {
            if self.gather(st) {
                break;
            }
            match st.seal(fusee::local::unix_ms(), true) {
                Ok(Some(w)) => {
                    self.jobs
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push_back((w, Instant::now()));
                    self.queued.fetch_add(1, Ordering::AcqRel);
                    self.job_ready.notify_one();
                }
                Ok(None) => break,
                Err(e) => return Some(format!("log write failed: {e}")),
            }
        }
        None
    }

    /// Whether the next batch should wait for shards a write just made
    /// durable (see [`GATHER_MAX_US`]). The first call arms the wait and
    /// wakes an I/O thread, which seals at its end if they do not all
    /// arrive first.
    fn gather(&self, st: &ServerWal<D>) -> bool {
        if !st.awaits_cohort() {
            self.gathering_since.store(0, Ordering::Release);
            return false;
        }
        let now = fusee::local::monotonic_us().max(1);
        let since = self.gathering_since.load(Ordering::Acquire);
        if since == 0 {
            self.gathering_since.store(now, Ordering::Release);
            self.job_ready.notify_one();
            return true;
        }
        if now < since + self.gather_us() {
            return true;
        }
        self.gathering_since.store(0, Ordering::Release);
        false
    }

    fn gather_us(&self) -> u64 {
        let cap = (self.last_flush_us() / 4).max(GATHER_MIN_US);
        (self.turnaround_us.load(Ordering::Relaxed) * 3).clamp(GATHER_MIN_US, cap)
    }

    /// Folds one shard's turnaround into the estimate (see
    /// [`TURNAROUND_MAX_US`]).
    fn record_turnaround(&self, us: u64) {
        if us > self.last_flush_us().max(TURNAROUND_MAX_US) {
            return;
        }
        let old = self.turnaround_us.load(Ordering::Relaxed);
        let new = if old == 0 { us } else { (old * 7 + us) / 8 };
        self.turnaround_us.store(new, Ordering::Relaxed);
    }

    /// When a wait for shards ends, if one runs.
    fn gather_deadline(&self) -> Option<u64> {
        match self.gathering_since.load(Ordering::Acquire) {
            0 => None,
            since => Some(since + self.gather_us()),
        }
    }

    fn spin_budget(&self) -> Duration {
        let flush = self.last_flush_us();
        Duration::from_micros(self.spin_cap_us.min(flush.saturating_mul(2)))
    }
}

impl<D: LogDevice + Clone + Send + Sync + 'static> Drop for LogService<D> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.job_ready.notify_all();
    }
}

/// Writes and syncs sealed batches outside the log lock, then publishes.
fn io_thread<D: LogDevice + Clone + Send + Sync + 'static>(log: Weak<LogService<D>>) {
    loop {
        let Some(log) = log.upgrade() else { return };
        let budget = log.spin_budget();
        if !budget.is_zero() {
            let until = Instant::now() + budget;
            while log.queued.load(Ordering::Acquire) == 0 && Instant::now() < until {
                std::hint::spin_loop();
            }
        }
        let job = {
            let mut jobs = log.jobs.lock().unwrap_or_else(|e| e.into_inner());
            if jobs.is_empty() && !log.stop.load(Ordering::Acquire) {
                // A batch waiting for shards is sealed here when the wait
                // ends, unless they all arrived first.
                let wait = log
                    .gather_deadline()
                    .map_or(Duration::from_millis(100), |at| {
                        Duration::from_micros(
                            at.saturating_sub(fusee::local::monotonic_us()).max(1),
                        )
                    });
                jobs = log
                    .job_ready
                    .wait_timeout(jobs, wait)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            if log.stop.load(Ordering::Acquire) {
                return;
            }
            let j = jobs.pop_front();
            if j.is_some() {
                log.queued.fetch_sub(1, Ordering::AcqRel);
            }
            j
        };
        let Some((mut w, started)) = job else {
            if log
                .gather_deadline()
                .is_some_and(|at| fusee::local::monotonic_us() >= at)
            {
                let mut st = log.lock();
                let failed = log.start_writes(&mut st);
                drop(st);
                if let Some(m) = failed {
                    log.fail(m);
                }
            }
            continue;
        };
        let written = Wal::perform(&log.dev, &mut w);
        log.last_flush_us
            .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        log.finish(w, written);
    }
}

#[cfg(test)]
mod tests;
