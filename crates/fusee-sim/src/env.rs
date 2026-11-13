//! [`SimEnv`]: the shard runtime's I/O on a simulated disk and clock.

use crate::disk::SimDisk;
use crate::rng::Rng;
use fusee::env::{Env, Notify};
use fusee_core::Lsn;
use fusee_core::segment::Batch;
use fusee_core::store::{Checkpoint, store_header};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;

/// Scheduled I/O completions, owned by the world.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Io {
    Flush {
        shard: u32,
        epoch: u64,
    },
    Checkpoint {
        shard: u32,
        epoch: u64,
    },
    /// One of the shared log's writes in flight may now sync.
    LogFlush {
        epoch: u64,
        seq: u64,
    },
    /// The shared log's blob writes through job `id` may now sync.
    BlobFlush {
        epoch: u64,
        id: u64,
    },
}

/// The shared log's segment files on the simulated disk (`wal/`), or its
/// blob area's files (`blob/`, which grow by appends; a crash resolves
/// their pending writes like any others', independently of the log's).
pub struct SimDevice {
    m: MachineRef,
    prefix: &'static str,
    /// The 4 KiB blocks each `write` call touched, as (segment, first,
    /// last), for [`SimLog`] to attribute to the write in flight.
    touched: Rc<RefCell<Vec<(u64, u64, u64)>>>,
}

impl SimDevice {
    fn blobs(m: &MachineRef) -> Self {
        Self {
            m: m.clone(),
            prefix: "blob/",
            touched: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn name(&self, number: u64) -> String {
        format!("{}{number:020}", self.prefix)
    }
}

impl LogDevice for SimDevice {
    fn segments(&self) -> io::Result<Vec<u64>> {
        let m = self.m.borrow();
        let mut v: Vec<u64> = m
            .disk
            .names_with_prefix(self.prefix)
            .iter()
            .filter_map(|n| n.strip_prefix(self.prefix)?.parse().ok())
            .collect();
        v.sort_unstable();
        Ok(v)
    }
    fn len(&self, segment: u64) -> io::Result<u64> {
        let m = self.m.borrow();
        let f = m
            .disk
            .get(&self.name(segment))
            .ok_or_else(|| io::Error::other("missing log segment"))?;
        Ok(f.view().len() as u64)
    }
    fn read(&self, segment: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let m = self.m.borrow();
        let img = m
            .disk
            .get(&self.name(segment))
            .ok_or_else(|| io::Error::other("missing log segment"))?
            .view();
        let start = (offset as usize).min(img.len());
        let end = start.saturating_add(len).min(img.len());
        Ok(img[start..end].to_vec())
    }
    fn create(&self, segment: u64, len: u64) -> io::Result<()> {
        let mut m = self.m.borrow_mut();
        m.disk.remove(&self.name(segment));
        let f = m.disk.file(&self.name(segment));
        f.write(0, &vec![0; len as usize]);
        f.flush();
        Ok(())
    }
    fn write(&self, segment: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let block = fusee_core::wal::BLOCK;
        let last = (offset + (bytes.len() as u64).max(1) - 1) / block;
        if self.prefix == "wal/" {
            self.touched
                .borrow_mut()
                .push((segment, offset / block, last));
        }
        self.m
            .borrow_mut()
            .disk
            .file(&self.name(segment))
            .write(offset, bytes);
        Ok(())
    }
    fn sync(&self, segment: u64) -> io::Result<()> {
        self.m.borrow_mut().disk.file(&self.name(segment)).flush();
        Ok(())
    }
    fn rename(&self, from: u64, to: u64) -> io::Result<()> {
        let mut m = self.m.borrow_mut();
        let f = m
            .disk
            .files
            .remove(&self.name(from))
            .ok_or_else(|| io::Error::other("missing log segment"))?;
        m.disk.files.insert(self.name(to), f);
        Ok(())
    }
    fn remove(&self, segment: u64) -> io::Result<()> {
        self.m.borrow_mut().disk.remove(&self.name(segment));
        Ok(())
    }
}

/// How the simulated shared log treats records over its offload threshold.
#[derive(Clone, Copy, Debug, Default)]
pub struct Offload {
    /// Records whose canonical encoding is longer than this go to the blob
    /// area; 0: none.
    pub threshold: u64,
    /// Sabotage: sections are released to batches as soon as their blobs
    /// are written, before the blobs are durable.
    pub early: bool,
}

/// The shared log of one simulated machine (log format 3): one write in
/// flight, completed by the world after a random latency.
pub struct SimLog {
    pub wal: Wal<SimDevice>,
    /// Blob files written to since their last sync.
    blob_files: Vec<u64>,
    offload: Offload,
    in_flight: Vec<InFlight>,
    failed: bool,
    epoch: u64,
    touched: Rc<RefCell<Vec<(u64, u64, u64)>>>,
    /// Blocks each write in flight touched: (seq, segment, first, last).
    blocks: Vec<(u64, u64, u64, u64)>,
    /// Two writes in flight together touched one block. Under `O_DIRECT`
    /// each rewrites its blocks whole, so one could erase the other's
    /// bytes; the simulated disk merges them, so this is checked instead.
    violation: Option<String>,
}

pub type SimLogRef = Rc<RefCell<SimLog>>;

impl SimLog {
    pub fn open(
        m: &MachineRef,
        shards: u32,
        segment_bytes: u64,
        in_flight: usize,
        spares: usize,
        offload: Offload,
    ) -> io::Result<SimLogRef> {
        let (now, life, epoch) = {
            let mut mm = m.borrow_mut();
            (mm.now, mm.rng.next() | 1, mm.epoch)
        };
        let opts = WalOptions {
            segment_bytes,
            first_segment_bytes: (segment_bytes / 4).max(16 << 10),
            in_flight,
            spares,
            ..WalOptions::default()
        };
        let touched = Rc::new(RefCell::new(Vec::new()));
        let dev = SimDevice {
            m: m.clone(),
            prefix: "wal/",
            touched: touched.clone(),
        };
        let wal = Wal::open_with_blobs(
            dev,
            Blobs {
                dev: SimDevice::blobs(m),
                threshold: offload.threshold,
            },
            shards,
            opts,
            life,
            now,
        )?;
        touched.borrow_mut().clear();
        Ok(Rc::new(RefCell::new(Self {
            wal,
            blob_files: Vec::new(),
            offload,
            in_flight: Vec::new(),
            failed: false,
            epoch,
            touched,
            blocks: Vec::new(),
            violation: None,
        })))
    }

    /// Two writes in flight shared a block, if they ever did.
    pub fn violation(&self) -> Option<&str> {
        self.violation.as_deref()
    }

    /// Issues the blob writes staged sections need, and schedules their
    /// sync after a random latency (sections that reference them are staged
    /// once it completes, or at once under the `early` sabotage).
    fn start_blobs(&mut self, m: &MachineRef) {
        if self.failed {
            return;
        }
        let jobs = self.wal.take_blob_jobs();
        let Some(last) = jobs.last().map(|j| j.id) else {
            return;
        };
        let Some(dev) = self.wal.blob_device() else {
            return;
        };
        for j in &jobs {
            if j.perform(dev).is_err() {
                self.failed = true;
                return;
            }
            if !self.blob_files.contains(&j.file) {
                self.blob_files.push(j.file);
            }
        }
        if self.offload.early && self.wal.blobs_written(last).is_err() {
            self.failed = true;
        }
        let mut mm = m.borrow_mut();
        let after = mm.latency();
        let epoch = self.epoch;
        mm.schedule(after, Io::BlobFlush { epoch, id: last });
    }

    /// The blob writes through job `id` may sync now, or fail.
    pub fn complete_blobs(log: &SimLogRef, m: &MachineRef, id: u64) {
        let mut l = log.borrow_mut();
        let fail = {
            let mut mm = m.borrow_mut();
            let p = mm.flush_fail_per_mille;
            let f = mm.rng.chance(p);
            if f {
                mm.log.push("blob area flush failed".to_string());
            }
            f
        };
        if fail || l.failed {
            l.failed = true;
            return;
        }
        let files = std::mem::take(&mut l.blob_files);
        let synced = match l.wal.blob_device() {
            Some(dev) => files.iter().try_for_each(|f| dev.sync(*f)),
            None => Ok(()),
        };
        if synced.and_then(|()| l.wal.blobs_written(id)).is_err() {
            l.failed = true;
            return;
        }
        l.start(m);
    }

    /// Starts writes of everything staged while the log allows another in
    /// flight (two at once: their completions arrive in any order).
    fn start(&mut self, m: &MachineRef) {
        self.start_blobs(m);
        while !self.failed && self.wal.has_staged() && self.wal.can_begin() {
            // Like the engine: with nothing in flight a write is sometimes
            // packed (the inline path), which leaves the next batch starting
            // inside a block; otherwise it is padded so another may follow.
            let (now, overlap) = {
                let mut mm = m.borrow_mut();
                let packed = self.in_flight.is_empty() && mm.rng.chance(500);
                (mm.now, !packed)
            };
            match self.wal.begin_write(now, overlap) {
                Ok(Some(w)) => {
                    let seq = w.seq();
                    let touched: Vec<_> = self.touched.borrow_mut().drain(..).collect();
                    for (segment, first, last) in touched {
                        if let Some(other) = self
                            .blocks
                            .iter()
                            .find(|b| b.0 != seq && b.1 == segment && b.2 <= last && b.3 >= first)
                            && self.violation.is_none()
                        {
                            self.violation = Some(format!(
                                "log writes {} and {seq} were in flight together and both \
                                 touched segment {segment} block {}",
                                other.0,
                                first.max(other.2)
                            ));
                        }
                        self.blocks.push((seq, segment, first, last));
                    }
                    self.in_flight.push(w);
                    let mut mm = m.borrow_mut();
                    let after = mm.latency();
                    let epoch = self.epoch;
                    mm.schedule(after, Io::LogFlush { epoch, seq });
                }
                Ok(None) => break,
                Err(_) => self.failed = true,
            }
        }
    }

    /// Writes in flight.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// The write `seq` may sync now, or fail.
    pub fn complete(log: &SimLogRef, m: &MachineRef, seq: u64) {
        let mut l = log.borrow_mut();
        let Some(i) = l.in_flight.iter().position(|w| w.seq() == seq) else {
            return;
        };
        let w = l.in_flight.remove(i);
        l.blocks.retain(|b| b.0 != seq);
        let fail = {
            let mut mm = m.borrow_mut();
            let p = mm.flush_fail_per_mille;
            let f = mm.rng.chance(p);
            if f {
                mm.log.push("shared log flush failed".to_string());
            }
            f
        };
        if fail || l.failed || l.wal.finish_write(w).is_err() {
            // The outcome of the writes is unknown: every shard stops.
            l.failed = true;
            return;
        }
        l.start(m);
    }
}

use fusee::wal::{Blobs, InFlight, LogDevice, Wal, WalOptions};

/// State shared by all shards of one simulated machine.
pub struct Machine {
    pub disk: SimDisk,
    pub now: u64,
    pub rng: Rng,
    /// (due time, insertion order) -> completion
    pub io: BTreeMap<(u64, u64), Io>,
    pub io_seq: u64,
    pub epoch: u64,
    pub flush_fail_per_mille: u32,
    pub max_io_ms: u64,
    pub log: Vec<String>,
}

pub type MachineRef = Rc<RefCell<Machine>>;

impl Machine {
    pub fn schedule(&mut self, after: u64, io: Io) {
        let t = self.now + after;
        let s = self.io_seq;
        self.io_seq += 1;
        self.io.insert((t, s), io);
    }

    pub fn latency(&mut self) -> u64 {
        let max = self.max_io_ms.max(1);
        self.rng.below(max + 1)
    }
}

pub struct SimEnv {
    /// Log format 3: the machine's shared log, and the last LSN submitted.
    log: Option<SimLogRef>,
    log_in_flight: Option<Lsn>,
    /// Sections of this shard the runtime may have in flight at once, and
    /// the durable position last reported to it. Half the shards of an
    /// epoch keep two in flight, the others one.
    log_max_in_flight: usize,
    log_reported: Lsn,
    /// Checkpoints submitted, and what the two newest durable ones cover.
    log_checkpoints: Vec<Lsn>,
    log_retained: Vec<Lsn>,
    shard: u32,
    epoch: u64,
    m: MachineRef,
    rng: Rng,
    flushing: Option<Batch>,
    flushed: Option<io::Result<Lsn>>,
    ckpt: Option<(Checkpoint, Option<u64>)>,
    ckpt_done: Option<io::Result<Lsn>>,
    store_gen: u64,
    store_len: u64,
    /// The prefix of the loaded checkpoint, when recovery reads its
    /// persisted index instead of scanning the store (most opens; the
    /// others scan, so both ways are exercised).
    indexed_prefix: Option<(u64, u64)>,
    failed: bool,
}

fn seg_name(shard: u32, seg: u64) -> String {
    format!("s{shard:03}/log/{seg:020}")
}

fn store_name(shard: u32, generation: u64) -> String {
    format!("s{shard:03}/runs-{generation:010}")
}

fn ckpt_name(shard: u32, lsn: Lsn) -> String {
    format!("s{shard:03}/cp-{lsn:020}")
}

impl SimEnv {
    pub fn new(shard: u32, m: MachineRef) -> Self {
        let (epoch, rng) = {
            let mut mm = m.borrow_mut();
            (mm.epoch, mm.rng.fork())
        };
        Self {
            log: None,
            log_in_flight: None,
            log_max_in_flight: 1,
            log_reported: 0,
            log_checkpoints: Vec::new(),
            log_retained: Vec::new(),
            shard,
            epoch,
            m,
            rng,
            flushing: None,
            flushed: None,
            ckpt: None,
            ckpt_done: None,
            store_gen: 0,
            store_len: 0,
            indexed_prefix: None,
            failed: false,
        }
    }

    /// A shard whose records go to the machine's shared log.
    pub fn with_log(shard: u32, m: MachineRef, log: SimLogRef) -> Self {
        let mut env = Self::new(shard, m);
        // Not from the random stream, so that adding this left every
        // seed's other choices as they were.
        env.log_max_in_flight = 1 + ((env.epoch + u64::from(shard)) % 2) as usize;
        env.log_reported = log.borrow().wal.durable(shard);
        env.log = Some(log);
        env
    }

    fn report_retained(&self) -> io::Result<()> {
        if let (Some(log), Some(min)) = (&self.log, self.log_retained.iter().copied().min()) {
            log.borrow_mut().wal.set_retain(self.shard, min)?;
        }
        Ok(())
    }

    /// The scheduled flush completed (or failed).
    pub fn complete_flush(&mut self) {
        let Some(batch) = self.flushing.take() else {
            return;
        };
        let mut m = self.m.borrow_mut();
        let fail = {
            let p = m.flush_fail_per_mille;
            m.rng.chance(p)
        };
        if fail || self.failed {
            // After a failed fsync the kernel may already have dropped the
            // dirty pages: the outcome of these writes is unknown. The
            // runtime must stop using this log.
            self.failed = true;
            m.log.push(format!(
                "shard {} flush failed (batch ..={})",
                self.shard, batch.last_lsn
            ));
            self.flushed = Some(Err(io::Error::other("simulated fsync failure")));
            return;
        }
        let mut names: Vec<String> = Vec::new();
        for c in &batch.chunks {
            let n = seg_name(self.shard, c.segment);
            if !names.contains(&n) {
                names.push(n);
            }
        }
        for n in names {
            m.disk.file(&n).flush();
        }
        self.flushed = Some(Ok(batch.last_lsn));
    }

    pub fn complete_checkpoint(&mut self) {
        let Some((cp, delete)) = self.ckpt.take() else {
            return;
        };
        let mut m = self.m.borrow_mut();
        let shard = self.shard;
        m.disk.file(&store_name(shard, cp.store_generation)).flush();
        let bytes = cp.encode();
        let name = ckpt_name(shard, cp.lsn);
        let f = m.disk.file(&name);
        f.truncate(0);
        f.write(0, &bytes);
        f.flush();
        let mut cps = m.disk.names_with_prefix(&format!("s{shard:03}/cp-"));
        cps.sort();
        if cps.len() > 2 {
            for old in &cps[..cps.len() - 2] {
                m.disk.remove(old);
            }
        }
        let saved: Vec<_> = cps
            .iter()
            .rev()
            .take(2)
            .filter_map(|name| m.disk.get(name))
            .filter_map(|f| Checkpoint::decode(&f.view()).ok())
            .collect();
        for name in m.disk.names_with_prefix(&format!("s{shard:03}/runs-")) {
            if !saved
                .iter()
                .any(|c| store_name(shard, c.store_generation) == name)
            {
                m.disk.remove(&name);
            }
        }
        if self.log.is_some() {
            if let Some(i) = self.log_checkpoints.iter().position(|l| *l == cp.lsn) {
                self.log_checkpoints.remove(i);
                self.log_retained.push(cp.lsn);
                if self.log_retained.len() > 2 {
                    self.log_retained.remove(0);
                }
            }
            drop(m);
            self.ckpt_done = Some(self.report_retained().map(|()| cp.lsn));
            return;
        }
        if let (Some(through), Some(oldest)) = (delete, saved.last()) {
            let mut segments = m.disk.names_with_prefix(&format!("s{shard:03}/log/"));
            segments.sort();
            let boundary = segments.iter().rev().find_map(|path| {
                let number = path.rsplit('/').next()?.parse::<u64>().ok()?;
                if number > through.saturating_add(1) {
                    return None;
                }
                let bytes = m.disk.get(path)?.read(0, 44)?;
                let header = fusee_core::segment::SegmentHeader::decode(&bytes).ok()??;
                (header.segment == number && header.base_lsn <= oldest.lsn.saturating_add(1))
                    .then_some(path.clone())
            });
            if let Some(boundary) = boundary {
                for name in segments.into_iter().take_while(|n| *n < boundary) {
                    m.disk.remove(&name);
                }
            }
        }
        self.ckpt_done = Some(Ok(cp.lsn));
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Env for SimEnv {
    fn now_ms(&self) -> u64 {
        self.m.borrow().now
    }

    fn random(&mut self) -> u64 {
        self.rng.next()
    }

    fn set_notify(&mut self, _notify: Notify) {}

    fn max_in_flight(&self) -> usize {
        if self.log.is_some() {
            self.log_max_in_flight
        } else {
            1
        }
    }

    fn submit(&mut self, batch: Batch) {
        if let Some(log) = &self.log {
            self.log_in_flight = Some(batch.last_lsn);
            let mut l = log.borrow_mut();
            if l.wal.stage(self.shard, batch).is_err() {
                l.failed = true;
            }
            l.start(&self.m);
            return;
        }
        let mut m = self.m.borrow_mut();
        for c in &batch.chunks {
            m.disk
                .file(&seg_name(self.shard, c.segment))
                .write(c.offset, &c.bytes);
        }
        let after = m.latency();
        let (shard, epoch) = (self.shard, self.epoch);
        m.schedule(after, Io::Flush { shard, epoch });
        self.flushing = Some(batch);
    }

    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>> {
        if let Some(log) = &self.log {
            let want = self.log_in_flight?;
            let l = log.borrow();
            if l.failed {
                self.log_in_flight = None;
                return Some(Err(io::Error::other("simulated shared log failure")));
            }
            let durable = l.wal.durable(self.shard);
            if durable >= want {
                self.log_in_flight = None;
                self.log_reported = durable;
                return Some(Ok(durable));
            }
            if durable > self.log_reported {
                self.log_reported = durable;
                return Some(Ok(durable));
            }
            return None;
        }
        self.flushed.take()
    }

    fn store_append(&mut self, frame: &[u8]) -> io::Result<u64> {
        let off = self.store_len;
        let name = store_name(self.shard, self.store_gen);
        self.m.borrow_mut().disk.file(&name).write(off, frame);
        self.store_len += frame.len() as u64;
        Ok(off)
    }

    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        let name = store_name(self.shard, self.store_gen);
        self.m
            .borrow()
            .disk
            .get(&name)
            .and_then(|f| f.read(offset, len as usize))
            .ok_or_else(|| io::Error::other("run store read past end"))
    }

    fn store_len(&self) -> u64 {
        self.store_len
    }

    fn store_generation(&self) -> u64 {
        self.store_gen
    }

    fn compact_store(
        &mut self,
        live: &[(fusee_core::RunId, fusee_core::shard::BlobRef)],
    ) -> io::Result<bool> {
        let mut generation = self.store_gen;
        let name = loop {
            generation = generation
                .checked_add(1)
                .ok_or_else(|| io::Error::other("store generation overflow"))?;
            let name = store_name(self.shard, generation);
            if self.m.borrow().disk.get(&name).is_none() {
                break name;
            }
        };
        let header = store_header();
        self.m.borrow_mut().disk.file(&name).write(0, &header);
        let mut length = header.len() as u64;
        for &(run, blob) in live {
            let frame = self.store_read(blob.offset, blob.len)?;
            fusee::env::check_live_frame(&frame, run)?;
            self.m.borrow_mut().disk.file(&name).write(length, &frame);
            length += u64::from(blob.len);
        }
        self.store_gen = generation;
        self.store_len = length;
        Ok(true)
    }

    fn checkpoint(&mut self, checkpoint: Checkpoint, delete_through_segment: Option<u64>) {
        if self.log.is_some() {
            self.log_checkpoints.push(checkpoint.lsn);
        }
        let mut m = self.m.borrow_mut();
        let after = m.latency();
        let (shard, epoch) = (self.shard, self.epoch);
        m.schedule(after, Io::Checkpoint { shard, epoch });
        self.ckpt = Some((checkpoint, delete_through_segment));
    }

    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>> {
        self.ckpt_done.take()
    }

    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        let cp = self.load_newest_checkpoint()?;
        self.indexed_prefix = cp
            .as_ref()
            .filter(|c| {
                c.index.is_some() && !(self.epoch + u64::from(self.shard)).is_multiple_of(4)
            })
            .map(|c| (c.store_generation, c.store_len));
        if self.log.is_some() {
            let m = self.m.borrow();
            let mut cps = m.disk.names_with_prefix(&format!("s{:03}/cp-", self.shard));
            cps.sort();
            let mut positions: Vec<u64> = cps
                .iter()
                .rev()
                .filter_map(|n| m.disk.get(n))
                .filter_map(|f| Checkpoint::decode(&f.view()).ok())
                .map(|c| c.lsn)
                .take(2)
                .collect();
            positions.reverse();
            drop(m);
            self.log_retained = if cp.is_some() { positions } else { vec![0] };
            self.report_retained()?;
        }
        Ok(cp)
    }

    fn shared_log(&self) -> bool {
        self.log.is_some()
    }

    fn recover_log(
        &mut self,
        after: Lsn,
    ) -> io::Result<(Vec<(Lsn, fusee_core::record::RecordBuf)>, Lsn)> {
        match &self.log {
            Some(log) => log.borrow_mut().wal.take_recovered(self.shard, after),
            None => Err(io::Error::other("this shard has per-shard segments")),
        }
    }

    fn log_position(&self) -> u64 {
        self.log.as_ref().map_or(0, |l| l.borrow().wal.position())
    }

    fn log_segment(&self) -> u64 {
        self.log
            .as_ref()
            .map_or(0, |l| l.borrow().wal.active_segment())
    }

    fn checkpoint_wanted(&self) -> bool {
        self.log
            .as_ref()
            .is_some_and(|l| l.borrow().wal.pins(self.shard))
    }
    fn checkpoint_wanted_on_close(&self) -> bool {
        self.log
            .as_ref()
            .is_some_and(|l| l.borrow().wal.holds_old(self.shard))
    }
    fn recover_store(
        &mut self,
        generation: u64,
        len: u64,
        visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry),
    ) -> io::Result<()> {
        let indexed = generation != 0 && self.indexed_prefix.take() == Some((generation, len));
        let bytes = self.load_store(generation, len)?;
        if indexed {
            return Ok(());
        }
        fusee_core::store::scan_store(&bytes, visit).map_err(io::Error::other)
    }

    fn load_store(&mut self, generation: u64, len: u64) -> io::Result<Vec<u8>> {
        let mut m = self.m.borrow_mut();
        if generation == 0 {
            let name = store_name(self.shard, 1);
            m.disk.remove(&name);
            let f = m.disk.file(&name);
            let h = store_header();
            f.write(0, &h);
            f.flush();
            self.store_gen = 1;
            self.store_len = h.len() as u64;
            return Ok(h);
        }
        let name = store_name(self.shard, generation);
        let f = m.disk.file(&name);
        f.truncate(len);
        let bytes = f.view();
        if (bytes.len() as u64) < len {
            return Err(io::Error::other(
                "run store shorter than its checkpoint says",
            ));
        }
        self.store_gen = generation;
        self.store_len = len;
        Ok(bytes)
    }

    fn segments(&mut self) -> io::Result<Vec<u64>> {
        let m = self.m.borrow();
        let mut v: Vec<u64> = m
            .disk
            .names_with_prefix(&format!("s{:03}/log/", self.shard))
            .iter()
            .filter_map(|n| n.rsplit('/').next()?.parse().ok())
            .collect();
        v.sort_unstable();
        Ok(v)
    }

    fn read_segment(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        let m = self.m.borrow();
        m.disk
            .get(&seg_name(self.shard, segment))
            .map(|f| f.view())
            .ok_or_else(|| io::Error::other("missing segment"))
    }

    fn sync_recovered(&mut self) -> io::Result<()> {
        let mut m = self.m.borrow_mut();
        for n in m
            .disk
            .names_with_prefix(&format!("s{:03}/log/", self.shard))
        {
            m.disk.file(&n).flush();
        }
        Ok(())
    }

    fn discard_segment(&mut self, segment: u64) -> io::Result<()> {
        self.m
            .borrow_mut()
            .disk
            .remove(&seg_name(self.shard, segment));
        Ok(())
    }

    fn exclusive_log(&self) -> bool {
        true
    }

    fn open_log(&mut self, segment: u64) -> io::Result<()> {
        let mut m = self.m.borrow_mut();
        let f = m.disk.file(&seg_name(self.shard, segment));
        f.flush();
        Ok(())
    }
}

impl SimEnv {
    fn load_newest_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        let m = self.m.borrow();
        let mut cps = m.disk.names_with_prefix(&format!("s{:03}/cp-", self.shard));
        cps.sort();
        for name in cps.iter().rev() {
            if let Some(f) = m.disk.get(name)
                && let Ok(cp) = Checkpoint::decode(&f.view())
            {
                return Ok(Some(cp));
            }
        }
        Ok(None)
    }
}
