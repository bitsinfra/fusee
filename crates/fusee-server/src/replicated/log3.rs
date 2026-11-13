//! Replicating the log, not the shard: one protocol entry is one
//! log-format-3 batch carrying a section from every shard of the primary's
//! engine that had records (`docs/replication.md`).
//!
//! Shards hand their sections to the [`Batcher`] and wake the I/O thread,
//! which proposes everything staged as one entry. When the entry commits it
//! publishes each shard's committed LSN in the shard's [`ShardSlot`] and
//! wakes the shard. The engine recovers from the committed entries
//! themselves: they are the log, so no second copy is materialized.
use fusee_core::record::{Lsn, RecordBuf};
use fusee_core::segment::Batch;
use fusee_core::vsr::{Entry, Op};
use fusee_core::wal::{SectionRef, section_records};
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Body prefix of a log batch. `0xF3 0x00` never starts a segment-group (format 2)
/// batch body, whose first bytes are a canonical varint (a continuation
/// byte is never followed by zero), nor a permit command (`0`).
const MAGIC: [u8; 4] = [0xF3, 0x00, 1, 0];
const HEADER: usize = 8;

pub(super) fn is_log_batch(body: &[u8]) -> bool {
    body.starts_with(&MAGIC[..2])
}

/// Encodes an entry body: the magic, the group's shard count, the sections.
pub(super) fn encode(shards: u32, sections: &[SectionRef<'_>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        HEADER
            + 4
            + sections
                .iter()
                .map(|s| fusee_core::wal::SECTION_HEADER + s.records.len())
                .sum::<usize>(),
    );
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&shards.to_le_bytes());
    fusee::wal::encode_sections(&mut out, sections);
    out
}

/// The group's shard count and the sections of a log batch body.
pub(super) fn decode(body: &[u8]) -> io::Result<(u32, Vec<SectionRef<'_>>)> {
    if body.len() < HEADER || body[..4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported replicated log batch",
        ));
    }
    let shards = u32::from_le_bytes(body[4..8].try_into().unwrap());
    let sections = fusee::wal::decode_sections(&body[HEADER..])?;
    if shards == 0 || sections.iter().any(|s| s.shard >= shards) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "replicated log batch names a shard outside its group",
        ));
    }
    Ok((shards, sections))
}

/// One engine shard's view of the group: how far its records are committed.
#[derive(Default)]
pub(super) struct ShardSlot {
    pub committed: AtomicU64,
    pub failed: AtomicBool,
    pub notify: Mutex<Option<fusee::env::Notify>>,
    /// When the I/O thread last published a commit to this shard
    /// ([`super::timings::now_ns`]; 0: none, or timings are off).
    pub released_ns: AtomicU64,
}

impl ShardSlot {
    pub fn wake(&self) {
        if let Some(n) = self
            .notify
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            n();
        }
    }

    pub fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        self.wake();
    }
}

pub(super) struct Staged {
    pub epoch: u64,
    pub shard: u32,
    pub batch: Batch,
    pub slot: Arc<ShardSlot>,
    /// When the shard staged it ([`super::timings::now_ns`]; 0: timings off).
    pub staged_ns: u64,
}

/// Sections waiting for the I/O thread.
#[derive(Default)]
pub(super) struct Batcher {
    staged: Mutex<Vec<Staged>>,
    any: AtomicBool,
}

impl Batcher {
    pub fn stage(&self, s: Staged) {
        self.staged
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(s);
        self.any.store(true, Ordering::Release);
    }

    pub fn has_staged(&self) -> bool {
        self.any.load(Ordering::Acquire)
    }

    /// The sections of the next entry: the oldest staged, up to
    /// [`ENTRY_BYTES`] of records (one section at least). The rest stay
    /// staged, in order, for the entries after it, so an entry never grows
    /// with a backlog past what a peer decodes
    /// ([`fusee_core::wire::MAX_PAYLOAD`]) and a frame carries.
    pub fn take(&self) -> Vec<Staged> {
        if !self.has_staged() {
            return Vec::new();
        }
        let mut staged = self.staged.lock().unwrap_or_else(|e| e.into_inner());
        let mut bytes = 0usize;
        let fit = staged
            .iter()
            .take_while(|s| {
                let fits = bytes == 0 || bytes + s.batch.section.len() <= ENTRY_BYTES;
                bytes += s.batch.section.len();
                fits
            })
            .count();
        if fit == staged.len() {
            self.any.store(false, Ordering::Release);
            return std::mem::take(&mut *staged);
        }
        staged.drain(..fit).collect()
    }
}

/// The most bytes of records one entry carries, unless its first section
/// alone is longer (a section is at most [`fusee_core::wal::MAX_SECTION`],
/// or one record). A view change sends every uncommitted entry in one
/// message, and the primary keeps at most four uncommitted: four entries of
/// this size fit a frame ([`fusee_core::frame::MAX_FRAME`]).
pub(super) const ENTRY_BYTES: usize = 16 << 20;

/// Committed log batches after the checkpoint base, as the engine of a new
/// primary epoch recovers from them.
pub(super) struct Recovery {
    pub epoch: u64,
    pub entries: Vec<Entry>,
}

impl Recovery {
    /// Shard `shard`'s records after `after`, in order, and its last LSN in
    /// the log (`after` if none is later).
    pub fn records(&self, shard: u32, after: Lsn) -> io::Result<(Vec<(Lsn, RecordBuf)>, Lsn)> {
        let mut out = Vec::new();
        let mut last = after;
        for e in &self.entries {
            let (_, sections) = decode(&e.body)?;
            for s in sections.iter().filter(|s| s.shard == shard) {
                let end = s.first_lsn + u64::from(s.count) - 1;
                if end <= last {
                    continue;
                }
                if s.first_lsn > last + 1 {
                    return Err(io::Error::other(format!(
                        "shard {shard} records {}..{} are missing from the replicated log (entry {})",
                        last + 1,
                        s.first_lsn - 1,
                        e.op
                    )));
                }
                for (lsn, rec) in section_records(s).map_err(io::Error::other)? {
                    if lsn > last {
                        out.push((lsn, rec));
                    }
                }
                last = end;
            }
        }
        Ok((out, last))
    }
}

/// For each shard, the first op in `entries` holding one of its records
/// after `covered[shard]` (what its checkpoint covers).
pub(super) fn first_uncovered<'a>(
    entries: impl Iterator<Item = &'a Entry>,
    covered: &[Lsn],
) -> io::Result<Vec<Option<Op>>> {
    let mut first = vec![None; covered.len()];
    for e in entries.filter(|e| is_log_batch(&e.body)) {
        for s in decode(&e.body)?.1 {
            let i = s.shard as usize;
            let end = s.first_lsn + u64::from(s.count) - 1;
            if i < first.len() && first[i].is_none() && end > covered[i] {
                first[i] = Some(e.op);
            }
        }
        if first.iter().all(Option::is_some) {
            break;
        }
    }
    Ok(first)
}

/// [`first_uncovered`] kept up to date as entries commit, so the I/O
/// thread's checkpoint check reads a few numbers per shard instead of
/// decoding every entry since the image on every tick.
///
/// It indexes only committed entries (at or below the limit it is given),
/// which never change, so it survives view changes; entries at or below
/// the checkpoint base are dropped as the base moves. Replace it whenever
/// the protocol log is replaced by an installed image.
#[derive(Default)]
pub(super) struct Uncovered {
    /// The highest op indexed.
    through: Op,
    /// Per shard, every indexed entry holding one of its sections: the op
    /// and the highest last LSN of its sections up to that entry (both
    /// ascending). After compaction the maximum may include a dropped
    /// entry's; any checkpoint taken since covers that one.
    shards: Vec<VecDeque<(Op, Lsn)>>,
    /// Ops of indexed log batches.
    batches: VecDeque<Op>,
    /// Ops of indexed control entries (permit commands).
    control: VecDeque<Op>,
}

impl Uncovered {
    pub fn new(base: Op) -> Self {
        Self {
            through: base,
            ..Self::default()
        }
    }

    /// The highest op indexed.
    pub fn through(&self) -> Op {
        self.through
    }

    /// Drops what is at or below `base`, the checkpoint base.
    pub fn compact(&mut self, base: Op) {
        for q in &mut self.shards {
            while q.front().is_some_and(|&(op, _)| op <= base) {
                q.pop_front();
            }
        }
        while self.batches.front().is_some_and(|&op| op <= base) {
            self.batches.pop_front();
        }
        while self.control.front().is_some_and(|&op| op <= base) {
            self.control.pop_front();
        }
        self.through = self.through.max(base);
    }

    /// Indexes `entries`, which must follow [`Self::through`] in op order
    /// and be committed.
    pub fn extend<'a>(
        &mut self,
        entries: impl Iterator<Item = &'a Entry>,
        is_control: impl Fn(&[u8]) -> bool,
    ) -> io::Result<()> {
        for e in entries {
            if e.op <= self.through {
                return Err(io::Error::other(format!(
                    "checkpoint index given entry {} after {}",
                    e.op, self.through
                )));
            }
            if is_log_batch(&e.body) {
                self.batches.push_back(e.op);
                for s in decode(&e.body)?.1 {
                    let i = s.shard as usize;
                    if self.shards.len() <= i {
                        self.shards.resize_with(i + 1, VecDeque::new);
                    }
                    // The running maximum: a repeated (older) section never
                    // lowers it, and the first entry whose maximum passes a
                    // checkpoint is the first whose own section does.
                    let q = &mut self.shards[i];
                    let end = (s.first_lsn + u64::from(s.count) - 1)
                        .max(q.back().map_or(0, |&(_, end)| end));
                    match q.back_mut() {
                        Some(last) if last.0 == e.op => last.1 = end,
                        _ => q.push_back((e.op, end)),
                    }
                }
            } else if is_control(&e.body) {
                self.control.push_back(e.op);
            }
            self.through = e.op;
        }
        Ok(())
    }

    /// Whether a log batch is indexed at or below `limit`.
    pub fn any_batch(&self, limit: Op) -> bool {
        self.batches.front().is_some_and(|&op| op <= limit)
    }

    /// The control entries indexed at or below `through`, in order.
    pub fn control(&self, through: Op) -> impl Iterator<Item = Op> + '_ {
        self.control
            .iter()
            .copied()
            .take_while(move |&op| op <= through)
    }

    /// [`first_uncovered`] over the indexed entries at or below `limit`.
    pub fn first_uncovered(&self, covered: &[Lsn], limit: Op) -> Vec<Option<Op>> {
        covered
            .iter()
            .enumerate()
            .map(|(i, &lsn)| {
                let q = self.shards.get(i)?;
                // Sections of a shard end at ascending LSNs.
                let at = q.partition_point(|&(_, end)| end <= lsn);
                q.get(at).map(|&(op, _)| op).filter(|&op| op <= limit)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fusee_core::record::{Record, Status};
    use fusee_core::wal::SectionWriter;

    fn section(shard: u32, from: Lsn, n: u64) -> (u32, Batch) {
        let mut w = SectionWriter::new(from);
        for i in 0..n {
            let rec: fusee_core::RecordRef<'_> = Record::Done {
                run: from + i,
                status: Status::Ok,
                finished_ms: 1,
                payload: b"x",
                by: None,
            };
            w.append(&rec).unwrap();
        }
        (shard, w.take().unwrap())
    }

    /// An entry takes the oldest staged sections up to its size, one at
    /// least, and leaves the rest staged in order.
    #[test]
    fn the_batcher_bounds_what_an_entry_takes() {
        let batcher = Batcher::default();
        let stage = |shard: u32, first: Lsn, bytes: usize| {
            batcher.stage(Staged {
                epoch: 1,
                shard,
                batch: Batch {
                    chunks: Vec::new(),
                    first_lsn: first,
                    last_lsn: first,
                    records: 1,
                    bytes: bytes as u64,
                    section: vec![0; bytes],
                },
                slot: Arc::new(ShardSlot::default()),
                staged_ns: 0,
            });
        };
        assert!(batcher.take().is_empty());
        // Everything staged fits: one entry.
        stage(0, 1, 100);
        stage(1, 1, 200);
        assert_eq!(batcher.take().len(), 2);
        assert!(!batcher.has_staged());
        // A backlog: entries of at most `ENTRY_BYTES`, oldest first; a
        // section longer than that goes alone.
        stage(0, 2, ENTRY_BYTES / 2);
        stage(1, 2, ENTRY_BYTES / 2);
        stage(2, 1, 1);
        stage(0, 3, ENTRY_BYTES + 1);
        stage(1, 3, 7);
        let taken: Vec<Vec<(u32, Lsn)>> = std::iter::from_fn(|| {
            let t = batcher.take();
            (!t.is_empty()).then(|| t.iter().map(|s| (s.shard, s.batch.first_lsn)).collect())
        })
        .collect();
        assert_eq!(
            taken,
            vec![
                vec![(0, 2), (1, 2)],
                vec![(2, 1)],
                vec![(0, 3)],
                vec![(1, 3)],
            ]
        );
        assert!(!batcher.has_staged());
    }

    fn entry(op: Op, parts: &[(u32, Batch)]) -> Entry {
        let refs: Vec<SectionRef<'_>> = parts
            .iter()
            .map(|(s, b)| SectionRef {
                shard: *s,
                first_lsn: b.first_lsn,
                count: b.records,
                records: &b.section,
            })
            .collect();
        Entry::new(op, 0, 0, Arc::from(encode(4, &refs)))
    }

    #[test]
    fn each_shard_recovers_its_own_records_after_its_checkpoint() {
        let entries = vec![
            entry(1, &[section(0, 1, 2), section(2, 1, 1)]),
            entry(2, &[section(2, 2, 3)]),
            entry(3, &[section(0, 3, 1), section(2, 5, 1)]),
        ];
        assert!(entries.iter().all(|e| is_log_batch(&e.body)));
        let r = Recovery { epoch: 1, entries };
        let (recs, last) = r.records(2, 3).unwrap();
        assert_eq!(recs.iter().map(|(l, _)| *l).collect::<Vec<_>>(), [4, 5]);
        assert_eq!(last, 5);
        let (recs, last) = r.records(1, 0).unwrap();
        assert!(recs.is_empty());
        assert_eq!(last, 0);
        assert_eq!(r.records(0, 0).unwrap().1, 3);
        assert_eq!(decode(&r.entries[0].body).unwrap().0, 4);
        let first = first_uncovered(r.entries.iter(), &[2, 0, 1, 6]).unwrap();
        assert_eq!(first, vec![Some(3), None, Some(2), None]);
        // A gap in a shard's records is refused, never skipped.
        let gap = Recovery {
            epoch: 1,
            entries: vec![entry(1, &[section(1, 1, 1)]), entry(2, &[section(1, 3, 1)])],
        };
        assert!(gap.records(1, 0).is_err());
    }

    /// A small deterministic generator, so a failure names its seed.
    fn rng(seed: u64) -> impl FnMut(u64) -> u64 {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        move |n| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n.max(1)
        }
    }

    #[test]
    fn the_incremental_index_matches_a_full_scan_over_random_logs() {
        const SHARDS: u32 = 4;
        for seed in 0..300 {
            let mut r = rng(seed);
            let mut next = [1u64; SHARDS as usize];
            let mut entries = Vec::new();
            for op in 1..=(20 + r(60)) {
                if r(5) == 0 {
                    // A permit command.
                    entries.push(Entry::new(op, 0, 0, Arc::from(vec![0u8])));
                    continue;
                }
                let mut parts = Vec::new();
                for shard in 0..SHARDS {
                    if r(3) == 0 {
                        let i = shard as usize;
                        let n = 1 + r(4);
                        // Now and then a section repeats older records.
                        let from = if r(8) == 0 && next[i] > 2 {
                            1 + r(next[i] - 1)
                        } else {
                            next[i]
                        };
                        parts.push(section(shard, from, n));
                        next[i] = next[i].max(from + n);
                    }
                }
                entries.push(entry(op, &parts));
            }
            let last = entries.last().unwrap().op;
            let control = |b: &[u8]| b == [0];
            let mut index = Uncovered::new(0);
            let mut base = 0;
            let mut through = 0;
            while through < last {
                through = (through + 1 + r(10)).min(last);
                // Compaction never passes what the index has seen.
                if r(4) == 0 {
                    base = base.max(r(through + 1));
                }
                index.compact(base);
                let from = index.through();
                index
                    .extend(
                        entries.iter().filter(|e| e.op > from && e.op <= through),
                        control,
                    )
                    .unwrap();
                let limit = through - r(3).min(through - base.min(through));
                // Within an epoch a checkpoint covers at least what the
                // compacted entries held; after an epoch change, nothing.
                let covered: Vec<Lsn> = (0..SHARDS)
                    .map(|i| {
                        let floor = entries
                            .iter()
                            .filter(|e| e.op <= base && is_log_batch(&e.body))
                            .flat_map(|e| decode(&e.body).unwrap().1)
                            .filter(|s| s.shard == i)
                            .map(|s| s.first_lsn + u64::from(s.count) - 1)
                            .max()
                            .unwrap_or(0);
                        if r(6) == 0 { 0 } else { floor + r(8) }
                    })
                    .collect();
                let window = || entries.iter().filter(|e| e.op > base && e.op <= limit);
                assert_eq!(
                    index.first_uncovered(&covered, limit),
                    first_uncovered(window(), &covered).unwrap(),
                    "seed {seed}, base {base}, limit {limit}, covered {covered:?}"
                );
                assert_eq!(
                    index.any_batch(limit),
                    window().any(|e| is_log_batch(&e.body)),
                    "seed {seed}"
                );
                assert_eq!(
                    index.control(limit).collect::<Vec<_>>(),
                    window()
                        .filter(|e| control(&e.body))
                        .map(|e| e.op)
                        .collect::<Vec<_>>(),
                    "seed {seed}"
                );
            }
        }
    }

    /// The tick of an idle group of eight shards with 50,000 entries since
    /// its image, one shard writing: the check
    /// reads the index, it never walks the entries again.
    #[test]
    fn a_checkpoint_check_with_fifty_thousand_entries_since_the_image_is_cheap() {
        let entries: Vec<Entry> = (1..=50_000u64)
            .map(|op| entry(op, &[section(3, op, 1)]))
            .collect();
        let mut index = Uncovered::new(0);
        index.extend(entries.iter(), |_| false).unwrap();
        // Shard 3's checkpoint covers the first 10,000 records.
        let mut covered = vec![0; 8];
        covered[3] = 10_000;
        let started = std::time::Instant::now();
        let mut first = Vec::new();
        for _ in 0..100 {
            index.compact(0);
            // What committed since the last check: nothing (the I/O thread
            // reads it with a range of the log's map).
            let from = index.through() as usize;
            index.extend(entries[from..].iter(), |_| false).unwrap();
            first = index.first_uncovered(&covered, 50_000);
            assert!(index.any_batch(50_000));
        }
        let per_check = started.elapsed() / 100;
        let mut want = vec![None; 8];
        want[3] = Some(10_001);
        assert_eq!(first, want);
        assert_eq!(
            first_uncovered(entries.iter(), &covered).unwrap(),
            want,
            "the full scan agrees"
        );
        assert!(
            per_check < std::time::Duration::from_micros(50),
            "a check took {per_check:?}"
        );
    }

    #[test]
    fn a_segment_body_or_a_permit_command_is_never_a_log_batch() {
        let mut segment = Vec::new();
        fusee_core::wire::put_varint(&mut segment, 0xF3);
        assert!(!is_log_batch(&segment));
        assert!(!is_log_batch(&[0]));
        assert!(decode(&[0xF3, 0, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }
}
