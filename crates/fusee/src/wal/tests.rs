use super::*;
use fusee_core::record::{Record, Status, encode_canonical};
use fusee_core::wal::SectionWriter;
use std::cell::RefCell;
use std::rc::Rc;

/// A disk in memory. Writes stay pending until the file is synced; a crash
/// keeps, drops or tears each pending write.
#[derive(Default)]
struct Files {
    durable: BTreeMap<u64, Vec<u8>>,
    pending: Vec<(u64, u64, Vec<u8>)>,
    /// The process dies inside the next sync: it returns an error and
    /// nothing pending becomes durable.
    die_in_sync: bool,
    /// The process dies inside the sync after this many more.
    syncs_left: Option<u32>,
    /// Files grow by writes past their end (a blob area); otherwise such a
    /// write is refused (segments have a fixed size).
    grow: bool,
    /// Hold a background preparation open while a ready spare is used.
    delay_preparation: bool,
}

#[derive(Clone, Default)]
struct MemDevice(Rc<RefCell<Files>>);

impl MemDevice {
    /// A device whose files grow by appends, like a blob area.
    fn growing() -> Self {
        let d = Self::default();
        d.0.borrow_mut().grow = true;
        d
    }

    /// The view a reader sees: durable bytes plus pending writes.
    fn view(&self, segment: u64) -> Option<Vec<u8>> {
        let f = self.0.borrow();
        let mut img = f.durable.get(&segment)?.clone();
        for (s, off, bytes) in &f.pending {
            if *s == segment {
                img[*off as usize..*off as usize + bytes.len()].copy_from_slice(bytes);
            }
        }
        Some(img)
    }

    /// The process dies: each pending write lands, is lost or tears.
    fn crash(&self, rng: &mut u64) -> Self {
        let mut f = self.0.borrow_mut();
        let pending = std::mem::take(&mut f.pending);
        for (s, off, mut bytes) in pending {
            *rng = xorshift(*rng);
            match *rng % 3 {
                0 => {}
                1 => continue,
                _ => {
                    let cut = (*rng >> 8) as usize % bytes.len().max(1);
                    for b in &mut bytes[cut..] {
                        *rng = xorshift(*rng);
                        *b = *rng as u8;
                    }
                }
            }
            if let Some(img) = f.durable.get_mut(&s) {
                img[off as usize..off as usize + bytes.len()].copy_from_slice(&bytes);
            }
        }
        drop(f);
        self.clone()
    }
}

impl LogDevice for MemDevice {
    fn is_ready(&self, _segment: u64) -> bool {
        !self.0.borrow().delay_preparation
    }
    fn segments(&self) -> io::Result<Vec<u64>> {
        Ok(self.0.borrow().durable.keys().copied().collect())
    }
    fn len(&self, segment: u64) -> io::Result<u64> {
        Ok(self.view(segment).ok_or_else(|| io_other("missing"))?.len() as u64)
    }
    fn read(&self, segment: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let img = self.view(segment).ok_or_else(|| io_other("missing"))?;
        let start = (offset as usize).min(img.len());
        let end = start.saturating_add(len).min(img.len());
        Ok(img[start..end].to_vec())
    }
    fn create(&self, segment: u64, len: u64) -> io::Result<()> {
        self.0
            .borrow_mut()
            .durable
            .insert(segment, vec![0; len as usize]);
        Ok(())
    }
    fn write(&self, segment: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut f = self.0.borrow_mut();
        let size = f.durable.get(&segment).map(Vec::len).unwrap_or(0);
        if offset as usize + bytes.len() > size {
            if !f.grow || !f.durable.contains_key(&segment) {
                return Err(io_other("write past the end of a segment"));
            }
            // The file's new length is durable at once; its bytes are not
            // (a crash may leave zeros, or garbage, where they go).
            let img = f.durable.get_mut(&segment).unwrap();
            img.resize(offset as usize + bytes.len(), 0);
        }
        // Writes in flight together never share a block: with O_DIRECT
        // each rewrites its blocks whole, padding included.
        let blocks = |off: u64, len: usize| (off / BLOCK, (off + len.max(1) as u64 - 1) / BLOCK);
        let (first, last) = blocks(offset, bytes.len());
        if f.pending.iter().any(|(s, o, b)| {
            let (a, z) = blocks(*o, b.len());
            *s == segment && a <= last && z >= first
        }) {
            return Err(io_other("a write shares a block with one in flight"));
        }
        f.pending.push((segment, offset, bytes.to_vec()));
        Ok(())
    }
    fn sync(&self, segment: u64) -> io::Result<()> {
        let mut f = self.0.borrow_mut();
        if f.die_in_sync {
            return Err(io_other("the process died"));
        }
        match &mut f.syncs_left {
            Some(0) => return Err(io_other("the process died")),
            Some(n) => *n -= 1,
            None => {}
        }
        let pending = std::mem::take(&mut f.pending);
        for (s, off, bytes) in pending {
            if s == segment {
                let img = f.durable.get_mut(&s).unwrap();
                img[off as usize..off as usize + bytes.len()].copy_from_slice(&bytes);
            } else {
                f.pending.push((s, off, bytes));
            }
        }
        Ok(())
    }
    fn rename(&self, from: u64, to: u64) -> io::Result<()> {
        let mut f = self.0.borrow_mut();
        // Pending writes to a renamed file stay with it.
        for p in &mut f.pending {
            if p.0 == from {
                p.0 = to;
            }
        }
        let img = f.durable.remove(&from).ok_or_else(|| io_other("missing"))?;
        f.durable.insert(to, img);
        Ok(())
    }
    fn remove(&self, segment: u64) -> io::Result<()> {
        let mut f = self.0.borrow_mut();
        f.pending.retain(|p| p.0 != segment);
        f.durable.remove(&segment);
        Ok(())
    }
}

fn opts() -> WalOptions {
    WalOptions {
        group: 0,
        segment_bytes: 64 << 10,
        first_segment_bytes: 16 << 10,
        spares: 2,
        in_flight: 2,
    }
}

fn step(run: u64, seq: u32, size: usize) -> Record<&'static str, Vec<u8>> {
    Record::Step {
        run,
        seq,
        name: "s",
        attempt: 1,
        status: Status::Ok,
        payload: vec![b'p'; size],
        time: None,
    }
}

/// Appends `n` records to a shard's writer and stages them.
fn stage_records<D: LogDevice>(
    wal: &mut Wal<D>,
    w: &mut SectionWriter,
    shard: u32,
    n: u32,
    size: usize,
) {
    for i in 0..n {
        w.append(&step(u64::from(shard), i, size)).unwrap();
    }
    if let Some(b) = w.take() {
        wal.stage(shard, b).unwrap();
    }
}

#[test]
fn shards_share_batches_and_recover_their_own_records() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 3, opts(), 1, 0).unwrap();
    let mut writers: Vec<SectionWriter> = (0..3).map(|_| SectionWriter::new(1)).collect();
    for round in 0..200u32 {
        for s in 0..3u32 {
            if (round + s) % 2 == 0 {
                stage_records(&mut wal, &mut writers[s as usize], s, 1 + round % 3, 50);
            }
        }
        let wrote = wal.write(round as u64).unwrap();
        assert!(!wrote.is_empty());
        for s in wrote {
            assert_eq!(wal.durable(s), writers[s as usize].last_lsn());
        }
    }
    let (live, _) = wal.files();
    assert!(live.len() > 2, "segments rolled: {live:?}");
    drop(wal);
    let mut wal = Wal::open(dev, 3, opts(), 2, 0).unwrap();
    for s in 0..3u32 {
        let (records, last) = wal.take_recovered(s, 0).unwrap();
        assert_eq!(last, writers[s as usize].last_lsn());
        assert_eq!(records.len() as u64, last);
        for (i, (lsn, _)) in records.iter().enumerate() {
            assert_eq!(*lsn, i as u64 + 1);
        }
    }
    // Recovery after a checkpoint returns only what follows it.
    let dev2 = wal.device().clone();
    drop(wal);
    let mut wal = Wal::open(dev2, 3, opts(), 3, 0).unwrap();
    let (records, _) = wal.take_recovered(1, 40).unwrap();
    assert_eq!(records.first().map(|r| r.0), Some(41));
}

#[test]
fn segments_grow_from_the_first_size() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 1, opts(), 1, 0).unwrap();
    let mut w = SectionWriter::new(1);
    for _ in 0..400 {
        stage_records(&mut wal, &mut w, 0, 4, 100);
        wal.write(0).unwrap();
    }
    let (live, _) = wal.files();
    let sizes: Vec<u64> = live.iter().map(|n| dev.len(*n).unwrap()).collect();
    assert_eq!(sizes[0], 16 << 10);
    assert_eq!(sizes[1], 32 << 10);
    assert!(sizes[2..].iter().all(|s| *s == 64 << 10), "{sizes:?}");
}

/// Segments keep growing to the largest size while retired ones are
/// recycled: without a spare as large as the next segment, one of that size
/// is prepared, and a smaller spare carries a roll only while it is not
/// ready (renaming small spares kept a busy log rolling its first files).
#[test]
fn recycled_segments_still_grow_to_the_largest_size() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 1, opts(), 1, 0).unwrap();
    let mut w = SectionWriter::new(1);
    for round in 0..2000u32 {
        stage_records(&mut wal, &mut w, 0, 4, 100);
        wal.write(0).unwrap();
        if round % 20 == 19 {
            wal.set_retain(0, w.last_lsn()).unwrap();
        }
    }
    let (live, _) = wal.files();
    let sizes: Vec<u64> = live.iter().map(|n| dev.len(*n).unwrap()).collect();
    assert!(sizes.iter().all(|s| *s == 64 << 10), "{sizes:?}");
}

#[test]
fn a_prepared_segment_overtaken_by_a_spare_keeps_the_chain_in_order() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 1, opts(), 1, 0).unwrap();
    let mut w = SectionWriter::new(1);
    stage_records(&mut wal, &mut w, 0, 1, 100);
    wal.write(0).unwrap();
    drop(wal);
    // Recovery finds the current segment and a spare too small for
    // the next planned size, but large enough to carry a roll.
    dev.create(2, opts().first_segment_bytes).unwrap();
    let mut wal = Wal::open(dev.clone(), 1, opts(), 2, 0).unwrap();
    dev.0.borrow_mut().delay_preparation = true;
    while wal.files().0.len() < 2 {
        stage_records(&mut wal, &mut w, 0, 2, 120);
        wal.write(0).unwrap();
    }
    let prepared = wal.prepared.unwrap().0;
    assert!(prepared < wal.writer.segment());
    dev.0.borrow_mut().delay_preparation = false;
    while wal.files().0.len() < 3 {
        stage_records(&mut wal, &mut w, 0, 2, 120);
        wal.write(0).unwrap();
    }
    let (live, _) = wal.files();
    assert!(live.windows(2).all(|pair| pair[0] < pair[1]), "{live:?}");
    drop(wal);
    let mut wal = Wal::open(dev, 1, opts(), 3, 0).unwrap();
    let (records, last) = wal.take_recovered(0, 0).unwrap();
    assert_eq!(last, w.last_lsn());
    assert_eq!(records.len() as u64, last);
}

#[test]
fn covered_segments_are_retired_and_reused_without_creating_files() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 2, opts(), 1, 0).unwrap();
    let mut w: Vec<SectionWriter> = (0..2).map(|_| SectionWriter::new(1)).collect();
    let mut created = std::collections::BTreeSet::new();
    for round in 0..3000u32 {
        for s in 0..2u32 {
            stage_records(&mut wal, &mut w[s as usize], s, 2, 80);
        }
        wal.write(0).unwrap();
        for n in dev.segments().unwrap() {
            created.insert(n);
        }
        if round % 50 == 49 {
            // Both shards checkpoint: everything written so far is covered.
            wal.set_retain(0, w[0].last_lsn()).unwrap();
            wal.set_retain(1, w[1].last_lsn()).unwrap();
            let files = dev.segments().unwrap().len();
            assert!(files <= 4, "round {round}: {files} files");
        }
    }
    // After warm-up the same few files are renamed in turn.
    let (live, spares) = wal.files();
    assert!(live.len() + spares.len() <= 4);
    // Retired files never read as chain again.
    drop(wal);
    let mut wal = Wal::open(dev, 2, opts(), 2, 0).unwrap();
    let (records, last) = wal.take_recovered(0, 0).unwrap_or_default();
    let _ = (records, last);
    let (_, last) = wal.take_recovered(1, w[1].last_lsn() - 10).unwrap();
    assert_eq!(last, w[1].last_lsn());
}

#[test]
fn a_shard_without_a_reported_position_holds_every_segment() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 2, opts(), 1, 0).unwrap();
    let mut w = SectionWriter::new(1);
    for _ in 0..500 {
        stage_records(&mut wal, &mut w, 0, 2, 100);
        wal.write(0).unwrap();
    }
    let before = wal.files().0.len();
    wal.set_retain(0, w.last_lsn()).unwrap();
    assert_eq!(wal.files().0.len(), before, "shard 1 never reported");
    // Shard 1 wrote nothing: once it reports, it holds nothing back.
    assert!(!wal.pins(0));
    wal.set_retain(1, 0).unwrap();
    assert_eq!(wal.files().0.len(), 1);
}

/// Crash campaigns: every acknowledged record of every shard survives any
/// number of crashes, and LSNs stay consecutive per shard across lives.
#[test]
fn crashes_never_lose_acknowledged_records_across_lives() {
    for seed in 0..400u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut dev = MemDevice::default();
        let shards = 3u32;
        let mut acked: Vec<Lsn> = vec![0; shards as usize];
        for life in 0..6u64 {
            let mut wal = Wal::open(dev.clone(), shards, opts(), life + 1, 0).unwrap();
            let mut writers = Vec::new();
            for s in 0..shards {
                let (records, last) = wal.take_recovered(s, 0).unwrap();
                assert!(
                    last >= acked[s as usize],
                    "seed {seed} life {life}: shard {s} lost acknowledged {} (has {last})",
                    acked[s as usize]
                );
                for (i, (lsn, _)) in records.iter().enumerate() {
                    assert_eq!(*lsn, i as u64 + 1, "seed {seed}: LSNs not consecutive");
                }
                acked[s as usize] = last;
                writers.push(SectionWriter::new(last + 1));
            }
            rng = xorshift(rng);
            let rounds = rng % 40;
            for _ in 0..rounds {
                for s in 0..shards {
                    rng = xorshift(rng);
                    if rng % 2 == 0 {
                        stage_records(
                            &mut wal,
                            &mut writers[s as usize],
                            s,
                            1 + (rng % 3) as u32,
                            (rng % 300) as usize,
                        );
                    }
                }
                rng = xorshift(rng);
                if rng % 5 == 0 && wal.has_staged() {
                    // The process dies while this batch (and a segment
                    // header, if it rolls) is written.
                    dev.0.borrow_mut().die_in_sync = true;
                    assert!(wal.write(0).is_err());
                    break;
                }
                for s in wal.write(0).unwrap() {
                    acked[s as usize] = wal.durable(s);
                }
            }
            drop(wal);
            dev = dev.crash(&mut rng);
            dev.0.borrow_mut().die_in_sync = false;
        }
    }
}

#[test]
fn a_write_torn_by_a_crash_is_resumed_over_in_place() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 1, opts(), 1, 0).unwrap();
    let mut w = SectionWriter::new(1);
    for _ in 0..5 {
        stage_records(&mut wal, &mut w, 0, 1, 200);
        wal.write(0).unwrap();
    }
    // A sixth batch is written but never synced, and tears.
    stage_records(&mut wal, &mut w, 0, 1, 200);
    let placed_before = wal.writer.offset();
    {
        let staged: Vec<(u32, Batch)> = wal
            .staged
            .iter_mut()
            .enumerate()
            .filter_map(|(i, s)| s.take().map(|b| (i as u32, b)))
            .collect();
        let sections: Vec<SectionRef<'_>> = staged
            .iter()
            .map(|(s, b)| SectionRef {
                shard: *s,
                first_lsn: b.first_lsn,
                count: b.records,
                records: &b.section,
            })
            .collect();
        let p = wal
            .writer
            .place(&sections, wal.acked, false, 0, || unreachable!());
        let mut torn = p.bytes.clone();
        for b in &mut torn[30..] {
            *b = 0xA5;
        }
        dev.write(p.segment, p.offset, &torn).unwrap();
        dev.sync(p.segment).unwrap();
    }
    drop(wal);
    let mut wal = Wal::open(dev.clone(), 1, opts(), 2, 0).unwrap();
    let (records, last) = wal.take_recovered(0, 0).unwrap();
    assert_eq!((records.len(), last), (5, 5));
    assert_eq!(
        wal.writer.offset(),
        placed_before,
        "resumes over the torn bytes"
    );
    let mut w = SectionWriter::new(6);
    stage_records(&mut wal, &mut w, 0, 1, 10);
    wal.write(0).unwrap();
    drop(wal);
    let mut wal = Wal::open(dev, 1, opts(), 3, 0).unwrap();
    let (records, last) = wal.take_recovered(0, 0).unwrap();
    assert_eq!((records.len(), last), (6, 6));
    let mut buf = Vec::new();
    encode_canonical(&step(0, 0, 10), &mut buf);
    let _ = buf;
}

#[test]
fn two_writes_in_flight_publish_in_order_and_survive_a_torn_first() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 2, opts(), 1, 0).unwrap();
    let mut w0 = SectionWriter::new(1);
    let mut w1 = SectionWriter::new(1);
    stage_records(&mut wal, &mut w0, 0, 1, 100);
    let first = wal.begin_write(0, false).unwrap().unwrap();
    // The first write is in flight unpadded: the next must wait for it.
    stage_records(&mut wal, &mut w1, 1, 1, 100);
    assert!(!wal.can_begin());
    assert!(wal.begin_write(0, true).unwrap().is_none());
    dev.sync(first.segment()).unwrap();
    assert_eq!(wal.complete(first), vec![0]);
    // Under load: the second goes out while it is in flight, padded, and a
    // third may follow it.
    let second = wal.begin_write(0, true).unwrap().unwrap();
    stage_records(&mut wal, &mut w0, 0, 2, 100);
    let third = wal.begin_write(0, true).unwrap().unwrap();
    assert!(wal.complete_is_pending());
    // The third completes first: nothing is published until the second does.
    assert!(wal.complete(third).is_empty());
    assert_eq!(wal.durable(0), 1);
    let mut shards = wal.complete(second);
    shards.sort_unstable();
    assert_eq!(shards, vec![0, 1]);
    assert_eq!((wal.durable(0), wal.durable(1)), (3, 1));
}

#[test]
fn a_crash_with_two_writes_in_flight_keeps_only_whole_acknowledged_batches() {
    let only: Option<u64> = std::env::var("WAL_SEED").ok().and_then(|s| s.parse().ok());
    for seed in 0..1500u64 {
        if only.is_some_and(|o| o != seed) {
            continue;
        }
        let trace = only.is_some();
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut dev = MemDevice::default();
        let mut acked = 0u64;
        let o = WalOptions {
            in_flight: 1 + (seed % 3) as usize,
            spares: (seed / 3 % 3) as usize,
            ..opts()
        };
        for life in 0..5u64 {
            let mut wal = Wal::open(dev.clone(), 1, o, life + 1, 0).unwrap();
            if trace {
                eprintln!(
                    "life {life}: files {:?} writer seg {} off {}",
                    wal.files(),
                    wal.writer.segment(),
                    wal.writer.offset()
                );
            }
            let (_, last) = wal.take_recovered(0, 0).unwrap();
            if trace {
                eprintln!("  recovered last {last}, acked {acked}");
            }
            assert!(
                last >= acked,
                "seed {seed}: lost acknowledged {acked}, have {last}"
            );
            acked = last;
            let mut w = SectionWriter::new(last + 1);
            let mut inflight: Vec<InFlight> = Vec::new();
            for _ in 0..30 {
                rng = xorshift(rng);
                if rng % 3 != 0 || inflight.is_empty() {
                    stage_records(
                        &mut wal,
                        &mut w,
                        0,
                        1 + (rng % 3) as u32,
                        (rng % 500) as usize,
                    );
                    if let Some(f) = wal.begin_write(0, rng % 2 == 0).unwrap() {
                        if trace {
                            eprintln!(
                                "  begin seq {} seg {} (lsn ..{})",
                                f.seq(),
                                f.segment(),
                                w.last_lsn()
                            );
                        }
                        inflight.push(f);
                    }
                } else {
                    // Complete one of the writes in flight, in any order: a
                    // completion syncs that write's segment. The memory
                    // device's sync makes every pending write of the segment
                    // durable, so writes in flight in the same segment are
                    // complete too; those in another segment are not.
                    let i = (rng >> 8) as usize % inflight.len();
                    let f = inflight.remove(i);
                    let segment = f.segment();
                    dev.sync(segment).unwrap();
                    wal.complete(f);
                    let (same, other): (Vec<_>, Vec<_>) =
                        inflight.drain(..).partition(|g| g.segment() == segment);
                    inflight = other;
                    for g in same {
                        wal.complete(g);
                    }
                    acked = acked.max(wal.durable(0));
                    if trace {
                        eprintln!(
                            "  completed seg {segment}: durable {} acked {acked}",
                            wal.durable(0)
                        );
                    }
                }
            }
            if trace {
                eprintln!("  crash with {} in flight", inflight.len());
            }
            drop(wal);
            dev = dev.crash(&mut rng);
        }
    }
}

/// A roll whose earlier batch never became durable leaves a segment with a
/// valid header the chain does not pass through. It must neither end the
/// chain at the next open nor be taken for chain after it is reused or
/// skipped: here the next life rolls past it into a higher spare.
#[test]
fn a_stale_segment_below_the_chain_never_hides_later_segments() {
    use fusee_core::wal::{BatchHeader, encode_batch};
    let dev = MemDevice::default();
    let o = WalOptions {
        spares: 4,
        ..opts()
    };
    // Life 1: a few acknowledged batches in segment 1.
    let mut wal = Wal::open(dev.clone(), 1, o, 1, 0).unwrap();
    let mut w = SectionWriter::new(1);
    for _ in 0..3 {
        stage_records(&mut wal, &mut w, 0, 1, 100);
        wal.write(0).unwrap();
    }
    let acked = w.last_lsn();
    drop(wal);
    // Segment 2: the roll of a batch whose predecessor was lost. Its header
    // and its one batch are valid, but it does not continue the chain.
    dev.create(2, 16 << 10).unwrap();
    let header = WalHeader {
        group: 0,
        segment: 2,
        salt: 99,
        base_seq: 5,
        base_prev: 0xdead_beef,
        created_ms: 0,
    };
    dev.write(2, 0, &header.encode()).unwrap();
    let mut rec = Vec::new();
    encode_canonical(&step(0, 0, 10), &mut rec);
    let mut batch = Vec::new();
    encode_batch(
        &mut batch,
        99,
        &BatchHeader {
            seq: 5,
            prev: 0xdead_beef,
            acked: 3,
            life: 1,
            flags: 0,
        },
        &[SectionRef {
            shard: 0,
            first_lsn: acked + 2,
            count: 1,
            records: &rec,
        }],
        8,
    );
    dev.write(2, WAL_HEADER_BYTES, &batch).unwrap();
    dev.sync(2).unwrap();
    // Segment 3: a spare prepared ahead of that roll, never activated.
    dev.create(3, 64 << 10).unwrap();
    // Life 2 writes enough to roll at least once.
    let mut wal = Wal::open(dev.clone(), 1, o, 2, 0).unwrap();
    let (_, last) = wal.take_recovered(0, 0).unwrap();
    assert_eq!(last, acked);
    let mut w = SectionWriter::new(last + 1);
    // Exactly one roll, into the higher spare: the stale segment still sits
    // between the chain's two segments when the log reopens.
    while wal.files().0.len() < 2 {
        stage_records(&mut wal, &mut w, 0, 2, 120);
        wal.write(0).unwrap();
    }
    let (live, _) = wal.files();
    assert!(live[1] > 2, "{live:?}");
    let acked = w.last_lsn();
    drop(wal);
    for life in 3..6 {
        let mut wal = Wal::open(dev.clone(), 1, o, life, 0).unwrap();
        let (records, last) = wal.take_recovered(0, 0).unwrap();
        assert_eq!(last, acked, "life {life}: lost acknowledged records");
        assert_eq!(records.len() as u64, acked);
    }
}

#[test]
fn an_idle_shard_holding_old_records_is_asked_for_a_checkpoint() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev, 2, opts(), 1, 0).unwrap();
    let mut idle = SectionWriter::new(1);
    let mut busy = SectionWriter::new(1);
    stage_records(&mut wal, &mut idle, 1, 3, 50);
    wal.write(0).unwrap();
    // The idle shard's checkpoints cover LSN 0 (none) and later its records.
    wal.set_retain(1, 0).unwrap();
    for _ in 0..600 {
        stage_records(&mut wal, &mut busy, 0, 2, 100);
        wal.write(0).unwrap();
        wal.set_retain(0, busy.last_lsn()).unwrap();
    }
    let held = wal.files().0.len();
    assert!(
        held > 2,
        "the idle shard's old records hold segments: {held}"
    );
    assert!(wal.pins(1));
    assert!(!wal.pins(0));
    // It checkpoints with nothing new: both retained checkpoints cover its
    // last record, and the log shrinks.
    wal.set_retain(1, idle.last_lsn()).unwrap();
    assert!(!wal.pins(1));
    assert!(wal.files().0.len() <= 2, "{:?}", wal.files());
}

/// Real files with `O_DIRECT` (Linux): packed and padded writes, rolls, and
/// a resume at an unaligned end all read back exactly.
#[cfg(target_os = "linux")]
#[test]
fn direct_io_writes_read_back_across_rolls_and_resumes() {
    let dir = std::env::temp_dir().join(format!("fusee-wal-direct-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let local = LocalOptions {
        direct_io: true,
        flush: FlushMethod::Dsync,
        ..LocalOptions::default()
    };
    let o = WalOptions {
        segment_bytes: 256 << 10,
        first_segment_bytes: 64 << 10,
        ..opts()
    };
    let dev = FileDevice::open(&dir, local.clone()).unwrap();
    if !dev.direct_io() {
        eprintln!("skipped: the temp directory does not support O_DIRECT");
        return;
    }
    let mut wal = Wal::open(dev, 2, o, 1, 0).unwrap();
    let mut w = [SectionWriter::new(1), SectionWriter::new(1)];
    for i in 0..300u32 {
        for s in 0..2u32 {
            stage_records(
                &mut wal,
                &mut w[s as usize],
                s,
                1 + i % 3,
                (i as usize * 37) % 700,
            );
        }
        if i % 3 == 0 {
            wal.write(0).unwrap();
        } else {
            // Overlapping writes: padded, synced after both are written.
            let a = wal.begin_write(0, true).unwrap();
            if let Some(a) = a {
                let sync = a.segment();
                wal.device().sync(sync).unwrap();
                wal.complete(a);
            }
        }
    }
    let last = [w[0].last_lsn(), w[1].last_lsn()];
    drop(wal);
    {
        let life = 2;
        let dev = FileDevice::open(&dir, local.clone()).unwrap();
        let mut wal = Wal::open(dev, 2, o, life, 0).unwrap();
        for s in 0..2u32 {
            let (records, l) = wal.take_recovered(s, 0).unwrap();
            assert_eq!(l, last[s as usize], "life {life} shard {s}");
            assert_eq!(records.len() as u64, l);
        }
        // Resume in place at whatever (unaligned) end the chain has.
        let mut more = SectionWriter::new(last[0] + 1);
        stage_records(&mut wal, &mut more, 0, 1, 13);
        wal.write(0).unwrap();
        drop(wal);
        let dev = FileDevice::open(&dir, local.clone()).unwrap();
        let mut wal = Wal::open(dev, 2, o, 100 + life, 0).unwrap();
        let (_, l) = wal.take_recovered(0, 0).unwrap();
        assert_eq!(l, last[0] + 1);
    }
    let _ = fs::remove_dir_all(&dir);
}

/// Real files with `O_DIRECT` (Linux): two writes in flight together, the
/// first starting inside the block a packed write ended in, land in the
/// opposite order. Each rewrites its blocks whole, zero padding included,
/// so they must not share one; both batches survive.
#[cfg(target_os = "linux")]
#[test]
fn direct_io_writes_in_flight_together_survive_either_order() {
    let dir = std::env::temp_dir().join(format!("fusee-wal-direct-order-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let local = LocalOptions {
        direct_io: true,
        flush: FlushMethod::Dsync,
        ..LocalOptions::default()
    };
    let o = WalOptions {
        segment_bytes: 256 << 10,
        first_segment_bytes: 64 << 10,
        ..opts()
    };
    let dev = FileDevice::open(&dir, local.clone()).unwrap();
    if !dev.direct_io() {
        eprintln!("skipped: the temp directory does not support O_DIRECT");
        return;
    }
    let mut wal = Wal::open(dev, 2, o, 1, 0).unwrap();
    let mut w = [SectionWriter::new(1), SectionWriter::new(1)];
    for i in 0..60usize {
        // A packed write: the log's end is now inside a block.
        stage_records(&mut wal, &mut w[0], 0, 1, 100 + i * 13);
        wal.write(0).unwrap();
        stage_records(&mut wal, &mut w[0], 0, 1, 300 + i * 7);
        let mut first = wal.seal(0, true).unwrap().unwrap();
        stage_records(&mut wal, &mut w[1], 1, 2, 500);
        let mut second = wal
            .seal(0, true)
            .unwrap()
            .expect("a second write in flight");
        Wal::perform(wal.device(), &mut second).unwrap();
        Wal::perform(wal.device(), &mut first).unwrap();
        wal.complete(second);
        wal.complete(first);
    }
    let last = [w[0].last_lsn(), w[1].last_lsn()];
    drop(wal);
    let dev = FileDevice::open(&dir, local).unwrap();
    let mut wal = Wal::open(dev, 2, o, 2, 0).unwrap();
    for s in 0..2u32 {
        let (records, l) = wal.take_recovered(s, 0).unwrap();
        assert_eq!(l, last[s as usize], "shard {s}");
        assert_eq!(records.len() as u64, l);
    }
    drop(wal);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn the_shared_log_stops_its_threads_when_its_last_user_drops_it() {
    let dir = std::env::temp_dir().join(format!(
        "fusee-wal-drop-{}-{}",
        std::process::id(),
        unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let local = LocalOptions {
        flush: crate::local::FlushMethod::Fsync,
        direct_io: false,
        ..LocalOptions::default()
    };
    for _ in 0..5 {
        let log = SharedWal::open(&dir, 2, local.clone(), WalOptions::default()).unwrap();
        let weak = Arc::downgrade(&log);
        // Let the threads reach their waits, which once held the log.
        std::thread::sleep(std::time::Duration::from_millis(30));
        drop(log);
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        while weak.strong_count() > 0 {
            assert!(
                Instant::now() < deadline,
                "the log's threads kept it alive after its last user dropped it"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Shards flushing inline from their own threads at once: every record is
/// durable when its write returns and recovers after a reopen, and writes
/// from different threads share batches.
#[test]
fn concurrent_inline_writers_share_batches() {
    concurrent_inline_writers(
        "buffered",
        LocalOptions {
            flush: crate::local::FlushMethod::Fsync,
            direct_io: false,
            ..LocalOptions::default()
        },
    );
}

/// [`concurrent_inline_writers_share_batches`] with the platform's flush:
/// on Linux `O_DIRECT` writes with `RWF_DSYNC`, through the log owner
/// where `io_uring` is allowed, across segment starts (the test segments
/// are small).
#[test]
fn concurrent_inline_writers_with_the_default_flush() {
    concurrent_inline_writers("default", LocalOptions::default());
}

fn concurrent_inline_writers(name: &str, local: LocalOptions) {
    let dir = std::env::temp_dir().join(format!(
        "fusee-wal-concurrent-{name}-{}-{}",
        std::process::id(),
        unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    const SHARDS: u32 = 4;
    const STEPS: u32 = 150;
    let started = Instant::now();
    let batches = {
        let log = SharedWal::open(&dir, SHARDS, local.clone(), opts()).unwrap();
        let first = log.position.load(Ordering::Acquire);
        let threads: Vec<_> = (0..SHARDS)
            .map(|shard| {
                let log = log.clone();
                std::thread::spawn(move || {
                    let mut w = SectionWriter::new(1);
                    for i in 0..STEPS {
                        w.append(&step(u64::from(shard), i, 40)).unwrap();
                        let b = w.take().unwrap();
                        let last = b.last_lsn;
                        let durable = log.write_now(shard, b).unwrap();
                        assert!(durable >= last, "shard {shard}: {durable} < {last}");
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        log.position.load(Ordering::Acquire) - first
    };
    let elapsed = started.elapsed();
    let records = u64::from(SHARDS * STEPS);
    eprintln!(
        "{records} records in {batches} batches ({:.2} per batch), {elapsed:?}",
        records as f64 / batches as f64
    );
    // Everything is there after a reopen.
    let log = SharedWal::open(&dir, SHARDS, local, opts()).unwrap();
    for shard in 0..SHARDS {
        let (recs, _) = log.take_recovered(shard, 0).unwrap();
        assert_eq!(recs.len() as u32, STEPS, "shard {shard}");
    }
    assert!(batches <= records);
    drop(log);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Shards flushing inline (waiting, handed writes in turn) and shards
/// submitting (staged for the log and I/O threads, told once durable) at
/// once: no flush waits out a park timeout for long, and every record
/// recovers.
#[test]
fn inline_and_submitted_flushes_interleave_without_stalls() {
    inline_and_submitted(
        "buffered",
        LocalOptions {
            flush: crate::local::FlushMethod::Fsync,
            direct_io: false,
            ..LocalOptions::default()
        },
    );
}

/// [`inline_and_submitted_flushes_interleave_without_stalls`] with the
/// platform's flush (on Linux, the log owner where `io_uring` is allowed).
#[test]
fn inline_and_submitted_flushes_interleave_with_the_default_flush() {
    inline_and_submitted("default", LocalOptions::default());
}

/// The log owner's thread exits and frees the log once its last user
/// drops it, with writes just completed (on other platforms: the log's
/// threads, as in the test above).
#[test]
fn the_shared_log_with_the_default_flush_stops_when_dropped() {
    let dir = std::env::temp_dir().join(format!(
        "fusee-wal-drop-default-{}-{}",
        std::process::id(),
        unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    for round in 0..5u64 {
        let log = SharedWal::open(&dir, 2, LocalOptions::default(), opts()).unwrap();
        let (recs, last) = log.take_recovered(0, 0).unwrap();
        assert_eq!(recs.len() as u64, round * 10, "round {round}");
        let mut w = SectionWriter::new(last + 1);
        for i in 0..10 {
            w.append(&step(round, i, 40)).unwrap();
            let b = w.take().unwrap();
            let last = b.last_lsn;
            log.submit(0, b);
            // Durable before the next: the round's records all recover.
            let deadline = Instant::now() + std::time::Duration::from_secs(5);
            while log.shards[0].durable.load(Ordering::Acquire) < last {
                assert!(
                    Instant::now() < deadline,
                    "round {round}: write {i} never completed"
                );
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        }
        let weak = Arc::downgrade(&log);
        drop(log);
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        while weak.strong_count() > 0 {
            assert!(
                Instant::now() < deadline,
                "the log's threads kept it alive after its last user dropped it"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn inline_and_submitted(name: &str, local: LocalOptions) {
    let dir = std::env::temp_dir().join(format!(
        "fusee-wal-mixed-{name}-{}-{}",
        std::process::id(),
        unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    const SHARDS: u32 = 6;
    const STEPS: u32 = 200;
    let started = Instant::now();
    {
        let log = SharedWal::open(&dir, SHARDS, local.clone(), opts()).unwrap();
        let threads: Vec<_> = (0..SHARDS)
            .map(|shard| {
                let log = log.clone();
                std::thread::spawn(move || {
                    let me = std::thread::current();
                    *log.shards[shard as usize].notify.lock().unwrap() =
                        Some(Arc::new(move || me.unpark()));
                    let mut w = SectionWriter::new(1);
                    let mut slowest = std::time::Duration::ZERO;
                    for i in 0..STEPS {
                        w.append(&step(u64::from(shard), i, 40)).unwrap();
                        let b = w.take().unwrap();
                        let last = b.last_lsn;
                        let t = Instant::now();
                        // Even shards flush inline, odd ones submit, and each
                        // switches halfway.
                        if (shard + u32::from(i >= STEPS / 2)).is_multiple_of(2) {
                            assert!(log.write_now(shard, b).unwrap() >= last);
                        } else {
                            log.submit(shard, b);
                            let slot = &log.shards[shard as usize];
                            while slot.durable.load(Ordering::Acquire) < last {
                                std::thread::park_timeout(std::time::Duration::from_secs(1));
                            }
                        }
                        slowest = slowest.max(t.elapsed());
                    }
                    slowest
                })
            })
            .collect();
        for t in threads {
            let slowest = t.join().unwrap();
            assert!(
                slowest < std::time::Duration::from_millis(900),
                "a flush waited {slowest:?}: a wake was lost"
            );
        }
    }
    eprintln!("{} flushes in {:?}", SHARDS * STEPS, started.elapsed());
    let log = SharedWal::open(&dir, SHARDS, local, opts()).unwrap();
    for shard in 0..SHARDS {
        let (recs, _) = log.take_recovered(shard, 0).unwrap();
        assert_eq!(recs.len() as u32, STEPS, "shard {shard}");
    }
    drop(log);
    let _ = std::fs::remove_dir_all(&dir);
}

// Payload offload: records longer than the blob area's threshold.

/// A payload that names its shard, LSN, writer life and size, filled with
/// bytes derived from them, so a recovered record shows whether it is
/// exactly what was written.
fn named_payload(shard: u32, lsn: Lsn, life: u64, size: usize) -> Vec<u8> {
    let mut p = format!("{shard}/{lsn}/{life}/{size}/").into_bytes();
    let mut x = (u64::from(shard) << 40) ^ (lsn << 8) ^ life ^ 0x9E37;
    while p.len() < size {
        x = xorshift(x | 1);
        p.push(x as u8);
    }
    p
}

/// Checks a recovered record's payload against [`named_payload`] and
/// returns it.
fn check_named(shard: u32, lsn: Lsn, rec: &RecordBuf) -> Result<Vec<u8>, String> {
    let Record::Step { payload, .. } = rec else {
        return Err(format!("shard {shard} record {lsn} is {rec:?}"));
    };
    let text = String::from_utf8_lossy(&payload[..payload.len().min(48)]).into_owned();
    let f: Vec<&str> = text.splitn(5, '/').collect();
    let (Some(life), Some(size)) = (
        f.get(2).and_then(|v| v.parse::<u64>().ok()),
        f.get(3).and_then(|v| v.parse::<usize>().ok()),
    ) else {
        return Err(format!(
            "shard {shard} record {lsn}: unreadable payload {text:?}"
        ));
    };
    if f[0] != shard.to_string() || f[1] != lsn.to_string() {
        return Err(format!("shard {shard} record {lsn} holds {text:?}"));
    }
    if payload[..] != named_payload(shard, lsn, life, size)[..] {
        return Err(format!("shard {shard} record {lsn}: payload damaged"));
    }
    Ok(payload.to_vec())
}

fn named_step(shard: u32, lsn: Lsn, life: u64, size: usize) -> Record<&'static str, Vec<u8>> {
    Record::Step {
        run: u64::from(shard),
        seq: lsn as u32,
        name: "s",
        attempt: 1,
        status: Status::Ok,
        payload: named_payload(shard, lsn, life, size),
        time: None,
    }
}

fn blob_wal(
    dev: &MemDevice,
    blobs: &MemDevice,
    shards: u32,
    threshold: u64,
    life: u64,
) -> io::Result<Wal<MemDevice>> {
    Wal::open_with_blobs(
        dev.clone(),
        Blobs {
            dev: blobs.clone(),
            threshold,
        },
        shards,
        opts(),
        life,
        0,
    )
}

#[test]
fn records_over_the_threshold_round_trip_through_the_blob_area() {
    let dev = MemDevice::default();
    let blobs = MemDevice::growing();
    let mut wal = blob_wal(&dev, &blobs, 2, 1000, 1).unwrap();
    let mut w = [SectionWriter::new(1), SectionWriter::new(1)];
    let sizes = [10usize, 5000, 20, 3000, 900, 40_000, 7];
    for (i, size) in sizes.iter().enumerate() {
        let s = (i % 2) as u32;
        let lsn = w[s as usize].next_lsn();
        w[s as usize].append(&named_step(s, lsn, 1, *size)).unwrap();
        wal.stage(s, w[s as usize].take().unwrap()).unwrap();
    }
    // Shard 1's records all wait behind its first large one; shard 0's
    // small first record does not wait for anything.
    assert_eq!(wal.blob_stats().jobs, 3);
    wal.write(0).unwrap();
    assert_eq!((wal.durable(0), wal.durable(1)), (4, 0));
    wal.flush_blobs().unwrap();
    wal.write(0).unwrap();
    assert_eq!((wal.durable(0), wal.durable(1)), (4, 3));
    assert_eq!(blobs.segments().unwrap(), vec![1]);
    let log_bytes = wal.bytes_written;
    assert!(
        log_bytes < 16 << 10,
        "large payloads stayed out of the log: {log_bytes}"
    );
    drop(wal);
    let mut wal = blob_wal(&dev, &blobs, 2, 1000, 2).unwrap();
    for s in 0..2u32 {
        let (records, last) = wal.take_recovered(s, 0).unwrap();
        assert_eq!(records.len() as u64, last);
        for (lsn, rec) in &records {
            check_named(s, *lsn, rec).unwrap();
        }
    }
    // A log opened without its blob area cannot read them back, and says why.
    drop(wal);
    let mut plain = Wal::open(dev.clone(), 2, opts(), 3, 0).unwrap();
    let e = plain.take_recovered(1, 0).unwrap_err().to_string();
    assert!(e.contains("blob"), "{e}");
    // Damage to a blob is caught by its checksum, naming the record.
    drop(plain);
    let first = blobs.view(1).unwrap();
    let mut damaged = first.clone();
    damaged[100] ^= 1;
    blobs.0.borrow_mut().durable.insert(1, damaged);
    let mut wal = blob_wal(&dev, &blobs, 2, 1000, 4).unwrap();
    let e = wal.take_recovered(1, 0).unwrap_err().to_string();
    assert!(e.contains("acknowledged record was lost or damaged"), "{e}");
}

/// Below the threshold nothing changes: no blob file, no waiting, and the
/// log holds the same bytes as a log without a blob area.
#[test]
fn records_under_the_threshold_cost_nothing() {
    let (a, b) = (MemDevice::default(), MemDevice::default());
    let blobs = MemDevice::growing();
    let mut with = blob_wal(&a, &blobs, 2, 64 << 10, 1).unwrap();
    let mut without = Wal::open(b.clone(), 2, opts(), 1, 0).unwrap();
    let mut w1 = [SectionWriter::new(1), SectionWriter::new(1)];
    let mut w2 = w1.clone();
    for round in 0..300u32 {
        for s in 0..2u32 {
            let size = (round as usize * 37 + s as usize * 11) % 2000;
            stage_records(&mut with, &mut w1[s as usize], s, 2, size);
            stage_records(&mut without, &mut w2[s as usize], s, 2, size);
        }
        with.write(0).unwrap();
        without.write(0).unwrap();
        assert_eq!(with.blob_stats(), BlobStats::default());
    }
    assert!(blobs.segments().unwrap().is_empty());
    assert_eq!(a.segments().unwrap(), b.segments().unwrap());
    for n in a.segments().unwrap() {
        assert_eq!(a.view(n), b.view(n), "segment {n}");
    }
}

/// Blob files go with the last segment that references them, and a file
/// nothing references (a crash between a blob and its batch) is deleted at
/// the next open.
#[test]
fn blob_files_are_retired_with_the_segments_that_reference_them() {
    let dev = MemDevice::default();
    let blobs = MemDevice::growing();
    let mut wal = blob_wal(&dev, &blobs, 1, 5000, 1).unwrap();
    let mut w = SectionWriter::new(1);
    let mut max_files = 0;
    let mut retained = 0;
    for round in 0..400u32 {
        let lsn = w.next_lsn();
        // About 3 KiB of log per round: a segment holds some twenty rounds.
        let size = if round % 4 == 0 { 300_000 } else { 4000 };
        w.append(&named_step(0, lsn, 1, size)).unwrap();
        wal.stage(0, w.take().unwrap()).unwrap();
        wal.flush_blobs().unwrap();
        wal.write(0).unwrap();
        max_files = max_files.max(blobs.segments().unwrap().len());
        if round % 40 == 39 {
            retained = w.last_lsn();
            wal.set_retain(0, retained).unwrap();
            // Only files the active segment's batches reference are left.
            let files = blobs.segments().unwrap().len();
            assert!(files <= 4, "round {round}: {files} blob files kept");
        }
    }
    assert!(max_files >= 2, "blob files rolled: {max_files}");
    // A blob whose batch was never written is garbage after a crash.
    let lsn = w.next_lsn();
    w.append(&named_step(0, lsn, 1, 400_000)).unwrap();
    wal.stage(0, w.take().unwrap()).unwrap();
    let jobs = wal.take_blob_jobs();
    for j in &jobs {
        j.perform(&blobs).unwrap();
        blobs.sync(j.file).unwrap();
    }
    drop(wal);
    let before = blobs.segments().unwrap();
    let mut wal = blob_wal(&dev, &blobs, 1, 5000, 2).unwrap();
    let (records, last) = wal.take_recovered(0, retained).unwrap();
    assert_eq!(last, lsn - 1);
    for (l, rec) in &records {
        check_named(0, *l, rec).unwrap();
    }
    let after = blobs.segments().unwrap();
    assert!(
        after.len() < before.len() || !jobs[0].create,
        "an unreferenced blob file was kept: {before:?} -> {after:?}"
    );
    // Once no segment references a blob, no blob file is left.
    wal.set_retain(0, last).unwrap();
    let mut w = SectionWriter::new(last + 1);
    for _ in 0..600 {
        let lsn = w.next_lsn();
        w.append(&named_step(0, lsn, 2, 120)).unwrap();
        wal.stage(0, w.take().unwrap()).unwrap();
        wal.write(0).unwrap();
        wal.set_retain(0, lsn).unwrap();
    }
    assert!(
        blobs.segments().unwrap().is_empty(),
        "blob files left: {:?}",
        blobs.segments().unwrap()
    );
}

/// One run of the blob crash campaign: shards stage records of mixed sizes;
/// blob writes, blob syncs, batch writes and their completions happen in a
/// random order with up to two writes in flight; then the process dies and
/// both devices resolve their pending writes, each on its own. `early` is
/// the sabotage: sections are released as soon as their blobs are written
/// instead of once they are durable.
fn blob_campaign(seed: u64, early: bool) -> Result<(), String> {
    let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let (mut dev, mut blobs) = (MemDevice::default(), MemDevice::growing());
    let shards = 2u32;
    let threshold = 400u64;
    // Acknowledged records' payloads, per shard and LSN.
    let mut acked: Vec<BTreeMap<Lsn, Vec<u8>>> = vec![BTreeMap::new(); shards as usize];
    for life in 1..=5u64 {
        let mut wal = blob_wal(&dev, &blobs, shards, threshold, life)
            .map_err(|e| format!("seed {seed} life {life}: open: {e}"))?;
        let mut writers = Vec::new();
        for s in 0..shards {
            let (records, last) = wal
                .take_recovered(s, 0)
                .map_err(|e| format!("seed {seed} life {life}: {e}"))?;
            let have: BTreeMap<Lsn, Vec<u8>> = records
                .iter()
                .map(|(l, r)| check_named(s, *l, r).map(|p| (*l, p)))
                .collect::<Result<_, _>>()
                .map_err(|e| format!("seed {seed} life {life}: {e}"))?;
            for (lsn, p) in &acked[s as usize] {
                if have.get(lsn) != Some(p) {
                    return Err(format!(
                        "seed {seed} life {life}: shard {s} lost acknowledged record {lsn}"
                    ));
                }
            }
            writers.push(SectionWriter::new(last + 1));
        }
        let mut inflight: Vec<InFlight> = Vec::new();
        // Blob writes performed and not yet synced.
        let mut unsynced: Vec<BlobJob> = Vec::new();
        let mut appended: Vec<BTreeMap<Lsn, Vec<u8>>> = vec![BTreeMap::new(); shards as usize];
        rng = xorshift(rng);
        let ops = 10 + rng % 50;
        for _ in 0..ops {
            rng = xorshift(rng);
            match rng % 6 {
                0 | 1 => {
                    let s = (rng >> 8) as u32 % shards;
                    for _ in 0..1 + (rng >> 16) % 3 {
                        rng = xorshift(rng);
                        let size = [20, 150, 390, 900, 3000, 20_000][(rng % 6) as usize];
                        let w = &mut writers[s as usize];
                        let lsn = w.next_lsn();
                        let rec = named_step(s, lsn, life, size);
                        if let Record::Step { payload, .. } = &rec {
                            appended[s as usize].insert(lsn, payload.clone());
                        }
                        w.append(&rec).unwrap();
                    }
                    if let Some(b) = writers[s as usize].take() {
                        wal.stage(s, b).unwrap();
                    }
                }
                2 => {
                    for j in wal.take_blob_jobs() {
                        j.perform(&blobs).unwrap();
                        if early {
                            wal.blobs_written(j.id).unwrap();
                        }
                        unsynced.push(j);
                    }
                }
                3 => {
                    if let Some(last) = unsynced.last().map(|j| j.id) {
                        let mut files: Vec<u64> = unsynced.iter().map(|j| j.file).collect();
                        files.dedup();
                        for f in files {
                            blobs.sync(f).unwrap();
                        }
                        unsynced.clear();
                        wal.blobs_written(last).unwrap();
                    }
                }
                4 => {
                    if let Some(f) = wal.begin_write(0, rng.is_multiple_of(2)).unwrap() {
                        inflight.push(f);
                    }
                }
                _ => {
                    if !inflight.is_empty() {
                        let i = (rng >> 8) as usize % inflight.len();
                        let f = inflight.remove(i);
                        let segment = f.segment();
                        dev.sync(segment).unwrap();
                        wal.complete(f);
                        let (same, other): (Vec<_>, Vec<_>) =
                            inflight.drain(..).partition(|g| g.segment() == segment);
                        inflight = other;
                        for g in same {
                            wal.complete(g);
                        }
                        for s in 0..shards as usize {
                            let durable = wal.durable(s as u32);
                            for (lsn, p) in appended[s].range(..=durable) {
                                acked[s].insert(*lsn, p.clone());
                            }
                        }
                    }
                }
            }
        }
        drop(wal);
        dev = dev.crash(&mut rng);
        blobs = blobs.crash(&mut rng);
    }
    Ok(())
}

/// Crash campaigns where blob writes and batches tear independently:
/// every acknowledged record comes back whole, through any number of lives.
#[test]
fn offloaded_records_survive_crashes_that_tear_blobs_and_batches_independently() {
    let only: Option<u64> = std::env::var("WAL_SEED").ok().and_then(|s| s.parse().ok());
    for seed in 0..1500u64 {
        if only.is_some_and(|o| o != seed) {
            continue;
        }
        if let Err(e) = blob_campaign(seed, false) {
            panic!("{e}");
        }
    }
}

/// The campaign's checker catches a log that writes a batch before the
/// blobs it references are durable.
#[test]
fn the_campaign_catches_batches_written_before_their_blobs_are_durable() {
    for seed in 0..1500u64 {
        if let Err(e) = blob_campaign(seed, true) {
            eprintln!("caught by seed {seed}: {e}");
            return;
        }
    }
    panic!("no seed caught batches written ahead of their blobs");
}

/// The shared log offloads large records on its own thread: shards write
/// inline while another shard's large records go to the blob area, and
/// everything recovers after a reopen.
#[test]
fn the_shared_log_writes_blobs_in_parallel_with_batches() {
    let dir = std::env::temp_dir().join(format!(
        "fusee-wal-blobs-{}-{}",
        std::process::id(),
        unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let local = LocalOptions {
        flush: crate::local::FlushMethod::Fsync,
        direct_io: false,
        ..LocalOptions::default()
    };
    let big = 2 << 20;
    {
        let log =
            SharedWal::open_with_offload(&dir, 2, local.clone(), WalOptions::default(), 64 << 10)
                .unwrap();
        let threads: Vec<_> = (0..2u32)
            .map(|shard| {
                let log = log.clone();
                std::thread::spawn(move || {
                    let mut w = SectionWriter::new(1);
                    for i in 0..40u64 {
                        let lsn = w.next_lsn();
                        let size = if shard == 0 && i % 10 == 0 { big } else { 64 };
                        w.append(&named_step(shard, lsn, 1, size)).unwrap();
                        let b = w.take().unwrap();
                        let last = b.last_lsn;
                        let durable = log.write_now(shard, b).unwrap();
                        assert!(durable >= last, "shard {shard}: {durable} < {last}");
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let stats = log.blob_stats();
        assert_eq!(stats.records, 4);
        assert_eq!((stats.held, stats.jobs), (0, 0));
        assert!(dir.join("blobs").is_dir());
    }
    let log = SharedWal::open(&dir, 2, local, WalOptions::default()).unwrap();
    for shard in 0..2u32 {
        let (recs, _) = log.take_recovered(shard, 0).unwrap();
        assert_eq!(recs.len(), 40, "shard {shard}");
        for (lsn, rec) in &recs {
            check_named(shard, *lsn, rec).unwrap();
        }
    }
    drop(log);
    let _ = std::fs::remove_dir_all(&dir);
}

/// An `O_DIRECT` write the kernel rejects with `EINVAL` (a filesystem or
/// device that took the open but not the write) moves the log to buffered
/// writes instead of failing it: shards keep flushing, and every record
/// written before and after recovers.
#[cfg(target_os = "linux")]
#[test]
fn a_rejected_direct_write_moves_the_log_to_buffered_writes() {
    let dir = std::env::temp_dir().join(format!(
        "fusee-wal-einval-{}-{}",
        std::process::id(),
        unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let local = LocalOptions {
        flush: crate::local::FlushMethod::Dsync,
        direct_io: true,
        ..LocalOptions::default()
    };
    const SHARDS: u32 = 4;
    const STEPS: u32 = 300;
    {
        let log = SharedWal::open(&dir, SHARDS, local.clone(), opts()).unwrap();
        if !log.direct_io() {
            eprintln!("no O_DIRECT under {}: nothing to test", dir.display());
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        let threads: Vec<_> = (0..SHARDS)
            .map(|shard| {
                let log = log.clone();
                std::thread::spawn(move || {
                    let mut w = SectionWriter::new(1);
                    for i in 0..STEPS {
                        if shard == 0 && i == STEPS / 3 {
                            log.reject_direct_writes();
                        }
                        w.append(&step(u64::from(shard), i, 40 + (i as usize % 300)))
                            .unwrap();
                        let b = w.take().unwrap();
                        let last = b.last_lsn;
                        assert!(log.write_now(shard, b).unwrap() >= last);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert!(
            !log.direct_io(),
            "the log should have moved to buffered writes"
        );
    }
    let log = SharedWal::open(&dir, SHARDS, local, opts()).unwrap();
    for shard in 0..SHARDS {
        let (recs, _) = log.take_recovered(shard, 0).unwrap();
        assert_eq!(recs.len() as u32, STEPS, "shard {shard}");
    }
    drop(log);
    let _ = std::fs::remove_dir_all(&dir);
}

/// More than one batch holds, staged before a write (large records, or a
/// slow flush with many shards busy): the batches written stay within
/// [`MAX_BATCH`], which recovery enforces, and every acknowledged record
/// is recovered. (Sealed as one batch, the write succeeded, the records
/// were acknowledged, and the next open dropped them as a torn tail.)
#[test]
fn more_than_a_batch_staged_is_written_as_several_and_recovered() {
    let dev = MemDevice::default();
    let big = WalOptions {
        segment_bytes: 256 << 20,
        first_segment_bytes: 1 << 20,
        ..opts()
    };
    let mut wal = Wal::open(dev.clone(), 2, big, 1, 0).unwrap();
    let mut w0 = SectionWriter::new(1);
    let mut w1 = SectionWriter::new(1);
    // 90 MiB for shard 0 and a small section for shard 1, in one write.
    stage_records(&mut wal, &mut w0, 0, 3, 30 << 20);
    stage_records(&mut wal, &mut w1, 1, 5, 100);
    let before = wal.writes;
    let wrote = wal.write(0).unwrap();
    assert_eq!(wrote.len(), 2);
    assert_eq!(wal.writes - before, 2, "two batches");
    assert_eq!((wal.durable(0), wal.durable(1)), (3, 5));
    drop(wal);
    let mut wal = Wal::open(dev, 2, big, 2, 0).unwrap();
    let (records, last) = wal.take_recovered(0, 0).unwrap();
    assert_eq!((records.len(), last), (3, 3));
    let (records, last) = wal.take_recovered(1, 0).unwrap();
    assert_eq!((records.len(), last), (5, 5));
}

/// Sections cut at record boundaries across many batches and shards: with
/// a small batch limit, every shard's records come back in order after
/// each life, no shard waits behind another's long section forever, and a
/// record longer than a batch is an error, not a write.
#[test]
fn cut_sections_keep_every_shard_in_order_across_lives() {
    const LIMIT: usize = 2048;
    let dev = MemDevice::default();
    let shards = 4u32;
    let mut writers: Vec<SectionWriter> = (0..shards).map(|_| SectionWriter::new(1)).collect();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    for life in 0..5u64 {
        let mut wal = Wal::open(dev.clone(), shards, opts(), life + 1, 0).unwrap();
        wal.limit_batches(LIMIT);
        for s in 0..shards {
            let (records, last) = wal.take_recovered(s, 0).unwrap();
            assert_eq!(
                last,
                writers[s as usize].last_lsn(),
                "life {life} shard {s}"
            );
            assert_eq!(records.len() as u64, last);
            for (i, (lsn, _)) in records.iter().enumerate() {
                assert_eq!(*lsn, i as u64 + 1);
            }
        }
        for _ in 0..60 {
            for s in 0..shards {
                rng = xorshift(rng);
                // Shard 0 always stages several batches' worth; the others
                // together less than one.
                let (n, size) = if s == 0 {
                    (40, 20 + (rng >> 8) as usize % 300)
                } else {
                    ((rng % 4) as u32, 20 + (rng >> 8) as usize % 130)
                };
                stage_records(&mut wal, &mut writers[s as usize], s, n, size);
            }
            let before = wal.bytes_written;
            let batches = wal.writes;
            // One batch at a time: none is longer than the limit, and the
            // other shards' sections are not left behind shard 0's.
            let mut rounds = 0;
            while wal.has_staged() {
                let w = wal.begin_write(0, false).unwrap().unwrap();
                wal.finish_write(w).unwrap();
                rounds += 1;
                if rounds == 2 {
                    for s in 1..shards {
                        assert_eq!(wal.durable(s), writers[s as usize].last_lsn(), "shard {s}");
                    }
                }
            }
            let written = wal.bytes_written - before;
            assert!(written <= (wal.writes - batches) * (LIMIT as u64 + 8));
            for s in 0..shards {
                assert_eq!(wal.durable(s), writers[s as usize].last_lsn());
            }
        }
        if life == 4 {
            let mut w = SectionWriter::new(writers[1].next_lsn());
            w.append(&step(1, 0, LIMIT)).unwrap();
            wal.stage(1, w.take().unwrap()).unwrap();
            assert!(wal.write(0).is_err(), "a record longer than a batch");
        }
    }
}

/// The batches of the chain's segments, as recovery would accept them.
fn chain_batches(dev: &MemDevice, wal: &Wal<MemDevice>) -> u64 {
    let mut batches = 0;
    for number in wal.files().0 {
        let bytes = dev.view(number).unwrap();
        let mut scanner = WalScanner::start(&bytes, true).unwrap().unwrap();
        match scanner.advance(&bytes, true, bytes.len(), &mut |_| {}) {
            Progress::Done(s) => batches += s.batches,
            Progress::NeedMore => panic!("a whole segment was read"),
        }
    }
    batches
}

#[test]
fn a_log_nothing_needs_is_left_short_and_goes_on_from_there() {
    let dev = MemDevice::default();
    let mut wal = Wal::open(dev.clone(), 2, opts(), 1, 0).unwrap();
    let mut w: Vec<SectionWriter> = (0..2).map(|_| SectionWriter::new(1)).collect();
    for _ in 0..300 {
        for s in 0..2u32 {
            stage_records(&mut wal, &mut w[s as usize], s, 2, 90);
        }
        wal.write(0).unwrap();
    }
    assert!(wal.files().0.len() > 2);
    // A shard still needs its records: nothing moves.
    wal.set_retain(0, w[0].last_lsn()).unwrap();
    assert!(!wal.shorten(0).unwrap());
    wal.set_retain(1, w[1].last_lsn() - 1).unwrap();
    assert!(!wal.shorten(0).unwrap());
    // Nor while a section waits to be written.
    wal.set_retain(1, w[1].last_lsn()).unwrap();
    stage_records(&mut wal, &mut w[0], 0, 1, 90);
    assert!(!wal.shorten(0).unwrap());
    wal.write(0).unwrap();
    assert!(!wal.shorten(0).unwrap(), "shard 0 has a record since");
    wal.set_retain(0, w[0].last_lsn()).unwrap();
    let position = wal.position();
    let files = dev.segments().unwrap().len();
    assert!(wal.shorten(0).unwrap());
    // One segment, holding no batch; no file was added for it.
    assert_eq!(wal.files().0.len(), 1);
    assert_eq!(chain_batches(&dev, &wal), 0);
    assert_eq!(wal.position(), position);
    assert!(dev.segments().unwrap().len() <= files);
    // Already as short as it gets.
    assert!(!wal.shorten(0).unwrap());
    let last = [w[0].last_lsn(), w[1].last_lsn()];
    drop(wal);

    // The next life finds the chain's place and nothing to replay.
    let mut wal = Wal::open(dev.clone(), 2, opts(), 2, 0).unwrap();
    assert_eq!(wal.position(), position);
    assert_eq!(wal.files().0.len(), 1);
    for s in 0..2u32 {
        let (records, end) = wal.take_recovered(s, last[s as usize]).unwrap();
        assert!(records.is_empty());
        assert_eq!(end, last[s as usize]);
        wal.set_retain(s, end).unwrap();
    }
    // It writes on, rolls, and a third life reads exactly what it wrote.
    let mut w: Vec<SectionWriter> = last.iter().map(|l| SectionWriter::new(l + 1)).collect();
    for _ in 0..300 {
        for s in 0..2u32 {
            stage_records(&mut wal, &mut w[s as usize], s, 2, 90);
        }
        wal.write(0).unwrap();
    }
    assert!(wal.files().0.len() > 1);
    drop(wal);
    let mut wal = Wal::open(dev, 2, opts(), 3, 0).unwrap();
    for s in 0..2u32 {
        let (records, end) = wal.take_recovered(s, last[s as usize]).unwrap();
        assert_eq!(end, w[s as usize].last_lsn());
        assert_eq!(records.len() as u64, end - last[s as usize]);
        assert_eq!(records.first().map(|r| r.0), Some(last[s as usize] + 1));
    }
}

/// Crash campaigns with clean closes that shorten the log, the process
/// dying before, between and after the shortening's two durable steps:
/// every record a shard was told is durable and its checkpoints do not
/// cover is recovered, in order, in the next life.
#[test]
fn shortening_the_log_at_a_close_survives_a_crash_at_every_step() {
    let mut shortened = 0;
    let mut kept = 0;
    let mut died = [0u32; 2];
    for seed in 0..600u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut dev = MemDevice::default();
        let shards = 3u32;
        // What each shard was told is durable, and what its checkpoints
        // cover (both of them: a clean close leaves two of its end).
        let mut acked: Vec<Lsn> = vec![0; shards as usize];
        let mut covered: Vec<Lsn> = vec![0; shards as usize];
        // The chain's position after the last acknowledged batch: a later
        // life never starts before it.
        let mut position = 0;
        for life in 0..8u64 {
            let mut wal = Wal::open(dev.clone(), shards, opts(), life + 1, 0)
                .unwrap_or_else(|e| panic!("seed {seed} life {life}: {e}"));
            assert!(
                wal.position() >= position,
                "seed {seed} life {life}: the chain went back from {position} to {}",
                wal.position()
            );
            // A batch written as the last life died may have landed.
            position = wal.position();
            let mut writers = Vec::new();
            for s in 0..shards {
                let after = covered[s as usize];
                let (records, last) = wal
                    .take_recovered(s, after)
                    .unwrap_or_else(|e| panic!("seed {seed} life {life}: {e}"));
                assert!(
                    last >= acked[s as usize],
                    "seed {seed} life {life}: shard {s} lost acknowledged {} (has {last})",
                    acked[s as usize]
                );
                assert_eq!(
                    records.len() as u64,
                    last - after,
                    "seed {seed} life {life}"
                );
                for (i, (lsn, _)) in records.iter().enumerate() {
                    assert_eq!(
                        *lsn,
                        after + 1 + i as u64,
                        "seed {seed}: LSNs not consecutive"
                    );
                }
                wal.set_retain(s, after).unwrap();
                acked[s as usize] = last;
                writers.push(SectionWriter::new(last + 1));
            }
            rng = xorshift(rng);
            let rounds = rng % 40;
            let mut dead = false;
            for _ in 0..rounds {
                for s in 0..shards {
                    rng = xorshift(rng);
                    if rng % 2 == 0 {
                        stage_records(
                            &mut wal,
                            &mut writers[s as usize],
                            s,
                            1 + (rng % 3) as u32,
                            (rng % 300) as usize,
                        );
                    }
                }
                rng = xorshift(rng);
                if rng % 9 == 0 && wal.has_staged() {
                    dev.0.borrow_mut().die_in_sync = true;
                    assert!(wal.write(0).is_err());
                    dead = true;
                    break;
                }
                for s in wal.write(0).unwrap() {
                    acked[s as usize] = wal.durable(s);
                }
                position = wal.position();
                // A shard checkpoints now and then.
                rng = xorshift(rng);
                if rng % 7 == 0 {
                    let s = (rng >> 8) as usize % shards as usize;
                    covered[s] = acked[s];
                    wal.set_retain(s as u32, covered[s]).unwrap();
                }
            }
            rng = xorshift(rng);
            if !dead && rng % 3 != 0 {
                // A clean close: every shard's checkpoints cover its end,
                // or (one time in five) one shard's do not.
                rng = xorshift(rng);
                let behind = (rng % 5 == 0).then_some((rng >> 8) as usize % shards as usize);
                for s in 0..shards as usize {
                    if behind != Some(s) {
                        covered[s] = acked[s];
                        wal.set_retain(s as u32, covered[s]).unwrap();
                    }
                }
                let held = behind.is_some_and(|s| covered[s] < acked[s]);
                rng = xorshift(rng);
                let step = rng % 4;
                if step < 2 {
                    dev.0.borrow_mut().syncs_left = Some(step as u32);
                }
                match wal.shorten(0) {
                    Ok(true) => {
                        assert!(!held, "seed {seed} life {life}: a shard needed its records");
                        assert!(step >= 2, "seed {seed}: no sync died");
                        assert_eq!(wal.files().0.len(), 1, "seed {seed} life {life}");
                        assert_eq!(chain_batches(&dev, &wal), 0, "seed {seed} life {life}");
                        assert_eq!(wal.position(), position, "seed {seed} life {life}");
                        shortened += 1;
                    }
                    // A shard holds records, or nothing was written since
                    // the last shortening.
                    Ok(false) => kept += u32::from(held),
                    Err(_) => {
                        assert!(step < 2, "seed {seed} life {life}: shorten failed");
                        died[step as usize] += 1;
                    }
                }
            }
            drop(wal);
            dev = dev.crash(&mut rng);
            let mut f = dev.0.borrow_mut();
            f.die_in_sync = false;
            f.syncs_left = None;
        }
    }
    // The campaign reached each case.
    assert!(
        shortened > 200 && kept > 100 && died[0] > 100 && died[1] > 100,
        "{shortened} {kept} {died:?}"
    );
}
