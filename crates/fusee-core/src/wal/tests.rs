use super::*;
use crate::record::{Record, RecordRef, Status};
use rustc_hash::FxHashMap;

#[test]
fn xxh64_matches_reference_vectors() {
    assert_eq!(xxh64(b"", 0), 0xEF46_DB37_51D8_E999);
    assert_eq!(xxh64(b"a", 0), 0xD24E_C4F1_A98C_6E5B);
    assert_eq!(xxh64(b"abc", 0), 0x44BC_2CF5_AD77_0999);
    assert_eq!(
        xxh64(b"Nobody inspects the spammish repetition", 0),
        0xFBCE_A83C_8A37_8BF1
    );
}

fn step(run: u64, seq: u32, payload: &[u8]) -> RecordRef<'_> {
    Record::Step {
        run,
        seq,
        name: "s",
        attempt: 1,
        status: Status::Ok,
        payload,
        time: None,
    }
}

/// Records for shard `shard` starting at `first`: `n` small steps.
fn records(first: u64, n: u32, size: usize) -> Vec<u8> {
    let payload = vec![b'x'; size];
    let mut out = Vec::new();
    for i in 0..n {
        encode_canonical(&step(first, i, &payload), &mut out);
    }
    out
}

/// A segment image on a simulated disk.
#[derive(Clone)]
struct Disk {
    segments: FxHashMap<u64, Vec<u8>>,
    size: usize,
}

impl Disk {
    fn new(size: usize) -> Self {
        Self {
            segments: FxHashMap::default(),
            size,
        }
    }

    fn write(&mut self, segment: u64, offset: u64, bytes: &[u8]) {
        let img = self
            .segments
            .entry(segment)
            .or_insert_with(|| vec![0; self.size]);
        img[offset as usize..offset as usize + bytes.len()].copy_from_slice(bytes);
    }

    fn apply(&mut self, p: &Placed) {
        if let Some(h) = &p.header {
            self.write(p.segment, 0, &h.encode());
        }
        self.write(p.segment, p.offset, &p.bytes);
    }

    /// Recovery: follows the chain through the segments in order. Returns
    /// the accepted batches (seq, life, sections) and the final scan.
    fn recover(&self) -> (Vec<(u64, u64, Vec<(u32, Lsn, u32)>)>, Option<Scanned>) {
        let mut numbers: Vec<u64> = self.segments.keys().copied().collect();
        numbers.sort_unstable();
        let mut out = Vec::new();
        let mut last = None;
        let mut expect: Option<(u64, u64)> = None;
        for n in numbers {
            let img = &self.segments[&n];
            let Some(mut s) = WalScanner::start(img, true).unwrap() else {
                continue;
            };
            // The chain continues into a segment whose header continues it.
            if let Some((seq, checksum)) = expect
                && (s.header().base_seq != seq + 1 || s.header().base_prev != checksum)
            {
                break;
            }
            let Progress::Done(scanned) = s.advance(img, true, img.len(), &mut |b| {
                out.push((
                    b.header.seq,
                    b.header.life,
                    b.sections()
                        .map(|s| {
                            section_records(&s).unwrap();
                            (s.shard, s.first_lsn, s.count)
                        })
                        .collect(),
                ));
            }) else {
                unreachable!()
            };
            expect = Some((scanned.last_seq, scanned.last_checksum));
            last = Some(scanned);
            if matches!(scanned.tail, End::Damaged { .. }) {
                break;
            }
        }
        (out, last)
    }
}

fn first_header(segment: u64, salt: u64) -> WalHeader {
    WalHeader {
        group: 0,
        segment,
        salt,
        base_seq: 1,
        base_prev: 0,
        created_ms: 0,
    }
}

#[test]
fn batches_round_trip_with_sections_and_roll_segments() {
    let size = 64 * 1024;
    let mut disk = Disk::new(size);
    disk.write(1, 0, &first_header(1, 11).encode());
    let mut w = WalWriter::resume(0, 7, 1, 11, size as u64, WAL_HEADER_BYTES, 1, 0);
    let mut next = 2u64;
    let mut lsn = [1u64; 3];
    for i in 0..200u32 {
        let recs: Vec<Vec<u8>> = (0..3).map(|s| records(lsn[s], 1 + i % 3, 40)).collect();
        let sections: Vec<SectionRef<'_>> = (0..3)
            .map(|s| SectionRef {
                shard: s as u32,
                first_lsn: lsn[s],
                count: 1 + i % 3,
                records: &recs[s],
            })
            .collect();
        let p = w.place(&sections, u64::from(i), i % 2 == 0, 0, || {
            next += 1;
            (next - 1, next * 1000, size as u64)
        });
        disk.apply(&p);
        for l in &mut lsn {
            *l += u64::from(1 + i % 3);
        }
    }
    assert!(disk.segments.len() > 2);
    let (batches, last) = disk.recover();
    assert_eq!(batches.len(), 200);
    assert_eq!(last.unwrap().tail, End::Clean);
    // Each shard's LSNs are consecutive across batches and segments.
    let mut expect = [1u64; 3];
    for (seq, life, sections) in &batches {
        assert_eq!(*life, 7);
        for (shard, first, count) in sections {
            assert_eq!(*first, expect[*shard as usize], "batch {seq}");
            expect[*shard as usize] += u64::from(*count);
        }
    }
}

fn single(lsn: u64, size: usize) -> Vec<u8> {
    records(lsn, 1, size)
}

fn place_one(w: &mut WalWriter, recs: &[u8], lsn: u64, acked: u64, pipelined: bool) -> Placed {
    w.place(
        &[SectionRef {
            shard: 0,
            first_lsn: lsn,
            count: 1,
            records: recs,
        }],
        acked,
        pipelined,
        0,
        || panic!("one segment is enough"),
    )
}

/// Writes in flight together must not share a block (`O_DIRECT` rewrites
/// whole blocks). A pipelined batch that starts inside a block, after a
/// packed one or after a resume, still ends on a block boundary.
#[test]
fn a_pipelined_batch_ends_on_a_block_boundary_wherever_it_starts() {
    let size = 1 << 20;
    let mut disk = Disk::new(size);
    disk.write(1, 0, &first_header(1, 3).encode());
    let mut w = WalWriter::resume(0, 1, 1, 3, size as u64, WAL_HEADER_BYTES, 1, 0);
    let shapes = [
        (false, 100),
        (true, 100),
        (true, 5000),
        (false, 30),
        (false, 4000),
        (true, 1),
        (true, 4096),
        (false, 7),
    ];
    for (i, (pipelined, len)) in shapes.into_iter().enumerate() {
        let lsn = i as u64 + 1;
        let p = place_one(&mut w, &single(lsn, len), lsn, i as u64, pipelined);
        let end = p.offset + p.bytes.len() as u64;
        assert!(end.is_multiple_of(8));
        if pipelined {
            assert!(end.is_multiple_of(BLOCK), "batch {lsn} ends at {end}");
            assert!(p.bytes.len() as u64 <= batch_body_len_of(&p) as u64 + BLOCK);
        }
        disk.apply(&p);
    }
    let (batches, last) = disk.recover();
    assert_eq!(batches.len(), shapes.len());
    assert_eq!(last.unwrap().tail, End::Clean);
    // Too large to pad to a block's end: keeps the 8-byte rounding.
    assert_eq!(batch_span(16, MAX_BATCH - 8, true), (MAX_BATCH - 8) as u64);
    assert_eq!(batch_span(4096, 100, true), 4096);
    assert_eq!(batch_span(4000, 100, true), 4192);
    assert_eq!(batch_span(4000, 100, false), 104);
}

/// The encoded body length a placed batch records (bytes 8..12).
fn batch_body_len_of(p: &Placed) -> usize {
    u32::from_le_bytes(p.bytes[8..12].try_into().unwrap()) as usize
}

#[test]
fn a_torn_write_in_flight_ends_the_chain_and_proves_nothing() {
    let size = 64 * 1024;
    let mut disk = Disk::new(size);
    disk.write(1, 0, &first_header(1, 5).encode());
    let mut w = WalWriter::resume(0, 1, 1, 5, size as u64, WAL_HEADER_BYTES, 1, 0);
    let mut placed = Vec::new();
    for i in 0..6u64 {
        // Batches 5 and 6 are in flight together; 1..=4 were acknowledged.
        let acked = i.min(4);
        placed.push(place_one(&mut w, &single(i + 1, 100), i + 1, acked, true));
    }
    for p in &placed[..4] {
        disk.apply(p);
    }
    // Batch 6 landed before batch 5, and 5 tore.
    disk.apply(&placed[5]);
    let mut torn = placed[4].clone();
    for b in &mut torn.bytes[60..] {
        *b ^= 0x55;
    }
    disk.apply(&torn);
    let (batches, last) = disk.recover();
    assert_eq!(batches.len(), 4);
    assert_eq!(last.unwrap().tail, End::Torn);
}

#[test]
fn a_lost_acknowledged_batch_is_reported_as_damage() {
    let size = 64 * 1024;
    let mut disk = Disk::new(size);
    disk.write(1, 0, &first_header(1, 5).encode());
    let mut w = WalWriter::resume(0, 1, 1, 5, size as u64, WAL_HEADER_BYTES, 1, 0);
    for i in 0..8u64 {
        // Written one at a time: each batch says the previous was acked.
        let p = place_one(&mut w, &single(i + 1, 100), i + 1, i, false);
        disk.apply(&p);
    }
    // Media damage inside batch 3, which batch 4 says was acknowledged.
    let img = disk.segments.get_mut(&1).unwrap();
    let mut s = WalScanner::start(img, true).unwrap().unwrap();
    let mut offsets = Vec::new();
    let mut pos = WAL_HEADER_BYTES as usize;
    let _ = s.advance(img, true, size, &mut |b| {
        offsets.push(pos);
        pos += b.len;
    });
    img[offsets[2] + BATCH_HEADER + 3] ^= 1;
    let (batches, last) = disk.recover();
    assert_eq!(batches.len(), 2);
    assert!(
        matches!(last.unwrap().tail, End::Damaged { later_seq, .. } if later_seq >= 4),
        "{:?}",
        last.unwrap().tail
    );
}

#[test]
fn resuming_in_place_never_accepts_a_stale_batch_of_an_earlier_life() {
    let size = 64 * 1024;
    let mut disk = Disk::new(size);
    disk.write(1, 0, &first_header(1, 9).encode());
    // Life 1 writes batches 1..=5 of identical size; 4 tears, 5 lands.
    let mut w = WalWriter::resume(0, 100, 1, 9, size as u64, WAL_HEADER_BYTES, 1, 0);
    let mut placed = Vec::new();
    for i in 0..5u64 {
        placed.push(place_one(
            &mut w,
            &single(i + 1, 64),
            i + 1,
            i.min(3),
            false,
        ));
    }
    for p in &placed[..3] {
        disk.apply(p);
    }
    disk.apply(&placed[4]);
    let mut torn = placed[3].clone();
    torn.bytes[70] ^= 1;
    disk.apply(&torn);
    let (batches, last) = disk.recover();
    assert_eq!(batches.len(), 3);
    let last = last.unwrap();
    // Life 2 resumes at the block after the chain and writes the same shape
    // of batches, so stale batch 5 can sit right where 5 is expected.
    let mut w = WalWriter::resume(
        0,
        200,
        1,
        9,
        size as u64,
        last.end as u64,
        last.last_seq + 1,
        last.last_checksum,
    );
    assert_eq!(w.offset(), last.end as u64);
    let p4 = place_one(&mut w, &single(4, 64), 4, 3, false);
    // Put stale batch 5 of life 1 exactly after life 2's batch 4.
    let stale5 = {
        let mut s = placed[4].clone();
        s.offset = p4.offset + p4.bytes.len() as u64;
        s
    };
    disk.apply(&p4);
    disk.apply(&stale5);
    let (batches, _) = disk.recover();
    assert_eq!(
        batches.iter().map(|b| (b.0, b.1)).collect::<Vec<_>>(),
        vec![(1, 100), (2, 100), (3, 100), (4, 200)],
        "the stale batch of life 1 must not follow life 2's batch"
    );
}

#[test]
fn a_recycled_segment_ignores_bytes_of_its_previous_life() {
    let size = 64 * 1024;
    let mut disk = Disk::new(size);
    disk.write(1, 0, &first_header(1, 1).encode());
    let mut w = WalWriter::resume(0, 1, 1, 1, size as u64, WAL_HEADER_BYTES, 1, 0);
    for i in 0..20u64 {
        disk.apply(&place_one(&mut w, &single(i + 1, 200), i + 1, i, false));
    }
    // Recycle: the same file becomes segment 9 with a new salt and base.
    let mut img = disk.segments.remove(&1).unwrap();
    let header = WalHeader {
        group: 0,
        segment: 9,
        salt: 2,
        base_seq: 21,
        base_prev: 77,
        created_ms: 0,
    };
    img[..WAL_HEADER_BYTES as usize].copy_from_slice(&header.encode());
    disk.segments.insert(9, img);
    let mut w = WalWriter::resume(0, 2, 9, 2, size as u64, WAL_HEADER_BYTES, 21, 77);
    for i in 0..3u64 {
        disk.apply(&place_one(
            &mut w,
            &single(i + 21, 50),
            i + 21,
            i + 20,
            false,
        ));
    }
    let img = &disk.segments[&9];
    let mut s = WalScanner::start(img, true).unwrap().unwrap();
    let mut seqs = Vec::new();
    let Progress::Done(scanned) = s.advance(img, true, size, &mut |b| seqs.push(b.header.seq))
    else {
        unreachable!()
    };
    assert_eq!(seqs, vec![21, 22, 23]);
    // Old batches follow, but none passes the new salt, and none proves damage.
    assert_eq!(scanned.tail, End::Torn);
}

#[test]
fn prefix_scans_match_whole_image_scans() {
    let size = 256 * 1024;
    let mut img_disk = Disk::new(size);
    img_disk.write(1, 0, &first_header(1, 3).encode());
    let mut w = WalWriter::resume(0, 1, 1, 3, size as u64, WAL_HEADER_BYTES, 1, 0);
    let mut offs = Vec::new();
    for i in 0..60u64 {
        let p = place_one(
            &mut w,
            &single(i + 1, 300 + (i as usize % 7) * 900),
            i + 1,
            i,
            i % 3 == 0,
        );
        offs.push(p.offset as usize);
        img_disk.apply(&p);
    }
    let clean = img_disk.segments[&1].clone();
    let mut variants = vec![clean.clone()];
    let mut torn = clean.clone();
    torn[offs[59] + 70] ^= 1;
    variants.push(torn);
    let mut damaged = clean.clone();
    damaged[offs[10] + 70] ^= 1;
    variants.push(damaged);
    for img in &variants {
        let whole = {
            let mut s = WalScanner::start(img, true).unwrap().unwrap();
            let mut seqs = Vec::new();
            match s.advance(img, true, size, &mut |b| seqs.push(b.header.seq)) {
                Progress::Done(d) => (d, seqs),
                Progress::NeedMore => unreachable!(),
            }
        };
        for first in [4096usize, 6000, 65536] {
            let mut len = first;
            let mut s = WalScanner::start(&img[..len], false).unwrap().unwrap();
            let mut seqs = Vec::new();
            let done = loop {
                let complete = len == img.len();
                match s.advance(&img[..len], complete, size, &mut |b| {
                    seqs.push(b.header.seq)
                }) {
                    Progress::Done(d) => break d,
                    Progress::NeedMore => len = (len * 2).min(img.len()),
                }
            };
            assert_eq!((done, seqs), whole.clone(), "first read {first}");
        }
    }
}

/// A small deterministic generator (no dependency).
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The model of spec/FuseeLog.tla on real bytes: lives write batches with up
/// to two writes in flight, completions arrive in any order, crashes keep,
/// lose or tear each write in flight, and recovery resumes in place.
#[test]
fn crashes_with_two_writes_in_flight_never_lose_or_splice() {
    for seed in 0..3000u64 {
        let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let size = 32 * 1024;
        let mut disk = Disk::new(size);
        disk.write(1, 0, &first_header(1, 42).encode());
        let mut acked_ids: Vec<(u64, u64)> = Vec::new(); // (seq, life)
        let mut resume = (WAL_HEADER_BYTES, 1u64, 0u64);
        for life in 1..=4u64 {
            let mut w =
                WalWriter::resume(0, life, 1, 42, size as u64, resume.0, resume.1, resume.2);
            let base = resume.1;
            let mut inflight: Vec<Placed> = Vec::new();
            let mut done: FxHashMap<u64, ()> = FxHashMap::default();
            let mut acked = base - 1;
            let steps = rng.below(12);
            for _ in 0..steps {
                let room = w.offset() + 4 * BLOCK < size as u64;
                if room && inflight.len() < 2 && rng.below(2) == 0 {
                    let seq = w.next_seq();
                    let recs = single(seq, 20 + rng.below(200) as usize);
                    inflight.push(place_one(&mut w, &recs, seq, acked, true));
                } else if !inflight.is_empty() {
                    let i = rng.below(inflight.len() as u64) as usize;
                    let p = inflight.remove(i);
                    disk.apply(&p);
                    done.insert(p.seq, ());
                    while done.contains_key(&(acked + 1)) {
                        acked += 1;
                        acked_ids.push((acked, life));
                    }
                }
            }
            // Crash: each write in flight lands, is lost or tears.
            for p in &inflight {
                match rng.below(3) {
                    0 => disk.apply(p),
                    1 => {}
                    _ => {
                        let mut t = p.clone();
                        let cut = 1 + rng.below(t.bytes.len() as u64 - 1) as usize;
                        for b in &mut t.bytes[cut..] {
                            *b = rng.next() as u8;
                        }
                        disk.apply(&t);
                    }
                }
            }
            let (batches, last) = disk.recover();
            let last = last.unwrap();
            assert!(
                !matches!(last.tail, End::Damaged { .. }),
                "seed {seed}: a crash looked like damage"
            );
            for (seq, l) in &acked_ids {
                assert!(
                    batches.iter().any(|b| b.0 == *seq && b.1 == *l),
                    "seed {seed}: acknowledged batch {seq} of life {l} was lost"
                );
            }
            for pair in batches.windows(2) {
                assert!(
                    pair[1].1 >= pair[0].1,
                    "seed {seed}: a batch of life {} follows one of life {}",
                    pair[1].1,
                    pair[0].1
                );
            }
            // Everything accepted is durable now.
            acked_ids = batches.iter().map(|b| (b.0, b.1)).collect();
            resume = (last.end as u64, last.last_seq + 1, last.last_checksum);
        }
    }
}

/// A capped take hands out the oldest records that fit, in order and whole,
/// one at least; appends in between land after what is pending; and the
/// sections together are exactly what one uncapped take would have been.
#[test]
fn capped_takes_drain_a_backlog_in_order() {
    let mut rng = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for max in [1usize, 64, 300, 4096] {
        let mut capped = SectionWriter::new(1);
        let mut whole = SectionWriter::new(1);
        let mut taken: Vec<u8> = Vec::new();
        let mut next_lsn = 1u64;
        let mut seq = 0u32;
        for round in 0..200 {
            for _ in 0..next() % 8 {
                let payload = vec![b'p'; (next() % 700) as usize];
                let rec = step(1, seq, &payload);
                seq += 1;
                let lsn = capped.append(&rec).unwrap();
                assert_eq!(whole.append(&rec).unwrap(), lsn);
                // The last record's encoding is readable right after it.
                let mut alone = Vec::new();
                encode_canonical(&rec, &mut alone);
                assert_eq!(capped.last_encoded(), &alone[..]);
            }
            // Some rounds drain everything, most take one section.
            let takes = if round % 17 == 16 { usize::MAX } else { 1 };
            for _ in 0..takes {
                let Some(b) = capped.take_up_to(max) else {
                    break;
                };
                assert_eq!(b.first_lsn, next_lsn);
                assert_eq!(b.last_lsn, next_lsn + u64::from(b.records) - 1);
                assert!(b.records >= 1);
                assert!(b.records == 1 || b.section.len() <= max, "{max}");
                let s = SectionRef {
                    shard: 0,
                    first_lsn: b.first_lsn,
                    count: b.records,
                    records: &b.section,
                };
                assert_eq!(section_records(&s).unwrap().len() as u32, b.records);
                next_lsn = b.last_lsn + 1;
                taken.extend_from_slice(&b.section);
            }
        }
        while let Some(b) = capped.take_up_to(max) {
            assert_eq!(b.first_lsn, next_lsn);
            next_lsn = b.last_lsn + 1;
            taken.extend_from_slice(&b.section);
        }
        assert!(!capped.has_pending());
        assert_eq!(capped.last_lsn(), whole.last_lsn());
        let all = whole.take().unwrap();
        assert_eq!(next_lsn, all.last_lsn + 1);
        assert_eq!(taken, all.section, "max {max}");
        // An uncapped take after capped ones returns the rest, and only it.
        let rec = step(1, seq, b"tail");
        capped.append(&rec).unwrap();
        capped.append(&rec).unwrap();
        capped.append(&rec).unwrap();
        let head = capped.take_up_to(1).unwrap();
        let rest = capped.take().unwrap();
        assert_eq!((head.records, rest.records), (1, 2));
        assert_eq!(rest.first_lsn, head.last_lsn + 1);
        let mut twice = Vec::new();
        encode_canonical(&rec, &mut twice);
        encode_canonical(&rec, &mut twice);
        assert_eq!(rest.section, twice);
    }
}
