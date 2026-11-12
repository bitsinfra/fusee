//! Deterministic tests of the server log: leases, fencing, read-back and
//! retention over an in-memory device whose unsynced writes a crash keeps,
//! drops or tears.

use super::*;
use fusee::wal::records_after;
use fusee_core::record::{Record, RecordBuf, Status};
use fusee_core::wal::{BLOCK, SectionWriter};
use std::collections::BTreeMap;

fn xorshift(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[derive(Default)]
struct Files {
    durable: BTreeMap<u64, Vec<u8>>,
    pending: Vec<(u64, u64, Vec<u8>)>,
}

/// A disk in memory, shareable between threads. Writes stay pending until
/// their file is synced; [`crash`](Self::crash) keeps, drops or tears each.
#[derive(Clone, Default)]
struct MemDevice(Arc<Mutex<Files>>);

impl MemDevice {
    fn files(&self) -> std::sync::MutexGuard<'_, Files> {
        self.0.lock().unwrap()
    }

    fn view(&self, segment: u64) -> Option<Vec<u8>> {
        let f = self.files();
        let mut img = f.durable.get(&segment)?.clone();
        for (s, off, bytes) in &f.pending {
            if *s == segment {
                img[*off as usize..*off as usize + bytes.len()].copy_from_slice(bytes);
            }
        }
        Some(img)
    }

    /// The server dies: each pending write lands, is lost, or tears.
    fn crash(&self, rng: &mut u64) {
        let mut f = self.files();
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
    }
}

impl LogDevice for MemDevice {
    fn segments(&self) -> io::Result<Vec<u64>> {
        Ok(self.files().durable.keys().copied().collect())
    }
    fn len(&self, segment: u64) -> io::Result<u64> {
        Ok(self.view(segment).ok_or_else(|| other("missing"))?.len() as u64)
    }
    fn read(&self, segment: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let img = self.view(segment).ok_or_else(|| other("missing"))?;
        let start = (offset as usize).min(img.len());
        let end = start.saturating_add(len).min(img.len());
        Ok(img[start..end].to_vec())
    }
    fn create(&self, segment: u64, len: u64) -> io::Result<()> {
        self.files().durable.insert(segment, vec![0; len as usize]);
        Ok(())
    }
    fn write(&self, segment: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut f = self.files();
        let size = f.durable.get(&segment).map_or(0, Vec::len);
        if offset as usize + bytes.len() > size {
            return Err(other("write past the end of a segment"));
        }
        // Writes in flight together never share a block: under O_DIRECT
        // each rewrites its blocks whole.
        let blocks = |off: u64, len: usize| (off / BLOCK, (off + len.max(1) as u64 - 1) / BLOCK);
        let (first, last) = blocks(offset, bytes.len());
        if f.pending.iter().any(|(s, o, b)| {
            let (a, z) = blocks(*o, b.len());
            *s == segment && a <= last && z >= first
        }) {
            return Err(other("a write shares a block with one in flight"));
        }
        f.pending.push((segment, offset, bytes.to_vec()));
        Ok(())
    }
    fn sync(&self, segment: u64) -> io::Result<()> {
        let mut f = self.files();
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
        let mut f = self.files();
        for p in &mut f.pending {
            if p.0 == from {
                p.0 = to;
            }
        }
        let img = f.durable.remove(&from).ok_or_else(|| other("missing"))?;
        f.durable.insert(to, img);
        Ok(())
    }
    fn remove(&self, segment: u64) -> io::Result<()> {
        let mut f = self.files();
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

/// A record whose payload names its shard, LSN and the lease that wrote
/// it, so recovery can tell any mix-up apart.
fn record(shard: u32, lsn: Lsn, epoch: u64, size: usize) -> Record<&'static str, Vec<u8>> {
    let mut payload = format!("{shard}/{lsn}/{epoch}/").into_bytes();
    payload.resize(payload.len() + size, b'p');
    Record::Step {
        run: u64::from(shard) + 1,
        seq: lsn as u32,
        name: "s",
        attempt: 1,
        status: Status::Ok,
        payload,
        time: None,
    }
}

fn payload(r: &RecordBuf) -> Vec<u8> {
    match r {
        Record::Step { payload, .. } => payload.to_vec(),
        other => panic!("unexpected record {other:?}"),
    }
}

/// `n` records of `shard` from `next` as one section.
fn section(shard: u32, next: Lsn, n: u32, epoch: u64, size: usize) -> (Batch, Vec<Vec<u8>>) {
    let mut w = SectionWriter::new(next);
    let mut payloads = Vec::new();
    for i in 0..u64::from(n) {
        let rec = record(shard, next + i, epoch, size);
        if let Record::Step { payload, .. } = &rec {
            payloads.push(payload.clone());
        }
        w.append(&rec).unwrap();
    }
    (w.take().unwrap(), payloads)
}

/// Recovers `shard` for the lease `epoch` as the service does: waiting is
/// not possible here, so the fence must be durable.
fn recover(
    st: &mut ServerWal<MemDevice>,
    dev: &MemDevice,
    shard: u32,
    epoch: u64,
    after: Lsn,
) -> (Vec<(Lsn, RecordBuf)>, Lsn) {
    recover_counting(st, dev, shard, epoch, after, &mut 0)
}

fn recover_counting(
    st: &mut ServerWal<MemDevice>,
    dev: &MemDevice,
    shard: u32,
    epoch: u64,
    after: Lsn,
    scans: &mut u32,
) -> (Vec<(Lsn, RecordBuf)>, Lsn) {
    loop {
        match st.recovery(shard, epoch).unwrap() {
            Recovery::Ready(sections) => {
                let (records, end) = records_after(shard, &sections, after).unwrap();
                let last = sections
                    .iter()
                    .map(|(f, c, _)| f + u64::from(*c) - 1)
                    .max()
                    .unwrap_or(0)
                    .max(after)
                    .max(end);
                st.recovered(shard, epoch, last).unwrap();
                return (records, last);
            }
            Recovery::Scan(chain, wanted) => {
                *scans += 1;
                let found = read_sections(dev, &chain, &wanted);
                st.scanned(&wanted, found);
            }
            Recovery::Wait => panic!("shard {shard}: recovery must wait for its fence"),
        }
    }
}

/// What the test knows about one shard.
#[derive(Default, Clone)]
struct Model {
    /// Payloads of records sealed into the log so far, by LSN - 1. A crash
    /// may lose a suffix of those not yet acknowledged.
    sealed: Vec<Vec<u8>>,
    /// Staged, not sealed: dropped by a new grant.
    staged: Vec<Vec<u8>>,
    /// Highest LSN the log published as durable to the holder.
    acked: Lsn,
    epoch: u64,
    /// The two newest checkpoints, oldest first.
    checkpoints: Vec<Lsn>,
}

impl Model {
    fn next(&self) -> Lsn {
        (self.sealed.len() + self.staged.len()) as u64 + 1
    }
    fn newest_checkpoint(&self) -> Lsn {
        self.checkpoints.last().copied().unwrap_or(0)
    }
}

/// Checks recovered records against the model and adopts what survived.
fn check_recovered(
    m: &mut Model,
    shard: u32,
    after: Lsn,
    records: &[(Lsn, RecordBuf)],
    last: Lsn,
    complete: bool,
) {
    for (i, (lsn, rec)) in records.iter().enumerate() {
        assert_eq!(
            *lsn,
            after + 1 + i as u64,
            "shard {shard}: records are contiguous"
        );
        let want = m
            .sealed
            .get(*lsn as usize - 1)
            .unwrap_or_else(|| panic!("shard {shard}: LSN {lsn} was never written"));
        assert_eq!(
            &payload(rec),
            want,
            "shard {shard}: LSN {lsn} holds another lease's record"
        );
    }
    assert!(
        last >= m.acked,
        "shard {shard}: acknowledged LSN {} lost (log ends at {last})",
        m.acked
    );
    if complete {
        assert_eq!(
            last as usize,
            m.sealed.len(),
            "shard {shard}: a sealed record is missing"
        );
    }
    m.sealed.truncate(last as usize);
    m.staged.clear();
}

/// Random workloads over three shards: appends from the holders, writes
/// that complete in any order, re-leases to new epochs (read back from the
/// device while the log keeps writing), checkpoints that retire segments,
/// and crashes that keep, drop or tear every unsynced write. Recovery must
/// always return each shard's records contiguously, every acknowledged one,
/// none written by another lease at the same LSN, and, after a re-lease,
/// every sealed one.
#[test]
fn every_acknowledged_record_survives_releases_and_crashes() {
    const SHARDS: u32 = 3;
    // How much of the interesting ground the seeds covered.
    let (mut scans, mut releases, mut lost, mut replayed, mut retired) = (0, 0, 0, 0, 0);
    for seed in 1..=60u64 {
        let mut rng = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let dev = MemDevice::default();
        let mut models: Vec<Model> = (0..SHARDS).map(|_| Model::default()).collect();
        let mut epoch = 0u64;
        let mut life = 1u64;
        let mut st = ServerWal::open(dev.clone(), SHARDS, opts(), life, 0).unwrap();
        // Every shard's first lease, recovered from the open-time read.
        for s in 0..SHARDS {
            epoch += 1;
            st.grant(s, epoch).unwrap();
            let (records, last) = recover(&mut st, &dev, s, epoch, 0);
            assert!(records.is_empty() && last == 0);
            models[s as usize].epoch = epoch;
        }
        let mut in_flight: Vec<(InFlight, Vec<(u32, usize)>)> = Vec::new();
        let mut crashes = 0;
        for _op in 0..600 {
            rng = xorshift(rng);
            let choice = rng % 100;
            let shard = ((rng >> 16) % u64::from(SHARDS)) as u32;
            let m = &mut models[shard as usize];
            match choice {
                // A holder appends a section.
                0..=39 => {
                    let n = 1 + ((rng >> 24) % 4) as u32;
                    let size = ((rng >> 32) % 700) as usize;
                    let (batch, payloads) = section(shard, m.next(), n, m.epoch, size);
                    st.append(shard, m.epoch, batch).unwrap();
                    m.staged.extend(payloads);
                }
                // A write starts (padded when another may follow it).
                40..=59 => {
                    if !st.wal().can_begin() || !st.wal().has_staged() {
                        continue;
                    }
                    let overlap = (rng >> 20) % 2 == 0;
                    let Some(w) = st.wal_mut().begin_write(0, overlap).unwrap() else {
                        continue;
                    };
                    let mut moved = Vec::new();
                    for (s, m) in models.iter_mut().enumerate() {
                        if !m.staged.is_empty() {
                            moved.push((s as u32, m.staged.len()));
                            m.sealed.append(&mut m.staged);
                        }
                    }
                    in_flight.push((w, moved));
                }
                // A write completes, possibly before an earlier one.
                60..=74 => {
                    if in_flight.is_empty() {
                        continue;
                    }
                    let i = ((rng >> 28) as usize) % in_flight.len();
                    let (w, _) = in_flight.remove(i);
                    dev.sync(w.segment()).unwrap();
                    for mark in st.complete(w, 0) {
                        let m = &mut models[mark.shard as usize];
                        assert!(mark.lsn >= m.acked, "durable positions never go back");
                        assert!(mark.lsn as usize <= m.sealed.len());
                        m.acked = mark.lsn;
                    }
                }
                // The shard checkpoints at its durable position.
                75..=82 => {
                    let lsn = m.acked;
                    if lsn <= m.newest_checkpoint() {
                        continue;
                    }
                    m.checkpoints.push(lsn);
                    if m.checkpoints.len() > 2 {
                        m.checkpoints.remove(0);
                    }
                    let oldest = m.checkpoints[0];
                    st.set_retain(shard, oldest).unwrap();
                }
                // The shard moves to another lease: its old holder's staged
                // section is dropped; recovery waits for the writes in
                // flight and reads the shard back from the device.
                83..=94 => {
                    epoch += 1;
                    st.grant(shard, epoch).unwrap();
                    // A stale append is refused.
                    let (b, _) = section(shard, m.next(), 1, m.epoch, 10);
                    assert!(st.append(shard, m.epoch, b).is_err());
                    m.staged.clear();
                    m.epoch = epoch;
                    // Recovery waits for the writes in flight, which may
                    // hold the old holder's records; finish them.
                    if st.wal().acked() < st.fence(shard) {
                        assert!(matches!(st.recovery(shard, epoch).unwrap(), Recovery::Wait));
                    }
                    while st.wal().acked() < st.fence(shard) {
                        let (w, _) = in_flight.remove(0);
                        dev.sync(w.segment()).unwrap();
                        for mark in st.complete(w, 0) {
                            models[mark.shard as usize].acked = mark.lsn;
                        }
                    }
                    let m = &mut models[shard as usize];
                    let after = m.newest_checkpoint();
                    let (records, last) =
                        recover_counting(&mut st, &dev, shard, epoch, after, &mut scans);
                    releases += 1;
                    replayed += records.len();
                    check_recovered(m, shard, after, &records, last, true);
                    m.acked = last;
                }
                // The server dies; it restarts with a new writer life and
                // every shard gets a new lease.
                _ => {
                    crashes += 1;
                    in_flight.clear();
                    drop(st);
                    dev.crash(&mut rng);
                    life += 1;
                    st = ServerWal::open(dev.clone(), SHARDS, opts(), life, 0).unwrap();
                    for s in 0..SHARDS {
                        let m = &mut models[s as usize];
                        // What survives on disk tells retention again.
                        let oldest = m.checkpoints.first().copied().unwrap_or(0);
                        st.set_retain(s, oldest).unwrap();
                        epoch += 1;
                        st.grant(s, epoch).unwrap();
                        m.epoch = epoch;
                        let after = m.newest_checkpoint();
                        let (records, last) = recover(&mut st, &dev, s, epoch, after);
                        if (last as usize) < m.sealed.len() {
                            lost += 1;
                        }
                        replayed += records.len();
                        check_recovered(m, s, after, &records, last, false);
                        m.acked = last;
                    }
                }
            }
        }
        assert!(crashes > 0 || seed > 50, "seed {seed} never crashed");
        let (live, spares) = st.wal().files();
        retired += spares.len();
        assert!(
            live.len() + spares.len() < 12,
            "seed {seed}: retired segments are reused ({live:?} + {spares:?})"
        );
    }
    eprintln!(
        "{releases} re-leases ({scans} reads back), {lost} crashes lost unacknowledged records, \
         {replayed} records recovered, {retired} spare segments"
    );
    assert!(scans > 50 && releases > 100 && lost > 10 && replayed > 1000 && retired > 0);
}

#[test]
fn appends_are_refused_before_recovery_out_of_order_and_from_stale_leases() {
    let dev = MemDevice::default();
    let mut st = ServerWal::open(dev.clone(), 2, opts(), 1, 0).unwrap();
    st.grant(0, 7).unwrap();
    let (b, _) = section(0, 1, 2, 7, 10);
    let e = st.append(0, 7, b).unwrap_err().to_string();
    assert!(e.contains("must be recovered"), "{e}");
    recover(&mut st, &dev, 0, 7, 0);
    let (b, _) = section(0, 2, 1, 7, 10);
    let e = st.append(0, 7, b).unwrap_err().to_string();
    assert!(e.contains("does not follow"), "{e}");
    let (b, _) = section(0, 1, 2, 7, 10);
    st.append(0, 7, b).unwrap();
    let (b, _) = section(0, 3, 1, 6, 10);
    let e = st.append(0, 6, b).unwrap_err().to_string();
    assert!(e.contains("stale"), "{e}");
    // The other shard was never granted.
    let (b, _) = section(1, 1, 1, 7, 10);
    assert!(st.append(1, 7, b).is_err());
    // A failed log refuses everything.
    st.fail("disk gone".into());
    let (b, _) = section(0, 3, 1, 7, 10);
    let e = st.append(0, 7, b).unwrap_err().to_string();
    assert!(e.contains("disk gone"), "{e}");
}

/// A new holder's recovery waits for the previous holder's write in
/// flight, then sees it; the previous holder's staged section never
/// reaches the log.
#[test]
fn a_new_lease_sees_the_old_leases_writes_in_flight_and_never_follows_them() {
    let dev = MemDevice::default();
    let mut st = ServerWal::open(dev.clone(), 2, opts(), 1, 0).unwrap();
    st.grant(0, 1).unwrap();
    recover(&mut st, &dev, 0, 1, 0);
    st.grant(1, 1).unwrap();
    recover(&mut st, &dev, 1, 1, 0);
    let (b, _) = section(0, 1, 3, 1, 100);
    st.append(0, 1, b).unwrap();
    let w = st.wal_mut().begin_write(0, true).unwrap().unwrap();
    // Shard 1 keeps writing on its own lease meanwhile.
    let (b, _) = section(1, 1, 2, 1, 100);
    st.append(1, 1, b).unwrap();
    let w1 = st.wal_mut().begin_write(0, true).unwrap().unwrap();
    // Unsealed when the lease moves: dropped.
    let (b, _) = section(0, 4, 2, 1, 100);
    st.append(0, 1, b).unwrap();
    st.grant(0, 2).unwrap();
    assert!(matches!(st.recovery(0, 2).unwrap(), Recovery::Wait));
    // The later write completes first: positions wait for the earlier one.
    dev.sync(w1.segment()).unwrap();
    assert!(st.complete(w1, 0).is_empty());
    assert!(matches!(st.recovery(0, 2).unwrap(), Recovery::Wait));
    dev.sync(w.segment()).unwrap();
    let marks = st.complete(w, 0);
    assert_eq!(marks.len(), 2);
    let (records, last) = recover(&mut st, &dev, 0, 2, 0);
    assert_eq!(last, 3);
    assert_eq!(records.len(), 3);
    // The new holder continues right after; the old one cannot append.
    let (b, _) = section(0, 4, 1, 2, 10);
    st.append(0, 2, b).unwrap();
    let (b, _) = section(0, 5, 1, 1, 10);
    assert!(st.append(0, 1, b).is_err());
    st.wal_mut().write(0).unwrap();
    drop(st);
    let mut st = ServerWal::open(dev.clone(), 2, opts(), 2, 0).unwrap();
    st.grant(0, 3).unwrap();
    let (records, last) = recover(&mut st, &dev, 0, 3, 0);
    assert_eq!(last, 4);
    let epochs: Vec<String> = records
        .iter()
        .map(|(_, r)| {
            String::from_utf8_lossy(&payload(r))
                .split('/')
                .nth(2)
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(epochs, ["1", "1", "1", "2"]);
}

/// Segments stay while a read-back runs; retention reported meanwhile
/// applies once it ends. One read serves every shard granted since.
#[test]
fn retirement_waits_for_a_read_back_and_one_read_serves_every_granted_shard() {
    let dev = MemDevice::default();
    let mut st = ServerWal::open(dev.clone(), 3, opts(), 1, 0).unwrap();
    let mut next = [1u64; 3];
    for s in 0..3 {
        st.grant(s, 1).unwrap();
        recover(&mut st, &dev, s, 1, 0);
    }
    for _ in 0..300 {
        for s in 0..3u32 {
            let (b, _) = section(s, next[s as usize], 2, 1, 120);
            next[s as usize] += 2;
            st.append(s, 1, b).unwrap();
        }
        st.wal_mut().write(0).unwrap();
    }
    let (live, _) = st.wal().files();
    assert!(live.len() > 3, "segments rolled: {live:?}");
    // Shards 0 and 1 move to a new lease together.
    st.grant(0, 2).unwrap();
    st.grant(1, 2).unwrap();
    let Recovery::Scan(chain, wanted) = st.recovery(0, 2).unwrap() else {
        panic!("a re-granted shard is read back");
    };
    assert_eq!(wanted, [0, 1]);
    assert!(matches!(st.recovery(1, 2).unwrap(), Recovery::Wait));
    // Every shard's checkpoint now covers everything: retirement waits.
    for s in 0..3 {
        st.set_retain(s, next[s as usize] - 1).unwrap();
    }
    assert_eq!(
        st.wal().files().0,
        live,
        "no segment retires during a read-back"
    );
    let found = read_sections(&dev, &chain, &wanted);
    st.scanned(&wanted, found);
    assert!(
        st.wal().files().0.len() < live.len(),
        "retention applied after it"
    );
    for s in 0..2 {
        let Recovery::Ready(sections) = st.recovery(s, 2).unwrap() else {
            panic!("shard {s} was read back by the first read");
        };
        let (records, _) = records_after(s, &sections, 0).unwrap();
        assert_eq!(records.len() as u64, next[s as usize] - 1);
    }
}

/// Batches of writes in flight together are padded to whole blocks, so the
/// device's block check passes and positions are published in order.
#[test]
fn pipelined_writes_never_share_a_block() {
    let dev = MemDevice::default();
    let mut st = ServerWal::open(dev.clone(), 2, opts(), 1, 0).unwrap();
    for s in 0..2 {
        st.grant(s, 1).unwrap();
        recover(&mut st, &dev, s, 1, 0);
    }
    let mut next = [1u64; 2];
    for round in 0..200 {
        let mut flights = Vec::new();
        for s in 0..2u32 {
            let (b, _) = section(s, next[s as usize], 1, 1, 30 + round % 300);
            next[s as usize] += 1;
            st.append(s, 1, b).unwrap();
            assert!(
                st.wal().can_begin(),
                "round {round}: a second write may start"
            );
            flights.push(st.wal_mut().begin_write(0, true).unwrap().unwrap());
        }
        // Complete newest first.
        for w in flights.into_iter().rev() {
            dev.sync(w.segment()).unwrap();
            st.complete(w, 0);
        }
        assert_eq!(st.durable(0), next[0] - 1);
        assert_eq!(st.durable(1), next[1] - 1);
    }
}

/// The shards a write made durable are awaited until each has sent its
/// next section or the next batch is sealed without them, and each one's
/// turnaround is measured once per publication.
#[test]
fn the_shards_a_write_released_are_awaited_until_each_returns_or_a_seal() {
    let dev = MemDevice::default();
    let mut st = ServerWal::open(dev.clone(), 3, opts(), 1, 0).unwrap();
    for s in 0..3 {
        st.grant(s, 1).unwrap();
        recover(&mut st, &dev, s, 1, 0);
    }
    assert!(!st.awaits_cohort(), "nothing was released yet");
    let mut next = [1u64; 3];
    for s in 0..2u32 {
        let (b, _) = section(s, next[s as usize], 1, 1, 50);
        next[s as usize] += 1;
        st.append(s, 1, b).unwrap();
    }
    let w = st.seal(0, true).unwrap().unwrap();
    dev.sync(w.segment()).unwrap();
    st.complete(w, 1000);
    assert!(st.awaits_cohort(), "shards 0 and 1 were released");
    // A shard the write did not hold changes nothing.
    let (b, _) = section(2, next[2], 1, 1, 50);
    next[2] += 1;
    st.append(2, 1, b).unwrap();
    assert!(st.awaits_cohort());
    assert_eq!(st.turnaround(2, 1500), None);
    let (b, _) = section(0, next[0], 1, 1, 50);
    next[0] += 1;
    st.append(0, 1, b).unwrap();
    assert_eq!(st.turnaround(0, 1300), Some(300));
    assert_eq!(st.turnaround(0, 1400), None, "once per publication");
    assert!(st.awaits_cohort(), "shard 1 is still awaited");
    let (b, _) = section(1, next[1], 1, 1, 50);
    next[1] += 1;
    st.append(1, 1, b).unwrap();
    assert!(!st.awaits_cohort(), "every released shard returned");
    // A seal stops awaiting stragglers.
    let w = st.seal(0, true).unwrap().unwrap();
    dev.sync(w.segment()).unwrap();
    st.complete(w, 2000);
    assert!(st.awaits_cohort());
    let (b, _) = section(0, next[0], 1, 1, 50);
    st.append(0, 1, b).unwrap();
    st.seal(0, true).unwrap().unwrap();
    assert!(!st.awaits_cohort(), "shards 1 and 2 are no longer awaited");
}
