//! The run table: one 32-byte entry for every run a shard knows, loaded or
//! parked, found by run ID or by workflow ID hash.
//!
//! A parked run lives only here: where its journal is in the run store,
//! its earliest timer (or, once finished, when it finished) and its status.
//! Entries sit in fixed chunks, so growing never copies them and never
//! leaves more than one chunk unused. Two open-addressing indexes of 4-byte
//! buckets find an entry by run and by ID hash; each is rebuilt to about 65%
//! full when it reaches 85%, so a parked run costs 32 + 2 × 4 / load bytes:
//! 41.4 to 44.3.
//!
//! The same 32 bytes (without the in-memory flags) are the record format of
//! the persisted index (see [`crate::store::StoreEntry::Index`]).

use crate::record::{RunId, Status};

/// Position of an entry. Stable while the entry exists, until [`RunTable::compact`].
pub type Slot = u32;

/// Entries per chunk of the table, as a power of two. A chunk is 512 KiB:
/// above glibc's mmap threshold (which the engine fixes at 256 KiB), so
/// each chunk is mapped on its own instead of sitting in a thread's heap
/// among loaded runs' short-lived blocks, where it would keep the pages
/// around it resident after those are freed.
pub const CHUNK_BITS: u32 = 14;
pub const CHUNK: usize = 1 << CHUNK_BITS;
const NONE: u32 = u32::MAX;
const MASK48: u64 = (1 << 48) - 1;

/// Entry states.
pub const LOADED: u8 = 0;
pub const PARKED: u8 = 1;
/// `DONE + status`: parked, finished with that status.
pub const DONE: u8 = 2;
/// Only in persisted index deltas: the run is gone.
pub const DELETED: u8 = 0xff;

/// In-memory flags.
pub mod flag {
    /// A near-timer item is queued for the entry's current time.
    pub const NEAR: u8 = 1;
    /// A load was requested and has not happened yet.
    pub const PENDING: u8 = 2;
    /// Changed since the persisted index last recorded it.
    pub const DIRTY: u8 = 4;
    /// The ID index points at this entry (the latest run of its ID).
    pub const IN_ID: u8 = 8;
    /// The run's frame is a `Delta`: compaction writes its journal whole.
    /// The only flag the persisted record keeps.
    pub const CHAINED: u8 = 16;
}

/// Size of one entry and of one persisted index record.
pub const RECORD: usize = 32;

/// One run. Four words: the ID hash; the run ID (48 bits), state and flags;
/// the store offset (48 bits) and the low half of the frame length; the
/// time (48 bits) and the high half of the length.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Entry {
    w: [u64; 4],
}

impl Entry {
    pub fn new(run: RunId, id_hash: u64, state: u8) -> Self {
        let mut e = Self::default();
        e.w[0] = id_hash;
        e.w[1] = (run & MASK48) | (u64::from(state) << 48);
        e
    }

    pub fn id_hash(&self) -> u64 {
        self.w[0]
    }

    pub fn run(&self) -> RunId {
        self.w[1] & MASK48
    }

    pub fn state(&self) -> u8 {
        (self.w[1] >> 48) as u8
    }

    pub fn set_state(&mut self, state: u8) {
        self.w[1] = (self.w[1] & !(0xff << 48)) | (u64::from(state) << 48);
    }

    pub fn flags(&self) -> u8 {
        (self.w[1] >> 56) as u8
    }

    pub fn has(&self, f: u8) -> bool {
        self.flags() & f != 0
    }

    pub fn set(&mut self, f: u8, on: bool) {
        let flags = if on {
            self.flags() | f
        } else {
            self.flags() & !f
        };
        self.w[1] = (self.w[1] & !(0xff << 56)) | (u64::from(flags) << 56);
    }

    pub fn offset(&self) -> u64 {
        self.w[2] & MASK48
    }

    pub fn len(&self) -> u32 {
        ((self.w[2] >> 48) | ((self.w[3] >> 48) << 16)) as u32
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set_location(&mut self, offset: u64, len: u32) {
        let len = u64::from(len);
        self.w[2] = (offset & MASK48) | ((len & 0xffff) << 48);
        self.w[3] = (self.w[3] & MASK48) | ((len >> 16) << 48);
    }

    /// Parked suspended: the earliest timer (0: none). Parked finished: the
    /// finish time. Loaded: the earliest timer not yet in the timing wheel.
    pub fn time(&self) -> u64 {
        self.w[3] & MASK48
    }

    pub fn set_time(&mut self, t: u64) {
        self.w[3] = (self.w[3] & !MASK48) | (t.min(MASK48));
    }

    pub fn is_parked(&self) -> bool {
        self.state() != LOADED
    }

    /// `Some` for a parked finished run.
    pub fn done(&self) -> Option<Status> {
        self.state()
            .checked_sub(DONE)
            .and_then(|s| Status::from_u8(s).ok())
    }

    /// The persisted record: the entry without its in-memory flags.
    pub fn record(&self) -> [u8; RECORD] {
        let mut out = [0u8; RECORD];
        let mut w = self.w;
        w[1] &= !(u64::from(!flag::CHAINED) << 56);
        for (i, word) in w.iter().enumerate() {
            out[i * 8..i * 8 + 8].copy_from_slice(&word.to_le_bytes());
        }
        out
    }

    pub fn from_record(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != RECORD {
            return None;
        }
        let mut w = [0u64; 4];
        for (i, word) in w.iter_mut().enumerate() {
            *word = u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().ok()?);
        }
        let e = Self { w };
        let valid = e.run() != 0
            && e.flags() & !flag::CHAINED == 0
            && (e.state() == PARKED || e.state() == DELETED || e.done().is_some());
        valid.then_some(e)
    }
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn run_key(run: RunId) -> u64 {
    mix(run)
}

fn id_key(id_hash: u64) -> u64 {
    mix(id_hash ^ 0x9e37_79b9_7f4a_7c15)
}

const EMPTY: u32 = u32::MAX;
const TOMB: u32 = u32::MAX - 1;

/// Open addressing with linear probing. A bucket holds a slot and, in the
/// bits the slot does not need, a tag of the key's hash, so a probe rarely
/// reads an entry that is not the one looked for.
#[derive(Default)]
struct Index {
    b: Vec<u32>,
    live: usize,
    tombs: usize,
}

impl Index {
    fn home(&self, h: u64) -> usize {
        ((u128::from(h) * self.b.len() as u128) >> 64) as usize
    }

    fn full(&self) -> bool {
        (self.live + self.tombs + 1) * 20 > self.b.len() * 17
    }

    fn insert(&mut self, h: u64, bucket: u32) {
        let n = self.b.len();
        let mut i = self.home(h);
        loop {
            match self.b[i] {
                EMPTY => break,
                TOMB => {
                    self.tombs -= 1;
                    break;
                }
                _ => i = if i + 1 == n { 0 } else { i + 1 },
            }
        }
        self.b[i] = bucket;
        self.live += 1;
    }

    fn remove_at(&mut self, i: usize) {
        let n = self.b.len();
        let next = if i + 1 == n { 0 } else { i + 1 };
        if self.b[next] == EMPTY {
            // The end of a probe run: no tombstone needed, and the ones
            // just before it are not needed either.
            self.b[i] = EMPTY;
            let mut j = i;
            loop {
                j = if j == 0 { n - 1 } else { j - 1 };
                if self.b[j] != TOMB {
                    break;
                }
                self.b[j] = EMPTY;
                self.tombs -= 1;
            }
        } else {
            self.b[i] = TOMB;
            self.tombs += 1;
        }
        self.live -= 1;
    }

    fn bytes(&self) -> usize {
        self.b.capacity() * 4
    }
}

/// See the module documentation.
pub struct RunTable {
    chunks: Vec<Box<[Entry]>>,
    /// Slots handed out so far (free ones included).
    used: u32,
    live: u32,
    free: u32,
    /// Bits of a bucket that hold the slot; the rest hold the tag.
    slot_bits: u32,
    by_run: Index,
    by_id: Index,
    /// The slot each recently found run had, by its low bits: a wake looks
    /// its run up several times (the timer, the load, the park), and a
    /// table of millions of runs misses the cache on every probe. Each hit
    /// is checked against the entry, so a stale one only falls back to the
    /// index. Allocated on first use.
    recent: std::cell::RefCell<Vec<(RunId, Slot)>>,
}

/// Runs [`RunTable::find_run`] remembers: a round of loads (two chunks of
/// 1,024), with room so that consecutive runs do not evict each other.
const RECENT: usize = 4096;

impl Default for RunTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RunTable {
    pub fn new() -> Self {
        Self {
            chunks: Vec::new(),
            used: 0,
            live: 0,
            free: NONE,
            slot_bits: 28,
            by_run: Index::default(),
            by_id: Index::default(),
            recent: std::cell::RefCell::new(Vec::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.live as usize
    }

    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Slots handed out, live or free: the range a sweep walks.
    pub fn used(&self) -> u32 {
        self.used
    }

    /// Heap bytes held by the table and its indexes.
    pub fn heap_bytes(&self) -> usize {
        self.chunks.len() * CHUNK * RECORD
            + self.chunks.capacity() * std::mem::size_of::<Box<[Entry]>>()
            + self.by_run.bytes()
            + self.by_id.bytes()
            + self.recent.borrow().capacity() * std::mem::size_of::<(RunId, Slot)>()
    }

    pub fn get(&self, slot: Slot) -> &Entry {
        &self.chunks[(slot >> CHUNK_BITS) as usize][slot as usize & (CHUNK - 1)]
    }

    pub fn get_mut(&mut self, slot: Slot) -> &mut Entry {
        &mut self.chunks[(slot >> CHUNK_BITS) as usize][slot as usize & (CHUNK - 1)]
    }

    /// A live entry at `slot`, if the slot is in use.
    pub fn live_at(&self, slot: Slot) -> Option<&Entry> {
        (slot < self.used)
            .then(|| self.get(slot))
            .filter(|e| e.run() != 0)
    }

    fn tag_mask(&self) -> u32 {
        if self.slot_bits >= 32 {
            0
        } else {
            (1u32 << (32 - self.slot_bits)) - 1
        }
    }

    fn bucket(&self, h: u64, slot: Slot) -> u32 {
        if self.slot_bits >= 32 {
            slot
        } else {
            ((h as u32 & self.tag_mask()) << self.slot_bits) | slot
        }
    }

    fn slot_mask(&self) -> u32 {
        if self.slot_bits >= 32 {
            u32::MAX
        } else {
            (1u32 << self.slot_bits) - 1
        }
    }

    /// Finds the bucket in `ix` for which `eq(slot)` holds.
    fn probe(&self, by_id: bool, h: u64, eq: impl Fn(&Entry) -> bool) -> Option<(usize, Slot)> {
        let ix = if by_id { &self.by_id } else { &self.by_run };
        let n = ix.b.len();
        if n == 0 {
            return None;
        }
        let tag = h as u32 & self.tag_mask();
        let shift = self.slot_bits.min(31);
        let mask = self.slot_mask();
        let mut i = ix.home(h);
        loop {
            let b = ix.b[i];
            if b == EMPTY {
                return None;
            }
            if b != TOMB && (self.slot_bits >= 32 || b >> shift == tag) {
                let slot = b & mask;
                if eq(self.get(slot)) {
                    return Some((i, slot));
                }
            }
            i = if i + 1 == n { 0 } else { i + 1 };
        }
    }

    pub fn find_run(&self, run: RunId) -> Option<Slot> {
        let i = run as usize & (RECENT - 1);
        if let Some(&(r, slot)) = self.recent.borrow().get(i)
            && r == run
            && self.live_at(slot).is_some_and(|e| e.run() == run)
        {
            return Some(slot);
        }
        let slot = self
            .probe(false, run_key(run), |e| e.run() == run)
            .map(|(_, s)| s)?;
        let mut recent = self.recent.borrow_mut();
        if recent.is_empty() {
            recent.resize(RECENT, (0, NONE));
        }
        recent[i] = (run, slot);
        Some(slot)
    }

    /// Whether `run` has an entry. Unlike [`find_run`](Self::find_run) it
    /// leaves the recent-lookup cache alone: a listing probes many numbers,
    /// and the step path's cache should not pay for it.
    pub fn has_run(&self, run: RunId) -> bool {
        self.probe(false, run_key(run), |e| e.run() == run)
            .is_some()
    }

    /// The latest run of the ID with this hash.
    pub fn find_id(&self, id_hash: u64) -> Option<Slot> {
        self.probe(true, id_key(id_hash), |e| {
            e.id_hash() == id_hash && e.has(flag::IN_ID)
        })
        .map(|(_, s)| s)
    }

    fn alloc(&mut self) -> Slot {
        if self.free != NONE {
            let s = self.free;
            self.free = self.get(s).w[0] as u32;
            return s;
        }
        let s = self.used;
        if s as usize == self.chunks.len() * CHUNK {
            self.chunks
                .push(vec![Entry::default(); CHUNK].into_boxed_slice());
        }
        self.used += 1;
        if self.used as u64 + 2 >= 1u64 << self.slot_bits.min(32) && self.slot_bits < 32 {
            // Slots outgrow the bucket: give up a tag bit.
            self.slot_bits += 1;
            self.rebuild(false, self.by_run.b.len());
            self.rebuild(true, self.by_id.b.len());
        }
        s
    }

    /// Rebuilds one index with `n` buckets (at least enough for 65% load).
    fn rebuild(&mut self, by_id: bool, n: usize) {
        let live = if by_id {
            self.by_id.live
        } else {
            self.by_run.live
        };
        let n = n.max((live + 1) * 100 / 65 + 1).max(8);
        let mut ix = Index {
            b: vec![EMPTY; n],
            live: 0,
            tombs: 0,
        };
        for chunk in 0..self.chunks.len() {
            let base = (chunk * CHUNK) as u32;
            for (k, e) in self.chunks[chunk].iter().enumerate() {
                let slot = base + k as u32;
                if slot >= self.used || e.run() == 0 {
                    continue;
                }
                let h = if by_id {
                    if !e.has(flag::IN_ID) {
                        continue;
                    }
                    id_key(e.id_hash())
                } else {
                    run_key(e.run())
                };
                let bucket = self.bucket(h, slot);
                ix.insert(h, bucket);
            }
        }
        if by_id {
            self.by_id = ix;
        } else {
            self.by_run = ix;
        }
    }

    fn grow(&mut self, by_id: bool) {
        let ix = if by_id { &self.by_id } else { &self.by_run };
        if ix.full() {
            // Size for the live entries only: tombstones are dropped.
            let n = (ix.live + 1) * 100 / 65 + 1;
            self.rebuild(by_id, n);
        }
    }

    /// Adds an entry for a run not in the table. The ID index is separate:
    /// see [`index_id`](Self::index_id).
    pub fn insert(&mut self, mut e: Entry) -> Slot {
        debug_assert!(self.find_run(e.run()).is_none());
        e.set(flag::IN_ID, false);
        let slot = self.alloc();
        *self.get_mut(slot) = e;
        self.live += 1;
        self.grow(false);
        let h = run_key(e.run());
        let bucket = self.bucket(h, slot);
        self.by_run.insert(h, bucket);
        slot
    }

    /// Makes `slot` the entry its ID hash finds, unless a later run of the
    /// same ID holds the place (the latest run for an ID wins).
    pub fn index_id(&mut self, slot: Slot) {
        let (run, hash) = {
            let e = self.get(slot);
            (e.run(), e.id_hash())
        };
        if self.get(slot).has(flag::IN_ID) {
            return;
        }
        let h = id_key(hash);
        if let Some((i, other)) = self.probe(true, h, |e| e.id_hash() == hash && e.has(flag::IN_ID))
        {
            if self.get(other).run() > run {
                return;
            }
            self.get_mut(other).set(flag::IN_ID, false);
            self.by_id.b[i] = self.bucket(h, slot);
            self.get_mut(slot).set(flag::IN_ID, true);
            return;
        }
        self.grow(true);
        let bucket = self.bucket(h, slot);
        self.by_id.insert(h, bucket);
        self.get_mut(slot).set(flag::IN_ID, true);
    }

    pub fn remove(&mut self, slot: Slot) -> Entry {
        let e = *self.get(slot);
        let run = e.run();
        if let Some((i, _)) = self.probe(false, run_key(run), |x| x.run() == run) {
            self.by_run.remove_at(i);
        }
        if e.has(flag::IN_ID) {
            let hash = e.id_hash();
            if let Some((i, _)) = self.probe(true, id_key(hash), |x| x.run() == run) {
                self.by_id.remove_at(i);
            }
        }
        let next = u64::from(self.free);
        let f = self.get_mut(slot);
        *f = Entry::default();
        f.w[0] = next;
        self.free = slot;
        self.live -= 1;
        e
    }

    /// Live entries in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (Slot, &Entry)> + '_ {
        let used = self.used;
        self.chunks
            .iter()
            .enumerate()
            .flat_map(move |(c, chunk)| {
                chunk
                    .iter()
                    .enumerate()
                    .map(move |(k, e)| ((c * CHUNK + k) as Slot, e))
            })
            .take_while(move |(s, _)| *s < used)
            .filter(|(_, e)| e.run() != 0)
    }

    /// Makes room for `n` more entries (bulk loading).
    pub fn reserve(&mut self, n: usize) {
        let want = self.live as usize + n;
        let chunks = want.div_ceil(CHUNK);
        if chunks > self.chunks.len() {
            self.chunks.reserve_exact(chunks - self.chunks.len());
        }
        for by_id in [false, true] {
            let ix = if by_id { &self.by_id } else { &self.by_run };
            let need = (ix.live + n + 1) * 100 / 70 + 1;
            if need > ix.b.len() {
                self.rebuild(by_id, need);
            }
        }
    }

    /// Moves entries into the holes left by removed ones when at least
    /// three quarters of the slots are free, and gives back the chunks
    /// emptied. Slots change: returns whether anything moved.
    pub fn compact(&mut self) -> bool {
        let used = self.used as usize;
        // Worth it only if it gives back at least one chunk.
        if used < 4 * 4096
            || (self.live as usize) * 4 > used
            || (self.live as usize).div_ceil(CHUNK) >= self.chunks.len()
        {
            return false;
        }
        let live: Vec<Entry> = self.iter().map(|(_, e)| *e).collect();
        self.chunks.clear();
        self.chunks.shrink_to_fit();
        self.used = 0;
        self.live = 0;
        self.free = NONE;
        self.chunks.reserve_exact(live.len().div_ceil(CHUNK));
        for e in live {
            let slot = self.alloc();
            *self.get_mut(slot) = e;
            self.live += 1;
        }
        self.rebuild(false, 0);
        self.rebuild(true, 0);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_fields_round_trip() {
        let mut e = Entry::new(0xabcd_ef12_3456, 0x1234_5678_9abc_def0, PARKED);
        e.set_location(0x0000_7fff_ffff_fff0, 0xfedc_ba98);
        e.set_time(1_700_000_000_123);
        e.set(flag::NEAR, true);
        e.set(flag::DIRTY, true);
        assert_eq!(e.run(), 0xabcd_ef12_3456);
        assert_eq!(e.id_hash(), 0x1234_5678_9abc_def0);
        assert_eq!(e.offset(), 0x0000_7fff_ffff_fff0);
        assert_eq!(e.len(), 0xfedc_ba98);
        assert_eq!(e.time(), 1_700_000_000_123);
        assert_eq!(e.state(), PARKED);
        assert!(e.has(flag::NEAR) && e.has(flag::DIRTY) && !e.has(flag::IN_ID));
        e.set(flag::NEAR, false);
        assert!(!e.has(flag::NEAR) && e.has(flag::DIRTY));
        e.set(flag::CHAINED, true);
        let back = Entry::from_record(&e.record()).unwrap();
        assert_eq!(back.flags(), flag::CHAINED);
        assert_eq!(back.run(), e.run());
        assert_eq!(back.len(), e.len());
        assert_eq!(back.time(), e.time());
        let mut done = Entry::new(5, 6, DONE + Status::Failed as u8);
        done.set_time(9);
        assert_eq!(done.done(), Some(Status::Failed));
        assert!(Entry::from_record(&done.record()).is_some());
        assert!(Entry::from_record(&Entry::new(5, 6, LOADED).record()).is_none());
        assert!(Entry::from_record(&[0; 31]).is_none());
    }

    #[test]
    fn finds_by_run_and_by_id_through_churn() {
        let mut t = RunTable::new();
        let mut alive: Vec<(u64, u64)> = Vec::new();
        let mut latest = std::collections::BTreeMap::new();
        let mut x = 7u64;
        let mut next = 1u64;
        for round in 0..60_000u64 {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            if x >> 62 != 0 || alive.is_empty() {
                let run = next;
                next += 1;
                // Every third run reuses an earlier ID: the later one wins.
                let id = if run.is_multiple_of(3) {
                    run / 3
                } else {
                    run * 1000
                };
                let s = t.insert(Entry::new(run, id, PARKED));
                t.index_id(s);
                alive.push((run, id));
                latest.insert(id, run);
            } else {
                let k = (x >> 20) as usize % alive.len();
                let (run, _) = alive.swap_remove(k);
                let s = t.find_run(run).unwrap();
                t.remove(s);
            }
            if round % 997 == 0 {
                let live: std::collections::BTreeSet<u64> = alive.iter().map(|a| a.0).collect();
                for &(run, id) in &alive {
                    let s = t.find_run(run).unwrap();
                    assert_eq!(t.get(s).run(), run);
                    let newest = latest[&id];
                    match t.find_id(id) {
                        Some(f) => {
                            assert_eq!(t.get(f).run(), newest);
                            assert!(live.contains(&newest));
                        }
                        // The ID's latest run is gone: older retained runs
                        // of it are not found by ID.
                        None => assert!(!live.contains(&newest)),
                    }
                }
                assert_eq!(t.len(), alive.len());
                assert_eq!(t.iter().count(), alive.len());
            }
        }
        assert!(t.find_run(next + 5).is_none());
        let before = t.heap_bytes();
        alive.sort_unstable();
        for &(run, _) in &alive[10..] {
            let s = t.find_run(run).unwrap();
            t.remove(s);
        }
        assert!(t.compact());
        assert!(t.heap_bytes() < before);
        for &(run, _) in &alive[..10] {
            assert_eq!(t.get(t.find_run(run).unwrap()).run(), run);
        }
        assert_eq!(t.iter().count(), 10);
    }

    #[test]
    fn about_45_bytes_per_entry() {
        let mut t = RunTable::new();
        let n = 300_000u64;
        for run in 1..=n {
            let s = t.insert(Entry::new(run, run.wrapping_mul(0x9e37_79b9), PARKED));
            t.index_id(s);
        }
        let per = t.heap_bytes() as f64 / n as f64;
        assert!(per <= 48.0, "{per} bytes per entry");
    }
}
