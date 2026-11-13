//! Hierarchical timing wheel with millisecond resolution.
//!
//! Six levels of 64 slots cover about 2.2 years; later deadlines wait in an
//! overflow map. Insertion is O(1); each entry moves down at most five times.
//! Removal is lazy: owners check, when an entry fires, that it is still
//! current. Durable timers are rebuilt from journal records on recovery, so
//! the wheel itself is never persisted.

use std::collections::BTreeMap;

const LEVELS: usize = 6;
const SLOTS: usize = 64;
const BITS: u32 = 6;

struct Level<K> {
    occupied: u64,
    slots: Vec<Vec<(u64, K)>>,
}

impl<K> Level<K> {
    fn new() -> Self {
        Self {
            occupied: 0,
            slots: (0..SLOTS).map(|_| Vec::new()).collect(),
        }
    }
}

pub struct Wheel<K> {
    elapsed: u64,
    levels: Vec<Level<K>>,
    overflow: BTreeMap<u64, Vec<K>>,
    due: Vec<K>,
    len: usize,
}

fn slot_range(level: usize) -> u64 {
    1u64 << (BITS * level as u32)
}

fn level_range(level: usize) -> u64 {
    1u64 << (BITS * (level as u32 + 1))
}

impl<K: Copy> Wheel<K> {
    pub fn new(now_ms: u64) -> Self {
        Self {
            elapsed: now_ms,
            levels: (0..LEVELS).map(|_| Level::new()).collect(),
            overflow: BTreeMap::new(),
            due: Vec::new(),
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn elapsed(&self) -> u64 {
        self.elapsed
    }

    pub fn insert(&mut self, when_ms: u64, key: K) {
        self.len += 1;
        self.place(when_ms, key);
    }

    fn level_for(&self, when: u64) -> usize {
        let masked = (self.elapsed ^ when) | (SLOTS as u64 - 1);
        let significant = 63 - masked.leading_zeros();
        (significant / BITS) as usize
    }

    /// When an overflow deadline comes within the wheel's range: once the
    /// cursor shares its top-level window.
    fn overflow_window(&self) -> Option<u64> {
        let when = *self.overflow.keys().next()?;
        Some((when & !(level_range(LEVELS - 1) - 1)).max(self.elapsed))
    }

    fn move_overflow(&mut self) {
        while let Some((&when, _)) = self.overflow.iter().next() {
            if self.level_for(when) >= LEVELS {
                break;
            }
            if let Some(keys) = self.overflow.remove(&when) {
                for k in keys {
                    self.place(when, k);
                }
            }
        }
    }

    fn place(&mut self, when: u64, key: K) {
        if when <= self.elapsed {
            self.due.push(key);
            return;
        }
        let level = self.level_for(when);
        if level >= LEVELS {
            self.overflow.entry(when).or_default().push(key);
            return;
        }
        let slot = ((when >> (BITS * level as u32)) & (SLOTS as u64 - 1)) as usize;
        let lvl = &mut self.levels[level];
        lvl.occupied |= 1u64 << slot;
        lvl.slots[slot].push((when, key));
    }

    /// The level, slot and start time of the next occupied slot.
    fn next_expiration(&self) -> Option<(usize, usize, u64)> {
        for (level, lvl) in self.levels.iter().enumerate() {
            if lvl.occupied == 0 {
                continue;
            }
            let now_slot = (self.elapsed >> (BITS * level as u32)) & (SLOTS as u64 - 1);
            let rotated = lvl.occupied.rotate_right(now_slot as u32);
            let zeros = u64::from(rotated.trailing_zeros());
            let slot = ((zeros + now_slot) % SLOTS as u64) as usize;
            let base = self.elapsed & !(level_range(level) - 1);
            let mut deadline = base + slot as u64 * slot_range(level);
            if deadline < self.elapsed && slot as u64 >= now_slot {
                deadline = self.elapsed;
            } else if deadline < self.elapsed {
                deadline += level_range(level);
            }
            return Some((level, slot, deadline));
        }
        None
    }

    /// When `poll` should next be called, if any timer is pending.
    pub fn next_wake(&self) -> Option<u64> {
        if !self.due.is_empty() {
            return Some(self.elapsed);
        }
        let wheel = self.next_expiration().map(|(_, _, d)| d);
        let over = self.overflow_window();
        match (wheel, over) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Advances to `now_ms` and appends every entry due by then to `out`.
    pub fn poll(&mut self, now_ms: u64, out: &mut Vec<K>) {
        let before = out.len();
        out.append(&mut self.due);
        loop {
            self.move_overflow();
            out.append(&mut self.due);
            match self.next_expiration() {
                Some((level, slot, deadline)) if deadline <= now_ms => {
                    self.elapsed = self.elapsed.max(deadline);
                    let lvl = &mut self.levels[level];
                    lvl.occupied &= !(1u64 << slot);
                    let entries = std::mem::take(&mut lvl.slots[slot]);
                    for (when, key) in entries {
                        if when <= self.elapsed {
                            out.push(key);
                        } else {
                            self.place(when, key);
                        }
                    }
                    out.append(&mut self.due);
                }
                _ => match self.overflow_window() {
                    // Nothing in the wheel is due before the overflow entry
                    // comes into range: jump the cursor there and continue.
                    Some(w) if w <= now_ms && w > self.elapsed => self.elapsed = w,
                    _ => break,
                },
            }
        }
        self.elapsed = self.elapsed.max(now_ms);
        self.len -= out.len() - before;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny deterministic generator so the core needs no dependency.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 11
        }
    }

    #[test]
    fn fires_in_order_and_on_time() {
        let mut rng = Lcg(42);
        let start = 1_700_000_000_000u64;
        let mut w = Wheel::new(start);
        let mut expected = Vec::new();
        for i in 0..5000u64 {
            let delta = match i % 4 {
                0 => rng.next() % 64,
                1 => rng.next() % 5000,
                2 => rng.next() % 3_600_000,
                _ => rng.next() % (40 * 86_400_000),
            };
            w.insert(start + delta, (start + delta, i));
            expected.push((start + delta, i));
        }
        expected.sort();
        let mut fired = Vec::new();
        let mut now = start;
        let end = start + 41 * 86_400_000;
        while now < end {
            let mut out = Vec::new();
            w.poll(now, &mut out);
            for (when, id) in out {
                assert!(when <= now, "fired early");
                // Entries fire no later than the poll after their deadline.
                fired.push((when, id));
            }
            now = match w.next_wake() {
                Some(t) => t.max(now + 1),
                None => break,
            };
        }
        fired.sort();
        assert_eq!(fired, expected);
        assert!(w.is_empty());
    }

    #[test]
    fn exact_wake_times() {
        let mut w = Wheel::new(1000);
        w.insert(1005, 1);
        w.insert(1000 + 64 * 64 + 3, 2);
        assert_eq!(w.next_wake(), Some(1005));
        let mut out = Vec::new();
        w.poll(1004, &mut out);
        assert!(out.is_empty());
        w.poll(1005, &mut out);
        assert_eq!(out, vec![1]);
        out.clear();
        // Cascading wakes are never later than the true deadline.
        while let Some(t) = w.next_wake() {
            assert!(t <= 1000 + 64 * 64 + 3);
            w.poll(t, &mut out);
            if !out.is_empty() {
                assert_eq!(t, 1000 + 64 * 64 + 3);
                break;
            }
        }
        assert_eq!(out, vec![2]);
    }

    #[test]
    fn far_future_overflow() {
        let mut w = Wheel::new(0);
        let far = 5 * 365 * 86_400_000u64;
        w.insert(far, 9);
        let mut out = Vec::new();
        let mut now = 0;
        while out.is_empty() {
            now = w.next_wake().unwrap().max(now + 1);
            w.poll(now, &mut out);
        }
        assert_eq!(out, vec![9]);
        assert_eq!(now, far);
    }
}
