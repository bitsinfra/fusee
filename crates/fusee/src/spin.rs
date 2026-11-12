//! Spin, then park: how a thread that expects an answer soon waits for it.
//!
//! A thread that parks pays a wake-up when the answer arrives, which can
//! cost more than the answer itself. Spinning for a while first avoids that
//! when the answer is close, at the price of a busy core. The budget
//! follows recent waits: twice the longer of the last two, at most a cap,
//! and nothing once two waits in a row took longer than the cap, so a
//! thread spins only while answers keep arriving quickly (one wait made
//! long by a preempted thread does not stop it). On hosts with two or
//! fewer cores it never spins: the spinner would take the core the answer
//! has to be produced on.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Whether this host has cores to spare for spinning.
/// When each shard last did work (nanoseconds since the first call, 0 =
/// never), by shard number modulo 64.
static BUSY_AT: [Slot; 64] = [const { Slot(AtomicU64::new(0)) }; 64];

/// One shard's or host thread's time, on a cache line of its own: eight
/// shards storing into one line moved it between their cores on every
/// busy tick.
#[repr(align(128))]
struct Slot(AtomicU64);

impl Slot {
    /// Records `now`, unless the time held is recent enough for the busy
    /// window already (a store at most every half millisecond).
    fn touch(&self, now: u64) {
        if now.saturating_sub(self.0.load(Ordering::Relaxed)) >= BUSY_WINDOW_NS / 4 {
            self.0.store(now, Ordering::Relaxed);
        }
    }
}

/// How recently a shard must have worked to count as busy.
const BUSY_WINDOW_NS: u64 = 2_000_000;

pub(crate) fn clock_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    // At least 1, so a recorded time is never "never".
    (START.get_or_init(Instant::now).elapsed().as_nanos() as u64).max(1)
}

/// A shard did work now.
pub fn mark_busy(shard: u32) {
    BUSY_AT[shard as usize % 64].touch(clock_ns());
}

/// Whether spinning is worth a core now: at most two shards of the process
/// worked in the last couple of milliseconds. One sequential workflow (or a
/// parent and child on two shards) gains a flush's worth of wake-up latency
/// from spinning; when many shards are busy, work arrives batched behind
/// other runs anyway and every spinning thread would burn a core: spend a
/// core only where one is spare.
pub fn few_busy() -> bool {
    if spin_off() {
        return false;
    }
    let now = clock_ns();
    let mut busy = 0;
    for at in &BUSY_AT {
        let t = at.0.load(Ordering::Relaxed);
        if t != 0 && now.saturating_sub(t) <= BUSY_WINDOW_NS {
            busy += 1;
            if busy > 2 {
                return false;
            }
        }
    }
    true
}

/// When each host thread (a language binding's: a Node or Python worker,
/// an interpreter's main thread) last called into the engine, by a slot of
/// its own.
static HOST_BUSY_AT: [Slot; 64] = [const { Slot(AtomicU64::new(0)) }; 64];

/// A host thread is doing engine work now.
pub fn mark_host_busy() {
    use std::sync::atomic::AtomicUsize;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SLOT: usize = NEXT.fetch_add(1, Ordering::Relaxed) % 64;
    }
    let i = SLOT.with(|s| *s);
    HOST_BUSY_AT[i].touch(clock_ns());
}

/// Whether a thread may spin now without taking a core something else
/// needs: the shards and host threads that worked in the last couple of
/// milliseconds, and the spinner itself, fit in the host's cores
///. Eight worker threads on eight vCPUs never
/// spin under it; one host thread and its shards on eight do.
pub fn spare_core() -> bool {
    static CORES: OnceLock<usize> = OnceLock::new();
    let cores = *CORES.get_or_init(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
    if cores <= 2 || spin_off() {
        return false;
    }
    let now = clock_ns();
    let recent = |at: &Slot| {
        let t = at.0.load(Ordering::Relaxed);
        t != 0 && now.saturating_sub(t) <= BUSY_WINDOW_NS
    };
    let mut running = 1;
    for at in BUSY_AT.iter().chain(HOST_BUSY_AT.iter()) {
        if recent(at) {
            running += 1;
            if running >= cores {
                return false;
            }
        }
    }
    true
}

pub fn may_spin() -> bool {
    static SPARE: OnceLock<bool> = OnceLock::new();
    *SPARE.get_or_init(|| {
        !spin_off() && std::thread::available_parallelism().is_ok_and(|n| n.get() > 2)
    })
}

/// `FUSEE_SPIN=0`: no thread spins for anything, it waits (for the least
/// CPU per step at low rates, where spinning is most of the cost).
pub fn spin_off() -> bool {
    static OFF: OnceLock<bool> = OnceLock::new();
    *OFF.get_or_init(|| std::env::var("FUSEE_SPIN").is_ok_and(|v| v.trim() == "0"))
}

/// The spin budget of one kind of wait, from how long the last two took.
#[derive(Debug)]
pub struct Budget {
    /// The last two waits in ns, the older one in the high half (0: none).
    recent_ns: AtomicU64,
    cap_ns: u64,
}

impl Budget {
    pub const fn new(cap: Duration) -> Self {
        Self {
            recent_ns: AtomicU64::new(0),
            cap_ns: cap.as_nanos() as u64,
        }
    }

    /// How long the next wait may spin.
    pub fn get(&self) -> Duration {
        let both = self.recent_ns.load(Ordering::Relaxed);
        let (older, last) = (both >> 32, both & u64::from(u32::MAX));
        let shorter = match older {
            0 => last,
            _ => older.min(last),
        };
        if shorter == 0 || shorter > self.cap_ns || !may_spin() {
            return Duration::ZERO;
        }
        Duration::from_nanos((older.max(last) * 2).min(self.cap_ns))
    }

    /// A wait took `waited`.
    pub fn record(&self, waited: Duration) {
        let ns = u64::try_from(waited.as_nanos())
            .unwrap_or(u64::MAX)
            .clamp(1, u64::from(u32::MAX));
        let last = self.recent_ns.load(Ordering::Relaxed) & u64::from(u32::MAX);
        self.recent_ns.store(last << 32 | ns, Ordering::Relaxed);
    }
}

/// Spins until `ready()` or `budget` has passed. Returns whether `ready()`.
pub fn spin_until(budget: Duration, mut ready: impl FnMut() -> bool) -> bool {
    if ready() {
        return true;
    }
    if budget.is_zero() || spin_off() {
        return false;
    }
    let until = Instant::now() + budget;
    loop {
        for _ in 0..32 {
            std::hint::spin_loop();
            if ready() {
                return true;
            }
        }
        if Instant::now() >= until {
            return false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_budget_follows_the_last_two_waits() {
        let b = Budget::new(Duration::from_micros(50));
        assert_eq!(b.get(), Duration::ZERO, "no wait seen yet");
        let us = |n: u64| {
            if may_spin() {
                Duration::from_micros(n)
            } else {
                Duration::ZERO
            }
        };
        b.record(Duration::from_micros(10));
        assert_eq!(b.get(), us(20), "twice the last wait");
        b.record(Duration::from_micros(20));
        assert_eq!(b.get(), us(40), "twice the longer of the last two");
        b.record(Duration::from_micros(5));
        assert_eq!(b.get(), us(40), "twice the longer of the last two");
        b.record(Duration::from_micros(30));
        assert_eq!(b.get(), us(50), "at most the cap");
        b.record(Duration::from_millis(5));
        assert_eq!(b.get(), us(50), "one long wait does not stop it");
        b.record(Duration::from_secs(10));
        assert_eq!(b.get(), Duration::ZERO, "two long waits stop it");
    }

    #[test]
    fn spinning_stops_when_ready_or_out_of_budget() {
        let mut n = 0;
        assert!(spin_until(Duration::from_millis(100), || {
            n += 1;
            n == 100
        }));
        let t = Instant::now();
        assert!(!spin_until(Duration::from_micros(200), || false));
        assert!(t.elapsed() >= Duration::from_micros(200));
        assert!(!spin_until(Duration::ZERO, || false));
    }
}
