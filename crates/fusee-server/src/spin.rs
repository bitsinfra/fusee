//! Spin, then park: how a thread that expects an answer soon waits for it.
//!
//! A thread that parks pays a wake-up on the answer's arrival, which on an
//! idle core can cost more than the answer itself. Spinning for a while
//! first avoids that when the answer is close, at the price of a busy core.
//! The budget follows recent waits, so a thread spins only while answers
//! keep arriving quickly. On hosts with two or fewer cores it never spins:
//! the spinner would take the core the answer has to be produced on.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread::Thread;
use std::time::{Duration, Instant};

/// Whether this host has cores to spare for spinning.
pub(crate) fn may_spin() -> bool {
    static SPARE: OnceLock<bool> = OnceLock::new();
    *SPARE.get_or_init(|| std::thread::available_parallelism().is_ok_and(|n| n.get() > 2))
}

/// The spin budget for a wait expected to take about `recent`: twice that,
/// at most `cap`, and nothing when the wait has been longer than `cap` or
/// the host cannot spare a core.
pub(crate) fn budget(recent: Duration, cap: Duration) -> Duration {
    if !may_spin() || recent.is_zero() || recent > cap {
        return Duration::ZERO;
    }
    (recent * 2).min(cap)
}

/// Spins until `done` or `budget` has passed. Returns whether `done`.
pub(crate) fn spin_until(budget: Duration, done: &dyn Fn() -> bool) -> bool {
    if done() {
        return true;
    }
    if budget.is_zero() {
        return false;
    }
    let until = Instant::now() + budget;
    loop {
        for _ in 0..64 {
            if done() {
                return true;
            }
            std::hint::spin_loop();
        }
        if Instant::now() >= until {
            return done();
        }
    }
}

/// A flag one thread waits on and others raise. Raising it unparks the
/// waiter only if the waiter has stopped spinning and gone to sleep.
#[derive(Default)]
pub(crate) struct Signal {
    raised: AtomicBool,
    sleeping: AtomicBool,
    waiter: Mutex<Option<Thread>>,
}

impl Signal {
    pub(crate) fn raise(&self) {
        self.raised.store(true, Ordering::SeqCst);
        if self.sleeping.load(Ordering::SeqCst)
            && let Some(t) = self
                .waiter
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
        {
            t.unpark();
        }
    }

    /// Clears the flag; returns whether it was raised.
    pub(crate) fn take(&self) -> bool {
        self.raised.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn is_raised(&self) -> bool {
        self.raised.load(Ordering::Acquire)
    }

    /// Waits until the flag is raised, `spin` has passed spinning and then
    /// `sleep` sleeping, or `stop` says to give up (checked every 50 ms).
    /// Does not clear the flag. Returns whether it was raised.
    pub(crate) fn wait(
        &self,
        spin: Duration,
        sleep: Duration,
        stop: Option<&dyn Fn() -> bool>,
    ) -> bool {
        if self.is_raised() {
            return true;
        }
        if !spin.is_zero() {
            let until = Instant::now() + spin;
            loop {
                for _ in 0..64 {
                    if self.is_raised() {
                        return true;
                    }
                    std::hint::spin_loop();
                }
                if Instant::now() >= until {
                    break;
                }
            }
        }
        *self.waiter.lock().unwrap_or_else(|e| e.into_inner()) = Some(std::thread::current());
        let deadline = Instant::now() + sleep;
        let raised = loop {
            self.sleeping.store(true, Ordering::SeqCst);
            if self.raised.load(Ordering::SeqCst) {
                break true;
            }
            if stop.is_some_and(|stop| stop()) {
                break false;
            }
            let now = Instant::now();
            if now >= deadline {
                break false;
            }
            let mut nap = deadline - now;
            if stop.is_some() {
                // Bounded, so `stop` is seen even without an unpark.
                nap = nap.min(Duration::from_millis(50));
            }
            std::thread::park_timeout(nap);
        };
        self.sleeping.store(false, Ordering::SeqCst);
        raised
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn a_raise_reaches_a_sleeping_waiter() {
        let s = Arc::new(Signal::default());
        let s2 = s.clone();
        let t = std::thread::spawn(move || s2.wait(Duration::ZERO, Duration::from_secs(10), None));
        std::thread::sleep(Duration::from_millis(20));
        s.raise();
        assert!(t.join().unwrap());
        assert!(s.take());
        assert!(!s.is_raised());
    }

    #[test]
    fn a_wait_gives_up_when_told_to_stop() {
        let s = Signal::default();
        assert!(!s.wait(
            Duration::from_micros(10),
            Duration::from_secs(10),
            Some(&|| true)
        ));
    }

    #[test]
    fn budgets_follow_recent_waits() {
        let cap = Duration::from_micros(100);
        if may_spin() {
            assert_eq!(
                budget(Duration::from_micros(30), cap),
                Duration::from_micros(60)
            );
            assert_eq!(budget(Duration::from_micros(80), cap), cap);
        }
        assert_eq!(budget(Duration::from_micros(300), cap), Duration::ZERO);
        assert_eq!(budget(Duration::ZERO, cap), Duration::ZERO);
    }
}
