//! Gives memory that parking freed back to the operating system.
//!
//! A mass park (a population entering long sleeps, or the memory budget
//! parking the least recently used runs) frees loaded journals, slots and
//! code by the hundreds of megabytes. glibc's allocator keeps freed memory
//! mapped for reuse, so the process's resident size stays at its high-water
//! mark although the runs now take 32 bytes each. Shards report how far
//! their loaded memory fell; once enough has been freed, a background
//! thread calls `malloc_trim`, at most once a second, off the shard threads
//! (a trim of a large heap takes milliseconds).
//!
//! `malloc_trim` cannot return the free space at the top of a thread's
//! arena, and glibc lets that space grow: each time a large block that was
//! mapped on its own is freed (a run index rebuilt into a bigger one, a read
//! buffer), glibc raises its mmap threshold to that block's size and its
//! trim threshold to twice that, so each shard's arena keeps up to twice
//! its largest freed buffer resident for good. [`tune`] fixes both
//! thresholds once per process, which turns that adjustment off, unless the
//! environment sets them (`MALLOC_MMAP_THRESHOLD_`, `MALLOC_TRIM_THRESHOLD_`).
//! With two million parked runs on eight shards this took the resident size
//! from 203 MB to 141 MB in a Linux container (malloc counted 103 MB in use).
//! Other platforms and allocators return memory on their own; there this
//! does nothing.

/// Freed bytes that warrant a trim.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const TRIM_BYTES: u64 = 32 << 20;
/// After a trim, freed bytes that warrant another a second later.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const TAIL_BYTES: u64 = 1 << 20;
/// Blocks at least this large are mapped on their own and unmapped when
/// freed (glibc's starting value, kept from rising).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const MMAP_THRESHOLD: i32 = 256 << 10;
/// Free space at the top of an arena beyond this is returned on `free`.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const TRIM_THRESHOLD: i32 = 1 << 20;

/// Fixes glibc's mmap and trim thresholds, once per process.
pub(crate) fn tune() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    imp::tune();
}

/// A shard's sweep freed about `bytes` of loaded state.
pub(crate) fn note_freed(bytes: u64) {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    imp::note_freed(bytes);
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let _ = bytes;
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod imp {
    use super::{MMAP_THRESHOLD, TAIL_BYTES, TRIM_BYTES, TRIM_THRESHOLD};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Condvar, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    static FREED: AtomicU64 = AtomicU64::new(0);

    struct Trimmer {
        due: Mutex<bool>,
        wake: Condvar,
    }

    fn trimmer() -> Option<&'static Trimmer> {
        static T: OnceLock<Option<&'static Trimmer>> = OnceLock::new();
        *T.get_or_init(|| {
            let t: &'static Trimmer = Box::leak(Box::new(Trimmer {
                due: Mutex::new(false),
                wake: Condvar::new(),
            }));
            std::thread::Builder::new()
                .name("fusee-trim".into())
                .spawn(move || run(t))
                .ok()
                .map(|_| t)
        })
    }

    fn run(t: &'static Trimmer) {
        let mut last: Option<Instant> = None;
        loop {
            {
                let mut due = t.due.lock().unwrap_or_else(|e| e.into_inner());
                while !*due {
                    due = t.wake.wait(due).unwrap_or_else(|e| e.into_inner());
                }
                *due = false;
            }
            // Let the rest of a mass park land, and trim at most once a second.
            let wait = last.map_or(Duration::from_millis(200), |l| {
                Duration::from_secs(1).saturating_sub(l.elapsed())
            });
            std::thread::sleep(wait.max(Duration::from_millis(200)));
            loop {
                FREED.store(0, Ordering::Relaxed);
                // SAFETY: malloc_trim has no preconditions; it only returns
                // free pages of every arena to the kernel.
                unsafe {
                    libc::malloc_trim(0);
                }
                // A mass park goes on after the first trim: keep trimming
                // once a second while memory is still being freed, so the
                // last of it is returned too.
                std::thread::sleep(Duration::from_secs(1));
                if FREED.load(Ordering::Relaxed) < TAIL_BYTES {
                    break;
                }
            }
            // What the shards do not count (their callers' tasks, channels
            // and buffers) is freed over the next seconds: trim a few more
            // times, further apart.
            for secs in [2, 4, 8] {
                std::thread::sleep(Duration::from_secs(secs));
                // SAFETY: as above.
                unsafe {
                    libc::malloc_trim(0);
                }
            }
            last = Some(Instant::now());
        }
    }

    pub(super) fn tune() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let set = |name: &str| std::env::var_os(name).is_some();
            if set("MALLOC_MMAP_THRESHOLD_") || set("MALLOC_TRIM_THRESHOLD_") {
                return;
            }
            // SAFETY: mallopt only changes the allocator's parameters; it is
            // safe to call at any time from any thread.
            unsafe {
                libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD);
                libc::mallopt(libc::M_TRIM_THRESHOLD, TRIM_THRESHOLD);
            }
        });
    }

    pub(super) fn note_freed(bytes: u64) {
        if bytes == 0 {
            return;
        }
        let total = FREED.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if total >= TRIM_BYTES
            && let Some(t) = trimmer()
        {
            *t.due.lock().unwrap_or_else(|e| e.into_inner()) = true;
            t.wake.notify_one();
        }
    }
}
