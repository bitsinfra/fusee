//! Per-step cost of the shard runtime in `memory` mode: wall time,
//! allocations, instructions retired and CPU cycles per sequential no-op
//! step (macOS: `proc_pid_rusage`; Linux: `perf_event_open`, with branch
//! misses, L1 misses and front- and back-end stall cycles where the PMU
//! has them), plus start-to-result latency of a one-step workflow.
//!
//! cargo run --release -p fusee --example step_cost -- [steps] [reps]
//!
//! A chain of `steps` sequential steps runs on one shard; the cost of a short
//! chain is subtracted so the figures are per step, without the start and the
//! result. Allocations count every thread; the caller is blocked on the
//! result meanwhile, so they are the shard's.
use fusee::{Config, Ctx, Engine, Error};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
/// `FUSEE_ALLOC_TRACE=1`: sample allocation call sites during the long chain.
static TRACE: AtomicBool = AtomicBool::new(false);
static SITES: Mutex<Option<HashMap<String, u64>>> = Mutex::new(None);

thread_local! {
    static IN_TRACE: Cell<bool> = const { Cell::new(false) };
}

static SAMPLED: AtomicU64 = AtomicU64::new(0);

fn note(size: usize) {
    ALLOCS.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(size as u64, Ordering::Relaxed);
    if TRACE.load(Ordering::Relaxed)
        && !IN_TRACE.with(|t| t.get())
        && SAMPLED.fetch_add(1, Ordering::Relaxed).is_multiple_of(211)
        && !IN_TRACE.with(|t| t.replace(true))
    {
        let bt = std::backtrace::Backtrace::force_capture().to_string();
        let frames: Vec<&str> = bt
            .lines()
            .filter(|l| !l.trim_start().starts_with("at "))
            .map(|l| l.split_once(": ").map_or(l, |(_, f)| f).trim())
            .filter(|f| {
                !f.starts_with("std::backtrace")
                    && !f.starts_with("step_cost::note")
                    && !f.contains("GlobalAlloc")
                    && !f.starts_with("__rust")
                    && !f.starts_with("alloc::")
                    && !f.starts_with("core::")
            })
            .take(6)
            .collect();
        let key = format!("{size:>5} B  {}", frames.join(" < "));
        if let Ok(mut s) = SITES.lock() {
            *s.get_or_insert_with(HashMap::new).entry(key).or_default() += 1;
        }
        IN_TRACE.with(|t| t.set(false));
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: forwarded unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        note(new);
        // SAFETY: forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Instructions retired and CPU cycles of this process so far (macOS:
/// `proc_pid_rusage`; Linux: the `perf_event_open` counters opened by
/// [`linux::open`] before any thread started), else 0. Cycles count only
/// time on a core, so they vary less with the host's load than wall time
/// does.
fn instructions() -> (u64, u64) {
    #[cfg(target_os = "macos")]
    {
        let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
        // SAFETY: `info` is a valid rusage_info_v4 buffer for this flavor.
        let r = unsafe {
            libc::proc_pid_rusage(
                libc::getpid(),
                libc::RUSAGE_INFO_V4,
                (&raw mut info).cast::<libc::rusage_info_t>(),
            )
        };
        if r == 0 {
            return (info.ri_instructions, info.ri_cycles);
        }
    }
    #[cfg(target_os = "linux")]
    {
        let c = linux::read();
        return (c[0], c[1]);
    }
    #[allow(unreachable_code)]
    (0, 0)
}

/// Hardware counters through `perf_event_open(2)`: one counter per event for
/// the whole process (`inherit`: threads created after the open are counted
/// too, so it is opened before the runtime starts any). Kernel and user
/// space when `perf_event_paranoid` allows it, else user space only. A host
/// without a PMU (most containers and VMs) reports 0 and says why.
#[cfg(target_os = "linux")]
mod linux {
    use std::sync::OnceLock;

    /// `perf_event_attr` up to `PERF_ATTR_SIZE_VER0` (64 bytes): enough for
    /// type, config, read_format and the flag bits.
    #[repr(C)]
    #[derive(Default)]
    struct Attr {
        kind: u32,
        size: u32,
        config: u64,
        sample_period: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events: u32,
        bp_type: u32,
        config1: u64,
    }

    const HARDWARE: u32 = 0;
    const HW_CACHE: u32 = 3;
    // Flag bits of perf_event_attr.
    const INHERIT: u64 = 1 << 1;
    const EXCLUDE_KERNEL: u64 = 1 << 5;
    const EXCLUDE_HV: u64 = 1 << 6;
    // read_format: the value, then time enabled and running (multiplexing).
    const TIMES: u64 = 1 | 2;
    const PERF_FLAG_FD_CLOEXEC: libc::c_ulong = 1 << 3;

    /// Name, type and config. The first two are the ones `instructions()`
    /// returns; the rest are reported per step when they open.
    pub const EVENTS: [(&str, u32, u64); 7] = [
        ("instructions", HARDWARE, 1),
        ("cycles", HARDWARE, 0),
        ("branch-misses", HARDWARE, 5),
        // L1D | OP_READ << 8 | RESULT_MISS << 16, and the same for L1I.
        ("L1D-read-misses", HW_CACHE, 0x1_0000),
        ("L1I-read-misses", HW_CACHE, 0x1_0001),
        ("frontend-stall-cycles", HARDWARE, 7),
        ("backend-stall-cycles", HARDWARE, 8),
    ];

    pub struct Counters {
        fds: Vec<Option<i32>>,
        pub user_only: bool,
        pub why: Option<String>,
    }

    static COUNTERS: OnceLock<Counters> = OnceLock::new();

    fn open_one(kind: u32, config: u64, exclude_kernel: bool) -> Result<i32, i32> {
        let attr = Attr {
            kind,
            size: std::mem::size_of::<Attr>() as u32,
            config,
            read_format: TIMES,
            flags: INHERIT
                | if exclude_kernel {
                    EXCLUDE_KERNEL | EXCLUDE_HV
                } else {
                    0
                },
            ..Attr::default()
        };
        // SAFETY: `attr` is a valid perf_event_attr prefix of the size it
        // states; pid 0 / cpu -1 counts this process on any CPU.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &raw const attr,
                0,
                -1,
                -1,
                PERF_FLAG_FD_CLOEXEC,
            )
        };
        if fd < 0 {
            Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
        } else {
            Ok(fd as i32)
        }
    }

    /// Opens the counters. Call before any thread is started.
    pub fn open() {
        COUNTERS.get_or_init(|| {
            let mut user_only = false;
            let mut why = None;
            let first = match open_one(HARDWARE, 1, false) {
                Ok(fd) => Some(fd),
                Err(e) if e == libc::EACCES || e == libc::EPERM => {
                    user_only = true;
                    open_one(HARDWARE, 1, true)
                        .map_err(|e| why = Some(format!("perf_event_open: errno {e}")))
                        .ok()
                }
                Err(e) => {
                    why = Some(format!(
                        "perf_event_open: errno {e} (no hardware counters here)"
                    ));
                    None
                }
            };
            let mut fds = vec![first];
            if first.is_some() {
                for &(_, kind, config) in &EVENTS[1..] {
                    fds.push(open_one(kind, config, user_only).ok());
                }
            }
            Counters {
                fds,
                user_only,
                why,
            }
        });
    }

    fn value(fd: i32) -> u64 {
        let mut v = [0u64; 3];
        // SAFETY: reads at most size_of(v) bytes into `v`.
        let n = unsafe { libc::read(fd, v.as_mut_ptr().cast(), std::mem::size_of_val(&v)) };
        if n != std::mem::size_of_val(&v) as isize || v[2] == 0 {
            return 0;
        }
        // Scaled by the share of the time it was counting (multiplexed).
        (v[0] as f64 * v[1] as f64 / v[2] as f64) as u64
    }

    /// Every event's count so far (0 where it did not open).
    pub fn read() -> [u64; 7] {
        let mut out = [0; 7];
        if let Some(c) = COUNTERS.get() {
            for (o, fd) in out.iter_mut().zip(&c.fds) {
                *o = fd.map_or(0, value);
            }
        }
        out
    }

    pub fn counters() -> Option<&'static Counters> {
        COUNTERS.get()
    }

    pub fn opened(i: usize) -> bool {
        counters().is_some_and(|c| c.fds.get(i).is_some_and(Option::is_some))
    }
}

#[derive(Clone, Copy, Default)]
struct Sample {
    ns: f64,
    allocs: f64,
    bytes: f64,
    instructions: f64,
    cycles: f64,
    /// The other Linux counters ([`linux::EVENTS`] after the first two).
    extra: [f64; 5],
}

type Snap = (Instant, u64, u64, (u64, u64), [u64; 5]);

fn extra() -> [u64; 5] {
    #[cfg(target_os = "linux")]
    {
        let c = linux::read();
        return [c[2], c[3], c[4], c[5], c[6]];
    }
    #[allow(unreachable_code)]
    [0; 5]
}

fn snapshot() -> Snap {
    (
        Instant::now(),
        ALLOCS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
        instructions(),
        extra(),
    )
}

fn since(s: Snap) -> Sample {
    let e = snapshot();
    let mut extra = [0.0; 5];
    for (k, x) in extra.iter_mut().enumerate() {
        *x = e.4[k].saturating_sub(s.4[k]) as f64;
    }
    Sample {
        ns: (e.0 - s.0).as_nanos() as f64,
        allocs: (e.1 - s.1) as f64,
        bytes: (e.2 - s.2) as f64,
        instructions: e.3.0.saturating_sub(s.3.0) as f64,
        cycles: e.3.1.saturating_sub(s.3.1) as f64,
        extra,
    }
}

fn main() -> Result<(), Error> {
    // Counters first: they count only threads started after them.
    #[cfg(target_os = "linux")]
    linux::open();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(measure())
}

async fn measure() -> Result<(), Error> {
    let steps: u32 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);
    let reps: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let short = 100u32;
    let engine = Engine::open(Config::memory().shards(1)).await?;
    let chain = engine.register("chain", move |ctx: Ctx, n: u32| async move {
        for i in 0..n {
            ctx.step("noop", move || async move { Ok::<_, Error>(i) })
                .await?;
        }
        Ok(n)
    });
    let mut next = 0u64;
    let mut run = async |n: u32| -> Result<Sample, Error> {
        next += 1;
        let id = format!("c{next}");
        let s = snapshot();
        engine.start(&chain, &id, &n).await?.result().await?;
        Ok(since(s))
    };
    run(1000).await?;
    let mut best: Option<Sample> = None;
    for _ in 0..reps {
        let a = run(short).await?;
        let b = run(steps + short).await?;
        let n = f64::from(steps);
        let mut extra = [0.0; 5];
        for (k, x) in extra.iter_mut().enumerate() {
            *x = (b.extra[k] - a.extra[k]) / n;
        }
        let per = Sample {
            ns: (b.ns - a.ns) / n,
            allocs: (b.allocs - a.allocs) / n,
            bytes: (b.bytes - a.bytes) / n,
            instructions: (b.instructions - a.instructions) / n,
            cycles: (b.cycles - a.cycles) / n,
            extra,
        };
        // The repetition with the fewest cycles (else the fastest) supplies
        // every counter, so they describe one and the same run.
        let better = |p: &Sample| {
            if per.cycles > 0.0 && p.cycles > 0.0 {
                per.cycles < p.cycles
            } else {
                per.ns < p.ns
            }
        };
        best = Some(match best {
            Some(p) if !better(&p) => Sample {
                ns: p.ns.min(per.ns),
                allocs: p.allocs.min(per.allocs),
                bytes: p.bytes.min(per.bytes),
                ..p
            },
            Some(p) => Sample {
                ns: p.ns.min(per.ns),
                allocs: p.allocs.min(per.allocs),
                bytes: p.bytes.min(per.bytes),
                ..per
            },
            None => per,
        });
    }
    let p = best.unwrap_or_default();
    println!(
        "memory chain, {steps} steps x {reps}: per step {:.3} µs (best), {:.2} allocations, {:.0} bytes, {:.0} instructions, {:.0} cycles",
        p.ns / 1e3,
        p.allocs,
        p.bytes,
        p.instructions,
        p.cycles
    );
    #[cfg(target_os = "linux")]
    match linux::counters() {
        Some(c) if c.why.is_none() => {
            let mut parts = Vec::new();
            for (k, (name, _, _)) in linux::EVENTS[2..].iter().enumerate() {
                if linux::opened(k + 2) {
                    parts.push(format!("{:.2} {name}", p.extra[k]));
                }
            }
            if p.cycles > 0.0 {
                parts.push(format!("IPC {:.2}", p.instructions / p.cycles));
            }
            println!(
                "counters per step ({}): {}",
                if c.user_only {
                    "user space only"
                } else {
                    "user and kernel"
                },
                parts.join(", ")
            );
        }
        Some(c) => println!(
            "counters: unavailable ({})",
            c.why.as_deref().unwrap_or("?")
        ),
        None => {}
    }
    if std::env::var_os("FUSEE_ALLOC_TRACE").is_some() {
        TRACE.store(true, Ordering::Relaxed);
        run(steps).await?;
        TRACE.store(false, Ordering::Relaxed);
        let sites = SITES
            .lock()
            .ok()
            .and_then(|mut s| s.take())
            .unwrap_or_default();
        let total: u64 = sites.values().sum();
        let mut sites: Vec<_> = sites.into_iter().collect();
        sites.sort_by_key(|s| std::cmp::Reverse(s.1));
        println!("allocation sites ({total} sampled, 1 in 211):");
        for (site, n) in sites.iter().take(25) {
            println!(
                "  {:5.2}/step  {site}",
                *n as f64 * 211.0 / f64::from(steps)
            );
        }
    }
    // Start to result of a one-step workflow, one at a time.
    let one = engine.register("one", |ctx: Ctx, n: u32| async move {
        ctx.step("noop", move || async move { Ok::<_, Error>(n) })
            .await
    });
    let mut lat = Vec::new();
    for k in 0..3000u32 {
        let t = Instant::now();
        engine
            .start(&one, &format!("o{k}"), &k)
            .await?
            .result()
            .await?;
        lat.push(t.elapsed().as_secs_f64() * 1e6);
    }
    lat.sort_by(|a, b| a.total_cmp(b));
    let pct = |q: f64| lat[((lat.len() - 1) as f64 * q) as usize];
    println!(
        "one-step workflow start to result: p50 {:.1} µs  p90 {:.1} µs  p99 {:.1} µs",
        pct(0.5),
        pct(0.9),
        pct(0.99)
    );
    engine.shutdown().await?;
    Ok(())
}
