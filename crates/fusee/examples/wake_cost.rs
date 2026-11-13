//! Mass wake cost: parked runs on one shard whose timers come due together.
//! Prints wakes per second and, per wake, CPU time, allocations and (on
//! macOS) instructions retired and CPU cycles, for the whole process and for
//! the shard thread alone.
//!
//! cargo run --release -p fusee --example wake_cost -- memory <runs> [budget_mib] [shape]
//! cargo run --release -p fusee --example wake_cost -- create <dir> <runs> [shape]
//! cargo run --release -p fusee --example wake_cost -- wake <dir> [budget_mib] [shape]
//!
//! `memory` creates the runs in `memory` mode, waits for them to park and
//! measures their wake (no file I/O). `create` writes a `local` directory of
//! parked runs whose timers are already due when it returns; `wake` opens a
//! copy of it (an APFS clone, `cp -c`, is instant) and measures from the end
//! of the open until every run finished, so base and new builds can wake the
//! same data in turn. `budget_mib` 0 turns the memory budget off (the
//! default, 256, is the engine's). `shape` is `finish` (sleep, then return:
//! a wake loads, replays, finishes and writes the run's summary) or `repark`
//! (sleep twice: a wake loads, replays and parks again with a new timer).
//! `FUSEE_SAMPLE=<file>` runs macOS `sample` on the process for 4 s of the
//! wake.
use fusee::{Config, Ctx, Engine, Error, Metrics};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
/// `FUSEE_ALLOC_TRACE=1`: sample allocation call sites during the wake.
static TRACE: AtomicBool = AtomicBool::new(false);
static SITES: Mutex<Option<HashMap<String, u64>>> = Mutex::new(None);
static SAMPLED: AtomicU64 = AtomicU64::new(0);
const EVERY: u64 = 211;

thread_local! {
    static IN_TRACE: Cell<bool> = const { Cell::new(false) };
}

fn note(size: usize) {
    ALLOCS.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(size as u64, Ordering::Relaxed);
    if TRACE.load(Ordering::Relaxed)
        && !IN_TRACE.with(|t| t.get())
        && SAMPLED
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(EVERY)
        && !IN_TRACE.with(|t| t.replace(true))
    {
        let bt = std::backtrace::Backtrace::force_capture().to_string();
        let frames: Vec<&str> = bt
            .lines()
            .filter(|l| !l.trim_start().starts_with("at "))
            .map(|l| l.split_once(": ").map_or(l, |(_, f)| f).trim())
            .filter(|f| {
                ![
                    "backtrace",
                    "wake_cost::",
                    "GlobalAlloc",
                    "__rust",
                    "alloc::",
                    "Allocator",
                    "core::",
                    "hashbrown::",
                    "hash::map",
                    "RawVec",
                ]
                .iter()
                .any(|x| f.contains(x))
            })
            .take(4)
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

/// Instructions, cycles and CPU microseconds of one thread or the process.
#[derive(Clone, Copy, Default, Debug)]
struct Counts {
    instructions: u64,
    cycles: u64,
    cpu_us: u64,
}

impl std::ops::Sub for Counts {
    type Output = Counts;
    fn sub(self, o: Counts) -> Counts {
        Counts {
            instructions: self.instructions.saturating_sub(o.instructions),
            cycles: self.cycles.saturating_sub(o.cycles),
            cpu_us: self.cpu_us.saturating_sub(o.cpu_us),
        }
    }
}

fn process_counts() -> Counts {
    // SAFETY: `u` is a valid rusage.
    let cpu_us = unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut u);
        let t = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64;
        t(u.ru_utime) + t(u.ru_stime)
    };
    // Filled in further on macOS only.
    #[cfg_attr(not(target_os = "macos"), allow(unused_mut))]
    let mut c = Counts {
        cpu_us,
        ..Counts::default()
    };
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
            c.instructions = info.ri_instructions;
            c.cycles = info.ri_cycles;
        }
    }
    c
}

/// The counts of the process's thread named `name` (macOS), else zero.
fn thread_counts(name: &str) -> Counts {
    #[cfg(target_os = "macos")]
    {
        const PROC_PIDLISTTHREADIDS: libc::c_int = 28;
        const PROC_PIDTHREADID64INFO: libc::c_int = 15;
        const PROC_PIDTHREADCOUNTS: libc::c_int = 34;
        let pid = std::process::id() as libc::c_int;
        let mut ids = vec![0u64; 512];
        // SAFETY: `ids` is a writable buffer of the size passed.
        let n = unsafe {
            libc::proc_pidinfo(
                pid,
                PROC_PIDLISTTHREADIDS,
                0,
                ids.as_mut_ptr().cast(),
                (ids.len() * 8) as libc::c_int,
            )
        };
        if n <= 0 {
            return Counts::default();
        }
        ids.truncate(n as usize / 8);
        for tid in ids {
            let mut info: libc::proc_threadinfo = unsafe { std::mem::zeroed() };
            let size = std::mem::size_of::<libc::proc_threadinfo>() as libc::c_int;
            // SAFETY: `info` is a valid proc_threadinfo buffer.
            let r = unsafe {
                libc::proc_pidinfo(
                    pid,
                    PROC_PIDTHREADID64INFO,
                    tid,
                    (&raw mut info).cast(),
                    size,
                )
            };
            if r != size {
                continue;
            }
            let raw: Vec<u8> = info
                .pth_name
                .iter()
                .take_while(|c| **c != 0)
                .map(|c| *c as u8)
                .collect();
            if raw != name.as_bytes() {
                continue;
            }
            // struct proc_threadcounts: u16 len, u16, u32, then one
            // {instructions, cycles, user mach, system mach, energy} per
            // performance level.
            let mut buf = [0u64; 1 + 5 * 4];
            // SAFETY: `buf` is a writable buffer of the size passed.
            let r = unsafe {
                libc::proc_pidinfo(
                    pid,
                    PROC_PIDTHREADCOUNTS,
                    tid,
                    buf.as_mut_ptr().cast(),
                    std::mem::size_of_val(&buf) as libc::c_int,
                )
            };
            let mut c = Counts {
                cpu_us: (info.pth_user_time + info.pth_system_time) / 1000,
                ..Counts::default()
            };
            if r > 8 {
                let levels = (r as usize - 8) / 40;
                for l in 0..levels.min(4) {
                    c.instructions += buf[1 + 5 * l];
                    c.cycles += buf[2 + 5 * l];
                }
            }
            return c;
        }
    }
    let _ = name;
    Counts::default()
}

struct Snap {
    at: Instant,
    allocs: u64,
    bytes: u64,
    process: Counts,
    shard: Counts,
}

fn snap() -> Snap {
    Snap {
        at: Instant::now(),
        allocs: ALLOCS.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
        process: process_counts(),
        shard: thread_counts("fusee-shard-0"),
    }
}

const DAY_MS: u64 = 24 * 3_600_000;

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// `finish`: sleep until the input, then return. `repark`: then sleep a day.
fn register(engine: &Engine, shape: &str) -> Arc<fusee::WorkflowRef<u64, ()>> {
    let repark = engine.register("repark", |ctx: Ctx, at: u64| async move {
        ctx.sleep_until("nap", at).await?;
        ctx.sleep_until("again", at + DAY_MS).await?;
        Ok(())
    });
    let finish = engine.register("finish", |ctx: Ctx, at: u64| async move {
        ctx.sleep_until("nap", at).await?;
        Ok(())
    });
    Arc::new(if shape == "repark" { repark } else { finish })
}

async fn wait_for(engine: &Engine, what: &str, done: impl Fn(&Metrics) -> bool) -> Metrics {
    let t0 = Instant::now();
    let until = t0 + Duration::from_secs(1800);
    let mut report = t0 + Duration::from_secs(30);
    // `FUSEE_TIMELINE=1`: progress every 250 ms.
    let timeline = std::env::var_os("FUSEE_TIMELINE").is_some();
    let mut line = t0;
    loop {
        let m = engine.metrics().await.unwrap();
        if done(&m) {
            return m;
        }
        assert!(Instant::now() < until, "waiting for {what}: {m:?}");
        if Instant::now() > report {
            report += Duration::from_secs(30);
            eprintln!("still waiting for {what}: {m:?}");
        }
        if timeline && Instant::now() >= line {
            line += Duration::from_millis(250);
            eprintln!(
                "{:6.2}s loaded={} finished={} parked={} flushes={} records={} checkpoints={} max_tick_us={}",
                t0.elapsed().as_secs_f64(),
                m.runs_loaded,
                m.runs_finished,
                m.runs_parked,
                m.flushes,
                m.records,
                m.checkpoints,
                m.max_tick_us
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Starts `n` runs of `shape` sleeping until `at` and waits until all
/// are parked.
async fn create(engine: &Engine, shape: &str, n: u64, at: u64) {
    let wf = register(engine, shape);
    let t0 = Instant::now();
    let batch = 20_000u64;
    for b in (0..n).step_by(batch as usize) {
        let mut tasks = Vec::new();
        for k in b..(b + batch).min(n) {
            let (wf, engine) = (wf.clone(), engine.clone());
            tasks.push(tokio::spawn(async move {
                loop {
                    match engine.start(&wf, &format!("z{k}"), &at).await {
                        Ok(_) => return,
                        Err(Error::Busy) => tokio::time::sleep(Duration::from_millis(1)).await,
                        Err(e) => panic!("{e}"),
                    }
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    }
    wait_for(engine, "parked", |m| m.runs_parked >= n).await;
    eprintln!(
        "created {n} parked {shape} runs in {:.1} s",
        t0.elapsed().as_secs_f64()
    );
}

/// Waits until every run woke and finished (`finish`) or parked again
/// (`repark`), and prints the figures.
async fn measure(engine: &Engine, s: Snap, before: Metrics, label: &str, shape: &str, n: u64) {
    let trace = std::env::var_os("FUSEE_ALLOC_TRACE").is_some();
    TRACE.store(trace, Ordering::Relaxed);
    let sampler = std::env::var_os("FUSEE_SAMPLE").map(|file| {
        std::process::Command::new("sample")
            .arg(std::process::id().to_string())
            .arg(std::env::var("FUSEE_SAMPLE_SECS").unwrap_or_else(|_| "4".into()))
            .arg("-mayDie")
            .arg("-file")
            .arg(file)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    });
    let m = if shape == "repark" {
        wait_for(engine, "reparks", |m| {
            m.runs_parked - before.runs_parked >= n
        })
        .await
    } else {
        wait_for(engine, "wakes", |m| {
            m.runs_finished - before.runs_finished >= n
        })
        .await
    };
    TRACE.store(false, Ordering::Relaxed);
    let e = snap();
    if trace {
        let sites = SITES
            .lock()
            .ok()
            .and_then(|mut s| s.take())
            .unwrap_or_default();
        let total: u64 = sites.values().sum();
        let mut sites: Vec<_> = sites.into_iter().collect();
        sites.sort_by_key(|s| std::cmp::Reverse(s.1));
        println!(
            "allocation sites ({total} sampled, 1 in {EVERY}; counts and times are skewed by tracing):"
        );
        for (site, k) in sites.iter().take(40) {
            println!("  {:5.2}/wake  {site}", (*k * EVERY) as f64 / n as f64);
        }
    }
    if let Some(mut p) = sampler {
        let _ = p.wait();
    }
    let secs = (e.at - s.at).as_secs_f64();
    let (p, t) = (e.process - s.process, e.shard - s.shard);
    let per = |x: u64| x as f64 / n as f64;
    let loads = (m.runs_loaded - before.runs_loaded).max(1);
    println!(
        "{label} {shape} wakes={n} seconds={secs:.2} wakes_per_s={:.0} | process: cpu_us={:.2} \
         instructions={:.0} cycles={:.0} allocs={:.1} alloc_bytes={:.0} | shard thread: \
         cpu_us={:.2} instructions={:.0} cycles={:.0} | loads={} frames_per_load={:.2} \
         bytes_per_load={:.0} flushes={} budget_parks={} records={}",
        n as f64 / secs,
        per(p.cpu_us),
        per(p.instructions),
        per(p.cycles),
        per(e.allocs - s.allocs),
        per(e.bytes - s.bytes),
        per(t.cpu_us),
        per(t.instructions),
        per(t.cycles),
        m.runs_loaded - before.runs_loaded,
        (m.journal_frames_read - before.journal_frames_read) as f64 / loads as f64,
        (m.journal_bytes_read - before.journal_bytes_read) as f64 / loads as f64,
        m.flushes - before.flushes,
        m.budget_parks - before.budget_parks,
        m.records - before.records,
    );
}

fn local_cfg(dir: &str, budget_mib: u64) -> Config {
    let mut c = Config::local(dir)
        .shards(1)
        .park_after(Duration::from_millis(0))
        .memory_budget(budget_mib << 20)
        .checkpoint_interval(Duration::from_secs(3600));
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize| args.get(i).map(String::as_str);
    match arg(0) {
        Some("memory") => {
            let n: u64 = arg(1).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
            let budget: u64 = arg(2).and_then(|s| s.parse().ok()).unwrap_or(0);
            let shape = arg(3).unwrap_or("finish");
            let engine = Engine::open(
                Config::memory()
                    .shards(1)
                    .park_after(Duration::from_millis(0))
                    .memory_budget(budget << 20)
                    .checkpoint_interval(Duration::from_secs(3600)),
            )
            .await?;
            // Creation runs at 60-120 K/s on one shard; leave room.
            let at = unix_ms() + 3_000 + n / 25;
            create(&engine, shape, n, at).await;
            let now = unix_ms();
            assert!(now < at, "creation took too long");
            // Start counting a little before the timers come due; the wall
            // time counts from when they do.
            tokio::time::sleep(Duration::from_millis((at - now).saturating_sub(300))).await;
            let before = engine.metrics().await?;
            let mut s = snap();
            s.at += Duration::from_millis(at.saturating_sub(unix_ms()));
            measure(
                &engine,
                s,
                before,
                &format!("memory budget_mib={budget}"),
                shape,
                n,
            )
            .await;
            engine.shutdown().await?;
        }
        Some("create") => {
            let dir = arg(1).expect("dir");
            let n: u64 = arg(2).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
            let shape = arg(3).unwrap_or("finish");
            let at = unix_ms() + 3_000 + n / 25;
            let engine = Engine::open(local_cfg(dir, 0)).await?;
            create(&engine, shape, n, at).await;
            engine.shutdown().await?;
            // The number of runs, for `wake` (some wake before it can count them).
            std::fs::write(
                std::path::Path::new(dir).join("wake_cost.runs"),
                n.to_string(),
            )
            .map_err(|e| Error::Io(e.to_string()))?;
            let now = unix_ms();
            assert!(now < at, "creation took too long");
            std::thread::sleep(Duration::from_millis(at - now + 10));
        }
        Some("wake") => {
            let dir = arg(1).expect("dir");
            let budget: u64 = arg(2).and_then(|s| s.parse().ok()).unwrap_or(256);
            let shape = arg(3).unwrap_or("finish");
            let t0 = Instant::now();
            let engine = Engine::open(local_cfg(dir, budget)).await?;
            // The timers are due: count from the end of the open, before the
            // code is registered (runs load meanwhile but cannot run).
            let s = snap();
            let open_ms = t0.elapsed().as_secs_f64() * 1e3;
            let _wf = register(&engine, shape);
            let m = engine.metrics().await?;
            let n = std::fs::read_to_string(std::path::Path::new(dir).join("wake_cost.runs"))
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(m.parked_entries);
            eprintln!("opened {n} parked runs in {open_ms:.0} ms");
            // The counters started at zero with the open.
            let before = Metrics::default();
            measure(
                &engine,
                s,
                before,
                &format!("local budget_mib={budget}"),
                shape,
                n,
            )
            .await;
            engine.shutdown().await?;
        }
        _ => {
            eprintln!(
                "usage: wake_cost memory <runs> [budget_mib] [shape] | create <dir> <runs> [shape] | wake <dir> [budget_mib] [shape]"
            );
            std::process::exit(2);
        }
    }
    Ok(())
}
