//! `fusee-bench`: the harness behind every published Fusee number.
//!
//! ```text
//! fusee-bench floors <dir>                       disk flush and loopback round trip
//! fusee-bench write-cpu <dir> [seconds]          CPU per small O_DIRECT durable write (Linux)
//! fusee-bench run <workload> [options]           one workload, JSON on stdout
//! fusee-bench all [options]                      the standard matrix
//!
//! options: --mode local|fsync|memory|server  --dir D  --shards N  --trials T
//!          --rate R (open loop, starts/s)  --seconds S  --steps K
//!          --server host:port|unix:/path.sock  (server mode; default 127.0.0.1:7600)
//!          --stall-us US  --stall-trace FILE   (engine stall tracer, any cell)
//!          --rates R1,R2,..  --window-seconds W (soak)
//!          --growth G --past N (saturate: rate multiplier, points past the first miss)
//!          --concurrency N (service-concurrent)
//!
//! workloads: chain fanout fanout-1000 fanout-10000 service service-concurrent
//!            open max saturate soak payload children signal sleepers cost
//! ```
//!
//! Every result line is JSON with the machine, the engine configuration and
//! the durability boundary, so a number never travels without what it means.
//! Latencies are HdrHistograms. Open-loop latency is measured from each
//! workflow's *scheduled* start, so queueing is never hidden (coordinated
//! omission).

use fusee::{Config, Ctx, Engine, Error, FlushMethod};
use fusee_server::replicated::{ReplicaConfig, ReplicaNode};
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

mod cells;
mod cpu;
mod external;
#[cfg(target_os = "linux")]
mod linux_floors;
#[cfg(target_os = "macos")]
mod mac_floors;
mod segment_open;
mod stalls;

#[derive(Clone, Debug)]
struct Opts {
    mode: String,
    dir: String,
    shards: u32,
    trials: u32,
    rate: f64,
    seconds: f64,
    steps: u32,
    server: Option<String>,
    usd_per_hour: Option<f64>,
    replica_id: Option<u8>,
    peers: Vec<String>,
    replicated_config: Option<Config>,
    extra: cells::Extra,
}

fn parse(args: &[String]) -> Result<Opts, Error> {
    let mut o = Opts {
        mode: "local".into(),
        dir: std::env::temp_dir()
            .join("fusee-bench")
            .display()
            .to_string(),
        shards: 4,
        trials: 5,
        rate: 1000.0,
        seconds: 5.0,
        steps: 10,
        server: None,
        usd_per_hour: None,
        replica_id: None,
        peers: Vec::new(),
        replicated_config: None,
        extra: Default::default(),
    };
    let mut i = 0;
    while i < args.len() {
        let invalid = || Error::failed(format!("invalid or missing value for {}", args[i]));
        let v = args.get(i + 1).cloned().ok_or_else(invalid)?;
        match args[i].as_str() {
            "--mode" => o.mode = v,
            "--dir" => o.dir = v,
            "--shards" => o.shards = v.parse().map_err(|_| invalid())?,
            "--trials" => o.trials = v.parse().map_err(|_| invalid())?,
            "--rate" => o.rate = v.parse().map_err(|_| invalid())?,
            "--seconds" => o.seconds = v.parse().map_err(|_| invalid())?,
            "--steps" => o.steps = v.parse().map_err(|_| invalid())?,
            "--server" => o.server = Some(v),
            "--usd-per-hour" => o.usd_per_hour = Some(v.parse().map_err(|_| invalid())?),
            "--replica-id" => o.replica_id = Some(v.parse().map_err(|_| invalid())?),
            "--peers" => o.peers = v.split(',').map(str::to_owned).collect(),
            flag if cells::Extra::accepts(flag) => o.extra.set(flag, &v).map_err(|_| invalid())?,
            unknown => {
                return Err(Error::failed(format!(
                    "unknown benchmark option '{unknown}'"
                )));
            }
        }
        i += 2;
    }
    if o.shards == 0
        || o.trials == 0
        || o.steps == 0
        || !o.rate.is_finite()
        || o.rate <= 0.0
        || !o.seconds.is_finite()
        || o.seconds <= 0.0
        || o.usd_per_hour.is_some_and(|p| !p.is_finite() || p <= 0.0)
    {
        return Err(Error::Failed(
            "counts, durations, rates and prices must be positive".into(),
        ));
    }
    Ok(o)
}

fn sh(cmd: &str, args: &[&str]) -> String {
    std::process::Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn machine() -> Value {
    let cpu = if cfg!(target_os = "macos") {
        sh("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("model name"))
                    .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_owned())
            })
            .unwrap_or_default()
    };
    let load = sh("uptime", &[]);
    json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "cpu": cpu,
        "cores": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        "kernel": sh("uname", &["-r"]),
        "host_load": load,
        "fusee": env!("CARGO_PKG_VERSION"),
        "revision": std::env::var("FUSEE_REVISION").unwrap_or_else(|_| sh("git", &["rev-parse", "HEAD"])),
        "instance_type": std::env::var("FUSEE_INSTANCE_TYPE").ok(),
        "zone": std::env::var("FUSEE_ZONE").ok(),
    })
}

fn durability(mode: &str) -> &'static str {
    match mode {
        "local" if cfg!(target_os = "macos") => {
            "local: F_FULLFSYNC before outputs and before each step's function"
        }
        "local" => "local: fdatasync before outputs and before each step's function",
        "fsync" => "DEVELOPMENT ONLY on macOS: plain fsync (does not flush the drive cache)",
        "memory" => "none (memory mode)",
        "server" => "server: the server's flush before outputs and step functions",
        "replicated" => {
            "replicated: a quorum of replicas flushes before outputs and step functions"
        }
        _ => "unknown",
    }
}

/// How a server-mode worker reaches its server; results record it, since a
/// Unix socket and loopback TCP have different floors.
fn transport(o: &Opts) -> Option<&'static str> {
    if o.mode != "server" {
        return None;
    }
    let addr = o.server.as_deref().unwrap_or("127.0.0.1:7600");
    Some(if addr.starts_with("unix:") {
        "unix"
    } else if addr.starts_with("replicas://") {
        "replicas"
    } else {
        "tcp"
    })
}

fn hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3)
        .unwrap_or_else(|_| Histogram::new(3).unwrap_or_else(|_| unreachable_hist()))
}

/// Histograms recorded from many threads: each thread records into its own
/// shard (no shared lock between shard threads), merged at the end.
/// One histogram per thread slot, each on cache lines of its own: slots
/// side by side made shard threads that lock neighbouring slots share a
/// line. A slot's histogram is made by the first thread that records into
/// it: sixty-four made and cleared up front are 12 MiB of resident memory
/// before any workflow runs, which a workload that reports the process's
/// memory (the sleepers') would count as the engine's.
struct ShardedHist(Vec<Padded<Mutex<Option<Histogram<u64>>>>>);

#[repr(align(128))]
struct Padded<T>(T);

impl<T> std::ops::Deref for Padded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl ShardedHist {
    const SHARDS: usize = 64;

    fn new() -> Self {
        Self(
            (0..Self::SHARDS)
                .map(|_| Padded(Mutex::new(None)))
                .collect(),
        )
    }

    fn with<R>(&self, f: impl FnOnce(&mut Histogram<u64>) -> R) -> R {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        thread_local! {
            static SLOT: usize = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let i = SLOT.with(|s| *s) % Self::SHARDS;
        let mut h = self.0[i].lock().unwrap_or_else(|e| e.into_inner());
        f(h.get_or_insert_with(hist))
    }

    fn clear(&self) {
        for h in &self.0 {
            if let Some(h) = h.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
                h.reset();
            }
        }
    }

    fn merged(&self) -> Histogram<u64> {
        let mut out = hist();
        for h in &self.0 {
            if let Some(h) = h.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
                let _ = out.add(h);
            }
        }
        out
    }
}

/// Asks the kernel to wake this thread from a sleep as close to the time
/// asked for as it can: Linux otherwise lets a thread's timers fire up to
/// 50 µs late so that it can batch them.
fn precise_sleeps() {
    #[cfg(target_os = "linux")]
    // SAFETY: a plain system call that changes only the calling thread's
    // timer slack; a failure leaves the default.
    unsafe {
        let nanoseconds: libc::c_ulong = 1;
        libc::prctl(libc::PR_SET_TIMERSLACK, nanoseconds);
    }
}

/// `prefix-k` without `format!`.
fn arrival_id(prefix: &str, k: u64) -> String {
    let mut id = String::with_capacity(prefix.len() + 21);
    id.push_str(prefix);
    id.push('-');
    let mut digits = [0u8; 20];
    let mut n = k;
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    id.push_str(std::str::from_utf8(&digits[at..]).unwrap_or("0"));
    id
}

#[allow(clippy::panic)]
fn unreachable_hist() -> Histogram<u64> {
    panic!("histogram")
}

fn summary(h: &Histogram<u64>) -> Value {
    json!({
        "count": h.len(),
        "p50_us": h.value_at_quantile(0.50),
        "p90_us": h.value_at_quantile(0.90),
        "p99_us": h.value_at_quantile(0.99),
        "p999_us": h.value_at_quantile(0.999),
        "max_us": h.max(),
        "mean_us": (h.mean() * 10.0).round() / 10.0,
        "histogram_us": h.iter_recorded().map(|bucket| [bucket.value_iterated_to(), bucket.count_at_value()]).collect::<Vec<_>>(),
    })
}

async fn open_engine(o: &Opts, tag: &str) -> Result<Engine, Error> {
    let dir = format!("{}/{tag}", o.dir);
    let mut cfg = match o.mode.as_str() {
        "memory" => Config::memory(),
        "server" => {
            let addr = o.server.clone().unwrap_or_else(|| "127.0.0.1:7600".into());
            tokio::task::spawn_blocking(move || fusee_server::remote_config(&addr))
                .await
                .map_err(|e| Error::Io(e.to_string()))??
        }
        "local" | "fsync" => {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            Config::local(&dir)
        }
        "replicated" => o
            .replicated_config
            .clone()
            .ok_or_else(|| Error::Failed("no ready replica primary".into()))?,
        other => {
            return Err(Error::failed(format!(
                "unsupported benchmark mode '{other}'"
            )));
        }
    };
    if o.mode != "server" && o.mode != "replicated" {
        cfg = cfg.shards(o.shards);
    }
    if o.mode == "fsync" {
        cfg.local.flush = FlushMethod::Fsync;
    }
    // Experiments on a disk's flush paths (the gp3 case of bench/aws).
    match std::env::var("FUSEE_BENCH_FLUSH").as_deref() {
        Ok("fdatasync") => cfg.local.flush = FlushMethod::Fdatasync,
        Ok("dsync") => cfg.local.flush = FlushMethod::Dsync,
        _ => {}
    }
    if std::env::var("FUSEE_BENCH_DIRECT").as_deref() == Ok("0") {
        cfg.local.direct_io = false;
    }
    // FUSEE_BENCH_INLINE=0: no shard writes the log on its own thread;
    // every write goes through the log's owner (or I/O threads).
    if std::env::var("FUSEE_BENCH_INLINE").as_deref() == Ok("0") {
        cfg = cfg.inline_flush(false);
    }
    if let Some(us) = std::env::var("FUSEE_BENCH_INLINE_MAX_US")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        cfg.inline_flush_max = std::time::Duration::from_micros(us);
    }
    Engine::open(cfg).await
}

/// A remote journal survives worker restarts and successive benchmark cases.
fn run_prefix() -> String {
    static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let serial = SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!(
        "bench-{}-{}-{serial}",
        fusee::local::unix_ms(),
        std::process::id()
    )
}

type Stamps = Arc<Mutex<Vec<(Instant, Instant)>>>;

fn chain_wf(engine: &Engine, stamps: Stamps) -> fusee::WorkflowRef<u32, u32> {
    engine.register("chain", move |ctx: Ctx, n: u32| {
        let stamps = stamps.clone();
        async move {
            let mut value = 0u32;
            for _ in 0..n {
                let s = stamps.clone();
                value = ctx
                    .step("noop", move || {
                        let s = s.clone();
                        async move {
                            let start = Instant::now();
                            let next = value + 1;
                            let mut marks = s.lock().unwrap();
                            if marks.capacity() > marks.len() {
                                marks.push((start, Instant::now()));
                            }
                            Ok::<_, Error>(next)
                        }
                    })
                    .await?;
            }
            Ok(value)
        }
    })
}

/// Workload 1: sequential no-op chains; step overhead (closed loop).
async fn w_chain(o: &Opts) -> Result<Value, Error> {
    let mut out = Vec::new();
    for &n in &[1u32, 10, 100, 1000] {
        let window = stalls::Window::begin();
        let mut step = hist();
        let mut whole = hist();
        let mut observations = Vec::new();
        for t in 0..o.trials {
            let engine = open_engine(o, &format!("chain-{n}-{t}")).await?;
            let stamps: Stamps = Arc::new(Mutex::new(Vec::new()));
            let wf = chain_wf(&engine, stamps.clone());
            let prefix = run_prefix();
            let runs = if o.mode == "local" && cfg!(target_os = "macos") {
                3
            } else {
                20
            };
            let runs = std::env::var("FUSEE_BENCH_CHAIN_RUNS")
                .ok()
                .map_or(Ok(runs), |s| s.parse::<u32>())
                .map_err(|_| Error::Failed("invalid FUSEE_BENCH_CHAIN_RUNS".into()))?;
            if runs == 0 {
                return Err(Error::Failed("chain requires at least one workflow".into()));
            }
            // The first execution on each ID includes any cold shard costs.
            // Run that same ID again so the measured workflow uses a warm shard.
            for cold in [true, false] {
                for r in 0..runs {
                    let id = format!("{prefix}-{r}");
                    *stamps.lock().unwrap() = Vec::with_capacity(n as usize);
                    let started = Instant::now();
                    let value = engine.start(&wf, &id, &n).await?.result().await?;
                    let elapsed = started.elapsed().as_nanos() as u64;
                    let marks = stamps.lock().unwrap();
                    if value != n || marks.len() != n as usize {
                        return Err(Error::Failed(
                            "incorrect chain result or sample population".into(),
                        ));
                    }
                    let gaps: Vec<u64> = marks
                        .windows(2)
                        .map(|w| (w[1].0 - w[0].1).as_nanos() as u64)
                        .collect();
                    if !cold {
                        let _ = whole.record((elapsed / 1000).max(1));
                        for gap in &gaps {
                            let _ = step.record((gap / 1000).max(1));
                        }
                    }
                    observations.push(json!({"id":id,"trial":t,"first_execution":cold,
                        "shard":fusee::core::shard::shard_of(&id,engine.shards()),
                        "workflow_ns":elapsed,"transition_samples_ns":gaps}));
                }
            }
            engine.shutdown().await?;
        }
        out.push(json!({"steps": n, "step_to_step": summary(&step), "workflow": summary(&whole), "observations": observations,
            "stalls": window.summary(None)}));
    }
    Ok(json!(out))
}

/// Workload 2: fan-out of `width` parallel steps (100; 1,000 and 10,000 for
/// the wide cells) and fan-in.
async fn w_fanout(o: &Opts, width: u32) -> Result<Value, Error> {
    let tag = if width == 100 {
        "fanout".to_owned()
    } else {
        format!("fanout-{width}")
    };
    let window = stalls::Window::begin();
    let engine = open_engine(o, &tag).await?;
    let wf = engine.register("fan", |ctx: Ctx, n: u32| async move {
        let branches: Vec<_> = (0..n)
            .map(|i| {
                let c = ctx.clone();
                async move { c.step("leg", || async move { Ok::<_, Error>(i) }).await }
            })
            .collect();
        let r = ctx.all(branches).await;
        let s: u32 = r.into_iter().collect::<Result<Vec<_>, _>>()?.iter().sum();
        ctx.step("join", || async move { Ok::<_, Error>(s) }).await
    });
    let mut h = hist();
    let prefix = run_prefix();
    let before = cpu::snapshot();
    for r in 0..(10 * o.trials) {
        let t0 = Instant::now();
        let sum = engine
            .start(&wf, &format!("{prefix}-f{r}"), &width)
            .await?
            .result()
            .await?;
        let _ = h.record(t0.elapsed().as_micros() as u64);
        if u64::from(sum) != u64::from(width) * u64::from(width - 1) / 2 {
            return Err(Error::Failed("incorrect fan-in result".into()));
        }
    }
    let after = cpu::snapshot();
    let m = engine.metrics().await?;
    engine.shutdown().await?;
    let steps = f64::from(10 * o.trials) * f64::from(width + 1);
    Ok(json!({"fanout": width, "workflow": summary(&h),
        "us_per_branch": (h.mean() / f64::from(width) * 1000.0).round() / 1000.0,
        "flushes": m.flushes, "inline_flushes": m.inline_flushes,
        "records_per_flush": m.flushed_records as f64 / m.flushes.max(1) as f64,
        "cpu": cpu::report(&before, &after, steps), "stalls": window.summary(Some(&m))}))
}

/// Workload 3: steps calling a mock service with 1 ms and 50 ms latency.
/// Engine overhead is the gap from one step function's return to the next
/// one's start, measured inside the functions (timer overshoot in the mock
/// service is not the engine's).
async fn w_service(o: &Opts) -> Result<Value, Error> {
    let mut out = Vec::new();
    for &lat in &[1u64, 50] {
        let window = stalls::Window::begin();
        let engine = open_engine(o, &format!("svc-{lat}")).await?;
        let marks: Arc<Mutex<Vec<(Instant, Instant)>>> = Arc::default();
        let m2 = marks.clone();
        let wf = engine.register("svc", move |ctx: Ctx, n: u32| {
            let marks = m2.clone();
            async move {
                for _ in 0..n {
                    let marks = marks.clone();
                    ctx.step("call", move || {
                        let marks = marks.clone();
                        async move {
                            let t0 = Instant::now();
                            tokio::time::sleep(Duration::from_millis(lat)).await;
                            if let Ok(mut m) = marks.lock() {
                                m.push((t0, Instant::now()));
                            }
                            Ok::<_, Error>(())
                        }
                    })
                    .await?;
                }
                Ok(n)
            }
        });
        let mut h = hist();
        let prefix = run_prefix();
        for r in 0..o.trials.max(3) {
            if let Ok(mut m) = marks.lock() {
                m.clear();
            }
            engine
                .start(&wf, &format!("{prefix}-s{r}"), &10)
                .await?
                .result()
                .await?;
            let m = marks.lock().map(|m| m.clone()).unwrap_or_default();
            for w in m.windows(2) {
                let _ = h.record((w[1].0 - w[0].1).as_micros().max(1) as u64);
            }
        }
        let m = engine.metrics().await?;
        engine.shutdown().await?;
        out.push(
            json!({"service_ms": lat, "engine_overhead_per_step": summary(&h),
            "stalls": window.summary(Some(&m))}),
        );
    }
    Ok(json!(out))
}

/// Workloads 4 and 5: open-loop arrivals; workflow latency from the
/// scheduled start; start-to-first-step latency; throughput.
async fn w_open(o: &Opts, rate: f64) -> Result<Value, Error> {
    let stall_window = stalls::Window::begin();
    let engine = open_engine(o, &format!("open-{rate}")).await?;
    // Statics, cleared for each point (one runs at a time): the workflow
    // body and the result callbacks run on the shard threads, and a
    // reference count that every shard clones for each run is a contended
    // cache line, which the engine's CPU would be charged for.
    static FIRST: OnceLock<ShardedHist> = OnceLock::new();
    static GAPS: OnceLock<ShardedHist> = OnceLock::new();
    static LATENCY: OnceLock<ShardedHist> = OnceLock::new();
    // Runs not yet answered, and the wake for the last answer.
    static LEFT: AtomicU64 = AtomicU64::new(0);
    static ANSWERED: tokio::sync::Notify = tokio::sync::Notify::const_new();
    let first = FIRST.get_or_init(ShardedHist::new);
    let transitions = GAPS.get_or_init(ShardedHist::new);
    let latency = LATENCY.get_or_init(ShardedHist::new);
    for h in [first, transitions, latency] {
        h.clear();
    }
    let start = Instant::now() + Duration::from_millis(50);
    let windows = cells::Windows::new(
        o.extra.window_seconds,
        o.seconds,
        fusee::local::unix_ms() + 50,
    );
    let w2 = windows.clone();
    let steps = o.steps;
    let wf = engine.register("open", move |ctx: Ctx, (n, offset_ns): (u32, u64)| {
        let windows = w2.clone();
        let scheduled = start + Duration::from_nanos(offset_ns);
        async move {
            let previous_end = std::cell::Cell::new(None::<Instant>);
            let samples = std::cell::RefCell::new(Vec::with_capacity(n.saturating_sub(1) as usize));
            let mut value = 0u32;
            for _ in 0..n {
                value = ctx
                    .step("s", || async {
                        let start = Instant::now();
                        if let Some(end) = previous_end.get() {
                            samples
                                .borrow_mut()
                                .push((start - end).as_micros().max(1) as u64);
                        } else if let Some(first) = FIRST.get() {
                            first.with(|h| {
                                let _ = h.record((start - scheduled).as_micros().max(1) as u64);
                            });
                        }
                        // The next gap is measured from this step's start:
                        // the step's own body (an add and a push) is a few
                        // nanoseconds, and a second clock read a step would
                        // cost a few percent of a shard's time at high rates.
                        previous_end.set(Some(start));
                        Ok::<_, Error>(value + 1)
                    })
                    .await?;
            }
            let samples = samples.into_inner();
            windows.steps(offset_ns, &samples);
            if let Some(gaps) = GAPS.get() {
                gaps.with(|h| {
                    for us in samples {
                        let _ = h.record(us);
                    }
                });
            }
            Ok(value)
        }
    });
    let wf = Arc::new(wf);
    let total = (rate * o.seconds) as u64;
    if total == 0 {
        return Err(Error::Failed("offered load schedules zero arrivals".into()));
    }
    LEFT.store(total, Ordering::Release);
    let answered = || {
        if LEFT.fetch_sub(1, Ordering::AcqRel) == 1 {
            ANSWERED.notify_one();
        }
    };
    let prefix = run_prefix();
    // Pace one producer, independent of completions. Backlog remains visible
    // because both latency clocks use the original scheduled arrival.
    // Its thread is named, and outlives its arrivals until the end snapshot,
    // so its CPU is counted as the generator's and not the engine's.
    let cpu_before = cpu::snapshot();
    let (release, released) = std::sync::mpsc::channel::<()>();
    // Above 100,000 starts a second, two producers take alternate arrivals
    // (one thread's spawns and sends would take a third of the CPU at
    // 8 M steps/s; `harness_us_per_step` reports what they cost).
    let producers: u64 = if rate > 100_000.0 { 2 } else { 1 };
    let (release2, released2) = std::sync::mpsc::channel::<()>();
    let releases = [release, release2];
    let produce = {
        let engine = engine.clone();
        let windows = windows.clone();
        move |first_k: u64, released: std::sync::mpsc::Receiver<()>| {
            // Latency is measured from each arrival's scheduled time, so a
            // late producer is counted as a slow engine. A sleep alone
            // returns late by the thread's timer slack (50 µs by default on
            // Linux) and a wake-up: at 1,000 starts a second that is about
            // 45 µs in every start-to-first-step and workflow latency. So
            // the thread asks for the smallest timer slack, and arrivals at
            // least SPARSE apart are slept to within SPIN of their time and
            // spun to, which keeps this thread busy 15% of the time at most.
            // Closer arrivals are slept to, at least a tick at a time, and
            // then every arrival that came due is issued: spinning to each
            // would keep a core busy on a host whose shards need every core.
            // FUSEE_BENCH_SPIN=1 spins to each arrival at any rate.
            const TICK: Duration = Duration::from_micros(50);
            const SPIN: Duration = Duration::from_micros(75);
            const SPARSE: Duration = Duration::from_micros(500);
            precise_sleeps();
            let gap = Duration::from_secs_f64(producers as f64 / rate);
            let spin = gap >= SPARSE || std::env::var_os("FUSEE_BENCH_SPIN").is_some();
            for k in (first_k..total).step_by(producers as usize) {
                let offset = Duration::from_secs_f64(k as f64 / rate);
                let at = start + offset;
                let remaining = at.saturating_duration_since(Instant::now());
                if spin {
                    if remaining > SPIN {
                        std::thread::sleep(remaining - SPIN);
                    }
                    while Instant::now() < at {
                        std::hint::spin_loop();
                    }
                } else if !remaining.is_zero() {
                    std::thread::sleep(remaining.max(TICK));
                }
                let id = arrival_id(&prefix, k);
                let offset_ns = offset.as_nanos() as u64;
                let windows = windows.clone();
                // One call: the start and its result, answered on the
                // run's shard thread, with no task or wake per run.
                let sent = engine.execute_then(&*wf, &id, &(steps, offset_ns), move |result| {
                    if result.is_ok_and(|value: u32| value == steps) {
                        let us = at.elapsed().as_micros().max(1) as u64;
                        if let Some(latency) = LATENCY.get() {
                            latency.with(|h| {
                                let _ = h.record(us);
                            });
                        }
                        windows.workflow(offset_ns, us);
                    }
                    answered();
                });
                if sent.is_err() {
                    answered();
                }
            }
            let _ = released.recv();
        }
    };
    let mut threads = Vec::new();
    for (first_k, released) in [(0, released), (1, released2)]
        .into_iter()
        .take(producers as usize)
    {
        let produce = produce.clone();
        threads.push(
            std::thread::Builder::new()
                .name("fusee-bench-gen".into())
                .spawn(move || produce(first_k, released))?,
        );
    }
    drop(produce);
    // Every arrival is answered, by its result or by a refused start.
    while LEFT.load(Ordering::Acquire) > 0 {
        ANSWERED.notified().await;
    }
    let elapsed = start.elapsed().as_secs_f64().max(o.seconds);
    let cpu_after = cpu::snapshot();
    for r in &releases {
        let _ = r.send(());
    }
    for producer in threads {
        producer
            .join()
            .map_err(|_| Error::Failed("arrival thread panicked".into()))?;
    }
    let m = engine.metrics().await?;
    engine.shutdown().await?;
    let lat = latency.merged();
    let ok = lat.len();
    let h = summary(&lat);
    let f = summary(&first.merged());
    let (stall_records, _) = stall_window.records();
    Ok(json!({
        "offered_starts_per_s": rate,
        "steps_per_workflow": steps,
        "completed": ok,
        "offered": total,
        "step_to_step": summary(&transitions.merged()),
        "arrival_threads": producers,
        "achieved_steps_per_s": (ok as f64 * f64::from(steps)) / elapsed,
        "workflow_latency_from_schedule": h,
        "start_to_first_step": f,
        "flushes": m.flushes,
        "records_per_flush": m.flushed_records as f64 / m.flushes.max(1) as f64,
        // A section's time from its submission to its durability, and a
        // run's wait at its gate, on average: where a step's time between
        // steps goes under load.
        "flush_us_mean": m.flush_us_total as f64 / m.flushes.max(1) as f64,
        "gate_wait_us_mean": m.gate_wait_us_total as f64 / m.gate_waits.max(1) as f64,
        "gate_waits_per_step": m.gate_waits as f64 / (ok as f64 * f64::from(steps)).max(1.0),
        "checkpoints": m.checkpoints,
        // The shared log's device writes: how many, how many shards'
        // sections each carried, and the bytes they wrote against the
        // records' (headers and padding).
        "log": {
            "writes": m.log_writes,
            "writes_per_s": m.log_writes as f64 / elapsed,
            "sections_per_write": m.flushes as f64 / m.log_writes.max(1) as f64,
            "device_bytes": m.log_device_bytes,
            "record_bytes": m.log_bytes,
            "device_bytes_per_record_byte": m.log_device_bytes as f64 / m.log_bytes.max(1) as f64,
            "segment_rolls": m.log_rolls,
            "owner": m.log_owner == 1,
        },
        "cpu": cpu::report(&cpu_before, &cpu_after, ok as f64 * f64::from(steps)),
        "stalls": stall_window.summary(Some(&m)),
        "windows": windows.report(&stall_records),
    }))
}

/// Workload 4: highest tested sustained rate with measured transition p99 <= 1 ms.
async fn w_max(o: &Opts) -> Result<Value, Error> {
    if o.steps < 2 {
        return Err(Error::Failed(
            "max requires at least two dependent steps".into(),
        ));
    }
    let mut rate = o.rate;
    let mut best = Value::Null;
    let mut sweep = Vec::new();
    for _ in 0..24 {
        let r = w_open(o, rate).await?;
        let p99 = r["step_to_step"]["p99_us"].as_u64().unwrap_or(u64::MAX);
        let done = r["offered"] == r["completed"]
            && r["step_to_step"]["count"].as_u64()
                == r["offered"].as_u64().map(|n| n * u64::from(o.steps - 1))
            && r["start_to_first_step"]["count"] == r["offered"]
            && r["achieved_steps_per_s"].as_f64().unwrap_or(0.0)
                >= rate * f64::from(o.steps) * 0.99;
        let t = &r["step_to_step"];
        eprintln!(
            "  offered {rate:.0} starts/s: transition p50/p99/p99.9/max {}/{p99}/{}/{} µs, completed {}",
            t["p50_us"], t["p999_us"], t["max_us"], r["completed"]
        );
        sweep.push(r.clone());
        if p99 <= 1000 && done {
            best = r;
            rate *= 2.0;
        } else {
            break;
        }
    }
    Ok(json!({"best":best, "trials":sweep}))
}

/// Workload 6: many sleeping workflows: memory, and timer accuracy when
/// they wake together.
async fn w_sleepers(o: &Opts, n: u64) -> Result<Value, Error> {
    let engine = open_engine(o, "sleepers").await?;
    // Per shard thread (`ShardedHist`): one histogram behind one lock for
    // every shard's wakes made eight shards wake about twice as fast as
    // one instead of eight times. A static, so the code clones no
    // reference count every shard shares on each wake.
    static WAKE_ERR: std::sync::OnceLock<ShardedHist> = std::sync::OnceLock::new();
    let wake_err = WAKE_ERR.get_or_init(ShardedHist::new);
    wake_err.clear();
    let wf = engine.register("sleeper", move |ctx: Ctx, at: u64| async move {
        ctx.sleep_until("nap", at).await?;
        let late = fusee::local::unix_ms().saturating_sub(at);
        if let Some(w) = WAKE_ERR.get() {
            w.with(|h| {
                let _ = h.record(late.max(1) * 1000);
            });
        }
        Ok(())
    });
    let rss0 = rss_kb();
    let at = fusee::local::unix_ms() + 30_000 + n / 20;
    let wf = Arc::new(wf);
    let batch = 5000u64;
    let t0 = Instant::now();
    let prefix = run_prefix();
    for b in (0..n).step_by(batch as usize) {
        let mut tasks = Vec::new();
        for k in b..(b + batch).min(n) {
            let (wf, engine) = (wf.clone(), engine.clone());
            let id = format!("{prefix}-z{k}");
            tasks.push(tokio::spawn(async move {
                engine.start(&wf, &id, &at).await.map(|_| ())
            }));
        }
        for t in tasks {
            t.await.map_err(|e| Error::failed(e.to_string()))??;
        }
    }
    let started = t0.elapsed().as_secs_f64();
    // Let them park.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rss1 = rss_kb();
    let m = engine.metrics().await?;
    let now = fusee::local::unix_ms();
    if now >= at
        || m.parked_entries != n
        || m.runs_finished != 0
        || m.runs_started != n
        || rss1 == 0
    {
        return Err(Error::failed(format!(
            "sleep population check failed before measurement: requested={n} started={} parked_entries={} finished={} now={now} wake_at={at} rss_kb={rss1}",
            m.runs_started, m.parked_entries, m.runs_finished
        )));
    }
    tokio::time::sleep(Duration::from_millis(at - now)).await;
    let mut finished = 0u64;
    for _ in 0..600 {
        finished = engine.metrics().await?.runs_finished;
        if finished >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    engine.shutdown().await?;
    if m.runs_parked != n || finished != n {
        return Err(Error::failed(format!(
            "requested {n} sleepers, parked {}, finished {finished}",
            m.runs_parked
        )));
    }
    let merged = wake_err.merged();
    let w = summary(&merged);
    Ok(json!({
        "workflows": n,
        "start_seconds": started,
        "parked": m.parked_entries,
        "within_2gb_at_observation": rss1 * 1024 <= 2_000_000_000,
        "rss_kb_before": rss0,
        "rss_kb_parked": rss1,
        "bytes_per_parked_workflow": ((rss1.saturating_sub(rss0)) * 1024) as f64 / n.max(1) as f64,
        "finished": finished,
        "wake_samples": merged.len(),
        "lateness_at_wake": w,
    }))
}

/// Workload 7: a signal from outside to resumed code.
async fn w_signal(o: &Opts) -> Result<Value, Error> {
    let engine = open_engine(o, "signal").await?;
    let got: Arc<Mutex<Option<Instant>>> = Arc::default();
    let g2 = got.clone();
    let wf = engine.register("sig", move |ctx: Ctx, _: ()| {
        let g = g2.clone();
        async move {
            ctx.wait_for::<u32>("go").await?;
            ctx.step("seen", || {
                let g = g.clone();
                async move {
                    if let Ok(mut v) = g.lock() {
                        *v = Some(Instant::now());
                    }
                    Ok::<_, Error>(())
                }
            })
            .await
        }
    });
    let mut h = hist();
    let prefix = run_prefix();
    for r in 0..(5 * o.trials) {
        let run = engine.start(&wf, &format!("{prefix}-g{r}"), &()).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(mut v) = got.lock() {
            *v = None;
        }
        let t0 = Instant::now();
        run.signal("go", &1).await?;
        run.result().await?;
        if let Some(t) = got.lock().ok().and_then(|v| *v) {
            let _ = h.record((t - t0).as_micros().max(1) as u64);
        }
    }
    let m = engine.metrics().await?;
    engine.shutdown().await?;
    // How the log was written: a flush that is not inline went through the
    // log's threads, a hand-off there and back.
    Ok(json!({"signal_to_code": summary(&h),
        "flushes": m.flushes, "inline_flushes": m.inline_flushes}))
}

/// CPU seconds per million durable steps at a load point (cost basis).
async fn w_cost(o: &Opts) -> Result<Value, Error> {
    let mut out = Vec::new();
    for &rate in &[100.0, 1000.0, 10_000.0] {
        let (u0, s0) = cpu_seconds();
        let r = w_open(o, rate).await?;
        let (u1, s1) = cpu_seconds();
        let steps = r["completed"].as_f64().unwrap_or(0.0) * f64::from(o.steps);
        let cpu = (u1 - u0) + (s1 - s0);
        out.push(json!({
            "offered_starts_per_s": rate,
            "steps": steps,
            "cpu_seconds": cpu,
            "cpu_seconds_per_million_steps": if steps > 0.0 { cpu * 1e6 / steps } else { 0.0 },
            // Same steps, measured from the first arrival to the last
            // completion: the whole process and the engine's threads alone.
            "process_cpu_us_per_step": r["cpu"]["process_us_per_step"],
            "engine_cpu_us_per_step": r["cpu"]["engine_us_per_step"],
            "engine_dedicated_cpu_us_per_step": r["cpu"]["engine_dedicated_us_per_step"],
            "cpu": r["cpu"],
            "achieved_steps_per_s": r["achieved_steps_per_s"],
            "usd_per_hour": o.usd_per_hour,
            "usd_per_million_steps": o.usd_per_hour.zip(r["achieved_steps_per_s"].as_f64().filter(|s| *s > 0.0)).map(|(price, throughput)| price * 1e6 / (3600.0 * throughput)),
        }));
    }
    Ok(json!(out))
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn malloc_zone_pressure_relief(zone: *mut libc::c_void, goal: libc::size_t) -> libc::size_t;
}

/// Asks the allocator to return freed pages, so RSS reflects live memory.
fn trim() {
    #[cfg(target_os = "macos")]
    // SAFETY: a null zone means all zones; the call only releases free pages.
    unsafe {
        malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: as above.
    unsafe {
        libc::malloc_trim(0);
    }
}

/// Current resident set size in KiB (not the peak).
fn rss_kb() -> u64 {
    trim();
    // On macOS, resident size undercounts under memory pressure (pages are
    // compressed); the physical footprint counts them.
    #[cfg(target_os = "macos")]
    // SAFETY: `info` is a valid rusage_info_v2 for the flavor requested.
    unsafe {
        let mut info: libc::rusage_info_v2 = std::mem::zeroed();
        let p = (&mut info as *mut libc::rusage_info_v2).cast::<libc::rusage_info_t>();
        if libc::proc_pid_rusage(std::process::id() as i32, libc::RUSAGE_INFO_V2, p) == 0 {
            return info.ri_phys_footprint / 1024;
        }
    }
    let pid = std::process::id().to_string();
    sh("ps", &["-o", "rss=", "-p", &pid])
        .trim()
        .parse()
        .unwrap_or(0)
}

fn cpu_seconds() -> (f64, f64) {
    // SAFETY: as above.
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut u);
        let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
        (t(u.ru_utime), t(u.ru_stime))
    }
}

async fn floors(dir: &str) -> Value {
    let disk = fusee::disk::disk_check(std::path::Path::new(dir), Duration::from_secs(2))
        .map(|r| {
            json!(
                r.probes
                    .iter()
                    .map(
                        |p| json!({"method": p.method, "bytes": p.bytes, "samples": p.samples,
                "p50_us": p.p50_us, "p99_us": p.p99_us, "p999_us": p.p999_us, "max_us": p.max_us})
                    )
                    .collect::<Vec<_>>()
            )
        })
        .unwrap_or(Value::Null);
    // Loopback TCP and Unix socket round trips: the floors for server mode
    // on one machine over each transport.
    let tcp = async {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.ok()?;
        let c = tokio::net::TcpStream::connect(l.local_addr().ok()?)
            .await
            .ok()?;
        let (s, _) = l.accept().await.ok()?;
        let _ = c.set_nodelay(true);
        let _ = s.set_nodelay(true);
        echo_rtt(c, s).await
    }
    .await;
    #[cfg(unix)]
    let unix = async {
        let (c, s) = tokio::net::UnixStream::pair().ok()?;
        echo_rtt(c, s).await
    }
    .await;
    #[cfg(not(unix))]
    let unix: Option<Value> = None;
    json!({"disk": disk, "loopback_tcp_rtt_64b": tcp, "unix_socket_rtt_64b": unix})
}

/// 20,000 round trips of 64 bytes through an echoing peer.
async fn echo_rtt<S>(mut c: S, mut s: S) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::spawn(async move {
        let mut b = [0u8; 64];
        while let Ok(n) = s.read(&mut b).await {
            if n == 0 || s.write_all(&b[..n]).await.is_err() {
                break;
            }
        }
    });
    let mut h = hist();
    let mut b = [0u8; 64];
    for _ in 0..20_000 {
        let t = Instant::now();
        c.write_all(&[1u8; 64]).await.ok()?;
        let mut got = 0;
        while got < 64 {
            let n = c.read(&mut b[got..]).await.ok()?;
            if n == 0 {
                return None;
            }
            got += n;
        }
        let _ = h.record(t.elapsed().as_micros().max(1) as u64);
    }
    Some(summary(&h))
}

async fn workload(name: &str, o: &Opts) -> Result<Value, Error> {
    match name {
        "chain" => w_chain(o).await,
        "fanout" => w_fanout(o, 100).await,
        "fanout-1000" => w_fanout(o, 1000).await,
        "fanout-10000" => w_fanout(o, 10_000).await,
        "service-concurrent" => cells::service_concurrent(o).await,
        "payload" => cells::payload(o).await,
        "children" => cells::children(o).await,
        "saturate" => cells::saturate(o).await,
        "soak" => cells::soak(o).await,
        "service" => w_service(o).await,
        "open" => w_open(o, o.rate).await,
        "max" => w_max(o).await,
        "sleepers" => w_sleepers(o, o.rate as u64).await,
        "signal" => w_signal(o).await,
        "cost" => w_cost(o).await,
        _ => Err(Error::failed(format!("unknown workload '{name}'"))),
    }
}

/// The coordinator requests the final snapshot on every host before killing
/// any member. CPU is total user + system CPU of this workload process.
fn start_replica_measurement(node: &ReplicaNode, prefix: std::path::PathBuf) -> Result<(), Error> {
    let observer = node.observer();
    observer.enable_timings();
    let snapshot = move || -> Result<Value, std::io::Error> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the output on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let usage = unsafe { usage.assume_init() };
        let micros = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
        let (s, sent, received) = observer.snapshot();
        Ok(
            json!({"view":s.view,"op":s.op,"commit":s.commit,"applied":s.applied,"stopped":s.stopped,
            "checkpoint":s.checkpoint,"timings":observer.timings(),"sent_bytes":sent,"received_bytes":received,"user_us":micros(usage.ru_utime),"system_us":micros(usage.ru_stime)}),
        )
    };
    let before = snapshot()?;
    std::thread::Builder::new()
        .name("fusee-bench-counters".into())
        .spawn(move || {
            let finish = prefix.with_extension("end");
            while !finish.is_file() {
                std::thread::sleep(Duration::from_millis(10));
            }
            match snapshot().and_then(|after| {
                std::fs::write(
                    prefix.with_extension("tmp"),
                    json!({"before":before,"after":after}).to_string(),
                )?;
                std::fs::rename(prefix.with_extension("tmp"), prefix.with_extension("json"))
            }) {
                Ok(()) => (),
                Err(e) => eprintln!("replica measurement failed: {e}"),
            }
        })?;
    Ok(())
}

fn main() -> Result<(), Error> {
    // The engine reads the stall tracer's environment when its first shard
    // opens; set it while this is still the only thread.
    stalls::configure(&std::env::args().skip(1).collect::<Vec<_>>())?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str);
    if cmd == Some("segment-open") && args.len() == 2 {
        println!(
            "{}",
            segment_open::run(std::path::Path::new(&args[1])).await?
        );
        return Ok(());
    }
    if cmd == Some("external") && args.len() >= 2 {
        let opts = parse(&args[2..])?;
        println!("{}", external::run(&args[1], &opts)?);
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    if cmd == Some("io-floors") {
        return linux_floors::run(&args[1..]).map_err(Error::from);
    }
    #[cfg(target_os = "linux")]
    if cmd == Some("write-cpu") {
        return linux_floors::write_cpu(&args[1..]).map_err(Error::from);
    }
    #[cfg(target_os = "macos")]
    if cmd == Some("io-floors") {
        return mac_floors::run(&args[1..]).map_err(Error::from);
    }
    let (names, options): (Vec<&str>, &[String]) = match cmd {
        Some("floors") if args.len() == 2 => {
            let result = floors(&args[1]).await;
            if result["disk"].is_null() || result["loopback_tcp_rtt_64b"].is_null() {
                return Err(Error::failed(format!("floor probe failed: {result}")));
            }
            println!(
                "{}",
                json!({"machine":machine(),"workload":"floors","result":result})
            );
            return Ok(());
        }
        Some("run") if args.len() >= 2 => (vec![&args[1]], &args[2..]),
        Some("all") => (
            vec!["chain", "fanout", "service", "max", "signal", "cost"],
            &args[1..],
        ),
        _ => {
            return Err(Error::Failed(
                "usage: fusee-bench floors <dir> | run <workload> [options] | all [options]".into(),
            ));
        }
    };
    let mut o = parse(options)?;
    let mut node = if o.mode == "replicated" {
        let id = o.replica_id.ok_or_else(|| {
            Error::Failed("replicated mode requires --replica-id and --peers".into())
        })?;
        let mut replica = ReplicaNode::start(ReplicaConfig {
            id,
            peers: o.peers.clone(),
            join: false,
            dir: std::path::Path::new(&o.dir).join(format!("replica-{id}")),
            local: Default::default(),
            timing: Default::default(),
            security: Default::default(),
        })
        .await?;
        if let Some(path) = std::env::var_os("FUSEE_BENCH_METRICS") {
            start_replica_measurement(&replica, path.into())?;
        }
        // The same command runs on all members. Only the elected primary
        // executes the workload. The runner stops backups when the result arrives.
        loop {
            replica.wait_ready().await;
            let status = replica.status();
            if !status.ready {
                return Err(Error::Failed("replica stopped before election".into()));
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(3)) => break,
                _ = replica.wait_lost() => eprintln!("replica {id} lost initial view {}", status.view),
            }
        }
        eprintln!("replica {id} begins measurement at {:?}", replica.status());
        let _ = cells::PRIMARY.set(replica.observer());
        // Stage timings of the step's critical path on this primary, added
        // to every result line (`replica_stages`; backups' stages are in
        // their FUSEE_BENCH_METRICS files).
        replica.observer().enable_timings();
        let cfg = replica.engine_config();
        // The group's shard count: every shard of the primary replicates.
        o.shards = cfg.shards.unwrap_or(1);
        o.replicated_config = Some(cfg);
        Some(replica)
    } else {
        if o.replica_id.is_some() || !o.peers.is_empty() {
            return Err(Error::Failed(
                "replica addresses require --mode replicated".into(),
            ));
        }
        None
    };
    let host = machine();
    for name in names {
        let stall_window = stalls::Window::begin();
        let stages_before = node.as_ref().map(|n| n.observer().timings());
        let result = if let Some(replica) = &mut node
            && name != "saturate"
        {
            tokio::select! {
                result = workload(name, &o) => result?,
                _ = replica.wait_lost() => return Err(Error::failed(format!("primary changed during benchmark trial: {:?}", replica.status()))),
            }
        } else {
            workload(name, &o).await?
        };
        let mut line = json!({"machine":host,"workload":name,"mode":o.mode,"durability":durability(&o.mode),
            "transport":transport(&o),"shards":o.shards,"trials":o.trials,"rate":o.rate,"seconds":o.seconds,"steps":o.steps,
            "replicas":o.peers.len(),"primary_id":o.replica_id,"usd_per_hour":o.usd_per_hour,
            "date":sh("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]),"result":result});
        // Every stall this cell traced (null when tracing is off).
        line["stall_us"] = json!(stalls::threshold_us());
        line["stalls"] = stall_window.summary(None);
        if let (Some(n), Some(before)) = (&node, &stages_before) {
            line["replica_stages"] =
                fusee_server::replicated::stage_summary(before, &n.observer().timings());
        }
        println!("{line}");
    }
    // Keep the elected primary alive until every peer has observed the result.
    if node.is_some() && std::env::var_os("FUSEE_BENCH_HOLD_PRIMARY").is_some() {
        std::future::pending::<()>().await;
    }
    Ok(())
}
