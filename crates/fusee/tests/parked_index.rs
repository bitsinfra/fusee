//! State at scale: the persisted run index, delta park frames, directories
//! written before either, and (ignored, run by hand) the figures for many
//! parked runs.
//!
//! `cargo test --release -p fusee --test parked_index -- --ignored --nocapture
//! scale` measures one shard with `FUSEE_SCALE_RUNS` (default 1,000,000)
//! parked runs: bytes per parked run, what an open reads and how long it
//! takes, and how fast the runs wake.

use fusee::core::store::{
    CHECKPOINT_MAGIC, Checkpoint, STORE_HEADER, StoreEntry, read_entry, scan_store,
};
use fusee::core::wire::{put_bytes, put_u32, put_u64, put_varint};
use fusee::{Config, Ctx, Engine, Error, Metrics};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-index-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &Path) -> Config {
    let mut c = Config::local(dir)
        .shards(1)
        .park_after(Duration::from_millis(0));
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

const DAY_MS: u64 = 24 * 3_600_000;

async fn wait_for(engine: &Engine, what: &str, done: impl Fn(&Metrics) -> bool) -> Metrics {
    let until = Instant::now() + Duration::from_secs(600);
    loop {
        let m = engine.metrics().await.unwrap();
        if done(&m) {
            return m;
        }
        assert!(Instant::now() < until, "waiting for {what}: {m:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Runs that wait for a signal, take a step and then sleep until `wake_at`:
/// each parks twice, the second time with a delta frame.
fn sleeper(engine: &Engine, wake_at: u64) -> fusee::WorkflowRef<u32, u32> {
    engine.register("sleeper", move |ctx: Ctx, n: u32| async move {
        let add: u32 = ctx.wait_for("go").await?.unwrap_or(0);
        let m = ctx
            .step("add", || async move { Ok::<_, Error>(n + add) })
            .await?;
        ctx.sleep_until("nap", wake_at).await?;
        Ok(m)
    })
}

fn shard_dir(dir: &Path) -> PathBuf {
    dir.join("shard-000")
}

fn newest_checkpoint(dir: &Path) -> PathBuf {
    let mut cps: Vec<PathBuf> = std::fs::read_dir(shard_dir(dir))
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "fck"))
        .collect();
    cps.sort();
    cps.pop().unwrap()
}

fn store_file(dir: &Path, generation: u64) -> PathBuf {
    shard_dir(dir).join(format!("runs-{generation:010}.dat"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_reads_the_index_and_delta_frames_rebuild_journals() {
    let dir = tmp("open");
    const RUNS: u32 = 3000;
    let wake_at = fusee::local::unix_ms() + DAY_MS;
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = sleeper(&engine, wake_at);
        for i in 0..RUNS {
            engine.start(&wf, &format!("r-{i}"), &i).await.unwrap();
        }
        wait_for(&engine, "first parks", |m| {
            m.parked_entries == u64::from(RUNS)
        })
        .await;
        for i in 0..RUNS {
            engine
                .run::<u32>(&format!("r-{i}"))
                .signal("go", &1000u32)
                .await
                .unwrap();
        }
        let m = wait_for(&engine, "second parks", |m| {
            m.runs_parked >= 2 * u64::from(RUNS) && m.parked_entries == u64::from(RUNS)
        })
        .await;
        assert!(m.run_table_bytes > 0);
        engine.shutdown().await.unwrap();
    }
    // The store holds a delta frame per run and the index.
    let cp = Checkpoint::decode(&std::fs::read(newest_checkpoint(&dir)).unwrap()).unwrap();
    let index = cp
        .index
        .clone()
        .expect("a format 3 checkpoint names its index");
    let store = std::fs::read(store_file(&dir, cp.store_generation)).unwrap();
    let mut deltas = 0;
    scan_store(&store[..cp.store_len as usize], |_, _, e| {
        deltas += usize::from(matches!(e, StoreEntry::Delta { .. }));
    })
    .unwrap();
    assert!(deltas >= RUNS as usize, "{deltas} delta frames");
    let index_bytes: u64 = index.iter().map(|b| u64::from(b.len)).sum();

    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let _wf = sleeper(&engine, wake_at);
    let m = engine.metrics().await.unwrap();
    assert_eq!(m.parked_entries, u64::from(RUNS));
    // Only the index frames were read: a 32-byte record per parked run in
    // the base, and one more in the delta of the checkpoint after each
    // run parked again.
    assert_eq!(m.open_store_frames, index.len() as u64);
    assert_eq!(m.open_store_bytes, index_bytes);
    assert!(
        m.open_store_bytes <= u64::from(RUNS) * 2 * 32 + 4096,
        "{} bytes for {RUNS} runs",
        m.open_store_bytes
    );
    // The rest of the store, the runs' journal frames, was not read (how
    // large it is depends on how often each run parked before shutdown).
    assert!(
        cp.store_len > index_bytes + u64::from(RUNS) * 32,
        "store {} bytes, index {index_bytes}: {m:?}",
        cp.store_len
    );
    // A parked run's journal comes back whole from its delta chain: the
    // wait from its first frame, the timer from the delta.
    let detail = engine.describe("r-7").await.unwrap().unwrap();
    assert_eq!(detail.summary.status, "parked");
    let has = |f: &dyn Fn(&fusee::core::RecordBuf) -> bool| detail.journal.iter().any(f);
    assert!(has(&|r| matches!(r, fusee::core::Record::Waited { .. })));
    assert!(has(&|r| matches!(r, fusee::core::Record::Timer { .. })));
    // Ending runs loads them from their chains and finishes them.
    for i in (0..RUNS).step_by(301) {
        let run = engine.run::<u32>(&format!("r-{i}"));
        run.cancel("done").await.unwrap();
        assert!(run.result().await.is_err());
    }
    let s = engine.run::<u32>("r-9").status().await.unwrap();
    assert_eq!(s.status, "parked");
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

/// Runs that take a step each time they are signalled (`go-0`, `go-1`, ...)
/// and park in between; a 0 ends them with their total.
fn ticker(engine: &Engine) -> fusee::WorkflowRef<u32, u32> {
    engine.register("ticker", |ctx: Ctx, n: u32| async move {
        let mut total = n;
        for k in 0.. {
            let add: u32 = ctx.wait_for(&format!("go-{k}")).await?.unwrap_or(0);
            if add == 0 {
                break;
            }
            total = ctx
                .step("add", move || async move { Ok::<_, Error>(total + add) })
                .await?;
        }
        Ok(total)
    })
}

/// Parked runs that keep changing between frequent checkpoints: deltas pile
/// up until a new index base is written a frame at a time between
/// checkpoints and named by the next one. Every run comes back from the
/// index at the next open, parked, with its journal and its total.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_bases_written_between_checkpoints_reopen_every_run() {
    let dir = tmp("built");
    const RUNS: u32 = 20_000;
    const BATCH: u32 = 400;
    let config = || cfg(&dir).checkpoint_interval(Duration::from_millis(5));
    let mut signals = vec![0u32; RUNS as usize];
    let built;
    {
        let engine = Engine::open(config()).await.unwrap();
        let wf = ticker(&engine);
        for i in 0..RUNS {
            engine.start(&wf, &format!("t-{i}"), &i).await.unwrap();
        }
        wait_for(&engine, "parks", |m| m.parked_entries == u64::from(RUNS)).await;
        let until = Instant::now() + Duration::from_secs(300);
        let mut next = 0u32;
        loop {
            let m = engine.metrics().await.unwrap();
            if m.index_bases_built >= 3 {
                break;
            }
            assert!(Instant::now() < until, "no index base built: {m:?}");
            for _ in 0..BATCH {
                let i = next % RUNS;
                next += 1;
                let k = signals[i as usize];
                engine
                    .run::<u32>(&format!("t-{i}"))
                    .signal(&format!("go-{k}"), &1u32)
                    .await
                    .unwrap();
                signals[i as usize] += 1;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let sent: u64 = signals.iter().map(|&k| u64::from(k)).sum();
        let m = wait_for(&engine, "steps", |m| m.steps_run >= sent).await;
        let m = wait_for(&engine, "parks", |x| {
            x.parked_entries == u64::from(RUNS) && x.runs_parked > m.runs_parked
        })
        .await;
        built = m.index_bases_built;
        engine.shutdown().await.unwrap();
    }
    assert!(built >= 3, "{built} bases built");
    let cp = Checkpoint::decode(&std::fs::read(newest_checkpoint(&dir)).unwrap()).unwrap();
    let index = cp
        .index
        .clone()
        .expect("a format 3 checkpoint names its index");

    let engine = Engine::open(config()).await.unwrap();
    let _wf = ticker(&engine);
    let m = engine.metrics().await.unwrap();
    // Each run is in the index or, if it was in memory at the shutdown's
    // checkpoint, in that checkpoint; only the index frames were read.
    assert_eq!(
        m.parked_entries + cp.runs.len() as u64,
        u64::from(RUNS),
        "{m:?}"
    );
    assert_eq!(m.open_store_frames, index.len() as u64, "{m:?}");
    wait_for(&engine, "parks after the open", |m| {
        m.parked_entries == u64::from(RUNS)
    })
    .await;
    for i in (0..RUNS).step_by(97) {
        let run = engine.run::<u32>(&format!("t-{i}"));
        let s = run.status().await.unwrap();
        assert_eq!(s.status, "parked", "t-{i}");
        let k = signals[i as usize];
        run.signal(&format!("go-{k}"), &0u32).await.unwrap();
        assert_eq!(run.result().await.unwrap(), i + k, "t-{i}");
    }
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

/// A checkpoint as format 2 wrote it (no index, no obsolete count).
fn encode_v2(c: &Checkpoint) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(CHECKPOINT_MAGIC);
    put_u32(&mut out, 2);
    put_u32(&mut out, c.shard);
    put_u32(&mut out, c.shards);
    put_u64(&mut out, c.lsn);
    put_u64(&mut out, c.next_run);
    put_u64(&mut out, c.store_generation);
    put_u64(&mut out, c.store_len);
    put_u64(&mut out, c.created_ms);
    put_varint(&mut out, c.runs.len() as u64);
    for r in &c.runs {
        put_varint(&mut out, r.run);
        put_varint(&mut out, r.last_lsn);
        match r.blob {
            None => out.push(0),
            Some(b) => {
                out.push(1);
                put_varint(&mut out, b.offset);
                put_varint(&mut out, u64::from(b.len));
            }
        }
        put_bytes(&mut out, &r.journal);
    }
    put_bytes(&mut out, &c.outbox);
    if c.log_position != 0 {
        put_varint(&mut out, c.log_position);
    }
    let crc = crc32c::crc32c(&out);
    put_u32(&mut out, crc);
    out
}

fn store_header_v2() -> Vec<u8> {
    let mut out = b"FUSEERUN".to_vec();
    put_u32(&mut out, 2);
    let crc = crc32c::crc32c(&out);
    put_u32(&mut out, crc);
    out
}

/// Rewrites a shard directory as the previous build left it: a format 2
/// store holding each run's newest whole frame, and a format 2 checkpoint.
fn downgrade(dir: &Path) {
    let path = newest_checkpoint(dir);
    let cp = Checkpoint::decode(&std::fs::read(&path).unwrap()).unwrap();
    assert!(cp.runs.is_empty(), "every run is parked");
    let old = std::fs::read(store_file(dir, cp.store_generation)).unwrap();
    let mut newest = std::collections::BTreeMap::new();
    scan_store(&old[..cp.store_len as usize], |off, len, e| match e {
        StoreEntry::Park { run, .. } | StoreEntry::Done { run, .. } => {
            newest.insert(run, (off, len));
        }
        StoreEntry::Tombstone { run } => {
            newest.remove(&run);
        }
        StoreEntry::Index { .. } => {}
        StoreEntry::Delta { .. } => panic!("the fixture parks each run once"),
    })
    .unwrap();
    let mut store = store_header_v2();
    for (off, len) in newest.values() {
        let frame = &old[*off as usize..(*off + u64::from(*len)) as usize];
        assert!(read_entry(frame).is_ok());
        store.extend_from_slice(frame);
    }
    for e in std::fs::read_dir(shard_dir(dir)).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_str().unwrap().to_owned();
        if name.ends_with(".fck") || name.starts_with("runs-") {
            std::fs::remove_file(p).unwrap();
        }
    }
    std::fs::write(store_file(dir, cp.store_generation), &store).unwrap();
    let v2 = Checkpoint {
        store_len: store.len() as u64,
        index: None,
        obsolete: 0,
        index_runs: 0,
        ..cp
    };
    std::fs::write(&path, encode_v2(&v2)).unwrap();
    assert_eq!(Checkpoint::decode(&encode_v2(&v2)).unwrap().index, None);
}

fn waiter(engine: &Engine) -> fusee::WorkflowRef<u32, u32> {
    engine.register("waiter", |ctx: Ctx, n: u32| async move {
        let add: u32 = ctx.wait_for("go").await?.unwrap_or(0);
        Ok(n + add)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_directory_from_before_the_index_opens_and_is_upgraded() {
    let dir = tmp("upgrade");
    const RUNS: u32 = 500;
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = waiter(&engine);
        for i in 0..RUNS {
            engine.start(&wf, &format!("w-{i}"), &i).await.unwrap();
        }
        wait_for(&engine, "parks", |m| m.parked_entries == u64::from(RUNS)).await;
        engine.shutdown().await.unwrap();
    }
    downgrade(&dir);
    let generation = {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let _wf = waiter(&engine);
        let m = engine.metrics().await.unwrap();
        // No index: the open scanned every frame.
        assert_eq!(m.parked_entries, u64::from(RUNS));
        assert_eq!(m.open_store_frames, u64::from(RUNS));
        for i in 0..10 {
            let run = engine.run::<u32>(&format!("w-{i}"));
            run.signal("go", &100u32).await.unwrap();
            assert_eq!(run.result().await.unwrap(), i + 100);
        }
        engine.shutdown().await.unwrap();
        Checkpoint::decode(&std::fs::read(newest_checkpoint(&dir)).unwrap())
            .unwrap()
            .store_generation
    };
    // The first checkpoint rewrote the store in the current format and
    // named an index in it.
    let store = std::fs::read(store_file(&dir, generation)).unwrap();
    assert_eq!(
        fusee::core::store::store_format(&store[..STORE_HEADER as usize]),
        Ok(fusee::core::store::STORE_FORMAT)
    );
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let _wf = waiter(&engine);
    let m = engine.metrics().await.unwrap();
    assert_eq!(m.parked_entries, u64::from(RUNS));
    assert!(m.open_store_frames <= 2, "{m:?}");
    for i in 0..RUNS {
        let run = engine.run::<u32>(&format!("w-{i}"));
        if i >= 10 {
            run.signal("go", &1u32).await.unwrap();
        }
        let want = if i < 10 { i + 100 } else { i + 1 };
        assert_eq!(run.result().await.unwrap(), want, "w-{i}");
    }
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

// ---- Figures at scale (ignored; see the module documentation). ----

fn scale_runs() -> u64 {
    std::env::var("FUSEE_SCALE_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

/// Bytes of memory the process uses (physical footprint on macOS, resident
/// set on Linux), after returning freed pages.
fn footprint() -> u64 {
    #[cfg(target_os = "macos")]
    unsafe extern "C" {
        fn malloc_zone_pressure_relief(zone: *mut std::ffi::c_void, goal: usize) -> usize;
    }
    #[cfg(target_os = "macos")]
    // SAFETY: a null zone means all zones; `info` is a valid rusage_info_v2.
    unsafe {
        malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
        let mut info: libc::rusage_info_v2 = std::mem::zeroed();
        let p = (&mut info as *mut libc::rusage_info_v2).cast::<libc::rusage_info_t>();
        if libc::proc_pid_rusage(std::process::id() as i32, libc::RUSAGE_INFO_V2, p) == 0 {
            return info.ri_phys_footprint;
        }
    }
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096)
}

fn cpu_us() -> u64 {
    // SAFETY: `u` is a valid rusage.
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut u);
        let t = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64;
        t(u.ru_utime) + t(u.ru_stime)
    }
}

fn scale_cfg(dir: &Path) -> Config {
    // One shard: the figures are per shard (shards open and wake in
    // parallel, one thread each).
    cfg(dir).checkpoint_interval(Duration::from_secs(3600))
}

fn nap(engine: &Engine) -> fusee::WorkflowRef<u64, ()> {
    engine.register("nap", |ctx: Ctx, at: u64| async move {
        ctx.sleep_until("nap", at).await?;
        Ok(())
    })
}

/// Starts `n` runs that sleep until `at` and waits until all are parked.
/// Returns the seconds it took.
async fn create(dir: &Path, n: u64, at: u64) -> f64 {
    let engine = Engine::open(scale_cfg(dir)).await.unwrap();
    let wf = std::sync::Arc::new(nap(&engine));
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
    wait_for(&engine, "parked", |m| m.parked_entries == n).await;
    let secs = t0.elapsed().as_secs_f64();
    engine.shutdown().await.unwrap();
    secs
}

fn run_child(test: &str, dir: &Path) -> String {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            test,
            "--nocapture",
            "--test-threads=1",
        ])
        .env("FUSEE_SCALE_DIR", dir)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The harness prints the test's name on the same line first.
    text.lines()
        .filter_map(|l| l.find("SCALE ").map(|at| &l[at..]))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn scale() {
    let n = scale_runs();
    let dir = tmp("scale");
    let at = fusee::local::unix_ms() + DAY_MS;
    let secs = create(&dir, n, at).await;
    let store_bytes: u64 = std::fs::read_dir(shard_dir(&dir))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("runs-"))
        .map(|e| e.metadata().unwrap().len())
        .sum();
    println!(
        "SCALE create runs={n} seconds={secs:.1} rate={:.0}/s store_bytes={store_bytes}",
        n as f64 / secs
    );
    println!("{}", run_child("scale_open_child", &dir));
    std::fs::remove_dir_all(&dir).unwrap();
    // What a loaded (suspended, not parked) run costs, for the memory
    // budget's estimate.
    println!("{}", run_child("scale_loaded_child", &dir));

    // Wake: `n` parked runs whose timers are due together.
    let dir = tmp("wake");
    let at = fusee::local::unix_ms() + (secs * 1500.0) as u64 + 15_000;
    let secs = create(&dir, n, at).await;
    assert!(fusee::local::unix_ms() < at, "creation took {secs:.1}s");
    let engine = Engine::open(scale_cfg(&dir)).await.unwrap();
    let _wf = nap(&engine);
    let before = engine.metrics().await.unwrap();
    assert_eq!(before.parked_entries, n);
    tokio::time::sleep(Duration::from_millis(
        at.saturating_sub(fusee::local::unix_ms()),
    ))
    .await;
    let (t0, cpu0) = (Instant::now(), cpu_us());
    let m = wait_for(&engine, "wakes", |m| m.runs_finished >= n).await;
    let (secs, cpu) = (t0.elapsed().as_secs_f64(), cpu_us() - cpu0);
    println!(
        "SCALE wake runs={n} seconds={secs:.2} rate={:.0}/s cpu_us_per_wake={:.2} loads={} \
         frames_per_load={:.2} bytes_per_load={:.0} budget_parks={} footprint_after={}",
        n as f64 / secs,
        cpu as f64 / n as f64,
        m.runs_loaded,
        (m.journal_frames_read - before.journal_frames_read) as f64 / m.runs_loaded.max(1) as f64,
        (m.journal_bytes_read - before.journal_bytes_read) as f64 / m.runs_loaded.max(1) as f64,
        m.budget_parks,
        footprint()
    );
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn scale_open_child() {
    let Some(dir) = std::env::var_os("FUSEE_SCALE_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let before = footprint();
    let t0 = Instant::now();
    let engine = Engine::open(scale_cfg(&dir)).await.unwrap();
    let open_secs = t0.elapsed().as_secs_f64();
    let _wf = nap(&engine);
    let m = engine.metrics().await.unwrap();
    // The first poll sweeps the table for timers within the horizon.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = footprint();
    let n = m.parked_entries.max(1);
    println!(
        "SCALE open parked={} open_ms={:.1} shard_open_us={} store_frames_read={} store_bytes_read={} \
         table_bytes={} table_bytes_per_run={:.1} footprint_before={before} footprint_after={after} \
         footprint_per_run={:.1}",
        m.parked_entries,
        open_secs * 1000.0,
        m.open_us,
        m.open_store_frames,
        m.open_store_bytes,
        m.run_table_bytes,
        m.run_table_bytes as f64 / n as f64,
        after.saturating_sub(before) as f64 / n as f64,
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn scale_loaded_child() {
    if std::env::var_os("FUSEE_SCALE_DIR").is_none() {
        return;
    }
    let n = (scale_runs() / 10).max(1000);
    let engine = Engine::open(
        Config::memory()
            .shards(1)
            .park_after(Duration::MAX)
            .memory_budget(0),
    )
    .await
    .unwrap();
    let wf = std::sync::Arc::new(nap(&engine));
    let at = fusee::local::unix_ms() + DAY_MS;
    let before = footprint();
    for b in (0..n).step_by(10_000) {
        let mut tasks = Vec::new();
        for k in b..(b + 10_000).min(n) {
            let (wf, engine) = (wf.clone(), engine.clone());
            tasks.push(tokio::spawn(async move {
                engine.start(&wf, &format!("l{k}"), &at).await.unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    }
    // Each run records its start and its timer, then is suspended.
    let m = wait_for(&engine, "suspended", |m| m.records >= 2 * n).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = footprint();
    println!(
        "SCALE loaded runs={n} footprint_per_run={:.0} estimate_per_run={:.0}",
        after.saturating_sub(before) as f64 / n as f64,
        m.loaded_bytes as f64 / n as f64,
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_timers_fire_after_an_open_from_the_index() {
    let dir = tmp("timers");
    let runs: u64 = std::env::var("FUSEE_TIMER_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let at = fusee::local::unix_ms() + 2_000 + runs / 2;
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = nap(&engine);
        for i in 0..runs {
            engine.start(&wf, &format!("t-{i}"), &at).await.unwrap();
        }
        wait_for(&engine, "parks", |m| m.parked_entries == runs).await;
        engine.shutdown().await.unwrap();
    }
    assert!(
        fusee::local::unix_ms() < at,
        "the runs were not all parked in time"
    );
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let _wf = nap(&engine);
    let m = engine.metrics().await.unwrap();
    assert!(
        m.open_store_frames >= 1 && m.open_store_bytes <= 64 * runs + 4096,
        "{m:?}"
    );
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let m = engine.metrics().await.unwrap();
        if m.runs_finished >= runs {
            break;
        }
        assert!(Instant::now() < until, "{m:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(fusee::local::unix_ms() >= at);
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
