//! `fusee-crashtest`: kills a real engine process with SIGKILL under load,
//! over and over, then checks the guarantees on real files.
//!
//! ```text
//! fusee-crashtest driver <dir> <iterations> [per-iteration-workflows]
//! ```
//!
//! The child process runs workflows whose steps call an "external service":
//! an append-only file outside the engine (the page cache survives SIGKILL,
//! as a remote service's state would). Responses differ on every call, so a
//! step that runs twice returns a different value. The driver records every
//! acknowledgment the child printed before it was killed. At the end:
//!
//! - every acknowledged start finished;
//! - every result printed before a kill is the result after recovery;
//! - the service never saw one idempotency key with two different inputs.

use fusee::{Config, Ctx, Engine, Error, FlushMethod};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Service {
    file: std::fs::File,
    n: u64,
}

impl Service {
    fn call(&mut self, key: &str, input: u64) -> u64 {
        self.n += 1;
        // One write per line, so a killed process leaves whole lines.
        let line = format!("{key} {input}\n");
        let _ = self.file.write_all(line.as_bytes());
        let mut h = std::process::id() as u64 ^ self.n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        for b in key.as_bytes() {
            h = (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3);
        }
        (h ^ input) % 1_000_000
    }
}

async fn child(
    dir: &str,
    svc_path: &str,
    start: u64,
    count: u64,
    finish: Vec<String>,
) -> Result<(), Error> {
    let mut cfg = Config::local(dir)
        .shards(2)
        .checkpoint_interval(Duration::from_millis(100));
    // `FUSEE_CRASHTEST_SHARED_LOG=0` creates per-shard segment logs (log
    // format 2) instead of the shared log. The children inherit it.
    if std::env::var_os("FUSEE_CRASHTEST_SHARED_LOG").is_some_and(|v| v == "0") {
        cfg = cfg.shared_log(false);
    }
    // `FUSEE_CRASHTEST_DIRECT_IO=0` writes buffered (Linux A/B runs).
    if std::env::var_os("FUSEE_CRASHTEST_DIRECT_IO").is_some_and(|v| v == "0") {
        cfg.local.direct_io = false;
    }
    // Linux runs the default write path (O_DIRECT with RWF_DSYNC, or the
    // buffered fallback). macOS uses plain fsync: F_FULLFSYNC would only
    // slow the run, and a process kill does not need the drive cache flushed.
    if cfg!(target_vendor = "apple") {
        cfg.local.flush = FlushMethod::Fsync;
    }
    cfg.local.segment_bytes = 256 << 10;
    // `FUSEE_CRASHTEST_LARGE=<bytes>`: every fifth workflow also runs a step
    // whose result is that large (over the payload offload threshold, it
    // goes to the shared log's blob area), and checks it after replay.
    let large: usize = std::env::var("FUSEE_CRASHTEST_LARGE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let engine = Engine::open(cfg).await?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o644)
        .open(svc_path)?;
    let svc = Arc::new(Mutex::new(Service { file, n: 0 }));
    let wf = engine.register("job", move |ctx: Ctx, n: u64| {
        let svc = svc.clone();
        async move {
            println!("DRIVEN {}", ctx.workflow_id());
            let mut v = n;
            for name in ["a", "b", "c", "d"] {
                v = ctx
                    .step(name, || {
                        let key = ctx.key();
                        let svc = svc.clone();
                        async move {
                            Ok::<_, Error>(
                                svc.lock().unwrap_or_else(|e| e.into_inner()).call(&key, v),
                            )
                        }
                    })
                    .await?;
                if name == "b" {
                    ctx.sleep("pause", Duration::from_millis(3)).await?;
                }
                if name == "c" && large > 0 && n.is_multiple_of(5) {
                    let page = |v: u64| format!("{v:016x}").repeat(large / 16);
                    let got: String = ctx
                        .step("large", move || async move { Ok::<_, Error>(page(v)) })
                        .await?;
                    if got != page(v) {
                        return Err(Error::Codec(format!(
                            "large step result of {} came back damaged",
                            ctx.workflow_id()
                        )));
                    }
                }
            }
            Ok(v)
        }
    });
    let out = Arc::new(Mutex::new(std::io::stdout()));
    let mut tasks = Vec::new();
    for id in finish {
        let engine = engine.clone();
        let out = out.clone();
        tasks.push(tokio::spawn(async move {
            let r: Result<u64, Error> = engine.run::<u64>(&id).result().await;
            match r {
                Ok(v) => {
                    let mut o = out.lock().unwrap_or_else(|e| e.into_inner());
                    let _ = writeln!(o, "RESULT {id} {v}");
                    let _ = o.flush();
                }
                Err(e) => eprintln!("child: acknowledged {id} has no result: {e}"),
            }
        }));
    }
    let wf = Arc::new(wf);
    for i in start..start + count {
        let (wf, engine) = (wf.clone(), engine.clone());
        let out = out.clone();
        tasks.push(tokio::spawn(async move {
            let id = format!("wf-{i}");
            if let Ok(h) = engine.start(&wf, &id, &i).await {
                {
                    let mut o = out.lock().unwrap_or_else(|e| e.into_inner());
                    let _ = writeln!(o, "ACK {id}");
                    let _ = o.flush();
                }
                if let Ok(v) = h.result().await {
                    let mut o = out.lock().unwrap_or_else(|e| e.into_inner());
                    let _ = writeln!(o, "RESULT {id} {v}");
                    let _ = o.flush();
                }
            }
        }));
        if i % 8 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    for t in tasks {
        let _ = t.await;
    }
    {
        let mut o = out.lock().unwrap_or_else(|e| e.into_inner());
        let _ = writeln!(o, "DONE");
        let _ = o.flush();
    }
    engine.shutdown().await
}

fn driver(
    dir: &str,
    svc: &str,
    iterations: u64,
    per: u64,
    mut after_kill: impl FnMut() -> std::io::Result<()>,
) -> Result<(), String> {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let _ = std::fs::remove_file(svc);
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let mut acked: Vec<String> = Vec::new();
    let mut results: BTreeMap<String, u64> = BTreeMap::new();
    let mut rng = 0x2545_f491_4f6c_dd1du64 ^ std::process::id() as u64;
    let parse = |line: &str,
                 acked: &mut Vec<String>,
                 results: &mut BTreeMap<String, u64>|
     -> Result<(), String> {
        let mut it = line.split_whitespace();
        match (it.next(), it.next(), it.next()) {
            (Some("ACK"), Some(id), _) => acked.push(id.to_owned()),
            (Some("RESULT"), Some(id), Some(v)) => {
                let v: u64 = v.parse().map_err(|_| format!("bad line {line}"))?;
                if let Some(old) = results.insert(id.to_owned(), v)
                    && old != v
                {
                    return Err(format!("{id} reported {old} and later {v}"));
                }
            }
            _ => {}
        }
        Ok(())
    };
    if iterations == 0 || per == 0 {
        return Err("iterations and workload count must be positive".into());
    }
    let mut crash = None;
    for i in 0..=iterations {
        let final_run = i == iterations;
        let mut pending: BTreeSet<_> = acked
            .iter()
            .filter(|id| !results.contains_key(*id))
            .cloned()
            .collect();
        let outstanding = pending.len();
        let mut child = Command::new(&exe)
            .args([
                "child",
                dir,
                svc,
                &(i * per).to_string(),
                &(if final_run { 0 } else { per }).to_string(),
            ])
            .args(if final_run {
                acked.iter().collect::<Vec<_>>()
            } else {
                pending.iter().collect()
            })
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| e.to_string())?;
        let out = child.stdout.take().ok_or("no stdout")?;
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(out).lines() {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let kill_at = acked.len() + 1 + (rng % (per / 3).max(1)) as usize;
        let mut done = false;
        let run = (|| -> Result<(), String> {
            let until = Instant::now() + Duration::from_secs(120);
            while final_run || !pending.is_empty() || acked.len() < kill_at {
                let line = rx
                    .recv_timeout(until.saturating_duration_since(Instant::now()))
                    .map_err(|e| {
                        format!(
                            "child {i}: {e}; {} acknowledged, {} waiting for recovery",
                            acked.len(),
                            pending.len()
                        )
                    })?
                    .map_err(|e| e.to_string())?;
                parse(&line, &mut acked, &mut results)?;
                if let Some(id) = line.strip_prefix("DRIVEN ").or_else(|| {
                    line.strip_prefix("RESULT ")
                        .and_then(|v| v.split_whitespace().next())
                }) {
                    pending.remove(id);
                }
                if pending.is_empty()
                    && let Some(since) = crash.take()
                {
                    let elapsed: Duration = Instant::now() - since;
                    println!(
                        "{}",
                        serde_json::json!({"workload":"crash-recovery","iteration":i,
                        "acknowledged_pending":outstanding,"kill_to_resumed_or_finished_us":elapsed.as_micros()})
                    );
                }
                if line == "DONE" {
                    done = true;
                    break;
                }
            }
            Ok(())
        })();
        if !final_run || run.is_err() {
            if !done {
                child.kill().map_err(|e| e.to_string())?;
            }
            crash = Some(Instant::now());
        }
        let status = child.wait().map_err(|e| e.to_string())?;
        run?;
        if final_run && (!status.success() || !done) {
            return Err("final run did not complete".into());
        }
        if !final_run && done {
            return Err(format!(
                "workload finished before the requested SIGKILL; {} acknowledged workflows \
                 never resumed: {:?}",
                pending.len(),
                pending.iter().take(10).collect::<Vec<_>>()
            ));
        }
        for line in rx {
            parse(&line.map_err(|e| e.to_string())?, &mut acked, &mut results)?;
        }
        if !final_run {
            after_kill().map_err(|e| format!("fault injection after kill {i}: {e}"))?;
        }
    }
    let missing: Vec<&String> = acked
        .iter()
        .filter(|id| !results.contains_key(*id))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "{} acknowledged workflows never finished: {:?}",
            missing.len(),
            &missing[..missing.len().min(10)]
        ));
    }
    // External calls: one key, one input.
    let text = std::fs::read_to_string(svc).map_err(|e| e.to_string())?;
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    let mut calls = 0;
    for line in text.lines() {
        let Some((key, input)) = line.split_once(' ') else {
            continue;
        };
        if !key.starts_with("wf-") || input.contains(' ') {
            continue;
        }
        calls += 1;
        if let Some(prev) = seen.insert(key, input)
            && prev != input
        {
            return Err(format!(
                "service key {key} was called with {prev} and later {input}"
            ));
        }
    }
    println!(
        "{iterations} SIGKILLs: {} workflows acknowledged, all finished; {} service calls, {} distinct keys, no contradictions",
        acked.len(),
        calls,
        seen.len()
    );
    Ok(())
}

/// Microseconds since the Unix epoch: one clock for every process on the
/// host, so the driver can time a failover across them.
fn unix_us() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros())
}

/// The replicas' failure-detection timing: `FUSEE_REPLICA_TIMING=heartbeat,election`
/// in milliseconds, else the library default.
fn replica_timing() -> fusee_core::vsr::Timing {
    let mut timing = fusee_core::vsr::Timing::default();
    if let Ok(v) = std::env::var("FUSEE_REPLICA_TIMING")
        && let Some((h, e)) = v.split_once(',')
        && let (Ok(h), Ok(e)) = (h.trim().parse(), e.trim().parse())
    {
        timing.heartbeat_ms = h;
        timing.election_ms = e;
    }
    timing
}

/// A replica process: joins the group; whenever it is the ready primary it
/// opens the engine, (re)starts the workload and reports results. With
/// `stream`, it also starts a workflow every few milliseconds for as long as
/// it is primary, and prints when the first step of its tenure executes:
/// the driver times a failover from the kill to that line.
async fn replica(
    id: u8,
    peers: Vec<String>,
    dir: &str,
    svc_path: &str,
    count: u64,
    permits: u32,
    stream: bool,
) -> Result<(), Error> {
    use fusee_server::replicated::{ReplicaConfig, ReplicaNode};
    // Plain fsync (a development figure on macOS) unless
    // FUSEE_REPLICA_FLUSH=default asks for the platform's durable default
    // (on Linux O_DIRECT with RWF_DSYNC), as the benchmark hosts run it.
    let local = fusee::LocalOptions {
        flush: if std::env::var("FUSEE_REPLICA_FLUSH").is_ok_and(|v| v == "default") {
            FlushMethod::platform_default()
        } else {
            FlushMethod::Fsync
        },
        ..Default::default()
    };
    let mut node = ReplicaNode::start(ReplicaConfig {
        id,
        peers,
        join: false,
        dir: std::path::PathBuf::from(dir),
        local,
        timing: replica_timing(),
        security: Default::default(),
    })
    .await?;
    {
        // Every view this member enters: the driver counts elections.
        let mut status = node.observer();
        tokio::spawn(async move {
            let mut last = None;
            loop {
                let s = status.snapshot().0;
                if last != Some((s.view, s.primary)) {
                    last = Some((s.view, s.primary));
                    println!("VIEW {id} {} {} {}", s.view, u8::from(s.primary), unix_us());
                    let _ = std::io::stdout().flush();
                }
                if !status.changed().await {
                    return;
                }
            }
        });
    }
    let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Hot standby (the default; FUSEE_REPLICA_STANDBY=0 opens the engine
    // only once primary): the engine is open while this member is a backup,
    // and its shards start at promotion.
    let standby = std::env::var("FUSEE_REPLICA_STANDBY").map_or(true, |v| v != "0");
    loop {
        let standing = if standby {
            Engine::open(
                node.standby_config()
                    .checkpoint_interval(Duration::from_millis(200)),
            )
            .await
            .ok()
        } else {
            None
        };
        node.wait_ready().await;
        println!("READY {id} {}", unix_us());
        executed.store(false, std::sync::atomic::Ordering::SeqCst);
        let elected = node.status();
        if elected.stopped {
            return Err(Error::Io(
                "replica stopped before opening its workflow engine".into(),
            ));
        }
        // Exercise checkpoint publication, transfer and replacement during
        // the same primary kills and network partitions as normal flushes.
        let engine = match standing {
            Some(engine) => engine,
            None => match Engine::open(
                node.engine_config()
                    .checkpoint_interval(Duration::from_millis(200)),
            )
            .await
            {
                Ok(engine) => engine,
                Err(error) => {
                    let now = node.status();
                    if !now.stopped && (!now.ready || now.view != elected.view) {
                        continue;
                    }
                    return Err(error);
                }
            },
        };
        println!("PRIMARY {id} {}", unix_us());
        let _ = std::io::stdout().flush();
        if permits > 0 {
            engine.limit(
                "external",
                fusee::limits::LimitSpec {
                    concurrent: permits,
                    per_second: 0.0,
                },
            )?;
        }
        let lease_path = std::path::PathBuf::from(format!("{svc_path}.leases"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o644)
            .open(svc_path)?;
        let svc = Arc::new(Mutex::new(Service { file, n: 0 }));
        let first = executed.clone();
        let wf = Arc::new(engine.register("job", move |ctx: Ctx, n: u64| {
            let svc = svc.clone();
            let lease_path = lease_path.clone();
            let first = first.clone();
            async move {
                if !first.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    // The first workflow code this tenure runs: work resumed.
                    println!("EXEC {id} {}", unix_us());
                    let _ = std::io::stdout().flush();
                }
                let mut v = n;
                for name in ["a", "b", "c"] {
                    let options = if permits > 0 {
                        fusee::StepOptions::default().limit("external")
                    } else {
                        fusee::StepOptions::default()
                    };
                    v = ctx
                        .step(name, || {
                            let key = ctx.key();
                            let svc = svc.clone();
                            let path = lease_path.clone();
                            async move {
                                if permits > 0 {
                                    // The observer finishes the effect even if the
                                    // workflow future is dropped during failover.
                                    let key = key.to_string();
                                    return tokio::task::spawn_blocking(
                                        move || -> Result<u64, Error> {
                                            let mut witness = std::fs::OpenOptions::new()
                                                .create(true)
                                                .append(true)
                                                .open(path)?;
                                            let mark = |w: &mut std::fs::File, phase: &str| {
                                                w.write_all(
                                                    format!("{phase} {id} {key}\n").as_bytes(),
                                                )
                                            };
                                            mark(&mut witness, "begin")?;
                                            println!("EFFECT {id}");
                                            std::thread::sleep(Duration::from_millis(80));
                                            let result = svc
                                                .lock()
                                                .unwrap_or_else(|e| e.into_inner())
                                                .call(&key, v);
                                            mark(&mut witness, "end")?;
                                            Ok(result)
                                        },
                                    )
                                    .await
                                    .map_err(|e| Error::failed(e.to_string()))?;
                                }
                                Ok::<_, Error>(
                                    svc.lock().unwrap_or_else(|e| e.into_inner()).call(&key, v),
                                )
                            }
                        })
                        .options(options)
                        .await?;
                }
                Ok(v)
            }
        }));
        let streaming = stream.then(|| {
            let (wf, engine) = (wf.clone(), engine.clone());
            let tag = format!("{}-{}", std::process::id(), elected.view);
            tokio::spawn(async move {
                let mut acked = false;
                for n in 0..=u64::MAX {
                    let wid = format!("wf-s{tag}-{n}");
                    if engine.start(&wf, &wid, &n).await.is_ok() && !acked {
                        // The first workflow this tenure committed.
                        acked = true;
                        println!("SACK {id} {}", unix_us());
                        let _ = std::io::stdout().flush();
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
        });
        let mut tasks = Vec::new();
        for i in 0..count {
            let (wf, engine) = (wf.clone(), engine.clone());
            tasks.push(tokio::spawn(async move {
                let wid = format!("wf-{i}");
                // A client retrying after failover attaches to the run.
                let attach = fusee::StartOptions::default().id_policy(fusee::IdPolicy::ATTACH);
                if let Ok(h) = engine.start(&wf, &wid, &i).options(attach).await {
                    println!("ACK {wid}");
                    if let Ok(v) = h.result().await {
                        println!("RESULT {wid} {v}");
                    }
                }
                let _ = std::io::stdout().flush();
            }));
        }
        tokio::select! {
            _ = async { for t in tasks { let _ = t.await; } } => {
                println!("DONE {id}");
                let _ = std::io::stdout().flush();
                node.wait_lost().await;
            }
            _ = node.wait_lost() => {}
        }
        if let Some(s) = streaming {
            s.abort();
        }
        let _ = tokio::time::timeout(Duration::from_secs(1), engine.shutdown()).await;
    }
}

/// `kills` primary SIGKILLs (or, with `quiet_secs`, none: the group runs
/// that long and the driver counts elections). With `stream`, every primary
/// also starts new workflows continuously, and the driver reports how long
/// after each kill the next primary became ready, opened its engine, ran
/// workflow code and committed a new workflow.
fn replicated_driver(
    dir: &str,
    kills: u32,
    count: u64,
    stream: bool,
    quiet_secs: u64,
) -> Result<(), String> {
    use std::sync::mpsc;
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let svc = format!("{dir}/service.log");
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let ports: Vec<String> = (0..3)
        .map(|_| {
            std::net::TcpListener::bind("127.0.0.1:0")
                .and_then(|l| l.local_addr())
                .map(|a| a.to_string())
        })
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    let peers = ports.join(",");
    let (tx, rx) = mpsc::channel::<(u8, String)>();
    let spawn = |id: u8| -> Result<std::process::Child, String> {
        let mut c = Command::new(&exe)
            .args([
                "replica",
                &id.to_string(),
                &peers,
                &format!("{dir}/r{id}"),
                &svc,
                &count.to_string(),
                "0",
                if stream { "1" } else { "0" },
            ])
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| e.to_string())?;
        let out = c.stdout.take().ok_or("no stdout")?;
        let tx = tx.clone();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(out).lines().map_while(Result::ok) {
                let _ = tx.send((id, line));
            }
        });
        Ok(c)
    };
    let mut children: Vec<Option<std::process::Child>> = (0..3)
        .map(|i| spawn(i).map(Some))
        .collect::<Result<_, _>>()?;
    let mut st = Seen::default();
    let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ u64::from(std::process::id());
    let mut killed = 0;
    if quiet_secs > 0 {
        // No faults: every view after the group's first stable one is an
        // election nobody caused.
        let until = std::time::Instant::now() + Duration::from_secs(quiet_secs);
        let mut settled = None;
        while std::time::Instant::now() < until {
            if let Ok((id, line)) = rx.recv_timeout(Duration::from_millis(20)) {
                st.handle(id, &line)?;
            }
            if settled.is_none() && st.primary.is_some() {
                settled = st.views.keys().next_back().copied();
            }
        }
        for c in children.iter_mut().flatten() {
            let _ = c.kill();
            let _ = c.wait();
        }
        while let Ok((id, line)) = rx.try_recv() {
            st.handle(id, &line)?;
        }
        let first = settled.ok_or("no primary was elected")?;
        let later: Vec<_> = st.views.range(first + 1..).collect();
        println!(
            "replicated, {quiet_secs} s without faults (stream {stream}): settled in view {first}; {} later views {:?}; {} primaries took over",
            later.len(),
            later.iter().map(|(v, _)| **v).collect::<Vec<_>>(),
            later.iter().filter(|(_, p)| p.is_some()).count()
        );
        return Ok(());
    }
    // FUSEE_KILL_SETTLE_MS: the least time from bringing a killed member
    // back to the next kill, so each failover starts from a whole group (a
    // single fault) rather than one still catching up.
    let settle = Duration::from_millis(
        std::env::var("FUSEE_KILL_SETTLE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
    );
    let mut respawned = std::time::Instant::now();
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while killed < kills {
        if std::time::Instant::now() > deadline {
            return Err("timed out waiting for a primary".into());
        }
        if let Ok((id, line)) = rx.recv_timeout(Duration::from_millis(20)) {
            st.handle(id, &line)?;
        }
        if st.primary.is_some() {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            std::thread::sleep(Duration::from_millis(30 + rng % 200));
            if let Some(wait) = settle.checked_sub(respawned.elapsed()) {
                std::thread::sleep(wait);
            }
            while let Ok((id, line)) = rx.try_recv() {
                st.handle(id, &line)?;
            }
            // A kill during settling (a new primary) is measured too.
            let Some(p) = st.primary else { continue };
            // SIGKILL the primary's process, then bring it back as a backup.
            if let Some(mut c) = children[usize::from(p)].take() {
                st.killed(p, unix_us());
                let _ = c.kill();
                let _ = c.wait();
            }
            killed += 1;
            st.primary = None;
            st.done_by = None;
            std::thread::sleep(Duration::from_millis(100));
            children[usize::from(p)] = Some(spawn(p)?);
            respawned = std::time::Instant::now();
        }
    }
    // No more faults: the current primary must finish everything.
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while st.done_by.is_none() || st.acked.iter().any(|w| !st.results.contains_key(w)) {
        if std::time::Instant::now() > deadline {
            let missing: Vec<_> = st
                .acked
                .iter()
                .filter(|w| !st.results.contains_key(*w))
                .take(10)
                .collect();
            return Err(format!(
                "acknowledged workflows never finished: {missing:?}"
            ));
        }
        if let Ok((id, line)) = rx.recv_timeout(Duration::from_millis(50)) {
            st.handle(id, &line)?;
        }
    }
    for c in children.iter_mut().flatten() {
        let _ = c.kill();
        let _ = c.wait();
    }
    let text = std::fs::read_to_string(&svc).map_err(|e| e.to_string())?;
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    let mut calls = 0;
    for line in text.lines() {
        let Some((key, input)) = line.split_once(' ') else {
            continue;
        };
        if !key.starts_with("wf-") || input.contains(' ') {
            continue;
        }
        calls += 1;
        if let Some(prev) = seen.insert(key, input)
            && prev != input
        {
            return Err(format!(
                "service key {key} was called with {prev} and later {input}"
            ));
        }
    }
    if std::env::var_os("FUSEE_FAILOVER_DETAIL").is_some() {
        for (kill, f) in &st.failovers {
            let ms = |t: Option<u128>| t.map(|t| t.saturating_sub(*kill) as f64 / 1000.0);
            println!(
                "failover at {kill}: ready {:?} open {:?} code {:?} commit {:?}",
                ms(f[0]),
                ms(f[1]),
                ms(f[2]),
                ms(f[3])
            );
        }
        for (view, (at, leader)) in &st.view_times {
            println!("view {view} first entered at {at} led by {leader:?}");
        }
    }
    for (i, name) in ["ready", "engine open", "workflow code", "new commit"]
        .iter()
        .enumerate()
    {
        let mut ms: Vec<f64> = st
            .failovers
            .iter()
            .filter_map(|f| f.1[i].map(|t| t.saturating_sub(f.0) as f64 / 1000.0))
            .collect();
        if ms.is_empty() {
            continue;
        }
        ms.sort_by(f64::total_cmp);
        let q = |p: f64| ms[((ms.len() - 1) as f64 * p).round() as usize];
        println!(
            "failover, kill to {name}: n={} p50 {:.1} ms, p90 {:.1} ms, p99 {:.1} ms, max {:.1} ms",
            ms.len(),
            q(0.5),
            q(0.9),
            q(0.99),
            ms[ms.len() - 1]
        );
    }
    println!(
        "replicated, {killed} primary SIGKILLs: {} workflows acknowledged, all finished; {calls} service calls, {} distinct keys, no contradictions",
        st.acked.len(),
        seen.len()
    );
    Ok(())
}

#[derive(Default)]
struct Seen {
    acked: std::collections::BTreeSet<String>,
    results: BTreeMap<String, u64>,
    primary: Option<u8>,
    done_by: Option<u8>,
    /// Every view any member entered, and who led it.
    views: BTreeMap<u64, Option<u8>>,
    /// Per kill: when (Unix µs), and when another member next became ready,
    /// opened its engine, ran workflow code and committed a new workflow.
    failovers: Vec<(u128, [Option<u128>; 4])>,
    victim: Option<u8>,
    /// When some member first entered each view, and who led it.
    view_times: BTreeMap<u64, (u128, Option<u8>)>,
}

impl Seen {
    fn killed(&mut self, id: u8, at: u128) {
        self.victim = Some(id);
        self.failovers.push((at, [None; 4]));
    }

    /// The first `stage` event after the latest kill, from another member.
    fn mark(&mut self, id: u8, stage: usize, at: Option<&str>) {
        let Some(at) = at.and_then(|s| s.parse::<u128>().ok()) else {
            return;
        };
        if self.victim == Some(id) {
            return;
        }
        if let Some((kill, f)) = self.failovers.last_mut()
            && at >= *kill
            && f[stage].is_none()
            && (stage == 0 || f[stage - 1].is_some())
        {
            f[stage] = Some(at);
        }
    }

    fn handle(&mut self, id: u8, line: &str) -> Result<(), String> {
        let mut it = line.split_whitespace();
        let fields: Vec<&str> = line.split_whitespace().collect();
        match fields.first().copied() {
            Some("READY") => self.mark(id, 0, fields.get(2).copied()),
            Some("PRIMARY") => self.mark(id, 1, fields.get(2).copied()),
            Some("EXEC") => self.mark(id, 2, fields.get(2).copied()),
            Some("SACK") => self.mark(id, 3, fields.get(2).copied()),
            Some("VIEW") => {
                if let (Some(view), Some(primary)) = (
                    fields.get(2).and_then(|v| v.parse::<u64>().ok()),
                    fields.get(3),
                ) {
                    let leader = self.views.entry(view).or_default();
                    if *primary == "1" {
                        *leader = Some(id);
                    }
                    let at = fields
                        .get(4)
                        .and_then(|t| t.parse::<u128>().ok())
                        .unwrap_or(0);
                    let seen = self.view_times.entry(view).or_insert((at, None));
                    seen.0 = seen.0.min(at);
                    if *primary == "1" {
                        seen.1 = Some(id);
                    }
                }
            }
            _ => {}
        }
        match (it.next(), it.next(), it.next()) {
            (Some("PRIMARY"), _, _) => self.primary = Some(id),
            (Some("DONE"), _, _) => self.done_by = Some(id),
            (Some("ACK"), Some(w), _) => {
                self.acked.insert(w.to_owned());
            }
            (Some("RESULT"), Some(w), Some(v)) => {
                let v: u64 = v.parse().map_err(|_| format!("bad line {line}"))?;
                if let Some(old) = self.results.insert(w.to_owned(), v)
                    && old != v
                {
                    return Err(format!("{w} reported {old} and later {v}"));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("replica") => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap_or_else(|e| panic!("runtime: {e}"));
            let id: u8 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            let peers: Vec<String> = args
                .get(2)
                .map(|s| s.split(',').map(str::to_owned).collect())
                .unwrap_or_default();
            let count: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(100);
            let permits = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(0);
            let stream = args.get(7).is_some_and(|s| s == "1");
            if let Err(e) = rt.block_on(replica(
                id, peers, &args[3], &args[4], count, permits, stream,
            )) {
                eprintln!("replica: {e}");
                std::process::exit(1);
            }
        }
        Some("replicated-driver") => {
            // replicated-driver <dir> <kills> <workflows> [stream] [quiet seconds]
            let dir = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "/tmp/fusee-replicated".into());
            let kills: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
            let count: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(200);
            let stream = args.get(4).is_some_and(|s| s == "stream");
            let quiet: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);
            if let Err(e) = replicated_driver(&dir, kills, count, stream, quiet) {
                eprintln!("replicated crash test FAILED: {e}");
                std::process::exit(1);
            }
        }
        Some("child") => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap_or_else(|e| panic!("runtime: {e}"));
            let start: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
            let count: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            let finish = args.get(5..).map(<[String]>::to_vec).unwrap_or_default();
            if let Err(e) = rt.block_on(child(&args[1], &args[2], start, count, finish)) {
                eprintln!("child: {e}");
                std::process::exit(1);
            }
        }
        Some("driver") => {
            let dir = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "/tmp/fusee-crashtest".into());
            let n: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20);
            let per: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(200);
            if let Err(e) = driver(&dir, &format!("{dir}.service"), n, per, || Ok(())) {
                eprintln!("crash test FAILED: {e}");
                std::process::exit(1);
            }
        }
        Some("lazyfs-driver") if args.len() == 7 => {
            let run = || -> Result<(), String> {
                let n = args[5].parse().map_err(|_| "invalid iteration count")?;
                let per = args[6].parse().map_err(|_| "invalid workload count")?;
                let mut commands = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&args[3])
                    .map_err(|e| e.to_string())?;
                let mut completed = std::io::BufReader::new(
                    std::fs::File::open(&args[4]).map_err(|e| e.to_string())?,
                );
                driver(&args[1], &args[2], n, per, || {
                    use std::os::unix::fs::FileExt;
                    let path = std::path::Path::new(&args[1]).join("fault-probe");
                    let witness = std::fs::File::create(&path)?;
                    witness.write_all_at(b"saved", 0)?;
                    witness.sync_all()?;
                    witness.write_all_at(b"dirty", 0)?;
                    drop(witness);
                    commands.write_all(b"lazyfs::clear-cache\n")?;
                    let mut line = String::new();
                    completed.read_line(&mut line)?;
                    if line.trim() != "finished::clear-cache" {
                        return Err(std::io::Error::other(format!(
                            "unexpected LazyFS response {line:?}"
                        )));
                    }
                    if std::fs::read(path)? != b"saved" {
                        return Err(std::io::Error::other(
                            "LazyFS did not roll back the unsynced witness write",
                        ));
                    }
                    Ok(())
                })
            };
            if let Err(e) = run() {
                eprintln!("LazyFS crash test failed: {e}");
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("usage: fusee-crashtest driver <dir> <iterations> [workflows-per-iteration]");
            std::process::exit(2);
        }
    }
}
