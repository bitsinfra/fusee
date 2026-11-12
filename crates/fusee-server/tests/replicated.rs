use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_core::vsr::Timing;
use fusee_server::replicated::{ReplicaConfig, ReplicaNode};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

fn free_ports(n: usize) -> Vec<String> {
    let ls: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect()
}

async fn start(id: u8, peers: &[String], root: &std::path::Path) -> ReplicaNode {
    let local = LocalOptions {
        flush: FlushMethod::Fsync,
        ..Default::default()
    };
    ReplicaNode::start(ReplicaConfig {
        id,
        peers: peers.to_vec(),
        join: false,
        dir: root.join(format!("r{id}")),
        local,
        timing: Timing {
            heartbeat_ms: 20,
            election_ms: 150,
        },
        security: Default::default(),
    })
    .await
    .unwrap()
}

async fn ready_one(nodes: &mut [Option<ReplicaNode>]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    // These tests inject a failure into an established view. A transient
    // leader during initial cluster formation can lose primacy before the
    // workload even starts, especially with concurrent fsync-heavy tests.
    let mut stable = None;
    loop {
        let mut ready = nodes.iter().enumerate().filter_map(|(i, n)| {
            let status = n.as_ref()?.status();
            status.ready.then_some((i, status.view))
        });
        match (ready.next(), ready.next()) {
            (Some(leader), None) => match stable {
                Some((prior, since))
                    if prior == leader && Instant::now() - since > Duration::from_millis(300) =>
                {
                    return leader.0;
                }
                Some((prior, _)) if prior == leader => {}
                _ => stable = Some((leader, Instant::now())),
            },
            _ => stable = None,
        }
        assert!(
            Instant::now() < deadline,
            "no stable primary: {:?}",
            nodes
                .iter()
                .map(|n| n.as_ref().map(|n| n.status()))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until the primary has proposed nothing new for a few
/// milliseconds and every other member has committed all of it: a run
/// that shows "suspended" may not have its step result committed yet, and
/// a kill before that commit rightly runs the step again.
async fn quiesce(nodes: &[Option<ReplicaNode>], p: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let op = nodes[p].as_ref().unwrap().status().op;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let still = nodes[p].as_ref().unwrap().status().op == op;
        let committed = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != p)
            .filter_map(|(_, n)| n.as_ref())
            .all(|n| n.status().commit >= op);
        if still && committed {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the group did not commit what the primary proposed"
        );
    }
}

fn register(engine: &Engine, calls: Arc<AtomicU32>) -> fusee::WorkflowRef<u32, u32> {
    engine.register("job", move |ctx: Ctx, n: u32| {
        let calls = calls.clone();
        async move {
            let a: u32 = ctx
                .step("a", || {
                    let c = calls.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(n + 1)
                    }
                })
                .await?;
            let go: Option<u32> = ctx.wait_for("go").await?;
            ctx.step("b", || async move { Ok::<_, Error>(a + go.unwrap_or(0)) })
                .await
        }
    })
}

/// Hot standby: every member opens its engine from the start with
/// `standby_config`; only the primary's runs. When the primary dies, the
/// promoted backup's engine, already open with its workflows registered,
/// starts its shards from the committed log in memory and finishes the
/// runs, their recorded steps not run again; a call made to it while it
/// stood by is answered once it is promoted, and it stops when its member
/// loses the primary role.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_engine_takes_over_at_promotion_without_reopening() {
    let root = std::env::temp_dir().join(format!("fusee-standby-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let mut nodes: Vec<Option<ReplicaNode>> = Vec::new();
    for id in 0..3u8 {
        nodes.push(Some(start(id, &peers, &root).await));
    }
    let p = ready_one(&mut nodes).await;
    let calls = Arc::new(AtomicU32::new(0));
    let mut engines = Vec::new();
    let mut flows = Vec::new();
    let mut barriers = Vec::new();
    let mut limited = Vec::new();
    for n in &nodes {
        let engine = Engine::open(n.as_ref().unwrap().standby_config())
            .await
            .unwrap();
        flows.push(register(&engine, calls.clone()));
        barriers.push(engine.register(
            "barrier",
            |_ctx: Ctx, n: u32| async move { Ok::<_, Error>(n) },
        ));
        // A limit defined while standing by holds once promoted.
        engine
            .limit(
                "api",
                fusee::limits::LimitSpec {
                    concurrent: 1,
                    per_second: 0.0,
                },
            )
            .unwrap();
        limited.push(engine.register("limited", |ctx: Ctx, n: u32| async move {
            ctx.step("call", || async move { Ok::<_, Error>(n * 2) })
                .options(fusee::StepOptions::default().limit("api"))
                .await
        }));
        engines.push(Some(engine));
    }
    for i in 0..5u32 {
        engines[p]
            .as_ref()
            .unwrap()
            .start(&flows[p], &format!("job-{i}"), &i)
            .await
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while calls.load(Ordering::SeqCst) < 5 {
        assert!(Instant::now() < deadline, "steps did not run");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for i in 0..5u32 {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let s = engines[p]
                .as_ref()
                .unwrap()
                .status(&format!("job-{i}"))
                .await
                .unwrap()
                .map(|s| s.status);
            if matches!(s.as_deref(), Some("suspended" | "parked")) {
                break;
            }
            assert!(Instant::now() < deadline, "job-{i} did not reach its wait");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    engines[p]
        .as_ref()
        .unwrap()
        .start(&barriers[p], "barrier", &7)
        .await
        .unwrap();
    quiesce(&nodes, p).await;
    // A backup's standby engine holds a call until its member is promoted
    // (the one that is not stays standing by, its call waiting).
    let early: Vec<_> = (0..3)
        .map(|i| {
            (i != p).then(|| {
                let h = engines[i].as_ref().unwrap().run::<u32>("job-0");
                tokio::spawn(async move { h.signal("go", &100).await })
            })
        })
        .collect();
    nodes[p].take().unwrap().kill();
    if let Some(engine) = engines[p].take() {
        let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    }
    let q = ready_one(&mut nodes).await;
    assert_ne!(q, p);
    let engine = engines[q].as_ref().unwrap();
    for i in 1..5u32 {
        let h = engine.run::<u32>(&format!("job-{i}"));
        h.signal("go", &100).await.unwrap();
        assert_eq!(h.result().await.unwrap(), i + 101);
    }
    // The promoted engine answers the call made while it stood by.
    let mut early = early;
    let answered = early[q].take().unwrap();
    tokio::time::timeout(Duration::from_secs(10), answered)
        .await
        .expect("the standby call was never answered")
        .unwrap()
        .unwrap();
    for e in early.into_iter().flatten() {
        e.abort();
    }
    assert_eq!(engine.run::<u32>("job-0").result().await.unwrap(), 101);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        5,
        "recorded steps were not run again"
    );
    // A permit-limited step runs on the promoted engine.
    assert_eq!(
        engine
            .start(&limited[q], "limited", &21)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        42
    );
    // A new workflow commits on the promoted engine.
    assert_eq!(
        engine
            .start(&barriers[q], "after", &1)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        1
    );
    // Losing the role stops the engine: a standby engine serves one tenure.
    let survivor = (0..3).find(|&i| i != p && i != q).unwrap();
    nodes[survivor].take().unwrap().kill();
    let deadline = Instant::now() + Duration::from_secs(20);
    while nodes[q].as_ref().unwrap().status().ready {
        assert!(
            Instant::now() < deadline,
            "a primary without a quorum kept its role"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(flows);
    drop(barriers);
    drop(limited);
    for e in engines.into_iter().flatten() {
        let _ = tokio::time::timeout(Duration::from_secs(2), e.shutdown()).await;
    }
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_loss_fails_over_and_resumes_workflows() {
    let root = std::env::temp_dir().join(format!("fusee-replicated-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let mut nodes: Vec<Option<ReplicaNode>> = Vec::new();
    for id in 0..3u8 {
        nodes.push(Some(start(id, &peers, &root).await));
    }
    let p = ready_one(&mut nodes).await;
    let calls = Arc::new(AtomicU32::new(0));
    let engine = Engine::open(nodes[p].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    let wf = register(&engine, calls.clone());

    // Replicated step latency: a sequential chain.
    let chain = engine.register("chain", |ctx: Ctx, n: u32| async move {
        for i in 0..n {
            ctx.step("noop", || async move { Ok::<_, Error>(i) })
                .await?;
        }
        Ok(n)
    });
    let t = Instant::now();
    assert_eq!(
        engine
            .start(&chain, "chain", &200)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        200
    );
    eprintln!(
        "replicated (3 replicas, loopback, plain fsync): {:.1} µs per sequential step",
        t.elapsed().as_secs_f64() * 1e6 / 200.0
    );

    for i in 0..10u32 {
        engine.start(&wf, &format!("job-{i}"), &i).await.unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while calls.load(Ordering::SeqCst) < 10 {
        assert!(Instant::now() < deadline, "steps did not run");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Every run has reached its wait; an acknowledged start after that
    // proves their step results are committed (commits are in log order).
    for i in 0..10u32 {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let s = engine
                .status(&format!("job-{i}"))
                .await
                .unwrap()
                .map(|s| s.status);
            if matches!(s.as_deref(), Some("suspended" | "parked")) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "job-{i} did not reach its wait: {s:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    engine.start(&chain, "barrier", &0).await.unwrap();

    // The primary's machine is lost.
    nodes[p].take().unwrap().kill();
    let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    drop(wf);
    drop(chain);
    drop(engine);

    // A backup takes over from its own log.
    let q = ready_one(&mut nodes).await;
    assert_ne!(q, p);
    let engine = Engine::open(nodes[q].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    let _wf = register(&engine, calls.clone());
    for i in 0..10u32 {
        let h = engine.run::<u32>(&format!("job-{i}"));
        h.signal("go", &100).await.unwrap();
        assert_eq!(h.result().await.unwrap(), i + 101);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        10,
        "recorded steps were not run again"
    );
    assert_eq!(engine.run::<u32>("chain").result().await.unwrap(), 200);

    // The old primary comes back and catches up as a backup.
    nodes[p] = Some(start(p as u8, &peers, &root).await);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let back = nodes[p].as_ref().unwrap().status();
        let lead = nodes[q].as_ref().unwrap().status();
        if !back.primary && back.applied >= lead.commit && lead.commit > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "old primary did not catch up: {back:?} vs {lead:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    engine.shutdown().await.unwrap();
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedules_and_pending_updates_carry_over_a_failover() {
    let root = std::env::temp_dir().join(format!("fusee-replicated-carry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let mut nodes: Vec<Option<ReplicaNode>> = Vec::new();
    for id in 0..3u8 {
        nodes.push(Some(start(id, &peers, &root).await));
    }
    let ticks = Arc::new(AtomicU32::new(0));
    let register = |engine: &Engine, ticks: Arc<AtomicU32>| {
        let _t = engine.register("tick", move |ctx: Ctx, _: serde_json::Value| {
            let ticks = ticks.clone();
            async move {
                ticks.fetch_add(1, Ordering::SeqCst);
                Ok(ctx.workflow_id().to_owned())
            }
        });
        engine.register("cart", |ctx: Ctx, _: ()| async move {
            let c = ctx.clone();
            ctx.on_update(
                "add",
                |_: &u32| Ok(()),
                move |n: u32| {
                    let c = c.clone();
                    async move {
                        // The handler waits: the update is accepted and in
                        // flight when the primary dies.
                        let go: Option<u32> = c.wait_for("go").await?;
                        Ok(n + go.unwrap_or(0))
                    }
                },
            );
            let _: Option<()> = ctx.wait_for("done").await?;
            Ok(())
        })
    };
    let p = ready_one(&mut nodes).await;
    let engine = Engine::open(nodes[p].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    let cart = register(&engine, ticks.clone());
    let spec = fusee::ScheduleSpec {
        every_ms: Some(50),
        workflow: "tick".into(),
        ..Default::default()
    };
    engine.schedule("ticks", spec).await.unwrap();
    let run = engine.start(&cart, "cart-1", &()).await.unwrap();
    let pending = {
        let r = run.clone();
        tokio::spawn(async move { r.update::<u32, u32>("add", &5).id("u-1").await })
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let j = engine.describe("cart-1").await.unwrap().unwrap().journal;
        if ticks.load(Ordering::SeqCst) >= 2
            && j.iter()
                .any(|r| matches!(r, fusee::core::Record::Update { .. }))
        {
            break;
        }
        assert!(Instant::now() < deadline, "update not accepted / no ticks");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A durable barrier, then the primary's machine is lost.
    let barrier = engine.register("barrier", |_ctx: Ctx, _: ()| async move { Ok(()) });
    engine.start(&barrier, "barrier", &()).await.unwrap();
    nodes[p].take().unwrap().kill();
    let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    drop(pending);
    drop(engine);

    let q = ready_one(&mut nodes).await;
    assert_ne!(q, p);
    let engine = Engine::open(nodes[q].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    let _cart = register(&engine, ticks.clone());
    let before = ticks.load(Ordering::SeqCst);
    // The schedule keeps firing on the backup.
    let deadline = Instant::now() + Duration::from_secs(10);
    while ticks.load(Ordering::SeqCst) < before + 2 {
        assert!(
            Instant::now() < deadline,
            "the schedule stopped after failover"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The accepted update is still in flight: a retried call with its ID
    // gets the handler's result once it finishes.
    let run = engine.run::<()>("cart-1");
    let again = {
        let r = run.clone();
        tokio::spawn(async move { r.update::<u32, u32>("add", &5).id("u-1").await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    run.signal("go", &10).await.unwrap();
    assert_eq!(again.await.unwrap().unwrap(), 15);
    let j = run.describe().await.unwrap().journal;
    let accepted = j
        .iter()
        .filter(|r| matches!(r, fusee::core::Record::Update { .. }))
        .count();
    assert_eq!(accepted, 1, "the update was not accepted twice");
    engine.schedule_handle("ticks").delete().await.unwrap();
    engine.shutdown().await.unwrap();
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_damaged_committed_tail_keeps_its_watermark_and_repairs_from_peers() {
    let root = std::env::temp_dir().join(format!("fusee-repair-tail-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let mut nodes = Vec::new();
    for id in 0..3 {
        nodes.push(Some(start(id, &peers, &root).await));
    }
    let p = ready_one(&mut nodes).await;
    // The test damages the committed protocol suffix, which a shutdown
    // checkpoint would fold into a snapshot.
    let engine = Engine::open(
        nodes[p]
            .as_ref()
            .unwrap()
            .engine_config()
            .checkpoint_on_shutdown(false),
    )
    .await
    .unwrap();
    let wf = engine.register("once", |_ctx: Ctx, n: u32| async move { Ok(n) });
    assert_eq!(
        engine
            .start(&wf, "work", &41)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        41
    );
    engine.shutdown().await.unwrap();
    let b = (p + 1) % 3;
    let dir = root.join(format!("r{b}"));
    let sb = dir.join("vsr/superblock");
    let hint = dir.join("vsr/commit");
    let log = dir.join("vsr/log");
    // The commit position the backup saved: its superblock's, or the
    // background commit hint's if higher.
    let saved_commit = || {
        let bytes = std::fs::read(&sb).unwrap();
        let sb = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let hint = std::fs::read(&hint)
            .ok()
            .filter(|b| b.len() >= 12)
            .map_or(0, |b| u64::from_le_bytes(b[..8].try_into().unwrap()));
        sb.max(hint)
    };
    let committed = nodes[p].as_ref().unwrap().status().commit;
    let until = Instant::now() + Duration::from_secs(10);
    while saved_commit() != committed || committed == 0 {
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    nodes[b].take().unwrap().kill();
    loop {
        if let Ok(writer) = fusee::local::lock_directory(&dir.join("engine")) {
            drop(writer);
            break;
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let intact = std::fs::read(&log).unwrap();
    let mut damaged = intact.clone();
    // Change the final frame's payload, before the padding of the prepared extent.
    let mut end = 0;
    while end + 8 <= intact.len() {
        let len = u32::from_le_bytes(intact[end..end + 4].try_into().unwrap()) as usize;
        if len == 0 {
            break;
        }
        end += 8 + len;
    }
    assert!(end > 8 && end < intact.len());
    damaged[end - 1] ^= 1;
    std::fs::write(&log, &damaged).unwrap();

    // First re-open in isolation so peer repair cannot hide a local truncation.
    let isolated = start(b as u8, &free_ports(3), &root).await;
    assert_eq!(saved_commit(), committed);
    assert_eq!(std::fs::read(&log).unwrap(), damaged);
    isolated.kill();
    loop {
        if fusee::local::lock_directory(&dir.join("engine")).is_ok() {
            break;
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    nodes[b] = Some(start(b as u8, &peers, &root).await);
    while std::fs::read(&log).unwrap() != intact {
        assert!(
            Instant::now() < until,
            "peer repair did not replace the damaged frame"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(saved_commit(), committed);
    drop(nodes);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_primary_observes_named_limits_and_the_previous_holders_lease() {
    use fusee::limits::{Acquire, LimitBackend, LimitSpec};
    async fn acquire(backend: &dyn LimitBackend, ticket: u64) {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if backend.acquire("payments", ticket, fusee::local::unix_ms(), None)
                    == Acquire::Granted
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("permit timed out");
    }

    let root = std::env::temp_dir().join(format!("fusee-vsr-leases-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let mut nodes = Vec::new();
    for id in 0..3 {
        nodes.push(Some(start(id, &peers, &root).await));
    }
    let first = ready_one(&mut nodes).await;
    let old = nodes[first]
        .as_ref()
        .unwrap()
        .engine_config()
        .limits
        .unwrap();
    old.define(
        "payments",
        LimitSpec {
            concurrent: 1,
            per_second: 0.0,
        },
    );
    let old_ticket = old.ticket();
    acquire(&*old, old_ticket).await;
    nodes[first].take().unwrap().kill();
    let next = ready_one(&mut nodes).await;
    assert_ne!(first, next);
    let fresh = nodes[next]
        .as_ref()
        .unwrap()
        .engine_config()
        .limits
        .unwrap();
    let start = Instant::now();
    let a = fresh.ticket();
    // No re-registration on the successor. The committed definition and the
    // old grant must survive, even when the numeric ticket is reused.
    acquire(&*fresh, a).await;
    assert!(
        start.elapsed() >= Duration::from_millis(2800),
        "former holder's lease was lost"
    );
    let b = fresh.ticket();
    for _ in 0..10 {
        assert!(matches!(
            fresh.acquire("payments", b, fusee::local::unix_ms(), None),
            Acquire::Wait { .. }
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fresh.release("payments", a);
    acquire(&*fresh, b).await;
    fresh.release("payments", b);
    drop(fresh);
    drop(old);
    drop(nodes);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_replica_installs_a_checkpoint_then_recovers_without_the_protocol_prefix() {
    for members in [3, 5] {
        checkpoint_cluster(members).await;
    }
}

fn damage_image(root: &std::path::Path, member: usize) -> u64 {
    let vsr = root.join(format!("r{member}/vsr"));
    let bytes = std::fs::read(vsr.join("current")).unwrap();
    let pointer: serde_json::Value = serde_json::from_slice(&bytes[12..]).unwrap();
    let meta = &pointer["image"];
    let hash: String = meta["sha256"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| format!("{:02x}", b.as_u64().unwrap()))
        .collect();
    let damaged = vsr.join(format!(
        "snapshot-{:020}-{hash}",
        meta["op"].as_u64().unwrap()
    ));
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&damaged)
        .unwrap();
    let offset = file.metadata().unwrap().len() / 2;
    let mut byte = [0];
    file.read_exact_at(&mut byte, offset).unwrap();
    file.write_all_at(&[byte[0] ^ 0xff], offset).unwrap();
    file.sync_all().unwrap();
    drop(file);
    meta["op"].as_u64().unwrap()
}

async fn checkpoint_cluster(members: usize) {
    let root =
        std::env::temp_dir().join(format!("fusee-checkpoint-{members}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(members);
    let mut nodes = Vec::new();
    for id in 0..members {
        nodes.push(Some(start(id as u8, &peers, &root).await));
    }
    let p = ready_one(&mut nodes).await;
    tokio::time::sleep(Duration::from_millis(650)).await;
    let lag = (p + 1) % members;
    nodes[lag].take().unwrap().kill();
    let setup = |node: &ReplicaNode| {
        node.engine_config()
            .checkpoint_interval(Duration::from_millis(50))
            .park_after(Duration::from_millis(0))
    };
    let engine = Engine::open(setup(nodes[p].as_ref().unwrap()))
        .await
        .unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let register = |engine: &Engine, calls: Arc<AtomicU32>| {
        engine.register("checkpoint-job", move |ctx: Ctx, data: String| {
            let calls = calls.clone();
            async move {
                let n = ctx
                    .step("size", || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(data.len())
                    })
                    .await?;
                let delta: Option<usize> = ctx.wait_for("go").await?;
                Ok(n + delta.unwrap_or(0))
            }
        })
    };
    let wf = register(&engine, calls.clone());
    let size = (1 << 20) + 200;
    engine.start(&wf, "saved", &"x".repeat(size)).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    while engine.metrics().await.unwrap().runs_parked == 0 {
        assert!(Instant::now() < deadline, "workflow failed to park");
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    while nodes[p].as_ref().unwrap().status().checkpoint == 0 {
        assert!(
            Instant::now() < deadline,
            "primary did not publish its checkpoint: {:?}",
            nodes[p].as_ref().unwrap().status()
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let base = nodes[p].as_ref().unwrap().status().checkpoint;
    assert!(!root.join(format!("r{p}/vsr/log")).exists());
    let sizes: Vec<_> = std::fs::read_dir(root.join(format!("r{p}/vsr")))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot-") && e.path().is_file())
        .map(|e| e.metadata().unwrap().len())
        .collect();
    assert!(
        sizes.iter().any(|&n| n > 1 << 20),
        "a multi-page image must be used: {sizes:?}"
    );
    // A log group's image holds every shard's checkpoint, not a segment.
    let bytes = std::fs::read(root.join(format!("r{p}/vsr/current"))).unwrap();
    let pointer: serde_json::Value = serde_json::from_slice(&bytes[12..]).unwrap();
    assert!(
        pointer["image"]["shards"].as_u64().unwrap() >= 1,
        "{pointer}"
    );
    assert!(segment_files(&root).is_empty());
    nodes[lag] = Some(start(lag as u8, &peers, &root).await);
    for n in &nodes {
        let n = n.as_ref().unwrap();
        while n.status().checkpoint < base {
            assert!(
                Instant::now() < deadline,
                "lagging replica failed to install base {base}: {:?}",
                n.status()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Stop producing new checkpoints before damaging two copies of the
    // same image; all other members must hold that exact advertised digest.
    engine.shutdown().await.unwrap();
    drop(wf);
    drop(engine);
    let ready_deadline = Instant::now() + Duration::from_secs(15);
    let mut stable = None;
    loop {
        let base = nodes[p].as_ref().unwrap().status().checkpoint;
        if nodes
            .iter()
            .flatten()
            .all(|n| n.status().checkpoint == base)
        {
            match stable {
                Some((prev, since))
                    if prev == base && Instant::now() - since > Duration::from_millis(550) =>
                {
                    break;
                }
                Some((prev, _)) if prev == base => {}
                _ => stable = Some((base, Instant::now())),
            }
        } else {
            stable = None;
        }
        assert!(
            Instant::now() < ready_deadline,
            "members did not acquire the final checkpoint"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Corrupt the active portable image on an otherwise caught-up backup.
    // It must fetch a verified copy and stay out of elections until repaired.
    nodes[lag].take().unwrap().kill();
    let damaged_base = damage_image(&root, lag);
    // The advertised donor has readable engine state but its immutable file
    // is now bad too. An independent copy must satisfy the original digest.
    let donor_base = damage_image(&root, p);
    assert_eq!(donor_base, damaged_base);
    nodes[lag] = Some(start(lag as u8, &peers, &root).await);
    let repair_deadline = Instant::now() + Duration::from_secs(15);
    while nodes[lag].as_ref().unwrap().status().checkpoint < damaged_base {
        let node = nodes[lag].as_ref().unwrap();
        assert!(
            !node.status().ready && !node.status().stopped,
            "damaged replica voted or stopped: {:?}",
            node.status()
        );
        assert!(
            Instant::now() < repair_deadline,
            "damaged replica failed to repair: {:?}",
            node.status()
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    // Every member now has a disk baseline. Remove all in-memory state and
    // force a member other than the old primary to resume the workflow.
    for node in &mut nodes {
        node.take().unwrap().kill();
    }
    // Lose every mutable engine file on one member. The surviving immutable
    // checkpoint and committed protocol suffix are sufficient to reopen it.
    let bytes = std::fs::read(root.join(format!("r{lag}/vsr/current"))).unwrap();
    let current: serde_json::Value = serde_json::from_slice(&bytes[12..]).unwrap();
    let engine_dir = root
        .join(format!("r{lag}"))
        .join(current["engine"].as_str().unwrap());
    for shard in std::fs::read_dir(&engine_dir).unwrap().flatten() {
        if shard.file_name().to_string_lossy().starts_with("shard-") {
            std::fs::remove_dir_all(shard.path()).unwrap();
        }
    }
    for id in (0..members).filter(|&i| i != p) {
        nodes[id] = Some(start(id as u8, &peers, &root).await);
    }
    let q = ready_one(&mut nodes).await;
    let engine = Engine::open(setup(nodes[q].as_ref().unwrap()))
        .await
        .unwrap();
    let _wf = register(&engine, calls.clone());
    let run = engine.run::<usize>("saved");
    run.signal("go", &3usize).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(12), run.result())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, size + 3);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    engine.shutdown().await.unwrap();
    drop(nodes);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dedicated_servers_recover_remote_work_and_fence_worker_permits() {
    use fusee::limits::{Acquire, LimitBackend, LimitSpec};
    use fusee_server::{Client, Server};
    tokio::time::timeout(Duration::from_secs(45), async {
        let root = std::env::temp_dir().join(format!("fusee-dedicated-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let peers = free_ports(3);
        let mut nodes = Vec::new();
        let mut endpoints = Vec::new();
        let mut servers = Vec::new();
        for id in 0..3 {
            let node = start(id, &peers, &root).await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.push(listener.local_addr().unwrap().to_string());
            servers.push(tokio::spawn(fusee_server::server::serve_tcp(
                Server::replicated(&node),
                listener,
            )));
            nodes.push(Some(node));
        }
        let primary = ready_one(&mut nodes).await;
        let address = format!("replicas://{}", endpoints.join(","));
        let worker = |addr: String| {
            tokio::task::spawn_blocking(move || fusee_server::remote_worker(&addr).unwrap())
        };
        let client =
            |addr: String| tokio::task::spawn_blocking(move || Client::connect(&addr).unwrap());
        let (cfg, remote) = worker(address.clone()).await.unwrap();
        let first_limits = remote.limits();
        let engine = Engine::open(cfg).await.unwrap();
        let calls = Arc::new(AtomicU32::new(0));
        let wf = register(&engine, calls.clone());
        let old_relay = tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));
        engine.start(&wf, "from-a-worker", &41).await.unwrap();
        loop {
            if matches!(
                engine
                    .status("from-a-worker")
                    .await
                    .unwrap()
                    .map(|r| r.status)
                    .as_deref(),
                Some("suspended" | "parked")
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let stale_client = client(address.clone()).await.unwrap();
        first_limits.define(
            "external",
            LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        );
        let old_ticket = first_limits.ticket();
        let acquired_at = Instant::now();
        while !matches!(
            first_limits.acquire("external", old_ticket, fusee::local::unix_ms(), None),
            Acquire::Granted
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Kill the primary itself. Its TCP worker and client connections must close.
        nodes[primary].take().unwrap().kill();
        let successor = ready_one(&mut nodes).await;
        assert_ne!(primary, successor);
        let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
        old_relay.abort();
        let old = tokio::task::spawn_blocking(move || stale_client.result("from-a-worker"))
            .await
            .unwrap();
        assert!(
            old.is_err(),
            "a deposed client must return after its reader exits"
        );
        drop(wf);
        drop(engine);
        drop(first_limits);

        let (cfg, remote) = worker(address.clone()).await.unwrap();
        let next_limits = remote.limits();
        let next_engine = Engine::open(cfg).await.unwrap();
        let wf = register(&next_engine, calls.clone());
        let relay = tokio::spawn(fusee_server::serve_calls(remote, next_engine.clone()));
        let ticket = next_limits.ticket();
        while !matches!(
            next_limits.acquire("external", ticket, fusee::local::unix_ms(), None),
            Acquire::Granted
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            acquired_at.elapsed() >= Duration::from_millis(fusee_server::limits::LEASE_MS),
            "the new worker reused an unexpired grant"
        );
        next_limits.release("external", ticket);
        let c = client(address.clone()).await.unwrap();
        let result = tokio::task::spawn_blocking(move || {
            c.signal("from-a-worker", "go", &serde_json::json!(1))
                .unwrap();
            c.result("from-a-worker").unwrap()
        })
        .await
        .unwrap();
        assert_eq!(result, serde_json::json!(43));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // The remaining primary stays reachable while the last backup goes
        // offline. The server must fence an outstanding consumer even when
        // this primary has not learned of any higher view.
        next_engine.start(&wf, "isolated", &10).await.unwrap();
        let blocked = client(address.clone()).await.unwrap();
        let waiting = tokio::task::spawn_blocking(move || blocked.result("isolated"));
        let backup = nodes
            .iter()
            .enumerate()
            .find_map(|(i, n)| (i != successor && n.is_some()).then_some(i))
            .unwrap();
        servers[backup].abort();
        let _ = (&mut servers[backup]).await;
        nodes[backup].take().unwrap().kill();
        let error = tokio::time::timeout(Duration::from_secs(8), waiting)
            .await
            .expect("isolated primary kept a client socket")
            .unwrap();
        assert!(error.is_err());
        drop(wf);
        let _ = next_engine.shutdown().await;
        relay.abort();
        drop(next_engine);

        // Restore a quorum and recover the acknowledged start from durable state.
        let returning = start(backup as u8, &peers, &root).await;
        let listener = tokio::net::TcpListener::bind(&endpoints[backup])
            .await
            .unwrap();
        servers[backup] = tokio::spawn(fusee_server::server::serve_tcp(
            Server::replicated(&returning),
            listener,
        ));
        nodes[backup] = Some(returning);
        ready_one(&mut nodes).await;
        let (cfg, _) = worker(address.clone()).await.unwrap();
        let restored = Engine::open(cfg).await.unwrap();
        let wf = register(&restored, calls.clone());
        let run = restored.run::<u32>("isolated");
        run.signal("go", &2).await.unwrap();
        assert_eq!(run.result().await.unwrap(), 13);
        drop(wf);
        restored.shutdown().await.unwrap();
        drop(restored);
        for task in servers {
            task.abort();
        }
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("dedicated worker failover timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_only_prefix_survives_restarts_and_preserves_the_first_workflow() {
    use fusee::limits::LimitSpec;
    tokio::time::timeout(Duration::from_secs(45), async {
        let root =
            std::env::temp_dir().join(format!("fusee-control-checkpoints-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let peers = free_ports(3);
        let mut nodes = Vec::new();
        for id in 0..3 {
            nodes.push(Some(start(id, &peers, &root).await));
        }
        let primary = ready_one(&mut nodes).await;
        let limits = nodes[primary]
            .as_ref()
            .unwrap()
            .engine_config()
            .limits
            .unwrap();
        let spec = LimitSpec {
            concurrent: 1,
            per_second: 0.0,
        };
        for _ in 0..1300 {
            limits.define("payments", spec);
        }
        for n in nodes.iter().flatten() {
            while n.status().checkpoint < 1024 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let ptr = std::fs::read(root.join(format!("r{primary}/vsr/current"))).unwrap();
        let image: serde_json::Value = serde_json::from_slice(&ptr[12..]).unwrap();
        assert_eq!(image["image"]["lsn"], 0);
        drop(limits);
        drop(nodes);
        let mut nodes = Vec::new();
        for id in 0..3 {
            nodes.push(Some(start(id, &peers, &root).await));
        }
        let primary = ready_one(&mut nodes).await;
        let cfg = nodes[primary]
            .as_ref()
            .unwrap()
            .engine_config()
            .checkpoint_interval(Duration::from_millis(25))
            .park_after(Duration::ZERO);
        let limits = cfg.limits.clone().unwrap();
        let engine = Engine::open(cfg).await.unwrap();
        let calls = Arc::new(AtomicU32::new(0));
        let wf = register(&engine, calls.clone());
        engine.start(&wf, "first", &39).await.unwrap();
        while engine.metrics().await.unwrap().runs_parked == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Change the engine image, then advance again with control records
        // while the workflow is asleep. Preserve its exact archive and store.
        let anchor = nodes[primary].as_ref().unwrap().status().checkpoint;
        for _ in 0..1400 {
            limits.define("payments", spec);
        }
        // A new workflow batch induces an ordinary engine checkpoint at the
        // new position. Finish one run to exercise archived terminal state.
        let second = engine.start(&wf, "second", &4).await.unwrap();
        second.signal("go", &1).await.unwrap();
        assert_eq!(second.result().await.unwrap(), 6);
        loop {
            let p = std::fs::read(root.join(format!("r{primary}/vsr/current"))).unwrap();
            let v: serde_json::Value = serde_json::from_slice(&p[12..]).unwrap();
            if v["image"]["lsn"].as_u64().unwrap() > 0
                && v["image"]["op"].as_u64().unwrap() > anchor
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let anchor = nodes[primary].as_ref().unwrap().status().checkpoint;
        let hold = limits.ticket();
        while !matches!(
            limits.acquire("payments", hold, fusee::local::unix_ms(), None),
            fusee::limits::Acquire::Granted
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for _ in 0..2300 {
            limits.define("another", spec);
        }
        for node in nodes.iter().flatten() {
            while node.status().checkpoint < anchor + 1024 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        engine.shutdown().await.unwrap();
        drop(second);
        drop(wf);
        drop(engine);
        drop(limits);
        drop(nodes);
        let mut nodes = Vec::new();
        for id in 0..3 {
            nodes.push(Some(start(id, &peers, &root).await));
        }
        let p = ready_one(&mut nodes).await;
        let engine = Engine::open(nodes[p].as_ref().unwrap().engine_config())
            .await
            .unwrap();
        let wf = register(&engine, calls.clone());
        let first = engine.run::<u32>("first");
        first.signal("go", &2).await.unwrap();
        assert_eq!(first.result().await.unwrap(), 42);
        assert_eq!(engine.run::<u32>("second").result().await.unwrap(), 6);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(wf);
        engine.shutdown().await.unwrap();
        drop(engine);
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("control checkpoint did not make progress");
}

fn segment_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|x| x == "seg") {
                found.push(path);
            }
        }
    }
    found
}

/// One group serves every shard of the primary's engine, and the protocol
/// log is the only copy of the log: no replica materializes segment files,
/// and a new primary's engine recovers each shard from protocol entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_shard_fails_over_from_the_only_copy_of_the_log() {
    let root = std::env::temp_dir().join(format!("fusee-one-copy-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let mut nodes = Vec::new();
    for id in 0..3 {
        nodes.push(Some(start(id, &peers, &root).await));
    }
    let p = ready_one(&mut nodes).await;
    let engine = Engine::open(nodes[p].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    // A new group gets one shard per core, up to eight.
    let n = engine.shards();
    let cores = std::thread::available_parallelism().map_or(1, |c| c.get().min(8) as u32);
    assert_eq!(n, cores);
    let calls = Arc::new(AtomicU32::new(0));
    let wf = register(&engine, calls.clone());
    let runs = 40u32;
    for i in 0..runs {
        engine.start(&wf, &format!("job-{i}"), &i).await.unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    for i in 0..runs {
        loop {
            let s = engine
                .status(&format!("job-{i}"))
                .await
                .unwrap()
                .map(|s| s.status);
            if matches!(s.as_deref(), Some("suspended" | "parked")) {
                break;
            }
            assert!(Instant::now() < deadline, "job-{i} did not reach its wait");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), runs);
    let shards: std::collections::BTreeSet<u32> = (0..runs)
        .map(|i| fusee::core::shard::shard_of(&format!("job-{i}"), n))
        .collect();
    assert!(n == 1 || shards.len() > 1, "the runs must span shards");
    assert!(
        segment_files(&root).is_empty(),
        "a log group materialized segments: {:?}",
        segment_files(&root)
    );
    nodes[p].take().unwrap().kill();
    let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    drop(wf);
    drop(engine);

    let q = ready_one(&mut nodes).await;
    // The group's shard count comes from its log.
    let engine = Engine::open(nodes[q].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    assert_eq!(engine.shards(), n);
    let _wf = register(&engine, calls.clone());
    for i in 0..runs {
        let h = engine.run::<u32>(&format!("job-{i}"));
        h.signal("go", &1).await.unwrap();
        assert_eq!(h.result().await.unwrap(), i + 2);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        runs,
        "a recorded step ran again"
    );
    assert!(segment_files(&root).is_empty());
    engine.shutdown().await.unwrap();
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

/// Commit on any durable quorum: the primary's own log write is in the
/// background, so a primary whose every flush takes 40 ms still commits
/// at the speed of its backups. Its log then lags what the group
/// committed; killed, it comes back as a backup and nothing is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_primary_disk_does_not_slow_commits_and_loses_nothing() {
    let root = std::env::temp_dir().join(format!("fusee-slow-primary-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let slow = |id: u8, peers: &[String], root: &std::path::Path, slow: bool| {
        let local = LocalOptions {
            flush: FlushMethod::Fsync,
            emulated_flush: if slow {
                Duration::from_millis(40)
            } else {
                Duration::ZERO
            },
            ..Default::default()
        };
        ReplicaNode::start(ReplicaConfig {
            id,
            peers: peers.to_vec(),
            join: false,
            dir: root.join(format!("r{id}")),
            local,
            timing: Timing {
                heartbeat_ms: 20,
                election_ms: 150,
            },
            security: Default::default(),
        })
    };
    // Member 1 has the slow disk. Move the primary role until it has it.
    let mut nodes = Vec::new();
    for id in 0..3u8 {
        nodes.push(Some(slow(id, &peers, &root, id == 1).await.unwrap()));
    }
    let mut p = ready_one(&mut nodes).await;
    while p != 1 {
        nodes[p].take().unwrap().kill();
        nodes[p] = Some(slow(p as u8, &peers, &root, false).await.unwrap());
        p = ready_one(&mut nodes).await;
    }
    let engine = Engine::open(nodes[p].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    let chain = engine.register("chain", |ctx: Ctx, n: u32| async move {
        for i in 0..n {
            ctx.step(&format!("s{i}"), || async move { Ok::<_, Error>(i) })
                .await?;
        }
        Ok(n)
    });
    let steps = 50u32;
    let t = Instant::now();
    assert_eq!(
        engine
            .start(&chain, "slow", &steps)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        steps
    );
    let per_step = t.elapsed() / steps;
    assert!(
        per_step < Duration::from_millis(20),
        "commits waited for the primary's 40 ms flush: {per_step:?} per step"
    );
    // Acknowledged, then the primary dies with its own log behind.
    let calls = Arc::new(AtomicU32::new(0));
    let wf = register(&engine, calls.clone());
    engine.start(&wf, "after", &7).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while calls.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    while engine
        .status("after")
        .await
        .unwrap()
        .is_none_or(|s| !matches!(s.status.as_str(), "suspended" | "parked"))
    {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    quiesce(&nodes, p).await;
    nodes[p].take().unwrap().kill();
    let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    drop(wf);
    drop(chain);
    drop(engine);
    nodes[p] = Some(slow(p as u8, &peers, &root, true).await.unwrap());
    let q = ready_one(&mut nodes).await;
    let engine = Engine::open(nodes[q].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    let _wf = register(&engine, calls.clone());
    assert_eq!(engine.run::<u32>("slow").result().await.unwrap(), steps);
    let h = engine.run::<u32>("after");
    h.signal("go", &1).await.unwrap();
    assert_eq!(h.result().await.unwrap(), 9);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // The restarted member catches up as a backup.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let back = nodes[p].as_ref().unwrap().status();
        let lead = nodes[q].as_ref().unwrap().status();
        if q == p || (!back.primary && back.commit >= lead.commit && lead.commit > 0) {
            break;
        }
        assert!(Instant::now() < deadline, "{back:?} vs {lead:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    engine.shutdown().await.unwrap();
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

/// An engine reopened by a primary that never lost its role recovers from
/// the log as it is now, including everything the previous engine wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_reopened_by_the_same_primary_sees_what_the_last_one_wrote() {
    let root = std::env::temp_dir().join(format!("fusee-reopen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let mut nodes = Vec::new();
    for id in 0..3 {
        nodes.push(Some(start(id, &peers, &root).await));
    }
    let p = ready_one(&mut nodes).await;
    let view = nodes[p].as_ref().unwrap().status().view;
    let calls = Arc::new(AtomicU32::new(0));
    for round in 0..3u32 {
        let engine = Engine::open(
            nodes[p]
                .as_ref()
                .unwrap()
                .engine_config()
                .checkpoint_on_shutdown(round % 2 == 0),
        )
        .await
        .unwrap();
        let wf = register(&engine, calls.clone());
        for earlier in 0..round {
            let h = engine.run::<u32>(&format!("r{earlier}"));
            if earlier + 1 == round {
                h.signal("go", &0).await.unwrap();
            }
            assert_eq!(h.result().await.unwrap(), earlier + 1);
        }
        engine
            .start(&wf, &format!("r{round}"), &round)
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while engine
            .status(&format!("r{round}"))
            .await
            .unwrap()
            .is_none_or(|s| !matches!(s.status.as_str(), "suspended" | "parked"))
        {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        engine.shutdown().await.unwrap();
    }
    assert_eq!(nodes[p].as_ref().unwrap().status().view, view);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

/// Process-wide system calls and context switches so far (macOS task
/// events; zeros elsewhere).
fn task_events() -> (u64, u64) {
    #[cfg(target_os = "macos")]
    {
        #[repr(C)]
        #[derive(Default)]
        struct Events {
            faults: i32,
            pageins: i32,
            cow_faults: i32,
            messages_sent: i32,
            messages_received: i32,
            syscalls_mach: i32,
            syscalls_unix: i32,
            csw: i32,
        }
        unsafe extern "C" {
            static mach_task_self_: u32;
            fn task_info(task: u32, flavor: i32, info: *mut Events, count: *mut u32) -> i32;
        }
        let mut e = Events::default();
        let mut count = 8u32;
        // SAFETY: TASK_EVENTS_INFO (2) fills eight 32-bit counters.
        let r = unsafe { task_info(mach_task_self_, 2, &mut e, &mut count) };
        if r == 0 {
            return (
                (e.syscalls_unix as u32 as u64) + (e.syscalls_mach as u32 as u64),
                e.csw as u32 as u64,
            );
        }
    }
    (0, 0)
}

/// The same sequential chain in local mode, for the replicated/local ratio.
/// `cargo test --release -p fusee-server --test replicated local_step_cost -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn local_step_cost() {
    let root = std::env::temp_dir().join(format!("fusee-local-cost-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let mut cfg = fusee::Config::local(&root);
    cfg.local = LocalOptions {
        flush: FlushMethod::Fsync,
        ..Default::default()
    };
    let engine = Engine::open(cfg).await.unwrap();
    let (p50, p99, sys, csw, n) = chain_cost(&engine).await;
    println!(
        "{{\"workload\":\"local-chain\",\"shards\":{},\"steps\":{n},\"p50_us\":{p50},\"p99_us\":{p99},\"syscalls_per_step\":{sys:.1},\"context_switches_per_step\":{csw:.1}}}",
        engine.shards()
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

/// p50 and p99 step latency of one sequential chain, with process-wide
/// system calls and context switches per step.
async fn chain_cost(engine: &Engine) -> (u64, u64, f64, f64, usize) {
    let marks = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
    let steps: u32 = std::env::var("FUSEE_LOOPBACK_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let wf = engine.register("cost-chain", {
        let marks = marks.clone();
        move |ctx: Ctx, n: u32| {
            let marks = marks.clone();
            async move {
                let mut v = 0u32;
                for i in 0..n {
                    let marks = marks.clone();
                    v = ctx
                        .step(&format!("s{i}"), || {
                            let marks = marks.clone();
                            async move {
                                marks.lock().unwrap().push(Instant::now());
                                Ok::<_, Error>(v + 1)
                            }
                        })
                        .await?;
                }
                Ok(v)
            }
        }
    });
    engine
        .start(&wf, "cost-warm", &50)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    marks.lock().unwrap().clear();
    let before = task_events();
    let h = engine.start(&wf, "cost", &steps).await.unwrap();
    assert_eq!(h.result().await.unwrap(), steps);
    let after = task_events();
    let m = std::mem::take(&mut *marks.lock().unwrap());
    let mut gaps: Vec<u64> = m
        .windows(2)
        .map(|w| (w[1] - w[0]).as_micros() as u64)
        .collect();
    gaps.sort_unstable();
    let q = |p: f64| gaps[((gaps.len() - 1) as f64 * p) as usize];
    let per = |a: u64, b: u64| (b.saturating_sub(a)) as f64 / f64::from(steps);
    (
        q(0.5),
        q(0.99),
        per(before.0, after.0),
        per(before.1, after.1),
        gaps.len(),
    )
}

/// Loopback latency and throughput of a three-member group in one process
/// (a development figure: the members share this host's CPU and disk).
/// `cargo test --release -p fusee-server --test replicated loopback_step_cost -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn loopback_step_cost() {
    let root = std::env::temp_dir().join(format!("fusee-loopback-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let mut nodes = Vec::new();
    for id in 0..3 {
        nodes.push(Some(start(id, &peers, &root).await));
    }
    let p = ready_one(&mut nodes).await;
    let engine = Engine::open(nodes[p].as_ref().unwrap().engine_config())
        .await
        .unwrap();
    let marks = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
    let steps: u32 = std::env::var("FUSEE_LOOPBACK_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let wf = Arc::new(engine.register("chain", {
        let marks = marks.clone();
        move |ctx: Ctx, n: u32| {
            let marks = marks.clone();
            async move {
                let mut v = 0u32;
                for i in 0..n {
                    let marks = marks.clone();
                    v = ctx
                        .step(&format!("s{i}"), || {
                            let marks = marks.clone();
                            async move {
                                marks.lock().unwrap().push(Instant::now());
                                Ok::<_, Error>(v + 1)
                            }
                        })
                        .await?;
                }
                Ok(v)
            }
        }
    }));
    let (_, _, sys, csw, _) = chain_cost(&engine).await;
    // Warm up, then one long sequential chain, with every member's stage
    // timings (the step's critical path, docs/replication.md).
    engine
        .start(&wf, "warm", &50)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let observers: Vec<_> = nodes
        .iter()
        .map(|n| n.as_ref().unwrap().observer())
        .collect();
    for o in &observers {
        o.enable_timings();
    }
    let before: Vec<_> = observers.iter().map(|o| o.timings()).collect();
    marks.lock().unwrap().clear();
    let h = engine.start(&wf, "chain", &steps).await.unwrap();
    assert_eq!(h.result().await.unwrap(), steps);
    for (i, (o, b)) in observers.iter().zip(&before).enumerate() {
        let role = if i == p { "primary" } else { "backup" };
        println!(
            "{{\"workload\":\"loopback-chain-stages\",\"member\":{i},\"role\":\"{role}\",\"stages\":{}}}",
            fusee_server::replicated::stage_summary(b, &o.timings())
        );
    }
    let m = std::mem::take(&mut *marks.lock().unwrap());
    let mut gaps: Vec<u64> = m
        .windows(2)
        .map(|w| (w[1] - w[0]).as_micros() as u64)
        .collect();
    gaps.sort_unstable();
    let q = |p: f64| gaps[((gaps.len() - 1) as f64 * p) as usize];
    // Throughput: many concurrent short chains, started concurrently.
    let runs: u32 = std::env::var("FUSEE_LOOPBACK_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let each = 10u32;
    let started = Instant::now();
    let tasks: Vec<_> = (0..runs)
        .map(|i| {
            let (wf, engine) = (wf.clone(), engine.clone());
            tokio::spawn(async move {
                let h = engine.start(&wf, &format!("t{i}"), &each).await.unwrap();
                h.result().await.unwrap()
            })
        })
        .collect();
    for t in tasks {
        assert_eq!(t.await.unwrap(), each);
    }
    let rate = f64::from(runs * each) / started.elapsed().as_secs_f64();
    println!(
        "{{\"workload\":\"loopback-chain\",\"shards\":{},\"steps\":{},\"p50_us\":{},\"p90_us\":{},\"p99_us\":{},\"concurrent_steps_per_s\":{:.0},\"syscalls_per_step\":{sys:.1},\"context_switches_per_step\":{csw:.1}}}",
        engine.shards(),
        gaps.len(),
        q(0.5),
        q(0.9),
        q(0.99),
        rate
    );
    engine.shutdown().await.unwrap();
    drop(nodes);
    let _ = std::fs::remove_dir_all(root);
}

async fn start_timed(
    id: u8,
    peers: &[String],
    root: &std::path::Path,
    timing: Timing,
) -> ReplicaNode {
    ReplicaNode::start(ReplicaConfig {
        id,
        peers: peers.to_vec(),
        join: false,
        dir: root.join(format!("r{id}")),
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            ..Default::default()
        },
        timing,
        security: Default::default(),
    })
    .await
    .unwrap()
}

/// With the default timing a ready primary holds a read lease while its
/// backups answer; cut off from both, it loses the lease within the lease
/// time, steps down within an election timeout, and a read that must not
/// be stale is refused rather than served from its state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_off_primary_loses_its_read_lease_then_steps_down() {
    let root = std::env::temp_dir().join(format!("fusee-lease-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers = free_ports(3);
    let timing = Timing::default();
    let mut nodes: Vec<Option<ReplicaNode>> = Vec::new();
    for id in 0..3u8 {
        nodes.push(Some(start_timed(id, &peers, &root, timing.clone()).await));
    }
    let p = ready_one(&mut nodes).await;
    let reads = nodes[p].as_ref().unwrap().reads();
    // Steady state: the lease is held nearly all the time (each heartbeat
    // renews it; a load spike can make it lapse briefly).
    let (mut held, mut samples) = (0u32, 0u32);
    let until = Instant::now() + Duration::from_millis(500);
    while Instant::now() < until {
        samples += 1;
        held += u32::from(reads.lease_valid());
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    println!("lease held in {held} of {samples} samples");
    assert!(held > 0, "a healthy primary never held a read lease");
    tokio::time::timeout(Duration::from_secs(2), reads.confirm_reads())
        .await
        .expect("confirming reads timed out")
        .expect("a healthy primary could not confirm its reads");
    let status = nodes[p].as_ref().unwrap().observer();
    for (i, n) in nodes.iter_mut().enumerate() {
        if i != p {
            n.take().unwrap().kill();
        }
    }
    let cut = Instant::now();
    while reads.lease_valid() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let lapsed = cut.elapsed();
    let deadline = Instant::now() + Duration::from_secs(5);
    while status.snapshot().0.ready {
        assert!(
            Instant::now() < deadline,
            "the cut-off primary never stepped down"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let stepped = cut.elapsed();
    println!("cut off: lease lapsed after {lapsed:?}, stepped down after {stepped:?}");
    // The lease time plus a heartbeat, and slack for a loaded host.
    assert!(
        lapsed <= Duration::from_millis(timing.lease_ms() + timing.heartbeat_ms + 250),
        "the lease outlived its bound: {lapsed:?}"
    );
    let refused = tokio::time::timeout(Duration::from_secs(2), reads.confirm_reads()).await;
    assert!(
        !matches!(refused, Ok(Ok(()))),
        "a primary without a quorum confirmed its reads"
    );
    drop(nodes);
    let _ = std::fs::remove_dir_all(root);
}

async fn start_member(id: u8, peers: &[String], root: &std::path::Path, join: bool) -> ReplicaNode {
    ReplicaNode::start(ReplicaConfig {
        id,
        peers: peers.to_vec(),
        join,
        dir: root.join(format!("r{id}")),
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            ..Default::default()
        },
        // Slower failure detection: concurrent tests make flushes slow.
        timing: Timing {
            heartbeat_ms: 20,
            election_ms: 150,
        },
        security: Default::default(),
    })
    .await
    .unwrap()
}

fn doubler(engine: &Engine) -> fusee::WorkflowRef<u32, u32> {
    engine.register("double", |ctx: Ctx, n: u32| async move {
        ctx.step("x2", || async move { Ok::<_, Error>(n * 2) })
            .await
    })
}

/// Waits until every listed member is in `epoch`, not joining, and has
/// caught up with the primary's commit.
async fn members_in(nodes: &[Option<ReplicaNode>], ids: &[usize], epoch: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let commit = nodes
            .iter()
            .flatten()
            .map(|n| n.status())
            .filter(|s| s.ready)
            .map(|s| s.commit)
            .max();
        let done = commit.is_some_and(|c| {
            ids.iter().all(|i| {
                let s = nodes[*i].as_ref().unwrap().status();
                s.epoch == epoch && !s.joining && !s.retired && s.commit >= c
            })
        });
        if done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "members {ids:?} did not reach epoch {epoch}: {:?}",
            ids.iter()
                .map(|i| nodes[*i].as_ref().unwrap().status())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A running group changes its membership while workflows run: 3 → 5 (two
/// new members join), 5 → 3 (the primary and a new member leave), then a
/// member is lost for good and replaced by a new one. Every change is
/// requested from a member that is not the primary. After each, the new
/// primary's engine recovers every result, removed members retire, and the
/// new members catch up.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn membership_changes_three_to_five_to_three_and_replace_a_lost_member() {
    let root = std::env::temp_dir().join(format!("fusee-members-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let all = free_ports(6);
    let mut nodes: Vec<Option<ReplicaNode>> = (0..6).map(|_| None).collect();
    for id in 0..3u8 {
        nodes[usize::from(id)] = Some(start_member(id, &all[..3], &root, false).await);
    }
    let mut results: Vec<(String, u32)> = Vec::new();
    // Runs `count` workflows on the current primary and checks every result
    // so far, then stops its engine (a membership change deposes it).
    async fn round(
        nodes: &mut [Option<ReplicaNode>],
        results: &mut Vec<(String, u32)>,
        count: u32,
    ) {
        let p = ready_one(nodes).await;
        let engine = Engine::open(nodes[p].as_ref().unwrap().engine_config())
            .await
            .unwrap();
        let wf = doubler(&engine);
        for (id, want) in results.iter() {
            let got: u32 = engine.run::<u32>(id).result().await.unwrap();
            assert_eq!(got, *want, "{id} after a membership change");
        }
        let base = results.len() as u32;
        for n in base..base + count {
            let id = format!("w{n}");
            let h = engine.start(&wf, &id, &n).await.unwrap();
            assert_eq!(h.result().await.unwrap(), n * 2);
            results.push((id, n * 2));
        }
        engine.shutdown().await.unwrap();
    }
    let members = |ids: &[usize]| -> Vec<(u8, String)> {
        ids.iter().map(|i| (*i as u8, all[*i].clone())).collect()
    };
    let backup = |nodes: &[Option<ReplicaNode>], among: &[usize]| -> usize {
        *among
            .iter()
            .find(|i| nodes[**i].as_ref().is_some_and(|n| !n.status().primary))
            .unwrap()
    };
    round(&mut nodes, &mut results, 10).await;

    // 3 → 5: two new members join.
    for id in [3u8, 4] {
        nodes[usize::from(id)] = Some(start_member(id, &all[..5], &root, true).await);
    }
    assert!(nodes[3].as_ref().unwrap().status().joining);
    let via = backup(&nodes, &[0, 1, 2]);
    let epoch = nodes[via]
        .as_ref()
        .unwrap()
        .change_members(members(&[0, 1, 2, 3, 4]), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(epoch, 1);
    round(&mut nodes, &mut results, 10).await;
    members_in(&nodes, &[0, 1, 2, 3, 4], 1).await;

    // 5 → 3: the primary and one of the new members leave.
    let p = ready_one(&mut nodes).await;
    let leaving = [p, 3];
    let staying: Vec<usize> = (0..5).filter(|i| !leaving.contains(i)).collect();
    let via = backup(&nodes, &staying);
    let epoch = nodes[via]
        .as_ref()
        .unwrap()
        .change_members(members(&staying), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(epoch, 2);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !leaving
        .iter()
        .all(|i| nodes[*i].as_ref().unwrap().status().retired)
    {
        assert!(Instant::now() < deadline, "removed members did not retire");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for i in leaving {
        nodes[i].take().unwrap().kill();
    }
    round(&mut nodes, &mut results, 10).await;
    members_in(&nodes, &staying, 2).await;

    // A member is lost for good; a new member replaces it.
    let p = ready_one(&mut nodes).await;
    let lost = *staying.iter().find(|i| **i != p).unwrap();
    nodes[lost].take().unwrap().kill();
    let mut next: Vec<usize> = staying.iter().copied().filter(|i| *i != lost).collect();
    next.push(5);
    let mut addresses = all.clone();
    addresses[lost] = String::new();
    for i in leaving_ids(&staying, &next) {
        addresses[i] = String::new();
    }
    nodes[5] = Some(start_member(5, &addresses, &root, true).await);
    let via = backup(&nodes, &next);
    let via = if nodes[via].as_ref().unwrap().status().joining {
        p
    } else {
        via
    };
    let epoch = nodes[via]
        .as_ref()
        .unwrap()
        .change_members(members(&next), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(epoch, 3);
    round(&mut nodes, &mut results, 10).await;
    members_in(&nodes, &next, 3).await;
    let (epoch, final_members) = nodes[5].as_ref().unwrap().membership();
    assert_eq!(epoch, 3);
    assert_eq!(final_members.len(), 3);
    println!(
        "membership: {} workflows across epochs 0 to 3, final members {final_members:?}",
        results.len()
    );
    drop(nodes);
    let _ = std::fs::remove_dir_all(root);
}

/// Members of `before` not in `after`.
fn leaving_ids(before: &[usize], after: &[usize]) -> Vec<usize> {
    before
        .iter()
        .copied()
        .filter(|i| !after.contains(i))
        .collect()
}
