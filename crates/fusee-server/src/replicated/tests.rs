//! Group tests that need crate internals: the proposal queue bound, status
//! notifications, and compaction while a configured member never says Hello.
use super::transfer::ABSENT_MS;
use super::*;
use fusee::{Ctx, Engine, Error};
use std::sync::atomic::AtomicU32;
use tokio::time::{sleep, timeout};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn free_ports(n: usize) -> Vec<String> {
    let ls: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect()
}

async fn member(id: u8, peers: &[String], root: &Path, election_ms: u64) -> ReplicaNode {
    ReplicaNode::start(ReplicaConfig {
        id,
        peers: peers.to_vec(),
        join: false,
        dir: root.join(format!("r{id}")),
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            ..Default::default()
        },
        timing: Timing {
            heartbeat_ms: 20,
            election_ms,
        },
        security: Default::default(),
    })
    .await
    .unwrap()
}

/// One ready primary, stable for 300 ms.
async fn ready_one(nodes: &[Option<ReplicaNode>]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
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
        assert!(Instant::now() < deadline, "no stable primary");
        sleep(Duration::from_millis(10)).await;
    }
}

async fn until(
    what: &str,
    deadline: Instant,
    nodes: &[Option<ReplicaNode>],
    mut done: impl FnMut(&[Option<ReplicaNode>]) -> bool,
) {
    while !done(nodes) {
        let statuses: Vec<_> = nodes
            .iter()
            .map(|n| n.as_ref().map(|n| n.status()))
            .collect();
        assert!(
            Instant::now() < deadline,
            "timed out waiting: {what}: {statuses:#?}"
        );
        sleep(Duration::from_millis(15)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_beyond_the_proposal_bound_commits_without_waking_status_waiters() {
    let root = scratch("proposal-burst");
    let peers = free_ports(3);
    let mut nodes = Vec::new();
    for id in 0..3 {
        // A long election timeout keeps one view for the whole burst.
        nodes.push(Some(member(id, &peers, &root, 1_000).await));
    }
    let p = ready_one(&nodes).await;
    let node = nodes[p].as_ref().unwrap();
    let mut watcher = node.subscribe();
    let before = *watcher.borrow_and_update();
    let limits = node.engine_config().limits.unwrap();
    const THREADS: usize = 8;
    const EACH: usize = PROPOSAL_QUEUE / 2;
    const PROBES: usize = 256;
    let spec = fusee::limits::LimitSpec {
        concurrent: 1,
        per_second: 0.0,
    };
    // Blocking producers, as the shard and permit threads are: four times
    // the queue bound, offered at once.
    let producers: Vec<_> = (0..THREADS)
        .map(|t| {
            let limits = limits.clone();
            std::thread::spawn(move || {
                for _ in 0..EACH {
                    limits.define(&format!("burst-{t}"), spec);
                }
            })
        })
        .collect();
    // Async producers share the same bound without blocking runtime threads.
    let storage = ReplicaStorage::new(node);
    let mut probes = tokio::task::JoinSet::new();
    for _ in 0..PROBES {
        let storage = storage.clone();
        probes.spawn(async move { storage.probe().await });
    }
    while let Some(probe) = probes.join_next().await {
        probe.unwrap().unwrap();
    }
    for producer in producers {
        producer.join().unwrap();
    }
    let target = before.commit + (THREADS * EACH + PROBES) as u64;
    until(
        "every proposal of the burst commits",
        Instant::now() + Duration::from_secs(60),
        &nodes,
        |_| node.status().applied >= target,
    )
    .await;
    let after = node.status();
    assert!(after.op >= target && after.commit >= target, "{after:?}");
    assert_eq!((after.view, after.ready), (before.view, before.ready));
    // Progress is visible by polling but wakes nobody.
    assert!(
        !watcher.has_changed().unwrap(),
        "commits woke status waiters"
    );
    drop(nodes);
    std::fs::remove_dir_all(root).unwrap();
}

fn register(engine: &Engine, calls: Arc<AtomicU32>) -> fusee::WorkflowRef<String, usize> {
    engine.register("absent-job", move |ctx: Ctx, data: String| {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_waits_only_for_reachable_members_and_a_late_member_catches_up() {
    let root = scratch("absent-member");
    let peers = free_ports(3);
    // Member 0, the primary of view 0, starts as an endpoint that accepts
    // connections and never says Hello, as a binary without checkpoint
    // support would.
    let silent = tokio::net::TcpListener::bind(&peers[0]).await.unwrap();
    let mute = tokio::spawn(async move {
        let mut readers = tokio::task::JoinSet::new();
        loop {
            let Ok((mut stream, _)) = silent.accept().await else {
                continue;
            };
            readers.spawn(async move {
                let mut buf = vec![0; 1 << 16];
                while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
            });
        }
    });
    // Long enough that fsync contention from parallel tests does not move
    // the view; each phase below expects a fixed primary.
    let election = 1_000;
    let mut nodes = vec![
        None,
        Some(member(1, &peers, &root, election).await),
        Some(member(2, &peers, &root, election).await),
    ];
    let p = ready_one(&nodes).await;
    let b = 3 - p;
    let setup = |node: &ReplicaNode| {
        node.engine_config()
            .checkpoint_interval(Duration::from_millis(50))
            .park_after(Duration::ZERO)
    };
    let engine = Engine::open(setup(nodes[p].as_ref().unwrap()))
        .await
        .unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let wf = register(&engine, calls.clone());
    let payload = "x".repeat(4096);
    engine.start(&wf, "kept", &payload).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while engine.metrics().await.unwrap().runs_parked == 0 {
        assert!(Instant::now() < deadline, "workflow failed to park");
        sleep(Duration::from_millis(15)).await;
    }
    // Reachable without Hello: the first checkpoint must wait.
    let hold = Instant::now() + Duration::from_millis(2 * ABSENT_MS);
    while Instant::now() < hold {
        let status = nodes[p].as_ref().unwrap().status();
        assert!(status.ready, "primary changed while holding: {status:?}");
        assert_eq!(
            status.checkpoint, 0,
            "compacted past a reachable member without checkpoint support"
        );
        sleep(Duration::from_millis(25)).await;
    }
    // The member goes away entirely: its connections close and new ones
    // are refused. After the window the log no longer waits for it.
    mute.abort();
    let _ = mute.await;
    let gone = Instant::now();
    until("the primary compacts", deadline, &nodes, |nodes| {
        nodes[p].as_ref().unwrap().status().checkpoint > 0
    })
    .await;
    // Reachability is sampled every 500 ms, so the window can start up to
    // one sample before the endpoint closed.
    assert!(
        gone.elapsed() >= Duration::from_millis(ABSENT_MS - 600),
        "compacted {:?} after the member left",
        gone.elapsed()
    );
    let base = nodes[p].as_ref().unwrap().status().checkpoint;
    assert!(!root.join(format!("r{p}/vsr/log")).exists());
    until(
        "the other backup installs the image",
        deadline,
        &nodes,
        |nodes| nodes[b].as_ref().unwrap().status().checkpoint >= base,
    )
    .await;

    // Member 0 starts for the first time, with an empty log in view 0, and
    // must take the image and a verified suffix rather than any log with a
    // hole.
    nodes[0] = Some(member(0, &peers, &root, election).await);
    let committed = nodes[p].as_ref().unwrap().status().commit;
    until("the late member catches up", deadline, &nodes, |nodes| {
        let s = nodes[0].as_ref().unwrap().status();
        s.checkpoint >= base && s.commit >= committed && !s.primary
    })
    .await;
    // It alone completes the primary's quorum now.
    nodes[b].take().unwrap().kill();
    let second = engine
        .start(&wf, "second", &"yy".to_string())
        .await
        .unwrap();
    second.signal("go", &1usize).await.unwrap();
    let result = timeout(Duration::from_secs(20), second.result())
        .await
        .expect("no quorum with the late member")
        .unwrap();
    assert_eq!(result, 3);
    engine.shutdown().await.unwrap();
    drop(second);
    drop(wf);
    drop(engine);

    // Without the old primary, the restarted backup and the late member
    // elect a primary from the late member's log, the only copy of the
    // second workflow outside the old primary.
    nodes[p].take().unwrap().kill();
    nodes[b] = Some(member(b as u8, &peers, &root, election).await);
    let q = ready_one(&nodes).await;
    let engine = Engine::open(setup(nodes[q].as_ref().unwrap()))
        .await
        .unwrap();
    let _wf = register(&engine, calls.clone());
    assert_eq!(
        timeout(
            Duration::from_secs(20),
            engine.run::<usize>("second").result()
        )
        .await
        .unwrap()
        .unwrap(),
        3
    );
    let kept = engine.run::<usize>("kept");
    kept.signal("go", &3usize).await.unwrap();
    let result = timeout(Duration::from_secs(20), kept.result())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, payload.len() + 3);
    assert_eq!(calls.load(Ordering::SeqCst), 2, "a recorded step ran again");
    engine.shutdown().await.unwrap();
    drop(engine);
    drop(nodes);
    std::fs::remove_dir_all(root).unwrap();
}
