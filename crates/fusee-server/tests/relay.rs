//! Messages between workflows on shards held by different workers (child
//! starts and results, signals, cancels) and their acknowledgments go
//! through the server, survive either worker dying, a lease moving while a
//! message is in flight, and duplicates.

use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_core::frame;
use fusee_core::wire::{Reader, put_bytes, put_u32, put_u64, put_varint};
use fusee_server::proto::{self, req, resp};
use fusee_server::{Server, ServerConfig};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("fusee-relay-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn start_server(dir: &std::path::Path, shards: u32) -> (String, Arc<Server>) {
    let server = Server::new(ServerConfig {
        dir: dir.to_owned(),
        shards,
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            ..Default::default()
        },
        security: Default::default(),
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fusee_server::server::serve_tcp(server.clone(), listener));
    (addr, server)
}

/// How often each workflow's step body ran, by workflow ID.
type Runs = Arc<Mutex<HashMap<String, u32>>>;

fn ran(runs: &Runs, id: &str) -> u32 {
    runs.lock().unwrap().get(id).copied().unwrap_or(0)
}

fn register(engine: &Engine, runs: Runs) {
    let r = runs.clone();
    engine.register("child", move |ctx: Ctx, n: u32| {
        let r = r.clone();
        async move {
            let id = ctx.workflow_id().to_owned();
            let v: u32 = ctx
                .step("work", move || {
                    let (r, id) = (r.clone(), id.clone());
                    async move {
                        *r.lock().unwrap().entry(id).or_default() += 1;
                        Ok::<_, Error>(n * 2)
                    }
                })
                .await?;
            Ok(v)
        }
    });
    let r = runs.clone();
    engine.register("slow_child", move |ctx: Ctx, n: u32| {
        let r = r.clone();
        async move {
            let id = ctx.workflow_id().to_owned();
            let v: u32 = ctx
                .step("work", move || {
                    let (r, id) = (r.clone(), id.clone());
                    async move {
                        *r.lock().unwrap().entry(id).or_default() += 1;
                        Ok::<_, Error>(n * 2)
                    }
                })
                .await?;
            let go: u32 = ctx.wait_for("go").await?.unwrap_or(0);
            Ok(v + go)
        }
    });
    engine.register(
        "parent",
        |ctx: Ctx, (ty, child): (String, String)| async move {
            let v: u32 = ctx.child(&ty, &child, &7u32).await?;
            Ok(v + 1)
        },
    );
    engine.register("waiter", |ctx: Ctx, _: u32| async move {
        let first: u32 = ctx.wait_for("poke").await?.unwrap_or(0);
        Ok(first)
    });
    engine.register("poker", |ctx: Ctx, target: String| async move {
        ctx.signal_workflow(&target, "poke", &42u32)?;
        Ok(0u32)
    });
    engine.register("victim", |ctx: Ctx, _: u32| async move {
        let v: u32 = ctx.wait_for("never").await?.unwrap_or(0);
        Ok(v)
    });
    engine.register("canceller", |ctx: Ctx, target: String| async move {
        ctx.cancel_workflow(&target, "bye")?;
        Ok(0u32)
    });
}

struct Worker {
    engine: Engine,
    remote: fusee_server::remote::Remote,
    runs: Runs,
}

async fn worker(addr: &str, most: u32) -> Worker {
    let a = format!("{addr}?max-shards={most}");
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let runs = Runs::default();
    register(&engine, runs.clone());
    tokio::spawn(fusee_server::serve_calls(remote.clone(), engine.clone()));
    Worker {
        engine,
        remote,
        runs,
    }
}

/// An ID whose shard is in `shards`, starting with `prefix`.
fn id_on(prefix: &str, shards: &[u32], of: u32) -> String {
    (0..)
        .map(|k| format!("{prefix}-{k}"))
        .find(|id| shards.contains(&fusee_core::shard::shard_of(id, of)))
        .unwrap()
}

async fn start(engine: &Engine, ty: &str, id: &str, input: &impl serde::Serialize) {
    let input: Arc<[u8]> = Arc::from(serde_json::to_vec(input).unwrap());
    engine
        .start_raw(ty, id, input, Default::default())
        .await
        .unwrap();
}

async fn wait<O: serde::de::DeserializeOwned>(engine: &Engine, id: &str) -> Result<O, Error> {
    tokio::time::timeout(Duration::from_secs(30), engine.run::<O>(id).result())
        .await
        .map_err(|_| Error::TimedOut)?
}

async fn result<O: serde::de::DeserializeOwned>(
    engine: &Engine,
    ty: &str,
    id: &str,
    input: &impl serde::Serialize,
) -> Result<O, Error> {
    start(engine, ty, id, input).await;
    wait(engine, id).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn children_signals_and_cancels_cross_workers_and_listing_covers_both() {
    const SHARDS: u32 = 4;
    let dir = tmp("cross");
    let (addr, _server) = start_server(&dir, SHARDS).await;
    let a = worker(&addr, 2).await;
    let b = worker(&addr, 2).await;
    let (on_a, on_b) = (a.remote.held_shards(), b.remote.held_shards());
    assert_eq!((on_a.len(), on_b.len()), (2, 2));

    // A parent on A's shards, its child on B's, and the other way round.
    for (parent_side, child_side, name) in [(&a, &on_b, "ab"), (&b, &on_a, "ba")] {
        let parent_shards = parent_side.remote.held_shards();
        let parent = id_on(&format!("parent-{name}"), &parent_shards, SHARDS);
        let child = id_on(&format!("child-{name}"), child_side, SHARDS);
        let v: u32 = result(
            &parent_side.engine,
            "parent",
            &parent,
            &("child", child.as_str()),
        )
        .await
        .unwrap();
        assert_eq!(v, 15, "{parent} got its child's result");
        assert_eq!(ran(&a.runs, &child) + ran(&b.runs, &child), 1);
    }

    // A signal from a workflow on B to one on A.
    let waiter = id_on("waiter", &on_a, SHARDS);
    start(&a.engine, "waiter", &waiter, &0).await;
    let poker = id_on("poker", &on_b, SHARDS);
    let _: u32 = result(&b.engine, "poker", &poker, &waiter).await.unwrap();
    assert_eq!(wait::<u32>(&a.engine, &waiter).await.unwrap(), 42);

    // A cancel from a workflow on A to one on B.
    let victim = id_on("victim", &on_b, SHARDS);
    start(&b.engine, "victim", &victim, &0).await;
    let canceller = id_on("canceller", &on_a, SHARDS);
    let _: u32 = result(&a.engine, "canceller", &canceller, &victim)
        .await
        .unwrap();
    let r = wait::<u32>(&b.engine, &victim).await;
    assert!(matches!(r, Err(Error::Cancelled(_))), "{r:?}");

    // A client's listing reaches both workers.
    let client = fusee_server::Client::connect(&addr).unwrap();
    let listed = tokio::task::spawn_blocking(move || {
        let all = client.list(100).unwrap();
        let only = client
            .list_where(&serde_json::json!({"type": "victim"}), 100)
            .unwrap();
        (all, only)
    })
    .await
    .unwrap();
    let ids: Vec<&str> = listed
        .0
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    for id in [&waiter, &poker, &victim, &canceller] {
        assert!(ids.contains(&id.as_str()), "{id} missing from {ids:?}");
    }
    let shards: Vec<u64> = listed
        .0
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["shard"].as_u64().unwrap())
        .collect();
    assert!(
        shards.windows(2).all(|w| w[0] <= w[1]),
        "in shard order: {shards:?}"
    );
    let only = listed.1.as_array().unwrap();
    assert_eq!(only.len(), 1);
    assert_eq!(only[0]["id"], victim.as_str());

    a.engine.shutdown().await.unwrap();
    b.engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A TCP proxy between one worker and the server that can stop forwarding
/// (a hung worker) or close both sides at once (a worker process dying).
struct Proxy {
    addr: String,
    frozen: tokio::sync::watch::Sender<bool>,
    pipes: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Proxy {
    async fn start(target: String) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (frozen, gate) = tokio::sync::watch::channel(false);
        let pipes = Arc::new(Mutex::new(Vec::new()));
        let p = pipes.clone();
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let outbound = tokio::net::TcpStream::connect(&target).await.unwrap();
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (wr, ww) = inbound.into_split();
                let (sr, sw) = outbound.into_split();
                let mut p = p.lock().unwrap();
                p.push(tokio::spawn(pipe(wr, sw, gate.clone())));
                p.push(tokio::spawn(pipe(sr, ww, gate.clone())));
            }
        });
        Self {
            addr,
            frozen,
            pipes,
        }
    }

    fn freeze(&self) {
        self.frozen.send_replace(true);
    }

    fn kill(&self) {
        for p in self.pipes.lock().unwrap().drain(..) {
            p.abort();
        }
    }
}

async fn pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut gate: tokio::sync::watch::Receiver<bool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = from.read(&mut buf).await.unwrap_or(0);
        if gate.wait_for(|frozen| !*frozen).await.is_err() || n == 0 {
            break;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
}

/// The child's worker hangs with the child's start in flight and dies; a
/// standby takes its shards over and runs the child, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_whose_target_lease_moves_is_delivered_to_the_next_holder_once() {
    const SHARDS: u32 = 4;
    let dir = tmp("moves");
    let (addr, _server) = start_server(&dir, SHARDS).await;
    let a = worker(&addr, 2).await;
    let proxy = Proxy::start(addr.clone()).await;
    let b = worker(&proxy.addr, 2).await;
    let on_b = b.remote.held_shards();
    // C waits for shards.
    let c_addr = addr.clone();
    let standby = tokio::spawn(async move { worker(&c_addr, 2).await });

    proxy.freeze();
    let parent = id_on("parent-moves", &a.remote.held_shards(), SHARDS);
    let child = id_on("child-moves", &on_b, SHARDS);
    start(&a.engine, "parent", &parent, &("child", child.as_str())).await;
    // The start reaches B's frozen pipe; then B dies.
    tokio::time::sleep(Duration::from_millis(300)).await;
    proxy.kill();
    let c = tokio::time::timeout(Duration::from_secs(30), standby)
        .await
        .expect("C never got B's shards")
        .unwrap();
    assert_eq!(c.remote.held_shards(), on_b);
    let v: u32 = wait(&a.engine, &parent)
        .await
        .expect("the parent never got its child's result");
    assert_eq!(v, 15);
    assert_eq!(ran(&b.runs, &child), 0, "the frozen worker ran the child");
    assert_eq!(ran(&c.runs, &child), 1, "the child ran once on C");
    // One run of the child exists.
    let only: Vec<_> = c
        .engine
        .list_where_in(&on_b, &serde_json::json!({"type": "child"}), 100)
        .await
        .unwrap();
    assert_eq!(only.len(), 1, "{only:?}");
    a.engine.shutdown().await.unwrap();
    c.engine.shutdown().await.unwrap();
    drop(b);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The parent's worker dies while its child runs on another worker; the
/// child's result reaches the parent's next holder, and the child stays one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_parents_worker_dies_and_its_successor_gets_the_childs_result() {
    const SHARDS: u32 = 4;
    let dir = tmp("parent-dies");
    let (addr, _server) = start_server(&dir, SHARDS).await;
    let proxy = Proxy::start(addr.clone()).await;
    let a = worker(&proxy.addr, 2).await;
    let b = worker(&addr, 2).await;
    let on_a = a.remote.held_shards();
    let parent = id_on("parent-dies", &on_a, SHARDS);
    let child = id_on("child-dies", &b.remote.held_shards(), SHARDS);
    start(
        &a.engine,
        "parent",
        &parent,
        &("slow_child", child.as_str()),
    )
    .await;
    // The child ran its step on B and waits.
    for _ in 0..1000 {
        if ran(&b.runs, &child) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(ran(&b.runs, &child), 1);
    let c_addr = addr.clone();
    let standby = tokio::spawn(async move { worker(&c_addr, 2).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    proxy.kill();
    let c = tokio::time::timeout(Duration::from_secs(30), standby)
        .await
        .expect("C never got A's shards")
        .unwrap();
    assert_eq!(c.remote.held_shards(), on_a);
    // The child finishes; its result goes to C, which holds the parent now.
    b.engine
        .run::<u32>(&child)
        .signal("go", &3u32)
        .await
        .unwrap();
    let v: u32 = wait(&c.engine, &parent)
        .await
        .expect("the parent never finished on C");
    assert_eq!(v, 18);
    assert_eq!(ran(&b.runs, &child), 1, "the child's step ran again");
    a.engine.shutdown().await.ok();
    b.engine.shutdown().await.unwrap();
    c.engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A raw worker connection: holds `most` shards and records what arrives.
struct Fake {
    sock: std::net::TcpStream,
    grant: Vec<(u32, u64)>,
    frames: std::sync::mpsc::Receiver<(u8, Vec<u8>)>,
}

impl Fake {
    fn connect(addr: &str, most: u32) -> Self {
        let mut sock = std::net::TcpStream::connect(addr).unwrap();
        sock.set_nodelay(true).unwrap();
        let mut b = Vec::new();
        put_u64(&mut b, 1);
        put_u32(
            &mut b,
            proto::SHARED_LOG | proto::SHARD_LEASES | proto::MESSAGE_RELAY,
        );
        put_u32(&mut b, most);
        let mut out = Vec::new();
        frame::encode(req::HELLO, &b, &mut out);
        sock.write_all(&out).unwrap();
        let mut dec = frame::Decoder::new();
        let mut buf = vec![0u8; 1 << 16];
        let (kind, body) = loop {
            if let Some(f) = dec.next().unwrap() {
                break f;
            }
            let n = sock.read(&mut buf).unwrap();
            assert!(n > 0, "the server closed the connection");
            dec.feed(&buf[..n]);
        };
        assert_eq!(kind, resp::LEASE);
        let mut r = Reader::new(&body);
        let (_id, _shards, _epoch, caps) = (r.u64(), r.u32(), r.u64(), r.u32().unwrap());
        assert_ne!(caps & proto::MESSAGE_RELAY, 0, "the server relays messages");
        if caps & proto::FIRST_SEGMENT_LIMIT != 0 {
            r.u64().unwrap();
        }
        let n = r.u32().unwrap();
        let grant = (0..n)
            .map(|_| (r.u32().unwrap(), r.u64().unwrap()))
            .collect();
        let (tx, frames) = std::sync::mpsc::channel();
        let mut reader = sock.try_clone().unwrap();
        std::thread::spawn(move || {
            loop {
                while let Ok(Some(f)) = dec.next() {
                    if tx.send(f).is_err() {
                        return;
                    }
                }
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => dec.feed(&buf[..n]),
                }
            }
        });
        Self {
            sock,
            grant,
            frames,
        }
    }

    fn shard(&self) -> (u32, u64) {
        self.grant[0]
    }

    fn send(&mut self, kind: u8, shard: u32, epoch: u64, body: &[u8]) {
        let mut b = Vec::new();
        proto::head(
            &mut b,
            proto::Head {
                id: 0,
                shard,
                epoch,
            },
        );
        b.extend_from_slice(body);
        let mut out = Vec::new();
        frame::encode(kind, &b, &mut out);
        self.sock.write_all(&out).unwrap();
    }

    fn relay(&mut self, epoch: u64, target: u32, entries: &[u8]) {
        let (shard, _) = self.shard();
        let mut b = Vec::new();
        put_u32(&mut b, target);
        put_bytes(&mut b, entries);
        self.send(req::RELAY, shard, epoch, &b);
    }

    fn ack(&mut self, acks: &[(u32, u64, u64)]) {
        let (shard, epoch) = self.shard();
        let mut b = Vec::new();
        put_u32(&mut b, acks.len() as u32);
        for &(s, e, id) in acks {
            put_u32(&mut b, s);
            put_u64(&mut b, e);
            put_u64(&mut b, id);
        }
        self.send(req::RELAY_ACKS, shard, epoch, &b);
    }

    /// The next relayed frame, skipping `DURABLE` and the like.
    fn next(&self, within: Duration) -> Option<(u8, Vec<u8>)> {
        let deadline = std::time::Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            match self.frames.recv_timeout(left) {
                Ok((k, b)) if k == resp::MESSAGES || k == resp::MESSAGE_ACKS => {
                    return Some((k, b));
                }
                Ok(_) => {}
                Err(_) => return None,
            }
        }
    }
}

/// `MESSAGES` body: `(to, from, epoch, entries)`.
fn messages(body: &[u8]) -> (u32, u32, u64, Vec<u8>) {
    let mut r = Reader::new(body);
    r.u64().unwrap();
    (
        r.u32().unwrap(),
        r.u32().unwrap(),
        r.u64().unwrap(),
        r.bytes().unwrap().to_vec(),
    )
}

/// `MESSAGE_ACKS` body: `(shard, epoch, id)` each.
fn acks(body: &[u8]) -> Vec<(u32, u64, u64)> {
    let mut r = Reader::new(body);
    r.u64().unwrap();
    let n = r.u32().unwrap();
    (0..n)
        .map(|_| (r.u32().unwrap(), r.u64().unwrap(), r.u64().unwrap()))
        .collect()
}

/// One outbox entry carrying a signal, as `encode_outbox` writes it.
fn signal_entry(id: u64, target_shard: u32, target: &str, value: u32, dedup: u64) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, 1);
    put_varint(&mut out, id);
    put_varint(&mut out, u64::from(target_shard));
    out.push(3);
    put_bytes(&mut out, target.as_bytes());
    put_bytes(&mut out, b"poke");
    put_bytes(&mut out, value.to_string().as_bytes());
    put_varint(&mut out, dedup);
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_server_forwards_messages_to_holders_and_acks_only_to_their_lease() {
    let dir = tmp("forward");
    let (addr, _server) = start_server(&dir, 3).await;
    let a = addr.clone();
    let (mut f1, mut f2) =
        tokio::task::spawn_blocking(move || (Fake::connect(&a, 1), Fake::connect(&a, 1)))
            .await
            .unwrap();
    let ((s1, e1), (s2, e2)) = (f1.shard(), f2.shard());
    let s3 = (0..3).find(|s| *s != s1 && *s != s2).unwrap();
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        // A message from s1 reaches s2's holder with s1's epoch.
        f1.relay(e1, s2, b"one");
        let (k, b) = f2.next(Duration::from_secs(5)).expect("no message");
        assert_eq!(k, resp::MESSAGES);
        assert_eq!(messages(&b), (s2, s1, e1, b"one".to_vec()));
        // A stale epoch is not s1's lease: nothing is forwarded.
        f1.relay(e1 + 100, s2, b"stale");
        f1.relay(e1, s2, b"two");
        let (_, b) = f2.next(Duration::from_secs(5)).expect("no message");
        assert_eq!(messages(&b).3, b"two");
        // Acknowledgments reach the sender's lease only.
        f2.ack(&[(s1, e1 + 7, 10)]);
        f2.ack(&[(s1, e1, 9)]);
        let (k, b) = f1.next(Duration::from_secs(5)).expect("no ack");
        assert_eq!(k, resp::MESSAGE_ACKS);
        assert_eq!(acks(&b), vec![(s1, e1, 9)]);
        // A message for a shard nobody holds waits for its holder.
        f1.relay(e1, s3, b"kept");
        std::thread::sleep(Duration::from_millis(100));
        let f3 = Fake::connect(&a, 1);
        assert_eq!(f3.shard().0, s3);
        let (_, b) = f3.next(Duration::from_secs(5)).expect("kept message");
        assert_eq!(messages(&b), (s3, s1, e1, b"kept".to_vec()));
        // s1's holder goes; an ack for its lease reaches no later holder.
        drop(f1);
        std::thread::sleep(Duration::from_millis(200));
        f2.ack(&[(s1, e1, 11)]);
        let mut f4 = Fake::connect(&a, 1);
        let (s4, e4) = f4.shard();
        assert_eq!(s4, s1);
        assert!(e4 > e1);
        f2.relay(e2, s1, b"after");
        let (k, b) = f4.next(Duration::from_secs(5)).expect("no message");
        assert_eq!(k, resp::MESSAGES, "the old lease's ack reached the new one");
        assert_eq!(messages(&b), (s1, s2, e2, b"after".to_vec()));
        f4.ack(&[(s2, e2, 1)]);
        let (_, b) = f2.next(Duration::from_secs(5)).expect("no ack");
        assert_eq!(acks(&b), vec![(s2, e2, 1)]);
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A message relayed twice (a resend whose first acknowledgment was lost)
/// is applied once, and each delivery is acknowledged to the sender's
/// lease with the sender's own outbox ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_relayed_message_is_absorbed_and_acknowledged_each_time() {
    const SHARDS: u32 = 2;
    let dir = tmp("dup");
    let (addr, _server) = start_server(&dir, SHARDS).await;
    let a = addr.clone();
    let mut fake = tokio::task::spawn_blocking(move || Fake::connect(&a, 1))
        .await
        .unwrap();
    let (fs, fe) = fake.shard();
    let w = worker(&addr, 1).await;
    let on_w = w.remote.held_shards();
    assert_eq!(on_w.len(), 1);
    let waiter = id_on("dup-waiter", &on_w, SHARDS);
    let wf = w.engine.register("counter", |ctx: Ctx, _: u32| async move {
        let mut seen = Vec::new();
        while let Some(v) = ctx
            .wait_for::<u32>("poke")
            .timeout(Duration::from_millis(1500))
            .await?
        {
            seen.push(v);
        }
        Ok(seen)
    });
    let h = w.engine.start(&wf, &waiter, &0).await.unwrap();
    let target = on_w[0];
    let entry = signal_entry(5, target, &waiter, 7, 77);
    let other = signal_entry(6, target, &waiter, 8, 78);
    let got = tokio::task::spawn_blocking(move || {
        fake.relay(fe, target, &entry);
        fake.relay(fe, target, &entry);
        // From a stale lease: dropped by the server.
        fake.relay(fe + 50, target, &other);
        let mut got = Vec::new();
        while let Some((k, b)) = fake.next(Duration::from_secs(3)) {
            if k == resp::MESSAGE_ACKS {
                got.extend(acks(&b));
            }
            if got.len() >= 2 {
                break;
            }
        }
        got
    })
    .await
    .unwrap();
    assert_eq!(got, vec![(fs, fe, 5), (fs, fe, 5)], "one ack per delivery");
    let seen = tokio::time::timeout(Duration::from_secs(30), h.result())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        seen,
        vec![7],
        "the duplicate was absorbed, the stale one dropped"
    );
    w.engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
