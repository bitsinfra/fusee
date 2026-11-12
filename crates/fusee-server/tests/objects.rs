//! Keyed objects in server mode: a client calls keys on whichever worker
//! holds their shards, and a workflow on one worker calls a key another
//! worker holds (the call and its result relayed through the server).

use fusee::object::ObjectCtx;
use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_server::{Client, Server, ServerConfig};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("fusee-srv-obj-{name}-{}", std::process::id()));
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

fn register(engine: &Engine, ran: Arc<AtomicU32>) {
    engine
        .object_type("acct")
        .exclusive("deposit", move |ctx: ObjectCtx, n: u64| {
            let ran = ran.clone();
            async move {
                ran.fetch_add(1, Ordering::SeqCst);
                let b: u64 = ctx.state().get("balance")?.unwrap_or(0);
                ctx.state().set("balance", &(b + n))?;
                Ok(b + n)
            }
        })
        .shared("balance", |ctx: ObjectCtx, _: ()| async move {
            Ok(ctx.state().get::<u64>("balance")?.unwrap_or(0))
        })
        .register()
        .unwrap();
    engine.register("payer", |ctx: Ctx, (key, n): (String, u64)| async move {
        let after: u64 = ctx.object("acct", &key).call("deposit", &n).await?;
        ctx.object("acct", &key).send("deposit", &1u64)?;
        Ok(after)
    });
}

struct Worker {
    engine: Engine,
    remote: fusee_server::remote::Remote,
}

async fn worker(addr: &str, most: u32, ran: Arc<AtomicU32>) -> Worker {
    let a = format!("{addr}?max-shards={most}");
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    register(&engine, ran);
    tokio::spawn(fusee_server::serve_calls(remote.clone(), engine.clone()));
    Worker { engine, remote }
}

/// A key whose run's shard is in `shards`.
fn key_on(prefix: &str, shards: &[u32], of: u32) -> String {
    (0..)
        .map(|k| format!("{prefix}-{k}"))
        .find(|k| {
            shards.contains(&fusee_core::shard::shard_of(
                &fusee::object::object_id("acct", k),
                of,
            ))
        })
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clients_and_workflows_call_keys_on_any_worker() {
    const SHARDS: u32 = 4;
    let dir = tmp("cross");
    let (addr, _server) = start_server(&dir, SHARDS).await;
    let ran = Arc::new(AtomicU32::new(0));
    let a = worker(&addr, 2, ran.clone()).await;
    let b = worker(&addr, 2, ran.clone()).await;
    let (on_a, on_b) = (a.remote.held_shards(), b.remote.held_shards());
    assert_eq!((on_a.len(), on_b.len()), (2, 2));
    let (ka, kb) = (key_on("a", &on_a, SHARDS), key_on("b", &on_b, SHARDS));

    // A client: calls, a retry with the same ID, a one-way call, the state.
    let client = Client::connect(&addr).unwrap();
    let (c, ka2, kb2) = (client.clone(), ka.clone(), kb.clone());
    tokio::task::spawn_blocking(move || {
        for k in [&ka2, &kb2] {
            assert_eq!(
                c.object_call("acct", k, "deposit", &json!(5), "d-1")
                    .unwrap(),
                json!(5)
            );
            assert_eq!(
                c.object_call("acct", k, "deposit", &json!(5), "d-1")
                    .unwrap(),
                json!(5)
            );
            assert_eq!(
                c.object_call("acct", k, "deposit", &json!(2), "").unwrap(),
                json!(7)
            );
            c.object_send("acct", k, "deposit", &json!(3), "s-1")
                .unwrap();
            c.object_send("acct", k, "deposit", &json!(3), "s-1")
                .unwrap();
            assert_eq!(
                c.object_call("acct", k, "balance", &json!(null), "")
                    .unwrap(),
                json!(10)
            );
            assert_eq!(c.object_state("acct", k).unwrap(), json!({"balance": 10}));
        }
        assert_eq!(c.object_state("acct", "never").unwrap(), json!({}));
    })
    .await
    .unwrap();
    assert_eq!(ran.load(Ordering::SeqCst), 6);

    // A workflow on A calls a key on B, and the other way round.
    for (side, key, name) in [(&a, &kb, "ab"), (&b, &ka, "ba")] {
        let payer = (0..)
            .map(|i| format!("payer-{name}-{i}"))
            .find(|id| {
                side.remote
                    .held_shards()
                    .contains(&fusee_core::shard::shard_of(id, SHARDS))
            })
            .unwrap();
        let input: Arc<[u8]> = Arc::from(serde_json::to_vec(&(key, 100u64)).unwrap());
        side.engine
            .start_raw("payer", &payer, input, Default::default())
            .await
            .unwrap();
        let after = tokio::time::timeout(
            Duration::from_secs(30),
            side.engine.run::<u64>(&payer).result(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(after, 110, "{payer} called {key}");
    }
    let (c, ka2, kb2) = (client.clone(), ka.clone(), kb.clone());
    tokio::task::spawn_blocking(move || {
        'keys: for k in [&ka2, &kb2] {
            // The send from the workflow arrives too, once.
            for _ in 0..1000 {
                let s = c.object_state("acct", k).unwrap();
                if s == json!({"balance": 111}) {
                    continue 'keys;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            panic!("{k}: {:?}", c.object_state("acct", k));
        }
    })
    .await
    .unwrap();
    let _: Option<Error> = None;
    a.engine.shutdown().await.unwrap();
    b.engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
