//! What a call to a keyed object costs next to an update of a workflow:
//! latency (p50, p90, p99) and log records per call, sequential calls from
//! one caller, on a local directory (or `memory`).
//!
//! cargo run --release -p fusee --example object_cost -- [calls] [dir|memory]
//!
//! Both handlers read a counter, add one and return it; the object keeps it
//! in its state (committed with the call's result), the workflow in a
//! variable. A third run calls an object from workflow code (a journal
//! operation through the outbox), and a fourth makes concurrent calls to
//! one key (exclusive: they take turns) and to many keys.
use fusee::{Config, Ctx, Engine, Error, ObjectCtx};
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() as f64 * p) as usize).min(v.len() - 1)]
}

fn line(what: &str, mut lat: Vec<f64>, records: u64, n: usize) {
    println!(
        "{what:<34} p50 {:>7.1} µs  p90 {:>7.1}  p99 {:>7.1}  records/call {:.2}",
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.9),
        pct(&mut lat, 0.99),
        records as f64 / n as f64
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let n: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(5_000);
    let where_ = args.next().unwrap_or_else(|| {
        std::env::temp_dir()
            .join(format!("fusee-object-cost-{}", std::process::id()))
            .display()
            .to_string()
    });
    let cfg = if where_ == "memory" {
        Config::memory().shards(1)
    } else {
        let _ = std::fs::remove_dir_all(&where_);
        let mut c = Config::local(&where_).shards(1);
        // `fsync`: plain fsync, for development figures on macOS (it does
        // not flush the drive cache there).
        if args.next().as_deref() == Some("fsync") {
            c.local.flush = fusee::FlushMethod::Fsync;
        }
        c
    };
    let engine = Engine::open(cfg).await?;
    let counter = engine
        .object_type("counter")
        .exclusive("add", |ctx: ObjectCtx, _: ()| async move {
            let n: u64 = ctx.state().get("n")?.unwrap_or(0) + 1;
            ctx.state().set("n", &n)?;
            Ok(n)
        })
        .register()?;
    engine.register("holder", |ctx: Ctx, _: ()| async move {
        let n = Rc::new(Cell::new(0u64));
        let m = n.clone();
        ctx.on_update(
            "add",
            |_: &()| Ok(()),
            move |_: ()| {
                let m = m.clone();
                async move {
                    m.set(m.get() + 1);
                    Ok(m.get())
                }
            },
        );
        let _: Option<()> = ctx.wait_for("stop").await?;
        Ok(n.get())
    });
    engine.register("caller", |ctx: Ctx, n: u64| async move {
        let mut last = 0u64;
        for _ in 0..n {
            last = ctx.object("counter", "from-code").call("add", &()).await?;
        }
        Ok(last)
    });
    let records = async || -> Result<u64, Error> { Ok(engine.metrics().await?.records) };

    // Warm both paths.
    let k = counter.key("warm");
    for _ in 0..200 {
        let _: u64 = k.call("add", &()).await?;
    }
    let h = engine.run::<u64>("holder-1");
    engine
        .start_raw(
            "holder",
            "holder-1",
            std::sync::Arc::from(&b"null"[..]),
            Default::default(),
        )
        .await?;
    for _ in 0..200 {
        let _: u64 = h.update("add", &()).await?;
    }

    let k = counter.key("k");
    let before = records().await?;
    let mut lat = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _: u64 = k.call("add", &()).await?;
        lat.push(t.elapsed().as_secs_f64() * 1e6);
    }
    line("object call (exclusive)", lat, records().await? - before, n);

    let before = records().await?;
    let mut lat = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _: u64 = h.update("add", &()).await?;
        lat.push(t.elapsed().as_secs_f64() * 1e6);
    }
    line("workflow update", lat, records().await? - before, n);

    // From workflow code: each call is a `Child` record and its result, at
    // the caller, plus the key's update and its result.
    let m = (n / 5).max(1);
    let before = records().await?;
    let t = Instant::now();
    engine
        .start_raw(
            "caller",
            "caller-1",
            std::sync::Arc::from(m.to_string().into_bytes()),
            Default::default(),
        )
        .await?;
    let _ = engine.run::<u64>("caller-1").result().await?;
    let per = t.elapsed().as_secs_f64() * 1e6 / m as f64;
    println!(
        "{:<34} mean {per:>6.1} µs                    records/call {:.2}",
        "call from workflow code",
        (records().await? - before) as f64 / m as f64
    );

    // Concurrency: 64 callers on one key take turns; on 64 keys they do not.
    for (what, keys) in [("64 callers, one key", 1usize), ("64 callers, 64 keys", 64)] {
        let before = records().await?;
        let t = Instant::now();
        let per_caller = (n / 64).max(1);
        let mut tasks = Vec::new();
        for c in 0..64 {
            let k = counter.key(&format!("c-{}", c % keys));
            tasks.push(tokio::spawn(async move {
                for _ in 0..per_caller {
                    let _: u64 = k.call("add", &()).await?;
                }
                Ok::<_, Error>(())
            }));
        }
        for t in tasks {
            t.await.map_err(|e| Error::failed(e.to_string()))??;
        }
        let total = per_caller * 64;
        let secs = t.elapsed().as_secs_f64();
        println!(
            "{what:<34} {:>9.0} calls/s                  records/call {:.2}",
            total as f64 / secs,
            (records().await? - before) as f64 / total as f64
        );
    }
    let m = engine.metrics().await?;
    println!("compactions {}", m.object_compactions);
    h.signal("stop", &()).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    engine.shutdown().await?;
    if where_ != "memory" {
        let _ = std::fs::remove_dir_all(&where_);
    }
    Ok(())
}
