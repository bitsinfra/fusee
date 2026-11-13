//! Durable streams end to end on the embedded engine: chunk order and
//! completeness, readers that reconnect from an offset, a restart in the
//! middle of a streaming step, replays that append nothing, retention and
//! parking (docs/features.md#streams).

use fusee::core::Record;
use fusee::{Config, Ctx, Engine, Error, RetryPolicy, StepOptions};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-streams-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &std::path::Path) -> Config {
    let mut c = Config::local(dir).shards(2);
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

/// Every chunk of `id`'s stream `name` from `from`, as strings, and how it
/// ended.
async fn read_all(engine: &Engine, id: &str, name: &str, from: u64) -> (Vec<String>, String) {
    let mut r = engine.run::<serde_json::Value>(id).stream(name).from(from);
    let mut out = Vec::new();
    while let Some(c) = r.next().await.unwrap() {
        assert_eq!(c.offset, from + out.len() as u64, "chunks come in order");
        out.push(c.json::<String>().unwrap());
    }
    (out, r.end().unwrap().status.clone())
}

fn token(i: usize) -> String {
    format!("t{i} ")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunks_arrive_in_order_and_the_step_ends_the_stream() {
    let dir = tmp("order");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("llm", |ctx: Ctx, n: usize| async move {
        let c = ctx.clone();
        let text: String = ctx
            .step("generate", move || {
                let c = c.clone();
                async move {
                    let out = c.stream("tokens")?;
                    let mut text = String::new();
                    for i in 0..n {
                        out.append(&token(i))?;
                        text.push_str(&token(i));
                        if i % 50 == 0 {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    }
                    Ok::<_, Error>(text)
                }
            })
            .await?;
        Ok(text)
    });
    let run = engine.start(&wf, "q-1", &500).await.unwrap();
    // A reader that starts with the run sees every chunk once, in order.
    let (chunks, status) = read_all(&engine, "q-1", "tokens", 0).await;
    assert_eq!(chunks.len(), 500);
    assert_eq!(status, "ok");
    let text = run.result().await.unwrap();
    assert_eq!(chunks.concat(), text);
    // A reader that reconnects at an offset continues exactly there.
    let (tail, _) = read_all(&engine, "q-1", "tokens", 437).await;
    assert_eq!(tail, chunks[437..].to_vec());
    // Past the end: nothing, and the end.
    let (none, status) = read_all(&engine, "q-1", "tokens", 900).await;
    assert!(none.is_empty());
    assert_eq!(status, "ok");
    // The journal: chunks of the step's first attempt, then its end just
    // before its result.
    let j = run.describe().await.unwrap().journal;
    let ends: Vec<_> = j
        .iter()
        .filter(|r| matches!(r, Record::StreamEnd { .. }))
        .collect();
    assert_eq!(ends.len(), 1);
    assert!(
        matches!(
            ends[0],
            Record::StreamEnd {
                chunks: 500,
                seq: 0,
                ..
            }
        ),
        "{:?}",
        ends[0]
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bytes_and_json_chunks_and_a_long_poll() {
    let dir = tmp("kinds");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("events", |ctx: Ctx, _: ()| async move {
        let out = ctx.stream("events")?;
        out.append(&serde_json::json!({"stage": "start"}))?;
        out.append_bytes(&[0, 1, 2, 255])?;
        let _: Option<u32> = ctx.wait_for("go").await?;
        out.append(&"after")?;
        out.close()?;
        let _: Option<u32> = ctx.wait_for("finish").await?;
        Ok(())
    });
    let run = engine.start(&wf, "e-1", &()).await.unwrap();
    let mut r = run.stream("events").wait(Duration::from_millis(50));
    let a = r.next().await.unwrap().unwrap();
    assert_eq!(a.json::<serde_json::Value>().unwrap()["stage"], "start");
    let b = r.next().await.unwrap().unwrap();
    assert_eq!(b.bytes(), Some(&[0u8, 1, 2, 255][..]));
    // Nothing yet: the read waits (asking again every 50 ms).
    let reader = tokio::spawn(async move {
        let c = r.next().await.unwrap().unwrap();
        let end = r.next().await.unwrap();
        (c, end, r.end().cloned())
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!reader.is_finished());
    run.signal("go", &1).await.unwrap();
    let (c, end, how) = reader.await.unwrap();
    assert_eq!(c.json::<String>().unwrap(), "after");
    assert_eq!(c.offset, 2);
    assert!(end.is_none());
    let how = how.unwrap();
    assert_eq!((how.status.as_str(), how.closed), ("ok", true));
    run.signal("finish", &1).await.unwrap();
    run.result().await.unwrap();
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The workflow appends to its own stream between steps and sleeps: after
/// a restart its code replays from the top, and the replay appends nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replay_appends_nothing() {
    let dir = tmp("replay");
    let register = |engine: &Engine| {
        engine.register("progress", |ctx: Ctx, _: ()| async move {
            let p = ctx.stream("progress")?;
            p.append(&"fetched")?;
            let a: u32 = ctx.step("a", || async { Ok::<_, Error>(1) }).await?;
            p.append(&format!("a={a}"))?;
            let _: Option<u32> = ctx.wait_for("go").await?;
            p.append(&"resumed")?;
            Ok(a)
        })
    };
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = register(&engine);
        engine.start(&wf, "p-1", &()).await.unwrap();
        let mut r = engine.run::<u32>("p-1").stream("progress");
        assert_eq!(
            r.next().await.unwrap().unwrap().json::<String>().unwrap(),
            "fetched"
        );
        assert_eq!(
            r.next().await.unwrap().unwrap().json::<String>().unwrap(),
            "a=1"
        );
        engine.shutdown().await.unwrap();
    }
    for _ in 0..2 {
        // The run replays its journal at every open.
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let _wf = register(&engine);
        let run = engine.run::<u32>("p-1");
        let detail = run.describe().await.unwrap();
        let chunks = detail
            .journal
            .iter()
            .filter(|r| matches!(r, Record::Chunk { .. }))
            .count();
        assert_eq!(chunks, 2, "{:?}", detail.journal);
        engine.shutdown().await.unwrap();
    }
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let _wf = register(&engine);
        let run = engine.run::<u32>("p-1");
        // The code is past its appends (it is waiting): make sure it ran.
        tokio::time::sleep(Duration::from_millis(50)).await;
        run.signal("go", &1).await.unwrap();
        assert_eq!(run.result().await.unwrap(), 1);
        let (all, status) = read_all(&engine, "p-1", "progress", 0).await;
        assert_eq!(all, ["fetched", "a=1", "resumed"]);
        // The workflow's stream ends with the run.
        assert_eq!(status, "ok");
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The common case: the model call is the step, its output so far was
/// recorded, the process stops, the step runs again. It continues after the
/// recorded prefix, so a reader that resumes at its offset sees each token
/// once and the model is not asked again for what was recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_mid_stream_keeps_the_recorded_prefix() {
    let dir = tmp("crash");
    let resumed_at = Arc::new(Mutex::new(Vec::<usize>::new()));
    let generated = Arc::new(AtomicU32::new(0));
    let register = |engine: &Engine, at: Arc<Mutex<Vec<usize>>>, generated: Arc<AtomicU32>| {
        engine.register("llm", move |ctx: Ctx, n: usize| {
            let (at, generated) = (at.clone(), generated.clone());
            async move {
                let c = ctx.clone();
                ctx.step("generate", move || {
                    let (c, at, generated) = (c.clone(), at.clone(), generated.clone());
                    async move {
                        let out = c.stream("tokens")?;
                        let prefix: Vec<String> = out.recorded()?;
                        at.lock().unwrap().push(prefix.len());
                        let mut text = prefix.concat();
                        for i in prefix.len()..n {
                            generated.fetch_add(1, Ordering::SeqCst);
                            out.append(&token(i))?;
                            text.push_str(&token(i));
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                        Ok::<_, Error>(text)
                    }
                })
                .await
            }
        })
    };
    let n = 60usize;
    let mut seen = Vec::new();
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = register(&engine, resumed_at.clone(), generated.clone());
        engine.start(&wf, "c-1", &n).await.unwrap();
        let mut r = engine.run::<String>("c-1").stream("tokens");
        while seen.len() < 20 {
            seen.push(r.next().await.unwrap().unwrap().json::<String>().unwrap());
        }
        // The process stops in the middle of the step.
        engine.shutdown().await.unwrap();
    }
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let _wf = register(&engine, resumed_at.clone(), generated.clone());
        // The reader comes back at its offset.
        let (rest, status) = read_all(&engine, "c-1", "tokens", seen.len() as u64).await;
        assert_eq!(status, "ok");
        seen.extend(rest);
        let text = engine.run::<String>("c-1").result().await.unwrap();
        let expected: Vec<String> = (0..n).map(token).collect();
        assert_eq!(seen, expected, "no token is missing, repeated or changed");
        assert_eq!(text, expected.concat());
        let at = resumed_at.lock().unwrap().clone();
        assert_eq!(at.len(), 2, "{at:?}");
        assert_eq!(at[0], 0);
        assert!(
            at[1] >= 20,
            "the retry continued after the recorded prefix: {at:?}"
        );
        // Every token was generated once: none of the recorded ones again.
        assert_eq!(generated.load(Ordering::SeqCst) as usize, n);
        let (all, _) = read_all(&engine, "c-1", "tokens", 0).await;
        assert_eq!(all, expected);
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A step that fails and is retried in the same process continues after
/// what its failed attempt recorded; a step that fails for good ends its
/// stream with `failed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retries_continue_and_a_failed_step_ends_its_stream() {
    let dir = tmp("retry");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let wf = engine.register("flaky", move |ctx: Ctx, fail_always: bool| {
        let a = a.clone();
        async move {
            let c = ctx.clone();
            let opts = StepOptions::default().retry(RetryPolicy::exponential(
                3,
                Duration::from_millis(1),
                Duration::from_millis(2),
            ));
            let r: Result<String, Error> = ctx
                .step("generate", move || {
                    let (c, a) = (c.clone(), a.clone());
                    async move {
                        let out = c.stream("tokens")?;
                        let prefix: Vec<String> = out.recorded()?;
                        a.fetch_add(1, Ordering::SeqCst);
                        for i in prefix.len()..prefix.len() + 3 {
                            out.append(&token(i))?;
                        }
                        if fail_always || prefix.is_empty() {
                            return Err::<String, Error>(Error::Failed("rate limited".into()));
                        }
                        Ok(format!("{} chunks", prefix.len() + 3))
                    }
                })
                .options(opts)
                .await;
            Ok(r.unwrap_or_else(|e| e.to_string()))
        }
    });
    let run = engine.start(&wf, "f-1", &false).await.unwrap();
    assert_eq!(run.result().await.unwrap(), "6 chunks");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let (all, status) = read_all(&engine, "f-1", "tokens", 0).await;
    assert_eq!(all, (0..6).map(token).collect::<Vec<_>>());
    assert_eq!(status, "ok");
    let run = engine.start(&wf, "f-2", &true).await.unwrap();
    run.result().await.unwrap();
    let (all, status) = read_all(&engine, "f-2", "tokens", 0).await;
    assert_eq!(all.len(), 9);
    assert_eq!(status, "failed");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Streams of finished runs stay readable (their summary keeps them, without
/// `keep_history`) after a restart, and from runs parked out of memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_survive_parking_finishing_and_restarts() {
    let dir = tmp("park");
    let register = |engine: &Engine| {
        engine.register("w", |ctx: Ctx, _: ()| async move {
            let out = ctx.stream("log")?;
            for i in 0..3 {
                out.append(&token(i))?;
            }
            // Suspended: parked out of memory after `park_after`.
            let _: Option<u32> = ctx.wait_for("go").await?;
            out.append(&token(3))?;
            Ok(())
        })
    };
    let mut c = cfg(&dir);
    c.park_after = Duration::from_millis(1);
    {
        let engine = Engine::open(c.clone()).await.unwrap();
        let wf = register(&engine);
        let run = engine.start(&wf, "w-1", &()).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while run.status().await.ok().map(|s| s.status).as_deref() != Some("parked") {
            assert!(Instant::now() < deadline, "the run parks");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // A parked run's stream is read from the run store.
        let mut r = run.stream("log").wait(Duration::from_millis(20));
        for i in 0..3 {
            assert_eq!(
                r.next().await.unwrap().unwrap().json::<String>().unwrap(),
                token(i)
            );
        }
        // A reader waiting on a parked run gets the next chunk when the
        // run loads again and appends it.
        let waiting = tokio::spawn(async move { r.next().await.unwrap().map(|c| c.offset) });
        tokio::time::sleep(Duration::from_millis(60)).await;
        run.signal("go", &1).await.unwrap();
        assert_eq!(waiting.await.unwrap(), Some(3));
        run.result().await.unwrap();
        engine.shutdown().await.unwrap();
    }
    {
        let engine = Engine::open(c).await.unwrap();
        let _wf = register(&engine);
        let (all, status) = read_all(&engine, "w-1", "log", 0).await;
        assert_eq!(all, (0..4).map(token).collect::<Vec<_>>());
        assert_eq!(status, "ok");
        let (tail, _) = read_all(&engine, "w-1", "log", 2).await;
        assert_eq!(tail, [token(2), token(3)]);
        let err = engine
            .run::<()>("nope")
            .stream("log")
            .next()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err}");
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Misuse is refused: a step writing another step's stream, workflow code
/// appending to a closed stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owners_are_enforced() {
    let dir = tmp("owners");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("w", |ctx: Ctx, _: ()| async move {
        let c = ctx.clone();
        ctx.step("a", move || {
            let c = c.clone();
            async move {
                c.stream("s")?.append(&1)?;
                Ok::<_, Error>(())
            }
        })
        .await?;
        let c = ctx.clone();
        let second: Result<(), Error> = ctx
            .step("b", move || {
                let c = c.clone();
                async move {
                    c.stream("s")?.append(&2)?;
                    Ok::<_, Error>(())
                }
            })
            .options(StepOptions::default().retry(RetryPolicy::none()))
            .await;
        let w = ctx.stream("mine")?;
        w.append(&1)?;
        w.close()?;
        let again = w.append(&2);
        Ok(format!(
            "{} | {}",
            second.unwrap_err(),
            again
                .map(|_| "appended".to_string())
                .unwrap_or_else(|e| e.to_string())
        ))
    });
    let out: String = engine
        .start(&wf, "o-1", &())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert!(out.contains("is closed"), "{out}");
    let (s, _) = {
        let mut r = engine.run::<String>("o-1").stream("s");
        let mut v = Vec::new();
        while let Some(c) = r.next().await.unwrap() {
            v.push(c.json::<u32>().unwrap());
        }
        (v, r.end().cloned())
    };
    assert_eq!(s, [1]);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Append cost and reader latency (printed; `--nocapture` shows them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_rate_and_reader_latency() {
    let dir = tmp("rate");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let n = 200_000usize;
    let wf = engine.register("bulk", |ctx: Ctx, n: usize| async move {
        let c = ctx.clone();
        let us: u64 = ctx
            .step("generate", move || {
                let c = c.clone();
                async move {
                    let out = c.stream("tokens")?;
                    let t = Instant::now();
                    for i in 0..n {
                        out.append(&(i as u32))?;
                    }
                    Ok::<_, Error>(t.elapsed().as_micros() as u64)
                }
            })
            .await?;
        Ok(us)
    });
    let run = engine.start(&wf, "b-1", &n).await.unwrap();
    let us = run.result().await.unwrap();
    let mut r = run.stream("tokens");
    let t = Instant::now();
    let mut count = 0;
    while let Some(c) = r.next().await.unwrap() {
        assert_eq!(c.offset, count);
        count += 1;
    }
    assert_eq!(count as usize, n);
    eprintln!(
        "append: {n} chunks in {us} us = {:.0} chunks/s ({:.0} ns each); read back {:.0} chunks/s",
        n as f64 / (us as f64 / 1e6),
        us as f64 * 1e3 / n as f64,
        n as f64 / t.elapsed().as_secs_f64()
    );
    // Reader latency: from append to the reader holding the chunk.
    let sent = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let s = sent.clone();
    let wf = engine.register("paced", move |ctx: Ctx, n: usize| {
        let s = s.clone();
        async move {
            let c = ctx.clone();
            ctx.step("generate", move || {
                let (c, s) = (c.clone(), s.clone());
                async move {
                    let out = c.stream("tokens")?;
                    for i in 0..n {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        s.lock().unwrap().push(Instant::now());
                        out.append(&(i as u32))?;
                    }
                    Ok::<_, Error>(())
                }
            })
            .await
        }
    });
    let samples = 200usize;
    let run = engine.start(&wf, "l-1", &samples).await.unwrap();
    let mut r = run.stream("tokens");
    let mut lat = Vec::new();
    while let Some(c) = r.next().await.unwrap() {
        let at = Instant::now();
        let sent_at = sent.lock().unwrap()[c.offset as usize];
        lat.push(at.duration_since(sent_at).as_micros() as u64);
    }
    lat.sort_unstable();
    eprintln!(
        "reader latency (append to reader, local fsync): p50 {} us, p90 {} us, p99 {} us",
        lat[lat.len() / 2],
        lat[lat.len() * 9 / 10],
        lat[lat.len() * 99 / 100]
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
