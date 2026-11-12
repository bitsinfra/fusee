//! Conformance runner (Rust). See conformance/README.md.
use fusee::{
    Batch, ChildOptions, Config, Ctx, Debounce, Engine, Error, LimitSpec, ParentClose, RetryPolicy,
    StepOptions,
};
use serde_json::{Value, json};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn retry3() -> StepOptions {
    StepOptions::default().retry(RetryPolicy::exponential(
        3,
        Duration::from_millis(1),
        Duration::from_millis(1),
    ))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let engine = Engine::open(Config::memory().shards(2).keep_history(true)).await?;
    engine.register("seq", |ctx: Ctx, input: Value| async move {
        let n = input["n"].as_i64().unwrap_or(0);
        let a: i64 = ctx
            .step("a", || async move { Ok::<_, Error>(n + 1) })
            .await?;
        let b: i64 = ctx
            .step("b", || async move { Ok::<_, Error>(a * 2) })
            .await?;
        let c: Value = ctx
            .step(
                "c",
                || async move { Ok::<_, Error>(json!({"a": a, "b": b})) },
            )
            .await?;
        Ok(c)
    });
    engine.register("parallel", |ctx: Ctx, n: i64| async move {
        let (c1, c2) = (ctx.clone(), ctx.clone());
        let r = ctx
            .all(vec![
                Box::pin(async move { c1.step("x", || async move { Ok::<_, Error>(n) }).await })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<i64, Error>>>>,
                Box::pin(
                    async move { c2.step("y", || async move { Ok::<_, Error>(n * 10) }).await },
                ),
            ])
            .await;
        let s = r[0].clone()? + r[1].clone()?;
        let z: i64 = ctx.step("z", || async move { Ok::<_, Error>(s) }).await?;
        Ok(z)
    });
    let tries: Arc<Mutex<BTreeMap<String, u32>>> = Arc::default();
    let t = tries.clone();
    engine.register("retry", move |ctx: Ctx, _: Value| {
        let t = t.clone();
        async move {
            let id = ctx.workflow_id().to_owned();
            let v: String = ctx
                .step("flaky", || {
                    let t = t.clone();
                    let id = id.clone();
                    async move {
                        let mut m = t.lock().unwrap_or_else(|e| e.into_inner());
                        let n = m.entry(id).or_insert(0);
                        *n += 1;
                        if *n < 3 {
                            Err(format!("try {n}"))
                        } else {
                            Ok("ok".to_string())
                        }
                    }
                })
                .options(retry3())
                .await?;
            Ok(v)
        }
    });
    let t = tries.clone();
    engine.register("read_only", move |ctx: Ctx, _: Value| {
        let t = t.clone();
        async move {
            let id = ctx.workflow_id().to_owned();
            let v: String = ctx
                .step("fetch", || {
                    let t = t.clone();
                    let id = id.clone();
                    async move {
                        let mut m = t.lock().unwrap_or_else(|e| e.into_inner());
                        let n = m.entry(id).or_insert(0);
                        *n += 1;
                        if *n < 2 {
                            Err(format!("try {n}"))
                        } else {
                            Ok("data".to_string())
                        }
                    }
                })
                .options(retry3().read_only())
                .await?;
            let w: String = ctx
                .step("store", move || {
                    let v = v.clone();
                    async move { Ok::<_, Error>(format!("{v}!")) }
                })
                .await?;
            Ok(w)
        }
    });
    engine.register("signal", |ctx: Ctx, _: Value| async move {
        let v: String = ctx.wait_for("go").await?.unwrap_or_default();
        let r: String = ctx
            .step("use", || {
                let v = v.clone();
                async move { Ok::<_, Error>(format!("{v}!")) }
            })
            .await?;
        Ok(r)
    });
    engine.register("double", |ctx: Ctx, x: i64| async move {
        ctx.step("mul", || async move { Ok::<_, Error>(x * 2) })
            .await
    });
    engine.register("child", |ctx: Ctx, _: Value| async move {
        let id = format!("{}/c", ctx.workflow_id());
        ctx.child::<i64, i64>("double", &id, &21).await
    });
    engine.register("sleep", |ctx: Ctx, _: Value| async move {
        ctx.sleep("nap", Duration::from_millis(1)).await?;
        ctx.step("after", || async { Ok::<_, Error>("woke".to_string()) })
            .await
    });
    engine.register("values", |ctx: Ctx, _: Value| async move {
        ctx.now_ms()?;
        ctx.random()?;
        ctx.uuid()?;
        Ok("values".to_string())
    });
    engine.register("cancel", |ctx: Ctx, _: Value| async move {
        match ctx.wait_for::<Value>("never").await {
            Err(Error::Cancelled(reason)) => {
                let r2 = reason.clone();
                ctx.step("undo", || {
                    let r = r2.clone();
                    async move { Ok::<_, Error>(r) }
                })
                .await?;
                Err::<Value, _>(Error::Cancelled(format!("undone: {reason}")))
            }
            other => other.map(|v| v.unwrap_or(Value::Null)),
        }
    });
    engine.register("fail", |ctx: Ctx, _: Value| async move {
        let r: Result<i64, Error> = ctx
            .step("boom", || async { Err::<i64, _>("boom".to_string()) })
            .options(StepOptions::default().retry(RetryPolicy::none()))
            .await;
        match r {
            Err(Error::Step { .. }) => {
                ctx.step("recover", || async {
                    Ok::<_, Error>("recovered".to_string())
                })
                .await
            }
            Err(e) => Err(e),
            Ok(_) => Ok("unexpected".into()),
        }
    });
    engine.register("listener", |ctx: Ctx, _: Value| async move {
        Ok(ctx.wait_for::<String>("ping").await?.unwrap_or_default())
    });
    engine.register("send", |ctx: Ctx, target: String| async move {
        ctx.signal_workflow(&target, "ping", &"hello")?;
        Ok("sent".to_string())
    });
    engine.register("timeout_wait", |ctx: Ctx, _: Value| async move {
        let v: Option<Value> = ctx
            .wait_for("never")
            .timeout(Duration::from_millis(5))
            .await?;
        let s = if v.is_none() { "none" } else { "some" };
        ctx.step("after", || async move { Ok::<_, Error>(s.to_string()) })
            .await
    });

    let o = fusee::StartOptions::default();
    engine
        .start_raw("seq", "seq", Arc::from(&br#"{"n":3}"#[..]), o.clone())
        .await?;
    engine
        .start_raw("parallel", "parallel", Arc::from(&b"5"[..]), o.clone())
        .await?;
    engine
        .start_raw("retry", "retry", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .start_raw("read_only", "read_only", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .start_raw("signal", "signal", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .signal_raw("signal", "go", Arc::from(&br#""hi""#[..]))
        .await?;
    engine
        .start_raw("child", "child", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .start_raw("sleep", "sleep", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .start_raw("values", "values", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .start_raw("cancel", "cancel", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .start_raw("fail", "fail", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .start_raw("listener", "listener", Arc::from(&b"null"[..]), o.clone())
        .await?;
    engine
        .start_raw("send", "send", Arc::from(&br#""listener""#[..]), o.clone())
        .await?;
    engine
        .start_raw(
            "timeout_wait",
            "timeout_wait",
            Arc::from(&b"null"[..]),
            o.clone(),
        )
        .await?;
    // Let the cancel scenario reach its wait, then cancel it.
    for _ in 0..100 {
        if engine
            .status("cancel")
            .await?
            .is_some_and(|s| s.status == "suspended" || s.status == "parked")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    engine.run::<Value>("cancel").cancel("stop").await?;
    let ids = [
        "seq",
        "parallel",
        "retry",
        "read_only",
        "signal",
        "child",
        "child/c",
        "sleep",
        "values",
        "cancel",
        "fail",
        "listener",
        "send",
        "timeout_wait",
    ];
    let mut out = serde_json::Map::new();
    for id in ids {
        let _ = engine.result_raw(id).await;
        let d = engine
            .describe(id)
            .await?
            .ok_or_else(|| Error::NotFound(id.into()))?;
        out.insert(id.to_string(), fusee::journal::normalize(&d.journal));
    }
    features(&engine, &mut out).await?;
    streams(&engine, &mut out).await?;
    queues(&engine, &mut out).await?;
    pause(&engine, &mut out).await?;
    objects(&mut out).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&Value::Object(out)).unwrap_or_default()
    );
    engine.shutdown().await
}

async fn until(engine: &Engine, id: &str, states: &[&str]) -> Result<(), Error> {
    for _ in 0..2000 {
        if let Some(s) = engine.status(id).await?
            && states.contains(&s.status.as_str())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Err(Error::failed(format!("{id} never reached {states:?}")))
}

fn raw(v: Value) -> Arc<[u8]> {
    Arc::from(v.to_string().into_bytes())
}

/// The workflow features (docs/features.md).
async fn features(engine: &Engine, out: &mut serde_json::Map<String, Value>) -> Result<(), Error> {
    engine.register("updates", |ctx: Ctx, _: Value| async move {
        let total = Rc::new(Cell::new(0i64));
        let (t, c) = (total.clone(), ctx.clone());
        ctx.on_update(
            "add",
            |v: &i64| {
                if *v < 0 {
                    Err("negative".to_string())
                } else {
                    Ok(())
                }
            },
            move |v: i64| {
                let (t, c) = (t.clone(), c.clone());
                async move {
                    let d: i64 = c
                        .step("apply", || async move { Ok::<_, Error>(v * 2) })
                        .await?;
                    t.set(t.get() + d);
                    Ok(t.get())
                }
            },
        );
        let t2 = total.clone();
        ctx.on_query("total", move |_: Value| t2.get());
        let _: Option<Value> = ctx.wait_for("done").await?;
        Ok(total.get())
    });
    engine.register("update_start", |ctx: Ctx, start: i64| async move {
        let n = Rc::new(Cell::new(start));
        let n2 = n.clone();
        ctx.on_update(
            "inc",
            |_: &i64| Ok(()),
            move |by: i64| {
                let n = n2.clone();
                async move {
                    n.set(n.get() + by);
                    Ok(n.get())
                }
            },
        );
        let _: Option<Value> = ctx.wait_for("stop").await?;
        Ok(n.get())
    });
    engine.register("heartbeat", |ctx: Ctx, _: Value| async move {
        let c = ctx.clone();
        let v: i64 = ctx
            .step("work", || {
                let c = c.clone();
                async move {
                    c.heartbeat(&1)?;
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok::<_, Error>(c.progress::<i64>()?.unwrap_or(0))
                }
            })
            .await?;
        Ok(v)
    });
    let token: Arc<Mutex<Option<String>>> = Arc::default();
    let tk = token.clone();
    engine.register("await", move |ctx: Ctx, _: Value| {
        let tk = tk.clone();
        async move {
            let v: String = ctx
                .await_completion("approval", move |c: fusee::Completion| {
                    let t = c.token;
                    let tk = tk.clone();
                    async move {
                        *tk.lock().map_err(|_| Error::Closed)? = Some(t);
                        Ok::<_, Error>(())
                    }
                })
                .await?;
            Ok(v)
        }
    });
    engine.register("continued", |ctx: Ctx, left: i64| async move {
        let v: i64 = ctx
            .step("work", || async move { Ok::<_, Error>(left) })
            .await?;
        if left == 0 {
            return Ok(v);
        }
        ctx.continue_as_new(&(left - 1)).await
    });
    engine.register("patched", |ctx: Ctx, _: Value| async move {
        ctx.deprecate_patch("old")?;
        let on = ctx.patched("new")?;
        let label = if on { "new" } else { "old" };
        let v: String = ctx
            .step("after", || async move { Ok::<_, Error>(label.to_string()) })
            .await?;
        Ok(v)
    });
    engine.register("attributes", |ctx: Ctx, _: Value| async move {
        ctx.set_attributes(&json!({"customer": "c-1", "amount": 5}))?;
        ctx.set_attributes(&json!({"stage": "paid"}))?;
        Ok("attributes".to_string())
    });
    engine.register("kid", |ctx: Ctx, _: Value| async move {
        let v: Option<String> = ctx.wait_for("go").await?;
        Ok(v.unwrap_or_default())
    });
    engine.register("parent_close", |ctx: Ctx, _: Value| async move {
        ctx.start_child("kid", "parent_close/terminate", &Value::Null)
            .await?;
        ctx.start_child("kid", "parent_close/abandon", &Value::Null)
            .options(ChildOptions {
                close: ParentClose::Abandon,
                timeout: None,
            })
            .await?;
        let _: Option<Value> = ctx.wait_for("never").await?;
        Ok("unreachable".to_string())
    });
    engine.register("reset", |ctx: Ctx, _: Value| async move {
        let a: i64 = ctx.step("a", || async { Ok::<_, Error>(1) }).await?;
        let b: i64 = ctx
            .step("b", || async move { Ok::<_, Error>(a + 1) })
            .await?;
        let c: i64 = ctx
            .step("c", || async move { Ok::<_, Error>(b + 1) })
            .await?;
        Ok(c)
    });
    engine.register("victim", |ctx: Ctx, _: Value| async move {
        match ctx.wait_for::<Value>("never").await {
            Err(Error::Cancelled(r)) => {
                let _: String = ctx
                    .step("undo", || {
                        let r = r.clone();
                        async move { Ok::<_, Error>(r) }
                    })
                    .await?;
                Err(Error::Cancelled("undone".into()))
            }
            other => other.map(|_| "unexpected".to_string()),
        }
    });
    engine.register("canceller", |ctx: Ctx, _: Value| async move {
        ctx.cancel_workflow("victim", "stop it")?;
        Ok("sent".to_string())
    });
    engine.register("race_wait", |ctx: Ctx, _: Value| async move {
        type F<'a> = Pin<Box<dyn Future<Output = Result<String, Error>> + 'a>>;
        let _ = ctx
            .race(vec![
                Box::pin(async {
                    ctx.wait_for::<String>("x")
                        .await
                        .map(|v| v.unwrap_or_default())
                }) as F<'_>,
                Box::pin(async {
                    ctx.sleep("nap", Duration::from_millis(1))
                        .await
                        .map(|_| "slept".to_string())
                }),
            ])
            .await;
        let v: Option<String> = ctx.wait_for("x").await?;
        Ok(v.unwrap_or_default())
    });

    let o = fusee::StartOptions::default();
    let n = || Arc::from(&b"null"[..]);
    engine
        .start_raw("updates", "updates", n(), o.clone().use_existing())
        .await?;
    let run = engine.run::<i64>("updates");
    let a: i64 = run.update("add", &3).id("u1").await?;
    let b: i64 = run.update("add", &3).id("u1").await?;
    let rejected = run.update::<i64, i64>("add", &-1).await.is_err();
    let total: i64 = run.query("total", &Value::Null).await?;
    assert!(
        a == 6 && b == 6 && rejected && total == 6,
        "updates {a} {b} {rejected} {total}"
    );
    run.signal("done", &Value::Null).await?;
    let (_, v) = engine
        .update_with_start_raw(
            "update_start",
            "update_start",
            raw(json!(10)),
            o.clone(),
            "inc",
            "",
            raw(json!(5)),
        )
        .await?;
    assert_eq!(&*v, b"15");
    engine
        .run::<i64>("update_start")
        .signal("stop", &Value::Null)
        .await?;
    engine
        .start_raw("heartbeat", "heartbeat", n(), o.clone())
        .await?;
    engine.start_raw("await", "await", n(), o.clone()).await?;
    let t = loop {
        if let Some(t) = token.lock().map_err(|_| Error::Closed)?.clone() {
            break t;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    until(engine, "await", &["suspended", "parked"]).await?;
    engine.complete(&t, &"approved").await?;
    engine
        .start_raw("continued", "continued", raw(json!(2)), o.clone())
        .await?;
    engine
        .start_raw("patched", "patched", n(), o.clone())
        .await?;
    engine
        .start_raw("attributes", "attributes", n(), o.clone())
        .await?;
    engine
        .start_raw("parent_close", "parent_close", n(), o.clone())
        .await?;
    until(engine, "parent_close/terminate", &["suspended", "parked"]).await?;
    until(engine, "parent_close/abandon", &["suspended", "parked"]).await?;
    engine
        .run::<Value>("parent_close")
        .terminate("stop")
        .await?;
    until(engine, "parent_close/terminate", &["terminated"]).await?;
    engine
        .run::<Value>("parent_close/abandon")
        .signal("go", &"free")
        .await?;
    engine.start_raw("reset", "reset", n(), o.clone()).await?;
    let _ = engine.result_raw("reset").await?;
    engine.reset("reset", "b", true).await?;
    engine.start_raw("victim", "victim", n(), o.clone()).await?;
    until(engine, "victim", &["suspended", "parked"]).await?;
    engine
        .start_raw("canceller", "canceller", n(), o.clone())
        .await?;
    engine
        .start_raw("race_wait", "race_wait", n(), o.clone())
        .await?;
    for _ in 0..2000 {
        let d = engine.describe("race_wait").await?;
        if d.is_some_and(|d| d.journal.len() >= 6) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    engine
        .run::<Value>("race_wait")
        .signal("x", &"late")
        .await?;
    let ids = [
        "updates",
        "update_start",
        "heartbeat",
        "await",
        "continued",
        "patched",
        "attributes",
        "parent_close",
        "parent_close/terminate",
        "parent_close/abandon",
        "reset",
        "victim",
        "canceller",
        "race_wait",
    ];
    for id in ids {
        let _ = engine.result_raw(id).await;
        let d = engine
            .describe(id)
            .await?
            .ok_or_else(|| Error::NotFound(id.into()))?;
        out.insert(id.to_string(), fusee::journal::normalize(&d.journal));
    }
    Ok(())
}

/// A step whose retries run out pauses its run; a resume runs it again
/// with a fresh budget and it succeeds (`Attempt` with `paused`, `Resume`).
async fn pause(engine: &Engine, out: &mut serde_json::Map<String, Value>) -> Result<(), Error> {
    let calls = Arc::new(Mutex::new(0u32));
    let c = calls.clone();
    engine.register("pause", move |ctx: Ctx, _: Value| {
        let c = c.clone();
        async move {
            let v: String = ctx
                .step("flaky", || {
                    let c = c.clone();
                    async move {
                        let mut n = c.lock().unwrap_or_else(|e| e.into_inner());
                        *n += 1;
                        if *n < 3 {
                            Err(format!("try {n}"))
                        } else {
                            Ok("ok".to_string())
                        }
                    }
                })
                .retry(
                    fusee::RetryPolicy::exponential(
                        2,
                        Duration::from_millis(1),
                        Duration::from_millis(1),
                    )
                    .on_exhausted(fusee::Exhausted::Pause),
                )
                .await?;
            Ok(v)
        }
    });
    engine
        .start_raw(
            "pause",
            "pause",
            Arc::from(&b"null"[..]),
            fusee::StartOptions::default(),
        )
        .await?;
    until(engine, "pause", &["paused"]).await?;
    engine.run::<Value>("pause").resume().await?;
    let _ = engine.result_raw("pause").await;
    let d = engine
        .describe("pause")
        .await?
        .ok_or_else(|| Error::NotFound("pause".into()))?;
    out.insert("pause".into(), fusee::journal::normalize(&d.journal));
    Ok(())
}

/// Streams: a workflow's own stream (appended once, closed by the code), a
/// step's stream (JSON and bytes chunks, ended by the step's result), and a
/// step whose retry continues after what its failed attempt recorded. The
/// reader's view of the workflow's stream is compared too.
async fn streams(engine: &Engine, out: &mut serde_json::Map<String, Value>) -> Result<(), Error> {
    engine.register("streams", |ctx: Ctx, _: Value| async move {
        let p = ctx.stream("progress")?;
        p.append(&"start")?;
        let c = ctx.clone();
        let text: String = ctx
            .step("generate", move || {
                let c = c.clone();
                async move {
                    let out = c.stream("tokens")?;
                    for t in ["a", "b", "c"] {
                        out.append(&t)?;
                    }
                    out.append_bytes(&[1, 2])?;
                    Ok::<_, Error>("abc".to_string())
                }
            })
            .await?;
        p.append(&json!({ "text": text }))?;
        p.close()?;
        Ok(text)
    });
    engine.register("stream_retry", |ctx: Ctx, _: Value| async move {
        let c = ctx.clone();
        ctx.step("generate", move || {
            let c = c.clone();
            async move {
                let out = c.stream("tokens").map_err(|e| e.to_string())?;
                let prev: Vec<String> = out.recorded().map_err(|e| e.to_string())?;
                if prev.is_empty() {
                    out.append(&"x").map_err(|e| e.to_string())?;
                    out.append(&"y").map_err(|e| e.to_string())?;
                    return Err::<i64, String>("cut off".into());
                }
                out.append(&"z").map_err(|e| e.to_string())?;
                Ok(prev.len() as i64 + 1)
            }
        })
        .options(retry3())
        .await
    });
    let o = fusee::StartOptions::default();
    for id in ["streams", "stream_retry"] {
        engine
            .start_raw(id, id, Arc::from(&b"null"[..]), o.clone())
            .await?;
        let _ = engine.result_raw(id).await;
        journal(engine, id, id, out).await?;
    }
    let mut r = engine.run::<Value>("streams").stream("progress");
    let mut chunks = Vec::new();
    while let Some(c) = r.next().await? {
        chunks.push(c.json::<Value>()?);
    }
    let status = r.end().map(|e| e.status.clone()).unwrap_or_default();
    out.insert(
        "streams/read".into(),
        json!({ "chunks": chunks, "status": status }),
    );
    Ok(())
}

/// Keyed objects (`fusee::object`): exclusive calls with a step, a retried
/// call ID, a failed call that changes nothing, a shared read, a one-way
/// call, calls from a workflow (one awaited, one sent), clearing, and a key
/// compacted after 128 operations. On an engine that never parks, so a
/// compaction happens only where the scenario makes it.
async fn objects(out: &mut serde_json::Map<String, Value>) -> Result<(), Error> {
    let engine = Engine::open(
        Config::memory()
            .shards(2)
            .keep_history(true)
            .park_after(Duration::from_secs(3600)),
    )
    .await?;
    engine
        .object_type("conf_cart")
        .exclusive("add", |ctx: fusee::ObjectCtx, item: String| async move {
            let priced: String = ctx
                .step("price", move || {
                    let item = item.clone();
                    async move { Ok::<_, Error>(format!("{item}$")) }
                })
                .await?;
            let mut items: Vec<String> = ctx.state().get("items")?.unwrap_or_default();
            items.push(priced);
            ctx.state().set("items", &items)?;
            ctx.state().set("last", &items.last())?;
            Ok(items.len())
        })
        .exclusive("remove", |ctx: fusee::ObjectCtx, item: String| async move {
            let mut items: Vec<String> = ctx.state().get("items")?.unwrap_or_default();
            ctx.state().set("touched", &true)?;
            let Some(at) = items.iter().position(|i| *i == item) else {
                return Err(Error::failed(format!("missing {item}")));
            };
            items.remove(at);
            ctx.state().set("items", &items)?;
            Ok(items.len())
        })
        .shared("get", |ctx: fusee::ObjectCtx, _: Value| async move {
            Ok(json!({"items": ctx.state().get::<Value>("items")?, "keys": ctx.state().keys()?}))
        })
        .exclusive("clear", |ctx: fusee::ObjectCtx, _: Value| async move {
            ctx.state().clear_all()?;
            Ok(ctx.state().keys()?.len())
        })
        .register()?;
    engine
        .object_type("conf_tally")
        .exclusive("add", |ctx: fusee::ObjectCtx, n: i64| async move {
            let sum = ctx.state().get::<i64>("sum")?.unwrap_or(0) + n;
            ctx.state().set("sum", &sum)?;
            Ok(sum)
        })
        .register()?;
    let caller = engine.register("obj_caller", |ctx: Ctx, key: String| async move {
        let n: i64 = ctx.object("conf_cart", &key).call("add", &"d").await?;
        ctx.object("conf_cart", &key).send("add", &"e")?;
        Ok(n)
    });
    let k = engine.object("conf_cart", "k1");
    let mut calls = Vec::new();
    calls.push(k.call::<_, Value>("add", &"a").id("c1").await?);
    calls.push(k.call::<_, Value>("add", &"b").id("c2").await?);
    calls.push(k.call::<_, Value>("add", &"b").id("c2").await?);
    calls.push(match k.call::<_, Value>("remove", &"zz").id("c3").await {
        Ok(v) => v,
        Err(_) => json!({ "error": true }),
    });
    calls.push(k.call::<_, Value>("get", &Value::Null).id("c4").await?);
    k.send("add", &"c").id("s1").await?;
    let run = engine
        .start(&caller, "obj_caller", &"k1".to_owned())
        .await?;
    calls.push(json!(run.result().await?));
    for _ in 0..2000 {
        if k.state().await?["items"]
            .as_array()
            .is_some_and(|i| i.len() == 5)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    calls.push(k.call::<_, Value>("get", &Value::Null).id("c5").await?);
    calls.push(k.call::<_, Value>("clear", &Value::Null).id("c6").await?);
    calls.push(k.call::<_, Value>("add", &"f").id("c7").await?);
    // An edit of the state from outside the handlers.
    let edit = json!({"set": {"owner": "ana"}, "patch": [{"op": "add", "path": "/items/-", "value": "g"}]});
    calls.push(
        k.call::<_, Value>(fusee::object::EDIT_HANDLER, &edit)
            .id("c8")
            .await?,
    );
    calls.push(k.state().await?);
    out.insert("obj/calls".into(), Value::Array(calls));
    journal(&engine, &k.id(), "obj/cart", out).await?;
    journal(&engine, "obj_caller", "obj/caller", out).await?;
    let t = engine.object("conf_tally", "t");
    let mut sums = Vec::new();
    for n in 1..=130i64 {
        sums.push(t.call::<_, i64>("add", &n).id(&format!("t-{n}")).await?);
    }
    // A retry from before the compaction gets its recorded result.
    sums.push(t.call::<_, i64>("add", &5).id("t-5").await?);
    out.insert(
        "obj/sums".into(),
        json!([sums[0], sums[127], sums[129], sums[130]]),
    );
    out.insert("obj/tally_state".into(), t.state().await?);
    journal(&engine, &t.id(), "obj/tally", out).await?;
    engine.shutdown().await
}

async fn journal(
    engine: &Engine,
    id: &str,
    name: &str,
    out: &mut serde_json::Map<String, Value>,
) -> Result<(), Error> {
    let d = engine
        .describe(id)
        .await?
        .ok_or_else(|| Error::NotFound(id.into()))?;
    out.insert(name.to_string(), fusee::journal::normalize(&d.journal));
    Ok(())
}

/// Queues: steps under a limit with priorities and fairness keys (the
/// order is not journaled), a debounced start and two batches. A run a
/// coordinator starts has a random ID (`<key>@<uuid>`), so its journal is
/// listed under a fixed name, next to its coordinator's `describe`.
async fn queues(engine: &Engine, out: &mut serde_json::Map<String, Value>) -> Result<(), Error> {
    engine.limit(
        "conf",
        LimitSpec {
            concurrent: 1,
            per_second: 0.0,
        },
    )?;
    engine.register("prio", |ctx: Ctx, p: Value| async move {
        let level = p["level"].as_i64().unwrap_or(0) as i32;
        let key = p["key"].as_str().unwrap_or_default().to_owned();
        let weight = p["weight"].as_u64().unwrap_or(0) as u32;
        let first = StepOptions::default()
            .limit("conf")
            .priority(level)
            .fairness(&key)
            .fairness_weight(weight);
        let a: i64 = ctx
            .step(
                "first",
                move || async move { Ok::<_, Error>(i64::from(level)) },
            )
            .options(first)
            .await?;
        let second = StepOptions::default()
            .limit("conf")
            .priority(-level)
            .fairness("other");
        ctx.step("second", move || {
            let key = key.clone();
            async move { Ok::<_, Error>(format!("{key}:{a}")) }
        })
        .options(second)
        .await
    });
    let deb = engine.register("deb_target", |ctx: Ctx, v: Value| async move {
        ctx.step("got", move || {
            let v = v.clone();
            async move { Ok::<_, Error>(v) }
        })
        .await
    });
    let bat = engine.register("batch_target", |ctx: Ctx, events: Vec<Value>| async move {
        ctx.step("got", move || {
            let e = events.clone();
            async move { Ok::<_, Error>(e) }
        })
        .await
    });

    let o = fusee::StartOptions::default();
    let mut prio = Vec::new();
    for i in 0..4i64 {
        let id = format!("prio/{i}");
        let key = if i % 2 == 0 { "even" } else { "odd" };
        let input = json!({"level": i, "key": key, "weight": i + 1});
        engine.start_raw("prio", &id, raw(input), o.clone()).await?;
        prio.push(id);
    }

    // Three starts sent together collapse into one run with the latest
    // input; a retried event ID joins once and its input is not used.
    let rule = Debounce::new(Duration::from_millis(500));
    let inputs = [json!(1), json!(2), json!(3)];
    let (a, b, c) = tokio::join!(
        engine.debounce(&deb, "k", &inputs[0], &rule).event_id("d1"),
        engine.debounce(&deb, "k", &inputs[1], &rule).event_id("d2"),
        engine.debounce(&deb, "k", &inputs[2], &rule).event_id("d3"),
    );
    let (a, b, c) = (a?, b?, c?);
    let again = engine
        .debounce(&deb, "k", &json!(99), &rule)
        .event_id("d2")
        .await?;
    assert!(
        [b.id(), c.id(), again.id()].iter().all(|id| *id == a.id()),
        "debounced starts joined different runs"
    );
    a.started().await?;
    let _ = a.result().await;
    journal(engine, a.id(), "debounce/run", out).await?;

    // A batch closed by its count (the repeated ID adds nothing), and one
    // closed by its wait.
    let full = Batch::new(3, Duration::from_secs(60));
    let mut runs = Vec::new();
    for (id, e) in [("b1", "a"), ("b2", "b"), ("b2", "x"), ("b3", "c")] {
        runs.push(
            engine
                .batch(&bat, "k", &json!(e), &full)
                .event_id(id)
                .await?,
        );
    }
    assert!(
        runs.iter().all(|r| r.id() == runs[0].id()),
        "batched events joined different runs"
    );
    runs[0].started().await?;
    let _ = runs[0].result().await;
    journal(engine, runs[0].id(), "batch/full", out).await?;
    let timed = engine
        .batch(
            &bat,
            "t",
            &json!("z"),
            &Batch::new(100, Duration::from_millis(20)),
        )
        .await?;
    timed.started().await?;
    let _ = timed.result().await;
    journal(engine, timed.id(), "batch/timed", out).await?;

    for (name, id) in [
        (
            "debounce/describe",
            fusee::queue::debounce_id("deb_target", "k"),
        ),
        (
            "batch/describe",
            fusee::queue::batch_id("batch_target", "k"),
        ),
    ] {
        let d: Value = engine
            .run::<Value>(&id)
            .query("describe", &Value::Null)
            .await?;
        out.insert(name.to_string(), d);
    }
    for id in prio {
        let _ = engine.result_raw(&id).await;
        journal(engine, &id, &id, out).await?;
    }
    Ok(())
}
