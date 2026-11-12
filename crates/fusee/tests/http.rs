//! The HTTP API (`fusee::http`) against a Rust engine, over raw HTTP.

use fusee::http::{HttpClient, HttpOptions};
use fusee::{Config, Ctx, Engine, Error, Failure, RetryPolicy, StepOptions};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn request(addr: std::net::SocketAddr, raw: String) -> (u16, String, Vec<u8>) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(raw.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, head, buf[head_end + 4..].to_vec())
}

async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, Value) {
    let (s, _, b) = request(addr, format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n")).await;
    (s, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

async fn post(addr: std::net::SocketAddr, path: &str, body: &Value) -> (u16, Value) {
    let b = body.to_string();
    let (s, _, out) = request(
        addr,
        format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{b}",
            b.len()
        ),
    )
    .await;
    (s, serde_json::from_slice(&out).unwrap_or(Value::Null))
}

fn fast(n: u32) -> StepOptions {
    StepOptions::default().retry(RetryPolicy::exponential(
        n,
        Duration::from_millis(1),
        Duration::from_millis(1),
    ))
}

async fn engine() -> Engine {
    let engine = Engine::open(Config::memory().shards(2).keep_history(true))
        .await
        .unwrap();
    engine.register("greet", |ctx: Ctx, name: String| async move {
        let s: String = ctx
            .step("hello", move || {
                let name = name.clone();
                async move { Ok::<_, Error>(format!("hello {name}")) }
            })
            .await?;
        let total = std::rc::Rc::new(std::cell::Cell::new(0u32));
        let t = total.clone();
        ctx.on_query("total", move |_: Value| t.get());
        let t = total.clone();
        ctx.on_update(
            "add",
            |n: &u32| {
                if *n > 100 {
                    Err("too big".into())
                } else {
                    Ok(())
                }
            },
            move |n: u32| {
                let t = t.clone();
                async move {
                    t.set(t.get() + n);
                    Ok(t.get())
                }
            },
        );
        let go: Option<u32> = ctx.wait_for("go").timeout(Duration::from_secs(60)).await?;
        Ok(format!("{s}/{}", go.unwrap_or(0) + total.get()))
    });
    engine.register("pay", |ctx: Ctx, fail: bool| async move {
        ctx.step("quote", || async { Ok::<_, Error>(1) }).await?;
        ctx.step("charge", || async move {
            if fail {
                Err(Failure::fatal("card declined")
                    .kind("CardDeclined")
                    .code("card_declined"))
            } else {
                Ok(2)
            }
        })
        .options(fast(1))
        .await
    });
    engine.register("talk", |ctx: Ctx, n: u32| async move {
        let w = ctx.stream("tokens")?;
        for i in 0..n {
            w.append(&format!("t{i}"))?;
        }
        w.close()?;
        Ok(n)
    });
    engine
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_api_starts_signals_queries_and_reads_runs() {
    let engine = engine().await;
    let server = fusee::http::serve(engine.clone(), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = server.addr();

    let (s, v) = post(
        addr,
        "/v1/runs",
        &json!({"type": "greet", "id": "g/1 x", "input": "ana"}),
    )
    .await;
    assert_eq!((s, &v), (200, &json!({"id": "g/1 x"})));
    let (s, v) = post(
        addr,
        "/v1/runs",
        &json!({"type": "greet", "id": "g/1 x", "input": "ana"}),
    )
    .await;
    assert_eq!(s, 409, "{v}");
    assert_eq!(v["error"]["code"], "AlreadyExists");
    assert!(
        v["error"]["docs"]
            .as_str()
            .unwrap()
            .ends_with("#alreadyexists")
    );
    let (s, v) = post(addr, "/v1/runs", &json!({"type": "nope", "id": "n-1"})).await;
    assert_eq!(
        (s, v["error"]["code"].as_str()),
        (400, Some("UnknownWorkflow")),
        "{v}"
    );

    // The query waits until the run's code is up.
    let mut total = Value::Null;
    for _ in 0..400 {
        let (s, v) = post(addr, "/v1/runs/g%2F1%20x/queries/total", &Value::Null).await;
        if s == 200 {
            total = v;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(total, json!(0));
    let (s, v) = post(addr, "/v1/runs/g%2F1%20x/updates/add?id=u-1", &json!(5)).await;
    assert_eq!((s, &v), (200, &json!(5)));
    let (s, v) = post(addr, "/v1/runs/g%2F1%20x/updates/add?id=u-1", &json!(5)).await;
    assert_eq!(
        (s, &v),
        (200, &json!(5)),
        "a retried update ID gets the recorded result"
    );
    let (s, v) = post(addr, "/v1/runs/g%2F1%20x/updates/add", &json!(500)).await;
    assert_eq!(
        (s, v["error"]["code"].as_str()),
        (400, Some("Rejected")),
        "{v}"
    );
    let (s, _) = post(addr, "/v1/runs/g%2F1%20x/signals/go", &json!(1)).await;
    assert_eq!(s, 200);
    let (s, v) = get(addr, "/v1/runs/g%2F1%20x/result").await;
    assert_eq!(
        (s, &v),
        (200, &json!({"status": "ok", "value": "hello ana/6"}))
    );

    let (s, v) = get(addr, "/v1/runs/g%2F1%20x").await;
    assert_eq!(s, 200);
    assert_eq!(v["status"], "ok");
    let kinds: Vec<&str> = v["journal"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["kind"].as_str().unwrap())
        .collect();
    assert!(
        kinds.contains(&"step") && kinds.contains(&"update"),
        "{kinds:?}"
    );
    let (s, rows) = get(addr, "/v1/runs?type=greet").await;
    assert_eq!(s, 200);
    assert_eq!(rows.as_array().unwrap().len(), 1);
    let (_, rows) = get(addr, "/v1/runs?type=nope").await;
    assert_eq!(rows, json!([]));
    let (s, v) = get(addr, "/v1/runs/missing").await;
    assert_eq!((s, v["error"]["code"].as_str()), (404, Some("NotFound")));

    let (_, types) = get(addr, "/v1/types").await;
    let names: Vec<&str> = types
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["greet", "pay", "talk"]);
    let (_, info) = get(addr, "/v1/info").await;
    assert_eq!(
        (info["engine"].as_str(), info["sdk"].as_str()),
        (Some("fusee"), Some("rust"))
    );
    let (s, m) = get(addr, "/v1/metrics").await;
    assert_eq!(s, 200);
    assert!(m["total"]["steps_run"].as_u64().unwrap() >= 1, "{m}");
    let (s, _, text) = request(
        addr,
        format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\n\r\n"),
    )
    .await;
    assert_eq!(s, 200);
    let text = String::from_utf8_lossy(&text);
    assert!(text.contains("# TYPE fusee_steps_run counter"), "{text}");
    assert!(text.contains("# TYPE fusee_runs_stuck gauge"), "{text}");
    assert!(
        text.contains("fusee_runs_waiting_for_code{shard=\"0\"} 0"),
        "{text}"
    );
    assert_eq!(m["total"]["runs_stuck"], 0, "{m}");
    let (s, api) = get(addr, "/v1/openapi.json").await;
    assert_eq!((s, api["openapi"].as_str()), (200, Some("3.1.0")));
    let (s, _, export) = request(
        addr,
        format!("GET /v1/runs/g%2F1%20x/export HTTP/1.1\r\nHost: {addr}\r\n\r\n"),
    )
    .await;
    assert_eq!(s, 200);
    assert!(fusee::export::ExportedJournal::decode(&export).is_ok());
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failures_come_back_whole_and_a_failed_run_retries() {
    let engine = engine().await;
    let server = fusee::http::serve(engine.clone(), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = server.addr();
    post(
        addr,
        "/v1/runs",
        &json!({"type": "pay", "id": "p-1", "input": true}),
    )
    .await;
    let (s, v) = get(addr, "/v1/runs/p-1/result").await;
    assert_eq!((s, v["status"].as_str()), (200, Some("failed")), "{v}");
    assert_eq!(v["error"]["type"], "StepFailed");
    assert_eq!(v["error"]["cause"]["type"], "CardDeclined");
    assert_eq!(v["error"]["cause"]["code"], "card_declined");
    let (s, v) = post(addr, "/v1/runs/p-1/retry", &Value::Null).await;
    assert_eq!((s, &v), (200, &json!({"resetBefore": "charge"})));
    let (s, v) = post(addr, "/v1/runs/missing/retry", &Value::Null).await;
    assert_eq!(s, 404, "{v}");

    // An SDK-free client calls the same protocol.
    let client = HttpClient::new(&server.url(), None).unwrap();
    let err = client.call_method("result", json!({"id": "p-1"})).await;
    assert!(
        matches!(&err, Err(Error::Step { name, .. }) if name == "charge"),
        "{err:?}"
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_follow_as_events() {
    let engine = engine().await;
    let server = fusee::http::serve(engine.clone(), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = server.addr();
    post(
        addr,
        "/v1/runs",
        &json!({"type": "talk", "id": "s-1", "input": 3}),
    )
    .await;
    let (s, head, body) = request(
        addr,
        format!("GET /v1/runs/s-1/streams/tokens HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\n\r\n"),
    )
    .await;
    assert_eq!(s, 200);
    assert!(head.contains("text/event-stream"), "{head}");
    let text = String::from_utf8_lossy(&body);
    let chunks: Vec<&str> = text
        .lines()
        .filter(|l| l.starts_with("data: {\"offset\""))
        .collect();
    assert_eq!(chunks.len(), 3, "{text}");
    assert!(text.contains("event: end"), "{text}");
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changes_need_a_json_body_and_the_token() {
    let engine = engine().await;
    let open = fusee::http::serve(engine.clone(), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = open.addr();
    // A form post (what another site's page can send) is refused.
    let (s, _, _) = request(
        addr,
        format!("POST /v1/runs HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 0\r\n\r\n"),
    )
    .await;
    assert_eq!(s, 403);
    let (s, _, _) = request(
        addr,
        format!("GET /v1/runs HTTP/1.1\r\nHost: {addr}\r\nOrigin: http://evil.example\r\n\r\n"),
    )
    .await;
    assert_eq!(s, 403);

    let locked = fusee::http::serve(
        engine.clone(),
        "127.0.0.1:0",
        HttpOptions {
            token: Some("s3cret".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (s, _) = get(locked.addr(), "/v1/runs").await;
    assert_eq!(s, 401);
    let (s, _, _) = request(
        locked.addr(),
        format!(
            "GET /v1/runs HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer s3cret\r\n\r\n",
            locked.addr()
        ),
    )
    .await;
    assert_eq!(s, 200);
    // Off loopback, a token is required.
    let err = fusee::http::serve(engine.clone(), "0.0.0.0:0", HttpOptions::default())
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("without a token"), "{err}");
    engine.shutdown().await.unwrap();
}

/// A read-only token reads and is refused for changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_token_only_reads() {
    let engine = engine().await;
    let opts = fusee::http::HttpOptions {
        token: Some("full-test-token".into()),
        read_token: Some("read-test-token".into()),
        ..Default::default()
    };
    let server = fusee::http::serve(engine.clone(), "127.0.0.1:0", opts)
        .await
        .unwrap();
    let addr = server.addr();
    let get = |tok: &str| {
        format!("GET /v1/runs HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {tok}\r\n\r\n")
    };
    let post = |tok: &str| {
        let body = r#"{"type":"greet","id":"ro-1","input":"x"}"#;
        format!(
            "POST /v1/runs HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {tok}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    assert_eq!(request(addr, get("read-test-token")).await.0, 200);
    assert_eq!(request(addr, post("read-test-token")).await.0, 403);
    assert_eq!(request(addr, get("wrong")).await.0, 401);
    assert_eq!(request(addr, post("full-test-token")).await.0, 200);
    engine.shutdown().await.unwrap();
}

/// A namespace's token reaches the runs under its ID prefix only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_namespace_token_reaches_its_namespace_only() {
    let engine = engine().await;
    let opts = fusee::http::HttpOptions {
        token: Some("full-test-token".into()),
        namespaces: vec![("acme-test-token".into(), "acme".into())],
        ..Default::default()
    };
    let server = fusee::http::serve(engine.clone(), "127.0.0.1:0", opts)
        .await
        .unwrap();
    let addr = server.addr();
    let call = |method: &str, path: &str, tok: &str, body: Option<Value>| {
        let b = body.map(|b| b.to_string()).unwrap_or_default();
        let head =
            format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {tok}\r\n");
        let raw = if method == "GET" {
            format!("{head}\r\n")
        } else {
            format!(
                "{head}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{b}",
                b.len()
            )
        };
        async move {
            let (s, _, out) = request(addr, raw).await;
            (
                s,
                serde_json::from_slice::<Value>(&out).unwrap_or(Value::Null),
            )
        }
    };
    let start = |id: &str| Some(json!({"type": "greet", "id": id, "input": "x"}));
    // The full token starts a run outside the namespace.
    assert_eq!(
        call("POST", "/v1/runs", "full-test-token", start("other/1"))
            .await
            .0,
        200
    );
    // The namespace's token starts and reaches runs under acme/ only.
    assert_eq!(
        call("POST", "/v1/runs", "acme-test-token", start("acme/1"))
            .await
            .0,
        200
    );
    let (s, body) = call("POST", "/v1/runs", "acme-test-token", start("other/2")).await;
    assert_eq!(s, 403, "{body}");
    assert!(
        body["error"]["message"].as_str().unwrap().contains("acme/"),
        "{body}"
    );
    // The engine's own types name the runs they start in their input: a
    // delayed start under acme/ for another namespace's run is refused, so
    // it can neither start that run nor read its result.
    let delay = json!({"type": "fusee.delay", "id": "acme/read",
        "input": {"type": "greet", "id": "other/1", "input": "x", "atMs": 0}});
    let (s, body) = call("POST", "/v1/runs", "acme-test-token", Some(delay.clone())).await;
    assert_eq!(s, 403, "{body}");
    let (s, _) = call("POST", "/v1/call/start", "acme-test-token", Some(delay)).await;
    assert_eq!(s, 403);
    let (s, body) = call(
        "POST",
        "/v1/call/updateWithStart",
        "acme-test-token",
        Some(json!({
        "type": "fusee.schedule", "id": "acme/s", "input": null, "name": "x", "arg": null})),
    )
    .await;
    assert_eq!(s, 403, "{body}");
    // A refused read of a result is a refusal, not a failed run.
    let (s, body) = call("GET", "/v1/runs/other%2F1/result", "acme-test-token", None).await;
    assert_eq!(s, 403, "{body}");
    assert_eq!(body["error"]["code"], "Rejected", "{body}");
    assert_eq!(
        call("GET", "/v1/runs/acme%2F1", "acme-test-token", None)
            .await
            .0,
        200
    );
    assert_eq!(
        call("GET", "/v1/runs/other%2F1", "acme-test-token", None)
            .await
            .0,
        403
    );
    let signal = Some(json!(1));
    assert_eq!(
        call(
            "POST",
            "/v1/runs/other%2F1/signals/go",
            "acme-test-token",
            signal.clone()
        )
        .await
        .0,
        403
    );
    // The generic call route is held to the namespace too.
    let (s, _) = call(
        "POST",
        "/v1/call/cancel",
        "acme-test-token",
        Some(json!({"id": "other/1", "reason": "x"})),
    )
    .await;
    assert_eq!(s, 403);
    // A list shows the namespace's runs only, with or without a filter.
    let ids = |v: &Value| -> Vec<String> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap().to_owned())
            .collect()
    };
    let (s, all) = call("GET", "/v1/runs", "full-test-token", None).await;
    assert_eq!(s, 200);
    assert_eq!(ids(&all).len(), 2);
    let (_, mine) = call("GET", "/v1/runs", "acme-test-token", None).await;
    assert_eq!(ids(&mine), ["acme/1"]);
    let (_, mine) = call("GET", "/v1/runs?id=other%2F1", "acme-test-token", None).await;
    assert!(ids(&mine).is_empty(), "{mine}");
    let (_, mine) = call(
        "POST",
        "/v1/call/list",
        "acme-test-token",
        Some(json!({"where": {"type": "greet"}})),
    )
    .await;
    assert_eq!(ids(&mine), ["acme/1"]);
    // The engine as a whole: info and types answer, the rest needs the
    // full token.
    let (s, info) = call("GET", "/v1/info", "acme-test-token", None).await;
    assert_eq!((s, info["namespace"].as_str()), (200, Some("acme")));
    assert_eq!(
        call("GET", "/v1/types", "acme-test-token", None).await.0,
        200
    );
    assert_eq!(
        call("GET", "/v1/metrics", "acme-test-token", None).await.0,
        403
    );
    // Schedules live outside every namespace: the list is empty.
    let (s, schedules) = call("GET", "/v1/schedules", "acme-test-token", None).await;
    assert_eq!((s, schedules), (200, json!([])));
    assert_eq!(
        call(
            "POST",
            "/v1/schedules/nightly/pause",
            "acme-test-token",
            Some(json!({}))
        )
        .await
        .0,
        403
    );
    assert_eq!(
        call("GET", "/v1/metrics", "full-test-token", None).await.0,
        200
    );
    // Signalling its own run works.
    assert_eq!(
        call(
            "POST",
            "/v1/runs/acme%2F1/signals/go",
            "acme-test-token",
            signal
        )
        .await
        .0,
        200
    );
    engine.shutdown().await.unwrap();
}

/// Encrypted payloads read as `{"$encoded": ...}`; the codec endpoint
/// decodes them for the UI's origin only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_payloads_decode_through_the_codec_endpoint() {
    let codec = fusee::aes_gcm([("k1", [7u8; 32])]);
    let engine = Engine::open(Config::memory().keep_history(true).codec(codec.clone()))
        .await
        .unwrap();
    engine.register("echo", |_ctx: Ctx, v: Value| async move { Ok(v) });
    let codec_server = fusee::http::serve_codec(
        codec,
        "127.0.0.1:0",
        fusee::http::CodecOptions {
            allow_origin: "http://ui.test:7601".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let api = fusee::http::serve(
        engine.clone(),
        "127.0.0.1:0",
        HttpOptions {
            codec_url: Some(format!("{}/", codec_server.url())),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let addr = api.addr();
    let (_, info) = get(addr, "/v1/info").await;
    assert_eq!(info["codecUrl"].as_str(), Some(codec_server.url().as_str()));
    let (_, head, _) = request(addr, format!("GET / HTTP/1.1\r\nHost: {addr}\r\n\r\n")).await;
    assert!(
        head.contains(&format!("connect-src 'self' {}", codec_server.url())),
        "{head}"
    );

    let input = json!({"card": "4242", "cents": 500});
    let (s, _) = post(
        addr,
        "/v1/runs",
        &json!({"type": "echo", "id": "e-1", "input": input}),
    )
    .await;
    assert_eq!(s, 200);
    let mut journal = Value::Null;
    for _ in 0..200 {
        let (_, d) = get(addr, "/v1/runs/e-1").await;
        if d["status"] == "ok" {
            journal = d["journal"].clone();
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let start = &journal[0];
    let sealed = start["input"]["$encoded"]
        .as_str()
        .expect("an encoded input")
        .to_owned();
    assert!(!start.to_string().contains("4242"), "{start}");

    let decode = |origin: &str| {
        // The second is marked as sealed but is not: it cannot decode.
        let b = json!({"payloads": [sealed, "RkVOMXh4eHg="]}).to_string();
        format!(
            "POST /decode HTTP/1.1\r\nHost: x\r\nOrigin: {origin}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{b}",
            b.len()
        )
    };
    let (s, head, body) = request(codec_server.addr(), decode("http://ui.test:7601")).await;
    assert_eq!(s, 200);
    assert!(
        head.contains("Access-Control-Allow-Origin: http://ui.test:7601"),
        "{head}"
    );
    let out: Value = serde_json::from_slice(&body).unwrap();
    let plain = fusee::http::calls::unbase64(out["payloads"][0].as_str().unwrap()).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&plain).unwrap(), input);
    assert!(out["payloads"][1].is_null(), "{out}");
    let (s, _, _) = request(codec_server.addr(), decode("http://evil.test")).await;
    assert_eq!(s, 403);
    engine.shutdown().await.unwrap();
}

/// Signals, cancellations, terminations and resets record who asked: the
/// proxy's identity header, else the token's kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changes_record_who_asked() {
    let engine = engine().await;
    let opts = fusee::http::HttpOptions {
        token: Some("full-test-token".into()),
        identity_header: Some("X-Forwarded-User".into()),
        ..Default::default()
    };
    let server = fusee::http::serve(engine.clone(), "127.0.0.1:0", opts)
        .await
        .unwrap();
    let addr = server.addr();
    let call = |path: &str, who: Option<&str>, body: Value| {
        let b = body.to_string();
        let auth = match who {
            Some(w) => format!("X-Forwarded-User: {w}\r\n"),
            None => "Authorization: Bearer full-test-token\r\n".into(),
        };
        let raw = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{b}",
            b.len()
        );
        async move {
            let (s, _, out) = request(addr, raw).await;
            (
                s,
                serde_json::from_slice::<Value>(&out).unwrap_or(Value::Null),
            )
        }
    };
    let journal = |id: &str| {
        let raw = format!(
            "GET /v1/runs/{id} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer full-test-token\r\n\r\n"
        );
        async move {
            let (_, _, out) = request(addr, raw).await;
            serde_json::from_slice::<Value>(&out).unwrap()["journal"].clone()
        }
    };
    let by = |j: &Value, kind: &str| -> Vec<Value> {
        j.as_array()
            .unwrap()
            .iter()
            .filter(|r| r["kind"] == kind)
            .map(|r| r["by"].clone())
            .collect()
    };
    let start = |id: &str| json!({"type": "greet", "id": id, "input": "x"});
    // Without the header or a token: refused.
    let (s, _, _) = request(
        addr,
        format!("GET /v1/runs HTTP/1.1\r\nHost: {addr}\r\n\r\n"),
    )
    .await;
    assert_eq!(s, 401);
    let (_, info) = {
        let raw = format!(
            "GET /v1/info HTTP/1.1\r\nHost: {addr}\r\nX-Forwarded-User: ana@example.com\r\n\r\n"
        );
        let (s, _, out) = request(addr, raw).await;
        (s, serde_json::from_slice::<Value>(&out).unwrap())
    };
    assert_eq!(info["viewer"], "ana@example.com");

    assert_eq!(
        call("/v1/runs", Some("ana@example.com"), start("a-1"))
            .await
            .0,
        200
    );
    // A signal through the proxy, then one with the token.
    assert_eq!(
        call(
            "/v1/runs/a-1/signals/other",
            Some("ana@example.com"),
            json!(1)
        )
        .await
        .0,
        200
    );
    assert_eq!(
        call("/v1/runs/a-1/signals/other", None, json!(2)).await.0,
        200
    );
    // A body's own `by` is replaced by who the API saw.
    assert_eq!(
        call(
            "/v1/call/signal",
            Some("bo"),
            json!({"id": "a-1", "name": "other", "value": 3, "by": "someone-else"})
        )
        .await
        .0,
        200
    );
    // Cancel: the cancellation is recorded once the run takes it.
    assert_eq!(
        call("/v1/runs/a-1/cancel", Some("cy"), json!({"reason": "test"}))
            .await
            .0,
        200
    );
    let mut j = Value::Null;
    for _ in 0..200 {
        j = journal("a-1").await;
        if !by(&j, "cancel").is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        by(&j, "signal"),
        [json!("ana@example.com"), json!("token"), json!("bo")]
    );
    assert_eq!(by(&j, "cancel"), [json!("cy")]);
    // A start records nobody; nor does a run's own end.
    assert!(j[0].get("by").is_none(), "{}", j[0]);

    // Terminate and reset.
    assert_eq!(call("/v1/runs", None, start("a-2")).await.0, 200);
    assert_eq!(
        call(
            "/v1/runs/a-2/terminate",
            Some("dee"),
            json!({"reason": "stop"})
        )
        .await
        .0,
        200
    );
    let j = journal("a-2").await;
    assert_eq!(by(&j, "done"), [json!("dee")]);
    assert_eq!(call("/v1/runs", None, start("a-3")).await.0, 200);
    for _ in 0..200 {
        if by(&journal("a-3").await, "step").len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (s, body) = call(
        "/v1/runs/a-3/reset",
        Some("eve"),
        json!({"beforeStep": "hello"}),
    )
    .await;
    assert_eq!(s, 200, "{body}");
    let j = journal("a-3").await;
    assert_eq!(by(&j, "start"), [json!("eve")], "{j}");

    // In code, `audit::as_requester` names the requester.
    fusee::audit::as_requester("job:nightly", async {
        engine.run::<String>("a-3").signal("other", &4u32).await
    })
    .await
    .unwrap();
    let j = journal("a-3").await;
    assert_eq!(by(&j, "signal").last(), Some(&json!("job:nightly")));
    engine.shutdown().await.unwrap();
}

/// An API on loopback without a token answers requests for this machine's
/// names only: a page that rebinds its own name to 127.0.0.1 is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tokenless_api_refuses_other_host_names() {
    let engine = engine().await;
    let server = fusee::http::serve(engine.clone(), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = server.addr();
    let port = addr.port();
    let with_host = |host: String| {
        let b = json!({"type": "greet", "id": format!("h-{host}"), "input": "x"}).to_string();
        format!(
            "POST /v1/runs HTTP/1.1\r\nHost: {host}\r\nOrigin: http://{host}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{b}",
            b.len()
        )
    };
    for host in [format!("evil.example:{port}"), "attacker.test".to_owned()] {
        let (s, _, body) = request(addr, with_host(host.clone())).await;
        assert_eq!(s, 403, "{host}: {}", String::from_utf8_lossy(&body));
    }
    for host in [
        format!("127.0.0.1:{port}"),
        format!("localhost:{port}"),
        format!("app.localhost:{port}"),
        format!("[::1]:{port}"),
    ] {
        let (s, _, body) = request(addr, with_host(host.clone())).await;
        assert_eq!(s, 200, "{host}: {}", String::from_utf8_lossy(&body));
    }
    server.stop();
    // With a token, a page on another site has nothing to send.
    let opts = HttpOptions {
        token: Some("host-test-token".into()),
        ..Default::default()
    };
    let server = fusee::http::serve(engine.clone(), "127.0.0.1:0", opts)
        .await
        .unwrap();
    let raw = "GET /v1/runs HTTP/1.1\r\nHost: proxy.example\r\nAuthorization: Bearer host-test-token\r\n\r\n"
        .to_string();
    assert_eq!(request(server.addr(), raw).await.0, 200);
    engine.shutdown().await.unwrap();
}
