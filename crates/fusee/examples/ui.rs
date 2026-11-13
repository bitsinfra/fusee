//! Serves the HTTP API and the web UI over a memory engine with a few
//! workflows: `cargo run -p fusee --example ui`, then open
//! http://127.0.0.1:7601 (`FUSEE_UI_ADDR` picks another address).
//!
//! `FUSEE_UI_CODEC=decode` encrypts the payloads with a demo key and serves
//! a codec endpoint on the next port, so the UI shows them decoded;
//! `FUSEE_UI_CODEC=sealed` encrypts them with no endpoint.
//! `FUSEE_PROFILE=dev FUSEE_HTTP=off` runs it under the dev profile, whose
//! step times make a run's step list a timeline (`shipment-1`).

use fusee::{Config, Ctx, Engine, Error};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let codec_mode = std::env::var("FUSEE_UI_CODEC").ok();
    let codec = codec_mode
        .as_ref()
        .map(|_| fusee::aes_gcm([("demo", [42u8; 32])]));
    let mut config = Config::memory().shards(2).keep_history(true);
    if let Some(c) = &codec {
        config = config.codec(c.clone());
    }
    let engine = Engine::open(config).await?;
    engine.limit(
        "stripe",
        fusee::LimitSpec {
            concurrent: 2,
            per_second: 0.0,
        },
    )?;
    engine.limit(
        "emails",
        fusee::LimitSpec {
            concurrent: 0,
            per_second: 5.0,
        },
    )?;
    let orders = engine.register("order", |ctx: Ctx, n: u32| async move {
        let total: u32 = ctx
            .step("price", move || async move { Ok::<_, Error>(n * 7) })
            .await?;
        if n % 4 == 3 {
            ctx.step("charge", || async move {
                Err::<u32, _>(
                    fusee::Failure::fatal("card declined")
                        .kind("CardDeclined")
                        .code("card_declined"),
                )
            })
            .await?;
        }
        let paid: Option<bool> = ctx
            .wait_for("paid")
            .timeout(Duration::from_secs(3600))
            .await?;
        Ok(if paid.unwrap_or(false) { total } else { 0 })
    });
    for i in 0..12u32 {
        engine.start(&orders, &format!("order-{i:03}"), &i).await?;
    }
    // Orders whose charge was declined (every fourth) have already failed.
    for i in [0u32, 1, 2, 4] {
        engine
            .run::<u32>(&format!("order-{i:03}"))
            .signal("paid", &true)
            .await?;
    }
    // A model-like step that streams its words slowly: the run's page
    // follows the stream as it grows.
    let answers = engine.register("answer", |ctx: Ctx, words: u32| async move {
        let progress = ctx.stream("progress")?;
        progress.append(&serde_json::json!({ "stage": "thinking" }))?;
        let c = ctx.clone();
        let text: String = ctx
            .step("generate", move || {
                let c = c.clone();
                async move {
                    let out = c.stream("tokens")?;
                    let mut text: String = out.recorded::<String>()?.concat();
                    for i in out.recorded_len() as u32..words {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        let w = format!("word{i} ");
                        out.append(&w)?;
                        text.push_str(&w);
                    }
                    Ok::<_, Error>(text)
                }
            })
            .await?;
        progress.append(&serde_json::json!({ "stage": "done", "chars": text.len() }))?;
        Ok(text)
    });
    engine.start(&answers, "answer-1", &400).await?;
    // Steps that take a while: under the dev profile (`FUSEE_PROFILE=dev`)
    // each records when it began and how long it took, and the run's step
    // list is also a timeline.
    let shipments = engine.register("shipment", |ctx: Ctx, _: ()| async move {
        for (name, ms) in [
            ("reserve", 120u64),
            ("pack", 450),
            ("label", 80),
            ("handoff", 300),
        ] {
            ctx.step(name, move || async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok::<_, Error>(ms)
            })
            .await?;
        }
        Ok(())
    });
    engine.start(&shipments, "shipment-1", &()).await?;
    // A step that ran out of retries and paused its run, waiting for a
    // resume (Needs attention), and a key of an object type to edit.
    let refunds = engine.register("refund", |ctx: Ctx, _: ()| async move {
        ctx.step("refund", || async move {
            Err::<(), _>(fusee::Failure::new("the payment provider is down"))
        })
        .retry(
            fusee::RetryPolicy::exponential(2, Duration::from_millis(1), Duration::from_millis(1))
                .on_exhausted(fusee::Exhausted::Pause),
        )
        .await
    });
    engine.start(&refunds, "refund-7", &()).await?;
    let carts = engine
        .object_type("cart")
        .exclusive(
            "add",
            |ctx: fusee::object::ObjectCtx, item: String| async move {
                let mut items: Vec<String> = ctx.state().get("items")?.unwrap_or_default();
                items.push(item);
                ctx.state().set("items", &items)?;
                Ok(items.len())
            },
        )
        .register()?;
    for item in ["apple", "pear"] {
        carts.key("user-7").call::<_, usize>("add", &item).await?;
    }
    let addr = std::env::var("FUSEE_UI_ADDR").unwrap_or_else(|_| "127.0.0.1:7601".into());
    // FUSEE_UI_TOKEN: require a token, to see the page ask for it.
    let mut opts = fusee::http::HttpOptions {
        token: std::env::var("FUSEE_UI_TOKEN").ok(),
        ..Default::default()
    };
    let mut _codec_server = None;
    if let (Some(c), Some("decode")) = (codec, codec_mode.as_deref()) {
        let (host, port) = addr.rsplit_once(':').unwrap_or(("127.0.0.1", "7601"));
        let port: u16 = port.parse::<u16>().unwrap_or(7601) + 1;
        let s = fusee::http::serve_codec(
            c,
            &format!("{host}:{port}"),
            fusee::http::CodecOptions {
                allow_origin: format!("http://{addr}"),
                ..Default::default()
            },
        )
        .await?;
        opts.codec_url = Some(s.url());
        _codec_server = Some(s);
    }
    let server = fusee::http::serve(engine, &addr, opts).await?;
    println!("fusee UI on {}", server.url());
    std::future::pending::<()>().await;
    Ok(())
}
