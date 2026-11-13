//! A worker that exists only while there is work, in Rust: a server that
//! invokes an HTTP endpoint for shards nobody holds, the endpoint (here in
//! the same process; a function on a serverless platform in production),
//! and a reminder that sleeps with no engine open.
//!
//! ```text
//! cargo run --release -p fusee-server --example invoked_worker
//! ```
//!
//! In a function, give the request's `fusee-signature` header and body to
//! [`Handler::handle`] and answer with its status and body; any HTTP
//! framework works. [`fusee_server::invoke::serve`] is a small server of
//! its own, with optional HTTPS and client certificates.

use fusee::{Ctx, Engine, Error};
use fusee_server::invoke::{Endpoint, Handler, Secret};
use fusee_server::{Client, Server, ServerConfig};
use std::time::Duration;

const SECRET: &[u8] = b"use 32 random bytes in production";

fn register(engine: &Engine) {
    engine.register(
        "reminder",
        |ctx: Ctx, (to, text): (String, String)| async move {
            ctx.sleep("wait", Duration::from_secs(2)).await?;
            let sent: String = ctx
                .step("send", move || {
                    let (to, text) = (to.clone(), text.clone());
                    async move { Ok::<_, Error>(format!("sent {text:?} to {to}")) }
                })
                .await?;
            Ok(sent)
        },
    );
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let dir = std::env::temp_dir().join(format!("fusee-invoked-worker-{}", std::process::id()));
    let server = Server::new(ServerConfig {
        dir: dir.clone(),
        shards: 2,
        local: Default::default(),
        security: Default::default(),
    })?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    tokio::spawn(fusee_server::server::serve_tcp(server.clone(), listener));

    // The endpoint: each invocation opens a fresh engine with `register`.
    let handler = Handler::new(&addr, SECRET, register)?;
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/fusee", http.local_addr()?);
    tokio::spawn(fusee_server::invoke::serve(http, handler, None));
    server.set_invokers(vec![
        Endpoint::new(&url, Some(Secret::new(SECRET)?)).budget(Duration::from_secs(30)),
    ])?;

    let a = addr.clone();
    let client = tokio::task::spawn_blocking(move || Client::connect(&a)).await??;
    let c = client.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, fusee::Error> {
        c.start(
            "reminder",
            "remind-ana-1",
            &serde_json::json!(["ana", "stand-up"]),
        )?;
        c.result("remind-ana-1")
    })
    .await?
    .map_err(std::io::Error::other)?;
    println!("{result}");
    if let Some(m) = server.invocation_metrics() {
        let e = &m.endpoints[0];
        println!(
            "{} invocations so far (one per shard at startup, then the start and the timer), mean shard open {:.2} ms",
            e.invocations,
            e.open_us_total as f64 / e.opens.max(1) as f64 / 1e3
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
    std::process::exit(0);
}
