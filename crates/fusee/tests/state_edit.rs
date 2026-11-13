//! Edits of a key's state from outside its handlers: set, clear, JSON
//! Patch, versions, waiting for a running handler, a restart, and the HTTP
//! routes.

use fusee::object::ObjectCtx;
use fusee::{Config, Engine, Error};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-edit-{name}-{}-{}",
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

/// `add` appends an item (waiting while `hold` is set); `items` reads them.
fn cart(engine: &Engine, hold: Arc<AtomicBool>) -> fusee::Object {
    engine
        .object_type("cart")
        .exclusive("add", move |ctx: ObjectCtx, item: String| {
            let hold = hold.clone();
            async move {
                while hold.load(Ordering::SeqCst) {
                    ctx.sleep("hold", Duration::from_millis(5)).await?;
                }
                let mut items: Vec<String> = ctx.state().get("items")?.unwrap_or_default();
                items.push(item);
                ctx.state().set("items", &items)?;
                Ok(items.len())
            }
        })
        .shared("items", |ctx: ObjectCtx, _: ()| async move {
            Ok(ctx.state().get::<Vec<String>>("items")?.unwrap_or_default())
        })
        .register()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edits_change_state_as_handlers_do_and_survive_a_restart() {
    let dir = tmp("basic");
    let hold = Arc::new(AtomicBool::new(false));
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let carts = cart(&engine, hold.clone());
        let c = carts.key("u-1");
        assert_eq!(c.call::<_, usize>("add", &"apple").await.unwrap(), 1);
        let a = c.set_state("owner", &"ana").await.unwrap();
        assert_eq!(a["changed"], json!(["owner"]));
        c.patch_state(&json!([
            {"op": "add", "path": "/items/-", "value": "pear"},
            {"op": "test", "path": "/owner", "value": "ana"}
        ]))
        .await
        .unwrap();
        assert_eq!(
            c.state().await.unwrap(),
            json!({"items": ["apple", "pear"], "owner": "ana"})
        );
        // Handlers see the edit.
        assert_eq!(c.call::<_, usize>("add", &"fig").await.unwrap(), 3);
        // A patch that does not apply changes nothing.
        let e = c
            .patch_state(&json!([{"op": "remove", "path": "/nope"}]))
            .await
            .unwrap_err();
        assert!(e.to_string().contains("JSON Patch operation 0"), "{e}");
        // Versions: an edit made against another version is refused.
        let v = c.describe().await.unwrap().object.unwrap().version;
        let ok = c
            .edit_state(&json!({"ifVersion": v, "clear": ["owner"]}))
            .await
            .unwrap();
        let stale = c
            .edit_state(&json!({"ifVersion": v, "clearAll": true}))
            .await
            .unwrap_err();
        assert!(
            stale.to_string().contains("changed since version"),
            "{stale}"
        );
        assert_ne!(ok["version"], json!(v));
        assert_eq!(
            c.describe().await.unwrap().object.unwrap().version,
            ok["version"]
        );
        engine.shutdown().await.unwrap();
    }
    // After a restart the handlers replay and the edits apply again where
    // the journal has them.
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let carts = cart(&engine, hold.clone());
    let c = carts.key("u-1");
    assert_eq!(
        c.state().await.unwrap(),
        json!({"items": ["apple", "pear", "fig"]})
    );
    assert_eq!(c.call::<_, usize>("add", &"kiwi").await.unwrap(), 4);
    c.clear_state(None).await.unwrap();
    assert_eq!(c.state().await.unwrap(), json!({}));
    assert_eq!(
        c.call::<_, Vec<String>>("items", &()).await.unwrap(),
        Vec::<String>::new()
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_edit_waits_for_the_running_handler() {
    let dir = tmp("wait");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let hold = Arc::new(AtomicBool::new(true));
    let carts = cart(&engine, hold.clone());
    let c = carts.key("u-2");
    let e2 = engine.clone();
    let add = tokio::spawn(async move {
        e2.object("cart", "u-2")
            .call::<_, usize>("add", &"apple")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let e3 = engine.clone();
    let edit = tokio::spawn(async move {
        e3.object("cart", "u-2")
            .patch_state(&json!([{"op": "add", "path": "/items/-", "value": "edited"}]))
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!edit.is_finished(), "the edit waits for the handler");
    hold.store(false, Ordering::SeqCst);
    assert_eq!(add.await.unwrap().unwrap(), 1);
    edit.await.unwrap().unwrap();
    assert_eq!(
        c.state().await.unwrap(),
        json!({"items": ["apple", "edited"]})
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

async fn http(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> (u16, Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nX-Fusee-Action: 1\r\n"
    );
    if !body.is_empty() {
        req.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(&body);
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8_lossy(&out);
    let status: u16 = text.split(' ').nth(1).unwrap().parse().unwrap();
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_http_api_edits_state() -> Result<(), Error> {
    let dir = tmp("http");
    let engine = Engine::open(cfg(&dir)).await?;
    let carts = cart(&engine, Arc::new(AtomicBool::new(false)));
    carts.key("u/3").call::<_, usize>("add", &"apple").await?;
    let server = fusee::http::serve(
        engine.clone(),
        "127.0.0.1:0",
        fusee::http::HttpOptions::default(),
    )
    .await
    .unwrap();
    let a = server.addr();
    let (st, v) = http(
        a,
        "PUT",
        "/v1/objects/cart/u/3/state/owner",
        &[],
        Some(json!("ana")),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    let version = v["version"].as_str().unwrap().to_owned();
    let (st, v) = http(
        a,
        "PATCH",
        "/v1/objects/cart/u/3/state",
        &[("If-Match", &format!("\"{version}\""))],
        Some(json!([{"op": "add", "path": "/items/-", "value": "pear"}])),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    let (st, v) = http(
        a,
        "DELETE",
        "/v1/objects/cart/u/3/state/owner",
        &[("If-Match", &version)],
        None,
    )
    .await;
    assert_eq!(st, 412, "a stale version: {v}");
    let (st, v) = http(a, "GET", "/v1/objects/cart/u/3", &[], None).await;
    assert_eq!(st, 200);
    assert_eq!(
        v["state"],
        json!({"items": ["apple", "pear"], "owner": "ana"})
    );
    assert_eq!(
        v["objectDetail"]["version"].as_str().map(str::len),
        Some(16)
    );
    // Who asked is in the edit the journal keeps.
    let edits: Vec<&Value> = v["journal"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["kind"] == "update" && r["name"] == "fusee.state.edit")
        .collect();
    assert_eq!(edits.len(), 3, "{v}");
    assert!(
        edits.iter().all(|r| r["payload"]["by"].is_string()),
        "{edits:?}"
    );
    let (st, _) = http(a, "DELETE", "/v1/objects/cart/u/3/state", &[], None).await;
    assert_eq!(st, 200);
    assert_eq!(carts.key("u/3").state().await?, json!({}));
    server.stop();
    engine.shutdown().await?;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
