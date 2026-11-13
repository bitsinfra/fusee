wit_bindgen::generate!({ path: "../wit", world: "bench-activity" });
use std::time::{SystemTime, UNIX_EPOCH};
use wstd::http::{Body, Client, Method, Request};

struct Component;
export!(Component);

fn now() -> Result<u64, ()> {
    SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64).map_err(|_| ())
}

impl exports::fusee::bench::activity::Guest for Component {
    fn crash_effect(run: String, index: u32, value: u32) -> Result<u32, ()> {
        let generation = std::env::var("FUSEE_GENERATION").map_err(|_| ())?;
        if !run.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') || !matches!(generation.as_str(), "0" | "1") {
            return Err(());
        }
        wstd::runtime::block_on(async {
            let request = Request::builder().method(Method::GET)
                .uri(format!("http://127.0.0.1:29780/effect/{run}/{index}/{value}/{generation}"))
                .body(Body::empty()).map_err(|_| ())?;
            let response = Client::new().send(request).await.map_err(|_| ())?;
            if !response.status().is_success() { return Err(()); }
            let body = response.into_body().bytes_contents().await.map_err(|_| ())?;
            serde_json::from_slice::<serde_json::Value>(&body).map_err(|_| ())?
                .get("value").and_then(|v| v.as_u64()).and_then(|v| v.try_into().ok()).ok_or(())
        })
    }

    fn noop(value: u32) -> Result<(u32, u64, u64), ()> {
        let start = now()?;
        let next = value.checked_add(1).ok_or(())?;
        Ok((next, start, now()?))
    }

    fn service_increment(value: u32, delay_ms: u32) -> Result<(u32, u64, u64), ()> {
        if !matches!(delay_ms,1|50) { return Err(()); }
        let start=now()?;
        wstd::runtime::block_on(async {
            let request=Request::builder().method(Method::GET)
                .uri(format!("http://127.0.0.1:29780/service-{delay_ms}"))
                .body(Body::empty()).map_err(|_|())?;
            let response=Client::new().send(request).await.map_err(|_|())?;
            if !response.status().is_success() { return Err(()); }
            let result=response.into_body().bytes_contents().await.map_err(|_|())?;
            if &result[..]!=b"ok" { return Err(()); }
            Ok(())
        })?;
        Ok((value.checked_add(1).ok_or(())?,start,now()?))
    }
}
