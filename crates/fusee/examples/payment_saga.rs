//! A payment saga: reserve stock, charge, ship; compensate on failure or
//! cancellation. Every external call gets `ctx.key()` as its idempotency key.
use fusee::{Config, Ctx, Engine, Error, RetryPolicy, StepOptions};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone)]
struct Order {
    id: String,
    sku: String,
    cents: u64,
}

/// A short ID made from a step's idempotency key (as a service would): FNV-1a.
fn short(key: &str) -> String {
    let h = key.bytes().fold(0x811c_9dc5_u32, |h, b| {
        (h ^ u32::from(b)).wrapping_mul(0x0100_0193)
    });
    format!("{h:08x}")
}

// Stand-ins for real services. Each accepts an idempotency key.
async fn reserve(sku: &str, key: &str) -> Result<String, String> {
    Ok(format!("res-{sku}-{}", short(key)))
}
async fn charge(cents: u64, key: &str) -> Result<String, String> {
    if cents > 1_000_000 {
        return Err("card declined".into());
    }
    Ok(format!("ch-{}", short(key)))
}
async fn refund(charge: &str) -> Result<(), String> {
    println!("  refunded {charge}");
    Ok(())
}
async fn release(reservation: &str) -> Result<(), String> {
    println!("  released {reservation}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let engine = Engine::open(Config::local("./data/payment-saga")).await?;
    let saga = engine.register("checkout", |ctx: Ctx, order: Order| async move {
        let reservation: String = ctx
            .step("reserve", || {
                let (sku, key) = (order.sku.clone(), ctx.key());
                async move { reserve(&sku, &key).await }
            })
            .await?;
        let retry = StepOptions::default().retry(RetryPolicy::exponential(
            5,
            Duration::from_millis(200),
            Duration::from_secs(60),
        ));
        let charged: Result<String, Error> = ctx
            .step("charge", || {
                let key = ctx.key();
                async move { charge(order.cents, &key).await }
            })
            .options(retry)
            .await;
        let charge_id = match charged {
            Ok(c) => c,
            Err(e) => {
                ctx.step("release", || {
                    let r = reservation.clone();
                    async move { release(&r).await }
                })
                .await?;
                return Err(e);
            }
        };
        // Wait for the warehouse to confirm, at most a day.
        match ctx
            .wait_for::<bool>("shipped")
            .timeout(Duration::from_secs(86_400))
            .await
        {
            Ok(Some(true)) => Ok(charge_id),
            Ok(_) | Err(Error::Cancelled(_)) => {
                ctx.step("refund", || {
                    let c = charge_id.clone();
                    async move { refund(&c).await }
                })
                .await?;
                ctx.step("release", || {
                    let r = reservation.clone();
                    async move { release(&r).await }
                })
                .await?;
                Err(Error::Cancelled("not shipped; refunded".into()))
            }
            Err(e) => Err(e),
        }
    });
    let order = Order {
        id: "order-123".into(),
        sku: "SKU-9".into(),
        cents: 4_200,
    };
    let run = engine.start(&saga, &order.id, &order).await?;
    run.signal("shipped", &true).await?;
    println!("charged: {}", run.result().await?);
    engine.shutdown().await
}
