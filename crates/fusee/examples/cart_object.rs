//! A cart object: one object per user, with its own state; calls to a key
//! take turns, and a call sent again with the same ID runs once.
use fusee::{Config, Engine, Error, ObjectCtx};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct Item {
    sku: String,
    cents: u64,
}

fn items(ctx: &ObjectCtx) -> fusee::Result<Vec<Item>> {
    Ok(ctx.state().get::<Vec<Item>>("items")?.unwrap_or_default())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let engine = Engine::open(Config::local("./data/cart-object")).await?;
    engine
        .object_type("cart")
        .exclusive("add", |ctx: ObjectCtx, item: Item| async move {
            let mut all = items(&ctx)?;
            all.push(item);
            ctx.state().set("items", &all)?;
            Ok(all.len())
        })
        .shared("total", |ctx: ObjectCtx, _: ()| async move {
            Ok(items(&ctx)?.iter().map(|i| i.cents).sum::<u64>())
        })
        .exclusive("checkout", |ctx: ObjectCtx, _: ()| async move {
            let all = items(&ctx)?;
            let (cents, n) = (all.iter().map(|i| i.cents).sum::<u64>(), all.len());
            let receipt: String = ctx
                .step("charge", move || async move {
                    Ok::<_, Error>(format!("charged {cents} cents for {n} items"))
                })
                .await?;
            ctx.state().clear("items")?;
            Ok(receipt)
        })
        .register()?;

    let user = engine.object("cart", "user-7");
    let tea = Item {
        sku: "tea".into(),
        cents: 450,
    };
    let milk = Item {
        sku: "milk".into(),
        cents: 120,
    };
    let _: usize = user.call("add", &tea).id("r1").await?;
    let n: usize = user.call("add", &tea).id("r1").await?; // sent again: once
    println!("items: {n}");
    let n: usize = user.call("add", &milk).id("r2").await?;
    println!("items: {n}");
    let total: u64 = user.call("total", &()).await?;
    println!("total: {total}");
    let receipt: String = user.call("checkout", &()).id("c1").await?;
    println!("receipt: {receipt}");
    engine.shutdown().await
}
