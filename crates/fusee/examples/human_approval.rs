//! Human approval: a workflow that asks a person, waits up to three days for
//! the answer (durably: a crash meanwhile loses nothing), and acts on it.
use fusee::{Config, Ctx, Engine, Error, IdPolicy, OnFinished, OnRunning, Signal, WorkflowRef};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Claim {
    by: String,
    cents: u64,
}

#[derive(Serialize, Deserialize)]
struct Decision {
    approved: bool,
    by: String,
}

const EXPENSE: WorkflowRef<Claim, String> = WorkflowRef::new("expense");
const DECISION: Signal<Decision> = Signal::new("decision");

async fn expense(ctx: Ctx, claim: Claim) -> fusee::Result<String> {
    let (by, cents) = (claim.by.clone(), claim.cents);
    ctx.step("ask", move || {
        println!("  asked finance to approve {cents} cents for {by}");
        async { Ok::<_, Error>(()) }
    })
    .await?;
    let answer = ctx
        .wait_for(DECISION)
        .timeout(Duration::from_secs(3 * 86_400))
        .await?;
    let Some(answer) = answer else {
        return Ok("expired: nobody answered in three days".into());
    };
    if !answer.approved {
        return Ok(format!("rejected by {}", answer.by));
    }
    let paid: String = ctx
        .step("pay", move || async move {
            Ok::<_, Error>(format!("paid {cents} cents"))
        })
        .await?;
    Ok(format!("{paid} (approved by {})", answer.by))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let engine = Engine::open(Config::local("./data/human-approval")).await?;
    engine.register(EXPENSE, expense);
    let claim = Claim {
        by: "ana".into(),
        cents: 12_000,
    };
    let run = engine
        .start(EXPENSE, "expense-7", &claim)
        .id_policy(IdPolicy {
            running: OnRunning::UseExisting,
            finished: OnFinished::UseExisting,
        })
        .await?;
    run.signal(
        DECISION,
        &Decision {
            approved: true,
            by: "dana".into(),
        },
    )
    .await?;
    println!("result: {}", run.result().await?);
    engine.shutdown().await
}
