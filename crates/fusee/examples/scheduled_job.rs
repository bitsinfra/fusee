//! A scheduled job: a schedule starts the report workflow on a cron
//! expression or an interval. The schedule is itself a workflow on durable
//! timers, and each scheduled time runs once, even across restarts.
use fusee::{Config, Ctx, Engine, Error, Overlap, ScheduleSpec};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let engine = Engine::open(Config::local("./data/scheduled-job")).await?;
    let sent = Arc::new(AtomicU32::new(0));
    let s = sent.clone();
    engine.register("report", move |ctx: Ctx, _: serde_json::Value| {
        let s = s.clone();
        async move {
            let id = ctx.workflow_id().to_owned();
            ctx.step("send-report", || {
                let id = id.clone();
                async move {
                    println!("  {id} sent");
                    Ok::<_, Error>(())
                }
            })
            .await?;
            s.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    // A real job: cron "0 2 * * *" in "Europe/London".
    let spec = ScheduleSpec {
        every_ms: Some(1_000),
        overlap: Overlap::Skip,
        workflow: "report".into(),
        ..Default::default()
    };
    let schedule = engine.schedule("nightly-report", spec).await?;
    while sent.load(Ordering::SeqCst) < 3 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!("next: {}", schedule.describe().await?["nextMs"]);
    schedule.delete().await?;
    engine.shutdown().await
}
