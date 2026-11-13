# Testing your workflows

The test kit gives a test an engine in memory under the [dev
profile](operations.md): every run's history is kept, each run can be
replayed against your code (strict replay), and the result of a stuck run
fails at once instead of waiting.

## Rust: `fusee::testing`

```toml
[dev-dependencies]
fusee = { version = "1.0.1", features = ["testing"] }
```

The feature exists only for tests: a build without it carries none of the
kit's hooks, so the step path is the same as in production.

```rust
use fusee::testing::TestEngine;
use std::time::Duration;

#[tokio::test]
async fn renews_after_a_day() -> fusee::Result<()> {
    let t = TestEngine::open().await?;
    t.register(RENEW, renew);
    // Replace a step's function by workflow type and step name.
    t.mock_step(RENEW, "charge", |_attempt| Ok(serde_json::json!("ch_1")));
    t.engine().start(RENEW, "r-1", &"pro".to_string()).await?;
    // The workflow sleeps a day: while every run waits on a timer, result()
    // moves the virtual clock to it, so this takes milliseconds.
    let out: String = t.result("r-1").await?;
    assert_eq!(out, "pro:42:ch_1");
    assert_eq!(t.steps("r-1").await?, ["quote", "charge"]);
    t.check().await?; // strict replay of every run
    Ok(())
}
```

| | |
| --- | --- |
| `TestEngine::open()`, `with_shards(n)` | a `memory` engine, dev profile, nothing served |
| `register(wf, f)` | as `Engine::register`, and again after each crash |
| `mock_step(wf, step, f)` | `f(attempt)` returns the step's result, or `Err(Failure)` to fail that attempt (retried under the step's policy unless final) |
| `advance(d)`, `now_ms()` | move the virtual clock; timers that fall due fire |
| `result(id)` | the result; while every run waits on a timer (and no step function waits on outside work), time skips to it |
| `crash()` | drop the engine's memory and recover every run from its journal: the code replays, recorded steps do not run again |
| `steps(id)`, `journal(id)`, `snapshot(id)` | what the run recorded; `snapshot` is stable JSON for snapshot tests |
| `check()` | replays every run against the registered code; an error names each run and step that differ |

### Finding steps that are not idempotent

A step's function can run twice: when the process dies after the function
ran and before its result was recorded, the step runs again after recovery.
`crash_everywhere` runs a workflow once per step boundary with such a crash
at each, and requires the same result and the same outside effects as a run
without one:

```rust
use fusee::testing::{crash_everywhere, Case};

#[tokio::test]
async fn checkout_survives_a_crash_anywhere() {
    crash_everywhere(|t| async move {
        let payments = Arc::new(FakePayments::default()); // fresh per run
        t.register(CHECKOUT, checkout_with(payments.clone()));
        Ok(Case {
            workflow: "checkout".into(),
            id: "order-1".into(),
            input: serde_json::json!({"cents": 500}),
            effects: Box::new(move || payments.snapshot()),
        })
    })
    .await
    .unwrap();
}
```

A step that charges a card without passing `ctx.key()` to the payment
service as an idempotency key fails with a message naming the step and both
sets of effects.

## Python: `fusee.testing`

```python
from fusee.testing import TestEngine

async def test_renewal():
    t = await TestEngine.open(workflows=[renew])
    t.mock_step(renew, "charge", lambda attempt: {"id": "ch_1"})
    await t.start(renew, "r-1", "pro")
    # The workflow sleeps a day: result() skips time while every run waits.
    assert await t.result("r-1") == "pro:ch_1"
    assert await t.steps("r-1") == ["quote", "charge"]
    await t.crash()          # drop memory, recover from the journals
    await t.check()          # strict replay of every run
    await t.close()
```

`mock_step` takes a function of the attempt number; raising fails that
attempt. `advance("1d")` moves the clock; `result()` moves it to the next
timer while no step function is running. `crash_everywhere(build)` takes a
function that returns a `Case(workflows, workflow, id, input, effects)` with
fresh fakes and raises `AssertionError` naming the step whose rerun changed
the result or the effects.

## TypeScript: `@bitsinfra/fusee/testing`

```ts
import { TestEngine, crashEverywhere } from "@bitsinfra/fusee/testing";

const t = await TestEngine.open({ workflows: [renew] });
t.mockStep(renew, "charge", (attempt) => ({ id: "ch_1" }));
await t.start(renew, "r-1", "pro");
assert.equal(await t.result("r-1"), "pro:ch_1"); // a day's sleep, skipped
await t.crash();
await t.check();
await t.close();

await crashEverywhere(() => {
  const payments = new FakePayments(); // fresh per run
  return { workflows: [checkoutWith(payments)], workflow: "checkout", id: "o-1", input: order, effects: () => payments.snapshot() };
});
```

The virtual clock of the Python and TypeScript kits is the process's: one
test engine at a time.
