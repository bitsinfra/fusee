# Fusee

**Durable execution at the speed of the disk.** Fusee is an embeddable
durable execution engine for Rust, TypeScript (Node.js and Bun), Python, Go
and Java.
Workflows are ordinary async code; every step's result is journaled to a
per-shard log, one durable write covers every record that arrived meanwhile,
and the next step starts as soon as its input is durable. Code runs ahead of
the disk. Only work that leaves the process waits, and only for the records
it depends on.

A fusee is the cone-shaped pulley in a mechanical clock that evens out the
mainspring's pull, so the clock keeps the same pace whether the spring is
fully wound or nearly run down. That is the engine's promise: the same step
time at any load, with p99 close to p50.

## Quick start

[Installation](docs/installation.md) · [TypeScript quickstart](docs/tutorial-typescript.md) · [Python quickstart](docs/tutorial-python.md) · [Rust quickstart](docs/tutorial-rust.md) · [Documentation](https://fusee.bitsinfra.com/)

```rust
use fusee::{Config, Ctx, Engine, RetryPolicy, Signal, WorkflowRef};
use std::time::Duration;

const CHECKOUT: WorkflowRef<Order, String> = WorkflowRef::new("checkout");
const REVIEW: Signal<bool> = Signal::new("review");

let engine = Engine::open(Config::local("./data")).await?;
engine.register(CHECKOUT, |ctx: Ctx, order: Order| async move {
    let quote: Quote = ctx.step("quote", || pricing::quote(&order)).await?;
    let charge: Charge = ctx
        .step("charge", || payments::charge(&quote, ctx.key()))
        .retry(RetryPolicy::attempts(5))
        .await?;
    ctx.sleep("cool-off", Duration::from_secs(600)).await?;
    match ctx.wait_for(REVIEW).timeout(Duration::from_secs(86_400)).await? {
        Some(true) => Ok(charge.id),
        _ => ctx.step("refund", || payments::refund(&charge)).await,
    }
});
let run = engine.start(CHECKOUT, "order-123", &order).await?;
run.signal(REVIEW, &true).await?;
println!("{}", run.result().await?);
```

```ts
import { Engine, signal, workflow } from "fusee";

const review = signal<boolean>("review");

const checkout = workflow("checkout", async (ctx, order: Order) => {
  const quote = await ctx.step("quote", () => pricing.quote(order));
  const charge = await ctx.step("charge", (s) => payments.charge(quote, { idempotencyKey: s.key, signal: s.signal }), {
    retry: { attempts: 5, max: "1m" },
  });
  await ctx.sleep("cool-off", "10m");
  const approved = await ctx.waitFor(review, { timeout: "1d" });
  return approved ? charge.id : await ctx.step("refund", () => payments.refund(charge.id));
});

const engine = await Engine.open({ dir: "./data", workflows: [checkout] });
const run = await engine.start(checkout, "order-123", order);
await run.signal(review, true);
console.log(await run.result());
```

```python
import fusee

review = fusee.signal("review", bool)

@fusee.workflow("checkout")
async def checkout(ctx: fusee.Ctx, order: Order) -> str:
    quote = await ctx.step("quote", lambda: pricing.quote(order))
    charge = await ctx.step("charge", lambda s: payments.charge(quote, key=s.key), retry=fusee.Retry(attempts=5))
    await ctx.sleep("cool-off", "10m")
    approved = await ctx.wait_for(review, timeout="1d")
    return charge["id"] if approved else await ctx.step("refund", lambda: payments.refund(charge))

engine = await fusee.Engine.open("./data", workflows=[checkout])
run = await engine.start(checkout, "order-123", order)
await run.signal(review, True)
print(await run.result())
```

More: [`examples/`](examples) (payment saga, AI agent loop with tool calls,
scheduled job) and [`crates/fusee/examples`](crates/fusee/examples).

## What you get

- An acknowledged start, signal or step result is never lost within the
  durability mode's failure model.
- A recorded step result never changes, and code after it sees the same
  value on every run.
- A step's function runs at least once. It runs again only if the process
  failed before that step's result became durable. The step's key
  (`ctx.key()` in Rust, `s.key` in TypeScript and Python) is stable across retries and
  recovery; passing it to services that accept idempotency keys makes the
  effect exactly once.
- Nothing leaves the process based on state a crash could take back.

See [docs/guarantees.md](docs/guarantees.md).

The rest of the surface: retries with backoff, step and workflow timeouts,
durable sleeps and timers, signals with timeouts, child workflows, signals
between workflows, `all` and `race`, cancellation with compensation,
versioning (running executions finish on the version they started with),
determinism errors that name the step, parking of suspended runs out of
memory, checkpoints and log compaction, read-only steps that run without
waiting for the disk ([docs/features.md](docs/features.md#read-only-steps)),
and `fusee disk-check`.

Also in every SDK (Rust, TypeScript, Python, Go, Java)
([docs/features.md](docs/features.md)): queries; updates and
update-with-start; step heartbeats; async step completion by token;
continue-as-new; patching; terminate and reset; schedules (cron with time
zones, overlap rules, backfill); search attributes and filtered listing;
concurrency and rate limits; payload encryption; replay testing with
`fusee export` and a `Replayer`; and workflow ID and child close policies.

```ts
ctx.onUpdate("addItem", { validate: (i) => (i.qty > 0 ? undefined : "bad quantity"), handle: (i) => cart.add(i) });
ctx.onQuery("cart", () => cart.items);
const approved = await ctx.awaitCompletion("approval", (cb) => mail.sendApproval(cb.url ?? cb.token));
if (ctx.journalSize() > 1000) return ctx.continueAsNew(state);
await engine.schedule("nightly", { cron: "0 2 * * *", timezone: "Europe/London" }, "report");
```

## Durability modes

| Mode | Outputs wait for | Survives |
| --- | --- | --- |
| `local` | a durable flush on this machine (`F_FULLFSYNC` on macOS) | process crash; power loss on drives that honor flushes |
| server (`fusee server`) | the server's flush; one pipelined round trip | application crash and failover to a standby application |
| `memory` | nothing | nothing (tests) |
| `replicated` | a quorum of 2 of 3 (or 3 of 5) | loss of a minority of replicas |

`replicated` (embedded: three application instances form the group,
workflows run on the primary, a backup takes over from its own disk) uses a
Viewstamped Replication style protocol with protocol-aware recovery, clean
across 5,000 simulated seeds with crashes, partitions, message loss and
damaged entries, and tested end to end with primary loss and rejoin. See
[docs/replication.md](docs/replication.md).

## How fast

Measured on AWS i8g.2xlarge hosts (Graviton4, 8 vCPUs, instance-store NVMe,
ext4), whose 4 KiB durable write takes 12 µs. A step's time is from one
step returning to the next step starting, where the next depends on the
first and the first is durable before it starts.

| Step latency | p50 | p99 |
| --- | ---: | ---: |
| Rust, `local` | 14 µs | 22 µs |
| TypeScript (Node.js), `local`, one chain with seven workflows waiting | 19.1 µs | 63.8 µs |
| Python, `local`, pipelined flush | 21.8 µs | 92.4 µs |
| Server mode, shared memory | 15.9 µs | 59.2 µs |
| `replicated`, three hosts in one placement group | 64–68 µs | 87–137 µs |

With disk writes disabled, the Rust `memory` benchmark averaged 0.22 µs per
step over 200,000 steps in its fastest repetition.

- **Flat under load:** the step stays at 14 µs from 3,200 to 51,200 steps a
  second, and step p99 stays under 1 ms up to 6.5 million steps a second
  (8 million with a profile-guided build). `replicated` sustains 1.3
  million steps a second with p99 under 1 ms.
- **Idle workflows are cheap:** ten million parked workflows take 461 MiB
  (47 bytes each), and the engine reopens with all of them in 323 ms. Ten
  million timers due at once all fire within 4.2 seconds.
- **Failover:** after the primary process is killed, a replicated group of
  i4i.large hosts is ready for new work in 25 ms (p50; 30 ms at most).

Method, workloads and the full results: [docs/benchmark.md](docs/benchmark.md).

## How it is tested

- **Deterministic simulation** of the real runtime and Rust SDK: a disk that
  tears writes, loses unflushed data at crashes and fails flushes; shards
  whose messages die with the process; a mock external service. 300,000
  seeds, 2.1 million crash-recoveries, no violations of the checkers
  (acknowledged starts and signals survive; results never change; no
  external call on state a crash took back; everything finishes once faults
  stop). Deliberately broken engines (steps skipping their durability gate,
  outputs released early) are caught.
- **Replication simulation:** 5,000 seeds of the replication protocol under
  crashes, partitions, drops, duplicates, reordering and damaged entries.
- **SIGKILL loop** on real files (`fusee-crashtest`).
- **Cross-SDK conformance:** the same scenarios in Rust, TypeScript,
  Python, Go and Java, every workflow feature included, produce identical
  journals.
- **Fuzzing** targets for every decoder, plus a stable-toolchain smoke test.
- **TLA+ models** of the output-release rule and the replication protocol
  ([specifications and TLC results](spec/README.md)).

See [docs/testing.md](docs/testing.md).

## Repository

| Path | |
| --- | --- |
| `crates/fusee-core` | Deterministic core: records, segments, journal machine, shard state, timing wheel, replication protocol. No I/O. |
| `crates/fusee` | The engine and Rust SDK: local log with continuous flusher, shard runtime, recovery, checkpoints, `disk-check`, schedules, limits, codec, export and replay. |
| `crates/fusee-sim` | Deterministic simulator, checkers and the SIGKILL crash test. |
| `crates/fusee-server` | Server mode and replicated servers. |
| `crates/fusee-cli` | The `fusee` command. |
| `bindings/node`, `bindings/python`, `bindings/go`, `bindings/java` | TypeScript, Python, Go and Java SDKs. |
| `bindings/c` | The C interface the Go and Java SDKs (and other languages) embed. |
| `bench/` | The benchmark harness, drivers for other engines, fault-injection tools. |
| `conformance/`, `spec/`, `fuzz/` | Cross-SDK conformance, TLA+ models, fuzz targets. |
| `docs/` | Documentation. |

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Contributions are accepted under
the same terms with a DCO sign-off; see [CONTRIBUTING.md](CONTRIBUTING.md).
The web UI bundles third-party packages and fonts under their own licenses,
listed in [`licenses.txt`](crates/fusee/src/http/ui-dist/assets/licenses.txt)
and served by the UI at `/assets/licenses.txt`. Fusee sends no telemetry.
