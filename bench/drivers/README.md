# Drivers for other engines

Each driver runs the harness's workload 1 (a sequential chain of no-op
steps) on another engine and prints one JSON line in the same shape as
`fusee-bench run chain`: step-to-step overhead at p50/p90/p99/p99.9 and
whole-workflow latency, measured the same way (a timestamp taken inside each
step function, in the process that runs it).

The Obelisk workflow executes real WASM activities. Temporal runs with
default activities, with Local Activities, and with the dynamic
configuration of Windmill's published comparison (`temporal/tuned.yaml`).
The Inngest variants use its persistent development server.
Five-trial runs on an isolated AWS host use `matrix.py`. Each driver executes
a warmup, then groups transitions by workflow and checks the exact sample count.

## Matched durability

A comparison counts only when both engines answer these the
same way. The configuration below is chosen to match Fusee `local` (each
step's result durable before the next step starts; outputs after durability;
survives process crash and power loss on drives that honor flushes).

| Engine | Configuration | Durable before next step? | Notes |
| --- | --- | --- | --- |
| Temporal (default) | `temporalio/auto-setup` + Postgres, `synchronous_commit=on` | Yes | Activity per step |
| Temporal (tuned) | `temporal/tuned.yaml` | Yes | |
| Temporal Local Activities | `proxyLocalActivities` | **No** (recorded when the workflow task completes) | Shown labeled, not a matched comparison |
| Restate single node / 3 nodes | `restatedev/restate`, `ctx.run` per step | Yes (quorum for 3 nodes) | |
| DBOS on Postgres | same NVMe, `synchronous_commit=on` | Yes | |
| DBOS on SQLite | `synchronous=FULL` | Yes | |
| Inngest with checkpointing | dev server, `CHECKPOINT=1` | **No** (asynchronous checkpoint) | Shown labeled |
| Inngest without checkpointing | persistent dev server | Storage policy unverified | Standard orchestration path |
| Hatchet | `hatchet-lite` + Postgres | Yes | DAG of tasks |
| Absurd | Postgres stored procedures | Yes | |
| Obelisk | SQLite `synchronous=full` | Yes | WASM components |

Versions are pinned in `docker-compose.yml` and in each driver's
`package.json`, and recorded in the output line at run time.

## Running

```bash
docker compose -f bench/drivers/docker-compose.yml up -d postgres temporal restate hatchet
(cd bench/drivers/temporal && npm install && node chain.mjs 100)
(cd bench/drivers/restate && npm install && node chain.mjs 100)
(cd bench/drivers/dbos && npm install && DBOS_DATABASE_URL=postgres://postgres:bench@localhost:5432/dbos node chain.mjs 100)
```

Run order is rotated across trials (at low load, rankings otherwise follow
run order), and host load is recorded with each line.
