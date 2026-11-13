# Benchmark

## Method

The harness is `bench/harness` (`fusee-bench`). Every result line is JSON
with the machine (CPU, cores, kernel, host load), the engine configuration
and the durability boundary.

- **Durable step overhead:** the time from one step returning to the next
  step starting, where the next depends on the first and the first is
  durable before it starts. Steps are no-ops; timestamps are taken inside the
  step functions. p50, p99, p99.9.
- **Closed loop** (one workflow at a time) for pure latency; **open loop**
  (arrivals on a schedule) for throughput, with latency measured from each
  workflow's *scheduled* start so queueing is never hidden.
- **Workloads:** (1) sequential chains of 1, 10, 100 and 1,000 steps;
  (2) fan-out of 100 parallel steps and fan-in; (3) steps calling a mock
  service with 1 ms and 50 ms latency; (4) highest sustained rate with step
  overhead p99 ≤ 1 ms; (5) start-to-first-step; (6) many sleeping workflows:
  memory and wake accuracy; (7) signal to resumed code; (8) SIGKILL at full
  load (`fusee-crashtest`); (9) CPU per million steps at three load points.
- **Floors:** `fusee-bench floors <dir>` (flush latency and loopback round
  trip) and `fusee-bench io-floors` (4 KiB `O_DIRECT` + `RWF_DSYNC` writes
  at depth 1 on the measured volume); `fio`, `io_uring` and `sockperf` on
  the benchmark machines.
- **Matched durability:** see `bench/drivers/README.md`. A comparison with
  another engine counts only when both make a step's result durable before
  the next step starts, release outputs only after durability, and survive
  the same failures. Margins are claimed only within a deployment class.
- **Procedure:** warm-up, then five trials with engine and case order
  rotated; host load recorded with every line.

### Instrumentation

- **Stall trace (any cell):** `--stall-us N` turns on the engine's stall
  tracer (every tick or flush over N µs, with what else the shard was doing);
  `--stall-trace FILE` names the trace (default `<dir>/stalls-<pid>.jsonl`).
  Every result line carries `stall_us` and `stalls`, and each open-loop point
  and sub-cell its own `stalls`: `count`, `max_us`, `by_kind` (`tick`,
  `inline-flush`, `flush`), `by_context` (`preparing_segment`,
  `checkpointing`, `parking`, `no_context`; each with `count`, `max_us`,
  `share`), `largest` (five lines verbatim), and the engine's own
  `engine_counted`, `engine_max_tick_us`, `engine_max_flush_us`. `null` when
  off. The AWS scripts trace at 1 ms and archive `<cell>.stalls.jsonl` beside
  `<cell>.jsonl`.
- **CPU by thread:** open-loop points and the extra cells report `cpu` over
  the measured window. On Linux, `threads` groups `/proc/self/task` by name
  (shard, flush, segment, checkpoint, remote, ping, replica, tokio, harness)
  with user, system and `schedstat` time; `engine_us_per_step` counts the
  engine's threads and the tokio workers, `engine_dedicated_us_per_step`
  leaves the tokio workers out (they also run the harness's per-workflow
  tasks), `harness_us_per_step` is the generator, and `exited_threads_us` is
  CPU of threads that ended inside the window. `process_us_per_step` is the
  whole process. Other systems report `threads: null` with a reason. `cost`
  shows both next to `cpu_seconds_per_million_steps`.
- **Interval series:** `--window-seconds W` adds `windows`, latency and stall
  counts per W seconds of scheduled arrivals.
- **The log, per open-loop point:** `log` gives the shared log's device
  writes, writes per second, shards' sections per write, the bytes the
  writes put on the device against the records' bytes (headers and
  padding), and segment rolls. The open loop starts each workflow and waits
  for its result with one engine call (`Engine::execute_then`), records into
  per-thread histograms, and above 100,000 starts a second paces arrivals
  from two threads (`arrival_threads`); their CPU is the generator's, in
  `harness_us_per_step`. Arrivals at least 500 µs apart are slept to
  within 75 µs and then spun to, so the generator is on time and its
  lateness is not counted as the engine's; closer arrivals are slept to
  with the smallest timer slack, at least 50 µs at a time
  (`FUSEE_BENCH_SPIN=1` spins to every arrival).
- **Cluster runs:** with `chain` among the workloads, `cluster_run.py` also
  runs five chain trials with adaptive receive moderation off on every
  member (`--no-rx-off` skips them) and, with `--busy-poll-trial-us N`, five
  with busy-polled receive; every member records `ethtool -c` and `-S`.
  `check_cluster.py` reports them as `closed_loop_rx_off` and
  `closed_loop_busy_poll`, with each trial's `io_thread_tick` and
  `io_thread_lock_hold` stages.

### Extra cells

`fanout-1000`, `fanout-10000`; `payload` (1 KiB, 64 KiB, 1 MiB step
results, `--steps` each); `children` (10 sequential and 100 parallel child
workflows); `service-concurrent` (1 ms and 50 ms steps with `--concurrency`
workflows in flight, 10,000 by default); `saturate` (open loop from
`--rate`, times `--growth` per point (1.5 unless given), until step p99
passes 1 ms or throughput rises less than 2%, then `--past` points more (2
unless given) to show whether latency recovers as batches grow; an
incomplete point ends it; `highest_within_1ms` is the highest complete
point at or under 1 ms wherever it falls, beside
`highest_within_1ms_before_first_miss`); `soak` (open loop at each of
`--rates` for `--seconds`, a `table` row per rate with p50, p99, p99.9 and
maximum).

### Running on AWS

`bench/aws` runs every case on a fresh, disposable EC2 instance with no
inbound access and verifies the archive it uploads. Set
`FUSEE_AWS_SPEND_LIMIT` (USD) first: each launch reserves its worst-case
cost against it and refuses to start past it.

- `host_run.py --case local|floors|gp3|cells|soak|sleepers|sdk|micro|pgo|comparisons`
  for one host (`--case cells [--cells …] [--cell-trials N]`; `--case soak
  [--soak-rates 1600,12800,51200] [--soak-seconds 600]`; `--stall-us N`, 0
  turns tracing off). `pgo` builds the harness with profile-guided
  optimization on the host, trained on the open loop and a mass wake.
- `cluster_run.py` for three replicas (`--workloads chain,open,saturate,failover`).
- `check_local.py`, `check_sdk.py`, `check_cluster.py` and the other
  `check_*.py` scripts recompute each summary from the archive.

## Results

All on the release code, AWS spot instances, ext4.

### One host

AWS i8g.2xlarge: Graviton4, 8 vCPUs, instance-store NVMe. The floor, a
4 KiB `O_DIRECT` + `RWF_DSYNC` write at depth 1, is 12 µs at p50 (p99 20 µs).

| Workload | Result |
| --- | --- |
| Sequential 1,000-step chain, step to step (five trials) | p50 14 µs, p99 22 µs, p99.9 108–112 µs |
| Start to first step, open loop at 1,000 starts/s | p50 24–27 µs, p99 40–44 µs |
| One-step workflow, start to result, `memory` mode | p50 6.2 µs |
| Fan-out of 100 parallel steps and fan-in | p50 211–457 µs, three log flushes |
| Signal to resumed code | p50 55–69 µs |
| Engine overhead per step calling a service (1 ms and 50 ms) | p50 17–19 µs |
| Rust `memory` mode, 200,000 steps | 0.22 µs per step (best chain average over five repetitions), about 1,750–1,950 instructions per step |
| Step p50 from 3,200 to 51,200 steps/s, 32-step workflows | 14 µs at every point (1.03 records per flush) |
| 1.64 million steps/s | step p50 51 µs, p99 101 µs; 46 / 97 µs with the profile-guided build |
| Highest rate with step p99 ≤ 1 ms | 6.55 million steps/s (p99 495 µs); 8 million with the profile-guided build (p99 708–763 µs, three trials of three) |
| CPU per step at 100,000 steps/s | 9.7 µs; 8.5 µs with the profile-guided build |
| Soak, 32-step workflows, ten minutes per rate | 51,000 steps/s: workflow p50 549 µs, p99 651 µs, p99.9 911 µs; 410,000: 1.9 / 3.3 / 3.7 ms; 1.64 million: 2.1 / 3.0 / 19.7 ms |
| Ten million parked workflows | 461 MiB resident (47 bytes each); reopened in 323 ms |
| Ten million timers due at once, eight shards | all fired within 4.2 s (p50 2.2 s), 2.4 million wakes/s |
| A million timers due at once, eight shards | all fired within 405 ms, 2.5 million wakes/s; within 331 ms (3.0 million/s) with the profile-guided build |
| Twenty SIGKILLs under load | no acknowledged workflow lost (1,521 workflows, 6,254 service calls) |

On EBS gp3 (c7i.2xlarge) the same 1,000-step chain runs at p50 1,040 µs,
within 21–39 µs of the volume's own 4 KiB durable write (1,001–1,019 µs).

### SDKs

Same instance type. The SDK runs use `local` durability and eight shards.

| | Result |
| --- | --- |
| TypeScript (Node.js), step to step, one chain with seven workflows waiting | p50 19.1 µs, p99 63.8 µs |
| TypeScript, eight chains at once on one thread | p50 51.4 µs |
| TypeScript, fan-out of 100 | p50 441 µs |
| TypeScript worker threads, step p99 ≤ 1 ms | 256,000 steps/s on one thread, 805,000 on eight |
| Python, step to step, pipelined flush | p50 21.8 µs, p99 92.4 µs |
| Server mode over shared memory, one shard | p50 15.9 µs, p99 59.2 µs (local in the same test took 14.9 µs and 55.5 µs, respectively) |
| Server mode over a Unix socket / TCP loopback | local + 12–18 µs / + 20–21 µs |

For the 1,000-step chains, each SDK percentile is the median of the
reported values across 15 Node repetitions (`sdk-live-1-7-default-*.jsonl`)
or five Python trials (`sdk-python-pipeline-*.jsonl`). The server benchmark
alternates paths for seven rounds of 500 steps each. It takes the median
of each percentile across rounds, then across three trials
(`server-step-cost-*.txt`).

### Replicated

Three i8g.2xlarge in one placement group; member round trips 30–134 µs
(sockperf p50 by pair), backup flush p50 17 µs.

| | Result |
| --- | --- |
| Step to step, closed loop (five trials) | p50 64–68 µs, p99 87–137 µs; with adaptive receive moderation off, p50 61–62 µs and p99 72–75 µs |
| Highest rate with step p99 ≤ 1 ms | 1.3 million steps/s in both trials (p99 813 µs), peak 2.9 million |

Three i4i.large in one placement group (round trips 56–98 µs, flush 31 µs):
chain p50 126–146 µs, p99 177–212 µs. Over 30 primary kills, the 10
failovers that completed before the next kill were ready for new work in
p50 25.0 ms (at most 30.3 ms) and committed new work in p50 30.3 ms (at
most 35.3 ms).
