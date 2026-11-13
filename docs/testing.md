# How Fusee is tested

## Deterministic simulation (`crates/fusee-sim`)

The real shard runtime, journal machine and Rust SDK run in one thread under
a seeded scheduler. One seed reproduces a run exactly
(`fusee-sim --seed 42`).

- **Disk:** writes since the last flush are lost, land whole, or tear
  (random sectors, sometimes garbage) at a crash, in any order; flushes fail
  at a configured rate, after which the shard stops and the process restarts.
- **Process:** crashes at random times; messages between shards die with it.
- **External service:** logs every call with its idempotency key and input;
  responses differ on every call, so a step that runs again returns a
  different value.
- **Workloads:** orders (steps with retries, a sleep, a signal with a
  timeout, a child, compensation on cancel), fan-out, workflows signalling
  workflows, read-only steps whose results feed a step with effects, and one scenario per [workflow feature](features.md): a counter
  taking updates (some refused, some retried with the same ID) and queries,
  a loop that continues as new, a workflow whose code is patched at the first
  crash, a heartbeating export whose step stalls once past its heartbeat
  timeout, an approval completed asynchronously by the client (stale tokens
  included), parents terminated with children under both close policies,
  runs reset before a step, a schedule with a random overlap rule, a
  concurrency limit (on odd seeds its waiters carry priorities and weighted
  fairness keys), debounced starts and batched events in bursts on a
  quarter of the seeds (their coordinators continuing as new every few
  events on two thirds of those), and a payload codec on a third of the
  seeds.
- **Checkers**, after every recovery and at the end:
  - every acknowledged start and signal exists;
  - a result once reported never changes;
  - the service never saw one key with two different inputs (read-only
    calls are logged apart: one that runs again with a different input is
    counted, not a violation, and shows read-only steps run ahead of the
    disk);
  - once faults stop, every workflow finishes;
  - an accepted update runs once (its ID appears once in the journal), and
    every call for it gets the same result;
  - every query answer is a state the journal recorded (an answer never
    reflects state a crash took back), and answers never go backwards;
  - a schedule never starts two runs for one scheduled time;
  - a limit never has more permits out than its size;
  - every acknowledged debounced or batched event names one run, always
    the same one, and that run starts and finishes; a batch run's input
    holds each of its events once and nothing else, a debounced run's
    input is one of its window's starts, and no run of the target type
    starts for a window no acknowledged event joined;
  - no run stops on a determinism error: replaying patched, continued and
    reset runs reproduces their journals;
  - at most one completion is accepted per async step;
  - a step resumed from heartbeat progress resumes from progress it reported;
  - children of closed parents end as their close policy says.
- **Self-test:** deliberately broken engines are caught: steps that skip
  their durability gate (caught by the service checker), outputs released
  before durability (the result checker), updates accepted twice, a limit
  handing out too many permits, starts between workflows that do not attach
  to an existing run (a scheduled time or child started twice), and query
  answers released before durability.

Results (`fusee-sim --seeds 0..300000`): 300,000 seeds, 2,144,395
crash-recoveries, 19,941,070 workflows, no violations, with 867,529 update
IDs answered, 436,232 runs queried, 436,644 completions, 6,800,903 scheduled
runs, 225,289 resets, 3,532,256 heartbeats, 1,650,008 debounced or batched
events in 795,030 windows, and 8,313 read-only steps that ran again with a
different input after a crash took back what they had read.

### The shared log in the simulator

Half the seeds (the odd ones) run the shards on one shared log
([`crates/fusee/src/wal.rs`](../crates/fusee/src/wal.rs)) over the simulated
disk, with one to three writes in flight completing in any order, zero to
two spare segments kept for reuse, and crashes injected in the window where
one write in flight is durable and another is not: of the 300,000 seeds
above, 150,000 ran on the shared log (32.5 million batches in their final
logs) and 95,833 offloaded large payloads (15.4 million records in the
blob area). The unit-level crash campaigns in
`crates/fusee/src/wal/tests.rs` drive the same code over an in-memory disk
that keeps, drops or tears each unsynced write, including a roll whose
earlier batch never became durable
(`a_stale_segment_below_the_chain_never_hides_later_segments`).

### Your workflows in the simulator

`fusee_sim::Scenario` runs application workflows the same way. A setup
function, called once per seed on a fresh simulated machine, registers
workflow functions (the same `async fn`s an `Engine` registers), the starts,
signals, cancels and terminates a client sends at given simulated times,
and checks on the final results. The seed chooses the shard count, park and
checkpoint timing, segment size, log format and payload encryption, and
injects process crashes, torn and lost writes, failed flushes and delayed
messages; the clock skips ahead whenever nothing is runnable.

```rust
let scenario = Scenario::new(|sim| {
    sim.workflow("pay", pay);                      // your workflow function
    sim.start(Duration::from_millis(5), "p-1", "pay", &Order { cents: 500 });
    sim.signal(Duration::from_millis(40), "p-1", "receipt", &true);
    sim.check(|out| {
        assert_eq!(out.result::<u64>("p-1"), Some(Ok(500)));
        Ok(())
    });
})
.crash_every(Duration::from_millis(150));
scenario.run_seeds(0..1_000)?;      // a failure names its seed:
scenario.run(417)?;                 // and the seed reproduces it
```

Besides the user's checks, every seed asserts that acknowledged starts and
signals survive recovery, reported results never change, every workflow
finishes once faults stop, and no run stops on a determinism error. The last
one catches workflow code that is not deterministic (a step name built from
a counter, iteration over a `HashMap`, the wall clock): the run replays
after a crash or a park and takes a different path than its journal.
Step bodies run on the simulator's thread, so they wait with
`fusee_sim::sleep` and stand in for external systems with shared in-memory
state, which outlives simulated crashes as a real external system would.
See `crates/fusee-sim/tests/scenario.rs`.

## Replication simulation

See [replication.md](replication.md): 5,000 seeds of the protocol under
crashes, partitions, drops, duplicates, reordering and damaged entries.

## Real crash tests

`fusee-crashtest driver <dir> <iterations>` kills a real engine process with
SIGKILL under load, again and again, then checks the same properties on real
files: every acknowledged workflow finishes, every result returned before a
kill is returned again, and no step reruns with a different input. On an
AWS i8g.2xlarge, 20 kills under load: 1,521 acknowledged workflows, all
finished, 6,254 service calls, no contradictions. On Linux the driver runs on ext4 with the shared log and
with per-shard logs (`O_DIRECT` and `RWF_DSYNC`), on tmpfs, and on ramfs
(which refuses `O_DIRECT`, so the logs fall back to buffered writes); every
acknowledged workflow finishes on each.

Under [LazyFS](https://github.com/dsrhaslab/lazyfs), which drops the page
cache's unsynced writes at each kill, 20 kills recovered all 864
acknowledged starts, and a dirty overwrite disappeared at each eviction as
it should. Eight campaigns of 20 kills with page-cache eviction finished
every acknowledged workflow. Block-write replay on EC2 (`bench/faults`,
device-mapper) cut the device at four successive points and recovered 9,
69, 187 and 401 acknowledged starts, all completed; every result returned
before the cut matched and no key changed inputs. Physical power interruption is not part
of these tests.

Replicated mode has the same test with three replica processes, killing
whichever is primary (`fusee-crashtest replicated-driver`): 20 primary
SIGKILLs, 400 acknowledged workflows, all finished, no contradicting
external calls.

## Cross-SDK conformance

The same scenarios in Rust, TypeScript, Python, Go and Java produce
identical normalized journals (`conformance/`), every workflow feature
included: read-only steps, updates (accepted, deduplicated, refused) and
queries, update-with-start, heartbeats, async completion, continue-as-new,
patching, search attributes, parent close policies, reset, cancelling
another workflow, a race whose losing wait is dropped, priorities and
fairness, debounce, batching and keyed objects. Schedules and the payload codec are native code in
every SDK, so they are the same by construction.

## Fuzzing

`fuzz/` has cargo-fuzz targets for record decoding, segment scanning,
protocol frames, the run store and checkpoints, and the journal matcher
(including updates, async completions, patches and heartbeats).
`crates/fusee-core/tests/fuzz_smoke.rs` runs mutated inputs and random
journal-machine operations on stable Rust.

## Models

`spec/` holds TLA+ models of the output-release rule, the replication
protocol, checkpoint installation and the acceptance rule of the shared log
(`FuseeLog.tla`), each checked by TLC ([specifications](../spec/README.md)).
