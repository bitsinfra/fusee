# Cross-SDK conformance

The same workflow scenarios are written in Rust, TypeScript, Python, Go and
Java.
Each runner executes them on a `memory` engine with `keep_history` on and
prints every run's normalized journal (`fusee::journal::normalize`: times,
run IDs and recorded random values left out). `compare.py` requires the
outputs to be identical.

```bash
cargo run --release -p fusee --example conformance > /tmp/rust.json
node bindings/node/conformance.mjs > /tmp/ts.json
PYTHONPATH=bindings/python/python python3 bindings/python/conformance.py > /tmp/py.json
cargo build --release -p fusee-c && (cd bindings/go && go run ./cmd/conformance) > /tmp/go.json
(cd bindings/java && mvn -q test-compile && java -cp target/classes:target/test-classes dev.fusee.cmd.Conformance) > /tmp/java.json
python3 conformance/compare.py /tmp/rust.json /tmp/ts.json /tmp/py.json /tmp/go.json /tmp/java.json
```

In Go and Java, step functions run on threads of their own, so two parallel
steps record in the order they finish; the Go and Java runners' `parallel`
scenario runs its two steps inline (`StepOptions{Inline: true}`,
`StepOptions.defaults().inline()`), in the order they start, as the other
SDKs' synchronous step functions do.

`FUSEE_WORKERS=2 node bindings/node/conformance.mjs` (or `bun`) runs the
TypeScript scenarios on two worker threads, one shard each, with the calls
routed from the main thread; its journals must match the others too.

| Scenario | Exercises |
| --- | --- |
| `seq` | three dependent steps |
| `parallel` | `all` over two steps, then a step on both results |
| `retry` | a step that fails twice, then succeeds (`Attempt` records) |
| `read_only` | a read-only step that fails once, then succeeds, feeding an ordinary step (read-only steps journal like any step) |
| `signal` | `wait_for` resumed by an external signal |
| `child` | a child workflow and its result |
| `sleep` | a durable timer |
| `values` | recorded `now`, `random`, `uuid` |
| `cancel` | cancellation delivered to a wait, then compensation |
| `fail` | a step's final failure caught by the workflow |
| `pause` | a step out of retries pauses its run (`Attempt` marked `paused`), a resume (`Resume`), then success |
| `send` / `listener` | a workflow signalling another workflow |
| `timeout_wait` | a wait that times out |
| `updates` | an update accepted, the same update ID again (deduplicated), one refused by the validator, a query; started with `use-existing` |
| `update_start` | update-with-start: the update in the start record |
| `heartbeat` | a step's heartbeat and its progress |
| `await` | an async completion finished with its token |
| `continued` | continue-as-new twice; the last run's journal |
| `patched` | `patched` and `deprecatePatch` markers |
| `attributes` | search attributes set twice |
| `parent_close`, `parent_close/terminate`, `parent_close/abandon` | a terminated parent and its children under both close policies |
| `reset` | a finished run reset before its second step |
| `victim` / `canceller` | a workflow cancelling another, then compensation |
| `race_wait` | a race whose losing wait is dropped, then a new wait gets the signal |
| `prio/0` … `prio/3` | four runs contending for a one-permit limit, each step with its own priority, fairness key and weight (the order they are served in is not journaled) |
| `debounce/run`, `debounce/describe` | three debounced starts sent together and a retried event ID: one run with the latest input, and the coordinator's `describe` |
| `batch/full`, `batch/timed`, `batch/describe` | a batch closed by its event count (a repeated event ID adds nothing), one closed by its wait, and the first coordinator's `describe` |
| `obj/cart`, `obj/caller`, `obj/calls` | a key of an object type: exclusive calls with a step, a retried call ID, a failed call that commits nothing, shared reads, a one-way call, a workflow's call and send (`obj/caller`), clearing, and an edit of the state from outside (`fusee.state.edit`); the answers in `obj/calls` |
| `obj/tally`, `obj/sums`, `obj/tally_state` | a key compacted after 128 operations: the new run's snapshot (state and kept call IDs) and the calls after it; a retried ID from before the compaction gets its kept result |

A run that a debounce or batch coordinator starts has a random workflow ID
(`<key>@<uuid>`), so the runners list its journal under the scenario's name.

`python3 conformance/remote.py` runs a real server, a Node worker with a Python
client, then a Python worker with a Node client, and, when a Go toolchain is
on the path (`FUSEE_REMOTE_GO=0` skips it), a Go worker with each of those
clients and each of those workers with a Go client (`bindings/go/cmd/remote`),
and, when the Java test classes are built (`mvn test-compile` in
`bindings/java`; `FUSEE_REMOTE_JAVA=0` skips it), a Java worker and a Java
client the same way with each of the others (`dev.fusee.cmd.Remote`).
It checks update-with-start, deduplication, queries, completion tokens,
listings, a step's stream read from the start and from an offset, final
results and calls to a key of an object type (a call, its retry, a one-way
call, the state), in every pairing.
`python3 conformance/replica.py` kills each language's primary in a separate
three-process mixed-language group and checks the recovered result and external
inputs. Build the server CLI, both native libraries and the TypeScript package
first, as in the bindings CI job.
