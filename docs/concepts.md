# Concepts

**Workflow.** Ordinary async code in Rust, TypeScript or Python, registered
under a type name and version. A run is identified by a workflow ID you
choose. Starting an ID that is running fails unless the start says to use
the existing run (or replace it); starting an ID whose last run finished
starts a new run unless the start says otherwise. See
[ID policies](features.md#workflow-id-and-child-policies).

**Journal.** Every operation a workflow makes through `ctx` is a journal
entry: `step`, `sleep`, `wait_for`, `child`, `signal`, `now`, `random`,
`uuid`, `await_completion`, `set_attributes`, an update handler's result.
Accepted updates, signals and cancellation are journal entries too, and so
are heartbeats and [stream](features.md#streams) chunks, which take no
position. Operations are numbered in the order the code calls them. Their
results are recorded in the order they are handed to the code. After a crash
the code runs again from the top, and the journal hands back the recorded
results in the same order, so the code takes the same path. Code between
`ctx` calls must be deterministic.

**Step.** `ctx.step(name, fn)` runs `fn` once every earlier record of the run
is durable, records its result, and returns it immediately. On replay the
recorded result is returned and `fn` does not run. If the process died after
`fn` ran but before its result was durable, `fn` runs again: steps are at
least once, and `ctx.key()` gives a stable idempotency key that makes them
exactly once at services that accept such keys. A step marked read-only runs
`fn` at once instead, without waiting for the records before it; see
[read-only steps](features.md#read-only-steps).

**Shards.** A workflow ID hashes to one of the engine's shards. Each shard is
single-threaded and owns one log, its in-memory state and its timers.
Messages between shards (child starts and results, signals between
workflows) go through a durable outbox and are acknowledged by the receiver.

**Running ahead of the disk.** Appending a record makes it visible at once.
A flusher makes records durable in batches; exactly one flush is in flight
per shard, and the next starts as soon as the last completes, so batches grow
with load and an idle shard flushes a single record immediately. Three kinds
of things wait for durability, and nothing else does:

1. a step's function, for the records before it (unless the step is
   [read-only](features.md#read-only-steps));
2. an output: an acknowledgment, a result, a message to another shard, or a
   stream chunk handed to a reader;
3. nothing else. Code keeps running.

**Objects.** An [object type](features.md#objects-keyed-state) is a set of
handlers over per-key JSON state. Each key is a long-lived run
(`<type>/<key>`) and each call is an update of it: exclusive calls run one
at a time per key, shared calls run at once and only read. An exclusive
handler's state changes are committed with its result, in its `UpdateDone`
record. The engine continues a key's run as new with a snapshot of its
state when its journal grows or before it parks, so a key's history stays
short.

**Parking.** A run that is only waiting on timers, signals or children is
written to the run store and dropped from memory after `park_after`
(default 1 s), or sooner, least recently used first, while loaded runs take
more than `memory_budget` (default 128 MiB across shards). A parked run
costs a 32-byte table entry plus its share of two hash indexes, under 48
bytes; parking again appends only the records added since its last frame.
It is loaded back when a timer fires or a signal arrives.

**Checkpoints.** Each shard checkpoints periodically. A checkpoint plus the
run store plus the log after it reconstructs the shard, so older log
segments are deleted.

**Determinism errors.** If the code calls a different operation than its
journal recorded at that position, the run stops with an error that names
both, for example "the journal recorded step 'charge' here but the code
called step 'refund'". The run stays stopped until code that matches is
deployed; it is not marked failed.

**Versions.** Register new code as a new version. Runs finish on the version
they started with; new starts use the newest.


When hot retention expires, a shard removes the run from lookups. It rewrites
a run store once obsolete frames occupy at least 8 MiB and half its bytes.
Two checkpoints keep their required store generations and log tails. To keep
full histories beyond hot retention, enable Rust `Config::archive_history(true)`,
TypeScript `archiveHistory: true`, or Python `archive_history=True`. The
checkpoint worker writes checksummed immutable segments under the shard’s
`archive/` directory. `fusee export` can read them after the live run expires.
Archive files keep the journal’s encoded payloads and grow with retained
history. The default keeps archival off.

## Checkpoints and cold state

A checkpoint records the shard’s durable log position, the run-store generation and its exact byte prefix, and references for active runs and outgoing messages. An unchanged active journal can reuse its existing stored prefix. Recovery loads the newest readable retained checkpoint and replays later records. Segment cleanup removes a contiguous prefix safe for the older surviving checkpoint. Finishing or expiring an ID leaves enough information in the store to keep an old value from reappearing after recovery.

For attribute searches, each shard builds a secondary index from the checkpointed prefix. Records newer than that prefix are checked separately. When the index is unavailable the query scans the stored state. With history archival enabled, terminal histories go to immutable checksummed files. Retrying an existing archive write verifies its contents before treating it as complete.
