# Workflow features

Besides steps, sleeps, signals and child workflows, Fusee has the workflow
features below. Each one is a record type in the
journal plus an SDK call, with the same durability rules as steps, the same
simulator checkers and the same cross-SDK conformance scenarios. Names below
are TypeScript; Rust and Python use the same names in their own case
(`on_update`, `await_completion`, `continue_as_new`), Go in its own
(`fusee.OnUpdate`, `fusee.AwaitCompletion`, `ctx.ContinueAsNew`; see
[bindings/go](../bindings/go/README.md)), and Java as TypeScript does, with
types for results (`ctx.onUpdate(name, A.class, ...)`,
`ctx.awaitCompletion(name, T.class, ...)`, `ctx.continueAsNew`; see
[bindings/java](../bindings/java/README.md)). Read-only steps, first
below, are Fusee's own: an option on a step rather than a record type.

## Read-only steps

```ts
const order = await ctx.step("load", () => db.getOrder(id), { readOnly: true });
const price = await ctx.step("quote", () => pricing.quote(order), { readOnly: true });
const charge = await ctx.step("charge", () => payments.charge(price, { idempotencyKey: ctx.key() }));
```

A step's function normally waits until every record before it is durable,
so a crash can never take back an input it saw. A read-only step skips that
wait and runs at once. Mark steps that change nothing outside the process:
lookups, fetches, model calls. The result is recorded and replayed like any
step's, and later steps and outputs still wait for it to be durable. Above,
`quote` runs as soon as `load` returns, and `charge` waits for one flush
that carries both results instead of two.

The cost: if the process fails before the records ahead of a read-only step
are durable, the step may run again with different inputs, because it read
state the crash took back. That is harmless for a read and wrong for a step
with effects, which is why it is opt-in per step. The option is not
journaled, so adding or removing it never changes a journal.
`awaitCompletion` ignores it: its send function always has an effect. In
Rust: `StepOptions::default().read_only()`; in Python: `read_only=True`.

## Queries

```ts
ctx.onQuery("status", () => state);          // in the workflow
const s = await run.query("status");          // anywhere
```

The handler runs on the workflow's shard against its in-memory state; a
parked run is loaded first, and a run that is still replaying its journal is
answered once it has caught up. A query writes nothing. Its answer leaves the
process only once the state it read is durable, normally the next flush. A
handler must not change state or call `ctx`.

## Updates and update-with-start

```ts
ctx.onUpdate("addItem", {
  validate: (item) => (item.qty > 0 ? undefined : "quantity must be positive"),
  handle: async (item) => {
    const price = await ctx.step("price", () => pricing.quote(item));
    cart.push({ ...item, price });
    return cart.length;
  },
});
const n = await run.update("addItem", item, { id: "req-42" });
const { run, result } = await checkout.updateWithStart("order-123", order, "addItem", item);
```

- The validator runs first, against the current state. A refusal records
  nothing; the caller gets a `RejectedError`.
- An accepted update is appended with its update ID and delivered to the
  code at its place in the journal, so its handler's operations replay at the
  same positions. A retried call with the same ID gets the recorded result
  instead of running the handler twice. The result is released once its
  records are durable.
- `updateWithStart` starts the workflow if it is not running and puts the
  update in the start record itself, so the two never come apart; if the
  workflow is running, the update goes to it. The validator does not run for
  an update delivered with the start.
- Callers waiting on an update whose workflow ends first get an error; the
  accepted update stays in the journal.

## Step heartbeats

```ts
await ctx.step("export", async () => {
  for (let page = ctx.progress() ?? 0; page < pages; page++) {
    await exportPage(page);
    ctx.heartbeat(page + 1);
  }
}, { heartbeatTimeout: "30s" });
```

Heartbeats are coalesced (only the latest per step is appended, just before
the next flush) and never wait for the disk. A step retried after a crash
gets its last durable progress from `ctx.progress()`; a retry within the same
process gets the latest progress. With `heartbeatTimeout`, an attempt that
stops sending heartbeats fails and is retried under its retry policy.

## Streams

```ts
// The model call is the step; its tokens are a stream.
const answer = await ctx.step("generate", async () => {
  const out = ctx.stream("tokens");
  let text = out.recorded().join("");   // what an earlier attempt recorded
  for await (const token of model.stream(prompt, { prefill: text })) {
    out.append(token);
    text += token;
  }
  return text;
});

// Anywhere: a web handler, another process through a server, the UI.
const tokens = engine.run(id).stream("tokens", { from: lastSeen });
for await (const token of tokens) res.write(token);   // tokens.offset: where to reconnect
```

A run appends chunks (JSON, or bytes) to named streams; readers follow a
stream from an offset as it grows. The rules:

- **Appending never waits.** `append` records a chunk and returns: no
  flush per chunk. Chunks go to the log with whatever else is flushing,
  so one write covers every chunk appended since the last. A chunk takes
  no journal position (as a heartbeat), so adding or removing streaming
  never shifts a journal, and its offsets are 0, 1, 2, ... in the order of
  the appends.
- **Who writes it, and what completes it.** `ctx.stream(name)` inside a
  step's function opens a stream of that step: the step's final result
  completes it (a `StreamEnd` record with the step's status is appended
  just before the result's). In workflow code it opens a stream of the
  run, which the code's `close()` or the run's end completes; the run's
  steps may append to it too, so one stream can carry every step of an
  agent loop. A step cannot write a stream another step opened, and
  nothing can append to a complete stream.
- **Readers see only durable chunks.** A read returns the chunks from its
  offset that are durable (committed to a quorum, when replicated), or
  waits until one is (a long poll, 30 s by default, after which it asks
  again). It ends once the stream is complete and it has every chunk. A
  crash can lose chunks that were not durable yet, but never one a reader
  saw, so nothing a reader shows has to be taken back. The price is one
  flush between an append and its readers (tens of microseconds on a local
  disk) against tens of milliseconds between a model's tokens, which is why
  there is no option to read ahead of the disk.
- **Reconnecting.** A stream is addressed by offset: a reader that
  reconnects with the offset it reached (`reader.offset`) continues
  exactly there, in this process, in another one through a server, or
  after the worker failed over. A reader stays on the run it started on
  when the workflow ID moves on (continue-as-new, reset).
- **A step that runs again keeps the recorded prefix.** When a step that
  streamed runs again (a retry, or after a crash: what was durable), its
  earlier chunks stay and the new attempt appends after them.
  `recorded()` hands it those chunks, so it continues instead of starting
  over: pass them to the model as the start of its answer (an assistant
  prefill) and stream the rest. The model is not asked again for the
  recorded tokens, and readers see each token once. Streams have no
  retraction: an attempt that ignores `recorded()` and starts over
  appends a second answer after the first one's start. If an earlier
  attempt completed the stream and the crash took only the step's result,
  `ctx.stream` reports it closed (`closed`) and appends are dropped.
- **Replay appends nothing.** Workflow code runs again from the top after
  a crash; its appends are matched to the recorded ones, in order, and
  only those beyond them are appended. A replayed step's function does not
  run, so it appends nothing either.
- **Retention and memory.** Chunks are journal records: a loaded run's
  chunks count toward `memoryBudget`, go to the run store when the run
  parks and come back when it loads, and a finished run keeps its streams
  (in its summary, without `keepHistory` as well) until its retention ends.
  For readers that follow a stream, each stream keeps its newest durable
  chunks in memory (64 KiB); a reader behind them reads the run's journal
  once, and what it read (up to 8 MiB) is kept for its next reads.

A read's answer is the same JSON everywhere (see `fusee::StreamBatch`):
`{"run", "from", "next", "chunks", "bytes", "end"}`, where `bytes` lists
the chunks that are bytes (base64) and `end` is `null` or `{"status",
"closed"}`. `status` is `ok` for a stream its writer closed or whose step
succeeded, a failed step's `failed` (or `cancelled`), or the run's status
when the run ended with the stream open.

### In every SDK

| | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- |
| Open | `ctx.stream(name)?` | `ctx.stream(name)` | `ctx.stream(name)` | `c.Stream(name)` in workflow code, `fusee.Stream(ctx, name)` in a step function | `ctx.stream(name)` |
| Append | `w.append(&v)?`, `w.append_bytes(&b)?` | `w.append(v)` (a `Uint8Array` is bytes) | `w.append(v)` (`bytes` are bytes) | `w.Append(v)` (a `[]byte` is bytes) | `w.append(v)` (a `byte[]` is bytes) |
| Complete | `w.close()?` | `w.close()` | `w.close()` | `w.Close()` | `w.close()` |
| Earlier attempts' chunks | `w.recorded::<T>()?`, `w.recorded_len()`, `w.is_closed()` | `w.recorded()`, `w.recordedCount`, `w.closed` | `w.recorded()`, `w.recorded_count`, `w.closed` | `fusee.Recorded[T](w)`, `w.RecordedCount()`, `w.Closed()` | `w.recorded(T.class)`, `w.recordedCount()`, `w.closed()` |
| Read | `run.stream(name).from(n)`, `reader.next().await?` | `for await (const c of run.stream(name, { from }))` | `async for c in run.stream(name, from_=n)` | `for c, err := range run.Stream(name, n).All(ctx)`, `reader.Next(ctx)` | `for (Chunk c : run.stream(name, n))` (an `Iterator`) |
| Where it stopped | `reader.offset()`, `reader.end()` | `reader.offset`, `reader.status` | `reader.offset`, `reader.status` | `reader.Offset()`, `reader.Status()`, `reader.End()` | `reader.offset()`, `reader.status()`, `reader.end()` |
| From another process | `Client::stream(id, name, from)`, `Client::stream_read` | `client.run(id).stream(name, { from })` | `client.run(id).stream(name, from_=n)` | `client.Run(id).Stream(name, n)` | `client.run(id).stream(name, n)` |

Go and Java run step functions on goroutines and threads of their own, so
an append there is queued for the engine's loop and returns (one step's
appends stay in order, and come before its result); a failure is returned
by the next append or close. In Go a step opens its stream from the
context its function received (a goroutine has no identity to find the
step by); in Java `ctx.stream` inside a step function finds the step from
its thread, and a writer workflow code opened appends for the step whose
thread calls it. An attempt that timed out cannot append after its retry
started. The C interface (`bindings/c`, version 2) has the operations for
any other SDK: `FUSEE_OP_STREAM_OPEN`, `_APPEND`, `_CLOSE` and `_RECORDED`
and `fusee_engine_stream_read`, and a worker in any language answers the
`streamRead` calls clients send through a server.

The web UI shows a run's streams as they grow, and
`GET /v1/runs/<id>/streams/<name>?from=N&wait=MS` reads one
(`Accept: text/event-stream` follows it as it grows).

A chunk is a journal record, so a reader that reconnects continues at its
offset, a step that runs again continues after what it recorded, and a
crash never takes back a chunk a reader saw.

## Async step completion

```ts
const approved = await ctx.awaitCompletion("approval", (token) => mail.sendApproval(token));
// in another process, any time later:
await engine.complete(token, true);       // or engine.fail(token, "rejected")
```

The send function runs as a step does (after its gate, retried under the
retry policy). Each attempt gets its own token, which names the workflow, the
run, the step and the attempt; a completion with an older attempt's token, or
after the step finished, is refused. The completion is recorded like a step
result; the caller of `complete` is answered once it is durable. While it
waits, a Rust run is parked out of memory (TypeScript and Python runs stay
loaded). In server mode any machine can complete a token through
`fusee_server::Client`.

### Callback URLs

When the engine serves its HTTP API (the dev profile, `Config::http`,
`engine.serve()`), the send function also gets a URL that completes the
step: `POST` the value as JSON to it, or to `…/v1/fail/<token>` with a
failure. Behind a proxy, `engine.set_callback_url(base)` says where the API
is reached from outside.

```python
await ctx.await_completion("approval", lambda cb: mail.send_link(cb.url))
```

```rust
ctx.await_completion("approval", |cb: fusee::Completion| mail.send_link(cb.url))
```

## Compensation

```python
hold = await ctx.step("reserve", lambda: inventory.reserve(sku))
ctx.compensate("release", lambda: inventory.release(hold))
await ctx.step("charge", lambda: payments.charge(cents))   # fails: "release" runs
```

A compensation is a step registered after the step it undoes. If the
workflow fails or is cancelled, every registered compensation runs as a step
under its name, newest first, then the run ends with its failure. A
compensation that fails after its retries is logged and the others still
run. They are steps like any other: recorded, retried, and replayed, not run
again, after a crash. Rust: `ctx.compensate("release", move || release(hold))`.

## Signal with start

```python
await engine.signal_with_start(cart, "cart-7", Cart(), add, item)
```

Signals the run `id`, starting it with the input first if it is not
running. The start and the signal reach the run's shard back to back, so
the signal is recorded before a new run's code first runs; the call returns
once both are durable. Rust: `engine.signal_with_start(CART, "cart-7",
&cart, ADD, &item)`; over the protocol, `signalWithStart`.

## Cancellation into steps

Cancelling, terminating or closing a run cancels its running step functions:
in Rust and in Python's async steps at their next `await` (the future is
dropped; the coroutine gets `CancelledError`). A Python step on a thread
(`thread=True`) cannot be interrupted; it reads `s.cancelled` to stop early.

## Continue-as-new

```ts
if (ctx.journalSize() > 1000) return ctx.continueAsNew(state);
```

Ends the run and starts a new run of the same workflow ID with an empty
journal, in one record (the old run's end carries the new run's start).
Signals and updates that arrive afterwards go to the new run; callers waiting
for the result follow it. The old run's records become eligible for
compaction. `ctx.journalSize()` counts the run's journal operations and is
deterministic.

## Patching

```ts
const fee = ctx.patched("refund-rounding") ? Math.round(x) : Math.floor(x);
```

`patched` is true for new runs and for running ones the first time their code
reaches this point after the deploy, and false while replaying a journal that
passed this point before the patch existed. The decision is recorded as a
marker that takes no journal position, so adding a patch never shifts the
journal. Once no running workflow predates the patch, replace the call with
`ctx.deprecatePatch("refund-rounding")` and keep only the new branch; later
the call can be deleted. Pinned versions stay the default; patching is how a
workflow sleeping for months picks up a fix.

## Terminate and reset

- `run.terminate(reason)` ends a run at once. No more of its code runs, the
  result of a step still running is discarded, and its children follow
  their parent close policy.
- `run.reset({ beforeStep: "charge" })` starts a new run of the same ID whose
  journal is the old one cut just before the named step's first record, then
  runs the code from there. The copied records do not send their signals
  again; children they started are attached to, not started again. Signals
  received after the cut are reapplied unless
  `reapplySignals: false`. The old run is kept and marked `reset` (ended, if
  it was still running). A finished run can be reset when the engine keeps
  history (`keepHistory`).
- `ctx.key()` includes the run, so steps after the cut get new idempotency
  keys and really run again.

## Pausing when retries run out

A step that fails its last attempt fails its run, and its parent, the
callers waiting on `result()` and the calls queued behind an object's
handler all get the error. A step can pause the run instead, so the cause
is fixed (the fix deployed, if it is in the code) and the run continues as
if the step had succeeded the first time:

```ts
await ctx.step("charge", (s) => payments.charge(order, { idempotencyKey: s.key }), {
  retry: { attempts: 5, onExhausted: "pause" },
});
const engine = await Engine.open({ workflows, onExhausted: "pause" }); // every step's default
// Later, once payments are back:
await engine.run("order-1").resume();
```

```python
await ctx.step("charge", charge, retry=fusee.Retry(attempts=5, on_exhausted="pause"))
engine = await fusee.Engine.open(dir, on_exhausted="pause")
await engine.run("order-1").resume()
```

Rust `RetryPolicy::attempts(5).on_exhausted(Exhausted::Pause)` and
`Config::on_exhausted`, Go `RetryPolicy{Attempts: 5, OnExhausted:
fusee.Pause}` and `Options.OnExhausted`, Java
`RetryPolicy.attempts(5).onExhausted(Exhausted.PAUSE)` and
`Options.onExhausted`. The default is to fail.

- **What pauses.** A failure the policy would retry, on the last attempt
  or once `maxElapsed` has passed: an exception, a step timeout, a missed
  heartbeat. A final failure (`FatalError`, `retryable = false`,
  `nonRetryable`) still fails at once. An async completion's `send`
  function fails when its retries run out; only steps pause.
- **While paused.** The run's status is `paused`. Its parent, callers of
  `result()` and an object's queued calls keep waiting; shared handlers
  still run. A paused run parks like a suspended one. Its deadline keeps
  running: a workflow timeout ends it as `timed_out`. A cancellation
  reaches the paused step (it ends `Cancelled`, without another attempt,
  and compensations run); terminate and reset work as on any run.
- **Resume.** `run.resume()` in every client, `fusee runs resume <id>`,
  `fusee runs bulk resume --status paused`, `POST /v1/runs/{id}/resume`,
  and Resume on the UI's Needs attention screen. The step runs again at
  once with a fresh set of attempts: `s.attempt` counts from 1 again, and
  `maxElapsed` from the resume; the journal goes on numbering the step's
  attempts. A resume records who asked (`by`, see [Audit](#audit)); one
  for a run with nothing paused is refused.
- **Which code.** The resume runs the code the process has then. A fix
  that leaves the steps before the paused one as they were needs nothing
  more; one that changes them is a [patch](#patching), as for any deploy.
- **Where it shows.** `paused` in listings, `fusee runs ls --status
  paused`, the Needs attention screen, and the `runs_paused` gauge. Each
  pause writes one warning line naming the run, the step and the failure;
  Python, Go and Java also call `engine.on("paused", ...)` handlers.

In the journal the last failed attempt is an `Attempt` whose retry time
is "until resumed" (`"paused": true` in normalized journals), and a resume
is a `Resume` record naming the step and the attempt it resumes after.
Neither is written on the successful step path.

## Schedules

```ts
const s = await engine.schedule("nightly-report",
  { cron: "0 2 * * *", timezone: "Europe/London", overlap: "skip" }, "report", input);
await s.pause(); await s.resume(); await s.trigger();
await s.backfill(new Date("2026-11-01"), new Date("2026-11-07"));
await s.describe(); await s.delete();
```

A schedule is itself a Fusee workflow (`fusee.schedule/<name>`) built on
durable timers, so there is no separate scheduler. Cron has five fields plus
aliases (`@daily`, `@hourly`...); `every` sets a fixed interval. Each run's
workflow ID is the schedule name and its scheduled time
(`nightly-report@2026-11-09T02:00:00Z`) and attaches to an existing run with
that ID, so one time never fires twice, even across crashes and backfills.

- Overlap: `skip` while the previous run runs, `buffer-one`, `allow-all`, or
  `cancel-other` (cancel the running one, then start).
- `jitter` delays each start by a recorded random amount; after downtime,
  missed times within `catchupWindow` (default one year) still start.
- Runs a schedule starts are abandoned by it: deleting the schedule leaves
  them running.
- The schedule workflow is the same native code in every SDK and continues
  as new every 50 actions.

## Search attributes

```ts
ctx.setAttributes({ customerId, status: "paid", amount });
engine.list({ where: { status: "paid", amount: { gte: 100 }, type: "checkout" } });
```

Attributes are recorded in the journal (`null` removes a key) and are never
encrypted. A filter matches the run's `type`, `status`, `id`, `startedMs` and
any attribute, with equality or `eq`, `ne`, `gt`, `gte`, `lt`, `lte`. Each
shard answers from memory for loaded runs and from the run store for parked
and finished ones; a finished run keeps its merged attributes there even
without `keepHistory`. The engine merges the shards' answers.

## Concurrency and rate limits

```ts
engine.limit("stripe", { concurrent: 10, perSecond: 100 });
await ctx.step("charge", () => stripe.charge(...), { limit: "stripe" });
```

A step with a limit takes a permit after its gate and before each attempt,
and returns it when the attempt ends. In one process every shard shares one
counting semaphore and token bucket. In server mode
the server persists each definition and renews held permits on its monotonic
clock. The replicated primary records definitions, grants, renewals and
releases in the quorum log. A successor waits for prior leases to lapse.
The concurrency bound counts valid leases. Work already dispatched to an
external service can continue after its lease expires.
`engine.limit_waiting(name)` (Rust) counts this process's steps waiting for
a permit.

### Priority and fairness

```ts
await ctx.step("charge", () => stripe.charge(...), {
  limit: "stripe", priority: 10, fairnessKey: tenantId, fairnessWeight: 2,
});
```

Steps waiting for a limit's permit are served in this order:

1. **Priority**, higher first (default 0, any 32-bit integer). A lower level
   waits while a higher one has waiters, so a steady stream of urgent work
   can hold background work back indefinitely; that is what strict
   priority means.
2. **Fairness key**, within one level: waiters are grouped by key and the
   groups take turns (round-robin), a key taking up to its weight in
   permits per turn (default 1; the latest waiter of a key sets it). A
   tenant with ten thousand queued steps gets one permit in each round, the
   same as a tenant with one. A key that runs out of waiters leaves the
   rotation and rejoins at the back. Steps without a key share one group.
3. **Arrival order** within a key.

With neither option every step is in one group at one level, which is the
arrival order limits always had. The rate limit applies to whoever is
next: when the bucket is empty, the next waiter waits for the refill and
the others wait behind it.

The order is not journaled, so adding or changing a priority never changes
a journal. A waiting step holds no permit and no record; after a crash its
code reaches the step again with the same options and asks again, at the
same priority and key, behind the waiters that asked first. In server mode
the server keeps the queue (every waiting ticket with the priority it was
first asked with) and decides the order, so the same rules hold across
worker processes; a worker asks again only for its own best waiting
ticket, and the server may answer with whichever of that worker's tickets
it serves next. Replicated groups record the priority with each request in
the quorum log, and checkpoints carry the queue with its rotation.

```python
await ctx.step("charge", lambda: stripe.charge(...), limit="stripe",
               priority=10, fairness_key=tenant_id, fairness_weight=2)
```

In Rust: `StepOptions::default().limit("stripe").priority(10).fairness(&tenant).fairness_weight(2)`.
`engine.limitInUse(name)` / `limitWaiting(name)` (TypeScript),
`limit_in_use` / `limit_waiting` (Python and Rust) report a limit's held
permits and this process's waiting steps.

Priority and fairness order the steps waiting for one named limit, shared
by every step that calls one external resource, whichever workflow type or
shard it runs on; there is no task queue in between because the engine runs
the step itself. Fairness keys share one bound between keys; use one limit
per key for a per-tenant cap.

## Runs at once

```ts
const report = workflow("report", { concurrency: { limit: 2, key: (input) => input.tenant } }, async (ctx, input) => {
  // At most two reports per tenant run at a time; the rest wait their turn.
});
```

```python
@fusee.workflow("report", concurrency=fusee.Concurrency(2, key=lambda input: input["tenant"]))
async def report(ctx, input): ...
```

At most `limit` runs of the type, or of each key, run at a time; the
others wait in the order they were started and run as turns free up. In
Rust, `engine.limit_runs("report", 2)` and `.concurrency_key(tenant)` on
the start; over the HTTP API, `options.concurrencyKey`.

A waiting run is a slot run, `fusee.queued/<id>`, until its turn: the UI
lists it as queued, and `result(id)` waits through the queue. The type's
(or key's) coordinator, `fusee.concurrency/<type>[/<key>]`, keeps the queue
and the runs holding a turn in its journal, so a run keeps its turn while
it sleeps for a week and across crashes, and each waiting run is admitted
once. Nothing of this touches a step: it costs a few records per run, at
its start and its end.

- Starts from outside a workflow take turns (an engine, a client, the HTTP
  API); runs a workflow starts do not, including a child, a schedule's run
  and a delayed start.
- Cancelling a run that waits takes it out of the queue. Terminate the run
  itself, not its slot, once it runs.
- The newest start's `limit` applies, so a deploy can change it.
- [Limits](#concurrency-and-rate-limits) bound steps, not runs: use them
  for a rate-limited API, and runs at once for how many runs work at a time.

## Debounce

```ts
const run = await reindex.debounce("doc-42", doc, { window: "5s", maxWait: "1m" });
await run.started();          // the run exists once the window closes
const out = await run.result();
```

```python
run = await reindex.debounce("doc-42", doc, window="5s", max_wait="1m")
await run.started()
out = await run.result()
```

Starts of a workflow type with the same key inside the window collapse
into one run. The run begins when the window closes: `window` after the
latest start (trailing edge), or `maxWait` after the first start, whichever
comes first (without `maxWait` a steady stream of starts delays it
indefinitely). The run's input is the latest start's; the latest start's
window settings apply. Each key of each workflow type has its own window.

**What the caller gets back.** The call resolves once the start is durable,
with the handle of the run that will take it: its workflow ID
(`<key>@<uuid>`, chosen when the window opens) is known at once, but the
run exists only when the window closes. Until then `status` is empty and
`result` fails with `NotFound`; `started()` waits. Every start that joined
the window gets the same handle, although only the latest start's input is
used.

**Across crashes.** Each key has a coordinator, a built-in workflow
(`fusee.debounce`, ID `fusee.debounce/<workflow>/<key>`) made from
existing records, as schedules are. A start is an update sent with
update-with-start: one record starts the coordinator if needed and delivers
the start; the update's handler puts it in the open window and returns the
receipt; a durable timer closes the window; a child start (abandoned by its
parent) begins the run. So:

- an acknowledged start is part of exactly one window, across any crash:
  the handler runs again at its place in the coordinator's journal;
- a window starts one run: its ID was recorded when the window opened, and
  the child start is sent again after a crash with that ID and attaches;
- a start that fails was not counted, unless the failure came after the
  start became durable (the caller's own process crashed, or a network
  reply was lost). Pass an event ID (`id`) to make such a retry safe: the
  key answers a repeated ID with the first receipt and adds nothing, for
  `dedupWindow` (default ten minutes) after that event, and for its latest
  1,024 IDs.

The coordinator ends once no window is open and no remembered ID is
younger than its dedup window, so idle keys cost nothing; it continues as
new every few hundred journal operations, carrying the open window and the
remembered IDs. A start that reaches a coordinator as it ends is refused
before its handler runs and the client sends it to the next one, which is
what `debounce` does. A coordinator is an ordinary run: `inspect`,
listings and the UI show it, and its `describe` query returns the open
window and its due time. Terminating a coordinator drops its open window.
`fusee export --sample` leaves coordinators out, as it does schedules.

In Rust: `wf.debounce(key, &input, &Debounce::new(window).max_wait(cap))`
or `debounce_with_id`. The event ID is `{ id }` in TypeScript and `id=` in
Python, and the dedup window `dedupWindow` / `dedup_window`.

## Batching

```ts
const run = await ingest.batch("tenant-7", event, { maxEvents: 100, maxWait: "2s" });
```

```python
run = await ingest.batch("tenant-7", event, max_events=100, max_wait="2s")
```

Events for a workflow type with the same batch key accumulate until the
batch holds `maxEvents` events or `maxWait` has passed since its first
event; then one run starts with every event, in the order they were
accepted, as its input (an array; at most 10,000 events). The call
resolves once the event is durable, with the handle of the batch's run, as
for debounce. The durability rules are the debounce ones: an acknowledged
event is in exactly one batch's input, once; a batch starts one run; a
retried event with the same `id` is added once. The coordinator type is
`fusee.batch`, ID `fusee.batch/<workflow>/<key>`. A full batch starts its
run from the update's handler, so a batch never holds more than
`maxEvents`.

Events can come from anywhere a start can: callers outside the engine, and
workflow code through a step (`ctx.step("enqueue", () => ingest.batch(key, e, { id: ctx.key() }))`),
where the step's idempotency key makes the event exactly once despite the
step's at-least-once retries.

In Rust: `Workflow<Vec<E>, O>::batch(key, &event, &Batch::new(max, wait))` or
`batch_with_id`.

### In every SDK

The coordinators are native code shared by every SDK, so the SDKs add only
calls, no workflow code. Each call is update-with-start on the key's
coordinator, built by `fusee::queue::request` and sent again when the
coordinator it reached was ending ("the workflow ended before the update
finished"):

| | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- |
| Debounce | `wf.debounce(key, &input, &Debounce)`, `debounce_with_id` | `wf.debounce(key, input, { window, maxWait, dedupWindow, id })` | `await wf.debounce(key, input, window=, max_wait=, dedup_window=, id=)` | `wf.Debounce(ctx, key, input, fusee.DebounceOptions{Window, MaxWait, DedupWindow, ID})` | `wf.debounce(key, input, DebounceOptions.window(d).maxWait(d).dedupWindow(d).id(id))` |
| Batch | `wf.batch(key, &event, &Batch)`, `batch_with_id` | `wf.batch(key, event, { maxEvents, maxWait, dedupWindow, id })` | `await wf.batch(key, event, max_events=, max_wait=, dedup_window=, id=)` | `wf.Batch(ctx, key, event, fusee.BatchOptions{MaxEvents, MaxWait, DedupWindow, ID})` | `wf.batch(key, event, new BatchOptions().maxEvents(n).maxWait(d).dedupWindow(d).id(id))` |
| Wait for the run | `RunHandle::started()` | `run.started()` | `await run.started()` | `run.Started(ctx)` | `run.started()` |
| From another process | `Client::update_with_start` with `queue::request` | `client.debounce(type, key, input, opts)`, `client.batch(...)`, `remoteRun.started()` | `client.debounce(type, key, input, ...)`, `client.batch(...)`, `remote_run.started()` | `client.Debounce(ctx, type, key, input, opts)`, `client.Batch(...)`, `remoteRun.Started(ctx)` | `client.debounce(type, key, input, opts)`, `client.batch(...)`, `remoteRun.started()` |

They work from a `Client` through a server (or a replicated server group)
whatever language the worker is written in, and with worker threads in
TypeScript
(`Engine.open({ workers })`): the main thread routes each event to the
worker that owns its coordinator's ID, and each run starts on the worker
that owns its own ID. The conformance suite checks that a debounced start
and two batches journal identically in every SDK.

## Objects (keyed state)

```ts
import { shared } from "fusee";

engine.object("cart", {
  // Exclusive (the default): one call at a time per key, in arrival order.
  add: async (ctx, item) => {
    const price = await ctx.step("price", () => pricing.quote(item));
    const items = ctx.state.get("items") ?? [];
    items.push({ ...item, price });
    ctx.state.set("items", items);
    return items.length;
  },
  // Shared: runs at once, next to other calls; reads only.
  get: shared(async (ctx) => ctx.state.get("items") ?? []),
});

const cart = engine.objectRef("cart", "user-7");
const n = await cart.call("add", item, { id: "req-42" });   // the handler's result
await cart.send("add", item);                              // once it is accepted
const state = await cart.state();                          // { items: [...] }

// In a workflow or another object's handler: durable, exactly once.
const m = await ctx.object("cart", "user-7").call("add", item);
ctx.object("cart", "user-7").send("add", item);
```

An object type is a set of handlers; each key of the type is one object
with its own key/value state, built from
Fusee's own records: a key is a long-lived run and each call is an update
of it, so it needs no new kind of storage and works in every mode
(embedded, worker threads, server, replicated) and through every client.

**A key is a run.** The first call to a key creates a run with workflow ID
`<type>/<key>` and workflow type `<type>`, marked as an object in its
`Start` record. It never finishes on its own (terminating it deletes the
key: the next call starts with empty state). `inspect`, listings, export and
the web UI show it like any run; when it is idle it is parked like a
suspended run (a table entry under 48 bytes and a store frame of one
record). An ID held by a run that is not an object of that type refuses
the call.

**Handlers.**

- *Exclusive* handlers (the default) run one at a time per key, in the
  order their calls reach the key's shard, and may read and change state.
- *Shared* handlers run as soon as they arrive, concurrently with each
  other and with a running exclusive handler. They read state and cannot
  change it.
- Both are workflow code: `ctx.step`, `sleep`, `awaitCompletion`, child
  workflows, calls to other objects and streams all work, with the usual
  replay rules. Each call is an update of the key's run: it starts at its
  place in the journal and its operations replay at the same positions.
  Handlers have no validators.

**State.** JSON values under string keys: `get(key)` (absent: `None` in
Rust and Python, `undefined` in TypeScript), `set(key, value)` (null
clears), `clear(key)`, `clearAll()`, `keys()`. Reads are synchronous: the
engine holds the key's state next to its run.

- An exclusive handler sees its own writes at once. They take effect for
  everyone else when it returns: its `UpdateDone` record carries them
  with its result, so the change and the result are durable together,
  and released together. A handler that fails (an exception, or a step
  that fails for good) changes nothing.
- A shared handler sees the state as last committed, never a running
  handler's uncommitted writes.
- Reads are not journaled. Commits happen when handlers return, at
  positions the journal fixes, so a handler replayed after a crash reads
  exactly what it read the first time.

**When a call is durable, and exactly once.** A call is *accepted* when its
handler starts: its `Update` record is appended. From then on it runs to
completion exactly once across crashes and its result is released once
durable. An exclusive call that arrives while another runs waits for the
key in memory at the key's shard, in arrival order; a crash drops waiting
calls (none of them had started) and their callers send them again:

- **From a client** (`call`): pass an idempotency ID. A call sent again
  with the same ID, whether the first one was waiting, running or done,
  runs once and every attempt gets the same recorded result. A key
  remembers the IDs and results of its calls with IDs while they are in
  its current run, and across compaction (below) its latest 1,024 for an
  hour after the compaction that kept each. Without an ID a
  retry is a new call. `send` is a call whose caller does not take the
  result: it resolves once the call is accepted.
- **From a workflow or a handler** (`ctx.object(type, key).call`): the call
  is a journal operation, recorded as a `Child` record flagged as a call,
  and goes through the durable outbox like a child start. The outbox sends
  it again until the key accepts it, the key recognizes it by an ID made
  from the caller's journal position, and the result comes back to the
  caller's journal as a `ChildDone` (the key's `Update` record names its
  caller, so the answer is sent again after a crash on either side). So the
  handler runs exactly once and its result is recorded once.
  `ctx.object(...).send` is the same without waiting: it returns at once
  and the outbox delivers it.

**Compaction.** A key's history is bounded. When no handler is running and
either the key's run has made 128 journal operations or it is about to be
parked, the engine continues the run as new with a snapshot as the new
run's input: the committed state and the IDs and results of recent calls,
in one record. Calls waiting for the key move to the new run. So loading a
parked key, recovering one, or reading its state never replays more than
about 128 operations of handler code, and an idle key's journal is one
record. Reading a key's state never runs code: it is the snapshot plus the
changes recorded since. A new version of the object's code takes over at
the next compaction; calls running at a deploy replay against the new code
as workflow updates do.

**Deadlocks.** An exclusive handler that calls an exclusive handler of its
own key, or keys whose exclusive handlers call each other in a cycle, wait
forever. Use `send` for such calls, or a shared handler.

**Cost.** A call is an update: an `Update` and an `UpdateDone` record
(which also carries the state changes) and one flush before the result is
released, plus a compaction record every 128 operations or idle period.

**Code that is not registered.** A call from workflow code to a type no
process has registered waits (the outbox sends it again each second) until
one does; register object types right after the engine opens.

**Reading from outside.** `state()` returns the committed state, once it
is durable (as a query). `inspect()` returns the key's run with its
journal, its state and its pending calls (running and waiting).
`engine.objects(type)` lists a type's keys.

| | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Define | `engine.object("cart").exclusive("add", h).shared("get", h).register()` | `engine.object("cart", { add, get: shared(get) })` | `cart = engine.object("cart")`, `@cart.exclusive`, `@cart.shared` |
| Handler | `\|ctx: ObjectCtx, arg: A\| async move { ... }` | `async (ctx, arg) => ...` | `async def add(ctx, arg)` |
| State | `ctx.state().get::<T>(k)?`, `set(k, &v)?`, `clear(k)?`, `clear_all()?`, `keys()?` | `ctx.state.get(k)`, `set(k, v)`, `clear(k)`, `clearAll()`, `keys()` | `ctx.state.get(k)`, `set(k, v)`, `clear(k)`, `clear_all()`, `keys()` |
| The key | `ctx.object_key()` | `ctx.objectKey` | `ctx.object_key` |
| Call from outside | `engine.object_ref("cart", k).call_with_id::<_, R>("add", &item, id)` | `engine.objectRef("cart", k).call("add", item, { id })` | `await engine.object_ref("cart", k).call("add", item, id=...)` |
| Send from outside | `.send("add", &item)` | `.send("add", item, { id })` | `await ref.send("add", item, id=...)` |
| Call from code | `ctx.object("cart", k).call::<_, R>("add", &item).await?` | `await ctx.object("cart", k).call("add", item)` | `await ctx.object("cart", k).call("add", item)` |
| Send from code | `ctx.object("cart", k).send("add", &item)?` | `ctx.object("cart", k).send("add", item)` | `ctx.object("cart", k).send("add", item)` |
| Read | `.state().await?`, `.inspect().await?`, `engine.objects("cart", n)` | `.state()`, `.inspect()`, `engine.objects("cart")` | `await ref.state()`, `ref.inspect()`, `engine.objects("cart")` |
| From another process | `Client::object_call`, `object_send`, `object_state` | `client.objectRef("cart", k)` | `client.object_ref("cart", k)` |

| | Go | Java |
| --- | --- | --- |
| Define | `fusee.RegisterObject(e, "cart", fusee.Handlers{"add": fusee.Exclusive(add), "get": fusee.Shared(get)})` | `e.registerObject("cart", Map.of("add", ObjectHandler.exclusive(Item.class, add), "get", ObjectHandler.shared(Object.class, get)))` |
| Handler | `func(c *fusee.ObjectCtx, arg A) (R, error)` (steps: `fusee.Step(c.Ctx, ...)`) | `(ObjectCtx c, A arg) -> ...` (steps: `c.ctx().step(...)`) |
| State | `fusee.StateGet[T](c.State(), k)`, `Set(k, v)`, `Clear(k)`, `ClearAll()`, `Keys()`, `Raw(k)` | `c.state().get(k, T.class)`, `set(k, v)`, `clear(k)`, `clearAll()`, `keys()`, `raw(k)` |
| The key | `c.ObjectKey()` | `c.key()` |
| Call from outside | `fusee.CallRef[R](ctx, e.Object("cart", k), "add", item, fusee.CallOptions{ID: id})` | `e.object("cart", k).call("add", item, R.class, CallOptions.id(id))` |
| Send from outside | `ref.Send(ctx, "add", item, fusee.CallOptions{ID: id})` | `ref.send("add", item, CallOptions.id(id))` |
| Call from code | `fusee.Call[R](c.Object("cart", k), "add", item)` | `ctx.object("cart", k).call("add", item, R.class)` |
| Send from code | `c.Object("cart", k).Send("add", item)` | `ctx.object("cart", k).send("add", item)` |
| Read | `ref.State(ctx)`, `ref.Inspect(ctx)`, `e.Objects(ctx, "cart", n)` | `ref.state()`, `ref.inspect()`, `e.objects("cart", n)` |
| From another process | `client.Object("cart", k)` | `client.object("cart", k)` |

State reads are synchronous and not journaled (the exclusive order and the
journal's commit points make them replay-safe). A call waiting for its key
is held at the key in memory rather than in a durable queue, so it is
durable at its caller (a workflow's outbox, or a client retrying with the
same ID) until it starts; a client's `send` resolves once the call has
started. The engine keeps the lock and the state, deduplicates calls across
continue-as-new and compacts a key's journal.

### Editing state from outside

A key with bad state can be repaired without a handler written for it:

```ts
const cart = engine.objectRef("cart", "user-7");
await cart.setState("note", "gift wrap");
await cart.patchState([{ op: "remove", path: "/items/0" }]);      // JSON Patch, RFC 6902
await cart.clearState("coupon");                                // one field; none named: all of it
const { version } = cart.inspect().object;
await cart.editState({ set: { owner: "ana" } }, { ifVersion: version });
```

```bash
fusee objects set cart user-7 note '"gift wrap"'
fusee objects patch cart user-7 '[{"op": "remove", "path": "/items/0"}]' --dry-run
fusee objects clear cart user-7 coupon
fusee objects edit cart user-7          # $EDITOR on the state; what changed is applied
```

- **An edit is an exclusive call.** Every object type has the handler
  `fusee.state.edit`, which the engine runs: it waits for the key like any
  exclusive call, and its changes commit with its result in the
  `UpdateDone` record a handler's writes make. It replays, deduplicates by
  call ID, is sealed by the codec and compacts as a handler's call does,
  and handlers that run after it see the new state.
- **The edit**, applied in this order: `clearAll`, `clear` (fields;
  absent ones are ignored), `set` (fields and values; `null` removes one),
  `patch` (a JSON Patch over the state as one object). A patch that does
  not apply changes nothing and names the operation. The answer is
  `{version, changed}`.
- **Versions.** A key's version is a hash of its state (`version` in
  `inspect()`'s object detail and in `fusee objects show`). `ifVersion`
  (`--if-version`, HTTP `If-Match`) refuses an edit if the state changed
  since that version was read, so an editor never overwrites a change it
  did not show.
- **HTTP.** `PUT /v1/objects/{type}/{key}/state/{field}` (the value),
  `DELETE /v1/objects/{type}/{key}/state/{field}`, `PATCH
  /v1/objects/{type}/{key}/state` (a JSON Patch), `POST
  /v1/objects/{type}/{key}/state` (an edit), `DELETE
  /v1/objects/{type}/{key}/state` (all of it); a stale `If-Match` answers
  412. The protocol call is `editState`. The UI's Objects screen edits a
  key's state as JSON, shows the fields the edit changes, and saves it with
  the version it was read at.
- **Who asked.** Edits through the HTTP API, a server or the CLI carry the
  requester (`by`, as in [Audit](#audit)) in the edit, which the key's
  `Update` record keeps. A read token cannot edit; a namespace token edits
  only keys whose ID is under its namespace.

The same calls in every SDK: Rust `ObjectRef::edit_state`, `set_state`,
`patch_state`, `clear_state`; Python `edit_state`, `set_state`,
`patch_state`, `clear_state` (`if_version=`); Go `EditState`, `SetState`,
`PatchState`, `ClearState` (`StateEdit{IfVersion}`); Java `editState(edit,
ifVersion)`, `setState`, `patchState`, `clearState`.

## Payload encryption

```ts
const engine = await Engine.open({ dir, codec: aesGcm({ k2: newKey, k1: oldKey }) });
```

Workflow inputs and results, step results, signals, update arguments and
results, child inputs and heartbeat progress are encrypted with AES-256-GCM
inside the application before they become records, so the log, checkpoints,
the run store, replicas and a Fusee server hold only ciphertext. Record
headers (IDs, names, positions), error messages and search attributes stay
readable. Each payload carries its key ID: the first key encrypts and every
listed key decrypts, so keys rotate by adding a new first key. In Rust:
`Config::local(dir).codec(fusee::aes_gcm([("k1", key)]))`; in Python:
`Engine.open(dir, codec=aes_gcm({"k1": key}))`.

The HTTP API and `fusee runs show` show an encrypted payload as
`{"$encoded": "<base64>"}`, and the UI as *encrypted*. To read them in the
UI, run a codec endpoint where the keys are and point the server at it:

```sh
fusee codec-server --keys-file keys.txt --allow-origin https://fusee.example.com
fusee server --dir /var/lib/fusee --http 0.0.0.0:7601 --token-file token --codec-url http://127.0.0.1:7602
```

`keys.txt` holds lines of `<key-id> <64 hex digits>`. The browser sends the
payloads it shows to the endpoint (`POST /decode` with
`{"payloads": [base64, ...]}`, answered with the decoded payloads, `null`
for one it cannot decode), so the keys and the plain payloads never reach
the Fusee server. Only pages from `--allow-origin` may call it, and it
listens on loopback unless it sits behind a proxy that signs people in
(`--insecure`). Any service that answers the same call works as an
endpoint, for keys held in a KMS.

## Audit

Signals, cancellations, terminations and resets record who asked, in the
journal record they write (`by`), and the UI lists them under a run's
Diagnostics. The HTTP API names the requester: the user a proxy in front
signed in (`fusee server --identity-header X-Forwarded-User`), else the
token (`token`, `token:client`, `token:namespace:acme`), else `http`. The
CLI names its user (`cli:ana`) when it talks to a server or a data
directory itself; through an HTTP API the API's name wins. In code:

```ts
await asRequester("job:nightly", () => engine.run("order-1").cancel("expired"));
```

```python
with fusee.as_requester("job:nightly"):
    await engine.run("order-1").cancel("expired")
```

```rust
fusee::audit::as_requester("job:nightly", engine.run::<Order>("order-1").cancel("expired")).await?;
```

Schedule changes are signals and terminations of the schedule's workflow,
so they are recorded the same way. A request that names nobody writes its
record exactly as before. Over the server protocol a client names itself:
trust the name as far as you trust the client's token.

## Replay testing

```bash
fusee export ./data order-123 -o order-123.fjnl
fusee export --sample 1000 ./data -o journals/
fusee show order-123.fjnl
```

```ts
const r = await new Replayer().workflow("checkout", checkout).replayDir("journals");
// [{ workflowId, ok: false, error: "... recorded step 'charge' here but the code called step 'refund'" }]
```

`fusee export` copies the data directory (checkpoints first, so a copy taken
while the application runs is consistent), recovers the copy and writes each
run's journal. `--sample` takes the most recent runs whose whole journal is
available: running and suspended runs, and finished runs of engines with
`keepHistory`. `engine.exportJournal(id)` does the same from a running
engine. A `Replayer` runs new code against exported journals with no step
function or timer running, and reports the first difference by step name.
Run it in CI before a deploy. Encrypted journals need the codec:
`new Replayer({ codec })`.

## Workflow ID and child policies

```ts
await checkout.start("order-123", order, {
  idPolicy: { running: "use-existing", finished: "allow-if-failed" },
});
ctx.spawnChild("audit", `${ctx.workflowId}/audit`, data, { parentClose: "abandon" });
```

- Starting an ID that is running fails by default (`AlreadyExists`). It can
  return the running workflow (`use-existing`) or terminate it and start a
  new run (`terminate-existing`).
- Starting an ID whose last run finished within retention starts a new run
  by default. It can be allowed only if that run failed (`allow-if-failed`),
  refused (`refuse`), or answered with that run (`use-existing`).
- A child's parent close policy is `terminate` (the default), `request-cancel`
  or `abandon`, for children the parent waits for and for spawned ones alike.
  A close that reaches a child's shard before the child's start records the
  child as closed, so the late start does not run it.
- The policies are recorded with the start, so replay and recovery apply
  them the same way.

## Across threads (TypeScript)

One JavaScript thread runs one engine's workflow code. To use more cores in
one process, open the engine with worker threads:

```ts
// main.ts
const engine = await Engine.open({ dir, workers: 4, module: new URL("./workflows.js", import.meta.url) });
const order = engine.workflow("order"); // a handle: the code is in the module
const run = await order.start("order-42", { amount: 42 });
console.log(await run.result());

// workflows.ts: each worker imports it and calls the default export
export default (engine: Engine) => {
  engine.workflow("order", async (ctx, input) => ctx.step("charge", () => charge(input)));
};
```

- Each worker thread owns a set of shards (shard `s` belongs to worker
  `s % workers`), holds its own copy of the workflow code, and runs those
  shards' workflows. All workers append to the data directory's one shared
  log, so their steps share flushes as shards on one thread do.
- The thread that opened the engine holds no shards: it routes `start`,
  `signal`, `update`, `query`, `cancel`, `terminate`, `reset`, `result`,
  `complete` and `fail` to the worker that owns the workflow ID's shard,
  and asks every worker for `list`, `inspect`, `exportJournal` and
  `shardMetrics` (these block until each worker answers). A limit set with
  `engine.limit` holds across all workers.
- Children, signals and cancellations between workflows on different
  workers go through the same durable outbox as between shards, delivered to
  the owning worker's inbox. Code inside a worker may call its own engine
  for any ID: a call for an ID another worker owns goes there through the
  main thread. The synchronous reads are the exception: in a worker, `list`
  and `shardMetrics` cover that worker's shards, and `inspect` and
  `exportJournal` refuse another worker's IDs.
- A new directory gets one shard per worker unless `shards` is given; the
  count is then fixed, and there are at most as many workers as shards. A
  directory opens with any worker count, or on one thread with `shards` set
  to its count: after a crash every shard recovers in whichever worker (or
  thread) now owns it.
- `engine.close()` closes every worker's shards (with the usual checkpoint)
  and stops the threads. Query arguments and answers cross threads with the
  structured clone algorithm; other values cross as JSON or bytes, as they
  are recorded. `telemetry` is not available with workers; local and memory
  durability only. Node.js and Bun.

## Across processes

In server mode workflow code runs in the worker that holds the server's
lease. Other processes connect with `fusee_server::Client` and send
`complete`, `fail`, `update`, `query`, `signal`, `start`, `result`, `cancel`
and `terminate`; the server relays each call to the worker, which runs it
against its engine (`fusee_server::serve_calls`). Listings fan out across the
engine's shards. On a failover (server standby or replicated backup) runs
resume from the journal: schedules keep firing, accepted updates whose
handlers had not finished run again from their records (a retried call with
the same update ID gets the result), and persisted limit definitions continue to apply while prior permits expire.
