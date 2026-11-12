# fusee (Node.js and Bun)

Durable execution at the speed of the disk, embedded in your process.

```ts
import { Engine } from "fusee";

const engine = await Engine.open({ dir: "./data" });
const wf = engine.workflow("greet", async (ctx, name: string) => {
  const greeting = await ctx.step("compose", () => `hello ${name}`);
  await ctx.sleep("pause", "1s");
  return greeting;
});
console.log(await (await wf.start("greet-1", "world")).result());
await engine.close();
```

Workflow code must be deterministic between `ctx` calls: use `ctx.now()`,
`ctx.random()` and `ctx.uuid()`, and await only `ctx` operations (or
`ctx.all` / `ctx.race` of them). Step functions may do anything.

API:

- `ctx`: `step(name, fn, { retry, timeout, heartbeatTimeout, limit, priority, fairnessKey, fairnessWeight, readOnly })`,
  `heartbeat(progress)`, `progress()`, `awaitCompletion(name, send)`,
  `sleep(name, duration)`, `waitFor(name, { timeout })`,
  `child(type, id, input, { parentClose })`, `spawnChild`,
  `signal(target, name, value)`, `cancelWorkflow(target)`,
  `onQuery(name, fn)`, `onUpdate(name, { validate, handle })`,
  `continueAsNew(input)`, `journalSize()`, `patched(id)`,
  `deprecatePatch(id)`, `setAttributes(obj)`, `now/random/uuid`, `key()`,
  `all`, `race`.
- `run`: `signal`, `result`, `started()`, `update(name, arg, { id })`,
  `query(name, arg)`, `cancel`, `terminate`, `reset({ beforeStep })`,
  `resume()` (a run paused by a step out of retries, see below), `inspect`.
- Step options: `retry: { attempts, initial, max, maxElapsed,
  nonRetryable, retryIf, onExhausted }`: `onExhausted: "pause"` pauses the
  run when the retries run out, until `run.resume()`;
  `Engine.open({ onExhausted: "pause" })` makes it every step's default
  ([Pausing](../../docs/features.md#pausing-when-retries-run-out)).
- `workflow`: `start(id, input, { idPolicy, timeout })`,
  `updateWithStart(id, input, name, arg)`,
  `debounce(key, input, { window, maxWait, dedupWindow, id })`,
  `batch(key, event, { maxEvents, maxWait, dedupWindow, id })`.
- `engine`: `complete(token, value)`, `fail(token, error)`,
  `schedule(name, { cron | every, timezone, overlap }, workflow, input)`,
  `list({ where })`, `limit(name, { concurrent, perSecond })`,
  `limitInUse(name)`, `limitWaiting(name)`, `exportJournal(id)`;
  `Engine.open({ codec: aesGcm(keys) })`;
  `new Replayer().workflow(name, fn).replayFile(path)`.
- Queues: steps waiting for a limit's permit go by `priority` (higher
  first), then take turns by `fairnessKey` (`fairnessWeight` permits per
  turn); the order is not journaled. `debounce` collapses starts with one
  key inside a window into one run with the latest input; `batch` starts
  one run with every event once `maxEvents` arrived or `maxWait` passed.
  Both resolve once the event is durable with the run that will take it
  (`<key>@<uuid>`); `run.started()` waits until it exists. See
  [Debounce](../../docs/features.md#debounce) and
  [Batching](../../docs/features.md#batching).

```ts
engine.limit("search", { concurrent: 4 });
const reindex = engine.workflow("reindex", async (ctx, doc: Doc) =>
  ctx.step("index", () => search.index(doc), { limit: "search", priority: doc.urgent ? 10 : 0, fairnessKey: doc.tenant }));
const run = await reindex.debounce(doc.id, doc, { window: "5s", maxWait: "1m", id: eventId });
await run.started();
```

- Streams: `ctx.stream(name)` returns a writer; `append(value)` (a
  `Uint8Array` is bytes) never waits for the disk, `close()` completes the
  stream, and inside a step `recorded()` returns what the step's earlier
  attempts recorded, so an attempt that runs again continues after it (the
  step's result completes its stream). `run.stream(name, { from })` and
  `client.run(id).stream(name, { from })` are async iterables of the
  durable chunks; `reader.offset` is where to reconnect, `reader.status`
  how it ended. See [Streams](../../docs/features.md#streams).

```ts
const answer = await ctx.step("generate", async () => {
  const out = ctx.stream("tokens");
  let text = out.recorded().join("");
  for await (const t of model.stream(prompt, { prefill: text })) { out.append(t); text += t; }
  return text;
});
for await (const t of engine.run(id).stream("tokens", { from: 0 })) process.stdout.write(t);
```

- Objects (keyed state): `engine.object(type, { handler, other: shared(fn) })`
  registers handlers by name, exclusive (one call at a time per key, in
  arrival order) unless marked `shared` (they run at once and only read).
  A handler is workflow code with `ctx.state` (`get`, `set`, `clear`,
  `clearAll`, `keys`; JSON values, committed when an exclusive handler
  returns) and `ctx.objectKey`. `engine.objectRef(type, key)` has
  `call(handler, arg, { id })` (calls with one ID run once), `send(...)`
  (resolves once accepted), `state()`, `inspect()` and `delete()`, and
  edits the state from outside: `setState(field, value)`,
  `patchState(ops)` (JSON Patch), `clearState(field?)`,
  `editState(edit, { ifVersion })`
  ([Editing state](../../docs/features.md#editing-state-from-outside));
  `engine.objects(type)` lists keys. From workflow code,
  `ctx.object(type, key).call(...)` and `.send(...)` are journal operations
  that run exactly once. With `workers`, register objects in the workers'
  module; calls go to the worker that owns the key. Through a server:
  `client.objectRef(type, key)`. See
  [Objects](../../docs/features.md#objects-keyed-state).

```ts
engine.object("cart", {
  add: async (ctx, item) => {
    const items = ctx.state.get("items") ?? [];
    items.push(await ctx.step("price", () => pricing.quote(item)));
    ctx.state.set("items", items);
    return items.length;
  },
  get: shared((ctx) => ctx.state.get("items") ?? []),
});
const n = await engine.objectRef("cart", userId).call("add", item, { id: requestId });
```

- `Engine.open` options beyond storage: `telemetry: openTelemetry(api)`
  (a `fusee.workflow` span per run, in the trace of the request that
  started it, with a `fusee.step` span per attempt below it, and step and
  workflow metrics; see [traces](../../docs/operations.md#traces)),
  `inlineFlush: false` (every flush on the log thread; slower for a lone
  sequential workflow on a fast disk, so off by default), `driver`
  (`"native"`, the default on Node: the engine's turns run back to back in
  one event-loop callback, each drained of microtasks as a macrotask would
  be; `"macrotask"`, always on Bun, or `FUSEE_DRIVER=macrotask`: one
  event-loop callback per turn), and `workers: n, module` (workflow
  code on n worker threads, each owning a set of shards and loading
  `module` to register its workflows; this thread routes calls by workflow
  ID; see [Across threads](../../docs/features.md#across-threads-typescript)).
- The pipelined flush (the default in local mode with the shared log, on
  hosts with more than two cores): each record goes to the log's threads as
  soon as a delivery appends it, and the next step's gate spins for it
  while a core is spare (shards and host threads that worked in the last
  2 ms fit in the host's cores), instead of flushing inline at the gate.
  Faster for a lone chain on Linux (22 against 33 µs a step on an
  i8g.2xlarge). `FUSEE_SDK_FLUSH=inline` flushes inline at the gate
  instead, which is faster for a fan-out.
- Payloads are JSON, except `Uint8Array` inputs, step results, signals and
  results, which are recorded as raw bytes and come back as bytes after a
  restart.

See [docs/features.md](../../docs/features.md).

On an AWS i8g.2xlarge (instance-store NVMe, a 12 µs flush), a step takes
19.1 µs from one step to the next with eight workflows live
([benchmark](../../docs/benchmark.md#sdks)).

Build from source: `cargo build --release -p fusee-node && node scripts/copy-native.mjs && npx tsc`.

## Agents

`fusee/ai` makes a model call a step whose tokens stream to readers
(`modelCall`). `fusee/ai/vercel` makes an agent written with the Vercel AI
SDK durable without changing its loop: `durableModel(ctx, model)` makes
each model call a step (`streamText` too: its text is the stream
`model#<n>`), and `durableTools(ctx, tools, { approve })` makes each tool
call a step, with tools that wait for a person's approval first.

```ts
import { generateText } from "ai";
import { durableModel, durableTools } from "fusee/ai/vercel";

const { text } = await generateText({
  model: durableModel(ctx, openai("gpt-5")),
  tools: durableTools(ctx, { search }),
  maxRetries: 0,
  prompt,
});
```

See [Agents](../../docs/guides/agents.mdx) and [Agent
patterns](../../docs/guides/agent-patterns.md).

## Server and replicated modes

A worker can acquire a server lease over TCP or a Unix socket. A separate
application uses `Client` to start, signal, query, update or complete work.

```ts
const engine = await Engine.open({ durability: "server", address: "127.0.0.1:7600" });
const client = await Client.connect("127.0.0.1:7600");
const run = await client.start("checkout", "order-42", { amount: 42 });
await run.signal("approved", true);
const batched = await client.batch("ingest", "tenant-7", event, { maxEvents: 100, maxWait: "2s" });
await batched.started();
client.close();
```

`client.debounce(type, key, input, opts)` and `client.batch(type, key,
event, opts)` take the options of `workflow.debounce` and `workflow.batch`.
Register workflows on the worker as in local mode. For replicated operation
start a worker process on each of three or five machines, each with its own
data directory and member index. Pass `durability: "replicated"`, `replicaId`,
`peers` and `dir` to `Engine.open`. Each process serves replication traffic while
the call waits for its member to become primary. Close the engine before
opening it again after loss of the primary role. Replicas require a trusted
network. The protocol has no peer authentication.

## Invoked workers (scale to zero)

A server started with `--invoke URL` POSTs to that URL when a shard that no
worker holds has work due (a start, a signal, a timer, a child's result).
`invocationHandler` answers: each request opens a fresh engine for the
invocation's shards, runs them until they are idle or the budget ends, closes
it and returns. Nothing stays up while workflows sleep.

```ts
import { invocationHandler } from "fusee";

const handler = invocationHandler({
  address: "fusee.internal:7600?ca=/etc/fusee/ca.pem&token-file=/etc/fusee/token",
  secret: process.env.FUSEE_INVOKE_SECRET, // the server's --invoke-secret-file
  register: (engine) => {
    engine.workflow("order", order);
  },
});

http.createServer(handler.node).listen(8080);    // node:http
app.post("/fusee", handler.node);                 // Express, before any body parser (or after express.raw())
export const lambda = handler.lambda;             // AWS Lambda (function URL, API Gateway, ALB)
export default { fetch: handler.fetch };          // Bun.serve, Deno, Vercel, Cloudflare
```

`handler.invoke({ headers, body })` serves any other framework and resolves
to `{ statusCode, headers, body }`. A request without a valid signature gets
401 before anything connects. Other `OpenOptions` (`codec`, `telemetry`,
`retention`...) pass through to each engine. Semantics, the time budget and
the server's options are in [server mode](../../docs/server-mode.md#invoked-workers).
