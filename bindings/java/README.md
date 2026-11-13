# Fusee for Java

Durable workflows in Java 17 and later, on the same engine as the Rust,
TypeScript, Python and Go SDKs. Workflow code is plain Java: each
`ctx.step`, sleep, wait or child is a journal operation recorded in the
engine's log and replayed after a crash.

```java
try (Engine e = Engine.open(Options.local("./data"))) {
    Workflow<Order, String> checkout = e.register("checkout", Order.class, String.class, (ctx, o) -> {
        Quote quote = ctx.step("quote", Quote.class, () -> pricing.quote(o));
        Charge charge = ctx.step("charge", Charge.class, () -> payments.charge(quote, ctx.key()), // idempotency key
                StepOptions.defaults().retry(RetryPolicy.attempts(5)));
        ctx.sleep("cool-off", Duration.ofMinutes(10));
        Optional<Boolean> approved = ctx.waitFor("review", Boolean.class, Duration.ofHours(24));
        if (approved.orElse(false)) return charge.id();
        return ctx.step("refund", String.class, () -> payments.refund(charge));
    });

    Run<String> run = checkout.start("order-123", order);
    run.signal("review", true);
    String id = run.result();
}
```

## Building

The SDK is a Maven project (`dev.fusee:fusee`, no runtime dependencies)
over `libfusee_jni`, a small JNI layer (`native/fusee_jni.c`) statically
linked with `libfusee_c`, the engine's C interface (`bindings/c`, header
`bindings/c/include/fusee.h`). Build the C interface first, from the
repository root, then the SDK:

```bash
cargo build --release -p fusee-c
cd bindings/java && mvn test
```

`mvn` runs `native/build.sh` before compiling: it compiles the JNI layer
with the system C compiler against the JDK's `jni.h` (and checks every C
signature against the header `javac -h` generates from `Native.java`), links
`../../target/release/libfusee_c.a`, and puts the one shared library at
`target/classes/dev/fusee/native/<platform>/libfusee_jni.<dylib|so>`, so it
ends up in the jar. `<platform>` is `darwin-arm64`, `darwin-x86_64`,
`linux-x86_64` or `linux-aarch64`. At run time the loader takes, in order,
the file named by the system property `fusee.library`, the library for this
platform on the class path (copied to a temporary file when it is inside a
jar), or `fusee_jni` from `java.library.path`.

A jar for several platforms: build the library on each (`cargo build
--release -p fusee-c && native/build.sh out/`, or set `FUSEE_LIB_DIR` and
`FUSEE_PLATFORM` for a cross-compiled `libfusee_c.a`), copy each
`out/dev/fusee/native/<platform>` directory into `target/classes/dev/fusee/native/`,
and package with `mvn package -Dfusee.native.skip=true`. CI builds and tests
`linux-x86_64`, `linux-aarch64` and `darwin-arm64`. Building needs a JDK
(17 or later) and a C compiler; running needs a JRE only.

## JNI, not the Foreign Function & Memory API

The FFM API is final in Java 22; in Java 21 it is a preview that needs
`--enable-preview` at compile and run time and ties the class files to
exactly that release, and Java 17 has only an incubator version with a
different API. JNI runs on every LTS release without flags. Measured on the
same functions in the same library (Java 21, macOS arm64, 2 M calls, median
of 5 rounds; `bench/FfmBench.java`):

| | JNI | FFM (21, preview) |
| --- | --- | --- |
| `fusee_abi_version()` | 4–20 ns | 9–30 ns (12–15 ns with `isTrivial`) |
| `fusee_engine_code_state(e, u32, u64, *u32)` | 7–19 ns | 17–42 ns |
| `fusee_engine_limit_usage(e, str, *u32, *u64)`, a `byte[]` and a `long[]` | 334–558 ns | 54–117 ns |

A call with primitive arguments costs less through JNI than through FFM.
What costs is a JNI function the native side calls back into the JVM: each
array access (`GetByteArrayRegion`, `SetLongArrayRegion`) costs about 70 ns
on macOS arm64, where HotSpot switches the thread's W^X mode on every entry
into the VM. So the calls on the step path (`begin_step`, `step_done`,
`host`, `quiescent`, `tick`, `finish`) make none: Java writes their byte
arguments into a direct `ByteBuffer` the native side registered once, and
they leave numbers and bytes in a second one. The engine's commit positions
and its owed-tick flag are read from direct buffers over `fusee_io`, without
a call. Calls off the step path (start, signal, inspect, list) pass arrays.

## How it runs

The engine is embedded, as in the other SDKs: Java runs and replays
workflow code, and the engine records the journal, decides when a step's
inputs are durable, and hands Java events and deliveries through the C
interface. Every durability mode goes through it: `memory`, `local`, and
`server` and `replicated`, where the native core speaks the log service
protocol to `fusee server` or to the other replicas itself.

An engine has one loop thread. Workflow code runs on threads the engine
schedules one at a time, Go's model: the thread holding the engine hands it
to a run's thread and waits until that one blocks on a journal operation or
returns, and between two deliveries a run's code runs until all its threads
are blocked, exactly as on replay. On Java 21 and later the loop and
workflow code are virtual threads (a handoff is an unpark and a park on the
same carrier, and a waiting run costs no OS thread); on Java 17 they are
platform threads. Two shortcuts keep a run on its own thread: a workflow
thread about to block takes the run's next delivery itself when every other
thread of the run is blocked (what the loop would do next), so a chain of
inline steps makes no handoff at all; and one with nothing to deliver runs
the loop's turns itself until its run has work, so a step's result reaches
it with one wake.

**Step functions run on threads of their own**, from a cached pool of
platform threads shared by every engine, whose most recently idle worker
spins for 50 µs before it parks (and the thread waiting for the engine's
next event spins for 30 µs), so a step reaches a running thread without a
wake. A step function may block; it is interrupted when its attempt times
out or the engine closes. For many concurrent steps that block on I/O, pass
`Options.stepExecutor(Executors.newVirtualThreadPerTaskExecutor())`. Two
consequences of their own threads:

- A step function must not read or write workflow state that workflow code
  changes meanwhile: give it what it needs, and return its result.
  `ctx.key()`, `ctx.heartbeat(v)`, `ctx.progress(type)` and
  `ctx.stream(name)` work inside it, on the step's thread.
- Parallel steps record in the order they finish. Replay follows the
  journal, so this is only visible in the journal itself.

`StepOptions.defaults().inline()` runs the function on the workflow's
thread while it holds the engine instead: no handoff, the order it starts
in, and access to workflow state, but nothing else on the engine runs until
it returns and a timeout cannot cut it short. Use it for fast, CPU-only
functions.

The system properties `fusee.threads` (`virtual` or `platform`),
`fusee.steps=virtual`, `fusee.spin.us`, `fusee.wait.spin.us` and
`fusee.pool.spin.us` change these choices (0 turns a spin off).

## Streams

A run appends chunks (JSON, or `byte[]` as bytes) to named streams, and
readers follow them from an offset ([docs/features.md](../../docs/features.md#streams)):

```java
String answer = ctx.step("generate", String.class, () -> {
    StreamWriter out = ctx.stream("tokens");                // the step's stream: its result completes it
    String text = String.join("", out.recorded(String.class)); // what an earlier attempt recorded
    for (String token : model.stream(prompt, text)) {       // continue after the prefix
        out.append(token);
        text += token;
    }
    return text;
});

// Anywhere outside workflow code: this process, or a Client through a server.
StreamReader tokens = engine.run(id).stream("tokens", lastSeen);
for (Chunk c : tokens) response.write(c.as(String.class)); // c.bytes() for a chunk of bytes
// tokens.offset(): where to reconnect; tokens.status(): how the stream ended
```

- **Appending never waits.** From a step's thread `append` queues the
  chunk for the engine's loop and returns; the loop records every queued
  chunk in its next turn, in order, before the step's result. A failure
  (say, the stream belongs to another step) is thrown by the next
  `append` or `close`. An attempt that timed out cannot append once its
  retry started. In workflow code and inline steps an append is recorded
  at once.
- **Readers see only durable chunks**, so nothing a reader saw is taken
  back by a crash. `hasNext()` waits (a long poll of 30 s per request,
  `waitFor(d)`) and returns false once the stream is complete.
- **A step that runs again keeps the recorded prefix**: `recordedCount()`
  and `recorded(T.class)` give it what earlier attempts recorded (after a
  crash, what was durable), and `closed()` is true when an earlier attempt
  completed the stream (appends are then dropped).
- **Workflow code** opens the run's stream with `ctx.stream(name)`: its
  appends are recorded once (a replay appends nothing), and `close()` or
  the run's end completes it. Step functions may append to it as well: the
  writer finds the step from the calling thread.
- In server mode the engine answers the `streamRead` calls clients send
  through the server, whatever their language.

## Objects

An object type is a set of handlers; each key is one object with its own
JSON state, and calls to a key's exclusive handlers take turns
([docs/features.md](../../docs/features.md#objects-keyed-state)):

```java
ObjectType cart = engine.registerObject("cart", Map.of(
        "add", ObjectHandler.exclusive(String.class, (c, item) -> {
            int price = c.ctx().step("price", Integer.class, () -> pricing.quote(item));
            List<String> items = c.state().get("items", new TypeRef<List<String>>() {});
            List<String> next = items == null ? new ArrayList<>() : new ArrayList<>(items);
            next.add(item + "@" + price);
            c.state().set("items", next); // committed with the result
            return next.size();
        }),
        "get", ObjectHandler.shared(Object.class, (c, ignored) -> c.state().raw("items"))));

// From outside: an ID makes a retried call run once.
int n = cart.key("user-7").call("add", "tea", Integer.class, CallOptions.id("req-42"));
// From workflow code: a journal operation, run exactly once.
int m = ctx.object("cart", "user-7").call("add", "tea", Integer.class);
```

`ref.state()` reads the committed state, `ref.inspect().object()` adds the
running and waiting calls, `engine.objects("cart", 0)` lists the keys, and
`client.object("cart", k)` calls a key through a server.

## Deterministic replay in Java

Workflow code (the function given to `register`, functions started with
`ctx.async`, update handlers) must make the same journal operations in the
same order on every run:

- Wait, branch on time or randomness, and run code concurrently only
  through the `Ctx`: `step`, `sleep`, `waitFor`, `child`, `all`, `race`,
  `async`, `now`, `random`, `uuid`.
- Start no threads or executors, and use no `CompletableFuture`, locks,
  `wait`/`notify`, `Thread.sleep`, timers, `System.currentTimeMillis`,
  `Instant.now`, `Math.random`, `Random` or `UUID.randomUUID` in workflow
  code. Step functions may do anything.
- `HashMap` and `HashSet` iteration order is not part of a program's
  meaning: do not let it decide which journal operations run, or in which
  order (use `LinkedHashMap`, or sort).
- A `Ctx` and the `Op`s it made belong to one run. A `Ctx` used outside its
  run's workflow threads throws `IllegalStateException`.
- Do not call `Engine` or `Run` methods from workflow code or inline steps
  (they wait for the engine, which the caller holds; they throw
  `IllegalStateException`): use the `Ctx` (`ctx.signal`, `ctx.child`,
  `ctx.cancelWorkflow`).
- Do not catch `Throwable` or `Error` around `Ctx` calls (a run that ends
  unwinds its threads with an `Error`), and do not swallow a
  `DeterminismException`: the engine has stopped the run.

Code that changed under a recorded run stops it with a
`DeterminismException` (status `stuck`) naming the step the journal has and
the one the code asked for; deploy code that matches, or use
`ctx.patched(id)` for new branches.

## Payloads

Payloads are JSON. The built-in codec (`JsonCodec.builtIn()`, no
dependencies) handles strings, numbers, booleans, enums (by name), records,
classes with public fields (or getters and setters) and a public no-argument
constructor, `List`, `Set`, `Map` with string keys, arrays, `Optional`,
`UUID` and `java.time` values (as ISO text), and `Json` (a JSON value kept as
text). Generic types take a `TypeRef`: `new TypeRef<List<Item>>() {}`. A
`byte[]` payload is recorded as raw bytes, as `[]byte`, `bytes` and
`Uint8Array` are in Go, Python and TypeScript, so the SDKs read each other's
payloads. To use Jackson, implement `JsonCodec` in two lines (the interface's
Javadoc has them) and pass it with `Options.json(codec)`.

## API

| | |
| --- | --- |
| Engine | `Engine.open(Options.local(dir) / memory() / server(address) / replicated(id, peers, dir)` with `.shards`, `.retention`, `.parkAfter`, `.checkpointInterval`, `.flush`, `.segmentBytes`, `.keepHistory`, `.archiveHistory`, `.codecKey(id, key)`, `.inlineFlush`, `.onExhausted`, `.json`, `.stepExecutor)`, `e.close()` |
| Workflows | `WorkflowDef.workflow(name, In.class, Out.class, (ctx, in) -> ...)` (a definition apart from any engine: `Options.workflows(defs...)`, `def.register(e)`, `def.on(e)`, `.version(n)`), `e.register(name, [version,] In.class, Out.class, (ctx, in) -> ...)` (or `TypeRef`s), `e.workflow(name, In.class, Out.class)`; the engine and a `Client` share `start(type, id, input[, opts])`, `signalWithStart(type, id, input, signal, value[, opts])`, `updateWithStart(type, id, input, update, arg[, opts])`, `debounce`, `batch`, `run(id)`, `list`; `wf.start(id, in, new StartOptions().timeout(d).ifRunning(..).ifFinished(..).version(n))`, `wf.updateWithStart(id, in, update, arg, R.class)` (`.run()`, `.result()`), `wf.debounce(key, in, DebounceOptions.window(d).maxWait(d).id(..))`, `wf.batch(key, event, new BatchOptions().maxEvents(n).maxWait(d).id(..))` |
| Steps | `ctx.step(name, T.class, () -> ..., StepOptions.defaults().retry(RetryPolicy.attempts(n).initial(d).max(d).fixed().nonRetryable("CardDeclined").retryIf((failure, attempt) -> ...).onExhausted(Exhausted.PAUSE)).timeout(d).heartbeatTimeout(d).limit(name).priority(p).fairness(key, weight).readOnly().inline())`, `ctx.startStep` (an `Op`); a function that takes one gets its step context: `ctx.step(name, T.class, s -> ...)` with `s.key()`, `s.attempt()`, `s.name()`, `s.workflowId()`, `s.heartbeat(v)`; inside a plain one: `ctx.key()`, `ctx.heartbeat(v)`, `ctx.progress(T.class)` |
| Waiting | `ctx.sleep(name, d)`, `ctx.startSleep`, `ctx.waitFor(signal, T.class)`, `ctx.waitFor(signal, T.class, timeout)` (an `Optional`, empty on timeout), `ctx.startWait` |
| Children and signals | `ctx.child(type, id, input, O.class, new ChildOptions().parentClose("abandon").timeout(d))`, `ctx.startChild`, `ctx.spawnChild`, `ctx.signal(target, name, v)`, `ctx.cancelWorkflow(target, reason)` |
| Concurrency | `ctx.async(() -> ...)`, `ctx.all(ops...)`, `ctx.race(ops...)` (the index; losers dropped); `op.get()`, `op.isDone()`, `op.timedOut()` |
| Queries and updates | `ctx.onQuery(name, A.class, a -> ...)`, `ctx.onQuery(name, () -> ...)`, `ctx.onUpdate(name, A.class, a -> ..., a -> { if (bad) throw ...; })`; `run.query(name, arg, R.class)`, `run.update(name, arg, R.class, UpdateOptions.id(..))` |
| Durable values | `ctx.now()` (`Instant`), `ctx.random()`, `ctx.uuid()` (`UUID`), `ctx.journalSize()` |
| Versions and attributes | `ctx.patched(id)`, `ctx.deprecatePatch(id)`, `ctx.setAttributes(map)`, `return ctx.continueAsNew(input[, version])` |
| Completion tokens | `ctx.awaitCompletion(name, T.class, token -> ...)`; `e.complete(token, v)`, `e.fail(token, message)` |
| Streams | `ctx.stream(name)` (workflow code, or a step function on its thread): `append(v)`, `close()`, `recorded(T.class)`, `recordedCount()`, `closed()`; `run.stream(name[, from])`, a `StreamReader` (`Iterator<Chunk>` and `Iterable`): `offset()`, `status()`, `end()`, `runId()`, `waitFor(d)`, `ofRun(run)`; a `Chunk` has `offset()`, `json()` or `bytes()`, `value()`, `as(T.class)` |
| Runs | `e.run(id[, O.class])`: `signal`, `result([timeout])`, `started([timeout])`, `update`, `query`, `cancel`, `terminate`, `reset(beforeStep[, new ResetOptions().dropSignals().version(n)])`, `resume()` (a run paused when a step's retries ran out), `inspect()` (a `RunDetail`: summary and journal), `stream(name[, from])` |
| Engine | `e.on(event, (workflowId, message) -> ...)` (`"stuck"`, `"paused"`), `e.list(ListOptions.where("status", "ok").and("amount", Map.of("gte", 100)).limit(n))`, `e.limit(name, LimitOptions.concurrent(n).withPerSecond(r))`, `e.limitInUse`, `e.limitWaiting`, `e.schedule(name, workflow, input, ScheduleOptions.cron(expr) / every(d)...)`, `e.scheduleHandle(name)` (`pause`, `resume`, `trigger`, `backfill`, `describe`, `delete`), `e.exportJournal(id)`, `e.shardMetrics()`, `e.stats()`, `e.shards()` |
| Objects | `engine.registerObject(type, Map.of(name, ObjectHandler.exclusive(Arg.class, fn), ...))` (and `shared`); in a handler `c.state()` (`get`, `set`, `clear`, `clearAll`, `keys`, `raw`), `c.key()`, `c.ctx()`; `ctx.object(type, key)` (`call`, `startCall`, `send`); `engine.object(type, key)` (`call`, `send`, `state`, `inspect`, `delete`, with `CallOptions.id`; from outside, `setState`, `patchState`, `clearState` and `editState(edit, ifVersion)`), `engine.objects(type, n)` |
| Remote | `Client.connect(address)`: `call`, `start`, `updateWithStart`, `debounce`, `batch`, `list`, `complete`, `fail`, `object(type, key)`, `run(id)` (`result`, `started`, `signal`, `query`, `update`, `cancel`, `terminate`, `reset`, `resume`, `stream`) |
| Invoked workers | `new InvocationHandler(address, secret, options, register)`: `httpHandler()` for the JDK's `HttpServer`, `invoke(signature, body)` for any framework; `InvocationHandler.verifySignature` |
| Errors | Failures travel as the [envelope](../../docs/errors.md#the-failure-envelope): throw a `FailureException` (`.code(..)`, `.details(..)`, `.type(..)`, `.retryable(false)`; subclass it to name the type) or a `FatalException` (never retried); any other exception records its message, class name and cause. `Failure` (`message()`, `type()`, `code()`, `details()`, `retryable()`, `cause()`, `isFinal()`, `Failure.of(t)`). Raised: `StepFailedException` (`step()`, `detail()`, `failure()`), `WorkflowFailedException` (`code()` `WorkflowFailed` or `ChildFailed`, `failure()`, `workflowId()`), `FuseeException` (`code()`: `NotFound`, `Closed`, `Invalid`, ...), `CancelledException` (throw one to finish cancelled), `TimedOutException`, `TerminatedException`, `DeterminismException`, `RejectedException` (`reason()`) |

Calls that wait block the calling thread; an interrupt ends the wait with a
`FuseeException` of code `Interrupted` (a start or signal already sent still
happens). In server mode (`Options.server("host:port")`) the engine answers
calls that clients send through the server, whatever their language.

## Not supported

- Worker parts: one engine's workflow code runs one thread at a time (the
  step functions run in parallel). TypeScript's `workers` option has no Java
  counterpart; run several engines on separate directories instead.
- A `Replayer` for exported journals, and OpenTelemetry instruments.
- Replica metrics while a replicated member waits to lead.
- Published jars for Linux and macOS together: CI builds each platform's
  library; assembling one jar from them is the manual step above.
- On Java 17 (platform threads), many concurrent runs cost an OS thread
  wake per resumed run: about 5,000 steps/s with 1,000 concurrent
  workflows on the machine below, against 100,000–135,000 on Java 21 (the
  sequential chain is as fast: 2.7 µs, inline 0.8 µs). Measured on Java 21
  with `-Dfusee.threads=platform`, the path Java 17 takes; CI runs the tests
  on Java 17 itself.

## Tests and conformance

```bash
mvn test                       # server tests start ../../target/release/fusee (-Dfusee.cli=...)
java -cp target/classes:target/test-classes dev.fusee.cmd.Conformance > java.json
java -cp target/classes:target/test-classes dev.fusee.cmd.Bench memory
```

`mvn test` covers steps, retries, sleeps, signals, children, races,
`ctx.async` and `all`, updates and queries, update-with-start, cancel,
terminate and start policies, workflow timeouts, continue-as-new, patches,
attributes and listing, limits, step timeouts and heartbeats, completion
tokens, schedules, debounce and batch, export, raw bytes, payloads larger
than the buffers the step path shares with the native side, inline steps
and keys, the JSON codec, a clean close and reopen, a process killed with
SIGKILL mid-run and reopened, a determinism error after a code change,
payload encryption with key rotation (no plaintext in the data directory),
and server mode against `fusee server` with a client. `StreamsTest` covers
chunk order and completeness while a step streams, reconnecting from an
offset, a retry continuing after the recorded prefix, workflow appends
(one from a step's thread) recorded once across a reopen and replay, a
process killed mid-stream with a resuming reader that sees no chunk twice,
an attempt that timed out, inline steps, and readers through a server (the
Java worker answering `streamRead`); `FUSEE_STREAM_COST=1 mvn test
-Dtest=StreamsTest#cost` prints append cost and reader latency (see
[docs/features.md](../../docs/features.md#streams)). The conformance runner
prints the 40 scenarios' journals, streams included, for
`conformance/compare.py`, and `conformance/remote.py` runs a Java worker
and a Java client against the other languages' through a server, a stream
read among them.

## Cost per step

A sequential chain of no-op steps in memory mode (`dev.fusee.cmd.Bench`),
against the other SDKs' benchmarks on the same machine (an 18-core Apple
M-series laptop), p50 step to step, median of
three interleaved runs, then 1,000 concurrent 20-step workflows (Node:
2,000). Calls per step for Node and Python are from the Go SDK's
measurement.

| | p50 | inline steps | calls into the engine per step | steps/s, concurrent |
| --- | --- | --- | --- | --- |
| Java 21 | 2.8 µs | 1.2 µs | 7.7 (1.7 ticks) | 135,000 |
| Java 21, one carrier | 2.8 µs | 1.1 µs | 7.6 (1.6 ticks) | 271,000 |
| Go | 4.8 µs | 1.1 µs | 7.0 (2.0 ticks) | 436,000 |
| Node | 2.2 µs | | 5.0 | 264,000 |
| Python | 4.8 µs | | 3.1 | 114,000 |

Java's default path costs 0.58 of Go's, 1.3 times Node's and 0.58 of
Python's; inline steps cost 1.1 times Go's. Concurrent throughput is 0.31
of Go's, 0.51 of Node's and 1.2 times Python's: with many runs, each
resumed run is a virtual-thread switch the engine waits for, and with idle
carriers the scheduler wakes one on each switch (2 µs a handoff, against
0.2 µs on one carrier). A JVM that runs mostly workflows can pass
`-Djdk.virtualThreadScheduler.parallelism=1` (the "one carrier" row; the
engine runs one workflow thread at a time anyway, and step functions run on
the pool's platform threads), but that setting applies to every virtual
thread in the JVM. With step functions on a virtual thread each
(`-Dfusee.steps=virtual`) the chain costs 11–13 µs and the concurrent case
120,000–140,000 steps/s. In local mode with the default flush (`F_FULLFSYNC`
on macOS) the chain is disk-bound: 5.1 ms against Go's 4.8 ms, and 1,460
steps/s concurrent against Go's 1,350.
