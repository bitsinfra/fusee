# Fusee for Go

Durable workflows in Go, on the same engine as the Rust, TypeScript and
Python SDKs. Workflow code is plain Go: each `fusee.Step`, sleep, wait or
child is a journal operation recorded in the engine's log and replayed
after a crash.

```go
e, err := fusee.Open(fusee.Options{Dir: "./data"})
defer e.Close()

checkout := fusee.Register(e, "checkout", func(c *fusee.Ctx, o Order) (string, error) {
	quote, err := fusee.Step(c, "quote", func(ctx context.Context) (Quote, error) {
		return pricing.Quote(ctx, o)
	})
	if err != nil {
		return "", err
	}
	charge, err := fusee.Step(c, "charge", func(ctx context.Context) (Charge, error) {
		return payments.Charge(ctx, quote, fusee.Key(ctx)) // idempotency key
	}, fusee.StepOptions{Retry: &fusee.RetryPolicy{Attempts: 5}})
	if err != nil {
		return "", err
	}
	if err := c.Sleep("cool-off", 10*time.Minute); err != nil {
		return "", err
	}
	approved, ok, err := fusee.WaitFor[bool](c, "review", 24*time.Hour)
	if err != nil {
		return "", err
	}
	if ok && approved {
		return charge.ID, nil
	}
	return fusee.Step(c, "refund", func(ctx context.Context) (string, error) {
		return payments.Refund(ctx, charge)
	})
})

run, err := checkout.Start(ctx, "order-123", order)
err = run.Signal(ctx, "review", true)
id, err := run.Result(ctx)
```

## Building

The package uses cgo and links `libfusee_c`, the engine's C interface
(`bindings/c`, header `bindings/c/include/fusee.h`). Build it first, from
the repository root:

```bash
cargo build --release -p fusee-c
cd bindings/go && go test ./...
```

By default the package links `target/release/libfusee_c.a` statically, so a
Go binary carries the engine and needs no shared library. Build tags choose
otherwise: `fusee_dynamic` links `target/release/libfusee_c.dylib` / `.so`
(with an rpath to it), `fusee_system` links `-lfusee_c` from the system or
`CGO_LDFLAGS`. On macOS the linker may warn that objects were built for a
newer macOS than the one being linked; it is harmless (set
`MACOSX_DEPLOYMENT_TARGET` when building the library to match). Go 1.23 or
newer.

## How it runs

The engine is embedded, as in the TypeScript and Python SDKs: Go runs and
replays workflow code, and the engine records the journal, decides when a
step's inputs are durable, and hands the Go side events and deliveries
through a flat interface (numeric run keys, events as rows of numbers,
commit positions read from shared memory, payloads as JSON or raw bytes).
Every durability mode goes through it: `memory`, `local`, and `server` and
`replicated`, where the native core speaks the log service protocol to
`fusee server` or to the other replicas itself: one core speaks the
protocol for every SDK.

An engine has one loop goroutine. Workflow code runs on goroutines the
engine schedules one at a time: the goroutine holding the engine hands it
to a run's goroutine and waits until that one blocks on a journal operation
or returns, through channel handoffs, so whatever one wrote the next sees.
Between two deliveries a run's code runs until all its goroutines are
blocked, exactly as on replay, which is what makes the journal the same
live and replaying. The engine handle moves between OS threads with the
goroutine holding it; the C interface allows that (calls must not overlap,
and need no particular thread). The loop is not locked to one OS thread:
a handoff to a locked thread costs about 25 µs on macOS, against 0.2 µs
between ordinary goroutines.

**Step functions run on goroutines of their own**, concurrently with
everything else on the engine, and receive a `context.Context` that is
cancelled when the attempt times out or the engine closes. So a slow step
does not hold up other workflows. Two consequences:

- A step function must not read or write the workflow's variables (it runs
  beside the workflow's code): give it what it needs, and return its
  result. `fusee.Key(ctx)`, `fusee.Heartbeat(ctx, v)`,
  `fusee.Progress[T](ctx)` and `fusee.Stream(ctx, name)` work inside it.
- Parallel steps record in the order they finish. Replay follows the
  journal, so this is only visible in the journal itself.

`StepOptions{Inline: true}` runs the function on the workflow's goroutine
while it holds the engine instead: no handoff (a sequential chain of inline
no-op steps costs 1.5 µs a step against 3.8 µs), the order it starts in,
and access to workflow state, but nothing else on the engine runs until it
returns and `Timeout` cannot cut it short. Use it for fast, CPU-only
functions.

## Streams

A run appends chunks (JSON, or `[]byte` as bytes) to named streams, and
readers follow them from an offset ([docs/features.md](../../docs/features.md#streams)):

```go
answer, err := fusee.Step(c, "generate", func(ctx context.Context) (string, error) {
	out, err := fusee.Stream(ctx, "tokens") // the step's stream: its result completes it
	if err != nil {
		return "", err
	}
	prev, err := fusee.Recorded[string](out) // what an earlier attempt recorded
	if err != nil {
		return "", err
	}
	text := strings.Join(prev, "")
	for token := range model.Stream(ctx, prompt, text) { // continue after the prefix
		if err := out.Append(token); err != nil {
			return "", err
		}
		text += token
	}
	return text, nil
})

// Anywhere outside workflow code: this process, or a Client through a server.
r := e.Run(id).Stream("tokens", lastSeen)
for chunk, err := range r.All(ctx) {
	if err != nil {
		return err
	}
	w.Write(chunk.JSON) // chunk.Bytes for a chunk of bytes; fusee.ChunkAs[T](chunk)
}
// r.Offset(): where to reconnect; r.Status(): how the stream ended
```

- **Appending never waits.** From a step's goroutine `Append` queues the
  chunk for the engine's loop and returns; the loop records every queued
  chunk in its next turn, in order, before the step's result. A failure
  (say, the stream belongs to another step) is returned by the next
  `Append` or `Close`. An attempt that timed out cannot append once its
  retry started.
- **Readers see only durable chunks**, so nothing a reader saw is taken
  back by a crash. `r.Next(ctx)` waits (a long poll of 30 s per request,
  `r.Wait(d)`) and reports the end once the stream is complete.
- **A step that runs again keeps the recorded prefix**: `RecordedCount`
  and `Recorded[T]` give it what earlier attempts recorded (after a crash,
  what was durable), and `Closed` is true when an earlier attempt
  completed the stream (appends are then dropped).
- **Workflow code** opens the run's stream with `c.Stream(name)`: its
  appends are recorded once (a replay appends nothing), and `Close` or the
  run's end completes it. A step appends to the same stream by opening it
  with `fusee.Stream(ctx, name)` (a goroutine has no identity, so a writer
  workflow code opened cannot tell which step calls it; it is for workflow
  code and inline steps).
- In server mode the engine answers the `streamRead` calls clients send
  through the server, whatever their language.

## Objects

An object type is a set of handlers; each key is one object with its own
JSON state, and calls to a key's exclusive handlers take turns
([docs/features.md](../../docs/features.md#objects-keyed-state)):

```go
cart, err := fusee.RegisterObject(e, "cart", fusee.Handlers{
	"add": fusee.Exclusive(func(c *fusee.ObjectCtx, item string) (int, error) {
		price, err := fusee.Step(c.Ctx, "price", func(ctx context.Context) (int, error) {
			return pricing.Quote(ctx, item)
		})
		if err != nil {
			return 0, err
		}
		items, _, err := fusee.StateGet[[]string](c.State(), "items")
		if err != nil {
			return 0, err
		}
		items = append(items, fmt.Sprintf("%s@%d", item, price))
		return len(items), c.State().Set("items", items) // committed with the result
	}),
	"get": fusee.Shared(func(c *fusee.ObjectCtx, _ any) ([]string, error) {
		items, _, err := fusee.StateGet[[]string](c.State(), "items")
		return items, err
	}),
})

// From outside: an ID makes a retried call run once.
n, err := fusee.CallRef[int](ctx, cart.Key("user-7"), "add", "tea", fusee.CallOptions{ID: "req-42"})
// From workflow code: a journal operation, run exactly once.
n, err = fusee.Call[int](c.Object("cart", "user-7"), "add", "tea")
```

`ref.State(ctx)` reads the committed state, `ref.Inspect(ctx)` adds the
running and waiting calls, `e.Objects(ctx, "cart", 0)` lists the keys, and
`client.Object("cart", k)` calls a key through a server.

## Deterministic replay in Go

Workflow code (the function given to `Register`, functions started with
`fusee.Go`, update handlers) must make the same journal operations in the
same order on every run:

- Wait, branch on time or randomness, and run code concurrently only
  through the package: `fusee.Step`, `c.Sleep`, `fusee.WaitFor`,
  `fusee.Child`, `fusee.All`, `fusee.Race`, `fusee.Go`, `c.Now`,
  `c.Random`, `c.UUID`.
- Start no goroutines, and use no channels, `select`, mutexes, timers,
  `time.Now`, `time.Sleep` or `math/rand` in workflow code. Step functions
  may do anything.
- Map iteration order is random in Go: do not let it decide which journal
  operations run, or in which order (sort the keys).
- A `*fusee.Ctx` and the futures it made belong to one run; a `Ctx` used
  outside its own goroutine panics.
- Do not call `Engine` or `Run` methods from workflow code or inline steps
  (they wait for the engine, which the caller holds): use the `Ctx`
  (`c.Signal`, `fusee.Child`, `c.CancelWorkflow`).
- Do not recover the panics the package raises, and do not swallow a
  `*fusee.DeterminismError`: the engine has stopped the run.

Code that changed under a recorded run stops it with a
`*fusee.DeterminismError` (status `stuck`) naming the step the journal has
and the one the code asked for; deploy code that matches, or use
`c.Patched(id)` for new branches.

## API

| | |
| --- | --- |
| Engine | `fusee.Open(fusee.Options{Durability, Dir, Address, ReplicaID, Peers, Shards, Retention, ParkAfter, CheckpointInterval, Flush, SegmentBytes, KeepHistory, ArchiveHistory, Codec, InlineFlush, OnExhausted})`, `e.Close()` |
| Workflows | `fusee.Define[In, Out](name, fn, fusee.RegisterOptions{Version})` (a definition apart from any engine: `fusee.Options{Workflows: []fusee.Definition{def}}`, `def.Register(e)`, `def.On(e)`), `fusee.Register[In, Out](e, name, fn, fusee.RegisterOptions{Version})`, `fusee.WorkflowOf[In, Out](e, name)`; `wf.Start(ctx, id, in, fusee.StartOptions{Timeout, IfRunning, IfFinished, Version})`, `fusee.UpdateWithStart[R](ctx, wf, id, in, update, arg, opts)`, `wf.Debounce`, `wf.Batch` |
| Steps | `fusee.Step[T](c, name, fn, fusee.StepOptions{Retry, Timeout, HeartbeatTimeout, Limit, Priority, FairnessKey, FairnessWeight, ReadOnly, Inline})`, `fusee.StartStep` (a future); `fusee.RetryPolicy{Attempts, Backoff, Initial, Max, NonRetryable, RetryIf, OnExhausted}` (`fusee.Pause` pauses the run when the retries run out, until `Resume`); the step's `context.Context` is its step context: `fusee.Key(ctx)`, `fusee.Heartbeat(ctx, v)`, `fusee.Progress[T](ctx)`, `ctx.Done()` on cancellation, timeout or run end |
| Waiting | `c.Sleep(name, d)`, `c.StartSleep`, `fusee.WaitFor[T](c, signal, timeout)` (ok false on timeout), `fusee.StartWait` |
| Children and signals | `fusee.Child[O](c, type, id, input, fusee.ChildOptions{ParentClose, Timeout})`, `fusee.StartChild`, `c.SpawnChild`, `c.Signal(target, name, v)`, `c.CancelWorkflow(target, reason)` |
| Concurrency | `fusee.Go[T](c, fn)`, `fusee.All(c, futures...)`, `fusee.Race(c, futures...)` (index; losers dropped); `future.Get(c)` |
| Queries and updates | `fusee.OnQuery[A, R](c, name, fn)`, `fusee.OnUpdate[A, R](c, name, handle, validate)`; `run.Query`, `run.Update`, typed `fusee.Query[R]`, `fusee.Update[R]` |
| Durable values | `c.Now()`, `c.Random()`, `c.UUID()`, `c.JournalSize()` |
| Versions and attributes | `c.Patched(id)`, `c.DeprecatePatch(id)`, `c.SetAttributes(map)`, `c.ContinueAsNew(input, fusee.ContinueOptions{Version})` |
| Completion tokens | `fusee.AwaitCompletion[T](c, name, send)`; `e.Complete(ctx, token, v)`, `e.Fail(ctx, token, msg)` |
| Streams | `c.Stream(name)` (workflow code), `fusee.Stream(ctx, name)` (a step function): `w.Append(v)`, `w.Close()`, `fusee.Recorded[T](w)`, `w.RecordedRaw()`, `w.RecordedCount()`, `w.Closed()`; `run.Stream(name, from)`: `Next(ctx)`, `All(ctx)` (an iterator), `Offset`, `Status`, `End`, `RunID`, `Wait(d)`, `OfRun(run)`; a `fusee.Chunk` has `Offset`, `JSON` or `Bytes`, `Value()`, and `fusee.ChunkAs[T](chunk)` |
| Runs | `e.Run(id)`, `fusee.RunOf[Out](e, id)`: `Signal`, `Result`, `Started`, `Update`, `Query`, `Cancel`, `Terminate`, `Reset`, `Resume` (a paused run), `Inspect`, `Stream` |
| Engine | `e.Serve(addr, token...)` (the HTTP API and UI, answered by this engine), `e.On(event, fn)` (`"stuck"`, `"paused"`), `e.List(ctx, fusee.ListOptions{Limit, Where})`, `e.Limit(name, fusee.LimitOptions{Concurrent, PerSecond})`, `e.LimitInUse`, `e.LimitWaiting`, `e.Schedule(ctx, name, workflow, input, fusee.ScheduleOptions{...})`, `e.ScheduleHandle(name)` (`Pause`, `Resume`, `Trigger`, `Backfill`, `Describe`, `Delete`), `e.ExportJournal`, `e.ShardMetrics`, `e.Stats` |
| Objects | `fusee.RegisterObject(e, type, fusee.Handlers{...})` with `fusee.Exclusive` and `fusee.Shared`; in a handler `c.State()` (`fusee.StateGet[T]`, `Set`, `Clear`, `ClearAll`, `Keys`, `Raw`), `c.ObjectKey()`; `c.Object(type, key)` (`fusee.Call[R]`, `fusee.StartCall[R]`, `Send`); `e.Object(type, key)` and `ty.Key(key)` (`Call`, `fusee.CallRef[R]`, `Send`, `State`, `Inspect`, `Delete`; from outside, `SetState`, `PatchState`, `ClearState` and `EditState` with `IfVersion`), `e.Objects(ctx, type, n)` |
| Remote | `fusee.Connect(address)`: `Call`, `Start`, `UpdateWithStart`, `Debounce`, `Batch`, `List`, `Complete`, `Fail`, `Object(type, key)`, `Run(id)` (`Result`, `Started`, `Signal`, `Query`, `Update`, `Cancel`, `Terminate`, `Reset`, `Resume`, `Stream`) |
| Invoked workers | `fusee.NewInvocationHandler(fusee.InvocationOptions{Engine, Address, Secret, Register})`: an `http.Handler`, and `Invoke(ctx, signature, body)`; `fusee.VerifySignature` |
| Errors | Failures travel as the [envelope](../../docs/errors.md#the-failure-envelope): return a `*fusee.Failure{Message, Type, Code, Details, Retryable, Cause}` (or any error: its message, type name and wrapped errors are recorded); `fusee.Fatal(err)` is never retried; `fusee.FailureOf(err)`. Raised: `*StepError{Step, Detail, Cause}`, `*WorkflowError{Code, ID, Cause}` (a failed run, child or update), `*fusee.Error{Code, Message}`, `*CancelledError` (return `fusee.Cancelled(reason)` to finish cancelled), `*TimeoutError`, `*TerminatedError`, `*DeterminismError`, `*RejectedError`, `fusee.ErrClosed` |

Calls that wait take a `context.Context`; cancelling it stops the wait (a
start or signal already sent still happens). Payloads are JSON
(`encoding/json`); a `[]byte` is recorded as raw bytes, as `bytes` and
`Uint8Array` are in Python and TypeScript, so the SDKs read each other's
payloads.

In server mode (`Durability: "server"`, `Address: "host:port"`) the engine
answers calls that clients send through the server, whatever their
language.

## Not supported

- Worker parts: one engine's workflow code runs on one goroutine at a time
  (the step functions run in parallel). TypeScript's `workers` option,
  which splits shards across threads, has no Go counterpart; run
  several engines on separate directories instead.
- A `Replayer` for exported journals, and OpenTelemetry instruments.
- Replica metrics while a replicated member waits to lead.

## Tests and conformance

```bash
go test -race ./...                 # server tests start ../../target/release/fusee (FUSEE_CLI)
go run ./cmd/conformance > go.json  # then conformance/compare.py with the other SDKs
go run ./cmd/bench memory           # step-to-step latency, calls per step
```

`go test` covers steps, retries, sleeps, signals, children, races,
`fusee.Go`, updates, queries, continue-as-new, limits, timeouts,
heartbeats, completion tokens, schedules, debounce and batch, a clean close
and reopen, a process killed mid-run and reopened, a determinism error after
a code change, and server mode against `fusee server` with a client.
Stream tests (`stream_test.go`) cover chunk order and completeness while a
step streams, reconnecting from an offset, a retry continuing after the
recorded prefix, workflow appends recorded once across a reopen and
replay, a process killed mid-stream with a resuming reader that sees no
chunk twice, an attempt that timed out, and readers through a server
(the Go worker answering `streamRead`); `FUSEE_STREAM_COST=1 go test -run
StreamCost` prints append cost and reader latency (see
[docs/features.md](../../docs/features.md#streams)). The conformance runner
prints the 40 scenarios' journals, streams included, and
`conformance/remote.py` runs a Go worker and a Go client against the other
languages' through a server, a stream read among them.

## Cost per step

A sequential chain of no-op steps in memory mode (`go run ./cmd/bench`),
against the other SDKs' `bench.mjs` and `bench.py` on the same 18-core
Apple M-series laptop, p50 step to step:

| | p50 | calls into the engine per step |
| --- | --- | --- |
| Go | 3.8 µs | 7.0 (2 ticks) |
| Go, inline steps | 1.5 µs | 3.1 |
| Node | 3.8 µs | 5.0 |
| Python | 8.1 µs | 3.1 |

Go's default path costs what Node's does (ratio 1.0) and half of Python's
(0.47); inline steps cost 0.39 of Node's and 0.19 of Python's. The default
path's extra calls come from the step finishing on another goroutine: its
result comes back through the loop, which ticks between.
