# fusee (Python)

Durable execution at the speed of the disk, embedded in your process
(asyncio). Python 3.9 or later; typed (`py.typed`).

```python
import asyncio
import fusee


@fusee.workflow("greet")
async def greet(ctx: fusee.Ctx, name: str) -> str:
    greeting = await ctx.step("compose", lambda: f"hello {name}")
    await ctx.sleep("pause", "1s")
    return greeting


async def main():
    async with await fusee.Engine.open("./data", workflows=[greet]) as engine:
        run = await engine.start(greet, "greet-1", "world")
        print(await run.result())


asyncio.run(main())
```

Kill the process during the sleep and run it again: the run resumes where it
was, and `compose` does not run twice.

## Definitions

Workflows, signals, queries, updates and object types are plain values,
made apart from any engine, so a module can define them and a client can
import them for their names and types alone:

```python
review = fusee.signal("review", bool)
status = fusee.query("status")


@fusee.workflow("checkout")
async def checkout(ctx: fusee.Ctx, order: Order) -> Receipt:   # Order: dataclass, pydantic, msgspec, TypedDict
    quote = await ctx.step("quote", lambda: pricing.quote(order))
    charge = await ctx.step("charge", lambda s: payments.charge(quote, key=s.key),
                            retry=fusee.Retry(attempts=5))
    ctx.on_query(status, lambda _: {"phase": "review"})
    approved = await ctx.wait_for(review, timeout="1d")             # bool | None
    if not approved:
        await ctx.step("refund", lambda: payments.refund(charge))
    return Receipt(order, charge)


engine = await fusee.Engine.open("./data", workflows=[checkout])
run = await engine.start(checkout, "order-123", order)              # Run[Receipt]
await run.signal(review, True)
receipt = await run.result()                                        # a Receipt again
```

- `@fusee.workflow("name")` (or bare `@fusee.workflow`, named after the
  function), with `version=`, `timeout=`, `input=`, `output=`. The input's
  and result's annotations are read once and decide what the code receives
  and what `run.result()` returns: dataclasses, pydantic models, msgspec
  structs, `datetime`, `UUID`, enums, and lists and dicts of them. A start
  whose input does not match its type is refused before anything is
  recorded.
- `fusee.signal(name, type)`, `fusee.query(name, arg=, result=)`,
  `fusee.update(name, arg=, result=)`. A plain string works wherever a
  definition does, for calls across languages.
- `fusee.object("cart", {"add": add, "items": fusee.shared(items)})`, or
  `cart = fusee.object("cart")` with `@cart.exclusive` and `@cart.shared`.
- `Engine.open(workflows=[...], objects=[...])` registers them;
  `engine.register(...)` adds more later. Registering a type twice is an
  error that names both places.

## The step context

A step function may take one argument, the step context `s`:

| | |
| --- | --- |
| `s.key` | an idempotency key, stable across retries and recovery |
| `s.attempt` | 1, 2, ... |
| `s.heartbeat(p)`, `s.progress()` | progress, also from before a crash |
| `s.stream(name)` | the step's own stream |
| `s.log` | a logger tagged with the workflow ID, run and step |

It is built only when the function declares the parameter
(`lambda s: ...`); `lambda: ...` costs nothing. Cancellation of the
workflow, its deadline and the step's own timeouts cancel the step's
coroutine.

A synchronous step function runs on the event loop and holds every other
run while it runs: make it `async`, or pass `thread=True` to run it on a
thread (or open the engine with `sync_steps="thread"`). Under the `dev`
profile a synchronous step that holds the loop over 50 ms is reported once
with both fixes.

## Errors and retries

A failed step's error reaches the caller whole: the step's name, and its
failure with the exception's type, `code`, `details`, stack and cause.

```python
class CardDeclined(fusee.Fatal):          # Fatal: never retried
    code = "card_declined"

raise CardDeclined("card declined", details={"last4": "4242"})

try:
    await run.result()
except fusee.StepError as e:
    e.step, e.cause.type, e.cause.code, e.cause.details   # "charge", "CardDeclined", ...
```

- A failure is final when it is a `fusee.Fatal`, when the exception has
  `retryable = False`, when its type (or a cause's) is in
  `Retry(non_retryable=(...))`, or when `Retry(retry_if=lambda failure,
  attempt: ...)` says so. `Retry(attempts=3, initial="100ms", factor=2,
  max="10s", jitter=True, max_elapsed=None)` is the default policy.
- A step result JSON cannot encode is a `CodecError` naming the path of
  the value (`a set at $.items[1].tags`), and is not retried.
- Every error has `code` (the same in every SDK) and `docs`, a link to its
  page in [docs/errors.md](../../docs/errors.md): `NotFoundError`,
  `AlreadyExistsError`, `UnknownWorkflowError`, `StepError`,
  `WorkflowError`, `ChildError`, `CancelledError`, `TimeoutError`,
  `TerminatedError`, `DeterminismError`, `RejectedError`, `CodecError`,
  `ConnectionError`, `IoError`, `BusyError`, `ClosedError`.
- `Engine.open(record_stacks=False)` keeps stack traces out of the journal.

Mistakes fail where they are made: starting a type no code is registered
for raises `UnknownWorkflowError` with the registered types and the
nearest name; a run whose code no longer matches its journal logs one line
with the difference and the three fixes (and `engine.on("stuck", fn)` is
called); a run that fails keeps its whole journal for `run.describe()` and
`run.retry()`.

## The engine and the client

`Engine` (embedded) and `Client` (through a Fusee server) have the same
interface, so code that starts, signals and reads runs does not know which
it holds:

| | |
| --- | --- |
| `start(wf, id, input, id_policy=, timeout=, version=)` | a `Run`; `id_policy={"running": "use-existing"}` attaches to a running ID |
| `run(id, wf=None)` | a handle; `wf` types its result |
| `signal_with_start(wf, id, input, signal, value)` | a `Run`; starts it if it is not running, and the signal is recorded before its code first runs |
| `update_with_start(wf, id, input, update, arg)` | `(run, result)` |
| `object(type, key)` | `call(handler, arg, id=)`, `send`, `state()`, `describe()`, `delete()`; `set_state(field, value)`, `patch_state(ops)`, `clear_state(field=None)`, `edit_state(edit, if_version=)` |
| `schedule(name, spec, wf, input)` | `spec`: a cron string, `{"every": "1h"}`, or a `ScheduleSpec`; `schedules.get(name)`, `schedules.list()` |
| `list(type=, status=, started_after=, started_before=, attributes=)` | an async iterator of `RunSummary` |
| `complete(token, value)`, `fail(token, failure)` | async completions |
| `debounce(wf, key, input, window=, max_wait=, event_id=)`, `batch(wf, key, event, max_events=, max_wait=, event_id=)` | queued starts |
| `close()` | also `async with` |

A `Run` has `signal(sig, value)`, `query(q, arg)`, `update(u, arg, id=)`,
`result()`, `status()` (a `RunSummary`), `describe()` (the summary and the
journal), `cancel(reason)`, `terminate(reason)`, `reset(before_step)`,
`retry()` (from the failed step), `resume()` (a run paused by a step whose
retries ran out: `Retry(on_exhausted="pause")`, or `Engine.open(...,
on_exhausted="pause")` for every step), `stream(name, from_=)` and
`started()`.

The engine alone has `limit(name, concurrent=, per_second=)`, `limits()`,
`limit_usage(name)`, `on(event, fn)`, `metrics()`, `shard_metrics()`,
`serve(addr, token=)` (the HTTP API and the UI) and `export_journal(id)`.

For code without an event loop (Django, Flask, scripts), `fusee.sync.Client`
has the same methods, blocking: `Client.connect(address)`, then
`client.start(...)`, `run.result(timeout=30)`, `run.signal(...)`.

`Engine.open()` with no directory reads `FUSEE_DIR`, else `FUSEE_URL` (a
worker of a server), else `./fusee-data`; so one program runs embedded on a
laptop and against a server in production.

## The `dev` profile

`Engine.open(profile="dev")` or `FUSEE_PROFILE=dev` (never inferred) says
so on stderr, keeps every run's history, flushes with `fsync` instead of
`F_FULLFSYNC` on macOS (a process crash loses nothing; power loss may),
reports synchronous steps that hold the event loop, replays each run against
the code when it parks or ends and logs any difference (strict replay),
makes `result()` of a stuck run raise `DeterminismError` at once instead of
waiting, shows `ctx.log` records on stderr when logging is not configured,
and serves the UI (`FUSEE_HTTP`, else port 7601, else a free port;
`http="off"` serves nothing).

## Testing

```python
from fusee.testing import TestEngine

t = await TestEngine.open(workflows=[checkout])
t.mock_step(checkout, "charge", lambda attempt: {"id": "ch_1"})
run = await t.engine.start(checkout, "order-1", order)
assert await t.steps("order-1") == ["quote", "charge"]
await t.check()   # strict replay of every run
```

See [docs/testing-workflows.md](../../docs/testing-workflows.md).

## Workflow code

Workflow coroutines run under Fusee's deterministic scheduler and may only
await `ctx` operations; awaiting anything else is an error that says to
put it in a step. `ctx` has `step`, `sleep(name, duration)`,
`sleep_until(name, at)`, `wait_for(signal, timeout=)`, `child(wf, id,
input, parent_close=, timeout=)`, `start_child`, `signal_workflow(id, sig,
value)`, `cancel_workflow(id, reason=)`, `object(type, key)`,
`on_query`, `on_update(update, handler, validate=)`,
`await_completion(name, send)` (`send(cb)` gets `cb.token` and, when the
engine serves its API, `cb.url`), `compensate(name, fn)` (a step that runs,
newest first, if the workflow fails or is cancelled),
`continue_as_new`, `patched`, `deprecate_patch`, `set_attributes`, `stream`,
`now`, `random`, `uuid`, `all`, `race`, `log` and `journal_size`.
Durations are milliseconds, `"500ms"`, `"10s"`, `"5m"`, `"2h"`, `"1d"`, or
a `timedelta`. Payloads are JSON (no pickle), except `bytes`, recorded raw.

Steps waiting for a limit's permit (`limit=`) go by `priority` (higher
first), then take turns by `fairness_key` (`fairness_weight` permits per
turn). See [docs/features.md](../../docs/features.md) for streams, objects,
debounce, batching, schedules, patching, encryption
(`Engine.open(codec=fusee.aes_gcm(keys))`), telemetry
(`telemetry=fusee.open_telemetry()`) and replay testing
(`fusee.Replayer(workflows=[...]).replay_file(path)`).

## Agents

`fusee.ai.model_call` makes a model call a step whose tokens stream to
readers. Agents written with a framework become durable without changing
their loop: each model call and each tool call is a step, so a crash
resumes at the call that was running.

```python
from fusee.ai.pydantic import FuseeDurability, run      # pip install 'fusee[pydantic-ai]'

agent = Agent("openai:gpt-5", name="support", tools=[refund], capabilities=[FuseeDurability()])

@fusee.workflow("support")
async def support(ctx, question):
    return (await run(ctx, lambda: agent.run(question))).output
```

`fusee.ai.openai.run(ctx, agent, question)` does the same for the OpenAI
Agents SDK (`pip install 'fusee[openai-agents]'`). See
[Agents](../../docs/guides/agents.mdx) and [Agent
patterns](../../docs/guides/agent-patterns.md).

## Server and replicated modes

```python
engine = await fusee.Engine.open(durability="server", address="127.0.0.1:7600", workflows=[checkout])

client = await fusee.Client.connect("127.0.0.1:7600", token_file="/etc/fusee/token",
                                    tls={"ca": "/etc/fusee/ca.pem"})
run = await client.start(checkout, "order-42", order)
```

Several Python processes can run one server's workflows, each on its own
core: start each worker with `max-shards=N` (and `shm=1` for shared memory
on the same host), as in `address="unix:/run/fusee.sock?shm=1&max-shards=2"`.
Each takes the free shards it may hold and runs only their workflows; all
of them append to the server's one log. A worker's own `engine.start` or
`engine.run(id)` works for IDs of its own shards only; use a `Client` for
the others. For replication, start a worker on each of three or five
machines with `durability="replicated"`, `replica_id`, `peers` and `dir`.

## Invoked workers (scale to zero)

A server started with `--invoke URL` POSTs to that URL when a shard that no
worker holds has work due. `invocation_handler` answers: each request opens
a fresh engine for the invocation's shards, runs them until they are idle or
the budget ends, and closes it.

```python
app = fusee.invocation_handler(
    "fusee.internal:7600?ca=/etc/fusee/ca.pem&token-file=/etc/fusee/token",
    workflows=[order],
    secret=os.environ["FUSEE_INVOKE_SECRET"],  # the server's --invoke-secret-file
)
# ASGI: uvicorn module:app;  AWS Lambda: app.lambda_handler
```

## Building

`maturin develop` in this directory, or `cargo build --release -p
fusee-python` and copy the library to `python/fusee/_native.abi3.so` (on
macOS remove the old file first: overwriting a loaded library in place
invalidates its signature).
