# API at a glance

Generated from [`api/spec.json`](../api/spec.json) by `api/gen.py`; do not edit.
Every operation has one name, one argument order and one option set,
written in each language's case. `api/check.py` checks the SDKs against it.

## Definitions

| Operation | Arguments | Options | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **workflow** | name, fn | version, input, output, timeout, concurrency | `WorkflowRef::new` | `workflow` | `workflow` | `Define` | `workflow` |
| **signal** | name | schema | `Signal::new` | `signal` | `signal` | `Signal` | `signal` |
| **query** | name | schema | `Query::new` | `query` | `query` | `Query` | `query` |
| **update** | name | schema | `Update::new` | `update` | `update` | `Update` | `update` |
| **object** | name, handlers | version | `Engine::object_type(name).exclusive(..)` | `object` | `object` | `Object` | `object` |
| **shared** | handler |  | `ObjectType::shared` | `shared` | `shared` | `Shared` | `shared` |

- **workflow**: A workflow type: a plain value, apart from any engine. Pass it to Engine.open({ workflows }), engine.start, ctx.child. The options form is workflow(name, options, fn).
- **signal**: A signal's name and payload type. A plain string works wherever a definition does.
- **query**: A query's name, argument and result types.
- **update**: An update's name, argument and result types.
- **object**: A keyed object type. Handlers are exclusive unless wrapped in shared().
- **shared**: Marks an object handler as shared: it reads state and runs beside other shared calls.

## Engine and Client (one interface)

| Operation | Arguments | Options | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **start** | workflow, id, input | idPolicy, timeout, version, delay, at, attributes | `start` | `start` | `start` | `Start` | `start` |
| **run** | id |  | `run` | `run` | `run` | `Run` | `run` |
| **signalWithStart** | workflow, id, input, signal, value | idPolicy, timeout, version | `signal_with_start` | `signalWithStart` | `signal_with_start` | `SignalWithStart` | `signalWithStart` |
| **updateWithStart** | workflow, id, input, update, arg | idPolicy, timeout, version, updateId | `update_with_start` | `updateWithStart` | `update_with_start` | `UpdateWithStart` | `updateWithStart` |
| **object** | type, key |  | `object` | `object` | `object` | `Object` | `object` |
| **schedule** | name, spec, workflow, input |  | `schedule` | `schedule` | `schedule` | `Schedule` | `schedule` |
| **schedules** |  |  | `Engine::list_schedules` | `schedules` | `schedules` | `Schedules` | `schedules` |
| **list** |  | type, status, version, startedAfter, startedBefore, attributes, limit, pageSize | `Engine::list_where` | `list` | `list` | `List` | `list` |
| **complete** | token, value |  | `Engine::complete` | `complete` | `complete` | `Complete` | `complete` |
| **fail** | token, failure |  | `fail` | `fail` | `fail` | `Fail` | `fail` |
| **debounce** | workflow, key, input | window, maxWait, dedupWindow, eventId | `debounce` | `debounce` | `debounce` | `Debounce` | `debounce` |
| **batch** | workflow, key, event | maxEvents, maxWait, dedupWindow, eventId | `batch` | `batch` | `batch` | `Batch` | `batch` |
| **close** |  |  | `close` | `close` | `close` | `Close` | `close` |

- **run**: A handle; makes no call.
- **schedule**: Creates or updates the schedule. spec: { cron, every, timezone, overlap, catchupWindow, jitter, paused }.
- **schedules**: schedules.get(name), schedules.list().

## Engine only

| Operation | Arguments | Options | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **open** |  | dir, url, durability, workflows, objects, shards, workers, module, profile, serializer, codec, keepHistory, recordStacks, onExhausted, retention, telemetry, logger | `open` | `open` | `open` | `Open` | `open` |
| **limit** | name, spec |  | `limit` | `limit` | `limit` | `Limit` | `limit` |
| **limits** |  |  | `limits` | `limits` | `limits` | `Limits` | `limits` |
| **serve** |  | port, host, token, ui | `Config::http / http::serve` | `serve` | `serve` | `Serve` | `serve` |
| **on** | event, handler |  | `log::set_hook` | `on` | `on` | `On` | `on` |
| **metrics** |  |  | `metrics` | `metrics` | `metrics` | `Metrics` | `metrics` |

- **open**: No arguments: FUSEE_DIR, else FUSEE_URL (a client), else ./fusee-data.
- **limit**: spec: { concurrent, perSecond }.
- **serve**: Serves the HTTP API (and the UI) from its own thread.
- **on**: Events: stuck, waitingForCode.

## A run

| Operation | Arguments | Options | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **signal** | signal, value |  | `signal` | `signal` | `signal` | `Signal` | `signal` |
| **query** | query, arg |  | `query` | `query` | `query` | `Query` | `query` |
| **update** | update, arg | id | `update` | `update` | `update` | `Update` | `update` |
| **result** |  |  | `result` | `result` | `result` | `Result` | `result` |
| **status** |  |  | `status` | `status` | `status` | `Status` | `status` |
| **describe** |  |  | `describe` | `describe` | `describe` | `Describe` | `describe` |
| **cancel** |  | reason | `cancel` | `cancel` | `cancel` | `Cancel` | `cancel` |
| **terminate** |  | reason | `terminate` | `terminate` | `terminate` | `Terminate` | `terminate` |
| **reset** | beforeStep | reapplySignals, version | `reset` | `reset` | `reset` | `Reset` | `reset` |
| **retry** |  |  | `retry` | `retry` | `retry` | `Retry` | `retry` |
| **resume** |  |  | `resume` | `resume` | `resume` | `Resume` | `resume` |
| **stream** | name | from, wait | `stream` | `stream` | `stream` | `Stream` | `stream` |

- **describe**: The summary and the journal as a typed union of records.
- **retry**: Resets a failed run to before its failed step.
- **resume**: Runs a paused step again with a fresh set of attempts.

## An object's key

| Operation | Arguments | Options | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **call** | handler, arg | id | `call` | `call` | `call` | `Call` | `call` |
| **send** | handler, arg | id | `send` | `send` | `send` | `Send` | `send` |
| **state** |  |  | `state` | `state` | `state` | `State` | `state` |
| **describe** |  |  | `describe` | `describe` | `describe` | `Describe` | `describe` |
| **delete** |  |  | `delete` | `delete` | `delete` | `Delete` | `delete` |

## A schedule

| Operation | Arguments | Options | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **pause** |  |  | `pause` | `pause` | `pause` | `Pause` | `pause` |
| **resume** |  |  | `resume` | `resume` | `resume` | `Resume` | `resume` |
| **trigger** |  |  | `trigger` | `trigger` | `trigger` | `Trigger` | `trigger` |
| **backfill** | from, to |  | `backfill` | `backfill` | `backfill` | `Backfill` | `backfill` |
| **describe** |  |  | `describe` | `describe` | `describe` | `Describe` | `describe` |
| **delete** |  |  | `delete` | `delete` | `delete` | `Delete` | `delete` |

## Workflow context

| Operation | Arguments | Options | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **step** | name, fn | retry, timeout, heartbeatTimeout, readOnly, limit, priority, fairnessKey, fairnessWeight, output, thread | `step` | `step` | `step` | `Step` | `step` |
| **sleep** | name, duration |  | `sleep` | `sleep` | `sleep` | `Sleep` | `sleep` |
| **sleepUntil** | name, at |  | `Ctx::sleep_until` | `sleepUntil` | `sleep_until` | `SleepUntil` | `sleepUntil` |
| **waitFor** | signal | timeout | `wait_for` | `waitFor` | `wait_for` | `WaitFor` | `waitFor` |
| **child** | workflow, id, input | parentClose, timeout | `child` | `child` | `child` | `Child` | `child` |
| **startChild** | workflow, id, input | parentClose, timeout | `start_child` | `startChild` | `start_child` | `StartChild` | `startChild` |
| **signalWorkflow** | id, signal, value |  | `signal_workflow` | `signalWorkflow` | `signal_workflow` | `SignalWorkflow` | `signalWorkflow` |
| **cancelWorkflow** | id | reason | `cancel_workflow` | `cancelWorkflow` | `cancel_workflow` | `CancelWorkflow` | `cancelWorkflow` |
| **object** | type, key |  | `Ctx::object` | `object` | `object` | `Object` | `object` |
| **onQuery** | query, handler |  | `on_query` | `onQuery` | `on_query` | `OnQuery` | `onQuery` |
| **onUpdate** | update, handler | validate | `on_update` | `onUpdate` | `on_update` | `OnUpdate` | `onUpdate` |
| **awaitCompletion** | name, send | retry, timeout | `await_completion` | `awaitCompletion` | `await_completion` | `AwaitCompletion` | `awaitCompletion` |
| **compensate** | name, fn | retry, timeout | `compensate` | `compensate` | `compensate` | `Compensate` | `compensate` |
| **continueAsNew** | input | version | `continue_as_new` | `continueAsNew` | `continue_as_new` | `ContinueAsNew` | `continueAsNew` |
| **patched** | id |  | `patched` | `patched` | `patched` | `Patched` | `patched` |
| **deprecatePatch** | id |  | `deprecate_patch` | `deprecatePatch` | `deprecate_patch` | `DeprecatePatch` | `deprecatePatch` |
| **setAttributes** | attributes |  | `set_attributes` | `setAttributes` | `set_attributes` | `SetAttributes` | `setAttributes` |
| **stream** | name |  | `stream` | `stream` | `stream` | `Stream` | `stream` |
| **now** |  |  | `Ctx::now_ms` | `now` | `now` | `Now` | `now` |
| **random** |  |  | `random` | `random` | `random` | `Random` | `random` |
| **uuid** |  |  | `uuid` | `uuid` | `uuid` | `UUID` | `uuid` |
| **all** | operations |  | `futures::join! (Rust's own)` | `all` | `all` | `All` | `all` |
| **race** | operations |  | `futures::select! (Rust's own)` | `race` | `race` | `Race` | `race` |

- **step**: fn may take the step context s.
- **awaitCompletion**: send(cb) receives { token, url }.
- **compensate**: Registers a step that runs, newest first, if the workflow fails or is cancelled.

## Step context

| Operation | Arguments | Options | Rust | TypeScript | Python | Go | Java |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **heartbeat** | progress |  | `ctx.heartbeat` | `heartbeat` | `heartbeat` | `Heartbeat` | `heartbeat` |
| **progress** |  |  | `ctx.progress` | `progress` | `progress` | `Progress` | `progress` |
| **stream** | name |  | `ctx.stream` | `stream` | `stream` | `Stream` | `stream` |

## Errors

The same codes in every SDK; each has a page in [errors.md](errors.md).

- [`NotFound`](errors.md#notfound)
- [`AlreadyExists`](errors.md#alreadyexists)
- [`UnknownWorkflow`](errors.md#unknownworkflow)
- [`StepFailed`](errors.md#stepfailed)
- [`WorkflowFailed`](errors.md#workflowfailed)
- [`ChildFailed`](errors.md#childfailed)
- [`Cancelled`](errors.md#cancelled)
- [`TimedOut`](errors.md#timedout)
- [`Terminated`](errors.md#terminated)
- [`Determinism`](errors.md#determinism)
- [`Rejected`](errors.md#rejected)
- [`Codec`](errors.md#codec)
- [`Connection`](errors.md#connection)
- [`Io`](errors.md#io)
- [`Busy`](errors.md#busy)
- [`Closed`](errors.md#closed)
