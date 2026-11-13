# Errors

Every SDK raises the same errors, named by the same codes. Each error
carries its `code`, the IDs involved, and `docs`, a link to its section
here. A failure that application code raised (a step's, a workflow's, an
update's) travels as a [failure envelope](#the-failure-envelope), so its
type, code and details reach the caller in any language.

| Code | Raised when | Retried by a step's policy |
| --- | --- | --- |
| [`NotFound`](#notfound) | no run has this workflow ID | — |
| [`AlreadyExists`](#alreadyexists) | a start's ID is taken and its ID policy refuses | — |
| [`UnknownWorkflow`](#unknownworkflow) | no code is registered for the type | — |
| [`StepFailed`](#stepfailed) | a step failed after its last attempt | — |
| [`WorkflowFailed`](#workflowfailed) | a workflow, update or object handler failed | — |
| [`ChildFailed`](#childfailed) | a child workflow failed | — |
| [`Cancelled`](#cancelled) | cancellation reached the operation | yes |
| [`TimedOut`](#timedout) | a step, wait or workflow deadline passed | yes |
| [`Terminated`](#terminated) | the run was terminated | no |
| [`Determinism`](#determinism) | the code no longer matches the run's journal | no |
| [`Rejected`](#rejected) | a request was refused and nothing was recorded | no |
| [`Codec`](#codec) | a value cannot be encoded or decoded | no |
| [`Connection`](#connection) | no connection to a Fusee server | yes |
| [`Io`](#io) | a disk or network failure | yes |
| [`Busy`](#busy) | the shard's queue is full | yes |
| [`Closed`](#closed) | the engine is closed | no |

## The failure envelope

A failed attempt, step, update, object call or workflow records its failure
as JSON, with absent fields left out:

```json
{ "message": "card declined", "type": "CardDeclined", "code": "card_declined",
  "details": { "last4": "4242" }, "retryable": false, "stack": "...", "cause": null }
```

- `type` is the error's class or type name, left out for a language's plain
  error type. Fusee's own errors use their code (`StepFailed`,
  `ChildFailed`, ...) so every SDK reads them back as the same error.
- `code` and `details` are the application's: set them on the error you
  throw (TypeScript, Python) or on the `Failure` you return (Rust).
- `retryable: false` makes the failure final: no retry policy retries it.
- `stack` is recorded by default, cut to 4 KiB; turn it off with
  `recordStacks: false` where stacks must not be journaled. Rust records
  none unless asked.
- `cause` is the failure's cause, with the same fields. A step's failure
  that ends a workflow is its `StepFailed` envelope with the step's failure
  as `cause`, so `ChildFailed` → `StepFailed` → the original error survives
  any number of levels.

A successful step records nothing of this: the envelope costs only
failures.

## Retries

A step's failure is retried under its retry policy (three attempts by
default, about 100 ms and 200 ms apart, with jitter) unless:

- the failure is marked final: `FatalError` (TypeScript), `fusee.Fatal`
  (Python), `Failure::fatal` (Rust), or any error with `retryable = false`;
- its type, or a cause's, is in the policy's `nonRetryable` list;
- the policy's `retryIf(failure, attempt)` returns false;
- the next retry would start after the policy's `maxElapsed` from the first
  failure;
- the step's result cannot be encoded ([`Codec`](#codec)), since every
  attempt would fail the same way.

The delay taken is recorded, so a replay waits for the same time.

## NotFound

No run with this workflow ID is known: it was never started, or its
retention ended. Check the ID; `list` shows the runs that exist.

## AlreadyExists

A start named a workflow ID that is taken, and the start's ID policy
refuses. The message names the run that holds the ID and whether it is
still running. To attach to a running run instead (a client that retries a
start should), start with the ID policy `running = use-existing`. To run a
finished ID again, use `finished = allow`.

## UnknownWorkflow

No code is registered for the workflow type in this process. The message
lists the registered types and the nearest one when the name looks like a
typo. Register the workflow before starting it, or start it where its code
runs. Where another worker may hold the code (server mode, worker threads),
the run waits with status `waiting_for_code` instead.

## StepFailed

A step failed after its last attempt, or with a failure that is not retried.
The error carries the step's name and its failure (`cause`), with the type,
code and details the step's error had.

## WorkflowFailed

The workflow, an update or an object's handler failed with this failure.
Its fields are the error the code raised.

## ChildFailed

A child workflow failed. The error carries the child's workflow ID and its
failure as `cause`.

## Cancelled

Cancellation reached this operation, with the reason given to `cancel`.
Workflow code may catch it to compensate.

## TimedOut

A timeout passed: a step attempt's `timeout` or `heartbeatTimeout`, a wait's
timeout, or the workflow's deadline. Step timeouts are retried under the
step's policy.

## Terminated

The run was terminated. Nothing runs after it, not even compensation.

## Determinism

The run's code makes different operations than its journal records: a step
was renamed, added or removed before others, or the code reads the clock or
randomness without going through the context. The run stops (status
`stuck`), one log line names the difference, and it waits, unchanged, until
one of these:

1. deploy code that matches its journal;
2. guard the change with `patched("id")`, so runs that started before it take
   the old path;
3. reset the run to before the step that differs.

Under the `dev` profile and in tests, `result()` rejects with this error at
once instead of waiting.

### key() outside a step

`key()` names the step whose function is running. Called anywhere else there
is no step to key, and every such call would get the same key, so unrelated
effects would look like retries of one another. The run stops with this
message. Call it inside the step's function; in TypeScript and Python, use
the step context's `s.key`.

## Rejected

A request was refused and nothing was recorded: an update's validator
refused it, a completion token is stale, a schedule's spec is invalid (the
message names the schedule), or an argument is out of range.

## Codec

A value could not be encoded or decoded. A step whose result cannot be
encoded fails at once, without retries, naming the step (and in TypeScript
and Python, the path to the value). Return plain data: objects, arrays,
strings, numbers, booleans and null. For dates, maps, sets and big integers
in TypeScript, open the engine with the `rich` serializer:
`Engine.open({ serializer: rich })`, with `import { rich } from "@bitsinfra/fusee/rich"`.

## Connection

No Fusee server answers at the address, or the connection was lost. The
message names the address. Start one with `fusee server --dir ...`, or check
`FUSEE_URL`.

## Io

A disk or network failure. The message names the path. After a failed flush
the shard stops, since acknowledging work it could not make durable would
break the guarantees. A directory that another process has open says so,
with that process's pid: one process writes a directory; use server mode for
several.

## Busy

The shard's queue is full and nothing was recorded. Retry later, or raise
the queue size.

## Closed

The engine was closed. Open a new one.
