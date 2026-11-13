# Writing Fusee code with an AI assistant

Claude, Cursor, Codex and the others write Fusee code well once they know
the handful of rules that make a workflow replay. This page is those rules,
each with the version a model tends to write and the one that works. Point
your assistant here, or at the whole documentation as one file:
[`llms-full.txt`](https://fusee.bitsinfra.com/llms-full.txt) (the index is
[`llms.txt`](https://fusee.bitsinfra.com/llms.txt)), or at one page as
Markdown by adding `.md` to its address
(`https://fusee.bitsinfra.com/guides/sagas.md`). A project made with
`fusee init` has the same rules in its `AGENTS.md`, which a `CLAUDE.md`
imports.

## Workflow code is replayed

After a crash, the workflow function runs again from the top against its
journal: recorded steps return their results without running. So the code
between `ctx` calls must make the same decisions every time.

**Await only `ctx` operations in workflow code.** Anything else (a fetch, a
database query, a sleep with `setTimeout` or `asyncio.sleep`) goes inside a
step.

```ts
// Wrong: the fetch runs again on every replay, and its answer is not recorded.
const user = await fetch(`/users/${id}`).then((r) => r.json());
// Right:
const user = await ctx.step("load-user", () => fetch(`/users/${id}`).then((r) => r.json()));
```

**Read time, randomness and IDs through the context.** They are recorded,
so a replay sees the values the first run saw.

```python
# Wrong: a replay gets another time and another ID.
started = time.time()
order_id = str(uuid.uuid4())
# Right:
started = ctx.now()
order_id = ctx.uuid()
```

**Keep step names stable and in the same order.** Steps are matched to the
journal by position and name. Renaming or reordering steps of a workflow
that has runs in flight is a determinism error: register the change as a
new version, or guard it with `ctx.patched("id")` so older runs take the
old path.

## A step's function may run more than once

A step runs again if the process died after its function ran but before
its result was durable. Pass the step's key to every service that accepts
an idempotency key, and the effect happens once.

```ts
// Wrong: a retry after a crash charges twice.
await ctx.step("charge", () => stripe.charges.create({ amount, customer }));
// Right:
await ctx.step("charge", (s) => stripe.charges.create({ amount, customer }, { idempotencyKey: s.key }));
```

```python
await ctx.step("charge", lambda s: stripe.Charge.create(amount=amount, customer=customer, idempotency_key=s.key))
```

## Failures

- Errors thrown in a step are retried (three attempts by default). A
  failure that retrying cannot fix is a `FatalError` (`fusee.Fatal` in
  Python, `Failure::fatal` in Rust): a declined card, a validation error.
- `retry: { attempts, initial, max, maxElapsed }` sets the policy;
  `onExhausted: "pause"` makes a run wait for `fusee runs resume` instead of
  failing when the retries run out.
- To undo earlier steps when a later one fails, register compensations
  with `ctx.compensate(name, fn)`; they run newest first.

## Values

Step results, inputs and signals are stored as JSON. A `Date` comes back as
a string, a `Map` or a class instance as a plain object, unless the engine
opens with the `rich` serializer (`@bitsinfra/fusee/rich`). In Python, annotate
inputs and outputs with dataclasses or pydantic models to get them back
typed.

## Testing

Use the test kit, not a real engine with sleeps: virtual time skips
`ctx.sleep`, and `crashEverywhere` (`crash_everywhere`) runs a workflow
once per step boundary with a crash there, finding steps that forget their
idempotency key. See [testing your workflows](../testing-workflows.md).

## Checklist for a reviewer

- Every `await` in workflow code is a `ctx` call.
- No `Date.now()`, `Math.random()`, `time.time()`, `uuid4()` or environment
  reads in workflow code outside a step.
- Every step that changes something outside passes `s.key` on.
- Step names are fixed strings, or built only from the input and earlier
  step results.
- Final failures are `FatalError`; everything else may be retried.
- A change to the steps of a workflow with runs in flight is a new version
  or a patch.
