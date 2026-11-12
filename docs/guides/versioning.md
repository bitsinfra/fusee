# Versioning and deploys

A run that started last month replays its journal against today's code.
Changing workflow code is safe when the new code makes the same journal
operations, in the same order, for the runs that already passed that
point. This guide is how to change code anyway.

## What changes are safe

- Anything inside a step's function: the step runs again only if it never
  finished, and its recorded result is used otherwise.
- New code after the point every running run has passed.
- Queries, logging, and anything that reads without making journal
  operations.

Renaming a step, removing one, adding one before others, or changing the
order of steps is not safe for running runs. Fusee notices: the run stops
(`stuck`, with a `Determinism` error naming the step that differs), keeps
its state, and waits for code that matches. Nothing is lost; the [errors
page](../errors.md#determinism) lists the ways out.

## Three ways to change code

**A patch** keeps both paths in one function:

```ts
const fee = ctx.patched("refund-rounding") ? Math.round(x) : Math.floor(x);
```

`patched` is true for new runs and for running ones the first time their
code reaches it after the deploy, false for runs that passed it before.
Once no running run predates the patch, replace it with
`ctx.deprecatePatch("refund-rounding")` and keep the new branch; later,
delete the call.

**A version** keeps two functions:

```ts
const checkoutV1 = workflow("checkout", { version: 1 }, oldCode);
const checkoutV2 = workflow("checkout", { version: 2 }, newCode);
```

New runs start under the newest registered version; a run keeps the
version it started under. `fusee versions` lists live runs per type and
version: when v1 has none, delete its code.

**A reset** moves one run onto the new code:
`fusee runs reset order-7 --before charge` starts it again from its
journal cut just before `charge`, with the signals it received after.

## Before the deploy

Replay a sample of real journals against the new code in CI:

```sh
fusee export --sample 1000 ./data -o journals/
fusee replay-check journals/ --module ./dist/workflows.js
```

`replay-check` runs the new code against each exported journal without
running any step, and names the first difference for each run that would
get stuck. Under the dev profile every run is replayed against the code
as it parks or ends, so a change that breaks running runs fails in
development, at the line that changed.

## During the deploy

Old and new processes can run side by side: each run is held by one
process at a time, and a run waiting for code that only the new version
has (`waiting_for_code`) waits until a process with that code takes its
shard. Roll forward; to roll back, keep the patch branches until no run
needs them.
