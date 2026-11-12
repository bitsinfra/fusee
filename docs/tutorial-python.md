# Python quickstart

Ten minutes. You will start a workflow, kill the process in the middle of
it, and watch it carry on from where it was.

Before you begin, follow the [Python installation instructions](installation.md#python). If you already started the sample there, continue at [Run it](#2-run-it).

## 1. A project

```sh
mkdir hello && cd hello
fusee init --lang python
```

`fusee init` writes three files:

- `workflows.py`: the workflow, a plain value apart from any engine.
- `app.py`: opens an engine, starts one run, and keeps working.
- `test_workflows.py`: a test with `fusee.testing`.

It also writes `AGENTS.md` (and a `CLAUDE.md` that imports it): the rules a
coding assistant needs to change the workflows safely.

The workflow composes a greeting in a step, pauses five seconds, then waits
for an approval signal:

```python
@fusee.workflow("greet")
async def greet(ctx: fusee.Ctx, name: str) -> str:
    greeting = await ctx.step("compose", lambda: f"hello {name}")
    await ctx.sleep("pause", "5s")
    ok = await ctx.wait_for(approve, timeout="1h")
    return greeting if ok else f"{greeting} (not approved)"
```

A step's result is recorded once it runs. When the code runs again after a
crash, `compose` returns its recorded result instead of running again; the
sleep and the wait pick up where they were.

## 2. Run it

```sh
fusee dev
```

`fusee dev` runs `app.py` under the dev profile and prints:

```
fusee: dev profile (/path/to/hello/.fusee/dev): every run's history is kept, each run is replayed against the code when it parks or ends. Not for production.
fusee: UI and HTTP API at http://127.0.0.1:7601
started greet-1; approve it with: fusee runs signal greet-1 approve true
```

Open http://127.0.0.1:7601: the run `greet-1` is there, with its `compose`
step done and its pause running. `fusee dev` restarts the app when a file
changes or the app exits.

## 3. Kill it

Within the five seconds of the pause, in another terminal:

```sh
pkill -9 -f app.py
```

That is a crash: no shutdown, no flush. `fusee dev` starts the app again,
and the run continues from its journal. The UI shows `compose` once; it did
not run a second time.

## 4. Approve it

```sh
fusee runs signal greet-1 approve true
```

The app prints `hello world`. `fusee runs ls` lists the run as `ok`;
`fusee runs show greet-1` prints its timeline.

## 5. Test it

```sh
python test_workflows.py
```

The test opens a `TestEngine` (in memory, under the dev profile), starts the
run, signals it, and checks the result, the steps it recorded, and that the
code replays against its journal (`t.check()`). See
[testing your workflows](testing-workflows.md).

## Next

- [Errors and retries](errors.md): every error code and what to do.
- [Features](features.md): steps, signals, queries, updates, children,
  compensation, schedules, objects, streams, limits.
- [Operations](operations.md) and [deploying](../deploy/README.md): a server,
  replicated groups, monitoring.
