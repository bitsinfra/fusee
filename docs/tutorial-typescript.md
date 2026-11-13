# TypeScript quickstart

Ten minutes. You will start a workflow, kill the process in the middle of
it, and watch it carry on from where it was.

Use Node.js 22.18 or later. If you already installed the starter, continue at [Run it](#2-run-it).

## 1. A project

```sh
mkdir hello && cd hello
npm exec --yes --package=@bitsinfra/fusee@1.0.2 -- fusee init --lang ts
npm install
```

The project includes three TypeScript files:

- `workflows.ts`: the workflow, a plain value apart from any engine.
- `app.ts`: opens an engine, starts one run, and keeps working.
- `workflows.test.ts`: a test with the SDK’s test engine.

It also writes `AGENTS.md` (and a `CLAUDE.md` that imports it): the rules a
coding assistant needs to change the workflows safely.

Run the commands below from the project directory.

The workflow composes a greeting in a step, pauses five seconds, then waits
for an approval signal:

```ts
export const approve = signal<boolean>("approve");

export const greet = workflow("greet", async (ctx, name: string) => {
  const greeting = await ctx.step("compose", () => `hello ${name}`);
  await ctx.sleep("pause", "5s");
  const ok = await ctx.waitFor(approve, { timeout: "1h" });
  return ok ? greeting : `${greeting} (not approved)`;
});
```

A step's result is recorded once it runs. When the code runs again after a
crash, `compose` returns its recorded result instead of running again; the
sleep and the wait pick up where they were.

## 2. Run it

```sh
npx --no-install fusee dev
```

`fusee dev` runs `node app.ts` under the dev profile and prints:

```
fusee: dev profile (/path/to/hello/.fusee/dev): every run's history is kept, flushes use fsync (a process crash loses nothing; power loss may). Not for production.
fusee: UI and HTTP API at http://127.0.0.1:7601
started greet-1; approve it with: fusee runs signal greet-1 approve true
```

Open http://127.0.0.1:7601: the run `greet-1` is there, with its `compose`
step done and its pause running. `fusee dev` restarts the app when a file
changes or the app exits.

## 3. Kill it

Within the five seconds of the pause, in another terminal:

```sh
pkill -9 -f app.ts
```

That is a crash: no shutdown, no flush. `fusee dev` starts the app again,
and the run continues from its journal. The UI shows `compose` once; it did
not run a second time.

## 4. Approve it

```sh
npx --no-install fusee runs signal greet-1 approve true
```

The app prints `hello world`. `fusee runs ls` lists the run as `ok`;
`fusee runs show greet-1` prints its timeline.

## 5. Test it

```sh
node --test workflows.test.ts
```

The test opens a `TestEngine` (in memory, under the dev profile), starts the
run, signals it (the signal waits for the workflow to ask for it), and checks
the result with `t.result`, which moves the virtual clock past the
five-second pause: the test takes milliseconds, not five seconds. See
[testing your workflows](testing-workflows.md).

## Next

- [Errors and retries](errors.md): every error code and what to do.
- [Features](features.md): steps, signals, queries, updates, children,
  compensation, schedules, objects, streams, limits.
- [Operations](operations.md) and [deploying](../deploy/README.md): a server,
  replicated groups, monitoring.
