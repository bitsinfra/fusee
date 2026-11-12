# Rust quickstart

Ten minutes. You will start a workflow, kill the process in the middle of
it, and watch it carry on from where it was.

Before you begin, follow the [Rust installation instructions](installation.md#rust). If you already started the sample there, continue at [Run it](#2-run-it).

## 1. A project

```sh
mkdir hello && cd hello
fusee init --lang rust
```

`fusee init` writes `Cargo.toml` and `src/main.rs`, and `AGENTS.md` (which a
`CLAUDE.md` imports) with the rules a coding assistant needs. The workflow is a
reference (`WorkflowRef`) and a function:

```rust
pub const GREET: WorkflowRef<String, String> = WorkflowRef::new("greet");
pub const APPROVE: Signal<bool> = Signal::new("approve");

engine.register(GREET, |ctx: Ctx, name: String| async move {
    let greeting: String = ctx
        .step("compose", || async { Ok::<_, Error>(format!("hello {name}")) })
        .await?;
    ctx.sleep("pause", Duration::from_secs(5)).await?;
    let ok = ctx.wait_for(APPROVE).timeout(Duration::from_secs(3600)).await?;
    Ok(if ok == Some(true) { greeting } else { format!("{greeting} (not approved)") })
});
```

A step's result is recorded once it runs. When the code runs again after a
crash, `compose` returns its recorded result instead of running again; the
sleep and the wait pick up where they were.

## 2. Run it

```sh
fusee dev -- cargo run
```

Under `fusee dev` the engine runs with the dev profile (`FUSEE_PROFILE=dev`):
it keeps every run's history, replays each run against the code when it
waits or ends, and serves the UI:

```
fusee: dev profile (/path/to/hello/.fusee/dev): every run's history is kept, each run is replayed against the code when it waits or ends. Not for production.
fusee: UI and HTTP API at http://127.0.0.1:7601
started greet-1; approve it: fusee runs signal greet-1 approve true
```

Open http://127.0.0.1:7601 to see the run.

## 3. Kill it

Within the five seconds of the pause, in another terminal:

```sh
pkill -9 fusee-app
```

`fusee dev` starts the app again; the run continues from its journal, and
`compose` does not run twice.

## 4. Approve it

```sh
fusee runs signal greet-1 approve true
```

The app prints `hello world`.

## 5. Test it

Move the workflow's closure into a function,
`async fn greet(ctx: Ctx, name: String) -> fusee::Result<String>`, so a test
can register it too, and add the test kit to `Cargo.toml`:

```toml
[dev-dependencies]
fusee = { path = "../fusee/crates/fusee", features = ["testing"] }
```

```rust
#[tokio::test]
async fn greets_after_approval() -> fusee::Result<()> {
    let t = fusee::testing::TestEngine::open().await?;
    t.register(GREET, greet);
    let run = t.engine().start(GREET, "g-1", &"test".to_string()).await?;
    run.signal(APPROVE, &true).await?;
    // The five-second pause is skipped: virtual time.
    assert_eq!(t.result::<String>("g-1").await?, "hello test");
    t.check().await // strict replay
}
```

See [testing your workflows](testing-workflows.md).

## Next

- [Errors and retries](errors.md), [features](features.md),
  [operations](operations.md), [deploying](../deploy/README.md).
