//! Commands about a project rather than runs: `init`, `dev`, `doctor`,
//! `replay-check`.

use crate::cli::DevArgs;
use fusee::Error;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime};

// ------------------------------------------------------------------ init

struct File {
    path: &'static str,
    body: &'static str,
}

const TS_HELLO: &[File] = &[
    File {
        path: "package.json",
        body: r#"{
  "name": "fusee-app",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "fusee dev",
    "start": "node app.ts",
    "test": "node --test workflows.test.ts"
  },
  "dependencies": {
    "@bitsinfra/fusee": "^1.0.2"
  }
}
"#,
    },
    File {
        path: "workflows.ts",
        body: r#"// Workflows are plain values: an app runs them, a client may import them for their types.
import { signal, workflow } from "@bitsinfra/fusee";

export const approve = signal<boolean>("approve");

export const greet = workflow("greet", async (ctx, name: string) => {
  const greeting = await ctx.step("compose", () => `hello ${name}`);
  // Kill the process here and start it again: the run resumes, and `compose` does not run twice.
  await ctx.sleep("pause", "5s");
  const ok = await ctx.waitFor(approve, { timeout: "1h" });
  return ok ? greeting : `${greeting} (not approved)`;
});
"#,
    },
    File {
        path: "app.ts",
        body: r#"import { Engine } from "@bitsinfra/fusee";
import { greet } from "./workflows.ts";

// FUSEE_DIR picks the directory (`fusee dev` sets it and the dev profile).
const engine = await Engine.open({ workflows: [greet] });
const run = await engine.start(greet, "greet-1", "world", { idPolicy: { running: "use-existing", finished: "use-existing" } });
console.log(`started ${run.id}; approve it with: fusee runs signal ${run.id} approve true`);
console.log(await run.result());
// A worker keeps running: new runs, timers and signals need it.
setInterval(() => {}, 1 << 30);
"#,
    },
    File {
        path: "workflows.test.ts",
        body: r#"import { test } from "node:test";
import assert from "node:assert/strict";
import { TestEngine } from "@bitsinfra/fusee/testing";
import { approve, greet } from "./workflows.ts";

test("greet waits for approval", async () => {
  const t = await TestEngine.open({ workflows: [greet] });
  const run = await t.start(greet, "g-1", "test");
  // Kept until the workflow waits for it, after its five-second pause.
  await run.signal(approve, true);
  // t.result moves the virtual clock to each timer: the pause takes no real time.
  assert.equal(await t.result("g-1"), "hello test");
  await t.close();
});
"#,
    },
];

const PY_HELLO: &[File] = &[
    File {
        path: "workflows.py",
        body: r#""""Workflows are plain values: an app runs them, a client may import them."""
import fusee

approve = fusee.signal("approve", bool)


@fusee.workflow("greet")
async def greet(ctx: fusee.Ctx, name: str) -> str:
    greeting = await ctx.step("compose", lambda: f"hello {name}")
    # Kill the process here and start it again: the run resumes, and
    # `compose` does not run twice.
    await ctx.sleep("pause", "5s")
    ok = await ctx.wait_for(approve, timeout="1h")
    return greeting if ok else f"{greeting} (not approved)"
"#,
    },
    File {
        path: "app.py",
        body: r#"import asyncio

import fusee
from workflows import approve, greet


async def main():
    # FUSEE_DIR picks the directory (`fusee dev` sets it and the dev profile).
    async with await fusee.Engine.open(workflows=[greet]) as engine:
        run = await engine.start(greet, "greet-1", "world",
                                 id_policy={"running": "use-existing", "finished": "use-existing"})
        print(f"started {run.id}; approve it with: fusee runs signal {run.id} approve true")
        print(await run.result())
        # A worker keeps running: new runs, timers and signals need it.
        await asyncio.Event().wait()


asyncio.run(main())
"#,
    },
    File {
        path: "test_workflows.py",
        body: r#"import asyncio

from fusee.testing import TestEngine
from workflows import approve, greet


async def test_greet_waits_for_approval():
    t = await TestEngine.open(workflows=[greet])
    run = await t.engine.start(greet, "g-1", "test")
    # Buffered until the workflow waits for it (after its 5 s pause).
    await run.signal(approve, True)
    # t.result moves the virtual clock to each timer: the pause takes no
    # real time.
    assert await t.result("g-1") == "hello test"
    assert await t.steps("g-1") == ["compose"]
    await t.check()  # strict replay: the code matches its journal
    await t.close()


if __name__ == "__main__":
    asyncio.run(test_greet_waits_for_approval())
    print("ok")
"#,
    },
];

const RUST_HELLO: &[File] = &[
    File {
        path: "Cargo.toml",
        body: r#"[package]
name = "fusee-app"
version = "0.1.0"
edition = "2024"

[dependencies]
fusee = "1.0.2"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
"#,
    },
    File {
        path: "src/main.rs",
        body: r#"use fusee::{Config, Ctx, Engine, Error, Signal, WorkflowRef};
use std::time::Duration;

pub const GREET: WorkflowRef<String, String> = WorkflowRef::new("greet");
pub const APPROVE: Signal<bool> = Signal::new("approve");

#[tokio::main]
async fn main() -> Result<(), Error> {
    // `fusee dev` sets FUSEE_DIR and FUSEE_PROFILE=dev: history kept, each
    // run replayed against this code when it waits or ends, and the UI
    // served (its address is printed).
    let dir = std::env::var("FUSEE_DIR").unwrap_or_else(|_| "./fusee-data".into());
    let engine = Engine::open(Config::local(dir)).await?;
    engine.register(GREET, |ctx: Ctx, name: String| async move {
        let greeting: String = ctx
            .step("compose", || async { Ok::<_, Error>(format!("hello {name}")) })
            .await?;
        // Kill the process here and start it again: the run resumes.
        ctx.sleep("pause", Duration::from_secs(5)).await?;
        let ok = ctx.wait_for(APPROVE).timeout(Duration::from_secs(3600)).await?;
        Ok(if ok == Some(true) { greeting } else { format!("{greeting} (not approved)") })
    });
    let run = engine.start(GREET, "greet-1", &"world".to_string()).use_existing().await?;
    println!("started {}; approve it: fusee runs signal {} approve true", run.id(), run.id());
    println!("{}", run.result().await?);
    // A worker keeps running: new runs, timers and signals need it.
    std::future::pending::<()>().await;
    Ok(())
}
"#,
    },
];

/// What a coding assistant needs to write this project's workflows:
/// `AGENTS.md`, which `CLAUDE.md` imports. Kept short; the guide has the
/// wrong and right version of each rule.
const AGENTS_MD: &str = r#"# Working on this project

The workflows here run on Fusee (durable execution). Read these rules
before changing a workflow; the full guide is
https://fusee.bitsinfra.com/guides/coding-agents.md and every page of the
documentation is in https://fusee.bitsinfra.com/llms-full.txt.

- Workflow code is replayed after a crash: it runs again from the top and
  recorded steps return their results. Await only `ctx` operations in
  workflow code (step, sleep, waitFor/wait_for, child, all, race,
  awaitCompletion); put fetches, queries and other I/O inside a step.
- Read time, randomness and IDs through the context: `ctx.now()`,
  `ctx.random()`, `ctx.uuid()`. Never `Date.now()`, `Math.random()`,
  `time.time()` or `uuid4()` in workflow code.
- A step's function may run more than once. Pass its key (`s.key`, or
  `ctx.key()` in Rust) to every service that takes an idempotency key.
- Keep step names fixed and in order. Changing the steps of a workflow that
  has runs in flight needs a new version or `ctx.patched("id")`.
- A failure that retrying cannot fix is a `FatalError` (`fusee.Fatal` in
  Python, `Failure::fatal` in Rust). Undo earlier steps with
  `ctx.compensate(name, fn)`.
- Test with the test kit (`@bitsinfra/fusee/testing`, `fusee.testing`, the `testing`
  feature in Rust): virtual time, and `crashEverywhere` /
  `crash_everywhere` to find steps that repeat their effects.
- `fusee dev` runs the app with the UI; `fusee runs ls`, `fusee runs show
  <id>` and `fusee runs resume <id>` inspect and repair runs.
"#;

pub fn init(template: &str, lang: Option<&str>, path: &Path) -> Result<(), Error> {
    if template != "hello" {
        return Err(Error::Rejected(format!(
            "template '{template}' is not available yet; the templates are: hello"
        )));
    }
    let lang = match lang {
        Some(l) => l.to_owned(),
        None if path.join("pyproject.toml").exists() || path.join("requirements.txt").exists() => {
            "python".into()
        }
        None if path.join("Cargo.toml").exists() => "rust".into(),
        None => "typescript".into(),
    };
    let files = match lang.as_str() {
        "typescript" | "ts" | "node" => TS_HELLO,
        "python" | "py" => PY_HELLO,
        "rust" | "rs" => RUST_HELLO,
        other => {
            return Err(Error::Rejected(format!(
                "--lang is typescript, python or rust, not '{other}'"
            )));
        }
    };
    for f in files {
        let p = path.join(f.path);
        if p.exists() {
            return Err(Error::Rejected(format!(
                "{} exists; `fusee init` writes into an empty project",
                p.display()
            )));
        }
    }
    for f in files {
        let p = path.join(f.path);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&p, f.body)?;
        println!("  wrote {}", p.display());
    }
    // Instructions for coding assistants, unless the project has its own.
    for (name, body) in [("AGENTS.md", AGENTS_MD), ("CLAUDE.md", "@AGENTS.md\n")] {
        let p = path.join(name);
        if p.exists() {
            println!(
                "  kept {} (Fusee's rules: https://fusee.bitsinfra.com/guides/coding-agents.md)",
                p.display()
            );
        } else {
            std::fs::write(&p, body)?;
            println!("  wrote {}", p.display());
        }
    }
    let ignore = path.join(".gitignore");
    if !ignore.exists() {
        std::fs::write(
            &ignore,
            ".fusee/\nfusee-data/\nnode_modules/\ntarget/\n__pycache__/\n",
        )?;
    }
    let next = match lang.as_str() {
        "python" | "py" => "pip install fusee && fusee dev",
        "rust" | "rs" => "fusee dev -- cargo run",
        _ => "npm install && npx fusee dev",
    };
    println!(
        "\nNext: {next}\nThen kill the app mid-workflow (Ctrl-C) and run `fusee dev` again: the run resumes."
    );
    Ok(())
}

// ------------------------------------------------------------------ dev

fn app_command(dir: &Path) -> Option<Vec<String>> {
    let has = |f: &str| dir.join(f).exists();
    let v = |a: &[&str]| Some(a.iter().map(|s| s.to_string()).collect());
    if has("app.py") {
        return v(&["python3", "app.py"]);
    }
    if has("app.ts") {
        return v(&["node", "app.ts"]);
    }
    if has("app.mjs") {
        return v(&["node", "app.mjs"]);
    }
    if has("app.js") {
        return v(&["node", "app.js"]);
    }
    if has("main.py") {
        return v(&["python3", "main.py"]);
    }
    if has("Cargo.toml") {
        return v(&["cargo", "run", "--quiet"]);
    }
    None
}

const SKIP: &[&str] = &[
    ".fusee",
    "node_modules",
    "target",
    ".git",
    "__pycache__",
    "dist",
    ".venv",
];
const WATCH: &[&str] = &["py", "ts", "mts", "js", "mjs", "rs", "toml", "json"];

/// The newest modification among the project's source files.
fn newest(dir: &Path, depth: u32) -> SystemTime {
    let mut t = SystemTime::UNIX_EPOCH;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return t;
    };
    for e in entries.flatten() {
        let name = e.file_name();
        let name = name.to_string_lossy();
        if SKIP.contains(&name.as_ref()) || name.starts_with('.') {
            continue;
        }
        let p = e.path();
        if p.is_dir() {
            if depth < 6 {
                t = t.max(newest(&p, depth + 1));
            }
        } else if p
            .extension()
            .and_then(|x| x.to_str())
            .is_some_and(|x| WATCH.contains(&x))
            && let Ok(m) = e.metadata().and_then(|m| m.modified())
        {
            t = t.max(m);
        }
    }
    t
}

fn spawn(cmd: &[String], data: &Path, http: &str) -> std::io::Result<Child> {
    Command::new(&cmd[0])
        .args(&cmd[1..])
        .env("FUSEE_PROFILE", "dev")
        .env("FUSEE_DIR", data)
        .env("FUSEE_HTTP", http)
        // The app's output as it happens, also when `fusee dev` is piped
        // (`| tee dev.log`): Python buffers a pipe's output otherwise.
        .env("PYTHONUNBUFFERED", "1")
        .stdin(Stdio::inherit())
        .spawn()
        .map_err(|e| std::io::Error::new(e.kind(), format!("cannot run `{}`: {e}", cmd.join(" "))))
}

fn stop(child: &mut Child) {
    #[cfg(unix)]
    {
        // SIGTERM first: an engine closes cleanly; then kill.
        unsafe {
            libc_kill(child.id() as i32, 15);
        }
        let until = Instant::now() + Duration::from_secs(3);
        while Instant::now() < until {
            if let Ok(Some(_)) = child.try_wait() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

pub async fn dev(a: DevArgs) -> Result<(), Error> {
    let cwd = std::env::current_dir()?;
    let data = if a.data.is_absolute() {
        a.data.clone()
    } else {
        cwd.join(&a.data)
    };
    std::fs::create_dir_all(&data)?;
    let url = format!("http://{}", a.http);
    let cmd = if a.command.is_empty() {
        app_command(&cwd)
    } else {
        Some(a.command.clone())
    };
    let Some(cmd) = cmd else {
        // No app: serve the directory itself, so the UI shows its runs.
        println!(
            "fusee dev: no app here (app.py, app.ts, app.mjs, Cargo.toml); serving {} without code",
            data.display()
        );
        println!("fusee dev: `fusee init` creates one; `fusee dev -- <command>` runs another");
        // Without the app, nothing expires: retention is the app's.
        let config = crate::cli::tool_config(&data).dev().http(&a.http);
        let engine = fusee::Engine::open(config).await?;
        let _ = tokio::signal::ctrl_c().await;
        engine.shutdown().await?;
        return Ok(());
    };
    println!(
        "fusee dev: `{}` under the dev profile, data in {}",
        cmd.join(" "),
        data.display()
    );
    let mut child = spawn(&cmd, &data, &a.http)?;
    let ui = url.clone();
    tokio::spawn(async move {
        if crate::cli::wait_for(&ui, Duration::from_secs(30)).await {
            println!("fusee dev: UI at {ui}");
        }
    });
    let mut seen = newest(&cwd, 0);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                stop(&mut child);
                println!("\nfusee dev: stopped. Runs stay in {}; `fusee dev` again resumes them.", data.display());
                return Ok(());
            }
            _ = term.recv() => {
                stop(&mut child);
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_millis(400)) => {}
        }
        if let Ok(Some(status)) = child.try_wait() {
            if a.no_watch {
                return if status.success() {
                    Ok(())
                } else {
                    Err(Error::Io(format!("the app exited with {status}")))
                };
            }
            if !status.success() {
                // A crash (kill -9 included): bring it back; its runs resume
                // from their journals.
                println!(
                    "fusee dev: the app exited ({status}); starting it again, its runs resume"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
                child = spawn(&cmd, &data, &a.http)?;
                continue;
            }
            // Wait for a change before running it again.
            let now = newest(&cwd, 0);
            if now > seen {
                seen = now;
                println!("fusee dev: a file changed; starting the app again");
                child = spawn(&cmd, &data, &a.http)?;
            }
            continue;
        }
        if a.no_watch {
            continue;
        }
        let now = newest(&cwd, 0);
        if now > seen {
            seen = now;
            println!("fusee dev: a file changed; restarting the app (its runs resume)");
            stop(&mut child);
            child = spawn(&cmd, &data, &a.http)?;
        }
    }
}

// ------------------------------------------------------------------ doctor

pub async fn doctor(dir: Option<PathBuf>) -> Result<(), Error> {
    let dir = dir
        .or_else(|| std::env::var_os("FUSEE_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("./fusee-data"));
    let mut problems = 0;
    let mut ok = |good: bool, what: String| {
        println!("  {} {what}", if good { "ok  " } else { "FAIL" });
        if !good {
            problems += 1;
        }
    };
    println!(
        "fusee {} on {}",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS
    );
    println!("\nData directory {}", dir.display());
    if !dir.exists() {
        println!("  -    does not exist yet (an engine creates it)");
    } else {
        match fusee::engine::read_meta(&dir) {
            Ok(Some((shards, log))) => ok(true, format!("format: log {log}, {shards} shards")),
            Ok(None) => ok(true, "no runs written yet".into()),
            Err(e) => ok(false, format!("format: {e}")),
        }
        match fusee::local::lock_directory(&dir) {
            Ok(_) => ok(true, "not open in another process".into()),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                println!("  info {e}");
            }
            Err(e) => ok(false, format!("lock: {e}")),
        }
        match fusee::disk::disk_check(&dir, Duration::from_millis(300)) {
            Ok(r) => {
                for p in &r.probes {
                    println!(
                        "  info flush {}: p50 {:.0} µs, p99 {:.0} µs",
                        p.method, p.p50_us, p.p99_us
                    );
                }
                for n in &r.notes {
                    println!("  note {n}");
                }
            }
            Err(e) => ok(false, format!("disk check: {e}")),
        }
    }
    println!("\nConfiguration");
    for v in [
        "FUSEE_DIR",
        "FUSEE_URL",
        "FUSEE_PROFILE",
        "FUSEE_HTTP",
        "FUSEE_TOKEN_FILE",
    ] {
        match std::env::var(v) {
            Ok(x) => println!("  info {v}={x}"),
            Err(_) => println!("  -    {v} not set"),
        }
    }
    if cfg!(target_os = "macos") {
        println!(
            "  note macOS flushes with F_FULLFSYNC (survives power loss, ~4 ms); the dev profile uses fsync"
        );
    }
    if problems > 0 {
        return Err(Error::Io(format!("{problems} problem(s) above")));
    }
    println!("\nNo problems found.");
    Ok(())
}

// ------------------------------------------------------------------ replay-check

/// The replay scripts' exit status when a journal differs from the code.
const REPLAY_DIFFER: i32 = 3;

const PY_REPLAY: &str = r#"
import asyncio, importlib.util, sys, fusee
spec = importlib.util.spec_from_file_location("workflows_under_check", sys.argv[1])
mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
defs = [v for v in vars(mod).values() if isinstance(v, fusee.Workflow)]
async def main():
    bad = 0
    rep = fusee.Replayer(workflows=defs)
    for f in sys.argv[2:]:
        r = await rep.replay_file(f)
        print(("ok   " if r.ok else "FAIL ") + f + ("" if r.ok else ": " + str(r.error)))
        bad += not r.ok
    sys.exit(DIFFER if bad else 0)
DIFFER = 3
asyncio.run(main())
"#;

const JS_REPLAY: &str = r#"
import { pathToFileURL } from "node:url";
const fusee = await import("fusee");
// Under `node -e` the arguments start at argv[1]: the module, then the journals.
const mod = await import(pathToFileURL(process.argv[1]).href);
// A workflow definition is a plain value (`workflow(...)`), with its code.
const defs = Object.values(mod).filter((v) => v?.kind === "workflow" && typeof v.fn === "function");
const rep = new fusee.Replayer({ workflows: defs });
let bad = 0;
for (const f of process.argv.slice(2)) {
  const r = await rep.replayFile(f);
  console.log((r.ok ? "ok   " : "FAIL ") + f + (r.ok ? "" : ": " + r.error));
  if (!r.ok) bad++;
}
process.exit(bad ? 3 : 0);
"#;

pub fn replay_check(journals: &[PathBuf], module: &Path) -> Result<(), Error> {
    let mut files = Vec::new();
    for j in journals {
        if j.is_dir() {
            let mut v: Vec<PathBuf> = std::fs::read_dir(j)?
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "fjnl"))
                .collect();
            v.sort();
            files.extend(v);
        } else {
            files.push(j.clone());
        }
    }
    if files.is_empty() {
        return Err(Error::Rejected(
            "no .fjnl files to replay (export them with `fusee runs export`)".into(),
        ));
    }
    let ext = module.extension().and_then(|x| x.to_str()).unwrap_or("");
    let mut cmd = match ext {
        "py" => {
            let mut c = Command::new("python3");
            c.arg("-c").arg(PY_REPLAY).arg(module);
            c
        }
        "js" | "mjs" | "ts" | "mts" => {
            let mut c = Command::new("node");
            c.arg("--input-type=module")
                .arg("-e")
                .arg(JS_REPLAY)
                .arg(module);
            c
        }
        other => {
            return Err(Error::Rejected(format!(
                "--module is a .py, .js, .mjs or .ts file, not '.{other}'"
            )));
        }
    };
    cmd.args(&files);
    let status = cmd.status()?;
    match status.code() {
        Some(0) => {
            println!(
                "{} journal(s) replay against {}",
                files.len(),
                module.display()
            );
            Ok(())
        }
        // The scripts exit with REPLAY_DIFFER when a journal differs; any
        // other failure is the script's (the module did not load, ...).
        Some(REPLAY_DIFFER) => Err(Error::Determinism(format!(
            "some journals differ from the code in {}: deploying it would make their runs stuck",
            module.display()
        ))),
        _ => Err(Error::Io(format!(
            "the replay of {} did not run ({status}); its output is above",
            module.display()
        ))),
    }
}
