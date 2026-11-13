// Engine.open({ workers }): workflow code on worker threads, routed by
// workflow ID from the thread that opened the engine.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { threadId } from "node:worker_threads";
import { Engine, FuseeError, RejectedError, StepError, TerminatedError, CancelledError } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-workers-"));
const module = new URL("./workers.module.mjs", import.meta.url);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function eventually(what, f) {
  for (let i = 0; i < 2000; i++) {
    const v = await f();
    if (v) return v;
    await sleep(5);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/** Runs `where` for each ID: which worker thread ran it, by ID. */
async function threadsOf(engine, ids) {
  const where = engine.workflow("where");
  const out = await Promise.all(ids.map(async (id) => (await where.start(id, id)).result()));
  return new Map(out.map((o) => [o.id, o.thread]));
}

test("calls are routed to the worker that owns the ID's shard", async () => {
  const engine = await Engine.open({ durability: "memory", workers: 4, module });
  try {
    const ids = Array.from({ length: 64 }, (_, i) => `route-${i}`);
    const threads = await threadsOf(engine, ids);
    const runs = engine.list(1000);
    assert.equal(runs.length, ids.length);
    // One engine with all four shards lists them in shard order.
    assert.ok(runs.every((r, i) => i === 0 || runs[i - 1].shard <= r.shard));
    // Shard s belongs to worker s % 4: one thread per shard, four in all,
    // none of them this one.
    const byShard = new Map();
    for (const r of runs) {
      const t = threads.get(r.id);
      assert.notEqual(t, threadId);
      assert.equal(byShard.get(r.shard) ?? t, t);
      byShard.set(r.shard, t);
    }
    assert.equal(new Set(byShard.values()).size, 4);
    assert.equal(engine.shardMetrics().length, 4);
    assert.deepEqual(engine.list({ where: { status: "ok" }, limit: 1000 }).length, ids.length);
    const detail = engine.run("route-3").inspect();
    assert.equal(detail.status, "ok");
    assert.equal(detail.id, "route-3");
    assert.ok(detail.journal.length > 0);
    assert.ok(engine.exportJournal("route-3")?.length > 0);
    assert.equal(engine.run("nope").inspect(), null);

    // Signals, queries, updates (and a refused one), cancel and terminate.
    const counter = engine.workflow("counter");
    const c = await counter.start("counter-1", null);
    assert.equal(await c.update("add", 2), 2);
    assert.equal(await c.update("add", 3, { id: "u3" }), 5);
    assert.equal(await c.update("add", 3, { id: "u3" }), 5);
    await assert.rejects(c.update("add", -1), (e) => e instanceof RejectedError && /positive only/.test(e.message));
    assert.equal(await c.query("n"), 5);
    await assert.rejects(c.query("missing"), RejectedError);
    await c.signal("stop", null);
    assert.equal(await c.result(), 5);
    const { result } = await counter.updateWithStart("counter-2", null, "add", 7);
    assert.equal(result, 7);
    await engine.run("counter-2").terminate("enough");
    await assert.rejects(engine.run("counter-2").result(), TerminatedError);
    const w = await engine.workflow("waiter").start("waiter-c", null);
    await w.cancel("stop waiting");
    await assert.rejects(w.result(), FuseeError);

    // Errors and bytes cross with their class and type.
    await assert.rejects((await engine.workflow("boom").start("boom-1", null)).result(), (e) => e instanceof FuseeError && /kaput/.test(e.message));
    const dup = await engine.workflow("waiter").start("dup-1", null);
    await assert.rejects(engine.workflow("waiter").start("dup-1", null), (e) => e instanceof FuseeError && e.code === "Rejected");
    await dup.signal("go", 0);
    await dup.result();
    await assert.rejects(engine.run("never").result(), (e) => e instanceof FuseeError && e.code === "NotFound");
    const bytes = await (await engine.workflow("bytes").start("bytes-1", new Uint8Array([1, 2]))).result();
    assert.ok(Buffer.isBuffer(bytes));
    assert.deepEqual([...bytes], [1, 2, 9]);

    // A completion token is routed by the workflow ID inside it.
    const a = await engine.workflow("async").start("async-1", null);
    const token = await eventually("the completion token", () => a.query("token"));
    await engine.complete(token, { paid: true });
    assert.deepEqual(await a.result(), { paid: true });
    await assert.rejects(engine.complete("not-a-token", 1), RejectedError);

    // Without a function, a handle; code belongs in the module.
    assert.throws(() => engine.workflow("x", async () => 1), /module/);
  } finally {
    await engine.close();
  }
});

test("children, signals and starts between runs on different workers", async () => {
  const engine = await Engine.open({ durability: "memory", workers: 3, module });
  try {
    // Children spread over every worker.
    const kids = Array.from({ length: 12 }, (_, i) => `kid-${i}`);
    const p = await engine.workflow("parent").start("parent-1", kids);
    const threads = await p.result();
    assert.equal(threads.length, 12);
    assert.equal(new Set(threads).size, 3);
    // Signals from runs on one worker to runs on another (shard s belongs
    // to worker s % 3). Probe runs show which shard each candidate ID has.
    const candidates = Array.from({ length: 24 }, (_, i) => `ping-${i}`);
    await threadsOf(engine, candidates);
    const shardOf = new Map(engine.list(1000).map((r) => [r.id, r.shard]));
    let pairs = 0;
    for (let k = 0; k < 3; k++) {
      const waiterId = `pair-waiter-${k}`;
      const w = await engine.workflow("waiter").start(waiterId, null);
      const worker = engine.run(waiterId).inspect().shard % 3;
      const from = candidates.find((id) => shardOf.get(id) % 3 !== worker && !id.endsWith("used"));
      assert.ok(from, "a sender ID on another worker");
      candidates[candidates.indexOf(from)] = `${from}-used`;
      // A finished probe's ID can start again.
      await engine.workflow("pinger").start(from, waiterId);
      assert.equal((await w.result()).v, `from ${from}`);
      pairs += 1;
    }
    assert.equal(pairs, 3);

    // A workflow starts another through its own engine; when another
    // worker owns that ID, the call goes there through this thread.
    const starts = Array.from({ length: 6 }, (_, i) => engine.workflow("starter").start(`starter-${i}`, `started-${i}`));
    const results = await Promise.all((await Promise.all(starts)).map((r) => r.result()));
    assert.ok(results.every((r, i) => r.x === "started" && r.id === `started-${i}`));
    const owners = new Set(engine.list(1000).filter((r) => r.id.startsWith("started-") || r.id.startsWith("starter-")).map((r) => r.shard % 3));
    assert.ok(owners.size > 1);
  } finally {
    await engine.close();
  }
});

test("a schedule on one worker starts runs on every worker", async () => {
  const engine = await Engine.open({ durability: "memory", workers: 3, module });
  try {
    const s = await engine.schedule("ticker", { every: "20ms" }, "where", "tick");
    const runs = await eventually("runs on every worker", () => {
      const done = engine.list({ where: { type: "where", status: "ok" }, limit: 1000 });
      return new Set(done.map((r) => r.shard % 3)).size === 3 && done;
    });
    assert.ok(runs.every((r) => r.id.startsWith("ticker@")), runs.map((r) => r.id).join());
    const d = await s.describe();
    assert.ok(d.started >= 3, JSON.stringify(d));
    await s.pause();
    await s.delete();
    await assert.rejects(engine.schedule("bad", { cron: "nope" }, "where"), /cron/);
  } finally {
    await engine.close();
  }
});

test("a limit defined on the main thread holds across workers", async () => {
  const engine = await Engine.open({ durability: "memory", workers: 2, module });
  try {
    engine.limit("one", { concurrent: 1 });
    const runs = await Promise.all(Array.from({ length: 6 }, (_, i) => engine.workflow("limited").start(`lim-${i}`, null)));
    const spans = (await Promise.all(runs.map((r) => r.result()))).sort((a, b) => a[0] - b[0]);
    assert.equal(new Set(spans.map((s) => s[2])).size, 2, "both workers ran steps");
    for (let i = 1; i < spans.length; i++) assert.ok(spans[i][0] >= spans[i - 1][1], `step ${i} overlapped the one before`);
  } finally {
    await engine.close();
  }
});

test("debounce, batch and priorities with worker threads", async () => {
  const engine = await Engine.open({ durability: "memory", workers: 2, module });
  try {
    // Coordinators of several keys land on both workers; each run starts on
    // the worker that owns its own ID.
    const echo = engine.workflow("echo");
    const keys = ["k1", "k2", "k3", "k4", "k5", "k6"];
    const runs = await Promise.all(keys.flatMap((k) => [1, 2].map((n) => echo.debounce(k, `${k}:${n}`, { window: 150 }))));
    const threads = new Set();
    for (let i = 0; i < keys.length; i++) {
      const [first, latest] = [runs[2 * i], runs[2 * i + 1]];
      assert.equal(first.id, latest.id);
      await first.started();
      const out = await first.result();
      assert.equal(out.v, `${keys[i]}:2`);
      threads.add(out.thread);
    }
    assert.equal(threads.size, 2, "runs on both workers");
    // Batched events from the main thread, deduplicated by ID.
    const collect = engine.workflow("collect");
    const ids = [];
    for (const e of ["x", "y", "y", "z"]) ids.push((await collect.batch("main", e, { maxEvents: 3, maxWait: "1m", id: e })).id);
    assert.equal(new Set(ids).size, 1);
    const batch = engine.run(ids[0]);
    await batch.started();
    assert.deepEqual(await batch.result(), ["x", "y", "z"]);
    // ... and from workflow code on either worker.
    const feeders = await Promise.all(["f1", "f2", "f3", "f4"].map((id) => engine.workflow("feeder").start(id, ["fed", [id, `${id}!`]])));
    const fed = [...new Set((await Promise.all(feeders.map((f) => f.result()))).flat())];
    assert.equal(fed.length, 4, "eight events in batches of two");
    const inputs = [];
    for (const id of fed) {
      await engine.run(id).started();
      inputs.push(...(await engine.run(id).result()));
    }
    assert.deepEqual(inputs.sort(), ["f1", "f1!", "f2", "f2!", "f3", "f3!", "f4", "f4!"]);
    // A limit with priorities across workers.
    engine.limit("ranked", { concurrent: 1 });
    const ranked = await Promise.all([0, 1, 2, 3, 4, 5].map((l) => engine.workflow("ranked").start(`ranked-${l}`, l)));
    const done = await Promise.all(ranked.map((r) => r.result()));
    assert.deepEqual(done.map((d) => d[1]).sort(), [0, 1, 2, 3, 4, 5]);
    assert.equal(engine.limitInUse("ranked"), 0);
    assert.equal(engine.limitWaiting("ranked"), 0);
  } finally {
    await engine.close();
  }
});

test("a crashed process's runs recover with any worker count", async () => {
  const dir = tmp();
  try {
    const child = spawnSync(process.execPath, [fileURLToPath(new URL("./workers.child.mjs", import.meta.url)), dir, "4", "16"], {
      encoding: "utf8",
      timeout: 60_000,
    });
    assert.equal(child.signal, "SIGKILL", child.stderr);
    const shards = JSON.parse(child.stdout.trim().split("\n").pop());
    assert.equal(new Set(shards).size, 4, "the runs were spread over four workers' shards");
    const ids = Array.from({ length: 16 }, (_, i) => `crash-${i}`);
    {
      // Two workers now, two shards each.
      const engine = await Engine.open({ dir, flush: "fsync", workers: 2, module });
      assert.equal(engine.shardMetrics().length, 4);
      for (const id of ids.slice(0, 8)) await engine.run(id).signal("go", id);
      for (const id of ids.slice(0, 8)) assert.equal((await engine.run(id).result()).v, id);
      assert.equal(await engine.run("crash-chain").result(), 1225);
      await engine.close();
    }
    {
      // More workers than before is fine as well: three over four shards.
      const engine = await Engine.open({ dir, flush: "fsync", workers: 3, module });
      for (const id of ids.slice(8, 12)) await engine.run(id).signal("go", id);
      for (const id of ids.slice(8, 12)) assert.equal((await engine.run(id).result()).v, id);
      await engine.close();
    }
    {
      // And on the main thread alone.
      const engine = await Engine.open({ dir, flush: "fsync", shards: 4 });
      const { default: register } = await import(module);
      register(engine);
      for (const id of ids.slice(12)) await engine.run(id).signal("go", id);
      for (const id of ids.slice(12)) assert.equal((await engine.run(id).result()).thread, threadId);
      assert.equal(engine.list(1000).filter((r) => r.status === "ok").length, 17);
      await engine.close();
    }
    {
      // More workers than shards: one per shard.
      const engine = await Engine.open({ dir, flush: "fsync", workers: 8, module });
      assert.equal(new Set((await threadsOf(engine, ["a", "b", "c", "d", "e", "f", "g", "h"])).values()).size <= 4, true);
      await engine.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("close stops every worker and releases the directory", async () => {
  const dir = tmp();
  try {
    const engine = await Engine.open({ dir, flush: "fsync", workers: 2, module });
    await engine.workflow("sleeper").start("sleeper-1", null);
    const pending = assert.rejects(
      (await engine.workflow("waiter").start("waiter-1", null)).result(),
      (e) => e instanceof FuseeError && e.code === "Closed",
    );
    await engine.close();
    await engine.close();
    await pending;
    await assert.rejects(engine.workflow("where").start("late", 1), (e) => e instanceof FuseeError && e.code === "Closed");
    // The lock is free again, and the runs are where they were.
    const again = await Engine.open({ dir, flush: "fsync", workers: 2, module });
    assert.deepEqual(again.list(10).map((r) => r.id).sort(), ["sleeper-1", "waiter-1"]);
    await again.run("waiter-1").signal("go", 1);
    assert.equal((await again.run("waiter-1").result()).v, 1);
    await again.close();
    // A module that does not load fails the open, and leaves nothing
    // running or locked.
    await assert.rejects(Engine.open({ dir, flush: "fsync", workers: 2, module: join(dir, "missing.mjs") }), Error);
    await assert.rejects(Engine.open({ dir, workers: 2 }), /module/);
    await assert.rejects(Engine.open({ dir, workers: 0, module }), /at least 1/);
    const last = await Engine.open({ dir, flush: "fsync", workers: 1, module });
    await last.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("errors keep their class across threads", async () => {
  const engine = await Engine.open({ durability: "memory", workers: 2, module });
  try {
    const w = await engine.workflow("waiter").start("err-1", null);
    await w.cancel("why");
    await assert.rejects(w.result(), (e) => e instanceof CancelledError && /why/.test(e.reason));
    // A run that failed in a step fails with that StepError, its cause whole.
    await assert.rejects((await engine.workflow("boom").start("err-2", null)).result(), (e) => e instanceof StepError && e.step === "boom" && e.cause.message === "kaput");
  } finally {
    await engine.close();
  }
});
