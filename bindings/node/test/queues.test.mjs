// Queues in the TypeScript SDK: priorities and fairness keys on limits,
// debounced starts and batches.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, FuseeError, RejectedError } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-queues-"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function eventually(what, f) {
  for (let i = 0; i < 2000; i++) {
    if (await f()) return;
    await sleep(5);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/**
 * Holds the one permit of `one`, queues a job per ask (`[level, key,
 * weight]`) in that order, then lets them through: the order the jobs ran in.
 */
async function serveOrder(asks) {
  const engine = await Engine.open({ durability: "memory" });
  try {
    engine.limit("one", { concurrent: 1 });
    let open;
    const gate = new Promise((r) => (open = r));
    const served = [];
    const hold = engine.workflow("hold", async (ctx) => ctx.step("hold", () => gate, { limit: "one" }));
    const job = engine.workflow("job", async (ctx, [level, key, weight]) =>
      ctx.step("work", () => void served.push(`${key}@${level}`), {
        limit: "one",
        priority: level,
        fairnessKey: key,
        fairnessWeight: weight,
      }),
    );
    const blocker = await hold.start("hold", null);
    await eventually("the blocker's permit", () => engine.limitInUse("one") === 1);
    // Each start waits until its step is queued, so the arrival order is fixed.
    const runs = [];
    for (const [i, ask] of asks.entries()) {
      runs.push(await job.start(`job-${i}`, ask));
      await eventually("the step to queue", () => engine.limitWaiting("one") === i + 1);
    }
    open();
    await blocker.result();
    for (const r of runs) await r.result();
    assert.equal(engine.limitInUse("one"), 0);
    assert.equal(engine.limitWaiting("one"), 0);
    return served;
  } finally {
    await engine.close();
  }
}

test("waiting steps take permits by priority, then by fairness key", async () => {
  // Tenant "big" queues six steps, then "a" and "b" two each, then one
  // urgent step: the deep key gets one permit per round, like the others.
  const asks = [...Array(6).fill([0, "big"]), [0, "a"], [0, "b"], [0, "a"], [0, "b"], [5, "urgent"]];
  assert.deepEqual(await serveOrder(asks), [
    "urgent@5", "big@0", "a@0", "b@0", "big@0", "a@0", "b@0", "big@0", "big@0", "big@0", "big@0",
  ]);
});

test("a fairness key's weight is its permits per turn", async () => {
  const asks = [];
  for (let i = 0; i < 4; i++) asks.push([0, "w2", 2], [0, "w1", 1]);
  assert.deepEqual(await serveOrder(asks), ["w2@0", "w2@0", "w1@0", "w2@0", "w2@0", "w1@0", "w1@0", "w1@0"]);
});

test("a priority is a 32-bit integer and a weight a whole number", async () => {
  const engine = await Engine.open({ durability: "memory" });
  try {
    const wf = engine.workflow("bad", async (ctx, opts) => ctx.step("s", () => 1, { limit: "one", ...opts }));
    await assert.rejects((await wf.start("p", { priority: 1.5 })).result(), /priority must be a 32-bit integer/);
    await assert.rejects((await wf.start("q", { priority: 2 ** 31 })).result(), /priority must be a 32-bit integer/);
    await assert.rejects((await wf.start("w", { fairnessWeight: -1 })).result(), /fairnessWeight must be a whole number/);
    assert.equal(await (await wf.start("ok", { priority: -7, fairnessKey: "t", fairnessWeight: 3 })).result(), 1);
  } finally {
    await engine.close();
  }
});

test("debounced starts collapse into one run with the latest input; maxWait bounds a busy window", async () => {
  const engine = await Engine.open({ durability: "memory", keepHistory: true });
  try {
    let starts = 0;
    const wf = engine.workflow("reindex", async (ctx, n) => {
      starts += 1;
      return n;
    });
    const rule = { window: 300 };
    const [a, b, other, c] = await Promise.all([
      wf.debounce("doc-1", 1, rule),
      wf.debounce("doc-1", 2, rule),
      // Another key has its own window.
      wf.debounce("doc-2", 20, rule),
      wf.debounce("doc-1", 3, rule),
    ]);
    assert.equal(a.id, b.id);
    assert.equal(a.id, c.id);
    assert.notEqual(a.id, other.id);
    assert.match(a.id, /^doc-1@[0-9a-f-]{36}$/);
    // Nothing runs until the window closes.
    await assert.rejects(a.result(), (e) => e instanceof FuseeError && e.code === "NotFound");
    await a.started();
    assert.equal(await a.result(), 3);
    await other.started();
    assert.equal(await other.result(), 20);
    assert.equal(starts, 2);
    // The run is a child the coordinator abandoned, with the latest input.
    const start = a.inspect().journal[0];
    assert.equal(start.child, true);
    assert.equal(start.input, 3);
    // After the window closed, a start opens a new window and a new run.
    const d = await wf.debounce("doc-1", 4, rule);
    assert.notEqual(d.id, a.id);
    await d.started();
    assert.equal(await d.result(), 4);
    // A maximum wait bounds a window that starts keep extending.
    const capped = { window: 80, maxWait: 200 };
    const ids = new Set();
    const begin = Date.now();
    for (let n = 100; Date.now() - begin < 700; n++) {
      ids.add((await wf.debounce("busy", n, capped)).id);
      await sleep(20);
    }
    assert.ok(ids.size >= 3, [...ids].join());
    for (const id of ids) {
      const run = engine.run(id);
      await run.started();
      await run.result();
    }
    await assert.rejects(wf.debounce("doc-1", 1, {}), /needs a window/);
  } finally {
    await engine.close();
  }
});

test("batches start one run per full or timed-out batch; an event ID is added once", async () => {
  const engine = await Engine.open({ durability: "memory" });
  try {
    const wf = engine.workflow("bulk", async (ctx, events) => events);
    const rule = { maxEvents: 3, maxWait: 500 };
    const handles = [];
    for (let i = 0; i < 7; i++) handles.push(await wf.batch("tenant-1", i, rule));
    // A retried event with the same ID is added once.
    const again = await wf.batch("tenant-1", 7, { ...rule, id: "evt-7" });
    const dup = await wf.batch("tenant-1", 7, { ...rule, id: "evt-7" });
    assert.equal(again.id, dup.id);
    const ids = handles.map((h) => h.id);
    assert.equal(ids[0], ids[2]);
    assert.notEqual(ids[2], ids[3]);
    assert.equal(ids[3], ids[5]);
    assert.notEqual(ids[5], ids[6]);
    assert.equal(ids[6], again.id);
    const results = [];
    for (const id of [ids[0], ids[3], ids[6]]) {
      const run = engine.run(id);
      await run.started();
      results.push(await run.result());
    }
    // Two closed by their count, the last by its wait.
    assert.deepEqual(results, [[0, 1, 2], [3, 4, 5], [6, 7]]);
    // A rule the coordinator cannot use is refused before anything is sent.
    await assert.rejects(wf.batch("tenant-1", 1, { maxEvents: 0 }), RejectedError);
    await assert.rejects(wf.batch("tenant-1", 1, { maxEvents: 10_001 }), RejectedError);
    await assert.rejects(wf.batch("tenant-1", new Uint8Array([1]), rule), /JSON, not bytes/);
  } finally {
    await engine.close();
  }
});

test("an event a coordinator refused as it ended is sent to the next one", async () => {
  const engine = await Engine.open({ durability: "memory" });
  try {
    const wf = engine.workflow("bulk", async (ctx, events) => events);
    // The refusal as it comes from this engine, and through a server.
    const refusals = ["the workflow ended before the update finished", "workflow failed: the workflow ended before the update finished"];
    const send = engine.updateWithStartWire.bind(engine);
    let calls = 0;
    engine.updateWithStartWire = (...a) => {
      calls += 1;
      return calls <= refusals.length ? Promise.reject(new FuseeError(refusals[calls - 1])) : send(...a);
    };
    const run = await wf.batch("k", "e", { maxEvents: 1 });
    assert.equal(calls, 3);
    await run.started();
    assert.deepEqual(await run.result(), ["e"]);
    // Any other failure is the caller's.
    engine.updateWithStartWire = () => Promise.reject(new FuseeError("disk on fire"));
    await assert.rejects(wf.batch("k", "f", { maxEvents: 1 }), /disk on fire/);
  } finally {
    await engine.close();
  }
});

test("an open window survives a restart in its middle", async () => {
  const dir = tmp();
  const register = (engine) => ({
    notify: engine.workflow("notify", async (ctx, n) => ctx.step("x10", () => n * 10)),
    bulk: engine.workflow("bulk", async (ctx, events) => ctx.step("all", () => events)),
  });
  const rule = { window: 300 };
  const full = { maxEvents: 3, maxWait: "1m" };
  let id;
  let batchId;
  try {
    {
      const engine = await Engine.open({ dir, flush: "fsync" });
      const { notify, bulk } = register(engine);
      const first = await notify.debounce("user-9", 1, { ...rule, id: "e-1" });
      const second = await notify.debounce("user-9", 2, { ...rule, id: "e-2" });
      assert.equal(first.id, second.id);
      id = first.id;
      batchId = (await bulk.batch("t", "a", { ...full, id: "a" })).id;
      assert.equal((await bulk.batch("t", "b", { ...full, id: "b" })).id, batchId);
      // The process stops before either window closes.
      await engine.close();
    }
    const engine = await Engine.open({ dir, flush: "fsync" });
    const { notify, bulk } = register(engine);
    // A retry of an acknowledged event joins the same window once.
    assert.equal((await notify.debounce("user-9", 2, { ...rule, id: "e-2" })).id, id);
    const run = engine.run(id);
    await run.started();
    assert.equal(await run.result(), 20);
    // Once fired, the same event ID still answers with its run.
    assert.equal((await notify.debounce("user-9", 2, { ...rule, id: "e-2" })).id, id);
    // The batch kept its two events: the third fills it.
    assert.equal((await bulk.batch("t", "b", { ...full, id: "b" })).id, batchId);
    const filled = await bulk.batch("t", "c", { ...full, id: "c" });
    assert.equal(filled.id, batchId);
    await filled.started();
    assert.deepEqual(await filled.result(), ["a", "b", "c"]);
    assert.equal(engine.list(100).filter((r) => r.type === "notify").length, 1);
    await engine.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
