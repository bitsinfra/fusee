// Keyed objects in the TypeScript SDK: exclusive order, shared reads, state
// across restarts, exactly-once calls, calls from workflows and between
// objects, compaction, and worker threads routing by key.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, shared } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-obj-"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function eventually(what, f) {
  for (let i = 0; i < 4000; i++) {
    if (await f()) return;
    await sleep(5);
  }
  throw new Error(`timed out waiting for ${what}`);
}

function cart(engine, counters) {
  return engine.object("cart", {
    add: async (ctx, item) => {
      const priced = await ctx.step("price", () => {
        counters.ran += 1;
        return `${item}$`;
      });
      const items = ctx.state.get("items") ?? [];
      items.push(priced);
      ctx.state.set("items", items);
      return items.length;
    },
    get: shared(async (ctx) => ctx.state.get("items") ?? []),
    fail: async (ctx) => {
      ctx.state.set("items", ["lost"]);
      throw new Error("out of stock");
    },
    key: shared((ctx) => ctx.objectKey),
  });
}

test("calls change state, failures change nothing, retries run once", async () => {
  const dir = tmp();
  const counters = { ran: 0 };
  {
    const engine = await Engine.open({ dir, flush: "fsync" });
    const k = cart(engine, counters).key("u-1");
    assert.equal(await k.call("add", "apple", { id: "a" }), 1);
    assert.equal(await k.call("add", "pear", { id: "b" }), 2);
    assert.equal(await k.call("add", "pear", { id: "b" }), 2);
    assert.equal(counters.ran, 2);
    assert.deepEqual(await k.call("get"), ["apple$", "pear$"]);
    await assert.rejects(k.call("fail"), /out of stock/);
    assert.deepEqual(await k.state(), { items: ["apple$", "pear$"] });
    assert.equal(await k.call("key"), "u-1");
    assert.deepEqual(await engine.objectRef("cart", "nobody").state(), {});
    const d = k.inspect();
    assert.deepEqual(d.object.state, { items: ["apple$", "pear$"] });
    const done = d.journal.filter((r) => r.kind === "update_done" && r.state);
    assert.equal(done.length, 2);
    await k.send("add", "fig", { id: "c" });
    assert.deepEqual(engine.objects("cart"), ["u-1"]);
    await engine.close();
  }
  {
    const engine = await Engine.open({ dir, flush: "fsync" });
    const k = cart(engine, counters).key("u-1");
    await eventually("the send", async () => (await k.state()).items?.length === 3);
    assert.equal(await k.call("add", "pear", { id: "b" }), 2);
    assert.equal(counters.ran, 3);
    await engine.close();
  }
  rmSync(dir, { recursive: true, force: true });
});

test("exclusive calls take turns; shared calls do not wait", async () => {
  const engine = await Engine.open({ durability: "memory" });
  let busy = false;
  let overlaps = 0;
  let release;
  const gate = new Promise((r) => (release = r));
  const k = engine
    .object("doc", {
      bump: async (ctx, who) => {
        if (busy) overlaps += 1;
        busy = true;
        await ctx.step("work", () => sleep(1));
        const seen = ctx.state.get("seen") ?? [];
        seen.push(who);
        ctx.state.set("seen", seen);
        busy = false;
        return seen.length;
      },
      hold: async (ctx) => {
        ctx.state.set("held", true);
        await ctx.step("hold", () => gate);
      },
      read: shared((ctx) => ctx.state.get("held") ?? false),
    })
    .key("d");
  const places = await Promise.all(Array.from({ length: 30 }, (_, who) => k.call("bump", who).then((p) => [who, p])));
  assert.equal(overlaps, 0);
  const { seen } = await k.state();
  for (const [who, place] of places) assert.equal(seen[place - 1], who);
  const held = k.call("hold");
  await eventually("hold to start", () => k.inspect().object.running.length === 1);
  // The exclusive call holds the key; a shared read answers at once, with
  // the committed state.
  assert.equal(await k.call("read"), false);
  const next = k.call("bump", 99);
  await eventually("bump to wait", () => k.inspect().object.waiting.length === 1);
  release();
  await held;
  assert.equal(await next, 31);
  assert.equal(await k.call("read"), true);
  await engine.close();
});

test("workflows and objects call objects exactly once", async () => {
  const engine = await Engine.open({ durability: "memory", keepHistory: true });
  const counters = { ran: 0 };
  cart(engine, counters);
  engine.object("order", {
    place: async (ctx, items) => {
      let n = 0;
      for (const i of items) n = await ctx.object("cart", ctx.objectKey).call("add", i);
      ctx.state.set("placed", n);
      return n;
    },
  });
  const wf = engine.workflow("shopper", async (ctx, key) => {
    const a = await ctx.object("cart", key).call("add", "tea");
    ctx.object("cart", key).send("add", "milk");
    const placed = await ctx.object("order", key).call("place", ["jam", "bread"]);
    const items = await ctx.object("cart", key).call("get");
    return { a, placed, items };
  });
  const run = await wf.start("shop-1", "s1");
  const out = await run.result();
  assert.equal(out.a, 1);
  assert.deepEqual([...out.items].sort(), ["bread$", "jam$", "milk$", "tea$"]);
  assert.equal(counters.ran, 4);
  assert.equal(run.inspect().journal.filter((r) => r.kind === "call").length, 4);
  await engine.close();
});

test("compaction keeps state and remembered IDs", async () => {
  const engine = await Engine.open({ durability: "memory" });
  let ran = 0;
  const k = engine
    .object("tally", {
      add: (ctx, n) => {
        ran += 1;
        const sum = (ctx.state.get("sum") ?? 0) + n;
        ctx.state.set("sum", sum);
        return sum;
      },
    })
    .key("t");
  for (let n = 1; n <= 300; n++) assert.equal(await k.call("add", n, { id: `id-${n}` }), (n * (n + 1)) / 2);
  const d = k.inspect();
  assert.ok(d.journal.length <= 2 * 128 + 4, `${d.journal.length} records`);
  assert.deepEqual(d.object.state, { sum: 45150 });
  const before = ran;
  assert.equal(await k.call("add", 5, { id: "id-5" }), 15);
  assert.equal(ran, before);
  await engine.close();
});

test("worker threads route each key to its worker", async () => {
  const dir = tmp();
  const engine = await Engine.open({ dir, workers: 2, module: new URL("./workers.module.mjs", import.meta.url) });
  const counter = engine.object("ocounter");
  const keys = ["a", "b", "c", "d", "e"];
  for (let i = 0; i < 3; i++) await Promise.all(keys.map((k) => counter.key(k).call("bump", 1)));
  for (const k of keys) assert.deepEqual(await counter.key(k).state(), { n: 3 });
  assert.deepEqual(engine.objects("ocounter").sort(), keys);
  const wf = engine.workflow("bumper");
  const run = await wf.start("bumper-1", "a");
  assert.equal(await run.result(), 4);
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
});
