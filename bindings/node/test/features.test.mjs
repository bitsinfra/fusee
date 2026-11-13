// The workflow features (docs/features.md), in the TypeScript SDK.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, Replayer, RejectedError, TerminatedError, aesGcm } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-feat-"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function eventually(what, f) {
  for (let i = 0; i < 2000; i++) {
    if (await f()) return;
    await sleep(5);
  }
  throw new Error(`timed out waiting for ${what}`);
}

test("queries and updates, deduplicated across a restart", async () => {
  const dir = tmp();
  let priced = 0;
  const register = (engine) =>
    engine.workflow("cart", async (ctx) => {
      const items = [];
      ctx.onQuery("items", () => [...items]);
      ctx.onUpdate("add", {
        validate: (item) => (item ? undefined : "empty item"),
        handle: async (item) => {
          const price = await ctx.step("price", () => {
            priced += 1;
            return item.length * 10;
          });
          items.push(item);
          return price;
        },
      });
      await ctx.waitFor("checkout");
      return items;
    });
  {
    const engine = await Engine.open({ dir, flush: "fsync", keepHistory: true, parkAfter: 10 });
    const run = await register(engine).start("cart-1", null);
    assert.equal(await run.update("add", "apple", { id: "u-1" }), 50);
    assert.equal(await run.update("add", "apple", { id: "u-1" }), 50);
    assert.equal(priced, 1);
    await eventually("foreign workflow parks", () => engine.shardMetrics().some((m) => m.runs_parked > 0));
    assert.deepEqual(await run.query("items"), ["apple"]);
    assert.ok(engine.shardMetrics().some((m) => m.runs_loaded > 0));
    assert.equal(priced, 1);
    await assert.rejects(run.update("add", ""), (e) => e instanceof RejectedError && /empty/.test(e.message));
    assert.deepEqual(await run.query("items"), ["apple"]);
    await assert.rejects(run.query("nope"), RejectedError);
    const kinds = run.inspect().journal.map((r) => r.kind);
    assert.equal(kinds.filter((k) => k === "update").length, 1);
    await engine.close();
  }
  {
    const engine = await Engine.open({ dir, flush: "fsync", keepHistory: true });
    register(engine);
    const run = engine.run("cart-1");
    assert.equal(await run.update("add", "apple", { id: "u-1" }), 50);
    assert.equal(await run.update("add", "pear"), 40);
    assert.deepEqual(await run.query("items"), ["apple", "pear"]);
    await run.signal("checkout", null);
    assert.deepEqual(await run.result(), ["apple", "pear"]);
    assert.equal(priced, 2);
    await engine.close();
  }
  rmSync(dir, { recursive: true, force: true });
});

test("update-with-start is one record", async () => {
  const engine = await Engine.open({ durability: "memory", keepHistory: true });
  const wf = engine.workflow("counter", async (ctx, start) => {
    let n = start;
    ctx.onUpdate("inc", (by) => (n += by));
    await ctx.waitFor("stop");
    return n;
  });
  const { run, result } = await wf.updateWithStart("c-1", 10, "inc", 5);
  assert.equal(result, 15);
  assert.equal(run.inspect().journal[0].update.name, "inc");
  assert.equal((await wf.updateWithStart("c-1", 99, "inc", 1)).result, 16);
  await run.signal("stop", null);
  assert.equal(await run.result(), 16);
  await engine.close();
});

test("heartbeats resume after a restart; a silent step times out", async () => {
  const dir = tmp();
  const from = [];
  const register = (engine) =>
    engine.workflow("export", async (ctx) =>
      ctx.step(
        "export",
        async () => {
          const start = ctx.progress() ?? 0;
          from.push(start);
          for (let p = start; p < 6; p++) {
            ctx.heartbeat(p + 1);
            await sleep(15);
          }
          return 6;
        },
        { retry: { attempts: 3, initial: 1 }, heartbeatTimeout: "1s" },
      ),
    );
  {
    const engine = await Engine.open({ dir, flush: "fsync" });
    await register(engine).start("exp-1", null);
    await sleep(60);
    await engine.close();
  }
  {
    const engine = await Engine.open({ dir, flush: "fsync" });
    register(engine);
    assert.equal(await engine.run("exp-1").result(), 6);
    assert.equal(from[0], 0);
    assert.ok(from[1] > 0, `resumed from durable progress: ${from}`);
    await engine.close();
  }
  const engine = await Engine.open({ durability: "memory" });
  let attempts = 0;
  const wf = engine.workflow("slow", async (ctx) =>
    ctx.step(
      "slow",
      async () => {
        attempts += 1;
        const start = ctx.progress() ?? 0;
        ctx.heartbeat(start + 1);
        if (attempts === 1) await sleep(10_000);
        return start;
      },
      { retry: { attempts: 3, initial: 1 }, heartbeatTimeout: 100 },
    ),
  );
  assert.equal(await (await wf.start("s-1", null)).result(), 1);
  assert.equal(attempts, 2);
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
});

test("async completion refuses stale tokens", async () => {
  const engine = await Engine.open({ durability: "memory" });
  const tokens = [];
  const wf = engine.workflow("approve", async (ctx) =>
    ctx.awaitCompletion(
      "approval",
      ({ token }) => {
        tokens.push(token);
        if (tokens.length === 1) throw new Error("mail server down");
      },
      { retry: { attempts: 3, initial: 1 } },
    ),
  );
  const run = await wf.start("a-1", null);
  await eventually("two tokens", () => tokens.length === 2);
  await assert.rejects(engine.complete(tokens[0], true), RejectedError);
  await engine.complete(tokens[1], true);
  assert.equal(await run.result(), true);
  await assert.rejects(engine.complete(tokens[1], false), RejectedError);
  await engine.close();
});

test("continue-as-new keeps the ID; patching; reset", async () => {
  const engine = await Engine.open({ durability: "memory", keepHistory: true });
  const loop = engine.workflow("loop", async (ctx, [left, acc]) => {
    const v = await ctx.step("work", () => left);
    assert.ok(ctx.journalSize() <= 2);
    if (left === 0) return acc + v;
    return ctx.continueAsNew([left - 1, acc + v]);
  });
  const run = await loop.start("loop-1", [5, 0]);
  assert.equal(await run.result(), 15);
  assert.equal(run.inspect().journal[0].from, "continued");

  const patchy = engine.workflow("patchy", async (ctx) => (ctx.patched("fix") ? "new" : "old"));
  assert.equal(await (await patchy.start("p-1", null)).result(), "new");

  let charged = 0;
  const pay = engine.workflow("pay", async (ctx, n) => {
    const quote = await ctx.step("quote", () => n);
    const key = await ctx.step("charge", () => {
      charged += 1;
      return ctx.key();
    });
    const go = await ctx.waitFor("go");
    return `${quote}/${key}/${go}`;
  });
  const p = await pay.start("pay-1", 7);
  await p.signal("go", 1);
  const first = await p.result();
  await p.reset({ beforeStep: "charge" });
  const second = await p.result();
  assert.equal(charged, 2);
  assert.notEqual(first, second);
  assert.ok(second.startsWith("7/") && second.endsWith("/1"), second);
  await engine.close();
});

test("terminate applies the parent close policy", async () => {
  const engine = await Engine.open({ durability: "memory" });
  engine.workflow("kid", async (ctx) => ctx.waitFor("never"));
  const parent = engine.workflow("parent", async (ctx) => {
    ctx.spawnChild("kid", "kid-terminate", null);
    ctx.spawnChild("kid", "kid-cancel", null, { parentClose: "request-cancel" });
    ctx.spawnChild("kid", "kid-abandon", null, { parentClose: "abandon" });
    return ctx.waitFor("never");
  });
  const run = await parent.start("parent-1", null);
  await eventually("children", () => ["kid-terminate", "kid-cancel", "kid-abandon"].every((id) => engine.run(id).inspect()));
  await run.terminate("stop");
  await assert.rejects(run.result(), TerminatedError);
  const st = (id) => engine.run(id).inspect()?.status;
  await eventually("children closed", () => st("kid-terminate") === "terminated" && st("kid-cancel") === "cancelled");
  assert.ok(["suspended", "running", "parked"].includes(st("kid-abandon")));
  await engine.close();
});

test("schedules, search attributes and limits", async () => {
  const engine = await Engine.open({ durability: "memory" });
  let runs = 0;
  engine.workflow("report", async (ctx) => {
    runs += 1;
    return ctx.workflowId;
  });
  const s = await engine.schedule("nightly", { every: "100ms" }, "report");
  await eventually("three runs", () => runs >= 3);
  assert.ok((await s.describe()).started >= 3);
  await s.pause();
  await sleep(50);
  const n = runs;
  await sleep(300);
  assert.equal(runs, n);
  const ids = engine.list({ where: { type: "report" } }).map((r) => r.id);
  assert.equal(new Set(ids).size, ids.length);
  assert.ok(ids.every((i) => i.startsWith("nightly@")), ids.join());
  await s.delete();

  const order = engine.workflow("order", async (ctx, amount) => {
    ctx.setAttributes({ customer: "c-1", amount });
    if (amount > 100) throw new Error("too big");
    ctx.setAttributes({ stage: "paid" });
    return amount;
  });
  for (const [i, a] of [50, 150, 80].entries()) await (await order.start(`o-${i}`, a)).result().catch(() => {});
  const q = (where) => engine.list({ where }).map((r) => r.id).sort();
  assert.deepEqual(q({ customer: "c-1" }), ["o-0", "o-1", "o-2"]);
  assert.deepEqual(q({ status: "failed" }), ["o-1"]);
  assert.deepEqual(q({ amount: { gte: 60, lt: 200 }, stage: "paid" }), ["o-2"]);

  engine.limit("stripe", { concurrent: 2 });
  let now = 0;
  let max = 0;
  const charge = engine.workflow("charge", async (ctx) =>
    ctx.step(
      "charge",
      async () => {
        now += 1;
        max = Math.max(max, now);
        await sleep(20);
        now -= 1;
      },
      { limit: "stripe" },
    ),
  );
  const all = [];
  for (let i = 0; i < 8; i++) all.push(await charge.start(`c-${i}`, null));
  for (const r of all) await r.result();
  assert.equal(max, 2);
  await engine.close();
});

test("encrypted payloads never reach the disk", async () => {
  const dir = tmp();
  const codec = aesGcm({ k1: Buffer.alloc(32, 3) });
  {
    const engine = await Engine.open({ dir, flush: "fsync", codec });
    const wf = engine.workflow("secret", async (ctx, card) => {
      const masked = await ctx.step("mask", () => `${card.slice(0, 4)}-SECRETRESULT`);
      const note = await ctx.waitFor("note");
      return `${masked}/${note}`;
    });
    const run = await wf.start("s-1", "4242SECRETCARD");
    await run.signal("note", "SECRETSIGNAL");
    assert.equal(await run.result(), "4242-SECRETRESULT/SECRETSIGNAL");
    await engine.close();
  }
  const walk = (d) => readdirSync(d).flatMap((f) => (statSync(join(d, f)).isDirectory() ? walk(join(d, f)) : [join(d, f)]));
  for (const f of walk(dir)) {
    const b = readFileSync(f);
    for (const needle of ["SECRETCARD", "SECRETRESULT", "SECRETSIGNAL"]) assert.equal(b.indexOf(needle), -1, `${f} holds ${needle}`);
  }
  const engine = await Engine.open({ dir, flush: "fsync", codec });
  assert.equal(await engine.run("s-1").result(), "4242-SECRETRESULT/SECRETSIGNAL");
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
});

test("replay testing names the first difference; ID policies", async () => {
  const engine = await Engine.open({ durability: "memory", keepHistory: true });
  const pay = engine.workflow("pay", async (ctx) => {
    const q = await ctx.step("quote", () => 3);
    return ctx.step("charge", () => q * 2);
  });
  await (await pay.start("p-1", null)).result();
  const file = engine.exportJournal("p-1");
  const same = new Replayer().workflow("pay", async (ctx) => {
    const q = await ctx.step("quote", () => 0);
    return ctx.step("charge", () => q);
  });
  const ok = await same.replay(file);
  assert.ok(ok.ok && ok.finished, JSON.stringify(ok));
  const changed = new Replayer().workflow("pay", async (ctx) => {
    const q = await ctx.step("quote", () => 3);
    return ctx.step("refund", () => q);
  });
  const bad = await changed.replay(file);
  assert.ok(!bad.ok && /'charge'/.test(bad.error) && /'refund'/.test(bad.error), JSON.stringify(bad));

  const w = engine.workflow("w", async (ctx, fail) => {
    await ctx.waitFor("go", { timeout: 50 });
    if (fail) throw new Error("no");
    return ctx.runId;
  });
  const a = await w.start("id-1", false);
  await assert.rejects(w.start("id-1", false), /in use|already/);
  const same2 = await w.start("id-1", false, { idPolicy: { running: "use-existing" } });
  const first = await a.result();
  assert.equal(await same2.result(), first);
  await (await w.start("id-1", true)).result().catch(() => {});
  const c = await w.start("id-1", false, { idPolicy: { finished: "allow-if-failed" } });
  assert.ok((await c.result()) > first);
  await assert.rejects(w.start("id-1", false, { idPolicy: { finished: "allow-if-failed" } }), /in use|already/);
  await assert.rejects(w.start("id-1", false, { idPolicy: { finished: "refuse" } }), /in use|already/);
  await engine.close();
});
