import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, CancelledError, StepError } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-"));

test("steps, sleeps, signals and children", async () => {
  const dir = tmp();
  const engine = await Engine.open({ dir, flush: "fsync", shards: 2 });
  let charged = 0;
  engine.workflow("ship", async (ctx, x) => ctx.step("label", () => `label-${x}`));
  const checkout = engine.workflow("checkout", async (ctx, order) => {
    const quote = await ctx.step("quote", () => order.amount * 2);
    const charge = await ctx.step("charge", () => {
      charged += 1;
      return { id: `ch_${quote}`, key: ctx.key() };
    }, { retry: { attempts: 3 } });
    await ctx.sleep("cool-off", "5ms");
    const approved = await ctx.waitFor("review", { timeout: "2s" });
    const label = await ctx.child("ship", `${ctx.workflowId}/ship`, quote);
    const [a, b] = await ctx.all([ctx.step("a", () => 1), ctx.step("b", () => 2)]);
    return { charge: charge.id, approved, label, sum: a + b, t: ctx.now() > 0, u: ctx.uuid().length };
  });
  const run = await checkout.start("order-123", { amount: 21 });
  await run.signal("review", true);
  const out = await run.result();
  assert.deepEqual(out, { charge: "ch_42", approved: true, label: "label-42", sum: 3, t: true, u: 36 });
  assert.equal(charged, 1);
  // A start that attaches returns the same run.
  const again = await checkout.start("order-123", { amount: 99 }, {
    idPolicy: { running: "use-existing", finished: "use-existing" },
  });
  assert.deepEqual(await again.result(), out);
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
});

test("retries and failures", async () => {
  const engine = await Engine.open({ durability: "memory" });
  let n = 0;
  const wf = engine.workflow("flaky", async (ctx, failAlways) =>
    ctx.step("call", () => {
      n += 1;
      if (failAlways || n < 3) throw new Error(`attempt ${n}`);
      return n;
    }, { retry: { attempts: 4, initial: "1ms" } }),
  );
  assert.equal(await (await wf.start("f1", false)).result(), 3);
  n = 0;
  await assert.rejects((await wf.start("f2", true)).result(), /attempt 4/);
  await engine.close();
});

test("cancellation runs compensation", async () => {
  const engine = await Engine.open({ durability: "memory" });
  const wf = engine.workflow("saga", async (ctx) => {
    await ctx.step("reserve", () => true);
    try {
      await ctx.waitFor("confirm");
      return "confirmed";
    } catch (e) {
      if (e instanceof CancelledError) {
        await ctx.step("release", () => true);
        throw new CancelledError(`compensated: ${e.reason}`);
      }
      throw e;
    }
  });
  const run = await wf.start("s1", null);
  await new Promise((r) => setTimeout(r, 10));
  await run.cancel("customer left");
  await assert.rejects(run.result(), (e) => e instanceof CancelledError && e.reason === "compensated: customer left");
  await engine.close();
});

test("recovery replays recorded steps", async () => {
  const dir = tmp();
  let calls = 0;
  const define = (engine) =>
    engine.workflow("pausing", async (ctx) => {
      const a = await ctx.step("a", () => {
        calls += 1;
        return 7;
      });
      const v = await ctx.waitFor("go");
      return a + v;
    });
  {
    const engine = await Engine.open({ dir, flush: "fsync" });
    const wf = define(engine);
    await wf.start("p1", null);
    while (calls === 0) await new Promise((r) => setTimeout(r, 2));
    await new Promise((r) => setTimeout(r, 20));
    await engine.close();
  }
  const engine = await Engine.open({ dir, flush: "fsync" });
  define(engine);
  const run = engine.run("p1");
  await run.signal("go", 5);
  assert.equal(await run.result(), 12);
  assert.equal(calls, 1, "step a ran once");
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a step error is a StepError", async () => {
  const engine = await Engine.open({ durability: "memory" });
  const wf = engine.workflow("bad", async (ctx) => {
    try {
      await ctx.step("boom", () => { throw new Error("nope"); }, { retry: { attempts: 1 } });
    } catch (e) {
      return e instanceof StepError ? e.step : "other";
    }
  });
  assert.equal(await (await wf.start("b1", null)).result(), "boom");
  await engine.close();
});
