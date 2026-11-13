import { test } from "node:test";
import assert from "node:assert/strict";
import { DeterminismError, workflow } from "../dist/index.js";
import { TestEngine } from "../dist/testing.js";

let charged = 0;
const checkout = workflow("checkout", async (ctx, cents) => {
  const quote = await ctx.step("quote", () => cents + 1);
  const ch = await ctx.step("charge", () => {
    charged += 1;
    return "real";
  }, { retry: { attempts: 3, initial: 1 } });
  return `${quote}:${ch}`;
});

let flips = 0;
const nondet = workflow("nondet", async (ctx) => {
  flips += 1;
  return ctx.step(flips % 2 ? "a" : "b", () => 1);
});

test("mocks, steps, snapshots and the strict replay check", async () => {
  const t = await TestEngine.open({ workflows: [checkout] });
  t.mockStep(checkout, "charge", (attempt) => {
    if (attempt < 2) throw new Error("unavailable");
    return `ch_${attempt}`;
  });
  const run = await t.start(checkout, "o-1", 41);
  assert.equal(await run.result(), "42:ch_2");
  assert.equal(charged, 0);
  assert.deepEqual(await t.steps("o-1"), ["quote", "charge"]);
  assert.equal((await t.snapshot("o-1"))[0].kind, "start");
  await t.check();
  await t.close();
});

test("check finds a nondeterministic workflow", async () => {
  const t = await TestEngine.open({ workflows: [nondet] });
  await (await t.start(nondet, "n-1", null)).result();
  await assert.rejects(t.check(), (e) => e instanceof DeterminismError && /n-1 \(nondet\)/.test(e.message) && /'a'/.test(e.message));
  await t.close();
});

import { signal } from "../dist/index.js";

const approve = signal("approve");
let renewCharged = 0;
const renew = workflow("renew", async (ctx, plan) => {
  const q = await ctx.step("quote", () => 42);
  await ctx.sleep("a day", "1d");
  const c = await ctx.step("charge", () => {
    renewCharged += 1;
    return "ok";
  });
  const ok = await ctx.waitFor(approve, { timeout: "7d" });
  return `${plan}:${q}:${c}:${ok ?? "none"}`;
});

test("virtual time and crashes", async () => {
  const t = await TestEngine.open({ workflows: [renew] });
  const began = Date.now();
  const before = t.now();
  await t.start(renew, "r-1", "pro");
  // A day's sleep, then a week's wait that times out: skipped.
  assert.equal(await t.result("r-1"), "pro:42:ok:none");
  assert.ok(Date.now() - began < 2000, `${Date.now() - began} ms`);
  assert.ok(t.now() - before >= 8 * 86_400_000, `${t.now() - before}`);
  await t.check();

  renewCharged = 0;
  await t.start(renew, "r-2", "basic");
  for (let i = 0; i < 400 && !(await t.steps("r-2")).includes("charge"); i++) {
    await t.advance("1d");
    await new Promise((r) => setTimeout(r, 5));
  }
  assert.equal(renewCharged, 1);
  await t.crash();
  await t.run("r-2").signal(approve, true);
  assert.equal(await t.result("r-2"), "basic:42:ok:true");
  // Recorded before the crash: not run again.
  assert.equal(renewCharged, 1);
  await t.close();
});

import { crashEverywhere } from "../dist/testing.js";

function paymentsCase(planted) {
  const charges = new Set();
  let emails = 0;
  const order = workflow("order", async (ctx, cents) => {
    await ctx.step("charge", (s) => { charges.add(s.key); });
    await ctx.step("email", () => { emails = planted ? emails + 1 : 1; });
    return cents;
  });
  return { workflows: [order], workflow: order, id: "o-1", input: 500, effects: () => ({ charges: charges.size, emails }) };
}

test("crashEverywhere finds a step that is not idempotent", async () => {
  await crashEverywhere(() => paymentsCase(false));
  await assert.rejects(crashEverywhere(() => paymentsCase(true)), /"emails":2/);
});
