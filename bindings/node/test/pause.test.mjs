// A step whose retries run out pauses its run; a resume runs it again with
// a fresh set of attempts.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, FatalError, RejectedError, workflow } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-pause-"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function eventually(what, f) {
  for (let i = 0; i < 2000; i++) {
    if (await f()) return;
    await sleep(5);
  }
  throw new Error(`timed out waiting for ${what}`);
}

function service() {
  const s = { broken: true, calls: 0, attempts: [] };
  s.charge = (n, attempt) => {
    s.calls += 1;
    s.attempts.push(attempt);
    if (s.broken) throw new Error("payments are down");
    return n * 10;
  };
  return s;
}

const pay = (svc, options = { retry: { attempts: 2, initial: 1, onExhausted: "pause" } }) =>
  workflow("pay", async (ctx, n) => (await ctx.step("charge", (s) => svc.charge(n, s.attempt), options)) + 1);

const status = (engine, id) => engine.run(id).status().then((s) => s.status, () => "");

test("a paused child keeps its parent waiting until it is resumed", async () => {
  const dir = tmp();
  const svc = service();
  const p = pay(svc);
  const order = workflow("order", async (ctx, n) => ctx.child(p, "pay-1", n));
  const engine = await Engine.open({ dir, workflows: [p, order], keepHistory: true });
  try {
    const run = await engine.start(order, "order-1", 4);
    await eventually("the pause", async () => (await status(engine, "pay-1")) === "paused");
    assert.equal(svc.calls, 2);
    await sleep(50);
    assert.equal(svc.calls, 2, "nothing retries while paused");
    assert.ok(["suspended", "parked"].includes(await status(engine, "order-1")));

    svc.broken = false;
    await engine.run("pay-1").resume();
    assert.equal(await run.result(), 41);
    // The resume starts a fresh set of attempts.
    assert.deepEqual(svc.attempts, [1, 2, 1]);
    const kinds = (await engine.run("pay-1").describe()).journal.map((r) => r.kind);
    assert.deepEqual(kinds, ["start", "attempt", "attempt", "resume", "step", "done"]);
    await assert.rejects(engine.run("pay-1").resume(), RejectedError);
  } finally {
    await engine.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a run stays paused across a restart; the engine default pauses; final errors fail", async () => {
  const dir = tmp();
  const svc = service();
  const p = pay(svc, { retry: { attempts: 2, initial: 1 } });
  {
    const engine = await Engine.open({ dir, workflows: [p], onExhausted: "pause" });
    await engine.start(p, "pay-2", 1);
    await eventually("the pause", async () => (await status(engine, "pay-2")) === "paused");
    await engine.close();
  }
  const engine = await Engine.open({ dir, workflows: [p], onExhausted: "pause" });
  try {
    await eventually("the paused run", async () => (await status(engine, "pay-2")) === "paused");
    await sleep(50);
    assert.equal(svc.calls, 2);
    await engine.run("pay-2").resume();
    await eventually("two more attempts and a pause", async () => svc.calls === 4 && (await status(engine, "pay-2")) === "paused");
    svc.broken = false;
    await engine.run("pay-2").resume();
    assert.equal(await engine.run("pay-2").result(), 11);

    const fatal = workflow("fatal", async (ctx) => ctx.step("s", () => { throw new FatalError("card declined"); }));
    engine.register(fatal);
    const f = await engine.start(fatal, "fatal-1", null);
    await assert.rejects(f.result());
    assert.equal(await status(engine, "fatal-1"), "failed");
  } finally {
    await engine.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("cancel reaches a paused run", async () => {
  const dir = tmp();
  const svc = service();
  const p = pay(svc);
  const engine = await Engine.open({ dir, workflows: [p] });
  try {
    const run = await engine.start(p, "pay-c", 1);
    await eventually("the pause", async () => (await status(engine, "pay-c")) === "paused");
    await run.cancel("not needed");
    await assert.rejects(run.result());
    assert.equal(await status(engine, "pay-c"), "cancelled");
    assert.equal(svc.calls, 2);
  } finally {
    await engine.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
