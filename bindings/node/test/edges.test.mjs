// Results JSON cannot encode, wakes during a retry delay, deliveries at
// completion, payloads that are bytes, and turns a native driver runs.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, Replayer, StepError } from "../dist/index.js";

/** Collects unhandled rejections while `f` runs. */
async function noUnhandledRejections(f) {
  const seen = [];
  const listener = (e) => seen.push(e);
  process.on("unhandledRejection", listener);
  try {
    await f();
    // Rejections are reported after the microtask queue drains.
    await new Promise((r) => setTimeout(r, 20));
  } finally {
    process.off("unhandledRejection", listener);
  }
  assert.deepEqual(seen.map(String), []);
}

test("a step result JSON cannot encode fails the attempt", async () => {
  await noUnhandledRejections(async () => {
    const engine = await Engine.open({ durability: "memory", keepHistory: true });
    const calls = { big: 0, loop: 0 };
    const define = (host, live) =>
      host.workflow("unencodable", async (ctx) => {
        const out = [];
        for (const [name, make] of [
          ["big", () => 1n],
          ["loop", () => { const o = {}; o.self = o; return o; }],
        ]) {
          try {
            await ctx.step(name, () => {
              if (live) calls[name] += 1;
              return make();
            }, { retry: { attempts: 2, initial: 1 } });
            out.push(`${name}: ok`);
          } catch (e) {
            out.push(`${name}: ${e instanceof StepError ? "StepError" : e.constructor.name}: ${e.message}`);
          }
        }
        // The run goes on after the failures.
        out.push(await ctx.step("after", () => "after"));
        return out;
      });
    const wf = define(engine, true);
    const out = await (await wf.start("u-1", null)).result();
    assert.equal(out.length, 3);
    assert.match(out[0], /^big: StepError: step 'big' failed: .*\$ \(a BigInt\)/);
    assert.match(out[1], /^loop: StepError: step 'loop' failed: .*\$\.self \(a cycle\)/);
    assert.equal(out[2], "after");
    // A result JSON cannot encode fails the same way every time: it is a
    // final Codec failure, not retried.
    assert.deepEqual(calls, { big: 1, loop: 1 });
    const kinds = engine.run("u-1").inspect().journal.map((r) => r.kind);
    assert.ok(!kinds.includes("attempt"), JSON.stringify(kinds));
    // Replay reaches the same failures from the journal alone.
    const report = await define(new Replayer(), false).replay(engine.exportJournal("u-1"));
    assert.ok(report.ok && report.finished, JSON.stringify(report));
    await engine.close();
  });
});

test("a workflow or update result JSON cannot encode fails the call", async () => {
  await noUnhandledRejections(async () => {
    const engine = await Engine.open({ durability: "memory" });
    const big = engine.workflow("big-result", async (ctx) => ({ n: await ctx.step("n", () => 1), v: 2n }));
    await assert.rejects((await big.start("b-1", null)).result(), /cannot be encoded as JSON: \$\.v \(a BigInt\)/);
    const upd = engine.workflow("loop-update", async (ctx) => {
      ctx.onUpdate("get", () => { const o = {}; o.self = o; return o; });
      ctx.onUpdate("ok", () => "fine");
      await ctx.waitFor("done");
      return "done";
    });
    const run = await upd.start("l-1", null);
    await assert.rejects(run.update("get", null), /cannot be encoded as JSON: \$\.self \(a cycle\)/);
    assert.equal(await run.update("ok", null), "fine");
    await run.signal("done", null);
    assert.equal(await run.result(), "done");
    await engine.close();
  });
});

test("wakes during a retry delay neither ask the gate again nor add timers", async () => {
  const engine = await Engine.open({ durability: "memory" });
  const delay = 600;
  let tries = 0;
  const wf = engine.workflow("backoff", async (ctx) => {
    const flaky = ctx.step("flaky", () => {
      tries += 1;
      if (tries === 1) throw new Error("first attempt fails");
      return tries;
    }, { retry: { attempts: 2, initial: delay } });
    const ticks = (async () => {
      let n = 0;
      // Each signal wakes the run while the flaky step waits out its delay.
      for (let i = 0; i < 20; i++) n += await ctx.waitFor("tick");
      return n;
    })();
    return ctx.all([flaky, ticks]);
  });
  // Count the gate checks of the retry (the only ones with a not-before time)
  // and the timers armed for its delay.
  const native = engine.native;
  const stepGate = native.stepGate.bind(native);
  let retryGates = 0;
  native.stepGate = (shard, run, gate, notBefore, inline) => {
    if (notBefore > 0) retryGates += 1;
    return stepGate(shard, run, gate, notBefore, inline);
  };
  const realSetTimeout = globalThis.setTimeout;
  let delayTimers = 0;
  globalThis.setTimeout = (f, ms, ...rest) => {
    if (ms > delay / 2 && ms <= delay) delayTimers += 1;
    return realSetTimeout(f, ms, ...rest);
  };
  try {
    const run = await wf.start("r-1", null);
    while (tries === 0) await new Promise((r) => realSetTimeout(r, 1));
    const began = Date.now();
    for (let i = 0; i < 20; i++) await run.signal("tick", 1);
    assert.ok(Date.now() - began < delay, "the signals arrived during the delay");
    assert.deepEqual(await run.result(), [2, 20]);
  } finally {
    globalThis.setTimeout = realSetTimeout;
    native.stepGate = stepGate;
  }
  // Once when the delay began, once when its timer fired.
  assert.ok(retryGates <= 2, `the gate was asked ${retryGates} times`);
  assert.equal(delayTimers, 1);
  await engine.close();
});

test("deliveries at completion keep the order replay reproduces", async () => {
  const engine = await Engine.open({ durability: "memory", keepHistory: true, shards: 2 });
  // Count the deliveries made in the microtask where a step settled.
  let inline = 0;
  const deliverNow = engine.deliverNow.bind(engine);
  engine.deliverNow = () => {
    const ok = deliverNow();
    if (ok) inline += 1;
    return ok;
  };
  const hop = async (n) => {
    for (let i = 0; i < n; i++) await null;
  };
  const later = (ms) => new Promise((r) => setTimeout(() => r(1), ms));
  const sum = (parts) => parts.reduce((x, y) => x + y);
  const define = (host) => ({
    // Branches that spend different numbers of microtasks between steps.
    hops: host.workflow("hops", async (ctx) => {
      const branch = async (tag, hops, readOnly) => {
        let t = 0;
        for (let i = 0; i < 4; i++) {
          await hop(hops);
          t += await ctx.step(`${tag}-${i}`, () => 1, { readOnly });
        }
        return t;
      };
      const parts = await ctx.all([branch("a", 0, false), branch("b", 3, false), branch("c", 1, true)]);
      return ctx.step("sum", () => sum(parts));
    }),
    // Steps that settle from timers, in later macrotasks.
    timers: host.workflow("timers", async (ctx) => {
      const parts = await ctx.all([0, 1, 2].map(async (j) => {
        let t = 0;
        for (let i = 0; i < 3; i++) t += await ctx.step(`x${j}-${i}`, () => later(1 + ((i + j) % 3)));
        return t;
      }));
      return ctx.step("sum", () => sum(parts));
    }),
  });
  const flows = define(engine);
  const ids = [];
  const starts = [];
  for (let i = 0; i < 10; i++) {
    for (const name of ["hops", "timers"]) {
      ids.push(`${name}-${i}`);
      starts.push(flows[name].start(`${name}-${i}`, null));
    }
  }
  const runs = await Promise.all(starts);
  assert.deepEqual(await Promise.all(runs.map((r) => r.result())), ids.map((id) => (id.startsWith("hops") ? 12 : 9)));
  assert.ok(inline > 0, "some deliveries happened at completion");
  for (const id of ids) {
    const replayer = new Replayer();
    define(replayer);
    const report = await replayer.replay(engine.exportJournal(id));
    assert.ok(report.ok && report.finished, `${id}: ${JSON.stringify(report)}`);
  }
  await engine.close();
});

test("bytes cross without JSON and come back as bytes, also after a restart", async () => {
  const dir = mkdtempSync(join(tmpdir(), "fusee-raw-"));
  try {
    let ran = 0;
    const define = (engine) =>
      engine.workflow("blob", async (ctx, input) => {
        const a = await ctx.step("make", () => {
          ran += 1;
          return Buffer.concat([Buffer.from([0, 255]), Buffer.from("raw"), input]);
        });
        const b = await ctx.step("json", () => ({ n: a.length }));
        const more = await ctx.waitFor("more");
        return Buffer.concat([a, more, Buffer.from(String(b.n))]);
      });
    const want = Buffer.from("\x00\xffrawin!7", "latin1");
    let engine = await Engine.open({ dir, flush: "fsync" });
    const run = await define(engine).start("b-1", new Uint8Array([105, 110]));
    await run.signal("more", Buffer.from("!"));
    const out = await run.result();
    assert.ok(out instanceof Uint8Array);
    assert.deepEqual(Buffer.from(out), want);
    await engine.close();
    // Recovery replays the recorded bytes; the step does not run again.
    engine = await Engine.open({ dir, flush: "fsync" });
    define(engine);
    assert.deepEqual(Buffer.from(await engine.run("b-1").result()), want);
    assert.equal(ran, 1);
    await engine.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("turns a native driver runs keep the order replay reproduces", async () => {
  const engine = await Engine.open({ durability: "memory", keepHistory: true, driver: "native" });
  if (!engine.native.driving()) {
    // No native driver on this runtime (Bun): nothing to check.
    await engine.close();
    return;
  }
  const define = (host) =>
    host.workflow("mix", async (ctx) => {
      // Branches that spend different numbers of microtasks between steps.
      const branch = async (tag, hops) => {
        let s = 0;
        for (let i = 0; i < 5; i++) {
          for (let h = 0; h < hops; h++) await null;
          s += await ctx.step(`${tag}-${i}`, () => i);
        }
        return s;
      };
      const parts = await ctx.all([branch("a", 0), branch("b", 2), branch("c", 5)]);
      // A race whose losing wait is dropped.
      const raced = await ctx.race([ctx.waitFor("never", { timeout: 50 }), ctx.step("fast", () => "fast")]);
      return { parts, raced };
    });
  const wf = define(engine);
  const ids = Array.from({ length: 10 }, (_, i) => `m-${i}`);
  const runs = await Promise.all(ids.map((id) => wf.start(id, null)));
  for (const r of runs) assert.deepEqual(await r.result(), { parts: [10, 10, 10], raced: "fast" });
  for (const id of ids) {
    const replayer = new Replayer();
    define(replayer);
    const report = await replayer.replay(engine.exportJournal(id));
    assert.ok(report.ok && report.finished, `${id}: ${JSON.stringify(report)}`);
  }
  await engine.close();
});
