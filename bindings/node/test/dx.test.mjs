// Developer-experience checks for TypeScript: each mistake fails where it
// is wrong, with the thing named and the fix.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  Engine,
  asRequester,
  FatalError,
  StepError,
  UnknownWorkflowError,
  NotFoundError,
  FuseeError,
} from "../dist/index.js";

class CardDeclined extends FatalError {
  code = "card_declined";
}

test("a FatalError is not retried and keeps its type, code and details", async () => {
  const engine = await Engine.open({ durability: "memory" });
  let tries = 0;
  const wf = engine.workflow("pay", async (ctx) => {
    await ctx.step("charge", () => {
      tries += 1;
      throw new CardDeclined("card declined", { details: { last4: "4242" } });
    }, { retry: { attempts: 5, initial: 1 } });
  });
  await assert.rejects((await wf.start("p-1", null)).result(), (e) => {
    assert.ok(e instanceof StepError, String(e));
    assert.equal(e.step, "charge");
    assert.equal(e.cause.type, "CardDeclined");
    assert.equal(e.cause.code, "card_declined");
    assert.deepEqual(e.cause.details, { last4: "4242" });
    assert.equal(e.cause.retryable, false);
    assert.match(e.cause.stack ?? "", /CardDeclined/);
    assert.equal(e.code, "StepFailed");
    assert.match(e.docs, /errors\.md#stepfailed$/);
    return true;
  });
  assert.equal(tries, 1);
  await engine.close();
});

test("nonRetryable and retryIf stop retries", async () => {
  const engine = await Engine.open({ durability: "memory" });
  let tries = 0;
  const wf = engine.workflow("lookup", async (ctx) => {
    await ctx.step("get", () => {
      tries += 1;
      throw new RangeError("no such key");
    }, { retry: { attempts: 5, initial: 1, nonRetryable: ["RangeError"] } });
  });
  await assert.rejects((await wf.start("l-1", null)).result(), (e) => e instanceof StepError && e.cause.type === "RangeError");
  assert.equal(tries, 1);
  await engine.close();
});

test("an unregistered type fails at once with the nearest name", async () => {
  const engine = await Engine.open({ durability: "memory" });
  engine.workflow("checkout", async () => null);
  await assert.rejects(engine.workflow("chekout").start("x-1", null), (e) => {
    assert.ok(e instanceof UnknownWorkflowError, String(e));
    assert.match(e.message, /did you mean 'checkout'\?/);
    return true;
  });
  await assert.rejects(engine.run("nope").result(), (e) => e instanceof NotFoundError && e instanceof FuseeError);
  await engine.close();
});

test("ctx.key() outside a step stops the run", async () => {
  const engine = await Engine.open({ durability: "memory" });
  const wf = engine.workflow("keyed", async (ctx) => {
    ctx.key();
    await ctx.waitFor("never");
  });
  await wf.start("k-1", null);
  for (let i = 0; i < 200; i++) {
    const s = await engine.run("k-1").status();
    if (s.status === "stuck") break;
    await new Promise((r) => setTimeout(r, 5));
  }
  const s = await engine.run("k-1").status();
  assert.equal(s.status, "stuck", JSON.stringify(s));
  assert.match(s.error ?? "", /key\(\) was called outside a step/);
  await engine.close();
});

import { workflow, signal, update, query } from "../dist/index.js";

const addItem = signal("add");
const cart = workflow("cart", async (ctx, items) => {
  while (items.length < 2) items.push(await ctx.waitFor(addItem));
  return items;
});

test("definitions apart from the engine; signalWithStart; status and describe", async () => {
  const engine = await Engine.open({ durability: "memory", workflows: [cart] });
  const run = await engine.signalWithStart(cart, "cart-1", [], addItem, "apple");
  await engine.signalWithStart(cart, "cart-1", [], addItem, "pear");
  assert.deepEqual(await run.result(), ["apple", "pear"]);
  const s = await run.status();
  assert.equal(s.status, "ok");
  assert.equal(s.type, "cart");
  const d = await run.describe();
  assert.equal(d.journal[0].kind, "start");
  assert.deepEqual(engine.limits(), []);
  assert.ok(engine.metrics().runs_started >= 1);
  await engine.close();
});

test("compensations run newest first on failure", async () => {
  const undone = [];
  const trip = workflow("trip", async (ctx) => {
    for (const what of ["flight", "hotel"]) {
      await ctx.step(`book ${what}`, () => 1);
      ctx.compensate(`cancel ${what}`, () => undone.push(what));
    }
    await ctx.step("pay", () => {
      throw new FatalError("card declined");
    });
  });
  const engine = await Engine.open({ durability: "memory", workflows: [trip] });
  await assert.rejects((await engine.start(trip, "t-1", null)).result(), (e) => e instanceof StepError && e.step === "pay");
  assert.deepEqual(undone, ["hotel", "flight"]);
  const steps = (await engine.run("t-1").describe()).journal.filter((r) => r.kind === "step").map((r) => r.name);
  assert.deepEqual(steps, ["book flight", "book hotel", "pay", "cancel hotel", "cancel flight"]);
  await engine.close();
});

test("a delayed start runs later under its ID", async () => {
  const remind = workflow("remind", async (ctx, who) => `hi ${who}`);
  const engine = await Engine.open({ durability: "memory", workflows: [remind] });
  const began = Date.now();
  const run = await engine.start(remind, "rm-1", "ana", { delay: "200ms" });
  assert.equal(run.id, "rm-1");
  assert.equal(await run.result(), "hi ana");
  assert.ok(Date.now() - began >= 200);
  await engine.close();
});

test("the step context: key, attempt and an abort signal", async () => {
  const seen = [];
  const wf = workflow("ctx-steps", async (ctx) => {
    const a = await ctx.step("a", (s) => {
      seen.push([s.name, s.attempt, s.key.length > 0]);
      if (s.attempt < 2) throw new Error("again");
      return s.key;
    }, { retry: { attempts: 3, initial: 1 } });
    const b = await ctx.step("b", (s) => s.key);
    let aborted = "";
    try {
      await ctx.step("slow", (s) => new Promise((_, rej) => {
        s.signal.addEventListener("abort", () => { aborted = s.signal.reason.name; rej(s.signal.reason); });
      }), { timeout: 30, retry: { attempts: 1 } });
    } catch (e) {
      // the timeout's error
    }
    return [a !== b, aborted];
  });
  const engine = await Engine.open({ durability: "memory", workflows: [wf] });
  assert.deepEqual(await (await engine.start(wf, "c-1", null)).result(), [true, "TimeoutError"]);
  assert.deepEqual(seen, [["a", 1, true], ["a", 2, true]]);
  await engine.close();
});

test("engine.serve: the HTTP API answers through the engine's loop", async () => {
  const greet = workflow("greet-http", async (ctx, name) => {
    const g = await ctx.step("compose", () => `hello ${name}`);
    const ok = await ctx.waitFor(signal("approve"));
    return ok ? g : "no";
  });
  const engine = await Engine.open({ durability: "memory", workflows: [greet], http: "127.0.0.1:0" });
  // http opens it; find its URL through a second serve on a free port.
  const url = engine.serve({ addr: "127.0.0.1:0" });
  const call = async (method, path, body) => {
    const r = await fetch(url + path, { method, headers: { "Content-Type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body) });
    return [r.status, await r.json()];
  };
  assert.deepEqual(await call("POST", "/v1/runs", { type: "greet-http", id: "h-1", input: "web" }), [200, { id: "h-1" }]);
  const [s, bad] = await call("POST", "/v1/runs", { type: "nope", id: "h-2" });
  assert.equal(s, 400);
  assert.equal(bad.error.code, "UnknownWorkflow");
  assert.equal((await call("POST", "/v1/runs/h-1/signals/approve", true))[0], 200);
  const [, res] = await call("GET", "/v1/runs/h-1/result");
  assert.deepEqual(res, { status: "ok", value: "hello web" });
  const [, info] = await call("GET", "/v1/info");
  assert.equal(info.sdk, "typescript");
  const [, types] = await call("GET", "/v1/types");
  assert.deepEqual(types.map((t) => t.name), ["greet-http"]);
  await engine.close();
});

test("engine.serve: a malformed request answers 400, with no stack", async () => {
  const greet = workflow("greet-bad", async (ctx, name) => ctx.step("compose", () => `hello ${name}`));
  const engine = await Engine.open({ durability: "memory", workflows: [greet] });
  const url = engine.serve({ addr: "127.0.0.1:0" });
  const send = async (path, body) => {
    const r = await fetch(url + path, { method: "POST", headers: { "Content-Type": "application/json" }, body });
    return [r.status, await r.text()];
  };
  for (const [path, body] of [
    ["/v1/runs", "{}"],
    ["/v1/runs", "[]"],
    ["/v1/runs", "null"],
    ["/v1/runs", '{"type":"greet-bad"}'],
    ["/v1/runs", '{"type":123,"id":[1]}'],
    ["/v1/runs", '{"type":"greet-bad","id":"b-1","idPolicy":"bogus"}'],
    ["/v1/runs/b-1/reset", "{}"],
    ["/v1/call/start", '{"type":"greet-bad","id":5}'],
    ["/v1/call/list", '{"where":"x","limit":"y"}'],
    ["/v1/call/streamRead", '{"id":"x","name":"s","from":"z"}'],
    ["/v1/call/reset", '{"id":"x","beforeStep":7}'],
    ["/v1/call/complete", '{"token":5}'],
    ["/v1/call/nonsense", "{}"],
  ]) {
    const [status, text] = await send(path, body);
    assert.equal(status, 400, `${path} ${body}: ${text}`);
    assert.equal(JSON.parse(text).error.code, "Rejected", `${path} ${body}: ${text}`);
    assert.ok(!/stack|\bat (file|node):/.test(text), `${path} ${body}: ${text}`);
  }
  assert.equal((await send("/v1/runs", '{"type":"greet-bad","id":"b-2","input":"x","idPolicy":{"running":"use-existing"}}'))[0], 200);
  await engine.close();
});

test("Engine.open: a shard count outside 1 to 1024 is refused at once", async () => {
  for (const shards of [-1, 0, 1.5, NaN, Infinity, 1025, 2 ** 32 - 1]) {
    const started = Date.now();
    await assert.rejects(Engine.open({ durability: "memory", shards }), (e) => {
      assert.equal(e.code, "Rejected", `${shards}: ${e.message}`);
      assert.match(e.message, /from 1 to 1024/);
      return true;
    });
    assert.ok(Date.now() - started < 1000, `${shards} took ${Date.now() - started} ms`);
  }
  const engine = await Engine.open({ durability: "memory", shards: 3 });
  assert.equal(engine.shardMetrics().length, 3);
  await engine.close();
});

test("audit: signals and cancellations record who asked", async () => {
  const engine = await Engine.open({ durability: "memory", keepHistory: true, workflows: [cart] });
  const url = engine.serve({ addr: "127.0.0.1:0" });
  const run = await engine.start(cart, "aud-1", []);
  await asRequester("job:nightly", () => run.signal(addItem, "apple"));
  const r = await fetch(url + "/v1/runs/aud-1/signals/add", {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify("pear"),
  });
  assert.equal(r.status, 200);
  assert.deepEqual(await run.result(), ["apple", "pear"]);
  const by = (j, kind) => j.filter((x) => x.kind === kind).map((x) => x.by ?? null);
  assert.deepEqual(by((await run.describe()).journal, "signal"), ["job:nightly", "http"]);
  const run2 = await engine.start(cart, "aud-2", []);
  await run2.signal(addItem, "x");
  await asRequester("ops:ana", () => run2.cancel("not needed"));
  await assert.rejects(run2.result());
  const j = (await run2.describe()).journal;
  assert.deepEqual(by(j, "signal"), [null]);
  assert.equal([...by(j, "cancel"), ...by(j, "done")][0], "ops:ana");
  await engine.close();
});

test("awaiting something outside ctx stops the run and names the fix", async () => {
  const engine = await Engine.open({ durability: "memory" });
  const sleepy = workflow("sleepy", async (ctx) => {
    const a = await ctx.step("a", () => 1);
    await new Promise((r) => setTimeout(r, 20)); // not a ctx operation
    return a;
  });
  engine.register(sleepy);
  const run = await engine.start(sleepy, "s-1", null);
  let s;
  for (let i = 0; i < 200; i++) {
    s = await run.status();
    if (s.status === "stuck") break;
    await new Promise((r) => setTimeout(r, 5));
  }
  assert.equal(s.status, "stuck");
  assert.match(s.error, /awaited something else.*ctx\.step\(name, fn\)/);
  await engine.close();
});

test("runs at once per key take turns, in order", async () => {
  const report = workflow("report", { concurrency: { limit: 1, key: (input) => input.tenant } }, async (ctx, input) => {
    const go = await ctx.waitFor(signal("go"));
    return `${input.tenant}/${input.n}:${go}`;
  });
  const engine = await Engine.open({ durability: "memory", workflows: [report] });
  const runs = [];
  for (const [i, tenant] of ["a", "a", "b", "a"].entries()) runs.push(await engine.start(report, `r-${i}`, { tenant, n: i }));
  const live = async () => (await engine.list({ where: { type: "report" } })).filter((r) => r.finishedMs == null).map((r) => r.id).sort();
  const until = async (want) => {
    for (let i = 0; i < 400; i++) {
      const got = await live();
      if (JSON.stringify(got) === JSON.stringify(want)) return got;
      await new Promise((r) => setTimeout(r, 5));
    }
    return live();
  };
  // One run per tenant at a time: r-0 (a) and r-2 (b) run; r-1 and r-3 wait.
  assert.deepEqual(await until(["r-0", "r-2"]), ["r-0", "r-2"]);
  await runs[0].signal("go", 1);
  assert.equal(await runs[0].result(), "a/0:1");
  // r-1 takes tenant a's turn before r-3.
  assert.deepEqual(await until(["r-1", "r-2"]), ["r-1", "r-2"]);
  for (const i of [1, 2]) await runs[i].signal("go", i);
  assert.deepEqual(await until(["r-3"]), ["r-3"]);
  await runs[3].signal("go", 3);
  assert.deepEqual(await Promise.all(runs.map((r) => r.result())), ["a/0:1", "a/1:1", "b/2:2", "a/3:3"]);
  await engine.close();
});
