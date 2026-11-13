// Conformance runner (TypeScript SDK). See conformance/README.md.
// FUSEE_WORKERS=n runs the workflows on n worker threads; this file is
// then also the module each worker loads (its default export).
import { isMainThread } from "node:worker_threads";
import { Engine, CancelledError, StepError, shared } from "./dist/index.js";

const r3 = { retry: { attempts: 3, initial: 1, max: 1 } };

function registerStreams(engine) {
  engine.workflow("streams", async (ctx) => {
    const p = ctx.stream("progress");
    p.append("start");
    const text = await ctx.step("generate", () => {
      const out = ctx.stream("tokens");
      for (const t of ["a", "b", "c"]) out.append(t);
      out.append(new Uint8Array([1, 2]));
      return "abc";
    });
    p.append({ text });
    p.close();
    return text;
  });
  engine.workflow("stream_retry", async (ctx) =>
    ctx.step("generate", () => {
      const out = ctx.stream("tokens");
      const prev = out.recorded();
      if (prev.length === 0) {
        out.append("x");
        out.append("y");
        throw new Error("cut off");
      }
      out.append("z");
      return prev.length + 1;
    }, r3),
  );
}

async function streams(engine, out) {
  for (const id of ["streams", "stream_retry"]) {
    await engine["startRaw"](id, 1, id, null, {});
    try { await engine.run(id).result(); } catch {}
    out[id] = engine.run(id).inspect().journal;
  }
  const r = engine.run("streams").stream("progress");
  const chunks = [];
  for await (const c of r) chunks.push(c);
  out["streams/read"] = { chunks, status: r.status };
}

function registerObjects(engine) {
  engine.object("conf_cart", {
    add: async (ctx, item) => {
      const priced = await ctx.step("price", () => `${item}$`);
      const items = ctx.state.get("items") ?? [];
      items.push(priced);
      ctx.state.set("items", items);
      ctx.state.set("last", items[items.length - 1]);
      return items.length;
    },
    remove: async (ctx, item) => {
      const items = ctx.state.get("items") ?? [];
      ctx.state.set("touched", true);
      const at = items.indexOf(item);
      if (at < 0) throw new Error(`missing ${item}`);
      items.splice(at, 1);
      ctx.state.set("items", items);
      return items.length;
    },
    get: shared((ctx) => ({ items: ctx.state.get("items") ?? null, keys: ctx.state.keys() })),
    clear: (ctx) => {
      ctx.state.clearAll();
      return ctx.state.keys().length;
    },
  });
  engine.object("conf_tally", {
    add: (ctx, n) => {
      const sum = (ctx.state.get("sum") ?? 0) + n;
      ctx.state.set("sum", sum);
      return sum;
    },
  });
  engine.workflow("obj_caller", async (ctx, key) => {
    const n = await ctx.object("conf_cart", key).call("add", "d");
    ctx.object("conf_cart", key).send("add", "e");
    return n;
  });
}

/** Keyed objects, on an engine that never parks (see the Rust runner). */
async function objects(workers, out) {
  const opts = { durability: "memory", shards: 2, keepHistory: true, parkAfter: 3_600_000 };
  const engine = workers
    ? await Engine.open({ ...opts, workers, module: import.meta.url })
    : await Engine.open(opts);
  if (!workers) registerObjects(engine);
  const k = engine.objectRef("conf_cart", "k1");
  const calls = [];
  calls.push(await k.call("add", "a", { id: "c1" }));
  calls.push(await k.call("add", "b", { id: "c2" }));
  calls.push(await k.call("add", "b", { id: "c2" }));
  calls.push(await k.call("remove", "zz", { id: "c3" }).catch(() => ({ error: true })));
  calls.push(await k.call("get", null, { id: "c4" }));
  await k.send("add", "c", { id: "s1" });
  await engine.workflow("obj_caller").start("obj_caller", "k1");
  calls.push(await engine.run("obj_caller").result());
  for (let i = 0; i < 2000; i++) {
    if ((await k.state()).items?.length === 5) break;
    await new Promise((r) => setTimeout(r, 1));
  }
  calls.push(await k.call("get", null, { id: "c5" }));
  calls.push(await k.call("clear", null, { id: "c6" }));
  calls.push(await k.call("add", "f", { id: "c7" }));
  calls.push(await k.call("fusee.state.edit", { set: { owner: "ana" }, patch: [{ op: "add", path: "/items/-", value: "g" }] }, { id: "c8" }));
  calls.push(await k.state());
  out["obj/calls"] = calls;
  out["obj/cart"] = k.inspect().journal;
  out["obj/caller"] = engine.run("obj_caller").inspect().journal;
  const t = engine.objectRef("conf_tally", "t");
  const sums = [];
  for (let n = 1; n <= 130; n++) sums.push(await t.call("add", n, { id: `t-${n}` }));
  sums.push(await t.call("add", 5, { id: "t-5" }));
  out["obj/sums"] = [sums[0], sums[127], sums[129], sums[130]];
  out["obj/tally_state"] = await t.state();
  out["obj/tally"] = t.inspect().journal;
  await engine.close();
}

export default function register(engine) {
  registerStreams(engine);
  registerObjects(engine);
  engine.workflow("seq", async (ctx, input) => {
    const a = await ctx.step("a", () => input.n + 1);
    const b = await ctx.step("b", () => a * 2);
    return ctx.step("c", () => ({ a, b }));
  });
  engine.workflow("parallel", async (ctx, n) => {
    const [x, y] = await ctx.all([ctx.step("x", () => n), ctx.step("y", () => n * 10)]);
    return ctx.step("z", () => x + y);
  });
  const tries = new Map();
  engine.workflow("retry", async (ctx) =>
    ctx.step("flaky", () => {
      const n = (tries.get(ctx.workflowId) ?? 0) + 1;
      tries.set(ctx.workflowId, n);
      if (n < 3) throw new Error(`try ${n}`);
      return "ok";
    }, r3),
  );
  engine.workflow("read_only", async (ctx) => {
    const v = await ctx.step("fetch", () => {
      const n = (tries.get(ctx.workflowId) ?? 0) + 1;
      tries.set(ctx.workflowId, n);
      if (n < 2) throw new Error(`try ${n}`);
      return "data";
    }, { ...r3, readOnly: true });
    return ctx.step("store", () => `${v}!`);
  });
  engine.workflow("signal", async (ctx) => {
    const v = await ctx.waitFor("go");
    return ctx.step("use", () => `${v}!`);
  });
  engine.workflow("double", async (ctx, x) => ctx.step("mul", () => x * 2));
  engine.workflow("child", async (ctx) => ctx.child("double", `${ctx.workflowId}/c`, 21));
  engine.workflow("sleep", async (ctx) => {
    await ctx.sleep("nap", 1);
    return ctx.step("after", () => "woke");
  });
  engine.workflow("values", async (ctx) => {
    ctx.now();
    ctx.random();
    ctx.uuid();
    return "values";
  });
  engine.workflow("cancel", async (ctx) => {
    try {
      return await ctx.waitFor("never");
    } catch (e) {
      if (!(e instanceof CancelledError)) throw e;
      await ctx.step("undo", () => e.reason);
      throw new CancelledError(`undone: ${e.reason}`);
    }
  });
  engine.workflow("fail", async (ctx) => {
    try {
      await ctx.step("boom", () => { throw new Error("boom"); }, { retry: { attempts: 1 } });
      return "unexpected";
    } catch (e) {
      if (!(e instanceof StepError)) throw e;
      return ctx.step("recover", () => "recovered");
    }
  });
  engine.workflow("listener", async (ctx) => (await ctx.waitFor("ping")) ?? "");
  engine.workflow("send", async (ctx, target) => {
    ctx.signal(target, "ping", "hello");
    return "sent";
  });
  engine.workflow("timeout_wait", async (ctx) => {
    const v = await ctx.waitFor("never", { timeout: 5 });
    return ctx.step("after", () => (v == null ? "none" : "some"));
  });

  // ---- the workflow features (docs/features.md)
  engine.workflow("updates", async (ctx) => {
    let total = 0;
    ctx.onUpdate("add", {
      validate: (v) => (v < 0 ? "negative" : undefined),
      handle: async (v) => {
        const d = await ctx.step("apply", () => v * 2);
        total += d;
        return total;
      },
    });
    ctx.onQuery("total", () => total);
    await ctx.waitFor("done");
    return total;
  });
  engine.workflow("update_start", async (ctx, start) => {
    let n = start;
    ctx.onUpdate("inc", (by) => (n += by));
    await ctx.waitFor("stop");
    return n;
  });
  engine.workflow("heartbeat", async (ctx) =>
    ctx.step("work", async () => {
      ctx.heartbeat(1);
      await new Promise((r) => setTimeout(r, 30));
      return ctx.progress() ?? 0;
    }),
  );
  engine.workflow("await", async (ctx) => {
    // A query hands the token to the driver, which may be another thread.
    let token = null;
    ctx.onQuery("token", () => token);
    return ctx.awaitCompletion("approval", (c) => { token = c.token; });
  });
  engine.workflow("continued", async (ctx, left) => {
    const v = await ctx.step("work", () => left);
    if (left === 0) return v;
    return ctx.continueAsNew(left - 1);
  });
  engine.workflow("patched", async (ctx) => {
    ctx.deprecatePatch("old");
    const label = ctx.patched("new") ? "new" : "old";
    return ctx.step("after", () => label);
  });
  engine.workflow("attributes", async (ctx) => {
    ctx.setAttributes({ customer: "c-1", amount: 5 });
    ctx.setAttributes({ stage: "paid" });
    return "attributes";
  });
  engine.workflow("kid", async (ctx) => (await ctx.waitFor("go")) ?? "");
  engine.workflow("parent_close", async (ctx) => {
    ctx.spawnChild("kid", "parent_close/terminate", null);
    ctx.spawnChild("kid", "parent_close/abandon", null, { parentClose: "abandon" });
    await ctx.waitFor("never");
    return "unreachable";
  });
  engine.workflow("reset", async (ctx) => {
    const a = await ctx.step("a", () => 1);
    const b = await ctx.step("b", () => a + 1);
    return ctx.step("c", () => b + 1);
  });
  engine.workflow("victim", async (ctx) => {
    try {
      await ctx.waitFor("never");
      return "unexpected";
    } catch (e) {
      if (!(e instanceof CancelledError)) throw e;
      await ctx.step("undo", () => e.reason);
      throw new CancelledError("undone");
    }
  });
  engine.workflow("canceller", async (ctx) => {
    ctx.cancelWorkflow("victim", "stop it");
    return "sent";
  });
  engine.workflow("race_wait", async (ctx) => {
    await ctx.race([ctx.waitFor("x"), ctx.sleep("nap", 1).then(() => "slept")]);
    return (await ctx.waitFor("x")) ?? "";
  });

  // ---- queues
  engine.workflow("prio", async (ctx, p) => {
    const a = await ctx.step("first", () => p.level, {
      limit: "conf", priority: p.level, fairnessKey: p.key, fairnessWeight: p.weight,
    });
    return ctx.step("second", () => `${p.key}:${a}`, { limit: "conf", priority: -p.level, fairnessKey: "other" });
  });
  engine.workflow("deb_target", async (ctx, v) => ctx.step("got", () => v));
  engine.workflow("batch_target", async (ctx, events) => ctx.step("got", () => events));
}

/** Queues: see `queues` in crates/fusee/examples/conformance.rs. */
/** Pause: see `pause` in crates/fusee/examples/conformance.rs. */
async function pause(engine, out) {
  let calls = 0;
  engine.workflow("pause", async (ctx) =>
    ctx.step("flaky", () => {
      calls += 1;
      if (calls < 3) throw new Error(`try ${calls}`);
      return "ok";
    }, { retry: { attempts: 2, initial: 1, max: 1, onExhausted: "pause" } }),
  );
  await engine["startRaw"]("pause", 1, "pause", null, {});
  for (let i = 0; i < 4000 && engine.run("pause").inspect()?.status !== "paused"; i++) {
    await new Promise((r) => setTimeout(r, 1));
  }
  await engine.run("pause").resume();
  try { await engine.run("pause").result(); } catch {}
  out.pause = engine.run("pause").inspect().journal;
}

async function queues(engine, out) {
  engine.limit("conf", { concurrent: 1 });
  const prio = [];
  for (let i = 0; i < 4; i++) {
    const id = `prio/${i}`;
    await engine["startRaw"]("prio", 1, id, { level: i, key: i % 2 ? "odd" : "even", weight: i + 1 }, {});
    prio.push(id);
  }
  const deb = engine.workflow("deb_target");
  const rule = { window: 500 };
  const [a, b, c] = await Promise.all([1, 2, 3].map((n) => deb.debounce("k", n, { ...rule, id: `d${n}` })));
  const again = await deb.debounce("k", 99, { ...rule, id: "d2" });
  if (![b, c, again].every((r) => r.id === a.id)) throw new Error("debounced starts joined different runs");
  await a.started();
  await a.result().catch(() => {});
  out["debounce/run"] = engine.run(a.id).inspect().journal;
  const bat = engine.workflow("batch_target");
  const full = { maxEvents: 3, maxWait: "60s" };
  const runs = [];
  for (const [id, e] of [["b1", "a"], ["b2", "b"], ["b2", "x"], ["b3", "c"]]) runs.push(await bat.batch("k", e, { ...full, id }));
  if (!runs.every((r) => r.id === runs[0].id)) throw new Error("batched events joined different runs");
  await runs[0].started();
  await runs[0].result().catch(() => {});
  out["batch/full"] = engine.run(runs[0].id).inspect().journal;
  const timed = await bat.batch("t", "z", { maxEvents: 100, maxWait: 20 });
  await timed.started();
  await timed.result().catch(() => {});
  out["batch/timed"] = engine.run(timed.id).inspect().journal;
  out["debounce/describe"] = await engine.run("fusee.debounce/deb_target/k").query("describe");
  out["batch/describe"] = await engine.run("fusee.batch/batch_target/k").query("describe");
  for (const id of prio) {
    await engine.run(id).result().catch(() => {});
    out[id] = engine.run(id).inspect().journal;
  }
}

async function main() {
  const workers = Number(process.env.FUSEE_WORKERS ?? 0);
  const opts = { durability: "memory", shards: 2, keepHistory: true };
  const engine = workers
    ? await Engine.open({ ...opts, workers, module: import.meta.url })
    : await Engine.open(opts);
  if (!workers) register(engine);
  const wf = (t) => ({ start: (id, input) => engine["startRaw"](t, 1, id, input, {}) });
  await wf("seq").start("seq", { n: 3 });
  await wf("parallel").start("parallel", 5);
  await wf("retry").start("retry", null);
  await wf("read_only").start("read_only", null);
  await wf("signal").start("signal", null);
  await engine.run("signal").signal("go", "hi");
  await wf("child").start("child", null);
  await wf("sleep").start("sleep", null);
  await wf("values").start("values", null);
  await wf("cancel").start("cancel", null);
  await wf("fail").start("fail", null);
  await wf("listener").start("listener", null);
  await wf("send").start("send", "listener");
  await wf("timeout_wait").start("timeout_wait", null);
  for (let i = 0; i < 100; i++) {
    const s = engine.run("cancel").inspect()?.status;
    if (s === "suspended" || s === "parked") break;
    await new Promise((r) => setTimeout(r, 1));
  }
  await engine.run("cancel").cancel("stop");
  const ids = ["seq", "parallel", "retry", "read_only", "signal", "child", "child/c", "sleep", "values", "cancel", "fail", "listener", "send", "timeout_wait"];
  const out = {};
  for (const id of ids) {
    try { await engine.run(id).result(); } catch {}
    out[id] = engine.run(id).inspect().journal;
  }

  // ---- the workflow features (docs/features.md)
  const until = async (id, states) => {
    for (let i = 0; i < 2000; i++) {
      if (states.includes(engine.run(id).inspect()?.status)) return;
      await new Promise((r) => setTimeout(r, 1));
    }
    throw new Error(`${id} never reached ${states}`);
  };
  await engine["startRaw"]("updates", 1, "updates", null, { idPolicy: { running: "use-existing" } });
  const up = engine.run("updates");
  const a = await up.update("add", 3, { id: "u1" });
  const b = await up.update("add", 3, { id: "u1" });
  const rejected = await up.update("add", -1).then(() => false, () => true);
  const total = await up.query("total");
  if (!(a === 6 && b === 6 && rejected && total === 6)) throw new Error(`updates ${a} ${b} ${rejected} ${total}`);
  await up.signal("done", null);
  const uws = await engine.workflow("update_start").updateWithStart("update_start", 10, "inc", 5);
  if (uws.result !== 15) throw new Error(`update_start ${uws.result}`);
  await engine.run("update_start").signal("stop", null);
  await wf("heartbeat").start("heartbeat", null);
  await wf("await").start("await", null);
  let token = null;
  while (!token) {
    token = await engine.run("await").query("token").catch(() => null);
    if (!token) await new Promise((r) => setTimeout(r, 1));
  }
  await until("await", ["suspended", "parked"]);
  await engine.complete(token, "approved");
  await wf("continued").start("continued", 2);
  await wf("patched").start("patched", null);
  await wf("attributes").start("attributes", null);
  await wf("parent_close").start("parent_close", null);
  await until("parent_close/terminate", ["suspended", "parked"]);
  await until("parent_close/abandon", ["suspended", "parked"]);
  await engine.run("parent_close").terminate("stop");
  await until("parent_close/terminate", ["terminated"]);
  await engine.run("parent_close/abandon").signal("go", "free");
  await wf("reset").start("reset", null);
  await engine.run("reset").result();
  await engine.run("reset").reset({ beforeStep: "b" });
  await wf("victim").start("victim", null);
  await until("victim", ["suspended", "parked"]);
  await wf("canceller").start("canceller", null);
  await wf("race_wait").start("race_wait", null);
  for (let i = 0; i < 2000; i++) {
    if ((engine.run("race_wait").inspect()?.journal.length ?? 0) >= 6) break;
    await new Promise((r) => setTimeout(r, 1));
  }
  await engine.run("race_wait").signal("x", "late");
  for (const id of ["updates", "update_start", "heartbeat", "await", "continued", "patched", "attributes", "parent_close",
    "parent_close/terminate", "parent_close/abandon", "reset", "victim", "canceller", "race_wait"]) {
    try { await engine.run(id).result(); } catch {}
    out[id] = engine.run(id).inspect().journal;
  }
  await streams(engine, out);
  await queues(engine, out);
  await pause(engine, out);
  await engine.close();
  await objects(workers, out);
  console.log(JSON.stringify(out, null, 2));
}

if (isMainThread) await main();
