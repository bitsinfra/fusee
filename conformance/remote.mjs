// Companion to remote.py. Both clients drive both worker languages through fusee server.
import assert from "node:assert/strict";
import { Client, Engine } from "../bindings/node/dist/index.js";

const [role, address, tag = ""] = process.argv.slice(2);
if (role === "worker") {
  const engine = await Engine.open({ durability: "server", address });
  engine.workflow("approval", async (ctx, initial) => {
    let count = initial, token = null;
    ctx.onUpdate("add", (n) => count += n);
    ctx.onQuery("count", () => count);
    ctx.onQuery("token", () => token);
    const approved = await ctx.awaitCompletion("decision", async (c) => { token = c.token; });
    return [count, approved];
  });
  // Debounced and batched runs, their steps queued for a limit by priority.
  engine.limit("remote", { concurrent: 1 });
  engine.workflow("echo", async (ctx, v) => ctx.step("v", () => v, { limit: "remote", priority: 2, fairnessKey: "t", fairnessWeight: 2 }));
  engine.workflow("collect", async (ctx, events) => ctx.step("all", () => events, { limit: "remote", priority: -1 }));
  // A step that streams tokens, then bytes.
  engine.workflow("talk", async (ctx, n) => ctx.step("generate", async () => {
    const out = ctx.stream("tokens");
    for (let i = 0; i < n; i++) {
      out.append(`t${i}`);
      await new Promise((r) => setTimeout(r, 5));
    }
    out.append(new Uint8Array([1, 2]));
    return n;
  }));
  // A key of an object type, called by both clients.
  engine.object("tally", {
    add: (ctx, n) => {
      const sum = (ctx.state.get("sum") ?? 0) + n;
      ctx.state.set("sum", sum);
      return sum;
    },
  });
  console.log("READY");
  process.once("SIGTERM", async () => { await engine.close(); process.exit(0); });
} else {
  const client = await Client.connect(address);
  try {
    const id = "from-js" + tag;
    const started = await client.updateWithStart("approval", id, 4, "add", 3, { id: "once" });
    assert.equal(started.result, 7);
    assert.equal(started.run.id, id);
    const run = client.run(id);
    assert.equal(await run.update("add", 3, { id: "once" }), 7);
    assert.equal(await run.query("count"), 7);
    let token;
    for (let i = 0; i < 100 && !token; i++) {
      token = await run.query("token");
      if (!token) await new Promise((r) => setTimeout(r, 10));
    }
    assert.equal(typeof token, "string");
    await client.complete(token, true);
    assert.deepEqual(await run.result(), [7, true]);
    assert((await client.list({ status: "ok" })).some((r) => r.id === id && typeof r.startedMs === "number"));
    // Two starts inside the window: one run, the latest input.
    const first = await client.debounce("echo", `d-${id}`, 1, { window: 500, id: "d1" });
    const latest = await client.debounce("echo", `d-${id}`, 2, { window: 500, id: "d2" });
    assert.equal(first.id, latest.id);
    await first.started();
    assert.equal(await first.result(), 2);
    // A repeated event ID adds nothing; the batch closes on its wait.
    const events = [];
    for (const e of ["a", "b", "b"]) events.push(await client.batch("collect", `b-${id}`, e, { maxEvents: 3, maxWait: 300, id: e }));
    assert.equal(new Set(events.map((r) => r.id)).size, 1);
    await events[0].started();
    assert.deepEqual(await events[0].result(), ["a", "b"]);
    // A stream read through the server, then again from where it stopped.
    await client.start("talk", `s-${id}`, 10);
    const r = client.run(`s-${id}`).stream("tokens");
    const got = [];
    for await (const c of r) {
      got.push(c);
      if (got.length === 4) break;
    }
    const rest = client.run(`s-${id}`).stream("tokens", { from: r.offset });
    for await (const c of rest) got.push(c);
    assert.deepEqual(got.slice(0, 10), Array.from({ length: 10 }, (_, i) => `t${i}`));
    assert.deepEqual([...got[10]], [1, 2]);
    assert.equal(got.length, 11);
    assert.equal(rest.status, "ok");
    // A key of an object type: a call, its retry, a one-way call, the state.
    const k = client.objectRef("tally", id);
    assert.equal(await k.call("add", 2, { id: "o1" }), 2);
    assert.equal(await k.call("add", 2, { id: "o1" }), 2);
    await k.send("add", 3, { id: "o2" });
    for (let i = 0; i < 200 && (await k.state()).sum !== 5; i++) await new Promise((r) => setTimeout(r, 10));
    assert.deepEqual(await k.state(), { sum: 5 });
    console.log("PASS Node client");
  } finally { client.close(); }
}
