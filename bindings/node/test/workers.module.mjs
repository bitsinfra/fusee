// Workflows for the worker-thread tests (workers.test.mjs): each worker
// thread imports this module and registers them on its own engine.
import { threadId } from "node:worker_threads";

export default function register(engine) {
  // Which thread ran the code.
  engine.workflow("where", async (ctx, x) => ctx.step("where", () => ({ x, thread: threadId, id: ctx.workflowId })));
  engine.workflow("chain", async (ctx, n) => {
    let sum = 0;
    for (let i = 0; i < n; i++) sum += await ctx.step("s", () => i);
    return sum;
  });
  engine.workflow("waiter", async (ctx) => {
    const v = await ctx.waitFor("go");
    return ctx.step("got", () => ({ v, thread: threadId }));
  });
  // Children and signals across workers: `ids` lists child IDs.
  engine.workflow("parent", async (ctx, ids) => {
    const kids = await ctx.all(ids.map((id) => ctx.child("where", id, id)));
    return kids.map((k) => k.thread);
  });
  engine.workflow("pinger", async (ctx, target) => {
    ctx.signal(target, "go", `from ${ctx.workflowId}`);
    return ctx.step("sent", () => threadId);
  });
  engine.workflow("counter", async (ctx) => {
    let n = 0;
    ctx.onQuery("n", () => n);
    ctx.onUpdate("add", { validate: (k) => (k > 0 ? undefined : "positive only"), handle: (k) => (n += k) });
    await ctx.waitFor("stop");
    return n;
  });
  engine.workflow("bytes", async (ctx, b) => ctx.step("b", () => new Uint8Array([...b, 9])));
  engine.workflow("boom", async (ctx) => {
    await ctx.step("boom", () => {
      throw new Error("kaput");
    });
  });
  engine.workflow("async", async (ctx) => {
    let token = null;
    ctx.onQuery("token", () => token);
    return ctx.awaitCompletion("ext", (c) => {
      token = c.token;
    });
  });
  // Starts another workflow through its own engine (forwarded when another
  // worker owns the ID).
  engine.workflow("starter", async (ctx, id) => {
    await ctx.step("start", () => engine.workflow("where").start(id, "started").then(() => null));
    return ctx.step("result", () => engine.run(id).result());
  });
  // A step under a limit the main thread defines for every worker.
  engine.workflow("limited", async (ctx) =>
    ctx.step("hold", async () => {
      const began = Date.now();
      await new Promise((r) => setTimeout(r, 40));
      return [began, Date.now(), threadId];
    }, { limit: "one" }),
  );
  engine.workflow("sleeper", async (ctx) => {
    await ctx.sleep("nap", "1h");
    return "woke";
  });
  // Debounced and batched runs (their coordinators are native workflows).
  engine.workflow("echo", async (ctx, v) => ctx.step("v", () => ({ v, thread: threadId })));
  engine.workflow("collect", async (ctx, events) => ctx.step("all", () => events));
  // Batches from workflow code, through this worker's engine (forwarded
  // when another worker owns the coordinator).
  engine.workflow("feeder", async (ctx, [key, events]) => {
    const ids = [];
    for (const [i, e] of events.entries())
      ids.push(await ctx.step(`send-${i}`, async () => (await engine.workflow("collect").batch(key, e, { maxEvents: 2, maxWait: "1m", id: ctx.key() })).id));
    return ids;
  });
  // A keyed object (its calls are routed to the worker that owns the key)
  // and a workflow that calls it.
  engine.object("ocounter", {
    bump: async (ctx, k) => {
      const n = (ctx.state.get("n") ?? 0) + k;
      ctx.state.set("n", n);
      return n;
    },
  });
  engine.workflow("bumper", async (ctx, key) => ctx.object("ocounter", key).call("bump", 1));
  // A step under a limit, queued with a priority.
  engine.workflow("ranked", async (ctx, level) =>
    ctx.step("hold", async () => {
      await new Promise((r) => setTimeout(r, 5));
      return [Date.now(), level];
    }, { limit: "ranked", priority: level, fairnessKey: `k${level % 2}` }),
  );
}
