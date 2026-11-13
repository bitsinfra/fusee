// The Node SDK cells, on the mounted disk.
//
//   node node_cells.mjs <dir> live <shards> <busy> <idle> <steps> [repeats]
//   node node_cells.mjs <dir> fanout <shards> <width> <runs>
//
// `live`: `busy` chains of `steps` no-op steps run at once, each on its own
// shard where there are enough, while `idle` more workflows are live and
// waiting for a signal on the other shards (busy + idle = 8: "eight live
// workflows"). Each chain is warmed up first (the JIT, the
// log), so the figures are the steady state. One JSON line per repeat:
// the step-to-step gap inside each chain (taken in the step functions, as
// node.mjs does), engine flushes per step and how many ran inline.
//
// `fanout`: a workflow starts `width` no-op steps at once and returns when
// all are done; from start to result, `runs` times after a warm-up.
//
// FUSEE_BENCH_NODE_SDK overrides the SDK (another build of dist/index.js);
// FUSEE_SDK_FLUSH (inline, pipeline) and FUSEE_DRIVER (native, macrotask)
// pick the flush mode and the turn driver; FUSEE_BENCH_FLUSH the flush call
// (`fsync` for development figures on macOS).
import { pathToFileURL } from "node:url";
import { rmSync } from "node:fs";

const { Engine } = await import(process.env.FUSEE_BENCH_NODE_SDK
  ? pathToFileURL(process.env.FUSEE_BENCH_NODE_SDK).href
  : "../../../bindings/node/dist/index.js");
const [dir, kind, ...args] = process.argv.slice(2);
const pct = (v, q) => (v.length ? v[Math.min(v.length - 1, Math.round((v.length - 1) * q))] : null);
const stats = (v) => {
  const s = [...v].sort((a, b) => a - b);
  return { count: s.length, p50_us: pct(s, 0.5), p90_us: pct(s, 0.9), p99_us: pct(s, 0.99), p999_us: pct(s, 0.999), max_us: s.at(-1) ?? null };
};
const machine = { os: process.platform, arch: process.arch, instance_type: process.env.FUSEE_INSTANCE_TYPE ?? null, revision: process.env.FUSEE_REVISION ?? null };
const settings = { flush: process.env.FUSEE_SDK_FLUSH ?? "default", driver: process.env.FUSEE_DRIVER ?? "default" };

rmSync(dir, { recursive: true, force: true });

async function live(shards, busy, idle, steps, repeats) {
  // The waiting workflows stay loaded (live), not parked, for the whole cell.
  const engine = await Engine.open({ dir, durability: "local", shards, parkAfter: "1h", flush: process.env.FUSEE_BENCH_FLUSH });
  const sum = (k) => engine.shardMetrics().reduce((a, m) => a + m[k], 0);
  const marks = new Map();
  const chain = engine.workflow("chain", async (ctx, n) => {
    const mine = [];
    marks.set(ctx.workflowId, mine);
    let v = 0;
    for (let i = 0; i < n; i++) {
      v = await ctx.step("noop", () => {
        const a = process.hrtime.bigint();
        const r = v + 1;
        mine.push(a, process.hrtime.bigint());
        return r;
      });
    }
    return v;
  });
  const waiter = engine.workflow("waiter", async (ctx) => (await ctx.waitFor("go")) ?? 0);
  // IDs by shard: busy chains first on distinct shards, then the waiters.
  const shardOf = (id) => engine.native.result(id).shard;
  const pick = (prefix, n, avoid) => {
    const out = [];
    const used = new Set(avoid);
    for (let i = 0; out.length < n; i++) {
      const id = `${prefix}-${i}`;
      const s = shardOf(id);
      if (used.size < shards && used.has(s)) continue;
      used.add(s);
      out.push([id, s]);
    }
    return out;
  };
  const busyIds = pick("chain", busy, []);
  const idleIds = pick("wait", idle, busyIds.map(([, s]) => s));
  for (const [id] of idleIds) await waiter.start(id, null);
  const rows = [];
  for (let r = -1; r < repeats; r++) {
    // Repeat -1 warms up with the same shape.
    const n = r < 0 ? Math.min(steps, 2000) : steps;
    marks.clear();
    const f0 = sum("flushes"), i0 = sum("inline_flushes"), u0 = sum("flush_us_total");
    const t0 = process.hrtime.bigint();
    await Promise.all(busyIds.map(async ([id]) => (await chain.start(`${id}-${r}`, n)).result()));
    const wall = Number(process.hrtime.bigint() - t0) / 1000;
    if (r < 0) continue;
    const gaps = [];
    for (const m of marks.values()) for (let i = 2; i < m.length; i += 2) gaps.push(Number(m[i] - m[i - 1]) / 1000);
    const flushes = sum("flushes") - f0;
    rows.push({ engine: "fusee-node", version: process.version, workload: "live", shards, busy, idle, steps,
      step_to_step: stats(gaps), steps_per_second: (busy * steps) / (wall / 1e6),
      flushes_per_step: flushes / (busy * steps), inline_share: (sum("inline_flushes") - i0) / Math.max(1, flushes),
      flush_us: (sum("flush_us_total") - u0) / Math.max(1, flushes), machine, settings, repeat: r });
  }
  for (const [id] of idleIds) await engine.run(id).signal("go", 1);
  for (const [id] of idleIds) await engine.run(id).result();
  await engine.close();
  for (const row of rows) console.log(JSON.stringify(row));
}

async function fanout(shards, width, runs) {
  const engine = await Engine.open({ dir, durability: "local", shards, flush: process.env.FUSEE_BENCH_FLUSH });
  const sum = (k) => engine.shardMetrics().reduce((a, m) => a + m[k], 0);
  const fan = engine.workflow("fan", async (ctx, n) => {
    const all = [];
    for (let i = 0; i < n; i++) all.push(ctx.step(`s${i}`, () => i));
    return (await Promise.all(all)).length;
  });
  for (let i = 0; i < 50; i++) await (await fan.start(`warm-${i}`, width)).result();
  const whole = [];
  const f0 = sum("flushes"), t0 = engine.turn;
  for (let i = 0; i < runs; i++) {
    const a = process.hrtime.bigint();
    if ((await (await fan.start(`fan-${i}`, width)).result()) !== width) throw new Error("incorrect fan-out result");
    whole.push(Number(process.hrtime.bigint() - a) / 1000);
  }
  const report = { engine: "fusee-node", version: process.version, workload: "fanout", shards, width, runs,
    workflow: stats(whole), flushes_per_workflow: (sum("flushes") - f0) / runs, turns_per_workflow: (engine.turn - t0) / runs,
    machine, settings };
  await engine.close();
  console.log(JSON.stringify(report));
}

try {
  if (kind === "live") await live(Number(args[0] ?? 8), Number(args[1] ?? 8), Number(args[2] ?? 0), Number(args[3] ?? 1000), Number(args[4] ?? 3));
  else if (kind === "fanout") await fanout(Number(args[0] ?? 8), Number(args[1] ?? 100), Number(args[2] ?? 500));
  else throw new Error(`unknown case '${kind}' (live, fanout)`);
} finally {
  rmSync(dir, { recursive: true, force: true });
}
