// Steps per second per worker thread (Engine.open({ workers })), closed
// loop: each worker runs `concurrency` chains of no-op steps at once.
//
//   node bench-workers.mjs [memory|local] [workers,...] [concurrency,...]
//   (FUSEE_BENCH_DIR: where `local` puts its directory; FUSEE_JSON=1: one
//   JSON line per configuration)
//   node bench-workers.mjs route [workers,...]
//
// (0 workers: the engine on the main thread, for comparison.)
// Reports, per configuration: steps/s in all and per worker thread, the
// step-to-step gap p50 and p99 inside each chain, and the scaling over one
// worker. FUSEE_COUNT=1 also counts native calls and engine ticks per step
// (the counting wrapper costs time, so it runs separately).
// This file is also the module each worker loads.
import { isMainThread, threadId } from "node:worker_threads";
import { rmSync } from "node:fs";
import { Engine } from "./dist/index.js";

const COUNT = process.env.FUSEE_COUNT === "1";
// `local`: the platform's default flush (on Linux O_DIRECT with RWF_DSYNC),
// except on macOS, where F_FULLFSYNC takes milliseconds and `fsync` stands
// in; FUSEE_FLUSH overrides either.
const FLUSH = process.env.FUSEE_FLUSH ?? (process.platform === "darwin" ? "fsync" : undefined);
const counts = {};

export default function register(engine) {
  if (COUNT) {
    // Count every call into the native engine on this thread (the methods,
    // and the step-path functions bound to its slot).
    const n = engine.native;
    const names = new Set([...Object.getOwnPropertyNames(Object.getPrototypeOf(n)), ...Object.keys(n)]);
    for (const k of names) {
      const f = n[k];
      if (typeof f !== "function" || k === "constructor") continue;
      n[k] = (...a) => {
        counts[k] = (counts[k] ?? 0) + 1;
        return f.apply(n, a);
      };
    }
  }
  // Returns the gaps between consecutive steps, in µs.
  engine.workflow("chain", async (ctx, steps) => {
    let last = await ctx.step("s", () => performance.now());
    const gaps = [];
    for (let i = 1; i < steps; i++) {
      const t = await ctx.step("s", () => performance.now());
      gaps.push((t - last) * 1000);
      last = t;
    }
    return gaps;
  });
  engine.workflow("counts", async (ctx) => ctx.step("counts", () => ({ thread: threadId, counts: { ...counts } })));
}

const pct = (sorted, q) => sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * q))];

async function trial(mode, workers, concurrency, steps) {
  const dir = `${process.env.FUSEE_BENCH_DIR ?? process.env.TMPDIR ?? "/tmp"}/fusee-node-bench-workers`;
  rmSync(dir, { recursive: true, force: true });
  const base = mode === "memory" ? { durability: "memory" } : { dir, ...(FLUSH ? { flush: FLUSH } : {}) };
  const opts = workers
    ? { ...base, workers, shards: workers, module: import.meta.url }
    : { ...base, shards: 1 };
  const engine = await Engine.open(opts);
  if (!workers) register(engine);
  const threads = workers || 1;
  const chain = engine.workflow("chain");
  // Warm up every worker.
  await Promise.all(Array.from({ length: threads * 4 }, async (_, i) => (await chain.start(`warm-${i}`, 20)).result()));
  const runs = threads * concurrency;
  // Exactly `concurrency` chains per worker: IDs hash to shards, so pick
  // them by owner (the engine's internal router; a benchmark-only peek).
  const ids = [];
  const per = new Array(threads).fill(0);
  for (let i = 0; ids.length < runs; i++) {
    const w = workers ? engine.group.route(`c-${i}`, false) : 0;
    if (per[w] < concurrency) {
      per[w] += 1;
      ids.push(`c-${i}`);
    }
  }
  const sum = (k) => engine.shardMetrics().reduce((a, m) => a + m[k], 0);
  const before = { flushes: sum("flushes"), inline: sum("inline_flushes") };
  const t0 = performance.now();
  const gaps = (await Promise.all(ids.map(async (id) => (await chain.start(id, steps)).result()))).flat();
  const seconds = (performance.now() - t0) / 1000;
  const flushes = sum("flushes") - before.flushes;
  const inline = sum("inline_flushes") - before.inline;
  let calls;
  if (COUNT) {
    // Enough runs to reach every worker; each reports its thread's counts.
    calls = {};
    for (let i = 0; i < threads * 8; i++) {
      const c = await (await engine.workflow("counts").start(`counts-${i}`, null)).result();
      calls[c.thread] = c.counts;
    }
  }
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
  gaps.sort((a, b) => a - b);
  const total = runs * steps;
  return { mode, workers, concurrency, steps: total, rate: total / seconds, p50: pct(gaps, 0.5), p99: pct(gaps, 0.99), calls, flushes, inline };
}

/** Routing alone: one-step workflows started and awaited from this thread. */
async function route(workers, n) {
  const opts = workers
    ? { durability: "memory", workers, shards: workers, module: import.meta.url }
    : { durability: "memory", shards: 1 };
  const engine = await Engine.open(opts);
  if (!workers) register(engine);
  const chain = engine.workflow("chain");
  await Promise.all(Array.from({ length: 200 }, async (_, i) => (await chain.start(`warm-${i}`, 1)).result()));
  const t0 = performance.now();
  await Promise.all(Array.from({ length: n }, async (_, i) => (await chain.start(`r-${i}`, 1)).result()));
  const rate = n / ((performance.now() - t0) / 1000);
  await engine.close();
  return rate;
}

async function main() {
  const mode = process.argv[2] ?? "memory";
  if (mode === "route") {
    for (const w of (process.argv[3] ?? "0,1,2,4").split(",").map(Number)) {
      const rate = await route(w, Number(process.env.FUSEE_RUNS ?? 20_000));
      console.log(`route ${w ? `${w} workers` : "main thread"}: ${Math.round(rate)} one-step workflows/s (start and result)`);
    }
    return;
  }
  const workerCounts = (process.argv[3] ?? "0,1,2,4").split(",").map(Number);
  const concurrencies = (process.argv[4] ?? "1,16,64").split(",").map(Number);
  const perThread = Number(process.env.FUSEE_STEPS ?? (mode === "memory" ? 200_000 : 60_000));
  const one = {};
  for (const c of concurrencies) {
    for (const w of workerCounts) {
      const steps = Math.max(20, Math.round(perThread / c));
      const r = await trial(mode, w, c, steps);
      const threads = w || 1;
      if (w === 1) one[c] = r.rate;
      const scale = w > 1 && one[c] ? ` x${(r.rate / one[c]).toFixed(2)} over 1 worker` : "";
      const label = w ? `${w} worker${w > 1 ? "s" : ""}` : "main thread";
      if (process.env.FUSEE_JSON === "1") {
        console.log(JSON.stringify({ engine: "fusee-node", workload: "workers", mode, flush: FLUSH ?? "default", workers: w, threads,
          chains_per_thread: c, steps: r.steps, rate: r.rate, rate_per_thread: r.rate / threads, gap_p50_us: r.p50, gap_p99_us: r.p99,
          steps_per_flush: r.steps / Math.max(1, r.flushes), inline_share: r.inline / Math.max(1, r.flushes) }));
        continue;
      }
      console.log(
        `${mode} ${label.padEnd(11)} ${String(c).padStart(4)} chains/thread: ${Math.round(r.rate).toString().padStart(7)} steps/s` +
          ` (${Math.round(r.rate / threads)} per thread)  gap p50 ${r.p50.toFixed(1)} µs  p99 ${r.p99.toFixed(1)} µs${scale}` +
          (mode === "local" ? `  ${(r.steps / Math.max(1, r.flushes)).toFixed(1)} steps/flush, ${Math.round((100 * r.inline) / Math.max(1, r.flushes))}% inline` : ""),
      );
      if (r.calls) {
        // Per step run on that thread (each runs `finishStep` once).
        for (const [t, c2] of Object.entries(r.calls)) {
          const steps = c2.finishStep ?? 1;
          const all = Object.values(c2).reduce((a, b) => a + b, 0);
          const per = Object.entries(c2)
            .filter(([, n]) => n / steps >= 0.01)
            .map(([k, n]) => `${k} ${(n / steps).toFixed(2)}`)
            .join(", ");
          console.log(`    thread ${t}: ${(all / steps).toFixed(2)} native calls per step (${per})`);
        }
      }
    }
  }
}

if (isMainThread) await main();
