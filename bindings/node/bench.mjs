// Per-step overhead of a sequential no-op chain, closed loop.
import { Engine } from "./dist/index.js";
import { rmSync } from "node:fs";
const mode = process.argv[2] ?? "memory";
const dir = `${process.env.TMPDIR ?? "/tmp"}/fusee-node-bench`;
rmSync(dir, { recursive: true, force: true });
const engine = await Engine.open(mode === "memory" ? { durability: "memory" } : { dir, flush: mode === "fsync" ? "fsync" : undefined });
const stamps = [];
const chain = engine.workflow("chain", async (ctx, n) => {
  for (let i = 0; i < n; i++) await ctx.step("noop", () => { stamps.push(performance.now()); return i; });
  return n;
});
const N = mode === "local" ? 300 : 3000;
await (await chain.start("solo", N)).result();
const gaps = stamps.slice(1).map((t, i) => (t - stamps[i]) * 1000).sort((a, b) => a - b);
const p = (q) => gaps[Math.floor((gaps.length - 1) * q)].toFixed(1);
console.log(`${typeof Bun !== "undefined" ? "bun" : "node"} ${mode}: step-to-step p50 ${p(0.5)} µs  p99 ${p(0.99)} µs  (${N} steps)`);
// Throughput: concurrent workflows.
const W = 2000, S = 20;
const t0 = performance.now();
await Promise.all(Array.from({ length: W }, async (_, i) => (await chain.start(`w${i}`, S)).result()));
const el = (performance.now() - t0) / 1000;
console.log(`  ${W * S} durable steps in ${el.toFixed(2)} s = ${Math.round((W * S) / el)} steps/s`);
await engine.close();
