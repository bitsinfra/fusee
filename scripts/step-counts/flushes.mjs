// node flushes.mjs <package dir> <data dir> [fan-out]: flushes per step of a
// `local` engine over a 1,000-step chain and over 100 (or `fan-out`)
// one-step runs at once, from shardMetrics.
const [pkg, dir, fanArg = "100"] = process.argv.slice(2);
const fan = Number(fanArg);
const { Engine } = await import(`${pkg}/dist/index.js`);
const { rmSync } = await import("node:fs");
rmSync(dir, { recursive: true, force: true });
const engine = await Engine.open({ dir, shards: 1 });
const chain = engine.workflow("chain", async (ctx, k) => { for (let i = 0; i < k; i++) await ctx.step("noop", () => i); return k; });
const one = engine.workflow("one", async (ctx, i) => ctx.step("s", () => i));
const sum = (k) => (Array.isArray(engine.shardMetrics()) ? engine.shardMetrics() : JSON.parse(engine.shardMetrics())).reduce((a, m) => a + Number(m[k] ?? 0), 0);
const measure = async (f) => { const f0 = sum("flushes"), s0 = sum("steps_run"); await f(); return (sum("flushes") - f0) / (sum("steps_run") - s0); };
await (await chain.start("warm", 20)).result();
const chainFlushes = await measure(async () => (await chain.start("c", 1000)).result());
const fanFlushes = await measure(async () => Promise.all(Array.from({ length: fan }, async (_, i) => (await one.start(`f${i}`, i)).result())));
await engine.close();
console.log(JSON.stringify({ chain: +chainFlushes.toFixed(3), fanout: +fanFlushes.toFixed(3) }));
