// node --require count-native.cjs chain.mjs <package dir> <steps>: one memory
// engine, one sequential no-op chain; prints the native calls it made.
const [pkg, n] = [process.argv[2], Number(process.argv[3])];
const { Engine } = await import(`${pkg}/dist/index.js`);
const engine = await Engine.open({ durability: "memory" });
const chain = engine.workflow("chain", async (ctx, k) => {
  for (let i = 0; i < k; i++) await ctx.step("noop", () => i);
  return k;
});
await (await chain.start("warm", 50)).result();
const before = globalThis.__fuseeNativeCalls();
await (await chain.start("solo", n)).result();
const after = globalThis.__fuseeNativeCalls();
const per = {};
for (const [k, v] of Object.entries(after.counts)) { const d = v - (before.counts[k] || 0); if (d) per[k] = d; }
await engine.close();
console.log(JSON.stringify({ steps: n, calls: after.total - before.total, by: per }));
