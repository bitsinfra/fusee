// Simultaneous wakes on one Node thread: `count` workflows on one engine
// (one JavaScript thread, `shards` shards, local durability) each sleep
// until the same instant, parking meanwhile; then all wake at once.
//
//   node node_wakes.mjs <dir> <count> [shards] [margin seconds]
//
// Every workflow is started (each start acknowledged durable) before the
// common deadline, which is `margin` seconds after the first start; a trial
// whose admission overran it is reported as such. Lateness is the time from
// the deadline to the moment the workflow's code ran again after its
// sleep, taken in this process (outside the journal).
// One JSON line: admission, parked runs before the deadline, lateness
// percentiles in ms, the wake rate, and RSS.
import { pathToFileURL } from "node:url";
import { rmSync } from "node:fs";

const { Engine } = await import(process.env.FUSEE_BENCH_NODE_SDK
  ? pathToFileURL(process.env.FUSEE_BENCH_NODE_SDK).href
  : "../../../bindings/node/dist/index.js");
const [dir, countArg = "1000000", shardsArg = "8", marginArg] = process.argv.slice(2);
const count = Number(countArg), shards = Number(shardsArg);
// About 25,000 starts per second on a slow host, with room.
const margin = Number(marginArg ?? Math.max(20, Math.ceil(count / 15_000) + 10)) * 1000;
rmSync(dir, { recursive: true, force: true });
const engine = await Engine.open({ dir, durability: "local", shards, parkAfter: "1s", flush: process.env.FUSEE_BENCH_FLUSH });
const woke = new Float64Array(count).fill(NaN);
let entered = 0;
const wf = engine.workflow("sleeper", async (ctx, input) => {
  const { i, deadline } = input;
  entered += 1;
  await ctx.sleep("wake", Math.max(0, deadline - ctx.now()));
  // Outside the journal: when this code ran again (a replay of a woken,
  // parked run passes here once, at its wake).
  if (Number.isNaN(woke[i])) woke[i] = Date.now();
  return i;
});
const sum = (k) => engine.shardMetrics().reduce((a, m) => a + m[k], 0);
const t0 = Date.now();
const deadline = t0 + margin;
// Admission: a bounded number of starts in flight, each awaited durable.
const inflight = Number(process.env.FUSEE_WAKE_INFLIGHT ?? 2048);
let next = 0;
async function admit() {
  while (next < count) {
    const i = next++;
    await wf.start(`s-${i}`, { i, deadline });
  }
}
await Promise.all(Array.from({ length: inflight }, admit));
const admittedMs = Date.now() - t0;
const overran = Date.now() >= deadline;
// Ask for every workflow's result now, while the runs sleep: a million
// calls take about a second of this thread, which done just before the
// deadline would delay every wake by the harness's own work.
const done = { n: 0 };
const results = [];
for (let i = 0; i < count; i += 1) results.push(engine.run(`s-${i}`).result().then(() => { done.n += 1; }));
const registeredMs = Date.now() - t0;
// Wait for the deadline, noting how many runs parked.
while (Date.now() < deadline - 200) await new Promise((r) => setTimeout(r, 100));
const parked = sum("runs_parked");
const rssBefore = process.memoryUsage().rss;
const lateRegistration = registeredMs >= margin - 200;
await Promise.all(results);
const finishedMs = Date.now() - deadline;
const lateness = Array.from(woke, (w) => w - deadline).sort((a, b) => a - b);
const missing = lateness.filter((x) => Number.isNaN(x)).length;
const pct = (q) => lateness[Math.min(lateness.length - 1, Math.round((lateness.length - 1) * q))];
const report = {
  engine: "fusee-node", version: process.version, workload: "simultaneous-wakes", count, shards,
  threads: 1, admitted_ms: admittedMs, admission_overran_deadline: overran || lateRegistration,
  results_registered_ms: registeredMs, entered, parked_before_deadline: parked,
  margin_ms: margin, missing,
  lateness_ms: { min: lateness[0], p10: pct(0.1), p50: pct(0.5), p90: pct(0.9), p99: pct(0.99), max: lateness[lateness.length - 1 - missing] },
  wakes_per_second: count / Math.max(0.001, (lateness[lateness.length - 1 - missing] - Math.max(0, lateness[0])) / 1000),
  finished_ms_after_deadline: finishedMs, rss_bytes_parked: rssBefore, rss_bytes_end: process.memoryUsage().rss,
  machine: { platform: process.platform, arch: process.arch, instance_type: process.env.FUSEE_INSTANCE_TYPE ?? null },
};
console.log(JSON.stringify(report));
await engine.close();
rmSync(dir, { recursive: true, force: true });
