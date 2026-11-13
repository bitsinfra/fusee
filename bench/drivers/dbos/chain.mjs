// DBOS: one step per step (a Postgres commit each).
import { DBOS } from "@dbos-inc/dbos-sdk";
import { finishRun, report, sdkVersion, stamp } from "../common.mjs";
const steps = Number(process.argv[2] ?? 100);
const runs = Number(process.argv[3] ?? 10);
const noop = DBOS.registerStep(async (value) => stamp(() => value + 1), { name: "noop" });
const chain = DBOS.registerWorkflow(async (n) => {
  let value = 0;
  for (let i = 0; i < n; i++) value = await noop(value);
  return value;
}, { name: "chain" });
DBOS.setConfig({ name: "bench", systemDatabaseUrl: process.env.DBOS_DATABASE_URL });
await DBOS.launch();
const whole = [];
for (let r = -1; r < runs; r++) {
  const t = process.hrtime.bigint();
  if (await chain(steps) !== steps) throw new Error("incorrect DBOS chain result");
  finishRun(steps, t, whole, r < 0);
}
report("dbos", sdkVersion(import.meta.url, "@dbos-inc/dbos-sdk"), "one Postgres commit per step (synchronous_commit=on)", steps, whole);
await DBOS.shutdown();
