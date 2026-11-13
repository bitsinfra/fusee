// Each checkpoint commits to PostgreSQL before the workflow continues.
import { Absurd } from "absurd-sdk";
import { finishRun, report, sdkVersion, stamp } from "../common.mjs";
const steps = Number(process.argv[2] ?? 100);
const runs = Number(process.argv[3] ?? 10);
const app = new Absurd({ db: process.env.ABSURD_DATABASE_URL, queueName: "bench" });
await app.createQueue();
app.registerTask({ name: "chain" }, async (params, ctx) => {
  let value = 0;
  for (let i = 0; i < params.n; i++) value = await ctx.step(`noop-${i}`, async () => stamp(() => value + 1));
  return value;
});
const worker = await app.startWorker({ concurrency: 4 });
const whole = [];
for (let r = -1; r < runs; r++) {
  const t = process.hrtime.bigint();
  const { taskID } = await app.spawn("chain", { n: steps });
  const result = await app.awaitTaskResult(taskID, { timeout: 120 });
  if (result.state !== "completed" || result.result !== steps) throw new Error(`Absurd task ${taskID}: ${JSON.stringify(result)}`);
  finishRun(steps, t, whole, r < 0);
}
report("absurd", sdkVersion(import.meta.url, "absurd-sdk"), "Postgres commit per checkpoint", steps, whole);
await worker.close();
process.exit(0);
