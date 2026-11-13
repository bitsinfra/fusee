// Temporal: one activity per step (durable before the next step starts).
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { fileURLToPath } from "node:url";
import { finishRun, report, sdkVersion, stamp } from "../common.mjs";
const steps = Number(process.argv[2] ?? 100);
const runs = Number(process.argv[3] ?? 10);
const useLocal = process.env.TEMPORAL_LOCAL_ACTIVITIES === "1";
const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const activities = { async noop(value) { return stamp(() => value + 1); } };
const worker = await Worker.create({
  connection: await NativeConnection.connect({ address }),
  taskQueue: "bench",
  workflowsPath: fileURLToPath(new URL("./workflows.mjs", import.meta.url)),
  activities,
});
const running = worker.run();
const client = new Client({ connection: await Connection.connect({ address }) });
const whole = [];
for (let r = -1; r < runs; r++) {
  const t = process.hrtime.bigint();
  const result = await client.workflow.execute("chain", { taskQueue: "bench", workflowId: `chain-${Date.now()}-${r}`, args: [steps,useLocal] });
  if (result !== steps) throw new Error(`wrong result: ${result}`);
  finishRun(steps, t, whole, r < 0);
}
report(useLocal ? "temporal-local-activities" : process.env.TEMPORAL_TUNED === "1" ? "temporal-tuned" : "temporal", sdkVersion(import.meta.url, "@temporalio/client"), useLocal ? "local activities recorded at workflow task completion" : "each activity's events recorded before the workflow continues", steps, whole);
worker.shutdown();
await running;
