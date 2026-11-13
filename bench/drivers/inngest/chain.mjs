// Inngest: step.run per step, served to the Inngest dev server.
// CHECKPOINT=1 enables checkpointing (asynchronous: the next step starts
// before the checkpoint is saved), which is labeled as not matching durability.
import http from "node:http";
import { randomUUID } from "node:crypto";
import { Inngest } from "inngest";
import { serve } from "inngest/node";
import { finishRun, report, sdkVersion, stamp } from "../common.mjs";
const steps = Number(process.argv[2] ?? 100);
const runs = Number(process.argv[3] ?? 10);
const checkpoint = process.env.CHECKPOINT === "1";
const origin = process.env.INNGEST_API_BASE_URL ?? "http://127.0.0.1:8288";
const inngest = new Inngest({ id: "fusee-bench", isDev: true });
const finished = new Map();
const chain = inngest.createFunction(
  { id: "chain", checkpointing: checkpoint ? { bufferedSteps: 10, maxInterval: "1s" } : false },
  { event: "bench/chain" },
  async ({ event, step }) => {
    let value = 0;
    for (let i = 0; i < event.data.n; i++) value = await step.run(`noop-${i}`, () => stamp(() => value + 1));
    finished.set(event.data.attempt,{ value, at:process.hrtime.bigint() });
    return value;
  },
);
const server = http.createServer(serve({ client: inngest, functions: [chain] }));
await new Promise((resolve) => server.listen(3939,"127.0.0.1",resolve));
const registration = await fetch("http://127.0.0.1:3939/api/inngest", {method:"PUT"});
if (!registration.ok) throw new Error(`Inngest registration: ${await registration.text()}`);
const whole = [], handlerUs = [], statusUs = [];
for (let r = -1; r < runs; r++) {
  const attempt = randomUUID();
  const t = process.hrtime.bigint();
  const { ids } = await inngest.send({ name: "bench/chain", data: { n: steps, attempt } });
  // Wait for the run to finish through the dev server's API.
  const deadline = Date.now() + 900000;
  let observed;
  for (;;) {
    const response = await fetch(`${origin}/v1/events/${ids[0]}/runs`);
    const runs = await response.json();
    const status = runs.data?.[0]?.status?.toLowerCase();
    if (status === "completed") {
      observed ??= process.hrtime.bigint();
      if (finished.get(attempt)?.value === steps) break;
    }
    const error = runs.data?.[0]?.output?.error;
    if (status === "failed" && error?.name === "InngestErrFunctionOverflowed") {
      console.log(JSON.stringify({engine:"inngest",version:sdkVersion(import.meta.url,"inngest"),workload:"chain",
        outcome:"unsupported",steps,reason:error.message,server_response:runs.data[0],result:[]}));
      process.exit(0);
    }
    if (["failed","cancelled"].includes(status) || Date.now() > deadline) throw new Error(`run did not complete: ${JSON.stringify(runs)}`);
    await new Promise((r) => setTimeout(r, 10));
  }
  const handler=finished.get(attempt);
  finished.delete(attempt);
  if (r>=0) {
    handlerUs.push(Number(handler.at-t)/1000);
    statusUs.push(Number(observed-t)/1000);
  }
  finishRun(steps, t, whole, r < 0);
}
report(
  checkpoint ? "inngest (checkpointing)" : "inngest",
  sdkVersion(import.meta.url,"inngest"),
  checkpoint ? "buffered checkpoints: up to ten steps before persistence" : "standard orchestration; see server persistence configuration",
  steps,
  whole,
  {handler_return_us:handlerUs, first_completed_status_us:statusUs},
);
process.exit(0);
