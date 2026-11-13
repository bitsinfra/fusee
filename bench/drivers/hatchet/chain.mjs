// Each task consumes its predecessor's committed output.
import { randomUUID } from "node:crypto";
import { Hatchet } from "@hatchet-dev/typescript-sdk";
import { finishRun, report, sdkVersion, stamp } from "../common.mjs";
const steps = Number(process.argv[2] ?? 100);
const runs = Number(process.argv[3] ?? 10);
const hatchet = Hatchet.init();
const wf = hatchet.workflow({ name: `chain-${steps}-${randomUUID().slice(0,8)}` });
const ends=new Map();
let activeAttempt;
let prev;
for (let i = 0; i < steps; i++) {
  const parent = prev;
  prev = wf.task({ name: `noop-${i}`, parents: parent ? [parent] : [], fn: async (input, ctx) => {
    const value = parent ? (await ctx.parentOutput(parent)).value : 0;
    const result = input.attempt===activeAttempt ? stamp(() => ({ value: value + 1 })) : {value:value+1};
    if (steps >= 1000 && (i + 1) % 100 === 0) console.error(`completed task ${i + 1}/${steps} at ${new Date().toISOString()}`);
    if (i+1===steps) ends.set(input.attempt,result.value);
    return result;
  } });
}
const worker = await hatchet.worker("fusee-bench", { workflows: [wf], slots: 100 });
worker.start();
await new Promise((r) => setTimeout(r, 2000));
const whole = [];
for (let r = -1; r < runs; r++) {
  const t = process.hrtime.bigint();
  const attempt=randomUUID();
  activeAttempt=attempt;
  const ref=await wf.runNoWait({attempt});
  const id=await ref.runId;
  const deadline=Date.now()+1800_000;
  // The gRPC finished-event listener can drop the completion of a large DAG.
  // Read the authoritative terminal status for this exact run, then verify
  // the value produced by its last activity.
  for(;;) {
    let status;
    try {const response=await hatchet.api.v1WorkflowRunGetStatus(id);status=response.data;}
    catch(error) {
      // The run detail becomes visible shortly after runNoWait returns.
      if(error?.response?.status!==404)throw Error(`Hatchet status API HTTP ${error?.response?.status ?? "transport failure"}`);
    }
    if(status==="COMPLETED")break;
    if(["FAILED","CANCELLED"].includes(status)||Date.now()>deadline)throw Error(`Hatchet run ${id}: ${status}`);
    await new Promise(resolve=>setTimeout(resolve,50));
  }
  if(ends.get(attempt)!==steps)throw Error("incorrect Hatchet chain result");
  ends.delete(attempt);
  finishRun(steps, t, whole, r < 0);
}
report("hatchet", sdkVersion(import.meta.url, "@hatchet-dev/typescript-sdk"), "Postgres commit per task", steps, whole,{completion_observer:"exact run REST status polled at 50 ms"});
await worker.stop();
process.exit(0);
