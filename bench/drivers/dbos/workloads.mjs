import { DBOS } from "@dbos-inc/dbos-sdk";
import { sdkVersion } from "../common.mjs";
import { executeSteps } from "../workloads/scenario.mjs";
import { activity, serveAdapter, waiting, resumed, externalSignal } from "../workloads/adapter.mjs";

const step = DBOS.registerStep(activity, {name:"matrix-increment"});
const run = DBOS.registerWorkflow(async input => {
  if (input.workload==="signal") {
    const receive=DBOS.recv("matrix-signal",90);
    waiting(input);
    return resumed(input,await receive);
  }
  return executeSteps(input,step);
}, {name:"matrix-workflow"});
DBOS.setConfig({name:"bench",systemDatabaseUrl:process.env.DBOS_DATABASE_URL});
await DBOS.launch();
await serveAdapter({name:"dbos-postgres",version:sdkVersion(import.meta.url,"@dbos-inc/dbos-sdk"),
  durability:"PostgreSQL, synchronous_commit=on"}, async input => {
  if(input.workload!=="signal") return run(input);
  const handle=await DBOS.startWorkflow(run,{workflowID:input.attempt})(input);
  return externalSignal(input,handle.getResult(),value=>DBOS.send(input.attempt,value,"matrix-signal"));
});
