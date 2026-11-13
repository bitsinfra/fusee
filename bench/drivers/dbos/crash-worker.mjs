import { DBOS } from "@dbos-inc/dbos-sdk";
import { effect, runChain, serveRecovery } from "../workloads/crash-worker.mjs";
const step=DBOS.registerStep(effect,{name:"crash-effect"});
const run=DBOS.registerWorkflow(input=>runChain(input,step),{name:"crash-workflow"});
DBOS.setConfig({name:"bench",systemDatabaseUrl:process.env.DBOS_DATABASE_URL});
await DBOS.launch();
serveRecovery(async input=>{
  const handle=await DBOS.startWorkflow(run,{workflowID:input.run})(input);
  return handle.workflowID;
},receipt=>DBOS.retrieveWorkflow(receipt).getResult());
