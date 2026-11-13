import { DBOS } from "@dbos-inc/dbos-sdk";
import { entered,woke,serveSleep } from "../workloads/sleepers.mjs";
const wf=DBOS.registerWorkflow(async data=>{
  const now=await DBOS.now();
  const timer=DBOS.sleep(Math.max(0,data.deadline-now));entered(data);await timer;return woke(data);
},{name:"sleep-population"});
DBOS.setConfig({name:"bench",systemDatabaseUrl:process.env.DBOS_DATABASE_URL});
await DBOS.launch();
await serveSleep(async data=>(await DBOS.startWorkflow(wf,{workflowID:data.campaign+"-"+data.i})(data)).workflowID,
  receipt=>DBOS.retrieveWorkflow(receipt).getResult());
