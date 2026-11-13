import { Client,Connection } from "@temporalio/client";
import { NativeConnection,Worker } from "@temporalio/worker";
import { fileURLToPath } from "node:url";
import { entered,woke,serveSleep } from "../workloads/sleepers.mjs";
const address=process.env.TEMPORAL_ADDRESS??"127.0.0.1:7233";
const taskQueue=process.env.FUSEE_SLEEP_CAMPAIGN;
const w=await Worker.create({connection:await NativeConnection.connect({address}),taskQueue,
  workflowsPath:fileURLToPath(new URL("./workflows.mjs",import.meta.url)),sinks:{population:{
    entered:{fn:(_,data)=>entered(data),callDuringReplay:false},woke:{fn:(_,data)=>woke(data),callDuringReplay:false}}}});
void w.run();
const client=new Client({connection:await Connection.connect({address})});
await serveSleep(async data=>(await client.workflow.start("population",{workflowId:data.campaign+"-"+data.i,taskQueue,args:[data]})).workflowId,
  receipt=>client.workflow.getHandle(receipt).result());
