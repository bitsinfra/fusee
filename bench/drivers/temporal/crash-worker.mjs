import { Client,Connection,WorkflowExecutionAlreadyStartedError } from "@temporalio/client";
import { Worker,NativeConnection } from "@temporalio/worker";
import { fileURLToPath } from "node:url";
import { effect,serveRecovery } from "../workloads/crash-worker.mjs";
const address=process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const queue=process.env.FUSEE_CRASH_CAMPAIGN;
const worker=await Worker.create({connection:await NativeConnection.connect({address}),taskQueue:queue,
  workflowsPath:fileURLToPath(new URL("./workflows.mjs",import.meta.url)),activities:{crashEffect:effect}});
void worker.run();
const client=new Client({connection:await Connection.connect({address})});
serveRecovery(async input=>{
  try{
    await client.workflow.start("crashChain",{taskQueue:queue,workflowId:input.run,
      workflowIdConflictPolicy:"USE_EXISTING",workflowIdReusePolicy:"REJECT_DUPLICATE",
      args:[input,process.env.TEMPORAL_LOCAL_ACTIVITIES==="1"]});
  }catch(e){
    if(!(e instanceof WorkflowExecutionAlreadyStartedError))throw e;
  }
  return input.run;
},receipt=>client.workflow.getHandle(receipt).result());
