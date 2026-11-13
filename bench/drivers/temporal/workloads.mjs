import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { fileURLToPath } from "node:url";
import { sdkVersion } from "../common.mjs";
import { activity, serveAdapter, waiting, resumed, externalSignal } from "../workloads/adapter.mjs";
const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const local = process.env.TEMPORAL_LOCAL_ACTIVITIES === "1";
const worker = await Worker.create({connection:await NativeConnection.connect({address}),
  taskQueue:"matrix",workflowsPath:fileURLToPath(new URL("./workflows.mjs",import.meta.url)),activities:{increment:activity},sinks:{observer:{
    waiting:{fn:(_info,input)=>waiting(input)},resumed:{fn:(_info,input,value)=>resumed(input,value)}
  }}});
void worker.run();
const client = new Client({connection:await Connection.connect({address})});
await serveAdapter({name:local?"temporal-local":process.env.TEMPORAL_TUNED==="1"?"temporal-tuned":"temporal",
  version:sdkVersion(import.meta.url,"@temporalio/client"),durability:local?"local activities recorded at workflow task completion":"activity events recorded before resuming workflow"},
  async input => {
    const handle=await client.workflow.start("matrix",{taskQueue:"matrix",workflowId:input.attempt,args:[input,local]});
    const result=handle.result();
    return input.workload==="signal"?externalSignal(input,result,value=>handle.signal("matrix-signal",value)):result;
  });
