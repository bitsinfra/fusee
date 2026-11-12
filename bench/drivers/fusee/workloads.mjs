import { Engine } from "../../../bindings/node/dist/index.js";
import { executeSteps } from "../workloads/scenario.mjs";
import { activity, serveAdapter, waiting, resumed, externalSignal } from "../workloads/adapter.mjs";
import { readFileSync } from "node:fs";
const mode=process.env.FUSEE_MODE ?? "local";
const engine=await Engine.open({durability:mode,shards:8,dir:process.env.FUSEE_WORKLOAD_DIR ?? "/mnt/fusee/matrix",
  ...(mode==="server"?{address:process.env.FUSEE_SERVER_ADDRESS??"127.0.0.1:7600"}:{})});
const wf=engine.workflow("matrix",async (ctx,input)=>{
  if(input.workload==="signal"){
    const receive=ctx.waitFor("matrix-signal");
    waiting(input);
    return resumed(input,await receive);
  }
  return executeSteps(input,(data,index,value) => ctx.step("increment-"+index,()=>activity(data,index,value)));
});
await serveAdapter({name:"fusee-node-"+mode,version:JSON.parse(readFileSync(new URL("../../../bindings/node/package.json",import.meta.url))).version,
  durability:mode==="memory"?"volatile memory":mode==="server"?"remote server flush before external effects":"local flush before external effects"},
  async input => {
    const handle=await wf.start(input.attempt,input);
    const result=handle.result();
    return input.workload==="signal"?externalSignal(input,result,value=>handle.signal("matrix-signal",value)):result;
  });
