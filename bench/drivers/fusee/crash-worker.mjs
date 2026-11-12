import { Engine } from "../../../bindings/node/dist/index.js";
import { effect,runChain,serveRecovery } from "../workloads/crash-worker.mjs";
const mode=process.env.FUSEE_MODE ?? "local";
const e=await Engine.open({durability:mode,dir:process.env.FUSEE_WORKLOAD_DIR,shards:8,
  ...(mode==="server"?{address:process.env.FUSEE_SERVER_ADDRESS??"127.0.0.1:7600"}:{})});
const wf=e.workflow("crash-workflow",(ctx,input)=>runChain(input,(data,i,v)=>ctx.step("effect-"+i,()=>effect(data,i,v))));
serveRecovery(async input=>{
  await wf.start(input.run,input,{idPolicy:{running:"use-existing",finished:"use-existing"}});
  return input.run;
},receipt=>e.run(receipt).result());
