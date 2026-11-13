import { Engine } from "../../../bindings/node/dist/index.js";
import { entered,woke,serveSleep } from "../workloads/sleepers.mjs";
const mode=process.env.FUSEE_MODE??"local";
const e=await Engine.open({durability:mode,dir:process.env.FUSEE_WORKLOAD_DIR,shards:8,
  ...(mode==="server"?{address:process.env.FUSEE_SERVER_ADDRESS??"127.0.0.1:7600"}:{})});
const wf=e.workflow("sleep-population",async(ctx,data)=>{
  const timer=ctx.sleep("wake",Math.max(0,data.deadline-Date.now()));
  entered(data);await timer;return woke(data);
});
await serveSleep(async data=>{const h=await wf.start(data.campaign+"-"+data.i,data);return h.id;},
  receipt=>e.run(receipt).result());
