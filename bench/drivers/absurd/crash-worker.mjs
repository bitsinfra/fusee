import { Absurd } from "absurd-sdk";
import { effect,runChain,serveRecovery } from "../workloads/crash-worker.mjs";
const app=new Absurd({db:process.env.ABSURD_DATABASE_URL,queueName:process.env.FUSEE_CRASH_CAMPAIGN});
await app.createQueue();
app.registerTask({name:"crash-workflow"},(input,ctx)=>runChain(input,(data,i,v)=>ctx.step("effect-"+i,()=>effect(data,i,v))));
await app.startWorker({concurrency:32,claimTimeout:5});
serveRecovery(async input=>(await app.spawn("crash-workflow",input,{idempotencyKey:input.run})).taskID,
 async receipt=>{
   const r=await app.awaitTaskResult(receipt,{timeout:180});
   if(r.state!=="completed")throw Error("Absurd run "+r.state);
   return r.result;
 });
