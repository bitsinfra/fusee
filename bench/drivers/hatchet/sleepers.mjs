import { Hatchet } from "@hatchet-dev/typescript-sdk";
import { entered,woke,serveSleep,throttled } from "../workloads/sleepers.mjs";
const app=Hatchet.init();
const wf=app.workflow({name:process.env.FUSEE_SLEEP_CAMPAIGN});
wf.durableTask({name:"sleep",fn:async(data,ctx)=>{
  const t=ctx.sleepUntil(new Date(data.deadline));entered(data);await t;return {value:woke(data)};
}});
const worker=await app.worker("fusee-sleep",{workflows:[wf],slots:128});
void worker.start();
let retryAfter=0;
await serveSleep(async data=>{
  let delay=100;
  for(;;){
    if(Date.now()<retryAfter)await new Promise(resolve=>setTimeout(resolve,retryAfter-Date.now()));
    try{return await (await wf.runNoWait(data)).runId;}
    catch(e){
      if(!String(e.message).includes("admin rate limit exceeded"))throw e;
      // The server explicitly rejected this submission. Share the backoff
      // across admissions and leave ambiguous network errors visible.
      throttled(data,delay);retryAfter=Math.max(retryAfter,Date.now()+delay);
      delay=Math.min(delay*2,2000);
    }
  }
},async receipt=>(await app.runs.runRef(receipt).result()).sleep.value);
