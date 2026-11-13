import http from "node:http";
import { Inngest } from "inngest";
import { serve } from "inngest/node";
import { entered,woke,serveSleep } from "../workloads/sleepers.mjs";
const campaign=process.env.FUSEE_SLEEP_CAMPAIGN;
const app=new Inngest({id:campaign,isDev:true}),eventName="sleep/"+campaign;
const returned=new Map();
const wf=app.createFunction({id:"population",checkpointing:process.env.CHECKPOINT==="1"?{bufferedSteps:10,maxInterval:"1s"}:false},
 {event:eventName},async({event,step})=>{
   const timer=step.sleepUntil("wake",new Date(event.data.deadline));entered(event.data);await timer;const result=woke(event.data);returned.set(event.data.i,result);return result;
 });
const server=http.createServer(serve({client:app,functions:[wf]}));
await new Promise(resolve=>server.listen(3939,"127.0.0.1",resolve));
const registration=await fetch("http://127.0.0.1:3939/api/inngest",{method:"PUT"});
if(!registration.ok)throw Error("Inngest registration HTTP "+registration.status);
const origin=process.env.INNGEST_API_BASE_URL??"http://127.0.0.1:8288";
await serveSleep(async data=>(await app.send({id:campaign+"-"+data.i,name:eventName,data})).ids[0],async (receipt,index)=>{
  const until=Date.now()+180_000;
  for(;;){
    const r=await fetch(origin+"/v1/events/"+receipt+"/runs");
    if(!r.ok)throw Error("Inngest status HTTP "+r.status);
    const run=(await r.json()).data?.[0];if(run?.status?.toLowerCase()==="completed" && returned.has(index)){
      const value=returned.get(index);returned.delete(index);
      if(typeof run.output==="number" && run.output!==value)throw Error("Inngest terminal output disagrees with handler");
      return value;
    }
    if(Date.now()>until)throw Error("missing Inngest result");
    await new Promise(resolve=>setTimeout(resolve,25));
  }
});
