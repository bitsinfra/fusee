import http from "node:http";
import { Inngest } from "inngest";
import { serve } from "inngest/node";
import { effect,runChain,serveRecovery } from "../workloads/crash-worker.mjs";

const campaign=process.env.FUSEE_CRASH_CAMPAIGN;
const eventName="recovery/"+campaign;
const app=new Inngest({id:campaign,isDev:true});
const workflow=app.createFunction({id:"crash",checkpointing:process.env.CHECKPOINT==="1"?
  {bufferedSteps:10,maxInterval:"1s"}:false},{event:eventName},async({event,step})=>{
  const value=await runChain(event.data,(data,i,v)=>step.run("effect-"+i,()=>effect(data,i,v)));
  const ack=await fetch(process.env.FUSEE_WITNESS+"/handler/"+event.data.run,{method:"POST",
    body:JSON.stringify({value,generation:Number(process.env.FUSEE_GENERATION)})});
  if(!ack.ok)throw Error("handler witness HTTP "+ack.status);
  return value;
});
const origin=process.env.INNGEST_API_BASE_URL??"http://127.0.0.1:8288";
if(process.env.FUSEE_ADMISSION_ONLY!=="1"){
const server=http.createServer(serve({client:app,functions:[workflow]}));
await new Promise(resolve=>server.listen(3939,"127.0.0.1",resolve));
const registration=await fetch("http://127.0.0.1:3939/api/inngest",{method:"PUT"});
if(!registration.ok)throw Error("Inngest registration HTTP "+registration.status);
  process.send({kind:"ready"});
}else serveRecovery(async data=>{
  const {ids}=await app.send({id:data.run,name:eventName,data});
  if(ids.length!==1)throw Error("missing event receipt");
  return ids[0];
},async (receipt,input)=>{
  const deadline=Date.now()+180_000;
  for(;;){
    const response=await fetch(origin+"/v1/events/"+receipt+"/runs");
    if(!response.ok)throw Error("Inngest query HTTP "+response.status);
    const run=(await response.json()).data?.[0],status=run?.status?.toLowerCase();
    if(status==="completed"){
      // Buffered dev-server checkpoints can expose an empty terminal output.
      // Require the independently observed handler return for this exact run.
      const witness=await fetch(process.env.FUSEE_WITNESS+"/handler/"+input.run);
      if(!witness.ok)throw Error("handler witness query HTTP "+witness.status);
      const {value}=await witness.json();
      if(value!=null){
        if(typeof run.output==="number" && run.output!==value)throw Error("terminal output disagrees with handler");
        return value;
      }
    }
    // The dev API can label an HTTP attempt Failed while its retry is queued.
    if((["failed","cancelled"].includes(status) && (run.ended_at||run.output)) || Date.now()>deadline)
      throw Error("Inngest run "+JSON.stringify({id:run?.run_id,status,ended_at:run?.ended_at,output:run?.output}).slice(0,1500));
    await new Promise(resolve=>setTimeout(resolve,20));
  }
});
