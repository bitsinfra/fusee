import http from "node:http";
import { Inngest } from "inngest";
import { serve } from "inngest/node";
import { sdkVersion } from "../common.mjs";
import { executeSteps } from "../workloads/scenario.mjs";
import { activity, serveAdapter, externalSignal, waiting, resumed } from "../workloads/adapter.mjs";
const buffered=process.env.CHECKPOINT==="1";
const app=new Inngest({id:"fusee-matrix",isDev:true});
const returned=new Map();
const workflow=app.createFunction({id:"matrix", checkpointing:buffered?{bufferedSteps:10,maxInterval:"1s"}:false},
  {event:"matrix/run"},async ({event,step})=>{
    let value;
    if(event.data.workload==="signal"){
      waiting(event.data);
      const message=await step.waitForEvent("external-signal",{event:"matrix/signal",match:"data.attempt",timeout:"60s"});
      value=resumed(event.data,message?.data.value);
    }else{
      value=await executeSteps(event.data,(input,i,v) => step.run("increment-"+i,()=>activity(input,i,v)));
    }
    returned.set(event.data.attempt,value);
    return value;
  });
const httpServer=http.createServer(serve({client:app,functions:[workflow]}));
await new Promise(resolve=>httpServer.listen(3939,"127.0.0.1",resolve));
const registration=await fetch("http://127.0.0.1:3939/api/inngest",{method:"PUT"});
if(!registration.ok)throw Error("Inngest registration: "+await registration.text());
const origin=process.env.INNGEST_API_BASE_URL ?? "http://127.0.0.1:8288";
await serveAdapter({name:buffered?"inngest-buffered":"inngest",version:sdkVersion(import.meta.url,"inngest"),
  durability:buffered?"asynchronous buffered checkpoints":"persistent dev server; standard orchestration"},async input=>{
  const {ids}=await app.send({name:"matrix/run",data:input});
  const until=Date.now()+300_000;
  const result=(async()=>{for (;;) {
    const response=await fetch(origin+"/v1/events/"+ids[0]+"/runs");
    if(!response.ok)throw Error("Inngest observation HTTP "+response.status);
    const run=(await response.json()).data?.[0];
    const status=run?.status?.toLowerCase();
    if(status==="completed" && returned.has(input.attempt)){
      const value=returned.get(input.attempt);
      returned.delete(input.attempt);
      return value;
    }
    if(["failed","cancelled"].includes(status)||Date.now()>until)throw Error("Inngest: "+JSON.stringify(run?.output));
    await new Promise(resolve=>setTimeout(resolve,10));
  }})();
  return input.workload==="signal"?externalSignal(input,result,value=>app.send({name:"matrix/signal",data:{attempt:input.attempt,value}})):result;
});
