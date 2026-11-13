import { randomUUID } from "node:crypto";
import { Hatchet } from "@hatchet-dev/typescript-sdk";
import { sdkVersion } from "../common.mjs";
import { activity, serveAdapter, externalSignal, waiting, resumed } from "../workloads/adapter.mjs";
const app=Hatchet.init();
const results=new Map();
const suffix=randomUUID().slice(0,12);
const steps=Number(process.env.FUSEE_EXTERNAL_STEPS ?? 10);
function workflow(parallel){
  const n=parallel?100:steps;
  const wf=app.workflow({name:`matrix-${parallel?"fanout":"chain"}-${n}-${suffix}`});
  const tasks=[];
  for(let i=0;i<n;i++){
    const parent=parallel?null:tasks.at(-1);
    tasks.push(wf.task({name:"increment-"+i,parents:parent?[parent]:[],fn:async (input,ctx)=>{
      const previous=parallel?i:parent?(await ctx.parentOutput(parent)).value:0;
      const value=await activity(input,i,previous);
      if(!parallel && i===n-1)results.set(input.attempt,value);
      return {value};
    }}));
  }
  if(parallel)wf.task({name:"join",parents:tasks,fn:async(input,ctx)=>{
    const bodies=await Promise.all(tasks.map(parent=>ctx.parentOutput(parent)));
    const value=bodies.reduce((sum,body)=>sum+body.value,0);
    results.set(input.attempt,value);
    return {value};
  }});
  return wf;
}
const serial=workflow(false), fanout=workflow(true);
const signal=app.workflow({name:"matrix-signal-"+suffix});
signal.durableTask({name:"receive",fn:async(input,ctx)=>{
  const key="sig-"+input.attempt;
  const receive=ctx.waitForEvent(key,undefined,undefined,undefined,undefined,"matrix-signal");
  // The worker can reach this line before the server registers its wait.
  // Confirm the exact durable wait through the public API before timing the send.
  const deadline=Date.now()+45_000;
  for (;;) {
    let entries=[];
    try {entries=(await app.api.v1DurableTaskEventLogList(app.tenantId,ctx.taskRunId())).data;}
    catch(e){if(e.response?.status!==404)throw Error("Hatchet durable log HTTP "+(e.response?.status??"transport failure"));}
    if(entries.some(e=>!e.isSatisfied && e.userMessage==="matrix-signal" &&
         JSON.stringify(e.waitData).includes(key)))break;
    if(Date.now()>deadline)throw Error("Hatchet durable wait did not become visible");
    await new Promise(resolve=>setTimeout(resolve,20));
  }
  waiting(input);
  const event=await receive;
  // The v1 durable listener may return the event wrapper or its data.
  const payload=event.data ?? event;
  if(payload.value!==input.challenge) throw Error("Hatchet signal payload missing expected value (fields "+Object.keys(event).join(",")+")");
  const value=resumed(input,payload.value);
  results.set(input.attempt,value);
  return {value};
}});
const worker=await app.worker("fusee-matrix",{workflows:[serial,fanout,signal],slots:100});
void worker.start();
await new Promise(resolve=>setTimeout(resolve,1500));
await serveAdapter({name:"hatchet",version:sdkVersion(import.meta.url,"@hatchet-dev/typescript-sdk"),
  durability:"PostgreSQL task DAG; fan-in is a dependent join task; terminal status polled at 20 ms"},async input=>{
  const parallel=input.workload==="fanout";
  const isSignal=input.workload==="signal";
  if(!parallel && !isSignal && input.steps!==steps)throw Error("Hatchet chain shape differs from requested steps");
  const ref=await (isSignal?signal:parallel?fanout:serial).runNoWait(input);
  const id=await ref.runId,until=Date.now()+300_000;
  const result=(async()=>{for(;;){
    let status;
    try { status=(await app.api.v1WorkflowRunGetStatus(id)).data; }
    catch(e){ if(e.response?.status!==404)throw Error("Hatchet status HTTP "+(e.response?.status??"transport failure")); }
    if(status==="COMPLETED" && results.has(input.attempt)){
      const value=results.get(input.attempt);results.delete(input.attempt);return value;
    }
    if(["FAILED","CANCELLED"].includes(status)||Date.now()>until) throw Error("Hatchet workflow "+status);
    await new Promise(resolve=>setTimeout(resolve,20));
  }})();
  return isSignal?externalSignal(input,result,value=>app.events.push("sig-"+input.attempt,{value})):result;
});
