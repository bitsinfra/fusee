import * as restate from "@restatedev/restate-sdk";
import { effect,runChain,serveRecovery } from "../workloads/crash-worker.mjs";
const svc=restate.workflow({name:"recovery",handlers:{
  run:(ctx,input)=>runChain(input,(data,i,v)=>ctx.run("effect-"+i,()=>effect(data,i,v)))
}});
await restate.serve({services:[svc],port:9080});
const r=await fetch((process.env.RESTATE_ADMIN??"http://127.0.0.1:9070")+"/deployments",{method:"POST",
  headers:{"content-type":"application/json"},body:JSON.stringify({uri:process.env.RESTATE_WORKER_URI??"http://host.docker.internal:9080",force:true})});
if(!r.ok)throw Error("Restate registration HTTP "+r.status);
const origin=process.env.RESTATE_INGRESS??"http://127.0.0.1:8080";
serveRecovery(async input=>{
  const res=await fetch(origin+"/recovery/"+input.run+"/run/send",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(input)});
  if(!res.ok)throw Error("Restate submission HTTP "+res.status);
  const value=await res.json();if(!value.invocationId)throw Error("missing accepted invocation");
  return value.invocationId;
},async receipt=>{
  const res=await fetch(origin+"/restate/invocation/"+receipt+"/attach");
  if(!res.ok)throw Error("Restate attach HTTP "+res.status);
  return res.json();
});
