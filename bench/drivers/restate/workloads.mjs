import * as restate from "@restatedev/restate-sdk";
import { sdkVersion } from "../common.mjs";
import { executeSteps } from "../workloads/scenario.mjs";
import { activity, serveAdapter, waiting, resumed, externalSignal } from "../workloads/adapter.mjs";
const name = "matrix";
const svc = restate.service({name,handlers:{
  run: (ctx,input) => executeSteps(input,
    (data,index,value) => ctx.run("increment-"+index, () => activity(data,index,value)),
    calls => restate.RestatePromise.all(calls))
}});
const signal=restate.workflow({name:"matrixSignal",handlers:{
  run:async(ctx,input)=>{
    waiting(input);
    const value=await ctx.promise("signal").get();
    return resumed(input,value);
  },
  wake:async(ctx,value)=>{ await ctx.promise("signal").resolve(value); }
}});
await restate.serve({services:[svc,signal],port:9080});
const registered = await fetch((process.env.RESTATE_ADMIN ?? "http://127.0.0.1:9070")+"/deployments",
  {method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({uri:process.env.RESTATE_WORKER_URI ?? "http://host.docker.internal:9080",force:true})});
if (!registered.ok) throw Error("Restate deployment: "+await registered.text());
await serveAdapter({name:"restate",version:sdkVersion(import.meta.url,"@restatedev/restate-sdk"),durability:"Restate journal; deployment durability recorded separately"},async input => {
  if(input.workload==="signal"){
    const url=(process.env.RESTATE_INGRESS ?? "http://127.0.0.1:8080")+"/matrixSignal/"+input.attempt;
    const post=async (path,data)=>{
      const response=await fetch(url+path,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(data)});
      if(!response.ok)throw Error("Restate signal HTTP "+response.status);
      return response;
    };
    return externalSignal(input,post("/run",input).then(r=>r.json()),value=>post("/wake",value));
  }
  const res=await fetch((process.env.RESTATE_INGRESS ?? "http://127.0.0.1:8080")+"/matrix/run",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(input)});
  if (!res.ok) throw Error("Restate workflow: "+await res.text());
  return res.json();
});
