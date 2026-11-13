import * as restate from "@restatedev/restate-sdk";
import { entered,woke,serveSleep } from "../workloads/sleepers.mjs";
const svc=restate.workflow({name:"SleepPopulation",handlers:{run:async(ctx,data)=>{
  const delay=data.deadline-await ctx.date.now();const timer=ctx.sleep(Math.max(0,delay),"wake");
  entered(data);await timer;return woke(data);
}}});
await restate.serve({services:[svc],port:9080});
const registration=await fetch((process.env.RESTATE_ADMIN??"http://127.0.0.1:9070")+"/deployments",{method:"POST",
  headers:{"content-type":"application/json"},body:JSON.stringify({uri:process.env.RESTATE_WORKER_URI??"http://host.docker.internal:9080",force:true})});
if(!registration.ok)throw Error("Restate registration HTTP "+registration.status);
const origin=process.env.RESTATE_INGRESS??"http://127.0.0.1:8080";
await serveSleep(async data=>{const res=await fetch(origin+"/SleepPopulation/"+data.campaign+"-"+data.i+"/run/send",{method:"POST",
  headers:{"content-type":"application/json"},body:JSON.stringify(data)});if(!res.ok)throw Error("Restate start HTTP "+res.status);return (await res.json()).invocationId;},
  async receipt=>{const res=await fetch(origin+"/restate/invocation/"+receipt+"/attach");if(!res.ok)throw Error("Restate result HTTP "+res.status);return res.json();});
