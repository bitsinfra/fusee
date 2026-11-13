// The Obelisk server hosts the actual workflow and activity components. This
// separate process stays alive to submit requests and observe their receipts.
import { createHash } from "node:crypto";
import { serveRecovery } from "../workloads/crash-worker.mjs";
const api=process.env.OBELISK_API_URL??"http://127.0.0.1:5005";
function executionId(run){
  let bits=BigInt("0x"+createHash("sha256").update(run).digest("hex").slice(0,32));
  let code="";
  for(let i=0;i<26;i++){code="0123456789ABCDEFGHJKMNPQRSTVWXYZ"[Number(bits&31n)]+code;bits>>=5n;}
  return "E_"+code;
}
async function request(url,opts){
  const until=Date.now()+180_000;
  for(;;){
    try{return await fetch(api+url,opts);}catch(e){if(Date.now()>=until)throw e;await new Promise(r=>setTimeout(r,50));}
  }
}
serveRecovery(async data=>{
  const id=executionId(data.run);
  const res=await request("/v1/executions/"+id,{method:"PUT",headers:{"content-type":"application/json","accept":"application/json"},
    body:JSON.stringify({ffqn:"fusee:bench/workflow.recovery",params:[data.run,data.value]})});
  if(!res.ok&&res.status!==409)throw Error("Obelisk submission HTTP "+res.status);
  return id;
},async receipt=>{
  const end=Date.now()+180_000;
  for(;;){
    const res=await request("/v1/executions/"+receipt+"/status",{headers:{accept:"application/json"}});
    if(res.ok && (await res.json()).pending_state?.status==="finished"){
      const terminal=await request("/v1/executions/"+receipt,{headers:{accept:"application/json"}});
      const value=await terminal.json();if(typeof value.ok!=="number")throw Error("Obelisk terminal result "+JSON.stringify(value));
      return value.ok;
    }
    if(Date.now()>end)throw Error("Obelisk workflow did not finish");
    await new Promise(r=>setTimeout(r,25));
  }
});
