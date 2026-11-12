import http from "node:http";
import { fork,spawn } from "node:child_process";
import { once } from "node:events";
import { randomUUID } from "node:crypto";
import { copyFile,mkdir,open,readFile,writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { setTimeout as wait } from "node:timers/promises";

const [system,destination,nArg="200",rateArg="100"]=process.argv.slice(2);
const count=Number(nArg), rate=Number(rateArg), steps=3;
if(!["dbos","dbos-sqlite","absurd","fusee","temporal","restate","hatchet","inngest","obelisk"].includes(system)||!Number.isInteger(count)||count<4||!Number.isFinite(rate)||rate<=0)throw Error("invalid crash workload arguments");
const folder=resolve(destination);
await mkdir(folder); // Refuse to overwrite an earlier witness.
const ledger=await open(folder+"/external.jsonl","wx");
const handlerReturns=new Map();
const returnLog=system==="inngest"?await open(folder+"/handler-returns.jsonl","wx"):null;
const campaign="crash-"+randomUUID();
const records=new Map(), inputByKey=new Map(), attempts=new Map(), recovered=new Map();
let worker,ingress,gen=0,killedAt=0,fatal,serviceCalls=0;
const pendingAtCut=new Set();
const stats={witness_version:2,system,campaign,offered_starts_per_s:rate,workflows:count,steps_per_workflow:steps,
  killed_process:system==="obelisk"?"Obelisk server (WASM workflow and activity host)":"application worker",effect_service_ms:35,scheduled:[],worker_events:[]};
const clock=()=>performance.now();
const noteRecovery=(run,at)=>{
  if(killedAt && pendingAtCut.has(run)){
    const elapsed=at-killedAt;recovered.set(run,Math.min(elapsed,recovered.get(run)??Infinity));
  }
};
const httpServer=http.createServer(async(req,res)=>{
  try{
    const chunks=[];for await(const chunk of req)chunks.push(chunk);
    if(system==="inngest" && req.url.startsWith("/handler/")){
      const run=req.url.slice("/handler/".length), record=records.get(run);
      if(!record)throw Error("unknown handler return "+run);
      if(req.method==="POST"){
        const {value,generation}=JSON.parse(Buffer.concat(chunks).toString("utf8"));
        if(value!==record.input.value+steps||![0,1].includes(generation))throw Error("contradictory handler return");
        await returnLog.appendFile(JSON.stringify({run,value,generation,at_ms:clock()})+"\n");
        await returnLog.datasync();handlerReturns.set(run,value);
      }
      res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({value:handlerReturns.get(run)??null}));return;
    }
    let data;
    if(system==="obelisk"){
      const match=/^\/effect\/([A-Za-z0-9-]+)\/(\d+)\/(\d+)\/([01])$/.exec(req.url);
      if(req.method!=="GET"||!match)throw Error("invalid WASM effect request");
      data={run:match[1],seq:Number(match[2]),value:Number(match[3]),generation:Number(match[4])};
    }else data=JSON.parse(Buffer.concat(chunks).toString("utf8"));
    const {run,seq,value,generation}=data;
    const record=records.get(run);
    if(!record||!Number.isInteger(seq)||seq<0||seq>=steps||![0,1].includes(generation)||value!==record.input.value+seq)throw Error("contradictory effect input");
    const key=run+"/"+seq;
    if(inputByKey.has(key)&&inputByKey.get(key)!==value)throw Error("idempotency key changed input");
    inputByKey.set(key,value);attempts.set(key,1+(attempts.get(key)??0));serviceCalls++;
    const observedAt=clock();
    await ledger.appendFile(JSON.stringify({at_ms:observedAt,generation,run,seq,value})+"\n");
    await ledger.datasync();
    if(generation===1)noteRecovery(run,observedAt);
    await wait(stats.effect_service_ms);
    res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({value:value+1}));
  }catch(e){fatal=e;res.writeHead(500).end();}
});
await new Promise(resolve=>httpServer.listen(system==="obelisk"?29780:0,"127.0.0.1",resolve));
const witness="http://127.0.0.1:"+httpServer.address().port;
async function startWorker(admission=false){
  const generation=gen;
  let ready=false;
  const opts={
    env:{...process.env,FUSEE_WITNESS:witness,FUSEE_GENERATION:String(gen),FUSEE_ADMISSION_ONLY:admission?"1":"0",FUSEE_CRASH_CAMPAIGN:campaign,
      FUSEE_WORKLOAD_DIR:process.env.FUSEE_WORKLOAD_DIR??folder+"/engine"},stdio:["ignore","inherit","inherit","ipc"]};
  if(system==="obelisk"&&!admission){
    const base=fileURLToPath(new URL("../obelisk/",import.meta.url));
    // The no-fault chains disable activity retries. For a killed server, let
    // expiring activity locks retry under the same external idempotency key.
    const deploy=opts.env.FUSEE_WORKLOAD_DIR+"/obelisk-recovery.toml";
    await mkdir(opts.env.FUSEE_WORKLOAD_DIR+"/build",{recursive:true});
    for(const component of ["chain_workflow.wasm","noop_activity.wasm"])
      await copyFile(base+"build/"+component,opts.env.FUSEE_WORKLOAD_DIR+"/build/"+component);
    const template=await readFile(base+"obelisk.toml","utf8");
    if(!template.includes("max_retries = 0"))throw Error("unrecognized Obelisk configuration");
    await writeFile(deploy,template.replace("max_retries = 0","max_retries = 3"));
    await copyFile(deploy,folder+"/obelisk-recovery.toml");
    worker=spawn(process.env.OBELISK??"obelisk",["server","run","--no-auth","-s",process.env.OBELISK_SERVER_CONFIG??base+"server.toml",
      "-a",base+"app.toml","-d",deploy],{env:{...opts.env,XDG_DATA_HOME:opts.env.FUSEE_WORKLOAD_DIR,
      OBELISK_APP_NAME:"fusee-bench"},stdio:["ignore","inherit","inherit"]});
    const end=clock()+90_000;
    for(;;){
      if(worker.exitCode!==null||worker.signalCode||clock()>end)throw Error("Obelisk server failed to start");
      try{const r=await fetch((process.env.OBELISK_API_URL??"http://127.0.0.1:5005")+"/v1/functions");
        if(r.ok)return;}catch{}
      await wait(30);
    }
  }
  const child=system==="dbos-sqlite"?
    spawn(process.env.DBOS_PYTHON??"/opt/fusee-python/bin/python",[fileURLToPath(new URL("../dbos/sqlite-crash.py",import.meta.url))],opts):
    fork(new URL("../"+system+"/crash-worker.mjs",import.meta.url),[],opts);
  if(admission)ingress=child;else worker=child;
  child.on("message",m=>{
    if(m.kind==="ready"){ready=true;return;}
    const r=records.get(m.run);
    if(!r){fatal=Error("unknown run receipt");return;}
    if(m.kind==="ack"){
      if(!m.receipt||(r.receipt&&r.receipt!==m.receipt)){fatal=Error("unstable admission receipt");return;}
      r.receipt=m.receipt;r.ackAt??=clock();
    }else if(m.kind==="done"){
      if(m.value!==r.input.value+steps){fatal=Error("wrong workflow result for "+m.run+": expected "+(r.input.value+steps)+", received "+JSON.stringify(m.value));return;}
      r.result=m.value;r.doneAt??=clock();if(generation===1||admission)noteRecovery(m.run,r.doneAt);
    }else if(m.kind==="error")fatal=Error(m.run+": "+m.message);
  });
  const until=clock()+90_000;
  while(!ready){if(child.exitCode!==null||child.signalCode||clock()>until)throw Error("worker did not start");await wait(10);}
}
function submit(r){
  (ingress??worker).send({kind:r.receipt?"attach":"start",run:r.input.run,input:r.input,receipt:r.receipt});
}
let warm;
try{
  if(["hatchet","obelisk","inngest"].includes(system))await startWorker(true);
  await startWorker();
  warm={input:{run:campaign+"-warm",steps,value:50}};records.set(warm.input.run,warm);stats.warm=warm;
  submit(warm);
  const warmEnd=clock()+180_000;
  while(!warm.doneAt){if(fatal)throw fatal;if(clock()>warmEnd)throw Error("warmup timed out");await wait(5);}
  let next=0,available=true;
  let replacement;
  const start=clock();
  stats.start_ms=start;
  const deadline=start+Math.max(300_000,(count/rate)*1000+180_000);
  while(clock()<deadline){
    if(fatal)throw fatal;
    while(next<count && start+next*1000/rate<=clock()){
      const r={input:{run:campaign+"-"+next,value:next+100,steps},scheduledMs:next*1000/rate};
      records.set(r.input.run,r);stats.scheduled.push(r);next++;
      if(available||ingress){r.firstSentAt=clock();submit(r);}
    }
    if(gen===0 && next>=Math.max(2,Math.ceil(count*.20))){
      const victims=stats.scheduled.filter(r=>r.receipt&&!r.doneAt);
      const working=victims.some(r=>inputByKey.has(r.input.run+"/0"));
      if(victims.length && working){
        for(const r of victims)pendingAtCut.add(r.input.run);
        killedAt=clock();
        available=false;gen=1;
        const died=once(worker,"exit");worker.kill("SIGKILL");
        stats.worker_events.push({event:"SIGKILL",at_ms:killedAt-start,acknowledged_not_finished:pendingAtCut.size});
        replacement=(async()=>{
          const [code,signal]=await died;
          if(signal!=="SIGKILL"||code!==null)throw Error("worker was not killed as requested");
          await startWorker();
          stats.worker_events.push({event:"replacement_ready",at_ms:clock()-start});
          available=true;
          for(const r of stats.scheduled)if(!r.doneAt&&!ingress){r.firstSentAt??=clock();submit(r);}
        })().catch(e=>{fatal=e;});
      }
    }
    if(gen===1 && next===count && stats.scheduled.every(r=>r.receipt&&r.doneAt))break;
    await wait(2);
  }
  await replacement;
  if(!killedAt||stats.scheduled.some(r=>!r.receipt||!r.doneAt)||stats.scheduled.length!==count||recovered.size!==pendingAtCut.size)throw Error("missing acknowledged workflow after worker death");
  if(inputByKey.size!==(count+1)*steps)throw Error("missing external keys");
  stats.kill_to_all_resumed_or_finished_ms=Math.max(...recovered.values());
  stats.kill_to_pending_finished_ms=Math.max(...[...pendingAtCut].map(id=>records.get(id).doneAt-killedAt));
  stats.recovered=[...recovered].map(([run,ms])=>({run,ms}));
  stats.external_calls=serviceCalls;stats.distinct_external_keys=inputByKey.size;
  stats.repeated_calls=[...attempts.values()].reduce((sum,n)=>sum+n-1,0);
  await writeFile(folder+"/result.json",JSON.stringify(stats));
  console.log(JSON.stringify({system,workload:"kill",acknowledged:count,pending_at_kill:pendingAtCut.size,
    kill_to_all_resumed_or_finished_ms:stats.kill_to_all_resumed_or_finished_ms,
    external_calls:serviceCalls,distinct_keys:inputByKey.size}));
}catch(error){
  await writeFile(folder+"/failure.json",JSON.stringify({...stats,error:String(error.message??error)}));
  throw error;
}finally{
  if(worker && worker.exitCode===null && worker.signalCode===null){const end=once(worker,"exit");worker.kill("SIGKILL");await end;}
  if(ingress && ingress.exitCode===null && ingress.signalCode===null){const end=once(ingress,"exit");ingress.kill("SIGKILL");await end;}
  httpServer.close();await ledger.close();if(returnLog)await returnLog.close();
}
