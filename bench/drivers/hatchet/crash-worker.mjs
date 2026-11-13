import { Hatchet } from "@hatchet-dev/typescript-sdk";
import { effect,serveRecovery } from "../workloads/crash-worker.mjs";
const app=Hatchet.init();
const wf=app.workflow({name:process.env.FUSEE_CRASH_CAMPAIGN});
let previous;
for(let i=0;i<3;i++){
  const parent=previous;
  previous=wf.task({name:"effect-"+i,parents:parent?[parent]:[],retries:3,fn:async(data,ctx)=>{
    const value=parent?(await ctx.parentOutput(parent)).value:data.value;
    return {value:await effect(data,i,value)};
  }});
}
if(process.env.FUSEE_ADMISSION_ONLY==="1"){
  serveRecovery(async data=>await (await wf.runNoWait(data)).runId,async receipt=>{
    const output=await app.runs.runRef(receipt).result();
    return output["effect-2"].value;
  });
}else{
  const worker=await app.worker("fusee-recovery-"+process.env.FUSEE_GENERATION,{workflows:[wf],slots:32});
  void worker.start();
  process.send({kind:"ready"});
}
