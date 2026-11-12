import { Absurd } from "absurd-sdk";
import { sdkVersion } from "../common.mjs";
import { executeSteps } from "../workloads/scenario.mjs";
import { activity, serveAdapter, waiting, resumed, externalSignal } from "../workloads/adapter.mjs";
const app = new Absurd({db:process.env.ABSURD_DATABASE_URL,queueName:"matrix"});
await app.createQueue();
app.registerTask({name:"matrix"}, async (input,ctx) => {
  if(input.workload==="signal"){
    waiting(input);
    return resumed(input,await ctx.awaitEvent(input.attempt,{timeout:90}));
  }
  return executeSteps(input,(data,index,value) => ctx.step("increment-"+index, () => activity(data,index,value)));
});
await app.startWorker({concurrency:32});
await serveAdapter({name:"absurd",version:sdkVersion(import.meta.url,"absurd-sdk"),durability:"PostgreSQL commit per checkpoint"},async input => {
  const {taskID}=await app.spawn("matrix",input);
  const result=app.awaitTaskResult(taskID,{timeout:300}).then(r=>{
    if(r.state!=="completed")throw Error("Absurd workflow "+r.state);
    return r.result;
  });
  return input.workload==="signal"?externalSignal(input,result,value=>app.emitEvent(input.attempt,value)):result;
});
