import { Absurd } from "absurd-sdk";
import { entered,woke,serveSleep } from "../workloads/sleepers.mjs";
const app=new Absurd({db:process.env.ABSURD_DATABASE_URL,queueName:process.env.FUSEE_SLEEP_CAMPAIGN});
await app.createQueue();
app.registerTask({name:"sleep-population"},async(data,ctx)=>{
  entered(data);await ctx.sleepUntil("wake",new Date(data.deadline));return woke(data);
});
await app.startWorker({concurrency:128});
await serveSleep(async data=>(await app.spawn("sleep-population",data,{idempotencyKey:data.campaign+"-"+data.i})).taskID,
  async receipt=>{const result=await app.awaitTaskResult(receipt,{timeout:180});if(result.state!=="completed")throw Error(result.state);return result.result;});
