// Matching closed-loop Fusee SDK chain on the mounted measurement disk.
import { pathToFileURL } from "node:url";
const { Engine } = await import(process.env.FUSEE_BENCH_NODE_SDK
  ? pathToFileURL(process.env.FUSEE_BENCH_NODE_SDK).href
  : "../../../bindings/node/dist/index.js");
import { finishRun, report, stamp } from "../common.mjs";
const [dir, size, repetitions] = process.argv.slice(2);
const steps = Number(size), runs = Number(repetitions);
const engine = await Engine.open({dir, durability:"local", shards:Number(process.env.FUSEE_SDK_SHARDS ?? 8)});
try {
  const wf=engine.workflow("chain",async (ctx,n)=>{
    let v=0;
    for(let i=0;i<n;i++) v=await ctx.step("noop",()=>stamp(()=>v+1));
    return v;
  });
  const whole=[],observations=[];
  for(const cold of [true,false]) for(let r=0;r<runs;r++){
    const before=engine.shardMetrics();
    const start=process.hrtime.bigint();
    if(await (await wf.start(`chain-${r}`,steps)).result()!==steps) throw new Error("incorrect dependency result");
    observations.push({id:`chain-${r}`,first_execution:cold,...finishRun(steps,start,whole,cold),metrics_before:before,metrics_after:engine.shardMetrics()});
  }
  report("fusee-node",process.version,"local fdatasync before outputs and before dependent effects",steps,whole,{observations,shards:Number(process.env.FUSEE_SDK_SHARDS ?? 8)});
} finally {await engine.close();}
