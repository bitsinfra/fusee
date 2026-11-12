import { proxyActivities, proxyLocalActivities, sleep, condition, defineSignal, setHandler, proxySinks } from "@temporalio/workflow";
const { noop } = proxyActivities({ startToCloseTimeout: "10s" });
const local = proxyLocalActivities({ startToCloseTimeout: "10s" });
export async function chain(n, useLocal) {
  let value = 0;
  for (let i = 0; i < n; i++) value = await (useLocal ? local.noop(value) : noop(value));
  return value;
}

import { executeSteps } from "../workloads/scenario.mjs";
const regularMatrix = proxyActivities({startToCloseTimeout:"2m"});
const localMatrix = proxyLocalActivities({startToCloseTimeout:"2m"});
const signal = defineSignal("matrix-signal");
const {observer} = proxySinks();
export async function matrix(input, localActivities) {
  if (input.workload==="signal") {
    let value;
    setHandler(signal,received=>{value=received;});
    observer.waiting(input);
    await condition(()=>value!==undefined);
    observer.resumed(input,value);
    if(value!==input.challenge)throw Error("misdirected signal");
    return 1;
  }
  return executeSteps(input, (localActivities ? localMatrix : regularMatrix).increment);
}

const crashRegular=proxyActivities({startToCloseTimeout:"5s",retry:{initialInterval:"100ms",maximumAttempts:5}});
const crashLocal=proxyLocalActivities({startToCloseTimeout:"5s",retry:{initialInterval:"100ms",maximumAttempts:5}});
export async function crashChain(input,local) {
  const service=local?crashLocal:crashRegular;
  let value=input.value;
  for(let i=0;i<input.steps;i++)value=await service.crashEffect(input,i,value);
  return value;
}

const {population:populationSink}=proxySinks();
export async function population(data) {
  const timer=sleep(Math.max(1,data.deadline-Date.now()));
  populationSink.entered(data);
  await timer;
  populationSink.woke(data);
  return data.i;
}
