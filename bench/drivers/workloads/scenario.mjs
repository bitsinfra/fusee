// This module also runs in Temporal's deterministic workflow isolate.
export async function executeSteps(input, step, join = (calls) => Promise.all(calls)) {
  if (input.workload === "fanout") {
    const calls = Array.from({length: 100}, (_, i) => step(input, i, i));
    const values = await join(calls);
    return values.reduce((sum, value) => sum + value, 0);
  }
  let value = 0;
  for (let i = 0; i < input.steps; i++) value = await step(input, i, value);
  return value;
}
