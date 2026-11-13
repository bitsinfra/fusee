// A worker that exists only while there is work: a Fusee server invokes this
// HTTP endpoint when a shard nobody holds has work due, and each request opens
// a fresh engine, runs the shard until it is idle, and closes it.
//
//   fusee server --dir ./data/invoked --listen 127.0.0.1:7600 \
//     --invoke http://127.0.0.1:8080/fusee --invoke-secret-file ./secret
//   FUSEE_INVOKE_SECRET="$(cat ./secret)" node examples/typescript/invoked-worker.mjs
//
// The same handler deploys as an AWS Lambda (`export const handler =
// worker.lambda`) or on the Fetch API (`export default { fetch: worker.fetch }`).
import { createServer } from "node:http";
import { Client, invocationHandler, workflow } from "@bitsinfra/fusee";

const address = process.env.FUSEE_ADDRESS ?? "127.0.0.1:7600";

// A reminder: sleeps for a day with no process up, then sends.
export const reminder = workflow("reminder", async (ctx, { to, text, after }) => {
  await ctx.step("schedule", () => console.log(`reminding ${to} in ${after}`));
  await ctx.sleep("wait", after);
  return ctx.step("send", async () => `sent "${text}" to ${to}`);
});

const worker = invocationHandler({
  address,
  secret: process.env.FUSEE_INVOKE_SECRET,
  register: (engine) => engine.register(reminder),
});

createServer(worker.node).listen(8080, "127.0.0.1", async () => {
  console.log("invoked worker listening on http://127.0.0.1:8080/fusee");
  const client = await Client.connect(address);
  const run = await client.start(reminder, "remind-ana-1", { to: "ana", text: "stand-up", after: "2s" });
  console.log(await run.result());
  client.close();
});
