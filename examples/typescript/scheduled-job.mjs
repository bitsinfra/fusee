// A scheduled job: a schedule starts the report workflow on a cron
// expression; each scheduled time runs once, even across restarts.
import { Engine, workflow } from "@bitsinfra/fusee";

let sent = 0;
export const report = workflow("report", async (ctx) => {
  await ctx.step("send-report", () => console.log(`  ${ctx.workflowId} sent`));
  sent += 1;
});

const engine = await Engine.open({ dir: "./data/scheduled-job", workflows: [report] });
// A real job: { cron: "0 2 * * *", timezone: "Europe/London" }.
const schedule = await engine.schedule("nightly-report", { every: "1s", overlap: "skip" }, "report");
while (sent < 3) await new Promise((r) => setTimeout(r, 50));
console.log("next:", (await schedule.describe()).nextMs);
await schedule.delete();
await engine.close();
