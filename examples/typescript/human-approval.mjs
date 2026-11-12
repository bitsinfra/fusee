// Human approval in TypeScript: a workflow that asks a person, waits up to
// three days for the answer (durably: a crash meanwhile loses nothing), and
// acts on it.
import { Engine, signal, workflow } from "fusee";

export const decision = signal("decision");

export const expense = workflow("expense", async (ctx, claim) => {
  await ctx.step("ask", () => console.log(`  asked finance to approve ${claim.cents} cents for ${claim.by}`));
  const answer = await ctx.waitFor(decision, { timeout: "3d" });
  if (answer === undefined) return "expired: nobody answered in three days";
  if (!answer.approved) return `rejected by ${answer.by}`;
  const paid = await ctx.step("pay", () => `paid ${claim.cents} cents`);
  return `${paid} (approved by ${answer.by})`;
});

const engine = await Engine.open({ dir: "./data/human-approval", workflows: [expense] });
const run = await engine.start(expense, "expense-7", { by: "ana", cents: 12000 }, {
  idPolicy: { running: "use-existing", finished: "use-existing" },
});
await run.signal(decision, { approved: true, by: "dana" });
console.log("result:", await run.result());
await engine.close();
