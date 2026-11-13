// Every entry point of the package, imported by its published name (which
// resolves to this package's dist through its "exports").
import { Engine, signal, workflow } from "@bitsinfra/fusee";
import { TestEngine } from "@bitsinfra/fusee/testing";
import { Client } from "@bitsinfra/fusee/client";
import * as rich from "@bitsinfra/fusee/rich";
import * as ai from "@bitsinfra/fusee/ai";
import * as aiVercel from "@bitsinfra/fusee/ai/vercel";

const approve = signal<boolean>("approve");
export const greet = workflow("greet", async (ctx, name: string) => {
  const greeting = await ctx.step("compose", () => `hello ${name}`);
  await ctx.sleep("pause", "5s");
  const ok = await ctx.waitFor(approve, { timeout: "1h" });
  return ok ? greeting : `${greeting} (not approved)`;
});

export async function check(): Promise<string> {
  const t = await TestEngine.open({ workflows: [greet] });
  const run = await t.start(greet, "g-1", "test");
  await run.signal(approve, true);
  const out: string = await t.result<string>("g-1");
  await t.close();
  return out;
}

export { Client, Engine, ai, aiVercel, rich };
