import { appendFileSync, existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { Engine } from "../bindings/node/dist/index.js";

const [root, index, endpoints] = process.argv.slice(2);
const id = Number(index);
const engine = await Engine.open({ durability: "replicated", replicaId: id, peers: endpoints.split(","), dir: join(root, `node-${id}`) });
const workflow = engine.workflow("mixed", async (ctx, input) => {
  let stage = 0;
  ctx.onQuery("stage", () => stage);
  const effect = (value) => {
    appendFileSync(join(root, "effects"), `${ctx.key()} ${value}\n`);
    return value + 1;
  };
  const first = await ctx.step("first", () => effect(input));
  stage = first;
  await ctx.waitFor("go");
  return await ctx.step("second", () => effect(first));
});
if (!existsSync(join(root, "committed"))) {
  const run = await workflow.start("known", 41, { idPolicy: { running: "use-existing" } });
  while (await run.query("stage") !== 42) await new Promise((r) => setTimeout(r, 5));
  writeFileSync(join(root,"committed"), String(id));
} else {
  const run = engine.run("known");
  await run.signal("go", true);
  writeFileSync(join(root,"finished"), JSON.stringify({ id, result: await run.result() }));
}
