// Edits of a key's state from outside its handlers, in the TypeScript SDK.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, shared } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-edit-"));

function cart(engine) {
  return engine.object("cart", {
    add: async (ctx, item) => {
      const items = ctx.state.get("items") ?? [];
      items.push(item);
      ctx.state.set("items", items);
      return items.length;
    },
    items: shared((ctx) => ctx.state.get("items") ?? []),
  });
}

test("edits set, clear and patch state, check versions and survive a restart", async () => {
  const dir = tmp();
  {
    const engine = await Engine.open({ dir });
    try {
      const c = cart(engine).key("u-1");
      assert.equal(await c.call("add", "apple"), 1);
      assert.deepEqual((await c.setState("owner", "ana")).changed, ["owner"]);
      await c.patchState([
        { op: "add", path: "/items/-", value: "pear" },
        { op: "test", path: "/owner", value: "ana" },
      ]);
      assert.deepEqual(await c.state(), { items: ["apple", "pear"], owner: "ana" });
      assert.equal(await c.call("add", "fig"), 3);
      await assert.rejects(c.patchState([{ op: "remove", path: "/nope" }]), /JSON Patch operation 0/);
      const version = engine.run(c.id).inspect().object.version;
      const ok = await c.clearState("owner", { ifVersion: version });
      assert.notEqual(ok.version, version);
      await assert.rejects(c.clearState(undefined, { ifVersion: version }), /changed since version/);
      assert.deepEqual(await c.call("items"), ["apple", "pear", "fig"]);
    } finally {
      await engine.close();
    }
  }
  const engine = await Engine.open({ dir });
  try {
    const c = cart(engine).key("u-1");
    assert.deepEqual(await c.state(), { items: ["apple", "pear", "fig"] });
    await c.clearState();
    assert.deepEqual(await c.state(), {});
    assert.equal(await c.call("add", "kiwi"), 1);
  } finally {
    await engine.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
