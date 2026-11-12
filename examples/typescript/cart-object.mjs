// A cart object in TypeScript: one object per user, with its own state;
// calls to a key take turns, and a call sent again with the same ID runs once.
import { Engine, shared } from "fusee";

const engine = await Engine.open({ dir: "./data/cart-object" });
engine.object("cart", {
  add: async (ctx, item) => {
    const items = ctx.state.get("items") ?? [];
    items.push(item);
    ctx.state.set("items", items);
    return items.length;
  },
  total: shared(async (ctx) => (ctx.state.get("items") ?? []).reduce((n, i) => n + i.cents, 0)),
  checkout: async (ctx) => {
    const items = ctx.state.get("items") ?? [];
    const cents = items.reduce((n, i) => n + i.cents, 0);
    const receipt = await ctx.step("charge", () => `charged ${cents} cents for ${items.length} items`);
    ctx.state.clear("items");
    return receipt;
  },
});

const user = engine.objectRef("cart", "user-7");
await user.call("add", { sku: "tea", cents: 450 }, { id: "r1" });
console.log("items:", await user.call("add", { sku: "tea", cents: 450 }, { id: "r1" })); // sent again: once
console.log("items:", await user.call("add", { sku: "milk", cents: 120 }, { id: "r2" }));
console.log("total:", await user.call("total"));
console.log("receipt:", await user.call("checkout", undefined, { id: "c1" }));
await engine.close();
