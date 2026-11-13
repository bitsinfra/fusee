// A payment saga in TypeScript (plain ESM so it runs without a build step).
// Each step that changes the world registers the step that undoes it; if the
// order fails or is cancelled, the compensations run, newest first.
import { Engine, FatalError, signal, workflow } from "@bitsinfra/fusee";

// A short ID made from a step's idempotency key (as a service would): FNV-1a.
const short = (key) => {
  let h = 0x811c9dc5;
  for (const b of Buffer.from(key)) h = Math.imul(h ^ b, 0x01000193) >>> 0;
  return h.toString(16).padStart(8, "0");
};
const reserve = async (sku, key) => `res-${sku}-${short(key)}`;
const charge = async (cents, key) => {
  if (cents > 1_000_000) throw new FatalError("card declined", { code: "card_declined" });
  return `ch-${short(key)}`;
};
const refund = async (id) => console.log(`  refunded ${id}`);
const release = async (id) => console.log(`  released ${id}`);

export const shipped = signal("shipped");

export const checkout = workflow("checkout", async (ctx, order) => {
  const reservation = await ctx.step("reserve", (s) => reserve(order.sku, s.key));
  ctx.compensate("release", () => release(reservation));
  const chargeId = await ctx.step("charge", (s) => charge(order.cents, s.key), {
    retry: { attempts: 5, initial: "200ms", max: "1m" },
  });
  ctx.compensate("refund", () => refund(chargeId));
  if (!(await ctx.waitFor(shipped, { timeout: "1d" }))) throw new FatalError("not shipped in a day");
  return chargeId;
});

const engine = await Engine.open({ dir: "./data/payment-saga", workflows: [checkout] });
const run = await engine.start(checkout, "order-123", { sku: "SKU-9", cents: 4200 }, { idPolicy: { finished: "use-existing" } });
await run.signal(shipped, true);
console.log("charged:", await run.result());
await engine.close();
