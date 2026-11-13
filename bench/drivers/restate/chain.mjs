// Restate: ctx.run per step (journaled before the handler continues).
import * as restate from "@restatedev/restate-sdk";
import { finishRun, report, sdkVersion, stamp } from "../common.mjs";
const steps = Number(process.argv[2] ?? 100);
const runs = Number(process.argv[3] ?? 10);
const svc = restate.service({
  name: "bench",
  handlers: {
    chain: async (ctx, n) => {
      let value = 0;
      for (let i = 0; i < n; i++) value = await ctx.run("noop", () => stamp(() => value + 1));
      return value;
    },
  },
});
await restate.serve({ services: [svc], port: 9080 });
const registration = await fetch(`${process.env.RESTATE_ADMIN ?? "http://127.0.0.1:9070"}/deployments`, {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ uri: process.env.RESTATE_WORKER_URI ?? "http://host.docker.internal:9080", force: true }),
});
if (!registration.ok) throw new Error(`Restate registration failed: ${await registration.text()}`);
const whole = [];
for (let r = -1; r < runs; r++) {
  const t = process.hrtime.bigint();
  const res = await fetch(`${process.env.RESTATE_INGRESS ?? "http://127.0.0.1:8080"}/bench/chain`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(steps) });
  if (!res.ok || await res.json() !== steps) throw new Error(`unexpected Restate result (HTTP ${res.status})`);
  finishRun(steps, t, whole, r < 0);
}
report("restate", sdkVersion(import.meta.url, "@restatedev/restate-sdk"), "journaled step; see the recorded cluster configuration for replication and flush policy", steps, whole);
process.exit(0);
