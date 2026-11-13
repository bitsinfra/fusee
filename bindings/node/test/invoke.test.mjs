// Invoked workers in the TypeScript SDK: a Fusee server started with
// --invoke POSTs to a node:http endpoint whose handler opens a fresh engine
// per invocation; workflows sleep, take signals and finish with no engine
// open between invocations. The Lambda and Fetch shapes refuse bad
// signatures before connecting.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer, createConnection } from "node:net";
import { createServer as createHttp } from "node:http";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, invocationHandler, verifySignature } from "../dist/index.js";
import { createHmac } from "node:crypto";

const here = dirname(fileURLToPath(import.meta.url));
const cli = process.env.FUSEE_CLI ?? join(here, "../../../target/release/fusee");
const SECRET = "an invocation secret of 32 bytes";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function freePort() {
  return new Promise((resolve) => {
    const s = createServer();
    s.listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}

async function reachable(port) {
  for (let i = 0; i < 200; i++) {
    const ok = await new Promise((r) => {
      const c = createConnection(port, "127.0.0.1", () => { c.end(); r(true); });
      c.on("error", () => r(false));
    });
    if (ok) return;
    await sleep(25);
  }
  throw new Error("the server did not start");
}

const sign = (body, t = Date.now()) =>
  `t=${t},v1=${createHmac("sha256", SECRET).update(`${t}.`).update(body).digest("hex")}`;

test("signatures", () => {
  const body = Buffer.from('{"version":1}');
  assert.equal(verifySignature(SECRET, sign(body), body), null);
  assert.match(verifySignature(SECRET, sign(body), Buffer.from("{}")), /does not match/);
  assert.match(verifySignature(SECRET, sign(body, Date.now() - 600_000), body), /too old/);
  assert.match(verifySignature("another secret of 32 bytes......", sign(body), body), /does not match/);
});

test("bad requests are refused before connecting", async () => {
  let opened = 0;
  const h = invocationHandler({ address: "127.0.0.1:1", secret: SECRET, register: () => { opened++; } });
  const body = JSON.stringify({ version: 1, invocation: "00000000000000aa", shards: [0], budgetMs: 1000 });
  assert.equal((await h.lambda({ headers: {}, body })).statusCode, 401);
  assert.equal((await h.lambda({ headers: { "Fusee-Signature": sign(Buffer.from(body)) }, body: body + " " })).statusCode, 401);
  const res = await h.fetch(new Request("http://x/", { method: "POST", body: "{}", headers: { "fusee-signature": sign(Buffer.from("{}")) } }));
  assert.equal(res.status, 400);
  assert.equal(opened, 0);
});

test("workflows sleep, take signals and finish with no engine between invocations", { skip: !existsSync(cli) && `no ${cli}` }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "fusee-node-invoke-"));
  writeFileSync(join(dir, "secret"), SECRET, { mode: 0o600 });
  const port = await freePort();
  const address = `127.0.0.1:${port}`;
  let opened = 0;
  const steps = new Map();
  const handler = invocationHandler({
    address,
    secret: SECRET,
    register: (engine) => {
      opened++;
      engine.workflow("sleeper", async (ctx, n) => {
        const a = await ctx.step("first", async () => { steps.set(`${ctx.workflowId}/first`, (steps.get(`${ctx.workflowId}/first`) ?? 0) + 1); return n + 1; });
        await ctx.sleep("nap", 1200);
        const go = await ctx.waitFor("go");
        return ctx.step("second", async () => { steps.set(`${ctx.workflowId}/second`, (steps.get(`${ctx.workflowId}/second`) ?? 0) + 1); return a * 10 + go; });
      });
    },
  });
  let active = 0;
  let most = 0;
  const http = createHttp((req, res) => {
    active++;
    most = Math.max(most, active);
    res.on("finish", () => active--);
    handler.node(req, res);
  });
  await new Promise((r) => http.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${http.address().port}/invoke`;
  const server = spawn(cli, ["server", "--dir", join(dir, "server"), "--listen", address, "--shards", "2",
    "--invoke", url, "--invoke-secret-file", join(dir, "secret")], { stdio: ["ignore", "ignore", "inherit"] });
  try {
    await reachable(port);
    const client = await Client.connect(address);
    const ids = ["s-0", "s-1", "s-2"];
    for (const [k, id] of ids.entries()) await client.start("sleeper", id, k);
    // The runs sleep with nobody up, then wait for their signals.
    await sleep(2500);
    const quiet = await client.call("invocations", {});
    assert.equal(quiet.endpoints[0].inFlight, 0, JSON.stringify(quiet));
    for (const [k, id] of ids.entries()) {
      await client.run(id).signal("go", k + 5);
      assert.equal(await client.run(id).result(), (k + 1) * 10 + k + 5);
    }
    for (const id of ids) {
      assert.equal(steps.get(`${id}/first`), 1);
      assert.equal(steps.get(`${id}/second`), 1);
    }
    const m = await client.call("invocations", {});
    const e = m.endpoints[0];
    assert.equal(e.failed + e.timedOut + e.unleased, 0, JSON.stringify(m));
    assert.ok(opened >= 4, `${opened} engines`);
    console.log(`# ${opened} engines for ${e.invocations} invocations, ${e.steps} steps, mean open ${(e.openUsTotal / Math.max(e.opens, 1) / 1e3).toFixed(1)} ms`);
    client.close();
  } finally {
    server.kill("SIGKILL");
    http.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
