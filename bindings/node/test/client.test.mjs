// The one client interface: a Client of a server has the methods an embedded
// Engine has, on the same definitions.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer, createConnection } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Engine, FatalError, StepError, signal, workflow } from "../dist/index.js";

const here = dirname(fileURLToPath(import.meta.url));
const cli = process.env.FUSEE_CLI ?? join(here, "../../../target/release/fusee");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const freePort = () =>
  new Promise((resolve) => {
    const s = createServer();
    s.listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });

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

const add = signal("add");
const cart = workflow("cart", async (ctx, items) => {
  while (items.length < 2) items.push(await ctx.waitFor(add));
  return items;
});
let tries = 0;
const flaky = workflow("flaky", async (ctx) => ctx.step("call", () => {
  tries += 1;
  if (tries === 1) throw new FatalError("first time");
  return 7;
}));
const later = workflow("later", async (_ctx, who) => `hi ${who}`);

test("a client has the engine's interface", async () => {
  const dir = mkdtempSync(join(tmpdir(), "fusee-client-"));
  const port = await freePort();
  const address = `127.0.0.1:${port}`;
  const server = spawn(cli, ["server", "--dir", join(dir, "server"), "--listen", address], { stdio: "ignore" });
  try {
    await reachable(port);
    const worker = await Engine.open({ durability: "server", address, workflows: [cart, flaky, later] });
    const client = await Client.connect(address);
    try {
      const run = await client.signalWithStart(cart, "cart-1", [], add, "apple");
      await client.signalWithStart(cart, "cart-1", [], add, "pear");
      assert.deepEqual(await run.result(), ["apple", "pear"]);
      assert.equal((await run.status()).status, "ok");
      assert.equal((await run.describe()).journal[0].kind, "start");

      const f = await client.start(flaky, "f-1", null);
      await assert.rejects(f.result(), (e) => e instanceof StepError && e.cause.message === "first time");
      await f.retry();
      assert.equal(await f.result(), 7);

      const l = await client.start(later, "l-1", "ana", { delay: "100ms" });
      assert.equal(await l.result(), "hi ana");
    } finally {
      client.close();
      await worker.close();
    }
  } finally {
    server.kill("SIGTERM");
    await new Promise((r) => server.on("exit", r));
    rmSync(dir, { recursive: true, force: true });
  }
});

test("through a server, a client resumes a paused run and edits an object's state", async () => {
  const dir = mkdtempSync(join(tmpdir(), "fusee-client-"));
  const port = await freePort();
  const address = `127.0.0.1:${port}`;
  const server = spawn(cli, ["server", "--dir", join(dir, "server"), "--listen", address], { stdio: "ignore" });
  let down = true;
  const payout = workflow("payout", async (ctx) =>
    ctx.step("send", () => {
      if (down) throw new Error("bank is down");
      return "sent";
    }, { retry: { attempts: 2, initial: 1, onExhausted: "pause" } }),
  );
  try {
    await reachable(port);
    const worker = await Engine.open({ durability: "server", address, workflows: [payout] });
    worker.object("ledger", {
      add: async (ctx, n) => {
        const total = (ctx.state.get("total") ?? 0) + n;
        ctx.state.set("total", total);
        return total;
      },
    });
    const client = await Client.connect(address);
    try {
      const run = await client.start(payout, "p-1", null);
      for (let i = 0; i < 400 && (await run.status()).status !== "paused"; i++) await sleep(5);
      assert.equal((await run.status()).status, "paused");
      down = false;
      await run.resume();
      assert.equal(await run.result(), "sent");

      const ledger = client.objectRef("ledger", "acme");
      assert.equal(await ledger.call("add", 5), 5);
      assert.deepEqual((await ledger.setState("owner", "ana")).changed, ["owner"]);
      await ledger.patchState([{ op: "replace", path: "/total", value: 40 }]);
      assert.deepEqual(await ledger.state(), { owner: "ana", total: 40 });
      assert.equal(await ledger.call("add", 2), 42);
      await assert.rejects(ledger.clearState(undefined, { ifVersion: "0000000000000000" }), /changed since version/);
    } finally {
      client.close();
      await worker.close();
    }
  } finally {
    server.kill("SIGTERM");
    await new Promise((r) => server.on("exit", r));
    rmSync(dir, { recursive: true, force: true });
  }
});
