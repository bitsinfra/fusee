// A process that crashes with runs in flight on every worker
// (workers.test.mjs). Arguments: data directory, worker count, run count.
import { Engine } from "../dist/index.js";

const [dir, workers, runs] = process.argv.slice(2);
const engine = await Engine.open({
  dir,
  flush: "fsync",
  workers: Number(workers),
  module: new URL("./workers.module.mjs", import.meta.url),
});
const waiter = engine.workflow("waiter");
// Each start resolves once its record is durable.
await Promise.all(Array.from({ length: Number(runs) }, (_, i) => waiter.start(`crash-${i}`, null)));
await engine.workflow("chain").start("crash-chain", 50);
process.stdout.write(JSON.stringify(engine.list().map((r) => r.shard)) + "\n");
// No close: the process dies with every worker's shards open.
process.kill(process.pid, "SIGKILL");
