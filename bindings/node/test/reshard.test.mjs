// A directory opened with another shard count is refused with a message
// naming the command that changes it (`fusee reshard`); nothing reshards
// on its own.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine } from "../dist/index.js";

test("another shard count is refused, naming fusee reshard", async () => {
  const dir = mkdtempSync(join(tmpdir(), "fusee-reshard-"));
  try {
    const engine = await Engine.open({ dir, flush: "fsync", shards: 2 });
    await engine.close();
    await assert.rejects(Engine.open({ dir, flush: "fsync", shards: 3 }), /fusee reshard/);
    // The directory is unchanged and still opens with its own count.
    const again = await Engine.open({ dir, flush: "fsync", shards: 2 });
    await again.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
