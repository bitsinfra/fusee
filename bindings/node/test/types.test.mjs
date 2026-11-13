// The published declarations compile in a strict project that does not
// skip library checks: every name dist/*.d.ts exports must exist there
// (stripInternal removes `@internal` declarations, not re-exports of them).
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);

test("the declarations pass a strict check with skipLibCheck off", () => {
  const tsc = require.resolve("typescript/bin/tsc");
  const project = fileURLToPath(new URL("./types/tsconfig.json", import.meta.url));
  const r = spawnSync(process.execPath, [tsc, "-p", project], { encoding: "utf8" });
  assert.equal(r.status, 0, `${r.stdout}${r.stderr}`);
});
