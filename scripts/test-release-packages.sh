#!/usr/bin/env bash
# Exercise the built packages outside the checkout, with registry access disabled.
set -euo pipefail
platform=${1:?supply the npm platform name}
npm_dir=$(cd "${2:?supply the npm tarball directory}" && pwd)
wheel_dir=$(cd "${3:?supply the wheel directory}" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
unset FUSEE_CLI PYTHONPATH NODE_PATH

npm init -y >/dev/null
npm install --offline --no-audit --no-fund "$npm_dir"/bitsinfra-fusee-[0-9]*.tgz "$npm_dir"/fusee-"$platform"-[0-9]*.tgz
./node_modules/.bin/fusee version
node --input-type=module <<'JS'
import assert from "node:assert/strict";
import { Engine, workflow } from "@bitsinfra/fusee";

const sample = workflow("sample", async (ctx) => ctx.step("answer", () => 42));
const engine = await Engine.open({durability: "memory", workflows: [sample]});
try {
  const run = await engine.start(sample, "sample-run", null);
  assert.equal(await run.result(), 42);
  console.log("npm workflow returned 42");
} finally {
  await engine.close();
}
JS

python3 -m venv --without-pip venv
python3 -m pip --python venv/bin/python install --no-index "$wheel_dir"/*.whl
venv/bin/fusee version
venv/bin/python -I - <<'PY'
import asyncio
import fusee

@fusee.workflow("sample")
async def sample(ctx, _):
    return await ctx.step("answer", lambda: 42)

async def main():
    async with await fusee.Engine.open(durability="memory", workflows=[sample]) as engine:
        run = await engine.start(sample, "sample-run", None)
        assert await run.result() == 42
        print("Python workflow returned 42")

asyncio.run(main())
PY
