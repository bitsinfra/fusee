"""python3 flushes.py <package parent dir> <data dir>: flushes per step of a
`local` engine over a 1,000-step chain and 100 one-step runs at once."""
import asyncio, json, shutil, sys
sys.path.insert(0, sys.argv[1])
import fusee
from fusee import Engine
d = sys.argv[2]
shutil.rmtree(d, ignore_errors=True)
async def chain_body(ctx, k):
    for i in range(k):
        await ctx.step("noop", lambda: i)
    return k
async def one_body(ctx, i):
    return await ctx.step("s", lambda: i)
async def main():
    if hasattr(fusee, "workflow"):
        chain, one = fusee.workflow("chain", chain_body), fusee.workflow("one", one_body)
        e = await Engine.open(d, shards=1, workflows=[chain, one])
        start = lambda wf, i, x: e.start(wf, i, x)
    else:
        e = await Engine.open(d, shards=1)
        chain, one = e.workflow("chain")(chain_body), e.workflow("one")(one_body)
        start = lambda wf, i, x: wf.start(i, x)
    def total(k):
        return sum(int(m.get(k, 0)) for m in e.shard_metrics())
    async def measure(f):
        f0, s0 = total("flushes"), total("steps_run")
        await f()
        return round((total("flushes") - f0) / (total("steps_run") - s0), 3)
    await (await start(chain, "warm", 20)).result()
    c = await measure(lambda: _run(start(chain, "c", 1000)))
    f = await measure(lambda: asyncio.gather(*[_run(start(one, f"f{i}", i)) for i in range(100)]))
    await e.close()
    print(json.dumps({"chain": c, "fanout": f}))
async def _run(h):
    return await (await h).result()
asyncio.run(main())
