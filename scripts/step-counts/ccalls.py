"""python3 ccalls.py <package parent dir>: C calls per step of a 1000-step
chain (after a warm-up), by name, as sys.setprofile sees them."""
import sys, asyncio, collections, json
sys.path.insert(0, sys.argv[1])
import fusee
from fusee import Engine
seen = collections.Counter(); on = [False]
def prof(f, ev, arg):
    if on[0] and ev == "c_call":
        seen[getattr(arg, "__qualname__", None) or repr(arg)] += 1
async def body(ctx, k):
    for i in range(k):
        await ctx.step("noop", lambda: i)
    return k
async def main():
    if hasattr(fusee, "workflow"):
        chain = fusee.workflow("chain", body)
        e = await Engine.open(durability="memory", workflows=[chain]); run = lambda i, k: e.start(chain, i, k)
    else:
        e = await Engine.open(durability="memory"); chain = e.workflow("chain")(body); run = lambda i, k: chain.start(i, k)
    await (await run("w", 50)).result()
    on[0] = True
    await (await run("s", 1000)).result()
    on[0] = False
    await e.close()
sys.setprofile(prof)
asyncio.run(main())
print(json.dumps({k: round(v / 1000, 2) for k, v in seen.items() if v >= 500}))
