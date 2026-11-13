"""python3 chain.py <package parent dir> <steps>: one memory engine, one
sequential no-op chain; prints the native calls it made (C calls into
fusee._native, by sys.setprofile) and the allocations (countmalloc)."""
import asyncio, ctypes, json, sys, threading
sys.path.insert(0, sys.argv[1])
import fusee
from fusee import Engine
n = int(sys.argv[2])
lib = ctypes.CDLL(None)
allocs = lib.countmalloc_total if hasattr(lib, "countmalloc_total") else None
if allocs: allocs.restype = ctypes.c_ulonglong
counts = {}
on = [False]
def prof(frame, event, arg):
    if on[0] and event == "c_call":
        # pyo3 gives the native classes the module "builtins": match their
        # names (NativeEngine.begin_step, ...) and module-level functions.
        k = getattr(arg, "__qualname__", "") or ""
        if k.startswith("Native") or "_native" in (getattr(arg, "__module__", None) or ""):
            counts[k] = counts.get(k, 0) + 1
async def main():
    if hasattr(fusee, "workflow"):
        @fusee.workflow("chain")
        async def chain(ctx, k):
            for i in range(k):
                await ctx.step("noop", lambda: i)
            return k
        e = await Engine.open(durability="memory", workflows=[chain])
        run = lambda id, k: e.start(chain, id, k)
    else:
        e = await Engine.open(durability="memory")
        @e.workflow("chain")
        async def chain(ctx, k):
            for i in range(k):
                await ctx.step("noop", lambda: i)
            return k
        run = lambda id, k: chain.start(id, k)
    await (await run("warm", 50)).result()
    a0 = allocs() if allocs else 0
    on[0] = True
    await (await run("solo", n)).result()
    on[0] = False
    a1 = allocs() if allocs else 0
    await e.close()
    print(json.dumps({"steps": n, "calls": sum(counts.values()), "allocs": a1 - a0, "by": counts}))
sys.setprofile(prof); threading.setprofile(prof)
asyncio.run(main())
