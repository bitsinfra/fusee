"""Matching Python SDK chain from step return to its dependent successor."""
import asyncio
import json
import os
import platform
import sys
import time
import fusee
from fusee import Engine


async def main(folder,n,runs):
    shards=int(os.environ.get("FUSEE_SDK_SHARDS",8))
    e=await Engine.open(folder,durability="local",shards=shards)
    marks=[]
    @fusee.workflow("chain")
    async def chain(ctx,count):
        value=0
        def inc():
            start=time.perf_counter_ns()
            result=value+1
            marks.append((start,time.perf_counter_ns()))
            return result
        for _ in range(count):
            value=await ctx.step("noop",inc)
        return value
    e.register(chain)
    gaps,whole,observations=[],[],[]
    try:
        for cold,trial in [(cold,r) for cold in (True,False) for r in range(runs)]:
            marks.clear()
            before=e.shard_metrics()
            at=time.perf_counter_ns()
            result=await (await e.start(chain,f"chain-{trial}",n)).result()
            elapsed=(time.perf_counter_ns()-at)/1000
            if result!=n or len(marks)!=n:
                raise RuntimeError("incorrect dependency result or sample count")
            samples=[(right[0]-left[1])/1000 for left,right in zip(marks,marks[1:])]
            observations.append(dict(id=f"chain-{trial}",first_execution=cold,workflow_us=elapsed,transition_samples_us=samples,metrics_before=before,metrics_after=e.shard_metrics()))
            if not cold:
                whole.append(elapsed)
                gaps.append(samples)
    finally:
        await e.close()
    def stats(values):
        ordered=sorted(values)
        def p(q):
            return ordered[int((len(ordered)-1)*q+.5)] if ordered else None
        return dict(count=len(values),p50_us=p(.5),p90_us=p(.9),p99_us=p(.99),p999_us=p(.999),max_us=p(1))
    print(json.dumps({"machine":{"os":platform.system(),"arch":platform.machine(),"cores":os.cpu_count(),"host_load":os.getloadavg()},
            "engine":"fusee-python","version":sys.version.split()[0],"durability":"local fdatasync before outputs and before dependent effects",
            "workload":"chain","result":[{"steps":n,"step_to_step":stats([v for row in gaps for v in row]),"workflow":stats(whole),
            "transition_samples_us":gaps,"workflow_samples_us":whole,"observations":observations,"shards":shards}]}))


if __name__=="__main__":
    asyncio.run(main(sys.argv[1],int(sys.argv[2]),int(sys.argv[3])))
