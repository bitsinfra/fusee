"""Confirm Obelisk parked each durable delay; read the host timestamp at resumed WASM code."""
import asyncio
from pathlib import Path
import sys
import time

from workloads import api,instant_ns
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"workloads"))
from sleep_wire import SleepWire


async def main():
    wire=SleepWire()
    receipts={}
    batch=asyncio.Queue()
    async def submit(data):
        result=await asyncio.to_thread(api,"POST","/v1/executions",{
            "ffqn":"fusee:bench/workflow.sleeper","params":[data["i"],data["deadline"]]})
        path="/v1/executions/"+result["ok"]
        while time.time_ns()//1_000_000<data["deadline"]:
            row=await asyncio.to_thread(api,"GET",path+"/status")
            if row["pending_state"]["status"]=="blocked_by_join_set":
                wire.emit("entered",data)
                receipts[result["ok"]]=data["i"]
                batch.put_nowait((path,data["i"],data["deadline"]))
                return result["ok"]
            await asyncio.sleep(.005)
        raise TimeoutError("Obelisk workflow did not register its durable delay")

    async def read_wake():
        while True:
            path,index,deadline=await batch.get()
            await asyncio.sleep(max(0,deadline/1000-time.time()))
            entries=await asyncio.to_thread(api,"GET",path+"/logs?show_streams=false")
            rows=[e for e in entries if e.get("message")=="awake "+str(index)]
            if rows:
                stamp=min(instant_ns(r["created_at"]) for r in rows)//1_000_000
                wire.send(dict(event="woke",i=index,at=stamp))
            else:
                batch.put_nowait((path,index,deadline));await asyncio.sleep(.01)

    async def result(receipt):
        deadline=time.monotonic()+120
        while time.monotonic()<deadline:
            status=await asyncio.to_thread(api,"GET","/v1/executions/"+receipt+"/status")
            if status["pending_state"]["status"]=="finished":
                answer=await asyncio.to_thread(api,"GET","/v1/executions/"+receipt)
                if answer!={"ok":receipts.pop(receipt)}:raise ValueError("incorrect Obelisk timer result")
                return answer["ok"]
            await asyncio.sleep(.01)
        raise TimeoutError("Obelisk result missing")

    workers=[asyncio.create_task(read_wake()) for _ in range(16)]
    try:await wire.run(submit,result)
    finally:
        for worker in workers:worker.cancel()
        await asyncio.gather(*workers,return_exceptions=True)


if __name__=="__main__":
    asyncio.run(main())
