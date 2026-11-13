"""Use a DBOS durable sleep and reopen each accepted workflow by its ID."""
import asyncio
import os
from pathlib import Path
import sys
import time
from dbos import DBOS, SetWorkflowID
from sqlite import configure_dbos
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"workloads"))
from sleep_wire import SleepWire


async def main():
    wire=SleepWire()
    database=Path(os.environ["FUSEE_WORKLOAD_DIR"])/"timers.sqlite"
    database.parent.mkdir(exist_ok=True,parents=True)
    configure_dbos(database)

    @DBOS.step()
    async def clock_ms():
        return time.time_ns()//1_000_000

    @DBOS.workflow()
    async def population(data):
        now=await clock_ms()
        wire.emit("entered",data)
        await DBOS.sleep_async(max(0,(data["deadline"]-now)/1000))
        wire.emit("woke",data)
        return data["i"]
    DBOS.launch()
    async def submit(data):
        with SetWorkflowID(f'{data["campaign"]}-{data["i"]}'):
            handle=await DBOS.start_workflow_async(population,data)
        return handle.get_workflow_id()
    async def result(receipt):
        return await (await DBOS.retrieve_workflow_async(receipt)).get_result()
    await wire.run(submit,result)


if __name__=="__main__":
    asyncio.run(main())
