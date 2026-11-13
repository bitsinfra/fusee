"""DBOS SQLite worker. The parent kills this interpreter while effects are in flight."""
import asyncio
import json
import os
from pathlib import Path
import socket
from urllib.request import Request, urlopen

from dbos import DBOS, SetWorkflowID
from sqlite import configure_dbos


async def main():
    path = Path(os.environ["FUSEE_WORKLOAD_DIR"])
    path.mkdir(exist_ok=True)
    configure_dbos(path / "crash.sqlite")
    generation = int(os.environ["FUSEE_GENERATION"])

    @DBOS.step()
    async def effect(run, index, value):
        def invoke():
            data = json.dumps(dict(run=run, seq=index, value=value, generation=generation)).encode()
            req = Request(os.environ["FUSEE_WITNESS"], data=data, headers={"Content-Type":"application/json"})
            with urlopen(req, timeout=60) as reply:
                return json.load(reply)["value"]
        return await asyncio.to_thread(invoke)

    @DBOS.workflow()
    async def chain(data):
        value = data["value"]
        for i in range(data["steps"]):
            value = await effect(data["run"], i, value)
        return value

    DBOS.launch()
    sock = socket.socket(fileno=int(os.environ["NODE_CHANNEL_FD"]))
    reader, writer = await asyncio.open_connection(sock=sock)
    def send(**obj):
        writer.write(json.dumps(obj).encode()+b"\n")

    async def handle(cmd):
        run = cmd["run"]
        try:
            if cmd["kind"] == "start":
                with SetWorkflowID(run):
                    handle = await DBOS.start_workflow_async(chain, cmd["input"])
                send(kind="ack", run=run, receipt=handle.get_workflow_id())
            else:
                handle = await DBOS.retrieve_workflow_async(cmd["receipt"])
            send(kind="done", run=run, value=await handle.get_result())
        except Exception as error:
            send(kind="error", run=run, message=str(error))
        await writer.drain()
    send(kind="ready")
    async with asyncio.TaskGroup() as tasks:
        while line := await reader.readline():
            tasks.create_task(handle(json.loads(line)))


if __name__ == "__main__":
    asyncio.run(main())
