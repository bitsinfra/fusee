"""Resident DBOS SQLite SDK worker for fusee-bench external."""
import asyncio
import importlib.metadata
import json
import os
from pathlib import Path
import time
from urllib.request import urlopen
from uuid import uuid4

from dbos import DBOS, SetWorkflowID
from sqlite import configure_dbos

traces = {}
mock_port = None


async def mock(reader, writer):
    try:
        header = await reader.readuntil(b"\r\n\r\n")
        delay = {b"/service-1":.001,b"/service-50":.05}[header.split()[1]]
        await asyncio.sleep(delay)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def serve(database):
    global mock_port
    configure_dbos(database)

    @DBOS.step()
    async def increment(data, index, value):
        marks = traces[data["attempt"]]
        begin = time.monotonic_ns()
        if data["workload"].startswith("service-"):
            def call():
                with urlopen(f'http://127.0.0.1:{mock_port}/{data["workload"]}', timeout=10) as response:
                    if response.read() != b"ok":
                        raise ValueError("mock service failed")
            await asyncio.to_thread(call)
        result = value + 1
        if marks[index] is not None:
            raise RuntimeError("activity repeated in fault-free trial")
        marks[index] = [str(begin), str(time.monotonic_ns())]
        return result

    waiting = {}
    signal_times = {}
    @DBOS.workflow()
    async def matrix(data):
        if data["workload"] == "signal":
            attempt = data["attempt"]
            waiting[attempt].set()
            received = await DBOS.recv_async("wake",90)
            at = str(time.monotonic_ns())
            if received != attempt:
                raise ValueError("unexpected signal value")
            traces[attempt][0] = [at,at]
            return 1
        if data["workload"] == "fanout":
            return sum(await asyncio.gather(*(increment(data,i,i) for i in range(100))))
        value = 0
        for i in range(data["steps"]):
            value = await increment(data,i,value)
        return value

    async def request(line):
        data = json.loads(line)
        if data["action"] == "clock":
            return {"clock_ns":str(time.monotonic_ns()), "engine":{"name":"dbos-sqlite",
                    "version":version, "durability":"SQLite WAL, synchronous=FULL on every connection"}}
        if data["action"] != "run" or data["steps"] < 1:
            raise ValueError("invalid request")
        attempt = str(uuid4())
        data["attempt"] = attempt
        traces[attempt] = [None] * data["steps"]
        try:
            if data["workload"] == "signal":
                waiting[attempt] = asyncio.Event()
                with SetWorkflowID(attempt):
                    handle = await DBOS.start_workflow_async(matrix,data)
                await asyncio.wait_for(waiting[attempt].wait(),timeout=60)
                signal_times[attempt] = str(time.monotonic_ns())
                await DBOS.send_async(attempt,attempt,"wake")
                value = await handle.get_result()
            else:
                value = await matrix(data)
            expected = 5050 if data["workload"] == "fanout" else data["steps"]
            if value != expected or any(m is None for m in traces[attempt]):
                raise ValueError("incorrect activity results")
            return {"id":data["id"], "attempt":attempt, "value":value, "body_ns":traces[attempt], "signal_sent_ns":signal_times.get(attempt)}
        finally:
            traces.pop(attempt)
            waiting.pop(attempt,None)
            signal_times.pop(attempt,None)

    async def connection(reader, writer):
        async def handle(line):
            try:
                result = await request(line)
            except Exception as e:
                result = {"error":str(e)}
            writer.write(json.dumps(result).encode()+b"\n")
            await writer.drain()
        try:
            async with asyncio.TaskGroup() as tasks:
                while line := await reader.readline():
                    tasks.create_task(handle(line))
        finally:
            writer.close()
            await writer.wait_closed()

    DBOS.launch()
    version = importlib.metadata.version("dbos")
    http = await asyncio.start_server(mock,"127.0.0.1",0)
    mock_port = http.sockets[0].getsockname()[1]
    try:
        async with http, await asyncio.start_server(connection,"127.0.0.1",int(os.getenv("FUSEE_ADAPTER_PORT","29733"))) as server:
            await server.serve_forever()
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    asyncio.run(serve(Path(os.getenv("DBOS_SQLITE_FILE","/mnt/fusee/dbos-matrix.sqlite"))))
