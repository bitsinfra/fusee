"""Resident Obelisk workflow client. WASI activity clocks use host epoch nanoseconds."""
import asyncio
import calendar
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
import subprocess
import time
from urllib.parse import urlsplit, quote
from uuid import uuid4

API = urlsplit(os.getenv("OBELISK_API_URL","http://127.0.0.1:5005"))


def api(method, path, payload=None):
    connection=http.client.HTTPConnection(API.hostname,API.port,timeout=30)
    try:
        connection.request(method,path,None if payload is None else json.dumps(payload),
                           {"Content-Type":"application/json","Accept":"application/json"})
        response=connection.getresponse()
        body=response.read()
        if not 200<=response.status<300:
            raise RuntimeError(f"{method} {path} returned {response.status}: {body[:300]!r}")
        return json.loads(body) if body else None
    finally:connection.close()


def instant_ns(iso):
    seconds,dot,fraction=iso.removesuffix("Z").partition(".")
    return calendar.timegm(time.strptime(seconds,"%Y-%m-%dT%H:%M:%S"))*10**9+int((fraction+"000000000")[:9])


def signal(request):
    challenge=str(uuid4())
    parent=api("POST","/v1/executions",{"ffqn":"fusee:bench/workflow.await-signal","params":[challenge]})
    if isinstance(parent,dict):parent=parent.get("ok",parent.get("execution_id"))
    path="/v1/executions/"+quote(parent,safe="")
    deadline=time.monotonic()+60
    def logs():
        return api("GET",path+"/logs?show_streams=false")
    while time.monotonic()<deadline:
        waiting=next((e for e in logs() if e.get("message","").startswith("waiting ")),None)
        if waiting and api("GET",path+"/status")["pending_state"]["status"]=="blocked_by_join_set":
            child=waiting["message"].removeprefix("waiting ")
            break
        time.sleep(.005)
    else:raise TimeoutError("missing durable external wait")
    sent=time.time_ns()
    api("PUT","/v1/executions/"+quote(child,safe="")+"/stub",{"ok":challenge})
    while time.monotonic()<deadline:
        entries=[e for e in logs() if e.get("message")=="resumed "+challenge]
        if entries:
            # This timestamp comes from the workflow host at its first user
            # instruction after join, before the client polls for the result.
            resumed=min(instant_ns(e["created_at"]) for e in entries)
            result=api("GET",path)
            if result=={"ok":challenge}:
                return dict(id=request["id"],attempt=challenge,value=1,body_ns=[[str(resumed),str(resumed)]],
                            signal_sent_ns=str(sent),parent=parent,child=child)
        time.sleep(.005)
    raise TimeoutError("missing resumed challenge or workflow result")


def execute(request):
    if request["workload"]=="signal":return signal(request)
    connection=http.client.HTTPConnection(API.hostname,API.port,timeout=300)
    kind,steps=request["workload"],request["steps"]
    try:
        connection.request("POST","/v1/executions?follow=true",
            json.dumps({"ffqn":"fusee:bench/workflow.matrix","params":[kind,steps]}),
            {"Content-Type":"application/json","Accept":"text/event-stream"})
        response=connection.getresponse()
        if not 200 <= response.status < 300:
            raise RuntimeError(f"workflow HTTP {response.status}: {response.read()[:400]!r}")
        events=[json.loads(line[6:]) for line in response if line.startswith(b"data: ")]
        marks=next((e["ok"] for e in events if isinstance(e.get("ok"),list)),None)
        if marks is None:raise RuntimeError("Obelisk workflow result missing: "+str(events[-1])[:400])
        if kind=="fanout":marks.sort(key=lambda m:m[0])
        if len(marks)!=steps or [m[0] for m in marks]!=list(range(1,steps+1)):
            raise ValueError("incorrect WASI activity population")
        value=sum(m[0] for m in marks) if kind=="fanout" else marks[-1][0]
        return dict(id=request["id"],attempt=str(uuid4()),value=value,body_ns=[[str(a),str(b)] for _,a,b in marks])
    finally:connection.close()


async def mock(reader,writer):
    try:
        head=await reader.readuntil(b"\r\n\r\n")
        delay={b"/service-1":.001,b"/service-50":.05}[head.split()[1]]
        await asyncio.sleep(delay)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        await writer.drain()
    finally:writer.close();await writer.wait_closed()


async def main():
    version=subprocess.check_output([os.getenv("OBELISK","obelisk"),"--version"],text=True).strip()
    engine=dict(name="obelisk",version=version,durability="WASM activity, SQLite synchronous=FULL",
                clock="host epoch nanoseconds; calibrated against generator monotonic time")
    loop=asyncio.get_running_loop()
    async def connection(reader,writer):
        async def one(line):
            try:
                req=json.loads(line)
                if req["action"]=="clock":
                    result=dict(clock_ns=str(time.time_ns()),engine=engine)
                elif req["action"]=="run":
                    result=await loop.run_in_executor(pool,execute,req)
                else:raise ValueError("invalid benchmark request")
            except Exception as e:result=dict(error=str(e))
            writer.write(json.dumps(result).encode()+b"\n")
            await writer.drain()
        async with asyncio.TaskGroup() as tasks:
            while line:=await reader.readline():tasks.create_task(one(line))
        writer.close();await writer.wait_closed()
    with ThreadPoolExecutor(max_workers=128) as pool:
        async with await asyncio.start_server(mock,"127.0.0.1",29780), await asyncio.start_server(connection,"127.0.0.1",int(os.getenv("FUSEE_ADAPTER_PORT","29733"))) as server:
            await server.serve_forever()


if __name__=="__main__":asyncio.run(main())
