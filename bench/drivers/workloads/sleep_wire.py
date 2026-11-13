"""Control channel for Python sleep workers. The external driver owns the observations."""
import asyncio
import json
import os
import time


class SleepWire:
    def __init__(self):
        self.writer=None
        self.campaign=os.environ["FUSEE_SLEEP_CAMPAIGN"]

    def emit(self, event, data):
        if self.writer and data["campaign"]==self.campaign:
            self.send(dict(event=event,i=data["i"],at=time.time_ns()//1_000_000))

    def send(self, msg):
        self.writer.write(json.dumps(msg).encode()+b"\n")

    async def run(self, submit, result):
        reader,self.writer=await asyncio.open_connection("127.0.0.1",int(os.environ["FUSEE_SLEEP_PORT"]))
        self.send(dict(event="ready",pid=os.getpid()))
        async def one(cmd):
            i=cmd["i"]
            try:
                if cmd["action"]=="start":
                    self.send(dict(event="ack",i=i,receipt=await submit(cmd)))
                elif cmd["action"]=="result":
                    self.send(dict(event="done",i=i,value=await result(cmd["receipt"])))
                else:raise ValueError("unknown sleep command")
            except Exception as e:
                self.send(dict(event="error",i=i,message=str(e)))
            await self.writer.drain()
        async with asyncio.TaskGroup() as tasks:
            while line:=await reader.readline():tasks.create_task(one(json.loads(line)))
