"""Measure a common wake deadline from outside the measured SDK worker."""
import argparse
import asyncio
from array import array
import gzip
import resource
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

BASE=Path(__file__).resolve().parents[1]


def memory(worker,extra_pids):
    roots={worker,*extra_pids}
    if Path("/proc/self/smaps_rollup").exists():
        entries={}
        for p in Path("/proc").glob("[0-9]*"):
            try:
                status=dict(line.split(":",1) for line in (p/"status").read_text().splitlines() if ":" in line)
                entries[int(p.name)]=(int(status["PPid"]),status["Name"].strip())
            except (OSError,KeyError,ValueError):continue
        included=set(roots)
        while True:
            nxt=included|{pid for pid,(parent,_) in entries.items() if parent in included}
            if nxt==included:break
            included=nxt
        rows=[]
        for pid in sorted(included):
            try:
                with Path(f"/proc/{pid}/smaps_rollup").open() as f:
                    sizes={k:int(v.split()[0])*1024 for line in f if ":" in line
                           for k,v in [line.split(":",1)] if k in ("Rss","Pss")}
                rows.append(dict(pid=pid,process=entries[pid][1],rss_bytes=sizes["Rss"],pss_bytes=sizes["Pss"]))
            except (OSError,KeyError):continue
        if not rows or worker not in [r["pid"] for r in rows]:raise RuntimeError("missing measured worker memory")
        return dict(processes=rows,rss_bytes=sum(r["rss_bytes"] for r in rows),pss_bytes=sum(r["pss_bytes"] for r in rows))
    rss=int(subprocess.check_output(["ps","-o","rss=","-p",str(worker)]))*1024
    return dict(worker_rss_bytes=rss,scope="development Mac SDK worker; service RSS unavailable")


async def trial(system,dest,count,lead,drain,parallel,extra_pids):
    dest.mkdir()
    campaign="sleep-"+uuid4().hex
    ack=bytearray(count);entered=array("q",[0])*count;woke=array("q",[0])*count;done=bytearray(count)
    receipts=[None]*count
    requested=acked=entered_before=woken=finished=checked=0
    pending_results=asyncio.Queue()
    state=dict(system=system,campaign=campaign,requested=count,window=parallel,deadline_lead_s=lead)
    outcome=asyncio.get_running_loop().create_future()
    snapshot_taken=False
    def save():
        state.update(offered=requested,acknowledged=acked,entered_before_deadline=entered_before,woken=woken,
                     results_confirmed=finished)
        tmp=dest/"state.tmp";tmp.write_text(json.dumps(state,indent=2));tmp.replace(dest/"state.json")
    async def client(reader,writer):
        nonlocal requested,acked,entered_before,woken,finished,checked,snapshot_taken
        def send(data):writer.write(json.dumps(data,separators=(",",":")).encode()+b"\n")
        try:
            hello=json.loads(await asyncio.wait_for(reader.readline(),120))
            if hello["event"]!="ready":raise ValueError("worker not ready")
            pid=hello["pid"];state["baseline"]=memory(pid,extra_pids)
            deadline=(int(time.time())+1+int(lead))*1000;state["deadline_ms"]=deadline;save()
            with gzip.open(dest/"observations.jsonl.gz","wt",compresslevel=1) as raw:
                last_save=time.monotonic()
                while finished<count:
                    now=int(time.time()*1000)
                    if now>=deadline+drain*1000:raise TimeoutError("timers or results missing at drain cutoff")
                    # Stop a partial trial at the common deadline. The exact
                    # population achieved is retained in state and the witness.
                    if now>=deadline and entered_before<count:
                        state["at_deadline"]=memory(pid,extra_pids)
                        raise TimeoutError("population did not enter its timers before the common deadline")
                    while requested<count and requested-acked<parallel:
                        send(dict(action="start",i=requested,campaign=campaign,deadline=deadline))
                        requested+=1
                    while checked-finished<parallel and not pending_results.empty():
                        index=pending_results.get_nowait()
                        if receipts[index] is None:
                            pending_results.put_nowait(index);break
                        send(dict(action="result",i=index,receipt=receipts[index]));checked+=1
                    await writer.drain()
                    if not snapshot_taken and acked==count and entered_before==count and not woken:
                        state["sleeping"]=memory(pid,extra_pids);state["sample_time_ms"]=now;snapshot_taken=True;save()
                    try:line=await asyncio.wait_for(reader.readline(),.3)
                    except TimeoutError:continue
                    if not line:raise RuntimeError("worker exited during timer trial")
                    item=json.loads(line);index=item["i"];kind=item["event"]
                    raw.write(json.dumps(item,separators=(",",":"))+"\n")
                    if not isinstance(index,int) or not 0<=index<requested:raise ValueError("invalid timer identity")
                    if kind=="error":raise RuntimeError(item["message"])
                    if kind=="ack":
                        if ack[index] or not item["receipt"]:raise ValueError("duplicate or empty receipt")
                        receipts[index]=item["receipt"];ack[index]=1;acked+=1
                    elif kind=="entered":
                        if not entered[index]:
                            entered[index]=item["at"];entered_before+=int(item["at"]<deadline)
                    elif kind=="woke":
                        if not woke[index]:
                            woke[index]=item["at"];woken+=1;pending_results.put_nowait(index)
                    elif kind=="throttled":
                        state["explicit_throttles"]=state.get("explicit_throttles",0)+1
                    elif kind=="done":
                        if done[index] or item["value"]!=index or not woke[index]:raise ValueError("invalid terminal result")
                        done[index]=1;finished+=1
                    else:raise ValueError("unknown observation")
                    if time.monotonic()-last_save>5:save();last_save=time.monotonic()
            if len(set(receipts))!=count or not snapshot_taken:raise ValueError("incomplete sleeping population")
            late=sorted(w-deadline for w in woke)
            state["early_wakes"]=sum(v<0 for v in late)
            state["lateness_ms"]={p:late[round((count-1)*q)] for p,q in [("p50",.5),("p99",.99),("max",1)]}
            state["complete"]=True
            outcome.set_result(None)
        except Exception as error:
            state["complete"]=False;state["error"]=str(error)
            if not outcome.done():outcome.set_exception(error)
        finally:
            state["controller_peak_rss_native_units"]=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            save();writer.close()
    server=await asyncio.start_server(client,"127.0.0.1",0)
    env=dict(os.environ,FUSEE_SLEEP_PORT=str(server.sockets[0].getsockname()[1]),FUSEE_SLEEP_CAMPAIGN=campaign)
    name=system.split("-")[0]
    if system.startswith("fusee-"):env["FUSEE_MODE"]=system.split("-")[1]
    if system=="hatchet":args=[BASE/"hatchet/run.sh","--sleepers"]
    elif system=="dbos-sqlite":args=[env.get("DBOS_PYTHON","/opt/fusee-python/bin/python"),BASE/"dbos/sqlite-sleepers.py"]
    elif system=="obelisk":args=[sys.executable,BASE/"obelisk/sleepers.py"]
    else:args=["node",BASE/name/"sleepers.mjs"]
    async with server:
        proc=await asyncio.create_subprocess_exec(*map(str,args),env=env)
        exited=asyncio.create_task(proc.wait())
        try:
            completed,_=await asyncio.wait([outcome,exited],timeout=lead+drain+180,return_when=asyncio.FIRST_COMPLETED)
            if outcome in completed:await outcome
            else:raise RuntimeError("timer worker stopped or timed out before completing the population")
        finally:
            if proc.returncode is None:proc.terminate()
            try:await asyncio.wait_for(exited,10)
            except TimeoutError:proc.kill();await proc.wait()


if __name__=="__main__":
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("system");cli.add_argument("directory",type=Path)
    cli.add_argument("--count",type=int,default=1_000_000);cli.add_argument("--lead",type=int,default=900)
    cli.add_argument("--drain",type=int,default=600);cli.add_argument("--window",type=int,default=128)
    cli.add_argument("--service-pid",action="append",default=[],type=int)
    args=cli.parse_args()
    if min(args.count,args.lead,args.drain,args.window)<=0:cli.error("positive timings and population required")
    asyncio.run(trial(args.system,args.directory,args.count,args.lead,args.drain,args.window,args.service_pid))
