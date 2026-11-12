"""Drive Obelisk's execution API and measure dependent WASI activity boundaries."""
import argparse
import http.client
import json
import os
import platform
import subprocess
import time
from urllib.parse import urlsplit


def run(connection, count):
    start = time.monotonic_ns()
    connection.request("POST", "/v1/executions?follow=true",
                       json.dumps({"ffqn":"fusee:bench/workflow.chain", "params":[count]}),
                       {"Content-Type":"application/json", "Accept":"text/event-stream"})
    response = connection.getresponse()
    if not 200 <= response.status < 300:
        raise RuntimeError(f"HTTP {response.status}: {response.read()!r}")
    data = [json.loads(line[6:]) for line in response if line.startswith(b"data: ")]
    elapsed = (time.monotonic_ns()-start)/1000
    marks = next(item["ok"] for item in data if isinstance(item.get("ok"),list))
    if len(marks)!=count or [m[0] for m in marks] != list(range(1,count+1)):
        raise ValueError("workflow returned an incorrect count or dependency")
    gaps = [(nxt[1]-prev[2])/1000 for prev,nxt in zip(marks,marks[1:])]
    if any(end<begin for _,begin,end in marks) or any(g<0 for g in gaps):
        raise ValueError("the host clock went backwards")
    return elapsed, gaps


def stats(samples):
    s = sorted(samples)
    def percentile(q):
        return s[int((len(s)-1)*q+.5)] if s else None
    return {"count":len(s),"p50_us":percentile(.5),"p90_us":percentile(.9),"p99_us":percentile(.99),
            "p999_us":percentile(.999),"max_us":percentile(1)}


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("steps", type=int)
    cli.add_argument("runs", type=int)
    args = cli.parse_args()
    if min(args.steps,args.runs)<1:
        cli.error("steps and runs must be positive")
    url = urlsplit(os.environ.get("OBELISK_API_URL","http://127.0.0.1:5005"))
    connection = http.client.HTTPConnection(url.hostname,url.port,timeout=900)
    version = subprocess.check_output([os.environ.get("OBELISK","obelisk"),"--version"],text=True).strip()
    try:
        run(connection,min(args.steps,10))
        trials = [run(connection,args.steps) for _ in range(args.runs)]
    finally:
        connection.close()
    whole, gaps = zip(*trials)
    print(json.dumps({"machine":{"os":platform.system(),"arch":platform.machine(),"cores":os.cpu_count(),
                                "host_load":os.getloadavg(),"instance_type":os.environ.get("FUSEE_INSTANCE_TYPE")},
                      "engine":"obelisk","version":version,"durability":"WASM activity, SQLite synchronous=FULL",
                      "workload":"chain","result":[{"steps":args.steps,"step_to_step":stats([g for group in gaps for g in group]),
                      "workflow":stats(whole),"transition_samples_us":gaps,"workflow_samples_us":whole}]}))
