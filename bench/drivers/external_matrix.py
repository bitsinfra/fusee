"""Rotate validated SDK workloads. The Rust generator retains scheduled arrivals and raw samples."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from matrix import BASE, COMPOSE, OPTIONAL, port_up, service, wait_ready

DEFAULT = ("temporal","restate","dbos-postgres","dbos-sqlite","absurd","inngest","hatchet",
           "inngest-buffered","temporal-local","temporal-tuned","fusee-local","fusee-server","obelisk")
RATES = (1, 4, 16)
STEPS = 10

def worker(system, env):
    if system == "dbos-sqlite":
        return [env.get("DBOS_PYTHON","/opt/fusee-python/bin/python"),BASE/"dbos/sqlite-workloads.py"]
    if system == "obelisk":
        return ["python3",BASE/"obelisk/workloads.py"]
    if system == "hatchet":
        return [BASE/"hatchet/run.sh","--workloads"]
    return ["node",BASE/system.split("-")[0]/"workloads.mjs"]


def kill_trial(system, trial, dest, env, log):
    adapter = "dbos" if system == "dbos-postgres" else system if system == "dbos-sqlite" else system.split("-")[0]
    # Offer arrivals faster than a worker can drain its in-flight effects.
    # Retain the unfinished population and the actual run receipts at the cut.
    rate = 500
    folder = dest / f"kill-{system}-{trial}"
    with tempfile.TemporaryDirectory(prefix="crash-",dir=env.get("FUSEE_DATA_ROOT","/mnt/fusee")) as engine:
        env=dict(env,FUSEE_WORKLOAD_DIR=engine)
        args=[BASE/"hatchet/run.sh","--kill"] if system=="hatchet" else ["node",BASE/"workloads/kill.mjs",adapter]
        subprocess.run([*map(str,args),str(folder),"200",str(rate)],env=env,stdout=log,stderr=log,check=True,timeout=450)


def sleep_trial(system,trial,dest,env,log,process):
    # Include the SDK, orchestration services and the database. The coordinator
    # is separate and its own memory is reported outside these measurements.
    roots=[] if process is None else [process.pid]
    services={"hatchet":["hatchet","postgres"],"restate":["restate"],"absurd":["postgres"],
              "dbos-postgres":["postgres"]}.get(system,["temporal","postgres"] if system.startswith("temporal") else [])
    for service_name in services:
        cid=subprocess.check_output([*COMPOSE,"ps","-q",service_name],text=True).strip()
        if not cid:raise RuntimeError("missing service for memory accounting: "+service_name)
        roots.append(int(subprocess.check_output(["docker","inspect","-f","{{.State.Pid}}",cid],text=True)))
    folder=dest/f"sleepers-{system}-{trial}"
    with tempfile.TemporaryDirectory(prefix="population-",dir=env.get("FUSEE_DATA_ROOT","/mnt/fusee")) as directory:
        child_env=dict(env,FUSEE_WORKLOAD_DIR=directory)
        count=int(env.get("FUSEE_SLEEP_COUNT","1000000"))
        lead=int(env.get("FUSEE_SLEEP_LEAD","420"))
        drain=int(env.get("FUSEE_SLEEP_DRAIN","240"))
        args=["python3",BASE/"workloads/sleepers.py",system,folder,"--count",str(count),
              "--lead",str(lead),"--drain",str(drain)]
        for pid in roots:args+=["--service-pid",str(pid)]
        result=subprocess.run(list(map(str,args)),env=child_env,stdout=log,stderr=log,timeout=lead+drain+200)
        state=json.loads((folder/"state.json").read_text())
        if result.returncode:
            if state.get("error")=="population did not enter its timers before the common deadline":
                # A bounded attempted population is a measured capacity result.
                return False
            raise RuntimeError("sleeper verification failed: "+str(state.get("error")))
        return state.get("complete") is True


def observed(system, trial, dest, kinds):
    env = dict(os.environ,DBOS_DATABASE_URL="postgres://postgres:bench@127.0.0.1:5432/dbos",
               ABSURD_DATABASE_URL="postgres://postgres:bench@127.0.0.1:5432/absurd",
               FUSEE_EXTERNAL_STEPS=str(STEPS),FUSEE_ADAPTER_PORT="29733")
    if system.startswith("fusee-"):
        # fusee-server-unix is server mode; its address comes from the service.
        env["FUSEE_MODE"]=system.split("-")[1]
    env.setdefault("HOME",str(Path.home()))
    if system=="obelisk" and "kill" in kinds:
        with (dest/f"external-kill-{system}-{trial}.log").open("w") as log:
            kill_trial(system,trial,dest,env,log)
        kinds=[k for k in kinds if k!="kill"]
        if not kinds:return
    with (dest/f"external-{system}-{trial}.log").open("w") as log, service(system,env,log) as server:
        if "sleepers" in kinds:
            if not sleep_trial(system,trial,dest,env,log,server):return False
            kinds=[k for k in kinds if k!="sleepers"]
            if not kinds:return
        if "kill" in kinds:
            kill_trial(system,trial,dest,env,log)
            kinds=[k for k in kinds if k!="kill"]
            if not kinds:return
        process = subprocess.Popen(list(map(str,worker(system,env))),env=env,stdout=log,stderr=log)
        try:
            def ready():
                if process.poll() is not None:
                    raise RuntimeError("worker exited before it was ready")
                return port_up(29733)
            wait_ready(ready)
            def run(kind, label, rate=1, seconds=5):
                args=[env["FUSEE_BENCH"],"external",kind,"--server","127.0.0.1:29733","--trials","2",
                      "--rate",str(rate),"--seconds",str(seconds),"--steps",str(STEPS)]
                if env.get("FUSEE_USD_PER_HOUR"):args += ["--usd-per-hour",env["FUSEE_USD_PER_HOUR"]]
                result=subprocess.run(args,stdout=subprocess.PIPE,stderr=log,check=True,timeout=600)
                row=json.loads(result.stdout)
                assert row["offered"]==row["completed"]==len(row["samples"])
                row.update(trial=trial,system=system,price_usd_per_hour=env.get("FUSEE_USD_PER_HOUR"))
                (dest/f"external-{system}-{trial}-{label}.json").write_text(json.dumps(row)+"\n")
                return row
            for kind in kinds:
                if kind=="cost":
                    for rate in RATES:run("open",f"cost-{rate}",rate)
                elif kind=="max":
                    best=None
                    for k in range(16):
                        rate=2**k
                        row=run("open",f"max-{rate}",rate)
                        if row["step_to_step"]["p99_us"]>1000 or row["achieved_steps_per_s"]<.99*rate*STEPS:break
                        best=rate
                    (dest/f"external-{system}-{trial}-max.json").write_text(json.dumps({"highest_passing_starts_per_s":best,"last_tested":rate})+"\n")
                else:run(kind,kind)
        finally:
            process.terminate()
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("directory",type=Path)
    p.add_argument("--systems",default=",".join(DEFAULT))
    p.add_argument("--workloads",default="fanout,service-1,service-50,open,max,cost")
    p.add_argument("--trials",type=int,default=5)
    args=p.parse_args()
    systems=args.systems.split(",")
    if not 1 <= args.trials <= 5 or set(systems)-set(DEFAULT+OPTIONAL):
        p.error("invalid systems or trial count")
    failures=[]
    partial=set()
    for trial in range(args.trials):
        offset=(trial*((len(systems)+1)//2))%len(systems)
        for system in systems[offset:]+systems[:offset]:
            if system in partial:continue
            print(f"trial {trial}, external workloads, {system}",flush=True)
            try:
                if observed(system,trial,args.directory,args.workloads.split(",")) is False:partial.add(system)
            except (ValueError,RuntimeError,AssertionError,OSError,subprocess.SubprocessError) as err:
                failures.append(dict(system=system,trial=trial,error=str(err)))
                (args.directory/"external-failures.json").write_text(json.dumps(failures,indent=2))
    if failures:raise SystemExit("external benchmark errors: "+str(len(failures)))


if __name__=="__main__":
    main()
