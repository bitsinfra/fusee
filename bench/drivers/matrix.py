"""Rotate single-host chain trials. Keep every attempt and each engine's storage configuration."""
import argparse
from contextlib import contextmanager
import json
import os
import pwd
from pathlib import Path
import socket
import subprocess
import time
from urllib.request import urlopen

BASE = Path(__file__).resolve().parent
DATA = Path("/mnt/fusee")
SIZES = (1, 10, 100, 1000)
COMPOSE = ["docker","compose","-p","fusee-bench","-f",str(BASE/"docker-compose.yml")]
SYSTEMS = ("fusee-local","temporal","restate","dbos-postgres","inngest","hatchet","absurd",
           "obelisk","temporal-tuned","dbos-sqlite","inngest-buffered","temporal-local","fusee-server")
# Selectable with --systems or FUSEE_MATRIX_CELLS; not in the default rotation.
# fusee-server-unix: the same server, with workers connected over a Unix socket.
OPTIONAL = ("fusee-server-unix",)
SERVER_SOCKET = DATA/"fusee-server.sock"


def cmd(args, **opts):
    return subprocess.run(list(map(str,args)),check=True,**opts)


def wait_ready(fn):
    for _ in range(150):
        try:
            if fn():
                return
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError("service did not become ready")


def port_up(port):
    with socket.create_connection(("127.0.0.1",port),timeout=1):
        return True


@contextmanager
def service(system, env, log):
    process = None
    compose, containers = COMPOSE.copy(), []
    try:
        if system.startswith("temporal"):
            if system=="temporal-tuned":
                compose += ["-f",str(BASE/"temporal/tuned-compose.yml")]
                env["TEMPORAL_TUNED"]="1"
            env["TEMPORAL_LOCAL_ACTIVITIES"]="1" if system=="temporal-local" else "0"
            containers=["temporal"]
            cmd([*compose,"up","-d",*containers], stdout=log, stderr=log)
            wait_ready(lambda: subprocess.run([*compose,"exec","-T","temporal","temporal","operator",
                "namespace","describe","--namespace","default","--address","temporal:7233"],
                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0)
            if system=="temporal-tuned":
                time.sleep(125)
        elif system in ("hatchet","restate"):
            containers=[system]
            cmd([*compose,"up","-d",system], stdout=log, stderr=log)
            wait_ready(lambda: port_up(7077 if system=="hatchet" else 9070))
            if system=="restate":
                env["RESTATE_WORKER_URI"]="http://host.docker.internal:9080"
        elif system.startswith("inngest"):
            env["CHECKPOINT"]="1" if system=="inngest-buffered" else "0"
            folder=DATA/system
            folder.mkdir(exist_ok=True)
            process=subprocess.Popen(["inngest","dev","--persist","--no-discovery","--no-poll","--host","127.0.0.1"],
                                     cwd=folder,env=env,stdout=log,stderr=log)
            wait_ready(lambda: port_up(8288))
        elif system=="obelisk":
            env.update(XDG_DATA_HOME=str(DATA/"obelisk"),OBELISK_APP_NAME="fusee-bench")
            process=subprocess.Popen(["obelisk","server","run","--no-auth","-s",BASE/"obelisk/server.toml","-a",BASE/"obelisk/app.toml",
                                      "-d",BASE/"obelisk/obelisk.toml"],env=env,stdout=log,stderr=log)
            wait_ready(lambda: port_up(5005))
        elif system in ("fusee-server","fusee-server-unix"):
            server=[env["FUSEE_CLI"],"server","--dir",DATA/"fusee-server","--listen","127.0.0.1:7600","--shards","8"]
            if system=="fusee-server-unix":
                SERVER_SOCKET.unlink(missing_ok=True)
                server+=["--unix",SERVER_SOCKET]
                # Every worker of this cell, Rust or SDK, connects here.
                env["FUSEE_SERVER_ADDRESS"]=f"unix:{SERVER_SOCKET}"
            process=subprocess.Popen(list(map(str,server)),stdout=log,stderr=log)
            wait_ready(lambda: port_up(7600) and (system=="fusee-server" or SERVER_SOCKET.exists()))
        if system=="hatchet":
            # Docker publishes the port before the default tenant has been
            # provisioned. The CLI needs that tenant to issue a worker token.
            wait_ready(lambda: subprocess.run([*compose,"exec","-T","postgres","psql","-U","postgres","-d","hatchet","-At",
                "-c",'select id from "Tenant" where slug = $$default$$'],capture_output=True).stdout.strip())
        yield process
    finally:
        if process:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if containers:
            cmd([*compose,"stop",*containers], stdout=log,stderr=log)


def run_engine(system,trial,dest,sizes=SIZES):
    env = dict(os.environ,DBOS_DATABASE_URL="postgres://postgres:bench@127.0.0.1:5432/dbos",
               ABSURD_DATABASE_URL="postgres://postgres:bench@127.0.0.1:5432/absurd")
    # cloud-init runs this process without a login shell. Supply the actual
    # account home required by the official Inngest CLI.
    env.setdefault("HOME",pwd.getpwuid(os.getuid()).pw_dir)
    with (dest/f"{system}-{trial}.log").open("w") as log, service(system,env,log):
        if system.startswith("fusee-"):
            commands=[[env["FUSEE_BENCH"],"run","chain","--mode",system.split("-")[1],
                       "--dir",str(DATA/"chains"),"--trials","1","--shards","8"]]
            if "FUSEE_SERVER_ADDRESS" in env:
                commands[0]+=["--server",env["FUSEE_SERVER_ADDRESS"]]
        else:
            engine=system.split("-")[0]
            if system=="dbos-sqlite":
                cli=["/opt/fusee-python/bin/python",BASE/"dbos/sqlite.py",DATA/"dbos.sqlite"]
            elif system=="hatchet":
                cli=[BASE/"hatchet/run.sh"]
            elif system=="obelisk":
                cli=["python3",BASE/"obelisk/chain.py"]
            else:
                cli=["node",BASE/engine/"chain.mjs"]
            commands=[[*cli,size,2] for size in sizes]
        for args in commands:
            idx=0 if system.startswith("fusee-") else SIZES.index(int(args[-2]))
            result = cmd(args,env=env,stdout=subprocess.PIPE,stderr=log,timeout=2700 if system=="hatchet" else 1200,text=True)
            docs=[]
            for line in result.stdout.splitlines():
                if line.startswith("{"):
                    try:
                        obj=json.loads(line)
                    except ValueError:
                        continue
                    if obj.get("workload")=="chain" and isinstance(obj.get("result"),list):
                        docs.append(obj)
            if len(docs)!=1:
                log.write(result.stdout)
                raise ValueError(f"expected one chain observation from {system}")
            obj=docs[0]
            if not obj["result"] and not (system.startswith("inngest") and obj.get("outcome")=="unsupported"
                    and obj.get("steps")==int(args[-2]) and obj.get("server_response",{}).get("output",{}).get("error",{}).get("name")=="InngestErrFunctionOverflowed"):
                raise ValueError(f"no completed measurements or supported engine limitation from {system}")
            for row in obj["result"]:
                n=row["steps"]
                if row["workflow"]["count"]!=2 or row["step_to_step"]["count"]!=2*(n-1):
                    raise ValueError(f"unexpected sample population from {system}: {n}")
            obj.update(trial=trial, system=system, instance_type=env.get("FUSEE_INSTANCE_TYPE"),
                       zone=env.get("FUSEE_ZONE"),source=env.get("FUSEE_REVISION"))
            (dest/f"{system}-{trial}-{idx}.json").write_text(json.dumps(obj)+"\n")


def main():
    args=argparse.ArgumentParser(description=__doc__)
    args.add_argument("output",type=Path)
    args.add_argument("--systems",default=",".join(SYSTEMS))
    args.add_argument("--trials",type=int,default=5)
    opts=args.parse_args()
    systems=opts.systems.split(",")
    if set(systems)-set(SYSTEMS+OPTIONAL) or not 1<=opts.trials<=5:
        args.error("invalid engines or trials")
    selected=json.loads(os.environ["FUSEE_MATRIX_CELLS"]) if os.environ.get("FUSEE_MATRIX_CELLS") else None
    if selected is not None:
        if not isinstance(selected,dict) or set(selected)-set(SYSTEMS+OPTIONAL):
            args.error("unknown system in requested cells")
        for trials in selected.values():
            for trial,sizes in trials.items():
                if int(trial) not in range(5) or not sizes or set(sizes)-set(SIZES):
                    args.error("invalid trial or chain length")
    failures=[]
    for trial in range(opts.trials):
        offset=(trial*((len(systems)+1)//2))%len(systems)
        for system in systems[offset:]+systems[:offset]:
            sizes=SIZES if selected is None else selected.get(system,{}).get(str(trial),[])
            if not sizes:continue
            print(f"trial {trial}, {system}, lengths {list(sizes)}",flush=True)
            try:
                run_engine(system,trial,opts.output,sizes)
            except (OSError,RuntimeError,subprocess.SubprocessError,ValueError) as err:
                failures.append({"system":system,"trial":trial,"error":str(err)})
                print(f"{system} failed: {err}",flush=True)
                (opts.output/"failures.json").write_text(json.dumps(failures,indent=2))
    if failures:
        raise SystemExit(f"{len(failures)} failed engine trials; inspect individual logs")


if __name__ == "__main__":
    main()
