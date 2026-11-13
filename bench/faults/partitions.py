"""Partition three real replica processes on a private Docker network."""
import argparse
import collections
import json
import os
import queue
import subprocess
import threading
import time
import uuid


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflows", type=int, default=1000)
    parser.add_argument("--subnet",help="unused CIDR for the isolated test network when Docker's default pool is full")
    parser.add_argument("--image", default="fusee-linux-faults:0.3.1")
    parser.add_argument("--build-volume", default="fusee-validation-target")
    parser.add_argument("--permits",type=int,default=0,help="exercise a named quorum-wide concurrency limit")
    args = parser.parse_args()
    if args.permits<0:parser.error("permits must be nonnegative")
    prefix = "fusee-fault-" + uuid.uuid4().hex[:10]
    nodes = [f"{prefix}-{i}" for i in range(3)]
    peers = ",".join(f"{name}:7400" for name in nodes)
    events = queue.Queue()
    processes = []
    acknowledgments = set()
    results = {}
    elections = []
    done = set()
    effects = set()
    def skew(node, delta):
        docker("exec",node,"sh","-c",'printf "%s\\n" "$1" >/tmp/clock.next && mv /tmp/clock.next /tmp/clock',"sh",delta)
    def watch(i, process):
        for line in process.stdout:
            events.put((i, line.strip()))
    def until(predicate, seconds):
        deadline = time.monotonic() + seconds
        while not predicate():
            if time.monotonic() > deadline:
                raise RuntimeError(f"deadline: elections={elections}, acks={len(acknowledgments)}, results={len(results)}")
            try:
                i, line = events.get(timeout=.2)
            except queue.Empty:
                if any(p.poll() is not None for p in processes):
                    raise RuntimeError("replica exited unexpectedly")
                continue
            parts = line.split()
            if not parts:
                continue
            if parts[0] == "ACK":
                acknowledgments.add(parts[1])
            elif parts[0] == "RESULT":
                old = results.setdefault(parts[1], parts[2])
                if old != parts[2]:
                    raise RuntimeError(f"{parts[1]} returned both {old} and {parts[2]}")
            elif parts[0] == "PRIMARY":
                elections.append(i)
            elif parts[0]=="EFFECT":
                effects.add(i)
            elif parts[0] == "DONE":
                done.add(i)
    addressing=["--subnet",args.subnet] if args.subnet else []
    docker("network","create","--internal",*addressing,prefix)
    try:
        docker("volume","create",prefix)
        for i, name in enumerate(nodes):
            proc = subprocess.Popen(["docker","run","--name",name,"--network",prefix,"--cap-add","NET_ADMIN",
                "-v",f"{prefix}:/data","-v",f"{args.build_volume}:/build:ro",args.image,
                "bash","-c",'set -eu; echo "+0s" >/tmp/clock; export LD_PRELOAD=$(find /usr/lib -name libfaketime.so.1 -print -quit); export FAKETIME_TIMESTAMP_FILE=/tmp/clock FAKETIME_NO_CACHE=1 FAKETIME_DONT_FAKE_MONOTONIC=1; exec "$@"',"bash",
                "/build/release/fusee-crashtest","replica",str(i),peers,f"/data/r{i}","/data/oracle",str(args.workflows),str(args.permits)],
                stdout=subprocess.PIPE,text=True)
            processes.append(proc)
            threading.Thread(target=watch,args=(i,proc),daemon=True).start()
        until(lambda: elections and acknowledgments and (not args.permits or elections[-1] in effects), 60)
        primary = elections[-1]
        effects.clear()
        for chain in ("OUTPUT", "INPUT"):
            docker("exec",nodes[primary],"iptables","-I",chain,"-p","tcp","--dport","7400","-j","DROP")
        until(lambda: elections[-1] != primary, 30)
        if args.permits:
            # A ready election can be displaced before it actually executes a
            # leased effect. Hold the cut until the surviving side serves one.
            until(lambda: elections[-1] != primary and elections[-1] in effects, 30)
        successor = elections[-1]
        skew(nodes[primary],"-90s")
        skew(nodes[successor],"+90s")
        for chain in ("OUTPUT", "INPUT"):
            docker("exec",nodes[primary],"iptables","-D",chain,"-p","tcp","--dport","7400","-j","DROP")
        docker("exec",nodes[primary],"tc","qdisc","add","dev","eth0","root","netem","delay","20ms","5ms","loss","2%","duplicate","1%")
        time.sleep(1)
        docker("exec",nodes[primary],"tc","qdisc","del","dev","eth0","root")
        for name in nodes:
            skew(name,"+0s")
        until(lambda: len(results) == args.workflows and done, 120)
        if len(acknowledgments) != args.workflows or not acknowledgments.issubset(results):
            raise RuntimeError("acknowledged workflow missing")
        calls = collections.defaultdict(set)
        for line in docker("exec",nodes[successor],"cat","/data/oracle").splitlines():
            key, value = line.split()
            calls[key].add(value)
        if len(calls) != args.workflows*3 or any(len(values) != 1 for values in calls.values()):
            raise RuntimeError("contradictory or missing external calls")
        lease_report={}
        if args.permits:
            lines=docker("exec",nodes[successor],"cat","/data/oracle.leases").splitlines()
            active=collections.Counter()
            maximum=0
            owners=set()
            for line in lines:
                phase,owner,key=line.split()
                owners.add(int(owner))
                effect=(owner,key)
                if phase=="begin":active[effect]+=1
                elif phase=="end" and active[effect]>0:active[effect]-=1
                else:raise RuntimeError(f"malformed lease trace: {line}")
                maximum=max(maximum,sum(active.values()))
            if sum(active.values()) or maximum>args.permits or {primary,successor}-owners:
                raise RuntimeError(f"unexpected lease trace: active={+active}, max={maximum}, owners={owners}, "
                                   f"primary={primary}, successor={successor}, elections={elections}, events={len(lines)}")
            lease_report={"named_limit":args.permits,"observed_external_concurrency":maximum,"effect_events":len(lines),"effect_nodes":sorted(owners)}
        checkpoints=[]
        deadline=time.monotonic()+15
        for i in range(3):
            path=f"/data/r{i}/vsr/current"
            while subprocess.run(["docker","exec",nodes[i],"test","-f",path]).returncode:
                if time.monotonic()>deadline:raise RuntimeError(f"replica {i} did not checkpoint")
                time.sleep(.1)
            pointer=subprocess.check_output(["docker","exec",nodes[i],"cat",path])
            assert pointer[:8]==b"FSCUR001"
            checkpoint=json.loads(pointer[12:])["image"]
            checkpoints.append(dict(member=i,base=checkpoint["op"],lsn=checkpoint["lsn"],bytes=checkpoint["len"]))
        print(json.dumps({"checkpoints":checkpoints,**lease_report,"partitioned_primary":primary,"successor":successor,"elections":elections,
            "clock_offsets_seconds":[-90,90],"netem":"20ms +/-5ms, 2% loss, 1% duplicate",
            "acknowledged":len(acknowledgments),"completed":len(results),"external_keys":len(calls)}))
    finally:
        for name in nodes:
            subprocess.run(["docker","rm","-f",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        for p in processes:
            p.wait()
        subprocess.run(["docker","volume","rm",prefix],check=False,stdout=subprocess.DEVNULL)
        docker("network","rm",prefix)


if __name__ == "__main__":
    main()
