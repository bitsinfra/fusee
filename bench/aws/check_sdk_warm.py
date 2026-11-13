"""Validate first and second executions of each workflow per size, shard count and population."""
import argparse
import json
import hashlib
import math
from pathlib import Path
import tarfile


def percentile(values, q):
    ordered=sorted(values)
    return ordered[math.floor((len(values)-1)*q+.5)]


def summarize(folder):
    manifest=json.loads((folder/"run.json").read_text())
    assert manifest["case"]=="sdk" and manifest["instance_terminated"] and manifest["bucket_removed"]
    data=[]
    with tarfile.open(folder/"results.tgz") as archive:
        assert archive.extractfile("./exit-code").read().strip()==b"0"
        engines=["rust","node","python"]
        if manifest.get("sdk_paired"):
            engines.append("node-control")
            entries=archive.extractfile("./sdk-pair.sha256").read().decode().splitlines()
            candidate=archive.extractfile("./index.js").read()
            control=archive.extractfile("./gate-control.js").read()
            gate=b"this.e.native.stepGate(shard, run, gate, notBefore, true)"
            assert candidate.count(gate)==1 and candidate.replace(gate,gate.replace(b"true",b"false"))==control
            assert [row.split()[0] for row in entries[:2]]==[hashlib.sha256(b).hexdigest() for b in (candidate,control)]
            assert len(entries)==3 and len(entries[2].split()[0])==64
        def rows(name):
            return [json.loads(line) for line in archive.extractfile("./"+name+".jsonl")]
        for shards in (1,8):
            for trial in range(5):
                disk=rows(f"floor-{shards}-{trial}")[0]["result"]["disk"]
                floor=next(r for r in disk if r["method"]=="fdatasync" and r["bytes"]==4096)
                assert floor["samples"]==20000
                for runs in ([2,20] if trial==0 else [2]):
                    for engine in engines:
                        docs=rows(f"{engine}-{shards}-{runs}-{trial}")
                        sizes=[row for doc in docs for row in doc["result"]]
                        assert [row["steps"] for row in sizes]==[1,10,100,1000]
                        for row in sizes:
                            obs=row["observations"]
                            cold=[v for v in obs if v["first_execution"]]
                            warm=[v for v in obs if not v["first_execution"]]
                            assert len(warm)==len(cold)==len(set(v["id"] for v in warm))==runs
                            assert [v["id"] for v in warm]==[v["id"] for v in cold]
                            unit="ns" if engine=="rust" else "us"
                            key="transition_samples_"+unit
                            assert all(len(v[key])==row["steps"]-1 for v in obs)
                            assert row["workflow"]["count"]==runs and row["step_to_step"]["count"]==runs*(row["steps"]-1)
                        def samples(pop):
                            return [x/(1000 if unit=="ns" else 1) for o in pop for x in o[key]]
                        c,w=samples(cold),samples(warm)
                        row=dict(run=folder.name,shards=shards,engine=engine,trial=trial,workflows=runs,
                                 floor_p50_us=floor["p50_us"],floor_p99_us=floor["p99_us"],
                                 first_p50_us=percentile(c,.50),first_p99_us=percentile(c,.99),
                                 warm_p50_us=percentile(w,.50),warm_p99_us=percentile(w,.99),
                                 first_over_1ms=sum(v>1000 for v in c),warm_over_1ms=sum(v>1000 for v in w),
                                 samples_per_phase=len(c))
                        data.append(row)
    return data


if __name__=="__main__":
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("folder",type=Path)
    for row in summarize(cli.parse_args().folder):print(json.dumps(row))
