"""Compute per-completion CPU and replication bytes from all three real processes."""
import argparse
import json
import math
from pathlib import Path
import tarfile


def timing_deltas(before, after):
    """Cumulative stage counters, with quantiles reported as bucket upper bounds."""
    stages = {}
    for name, end in after.items():
        start = before[name]
        buckets = []
        for (limit_a, count_a), (limit_b, count_b) in zip(start["histogram_ns"], end["histogram_ns"], strict=True):
            assert limit_a == limit_b and count_b >= count_a
            buckets.append((limit_b, count_b - count_a))
        n = sum(count for _, count in buckets)
        elapsed = end["total_ns"] - start["total_ns"]
        assert elapsed >= 0 and (n != 0 or elapsed == 0)
        def upper(q):
            seen = 0
            for limit, count in buckets:
                seen += count
                if seen >= math.ceil(n * q):
                    return min(limit, end["max_ns"]) / 1000
        stages[name] = dict(samples=n, mean_us=elapsed / (1000*n) if n else None,
                            p50_upper_us=upper(.5) if n else None, p99_upper_us=upper(.99) if n else None)
    return stages


def summarize(directory):
    manifest=json.loads((directory/"run.json").read_text())
    assert manifest["bucket_removed"] and all(n.get("terminated") and n.get("exit_code")==0 for n in manifest["nodes"])
    trials={}
    for i in range(3):
        with tarfile.open(directory/f"node-{i}.tgz") as t:
            assert t.extractfile("./exit-code").read().strip()==b"0"
            for mode in ("open","chain"):
                for trial in range(1,6):
                    label=f"{mode}-{trial}"
                    out=trials.setdefault(label,{"nodes":[]})
                    observation=json.load(t.extractfile(f"./{label}-peer-{i}.json"))
                    before,after=observation["before"],observation["after"]
                    assert not after["stopped"] and after["applied"]==after["commit"]
                    diff={k:after[k]-before[k] for k in ("sent_bytes","received_bytes","user_us","system_us","commit")}
                    assert min(diff.values())>=0
                    if "timings" in after:
                        diff["timings"] = timing_deltas(before["timings"], after["timings"])
                    out["nodes"].append(dict(node=i,**diff))
                    f=t.getmember("./"+label+".jsonl")
                    if f.size:
                        doc=json.load(t.extractfile(f));r=doc["result"]
                        assert doc["primary_id"]==i and "primary" not in out
                        out["primary"]=i
                        if mode=="open":
                            assert r["offered"]==r["completed"]
                            out["workflows"]=r["completed"]
                            out["logical_commits"]=r["completed"]*(r["steps_per_workflow"]+1)
                        else:
                            out["workflows"]=sum(len(row["observations"]) for row in r)
                            out["logical_commits"]=sum(len(row["observations"])*(row["steps"]+1) for row in r)
    for label,out in sorted(trials.items()):
        nodes=out["nodes"];commits=out["logical_commits"]
        total_cpu=sum(n["user_us"]+n["system_us"] for n in nodes)
        sent=sum(n["sent_bytes"] for n in nodes);received=sum(n["received_bytes"] for n in nodes)
        assert len(nodes)==3 and commits>0 and sent>0 and received>0 and min(n["commit"] for n in nodes)>0
        yield dict(run=directory.name,trial=label,**out,cpu_us_per_logical_commit=total_cpu/commits,
                   sent_bytes_per_logical_commit=sent/commits,
                   endpoint_bytes_per_logical_commit=(sent+received)/commits)


if __name__=="__main__":
    cli=argparse.ArgumentParser(description=__doc__);cli.add_argument("directory",type=Path)
    for row in summarize(cli.parse_args().directory):print(json.dumps(row))
