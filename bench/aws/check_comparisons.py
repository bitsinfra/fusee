"""Validate raw chain populations and identify incomplete or unsupported matrix cells."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import tarfile

SYSTEMS=("fusee-local","fusee-server","temporal","temporal-tuned","temporal-local","restate",
         "dbos-postgres","dbos-sqlite","inngest","inngest-buffered","hatchet","absurd","obelisk")
SIZES=(1,10,100,1000)
EMPTY_CHAIN_SOURCE="32c5727552e6a4c3f8731c5ac1bba3a93affdbd8d05455c41533a46d7350e404"


def percentile(sorted_samples, fraction):
    """Interpolate between adjacent measurements. A one-step chain has no gaps."""
    if not sorted_samples:
        return 0
    position = fraction * (len(sorted_samples) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_samples) - 1)
    return sorted_samples[lower] + (sorted_samples[upper] - sorted_samples[lower]) * (position - lower)


def timing_summary(row):
    steps = row["steps"]
    histogram = row["step_to_step"]
    count = row["workflow"]["count"]
    assert count == 2 and histogram["count"] == count * (steps - 1)
    if "transition_samples_us" in row:
        runs = row["transition_samples_us"]
        whole = row["workflow_samples_us"]
    elif "observations" in row:
        observations = row["observations"]
        assert len(observations) == 2 * count and all(len(o["transition_samples_ns"]) == steps - 1 for o in observations)
        measured = [o for o in observations if not o["first_execution"]]
        runs = [[ns / 1000 for ns in o["transition_samples_ns"]] for o in measured]
        whole = [o["workflow_ns"] / 1000 for o in measured]
    else:
        # Without raw samples: the driver's whole-microsecond histogram.
        assert sum(n for _, n in histogram["histogram_us"]) == histogram["count"]
        return dict(p50_us=histogram["p50_us"], p99_us=histogram["p99_us"],
                    whole_p50_us=row["workflow"]["p50_us"], percentile_source="driver-summary")
    assert len(whole) == len(runs) == count and all(len(run) == steps - 1 for run in runs)
    gaps = sorted(sample for run in runs for sample in run)
    return dict(p50_us=percentile(gaps, .5), p99_us=percentile(gaps, .99),
                whole_p50_us=percentile(sorted(whole), .5), percentile_source="raw-samples")


def collect(directories, instance="i7i.2xlarge", disk="instance"):
    cells={}
    attempts=[]
    for path in sorted(directories):
        m=json.loads((path/"run.json").read_text())
        if m.get("case")!="comparisons" or m.get("type")!=instance or m.get("disk")!=disk:continue
        assert m["instance_terminated"] and m["bucket_removed"]
        file=path/"results.tgz"
        if not file.exists():
            file=path/"recovered-results.tgz"
            recovery=json.loads((path/"recovery.json").read_text())
            assert recovery["verified"] and recovery["reader_terminated"] and recovery["fallback_volume_removed"]
            assert recovery["guest_sha256"]==hashlib.sha256(file.read_bytes()).hexdigest()
        with tarfile.open(file) as archive:
            def get(name):return archive.extractfile("./"+name)
            exit_code=int(get("exit-code").read())
            assert get("postgres-settings.txt").read().split()==[b"on",b"on",b"on"]
            assert get("images.txt").read().strip()
            for f in archive:
                match=re.fullmatch(r"./([a-z-]+)-(\d+)-(\d+)\.json",f.name)
                if not match:continue
                system,trial,index=match[1],int(match[2]),int(match[3])
                doc=json.load(archive.extractfile(f))
                assert doc["system"]==system and doc["trial"]==trial and system in SYSTEMS and 0<=trial<5
                rows=doc["result"]
                if not rows:
                    assert doc["outcome"]=="unsupported" and doc["steps"]==SIZES[index]
                    rows=[{"steps":doc["steps"],"reason":doc["reason"]}]
                for row in rows:
                    n=row["steps"];assert n in SIZES
                    attempt=dict(system=system,trial=trial,steps=n,run=m["run"],archive=file.name,
                                 member=f.name,zone=m["zone"],instance_type=m["type"],storage=m["disk"],guest_exit=exit_code,source=m["source_sha256"])
                    if "reason" in row:
                        attempt.update(status="unsupported",reason=row["reason"])
                    else:
                        protocol="measured"
                        if system.startswith("fusee-") and m["source_sha256"]==EMPTY_CHAIN_SOURCE:
                            protocol="serial-index-only"
                        if system=="dbos-sqlite" and row.get("transition_boundary")!="body-end-to-next-body-start":
                            protocol="body-start-to-start"
                        if system.startswith("inngest") and "handler_return_us" not in row:
                            protocol="status-api-only"
                        attempt.update(status=protocol, **timing_summary(row))
                    attempts.append(attempt)
                    cells.setdefault((system,trial,n),[]).append(attempt)
    summary={}
    for system in SYSTEMS:
        expected=[(system,t,n) for t in range(5) for n in SIZES]
        def status(key):
            seen=cells.get(key,[])
            return next((s for s in ("measured","unsupported","status-api-only","serial-index-only","body-start-to-start") if any(a["status"]==s for a in seen)),"missing")
        summary[system]={s:[[t,n] for _,t,n in expected if status((system,t,n))==s] for s in ("measured","unsupported","status-api-only","serial-index-only","body-start-to-start","missing")}
    return dict(instance_type=instance,storage=disk,attempts=attempts,coverage=summary)


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("directories",nargs="+",type=Path)
    p.add_argument("--instance",default="i7i.2xlarge");p.add_argument("--disk",default="instance")
    args=p.parse_args()
    print(json.dumps(collect(args.directories,args.instance,args.disk),indent=2))
