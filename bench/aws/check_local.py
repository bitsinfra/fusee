"""Recompute a single-host Fusee summary from its results archive."""
import argparse
import json
from pathlib import Path
import tarfile


def summarize(folder):
    manifest=json.loads((folder/"run.json").read_text())
    if not manifest["instance_terminated"] or not manifest["bucket_removed"] or manifest["case"]!="local":
        raise ValueError("wrong case or incomplete cleanup")
    with tarfile.open(folder/"results.tgz") as archive:
        if archive.extractfile("./exit-code").read().strip()!=b"0":
            raise ValueError("guest failed")
        def data(name):
            return json.load(archive.extractfile("./"+name))
        p50, p99, p999, peak = [], [], [], []
        for trial in range(5):
            row = data(f"chain-{trial}.jsonl")["result"][-1]
            h=row["step_to_step"]
            if row["steps"]!=1000 or h["count"]!=row["workflow"]["count"]*999:
                raise ValueError("unexpected chain population")
            p50.append(h["p50_us"])
            p99.append(h["p99_us"])
            p999.append(h["p999_us"])
            peak.append(h["max_us"])
        sweep=data("max.jsonl")["result"]
        def passes(r):
            offered = r["offered"]
            return (offered>0 and offered==r["completed"] and r["start_to_first_step"]["count"]==offered
                    and r["step_to_step"]["count"]==offered*(r["steps_per_workflow"]-1)
                    and r["step_to_step"]["p99_us"]<=1000 and
                    r["achieved_steps_per_s"]>=.99*r["offered_starts_per_s"]*r["steps_per_workflow"])
        best = next((r for r in reversed(sweep["trials"]) if passes(r)),None)
        if best!=sweep["best"]:
            raise ValueError("reported throughput gate does not match the trial rows")
        return {"instance":manifest["type"],"run":folder.name,"chain_1000_trial_p50_us":p50,
                "chain_1000_trial_p99_us":p99,"chain_1000_trial_p999_us":p999,"chain_1000_trial_max_us":peak,
                "offered_starts_per_s":best["offered_starts_per_s"] if best else None,
                "completed_starts":best["completed"] if best else None,
                "achieved_steps_per_s":best["achieved_steps_per_s"] if best else None,
                **{f"transition_{q}_us":best["step_to_step"][f"{q}_us"] if best else None for q in ("p50","p99","p999","max")},
                **{f"next_sweep_{q}_us":sweep["trials"][-1]["step_to_step"][f"{q}_us"] for q in ("p50","p99","p999","max")},
                "crash_summary":archive.extractfile("./crash.log").read().decode().splitlines()[-1]}


if __name__=="__main__":
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("folders", type=Path, nargs="+")
    for folder in cli.parse_args().folders:
        print(json.dumps(summarize(folder)))
