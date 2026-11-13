"""Validate an `sdk` run (bench/aws/sdk.sh) and summarize each SDK cell.

    python3 check_sdk.py <run directory>...

Each directory holds the controller's run.json and the guest's results.tgz.
Prints one JSON object per run: the chain cells (Rust, Node and Python
1,000-step chains, per shard count and trial), the floor (the log's
O_DIRECT + RWF_DSYNC write from `bench floors`), and the SDK cells:

- sdk_checks: exit codes of the cross-language and SDK failover checks;
- node_step: Node step p50 with eight live workflows (one chain with seven
  waiting, and eight chains at once), per variant, and its distance from
  the floor;
- python_step: Python step p50 (1,000-step chains), and its distance from
  the floor;
- node_chain: Node 1,000-step chains, inline and pipelined flush;
- node_fanout: whole fan-out of 100 (p50);
- node_workers: the highest rate per worker thread with step p99 <= 1 ms on
  the real disk, and how it scales with threads;
- wakes: simultaneous wakes on one Node thread (lateness, admission, RSS);
- python_workers: Python worker processes of one server (rate per worker);
- flush_gap: the gap between an inline and a pipelined flush.
"""
import argparse
import json
import re
import statistics
import tarfile
from pathlib import Path


def lines(t, name):
    f = t.extractfile(name)
    return [json.loads(x) for x in f if x.strip()] if f else []


def median(v):
    return statistics.median(v) if v else None


def chain_cells(t, names):
    out = {}
    for name in names:
        m = re.fullmatch(r"\./(rust|node|python)-(1|8)-(\d+)-(\d+)\.jsonl", name)
        if not m:
            continue
        engine, shards, runs, trial = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
        rows = [row for obj in lines(t, name) for row in obj["result"]]
        if [r["steps"] for r in rows] != [1, 10, 100, 1000]:
            raise ValueError(f"{name}: missing chain size")
        for row in rows:
            if row["workflow"]["count"] != runs or row["step_to_step"]["count"] != runs * (row["steps"] - 1):
                raise ValueError(f"{name}: bad sample population")
        h = rows[-1]["step_to_step"]
        out.setdefault(f"{engine}-{shards}-{runs}", []).append(
            {"trial": trial, "p50_us": h["p50_us"], "p99_us": h["p99_us"], "p999_us": h.get("p999_us"), "max_us": h.get("max_us")})
    return out


def floor_us(t, names):
    """The log's write from every `bench floors` of the run: the median p50."""
    found = {}
    for name in names:
        if re.fullmatch(r"\./floor-.*\.jsonl", name):
            for d in lines(t, name):
                for row in d["result"]["disk"]:
                    if row["bytes"] == 4096:
                        found.setdefault(row["method"], []).append(row["p50_us"])
    # Some floor probes report the RWF_DSYNC write alone.
    for method in ("O_DIRECT + RWF_DSYNC write", "RWF_DSYNC write"):
        if found.get(method):
            return median(found[method]), {"method": method, "p50_us": found[method]}
    raise ValueError("no RWF_DSYNC floor in the run")


def summarize(directory):
    manifest = json.loads((directory / "run.json").read_text())
    if not manifest["instance_terminated"] or not manifest["bucket_removed"]:
        raise ValueError("incomplete cleanup")
    with tarfile.open(directory / "results.tgz") as t:
        if t.extractfile("./exit-code").read().strip() != b"0":
            raise ValueError("guest failed")
        names = t.getnames()
        floor, floors = floor_us(t, names)
        cells = chain_cells(t, names)
        sdk = {}
        # The cross-language and failover checks that run first (sdk.sh):
        # their exit codes, 0 when they passed.
        checks = {}
        for check in ("cross-language", "sdk-failover"):
            name = f"./{check}.exit"
            if name in names:
                checks[check] = int(t.extractfile(name).read().strip() or b"1")
        sdk["sdk_checks"] = {"exit": checks, "passed": all(v == 0 for v in checks.values())}

        # Node step with eight live workflows, per shape and variant.
        live = {}
        for name in names:
            m = re.fullmatch(r"\./sdk-live-(\d+)-(\d+)-([a-z]+)-(\d+)\.jsonl", name)
            if m:
                key = f"{m.group(1)}+{m.group(2)} {m.group(3)}"
                live.setdefault(key, []).extend(r["step_to_step"]["p50_us"] for r in lines(t, name))
        sdk["node_step"] = {k: {"p50_us": median(v), "over_floor_us": median(v) - floor, "repeats": len(v)}
                            for k, v in sorted(live.items())}

        # Python step: the default 1,000-step chains on 8 shards, and the pipelined ones.
        py = {}
        for name in names:
            m = re.fullmatch(r"\./sdk-python-([a-z]+)-(\d+)\.jsonl", name)
            if m:
                py.setdefault(m.group(1), []).extend(r["step_to_step"]["p50_us"] for d in lines(t, name) for r in d["result"])
        py["chains"] = [c["p50_us"] for c in cells.get("python-8-2", [])]
        sdk["python_step"] = {k: {"p50_us": median(v), "over_floor_us": median(v) - floor} for k, v in py.items() if v}
        nd = {}
        for name in names:
            m = re.fullmatch(r"\./sdk-node-([a-z]+)-(\d+)\.jsonl", name)
            if m:
                nd.setdefault(m.group(1), []).extend(r["step_to_step"]["p50_us"] for d in lines(t, name) for r in d["result"])
        sdk["node_chain"] = {k: {"p50_us": median(v), "over_floor_us": median(v) - floor} for k, v in nd.items() if v}

        # Fan-out of 100.
        fan = {}
        for name in names:
            m = re.fullmatch(r"\./sdk-fanout-([a-z]+)-(\d+)\.jsonl", name)
            if m:
                fan.setdefault(m.group(1), []).extend(r["workflow"]["p50_us"] for r in lines(t, name))
        sdk["node_fanout"] = {k: {"p50_us": median(v), "trials": len(v)} for k, v in fan.items()}

        # Worker threads on the disk: the best rate per thread whose p99 holds.
        rows = lines(t, "./workers-local.jsonl") if "./workers-local.jsonl" in names else []
        best = {}
        for r in rows:
            if r["workers"] and r["gap_p99_us"] <= 1000:
                w = r["workers"]
                if r["rate_per_thread"] > best.get(w, {}).get("rate_per_thread", 0):
                    best[w] = {"rate_per_thread": r["rate_per_thread"], "rate": r["rate"],
                               "chains_per_thread": r["chains_per_thread"], "gap_p99_us": r["gap_p99_us"]}
        one = best.get(1, {}).get("rate")
        for w, b in best.items():
            b["scale_over_one"] = b["rate"] / one if one else None
        sdk["node_workers"] = {"best_within_p99_1ms": best,
                               "rate_rises_with_threads": bool(best) and all(best[w]["rate"] > best[v]["rate"]
                                                                             for v, w in zip(sorted(best), sorted(best)[1:]))}

        # Simultaneous wakes on one Node thread.
        wakes = {}
        for name in names:
            m = re.fullmatch(r"\./wakes-(\d+)\.jsonl", name)
            if m:
                for d in lines(t, name):
                    wakes[int(m.group(1))] = {"lateness_ms": d["lateness_ms"], "admission_overran": d["admission_overran_deadline"],
                                              "missing": d["missing"], "rss_bytes_parked": d["rss_bytes_parked"]}
        sdk["wakes"] = wakes

        sdk["python_workers"] = lines(t, "./pyworkers.jsonl") if "./pyworkers.jsonl" in names else []
        sdk["flush_gap"] = lines(t, "./flush-gap.jsonl") if "./flush-gap.jsonl" in names else []
    return {"instance": manifest["type"], "run": directory.name, "floor_us": floor, "floors_us": floors,
            "chains": cells, "sdk": sdk}


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    cli.add_argument("directories", type=Path, nargs="+")
    for directory in cli.parse_args().directories:
        print(json.dumps(summarize(directory)))
