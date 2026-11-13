"""Validate all three guests, six network directions and the replicated trials.

For every closed-loop (chain) trial it also reports where a step's time went:
the primary's critical-path stages and each backup's, from the members'
stage counters (`docs/replication.md`, Where a step's time goes). With the
`saturate` workload it reports each trial's peak and the highest rate whose
step p99 stayed within 1 ms; with `failover`, the primary kills timed across
the three hosts' logs.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import tarfile

PRIMARY_STAGES = ["shard_stage_to_take", "take_to_prepare_sent", "prepare_sent_to_ack_read",
                  "ack_read_to_release", "release_to_shard_resume", "shard_resume_to_stage"]
BACKUP_STAGES = ["prepare_read_to_write", "protocol_log_append", "protocol_log_sync",
                 "durable_to_ack_sent", "prepare_read_to_ack_sent"]


def stage_summary(before, after):
    """Count, mean and bucket bounds of p50/p99 (µs) of each stage between two snapshots."""
    out = {}
    for name, a in after.items():
        b = before.get(name, {"total_ns": 0, "histogram_ns": []})
        was = {limit: n for limit, n in b["histogram_ns"]}
        hist = [(limit, n - was.get(limit, 0)) for limit, n in a["histogram_ns"]]
        count = sum(n for _, n in hist)
        if not count:
            continue

        def bound(q):
            need, seen = max(1, -(-int(q * count * 1000) // 1000)), 0
            for limit, n in hist:
                seen += n
                if seen >= need:
                    return None if limit >= 2**63 else limit / 1000
            return None
        out[name] = {"count": count, "mean_us": round((a["total_ns"] - b["total_ns"]) / count / 1000, 2),
                     "p50_bound_us": bound(.5), "p99_bound_us": bound(.99)}
    return out


def quantiles(values):
    values = sorted(values)
    if not values:
        return None
    q = lambda p: values[min(len(values) - 1, round((len(values) - 1) * p))]
    return {"n": len(values), "p50_ms": round(q(.5), 1), "p90_ms": round(q(.9), 1),
            "p99_ms": round(q(.99), 1), "max_ms": round(values[-1], 1),
            "over_150_ms": sum(1 for v in values if v > 150)}


def failovers(logs):
    """Kills and the other members' READY/PRIMARY/EXEC/SACK lines (Unix µs), merged across hosts."""
    kills, events = [], []
    for host, text in logs.items():
        for line in text.splitlines():
            f = line.split()
            if len(f) == 3 and f[0] == "KILL":
                kills.append((int(f[2]), int(f[1])))
            elif len(f) == 3 and f[0] in ("READY", "PRIMARY", "EXEC", "SACK") and f[2].isdigit():
                events.append((int(f[2]), f[0], int(f[1])))
    kills.sort()
    events.sort()
    stages = {"READY": "ready", "PRIMARY": "engine", "EXEC": "workflow_code", "SACK": "new_commit"}
    order = list(stages)
    times = {v: [] for v in stages.values()}
    for k, (at, victim) in enumerate(kills):
        until = kills[k + 1][0] if k + 1 < len(kills) else float("inf")
        reached = 0
        for t, kind, member in events:
            if t < at or t >= until or member == victim or reached == len(order):
                continue
            if kind == order[reached]:
                times[stages[kind]].append((t - at) / 1000)
                reached += 1
    return {"kills": len(kills), **{k: quantiles(v) for k, v in times.items()}}


def summarize(directory):
    manifest=json.loads((directory/"run.json").read_text())
    if not manifest["bucket_removed"] or not manifest["security_group_removed"] or len(manifest["nodes"])!=3:
        raise ValueError("incomplete cluster")
    if not all(n.get("terminated") for n in manifest["nodes"]):
        raise ValueError("cluster has an unterminated peer")
    recovered=None
    if not all(n.get("exit_code")==0 for n in manifest["nodes"]):
        recovered=json.loads((directory/"recovery.json").read_text())
        if not (recovered["verified"] and recovered["reader_terminated"] and recovered["diagnostic_snapshots_removed"]):
            raise ValueError("unverified snapshot recovery")
    workloads = manifest.get("workloads", ["chain", "open"])
    floors, rtts, rows, peers, fail_logs, nic = {}, {}, {}, {}, {}, {}
    raw_count = 0
    for node in manifest["nodes"]:
        i = node["index"]
        guest=directory/f"node-{i}.tgz"
        if recovered and hashlib.sha256(guest.read_bytes()).hexdigest()!=recovered["recovered_sha256"][guest.name]:
            raise ValueError("recovered guest digest mismatch")
        with tarfile.open(guest) as archive:
            if archive.extractfile("./exit-code").read().strip()!=b"0":
                raise ValueError("guest exited unsuccessfully")
            for f in archive:
                name = Path(f.name).name
                if not f.isfile() or not f.size:
                    continue
                if name=="disk-check.jsonl":
                    disk=json.load(archive.extractfile(f))["result"]["disk"]
                    floors[i]=next(p["p50_us"] for p in disk if p["method"]=="fdatasync" and p["bytes"]==4096)
                elif m:=re.fullmatch(r"rtt-(\d+)-(\d+)-(\d+)\.csv",name):
                    trial, sender, recv = map(int,m.groups())
                    if sender!=i or sender==recv:
                        raise ValueError("unexpected network trace sender")
                    samples=[]
                    meta=[]
                    for line in archive.extractfile(f):
                        value=line.strip()
                        if value and value[:1].isdigit() and value.count(b",")==3:
                            samples.append(float(value.rsplit(b",",1)[1]))
                        elif value.startswith(b"test was performed") or b"observations" in value:
                            meta.append(value)
                    if not samples or not any(b"--full-rtt" in v for v in meta):
                        raise ValueError("missing full round-trip samples")
                    if len(samples)!=int(re.search(rb"Total (\d+) observations",b" ".join(meta))[1]):
                        raise ValueError("sockperf trace count differs from its summary")
                    samples.sort()
                    raw_count+=len(samples)
                    rtts[trial,sender,recv]=samples[len(samples)//2]
                elif m:=re.fullmatch(r"(chain|chain-rxoff|chain-busypoll|open|saturate)-(\d+)\.jsonl",name):
                    workload, trial=m[1],int(m[2])
                    row=json.load(archive.extractfile(f))
                    store=rows.setdefault(workload, {})
                    if trial in store or row["primary_id"]!=i or row["replicas"]!=3 or row["mode"]!="replicated":
                        raise ValueError("wrong or duplicate primary result")
                    if workload=="open":
                        result=row["result"]
                        if result["completed"]!=result["offered"] or result["step_to_step"]["count"]!=result["offered"]*(row["steps"]-1):
                            raise ValueError("missing open-loop arrivals or transitions")
                    store[trial]=row
                elif m:=re.fullmatch(r"(chain|chain-rxoff|chain-busypoll|open|saturate)-(\d+)-peer-(\d+)\.json",name):
                    snap=json.load(archive.extractfile(f))
                    peers[m[1],int(m[2]),int(m[3])]=stage_summary(snap["before"]["timings"],snap["after"]["timings"])
                elif m:=re.fullmatch(r"nic-(coalesce|rx-off)-(\d+)\.txt",name):
                    nic.setdefault(m[1],{})[int(m[2])]=archive.extractfile(f).read().decode(errors="replace")
                elif m:=re.fullmatch(r"failover-r(\d+)\.log",name):
                    fail_logs[int(m[1])]=archive.extractfile(f).read().decode(errors="replace")
    trials=set(range(1,6))
    if set(floors)!=set(range(3)) or len(rtts)!=30:
        raise ValueError("incomplete network or disk floors")
    for workload in ("chain", "open"):
        if workload in workloads and trials!=set(rows.get(workload, {})):
            raise ValueError(f"incomplete {workload} trial matrix")
    allowance=100 if manifest["topology"]=="cross-zone" else 30
    def closed_loop(workload):
      out=[]
      for trial,row in sorted(rows.get(workload, {}).items()):
          i=row["primary_id"]
          h=row["result"][-1]["step_to_step"]
          if row["result"][-1]["steps"]!=1000 or h["count"]!=999*row["result"][-1]["workflow"]["count"]:
              raise ValueError("unexpected closed-loop population")
          # A primary needs one of two backups. Charge the longer of the two
          # overlapping local flushes and the measured link round trip.
          estimates=[rtts[trial,i,j]+max(floors[i],floors[j])+allowance for j in floors if j!=i]
          # Where the time went: the primary's stages over the whole cell, and
          # each backup's (its counters cover the same cell).
          primary=row.get("replica_stages") or peers.get((workload,trial,i),{})
          backups={j:{k:peers[(workload,trial,j)][k] for k in BACKUP_STAGES if k in peers.get((workload,trial,j),{})}
                   for j in range(3) if j!=i}
          turnaround=[b["prepare_read_to_ack_sent"]["mean_us"] for b in backups.values() if "prepare_read_to_ack_sent" in b]
          network=None
          if "prepare_sent_to_ack_read" in primary and turnaround:
              network=round(primary["prepare_sent_to_ack_read"]["mean_us"]-min(turnaround),2)
          out.append({"trial":trial,"primary":i,"chain_p50_us":h["p50_us"],"chain_p99_us":h["p99_us"],
                      "chain_p999_us":h["p999_us"],"chain_max_us":h["max_us"],
                      "fast_quorum_estimate_p50_us":round(min(estimates),3),"slower_link_estimate_p50_us":round(max(estimates),3),
                      "within_fast_quorum_estimate":h["p50_us"]<=min(estimates),
                      "breakdown":{"primary_mean_us":{k:primary[k]["mean_us"] for k in PRIMARY_STAGES if k in primary},
                                   "backups":backups,
                                   "network_and_wakeups_mean_us":network,
                                   # The I/O thread's timer work and lock holds.
                                   "primary_io_thread":{k:primary[k] for k in ("io_thread_tick","io_thread_lock_hold") if k in primary}}})
      return out
    out=closed_loop("chain")
    saturation=[]
    for trial,row in sorted(rows.get("saturate", {}).items()):
        r=row["result"]
        best=r.get("highest_within_1ms") or {}
        before=r.get("highest_within_1ms_before_first_miss") or best
        saturation.append({"trial":trial,"primary":row["primary_id"],"stopped":r["stopped"],
                           "growth":r.get("growth",2.0),"past_first_miss":r.get("past_first_miss",0),
                           "first_miss_steps_per_s":r.get("first_miss_achieved_steps_per_s"),
                           "highest_within_1ms_before_first_miss_steps_per_s":before.get("achieved_steps_per_s"),
                           "peak_steps_per_s":round(r["peak_achieved_steps_per_s"]),
                           "highest_within_1ms_steps_per_s":best.get("achieved_steps_per_s"),
                           # A point that cost the primary its role ends the sweep
                           # (the harness keeps the points before it).
                           "points":[{"offered_starts_per_s":p["offered_starts_per_s"],"primary_changed":True}
                                     if p.get("primary_changed") else
                                     {"offered_starts_per_s":p["offered_starts_per_s"],
                                      "achieved_steps_per_s":round(p["achieved_steps_per_s"] or 0),
                                      "step_p50_us":p["step_to_step"]["p50_us"],"step_p99_us":p["step_to_step"]["p99_us"],
                                      "records_per_flush":p["records_per_flush"],
                                      "past_first_miss":p.get("past_first_miss",False)} for p in r["table"]]})
    result={"run":manifest["run"],"topology":manifest["topology"],"instance_type":manifest.get("type"),
            "busy_poll_us":manifest.get("busy_poll_us",0),"sockperf_samples":raw_count,
            "pair_trial_rtt_p50_us_range":[min(rtts.values()),max(rtts.values())],
            "node_flush_p50_us":floors,"closed_loop":out}
    if saturation:
        result["saturation"]=saturation
    if rows.get("chain-rxoff"):
        # Adaptive receive moderation off on every member.
        result["closed_loop_rx_off"]=closed_loop("chain-rxoff")
    if rows.get("chain-busypoll"):
        result["closed_loop_busy_poll"]=closed_loop("chain-busypoll")
    if nic:
        adaptive={i:(re.search(r"^Adaptive RX:\s*(\S+)",t,re.M) or [None,None])[1] for i,t in nic.get("coalesce",{}).items()}
        result["nic"]={"adaptive_rx_at_start":adaptive,"rx_off":{i:t.strip() for i,t in nic.get("rx-off",{}).items()}}
    if "failover" in workloads:
        if set(fail_logs)!=set(range(3)):
            raise ValueError("missing failover logs")
        result["failover"]=failovers(fail_logs)
    return result


if __name__=="__main__":
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("directories",type=Path,nargs="+")
    for directory in cli.parse_args().directories:
        print(json.dumps(summarize(directory)))
