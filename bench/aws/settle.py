"""Release unused reservations only after the disposable AWS resources have gone."""
import argparse
import fcntl
import json
import math
from pathlib import Path

import boto3
from host_run import STORAGE_AND_NETWORK_HOURLY, spend_limits


def safe_bound(run, directory, ec2):
    if not run.get("bucket_removed") or "elapsed_seconds" not in run:
        return None
    if (directory/"diagnostic-snapshots.json").exists() or (directory/"recovery-volume.json").exists():
        # A run whose disks were kept for inspection keeps its full bound.
        return None
    if run.get("case")=="cluster":
        nodes=run.get("nodes",[])
        if not run.get("security_group_removed") or any(not n.get("terminated") for n in nodes):
            return None
        count=len(nodes)
    else:
        if run.get("instance") and not run.get("instance_terminated"):
            return None
        count=int(bool(run.get("instance")))
    filters=[{"Name":"tag:Name","Values":[run["run"]]}]
    active={"Name":"instance-state-name","Values":["pending","running","stopping","stopped","shutting-down"]}
    if ec2.describe_instances(Filters=[*filters,active])["Reservations"] or ec2.describe_volumes(Filters=filters)["Volumes"]:
        return None
    hours=(run["elapsed_seconds"]+180)/3600
    bound=count*hours*(run["usd_per_hour_ec2"]+STORAGE_AND_NETWORK_HOURLY)+max(1,count)*.10
    return math.ceil(bound*100)/100


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work",type=Path,default=Path.home()/"fusee-bench-runs")
    directory=parser.parse_args().work
    path=directory/"budget.json"
    clients={}
    # Other controllers may reserve new runs while EC2 inventories are queried.
    bounds={}
    for item in sorted(directory.glob("*/run.json")):
        data=json.loads(item.read_text())
        if "zone" in data:region=data["zone"][:-1]
        elif data.get("zones"):region=data["zones"][0][:-1]
        else:continue
        if region not in clients:clients[region]=boto3.client("ec2",region_name=region)
        estimate=safe_bound(data,item.parent,clients[region])
        if estimate is not None:bounds[data["run"]]=estimate
    with (directory/"budget.lock").open("a+") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        entries=json.loads(path.read_text())
        old=sum(entries.values())
        for run,bound in bounds.items():
            if run in entries:entries[run]=min(bound,entries[run])
        tmp=directory/"budget.tmp"
        tmp.write_text(json.dumps(entries,indent=2));tmp.replace(path)
        total_limit,prior_spend=spend_limits()
        used=round(prior_spend+sum(entries.values()),2)
        print(json.dumps({"prior_plus_reserved_usd":used,"unreserved_usd":round(total_limit-used,2),
                          "unused_reservations_released_usd":round(old-sum(entries.values()),2)}))


if __name__=="__main__":
    main()
