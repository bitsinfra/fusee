"""Run a time-limited single-host benchmark on a disposable EC2 instance."""
import argparse
from datetime import datetime, timezone
from evidence import cleanup_bucket, download_archive, save_manifest
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import shlex
import tarfile
import time
import uuid

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from cryptography.fernet import Fernet

# Cells a `cells` run (or a `local` run with --cells) can select.
CELLS = ("fanout-1000","fanout-10000","payload","children","service-concurrent","saturate")
# The soak's default rates bracket the published 32-step sweep: one record per
# flush, the device's write count already the limit, and the highest rate with
# step p99 under 1 ms (about 145,000 flushes/s against a 165,000/s ceiling).
SOAK_RATES = "1600,12800,51200"
# Per-hour allowance for a public IPv4 address, up to 32 GiB of root gp3,
# 32 GiB of test storage, and up to 3,000 provisioned io2 IOPS in us-east-1.
STORAGE_AND_NETWORK_HOURLY = 0.60


def hourly_price(session, instance):
    client = session.client("pricing", region_name="us-east-1")
    filters = {"instanceType":instance, "operatingSystem":"Linux", "tenancy":"Shared",
               "preInstalledSw":"NA", "capacitystatus":"Used", "regionCode":session.region_name}
    result = client.get_products(ServiceCode="AmazonEC2",
        Filters=[{"Type":"TERM_MATCH","Field":k,"Value":v} for k,v in filters.items()])
    rates = set()
    for product in map(json.loads,result["PriceList"]):
        for term in product["terms"]["OnDemand"].values():
            for dim in term["priceDimensions"].values():
                if dim["unit"] == "Hrs":
                    rates.add(float(dim["pricePerUnit"]["USD"]))
    if len(rates) != 1 or min(rates) <= 0:
        raise ValueError(f"ambiguous EC2 price for {instance}: {rates}")
    return rates.pop()


def spend_limits():
    """The most every run reserved through these scripts may cost together
    (`FUSEE_AWS_SPEND_LIMIT`, USD, required) and what was spent outside them
    (`FUSEE_AWS_PRIOR_SPEND`, USD, 0 unless set)."""
    limit = os.environ.get("FUSEE_AWS_SPEND_LIMIT")
    if limit is None:
        raise SystemExit("set FUSEE_AWS_SPEND_LIMIT (USD) to cap EC2 spending")
    return float(limit), float(os.environ.get("FUSEE_AWS_PRIOR_SPEND", "0"))


def reserve(work, run_id, amount):
    work.mkdir(parents=True,exist_ok=True)
    with (work/"budget.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = work/"budget.json"
        entries = json.loads(path.read_text()) if path.exists() else {}
        total_limit, prior_spend = spend_limits()
        committed = prior_spend + sum(entries.values())
        if committed + amount > total_limit:
            raise ValueError(f"remaining AWS budget ${total_limit-committed:.2f}; proposed bound ${amount:.2f}")
        entries[run_id] = amount
        tmp = work/"budget.tmp"
        tmp.write_text(json.dumps(entries,indent=2))
        tmp.replace(path)
        return round(total_limit-committed-amount,2)


def instance_state(ec2, instance_id):
    try:
        instance = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
        return instance["State"]["Name"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
            return "registering"
        raise


def terminate(ec2, instance_id):
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        state = instance_state(ec2,instance_id)
        if state == "terminated":
            return
        if state not in {"registering","shutting-down"}:
            ec2.terminate_instances(InstanceIds=[instance_id])
        time.sleep(5)
    raise RuntimeError(f"termination unconfirmed: {instance_id}")


def refresh_upload(ec2, instance_id, secret, result_url):
    # CloudShell's signing credentials can rotate before a long trial ends.
    # Only the guest with this one-time key can decrypt its tagged PUT grant.
    encrypted = Fernet(secret).encrypt(result_url.encode()).decode()
    parts=[encrypted[i:i+230] for i in range(0,len(encrypted),230)]
    if len(parts)>40:
        raise ValueError("upload grant exceeds EC2 tag capacity")
    tags=[{"Key":"FuseeGrantParts","Value":str(len(parts))}]
    tags.extend({"Key":f"FuseeGrant{i:02d}","Value":p} for i,p in enumerate(parts))
    ec2.create_tags(Resources=[instance_id],Tags=tags)


def userdata(source, result, expected, minutes, env):
    variables = "\n".join(f"export {k}={shlex.quote(str(v))}" for k,v in env.items())
    return f"""#!/bin/bash
set -eu
umask 077
mkdir -p /opt/fusee /var/tmp/fusee-results
systemd-run --unit=fusee-expire --on-active={minutes}min /sbin/shutdown -h now
exec > /var/tmp/fusee-results/host.log 2>&1
finish() {{
  code=$?
  trap - EXIT
  echo "$code" >/var/tmp/fusee-results/exit-code
  tar czf /var/tmp/fusee-results.tgz -C /var/tmp/fusee-results .
  http=$(curl -sS --retry 3 --max-time 120 -o /var/tmp/fusee-upload-response -w '%{{http_code}}' --upload-file /var/tmp/fusee-results.tgz {shlex.quote(result)}) || http=transport-error
  if [[ "$http" != 200 && "$http" != 201 && "$http" != 204 && -f /opt/fusee/bench/aws/guest-upload.py && -n "${{FUSEE_UPLOAD_KEY:-}}" ]]; then
    if python3 /opt/fusee/bench/aws/guest-upload.py /var/tmp/fusee-results.tgz {shlex.quote(result)} "$FUSEE_UPLOAD_KEY"; then http=200; fi
  fi
  echo "fusee benchmark exit=$code result_upload_http=$http" >/dev/console
  /sbin/shutdown -h now
}}
trap finish EXIT
http=$(curl -fsS --retry 3 -w '%{{http_code}}' {shlex.quote(source)} -o /var/tmp/source.tgz)
test "$http" = 200 || {{ echo "source download HTTP $http"; exit 1; }}
echo '{expected}  /var/tmp/source.tgz' | sha256sum -c -
tar xzf /var/tmp/source.tgz -C /opt/fusee
{variables}
export FUSEE_RESULTS=/var/tmp/fusee-results
export FUSEE_REVISION=$(tr '\\n' '/' </opt/fusee/SOURCE-REVISION)
timeout --signal=TERM --kill-after=10s {(minutes-5)*60} bash /opt/fusee/bench/aws/host.sh
"""


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("bundle",type=Path)
    cli.add_argument("--case",choices=["floors","gp3","local","cells","soak","dm-replay","sleepers","comparisons","external","sdk","packages","micro","pgo"],required=True)
    cli.add_argument("--instance",required=True)
    cli.add_argument("--disk",choices=["instance","gp3","io2"],required=True)
    cli.add_argument("--zone",default="us-east-1a")
    cli.add_argument("--minutes",type=int,default=45)
    cli.add_argument("--matrix-cells",type=Path,help="JSON selection of benchmark systems, trials and lengths")
    cli.add_argument("--segment-probe",action="store_true",help="measure paired eight-shard startup and recovery before the external suite")
    cli.add_argument("--sdk-paired",action="store_true",help="also measure Node with the low-load gate disabled on the same SDK host")
    cli.add_argument("--cells",help=f"comma-separated cells for --case cells (default all) or after the --case local suite: {','.join(CELLS)}")
    cli.add_argument("--cell-trials",type=int,choices=range(1,6),help="trials of each cell (default 3)")
    cli.add_argument("--soak-rates",help=f"comma-separated starts/s for --case soak (default {SOAK_RATES}, 32 steps per workflow)")
    cli.add_argument("--soak-seconds",type=int,help="seconds per soak rate (default 600)")
    cli.add_argument("--stall-us",type=int,help="engine stall-trace threshold for Fusee harness cells (default 1000; 0 turns tracing off)")
    cli.add_argument("--external-systems",help="comma-separated resident SDK workers for the external suite")
    cli.add_argument("--external-workloads",help="comma-separated workloads for the resident SDK workers")
    cli.add_argument("--external-trials", type=int, choices=range(1, 6), default=5)
    cli.add_argument("--sleep-count",type=int,default=1_000_000)
    cli.add_argument("--sleep-lead",type=int,default=420,help="seconds for the population to enter its timers")
    cli.add_argument("--sleep-drain",type=int,default=240)
    cli.add_argument("--spot",action="store_true",help="cap the spot bid at the same on-demand hourly ceiling")
    cli.add_argument("--work",type=Path,default=Path.home()/"fusee-bench-runs")
    args = cli.parse_args()
    cells={}
    if args.segment_probe:
        if args.case!="external":cli.error("--segment-probe needs --case external")
        cells["FUSEE_SEGMENT_PROBE"]="1"
    if args.sdk_paired:
        if args.case!="sdk":cli.error("--sdk-paired needs --case sdk")
        cells["FUSEE_SDK_PAIRED"]="1"
    if args.matrix_cells:
        if args.case!="comparisons":cli.error("--matrix-cells needs comparisons")
        cells={"FUSEE_MATRIX_CELLS":json.dumps(json.loads(args.matrix_cells.read_text()),separators=(",",":"))}
    if args.external_systems:
        if args.case!="external":cli.error("--external-systems needs --case external")
        cells["FUSEE_EXTERNAL_SYSTEMS"]=args.external_systems
    if args.case=="external":
        cells["FUSEE_EXTERNAL_TRIALS"]=args.external_trials
    if args.external_workloads:
        kinds=args.external_workloads.split(",")
        if args.case!="external" or not kinds or set(kinds)-{"fanout","service-1","service-50","open","max","cost","signal","kill","sleepers"}:
            cli.error("invalid requested external workloads")
        cells["FUSEE_EXTERNAL_WORKLOADS"]=args.external_workloads
        if "sleepers" in kinds:
            if not (1<=args.sleep_count<=1_000_000 and 5<=args.sleep_lead<=1800 and 5<=args.sleep_drain<=1200):
                cli.error("invalid sleeper population or deadline")
            cells.update(FUSEE_SLEEP_COUNT=args.sleep_count,FUSEE_SLEEP_LEAD=args.sleep_lead,FUSEE_SLEEP_DRAIN=args.sleep_drain)
    if args.cells is not None:
        chosen=args.cells.split(",")
        if args.case not in ("cells","local") or not chosen or set(chosen)-set(CELLS):
            cli.error(f"--cells needs --case cells or local and a subset of {','.join(CELLS)}")
        cells["FUSEE_CELLS"]=args.cells
    elif args.case=="cells":
        cells["FUSEE_CELLS"]=",".join(CELLS)
    if args.cell_trials is not None:
        if "FUSEE_CELLS" not in cells:cli.error("--cell-trials needs cells")
        cells["FUSEE_CELL_TRIALS"]=args.cell_trials
    if args.soak_rates is not None or args.soak_seconds is not None:
        if args.case!="soak":cli.error("--soak-rates and --soak-seconds need --case soak")
    if args.case=="soak":
        try:
            rates=[float(r) for r in (args.soak_rates or SOAK_RATES).split(",")]
        except ValueError:
            cli.error("--soak-rates must be numbers")
        seconds=600 if args.soak_seconds is None else args.soak_seconds
        if not 1<=len(rates)<=5 or not all(0<r<=200_000 for r in rates) or not 10<=seconds<=1200:
            cli.error("use one to five soak rates up to 200,000 starts/s and 10 to 1,200 seconds per rate")
        if args.disk!="instance" and args.soak_rates is None:
            cli.error("the default soak rates assume instance storage; pass --soak-rates for EBS")
        # Build, floors and engine opens take up to about 15 minutes; the guest
        # stops the run five minutes before the instance's lifetime ends.
        need=math.ceil(len(rates)*seconds/60)+20
        if args.minutes<need:
            cli.error(f"a soak of {len(rates)} x {seconds} s needs --minutes {need} or more")
        cells.update(FUSEE_SOAK_RATES=",".join(f"{r:g}" for r in rates),FUSEE_SOAK_SECONDS=seconds)
    if args.stall_us is not None:
        # Other cases run SDK engines that would read the variable too.
        if args.case not in ("local","cells","soak"):cli.error("--stall-us needs --case local, cells or soak")
        if not 0<=args.stall_us<=10_000_000:cli.error("--stall-us must be 0 to 10,000,000")
        cells["FUSEE_STALL_US"]=args.stall_us
    if args.case == "external" and not args.external_systems:
        cli.error("select the resident workers with --external-systems")
    if args.case == "comparisons" or (args.case == "external" and "obelisk" in args.external_systems.split(",")):
        with tarfile.open(args.bundle) as archive:
            members = {m.name: m for m in archive if m.isfile()}
            for component in ("noop_activity", "chain_workflow"):
                name = f"bench/drivers/obelisk/build/{component}.wasm"
                if name not in members or not members[name].size:
                    cli.error("this comparison needs Obelisk components; build the source bundle with --include-obelisk")
    with args.bundle.open("rb") as source:
        source_hash = hashlib.file_digest(source, "sha256").hexdigest()
    region = args.zone[:-1]
    if not 10 <= args.minutes <= 90 or region not in {"us-east-1","us-west-2"} or not args.zone[-1:].isalpha():
        cli.error("use a us-east-1 or us-west-2 zone and a 10 to 90 minute lifetime")
    session = boto3.Session(region_name=region)
    ec2 = session.client("ec2")
    # A newly created bucket can redirect the legacy global S3 hostname.
    # Sign the final regional URL for both the GET and the PUT.
    s3 = session.client("s3",endpoint_url=f"https://s3.{region}.amazonaws.com",
                        config=Config(signature_version="s3v4",s3={"addressing_style":"virtual"}))
    run_id = "fusee-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")+"-"+uuid.uuid4().hex[:6]
    price = hourly_price(session,args.instance)
    # Charge the full reserved lifetime plus a 15-minute boot/shutdown margin.
    reserve_usd = round((price+STORAGE_AND_NETWORK_HOURLY)*(args.minutes+15)/60+.10,2)
    remaining = reserve(args.work,run_id,reserve_usd)
    directory = args.work/run_id
    directory.mkdir()
    manifest = {"run":run_id,"type":args.instance,"zone":args.zone,"case":args.case,"disk":args.disk,
                "usd_per_hour_ec2":price,"reserved_usd":reserve_usd,"remaining_unreserved_usd":remaining,"source_sha256":source_hash,"source_bytes":args.bundle.stat().st_size}
    if args.sdk_paired:manifest["sdk_paired"]=True
    if args.segment_probe:manifest["segment_probe"]=True
    if "FUSEE_CELLS" in cells:manifest.update(cells=cells["FUSEE_CELLS"].split(","),cell_trials=cells.get("FUSEE_CELL_TRIALS",3))
    if args.case=="soak":manifest.update(soak_rates=[float(r) for r in cells["FUSEE_SOAK_RATES"].split(",")],soak_seconds=cells["FUSEE_SOAK_SECONDS"])
    if args.case in ("local","cells","soak"):manifest["stall_us"]=cells.get("FUSEE_STALL_US",1000)
    manifest.update(market="spot" if args.spot else "on-demand")
    if args.matrix_cells:manifest["matrix_cells"]=json.loads(cells["FUSEE_MATRIX_CELLS"])
    if args.external_systems:manifest["external_systems"]=args.external_systems.split(",")
    if args.external_workloads:manifest["external_workloads"]=args.external_workloads.split(",")
    if args.case=="external":manifest["external_trials"]=args.external_trials
    if "FUSEE_SLEEP_COUNT" in cells:manifest.update(sleep_count=args.sleep_count,sleep_lead_seconds=args.sleep_lead,sleep_drain_seconds=args.sleep_drain)
    def save():
        save_manifest(directory/"run.json",manifest)
        print(json.dumps(manifest),flush=True)
    save()
    info = ec2.describe_instance_types(InstanceTypes=[args.instance])["InstanceTypes"][0]
    if args.disk == "instance" and not info.get("InstanceStorageSupported"):
        raise ValueError("requested instance lacks local storage")
    arch = info["ProcessorInfo"]["SupportedArchitectures"][0]
    arch = {"x86_64":"amd64","arm64":"arm64"}[arch]
    ssm = session.client("ssm")
    ami = ssm.get_parameter(Name=f"/aws/service/canonical/ubuntu/server/24.04/stable/current/{arch}/hvm/ebs-gp3/ami-id")["Parameter"]["Value"]
    volume_root = ec2.describe_images(ImageIds=[ami])["Images"][0]["RootDeviceName"]
    subnets = ec2.describe_subnets(Filters=[{"Name":"default-for-az","Values":["true"]},
        {"Name":"availability-zone","Values":[args.zone]}])["Subnets"]
    if len(subnets) != 1:
        raise ValueError("expected one default subnet in the selected zone")
    subnet = subnets[0]
    bucket = run_id + "-" + uuid.uuid4().hex[:12]
    source_key, result_key = "run/source.tgz","run/results.tgz"
    sg = instance_id = None
    received = False
    manifest["bucket"]=bucket
    created_bucket = False
    started = time.monotonic()
    try:
        placement = {} if region == "us-east-1" else {"CreateBucketConfiguration":{"LocationConstraint":region}}
        s3.create_bucket(Bucket=bucket,**placement)
        created_bucket = True
        s3.put_public_access_block(Bucket=bucket,PublicAccessBlockConfiguration={
            "BlockPublicAcls":True,"IgnorePublicAcls":True,"BlockPublicPolicy":True,"RestrictPublicBuckets":True})
        s3.put_bucket_tagging(Bucket=bucket,Tagging={"TagSet":[{"Key":"Project","Value":"FuseeBench"}]})
        s3.upload_file(str(args.bundle),bucket,source_key)
        def url(operation,key):
            return s3.generate_presigned_url(operation,Params={"Bucket":bucket,"Key":key},ExpiresIn=(args.minutes+30)*60)
        sg = ec2.create_security_group(GroupName=run_id,Description="Isolated Fusee benchmark",VpcId=subnet["VpcId"])["GroupId"]
        # No inbound rules or instance profile. Source and result access is limited to signed object requests.
        devices = [{"DeviceName":volume_root,"Ebs":{"VolumeType":"gp3","VolumeSize":32,"DeleteOnTermination":True}}]
        if args.disk != "instance":
            extra = {"VolumeType":args.disk,"VolumeSize":32,"DeleteOnTermination":True}
            if args.disk == "io2":
                extra["Iops"]=3000
            devices.append({"DeviceName":"/dev/sdf","Ebs":extra})
        upload_secret=Fernet.generate_key()
        tags = [{"Key":"Name","Value":run_id},{"Key":"Project","Value":"FuseeBench"}]
        market={"InstanceMarketOptions":{"MarketType":"spot","SpotOptions":{"MaxPrice":str(price),"SpotInstanceType":"one-time"}}} if args.spot else {}
        launch = ec2.run_instances(**market,ClientToken=run_id,ImageId=ami,InstanceType=args.instance,MinCount=1,MaxCount=1,
            NetworkInterfaces=[{"DeviceIndex":0,"SubnetId":subnet["SubnetId"],"Groups":[sg],
                               "AssociatePublicIpAddress":True,"DeleteOnTermination":True}],
            BlockDeviceMappings=devices,InstanceInitiatedShutdownBehavior="terminate",
            MetadataOptions={"HttpTokens":"required","HttpEndpoint":"enabled","InstanceMetadataTags":"enabled"},
            TagSpecifications=[{"ResourceType":kind,"Tags":tags} for kind in ["instance","volume"]],
            UserData=userdata(url("get_object",source_key),url("put_object",result_key),source_hash,args.minutes,
                {**cells,"FUSEE_DATA":args.disk,"FUSEE_CASE":args.case,"FUSEE_INDEX":0,"FUSEE_UPLOAD_KEY":upload_secret.decode(),
                 "FUSEE_INSTANCE_TYPE":args.instance,"FUSEE_ZONE":args.zone,"FUSEE_USD_PER_HOUR":price}))
        instance_id = launch["Instances"][0]["InstanceId"]
        manifest.update(instance=instance_id,bucket=bucket,security_group=sg,source_sha256=source_hash,ami=ami)
        save()
        last_grant=0.0
        while time.monotonic()-started < (args.minutes+8)*60:
            if time.monotonic()-last_grant>=90 and instance_state(ec2,instance_id)=="running":
                refresh_upload(ec2,instance_id,upload_secret,url("put_object",result_key))
                last_grant=time.monotonic()
            try:
                s3.head_object(Bucket=bucket,Key=result_key)
                archive=download_archive(s3,bucket,result_key,directory/"results.tgz")
                manifest["archives"]={result_key:archive}
                received = True
                break
            except ClientError as e:
                if e.response["Error"]["Code"] not in ("404","NoSuchKey","NotFound"):
                    raise
            state = instance_state(ec2,instance_id)
            if state == "terminated":
                raise RuntimeError("instance terminated before returning results")
            time.sleep(10)
        if not received:
            raise RuntimeError("benchmark result deadline reached")
    finally:
        if instance_id:
            terminate(ec2,instance_id)
            manifest["instance_terminated"] = True
        if sg:
            ec2.delete_security_group(GroupId=sg)
        if created_bucket:
            verified=[k for k,v in manifest.get("archives",{}).items() if not v["scratch"]]
            keep=cleanup_bucket(s3,bucket,[source_key],[result_key],verified)
            manifest.update(bucket_removed=not keep,retained_results=keep)
        manifest["elapsed_seconds"] = round(time.monotonic()-started)
        save()
    with tarfile.open(directory/"results.tgz") as report:
        code=report.extractfile("./exit-code").read().decode().strip()
    if code != "0":
        raise RuntimeError(f"guest exited {code}; inspect results.tgz/host.log")
    print(f"verified result: {directory}/results.tgz",flush=True)


if __name__ == "__main__":
    main()
