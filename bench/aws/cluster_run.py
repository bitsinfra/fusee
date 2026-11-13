"""Run three isolated EC2 replicas with serial sockperf pairs and collect every node's evidence."""
import argparse
from contextlib import ExitStack
from evidence import cleanup_bucket, download_archive, save_manifest
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import uuid

import boto3
from cryptography.fernet import Fernet
from botocore.exceptions import ClientError
from botocore.config import Config
from host_run import STORAGE_AND_NETWORK_HOURLY, refresh_upload, hourly_price, instance_state, reserve, terminate, userdata


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("bundle", type=Path)
    cli.add_argument("placement", choices=["same-zone", "cluster", "cross-zone"])
    cli.add_argument("--system",choices=["fusee","restate","temporal"],default="fusee")
    cli.add_argument("--minutes", type=int, default=45)
    cli.add_argument("--workloads", default="chain,open",
                     help="comma-separated Fusee workloads: chain, open, saturate (open loop to saturation, "
                          "first two trials), failover (primary kills on the hosts, once)")
    cli.add_argument("--instance-type", default="i4i.large",
                     help="EC2 type of all three members; it needs an instance-store NVMe disk "
                          "(i4i.large: 2 vCPUs; i4i.2xlarge: 8 vCPUs, for throughput)")
    cli.add_argument("--failover-kills", type=int, help="primary kills in the failover workload (default 12)")
    cli.add_argument("--busy-poll-us", type=int,
                     help="busy-polled receive: replicas poll their sockets this long after any activity")
    cli.add_argument("--busy-poll-trial-us", type=int,
                     help="after the default trials, five more chain trials with replicas busy-polling this long")
    cli.add_argument("--no-rx-off", action="store_true",
                     help="skip the chain trials with adaptive receive moderation off (run by default with chain)")
    cli.add_argument("--stall-us", type=int, help="engine stall-trace threshold on every member (default 1000; 0 turns it off)")
    cli.add_argument("--work", type=Path, default=Path.home()/"fusee-bench-runs")
    cli.add_argument("--zone", default="us-east-1a",
                     help="zone of all three members for same-zone and cluster placement")
    cli.add_argument("--spot", action="store_true",
                     help="one-time spot members, the bid capped at the on-demand hourly price")
    args = cli.parse_args()
    if not 15 <= args.minutes <= 75:
        cli.error("lifetime must be 15 to 75 minutes")
    workloads = args.workloads.split(",")
    if not workloads or set(workloads) - {"chain", "open", "saturate", "failover"} or len(set(workloads)) != len(workloads):
        cli.error("--workloads takes distinct chain, open, saturate and failover")
    if args.system != "fusee" and (workloads != ["chain", "open"] or args.stall_us is not None
                                   or args.failover_kills is not None or args.busy_poll_us is not None
                                   or args.busy_poll_trial_us is not None):
        cli.error("--workloads, --stall-us, --failover-kills and --busy-poll-us apply to --system fusee")
    if args.busy_poll_trial_us is not None and not 1 <= args.busy_poll_trial_us <= 10_000:
        cli.error("--busy-poll-trial-us must be 1 to 10,000")
    if args.failover_kills is not None and not 1 <= args.failover_kills <= 100:
        cli.error("--failover-kills must be 1 to 100")
    if args.busy_poll_us is not None and not 0 <= args.busy_poll_us <= 10_000:
        cli.error("--busy-poll-us must be 0 to 10,000")
    if args.stall_us is not None and not 0 <= args.stall_us <= 10_000_000:
        cli.error("--stall-us must be 0 to 10,000,000")
    session = boto3.Session(region_name="us-east-1")
    ec2 = session.client("ec2")
    s3 = session.client("s3",endpoint_url="https://s3.us-east-1.amazonaws.com",config=Config(signature_version="s3v4"))
    instance_type = args.instance_type
    price = hourly_price(session, instance_type)
    ident = "fusee-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")+"-"+uuid.uuid4().hex[:6]
    amount = round(3*(price+STORAGE_AND_NETWORK_HOURLY)*(args.minutes+15)/60 + .3, 2)
    remaining = reserve(args.work, ident, amount)
    output = args.work/ident
    output.mkdir()
    zones = ["us-east-1a","us-east-1c","us-east-1b"] if args.placement=="cross-zone" else [args.zone]*3
    manifest = {"run":ident, "case":"cluster", "type":instance_type, "topology":args.placement, "system":args.system, "zones":zones,
                "usd_per_hour_ec2":price, "reserved_usd":amount, "remaining_unreserved_usd":remaining, "nodes":[],
                "market":"spot" if args.spot else "on-demand"}
    if args.system == "fusee":
        manifest.update(workloads=workloads, stall_us=1000 if args.stall_us is None else args.stall_us,
                        busy_poll_us=args.busy_poll_us or 0)
    def save():
        save_manifest(output/"run.json",manifest)
        print(json.dumps(manifest), flush=True)
    save()
    tags = [{"Key":"Project","Value":"FuseeBench"}, {"Key":"Name","Value":ident}]
    subnets = {}
    for zone in set(zones):
        entries = ec2.describe_subnets(Filters=[{"Name":"default-for-az","Values":["true"]},
                                                {"Name":"availability-zone","Values":[zone]}])["Subnets"]
        if len(entries)!=1:
            raise ValueError(f"expected one default subnet in {zone}")
        subnets[zone]=entries[0]
    if len({s["VpcId"] for s in subnets.values()}) != 1:
        raise ValueError("all replicas require the same VPC")
    info = ec2.describe_instance_types(InstanceTypes=[instance_type])["InstanceTypes"][0]
    arch = {"x86_64":"amd64","arm64":"arm64"}[info["ProcessorInfo"]["SupportedArchitectures"][0]]
    image = session.client("ssm").get_parameter(Name=f"/aws/service/canonical/ubuntu/server/24.04/stable/current/{arch}/hvm/ebs-gp3/ami-id")["Parameter"]["Value"]
    root = ec2.describe_images(ImageIds=[image])["Images"][0]["RootDeviceName"]
    bucket = ident+"-"+uuid.uuid4().hex[:12]
    manifest["bucket"]=bucket
    source, peers = "source.tgz", "peers.json"
    result_keys = [f"node-{i}.tgz" for i in range(3)]
    digest = hashlib.sha256(args.bundle.read_bytes()).hexdigest()
    manifest["source_sha256"]=digest
    def url(op, key):
        return s3.generate_presigned_url(op, Params={"Bucket":bucket,"Key":key}, ExpiresIn=(args.minutes+30)*60)
    def clean_bucket():
        verified=[k for k,v in manifest.get("archives",{}).items() if not v["scratch"]]
        keep=cleanup_bucket(s3,bucket,[source,peers],result_keys,verified)
        manifest.update(bucket_removed=not keep,retained_results=keep)
    def remove_node(node):
        terminate(ec2,node["id"])
        node["terminated"]=True
    def remove_group(group):
        # EC2 may keep the just-terminated instance's ENI briefly.
        for _ in range(30):
            try:
                ec2.delete_security_group(GroupId=group)
                manifest["security_group_removed"]=True
                return
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "DependencyViolation":
                    raise
                time.sleep(2)
        raise RuntimeError(f"security group cleanup timed out: {group}")
    start = time.monotonic()
    finished = set()
    secrets=[Fernet.generate_key() for _ in range(3)]
    try:
        with ExitStack() as cleanup:
            s3.create_bucket(Bucket=bucket)
            cleanup.callback(clean_bucket)
            s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
                "BlockPublicAcls":True,"IgnorePublicAcls":True,"BlockPublicPolicy":True,"RestrictPublicBuckets":True})
            s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet":tags})
            s3.upload_file(str(args.bundle),bucket,source)
            group = ec2.create_security_group(GroupName=ident, Description="Traffic between three benchmark peers",
                                                VpcId=subnets[zones[0]]["VpcId"])["GroupId"]
            cleanup.callback(remove_group,group)
            ec2.authorize_security_group_ingress(GroupId=group, IpPermissions=[
                {"IpProtocol":"tcp", "FromPort":port, "ToPort":port, "UserIdGroupPairs":[{"GroupId":group}]}
                for port in ([11111,39170,39990] if args.system=="fusee" else [11111,5122,9080,39990] if args.system=="restate" else [5432,6933,6934,6935,6939,7233,7234,7235,7239,11111,39990])])
            if args.placement=="cluster":
                ec2.create_placement_group(GroupName=ident, Strategy="cluster")
                cleanup.callback(ec2.delete_placement_group,GroupName=ident)
            for i, zone in enumerate(zones):
                placement = {"AvailabilityZone":zone}
                if args.placement=="cluster":
                    placement["GroupName"]=ident
                options = {"FUSEE_CLUSTER_WORKLOADS":" ".join(workloads)} if args.system == "fusee" else {}
                if args.stall_us is not None:
                    options["FUSEE_STALL_US"] = args.stall_us
                if args.failover_kills is not None:
                    options["FUSEE_FAILOVER_KILLS"] = args.failover_kills
                if args.busy_poll_us is not None:
                    options["FUSEE_REPLICA_BUSY_POLL_US"] = args.busy_poll_us
                if args.busy_poll_trial_us is not None:
                    options["FUSEE_CLUSTER_BUSY_POLL_TRIAL_US"] = args.busy_poll_trial_us
                if args.no_rx_off:
                    options["FUSEE_CLUSTER_RX_OFF"] = 0
                guest = userdata(url("get_object",source),url("put_object",result_keys[i]),digest,args.minutes,{**options,
                    "FUSEE_CASE":"cluster","FUSEE_CLUSTER_SYSTEM":args.system,"FUSEE_INDEX":i,"FUSEE_UPLOAD_KEY":secrets[i].decode(),"FUSEE_PEER_URL":url("get_object",peers),"FUSEE_ZONE":zone,
                    "FUSEE_INSTANCE_TYPE":instance_type,"FUSEE_DATA":"instance","FUSEE_USD_PER_HOUR":3*price})
                market = {"InstanceMarketOptions":{"MarketType":"spot","SpotOptions":{
                    "MaxPrice":str(price),"SpotInstanceType":"one-time"}}} if args.spot else {}
                host = ec2.run_instances(**market, ImageId=image, InstanceType=instance_type, Placement=placement,
                    ClientToken=f"{ident}-{i}", MinCount=1, MaxCount=1, UserData=guest,
                    NetworkInterfaces=[{"DeviceIndex":0,"SubnetId":subnets[zone]["SubnetId"],"Groups":[group],
                                        "AssociatePublicIpAddress":True,"DeleteOnTermination":True}],
                    BlockDeviceMappings=[{"DeviceName":root,"Ebs":{"VolumeType":"gp3","VolumeSize":24,"DeleteOnTermination":True}}],
                    TagSpecifications=[{"ResourceType":kind,"Tags":tags} for kind in ("instance","volume")],
                    MetadataOptions={"HttpTokens":"required","InstanceMetadataTags":"enabled"}, InstanceInitiatedShutdownBehavior="terminate")["Instances"][0]
                node = {"index":i,"id":host["InstanceId"],"address":host["PrivateIpAddress"],"zone":zone}
                cleanup.callback(remove_node,node)
                manifest["nodes"].append(node)
                save()
            s3.put_object(Bucket=bucket,Key=peers,Body=json.dumps(manifest["nodes"]).encode())
            grant_times=[0.0]*3
            while len(finished)<3 and time.monotonic()-start<(args.minutes+6)*60:
                for i, node in enumerate(manifest["nodes"]):
                    if i in finished:
                        continue
                    if time.monotonic()-grant_times[i]>=90 and instance_state(ec2,node["id"])=="running":
                        refresh_upload(ec2,node["id"],secrets[i],url("put_object",result_keys[i]))
                        grant_times[i]=time.monotonic()
                    try:
                        s3.head_object(Bucket=bucket,Key=result_keys[i])
                    except ClientError as exc:
                        if exc.response["Error"]["Code"] not in {"404","NoSuchKey","NotFound"}:
                            raise
                        if instance_state(ec2,node["id"])=="terminated":
                            node["result_missing"] = True
                            finished.add(i)
                            save()
                            continue
                        continue
                    dest = output/result_keys[i]
                    archive=download_archive(s3,bucket,result_keys[i],dest)
                    manifest.setdefault("archives",{})[result_keys[i]]=archive
                    node["exit_code"]=archive["exit_code"]
                    save()
                    finished.add(i)
                if len(finished)<3:
                    time.sleep(8)
            if len(finished)!=3 or any(node.get("exit_code") != 0 for node in manifest["nodes"]):
                raise RuntimeError("incomplete cluster measurement; inspect each collected guest archive")
    finally:
        manifest["elapsed_seconds"]=round(time.monotonic()-start)
        save()
    print(f"verified cluster result: {output}",flush=True)


if __name__ == "__main__":
    main()
