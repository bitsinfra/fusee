"""Match each EC2 guest's source revision to the exact retained source bundle."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import tarfile


def revision(bundle):
    digest=hashlib.sha256()
    with tarfile.open(bundle) as archive:
        stamp=archive.extractfile("SOURCE-REVISION").read().decode().splitlines()
        for member in sorted(archive.getmembers(),key=lambda m:m.name):
            if member.name=="SOURCE-REVISION":continue
            if not member.isfile():raise ValueError(f"unexpected bundle member {member.name}")
            digest.update(member.name.encode()+b"\0"+hashlib.file_digest(archive.extractfile(member),"sha256").digest())
    if len(stamp)!=2 or digest.hexdigest()!=stamp[1]:raise ValueError(f"source digest mismatch in {bundle}")
    return tuple(stamp)


def source_inventory(directory, bundles):
    sources={}
    by_revision=defaultdict(list)
    for bundle in bundles:
        blob=hashlib.file_digest(bundle.open("rb"),"sha256").hexdigest()
        stamp=revision(bundle)
        sources[blob]=dict(bundle=bundle.name,base_commit=stamp[0],source_digest=stamp[1],runs=[])
        by_revision[stamp].append(blob)
    for run in sorted(directory.glob("fusee-*/run.json")):
        manifest=json.loads(run.read_text())
        stamps=set()
        for raw in sorted(run.parent.glob("*.tgz")):
            with tarfile.open(raw) as archive:
                members=archive.getnames()
                if "./source-revision.txt" in members:
                    stamps.add(tuple(archive.extractfile("./source-revision.txt").read().decode().splitlines()))
        blob=manifest.get("source_sha256")
        if blob is None and len(stamps)==1:
            candidates=by_revision[next(iter(stamps))]
            if len(candidates)==1:blob=candidates[0]
        if blob is None:
            if not stamps and not any(run.parent.glob("*.tgz")):continue
            raise ValueError(f"no unique source for {run.parent.name}: {stamps}")
        source=sources[blob]
        if stamps and stamps!={(source["base_commit"],source["source_digest"])}:
            raise ValueError(f"guest and controller sources differ in {run.parent.name}")
        source["runs"].append(run.parent.name)
    return {k:v for k,v in sorted(sources.items()) if v["runs"]}


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory",type=Path)
    root=parser.parse_args().directory
    print(json.dumps(source_inventory(root,(root/"sources").glob("*.tgz")),indent=2))
