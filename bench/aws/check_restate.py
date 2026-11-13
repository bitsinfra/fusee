"""Validate Restate's replicated log configuration and every measured chain."""
import argparse
import json
from pathlib import Path
import re
import tarfile


def summarize(path):
    manifest=json.loads((path/"run.json").read_text())
    assert manifest["system"]=="restate" and manifest["bucket_removed"] and manifest["security_group_removed"]
    measurements={}
    for member in manifest["nodes"]:
        i=member["index"]
        assert member["terminated"] and member["exit_code"]==0
        with tarfile.open(path/f"node-{i}.tgz") as guest:
            assert guest.extractfile("./exit-code").read().strip()==b"0"
            env=json.load(guest.extractfile("./restate-config.json"))
            assert all(f"RESTATE_LOG_SERVER__{setting}=false" in env for setting in ["ROCKSDB_DISABLE_WAL","ROCKSDB_DISABLE_WAL_FSYNC"])
            assert len(re.findall(r"\bnode-[012]\b",guest.extractfile("./restate-final-status.log").read().decode()))==3
            if i==0:
                proof=guest.extractfile("./restate-log-zero.log").read().decode()
                assert "replicated" in proof and "{node: 2}" in proof and all(x in proof for x in ("N1","N2","N3"))
            for m in guest:
                match=re.fullmatch(r'./restate-(\d+)-(\d+)\.json',m.name)
                if not match:continue
                trial,n=map(int,match.groups());assert trial%3==i
                docs=[json.loads(s) for s in guest.extractfile(m) if s.startswith(b"{")]
                assert len(docs)==1
                row=docs[0]["result"][0]
                assert (trial,n) not in measurements and row["steps"]==n and row["workflow"]["count"]==2
                assert row["step_to_step"]["count"]==2*(n-1) and all(len(x)==n-1 for x in row["transition_samples_us"])
                measurements[trial,n]=dict(trial=trial,steps=n,node=i,p50_us=row["step_to_step"]["p50_us"],
                                            p99_us=row["step_to_step"]["p99_us"],workflow_p50_us=row["workflow"]["p50_us"])
    assert set(measurements)=={(t,n) for t in range(5) for n in (1,10,100,1000)}
    return {"run":manifest["run"],"topology":manifest["topology"],"measurements":[measurements[k] for k in sorted(measurements)]}


if __name__=="__main__":
    cli=argparse.ArgumentParser(description=__doc__);cli.add_argument("directory",type=Path)
    print(json.dumps(summarize(cli.parse_args().directory),indent=2))
