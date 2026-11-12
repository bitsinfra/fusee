"""Verify actual PostgreSQL quorum flushes, Temporal health and all rotated chain trials."""
import argparse
import json
from pathlib import Path
import re
import tarfile


def check(directory):
    manifest=json.loads((directory/'run.json').read_text())
    assert manifest['system']=='temporal' and manifest['bucket_removed'] and manifest['security_group_removed']
    cases={}
    snapshots=[]
    members=manifest['nodes']
    assert len(members)==3
    for node in members:
        i=node['index']
        assert node['terminated'] and node['exit_code']==0
        with tarfile.open(directory/f'node-{i}.tgz') as guest:
            assert guest.extractfile('./exit-code').read().strip()==b'0'
            assert b'SERVING' in guest.extractfile('./temporal-health.log').read().upper()
            config=json.load(guest.extractfile('./temporal-config.json'))
            assert all(v['sql']['connectAddr']==members[0]['address']+':5432' and 'password' not in v['sql']
                       for v in config['persistence']['datastores'].values())
            assert config['global']['membership']['broadcastAddress']==node['address']
            if i==0:
                for label in ('before','after'):
                    lines=guest.extractfile(f'./pg-durability-{label}.txt').read().decode().splitlines()
                    assert lines[:3]==['on','on','ANY 1 (standby1, standby2)']
                    assert len(lines)==5
                    for j,line in enumerate(lines[3:],1):
                        assert re.fullmatch(fr'standby{j}\|streaming\|quorum\|[A-F\d]+/[A-F\d]+',line)
                    snapshots.append(dict(position=label,standbys=lines[3:]))
                slots=guest.extractfile('./pg-replication-slots.txt').read().decode().splitlines()
                assert len(slots)==2 and all(l.startswith(f'standby{i}|t|') for i,l in enumerate(slots,1))
            for member in guest:
                m=re.fullmatch(r'./temporal-(\d+)-(\d+)\.json',member.name)
                if not m:continue
                trial,n=map(int,m.groups());assert i==trial%3 and (trial,n) not in cases
                docs=[json.loads(l) for l in guest.extractfile(member) if l.startswith(b'{')]
                assert len(docs)==1
                doc=docs[0];row=doc['result'][0]
                assert doc['engine']=='temporal-tuned' and row['steps']==n and row['workflow']['count']==2
                assert row['step_to_step']['count']==2*(n-1) and len(row['transition_samples_us'])==2
                assert all(len(g)==n-1 and all(v>=0 for v in g) for g in row['transition_samples_us'])
                cases[trial,n]=dict(trial=trial,steps=n,node=i,p50_us=row['step_to_step']['p50_us'],
                    p99_us=row['step_to_step']['p99_us'],workflow_p50_us=row['workflow']['p50_us'])
    assert set(cases)=={(t,n) for t in range(5) for n in (1,10,100,1000)}
    return dict(run=directory.name,topology=manifest['topology'],wal=snapshots,measurements=[cases[k] for k in sorted(cases)])


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory',type=Path)
    print(json.dumps(check(p.parse_args().directory),indent=2))
