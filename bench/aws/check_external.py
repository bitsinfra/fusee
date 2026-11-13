"""Audit resident-worker results from scheduled arrivals down to individual activity clocks."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import tarfile
from check_sleepers import from_archive as check_sleepers
from check_kill import from_archive as check_kill


def check_row(row, label, system, trial, manifest):
    assert row['system']==system and row['trial']==trial
    assert row['machine']['instance_type']==manifest['type'] and row['machine']['zone']==manifest['zone']
    assert row['engine']['version'] and row['engine']['durability']
    samples=row['samples']
    n,steps=len(samples),row['steps_per_workflow']
    assert n==row['offered']==row['completed'] and n>0
    ids=set()
    attempts=set()
    first=[]
    whole=[]
    gaps=[]
    signal=[]
    for sample in samples:
        observation=sample['observation']
        begin=int(sample['scheduled_ns'])
        done=int(sample['completed_ns'])
        assert observation['id'] not in ids and observation['attempt'] not in attempts
        ids.add(observation['id']);attempts.add(observation['attempt'])
        assert observation['value']==(5050 if row['workload']=='fanout' else steps)
        marks=[(int(a),int(b)) for a,b in observation['body_ns']]
        assert len(marks)==steps and all(a<=b for a,b in marks)
        if row['workload']!='fanout':
            gaps.extend((a-d)//1000 for (_,d),(a,_) in zip(marks,marks[1:]))
        start=min(a for a,_ in marks)
        first.append((start-int(row['worker_minus_generator_ns'])-begin)//1000)
        whole.append((done-begin)//1000)
        if row['workload']=='signal':
            signal.append((start-int(observation['signal_sent_ns']))//1000)
    assert ids==set(range(n)) and min(first)>=-row['clock_probe_rtt_ns']/1000-1 and min(whole)>=0
    assert not gaps or min(gaps)>=0
    for field,values in [('workflow_latency_from_schedule',whole),('start_to_first_step',first),('step_to_step',gaps)]:
        h=row[field]
        assert h['count']==len(values)==sum(count for _,count in h['histogram_us'])
    if signal:assert min(signal)>=0 and row['signal_to_resumed']['count']==n
    achieved=n*steps/row['window_s']
    assert abs(achieved-row['achieved_steps_per_s'])<1e-6*max(achieved,1)
    price=float(row['price_usd_per_hour'])
    assert abs(price-manifest['usd_per_hour_ec2'])<1e-9
    assert abs(row['usd_per_million_steps']-price*1e6/(3600*achieved))<1e-8
    return dict(system=system,trial=trial,case=label,workflows=n,steps_per_workflow=steps,
        transitions=len(gaps),step_p99_us=row['step_to_step']['p99_us'],
        step_p999_us=row['step_to_step']['p999_us'],step_max_us=row['step_to_step']['max_us'],
        first_p50_us=row['start_to_first_step']['p50_us'],
        first_p999_us=row['start_to_first_step']['p999_us'],first_max_us=row['start_to_first_step']['max_us'],
        signal_p50_us=row['signal_to_resumed']['p50_us'] if signal else None,
        signal_p99_us=row['signal_to_resumed']['p99_us'] if signal else None,
        signal_p999_us=row['signal_to_resumed']['p999_us'] if signal else None,
        signal_max_us=row['signal_to_resumed']['max_us'] if signal else None,
        workflow_p50_us=row['workflow_latency_from_schedule']['p50_us'],workflow_p99_us=row['workflow_latency_from_schedule']['p99_us'],
        workflow_p999_us=row['workflow_latency_from_schedule']['p999_us'],workflow_max_us=row['workflow_latency_from_schedule']['max_us'],
        achieved_steps_per_s=achieved,usd_per_million_steps=row['usd_per_million_steps'])


def summarize(directory):
    manifest=json.loads((directory/'run.json').read_text())
    assert manifest['case']=='external' and manifest['bucket_removed'] and manifest['instance_terminated']
    rows=[]
    coverage=defaultdict(set)
    maxima={}
    partial={}
    with tarfile.open(directory/'results.tgz') as archive:
        exit_code=int(archive.extractfile('./exit-code').read())
        assert archive.extractfile('./postgres-settings.txt').read().split()==[b'on']*3
        for f in archive:
            sleep=re.fullmatch(r'./sleepers-([a-z-]+)-(\d+)/state.json',f.name)
            if sleep:
                system,t=sleep.groups();trial=int(t)
                assert system in manifest['external_systems'] and trial in range(5)
                outcome=check_sleepers(archive,f.name.rsplit("/",1)[0],manifest["sleep_count"],exit_code)
                coverage[system,trial].add("sleepers")
                if not outcome["complete"]:partial[system]=trial
                rows.append(dict(member=f.name,trial=trial,case="sleepers",**outcome))
                continue
            kill=re.fullmatch(r'./(kill-([a-z-]+)-(\d+))/result.json',f.name)
            if kill:
                _,system,t=kill.groups()
                assert system in manifest['external_systems'] and int(t) in range(5)
                coverage[system,int(t)].add("kill")
                rows.append(dict(member=f.name,system=system,trial=int(t),case="kill",
                                 **check_kill(archive,f.name.rsplit("/",1)[0])))
                continue
            match=re.fullmatch(r'./external-([a-z-]+)-(\d+)-([a-z0-9-]+)\.json',f.name)
            if not match:continue
            system,t,case=match.groups()
            trial=int(t)
            assert system in manifest['external_systems'] and trial in range(5) and case not in coverage[system,trial]
            data=json.load(archive.extractfile(f))
            coverage[system,trial].add(case)
            if case=='max':
                maxima[system,trial]=data
            else:
                row=check_row(data,case,system,trial,manifest)
                rows.append(dict(member=f.name,**row))
    if exit_code==0:
        for system in manifest['external_systems']:
            for trial in range(manifest.get("external_trials",5)):
                cases=coverage[system,trial]
                expected=set(manifest.get('external_workloads',['open','fanout','service-1','service-50','max','cost']))
                if 'cost' in expected: expected=(expected-{'cost'})|{'cost-1','cost-4','cost-16'}
                if expected=={"sleepers"} and system in partial and trial>partial[system]:continue
                assert expected<=cases
                if 'max' not in expected:continue
                sweep=[r for r in rows if r['system']==system and r['trial']==trial and r['case'].startswith('max-')]
                passed=[int(r['case'][4:]) for r in sweep if r['step_p99_us']<=1000 and r['achieved_steps_per_s']>=.99*int(r['case'][4:])*r['steps_per_workflow']]
                assert maxima[system,trial]['highest_passing_starts_per_s']==max(passed,default=None)
    return dict(run=directory.name,guest_exit=exit_code,configurations=manifest['external_systems'],measurements=rows)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    print(json.dumps(summarize(parser.parse_args().directory),indent=2))
