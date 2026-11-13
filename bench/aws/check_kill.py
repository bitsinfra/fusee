"""Independently correlate the application receipts with the outside effect ledger."""
import json


def check(result, events):
    campaign = result["campaign"]
    rows = result["scheduled"]
    assert len(rows) == result["workflows"] and len({r["receipt"] for r in rows}) == len(rows)
    killed, ready = result["worker_events"]
    assert killed["event"] == "SIGKILL" and ready["event"] == "replacement_ready"
    assert killed["at_ms"] < ready["at_ms"]
    started = result["start_ms"]
    cut = started + killed["at_ms"]
    nsteps = result["steps_per_workflow"]
    pending = set()
    for index, row in enumerate(rows):
        expected = campaign + "-" + str(index)
        assert row["input"] == dict(run=expected,value=index+100,steps=nsteps)
        assert row["scheduledMs"] == index*1000/result["offered_starts_per_s"]
        assert started+row["scheduledMs"] <= row["firstSentAt"]+.01 <= row["ackAt"]+.02
        assert row["result"] == index+100+nsteps and row["ackAt"] <= row["doneAt"]
        if row["ackAt"] <= cut < row["doneAt"]:
            pending.add(expected)
    observations = {r["run"]:r["ms"] for r in result["recovered"]}
    assert pending == set(observations) and len(pending) == killed["acknowledged_not_finished"] > 0
    assert result["kill_to_all_resumed_or_finished_ms"] == max(observations.values())
    finished=max(r["doneAt"]-cut for r in rows if r["input"]["run"] in pending)
    assert abs(result["kill_to_pending_finished_ms"]-finished)<1e-5
    assert all(0 <= observations[r["input"]["run"]] <= r["doneAt"]-cut+.1
               for r in rows if r["input"]["run"] in pending)
    values = {(row["input"]["run"],i):row["input"]["value"]+i for row in rows for i in range(nsteps)}
    values.update({(campaign+"-warm",i):50+i for i in range(nsteps)})
    earliest={r["input"]["run"]:r["doneAt"] for r in rows if r["input"]["run"] in pending}
    seen=set()
    for event in events:
        key = (event["run"],event["seq"])
        assert key in values and event["value"] == values[key]
        assert event["generation"] in (0,1)
        if event["generation"] == 1:
            assert event["at_ms"] >= cut
            if event["run"] in earliest:earliest[event["run"]]=min(earliest[event["run"]],event["at_ms"])
        seen.add(key)
    assert set(values)==seen and len(seen)==result["distinct_external_keys"]
    assert len(events)==result["external_calls"] and len(events)-len(seen)==result["repeated_calls"]
    if result.get("witness_version",1)>=2:
        assert all(abs(earliest[run]-cut-observations[run])<1e-5 for run in pending)
    return dict(acknowledged=len(rows),pending_at_kill=len(pending),external_calls=len(events),
                distinct_keys=len(seen),kill_to_all_resumed_or_finished_ms=max(observations.values()),
                kill_to_all_pending_finished_ms=result["kill_to_pending_finished_ms"])


def from_archive(archive, folder):
    with archive.extractfile(folder+"/result.json") as result, archive.extractfile(folder+"/external.jsonl") as stream:
        data=json.load(result)
        row=check(data,[json.loads(line) for line in stream])
        if folder+"/handler-returns.jsonl" in archive.getnames():
            expected={r["input"]["run"]:r["result"] for r in [data["warm"],*data["scheduled"]]}
            seen=set()
            with archive.extractfile(folder+"/handler-returns.jsonl") as returns:
                for line in returns:
                    item=json.loads(line)
                    assert item["run"] in expected and expected[item["run"]]==item["value"]
                    seen.add(item["run"])
            assert seen==set(expected)
            row["result_observation"]="engine terminal status and externally witnessed handler result"
        return row
