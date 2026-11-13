"""Recompute successful and deadline-limited sleep populations from their event streams."""
import gzip
import json


def check(state, observations, count):
    assert state["requested"]==count and state["deadline_lead_s"]>0 and state["window"]>0
    ack=set();entered={};woken={};done=set();receipts=set();errors=[];throttles=0
    for item in observations:
        i=item["i"];assert isinstance(i,int) and 0<=i<count
        event=item["event"]
        if event=="ack":
            assert i not in ack and item["receipt"] not in receipts
            ack.add(i);receipts.add(item["receipt"])
        elif event=="entered":entered[i]=min(entered.get(i,item["at"]),item["at"])
        elif event=="woke":woken[i]=min(woken.get(i,item["at"]),item["at"])
        elif event=="done":
            assert i not in done and i in woken and i in ack and item["value"]==i
            done.add(i)
        elif event=="throttled":throttles+=1
        elif event=="error":errors.append(item["message"])
        else:raise AssertionError("unexpected timer event "+event)
    assert throttles==state.get("explicit_throttles",0)
    deadline=state["deadline_ms"]
    assert len(ack)==state["acknowledged"] and len(woken)==state["woken"] and len(done)==state["results_confirmed"]
    assert sum(t<deadline for t in entered.values())==state["entered_before_deadline"]
    if state["complete"]:
        assert count==len(done)==len(woken)==len(entered)==len(ack)
        assert state["sample_time_ms"]<deadline and state["entered_before_deadline"]==count
        lateness=sorted(t-deadline for t in woken.values())
        assert state["lateness_ms"]=={p:lateness[round((count-1)*q)] for p,q in [("p50",.5),("p99",.99),("max",1)]}
        assert sum(t<0 for t in lateness)==state["early_wakes"]
        memory=state["sleeping"]
    else:
        cause=state["error"]
        if cause=="population did not enter its timers before the common deadline":
            memory=state["at_deadline"]
        elif state["system"]=="hatchet" and "admin rate limit exceeded" in cause:
            # A witness may stop before appending the rejected command.
            assert not errors or cause in errors
            memory=None
        elif cause=="worker exited during timer trial":
            memory=None
        else:raise AssertionError(f"unclassified timer error: {cause}")

    if memory and "processes" in memory:
        assert memory["pss_bytes"]==sum(p["pss_bytes"] for p in memory["processes"])>0
        assert memory["rss_bytes"]==sum(p["rss_bytes"] for p in memory["processes"])>=memory["pss_bytes"]
    return {k:state[k] for k in ("system","requested","offered","acknowledged","entered_before_deadline","results_confirmed","complete",
            "lateness_ms","early_wakes","error") if k in state}|{"memory":memory}


def from_archive(archive,folder,count,guest_exit):
    with archive.extractfile(folder+"/state.json") as f:state=json.load(f)
    if "complete" not in state:
        # The guest lifetime can end while gzip and the per-ID witness are still open.
        # Keep the saved snapshot separate from independently checked trials.
        assert guest_exit==124 and state["requested"]==count
        return dict(system=state["system"],requested=count,complete=False,
                    error="guest timed out before the witness closed",last_saved_state=state)
    with archive.extractfile(folder+"/observations.jsonl.gz") as f, gzip.open(f,"rt") as lines:
        checked=check(state,(json.loads(line) for line in lines),count)
    if checked.get("error")=="worker exited during timer trial":
        worker_log="./external-"+folder.removeprefix("./sleepers-")+".log"
        with archive.extractfile(worker_log) as f:log=f.read()
        checked["worker_log"]=worker_log
        if b"Allocation failed - JavaScript heap out of memory" in log:
            checked["worker_failure"]="V8 heap exhausted"
    return checked
