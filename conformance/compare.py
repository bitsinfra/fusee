"""Requires every runner's normalized journals to be identical."""
import json
import sys

names = sys.argv[1:]
docs = [json.load(open(n)) for n in names]
base = docs[0]
bad = 0
for name, doc in zip(names[1:], docs[1:]):
    for scenario in sorted(set(base) | set(doc)):
        a, b = base.get(scenario), doc.get(scenario)
        if a != b:
            bad += 1
            print(f"DIFF {scenario}: {names[0]} vs {name}")
            if not isinstance(a, list) or not isinstance(b, list):
                # A missing scenario, or an answer that is not a journal.
                print(f"    {json.dumps(a)}\n    {json.dumps(b)}")
                continue
            for i in range(max(len(a), len(b))):
                x = a[i] if i < len(a) else None
                y = b[i] if i < len(b) else None
                if x != y:
                    print(f"  record {i}:\n    {json.dumps(x)}\n    {json.dumps(y)}")
if bad:
    print(f"{bad} scenario(s) differ")
    sys.exit(1)
print(f"{len(base)} journals identical across {len(names)} SDKs")
