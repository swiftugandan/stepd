#!/usr/bin/env python3
"""Coverage of the REAL simulator. A suite that explores nothing passes everything."""
import simulation as S
from collections import Counter

total = Counter()
feat = Counter()
states = Counter()
n = int(__import__("sys").argv[1]) if len(__import__("sys").argv) > 1 else 400

for seed in range(n):
    r = S.simulate(seed)
    assert r is None, r
    w = S.LAST_WORLD["w"]
    for f in w.features: feat[f] += 1
    for k, v in w.counters.items(): total[k] += v
    for run in w.runs.values(): states[run.status.value] += 1

print(f"coverage over {n} seeds of the real simulator\n")
print("fault activation:")
for f, c in feat.most_common(): print(f"   {f:22} {c}")
print("\nengine events exercised:")
for k, v in total.most_common(): print(f"   {k:26} {v}")
print("\nrun states reached:", dict(states))

gaps = [k for k, v in total.items() if v == 0]
print("\nNEVER EXERCISED:", gaps if gaps else "none — every path was hit")
