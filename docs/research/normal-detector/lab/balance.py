"""Pick a margin and wait from sweep.csv: the frontier of false warnings against missed drops, and the point each
way of weighing them picks.

    python3 balance.py sweep.csv
"""

import csv
import sys
from collections import defaultdict

import numpy as np

rows = list(csv.DictReader(open(sys.argv[1])))
DROPS = ("caught_0.0", "caught_0.1", "caught_0.3", "caught_0.5")
cell = defaultdict(list)
for r in rows:
    cell[(r["dataset"], float(r["margin"]), int(r["wait"]))].append(r)


def mean(rs, k):
    v = [float(r[k]) for r in rs if r[k] not in ("", "nan")]
    return float(np.mean(v)) if v else np.nan


for ds in ("nab", "modeled"):
    pts = []
    for (d, m, w), rs in sorted(cell.items()):
        if d != ds:
            continue
        fp = mean(rs, "false_below")
        caught = {k: mean(rs, k) for k in DROPS}
        # A real drop is taken to be a stop, a 90% drop or a 70% drop, equally often.
        recall = np.mean([caught["caught_0.0"], caught["caught_0.1"], caught["caught_0.3"]])
        pts.append(dict(m=m, w=w, fp=fp, recall=recall, **caught))
    print(f"\n## {ds}: margin, wait, false/month, caught stop/90%/70%/50%, recall (stop..70%)")
    for p in sorted(pts, key=lambda p: p["fp"]):
        dom = any(q["fp"] <= p["fp"] and q["recall"] >= p["recall"] and (q["fp"], q["recall"]) != (p["fp"], p["recall"]) for q in pts)
        print(f"{p['m']:4.0%} {p['w']}  {p['fp']:7.2f}  " + " ".join(f"{p[k]:4.0%}" for k in DROPS) + f"  {p['recall']:4.0%}{'' if dom else '  frontier'}")
    print(f"\n## {ds}: picks. d = real drops per source a month")
    for d in (0.25, 1.0, 4.0):
        for K in (10, 100):
            best = min(pts, key=lambda p: p["fp"] + K * d * (1 - p["recall"]))
            print(f"linear  d={d:<5} K={K:<4} -> {best['m']:.0%} margin, wait {best['w']}: {best['fp']:.2f} false/month, recall {best['recall']:.0%}, cost {best['fp'] + K * d * (1 - best['recall']):.2f}")
        # Fatigue: people act on a warning as often as warnings are right, so drops acted on = caught x precision.
        acted = lambda p: d * p["recall"] * (d * p["recall"]) / (d * p["recall"] + p["fp"])
        best = max(pts, key=acted)
        prec = d * best["recall"] / (d * best["recall"] + best["fp"])
        print(f"fatigue d={d:<5}        -> {best['m']:.0%} margin, wait {best['w']}: {best['fp']:.2f} false/month, recall {best['recall']:.0%}, precision {prec:.0%}, acted on {acted(best) / d:.0%} of drops")
