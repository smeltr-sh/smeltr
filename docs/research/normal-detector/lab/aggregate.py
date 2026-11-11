"""Summaries of results.csv: per candidate, per feed."""

import csv
import sys
from collections import defaultdict

import numpy as np

rows = list(csv.DictReader(open(sys.argv[1])))
for r in rows:
    for k in ("months", "false_below", "incidents", "above_info", "caught_0.0", "caught_0.5", "caught_0.1"):
        r[k] = float(r[k]) if r[k] not in ("", "nan") else np.nan

ORDER = ["today", "today_fixed", "today_m10", "pooled_m10", "ratio", "ratio_m10", "nb_m10", "ratio_m10_half3", "ratio_m10_groups", "ratio_m20", "ratio_m10_persist3", "ratio_m20_persist3", "ratio_m20_persist3_groups"]


def group(r):
    if r["dataset"] == "modeled":
        return r["series"]
    s = r["series"]
    if "Twitter" in s:
        return "tweets (10)"
    if "taxi" in s:
        return "nyc taxi"
    return "aws (3)"


cells = defaultdict(list)
for r in rows:
    cells[(group(r), r["candidate"])].append(r)

groups = list(dict.fromkeys(group(r) for r in rows))


def stat(rs, k, how=np.mean):
    v = [r[k] for r in rs if not np.isnan(r[k])]
    return how(v) if v else np.nan


for metric, how in (("false_below", np.mean), ("incidents", np.mean), ("caught_0.0", np.mean), ("caught_0.1", np.mean), ("caught_0.5", np.mean)):
    print(f"\n## {metric}")
    print("candidate".ljust(27) + "".join(g[:14].rjust(15) for g in groups))
    for c in ORDER:
        line = c.ljust(27)
        for g in groups:
            v = stat(cells.get((g, c), []), metric, how)
            line += (f"{v:15.1f}" if metric in ("false_below", "incidents") else f"{v:15.0%}") if not np.isnan(v) else "              -"
        print(line)

# per tweet series, false_below, for the spread
print("\n## tweets, false_below per series")
tw = sorted({r["series"] for r in rows if "Twitter" in r["series"]})
print("candidate".ljust(27) + "".join(s.split("_")[-1].rjust(7) for s in tw))
for c in ORDER:
    print(c.ljust(27) + "".join(f"{stat(cells_s, 'false_below'):7.1f}" for cells_s in ([r for r in rows if r['series'] == s and r['candidate'] == c] for s in tw)))
sil = [r for r in rows if r["candidate"] == "ratio_m10_groups"]
print("\n## day groups: share of days grouped, mean silhouette")
for r in sil:
    print(f"{r['series'][:40]:40s} seed {r['seed']}  grouped {r['grouped_days']}  silhouette {r['silhouette']}")
