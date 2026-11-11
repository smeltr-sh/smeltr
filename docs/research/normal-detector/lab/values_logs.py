"""The detector on each watched value of the real logs' fields: a value is watched when its longest silence in the
learning week is at most an hour; the field-level predicate of the sender rule is scored beside it.

    python3 values_logs.py real/ values_logs.json
"""

import glob
import json
import os
import sys
from multiprocessing import Pool

import numpy as np

import extend_lab
from extend_lab import judge
from real_logs import S, load_logs

CONFIG = dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True)


def longest_zero_run(c):
    best = cur = 0
    for x in c == 0:
        cur = cur + 1 if x else 0
        best = max(best, cur)
    return best


def job(args):
    name, field, value, b, lab, judge_from = args
    out = judge(b, lab, CONFIG, judge_from, sample_every=4)
    out.update(series=name, field=field, value=value, rows_per_bucket=float(np.median(b[judge_from:])))
    return out


def main(folder, out_path):
    masks = {name: lab for name, _, lab in load_logs(folder)}
    jobs, fields = [], []
    for path in sorted(glob.glob(os.path.join(folder, "*.json"))):
        d = json.load(open(path))
        n = len(d["counts"])
        judge_from = 28 * S if n >= 35 * S else 7 * S
        week = slice(judge_from - 7 * S, judge_from)
        for field, per in d["fields"].items():
            silences = {v: longest_zero_run(np.array(c[week])) for v, c in per.items()}
            watched = [v for v, s in silences.items() if s <= 12]
            fields.append(dict(series=d["name"], field=field, values=len(per), field_watched=len(per) <= 256 and len(watched) == len(per), values_watched=len(watched)))
            for v in watched:
                jobs.append((d["name"], field, v, np.array(per[v], float), masks[d["name"]], judge_from))
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(dict(fields=fields, values=results), open(out_path, "w"), indent=0)
    for r in sorted(results, key=lambda r: (r["series"], r["field"], r["value"])):
        print(f'{r["series"][:24]:24s} {r["field"]:8s} {r["value"]:20s} rows/bucket {r["rows_per_bucket"]:6.0f} false {r["false"]:.2f} ({r["false_count"]}) stop60 {r["by60_0.0"]:.2f} stop1440 {r["by1440_0.0"]:.2f} tenth60 {r["by60_0.1"]:.2f} 30%1440 {r["by1440_0.3"]:.2f}')


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
