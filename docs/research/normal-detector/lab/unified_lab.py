"""One rule at several widths: the last w minutes' rows against this source's own pooled band of that width, for
every w in a set, one margin and one wait for all of them; a source is below when any width has been below for k
readings in a row. The 30-minute-only set at 20% and 4 readings is the recommended single-width rule, judged here
the same way as a check.

    python3 unified_lab.py nab/ unified.json

Learning is the 30-minute run's (buckets its warnings covered are not learned), and the short widths spare their
own alarms on top, as in multi_lab.
"""

import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from multi_lab import DROPS, STEP_MIN, Short, below_over, combine, first_hit
from normal_lab import CANDIDATES, Run
from run_all import DAY, MODELED, NAB, modeled, nab_series

# (widths in 5-minute buckets, readings in a row, margins)
RULES = [
    ((6,), 4, (0.2,)),
    ((1, 6), 3, (0.2, 0.3, 0.4)),
    ((1, 6), 4, (0.2, 0.3, 0.4)),
    ((1, 3, 6), 3, (0.2, 0.3, 0.4)),
    ((1, 3, 6), 4, (0.2, 0.3, 0.4)),
    ((1, 2, 3, 6), 3, (0.3, 0.4)),
]


def job(args):
    dataset, series, seed, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    base = Run(b, CANDIDATES["ratio_m20_persist4"], judge_from, seed=seed)
    base.run()
    judged = ~np.isnan(base.low)
    judged[:judge_from] = False
    months = (judged & ~lab).sum() * STEP_MIN / (60 * 24 * 30)
    held30 = base.state_at == -1
    sample = judged & ~lab & (np.arange(len(b)) % 2 == 0) & ~held30 & ~np.roll(held30, 1)
    out = dict(dataset=dataset, series=series, seed=seed, months=float(months), configs=[])
    for widths, k, margins in RULES:
        for margin in margins:
            sh = Short(b, widths, margin, base.L, judge_from)
            held, _ = sh.run(lambda B, k=k: combine(B, "any", k))
            starts = [i for i in range(1, len(b)) if held[i] and not held[i - 1]]
            cfg = dict(widths=list(widths), k=k, margin=margin, false=float(sum(1 for s in starts if not lab[s]) / months))
            for f in DROPS:
                agree = np.logical_or.reduce([below_over(b, sh.low[m], sample, m, f) for m in widths])
                d = first_hit(agree, k)
                cfg[f"by15_{f}"] = float(np.mean(d <= 15))
                cfg[f"by60_{f}"] = float(np.mean(d <= 60))
                fin = d[np.isfinite(d)]
                cfg[f"median_{f}"] = float(np.median(fin)) if len(fin) else None
            out["configs"].append(cfg)
    return out


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = []
    for name in MODELED:
        for seed in (1, 2, 3):
            b = modeled(name, seed)
            jobs.append(("modeled", name, seed, b, np.zeros(len(b), bool)))
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs.append(("nab", name, 0, b, lab))
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
