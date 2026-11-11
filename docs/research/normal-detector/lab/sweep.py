"""False warnings against missed drops: the ratio band over a grid of margins and waits.

    python3 sweep.py nab/ sweep.csv
"""

import csv
import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from normal_lab import Run, score
from run_all import DAY, MODELED, NAB, modeled, nab_series

MARGINS = (0.0, 0.1, 0.2, 0.3, 0.4)
WAITS = (1, 2, 3, 4)  # readings in a row outside before a warning
DROPS = (0.0, 0.1, 0.3, 0.5)  # rows cut to this share: a stop, a 90% drop, a 70% drop, a halving


def job(args):
    dataset, series, seed, margin, wait, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    cfg = {"kind": "ratio", "p": 0.999, "margin": margin, "persist": wait}
    r = Run(b, cfg, judge_from, seed=seed)
    below, above = r.run()
    out = score(r, below, above, lab, drops=DROPS)
    out.update(dataset=dataset, series=series, seed=seed, margin=margin, wait=wait)
    return out


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    data = [("modeled", name, seed, modeled(name, seed), None) for name in MODELED for seed in (1, 2, 3)]
    data += [("nab", name, 0, *nab_series(root, name, labels)) for name in NAB]
    jobs = [
        (ds, name, seed, m, w, b, lab if lab is not None else np.zeros(len(b), bool))
        for ds, name, seed, b, lab in data
        for m in MARGINS
        for w in WAITS
    ]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    keys = ["dataset", "series", "seed", "margin", "wait", "months", "false_below", "incidents"] + [f"caught_{f}" for f in DROPS]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, keys, extrasaction="ignore")
        w.writeheader()
        for r in sorted(results, key=lambda r: (r["dataset"], r["series"], r["seed"], r["margin"], r["wait"])):
            w.writerow(r)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
