"""The baseline on every drop fraction the detector is scored on, including the drop to 30%.

    python3 baseline_drops.py nab/ baseline_drops.csv
"""

import csv
import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from normal_lab import CANDIDATES, Run, score
from run_all import DAY, MODELED, NAB, modeled, nab_series

DROPS = (0.0, 0.1, 0.3, 0.5)


def job(args):
    dataset, series, seed, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    r = Run(b, CANDIDATES["today"], judge_from, seed=seed)
    below, above = r.run()
    out = score(r, below, above, lab, drops=DROPS)
    out.update(dataset=dataset, series=series, seed=seed)
    return out


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = [("modeled", name, seed, modeled(name, seed), np.zeros(56 * DAY, bool)) for name in MODELED for seed in (1, 2, 3)]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs.append(("nab", name, 0, b, lab))
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    keys = ["dataset", "series", "seed", "months", "false_below"] + [f"caught_{f}" for f in DROPS]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, keys)
        w.writeheader()
        for r in sorted(results, key=lambda r: (r["dataset"], r["series"], r["seed"])):
            w.writerow({k: r.get(k, "") for k in keys})


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
