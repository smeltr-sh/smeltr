"""Stops on sparse sources: a few rows an hour, one an hour, one a day, at widths up to a week.

    python3 sparse_lab.py sparse_final.json            # two short widths; four widths with 7 days; four widths with 28 days behind the long ones
    python3 sparse_lab.py sparse_history.json history  # four widths with 7 and with 28 days of history behind every width, and a week-wide fifth width
"""

import json
import sys
from multiprocessing import Pool

import numpy as np

import extend_lab
from extend_lab import judge
from run_all import S

DAYS = 84
extend_lab.HORIZON = 10 * S  # ten days
extend_lab.HORIZONS = (20, 60, 360, 1440, 2 * 1440, 4 * 1440, 10 * 1440)
RATES = {"12 rows an hour": 1.0, "4 rows an hour": 1 / 3, "1 row an hour": 1 / 12, "1 row a day": 1 / 288}  # per 5-minute bucket
CONFIGS = [dict(widths=(1, 6), k=4, margin=0.2, minrows=10, wtyp=True), dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True), dict(widths=(1, 6, 72, 288), k=4, margin=0.2, minrows=10, wtyp=True)]


HISTORY = [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, minrows=10, wtyp=True, history=7), dict(widths=(1, 6, 72, 288), k=4, margin=0.2, minrows=10, wtyp=True, history=28),
           dict(widths=(1, 6, 72, 288, 2016), k=4, margin=0.2, minrows=10, wtyp=True, history=28)]


def job(args):
    name, seed = args
    rng = np.random.default_rng(seed)
    b = rng.poisson(RATES[name], DAYS * S).astype(float)
    lab = np.zeros(len(b), bool)
    out = [judge(b, lab, c, 28 * S, sample_every=12) for c in (HISTORY if len(sys.argv) > 2 else CONFIGS)]
    return dict(dataset="sparse", series=name, seed=seed, configs=out)


def main(out_path):
    jobs = [(name, seed) for name in RATES for seed in (1, 2)]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)
    for r in sorted(results, key=lambda r: (list(RATES).index(r["series"]), r["seed"])):
        for c in r["configs"]:
            print(r["series"], r["seed"], c["widths"], f'false {c["false"]}', " ".join(f'stop@{h}={c[f"by{h}_0.0"]:.2f}' for h in extend_lab.HORIZONS), "median", c["median_0.0"])


if __name__ == "__main__":
    main(sys.argv[1])
