"""One model: each 5-minute bucket's evidence is how rare it is against this source's own past week at its time of
day, measured as a rank (nines = -log10 of the share of past buckets at or below it), with nothing assumed about
the distribution. Evidence past a floor k adds up over a run of low buckets; the run warns when it reaches H and
ends when a bucket is not low. A bucket emptier than any in the week is as rare as a week can show (about 3.3
nines), so a dead busy source crosses H = 10 in three readings; a bucket at the 10th percentile adds nothing past
a floor of 1, so a mild dip never warns; a bucket at the 1st percentile adds one, so a sustained moderate drop
warns after about ten.

    python3 rank_lab.py nab/ rank.json
"""

import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from confidence_lab import job2
from run_all import MODELED, NAB, modeled, nab_series

GRID = [(k, h) for k in (0.5, 1.0, 1.5, 2.0) for h in (6, 8, 10, 14)]


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = []
    for name in MODELED:
        for seed in (1, 2, 3):
            b = modeled(name, seed)
            jobs += [("modeled", name, seed, g, b, np.zeros(len(b), bool)) for g in GRID]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs += [("nab", name, 0, g, b, lab) for g in GRID]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job2, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
