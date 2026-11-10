"""The silence rule: a source is stopped when its current quiet spell is longer than every quiet spell of its own past
28 days. n past spells give the bound 1/(n+1) on a healthy source doing that. Spells that alarmed are not learned.

    python3 quiet_lab.py nab/ quiet.json      # count series, modeled series, sparse sources and the ten logs
"""

import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from real_logs import load_logs
from run_all import DAY, MODELED, NAB, S, modeled, nab_series

HISTORY = 28 * S
MARGINS = (1.0, 1.5, 2.0)  # the current spell must exceed the longest past spell by this factor
MIN_SPELLS = 10
HORIZONS = (20, 60, 360, 1440, 4 * 1440)
STEP = 5


def spells(z, lo, hi, learned):
    """Lengths of the maximal runs of empty buckets that end in [lo, hi), excluding runs under an alarm."""
    out = []
    i = lo
    while i < hi:
        if z[i]:
            j = i
            while j < hi and z[j]:
                j += 1
            if j < hi and learned[i:j].all():  # a completed spell, none of it alarmed
                out.append(j - i)
            i = j
        else:
            i += 1
    return out


class Quiet:
    def __init__(self, b, judge_from, margin=1.0):
        self.b, self.judge_from, self.n, self.margin = b, judge_from, len(b), margin
        self.z = np.nan_to_num(b) == 0
        self.learned = np.ones(self.n, bool)
        self.held = np.zeros(self.n, bool)
        self.longest = np.full(self.n, np.nan)  # the longest learned spell of the past 28 days, in buckets
        self.count = np.zeros(self.n, int)

    def run(self):
        run = 0
        L, n = np.nan, 0
        for i in range(self.n):
            run = run + 1 if self.z[i] else 0
            if i < self.judge_from:
                continue
            if (i - self.judge_from) % 12 == 0:  # refresh hourly
                sp = spells(self.z, max(i - HISTORY, 0), i, self.learned)
                L, n = (max(sp), len(sp)) if len(sp) >= MIN_SPELLS else (np.nan, len(sp))
            self.longest[i], self.count[i] = L, n
            if not np.isnan(L) and run > self.margin * L:
                self.held[i] = True
                self.learned[i - run + 1 : i + 1] = False
        return self.held


def judge(b, lab, judge_from, margin=1.0, sample_every=2, seed=0):
    q = Quiet(b, judge_from, margin)
    q.learned &= ~lab  # a documented outage or gap is not a normal spell
    held = q.run()
    judged = ~np.isnan(q.longest)
    judged[:judge_from] = False
    clean = judged & ~lab
    months = clean.sum() * STEP / (60 * 24 * 30)
    starts = [i for i in range(1, len(b)) if held[i] and not held[i - 1]]
    false = sum(1 for s in starts if not lab[s])
    out = dict(months=float(months), false_count=false, false=float(false / months) if months else None,
               longest_h=float(np.nanmedian(q.longest[judge_from:]) * STEP / 60) if judged.any() else None,
               spells=float(np.median(q.count[judge_from:][judged[judge_from:]])) if judged.any() else None)
    # a stop at t: the alarm comes when the silence exceeds the longest spell as of t
    t0 = np.where(clean & ~held & (np.arange(len(b)) % sample_every == 0))[0]
    run_before = np.zeros(len(b), int)
    r = 0
    for i in range(len(b)):
        r = r + 1 if q.z[i] else 0
        run_before[i] = r
    delay = (np.floor(margin * q.longest[t0]) + 1 - run_before[np.maximum(t0 - 1, 0)]) * STEP
    delay = np.maximum(delay, STEP)
    for h in HORIZONS:
        out[f"by{h}_0.0"] = float(np.mean(delay <= h)) if len(t0) else None
    out["median_0.0"] = float(np.median(delay)) if len(t0) else None
    # a drop to a tenth: thin the future and find the first spell longer than the longest as of t
    rng = np.random.default_rng(seed)
    hz = HORIZONS[-1] // STEP
    hits = []
    for t in t0[::4]:
        L = int(q.longest[t])
        fut = rng.binomial(np.nan_to_num(b[t : t + hz]).astype(int), 0.1)
        r, d = run_before[t - 1] if t else 0, np.inf
        for j, v in enumerate(fut):
            r = r + 1 if v == 0 else 0
            if r > margin * L:
                d = (j + 1) * STEP
                break
        hits.append(d)
    hits = np.array(hits)
    for h in HORIZONS:
        out[f"by{h}_0.1"] = float(np.mean(hits <= h)) if len(hits) else None
    return out


def job(args):
    dataset, series, seed, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    out = dict(dataset=dataset, series=series, seed=seed, configs=[dict(judge(b, lab, judge_from, m), margin=m) for m in MARGINS])
    return out


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = [("modeled", name, seed, modeled(name, seed), np.zeros(56 * DAY, bool)) for name in MODELED for seed in (1, 2, 3)]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs.append(("nab", name, 0, b, lab))
    for name, b, lab in load_logs("real"):
        jobs.append(("logs", name, 0, b, lab))
    for name, rate in {"12 rows an hour": 1.0, "4 rows an hour": 1 / 3, "1 row an hour": 1 / 12, "1 row a day": 1 / 288}.items():
        for seed in (1, 2):
            rng = np.random.default_rng(seed)
            jobs.append(("sparse", name, seed, rng.poisson(rate, 84 * S).astype(float), np.zeros(84 * S, bool)))
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)
    for x in sorted(results, key=lambda r: (r["dataset"], r["series"], r["seed"])):
        for r in x["configs"]:
            if r["months"] == 0:
                continue
            f = lambda k: "—" if r.get(k) is None else f"{r[k]:.2f}"
            print(f'{x["dataset"]:8s} {x["series"][:28]:28s} {x["seed"]} x{r["margin"]} months {f("months")} false {f("false")} ({r["false_count"]}) longest {f("longest_h")} h stop by60 {f("by60_0.0")} by360 {f("by360_0.0")} by1440 {f("by1440_0.0")} median {f("median_0.0")} min; tenth by1440 {f("by1440_0.1")}')


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
