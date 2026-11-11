"""How fast a killed source warns, and what false warnings and misses look like.

    python3 patterns.py nab/ patterns.json
"""

import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from normal_lab import CANDIDATES, S, STEP_MIN, WIN, Run
from run_all import DAY, MODELED, NAB, modeled, nab_series

CANDS = ("today", "ratio_m20_persist3", "ratio_m20_persist4")
HORIZON = 6 * 60 // STEP_MIN  # how long a kill is followed before it counts as never caught


def kill_delays(b, W, low, judged, k):
    """Minutes from a kill at the start of each judged bucket to the warning: the k-th reading in a row whose window,
    holding only the rows read before the kill, is below the band. NaN when no warning within HORIZON."""
    cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(b))])
    n = len(b)
    out = np.full(n, np.nan)
    starts = np.where(judged)[0]
    starts = starts[starts + HORIZON < n]
    for t0 in starts:
        run = 0
        for d in range(HORIZON):
            j = t0 + d
            lo = low[j]
            if np.isnan(lo):
                run = 0
                continue
            pre = cs[t0] - cs[j - WIN + 1] if d < WIN - 1 else 0.0  # rows of the window read before the kill
            run = run + 1 if pre < lo else 0
            if run >= k:
                out[t0] = (d + 1) * STEP_MIN
                break
    return out


def job(args):
    dataset, series, seed, cand, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    cfg = CANDIDATES[cand]
    r = Run(b, cfg, judge_from, seed=seed)
    below, above = r.run()
    k = cfg.get("persist", 1)
    W = r.W
    judged = ~np.isnan(r.low)
    judged[:judge_from] = False
    clean = judged & ~lab
    # Sample kills every 3rd bucket: plenty, and three times faster.
    sample = clean.copy()
    sample[np.arange(len(b)) % 3 != 0] = False
    delays = kill_delays(b, W, r.low, sample, k)
    # Each time of day's typical window over the whole series, to tell quiet hours from busy ones.
    tod = np.arange(len(b)) % S
    prof = np.array([np.nanmedian(W[tod == t]) if np.any(~np.isnan(W[tod == t])) else np.nan for t in range(S)])
    quiet = prof[tod] < 0.5 * np.nanmedian(prof)
    fp = []
    ends = r.below_ends + [len(b)] * (len(below) - len(r.below_ends))
    for s, e in zip(below, ends):
        if lab[s]:
            continue
        depth = float(np.nanmin(W[s - k + 1 : s + 1] / r.low[s - k + 1 : s + 1]))
        fp.append(dict(at=int(s), tod=int(s % S), dow=int((s // S) % 7), minutes=int((e - s) * STEP_MIN), depth=depth,
                       quiet=bool(quiet[s]), typical=float(prof[s % S])))
    ks = np.where(~np.isnan(delays) | sample)[0]
    kills = [dict(at=int(t), delay=None if np.isnan(delays[t]) else float(delays[t]), quiet=bool(quiet[t]),
                  low_zero=bool(r.low[t] <= 0), typical=float(prof[t % S])) for t in np.where(sample & (np.arange(len(b)) + HORIZON < len(b)))[0]]
    return dict(dataset=dataset, series=series, seed=seed, cand=cand, fp=fp, kills=kills,
                months=float(clean.sum() * STEP_MIN / (60 * 24 * 30)),
                trace=dict(b=np.nan_to_num(b, nan=-1).tolist(), low=np.nan_to_num(r.low, nan=-1).tolist(),
                           high=np.nan_to_num(r.high, nan=-1).tolist()) if dataset == "nab" else None)


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = []
    for name in MODELED:
        for seed in (1, 2, 3):
            b = modeled(name, seed)
            jobs += [("modeled", name, seed, c, b, np.zeros(len(b), bool)) for c in CANDS]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs += [("nab", name, 0, c, b, lab) for c in CANDS]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
