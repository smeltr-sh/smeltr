"""Should warned windows be learned? The recommended band with above-normal windows learned, below-normal ones
learned at a weight, or every window feeding the typical level but not the band's tails.

    python3 learn.py nab/ learn.json
"""

import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from normal_lab import CANDIDATES, S, STEP_MIN, Run, score
from patterns import kill_delays
from run_all import DAY, MODELED, NAB, modeled, nab_series

CANDS = ("today", "ratio_m20_persist4", "m20p4_learn_above", "m20p4_below_w10", "m20p4_below_w01", "m20p4_median_all")
STEP_AT = 35  # day a stepped feed moves to its new level


def stepped(shape, factor, seed, days=56):
    """`modeled`'s steady or daily feed, its level times `factor` from day STEP_AT on: a new normal, or a death."""
    rng = np.random.default_rng(seed)
    n = days * S
    t = np.arange(n) / S
    hour = (t % 1) * 24
    base = np.ones(n) if shape == "steady" else 1 + 0.7 * np.sin(2 * np.pi * (hour - 9) / 24)
    sigma = 0.1 if shape == "steady" else 0.3
    rho = 0.9
    e = np.empty(n)
    e[0] = rng.normal(0, sigma)
    xi = rng.normal(0, sigma * np.sqrt(1 - rho**2), n)
    for k in range(1, n):
        e[k] = rho * e[k - 1] + xi[k]
    lam = 200.0 * base * np.where(t >= STEP_AT, factor, 1.0) * np.exp(e - sigma**2 / 2)
    return rng.poisson(lam).astype(float)


STEPS = [(f"{shape}, {word} on day {STEP_AT}", shape, f) for shape in ("steady", "daily")
         for word, f in (("halves", 0.5), ("doubles", 2.0), ("stops", 0.0))]


def job(args):
    dataset, series, seed, cand, b, lab, step = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    cfg = CANDIDATES[cand]
    r = Run(b, cfg, judge_from, seed=seed)
    below, above = r.run()
    out = dict(dataset=dataset, series=series, seed=seed, cand=cand)
    if step is None:
        out.update(score(r, below, above, lab))
        judged = ~np.isnan(r.low)
        judged[:judge_from] = False
        sample = judged & ~lab & (np.arange(len(b)) % 6 == 0)
        d = kill_delays(b, r.W, r.low, sample, cfg.get("persist", 1))
        d = d[sample & (np.arange(len(b)) + 72 < len(b))]
        out.update(kill_median=float(np.nanmedian(d)) if np.isfinite(d).any() else None,
                   kill_by60=float(np.mean(d <= 60)) if len(d) else None)
    else:
        at = STEP_AT * DAY
        st = r.state_at[at:]
        want = -1 if step < 1 else 1  # the side the step pushes the source to
        held = st == want
        # When the warning stops for good: hours after the step of the last step it held, None if it holds at the end.
        out.update(share_warned=float(held.mean()),
                   cleared_hours=None if held[-1] else (float((np.where(held)[0].max() + 1) * STEP_MIN / 60) if held.any() else 0.0),
                   warnings=int(sum(1 for s in (below if want < 0 else above) if s >= at)))
    return out


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = []
    for name in MODELED:
        for seed in (1, 2, 3):
            b = modeled(name, seed)
            jobs += [("modeled", name, seed, c, b, np.zeros(len(b), bool), None) for c in CANDS]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs += [("nab", name, 0, c, b, lab, None) for c in CANDS]
    for name, shape, f in STEPS:
        for seed in (1, 2, 3):
            b = stepped(shape, f, seed)
            jobs += [("step", name, seed, c, b, np.zeros(len(b), bool), f) for c in CANDS]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
