"""The normal, per value: the one band rule at several widths (unified_lab), applied to each watched value's own
rows as the tally sees them (SAMPLE of the rows), on the sender mixes of share_lab and silence_lab. A sender's
death is a stop of its own series, so the delay to warn is the rule's own delay on a stop (drop to 0.0).

    python3 value_lab.py '[1, 6]' 3 0.3 value.json
"""

import json
import sys
from multiprocessing import Pool

import numpy as np

from multi_lab import DROPS, STEP_MIN, Short, below_over, combine, first_hit
from normal_lab import CANDIDATES, S, Run
from run_all import DAY
from share_lab import MIXES as SHARE_MIXES, SAMPLE, senders
from silence_lab import MIXES as SILENCE_MIXES

MIXES = {**SHARE_MIXES, **SILENCE_MIXES}


def series(mix, seed):
    X = senders(MIXES[mix], seed)
    if mix == "10 sensors, nightly quiet":
        hour = ((np.arange(X.shape[1]) / S) % 1) * 24
        X = X * ((hour < 1) | (hour > 5))[None, :]
    rng = np.random.default_rng(1000 + seed)
    return rng.binomial(X.astype(int), SAMPLE).astype(float)


def job(args):
    mix, seed, widths, k, margin = args
    seen = series(mix, seed)
    judge_from = 28 * DAY
    fp, months, d_all = 0, 0.0, {f: [] for f in DROPS}
    for b in seen:
        base = Run(b, CANDIDATES["ratio_m20_persist4"], judge_from, seed=seed)
        base.run()
        judged = ~np.isnan(base.low)
        judged[:judge_from] = False
        held30 = base.state_at == -1
        sample = judged & (np.arange(len(b)) % 4 == 0) & ~held30 & ~np.roll(held30, 1)
        sh = Short(b, widths, margin, base.L, judge_from)
        held, _ = sh.run(lambda B, k=k: combine(B, "any", k))
        fp += sum(1 for i in range(1, len(b)) if held[i] and not held[i - 1])
        months += judged.sum() * STEP_MIN / (60 * 24 * 30)
        for f in DROPS:
            agree = np.logical_or.reduce([below_over(b, sh.low[m], sample, m, f) for m in widths])
            d_all[f].append(first_hit(agree, k))
    out = dict(mix=mix, seed=seed, widths=list(widths), k=k, margin=margin, senders=len(seen), false_per_value_month=fp / months)
    for f in DROPS:
        d = np.concatenate(d_all[f])
        out[f"by15_{f}"] = float(np.mean(d <= 15))
        out[f"by60_{f}"] = float(np.mean(d <= 60))
        fin = d[np.isfinite(d)]
        out[f"median_{f}"] = float(np.median(fin)) if len(fin) else None
    return out


def main(widths, k, margin, out_path):
    jobs = [(mix, seed, tuple(widths), k, margin) for mix in MIXES for seed in (1, 2)]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)
    for r in sorted(results, key=lambda r: r["mix"]):
        print(f"{r['mix']:30s} s{r['seed']} FP/value-month {r['false_per_value_month']:7.2f} stop by15 {r['by15_0.0']:.2f} by60 {r['by60_0.0']:.2f} med {r['median_0.0']} half by60 {r['by60_0.5']:.2f}")


if __name__ == "__main__":
    main(json.loads(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), sys.argv[4])
