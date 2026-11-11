"""The same band at several widths: the 30-minute window judged as recommended (ratios + 20%, 4 in a row), and
the last 5, 10 or 15 minutes each judged at once against their own pooled band of that width.

    python3 multi_lab.py nab/ multi.json

A width m learns, from the last 7 days' learned buckets, the ratios (rows over m buckets + 1) / (typical over
those m + 1); its band low is the typical now x the 0.05th percentile of those ratios x (1 - margin). Nothing is
assumed about their distribution: the band is a rank among this source's own past.
"""

import json
import os
import sys
import warnings
from multiprocessing import Pool

import numpy as np

from normal_lab import CANDIDATES, DAYS, NB, S, STEP_MIN, Run
from run_all import DAY, MODELED, NAB, modeled, nab_series

HORIZON = 72
DROPS = (0.0, 0.1, 0.3, 0.5)
TAIL = 0.0005  # the lower half of a 99.9% band
WIDTHS = ((3,), (2, 3), (1, 2, 3))
MARGINS = (0.2, 0.4, 0.6, 0.8)


class Short:
    """Bands for the short widths, learned from the buckets the 30-minute run learned and its own alarms spared."""

    def __init__(self, b, widths, margin, learned, judge_from):
        self.b, self.widths, self.margin, self.judge_from = b, widths, margin, judge_from
        self.n = len(b)
        self.L = learned.copy()
        self.low = {m: np.full(self.n, np.nan) for m in widths}
        self.typ = np.full(self.n, np.nan)

    def refresh(self, i):
        a = i - DAYS * S
        X = np.where(self.L[a:i], self.b[a:i], np.nan).reshape(DAYS, S)
        stack = np.stack([np.roll(X, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            col = np.nanmedian(stack, axis=0)
        prof = np.empty(S)
        prof[(a + np.arange(S)) % S] = col
        x = np.where(self.L[a:i], self.b[a:i], np.nan)
        t = prof[np.arange(a, i) % S]
        q = {}
        for m in self.widths:
            l = np.log((np.convolve(x, np.ones(m), "valid") + 1) / (np.convolve(t, np.ones(m), "valid") + 1))
            l = l[~np.isnan(l)]
            q[m] = float(np.quantile(l, TAIL)) if len(l) >= 3 * S else np.nan
        return prof, q

    def run(self, rule):
        """Fills low per width; returns per bucket whether the short path alarms: `rule` combines the widths' own
        below readings (each: rows under its band), a function of the (n, widths) matrix."""
        cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(self.b))])
        nanc = np.concatenate([[0], np.cumsum(np.isnan(self.b))])
        B = np.zeros((self.n, len(self.widths)), bool)
        below = np.zeros(self.n, bool)
        prof = q = None
        for i in range(self.judge_from, self.n):
            if (i - self.judge_from) % 12 == 0:
                prof, q = self.refresh(i)
            self.typ[i] = prof[i % S]
            for m in self.widths:
                typ_m = np.nansum(self.typ[i - m + 1 : i + 1]) if i - m + 1 >= self.judge_from else np.nan
                if np.isnan(q[m]) or np.isnan(typ_m):
                    continue
                self.low[m][i] = max((typ_m + 1) * np.exp(q[m]) - 1, 0.0) * (1 - self.margin)
                if nanc[i + 1] - nanc[i + 1 - m] == 0 and cs[i + 1] - cs[i + 1 - m] < self.low[m][i]:
                    B[i, self.widths.index(m)] = True
            below[i] = rule(B[max(i - 8, 0) : i + 1])[-1] if i >= self.judge_from + 3 else False  # k is at most 3
            if below[i]:
                self.L[i - max(self.widths) + 1 : i + 1] = False
        return below, B


def delays(b, low, sample, w, k, f):
    """Minutes from a drop to `f` of the rows at each sampled bucket to the k-th reading in a row whose w-bucket
    sum is below `low`; inf when none within HORIZON."""
    cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(b))])
    t0 = np.where(sample)[0]
    t0 = t0[t0 + HORIZON < len(b)]
    run = np.zeros(len(t0), int)
    out = np.full(len(t0), np.inf)
    for d in range(HORIZON):
        j = t0 + d
        first = j - w + 1
        pre = np.where(first < t0, cs[t0] - cs[np.maximum(first, 0)], 0.0)
        post = cs[j + 1] - cs[np.maximum(first, t0)]
        lo = low[j]
        run = np.where(np.isnan(lo), 0, np.where(pre + f * post < lo, run + 1, 0))
        hit = (run >= k) & np.isinf(out)
        out[hit] = (d + 1) * STEP_MIN
    return out


def below_over(b, low, sample, w, f):
    """(samples, HORIZON): whether the w-bucket sum ending d steps after a drop to `f` at each sample is below."""
    cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(b))])
    t0 = np.where(sample)[0]
    t0 = t0[t0 + HORIZON < len(b)]
    out = np.zeros((len(t0), HORIZON), bool)
    for d in range(HORIZON):
        j = t0 + d
        first = j - w + 1
        pre = np.where(first < t0, cs[t0] - cs[np.maximum(first, 0)], 0.0)
        post = cs[j + 1] - cs[np.maximum(first, t0)]
        lo = low[j]
        out[:, d] = ~np.isnan(lo) & (pre + f * post < lo)
    return out


def first_hit(M, k):
    """Minutes to the k-th True in a row along each row of M, inf when none."""
    run = np.zeros(len(M), int)
    out = np.full(len(M), np.inf)
    for d in range(M.shape[1]):
        run = np.where(M[:, d], run + 1, 0)
        hit = (run >= k) & np.isinf(out)
        out[hit] = (d + 1) * STEP_MIN
    return out


# (widths, how the widths combine, readings in a row, margins)
RULES = [
    ((1, 2, 3), "any", 1, (0.6,)),
    ((1, 2, 3), "all", 1, (0.2, 0.4, 0.6)),
    ((2, 3), "all", 1, (0.2, 0.4, 0.6)),
    ((3,), "any", 2, (0.2, 0.4, 0.6)),
    ((1,), "any", 3, (0.2, 0.4, 0.6)),
    ((1, 2, 3), "all", 2, (0.2, 0.4)),
]


def combine(B, how, k):
    """Per step, whether the short path alarms: the widths agree (`how`), k steps in a row."""
    agree = B.all(1) if how == "all" else B.any(1)
    if k == 1:
        return agree
    run = np.zeros(len(agree), int)
    out = np.zeros(len(agree), bool)
    for i in range(len(agree)):
        run[i] = run[i - 1] + 1 if agree[i] and i > 0 else int(agree[i])
        out[i] = run[i] >= k
    return out


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
    d30 = {f: delays(b, base.low, sample, 6, 4, f) for f in DROPS}
    out = dict(dataset=dataset, series=series, seed=seed, months=float(months), configs=[])
    starts30 = [i for i in range(1, len(b)) if held30[i] and not held30[i - 1]]
    out["base"] = dict(false=sum(1 for s in starts30 if not lab[s]) / months, **{f"by15_{f}": float(np.mean(d30[f] <= 15)) for f in DROPS},
                       **{f"by60_{f}": float(np.mean(d30[f] <= 60)) for f in DROPS}, **{f"median_{f}": float(np.median(d30[f][np.isfinite(d30[f])])) if np.isfinite(d30[f]).any() else None for f in DROPS})
    for widths, how, k, margins in RULES:
        for margin in margins:
            sh = Short(b, widths, margin, base.L, judge_from)
            rule = lambda B, how=how, k=k: combine(B, how, k)
            below, _ = sh.run(rule)
            held = held30 | below
            starts = [i for i in range(1, len(b)) if held[i] and not held[i - 1]]
            false = sum(1 for s in starts if not lab[s]) / months
            extra = sum(1 for s in starts if not lab[s] and not held30[s]) / months
            cfg = dict(widths=list(widths), how=how, k=k, margin=margin, false=float(false), extra=float(extra))
            for f in DROPS:
                Ms = [below_over(b, sh.low[m], sample, m, f) for m in widths]
                agree = np.logical_and.reduce(Ms) if how == "all" else np.logical_or.reduce(Ms)
                d = np.minimum(d30[f], first_hit(agree, k))
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
