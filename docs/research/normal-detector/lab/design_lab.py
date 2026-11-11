"""The detector of the paper, Section 4, as one self-consistent replay: its own alarms set its own learning set.

    python3 design_lab.py nab/ design.json            # the 35 source series: each width alone and both
    python3 design_lab.py senders design_values.json  # the detector on each sender of the 10 mixes

For a bucket t and width w, S_w(t) is the rows over the w buckets ending at t. The typical level m(s) of a time
of day is the median of the learned buckets within +-30 minutes of s over the past seven days, refreshed hourly;
the typical level of a window is the sum of m over its buckets. The ratios of the past week's learned windows,
(S_w + 1) / (M_w + 1), give the band's lower edge as their 0.05% empirical quantile (linear interpolation),
times (1 - delta). A reading is below when S_w(t) < low_w(t). An alarm is raised at the k-th consecutive below
reading at any width and ends at the first t at which no width has k in a row. Buckets under an alarm, and the
k - 1 readings that waited, with every bucket of the windows those readings judged, are not learned. Above-normal
alarms learn, so they are not modeled here. A width is judged once it has three days of learned ratios.
"""

import json
import math
import sys
import warnings
from multiprocessing import Pool

import numpy as np

from multi_lab import DROPS, below_over, first_hit
from normal_lab import DAYS, NB, S, STEP_MIN
from run_all import DAY, MODELED, NAB, modeled, nab_series

TAIL = 0.0005
REFRESH = 12  # buckets between band refreshes: hourly


class Design:
    def __init__(self, b, widths, k, margin, judge_from):
        self.b, self.widths, self.k, self.margin, self.judge_from = b, tuple(widths), k, margin, judge_from
        self.n = len(b)
        self.L = ~np.isnan(b)  # the learning set
        self.low = {w: np.full(self.n, np.nan) for w in widths}
        self.held = np.zeros(self.n, bool)

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
        for w in self.widths:
            r = (np.convolve(x, np.ones(w), "valid") + 1) / (np.convolve(t, np.ones(w), "valid") + 1)
            r = r[~np.isnan(r)]
            q[w] = float(np.quantile(r, TAIL, method="linear")) if len(r) >= 3 * S else np.nan
        return prof, q

    def run(self):
        cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(self.b))])
        nanc = np.concatenate([[0], np.cumsum(np.isnan(self.b))])
        run = {w: 0 for w in self.widths}
        wmax = max(self.widths)
        prof = q = None
        for i in range(self.judge_from, self.n):
            if (i - self.judge_from) % REFRESH == 0:
                prof, q = self.refresh(i)
            below_any = False
            for w in self.widths:
                if np.isnan(q[w]) or i - w + 1 < 0:
                    run[w] = 0
                    continue
                typ = prof[np.arange(i - w + 1, i + 1) % S].sum()
                self.low[w][i] = max((typ + 1) * q[w] - 1, 0.0) * (1 - self.margin)
                if nanc[i + 1] - nanc[i + 1 - w] == 0 and cs[i + 1] - cs[i + 1 - w] < self.low[w][i]:
                    run[w] += 1
                else:
                    run[w] = 0
                if run[w] >= self.k:
                    below_any = True
            if below_any:
                if not self.held[i - 1]:  # raised now: the readings that waited are not learned either
                    self.L[max(i - self.k + 1 - wmax + 1, 0) : i + 1] = False
                else:
                    self.L[max(i - wmax + 1, 0) : i + 1] = False
                self.held[i] = True
        return self.held


def judge(b, lab, widths, k, margin, judge_from):
    d = Design(b, widths, k, margin, judge_from)
    held = d.run()
    wide = max(widths)
    judged = ~np.isnan(d.low[wide])
    judged[:judge_from] = False
    clean = judged & ~lab
    months = clean.sum() * STEP_MIN / (60 * 24 * 30)
    starts = [i for i in range(1, len(b)) if held[i] and not held[i - 1]]
    false = sum(1 for s in starts if not lab[s])
    sample = clean & (np.arange(len(b)) % 2 == 0) & ~held & ~np.roll(held, 1)
    out = dict(widths=list(widths), k=k, margin=margin, months=float(months), false_count=false,
               false=float(false / months) if months else None)
    for f in DROPS:
        agree = np.logical_or.reduce([below_over(b, d.low[w], sample, w, f) for w in widths])
        t = first_hit(agree, k)
        out[f"by15_{f}"] = float(np.mean(t <= 15))
        out[f"by60_{f}"] = float(np.mean(t <= 60))
        fin = t[np.isfinite(t)]
        out[f"median_{f}"] = float(np.median(fin)) if len(fin) else None
    return out


RULES = [((1,), 3, 0.3), ((6,), 3, 0.3), ((1, 6), 3, 0.3), ((1, 6), 3, 0.2), ((1, 6), 3, 0.4), ((1, 6), 4, 0.3), ((6,), 4, 0.2)]


def job(args):
    dataset, series, seed, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    return dict(dataset=dataset, series=series, seed=seed,
                configs=[judge(b, lab, w, k, m, judge_from) for w, k, m in RULES])


def sender_job(args):
    from value_lab import series

    mix, seed = args
    seen = series(mix, seed)
    out = dict(mix=mix, seed=seed, senders=len(seen), configs=[])
    for widths, k, margin in RULES[:3]:
        fp, months, d_all = 0, 0.0, {f: [] for f in DROPS}
        for b in seen:
            d = Design(b, widths, k, margin, 28 * DAY)
            held = d.run()
            judged = ~np.isnan(d.low[max(widths)])
            judged[: 28 * DAY] = False
            fp += sum(1 for i in range(1, len(b)) if held[i] and not held[i - 1])
            months += judged.sum() * STEP_MIN / (60 * 24 * 30)
            sample = judged & (np.arange(len(b)) % 4 == 0) & ~held & ~np.roll(held, 1)
            for f in DROPS:
                agree = np.logical_or.reduce([below_over(b, d.low[w], sample, w, f) for w in widths])
                d_all[f].append(first_hit(agree, k))
        cfg = dict(widths=list(widths), k=k, margin=margin, false_count=fp, months=months, false=fp / months)
        for f in DROPS:
            t = np.concatenate(d_all[f])
            cfg[f"by15_{f}"] = float(np.mean(t <= 15))
            cfg[f"by60_{f}"] = float(np.mean(t <= 60))
            fin = t[np.isfinite(t)]
            cfg[f"median_{f}"] = float(np.median(fin)) if len(fin) else None
        out["configs"].append(cfg)
    return out


def main(root, out_path):
    if root == "senders":
        from value_lab import MIXES

        jobs = [(mix, seed) for mix in MIXES for seed in (1, 2)]
        with Pool(4) as pool:
            results = list(pool.imap_unordered(sender_job, jobs))
    else:
        labels = json.load(open(f"{root}/labels.json"))
        jobs = [("modeled", name, seed, modeled(name, seed), np.zeros(56 * S, bool)) for name in MODELED for seed in (1, 2, 3)]
        for name in NAB:
            b, lab = nab_series(root, name, labels)
            jobs.append(("nab", name, 0, b, lab))
        with Pool(4) as pool:
            results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
