"""How sure is a drop, without assuming a bell curve: a fast path that warns when the last 5, 10, 15 or 30
minutes are rarer than H nines below this source's own past week.

Rarity is measured, not assumed. Each scale m (1, 2, 3 or 6 buckets) learns, from the last 7 days' learned
buckets, its log ratios l = log((rows + 1) / (typical + 1)) over m buckets in a row. The body of their spread is
what was seen; the lower tail past its 2nd percentile u is fitted with a generalized Pareto distribution
(peaks over threshold, as SPOT does), so a value lower than any seen still has a rarity:

    P(l' <= l) = 0.02 * (1 + xi * (u - l) / sigma) ** (-1 / xi)        for l < u
    nines = -log10 P

Memory needs no model: a 30-minute dip is judged against the past week's 30-minute sums, natural dips included.

    python3 tail_lab.py nab/ tail.json
"""

import json
import os
import sys
import warnings
from multiprocessing import Pool

import numpy as np

from normal_lab import DAYS, NB, S, STEP_MIN
from run_all import DAY, MODELED, NAB, modeled, nab_series

SCALES = (1, 2, 3, 6)
TAIL = 0.02
HORIZON = 72
DROPS = (0.0, 0.1, 0.3, 0.5)
HS = (3.0, 4.0, 5.0, 6.0, 8.0)


def gpd_fit(y):
    """Method-of-moments generalized Pareto fit to exceedances y > 0: (xi, sigma), xi clamped to [-0.4, 0.9]."""
    mean, var = float(np.mean(y)), float(np.var(y))
    if var <= 0 or mean <= 0:
        return 0.0, max(mean, 1e-6)
    xi = 0.5 * (1 - mean * mean / var)
    xi = min(max(xi, -0.4), 0.9)
    sigma = mean * (1 - xi)
    return xi, max(sigma, 1e-6)


def nines(l, u, xi, sigma):
    """-log10 P(l' <= l) in the fitted tail; 0 above its threshold, where nothing is rare enough to matter here."""
    y = np.maximum(u - l, 0.0)
    if abs(xi) < 1e-6:
        tail = y / sigma / np.log(10)
    else:
        base = np.maximum(1 + xi * y / sigma, 1e-300)
        tail = np.log10(base) / xi
        if xi < 0:  # a bounded tail: past its end the value is impossible under the fit
            tail = np.where(1 + xi * y / sigma <= 0, 50.0, tail)
    return np.where(l < u, -np.log10(TAIL) + tail, 0.0)


class Tail:
    def __init__(self, b, judge_from):
        self.b, self.judge_from, self.n = b, judge_from, len(b)
        self.L = ~np.isnan(b)
        self.typ = np.full(self.n, np.nan)
        self.par = {m: np.full((self.n, 3), np.nan) for m in SCALES}  # u, xi, sigma read at each bucket
        self.alarm = {H: np.zeros(self.n, bool) for H in HS}

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
        par = {}
        for m in SCALES:
            xs = np.convolve(x, np.ones(m), "valid")  # NaN where any bucket is unlearned
            ts = np.convolve(t, np.ones(m), "valid")
            l = np.log((xs + 1) / (ts + 1))
            l = l[~np.isnan(l)]
            if len(l) < 3 * S:
                par[m] = (np.nan, np.nan, np.nan)
                continue
            u = float(np.quantile(l, TAIL))
            y = u - l[l < u]
            par[m] = (u, *gpd_fit(y)) if len(y) >= 10 else (np.nan, np.nan, np.nan)
        return prof, par

    def evidence(self, rows_by_scale, j):
        """The most nines any scale reads at bucket j, given each scale's rows ending there."""
        best = np.zeros(np.shape(j))
        for m in SCALES:
            u, xi, sigma = self.par[m][j].T
            typ_m = sum(self.typ[j - q] for q in range(m))
            l = np.log((rows_by_scale[m] + 1) / (typ_m + 1))
            e = np.zeros(np.shape(j))
            ok = ~(np.isnan(l) | np.isnan(u))
            if ok.any():
                # per-bucket fit parameters: vectorize over distinct fits
                for key in set(zip(u[ok], xi[ok], sigma[ok])):
                    sel = ok & (u == key[0]) & (xi == key[1]) & (sigma == key[2])
                    e[sel] = nines(l[sel], *key)
            best = np.maximum(best, e)
        return best

    def run(self):
        prof = par = None
        held = {H: False for H in HS}
        quiet_after = {H: -1 for H in HS}
        starts = {H: [] for H in HS}
        cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(self.b))])
        nanc = np.concatenate([[0], np.cumsum(np.isnan(self.b))])
        for i in range(self.judge_from, self.n):
            if (i - self.judge_from) % 12 == 0:
                prof, par = self.refresh(i)
            self.typ[i] = prof[i % S]
            for m in SCALES:
                self.par[m][i] = par[m]
            rows = {m: (cs[i + 1] - cs[i + 1 - m]) if nanc[i + 1] - nanc[i + 1 - m] == 0 else np.nan for m in SCALES}
            e = float(self.evidence({m: np.array([rows[m]]) for m in SCALES}, np.array([i]))[0])
            for H in HS:
                if e >= H:
                    self.alarm[H][i] = True
                    if not held[H] and i > quiet_after[H]:
                        starts[H].append(i)
                    held[H] = True
                    quiet_after[H] = i + 12  # alarms within the hour after one belong to the same warning
                else:
                    held[H] = False
            if e >= min(HS):
                self.L[i - max(SCALES) + 1 : i + 1] = False  # what raised an alarm is not learned
        return starts

    def delays(self, sample, f, H):
        t0 = np.where(sample)[0]
        t0 = t0[t0 + HORIZON < self.n]
        cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(self.b))])
        out = np.full(len(t0), np.inf)
        for d in range(HORIZON):
            j = t0 + d
            rows = {}
            for m in SCALES:
                first = j - m + 1
                pre = np.where(first < t0, cs[t0] - cs[np.maximum(first, 0)], 0.0)
                post = cs[j + 1] - cs[np.maximum(first, t0)]
                rows[m] = pre + f * post
            e = self.evidence(rows, j)
            hit = (e >= H) & np.isinf(out)
            out[hit] = (d + 1) * STEP_MIN
        return t0, out


def job(args):
    dataset, series, seed, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    t = Tail(b, judge_from)
    starts = t.run()
    judged = ~np.isnan(t.typ) & ~np.isnan(b)
    judged[:judge_from] = False
    months = (judged & ~lab).sum() * STEP_MIN / (60 * 24 * 30)
    out = dict(dataset=dataset, series=series, seed=seed, months=float(months))
    sample = judged & ~lab & (np.arange(len(b)) % 2 == 0)
    for H in HS:
        out[f"false_{H}"] = sum(1 for s in starts[H] if not lab[s]) / months if months else None
        s_H = sample & ~np.roll(t.alarm[H], 1)
        for f in DROPS:
            _, d = t.delays(s_H, f, H)
            out[f"by60_{H}_{f}"] = float(np.mean(d <= 60)) if len(d) else None
            out[f"by15_{H}_{f}"] = float(np.mean(d <= 15)) if len(d) else None
            fin = d[np.isfinite(d)]
            out[f"median_{H}_{f}"] = float(np.median(fin)) if len(fin) else None
    return out


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = [("modeled", name, seed, modeled(name, seed), None) for name in MODELED for seed in (1, 2, 3)]
    jobs = [(d, n, s, b, np.zeros(len(b), bool)) for d, n, s, b, _ in jobs]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs.append(("nab", name, 0, b, lab))
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
