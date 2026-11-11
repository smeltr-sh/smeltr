"""A confidence-based normal: each 5-minute bucket says how many deviations below its time of day's typical level
it is, and the evidence adds up (CUSUM) until it is sure enough to warn. A bucket far below warns at once; a bucket
just below has to be joined by others.

    python3 confidence_lab.py nab/ confidence.json

Per bucket i, learned from the buckets of the last 7 days that no warning held:
    typical = median of the buckets within 30 min of this time of day on each past day
    l = log((rows + 1) / (typical + 1)),  m, s = median and 1.4826 x MAD of every past bucket's l
    z = (l - m) / max(s, 1 / sqrt(typical + 1))       # never surer than Poisson counting allows
    S = max(0, S - z - k), capped at h;   warn when S reaches h, clear when it falls back to 0
"""

import json
import os
import sys
import warnings
from multiprocessing import Pool

import numpy as np

from normal_lab import CANDIDATES, DAYS, NB, S, STEP_MIN, WIN, Run
from run_all import DAY, MODELED, NAB, modeled, nab_series

HORIZON = 72  # buckets a drop is followed: 6 hours
DROPS = (0.0, 0.1, 0.3, 0.5)


class Confidence:
    def __init__(self, b, k, h, judge_from):
        self.b, self.k, self.h, self.judge_from = b, k, h, judge_from
        self.n = len(b)
        self.L = ~np.isnan(b)
        self.typ = np.full(self.n, np.nan)
        self.m = np.full(self.n, np.nan)
        self.s = np.full(self.n, np.nan)
        self.S = np.zeros(self.n)  # evidence after each bucket
        self.state = np.zeros(self.n, bool)  # below normal after each bucket

    def refresh(self, i):
        a = i - DAYS * S
        X = np.where(self.L[a:i], self.b[a:i], np.nan).reshape(DAYS, S)
        stack = np.stack([np.roll(X, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            col = np.nanmedian(stack, axis=0)
        prof = np.empty(S)
        prof[(a + np.arange(S)) % S] = col
        x = self.b[a:i][self.L[a:i]]
        t = prof[np.arange(a, i)[self.L[a:i]] % S]
        ok = ~np.isnan(t)
        if ok.sum() < 3 * S:
            return prof, np.nan, np.nan
        l = np.log((x[ok] + 1) / (t[ok] + 1))
        m = float(np.median(l))
        return prof, m, float(1.4826 * np.median(np.abs(l - m)))

    def z(self, rows, i):
        typ = self.typ[i]
        return (np.log((rows + 1) / (typ + 1)) - self.m[i]) / np.maximum(self.s[i], 1 / np.sqrt(typ + 1))

    def run(self):
        S_, below, started, starts, ends = 0.0, False, None, [], []
        prof = m = s = None
        for i in range(self.judge_from, self.n):
            if (i - self.judge_from) % 12 == 0:
                prof, m, s = self.refresh(i)
            self.typ[i], self.m[i], self.s[i] = prof[i % S], m, s
            if below:
                self.L[i] = False
            if np.isnan(self.b[i]) or np.isnan(self.typ[i]) or np.isnan(m):
                self.S[i], self.state[i] = S_, below
                continue
            before = S_
            S_ = min(max(0.0, S_ - self.z(self.b[i], i) - self.k), self.h)
            if before == 0 and S_ > 0:
                started = i
            if not below and S_ >= self.h:
                below = True
                starts.append(i)
                self.L[started : i + 1] = False  # the buckets that built the evidence are not learned
            elif below and S_ == 0:
                below = False
                ends.append(i)
            self.S[i], self.state[i] = S_, below
        return starts, ends

    def delays(self, sample, f):
        """Minutes from a drop to `f` of the rows at each sampled bucket to the warning, inf if none in HORIZON."""
        t0 = np.where(sample)[0]
        t0 = t0[t0 + HORIZON < self.n]
        S_ = self.S[t0 - 1].copy()
        out = np.full(len(t0), np.inf)
        for d in range(HORIZON):
            j = t0 + d
            rows = self.b[j] * f
            z = self.z(rows, j)
            ok = ~np.isnan(z)
            S_ = np.where(ok, np.minimum(np.maximum(0.0, S_ - np.nan_to_num(z) - self.k), self.h), S_)
            hit = (S_ >= self.h) & np.isinf(out)
            out[hit] = (d + 1) * STEP_MIN
        return t0, out


def window_delays(b, low, sample, k, f):
    """The same for a window band: the k-th reading in a row whose window, with its rows after the drop cut to `f`,
    is below the band."""
    cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(b))])
    n = len(b)
    t0 = np.where(sample)[0]
    t0 = t0[t0 + HORIZON < n]
    run = np.zeros(len(t0), int)
    out = np.full(len(t0), np.inf)
    for d in range(HORIZON):
        j = t0 + d
        first = np.maximum(t0, j - WIN + 1)
        pre = np.where(d < WIN - 1, cs[t0] - cs[np.maximum(j - WIN + 1, 0)], 0.0)
        w = pre + f * (cs[j + 1] - cs[first])
        lo = low[j]
        run = np.where(np.isnan(lo), 0, np.where(w < lo, run + 1, 0))
        hit = (run >= k) & np.isinf(out)
        out[hit] = (d + 1) * STEP_MIN
    return t0, out


def summary(starts, lab, judged, delays):
    months = (judged & ~lab).sum() * STEP_MIN / (60 * 24 * 30)
    fp = sum(1 for s in starts if not lab[s])
    out = {"months": float(months), "false_below": fp / months if months else None}
    for f, (t0, d) in delays.items():
        out[f"by60_{f}"] = float(np.mean(d <= 60)) if len(d) else None
        fin = d[np.isfinite(d)]
        out[f"median_{f}"] = float(np.median(fin)) if len(fin) else None
        out[f"p90_{f}"] = float(np.percentile(np.minimum(d, 1e9), 90)) if len(d) else None  # 1e9: a tenth never warn
    return out


def job(args):
    dataset, series, seed, method, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    every2 = np.arange(len(b)) % 2 == 0
    if method[0] == "window":
        cand = method[1]
        r = Run(b, CANDIDATES[cand], judge_from, seed=seed)
        starts, _ = r.run()
        judged = ~np.isnan(r.low)
        judged[:judge_from] = False
        sample = judged & ~lab & every2 & (r.state_at == 0) & np.roll(r.state_at == 0, 1)
        k = CANDIDATES[cand].get("persist", 1)
        delays = {f: window_delays(b, r.low, sample, k, f) for f in DROPS}
        name = cand
    else:
        _, k, h = method
        c = Confidence(b, k, h, judge_from)
        starts, _ = c.run()
        judged = ~np.isnan(c.typ) & ~np.isnan(b) & ~np.isnan(c.m)
        judged[:judge_from] = False
        sample = judged & ~lab & every2 & ~np.roll(c.state, 1) & (np.roll(c.S, 1) == 0)
        delays = {f: c.delays(sample, f) for f in DROPS}
        name = f"conf_k{k}_h{h}"
    out = summary(starts, lab, judged, delays)
    out.update(dataset=dataset, series=series, seed=seed, method=name)
    return out


METHODS = [("window", "today"), ("window", "ratio_m20_persist4")] + [("conf", k, h) for k in (1, 2, 3) for h in (5, 10, 20, 40)]


def main(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = []
    for name in MODELED:
        for seed in (1, 2, 3):
            b = modeled(name, seed)
            jobs += [("modeled", name, seed, mth, b, np.zeros(len(b), bool)) for mth in METHODS]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs += [("nab", name, 0, mth, b, lab) for mth in METHODS]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])


class Empirical(Confidence):
    """Evidence measured, not assumed: how rare this bucket's l is among the past week's, in nines,
    e = -log10(share of past buckets at or below it). A bucket lower than any seen scores log10(N + 1)."""

    TAIL = 200

    def __init__(self, *a):
        super().__init__(*a)
        self.ref = np.full(self.n, -1)
        self.lows, self.grids, self.Ns = [], [], []

    def refresh(self, i):
        a = i - DAYS * S
        X = np.where(self.L[a:i], self.b[a:i], np.nan).reshape(DAYS, S)
        stack = np.stack([np.roll(X, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            col = np.nanmedian(stack, axis=0)
        prof = np.empty(S)
        prof[(a + np.arange(S)) % S] = col
        x = self.b[a:i][self.L[a:i]]
        t = prof[np.arange(a, i)[self.L[a:i]] % S]
        ok = ~np.isnan(t)
        if ok.sum() < 3 * S:
            return prof, np.nan, np.nan
        l = np.sort(np.log((x[ok] + 1) / (t[ok] + 1)))
        low = np.full(self.TAIL, np.inf)
        low[: min(self.TAIL, len(l))] = l[: self.TAIL]
        self.lows.append(low)
        self.grids.append(np.quantile(l, np.linspace(0, 1, 101)))
        self.Ns.append(len(l))
        return prof, float(len(self.Ns) - 1), 1.0  # m carries which refresh this bucket reads

    def z(self, rows, i):
        i = np.atleast_1d(i)
        rows = np.atleast_1d(rows)
        typ = self.typ[i]
        l = np.log((rows + 1) / (typ + 1))
        ref = np.nan_to_num(self.m[i], nan=0).astype(int)
        low = np.array(self.lows)[ref]
        grid = np.array(self.grids)[ref]
        N = np.array(self.Ns)[ref]
        in_tail = l < low[:, -1]
        cnt = np.where(in_tail, (low <= l[:, None]).sum(1), N * np.array([np.interp(v, g, np.linspace(0, 1, 101)) for v, g in zip(l, grid)]))
        e = -np.log10((cnt + 1) / (N + 1))
        out = np.where(np.isnan(l) | np.isnan(self.m[i]), np.nan, -e)
        return out if out.size > 1 else float(out[0])


def job2(args):
    dataset, series, seed, (k, h), b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    every2 = np.arange(len(b)) % 2 == 0
    c = Empirical(b, k, h, judge_from)
    starts, _ = c.run()
    judged = ~np.isnan(c.typ) & ~np.isnan(b) & ~np.isnan(c.m)
    judged[:judge_from] = False
    sample = judged & ~lab & every2 & ~np.roll(c.state, 1) & (np.roll(c.S, 1) == 0)
    delays = {f: c.delays(sample, f) for f in DROPS}
    out = summary(starts, lab, judged, delays)
    out.update(dataset=dataset, series=series, seed=seed, method=f"emp_k{k}_h{h}")
    return out


def main2(root, out_path):
    labels = json.load(open(os.path.join(root, "labels.json")))
    grid = [(k, h) for k in (0.5, 1.0, 1.5) for h in (4, 8, 16)]
    jobs = []
    for name in MODELED:
        for seed in (1, 2, 3):
            b = modeled(name, seed)
            jobs += [("modeled", name, seed, g, b, np.zeros(len(b), bool)) for g in grid]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs += [("nab", name, 0, g, b, lab) for g in grid]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job2, jobs))
    json.dump(results, open(out_path, "w"), indent=0)
