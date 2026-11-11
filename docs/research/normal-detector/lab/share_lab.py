"""Part 2, the share watch: a source's output has a field whose values split its rows (observer.hostname, a
partition, a region). Each value's share of the rows is learned and a share that falls under its band warns,
which catches one sender going quiet when the total does not.

    python3 share_lab.py share.json

Modeled: K senders, each with its own level, daily shape (some offset by hours, as regions are) and drifting
noise; the leader sees the tally of one batch per heartbeat interval, so the counts it sums per 5-minute bucket
are a sample of the rows (SAMPLE of them). A value's share over the last 30 minutes is judged every 5 minutes
against the band of its shares at that time of day over the past week, pooled as ratios to the typical share,
99.9%, a margin, and a wait, exactly as the total's band. Scenarios inject one sender dying, two dying, one
halving; the total's own band (part 1) is judged alongside to show what it sees.
"""

import json
import sys
from multiprocessing import Pool

import numpy as np

from normal_lab import CANDIDATES, S, STEP_MIN, WIN, Run
from run_all import DAY

DAYS = 56
JUDGE = 28 * DAY
SAMPLE = 0.5  # share of rows the tally sees: one batch per 1.5 s heartbeat interval, batches about every 0.75 s
HORIZON = 72


def senders(spec, seed):
    """Rows per 5-minute bucket per sender: (K, n)."""
    rng = np.random.default_rng(seed)
    n = DAYS * S
    t = np.arange(n) / S
    out = []
    for level, offset_h, sigma in spec:
        hour = (t % 1) * 24
        shape = 1 + 0.7 * np.sin(2 * np.pi * (hour - 9 - offset_h) / 24)
        rho = 0.9
        e = np.empty(n)
        e[0] = rng.normal(0, sigma)
        xi = rng.normal(0, sigma * np.sqrt(1 - rho**2), n)
        for k in range(1, n):
            e[k] = rho * e[k - 1] + xi[k]
        out.append(rng.poisson(level * shape * np.exp(e - sigma**2 / 2)).astype(float))
    return np.array(out)


# A fleet reading about 70 rows a second in all: 2,000 rows per 5-minute bucket per sender for ten senders.
MIXES = {
    "10 equal, same hours": [(2000.0, 0, 0.3)] * 10,
    "10 equal, 3 regions": [(2000.0, 0, 0.3)] * 4 + [(2000.0, 6, 0.3)] * 3 + [(2000.0, -8, 0.3)] * 3,
    "3 unequal 60/30/10": [(12000.0, 0, 0.3), (6000.0, 0, 0.3), (2000.0, 0, 0.3)],
    "10 equal, noisy senders": [(2000.0, 0, 0.6)] * 10,
    "30 small senders": [(2000.0 / 3, 0, 0.3)] * 30,
}
# (which senders are hit, factor) from a random step in the judged period
SCENARIOS = {"one dies": (1, 0.0), "two die": (2, 0.0), "one halves": (1, 0.5)}


class ShareJudge:
    """A value's share of the rows over the last 30 minutes, judged every 5 minutes against the band of its shares
    at this time of day over the past week, pooled as ratios to the typical share: the total's own band, on a share."""

    def __init__(self, counts, total, judge_from, p=0.999, margin=0.2, persist=4):
        W_tot = np.convolve(total, np.ones(WIN), "full")[: len(total)]
        W_cnt = np.convolve(counts, np.ones(WIN), "full")[: len(counts)]
        W_tot[: WIN - 1] = np.nan
        self.share = np.where(W_tot >= 20, W_cnt / np.maximum(W_tot, 1), np.nan)  # too few rows seen: not judged
        self.n, self.judge_from, self.tail, self.margin, self.persist = len(counts), judge_from, (1 - p) / 2, margin, persist
        self.L = ~np.isnan(self.share)
        self.low = np.full(self.n, np.nan)

    def run(self):
        from normal_lab import DAYS, NB
        starts, state, pending, prof_at = [], False, 0, -1
        for i in range(self.judge_from, self.n):
            if state:
                self.L[i] = False
            if i - prof_at >= 12:
                prof_at = i
                a = i - DAYS * S
                X = np.where(self.L[a:i], self.share[a:i], np.nan).reshape(DAYS, S)
                stack = np.stack([np.roll(X, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
                col = np.nanmedian(stack, axis=0)
                prof = np.empty(S)
                prof[(a + np.arange(S)) % S] = col
                P = np.arange(a, i - WIN + 1)
                ok = self.L[P] & ~np.isnan(prof[P % S])
                r = (self.share[P][ok] + 0.001) / (prof[P[ok] % S] + 0.001)
                q = np.quantile(r, self.tail) if len(r) > 3 * S else np.nan
            if np.isnan(self.share[i]) or np.isnan(q) or np.isnan(prof[i % S]):
                pending = 0
                continue
            self.low[i] = max((prof[i % S] + 0.001) * q - 0.001, 0.0) * (1 - self.margin)
            below = self.share[i] < self.low[i]
            if below and not state:
                pending += 1
                if pending >= self.persist:
                    state = True
                    starts.append(i)
                    self.L[i - self.persist + 1 : i + 1] = False
            elif not below:
                pending = 0
                if state:
                    state = False
        return starts


def job(args):
    mix, seed, margin, persist = args
    X = senders(MIXES[mix], seed)
    K, n = X.shape
    rng = np.random.default_rng(1000 + seed)
    seen = rng.binomial(X.astype(int), SAMPLE).astype(float)  # what the tally samples
    total_seen = seen.sum(0)
    total = X.sum(0)
    out = dict(mix=mix, seed=seed, margin=margin, persist=persist, senders=K)
    fp, months, lows = 0, 0.0, []
    for k in range(K):
        j = ShareJudge(seen[k], total_seen, JUDGE, margin=margin, persist=persist)
        fp += len(j.run())
        judged = ~np.isnan(j.low)
        judged[:JUDGE] = False
        months = max(months, judged.sum() * STEP_MIN / (60 * 24 * 30))
        lows.append(np.nanmedian(j.low[judged] / np.maximum(j.share[judged], 1e-9)))
    out["false_share_per_source_month"] = fp / months if months else None
    out["share_low_over_share"] = float(np.nanmedian(lows))
    tot = Run(total, CANDIDATES["ratio_m20_persist4"], JUDGE)
    tb, _ = tot.run()
    out["false_total_per_month"] = len(tb) / months if months else None
    for name, (hit, f) in SCENARIOS.items():
        delays_share, delays_total = [], []
        for t0 in rng.choice(np.arange(JUDGE + DAY, n - HORIZON - 1), 16, replace=False):
            Y = X.copy()
            Y[:hit, t0:] *= f
            seen2 = seen.copy()
            seen2[:hit, t0:] = rng.binomial(Y[:hit, t0:].astype(int), SAMPLE)
            tot2 = seen2.sum(0)
            best = np.inf
            for k in range(hit):
                d = [s for s in ShareJudge(seen2[k], tot2, JUDGE, margin=margin, persist=persist).run() if t0 <= s < t0 + HORIZON]
                if d:
                    best = min(best, (d[0] - t0 + 1) * STEP_MIN)
            delays_share.append(best)
            r = Run(Y.sum(0), CANDIDATES["ratio_m20_persist4"], JUDGE)
            below, _ = r.run()
            d = [s for s in below if t0 <= s < t0 + HORIZON]
            delays_total.append((d[0] - t0 + 1) * STEP_MIN if d else np.inf)
        ds, dt = np.array(delays_share), np.array(delays_total)
        out[name] = dict(share_by60=float(np.mean(ds <= 60)), share_median=float(np.median(ds[np.isfinite(ds)])) if np.isfinite(ds).any() else None,
                         total_by60=float(np.mean(dt <= 60)))
    return out


def main(out_path):
    jobs = [(mix, seed, m, p) for mix in MIXES for seed in (1, 2) for m, p in ((0.2, 3), (0.2, 4), (0.4, 4))]
    with Pool(4) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1])
