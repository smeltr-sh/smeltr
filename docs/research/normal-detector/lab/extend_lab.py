"""Extensions of the detector of design_lab.py, each one rule added to the detector as written.

    python3 extend_lab.py nab/ long extend_long.json       # a third, long width (1 h, 3 h, 6 h, 1 day); horizons to a day
    python3 extend_lab.py nab/ minrows extend_minrows.json # a width is judged only when its typical window holds B rows
    python3 extend_lab.py senders minrows extend_minrows_values.json
    python3 extend_lab.py nab/ grid extend_grid.json       # margins x waits at the two widths, for the held-out check
    python3 extend_lab.py nab/ groups extend_groups.json   # the typical level learned per group of weekdays

Below-normal buckets are unlearned per width: an alarm at width w unlearns the w buckets of each reading that
fired, so a long width does not take a day of learning with it when the 5-minute width alarms.
"""

import json
import os
import sys
import warnings
from multiprocessing import Pool

import numpy as np

from design_lab import REFRESH, TAIL, Design
from multi_lab import DROPS, first_hit
from normal_lab import DAYS, NB, S, STEP_MIN, kmeans, silhouette
from run_all import DAY, MODELED, NAB, modeled, nab_series

HORIZON = 300  # buckets after a drop over which detection is scored: 25 hours
HORIZONS = (15, 20, 30, 60, 360, 1440)  # minutes


def below_over(b, low, sample, w, f, horizon):
    cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(b))])
    t0 = np.where(sample)[0]
    t0 = t0[t0 + horizon < len(b)]
    out = np.zeros((len(t0), horizon), bool)
    for d in range(horizon):
        j = t0 + d
        first = j - w + 1
        pre = np.where(first < t0, cs[t0] - cs[np.maximum(first, 0)], 0.0)
        post = cs[j + 1] - cs[np.maximum(first, t0)]
        lo = low[j]
        out[:, d] = ~np.isnan(lo) & (pre + f * post < lo)
    return out


class Extended(Design):
    def __init__(self, b, widths, k, margin, judge_from, minrows=0.0, groups=False, history=DAYS):
        super().__init__(b, widths, k, margin, judge_from)
        self.minrows, self.groups, self.history = minrows, groups, history  # history: days of ratios behind each width's quantile
        self.longhistory = 0  # when set, the days of ratios behind widths over 30 minutes
        self.wtyp = False  # the typical window is the median of the same window on the past seven days
        self.rng = np.random.default_rng(0)
        self.group_of_weekday = np.zeros(7, int)
        self.groups_at = -1
        self.silhouettes = []

    # ---- day groups: the typical level per group of weekdays, over 28 days ----------------------------------------
    def refresh_groups(self, i):
        day = i // S
        if self.groups_at == day:
            return
        self.groups_at = day
        X, wd = [], []
        for d in range(max(day - 28, 0), day):
            bb, ll = self.b[d * S : (d + 1) * S], self.L[d * S : (d + 1) * S]
            if np.isnan(bb).mean() > 0.1 or (~ll).mean() > 0.1:
                continue
            X.append(np.log1p(np.nan_to_num(bb).reshape(24, -1).sum(1)))
            wd.append(d % 7)
        X, wd = np.array(X), np.array(wd)
        best = (0.2, None)
        if len(X) >= 14:
            for kk in (2, 3):
                lab = kmeans(X, kk, self.rng)
                s = silhouette(X, lab)
                if s >= best[0]:
                    best = (s, lab)
        if best[1] is None:
            self.group_of_weekday = np.zeros(7, int)
            self.silhouettes.append(0.0)
        else:
            lab = best[1]
            self.group_of_weekday = np.array([np.bincount(lab[wd == w]).argmax() if (wd == w).any() else 0 for w in range(7)])
            self.silhouettes.append(best[0])

    def profile(self, i):
        """prof[g, s]: the typical level of time of day s for day group g; and a = the first bucket the profile reads."""
        if not self.groups:
            a = i - DAYS * S
            X = np.where(self.L[a:i], self.b[a:i], np.nan).reshape(DAYS, S)
            stack = np.stack([np.roll(X, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                col = np.nanmedian(stack, axis=0)
            prof = np.empty((1, S))
            prof[0, (a + np.arange(S)) % S] = col
            return prof
        self.refresh_groups(i)
        a = max(i - 28 * S, 0)
        a -= a % S  # whole days
        P = np.arange(a, i)
        g = self.group_of_weekday[(P // S) % 7]
        x = np.where(self.L[P], self.b[P], np.nan)
        G = int(self.group_of_weekday.max()) + 1
        prof = np.full((G, S), np.nan)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for gg in range(G):
                xs = np.where(g == gg, x, np.nan)
                pad = (-len(xs)) % S
                Xg = np.concatenate([xs, np.full(pad, np.nan)]).reshape(-1, S)
                stack = np.stack([np.roll(Xg, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
                prof[gg, (a + np.arange(S)) % S] = np.nanmedian(stack, axis=0)
        # a group with no learned day at some time of day takes the ungrouped level there
        allp = np.full(S, np.nan)
        Xa = np.concatenate([x, np.full((-len(x)) % S, np.nan)]).reshape(-1, S)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            allp[(a + np.arange(S)) % S] = np.nanmedian(np.stack([np.roll(Xa, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S), axis=0)
        prof = np.where(np.isnan(prof), allp[None, :], prof)
        return prof

    def typical_at(self, prof, u):
        """The typical level of bucket(s) u."""
        u = np.asarray(u)
        g = self.group_of_weekday[(u // S) % 7] if self.groups else np.zeros(u.shape, int)
        return prof[g, u % S]

    def window_profile(self, i, w):
        """prof[s]: the median of the learned windows of width w ending at time of day s on the past seven days,
        taking in the windows within 30 minutes of s as the typical level does."""
        a = i - DAYS * S
        lo = a - w + 1  # the first bucket a window of the profile reaches back to; before the series it is unknown
        x = np.where(self.L[max(lo, 0) : i], self.b[max(lo, 0) : i], np.nan)
        if lo < 0:
            x = np.concatenate([np.full(-lo, np.nan), x])
        sw = np.convolve(x, np.ones(w), "valid").reshape(DAYS, S)  # sw[d, j] ends at bucket a + d S + j
        stack = np.stack([np.roll(sw, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            col = np.nanmedian(stack, axis=0)
        prof = np.empty(S)
        prof[(a + np.arange(S)) % S] = col
        return prof

    def typical_window(self, prof, w, i):
        """The typical level of the window of width w ending at bucket i."""
        if self.wtyp:
            return prof[w][i % S]
        return self.typical_at(prof, np.arange(i - w + 1, i + 1)).sum()

    def refresh(self, i):
        prof = {w: self.window_profile(i, w) for w in self.widths} if self.wtyp else self.profile(i)
        q = {}
        for w in self.widths:
            days = self.history if w <= 6 or not self.longhistory else self.longhistory
            a = max(i - days * S, 0)
            x = np.where(self.L[a:i], self.b[a:i], np.nan)
            sw = np.convolve(x, np.ones(w), "valid")
            if self.wtyp:
                t = prof[w][np.arange(a + w - 1, i) % S]
            else:
                t = np.convolve(self.typical_at(prof, np.arange(a, i)), np.ones(w), "valid")
            r = (sw + 1) / (t + 1)
            r = r[~np.isnan(r)]
            q[w] = float(np.quantile(r, TAIL, method="linear")) if len(r) >= 3 * S else np.nan
        return prof, q

    def run(self):
        cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(self.b))])
        nanc = np.concatenate([[0], np.cumsum(np.isnan(self.b))])
        run = {w: 0 for w in self.widths}
        prof = q = None
        for i in range(self.judge_from, self.n):
            if (i - self.judge_from) % REFRESH == 0:
                prof, q = self.refresh(i)
            fired = []
            for w in self.widths:
                if np.isnan(q[w]) or i - w + 1 < 0:
                    run[w] = 0
                    continue
                typ = self.typical_window(prof, w, i)
                if typ < self.minrows:
                    run[w] = 0
                    continue
                self.low[w][i] = max((typ + 1) * q[w] - 1, 0.0) * (1 - self.margin)
                if nanc[i + 1] - nanc[i + 1 - w] == 0 and cs[i + 1] - cs[i + 1 - w] < self.low[w][i]:
                    run[w] += 1
                else:
                    run[w] = 0
                if run[w] >= self.k:
                    fired.append(w)
            if fired:
                back = 0 if self.held[i - 1] else self.k - 1  # raised now: the readings that waited are not learned either
                for w in fired:
                    self.L[max(i - back - w + 1, 0) : i + 1] = False
                self.held[i] = True
        return self.held


LADDER = (1, 2, 3, 6, 12, 36, 72, 288)


class Rows(Extended):
    """Widths defined in rows: the fast width is the shortest rung whose typical window holds B rows, the slow width
    the shortest holding 6B. The band is learned on every rung; each reading picks its two."""

    def __init__(self, b, k, margin, judge_from, rows, floor=False, history=DAYS):
        super().__init__(b, LADDER, k, margin, judge_from, history=history)
        self.rows, self.floor = rows, floor  # floor: the widths only lengthen from 5 and 30 minutes
        self.picked = np.full((self.n, 2), -1)

    def run(self):
        cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(self.b))])
        nanc = np.concatenate([[0], np.cumsum(np.isnan(self.b))])
        run = {w: 0 for w in self.widths}
        prof = q = None
        for i in range(self.judge_from, self.n):
            if (i - self.judge_from) % REFRESH == 0:
                prof, q = self.refresh(i)
            typ = {w: self.typical_at(prof, np.arange(max(i - w + 1, 0), i + 1)).sum() for w in self.widths}
            fmin, smin = (1, 6) if self.floor else (1, 1)
            fast = next((w for w in self.widths if w >= fmin and typ[w] >= self.rows and not np.isnan(q[w])), None)
            slow = next((w for w in self.widths if w >= smin and typ[w] >= 6 * self.rows and not np.isnan(q[w]) and w > (fast or 0)), None)
            chosen = [w for w in (fast, slow) if w is not None]
            self.picked[i] = [fast or -1, slow or -1]
            fired = []
            for w in self.widths:
                if w not in chosen or i - w + 1 < 0:
                    run[w] = 0
                    continue
                self.low[w][i] = max((typ[w] + 1) * q[w] - 1, 0.0) * (1 - self.margin)
                if nanc[i + 1] - nanc[i + 1 - w] == 0 and cs[i + 1] - cs[i + 1 - w] < self.low[w][i]:
                    run[w] += 1
                else:
                    run[w] = 0
                if run[w] >= self.k:
                    fired.append(w)
            if fired:
                back = 0 if self.held[i - 1] else self.k - 1
                for w in fired:
                    self.L[max(i - back - w + 1, 0) : i + 1] = False
                self.held[i] = True
        return self.held


def make(b, c, judge_from):
    if c.get("rows"):
        return Rows(b, c["k"], c["margin"], judge_from, c["rows"], c.get("floor", False), c.get("history", DAYS))
    d = Extended(b, c["widths"], c["k"], c["margin"], judge_from, c.get("minrows", 0.0), c.get("groups", False), c.get("history", DAYS))
    d.longhistory = c.get("longhistory", 0)
    d.wtyp = c.get("wtyp", False)
    return d


def judge(b, lab, c, judge_from, sample_every=2):
    d = make(b, c, judge_from)
    widths, k, margin = d.widths, c["k"], c["margin"]
    held = d.run()
    judged = np.zeros(len(b), bool)
    for w in widths:
        judged |= ~np.isnan(d.low[w])
    judged[:judge_from] = False
    clean = judged & ~lab
    months = clean.sum() * STEP_MIN / (60 * 24 * 30)
    starts = [i for i in range(1, len(b)) if held[i] and not held[i - 1]]
    false = sum(1 for s in starts if not lab[s])
    sample = clean & (np.arange(len(b)) % sample_every == 0) & ~held & ~np.roll(held, 1)
    out = dict(c, widths=list(widths), months=float(months), false_count=false, false=float(false / months) if months else None,
               silhouette=float(np.mean(d.silhouettes)) if d.silhouettes else None)
    if c.get("rows"):
        p = d.picked[judge_from:]
        out["picked"] = {str(w): float(np.mean(p == w)) for w in LADDER}
    for f in DROPS:
        agree = np.logical_or.reduce([below_over(b, d.low[w], sample, w, f, HORIZON) for w in widths])
        t = first_hit(agree, k)
        for h in HORIZONS:
            out[f"by{h}_{f}"] = float(np.mean(t <= h))
        fin = t[np.isfinite(t)]
        out[f"median_{f}"] = float(np.median(fin)) if len(fin) else None
    return out


EXPERIMENTS = {
    "long": [dict(widths=(1, 6), k=3, margin=0.3)] + [dict(widths=(1, 6, w), k=3, margin=0.3) for w in (12, 36, 72, 288)],
    "minrows": [dict(widths=(1, 6), k=3, margin=0.3, minrows=B) for B in (0, 10, 30)]
    + [dict(widths=(1, 6, 288), k=3, margin=0.3, minrows=B) for B in (0, 10, 30)],
    "grid": [dict(widths=(1, 6), k=k, margin=m) for m in (0.2, 0.3, 0.4) for k in (3, 4)],
    "groups": [dict(widths=(1, 6), k=3, margin=0.3, groups=g) for g in (False, True)],
    "rows": [dict(widths=(1, 6), k=3, margin=0.3)] + [dict(widths=(), k=3, margin=0.3, rows=B) for B in (10, 20, 40)],
    "rowsfloor": [dict(widths=(), k=3, margin=0.3, rows=B, floor=True) for B in (10, 20)],
    "final": [dict(widths=(1, 6), k=k, margin=m) for m in (0.2, 0.3) for k in (3, 4)]
    + [dict(widths=(), k=4, margin=0.2, rows=B, floor=True, history=28) for B in (10, 20)]
    + [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, history=28)],
    "final2": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=m) for m in (0, 10)]
    + [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, history=28, minrows=10)]
    + [dict(widths=(1, 6, 72, 288), k=3, margin=0.2, longhistory=28, minrows=10), dict(widths=(1, 6, 72, 288), k=4, margin=0.3, longhistory=28, minrows=10)]
    + [dict(widths=(1, 6), k=4, margin=0.2, minrows=10)],
    "paper": [dict(widths=(1, 6, 72, 288), k=3, margin=0.2, longhistory=28, minrows=10, wtyp=True), dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True),
              dict(widths=(1, 6, 72, 288), k=4, margin=0.3, longhistory=28, minrows=10, wtyp=True), dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, wtyp=True),
              dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10), dict(widths=(1, 6), k=4, margin=0.2)],
    "widths": [dict(widths=w, k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True) for w in ((1,), (6,), (72,), (288,), (1, 6), (1, 6, 72, 288))],
    "grid2": [dict(widths=(1, 6, 72, 288), k=k, margin=m, longhistory=28, minrows=10, wtyp=True) for m in (0.2, 0.3, 0.4) for k in (3, 4)],
    "hist7": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, minrows=10, wtyp=True)],
    "gate": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=B, wtyp=True) for B in (5, 10, 20, 40)],
    "paper_senders": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True)],
    "paper_senders_quiet": [dict(widths=(1, 6, 72, 288), k=4, margin=0.3, longhistory=28, minrows=10, wtyp=True)],
    "pick": [dict(widths=(1, 6), k=3, margin=0.3), dict(widths=(1, 6), k=4, margin=0.2), dict(widths=(1, 6), k=4, margin=0.3)],
}


def job(args):
    dataset, series, seed, b, lab, configs = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    out = []
    for c in configs:
        if c.get("groups") and judge_from < 28 * DAY:
            continue  # 28 days of history are needed to group days
        out.append(judge(b, lab, c, judge_from))
    return dict(dataset=dataset, series=series, seed=seed, configs=out)


def part_path(part_dir, mix, seed):
    return os.path.join(part_dir, f"{mix.replace('/', '-').replace(' ', '_')}-{seed}.json")


def sender_job(args):
    from value_lab import series

    mix, seed, configs = args
    seen = series(mix, seed)
    out = dict(mix=mix, seed=seed, senders=len(seen), configs=[])
    for c in configs:
        fp, months, hits = 0, 0.0, {f: [] for f in DROPS}
        for b in seen:
            d = make(b, c, 28 * DAY)
            held = d.run()
            judged = np.zeros(len(b), bool)
            for w in d.widths:
                judged |= ~np.isnan(d.low[w])
            judged[: 28 * DAY] = False
            fp += sum(1 for i in range(1, len(b)) if held[i] and not held[i - 1])
            months += judged.sum() * STEP_MIN / (60 * 24 * 30)
            sample = judged & (np.arange(len(b)) % 4 == 0) & ~held & ~np.roll(held, 1)
            for f in DROPS:
                agree = np.logical_or.reduce([below_over(b, d.low[w], sample, w, f, HORIZON) for w in d.widths])
                hits[f].append(first_hit(agree, c["k"]))
        cfg = dict(c, widths=list(d.widths), false_count=fp, months=months, false=fp / months if months else None)
        for f in DROPS:
            t = np.concatenate(hits[f])
            for h in HORIZONS:
                cfg[f"by{h}_{f}"] = float(np.mean(t <= h))
            fin = t[np.isfinite(t)]
            cfg[f"median_{f}"] = float(np.median(fin)) if len(fin) else None
        out["configs"].append(cfg)
    return out


def main(root, experiment, out_path):
    configs = EXPERIMENTS[experiment]
    if root == "senders":
        # One file per mix and seed, so a run cut short resumes where it stopped.
        from value_lab import MIXES

        part_dir = out_path + ".parts"
        os.makedirs(part_dir, exist_ok=True)
        mixes = [m for m in MIXES if m not in ("10 equal, same hours", "10 equal, 3 regions")]  # the same senders as two silence mixes
        jobs = [(mix, seed, configs) for mix in mixes for seed in (1, 2) if not os.path.exists(part_path(part_dir, mix, seed))]
        with Pool(4) as pool:
            for r in pool.imap_unordered(sender_job, jobs):
                json.dump(r, open(part_path(part_dir, r["mix"], r["seed"]), "w"))
        results = [json.load(open(part_path(part_dir, mix, seed))) for mix in mixes for seed in (1, 2)]
    elif root == "logs":
        from real_logs import load_logs

        part_dir = out_path + ".parts"
        os.makedirs(part_dir, exist_ok=True)
        series = load_logs("real")
        jobs = [("logs", name, 0, b, lab, configs) for name, b, lab in series if not os.path.exists(part_path(part_dir, name, 0))]
        with Pool(4) as pool:
            for r in pool.imap_unordered(job, jobs):
                json.dump(r, open(part_path(part_dir, r["series"], 0), "w"))
        results = [json.load(open(part_path(part_dir, name, 0))) for name, _, _ in series]
    else:
        labels = json.load(open(f"{root}/labels.json"))
        names = MODELED if experiment != "groups" else ["weekday/weekend", "daily", "steady, falling 3%/day"]
        jobs = [("modeled", name, seed, modeled(name, seed), np.zeros(56 * S, bool), configs) for name in names for seed in (1, 2, 3)]
        for name in NAB:
            b, lab = nab_series(root, name, labels)
            jobs.append(("nab", name, 0, b, lab, configs))
        with Pool(4) as pool:
            results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
