"""Extensions of the detector of design_lab.py, each one rule added to the detector as written.

    python3 extend_lab.py nab/ long extend_long.json       # a third, long width (1 h, 3 h, 6 h, 1 day); horizons to a day
    python3 extend_lab.py nab/ minrows extend_minrows.json # a width is judged only when its typical window holds B rows
    python3 extend_lab.py senders minrows extend_minrows_values.json
    python3 extend_lab.py nab/ grid extend_grid.json       # margins x waits at the two widths, for the held-out check
    python3 extend_lab.py nab/ groups extend_groups.json   # the typical level learned per group of weekdays
    python3 extend_lab.py nab/ learn extend_learn.json     # when a window is learned, and the cut learned or not
    python3 extend_lab.py senders learn_senders extend_learn_senders.json
    python3 extend_lab.py steps steps extend_steps.json    # feeds that step down on day 35 and stay there

Below-normal buckets are unlearned per width: an alarm at width w unlearns the w buckets of each reading that
fired, so a long width does not take a day of learning with it when the 5-minute width alarms. A window of width w is
learned once w and the k readings an alarm needs have passed since it ended (`learn`), and a cut drop is learned as the
detector would learn it: from each refresh on, the windows after the cut that have waited enter the width's ratios at
their cut sums (`below_cut`).
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
from run_all import DAY, MODELED, NAB, modeled, nab_series, with_added

HORIZON = 25 * 60 // STEP_MIN  # buckets after a drop over which detection is scored: 25 hours
HORIZONS = (15, 20, 30, 60, 360, 1440)  # minutes


def below_over(b, low, sample, w, f, horizon):
    """Whether the window of width w ending at each bucket of the horizon after each sampled cut, cut to f from the cut
    on, is below `low`: one band over the series, or one row of it per cut."""
    cs = np.concatenate([[0.0], np.cumsum(np.nan_to_num(b))])
    t0 = np.where(sample)[0]
    t0 = t0[t0 + horizon < len(b)]
    out = np.zeros((len(t0), horizon), bool)
    for d in range(horizon):
        j = t0 + d
        first = j - w + 1
        pre = np.where(first < t0, cs[t0] - cs[np.maximum(first, 0)], 0.0)
        post = cs[j + 1] - cs[np.maximum(first, t0)]
        lo = low[j] if low.ndim == 1 else low[:, d]
        out[:, d] = ~np.isnan(lo) & (pre + f * post < lo)
    return out


def below_cut(d, b, sample, w, f, horizon):
    """below_over against the band the detector learns with the cut in it: at each refresh after the cut, width w's
    quantile is the one `Extended.cut` computed with the windows after the cut at their cut sums. The typical window is
    the uncut one, which is learned from the same time of day on earlier days, at least a day before."""
    if not d.grid:
        return below_over(b, d.low[w], sample, w, f, horizon)
    t0 = np.where(sample)[0]
    t0 = t0[t0 + horizon < len(b)]
    low = d.low[w][t0[:, None] + np.arange(horizon)[None, :]]
    for r, (cuts, qq) in d.cut_q.get((w, f), {}).items():
        rows = np.where((t0 > r - horizon) & (t0 <= r))[0]
        at = np.searchsorted(cuts, t0[rows])
        found = (at < len(cuts)) & (cuts[np.minimum(at, len(cuts) - 1)] == t0[rows])
        rows, q = rows[found], qq[at[found]]
        for k in range(REFRESH):
            col = r + k - t0[rows]
            ok = col < horizon
            j = r + k
            if j >= len(b):
                break
            rr, cc, qk = rows[ok], col[ok], q[ok]
            typ = d.typ[w][j]
            low[rr, cc] = np.where(np.isnan(low[rr, cc]), np.nan, np.maximum((typ + 1) * qk - 1, 0.0) * (1 - d.margin))
    return below_over(b, low, sample, w, f, horizon)


PREDICTIVE = ("predictive", "exhaustive", "quietest", "hourly")  # the day-group forms the held-out test keeps
# The 63 splits of the seven weekdays into two: weekday 0 is always in group 0.
SPLITS = np.array([[0] + [(m >> (w - 1)) & 1 for w in range(1, 7)] for m in range(1, 64)])


def split_errors(X, wd):
    """err[s, h]: the squared error of the days X at hour h about the median of their group under split s."""
    err = np.zeros((len(SPLITS), X.shape[1]))
    for s, sp in enumerate(SPLITS):
        g = sp[wd]
        for k in (0, 1):
            Xk = X[g == k]
            if len(Xk):
                err[s] += ((Xk - np.median(Xk, axis=0)) ** 2).sum(0)
    return err


def predict_days(Xf, wdf, of, wdh, allm):
    """Each held-out day's profile: at each hour, the median of the fitted days of its group there, or allm where its
    group has no fitted day."""
    out = np.empty((len(wdh), Xf.shape[1]))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for j, w in enumerate(wdh):
            m = np.nanmedian(np.where(of[wdf] == of[w][None, :], Xf, np.nan), axis=0)
            out[j] = np.where(np.isnan(m), allm, m)
    return out


class Extended(Design):
    judged_after = 0  # days the oldest learned window must reach back before a width is judged; 0 keeps the 3-day count alone

    def __init__(self, b, widths, k, margin, judge_from, minrows=0.0, groups=False, history=DAYS):
        super().__init__(b, widths, k, margin, judge_from)
        self.minrows, self.groups, self.history = minrows, groups, history  # history: days of ratios behind each width's quantile
        self.longhistory = 0  # when set, the days of ratios behind widths over 30 minutes
        self.wtyp = False  # the typical window is the median of the same window on the past seven days
        # With the typical window, how days are told apart: not at all (None), by the day groups ("cluster"), by the day
        # groups where the 30-minute width is gated in at every time of day ("dense"), or by weekday ("weekday").
        self.daykind = None
        self.of = np.zeros((7, 24), int)  # each (weekday, hour of the day)'s kind, as of the last refresh
        self.off = 0  # buckets after UTC midnight at which a day starts ("quietest": at its quietest hour)
        self.offs = []  # the day start each refresh of the groups chose, in hours
        self.group_of = np.zeros((7, 24), int)  # the predictive forms' kinds per (weekday, hour)
        self.kinddays = 0  # when set, a kind's typical window is read from its newest kinddays days, not from 28 days
        self.kgroups = (2, 3)  # the numbers of day groups the predictive test tries; 7 is one group per weekday
        self.minweeks = 2  # the weeks the predictive test holds out before it may keep a grouping
        self.force = 0  # when set, the number of groups kept on every refresh, untested
        self.rng = np.random.default_rng(0)
        self.group_of_weekday = np.zeros(7, int)
        self.groups_at = -1
        self.silhouettes = []
        self.kept = []  # the number of day groups each predictive refresh kept, 0 for none
        self.learn = "width"  # a window is learned once w + k buckets have passed ("width"), k ("wait") or none ("now")
        self.grid = None  # (sample every, fractions, horizon): the cuts whose learning `cut` follows
        self.cut_q = {}
        self.typ = {w: np.full(self.n, np.nan) for w in self.widths}
        self.own = False  # each past window against the usual as of the start of its own day, not today's
        self.own_cache = {}  # (width, day, kinds, day start) -> that day's window profile
        self.dirty = None  # the first bucket whose learning changed since the cache was last pruned
        self.mark_from = 0  # when set, the first warning that starts at or after this bucket is marked Normal as it ends
        self.marked_at = None  # the bucket that warning ended at
        self.split = False  # a window that holds part of yesterday: each part's usual from its own day's group
        self.split_tab = {}  # width -> (head, tail): the usual first and last parts of a day, per group and day position
        self.onekind = False  # with split: a source with no kinds is one kind, split too, from its newest DAYS days
        self.evenparts = False  # a part's usual takes a day only with its part at every neighbor, as many either side
        self.wholeparts = False  # a part's usual takes a day only with its part at every neighbor, neighbors as before
        self.pairs = False  # a split window's usual: the median of every past end of yesterday's kind plus start of today's
        self.matched = False  # a split window's usual: real windows on days of today's kind after a day of yesterday's

    def lag(self, w):
        """Buckets after a window of width w ends before it is learned."""
        return {"width": w + self.k, "wait": self.k, "now": 0}[self.learn]

    # ---- day groups: the typical level per group of weekdays, over 28 days ----------------------------------------
    def refresh_groups(self, i):
        if self.groups_at == i // S:
            return
        self.groups_at = i // S
        if self.daykind == "quietest":
            self.off = self.quietest_hour(i) * (S // 24)
            self.offs.append(self.off // (S // 24))
        day = (i - self.off) // S  # the day bucket i falls in; the days before it are whole
        X, wd, age = [], [], []
        for d in range(max(day - 28, 0), day):
            lo = d * S + self.off
            bb, ll = self.b[lo : lo + S], self.L[lo : lo + S]
            if np.isnan(bb).mean() > 0.1 or (~ll).mean() > 0.1:
                continue
            X.append(np.log1p(np.nan_to_num(bb).reshape(24, -1).sum(1)))
            wd.append(d % 7)
            age.append(day - 1 - d)
        X, wd, age = np.array(X), np.array(wd), np.array(age)
        if self.daykind in PREDICTIVE:
            self.group_of = self.predictive_groups(X, wd, age // 7)
            return
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

    def predictive_groups(self, X, wd, week):
        """Each weekday's group, kept only when it predicts days it was not fitted on: each week in turn is held out, the
        groups are fitted on the others (k-means into each number of groups in kgroups, each weekday to the group most
        of its days fell in), and the held-out days are predicted by their group's median hourly profile, against the
        median profile of every day. The grouping that predicts best is kept if it predicts better than no grouping."""
        none = np.zeros((7, 24), int)
        if len(X) < 14 or len(set(week.tolist())) < self.minweeks:
            return none
        if self.force:  # a fixed number of groups, without the held-out test
            self.kept.append(self.force)
            return self.candidate(X, wd, self.force)
        weeks = sorted(set(week.tolist()))
        err = {kk: np.zeros(len(weeks)) for kk in (0, *self.kgroups)}
        for n, h in enumerate(weeks):
            fit, held = week != h, week == h
            allm = np.median(X[fit], axis=0)
            err[0][n] = ((X[held] - allm) ** 2).sum()
            for kk in self.kgroups:
                of = self.candidate(X[fit], wd[fit], kk)
                err[kk][n] = ((X[held] - predict_days(X[fit], wd[fit], of, wd[held], allm)) ** 2).sum()
        kk = min(self.kgroups, key=lambda k: err[k].sum())
        self.silhouettes.append(1 - err[kk].sum() / err[0].sum() if err[0].sum() else 0.0)  # held-out error removed
        # Better in every week held out, not merely in sum: with no weekly rhythm each week is a coin toss.
        self.kept.append(kk if (err[kk] < err[0]).all() else 0)
        return self.candidate(X, wd, kk) if self.kept[-1] else none

    def candidate(self, X, wd, kk):
        """Each (weekday, hour)'s group when the days X are split into kk groups: by k-means ("predictive"), by the split
        of the weekdays into two with the least error on X ("exhaustive", "quietest"), or by that split at each hour of
        the day on its own ("hourly"). Seven groups are one per weekday."""
        if kk == 7 or self.daykind == "predictive":
            return np.repeat(self.weekday_groups(X, wd, kk)[:, None], 24, 1)
        err = split_errors(X, wd)  # (63 splits, 24 hours)
        if self.daykind == "hourly":
            return SPLITS[err.argmin(0)].T
        return np.repeat(SPLITS[err.sum(1).argmin()][:, None], 24, 1)

    def quietest_hour(self, i):
        """The hour of the day (from UTC midnight) with the lowest median rows over the 28 days before bucket i."""
        a = max(i - 28 * S, 0)
        a += (-a) % S
        z = i - (i - a) % S
        if z - a < S:
            return 0
        x = np.where(self.L[a:z], self.b[a:z], np.nan).reshape(-1, 24, S // 24).sum(2)  # NaN where any bucket is
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            m = np.nanmedian(x, axis=0)
        return int(np.nanargmin(m)) if not np.isnan(m).all() else 0

    def kind(self, of, u):
        """The kind of the day bucket(s) u fall in, by the kinds `of` per (weekday, hour of the day)."""
        u = np.asarray(u) - self.off
        return of[(u // S) % 7, (u % S) // (S // 24)]

    def weekday_groups(self, X, wd, kk):
        """Each weekday's group when the days X are split into kk groups by k-means; a weekday with no day takes 0.
        Seven groups are one per weekday."""
        if kk == 7:
            return np.arange(7)
        lab = kmeans(X, kk, self.rng)
        return np.array([np.bincount(lab[wd == w]).argmax() if (wd == w).any() else 0 for w in range(7)])

    def profile(self, i):
        """prof[g, s]: the typical level of time of day s for day group g; and a = the first bucket the profile reads."""
        if not self.groups:
            a = i - DAYS * S
            x = np.where(self.L[max(a, 0) : i], self.b[max(a, 0) : i], np.nan)
            X = np.concatenate([np.full(max(-a, 0), np.nan), x]).reshape(DAYS, S)  # before the series it is unknown
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

    def window_profile(self, i, w, of=None, kd=None):
        """prof[g, s]: the median of the learned windows of width w ending at time of day s on the past days of kind g,
        taking in the windows within 30 minutes of s as the typical level does. Days are told apart by `of`, each
        weekday's kind, over the past 28 days; with `of` None there is one kind, over the past seven days. A kind with no
        learned window at s takes the one-kind level there."""
        days = DAYS if of is None else 28
        a = i - days * S
        lo = a - w + 1  # the first bucket a window of the profile reaches back to; before the series it is unknown
        x = np.where(self.L[max(lo, 0) : i], self.b[max(lo, 0) : i], np.nan)
        if lo < 0:
            x = np.concatenate([np.full(-lo, np.nan), x])
        sw = np.convolve(x, np.ones(w), "valid").reshape(days, S)  # sw[d, j] ends at bucket a + d S + j
        day = ((a + np.arange(days * S) - self.off) // S).reshape(days, S)  # the day each window ends on
        kind = np.zeros(sw.shape, int) if of is None else self.kind(of, a + np.arange(days * S)).reshape(days, S)
        prof = np.empty((int(kind.max()) + 1, S))
        for g in range(len(prof)):
            mine = kind == g
            kd = self.kinddays if kd is None else kd
            if of is not None and kd:  # the newest kd days of the kind, as many as today's seven
                mine &= np.isin(day, np.unique(day[mine])[-kd:])
            if of is not None and self.daykind == "hourly":
                # The days of kind g at this hour, and their windows within 30 minutes of it: an hour's kinds name
                # different days from its neighbor's.
                stack = np.stack([np.where(mine, np.roll(sw, -k, axis=1), np.nan) for k in range(-NB, NB + 1)]).reshape(-1, S)
            else:
                stack = np.stack([np.roll(np.where(mine, sw, np.nan), -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                prof[g, (a + np.arange(S)) % S] = np.nanmedian(stack, axis=0)
        if of is not None:
            prof = np.where(np.isnan(prof), self.window_profile(i, w)[0][None, :], prof)
        return prof

    def day_kinds(self, i):
        """Each weekday's kind of day as of bucket i, or None where days are not told apart."""
        if self.daykind == "weekday":
            return np.repeat(np.arange(7)[:, None], 24, 1) if i >= 28 * S else None
        if self.daykind in PREDICTIVE:
            self.refresh_groups(i)
            return None if self.group_of.max() == 0 else self.group_of.copy()
        if self.daykind in ("cluster", "dense"):
            self.refresh_groups(i)
            if self.group_of_weekday.max() == 0:
                return None
            if self.daykind == "dense":
                m = self.window_profile(i - self.lag(6), 6)[0]
                if np.isnan(m).all() or np.nanmin(m) < self.minrows:
                    return None
            return np.repeat(self.group_of_weekday[:, None], 24, 1)
        return None

    def typical_window(self, prof, w, i):
        """The typical level of the window of width w ending at bucket i."""
        if self.wtyp and self.split and w in self.split_tab:
            return self.split_typical(w, np.array([i]), prof[w])[0]
        if self.wtyp:
            return prof[w][self.kind(self.of, i), i % S]
        return self.typical_at(prof[w], np.arange(i - w + 1, i + 1)).sum()

    def split_parts(self, i, w, of, kd=0):
        """(head, tail), each (groups, S): head[g, p] the usual rows from the start of a day of group g to its position p,
        tail[g, p] the usual rows in the last w - p - 1 buckets of a day of group g, the part of yesterday a window of
        width w ending at position p holds. Each is the median over the learned days of the group in the 28 days before
        bucket i, taking in the parts of windows ending within 30 minutes of p as the usual does. Days start at self.off."""
        first = (max(i - 28 * S, 0) - self.off) // S
        last = (i - 1 - self.off) // S
        lo = first * S + self.off
        hi = (last + 1) * S + self.off
        x = np.full(hi - lo, np.nan)
        a, z = max(lo, 0), min(hi, i)
        x[a - lo : z - lo] = np.where(self.L[a:z], self.b[a:z], np.nan)
        D = x.reshape(-1, S)
        H = np.cumsum(D, axis=1)  # H[d, p]: rows in positions 0..p; NaN once any is missing
        T = np.cumsum(D[:, ::-1], axis=1)  # T[d, m - 1]: rows in the last m positions
        group = of[np.arange(first, last + 1) % 7, 0]
        G = int(of.max()) + 1
        pos = np.arange(S)
        heads, tails = [], []
        for k in range(-NB, NB + 1):
            p = pos + k
            m = w - p - 1  # the length of yesterday's part
            # A neighbor counts for both parts or for neither, so the parts the two medians take in still add up to w.
            ok = (p >= 0) & (p < S) & (m >= 1) & (m <= S)
            heads.append(np.where(ok[None, :], H[:, np.clip(p, 0, S - 1)], np.nan))
            tails.append(np.where(ok[None, :], T[:, np.clip(m - 1, 0, S - 1)], np.nan))
        heads, tails = np.stack(heads), np.stack(tails)  # (neighbors, days, S)
        if self.wholeparts:  # the neighbors each position has at all, the same on every day
            p = pos[None, :] + np.arange(-NB, NB + 1)[:, None]
            even = (p >= 0) & (p < S) & (w - p - 1 >= 1) & (w - p - 1 <= S)
        if self.evenparts:
            reach = np.minimum(np.minimum(NB, pos), w - 2 - pos)
            even = np.abs(np.arange(-NB, NB + 1))[:, None] <= reach[None, :]  # (neighbors, S)
        if self.evenparts or self.wholeparts:
            for arr in (heads, tails):
                arr[~np.broadcast_to(even[:, None, :], arr.shape)] = np.nan
                gap = (np.isnan(arr) & even[:, None, :]).any(axis=0)  # (days, S): a neighbor's part is missing
                arr[:, gap] = np.nan

        def newest(arr, mine):
            """The parts of the newest kd days of `mine` that have any at each position."""
            sel = arr[:, mine]
            valid = ~np.isnan(sel).all(axis=0)
            keep = valid & (np.cumsum(valid[::-1], axis=0)[::-1] <= kd)
            return np.where(keep[None], sel, np.nan)

        head = np.full((G, S), np.nan)
        tail = np.full((G, S), np.nan)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for g in range(G):
                mine = group == g
                if kd and (self.evenparts or self.wholeparts) and mine.any():
                    head[g] = np.nanmedian(newest(heads, mine).reshape(-1, S), axis=0)
                    tail[g] = np.nanmedian(newest(tails, mine).reshape(-1, S), axis=0)
                    continue
                if kd:  # the newest kd days of the group that hold any learned bucket
                    held = mine & ~np.isnan(D).all(axis=1)
                    mine = np.zeros_like(mine)
                    mine[np.flatnonzero(held)[-kd:]] = True
                if mine.any():
                    head[g] = np.nanmedian(heads[:, mine].reshape(-1, S), axis=0)
                    tail[g] = np.nanmedian(tails[:, mine].reshape(-1, S), axis=0)
            if self.pairs:
                def chosen(mine):
                    if kd:  # the newest kd days of the group that hold any learned bucket, as above
                        held = mine & ~np.isnan(D).all(axis=1)
                        mine = np.zeros_like(mine)
                        mine[np.flatnonzero(held)[-kd:]] = True
                    return mine

                pair = np.full((G, G, S), np.nan)
                for gt, gy in {(int(of[d, 0]), int(of[(d - 1) % 7, 0])) for d in range(7)}:
                    mt, my = chosen(group == gt), chosen(group == gy)
                    if mt.any() and my.any():
                        # (neighbors, starts, ends, S): every start of today's kind with every end of yesterday's, at
                        # the same neighbor, so each sum is a whole window's length.
                        sums = heads[:, mt][:, :, None, :] + tails[:, my][:, None, :, :]
                        pair[gt, gy] = np.nanmedian(sums.reshape(-1, S), axis=0)
                return head, tail, pair
        return head, tail

    def matched_tab(self, i, w, of, kd):
        """(None, None, pair): pair[gt, gy, s] the median of the learned windows of width w ending at time of day s, and
        within 30 minutes of it as the usual takes them in, on the newest kd days of kind gt whose day before was of kind
        gy, in the 28 days before bucket i. Days start at self.off."""
        days = 28
        a = i - days * S
        lo = a - w + 1
        x = np.where(self.L[max(lo, 0) : i], self.b[max(lo, 0) : i], np.nan)
        if lo < 0:
            x = np.concatenate([np.full(-lo, np.nan), x])
        sw = np.convolve(x, np.ones(w), "valid").reshape(days, S)  # sw[d, j] ends at bucket a + d S + j
        ends = a + np.arange(days * S)
        day = ((ends - self.off) // S).reshape(days, S)
        today = self.kind(of, ends).reshape(days, S)
        yesterday = self.kind(of, ends - S).reshape(days, S)
        G = int(of.max()) + 1
        pair = np.full((G, G, S), np.nan)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for gt, gy in {(int(of[d, 0]), int(of[(d - 1) % 7, 0])) for d in range(7)}:
                mine = (today == gt) & (yesterday == gy)
                if kd:
                    mine &= np.isin(day, np.unique(day[mine])[-kd:])
                stack = np.stack([np.roll(np.where(mine, sw, np.nan), -k, axis=1) for k in range(-NB, NB + 1)])
                pair[gt, gy, (a - self.off + np.arange(S)) % S] = np.nanmedian(stack.reshape(-1, S), axis=0)  # by place in the day
        return None, None, pair

    def split_typical(self, w, ends, profw):
        """The usual of each window of width w ending at `ends`: a window that holds part of yesterday takes the usual
        part of yesterday from yesterday's group and the usual part of today from today's; a window within today takes
        the usual of whole windows of its group (profw). A group's whole windows that reach into yesterday hold the
        days before it, which differ by weekday, so the parts are summed even where both days are of one group."""
        p = (ends - self.off) % S
        today = self.kind(self.of, ends)
        yesterday = self.kind(self.of, ends - p - 1)
        t = profw[today, ends % S].astype(float)
        cross = w > p + 1
        if self.pairs or self.matched:
            parts = self.split_tab[w][2][today, yesterday, p]
        else:
            head, tail = self.split_tab[w][:2]
            parts = head[today, p] + tail[yesterday, p]
        use = cross & ~np.isnan(parts)
        t[use] = parts[use]
        return t

    def own_typical(self, w, ends, of):
        """The usual of each window of width w ending at `ends`, as of the start of the day it ended on, under the kinds
        `of` in force now."""
        if self.dirty is not None:
            gone = [k for k in self.own_cache if k[1] * S > self.dirty]
            for k in gone:
                del self.own_cache[k]
            self.dirty = None
        key_of = None if of is None else of.tobytes()
        off = None if of is None else self.off
        if (key_of, off) != getattr(self, "own_key", None):  # new kinds: every day's profile is read afresh under them
            self.own_cache, self.own_key = {}, (key_of, off)
        day = ends // S
        t = np.empty(len(ends))
        for d in np.unique(day):
            k = (w, int(d), key_of, off)
            if k not in self.own_cache:
                at = int(d) * S - self.lag(w)
                # before a window of the series was learned, nothing was usual
                self.own_cache[k] = self.window_profile(at, w, of) if at > w else np.full((7, S), np.nan)
            m = day == d
            e = ends[m]
            t[m] = self.own_cache[k][self.kind(self.of, e), e % S]
        return t

    def refresh(self, i):
        """Each width's typical level and quantile from the windows learned by bucket i: those that ended lag(w) buckets
        or more before it."""
        prof, q = {}, {}
        if self.grid:
            self.cs_learned = np.concatenate([[0.0], np.cumsum(np.where(self.L, np.nan_to_num(self.b), 0.0))])
        of = self.day_kinds(i) if self.wtyp else None
        self.of = np.zeros((7, 24), int) if of is None else of
        one = self.wtyp and self.split and self.onekind and of is None
        use, kd = (self.of, DAYS) if one else (of, None)
        self.split_tab = {}
        for w in self.widths:
            iw = i - self.lag(w)
            prof[w] = self.window_profile(iw, w, use, kd) if self.wtyp else self.profile(iw)
            days = self.history if w * STEP_MIN <= 30 or not self.longhistory else self.longhistory
            a = max(iw - days * S, 0)
            x = np.where(self.L[a:iw], self.b[a:iw], np.nan)
            sw = np.convolve(x, np.ones(w), "valid")
            ends = np.arange(a + w - 1, iw)
            if self.wtyp and self.own:
                t = self.own_typical(w, ends, of)
            elif self.wtyp and self.split and use is not None:
                tab = self.matched_tab if self.matched else self.split_parts
                self.split_tab[w] = tab(iw, w, use, kd if kd is not None else self.kinddays)
                t = self.split_typical(w, ends, prof[w])
            elif self.wtyp:
                t = prof[w][self.kind(self.of, ends), ends % S]
            else:
                t = np.convolve(self.typical_at(prof[w], np.arange(a, iw)), np.ones(w), "valid")
            r = (sw + 1) / (t + 1)
            ok = ~np.isnan(r)
            q[w] = float(np.quantile(r[ok], TAIL, method="linear")) if ok.sum() >= 3 * S else np.nan
            if self.judged_after and not np.isnan(q[w]) and not self.reaches_back(i, iw, w):
                q[w] = np.nan
            if self.grid and not np.isnan(q[w]):
                self.cut(i, w, ends, r, t, ok)
        return prof, q

    def reaches_back(self, i, iw, w):
        """Whether width w's oldest learned window within smeltr's lookback (28 days and a day) began judged_after days
        or more before bucket i, as smeltr's JUDGED_AFTER_MS asks."""
        lo = max(i - 29 * S, 0)
        good = (self.L[lo:iw] & ~np.isnan(self.b[lo:iw])).astype(int)
        full = np.convolve(good, np.ones(w, int), "valid") == w if len(good) >= w else np.zeros(0, bool)
        return bool(full.any()) and i - (lo + int(np.argmax(full))) >= self.judged_after * S

    def cut(self, i, w, ends, r, t, ok):
        """For each cut on the scoring grid whose windows this refresh learns, width w's quantile with the windows that
        ended at or after the cut at their cut sums, f times the rows from the cut on, in place of their own."""
        every, fractions, horizon = self.grid
        t0 = np.arange(max(i - horizon + 1, self.judge_from), ends[-1] + 1)
        t0 = t0[t0 % every == 0]
        if not len(t0):
            return
        n = int(ok.sum())
        h = TAIL * (n - 1)
        lo = int(np.floor(h))
        need = lo + 2
        recent = ends >= t0[0]
        older = r[ok & ~recent]
        older = np.sort(np.partition(older, need - 1)[:need]) if len(older) > need else np.sort(older)
        e, te, oke = ends[recent], t[recent], ok[recent]
        clean = np.where(oke, r[recent], np.inf)
        cs = self.cs_learned
        s0 = e - w + 1
        at = np.clip(t0[:, None], s0[None, :], e[None, :] + 1)
        pre = cs[at] - cs[s0][None, :]
        post = cs[e + 1][None, :] - cs[at]
        after = e[None, :] >= t0[:, None]
        for f in fractions:
            cutr = np.where(oke[None, :], (pre + f * post + 1) / (te[None, :] + 1), np.inf)
            vals = np.concatenate([np.broadcast_to(older, (len(t0), len(older))), np.where(after, cutr, clean[None, :])], axis=1)
            low = np.sort(np.partition(vals, need - 1, axis=1)[:, :need], axis=1)
            self.cut_q.setdefault((w, f), {})[i] = (t0, low[:, lo] + (h - lo) * (low[:, lo + 1] - low[:, lo]))

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
                self.typ[w][i] = typ
                self.low[w][i] = max((typ + 1) * q[w] - 1, 0.0) * (1 - self.margin)
                if nanc[i + 1] - nanc[i + 1 - w] == 0 and cs[i + 1] - cs[i + 1 - w] < self.low[w][i]:
                    run[w] += 1
                else:
                    run[w] = 0
                if run[w] >= self.k:
                    fired.append(w)
            if fired:
                back = 0 if self.held[i - 1] else self.k - 1  # raised now: the readings that waited are not learned either
                if not self.held[i - 1]:
                    span_start, span_lo = i, i
                for w in fired:
                    lo = max(i - back - w + 1, 0)
                    self.L[lo : i + 1] = False
                    span_lo = min(span_lo, lo)
                    self.dirty = lo if self.dirty is None else min(self.dirty, lo)
                self.held[i] = True
            elif self.held[i - 1] and self.mark_from and self.marked_at is None and span_start >= self.mark_from:
                # an editor marks the warning that just ended Normal: every window it unlearned is learned
                self.L[span_lo:i] = True
                self.dirty = span_lo if self.dirty is None else min(self.dirty, span_lo)
                self.marked_at = i
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
            typ = {w: self.typical_at(prof[w], np.arange(max(i - w + 1, 0), i + 1)).sum() for w in self.widths}
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
                self.typ[w][i] = typ[w]
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


def make(b, c, judge_from, grid=None):
    """The detector of config c; with `grid`, (sample every, fractions, horizon), it also learns the cuts on that grid
    as it runs, unless c says the cut is scored against the uncut band (cut=False)."""
    if c.get("rows"):
        d = Rows(b, c["k"], c["margin"], judge_from, c["rows"], c.get("floor", False), c.get("history", DAYS))
    else:
        d = Extended(b, c["widths"], c["k"], c["margin"], judge_from, c.get("minrows", 0.0), c.get("groups", False), c.get("history", DAYS))
        d.longhistory = c.get("longhistory", 0)
        d.wtyp = c.get("wtyp", False)
        d.daykind = c.get("daykind")
        d.kinddays = c.get("kinddays", 0)
        d.kgroups = tuple(c.get("kgroups", (2, 3)))
        d.minweeks = c.get("minweeks", 2)
        d.force = c.get("force", 0)
        d.own = c.get("own", False)
        d.mark_from = c.get("mark_from", 0)
        d.split = c.get("split", False)
        d.onekind = c.get("onekind", False)
        d.evenparts = c.get("evenparts", False)
        d.wholeparts = c.get("wholeparts", False)
        d.pairs = c.get("pairs", False)
        d.judged_after = c.get("judged_after", 0)
    d.learn = c.get("learn", "width")
    d.grid = grid if c.get("cut", True) else None
    return d


def judge(b, lab, c, judge_from, sample_every=2):
    d = make(b, c, judge_from, (sample_every, DROPS, HORIZON))
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
               silhouette=float(np.mean(d.silhouettes)) if d.silhouettes else None,
               kept={str(k): int(n) for k, n in zip(*np.unique(d.kept, return_counts=True))},
               offs={str(k): int(n) for k, n in zip(*np.unique(d.offs, return_counts=True))})
    if c.get("starts"):  # where each false alarm began, in buckets
        out["false_starts"] = [int(s) for s in starts if not lab[s]]
    if c.get("rows"):
        p = d.picked[judge_from:]
        out["picked"] = {str(w): float(np.mean(p == w)) for w in LADDER}
    for f in DROPS:
        agree = np.logical_or.reduce([below_cut(d, b, sample, w, f, HORIZON) for w in widths])
        t = first_hit(agree, k)
        for h in HORIZONS:
            out[f"by{h}_{f}"] = float(np.mean(t <= h))
        fin = t[np.isfinite(t)]
        out[f"median_{f}"] = float(np.median(fin)) if len(fin) else None
    return out


BALANCED = dict(widths=(1, 6, 72, 288), k=4, margin=0.3, longhistory=28, minrows=10, wtyp=True, kgroups=(2, 7), minweeks=4)
FORMS = {"none": dict(daykind=None), "A": dict(daykind="predictive"), "B": dict(daykind="exhaustive"),
         "C": dict(daykind="quietest"), "H": dict(daykind="hourly")}
UTC_SHIFT = 5 * 12  # buckets: a US East source's local midnight is 5 am UTC


def placed(b, lab, shift):
    """The series with `shift` empty buckets before it, so its days split at UTC midnight."""
    return np.concatenate([np.full(shift, np.nan), b]), np.concatenate([np.zeros(shift, bool), lab])


def log_shifts(folder):
    """Buckets from UTC midnight to each log's first bucket, in load_logs' order. Each file opens with its start."""
    import datetime as dt
    import glob

    out = []
    for path in sorted(glob.glob(os.path.join(folder, "*.json"))):
        with open(path) as f:
            head = f.read(100)
        t = dt.datetime.fromisoformat(head.split('"start": "')[1].split('"')[0])
        out.append((t.hour * 60 + t.minute) // STEP_MIN)
    return out


def shift_of(dataset, series):
    """Buckets from UTC midnight to the first bucket of a NAB or modeled series: local-time series as a US East
    source's; the NAB tweets and AWS series are UTC already."""
    return UTC_SHIFT if dataset == "modeled" or series == "realKnownCause/nyc_taxi" else 0


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
    # the presets' extremes: margins from 0.1 to 0.9 and waits from 1 to 6
    "grid3": [dict(widths=(1, 6, 72, 288), k=k, margin=m, longhistory=28, minrows=10, wtyp=True)
              for m in (0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 0.9) for k in (1, 2, 3, 4, 6)],
    "grid2": [dict(widths=(1, 6, 72, 288), k=k, margin=m, longhistory=28, minrows=10, wtyp=True) for m in (0.2, 0.3, 0.4) for k in (3, 4)],
    "hist7": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, minrows=10, wtyp=True)],
    "gate": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=B, wtyp=True) for B in (5, 10, 20, 40)],
    "paper_senders": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True)],
    "paper_senders_quiet": [dict(widths=(1, 6, 72, 288), k=4, margin=0.3, longhistory=28, minrows=10, wtyp=True)],
    "paper_senders_q4": [dict(widths=(1, 6, 72, 288), k=4, margin=0.4, longhistory=28, minrows=10, wtyp=True)],
    "paper_senders_q56": [dict(widths=(1, 6, 72, 288), k=6, margin=0.5, longhistory=28, minrows=10, wtyp=True)],
    "steps": [dict(widths=(1, 6, 72, 288), k=4, margin=m, longhistory=28, minrows=10, wtyp=True, learn=l)
              for m in (0.2, 0.3) for l in ("width", "wait")],
    "pick": [dict(widths=(1, 6), k=3, margin=0.3), dict(widths=(1, 6), k=4, margin=0.2), dict(widths=(1, 6), k=4, margin=0.3)],
    # when a window is learned: after its width and the wait, after the wait only, at once; and the cut not learned
    "learn": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True, learn=l, cut=c)
              for l, c in (("width", True), ("wait", True), ("now", True), ("now", False))],
    # how days are told apart: not at all, day groups, day groups where every hour is dense, weekday (Section 6.7)
    "daygroups": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True, daykind=g)
                  for g in (None, "cluster", "dense", "weekday")],
    # two groups only, and one group per weekday among the candidates
    "daygroups_k": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True, daykind="predictive", kgroups=g)
                    for g in ((2,), (2, 7), (2, 3, 4, 5, 6, 7))],
    # none, two groups or one per weekday, kept only after four weeks held out: luck passes a grouping at most 1 in 16
    "daygroups_final": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True, daykind="predictive",
                             kgroups=(2, 7), minweeks=4)],
    "daygroups_cv": [dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True, daykind="predictive", kinddays=n)
                     for n in (0, 7)],
    # every day-group form, as the series were and with days split at UTC midnight (Section 6.7)
    "daygroups_forms": [dict(BALANCED, form=f, utc=u, **FORMS[f]) for u in (False, True) for f in FORMS],
    "daygroups_forms_senders": [dict(BALANCED, margin=0.5, k=6, form=f, utc=True, **FORMS[f]) for f in ("none", "B", "C", "H")],
    "daygroups_forms_senders_a": [dict(BALANCED, margin=0.5, k=6, form="A", utc=True, **FORMS["A"])],
    "learn_senders": [dict(widths=(1, 6, 72, 288), k=4, margin=0.3, longhistory=28, minrows=10, wtyp=True, learn=l) for l in ("width", "wait")],
}

# The detector smeltr ships (src/leader/throughput.rs): day groups as #741 ships them, placed at UTC, judged once its
# rows reach back a week; and its four presets, (k, margin).
SHIPPED = dict(BALANCED, utc=True, split=True, kinddays=7, matched=True, judged_after=7, **FORMS["C"])
PRESETS = {"Fast": (1, 0.5), "Sensitive": (3, 0.2), "Balanced": (4, 0.3), "Quiet": (6, 0.5)}
AT = {name: dict(SHIPPED, k=k, margin=m, form=name) for name, (k, m) in PRESETS.items()}
EXPERIMENTS.update({
    # the four presets, then the parts removed one at a time at Balanced; a typical level summed from buckets has no
    # typical window to learn day groups from, so it is set beside one kind
    "shipped": list(AT.values()) + [dict(AT["Balanced"], minrows=0, form="no gate"), dict(AT["Balanced"], widths=(1, 6), form="two widths"),
                                    dict(AT["Balanced"], daykind=None, form="one kind"), dict(AT["Balanced"], daykind=None, wtyp=False, form="summed")],
    "shipped_widths": [dict(AT["Balanced"], widths=w, form=str(w)) for w in ((1,), (6,), (72,), (288,), (1, 6), (1, 6, 72, 288))],
    "shipped_grid": [dict(SHIPPED, k=k, margin=m, form=f"k{k} m{m}") for m in (0.2, 0.3, 0.5) for k in (1, 3, 4, 6)],
    "shipped_hist7": [dict(AT["Balanced"], longhistory=0, form="7 days")],
    "shipped_gate": [dict(AT["Balanced"], minrows=B, form=f"gate {B}") for B in (5, 10, 20, 40)],
    "shipped_senders": [AT["Quiet"], AT["Balanced"]],
})

# The alternatives of Sections 6.5 to 6.7 were compared under the harness's first rules: every window learned at once, a
# cut scored against the band of the uncut series, and the archive's labels alone.
FIRST_RULES = ("long", "minrows", "grid", "groups", "rows", "rowsfloor", "final", "final2", "pick")
for name in FIRST_RULES:
    EXPERIMENTS[name] = [dict(c, learn="now", cut=False) for c in EXPERIMENTS[name]]

STEP_FRACTIONS = (0.5, 0.3, 0.0)  # the stepped feeds' new level: a halving, a drop to 30%, a stop


def step_job(args):
    """The recommended detector on a feed that steps to f of its level on day STEP_AT and stays: hours from the step to
    its first alarm and to the end of its last, the share of the time after the step under an alarm, and how the last
    alarm ended: on a reading inside the band with the drop learned ("learned"), with no width judging because every
    window that held the drop is unlearned ("no band"), or not by the end of the replay ("holds")."""
    from learn import STEP_AT, stepped

    shape, f, seed, configs = args
    b = stepped(shape, f, seed)
    at = STEP_AT * DAY
    out = dict(shape=shape, fraction=f, seed=seed, configs=[])
    for c in configs:
        d = make(b, c, 28 * DAY)
        held = d.run()
        judged = np.logical_or.reduce([~np.isnan(d.low[w]) for w in d.widths])
        after = held[at:]
        alarmed = np.where(after)[0]
        hours = lambda x: None if x is None else float(x * STEP_MIN / 60)
        first = last = ended = None
        if len(alarmed):
            first, last = alarmed[0], alarmed[-1] + 1
            ended = "holds" if last == len(after) else "learned" if judged[at + last] else "no band"
        out["configs"].append(dict(c, widths=list(d.widths), first_hours=hours(first), last_hours=hours(last), ended=ended,
                                   share_alarmed=float(after.mean())))
    return out


def job(args):
    dataset, series, seed, b, lab, configs = args[:6]
    shift = args[6] if len(args) > 6 else shift_of(dataset, series)
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    out = []
    for c in configs:
        if c.get("groups") and judge_from < 28 * DAY:
            continue  # 28 days of history are needed to group days
        if c.get("utc") and not shift:  # already split at UTC midnight: the same replay as the series was
            same = [o for o in out if not o.get("utc") and o.get("form") == c.get("form")]
            if same:
                out.append(dict(same[0], utc=True))
                continue
        bb, ll = placed(b, lab, shift) if c.get("utc") else (b, lab)
        out.append(dict(judge(bb, ll, c, judge_from), shift=shift if c.get("utc") else 0))
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
            if c.get("utc"):
                b = placed(b, np.zeros(len(b), bool), UTC_SHIFT)[0]
            d = make(b, c, 28 * DAY, (4, DROPS, HORIZON))
            held = d.run()
            judged = np.zeros(len(b), bool)
            for w in d.widths:
                judged |= ~np.isnan(d.low[w])
            judged[: 28 * DAY] = False
            fp += sum(1 for i in range(1, len(b)) if held[i] and not held[i - 1])
            months += judged.sum() * STEP_MIN / (60 * 24 * 30)
            sample = judged & (np.arange(len(b)) % 4 == 0) & ~held & ~np.roll(held, 1)
            for f in DROPS:
                agree = np.logical_or.reduce([below_cut(d, b, sample, w, f, HORIZON) for w in d.widths])
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
    if root == "steps":
        jobs = [(shape, f, seed, configs) for shape in ("steady", "daily") for f in STEP_FRACTIONS for seed in (1, 2, 3)]
        with Pool(4) as pool:
            results = list(pool.imap_unordered(step_job, jobs))
    elif root == "senders":
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
        jobs = [("logs", name, 0, b, lab, configs, sh) for (name, b, lab), sh in zip(series, log_shifts("real"))
                if not os.path.exists(part_path(part_dir, name, 0))]
        with Pool(4) as pool:
            for r in pool.imap_unordered(job, jobs):
                json.dump(r, open(part_path(part_dir, r["series"], 0), "w"))
        results = [json.load(open(part_path(part_dir, name, 0))) for name, _, _ in series]
    else:
        labels = json.load(open(f"{root}/labels.json"))
        if experiment not in FIRST_RULES:
            labels = with_added(labels)
        names = MODELED if experiment != "groups" else ["weekday/weekend", "daily", "steady, falling 3%/day"]
        jobs = [("modeled", name, seed, modeled(name, seed), np.zeros(56 * S, bool), configs) for name in names for seed in (1, 2, 3)]
        for name in NAB:
            b, lab = nab_series(root, name, labels)
            jobs.append(("nab", name, 0, b, lab, configs))
        # One file per series, so a run cut short resumes where it stopped.
        part_dir = out_path + ".parts"
        os.makedirs(part_dir, exist_ok=True)
        part = lambda dataset, name, seed: part_path(part_dir, f"{dataset}-{name}", seed)
        with Pool(4) as pool:
            for r in pool.imap_unordered(job, [j for j in jobs if not os.path.exists(part(*j[:3]))]):
                json.dump(r, open(part(r["dataset"], r["series"], r["seed"]), "w"))
        results = [json.load(open(part(*j[:3]))) for j in jobs]
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
