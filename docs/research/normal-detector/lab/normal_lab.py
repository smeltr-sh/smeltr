"""A source's normal, replayed: today's band from src/leader/throughput.rs and the candidates that might replace it.

A series is rows read per 5-minute bucket, index 0 at midnight, NaN where nothing was reported. Every 5 minutes the
30-minute window that just ended is judged against a band learned from past windows, as throughput.rs does:

- a window with a bucket nobody reported is neither judged nor learned;
- a window that overlaps a warning (below or above) is not learned;
- judging needs 3 past days with a window at this time of day.

Each candidate changes only how the band is set from the learned windows (`band_*`).
"""

import numpy as np

STEP_MIN = 5
S = 24 * 60 // STEP_MIN  # steps a day
WIN = 30 // STEP_MIN  # buckets a window
NB = 30 // STEP_MIN  # the neighborhood: steps either side of the same time of day
DAYS = 7
MIN_DAYS = 3


def windows(b):
    """Rows over the window ending with each bucket, NaN where any of its buckets is missing."""
    valid = ~np.isnan(b)
    cs = np.concatenate([[0.0], np.cumsum(np.where(valid, b, 0.0))])
    cv = np.concatenate([[0], np.cumsum(valid)])
    w = np.full(len(b), np.nan)
    i = np.arange(WIN - 1, len(b))
    full = (cv[i + 1] - cv[i + 1 - WIN]) == WIN
    w[i[full]] = (cs[i + 1] - cs[i + 1 - WIN])[full]
    return w


def quantile(vals, q):
    """Linear between the two nearest samples, as throughput.rs `quantile`."""
    return np.quantile(vals, q, method="linear")


def wquantile(vals, weights, q):
    """The weighted quantile: the smallest value whose running weight reaches q of the total."""
    order = np.argsort(vals)
    c = np.cumsum(weights[order])
    at = min(np.searchsorted(c, q * c[-1]), len(vals) - 1)
    return vals[order][at]


# The windows today's band reads: the same time of day, +-30 min, on each of the last 7 days.
_D = np.repeat(np.arange(1, DAYS + 1), 2 * NB + 1)
_K = np.tile(np.arange(-NB, NB + 1), DAYS)
SAME_TIME = _D * S - _K  # subtracted from i


def kmeans(X, k, rng, iters=30):
    best = None
    for _ in range(5):
        C = X[rng.choice(len(X), k, replace=False)]
        for _ in range(iters):
            lab = np.argmin(((X[:, None, :] - C[None]) ** 2).sum(-1), 1)
            C = np.array([X[lab == j].mean(0) if (lab == j).any() else C[j] for j in range(k)])
        inertia = ((X - C[lab]) ** 2).sum()
        if best is None or inertia < best[0]:
            best = (inertia, lab)
    return best[1]


def silhouette(X, lab):
    """The median silhouette, as the ITSI patent scores a grouping."""
    D = np.sqrt(((X[:, None, :] - X[None]) ** 2).sum(-1))
    s = []
    for i in range(len(X)):
        same = lab == lab[i]
        same[i] = False
        if not same.any():
            s.append(0.0)
            continue
        a = D[i, same].mean()
        b = min(D[i, lab == j].mean() for j in set(lab.tolist()) if j != lab[i])
        s.append((b - a) / max(a, b))
    return float(np.median(s))


class Run:
    """One candidate over one series."""

    def __init__(self, b, cfg, judge_from, seed=0):
        self.b, self.cfg = b, cfg
        self.W = windows(b)
        self.n = len(b)
        self.L = ~np.isnan(self.W)  # learned
        self.low = np.full(self.n, np.nan)
        self.high = np.full(self.n, np.nan)
        self.judge_from = judge_from
        self.rng = np.random.default_rng(seed)
        self.prof_at = -1
        self.groups_at = -1
        self.group_of_weekday = np.zeros(7, int)
        self.silhouettes = []
        self.wt = np.ones(self.n)  # each learned window's weight in the band's tails
        self.Lp = None  # with "median_all": the windows the typical level learns from, warned ones included
        self.state_at = np.zeros(self.n, np.int8)  # after each step: -1 below normal, 1 above, 0 neither

    # ---- bands -------------------------------------------------------------------------------------------------
    def same_time(self, i):
        J = i - SAME_TIME
        J = J[J >= 0]
        m = (self.L if self.Lp is None else self.Lp)[J]
        days = np.unique((i - J[m] + NB) // S)
        return J[m], len(days)

    def band(self, i):
        J, days = self.same_time(i)
        if days < MIN_DAYS:
            return None
        c = self.cfg
        tail = (1 - c["p"]) / 2
        kind = c["kind"]
        if kind == "today":
            v = self.W[J]
            lo, hi = quantile(v, tail), quantile(v, 1 - tail)
        elif kind == "pooled":
            P = self.past(i)
            v = self.W[P]
            lo, hi = quantile(v, tail), quantile(v, 1 - tail)
        elif kind in ("ratio", "nb"):
            P = self.past(i)
            if c.get("groups"):
                self.refresh_groups(i)
                typ_now = self.typical_now_grouped(i)
                prof = self.prof_grouped
                gP = self.group_of_weekday[(P // S) % 7]
                typ = prof[gP, P % S]
            else:
                typ_now = float(np.median(self.W[J]))
                self.refresh_profile(i)
                typ = self.prof[P % S]
            x = self.W[P]
            ok = ~np.isnan(typ)
            x, typ, Pk = x[ok], typ[ok], P[ok]
            if len(x) == 0:  # every past window warned: nothing left to learn the band's tails from
                return None
            if kind == "ratio":
                r = (x + 1) / (typ + 1)
                if c.get("half") or c.get("below_weight"):
                    w = self.wt[Pk] * (0.5 ** ((i - Pk) / S / c["half"]) if c.get("half") else 1.0)
                    qlo, qhi = wquantile(r, w, tail), wquantile(r, w, 1 - tail)
                else:
                    qlo, qhi = quantile(r, tail), quantile(r, 1 - tail)
                lo, hi = (typ_now + 1) * qlo - 1, (typ_now + 1) * qhi - 1
            else:
                mu = np.maximum(typ, 0.5)
                phi = max(float(np.mean(((x - mu) ** 2 - mu) / mu**2)), 0.0)
                z = (x - mu) / np.sqrt(mu + phi * mu**2)
                m = max(typ_now, 0.5)
                sd = np.sqrt(m + phi * m * m)
                lo, hi = m + quantile(z, tail) * sd, m + quantile(z, 1 - tail) * sd
        else:
            raise ValueError(kind)
        mg = c.get("margin", 0.0)
        return max(lo, 0.0) * (1 - mg), hi * (1 + mg)

    def past(self, i):
        """Every learned window of the last 7 days that does not overlap the one being judged."""
        P = np.arange(max(i - DAYS * S, 0), i - WIN + 1)
        return P[self.L[P]]

    def refresh_profile(self, i):
        """Each time of day's typical level: the median of its learned windows, +-30 min, over the last 7 days.
        Recomputed hourly; it moves slowly."""
        if self.prof_at >= 0 and i - self.prof_at < 12:
            return
        self.prof_at = i
        a = i - DAYS * S
        X = np.where((self.L if self.Lp is None else self.Lp)[a:i], self.W[a:i], np.nan).reshape(DAYS, S)
        stack = np.stack([np.roll(X, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
        with np.errstate(all="ignore"):
            import warnings

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                col = np.nanmedian(stack, axis=0)
        prof = np.empty(S)
        prof[(a + np.arange(S)) % S] = col
        self.prof = prof

    # ---- day groups (ITSI-style) ---------------------------------------------------------------------------------
    def refresh_groups(self, i):
        day = i // S
        if self.groups_at != day:
            self.groups_at = day
            X, wd = [], []
            for d in range(day - 28, day):
                if d < 0:
                    continue
                bb = self.b[d * S : (d + 1) * S]
                ll = self.L[d * S : (d + 1) * S]
                if np.isnan(bb).mean() > 0.1 or (~ll).mean() > 0.1:
                    continue
                X.append(np.log1p(np.nan_to_num(bb).reshape(24, -1).sum(1)))
                wd.append(d % 7)
            X, wd = np.array(X), np.array(wd)
            best = (0.2, None)
            if len(X) >= 14:
                for k in (2, 3):
                    lab = kmeans(X, k, self.rng)
                    s = silhouette(X, lab)
                    if s >= best[0]:
                        best = (s, lab)
            if best[1] is None:
                self.group_of_weekday = np.zeros(7, int)
                self.silhouettes.append(0.0)
            else:
                lab = best[1]
                self.group_of_weekday = np.array(
                    [np.bincount(lab[wd == w]).argmax() if (wd == w).any() else 0 for w in range(7)]
                )
                self.silhouettes.append(best[0])
            self.prof_at = -1
        if self.prof_at >= 0 and i - self.prof_at < 12:
            return
        self.prof_at = i
        a = max(i - 28 * S, 0)
        P = np.arange(a, i)
        g = self.group_of_weekday[(P // S) % 7]
        x = np.where(self.L[P], self.W[P], np.nan)
        G = int(self.group_of_weekday.max()) + 1
        prof = np.full((G, S), np.nan)
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for gg in range(G):
                xs = np.where(g == gg, x, np.nan)
                # pad to whole days, then the same +-30 min neighborhood as everywhere else
                pad = (-len(xs)) % S
                Xg = np.concatenate([np.full(pad, np.nan), xs]).reshape(-1, S)
                off = (a - pad) % S
                stack = np.stack([np.roll(Xg, -k, axis=1) for k in range(-NB, NB + 1)]).reshape(-1, S)
                col = np.nanmedian(stack, axis=0)
                prof[gg, (off + np.arange(S)) % S] = col
        self.prof_grouped = prof

    def typical_now_grouped(self, i):
        g = self.group_of_weekday[(i // S) % 7]
        J = i - (np.repeat(np.arange(1, 29), 2 * NB + 1) * S - np.tile(np.arange(-NB, NB + 1), 28))
        J = J[(J >= 0)]
        J = J[self.L[J] & (self.group_of_weekday[(J // S) % 7] == g)]
        if len(J) == 0:
            J, _ = self.same_time(i)
        return float(np.median(self.W[J]))

    # ---- the replay ----------------------------------------------------------------------------------------------
    def run(self):
        """Judge every step from `judge_from` on. Returns the steps each below and above warning started at."""
        fix_first = self.cfg.get("fix_first", True)
        learn_above = self.cfg.get("learn_above", False)
        below_w = self.cfg.get("below_weight", 0.0)
        if self.cfg.get("median_all"):
            self.Lp = self.L.copy()

        def unlearn(idx, side):
            """A window a warning on `side` held: learned anyway, at a weight, or not at all, as the candidate says."""
            if side == "above" and learn_above:
                return
            if side == "below" and below_w > 0:
                self.wt[idx] = below_w
                return
            self.L[idx] = False

        state, excl_until, excl_side = None, -1, None
        pending, pending_side = 0, None
        below_starts, above_starts = [], []
        self.below_ends = []
        for i in range(self.judge_from, self.n):
            if state is not None:
                unlearn(i, state)
            elif i <= excl_until:
                unlearn(i, excl_side)
            side = None
            if not np.isnan(self.W[i]):
                band = self.band(i)
                if band is not None:
                    self.low[i], self.high[i] = band
                    if self.W[i] < band[0]:
                        side = "below"
                    elif self.W[i] > band[1]:
                        side = "above"
            # A warning is raised once the source has been outside for `persist` readings in a row; the readings
            # that waited are not learned either.
            persist = self.cfg.get("persist", 1)
            if side is not None and side != state:
                pending = pending + 1 if side == pending_side else 1
                pending_side = side
                if pending < persist:
                    side = state
                else:
                    unlearn(slice(i - persist + 1, i), side)
            else:
                pending, pending_side = 0, None
            if side != state:
                if state is not None:
                    excl_until, excl_side = i + WIN, state
                    if state == "below":
                        self.below_ends.append(i)
                if side is not None:
                    (below_starts if side == "below" else above_starts).append(i)
                    if fix_first:
                        unlearn(i, side)
                state = side
            self.state_at[i] = {None: 0, "below": -1, "above": 1}[state]
        return below_starts, above_starts


def score(run, below_starts, above_starts, labeled, drops=(0.0, 0.5, 0.1)):
    """False warnings per 30 days of judged time outside labeled incidents, and the share of injected drops caught.

    A drop of factor f starting at step t0 is caught if any window wholly inside it within its first hour (ending
    t0+6 .. t0+12) is below the band: rows x f < low."""
    n = run.n
    judged = ~np.isnan(run.low)
    judged[: run.judge_from] = False
    clean = judged & ~labeled
    months = clean.sum() * STEP_MIN / (60 * 24 * 30)
    fb = sum(1 for s in below_starts if not labeled[s])
    fa = sum(1 for s in above_starts if not labeled[s])
    # Incidents: below warnings that start more than an hour after the last one ended count once.
    ends = run.below_ends + [n] * (len(below_starts) - len(run.below_ends))
    incidents, last_end = 0, -(10**9)
    for s, e in zip(below_starts, ends):
        if not labeled[s] and s - last_end > 12:
            incidents += 1
        last_end = e
    out = {"months": months, "false_below": fb / months if months else np.nan, "above_info": fa / months if months else np.nan,
           "incidents": incidents / months if months else np.nan}
    # injections
    # With `persist` k, the drop must hold k readings in a row below the band, the last within the first hour.
    k = run.cfg.get("persist", 1)
    t0 = np.arange(run.judge_from, n - 2 * WIN - k)
    ends = t0[:, None] + np.arange(WIN, 2 * WIN + k)[None, :]
    ok = clean[ends].all(1) & ~labeled[t0]
    for f in drops:
        caught_w = (run.W * f < run.low).astype(int)
        held = np.convolve(caught_w, np.ones(k, int))[: n] >= k  # held[i]: windows i-k+1..i all below
        caught = held[ends[:, k - 1 :]].any(1)
        out[f"caught_{f}"] = float(caught[ok].mean()) if ok.any() else np.nan
    out["injections"] = int(ok.sum())
    return out


CANDIDATES = {
    "today": {"kind": "today", "p": 0.99, "fix_first": False},
    "today_fixed": {"kind": "today", "p": 0.99},
    "today_m10": {"kind": "today", "p": 0.99, "margin": 0.1},
    "pooled_m10": {"kind": "pooled", "p": 0.999, "margin": 0.1},
    "ratio": {"kind": "ratio", "p": 0.999},
    "ratio_m10": {"kind": "ratio", "p": 0.999, "margin": 0.1},
    "nb_m10": {"kind": "nb", "p": 0.999, "margin": 0.1},
    "ratio_m10_half3": {"kind": "ratio", "p": 0.999, "margin": 0.1, "half": 3},
    "ratio_m10_groups": {"kind": "ratio", "p": 0.999, "margin": 0.1, "groups": True},
    "ratio_m20": {"kind": "ratio", "p": 0.999, "margin": 0.2},
    "ratio_m10_persist3": {"kind": "ratio", "p": 0.999, "margin": 0.1, "persist": 3},
    "ratio_m20_persist3": {"kind": "ratio", "p": 0.999, "margin": 0.2, "persist": 3},
    "ratio_m20_persist3_groups": {"kind": "ratio", "p": 0.999, "margin": 0.2, "persist": 3, "groups": True},
    "ratio_m20_persist4": {"kind": "ratio", "p": 0.999, "margin": 0.2, "persist": 4},
    "m20p4_learn_above": {"kind": "ratio", "p": 0.999, "margin": 0.2, "persist": 4, "learn_above": True},
    "m20p4_below_w10": {"kind": "ratio", "p": 0.999, "margin": 0.2, "persist": 4, "learn_above": True, "below_weight": 0.1},
    "m20p4_below_w01": {"kind": "ratio", "p": 0.999, "margin": 0.2, "persist": 4, "learn_above": True, "below_weight": 0.01},
    "m20p4_median_all": {"kind": "ratio", "p": 0.999, "margin": 0.2, "persist": 4, "median_all": True},
}
