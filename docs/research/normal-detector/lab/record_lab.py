"""The record rule against today's gap, on rare bursty feeds, long modeled sources, the sparse sources and the ten logs.

    python3 record_lab.py record.json   # run where real/ is

today   warn when the silence passes 1.5 times the longest completed silence of the past 28 days, given ten of them
record  warn when the silence passes every completed silence of the past year; while less than a year is watched,
        the longest plus m * ln(year / watched), m = sum(s^2) / (2 sum(s)) over the remembered silences
year    the same times 1.5: today's gap remembering a year, with that learning term in place of the ten-silence gate
Silences that warned or that a label covers are not remembered. Both judge from the same bucket.
"""

import json
import sys
from collections import deque
from multiprocessing import Pool

import numpy as np

from real_logs import load_logs
from run_all import S

YEAR = 365 * S
D28 = 28 * S


def bursty(seed, years, gap, burst=True):
    """Rows per bucket: events at random (gap 'exp') or every 10 to 18 days ('regular'), each a burst or one row."""
    rng = np.random.default_rng(seed)
    n = int(years * 365 * S)
    b = np.zeros(n)
    t = 0.0
    while True:
        t += (rng.exponential(14) if gap == "exp" else rng.uniform(10, 18)) * S
        if t >= n:
            return b
        if burst:  # 20 to 200 rows over 1 to 6 hours
            at = t + rng.uniform(0, rng.uniform(12, 72), rng.integers(20, 201))
            np.add.at(b, np.minimum(at.astype(int), n - 1), 1)
        else:
            b[int(t)] += 1


def steady(seed, years, kind):
    rng = np.random.default_rng(seed)
    n = int(years * 365 * S)
    if kind == "1 row an hour":
        return rng.poisson(1 / 12, n).astype(float)
    if kind == "1 row a day":
        return rng.poisson(1 / 288, n).astype(float)
    if kind == "hourly heartbeat":
        b = np.zeros(n)
        b[::12] = 1
        return b
    if kind == "daytime only":  # 4 rows an hour from 08:00 to 18:00, none at night
        tod = np.arange(n) % S
        return np.where((tod >= 96) & (tod < 216), rng.poisson(1 / 3, n), 0).astype(float)


def run(b, lab, judge_from, rule):
    z = np.nan_to_num(b) == 0
    n = len(b)
    stair = deque()  # remembered silences (end, length), lengths falling: the front is the longest
    recent = deque()  # for today's rule: every remembered silence of the past 28 days
    s1 = s2 = 0.0  # sums of s and s^2 over every remembered silence (only read in the first year)
    r, warned, tainted = 0, False, False
    T = np.inf
    out = dict(false_first=0, false_after=0, judged_first=0, judged_after=0, delays_first=[], delays_after=[])
    for i in range(n):
        if z[i]:
            r += 1
            tainted |= bool(lab[i])
        else:
            if r and not warned and not tainted:
                while stair and stair[-1][1] <= r:
                    stair.pop()
                stair.append((i, r))
                recent.append((i, r))
                s1 += r
                s2 += r * r
            r, warned, tainted = 0, False, False
        if i < judge_from:
            continue
        if (i - judge_from) % 12 == 0:  # refresh hourly
            if rule == "today":
                while recent and recent[0][0] <= i - D28:
                    recent.popleft()
                T = 1.5 * max(x[1] for x in recent) if len(recent) >= 10 else np.inf
            else:
                while stair and stair[0][0] <= i - YEAR:
                    stair.popleft()
                if not stair:
                    T = np.inf
                elif i >= YEAR:
                    T = stair[0][1]
                else:
                    T = stair[0][1] + s2 / (2 * s1) * np.log(YEAR / i)
                if rule == "year":
                    T *= 1.5
        first = i < YEAR
        if np.isfinite(T):
            if r > T:
                if not warned:
                    warned = True
                    if not lab[i]:
                        out["false_first" if first else "false_after"] += 1
            elif not lab[i]:
                out["judged_first" if first else "judged_after"] += 1
                if i % 12 == 0:
                    out["delays_first" if first else "delays_after"].append(max(np.floor(T) + 1 - r, 1))
    res = {}
    for part in ("first", "after"):
        span = (min(n, YEAR) - judge_from) if part == "first" else max(n - YEAR, 0)
        d = np.array(out[f"delays_{part}"]) * 5 / 60  # hours
        res[part] = dict(years=float(span / YEAR), false=out[f"false_{part}"], judged=float(out[f"judged_{part}"] / span) if span > 0 else None,
                         median_h=float(np.median(d)) if len(d) else None, p90_h=float(np.quantile(d, 0.9)) if len(d) else None)
    return res


def job(args):
    dataset, series, seed, b, lab, judge_from = args
    return dict(dataset=dataset, series=series, seed=seed, result={rule: run(b, lab, judge_from, rule) for rule in ("today", "record", "year")})


def main(out_path):
    jobs = []
    for name, kw in {"every 14 days at random, bursts": dict(gap="exp"), "every 14 days at random, single rows": dict(gap="exp", burst=False),
                     "every 10 to 18 days, bursts": dict(gap="regular")}.items():
        for seed in range(1, 6):
            jobs.append(("rare", name, seed, bursty(seed, 6, **kw), None, D28))
    for kind in ("1 row an hour", "1 row a day", "hourly heartbeat", "daytime only"):
        for seed in range(1, 4):
            jobs.append(("steady", kind, seed, steady(seed, 4, kind), None, D28))
    for name, rate in {"12 rows an hour": 1.0, "4 rows an hour": 1 / 3, "1 row an hour": 1 / 12, "1 row a day": 1 / 288}.items():
        for seed in (1, 2):
            rng = np.random.default_rng(seed)
            jobs.append(("sparse", name, seed, rng.poisson(rate, 84 * S).astype(float), None, D28))
    for name, b, lab in load_logs("real"):
        jobs.append(("logs", name, 0, b, lab, D28 if len(b) >= 35 * S else 7 * S))
    jobs = [(d, s, seed, b, np.zeros(len(b), bool) if lab is None else lab, j) for d, s, seed, b, lab, j in jobs]
    with Pool(4) as pool:
        results = pool.map(job, jobs)
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1])
