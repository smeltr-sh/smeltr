"""How far back the gap remembers a silence, on a feed that fires about once in 15 days.

    python3 gap_history_lab.py gap_history.json   # run where real/ is

The gap at margin 1.5 with the ten-silence gate, under three memories:
    28d      the completed silences of the past 28 days
    last30   those, and the last 30 completed silences however old
    own30    a silence is remembered for 28 days or 30 times its own length, whichever is longer
on modeled rare feeds over four years, the sparse sources and the ten logs.
"""

import json
import sys
from collections import deque
from multiprocessing import Pool

import numpy as np

from real_logs import load_logs
from run_all import S

MARGIN, MIN_SPELLS, D28 = 1.5, 10, 28 * S
MODES = ("28d", "last30", "own30")
YEARS = 4


def rare(seed, burst=0.0, jitter=False):
    """Rows per 5-minute bucket over four years: an event about every 15 days, a share of them a burst of rows."""
    rng = np.random.default_rng(seed)
    n = YEARS * 365 * S
    b = np.zeros(n)
    t = 0.0
    while True:
        t += (rng.uniform(10, 20) if jitter else rng.exponential(15)) * S
        if t >= n:
            return b
        if rng.random() < burst:  # 20 to 200 rows over 1 to 6 hours
            at = t + rng.uniform(0, rng.uniform(12, 72), rng.integers(20, 201))
            np.add.at(b, np.minimum(at.astype(int), n - 1), 1)
        else:
            b[int(t)] += 1


def run(b, lab, judge_from, mode):
    z = np.nan_to_num(b) == 0
    n = len(b)
    recent, last, kept = deque(), deque(maxlen=30), []  # completed learned silences as (end, length)
    L, r, alarmed, tainted = np.nan, 0, False, False
    false = judged = held = 0
    delays, lucky_stops = [], 0
    for i in range(n):
        if z[i]:
            r += 1
            tainted |= bool(lab[i])
        else:
            if r and not alarmed and not tainted:
                recent.append((i, r)), last.append((i, r)), kept.append((i, r))
            r, alarmed, tainted = 0, False, False
        if i < judge_from or i % 12:
            if i >= judge_from and not np.isnan(L) and r > MARGIN * L and not alarmed:
                alarmed = True
                false += not lab[i]
            continue
        while recent and recent[0][0] <= i - D28:
            recent.popleft()
        if mode == "28d":
            sp = [x[1] for x in recent]
        elif mode == "last30":
            sp = [x[1] for x in set(recent) | set(last)]
        else:
            kept = [x for x in kept if i - x[0] < max(D28, 30 * x[1])]
            sp = [x[1] for x in kept]
        L = max(sp) if len(sp) >= MIN_SPELLS else np.nan
        if not np.isnan(L):
            if r > MARGIN * L:
                if not alarmed:
                    alarmed = True
                    false += not lab[i]
                held += 1
            elif not lab[i]:
                judged += 1
                delays.append(max(np.floor(MARGIN * L) + 1 - r, 1))
    hours = (n - judge_from) / 12
    d = np.array(delays) * 5 / 1440
    return dict(false=int(false), years=float(hours / (24 * 365)), judged=float((judged + held) / hours), held=float(held / hours),
                stop_median_days=float(np.median(d)) if len(d) else None, stop_p90_days=float(np.quantile(d, 0.9)) if len(d) else None)


def job(args):
    dataset, series, seed, b, lab, judge_from = args
    return dict(dataset=dataset, series=series, seed=seed, result={m: run(b, lab, judge_from, m) for m in MODES})


def main(out_path):
    jobs = []
    for name, kw in {"once in 15 days": {}, "once in 15 days, 3 in 10 a burst": dict(burst=0.3), "every 10 to 20 days, 3 in 10 a burst": dict(burst=0.3, jitter=True)}.items():
        for seed in range(1, 6):
            b = rare(seed, **kw)
            jobs.append(("rare", name, seed, b, np.zeros(len(b), bool), 0))
    for name, rate in {"12 rows an hour": 1.0, "4 rows an hour": 1 / 3, "1 row an hour": 1 / 12, "1 row a day": 1 / 288}.items():
        for seed in (1, 2):
            rng = np.random.default_rng(seed)
            jobs.append(("sparse", name, seed, rng.poisson(rate, 84 * S).astype(float), np.zeros(84 * S, bool), 28 * S))
    for name, b, lab in load_logs("real"):
        jobs.append(("logs", name, 0, b, lab, 28 * S if len(b) >= 35 * S else 7 * S))
    with Pool(2) as pool:
        results = pool.map(job, jobs)
    json.dump(results, open(out_path, "w"), indent=0)
    for x in results:
        print(x["dataset"], x["series"], x["seed"], " | ".join(
            f'{m}: false {r["false"]} judged {r["judged"]:.2f} held {r["held"]:.2f} stop {r["stop_median_days"] if r["stop_median_days"] is None else round(r["stop_median_days"], 2)} d'
            for m, r in x["result"].items()), flush=True)


if __name__ == "__main__":
    main(sys.argv[1])
