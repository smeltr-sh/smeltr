"""Every candidate over every series: modeled feeds (fixed seeds) and NAB's public count series.

    python3 fetch_nab.py nab/      # once
    python3 run_all.py nab/ results.csv
"""

import csv
import datetime as dt
import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from normal_lab import CANDIDATES, S, STEP_MIN, WIN, Run, score

DAY = S


# ---- modeled feeds -------------------------------------------------------------------------------------------------
def modeled(name, seed, days=56):
    """Rows per 5-minute bucket: a level, a shape over the day and week, and noise that drifts (AR(1) in log space,
    about 50 minutes of memory), drawn as Poisson counts."""
    rng = np.random.default_rng(seed)
    n = days * S
    t = np.arange(n) / S  # days
    hour = (t % 1) * 24
    daily = 1 + 0.7 * np.sin(2 * np.pi * (hour - 9) / 24)
    weekday = (np.floor(t) % 7) < 5
    spec = {
        "steady": (200.0, np.ones(n), 0.1),
        "variable": (200.0, np.ones(n), 0.6),
        "daily": (200.0, daily, 0.3),
        "thin daily": (3.0, 1 + 0.95 * np.sin(2 * np.pi * (hour - 9) / 24), 0.3),
        "weekday/weekend": (200.0, daily * np.where(weekday, 1.0, 0.2), 0.3),
        "steady, falling 3%/day": (200.0, 0.97**t, 0.1),
        "variable, falling 3%/day": (200.0, 0.97**t, 0.3),
    }[name]
    level, shape, sigma = spec
    rho = 0.9
    e = np.empty(n)
    e[0] = rng.normal(0, sigma)
    xi = rng.normal(0, sigma * np.sqrt(1 - rho**2), n)
    for k in range(1, n):
        e[k] = rho * e[k - 1] + xi[k]
    lam = level * shape * np.exp(e - sigma**2 / 2)
    return rng.poisson(lam).astype(float)


MODELED = ["steady", "variable", "daily", "thin daily", "weekday/weekend", "steady, falling 3%/day", "variable, falling 3%/day"]


# ---- NAB -----------------------------------------------------------------------------------------------------------
NAB = [
    "realTweets/Twitter_volume_AAPL",
    "realTweets/Twitter_volume_AMZN",
    "realTweets/Twitter_volume_CRM",
    "realTweets/Twitter_volume_CVS",
    "realTweets/Twitter_volume_FB",
    "realTweets/Twitter_volume_GOOG",
    "realTweets/Twitter_volume_IBM",
    "realTweets/Twitter_volume_KO",
    "realTweets/Twitter_volume_PFE",
    "realTweets/Twitter_volume_UPS",
    "realKnownCause/nyc_taxi",
    "realAWSCloudwatch/elb_request_count_8c0756",
    "realAWSCloudwatch/ec2_network_in_257a54",
    "realAWSCloudwatch/ec2_network_in_5abac7",
]


def parse(ts):
    return dt.datetime.strptime(ts.split(".")[0], "%Y-%m-%d %H:%M:%S")


def nab_series(root, name, labels):
    rows = list(csv.reader(open(os.path.join(root, name + ".csv"))))[1:]
    ts = [parse(r[0]) for r in rows]
    v = [float(r[1]) for r in rows]
    start = dt.datetime(ts[0].year, ts[0].month, ts[0].day)
    step = (ts[1] - ts[0]).total_seconds() / 60
    per = int(round(step / STEP_MIN))  # 1 for 5-minute data, 6 for the taxi's 30 minutes
    n = int((ts[-1] - start).total_seconds() // (STEP_MIN * 60)) + per
    b = np.full(n, np.nan)
    for t, x in zip(ts, v):
        i = int((t - start).total_seconds() // (STEP_MIN * 60))
        b[i : i + per] = x / per  # a coarser bucket is spread evenly over its five-minute ones
    lab = np.zeros(n, bool)
    for a, z in labels.get(name + ".csv", []):
        ia = max(int((parse(a) - start).total_seconds() // (STEP_MIN * 60)), 0)
        iz = min(int((parse(z) - start).total_seconds() // (STEP_MIN * 60)), n - 1)
        lab[ia : iz + WIN + 1] = True  # every window that holds a labeled bucket
    return b, lab


def job(args):
    dataset, series, seed, cand, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    if CANDIDATES[cand].get("groups") and judge_from < 28 * DAY:
        return None
    r = Run(b, CANDIDATES[cand], judge_from, seed=seed)
    below, above = r.run()
    out = score(r, below, above, lab)
    out.update(dataset=dataset, series=series, seed=seed, candidate=cand)
    if r.silhouettes:
        out["silhouette"] = float(np.mean(r.silhouettes))
        out["grouped_days"] = float(np.mean([s > 0 for s in r.silhouettes]))
    return out


def main(root, out_path, only=None):
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = []
    for name in MODELED:
        for seed in (1, 2, 3):
            b = modeled(name, seed)
            for cand in (only or CANDIDATES):
                jobs.append(("modeled", name, seed, cand, b, np.zeros(len(b), bool)))
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        for cand in (only or CANDIDATES):
            jobs.append(("nab", name, 0, cand, b, lab))
    with Pool(4) as pool:
        results = [r for r in pool.imap_unordered(job, jobs) if r is not None]
    keys = ["dataset", "series", "seed", "candidate", "months", "false_below", "incidents", "above_info", "caught_0.0", "caught_0.5", "caught_0.1", "injections", "silhouette", "grouped_days"]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, keys)
        w.writeheader()
        for r in sorted(results, key=lambda r: (r["dataset"], r["series"], r["seed"], r["candidate"])):
            w.writerow({k: r.get(k, "") for k in keys})


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3].split(",") if len(sys.argv) > 3 else None)
