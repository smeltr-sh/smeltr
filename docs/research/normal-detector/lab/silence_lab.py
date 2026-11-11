"""A sender dying is a silence. Per value of a watched field: minutes since it was last seen in a tallied batch,
against the longest silence it had in the past week. Warn when the silence passes that longest by a margin.

    python3 silence_lab.py silence.json

Buckets are 5 minutes; a silence is a run of buckets in which the tally saw no row of the value. The learned
longest run comes from the last 7 days outside warnings; a warning ends when the value is seen again, and the
run that warned is not learned.
"""

import json
import sys
from multiprocessing import Pool

import numpy as np

from share_lab import DAYS, JUDGE, SAMPLE, senders
from run_all import DAY
from normal_lab import NB, S, STEP_MIN

HORIZON = 72
MIXES = {
    "10 sensors, 7 rows/s each": [(2000.0, 0, 0.3)] * 10,
    "100 sensors, 1 row/s each": [(300.0, 0, 0.3)] * 100,
    "100 sensors, 1 row/min each": [(5.0, 0, 0.3)] * 100,
    "10 sensors, 3 regions": [(2000.0, 0, 0.3)] * 4 + [(2000.0, 6, 0.3)] * 3 + [(2000.0, -8, 0.3)] * 3,
    "10 sensors, nightly quiet": [(2000.0, 0, 0.3)] * 10,  # shape made deep below
}


def runs_of_zero(z):
    """For each bucket, the length of the zero run ending there (0 where seen)."""
    out = np.zeros(len(z), int)
    for i in range(len(z)):
        out[i] = out[i - 1] + 1 if z[i] and i > 0 else int(z[i])
    return out


def judge(seen_k, judge_from, margin, floor=1):
    """Warning starts (bucket index) and the learned longest silence per bucket."""
    z = seen_k == 0
    run = runs_of_zero(z)
    n = len(z)
    learned = np.ones(n, bool)
    longest = np.full(n, np.nan)
    starts, state = [], False
    for i in range(judge_from, n):
        # The longest silence at this time of day (+-30 min) on each past day; the silence being judged teaches
        # nothing until it ends. A sensor quiet every night is not dead at night; it is dead by day in minutes.
        past = (i - np.repeat(np.arange(1, 8), 2 * NB + 1) * S + np.tile(np.arange(-NB, NB + 1), 7))
        past = past[(past >= 0) & (past < i - run[i])]
        past = past[learned[past]]
        L = run[past].max() if len(past) else 0
        longest[i] = L
        limit = max(np.ceil(L * (1 + margin)), L + floor)
        if state:
            learned[i] = False
            if not z[i]:
                state = False
        elif run[i] > limit:
            state = True
            starts.append(i)
            learned[i - run[i] + 1 : i + 1] = False
    return starts, longest


def job(args):
    mix, seed, margin = args
    spec = MIXES[mix]
    X = senders(spec, seed)
    if mix == "10 sensors, nightly quiet":
        t = np.arange(X.shape[1]) / S
        hour = (t % 1) * 24
        X = X * ((hour < 1) | (hour > 5))[None, :]  # nothing from 1 to 5 a.m.
    K, n = X.shape
    rng = np.random.default_rng(1000 + seed)
    seen = rng.binomial(X.astype(int), SAMPLE).astype(float)
    out = dict(mix=mix, seed=seed, margin=margin, senders=K)
    fp, months, Ls = 0, 0.0, []
    for k in range(K):
        starts, longest = judge(seen[k], JUDGE, margin)
        fp += len(starts)
        months = (n - JUDGE) * STEP_MIN / (60 * 24 * 30)
        Ls.append(np.nanmedian(longest[JUDGE:]))
    out["false_per_source_month"] = fp / months
    out["learned_longest_silence_min"] = float(np.median(Ls)) * STEP_MIN
    delays = []
    for t0 in rng.choice(np.arange(JUDGE + DAY, n - HORIZON - 1), 24, replace=False):
        k = int(rng.integers(K))
        s2 = seen[k].copy()
        s2[t0:] = 0
        starts, _ = judge(s2, JUDGE, margin)
        d = [s for s in starts if t0 <= s < t0 + HORIZON]
        delays.append((d[0] - t0 + 1) * STEP_MIN if d else np.inf)
    d = np.array(delays)
    out["one dies"] = dict(by15=float(np.mean(d <= 15)), by60=float(np.mean(d <= 60)), median=float(np.median(d[np.isfinite(d)])) if np.isfinite(d).any() else None, never6h=float(np.mean(np.isinf(d))))
    return out


def main(out_path):
    jobs = [(mix, seed, m) for mix in MIXES for seed in (1, 2) for m in (0.0, 0.5, 1.0)]
    with Pool(2) as pool:
        results = list(pool.imap_unordered(job, jobs))
    json.dump(results, open(out_path, "w"), indent=0)


if __name__ == "__main__":
    main(sys.argv[1])
