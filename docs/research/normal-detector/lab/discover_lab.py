"""Guessing the sensor field. A source's rows carry several string fields; which ones partition the rows into
values each seen steadily? A field qualifies when it has at most CAP values over the learning week and every
value's longest silence at any time of day is at most QUIET buckets. Modeled fields on one source:

    sensor      10 values, each a steady sender                      -> should qualify
    region      3 values, steady                                      -> qualifies (harmless: a region going quiet matters)
    method      GET 70% / POST 25% / DELETE 5%, steady mix            -> qualifies
    status      ok 97%, error in bursts a few times a day             -> must not qualify (error is silent for hours)
    user        2,000 values, Zipf                                    -> must not qualify (cardinality)
    build       one value per day, rotating                           -> must not qualify (each value silent after its day)

    python3 discover_lab.py discover.json
"""

import json
import sys

import numpy as np

from normal_lab import S
from run_all import DAY
from share_lab import senders
from silence_lab import runs_of_zero

CAP = 256
QUIET = 12  # an hour
DAYS = 14
JUDGE = 7 * DAY


def field_counts(rng, total, kind):
    """Rows per 5-minute bucket per value of the field, given the source's total rows per bucket."""
    n = len(total)
    if kind == "sensor":
        return senders([(total.mean() / 10, 0, 0.3)] * 10, int(rng.integers(1 << 30)))[:, :n]
    if kind == "region":
        p = np.array([0.5, 0.3, 0.2])
        return np.array([rng.binomial(total.astype(int), pi) for pi in p]).astype(float)
    if kind == "method":
        p = np.array([0.7, 0.25, 0.05])
        return np.array([rng.binomial(total.astype(int), pi) for pi in p]).astype(float)
    if kind == "status":
        burst = np.zeros(n, bool)
        for d in range(n // S):
            for _ in range(rng.integers(1, 4)):
                a = d * S + rng.integers(S)
                burst[a : a + rng.integers(1, 4)] = True
        err = np.where(burst, rng.binomial(total.astype(int), 0.2), 0).astype(float)
        return np.array([total - err, err])
    if kind == "user":
        w = 1 / np.arange(1, 2001) ** 1.1
        w /= w.sum()
        return rng.multinomial(total.astype(int), w).T.astype(float)
    if kind == "employee":
        # 120 people, each active only on their own 8-hour shift (three shifts), weekdays, with lunch away.
        shift = rng.integers(0, 3, 120)
        t = np.arange(n)
        hour = (t % S) * 24 / S
        day = t // S
        on = np.array([((hour >= 8 * sh) & (hour < 8 * sh + 8) & (day % 7 < 5)) for sh in shift])
        rate = rng.uniform(0.2, 2.0, 120)[:, None] / 5  # rows per 5-minute bucket while working
        return rng.poisson(rate * on).astype(float)
    if kind == "build":
        day = np.arange(n) // S
        out = np.zeros((n // S + 1, n))
        out[day, np.arange(n)] = total
        return out
    raise ValueError(kind)


def qualifies(counts, sample=0.5, rng=None):
    """(qualifies, values, longest silence in buckets over the week, as seen by the tally)."""
    seen = rng.binomial(counts.astype(int), sample)
    present = (seen[:, :JUDGE].sum(1) > 0)
    values = int(present.sum())
    if values > CAP:
        return False, values, None
    longest = max(runs_of_zero(seen[k, :JUDGE] == 0).max() for k in np.where(present)[0])
    return longest <= QUIET, values, int(longest)


def main(out_path):
    out = []
    for seed in (1, 2, 3):
        rng = np.random.default_rng(seed)
        total = senders([(20000.0, 0, 0.3)], seed)[0][: DAYS * S]
        for kind in ("sensor", "region", "method", "status", "user", "employee", "build"):
            q, values, longest = qualifies(field_counts(rng, total, kind), rng=rng)
            out.append(dict(seed=seed, field=kind, qualifies=bool(q), values=values, longest_silence_min=None if longest is None else longest * 5))
    json.dump(out, open(out_path, "w"), indent=0)
    for r in out:
        print(r)


if __name__ == "__main__":
    main(sys.argv[1])
