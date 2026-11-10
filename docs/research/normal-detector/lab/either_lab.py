"""Band and silence rule side by side: a stop is caught by whichever fires first.

    python3 either_lab.py nab/ either.json   # count series, modeled series, sparse sources and the ten logs

The band is the recommended detector (four widths, k = 4, delta = 0.2, 28 days behind the long widths, gate 10);
the silence rule runs at margin 1.5. A bucket is judged when either rule judges it. A false alarm is a start of
the union of the two rules' alarms outside a labeled incident. A stop at a judged bucket is caught at the
earlier of the two rules' delays.
"""

import json
import os
import sys
from multiprocessing import Pool

import numpy as np

from extend_lab import HORIZON, HORIZONS, below_over, make, part_path
from multi_lab import first_hit
from quiet_lab import STEP, Quiet
from real_logs import load_logs
from run_all import DAY, MODELED, NAB, S, modeled, nab_series

BAND = dict(widths=(1, 6, 72, 288), k=4, margin=0.2, longhistory=28, minrows=10, wtyp=True)
MARGIN = 1.5


def stop_delays(b, lab, judge_from):
    d = make(b, BAND, judge_from)
    held_b = d.run()
    judged_b = np.zeros(len(b), bool)
    for w in d.widths:
        judged_b |= ~np.isnan(d.low[w])
    q = Quiet(b, judge_from, MARGIN)
    q.learned &= ~lab
    held_q = q.run()
    judged_q = ~np.isnan(q.longest)
    for j in (judged_b, judged_q):
        j[:judge_from] = False
    held = held_b | held_q
    judged = judged_b | judged_q
    clean = judged & ~lab
    idx = np.arange(len(b))
    sample = clean & (idx % 2 == 0) & ~held & ~np.roll(held, 1)
    sample[len(b) - HORIZON :] = False
    t0 = np.where(sample)[0]
    # the band's delay on a stop at each sampled bucket
    agree = np.logical_or.reduce([below_over(b, d.low[w], sample, w, 0.0, HORIZON) for w in d.widths])
    t_band = first_hit(agree, BAND["k"])
    # the silence rule's delay
    run_before = np.zeros(len(b), int)
    r = 0
    for i in range(len(b)):
        r = r + 1 if q.z[i] else 0
        run_before[i] = r
    L = q.longest[t0]
    t_quiet = np.where(np.isnan(L), np.inf, np.maximum((np.floor(MARGIN * np.nan_to_num(L)) + 1 - run_before[np.maximum(t0 - 1, 0)]) * STEP, STEP))
    out = dict(months=float(clean.sum() * STEP / (60 * 24 * 30)), months_band=float((judged_b & ~lab).sum() * STEP / (60 * 24 * 30)),
               months_quiet=float((judged_q & ~lab).sum() * STEP / (60 * 24 * 30)))
    for name, h in (("band", held_b), ("quiet", held_q), ("either", held)):
        starts = [i for i in range(1, len(b)) if h[i] and not h[i - 1]]
        out[f"false_{name}"] = sum(1 for s in starts if not lab[s])
    for name, t in (("band", t_band), ("quiet", t_quiet), ("either", np.minimum(t_band, t_quiet))):
        for h in HORIZONS:
            out[f"by{h}_{name}"] = float(np.mean(t <= h)) if len(t) else None
        fin = t[np.isfinite(t)]
        out[f"median_{name}"] = float(np.median(fin)) if len(fin) else None
        out[f"caught_{name}"] = float(np.mean(np.isfinite(t))) if len(t) else None
    out["samples"] = int(len(t0))
    return out


def job(args):
    dataset, series, seed, b, lab = args
    judge_from = 28 * DAY if len(b) >= 35 * DAY else 7 * DAY
    return dict(dataset=dataset, series=series, seed=seed, result=stop_delays(b, lab, judge_from))


def main(root, out_path):
    part_dir = out_path + ".parts"
    os.makedirs(part_dir, exist_ok=True)
    labels = json.load(open(os.path.join(root, "labels.json")))
    jobs = [("modeled", name, seed, modeled(name, seed), np.zeros(56 * DAY, bool)) for name in MODELED for seed in (1, 2, 3)]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        jobs.append(("nab", name, 0, b, lab))
    for name, b, lab in load_logs("real"):
        jobs.append(("logs", name, 0, b, lab))
    for name, rate in {"12 rows an hour": 1.0, "4 rows an hour": 1 / 3, "1 row an hour": 1 / 12, "1 row a day": 1 / 288}.items():
        for seed in (1, 2):
            rng = np.random.default_rng(seed)
            jobs.append(("sparse", name, seed, rng.poisson(rate, 84 * S).astype(float), np.zeros(84 * S, bool)))
    order = [(j[0], j[1], j[2]) for j in jobs]
    todo = [j for j in jobs if not os.path.exists(part_path(part_dir, f"{j[0]}-{j[1]}", j[2]))]
    with Pool(4) as pool:
        for r in pool.imap_unordered(job, todo):
            json.dump(r, open(part_path(part_dir, f"{r['dataset']}-{r['series']}", r["seed"]), "w"))
    results = [json.load(open(part_path(part_dir, f"{d}-{s}", seed))) for d, s, seed in order]
    json.dump(results, open(out_path, "w"), indent=0)
    for x in results:
        r = x["result"]
        f = lambda k: "—" if r.get(k) is None else (f"{r[k]:.0%}" if k.startswith("by") else f"{r[k]:.1f}")
        print(f'{x["dataset"]:8s} {x["series"][:28]:28s} {x["seed"]} months {f("months")} false b/q/e {r["false_band"]}/{r["false_quiet"]}/{r["false_either"]} '
              f'stop within hour b/q/e {f("by60_band")}/{f("by60_quiet")}/{f("by60_either")} day {f("by1440_band")}/{f("by1440_quiet")}/{f("by1440_either")} median e {f("median_either")}')


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
