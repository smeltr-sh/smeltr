"""PR #741 (C 2/7 split) against one code path for every source (Section 6.7). One part per series and
config, longest first, so a run cut short resumes where it stopped. The PR #741 parts are copied from split_usual.

    python3 one_path.py
"""
import json
import os
from multiprocessing import Pool

import numpy as np

import extend_lab as e
from real_logs import load_logs
from run_all import MODELED, NAB, modeled, nab_series, with_added

C = dict(e.BALANCED, utc=True, **e.FORMS["C"])
CONFIGS = [dict(C, form="C 2/7 split", split=True), dict(C, form="A one kind split", split=True, onekind=True),
           dict(C, form="B one rule", split=True, onekind=True, kinddays=7),
           dict(C, form="B even parts", split=True, onekind=True, kinddays=7, evenparts=True),
           dict(C, form="B whole parts", split=True, onekind=True, kinddays=7, wholeparts=True),
           dict(C, form="O2", split=True, kinddays=7), dict(C, form="O2 pairs", split=True, kinddays=7, pairs=True),
           dict(C, form="B pairs", split=True, onekind=True, kinddays=7, pairs=True),
           dict(C, form="M matched", split=True, kinddays=7, matched=True)]
SHAPES = MODELED + ["office week", "nightlife week"]
OUT = "one_path"


def series():
    """(dataset, name, seed, rows, labels, buckets from UTC midnight to the first) for every series."""
    out = []
    labels = with_added(json.load(open("nab/labels.json")))
    for name in NAB:
        b, lab = nab_series("nab", name, labels)
        out.append(("nab", name, 0, b, lab, e.shift_of("nab", name)))
    for (name, b, lab), sh in zip(load_logs("real"), e.log_shifts("real")):
        out.append(("logs", name, 0, b, lab, sh))
    for shape in SHAPES:
        for seed in (1, 2, 3):
            b = modeled(shape, seed)
            out.append(("modeled", shape, seed, b, np.zeros(len(b), bool), e.UTC_SHIFT))
    return out


def part(dataset, name, seed, c):
    tag = f"{dataset}-{name}-{seed}-{c['form']}".replace("/", "-").replace(" ", "_").replace(",", "")
    return os.path.join(OUT + ".parts", tag + ".json")


def one(args):
    dataset, name, seed, b, lab, shift, c = args
    r = dict(e.job((dataset, name, seed, b, lab, [c], shift))["configs"][0], dataset=dataset, series=name, seed=seed)
    json.dump(r, open(part(dataset, name, seed, c), "w"))
    return r


if __name__ == "__main__":
    os.makedirs(OUT + ".parts", exist_ok=True)
    all_series = series()
    jobs = [(*s, c) for s in all_series for c in CONFIGS if not os.path.exists(part(*s[:3], c))]
    jobs.sort(key=lambda j: -len(j[3]))
    with Pool(int(os.environ.get("PROCS", 4))) as pool:
        for r in pool.imap_unordered(one, jobs):
            print(r["series"], r["seed"], r["form"], r["false_count"], flush=True)
    res = [json.load(open(part(*s[:3], c))) for s in all_series for c in CONFIGS]
    json.dump(res, open(OUT + ".json", "w"), indent=0)
