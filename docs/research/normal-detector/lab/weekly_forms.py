"""None, C and H on the office and nightlife weeks, days split at UTC midnight (Section 6.7)."""
import json
from multiprocessing import Pool

import numpy as np

import extend_lab as e
from run_all import modeled

SHAPES = ("office week", "nightlife week")
CONFIGS = [dict(e.BALANCED, form=f, utc=True, **e.FORMS[f]) for f in ("none", "C", "H")]


def one(args):
    shape, seed, c = args
    b = modeled(shape, seed)
    return dict(e.job(("modeled", shape, seed, b, np.zeros(len(b), bool), [c]))["configs"][0], series=shape, seed=seed)


if __name__ == "__main__":
    with Pool(4) as p:
        res = []
        for r in p.imap_unordered(one, [(s, seed, c) for s in SHAPES for seed in (1, 2, 3) for c in CONFIGS]):
            print(r["series"], r["seed"], r["form"], r["false_count"], flush=True)
            res.append(r)
    json.dump(res, open("weekly_forms.json", "w"), indent=0)
