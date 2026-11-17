"""extend_lab.main for the nab and logs roots, one process per series and config instead of per series, so the longest
series (NYC taxi, the Calgary log) finish a config at a time and a stopped run loses at most one config of each. Writes
the same parts and output as extend_lab.main.

    python3 split_run.py nab|logs <experiment> <out.json>
"""
import json
import os
import sys
from multiprocessing import Pool

import numpy as np

import extend_lab as e
from run_all import MODELED, NAB, S, modeled, nab_series, with_added


def jobs_of(root, experiment, part_dir):
    """(part path, job args without configs) for every series, in extend_lab.main's order."""
    if root == "logs":
        from real_logs import load_logs

        return [(e.part_path(part_dir, name, 0), ("logs", name, 0, b, lab, sh))
                for (name, b, lab), sh in zip(load_logs("real"), e.log_shifts("real"))]
    labels = json.load(open(f"{root}/labels.json"))
    if experiment not in e.FIRST_RULES:
        labels = with_added(labels)
    out = [(e.part_path(part_dir, f"modeled-{name}", seed), ("modeled", name, seed, modeled(name, seed), np.zeros(56 * S, bool), None))
           for name in MODELED for seed in (1, 2, 3)]
    for name in NAB:
        b, lab = nab_series(root, name, labels)
        out.append((e.part_path(part_dir, f"nab-{name}", 0), ("nab", name, 0, b, lab, None)))
    return out


def one(args):
    sub, (dataset, name, seed, b, lab, shift), c = args
    a = (dataset, name, seed, b, lab, [c]) + ((shift,) if shift is not None else ())
    json.dump(e.job(a)["configs"][0], open(sub, "w"))


if __name__ == "__main__":
    root, experiment, out_path = sys.argv[1:4]
    configs = e.EXPERIMENTS[experiment]
    part_dir = out_path + ".parts"
    os.makedirs(part_dir, exist_ok=True)
    series = jobs_of(root, experiment, part_dir)
    sub = lambda part, i: f"{part}.config{i}"
    todo = [(sub(p, i), j, c) for p, j in series if not os.path.exists(p) for i, c in enumerate(configs) if not os.path.exists(sub(p, i))]
    todo.sort(key=lambda t: -len(t[1][3]))
    with Pool(int(os.environ.get("PROCS", 4))) as pool:
        for _ in pool.imap_unordered(one, todo):
            pass
    for p, (dataset, name, seed, *_rest) in series:
        if not os.path.exists(p):
            json.dump(dict(dataset=dataset, series=name, seed=seed, configs=[json.load(open(sub(p, i))) for i in range(len(configs))]), open(p, "w"))
    json.dump([json.load(open(p)) for p, _ in series], open(out_path, "w"), indent=0)
