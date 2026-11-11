"""The held-out check: pick the margin and wait on one set of series, score on another.

    python3 heldout.py extend_grid.json

The pick is the setting with the fewest false alarms among those detecting the most drops within an hour, by the
K x lambda rule at K x lambda = 30 (a drop a quarter, a miss worth 100 false alarms): minimize F + 30 x M over the
mean of stops, drops to a tenth and drops to 30%.
"""

import json
import sys

import numpy as np

KL = 30


def table(results, series_filter):
    rs = [r for r in results if series_filter(r)]
    cfgs = rs[0]["configs"]
    out = []
    for i, c in enumerate(cfgs):
        F = np.mean([r["configs"][i]["false"] for r in rs if r["configs"][i]["false"] is not None])
        R = np.mean([[r["configs"][i][f"by60_{f}"] for f in ("0.0", "0.1", "0.3")] for r in rs])
        out.append(dict(margin=c["margin"], k=c["k"], false=F, detected=R, cost=F + KL * (1 - R)))
    return out


def pick(tab):
    return min(tab, key=lambda c: c["cost"])


def main(path):
    results = json.load(open(path))
    real = lambda r: r["dataset"] == "nab"
    modeled = lambda r: r["dataset"] == "modeled"
    names = sorted(set(r["series"] for r in results if real(r)))
    half_a = set(names[::2])
    splits = {
        "pick on modeled, score on real": (modeled, real),
        "pick on real half A, score on half B": (lambda r: real(r) and r["series"] in half_a, lambda r: real(r) and r["series"] not in half_a),
        "pick on real half B, score on half A": (lambda r: real(r) and r["series"] not in half_a, lambda r: real(r) and r["series"] in half_a),
        "pick on all real, score on all real (in-sample)": (real, real),
    }
    for name, (pf, sf) in splits.items():
        chosen = pick(table(results, pf))
        scored = [c for c in table(results, sf) if c["margin"] == chosen["margin"] and c["k"] == chosen["k"]][0]
        best = pick(table(results, sf))
        print(f'{name}: picked δ={chosen["margin"]}, k={chosen["k"]} -> scored false {scored["false"]:.2f}, detected {scored["detected"]:.2f}; '
              f'the best on the scored set is δ={best["margin"]}, k={best["k"]} at false {best["false"]:.2f}, detected {best["detected"]:.2f}')
    print("\nall settings, real / modeled:")
    tr, tm = table(results, real), table(results, modeled)
    for a, b in zip(tr, tm):
        print(f'δ={a["margin"]} k={a["k"]}: false {a["false"]:.2f} / {b["false"]:.2f}, detected {a["detected"]:.2f} / {b["detected"]:.2f}, cost {a["cost"]:.1f} / {b["cost"]:.1f}')
    print("\nhalf A:", sorted(half_a))


if __name__ == "__main__":
    main(sys.argv[1])
