"""Fills paper_v7_template.md from the lab's result files, so no number in the paper is typed by hand.

    python3 build_paper.py lab/ paper_v7_template.md paper_v7.md
"""

import csv
import json
import os
import re
import sys

import numpy as np

S = 288
NAMES = {"realAWSCloudwatch/ec2_network_in_257a54": "EC2 network, 257a54", "realAWSCloudwatch/ec2_network_in_5abac7": "EC2 network, 5abac7",
         "realAWSCloudwatch/elb_request_count_8c0756": "ELB requests", "realKnownCause/nyc_taxi": "NYC taxi"}
SHAPES = ["steady", "steady, falling 3%/day", "variable, falling 3%/day", "daily", "variable", "weekday/weekend", "thin daily"]
MIX_NAMES = {"10 sensors, 7 rows/s each": "10 senders, 7 a second each", "100 sensors, 1 row/s each": "100 senders, 1 a second each",
             "30 small senders": "30 small senders, 2 a second", "3 unequal 60/30/10": "3 unequal, 60/30/10% of the rows",
             "10 sensors, 3 regions": "10 senders in 3 regions, peaks 6 to 8 hours apart", "10 equal, noisy senders": "10 noisy senders, σ 0.6",
             "10 sensors, nightly quiet": "10 senders quiet from 1 to 5 a.m.", "100 sensors, 1 row/min each": "100 senders, 1 a minute each"}
FAST, DEFAULT, QUIET, NOGATE, OLDTYP, TWO = range(6)  # the "paper" experiment's configs


def nm(s):
    if s in NAMES:
        return NAMES[s]
    if "Twitter" in s:
        return "Tweets " + s.split("_")[-1]
    return s


def pct(v):
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{round(v * 100):d}%"


def num(v, d=2):
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.{d}f}"


class Lab:
    def __init__(self, root):
        self.root = root
        self.cache = {}

    def load(self, name):
        if name not in self.cache:
            self.cache[name] = json.load(open(os.path.join(self.root, name)))
        return self.cache[name]

    def rows(self, file, dataset):
        return [x for x in self.load(file) if x["dataset"] == dataset]

    def mean(self, rows, i, key):
        v = [x["configs"][i][key] for x in rows if len(x["configs"]) > i and x["configs"][i].get(key) is not None]
        v = [u for u in v if not (isinstance(u, float) and np.isnan(u))]
        return float(np.mean(v)) if v else None

    def judged(self, rows, i):
        return sum(1 for x in rows if len(x["configs"]) > i and x["configs"][i]["months"] > 0)

    def count(self, rows, i):
        return sum(x["configs"][i]["false_count"] for x in rows if len(x["configs"]) > i), sum(x["configs"][i]["months"] for x in rows if len(x["configs"]) > i)

    def baseline(self):
        return {(r["dataset"], r["series"], int(r["seed"])): r for r in csv.DictReader(open(os.path.join(self.root, "baseline_drops.csv")))}

    def medians(self):
        sys.path.insert(0, self.root)
        from run_all import NAB, nab_series

        labels = json.load(open(os.path.join(self.root, "nab/labels.json")))
        out = {}
        for n in NAB:
            b, _ = nab_series(os.path.join(self.root, "nab"), n, labels)
            out[n] = float(np.median(b[~np.isnan(b)]))
        return out


def headline(lab):
    base = lab.baseline()
    real, mod = lab.rows("extend_paper.json", "nab"), lab.rows("extend_paper.json", "modeled")
    bf = lambda ds: np.mean([float(r["false_below"]) for (d, s, sd), r in base.items() if d == ds])
    bc = lambda ds, f: np.mean([float(r[f"caught_{f}"]) for (d, s, sd), r in base.items() if d == ds])
    cell = lambda k: f"{pct(lab.mean(real, DEFAULT, k))} / {pct(lab.mean(mod, DEFAULT, k))}"
    L = ["| Real series / modeled series | Rule replaced | Recommended |", "|---|---|---|",
         f"| Full stops caught within 20 minutes | — | {cell('by20_0.0')} |",
         f"| Full stops caught within the hour | {pct(bc('nab', '0.0'))} / {pct(bc('modeled', '0.0'))} | {cell('by60_0.0')} |",
         f"| Full stops caught within the day | — | {cell('by1440_0.0')} |",
         f"| False alarms per source-month | {bf('nab'):.0f} / {bf('modeled'):.0f} | {num(lab.mean(real, DEFAULT, 'false'))} / {num(lab.mean(mod, DEFAULT, 'false'))} |",
         f"| Drops to a tenth caught within the hour | {pct(bc('nab', '0.1'))} / {pct(bc('modeled', '0.1'))} | {cell('by60_0.1')} |",
         f"| Drops to 30% caught within the hour | {pct(bc('nab', '0.3'))} / {pct(bc('modeled', '0.3'))} | {cell('by60_0.3')} |",
         f"| Drops to 30% caught within the day | — | {cell('by1440_0.3')} |",
         f"| Halvings caught within the hour | {pct(bc('nab', '0.5'))} / {pct(bc('modeled', '0.5'))} | {cell('by60_0.5')} |",
         f"| Halvings caught within the day | — | {cell('by1440_0.5')} |"]
    return "\n".join(L)


def settings(lab, horizons=True):
    real, mod = lab.rows("extend_paper.json", "nab"), lab.rows("extend_paper.json", "modeled")
    L = ["| Setting | k | δ | False a month, real / modeled | Stops in 15 min | Stops in 20 min | Stops in 1 h | Stops in 1 day | To a tenth in 1 h | To 30% in 1 h | To 30% in 1 day | Halving in 1 day |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for name, i in (("Fast", FAST), ("Default", DEFAULT), ("Quiet", QUIET)):
        c = real[0]["configs"][i]
        cell = lambda k: f"{pct(lab.mean(real, i, k))} / {pct(lab.mean(mod, i, k))}"
        L.append(f"| {name} | {c['k']} | {c['margin']} | {num(lab.mean(real, i, 'false'))} / {num(lab.mean(mod, i, 'false'))} | {cell('by15_0.0')} | {cell('by20_0.0')} | {cell('by60_0.0')} | {cell('by1440_0.0')} | {cell('by60_0.1')} | {cell('by60_0.3')} | {cell('by1440_0.3')} | {cell('by1440_0.5')} |")
    return "\n".join(L)


def settings_short(lab):
    real, mod = lab.rows("extend_paper.json", "nab"), lab.rows("extend_paper.json", "modeled")
    L = ["| Setting | k | δ | A stop on a dense source is caught at | False alarms a month, real / modeled |", "|---|---|---|---|---|"]
    for name, i, at in (("Fast", FAST, "15 minutes"), ("Default", DEFAULT, "20 minutes"), ("Quiet", QUIET, "20 minutes")):
        c = real[0]["configs"][i]
        L.append(f"| {name} | {c['k']} | {c['margin']} | {at} | {num(lab.mean(real, i, 'false'))} / {num(lab.mean(mod, i, 'false'))} |")
    return "\n".join(L)


def real_table(lab):
    base, meds = lab.baseline(), lab.medians()
    rows = sorted(lab.rows("extend_paper.json", "nab"), key=lambda x: -meds[x["series"]])
    L = ["| Series | Median | Months judged | False, baseline | False, recommended | Stops in 20 min | Stops in 1 h | Stops in 6 h | Stops in 1 day | To a tenth in 1 h | To 30% in 1 h | To 30% in 1 day | Halving in 1 day |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for x in rows:
        c, b = x["configs"][DEFAULT], base[("nab", x["series"], 0)]
        L.append(f"| {nm(x['series'])} | {meds[x['series']]:,.0f} | {c['months']:.1f} | {float(b['false_below']):.1f} | {num(c['false'], 1)} | {pct(c['by20_0.0'])} | {pct(c['by60_0.0'])} | {pct(c['by360_0.0'])} | {pct(c['by1440_0.0'])} | {pct(c['by60_0.1'])} | {pct(c['by60_0.3'])} | {pct(c['by1440_0.3'])} | {pct(c['by1440_0.5'])} |")
    return "\n".join(L)


def modeled_table(lab):
    base = lab.baseline()
    rs = lab.rows("extend_paper.json", "modeled")
    L = ["| Shape | False, baseline | False, recommended | Stops in 20 min | Stops in 1 h | To a tenth in 1 h | To 30% in 1 h | To 30% in 1 day | Halving in 1 h | Halving in 1 day |", "|---|---|---|---|---|---|---|---|---|---|"]
    for sh in SHAPES:
        xs = [x for x in rs if x["series"] == sh]
        bf = np.mean([float(base[("modeled", sh, s)]["false_below"]) for s in (1, 2, 3)])
        m = lambda k: lab.mean(xs, DEFAULT, k)
        L.append(f"| {sh[0].upper() + sh[1:]} | {bf:.1f} | {num(m('false'))} | {pct(m('by20_0.0'))} | {pct(m('by60_0.0'))} | {pct(m('by60_0.1'))} | {pct(m('by60_0.3'))} | {pct(m('by1440_0.3'))} | {pct(m('by60_0.5'))} | {pct(m('by1440_0.5'))} |")
    return "\n".join(L)


def logs_table(lab):
    sys.path.insert(0, lab.root)
    from real_logs import load_logs

    meds = {n: float(np.median(b)) for n, b, _ in load_logs(os.path.join(lab.root, "real"))}
    rows = sorted(lab.load("extend_paper_logs.json"), key=lambda x: -meds[x["series"]])
    L = ["| Log | Median lines a bucket | Months judged | False, fast / default / quiet | Stops in 20 min | Stops in 1 h | Stops in 6 h | Stops in 1 day | To a tenth in 1 h | To 30% in 1 h | To 30% in 1 day | Halving in 1 day |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for x in rows:
        c = x["configs"]
        d = c[DEFAULT]
        L.append(f"| {x['series']} | {meds[x['series']]:,.0f} | {d['months']:.1f} | {num(c[FAST]['false'])} / {num(d['false'])} / {num(c[QUIET]['false'])} | {pct(d['by20_0.0'])} | {pct(d['by60_0.0'])} | {pct(d['by360_0.0'])} | {pct(d['by1440_0.0'])} | {pct(d['by60_0.1'])} | {pct(d['by60_0.3'])} | {pct(d['by1440_0.3'])} | {pct(d['by1440_0.5'])} |")
    return "\n".join(L)


def either_table(lab):
    """Count widths and gap side by side (either_lab.py): shares over the buckets either rule judges."""
    e = lab.load("either.json")
    meds = lab.medians()
    def agg(rows):
        keys = ["months", "false_band", "false_quiet", "false_either", "by60_band", "by60_quiet", "by60_either", "by1440_band", "by1440_quiet", "by1440_either", "caught_either", "median_either"]
        out = {}
        for k in keys:
            v = [x["result"][k] for x in rows if x["result"].get(k) is not None]
            out[k] = float(np.mean(v)) if v else None
        for k in ("false_band", "false_quiet", "false_either"):
            out[k + "_rate"] = float(np.mean([x["result"][k] / x["result"]["months"] for x in rows]))
        return out
    def line(name, rows):
        a = agg(rows)
        med = "—" if a["median_either"] is None else (f"{a['median_either']:.0f} min" if a["median_either"] < 120 else f"{a['median_either'] / 60:.0f} h" if a["median_either"] < 48 * 60 else f"{a['median_either'] / 1440:.1f} d")
        return (f"| {name} | {a['months']:.1f} | {num(a['false_band_rate'])} / {num(a['false_quiet_rate'])} / {num(a['false_either_rate'])} | "
                f"{pct(a['by60_band'])} / {pct(a['by60_quiet'])} / {pct(a['by60_either'])} | {pct(a['by1440_band'])} / {pct(a['by1440_quiet'])} / {pct(a['by1440_either'])} | {pct(a['caught_either'])} | {med} |")
    L = ["| Source | Months judged by either | False alarms a month, count widths / gap / together | Stops within the hour, count widths / gap / together | Stops within the day, count widths / gap / together | Stops caught at all, together | Median stop, together |",
         "|---|---|---|---|---|---|---|"]
    nab = [x for x in e if x["dataset"] == "nab"]
    dense = [x for x in nab if x["result"]["months_quiet"] == 0]
    L.append(line(f"{len(dense)} count series the gap never judges", dense))
    for x in sorted([x for x in nab if x["result"]["months_quiet"] > 0], key=lambda x: -meds.get(x["series"], 0)):
        L.append(line(f"{nm(x['series'])} ({meds[x['series']]:.0f} a bucket)", [x]))
    for name in SHAPES:
        L.append(line(f"Modeled: {name}", [x for x in e if x["dataset"] == "modeled" and x["series"] == name]))
    for name in ("12 rows an hour", "4 rows an hour", "1 row an hour", "1 row a day"):
        L.append(line(f"Sparse: {name}", [x for x in e if x["dataset"] == "sparse" and x["series"] == name]))
    sys.path.insert(0, lab.root)
    from real_logs import load_logs
    lmeds = {n: float(np.median(b)) for n, b, _ in load_logs(os.path.join(lab.root, "real"))}
    for x in sorted([x for x in e if x["dataset"] == "logs"], key=lambda x: -lmeds[x["series"]]):
        L.append(line(x["series"], [x]))
    return "\n".join(L)


def sparse_table(lab):
    r = lab.load("sparse_final.json")
    L = ["| Source | Two short widths: false a month | stops in 1 h | stops in 1 day | Four widths, 7-day history: false a month | stops in 6 h | stops in 1 day | Four widths, 28 days behind the long ones: false a month | stops in 6 h | stops in 1 day | median stop |",
         "|---|---|---|---|---|---|---|---|---|---|---|"]
    for name in ("12 rows an hour", "4 rows an hour", "1 row an hour", "1 row a day"):
        xs = [x for x in r if x["series"] == name]
        a, b, c = (lambda k, i=i: lab.mean(xs, i, k) for i in (0, 2, 1))
        med = c("median_0.0")
        def trio(m, h):
            return "not judged | | " if m("false") is None else f"{num(m('false'), 1)} | {pct(m(f'by{h}_0.0'))} | {pct(m('by1440_0.0'))} "
        L.append(f"| {name} | {trio(a, 60)}| {trio(b, 360)}| {trio(c, 360)}| {'—' if med is None else f'{med / 60:.0f} h'} |")
    return "\n".join(L)


def senders_table(lab):
    r, rq = lab.load("extend_paper_values.json"), lab.load("extend_paper_values_quiet.json")
    L = ["| Senders behind one source | Sender-months | False per sender-month, default / quiet | Stops in 20 min | Stops in 1 h | Stops in 6 h | To a tenth in 1 h | To 30% in 1 h | To 30% in 1 day | Halving in 1 day |", "|---|---|---|---|---|---|---|---|---|---|"]
    for mix in MIX_NAMES:
        xs, xq = [x for x in r if x["mix"] == mix], [x for x in rq if x["mix"] == mix]
        if not xs:
            continue
        m = lambda k: lab.mean(xs, 0, k)
        months = sum(x["configs"][0]["months"] for x in xs)
        L.append(f"| {MIX_NAMES[mix]} | {months:.1f} | {num(m('false'))} / {num(lab.mean(xq, 0, 'false'))} | {pct(m('by20_0.0'))} | {pct(m('by60_0.0'))} | {pct(m('by360_0.0'))} | {pct(m('by60_0.1'))} | {pct(m('by60_0.3'))} | {pct(m('by1440_0.3'))} | {pct(m('by1440_0.5'))} |")
    return "\n".join(L)


def fields_table(lab):
    v = lab.load("values_logs.json")
    L = ["| Log | Field | Values | Values seen every hour of the learning week | Field watched |", "|---|---|---|---|---|"]
    for f in sorted(v["fields"], key=lambda f: (f["series"], f["field"])):
        L.append(f"| {f['series']} | {f['field']} | {f['values']} | {f['values_watched']} | {'yes' if f['field_watched'] else 'no'} |")
    return "\n".join(L)


def values_table(lab):
    v = lab.load("values_logs.json")["values"]
    L = ["| Log | Field | Value | Rows a bucket | Months | False a month | Stops in 20 min | Stops in 1 h | Stops in 1 day | To a tenth in 1 h | To 30% in 1 day |", "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in sorted(v, key=lambda r: (r["series"], r["field"], -r["rows_per_bucket"])):
        L.append(f"| {r['series']} | {r['field']} | `{r['value']}` | {r['rows_per_bucket']:,.0f} | {r['months']:.1f} | {num(r['false'])} | {pct(r['by20_0.0'])} | {pct(r['by60_0.0'])} | {pct(r['by1440_0.0'])} | {pct(r['by60_0.1'])} | {pct(r['by1440_0.3'])} |")
    return "\n".join(L)


def widths_table(lab):
    real, mod = lab.rows("extend_widths.json", "nab"), lab.rows("extend_widths.json", "modeled")
    labels = ["5 minutes alone", "30 minutes alone", "6 hours alone", "1 day alone", "5 and 30 minutes", "All four"]
    L = ["| Widths | Series judged, real / modeled | False a month, real / modeled | Stops in 20 min | Stops in 1 h | Stops in 1 day | To a tenth in 1 h | To 30% in 1 h | To 30% in 1 day | Halving in 1 day |", "|---|---|---|---|---|---|---|---|---|---|"]
    for i, name in enumerate(labels):
        cell = lambda k: f"{pct(lab.mean(real, i, k))} / {pct(lab.mean(mod, i, k))}"
        L.append(f"| {name} | {lab.judged(real, i)} of {len(real)} / {lab.judged(mod, i)} of {len(mod)} | {num(lab.mean(real, i, 'false'))} / {num(lab.mean(mod, i, 'false'))} | {cell('by20_0.0')} | {cell('by60_0.0')} | {cell('by1440_0.0')} | {cell('by60_0.1')} | {cell('by60_0.3')} | {cell('by1440_0.3')} | {cell('by1440_0.5')} |")
    return "\n".join(L)


def grid_table(lab):
    real, mod = lab.rows("extend_grid2.json", "nab"), lab.rows("extend_grid2.json", "modeled")
    L = ["| δ | k | False a month, real / modeled | Stops in 1 h | To a tenth in 1 h | To 30% in 1 h | To 30% in 1 day | Halving in 1 day | Cost at K·λ = 30, real / modeled |", "|---|---|---|---|---|---|---|---|---|"]
    for i, c in enumerate(real[0]["configs"]):
        cell = lambda k: f"{pct(lab.mean(real, i, k))} / {pct(lab.mean(mod, i, k))}"
        cost = []
        for rows in (real, mod):
            F = lab.mean(rows, i, "false")
            R = np.mean([lab.mean(rows, i, f"by60_{f}") for f in ("0.0", "0.1", "0.3")])
            cost.append(F + 30 * (1 - R))
        L.append(f"| {c['margin']} | {c['k']} | {num(lab.mean(real, i, 'false'))} / {num(lab.mean(mod, i, 'false'))} | {cell('by60_0.0')} | {cell('by60_0.1')} | {cell('by60_0.3')} | {cell('by1440_0.3')} | {cell('by1440_0.5')} | {cost[0]:.1f} / {cost[1]:.1f} |")
    return "\n".join(L)


def gate_table(lab):
    real, mod = lab.rows("extend_gate.json", "nab"), lab.rows("extend_gate.json", "modeled")
    L = ["| Gate, rows | False a month, real / modeled | Alarms on the real series | Real months judged | Stops in 20 min | Stops in 1 h | Stops in 6 h | Stops in 1 day | To a tenth in 1 h | To 30% in 1 day |", "|---|---|---|---|---|---|---|---|---|---|"]
    for i, c in enumerate(real[0]["configs"]):
        cell = lambda k: f"{pct(lab.mean(real, i, k))} / {pct(lab.mean(mod, i, k))}"
        fa, mo = lab.count(real, i)
        L.append(f"| {c['minrows']:.0f} | {num(lab.mean(real, i, 'false'))} / {num(lab.mean(mod, i, 'false'))} | {fa} | {mo:.1f} | {cell('by20_0.0')} | {cell('by60_0.0')} | {cell('by360_0.0')} | {cell('by1440_0.0')} | {cell('by60_0.1')} | {cell('by1440_0.3')} |")
    return "\n".join(L)


def quiet_table(lab):
    """The gap at margins 1, 1.5 and 2 on every series it judges, grouped."""
    q = lab.load("quiet.json")
    tweets = sorted(nm(x["series"]) for x in q if x["dataset"] == "nab" and x["configs"][0]["months"] > 0)
    groups = [(f"Tweet series ({', '.join(t.split()[-1] for t in tweets)})", lambda x: x["dataset"] == "nab"),
              ("Thin daily (modeled)", lambda x: x["series"] == "thin daily"), ("Weekday/weekend (modeled)", lambda x: x["series"] == "weekday/weekend"),
              ("12 rows an hour", lambda x: x["series"] == "12 rows an hour"), ("4 rows an hour", lambda x: x["series"] == "4 rows an hour"),
              ("1 row an hour", lambda x: x["series"] == "1 row an hour"), ("1 row a day", lambda x: x["series"] == "1 row a day")]
    groups += [(x["series"], (lambda x, n=x["series"]: x["series"] == n)) for x in sorted(q, key=lambda x: x["series"]) if x["dataset"] == "logs"]
    L = ["| Source | Spells judged on | Longest normal spell, with the first year's allowance | Margin 1: false a month | Margin 1.5: false a month | stop caught within | median | Margin 2: false a month | median |", "|---|---|---|---|---|---|---|---|---|"]
    for name, pred in groups:
        xs = [x for x in q if pred(x) and x["configs"][0]["months"] > 0]
        if not xs:
            continue
        m = lambda i, k: lab.mean(xs, i, k)
        lh = m(1, "longest_h")
        med = lambda i: ("—" if m(i, "median_0.0") is None else f"{m(i, 'median_0.0') / 1440:.0f} days" if m(i, "median_0.0") >= 48 * 60 else (f"{m(i, 'median_0.0') / 60:.0f} h" if m(i, "median_0.0") >= 90 else f"{m(i, 'median_0.0'):.0f} min"))
        within = f"{pct(m(1, 'by360_0.0'))} in 6 h, {pct(m(1, 'by1440_0.0'))} in 1 day"
        L.append(f"| {name} | {m(1, 'spells'):,.0f} | {lh:.1f} h | {num(m(0, 'false'))} | {num(m(1, 'false'))} | {within} | {med(1)} | {num(m(2, 'false'))} | {med(2)} |")
    return "\n".join(L)


def rare_table(lab):
    """The gap with 28 days of spells and a ten-spell gate against a year and the allowance (record_lab.py)."""
    r = lab.load("record.json")
    L = ["| Source | Years | Time judged, 28 days / a year | False alarms a year, 28 days / a year | Median stop, 28 days / a year |", "|---|---|---|---|---|"]
    dur = lambda h: "—" if h is None else (f"{h * 60:.0f} min" if h < 2 else f"{h:.0f} h" if h < 48 else f"{h / 24:.0f} days")
    names = []
    for x in r:
        if x["dataset"] in ("rare", "steady") and (x["dataset"], x["series"]) not in names:
            names.append((x["dataset"], x["series"]))
    for ds, name in names:
        xs = [x["result"] for x in r if x["dataset"] == ds and x["series"] == name]
        cells = []
        for k in ("judged", "false", "median"):
            v = []
            for rule in ("today", "year"):
                parts = [(x[rule]["first"], x[rule]["after"]) for x in xs]
                yrs = sum(a["years"] + b["years"] for a, b in parts)
                if k == "judged":
                    v.append(pct(sum((a["judged"] or 0) * a["years"] + (b["judged"] or 0) * b["years"] for a, b in parts) / yrs))
                elif k == "false":
                    v.append(num(sum(a["false"] + b["false"] for a, b in parts) / yrs))
                else:
                    m = [c["median_h"] for a, b in parts for c in (a, b) if c["median_h"] is not None]
                    v.append(dur(float(np.median(m)) if m else None))
            cells.append(" / ".join(v))
        yrs = sum(x["today"]["first"]["years"] + x["today"]["after"]["years"] for x in xs)
        L.append(f"| {name[0].upper() + name[1:]} | {yrs:.0f} | {cells[0]} | {cells[1]} | {cells[2]} |")
    return "\n".join(L)


def parts_table(lab):
    real, mod = lab.rows("extend_paper.json", "nab"), lab.rows("extend_paper.json", "modeled")
    L = ["| Detector | False a month, real / modeled | Alarms on the real series | Stops in 20 min | Stops in 1 h | Stops in 1 day | To 30% in 6 h | To 30% in 1 day | Halving in 6 h | Halving in 1 day |", "|---|---|---|---|---|---|---|---|---|---|"]
    try:
        h7r, h7m = lab.rows("extend_hist7.json", "nab"), lab.rows("extend_hist7.json", "modeled")
        cell = lambda k: f"{pct(lab.mean(h7r, 0, k))} / {pct(lab.mean(h7m, 0, k))}"
        fa, _ = lab.count(h7r, 0)
        L.append(f"| Four widths, 7 days behind every width, 10-row gate | {num(lab.mean(h7r, 0, 'false'))} / {num(lab.mean(h7m, 0, 'false'))} | {fa} | {cell('by20_0.0')} | {cell('by60_0.0')} | {cell('by1440_0.0')} | {cell('by360_0.3')} | {cell('by1440_0.3')} | {cell('by360_0.5')} | {cell('by1440_0.5')} |")
    except FileNotFoundError:
        pass
    for name, i in (("5 and 30 minutes, 7-day history, no gate", TWO), ("Four widths, 28 days behind the long ones, no gate", NOGATE),
                    ("Four widths, 28 days behind the long ones, 10-row gate, typical level summed from buckets", OLDTYP),
                    ("Four widths, 28 days behind the long ones, 10-row gate, typical window (recommended)", DEFAULT)):
        cell = lambda k: f"{pct(lab.mean(real, i, k))} / {pct(lab.mean(mod, i, k))}"
        fa, _ = lab.count(real, i)
        L.append(f"| {name} | {num(lab.mean(real, i, 'false'))} / {num(lab.mean(mod, i, 'false'))} | {fa} | {cell('by20_0.0')} | {cell('by60_0.0')} | {cell('by1440_0.0')} | {cell('by360_0.3')} | {cell('by1440_0.3')} | {cell('by360_0.5')} | {cell('by1440_0.5')} |")
    return "\n".join(L)


def facts(lab):
    """Numbers the prose quotes, by name."""
    f = {}
    base = lab.baseline()
    real, mod = lab.rows("extend_paper.json", "nab"), lab.rows("extend_paper.json", "modeled")
    for ds, rows in (("real", real), ("mod", mod)):
        for i, tag in ((FAST, "fast"), (DEFAULT, "def"), (QUIET, "quiet"), (NOGATE, "nogate"), (TWO, "two")):
            f[f"false_{tag}_{ds}"] = num(lab.mean(rows, i, "false"))
            fc, mo = lab.count(rows, i)
            f[f"alarms_{tag}_{ds}"], f[f"months_{tag}_{ds}"] = str(fc), f"{mo:.1f}"
            for key in ("by15_0.0", "by20_0.0", "by60_0.0", "by360_0.0", "by1440_0.0", "by60_0.1", "by1440_0.1", "by60_0.3", "by360_0.3", "by1440_0.3", "by60_0.5", "by360_0.5", "by1440_0.5"):
                f[f"{key.replace('.', '')}_{tag}_{ds}"] = pct(lab.mean(rows, i, key))
    f["base_false_real"] = f"{np.mean([float(r['false_below']) for (d, s, sd), r in base.items() if d == 'nab']):.0f}"
    f["base_false_mod"] = f"{np.mean([float(r['false_below']) for (d, s, sd), r in base.items() if d == 'modeled']):.0f}"
    for fr, tag in (("0.0", "stop"), ("0.1", "tenth"), ("0.3", "p30"), ("0.5", "half")):
        for ds, d in (("real", "nab"), ("mod", "modeled")):
            f[f"base_{tag}_{ds}"] = pct(np.mean([float(r[f"caught_{fr}"]) for (dd, s, sd), r in base.items() if dd == d]))
    # ratios between settings
    ff, fd, fq = (lab.mean(real, i, "false") for i in (FAST, DEFAULT, QUIET))
    f["fast_ratio"] = f"{ff / fd:.0f}" if fd else "—"
    f["quiet_cost"] = f"{round(100 * (lab.mean(real, DEFAULT, 'by60_0.3') - lab.mean(real, QUIET, 'by60_0.3'))):d}"
    # stops within the day: which real series fall short
    short = [(nm(x["series"]), x["configs"][DEFAULT]["by1440_0.0"], x["configs"][DEFAULT]["months"]) for x in real if x["configs"][DEFAULT]["by1440_0.0"] is not None and x["configs"][DEFAULT]["by1440_0.0"] < 0.99]
    try:
        rec = lab.load("record.json")
        for key, name in (("rare", "every 14 days at random, bursts"), ("regular", "every 10 to 18 days, bursts"), ("day", "1 row a day"), ("hour", "1 row an hour")):
            xs = [x["result"] for x in rec if x["series"] == name and x["dataset"] in ("rare", "steady")]
            for rule, tag in (("today", "28"), ("year", "year")):
                parts = [c for x in xs for c in (x[rule]["first"], x[rule]["after"])]
                f[f"{key}_false_{tag}"] = num(sum(c["false"] for c in parts) / sum(c["years"] for c in parts), 1)
                m = [c["median_h"] for c in parts if c["median_h"] is not None]
                f[f"{key}_stop_{tag}"] = (f"{np.median(m) / 24:.0f} days" if np.median(m) >= 48 else f"{np.median(m):.0f} hours") if m else "—"
            f[f"{key}_years"] = f"{sum(x['today']['first']['years'] + x['today']['after']['years'] for x in xs):.0f}"
    except FileNotFoundError:
        pass
    try:
        gh = lab.load("gap_history.json")
        xs = [x["result"] for x in gh if x["series"] == "once in 15 days, 3 in 10 a burst"]
        for mode, tag in (("28d", "28"), ("last30", "last30"), ("own30", "own30")):
            f[f"gh_{tag}"] = str(sum(x[mode]["false"] for x in xs))
        f["gh_years"] = f"{sum(x['28d']['years'] for x in xs):.0f}"
        xs = [x["result"]["record"]["after"] for x in lab.load("record.json") if x["series"] == "every 14 days at random, bursts"]
        f["c1_false"] = num(sum(c["false"] for c in xs) / sum(c["years"] for c in xs), 1)
    except FileNotFoundError:
        pass
    f["stops_day_note"] = ("Every stop on every real series is caught within the day except on " + ", ".join(f"{n} ({pct(v)}, judged {m:.1f} months)" for n, v, m in short) + ".") if short else "Every stop on every real series is caught within the day."
    # count widths or gap together
    try:
        e = lab.load("either.json")
        res = lambda ds: [x["result"] for x in e if x["dataset"] == ds]
        for ds, tag in (("nab", "real"), ("modeled", "mod"), ("logs", "logs"), ("sparse", "sparse")):
            r = res(ds)
            f[f"either_day_{tag}"] = pct(float(np.mean([x["by1440_either"] for x in r])))
            f[f"either_hour_{tag}"] = pct(float(np.mean([x["by60_either"] for x in r])))
            f[f"either_caught_{tag}"] = pct(float(np.mean([x["caught_either"] for x in r])))
            f[f"either_false_{tag}"] = num(float(np.mean([x["false_either"] / x["months"] for x in r])))
            f[f"band_false_{tag}"] = num(float(np.mean([x["false_band"] / x["months"] for x in r])))
        f["either_sources"] = str(len({(x["dataset"], x["series"]) for x in e}))
        f["either_not_day"] = ", ".join(f"{x['series'] if x['dataset'] != 'nab' else nm(x['series'])} ({pct(x['result']['by1440_either'])})" for x in e if x["result"]["by1440_either"] < 0.995 and x["seed"] in (0, 1))
    except FileNotFoundError:
        pass
    # logs
    try:
        logs = lab.load("extend_paper_logs.json")
        judged_logs = [x for x in logs if x["configs"][DEFAULT]["months"] >= 0.1]
        f["logs_judged"] = str(len(judged_logs))
        f["logs_false_def"] = num(float(np.mean([x["configs"][DEFAULT]["false"] for x in judged_logs])))
        for tag, start in (("sask", "Saskatchewan"), ("calgary", "Calgary"), ("linux", "Linux"), ("bgl", "BGL"), ("apache", "Apache"), ("google", "Google"), ("clarknet", "ClarkNet"), ("nasa", "NASA"), ("openssh", "OpenSSH")):
            x = [x for x in logs if x["series"].startswith(start)][0]["configs"][DEFAULT]
            f[f"q_{tag}_band"], f[f"q_{tag}_alarms"] = num(x["false"]), str(x["false_count"])
        for i, tag in ((FAST, "fast"), (DEFAULT, "def"), (QUIET, "quiet"), (NOGATE, "nogate"), (OLDTYP, "oldtyp"), (TWO, "two")):
            fc = sum(x["configs"][i]["false_count"] for x in logs); mo = sum(x["configs"][i]["months"] for x in logs)
            f[f"logs_alarms_{tag}"], f[f"logs_months_{tag}"] = str(fc), f"{mo:.1f}"
            f[f"logs_false_{tag}"] = num(float(np.mean([x["configs"][i]["false"] for x in logs if x["configs"][i]["months"] >= 0.1])))
            for key in ("by20_0.0", "by60_0.0", "by360_0.0", "by1440_0.0", "by60_0.1", "by60_0.3", "by1440_0.3", "by1440_0.5"):
                v = [x["configs"][i][key] for x in logs if x["configs"][i].get(key) is not None and x["configs"][i]["months"] > 0]
                f[f"logs_{key.replace('.', '')}_{tag}"] = pct(float(np.mean(v))) if v else "—"
    except FileNotFoundError:
        pass
    # sparse
    try:
        sp = lab.load("sparse_final.json")
        for name, tag in (("12 rows an hour", "12"), ("4 rows an hour", "4"), ("1 row an hour", "1")):
            xs = [x for x in sp if x["series"] == name]
            med = lab.mean(xs, 1, "median_0.0")
            f[f"sparse_median_{tag}"] = "—" if med is None else (f"{med / 60:.0f} hours" if med >= 90 else f"{med:.0f} minutes")
            f[f"sparse_false7_{tag}"] = num(lab.mean(xs, 2, "false"), 1)
            f[f"sparse_false28_{tag}"] = num(lab.mean(xs, 1, "false"), 1)
        # the week-wide width comes from the earlier sparse_history run (28 days, widths to a week), the one-row-a-day source
        sh = lab.load("sparse_history.json")
        xs = [x for x in sh if x["series"] == "1 row a day"]
        f["sparse_week_false"] = str(sum(x["configs"][2]["false_count"] for x in xs))
        f["sparse_week_months"] = f'{np.mean([x["configs"][2]["months"] for x in xs]):.1f}'
        f["sparse_months_day"] = f'{np.mean([x["configs"][2]["months"] for x in sh if x["series"] == "1 row an hour"]):.1f}'
        med = lab.mean(xs, 2, "median_0.0")
        f["sparse_week_median"] = f"{med / 1440:.0f} days"
    except FileNotFoundError:
        pass
    # fields and values on the logs
    try:
        v = lab.load("values_logs.json")
        f["fields_total"] = str(len(v["fields"]))
        f["fields_watched"] = str(sum(1 for x in v["fields"] if x["field_watched"]))
        f["values_watched"] = str(len(v["values"]))
        vals = [r for r in v["values"] if r["months"] > 0]
        f["values_false"] = num(float(np.mean([r["false"] for r in vals])))
        f["values_alarms"], f["values_months"] = str(sum(r["false_count"] for r in vals)), f"{sum(r['months'] for r in vals):.1f}"
        ssh = [r["false"] for r in vals if r["series"].startswith("OpenSSH")]
        f["values_openssh_lo"], f["values_openssh_hi"] = num(min(ssh)), num(max(ssh))
    except FileNotFoundError:
        pass
    try:
        for tag, file in (("def", "extend_paper_values.json"), ("quiet", "extend_paper_values_quiet.json")):
            r = lab.load(file)
            steady = [x for x in r if x["mix"] in ("10 sensors, 7 rows/s each", "100 sensors, 1 row/s each", "30 small senders", "3 unequal 60/30/10", "10 sensors, 3 regions")]
            f[f"senders_false_{tag}"] = num(lab.mean(steady, 0, "false"))
            f[f"senders_false_{tag}_lo"], f[f"senders_false_{tag}_hi"] = num(min(x["configs"][0]["false"] for x in steady)), num(max(x["configs"][0]["false"] for x in steady))
            f[f"senders_alarms_{tag}"] = str(sum(x["configs"][0]["false_count"] for x in r)); f[f"senders_months_{tag}"] = f"{sum(x['configs'][0]['months'] for x in r):.0f}"
    except FileNotFoundError:
        pass
    # precision model at a drop to a tenth once a month: acted = R * lambda R / (lambda R + F)
    R, F = lab.mean(real, DEFAULT, "by60_0.1"), lab.mean(real, DEFAULT, "false")
    prec = R / (R + F)
    f["precision_def"], f["acted_def"] = pct(prec), pct(R * prec)
    # held-out pick on the design's grid
    try:
        import subprocess
        txt = subprocess.run([sys.executable, os.path.join(lab.root, "heldout.py"), os.path.join(lab.root, "extend_grid2.json")], capture_output=True, text=True).stdout
        lines = [l for l in txt.splitlines() if l.startswith("pick on")]
        f["heldout_text"] = "The real series were split into two halves of seven by name, and the setting picked on either half scored on the other: " + " ".join(l.replace("pick on ", "Picking on ").replace(" -> scored", ", the pick scores").replace("; the best on the scored set is", "; the best setting there is") + "." for l in lines[1:3]) + " " + lines[0].replace("pick on modeled, score on real", "Picking on the modeled series alone and scoring on the real ones").replace(" -> scored", ", the pick scores").replace("; the best on the scored set is", "; the best setting there is") + "."
    except Exception as e:
        f["heldout_text"] = f"[[heldout failed: {e}]]"
    alarms = [(nm(x["series"]), x["configs"][DEFAULT]["false_count"]) for x in real if x["configs"][DEFAULT]["false_count"]]
    f["real_alarm_series"] = ", ".join(f"{n} {c}" for n, c in sorted(alarms, key=lambda t: -t[1])) or "none"
    f["mod_alarm_series"] = ", ".join(f"{x['series']} seed {x['seed']}: {x['configs'][DEFAULT]['false_count']}" for x in mod if x["configs"][DEFAULT]["false_count"]) or "none"
    try:
        wm = lab.rows("extend_widths.json", "modeled")
        f["widths_long_add_mod"] = num(lab.mean(wm, 5, "false") - lab.mean(wm, 1, "false"))
        gr = lab.rows("extend_grid2.json", "nab")
        costs = [lab.mean(gr, i, "false") + 30 * (1 - np.mean([lab.mean(gr, i, k) for k in ("by60_0.0", "by60_0.1", "by60_0.3")])) for i in range(len(gr[0]["configs"]))]
        f["m_cost_lo"], f["m_cost_hi"] = f"{min(costs):.1f}", f"{max(costs):.1f}"
    except FileNotFoundError:
        pass
    return f


def generic(lab, text):
    """{{m:file:dataset:i:key}} mean, {{p:...}} as a percent, {{c:...}} alarms pooled, {{mo:...}} months pooled,
    {{r:...}} pooled rate a month, {{j:file:dataset:i}} series judged, {{b:dataset:series:col}} baseline column,
    {{q:series:i:key}} one series of quiet.json (i the margin index), {{qh:series:i}} its median stop as hours."""
    def one(m):
        kind, *a = m.group(1).split(":")
        if kind == "b":
            ds, series, col = a
            return pct(float(lab.baseline()[(ds, series, 0)][col])) if col.startswith("caught") else num(float(lab.baseline()[(ds, series, 0)][col]))
        if kind in ("q", "qh"):
            series, i = a[0], int(a[1])
            xs = [x for x in lab.load("quiet.json") if x["series"].split("/")[-1] == series or nm(x["series"]) == series]
            if kind == "qh":
                v = lab.mean(xs, i, "median_0.0")
                return "—" if v is None else (f"{v / 1440:.0f} days" if v >= 48 * 60 else f"{v / 60:.0f} hours" if v >= 90 else f"{v:.0f} minutes")
            key = a[2]
            v = lab.mean(xs, i, key)
            return pct(v) if key.startswith("by") else (f"{v:.0f}" if key in ("false_count", "spells") else num(v, 1) if key == "longest_h" else num(v))
        if kind == "e":
            ds, series, key = a[0], a[1], a[2]
            xs = [x["result"] for x in lab.load("either.json") if x["dataset"] == ds and x["series"] == series]
            v = float(np.mean([x[key] for x in xs]))
            if key.startswith("median"):
                return f"{v / 1440:.0f} days" if v >= 48 * 60 else f"{v / 60:.0f} hours" if v >= 90 else f"{v:.0f} minutes"
            return pct(v) if key.startswith("by") or key.startswith("caught") else num(v)
        file, ds, i = a[0], a[1], int(a[2])
        rows = lab.rows(file, ds) if ds != "all" else lab.load(file)
        if kind == "j":
            return str(lab.judged(rows, i))
        key = a[3]
        if kind == "m":
            return num(lab.mean(rows, i, key))
        if kind == "p":
            return pct(lab.mean(rows, i, key))
        c, mo = lab.count(rows, i)
        return {"c": str(c), "mo": f"{mo:.1f}", "r": num(c / mo if mo else None)}[kind]
    return re.sub(r"\{\{((?:m|p|c|mo|r|j|b|q|qh|e):[^}]*)\}\}", one, text)


def main(root, template, out):
    lab = Lab(root)
    tables = {"HEADLINE": headline, "SETTINGS": settings, "SETTINGS_SHORT": settings_short, "REAL": real_table, "MODELED": modeled_table, "LOGS": logs_table, "EITHER": either_table,
              "SPARSE": sparse_table, "SENDERS": senders_table, "FIELDS": fields_table, "VALUES": values_table, "WIDTHS": widths_table, "GRID": grid_table, "PARTS": parts_table, "GATE": gate_table, "QUIET": quiet_table, "RARE": rare_table}
    text = open(template).read()
    missing = []
    for key, fn in tables.items():
        if f"{{{{{key}}}}}" in text:
            try:
                text = text.replace(f"{{{{{key}}}}}", fn(lab))
            except FileNotFoundError as e:
                missing.append(f"{key}: {e.filename}")
    try:
        text = generic(lab, text)
        f = facts(lab)
        text = re.sub(r"\{\{([a-z0-9_]+)\}\}", lambda m: f.get(m.group(1), m.group(0)), text)
    except FileNotFoundError as e:
        missing.append(f"facts: {e.filename}")
    open(out, "w").write(text)
    left = sorted(set(re.findall(r"\{\{[A-Za-z0-9_:/. ,-]+\}\}", text)))
    print("missing files:", missing or "none")
    print("unfilled:", left or "none")


if __name__ == "__main__":
    main(*sys.argv[1:4])
