"""Five-minute count series from public logs, with a few fields a sender rule could watch.

    python3 real_logs.py ../fetch real/

Writes real/<name>.json: {"start": iso, "counts": [...], "fields": {field: {value: [...]}}}. Buckets with no log
coverage (a gap the trace documents) are null.
"""

import datetime as dt
import glob
import gzip
import json
import os
import re
import sys
from collections import Counter, defaultdict

import numpy as np

S = 288
MONTHS = {m: i for i, m in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1)}


def bucketize(times, fields, start=None, end=None):
    """times: epoch seconds; fields: {name: list of values aligned with times}."""
    t = np.asarray(times, float)
    start = start if start is not None else np.floor(t.min() / 300) * 300
    end = end if end is not None else np.ceil(t.max() / 300) * 300
    n = int((end - start) // 300)
    idx = ((t - start) // 300).astype(int)
    ok = (idx >= 0) & (idx < n)
    counts = np.bincount(idx[ok], minlength=n).astype(float)
    out = {"start": dt.datetime.fromtimestamp(start, dt.timezone.utc).isoformat(), "counts": counts.tolist(), "fields": {}}
    for name, vals in fields.items():
        vals = np.asarray(vals, object)[ok]
        per = {}
        for v in Counter(vals).most_common(300):
            per[str(v[0])] = np.bincount(idx[ok][vals == v[0]], minlength=n).astype(float).tolist()
        out["fields"][name] = per
    return out


def clf(paths, tz_fixed=None):
    """Common log format: epoch seconds from the bracketed stamp, with status and method as fields."""
    pat = re.compile(rb'\[(\d\d)/(\w\w\w)/(\d{4}):(\d\d):(\d\d):(\d\d) ([-+]\d{4})\] "(\S*)[^"]*" (\d{3}|-)')
    times, status, method = [], [], []
    for p in paths:
        op = gzip.open if p.endswith(".gz") else open
        with op(p, "rb") as f:
            for line in f:
                m = pat.search(line)
                if not m:
                    continue
                d, mo, y, hh, mm, ss, tz, meth, st = m.groups()
                tzs = int(tz[1:3]) * 3600 + int(tz[3:5]) * 60
                tzs = -tzs if tz[:1] == b"-" else tzs
                try:
                    t = dt.datetime(int(y), MONTHS[mo.decode()], int(d), int(hh), int(mm), int(ss), tzinfo=dt.timezone.utc).timestamp() - tzs
                except ValueError:
                    continue
                times.append(t)
                status.append(st.decode("latin-1"))
                method.append(meth.decode("latin-1")[:8])
    return times, {"status": status, "method": method}


def syslog(path, year_start, kind_words=2):
    """BSD syslog stamps without a year: the year rolls over when the month goes backwards."""
    pat = re.compile(r"^(\w\w\w)\s+(\d+) (\d\d):(\d\d):(\d\d) \S+ (\S+?)(?:\[\d+\])?: (.*)")
    times, proc, kind = [], [], []
    year, last_mo = year_start, 0
    with open(path, errors="replace") as f:
        for line in f:
            m = pat.match(line)
            if not m:
                continue
            mo, d, hh, mm, ss, pr, msg = m.groups()
            mo = MONTHS[mo]
            if mo < last_mo:
                year += 1
            last_mo = mo
            times.append(dt.datetime(year, mo, int(d), int(hh), int(mm), int(ss), tzinfo=dt.timezone.utc).timestamp())
            proc.append(pr)
            kind.append(" ".join(re.sub(r"\S*\d\S*", "#", msg).split()[:kind_words]))
    return times, {"process": proc, "kind": kind}


def zookeeper(path):
    pat = re.compile(r"^(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d),\d+ - (\w+)\s+\[([^\]:]*)")
    times, level, thread = [], [], []
    with open(path, errors="replace") as f:
        for line in f:
            m = pat.match(line)
            if not m:
                continue
            y, mo, d, hh, mm, ss, lv, th = m.groups()
            times.append(dt.datetime(int(y), int(mo), int(d), int(hh), int(mm), int(ss), tzinfo=dt.timezone.utc).timestamp())
            level.append(lv)
            thread.append(re.sub(r"[\d./:-]+", "#", th)[:24])
    return times, {"level": level, "thread": thread}


def apache_error(path):
    pat = re.compile(r"^\[\w\w\w (\w\w\w) (\d\d) (\d\d):(\d\d):(\d\d) (\d{4})\] \[(\w+)\]")
    times, level = [], []
    with open(path, errors="replace") as f:
        for line in f:
            m = pat.match(line)
            if not m:
                continue
            mo, d, hh, mm, ss, y, lv = m.groups()
            times.append(dt.datetime(int(y), MONTHS[mo], int(d), int(hh), int(mm), int(ss), tzinfo=dt.timezone.utc).timestamp())
            level.append(lv)
    return times, {"level": level}


def gcluster(paths):
    """Google cluster job events: time in microseconds from 600 s before the trace; event type 0-8."""
    times, kind = [], []
    for p in sorted(paths):
        with gzip.open(p, "rt") as f:
            for line in f:
                c = line.rstrip("\n").split(",")
                t = int(c[0])
                if t == 0 or t >= 2**63 - 1:
                    continue  # events before or after the trace window
                times.append(t / 1e6)
                kind.append(c[3])
    return times, {"event": kind}


def bgl(path):
    times, level, comp = [], [], []
    with open(path, errors="replace") as f:
        for line in f:
            c = line.split(maxsplit=8)
            if len(c) < 9 or not c[1].isdigit():
                continue
            times.append(int(c[1]))
            comp.append(c[7])
            level.append(c[8].split(maxsplit=1)[0] if c[8] else "")
    return times, {"component": comp, "level": level}


def load_logs(folder):
    """(name, counts, incident mask) per series. A run of two hours or more without a line, on a series that
    usually has lines in every bucket, is an outage the log documents (NASA's hurricane shutdown) or a gap in the
    collection, and is masked like a labeled incident."""
    out = []
    for path in sorted(glob.glob(os.path.join(folder, "*.json"))):
        d = json.load(open(path))
        b = np.array(d["counts"], float)
        lab = np.zeros(len(b), bool)
        if np.median(b) >= 10:
            z = b == 0
            i = 0
            while i < len(b):
                if z[i]:
                    j = i
                    while j < len(b) and z[j]:
                        j += 1
                    if j - i >= 24:
                        lab[i:j] = True
                    i = j
                else:
                    i += 1
        out.append((d["name"], b, lab))
    return out


def main(fetch, out):
    os.makedirs(out, exist_ok=True)
    jobs = {
        "NASA web, Jul-Aug 1995": lambda: clf([f"{fetch}/ita/NASA_access_log_Jul95.gz", f"{fetch}/ita/NASA_access_log_Aug95.gz"]),
        "ClarkNet web, 2 weeks 1995": lambda: clf(sorted(glob.glob(f"{fetch}/ita/clarknet_access_log_*.gz"))),
        "Calgary web, 1994-95": lambda: clf([f"{fetch}/ita/calgary_access_log.gz"]),
        "Saskatchewan web, Jun-Dec 1995": lambda: clf([f"{fetch}/ita/usask_access_log.gz"]),
        "OpenSSH, 28 days": lambda: syslog(f"{fetch}/loghub/SSH/SSH.log", 2015),
        "Linux syslog, 264 days": lambda: syslog(f"{fetch}/loghub/Linux/Linux.log", 2005),
        "Apache error log, 264 days": lambda: apache_error(f"{fetch}/loghub/Apache/Apache.log"),
        "Zookeeper, 27 days": lambda: zookeeper(f"{fetch}/loghub/Zookeeper/Zookeeper.log"),
        "Google cluster jobs, 29 days": lambda: gcluster(glob.glob(f"{fetch}/gcluster/job_events_*.csv.gz")),
        "BGL supercomputer, 215 days": lambda: bgl(f"{fetch}/loghub/BGL/BGL.log"),
    }
    for name, fn in jobs.items():
        path = os.path.join(out, name.split(",")[0].replace(" ", "_") + ".json")
        if os.path.exists(path):
            continue
        try:
            times, fields = fn()
        except FileNotFoundError as e:
            print("skip", name, e)
            continue
        if not times:
            print("empty", name)
            continue
        d = bucketize(times, fields)
        d["name"] = name
        json.dump(d, open(path, "w"))
        c = np.array(d["counts"])
        print(f"{name}: {len(times)} lines, {len(c) / S:.1f} days, median {np.median(c):.0f} per bucket, fields {[ (k, len(v)) for k, v in d['fields'].items() ]}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
