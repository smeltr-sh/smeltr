"""Two facts quoted in Sections 4.2 and 7 that come straight from the data: the share of bursty 30-minute windows
on EC2 network 5abac7, and the Apache error log's longest completed silences.

    python3 facts.py nab/ real/
"""
import csv, json, statistics, sys


def ec2_bursts(nab):
    rows = csv.DictReader(open(f"{nab}/realAWSCloudwatch/ec2_network_in_5abac7.csv"))
    values = [float(r["value"]) for r in rows]
    windows = [sum(values[i : i + 6]) for i in range(len(values) - 5)]
    median = statistics.median(windows)
    return sum(w > 10 * median for w in windows) / len(windows)


def apache_spells(real):
    counts = json.load(open(f"{real}/Apache_error_log.json"))["counts"]
    spells, run = [], 0
    for c in counts:
        if c == 0:
            run += 1
        else:
            if run:
                spells.append(run)
            run = 0
    spells.sort()
    nonempty = sum(1 for c in counts if c > 0) / len(counts)
    per_day = sum(counts) / (len(counts) / 288)
    return [s * 5 / 60 for s in spells[-12:]], nonempty, per_day


if __name__ == "__main__":
    nab, real = sys.argv[1], sys.argv[2]
    print(f"EC2 network 5abac7: {ec2_bursts(nab):.1%} of 30-minute windows exceed ten times the median window")
    longest, nonempty, per_day = apache_spells(real)
    print(f"Apache error log: lines in {nonempty:.1%} of buckets, {per_day:.0f} lines a day; "
          f"longest completed silences (h): {', '.join(f'{h:.0f}' for h in longest)}")
