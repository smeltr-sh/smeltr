#!/usr/bin/env bash
# Appends a row per reference pipeline to the log BENCH_LOG names: HEAD
# against the pinned reference, interleaved, so host drift between runs
# cancels out of the ratio. Redraws the chart beside it, as an .svg of the
# same name. The log is kept outside the repository.
#
# The host is shared and its speed drifts, so absolute rates are not
# comparable across rows. The ratio is head's median docs/s over the
# reference's in the same run; a move under about 0.1 is noise. The per-CPU
# ratio is the reference's median CPU µs/doc over head's, and docs/CPU-s is
# documents per second of CPU, head's with the reference's in parentheses.
# faults/doc (page faults per document, from perf stat) does not depend on host
# speed.
# CPU µs/doc does, so read head against the reference, in parentheses, in the
# same row.
#
# The reference is built with HEAD's copy of the workload (tests/reference,
# tests/panos and the two tests over them), so both sides run the same
# pipelines, and with its own driver (tests/reference/driver.rs), the part
# that reaches into the engine. A pipeline the reference cannot run logs n/a
# for the reference; HEAD failing one stops the log.
#
# BENCH_HEAD logs another commit in HEAD's place, under the date BENCH_DATE
# names (today unset), as when the log is filled in for days it missed.
#
# threads is BENCH_THREADS (2 unset), the workers that run a pipeline at once, each on a
# thread of its own. Rows at another count measure something else, as rows
# against another reference do.
set -euo pipefail

REF=${BENCH_REF:-d8692751}
RUNS=${BENCH_RUNS:-7}
export BENCH_THREADS=${BENCH_THREADS:-2}
root=$(git rev-parse --show-toplevel)
log=${BENCH_LOG:?BENCH_LOG names the log file, kept outside the repository}
[ -s "$log" ] || echo '| date | pipeline | head | reference | ratio | per-CPU ratio | docs/s | docs/CPU-s | MB/s in | MB/s out | faults/doc | CPU µs/doc | threads |
|---|---|---|---|---|---|---|---|---|---|---|---|---|' >"$log"
grep -q '| per-CPU ratio |' "$log" || { echo "$log has the older columns: add per-CPU ratio and docs/CPU-s" >&2; exit 1; }
grep -q '| threads |' "$log" || { echo "$log has the older columns: add threads, 1 for the rows before it" >&2; exit 1; }
perf=$(ls /usr/lib/linux-tools/*/perf 2>/dev/null | head -1)
[ -x "$perf" ] || { echo "perf missing: apt-get install linux-tools-generic" >&2; exit 1; }

work=$(mktemp -d)
trap 'for w in "$work"/wt-*; do git -C "$root" worktree remove --force "$w" 2>/dev/null || true; done; rm -rf "$work"' EXIT

# HEAD's workload: tests/reference, tests/panos and the two tests over them. Each side runs it.
workload="tests/panos tests/reference tests/panos-ecs-profile.rs tests/reference-pipelines.rs"
workload_id=$(cd "$root" && { git ls-files -s $workload; git diff HEAD -- $workload; } | sha1sum | cut -c1-12)

# Builds `commit` with HEAD's workload and the commit's own driver into `bin`, in the release profile. Each side has
# a target of its own: cargo hashes a path package by name and version, so a shared one runs one side's build script
# for the other. The binary is kept under target/bench-bins by commit and workload, so a run that comes back to
# either builds neither.
# The test binary's path changes with its hash, so cargo is asked for it. A commit from before the rename reads only
# `SMELTR_ALLOW_STALE_GUI`, so both are set.
build() {
    local commit=$1 bin=$2 side=$3 wt=$work/wt-$1 kept=$root/target/bench-bins/$1-$workload_id
    [ -x "$kept" ] && { cp "$kept" "$bin"; return; }
    git -C "$root" worktree add --detach "$wt" "$commit" >/dev/null
    # A commit from before the driver had a file of its own keeps it in mod.rs. The workload writes
    # the lookup files, so the driver reads them from the workload's `bundle_dir`.
    if [ -f "$wt/tests/reference/driver.rs" ]; then
        cp "$wt/tests/reference/driver.rs" "$work/driver.rs"
    else
        { sed '/^pub mod /d; /^pub fn bundle_dir/,/^}/d' "$wt/tests/reference/mod.rs"
          echo 'use super::bundle_dir;'; } >"$work/driver.rs"
    fi
    (cd "$wt" && rm -rf tests/panos tests/reference)
    (cd "$root" && cp -r $workload "$wt/tests/")
    cp "$work/driver.rs" "$wt/tests/reference/driver.rs"
    (cd "$wt" && SMELTR_ALLOW_STALE_FRONTENDS=1 SMELTR_ALLOW_STALE_GUI=1 CARGO_TARGET_DIR=$root/target/bench-$side \
        cargo test --release --test reference-pipelines --no-run --message-format=json) |
        jq -r 'select(.executable != null and .target.name == "reference-pipelines") | .executable' |
        tail -1 | xargs -I{} cp {} "$bin"
    mkdir -p "$root/target/bench-bins" && cp "$bin" "$kept"
}

REF=$(git -C "$root" rev-parse --short=8 "$REF")
HEAD_COMMIT=$(git -C "$root" rev-parse --short=8 "${BENCH_HEAD:-HEAD}")
build "$REF" "$work/ref.bin" ref
build "$HEAD_COMMIT" "$work/head.bin" head

# Each pipeline runs in a process of its own, so `perf stat` counts one pipeline.
names=$("$work/head.bin" --ignored --exact log_run --nocapture | sed -n 's/^NAMES //p')
for name in $names; do
    sides="ref head"
    for i in $(seq "$RUNS"); do
        for side in $sides; do
            BENCH_PIPELINE=$name "$perf" stat -x, -e page-faults,task-clock -o "$work/$name.$side.$i.perf" \
                "$work/$side.bin" --ignored --exact log_run --nocapture --test-threads=1 >"$work/$name.$side.$i.out" 2>&1 &&
                grep -q '^BENCH ' "$work/$name.$side.$i.out" && continue
            tail -5 "$work/$name.$side.$i.out" >&2
            # Only the driver's own load or compile panic means the reference cannot run this pipeline.
            [ "$side" = ref ] && [ "$i" = 1 ] && grep -qE 'bundle loads: |the workload bundle must compile' \
                "$work/$name.$side.$i.out" || { echo "$name on $side printed no result" >&2; exit 1; }
            echo "$name: the reference cannot run it; logging n/a" >&2
            touch "$work/$name.no-ref"
            sides=head
        done
    done
done

python3 - "$work" "$RUNS" "$REF" "$HEAD_COMMIT" "$log" "$BENCH_THREADS" "${BENCH_DATE:-$(date -u +%F)}" $names <<'EOF'
import os, statistics, sys
work, runs, ref, head, log, threads, date, names = (sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4],
                                                    sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8:])

def one(name, side, i):
    line = next(l for l in open(f"{work}/{name}.{side}.{i}.out") if l.startswith("BENCH "))
    r = {k: float(v) for k, v in (kv.split("=") for kv in line.split()[1:])}
    for l in open(f"{work}/{name}.{side}.{i}.perf"):
        f = l.strip().split(",")
        if len(f) > 2 and f[2] in ("page-faults", "task-clock"):
            r[f[2]] = float(f[0])
    return r

with open(log, "a") as out:
    for name in names:
        med = lambda side, f: statistics.median(f(one(name, side, i)) for i in range(1, runs + 1))
        docs_s = lambda r: r["docs"] / r["secs"]
        has_ref = not os.path.exists(f"{work}/{name}.no-ref")
        h = med("head", docs_s)
        ratio = f"{h / med('ref', docs_s):.2f}" if has_ref else "n/a"
        mb_in = med("head", lambda r: r["bytes_in"] / r["secs"] / 1e6)
        mb_out = med("head", lambda r: r["bytes_out"] / r["secs"] / 1e6)
        pf = lambda s: f"{med(s, lambda r: r['page-faults'] / r['docs']):.2f}" if s == "head" or has_ref else "n/a"
        cpu_us = lambda s: med(s, lambda r: r['task-clock'] * 1000 / r['docs'])
        cpu = lambda s: f"{cpu_us(s):.2f}" if s == "head" or has_ref else "n/a"
        per_cpu = lambda s: f"{1e6 / cpu_us(s) / 1000:.1f}k" if s == "head" or has_ref else "n/a"
        cpu_ratio = f"{cpu_us('ref') / cpu_us('head'):.2f}" if has_ref else "n/a"
        row = (f"| {date} | {name} | {head} | {ref} | {ratio} | {cpu_ratio} | {h / 1000:.0f}k "
               f"| {per_cpu('head')} ({per_cpu('ref')}) | {mb_in:.0f} | {mb_out:.0f} | {pf('head')} ({pf('ref')}) "
               f"| {cpu('head')} ({cpu('ref')}) | {threads} |")
        out.write(row + "\n")
        print(row)
EOF

"$root/bench/bench-plot.py" "$log" "${log%.md}.svg"
