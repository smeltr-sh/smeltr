#!/usr/bin/env bash
# Appends a row per reference pipeline to the log BENCH_LOG names: HEAD
# against the pinned reference, interleaved, so host drift between runs
# cancels out of the ratio. Redraws the chart beside it, as an .svg of the
# same name. The log is kept outside the repository.
#
# The host is shared and its speed drifts, so absolute rates are not
# comparable across rows. The ratio is head's median docs/s over the
# reference's in the same run; a move under about 0.1 is noise. faults/doc
# (page faults per document, from perf stat) does not depend on host speed.
# CPU µs/doc does, so read head against the reference, in parentheses, in the
# same row.
#
# The reference is built with HEAD's copy of the workload (tests/reference,
# tests/panos and the two tests over them), so both sides run the same
# pipelines, and with its own driver (tests/reference/driver.rs), the part
# that reaches into the engine. A pipeline the reference cannot run logs n/a
# for the reference; HEAD failing one stops the log.
set -euo pipefail

REF=${BENCH_REF:-130b7412}
RUNS=${BENCH_RUNS:-7}
root=$(git rev-parse --show-toplevel)
log=${BENCH_LOG:?BENCH_LOG names the log file, kept outside the repository}
[ -s "$log" ] || echo '| date | pipeline | head | reference | ratio | docs/s | MB/s in | MB/s out | faults/doc | CPU µs/doc |
|---|---|---|---|---|---|---|---|---|---|' >"$log"
perf=$(ls /usr/lib/linux-tools/*/perf 2>/dev/null | head -1)
[ -x "$perf" ] || { echo "perf missing: apt-get install linux-tools-generic" >&2; exit 1; }

work=$(mktemp -d)
trap 'git -C "$root" worktree remove --force "$work/ref" 2>/dev/null || true; rm -rf "$work"' EXIT

# The test binary's path changes with its hash, so ask cargo for it and copy it aside. Both sides build as
# `[profile.dist]` does, what ships; as overrides, since the reference predates that profile. Each side has a
# target of its own: cargo hashes a path package by name and version, so a shared one runs one side's build script
# for the other, and a target an older run shared can still hold the other side's output.
build() {
    (cd "$1" && SMELTR_ALLOW_STALE_GUI=1 CARGO_TARGET_DIR=$3 \
        CARGO_PROFILE_RELEASE_LTO=fat CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
        cargo test --release --test reference-pipelines --no-run --message-format=json) |
        jq -r 'select(.executable != null and .target.name == "reference-pipelines") | .executable' |
        tail -1 | xargs -I{} cp {} "$2"
}

git -C "$root" worktree add --detach "$work/ref" "$REF" >/dev/null
# A reference from before the driver had a file of its own keeps it in mod.rs. The workload writes
# the lookup files, so the driver reads them from the workload's `bundle_dir`.
if [ -f "$work/ref/tests/reference/driver.rs" ]; then
    cp "$work/ref/tests/reference/driver.rs" "$work/driver.rs"
else
    { sed '/^pub mod /d; /^pub fn bundle_dir/,/^}/d' "$work/ref/tests/reference/mod.rs"
      echo 'use super::bundle_dir;'; } >"$work/driver.rs"
fi
rm -rf "$work/ref/tests/panos" "$work/ref/tests/reference"
cp -r "$root/tests/panos" "$root/tests/reference" "$root/tests/panos-ecs-profile.rs" \
    "$root/tests/reference-pipelines.rs" "$work/ref/tests/"
cp "$work/driver.rs" "$work/ref/tests/reference/driver.rs"
build "$work/ref" "$work/ref.bin" "$root/target/bench-ref"
build "$root" "$work/head.bin" "$root/target/bench-head"

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

python3 - "$work" "$RUNS" "$REF" "$(git -C "$root" rev-parse --short HEAD)" "$log" $names <<'EOF'
import datetime, os, statistics, sys
work, runs, ref, head, log, names = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6:]

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
        cpu = lambda s: f"{med(s, lambda r: r['task-clock'] * 1000 / r['docs']):.2f}" if s == "head" or has_ref else "n/a"
        row = (f"| {datetime.date.today()} | {name} | {head} | {ref} | {ratio} | {h / 1000:.0f}k "
               f"| {mb_in:.0f} | {mb_out:.0f} | {pf('head')} ({pf('ref')}) | {cpu('head')} ({cpu('ref')}) |")
        out.write(row + "\n")
        print(row)
EOF

"$root/bench/bench-plot.py" "$log" "${log%.md}.svg"
