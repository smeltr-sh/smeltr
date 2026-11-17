#!/bin/sh
# The runs on the detector smeltr ships. Resumable: each step skips when its output exists, and the long ones keep one
# part per series, config or mix.
cd "$(dirname "$0")"
step() { out=$1; shift; [ -f "$out" ] && return; s=$(date +%s); "$@" >> "$out.log" 2>&1 || { echo "FAILED $out"; return; }; echo "$out $(( $(date +%s) - s ))s"; }
step shipped.json python3 split_run.py nab shipped shipped.json
step shipped_logs.json python3 split_run.py logs shipped shipped_logs.json
step shipped_senders.json env SHIPPED=1 python3 extend_lab.py senders shipped_senders shipped_senders.json
step shipped_grid.json python3 split_run.py nab shipped_grid shipped_grid.json
step shipped_widths.json python3 split_run.py nab shipped_widths shipped_widths.json
step shipped_gate.json python3 split_run.py nab shipped_gate shipped_gate.json
step shipped_hist7.json python3 split_run.py nab shipped_hist7 shipped_hist7.json
step shipped_sparse.json env SHIPPED=1 python3 sparse_lab.py shipped_sparse.json
step shipped_sparse_history.json env SHIPPED=1 python3 sparse_lab.py shipped_sparse_history.json history
step shipped_either.json env SHIPPED=1 python3 either_lab.py nab shipped_either.json
step shipped_values_logs.json env SHIPPED=1 python3 values_logs.py real/ shipped_values_logs.json
step discover.json env SHIPPED=1 python3 discover_lab.py discover.json
echo done > shipped.done
