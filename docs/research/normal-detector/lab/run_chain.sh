#!/bin/sh
# Resumable: every step skips when its output exists; the long steps keep one part per series or mix.
cd "$(dirname "$0")"
[ -f extend_paper.json ] || python3 extend_lab.py nab/ paper extend_paper.json >> extend_paper.log 2>&1
[ -f sparse_final.json ] || python3 sparse_lab.py sparse_final.json >> sparse_final.log 2>&1
[ -f extend_hist7.json ] || python3 extend_lab.py nab/ hist7 extend_hist7.json >> extend_hist7.log 2>&1
[ -f extend_widths.json ] || python3 extend_lab.py nab/ widths extend_widths.json >> extend_widths.log 2>&1
[ -f extend_grid2.json ] || python3 extend_lab.py nab/ grid2 extend_grid2.json >> extend_grid2.log 2>&1
[ -f extend_gate.json ] || python3 extend_lab.py nab/ gate extend_gate.json >> extend_gate.log 2>&1
[ -f values_logs.json ] || python3 values_logs.py real/ values_logs.json >> values_logs.log 2>&1
[ -f extend_paper_logs.json ] || python3 extend_lab.py logs paper extend_paper_logs.json >> extend_paper_logs.log 2>&1
[ -f extend_paper_values.json ] || python3 extend_lab.py senders paper_senders extend_paper_values.json >> extend_paper_values.log 2>&1
echo done > chain2.done
