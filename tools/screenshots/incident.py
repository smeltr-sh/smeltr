#!/usr/bin/env python3
"""Freeze one worker until the leader reports it, capture the pages that show it, then thaw the worker.

    python3 incident.py [worker]      # default fw-collector-02

The shots are `incident.json`'s: Issues, and a Status that needs attention.
"""

import os
import signal
import subprocess
import sys
import time

import stage

worker = sys.argv[1] if len(sys.argv) > 1 else "fw-collector-02"
supervisor = stage.started().get(worker)
if supervisor is None:
    raise SystemExit(f"{worker} is not running; stage a fleet first")
# Frozen rather than stopped: it keeps its partitions and its credential, and picks up where it was.
frozen = [supervisor] + stage.children(supervisor)
stage.log(f"freezing {worker}")
for pid in frozen:
    os.kill(pid, signal.SIGSTOP)
try:
    admin = stage.as_("admin")
    for _ in range(60):
        conditions = {w["condition"] for w in admin.call("GET", "/api/warnings")}
        if "workerSilent" in conditions:
            break
        time.sleep(5)
    else:
        raise SystemExit(f"the leader never reported {worker} silent, so Issues would show nothing wrong")
    stage.log(f"  the leader reports {', '.join(sorted(conditions))}")
    # Long enough for the rows it stopped reading to show as lag beside it.
    time.sleep(20)
    shot = subprocess.run(["node", "shoot.mjs", "incident.json"], cwd=stage.DIR)
finally:
    for pid in frozen:
        os.kill(pid, signal.SIGCONT)
    stage.log(f"thawed {worker}")
sys.exit(shot.returncode)
