#!/usr/bin/env python3
"""Take one of a stream's two relays down until the leader says its source reads below normal, capture the pages
that show it, then bring the relay back.

    python3 outage.py [topic]      # default syslog.fortigate

The shots are `outage.json`'s. The source is judged on its last 30 minutes against the band its history gives, so the
warning comes once enough of that window reads half: ten minutes or so. The shots wait for `SHOWN_DOWN` of half, so
the Map's 24-hour chart shows a drop wide enough to see.
"""

import os
import subprocess
import sys
import time

import bundle
import stage

topic = sys.argv[1] if len(sys.argv) > 1 else "syslog.fortigate"
RELAYS = 2
SHOWN_DOWN = 15 * 60
source = next(s["id"] for s in bundle.NETWORK_SOURCES + bundle.APPS_SOURCES if topic in s["topics"])


def traffic(**env):
    stage.stop(stage.TRAFFIC_PIDS)
    stage.spawn(stage.TRAFFIC_PIDS, "traffic", [bundle.TRAFFIC], stage.DIR, os.path.join(stage.DIR, "traffic.log"),
                dict(os.environ, **env))


stage.log(f"taking one of {topic}'s {RELAYS} relays down")
traffic(SMELTR_DEMO_DOWN=f"{topic}/{RELAYS}")
down_at = time.monotonic()
try:
    # The leader ends a session that reports no activity for a while, and API calls report none, so each poll asks
    # for one afresh.
    for _ in range(90):
        below = [w for w in stage.as_("admin").call("GET", "/api/warnings")["warnings"]
                 if w["condition"] == "sourceRowsBelow" and w["subjectId"] == source]
        if below:
            stage.log(f"  {below[0]['message']}")
            break
        time.sleep(20)
    else:
        raise SystemExit(f"the leader never reported {source} below normal, so the shots would show nothing wrong")
    time.sleep(max(0.0, SHOWN_DOWN - (time.monotonic() - down_at)))
    stage.as_("admin")  # The shots sign in with the session it leaves.
    shot = subprocess.run(["node", "shoot.mjs", "outage.json"], cwd=stage.DIR)
finally:
    traffic()
    stage.log(f"{topic} is back")
sys.exit(shot.returncode)
