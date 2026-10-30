#!/usr/bin/env python3
"""A demo fleet from nothing: a fresh leader, four workers, and a change history written by three people.

    python3 stage.py          # stop the last run, reset, start, and write the history, paced over ~15 minutes
    PACE=0 python3 stage.py   # the same, with no waits between changes
    python3 stage.py traffic  # start gen.py writing to Kafka, unless it already is
    python3 stage.py stop     # stop what the last run started, and the traffic

Needs Kafka and Elasticsearch from `tests/live/compose.live.yaml`, and a release binary. The fleet writes under ./state.
"""

import contextlib
import copy
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse

import api
import bundle

DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(DIR))
STATE = os.path.join(DIR, "state")
PIDS = os.path.join(STATE, "pids")
TRAFFIC = os.path.join(DIR, ".gen.pid")
BINARY = os.path.join(REPO, os.environ.get("CARGO_TARGET_DIR", "target"), "release", "smeltr")
PACE = float(os.environ.get("PACE", "1"))

# A throwaway leader on loopback: these open nothing a run does not create and then discard.
ADMIN_PW = "Smeltr-Demo-2026!"
PEOPLE = {"priya": ("Priya Raman", "Firewall-Logs-2026!"), "marcus": ("Marcus Webb", "Platform-Logs-2026!")}
WORKERS = [("fw-collector-01", "network"), ("fw-collector-02", "network"),
           ("app-collector-01", "apps"), ("app-collector-02", "apps")]
TOPICS = [("syslog.panos", 4), ("syslog.fortigate", 4), ("web.nginx.access", 2), ("syslog.auth", 1),
          ("aws.cloudtrail", 1), ("syslog.asa", 2), ("smeltr.dlq", 1), ("alerts.security", 2)]
CONSUMER_GROUPS = [s["groupId"] for s in bundle.NETWORK_SOURCES + bundle.APPS_SOURCES]
KAFKA = ["docker", "compose", "-f", os.path.join(REPO, "tests", "live", "compose.live.yaml"), "exec", "-T", "kafka"]


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def pause(minutes):
    if PACE > 0:
        log(f"  waiting {minutes * PACE:.1f} min")
        time.sleep(minutes * 60 * PACE)


def as_(username):
    return api.Client.signed_in(username, ADMIN_PW if username == "admin" else PEOPLE[username][1])


# -- the processes this tool starts ------------------------------------------------------------------


def start_time(pid):
    """When `pid` started, fixed for the life of the process; None if there is no such process. Once a process ends,
    the system may give its pid to anything, so a pid is ours only while this still matches what was recorded."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            # The name in parentheses may hold spaces. After it come the state, where Z is a process that has ended
            # and waits only to be reaped, and 19 fields on, `starttime`, in ticks since boot.
            fields = f.read().rsplit(")", 1)[1].split()
            return None if fields[0] == "Z" else fields[19]
    except FileNotFoundError:
        if os.path.isdir("/proc/self"):
            return None
    # No procfs, as on macOS.
    out = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True,
                         env=dict(os.environ, LC_ALL="C")).stdout.strip()
    return out or None


def started(pidfile=PIDS):
    """`{name: pid}` for every process in `pidfile` that is still the one recorded there."""
    if not os.path.exists(pidfile):
        return {}
    procs = {}
    with open(pidfile) as f:
        for line in f:
            # A record with no start time cannot be told apart from whatever holds its pid now.
            fields = line.split(None, 2)
            if len(fields) == 3 and start_time(int(fields[1])) == fields[2].strip():
                procs[fields[0]] = int(fields[1])
    return procs


def children(pid):
    out = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True).stdout
    return [int(p) for p in out.split()]


def spawn(pidfile, name, argv, cwd, logfile, env=None):
    p = subprocess.Popen(argv, cwd=cwd, env=env, stdout=open(logfile, "w"), stderr=subprocess.STDOUT,
                         start_new_session=True)
    with open(pidfile, "a") as f:
        f.write(f"{name} {p.pid} {start_time(p.pid)}\n")


def fleet_env(**settings):
    """The caller's environment without any Smeltr setting, then the demo's own. A `SMELTR_*` exported for a
    developer's own fleet would otherwise point this one at that fleet's data."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("SMELTR_")}
    return dict(env, **settings)


def stop(pidfile=PIDS):
    """Stop what `pidfile` records, and nothing else: a developer's own leader may be running beside it."""
    if not os.path.exists(pidfile):
        return
    procs = started(pidfile)
    log(f"stop {', '.join(procs) or 'nothing still running'}")
    # A worker's supervisor ends its runtime child on the way down; the child is only killed if it outlives it.
    tracked = {pid: start_time(pid) for pid in list(procs.values()) + [k for p in procs.values() for k in children(p)]}

    def running():
        return [pid for pid, start in tracked.items() if start is not None and start_time(pid) == start]

    # Suppressed: a process may end between the check and the signal.
    for pid in procs.values():
        if pid in running():
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGTERM)
    deadline = time.time() + 120
    while time.time() < deadline and running():
        time.sleep(1)
    for pid in running():
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    os.remove(pidfile)


def start_traffic():
    """Start `gen.py`, unless the one started last is still running. It outlives this process and every reset."""
    if started(TRAFFIC):
        return
    stop(TRAFFIC)
    log("traffic")
    spawn(TRAFFIC, "gen", [sys.executable, "gen.py"], DIR, os.path.join(DIR, "gen.log"))


# -- reset and start ----------------------------------------------------------------------------------


def kafka(*args):
    return subprocess.run(KAFKA + list(args), capture_output=True, text=True)


def topics(*args):
    r = kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:19092", *args)
    if r.returncode != 0:
        raise SystemExit(f"kafka-topics {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


def topic_partitions():
    """`{topic: partitions}` for every topic Kafka holds."""
    have = {}
    for line in topics("--describe").splitlines():
        fields = dict(re.findall(r"(\w+): (\S+)", line))
        if "PartitionCount" in fields:
            have[fields["Topic"]] = int(fields["PartitionCount"])
    return have


def reset():
    stop()
    log("reset")
    # A previous run's committed offsets would replay its backlog: a source starts at `latest` only without them.
    # A group whose consumers just stopped stays non-empty until their sessions expire.
    for _ in range(12):
        listed = kafka("/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:19092", "--list")
        live = [g for g in CONSUMER_GROUPS if g in listed.stdout.split()]
        if not live:
            break
        deleted = kafka("/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:19092", "--delete",
                        *[a for g in live for a in ("--group", g)])
        if "not empty" not in deleted.stdout + deleted.stderr:
            break
        time.sleep(10)
    # A topic Kafka makes on a first write has one partition, and `gen.py` may write first. A group's two workers
    # split a topic by partition, so a short topic is grown rather than kept.
    have = topic_partitions()
    for topic, partitions in TOPICS:
        if topic not in have:
            topics("--create", "--if-not-exists", "--topic", topic, "--partitions", str(partitions),
                   "--replication-factor", "1")
    have = topic_partitions()
    for topic, partitions in TOPICS:
        if have[topic] < partitions:
            topics("--alter", "--topic", topic, "--partitions", str(partitions))
    es = [("DELETE", "logs-network,logs-web,logs-security", None),
          # One node: a replica has nowhere to go. And on a nearly full disk the watermark leaves every index
          # created after it unallocated, which stalls its destination.
          ("PUT", "_cluster/settings", '{"persistent":{"cluster.routing.allocation.disk.threshold_enabled":false}}'),
          ("PUT", "_index_template/smeltr-screenshots",
           '{"index_patterns":["logs-*"],"template":{"settings":{"number_of_replicas":0}}}')]
    for method, path, body in es:
        subprocess.run(["curl", "-s", "-o", "/dev/null", "-X", method, f"localhost:9200/{path}",
                        "-H", "Content-Type: application/json"] + (["-d", body] if body else []), check=False)
    shutil.rmtree(STATE, ignore_errors=True)
    os.makedirs(STATE)


def gui_port_taken():
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", 8443)) == 0


def start_leader():
    # Checked before the reset as well; this catches whatever took the port while it ran.
    if gui_port_taken():
        raise SystemExit("something took 127.0.0.1:8443 during the reset; stop it and run again")
    log("leader")
    wd = os.path.join(STATE, "leader")
    os.makedirs(wd)
    logfile = os.path.join(STATE, "leader.log")
    # No worker in this process; the group it names is the one a fresh leader seeds, so no `default` is left over.
    spawn(PIDS, "leader", [BINARY], wd, logfile, fleet_env(HOSTNAME="smeltr-leader", SMELTR_LEADER_ENABLED="1",
                                                           SMELTR_GUI_PORT="8443", SMELTR_WORKER_GROUP="network"))
    for _ in range(60):
        with open(logfile) as f:
            text = f.read()
        if "one-time password:" in text:
            return text.split("one-time password:")[1].split()[0]
        time.sleep(1)
    raise SystemExit(f"the leader printed no one-time password; see {logfile}")


def start_workers():
    log("workers")
    admin = as_("admin")
    codes = {}
    for group in ("network", "apps"):
        r = admin.ok(admin.call("POST", "/api/enrollment-codes",
                                {"groupId": group, "maxUses": 2, "lifetimeHours": 24}))
        codes[group] = r["code"]["value"]
    for host, group in WORKERS:
        wd = os.path.join(STATE, host)
        os.makedirs(os.path.join(wd, "smeltr-worker-data"))
        # A worker keeps the id it finds here, so the fleet reads as hosts rather than as `host-pid-nanos`.
        with open(os.path.join(wd, "smeltr-worker-data", "worker_id"), "w") as f:
            f.write(host)
        spawn(PIDS, host, [BINARY], wd, os.path.join(STATE, f"{host}.log"),
              fleet_env(HOSTNAME=host, SMELTR_WORKER_ENABLED="1", SMELTR_WORKER_GROUP=group,
                        SMELTR_WORKER_ENROLLMENT_CODE=codes[group], SMELTR_LEADER_URL=api.BASE,
                        SMELTR_WORKER_THREAD_LIMIT="2"))


# -- the people, and the changes they make ------------------------------------------------------------


def accounts(one_time):
    log("accounts")
    c = api.Client()
    c.login("admin", one_time)
    c.change_password("admin", ADMIN_PW)
    admin = as_("admin")
    for username, (display, pw) in PEOPLE.items():
        made = admin.call("POST", "/api/accounts", {"authSource": "local", "username": username, "displayName": display,
                                                   "roles": ["administrator"]})
        p = api.Client()
        p.login(username, made["temporaryPassword"])
        p.change_password(username, pw)


def upload_directory(c, branch, scope):
    boundary = "----smeltr-screenshots"
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"directory.csv\"\r\n"
            f"Content-Type: text/csv\r\n\r\n{bundle.directory_csv()}\r\n--{boundary}--\r\n").encode()
    res = c.call("POST", f"/api/areas/{scope}/lookups/upload", params={"branch": branch}, data=body,
                 content_type=f"multipart/form-data; boundary={boundary}")
    if not res.get("ok"):
        raise api.ApiError(f"upload refused: {res}")
    return {"id": "directory", "type": "csv", "path": res["path"], "delimiter": ",", "headers": res.get("headers", [])}


def lock_tests(c, branch, scope, tests, at=None):
    at = at or int(time.time() * 1e6)
    for name, pipeline, sample, records in tests:
        c.lock_test(branch, scope, name, pipeline, sample, records, at)


def groups():
    log("change: worker groups")
    admin = as_("admin")
    b = "add-worker-groups"
    admin.ok(admin.call("PUT", "/api/worker-groups/network", {"name": "Network security"}, {"branch": b}))
    admin.create_group(b, "apps", "Applications & cloud", hue=None)
    admin.open_change(b, "Add the Applications & cloud worker group",
                      "Firewall collectors stay in Network security. The storefront, bastion and CloudTrail "
                      "collectors get a group of their own, so each fleet deploys and scales on its own.")
    admin.deploy(b)


def secret():
    log("secret")
    admin = as_("admin")
    admin.ok(admin.call("POST", "/api/secrets", {
        "name": "es-ingest-api-key", "custody": "leader-stored",
        "description": "API key the Elasticsearch destinations send. Write access to logs-* only.",
        "value": "ApiKey c21lbHRyLWluZ2VzdDpkZW1vLWtleS12MQ=="}))


def firewalls():
    log("change: firewalls")
    c = as_("priya")
    b = "onboard-firewalls"
    c.put("destinations", bundle.destinations(bundle.NETWORK_DESTINATIONS, 1), b, "network")
    c.put("lookups", [bundle.SITES_TABLE, bundle.APPS_TABLE, upload_directory(c, b, "network")], b, "network")
    c.put("sources", bundle.NETWORK_SOURCES, b, "network")
    for p in (bundle.ECS_COMMON, bundle.PANOS, bundle.FORTIGATE):
        c.put_pipeline(p, b, "network")
    for s in bundle.samples()["network"]:
        c.put_sample(s, b, "network")
    c.put("routes", bundle.NETWORK_ROUTES, b, "network")
    c.put("manifest", {"exceptions": bundle.NETWORK_EXCEPTIONS}, b, "network")
    lock_tests(c, b, "network", [
        ("panos-traffic maps sessions to ECS", "panos-traffic", "panos-traffic-sample", 10),
        ("fortigate-traffic maps and drops NTP", "fortigate-traffic", "fortigate-sample", 10),
    ])
    c.open_change(b, "Onboard PAN-OS and FortiGate firewalls",
                  "Both firewall fleets normalized to ECS and indexed in logs-network. Sessions are enriched with the "
                  "site from the address plan, the user's department from the HR directory, and App-ID risk.")
    as_("admin").deploy(b)


def apps():
    log("change: apps")
    c = as_("marcus")
    b = "onboard-web-and-cloud"
    # The 5xx alert arrives in a change of its own.
    routes = copy.deepcopy(bundle.APPS_ROUTES)
    web = next(r for r in routes if r["id"] == "web")
    web["deliver"] = [d for d in web["deliver"] if "status_code" not in d.get("when", "")]
    c.put("destinations", bundle.destinations(bundle.APPS_DESTINATIONS, 1), b, "apps")
    c.put("sources", bundle.APPS_SOURCES, b, "apps")
    for p in (bundle.NGINX, bundle.AUTH, bundle.CLOUDTRAIL):
        c.put_pipeline(p, b, "apps")
    for s in bundle.samples()["apps"]:
        c.put_sample(s, b, "apps")
    c.put("routes", routes, b, "apps")
    lock_tests(c, b, "apps", [
        ("nginx-access maps requests, drops health checks", "nginx-access", "nginx-sample", 12),
        ("linux-auth classifies logins and sudo", "linux-auth", "auth-sample", 10),
        ("aws-cloudtrail maps management events", "aws-cloudtrail", "cloudtrail-sample", 8),
    ])
    c.open_change(b, "Onboard storefront, bastion and CloudTrail logs",
                  "nginx access logs to logs-web; sshd and sudo from the bastions and every CloudTrail management "
                  "event to logs-security. Failed CloudTrail calls also go to the alerts topic.")
    as_("admin").deploy(b)


def alert_5xx():
    log("change: 5xx alerts")
    c = as_("marcus")
    b = "alert-on-storefront-5xx"
    c.put("routes", bundle.APPS_ROUTES, b, "apps")
    c.open_change(b, "Alert on storefront 5xx responses",
                  "Server errors from the storefront also go to alerts.security, so the on-call sees an upstream "
                  "outage before customers report it.")
    as_("admin").deploy(b)


def rotate():
    log("change: rotate key")
    admin = as_("admin")
    b = "rotate-es-ingest-key"
    admin.ok(admin.call("POST", "/api/secrets/es-ingest-api-key/versions", {"value": "ApiKey c21lbHRyLWluZ2VzdDpkZW1vLWtleS12Mg=="}))
    r = admin.ok(admin.call("POST", "/api/secrets/es-ingest-api-key/references", {"version": 2, "branch": b}))
    if not r.get("openedChange"):
        admin.open_change(b, "Rotate the Elasticsearch ingest key")
    admin.ok(admin.call("PATCH", f"/api/branches/{b}", {
        "title": "Rotate the Elasticsearch ingest key to v2",
        "description": "Quarterly rotation. Every Elasticsearch destination in both groups moves to v2; v1 stays "
                       "stored until the fleet reports it running."}))
    admin.deploy(b)


def open_change():
    log("open change for review")
    c = as_("priya")
    b = "flag-high-risk-apps"
    p = copy.deepcopy(bundle.PANOS)
    at = next(i for i, s in enumerate(p["steps"]) if s["id"] == "derive")
    p["steps"].insert(at + 1, {"type": "eval", "id": "risk", "assignments": bundle.ev([
        ("event.risk_score", "panw.app.risk * 20"),
        ("panw.app.high_risk", "panw.app.risk >= 4"),
    ])})
    c.put_pipeline(p, b, "network")
    routes = copy.deepcopy(bundle.NETWORK_ROUTES)
    next(r for r in routes if r["id"] == "panos")["deliver"].insert(1, {"when": "panw.app.risk >= 5",
                                                                      "to": ["kafka-alerts"]})
    c.put("routes", routes, b, "network")
    # Re-locked at the instant it was first locked: a lock pins `now()`, so a new instant would rewrite every
    # `event.ingested` in the test and bury the two fields this change adds.
    name = "panos-traffic maps sessions to ECS"
    old = c.call("GET", f"/api/areas/network/tests/{urllib.parse.quote(name)}", params={"branch": b})["test"]
    lock_tests(c, b, "network", [(name, "panos-traffic", "panos-traffic-sample", 10)], at=old["lockedAt"])
    c.open_change(b, "PAN-OS: score App-ID risk and alert on the riskiest apps",
                  "Adds event.risk_score from App-ID risk and sends risk-5 applications (BitTorrent, Tor, SMTP) to "
                  "the alerts topic. The PAN-OS test is re-locked with the two new fields.")


def draft():
    log("draft branch")
    c = as_("marcus")
    c.put_pipeline({"id": "cisco-asa", "name": "cisco-asa", "steps": [
        {"type": "comment", "id": "todo",
         "text": "Draft: ASA syslog from the colo firewalls. Connection build and teardown (302013/302014) first."},
        {"type": "parser", "id": "syslog", "input": "_raw", "kind": {"type": "grok", "patterns": [
            "%{SYSLOGTIMESTAMP:syslog_time} %{HOSTNAME:host_name} %%{WORD:vendor}-%{INT:severity}-%{INT:message_id}: "
            "%{GREEDYDATA:message}"]},
         "declared": [{"name": n, "type": "str"} for n in
                      ["syslog_time", "host_name", "vendor", "severity", "message_id", "message"]]},
    ]}, "cisco-asa-draft", "network")


def main():
    if sys.argv[1:] == ["stop"]:
        stop(TRAFFIC)
        stop()
        return
    if sys.argv[1:] == ["traffic"]:
        start_traffic()
        return
    # Anything else would reset: a typo must not wipe a fleet.
    if sys.argv[1:]:
        raise SystemExit(__doc__)
    # The reset stops the last run's leader, so only another listener is a reason to refuse, and it is refused before
    # the reset destroys anything.
    if gui_port_taken() and "leader" not in started():
        raise SystemExit("something other than this tool's leader listens on 127.0.0.1:8443; stop it and run again. "
                         "Nothing was reset.")
    reset()
    one_time = start_leader()
    accounts(one_time)
    groups()
    start_workers()
    # Paced so History reads as work spread over a quarter of an hour, and each feed has come up before the next.
    pause(1)
    secret()
    firewalls()
    pause(4)
    apps()
    pause(4)
    alert_5xx()
    pause(3)
    rotate()
    pause(1)
    open_change()
    draft()
    log("staged")


if __name__ == "__main__":
    main()
