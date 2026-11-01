"""The demo deployment: what each scope holds, as the API takes it."""

import functools
import json
import os
import subprocess

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# The reference generators' driver (`examples/demo-traffic.rs`): the live traffic, the samples and the lookup tables
# all come from it, so a pipeline here joins on what the logs carry.
TRAFFIC = os.path.join(REPO, os.environ.get("CARGO_TARGET_DIR", "target"), "release", "examples", "demo-traffic")


def traffic(*args):
    return subprocess.run([TRAFFIC, *map(str, args)], capture_output=True, text=True, check=True).stdout


@functools.cache
def tables():
    return json.loads(traffic("tables"))


KAFKA = "localhost:19092"
ES = "http://localhost:9200"
SPLUNK = "http://localhost:8088"
S3 = "http://localhost:4566"
ARCHIVE_BUCKET = "acme-firewall-archive"

# -- shared ------------------------------------------------------------------------

# A refused record goes to the DLQ as it was sent.
TO_DLQ = {"kinds": {"Refused": {"deliver": ["kafka-dlq"]}}}

_ALL_DESTINATIONS = {
    "es-network": {"id": "es-network", "type": "elasticsearch", "url": ES, "defaultIndex": "logs-network",
                   "exceptions": TO_DLQ},
    "es-security": {"id": "es-security", "type": "elasticsearch", "url": ES, "defaultIndex": "logs-security",
                    "exceptions": TO_DLQ},
    "es-web": {"id": "es-web", "type": "elasticsearch", "url": ES, "defaultIndex": "logs-web",
               "exceptions": TO_DLQ},
    "kafka-alerts": {"id": "kafka-alerts", "type": "kafka", "brokers": KAFKA, "defaultTopic": "alerts.security",
                     "compression": "zstd"},
    "kafka-dlq": {"id": "kafka-dlq", "type": "kafka", "brokers": KAFKA, "defaultTopic": "smeltr.dlq"},
    # The SIEM, sent PAN-OS lines as they were sent, trimmed. The workers hold the token (stage.py).
    "splunk": {"id": "splunk", "type": "splunkHec", "url": SPLUNK, "token": "SPLUNK_HEC_TOKEN", "endpoint": "raw",
               "format": "text", "index": "main", "sourcetype": "pan:traffic"},
    # Every record as it arrived, for a search or a replay later. The workers' AWS settings sign for LocalStack.
    "s3-archive": {"id": "s3-archive", "type": "s3", "bucket": ARCHIVE_BUCKET, "region": "us-east-1",
                   "endpoint": S3, "format": "bytes"},
}


def destinations(ids, key_version):
    """Each group's own destinations; Elasticsearch authenticates with the stored ingest key at `key_version`."""
    out = []
    for i in ids:
        d = dict(_ALL_DESTINATIONS[i])
        if d["type"] == "elasticsearch":
            d["auth"] = {"secret": "es-ingest-api-key", "version": key_version}
        out.append(d)
    return out


NETWORK_DESTINATIONS = ["es-network", "kafka-alerts", "kafka-dlq"]
SIEM_DESTINATIONS = ["splunk", "s3-archive"]
APPS_DESTINATIONS = ["es-web", "es-security", "kafka-alerts", "kafka-dlq"]

def sites_table():
    return {"id": "sites", "type": "inline", **tables()["sites"]}


def apps_table():
    return {"id": "app-risk", "type": "inline", **tables()["apps"]}


def directory_csv():
    d = tables()["directory"]
    return "\n".join(",".join(row) for row in [d["headers"], *d["rows"]]) + "\n"


ECS_COMMON = {
    "id": "ecs-common", "name": "ecs-common",
    "steps": [
        {"type": "comment", "id": "why", "text": "Called at the end of every pipeline, so each document carries the same ECS version and ingest time."},
        {"type": "eval", "id": "stamp", "assignments": [
            {"target": "ecs.version", "expr": '"8.17.0"'},
            {"target": "event.ingested", "expr": "now()"},
        ]},
    ],
}

# -- network: PAN-OS ------------------------------------------------------------------

PANOS_HEADERS = [
    "future_use1", "receive_time", "serial", "type", "subtype", "future_use2", "generated_time",
    "source_address", "destination_address", "nat_source_ip", "nat_destination_ip", "rule_name", "source_user",
    "destination_user", "application", "virtual_system", "source_zone", "destination_zone", "inbound_interface",
    "outbound_interface", "log_action", "future_use3", "session_id", "repeat_count", "source_port",
    "destination_port", "nat_source_port", "nat_destination_port", "flags", "protocol", "action", "bytes",
    "bytes_sent", "bytes_received", "packets", "start_time", "elapsed_time", "category", "future_use4",
    "sequence_number", "action_flags", "source_location", "destination_location", "future_use5", "packets_sent",
    "packets_received", "session_end_reason", "device_group_hierarchy_level_1", "device_group_hierarchy_level_2",
    "device_group_hierarchy_level_3", "device_group_hierarchy_level_4", "virtual_system_name", "device_name",
    "action_source",
]
PANOS_KEEP = {
    "serial", "type", "subtype", "source_address", "destination_address", "nat_source_ip", "rule_name",
    "source_user", "application", "source_zone", "destination_zone", "inbound_interface", "outbound_interface",
    "session_id", "source_port", "destination_port", "protocol", "action", "bytes_sent", "bytes_received",
    "packets_sent", "packets_received", "elapsed_time", "category", "destination_location", "session_end_reason",
    "device_name",
}
PANOS_NUMERIC = {"source_port", "destination_port", "bytes_sent", "bytes_received", "packets_sent",
                 "packets_received", "elapsed_time"}


def panos_columns():
    cols = []
    for h in PANOS_HEADERS:
        if h == "receive_time":
            cols.append({"name": "@timestamp", "type": "datetime"})
        elif h in PANOS_NUMERIC:
            cols.append({"name": h, "type": "i64"})
        elif h in PANOS_KEEP:
            cols.append({"name": h, "type": "str"})
        else:
            cols.append({"name": h, "type": "skip"})
    return cols


def ev(pairs):
    return [{"target": t, "expr": e} for t, e in pairs]


PANOS = {
    "id": "panos-traffic", "name": "panos-traffic",
    "steps": [
        {"type": "comment", "id": "about", "text": "PAN-OS 10.x TRAFFIC over syslog, normalized to ECS. Enriched with the site, the user's department and App-ID risk."},
        {"type": "parser", "id": "syslog", "input": "_raw",
         "kind": {"type": "grok", "patterns": ["<%{INT:pri}>%{SYSLOGTIMESTAMP:syslog_time} %{HOSTNAME:relay} %{GREEDYDATA:panos}"]},
         "declared": [{"name": n, "type": "str"} for n in ["pri", "syslog_time", "relay", "panos"]]},
        {"type": "parser", "id": "fields", "input": "panos", "kind": {"type": "csv", "delimiter": ","},
         "declared": panos_columns()},
        {"type": "lookup", "id": "site", "input": "source_address", "table": "sites", "matchKind": "cidr",
         "multiplicity": "best", "keyColumn": "network",
         "outputs": [{"source": "site", "field": "source.site"}, {"source": "city", "field": "source.geo.city_name"},
                     {"source": "tier", "field": "source.tier"}]},
        {"type": "lookup", "id": "who", "input": "source_user", "table": "directory", "matchKind": "exact",
         "multiplicity": "first", "keyColumn": "account",
         "outputs": [{"source": "department", "field": "user.department"},
                     {"source": "full_name", "field": "user.full_name"}]},
        {"type": "lookup", "id": "app", "input": "application", "table": "app-risk", "matchKind": "exact",
         "multiplicity": "first", "keyColumn": "name",
         "outputs": [{"source": "category", "field": "panw.app.category"},
                     {"source": "risk", "field": "panw.app.risk", "type": "i64"}]},
        {"type": "eval", "id": "derive", "assignments": ev([
            ("event.dataset", '"panw.panos"'),
            ("event.kind", '"event"'),
            ("event.category", '"network"'),
            ("event.action", "action.toLowerCase()"),
            ("event.outcome", 'action == "allow" ? "success" : "failure"'),
            ("event.duration", "elapsed_time * 1000000000"),
            ("source.port", "source_port"),
            ("destination.port", "destination_port"),
            ("source.bytes", "bytes_sent"),
            ("destination.bytes", "bytes_received"),
            ("network.bytes", "bytes_sent + bytes_received"),
            ("network.packets", "packets_sent + packets_received"),
            ("network.direction", 'source_zone == "untrust" ? "inbound" : "outbound"'),
            ("user.name", 'source_user.split("\\\\")[-1]'),
            ("observer.vendor", '"Palo Alto Networks"'),
            ("observer.product", '"PAN-OS"'),
        ])},
        {"type": "rename", "id": "to-ecs", "pairs": [{"from": f, "to": t} for f, t in [
            ("source_address", "source.ip"), ("destination_address", "destination.ip"),
            ("nat_source_ip", "source.nat.ip"), ("protocol", "network.transport"),
            ("application", "network.application"), ("rule_name", "rule.name"),
            ("device_name", "observer.name"), ("serial", "observer.serial_number"),
            ("source_zone", "observer.ingress.zone"), ("destination_zone", "observer.egress.zone"),
            ("inbound_interface", "observer.ingress.interface.name"),
            ("outbound_interface", "observer.egress.interface.name"),
            ("destination_location", "destination.geo.country_name"),
            ("session_id", "panw.session_id"), ("category", "url.category"),
            ("session_end_reason", "panw.session_end_reason"), ("subtype", "panw.subtype"),
        ]]},
        {"type": "eval", "id": "prune", "assignments": ev([(n, "null") for n in [
            "_raw", "panos", "pri", "syslog_time", "relay", "type", "action", "elapsed_time", "source_port",
            "destination_port", "bytes_sent", "bytes_received", "packets_sent", "packets_received", "source_user",
        ]])},
        {"type": "call", "id": "common", "pipeline": "ecs-common"},
    ],
}

# -- network: PAN-OS trimmed for the SIEM ------------------------------------------------------------

# The pipeline the Cut SIEM costs page shows: the same line, with the columns no search uses left empty, and the
# allowed DNS lookups dropped.
PANOS_TRIM = {
    "id": "panos-trim", "name": "panos-trim",
    "steps": json.load(open(os.path.join(REPO, "website", "src", "lib", "siem-cut-example.json")))["steps"],
}

# A route runs a pipeline. The archive's changes nothing, and its destination writes the bytes each record arrived as.
AS_SENT = {
    "id": "as-sent", "name": "as-sent",
    "steps": [{"type": "comment", "id": "about", "text": "Nothing changed: the archive keeps each line as it was sent."}],
}

# -- network: FortiGate ---------------------------------------------------------------

FORTI_FIELDS = [
    ("date", "str"), ("time", "str"), ("devname", "str"), ("devid", "str"), ("type", "str"), ("subtype", "str"),
    ("level", "str"), ("srcip", "str"), ("srcport", "i64"), ("srcintf", "str"), ("dstip", "str"),
    ("dstport", "i64"), ("dstintf", "str"), ("dstcountry", "str"), ("sessionid", "i64"), ("proto", "i64"),
    ("action", "str"), ("policyid", "i64"), ("policyname", "str"), ("service", "str"), ("transip", "str"),
    ("transport", "i64"), ("app", "str"), ("appcat", "str"), ("apprisk", "str"), ("duration", "i64"),
    ("sentbyte", "i64"), ("rcvdbyte", "i64"), ("sentpkt", "i64"), ("rcvdpkt", "i64"),
]

FORTIGATE = {
    "id": "fortigate-traffic", "name": "fortigate-traffic",
    "steps": [
        {"type": "parser", "id": "syslog", "input": "_raw",
         "kind": {"type": "grok", "patterns": ["<%{INT:pri}>%{GREEDYDATA:body}"]},
         "declared": [{"name": "pri", "type": "str"}, {"name": "body", "type": "str"}]},
        {"type": "parser", "id": "kv", "input": "body",
         "kind": {"type": "keyValue", "pairSep": " ", "kvSep": "=", "quote": '"'},
         "declared": [{"name": n, "type": t} for n, t in FORTI_FIELDS]},
        {"type": "drop", "id": "drop-ntp", "condition": 'service == "NTP"'},
        {"type": "lookup", "id": "site", "input": "srcip", "table": "sites", "matchKind": "cidr",
         "multiplicity": "best", "keyColumn": "network",
         "outputs": [{"source": "site", "field": "source.site"}, {"source": "city", "field": "source.geo.city_name"}]},
        {"type": "eval", "id": "derive", "assignments": ev([
            ("@timestamp", '(date + " " + time).parseTime()'),
            ("event.dataset", '"fortinet_fortigate.log"'),
            ("event.category", '"network"'),
            ("event.action", "action"),
            ("event.outcome", 'action == "deny" ? "failure" : "success"'),
            ("event.duration", "duration * 1000000000"),
            ("network.transport", 'proto == 6 ? "tcp" : proto == 17 ? "udp" : "icmp"'),
            ("network.protocol", "service.toLowerCase()"),
            ("network.bytes", "sentbyte + rcvdbyte"),
            ("network.packets", "sentpkt + rcvdpkt"),
            ("observer.vendor", '"Fortinet"'),
            ("observer.product", '"FortiGate"'),
        ])},
        {"type": "rename", "id": "to-ecs", "pairs": [{"from": f, "to": t} for f, t in [
            ("srcip", "source.ip"), ("srcport", "source.port"), ("dstip", "destination.ip"),
            ("dstport", "destination.port"), ("sentbyte", "source.bytes"), ("rcvdbyte", "destination.bytes"),
            ("transip", "source.nat.ip"), ("transport", "source.nat.port"), ("devname", "observer.name"),
            ("devid", "observer.serial_number"), ("srcintf", "observer.ingress.interface.name"),
            ("dstintf", "observer.egress.interface.name"), ("policyid", "rule.id"), ("policyname", "rule.name"),
            ("app", "network.application"), ("appcat", "fortinet.firewall.appcat"),
            ("apprisk", "fortinet.firewall.apprisk"), ("sessionid", "fortinet.firewall.sessionid"),
            ("dstcountry", "destination.geo.country_name"), ("url", "url.original"),
        ]]},
        {"type": "eval", "id": "prune", "assignments": ev([(n, "null") for n in [
            "_raw", "pri", "body", "date", "time", "type", "subtype", "level", "action", "proto", "service", "duration",
            "sentpkt", "rcvdpkt", "eventtime", "tz", "logid", "vd", "srcintfrole", "dstintfrole", "srccountry",
            "policytype", "trandisp",
        ]])},
        {"type": "call", "id": "common", "pipeline": "ecs-common"},
    ],
}

# -- apps: nginx ------------------------------------------------------------------------

NGINX = {
    "id": "nginx-access", "name": "nginx-access",
    "steps": [
        {"type": "parser", "id": "access", "input": "_raw", "filter": '!_raw.startsWith("#")',
         "kind": {"type": "grok", "patterns": [
             '%{IPORHOST:client_ip} - (?:-|%{DATA:user_name}) \\[%{HTTPDATE:access_time}\\] "%{WORD:method} %{DATA:url_original} HTTP/%{NUMBER:http_version}" %{INT:status} (?:-|%{INT:body_bytes}) "(?:-|%{DATA:referrer})" "(?:-|%{DATA:ua})"'
         ], "customPatterns": [{"name": "IPORHOST", "pattern": "(?:%{IP}|%{HOSTNAME})"}]},
         "declared": [
             {"name": "client_ip", "type": "str"}, {"name": "user_name", "type": "str"},
             {"name": "access_time", "type": "datetime"}, {"name": "method", "type": "str"},
             {"name": "url_original", "type": "str"}, {"name": "http_version", "type": "str"},
             {"name": "status", "type": "i64"}, {"name": "body_bytes", "type": "i64"},
             {"name": "referrer", "type": "str"}, {"name": "ua", "type": "str"},
         ]},
        {"type": "drop", "id": "health-checks", "condition": 'url_original == "/healthz"'},
        {"type": "eval", "id": "derive", "assignments": ev([
            ("event.dataset", '"nginx.access"'),
            ("event.category", '"web"'),
            ("event.outcome", 'status >= 400 ? "failure" : "success"'),
            ("url.path", 'url_original.split("?")[0]'),
            ("url.extension", 'url_original.test(/\\.[a-z0-9]+$/) ? url_original.split(".")[-1] : null'),
            ("user_agent.name", 'ua.includes("Firefox") ? "Firefox" : ua.includes("Chrome") ? "Chrome" : ua.includes("Safari") ? "Safari" : ua.includes("bot") ? "Bot" : "Other"'),
            ("user_agent.device.name", 'ua.includes("iPhone") ? "iPhone" : ua.includes("Macintosh") ? "Mac" : ua.includes("Windows") ? "Windows PC" : "Other"'),
            ("source.address", "client_ip"),
            ("service.name", '"storefront"'),
            ("ecs.version", '"8.17.0"'),
            ("event.ingested", "now()"),
        ])},
        {"type": "rename", "id": "to-ecs", "pairs": [{"from": f, "to": t} for f, t in [
            ("access_time", "@timestamp"), ("client_ip", "source.ip"), ("user_name", "user.name"),
            ("method", "http.request.method"), ("url_original", "url.original"), ("http_version", "http.version"),
            ("status", "http.response.status_code"), ("body_bytes", "http.response.body.bytes"),
            ("referrer", "http.request.referrer"), ("ua", "user_agent.original"),
        ]]},
        {"type": "eval", "id": "prune", "assignments": ev([("_raw", "null")])},
    ],
}

# -- apps: auth.log -----------------------------------------------------------------------

AUTH = {
    "id": "linux-auth", "name": "linux-auth",
    "steps": [
        {"type": "parser", "id": "syslog", "input": "_raw",
         "kind": {"type": "grok", "patterns": [
             "%{SYSLOGTIMESTAMP:syslog_time} %{HOSTNAME:host_name} %{DATA:program}(?:\\[%{POSINT:pid}\\])?: %{GREEDYDATA:message}"
         ]},
         "declared": [{"name": "syslog_time", "type": "datetime"}, {"name": "host_name", "type": "str"},
                      {"name": "program", "type": "str"}, {"name": "pid", "type": "i64"},
                      {"name": "message", "type": "str"}]},
        {"type": "parser", "id": "message", "input": "message",
         "kind": {"type": "grok", "patterns": [
             "%{WORD:ssh_result} %{WORD:ssh_method} for (?:invalid user )?%{USERNAME:user_name} from %{IP:source_ip} port %{INT:source_port} ssh2(?:: %{GREEDYDATA:ssh_key})?",
             "Invalid user %{USERNAME:user_name} from %{IP:source_ip} port %{INT:source_port}",
             "\\s*%{USERNAME:user_name} : (?:%{DATA:sudo_error} ; )?TTY=%{DATA:sudo_tty} ; PWD=%{DATA:sudo_pwd} ; USER=%{DATA:sudo_as} ; COMMAND=%{GREEDYDATA:sudo_command}",
             "pam_unix\\(%{DATA:pam_service}:session\\): session %{WORD:session_state} for user %{USERNAME:user_name}",
         ]},
         "declared": [{"name": "ssh_result", "type": "str"}, {"name": "ssh_method", "type": "str"},
                      {"name": "user_name", "type": "str"}, {"name": "source_ip", "type": "str"},
                      {"name": "source_port", "type": "i64"}, {"name": "ssh_key", "type": "str"},
                      {"name": "sudo_error", "type": "str"}, {"name": "sudo_tty", "type": "str"},
                      {"name": "sudo_pwd", "type": "str"}, {"name": "sudo_as", "type": "str"},
                      {"name": "sudo_command", "type": "str"}, {"name": "pam_service", "type": "str"},
                      {"name": "session_state", "type": "str"}]},
        {"type": "code", "id": "classify", "body": "\n".join([
            'event.category = "authentication";',
            'if (program == "sudo") {',
            '  event.action = "sudo";',
            '  event.outcome = isNull(sudo_error) ? "success" : "failure";',
            '} else if (ssh_result == "Accepted") {',
            '  event.action = "ssh_login";',
            '  event.outcome = "success";',
            '} else if (ssh_result == "Failed" || message.startsWith("Invalid user")) {',
            '  event.action = "ssh_login";',
            '  event.outcome = "failure";',
            '} else {',
            '  event.category = "session";',
            '  event.action = "session_" + session_state;',
            '  event.outcome = "success";',
            '}',
        ])},
        {"type": "rename", "id": "to-ecs", "pairs": [{"from": f, "to": t} for f, t in [
            ("syslog_time", "@timestamp"), ("host_name", "host.hostname"), ("program", "process.name"),
            ("pid", "process.pid"), ("user_name", "user.name"), ("source_ip", "source.ip"),
            ("source_port", "source.port"), ("ssh_method", "system.auth.ssh.method"),
            ("ssh_key", "system.auth.ssh.signature"), ("sudo_command", "process.command_line"),
            ("sudo_as", "system.auth.sudo.user"), ("sudo_tty", "system.auth.sudo.tty"),
            ("sudo_pwd", "system.auth.sudo.pwd"), ("sudo_error", "system.auth.sudo.error"),
        ]]},
        {"type": "eval", "id": "stamp", "assignments": ev([
            ("event.dataset", '"system.auth"'),
            ("ecs.version", '"8.17.0"'),
            ("event.ingested", "now()"),
        ])},
        {"type": "eval", "id": "prune", "assignments": ev([(n, "null") for n in [
            "_raw", "ssh_result", "pam_service", "session_state",
        ]])},
    ],
}

# -- apps: CloudTrail ---------------------------------------------------------------------

CLOUDTRAIL = {
    "id": "aws-cloudtrail", "name": "aws-cloudtrail",
    "steps": [
        {"type": "parser", "id": "json", "input": "_raw", "kind": {"type": "json"},
         "declared": [{"name": "Records", "type": {"type": "array", "of": {"type": "struct", "fields": [
             {"name": "eventTime", "type": "str"}, {"name": "eventSource", "type": "str"},
             {"name": "eventName", "type": "str"}, {"name": "awsRegion", "type": "str"},
             {"name": "sourceIPAddress", "type": "str"}, {"name": "userAgent", "type": "str"},
             {"name": "errorCode", "type": "str"}, {"name": "errorMessage", "type": "str"},
             {"name": "eventType", "type": "str"}, {"name": "recipientAccountId", "type": "str"},
             {"name": "readOnly", "type": "bool"}, {"name": "eventID", "type": "str"},
             {"name": "userIdentity", "type": {"type": "struct", "fields": [
                 {"name": "type", "type": "str"}, {"name": "arn", "type": "str"},
                 {"name": "accountId", "type": "str"}, {"name": "userName", "type": "str"}]}},
             {"name": "responseElements", "type": {"type": "struct", "fields": [{"name": "ConsoleLogin", "type": "str"}]}},
             {"name": "additionalEventData", "type": {"type": "struct", "fields": [{"name": "MFAUsed", "type": "str"}]}},
         ]}}}]},
        {"type": "unroll", "id": "events", "field": "Records", "keepParentFields": False},
        {"type": "eval", "id": "derive", "assignments": ev([
            ("@timestamp", "parseTime(Records.eventTime)"),
            ("cloud.provider", '"aws"'),
            ("cloud.region", "Records.awsRegion"),
            ("cloud.account.id", "Records.recipientAccountId"),
            ("event.dataset", '"aws.cloudtrail"'),
            ("event.provider", "Records.eventSource"),
            ("event.action", "Records.eventName"),
            ("event.id", "Records.eventID"),
            ("event.outcome", '!isNull(Records.errorCode) || Records.responseElements.ConsoleLogin == "Failure" ? "failure" : "success"'),
            ("aws.cloudtrail.console_login.mfa_used", 'Records.eventName == "ConsoleLogin" ? Records.additionalEventData.MFAUsed == "Yes" : null'),
            ("event.category", 'Records.eventSource == "signin.amazonaws.com" ? "authentication" : Records.eventSource == "iam.amazonaws.com" ? "iam" : "configuration"'),
            ("source.ip", "isIp(Records.sourceIPAddress) ? Records.sourceIPAddress : null"),
            ("user.name", 'isNull(Records.userIdentity.userName) ? Records.userIdentity.arn.split("/")[-1] : Records.userIdentity.userName'),
            ("user_agent.original", "Records.userAgent"),
            ("aws.cloudtrail.user_identity.type", "Records.userIdentity.type"),
            ("aws.cloudtrail.user_identity.arn", "Records.userIdentity.arn"),
            ("aws.cloudtrail.error_code", "Records.errorCode"),
            ("aws.cloudtrail.error_message", "Records.errorMessage"),
            ("aws.cloudtrail.read_only", "Records.readOnly"),
            ("ecs.version", '"8.17.0"'),
            ("event.ingested", "now()"),
        ])},
        {"type": "eval", "id": "prune", "assignments": ev([("_raw", "null"), ("Records", "null")])},
    ],
}

# -- sources and routes ---------------------------------------------------------------------


def kafka_source(sid, topic, group):
    return {"id": sid, "type": "kafka", "brokers": KAFKA, "topics": [topic], "groupId": group,
            "start": {"type": "latest"}, "encoding": "utf-8"}


NETWORK_SOURCES = [
    kafka_source("panw-firewalls", "syslog.panos", "smeltr-panos"),
    kafka_source("fortigate-firewalls", "syslog.fortigate", "smeltr-fortigate"),
    # No route takes it: the `cisco-asa-draft` branch is where its pipeline is being written.
    kafka_source("cisco-asa", "syslog.asa", "smeltr-asa"),
    # Routed by no one, for a visitor to write a pipeline for.
    kafka_source("panw-threats", "syslog.panos.threat", "smeltr-panos-threat"),
    kafka_source("versa-sdwan", "syslog.versa", "smeltr-versa"),
    kafka_source("netscout-aed", "syslog.netscout", "smeltr-netscout"),
    kafka_source("router-snmp", "metrics.snmp.interface", "smeltr-snmp"),
]
# The same PAN-OS lines, read again for the SIEM and the archive.
SIEM_SOURCE = kafka_source("panw-siem", "syslog.panos", "smeltr-panos-siem")
APPS_SOURCES = [
    kafka_source("nginx-access", "web.nginx.access", "smeltr-nginx"),
    kafka_source("linux-auth", "syslog.auth", "smeltr-auth"),
    kafka_source("aws-cloudtrail", "aws.cloudtrail", "smeltr-cloudtrail"),
    # Routed by no one, for a visitor to write a pipeline for.
    kafka_source("elastic-agent-linux", "elastic.agent.linux", "smeltr-ea-linux"),
    kafka_source("elastic-agent-windows", "elastic.agent.windows", "smeltr-ea-windows"),
    kafka_source("sentinel-eventhub", "azure.eventhub.sentinel", "smeltr-sentinel"),
]

# Answers for a category and for one kind in it, so the Routes page shows the matrix's range.
NETWORK_EXCEPTIONS = {"categories": {"math": "drop", "limits": "block"},
                      "kinds": {"Overflow": "tag", "Unmatched": {"deliver": ["kafka-dlq"]}}}
# The feeds no route takes yet are dropped, not blocked: a source that blocks stops reading, and the leader would
# show it failed rather than waiting for a pipeline.
APPS_EXCEPTIONS = {"kinds": {"Unmatched": "drop"}}

NETWORK_ROUTES = [
    {"id": "panos", "source": "panw-firewalls", "condition": None, "pipeline": "panos-traffic",
     "deliver": [{"to": ["es-network"]}]},
    {"id": "fortigate", "source": "fortigate-firewalls", "condition": None, "pipeline": "fortigate-traffic",
     "deliver": [{"to": ["es-network"]}]},
]
# Two routes from one source, since every route whose condition matches runs: trimmed to the SIEM, and whole to
# the archive.
SIEM_ROUTES = [
    {"id": "panos-siem", "source": "panw-siem", "condition": None, "pipeline": "panos-trim",
     "deliver": [{"to": ["splunk"]}]},
    {"id": "panos-archive", "source": "panw-siem", "condition": None, "pipeline": "as-sent",
     "deliver": [{"to": ["s3-archive"]}]},
]
APPS_ROUTES = [
    {"id": "web", "source": "nginx-access", "condition": None, "pipeline": "nginx-access",
     "deliver": [{"to": ["es-web"]},
                 {"when": "http.response.status_code >= 500", "to": ["kafka-alerts"]}]},
    {"id": "auth", "source": "linux-auth", "condition": None, "pipeline": "linux-auth",
     "deliver": [{"to": ["es-security"]}]},
    {"id": "cloudtrail", "source": "aws-cloudtrail", "condition": None, "pipeline": "aws-cloudtrail",
     "deliver": [{"to": ["es-security"]},
                 {"when": 'event.outcome == "failure"', "to": ["kafka-alerts"]}]},
]

# -- samples: generated from the same code as the live traffic, at a fixed instant -----------------


def records(topic, n, seed):
    return [{"_raw": line} for line in traffic("sample", topic, n, seed).splitlines()]


def panos_sample():
    # Ten sessions picked for variety: web, SaaS, DNS, a DMZ server and a denied app.
    want = ["ms-office365-base", "ssl", "dns-base", "slack-base", "bittorrent", "github-base", "ms-teams",
            "web-browsing", "zoom-base", "salesforce-base"]
    out = []
    for r in records("syslog.panos", 5000, 11):
        line = r["_raw"]
        app = line.split(",")[14]
        if app in want and (app != "ssl" or ",dmz," in line) and (app == "ssl" or ",dmz," not in line) and (
                app in ("bittorrent", "dns-base", "ssl") or ",acme\\" in line):
            want.remove(app)
            out.append(r)
    assert not want, f"no {want} in the PAN-OS pool"
    return out


def samples():
    return {
        "network": [
            {"id": "panos-traffic-sample", "name": "PAN-OS traffic, mixed sessions", "source": "panw-firewalls",
             "records": panos_sample()},
            {"id": "fortigate-sample", "name": "FortiGate forward traffic", "source": "fortigate-firewalls",
             "records": records("syslog.fortigate", 10, 21)},
        ],
        "apps": [
            {"id": "nginx-sample", "name": "Storefront access log", "source": "nginx-access",
             "records": records("web.nginx.access", 12, 31)},
            {"id": "auth-sample", "name": "Bastion auth.log", "source": "linux-auth",
             "records": records("syslog.auth", 10, 41)},
            {"id": "cloudtrail-sample", "name": "CloudTrail management and data events", "source": "aws-cloudtrail",
             "records": records("aws.cloudtrail", 8, 51)},
        ],
    }


def waiting():
    """Samples of the feeds no route takes yet. A deploy leaves out a sample no test names, so these stay on the
    branch a visitor builds on, ready for the pipeline they write."""
    return {
        "network": [
            {"id": "panos-threat-sample", "name": "PAN-OS threats", "source": "panw-threats",
             "records": records("syslog.panos.threat", 10, 61)},
            {"id": "versa-sample", "name": "Versa flow, access, IDP and SLA logs", "source": "versa-sdwan",
             "records": records("syslog.versa", 12, 73)},
            {"id": "netscout-sample", "name": "Arbor Edge Defense events", "source": "netscout-aed",
             "records": records("syslog.netscout", 10, 81)},
            {"id": "snmp-sample", "name": "Router interface counters", "source": "router-snmp",
             "records": records("metrics.snmp.interface", 10, 91)},
        ],
        "apps": [
            {"id": "elastic-agent-linux-sample", "name": "Elastic Agent, Linux hosts", "source": "elastic-agent-linux",
             "records": records("elastic.agent.linux", 10, 101)},
            {"id": "elastic-agent-windows-sample", "name": "Elastic Agent, Windows hosts",
             "source": "elastic-agent-windows", "records": records("elastic.agent.windows", 10, 111)},
            {"id": "sentinel-sample", "name": "Sentinel tables from Event Hubs", "source": "sentinel-eventhub",
             "records": records("azure.eventhub.sentinel", 6, 121)},
        ],
    }
