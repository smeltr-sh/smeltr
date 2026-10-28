# Example pipelines

Seven common log types, each parsed and normalized to ECS, and one telemetry stream given its column names, in the language
every step lowers to ([Compiler](architecture/compiler.md#every-step-is-code)). `$ = {...}` builds the document,
`try`/`catch` handles the other case, `switch` branches, and `rest($)` is
the record's undeclared keys as an object. The GUI's step forms lower to
this code, and the compiler compiles it whole.

The shape every pipeline has:

```text
  declarations     name: type;                    a key the document reads, typed before the parse
  parse            Object.assign($, json(_raw))   a function of the text, merged into the record
  enrich           g = lookup(...);               a function of a key, an object of the columns `fields` names
  document         $ = { ... };                   the record becomes the literal
```

`_raw` is the source's payload, decoded as text. An undeclared key reads as it arrived and
can be copied into the document; a key the document compares, computes
with or puts in a template is declared, which types it. A declared key is
a field, so it is not in `rest($)`, the undeclared keys as one object. A parse that cannot
read a row is an exception on it; an exception no `catch` took goes to the
`exceptions` answer (ignore, tag, drop, deliver or block) at the end of
the row, so a bad row is answered without a line here.

## 1. JSON application log

In:

```json
{"ts": "2026-10-01T00:12:03.441Z", "level": "warn", "logger": "checkout", "msg": "retrying payment", "attempt": 2, "order_id": "o-7781", "user": {"id": "u-19", "tier": "gold"}, "latency_ms": 812.5}
```

```ts
ts: datetime; level: str; attempt: i64; latency_ms: f64;
Object.assign($, json(_raw))
if (level == "debug") drop();
$ = {
  "@timestamp": ts,
  log: {level, logger: $.logger},
  message: $.msg,
  event: {duration: (latency_ms * 1000000) as i64},
  user: {id: $.user.id},
  labels: {order_id: $.order_id, attempt, tier: $.user.tier}
};
```

Out:

```json
{"@timestamp": "2026-10-01T00:12:03.441000Z", "log": {"level": "warn", "logger": "checkout"}, "message": "retrying payment", "event": {"duration": 812500000}, "user": {"id": "u-19"}, "labels": {"order_id": "o-7781", "attempt": 2, "tier": "gold"}}
```

The four declared keys are typed at the parse: `level` is compared, the
others change type. `$.msg`, `$.logger` and `$.user.id` are copied as
they arrived. Everything the literal does not name is gone, `_raw` included.

## 2. Kubernetes container log, JSON inside JSON

Fluent Bit wraps the app's line. Most lines are JSON; a stack trace is not.

In:

```json
{"time": "2026-10-01T00:12:03Z", "stream": "stdout", "log": "{\"level\":\"error\",\"msg\":\"db timeout\",\"trace_id\":\"abc123\"}", "kubernetes": {"namespace_name": "shop", "pod_name": "checkout-7d9f", "container_name": "api"}}
{"time": "2026-10-01T00:12:04Z", "stream": "stderr", "log": "goroutine 12 [running]:", "kubernetes": {"namespace_name": "shop", "pod_name": "checkout-7d9f", "container_name": "api"}}
```

```ts
time: datetime; log: str;
Object.assign($, json(_raw))
message: str = null;
try {
  app: {msg: str, ...} = json(log);
  message = app.msg;
} catch {
  message = log;
}
let k = $.kubernetes;
$ = {
  "@timestamp": time,
  message, stream: $.stream,
  log: {level: app.level},
  trace: {id: app.trace_id},
  orchestrator: {namespace: k.namespace_name, pod: k.pod_name, container: k.container_name}
};
```

Out:

```json
{"@timestamp": "2026-10-01T00:12:03.000000Z", "message": "db timeout", "stream": "stdout", "log": {"level": "error"}, "trace": {"id": "abc123"}, "orchestrator": {"namespace": "shop", "pod": "checkout-7d9f", "container": "api"}}
{"@timestamp": "2026-10-01T00:12:04.000000Z", "message": "goroutine 12 [running]:", "stream": "stderr", "orchestrator": {"namespace": "shop", "pod": "checkout-7d9f", "container": "api"}}
```

The second row's `json(log)` is an exception inside the `try`; the `catch`
clears it and keeps the text as the message, so the row carries no exception. `app` is null
there, so `log` and `trace` hold only nulls, and the serializer leaves an
object with nothing in it out.

## 3. Key-value syslog (FortiGate)

In:

```text
date=2026-10-01 time=00:12:03 devname="fg-1" vd="root" type="traffic" subtype="forward" sessionid=1842 srcip=10.1.1.5 srcport=51000 dstip=93.184.216.34 dstport=443 proto=6 action="accept" policyid=12 app="HTTPS" sentbyte=1200 rcvdbyte=8800 duration=30 level="notice"
```

```ts
date: str; time: str; devname: str; type: str; srcip: str; dstip: str; srcport: i64; dstport: i64;
proto: str; action: str; policyid: str; app: str; sentbyte: i64; rcvdbyte: i64; duration: i64; level: str;
Object.assign($, kv(_raw))
$ = {
  "@timestamp": parseTime(`${date} ${time}`),
  event: {dataset: `fortinet.${type}`, action, duration: duration * 1000000000, severity: level},
  source: {ip: srcip, port: srcport, bytes: sentbyte},
  destination: {ip: dstip, port: dstport, bytes: rcvdbyte},
  network: {transport: {"6": "tcp", "17": "udp", "1": "icmp"}[proto] ?? proto, application: app},
  observer: {name: devname, vendor: "Fortinet", product: "FortiGate"},
  rule: {id: policyid},
  fortinet: rest($)
};
```

Out:

```json
{"@timestamp": "2026-10-01T00:12:03.000000Z", "event": {"dataset": "fortinet.traffic", "action": "accept", "duration": 30000000000, "severity": "notice"}, "source": {"ip": "10.1.1.5", "port": 51000, "bytes": 1200}, "destination": {"ip": "93.184.216.34", "port": 443, "bytes": 8800}, "network": {"transport": "tcp", "application": "HTTPS"}, "observer": {"name": "fg-1", "vendor": "Fortinet", "product": "FortiGate"}, "rule": {"id": "12"}, "fortinet": {"vd": "root", "subtype": "forward", "sessionid": "1842"}}
```

Every key the document moves is declared, even the ones that stay text,
so `rest($)` is exactly what it did not move: `vd`, `subtype`, `sessionid`, and any key
the firewall adds next release.

## 4. Positional CSV in a syslog envelope (PAN-OS TRAFFIC to ECS)

The reference workload: a Grok envelope, 56 positional columns of which
27 are kept, three lookups. In: `tests/panos/mod.rs::line`; the column
list below is its `HEADERS`.

```ts
Object.assign($, grok(_raw, ["<%{INT:pri}>%{SYSLOGTIMESTAMP:syslog_time} %{HOSTNAME:relay} %{GREEDYDATA:panos}"]))

receive_time: datetime;
source_port: i64; destination_port: i64; bytes_sent: i64; bytes_received: i64;
packets_sent: i64; packets_received: i64; elapsed_time: i64;
Object.assign($, csv(panos, ["future_use1", "receive_time", "serial", "type", "subtype", "future_use2",
  "generated_time", "source_address", "destination_address", "nat_source_ip", "nat_destination_ip",
  "rule_name", "source_user", "destination_user", "application", "virtual_system", "source_zone",
  "destination_zone", "inbound_interface", "outbound_interface", "log_action", "future_use3",
  "session_id", "repeat_count", "source_port", "destination_port", "nat_source_port",
  "nat_destination_port", "flags", "protocol", "action", "bytes", "bytes_sent", "bytes_received",
  "packets", "start_time", "elapsed_time", "category", "future_use4", "sequence_number",
  "action_flags", "source_location", "destination_location", "future_use5", "packets_sent",
  "packets_received", "session_end_reason", "device_group_hierarchy_level_1",
  "device_group_hierarchy_level_2", "device_group_hierarchy_level_3",
  "device_group_hierarchy_level_4", "virtual_system_name", "device_name", "action_source",
  "source_vm_uuid", "destination_vm_uuid"]))

site = lookup("sites", source_address, {match: "cidr", column: "network",
  fields: {site: "site", tier: "tier", business_unit: "business_unit"}});
who = lookup("users", source_user, {column: "account", first: true,
  fields: {department: "department", full_name: "full_name"}});
app: {category: str, risk: i64} = lookup("apps", application, {column: "name", first: true,
  fields: {category: "category", risk: "risk"}});

$ = {
  "@timestamp": receive_time,
  event: {
    dataset: "panw.traffic", module: "panw", kind: "event", category: "network",
    action: action.toLowerCase(),
    outcome: action == "allow" ? "success" : "failure",
    duration: elapsed_time * 1000000000
  },
  source: {
    ip: source_address, port: source_port, bytes: bytes_sent, packets: packets_sent,
    nat: {ip: test(nat_source_ip, /^$/) ? null : nat_source_ip},
    site: site.site, tier: site.tier
  },
  destination: {ip: destination_address, port: destination_port, bytes: bytes_received, packets: packets_received},
  network: {
    bytes: bytes_sent + bytes_received, packets: packets_sent + packets_received,
    transport: protocol, protocol: application,
    direction: source_zone == "trust" ? "outbound" : "inbound"
  },
  user: {name: source_user.split("\\")[-1], department: who.department, full_name: who.full_name},
  organization: {id: site.business_unit},
  rule: {name: rule_name},
  observer: {
    name: device_name, serial_number: serial,
    ingress: {zone: source_zone, interface: {name: inbound_interface}},
    egress: {zone: destination_zone, interface: {name: outbound_interface}}
  },
  panw: {session_id, category, session_end_reason, subtype,
         app_category: app.category, app_risk: app.risk}
};
```

Out, for `line(0)`:

```json
{"@timestamp": "2026-10-24T03:26:30.000000Z", "event": {"dataset": "panw.traffic", "module": "panw", "kind": "event", "category": "network", "action": "deny", "outcome": "failure", "duration": 55000000000}, "source": {"ip": "10.47.238.179", "port": 50803, "bytes": 46435, "packets": 37, "nat": {"ip": "198.51.100.210"}, "site": "dub", "tier": "general"}, "destination": {"ip": "203.0.205.194", "port": 443, "bytes": 507835, "packets": 137}, "network": {"bytes": 554270, "packets": 174, "transport": "tcp", "protocol": "snmp", "direction": "inbound"}, "user": {"name": "user0431", "department": "dept-31", "full_name": "User 0431"}, "organization": {"id": "bu-11"}, "rule": {"name": "block-untrusted"}, "observer": {"name": "fw-dub-02", "serial_number": "012801007535", "ingress": {"zone": "dmz", "interface": {"name": "ethernet1/4"}}, "egress": {"zone": "untrust", "interface": {"name": "ethernet1/8"}}}, "panw": {"session_id": "607535", "category": "computer-and-internet-info", "session_end_reason": "policy-deny", "subtype": "deny", "app_category": "general-internet", "app_risk": 1}}
```

The 29 columns the document never reads are typed `str` by the parse and
dropped by the `$ =`. A lookup's `fields` names its outputs; the declaration on
`app` types `risk`. A lookup miss is null in the document, not an exception.

## 5. Web server access log, Grok (nginx)

In:

```text
203.0.113.9 - alice [01/Oct/2026:00:12:03 +0000] "GET /cart?id=7 HTTP/1.1" 200 5120 "https://shop.example/" "Mozilla/5.0"
```

```ts
ts: datetime; status: i64; bytes: i64;
Object.assign($, grok(_raw, ['%{IP:client} - %{DATA:user} \\[%{HTTPDATE:ts}\\] "%{WORD:method} %{NOTSPACE:path} HTTP/%{NUMBER:version}" %{INT:status} %{INT:bytes} "%{DATA:referrer}" "%{DATA:agent}"'],
  {formats: {ts: "%d/%b/%Y:%H:%M:%S %z"}}))
geo: {country_iso_code: str, city_name: str, lat: f64, lon: f64} = lookup("geoip", client, {match: "cidr",
  fields: {country_iso_code: "country.iso_code", city_name: "city.names.en", lat: "location.latitude", lon: "location.longitude"}});
$ = {
  "@timestamp": ts,
  http: {
    request: {method, referrer: test(referrer, /^-$/) ? null : referrer},
    response: {status_code: status, body: {bytes}},
    version
  },
  url: {path: path.split("?")[0], query: path.split("?")[1]},
  user: {name: test(user, /^-$/) ? null : user},
  user_agent: {original: agent},
  source: {ip: client, geo: {country_iso_code: geo.country_iso_code, city_name: geo.city_name,
                             location: {lat: geo.lat, lon: geo.lon}}},
  event: {category: "web", outcome: status >= 400 ? "failure" : "success"}
};
```

Out:

```json
{"@timestamp": "2026-10-01T00:12:03.000000Z", "http": {"request": {"method": "GET", "referrer": "https://shop.example/"}, "response": {"status_code": 200, "body": {"bytes": 5120}}, "version": "1.1"}, "url": {"path": "/cart", "query": "id=7"}, "user": {"name": "alice"}, "user_agent": {"original": "Mozilla/5.0"}, "source": {"ip": "203.0.113.9", "geo": {"country_iso_code": "US", "city_name": "Los Angeles", "location": {"lat": 34.05, "lon": -118.24}}}, "event": {"category": "web", "outcome": "success"}}
```

Grok captures are text; `status: i64; bytes: i64;` before the parse types the
two that are numbers, and `ts: datetime;` with the call's `formats` reads the
time in nginx's shape. `path.split("?")[1]` is null where there is no query,
since an index past the end reads null. `geoip` is an MMDB table; each
output in `fields` names a path in the database's tree, and the declaration on
`geo` types what it reads.

## 6. An array of events (AWS CloudTrail)

One record carries many events. `unroll` makes one row per element.

In:

```json
{"Records": [
  {"eventVersion": "1.08", "eventTime": "2026-10-01T00:12:03Z", "eventSource": "sts.amazonaws.com", "eventName": "AssumeRole", "awsRegion": "us-east-1", "sourceIPAddress": "203.0.113.9", "userIdentity": {"type": "IAMUser", "arn": "arn:aws:iam::123456789012:user/alice"}, "requestParameters": {"roleArn": "arn:aws:iam::123456789012:role/deploy"}, "recipientAccountId": "123456789012"},
  {"eventVersion": "1.08", "eventTime": "2026-10-01T00:12:09Z", "eventSource": "s3.amazonaws.com", "eventName": "PutObject", "awsRegion": "us-east-1", "sourceIPAddress": "203.0.113.9", "userIdentity": {"type": "AssumedRole", "arn": "arn:aws:sts::123456789012:assumed-role/deploy/alice"}, "errorCode": "AccessDenied", "recipientAccountId": "123456789012"}
]}
```

```ts
Object.assign($, json(_raw))
records: array<{eventTime: datetime, eventName: str, eventSource: str, awsRegion: str,
                sourceIPAddress: str, recipientAccountId: str, errorCode: str,
                userIdentity: {type: str, arn: str, ...}, ...}> = Records;
unroll(records, false);
$ = records;
let extra = rest($);
$ = {
  "@timestamp": eventTime,
  event: {action: eventName, provider: eventSource, outcome: errorCode == null ? "success" : "failure"},
  user: {id: userIdentity.arn, type: userIdentity.type},
  source: {ip: sourceIPAddress},
  cloud: {provider: "aws", region: awsRegion, account: {id: recipientAccountId}},
  aws: {cloudtrail: {error_code: errorCode, ...extra}}
};
```

Out:

```json
{"@timestamp": "2026-10-01T00:12:03.000000Z", "event": {"action": "AssumeRole", "provider": "sts.amazonaws.com", "outcome": "success"}, "user": {"id": "arn:aws:iam::123456789012:user/alice", "type": "IAMUser"}, "source": {"ip": "203.0.113.9"}, "cloud": {"provider": "aws", "region": "us-east-1", "account": {"id": "123456789012"}}, "aws": {"cloudtrail": {"eventVersion": "1.08", "requestParameters": {"roleArn": "arn:aws:iam::123456789012:role/deploy"}}}}
{"@timestamp": "2026-10-01T00:12:09.000000Z", "event": {"action": "PutObject", "provider": "s3.amazonaws.com", "outcome": "failure"}, "user": {"id": "arn:aws:sts::123456789012:assumed-role/deploy/alice", "type": "AssumedRole"}, "source": {"ip": "203.0.113.9"}, "cloud": {"provider": "aws", "region": "us-east-1", "account": {"id": "123456789012"}}, "aws": {"cloudtrail": {"error_code": "AccessDenied", "eventVersion": "1.08"}}}
```

The typed assignment on `Records` declares the element's fields once, for
every element; `$ = records` makes the element the record, so the rest
reads as any JSON pipeline does. The request and response objects are
whatever the service sent and pass through in `extra`, the record's
undeclared keys, named before the literal because a spread takes a name.

## 7. Many message shapes in one stream (Cisco ASA)

Each message id has its own text. First matching pattern wins; a capture
the matched pattern lacks is null.

In:

```text
<166>Oct 01 2026 00:12:03 fw1 : %ASA-6-302013: Built outbound TCP connection 1234 for outside:93.184.216.34/443 (93.184.216.34/443) to inside:10.1.1.5/51000 (198.51.100.7/51000)
<166>Oct 01 2026 00:12:04 fw1 : %ASA-4-106023: Deny tcp src inside:10.1.1.9/40001 dst outside:203.0.113.5/22 by access-group "inside_in" [0x0, 0x0]
```

```ts
timestamp: datetime; severity: i64; src_port: i64; dst_port: i64;
Object.assign($, grok(_raw, [
  '<%{INT:pri}>%{DATA:timestamp} %{HOSTNAME:host} : %ASA-%{INT:severity}-%{INT:code}: %{WORD:verb} %{WORD:direction} %{WORD:protocol} connection %{INT:connection_id} for %{WORD}:%{IP:src_ip}/%{INT:src_port} \\(%{IP:src_xlated_ip}/%{INT}\\) to %{WORD}:%{IP:dst_ip}/%{INT:dst_port}',
  '<%{INT:pri}>%{DATA:timestamp} %{HOSTNAME:host} : %ASA-%{INT:severity}-%{INT:code}: Deny %{WORD:protocol} src %{WORD}:%{IP:src_ip}/%{INT:src_port} dst %{WORD}:%{IP:dst_ip}/%{INT:dst_port} by access-group "%{DATA:acl}"'
], {formats: {timestamp: "%b %d %Y %H:%M:%S"}}))
event = {code, severity};
switch (code) {
  case "302013": event.action = "connection-started"; break;
  case "302014": event.action = "connection-finished"; break;
  case "106023": event.action = "denied"; event.outcome = "failure"; break;
  default: event.action = "other";
}
$ = {
  "@timestamp": timestamp,
  event,
  source: {ip: src_ip, port: src_port, nat: {ip: src_xlated_ip}},
  destination: {ip: dst_ip, port: dst_port},
  network: {transport: protocol.toLowerCase()},
  rule: {name: acl},
  observer: {name: host}
};
```

Out:

```json
{"@timestamp": "2026-10-01T00:12:03.000000Z", "event": {"code": "302013", "severity": 6, "action": "connection-started"}, "source": {"ip": "93.184.216.34", "port": 443, "nat": {"ip": "93.184.216.34"}}, "destination": {"ip": "10.1.1.5", "port": 51000}, "network": {"transport": "tcp"}, "observer": {"name": "fw1"}}
{"@timestamp": "2026-10-01T00:12:04.000000Z", "event": {"code": "106023", "severity": 4, "action": "denied", "outcome": "failure"}, "source": {"ip": "10.1.1.9", "port": 40001}, "destination": {"ip": "203.0.113.5", "port": 22}, "network": {"transport": "tcp"}, "rule": {"name": "inside_in"}, "observer": {"name": "fw1"}}
```

A capture the matched pattern lacks is null (`acl` on the first row,
`src_xlated_ip` on the second), and the serializer leaves out an object
with nothing in it, so `rule` and `source.nat` appear only where the
pattern captured them.

## 8. Rows and their column names in one answer (Starlink telemetry)

Starlink's telemetry stream answers a POST with rows of values, and with the
column names of each device type in the same answer. The source is an
`httpApi` with `method` `POST` and the body `{"batchSize": 1000,
"maxLingerMs": 15000}`, and the answer is one record. `unroll` makes one row
per row of values, and a local carries the column names past it.

In:

```json
{"data": {
  "columnNamesByDeviceType": {
    "u": ["DeviceType", "UtcTimestampNs", "DeviceId", "PingLatencyMsAvg", "PingDropRateAvg"],
    "i": ["DeviceType", "UtcTimestampNs", "DeviceId", "Ipv4"]
  },
  "values": [
    ["u", 1681879752000000001, "ut1", 47, 0.000041],
    ["u", 1681879752000000000, "ut2", 48, 0],
    ["i", 1681879752000000000, "ut1", ["1.2.3.4"]]
  ]},
 "metadata": {}}
```

```ts
Object.assign($, json(_raw));
values: array<array<variant>> = data.values;
let cols: {...} = data.columnNamesByDeviceType;
unroll(values, false);
let t: str = values[0];
let names: array<str> = cols[t];
pairs: array<{key: str, value: variant}> = [];
let i = 0;
for (let v of values) { pairs = push(pairs, {key: names[i], value: v}); i = i + 1; }
r: {...} = fromEntries(pairs);
$ = r;
```

Out:

```json
{"DeviceId": "ut1", "DeviceType": "u", "PingDropRateAvg": 0.000041, "PingLatencyMsAvg": 47, "UtcTimestampNs": 1681879752000000001}
{"DeviceId": "ut2", "DeviceType": "u", "PingDropRateAvg": 0, "PingLatencyMsAvg": 48, "UtcTimestampNs": 1681879752000000000}
{"DeviceId": "ut1", "DeviceType": "i", "Ipv4": ["1.2.3.4"], "UtcTimestampNs": 1681879752000000000}
```

Each value keeps the type it arrived with, and a nanosecond time is an `i64`,
whole. Starlink moves its read position when it answers, so a batch the
source holds when its worker stops is not asked for again: a smaller
`batchSize` loses less.

## 9. A schema from a file (the ECS importer)

A schema is a table of fields, and a pipeline makes rows, so a schema is
imported by a pipeline: Add schema, Import, pick the pipeline and a file, and
each row it writes is a field, with `field`, `type`, and `required` and
`description` where it has them ([Config plane](architecture/config-plane.md#schemas)).
The file is cut into records as a source cuts its bytes: a record per line,
or where a line breaker breaks it. ECS's `generated/csv/fields.csv` is one row
per field, and some of its quoted examples span lines, so its line breaker
breaks where a row starts with its ECS version: `([\r\n]+)\d+\.\d+\.\d+`. A
Drop step takes out its header. In: `tests/gui/fixtures/ecs-fields-excerpt.csv`.

```ts
Object.assign($, csv(_raw, [null, null, null, "field", "ecs", null, "normalization", null, "description"]))
type = ecs == "date" ? "datetime"
  : ecs == "long" ? "i64"
  : ecs == "float" || ecs == "scaled_float" || ecs == "double" ? "f64"
  : ecs == "boolean" ? "bool"
  : ecs == "object" || ecs == "geo_point" ? "{...}"
  : ecs == "nested" ? "array<{...}>"
  : ecs == "flattened" ? "variant"
  : "str";
if (normalization == "array") { type = "array<" + type + ">"; }
required = field == "@timestamp";
delete(_raw, ecs, normalization);
```

Out, the schema's first rows:

```json
{"field": "@timestamp", "type": "datetime", "required": true, "description": "Date/time when the event originated."}
{"field": "labels", "type": "{...}", "description": "Custom key/value pairs."}
{"field": "tags", "type": "array<str>", "description": "List of keywords used to tag each event."}
```

A keyword, a text, an IP and a wildcard are each `str`, since that is what a
pipeline writes for them; an object a pipeline fills key by key is `{...}`.

