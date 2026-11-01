//! The reference workload: PAN-OS TRAFFIC syslog normalized to ECS.
//!
//! The record generator and the bundle it feeds. The two ends the runtime
//! runs it between are `tests/reference`'s, and so are the company's people
//! and sites the records name (`reference::people`). A record is a pure
//! function of its index.
//!
//! The pipeline is the shape real deployments run: a Grok pattern over the
//! syslog envelope, a positional CSV parse of the vendor's comma-separated
//! body, three enrichment lookups, and the field mapping that turns
//! vendor names into ECS ones.

#![allow(dead_code)]

use crate::reference::{Clock, mix, people};

// -- the record ---------------------------------------------------------------

/// PAN-OS 10.1 TRAFFIC, in the order the firewall writes it. Positional:
/// the parser names columns by index, so this list and `line` move
/// together.
pub const HEADERS: &[&str] = &[
    "future_use1",
    "receive_time",
    "serial",
    "type",
    "subtype",
    "future_use2",
    "generated_time",
    "source_address",
    "destination_address",
    "nat_source_ip",
    "nat_destination_ip",
    "rule_name",
    "source_user",
    "destination_user",
    "application",
    "virtual_system",
    "source_zone",
    "destination_zone",
    "inbound_interface",
    "outbound_interface",
    "log_action",
    "future_use3",
    "session_id",
    "repeat_count",
    "source_port",
    "destination_port",
    "nat_source_port",
    "nat_destination_port",
    "flags",
    "protocol",
    "action",
    "bytes",
    "bytes_sent",
    "bytes_received",
    "packets",
    "start_time",
    "elapsed_time",
    "category",
    "future_use4",
    "sequence_number",
    "action_flags",
    "source_location",
    "destination_location",
    "future_use5",
    "packets_sent",
    "packets_received",
    "session_end_reason",
    "device_group_hierarchy_level_1",
    "device_group_hierarchy_level_2",
    "device_group_hierarchy_level_3",
    "device_group_hierarchy_level_4",
    "virtual_system_name",
    "device_name",
    "action_source",
    "source_vm_uuid",
    "destination_vm_uuid",
];

/// The columns the ECS mapping reads, typed `i64` where `NUMERIC` names
/// them and `str` otherwise. With `receive_time`, which lands typed on
/// `@timestamp`, that is 27 kept; the other 29 are typed `skip` in the
/// step's column table: named so the columns after them stay aligned, and
/// dropped at the parse, which is what a real bundle does.
pub const DECLARED: &[&str] = &[
    "serial",
    "type",
    "subtype",
    "source_address",
    "destination_address",
    "nat_source_ip",
    "rule_name",
    "source_user",
    "application",
    "source_zone",
    "destination_zone",
    "inbound_interface",
    "outbound_interface",
    "session_id",
    "source_port",
    "destination_port",
    "protocol",
    "action",
    "bytes_sent",
    "bytes_received",
    "packets_sent",
    "packets_received",
    "elapsed_time",
    "category",
    "session_end_reason",
    "device_name",
];

/// The declared columns that are numbers. PAN-OS leaves a column blank
/// rather than omitting it, and a blank under `i64` is null, not a failure,
/// so these are declared as the numbers they are and read without a cast.
pub const NUMERIC: &[&str] = &[
    "source_port",
    "destination_port",
    "bytes_sent",
    "bytes_received",
    "packets_sent",
    "packets_received",
    "elapsed_time",
];

/// App-ID: name, port, transport, category, subcategory, risk (1 to 5), and
/// its share of sessions.
pub const APPS: &[(&str, u16, &str, &str, &str, u8, u64)] = &[
    ("ssl", 443, "tcp", "networking", "encrypted-tunnel", 4, 18),
    ("web-browsing", 80, "tcp", "general-internet", "internet-utility", 4, 8),
    ("dns-base", 53, "udp", "networking", "infrastructure", 3, 14),
    (
        "ms-office365-base",
        443,
        "tcp",
        "business-systems",
        "office-programs",
        2,
        10,
    ),
    ("ms-teams", 443, "tcp", "collaboration", "instant-messaging", 2, 6),
    ("slack-base", 443, "tcp", "collaboration", "instant-messaging", 2, 5),
    ("zoom-base", 443, "tcp", "collaboration", "voip-video", 2, 4),
    (
        "github-base",
        443,
        "tcp",
        "business-systems",
        "software-development",
        2,
        4,
    ),
    ("salesforce-base", 443, "tcp", "business-systems", "crm", 2, 3),
    ("google-base", 443, "tcp", "general-internet", "internet-utility", 4, 6),
    ("ssh", 22, "tcp", "networking", "encrypted-tunnel", 4, 2),
    ("ntp-base", 123, "udp", "networking", "infrastructure", 2, 2),
    ("ldap", 389, "tcp", "business-systems", "auth-service", 3, 2),
    ("kerberos", 88, "tcp", "business-systems", "auth-service", 2, 2),
    ("smtp-base", 25, "tcp", "collaboration", "email", 5, 1),
    ("bittorrent", 6881, "tcp", "general-internet", "file-sharing", 5, 1),
    ("tor", 9001, "tcp", "networking", "encrypted-tunnel", 5, 1),
];

/// The App-IDs `block-high-risk-apps` denies.
pub const DENIED: &[&str] = &["bittorrent", "tor", "smtp-base"];

/// The App-ID a draw of `r` lands on, by each one's share.
fn app(r: u64) -> (&'static str, u16, &'static str, &'static str, &'static str, u8, u64) {
    let total: u64 = APPS.iter().map(|a| a.6).sum();
    let mut at = r % total;
    for a in APPS {
        if at < a.6 {
            return *a;
        }
        at -= a.6;
    }
    unreachable!("the shares add up to the total")
}

/// A draw of `r` from the exponential distribution with mean `mean`.
fn exponential(r: u64, mean: f64) -> f64 {
    let u = (r >> 11) as f64 / (1u64 << 53) as f64;
    -mean * (1.0 - u).ln()
}

/// One PAN-OS TRAFFIC record as it arrives over syslog: a BSD syslog
/// envelope wrapping the vendor's comma-separated body.
pub fn line(i: u64) -> String {
    record(i, &Clock::of(i))
}

/// Session `i`, logged at `at`, when it ended. A share of sessions comes from
/// the DMZ, with no user; the risky App-IDs are denied by rule.
pub fn record(i: u64, at: &Clock) -> String {
    let a = mix(i);
    let b = mix(i ^ 0x5DEE_CE66);
    let c = mix(i.wrapping_mul(31));

    let net = a % people::NETWORKS;
    let (site, _) = people::site_of(net);
    let device = format!("pa-{site}-0{}", 1 + (a >> 16) % 2);
    let (app, port, proto, category, _, _, _) = app(c);
    let dmz = b % 100 < 12;
    let src = if dmz {
        format!("172.16.{}.{}", 1 + (b >> 8) % 20, 2 + (b >> 16) % 248)
    } else {
        format!("10.{net}.{}.{}", (b >> 24) % 41, 2 + (b >> 32) % 248)
    };
    let (dst, country) = people::PUBLIC[(c >> 8) as usize % people::PUBLIC.len()];
    let nat = format!("198.51.100.{}", 2 + (c >> 24) % 29);
    let user = if dmz || (a >> 24).is_multiple_of(4) {
        String::new()
    } else {
        format!("acme\\{}", people::person(a >> 28).login)
    };
    let deny = DENIED.contains(&app) || (dmz && (b >> 40) % 100 < 15);
    let rule = match () {
        _ if DENIED.contains(&app) => "block-high-risk-apps",
        _ if deny => "dmz-deny-default",
        _ if dmz => "dmz-to-internet",
        _ if category == "networking" && app != "ssl" => "allow-infrastructure",
        _ => "allow-outbound-web",
    };
    let (sent, recv) = if deny {
        (0, 0)
    } else {
        (
            exponential(mix(b), 2_400.0) as u64 + 60,
            exponential(mix(c), 12_000.0) as u64 + 60,
        )
    };
    let (psent, precv) = if deny {
        (1, 0)
    } else {
        ((sent / 700).max(2), (recv / 1100).max(1))
    };
    let elapsed = if deny { 0 } else { exponential(mix(a), 25.0) as i64 };
    let reason = match () {
        _ if deny => "policy-deny",
        _ if proto == "udp" => "aged-out",
        _ if (c >> 40).is_multiple_of(4) => "tcp-rst-from-client",
        _ => "tcp-fin",
    };
    let panos = |t: &Clock| format!("{}/{:02}/{:02} {}", t.year, t.month, t.day, t.hms());
    let (now, start) = (panos(at), panos(&at.plus(-elapsed)));

    // The empty fields are the vendor's own: PAN-OS writes a placeholder
    // column rather than omitting it, and a parser that mishandles those
    // shifts every column after them.
    format!(
        "<14>{syslog} {device}.acme.example 1,{now},0132010{serial:05},TRAFFIC,{subtype},2562,{now},\
         {src},{dst},{nat},{dst},{rule},{user},,{app},vsys1,{szone},untrust,ethernet1/{iif},ethernet1/1,\
         log-fwd-siem,,{session},1,{sport},{port},{natport},{port},0x400053,{proto},{action},{total},{sent},\
         {recv},{packets},{start},{elapsed},{url_category},,{sequence},0x8000000000000000,{range},{country},,\
         {psent},{precv},{reason},0,0,0,0,vsys1,{device},from-policy,,",
        syslog = at.syslog(),
        serial = net * 2 + (a >> 16) % 2,
        subtype = if deny { "deny" } else { "end" },
        szone = if dmz { "dmz" } else { "trust" },
        iif = if dmz { 3 } else { 2 },
        session = 100_000 + (a >> 32) % 900_000,
        sport = 32_768 + (b >> 44) % 28_000,
        natport = 20_000 + (c >> 44) % 40_000,
        action = if deny { "deny" } else { "allow" },
        total = sent + recv,
        packets = psent + precv,
        url_category = if category == "business-systems" {
            "business-and-economy"
        } else {
            "computer-and-internet-info"
        },
        sequence = 7_000_000 + (c >> 20) % 1_000_000,
        range = if dmz {
            "172.16.0.0-172.31.255.255"
        } else {
            "10.0.0.0-10.255.255.255"
        },
    )
}

// -- the lookup tables --------------------------------------------------------

fn inline(id: &str, (headers, rows): (Vec<&str>, Vec<Vec<String>>)) -> serde_json::Value {
    serde_json::json!({"id": id, "type": "inline", "headers": headers, "rows": rows})
}

/// `10.<n>.0.0/16` to the site that owns it, and the DMZ. A CIDR match,
/// which is what asset enrichment actually is.
pub fn sites() -> (Vec<&'static str>, Vec<Vec<String>>) {
    let (headers, mut rows) = people::networks();
    rows.push(
        ["172.16.0.0/12", "dmz", "New York", "restricted", "bu-00"]
            .map(String::from)
            .to_vec(),
    );
    (headers, rows)
}

/// The App-ID catalogue's view of each application.
pub fn apps() -> (Vec<&'static str>, Vec<Vec<String>>) {
    let rows = APPS
        .iter()
        .map(|(name, _, _, category, subcategory, risk, _)| {
            vec![
                name.to_string(),
                category.to_string(),
                subcategory.to_string(),
                risk.to_string(),
            ]
        })
        .collect();
    (vec!["name", "category", "subcategory", "risk"], rows)
}

// -- the pipeline -------------------------------------------------------------

/// How much of the pipeline to run. Each stage is the previous one plus
/// its own work, so the profile's rows subtract to a per-stage cost.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Stage {
    /// No steps: decode, route, serialize `_raw`. The floor.
    Passthrough,
    /// The syslog envelope.
    Grok,
    /// The vendor's 56 comma-separated columns, 27 of them kept.
    Csv,
    /// Site, directory and application enrichment.
    Lookup,
    /// The ECS field mapping. The whole workload.
    Ecs,
}

impl Stage {
    pub const ALL: [Stage; 5] = [Stage::Passthrough, Stage::Grok, Stage::Csv, Stage::Lookup, Stage::Ecs];

    pub fn label(self) -> &'static str {
        match self {
            Stage::Passthrough => "passthrough",
            Stage::Grok => "+ grok",
            Stage::Csv => "+ csv",
            Stage::Lookup => "+ lookups",
            Stage::Ecs => "+ ecs map",
        }
    }
}

fn grok_step() -> serde_json::Value {
    serde_json::json!({
        "id": "syslog", "type": "parser", "input": "_raw",
        "kind": {
            "type": "grok",
            "patterns": ["<%{INT:pri}>%{SYSLOGTIMESTAMP:syslog_time} %{HOSTNAME:relay} %{GREEDYDATA:panos}"],
        },
        // Grok produces exactly its captures, so they are declared rather
        // than discovered.
        "declared": [
            {"name": "pri", "type": "str"},
            {"name": "syslog_time", "type": "str"},
            {"name": "relay", "type": "str"},
            {"name": "panos", "type": "str"},
        ],
    })
}

fn csv_step() -> serde_json::Value {
    // The column table: one row per column, left to right. The 27 the pipeline keeps are typed, the timestamp as the
    // datetime it is; the other 29 are `skip` -- named, so every column after them stays aligned, and dropped at the
    // parse.
    let columns: Vec<_> = HEADERS
        .iter()
        .map(|h| match *h {
            // `2026/05/10 21:07:15` is a shape auto-detect knows, so the column lands on its ECS name, typed, at
            // the parse.
            "receive_time" => serde_json::json!({"name": "@timestamp", "type": "datetime"}),
            _ if NUMERIC.contains(h) => serde_json::json!({"name": h, "type": "i64"}),
            _ if DECLARED.contains(h) => serde_json::json!({"name": h, "type": "str"}),
            _ => serde_json::json!({"name": h, "type": "skip"}),
        })
        .collect();
    serde_json::json!({
        "id": "fields", "type": "parser", "input": "panos",
        "kind": {"type": "csv", "delimiter": ","},
        "declared": columns,
    })
}

fn lookup_steps() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "id": "site", "type": "lookup", "input": "source_address", "table": "sites",
            "matchKind": "cidr", "multiplicity": "best", "keyColumn": "network",
            "outputs": [
                {"source": "site", "field": "source.site"},
                {"source": "tier", "field": "source.tier"},
                {"source": "business_unit", "field": "organization.id"},
            ],
        }),
        serde_json::json!({
            "id": "who", "type": "lookup", "input": "source_user", "table": "users",
            "matchKind": "exact", "multiplicity": "first", "keyColumn": "account",
            "outputs": [
                {"source": "department", "field": "user.department"},
                {"source": "full_name", "field": "user.full_name"},
            ],
        }),
        serde_json::json!({
            "id": "app", "type": "lookup", "input": "application", "table": "apps",
            "matchKind": "exact", "multiplicity": "first", "keyColumn": "name",
            "outputs": [
                {"source": "category", "field": "panw.app_category"},
                {"source": "risk", "field": "panw.app_risk", "type": "i64"},
            ],
        }),
    ]
}

/// The mapping itself: derived and typed fields by `eval`, straight
/// vendor-to-ECS moves by `rename`, and the intermediates nulled out —
/// every non-null field is serialized, so nulling is how a field leaves
/// the document.
fn ecs_steps() -> Vec<serde_json::Value> {
    let evals = [
        ("event.dataset", r#""panw.traffic""#),
        ("event.module", r#""panw""#),
        ("event.kind", r#""event""#),
        ("event.category", r#""network""#),
        ("event.action", "action.toLowerCase()"),
        ("event.outcome", r#"action == "allow" ? "success" : "failure""#),
        ("event.duration", "elapsed_time * 1000000000"),
        ("source.port", "source_port"),
        ("destination.port", "destination_port"),
        ("source.bytes", "bytes_sent"),
        ("destination.bytes", "bytes_received"),
        ("source.packets", "packets_sent"),
        ("destination.packets", "packets_received"),
        ("network.bytes", "bytes_sent + bytes_received"),
        ("network.packets", "packets_sent + packets_received"),
        (
            "network.direction",
            r#"source_zone == "trust" ? "outbound" : "inbound""#,
        ),
        ("user.name", r#"source_user.split("\\")[-1]"#),
    ];
    let renames = [
        ("source_address", "source.ip"),
        ("destination_address", "destination.ip"),
        ("nat_source_ip", "source.nat.ip"),
        ("protocol", "network.transport"),
        ("application", "network.protocol"),
        ("rule_name", "rule.name"),
        ("device_name", "observer.name"),
        ("serial", "observer.serial_number"),
        ("source_zone", "observer.ingress.zone"),
        ("destination_zone", "observer.egress.zone"),
        ("inbound_interface", "observer.ingress.interface.name"),
        ("outbound_interface", "observer.egress.interface.name"),
        ("session_id", "panw.session_id"),
        ("category", "panw.category"),
        ("session_end_reason", "panw.session_end_reason"),
        ("subtype", "panw.subtype"),
    ];
    // Read by the evals above and not carried into the document.
    let dropped = [
        "_raw",
        "panos",
        "pri",
        "syslog_time",
        "relay",
        "type",
        "action",
        "elapsed_time",
        "source_port",
        "destination_port",
        "bytes_sent",
        "bytes_received",
        "packets_sent",
        "packets_received",
        "source_user",
    ];
    vec![
        serde_json::json!({
            "id": "derive", "type": "eval",
            "assignments": evals.iter().map(|(t, e)| serde_json::json!({"target": t, "expr": e})).collect::<Vec<_>>(),
        }),
        serde_json::json!({
            "id": "to-ecs", "type": "rename",
            "pairs": renames.iter().map(|(f, t)| serde_json::json!({"from": f, "to": t})).collect::<Vec<_>>(),
        }),
        serde_json::json!({
            "id": "prune", "type": "eval",
            "assignments": dropped.iter().map(|n| serde_json::json!({"target": n, "expr": "null"})).collect::<Vec<_>>(),
        }),
    ]
}

fn steps(stage: Stage) -> Vec<serde_json::Value> {
    let mut v = Vec::new();
    if stage >= Stage::Grok {
        v.push(grok_step());
    }
    if stage >= Stage::Csv {
        v.push(csv_step());
    }
    if stage >= Stage::Lookup {
        v.extend(lookup_steps());
    }
    if stage >= Stage::Ecs {
        v.extend(ecs_steps());
    }
    v
}

/// The bundle at `stage`. The source and destination are declared so the
/// compiler sees a real topology; the profile substitutes `Generator` and
/// `Counter` for them, so nothing here opens a socket.
pub fn bundle(stage: Stage) -> String {
    serde_json::json!({
        "version": 1,
        "sources": [{
            "id": "panw", "type": "kafka", "brokers": "127.0.0.1:1",
            "topics": ["panw-traffic"], "groupId": "panos-profile", "encoding": "utf-8",
        }],
        "destinations": [{
            "id": "ecs", "type": "kafka", "brokers": "127.0.0.1:1", "defaultTopic": "ecs-traffic",
        }],
        "lookupTables": [inline("sites", sites()), inline("users", people::directory()), inline("apps", apps())],
        "pipelines": [{ "id": "p", "name": "panos-to-ecs", "steps": steps(stage) }],
        "routes": [{
            "id": "r", "source": "panw", "condition": null, "pipeline": "p",
            "deliver": [{"to": ["ecs"]}],
        }],
    })
    .to_string()
}
