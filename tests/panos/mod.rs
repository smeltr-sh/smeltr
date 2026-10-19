//! The reference workload: PAN-OS TRAFFIC syslog normalized to ECS.
//!
//! The record generator and the bundle it feeds. The two ends the runtime
//! runs it between are `tests/reference`'s. The workload is one file and
//! reproducible from `seed` alone.
//!
//! The pipeline is the shape real deployments run: a Grok pattern over the
//! syslog envelope, a positional CSV parse of the vendor's comma-separated
//! body, three enrichment lookups, and the field mapping that turns
//! vendor names into ECS ones.

#![allow(dead_code)]

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

/// splitmix64. A record is a pure function of its index, so a run is
/// reproducible and two runs of different lengths agree row for row.
fn mix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Applications, with the app table's categories keyed on the same list.
pub const APPS: &[&str] = &[
    "ssl",
    "web-browsing",
    "dns",
    "ms-office365",
    "ssh",
    "smtp",
    "ldap",
    "ntp",
    "google-base",
    "salesforce",
    "zoom",
    "github",
    "slack",
    "apt-get",
    "kerberos",
    "snmp",
];

/// Sites the CIDR table maps `10.<n>.0.0/16` onto.
pub const SITES: &[&str] = &["nyc", "lon", "fra", "sin", "syd", "sfo", "tor", "dub"];

/// How many `10.<n>.0.0/16` rows the CIDR table carries.
pub const CIDR_ROWS: usize = 64;
/// How many users the department table carries.
pub const USER_ROWS: usize = 1024;

/// One PAN-OS TRAFFIC record as it arrives over syslog: a BSD syslog
/// envelope wrapping the vendor's comma-separated body.
pub fn line(i: u64) -> String {
    let a = mix(i);
    let b = mix(i ^ 0x5DEE_CE66);
    let c = mix(i.wrapping_mul(31));

    let day = 1 + (a % 28) as u32;
    let (hh, mm, ss) = ((b % 24) as u32, (b >> 8) % 60, (b >> 16) % 60);
    let month = (a >> 8) as usize % 12;
    let syslog_time = format!("{} {:>2} {:02}:{:02}:{:02}", MONTHS[month], day, hh, mm, ss);
    let panos_time = format!("2026/{:02}/{:02} {:02}:{:02}:{:02}", month + 1, day, hh, mm, ss);

    let site = SITES[(a % SITES.len() as u64) as usize];
    let net = (a % CIDR_ROWS as u64) as u8;
    let src = format!("10.{}.{}.{}", net, (b >> 24) % 256, (b >> 32) % 254 + 1);
    let dst = format!("203.0.{}.{}", (c >> 8) % 256, (c >> 16) % 254 + 1);
    let nat = format!("198.51.100.{}", (c >> 24) % 254 + 1);
    let user = format!("acme\\user{:04}", a % USER_ROWS as u64);
    let app = APPS[(c % APPS.len() as u64) as usize];
    let allow = !i.is_multiple_of(11);
    let sent = 400 + (b % 60_000);
    let recv = 300 + (c % 900_000);

    // The empty fields are the vendor's own: PAN-OS writes a placeholder
    // column rather than omitting it, and a parser that mishandles those
    // shifts every column after them.
    format!(
        "<14>{syslog_time} fw-{site}-0{n} 1,{panos_time},01280100{serial:04},TRAFFIC,{subtype},2561,{panos_time},\
         {src},{dst},{nat},{dst},{rule},{user},,{app},vsys1,{szone},{dzone},ethernet1/{iif},ethernet1/{oif},\
         log-fwd-default,,{session},1,{sport},{dport},{natport},{dport},0x400053,{proto},{action},{total},{sent},\
         {recv},{packets},{panos_time},{elapsed},{category},,{sequence},0x0,10.0.0.0-10.255.255.255,{dloc},,\
         {psent},{precv},{reason},0,0,0,0,vsys1,fw-{site}-0{n},from-policy,,",
        n = 1 + (a >> 16) % 4,
        serial = a % 10_000,
        subtype = if allow { "end" } else { "deny" },
        rule = if allow { "allow-web-outbound" } else { "block-untrusted" },
        szone = if i.is_multiple_of(5) { "dmz" } else { "trust" },
        dzone = "untrust",
        iif = 1 + (b % 4),
        oif = 5 + (b % 4),
        session = 100_000 + (a % 900_000),
        sport = 32_768 + (b % 28_000),
        dport = if app == "dns" { 53 } else { 443 },
        natport = 40_000 + (c % 20_000),
        proto = if app == "dns" { "udp" } else { "tcp" },
        action = if allow { "allow" } else { "deny" },
        total = sent + recv,
        packets = 4 + (b % 400),
        elapsed = a % 120,
        category = "computer-and-internet-info",
        sequence = 7_000_000 + (c % 1_000_000),
        dloc = "United States",
        psent = 2 + (b % 200),
        precv = 2 + (c % 200),
        reason = if allow { "tcp-fin" } else { "policy-deny" },
    )
}

// -- the lookup tables --------------------------------------------------------

/// `10.<n>.0.0/16` to the site that owns it. A CIDR match, which is what
/// asset enrichment actually is.
fn cidr_table() -> serde_json::Value {
    let rows: Vec<Vec<String>> = (0..CIDR_ROWS)
        .map(|n| {
            vec![
                format!("10.{n}.0.0/16"),
                SITES[n % SITES.len()].to_string(),
                if n % 4 == 0 { "restricted" } else { "general" }.to_string(),
                format!("bu-{:02}", n % 12),
            ]
        })
        .collect();
    serde_json::json!({
        "id": "sites", "type": "inline",
        "headers": ["network", "site", "tier", "business_unit"],
        "rows": rows,
    })
}

/// Directory enrichment: the widest of the three tables and an exact
/// match, which is the common case.
fn user_table() -> serde_json::Value {
    let rows: Vec<Vec<String>> = (0..USER_ROWS)
        .map(|n| {
            vec![
                format!("acme\\user{n:04}"),
                format!("dept-{:02}", n % 40),
                format!("User {n:04}"),
            ]
        })
        .collect();
    serde_json::json!({
        "id": "users", "type": "inline",
        "headers": ["account", "department", "full_name"],
        "rows": rows,
    })
}

fn app_table() -> serde_json::Value {
    let rows: Vec<Vec<String>> = APPS
        .iter()
        .enumerate()
        .map(|(n, app)| {
            vec![
                app.to_string(),
                ["business-systems", "collaboration", "networking", "general-internet"][n % 4].to_string(),
                (1 + n % 5).to_string(),
            ]
        })
        .collect();
    serde_json::json!({
        "id": "apps", "type": "inline",
        "headers": ["name", "category", "risk"],
        "rows": rows,
    })
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
        "lookupTables": [cidr_table(), user_table(), app_table()],
        "pipelines": [{ "id": "p", "name": "panos-to-ecs", "steps": steps(stage) }],
        "routes": [{
            "id": "r", "source": "panw", "condition": null, "pipeline": "p",
            "deliver": [{"to": ["ecs"]}],
        }],
    })
    .to_string()
}
