//! Cisco ASA syslog to ECS: many message shapes in one stream.
//!
//! One Grok reads the envelope every ASA message shares and takes the
//! message id. A branch per handled id parses that id's text with its own
//! Grok; an id no branch handles keeps its text as `message` and still
//! becomes a document. Two regex lookups map the interface names to the
//! site's policy zones, then the mapping moves vendor names to ECS ones.

use super::Pipeline;

pub const PIPELINE: Pipeline = Pipeline {
    name: "asa",
    line,
    bundle,
    check,
};

// -- the record ---------------------------------------------------------------

/// splitmix64. A record is a pure function of its index, so a run is
/// reproducible and two runs of different lengths agree row for row.
fn mix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn pick<'a>(pool: &[&'a str], r: u64) -> &'a str {
    pool[(r % pool.len() as u64) as usize]
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// The message ids, each with its share of the stream in thousandths. An
/// ASA logging at informational is mostly connection build and teardown;
/// denies, NAT and VPN follow; the ids no branch handles are the long tail
/// a real box also writes.
const MIX: &[(&str, u64)] = &[
    ("302013", 180),
    ("302014", 175),
    ("302015", 120),
    ("302016", 115),
    ("305011", 60),
    ("305012", 55),
    ("106023", 100),
    ("710003", 30),
    ("113019", 10),
    ("733100", 10),
    // Unhandled.
    ("106100", 30),
    ("302020", 20),
    ("302021", 20),
    ("609001", 15),
    ("609002", 15),
    ("725001", 10),
    ("725002", 10),
    ("111008", 5),
    ("113004", 10),
    ("605005", 5),
    ("106015", 5),
];

/// The ids the pipeline has a branch for.
const HANDLED: &[&str] = &[
    "302013", "302014", "302015", "302016", "305011", "305012", "106023", "710003", "113019", "733100",
];

/// The facility every message is sent on, `local4`, as most ASA configs do.
const FACILITY: u64 = 20;

/// Interfaces on the low-security side of a connection: what traffic comes
/// in from. `outside` dominates, as it does on a perimeter box.
const LOW_IFACES: &[&str] = &[
    "outside",
    "outside",
    "outside",
    "outside",
    "outside",
    "outside-isp2",
    "outside-isp3",
    "wan-mpls1",
    "wan-sdwan2",
    "partner-acme",
    "partner-globex",
    "partner-initech",
    "guest",
    "guest-wifi",
    "anyconnect-staff",
    "anyconnect-contractors",
    "vpn-pool",
    "cloud-aws",
    "cloud-azure",
    "dmz",
    "legacy-x25",
];

/// Interfaces on the high-security side: users, servers and the DMZ. Most
/// are named by a site convention a regex captures in one row; a few are
/// names no row was written for, as on any real box.
const HIGH_IFACES: &[&str] = &[
    "inside",
    "inside",
    "inside",
    "inside",
    "inside",
    "inside",
    "dmz",
    "dmz-web",
    "dmz-web2",
    "dmz-mail",
    "dmz-dns",
    "dmz-api-prod",
    "dmz-api-stage",
    "mgmt",
    "oob-mgmt",
    "vlan110-finance",
    "vlan112-finance",
    "vlan210-hr",
    "vlan310-eng",
    "vlan315-eng",
    "vlan410-sales",
    "vlan510-support",
    "pci-cde",
    "pci-dmz",
    "storage-iscsi",
    "storage-nfs",
    "voip",
    "voip-ucm",
    "iot-cameras",
    "iot-badges",
    "lab",
    "lab2",
    "dc1-core",
    "dc2-core",
    "branch-nyc01",
    "branch-lon02",
    "backup",
    "printers",
    "scada-plant1",
    "wireless-corp",
    "dev-k8s",
    "prod-k8s",
    "db-oracle",
    "db-sql",
    "citrix",
    "temp-migration",
    "test",
];

/// The access groups a deny names.
const ACLS: &[&str] = &[
    "outside_access_in",
    "outside_access_in",
    "inside_access_in",
    "dmz_access_in",
    "guest_access_in",
    "partner_access_in",
    "pci_cde_in",
    "mgmt_access_in",
    "vpn_filter_staff",
];

const USERS: &[&str] = &[
    "jdoe",
    "asmith",
    "mgarcia",
    "lchen",
    "pkumar",
    "omar.h",
    "svc-backup",
    "kwilliams",
    "tnguyen",
    "rjohnson",
];

const VPN_GROUPS: &[&str] = &[
    "AnyConnect-Staff",
    "AnyConnect-Contractors",
    "Site2Site-Partner",
    "DefaultWEBVPNGroup",
];

const VPN_REASONS: &[&str] = &[
    "User Requested",
    "Idle Timeout",
    "Max time exceeded",
    "Lost Service",
    "Port Preempted",
];

const TEARDOWN_REASONS: &[&str] = &[
    "TCP FINs",
    "TCP FINs",
    "TCP FINs",
    "TCP Reset-I",
    "TCP Reset-O",
    "Conn-timeout",
    "SYN Timeout",
    "Flow closed by inspection",
];

const THREATS: &[&str] = &[
    "Scanning",
    "Syn Attck",
    "Firewall",
    "Interface",
    "ACL drop",
    "Bad pkts",
    "Conn limit",
];

/// Well-known server ports, weighted the way a perimeter sees them.
const TCP_PORTS: &[u16] = &[
    443, 443, 443, 443, 80, 80, 22, 25, 587, 993, 3389, 8080, 8443, 445, 1433, 3306,
];
const UDP_PORTS: &[u16] = &[53, 53, 53, 53, 123, 161, 514, 500, 4500, 443, 5060, 1812];

/// The message id the random draw `r` picks, by `MIX`'s weights.
fn code(r: u64) -> &'static str {
    let mut at = r % 1000;
    for (c, w) in MIX {
        if at < *w {
            return c;
        }
        at -= w;
    }
    unreachable!("MIX sums to 1000")
}

fn severity(code: &str) -> u64 {
    match code {
        "710003" => 3,
        "106023" | "113019" | "733100" => 4,
        "111008" => 5,
        "609001" | "609002" => 7,
        _ => 6,
    }
}

fn private_ip(r: u64) -> String {
    format!("10.{}.{}.{}", (r >> 8) % 64, (r >> 16) % 256, (r >> 24) % 254 + 1)
}

fn public_ip(r: u64) -> String {
    match r % 4 {
        0 => format!("93.184.{}.{}", (r >> 8) % 256, (r >> 16) % 254 + 1),
        1 => format!("203.0.113.{}", (r >> 8) % 254 + 1),
        2 => format!("52.{}.{}.{}", (r >> 8) % 256, (r >> 16) % 256, (r >> 24) % 254 + 1),
        _ => format!("151.101.{}.{}", (r >> 8) % 256, (r >> 16) % 254 + 1),
    }
}

/// The NAT pool's addresses.
fn mapped_ip(r: u64) -> String {
    format!("198.51.100.{}", (r >> 12) % 254 + 1)
}

fn duration(r: u64) -> String {
    // Most connections are short; a few are hours long.
    let secs = if r.is_multiple_of(20) { r % 30_000 } else { r % 300 };
    format!("{}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
}

/// The text after `%ASA-<sev>-<id>: `, in the id's own wording.
fn body(code: &str, a: u64, b: u64, c: u64) -> String {
    let low = pick(LOW_IFACES, b);
    let high = pick(HIGH_IFACES, c);
    let inner = private_ip(a);
    let outer = public_ip(b);
    let mapped = mapped_ip(c);
    let eport = 1024 + (a >> 40) % 64_000;
    let conn = 100_000_000 + a % 900_000_000;
    let tcp_port = TCP_PORTS[(c >> 32) as usize % TCP_PORTS.len()];
    let udp_port = UDP_PORTS[(c >> 32) as usize % UDP_PORTS.len()];
    // One connection in eight is an inbound one, to a published server.
    let inbound = (b >> 40).is_multiple_of(8);
    let (dir, fip, fport, lport) = if inbound {
        ("inbound", outer.clone(), eport, u64::from(tcp_port))
    } else {
        ("outbound", outer.clone(), u64::from(tcp_port), eport)
    };
    match code {
        "302013" => format!(
            "Built {dir} TCP connection {conn} for {low}:{fip}/{fport} ({fip}/{fport}) to {high}:{inner}/{lport} ({mapped}/{lport})"
        ),
        "302015" => format!(
            "Built {dir} UDP connection {conn} for {low}:{outer}/{udp_port} ({outer}/{udp_port}) to {high}:{inner}/{eport} ({mapped}/{eport})"
        ),
        "302014" => format!(
            "Teardown TCP connection {conn} for {low}:{fip}/{fport} to {high}:{inner}/{lport} duration {} bytes {} {}",
            duration(a >> 3),
            (b >> 20) % 5_000_000,
            pick(TEARDOWN_REASONS, c >> 7),
        ),
        "302016" => format!(
            "Teardown UDP connection {conn} for {low}:{outer}/{udp_port} to {high}:{inner}/{eport} duration {} bytes {}",
            duration(a >> 5),
            (b >> 20) % 20_000,
        ),
        "305011" | "305012" => {
            let proto = ["TCP", "TCP", "TCP", "UDP", "ICMP"][(c >> 9) as usize % 5];
            let (sport, mport) = if proto == "ICMP" {
                (1 + a % 4, 20_000 + c % 40_000)
            } else {
                (eport, eport)
            };
            let verb = if code == "305011" { "Built" } else { "Teardown" };
            let tail = if code == "305012" {
                format!(" duration {}", duration(b >> 3))
            } else {
                String::new()
            };
            format!("{verb} dynamic {proto} translation from {high}:{inner}/{sport} to {low}:{mapped}/{mport}{tail}")
        }
        "106023" => {
            let acl = pick(ACLS, c >> 11);
            if (a >> 50).is_multiple_of(5) {
                format!(
                    "Deny icmp src {low}:{outer} dst {high}:{inner} (type 8, code 0) by access-group \"{acl}\" [0x{:x}, 0x0]",
                    b >> 36
                )
            } else {
                let proto = if (a >> 52).is_multiple_of(4) { "udp" } else { "tcp" };
                let dport = if proto == "udp" { udp_port } else { tcp_port };
                format!(
                    "Deny {proto} src {low}:{outer}/{eport} dst {high}:{inner}/{dport} by access-group \"{acl}\" [0x{:x}, 0x{:x}]",
                    b >> 36,
                    c >> 44,
                )
            }
        }
        "710003" => {
            let svc = [22u16, 443, 23, 161][(c >> 13) as usize % 4];
            let proto = if svc == 161 { "UDP" } else { "TCP" };
            format!("{proto} access denied by ACL from {outer}/{eport} to {low}:{mapped}/{svc}")
        }
        "113019" => {
            let secs = a % 40_000;
            format!(
                "Group = {}, Username = {}, IP = {outer}, Session disconnected. Session Type: {}, Duration: {}h:{:02}m:{:02}s, Bytes xmt: {}, Bytes rcv: {}, Reason: {}",
                pick(VPN_GROUPS, c >> 3),
                pick(USERS, b >> 5),
                ["SSL", "AnyConnect-Parent", "IKEv2"][(c >> 17) as usize % 3],
                secs / 3600,
                secs / 60 % 60,
                secs % 60,
                (b >> 8) % 900_000_000,
                (c >> 8) % 90_000_000,
                pick(VPN_REASONS, a >> 9),
            )
        }
        "733100" => {
            let max = [10u64, 20, 40][(c >> 3) as usize % 3];
            format!(
                "[ {}] drop rate-{} exceeded. Current burst rate is {} per second, max configured rate is {max}; Current average rate is {} per second, max configured rate is {}; Cumulative total count is {}",
                pick(THREATS, b >> 7),
                1 + a % 2,
                max + a % 40,
                1 + b % 15,
                max / 2,
                1_000 + c % 90_000,
            )
        }
        "106100" => format!(
            "access-list {} permitted tcp {high}/{inner}({eport}) -> {low}/{outer}({tcp_port}) hit-cnt 1 first hit [0x{:x}, 0x0]",
            pick(ACLS, c >> 11),
            b >> 36
        ),
        "302020" => format!(
            "Built outbound ICMP connection for faddr {outer}/0 gaddr {mapped}/{} laddr {inner}/1",
            eport
        ),
        "302021" => format!(
            "Teardown ICMP connection for faddr {outer}/0 gaddr {mapped}/{} laddr {inner}/1",
            eport
        ),
        "609001" => format!("Built local-host {high}:{inner}"),
        "609002" => format!("Teardown local-host {high}:{inner} duration {}", duration(b)),
        "725001" => {
            format!("Starting SSL handshake with client {low}:{outer}/{eport} to {mapped}/443 for TLS session.")
        }
        "725002" => format!(
            "Device completed SSL handshake with client {low}:{outer}/{eport} to {mapped}/443 for TLSv1.{} session",
            2 + a % 2
        ),
        "111008" => format!(
            "User '{}' executed the '{}' command.",
            pick(USERS, b),
            [
                "write memory",
                "show running-config",
                "configure terminal",
                "copy running-config startup-config"
            ][(c >> 5) as usize % 4]
        ),
        "113004" => format!(
            "AAA user authentication Successful : server =  10.0.0.{} : user = {}",
            20 + a % 4,
            pick(USERS, b)
        ),
        "605005" => format!(
            "Login permitted from 10.0.0.{}/{eport} to mgmt:10.0.0.1/{} for user \"{}\"",
            50 + a % 20,
            ["ssh", "https"][(b % 2) as usize],
            pick(USERS, c)
        ),
        "106015" => format!(
            "Deny TCP (no connection) from {inner}/{eport} to {outer}/{tcp_port} flags {}  on interface {high}",
            ["RST", "FIN ACK", "ACK", "RST ACK", "PSH ACK"][(c >> 21) as usize % 5]
        ),
        _ => unreachable!("every MIX id has a body"),
    }
}

/// One ASA message as the firewall sends it over syslog with `logging
/// timestamp` and `logging device-id hostname` set: a PRI, the box's own
/// timestamp with the year, its name, and `%ASA-<severity>-<id>:`.
pub fn line(i: u64) -> String {
    let a = mix(i);
    let b = mix(i ^ 0x5DEE_CE66);
    let c = mix(i.wrapping_mul(31));
    let code = code(c >> 20);
    let sev = severity(code);
    let month = (a >> 20) as usize % 12;
    let day = 1 + (b >> 20) % 28;
    let (hh, mm, ss) = ((a >> 28) % 24, (b >> 28) % 60, (c >> 28) % 60);
    format!(
        "<{}>{} {:02} 2026 {:02}:{:02}:{:02} fw-{}-0{} : %ASA-{sev}-{code}: {}",
        FACILITY * 8 + sev,
        MONTHS[month],
        day,
        hh,
        mm,
        ss,
        ["nyc", "lon", "fra", "sin"][(a >> 60) as usize % 4],
        1 + (b >> 60) % 2,
        body(code, a, b, c),
    )
}

// -- the lookup table ---------------------------------------------------------

/// The site's interface naming convention, one regex per row, to the policy
/// zone and category an interface belongs to. Anchored, so a name matches
/// at most one row.
const ZONES: &[(&str, &str, &str)] = &[
    ("^inside$", "trust", "internal"),
    ("^outside$", "untrust", "internet"),
    ("^outside-isp[0-9]+$", "untrust", "internet"),
    ("^wan-mpls[0-9]*$", "wan", "private-wan"),
    ("^wan-sdwan[0-9]*$", "wan", "private-wan"),
    ("^dmz$", "dmz", "dmz"),
    ("^dmz-web[0-9]*$", "dmz", "web"),
    ("^dmz-mail$", "dmz", "mail"),
    ("^dmz-dns$", "dmz", "dns"),
    ("^dmz-api-prod$", "dmz", "api"),
    ("^dmz-api-(stage|dev)$", "dmz", "api-nonprod"),
    ("^mgmt$", "management", "management"),
    ("^oob-mgmt$", "management", "out-of-band"),
    ("^vlan1[0-9]{2}-finance$", "trust", "finance"),
    ("^vlan2[0-9]{2}-hr$", "trust", "hr"),
    ("^vlan3[0-9]{2}-eng$", "trust", "engineering"),
    ("^vlan4[0-9]{2}-sales$", "trust", "sales"),
    ("^vlan5[0-9]{2}-support$", "trust", "support"),
    ("^vlan6[0-9]{2}-legal$", "trust", "legal"),
    ("^vlan7[0-9]{2}-exec$", "trust", "executive"),
    ("^pci-cde$", "pci", "cardholder-data"),
    ("^pci-dmz$", "pci", "pci-dmz"),
    ("^storage-iscsi$", "datacenter", "storage"),
    ("^storage-nfs$", "datacenter", "storage"),
    ("^backup$", "datacenter", "backup"),
    ("^db-[a-z]+$", "datacenter", "database"),
    ("^dc[0-9]+-core$", "datacenter", "core"),
    ("^(dev|stage)-k8s$", "datacenter", "container-nonprod"),
    ("^prod-k8s$", "datacenter", "container"),
    ("^citrix$", "datacenter", "vdi"),
    ("^voip$", "voice", "voice"),
    ("^voip-[a-z]+$", "voice", "call-control"),
    ("^iot-cameras$", "iot", "physical-security"),
    ("^iot-badges$", "iot", "physical-security"),
    ("^iot-[a-z]+$", "iot", "iot"),
    ("^printers$", "iot", "print"),
    ("^scada-[a-z0-9]+$", "ot", "industrial"),
    ("^lab[0-9]*$", "lab", "lab"),
    ("^branch-[a-z]{3}[0-9]{2}$", "branch", "branch"),
    ("^wireless-corp$", "trust", "wireless"),
    ("^guest$", "guest", "guest"),
    ("^guest-wifi$", "guest", "guest-wireless"),
    ("^anyconnect-staff$", "vpn", "remote-access"),
    ("^anyconnect-contractors$", "vpn", "remote-access-3p"),
    ("^vpn(-pool)?$", "vpn", "remote-access"),
    ("^partner-acme$", "partner", "extranet"),
    ("^partner-globex$", "partner", "extranet"),
    ("^partner-[a-z]+$", "partner", "extranet-other"),
    ("^cloud-aws$", "cloud", "iaas"),
    ("^cloud-azure$", "cloud", "iaas"),
    ("^cloud-gcp$", "cloud", "iaas"),
    ("^failover$", "ha", "failover"),
    ("^state(-link)?$", "ha", "state"),
];

fn zone_table() -> serde_json::Value {
    let rows: Vec<Vec<&str>> = ZONES.iter().map(|(p, z, c)| vec![*p, *z, *c]).collect();
    serde_json::json!({
        "id": "zones", "type": "inline",
        "headers": ["pattern", "zone", "category"],
        "rows": rows,
    })
}

// -- the pipeline -------------------------------------------------------------

/// What every branch's Grok may use. An ASA interface name (`nameif`) is
/// letters, digits, `-` and `_`; `NOTSPACE` would run through the `:` after
/// it.
fn custom_patterns() -> serde_json::Value {
    serde_json::json!([
        {"name": "IFACE", "pattern": "[A-Za-z0-9_-]+"},
        {"name": "ASATIME", "pattern": "%{MONTH} %{MONTHDAY} %{YEAR} %{TIME}"},
        {"name": "HMS", "pattern": "%{INT:dur_h}:%{INT:dur_m}:%{INT:dur_s}"},
    ])
}

/// The captures that are numbers. Every other capture is a string.
const NUMERIC: &[&str] = &[
    "pri",
    "severity",
    "src_port",
    "dst_port",
    "src_xlated_port",
    "dst_xlated_port",
    "dur_h",
    "dur_m",
    "dur_s",
    "bytes",
    "bytes_xmt",
    "bytes_rcv",
    "icmp_type",
    "icmp_code",
    "burst_rate",
    "burst_max",
    "avg_rate",
    "avg_max",
    "total_count",
];

/// Every field `patterns` capture, in order of first appearance, `HMS`
/// standing for its three.
fn captures(patterns: &[&str]) -> Vec<String> {
    let re = regex::Regex::new(r"%\{(\w+)(?::(\w+))?\}").unwrap();
    let mut out: Vec<String> = Vec::new();
    for p in patterns {
        for c in re.captures_iter(p) {
            let names: Vec<&str> = match (c.get(1).unwrap().as_str(), c.get(2)) {
                (_, Some(n)) => vec![n.as_str()],
                ("HMS", None) => vec!["dur_h", "dur_m", "dur_s"],
                _ => vec![],
            };
            for n in names {
                if !out.iter().any(|o| o == n) {
                    out.push(n.to_string());
                }
            }
        }
    }
    out
}

fn grok(id: &str, input: &str, patterns: &[&str]) -> serde_json::Value {
    // Grok produces exactly its captures, so they are declared rather than
    // discovered, the numbers as the numbers they are.
    let declared: Vec<serde_json::Value> = captures(patterns)
        .iter()
        .map(|n| match n.as_str() {
            "timestamp" => serde_json::json!({"name": n, "type": {"type": "datetime", "format": "%b %d %Y %H:%M:%S"}}),
            _ if NUMERIC.contains(&n.as_str()) => serde_json::json!({"name": n, "type": "i64"}),
            _ => serde_json::json!({"name": n, "type": "str"}),
        })
        .collect();
    serde_json::json!({
        "id": id, "type": "parser", "input": input,
        "kind": {"type": "grok", "patterns": patterns, "customPatterns": custom_patterns()},
        "declared": declared,
    })
}

/// The envelope every message shares. The box's timestamp carries its year,
/// so it lands typed on the parse.
fn header_step() -> serde_json::Value {
    grok(
        "envelope",
        "_raw",
        &[
            "^<%{INT:pri}>%{ASATIME:timestamp} %{HOSTNAME:host} : %ASA-%{INT:severity}-%{INT:code}: %{GREEDYDATA:message}$",
        ],
    )
}

/// A branch: when `code` is one of `codes`, parse `message` with
/// `patterns` and set what the id means.
fn branch(id: &str, codes: &[&str], patterns: &[&str], sets: &[(&str, &str)]) -> serde_json::Value {
    let condition = codes
        .iter()
        .map(|c| format!("code == \"{c}\""))
        .collect::<Vec<_>>()
        .join(" || ");
    serde_json::json!({
        "id": id, "type": "if", "condition": condition,
        "then": [
            grok(&format!("{id}-parse"), "message", patterns),
            {
                "id": format!("{id}-meaning"), "type": "eval",
                "assignments": sets.iter().map(|(t, e)| serde_json::json!({"target": t, "expr": e})).collect::<Vec<_>>(),
            },
        ],
        "else": [],
    })
}

fn branches() -> Vec<serde_json::Value> {
    vec![
        branch(
            "build",
            &["302013", "302015"],
            &[
                "^Built %{WORD:direction} %{WORD:protocol} connection %{INT:connection_id} for %{IFACE:src_interface}:%{IP:src_ip}/%{INT:src_port} \\(%{IP:src_xlated_ip}/%{INT:src_xlated_port}\\) to %{IFACE:dst_interface}:%{IP:dst_ip}/%{INT:dst_port} \\(%{IP:dst_xlated_ip}/%{INT:dst_xlated_port}\\)",
            ],
            &[("action", r#""connection-started""#)],
        ),
        branch(
            "teardown",
            &["302014", "302016"],
            &[
                "^Teardown %{WORD:protocol} connection %{INT:connection_id} for %{IFACE:src_interface}:%{IP:src_ip}/%{INT:src_port} to %{IFACE:dst_interface}:%{IP:dst_ip}/%{INT:dst_port} duration %{HMS} bytes %{INT:bytes}(?: %{GREEDYDATA:reason})?$",
            ],
            &[("action", r#""connection-finished""#)],
        ),
        branch(
            "xlate",
            &["305011", "305012"],
            &[
                "^%{WORD} dynamic %{WORD:protocol} translation from %{IFACE:src_interface}:%{IP:src_ip}/%{INT:src_port} to %{IFACE:dst_interface}:%{IP:src_xlated_ip}/%{INT:src_xlated_port}(?: duration %{HMS})?$",
            ],
            &[(
                "action",
                r#"code == "305011" ? "translation-built" : "translation-torn-down""#,
            )],
        ),
        branch(
            "deny",
            &["106023"],
            &[
                "^Deny %{WORD:protocol} src %{IFACE:src_interface}:%{IP:src_ip}/%{INT:src_port} dst %{IFACE:dst_interface}:%{IP:dst_ip}/%{INT:dst_port} by access-group \"%{DATA:acl}\"",
                "^Deny %{WORD:protocol} src %{IFACE:src_interface}:%{IP:src_ip} dst %{IFACE:dst_interface}:%{IP:dst_ip} \\(type %{INT:icmp_type}, code %{INT:icmp_code}\\) by access-group \"%{DATA:acl}\"",
            ],
            &[("action", r#""denied""#), ("outcome", r#""failure""#)],
        ),
        branch(
            "to-the-box",
            &["710003"],
            &[
                "^%{WORD:protocol} access denied by ACL from %{IP:src_ip}/%{INT:src_port} to %{IFACE:dst_interface}:%{IP:dst_ip}/%{INT:dst_port}$",
            ],
            &[("action", r#""denied""#), ("outcome", r#""failure""#)],
        ),
        branch(
            "vpn-end",
            &["113019"],
            &[
                "^Group = %{DATA:vpn_group}, Username = %{DATA:user}, IP = %{IP:src_ip}, Session disconnected\\. Session Type: %{DATA:session_type}, Duration: %{INT:dur_h}h:%{INT:dur_m}m:%{INT:dur_s}s, Bytes xmt: %{INT:bytes_xmt}, Bytes rcv: %{INT:bytes_rcv}, Reason: %{GREEDYDATA:reason}$",
            ],
            &[("action", r#""vpn-session-ended""#)],
        ),
        branch(
            "burst",
            &["733100"],
            &[
                "^\\[ *%{DATA:threat_object}\\] drop rate-%{INT} exceeded\\. Current burst rate is %{INT:burst_rate} per second, max configured rate is %{INT:burst_max}; Current average rate is %{INT:avg_rate} per second, max configured rate is %{INT:avg_max}; Cumulative total count is %{INT:total_count}$",
            ],
            &[("action", r#""drop-rate-exceeded""#), ("outcome", r#""failure""#)],
        ),
    ]
}

/// The interface on each side to its zone. A message with no interface on a
/// side skips that side's lookup.
fn lookup_steps() -> Vec<serde_json::Value> {
    let side = |id: &str, input: &str, zone: &str, category: &str| {
        serde_json::json!({
            "id": id, "type": "lookup", "input": input, "table": "zones",
            "filter": format!("!isNull({input})"),
            "matchKind": "regex", "multiplicity": "first", "keyColumn": "pattern",
            "outputs": [
                {"source": "zone", "field": zone},
                {"source": "category", "field": category},
            ],
        })
    };
    vec![
        side(
            "ingress-zone",
            "src_interface",
            "observer.ingress.zone",
            "cisco.asa.source_zone_category",
        ),
        side(
            "egress-zone",
            "dst_interface",
            "observer.egress.zone",
            "cisco.asa.destination_zone_category",
        ),
    ]
}

/// The mapping: derived and typed fields by `eval`, straight moves by
/// `rename`, and the intermediates nulled out — every non-null field is
/// serialized, so nulling is how a field leaves the document.
fn ecs_steps() -> Vec<serde_json::Value> {
    let evals = [
        ("event.dataset", r#""cisco.asa""#),
        ("event.module", r#""cisco""#),
        ("event.kind", r#""event""#),
        ("event.category", r#""network""#),
        ("event.severity", "severity"),
        ("event.action", r#"action ?? "other""#),
        ("event.duration", "(dur_h * 3600 + dur_m * 60 + dur_s) * 1000000000"),
        (
            "log.level",
            r#"severity == 0 ? "emergency" : severity == 1 ? "alert" : severity == 2 ? "critical" : severity == 3 ? "error" : severity == 4 ? "warning" : severity == 5 ? "notification" : severity == 6 ? "informational" : "debug""#,
        ),
        ("log.syslog.priority", "pri"),
        ("log.syslog.facility.code", "idiv(pri, 8)"),
        ("network.transport", "protocol.toLowerCase()"),
        ("observer.vendor", r#""Cisco""#),
        ("observer.product", r#""ASA""#),
        ("observer.type", r#""firewall""#),
    ];
    let renames = [
        ("timestamp", "@timestamp"),
        ("code", "event.code"),
        ("outcome", "event.outcome"),
        ("reason", "event.reason"),
        ("host", "observer.name"),
        ("src_ip", "source.ip"),
        ("src_port", "source.port"),
        ("src_xlated_ip", "source.nat.ip"),
        ("src_xlated_port", "source.nat.port"),
        ("dst_ip", "destination.ip"),
        ("dst_port", "destination.port"),
        ("dst_xlated_ip", "destination.nat.ip"),
        ("dst_xlated_port", "destination.nat.port"),
        ("bytes", "network.bytes"),
        ("bytes_rcv", "source.bytes"),
        ("bytes_xmt", "destination.bytes"),
        ("direction", "network.direction"),
        ("acl", "rule.name"),
        ("user", "user.name"),
        ("src_interface", "observer.ingress.interface.name"),
        ("dst_interface", "observer.egress.interface.name"),
        ("connection_id", "cisco.asa.connection_id"),
        ("icmp_type", "cisco.asa.icmp_type"),
        ("icmp_code", "cisco.asa.icmp_code"),
        ("vpn_group", "cisco.asa.tunnel_group"),
        ("session_type", "cisco.asa.session_type"),
        ("threat_object", "cisco.asa.burst.object"),
        ("burst_rate", "cisco.asa.burst.current_rate"),
        ("burst_max", "cisco.asa.burst.configured_rate"),
        ("avg_rate", "cisco.asa.burst.avg_rate"),
        ("avg_max", "cisco.asa.burst.configured_avg_rate"),
        ("total_count", "cisco.asa.burst.cumulative_count"),
    ];
    // Read by the evals above and not carried into the document.
    let dropped = [
        "_raw", "pri", "severity", "action", "protocol", "dur_h", "dur_m", "dur_s",
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

fn bundle() -> String {
    let mut steps = vec![header_step()];
    steps.extend(branches());
    steps.extend(lookup_steps());
    steps.extend(ecs_steps());
    super::bundle(steps, vec![zone_table()])
}

// -- the check ----------------------------------------------------------------

/// The zone row an interface name matches, as the regex lookup should find
/// it.
fn zone_of(iface: &str) -> Option<&'static str> {
    static COMPILED: std::sync::OnceLock<Vec<regex::Regex>> = std::sync::OnceLock::new();
    let compiled = COMPILED.get_or_init(|| ZONES.iter().map(|(p, _, _)| regex::Regex::new(p).unwrap()).collect());
    compiled.iter().position(|re| re.is_match(iface)).map(|n| ZONES[n].1)
}

fn str_at<'a>(doc: &'a serde_json::Value, ptr: &str) -> Option<&'a str> {
    doc.pointer(ptr).and_then(|v| v.as_str())
}

/// How many rows `check` runs again, keeping every document. The eight the
/// shared check keeps are too few to reach the rarer ids.
const SAMPLE: u64 = 4_000;

fn check(rows: u64, docs: u64, kept: &[serde_json::Value]) {
    assert_eq!(docs, rows, "every message, handled id or not, is a document");
    check_docs(kept);

    let (_, sink) = super::run(&bundle(), line, SAMPLE, 500, SAMPLE as usize);
    let all: Vec<serde_json::Value> = sink
        .kept()
        .iter()
        .map(|d| serde_json::from_slice(d).expect("the destination gets JSON"))
        .collect();
    // The warm-up batch is outside `run`'s counts but inside what the sink kept.
    assert_eq!(all.len() as u64, SAMPLE, "every message is a document");
    let (seen, looked_up, hit) = check_docs(&all);
    for code in HANDLED.iter().chain(&["106100", "609001"]) {
        assert!(seen.contains(code), "the sample reaches {code}");
    }
    assert!(
        hit * 100 >= looked_up * 85,
        "the regex lookup hit {hit} of {looked_up} interfaces"
    );
}

/// Checks each document against the record it came from: the kept
/// documents are the stream's first rows, in order. Returns the ids seen,
/// the interfaces that reached a lookup and how many of those found a zone.
fn check_docs(docs: &[serde_json::Value]) -> (Vec<&'static str>, usize, usize) {
    let (mut seen, mut looked_up, mut hit) = (Vec::new(), 0, 0);
    for (i, doc) in docs.iter().enumerate() {
        let code = code(mix((i as u64).wrapping_mul(31)) >> 20);
        if !seen.contains(&code) {
            seen.push(code);
        }
        assert!(doc.pointer("/smeltr/exception").is_none(), "row {i} failed: {doc}");
        assert!(doc.get("_raw").is_none(), "_raw is pruned");
        assert_eq!(str_at(doc, "/event/code"), Some(code), "row {i}: {doc}");
        assert_eq!(str_at(doc, "/observer/vendor"), Some("Cisco"));
        assert!(str_at(doc, "/observer/name").unwrap().starts_with("fw-"));
        assert!(str_at(doc, "/@timestamp").unwrap().starts_with("2026-"), "{doc}");
        let sev = doc.pointer("/event/severity").and_then(|v| v.as_i64());
        assert_eq!(sev, Some(severity(code) as i64), "event.severity is a number");
        assert!(str_at(doc, "/message").is_some(), "the message text is kept");

        let action = str_at(doc, "/event/action").unwrap();
        if !HANDLED.contains(&code) {
            assert_eq!(action, "other", "row {i}");
            assert!(
                doc.get("source").is_none(),
                "row {i}: an unhandled id is not parsed: {doc}"
            );
            continue;
        }
        assert_ne!(action, "other", "row {i}: a handled id gets its action");
        match code {
            "113019" => {
                assert!(str_at(doc, "/user/name").is_some(), "row {i}: {doc}");
                assert!(doc.pointer("/source/bytes").and_then(|v| v.as_i64()).is_some());
                assert!(doc.pointer("/event/duration").and_then(|v| v.as_i64()).is_some());
            }
            "733100" => {
                assert!(str_at(doc, "/cisco/asa/burst/object").is_some(), "row {i}: {doc}");
                assert!(
                    doc.pointer("/cisco/asa/burst/cumulative_count")
                        .and_then(|v| v.as_i64())
                        .is_some()
                );
            }
            _ => {
                assert!(str_at(doc, "/source/ip").is_some(), "row {i}: source.ip: {doc}");
                let transport = str_at(doc, "/network/transport").unwrap();
                assert!(["tcp", "udp", "icmp"].contains(&transport), "row {i}: {transport}");
            }
        }
        match code {
            "302013" | "302015" => assert!(str_at(doc, "/source/nat/ip").is_some(), "row {i}: {doc}"),
            "302014" | "302016" => {
                assert!(
                    doc.pointer("/network/bytes").and_then(|v| v.as_i64()).is_some(),
                    "row {i}: {doc}"
                );
                assert!(doc.pointer("/event/duration").and_then(|v| v.as_i64()).is_some());
            }
            "106023" => {
                assert_eq!(str_at(doc, "/event/outcome"), Some("failure"));
                assert!(str_at(doc, "/rule/name").is_some(), "row {i}: rule.name: {doc}");
                assert!(str_at(doc, "/destination/ip").is_some());
            }
            _ => {}
        }
        // The regex lookup: the zone is the one row the interface matches.
        for side in ["ingress", "egress"] {
            if let Some(name) = str_at(doc, &format!("/observer/{side}/interface/name")) {
                let zone = str_at(doc, &format!("/observer/{side}/zone"));
                assert_eq!(zone, zone_of(name), "row {i}: {side} zone of {name}");
                looked_up += 1;
                hit += usize::from(zone.is_some());
            }
        }
    }
    (seen, looked_up, hit)
}
