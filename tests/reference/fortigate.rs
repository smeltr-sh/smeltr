//! FortiGate key-value syslog to ECS, with the source host enriched from an
//! asset inventory kept as a CSV file -- the one lookup source no other
//! reference pipeline reads.
//!
//! The record is what FortiOS 7.4 sends to a syslog collector: a `<pri>`
//! header and some forty `key=value` pairs, values with spaces quoted. Grok
//! takes the header off, the key-value parser splits the body, a lookup on
//! the source IP adds the asset's owner and criticality, and the mapping
//! moves the vendor's names onto ECS ones. The keys the mapping does not
//! use go under `fortinet.`, which is where `rest($)` puts them in
//! `docs/examples.md`.

use super::Pipeline;

pub const PIPELINE: Pipeline = Pipeline {
    name: "fortigate",
    line,
    bundle,
    check,
};

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

// -- the asset inventory ------------------------------------------------------

/// Rows in the asset table.
const ASSETS: u64 = 2_000;
/// Internal hosts the traffic comes from. The inventory covers the first
/// `ASSETS` of them; the rest are the unmanaged devices every inventory
/// misses, so roughly one internal source in five misses.
const HOSTS: u64 = 2_500;

const SITES: &[&str] = &["hq", "nyc", "lon", "fra", "sin"];
const ROLES: &[&str] = &["wks", "wks", "wks", "lap", "lap", "srv", "prn", "voip"];
const OWNERS: &[&str] = &[
    "it-ops",
    "finance",
    "engineering",
    "sales",
    "hr",
    "legal",
    "marketing",
    "support",
    "facilities",
    "security",
];
const OSES: &[&str] = &[
    "Windows 11 23H2",
    "Windows 10 22H2",
    "macOS 14.6",
    "Ubuntu 22.04",
    "Windows Server 2022",
    "RHEL 9.4",
];
const CRITICALITY: &[&str] = &["low", "medium", "medium", "high", "critical"];

/// Internal host `k`'s address: one `/16` per site, a `/24` per hundred
/// hosts.
fn host_ip(k: u64) -> String {
    format!("10.{}.{}.{}", 10 + k % 5, 1 + k / 100, 10 + k % 100)
}

/// Asset `k`'s inventory row: ip, hostname, owner, criticality, os.
fn asset(k: u64) -> [String; 5] {
    let r = mix(k ^ 0xA55E_7000);
    let site = SITES[(k % 5) as usize];
    let role = pick(ROLES, r);
    [
        host_ip(k),
        format!("{site}-{role}-{k:04}"),
        pick(OWNERS, r >> 8).to_string(),
        if role == "srv" {
            "critical".to_string()
        } else {
            pick(CRITICALITY, r >> 16).to_string()
        },
        match role {
            "srv" => pick(&OSES[4..], r >> 24),
            "prn" | "voip" => "Embedded",
            _ => pick(&OSES[..4], r >> 24),
        }
        .to_string(),
    ]
}

/// The inventory as the CSV file the table reads.
fn assets_csv() -> String {
    let mut csv = String::from("ip,hostname,owner,criticality,os\n");
    for k in 0..ASSETS {
        csv.push_str(&asset(k).join(","));
        csv.push('\n');
    }
    csv
}

// -- the record ---------------------------------------------------------------

/// What record `i` is. The mix is a branch office's: mostly forwarded
/// sessions, some traffic aimed at the firewall itself from outside, and
/// the security profiles' own logs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Forward,
    Local,
    WebFilter,
    Ips,
}

fn kind(i: u64) -> Kind {
    match mix(i) % 100 {
        0..72 => Kind::Forward,
        72..82 => Kind::Local,
        82..96 => Kind::WebFilter,
        _ => Kind::Ips,
    }
}

/// The internal host record `i` comes from, or `None` when its source is
/// outside (traffic to the firewall itself).
fn source_host(i: u64) -> Option<u64> {
    (kind(i) != Kind::Local).then(|| mix(i ^ 0x5EED) % HOSTS)
}

const DEVICES: &[(&str, &str)] = &[
    ("FGT-HQ-01", "FG1K0F3922000117"),
    ("FGT-HQ-02", "FG1K0F3922000245"),
    ("FGT-NYC", "FG100FTK21004417"),
    ("FGT-LON", "FG100FTK21007731"),
    ("FGT-FRA", "FG60FTK2209A1C3"),
    ("FGT-SIN", "FG60FTK2209B0F7"),
];

/// Applications: FortiGuard name, id, category, risk, the service it rides,
/// destination port, protocol number.
const APPS: &[(&str, u32, &str, &str, &str, u16, u8)] = &[
    ("HTTPS.BROWSER", 40568, "Web.Client", "medium", "HTTPS", 443, 6),
    ("HTTP.BROWSER", 15893, "Web.Client", "medium", "HTTP", 80, 6),
    ("Microsoft.Portal", 41469, "Collaboration", "elevated", "HTTPS", 443, 6),
    ("Microsoft.Teams", 43541, "Collaboration", "elevated", "HTTPS", 443, 6),
    ("Google.Services", 41468, "General.Interest", "medium", "HTTPS", 443, 6),
    ("Zoom", 34039, "Video/Audio", "elevated", "udp/8801", 8801, 17),
    ("DNS", 16195, "Network.Service", "elevated", "DNS", 53, 17),
    ("NTP", 16270, "Network.Service", "elevated", "NTP", 123, 17),
    ("Salesforce", 16920, "Collaboration", "medium", "HTTPS", 443, 6),
    ("Slack", 38184, "Collaboration", "elevated", "HTTPS", 443, 6),
    ("SSH", 16060, "Network.Service", "elevated", "SSH", 22, 6),
    ("QUIC", 40169, "Network.Service", "low", "udp/443", 443, 17),
];

const COUNTRIES: &[&str] = &[
    "United States",
    "United States",
    "United States",
    "Ireland",
    "Germany",
    "Netherlands",
    "Singapore",
    "Japan",
    "United Kingdom",
];

const WEB: &[(&str, u32, &str, &str)] = &[
    ("login.microsoftonline.com", 52, "Information Technology", "ftgd_allow"),
    ("www.salesforce.com", 49, "Business", "ftgd_allow"),
    ("news.ycombinator.com", 36, "News and Media", "ftgd_allow"),
    ("cdn.jsdelivr.net", 52, "Information Technology", "ftgd_allow"),
    ("www.linkedin.com", 37, "Social Networking", "ftgd_allow"),
    ("bet365.example", 11, "Gambling", "ftgd_blk"),
    ("free-proxy.example", 59, "Proxy Avoidance", "ftgd_blk"),
    ("mal-dl.example", 26, "Malicious Websites", "ftgd_blk"),
];

const ATTACKS: &[(&str, u32, &str)] = &[
    ("Apache.Log4j.Error.Log.Remote.Code.Execution", 51006, "critical"),
    (
        "MS.SMB.Server.SMB1.Trans2.Secondary.Handling.Code.Execution",
        43796,
        "critical",
    ),
    ("HTTP.URI.SQL.Injection", 15621, "high"),
    ("Nmap.Script.Scanner", 41557, "low"),
    ("Bash.Function.Definitions.Remote.Code.Execution", 39294, "critical"),
];

/// One FortiOS 7.4 log line as a syslog collector receives it.
pub fn line(i: u64) -> String {
    let a = mix(i);
    let b = mix(i ^ 0xC0FF_EE00);
    let c = mix(i.wrapping_mul(0x2545_F491));
    let k = kind(i);

    let (devname, devid) = DEVICES[(b % DEVICES.len() as u64) as usize];
    let (mon, day) = (1 + (a >> 8) % 12, 1 + (a >> 12) % 28);
    let (hh, mm, ss) = ((b >> 8) % 24, (b >> 16) % 60, (b >> 24) % 60);
    // Days before each month of 2026, so `eventtime` is the same instant as
    // `date` and `time`.
    const BEFORE: [u64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let epoch = 1_767_225_600 + (BEFORE[mon as usize - 1] + day - 1) * 86_400 + hh * 3600 + mm * 60 + ss;
    let srcip = match source_host(i) {
        Some(h) => host_ip(h),
        None => format!(
            "{}.{}.{}.{}",
            45 + (c % 150),
            (c >> 8) % 256,
            (c >> 16) % 256,
            1 + (c >> 24) % 254
        ),
    };
    let srcport = 1024 + (c >> 32) % 64_000;
    let srcmac = format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        0x00,
        0x50,
        0x56,
        (c >> 40) & 0xff,
        (c >> 48) & 0xff,
        (c >> 56) & 0xff
    );
    let dstip = format!(
        "{}.{}.{}.{}",
        13 + (b >> 32) % 190,
        (b >> 40) % 256,
        (b >> 48) % 256,
        1 + (b >> 56) % 254
    );
    let session = 10_000_000 + (a >> 20) % 90_000_000;
    let policyid = 1 + (a >> 28) % 40;
    let poluuid = format!(
        "{:08x}-{:04x}-51ef-{:04x}-{:012x}",
        mix(policyid) as u32,
        (mix(policyid) >> 32) as u16,
        (mix(policyid) >> 48) as u16,
        mix(policyid ^ 7) & 0xffff_ffff_ffff
    );
    let country = pick(COUNTRIES, b >> 4);
    let head = format!(
        "<189>date=2026-{mon:02}-{day:02} time={hh:02}:{mm:02}:{ss:02} devname=\"{devname}\" devid=\"{devid}\" \
         eventtime={epoch}{nanos:09} tz=\"+0000\"",
        nanos = c % 1_000_000_000
    );

    match k {
        Kind::Forward => {
            let (app, appid, appcat, apprisk, service, dport, proto) = APPS[(a % APPS.len() as u64) as usize];
            // Most forwarded sessions end cleanly; a few time out, are
            // reset, or are refused by an implicit deny.
            let action = match (a >> 40) % 20 {
                0 => "deny",
                1 => "timeout",
                2 => "server-rst",
                3 => "client-rst",
                4..=7 if proto == 17 => "accept",
                _ => "close",
            };
            let denied = action == "deny";
            // A DNS or NTP exchange is a packet each way; everything else
            // is a session of some length.
            let query = dport == 53 || dport == 123;
            let duration = match () {
                _ if denied => 0,
                _ if query => (b >> 12) % 180,
                _ => (b >> 12) % 3600,
            };
            let sentpkt = match () {
                _ if denied || query => 1,
                _ => 2 + (c >> 12) % 4000,
            };
            let rcvdpkt = match () {
                _ if denied => 0,
                _ if query => 1,
                _ => 2 + (a >> 12) % 9000,
            };
            let sentbyte = sentpkt * (60 + (c >> 20) % 900);
            let rcvdbyte = rcvdpkt * (60 + (a >> 44) % 1400);
            let natport = 1024 + (a >> 48) % 64_000;
            format!(
                "{head} logid=\"0000000013\" type=\"traffic\" subtype=\"forward\" level=\"{level}\" vd=\"root\" \
                 srcip={srcip} srcport={srcport} srcintf=\"port{sif}\" srcintfrole=\"lan\" dstip={dstip} \
                 dstport={dport} dstintf=\"wan{wan}\" dstintfrole=\"wan\" srccountry=\"Reserved\" \
                 dstcountry=\"{country}\" sessionid={session} proto={proto} action=\"{action}\" \
                 policyid={pid} policytype=\"policy\" poluuid=\"{poluuid}\" policyname=\"{pname}\" \
                 service=\"{service}\" trandisp=\"snat\" transip=203.0.113.{nat} transport={natport} \
                 appid={appid} app=\"{app}\" appcat=\"{appcat}\" apprisk=\"{apprisk}\" applist=\"default\" \
                 duration={duration} sentbyte={sentbyte} rcvdbyte={rcvdbyte} sentpkt={sentpkt} rcvdpkt={rcvdpkt} \
                 vwlid=0 srcmac=\"{srcmac}\" mastersrcmac=\"{srcmac}\" srcserver=0 osname=\"{os}\" \
                 dstdevtype=\"Unknown\" masterdstmac=\"{srcmac}\"",
                level = if denied { "warning" } else { "notice" },
                sif = 2 + (b >> 2) % 4,
                wan = 1 + (b >> 6) % 2,
                pid = if denied { 0 } else { policyid },
                pname = if denied {
                    "Implicit Deny"
                } else {
                    pick(&["LAN-to-WAN", "Guest-Internet", "Servers-Out", "VoIP-Out"], policyid)
                },
                nat = 1 + (a >> 52) % 30,
                os = pick(&["Windows", "Windows", "macOS", "Linux"], c >> 3),
            )
        }
        Kind::Local => {
            // Probes at the firewall's own WAN address: refused, no
            // policy, no application.
            let dport = pick(&["443", "22", "23", "8443", "3389", "500"], b >> 2);
            format!(
                "{head} logid=\"0001000014\" type=\"traffic\" subtype=\"local\" level=\"notice\" vd=\"root\" \
                 srcip={srcip} srcport={srcport} srcintf=\"wan1\" srcintfrole=\"wan\" dstip=203.0.113.1 \
                 dstport={dport} dstintf=\"root\" dstintfrole=\"undefined\" srccountry=\"{country}\" \
                 dstcountry=\"Reserved\" sessionid={session} proto=6 action=\"deny\" policyid=0 \
                 policytype=\"local-in-policy\" service=\"tcp/{dport}\" trandisp=\"noop\" app=\"tcp/{dport}\" \
                 duration=0 sentbyte=0 rcvdbyte=0 sentpkt=0 rcvdpkt=0 appcat=\"unscanned\" crscore=5 \
                 craction=262144 crlevel=\"low\""
            )
        }
        Kind::WebFilter => {
            let (host, cat, catdesc, eventtype) = WEB[(a % WEB.len() as u64) as usize];
            let blocked = eventtype == "ftgd_blk";
            format!(
                "{head} logid=\"{logid}\" type=\"utm\" subtype=\"webfilter\" eventtype=\"{eventtype}\" \
                 level=\"{level}\" vd=\"root\" policyid={policyid} poluuid=\"{poluuid}\" \
                 policytype=\"policy\" sessionid={session} srcip={srcip} srcport={srcport} \
                 srccountry=\"Reserved\" srcintf=\"port{sif}\" srcintfrole=\"lan\" dstip={dstip} dstport=443 \
                 dstcountry=\"{country}\" dstintf=\"wan1\" dstintfrole=\"wan\" proto=6 service=\"HTTPS\" \
                 hostname=\"{host}\" profile=\"default\" action=\"{action}\" reqtype=\"direct\" \
                 url=\"https://{host}/{path}\" sentbyte={sent} rcvdbyte={rcvd} direction=\"outgoing\" \
                 msg=\"{msg}\" method=\"domain\" cat={cat} catdesc=\"{catdesc}\"",
                logid = if blocked { "0316013056" } else { "0316013057" },
                level = if blocked { "warning" } else { "notice" },
                sif = 2 + (b >> 2) % 4,
                action = if blocked { "blocked" } else { "passthrough" },
                path = pick(
                    &["", "index.html", "api/v2/session", "static/app.js", "login?next=%2F"],
                    c >> 5
                ),
                sent = if blocked { 0 } else { 300 + (c >> 10) % 4000 },
                rcvd = if blocked { 0 } else { 900 + (a >> 10) % 90_000 },
                msg = if blocked {
                    "URL belongs to a denied category in policy"
                } else {
                    "URL belongs to an allowed category in policy"
                },
            )
        }
        Kind::Ips => {
            let (attack, attackid, severity) = ATTACKS[(a % ATTACKS.len() as u64) as usize];
            format!(
                "{head} logid=\"0419016384\" type=\"utm\" subtype=\"ips\" eventtype=\"signature\" level=\"alert\" \
                 vd=\"root\" severity=\"{severity}\" srcip={srcip} srccountry=\"Reserved\" dstip={dstip} \
                 dstcountry=\"{country}\" srcintf=\"port{sif}\" srcintfrole=\"lan\" dstintf=\"wan1\" \
                 dstintfrole=\"wan\" sessionid={session} action=\"dropped\" proto=6 service=\"HTTPS\" \
                 policyid={policyid} poluuid=\"{poluuid}\" policytype=\"policy\" attack=\"{attack}\" \
                 srcport={srcport} dstport=443 direction=\"outgoing\" attackid={attackid} profile=\"default\" \
                 ref=\"http://www.fortinet.com/ids/VID{attackid}\" incidentserialno={incident} \
                 msg=\"web_server: {attack}\" crscore={crscore} craction=4096 crlevel=\"{severity}\"",
                sif = 2 + (b >> 2) % 4,
                incident = 100_000_000 + (c >> 8) % 900_000_000,
                crscore = if severity == "critical" { 50 } else { 10 },
            )
        }
    }
}

// -- the pipeline -------------------------------------------------------------

/// The keys the document reads, as `docs/examples.md` declares them: the
/// ports, byte counts and duration typed as the numbers they are.
const DECLARED: &[(&str, &str)] = &[
    ("date", "str"),
    ("time", "str"),
    ("devname", "str"),
    ("type", "str"),
    ("srcip", "str"),
    ("dstip", "str"),
    ("srcport", "i64"),
    ("dstport", "i64"),
    ("proto", "str"),
    ("action", "str"),
    ("policyid", "str"),
    ("app", "str"),
    ("sentbyte", "i64"),
    ("rcvdbyte", "i64"),
    ("duration", "i64"),
    ("level", "str"),
];

/// Every other key the four record kinds carry, moved under `fortinet.`.
/// `rest($)` gathers whatever the mapping did not move; a rename step moves
/// only a field the pipeline knows, so these are declared as the text a
/// key-value pair is, and a key a later FortiOS adds lands at the
/// document's top level instead.
const REST: &[&str] = &[
    "devid",
    "eventtime",
    "tz",
    "logid",
    "subtype",
    "eventtype",
    "vd",
    "srcintf",
    "srcintfrole",
    "dstintf",
    "dstintfrole",
    "srccountry",
    "dstcountry",
    "sessionid",
    "policytype",
    "poluuid",
    "policyname",
    "service",
    "trandisp",
    "transip",
    "transport",
    "appid",
    "appcat",
    "apprisk",
    "applist",
    "sentpkt",
    "rcvdpkt",
    "vwlid",
    "srcmac",
    "mastersrcmac",
    "srcserver",
    "osname",
    "dstdevtype",
    "masterdstmac",
    "crscore",
    "craction",
    "crlevel",
    "hostname",
    "profile",
    "reqtype",
    "url",
    "direction",
    "msg",
    "method",
    "cat",
    "catdesc",
    "severity",
    "attack",
    "attackid",
    "ref",
    "incidentserialno",
];

fn steps() -> Vec<serde_json::Value> {
    let evals = [
        ("@timestamp", "parseTime(`${date} ${time}`)"),
        ("event.dataset", "`fortinet.${type}`"),
        ("event.action", "action"),
        ("event.duration", "duration * 1000000000"),
        ("event.severity", "level"),
        ("source.port", "srcport"),
        ("source.bytes", "sentbyte"),
        ("destination.port", "dstport"),
        ("destination.bytes", "rcvdbyte"),
        (
            "network.transport",
            r#"{"6": "tcp", "17": "udp", "1": "icmp"}[proto] ?? proto"#,
        ),
        ("observer.vendor", r#""Fortinet""#),
        ("observer.product", r#""FortiGate""#),
    ];
    let renames = [
        ("srcip", "source.ip"),
        ("dstip", "destination.ip"),
        ("app", "network.application"),
        ("devname", "observer.name"),
        ("policyid", "rule.id"),
    ];
    // Read by the evals above and not carried into the document.
    let dropped = [
        "_raw", "pri", "body", "date", "time", "type", "action", "duration", "level", "srcport", "sentbyte", "dstport",
        "rcvdbyte", "proto",
    ];
    vec![
        serde_json::json!({
            "id": "syslog", "type": "parser", "input": "_raw",
            "kind": {"type": "grok", "patterns": ["<%{INT:pri}>%{GREEDYDATA:body}"]},
            "declared": [{"name": "pri", "type": "str"}, {"name": "body", "type": "str"}],
        }),
        serde_json::json!({
            "id": "pairs", "type": "parser", "input": "body",
            "kind": {"type": "keyValue", "pairSep": " ", "kvSep": "=", "quote": "\""},
            "declared": DECLARED
                .iter()
                .copied()
                .chain(REST.iter().map(|k| (*k, "str")))
                .map(|(n, t)| serde_json::json!({"name": n, "type": t}))
                .collect::<Vec<_>>(),
        }),
        // Before the rename, which moves `srcip` to `source.ip`.
        serde_json::json!({
            "id": "asset", "type": "lookup", "input": "srcip", "table": "assets",
            "matchKind": "exact", "multiplicity": "first", "keyColumn": "ip",
            "outputs": [
                {"source": "hostname", "field": "source.asset.hostname"},
                {"source": "owner", "field": "source.asset.owner"},
                {"source": "criticality", "field": "source.asset.criticality"},
                {"source": "os", "field": "source.asset.os"},
            ],
        }),
        serde_json::json!({
            "id": "derive", "type": "eval",
            "assignments": evals.iter().map(|(t, e)| serde_json::json!({"target": t, "expr": e})).collect::<Vec<_>>(),
        }),
        serde_json::json!({
            "id": "to-ecs", "type": "rename",
            "pairs": renames
                .iter()
                .map(|(f, t)| serde_json::json!({"from": f, "to": t}))
                .chain(REST.iter().map(|k| serde_json::json!({"from": k, "to": format!("fortinet.{k}")})))
                .collect::<Vec<_>>(),
        }),
        serde_json::json!({
            "id": "prune", "type": "eval",
            "assignments": dropped.iter().map(|n| serde_json::json!({"target": n, "expr": "null"})).collect::<Vec<_>>(),
        }),
    ]
}

/// The bundle, after writing the inventory file it names.
fn bundle() -> String {
    let path = super::lookup_file("assets.csv", assets_csv().as_bytes());
    let table = serde_json::json!({
        "id": "assets", "type": "csv", "path": path, "delimiter": ",",
        "headers": ["ip", "hostname", "owner", "criticality", "os"],
    });
    super::bundle(steps(), vec![table])
}

// -- the check ----------------------------------------------------------------

fn check(rows: u64, docs: u64, kept: &[serde_json::Value]) {
    assert_eq!(docs, rows, "no step drops a row");

    // The corpus mixes hits and misses the way an inventory does.
    let hits = (0..2_000)
        .filter(|&i| source_host(i).is_some_and(|h| h < ASSETS))
        .count();
    assert!(
        (1_200..1_800).contains(&hits),
        "{hits} of 2000 sources in the inventory"
    );

    // The sink keeps the first rows in order, so each document is checked
    // against the record it came from.
    let mut hit = 0;
    for (i, doc) in kept.iter().enumerate() {
        let i = i as u64;
        let at = |p: &str| doc.pointer(p).unwrap_or(&serde_json::Value::Null);
        let k = kind(i);

        assert_eq!(at("/observer/vendor"), "Fortinet", "{doc}");
        assert!(
            at("/observer/name").as_str().is_some_and(|n| n.starts_with("FGT-")),
            "{doc}"
        );
        let ts = at("/@timestamp").as_str().unwrap_or_default();
        assert!(ts.starts_with("2026-"), "@timestamp is the parsed date and time: {doc}");
        let dataset = if matches!(k, Kind::Forward | Kind::Local) {
            "fortinet.traffic"
        } else {
            "fortinet.utm"
        };
        assert_eq!(at("/event/dataset"), dataset, "{doc}");
        assert!(at("/source/port").is_i64(), "source.port is a number: {doc}");
        assert!(at("/destination/port").is_i64(), "destination.port is a number: {doc}");
        let transport = at("/network/transport").as_str();
        assert!(matches!(transport, Some("tcp" | "udp")), "proto mapped: {doc}");
        assert!(k == Kind::Forward || transport == Some("tcp"), "{doc}");
        assert!(at("/rule/id").is_string(), "rule.id stays text: {doc}");
        assert!(
            at("/fortinet/sessionid").is_string(),
            "unmoved keys under fortinet: {doc}"
        );
        assert!(at("/fortinet/vd") == "root", "{doc}");
        if k == Kind::Forward {
            let ns = at("/event/duration").as_i64().expect("event.duration");
            assert_eq!(ns % 1_000_000_000, 0, "duration in nanoseconds: {doc}");
            assert!(at("/source/bytes").is_i64(), "{doc}");
        }

        for gone in ["/_raw", "/body", "/srcip", "/date", "/proto", "/sentbyte"] {
            assert!(doc.pointer(gone).is_none(), "{gone} left the document: {doc}");
        }

        match source_host(i) {
            Some(h) => {
                let ip = host_ip(h);
                assert_eq!(at("/source/ip"), ip.as_str(), "{doc}");
                if h < ASSETS {
                    hit += 1;
                    let row = asset(h);
                    assert_eq!(at("/source/asset/hostname"), row[1].as_str(), "{doc}");
                    assert_eq!(at("/source/asset/owner"), row[2].as_str(), "{doc}");
                    assert_eq!(at("/source/asset/criticality"), row[3].as_str(), "{doc}");
                    assert_eq!(at("/source/asset/os"), row[4].as_str(), "{doc}");
                } else {
                    assert!(doc.pointer("/source/asset").is_none(), "an unknown host misses: {doc}");
                }
            }
            None => assert!(
                doc.pointer("/source/asset").is_none(),
                "an outside source misses: {doc}"
            ),
        }
    }
    assert!(hit > 0, "no kept document hit the inventory");
}
