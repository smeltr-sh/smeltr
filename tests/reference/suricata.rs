//! Suricata's EVE JSON log, one event per line, normalized to ECS and
//! enriched the way a SOC enriches network telemetry.
//!
//! Each `event_type` -- alert, flow, dns, http, tls, fileinfo, smtp --
//! carries an object of its own shape, some with arrays of objects, base64
//! payloads and text that is not ASCII. The pipeline moves what Suricata
//! sent under `suricata.eve`, maps each event type to ECS in a branch of
//! its own, then enriches both ends: GeoIP on both addresses, the company's
//! networks by CIDR, and the domain by an exact list and a regex list. The
//! field names are Suricata's EVE format's; the values are synthetic.

use base64::Engine as _;
use serde_json::{Value, json};

use super::{Clock, Pipeline, mix, people, pick};

pub const PIPELINE: Pipeline = Pipeline {
    name: "suricata",
    line,
    bundle,
    check,
};

// -- the hosts ----------------------------------------------------------------

/// Internal services, by role, as `<role>.<site>.acme.internal`.
const SERVICES: &[&str] = &["git", "wiki", "ci", "vault", "jira", "mail", "files", "sso"];

/// Outside domains the sensors see, with the category and owner the
/// company's domain list gives each.
const DOMAINS: &[(&str, &str, &str)] = &[
    ("login.idp.example", "identity", "it-ops"),
    ("api.payments.example", "finance", "finance"),
    ("crm.salescloud.example", "saas", "sales"),
    ("chat.teamtalk.example", "collaboration", "it-ops"),
    ("meet.teamtalk.example", "collaboration", "it-ops"),
    ("cdn.fastedge.example", "cdn", "engineering"),
    ("updates.osvendor.example", "software-update", "it-ops"),
    ("pkg.langrepo.example", "software-update", "engineering"),
    ("docs.office.example", "productivity", "it-ops"),
    ("mail.office.example", "email", "it-ops"),
    ("storage.cloudbox.example", "file-sharing", "it-ops"),
    ("telemetry.agent.example", "monitoring", "security"),
    ("news.daily.example", "news", ""),
    ("video.stream.example", "streaming", ""),
];

/// Domains no list names: the long tail of the web, and a few a malware
/// family generates.
const UNLISTED: &[&str] = &[
    "shop.tinystore.example",
    "blog.someone.example",
    "free-wallpapers.example",
    "paste.anon.example",
];

/// The regex list, first match wins: what a domain is, by its shape.
const PATTERNS: &[(&str, &str, &str)] = &[
    (r"\.acme\.internal$", "internal", "company domain"),
    (r"^[a-z0-9]{12,}\.(top|xyz|click)$", "suspicious", "generated name"),
    (
        r"(^|\.)(cloudbox|paste\.anon)\.example$",
        "data-exfil-risk",
        "file sharing",
    ),
    (r"(^|\.)(osvendor|langrepo)\.example$", "trusted", "software update"),
    (r"\.example$", "external", "internet"),
];

/// Internal client `k`: one of the company's `10.<n>.0.0/16` networks.
fn client(k: u64) -> String {
    let n = k % people::NETWORKS;
    format!("10.{n}.{}.{}", (k >> 8) % 256, 1 + (k >> 16) % 254)
}

/// A domain for draw `r`: mostly listed, some internal, the rest unlisted
/// or generated.
fn domain(r: u64) -> String {
    match r % 100 {
        0..55 => pick(DOMAINS, r >> 8).0.to_string(),
        55..80 => format!(
            "{}.{}.acme.internal",
            pick(SERVICES, r >> 8),
            people::site_of(r >> 16).0
        ),
        80..94 => pick(UNLISTED, r >> 8).to_string(),
        _ => format!("{:x}.{}", mix(r) >> 8, pick(&["top", "xyz", "click"], r >> 12)),
    }
}

/// The server a domain resolves to: inside for internal names, otherwise an
/// internet address.
fn server(domain: &str, r: u64) -> String {
    if domain.ends_with(".acme.internal") {
        client(r)
    } else {
        pick(people::PUBLIC, r).0.to_string()
    }
}

// -- the lookup files ---------------------------------------------------------

fn csv(header: &str, rows: impl Iterator<Item = Vec<String>>) -> String {
    let mut out = format!("{header}\n");
    for r in rows {
        // A field with a comma in it, as a regex's `{12,}`, is quoted.
        let quoted: Vec<String> = r
            .into_iter()
            .map(|f| if f.contains(',') { format!("\"{f}\"") } else { f })
            .collect();
        out.push_str(&quoted.join(","));
        out.push('\n');
    }
    out
}

fn tables() -> Vec<Value> {
    let (_, networks) = people::networks();
    let networks = csv("network,site,city,tier,business_unit", networks.into_iter());
    let domains = csv(
        "domain,category,owner",
        DOMAINS
            .iter()
            .map(|(d, c, o)| vec![d.to_string(), c.to_string(), o.to_string()]),
    );
    let patterns = csv(
        "pattern,verdict,reason",
        PATTERNS
            .iter()
            .map(|(p, v, r)| vec![p.to_string(), v.to_string(), r.to_string()]),
    );
    let file = |id: &str, name: &str, body: &str| json!({"id": id, "type": "csv", "path": super::lookup_file(name, body.as_bytes()), "delimiter": ","});
    // The repository's test database knows two private networks and costs a lookup little. A benchmark can name a
    // real City database instead, which the repository may not carry under its license; the check then does not hold.
    let geoip = match std::env::var("BENCH_GEOIP") {
        Ok(path) => super::lookup_file("city.mmdb", &std::fs::read(&path).expect("the BENCH_GEOIP database")),
        Err(_) => super::lookup_file(
            "city.mmdb",
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/lookup/test-data/test-city.mmdb"
            )),
        ),
    };
    vec![
        json!({"id": "geoip", "type": "mmdb", "path": geoip}),
        file("networks", "networks.csv", &networks),
        file("domains", "domains.csv", &domains),
        file("patterns", "domain-patterns.csv", &patterns),
    ]
}

// -- the record ---------------------------------------------------------------

/// What event `i` is: a sensor's mix, mostly flows and DNS.
fn event_type(i: u64) -> &'static str {
    match mix(i ^ 0xE7E) % 100 {
        0..34 => "flow",
        34..58 => "dns",
        58..72 => "tls",
        72..84 => "http",
        84..91 => "alert",
        91..96 => "fileinfo",
        _ => "smtp",
    }
}

/// Suricata's own timestamp: microseconds and a `+0000` offset.
fn stamp(c: &Clock, plus_us: u64) -> String {
    let at = Clock::at(c.unix + (c.nanos as u64 / 1000 + plus_us) as i64 / 1_000_000, 0);
    format!(
        "{}-{:02}-{:02}T{}.{:06}+0000",
        at.year,
        at.month,
        at.day,
        at.hms(),
        (c.nanos as u64 / 1000 + plus_us) % 1_000_000
    )
}

fn hex(r: u64, bytes: usize) -> String {
    (0..bytes)
        .map(|k| format!("{:02x}", mix(r ^ k as u64) & 0xff))
        .collect()
}

/// A flow's transport, port and the protocol Suricata recognized on it; `failed` where it recognized none.
const FLOWS: &[(&str, u64, &str)] = &[
    ("TCP", 443, "tls"),
    ("TCP", 443, "tls"),
    ("TCP", 443, "tls"),
    ("TCP", 80, "http"),
    ("UDP", 53, "dns"),
    ("UDP", 53, "dns"),
    ("TCP", 22, "ssh"),
    ("TCP", 3389, "rdp"),
    ("UDP", 123, "ntp"),
    ("TCP", 8443, "failed"),
];

const AGENTS: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "curl/8.9.1",
    "python-requests/2.32.3",
    "Microsoft-CryptoAPI/10.0",
    "Go-http-client/1.1",
];

const PATHS: &[&str] = &[
    "/",
    "/index.html",
    "/api/v2/items?page=2&sort=desc",
    "/static/app.3f9a1c.js",
    "/download/Rechnung_März_2026.pdf",
    "/search?q=caf%C3%A9+near+me",
    "/wp-login.php",
    "/update/check?os=win&arch=x64&channel=stable",
];

const FILES: &[&str] = &[
    "setup.exe",
    "Rechnung_März_2026.pdf",
    "quarterly-report.xlsx",
    "预算.docx",
    "invoice #4471.zip",
];

const SUBJECTS: &[&str] = &[
    "Q3 planning",
    "Réunion budget — T3",
    "Your invoice is ready",
    "Urgent: password expires today",
    "Re: Angebot für Wartungsvertrag",
];

/// Signatures, with their category and severity. Local rules: sids from
/// the 1,000,000 range Suricata leaves to a site's own.
const SIGNATURES: &[(u64, &str, &str, i64)] = &[
    (
        1_000_101,
        "LOCAL POLICY Outbound connection to generated domain name",
        "Potentially Bad Traffic",
        2,
    ),
    (
        1_000_102,
        "LOCAL MALWARE Beacon pattern to known C2 path",
        "A Network Trojan was detected",
        1,
    ),
    (
        1_000_103,
        "LOCAL POLICY Executable download over HTTP",
        "Potential Corporate Privacy Violation",
        2,
    ),
    (
        1_000_104,
        "LOCAL SCAN Inbound SSH brute force",
        "Attempted Administrator Privilege Gain",
        1,
    ),
    (
        1_000_105,
        "LOCAL INFO Cleartext credentials in HTTP request",
        "Misc activity",
        3,
    ),
];

fn http_object(host: &str, r: u64, with_headers: bool) -> Value {
    let path = pick(PATHS, r);
    let status = pick(&[200, 200, 200, 204, 301, 304, 403, 404, 500], r >> 8);
    let mut http = json!({
        "hostname": host,
        "url": path,
        "http_user_agent": pick(AGENTS, r >> 16),
        "http_content_type": pick(&["text/html", "application/json", "application/octet-stream"], r >> 20),
        "http_method": pick(&["GET", "GET", "GET", "POST", "PUT"], r >> 24),
        "protocol": "HTTP/1.1",
        "status": status,
        "length": (r >> 28) % 200_000,
    });
    if r.is_multiple_of(3) {
        http["http_refer"] = json!(format!("https://{host}/"));
    }
    if with_headers {
        http["request_headers"] = json!([
            {"name": "Host", "value": host},
            {"name": "User-Agent", "value": http["http_user_agent"]},
            {"name": "Accept", "value": "*/*"},
            {"name": "Accept-Language", "value": "de-DE,de;q=0.9,en;q=0.8"},
        ]);
        http["response_headers"] = json!([
            {"name": "Content-Type", "value": http["http_content_type"]},
            {"name": "Server", "value": "nginx"},
            {"name": "Cache-Control", "value": "no-store"},
        ]);
    }
    http
}

fn flow_object(c: &Clock, r: u64, alerted: bool) -> Value {
    let (to_s, to_c) = (1 + r % 400, 1 + (r >> 9) % 900);
    json!({
        "pkts_toserver": to_s,
        "pkts_toclient": to_c,
        "bytes_toserver": to_s * (60 + (r >> 20) % 1400),
        "bytes_toclient": to_c * (60 + (r >> 30) % 1400),
        "start": stamp(c, 0),
        "end": stamp(c, (r >> 40) % 120_000_000),
        "age": (r >> 40) % 120,
        "state": pick(&["closed", "established", "new"], r >> 44),
        "reason": pick(&["timeout", "shutdown"], r >> 48),
        "alerted": alerted,
    })
}

/// Event `i`.
pub fn event(i: u64) -> Value {
    let c = Clock::of(i);
    let (a, b, r) = (mix(i), mix(i ^ 0x5EB1), mix(i.wrapping_mul(0x9E37)));
    let kind = event_type(i);
    let host = domain(b);
    let src = client(a);
    let dst = if kind == "dns" {
        format!("10.{}.0.53", (a >> 4) % people::NETWORKS)
    } else {
        server(&host, b >> 8)
    };
    let (proto, dport, app) = match kind {
        "dns" => ("UDP", 53, "dns"),
        "tls" => ("TCP", 443, "tls"),
        "http" | "fileinfo" => ("TCP", pick(&[80, 8080], r), "http"),
        "smtp" => ("TCP", 25, "smtp"),
        "alert" => ("TCP", pick(&[80, 443, 22], r), pick(&["http", "tls", "ssh"], r)),
        _ => pick(FLOWS, r >> 4),
    };
    let mut e = json!({
        "timestamp": stamp(&c, 0),
        "flow_id": mix(i ^ 0xF10) >> 12,
        "in_iface": pick(&["eth1", "eth2", "ens5f0"], a >> 40),
        "vlan": [100 + (a >> 50) % 20],
        "event_type": kind,
        "src_ip": src,
        "src_port": 1024 + (a >> 20) % 64_000,
        "dest_ip": dst,
        "dest_port": dport,
        "proto": proto,
        "app_proto": app,
        "community_id": format!("1:{}", base64::engine::general_purpose::STANDARD.encode(hex(a ^ b, 10))),
        "host": format!("ids-{}-01", people::site_of(a >> 8).0),
    });
    match kind {
        "flow" => {
            e["flow"] = flow_object(&c, r, false);
            if proto == "TCP" {
                e["tcp"] = json!({"tcp_flags": "1b", "tcp_flags_ts": "1b", "tcp_flags_tc": "1b",
                    "syn": true, "fin": true, "psh": true, "ack": true, "state": "closed"});
            }
        }
        "dns" if r.is_multiple_of(2) => {
            e["dns"] =
                json!({"type": "query", "id": r % 65_536, "rrname": host, "rrtype": "A", "tx_id": 0, "opcode": 0});
        }
        "dns" => {
            let ip = server(&host, b >> 8);
            let nx = r % 10 == 1;
            let mut dns = json!({
                "version": 2, "type": "answer", "id": r % 65_536, "flags": if nx { "8183" } else { "8180" },
                "qr": true, "rd": true, "ra": true, "opcode": 0,
                "rrname": host, "rrtype": "A", "rcode": if nx { "NXDOMAIN" } else { "NOERROR" },
            });
            if !nx {
                let edge = format!("edge-{}.cdn.fastedge.example", r % 40);
                dns["answers"] = json!([
                    {"rrname": host, "rrtype": "CNAME", "ttl": 300, "rdata": edge},
                    {"rrname": edge, "rrtype": "A", "ttl": 60, "rdata": ip},
                ]);
                dns["grouped"] = json!({"A": [ip], "CNAME": [edge]});
            }
            e["dns"] = dns;
        }
        "tls" => {
            e["tls"] = json!({
                "subject": format!("CN={host}"),
                "issuerdn": pick(&["C=US, O=Example Trust Services, CN=Example TLS RSA CA G1", "CN=acme-internal-ca"], r),
                "serial": hex(r, 8).to_uppercase(),
                "fingerprint": hex(r >> 3, 20),
                "sni": host,
                "version": pick(&["TLS 1.3", "TLS 1.3", "TLS 1.2"], r >> 8),
                "notbefore": "2026-01-01T00:00:00",
                "notafter": "2026-12-31T23:59:59",
                "ja3": {"hash": hex(r >> 5, 16), "string": "771,4865-4866-4867-49195-49199,0-23-65281-10-11-35-16-5-13-18-51-45-43-27,29-23-24,0"},
                "ja3s": {"hash": hex(r >> 7, 16), "string": "771,4865,51-43"},
            });
        }
        "http" => {
            e["tx_id"] = json!(r % 4);
            e["http"] = http_object(&host, r, r.is_multiple_of(4));
        }
        "fileinfo" => {
            let name = pick(FILES, r);
            e["http"] = http_object(&host, r, false);
            e["fileinfo"] = json!({
                "filename": format!("/download/{name}"), "gaps": false, "state": "CLOSED",
                "md5": hex(r, 16), "sha1": hex(r >> 1, 20), "sha256": hex(r >> 2, 32),
                "stored": false, "size": 1_000 + (r >> 20) % 5_000_000, "tx_id": 0,
            });
        }
        "smtp" => {
            let who = people::person(a);
            let from = format!("{}@{}", pick(&["billing", "noreply", "hr"], r), pick(DOMAINS, r >> 4).0);
            e["smtp"] = json!({"helo": format!("mx{}.{}", r % 4, pick(DOMAINS, r >> 4).0),
                "mail_from": format!("<{from}>"), "rcpt_to": [format!("<{}@acme.example>", who.login)]});
            e["email"] = json!({
                "status": "PARSE_DONE",
                "from": format!("\"{}\" <{from}>", pick(&["Billing", "Ärzte Service", "IT Helpdesk"], r >> 8)),
                "to": [format!("\"{}\" <{}@acme.example>", who.full_name, who.login)],
                "subject": pick(SUBJECTS, r >> 12),
                "attachment": [pick(FILES, r >> 16)],
                "url": [format!("https://{host}/track?id={}", hex(r, 6))],
            });
        }
        _ => {
            let (sid, sig, cat, sev) = pick(SIGNATURES, r >> 4);
            e["tx_id"] = json!(0);
            e["alert"] = json!({
                "action": if sev == 1 && r.is_multiple_of(2) { "blocked" } else { "allowed" },
                "gid": 1, "signature_id": sid, "rev": 1 + r % 5, "signature": sig, "category": cat, "severity": sev,
                "metadata": {"created_at": ["2025_06_01"], "signature_severity": [pick(&["Major", "Minor"], r)]},
            });
            e["direction"] = json!("to_server");
            e["flow"] = flow_object(&c, r, true);
            let request = format!(
                "GET {} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: {}\r\n\r\n",
                pick(PATHS, r >> 8),
                pick(AGENTS, r >> 16)
            );
            e["payload"] = json!(base64::engine::general_purpose::STANDARD.encode(&request));
            e["payload_printable"] = json!(request);
            e["stream"] = json!(1);
            if app == "http" {
                e["http"] = http_object(&host, r, false);
            }
        }
    }
    e
}

pub fn line(i: u64) -> String {
    event(i).to_string()
}

// -- the pipeline -------------------------------------------------------------

/// What Suricata sends, each event type's object moved whole under
/// `suricata.eve` so the ECS objects of the same names start empty.
const EVE_OBJECTS: &[&str] = &[
    "alert",
    "flow",
    "tcp",
    "dns",
    "http",
    "tls",
    "fileinfo",
    "smtp",
    "email",
    "vlan",
    "tx_id",
    "direction",
    "payload",
    "payload_printable",
    "stream",
    "host",
];

fn steps() -> Vec<Value> {
    let s = |n: &str| json!({"name": n, "type": "str"});
    let i = |n: &str| json!({"name": n, "type": "i64"});
    let open =
        |n: &str, fields: Vec<Value>| json!({"name": n, "type": {"type": "struct", "open": true, "fields": fields}});
    let parse = json!({
        "id": "eve", "type": "parser", "input": "_raw", "kind": {"type": "json"},
        "declared": [
            {"name": "timestamp", "type": {"type": "datetime", "format": "%Y-%m-%dT%H:%M:%S.%f%z"}},
            s("event_type"), i("flow_id"), s("in_iface"), s("src_ip"), i("src_port"), s("dest_ip"), i("dest_port"),
            s("proto"), s("app_proto"), s("community_id"),
            open("alert", vec![s("action"), i("signature_id"), s("signature"), s("category"), i("severity")]),
            open("flow", vec![i("pkts_toserver"), i("pkts_toclient"), i("bytes_toserver"), i("bytes_toclient"), s("start"), s("end")]),
            open("dns", vec![s("type"), i("id"), s("rrname"), s("rrtype"), s("rcode")]),
            open("http", vec![s("hostname"), s("url"), s("http_user_agent"), s("http_method"), i("status"), i("length")]),
            open("tls", vec![s("sni"), s("version"), s("subject"), s("issuerdn")]),
            open("fileinfo", vec![s("filename"), i("size"), s("md5"), s("sha1"), s("sha256")]),
            open("smtp", vec![s("helo"), s("mail_from")]),
            open("email", vec![s("subject")]),
        ],
    });
    let eval = |id: &str, sets: &[(&str, &str)]| {
        json!({"id": id, "type": "eval",
            "assignments": sets.iter().map(|(t, e)| json!({"target": t, "expr": e})).collect::<Vec<_>>()})
    };
    let branch = |kind: &str, sets: &[(&str, &str)]| {
        let mut step = eval(kind, sets);
        step["filter"] = json!(format!("event_type == \"{kind}\""));
        step
    };
    let eve = "suricata.eve";
    let to_eve = json!({"id": "to-eve", "type": "rename",
        "pairs": EVE_OBJECTS.iter().map(|o| json!({"from": o, "to": format!("{eve}.{o}")})).collect::<Vec<_>>()});
    let common = eval(
        "common",
        &[
            ("@timestamp", "timestamp"),
            ("event.module", r#""suricata""#),
            ("event.dataset", r#""suricata.eve""#),
            ("event.kind", r#"event_type == "alert" ? "alert" : "event""#),
            (
                "event.category",
                r#"event_type == "alert" ? "intrusion_detection" : "network""#,
            ),
            ("source.ip", "src_ip"),
            ("source.port", "src_port"),
            ("destination.ip", "dest_ip"),
            ("destination.port", "dest_port"),
            ("network.transport", "proto.toLowerCase()"),
            ("network.protocol", r#"app_proto == "failed" ? null : app_proto"#),
            ("network.community_id", "community_id"),
            ("observer.ingress.interface.name", "in_iface"),
            ("observer.hostname", "suricata.eve.host"),
            ("suricata.eve.event_type", "event_type"),
            ("suricata.eve.flow_id", "flow_id"),
            // The name the far end goes by, whichever protocol said it.
            (
                "destination.domain",
                "suricata.eve.http.hostname ?? suricata.eve.tls.sni ?? suricata.eve.dns.rrname",
            ),
        ],
    );
    let flow_counts = [
        ("source.packets", "suricata.eve.flow.pkts_toserver"),
        ("source.bytes", "suricata.eve.flow.bytes_toserver"),
        ("destination.packets", "suricata.eve.flow.pkts_toclient"),
        ("destination.bytes", "suricata.eve.flow.bytes_toclient"),
        (
            "network.packets",
            "suricata.eve.flow.pkts_toserver + suricata.eve.flow.pkts_toclient",
        ),
        (
            "network.bytes",
            "suricata.eve.flow.bytes_toserver + suricata.eve.flow.bytes_toclient",
        ),
    ];
    let http = [
        ("url.domain", "suricata.eve.http.hostname"),
        ("url.original", "suricata.eve.http.url"),
        ("url.path", r#"suricata.eve.http.url.split("?")[0]"#),
        ("http.request.method", "suricata.eve.http.http_method"),
        ("http.response.status_code", "suricata.eve.http.status"),
        ("http.response.body.bytes", "suricata.eve.http.length"),
        ("user_agent.original", "suricata.eve.http.http_user_agent"),
    ];
    let mut alert = vec![
        ("rule.id", "suricata.eve.alert.signature_id"),
        ("rule.name", "suricata.eve.alert.signature"),
        ("rule.category", "suricata.eve.alert.category"),
        ("event.severity", "suricata.eve.alert.severity"),
        ("event.action", "suricata.eve.alert.action"),
        (
            "event.outcome",
            r#"suricata.eve.alert.action == "blocked" ? "failure" : "success""#,
        ),
    ];
    alert.extend(flow_counts);
    let mut flow = vec![
        ("event.start", "parseTime(suricata.eve.flow.start)"),
        ("event.end", "parseTime(suricata.eve.flow.end)"),
    ];
    flow.extend(flow_counts);
    let mut fileinfo = vec![
        ("file.name", r#"suricata.eve.fileinfo.filename.split("/")[2]"#),
        ("file.size", "suricata.eve.fileinfo.size"),
        ("file.hash.md5", "suricata.eve.fileinfo.md5"),
        ("file.hash.sha1", "suricata.eve.fileinfo.sha1"),
        ("file.hash.sha256", "suricata.eve.fileinfo.sha256"),
    ];
    fileinfo.extend(http);

    let geo = |side: &str| {
        json!({"id": format!("{side}-geo"), "type": "lookup", "input": format!("{side}.ip"), "table": "geoip",
        "matchKind": "cidr", "multiplicity": "best",
        "outputs": [
            {"source": "country.iso_code", "field": format!("{side}.geo.country_iso_code")},
            {"source": "city.names.en", "field": format!("{side}.geo.city_name")},
            {"source": "location.latitude", "field": format!("{side}.geo.location.lat"), "type": "f64"},
            {"source": "location.longitude", "field": format!("{side}.geo.location.lon"), "type": "f64"},
        ]})
    };
    let network = |side: &str| {
        json!({"id": format!("{side}-network"), "type": "lookup", "input": format!("{side}.ip"), "table": "networks",
        "matchKind": "cidr", "multiplicity": "best", "keyColumn": "network",
        "outputs": [
            {"source": "site", "field": format!("{side}.site")},
            {"source": "tier", "field": format!("{side}.network_tier")},
            {"source": "business_unit", "field": format!("{side}.business_unit")},
        ]})
    };
    vec![
        parse,
        to_eve,
        common,
        branch("alert", &alert),
        branch("flow", &flow),
        branch(
            "dns",
            &[
                ("dns.type", "suricata.eve.dns.type"),
                ("dns.id", "suricata.eve.dns.id"),
                ("dns.question.name", "suricata.eve.dns.rrname"),
                ("dns.question.type", "suricata.eve.dns.rrtype"),
                ("dns.response_code", "suricata.eve.dns.rcode"),
            ],
        ),
        branch("http", &http),
        branch(
            "tls",
            &[
                ("tls.client.server_name", "suricata.eve.tls.sni"),
                ("tls.version_protocol", r#""tls""#),
                ("tls.version", r#"suricata.eve.tls.version.split(" ")[1]"#),
                ("tls.server.subject", "suricata.eve.tls.subject"),
                ("tls.server.issuer", "suricata.eve.tls.issuerdn"),
                // Undeclared: the GUI declares one level of an object's fields.
                ("tls.client.ja3", "$.suricata.eve.tls.ja3.hash"),
            ],
        ),
        branch("fileinfo", &fileinfo),
        branch(
            "smtp",
            &[
                // SMTP's `<address>`, without the brackets.
                ("email.from.address", "suricata.eve.smtp.mail_from.slice(1, -1)"),
                ("email.subject", "suricata.eve.email.subject"),
                ("source.domain", "suricata.eve.smtp.helo"),
            ],
        ),
        geo("source"),
        geo("destination"),
        network("source"),
        network("destination"),
        json!({"id": "domain-list", "type": "lookup", "input": "destination.domain", "table": "domains",
        "filter": "!isNull(destination.domain)",
        "matchKind": "exact", "multiplicity": "first", "keyColumn": "domain",
        "outputs": [
            {"source": "category", "field": "destination.domain_category"},
            {"source": "owner", "field": "destination.domain_owner"},
        ]}),
        json!({"id": "domain-patterns", "type": "lookup", "input": "destination.domain", "table": "patterns",
        "filter": "!isNull(destination.domain)",
        "matchKind": "regex", "multiplicity": "first", "keyColumn": "pattern",
        "outputs": [
            {"source": "verdict", "field": "destination.domain_verdict"},
            {"source": "reason", "field": "destination.domain_reason"},
        ]}),
        eval(
            "prune",
            &[
                "_raw",
                "timestamp",
                "event_type",
                "flow_id",
                "in_iface",
                "src_ip",
                "src_port",
                "dest_ip",
                "dest_port",
                "proto",
                "app_proto",
                "community_id",
            ]
            .map(|n| (n, "null")),
        ),
    ]
}

fn bundle() -> String {
    super::bundle(steps(), tables())
}

// -- the check ----------------------------------------------------------------

/// Each kept document is event `i` of the corpus, in order. Every event type,
/// and a hit and a miss on every lookup, must be among them.
fn check(rows: u64, docs: u64, kept: &[Value]) {
    assert_eq!(docs, rows, "one document per event");
    let mut seen = std::collections::BTreeSet::new();
    let mut hits = std::collections::BTreeMap::<&str, (u32, u32)>::new();
    let mut count = |name: &'static str, hit: bool| {
        let e = hits.entry(name).or_default();
        if hit { e.0 += 1 } else { e.1 += 1 }
    };
    for (i, d) in kept.iter().enumerate() {
        let e = event(i as u64);
        let at = |p: &str| d.pointer(p).cloned().unwrap_or(Value::Null);
        let kind = e["event_type"].as_str().unwrap();
        seen.insert(kind.to_string());
        assert!(d.get("error").is_none(), "row {i} mapped with an error: {d}");
        let ts = at("/@timestamp");
        assert!(
            ts.as_str()
                .is_some_and(|t| t.starts_with(&e["timestamp"].as_str().unwrap()[..19])),
            "row {i}: {ts}"
        );
        assert_eq!(at("/source/ip"), e["src_ip"], "row {i}");
        assert_eq!(at("/destination/port"), e["dest_port"], "row {i}");
        assert_eq!(
            at("/network/transport"),
            e["proto"].as_str().unwrap().to_lowercase(),
            "row {i}"
        );
        assert_eq!(
            at("/event/kind"),
            if kind == "alert" { "alert" } else { "event" },
            "row {i}"
        );
        assert_eq!(at("/suricata/eve/event_type"), kind, "row {i}");
        for gone in ["_raw", "src_ip", "event_type", "timestamp", "alert", "flow", "fileinfo"] {
            assert!(d.get(gone).is_none(), "row {i}: {gone} is moved or pruned: {d}");
        }
        // What the mapping does not read rides along as Suricata sent it.
        for o in EVE_OBJECTS {
            assert_eq!(
                at(&format!("/suricata/eve/{o}")),
                e.get(*o).cloned().unwrap_or(Value::Null),
                "row {i}: {o}"
            );
        }
        match kind {
            "alert" => {
                assert_eq!(at("/rule/id"), e["alert"]["signature_id"], "row {i}");
                assert_eq!(at("/rule/name"), e["alert"]["signature"], "row {i}");
                assert_eq!(at("/event/severity"), e["alert"]["severity"], "row {i}");
            }
            "flow" => {
                let f = &e["flow"];
                let sum = f["bytes_toserver"].as_i64().unwrap() + f["bytes_toclient"].as_i64().unwrap();
                assert_eq!(at("/network/bytes").as_i64(), Some(sum), "row {i}");
                assert!(at("/event/end").is_string(), "row {i}: {d}");
            }
            "dns" => {
                assert_eq!(at("/dns/question/name"), e["dns"]["rrname"], "row {i}");
                assert_eq!(at("/dns/type"), e["dns"]["type"], "row {i}");
            }
            "http" => {
                assert_eq!(at("/url/domain"), e["http"]["hostname"], "row {i}");
                assert_eq!(at("/http/response/status_code"), e["http"]["status"], "row {i}");
                assert_eq!(at("/user_agent/original"), e["http"]["http_user_agent"], "row {i}");
            }
            "tls" => {
                assert_eq!(at("/tls/client/server_name"), e["tls"]["sni"], "row {i}");
                assert_eq!(at("/tls/client/ja3"), e["tls"]["ja3"]["hash"], "row {i}");
                assert_eq!(
                    at("/tls/version"),
                    e["tls"]["version"].as_str().unwrap()[4..],
                    "row {i}"
                );
            }
            "fileinfo" => {
                assert_eq!(at("/file/hash/sha256"), e["fileinfo"]["sha256"], "row {i}");
                assert_eq!(
                    at("/file/name"),
                    e["fileinfo"]["filename"]
                        .as_str()
                        .unwrap()
                        .trim_start_matches("/download/"),
                    "row {i}"
                );
            }
            _ => {
                assert_eq!(at("/email/subject"), e["email"]["subject"], "row {i}");
                assert_eq!(
                    at("/email/from/address"),
                    e["smtp"]["mail_from"].as_str().unwrap().trim_matches(['<', '>']),
                    "row {i}"
                );
            }
        }
        let domain = at("/destination/domain");
        if let Some(name) = domain.as_str() {
            let listed = DOMAINS.iter().find(|(d, ..)| *d == name);
            assert_eq!(
                at("/destination/domain_category").as_str(),
                listed.map(|l| l.1),
                "row {i}: {name}"
            );
            count("domain list", listed.is_some());
            assert!(
                at("/destination/domain_verdict").is_string(),
                "row {i}: every name matches a pattern: {name}"
            );
            count("generated name", at("/destination/domain_reason") == "generated name");
        }
        let net = |ip: &Value| ip.as_str().is_some_and(|ip| ip.starts_with("10."));
        assert_eq!(at("/source/site").is_string(), net(&e["src_ip"]), "row {i}");
        count("destination network", at("/destination/site").is_string());
        let geo = |ip: &Value| {
            ip.as_str()
                .is_some_and(|ip| ip.starts_with("10.1.") || ip.starts_with("10.2.0."))
        };
        assert_eq!(
            at("/source/geo/country_iso_code").is_string(),
            geo(&e["src_ip"]),
            "row {i}"
        );
        count("source geo", at("/source/geo").is_object());
    }
    for kind in ["alert", "flow", "dns", "http", "tls", "fileinfo", "smtp"] {
        assert!(seen.contains(kind), "the kept rows hold a {kind} event: {seen:?}");
    }
    for (name, (hit, miss)) in hits {
        assert!(
            hit > 0 && miss > 0,
            "{name}: {hit} hits and {miss} misses among the kept rows"
        );
    }
}
