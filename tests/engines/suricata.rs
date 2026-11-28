//! What the Suricata pipeline (`tests/reference/suricata.rs`) makes of an EVE event, as every engine must make it:
//! the event mapped to ECS, what the mapping does not read kept under `suricata.eve`, and both ends enriched from the
//! same four lookup files.

use std::net::IpAddr;
use std::sync::OnceLock;

use serde_json::{Map, Value, json};

use super::instant;
use crate::reference::{people, suricata};

/// The fields the mapping reads from the top of the event and then prunes.
const PRUNED: &[&str] = &[
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
];

/// The tables, read once from the files every engine reads.
struct Tables {
    geoip: maxminddb::Reader<Vec<u8>>,
    /// Each network, its prefix length, and the row's site, tier and business unit.
    networks: Vec<(IpAddr, u8, [String; 3])>,
    patterns: Vec<regex::Regex>,
}

fn tables() -> &'static Tables {
    static TABLES: OnceLock<Tables> = OnceLock::new();
    TABLES.get_or_init(|| {
        let geoip = suricata::files()
            .into_iter()
            .find(|f| f.0 == "city.mmdb")
            .expect("the GeoIP database")
            .1;
        let networks = (people::networks().1.into_iter())
            .map(|row| {
                let (net, len) = row[0].split_once('/').expect("a CIDR");
                (
                    net.parse().expect("a network address"),
                    len.parse().expect("a prefix length"),
                    [row[1].clone(), row[3].clone(), row[4].clone()],
                )
            })
            .collect();
        Tables {
            geoip: maxminddb::Reader::from_source(geoip).expect("a GeoIP database"),
            networks,
            patterns: (suricata::PATTERNS.iter())
                .map(|p| regex::Regex::new(p.0).expect("a pattern"))
                .collect(),
        }
    })
}

/// Whether `ip` is in `net/len`.
fn contains(net: IpAddr, len: u8, ip: IpAddr) -> bool {
    match (net, ip) {
        (IpAddr::V4(n), IpAddr::V4(a)) => {
            let mask = u32::MAX.checked_shl(32 - u32::from(len)).unwrap_or(0);
            u32::from(n) & mask == u32::from(a) & mask
        }
        (IpAddr::V6(n), IpAddr::V6(a)) => {
            let mask = u128::MAX.checked_shl(128 - u32::from(len)).unwrap_or(0);
            u128::from(n) & mask == u128::from(a) & mask
        }
        _ => false,
    }
}

/// Sets the field at dotted `path` of `doc` to `value`, making the objects on the way; `None` sets nothing, as an
/// assignment of a missing field does.
fn set(doc: &mut Value, path: &str, value: Option<Value>) {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return;
    };
    let mut at = doc;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        let object = at.as_object_mut().expect("an object on the way");
        if parts.peek().is_none() {
            object.insert(part.to_string(), value);
            return;
        }
        at = object.entry(part).or_insert_with(|| Value::Object(Map::new()));
    }
}

/// Suricata's instant, `+0000` and all, as `instant` writes one.
fn utc(at: &str) -> String {
    instant(&match at.strip_suffix("+0000") {
        Some(at) => format!("{at}Z"),
        None => at.to_string(),
    })
}

/// The ECS document the pipeline makes of `line`.
pub fn expect(line: &str) -> Option<Value> {
    let e: Value = serde_json::from_str(line).unwrap();
    let get = |path: &str| e.pointer(&format!("/{}", path.replace('.', "/"))).cloned();
    let text = |path: &str| get(path).and_then(|v| v.as_str().map(str::to_string));
    let kind = text("event_type").unwrap();
    let mut doc = json!({});

    // What Suricata sent, under `suricata.eve`; what the mapping reads from the top is pruned.
    let mut eve = Map::new();
    for (k, v) in e.as_object().unwrap() {
        if suricata::EVE_OBJECTS.contains(&k.as_str()) {
            eve.insert(k.clone(), v.clone());
        } else if !PRUNED.contains(&k.as_str()) {
            set(&mut doc, k, Some(v.clone()));
        }
    }
    eve.insert("event_type".into(), json!(kind));
    if let Some(flow) = get("flow_id") {
        eve.insert("flow_id".into(), flow);
    }
    set(&mut doc, "suricata.eve", Some(Value::Object(eve)));

    let alert = kind == "alert";
    set(&mut doc, "@timestamp", text("timestamp").map(|t| json!(utc(&t))));
    set(&mut doc, "event.module", Some(json!("suricata")));
    set(&mut doc, "event.dataset", Some(json!("suricata.eve")));
    set(
        &mut doc,
        "event.kind",
        Some(json!(if alert { "alert" } else { "event" })),
    );
    set(
        &mut doc,
        "event.category",
        Some(json!(if alert { "intrusion_detection" } else { "network" })),
    );
    set(&mut doc, "source.ip", get("src_ip"));
    set(&mut doc, "source.port", get("src_port"));
    set(&mut doc, "destination.ip", get("dest_ip"));
    set(&mut doc, "destination.port", get("dest_port"));
    set(
        &mut doc,
        "network.transport",
        text("proto").map(|p| json!(p.to_lowercase())),
    );
    set(&mut doc, "network.protocol", get("app_proto").filter(|p| p != "failed"));
    set(&mut doc, "network.community_id", get("community_id"));
    set(&mut doc, "observer.ingress.interface.name", get("in_iface"));
    set(&mut doc, "observer.hostname", get("host"));
    let domain = get("http.hostname")
        .or_else(|| get("tls.sni"))
        .or_else(|| get("dns.rrname"));
    set(&mut doc, "destination.domain", domain.clone());

    let counts = |doc: &mut Value| {
        let n = |k: &str| get(&format!("flow.{k}")).and_then(|v| v.as_i64());
        set(doc, "source.packets", get("flow.pkts_toserver"));
        set(doc, "source.bytes", get("flow.bytes_toserver"));
        set(doc, "destination.packets", get("flow.pkts_toclient"));
        set(doc, "destination.bytes", get("flow.bytes_toclient"));
        let sum = |a: &str, b: &str| Some(json!(n(a)? + n(b)?));
        set(doc, "network.packets", sum("pkts_toserver", "pkts_toclient"));
        set(doc, "network.bytes", sum("bytes_toserver", "bytes_toclient"));
    };
    let http = |doc: &mut Value| {
        set(doc, "url.domain", get("http.hostname"));
        set(doc, "url.original", get("http.url"));
        set(
            doc,
            "url.path",
            text("http.url").map(|u| json!(u.split('?').next().unwrap())),
        );
        set(doc, "http.request.method", get("http.http_method"));
        set(doc, "http.response.status_code", get("http.status"));
        set(doc, "http.response.body.bytes", get("http.length"));
        set(doc, "user_agent.original", get("http.http_user_agent"));
    };
    match kind.as_str() {
        "alert" => {
            set(&mut doc, "rule.id", get("alert.signature_id"));
            set(&mut doc, "rule.name", get("alert.signature"));
            set(&mut doc, "rule.category", get("alert.category"));
            set(&mut doc, "event.severity", get("alert.severity"));
            set(&mut doc, "event.action", get("alert.action"));
            let blocked = text("alert.action").map(|a| a == "blocked");
            set(
                &mut doc,
                "event.outcome",
                blocked.map(|b| json!(if b { "failure" } else { "success" })),
            );
            counts(&mut doc);
        }
        "flow" => {
            set(&mut doc, "event.start", text("flow.start").map(|t| json!(utc(&t))));
            set(&mut doc, "event.end", text("flow.end").map(|t| json!(utc(&t))));
            counts(&mut doc);
        }
        "dns" => {
            set(&mut doc, "dns.type", get("dns.type"));
            set(&mut doc, "dns.id", get("dns.id"));
            set(&mut doc, "dns.question.name", get("dns.rrname"));
            set(&mut doc, "dns.question.type", get("dns.rrtype"));
            set(&mut doc, "dns.response_code", get("dns.rcode"));
        }
        "http" => http(&mut doc),
        "tls" => {
            set(&mut doc, "tls.client.server_name", get("tls.sni"));
            set(&mut doc, "tls.version_protocol", Some(json!("tls")));
            set(
                &mut doc,
                "tls.version",
                text("tls.version").map(|v| json!(v.split(' ').nth(1).expect("a TLS version"))),
            );
            set(&mut doc, "tls.server.subject", get("tls.subject"));
            set(&mut doc, "tls.server.issuer", get("tls.issuerdn"));
            set(&mut doc, "tls.client.ja3", get("tls.ja3.hash"));
        }
        "fileinfo" => {
            set(
                &mut doc,
                "file.name",
                text("fileinfo.filename").map(|f| json!(f.split('/').nth(2).expect("a file under a folder"))),
            );
            set(&mut doc, "file.size", get("fileinfo.size"));
            set(&mut doc, "file.hash.md5", get("fileinfo.md5"));
            set(&mut doc, "file.hash.sha1", get("fileinfo.sha1"));
            set(&mut doc, "file.hash.sha256", get("fileinfo.sha256"));
            http(&mut doc);
        }
        "smtp" => {
            set(
                &mut doc,
                "email.from.address",
                text("smtp.mail_from").map(|m| {
                    let chars: Vec<char> = m.chars().collect();
                    json!(chars[1..chars.len() - 1].iter().collect::<String>())
                }),
            );
            set(&mut doc, "email.subject", get("email.subject"));
            set(&mut doc, "source.domain", get("smtp.helo"));
        }
        other => panic!("an event type the corpus has none of: {other}"),
    }

    let t = tables();
    for side in ["source", "destination"] {
        let Some(ip) = doc[side]["ip"].as_str().and_then(|ip| ip.parse::<IpAddr>().ok()) else {
            continue;
        };
        let city: Option<Value> = t.geoip.lookup(ip).ok().and_then(|r| r.decode().ok().flatten());
        if let Some(city) = city {
            let at = |p: &str| city.pointer(p).cloned();
            set(
                &mut doc,
                &format!("{side}.geo.country_iso_code"),
                at("/country/iso_code"),
            );
            set(&mut doc, &format!("{side}.geo.city_name"), at("/city/names/en"));
            let float = |p: &str| at(p).and_then(|v| v.as_f64()).map(|f| json!(f));
            set(
                &mut doc,
                &format!("{side}.geo.location.lat"),
                float("/location/latitude"),
            );
            set(
                &mut doc,
                &format!("{side}.geo.location.lon"),
                float("/location/longitude"),
            );
        }
        // The most specific network that holds the address.
        let network = (t.networks.iter())
            .filter(|n| contains(n.0, n.1, ip))
            .max_by_key(|n| n.1);
        if let Some((_, _, [site, tier, unit])) = network {
            set(&mut doc, &format!("{side}.site"), Some(json!(site)));
            set(&mut doc, &format!("{side}.network_tier"), Some(json!(tier)));
            set(&mut doc, &format!("{side}.business_unit"), Some(json!(unit)));
        }
    }
    if let Some(name) = domain.as_ref().and_then(Value::as_str) {
        if let Some((_, category, owner)) = suricata::DOMAINS.iter().find(|d| d.0 == name) {
            set(&mut doc, "destination.domain_category", Some(json!(category)));
            set(&mut doc, "destination.domain_owner", Some(json!(owner)));
        }
        if let Some(at) = t.patterns.iter().position(|p| p.is_match(name)) {
            let (_, verdict, reason) = suricata::PATTERNS[at];
            set(&mut doc, "destination.domain_verdict", Some(json!(verdict)));
            set(&mut doc, "destination.domain_reason", Some(json!(reason)));
        }
    }
    Some(doc)
}

/// An engine's document in the form [`expect`] writes: each instant written as `instant` writes one.
pub fn normal(mut doc: Value) -> Value {
    for path in ["/@timestamp", "/event/start", "/event/end"] {
        if let Some(at) = doc.pointer_mut(path) {
            *at = Value::String(instant(at.as_str().unwrap_or_default()));
        }
    }
    doc
}

/// What smeltr makes of the records an engine is checked on is what [`expect`] says, record for record: the check
/// holds every engine to smeltr's pipeline.
#[test]
fn smeltr_makes_what_the_check_expects() {
    let p = &suricata::PIPELINE;
    let rows = super::CHECK_ROWS;
    let (_, kept) = crate::reference::run(&(p.bundle)(), p.line, rows, 2000, rows as usize);
    let kept = kept.kept();
    assert_eq!(kept.len() as u64, rows, "one document per event");
    for (i, doc) in kept.iter().enumerate() {
        let made = super::canonical(&normal(serde_json::from_slice(doc).unwrap()));
        let want = super::canonical(&expect(&(p.line)(i as u64)).unwrap());
        assert_eq!(made, want, "event {i}: {}", (p.line)(i as u64));
    }
}
