//! The HTTP destination's fuzzers (`type: "http"`, `io::http_post` over `io::http`). Each seed draws one destination
//! as a bundle names it, every option at random, and sometimes breaks one of its settings.
//!
//! Offline, a destination the save accepts loads as its bundle says, loads the same after a save, builds when this
//! worker holds every credential it names and refuses to when it does not, and delivers drawn batches to a service on
//! this host. Every request that service reads is checked against the config: its method and target, the type and
//! encoding its body says it is, each committed header and credential, and its body, decompressed, item for item. The
//! service answers each request at random as an intake would: a 2xx lands every row the request carried, a failure
//! asking to wait is sent again whole, a refusal naming no row is split until the row at fault is alone, and anything
//! else stalls the destination naming the status. Every row lands exactly once or comes back refused. One the save
//! refuses is refused on the setting that was broken. Any other change to its JSON may be refused, and never panics.
//!
//! Live, the same draws, pointed at Splunk's HEC raw endpoint and signed with its token, land there and are searched
//! back.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use serde_json::{Map, Value, json};
use smeltr::config_file::load_str;
use smeltr::io::{Destination, IoError, OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{
    Compression, DestinationKind, HeldSecrets, HttpDestinationConfig, RequestBody, SecretRef, take_zone,
};

use crate::harness::destination::{self, Broken, Pki, Reached, connector_secrets, unknown_key_taken};
use crate::harness::fuzz::Rng;
use crate::harness::http::{self, Answer, Answers, Id, Seen, Sent, Service, fnv, gunzip, json_array_rows};
use crate::live;

const SEEDS: u64 = 350;
const LIVE_SEEDS: u64 = 24;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// A worker-local credential no test sets, so a destination naming it cannot build.
const UNSET_VARIABLE: &str = "SMELTR_FUZZ_NEVER_SET";
/// A worker-local credential every test process has: cargo sets it to the package's name.
const SET_VARIABLE: &str = "CARGO_PKG_NAME";

/// What a leader-stored credential may hold: what HTTP takes as a header value, and what a service's token looks like.
const SECRET_VALUES: &[&str] = &[
    "k-123",
    "Bearer abc.def.ghi",
    "Splunk 00000000-0000-0000-0000-000000000001",
    "tab\tinside",
    "x",
    "Basic dXNlcjpwYXNz",
];

/// Committed header values: plain, spaced, tabbed, punctuated.
const HEADER_VALUES: &[&str] = &["smeltr", "a b c", "tab\there", "v=1; q=\"x\"", "*/*", "0", "{\"k\":1}"];

/// Header names: every byte RFC 9110 allows in one, in mixed case.
const HEADER_NAMES: &[&str] = &[
    "X-Source",
    "x-team",
    "DD-API-KEY",
    "Authorization",
    "X-Odd!#$%&'*+-.^_`|~1",
    "Accept",
    "x-request-tag",
    "X-Splunk-Request-Channel",
];

/// Each header the destination sets itself, by the name it sends.
const OWNED_HEADERS: &[&str] = &[
    "Content-Type",
    "content-encoding",
    "Content-Length",
    "Transfer-Encoding",
    "Host",
    "Cookie",
];

/// Every option the generator knows. Each must be drawn into a destination the save accepts and that delivers to a
/// service checking every request, or into the refusal it names: a break of the save, or a credential the build
/// refuses (`secret.workerLocalUnset`, `secret.lineBreak`), over the seed range.
const OPTIONS: &[&str] = &[
    "url.http",
    "url.https",
    "url.schemeCase",
    "url.localhost",
    "url.path",
    "url.trailingSlash",
    "url.query",
    "url.padded",
    "url.fragment",
    "url.unencoded",
    "zoneUrls.own",
    "zoneUrls.other",
    "zoneUrls.noZone",
    "body.lines",
    "body.jsonArray",
    "body.single",
    "format.json",
    "format.text",
    "format.bytes",
    "format.sourceBytes",
    "field",
    "compression.none",
    "compression.gzip",
    "headers",
    "credentials",
    "secret.leaderStored",
    "secret.workerLocal",
    "secret.workerLocalUnset",
    "secret.lineBreak",
    "tls.trustedAuthorities",
    "tls.certificate",
    "inFlight",
    "maxRequestBytes",
    "maxRequestBytes.text",
    "bufferBytes",
    "stallAfterSecs",
    "batches.several",
    "requests.several",
    "row.padded",
    "row.unfit",
    "row.lineBreak",
    "answer.2xx",
    "answer.retry",
    "answer.retryAfter",
    "answer.split",
    "answer.stall",
    "answer.cookie",
    "answer.close",
    "broken.url.blank",
    "broken.url.notHttp",
    "broken.zoneUrls",
    "broken.certificate",
    "broken.tls.plainHttp",
    "broken.field.json",
    "broken.field.blank",
    "broken.compression",
    "broken.headers.name",
    "broken.headers.owned",
    "broken.headers.twice",
    "broken.headers.value",
    "broken.credentials.blank",
    "broken.inFlight",
    "broken.maxRequestBytes",
    "broken.bufferBytes",
    "broken.stallAfterSecs",
    "mangled",
];

// -- certificates -----------------------------------------------------------------------------------------------

/// One authority, the service's certificate under it, and a client's, minted once for every seed.
fn pki() -> &'static Pki {
    static PKI: OnceLock<Pki> = OnceLock::new();
    PKI.get_or_init(|| Pki::new("smeltr http fuzz", "localhost", &["localhost", "127.0.0.1"]))
}

/// What the service presents: its certificate, demanding one under the authority when the client presents one.
fn server_tls(client_certificate: bool) -> Arc<rustls::ServerConfig> {
    if client_certificate {
        pki().mutual()
    } else {
        pki().server_only()
    }
}

// -- drawing a destination --------------------------------------------------------------------------------------

/// Where a draw is pointed.
#[derive(Clone, Copy)]
enum Target<'a> {
    /// The service this seed starts on this host, at this port.
    Offline(u16),
    /// Splunk's HEC, at this base URL, signed with this token, filing what it takes under this sourcetype.
    Live {
        hec: &'a str,
        token: &'a str,
        sourcetype: &'a str,
    },
}

/// One drawn destination.
struct Draw {
    destination: Map<String, Value>,
    /// Whether the destination trusts the service's authority, and if so whether it presents a certificate.
    tls: Option<bool>,
    /// The zone of the worker that runs the destination.
    zone: Option<&'static str>,
    /// The setting broken, if one was: the field the save must refuse it on.
    broken: Option<&'static str>,
}

/// A credential: a value the leader holds, or a variable on the worker, set or not.
fn secret(rng: &mut Rng, cov: &mut Reached, live: bool, name: &str) -> Value {
    if !live && rng.chance(6) {
        if rng.chance(2) {
            cov.reach("secret.workerLocalUnset");
            return json!(UNSET_VARIABLE);
        }
        cov.reach("secret.workerLocal");
        return json!(SET_VARIABLE);
    }
    cov.reach("secret.leaderStored");
    json!({"secret": name, "version": 1 + rng.below(3)})
}

/// The value each leader-stored credential holds, by name, drawn with the destination.
type Stored = BTreeMap<String, String>;

fn draw(rng: &mut Rng, cov: &mut Reached, target: Target, stored: &mut Stored) -> Draw {
    let mut d = Map::new();
    d.insert("id".into(), json!("hook"));
    d.insert("type".into(), json!("http"));
    let live = matches!(target, Target::Live { .. });

    // Only an https URL carries TLS settings, and the service's certificate is only trusted through them.
    let https = !live && rng.chance(4);
    let mut tls = None;
    if https {
        let presents = rng.chance(2);
        cov.reach(if presents { "tls.certificate" } else { "tls.trustedAuthorities" });
        tls = Some(presents);
        set_tls(&mut d, presents);
    }
    d.insert("url".into(), json!(url(rng, cov, target, https)));
    let zone = if live {
        None
    } else {
        destination::draw_zones(rng, cov, &mut d)
    };

    let body = *rng.pick(&["lines", "jsonArray", "single"]);
    cov.reach(format!("body.{body}"));
    if body != "lines" || rng.chance(2) {
        d.insert("body".into(), json!(body));
    }
    let format = *rng.pick(&["json", "text", "bytes"]);
    cov.reach(format!("format.{format}"));
    if format == "bytes" && rng.chance(3) {
        cov.reach("format.sourceBytes");
        d.insert("format".into(), json!("sourceBytes"));
    } else if format != "json" || rng.chance(2) {
        d.insert("format".into(), json!(format));
    }
    if format != "json" && rng.chance(2) {
        cov.reach("field");
        let field = if format == "text" { "message" } else { "__source.bytes" };
        d.insert("field".into(), json!(field));
    }
    let compression = *rng.pick(&["none", "gzip"]);
    cov.reach(format!("compression.{compression}"));
    if compression != "none" || rng.chance(2) {
        d.insert("compression".into(), json!(compression));
    }

    let mut taken: BTreeSet<String> = BTreeSet::new();
    let mut headers = Map::new();
    if rng.chance(2) {
        cov.reach("headers");
        for _ in 0..1 + rng.below(3) {
            let name = *rng.pick(HEADER_NAMES);
            // HEC reads these two itself: its token, and a channel that must name one it knows.
            let hec_reads = ["Authorization", "X-Splunk-Request-Channel"].contains(&name);
            if !(live && hec_reads) && taken.insert(name.to_ascii_lowercase()) {
                headers.insert(name.to_string(), json!(rng.pick(HEADER_VALUES)));
            }
        }
    }
    let mut credentials = Map::new();
    match target {
        Target::Live { token, .. } => {
            // HEC takes its token as `Authorization: Splunk <token>`.
            stored.insert("hec".to_string(), format!("Splunk {token}"));
            credentials.insert("Authorization".into(), secret(rng, cov, live, "hec"));
            cov.reach("credentials");
        }
        Target::Offline(_) if rng.chance(2) => {
            cov.reach("credentials");
            for i in 0..1 + rng.below(2) {
                let name = *rng.pick(HEADER_NAMES);
                if taken.insert(name.to_ascii_lowercase()) {
                    let secret_name = format!("token-{i}");
                    let value = if rng.chance(12) {
                        cov.reach("secret.lineBreak");
                        "k-123\n"
                    } else {
                        rng.pick(SECRET_VALUES)
                    };
                    stored.insert(secret_name.clone(), value.to_string());
                    credentials.insert(name.to_string(), secret(rng, cov, live, &secret_name));
                }
            }
        }
        Target::Offline(_) => {}
    }
    if !headers.is_empty() {
        d.insert("headers".into(), Value::Object(headers));
    }
    if !credentials.is_empty() {
        d.insert("credentials".into(), Value::Object(credentials));
    }

    draw_limits(rng, cov, &mut d);
    if rng.chance(2) {
        cov.reach("stallAfterSecs");
        let low = if live { 30 } else { 1 };
        d.insert("stallAfterSecs".into(), json!(rng.pick(&[low, 5, 30, 120])));
    }
    let mut draw = Draw {
        destination: d,
        tls,
        zone,
        broken: None,
    };
    if !live && rng.chance(3) {
        draw.broken = Some(break_one(rng, cov, &mut draw));
    }
    draw
}

/// An address on the service this seed reaches, in each form a URL may take there.
fn url(rng: &mut Rng, cov: &mut Reached, target: Target, https: bool) -> String {
    let base = match target {
        Target::Live { hec, sourcetype, .. } => {
            // The raw endpoint files what it takes as events under the sourcetype the query names.
            cov.reach("url.query");
            let index = if rng.chance(2) { "index=main&" } else { "" };
            return format!("{hec}/services/collector/raw?{index}sourcetype={sourcetype}");
        }
        Target::Offline(port) => {
            let host = if https || rng.chance(3) {
                cov.reach("url.localhost");
                "localhost"
            } else {
                "127.0.0.1"
            };
            let mut scheme = if https { "https" } else { "http" }.to_string();
            cov.reach(format!("url.{scheme}"));
            if rng.chance(6) {
                cov.reach("url.schemeCase");
                scheme = scheme.to_ascii_uppercase();
            }
            format!("{scheme}://{host}:{port}")
        }
    };
    let mut url = base;
    if rng.chance(4) {
        cov.reach("url.trailingSlash");
        url.push_str(rng.pick(&["/", "/v1/input/", "/hook//"]));
    } else if rng.chance(2) {
        cov.reach("url.path");
        url.push_str(rng.pick(&["/v1/input", "/a/b/c", "/hook%20one", "/services/collector/raw"]));
    } else if rng.chance(2) {
        // Sent percent-encoded, as the save parsed it.
        cov.reach("url.unencoded");
        url.push_str(rng.pick(&["/hook one", "/ünï/ça", "/x|y{z}"]));
    }
    if rng.chance(4) {
        cov.reach("url.query");
        if !url[url.find("://").unwrap() + 3..].contains('/') {
            url.push('/');
        }
        url.push_str(rng.pick(&[
            "?token=a&b=c",
            "?a=%2F&empty=",
            "?x",
            "?q=a%20b&%C3%A9=%C3%BC",
            "?q=a b",
        ]));
    }
    if rng.chance(12) {
        cov.reach("url.fragment");
        url.push_str("#part");
    }
    if rng.chance(8) {
        cov.reach("url.padded");
        url = format!(" {url}\t");
    }
    url
}

/// Request size, buffer and senders, together, so the senders the buffer affords are never fewer than the start.
fn draw_limits(rng: &mut Rng, cov: &mut Reached, d: &mut Map<String, Value>) {
    let request = if rng.chance(2) {
        cov.reach("maxRequestBytes");
        *rng.pick(&[4 * KIB, 4 * KIB, 16 * KIB, MIB, 8 * MIB, 100 * MIB])
    } else {
        MIB
    };
    if request != MIB || rng.chance(3) {
        if rng.chance(3) {
            cov.reach("maxRequestBytes.text");
            d.insert("maxRequestBytes".into(), json!(size_text(request)));
        } else {
            d.insert("maxRequestBytes".into(), json!(request));
        }
    }
    let buffer = if rng.chance(2) {
        cov.reach("bufferBytes");
        let buffer = (*rng.pick(&[4 * MIB, 16 * MIB, 64 * MIB, 1024 * MIB])).max(request);
        d.insert("bufferBytes".into(), json!(buffer));
        buffer
    } else {
        (64 * MIB).max(request)
    };
    let affords = (buffer / request).min(64);
    if rng.chance(2) {
        cov.reach("inFlight");
        let start = *rng.pick(&[1, 2, 4, 16, 64]);
        d.insert("inFlight".into(), json!(start.min(affords)));
    }
}

/// `n` bytes as a bundle may write them: `"4 KiB"`, `"1MiB"`, `"16384"`.
fn size_text(n: usize) -> String {
    if n.is_multiple_of(MIB) {
        format!("{} MiB", n / MIB)
    } else if n.is_multiple_of(KIB) {
        format!("{}KiB", n / KIB)
    } else {
        n.to_string()
    }
}

/// Breaks one setting of `draw`, and answers the field the save must refuse it on.
fn break_one(rng: &mut Rng, cov: &mut Reached, draw: &mut Draw) -> &'static str {
    let d = &mut draw.destination;
    let format = d.get("format").and_then(Value::as_str).unwrap_or("json").to_string();
    let mut breaks = vec![
        "url.blank",
        "url.notHttp",
        "certificate",
        "tls.plainHttp",
        "compression",
        "headers.name",
        "headers.owned",
        "headers.twice",
        "headers.value",
        "credentials.blank",
        "inFlight",
        "maxRequestBytes",
        "bufferBytes",
        "stallAfterSecs",
    ];
    breaks.push(if format == "json" { "field.json" } else { "field.blank" });
    breaks.push("zoneUrls");
    let what = *rng.pick(&breaks);
    cov.reach(format!("broken.{what}"));
    let map = |d: &mut Map<String, Value>, key: &str| -> Map<String, Value> {
        match d.remove(key) {
            Some(Value::Object(m)) => m,
            _ => Map::new(),
        }
    };
    match what {
        "url.blank" => {
            d.insert("url".into(), json!(rng.pick(&["", "  "])));
            "url"
        }
        "url.notHttp" => {
            let bad = rng.pick(&[
                "not a url",
                "ftp://127.0.0.1/x",
                "http://",
                "mailto:a@b",
                "127.0.0.1:80/x",
                "file:///tmp/x",
            ]);
            d.insert("url".into(), json!(bad));
            // TLS settings on a URL that is not https are refused on the URL too.
            "url"
        }
        "zoneUrls" => destination::break_zones(rng, d),
        "certificate" => {
            d.insert("certificate".into(), json!(" "));
            draw.tls = None;
            "certificate"
        }
        "tls.plainHttp" => {
            set_tls(d, false);
            draw.tls = Some(false);
            let url = d["url"].as_str().unwrap().trim().to_string();
            let rest = &url[url.find("://").unwrap()..];
            d.insert("url".into(), json!(format!("http{rest}")));
            "url"
        }
        "field.json" => {
            d.insert("field".into(), json!("_raw"));
            "field"
        }
        "field.blank" => {
            d.insert("field".into(), json!(rng.pick(&["", " "])));
            "field"
        }
        "compression" => {
            d.insert("compression".into(), json!(rng.pick(&["snappy", "lz4", "zstd"])));
            "compression"
        }
        "headers.name" | "headers.owned" | "headers.value" => {
            let in_credentials = what != "headers.value" && rng.chance(2);
            let key = if in_credentials { "credentials" } else { "headers" };
            let mut m = map(d, key);
            let (name, value) = match what {
                "headers.name" => (
                    rng.pick(&["X Bad", "", "Bad:Name", "ünï", "a\nb", "(x)"]).to_string(),
                    json!("v"),
                ),
                "headers.owned" => (rng.pick(OWNED_HEADERS).to_string(), json!("v")),
                _ => (
                    "X-Broken".to_string(),
                    json!(rng.pick(&["a\nb", "a\r\nInjected: 1", "nul\u{0}", "\u{7f}"])),
                ),
            };
            let value = if in_credentials {
                json!({"secret": "token-x", "version": 1})
            } else {
                value
            };
            m.insert(name, value);
            d.insert(key.into(), Value::Object(m));
            if in_credentials { "credentials" } else { "headers" }
        }
        "headers.twice" => {
            // One name, two spellings: in one map, the second in sorted order is refused; across the two, the one in
            // `credentials`, which is read second.
            let mut h = map(d, "headers");
            let mut c = map(d, "credentials");
            h.retain(|k, _| !k.eq_ignore_ascii_case("x-twice"));
            c.retain(|k, _| !k.eq_ignore_ascii_case("x-twice"));
            h.insert("X-Twice".into(), json!("a"));
            let field = if rng.chance(2) {
                h.insert("x-twice".into(), json!("b"));
                "headers"
            } else {
                c.insert("x-TWICE".into(), json!({"secret": "token-x", "version": 1}));
                "credentials"
            };
            d.insert("headers".into(), Value::Object(h));
            if !c.is_empty() {
                d.insert("credentials".into(), Value::Object(c));
            }
            field
        }
        "credentials.blank" => {
            let mut c = map(d, "credentials");
            let blank = if rng.chance(2) {
                json!(rng.pick(&["", "  "]))
            } else {
                json!({"secret": " ", "version": 1})
            };
            c.insert("X-Blank-Secret".into(), blank);
            let mut h = map(d, "headers");
            h.retain(|k, _| !k.eq_ignore_ascii_case("x-blank-secret"));
            if !h.is_empty() {
                d.insert("headers".into(), Value::Object(h));
            }
            d.insert("credentials".into(), Value::Object(c));
            "credentials"
        }
        "inFlight" => {
            let request = d.get("maxRequestBytes").map_or(MIB, size_of);
            let buffer = d.get("bufferBytes").map_or((64 * MIB).max(request), size_of);
            let affords = (buffer / request).min(64);
            // One more than the buffer affords, unless that is the default, which starts at what it affords instead.
            let above = if affords + 1 == 4 { 5 } else { affords + 1 };
            d.insert("inFlight".into(), json!(rng.pick(&[0, 65, above])));
            "inFlight"
        }
        "maxRequestBytes" => {
            d.insert(
                "maxRequestBytes".into(),
                json!(rng.pick(&[0, 4 * KIB - 1, 100 * MIB + 1])),
            );
            "maxRequestBytes"
        }
        "bufferBytes" => {
            let request = d.get("maxRequestBytes").map_or(MIB, size_of);
            // Out of bounds, or in them and short of one whole request.
            let mut bad = vec![0, 4 * MIB - 1, 1024 * MIB + 1];
            if request > 4 * MIB {
                bad.push(request - 1);
            }
            d.insert("bufferBytes".into(), json!(rng.pick(&bad)));
            "bufferBytes"
        }
        "stallAfterSecs" => {
            d.insert("stallAfterSecs".into(), json!(rng.pick(&[0, 121, 3600])));
            "stallAfterSecs"
        }
        other => unreachable!("{other}"),
    }
}

/// A size the bundle states, as a number or text.
fn size_of(v: &Value) -> usize {
    match v {
        Value::String(s) => {
            let s = s.trim();
            if let Some(n) = s.strip_suffix("MiB") {
                n.trim().parse::<usize>().unwrap() * MIB
            } else if let Some(n) = s.strip_suffix("KiB") {
                n.trim().parse::<usize>().unwrap() * KIB
            } else {
                s.parse().unwrap()
            }
        }
        v => v.as_u64().unwrap() as usize,
    }
}

/// Stands for the authority's PEM until `bundle` has it.
const AUTHORITIES: &str = "@authorities";

/// Sets TLS on `d`: the authority as [`AUTHORITIES`], and a certificate to present when `presents`.
fn set_tls(d: &mut Map<String, Value>, presents: bool) {
    d.insert("trustedAuthorities".into(), json!(AUTHORITIES));
    if presents {
        d.insert("certificate".into(), json!("client"));
    }
}

/// `draw` as a whole bundle: a Kafka source, one route to the destination, and the authority it trusts.
fn bundle(draw: &Draw) -> Value {
    let mut b = json!({
        "version": 1,
        "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
        "destinations": [Value::Object(draw.destination.clone())],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["hook"]}]}],
    });
    if let Some(authorities) = b["destinations"][0].get_mut("trustedAuthorities") {
        *authorities = json!(pki().authority.pem);
    }
    b
}

/// What the worker holds: every leader-stored credential the destination names, at the version it names, and the
/// certificate it presents. `true` beside it when the destination names a credential this worker does not hold: a variable no
/// worker sets.
fn held(draw: &Draw, stored: &Stored) -> (HeldSecrets, bool) {
    let mut values = Vec::new();
    let mut unheld = false;
    if let Some(Value::Object(c)) = draw.destination.get("credentials") {
        for v in c.values() {
            match v {
                Value::Object(o) => {
                    let name = o["secret"].as_str().unwrap().to_string();
                    let value = stored[&name].clone();
                    values.push((name, o["version"].as_i64().unwrap(), value));
                }
                Value::String(s) if s == UNSET_VARIABLE => unheld = true,
                _ => {}
            }
        }
    }
    let mut secrets = HeldSecrets::default();
    secrets.set_stored(values);
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: "client".to_string(),
        version: 1,
        value: pki().client.key.clone(),
        chain: Some(pki().client.certificate.clone()),
    }]);

    (secrets, unheld)
}

// -- what the save says ------------------------------------------------------------------------------------------

/// The destination `json` loads as.
fn loaded(json: &str) -> Result<(HttpDestinationConfig, Vec<SecretRef>), String> {
    let system = load_str(json).map_err(|e| format!("{e:?}"))?;
    match &system.topology.destinations[0].kind {
        DestinationKind::Http(c) => Ok((c.clone(), connector_secrets(&system.topology))),
        other => Err(format!("loaded as {other:?}")),
    }
}

/// Checks the save: a broken draw is refused on its field, and an unbroken one is accepted, loads as drawn, and
/// loads the same once saved.
fn check_save(draw: &Draw, json: &str) -> Result<Option<HttpDestinationConfig>, String> {
    let config = destination::check_save(
        json,
        draw.broken.map(|f| Broken::Field(Some(f))),
        destination::loads,
        loaded,
    )?;
    if let Some((c, named)) = &config {
        check_loaded(draw, c, named)?;
    }
    // What the worker runs: its own zone's address, taken at deploy.
    Ok(config.map(|(mut c, _)| {
        take_zone(&mut c.url, &mut c.zone_urls, draw.zone);
        c
    }))
}

/// What `draw` says, against what it loaded as.
fn check_loaded(draw: &Draw, c: &HttpDestinationConfig, named: &[SecretRef]) -> Result<(), String> {
    let d = &draw.destination;
    let mut wrong = Vec::new();
    let mut expect = |what: &str, want: String, got: String| {
        if want != got {
            wrong.push(format!("{what}: drawn {want}, loaded {got}"));
        }
    };
    let s = |key: &str| d.get(key).and_then(Value::as_str);
    let url = s("url").unwrap().trim();
    expect("url", reqwest::Url::parse(url).unwrap().to_string(), c.url.clone());
    let zone_urls: BTreeMap<String, String> = destination::drawn_zones(d)
        .into_iter()
        .map(|(zone, url)| (zone, reqwest::Url::parse(url.trim()).unwrap().to_string()))
        .collect();
    expect("zoneUrls", format!("{zone_urls:?}"), format!("{:?}", c.zone_urls));
    let body = match s("body").unwrap_or("lines") {
        "lines" => RequestBody::Lines,
        "jsonArray" => RequestBody::JsonArray,
        _ => RequestBody::Single,
    };
    expect("body", format!("{body:?}"), format!("{:?}", c.body));
    let field = s("field").map(str::to_string);
    let payload = match s("format").unwrap_or("json") {
        "json" => Payload::Record,
        "text" => Payload::Text {
            field: field.unwrap_or("_raw".to_string()).into(),
        },
        _ => Payload::Bytes {
            field: field.unwrap_or("__source.bytes".to_string()).into(),
        },
    };
    expect("format", format!("{payload:?}"), format!("{:?}", c.payload));
    let compression = match s("compression").unwrap_or("none") {
        "gzip" => Compression::Gzip,
        _ => Compression::None,
    };
    expect(
        "compression",
        format!("{compression:?}"),
        format!("{:?}", c.compression),
    );
    let headers: Vec<(String, String)> = match d.get("headers") {
        Some(Value::Object(h)) => h
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
            .collect(),
        _ => Vec::new(),
    };
    expect("headers", format!("{headers:?}"), format!("{:?}", c.headers));
    let credentials: Vec<(String, SecretRef)> = match d.get("credentials") {
        Some(Value::Object(m)) => m
            .iter()
            .map(|(k, v)| {
                let r = match v {
                    Value::String(var) => SecretRef::WorkerLocal(var.clone()),
                    v => SecretRef::LeaderStored {
                        name: v["secret"].as_str().unwrap().to_string(),
                        version: v["version"].as_i64().unwrap(),
                    },
                };
                (k.clone(), r)
            })
            .collect(),
        _ => Vec::new(),
    };
    expect(
        "credentials",
        format!("{credentials:?}"),
        format!("{:?}", c.credentials),
    );
    expect(
        "credentials named",
        format!("{:?}", credentials.iter().map(|(_, r)| r).collect::<Vec<_>>()),
        format!("{:?}", named.iter().collect::<Vec<_>>()),
    );
    expect(
        "tls",
        format!("{:?}", draw.tls),
        format!("{:?}", c.tls.as_ref().map(|t| t.certificate.is_some())),
    );
    let request = d.get("maxRequestBytes").map_or(MIB, size_of);
    expect("maxRequestBytes", request.to_string(), c.max_request_bytes.to_string());
    let buffer = d.get("bufferBytes").map_or((64 * MIB).max(request), size_of);
    expect("bufferBytes", buffer.to_string(), c.buffer_bytes.to_string());
    // Unstated, the start is the default or what the buffer affords, whichever is fewer.
    let affords = (c.buffer_bytes / c.max_request_bytes.max(1)).max(1) as u64;
    let in_flight = d
        .get("inFlight")
        .map_or(4.min(affords) as usize, |v| v.as_u64().unwrap() as usize);
    expect("inFlight", in_flight.to_string(), c.in_flight.to_string());
    let stall = d.get("stallAfterSecs").map_or(30, |v| v.as_u64().unwrap());
    expect("stallAfterSecs", stall.to_string(), c.stall_after.as_secs().to_string());
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(wrong.join("\n"))
    }
}

// -- rows -------------------------------------------------------------------------------------------------------

/// Text in any script, with what a line format must escape and what JSON must.
///
/// Live, Splunk is the reader: it files a carriage return and a control character as it pleases, takes a line
/// starting with a digit for a timestamp that may start an event, and trims an event's trailing space, so live text
/// holds none of those and ends in a full stop.
fn text(rng: &mut Rng, cov: &mut Reached, len: usize, live: bool) -> String {
    let mut s: String = if live {
        (0..len)
            .map(|_| *rng.pick(&['a', 'z', ' ', 'é', '"', '\\', '\n', '\t', '\u{2028}', '😀', '{']))
            .collect()
    } else {
        (0..len)
            .map(|_| {
                *rng.pick(&[
                    'a', 'z', '0', ' ', 'é', '"', '\\', '\n', '\r', '\t', '\u{2028}', '😀', '{', '\u{1}',
                ])
            })
            .collect()
    };
    if s.contains(['\n', '\r']) {
        cov.reach("row.lineBreak");
    }
    if live {
        s.push('.');
    }
    s
}

/// What `row` is as one item of a request under `c`: a record as it is, text on one line in `lines` and a JSON
/// string in an array, bytes as base64 wherever they share a body.
fn item(c: &HttpDestinationConfig, row: &OwnedRecord) -> Vec<u8> {
    let b64 = |v: &[u8]| base64::engine::general_purpose::STANDARD.encode(v);
    match (&row.payload, c.body) {
        (Payload::Record, _) => row.value.trim_ascii().to_vec(),
        (Payload::Text { .. }, RequestBody::Lines) => String::from_utf8(row.value.clone())
            .unwrap()
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .into_bytes(),
        (Payload::Text { .. }, RequestBody::JsonArray) => {
            serde_json::to_vec(&String::from_utf8(row.value.clone()).unwrap()).unwrap()
        }
        (Payload::Bytes { .. }, RequestBody::Lines) => b64(&row.value).into_bytes(),
        (Payload::Bytes { .. }, RequestBody::JsonArray) => format!("\"{}\"", b64(&row.value)).into_bytes(),
        (_, RequestBody::Single) => row.value.clone(),
    }
}

/// The row `id`, carrying its id where any service shows it, under a tag no other row's starts with.
fn row(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &HttpDestinationConfig,
    run: &str,
    id: Id,
    big: bool,
    live: bool,
) -> OwnedRecord {
    let tag = format!("{run}b{}r{}.", id.0, id.1);
    let len = if big { 300 + rng.below(1200) } else { rng.below(24) };
    let (value, payload) = match &c.payload {
        Payload::Record => {
            let body = serde_json::to_string(&json!({"tag": tag, "msg": text(rng, cov, len, live)})).unwrap();
            let value = if rng.chance(4) {
                cov.reach("row.padded");
                format!(
                    "{}{body}{}",
                    rng.pick(&[" ", "\n", "\t "]),
                    rng.pick(&["\n", "\r\n", " "])
                )
            } else {
                body
            };
            (value.into_bytes(), Payload::Record)
        }
        Payload::Text { field } => (
            format!("{tag} {}", text(rng, cov, len, live)).into_bytes(),
            Payload::Text { field: field.clone() },
        ),
        Payload::Bytes { field } => {
            let mut v = format!("{tag} ").into_bytes();
            if live && c.body == RequestBody::Single {
                // Sent as they are, so Splunk reads them as text.
                v.extend(text(rng, cov, len, live).into_bytes());
            } else {
                v.extend((0..len).map(|_| *rng.pick(&[b'a', b'\n', b'\r', 0x00, 0xe9, 0xff, b'"', b'\\'])));
            }
            (v, Payload::Bytes { field: field.clone() })
        }
    };
    OwnedRecord {
        value,
        payload,
        ..OwnedRecord::default()
    }
}

/// One to three batches. Large rows under a small request size fill several requests.
fn batches_for(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &HttpDestinationConfig,
    run: &str,
    poison: bool,
    unfit: bool,
    live: bool,
) -> Vec<Vec<Sent>> {
    let count = 1 + rng.below(3);
    if count > 1 {
        cov.reach("batches.several");
    }
    (0..count)
        .map(|batch| {
            // Splunk cuts a line at 10,000 bytes, and an array is one line.
            let big = !live && c.max_request_bytes <= 16 * KIB && rng.chance(2);
            let rows = if big { 8 + rng.below(24) } else { 1 + rng.below(12) };
            (0..rows)
                .map(|at| {
                    let mut row = row(rng, cov, c, run, (batch, at), big, live);
                    if unfit && rng.chance(5) {
                        cov.reach("row.unfit");
                        row.unfit = Some("nested too deep".to_string());
                    }
                    let poison = poison && row.unfit.is_none() && rng.chance(5);
                    Sent {
                        item: item(c, &row),
                        row,
                        poison,
                    }
                })
                .collect()
        })
        .collect()
}

// -- the service ------------------------------------------------------------------------------------------------

/// How the service answers, and what it saw. Each answer is drawn from the seed and the request's body alone, so a
/// replayed seed gets the same answers however its concurrent requests arrive.
struct Script {
    c: HttpDestinationConfig,
    seed: u64,
    /// Each row's item, by what it is on the wire.
    items: HashMap<Vec<u8>, Id>,
    poison: BTreeSet<Id>,
    /// The status of the refusal a request carrying a poisoned row gets.
    split: u16,
    /// Whether some requests are asked for again, each body once.
    retry: bool,
    /// Each body asked for again.
    retried: BTreeSet<Vec<u8>>,
    /// The status that stalls the destination, given to every request.
    stall: Option<u16>,
    cookies: bool,
    close: bool,
    /// Each request, its rows, and the status it was answered with.
    log: Vec<(Seen, Result<Vec<Id>, String>, u16)>,
    /// Each cookie jar the service set one in, by the order it was set: the generation of its latest cookie.
    jars: Vec<u64>,
    /// The jar each connection's client keeps, once the service has set it or the client sent it.
    bound: HashMap<usize, usize>,
    /// A request that did not carry back what its client's jar holds.
    cookie_wrong: Vec<String>,
    answered: BTreeSet<&'static str>,
}

impl Answers for Script {
    fn answer(&mut self, seen: &Seen, conn: usize) -> Answer {
        let mut rng = Rng::seeded(self.seed ^ fnv(&seen.body));
        let ids = self.rows_of(seen);
        self.check_cookie(seen, conn);
        let mut headers = Vec::new();
        let status = match &ids {
            Err(_) => 200,
            Ok(_) if self.stall.is_some() => {
                let status = self.stall.unwrap();
                if status == 301 {
                    headers.push(("Location".to_string(), "/elsewhere".to_string()));
                }
                status
            }
            Ok(ids) if ids.iter().any(|id| self.poison.contains(id)) => {
                self.answered.insert("answer.split");
                self.split
            }
            // Each costs a backoff, so a body is asked for again once.
            Ok(_) if self.retry && rng.chance(3) && self.retried.insert(seen.body.clone()) => {
                self.answered.insert("answer.retry");
                if rng.chance(2) {
                    self.answered.insert("answer.retryAfter");
                    headers.push((
                        "Retry-After".to_string(),
                        rng.pick(&["0", "Thu, 01 Jan 1970 00:00:00 GMT"]).to_string(),
                    ));
                }
                *rng.pick(&[408, 429, 500, 502, 503, 504])
            }
            Ok(_) => {
                self.answered.insert("answer.2xx");
                *rng.pick(&[200, 201, 202, 204, 299])
            }
        };
        if self.cookies && rng.chance(2) {
            let jar = *self.bound.entry(conn).or_insert_with(|| {
                self.jars.push(0);
                self.jars.len() - 1
            });
            self.jars[jar] += 1;
            headers.push((
                "Set-Cookie".to_string(),
                format!("affinity=j{jar}g{}; Path=/; HttpOnly", self.jars[jar]),
            ));
        }
        if self.close && rng.chance(2) {
            self.answered.insert("answer.close");
            headers.push(("Connection".to_string(), "close".to_string()));
        }
        self.log.push((seen.clone(), ids, status));
        // A 204 carries no body.
        let body = if status == 204 { "" } else { "ok" };
        Answer {
            status,
            headers,
            body: body.to_string(),
        }
    }
}

impl Script {
    /// Checks `seen` carries back the latest cookie the service set in its client's jar, and nothing else. A client
    /// sends one request at a time on its own connections, so no answer setting its jar is still in flight. A
    /// connection's first request names its jar by the cookie it carries, if any.
    fn check_cookie(&mut self, seen: &Seen, conn: usize) {
        let sent = seen.header("cookie");
        let latest = |jar: usize, jars: &[u64]| format!("affinity=j{jar}g{}", jars[jar]);
        match (self.bound.get(&conn).copied(), sent.as_slice()) {
            (None, []) => {}
            (Some(jar), [got]) if *got == latest(jar, &self.jars) => {
                self.answered.insert("answer.cookie");
            }
            (None, [got]) => {
                let named = (got.strip_prefix("affinity=j"))
                    .and_then(|v| v.split_once('g'))
                    .and_then(|(j, _)| j.parse::<usize>().ok())
                    .filter(|j| *j < self.jars.len());
                match named {
                    Some(jar) if *got == latest(jar, &self.jars) => {
                        self.bound.insert(conn, jar);
                        self.answered.insert("answer.cookie");
                    }
                    _ => self
                        .cookie_wrong
                        .push(format!("a new connection sent {got:?}, which no jar holds")),
                }
            }
            (Some(jar), got) => self.cookie_wrong.push(format!(
                "sent {got:?}, not {:?}, the cookie its jar holds",
                latest(jar, &self.jars)
            )),
            (None, got) => self.cookie_wrong.push(format!("a new connection sent {got:?}")),
        }
    }

    /// The rows `seen` carries, in order, when its body is exactly their items in the shape the config says.
    fn rows_of(&self, seen: &Seen) -> Result<Vec<Id>, String> {
        let gzip = seen.header("content-encoding") == ["gzip"];
        let body = if gzip { gunzip(&seen.body)? } else { seen.body.clone() };
        let find = |item: &[u8]| {
            self.items
                .get(item)
                .copied()
                .ok_or_else(|| format!("an item no row is: {:?}", String::from_utf8_lossy(item)))
        };
        match self.c.body {
            RequestBody::Lines => {
                let Some(lines) = body.strip_suffix(b"\n") else {
                    return Err(format!(
                        "does not end in a line break: {:?}",
                        String::from_utf8_lossy(&body)
                    ));
                };
                lines.split(|b| *b == b'\n').map(find).collect()
            }
            RequestBody::JsonArray => json_array_rows(&body, find),
            RequestBody::Single => find(&body).map(|id| vec![id]),
        }
    }
}

// -- delivering -------------------------------------------------------------------------------------------------

fn build(c: &HttpDestinationConfig, secrets: &HeldSecrets) -> Result<Box<dyn Destination>, IoError> {
    let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
    smeltr::io::build_destination(&DestinationKind::Http(c.clone()), 0, metrics, secrets, None)
}

/// The content type a request under `c` says its body is.
fn content_type(c: &HttpDestinationConfig) -> &'static str {
    match (c.body, &c.payload) {
        (RequestBody::JsonArray, _) | (RequestBody::Single, Payload::Record) => "application/json",
        (RequestBody::Lines, Payload::Record) => "application/x-ndjson",
        (_, Payload::Text { .. }) => "text/plain; charset=utf-8",
        (RequestBody::Single, Payload::Bytes { .. }) => "application/octet-stream",
        (RequestBody::Lines, Payload::Bytes { .. }) => "text/plain",
    }
}

/// The request target a request to `url` names: its path and query as a URL is sent, percent-encoded, and never its
/// fragment.
fn target_of(url: &str) -> String {
    let parsed = reqwest::Url::parse(url).expect("a URL the save took");
    match parsed.query() {
        Some(query) => format!("{}?{query}", parsed.path()),
        None => parsed.path().to_string(),
    }
}

/// The rows each request carried, by how many requests landed each, and how many requests carried each batch.
type Carried = (BTreeMap<Id, usize>, BTreeMap<usize, usize>);

/// Checks every request the service read against `c`: its method, target, headers, cookie and body.
fn check_requests(
    c: &HttpDestinationConfig,
    credentials: &[(String, String)],
    sent: &[Vec<Sent>],
    script: &Script,
) -> Result<Carried, String> {
    if let Some(wrong) = script.cookie_wrong.first() {
        return Err(format!("a cookie: {wrong}"));
    }
    let target = target_of(&c.url);
    let mut landed: BTreeMap<Id, usize> = BTreeMap::new();
    let mut requests: BTreeMap<usize, usize> = BTreeMap::new();
    for (seen, ids, status) in &script.log {
        let ids = ids
            .as_ref()
            .map_err(|e| format!("a request the service could not read: {e}"))?;
        let mut wrong = Vec::new();
        if seen.method != "POST" {
            wrong.push(format!("method {}", seen.method));
        }
        if seen.target != target {
            wrong.push(format!("target {:?}, not {target:?}", seen.target));
        }
        if seen.header("content-type") != [content_type(c)] {
            wrong.push(format!("content-type {:?}", seen.header("content-type")));
        }
        let encoding: &[&str] = if c.compression == Compression::Gzip {
            &["gzip"]
        } else {
            &[]
        };
        if seen.header("content-encoding") != encoding {
            wrong.push(format!("content-encoding {:?}", seen.header("content-encoding")));
        }
        for (name, value) in c.headers.iter().chain(credentials) {
            if seen.header(&name.to_ascii_lowercase()) != [value.trim()] {
                wrong.push(format!(
                    "{name}: {:?}, not {value:?}",
                    seen.header(&name.to_ascii_lowercase())
                ));
            }
        }
        // Nothing the config does not name.
        let named: BTreeSet<String> = (c.headers.iter().chain(credentials))
            .map(|(n, _)| n.to_ascii_lowercase())
            .collect();
        for (name, _) in &seen.headers {
            let any_request = [
                "host",
                "content-type",
                "content-length",
                "content-encoding",
                "cookie",
                "user-agent",
                "accept",
                "accept-encoding",
                "connection",
            ];
            if !named.contains(name) && !any_request.contains(&name.as_str()) {
                wrong.push(format!("a header the config does not name: {name}"));
            }
        }
        if c.body == RequestBody::Single && ids.len() > 1 {
            wrong.push(format!("{} rows in one single request", ids.len()));
        }
        wrong.extend(http::wrong_rows(ids, sent, c.max_request_bytes));
        if !wrong.is_empty() {
            return Err(format!("a request carrying {ids:?}: {}", wrong.join("; ")));
        }
        if let Some((b, _)) = ids.first() {
            *requests.entry(*b).or_default() += 1;
        }
        if (200..300).contains(status) {
            for id in ids {
                *landed.entry(*id).or_default() += 1;
            }
        }
    }
    Ok((landed, requests))
}

/// Checks every request the service read against `c`, and that every row landed once or was refused as it had to be.
fn check_delivered(
    c: &HttpDestinationConfig,
    credentials: &[(String, String)],
    sent: &[Vec<Sent>],
    refused: &[Vec<Rejection>],
    script: &Script,
) -> Result<(), String> {
    let (landed, requests) = check_requests(c, credentials, sent, script)?;
    http::check_landed(sent, refused, &landed, script.split)?;
    // A batch nothing failed for goes in as few requests as the limit allows.
    if script.split == 0 && script.log.iter().all(|(_, _, s)| (200..300).contains(s)) {
        for (batch, rows) in sent.iter().enumerate() {
            let want = requests_for(c, rows);
            let got = requests.get(&batch).copied().unwrap_or(0);
            if got != want {
                return Err(format!("batch {batch} went in {got} requests, not {want}"));
            }
        }
    }
    Ok(())
}

/// How many requests `rows` make under `c` when every one lands: one per fit row under `single`, otherwise as
/// [`http::requests_for`] counts.
fn requests_for(c: &HttpDestinationConfig, rows: &[Sent]) -> usize {
    match c.body {
        RequestBody::Single => rows.iter().filter(|s| s.row.unfit.is_none()).count(),
        _ => http::requests_for(c.max_request_bytes, rows),
    }
}

// -- the offline fuzzer -----------------------------------------------------------------------------------------

/// Each seed's destination, against the save, what it builds with, and a service on this host it delivers to.
#[test]
fn http_destination_fuzz() {
    assert!(
        std::env::var(SET_VARIABLE).is_ok(),
        "{SET_VARIABLE} is set in a test process"
    );
    destination::run(0..SEEDS, OPTIONS.iter().copied(), offline_seed);
}

fn offline_seed(seed: u64, reached: &mut Reached) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let mut stored = Stored::new();
    // Bound first, so the draw's URL can name its port.
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = listener.local_addr().unwrap().port();
    let mut draw = draw(&mut rng, reached, Target::Offline(port), &mut stored);
    let mangled = draw.broken.is_none() && rng.chance(12);
    reached.only_breaks = mangled || draw.broken.is_some();
    let unknown = mangled && destination::mangle(&mut rng, reached, &mut draw.destination, &["unknownSetting"]);
    let json = bundle(&draw).to_string();
    let context = |e: String| format!("{e}\n{json}");
    if mangled {
        if unknown && unknown_key_taken(&json) {
            return Err(context("a key it does not know is taken".to_string()));
        }
        return Ok(());
    }
    let Some(c) = check_save(&draw, &json).map_err(context)? else {
        return Ok(());
    };

    // Each credential's value as this worker holds it, or why it cannot build.
    let (secrets, unheld) = held(&draw, &stored);
    let line_break = (c.credentials.iter()).find(|(_, r)| secrets.value(r).is_some_and(|v| v.contains('\n')));
    let built = build(&c, &secrets);
    // A destination that never builds delivers nothing, so it reaches only the refusal it was checked for.
    let mut d = match (built, unheld, line_break) {
        (Ok(d), false, None) => d,
        (Err(e), true, _) if e.message.contains(UNSET_VARIABLE) => {
            reached.keep_only(&["secret.workerLocalUnset"]);
            return Ok(());
        }
        // A destination naming both an unset variable and a line break is refused for whichever it reads first.
        (Err(e), _, Some((header, _))) if e.message.contains(header.as_str()) && e.message.contains("line break") => {
            if e.message.contains("k-123") {
                return Err(context(format!("the refusal shows the credential: {}", e.message)));
            }
            reached.keep_only(&["secret.lineBreak"]);
            return Ok(());
        }
        (Ok(_), _, _) => return Err(context("built with a credential it cannot send".to_string())),
        (Err(e), _, _) => return Err(context(format!("does not build: {}", e.message))),
    };
    let credentials: Vec<(String, String)> = (c.credentials.iter())
        .map(|(h, r)| (h.clone(), secrets.value(r).unwrap()))
        .collect();

    let poison = rng.chance(5);
    let unfit = rng.chance(5);
    let sent = batches_for(&mut rng, reached, &c, &format!("s{seed}"), poison, unfit, false);
    let stall = rng.chance(12).then(|| *rng.pick(&[301, 401, 403, 404, 501]));
    let script = Script {
        c: c.clone(),
        items: (sent.iter().enumerate())
            .flat_map(|(b, rows)| rows.iter().enumerate().map(move |(r, s)| (s.item.clone(), (b, r))))
            .collect(),
        poison: (sent.iter().enumerate())
            .flat_map(|(b, rows)| {
                rows.iter()
                    .enumerate()
                    .filter(|(_, s)| s.poison)
                    .map(move |(r, _)| (b, r))
            })
            .collect(),
        seed,
        split: if poison { *rng.pick(&[400, 413, 422]) } else { 0 },
        // Each costs a backoff, so they are kept rare.
        retry: rng.chance(24),
        retried: BTreeSet::new(),
        stall,
        cookies: rng.chance(4),
        close: rng.chance(4),
        log: Vec::new(),
        jars: Vec::new(),
        bound: HashMap::new(),
        cookie_wrong: Vec::new(),
        answered: BTreeSet::new(),
    };
    let script = Arc::new(Mutex::new(script));
    let tls = draw.tls.map(server_tls);
    let service = Service::start(listener, tls, script.clone());

    let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
    let delivered = destination::write(d.as_mut(), &batches, Duration::from_secs(60));
    drop(d);
    drop(service);
    let script = script.lock().unwrap();
    if let Some(status) = stall {
        check_requests(&c, &credentials, &sent, &script).map_err(context)?;
        reached.reach("answer.stall");
        let e = match delivered {
            Ok(_) => return Err(context(format!("answered {status}, and did not stall"))),
            Err(e) => e,
        };
        if !e.contains(&format!("HTTP {status}")) {
            return Err(context(format!("answered {status}, stalled saying {e:?}")));
        }
        // A value of a letter or two is in any message; one longer is the credential.
        let shown = (credentials.iter()).find(|(_, v)| v.len() > 3 && e.contains(v.as_str()));
        if let Some((name, _)) = shown {
            return Err(context(format!("the stall shows the {name} credential: {e}")));
        }
        return Ok(());
    }
    let refused = delivered.map_err(|e| context(format!("stalled: {e}")))?;
    check_delivered(&c, &credentials, &sent, &refused, &script).map_err(context)?;
    for name in &script.answered {
        reached.reach(*name);
    }
    if c.body != RequestBody::Single && sent.iter().any(|rows| requests_for(&c, rows) > 1) {
        reached.reach("requests.several");
    }
    Ok(())
}

// -- the live fuzzer --------------------------------------------------------------------------------------------

/// What a live draw reaches: every option Splunk's raw endpoint serves, signed with its token. Not TLS, as it
/// serves plain HTTP; not the answers, which are Splunk's to give.
fn live_options() -> Vec<&'static str> {
    OPTIONS
        .iter()
        .copied()
        .filter(|o| {
            !(o.starts_with("broken.")
                || o.starts_with("answer.")
                || o.starts_with("tls.")
                // One Splunk, at one address.
                || o.starts_with("zoneUrls.")
                || (o.starts_with("url.") && *o != "url.query")
                || [
                    "mangled",
                    "secret.workerLocal",
                    "secret.workerLocalUnset",
                    "secret.lineBreak",
                    "requests.several",
                ]
                .contains(o))
        })
        .collect()
}

/// A name no earlier run used, in letters alone: Splunk reads a long run of digits as a time.
fn unique() -> String {
    let mut n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut out = String::from("hf");
    while n > 0 {
        out.push((b'a' + (n % 26) as u8) as char);
        n /= 26;
    }
    out
}

/// `text` with each run of whitespace one space. Splunk merges an event's lines as it pleases, dropping blank ones,
/// so what it files is compared with what was sent up to whitespace.
fn spaced(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Every event Splunk filed under `sourcetype`, each one's text, joined and [`spaced`]. Asked until each of `items`
/// is there or a minute passes, since an event is searchable a moment after it is indexed.
fn filed(s: &live::Splunk, sourcetype: &str, items: &[&[u8]]) -> Result<String, String> {
    let agent = ureq::Agent::new_with_defaults();
    let (user, password) = s.auth.split_once(':').expect("user:password");
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"))
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let body = agent
            .post(format!("{}/services/search/jobs/export", s.api))
            .header("Authorization", &auth)
            .send_form([
                (
                    "search",
                    format!("search index=main sourcetype=\"{sourcetype}\" | table _raw").as_str(),
                ),
                ("output_mode", "json"),
                ("earliest_time", "0"),
            ])
            .map_err(|e| format!("searching Splunk: {e}"))?
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("reading the search: {e}"))?;
        let raw: Vec<String> = body
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter_map(|v| Some(v.get("result")?.get("_raw")?.as_str()?.to_string()))
            .collect();
        let all = spaced(&raw.join("\n"));
        let found = items.iter().all(|i| all.contains(&spaced(&String::from_utf8_lossy(i))));
        if found || Instant::now() > deadline {
            return Ok(all);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Each seed's destination, against the save, and delivering to Splunk's HEC raw endpoint, which files what each
/// request carries as events a search reads back: every row landed is there exactly once, as its item.
#[test]
fn live_http_fuzz() {
    let Some(s) = live::splunk("live_http_fuzz") else {
        return;
    };
    destination::run(0..LIVE_SEEDS, live_options(), |seed, reached| {
        let mut rng = Rng::seeded(seed);
        let run = unique();
        let sourcetype = format!("smeltr:fuzz:{run}");
        let target = Target::Live {
            hec: &s.hec,
            token: &s.token,
            sourcetype: &sourcetype,
        };
        let mut stored = Stored::new();
        let draw = draw(&mut rng, reached, target, &mut stored);
        let json = bundle(&draw).to_string();
        let context = |e: String| format!("{e}\n{json}");
        let c = check_save(&draw, &json)
            .map_err(context)?
            .expect("a live draw is never broken");
        let (secrets, _) = held(&draw, &stored);
        let mut d = build(&c, &secrets).map_err(|e| context(format!("does not build: {}", e.message)))?;
        let unfit = rng.chance(4);
        let sent = batches_for(&mut rng, reached, &c, &run, false, unfit, true);
        let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
        let refused = destination::write(d.as_mut(), &batches, Duration::from_secs(120))
            .map_err(|e| context(format!("stalled: {e}")))?;
        destination::check_refused(&sent, &refused, |s| s.row.unfit.is_some()).map_err(context)?;
        let landing: Vec<&Sent> = sent.iter().flatten().filter(|s| s.row.unfit.is_none()).collect();
        let items: Vec<&[u8]> = landing.iter().map(|s| s.item.as_slice()).collect();
        let all = filed(&s, &sourcetype, &items).map_err(context)?;
        for s in landing {
            let item = spaced(&String::from_utf8_lossy(&s.item));
            let times = all.matches(&item).count();
            if times != 1 {
                return Err(context(format!("{item:?} filed {times} times in:\n{all}")));
            }
        }
        for s in sent.iter().flatten().filter(|s| s.row.unfit.is_some()) {
            if all.contains(&spaced(&String::from_utf8_lossy(&s.item))) {
                return Err(context(format!(
                    "a refused row was filed: {:?}",
                    String::from_utf8_lossy(&s.item)
                )));
            }
        }
        Ok(())
    });
}
