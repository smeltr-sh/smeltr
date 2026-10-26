//! The Splunk HEC destination's fuzzers. Each seed draws one destination as a bundle names it, every option at
//! random, and sometimes breaks one of its settings.
//!
//! Offline, a destination the save refuses is refused on the setting that was broken. One it accepts loads as its
//! bundle says, loads the same after a save, builds when this worker holds its token and refuses to when it does not,
//! and delivers drawn batches to a collector in this process, over TLS when it sets its own TLS. That collector
//! answers as HEC does -- a channel where acknowledgement asks for one, an `ackId` to poll, a blank event refused by
//! its number, a busy answer, a body over its limit -- and what it took must be exactly the rows sent, each once in
//! its envelope or line, less the ones HEC refuses. A token whose acknowledgement disagrees with the destination's
//! stops delivery and says what to change. Any other change to its JSON may be refused, and never panics.
//!
//! Live, the same draws, pointed at the Splunk `just live-up` runs, land there and are searched back.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use serde_json::{Map, Value, json};
use smeltr::config_file::load_str;
use smeltr::io::{Destination, OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{
    Acknowledgement, DestinationKind, Endpoint, HeldSecrets, SecretRef, SplunkHecDestinationConfig, take_zone,
};

use crate::harness::destination::{self, Broken, Pki, Reached, connector_secrets, unknown_key_taken};
use crate::harness::fuzz::Rng;
use crate::live;

const SEEDS: u64 = 300;
const LIVE_SEEDS: u64 = 24;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

/// The token the collector in this process takes.
const TOKEN: &str = "f0220000-0000-4000-8000-000000000001";
/// A worker-local credential no test sets, so a destination naming it cannot build.
const UNSET_VARIABLE: &str = "SMELTR_FUZZ_NEVER_SET";
/// Where a collector behind a path prefix serves HEC.
const BASE_PATH: &str = "/splunk/hec";

const SUCCESS: &str = r#"{"text":"Success","code":0}"#;
const BUSY: &str = r#"{"text":"Server is busy","code":9}"#;
const CHANNEL_MISSING: &str = r#"{"text":"Data channel is missing","code":10}"#;
const ACK_DISABLED: &str = r#"{"text":"ACK is disabled","code":14}"#;
const TOO_LARGE: &str = r#"{"text":"Content too large","code":27}"#;

/// Every option the generator knows. Each must be drawn into a destination that is checked through, or into the break
/// it names, over the seed range.
const OPTIONS: &[&str] = &[
    "url.loopbackAddress",
    "url.localhost",
    "url.basePath",
    "url.trailingSlash",
    "zoneUrls.own",
    "zoneUrls.other",
    "zoneUrls.noZone",
    "token.leaderStored",
    "token.workerLocal",
    "token.lineBreak",
    "index",
    "sourcetype",
    "source",
    "host",
    "fields.encoded",
    "endpoint.event",
    "endpoint.default",
    "endpoint.raw",
    "format.json",
    "format.text",
    "format.bytes",
    "format.sourceBytes",
    "field",
    "acknowledgement.indexer",
    "acknowledgement.default",
    "acknowledgement.receipt",
    "tls.trustedAuthorities",
    "tls.certificate",
    "inFlight",
    "maxRequestBytes",
    "bufferBytes",
    "stallAfterSecs",
    "size.text",
    "row.blank",
    "row.padded",
    "row.notObject",
    "row.time.datetime",
    "row.time.epochText",
    "row.time.number",
    "row.time.junk",
    "row.filed",
    "row.filedNumber",
    "row.filedOther",
    "row.placement",
    "row.placementRefused",
    "row.text",
    "row.textLossy",
    "row.bytes",
    "mismatch.indexer",
    "mismatch.receipt",
    "mismatch.rawReceipt",
    "fault.busy",
    "fault.ackLag",
    "fault.ackBusy",
    "fault.tooLarge",
    "requests.several",
    "batches.several",
    "broken.url.empty",
    "broken.url.endpoint",
    "broken.zoneUrls",
    "broken.token",
    "broken.index",
    "broken.sourcetype",
    "broken.source",
    "broken.host",
    "broken.format.bytesOnEvent",
    "broken.field.json",
    "broken.field.blank",
    "broken.certificate",
    "broken.tls.plainHttp",
    "broken.stallAfterSecs",
    "broken.maxRequestBytes",
    "broken.bufferBytes.range",
    "broken.bufferBytes.belowRequest",
    "broken.inFlight.range",
    "broken.inFlight.affords",
    "broken.endpoint",
    "broken.acknowledgement",
    "broken.format",
    "mangled",
];

// -- drawing a destination --------------------------------------------------------------------------------------

/// Where a draw is pointed.
#[derive(Clone, Copy)]
enum Target<'a> {
    /// The collector in this process, on this loopback port.
    Offline { port: u16 },
    /// A real Splunk, and only what it takes: plain HTTP, the `main` index.
    Live(&'a live::Splunk),
}

impl Target<'_> {
    fn live(&self) -> bool {
        matches!(self, Target::Live(_))
    }
}

/// One drawn destination.
struct Draw {
    /// The destination's object in the bundle.
    destination: Map<String, Value>,
    /// Whether the destination trusts the collector's authority, and if so whether it presents a certificate.
    tls: Option<bool>,
    /// The path the collector serves HEC under.
    base_path: &'static str,
    /// The zone of the worker that runs the destination.
    zone: Option<&'static str>,
    broken: Option<Broken>,
}

fn draw(rng: &mut Rng, cov: &mut Reached, target: Target) -> Draw {
    let live = target.live();
    let mut d = Map::new();
    d.insert("id".into(), json!("hec"));
    d.insert("type".into(), json!("splunkHec"));

    let tls = (!live && rng.chance(4)).then(|| rng.chance(2));
    let mut base_path = "";
    let mut url = match target {
        Target::Live(s) => s.hec.trim_end_matches('/').to_string(),
        Target::Offline { port } => {
            let at = if tls.is_some() {
                cov.reach("url.localhost");
                format!("https://localhost:{port}")
            } else if rng.chance(2) {
                cov.reach("url.localhost");
                format!("http://localhost:{port}")
            } else {
                cov.reach("url.loopbackAddress");
                format!("http://127.0.0.1:{port}")
            };
            if rng.chance(4) {
                cov.reach("url.basePath");
                base_path = BASE_PATH;
            }
            at + base_path
        }
    };
    if rng.chance(4) {
        cov.reach("url.trailingSlash");
        url.push('/');
    }
    d.insert("url".into(), json!(url));
    let zone = if live {
        None
    } else {
        destination::draw_zones(rng, cov, &mut d)
    };

    let token = if !live && rng.chance(8) {
        cov.reach("token.workerLocal");
        json!(UNSET_VARIABLE)
    } else {
        cov.reach("token.leaderStored");
        json!({"secret": "hec-token", "version": 1 + rng.below(3)})
    };
    d.insert("token".into(), token);

    let raw = rng.chance(3);
    if raw {
        cov.reach("endpoint.raw");
        d.insert("endpoint".into(), json!("raw"));
    } else if rng.chance(2) {
        cov.reach("endpoint.event");
        d.insert("endpoint".into(), json!("event"));
    } else {
        cov.reach("endpoint.default");
    }

    let formats: &[&str] = if raw {
        &["json", "text", "bytes", "sourceBytes"]
    } else {
        &["json", "text"]
    };
    let format = *rng.pick(formats);
    cov.reach(format!("format.{format}"));
    if format != "json" || rng.chance(2) {
        d.insert("format".into(), json!(format));
    }
    if format != "json" && rng.chance(2) {
        cov.reach("field");
        // A bytes field is one the source does not declare as text.
        let fields: &[&str] = if format == "text" {
            &["message", "event.text", "_raw", "__source.bytes"]
        } else {
            &["__source.bytes"]
        };
        let field = *rng.pick(fields);
        d.insert("field".into(), json!(field));
    }

    match rng.below(3) {
        0 => {
            cov.reach("acknowledgement.indexer");
            d.insert("acknowledgement".into(), json!("indexer"));
        }
        1 => cov.reach("acknowledgement.default"),
        _ => {
            cov.reach("acknowledgement.receipt");
            d.insert("acknowledgement".into(), json!("receipt"));
        }
    }

    for key in ["index", "sourcetype", "source", "host"] {
        // Splunk parses a raw line by its sourcetype, and `_json` is one line one event whatever the line holds.
        let value = if live && raw && key == "sourcetype" {
            Some("_json".to_string())
        } else {
            rng.chance(2).then(|| filed_value(rng, target, key))
        };
        if let Some(value) = value {
            cov.reach(key);
            if raw
                && value
                    .bytes()
                    .any(|b| !b.is_ascii_alphanumeric() && !b"-._~".contains(&b))
            {
                cov.reach("fields.encoded");
            }
            d.insert(key.into(), json!(value));
        }
    }

    if let Some(certificate) = tls {
        cov.reach(if certificate {
            "tls.certificate"
        } else {
            "tls.trustedAuthorities"
        });
        set_tls(&mut d, certificate);
    }

    draw_limits(rng, cov, target, &mut d);

    let mut draw = Draw {
        destination: d,
        tls,
        base_path,
        zone,
        broken: None,
    };
    if !live && rng.chance(3) {
        draw.broken = Some(break_one(rng, cov, &mut draw));
    }
    draw
}

/// What an index, sourcetype, source or host is set to. Offline some need percent-encoding in the raw endpoint's
/// query; live, the index is one Splunk has.
fn filed_value(rng: &mut Rng, target: Target, key: &str) -> String {
    let pool: &[&str] = match (key, target.live()) {
        ("index", true) => &["main"],
        ("index", false) => &["main", "audit", "idx-2", "a b&c=d", "ünï"],
        ("sourcetype", true) => &["smeltr:fuzz", "smeltr fuzz/2"],
        ("sourcetype", false) => &["smeltr:json", "_json", "pan:traffic", "a b&c=d%"],
        ("source", true) => &["smeltr-fuzz", "/var/log/fuzz a.log", "http:x?y=z&w"],
        ("source", false) => &["smeltr fw", "/var/log/a.log", "http:x?y=z&w", "\"quoted\""],
        (_, true) => &["fw01", "host.example.test"],
        (_, false) => &["fw01", "host.example.test", "ünï-host", "a+b"],
    };
    rng.pick(pool).to_string()
}

/// A byte figure, as a number or as a size.
fn size(rng: &mut Rng, cov: &mut Reached, n: u64) -> Value {
    if !rng.chance(2) {
        return json!(n);
    }
    cov.reach("size.text");
    if n.is_multiple_of(MIB) {
        json!(format!("{} MiB", n / MIB))
    } else if n.is_multiple_of(KIB) {
        json!(format!("{}KiB", n / KIB))
    } else {
        json!(format!("{n} B"))
    }
}

/// The request size, buffer, concurrency and stall threshold, each within what the others allow.
fn draw_limits(rng: &mut Rng, cov: &mut Reached, target: Target, d: &mut Map<String, Value>) {
    let requests: &[Option<u64>] = if target.live() {
        &[None, Some(4 * KIB), Some(64 * KIB)]
    } else {
        &[
            None,
            Some(4 * KIB),
            Some(5 * KIB + 17),
            Some(64 * KIB),
            Some(16 * MIB),
            Some(100 * MIB),
        ]
    };
    let request = *rng.pick(requests);
    if let Some(r) = request {
        cov.reach("maxRequestBytes");
        d.insert("maxRequestBytes".into(), size(rng, cov, r));
    }
    let request = request.unwrap_or(MIB);
    // Unset, the buffer is the default or one whole request, whichever is larger.
    let mut buffer = (64 * MIB).max(request);
    if rng.chance(3) {
        let fits: Vec<u64> = [4 * MIB, 8 * MIB, 64 * MIB, GIB]
            .into_iter()
            .filter(|b| *b >= request)
            .collect();
        buffer = *rng.pick(&fits);
        cov.reach("bufferBytes");
        d.insert("bufferBytes".into(), size(rng, cov, buffer));
    }
    let affords = (buffer / request).min(64) as usize;
    if rng.chance(2) {
        cov.reach("inFlight");
        d.insert("inFlight".into(), json!(1 + rng.below(affords.min(8))));
    }
    if rng.chance(3) {
        cov.reach("stallAfterSecs");
        let stalls: &[u64] = if target.live() {
            &[30, 60, 120]
        } else {
            &[1, 5, 30, 120]
        };
        d.insert("stallAfterSecs".into(), json!(rng.pick(stalls)));
    }
}

/// Breaks one setting of `draw` and says how the save must refuse it.
fn break_one(rng: &mut Rng, cov: &mut Reached, draw: &mut Draw) -> Broken {
    let breaks = [
        "url.empty",
        "url.endpoint",
        "zoneUrls",
        "token",
        "index",
        "sourcetype",
        "source",
        "host",
        "format.bytesOnEvent",
        "field.json",
        "field.blank",
        "certificate",
        "tls.plainHttp",
        "stallAfterSecs",
        "maxRequestBytes",
        "bufferBytes.range",
        "bufferBytes.belowRequest",
        "inFlight.range",
        "inFlight.affords",
        "endpoint",
        "acknowledgement",
        "format",
    ];
    let what = *rng.pick(&breaks);
    cov.reach(format!("broken.{what}"));
    let d = &mut draw.destination;
    let mut set = |k: &str, v: Value| {
        d.insert(k.to_string(), v);
    };
    match what {
        "url.empty" => {
            set("url", json!(rng.pick(&["", "  "])));
            Broken::Field(Some("url"))
        }
        "zoneUrls" => Broken::Field(Some(destination::break_zones(rng, d))),
        "url.endpoint" => {
            let url = d["url"].as_str().unwrap().trim_end_matches('/').to_string();
            let endpoint = rng.pick(&[
                "/services/collector",
                "/services/collector/event",
                "/services/collector/raw",
            ]);
            d.insert("url".into(), json!(format!("{url}{endpoint}")));
            Broken::Field(Some("url"))
        }
        "token" => {
            let blank = rng
                .pick(&[json!(""), json!("  "), json!({"secret": " ", "version": 1})])
                .clone();
            set("token", blank);
            Broken::Field(Some("token"))
        }
        "index" | "sourcetype" | "source" | "host" => {
            set(what, json!(rng.pick(&["", "   "])));
            Broken::Field(Some(match what {
                "index" => "index",
                "sourcetype" => "sourcetype",
                "source" => "source",
                _ => "host",
            }))
        }
        "format.bytesOnEvent" => {
            if rng.chance(2) {
                d.remove("endpoint");
            } else {
                d.insert("endpoint".into(), json!("event"));
            }
            d.insert("format".into(), json!(rng.pick(&["bytes", "sourceBytes"])));
            Broken::Field(Some("format"))
        }
        "field.json" => {
            d.remove("format");
            d.insert("field".into(), json!("message"));
            Broken::Field(Some("field"))
        }
        "field.blank" => {
            set("format", json!("text"));
            set("field", json!(rng.pick(&["", " "])));
            Broken::Field(Some("field"))
        }
        "certificate" => {
            set("certificate", json!(" "));
            Broken::Field(Some("certificate"))
        }
        "tls.plainHttp" => {
            // TLS settings on a URL that never negotiates TLS, so nothing they say would be used.
            set("trustedAuthorities", json!(AUTHORITIES));
            set("url", json!("http://127.0.0.1:8088"));
            draw.tls = Some(false);
            Broken::Field(Some("url"))
        }
        "stallAfterSecs" => {
            set("stallAfterSecs", json!(rng.pick(&[0, 121, 3600])));
            Broken::Field(Some("stallAfterSecs"))
        }
        "maxRequestBytes" => {
            let n = *rng.pick(&[0, 4 * KIB - 1, 100 * MIB + 1, GIB]);
            d.remove("bufferBytes");
            d.insert("inFlight".into(), json!(1));
            d.insert("maxRequestBytes".into(), json!(n));
            Broken::Field(Some("maxRequestBytes"))
        }
        "bufferBytes.range" => {
            set("bufferBytes", json!(rng.pick(&[0, 4 * MIB - 1, GIB + 1])));
            Broken::Field(Some("bufferBytes"))
        }
        "bufferBytes.belowRequest" => {
            set("maxRequestBytes", json!(16 * MIB));
            set("bufferBytes", json!(8 * MIB));
            Broken::Field(Some("bufferBytes"))
        }
        "inFlight.range" => {
            set("inFlight", json!(rng.pick(&[0, 65, 1000])));
            Broken::Field(Some("inFlight"))
        }
        "inFlight.affords" => {
            // A buffer of 128 MiB holds two requests of 64 MiB.
            set("maxRequestBytes", json!("64 MiB"));
            set("bufferBytes", json!(128 * MIB));
            // Not 4: the default starts at what the buffer affords instead.
            set("inFlight", json!(rng.pick(&[3, 5, 6])));
            Broken::Field(Some("inFlight"))
        }
        "endpoint" => {
            set("endpoint", json!(rng.pick(&["collector", "Event", "/raw"])));
            Broken::Unread
        }
        "acknowledgement" => {
            set("acknowledgement", json!(rng.pick(&["none", "Indexer", "ack"])));
            Broken::Unread
        }
        "format" => {
            set("format", json!(rng.pick(&["ndjson", "JSON", "raw"])));
            Broken::Unread
        }
        other => unreachable!("{other}"),
    }
}

/// The authority, the collector's certificate under it, and a client's, minted once for every seed.
fn pki() -> &'static Pki {
    static PKI: OnceLock<Pki> = OnceLock::new();
    PKI.get_or_init(|| Pki::new("smeltr hec fuzz", "hec.internal", &["localhost"]))
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
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["hec"]}]}],
    });
    if let Some(authorities) = b["destinations"][0].get_mut("trustedAuthorities") {
        *authorities = json!(pki().authority.pem);
    }
    b
}

/// Whether `draw`'s token is one this worker never holds.
fn names_unheld_token(draw: &Draw) -> bool {
    draw.destination["token"].as_str() == Some(UNSET_VARIABLE)
}

/// What the worker holds of the credentials `draw` names, the token as `token`, and whether `draw` names a
/// credential this worker does not hold.
fn held(draw: &Draw, token: &str) -> (HeldSecrets, bool) {
    let mut stored = Vec::new();
    let named = &draw.destination["token"];
    let names_unheld = names_unheld_token(draw);
    if let Some(version) = named.get("version").and_then(Value::as_i64) {
        stored.push(("hec-token".to_string(), version, token.to_string()));
    }
    let mut secrets = HeldSecrets::default();
    secrets.set_stored(stored);
    if draw.tls == Some(true) {
        secrets.take([smeltr::heartbeat::SecretDelivery {
            name: "client".to_string(),
            version: 1,
            value: pki().client.key.clone(),
            chain: Some(pki().client.certificate.clone()),
        }]);
    }
    (secrets, names_unheld)
}

// -- what the save says ------------------------------------------------------------------------------------------

/// The destination `json` loads as.
fn loaded(json: &str) -> Result<(SplunkHecDestinationConfig, Vec<SecretRef>), String> {
    let system = load_str(json).map_err(|e| format!("{e:?}"))?;
    match &system.topology.destinations[0].kind {
        DestinationKind::SplunkHec(c) => Ok((c.clone(), connector_secrets(&system.topology))),
        other => Err(format!("loaded as {other:?}")),
    }
}

/// Checks the save: a broken draw is refused as its break says, and an unbroken one is accepted, loads as drawn, and
/// loads the same once saved.
fn check_save(draw: &Draw, json: &str) -> Result<Option<SplunkHecDestinationConfig>, String> {
    let config = destination::check_save(json, draw.broken, destination::loads, loaded)?;
    if let Some((c, named)) = &config {
        check_loaded(draw, c, named)?;
    }
    // What the worker runs: its own zone's address, taken at deploy.
    Ok(config.map(|(mut c, _)| {
        take_zone(&mut c.url, &mut c.zone_urls, draw.zone);
        c
    }))
}

/// A byte figure as drawn: a number, or a size.
fn bytes_of(v: &Value) -> u64 {
    match v {
        Value::Number(n) => n.as_u64().unwrap(),
        Value::String(s) => {
            let at = s.find(|c: char| !c.is_ascii_digit()).unwrap();
            let (n, unit) = s.split_at(at);
            let n: u64 = n.parse().unwrap();
            match unit.trim() {
                "MiB" => n * MIB,
                "KiB" => n * KIB,
                _ => n,
            }
        }
        other => panic!("not a byte figure: {other}"),
    }
}

/// What `draw` says, against what it loaded as.
fn check_loaded(draw: &Draw, c: &SplunkHecDestinationConfig, named: &[SecretRef]) -> Result<(), String> {
    let d = &draw.destination;
    let s = |key: &str| d.get(key).map(|v| v.as_str().unwrap().to_string());
    let mut wrong = Vec::new();
    let mut expect = |what: &str, want: String, got: String| {
        if want != got {
            wrong.push(format!("{what}: drawn {want}, loaded {got}"));
        }
    };
    expect("url", s("url").unwrap(), c.url.clone());
    expect(
        "zoneUrls",
        format!("{:?}", destination::drawn_zones(d)),
        format!("{:?}", c.zone_urls),
    );
    let token = match &d["token"] {
        Value::String(var) => SecretRef::WorkerLocal(var.clone()),
        v => SecretRef::LeaderStored {
            name: v["secret"].as_str().unwrap().to_string(),
            version: v["version"].as_i64().unwrap(),
        },
    };
    expect("token", format!("{token:?}"), format!("{:?}", c.token));
    expect("index", format!("{:?}", s("index")), format!("{:?}", c.index));
    expect(
        "sourcetype",
        format!("{:?}", s("sourcetype")),
        format!("{:?}", c.sourcetype),
    );
    expect("source", format!("{:?}", s("source")), format!("{:?}", c.source));
    expect("host", format!("{:?}", s("host")), format!("{:?}", c.host));
    let endpoint = match s("endpoint").as_deref() {
        Some("raw") => Endpoint::Raw,
        _ => Endpoint::Event,
    };
    expect("endpoint", format!("{endpoint:?}"), format!("{:?}", c.endpoint));
    let field = s("field");
    let payload = match s("format").as_deref() {
        None | Some("json") => Payload::Record,
        Some("text") => Payload::Text {
            field: field.unwrap_or("_raw".to_string()).into(),
        },
        _ => Payload::Bytes {
            field: field.unwrap_or("__source.bytes".to_string()).into(),
        },
    };
    expect("format", format!("{payload:?}"), format!("{:?}", c.payload));
    let acknowledgement = match s("acknowledgement").as_deref() {
        Some("receipt") => Acknowledgement::Receipt,
        _ => Acknowledgement::Indexer,
    };
    expect(
        "acknowledgement",
        format!("{acknowledgement:?}"),
        format!("{:?}", c.acknowledgement),
    );
    expect(
        "tls",
        format!("{:?}", draw.tls),
        format!("{:?}", c.tls.as_ref().map(|t| t.certificate.is_some())),
    );
    let request = d.get("maxRequestBytes").map_or(MIB, bytes_of);
    expect("maxRequestBytes", request.to_string(), c.max_request_bytes.to_string());
    let buffer = d.get("bufferBytes").map_or((64 * MIB).max(request), bytes_of);
    expect("bufferBytes", buffer.to_string(), c.buffer_bytes.to_string());
    // Unstated, the start is the default or what the buffer affords, whichever is fewer.
    let affords = (c.buffer_bytes / c.max_request_bytes.max(1)).max(1) as u64;
    let in_flight = d.get("inFlight").map_or(4.min(affords), |v| v.as_u64().unwrap());
    expect("inFlight", in_flight.to_string(), c.in_flight.to_string());
    let stall = d.get("stallAfterSecs").map_or(30, |v| v.as_u64().unwrap());
    expect("stallAfterSecs", stall.to_string(), c.stall_after.as_secs().to_string());
    let named: Vec<String> = named.iter().map(|r| format!("{r:?}")).collect();
    expect(
        "credentials",
        format!("{:?}", [format!("{token:?}")]),
        format!("{named:?}"),
    );
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(wrong.join("\n"))
    }
}

// -- the rows ----------------------------------------------------------------------------------------------------

/// What a row must become at the collector: the envelope it lands in, or the line and the query it came with. `None`
/// is a row HEC refuses alone.
#[derive(Clone, Debug)]
enum Landed {
    Event(Value),
    Raw {
        query: BTreeMap<String, String>,
        line: Vec<u8>,
    },
}

impl Landed {
    /// The same for two landings that are the same, whatever order their keys came in.
    fn canonical(&self) -> String {
        match self {
            Landed::Event(v) => format!("event {}", canonical(v)),
            Landed::Raw { query, line } => format!("raw {query:?} {line:?}"),
        }
    }
}

fn canonical(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .iter()
                .map(|k| format!("{}:{}", Value::String((*k).clone()), canonical(&m[*k])))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        Value::Array(a) => format!("[{}]", a.iter().map(canonical).collect::<Vec<_>>().join(",")),
        other => other.to_string(),
    }
}

struct Sent {
    row: OwnedRecord,
    landed: Option<Landed>,
    /// What the refusal of a row refused before it is sent must say.
    refused_for: Option<String>,
}

/// Text with what JSON escapes and what a line break would split, or, live, what Splunk keeps as it is.
fn text(rng: &mut Rng, live: bool, line_breaks: bool) -> String {
    let pool: &[char] = if live {
        &['a', 'Z', '0', '"', '\\', 'é', '✓', '{', '}', ':', '=', ',']
    } else if line_breaks {
        &[
            'a', 'Z', '0', ' ', '"', '\\', '\t', '\n', '\r', 'é', '✓', '\u{2028}', '\u{0}', '{', '}',
        ]
    } else {
        &[
            'a', 'Z', '0', ' ', '"', '\\', '\t', '\r', 'é', '✓', '\u{2028}', '\u{0}', '{', '}',
        ]
    };
    (0..rng.below(24)).map(|_| *rng.pick(pool)).collect()
}

/// A `_time`, and the HEC `time` it is filed under: Unix seconds with six decimals, or a number as it stands. `None`
/// leaves the event at the time Splunk receives it.
fn draw_time(rng: &mut Rng, cov: &mut Reached, live: bool) -> (Value, Option<&'static str>) {
    match rng.below(4) {
        0 => {
            cov.reach("row.time.datetime");
            let mut times = vec![
                ("2026-08-25T12:00:00.250000Z", "1787659200.250000"),
                ("2026-08-25T12:00:00Z", "1787659200.000000"),
                ("2026-08-25T14:00:00.000001+02:00", "1787659200.000001"),
            ];
            if !live {
                times.push(("1969-12-31T23:59:58.500000Z", "-1.500000"));
            }
            let (text, time) = *rng.pick(&times);
            (json!(text), Some(time))
        }
        1 => {
            cov.reach("row.time.epochText");
            (json!("1787659200.5"), Some("1787659200.500000"))
        }
        2 => {
            cov.reach("row.time.number");
            let numbers: &[&'static str] = if live {
                &["1787659200.5", "1787659200"]
            } else {
                &["1787659200.5", "1787659200", "-1.5", "0"]
            };
            let n = *rng.pick(numbers);
            (serde_json::from_str(n).unwrap(), Some(n))
        }
        _ => {
            cov.reach("row.time.junk");
            (
                rng.pick(&[json!("not a time"), json!(""), json!(true), json!({"at": 1})])
                    .clone(),
                None,
            )
        }
    }
}

/// A host, source or sourcetype the document carries, and what it is filed under: a string or a number as its text,
/// anything else nothing.
fn draw_filed(rng: &mut Rng, cov: &mut Reached, live: bool) -> (Value, Option<String>) {
    match rng.below(4) {
        0 | 1 => {
            cov.reach("row.filed");
            let s = *rng.pick(&["fw02", "doc source", "pan:traffic", "ünï"]);
            (json!(s), Some(s.to_string()))
        }
        2 => {
            cov.reach("row.filedNumber");
            let n: &str = rng.pick(&["7", "-3", "2.5"]);
            (serde_json::from_str(n).unwrap(), Some(n.to_string()))
        }
        _ if live => (json!("fw03"), Some("fw03".to_string())),
        _ => {
            cov.reach("row.filedOther");
            (
                rng.pick(&[json!({"a": 1}), json!(true), json!(null), json!([1])])
                    .clone(),
                None,
            )
        }
    }
}

/// One row for `c`, numbered `n`, and what it must become. Live, every row carries `marker`, so a search finds it.
/// `blanks` lets it be a row HEC refuses.
fn row_for(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &SplunkHecDestinationConfig,
    marker: Option<&str>,
    n: usize,
    blanks: bool,
) -> Sent {
    let live = marker.is_some();
    let placement = rng.chance(4).then(|| {
        cov.reach("row.placement");
        let mut pool: Vec<&str> = if live {
            vec!["main"]
        } else {
            vec!["main", "routed", "a\",\"event\":\"forged"]
        };
        // On the raw endpoint a row naming the destination's own index is the one that lands.
        if c.endpoint == Endpoint::Raw
            && let Some(own) = c.index.as_deref()
        {
            pool.push(own);
        }
        rng.pick(&pool).to_string()
    });
    let marker = marker.unwrap_or("fuzz");
    let row = |value: Vec<u8>, payload: &Payload| OwnedRecord {
        value,
        payload: payload.clone(),
        placement: placement.clone(),
        ..OwnedRecord::default()
    };

    if c.endpoint == Endpoint::Raw {
        // Each row is one line of the body, and the destination's own fields are the query.
        let query: BTreeMap<String, String> = [
            ("index", &c.index),
            ("sourcetype", &c.sourcetype),
            ("source", &c.source),
            ("host", &c.host),
        ]
        .into_iter()
        .filter_map(|(k, v)| Some((k.to_string(), v.clone()?)))
        .collect();
        // Live, the sourcetype is `_json`, so every line is a JSON object.
        let record = serde_json::to_vec(&json!({"run": marker, "n": n, "msg": text(rng, live, false)})).unwrap();
        let value = match &c.payload {
            Payload::Record => record,
            Payload::Text { .. } if live => record,
            Payload::Text { .. } => {
                cov.reach("row.text");
                text(rng, false, false).into_bytes()
            }
            Payload::Bytes { .. } if live => record,
            Payload::Bytes { .. } => {
                cov.reach("row.bytes");
                (0..rng.below(20))
                    .map(|_| *rng.pick(&[b'a', b'0', b' ', b'\\', b'\r', b'"', 0xe9, 0xff, 0x00, b'%']))
                    .collect()
            }
        };
        // The raw endpoint files every row of a request under the destination's index, so a row naming another is
        // refused alone, before it is sent.
        if let Some(own) = placement.as_ref().filter(|p| Some(*p) != c.index.as_ref()) {
            cov.reach("row.placementRefused");
            let filed = c
                .index
                .as_deref()
                .map_or("the token's default index".to_string(), |i| format!("{i:?}"));
            return Sent {
                row: row(value, &c.payload),
                landed: None,
                refused_for: Some(format!(
                    "names the index {own:?}, and the raw endpoint files every row under {filed}"
                )),
            };
        }
        return Sent {
            row: row(value.clone(), &c.payload),
            landed: Some(Landed::Raw { query, line: value }),
            refused_for: None,
        };
    }

    let mut envelope = Map::new();
    let index = placement.clone().or(c.index.clone());
    let mut filed: [(&str, Option<String>); 3] = [("sourcetype", None), ("source", None), ("host", None)];
    let (value, event) = match &c.payload {
        Payload::Text { .. } => {
            cov.reach("row.text");
            let lossy = !live && rng.chance(8);
            let mut value = if live {
                format!("{marker} n={n} {}", text(rng, true, true)).into_bytes()
            } else if blanks && !lossy && rng.chance(8) {
                Vec::new()
            } else {
                // Numbered, so every row that lands is one of a kind.
                format!("{n} {}", text(rng, false, true)).into_bytes()
            };
            if lossy {
                cov.reach("row.textLossy");
                value.extend_from_slice(&[0xff, b'x', 0xc3]);
            }
            let event = String::from_utf8_lossy(&value).into_owned();
            (value, json!(event))
        }
        _ => match rng.below(8) {
            0 if blanks => {
                cov.reach("row.blank");
                let blank: &[u8] = if live {
                    b""
                } else {
                    rng.pick(&[&b""[..], b"  ", b"\n", b" \r\n "])
                };
                (blank.to_vec(), json!(""))
            }
            1 => {
                cov.reach("row.notObject");
                let mut shapes = vec![json!([marker, n]), json!(format!("{marker} n={n}"))];
                if !live {
                    shapes.push(json!(n));
                }
                let doc = rng.pick(&shapes).clone();
                (serde_json::to_vec(&doc).unwrap(), doc)
            }
            _ => {
                let mut doc = Map::new();
                doc.insert("run".into(), json!(marker));
                doc.insert("n".into(), json!(n));
                doc.insert("msg".into(), json!(text(rng, live, true)));
                if rng.chance(2) {
                    let (time, filed_at) = draw_time(rng, cov, live);
                    doc.insert("_time".into(), time);
                    if let Some(at) = filed_at {
                        envelope.insert("time".into(), serde_json::from_str(at).unwrap());
                    }
                }
                for (key, at) in filed.iter_mut() {
                    if rng.chance(4) {
                        let (value, text) = draw_filed(rng, cov, live);
                        doc.insert(key.to_string(), value);
                        *at = text;
                    }
                }
                let doc = Value::Object(doc);
                let mut value = serde_json::to_vec(&doc).unwrap();
                if !live && rng.chance(4) {
                    cov.reach("row.padded");
                    value = [&b" \t"[..], &value, b"\r\n"].concat();
                }
                (value, doc)
            }
        },
    };
    if let Some(index) = index {
        envelope.insert("index".into(), json!(index));
    }
    for ((key, own), default) in filed.into_iter().zip([&c.sourcetype, &c.source, &c.host]) {
        if let Some(v) = own.or(default.clone()) {
            envelope.insert(key.into(), json!(v));
        }
    }
    let blank = event == json!("");
    envelope.insert("event".into(), event);
    Sent {
        row: row(value, &c.payload),
        landed: (!blank).then_some(Landed::Event(Value::Object(envelope))),
        refused_for: None,
    }
}

/// One to three batches for `c`, sometimes large enough to take several requests.
fn batches_for(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &SplunkHecDestinationConfig,
    marker: Option<&str>,
) -> Vec<Vec<Sent>> {
    let count = 1 + rng.below(3);
    if count > 1 {
        cov.reach("batches.several");
    }
    let mut n = 0;
    (0..count)
        .map(|_| {
            let rows = if c.max_request_bytes <= 64 * 1024 && rng.chance(3) {
                100 + rng.below(150)
            } else {
                1 + rng.below(12)
            };
            // Live, only a batch that fits one request holds a row Splunk refuses: which rows may come again depends
            // on what shared its request, and only the collector in this process can say what that was.
            let blanks = marker.is_none() || rows <= 12;
            (0..rows)
                .map(|_| {
                    n += 1;
                    row_for(rng, cov, c, marker, n, blanks)
                })
                .collect()
        })
        .collect()
}

/// Checks that each batch refused exactly the rows HEC refuses, and a row refused before it is sent for its reason.
fn check_refused(sent: &[Vec<Sent>], refused: &[Vec<Rejection>]) -> Result<(), String> {
    destination::check_refused(sent, refused, |s| s.landed.is_none())?;
    for (batch, rows) in sent.iter().enumerate() {
        for Rejection { doc, reason, .. } in &refused[batch] {
            if let Some(says) = &rows[*doc].refused_for
                && !reason.contains(says.as_str())
            {
                return Err(format!(
                    "batch {batch}: row {doc} refused as {reason:?}, not for {says:?}"
                ));
            }
        }
    }
    Ok(())
}

/// Whether every row of `sent` is refused before it is sent, so no request goes at all.
fn none_sent(sent: &[Vec<Sent>]) -> bool {
    sent.iter().flatten().all(|s| s.refused_for.is_some())
}

/// What the error says when the token's acknowledgement is not the destination's.
fn mismatch_says(token_acks: bool) -> &'static str {
    if token_acks {
        "indexer acknowledgement on"
    } else {
        "indexer acknowledgement off"
    }
}

fn mismatch_option(c: &SplunkHecDestinationConfig) -> &'static str {
    match (c.acknowledgement, c.endpoint) {
        (Acknowledgement::Indexer, _) => "mismatch.indexer",
        (Acknowledgement::Receipt, Endpoint::Event) => "mismatch.receipt",
        (Acknowledgement::Receipt, Endpoint::Raw) => "mismatch.rawReceipt",
    }
}

fn build(c: &SplunkHecDestinationConfig, secrets: &HeldSecrets) -> Result<Box<dyn Destination>, String> {
    let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
    smeltr::io::build_destination(&DestinationKind::SplunkHec(c.clone()), 0, metrics, secrets, None)
        .map_err(|e| e.to_string())
}

// -- the collector in this process -------------------------------------------------------------------------------

/// What the collector does wrong on purpose, once each. The busy faults key on the request that carries `needle`
/// rather than on the first to arrive, so a replayed seed meets them where it did, however its senders interleave.
#[derive(Default)]
struct Faults {
    /// Answers the first request carrying the needle busy.
    busy: bool,
    /// Polls of the ackId of the request that lands the needle that answer not yet.
    ack_lag: usize,
    /// Answers the first poll of that ackId busy.
    ack_busy: bool,
    /// The largest body it takes.
    limit: Option<usize>,
    /// One landing, of a kind in its seed, as [`Landed::canonical`] writes it.
    needle: Option<String>,
}

/// How the collector is set up: its token, whether that token has indexer acknowledgement on, and where it serves.
struct Hec {
    token: String,
    acks_on: bool,
    base_path: &'static str,
    faults: Faults,
}

/// What the collector saw.
#[derive(Default)]
struct Seen {
    landed: Vec<Landed>,
    /// Every request whose events were read, in arrival order.
    log: Vec<Logged>,
    next_ack: HashMap<String, u64>,
    /// Each issued ackId by its channel, and the polls left before it is indexed.
    unacked: HashMap<(String, u64), usize>,
    /// The ackId of the request that landed the needle.
    needle_ack: Option<(String, u64)>,
    busy_sent: bool,
    ack_busy_sent: bool,
    faults: Vec<&'static str>,
    /// What a client of HEC must never send.
    wrong: Vec<String>,
}

/// One request whose events were read: each line's landing as [`Landed::canonical`] writes it, `None` for an event
/// refused, and whether the request landed.
struct Logged {
    lines: Vec<Option<String>>,
    landed: bool,
}

struct Request {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A collector serving HEC on a loopback port, answering as Splunk does, until dropped.
struct FakeHec {
    seen: Arc<Mutex<Seen>>,
    stop: Arc<AtomicBool>,
    port: u16,
}

impl FakeHec {
    fn start(listener: TcpListener, hec: Hec, tls: Option<Arc<rustls::ServerConfig>>) -> FakeHec {
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Seen::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let (serving, stopping) = (seen.clone(), stop.clone());
        let hec = Arc::new(hec);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stopping.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
                let _ = stream.set_nodelay(true);
                let (hec, seen, tls) = (hec.clone(), serving.clone(), tls.clone());
                std::thread::spawn(move || match tls {
                    Some(config) => {
                        let Ok(connection) = rustls::ServerConnection::new(config) else {
                            return;
                        };
                        converse(rustls::StreamOwned::new(connection, stream), &hec, &seen);
                    }
                    None => converse(stream, &hec, &seen),
                });
            }
        });
        FakeHec { seen, stop, port }
    }
}

impl Drop for FakeHec {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
    }
}

/// Answers requests on one connection until the client closes it.
fn converse<S: Read + Write>(stream: S, hec: &Hec, seen: &Mutex<Seen>) {
    let mut reader = BufReader::new(stream);
    while let Some(request) = read_request(&mut reader) {
        let (status, body) = answer(hec, &mut seen.lock().unwrap(), &request);
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            403 => "Forbidden",
            404 => "Not Found",
            413 => "Payload Too Large",
            _ => "Service Unavailable",
        };
        let head = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json; charset=UTF-8\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        // One write, so the answer is not held back waiting on the client to acknowledge its head.
        let out = reader.get_mut();
        if out
            .write_all(format!("{head}{body}").as_bytes())
            .and_then(|()| out.flush())
            .is_err()
        {
            return;
        }
    }
}

fn read_request(reader: &mut impl BufRead) -> Option<Request> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let (method, target) = (parts.next()?.to_string(), parts.next()?.to_string());
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    let mut request = Request {
        method,
        target,
        headers,
        body: Vec::new(),
    };
    let length: usize = request.header("content-length").map_or(0, |v| v.parse().unwrap_or(0));
    request.body = vec![0; length];
    reader.read_exact(&mut request.body).ok()?;
    Some(request)
}

fn percent_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            out.push(u8::from_str_radix(text.get(i + 1..i + 3)?, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn is_guid(s: &str) -> bool {
    let lengths: Vec<usize> = s.split('-').map(str::len).collect();
    lengths == [8, 4, 4, 4, 12] && s.bytes().all(|b| b == b'-' || b.is_ascii_hexdigit())
}

fn invalid_event(text: &str, code: u32, at: usize) -> String {
    json!({"text": text, "code": code, "invalid-event-number": at}).to_string()
}

fn answer(hec: &Hec, seen: &mut Seen, r: &Request) -> (u16, String) {
    let (path, query) = r.target.split_once('?').unwrap_or((&r.target, ""));
    let Some(path) = path.strip_prefix(hec.base_path) else {
        seen.wrong
            .push(format!("a request outside the collector: {}", r.target));
        return (404, String::new());
    };
    if r.method != "POST" {
        seen.wrong.push(format!("{} {}", r.method, r.target));
    }
    if r.header("authorization") != Some(&format!("Splunk {}", hec.token)) {
        seen.wrong
            .push(format!("authorization {:?}", r.header("authorization")));
        return (403, r#"{"text":"Invalid token","code":4}"#.to_string());
    }
    if r.header("content-type") != Some("application/json") {
        seen.wrong.push(format!("content type {:?}", r.header("content-type")));
    }
    let channel = r.header("x-splunk-request-channel").map(str::to_string);
    if let Some(c) = &channel
        && !is_guid(c)
    {
        seen.wrong.push(format!("a channel that is no GUID: {c:?}"));
    }
    match path {
        "/services/collector/ack" => ack(hec, seen, r, channel),
        "/services/collector/event" => {
            if !query.is_empty() {
                seen.wrong.push(format!("the event endpoint with a query: {query}"));
            }
            data(hec, seen, r, channel, None)
        }
        "/services/collector/raw" => {
            let mut fields = BTreeMap::new();
            for pair in query.split('&').filter(|p| !p.is_empty()) {
                let decoded = pair
                    .split_once('=')
                    .and_then(|(k, v)| Some((percent_decode(k)?, percent_decode(v)?)));
                match decoded {
                    Some((k, v)) => {
                        fields.insert(k, v);
                    }
                    None => seen.wrong.push(format!("a query that does not decode: {query}")),
                }
            }
            data(hec, seen, r, channel, Some(fields))
        }
        other => {
            seen.wrong.push(format!("a request to {other}"));
            (
                404,
                r#"{"text":"The requested URL was not found on this server.","code":404}"#.to_string(),
            )
        }
    }
}

fn ack(hec: &Hec, seen: &mut Seen, r: &Request, channel: Option<String>) -> (u16, String) {
    if !hec.acks_on {
        return (400, ACK_DISABLED.to_string());
    }
    let Some(channel) = channel else {
        seen.wrong.push("an ack poll on no channel".to_string());
        return (400, CHANNEL_MISSING.to_string());
    };
    let asked: Value = serde_json::from_slice(&r.body).unwrap_or_default();
    let Some(ids) = asked["acks"]
        .as_array()
        .and_then(|a| a.iter().map(Value::as_u64).collect::<Option<Vec<u64>>>())
    else {
        seen.wrong.push(format!("an ack poll of {asked}"));
        return (400, r#"{"text":"Invalid data format","code":6}"#.to_string());
    };
    let needle = seen
        .needle_ack
        .as_ref()
        .is_some_and(|(c, id)| *c == channel && ids.contains(id));
    if needle && hec.faults.ack_busy && !seen.ack_busy_sent {
        seen.ack_busy_sent = true;
        seen.faults.push("fault.ackBusy");
        return (503, BUSY.to_string());
    }
    let mut acks = Map::new();
    for id in ids {
        let indexed = match seen.unacked.get_mut(&(channel.clone(), id)) {
            Some(0) => true,
            Some(left) => {
                *left -= 1;
                seen.faults.push("fault.ackLag");
                false
            }
            // Never issued on this channel, or the ask whether acknowledgement is on.
            None => false,
        };
        acks.insert(id.to_string(), json!(indexed));
    }
    (200, json!({"acks": acks}).to_string())
}

/// A request of events, or with `raw` the query of the raw endpoint.
fn data(
    hec: &Hec,
    seen: &mut Seen,
    r: &Request,
    channel: Option<String>,
    raw: Option<BTreeMap<String, String>>,
) -> (u16, String) {
    let Some(channel) = channel.or_else(|| (!hec.acks_on && raw.is_none()).then(String::new)) else {
        return (400, CHANNEL_MISSING.to_string());
    };
    if !channel.is_empty() && !hec.acks_on && raw.is_none() {
        seen.wrong
            .push("a channel on the event endpoint under receipt".to_string());
    }
    let Some(body) = r.body.strip_suffix(b"\n") else {
        seen.wrong.push(format!(
            "a body not ending in a line break: {:?}",
            String::from_utf8_lossy(&r.body)
        ));
        return (400, r#"{"text":"No data","code":5}"#.to_string());
    };
    // Each line's landing, `None` for a blank event, which HEC refuses.
    let mut lines: Vec<Option<Landed>> = Vec::new();
    for (i, line) in body.split(|b| *b == b'\n').enumerate() {
        let Some(query) = &raw else {
            let Ok(envelope) = serde_json::from_slice::<Value>(line) else {
                seen.wrong.push(format!(
                    "an envelope that is no JSON: {:?}",
                    String::from_utf8_lossy(line)
                ));
                return (400, invalid_event("Invalid data format", 6, i));
            };
            check_envelope(&envelope, &mut seen.wrong);
            match envelope.get("event") {
                None => {
                    seen.wrong.push(format!("an envelope with no event: {envelope}"));
                    return (400, invalid_event("Event field is required", 12, i));
                }
                Some(Value::String(s)) if s.is_empty() => lines.push(None),
                Some(_) => lines.push(Some(Landed::Event(envelope))),
            }
            continue;
        };
        lines.push(Some(Landed::Raw {
            query: query.clone(),
            line: line.to_vec(),
        }));
    }
    let canonical: Vec<Option<String>> = lines.iter().map(|l| l.as_ref().map(Landed::canonical)).collect();
    let needle = (hec.faults.needle.as_ref()).is_some_and(|n| canonical.iter().flatten().any(|c| c == n));
    if needle && hec.faults.busy && !seen.busy_sent {
        seen.busy_sent = true;
        seen.faults.push("fault.busy");
        return (503, BUSY.to_string());
    }
    if hec.faults.limit.is_some_and(|limit| r.body.len() > limit) {
        seen.faults.push("fault.tooLarge");
        return (413, TOO_LARGE.to_string());
    }
    let refused = lines.iter().position(Option::is_none);
    seen.log.push(Logged {
        lines: canonical,
        landed: refused.is_none(),
    });
    if let Some(at) = refused {
        // As Splunk, the events before one it refuses are queued all the same.
        seen.landed.extend(lines.into_iter().take(at).flatten());
        return (400, invalid_event("Event field cannot be blank", 13, at));
    }
    seen.landed.extend(lines.into_iter().flatten());
    if !hec.acks_on {
        return (200, SUCCESS.to_string());
    }
    let next = seen.next_ack.entry(channel.clone()).or_default();
    let id = *next;
    *next += 1;
    let lag = if needle { hec.faults.ack_lag } else { 0 };
    if needle {
        seen.needle_ack = Some((channel.clone(), id));
    }
    seen.unacked.insert((channel, id), lag);
    (200, json!({"text": "Success", "code": 0, "ackId": id}).to_string())
}

/// Notes in `wrong` what an envelope holds that HEC does not file an event under.
fn check_envelope(envelope: &Value, wrong: &mut Vec<String>) {
    let Some(fields) = envelope.as_object() else {
        wrong.push(format!("an envelope that is no object: {envelope}"));
        return;
    };
    for (key, value) in fields {
        let fits = match key.as_str() {
            "time" => value.is_number(),
            "index" | "sourcetype" | "source" | "host" => value.is_string(),
            "event" => true,
            _ => false,
        };
        if !fits {
            wrong.push(format!("an envelope with {key}: {value}"));
        }
    }
}

/// Checks that the collector took the rows `sent` that it did not refuse, and nothing else: each once, or once more
/// at most for each refused event after it in the first request that carried it. Splunk queues the events before
/// one it refuses, and they go again with the rest of the request.
fn check_landed(sent: &[Vec<Sent>], seen: &Seen) -> Result<(), String> {
    if !seen.wrong.is_empty() {
        return Err(format!("the collector was sent {:?}", seen.wrong));
    }
    let mut times: HashMap<String, usize> = HashMap::new();
    for landed in sent.iter().flatten().filter_map(|s| s.landed.as_ref()) {
        *times.entry(landed.canonical()).or_default() += 1;
    }
    let mut got: HashMap<String, usize> = HashMap::new();
    for landed in &seen.landed {
        *got.entry(landed.canonical()).or_default() += 1;
    }
    let mut wrong: Vec<String> = Vec::new();
    for (landing, times) in &times {
        let again = seen
            .log
            .iter()
            .find_map(|request| {
                let at = request.lines.iter().position(|l| l.as_ref() == Some(landing))?;
                Some(request.lines[at + 1..].iter().filter(|l| l.is_none()).count())
            })
            .unwrap_or(0);
        let n = got.remove(landing).unwrap_or(0);
        if n < *times || n > times + again {
            wrong.push(format!(
                "{landing} sent {times} times, may come {again} more, landed {n}"
            ));
        }
    }
    wrong.extend(got.iter().map(|(landed, n)| format!("{landed} never sent, landed {n}")));
    if wrong.is_empty() {
        return Ok(());
    }
    wrong.sort();
    wrong.truncate(6);
    Err(wrong.join("\n"))
}

/// Whether one batch went out in several requests that landed, each holding only its rows. A row lands in one
/// request, so with no body limit to split them that is `maxRequestBytes` at work.
fn split_by_size(sent: &[Vec<Sent>], seen: &Seen) -> bool {
    sent.iter().any(|batch| {
        let rows: std::collections::HashSet<String> = batch
            .iter()
            .filter_map(|s| s.landed.as_ref().map(Landed::canonical))
            .collect();
        let holding = (seen.log.iter())
            .filter(|r| r.landed && !r.lines.is_empty() && r.lines.iter().flatten().all(|l| rows.contains(l)))
            .count();
        holding > 1
    })
}

/// The first landing of the first batch that is the only one of its kind in the seed: what the busy faults key on.
fn needle(sent: &[Vec<Sent>]) -> Option<String> {
    let all: Vec<String> = sent
        .iter()
        .flatten()
        .filter_map(|s| s.landed.as_ref().map(Landed::canonical))
        .collect();
    let first = sent.first()?;
    first
        .iter()
        .filter_map(|s| s.landed.as_ref().map(Landed::canonical))
        .find(|c| all.iter().filter(|a| *a == c).count() == 1)
}

// -- the offline fuzzer -----------------------------------------------------------------------------------------

/// Each seed's destination, against the save, the destination it builds, and a collector in this process.
#[test]
fn splunk_hec_destination_fuzz() {
    destination::run(0..SEEDS, OPTIONS.iter().copied(), offline_seed);
}

fn offline_seed(seed: u64, reached: &mut Reached) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut draw = draw(&mut rng, reached, Target::Offline { port });
    let mangled = draw.broken.is_none() && rng.chance(12);
    reached.only_breaks = mangled || draw.broken.is_some();
    let unknown = mangled
        && destination::mangle(
            &mut rng,
            reached,
            &mut draw.destination,
            &["unknownSetting", "compression", "channel", "indexes"],
        );
    let json = bundle(&draw).to_string();
    if mangled {
        if unknown && unknown_key_taken(&json) {
            return Err(format!("a key it does not know is taken\n{json}"));
        }
        return Ok(());
    }
    let Some(c) = check_save(&draw, &json).map_err(|e| format!("{e}\n{json}"))? else {
        return Ok(());
    };

    // A token holding a line break would split the Authorization header: the build refuses it, naming the reference
    // and never the value.
    if !names_unheld_token(&draw) && rng.chance(12) {
        let broken = format!("{TOKEN}\n");
        let (secrets, _) = held(&draw, &broken);
        reached.keep_only(&[]);
        return match build(&c, &secrets) {
            Err(e) if e.contains("line break or control character") && !e.contains(TOKEN) => {
                reached.reach("token.lineBreak");
                Ok(())
            }
            Err(e) => Err(format!("a token with a line break is refused as {e:?}\n{json}")),
            Ok(_) => Err(format!("built with a token holding a line break\n{json}")),
        };
    }
    let (secrets, names_unheld) = held(&draw, TOKEN);
    let mut d = match (build(&c, &secrets), names_unheld) {
        (Ok(d), false) => d,
        (Err(e), true) if e.contains(UNSET_VARIABLE) => {
            // Only the refusal to build was checked: nothing was delivered under the rest of the draw.
            reached.keep_only(&["token.workerLocal"]);
            return Ok(());
        }
        (Ok(_), true) => return Err(format!("built with a token it does not hold\n{json}")),
        (Err(e), _) => return Err(format!("does not build: {e}\n{json}")),
    };

    let mismatch = rng.chance(6);
    let token_acks = (c.acknowledgement == Acknowledgement::Indexer) != mismatch;
    let sent = batches_for(&mut rng, reached, &c, None);
    // Each wait fault costs 100 ms or more, so few seeds draw one, and none whose stall threshold all of them together
    // with the round trips of a split could come near.
    let waits = !mismatch && c.stall_after >= Duration::from_secs(5);
    let faults = if mismatch {
        Faults::default()
    } else {
        Faults {
            busy: waits && rng.chance(14),
            ack_lag: if waits && rng.chance(14) { 1 + rng.below(2) } else { 0 },
            ack_busy: waits && rng.chance(14),
            limit: rng.chance(5).then_some(2048),
            needle: needle(&sent),
        }
    };
    let split_limit = faults.limit;
    let tls = draw.tls.map(|certificate| {
        if certificate {
            pki().mutual()
        } else {
            pki().server_only()
        }
    });
    let hec = Hec {
        token: TOKEN.to_string(),
        acks_on: token_acks,
        base_path: draw.base_path,
        faults,
    };
    let collector = FakeHec::start(listener, hec, tls);

    let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
    let written = destination::write(d.as_mut(), &batches, Duration::from_secs(60));
    drop(d);
    let seen = collector.seen.lock().unwrap();
    if mismatch {
        // No row was delivered, so what the rows drew was not checked.
        reached.forget_where(|n| n.starts_with("row.") || n == "batches.several");
        let says = mismatch_says(token_acks);
        return match written {
            Err(e) if e.contains(says) && seen.landed.is_empty() => {
                reached.reach(mismatch_option(&c));
                Ok(())
            }
            // Every row was refused before a request went, so the token was never asked.
            Ok(refused) if none_sent(&sent) && seen.landed.is_empty() => {
                check_refused(&sent, &refused).map_err(|e| format!("{e}\n{json}"))
            }
            Err(e) => Err(format!(
                "on a token with acknowledgement {token_acks}, failed with {e}\n{json}"
            )),
            Ok(_) => Err(format!(
                "delivered on a token with acknowledgement {token_acks}\n{json}"
            )),
        };
    }
    let refused = written.map_err(|e| format!("{e}\n{json}"))?;
    check_refused(&sent, &refused).map_err(|e| format!("{e}\n{json}"))?;
    check_landed(&sent, &seen).map_err(|e| format!("{e}\n{json}"))?;
    for fault in &seen.faults {
        reached.reach(*fault);
    }
    if split_limit.is_none() && split_by_size(&sent, &seen) {
        reached.reach("requests.several");
    }
    Ok(())
}

// -- the live fuzzer --------------------------------------------------------------------------------------------

/// A search term no earlier run wrote: letters and digits only, so Splunk indexes it whole.
fn marker(seed: u64) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("fz{nanos}s{seed}")
}

/// What a live draw reaches: every option a real Splunk on plain HTTP takes.
fn live_options() -> Vec<&'static str> {
    OPTIONS
        .iter()
        .copied()
        .filter(|o| {
            !o.starts_with("broken.")
                && !o.starts_with("fault.")
                && !o.starts_with("tls.")
                // One Splunk, at one address.
                && !o.starts_with("zoneUrls.")
                && ![
                    "mangled",
                    "requests.several",
                    "url.loopbackAddress",
                    "url.localhost",
                    "url.basePath",
                    "token.workerLocal",
                    "token.lineBreak",
                    "row.placementRefused",
                    "row.padded",
                    "row.filedOther",
                    "row.textLossy",
                    "row.bytes",
                    "fields.encoded",
                ]
                .contains(o)
        })
        .collect()
}

/// One event as a search returns it.
#[derive(Debug)]
struct Found {
    raw: String,
    /// Unix seconds with six decimals.
    time: String,
    fields: BTreeMap<String, String>,
}

/// The events Splunk holds that carry `marker`. Asked until `enough` holds of them or `within` passes, since an event
/// is searchable a moment after it is indexed.
fn search(
    s: &live::Splunk,
    marker: &str,
    enough: impl Fn(&[Found]) -> bool,
    within: Duration,
) -> Result<Vec<Found>, String> {
    let agent = ureq::Agent::new_with_defaults();
    let auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(&s.auth));
    let deadline = Instant::now() + within;
    loop {
        let body = agent
            .post(format!("{}/services/search/jobs/export", s.api))
            .header("Authorization", &auth)
            .send_form([
                (
                    "search",
                    format!("search index=main {marker} | table _raw _time host source sourcetype index").as_str(),
                ),
                ("output_mode", "json"),
                ("earliest_time", "0"),
                ("latest_time", "+10y"),
                ("time_format", "%s.%6N"),
            ])
            .map_err(|e| format!("searching Splunk: {e}"))?
            .body_mut()
            .read_to_string()
            .map_err(|e| e.to_string())?;
        let found: Vec<Found> = body
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter_map(|v| {
                let r = v.get("result")?;
                let fields = ["host", "source", "sourcetype", "index"]
                    .into_iter()
                    .filter_map(|k| Some((k.to_string(), r.get(k)?.as_str()?.to_string())))
                    .collect();
                Some(Found {
                    raw: r["_raw"].as_str()?.to_string(),
                    time: r["_time"].as_str()?.to_string(),
                    fields,
                })
            })
            .collect();
        if enough(&found) || Instant::now() > deadline {
            return Ok(found);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Seconds with six decimals, from HEC's `time`.
fn six_decimals(time: &Value) -> String {
    let micros = (time.as_f64().unwrap() * 1e6).round() as i64;
    format!("{}.{:06}", micros.div_euclid(1_000_000), micros.rem_euclid(1_000_000))
}

/// Whether `found` is the event `landed` says, as Splunk keeps it: the text, the JSON of an object or array, or the
/// raw line, filed under the index, host, source, sourcetype and time it was sent with.
fn is(found: &Found, landed: &Landed) -> bool {
    let (raw_matches, want): (bool, BTreeMap<String, String>) = match landed {
        Landed::Event(envelope) => {
            let raw = match &envelope["event"] {
                Value::String(s) => found.raw == *s,
                event => serde_json::from_str::<Value>(&found.raw).is_ok_and(|v| canonical(&v) == canonical(event)),
            };
            let mut want: BTreeMap<String, String> = ["host", "source", "sourcetype", "index"]
                .into_iter()
                .filter_map(|k| Some((k.to_string(), envelope.get(k)?.as_str()?.to_string())))
                .collect();
            if let Some(time) = envelope.get("time") {
                want.insert("_time".into(), six_decimals(time));
            }
            (raw, want)
        }
        Landed::Raw { query, line } => (found.raw.as_bytes() == line.as_slice(), query.clone()),
    };
    let mut got = found.fields.clone();
    got.insert("_time".into(), found.time.clone());
    raw_matches && want.iter().all(|(k, v)| got.get(k) == Some(v))
}

/// Checks that Splunk holds each row of `sent` it did not refuse: once, or once more at most for each refused row after
/// it in its batch. A batch with a refused row fits one request (`batches_for`), and Splunk queues the events before
/// one it refuses, which go again with the rest of the request.
fn check_found(sent: &[Vec<Sent>], found: &[Found]) -> Result<(), String> {
    let mut claimed = vec![false; found.len()];
    for rows in sent {
        for (at, s) in rows.iter().enumerate() {
            let Some(landed) = &s.landed else { continue };
            let most = 1 + rows[at + 1..].iter().filter(|r| r.landed.is_none()).count();
            let mut count = 0;
            for (i, f) in found.iter().enumerate() {
                if !claimed[i] && is(f, landed) {
                    claimed[i] = true;
                    count += 1;
                }
            }
            if count == 0 || count > most {
                let near: Vec<&Found> = found.iter().take(3).collect();
                return Err(format!(
                    "{landed:?} found {count} times, at most {most}; searched {near:?}"
                ));
            }
        }
    }
    if let Some(i) = claimed.iter().position(|c| !c) {
        return Err(format!(
            "{} events found that were not sent, as {:?}",
            found.len(),
            found[i]
        ));
    }
    Ok(())
}

/// Sets `d` to the `kind`th way a token can disagree with it: indexer, receipt on the event endpoint, receipt on the
/// raw one.
fn mismatched(d: &mut Map<String, Value>, kind: u64) {
    let raw = kind == 2;
    let acknowledgement = if kind == 0 { "indexer" } else { "receipt" };
    d.insert("acknowledgement".into(), json!(acknowledgement));
    d.insert("endpoint".into(), json!(if raw { "raw" } else { "event" }));
    let bytes = d.get("format").is_some_and(|f| f == "bytes" || f == "sourceBytes");
    if !raw && bytes {
        d.remove("format");
        d.remove("field");
    }
}

fn live_seed(seed: u64, s: &live::Splunk, reached: &mut Reached) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let marker = marker(seed);
    let mut draw = draw(&mut rng, reached, Target::Live(s));
    // Every fourth seed's token disagrees with it, each kind in turn: a mismatch is too rare drawn among a few seeds.
    let mismatch = seed % 4 == 3;
    if mismatch {
        mismatched(&mut draw.destination, seed / 4 % 3);
    }
    let json = bundle(&draw).to_string();
    let c = check_save(&draw, &json)
        .map_err(|e| format!("{e}\n{json}"))?
        .expect("a live draw is never broken");
    let token_acks = (c.acknowledgement == Acknowledgement::Indexer) != mismatch;
    let token = if token_acks { &s.ack_token } else { &s.token };
    let (secrets, _) = held(&draw, token);
    let mut d = build(&c, &secrets).map_err(|e| format!("does not build: {e}\n{json}"))?;
    let sent = batches_for(&mut rng, reached, &c, Some(&marker));
    let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
    let written = destination::write(d.as_mut(), &batches, Duration::from_secs(180));
    drop(d);
    if mismatch {
        // `mismatched` set the endpoint and acknowledgement, and may have dropped the format and field: the draw's
        // are not what was sent.
        let overwritten = |n: &str| {
            ["endpoint.", "acknowledgement.", "format."]
                .iter()
                .any(|p| n.starts_with(p))
                || n == "field"
        };
        reached.forget_where(|n| n.starts_with("row.") || n == "batches.several" || overwritten(n));
        let says = mismatch_says(token_acks);
        return match written {
            Err(e) if e.contains(says) => {
                // Nothing went out, so nothing turns up in the time a sent event takes to be searchable.
                let found = search(s, &marker, |f| !f.is_empty(), Duration::from_secs(10))?;
                if !found.is_empty() {
                    return Err(format!("stopped, yet landed {found:?}\n{json}"));
                }
                reached.reach(mismatch_option(&c));
                Ok(())
            }
            // Every row was refused before a request went, so the token was never asked.
            Ok(refused) if none_sent(&sent) => check_refused(&sent, &refused).map_err(|e| format!("{e}\n{json}")),
            Err(e) => Err(format!(
                "on a token with acknowledgement {token_acks}, failed with {e}\n{json}"
            )),
            Ok(_) => Err(format!(
                "delivered on a token with acknowledgement {token_acks}\n{json}"
            )),
        };
    }
    let refused = written.map_err(|e| format!("{e}\n{json}"))?;
    check_refused(&sent, &refused).map_err(|e| format!("{e}\n{json}"))?;
    // Each sent event, not a count: a retry may land one twice before another is searchable.
    let landed: Vec<&Landed> = sent.iter().flatten().filter_map(|s| s.landed.as_ref()).collect();
    let found = search(
        s,
        &marker,
        |f| landed.iter().all(|l| f.iter().any(|f| is(f, l))),
        Duration::from_secs(60),
    )?;
    check_found(&sent, &found).map_err(|e| format!("{e}\n{json}"))
}

/// The same draws against a real Splunk, each searched back by a marker every row carries.
#[test]
fn live_splunk_fuzz() {
    let Some(s) = live::splunk("live_splunk_fuzz") else {
        return;
    };
    destination::run(0..LIVE_SEEDS, live_options(), |seed, reached| {
        live_seed(seed, &s, reached)
    });
}
