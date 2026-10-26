//! Adversarial settings, rows and answers against the Prometheus destination (`io::remote_write` over `io::http`).
//! Its settings are whatever a bundle says, its rows whatever a pipeline makes and its answers whatever a store says,
//! so none of them may panic it, keep it past its stall threshold, or make it send a body a remote_write receiver
//! cannot read. smeltr's own receiver (`io::prometheus`) is that receiver: every body sent here is read back by it.
//!
//! Seeded over a range by `harness::fuzz`, so a failure names its seed, and each fuzzer asserts it reached every
//! setting, shape and answer it knows.
//!
//! The last fuzzer runs the destination against a real Prometheus, where what it sends is read back by a query.

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value, json};
use smeltr::io::http::{HttpDestination, Response, Transport, Unwritten, agent_config, credential_headers};
use smeltr::io::prometheus::{Samples, Version, Written};
use smeltr::io::remote_write::{RemoteWrite, transport};
use smeltr::io::{Destination, Dispatched, IoError, OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{DestinationKind, HeldSecrets, PrometheusDestinationConfig, RemoteWriteVersion};

use crate::certs;
use crate::harness::fuzz::{self, Coverage, Rng};

/// One of `items`, owned.
fn any(rng: &mut Rng, items: Vec<Value>) -> Value {
    let at = rng.below(items.len());
    items.into_iter().nth(at).expect("at is within items")
}

/// What the receiver reads of one request at most. Far past any body here, so a refusal is about what the body
/// holds, never its size.
const READ_LIMIT: usize = 1 << 40;

/// Reads `body` as the Prometheus receiver does, every record of it taken.
fn read_back(body: &[u8], version: RemoteWriteVersion) -> Result<Written, String> {
    let version = match version {
        RemoteWriteVersion::V1 => Version::V1,
        RemoteWriteVersion::V2 => Version::V2,
    };
    let mut samples = Samples::snappy(body, version, READ_LIMIT)?;
    let written = samples.check()?;
    while samples.next_record().is_some() {}
    Ok(written)
}

fn row(value: Vec<u8>) -> OwnedRecord {
    OwnedRecord {
        value,
        payload: Payload::Record,
        ..OwnedRecord::default()
    }
}

type Sent = Arc<Mutex<Vec<Vec<u8>>>>;

/// The kind of each answer a store gave, as the fuzzer's coverage names it.
type Answered = Arc<Mutex<Vec<&'static str>>>;

/// A store answering each request at random, recording its body and the kind of its answer.
struct FuzzStore {
    seed: u64,
    /// How often each body, by its hash, was sent.
    attempts: HashMap<u64, u64>,
    version: RemoteWriteVersion,
    sent: Sent,
    answered: Answered,
}

/// What a 2.0 store says it wrote: none, nothing, less or more than was sent, or no number.
const WRITTEN_COUNTS: &[&str] = &["0", "1", "3", "17", "18446744073709551615", "-1", "", "many", " 2 "];

const WRITTEN_HEADERS: [&str; 3] = [
    "x-prometheus-remote-write-samples-written",
    "x-prometheus-remote-write-histograms-written",
    "x-prometheus-remote-write-exemplars-written",
];

impl Transport for FuzzStore {
    fn post(&mut self, _path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        self.sent.lock().unwrap().push(body.to_vec());
        // The answer follows from the body and how often it was sent, not from how many requests came before it:
        // how many a batch makes before its stall deadline is timing.
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        let key = hasher.finish();
        let attempt = self.attempts.entry(key).or_default();
        *attempt += 1;
        let rng = &mut Rng::seeded(self.seed ^ key ^ *attempt);
        let mut headers = Vec::new();
        let mut answered = self.answered.lock().unwrap();
        let status = match rng.below(100) {
            0..=57 => {
                // A 2.0 store names what it wrote, and one of only 1.0 names nothing.
                if self.version == RemoteWriteVersion::V2 && !rng.chance(12) {
                    for header in WRITTEN_HEADERS {
                        if !rng.chance(3) {
                            headers.push((header.to_string(), rng.pick(WRITTEN_COUNTS).to_string()));
                        }
                    }
                }
                answered.push(match (self.version, headers.is_empty()) {
                    (RemoteWriteVersion::V2, true) => "answer 2xx naming nothing",
                    (RemoteWriteVersion::V2, false) => "answer 2xx naming what it wrote",
                    _ => "answer 2xx",
                });
                *rng.pick(&[200, 201, 204, 204, 299])
            }
            58..=83 => {
                // A 2.0 store that wrote part of the request says so, and one that refused it whole says 0 or nothing.
                if self.version == RemoteWriteVersion::V2 && rng.chance(2) {
                    for header in WRITTEN_HEADERS {
                        headers.push((header.to_string(), rng.pick(WRITTEN_COUNTS).to_string()));
                    }
                    answered.push("answer 2.0 refusal naming what it wrote");
                } else {
                    answered.push("answer split");
                }
                *rng.pick(&[400, 409, 413, 422])
            }
            // Each costs a backoff of up to the stall threshold, so they are kept rare.
            84..=87 => {
                let retry_after = *rng.pick(&[None, Some("0"), Some("1"), Some("soon"), Some("-5")]);
                answered.push("answer retry");
                if let Some(wait) = retry_after {
                    answered.push("answer Retry-After");
                    headers.push(("retry-after".to_string(), wait.to_string()));
                }
                *rng.pick(&[429, 500, 502, 503, 599])
            }
            _ => {
                answered.push("answer other");
                if rng.chance(3) {
                    rng.next_u64() as u16
                } else {
                    *rng.pick(&[415, 401, 403, 301, 302, 307, 308, 404, 100, 0, 600, 999])
                }
            }
        };
        Ok(Response {
            status,
            headers,
            body: format!("status {status}").into_bytes(),
        })
    }
}

// -- settings

/// URLs a bundle may hold: whole ones of either scheme, one carrying a password over plain HTTP, trailing slashes,
/// and ones that are no URL.
const URLS: &[&str] = &[
    "https://prometheus.example/api/v1/write",
    "http://prometheus.example/api/v1/write",
    "http://user:pass@prometheus.example/api/v1/write",
    "https://user:pass@prometheus.example/api/v1/push",
    "https://prometheus.example/api/v1/write///",
    "https://prometheus.example",
    "http://127.0.0.1:9090/api/v1/write",
    "http://[::1]:9090/api/v1/write",
    "https://prometheus.example:99999/api/v1/write",
    "https://ünïcødé.example/write",
    "  https://prometheus.example/api/v1/write  ",
    "",
    "   ",
    "prometheus.example",
    "ftp://prometheus.example/",
    "https://",
    "https://bad host/write",
    "http://\u{0}/",
    "//prometheus.example/write",
];

/// Header names: ordinary ones, every one smeltr sets itself in either case, ones HTTP does not allow, and one
/// twice in different cases.
const HEADER_NAMES: &[&str] = &[
    "X-Scope-OrgID",
    "x-scope-orgid",
    "Authorization",
    "Content-Type",
    "content-encoding",
    "Content-Length",
    "Transfer-Encoding",
    "Host",
    "Cookie",
    "X-Prometheus-Remote-Write-Version",
    "bad name",
    "",
    "X-Ünï",
    "X:Colon",
    "X-Tenant",
];

fn header_value(rng: &mut Rng) -> Value {
    match rng.below(10) {
        0 => json!(""),
        1 => json!("one\ntwo"),
        2 => json!("carriage\r"),
        3 => json!("\u{7f}del"),
        4 => json!("\ttab"),
        5 => json!("x".repeat(9000)),
        6 => json!(5),
        _ => json!("tenant-1"),
    }
}

fn credential(rng: &mut Rng) -> Value {
    match rng.below(8) {
        0 => json!("PROMETHEUS_TOKEN"),
        1 => json!({"secret": "", "version": 1}),
        2 => json!({"secret": "token", "version": -1}),
        3 => json!({"secret": "newline", "version": 1}),
        4 => json!({"secret": "missing", "version": 9}),
        5 => json!(7),
        _ => json!({"secret": "token", "version": 1}),
    }
}

/// A size as a bundle states one: in and around the bounds, in words and in numbers, and ones that are no size.
fn size(rng: &mut Rng) -> Value {
    match rng.below(16) {
        0 => json!(0),
        1 => json!(1),
        2 => json!(4095),
        3 => json!(4096),
        4 => json!("4 KiB"),
        5 => json!("1 MiB"),
        6 => json!("64 MiB"),
        7 => json!("100 MiB"),
        8 => json!(100 * 1024 * 1024 + 1),
        9 => json!("1 GiB"),
        10 => json!("2 GiB"),
        11 => json!(-1),
        12 => json!(1.5),
        13 => json!("lots"),
        14 => json!(u64::MAX),
        _ => json!(1024 * 1024),
    }
}

/// A system with one Prometheus destination of random settings, and the settings it sets. Half the systems take every setting from values the check passes, so valid ones are reached as often as
/// refused ones.
fn random_system(rng: &mut Rng, authority: &str) -> (String, Vec<String>) {
    let wild = rng.chance(2);
    // Whether this setting may be anything at all.
    let loose = |rng: &mut Rng| wild && rng.chance(2);
    let mut d = Map::new();
    d.insert("id".into(), json!("store"));
    d.insert("type".into(), json!("prometheus"));
    if loose(rng) {
        if !rng.chance(10) {
            d.insert("url".into(), json!(rng.pick(URLS)));
        }
    } else {
        d.insert("url".into(), json!(rng.pick(&URLS[..2])));
    }
    if rng.chance(2) {
        let version = if loose(rng) {
            any(rng, vec![json!("3.0"), json!(""), json!(2), json!(null)])
        } else {
            any(rng, vec![json!("1.0"), json!("2.0")])
        };
        d.insert("version".into(), version);
    }
    let maps = |rng: &mut Rng, names: &[&str], value: fn(&mut Rng) -> Value| {
        let mut map = Map::new();
        for _ in 0..rng.below(4) {
            map.insert(rng.pick(names).to_string(), value(rng));
        }
        Value::Object(map)
    };
    if rng.chance(2) {
        let headers = if loose(rng) {
            maps(rng, HEADER_NAMES, header_value)
        } else {
            maps(rng, &["X-Scope-OrgID", "X-Tenant"], |_| json!("tenant-1"))
        };
        d.insert("headers".into(), headers);
    }
    if rng.chance(3) {
        let credentials = if loose(rng) {
            maps(rng, HEADER_NAMES, credential)
        } else {
            maps(rng, &["Authorization"], |rng| {
                any(
                    rng,
                    vec![
                        json!({"secret": "token", "version": 1}),
                        json!({"secret": "newline", "version": 1}),
                        json!({"secret": "missing", "version": 9}),
                    ],
                )
            })
        };
        d.insert("credentials".into(), credentials);
    }
    // An authority, what is no certificate, or none; a certificate the worker holds, or one it does not; and
    // whether public roots are trusted, which beside neither is refused.
    if rng.chance(3) {
        let authorities = if loose(rng) {
            any(
                rng,
                vec![json!("not a certificate"), json!(""), json!(3), json!(authority)],
            )
        } else {
            json!(authority)
        };
        d.insert("trustedAuthorities".into(), authorities);
    }
    if rng.chance(4) {
        let certificate = if loose(rng) {
            any(rng, vec![json!(""), json!(" "), json!(3), json!("missing")])
        } else {
            json!("client")
        };
        d.insert("certificate".into(), certificate);
    }
    if rng.chance(4) {
        let trusts = if loose(rng) {
            any(rng, vec![json!("yes"), json!(1), json!(true)])
        } else {
            json!(rng.chance(2))
        };
        d.insert("trustPublic".into(), trusts);
    }
    for field in ["maxRequestBytes", "bufferBytes"] {
        if rng.chance(2) {
            let size = if loose(rng) {
                size(rng)
            } else {
                any(rng, vec![json!(4096), json!("256 KiB"), json!("4 MiB"), json!(4 << 20)])
            };
            d.insert(field.into(), size);
        }
    }
    if rng.chance(2) {
        let stall = if loose(rng) {
            any(rng, vec![json!(0), json!(121), json!(u64::MAX), json!(-1), json!("30")])
        } else {
            any(rng, vec![json!(1), json!(30), json!(120)])
        };
        d.insert("stallAfterSecs".into(), stall);
    }
    let mut set: Vec<String> = (d.keys())
        .filter(|k| !["id", "type"].contains(&k.as_str()))
        .cloned()
        .collect();
    if d.get("version") == Some(&json!("2.0")) {
        set.push("version 2.0".into());
    }
    let system = json!({
        "version": 1,
        "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
        "destinations": [d],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["store"]}]}],
    });
    (system.to_string(), set)
}

/// How far a bundle's settings got.
#[derive(Debug, PartialEq)]
enum Reached {
    /// No system configuration at all.
    Unparsed,
    Refused,
    /// Valid, and the destination they make delivered a sample.
    Delivered,
    /// Valid, and this worker holds no credential or certificate they name, which the destination says rather than
    /// sending without.
    Unresolved,
}

/// Checks `json` as a save does and, where it passes, loads it, builds its destination and its transport, and
/// delivers one sample through the destination to a store that takes it.
fn settle(json: &str, secrets: &HeldSecrets) -> Result<Reached, String> {
    let Ok(file) = serde_json::from_str::<smeltr::config_file::FileConfig>(json) else {
        return Ok(Reached::Unparsed);
    };
    let errors = smeltr::config_file::validation_errors(file);
    let loaded = smeltr::config_file::load_str(json);
    if !errors.is_empty() {
        return Ok(Reached::Refused);
    }
    let system = loaded.map_err(|e| format!("passes the check and does not load: {e}"))?;
    let kind = &system.topology.destinations[0].kind;
    let DestinationKind::Prometheus(c) = kind else {
        return Err(format!("loads as {kind:?}"));
    };
    let (Ok(credentials), Ok(agent)) = (
        credential_headers("prometheus destination", &c.credentials, secrets),
        agent_config(c.tls.as_ref(), secrets),
    ) else {
        return Ok(Reached::Unresolved);
    };
    // Built as a worker builds it, though never asked to connect: the bundle's host is no store.
    let _ = transport(c, &credentials, &agent);
    let sent = Sent::default();
    let store = Lands {
        sent: Arc::clone(&sent),
    };
    let mut d = HttpDestination::new(store, RemoteWrite::new(c))
        .with_max_request_bytes(c.max_request_bytes)
        .with_stall_after(c.stall_after);
    let sample = br#"{"time":"2026-10-11T18:00:00Z","name":"up","value":1,"attributes":{"job":"api"}}"#;
    let refused = deliver(&mut d, vec![row(sample.to_vec())]).map_err(|e| format!("delivering a sample: {e}"))?;
    if !refused.is_empty() {
        return Err(format!("a sample was refused: {}", refused[0].reason));
    }
    for body in sent.lock().unwrap().iter() {
        read_back(body, c.version).map_err(|e| format!("the receiver cannot read what was sent: {e}"))?;
    }
    Ok(Reached::Delivered)
}

/// A store that lands every request, saying what it wrote as a 2.0 store does.
struct Lands {
    sent: Sent,
}

impl Transport for Lands {
    fn post(&mut self, _path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        self.sent.lock().unwrap().push(body.to_vec());
        Ok(Response {
            status: 204,
            headers: vec![(WRITTEN_HEADERS[0].to_string(), "1".to_string())],
            body: Vec::new(),
        })
    }
}

fn deliver<T: Transport>(
    d: &mut HttpDestination<RemoteWrite, T>,
    rows: Vec<OwnedRecord>,
) -> Result<Vec<Rejection>, IoError> {
    let (dispatched, refused) = d.deliver(0, Cow::Owned(rows))?;
    assert_eq!(
        dispatched,
        Dispatched::Complete,
        "a destination of no senders answers before it returns"
    );
    Ok(refused)
}

/// Every setting of a Prometheus destination, as a bundle names it.
const SETTINGS: [&str; 10] = [
    "url",
    "version",
    "headers",
    "credentials",
    "certificate",
    "trustedAuthorities",
    "trustPublic",
    "maxRequestBytes",
    "bufferBytes",
    "stallAfterSecs",
];

/// Every combination of settings either fails the check, or passes it and loads into a destination that delivers;
/// none panics the check, the load or the build. Each setting is reached in a destination that delivers.
#[test]
fn random_settings_are_refused_or_build_a_destination_that_delivers() {
    let authority = certs::Authority::new("remote-write-fuzz");
    let client = authority.issue("client", Vec::new());
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("token".to_string(), 1, "Bearer t-123".to_string()),
        ("newline".to_string(), 1, "Bearer t-123\n".to_string()),
    ]);
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: "client".to_string(),
        version: 1,
        value: client.key,
        chain: Some(client.certificate),
    }]);
    let delivering = |setting: &str| format!("{setting} in a destination that delivers");
    let mut coverage = Coverage::new(
        (SETTINGS.iter().map(|s| s.to_string()))
            .chain(SETTINGS.iter().map(|s| delivering(s)))
            .chain(["version 2.0 in a destination that delivers".to_string()])
            .chain(["Unparsed", "Refused", "Delivered", "Unresolved"].map(String::from)),
    );
    fuzz::run(1..10_000, |seed| {
        let mut rng = Rng::seeded(seed);
        let (json, set) = random_system(&mut rng, &authority.pem);
        let reached = match catch_unwind(AssertUnwindSafe(|| settle(&json, &secrets))) {
            Ok(Ok(reached)) => reached,
            Ok(Err(e)) => return Err(format!("{e}\n{json}")),
            Err(_) => return Err(format!("panicked on {json}")),
        };
        coverage.reach(format!("{reached:?}"));
        for setting in set {
            if reached == Reached::Delivered {
                coverage.reach(delivering(&setting));
            }
            coverage.reach(setting);
        }
        Ok(())
    });
    coverage.assert_complete();
}

// -- rows

const NAMES: &[&str] = &[
    "up",
    "up",
    "requests_total",
    "latency",
    "latency_bucket",
    "latency_count",
    "latency_sum",
    "x",
    "ünïcødé",
    "a b",
    "",
];

const LABEL_NAMES: &[&str] = &[
    "job",
    "job",
    "instance",
    "le",
    "__name__",
    "",
    "trace_id",
    "span_id",
    "a b",
    "ünï",
    "0digit",
    "__reserved",
];

const TYPES: &[&str] = &[
    "gauge",
    "sum",
    "histogram",
    "exponentialHistogram",
    "summary",
    "info",
    "stateset",
    "table",
    "",
];

/// A datetime, mostly one a row carries, else one at either end of what reads, past it, or no datetime.
fn time(rng: &mut Rng) -> Value {
    match rng.below(24) {
        0 => Value::Null,
        1 => json!(1_791_741_600),
        2 => json!("not a time"),
        3 => json!("0000-01-01T00:00:00Z"),
        4 => json!("9999-12-31T23:59:59.999999Z"),
        5 => json!("10000-01-01T00:00:00Z"),
        6 => json!("1969-12-31T23:59:59.9995Z"),
        7 => json!("2026-10-11T18:00:00+14:00"),
        8 => json!(""),
        _ => json!(format!(
            "2026-10-11T18:{:02}:{:02}.{:06}Z",
            rng.below(3),
            rng.below(60),
            rng.below(1_000_000)
        )),
    }
}

/// A number field's value. JSON holds no NaN nor infinity, so one that was arrives as null.
fn number(rng: &mut Rng) -> Value {
    match rng.below(18) {
        0 => Value::Null,
        1 => json!(-0.0),
        2 => json!(1e300),
        3 => json!(-1e300),
        4 => json!(5e-324),
        5 => json!(i64::MAX),
        6 => json!(i64::MIN),
        7 => json!(u64::MAX),
        8 => json!("1"),
        9 => json!(true),
        10 => json!([1]),
        11 => json!(-2.5),
        _ => json!(rng.below(1000) as f64 / 4.0),
    }
}

/// A bucket's count: mostly small and whole, else fractional, past what a double holds exactly, or no count.
fn bucket_count(rng: &mut Rng) -> Value {
    match rng.below(30) {
        0 => json!(0.5),
        1 => json!(-1),
        // A count at the edge of what a double holds exactly, which a sum of counts passes.
        2..=4 => json!(9_007_199_254_740_992u64),
        5 => json!(9_007_199_254_740_993u64),
        6 => json!(1e300),
        7 => Value::Null,
        8 => json!("3"),
        9 => json!(0.1),
        10..=15 => json!(0),
        _ => json!(rng.below(6)),
    }
}

fn label_value(rng: &mut Rng) -> Value {
    match rng.below(14) {
        0 => json!(""),
        1 => Value::Null,
        2 => json!(rng.below(100)),
        3 => json!(-1.5),
        4 => json!(true),
        5 => json!([1, 2]),
        6 => json!({"a": 1}),
        7 => json!("ünïcødé \"quoted\" \\"),
        8 => json!("\u{0}"),
        9 => json!("v".repeat(300)),
        _ => json!(rng.pick(&["api", "db", "web"])),
    }
}

fn attributes(rng: &mut Rng) -> Option<Value> {
    match rng.below(20) {
        0..=3 => None,
        4 => Some(Value::Null),
        5 => Some(json!("job=api")),
        _ => {
            let mut map = Map::new();
            for _ in 0..rng.below(4) {
                map.insert(rng.pick(LABEL_NAMES).to_string(), label_value(rng));
            }
            Some(Value::Object(map))
        }
    }
}

fn exemplar(rng: &mut Rng) -> Value {
    if rng.chance(10) {
        return any(rng, vec![json!(1), json!("e"), Value::Null, json!([])]);
    }
    let mut e = Map::new();
    if !rng.chance(3) {
        e.insert("time".into(), time(rng));
    }
    if !rng.chance(8) {
        e.insert("value".into(), number(rng));
    }
    let id = |rng: &mut Rng| match rng.below(6) {
        0 => json!(""),
        1 => json!(7),
        2 => json!("f".repeat(200)),
        3 => json!("ünï"),
        _ => json!("5b8efff798038103d269b633813fc60c"),
    };
    if rng.chance(2) {
        e.insert("traceId".into(), id(rng));
    }
    if rng.chance(2) {
        e.insert("spanId".into(), id(rng));
    }
    if rng.chance(2) {
        let filtered = match attributes(rng) {
            Some(Value::Object(mut map)) => {
                // Long enough together to pass what Prometheus stores of an exemplar's labels.
                if rng.chance(4) {
                    map.insert("user".repeat(20), json!("u".repeat(rng.below(200))));
                }
                Value::Object(map)
            }
            other => other.unwrap_or(Value::Null),
        };
        e.insert("filteredAttributes".into(), filtered);
    }
    Value::Object(e)
}

fn exemplars(rng: &mut Rng) -> Value {
    match rng.below(8) {
        0 => json!({"value": 1}),
        1 => json!([]),
        _ => Value::Array((0..1 + rng.below(4)).map(|_| exemplar(rng)).collect()),
    }
}

fn temporality(rng: &mut Rng) -> Option<Value> {
    match rng.below(10) {
        0 => None,
        1 => Some(json!(1)),
        2 => Some(Value::Null),
        3 => Some(json!("2")),
        4 => Some(json!(3)),
        _ => Some(json!(2)),
    }
}

fn text(rng: &mut Rng) -> Value {
    match rng.below(8) {
        0 => json!(""),
        1 => json!(5),
        2 => Value::Null,
        3 => json!("h".repeat(5000)),
        4 => json!("ünïcødé\nline"),
        _ => json!(rng.pick(&["Requests served.", "seconds", "bytes"])),
    }
}

/// One side of an exponential histogram, and its bucket counts.
fn side(rng: &mut Rng) -> (Value, Vec<Value>) {
    if rng.chance(12) {
        return (any(rng, vec![json!([1, 2]), json!("side"), Value::Null]), Vec::new());
    }
    let offset = match rng.below(12) {
        0 => json!(i32::MIN),
        1 => json!(i32::MAX),
        2 => json!(i64::from(i32::MAX) + 1),
        3 => json!(1.5),
        4 => Value::Null,
        5 => json!(-(rng.below(1000) as i64)),
        _ => json!(rng.below(20) as i64 - 10),
    };
    let counts: Vec<Value> = (0..rng.below(9)).map(|_| bucket_count(rng)).collect();
    let mut side = Map::new();
    if !rng.chance(15) {
        side.insert("offset".into(), offset);
    }
    side.insert("bucketCounts".into(), Value::Array(counts.clone()));
    (Value::Object(side), counts)
}

/// The count of a histogram whose buckets, its zero bucket's included, hold `counts`, as the store adds them up:
/// exactly where each is whole and exact in a double, as the destination then sends them as integers, else as
/// doubles. None where one is no count.
fn total(counts: &[Value]) -> Option<Value> {
    let counts: Vec<f64> = (counts.iter())
        .map(|c| c.as_f64().filter(|c| *c >= 0.0))
        .collect::<Option<_>>()?;
    if counts.iter().all(|c| c.fract() == 0.0 && *c <= 9_007_199_254_740_992.0) {
        return Some(json!(counts.iter().map(|c| *c as u64).sum::<u64>()));
    }
    Some(json!(counts.iter().sum::<f64>()))
}

/// A row of every shape the destination takes and many it does not. Where `miscount`, an exponential histogram may
/// count other than what its buckets hold.
fn random_row(rng: &mut Rng, miscount: bool, coverage: &mut Coverage) -> Vec<u8> {
    if rng.chance(30) {
        coverage.reach("row garbage");
        let garbage: &[&[u8]] = &[b"", b"garbage", b"[]", b"null", b"\"up\"", b"{\"name\":", b"\xff\xfe"];
        return rng.pick(garbage).to_vec();
    }
    let mut r = Map::new();
    match rng.below(20) {
        0 => {}
        1 => {
            r.insert("name".into(), any(rng, vec![json!(5), Value::Null, json!(["up"])]));
        }
        _ => {
            r.insert("name".into(), json!(rng.pick(NAMES)));
        }
    }
    let shape = rng.below(14);
    // A family's metadata: no time, no value, no exemplars.
    if shape == 0 {
        if !rng.chance(6) {
            r.insert("type".into(), json!(rng.pick(TYPES)));
        }
        for field in ["description", "unit"] {
            if rng.chance(2) {
                r.insert(field.into(), text(rng));
            }
        }
        if rng.chance(2) {
            r.insert(
                "isMonotonic".into(),
                any(rng, vec![json!(true), json!(false), json!("true")]),
            );
        }
        if let Some(t) = temporality(rng) {
            r.insert("aggregationTemporality".into(), t);
        }
        if rng.chance(4) {
            r.insert("exemplars".into(), json!([]));
        }
        coverage.reach("row metadata");
        return Value::Object(r).to_string().into_bytes();
    }
    if !rng.chance(25) {
        r.insert("time".into(), time(rng));
    }
    match shape {
        // Exponential histograms, their count mostly what their buckets hold.
        1..=4 => {
            r.insert("type".into(), json!("exponentialHistogram"));
            let scale = match rng.below(16) {
                0 => json!(-100),
                1 => json!(-5),
                2 => json!(-4),
                3 => json!(9),
                4 => json!(20),
                5 => json!(63),
                6 => json!(64),
                7 => json!(1000),
                8 => json!(i64::MAX),
                9 => json!(i64::MIN),
                10 => json!(1.5),
                11 => json!("0"),
                _ => json!(rng.below(13) as i64 - 4),
            };
            if !rng.chance(20) {
                r.insert("scale".into(), scale);
            }
            let zero = match rng.below(6) {
                0 => None,
                1 => Some(bucket_count(rng)),
                _ => Some(json!(rng.below(3))),
            };
            // A null zero count reads as none, and so as 0.
            let mut counts: Vec<Value> = zero.iter().filter(|z| !z.is_null()).cloned().collect();
            if let Some(zero) = zero {
                r.insert("zeroCount".into(), zero);
            }
            if rng.chance(3) {
                let threshold = any(
                    rng,
                    vec![json!(0.001), json!(0), json!(-1), json!(1e300), json!("t"), Value::Null],
                );
                r.insert("zeroThreshold".into(), threshold);
            }
            for field in ["positive", "negative"] {
                if !rng.chance(3) {
                    let (side, counted) = side(rng);
                    r.insert(field.into(), side);
                    counts.extend(counted);
                }
            }
            let count = match total(&counts) {
                Some(total) if !(miscount && rng.chance(2)) => total,
                Some(_) => {
                    coverage.reach("row miscounted histogram");
                    bucket_count(rng)
                }
                None => bucket_count(rng),
            };
            if !rng.chance(20) {
                r.insert("count".into(), count);
            }
            if !rng.chance(15) {
                r.insert("sum".into(), number(rng));
            }
            if let Some(t) = temporality(rng) {
                r.insert("aggregationTemporality".into(), t);
            }
            if rng.chance(3) {
                let hint = any(
                    rng,
                    vec![json!("yes"), json!("no"), json!("maybe"), json!(1), Value::Null],
                );
                r.insert("resetHint".into(), hint);
            }
        }
        // Histograms of explicit buckets: bounds mostly increasing, counts mostly one more.
        5..=7 => {
            r.insert("type".into(), json!("histogram"));
            let n = rng.below(6);
            let mut bounds: Vec<Value> = Vec::new();
            let mut at = -2.0;
            for _ in 0..n {
                at += rng.below(4) as f64 * 0.5;
                bounds.push(match rng.below(20) {
                    0 => Value::Null,
                    1 => json!("1"),
                    2 => json!(1e300),
                    3 => json!(-1e-300),
                    _ => json!(at),
                });
            }
            if rng.chance(8) && bounds.len() > 1 {
                coverage.reach("row histogram unsorted bounds");
                bounds.swap(0, 1);
            }
            let counts = match rng.below(8) {
                0 => n.saturating_sub(1),
                1 => n + 2,
                _ => n + 1,
            };
            if counts != n + 1 {
                coverage.reach("row histogram mismatched counts");
            }
            r.insert(
                "explicitBounds".into(),
                if rng.chance(20) {
                    json!("bounds")
                } else {
                    Value::Array(bounds)
                },
            );
            if !rng.chance(20) {
                let counts: Vec<Value> = (0..counts).map(|_| bucket_count(rng)).collect();
                r.insert("bucketCounts".into(), Value::Array(counts));
            }
            r.insert("count".into(), bucket_count(rng));
            r.insert("sum".into(), number(rng));
            if let Some(t) = temporality(rng) {
                r.insert("aggregationTemporality".into(), t);
            }
        }
        8 => {
            r.insert("type".into(), json!("summary"));
            r.insert("count".into(), bucket_count(rng));
            r.insert("sum".into(), number(rng));
            r.insert("quantileValues".into(), json!([{"quantile": 0.5, "value": 1}]));
        }
        // Exemplars and no sample.
        9 => {
            if rng.chance(2) {
                r.insert("value".into(), Value::Null);
            }
            r.insert("exemplars".into(), exemplars(rng));
        }
        10 => {
            let kind = any(
                rng,
                vec![json!("info"), json!("stateset"), json!("table"), json!(3), json!("")],
            );
            r.insert("type".into(), kind);
            r.insert("value".into(), number(rng));
        }
        // Samples.
        _ => {
            match rng.below(4) {
                0 => {}
                1 => {
                    r.insert("type".into(), json!("gauge"));
                }
                _ => {
                    r.insert("type".into(), json!("sum"));
                    r.insert("isMonotonic".into(), json!(rng.chance(2)));
                }
            }
            if !rng.chance(15) {
                r.insert("value".into(), number(rng));
            }
            if let Some(t) = temporality(rng).filter(|_| rng.chance(3)) {
                r.insert("aggregationTemporality".into(), t);
            }
        }
    }
    if let Some(a) = attributes(rng) {
        r.insert("attributes".into(), a);
    }
    if shape != 9 && rng.chance(3) {
        r.insert("exemplars".into(), exemplars(rng));
    }
    if rng.chance(4) {
        r.insert("startTime".into(), time(rng));
    }
    for field in ["description", "unit"] {
        if rng.chance(6) {
            r.insert(field.into(), text(rng));
        }
    }
    reach_row(&r, coverage);
    Value::Object(r).to_string().into_bytes()
}

/// Names what a point's row holds, for the fuzzer's coverage.
fn reach_row(r: &Map<String, Value>, coverage: &mut Coverage) {
    if !matches!(r.get("name"), Some(Value::String(_))) {
        coverage.reach("row of no name");
    }
    coverage.reach(match r.get("type").and_then(Value::as_str) {
        Some("exponentialHistogram") => "row exponentialHistogram",
        Some("histogram") => "row histogram",
        Some("summary") => "row summary",
        None if r.get("value").is_none_or(Value::is_null) && r.contains_key("exemplars") => "row exemplars alone",
        None | Some("gauge") | Some("sum") => "row sample",
        Some(_) => "row of another type",
    });
    match r.get("scale").and_then(Value::as_i64) {
        Some(scale) if scale < -4 => coverage.reach("row scale below -4"),
        Some(scale) if scale > 8 => coverage.reach("row scale above 8"),
        _ => {}
    }
    for (field, name) in [
        ("resetHint", "row resetHint"),
        ("exemplars", "row exemplars"),
        ("startTime", "row startTime"),
        ("description", "row description or unit"),
        ("unit", "row description or unit"),
        ("attributes", "row attributes"),
    ] {
        if r.contains_key(field) {
            coverage.reach(name);
        }
    }
    if r.get("aggregationTemporality") == Some(&json!(1)) {
        coverage.reach("row delta");
    }
}

/// The settings, row shapes, answers and outcomes the second fuzzer must reach. The row shapes are named where
/// `random_row` makes them, and the answers where `FuzzStore` gives them.
const ROW_OPTIONS: &[&str] = &[
    "version 1.0",
    "version 2.0",
    "maxRequestBytes 1",
    "maxRequestBytes 64",
    "maxRequestBytes 512",
    "maxRequestBytes 4096",
    "maxRequestBytes 1048576",
    "maxRequestBytes unbounded",
    "stall 0ms",
    "stall 2ms",
    "stall 20ms",
    "stall 50ms",
    "stall 300ms",
    "row garbage",
    "row of no name",
    "row metadata",
    "row exponentialHistogram",
    "row scale below -4",
    "row scale above 8",
    "row miscounted histogram",
    "row histogram",
    "row histogram unsorted bounds",
    "row histogram mismatched counts",
    "row summary",
    "row exemplars alone",
    "row of another type",
    "row sample",
    "row delta",
    "row resetHint",
    "row exemplars",
    "row startTime",
    "row description or unit",
    "row attributes",
    "answer 2xx",
    "answer 2xx naming what it wrote",
    "answer 2xx naming nothing",
    "answer split",
    "answer 2.0 refusal naming what it wrote",
    "answer retry",
    "answer Retry-After",
    "answer other",
    "a row refused",
    "a batch failed by the store",
    "a batch stalled",
    "read 1.0",
    "read 2.0",
    "histograms read 1.0",
    "histograms read 2.0",
    "exemplars read 1.0",
    "exemplars read 2.0",
    "miscounted body refused by the receiver",
];

/// One seed: a destination of random version and request size, over a store answering at random, delivered
/// several random batches; then every body it sent, read back by the receiver.
fn fuzz_one(seed: u64, coverage: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let version = *rng.pick(&[RemoteWriteVersion::V1, RemoteWriteVersion::V2]);
    let named = |v: RemoteWriteVersion| if v == RemoteWriteVersion::V2 { "2.0" } else { "1.0" };
    coverage.reach(format!("version {}", named(version)));
    let max_request_bytes = *rng.pick(&[1, 64, 512, 4096, 1 << 20, usize::MAX]);
    coverage.reach(match max_request_bytes {
        usize::MAX => "maxRequestBytes unbounded".to_string(),
        n => format!("maxRequestBytes {n}"),
    });
    // A throttled answer naps up to the stall threshold, so most are short.
    let stall_after = *rng.pick(&[0, 2, 20, 20, 50, 50, 50, 300]);
    coverage.reach(format!("stall {stall_after}ms"));
    let sent = Sent::default();
    let answered = Answered::default();
    let store = FuzzStore {
        seed: !seed,
        attempts: HashMap::new(),
        version,
        sent: Arc::clone(&sent),
        answered: Arc::clone(&answered),
    };
    let config = PrometheusDestinationConfig {
        url: rng
            .pick(&[
                "http://store/api/v1/write",
                "http://store/api/v1/write//",
                "http://store",
            ])
            .to_string(),
        version,
        headers: Vec::new(),
        credentials: Vec::new(),
        tls: None,
        max_request_bytes,
        buffer_bytes: max_request_bytes,
        stall_after: Duration::from_millis(stall_after),
    };
    let unwritten = Unwritten::new(
        smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "remote-write-fuzz"),
        0,
    );
    let mut d = HttpDestination::new(store, RemoteWrite::new(&config).with_unwritten(unwritten))
        .with_max_request_bytes(max_request_bytes)
        .with_stall_after(config.stall_after);
    // Miscounted histograms come only in seeds of their own, which the receiver may refuse, so every other seed
    // holds the round trip to every body.
    let miscount = rng.chance(8);
    // Several batches through one destination, as 2.0 remembers each family's metadata across them.
    for _ in 0..1 + rng.below(3) {
        let rows: Vec<OwnedRecord> = (0..rng.below(40))
            .map(|_| row(random_row(&mut rng, miscount, coverage)))
            .collect();
        let given = rows.clone();
        let shown = || {
            (given.iter())
                .map(|r| String::from_utf8_lossy(&r.value).into_owned())
                .collect::<Vec<_>>()
        };
        match catch_unwind(AssertUnwindSafe(|| deliver(&mut d, rows))) {
            Err(_) => return Err(format!("panicked delivering {:#?}", shown())),
            Ok(Ok(refused)) => {
                let mut docs: Vec<usize> = refused.iter().map(|r| r.doc).collect();
                docs.sort_unstable();
                docs.dedup();
                if docs.len() != refused.len() {
                    return Err(format!("refused a row twice: {refused:?}"));
                }
                for r in &refused {
                    if r.doc >= given.len() {
                        return Err(format!("refused row {} of {}", r.doc, given.len()));
                    }
                    if r.record
                        .as_ref()
                        .is_some_and(|record| record.value != given[r.doc].value)
                    {
                        return Err(format!("refused row {} with another's record", r.doc));
                    }
                    coverage.reach("a row refused");
                }
            }
            // The store's own answer, or the stall threshold passed: an error the destination names.
            Ok(Err(e)) if e.message.starts_with("the store ") => coverage.reach("a batch failed by the store"),
            Ok(Err(e)) if e.message.contains("still undelivered after") => coverage.reach("a batch stalled"),
            Ok(Err(e)) => return Err(format!("failed a batch for another reason: {e:?}")),
        }
    }
    for answer in answered.lock().unwrap().iter() {
        coverage.reach(*answer);
    }
    let v = named(version);
    for body in sent.lock().unwrap().iter() {
        match catch_unwind(|| read_back(body, version)) {
            Err(_) => {
                return Err(format!(
                    "the receiver panicked reading what the destination sent: {body:02x?}"
                ));
            }
            Ok(Ok(written)) => {
                coverage.reach(format!("read {v}"));
                if written.histograms > 0 {
                    coverage.reach(format!("histograms read {v}"));
                }
                if written.exemplars > 0 {
                    coverage.reach(format!("exemplars read {v}"));
                }
            }
            Ok(Err(e)) if miscount && e.contains("observations") => {
                coverage.reach("miscounted body refused by the receiver")
            }
            Ok(Err(e)) => return Err(format!("the receiver cannot read what the destination sent ({v}): {e}")),
        }
    }
    Ok(())
}

/// Random batches against random answers: the destination never panics, fails a batch only for the store's answer
/// or its stall threshold, refuses only rows it was given, each once, and sends only what the receiver reads.
#[test]
fn random_rows_and_answers_send_only_what_the_receiver_reads() {
    let mut coverage = Coverage::new(ROW_OPTIONS.iter().copied());
    fuzz::run(1..1_200, |seed| fuzz_one(seed, &mut coverage));
    coverage.assert_complete();
}

// -- live: a real Prometheus

/// A container and the directory of its configuration, both removed when the test ends, passing or not.
struct Container {
    name: String,
    dir: std::path::PathBuf,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Runs Prometheus taking remote_write of both versions, native histograms and exemplars, holding out-of-order
/// samples for half an hour where `out_of_order`. Its base URL.
fn run_prometheus(image: &str, out_of_order: bool) -> (Container, String) {
    let dir = std::env::temp_dir().join(format!(
        "smeltr-remote-write-fuzz-{}-{out_of_order}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let window = if out_of_order {
        "storage:\n  tsdb:\n    out_of_order_time_window: 30m\n"
    } else {
        ""
    };
    std::fs::write(dir.join("prometheus.yml"), format!("global: {{}}\n{window}")).unwrap();
    let web = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let name = format!("smeltr-remote-write-fuzz-{}-{out_of_order}", std::process::id());
    let started = std::process::Command::new("docker")
        .args(["run", "-d", "--rm", "--network", "host", "--name", &name])
        .arg("-v")
        .arg(format!("{}:/etc/smeltr:ro", dir.display()))
        .arg(image)
        .arg("--config.file=/etc/smeltr/prometheus.yml")
        .arg(format!("--web.listen-address={web}"))
        .arg("--web.enable-remote-write-receiver")
        .arg("--web.remote-write-receiver.accepted-protobuf-messages=prometheus.WriteRequest")
        .arg("--web.remote-write-receiver.accepted-protobuf-messages=io.prometheus.write.v2.Request")
        .arg("--enable-feature=native-histograms,exemplar-storage")
        .arg("--storage.tsdb.path=/tmp/prometheus")
        .output()
        .expect("docker runs");
    let container = Container { name, dir };
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let base = format!("http://{web}");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while ureq::get(&format!("{base}/-/ready")).call().is_err() {
        assert!(std::time::Instant::now() < deadline, "Prometheus never became ready");
        std::thread::sleep(Duration::from_millis(200));
    }
    (container, base)
}

fn query(base: &str, path: &str, params: &[(&str, &str)]) -> Value {
    let mut request = ureq::get(&format!("{base}{path}"));
    for (k, v) in params {
        request = request.query(*k, *v);
    }
    request.call().unwrap().body_mut().read_json().unwrap()
}

/// An ISO 8601 datetime of `ms`, as a row carries one.
fn iso(ms: i64) -> String {
    let mut out = Vec::new();
    smeltr::ops::datetime::write_iso8601_micros_bare(ms * 1000, &mut out);
    String::from_utf8(out).unwrap()
}

/// What one series should hold once every batch is sent: each point's time in milliseconds and its value, a float's
/// or a histogram's count and sum.
#[derive(Default)]
struct Expected {
    floats: Vec<(i64, f64)>,
    histograms: Vec<(i64, f64, f64)>,
    /// Each exemplar's time and value.
    exemplars: Vec<(i64, f64)>,
    /// The newest time Prometheus holds of it, past which a point is not out of order.
    newest: i64,
}

/// How many samples the destination of `registry` counted unwritten.
fn unwritten_samples(registry: &smeltr::metrics::Metrics) -> u64 {
    (registry.snapshot().into_iter())
        .filter(|s| {
            s.key.name == "smeltr_destination_unwritten_total" && s.key.labels.iter().any(|(_, v)| v == "sample")
        })
        .map(|s| match s.value {
            smeltr::metrics::MetricValue::Counter(n) => n,
            smeltr::metrics::MetricValue::FloatCounter(_) | smeltr::metrics::MetricValue::Gauge(_) => 0,
        })
        .sum()
}

/// A point Prometheus is to hold.
enum Landed {
    /// With an exemplar of the same value at its time, where `exemplar`.
    Float {
        value: f64,
        exemplar: bool,
    },
    Histogram {
        count: f64,
        sum: f64,
    },
}

/// One run against `base`: a destination of random settings, several batches of rows whose fate is known, and a
/// check that Prometheus holds what it took, and that what it did not take was refused or counted unwritten.
fn live_run(seed: u64, base: &str, out_of_order: bool, coverage: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let version = *rng.pick(&[RemoteWriteVersion::V1, RemoteWriteVersion::V2]);
    coverage.reach(if version == RemoteWriteVersion::V2 {
        "version 2.0"
    } else {
        "version 1.0"
    });
    let max_request_bytes = *rng.pick(&[4096, 16 << 10, 1 << 20]);
    coverage.reach(format!("maxRequestBytes {max_request_bytes}"));
    let mut headers = Vec::new();
    if rng.chance(2) {
        coverage.reach("headers");
        headers.push(("X-Scope-OrgID".to_string(), format!("tenant-{}", rng.below(3))));
    }
    let c = PrometheusDestinationConfig {
        url: format!("{base}/api/v1/write"),
        version,
        headers,
        credentials: Vec::new(),
        tls: None,
        max_request_bytes,
        buffer_bytes: *rng.pick(&[4 << 20, 64 << 20]).max(&max_request_bytes),
        stall_after: Duration::from_secs(*rng.pick(&[5, 10, 30])),
    };
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let unwritten = Unwritten::new(smeltr::metrics::MetricsHandle::new(Arc::clone(&registry), "live"), 0);
    let mut d = HttpDestination::new(
        transport(&c, &[], &agent),
        RemoteWrite::new(&c).with_unwritten(unwritten),
    )
    .with_max_request_bytes(c.max_request_bytes)
    .with_stall_after(c.stall_after);
    let run = format!(
        "fuzz_{seed}_{}",
        if version == RemoteWriteVersion::V2 { "v2" } else { "v1" }
    );
    let floats: Vec<String> = (0..4).map(|i| format!("{run}_gauge_{i}")).collect();
    let natives: Vec<String> = (0..2).map(|i| format!("{run}_native_{i}")).collect();
    let classics: Vec<String> = (0..2).map(|i| format!("{run}_classic_{i}")).collect();
    let mut expected: std::collections::BTreeMap<String, Expected> = Default::default();
    // Every point lies in the ten minutes before now, each series' a second or more apart, so Prometheus holds all
    // of them and none is another's duplicate.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut clock = now - 600_000;
    // Every point's series and time, so no two points of a series share a millisecond.
    let mut taken: std::collections::HashSet<(String, i64)> = Default::default();
    let mut unique = |name: &str, mut t: i64| {
        while !taken.insert((name.to_string(), t)) {
            t += 1;
        }
        t
    };
    let mut unwritten_before = 0;
    for batch in 0..2 + rng.below(3) {
        let mut rows: Vec<Vec<u8>> = Vec::new();
        // Rows the destination refuses itself.
        let mut refused_ok: Vec<usize> = Vec::new();
        // Rows Prometheus refuses as out of order: refused where their request landed nothing, else counted unwritten.
        let mut late_rows: Vec<usize> = Vec::new();
        // Points of this batch, applied to `expected` once it is delivered: where each went, and what it was.
        let mut landing: Vec<(String, i64, Landed)> = Vec::new();
        for _ in 0..rng.below(120) {
            clock += 1000;
            let t = clock - rng.below(5) as i64 * 1000 + rng.below(1000) as i64;
            match rng.below(10) {
                0..=4 => {
                    let name = rng.pick(&floats).clone();
                    let newest = expected.get(&name).map_or(i64::MIN, |e| e.newest);
                    // Older than what Prometheus already holds of the series: out of order.
                    let (t, late) = if batch > 0 && newest > now - 590_000 && rng.chance(8) {
                        (unique(&name, newest - 1000 - rng.below(5000) as i64), true)
                    } else {
                        (unique(&name, t.max(newest.saturating_add(1))), false)
                    };
                    // Prometheus does not take it.
                    let refused = late && !out_of_order;
                    let value = rng.below(100_000) as f64 / 8.0 - 1000.0;
                    let exemplar = !late && rng.chance(3);
                    let mut row = json!({"time": iso(t), "name": name, "value": value,
                        "attributes": {"job": "fuzz", "seed": seed}});
                    if exemplar {
                        row["exemplars"] = json!([{"value": value, "traceId": format!("{seed:x}{t:x}")}]);
                    }
                    coverage.reach(match (late, refused) {
                        (false, _) => "float",
                        (true, true) => "out of order",
                        (true, false) => "out of order, taken",
                    });
                    if exemplar {
                        coverage.reach("exemplar");
                    }
                    if refused {
                        late_rows.push(rows.len());
                    } else {
                        landing.push((name, t, Landed::Float { value, exemplar }));
                    }
                    rows.push(row.to_string().into_bytes());
                }
                5 | 6 => {
                    let name = rng.pick(&natives).clone();
                    let newest = expected.get(&name).map_or(i64::MIN, |e| e.newest);
                    let t = unique(&name, t.max(newest.saturating_add(1)));
                    let scale = rng.below(16) as i64 - 4;
                    let positive: Vec<u64> = (0..rng.below(6)).map(|_| rng.below(5) as u64).collect();
                    let negative: Vec<u64> = (0..rng.below(3)).map(|_| rng.below(5) as u64).collect();
                    let zero = rng.below(3) as u64;
                    let count = zero + positive.iter().sum::<u64>() + negative.iter().sum::<u64>();
                    let sum = rng.below(1000) as f64 / 4.0;
                    let row = json!({"time": iso(t), "name": name, "type": "exponentialHistogram",
                        "aggregationTemporality": 2, "count": count, "sum": sum, "scale": scale, "zeroCount": zero,
                        "zeroThreshold": 0.001,
                        "positive": {"offset": rng.below(20) as i64 - 10, "bucketCounts": positive},
                        "negative": {"offset": rng.below(20) as i64 - 10, "bucketCounts": negative},
                        "attributes": {"job": "fuzz"}});
                    rows.push(row.to_string().into_bytes());
                    coverage.reach(if scale > 8 {
                        "native histogram, downscaled"
                    } else {
                        "native histogram"
                    });
                    landing.push((
                        name,
                        t,
                        Landed::Histogram {
                            count: count as f64,
                            sum,
                        },
                    ));
                }
                7 => {
                    let name = rng.pick(&classics).clone();
                    let newest = expected.get(&format!("{name}_count")).map_or(i64::MIN, |e| e.newest);
                    let t = unique(&name, t.max(newest.saturating_add(1)));
                    let counts: Vec<u64> = (0..4).map(|_| rng.below(5) as u64).collect();
                    let count: u64 = counts.iter().sum();
                    let row = json!({"time": iso(t), "name": name, "type": "histogram",
                        "aggregationTemporality": 2, "count": count, "sum": count as f64 * 1.5,
                        "explicitBounds": [0.5, 1, 5], "bucketCounts": counts});
                    rows.push(row.to_string().into_bytes());
                    coverage.reach("classic histogram");
                    let value = count as f64;
                    landing.push((format!("{name}_count"), t, Landed::Float { value, exemplar: false }));
                }
                // A family's metadata, which 2.0 carries on its series.
                8 => {
                    let name = rng.pick(&floats).clone();
                    let row =
                        json!({"name": name, "type": "gauge", "description": "A fuzzed gauge.", "unit": "seconds"});
                    rows.push(row.to_string().into_bytes());
                    coverage.reach("metadata");
                }
                // Rows the destination refuses itself.
                _ => {
                    coverage.reach("refused by the destination");
                    refused_ok.push(rows.len());
                    let row = rng
                        .pick(&[
                            json!({"time": iso(t), "name": "fuzz_summary", "type": "summary", "count": 1, "sum": 1}),
                            json!({"time": iso(t), "name": "fuzz_novalue"}),
                            json!({"time": iso(t), "name": "fuzz_delta", "type": "exponentialHistogram",
                                "aggregationTemporality": 1, "count": 0, "sum": 0, "scale": 0}),
                            json!({"time": iso(t), "name": "", "value": 1}),
                        ])
                        .clone();
                    rows.push(row.to_string().into_bytes());
                }
            }
        }
        let given: Vec<OwnedRecord> = rows.into_iter().map(row).collect();
        let shown = || {
            given
                .iter()
                .map(|r| String::from_utf8_lossy(&r.value).into_owned())
                .collect::<Vec<_>>()
        };
        let refused = match catch_unwind(AssertUnwindSafe(|| deliver(&mut d, given.clone()))) {
            Err(_) => return Err(format!("panicked delivering {:#?}", shown())),
            Ok(Err(e)) => return Err(format!("batch {batch} failed: {e:?}\n{:#?}", shown())),
            Ok(Ok(refused)) => refused,
        };
        let mut docs: Vec<usize> = refused.iter().map(|r| r.doc).collect();
        docs.sort_unstable();
        let (late, own): (Vec<usize>, Vec<usize>) = docs.iter().partition(|doc| late_rows.contains(doc));
        if own != refused_ok {
            return Err(format!(
                "batch {batch} refused {refused:#?}, and rows {refused_ok:?} should be, with any of the late {late_rows:?}\n{:#?}",
                shown()
            ));
        }
        // 1.0 lands a request whole or not at all, so the halves find each late row. A 2.0 request that landed in
        // part is not sent again, so its late rows are counted unwritten instead.
        let counted = unwritten_samples(&registry) - unwritten_before;
        unwritten_before += counted;
        let expected_counted = (late_rows.len() - late.len()) as u64;
        if counted != expected_counted || (version == RemoteWriteVersion::V1 && counted > 0) {
            return Err(format!(
                "batch {batch} ({version:?}) refused late rows {late:?} of {late_rows:?} and counted {counted} samples \
                 unwritten, where {expected_counted} should be"
            ));
        }
        if !late.is_empty() {
            coverage.reach("out of order, refused");
        }
        if counted > 0 {
            coverage.reach("out of order, counted unwritten");
        }
        for (name, t, landed) in landing {
            let e = expected.entry(name).or_default();
            e.newest = e.newest.max(t);
            match landed {
                Landed::Float { value, exemplar } => {
                    e.floats.push((t, value));
                    if exemplar {
                        e.exemplars.push((t, value));
                    }
                }
                Landed::Histogram { count, sum } => e.histograms.push((t, count, sum)),
            }
        }
    }
    let at = format!("{:.3}", (now + 120_000) as f64 / 1000.0);
    for (name, e) in &expected {
        let answer = query(
            base,
            "/api/v1/query",
            &[("query", &format!("{name}[20m]")), ("time", &at)],
        );
        let result = &answer["data"]["result"];
        // Several series where the attributes differ, as the classic `_count` series has no `job`.
        let mut floats: Vec<(i64, f64)> = Vec::new();
        let mut histograms: Vec<(i64, f64, f64)> = Vec::new();
        for series in result.as_array().into_iter().flatten() {
            for v in series["values"].as_array().into_iter().flatten() {
                let ms = (v[0].as_f64().unwrap() * 1000.0).round() as i64;
                floats.push((ms, v[1].as_str().unwrap().parse().unwrap()));
            }
            for h in series["histograms"].as_array().into_iter().flatten() {
                let ms = (h[0].as_f64().unwrap() * 1000.0).round() as i64;
                let number = |k: &str| h[1][k].as_str().unwrap().parse::<f64>().unwrap();
                histograms.push((ms, number("count"), number("sum")));
            }
        }
        let mut want = e.floats.clone();
        want.sort_by_key(|p| p.0);
        floats.sort_by_key(|p| p.0);
        if floats != want {
            return Err(format!("{name} holds floats {floats:?}, and was sent {want:?}"));
        }
        let mut want = e.histograms.clone();
        want.sort_by_key(|p| p.0);
        histograms.sort_by_key(|p| p.0);
        if histograms != want {
            return Err(format!("{name} holds histograms {histograms:?}, and was sent {want:?}"));
        }
        if !e.exemplars.is_empty() {
            let start = format!("{:.3}", (now - 700_000) as f64 / 1000.0);
            let answer = query(
                base,
                "/api/v1/query_exemplars",
                &[("query", name), ("start", &start), ("end", &at)],
            );
            let mut held: Vec<(i64, f64)> = (answer["data"].as_array().into_iter().flatten())
                .flat_map(|s| s["exemplars"].as_array().cloned().unwrap_or_default())
                .map(|x| {
                    let ms = (x["timestamp"].as_f64().unwrap() * 1000.0).round() as i64;
                    (ms, x["value"].as_str().unwrap().parse().unwrap())
                })
                .collect();
            held.sort_by_key(|p| p.0);
            let mut want = e.exemplars.clone();
            want.sort_by_key(|p| p.0);
            if held != want {
                return Err(format!("{name} holds exemplars {held:?}, and was sent {want:?}"));
            }
        }
    }
    Ok(())
}

/// What the live fuzzer must reach.
const LIVE_OPTIONS: &[&str] = &[
    "version 1.0",
    "version 2.0",
    "maxRequestBytes 4096",
    "maxRequestBytes 16384",
    "maxRequestBytes 1048576",
    "headers",
    "float",
    "out of order",
    "out of order, refused",
    "out of order, counted unwritten",
    "out of order, taken",
    "exemplar",
    "native histogram",
    "native histogram, downscaled",
    "classic histogram",
    "metadata",
    "refused by the destination",
];

/// Random settings and rows of every shape through the real destination to a real Prometheus, with and without an
/// out-of-order window: no batch stalls, the destination refuses the rows it cannot send, each row Prometheus does
/// not take is refused or counted unwritten, and Prometheus holds every float, histogram and exemplar it took, as
/// sent.
#[test]
fn prometheus_holds_what_the_fuzzed_destination_sends() {
    let Some(image) = crate::live::prometheus("prometheus_holds_what_the_fuzzed_destination_sends") else {
        return;
    };
    let mut coverage = Coverage::new(LIVE_OPTIONS.iter().copied());
    // Seeds 1 to 4 against one without an out-of-order window, 5 to 8 against one with it.
    let stores = [false, true].map(|out_of_order| (run_prometheus(&image, out_of_order), out_of_order));
    fuzz::run(1..9, |seed| {
        let ((_, base), out_of_order) = &stores[usize::from(seed > 4)];
        live_run(seed, base, *out_of_order, &mut coverage)
    });
    coverage.assert_complete();
}
