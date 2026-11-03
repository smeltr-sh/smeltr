//! The OTLP destination's fuzzers (`type: "otlp"`, `io::otlp::export` over `io::http`). Its settings are whatever a
//! bundle says, its rows whatever a pipeline makes and its answers whatever a service says, so none of them may panic
//! it, keep it past its stall threshold, or make it send a body an OTLP receiver cannot read.
//!
//! smeltr's own OTLP receiver (`io::otlp`) stands at both ends. Random exports go into one; its records, carried
//! through [`CARRY`], go through the destination; what lands is posted to a second, whose records, carried the same
//! way, must be the rows the destination was given less those it refused.
//!
//! Seeded over a range by `harness::fuzz`, so a failure names its seed, and each fuzzer asserts it reached every
//! setting, signal and answer it knows. The last fuzzer puts a real OpenTelemetry Collector between the destination
//! and the second receiver.

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{Ipv4Addr, SocketAddr};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};
use smeltr::io::http::{HttpDestination, Response, Transport, Unwritten, agent_config, credential_headers};
use smeltr::io::otlp::Encoding;
use smeltr::io::otlp::export::Otlp;
use smeltr::io::{Destination, Dispatched, IoError, OwnedRecord, Rejection, Source, SourceRecord, http_post};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{
    Compression, DestinationKind, HeldSecrets, HttpDestinationConfig, OtlpDestinationConfig, Signal,
};

use crate::certs;
use crate::harness::docker::{Container, Scratch, container_logs, share};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::http::gunzip;
use crate::harness::receiver::commit;
use crate::otlp_destination_tests::{carried, free_port, post, receiver};
use crate::otlp_receiver_fuzz::{Gen, pb_export, random_export};

const SIGNALS: [Signal; 3] = [Signal::Logs, Signal::Metrics, Signal::Traces];

/// One of `items`, owned.
fn any(rng: &mut Rng, items: Vec<Value>) -> Value {
    let at = rng.below(items.len());
    items.into_iter().nth(at).expect("at is within items")
}

fn row(value: Vec<u8>) -> OwnedRecord {
    OwnedRecord {
        value,
        payload: Payload::Record,
        ..OwnedRecord::default()
    }
}

/// Every record of `body`, an export of `signal` as the receiver reads protobuf, or why it reads none.
fn read_back(signal: Signal, body: &[u8]) -> Result<usize, String> {
    let mut export = smeltr::io::otlp::Export::protobuf(signal, body)?;
    Ok(std::iter::from_fn(|| export.next_record()).count())
}

fn deliver<T: Transport>(d: &mut HttpDestination<Otlp, T>, rows: Vec<OwnedRecord>) -> Result<Vec<Rejection>, IoError> {
    let (dispatched, refused) = d.deliver(0, Cow::Owned(rows))?;
    assert_eq!(
        dispatched,
        Dispatched::Complete,
        "a destination of no senders answers before it returns"
    );
    Ok(refused)
}

// -- settings

/// URLs a bundle may hold: whole ones of either scheme, one carrying a password over plain HTTP, ones with a path or
/// trailing slashes, and ones that are no URL.
const URLS: &[&str] = &[
    "https://collector.example:4318",
    "http://collector.example:4318",
    "http://user:pass@collector.example:4318",
    "https://user:pass@collector.example",
    "https://collector.example:4318///",
    "https://collector.example/otlp",
    "http://127.0.0.1:4318",
    "http://[::1]:4318",
    "https://collector.example:99999",
    "https://ünïcødé.example",
    "  https://collector.example:4318  ",
    "",
    "   ",
    "collector.example",
    "ftp://collector.example/",
    "https://",
    "https://bad host",
    "http://\u{0}/",
    "//collector.example",
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
        0 => json!("OTLP_TOKEN"),
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

/// A system with one OTLP destination of random settings, and the settings it sets. Half the systems take every
/// setting from values the check passes, so valid ones are reached as often as refused ones.
fn random_system(rng: &mut Rng, authority: &str) -> (String, Vec<String>) {
    let wild = rng.chance(2);
    // Whether this setting may be anything at all.
    let loose = |rng: &mut Rng| wild && rng.chance(2);
    let mut d = Map::new();
    d.insert("id".into(), json!("collector"));
    d.insert("type".into(), json!("otlp"));
    if loose(rng) {
        if !rng.chance(10) {
            d.insert("url".into(), json!(rng.pick(URLS)));
        }
    } else {
        d.insert("url".into(), json!(rng.pick(&URLS[..2])));
    }
    if rng.chance(2) {
        let signal = if loose(rng) {
            any(
                rng,
                vec![json!("profiles"), json!(""), json!(2), json!(null), json!("Logs")],
            )
        } else {
            any(rng, vec![json!("logs"), json!("metrics"), json!("traces")])
        };
        d.insert("signal".into(), signal);
    }
    if rng.chance(2) {
        let compression = if loose(rng) {
            any(
                rng,
                vec![json!("zstd"), json!("snappy"), json!("lz4"), json!(""), json!(1)],
            )
        } else {
            any(rng, vec![json!("none"), json!("gzip")])
        };
        d.insert("compression".into(), compression);
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
    if rng.chance(2) {
        let in_flight = if loose(rng) {
            any(rng, vec![json!(0), json!(-1), json!(1000), json!("2"), json!(1.5)])
        } else {
            any(rng, vec![json!(1), json!(2), json!(8)])
        };
        d.insert("inFlight".into(), in_flight);
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
    for signal in ["metrics", "traces"] {
        if d.get("signal") == Some(&json!(signal)) {
            set.push(format!("signal {signal}"));
        }
    }
    if d.get("compression") == Some(&json!("gzip")) {
        set.push("compression gzip".into());
    }
    let system = json!({
        "version": 1,
        "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
        "destinations": [d],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["collector"]}]}],
    });
    (system.to_string(), set)
}

/// How far a bundle's settings got.
#[derive(Debug, PartialEq)]
enum Reached {
    /// No system configuration at all.
    Unparsed,
    Refused,
    /// Valid, and the destination they make delivered a row.
    Delivered,
    /// Valid, and this worker holds no credential or certificate they name, which the destination says rather than
    /// sending without.
    Unresolved,
}

/// A row of each signal that the destination sends.
fn sample(signal: Signal) -> &'static [u8] {
    match signal {
        Signal::Logs => br#"{"time":"2026-10-11T18:00:00Z","body":"paid","resource":{"service.name":"api"}}"#,
        Signal::Metrics => br#"{"time":"2026-10-11T18:00:00Z","name":"up","type":"gauge","value":1}"#,
        Signal::Traces => br#"{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"pay"}"#,
    }
}

/// Checks `json` as a save does and, where it passes, loads it, builds its destination and its transport, and
/// delivers one row through the destination to a service that takes it.
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
    let DestinationKind::Otlp(c) = kind else {
        return Err(format!("loads as {kind:?}"));
    };
    let (Ok(credentials), Ok(agent)) = (
        credential_headers("otlp destination", &c.http.credentials, secrets),
        agent_config(c.http.tls.as_ref(), secrets),
    ) else {
        return Ok(Reached::Unresolved);
    };
    // Built as a worker builds it, though never asked to connect: the bundle's host is no service.
    let _ = http_post::transport_as(&c.http, Encoding::Protobuf.content_type(), &credentials, &agent);
    let landed = Landed::default();
    let service = Takes {
        signal: c.signal,
        gzip: c.http.compression == Compression::Gzip,
        landed: Arc::clone(&landed),
    };
    let mut d = HttpDestination::new(service, Otlp::new(c))
        .with_max_request_bytes(c.http.max_request_bytes)
        .with_stall_after(c.http.stall_after);
    let refused =
        deliver(&mut d, vec![row(sample(c.signal).to_vec())]).map_err(|e| format!("delivering a row: {e}"))?;
    if !refused.is_empty() {
        return Err(format!("a row was refused: {}", refused[0].reason));
    }
    let landed = landed.lock().unwrap();
    if landed.len() != 1 {
        return Err(format!("{} requests landed of one row", landed.len()));
    }
    for body in landed.iter() {
        let read = read_back(c.signal, body).map_err(|e| format!("the receiver cannot read what was sent: {e}"))?;
        if read != 1 {
            return Err(format!("the receiver read {read} records of one row"));
        }
    }
    Ok(Reached::Delivered)
}

/// The bodies a service took, as they were before their compression.
type Landed = Arc<Mutex<Vec<Vec<u8>>>>;

/// Opens a body as the service reads it, checking it went to its signal's path.
fn opened(path: &str, signal: Signal, gzip: bool, body: &[u8]) -> Vec<u8> {
    assert_eq!(
        path,
        smeltr::io::otlp::http_path(signal),
        "a request goes to its signal's path"
    );
    match gzip {
        true => gunzip(body).expect("a gzip destination sends gzip"),
        false => body.to_vec(),
    }
}

/// A service that takes every request.
struct Takes {
    signal: Signal,
    gzip: bool,
    landed: Landed,
}

impl Transport for Takes {
    fn post(&mut self, path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        let body = opened(path, self.signal, self.gzip, body);
        self.landed.lock().unwrap().push(body);
        Ok(Response {
            status: 200,
            headers: Vec::new(),
            body: Vec::new(),
        })
    }
}

/// Every setting of an OTLP destination, as a bundle names it.
const SETTINGS: [&str; 12] = [
    "url",
    "signal",
    "compression",
    "headers",
    "credentials",
    "certificate",
    "trustedAuthorities",
    "trustPublic",
    "inFlight",
    "maxRequestBytes",
    "bufferBytes",
    "stallAfterSecs",
];

/// Every combination of settings either fails the check, or passes it and loads into a destination that delivers;
/// none panics the check, the load or the build. Each setting is reached in a destination that delivers.
#[test]
fn random_settings_are_refused_or_build_a_destination_that_delivers() {
    let authority = certs::Authority::new("otlp-destination-fuzz");
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
            .chain(["signal metrics", "signal traces", "compression gzip"].map(delivering))
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
            if !setting.contains(' ') {
                coverage.reach(setting);
            }
        }
        Ok(())
    });
    coverage.assert_complete();
}

// -- rows and answers

/// The kind of each answer a service gave, as the fuzzer's coverage names it.
type Answered = Arc<Mutex<Vec<&'static str>>>;

/// A service answering each request at random, keeping each body it took.
struct FuzzService {
    seed: u64,
    signal: Signal,
    gzip: bool,
    /// How often each body, by its hash, was sent.
    attempts: HashMap<u64, u64>,
    landed: Landed,
    answered: Answered,
}

/// An `Export*ServiceResponse` whose partial success names `rejected` items refused.
fn partial_success(rejected: i64, why: &str) -> Vec<u8> {
    let mut inner = vec![0x08];
    let mut n = rejected as u64;
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            inner.push(byte);
            break;
        }
        inner.push(byte | 0x80);
    }
    inner.push(0x12);
    inner.push(why.len() as u8);
    inner.extend_from_slice(why.as_bytes());
    let mut body = vec![0x0a, inner.len() as u8];
    body.extend(inner);
    body
}

impl Transport for FuzzService {
    fn post(&mut self, path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        // The answer follows from the body and how often it was sent, not from how many requests came before it:
        // how many a batch makes before its stall deadline is timing.
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        let key = hasher.finish();
        let attempt = self.attempts.entry(key).or_default();
        *attempt += 1;
        let rng = &mut Rng::seeded(self.seed ^ key ^ *attempt);
        let mut answered = self.answered.lock().unwrap();
        let mut answer = Vec::new();
        let status = match rng.below(100) {
            0..=59 => {
                self.landed
                    .lock()
                    .unwrap()
                    .push(opened(path, self.signal, self.gzip, body));
                match rng.below(4) {
                    0 => {
                        answered.push("answer 2xx with a partial success");
                        answer = partial_success(*rng.pick(&[1, 3, i64::MAX, -1, 0]), "bad attributes");
                    }
                    1 => {
                        answered.push("answer 2xx with what is no response");
                        answer = rng.pick(&[&b"\xff\xff"[..], b"{}", b"\x0a\x05\x08"]).to_vec();
                    }
                    _ => answered.push("answer 2xx"),
                }
                *rng.pick(&[200, 200, 202, 204, 299])
            }
            60..=83 => {
                answered.push("answer split");
                *rng.pick(&[400, 413, 422])
            }
            // Each costs a backoff of up to the stall threshold, so they are kept rare.
            84..=87 => {
                answered.push("answer retry");
                *rng.pick(&[408, 429, 500, 502, 503, 504])
            }
            _ => {
                answered.push("answer other");
                *rng.pick(&[401, 403, 404, 409, 415, 301, 100, 0, 600, 999])
            }
        };
        Ok(Response {
            status,
            headers: Vec::new(),
            body: answer,
        })
    }
}

/// POSTs `body`, a protobuf export, to the receiver of `source` at `at`, and answers the status and every record it
/// gave for it. The receiver answers once those are committed, so each was taken by the time the post returns.
fn exchange(source: &mut Box<dyn Source>, at: SocketAddr, signal: Signal, body: Vec<u8>) -> (u16, Vec<SourceRecord>) {
    let sent = std::thread::spawn(move || post(at, signal, "application/x-protobuf", &body));
    let mut records = Vec::new();
    loop {
        let answered = sent.is_finished();
        let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(5)).unwrap();
        commit(source, &batch);
        let none = batch.is_empty();
        records.extend(batch);
        if answered && none {
            return (sent.join().unwrap(), records);
        }
    }
}

/// Junk for a field of a row: every JSON type, and ones each field reads wrongly.
fn junk(rng: &mut Rng) -> Value {
    let deep = (0..40).fold(json!([]), |inner, _| json!([inner]));
    any(
        rng,
        vec![
            json!(null),
            json!(1),
            json!(-1),
            json!(1.5),
            json!(1e308),
            json!(u64::MAX),
            json!(i64::MIN),
            json!("x"),
            json!(""),
            json!("zz"),
            json!("0102"),
            json!("2026-13-45T99:00:00Z"),
            json!("1969-12-31T23:59:59Z"),
            json!("9999-12-31T23:59:59Z"),
            json!("gauge"),
            json!("stateset"),
            json!(true),
            json!([]),
            json!({}),
            json!([1, "a", null]),
            json!([{}, {"a": 1}]),
            json!({"a": {"b": [{}]}}),
            json!({"name": 7, "attributes": [1]}),
            deep,
        ],
    )
}

/// Rows a pipeline may make that no receiver would: not JSON, no object, or an object nested past what OTLP holds.
const WHOLE_JUNK: &[&str] = &[
    "",
    "[]",
    "null",
    "{",
    "7",
    "\"text\"",
    r#"{"a":1}{"#,
    r#"{"time":1e400}"#,
];

/// Fields each signal's rows hold, and the two every row may.
fn fields_of(signal: Signal) -> &'static [&'static str] {
    match signal {
        Signal::Logs => &[
            "time",
            "severityNumber",
            "body",
            "attributes",
            "traceId",
            "spanId",
            "flags",
        ],
        Signal::Traces => &[
            "traceId",
            "spanId",
            "parentSpanId",
            "kind",
            "startTime",
            "events",
            "links",
            "statusCode",
        ],
        Signal::Metrics => &[
            "name",
            "type",
            "value",
            "count",
            "bucketCounts",
            "explicitBounds",
            "positive",
            "quantileValues",
            "exemplars",
            "aggregationTemporality",
        ],
    }
}

/// `value`, a row, made wrong.
fn mutated(rng: &mut Rng, signal: Signal, value: &[u8]) -> Vec<u8> {
    if rng.chance(6) {
        return rng.pick(WHOLE_JUNK).as_bytes().to_vec();
    }
    let Ok(Value::Object(mut fields)) = serde_json::from_slice::<Value>(value) else {
        return value.to_vec();
    };
    for _ in 0..1 + rng.below(3) {
        let field = match rng.below(3) {
            0 => *rng.pick(&["resource", "scope"]),
            _ => *rng.pick(fields_of(signal)),
        };
        let junk = match (field, rng.chance(2)) {
            ("scope", true) => json!({"name": junk(rng), "version": junk(rng), "attributes": junk(rng)}),
            _ => junk(rng),
        };
        fields.insert(field.into(), junk);
    }
    Value::Object(fields).to_string().into_bytes()
}

/// Refusals a row carried from an OTLP receiver earns of the destination itself: a span with no IDs, or an ID of
/// another length than OTLP's.
const REFUSALS: &[&str] = &["a span needs a traceId and a spanId", "hex digits"];

/// What the service said, as a refusal of the destination names it.
const SERVICE_REFUSALS: &[&str] = &["refused it: HTTP 400", "refused it: HTTP 413", "refused it: HTTP 422"];

/// The two receivers of a signal: the first takes the exports, the second what the destination sends.
struct Ends {
    signal: Signal,
    first: Box<dyn Source>,
    from: SocketAddr,
    second: Box<dyn Source>,
    to: SocketAddr,
}

/// Several exports of one signal, through one destination of random settings to a service of random answers.
fn fuzz_one(seed: u64, receivers: &mut [Ends], coverage: &mut Coverage) -> Result<(), String> {
    // Rows made wrong come only in seeds of their own, so every other seed holds the round trip to every row. Only
    // those seeds draw doubles a row or a request does not carry as they are.
    let wild = Rng::seeded(!seed).chance(4);
    let mut g = Gen {
        rng: Rng::seeded(seed),
        labels: Vec::new(),
        collected: false,
        exact: !wild,
    };
    let at = g.rng.below(receivers.len());
    let Ends {
        signal,
        first,
        from,
        second,
        to,
    } = &mut receivers[at];
    let signal = *signal;
    coverage.reach(format!("signal {}", signal.name()));
    let gzip = g.rng.chance(2);
    coverage.reach(if gzip { "gzip" } else { "no compression" });
    let max_request_bytes = *g.rng.pick(&[64, 512, 4096, 1 << 16, 1 << 20]);
    coverage.reach(format!("maxRequestBytes {max_request_bytes}"));
    // A throttled answer naps up to the stall threshold, so most are short.
    let stall_after = *g.rng.pick(&[0, 2, 20, 50, 50, 50, 300]);
    coverage.reach(format!("stall {stall_after}ms"));
    let landed = Landed::default();
    let answered = Answered::default();
    let service = FuzzService {
        seed: !seed,
        signal,
        gzip,
        attempts: HashMap::new(),
        landed: Arc::clone(&landed),
        answered: Arc::clone(&answered),
    };
    let config = OtlpDestinationConfig {
        http: HttpDestinationConfig {
            url: "http://collector:4318".into(),
            zone_urls: Default::default(),
            body: Default::default(),
            payload: Payload::Record,
            compression: if gzip { Compression::Gzip } else { Compression::None },
            headers: Vec::new(),
            credentials: Vec::new(),
            tls: None,
            in_flight: 1,
            max_request_bytes,
            buffer_bytes: max_request_bytes,
            stall_after: Duration::from_millis(stall_after),
        },
        signal,
    };
    let unwritten = Unwritten::new(
        smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "otlp-destination-fuzz"),
        0,
        "d",
    );
    let mut d = HttpDestination::new(service, Otlp::new(&config).with_unwritten(unwritten))
        .with_max_request_bytes(max_request_bytes)
        .with_stall_after(config.http.stall_after);
    if wild {
        coverage.reach("rows made wrong");
    }
    for _ in 0..1 + g.rng.below(3) {
        let export = random_export(&mut g, signal);
        let body = pb_export(&mut g, &export);
        let (status, records) = exchange(first, *from, signal, body);
        if status != 200 {
            coverage.reach("an export the receiver refuses");
            continue;
        }
        let mut rows = carried(signal, &records);
        if wild {
            for r in &mut rows {
                if g.rng.chance(2) {
                    r.value = mutated(&mut g.rng, signal, &r.value);
                }
            }
        }
        let given = rows.clone();
        let shown = |rows: &[OwnedRecord]| {
            (rows.iter())
                .map(|r| String::from_utf8_lossy(&r.value).into_owned())
                .collect::<Vec<_>>()
        };
        let delivered = match catch_unwind(AssertUnwindSafe(|| deliver(&mut d, rows))) {
            Err(_) => return Err(format!("panicked delivering {:#?}", shown(&given))),
            Ok(delivered) => delivered,
        };

        // What landed is what the second receiver takes, each body whole.
        let mut arrived = Vec::new();
        for body in std::mem::take(&mut *landed.lock().unwrap()) {
            let (status, records) = exchange(second, *to, signal, body.clone());
            if status != 200 {
                return Err(format!(
                    "the receiver answered {status} to what the destination sent: {body:02x?}"
                ));
            }
            coverage.reach("a body read back");
            arrived.extend(carried(signal, &records).into_iter().map(|r| r.value));
        }
        let mut expected: Vec<Option<Vec<u8>>> = given.iter().map(|r| Some(r.value.clone())).collect();
        match delivered {
            Ok(refused) => {
                for r in &refused {
                    let Some(slot) = expected.get_mut(r.doc) else {
                        return Err(format!("refused row {} of {}", r.doc, given.len()));
                    };
                    if slot.take().is_none() {
                        return Err(format!("refused row {} twice", r.doc));
                    }
                    if r.record
                        .as_ref()
                        .is_some_and(|record| record.value != given[r.doc].value)
                    {
                        return Err(format!("refused row {} with another's record", r.doc));
                    }
                    if SERVICE_REFUSALS.iter().any(|s| r.reason.contains(s)) {
                        coverage.reach("a row the service refused");
                    } else if REFUSALS.iter().any(|s| r.reason.contains(s)) {
                        coverage.reach("a row the destination refused");
                    } else if wild {
                        coverage.reach("a row made wrong refused");
                    } else {
                        return Err(format!(
                            "refused a row a receiver gave: {}\n{}",
                            r.reason,
                            String::from_utf8_lossy(&given[r.doc].value)
                        ));
                    }
                }
                if !wild {
                    let mut expected: Vec<Vec<u8>> = expected.into_iter().flatten().collect();
                    expected.sort();
                    arrived.sort();
                    if arrived != expected {
                        let at = (arrived.iter().zip(&expected)).position(|(a, e)| a != e);
                        let shown = |rows: &[Vec<u8>]| match at {
                            Some(at) => String::from_utf8_lossy(&rows[at]).into_owned(),
                            None => format!("{} rows", rows.len()),
                        };
                        return Err(format!(
                            "{} rows arrived of the {} expected; first apart at {at:?}:\narrived  {}\nexpected {}",
                            arrived.len(),
                            expected.len(),
                            shown(&arrived),
                            shown(&expected),
                        ));
                    }
                    if !expected.is_empty() {
                        coverage.reach("rows arrived as they were given");
                    }
                }
            }
            // The service's own answer, or the stall threshold passed: an error the destination names.
            Err(e) if e.message.starts_with("the service ") => coverage.reach("a batch failed by the service"),
            Err(e) if e.message.contains("still undelivered after") => coverage.reach("a batch stalled"),
            Err(e) => return Err(format!("failed a batch for another reason: {e:?}")),
        }
    }
    for answer in answered.lock().unwrap().iter() {
        coverage.reach(*answer);
    }
    Ok(())
}

const ROW_OPTIONS: &[&str] = &[
    "signal logs",
    "signal metrics",
    "signal traces",
    "gzip",
    "no compression",
    "maxRequestBytes 64",
    "maxRequestBytes 512",
    "maxRequestBytes 4096",
    "maxRequestBytes 65536",
    "maxRequestBytes 1048576",
    "stall 0ms",
    "stall 2ms",
    "stall 20ms",
    "stall 50ms",
    "stall 300ms",
    "rows made wrong",
    "answer 2xx",
    "answer 2xx with a partial success",
    "answer 2xx with what is no response",
    "answer split",
    "answer retry",
    "answer other",
    "a body read back",
    "a row the service refused",
    "a row the destination refused",
    "a row made wrong refused",
    "rows arrived as they were given",
    "a batch failed by the service",
    "a batch stalled",
];

/// Random exports, carried from one receiver through the destination to random answers: the destination never
/// panics, fails a batch only for the service's answer or its stall threshold, refuses only rows it was given, each
/// once, and what lands, read by a second receiver and carried the same way, is every row it did not refuse.
#[test]
fn random_exports_and_answers_arrive_as_they_were_received() {
    let mut receivers: Vec<_> = SIGNALS
        .into_iter()
        .map(|signal| {
            let (from, to) = (free_port(), free_port());
            Ends {
                signal,
                first: receiver(from, signal),
                from,
                second: receiver(to, signal),
                to,
            }
        })
        .collect();
    let mut coverage = Coverage::new(ROW_OPTIONS.iter().copied());
    fuzz::run(1..600, |seed| fuzz_one(seed, &mut receivers, &mut coverage));
    coverage.assert_complete();
}

// -- live: a real OpenTelemetry Collector

/// How the destination reaches the collector.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Reach {
    Plain,
    /// Over TLS, verifying the collector's certificate.
    Trusts,
    /// That, and presenting a certificate of its own, which the collector demands.
    Presents,
}

/// The bearer token the collector demands where it demands one.
const COLLECTOR_TOKEN: &str = "c0llect";

/// The collector's configuration: take OTLP/HTTP of `signal` at `listening`, reached as `reach` says and demanding
/// [`COLLECTOR_TOKEN`] when `token`, and pass it on as protobuf to `at`.
fn collector_config(listening: SocketAddr, at: SocketAddr, signal: Signal, reach: Reach, token: bool) -> String {
    let tls = match reach {
        Reach::Plain => String::new(),
        Reach::Trusts => "\n        tls: {cert_file: /smeltr/server.pem, key_file: /smeltr/server-key.pem}".to_string(),
        Reach::Presents => "\n        tls: {cert_file: /smeltr/server.pem, key_file: /smeltr/server-key.pem, \
                            client_ca_file: /smeltr/ca.pem}"
            .to_string(),
    };
    let (extensions, auth, service_extensions) = match token {
        true => (
            format!("extensions:\n  bearertokenauth:\n    token: \"{COLLECTOR_TOKEN}\"\n"),
            "\n        auth: {authenticator: bearertokenauth}",
            "\n  extensions: [bearertokenauth]",
        ),
        false => (String::new(), "", ""),
    };
    format!(
        "{extensions}receivers:\n  otlp:\n    protocols:\n      http:\n        endpoint: {listening}{tls}{auth}\n\
         exporters:\n  otlphttp:\n    endpoint: http://{at}\n    encoding: proto\n    compression: none\n    \
         sending_queue: {{enabled: false}}\n    retry_on_failure: {{enabled: false}}\n    headers: {{authorization: \
         \"Bearer s3cret\"}}\nservice:{service_extensions}\n  telemetry:\n    metrics: {{level: none}}\n  \
         pipelines:\n    {signal}:\n      receivers: [otlp]\n      exporters: [otlphttp]\n",
        signal = signal.name()
    )
}

/// Waits for something to listen at `at`.
fn listening(at: SocketAddr, within: Duration) -> bool {
    let until = Instant::now() + within;
    while Instant::now() < until {
        if std::net::TcpStream::connect_timeout(&at, Duration::from_millis(200)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// A bundle of one OTLP destination of `signal` sending to `url`, with the settings `d` adds.
fn bundle(url: &str, signal: Signal, mut d: Map<String, Value>) -> String {
    d.insert("id".into(), json!("collector"));
    d.insert("type".into(), json!("otlp"));
    d.insert("url".into(), json!(url));
    d.insert("signal".into(), json!(signal.name()));
    json!({
        "version": 1,
        "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
        "destinations": [d],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["collector"]}]}],
    })
    .to_string()
}

/// A real OpenTelemetry Collector takes what a destination loaded from a bundle sends, of each signal, compressed or
/// not, plain or over TLS with or without a client certificate, with or without a bearer token, in requests of
/// random sizes several at once, and passes it on to a receiver: every row the destination did not refuse arrives as
/// it was given.
#[test]
fn live_a_collector_passes_on_what_the_fuzzed_destination_sends() {
    const TEST: &str = "live_a_collector_passes_on_what_the_fuzzed_destination_sends";
    let Some(collector) = crate::live::otel_collector(TEST) else {
        return;
    };
    const REQUEST_BYTES: [usize; 3] = [4096, 16 << 10, 1 << 20];
    const IN_FLIGHT: [usize; 3] = [1, 2, 4];
    let pki = crate::harness::destination::Pki::new("otlp-live", "localhost", &["localhost"]);
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("collector-token".to_string(), 1, format!("Bearer {COLLECTOR_TOKEN}"))]);
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: "client".to_string(),
        version: 1,
        value: pki.client.key.clone(),
        chain: Some(pki.client.certificate.clone()),
    }]);
    let mut coverage = Coverage::new(
        (SIGNALS.iter().map(|s| format!("signal {}", s.name())))
            .chain(["Plain", "Trusts", "Presents"].map(|r| format!("reach {r}")))
            .chain(REQUEST_BYTES.map(|n| format!("maxRequestBytes {n}")))
            .chain(IN_FLIGHT.map(|n| format!("inFlight {n}")))
            .chain(
                [
                    "gzip",
                    "no compression",
                    "a bearer token",
                    "no token",
                    "bufferBytes",
                    "requests split",
                    "rows arrived as they were given",
                ]
                .map(String::from),
            ),
    );
    fuzz::run(1..19, |seed| {
        let mut g = Gen {
            rng: Rng::seeded(seed),
            labels: Vec::new(),
            collected: true,
            exact: true,
        };
        let signal = *g.rng.pick(&SIGNALS);
        let gzip = g.rng.chance(2);
        let reach = *g.rng.pick(&[Reach::Plain, Reach::Trusts, Reach::Presents]);
        let token = g.rng.chance(2);
        let max_request_bytes = *g.rng.pick(&REQUEST_BYTES);
        let in_flight = *g.rng.pick(&IN_FLIGHT);
        let mut settings = Map::new();
        settings.insert("compression".into(), json!(if gzip { "gzip" } else { "none" }));
        settings.insert("maxRequestBytes".into(), json!(max_request_bytes));
        settings.insert("inFlight".into(), json!(in_flight));
        settings.insert("stallAfterSecs".into(), json!(*g.rng.pick(&[10, 30])));
        let buffered = g.rng.chance(2);
        if buffered {
            settings.insert("bufferBytes".into(), json!(4 * max_request_bytes.max(1 << 20)));
        }
        if token {
            settings.insert(
                "credentials".into(),
                json!({"Authorization": {"secret": "collector-token", "version": 1}}),
            );
        }
        if reach != Reach::Plain {
            settings.insert("trustedAuthorities".into(), json!(pki.authority.pem));
        }
        if reach == Reach::Presents {
            settings.insert("certificate".into(), json!("client"));
        }
        let mut reached = vec![
            format!("signal {}", signal.name()),
            format!("reach {reach:?}"),
            format!("maxRequestBytes {max_request_bytes}"),
            format!("inFlight {in_flight}"),
            (if gzip { "gzip" } else { "no compression" }).to_string(),
            (if token { "a bearer token" } else { "no token" }).to_string(),
        ];
        if buffered {
            reached.push("bufferBytes".into());
        }

        let (from, to) = (free_port(), free_port());
        let (mut first, mut second) = (receiver(from, signal), receiver(to, signal));
        let mut given = Vec::new();
        for _ in 0..1 + g.rng.below(4) {
            let export = random_export(&mut g, signal);
            let body = pb_export(&mut g, &export);
            let (status, records) = exchange(&mut first, from, signal, body);
            if status != 200 {
                return Err(format!("the receiver answered {status} to a collected export"));
            }
            given.extend(carried(signal, &records));
        }

        let scratch = Scratch(std::env::temp_dir().join(format!("smeltr-otlp-out-{}-{seed}", std::process::id())));
        std::fs::create_dir_all(&scratch.0).unwrap();
        let collector_at = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), free_port().port());
        for (file, contents) in [
            ("config.yaml", collector_config(collector_at, to, signal, reach, token)),
            ("ca.pem", pki.authority.pem.clone()),
            ("server.pem", pki.served.certificate.clone()),
            ("server-key.pem", pki.served.key.clone()),
        ] {
            std::fs::write(scratch.0.join(file), contents).unwrap();
        }
        // The collector runs as a user of its own, which reads what this test wrote.
        share(&scratch.0);
        let name = format!("smeltr-otlp-out-{}-{seed}", std::process::id());
        let started = std::process::Command::new("docker")
            .args(["run", "-d", "--network", "host", "--name", &name, "-v"])
            .arg(format!("{}:/smeltr:ro", scratch.0.display()))
            .args([collector.as_str(), "--config", "/smeltr/config.yaml"])
            .output()
            .expect("docker runs");
        // A run that fails may still have created its container.
        let container = Container(name);
        if !started.status.success() {
            return Err(format!("docker run: {}", String::from_utf8_lossy(&started.stderr)));
        }
        let logs = || container_logs(&container.0);
        if !listening(collector_at, Duration::from_secs(30)) {
            return Err(format!("the collector never listened\n{}", logs()));
        }

        let scheme = if reach == Reach::Plain { "http" } else { "https" };
        let json = bundle(
            &format!("{scheme}://localhost:{}", collector_at.port()),
            signal,
            settings,
        );
        let system = smeltr::config_file::load_str(&json).map_err(|e| format!("does not load: {e}\n{json}"))?;
        let kind = &system.topology.destinations[0].kind;
        let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "otlp-live");
        let mut d = smeltr::io::build_destination(kind, 0, "collector", metrics, &secrets, None)
            .map_err(|e| format!("does not build: {e}\n{json}"))?;
        let rows = given.clone();
        // The collector answers each request once the receiver has committed what it passed on, so the receiver is
        // polled while the destination delivers.
        let delivered = std::thread::spawn(move || {
            crate::harness::destination::write(d.as_mut(), &[rows], Duration::from_secs(60))
        });
        let mut arrived = Vec::new();
        let mut requests = 0;
        let mut poll = |arrived: &mut Vec<Vec<u8>>| {
            let batch = second.poll_batch(10_000, 1 << 26, Duration::from_millis(20)).unwrap();
            commit(&mut second, &batch);
            if !batch.is_empty() {
                requests += 1;
            }
            arrived.extend(carried(signal, &batch).into_iter().map(|r| r.value));
        };
        while !delivered.is_finished() {
            poll(&mut arrived);
        }
        // A little longer, so a row sent twice shows.
        let settled = Instant::now() + Duration::from_secs(1);
        while Instant::now() < settled {
            poll(&mut arrived);
        }
        if requests > 1 {
            reached.push("requests split".into());
        }
        let refused = delivered
            .join()
            .unwrap()
            .map_err(|e| format!("delivering to the collector: {e}\n{json}\n{}", logs()))?
            .remove(0);
        let mut expected: Vec<Option<Vec<u8>>> = given.iter().map(|r| Some(r.value.clone())).collect();
        for r in &refused {
            if !REFUSALS.iter().any(|s| r.reason.contains(s)) {
                return Err(format!(
                    "refused a row a receiver gave: {}\n{}",
                    r.reason,
                    String::from_utf8_lossy(&given[r.doc].value)
                ));
            }
            expected[r.doc] = None;
        }
        let mut expected: Vec<Vec<u8>> = expected.into_iter().flatten().collect();
        expected.sort();
        arrived.sort();
        if arrived != expected {
            let at = (arrived.iter().zip(&expected)).position(|(a, e)| a != e);
            let shown = |rows: &[Vec<u8>]| match at {
                Some(at) => String::from_utf8_lossy(&rows[at]).into_owned(),
                None => format!("{} rows", rows.len()),
            };
            return Err(format!(
                "{} rows arrived of the {} expected; first apart at {at:?}:\narrived  {}\nexpected {}\n{}",
                arrived.len(),
                expected.len(),
                shown(&arrived),
                shown(&expected),
                logs(),
            ));
        }
        if !expected.is_empty() {
            reached.push("rows arrived as they were given".into());
        }
        for option in reached {
            coverage.reach(option);
        }
        Ok(())
    });
    coverage.assert_complete();
}
