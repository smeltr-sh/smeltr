//! The Kafka destination's fuzzers. Each seed draws one destination as a bundle names it, every option at random, and
//! sometimes breaks one of its settings.
//!
//! Offline, a destination the save refuses is refused on the setting that was broken. One it accepts loads as drawn,
//! loads the same once saved, builds when this worker holds every credential it names and refuses to when it does
//! not, and delivers drawn batches to librdkafka's mock cluster in this process. The mock now and then refuses a
//! produce request the producer must send again, or loses every broker, which the destination must report within its
//! stall. What the topics hold at the end is each row the destination took, once, byte for byte, with its key, on
//! the topic the row named or the default; what it refused is exactly what had to be: a row with nothing to write,
//! and one past the largest message the producer sends. A row reaches the destination already written in its format,
//! so the format and field are checked where the destination is loaded. Any other change to the JSON may be refused,
//! and never panics.
//!
//! Live, draws without TLS, SASL, a fault or a break are pointed at the broker at `KAFKA_TEST_BROKERS`, and
//! deliver to topics each seed makes and reads back.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{Map, Value, json};
use smeltr::config_file::load_str;
use smeltr::io::{Destination, Dispatched, IoError, OutgoingRecord, OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{Acks, DestinationKind, HeldSecrets, KafkaDestinationConfig, KafkaService, SecretRef};

use crate::harness::destination::{self, Broken, ClientTls, Pki, Reached, set_tls, unknown_key_taken};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::live;

const SEEDS: u64 = 300;
const LIVE_SEEDS: u64 = 24;

/// A worker-local credential no test sets, so a destination naming it cannot build.
const UNSET_VARIABLE: &str = "SMELTR_FUZZ_NEVER_SET";

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// Past the largest message the producer sends to the mock (librdkafka's default request ceiling) and to a broker
/// left at its own default, so the producer refuses it before sending.
const TOO_LARGE: usize = 1_100_000;

/// The topics a seed writes: the destination's default, and one rows name.
const DEFAULT_TOPIC: &str = "fuzz-default";
const OTHER_TOPIC: &str = "fuzz.other_2";

/// Every option the generator knows. Each must be drawn into a destination the save accepts, or into the break it
/// names, over the seed range.
const OPTIONS: &[&str] = &[
    "brokers.loopback",
    "brokers.localhost",
    "brokers.several",
    "brokers.spaced",
    "brokers.elsewhere",
    "format.json",
    "format.text",
    "format.bytes",
    "format.sourceBytes",
    "field",
    "compression.none",
    "compression.gzip",
    "compression.snappy",
    "compression.lz4",
    "compression.zstd",
    "acks.all",
    "acks.one",
    "acks.none",
    "stallAfterSecs",
    "bufferBytes",
    "bufferBytes.text",
    "tls.trustedAuthorities",
    "tls.certificate",
    "properties",
    "properties.plaintext",
    "properties.sasl",
    "credentials.leaderStored",
    "credentials.workerLocal",
    "partitions.several",
    "row.placement",
    "row.placedDefault",
    "row.key",
    "row.unfit",
    "row.tooLarge",
    "batches.several",
    "fault.produceRetried",
    "fault.brokersDown",
    "broken.brokers",
    "broken.defaultTopic",
    "broken.field.json",
    "broken.field.blank",
    "broken.certificate",
    "broken.tls.material",
    "broken.tls.plaintext",
    "broken.properties.reserved",
    "broken.properties.credential",
    "broken.properties.pem",
    "broken.properties.unknown",
    "broken.properties.value",
    "broken.credentials.mechanism",
    "broken.credentials.both",
    "broken.sasl.login",
    "broken.sasl.case",
    "broken.stallAfterSecs",
    "broken.bufferBytes",
    "mangled",
];

/// One authority, and a client certificate it issued, minted once per test.
fn pki() -> Pki {
    Pki::new("smeltr kafka fuzz", "kafka.fuzz", &["localhost"])
}

// -- drawing a destination ----------------------------------------------------------------------------------------

/// Where a draw is pointed.
#[derive(Clone, Copy)]
enum Target<'a> {
    /// The mock cluster in this process, at these bootstrap servers.
    Offline(&'a str),
    /// The broker at these bootstrap servers.
    Live(&'a str),
}

/// One drawn destination.
struct Draw {
    destination: Map<String, Value>,
    tls: Option<ClientTls>,
    /// Whether the producer reaches brokers that answer it, in plain text: the mock, or the live broker.
    reaches: bool,
    /// The leader-stored credentials it names, with their values.
    stored: Vec<(String, i64, String)>,
    /// Whether it names a worker-local credential, which this worker never has.
    unheld: bool,
    /// The field the save refuses a broken setting on, if one was broken.
    broken: Option<&'static str>,
}

fn draw(rng: &mut Rng, cov: &mut Reached, target: Target) -> Draw {
    let mut d = Map::new();
    d.insert("id".into(), json!("kafka"));
    d.insert("type".into(), json!("kafka"));
    let mut reaches = true;
    let brokers = match target {
        Target::Live(brokers) => brokers.to_string(),
        Target::Offline(bootstrap) => {
            let several = bootstrap.contains(',');
            let brokers = match rng.below(5) {
                0 => {
                    cov.reach("brokers.elsewhere");
                    reaches = false;
                    "kafka.internal:9092,kafka-2.internal:9092".to_string()
                }
                1 => {
                    cov.reach("brokers.localhost");
                    bootstrap.replace("127.0.0.1", "localhost")
                }
                2 if bootstrap.contains(',') => {
                    cov.reach("brokers.spaced");
                    bootstrap.replace(',', ", ")
                }
                _ => {
                    cov.reach("brokers.loopback");
                    bootstrap.to_string()
                }
            };
            if several && reaches {
                cov.reach("brokers.several");
            }
            brokers
        }
    };
    d.insert("brokers".into(), json!(brokers));
    d.insert("defaultTopic".into(), json!(DEFAULT_TOPIC));

    match rng.below(5) {
        0 => cov.reach("format.json"),
        n => {
            let format = ["json", "text", "bytes", "sourceBytes"][n - 1];
            cov.reach(format!("format.{format}"));
            d.insert("format".into(), json!(format));
            if format != "json" && rng.chance(2) {
                cov.reach("field");
                // `bytes` writes a field the source declares as bytes, and `text` one it may hold as text.
                let field = if format == "text" {
                    *rng.pick(&["message", "_raw"])
                } else {
                    "__source.bytes"
                };
                d.insert("field".into(), json!(field));
            }
        }
    }
    let compression = *rng.pick(&["", "none", "gzip", "snappy", "lz4", "zstd"]);
    if compression.is_empty() {
        cov.reach("compression.none");
    } else {
        cov.reach(format!("compression.{compression}"));
        d.insert("compression".into(), json!(compression));
    }
    let acks = *rng.pick(&["", "none", "one", "all"]);
    if acks.is_empty() {
        cov.reach("acks.all");
    } else {
        cov.reach(format!("acks.{acks}"));
        d.insert("acks".into(), json!(acks));
    }
    if rng.chance(2) {
        cov.reach("stallAfterSecs");
        d.insert("stallAfterSecs".into(), json!(rng.pick(&[1, 5, 30, 120])));
    }
    if rng.chance(2) {
        if rng.chance(2) {
            cov.reach("bufferBytes.text");
            d.insert("bufferBytes".into(), json!(rng.pick(&["4 MiB", "16MiB", "1 GiB"])));
        } else {
            cov.reach("bufferBytes");
            d.insert("bufferBytes".into(), json!(rng.pick(&[4 * MIB, 64 * MIB, 1024 * MIB])));
        }
    }

    let mut properties = Map::new();
    if rng.chance(2) {
        cov.reach("properties");
        let pool = [
            ("socket.nagle.disable", "true"),
            ("socket.keepalive.enable", "true"),
            ("request.timeout.ms", "20000"),
            ("connections.max.idle.ms", "60000"),
            ("retry.backoff.ms", "50"),
        ];
        for _ in 0..1 + rng.below(3) {
            let (k, v) = *rng.pick(&pool);
            properties.insert(k.into(), json!(v));
        }
    }
    let mut tls = None;
    if matches!(target, Target::Offline(_)) {
        if rng.chance(4) {
            // Over TLS, which the mock does not speak.
            let own = if rng.chance(2) {
                cov.reach("tls.trustedAuthorities");
                ClientTls::Trusts
            } else {
                cov.reach("tls.certificate");
                ClientTls::Presents
            };
            tls = Some(own);
            set_tls(&mut d, own);
            let protocol = *rng.pick(&["SSL", "ssl", "SASL_SSL"]);
            properties.insert("security.protocol".into(), json!(protocol));
            // Always with a mechanism: without one librdkafka takes GSSAPI, which builds here only with a keytab.
            if protocol == "SASL_SSL" {
                properties.insert("sasl.mechanism".into(), json!(rng.pick(&["PLAIN", "SCRAM-SHA-256"])));
                properties.insert("sasl.username".into(), json!("smeltr"));
            }
            reaches = false;
        } else if reaches && rng.chance(5) {
            if rng.chance(2) {
                cov.reach("properties.plaintext");
                properties.insert("security.protocol".into(), json!(rng.pick(&["PLAINTEXT", "plaintext"])));
            } else {
                // A SASL login the mock does not answer.
                cov.reach("properties.sasl");
                properties.insert("security.protocol".into(), json!("SASL_PLAINTEXT"));
                properties.insert("sasl.mechanism".into(), json!("PLAIN"));
                properties.insert("sasl.username".into(), json!("smeltr"));
                reaches = false;
            }
        }
    }

    let mut stored = Vec::new();
    let mut unheld = false;
    // A SASL login always names its password, which the save requires.
    let sasl = properties.contains_key("sasl.mechanism");
    if sasl || rng.chance(3) {
        let mut credentials = Map::new();
        if matches!(target, Target::Offline(_)) && rng.chance(4) {
            cov.reach("credentials.workerLocal");
            unheld = true;
            credentials.insert("sasl.password".into(), json!(UNSET_VARIABLE));
        } else {
            cov.reach("credentials.leaderStored");
            stored.push(("kafka-password".to_string(), 1, "hunter2".to_string()));
            credentials.insert(
                "sasl.password".into(),
                json!({"secret": "kafka-password", "version": 1}),
            );
        }
        d.insert("credentials".into(), Value::Object(credentials));
    }
    if !properties.is_empty() {
        d.insert("properties".into(), Value::Object(properties));
    }

    let mut draw = Draw {
        destination: d,
        tls,
        reaches,
        stored,
        unheld,
        broken: None,
    };
    if matches!(target, Target::Offline(_)) && rng.chance(3) {
        draw.broken = Some(break_one(rng, cov, &mut draw));
    }
    draw
}

/// The JSON object at `key` of `d`, made if absent.
fn object<'a>(d: &'a mut Map<String, Value>, key: &str) -> &'a mut Map<String, Value> {
    d.entry(key.to_string())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .unwrap()
}

/// Breaks one setting of `draw`, answering the field the save must refuse it on.
fn break_one(rng: &mut Rng, cov: &mut Reached, draw: &mut Draw) -> &'static str {
    let what = *rng.pick(&[
        "brokers",
        "defaultTopic",
        "field.json",
        "field.blank",
        "certificate",
        "tls.material",
        "tls.plaintext",
        "properties.reserved",
        "properties.credential",
        "properties.pem",
        "properties.unknown",
        "properties.value",
        "credentials.mechanism",
        "credentials.both",
        "sasl.login",
        "sasl.case",
        "stallAfterSecs",
        "bufferBytes",
    ]);
    cov.reach(format!("broken.{what}"));
    let d = &mut draw.destination;
    let stored = ("kafka-password".to_string(), 1, "hunter2".to_string());
    match what {
        "brokers" => {
            d.insert("brokers".into(), json!(rng.pick(&["", " "])));
            "brokers"
        }
        "defaultTopic" => {
            d.insert("defaultTopic".into(), json!(rng.pick(&["", " ", "\t"])));
            "defaultTopic"
        }
        "field.json" => {
            if rng.chance(2) {
                d.remove("format");
            } else {
                d.insert("format".into(), json!("json"));
            }
            d.insert("field".into(), json!("message"));
            "field"
        }
        "field.blank" => {
            d.insert("format".into(), json!(rng.pick(&["text", "bytes"])));
            d.insert("field".into(), json!(rng.pick(&["", "  "])));
            "field"
        }
        "certificate" => {
            d.insert("certificate".into(), json!(" "));
            draw.tls = None;
            "certificate"
        }
        "tls.material" => {
            draw.tls = Some(ClientTls::Trusts);
            set_tls(d, ClientTls::Trusts);
            let p = object(d, "properties");
            p.insert("security.protocol".into(), json!("SSL"));
            p.insert(
                rng.pick(&["ssl.ca.location", "enable.ssl.certificate.verification"])
                    .to_string(),
                json!("x"),
            );
            "properties"
        }
        "tls.plaintext" => {
            draw.tls = Some(ClientTls::Presents);
            set_tls(d, ClientTls::Presents);
            let p = object(d, "properties");
            if rng.chance(2) {
                p.remove("security.protocol");
            } else {
                p.insert(
                    "security.protocol".into(),
                    json!(rng.pick(&["PLAINTEXT", "SASL_PLAINTEXT"])),
                );
            }
            "properties.security.protocol"
        }
        "properties.reserved" => {
            let (k, v) = *rng.pick(&[
                ("bootstrap.servers", "elsewhere:9092"),
                ("compression.type", "gzip"),
                ("enable.idempotence", "false"),
                ("acks", "1"),
                ("request.required.acks", "0"),
            ]);
            object(d, "properties").insert(k.into(), json!(v));
            "properties"
        }
        "properties.credential" => {
            let k = *rng.pick(&["sasl.password", "ssl.key.password", "sasl.oauthbearer.client.secret"]);
            object(d, "properties").insert(k.into(), json!("hunter2"));
            "properties"
        }
        "properties.pem" => {
            object(d, "properties").insert(
                "ssl.certificate.pem".into(),
                json!("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----"),
            );
            "properties"
        }
        "properties.unknown" => {
            object(d, "properties").insert(rng.pick(&["no.such.property", "lingerms"]).to_string(), json!("1"));
            "properties"
        }
        "properties.value" => {
            let (k, v) = *rng.pick(&[("socket.keepalive.enable", "maybe"), ("request.timeout.ms", "-5")]);
            object(d, "properties").insert(k.into(), json!(v));
            "properties"
        }
        "credentials.mechanism" => {
            object(d, "credentials").insert(
                "sasl.mechanism".into(),
                json!({"secret": "kafka-password", "version": 1}),
            );
            draw.stored.push(stored);
            "credentials"
        }
        "credentials.both" => {
            object(d, "properties").insert("sasl.username".into(), json!("smeltr"));
            object(d, "credentials").insert(
                "sasl.username".into(),
                json!({"secret": "kafka-password", "version": 1}),
            );
            draw.stored.push(stored);
            "credentials"
        }
        "sasl.login" => {
            let p = object(d, "properties");
            p.insert(
                "security.protocol".into(),
                json!(rng.pick(&["SASL_SSL", "sasl_plaintext"])),
            );
            p.insert(
                "sasl.mechanism".into(),
                json!(rng.pick(&["PLAIN", "SCRAM-SHA-512", "SCRAM-SHA-256"])),
            );
            let missing = *rng.pick(&["sasl.username", "sasl.password"]);
            p.remove(missing);
            object(d, "credentials").remove(missing);
            "properties"
        }
        "sasl.case" => {
            let key = *rng.pick(&["sasl.mechanism", "sasl.mechanisms"]);
            let mechanism = *rng.pick(&["plain", "Scram-Sha-512", "gssapi", "oauthBearer"]);
            object(d, "properties").insert(key.into(), json!(mechanism));
            "properties"
        }
        "stallAfterSecs" => {
            d.insert("stallAfterSecs".into(), json!(rng.pick(&[0, 121, 3600])));
            "stallAfterSecs"
        }
        "bufferBytes" => {
            d.insert("bufferBytes".into(), json!(rng.pick(&[0, 4 * MIB - 1, 1024 * MIB + 1])));
            "bufferBytes"
        }
        other => unreachable!("{other}"),
    }
}

/// `draw` as a whole bundle: a Kafka source, one route to the destination, and the authority it trusts.
fn bundle(draw: &Draw, pki: &Pki) -> Value {
    let mut b = json!({
        "version": 1,
        "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
        "destinations": [Value::Object(draw.destination.clone())],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["kafka"]}]}],
    });
    if let Some(authorities) = b["destinations"][0].get_mut("trustedAuthorities") {
        *authorities = json!(pki.authority.pem);
    }
    b
}

/// The secrets the worker holds for `draw`: every credential the leader stores.
fn secrets_held(draw: &Draw, pki: &Pki) -> HeldSecrets {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored(draw.stored.iter().cloned());
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: "client".to_string(),
        version: 1,
        value: pki.client.key.clone(),
        chain: Some(pki.client.certificate.clone()),
    }]);
    secrets
}

// -- what the save says -------------------------------------------------------------------------------------------

/// The destination `json` loads as.
fn loaded(json: &str) -> Result<KafkaDestinationConfig, String> {
    let system = load_str(json).map_err(|e| format!("{e:?}"))?;
    match &system.topology.destinations[0].kind {
        DestinationKind::Kafka(c) => Ok(c.clone()),
        other => Err(format!("loaded as {other:?}")),
    }
}

/// Checks the save: a broken draw is refused on its field, and an unbroken one is accepted, loads as drawn, and
/// loads the same once saved.
fn check_save(draw: &Draw, json: &str) -> Result<Option<KafkaDestinationConfig>, String> {
    let config = destination::check_save(
        json,
        draw.broken.map(|f| Broken::Field(Some(f))),
        destination::compiles,
        loaded,
    )?;
    if let Some(c) = &config {
        check_loaded(draw, c)?;
    }
    Ok(config)
}

/// A credential as drawn, as the plan names it.
fn secret_ref(v: &Value) -> SecretRef {
    match v {
        Value::String(var) => SecretRef::WorkerLocal(var.clone()),
        v => SecretRef::LeaderStored {
            name: v["secret"].as_str().unwrap().to_string(),
            version: v["version"].as_i64().unwrap(),
        },
    }
}

/// What `draw` says, against what it loaded as.
fn check_loaded(draw: &Draw, c: &KafkaDestinationConfig) -> Result<(), String> {
    let d = &draw.destination;
    let mut wrong = Vec::new();
    let mut expect = |what: &str, want: String, got: String| {
        if want != got {
            wrong.push(format!("{what}: drawn {want}, loaded {got}"));
        }
    };
    let s = |key: &str| d.get(key).and_then(Value::as_str).map(str::to_string);
    expect("brokers", s("brokers").unwrap(), c.brokers.clone());
    expect("defaultTopic", s("defaultTopic").unwrap(), c.default_topic.clone());
    let field = s("field");
    let payload = match s("format").as_deref() {
        None | Some("json") => Payload::Record,
        Some("text") => Payload::Text {
            field: field.unwrap_or_else(|| "_raw".into()).into(),
        },
        _ => Payload::Bytes {
            field: field.unwrap_or_else(|| "__source.bytes".into()).into(),
        },
    };
    expect("format", format!("{payload:?}"), format!("{:?}", c.payload));
    let compression = s("compression").unwrap_or_else(|| "none".into());
    expect(
        "compression",
        compression,
        format!("{:?}", c.compression).to_lowercase(),
    );
    let acks = s("acks").unwrap_or_else(|| "all".into());
    expect("acks", acks, format!("{:?}", c.acks).to_lowercase());
    let stall = d.get("stallAfterSecs").map_or(30, |v| v.as_u64().unwrap());
    expect("stallAfterSecs", stall.to_string(), c.stall_after.as_secs().to_string());
    let buffer = d.get("bufferBytes").map_or(64 * MIB, |v| match v {
        Value::String(s) => smeltr::config_file::bytes::parse(s).unwrap(),
        v => v.as_u64().unwrap() as usize,
    });
    expect("bufferBytes", buffer.to_string(), c.buffer_bytes.to_string());
    let tls = c.tls.as_ref().map(|t| match t.certificate {
        Some(_) => ClientTls::Presents,
        None => ClientTls::Trusts,
    });
    expect("tls", format!("{:?}", draw.tls), format!("{tls:?}"));
    let literal: Vec<(String, String)> = (d.get("properties").and_then(Value::as_object).into_iter().flatten())
        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
        .collect();
    let mut got = c.props.literal.clone();
    got.sort();
    expect("properties", format!("{literal:?}"), format!("{got:?}"));
    let credentials: Vec<(String, SecretRef)> = (d.get("credentials").and_then(Value::as_object).into_iter().flatten())
        .map(|(k, v)| (k.clone(), secret_ref(v)))
        .collect();
    let mut got = c.props.credentials.clone();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    expect("credentials", format!("{credentials:?}"), format!("{got:?}"));
    expect(
        "service",
        format!("{:?}", KafkaService::Kafka),
        format!("{:?}", c.service),
    );
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(wrong.join("\n"))
    }
}

// -- the rows sent ------------------------------------------------------------------------------------------------

/// One row handed to the destination, and what must become of it.
#[derive(Clone)]
struct Sent {
    row: OwnedRecord,
    /// The topic it must be on, or `None` when it must be refused.
    topic: Option<String>,
    key: Option<Vec<u8>>,
}

/// A row's value: the seed's marker, so its rows are told from any others on the topic, then any bytes.
fn value(rng: &mut Rng, marker: &str) -> Vec<u8> {
    let mut v = marker.as_bytes().to_vec();
    let body: Vec<u8> = match rng.below(7) {
        0 => Vec::new(),
        1 => "é日本語🦀\u{202e}\t\r\n\"\\".as_bytes().to_vec(),
        2 => (0..rng.below(256)).map(|_| rng.next_u64() as u8).collect(),
        3 => vec![0; rng.below(64)],
        4 => (0..16 * KIB + rng.below(64 * KIB))
            .map(|i| b'a' + (i % 26) as u8)
            .collect(),
        5 => json!({ "msg": "x", "n": rng.next_u64(), "nested": { "a": [1, 2, 3] } })
            .to_string()
            .into_bytes(),
        _ => format!("line {}", rng.next_u64()).into_bytes(),
    };
    v.extend(body);
    v
}

/// The batches one seed hands over.
fn batches(rng: &mut Rng, cov: &mut Reached, c: &KafkaDestinationConfig, marker: &str, other: &str) -> Vec<Vec<Sent>> {
    let n = 1 + rng.below(3);
    if n > 1 {
        cov.reach("batches.several");
    }
    (0..n)
        .map(|batch| {
            (0..1 + rng.below(30))
                .map(|i| {
                    let mut v = value(rng, &format!("{marker}{batch}.{i}:"));
                    let unfit = rng.chance(12).then_some("no field to write");
                    if unfit.is_none() && rng.chance(25) {
                        v.resize(TOO_LARGE, b'z');
                    }
                    let placement = match rng.below(6) {
                        0 | 1 => Some(other.to_string()),
                        2 => Some(c.default_topic.clone()),
                        _ => None,
                    };
                    let key = rng.chance(3).then(|| {
                        let n = 1 + rng.below(40);
                        (0..n).map(|_| rng.next_u64() as u8).collect::<Vec<u8>>()
                    });
                    let refused = unfit.is_some() || v.len() >= TOO_LARGE;
                    if refused {
                        cov.reach(if unfit.is_some() { "row.unfit" } else { "row.tooLarge" });
                    } else {
                        if placement.as_deref() == Some(other) {
                            cov.reach("row.placement");
                        } else if placement.is_some() {
                            cov.reach("row.placedDefault");
                        }
                        if key.is_some() {
                            cov.reach("row.key");
                        }
                    }
                    let mut out = OutgoingRecord::new(&v);
                    out.payload = &c.payload;
                    out.placement = placement.as_deref();
                    out.key = key.as_deref();
                    out.unfit = unfit.map(Cow::Borrowed);
                    Sent {
                        row: OwnedRecord::from(&out),
                        topic: (!refused).then(|| placement.clone().unwrap_or_else(|| c.default_topic.clone())),
                        key: key.clone(),
                    }
                })
                .collect()
        })
        .collect()
}

/// Hands every batch to `d`. Answers each batch's refused rows, and the batches still in flight.
fn deliver(d: &mut dyn Destination, batches: &[Vec<Sent>]) -> Result<(Vec<Vec<Rejection>>, Vec<u64>), IoError> {
    let mut refused: Vec<Vec<Rejection>> = (0..batches.len()).map(|_| Vec::new()).collect();
    let mut pending = Vec::new();
    for (batch, rows) in batches.iter().enumerate() {
        let owned: Vec<OwnedRecord> = rows.iter().map(|s| s.row.clone()).collect();
        let (dispatched, now) = d.deliver(batch as u64, Cow::Owned(owned))?;
        refused[batch].extend(now);
        if dispatched == Dispatched::InFlight {
            pending.push(batch as u64);
        }
    }
    Ok((refused, pending))
}

/// Hands every batch to `d`, flushes, and waits for each to complete. Answers each batch's refused rows.
fn write(d: &mut dyn Destination, batches: &[Vec<Sent>], deadline: Duration) -> Result<Vec<Vec<Rejection>>, String> {
    let (mut refused, mut pending) = deliver(d, batches).map_err(|e| format!("not delivered: {e:?}"))?;
    let until = Instant::now() + deadline;
    // A flush that times out leaves its rows in flight, as the next tick's flush finds them.
    loop {
        match d.flush() {
            Ok(()) => break,
            Err(e) if Instant::now() > until => return Err(format!("not flushed: {e:?}")),
            Err(_) => {}
        }
    }
    while !pending.is_empty() {
        for done in d.poll_completions().map_err(|e| format!("not landed: {e:?}"))? {
            if let Some(u) = done.undelivered {
                return Err(format!(
                    "rows {:?} of batch {} not landed: {}",
                    u.rows, done.batch, u.error
                ));
            }
            refused[done.batch as usize].extend(done.rejections);
            pending.retain(|b| *b != done.batch);
        }
        if Instant::now() > until {
            return Err(format!("batches {pending:?} never completed"));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(refused)
}

/// One message read back: its topic, key and value.
type Read = (String, Option<Vec<u8>>, Vec<u8>);

/// Reads every partition of `topics` from the start to its end. Answers each message carrying `marker`.
fn read_back(brokers: &str, topics: &[&str], marker: &str) -> Result<Vec<Read>, String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "smeltr-fuzz-read")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| format!("a reader: {e}"))?;
    // Every partition's end, as the flush left it: the next offset to read on each until it is reached.
    let mut assignment = TopicPartitionList::new();
    let mut left: BTreeMap<(String, i32), (i64, i64)> = BTreeMap::new();
    for topic in topics {
        let md = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|e| format!("metadata of {topic}: {e}"))?;
        let partitions: Vec<i32> = md
            .topics()
            .iter()
            .flat_map(|t| t.partitions().iter().map(|p| p.id()))
            .collect();
        if partitions.is_empty() {
            return Err(format!("{topic} has no partitions"));
        }
        for p in partitions {
            let (low, high) = consumer
                .fetch_watermarks(topic, p, Duration::from_secs(10))
                .map_err(|e| format!("the end of {topic}/{p}: {e}"))?;
            if high > low {
                left.insert((topic.to_string(), p), (low, high));
            }
            assignment
                .add_partition_offset(topic, p, Offset::Beginning)
                .map_err(|e| e.to_string())?;
        }
    }
    consumer.assign(&assignment).map_err(|e| format!("assigning: {e}"))?;
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !left.is_empty() {
        if Instant::now() > deadline {
            return Err(format!(
                "never read to the end of {:?}",
                left.keys().collect::<Vec<_>>()
            ));
        }
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(m)) => {
                let v = m.payload().unwrap_or_default().to_vec();
                if v.starts_with(marker.as_bytes()) {
                    seen.push((m.topic().to_string(), m.key().map(<[u8]>::to_vec), v));
                }
                let at = (m.topic().to_string(), m.partition());
                if left.get(&at).is_some_and(|(_, high)| m.offset() + 1 >= *high) {
                    left.remove(&at);
                }
            }
            Some(Err(e)) => return Err(format!("reading: {e}")),
            None => {}
        }
    }
    Ok(seen)
}

/// Checks that the topics hold each row `sent` took, once, with its key, on its topic, and nothing else. A `lossy`
/// delivery may hold a row no times: a producer that waits for no acknowledgement never learns a broker refused it. A
/// `retried` one may hold a row more than once: without idempotence a retry can repeat what the broker already wrote.
fn check_held(sent: &[Vec<Sent>], read: &[Read], lossy: bool, retried: bool) -> Result<(), String> {
    let mut want: BTreeMap<&[u8], (&str, Option<&[u8]>)> = BTreeMap::new();
    for s in sent.iter().flatten() {
        if let Some(topic) = &s.topic {
            want.insert(&s.row.value, (topic, s.key.as_deref()));
        }
    }
    let mut counts: BTreeMap<&[u8], usize> = BTreeMap::new();
    for (topic, key, v) in read {
        let Some((on, k)) = want.get(v.as_slice()) else {
            return Err(format!("a row of {} bytes no batch took is on {topic}", v.len()));
        };
        if on != topic {
            return Err(format!("a row for {on} is on {topic}"));
        }
        if key.as_deref() != *k {
            return Err(format!("a row on {topic} has the key {key:?}, not {k:?}"));
        }
        *counts.entry(v).or_default() += 1;
    }
    for v in want.keys() {
        match counts.get(v) {
            Some(1) => {}
            None if lossy => {}
            Some(_) if retried => {}
            n => {
                return Err(format!(
                    "a row of {} bytes is on its topic {} times",
                    v.len(),
                    n.unwrap_or(&0)
                ));
            }
        }
    }
    Ok(())
}

// -- the offline fuzzer -------------------------------------------------------------------------------------------

/// Seeds checked at once. A seed that delivers builds a producer, whose `message_ceiling` asks the mock for its message
/// ceiling and waits out `MESSAGE_CEILING_TIMEOUT` for an answer the mock never gives, so seeds one after another
/// would take minutes.
const WORKERS: usize = 32;

#[test]
fn kafka_destination_fuzz() {
    let pki = pki();
    let seeds: Vec<u64> = fuzz::seeds(0..SEEDS).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let done = std::sync::Mutex::new(BTreeMap::new());
    std::thread::scope(|scope| {
        for _ in 0..WORKERS {
            scope.spawn(|| {
                while let Some(&seed) = seeds.get(next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)) {
                    let mut reached = Reached::default();
                    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        offline_seed(seed, &mut reached, &pki)
                    }))
                    .unwrap_or_else(|panic| {
                        let why = (panic.downcast_ref::<String>().cloned())
                            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                            .unwrap_or_default();
                        Err(format!("panicked: {why}"))
                    });
                    done.lock().unwrap().insert(seed, (checked, reached));
                }
            });
        }
    });
    let mut done = done.into_inner().unwrap();
    let mut cov = Coverage::new(OPTIONS.iter().copied());
    fuzz::run(0..SEEDS, |seed| {
        let (checked, reached) = done.remove(&seed).expect("every seed was checked");
        if checked.is_ok() {
            reached.credit(&mut cov);
        }
        checked
    });
    cov.assert_complete();
}

fn offline_seed(seed: u64, reached: &mut Reached, pki: &Pki) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let brokers = 1 + rng.below(3) as i32;
    let mock: MockCluster<'static, DefaultProducerContext> =
        MockCluster::new(brokers).map_err(|e| format!("a mock cluster: {e}"))?;
    let partitions = 1 + rng.below(4) as i32;
    if partitions > 1 {
        reached.reach("partitions.several");
    }
    mock.create_topic(DEFAULT_TOPIC, partitions, 1)
        .map_err(|e| e.to_string())?;
    mock.create_topic(OTHER_TOPIC, 1 + rng.below(3) as i32, 1)
        .map_err(|e| e.to_string())?;
    let bootstrap = mock.bootstrap_servers();

    let mut draw = draw(&mut rng, reached, Target::Offline(&bootstrap));
    let mangled = draw.broken.is_none() && rng.chance(12);
    reached.only_breaks = mangled || draw.broken.is_some();
    let unknown = mangled
        && destination::mangle(
            &mut rng,
            reached,
            &mut draw.destination,
            &["unknownSetting", "topic", "defaultIndex"],
        );
    let json = bundle(&draw, pki).to_string();
    if mangled {
        if unknown && unknown_key_taken(&json) {
            return Err(format!("a key it does not know is taken\n{json}"));
        }
        return Ok(());
    }
    let Some(c) = check_save(&draw, &json).map_err(|e| format!("{e}\n{json}"))? else {
        return Ok(());
    };

    let secrets = secrets_held(&draw, pki);
    let build = || {
        let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
        smeltr::io::build_destination(&DestinationKind::Kafka(c.clone()), 0, metrics, &secrets, None)
    };
    if draw.unheld {
        // Refused before it reaches for a broker, naming where the credential was to come from.
        return match build() {
            Ok(_) => Err(format!("built with a credential it does not hold\n{json}")),
            Err(e) if e.to_string().contains(UNSET_VARIABLE) => {
                reached.keep_only(&["credentials.workerLocal"]);
                Ok(())
            }
            Err(e) => Err(format!("refused a credential it does not hold with: {e}\n{json}")),
        };
    }
    if !draw.reaches {
        // Saved and loaded. A producer for brokers that never answer waits out its link before it is built, so only
        // some are: every SASL login, which is drawn seldom, and one in six of the rest.
        if !reached.reached("properties.sasl") && !rng.chance(6) {
            reached.forget(&[
                "brokers.elsewhere",
                "tls.trustedAuthorities",
                "tls.certificate",
                "properties.sasl",
            ]);
            return Ok(());
        }
        build().map_err(|e| format!("does not build: {e:?}\n{json}"))?;
        reached.keep_only(&[
            "brokers.elsewhere",
            "tls.trustedAuthorities",
            "tls.certificate",
            "properties.sasl",
        ]);
        return Ok(());
    }

    let mut d = build().map_err(|e| format!("does not build: {e:?}\n{json}"))?;
    let marker = format!("{seed}:");
    let sent = batches(&mut rng, reached, &c, &marker, OTHER_TOPIC);
    let taken = sent.iter().flatten().filter(|s| s.topic.is_some()).count();
    if c.stall_after <= Duration::from_secs(5) && taken > 0 && rng.chance(4) {
        // Every broker gone: the batches cannot land. A queue too small for them stops taking rows once the stall runs
        // out; otherwise the flush says so once it does.
        for id in 1..=brokers {
            mock.broker_down(id).map_err(|e| e.to_string())?;
        }
        let started = Instant::now();
        let failed = match deliver(d.as_mut(), &sent) {
            Err(e) if e.message.contains("not draining") => Ok(()),
            Err(e) => Err(format!("with every broker down, not delivered: {e:?}")),
            Ok(_) => match d.flush() {
                Ok(()) => Err("flushed with every broker down".to_string()),
                Err(e) if e.message.to_ascii_lowercase().contains("timed out") => Ok(()),
                Err(e) => Err(format!("with every broker down, not flushed: {e:?}")),
            },
        };
        let took = started.elapsed();
        failed.map_err(|e| format!("{e}\n{json}"))?;
        if took < c.stall_after || took > c.stall_after + Duration::from_secs(5) {
            return Err(format!("with every broker down, gave up after {took:?}\n{json}"));
        }
        // Nothing but the fault was checked: no row was read back.
        reached.keep_only(&["fault.brokersDown"]);
        reached.reach("fault.brokersDown");
        return Ok(());
    }
    let retried = taken > 0 && rng.chance(5);
    if retried {
        // Produce requests the broker refuses as sent to the wrong leader, which the producer sends again.
        let n = 1 + rng.below(3);
        mock.request_errors(
            RDKafkaApiKey::Produce,
            &vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION; n],
        );
    }
    let fail = |e: String| format!("{e}\n{json}");
    let refused = write(d.as_mut(), &sent, Duration::from_secs(30)).map_err(fail)?;
    drop(d);
    destination::check_refused(&sent, &refused, |s| s.topic.is_none()).map_err(fail)?;
    let read = read_back(&bootstrap, &[DEFAULT_TOPIC, OTHER_TOPIC], &marker).map_err(fail)?;
    check_held(&sent, &read, retried && c.acks == Acks::None, retried).map_err(fail)?;
    if retried {
        reached.reach("fault.produceRetried");
    }
    Ok(())
}

// -- the live fuzzer ----------------------------------------------------------------------------------------------

/// The options a live draw reaches: no TLS, SASL or worker-local credential, since the broker takes plain text
/// and none of them; no fault; and no break.
fn live_options() -> Vec<&'static str> {
    OPTIONS
        .iter()
        .copied()
        .filter(|o| {
            !o.starts_with("brokers.")
                && !o.starts_with("tls.")
                && !o.starts_with("fault.")
                && !o.starts_with("broken.")
                && ![
                    "mangled",
                    "properties.plaintext",
                    "properties.sasl",
                    "credentials.workerLocal",
                ]
                .contains(o)
        })
        .collect()
}

/// Makes `topics` on the broker, each with its partitions, and answers an admin client to delete them with.
fn make_topics(brokers: &str, topics: &[(String, i32)]) -> Result<AdminClient<DefaultClientContext>, String> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .map_err(|e| format!("an admin client: {e}"))?;
    let new: Vec<NewTopic> = topics
        .iter()
        .map(|(name, partitions)| NewTopic::new(name, *partitions, TopicReplication::Fixed(1)))
        .collect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    for made in runtime
        .block_on(admin.create_topics(&new, &AdminOptions::new()))
        .map_err(|e| format!("making topics: {e}"))?
    {
        made.map_err(|(t, e)| format!("making {t}: {e}"))?;
    }
    Ok(admin)
}

/// Topics made for one seed, deleted when it ends, a panic included.
struct Topics<'a> {
    admin: AdminClient<DefaultClientContext>,
    topics: &'a [(String, i32)],
}

impl Drop for Topics<'_> {
    fn drop(&mut self) {
        let names: Vec<&str> = self.topics.iter().map(|(t, _)| t.as_str()).collect();
        if let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
            let _ = runtime.block_on(self.admin.delete_topics(&names, &AdminOptions::new()));
        }
    }
}

#[test]
fn live_kafka_destination_fuzz() {
    let Some(brokers) = live::kafka("live_kafka_destination_fuzz") else {
        return;
    };
    let pki = pki();
    destination::run(0..LIVE_SEEDS, live_options(), |seed, reached| {
        live_seed(seed, &brokers, reached, &pki)
    });
}

fn live_seed(seed: u64, brokers: &str, reached: &mut Reached, pki: &Pki) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let run = format!("{}-{}-{seed}", std::process::id(), rng.next_u64() % 1_000_000);
    let default_topic = format!("fuzz-default-{run}");
    let other_topic = format!("fuzz.other_{run}");
    let partitions = 1 + rng.below(4) as i32;
    if partitions > 1 {
        reached.reach("partitions.several");
    }
    let topics = [
        (default_topic.clone(), partitions),
        (other_topic.clone(), 1 + rng.below(3) as i32),
    ];

    let mut draw = draw(&mut rng, reached, Target::Live(brokers));
    draw.destination.insert("defaultTopic".into(), json!(default_topic));
    let json = bundle(&draw, pki).to_string();
    let c = check_save(&draw, &json)
        .map_err(|e| format!("{e}\n{json}"))?
        .expect("a live draw is never broken");
    let _made = Topics {
        admin: make_topics(brokers, &topics)?,
        topics: &topics,
    };
    let checked = (|| {
        let secrets = secrets_held(&draw, pki);
        let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
        let mut d = smeltr::io::build_destination(&DestinationKind::Kafka(c.clone()), 0, metrics, &secrets, None)
            .map_err(|e| format!("does not build: {e:?}"))?;
        let marker = format!("{run}:");
        let sent = batches(&mut rng, reached, &c, &marker, &other_topic);
        let refused = write(d.as_mut(), &sent, Duration::from_secs(60))?;
        drop(d);
        destination::check_refused(&sent, &refused, |s| s.topic.is_none())?;
        let read = read_back(brokers, &[&default_topic, &other_topic], &marker)?;
        check_held(&sent, &read, c.acks == Acks::None, false)
    })();
    checked.map_err(|e| format!("{e}\n{json}"))
}
