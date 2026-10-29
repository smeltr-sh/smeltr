//! The Elasticsearch destination's fuzzers. Each seed draws one destination as a bundle names it, every option at
//! random, and sometimes breaks one of its settings.
//!
//! Offline, a destination the save refuses is refused on the setting that was broken. One it accepts loads as drawn,
//! loads the same once saved, builds when this worker holds every credential it names and refuses to when it does
//! not, and delivers drawn batches to a cluster in this process, over TLS when it sets its own TLS. That cluster
//! answers as Elasticsearch does: it files each document under its action's index and id, refuses some, asks for some
//! again, takes some into a failure store, refuses a request past its size limit whole, and is now and then busy or
//! too slow. What it holds at the end is exactly what was sent and not refused, each document once and byte for byte;
//! what was refused is exactly what had to be; and every request carried the credential, the action and the size the
//! destination was given. Any other change to the JSON may be refused, and never panics.
//!
//! A record type's mapping (`io::elastic_mapping`) is drawn too, and checked against the type it maps.
//!
//! Live, the same draws, pointed at the cluster at `ES_TEST_URL`, deliver there. Each seed's indices are made with
//! the mapping of the record its documents are, so a document the mapping refuses and one that is not JSON are
//! refused by the cluster itself, and the documents read back through the search API are the ones sent.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};
use smeltr::config_file::load_str;
use smeltr::core::column::DataType;
use smeltr::io::OwnedRecord;
use smeltr::io::elastic_mapping::{Ecs, disagreements, mapping};
use smeltr::plan::topology::{DestinationKind, DocIdPolicy, ElasticDestinationConfig, HeldSecrets, SecretRef};

use crate::harness::destination::{
    self, Broken, ClientTls, Head, Pki, Reached, connector_secrets, read_request, set_tls, unknown_key_taken,
};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::live;

const SEEDS: u64 = 250;
const LIVE_SEEDS: u64 = 24;
const MAPPING_SEEDS: u64 = 500;

/// A worker-local credential no test sets, so a destination naming it cannot build.
const UNSET_VARIABLE: &str = "SMELTR_FUZZ_NEVER_SET";

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// What the destination asks the bulk API to answer with, after the URL's own path.
const BULK_PATH: &str =
    "/_bulk?filter_path=errors,items.*.status,items.*.error.type,items.*.error.reason,items.*.failure_store";

/// Every option the generator knows. Each must be drawn into a destination the save accepts, or into the break it
/// names, over the seed range.
const OPTIONS: &[&str] = &[
    "url.ipv4",
    "url.localhost",
    "url.https",
    "url.elsewhere",
    "url.ipv6",
    "url.trailingSlash",
    "url.path",
    "zoneUrls.own",
    "zoneUrls.other",
    "zoneUrls.noZone",
    "sniff.listed",
    "sniff.refused",
    "sniff.zoneAttribute",
    "defaultIndex",
    "defaultIndex.escaped",
    "auth.none",
    "auth.leaderStored",
    "auth.workerLocal",
    "auth.apiKey",
    "auth.bareKey",
    "auth.basic",
    "auth.userPassword",
    "auth.lineBreak",
    "tls.trustedAuthorities",
    "tls.certificate",
    "docId.auto",
    "docId.dedupe",
    "docId.overwrite",
    "docId.pipeline",
    "idField",
    "inFlight",
    "maxBulkBytes",
    "bytes.text",
    "bufferBytes",
    "stallAfterSecs",
    "row.placement",
    "row.docId",
    "row.docIdIgnored",
    "row.docIdDuplicate",
    "row.docIdNull",
    "row.docIdEmpty",
    "row.docIdTooLong",
    "row.indexRefused",
    "row.notJson",
    "row.newlineRefused",
    "batches.several",
    "requests.several",
    "answer.refused",
    "answer.retried",
    "answer.failureStore",
    "answer.conflict",
    "answer.busy",
    "answer.split",
    "answer.stall",
    "broken.url.blank",
    "broken.url.tls",
    "broken.zoneUrls",
    "broken.zoneAttribute",
    "broken.defaultIndex",
    "broken.certificate",
    "broken.inFlight",
    "broken.inFlight.affords",
    "broken.maxBulkBytes",
    "broken.bufferBytes",
    "broken.bufferBytes.request",
    "broken.stallAfterSecs",
    "broken.auth",
    "broken.username",
    "broken.password",
    "broken.docId",
    "mangled",
];

/// One authority, the cluster's certificate under it for `localhost`, and a client's, minted once per test.
fn pki() -> Pki {
    Pki::new("smeltr elastic fuzz", "es.fuzz", &["localhost"])
}

// -- drawing a destination ----------------------------------------------------------------------------------------

/// Where a draw is pointed.
#[derive(Clone, Copy)]
enum Target<'a> {
    /// The cluster in this process, to listen on this port of 127.0.0.1.
    Offline(u16),
    /// The cluster at this base URL.
    Live(&'a str),
}

/// Where the drawn URL leads.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Endpoint {
    /// The cluster in this process, over plain HTTP.
    Plain,
    /// The cluster in this process, over TLS.
    Tls,
    /// Somewhere nothing listens: the destination is saved, loaded and built, and sends nothing.
    Elsewhere,
    /// The live cluster.
    Live,
}

/// One drawn destination.
struct Draw {
    destination: Map<String, Value>,
    tls: Option<ClientTls>,
    endpoint: Endpoint,
    /// The zone of the worker that runs the destination.
    zone: Option<&'static str>,
    /// Whether the destination sniffs, and if so whether the cluster lists its nodes.
    sniff: Option<bool>,
    /// The `Authorization` value the cluster is sent.
    auth: Option<String>,
    /// What the leader holds for the credential the destination names: `auth`'s, or `password`'s.
    held: Option<String>,
    /// A pipeline step setting `__destination.id`, for a policy that wants an id from a row with no `idField`.
    sets_id: bool,
    /// The setting broken, if one was. `Some(None)` is a break the save refuses on no one field.
    broken: Option<Option<&'static str>>,
}

fn draw(rng: &mut Rng, cov: &mut Reached, target: Target, index: &str) -> Draw {
    let mut d = Map::new();
    d.insert("id".into(), json!("es"));
    d.insert("type".into(), json!("elasticsearch"));
    let (url, endpoint, tls) = match target {
        Target::Live(url) => (url.to_string(), Endpoint::Live, None),
        Target::Offline(port) => draw_url(rng, cov, port),
    };
    d.insert("url".into(), json!(url));
    if let Some(tls) = tls {
        set_tls(&mut d, tls);
    }
    let mut zone = match target {
        Target::Live(_) => None,
        Target::Offline(_) => destination::draw_zones(rng, cov, &mut d),
    };
    // The cluster in this process names itself as its one node, or refuses to name any. The live cluster names
    // its two nodes, in zones `z1` and `z2` (`tests/live/compose.live.yaml`), and a live worker is in one of them or neither.
    let sniff = rng.chance(3).then(|| {
        d.insert("sniff".into(), json!(true));
        if rng.chance(3) {
            cov.reach("sniff.zoneAttribute");
            let attribute = if matches!(target, Target::Live(_)) {
                "zone"
            } else {
                "rack"
            };
            d.insert("zoneAttribute".into(), json!(attribute));
        }
        if matches!(target, Target::Live(_)) {
            zone = *rng.pick(&[None, Some("z1"), Some("z2")]);
        }
        let listed = matches!(target, Target::Live(_)) || rng.chance(2);
        cov.reach(if listed { "sniff.listed" } else { "sniff.refused" });
        listed
    });
    d.insert("defaultIndex".into(), json!(index));

    const API_KEY: &str = "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw==";
    // `elastic:changeme`, as Basic auth sends it.
    const BASIC: &str = "Basic ZWxhc3RpYzpjaGFuZ2VtZQ==";
    let (mut auth, mut held) = (None, None);
    match rng.below(4) {
        0 => cov.reach("auth.none"),
        3 if !matches!(target, Target::Live(_)) => {
            cov.reach("auth.workerLocal");
            d.insert("auth".into(), json!(UNSET_VARIABLE));
        }
        _ => {
            cov.reach("auth.leaderStored");
            let (field, value, sent) = match rng.below(4) {
                0 => {
                    cov.reach("auth.apiKey");
                    ("auth", format!("ApiKey {API_KEY}"), format!("ApiKey {API_KEY}"))
                }
                1 => {
                    cov.reach("auth.bareKey");
                    ("auth", API_KEY.to_string(), format!("ApiKey {API_KEY}"))
                }
                2 => {
                    cov.reach("auth.basic");
                    ("auth", BASIC.to_string(), BASIC.to_string())
                }
                _ => {
                    cov.reach("auth.userPassword");
                    d.insert("username".into(), json!("elastic"));
                    ("password", "changeme".to_string(), BASIC.to_string())
                }
            };
            // A value pasted with its trailing newline, which no header can carry.
            let pasted = !matches!(target, Target::Live(_)) && rng.chance(12);
            if pasted {
                cov.reach("auth.lineBreak");
            }
            held = Some(format!("{value}{}", if pasted { "\n" } else { "" }));
            auth = Some(sent);
            d.insert(field.into(), json!({"secret": "es-auth", "version": 1 + rng.below(3)}));
        }
    }

    let policy = *rng.pick(&["auto", "dedupe", "overwrite"]);
    cov.reach(format!("docId.{policy}"));
    if policy != "auto" || rng.chance(2) {
        d.insert("docId".into(), json!(policy));
    }
    let mut sets_id = false;
    if rng.chance(3) || (policy != "auto" && rng.chance(2)) {
        cov.reach("idField");
        d.insert("idField".into(), json!(rng.pick(&["event.id", "uid", "a.b"])));
    } else if policy != "auto" {
        cov.reach("docId.pipeline");
        sets_id = true;
    }

    draw_sizes(rng, cov, &mut d);
    if rng.chance(2) {
        cov.reach("stallAfterSecs");
        // A real cluster may take longer than a second to answer a request it is still making an index for.
        let stall = match target {
            Target::Live(_) => *rng.pick(&[30, 120]),
            Target::Offline(_) => *rng.pick(&[1, 1, 2, 30, 120]),
        };
        d.insert("stallAfterSecs".into(), json!(stall));
    }

    let mut draw = Draw {
        destination: d,
        tls,
        endpoint,
        zone,
        sniff,
        auth,
        held,
        sets_id,
        broken: None,
    };
    if !matches!(target, Target::Live(_)) && rng.chance(3) {
        draw.broken = Some(break_one(rng, cov, &mut draw));
    }
    draw
}

/// A URL for the cluster in this process on `port`, or one nothing listens at, and the TLS it is reached under.
fn draw_url(rng: &mut Rng, cov: &mut Reached, port: u16) -> (String, Endpoint, Option<ClientTls>) {
    let tail = match rng.below(6) {
        0 => {
            cov.reach("url.trailingSlash");
            "/"
        }
        1 => {
            cov.reach("url.path");
            "/es"
        }
        2 => {
            cov.reach("url.path");
            cov.reach("url.trailingSlash");
            "/es/"
        }
        _ => "",
    };
    let tls = |rng: &mut Rng, cov: &mut Reached| {
        if rng.chance(2) {
            cov.reach("tls.trustedAuthorities");
            ClientTls::Trusts
        } else {
            cov.reach("tls.certificate");
            ClientTls::Presents
        }
    };
    match rng.below(10) {
        0..=6 => {
            let host = if rng.chance(2) {
                cov.reach("url.ipv4");
                "127.0.0.1"
            } else {
                cov.reach("url.localhost");
                "localhost"
            };
            (format!("http://{host}:{port}{tail}"), Endpoint::Plain, None)
        }
        7 | 8 => {
            cov.reach("url.https");
            let own = tls(rng, cov);
            (format!("https://localhost:{port}{tail}"), Endpoint::Tls, Some(own))
        }
        _ => {
            cov.reach("url.elsewhere");
            let url = *rng.pick(&[
                "https://es.example.test:9200",
                "https://10.1.2.3:9243",
                "http://[::1]:9200",
                "http://es.internal:9200",
            ]);
            if url.contains("[::1]") {
                cov.reach("url.ipv6");
            }
            let own = (url.starts_with("https://") && rng.chance(2)).then(|| tls(rng, cov));
            (format!("{url}{tail}"), Endpoint::Elsewhere, own)
        }
    }
}

/// A byte count as a bundle may state it: a number, or text in any unit that names it exactly.
fn bytes_value(rng: &mut Rng, cov: &mut Reached, n: usize) -> Value {
    match rng.below(3) {
        0 => json!(n),
        1 => {
            cov.reach("bytes.text");
            json!(smeltr::config_file::bytes::exact(n))
        }
        _ if n.is_multiple_of(KIB) => {
            cov.reach("bytes.text");
            json!(format!("{}kib", n / KIB))
        }
        _ => json!(n),
    }
}

/// `maxBulkBytes`, `bufferBytes` and `inFlight`, each within what the others allow.
fn draw_sizes(rng: &mut Rng, cov: &mut Reached, d: &mut Map<String, Value>) {
    let bulk = if rng.chance(2) {
        cov.reach("maxBulkBytes");
        let bulk = *rng.pick(&[64 * KIB, 64 * KIB, 64 * KIB, MIB, 5 * MIB, 100 * MIB, 100_000_000]);
        let value = bytes_value(rng, cov, bulk);
        d.insert("maxBulkBytes".into(), value);
        bulk
    } else {
        5 * MIB
    };
    let mut buffer = (64 * MIB).max(bulk);
    if rng.chance(2) {
        let fits: Vec<usize> = [4 * MIB, 64 * MIB, 256 * MIB, 1024 * MIB]
            .into_iter()
            .filter(|b| *b >= bulk)
            .collect();
        buffer = *rng.pick(&fits);
        cov.reach("bufferBytes");
        let value = bytes_value(rng, cov, buffer);
        d.insert("bufferBytes".into(), value);
    }
    let affords = (buffer / bulk).min(64);
    if rng.chance(2) {
        let fits: Vec<usize> = [1, 2, 4, 8, 16, 64].into_iter().filter(|n| *n <= affords).collect();
        cov.reach("inFlight");
        d.insert("inFlight".into(), json!(rng.pick(&fits)));
    }
}

/// Breaks one setting of `draw`, and answers the field the save must refuse it on.
fn break_one(rng: &mut Rng, cov: &mut Reached, draw: &mut Draw) -> Option<&'static str> {
    let what = *rng.pick(&[
        "url.blank",
        "url.tls",
        "zoneUrls",
        "zoneAttribute",
        "defaultIndex",
        "certificate",
        "inFlight",
        "inFlight.affords",
        "maxBulkBytes",
        "bufferBytes",
        "bufferBytes.request",
        "stallAfterSecs",
        "auth",
        "username",
        "password",
        "docId",
    ]);
    cov.reach(format!("broken.{what}"));
    let d = &mut draw.destination;
    match what {
        "url.blank" => {
            d.insert("url".into(), json!(rng.pick(&["", "  "])));
            Some("url")
        }
        "url.tls" => {
            // TLS settings on a URL that never negotiates TLS.
            draw.tls = Some(ClientTls::Trusts);
            set_tls(d, ClientTls::Trusts);
            d.insert(
                "url".into(),
                json!(rng.pick(&["http://127.0.0.1:9200", "http://es.internal:9200"])),
            );
            Some("url")
        }
        "zoneUrls" => Some(destination::break_zones(rng, d)),
        "zoneAttribute" => {
            if rng.chance(2) {
                // Named, with nothing sniffing to read it.
                d.remove("sniff");
                d.insert("zoneAttribute".into(), json!("rack"));
            } else {
                d.insert("sniff".into(), json!(true));
                d.insert("zoneAttribute".into(), json!(rng.pick(&["", "  "])));
            }
            Some("zoneAttribute")
        }
        "defaultIndex" => {
            d.insert("defaultIndex".into(), json!(rng.pick(&["", " ", "\t"])));
            Some("defaultIndex")
        }
        "certificate" => {
            d.insert("certificate".into(), json!(" "));
            Some("certificate")
        }
        "inFlight" => {
            d.insert("inFlight".into(), json!(rng.pick(&[0, 65, 1000])));
            Some("inFlight")
        }
        "inFlight.affords" => {
            // A whole buffer is one request, so only one may be outstanding.
            d.insert("maxBulkBytes".into(), json!("100 MiB"));
            d.remove("bufferBytes");
            // A stated 4 is the default, which starts at what the buffer affords instead.
            d.insert("inFlight".into(), json!(rng.pick(&[2, 64])));
            Some("inFlight")
        }
        "maxBulkBytes" => {
            d.insert(
                "maxBulkBytes".into(),
                json!(rng.pick(&[0, 64 * KIB - 1, 100 * MIB + 1])),
            );
            Some("maxBulkBytes")
        }
        "bufferBytes" => {
            d.insert("bufferBytes".into(), json!(rng.pick(&[0, 4 * MIB - 1, 1024 * MIB + 1])));
            Some("bufferBytes")
        }
        "bufferBytes.request" => {
            d.insert("maxBulkBytes".into(), json!("100 MiB"));
            d.insert("bufferBytes".into(), json!("64 MiB"));
            d.insert("inFlight".into(), json!(1));
            Some("bufferBytes")
        }
        "stallAfterSecs" => {
            d.insert("stallAfterSecs".into(), json!(rng.pick(&[0, 121, 3600])));
            Some("stallAfterSecs")
        }
        "auth" => {
            let blank = rng
                .pick(&[json!(""), json!(" "), json!({"secret": "", "version": 1})])
                .clone();
            d.insert("auth".into(), blank);
            Some("auth")
        }
        "username" => {
            // A password with no one to sign in as: no username, or a blank one.
            d.remove("auth");
            d.insert("password".into(), json!({"secret": "es-auth", "version": 1}));
            if rng.chance(2) {
                d.insert("username".into(), json!(rng.pick(&["", " "])));
            } else {
                d.remove("username");
            }
            Some("username")
        }
        "password" => {
            d.insert("username".into(), json!("elastic"));
            if rng.chance(2) {
                // A user with no password.
                d.remove("auth");
                d.remove("password");
                Some("password")
            } else {
                // A password beside an API key.
                d.insert("auth".into(), json!({"secret": "es-auth", "version": 1}));
                d.insert("password".into(), json!({"secret": "es-password", "version": 1}));
                Some("auth")
            }
        }
        "docId" => {
            // A policy that wants an id, and nothing that gives one: no field, and no pipeline setting it.
            d.insert("docId".into(), json!(rng.pick(&["dedupe", "overwrite"])));
            d.remove("idField");
            draw.sets_id = false;
            None
        }
        other => unreachable!("{other}"),
    }
}

/// `draw` as a whole bundle: a Kafka source, one route to the destination, and the authority it trusts.
fn bundle(draw: &Draw, pki: &Pki) -> Value {
    let steps = if draw.sets_id {
        json!([{"id": "id", "type": "code", "body": "__destination.id = _raw;"}])
    } else {
        json!([])
    };
    let mut b = json!({
        "version": 1,
        "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
        "destinations": [Value::Object(draw.destination.clone())],
        "pipelines": [{"id": "p", "name": "p", "steps": steps}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["es"]}]}],
    });
    if let Some(authorities) = b["destinations"][0].get_mut("trustedAuthorities") {
        *authorities = json!(pki.authority.pem);
    }
    b
}

/// The secrets the worker holds for `draw`: every credential the leader stores. And whether `draw` names one unheld:
/// a worker-local one, which it never has.
fn secrets_held(draw: &Draw, pki: &Pki) -> (HeldSecrets, bool) {
    let mut stored = Vec::new();
    let mut unheld = false;
    match draw.destination.get("auth").or(draw.destination.get("password")) {
        Some(Value::Object(o)) => stored.push((
            o["secret"].as_str().unwrap().to_string(),
            o["version"].as_i64().unwrap(),
            draw.held.clone().unwrap(),
        )),
        Some(Value::String(_)) => unheld = true,
        _ => {}
    }
    let mut secrets = HeldSecrets::default();
    secrets.set_stored(stored);
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: "client".to_string(),
        version: 1,
        value: pki.client.key.clone(),
        chain: Some(pki.client.certificate.clone()),
    }]);
    (secrets, unheld)
}

// -- what the save says -------------------------------------------------------------------------------------------

/// The destination `json` loads as.
fn loaded(json: &str) -> Result<(ElasticDestinationConfig, Vec<SecretRef>), String> {
    let system = load_str(json).map_err(|e| format!("{e:?}"))?;
    match &system.topology.destinations[0].kind {
        DestinationKind::Elasticsearch(c) => Ok((c.clone(), connector_secrets(&system.topology))),
        other => Err(format!("loaded as {other:?}")),
    }
}

/// Checks the save: a broken draw is refused on its field, and an unbroken one is accepted, loads as drawn, and
/// loads the same once saved.
fn check_save(draw: &Draw, json: &str) -> Result<Option<ElasticDestinationConfig>, String> {
    let config = destination::check_save(json, draw.broken.map(Broken::Field), destination::compiles, loaded)?;
    if let Some((c, named)) = &config {
        check_loaded(draw, c, named)?;
    }
    // What the worker runs: its own zone's address, taken at deploy.
    Ok(config.map(|(c, _)| {
        let mut kind = DestinationKind::Elasticsearch(c);
        kind.take_zone(draw.zone);
        let DestinationKind::Elasticsearch(c) = kind else {
            unreachable!()
        };
        c
    }))
}

/// A byte count as drawn: a number, or text the bundle's own parser reads.
fn drawn_bytes(d: &Map<String, Value>, key: &str) -> Option<usize> {
    d.get(key).map(|v| match v {
        Value::String(s) => smeltr::config_file::bytes::parse(s).unwrap(),
        v => v.as_u64().unwrap() as usize,
    })
}

/// What `draw` says, against what it loaded as.
fn check_loaded(draw: &Draw, c: &ElasticDestinationConfig, named: &[SecretRef]) -> Result<(), String> {
    let d = &draw.destination;
    let mut wrong = Vec::new();
    let mut expect = |what: &str, want: String, got: String| {
        if want != got {
            wrong.push(format!("{what}: drawn {want}, loaded {got}"));
        }
    };
    let s = |key: &str| d.get(key).and_then(Value::as_str).map(str::to_string);
    expect("url", s("url").unwrap(), c.url.clone());
    expect(
        "zoneUrls",
        format!("{:?}", destination::drawn_zones(d)),
        format!("{:?}", c.zone_urls),
    );
    let sniff = d.get("sniff").is_some().then(|| smeltr::plan::topology::Sniff {
        zone_attribute: s("zoneAttribute").unwrap_or(smeltr::plan::topology::Sniff::DEFAULT_ZONE_ATTRIBUTE.to_string()),
        zone: None,
    });
    expect("sniff", format!("{sniff:?}"), format!("{:?}", c.sniff));
    expect("defaultIndex", s("defaultIndex").unwrap(), c.default_index.clone());
    let reference = |v: &Value| match v {
        Value::String(var) => SecretRef::WorkerLocal(var.clone()),
        v => SecretRef::LeaderStored {
            name: v["secret"].as_str().unwrap().to_string(),
            version: v["version"].as_i64().unwrap(),
        },
    };
    let auth = d.get("auth").map(reference);
    expect("auth", format!("{auth:?}"), format!("{:?}", c.auth));
    let password = d.get("password").map(reference);
    let basic = s("username").zip(password.clone());
    expect("basic", format!("{basic:?}"), format!("{:?}", c.basic));
    let tls = c.tls.as_ref().map(|t| match t.certificate {
        Some(_) => ClientTls::Presents,
        None => ClientTls::Trusts,
    });
    expect("tls", format!("{:?}", draw.tls), format!("{tls:?}"));
    let policy = match s("docId").as_deref() {
        None | Some("auto") => DocIdPolicy::Auto,
        Some("dedupe") => DocIdPolicy::Dedupe,
        _ => DocIdPolicy::Overwrite,
    };
    expect("docId", format!("{policy:?}"), format!("{:?}", c.doc_id));
    expect("idField", format!("{:?}", s("idField")), format!("{:?}", c.id_field));
    let bulk = drawn_bytes(d, "maxBulkBytes").unwrap_or(5 * MIB);
    expect("maxBulkBytes", bulk.to_string(), c.max_bulk_bytes.to_string());
    let buffer = drawn_bytes(d, "bufferBytes").unwrap_or((64 * MIB).max(bulk));
    expect("bufferBytes", buffer.to_string(), c.buffer_bytes.to_string());
    // Unstated, the start is the default or what the buffer affords, whichever is fewer.
    let affords = (c.buffer_bytes / c.max_bulk_bytes.max(1)).max(1) as u64;
    let in_flight = d.get("inFlight").map_or(4.min(affords), |v| v.as_u64().unwrap());
    expect("inFlight", in_flight.to_string(), c.in_flight.to_string());
    let stall = d.get("stallAfterSecs").map_or(30, |v| v.as_u64().unwrap());
    expect("stallAfterSecs", stall.to_string(), c.stall_after.as_secs().to_string());
    let credentials: Vec<String> = named.iter().map(|r| format!("{r:?}")).collect();
    let drawn: Vec<String> = auth.iter().chain(&password).map(|r| format!("{r:?}")).collect();
    expect("credentials", format!("{drawn:?}"), format!("{credentials:?}"));
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(wrong.join("\n"))
    }
}

// -- the rows sent ------------------------------------------------------------------------------------------------

/// What the cluster in this process does with one document, by its bytes.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Fate {
    Indexed,
    /// Asked for again this many times, then indexed.
    Retried(u32),
    Refused,
    /// Taken into its data stream's failure store: answered as written, though not to its index.
    FailureStored,
}

/// One row and what must become of it.
struct Sent {
    row: OwnedRecord,
    /// The index it is filed under.
    index: String,
    /// Its `_id` as the action names it.
    id: Option<String>,
    /// `None` when the destination refuses it before it is sent.
    fate: Option<Fate>,
}

impl Sent {
    fn refused(&self) -> bool {
        matches!(self.fate, None | Some(Fate::Refused))
    }
}

/// What a seed's documents are, and what the cluster does with them.
struct Rows<'a> {
    policy: DocIdPolicy,
    default_index: &'a str,
    /// Bytes past which a request is posted early: a batch over it goes in several.
    max_bulk: usize,
    /// The indices a row may name. The first is the default.
    placements: &'a [String],
    /// One of them whose name the cluster refuses.
    refused_index: Option<&'a str>,
    /// Live, a record type each document is, from which a body is drawn.
    record: Option<&'a DataType>,
    /// Whether the cluster asks for some documents again.
    retries: bool,
}

/// A message with what a JSON string escapes, and what a line-based reader trips on without a newline.
fn message(rng: &mut Rng) -> String {
    (0..rng.below(12))
        .map(|_| {
            *rng.pick(&[
                'a', 'Z', '0', 'é', '日', '"', '\\', ' ', '\t', '\r', '\u{2028}', '\u{0}', '🦀',
            ])
        })
        .collect()
}

/// The bytes of row `n`, and whether the cluster can read it as a document.
fn body(rng: &mut Rng, cov: &mut Reached, rows: &Rows, n: usize, pad: usize) -> (Vec<u8>, bool) {
    match rng.below(14) {
        0 => {
            cov.reach("row.notJson");
            let mut raw = format!("not json {n} ").into_bytes();
            if rows.record.is_none() {
                raw.extend_from_slice(&[0xff, 0x00, b'\r', b'"', 0xe9]);
            }
            (raw, false)
        }
        1 => {
            cov.reach("row.newlineRefused");
            let text = match rng.below(3) {
                0 => format!("{{\"n\":{n}}}\n"),
                1 => format!("{{\"n\":{n},\n\"m\":1}}"),
                _ => format!("\n{{\"n\":{n}}}"),
            };
            (text.into_bytes(), true)
        }
        _ => {
            let mut doc = match rows.record {
                Some(record) => document(rng, record, ""),
                None => json!({"msg": message(rng)}),
            };
            doc["n"] = json!(n);
            if pad > 0 {
                doc["pad"] = json!("x".repeat(pad));
            }
            // A value the record's mapping cannot take, which the cluster itself refuses.
            let mapped = rows.record.is_none() || !rng.chance(10);
            if !mapped {
                doc["num"] = json!("not a number");
            }
            (serde_json::to_vec(&doc).unwrap(), mapped)
        }
    }
}

/// One to three batches of rows, each with its index, id and fate drawn.
fn batches(rng: &mut Rng, cov: &mut Reached, rows: &Rows) -> Vec<Vec<Sent>> {
    let count = 1 + rng.below(3);
    if count > 1 {
        cov.reach("batches.several");
    }
    let mut n = 0;
    (0..count)
        .map(|batch| {
            // A batch of padded rows fills more than one request at the smallest bulk size.
            let (len, pad) = if rows.max_bulk <= 64 * KIB && rng.chance(3) {
                (24 + rng.below(16), 3000 + rng.below(2000))
            } else {
                (1 + rng.below(14), 0)
            };
            let mut sent: Vec<Sent> = (0..len)
                .map(|_| {
                    n += 1;
                    one_row(rng, cov, rows, batch, n, len, pad)
                })
                .collect();
            if pad > 0 && sent.iter().map(|s| s.row.value.len()).sum::<usize>() > rows.max_bulk {
                cov.reach("requests.several");
            }
            fates(rng, cov, rows, &mut sent);
            sent
        })
        .collect()
}

fn one_row(rng: &mut Rng, cov: &mut Reached, rows: &Rows, batch: usize, n: usize, len: usize, pad: usize) -> Sent {
    let (value, readable) = body(rng, cov, rows, n, pad);
    let placement = (rows.placements.len() > 1 && rng.chance(4)).then(|| {
        cov.reach("row.placement");
        rng.pick(&rows.placements[1..]).clone()
    });
    // Ids drawn from fewer than there are rows, so a batch now and then names one twice. Never across batches: the
    // order two batches in flight land in is the cluster's, and the last write of an id is the one it keeps.
    let doc_id = rng.chance(2).then(|| match rng.below(16) {
        0 => String::new(),
        1 => "x".repeat(513),
        _ => {
            let k = rng.below(len / 2 + 2);
            format!("b{batch}-{k}{}", ["", "\"q", "\\", "é", " "][k % 5])
        }
    });
    let id = match (rows.policy.wants_doc_id(), &doc_id) {
        (true, Some(_)) => {
            cov.reach("row.docId");
            doc_id.clone()
        }
        (true, None) => {
            cov.reach("row.docIdNull");
            None
        }
        (false, Some(_)) => {
            cov.reach("row.docIdIgnored");
            None
        }
        (false, None) => None,
    };
    let index = placement.clone().unwrap_or_else(|| rows.default_index.to_string());
    // The cluster refuses an empty id, one past 512 bytes, and an index it cannot name.
    let mut sendable = readable;
    match id.as_deref() {
        Some("") => {
            cov.reach("row.docIdEmpty");
            sendable = false;
        }
        Some(long) if long.len() > 512 => {
            cov.reach("row.docIdTooLong");
            sendable = false;
        }
        _ => {}
    }
    if rows.refused_index == Some(index.as_str()) {
        cov.reach("row.indexRefused");
        sendable = false;
    }
    let fate = (!value.contains(&b'\n')).then_some(if sendable { Fate::Indexed } else { Fate::Refused });
    Sent {
        index,
        row: OwnedRecord {
            value,
            placement,
            doc_id,
            ..OwnedRecord::default()
        },
        id,
        fate,
    }
}

/// What the cluster in this process does with each row of one batch. A row sharing its id with another is indexed or
/// refused in the order sent; asked for again it would land after a later copy, and which copy the cluster keeps
/// would then be its own choice.
fn fates(rng: &mut Rng, cov: &mut Reached, rows: &Rows, sent: &mut [Sent]) {
    let mut ids: HashMap<(String, String), usize> = HashMap::new();
    for s in sent.iter() {
        if let Some(id) = &s.id {
            *ids.entry((s.index.clone(), id.clone())).or_default() += 1;
        }
    }
    for s in sent.iter_mut() {
        let shared = s.id.as_ref().is_some_and(|id| ids[&(s.index.clone(), id.clone())] > 1);
        if shared {
            cov.reach("row.docIdDuplicate");
        }
        if rows.record.is_some() || s.fate != Some(Fate::Indexed) {
            continue;
        }
        if rng.chance(10) {
            s.fate = Some(Fate::Refused);
        } else if !shared && rows.retries && rng.chance(4) {
            s.fate = Some(Fate::Retried(1 + u32::from(rng.chance(4))));
        } else if !shared && rng.chance(12) {
            s.fate = Some(Fate::FailureStored);
        }
    }
}

/// What a cluster holds once `sent` is delivered under `policy`: by index, each document under its id, and the
/// documents filed under an id the cluster made up. In the order sent, so the first write of an id is the one a
/// `create` keeps and the last the one an `index` keeps, were they to land in that order; [`Versions`] says which
/// others may stand instead.
#[derive(Debug, Default, PartialEq)]
struct Held {
    by_id: BTreeMap<(String, String), Vec<u8>>,
    unnamed: BTreeMap<String, Vec<Vec<u8>>>,
    failure_store: Vec<Vec<u8>>,
}

impl Held {
    fn file(&mut self, policy: DocIdPolicy, index: &str, id: Option<&str>, doc: &[u8]) -> bool {
        match id {
            None => {
                self.unnamed.entry(index.to_string()).or_default().push(doc.to_vec());
                true
            }
            Some(id) => {
                let key = (index.to_string(), id.to_string());
                if policy == DocIdPolicy::Dedupe && self.by_id.contains_key(&key) {
                    return false;
                }
                self.by_id.insert(key, doc.to_vec());
                true
            }
        }
    }

    fn sorted(mut self) -> Held {
        for docs in self.unnamed.values_mut() {
            docs.sort();
        }
        self.unnamed.retain(|_, docs| !docs.is_empty());
        self.failure_store.sort();
        self
    }
}

/// Every document written under an id written more than once. Rows land in no promised order, so any of them may
/// be the one the cluster keeps.
#[derive(Default)]
struct Versions(BTreeMap<(String, String), Vec<Vec<u8>>>);

impl Versions {
    /// `want` with each id written more than once holding what `got` holds under it, where that is one of its versions.
    fn settle(&self, mut want: Held, got: &Held, canonical: impl Fn(&[u8]) -> Vec<u8>) -> Held {
        for (key, docs) in self.0.iter().filter(|(_, docs)| docs.len() > 1) {
            if let Some(doc) = got.by_id.get(key)
                && docs.iter().any(|d| canonical(d) == *doc)
            {
                want.by_id.insert(key.clone(), doc.clone());
            }
        }
        want
    }
}

/// What the cluster holds once `sent` is delivered under `policy`, how many documents it answered a conflict to
/// because their id was already there, and the versions of each id.
fn expected(policy: DocIdPolicy, sent: &[Vec<Sent>]) -> (Held, usize, Versions) {
    let mut held = Held::default();
    let mut conflicts = 0;
    let mut versions = Versions::default();
    for s in sent.iter().flatten() {
        match s.fate {
            Some(Fate::Indexed | Fate::Retried(_)) => {
                if let Some(id) = &s.id {
                    versions
                        .0
                        .entry((s.index.clone(), id.clone()))
                        .or_default()
                        .push(s.row.value.clone());
                }
                if !held.file(policy, &s.index, s.id.as_deref(), &s.row.value) {
                    conflicts += 1;
                }
            }
            Some(Fate::FailureStored) => held.failure_store.push(s.row.value.clone()),
            None | Some(Fate::Refused) => {}
        }
    }
    (held.sorted(), conflicts, versions)
}

// -- the cluster in this process ----------------------------------------------------------------------------------

/// What every request must carry.
#[derive(Default)]
struct Expect {
    path: String,
    /// Another path a request may reach: `url`'s, for one sent while the nodes are still being learned.
    or_path: Option<String>,
    /// The answer to `GET _nodes/http`, or `None` to refuse it as a credential without `monitor` is.
    nodes: Option<String>,
    auth: Option<String>,
    action: &'static str,
    ids: bool,
    max_bulk: usize,
}

/// How the cluster answers, beyond each document's fate.
#[derive(Default)]
struct Answers {
    /// A request carrying more documents than this is refused whole, as one past `http.max_content_length` is.
    split_over: Option<usize>,
    /// The first request carrying this document is answered that the cluster is busy, as a whole. By content, so
    /// which request it is does not turn on the order requests in flight arrive in.
    busy_on: Option<Vec<u8>>,
    /// The first request carrying this document is answered only after the wait.
    slow_on: Option<(Vec<u8>, Duration)>,
    /// Whether the busy answer was given.
    was_busy: bool,
}

/// The cluster's state: what it expects, how it answers, and what it holds.
#[derive(Default)]
struct Cluster {
    expect: Expect,
    answers: Answers,
    policy: DocIdPolicy,
    fates: HashMap<Vec<u8>, Fate>,
    attempts: HashMap<Vec<u8>, u32>,
    held: Held,
    /// What a request carried that it must not have.
    wrong: Vec<String>,
    splits: usize,
    conflicts: usize,
}

impl Cluster {
    fn answer(&mut self, head: &Head, body: &[u8]) -> (u16, Vec<(&'static str, String)>, String) {
        if head.method == "GET" && head.path.contains("/_nodes/http") {
            if head.header("authorization") != self.expect.auth.as_deref() {
                self.wrong.push(format!(
                    "a node lookup with authorization {:?}",
                    head.header("authorization")
                ));
            }
            return match &self.expect.nodes {
                Some(nodes) => (200, Vec::new(), nodes.clone()),
                None => (
                    403,
                    Vec::new(),
                    r#"{"error":{"type":"security_exception"}}"#.to_string(),
                ),
            };
        }
        if head.method != "POST" || (head.path != self.expect.path && self.expect.or_path.as_ref() != Some(&head.path))
        {
            self.wrong
                .push(format!("{} {}, not POST {}", head.method, head.path, self.expect.path));
        }
        if head.header("content-type") != Some("application/x-ndjson") {
            self.wrong
                .push(format!("content type {:?}", head.header("content-type")));
        }
        if head.header("authorization") != self.expect.auth.as_deref() {
            self.wrong.push(format!(
                "authorization {:?}, not {:?}",
                head.header("authorization"),
                self.expect.auth
            ));
        }
        let Some(lines) = body.strip_suffix(b"\n") else {
            self.wrong.push("a bulk body that does not end in a newline".into());
            return (400, Vec::new(), "{}".into());
        };
        let lines: Vec<&[u8]> = lines.split(|b| *b == b'\n').collect();
        if !lines.len().is_multiple_of(2) {
            self.wrong
                .push(format!("{} lines, not action and document pairs", lines.len()));
            return (400, Vec::new(), "{}".into());
        }
        let mut items = Vec::new();
        for pair in lines.chunks(2) {
            let action: Value = match serde_json::from_slice(pair[0]) {
                Ok(v) => v,
                Err(e) => {
                    self.wrong.push(format!("an action line that is not JSON: {e}"));
                    return (400, Vec::new(), "{}".into());
                }
            };
            let Some((name, meta)) = action.as_object().and_then(|o| o.iter().next()) else {
                self.wrong.push(format!("an action that names none: {action}"));
                return (400, Vec::new(), "{}".into());
            };
            if name != self.expect.action {
                self.wrong.push(format!("action {name}, not {}", self.expect.action));
            }
            let index = meta["_index"].as_str().unwrap_or_default().to_string();
            let id = meta.get("_id").and_then(Value::as_str).map(str::to_string);
            if id.is_some() && !self.expect.ids {
                self.wrong
                    .push(format!("an _id under a policy that names none: {action}"));
            }
            items.push((index, id, pair[1]));
        }
        let size: usize = items.iter().map(|(_, _, doc)| doc.len()).sum();
        if items.len() > 1 && size > self.expect.max_bulk {
            self.wrong.push(format!(
                "a request of {size} bytes in {} documents, past the limit of {}",
                items.len(),
                self.expect.max_bulk
            ));
        }

        // Elasticsearch validates every action before it indexes any document, and an id past 512 bytes fails the
        // request whole.
        if items
            .iter()
            .any(|(_, id, _)| id.as_ref().is_some_and(|id| id.len() > 512))
        {
            let body = r#"{"error":{"type":"action_request_validation_exception","reason":"Validation Failed: 1: id is too long, must be no longer than 512 bytes"},"status":400}"#;
            return (400, Vec::new(), body.into());
        }
        if self.answers.split_over.is_some_and(|k| items.len() > k) {
            self.splits += 1;
            let body =
                r#"{"error":{"type":"content_too_long_exception","reason":"request body is too large"},"status":413}"#;
            return (413, Vec::new(), body.into());
        }
        let carries = |doc: &Option<Vec<u8>>| doc.as_ref().is_some_and(|d| items.iter().any(|(_, _, i)| i == d));
        if carries(&self.answers.busy_on) {
            self.answers.busy_on = None;
            self.answers.was_busy = true;
            let body =
                r#"{"error":{"type":"es_rejected_execution_exception","reason":"rejected execution"},"status":429}"#;
            return (429, vec![("Retry-After", "0".to_string())], body.into());
        }

        let mut errors = false;
        let answered: Vec<Value> = items
            .into_iter()
            .map(|(index, id, doc)| {
                let (status, error, stored) = self.one(&index, id.as_deref(), doc);
                errors |= !(200..300).contains(&status);
                let mut result = json!({"_index": index, "status": status});
                if let Some((kind, reason)) = error {
                    result["error"] = json!({"type": kind, "reason": reason});
                }
                if stored {
                    result["failure_store"] = json!("used");
                }
                json!({ self.expect.action: result })
            })
            .collect();
        let body = json!({"took": 1, "errors": errors, "items": answered});
        (200, Vec::new(), body.to_string())
    }

    /// One document's answer: its status, the error it carries, and whether a failure store took it.
    fn one(&mut self, index: &str, id: Option<&str>, doc: &[u8]) -> (u16, Option<(&'static str, &'static str)>, bool) {
        let Some(fate) = self.fates.get(doc).copied() else {
            self.wrong
                .push(format!("a document no row sent: {:?}", String::from_utf8_lossy(doc)));
            return (400, Some(("mapper_parsing_exception", "unknown")), false);
        };
        let attempt = self.attempts.entry(doc.to_vec()).or_default();
        *attempt += 1;
        match fate {
            Fate::Refused => (400, Some(("document_parsing_exception", "failed to parse")), false),
            Fate::Retried(times) if *attempt <= times => {
                let busy = [
                    (
                        429,
                        (
                            "es_rejected_execution_exception",
                            "rejected execution of coordinating operation",
                        ),
                    ),
                    (503, ("unavailable_shards_exception", "primary shard is not active")),
                    (429, ("circuit_breaking_exception", "[parent] Data too large")),
                ];
                let (status, error) = busy[(*attempt as usize + doc.len()) % busy.len()];
                (status, Some(error), false)
            }
            Fate::FailureStored => {
                self.held.failure_store.push(doc.to_vec());
                (201, None, true)
            }
            Fate::Indexed | Fate::Retried(_) => {
                if self.held.file(self.policy, index, id, doc) {
                    (201, None, false)
                } else {
                    self.conflicts += 1;
                    (
                        409,
                        Some(("version_conflict_engine_exception", "document already exists")),
                        false,
                    )
                }
            }
        }
    }
}

/// Whether `body` holds `doc` as a line of its own.
fn contains(body: &[u8], doc: &[u8]) -> bool {
    body.split(|b| *b == b'\n').any(|line| line == doc)
}

/// Answers every request on one connection until the client hangs up.
fn converse(mut s: impl Read + Write, cluster: &Mutex<Cluster>) {
    let mut buf = Vec::new();
    while let Some((head, body)) = read_request(&mut s, &mut buf) {
        let slow = {
            let mut cluster = cluster.lock().unwrap();
            let carried = (cluster.answers.slow_on.as_ref()).is_some_and(|(doc, _)| contains(&body, doc));
            if carried { cluster.answers.slow_on.take() } else { None }
        };
        if let Some((_, wait)) = slow {
            std::thread::sleep(wait);
        }
        let (status, headers, text) = cluster.lock().unwrap().answer(&head, &body);
        let extra: String = headers.iter().map(|(n, v)| format!("{n}: {v}\r\n")).collect();
        // One write: a response in pieces waits out the client's delayed acknowledgement between them.
        let response = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}\r\n{text}",
            text.len()
        );
        let written = s.write_all(response.as_bytes()).and_then(|()| s.flush());
        if written.is_err() {
            return;
        }
    }
}

/// The cluster in this process, listening.
struct Fake {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accept: std::thread::JoinHandle<()>,
    cluster: Arc<Mutex<Cluster>>,
}

impl Fake {
    fn serve(listener: TcpListener, tls: Option<Arc<rustls::ServerConfig>>, cluster: Cluster) -> Fake {
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let cluster = Arc::new(Mutex::new(cluster));
        let (stopping, serving) = (stop.clone(), cluster.clone());
        let accept = std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stopping.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let (tls, cluster) = (tls.clone(), serving.clone());
                std::thread::spawn(move || {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
                    let _ = stream.set_nodelay(true);
                    match tls {
                        None => converse(stream, &cluster),
                        Some(config) => {
                            let Ok(connection) = rustls::ServerConnection::new(config) else {
                                return;
                            };
                            converse(rustls::StreamOwned::new(connection, stream), &cluster);
                        }
                    }
                });
            }
        });
        Fake {
            addr,
            stop,
            accept,
            cluster,
        }
    }

    /// Stops listening, and answers what the cluster ended up holding.
    fn stop(self) -> Cluster {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        let _ = self.accept.join();
        // A connection still open holds the cluster too, so what it holds is taken rather than unwrapped.
        std::mem::take(&mut *self.cluster.lock().unwrap())
    }
}

/// How many documents a failure store took, by the destination's own count.
fn unwritten(registry: &smeltr::metrics::Metrics) -> u64 {
    (registry.snapshot().into_iter())
        .filter(|s| s.key.name == "smeltr_destination_unwritten_total")
        .map(|s| match s.value {
            smeltr::metrics::MetricValue::Counter(n) => n,
            smeltr::metrics::MetricValue::FloatCounter(g) | smeltr::metrics::MetricValue::Gauge(g) => g as u64,
        })
        .sum()
}

fn action(policy: DocIdPolicy) -> &'static str {
    match policy {
        DocIdPolicy::Overwrite => "index",
        DocIdPolicy::Auto | DocIdPolicy::Dedupe => "create",
    }
}

/// The `_nodes/http` answer naming the cluster in this process, at `port`, as its one node: by the host `url`
/// names it by, which its certificate covers.
fn node_list(url: &str, port: u16) -> String {
    let host = if url.contains("://localhost") {
        "localhost/127.0.0.1"
    } else {
        "127.0.0.1"
    };
    json!({"nodes": {"n0": {"roles": ["data"], "attributes": {"zone": "us-east-1a", "rack": "us-east-1b"},
        "http": {"publish_address": format!("{host}:{port}")}}}})
    .to_string()
}

/// The path a request reaches under `url`: its own path, without the slash that ends it, then the bulk API's.
fn bulk_path(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let path = rest.find('/').map_or("", |at| &rest[at..]);
    format!("{}{BULK_PATH}", path.trim_end_matches('/'))
}

// -- the offline fuzzer -------------------------------------------------------------------------------------------

/// Each seed's destination, against the save, the client it builds, and a cluster in this process it delivers to.
#[test]
fn elastic_fuzz() {
    let pki = pki();
    destination::run(0..SEEDS, OPTIONS.iter().copied(), |seed, reached| {
        offline_seed(seed, reached, &pki)
    });
}

fn offline_seed(seed: u64, reached: &mut Reached, pki: &Pki) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let port = listener.local_addr().unwrap().port();
    let index = *rng.pick(&["logs", "logs-2024.01.01", "Upper", "we\"ird\\", "ünïcödé", " spaced "]);
    if index.contains(['"', '\\']) {
        reached.reach("defaultIndex.escaped");
    }
    if index != "logs" {
        reached.reach("defaultIndex");
    }
    let mut draw = draw(&mut rng, reached, Target::Offline(port), index);
    let mangled = draw.broken.is_none() && rng.chance(12);
    reached.only_breaks = mangled || draw.broken.is_some();
    let unknown = mangled
        && destination::mangle(
            &mut rng,
            reached,
            &mut draw.destination,
            &["unknownSetting", "index", "fileIntervalSecs"],
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

    let (secrets, unheld) = secrets_held(&draw, pki);
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let metrics = smeltr::metrics::MetricsHandle::new(registry.clone(), "fuzz");
    let built = smeltr::io::build_destination(&DestinationKind::Elasticsearch(c.clone()), 0, metrics, &secrets, None);
    if let Some(value) = draw.held.as_ref().filter(|v| v.ends_with('\n')) {
        // Refused where it is built, naming the reference and never the value.
        let why = match built {
            Ok(_) => return Err(format!("built with an `auth` value holding a line break\n{json}")),
            Err(e) => e.to_string(),
        };
        if !why.contains("line break or control character") || why.contains(value.trim_end()) {
            return Err(format!(
                "refused an `auth` value holding a line break with: {why}\n{json}"
            ));
        }
        reached.keep_only(&["auth.lineBreak"]);
        return Ok(());
    }
    let mut d = match (built, unheld) {
        (Ok(d), false) => d,
        (Err(e), true) if e.to_string().contains(UNSET_VARIABLE) => {
            reached.keep_only(&["auth.workerLocal"]);
            return Ok(());
        }
        (Ok(_), true) => return Err(format!("built with a credential it does not hold\n{json}")),
        (Err(e), _) => return Err(format!("does not build: {e:?}\n{json}")),
    };
    if draw.endpoint == Endpoint::Elsewhere {
        // Saved, loaded and built, and nothing sent.
        reached.keep_only(&["url.elsewhere", "url.ipv6"]);
        return Ok(());
    }

    let placements = [
        c.default_index.clone(),
        "other".to_string(),
        "logs-2024.01.02".to_string(),
        "we\"ird".to_string(),
        "ünï".to_string(),
        "Upper,Comma".to_string(),
    ];
    // A busy answer and a retried document each wait out a backoff, which a stall of a second or two would not
    // cover once a split sends documents one by one. At 30 seconds every wait a seed can draw fits.
    let throttles = c.stall_after >= Duration::from_secs(30);
    let rows = Rows {
        policy: c.doc_id,
        default_index: &c.default_index,
        max_bulk: c.max_bulk_bytes,
        placements: &placements,
        refused_index: Some("Upper,Comma"),
        record: None,
        retries: throttles && rng.chance(10),
    };
    let sent = batches(&mut rng, reached, &rows);
    let mut answers = Answers::default();
    if rng.chance(5) {
        answers.split_over = Some(1 + rng.below(4));
    }
    let carried: Vec<&Vec<u8>> = (sent.iter().flatten())
        .filter(|s| s.fate.is_some())
        .map(|s| &s.row.value)
        .collect();
    let busy = throttles && !carried.is_empty() && rng.chance(20);
    if busy {
        answers.busy_on = Some(rng.pick(&carried).to_vec());
    }
    let slow = c.stall_after == Duration::from_secs(1) && !carried.is_empty() && rng.chance(8);
    if slow {
        answers.slow_on = Some((rng.pick(&carried).to_vec(), Duration::from_millis(1300)));
    }
    let tls = match draw.tls {
        _ if draw.endpoint == Endpoint::Plain => None,
        Some(ClientTls::Presents) => Some(pki.mutual()),
        _ => Some(pki.server_only()),
    };
    let fates = (sent.iter().flatten())
        .filter_map(|s| s.fate.map(|f| (s.row.value.clone(), f)))
        .collect();
    let fake = Fake::serve(
        listener,
        tls,
        Cluster {
            expect: Expect {
                // A node the cluster names is reached at its own address, under no path.
                path: if draw.sniff == Some(true) {
                    BULK_PATH.to_string()
                } else {
                    bulk_path(&c.url)
                },
                or_path: (draw.sniff == Some(true)).then(|| bulk_path(&c.url)),
                nodes: draw.sniff.filter(|listed| *listed).map(|_| node_list(&c.url, port)),
                auth: draw.auth.clone(),
                action: action(c.doc_id),
                ids: c.doc_id.wants_doc_id(),
                max_bulk: c.max_bulk_bytes,
            },
            answers,
            policy: c.doc_id,
            fates,
            ..Cluster::default()
        },
    );
    let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
    let started = Instant::now();
    let written = destination::write(d.as_mut(), &batches, Duration::from_secs(60));
    let took = started.elapsed();
    drop(d);
    let cluster = fake.stop();
    let fail = |e: String| format!("{e}\n{json}");
    if !cluster.wrong.is_empty() {
        return Err(fail(format!("requests carried:\n  {}", cluster.wrong.join("\n  "))));
    }
    if slow {
        // A cluster answering after the stall is a batch that did not land, never one counted as landed: it fails
        // on the stall's own bound, the request's time running out or the round's, and about when the stall ends.
        return match written {
            Err(e)
                if (e.contains("elasticsearch: request failed: timeout") || e.contains("still undelivered after"))
                    && took < c.stall_after + Duration::from_secs(3) =>
            {
                reached.reach("answer.stall");
                Ok(())
            }
            Err(e) => Err(fail(format!(
                "a cluster slower than the stall failed after {took:?} with: {e}"
            ))),
            Ok(_) => Err(fail("a cluster slower than the stall answered in time".into())),
        };
    }
    let refused = written.map_err(fail)?;
    destination::check_refused(&sent, &refused, Sent::refused).map_err(fail)?;
    let (want, conflicts, versions) = expected(c.doc_id, &sent);
    let got = cluster.held.sorted();
    let want = versions.settle(want, &got, <[u8]>::to_vec);
    if got != want {
        return Err(fail(format!("the cluster holds\n  {got:?}\nnot\n  {want:?}")));
    }
    let failure_stored = want.failure_store.len() as u64;
    if unwritten(&registry) != failure_stored {
        return Err(fail(format!(
            "{} documents counted unwritten, not the {failure_stored} a failure store took",
            unwritten(&registry)
        )));
    }

    let flat = || sent.iter().flatten();
    if flat().any(|s| s.fate == Some(Fate::Refused)) {
        reached.reach("answer.refused");
    }
    if flat().any(|s| matches!(s.fate, Some(Fate::Retried(_)))) {
        reached.reach("answer.retried");
    }
    if failure_stored > 0 {
        reached.reach("answer.failureStore");
    }
    if cluster.conflicts != conflicts {
        return Err(fail(format!(
            "{} conflicts answered, not {conflicts}",
            cluster.conflicts
        )));
    }
    if conflicts > 0 {
        reached.reach("answer.conflict");
    }
    if cluster.splits > 0 {
        reached.reach("answer.split");
    }
    if busy && cluster.answers.was_busy {
        reached.reach("answer.busy");
    }
    Ok(())
}

// -- record types and their mappings ------------------------------------------------------------------------------

/// The names a record's fields take: ECS names a mapping treats apart, dotted ones, and ones a path would escape.
const TOP: &[&str] = &[
    "message",
    "url",
    "error",
    "event",
    "user",
    "host",
    "tags",
    "@timestamp",
    "status",
    "a",
    "a.b",
    "ünï",
    "x y",
];
const NESTED: &[&str] = &[
    "path",
    "original",
    "stack_trace",
    "name",
    "full_name",
    "b",
    "id",
    "when",
];

/// Every type and ECS mapping the mapping fuzzer must reach over its seeds.
const MAPPING_OPTIONS: &[&str] = &[
    "type.bool",
    "type.i64",
    "type.f64",
    "type.datetime",
    "type.duration",
    "type.str",
    "type.bytes",
    "type.array",
    "type.struct",
    "type.null",
    "type.variant",
    "ecs.keyword",
    "ecs.keywordText",
    "ecs.matchOnlyText",
    "ecs.wildcard",
    "ecs.wildcardText",
    "ecs.unindexed",
    "ecs.unindexedLong",
    "ecs.constantKeyword",
    "ecs.ip",
    "ecs.date",
    "ecs.long",
    "ecs.float",
    "ecs.double",
    "ecs.scaledFloat",
    "ecs.boolean",
    "ecs.geoPoint",
    "ecs.flattened",
    "ecs.object",
    "ecs.nested",
    "ecs.disagrees",
];

fn draw_type(rng: &mut Rng, depth: usize) -> DataType {
    match rng.below(if depth >= 2 { 9 } else { 11 }) {
        0 => DataType::Bool,
        1 => DataType::I64,
        2 => DataType::F64,
        3 => DataType::DateTime,
        4 => DataType::Duration,
        5 | 6 => DataType::Str,
        7 => DataType::Bytes,
        8 => {
            if rng.chance(3) {
                DataType::Null
            } else {
                DataType::Variant
            }
        }
        9 => DataType::Array(Box::new(draw_type(rng, depth + 1))),
        _ => draw_struct(rng, depth + 1, NESTED),
    }
}

fn draw_struct(rng: &mut Rng, depth: usize, names: &[&str]) -> DataType {
    let mut chosen = BTreeSet::new();
    for _ in 0..1 + rng.below(5) {
        chosen.insert(*rng.pick(names));
    }
    // A known bug, not drawn: `io::elastic_mapping::mapping` writes dotted names as they are, and Elasticsearch reads
    // `a.b` as `b` under `a`. A record naming both `a` and `a.b` maps one field twice, which the cluster refuses whole
    // when `a` is no object and maps by whichever it reads last when it is.
    if chosen.contains("a") {
        chosen.remove("a.b");
    }
    // An ECS object, so a field under it is one ECS names.
    let parent = ["url", "error", "event", "user", "host"];
    let fields = chosen
        .into_iter()
        .map(|name| {
            let ty = if parent.contains(&name) && depth == 0 && rng.chance(2) {
                draw_struct(rng, depth + 1, NESTED)
            } else {
                draw_type(rng, depth)
            };
            (name.to_string(), ty)
        })
        .collect();
    let mut record = DataType::Struct(fields, rng.chance(2));
    if depth == 0 {
        for _ in 0..rng.below(4) {
            let (path, ecs) = *rng.pick(ECS_TYPED);
            let ty = if rng.chance(2) {
                held_by(rng, ecs)
            } else {
                draw_type(rng, 1)
            };
            put(&mut record, &path.split('.').collect::<Vec<_>>(), ty);
        }
    }
    record
}

/// ECS fields of each type ECS maps a field as, but a string, under names apart from `TOP`'s but for `event`.
const ECS_TYPED: &[(&str, Ecs)] = &[
    ("source.ip", Ecs::Ip),
    ("source.port", Ecs::Long),
    ("source.geo.location", Ecs::GeoPoint),
    ("container.cpu.usage", Ecs::ScaledFloat),
    ("container.security_context.privileged", Ecs::Boolean),
    ("container.labels", Ecs::Object),
    ("data_stream.type", Ecs::ConstantKeyword),
    ("log.syslog.structured_data", Ecs::Flattened),
    ("email.attachments", Ecs::Nested),
    ("tls.server.x509.public_key_exponent", Ecs::UnindexedLong),
    ("gen_ai.request.temperature", Ecs::Double),
    ("event.risk_score", Ecs::Float),
];

/// A type ECS's `ecs` holds.
fn held_by(rng: &mut Rng, ecs: Ecs) -> DataType {
    let number = |rng: &mut Rng| if rng.chance(2) { DataType::I64 } else { DataType::F64 };
    match ecs {
        Ecs::Ip => DataType::Str,
        Ecs::Long | Ecs::UnindexedLong => DataType::I64,
        Ecs::GeoPoint if rng.chance(2) => DataType::Str,
        Ecs::GeoPoint => DataType::Struct(vec![("lat".into(), number(rng)), ("lon".into(), number(rng))], false),
        Ecs::Float | Ecs::Double | Ecs::ScaledFloat => number(rng),
        Ecs::Boolean => DataType::Bool,
        Ecs::Flattened if rng.chance(3) => DataType::Variant,
        Ecs::Nested if rng.chance(2) => DataType::Array(Box::new(draw_struct(rng, 1, NESTED))),
        Ecs::Flattened | Ecs::Object | Ecs::Nested => draw_struct(rng, 1, NESTED),
        _ => DataType::Str,
    }
}

/// `ty` at `keys` under `record`, making each object on the way. A field on the way that is no object is left as
/// it is, and `ty` with it.
fn put(record: &mut DataType, keys: &[&str], ty: DataType) {
    let DataType::Struct(fields, _) = record else {
        return;
    };
    let (key, rest) = keys.split_first().expect("a path has a key");
    let at = match fields.iter().position(|(k, _)| k == key) {
        Some(i) => i,
        None => {
            fields.push((key.to_string(), DataType::Struct(Vec::new(), false)));
            fields.len() - 1
        }
    };
    if rest.is_empty() {
        fields[at].1 = ty;
    } else {
        put(&mut fields[at].1, rest, ty);
    }
}

/// How the product's ECS table (`io::ecs_fields::FIELDS`) maps the field at `path`, where it names it.
fn ecs_at(path: &str) -> Option<Ecs> {
    let fields = smeltr::io::ecs_fields::FIELDS;
    let i = fields.binary_search_by(|(n, _)| (*n).cmp(path)).ok()?;
    Some(fields[i].1)
}

/// Whether ECS's mapping `ecs` holds a value of type `ty` as ECS means it: written out here, apart from the product's.
fn ecs_holds(ecs: Ecs, ty: &DataType) -> bool {
    use DataType as T;
    let number = |t: &T| matches!(t, T::I64 | T::F64);
    match (ecs, ty) {
        (Ecs::Nested, T::Array(e)) => matches!(**e, T::Struct(..)),
        (_, T::Array(_)) => false,
        (
            Ecs::Keyword
            | Ecs::KeywordText
            | Ecs::MatchOnlyText
            | Ecs::Wildcard
            | Ecs::WildcardText
            | Ecs::ConstantKeyword
            | Ecs::Unindexed
            | Ecs::Ip
            | Ecs::GeoPoint,
            T::Str,
        ) => true,
        (Ecs::Long | Ecs::UnindexedLong, T::I64 | T::Duration) => true,
        (Ecs::Float | Ecs::Double | Ecs::ScaledFloat, t) => number(t),
        (Ecs::Date, T::DateTime) | (Ecs::Boolean, T::Bool) => true,
        (Ecs::GeoPoint, T::Struct(f, false)) => {
            f.len() == 2
                && ["lat", "lon"]
                    .iter()
                    .all(|n| f.iter().any(|(k, t)| k == n && number(t)))
        }
        (Ecs::Flattened, T::Struct(..) | T::Variant) | (Ecs::Object | Ecs::Nested, T::Struct(..)) => true,
        _ => false,
    }
}

/// Checks `m` is the mapping of `ty` at `path`, and notes what it reached. `None` is a field left out. Each field ECS
/// maps as a type that does not hold its own is added to `disagree`.
fn check_mapping(
    ty: &DataType,
    m: Option<&Value>,
    path: &str,
    cov: &mut Coverage,
    disagree: &mut Vec<String>,
) -> Result<(), String> {
    let ecs = if path.is_empty() { None } else { ecs_at(path) };
    if let DataType::Array(element) = ty {
        cov.reach("type.array");
        if !(ecs == Some(Ecs::Nested) && ecs_holds(Ecs::Nested, ty)) {
            return check_mapping(element, m, path, cov, disagree);
        }
    }
    match ecs {
        Some(e) if ecs_holds(e, ty) => return check_ecs(e, ty, m, path, cov, disagree),
        Some(_) if !matches!(ty, DataType::Null | DataType::Variant) => {
            cov.reach("ecs.disagrees");
            disagree.push(path.to_string());
        }
        _ => {}
    }
    let scalar = |name: &str| Some(json!({ "type": name }));
    let want = match ty {
        DataType::Null | DataType::Variant => {
            cov.reach(if *ty == DataType::Null {
                "type.null"
            } else {
                "type.variant"
            });
            None
        }
        DataType::Bool => {
            cov.reach("type.bool");
            scalar("boolean")
        }
        DataType::I64 => {
            cov.reach("type.i64");
            scalar("long")
        }
        DataType::Duration => {
            cov.reach("type.duration");
            scalar("long")
        }
        DataType::F64 => {
            cov.reach("type.f64");
            scalar("double")
        }
        DataType::DateTime => {
            cov.reach("type.datetime");
            scalar("date")
        }
        DataType::Bytes => {
            cov.reach("type.bytes");
            scalar("binary")
        }
        DataType::Str => {
            cov.reach("type.str");
            return check_keyword(m, path);
        }
        DataType::Array(_) => unreachable!("an array is its elements"),
        DataType::Struct(..) => {
            cov.reach("type.struct");
            return check_properties(ty, m, path, cov, disagree);
        }
    };
    if m != want.as_ref() {
        return Err(format!("{path}: {ty:?} maps as {m:?}, not {want:?}"));
    }
    Ok(())
}

/// Checks `m` is `{"properties": ...}` of the struct `ty`, its fields each mapped as `check_mapping` says.
fn check_properties(
    ty: &DataType,
    m: Option<&Value>,
    path: &str,
    cov: &mut Coverage,
    disagree: &mut Vec<String>,
) -> Result<(), String> {
    let DataType::Struct(fields, _) = ty else {
        unreachable!("properties are a struct's")
    };
    let Some(properties) = m.and_then(|m| m.get("properties")).and_then(Value::as_object) else {
        return Err(format!("{path}: a struct maps as {m:?}"));
    };
    if m.unwrap().as_object().unwrap().len() != 1 {
        return Err(format!("{path}: a struct maps with more than its properties: {m:?}"));
    }
    for (name, t) in fields {
        let at = if path.is_empty() {
            name.clone()
        } else {
            format!("{path}.{name}")
        };
        check_mapping(t, properties.get(name), &at, cov, disagree)?;
    }
    let known: BTreeSet<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
    if let Some(extra) = properties.keys().find(|k| !known.contains(k.as_str())) {
        return Err(format!("{path}: maps {extra:?}, which is no field of it"));
    }
    Ok(())
}

/// Checks `m` is how ECS maps a field it maps as `ecs`, of a type `ecs` holds.
///
/// What ECS names is read from `io::ecs_fields::FIELDS`, the product's own table, so this checks that a field the
/// table names is mapped as the table says, not that the table is right: the `ecs.*` options record that each of its
/// kinds was reached. [`well_known_ecs_fields_map_as_ecs_says`] checks a few fields against ECS itself.
fn check_ecs(
    ecs: Ecs,
    ty: &DataType,
    m: Option<&Value>,
    path: &str,
    cov: &mut Coverage,
    disagree: &mut Vec<String>,
) -> Result<(), String> {
    let m = m.ok_or_else(|| format!("{path}: a field ECS maps as {ecs:?} left out"))?;
    let text = json!({ "text": { "type": "match_only_text" } });
    let only = |want: Value| {
        if *m == want {
            Ok(())
        } else {
            Err(format!("{path}: ECS's {ecs:?} maps as {m}, not {want}"))
        }
    };
    let (option, checked) = match ecs {
        Ecs::Keyword => ("ecs.keyword", check_keyword(Some(m), path)),
        Ecs::KeywordText => {
            let keyword = check_keyword(Some(m), path);
            (
                "ecs.keywordText",
                keyword.and_then(|_| {
                    only(json!({ "type": "keyword", "ignore_above": m["ignore_above"], "fields": text }))
                }),
            )
        }
        Ecs::Unindexed => (
            "ecs.unindexed",
            only(json!({ "type": "keyword", "index": false, "doc_values": false })),
        ),
        Ecs::UnindexedLong => (
            "ecs.unindexedLong",
            only(json!({ "type": "long", "index": false, "doc_values": false })),
        ),
        Ecs::MatchOnlyText => ("ecs.matchOnlyText", only(json!({ "type": "match_only_text" }))),
        Ecs::Wildcard => ("ecs.wildcard", only(json!({ "type": "wildcard" }))),
        Ecs::WildcardText => ("ecs.wildcardText", only(json!({ "type": "wildcard", "fields": text }))),
        Ecs::ConstantKeyword => ("ecs.constantKeyword", only(json!({ "type": "constant_keyword" }))),
        Ecs::Ip => ("ecs.ip", only(json!({ "type": "ip", "ignore_malformed": true }))),
        Ecs::Date => ("ecs.date", only(json!({ "type": "date" }))),
        Ecs::Long => ("ecs.long", only(json!({ "type": "long" }))),
        Ecs::Float => ("ecs.float", only(json!({ "type": "float" }))),
        Ecs::Double => ("ecs.double", only(json!({ "type": "double" }))),
        Ecs::ScaledFloat => (
            "ecs.scaledFloat",
            only(json!({ "type": "scaled_float", "scaling_factor": 1000 })),
        ),
        Ecs::Boolean => ("ecs.boolean", only(json!({ "type": "boolean" }))),
        Ecs::GeoPoint => (
            "ecs.geoPoint",
            only(json!({ "type": "geo_point", "ignore_malformed": true })),
        ),
        Ecs::Flattened => ("ecs.flattened", only(json!({ "type": "flattened" }))),
        Ecs::Object => ("ecs.object", check_properties(ty, Some(m), path, cov, disagree)),
        Ecs::Nested => {
            let element = match ty {
                DataType::Array(e) => &**e,
                t => t,
            };
            let mut properties = m.clone();
            let nested = properties.as_object_mut().and_then(|o| o.remove("type")) == Some(json!("nested"));
            let checked = if nested {
                check_properties(element, Some(&properties), path, cov, disagree)
            } else {
                Err(format!("{path}: ECS's nested field maps as {m}"))
            };
            ("ecs.nested", checked)
        }
    };
    cov.reach(option);
    checked
}

/// Checks `m` is a keyword that indexes a value only while it stays under the 32766 bytes past which Elasticsearch
/// refuses the whole document.
fn check_keyword(m: Option<&Value>, path: &str) -> Result<(), String> {
    let m = m.ok_or_else(|| format!("{path}: a string left out"))?;
    if m["type"] != "keyword" {
        return Err(format!("{path}: a string maps as {m}, not as a keyword"));
    }
    let above = m["ignore_above"].as_u64().unwrap_or(u64::MAX);
    if above * 4 > 32766 {
        return Err(format!(
            "{path}: a keyword indexes up to {above} characters, past 32766 bytes"
        ));
    }
    Ok(())
}

/// A few fields as the ECS reference gives their mappings, written out here rather than read from the product's
/// table.
#[test]
fn well_known_ecs_fields_map_as_ecs_says() {
    let st = |fields: &[(&str, DataType)]| {
        DataType::Struct(fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect(), false)
    };
    let record = st(&[
        ("@timestamp", DataType::DateTime),
        ("message", DataType::Str),
        ("host", st(&[("name", DataType::Str)])),
        ("user", st(&[("name", DataType::Str)])),
        ("url", st(&[("path", DataType::Str), ("original", DataType::Str)])),
        ("error", st(&[("stack_trace", DataType::Str)])),
        ("event", st(&[("original", DataType::Str)])),
    ]);
    let m = mapping(&record);
    let p = &m["mappings"]["properties"];
    let text = json!({"text": {"type": "match_only_text"}});
    assert_eq!(p["@timestamp"]["type"], "date", "{m}");
    assert_eq!(p["message"]["type"], "match_only_text", "{m}");
    assert_eq!(p["host"]["properties"]["name"]["type"], "keyword", "{m}");
    assert_eq!(p["user"]["properties"]["name"]["type"], "keyword", "{m}");
    assert_eq!(p["user"]["properties"]["name"]["fields"], text, "{m}");
    assert_eq!(p["url"]["properties"]["path"]["type"], "wildcard", "{m}");
    assert_eq!(p["url"]["properties"]["original"]["type"], "wildcard", "{m}");
    assert_eq!(p["url"]["properties"]["original"]["fields"], text, "{m}");
    assert_eq!(p["error"]["properties"]["stack_trace"]["type"], "wildcard", "{m}");
    let original = &p["event"]["properties"]["original"];
    assert_eq!(
        (&original["type"], &original["index"], &original["doc_values"]),
        (&json!("keyword"), &json!(false), &json!(false)),
        "{m}"
    );
}

/// Every record type maps as its fields say.
#[test]
fn elastic_mapping_fuzz() {
    let mut cov = Coverage::new(MAPPING_OPTIONS.iter().copied());
    fuzz::run(0..MAPPING_SEEDS, |seed| {
        let mut rng = Rng::seeded(seed);
        // A record is an object: its type is always a struct.
        let record = draw_struct(&mut rng, 0, TOP);
        let m = mapping(&record);
        let root = m.get("mappings").ok_or_else(|| format!("no mappings: {m}"))?;
        if m.as_object().unwrap().len() != 1 {
            return Err(format!("more than mappings: {m}"));
        }
        let mut disagree = Vec::new();
        check_mapping(&record, Some(root), "", &mut cov, &mut disagree).map_err(|e| format!("{e}\n{record:?}\n{m}"))?;
        let named: Vec<String> = disagreements(&record).iter().map(|d| d.keys.join(".")).collect();
        if named != disagree {
            return Err(format!("disagreements {named:?}, not {disagree:?}\n{record:?}"));
        }
        Ok(())
    });
    cov.assert_complete();
}

/// A JSON document of type `ty`, as the cluster reads one its mapping was made for.
fn document(rng: &mut Rng, ty: &DataType, path: &str) -> Value {
    // A field mapped as ECS maps it takes only what that mapping reads.
    let ecs = if path.is_empty() { None } else { ecs_at(path) }.filter(|e| ecs_holds(*e, ty));
    match (ecs, ty) {
        // A value an address or a point cannot read is kept in the document, left out of the index.
        (Some(Ecs::Ip), _) => return json!(rng.pick(&["10.0.0.1", "::1", "2001:db8::7", "-", "host.example"])),
        (Some(Ecs::GeoPoint), DataType::Str) => return json!(rng.pick(&["41.12,-71.34", "0,0", "unknown"])),
        (Some(Ecs::GeoPoint), _) => return json!({ "lat": 41.5, "lon": -71.25 }),
        // Every document of an index holds the one value its first gave.
        (Some(Ecs::ConstantKeyword), _) => return json!("logs"),
        // A float keeps no number past its range, and a scaled float none past a long's once scaled.
        (Some(Ecs::Float | Ecs::ScaledFloat), _) => return json!(*rng.pick(&[0, 1]) as f64 * 0.25),
        (Some(Ecs::Flattened), DataType::Variant) => return json!({ "k": format!("v{}", rng.below(100)) }),
        _ => {}
    }
    match ty {
        DataType::Null => Value::Null,
        DataType::Bool => json!(rng.chance(2)),
        DataType::I64 | DataType::Duration => json!(*rng.pick(&[0i64, -1, 7, i64::MAX, i64::MIN])),
        DataType::F64 => json!(*rng.pick(&[0.0, -1.5, 1e300, 3.25])),
        DataType::DateTime => json!(rng.pick(&[
            "2024-05-01T12:34:56.789012Z",
            "1969-12-31T23:59:59Z",
            "2038-01-19T03:14:08.001+02:00"
        ])),
        DataType::Str => {
            if rng.chance(20) {
                // Past what a keyword indexes: kept, not indexed.
                json!("é".repeat(9000))
            } else {
                json!(message(rng))
            }
        }
        DataType::Bytes => json!(rng.pick(&["", "aGVsbG8=", "AA==", "/+8="])),
        // Elasticsearch maps a field it was not told of from the first value it sees, so a variant is one kind.
        DataType::Variant => json!(format!("v{}", rng.below(100))),
        DataType::Array(element) => Value::Array((0..rng.below(3)).map(|_| document(rng, element, path)).collect()),
        DataType::Struct(fields, _) => {
            let mut doc = Map::new();
            for (name, t) in fields {
                // A row may leave out a field its record declares.
                if !rng.chance(5) {
                    let at = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{path}.{name}")
                    };
                    doc.insert(name.clone(), document(rng, t, &at));
                }
            }
            Value::Object(doc)
        }
    }
}

// -- the live fuzzer ----------------------------------------------------------------------------------------------

/// What a live draw reaches: every option a cluster with security off and plain HTTP serves.
fn live_options() -> Vec<&'static str> {
    OPTIONS
        .iter()
        .copied()
        .filter(|o| {
            !o.starts_with("broken.")
                && !o.starts_with("url.")
                && !o.starts_with("tls.")
                // One cluster, at one address.
                && !o.starts_with("zoneUrls.")
                && ![
                    "mangled",
                    "auth.workerLocal",
                    // Security is off on the live cluster, so it reads no `Authorization` header.
                    "auth.apiKey",
                    "auth.bareKey",
                    "auth.basic",
                    "auth.userPassword",
                    "auth.lineBreak",
                    "defaultIndex",
                    "defaultIndex.escaped",
                    "answer.retried",
                    "answer.failureStore",
                    "answer.busy",
                    "answer.split",
                    "answer.stall",
                    // The live cluster always names its nodes.
                    "sniff.refused",
                ]
                .contains(o)
        })
        .collect()
}

/// A request to the live cluster, and its status and answer.
fn call(method: &str, url: &str, body: Option<&Value>) -> Result<(u16, Value), String> {
    let agent: ureq::Agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
    let sent = match (method, body) {
        ("PUT", Some(b)) => agent.put(url).send_json(b),
        ("POST", None) => agent.post(url).send_empty(),
        ("GET", None) => agent.get(url).call(),
        ("DELETE", None) => agent.delete(url).call(),
        _ => unreachable!("{method}"),
    };
    let mut response = sent.map_err(|e| format!("{method} {url}: {e}"))?;
    let status = response.status().as_u16();
    let answer = response
        .body_mut()
        .read_json()
        .map_err(|e| format!("{method} {url}: {e}"))?;
    Ok((status, answer))
}

/// What `indices` hold, as `Held`, with an id the cluster made up told from one a row named by `named`.
fn read_back(url: &str, indices: &[String], named: &BTreeSet<(String, String)>) -> Result<Held, String> {
    let mut held = Held::default();
    for index in indices {
        call("POST", &format!("{url}/{index}/_refresh"), None)?;
        let (status, answer) = call("GET", &format!("{url}/{index}/_search?size=10000"), None)?;
        if status != 200 {
            return Err(format!("searching {index}: {status} {answer}"));
        }
        for hit in answer["hits"]["hits"].as_array().into_iter().flatten() {
            let id = hit["_id"].as_str().unwrap_or_default().to_string();
            let doc = canonical(serde_json::to_vec(&hit["_source"]).unwrap().as_slice());
            let key = (index.clone(), id);
            if named.contains(&key) {
                held.by_id.insert(key, doc);
            } else {
                held.unnamed.entry(index.clone()).or_default().push(doc);
            }
        }
    }
    Ok(held.sorted())
}

/// A JSON document's bytes as one parser writes them back, so the cluster's copy compares with the one sent.
fn canonical(doc: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::from_slice::<Value>(doc).unwrap()).unwrap()
}

/// `held` with every document written as [`canonical`] does.
fn canonical_held(held: Held) -> Held {
    Held {
        by_id: held.by_id.into_iter().map(|(k, v)| (k, canonical(&v))).collect(),
        unnamed: (held.unnamed.into_iter())
            .map(|(k, docs)| (k, docs.iter().map(|d| canonical(d)).collect()))
            .collect(),
        failure_store: held.failure_store,
    }
    .sorted()
}

/// A seed's indices, deleted when it ends, whether it passed or failed.
struct Indices<'a> {
    url: &'a str,
    names: &'a [String],
}

impl Drop for Indices<'_> {
    fn drop(&mut self) {
        for index in self.names {
            let _ = call("DELETE", &format!("{}/{index}", self.url), None);
        }
    }
}

/// A name no earlier run used, lowercase as an index's must be.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// Each seed's destination delivers to the cluster at `ES_TEST_URL`, into indices made with the mapping of the
/// record its documents are, and the documents searched back are the ones sent.
#[test]
fn live_elastic_fuzz() {
    let Some(url) = live::elastic("live_elastic_fuzz") else {
        return;
    };
    let url = url.trim_end_matches('/').to_string();
    let pki = pki();
    destination::run(0..LIVE_SEEDS, live_options(), |seed, reached| {
        live_seed(seed, &url, reached, &pki)
    });
}

fn live_seed(seed: u64, url: &str, reached: &mut Reached, pki: &Pki) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let base = unique(&format!("smeltr-fuzz-{seed}"));
    let placements = [
        base.clone(),
        format!("{base}-p0"),
        format!("{base}-p1"),
        format!("{base}-UPPER"),
    ];
    let made = &placements[..3];
    let _cleanup = Indices { url, names: made };
    let draw = draw(&mut rng, reached, Target::Live(url), &base);
    let json = bundle(&draw, pki).to_string();
    let fail = |e: String| format!("{e}\n{json}");
    let c = check_save(&draw, &json)
        .map_err(fail)?
        .expect("a live draw is never broken");

    // The record each document is: drawn, with the number every row carries and one a refused row breaks.
    let DataType::Struct(mut fields, open) = draw_struct(&mut rng, 0, TOP) else {
        unreachable!()
    };
    fields.extend([("n".to_string(), DataType::I64), ("num".to_string(), DataType::I64)]);
    let record = DataType::Struct(fields, open);
    let m = mapping(&record);
    for index in made {
        let (status, answer) = call("PUT", &format!("{url}/{index}"), Some(&m))?;
        if status != 200 {
            return Err(fail(format!(
                "the mapping of {record:?} is refused: {status} {answer}\n{m}"
            )));
        }
    }

    let (secrets, _) = secrets_held(&draw, pki);
    let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
    let mut d = smeltr::io::build_destination(&DestinationKind::Elasticsearch(c.clone()), 0, metrics, &secrets, None)
        .map_err(|e| fail(format!("does not build: {e:?}")))?;
    let rows = Rows {
        policy: c.doc_id,
        default_index: &c.default_index,
        max_bulk: c.max_bulk_bytes,
        placements: &placements,
        refused_index: Some(&placements[3]),
        record: Some(&record),
        retries: false,
    };
    let sent = batches(&mut rng, reached, &rows);
    let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
    let refused = destination::write(d.as_mut(), &batches, Duration::from_secs(300)).map_err(fail)?;
    drop(d);
    if let Err(e) = destination::check_refused(&sent, &refused, Sent::refused) {
        let why: Vec<String> = (sent.iter().flatten())
            .filter(|s| s.refused())
            .map(|s| String::from_utf8_lossy(&s.row.value).chars().take(200).collect())
            .collect();
        return Err(fail(format!("{e}\nrefusing {why:?}\nmapping {m}")));
    }
    let (want, conflicts, versions) = expected(c.doc_id, &sent);
    let want = canonical_held(want);
    let named: BTreeSet<(String, String)> = want.by_id.keys().cloned().collect();
    let got = read_back(url, made, &named)?;
    let want = versions.settle(want, &got, canonical);
    if got != want {
        return Err(fail(format!("the cluster holds\n  {got:?}\nnot\n  {want:?}")));
    }
    if (sent.iter().flatten()).any(|s| s.fate == Some(Fate::Refused)) {
        reached.reach("answer.refused");
    }
    if conflicts > 0 {
        reached.reach("answer.conflict");
    }
    Ok(())
}
