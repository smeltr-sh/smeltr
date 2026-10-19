//! Seeded fuzz of the Kafka source with no broker: its configuration through the loader, and its sources through the
//! shared client registry.
//!
//! A random file of Kafka and Event Hubs sources loads exactly when every source in it is valid and no two read one
//! topic in one group on one cluster. A topic pattern that does not compile, and a pattern among event hubs, is
//! refused naming its source and field. What loads carries what the file said, and its sources attach to a client.
//!
//! A random run of attaches, detaches, topics created and deleted, listings and clusters learned holds after every
//! step:
//!
//! - Each topic goes to at most one source in a group: within one client, and across clients that know the same
//!   cluster id. What a client routes is what its sources that are not refused read.
//! - A refused source routes nothing, and a source that names only patterns is never refused. A source is refused only
//!   over a topic it names outright that another source read before it did: on its own client by name or by pattern,
//!   on another client by name. A pattern on another client lets the topic go.
//! - A source that names a topic is refused on attach exactly when its own client already routes it, or a source on
//!   another client of the cluster names it outright.
//! - After a listing, a topic a pattern matches is read by some source: its own or the one it skipped it for, so a
//!   skipped topic is taken once its reader is gone. A topic deleted from the cluster leaves every source that matched
//!   it.
//!
//! Half the seeds use bare `Routes`, and half go through `Clients` with clients built against unreachable
//! brokers, each holding a source on a topic of its own from the start, so the run never waits on a client's first
//! listing and a client lives until the seed ends. A seed that makes no progress for a minute fails as deadlocked.

use std::collections::BTreeSet;

use serde_json::{Value, json};

use super::*;
use crate::config_file::{FetchBytes, LoadError, load_str};
use crate::format::encoding::Encoding;
use crate::fuzz::{self, Coverage, Rng};
use crate::plan::topology::{KafkaService, SourceKind, StopPosition};

// -- the configuration --------------------------------------------------------

const LITERALS: &[&str] = &["orders", "logs-raw", "__consumer_offsets", "a.b_c-1"];
const BLANKS: &[&str] = &["", "  "];
const GOOD_PATTERNS: &[&str] = &["^orders-.*", "^.*", "^(a|b)-[0-9]+", "^"];
const BAD_PATTERNS: &[&str] = &["^a(", "^[z-a]", "^orders-(", "^*"];
const BROKER_LISTS: &[&str] = &["b1:9092", "b1:9092,b2:9092", "B2:9092", "127.0.0.1:1"];
const EDGES: &[i64] = &[i64::MIN, -2, -1, 0, 1, 1_700_000_000_000, i64::MAX];
/// Each label, and whether it names an encoding.
const ENCODINGS: &[(&str, bool)] = &[
    ("auto", true),
    ("none", true),
    ("utf-8", true),
    ("utf-16le", true),
    ("shift_jis", true),
    ("no-such-encoding", false),
];
/// Each property, and whether a source may set it.
const PROPERTIES: &[(&str, &str, bool)] = &[
    ("client.rack", "r1", false),
    ("group.instance.id", "w1", false),
    ("fetch.min.bytes", "1", true),
    ("fetch.wait.max.ms", "100", true),
    ("group.id", "x", false),
    ("auto.offset.reset", "latest", false),
    ("enable.auto.commit", "true", false),
    ("bootstrap.servers", "x:1", false),
    ("partition.assignment.strategy", "range", false),
    ("fetch.max.bytes", "1", false),
    ("sasl.password", "p", false),
    ("no.such.property", "1", false),
];
/// Each namespace, and its brokers when it is a valid one.
const NAMESPACES: &[(&str, Option<&str>)] = &[
    ("acme", Some("acme.servicebus.windows.net:9093")),
    (
        "acme.servicebus.usgovcloudapi.net",
        Some("acme.servicebus.usgovcloudapi.net:9093"),
    ),
    ("127.0.0.1:5672", Some("127.0.0.1:5672")),
    ("", None),
    ("https://acme", None),
    ("a,b", None),
    ("acme/x", None),
];
const HUBS: &[&str] = &["logs", "metrics", "^logs-.*", "^(", ""];
const CONSUMER_GROUPS: &[&str] = &["$Default", "g", ""];

/// Every option the configuration generator knows.
const CONFIG_OPTIONS: &[&str] = &[
    "kafka",
    "eventHubs",
    "brokers blank",
    "topics empty",
    "topics all blank",
    "topic literal",
    "topic __",
    "topic blank",
    "pattern valid",
    "pattern invalid",
    "groupId absent",
    "groupId set",
    "start absent",
    "start earliest",
    "start latest",
    "start timestamp",
    "start negative",
    "start i64::MAX",
    "stop absent",
    "stop timestamp",
    "fetchBytes absent",
    "fetchBytes below MIN",
    "fetchBytes MIN",
    "fetchBytes MAX",
    "fetchBytes above MAX",
    "fetchBytes 0",
    "fetchBytes u64::MAX",
    "fetchBytes as text",
    "fetchBytes negative",
    "encoding set",
    "encoding unknown",
    "property allowed",
    "property smeltr sets",
    "property refused otherwise",
    "namespace valid",
    "namespace invalid",
    "event hub pattern",
    "event hubs all blank",
    "consumerGroup blank",
    "group overlap",
    "loads",
    "refused",
];

/// One generated source, and what the loader must make of it.
struct Gen {
    id: String,
    json: Value,
    /// The field its topics are under.
    field: &'static str,
    /// Something in it is wrong.
    invalid: bool,
    /// Something in it does not parse, so the loader stops before it validates.
    unparseable: bool,
    bad_patterns: Vec<String>,
    hub_patterns: Vec<String>,
    /// The group and brokers the load-time overlap check compares, and the topics it compares.
    group: String,
    cluster: HashSet<String>,
    topics: Vec<String>,
    /// What it loads as.
    expect: KafkaSourceConfig,
    /// The `fetchBytes` it names, however far out of range, when it is a number of bytes.
    raw_fetch: Option<usize>,
}

fn start(rng: &mut Rng, cov: &mut Coverage) -> (Option<Value>, StartPosition) {
    let v = *rng.pick(EDGES);
    let (json, start) = match rng.below(4) {
        0 => {
            cov.reach("start absent");
            return (None, StartPosition::Earliest);
        }
        1 => {
            cov.reach("start earliest");
            (json!({"type": "earliest"}), StartPosition::Earliest)
        }
        2 => {
            cov.reach("start latest");
            (json!({"type": "latest"}), StartPosition::Latest)
        }
        _ => {
            cov.reach("start timestamp");
            (json!({"type": "timestamp", "millis": v}), StartPosition::Timestamp(v))
        }
    };
    if matches!(start, StartPosition::Timestamp(_)) {
        if v < 0 {
            cov.reach("start negative");
        }
        if v == i64::MAX {
            cov.reach("start i64::MAX");
        }
    }
    (Some(json), start)
}

fn stop(rng: &mut Rng, cov: &mut Coverage) -> (Option<Value>, Option<StopPosition>) {
    let v = *rng.pick(EDGES);
    match rng.below(2) {
        0 => {
            cov.reach("stop absent");
            (None, None)
        }
        _ => {
            cov.reach("stop timestamp");
            (
                Some(json!({"type": "timestamp", "millis": v})),
                Some(StopPosition::Timestamp(v)),
            )
        }
    }
}

/// A `fetchBytes`: its JSON, the bytes it names when it parses, and whether it is in range.
fn fetch_bytes(rng: &mut Rng, cov: &mut Coverage) -> (Option<Value>, Option<usize>, bool) {
    let (min, max) = (FetchBytes::MIN, FetchBytes::MAX);
    let (name, json, bytes) = match rng.below(11) {
        0 => ("fetchBytes absent", None, Some(FetchBytes::DEFAULT)),
        1 => ("fetchBytes below MIN", Some(json!(min - 1)), Some(min - 1)),
        2 => ("fetchBytes MIN", Some(json!(min)), Some(min)),
        3 => ("fetchBytes MAX", Some(json!(max)), Some(max)),
        4 => ("fetchBytes above MAX", Some(json!(max + 1)), Some(max + 1)),
        5 => ("fetchBytes 0", Some(json!(0)), Some(0)),
        6 => ("fetchBytes u64::MAX", Some(json!(u64::MAX)), Some(usize::MAX)),
        7 => ("fetchBytes as text", Some(json!("4 MiB")), Some(min)),
        8 => ("fetchBytes as text", Some(json!("512 MiB")), Some(max)),
        9 => ("fetchBytes as text", Some(json!("1 TiB")), Some(1 << 40)),
        _ => ("fetchBytes negative", Some(json!(-1)), None),
    };
    cov.reach(name);
    let valid = bytes.is_some_and(|b| (min..=max).contains(&b));
    (json, bytes, valid)
}

fn encoding(rng: &mut Rng, cov: &mut Coverage, json: &mut Value) -> (Encoding, bool) {
    if rng.chance(2) {
        return (Encoding::Auto, true);
    }
    let (label, valid) = *rng.pick(ENCODINGS);
    cov.reach(if valid { "encoding set" } else { "encoding unknown" });
    json["encoding"] = json!(label);
    (Encoding::for_label(label).unwrap_or_default(), valid)
}

fn gen_kafka(rng: &mut Rng, cov: &mut Coverage, id: String) -> Gen {
    cov.reach("kafka");
    let mut invalid = false;
    let brokers = if rng.chance(8) {
        cov.reach("brokers blank");
        invalid = true;
        *rng.pick(BLANKS)
    } else {
        *rng.pick(BROKER_LISTS)
    };
    let mut topics = Vec::new();
    let mut bad_patterns = Vec::new();
    for _ in 0..rng.below(4) {
        let topic = match rng.below(10) {
            0..=3 => {
                cov.reach("topic literal");
                *rng.pick(LITERALS)
            }
            4 => {
                cov.reach("topic blank");
                *rng.pick(BLANKS)
            }
            5..=7 => {
                cov.reach("pattern valid");
                *rng.pick(GOOD_PATTERNS)
            }
            _ => {
                cov.reach("pattern invalid");
                let p = *rng.pick(BAD_PATTERNS);
                bad_patterns.push(p.to_string());
                invalid = true;
                p
            }
        };
        if topic.starts_with("__") {
            cov.reach("topic __");
        }
        topics.push(topic.to_string());
    }
    if topics.is_empty() {
        cov.reach("topics empty");
        invalid = true;
    } else if topics.iter().all(|t| t.trim().is_empty()) {
        cov.reach("topics all blank");
        invalid = true;
    }
    let mut json = json!({"id": id, "type": "kafka", "brokers": brokers, "topics": topics});
    let group = if rng.chance(3) {
        cov.reach("groupId absent");
        String::new()
    } else {
        cov.reach("groupId set");
        let g = *rng.pick(&["g", "h"]);
        json["groupId"] = json!(g);
        g.to_string()
    };
    let (start_json, start) = start(rng, cov);
    if let Some(s) = start_json {
        json["start"] = s;
    }
    let (stop_json, stop) = stop(rng, cov);
    if let Some(s) = stop_json {
        json["stop"] = s;
    }
    let (fetch_json, raw_fetch, fetch_valid) = fetch_bytes(rng, cov);
    if let Some(f) = fetch_json {
        json["fetchBytes"] = f;
    }
    invalid |= !fetch_valid;
    let (encoding, encoding_valid) = encoding(rng, cov, &mut json);
    invalid |= !encoding_valid;
    let mut literal = Vec::new();
    if rng.chance(3) {
        let mut properties = serde_json::Map::new();
        for _ in 0..1 + rng.below(2) {
            let (key, value, allowed) = *rng.pick(PROPERTIES);
            cov.reach(if allowed {
                "property allowed"
            } else if RESERVED_BY_SMELTR.contains(&key) {
                "property smeltr sets"
            } else {
                "property refused otherwise"
            });
            invalid |= !allowed;
            if properties.insert(key.to_string(), json!(value)).is_none() {
                literal.push((key.to_string(), value.to_string()));
            }
        }
        // The loader keeps properties in a `BTreeMap`, so they load in key order whatever order they were drawn in.
        literal.sort();
        json["properties"] = Value::Object(properties);
    }
    Gen {
        field: "topics",
        invalid,
        unparseable: raw_fetch.is_none(),
        bad_patterns,
        hub_patterns: Vec::new(),
        cluster: cluster_of(brokers),
        group: group.clone(),
        expect: KafkaSourceConfig {
            service: KafkaService::Kafka,
            encoding,
            fetch_bytes: raw_fetch.unwrap_or_default(),
            brokers: brokers.to_string(),
            topics: topics.clone(),
            group_id: if group.is_empty() { "smeltr".to_string() } else { group },
            identity: None,
            start,
            stop,
            deployed_at: 0,
            member: None,
            rack: None,
            props: crate::plan::topology::KafkaProps {
                literal,
                credentials: Vec::new(),
            },
            avro: None,
        },
        topics,
        raw_fetch,
        id,
        json,
    }
}

/// The properties of `PROPERTIES` smeltr sets itself.
const RESERVED_BY_SMELTR: &[&str] = &[
    "group.id",
    "client.rack",
    "group.instance.id",
    "auto.offset.reset",
    "enable.auto.commit",
    "bootstrap.servers",
    "partition.assignment.strategy",
    "fetch.max.bytes",
];

fn gen_event_hubs(rng: &mut Rng, cov: &mut Coverage, id: String) -> Gen {
    cov.reach("eventHubs");
    let (namespace, brokers) = *rng.pick(NAMESPACES);
    cov.reach(if brokers.is_some() {
        "namespace valid"
    } else {
        "namespace invalid"
    });
    let mut invalid = brokers.is_none();
    let hubs: Vec<String> = (0..1 + rng.below(3)).map(|_| rng.pick(HUBS).to_string()).collect();
    let hub_patterns: Vec<String> = hubs.iter().filter(|h| h.starts_with('^')).cloned().collect();
    if !hub_patterns.is_empty() {
        cov.reach("event hub pattern");
        invalid = true;
    }
    if hubs.iter().all(|h| h.trim().is_empty()) {
        cov.reach("event hubs all blank");
        invalid = true;
    }
    let group = *rng.pick(CONSUMER_GROUPS);
    if group.is_empty() {
        cov.reach("consumerGroup blank");
        invalid = true;
    }
    let mut json = json!({
        "id": id, "type": "eventHubs", "namespace": namespace, "eventHubs": hubs,
        "consumerGroup": group, "connectionString": "EVENTHUBS_CONNECTION_STRING",
    });
    let (start_json, start) = start(rng, cov);
    if let Some(s) = start_json {
        json["start"] = s;
    }
    let (stop_json, stop) = stop(rng, cov);
    if let Some(s) = stop_json {
        json["stop"] = s;
    }
    let (fetch_json, raw_fetch, fetch_valid) = fetch_bytes(rng, cov);
    if let Some(f) = fetch_json {
        json["fetchBytes"] = f;
    }
    invalid |= !fetch_valid;
    let (encoding, encoding_valid) = encoding(rng, cov, &mut json);
    invalid |= !encoding_valid;
    let brokers = brokers.unwrap_or_default();
    Gen {
        field: "eventHubs",
        invalid,
        unparseable: raw_fetch.is_none(),
        bad_patterns: Vec::new(),
        hub_patterns,
        cluster: cluster_of(brokers),
        group: group.to_string(),
        expect: KafkaSourceConfig {
            service: KafkaService::EventHubs,
            encoding,
            fetch_bytes: raw_fetch.unwrap_or_default(),
            brokers: brokers.to_string(),
            topics: hubs.clone(),
            group_id: group.to_string(),
            identity: None,
            start,
            stop,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        topics: hubs,
        raw_fetch,
        id,
        json,
    }
}

/// The brokers a list names, as the load-time overlap check compares them: two lists that share one name one cluster.
fn cluster_of(brokers: &str) -> HashSet<String> {
    brokers
        .split(',')
        .map(|b| b.trim().to_ascii_lowercase())
        .filter(|b| !b.is_empty())
        .collect()
}

/// Whether two of `gens` read one topic in one group on brokers they share. A blank group is left to the worker group's
/// merge, so it overlaps nothing at load.
fn overlap(gens: &[Gen]) -> bool {
    /// A source reading a topic: its brokers, and its id.
    type Reader<'a> = (&'a HashSet<String>, &'a str);
    let mut readers: HashMap<(&str, &str), Vec<Reader<'_>>> = HashMap::new();
    let mut found = false;
    for g in gens.iter().filter(|g| !g.group.trim().is_empty()) {
        for topic in g.topics.iter().filter(|t| !t.trim().is_empty()) {
            let seen = readers.entry((g.group.as_str(), topic.trim())).or_default();
            found |= seen
                .iter()
                .any(|(other, id)| *id != g.id && !other.is_disjoint(&g.cluster));
            seen.push((&g.cluster, &g.id));
        }
    }
    found
}

fn check_config(seed: u64, cov: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let gens: Vec<Gen> = (0..1 + rng.below(3))
        .map(|i| {
            let id = format!("s{i}");
            if rng.chance(3) {
                gen_event_hubs(&mut rng, cov, id)
            } else {
                gen_kafka(&mut rng, cov, id)
            }
        })
        .collect();
    let overlaps = overlap(&gens);
    if overlaps {
        cov.reach("group overlap");
    }
    let valid = !overlaps && gens.iter().all(|g| !g.invalid);
    let text = json!({"version": 1, "sources": gens.iter().map(|g| &g.json).collect::<Vec<_>>()}).to_string();
    let loaded = load_str(&text);
    let shown = || serde_json::to_string_pretty(&serde_json::from_str::<Value>(&text).unwrap()).unwrap();

    // Out-of-range fetch sizes reach the client's own arithmetic too, as the loader would never pass them.
    for g in &gens {
        if let Some(fetch_bytes) = g.raw_fetch {
            let config = KafkaSourceConfig {
                fetch_bytes,
                ..g.expect.clone()
            };
            let _ = client_config(&config, &Default::default());
            let _ = Slot::new(&config, 0);
        }
    }

    match (&loaded, valid) {
        (Ok(_), true) => cov.reach("loads"),
        (Err(_), false) => cov.reach("refused"),
        (Ok(_), false) => return Err(format!("this loads, and the generator made it invalid:\n{}", shown())),
        (Err(e), true) => {
            return Err(format!(
                "this is refused, and the generator made it valid: {e}\n{}",
                shown()
            ));
        }
    }
    if let Err(e) = &loaded
        && !gens.iter().any(|g| g.unparseable)
    {
        let LoadError::Validation(errors) = e else {
            return Err(format!("refused, and not by validation: {e}\n{}", shown()));
        };
        let said = |g: &Gen, words: &str, what: &str| {
            errors.iter().any(|e| {
                e.item.as_ref().is_some_and(|i| i.id == g.id)
                    && e.field == Some(g.field)
                    && e.message.contains(words)
                    && e.message.contains(&format!("{what:?}"))
            })
        };
        for g in &gens {
            for p in &g.bad_patterns {
                if !said(g, "does not compile", p) {
                    return Err(format!(
                        "{:?}'s pattern {p:?} is not refused: {errors:?}\n{}",
                        g.id,
                        shown()
                    ));
                }
            }
            for p in &g.hub_patterns {
                if !said(g, "named in full", p) {
                    return Err(format!(
                        "{:?}'s event hub {p:?} is not refused: {errors:?}\n{}",
                        g.id,
                        shown()
                    ));
                }
            }
        }
    }
    let Ok(system) = loaded else {
        return Ok(());
    };

    let mut world = World::bare();
    for g in &gens {
        let Some(loaded) = system.topology.sources.iter().find(|s| s.name == g.id) else {
            return Err(format!("{:?} is not loaded\n{}", g.id, shown()));
        };
        let SourceKind::Kafka(k) = &loaded.kind else {
            return Err(format!("{:?} loads as another kind\n{}", g.id, shown()));
        };
        let e = &g.expect;
        let same = k.service == e.service
            && k.brokers == e.brokers
            && k.topics == e.topics
            && k.group_id == e.group_id
            && k.start == e.start
            && k.stop == e.stop
            && k.fetch_bytes == e.fetch_bytes
            && k.encoding == e.encoding
            && (e.service == KafkaService::EventHubs || k.props.literal == e.props.literal);
        if !same {
            return Err(format!("{:?} loads as\n{k:?}\nnot\n{e:?}\n{}", g.id, shown()));
        }
        if k.service == KafkaService::Kafka
            && let Err(e) = client_config(k, &Default::default())
        {
            return Err(format!("{:?} loads and its client cannot be configured: {e}", g.id));
        }
        // What loads attaches: one client per group and brokers, as `Clients` keys them.
        let node = match world
            .nodes
            .iter()
            .position(|n| n.routes.0.group == k.group_id && n.routes.0.brokers == k.brokers)
        {
            Some(n) => n,
            None => world.add_bare_node(&k.group_id, &k.brokers, "c1"),
        };
        world.attach_bare(node, k)?;
    }
    let listed: BTreeSet<String> = gens
        .iter()
        .flat_map(|g| g.topics.iter())
        .filter(|t| topic_pattern(t).is_none())
        .cloned()
        .chain(["orders-eu".to_string(), "a-1".to_string()])
        .collect();
    world.topics.insert("c1", listed);
    for n in 0..world.nodes.len() {
        world.list(n, &mut rng)?;
    }
    Ok(())
}

#[test]
fn random_kafka_sources_load_exactly_when_valid_and_attach() {
    let mut cov = Coverage::new(CONFIG_OPTIONS.iter().copied());
    fuzz::run(0..1000, |seed| check_config(seed, &mut cov));
    cov.assert_complete();
}

// -- the registry ---------------------------------------------------------------

const TOPICS: &[&str] = &["a-1", "a-2", "b-1", "b-2", "c"];
const PATTERNS: &[&str] = &["^a-.*", "^.*-1", "^b-.*", "^.*", "^c", "^a-1"];
const UNCOMPILED: &[&str] = &["^a(", "^[b-a]"];
const GROUPS: &[&str] = &["g", "h"];
const BROKERS: &[&str] = &["127.0.0.1:1", "127.0.0.1:2"];

/// Every operation and outcome the registry run knows.
const REGISTRY_OPTIONS: &[&str] = &[
    "bare routes",
    "clients",
    "two clusters",
    "attach literal",
    "attach pattern",
    "attach literal and pattern",
    "attach uncompiled pattern",
    "attach to a shared client",
    "attach refused",
    "attach refused across clients",
    "detach",
    "topic created",
    "topic deleted",
    "list",
    "note cluster",
    "pattern skips",
    "pattern skips across clients",
    "skipped topic taken",
    "deleted topic leaves",
    "refused when a cluster is learned",
    "pattern lets a topic go across clients",
];

/// A client: bare `Routes`, or one `Clients` built, kept alive by `client`.
struct Node {
    routes: Routes,
    client: Option<Arc<Client>>,
    group: String,
    brokers: String,
    /// The cluster its brokers are, which it learns only when told.
    cluster: &'static str,
}

/// A source attached, and what it was after the last step.
struct Source {
    slot: Arc<Slot>,
    node: usize,
    refused: bool,
    matched: HashSet<String>,
    skipped: HashSet<String>,
    /// The step since which it has read each topic it reads.
    since: HashMap<String, usize>,
}

struct World {
    clients: Option<Arc<Clients>>,
    nodes: Vec<Node>,
    sources: Vec<Source>,
    /// The topics on each cluster.
    topics: HashMap<&'static str, BTreeSet<String>>,
    /// The next attach order of a bare source.
    seq: u64,
    step: usize,
    log: Vec<String>,
    reached: Vec<&'static str>,
}

fn source_config(brokers: &str, group: &str, topics: &[String]) -> KafkaSourceConfig {
    KafkaSourceConfig {
        service: KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: FetchBytes::DEFAULT,
        brokers: brokers.to_string(),
        topics: topics.to_vec(),
        group_id: group.to_string(),
        identity: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    }
}

fn names(slot: &Slot) -> HashSet<String> {
    slot.names().iter().map(|t| t.to_string()).collect()
}

fn refusal(slot: &Slot) -> Option<String> {
    slot.lock().refused.clone()
}

impl World {
    fn bare() -> World {
        World {
            clients: None,
            nodes: Vec::new(),
            sources: Vec::new(),
            topics: HashMap::new(),
            seq: 0,
            step: 0,
            log: Vec::new(),
            reached: Vec::new(),
        }
    }

    fn add_bare_node(&mut self, group: &str, brokers: &str, cluster: &'static str) -> usize {
        let routes = Routes(Arc::new(RoutesInner {
            group: group.to_string(),
            brokers: brokers.to_string(),
            ..RoutesInner::default()
        }));
        self.nodes.push(Node {
            routes,
            client: None,
            group: group.to_string(),
            brokers: brokers.to_string(),
            cluster,
        });
        self.nodes.len() - 1
    }

    /// A world of `Clients`, with one client for each of `keys`, each holding a source on a topic no listing holds.
    fn with_clients(keys: &[(&'static str, &'static str)], clusters: [&'static str; 2]) -> Result<World, String> {
        let clients = Arc::new(Clients::default());
        let mut world = World {
            clients: Some(Arc::clone(&clients)),
            ..World::bare()
        };
        for (i, &(brokers, group)) in keys.iter().enumerate() {
            let config = source_config(brokers, group, &[format!("own-{i}")]);
            let (client, slot) = clients
                .attach(&config, &Default::default())
                .map_err(|e| format!("a client did not start: {e}"))?;
            world.nodes.push(Node {
                routes: client.routes.clone(),
                client: Some(client),
                group: group.to_string(),
                brokers: brokers.to_string(),
                cluster: clusters[BROKERS.iter().position(|b| *b == brokers).unwrap()],
            });
            world.sources.push(Source {
                slot,
                node: i,
                refused: false,
                matched: HashSet::new(),
                skipped: HashSet::new(),
                since: HashMap::new(),
            });
        }
        world.check(None)?;
        Ok(world)
    }

    fn reach(&mut self, name: &'static str) {
        self.reached.push(name);
    }

    fn known(&self, n: usize) -> Option<&str> {
        self.nodes[n].routes.0.cluster.get().map(String::as_str)
    }

    /// Whether a source on `a` and one on `b` may not both read a topic, as far as the clients know.
    fn peers(&self, a: usize, b: usize) -> bool {
        a == b
            || (self.nodes[a].routes.0.group == self.nodes[b].routes.0.group
                && self.known(a).is_some()
                && self.known(a) == self.known(b))
    }

    /// Every topic read through a client other than `n` that `n`'s sources may not read.
    fn elsewhere(&self, n: usize) -> HashSet<String> {
        self.read_elsewhere(n).flat_map(|s| names(&s.slot)).collect()
    }

    /// Every topic named outright through a client other than `n` that `n`'s sources may not read.
    fn named_elsewhere(&self, n: usize) -> HashSet<String> {
        self.read_elsewhere(n)
            .flat_map(|s| s.slot.topics.iter().map(|t| t.to_string()))
            .collect()
    }

    fn read_elsewhere(&self, n: usize) -> impl Iterator<Item = &Source> {
        self.sources
            .iter()
            .filter(move |s| s.node != n && self.peers(n, s.node) && refusal(&s.slot).is_none())
    }

    fn by_topic(&self, n: usize) -> HashMap<String, Arc<Slot>> {
        self.nodes[n].routes.0.by_topic.lock().unwrap().clone()
    }

    /// Attaches a source on `topics` through node `n`, and checks it is refused exactly when it names a topic its own
    /// client routes, or a source on another client of the cluster names.
    fn attach(&mut self, n: usize, topics: Vec<String>) -> Result<(), String> {
        let literals: Vec<&String> = topics.iter().filter(|t| topic_pattern(t).is_none()).collect();
        let uncompiled = topics.iter().any(|t| matches!(topic_pattern(t), Some(Err(_))));
        let taken = self.by_topic(n);
        let elsewhere = self.named_elsewhere(n);
        let refused_here = literals.iter().any(|t| taken.contains_key(t.as_str()));
        let expect_refused = refused_here || literals.iter().any(|t| elsewhere.contains(t.as_str()));
        let patterns = literals.len() < topics.len();
        self.reach(match (literals.is_empty(), patterns) {
            (false, false) => "attach literal",
            (true, _) => "attach pattern",
            (false, true) => "attach literal and pattern",
        });
        let routes = self.nodes[n].routes.clone();
        let config = source_config(&self.nodes[n].brokers, &self.nodes[n].group, &topics);
        let slot = match self.clients.clone() {
            Some(clients) => match clients.attach(&config, &Default::default()) {
                Err(_) if uncompiled => {
                    self.reach("attach uncompiled pattern");
                    return Ok(());
                }
                Err(e) => return Err(format!("attaching {topics:?} failed: {e}")),
                Ok(_) if uncompiled => return Err(format!("{topics:?} attached with a pattern that does not compile")),
                Ok((client, slot)) => {
                    if !Arc::ptr_eq(&client.routes.0, &routes.0) {
                        return Err("it attached to another client than its configuration names".to_string());
                    }
                    if self.sources.iter().any(|s| s.node == n) {
                        self.reach("attach to a shared client");
                    }
                    slot
                }
            },
            None => {
                let slot = Arc::new(Slot::new(&config, self.seq));
                self.seq += 1;
                routes.attach(&slot);
                slot
            }
        };
        let refused = refusal(&slot).is_some();
        if refused != expect_refused {
            return Err(format!(
                "a source on {topics:?} is {}refused; this client routes {:?}, and another client names {:?}",
                if refused { "" } else { "not " },
                taken.keys().collect::<BTreeSet<_>>(),
                elsewhere,
            ));
        }
        if refused {
            self.reach(if refused_here {
                "attach refused"
            } else {
                "attach refused across clients"
            });
        }
        self.sources.push(Source {
            slot,
            node: n,
            refused: false,
            matched: HashSet::new(),
            skipped: HashSet::new(),
            since: HashMap::new(),
        });
        Ok(())
    }

    /// `attach` for a configuration that loaded, on bare routes.
    fn attach_bare(&mut self, n: usize, config: &KafkaSourceConfig) -> Result<(), String> {
        let slot = Arc::new(Slot::new(config, self.seq));
        self.seq += 1;
        self.nodes[n].routes.attach(&slot);
        self.sources.push(Source {
            slot,
            node: n,
            refused: false,
            matched: HashSet::new(),
            skipped: HashSet::new(),
            since: HashMap::new(),
        });
        self.check(None)
    }

    fn detach(&mut self, i: usize) {
        let s = self.sources.remove(i);
        match &self.nodes[s.node].client {
            Some(client) => client.detach(&s.slot),
            None => self.nodes[s.node].routes.detach(&s.slot),
        }
    }

    /// Lists node `n`'s cluster as `list_topics` does, and checks what a listing must leave.
    fn list(&mut self, n: usize, rng: &mut Rng) -> Result<(), String> {
        let mut listed: Vec<String> = self
            .topics
            .get(self.nodes[n].cluster)
            .into_iter()
            .flatten()
            .cloned()
            .collect();
        for i in (1..listed.len()).rev() {
            listed.swap(i, rng.below(i + 1));
        }
        self.step += 1;
        self.log.push(format!("{}: list node {n}: {listed:?}", self.step));
        let routes = &self.nodes[n].routes;
        let found = match &self.clients {
            Some(clients) => {
                let elsewhere = clients.read_elsewhere(routes);
                let found = routes.match_topics(&listed, &elsewhere);
                if found && let Some(cluster) = routes.0.cluster.get() {
                    clients.note_cluster(routes, cluster.clone());
                }
                found
            }
            None => routes.match_topics(&listed, &HashSet::new()),
        };
        let grew = self.sources.iter().any(|s| {
            s.node == n
                && s.slot
                    .lock()
                    .matched
                    .iter()
                    .any(|t| !s.matched.contains(t.as_ref()) && !s.refused)
        });
        // A topic matched and then settled away in one listing grows nothing, and the listing still found it.
        if grew && !found {
            return Err("a listing handed a topic over and answered that it found none".to_string());
        }
        self.reach("list");
        self.check(Some((n, &listed)))
    }

    /// Every invariant, after a step. `listed` is the node just listed and what its cluster holds.
    fn check(&mut self, listed: Option<(usize, &[String])>) -> Result<(), String> {
        let step = self.step;
        let now_refused: Vec<Option<String>> = self.sources.iter().map(|s| refusal(&s.slot)).collect();
        let reads = |i: usize| now_refused[i].is_none();
        let now_names: Vec<HashSet<String>> = self.sources.iter().map(|s| names(&s.slot)).collect();

        for n in 0..self.nodes.len() {
            let attached = self.nodes[n].routes.slots();
            let mine: Vec<usize> = (0..self.sources.len()).filter(|&i| self.sources[i].node == n).collect();
            if attached.len() != mine.len()
                || !mine
                    .iter()
                    .all(|&i| attached.iter().any(|s| Arc::ptr_eq(s, &self.sources[i].slot)))
            {
                return Err(format!("node {n} holds other sources than were attached to it"));
            }
            for (topic, owner) in self.by_topic(n) {
                let Some(&i) = mine.iter().find(|&&i| Arc::ptr_eq(&self.sources[i].slot, &owner)) else {
                    return Err(format!("node {n} routes {topic:?} to a source not attached to it"));
                };
                if !reads(i) {
                    return Err(format!("node {n} routes {topic:?} to source {i}, which is refused"));
                }
                if !now_names[i].contains(&topic) {
                    return Err(format!(
                        "node {n} routes {topic:?} to source {i}, which does not read it"
                    ));
                }
            }
            for &i in mine.iter().filter(|&&i| reads(i)) {
                for topic in &now_names[i] {
                    if !self.nodes[n]
                        .routes
                        .slot_for(topic)
                        .is_some_and(|s| Arc::ptr_eq(&s, &self.sources[i].slot))
                    {
                        return Err(format!(
                            "source {i} reads {topic:?} and node {n} does not route it there"
                        ));
                    }
                }
            }
        }

        for a in 0..self.sources.len() {
            for b in a + 1..self.sources.len() {
                if !reads(a) || !reads(b) || !self.peers(self.sources[a].node, self.sources[b].node) {
                    continue;
                }
                if let Some(t) = now_names[a].intersection(&now_names[b]).next() {
                    return Err(format!("sources {a} and {b} both read {t:?} in one group"));
                }
            }
        }

        let since = |i: usize, t: &str| self.sources[i].since.get(t).copied().unwrap_or(step);
        for (i, s) in self.sources.iter().enumerate() {
            match (s.refused, &now_refused[i]) {
                (true, None) => return Err(format!("source {i} was refused and no longer is")),
                (false, Some(why)) => {
                    if s.slot.topics.is_empty() {
                        return Err(format!("source {i} names only patterns and is refused: {why}"));
                    }
                    let Some(topic) = s.slot.topics.iter().find(|t| why.contains(&format!("topic {t:?}"))) else {
                        return Err(format!("source {i} is refused over none of the topics it names: {why}"));
                    };
                    let first = (0..self.sources.len()).find(|&o| {
                        o != i
                            && reads(o)
                            && self.peers(s.node, self.sources[o].node)
                            && now_names[o].contains(topic.as_ref())
                            && (self.sources[o].node == s.node || self.sources[o].slot.topics.contains(topic))
                            && since(o, topic) <= since(i, topic)
                    });
                    if first.is_none() {
                        return Err(format!(
                            "source {i} is refused over {topic:?}, which no source read before it, on its client or \
                             by name on another: {why}"
                        ));
                    }
                }
                _ => {}
            }
        }

        if let Some((n, listed)) = listed {
            let present: HashSet<&str> = listed.iter().map(String::as_str).collect();
            let by_topic = self.by_topic(n);
            let elsewhere = self.elsewhere(n);
            for (i, s) in self.sources.iter().enumerate().filter(|(_, s)| s.node == n) {
                let state = s.slot.lock();
                if let Some(t) = state
                    .matched
                    .iter()
                    .chain(&state.skipped)
                    .find(|t| !present.contains(t.as_ref()))
                {
                    return Err(format!("source {i} still holds {t:?}, which the cluster no longer has"));
                }
                drop(state);
                if !reads(i) {
                    continue;
                }
                for t in listed.iter().filter(|t| s.slot.matches(t)) {
                    if !by_topic.contains_key(t) && !elsewhere.contains(t) {
                        return Err(format!("source {i}'s pattern matches {t:?}, and no source reads it"));
                    }
                }
                for t in s.slot.lock().skipped.iter() {
                    let here = by_topic.get(t.as_ref()).is_some_and(|o| !Arc::ptr_eq(o, &s.slot));
                    if !here && !elsewhere.contains(t.as_ref()) {
                        return Err(format!("source {i} skips {t:?}, and no other source reads it"));
                    }
                }
            }
            for (topic, owner) in &by_topic {
                if !present.contains(topic.as_str()) && !owner.topics.iter().any(|t| t.as_ref() == topic) {
                    return Err(format!(
                        "node {n} still routes {topic:?}, which the cluster no longer has"
                    ));
                }
            }
        }

        let mut reached = Vec::new();
        for (i, s) in self.sources.iter_mut().enumerate() {
            let state = s.slot.lock();
            let matched: HashSet<String> = state.matched.iter().map(|t| t.to_string()).collect();
            let skipped: HashSet<String> = state.skipped.iter().map(|t| t.to_string()).collect();
            drop(state);
            if listed.is_none() && s.matched.iter().any(|t| skipped.contains(t)) {
                reached.push("pattern lets a topic go across clients");
            }
            if s.skipped.iter().any(|t| matched.contains(t)) {
                reached.push("skipped topic taken");
            }
            if let Some((n, listed)) = listed
                && n == s.node
                && s.matched.iter().any(|t| !listed.contains(t))
            {
                reached.push("deleted topic leaves");
            }
            if now_refused[i].is_none() && !skipped.is_empty() {
                reached.push("pattern skips");
                let routes = &self.nodes[s.node].routes;
                if skipped.iter().any(|t| routes.slot_for(t).is_none()) {
                    reached.push("pattern skips across clients");
                }
            }
            s.refused = now_refused[i].is_some();
            s.matched = matched;
            s.skipped = skipped;
            if !s.refused {
                s.since.retain(|t, _| now_names[i].contains(t));
                for t in &now_names[i] {
                    s.since.entry(t.clone()).or_insert(step);
                }
            }
        }
        self.reached.extend(reached);
        Ok(())
    }
}

/// Worlds of clients waiting to be dropped, each on a thread of its own: dropping a client waits out its thread's
/// poll, and one whose sampler first ran after a pattern source attached waits out a listing of the unreachable
/// cluster.
#[derive(Default)]
struct Graveyard(Vec<JoinHandle<()>>);

impl Graveyard {
    fn bury(&mut self, world: World) {
        if world.clients.is_none() {
            return;
        }
        self.collect();
        if self.0.len() >= 64 {
            self.0.remove(0).join().expect("a client panicked as it was dropped");
        }
        self.0.push(std::thread::spawn(move || drop(world)));
    }

    /// Joins every world dropped by now.
    fn collect(&mut self) {
        let (done, waiting) = std::mem::take(&mut self.0).into_iter().partition(|t| t.is_finished());
        self.0 = waiting;
        for t in done {
            t.join().expect("a client panicked as it was dropped");
        }
    }

    /// Joins every world, failing if one takes longer than [`TEARDOWN`] to drop: each waits at most one poll and one
    /// listing, so a longer wait is a client that hangs as it is dropped.
    fn finish(self) {
        let deadline = Instant::now() + TEARDOWN;
        while !self.0.iter().all(JoinHandle::is_finished) {
            assert!(
                Instant::now() < deadline,
                "a client took over {TEARDOWN:?} to drop: it hangs as it is dropped"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        for t in self.0 {
            t.join().expect("a client panicked as it was dropped");
        }
    }
}

fn run_registry(seed: u64, beat: &AtomicU64, graveyard: &mut Graveyard) -> Result<Vec<&'static str>, String> {
    let mut rng = Rng::seeded(seed);
    let mut world = if seed.is_multiple_of(2) {
        let clusters = if rng.chance(3) { ["c1", "c2"] } else { ["c1", "c1"] };
        let mut keys: Vec<(&str, &str)> = Vec::new();
        for _ in 0..2 + rng.below(2) {
            let key = (*rng.pick(BROKERS), *rng.pick(GROUPS));
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        let mut world = World::with_clients(&keys, clusters)?;
        world.reach("clients");
        if clusters[0] != clusters[1]
            && keys.iter().any(|k| k.0 == BROKERS[0])
            && keys.iter().any(|k| k.0 == BROKERS[1])
        {
            world.reach("two clusters");
        }
        world
    } else {
        let mut world = World::bare();
        for _ in 0..1 + rng.below(2) {
            world.add_bare_node(rng.pick(GROUPS), rng.pick(BROKERS), "c1");
        }
        world.reach("bare routes");
        world
    };
    for cluster in ["c1", "c2"] {
        let topics = TOPICS.iter().filter(|_| rng.chance(2)).map(|t| t.to_string()).collect();
        world.topics.insert(cluster, topics);
    }
    let steps = if world.clients.is_some() { 30 } else { 40 };
    let result: Result<(), String> = (|| {
        for _ in 0..steps {
            beat.fetch_add(1, Ordering::Relaxed);
            registry_step(&mut world, &mut rng)?;
        }
        Ok(())
    })();
    let reached = std::mem::take(&mut world.reached);
    let log = std::mem::take(&mut world.log);
    graveyard.bury(world);
    match result {
        Ok(()) => Ok(reached),
        Err(why) => Err(format!("{why}\nafter:\n  {}", log.join("\n  "))),
    }
}

fn registry_step(world: &mut World, rng: &mut Rng) -> Result<(), String> {
    let n = rng.below(world.nodes.len());
    match rng.below(20) {
        0..=6 => {
            let mut topics = Vec::new();
            for _ in 0..1 + rng.below(3) {
                topics.push(match rng.below(20) {
                    0..=10 => rng.pick(TOPICS).to_string(),
                    11 if world.clients.is_some() => rng.pick(UNCOMPILED).to_string(),
                    _ => rng.pick(PATTERNS).to_string(),
                });
            }
            world.step += 1;
            world.log.push(format!("{}: attach {topics:?} to node {n}", world.step));
            world.attach(n, topics)?;
            world.check(None)
        }
        7..=9 if !world.sources.is_empty() => {
            let i = rng.below(world.sources.len());
            world.step += 1;
            world.log.push(format!("{}: detach source {i}", world.step));
            world.detach(i);
            world.reach("detach");
            world.check(None)
        }
        10..=12 => {
            let cluster = *rng.pick(&["c1", "c2"]);
            let topic = rng.pick(TOPICS).to_string();
            let topics = world.topics.entry(cluster).or_default();
            world.step += 1;
            let created = topics.insert(topic.clone());
            if !created {
                topics.remove(&topic);
            }
            let what = if created { "topic created" } else { "topic deleted" };
            world.log.push(format!("{}: {what} {topic:?} on {cluster}", world.step));
            world.reach(what);
            Ok(())
        }
        13..=17 => world.list(n, rng),
        _ => {
            let Some(clients) = world.clients.clone() else {
                return world.list(n, rng);
            };
            world.step += 1;
            let cluster = world.nodes[n].cluster;
            world
                .log
                .push(format!("{}: node {n} learns its cluster {cluster}", world.step));
            let before: Vec<bool> = world.sources.iter().map(|s| refusal(&s.slot).is_some()).collect();
            clients.note_cluster(&world.nodes[n].routes, cluster.to_string());
            world.reach("note cluster");
            if world
                .sources
                .iter()
                .zip(before)
                .any(|(s, was)| !was && refusal(&s.slot).is_some())
            {
                world.reach("refused when a cluster is learned");
            }
            world.check(None)
        }
    }
}

/// How long a seed may go without a step before it counts as deadlocked.
const STALLED: Duration = Duration::from_secs(60);

/// How long the last clients may take to drop once every seed has run.
const TEARDOWN: Duration = Duration::from_secs(30);

#[test]
fn random_sources_on_shared_clients_keep_each_topic_to_one_reader() {
    let beat = Arc::new(AtomicU64::new(0));
    let current = Arc::new(AtomicU64::new(0));
    let worker = {
        let (beat, current) = (Arc::clone(&beat), Arc::clone(&current));
        std::thread::spawn(move || {
            let mut cov = Coverage::new(REGISTRY_OPTIONS.iter().copied());
            let mut graveyard = Graveyard::default();
            fuzz::run(0..100, |seed| {
                current.store(seed, Ordering::Relaxed);
                for name in run_registry(seed, &beat, &mut graveyard)? {
                    cov.reach(name);
                }
                Ok(())
            });
            graveyard.finish();
            cov.assert_complete();
        })
    };
    let (mut seen, mut since) = (0, Instant::now());
    while !worker.is_finished() {
        std::thread::sleep(Duration::from_millis(20));
        let now = beat.load(Ordering::Relaxed);
        if now != seen {
            (seen, since) = (now, Instant::now());
        } else if since.elapsed() > STALLED {
            panic!(
                "seed {} took no step for {STALLED:?}: deadlocked",
                current.load(Ordering::Relaxed)
            );
        }
    }
    if let Err(panic) = worker.join() {
        std::panic::resume_unwind(panic);
    }
}
