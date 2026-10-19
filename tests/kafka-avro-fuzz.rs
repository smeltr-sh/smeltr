//! Kafka's Avro, written and read, under random records and random registries. Each seed draws a route's record, a
//! few columns of random types, rows of random values, and a bundle naming a Kafka destination that writes them as
//! Avro and a Kafka source that reads them back into declared fields, both on one schema registry; and sometimes
//! breaks one setting.
//!
//! A bundle the save refuses is refused on the setting that was broken. One it accepts loads as drawn and loads the
//! same once saved. Its destination registers the record's schema under each topic's subject and writes each row
//! there; its source reads every row back, each declared field as its value was, a field the writer lacks or holds as
//! another type as null, and every top-level field it does not declare as its open root's. A row whose subject refuses the schema is refused and never written; a payload that
//! is no Avro, or names a schema the registry does not hold, reads as a row of nulls with the reason. A registry that
//! does not answer fails the delivery before anything is written, and the poll before anything is taken: asked again,
//! every row arrives once.
//!
//! Offline, against librdkafka's mock cluster and a registry in this process. Live, draws without a break or a fault
//! are pointed at the registry at `SCHEMA_REGISTRY_TEST_URL` and either the broker at `KAFKA_TEST_BROKERS` or the
//! Event Hubs emulator at `EVENT_HUBS_TEST_BROKERS`. The emulator's hubs are fixed and outlive each run, so there a
//! seed's ids are its own and it reads past what it did not write.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, DefaultProducerContext, Producer};
use serde_json::{Map, Value, json};
use smeltr::config_file::load_str;
use smeltr::io::kafka::KafkaSource;
use smeltr::io::{CommitMap, OwnedRecord, Source, SourceRecord, TopicPartition};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{DestinationKind, HeldSecrets, SourceKind, SourceMeta};

use crate::harness::destination::{self, Broken, Reached};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::registry::Registry;
use crate::{harness, live};

const SEEDS: u64 = 80;
const LIVE_SEEDS: u64 = 24;

/// The password a registry that signs in takes, as the leader holds it.
const PASSWORD: &str = "registry-password";

/// The Event Hubs emulator's published connection string, the same on every install.
const CONNECTION_STRING: &str = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;\
SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

/// Where a seed writes and reads: a Kafka broker's address, or an Event Hubs namespace's.
#[derive(Clone, Copy)]
enum Service<'a> {
    Kafka(&'a str),
    EventHubs(&'a str),
}

impl Service<'_> {
    /// A source reading `topics`, or hubs, under `group`.
    fn source(self, id: &str, topics: Value, group: &str) -> Value {
        match self {
            Service::Kafka(brokers) => {
                json!({ "id": id, "type": "kafka", "brokers": brokers, "topics": topics, "groupId": group })
            }
            Service::EventHubs(namespace) => json!({
                "id": id, "type": "eventHubs", "namespace": namespace, "eventHubs": topics, "consumerGroup": group,
                "connectionString": { "secret": "event-hubs", "version": 1 },
            }),
        }
    }

    /// A destination writing `topic`, or hub, by default.
    fn destination(self, id: &str, topic: &str) -> Value {
        match self {
            Service::Kafka(brokers) => json!({ "id": id, "type": "kafka", "brokers": brokers, "defaultTopic": topic }),
            Service::EventHubs(namespace) => json!({
                "id": id, "type": "eventHubs", "namespace": namespace, "defaultEventHub": topic,
                "connectionString": { "secret": "event-hubs", "version": 1 },
            }),
        }
    }
}

/// Every option the generator knows. Each must be drawn into a bundle the save accepts, or into the break it names,
/// over the seed range.
const OPTIONS: &[&str] = &[
    "type.bool",
    "type.i64",
    "type.f64",
    "type.str",
    "type.datetime",
    "type.variant",
    "type.array",
    "type.struct",
    "type.openStruct",
    "value.null",
    "field.nested",
    "field.absent",
    "field.none",
    "field.mistyped",
    "field.unclaimed",
    "topic.default",
    "topic.placed",
    "registry.trailingSlash",
    "registry.signsIn",
    "registry.refusesSubject",
    "registry.busy.destination",
    "registry.busy.source",
    "payload.unframed",
    "payload.unknownSchema",
    "break.registry.url",
    "break.registry.username",
    "break.registry.password",
    "break.registry.none",
    "break.registry.unused",
    "break.encoding",
    "break.fields",
    "break.fieldName",
];

/// The faults and breaks: no live draw makes one.
const OFFLINE_ONLY: &[&str] = &[
    "registry.signsIn",
    "registry.refusesSubject",
    "registry.busy.destination",
    "registry.busy.source",
    "payload.unframed",
    "payload.unknownSchema",
];

/// A column of the route's record.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Bool,
    Long,
    Double,
    Str,
    Time,
    Variant,
    Longs,
    Struct,
    Open,
}

const KINDS: &[Kind] = &[
    Kind::Bool,
    Kind::Long,
    Kind::Double,
    Kind::Str,
    Kind::Time,
    Kind::Variant,
    Kind::Longs,
    Kind::Struct,
    Kind::Open,
];

impl Kind {
    fn option(self) -> &'static str {
        match self {
            Kind::Bool => "type.bool",
            Kind::Long => "type.i64",
            Kind::Double => "type.f64",
            Kind::Str => "type.str",
            Kind::Time => "type.datetime",
            Kind::Variant => "type.variant",
            Kind::Longs => "type.array",
            Kind::Struct => "type.struct",
            Kind::Open => "type.openStruct",
        }
    }

    /// As the pipeline declares it.
    fn written(self) -> &'static str {
        match self {
            Kind::Bool => "bool",
            Kind::Long => "i64",
            Kind::Double => "f64",
            Kind::Str => "str",
            Kind::Time => "datetime",
            Kind::Variant => "variant",
            Kind::Longs => "array<i64>",
            Kind::Struct => "{name: str, n: i64}",
            Kind::Open => "{name: str, ...}",
        }
    }

    /// As the source declares it: a variant is written as its JSON text, and an open struct's unclaimed fields as
    /// the JSON text of `rest`.
    fn read(self) -> &'static str {
        match self {
            Kind::Variant => "str",
            Kind::Open => "{name: str, rest: str}",
            other => other.written(),
        }
    }

    /// A value of it, as a row's JSON carries it.
    fn value(self, rng: &mut Rng) -> Value {
        let n = rng.below(1000) as i64 - 500;
        match self {
            Kind::Bool => json!(rng.chance(2)),
            Kind::Long => json!(n * 1_000_003),
            Kind::Double => json!(n as f64 + 0.25),
            Kind::Str => json!(*rng.pick(&["", "a", "ü ✓", "with \"quotes\"", "x".repeat(300).as_str()])),
            Kind::Time => json!(format!(
                "2024-02-29T12:34:{:02}.{:06}Z",
                rng.below(60),
                rng.below(1_000_000)
            )),
            Kind::Variant => json!({ "k": n, "list": [1, "two"], "s": "v" }),
            Kind::Longs => Value::Array((0..rng.below(4)).map(|i| json!(n + i as i64)).collect()),
            Kind::Struct => {
                let mut s = json!({ "name": format!("n{n}") });
                if rng.chance(3) {
                    s["n"] = json!(n);
                }
                s
            }
            Kind::Open => {
                let mut s = json!({ "name": format!("o{n}") });
                if rng.chance(2) {
                    s["more"] = json!({ "deep": n });
                }
                s
            }
        }
    }
}

/// One field the source declares: its name, its type, and what it reads of a row's column.
#[derive(Clone, Debug)]
struct Declared {
    name: String,
    ty: String,
    reads: Reads,
}

#[derive(Clone, Copy, Debug)]
enum Reads {
    /// The column, as it was.
    Column(usize),
    /// `name` of the struct column.
    Nested(usize),
    /// Nothing: the writer has no such field, or holds it as another type.
    Nothing,
}

struct Draw {
    columns: Vec<(String, Kind)>,
    rows: Vec<Vec<Value>>,
    /// Whether each row goes to the other topic.
    placed: Vec<bool>,
    declared: Vec<Declared>,
    signs_in: bool,
    source_registry: Value,
    destination_registry: Value,
    broken: Option<&'static str>,
    /// Whether the break is the source's rather than the destination's.
    broken_at_source: bool,
    /// Breaks that show only where the topology compiles.
    uncompiled: bool,
}

fn draw(rng: &mut Rng, cov: &mut Reached, url: &str, live: bool) -> Draw {
    let mut columns = vec![("id".to_string(), Kind::Long)];
    for i in 0..1 + rng.below(5) {
        let kind = *rng.pick(KINDS);
        cov.reach(kind.option());
        columns.push((format!("c{i}"), kind));
    }
    let rows: Vec<Vec<Value>> = (0..1 + rng.below(12))
        .map(|r| {
            let mut row = vec![json!(r)];
            for (_, kind) in &columns[1..] {
                row.push(if rng.chance(5) {
                    cov.reach("value.null");
                    Value::Null
                } else {
                    kind.value(rng)
                });
            }
            row
        })
        .collect();
    let placed: Vec<bool> = rows.iter().map(|_| rng.chance(3)).collect();
    for p in &placed {
        cov.reach(if *p { "topic.placed" } else { "topic.default" });
    }

    let mut declared = Vec::new();
    for (i, (name, kind)) in columns.iter().enumerate() {
        match kind {
            Kind::Struct if rng.chance(3) => {
                cov.reach("field.nested");
                declared.push(Declared {
                    name: format!("{name}.name"),
                    ty: "str".into(),
                    reads: Reads::Nested(i),
                });
            }
            Kind::Str if rng.chance(4) => {
                cov.reach("field.mistyped");
                declared.push(Declared {
                    name: name.clone(),
                    ty: "i64".into(),
                    reads: Reads::Nothing,
                });
            }
            // The id identifies the row it was read from.
            _ if i == 0 || !rng.chance(5) => declared.push(Declared {
                name: name.clone(),
                ty: kind.read().into(),
                reads: Reads::Column(i),
            }),
            _ => cov.reach("field.unclaimed"),
        }
    }
    // A source may declare nothing: its open root keeps every top-level field.
    if rng.chance(8) {
        cov.reach("field.none");
        declared.clear();
    } else if rng.chance(4) {
        cov.reach("field.absent");
        declared.push(Declared {
            name: "absent".into(),
            ty: "str".into(),
            reads: Reads::Nothing,
        });
    }

    let signs_in = !live && rng.chance(4);
    let registry = |rng: &mut Rng, cov: &mut Reached| {
        let mut r = json!({ "url": url });
        if rng.chance(4) {
            cov.reach("registry.trailingSlash");
            r["url"] = json!(format!("{url}/"));
        }
        if signs_in {
            cov.reach("registry.signsIn");
            r["username"] = json!("fuzz");
            r["password"] = json!({ "secret": "registry", "version": 1 });
        }
        r
    };
    let source_registry = registry(rng, cov);
    let destination_registry = registry(rng, cov);
    let mut draw = Draw {
        columns,
        rows,
        placed,
        declared,
        signs_in,
        source_registry,
        destination_registry,
        broken: None,
        broken_at_source: false,
        uncompiled: false,
    };
    if !live && rng.chance(3) {
        draw.broken = Some(break_one(rng, cov, &mut draw));
    }
    draw
}

/// Breaks one setting of `draw`, and answers the field the save must refuse.
fn break_one(rng: &mut Rng, cov: &mut Reached, draw: &mut Draw) -> &'static str {
    let at_source = rng.chance(2);
    draw.broken_at_source = at_source;
    let registry = if at_source {
        &mut draw.source_registry
    } else {
        &mut draw.destination_registry
    };
    match *rng.pick(&[0, 1, 2, 3, 5, 6, 7, 8]) {
        0 => {
            cov.reach("break.registry.url");
            registry["url"] = json!(" ");
            "schemaRegistry.url"
        }
        1 => {
            cov.reach("break.registry.username");
            registry["username"] = json!("fuzz");
            registry.as_object_mut().unwrap().remove("password");
            "schemaRegistry.password"
        }
        2 => {
            cov.reach("break.registry.password");
            registry["password"] = json!({ "secret": "registry", "version": 1 });
            registry.as_object_mut().unwrap().remove("username");
            "schemaRegistry.username"
        }
        7 => {
            // A stream's Avro names its schema by its id in a registry, so it reads or writes none without one.
            cov.reach("break.registry.none");
            *registry = Value::Null;
            "schemaRegistry.url"
        }
        8 => {
            cov.reach("break.registry.unused");
            "schemaRegistry"
        }
        3 => {
            cov.reach("break.encoding");
            "encoding"
        }
        5 => {
            cov.reach("break.fields");
            draw.declared.push(Declared {
                name: "__source.topic".into(),
                ty: "str".into(),
                reads: Reads::Nothing,
            });
            "fields"
        }
        _ => {
            // A name the pipeline takes and Avro does not: refused once the topology compiles.
            cov.reach("break.fieldName");
            draw.columns.push(("a-b".into(), Kind::Long));
            for row in &mut draw.rows {
                row.push(json!(1));
            }
            draw.uncompiled = true;
            ""
        }
    }
}

/// The pipeline the route runs: each column declared, read from the row's JSON.
fn code(draw: &Draw) -> String {
    let declarations: String = (draw.columns.iter())
        .map(
            |(n, k)| match n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                true => format!("{n}: {}; ", k.written()),
                false => format!("$[{n:?}]: {}; ", k.written()),
            },
        )
        .collect();
    format!("{declarations}Object.assign($, json(_raw));")
}

/// `draw` as a whole bundle: JSON rows in, through the pipeline, out as Avro on `topics`; and those topics read back.
fn bundle(draw: &Draw, service: Service, topics: [&str; 2], group: &str) -> Value {
    let mut source = service.source("avro", json!(topics), group);
    source["format"] = json!("avro");
    source["schemaRegistry"] = draw.source_registry.clone();
    source["fields"] = (draw.declared.iter())
        .map(|d| json!({ "name": d.name, "type": d.ty }))
        .collect();
    let mut destination = service.destination("out", topics[0]);
    destination["format"] = json!("avro");
    destination["schemaRegistry"] = draw.destination_registry.clone();
    for o in [&mut source, &mut destination] {
        if o["schemaRegistry"].is_null() {
            o.as_object_mut().unwrap().remove("schemaRegistry");
        }
    }
    match draw.broken {
        Some("encoding") => source["encoding"] = json!("utf-16le"),
        Some("format") => destination["format"] = json!("text"),
        // A registry where nothing is Avro: a source or a destination that names one and reads or writes text.
        Some("schemaRegistry") => {
            let side = if draw.broken_at_source {
                &mut source
            } else {
                &mut destination
            };
            side.as_object_mut().unwrap().remove("format");
        }
        _ => {}
    }
    json!({
        "version": 1,
        "sources": [service.source("json", json!(["in"]), group), source],
        "destinations": [destination, service.destination("sink", "sink")],
        "pipelines": [
            { "id": "p", "name": "p", "steps": [{ "id": "declare", "type": "code", "body": code(draw) }] },
            { "id": "none", "name": "none", "steps": [] },
        ],
        "routes": [
            { "id": "write", "source": "json", "pipeline": "p", "deliver": [{ "to": ["out"] }] },
            { "id": "read", "source": "avro", "pipeline": "none", "deliver": [{ "to": ["sink"] }] },
        ],
    })
}

/// The bundle's Avro source and destination, as loaded.
type Loaded = (
    smeltr::plan::topology::KafkaSourceConfig,
    SourceMeta,
    smeltr::plan::topology::KafkaDestinationConfig,
);

fn loaded(json: &str) -> Result<Loaded, String> {
    let system = load_str(json).map_err(|e| format!("{e:?}"))?;
    system
        .compile_topology()
        .map_err(|e| format!("does not compile: {e:?}"))?;
    let source = &system.topology.sources[1];
    let SourceKind::Kafka(s) = &source.kind else {
        return Err(format!("the source loaded as {:?}", source.kind));
    };
    let DestinationKind::Kafka(d) = &system.topology.destinations[0].kind else {
        return Err("the destination loaded as no Kafka destination".into());
    };
    if d.avro.is_none() || s.avro.is_none() {
        return Err("loaded without its registry".into());
    }
    Ok((s.clone(), source.kind.meta(), d.clone()))
}

fn secrets() -> HeldSecrets {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("registry".to_string(), 1, PASSWORD.to_string()),
        ("event-hubs".to_string(), 1, CONNECTION_STRING.to_string()),
    ]);
    secrets
}

/// What the source reads of `row`, written from `text`: each declared field it has, as `record_json` writes it, and
/// each top-level field no declared field claims, `_raw` among them, nulls left out.
fn expected(draw: &Draw, row: &[Value], text: &str) -> Value {
    let mut out = Map::new();
    let claimed = |name: &str| (draw.declared.iter()).any(|d| d.name.split('.').next() == Some(name));
    for (i, (name, _)) in draw.columns.iter().enumerate() {
        if !claimed(name) {
            out.insert(name.clone(), row[i].clone());
        }
    }
    if !claimed("_raw") {
        out.insert("_raw".into(), json!(text));
    }
    for d in &draw.declared {
        let (name, value) = match d.reads {
            Reads::Column(i) => (draw.columns[i].0.clone(), row[i].clone()),
            Reads::Nested(i) if row[i].is_null() => continue,
            Reads::Nested(i) => (draw.columns[i].0.clone(), json!({ "name": row[i]["name"] })),
            Reads::Nothing => continue,
        };
        out.insert(name, value);
    }
    unnulled(Value::Object(out))
}

/// `read` as `expected` writes it: a variant's text and an open struct's `rest` read as the JSON they hold.
fn normal(draw: &Draw, read: Value) -> Value {
    let Value::Object(mut read) = read else {
        return read;
    };
    for (name, kind) in &draw.columns {
        let Some(v) = read.get_mut(name) else { continue };
        match kind {
            Kind::Variant => {
                if let Some(text) = v.as_str() {
                    *v = serde_json::from_str(text).unwrap_or(json!({ "unparsed": text }));
                }
            }
            Kind::Open => {
                if let Some(rest) = v.as_object_mut().and_then(|o| o.remove("rest"))
                    && let Some(text) = rest.as_str()
                    && let Ok(Value::Object(unclaimed)) = serde_json::from_str::<Value>(text)
                {
                    v.as_object_mut().unwrap().extend(unclaimed);
                }
            }
            _ => {}
        }
    }
    unnulled(Value::Object(read))
}

fn unnulled(v: Value) -> Value {
    match v {
        Value::Object(o) => Value::Object(
            o.into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, unnulled(v)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.into_iter().map(unnulled).collect()),
        other => other,
    }
}

/// Faults a seed makes, offline.
#[derive(Default)]
struct Faults {
    refuses_subject: bool,
    busy_destination: bool,
    busy_source: bool,
    unframed: bool,
    unknown_schema: bool,
}

fn faults(rng: &mut Rng, cov: &mut Reached) -> Faults {
    let mut f = Faults::default();
    let mut maybe = |name: &'static str, flag: &mut bool| {
        if rng.chance(5) {
            cov.reach(name);
            *flag = true;
        }
    };
    maybe("registry.refusesSubject", &mut f.refuses_subject);
    maybe("registry.busy.destination", &mut f.busy_destination);
    maybe("registry.busy.source", &mut f.busy_source);
    maybe("payload.unframed", &mut f.unframed);
    maybe("payload.unknownSchema", &mut f.unknown_schema);
    f
}

/// One seed: draws, checks the save, and when it is accepted writes the rows and reads them back.
fn seed(
    rng: &mut Rng,
    cov: &mut Reached,
    service: Service,
    url: &str,
    topics: [&str; 2],
    group: &str,
    registry: Option<&Registry>,
) -> Result<(), String> {
    let live = registry.is_none();
    let mut draw = draw(rng, cov, url, live);
    // Hubs that outlive the run hold earlier seeds' rows: this seed's ids are past any of theirs.
    let first = match service {
        Service::Kafka(_) => 0,
        Service::EventHubs(_) => {
            let micros = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64;
            micros * 64
        }
    };
    for (i, row) in draw.rows.iter_mut().enumerate() {
        row[0] = json!(first + i as i64);
    }
    let mine = |id: i64| (first..first + draw.rows.len() as i64).contains(&id);
    let json = bundle(&draw, service, topics, group).to_string();
    let broken = draw.broken.map(|field| Broken::Field(Some(field)));
    let fail = |e: String| format!("{e}\n{json}");
    if draw.uncompiled {
        // Saved, and refused once it compiles.
        let refused = load_str(&json).map_err(|e| fail(format!("{e:?}")))?.compile_topology();
        let errors = refused
            .err()
            .ok_or_else(|| fail("compiled a field Avro cannot name".into()))?;
        if !format!("{errors:?}").contains("cannot write its rows as Avro") {
            return Err(fail(format!("refused otherwise: {errors:?}")));
        }
        return Ok(());
    }
    let Some((source_config, meta, destination_config)) =
        destination::check_save(&json, broken, destination::compiles, loaded).map_err(fail)?
    else {
        return Ok(());
    };
    let faults = match registry {
        Some(r) => {
            if draw.signs_in {
                r.sign_in("fuzz", PASSWORD);
            }
            faults(rng, cov)
        }
        None => Faults::default(),
    };
    if faults.refuses_subject {
        registry.unwrap().refuse(&format!("{}-value", topics[1]));
    }

    // Written.
    let plan = harness::compile_ok(vec![harness::code(0, &code(&draw))]);
    let texts: Vec<String> = (draw.rows.iter())
        .map(|row| {
            let object: Map<String, Value> = (draw.columns.iter().zip(row))
                .filter(|(_, v)| !v.is_null())
                .map(|((n, _), v)| (n.clone(), v.clone()))
                .collect();
            Value::Object(object).to_string()
        })
        .collect();
    let chunk = harness::cut(&plan, &texts.iter().map(String::as_str).collect::<Vec<_>>());
    let rows: Vec<OwnedRecord> = (0..texts.len() as u32)
        .map(|row| OwnedRecord {
            payload: Payload::Record,
            placement: draw.placed[row as usize].then(|| topics[1].to_string()),
            columns: Some(smeltr::io::parquet::Columns {
                chunk: chunk.clone(),
                row,
            }),
            ..OwnedRecord::default()
        })
        .collect();
    let secrets = secrets();
    let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
    let mut d = smeltr::io::build_destination(&DestinationKind::Kafka(destination_config), 0, metrics, &secrets, None)
        .map_err(|e| fail(format!("does not build: {e:?}")))?;
    if faults.busy_destination {
        registry.unwrap().fail(1);
        match d.deliver(99, std::borrow::Cow::Borrowed(&rows)) {
            Err(e) if e.message.contains("busy") => {}
            other => return Err(fail(format!("delivered to a registry that did not answer: {other:?}"))),
        }
    }
    let refused = destination::write(d.as_mut(), &[rows], Duration::from_secs(30)).map_err(fail)?;
    drop(d);
    let refuses = |i: usize| faults.refuses_subject && draw.placed[i];
    let mut want: BTreeMap<i64, Value> = BTreeMap::new();
    for (i, row) in draw.rows.iter().enumerate() {
        match refused[0].iter().find(|r| r.doc == i) {
            Some(r) if refuses(i) && r.reason.contains("incompatible") => {}
            Some(r) => return Err(fail(format!("row {i} refused: {}", r.reason))),
            None if refuses(i) => return Err(fail(format!("row {i} written under a subject that refuses it"))),
            None => {
                want.insert(first + i as i64, expected(&draw, row, &texts[i]));
            }
        }
    }

    // Payloads no destination wrote, at the end of the default topic.
    let mut unread = Vec::new();
    if faults.unframed {
        unread.push((b"not avro".to_vec(), "0x6e"));
    }
    if faults.unknown_schema {
        unread.push((vec![0, 0, 0, 0x27, 0x0f, 2], "Schema not found"));
    }
    if let (false, Service::Kafka(brokers)) = (unread.is_empty(), service) {
        let producer: BaseProducer = (ClientConfig::new().set("bootstrap.servers", brokers))
            .create()
            .map_err(|e| e.to_string())?;
        for (payload, _) in &unread {
            producer
                .send(BaseRecord::<(), _>::to(topics[0]).payload(payload))
                .map_err(|(e, _)| e.to_string())?;
        }
        producer.flush(Duration::from_secs(10)).map_err(|e| e.to_string())?;
    }

    // Read.
    let mut source = KafkaSource::new(&source_config, &secrets).map_err(|e| fail(format!("does not open: {e:?}")))?;
    if faults.busy_source {
        registry.unwrap().fail(1);
    }
    let mut read: Vec<SourceRecord> = Vec::new();
    let mut commit = CommitMap::new();
    let mut failed_poll = None;
    let deadline = Instant::now() + Duration::from_secs(60);
    while read.len() < want.len() + unread.len() {
        if Instant::now() > deadline {
            return Err(fail(format!(
                "read {} of {} records",
                read.len(),
                want.len() + unread.len()
            )));
        }
        match source.poll_batch(100, 1 << 20, Duration::from_millis(100)) {
            Ok(records) => {
                for r in records {
                    commit.insert(TopicPartition::new(Arc::clone(&r.topic), r.partition), r.offset);
                    if let Service::EventHubs(_) = service {
                        let row: Value = serde_json::from_slice(&meta.record_json(&r, "")).unwrap_or_default();
                        if r.chunk.unread(r.row).is_some() || !row["id"].as_i64().is_some_and(mine) {
                            continue;
                        }
                    }
                    read.push(r);
                }
            }
            Err(e) if faults.busy_source && failed_poll.is_none() && e.message.contains("busy") => {
                failed_poll = Some(e)
            }
            Err(e) => return Err(fail(format!("a poll failed: {e:?}"))),
        }
    }
    if faults.busy_source && failed_poll.is_none() {
        // The registry was asked before the source read anything, or not at all.
        cov.forget(&["registry.busy.source"]);
    }
    let mut got: BTreeMap<i64, Value> = BTreeMap::new();
    let mut unread_got = Vec::new();
    for r in &read {
        let row: Value = serde_json::from_slice(&meta.record_json(r, "")).map_err(|e| e.to_string())?;
        if let Some(why) = r.chunk.unread(r.row) {
            unread_got.push(why.to_string());
            if row != json!({}) {
                return Err(fail(format!("a payload that did not read read as {row}")));
            }
            continue;
        }
        let Some(id) = row["id"].as_i64() else {
            return Err(fail(format!("a row read without its id: {row}")));
        };
        if got.insert(id, normal(&draw, row)).is_some() {
            return Err(fail(format!("row {id} read twice")));
        }
    }
    if got != want {
        return Err(fail(format!("read\n  {got:?}\nnot\n  {want:?}")));
    }
    source.commit(&commit).map_err(|e| fail(format!("committing: {e}")))?;
    // Each says why in its own words, and the default topic may have several partitions, so in any order.
    for (_, why) in &unread {
        if !unread_got.iter().any(|got| got.contains(why)) {
            return Err(fail(format!(
                "no payload that does not read said {why:?}: {unread_got:?}"
            )));
        }
    }
    Ok(())
}

/// Seeds at once: each waits mostly on its consumer group.
const WORKERS: usize = 16;

#[test]
fn kafka_avro_fuzz() {
    let seeds: Vec<u64> = fuzz::seeds(0..SEEDS).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let done = std::sync::Mutex::new(BTreeMap::new());
    std::thread::scope(|scope| {
        for _ in 0..WORKERS {
            scope.spawn(|| {
                while let Some(&n) = seeds.get(next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)) {
                    let mut reached = Reached::default();
                    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| offline(n, &mut reached)))
                        .unwrap_or_else(|panic| {
                            let why = (panic.downcast_ref::<String>().cloned())
                                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                                .unwrap_or_default();
                            Err(format!("panicked: {why}"))
                        });
                    done.lock().unwrap().insert(n, (checked, reached));
                }
            });
        }
    });
    let mut done = done.into_inner().unwrap();
    let mut cov = Coverage::new(OPTIONS.iter().copied());
    fuzz::run(0..SEEDS, |n| {
        let (checked, reached) = done.remove(&n).expect("every seed was checked");
        if checked.is_ok() {
            reached.credit(&mut cov);
        }
        checked
    });
    cov.assert_complete();
}

fn offline(n: u64, cov: &mut Reached) -> Result<(), String> {
    let mut rng = Rng::seeded(n);
    let mock: MockCluster<'static, DefaultProducerContext> =
        MockCluster::new(1).map_err(|e| format!("a mock cluster: {e}"))?;
    let topics = ["avro-default", "avro.other_2"];
    for t in topics {
        mock.create_topic(t, 1 + rng.below(3) as i32, 1)
            .map_err(|e| e.to_string())?;
    }
    let registry = Registry::default();
    let url = registry.serve();
    seed(
        &mut rng,
        cov,
        Service::Kafka(&mock.bootstrap_servers()),
        &url,
        topics,
        "g",
        Some(&registry),
    )
}

#[test]
fn live_kafka_avro_fuzz() {
    let Some(brokers) = live::kafka("live_kafka_avro_fuzz") else {
        return;
    };
    let Some(url) = live::schema_registry("live_kafka_avro_fuzz") else {
        return;
    };
    let options = OPTIONS
        .iter()
        .copied()
        .filter(|o| !o.starts_with("break.") && !OFFLINE_ONLY.contains(o));
    let run = std::process::id();
    destination::run(0..LIVE_SEEDS, options, |n, cov| {
        let mut rng = Rng::seeded(n);
        // Topics and a group of this run's own: the registry and the broker keep what earlier runs wrote.
        let (default, other) = (format!("avro-fuzz-{run}-{n}"), format!("avro-fuzz-{run}-{n}.other"));
        seed(
            &mut rng,
            cov,
            Service::Kafka(&brokers),
            &url,
            [&default, &other],
            &format!("avro-fuzz-{run}-{n}"),
            None,
        )
    });
}

/// The emulator's hubs (`tests/fixtures/event-hubs-emulator.json`), read under a group of this fuzzer's own.
const HUBS: [&str; 2] = ["smeltr-live", "smeltr-live-routed"];

#[test]
fn live_event_hubs_avro_fuzz() {
    let Some(namespace) = live::event_hubs("live_event_hubs_avro_fuzz") else {
        return;
    };
    let Some(url) = live::schema_registry("live_event_hubs_avro_fuzz") else {
        return;
    };
    let options = OPTIONS
        .iter()
        .copied()
        .filter(|o| !o.starts_with("break.") && !OFFLINE_ONLY.contains(o));
    destination::run(0..LIVE_SEEDS, options, |n, cov| {
        let mut rng = Rng::seeded(n);
        seed(
            &mut rng,
            cov,
            Service::EventHubs(&namespace),
            &url,
            HUBS,
            "smeltr-avro-fuzz",
            None,
        )
    });
}
