//! Generated cases against an Event Hubs destination on the Event Hubs emulator, as `just live-up` runs it.
//!
//! Each seed draws a destination's settings and writes them into a config file, which the loader turns into the
//! Kafka destination an Event Hubs destination runs as. The destination is then handed a few batches of rows of any
//! bytes, some sent to the other event hub by name, some it cannot write, and the hubs are read back. They must hold
//! each row the destination took, once, byte for byte, on the hub the row named; and every row it refused must have
//! been refused by its position, and be on no hub. The emulator, like Event Hubs' Basic and Standard tiers, takes no
//! gzip: a destination compressing with it must fail its batches naming the tier.
//!
//! Point it at `EVENT_HUBS_TEST_BROKERS`. Under `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips.

use crate::harness::fuzz::{self, Coverage, Rng};
use crate::live;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use smeltr::io::kafka::{KafkaDestination, KafkaSource};
use smeltr::io::{CommitMap, Destination, Dispatched, OutgoingRecord, OwnedRecord, Source, TopicPartition};
use smeltr::plan::topology::{Compression, DestinationKind, HeldSecrets, KafkaService, SourceKind};

/// The emulator's published connection string, the same on every install.
const CONNECTION_STRING: &str = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;\
SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

/// The emulator's fixed hubs and the consumer group this fuzzer reads them under
/// (`tests/fixtures/event-hubs-emulator.json`).
const HUBS: [&str; 2] = ["smeltr-live", "smeltr-live-routed"];
const GROUP: &str = "smeltr-fuzz";

/// Each seed writes a few batches; this many run in about a minute against the emulator.
const SEEDS: std::ops::Range<u64> = 0..24;

/// Past what one Event Hubs event may hold, and what the producer refuses before sending.
const TOO_BIG: usize = 1024 * 1024 + 1;

/// A row's value: the run's marker, so this run's rows are told from those already on the hub, then any bytes.
fn value(rng: &mut Rng, marker: &str) -> Vec<u8> {
    let mut v = marker.as_bytes().to_vec();
    let body: Vec<u8> = match rng.below(8) {
        0 => Vec::new(),
        1 => "é日本語🦀\u{202e}\t\r\n\"\\".as_bytes().to_vec(),
        2 => (0..rng.below(256)).map(|_| rng.next_u64() as u8).collect(),
        3 => vec![0; rng.below(64)],
        4 => (0..64 * 1024 + rng.below(64 * 1024))
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

/// One generated destination's settings, as its config file states them.
struct Settings {
    dest: serde_json::Value,
    options: Vec<String>,
}

fn settings(rng: &mut Rng, namespace: &str) -> Settings {
    let mut dest = json!({
        "id": "hub",
        "type": "eventHubs",
        "namespace": namespace,
        "defaultEventHub": rng.pick(&HUBS),
        "connectionString": { "secret": "event-hubs", "version": 1 },
    });
    let fields = dest.as_object_mut().unwrap();
    let mut options = vec![
        "namespace".to_string(),
        "defaultEventHub".into(),
        "connectionString".into(),
    ];
    let pools: [(&str, &[serde_json::Value]); 5] = [
        ("format", &[json!("json"), json!("text"), json!("bytes")]),
        ("field", &[json!("message"), json!("_raw")]),
        ("compression", &[json!("none"), json!("gzip")]),
        ("bufferBytes", &[json!(4 * 1024 * 1024), json!("64 MiB")]),
        ("stallAfterSecs", &[json!(10), json!(30)]),
    ];
    for (key, pool) in pools {
        // A field is read only beside the `text` and `bytes` formats.
        let readable = fields.get("format").is_some_and(|f| f != "json");
        if rng.chance(2) && (key != "field" || readable) {
            let v = rng.pick(pool).clone();
            options.push(format!("{key}={v}"));
            fields.insert(key.into(), v);
        }
    }
    Settings { dest, options }
}

/// A source reading both hubs under the fuzzer's group, as raw bytes.
fn reader(namespace: &str, secrets: &HeldSecrets) -> KafkaSource {
    let json = json!({
        "version": 1,
        "sources": [{"id": "read", "type": "eventHubs", "namespace": namespace, "eventHubs": HUBS,
            "consumerGroup": GROUP, "connectionString": {"secret": "event-hubs", "version": 1}, "encoding": "none"}],
    });
    let system = smeltr::config_file::load_str(&json.to_string()).unwrap();
    let SourceKind::Kafka(config) = &system.topology.sources[0].kind else {
        panic!("an eventHubs source loads as a Kafka source");
    };
    KafkaSource::new(config, secrets).unwrap()
}

/// Reads until every value in `expected` was seen or the deadline passes, and commits what it read. Answers each
/// value of this run read, with the hub it was on.
fn read_back(source: &mut KafkaSource, marker: &str, expected: usize) -> Result<Vec<(String, Vec<u8>)>, String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut seen = Vec::new();
    let mut commit = CommitMap::new();
    while seen.len() < expected {
        if Instant::now() > deadline {
            return Err(format!("read {} of {expected} rows", seen.len()));
        }
        let batch = source
            .poll_batch(500, 64 * 1024 * 1024, Duration::from_secs(2))
            .map_err(|e| format!("reading the hubs: {e}"))?;
        for r in batch {
            let bytes = r.bytes().unwrap_or_default().to_vec();
            if bytes.starts_with(marker.as_bytes()) {
                seen.push((r.topic.to_string(), bytes));
            }
            commit.insert(TopicPartition::new(Arc::clone(&r.topic), r.partition), r.offset);
        }
    }
    // Late copies would come within a poll or two of the rest.
    for r in source
        .poll_batch(500, 64 * 1024 * 1024, Duration::from_secs(1))
        .map_err(|e| format!("reading the hubs: {e}"))?
    {
        let bytes = r.bytes().unwrap_or_default().to_vec();
        if bytes.starts_with(marker.as_bytes()) {
            seen.push((r.topic.to_string(), bytes));
        }
        commit.insert(TopicPartition::new(Arc::clone(&r.topic), r.partition), r.offset);
    }
    source.commit(&commit).map_err(|e| format!("committing: {e}"))?;
    Ok(seen)
}

/// One seed: a destination from a generated config, its batches handed over, and the hubs read back.
fn case(
    seed: u64,
    namespace: &str,
    secrets: &HeldSecrets,
    source: &mut KafkaSource,
    coverage: &mut Coverage,
) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let s = settings(&mut rng, namespace);
    let described = s.dest.to_string();
    write(seed, &mut rng, s, secrets, source, coverage).map_err(|why| format!("{why}\ndestination: {described}"))
}

/// Writes `s`'s batches and reads them back.
fn write(
    seed: u64,
    rng: &mut Rng,
    s: Settings,
    secrets: &HeldSecrets,
    source: &mut KafkaSource,
    coverage: &mut Coverage,
) -> Result<(), String> {
    let json = json!({ "version": 1, "destinations": [s.dest] });
    let system = smeltr::config_file::load_str(&json.to_string()).map_err(|e| format!("refused: {e}"))?;
    let DestinationKind::Kafka(config) = &system.topology.destinations[0].kind else {
        return Err("an eventHubs destination loads as a Kafka destination".into());
    };
    if config.service != KafkaService::EventHubs {
        return Err(format!("loaded as {:?}", config.service));
    }
    let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
    let mut dest = KafkaDestination::new(config, 0, metrics, secrets).map_err(|e| format!("building: {e}"))?;

    let marker = format!("{}-{seed}:", std::process::id());
    // Every row taken, by value, with the hub it must be on.
    let mut taken: BTreeMap<Vec<u8>, String> = BTreeMap::new();
    let mut refused_values = BTreeSet::new();
    let mut in_flight = BTreeSet::new();
    for batch in 0..1 + rng.below(3) as u64 {
        let rows: Vec<(Vec<u8>, Option<&str>, Option<&str>)> = (0..1 + rng.below(40))
            .map(|i| {
                let mut v = value(rng, &format!("{marker}{batch}.{i}:"));
                let unfit = rng.chance(12).then_some("no field to write");
                if unfit.is_none() && rng.chance(40) {
                    v.resize(TOO_BIG, b'z');
                }
                let placement = rng.chance(3).then(|| *rng.pick(&HUBS));
                (v, placement, unfit)
            })
            .collect();
        let owned: Vec<OwnedRecord> = rows
            .iter()
            .map(|(v, placement, unfit)| {
                let mut out = OutgoingRecord::new(v);
                out.payload = &config.payload;
                out.placement = *placement;
                out.unfit = unfit.map(Cow::Borrowed);
                OwnedRecord::from(&out)
            })
            .collect();
        let (dispatched, rejections) = dest
            .deliver(batch, Cow::Owned(owned))
            .map_err(|e| format!("delivering batch {batch}: {e}"))?;
        let rejected: BTreeSet<usize> = rejections.iter().map(|r| r.doc).collect();
        for (i, (v, placement, unfit)) in rows.iter().enumerate() {
            let must_refuse = unfit.is_some() || v.len() >= TOO_BIG;
            if must_refuse != rejected.contains(&i) {
                return Err(format!(
                    "row {i} of batch {batch} ({} bytes, unfit {unfit:?}) refused: {}",
                    v.len(),
                    rejected.contains(&i)
                ));
            }
            if must_refuse {
                coverage.reach(if unfit.is_some() {
                    "refused unfit"
                } else {
                    "refused too big"
                });
                refused_values.insert(v.clone());
            } else {
                coverage.reach(if placement.is_some() {
                    "placed by row"
                } else {
                    "default hub"
                });
                taken.insert(v.clone(), placement.unwrap_or(&config.default_topic).to_string());
            }
        }
        if dispatched == Dispatched::InFlight {
            in_flight.insert(batch);
        }
    }
    // The emulator, like Event Hubs' Basic and Standard tiers, refuses gzip: what was compressed comes back
    // undelivered with its batch, and the error says why.
    if config.compression == Compression::Gzip {
        dest.flush()
            .map_err(|e| format!("gzip on the emulator did not flush: {e:?}"))?;
        let done = dest
            .poll_completions()
            .map_err(|e| format!("gzip on the emulator: {e:?}"))?;
        let errors: Vec<String> = done
            .into_iter()
            .filter_map(|c| c.undelivered)
            .map(|u| u.error.message)
            .collect();
        if !errors.iter().any(|e| e.contains("does not take gzip")) {
            return Err(format!("gzip on the emulator came back undelivered for {errors:?}"));
        }
        // A batch too small to shrink goes uncompressed, so a few rows may land: only on their own hub, once.
        for (hub, v) in read_back(source, &marker, 0)? {
            if taken.get(&v) != Some(&hub) {
                return Err(format!("a row of {} bytes is on {hub}, not its hub", v.len()));
            }
        }
        coverage.reach("gzip refused by the tier");
        for option in s.options {
            coverage.reach(option);
        }
        return Ok(());
    }
    dest.flush().map_err(|e| format!("flushing: {e}"))?;
    for c in dest.poll_completions().map_err(|e| format!("completions: {e}"))? {
        if !c.rejections.is_empty() {
            return Err(format!("batch {} refused late: {:?}", c.batch, c.rejections));
        }
        in_flight.remove(&c.batch);
    }
    if !in_flight.is_empty() {
        return Err(format!("batches {in_flight:?} never completed"));
    }

    let seen = read_back(source, &marker, taken.len())?;
    let mut counts: BTreeMap<&[u8], usize> = BTreeMap::new();
    for (hub, v) in &seen {
        if refused_values.contains(v) {
            return Err(format!("a refused row of {} bytes is on {hub}", v.len()));
        }
        match taken.get(v) {
            Some(want) if want == hub => *counts.entry(v).or_default() += 1,
            Some(want) => return Err(format!("a row for {want} is on {hub}")),
            None => return Err(format!("a row of {} bytes no batch held is on {hub}", v.len())),
        }
    }
    if let Some((v, n)) = counts.iter().find(|(_, n)| **n != 1) {
        return Err(format!("a row of {} bytes is on its hub {n} times", v.len()));
    }
    for option in s.options {
        coverage.reach(option);
    }
    Ok(())
}

#[test]
fn live_generated_rows_reach_their_event_hub_once_or_are_refused() {
    let Some(namespace) = live::event_hubs("live_generated_rows_reach_their_event_hub_once_or_are_refused") else {
        return;
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("event-hubs".to_string(), 1, CONNECTION_STRING.to_string())]);
    let mut source = reader(&namespace, &secrets);
    let mut coverage = Coverage::new([
        "namespace",
        "defaultEventHub",
        "connectionString",
        r#"format="json""#,
        r#"format="text""#,
        r#"format="bytes""#,
        r#"field="message""#,
        r#"field="_raw""#,
        r#"compression="none""#,
        r#"compression="gzip""#,
        "bufferBytes=4194304",
        r#"bufferBytes="64 MiB""#,
        "stallAfterSecs=10",
        "stallAfterSecs=30",
        "default hub",
        "placed by row",
        "refused unfit",
        "refused too big",
        "gzip refused by the tier",
    ]);
    fuzz::run(SEEDS, |seed| {
        case(seed, &namespace, &secrets, &mut source, &mut coverage)
    });
    coverage.assert_complete();
}
