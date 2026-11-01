//! The demo fleet's logs, from the reference generators (`tests/reference/demo.rs`): live traffic into Kafka, the
//! samples a staged fleet saves, and the lookup tables its pipelines join on.
//!
//! ```text
//! demo-traffic [scale]                    # every stream into Kafka until killed; scale multiplies every rate
//! demo-traffic sample <stream> <n> <seed> # n lines of a stream at the samples' instant, one per line
//! demo-traffic tables                     # the lookup tables, as JSON
//! demo-traffic fingerprint                # a digest of what the generators make (`reference::demo::fingerprint`)
//! demo-traffic history <registry.db>      # a week of the fleet's throughput before what it has read so far
//! ```
//!
//! `SMELTR_DEMO_KAFKA` names the brokers, `localhost:19092` (`tests/live/compose.live.yaml`'s) when unset.
//! `SMELTR_DEMO_DOWN` names streams, separated by commas, whose sender went down. `topic` sends nothing;
//! `topic/n` sends through n relays, one of them down, so it loses every nth record.
//! `tools/screenshots` runs it.

#[path = "../tests/panos/mod.rs"]
mod panos;
#[path = "../tests/reference/mod.rs"]
mod reference;

use std::f64::consts::TAU;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord};
use reference::demo::{self, STREAMS, Stream};
use reference::{Clock, mix};
use smeltr::leader::series::{RegistryStore, Tier};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["tables"] => println!("{}", demo::tables()),
        ["fingerprint"] => println!("{}", demo::fingerprint()),
        ["history", registry] => history(std::path::Path::new(registry)),
        ["sample", topic, n, seed] => {
            for line in demo::sample(stream(topic), number(n), number(seed)) {
                println!("{line}");
            }
        }
        [] => traffic(1.0),
        [scale] => traffic(scale.parse().unwrap_or_else(|_| usage())),
        _ => usage(),
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: demo-traffic [scale] | sample <stream> <n> <seed> | tables | fingerprint | history <registry.db>"
    );
    std::process::exit(2)
}

fn number(s: &str) -> u64 {
    s.parse().unwrap_or_else(|_| usage())
}

fn stream(topic: &str) -> &'static Stream {
    STREAMS.iter().find(|s| s.topic == topic).unwrap_or_else(|| {
        let topics: Vec<_> = STREAMS.iter().map(|s| s.topic).collect();
        eprintln!("no stream {topic}: one of {}", topics.join(", "));
        std::process::exit(2)
    })
}

fn now() -> Clock {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past 1970");
    Clock::at(t.as_secs() as i64, t.subsec_nanos())
}

/// Every stream into Kafka, a tenth of a second at a time, until killed. Each rate drifts on a slow wave of its
/// own, so the charts move.
fn traffic(scale: f64) {
    let brokers = std::env::var("SMELTR_DEMO_KAFKA").unwrap_or_else(|_| "localhost:19092".to_string());
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("linger.ms", "50")
        .set("compression.type", "lz4")
        .set("queue.buffering.max.messages", "500000")
        .create()
        .expect("a Kafka producer");
    const TICK: Duration = Duration::from_millis(100);
    let start = Instant::now();
    // Each run starts its records where no earlier run's are, so a restart sends new ones.
    let first = now().unix as u64 * 10_000;
    let mut next = vec![first; STREAMS.len()];
    let mut debt = vec![0.0; STREAMS.len()];
    let down = std::env::var("SMELTR_DEMO_DOWN").unwrap_or_default();
    // Each down stream, and the share of its records that still arrive: one relay of n lost.
    let down: Vec<(&str, u64)> = down
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| match t.split_once('/') {
            Some((topic, n)) => (topic, number(n).max(2)),
            None => (t, 1),
        })
        .collect();
    for (topic, _) in &down {
        stream(topic);
    }
    let (mut sent, mut reported) = (0u64, start);
    loop {
        let tick = Instant::now();
        let t = start.elapsed().as_secs_f64();
        let at = now();
        for (k, s) in STREAMS.iter().enumerate() {
            let lost = down.iter().find(|(t, _)| *t == s.topic).map(|&(_, n)| n);
            if lost == Some(1) {
                continue;
            }
            let wave = 1.0 + 0.25 * (TAU * t / s.period + s.phase).sin();
            // A jitter of ±15% on each tick's share.
            let jitter = 0.85 + 0.3 * (mix(next[k] ^ 0x7177) % 1_000) as f64 / 1_000.0;
            debt[k] += s.rate * scale * wave * TICK.as_secs_f64() * jitter;
            let trouble = (s.trouble)(t as u64);
            while debt[k] >= 1.0 {
                debt[k] -= 1.0;
                let i = next[k];
                next[k] += 1;
                if lost.is_some_and(|n| i.is_multiple_of(n)) {
                    continue;
                }
                let mut line = (s.record)(i, &at, trouble);
                // Now and then an access line is cut short in transit: it fails to parse and takes the DLQ path.
                if s.topic == "web.nginx.access" && mix(i ^ 0xC07).is_multiple_of(250) {
                    line.truncate(line.len() / 4 + (mix(i) % (line.len() as u64 / 4 + 1)) as usize);
                }
                send(&producer, s.topic, &line);
                sent += 1;
            }
        }
        producer.poll(Duration::ZERO);
        if reported.elapsed() > Duration::from_secs(30) {
            println!("{} sent {sent}", at.hms());
            reported = Instant::now();
        }
        std::thread::sleep(TICK.saturating_sub(tick.elapsed()));
    }
}

/// Sends `line`, waiting for room while the producer's queue is full.
fn send(producer: &BaseProducer, topic: &str, line: &str) {
    let mut record = BaseRecord::<(), str>::to(topic).payload(line);
    loop {
        match producer.send(record) {
            Ok(()) => return,
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                producer.poll(Duration::from_millis(50));
                record = back;
            }
            Err((e, _)) => panic!("sending to {topic}: {e}"),
        }
    }
}

/// The counters the fleet's throughput charts and each source's expected band are drawn from.
const THROUGHPUT: &[&str] = &[
    "smeltr_rows_polled_total",
    "smeltr_bytes_polled_total",
    "smeltr_rows_sent_total",
    "smeltr_rows_delivered_total",
    "smeltr_bytes_delivered_total",
    "smeltr_rows_unmatched_total",
    "smeltr_route_matched_total",
    "smeltr_rows_dropped_total",
    "smeltr_rows_errored_total",
];

/// How far back the history goes: the week a source's expected band learns from, and the day before it that its
/// oldest windows reach into.
const HISTORY_MS: i64 = 8 * 86_400_000;

/// Writes into a staged leader's registry the throughput its fleet would have had over the last [`HISTORY_MS`]: each
/// series of [`THROUGHPUT`] at the rate it has read since it started, on a daily curve that is at its mean now, up to
/// its first point. Each bucket is the width the registry keeps at its age, so a chart of any range is as smooth as
/// a fleet that ran all week.
fn history(registry: &std::path::Path) {
    let store = RegistryStore::open(registry).expect("the leader's registry");
    let now = now().unix * 1_000;
    const DAY: f64 = 86_400_000.0;
    let mut written = 0;
    for metric in THROUGHPUT {
        let fine = store
            .series_since_at(metric, &[Tier::TenSeconds, Tier::OneMinute], 0)
            .expect("the series");
        let mut subjects = std::collections::BTreeMap::<String, Vec<(i64, f64)>>::new();
        for (subject, points) in fine.into_iter().flatten() {
            subjects.entry(subject).or_default().extend(points);
        }
        for (subject, points) in subjects {
            let first = points.iter().map(|p| p.0).min().expect("a series has a point");
            let read: f64 = points.iter().map(|p| p.1).sum();
            let rate = read / (now - first).max(60_000) as f64;
            let salt = subject.bytes().fold(0u64, |h, b| mix(h ^ u64::from(b)));
            let mut at = first;
            while at > now - HISTORY_MS {
                let width = Tier::for_window(Duration::from_millis((now - at) as u64)).bucket_ms();
                at -= width;
                // A day's curve, at its mean at this time of day, with each bucket's own noise.
                let curve = 1.0 + 0.3 * (TAU * (at - now) as f64 / DAY).sin();
                let noise = 0.8 + 0.4 * (mix(salt ^ at as u64) % 1_000) as f64 / 1_000.0;
                let delta = (rate * width as f64 * curve * noise).round();
                if delta > 0.0 {
                    write(|| store.record_delta(metric, &subject, at, delta));
                    written += 1;
                }
            }
        }
    }
    write(|| store.maintain(now));
    println!("{written} buckets of history");
}

/// Runs a write to the registry, again while the leader's own write holds it. The leader keeps running: its sign-in
/// sessions live in memory, and a restart would end the recorder's. Any other error ends the run.
fn write(mut attempt: impl FnMut() -> Result<(), smeltr::leader::store::StoreError>) {
    for _ in 0..50 {
        match attempt() {
            Ok(()) => return,
            // SQLITE_BUSY ("database is locked") and SQLITE_LOCKED ("database table is locked"), which `StoreError`
            // carries only as their message.
            Err(e) if e.message.contains("is locked") => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("writing to the registry: {e}"),
        }
    }
    attempt().expect("a write to the registry");
}
