//! The demo fleet's logs, from the reference generators (`tests/reference/demo.rs`): live traffic into Kafka, the
//! samples a staged fleet saves, and the lookup tables its pipelines join on.
//!
//! ```text
//! demo-traffic [scale]                    # every stream into Kafka until killed; scale multiplies every rate
//! demo-traffic sample <stream> <n> <seed> # n lines of a stream at the samples' instant, one per line
//! demo-traffic tables                     # the lookup tables, as JSON
//! demo-traffic fingerprint                # a digest of what the generators make (`reference::demo::fingerprint`)
//! ```
//!
//! `SMELTR_DEMO_KAFKA` names the brokers, `localhost:19092` (`tests/live/compose.live.yaml`'s) when unset.
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

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["tables"] => println!("{}", demo::tables()),
        ["fingerprint"] => println!("{}", demo::fingerprint()),
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
    eprintln!("usage: demo-traffic [scale] | sample <stream> <n> <seed> | tables | fingerprint");
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
    let (mut sent, mut reported) = (0u64, start);
    loop {
        let tick = Instant::now();
        let t = start.elapsed().as_secs_f64();
        let at = now();
        for (k, s) in STREAMS.iter().enumerate() {
            let wave = 1.0 + 0.25 * (TAU * t / s.period + s.phase).sin();
            // A jitter of ±15% on each tick's share.
            let jitter = 0.85 + 0.3 * (mix(next[k] ^ 0x7177) % 1_000) as f64 / 1_000.0;
            debt[k] += s.rate * scale * wave * TICK.as_secs_f64() * jitter;
            let trouble = (s.trouble)(t as u64);
            while debt[k] >= 1.0 {
                debt[k] -= 1.0;
                let i = next[k];
                next[k] += 1;
                let mut line = (s.record)(i, &at, trouble);
                // Now and then an access line is cut short in transit: it fails to parse and takes the DLQ path.
                if s.topic == "web.nginx.access" && mix(i ^ 0xC07).is_multiple_of(250) {
                    line.truncate(20 + (mix(i) % (line.len() as u64 / 2 - 20)) as usize);
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
