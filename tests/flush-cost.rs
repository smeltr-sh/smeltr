//! What one `KafkaDestination::flush` costs against a real broker, by
//! producer configuration and batch size: the real destination through
//! the real property passthrough, sending N rows and timing the `flush`
//! that follows. Needs `KAFKA_TEST_BROKERS`; `just measure` runs it.

mod live;

use smeltr::io::kafka::KafkaDestination;
use smeltr::io::{Destination, OutgoingRecord, OwnedRecord};
use smeltr::plan::topology::{KafkaDestinationConfig, KafkaProps};
use std::borrow::Cow;
use std::time::{Duration, Instant};

/// A destination reports librdkafka's statistics through a handle; these tests do not read them.
fn kafka_metrics() -> smeltr::metrics::MetricsHandle {
    smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "test")
}

fn unique(p: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{p}-{n}")
}

fn row(i: usize) -> String {
    format!(
        r#"{{"ts":"2026-08-30T12:00:{:02}Z","status":{},"bytes":{},"host":"web-{:02}"}}"#,
        i % 60,
        if i.is_multiple_of(7) { 500 } else { 200 },
        1000 + (i % 9000),
        i % 32,
    )
}

fn dest(brokers: &str, topic: &str, props: &[(&str, &str)]) -> KafkaDestination {
    KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            acks: smeltr::plan::topology::Acks::All,
            stall_after: std::time::Duration::from_secs(30),
            brokers: brokers.to_string(),
            default_topic: topic.to_string(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            identity: None,
            buffer_bytes: 64 * 1024 * 1024,
            props: KafkaProps {
                literal: props.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
                credentials: Vec::new(),
            },
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    )
    .expect("producer")
}

/// Hand over `rows`, then time `flush()` alone, `reps` times against one
/// producer.
fn flush_cost(d: &mut KafkaDestination, rows: usize, reps: usize) -> (Duration, Duration) {
    let mut total_send = Duration::ZERO;
    let mut total_flush = Duration::ZERO;
    for rep in 0..reps {
        let batch: Vec<OwnedRecord> = (0..rows)
            .map(|i| OwnedRecord::from(&OutgoingRecord::new(row(i).as_bytes())))
            .collect();
        let t = Instant::now();
        d.deliver(rep as u64, Cow::Owned(batch)).expect("deliver");
        total_send += t.elapsed();
        let t = Instant::now();
        d.flush().expect("flush");
        total_flush += t.elapsed();
    }
    (total_send / reps as u32, total_flush / reps as u32)
}

/// A bare `BaseProducer`, polled by the caller: the floor a flush can
/// reach with no background polling thread.
#[test]
fn base_producer_flush_has_no_fixed_cost() {
    let Some(brokers) = live::kafka("base_producer_flush_has_no_fixed_cost") else {
        return;
    };
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let topic = unique("flushcost-base");
    let p: BaseProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("producer");

    println!("\nBaseProducer (no background polling thread)");
    println!("{:>8}  {:>12}  {:>12}", "rows", "flush", "flush/row");
    for rows in [1usize, 10, 100, 1_000, 10_000] {
        let reps = if rows >= 10_000 { 3 } else { 10 };
        let mut total = Duration::ZERO;
        for _ in 0..reps {
            for i in 0..rows {
                let s = row(i);
                let mut rec = BaseRecord::to(&topic).payload(&s).key("k");
                while let Err((_, r)) = p.send(rec) {
                    p.poll(Duration::from_millis(1));
                    rec = r;
                }
            }
            let t = Instant::now();
            p.flush(Duration::from_secs(30)).expect("flush");
            total += t.elapsed();
        }
        let flush = total / reps as u32;
        println!(
            "{:>8}  {:>12?}  {:>10.1} us",
            rows,
            flush,
            flush.as_micros() as f64 / rows as f64
        );
    }
}

#[test]
fn flush_fixed_cost() {
    let Some(brokers) = live::kafka("flush_fixed_cost") else {
        return;
    };

    let configs: [(&str, &[(&str, &str)]); 5] = [
        ("defaults (idempotent, acks=all)", &[]),
        ("linger.ms=0", &[("linger.ms", "0")]),
        ("linger.ms=100", &[("linger.ms", "100")]),
        (
            "batch.size=1MB, linger.ms=0",
            &[("batch.size", "1048576"), ("linger.ms", "0")],
        ),
        (
            "socket.nagle.disable=true, linger.ms=0",
            &[("socket.nagle.disable", "true"), ("linger.ms", "0")],
        ),
    ];

    for (label, props) in configs {
        println!("\n{label}");
        println!("{:>8}  {:>12}  {:>12}  {:>12}", "rows", "send", "flush", "flush/row");
        let topic = unique("flushcost");
        let mut d = dest(&brokers, &topic, props);
        // Warm the connection and topic metadata so the first timed call
        // is not measuring topic auto-creation.
        let _ = flush_cost(&mut d, 1, 2);
        for rows in [1usize, 10, 100, 1_000, 10_000] {
            let reps = if rows >= 10_000 { 3 } else { 10 };
            let (send, flush) = flush_cost(&mut d, rows, reps);
            println!(
                "{:>8}  {:>12?}  {:>12?}  {:>10.1} us",
                rows,
                send,
                flush,
                flush.as_micros() as f64 / rows as f64
            );
        }
    }
}
