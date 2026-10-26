//! `io::kafka`. Construction and timeout tests run against `UNREACHABLE`;
//! `live_*` tests need `KAFKA_TEST_BROKERS` and skip otherwise
//! (`tests/live/mod.rs`).
//!
//! Every `live_*` test that asserts an order or an offset range reads a
//! topic `unique()` names and the broker auto-creates, so it assumes that
//! topic has **one** partition: offsets are per partition, and a record's
//! position in a poll is only a total order when there is one of them. A
//! broker whose `num.partitions` default is higher fails those tests with
//! a payload diff rather than a word about partitions, so
//! `tests/live/compose.live.yaml` pins it.

use std::time::{Duration, Instant};

use std::sync::Arc;

use smeltr::format::encoding::Encoding;
use smeltr::handoff::{Handoff, reports};
use smeltr::io::kafka::{KafkaDestination, KafkaSource};
use smeltr::io::{CommitMap, Destination, OutgoingRecord, OwnedRecord, Source, TopicPartition};
use smeltr::plan::topology::{KafkaDestinationConfig, KafkaSourceConfig, StartPosition, StopPosition};
use smeltr::runtime::{Polled, Reader};
use std::borrow::Cow;

/// Hand `rows` to `dest` as one batch.
fn hand(dest: &mut dyn Destination, rows: &[OutgoingRecord<'_>]) {
    let rows: Vec<OwnedRecord> = rows.iter().map(OwnedRecord::from).collect();
    dest.deliver(0, Cow::Owned(rows)).unwrap();
}

/// A destination reports librdkafka's statistics through a handle; these tests do not read them.
fn kafka_metrics() -> smeltr::metrics::MetricsHandle {
    smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "test")
}

use crate::live;

fn live_brokers(test_name: &str) -> Option<String> {
    live::kafka(test_name)
}

/// A topic name unique to this run.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// A `KafkaDestination` with `topic` as its default.
fn dest_for(brokers: &str, topic: &str) -> KafkaDestination {
    KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            stall_after: std::time::Duration::from_secs(30),
            brokers: brokers.to_string(),
            default_topic: topic.to_string(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 64 * 1024 * 1024,
            props: Default::default(),
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    )
    .unwrap()
}

/// Nothing listens here: port 1 is reserved, never a real service.
const UNREACHABLE: &str = "127.0.0.1:1";

/// The constraint this replaces: one consumer, one thread, one topic. A
/// streaming source now subscribes to as many topics as it names, which is
/// what stops a fleet of topics from costing a consumer and a thread each.
#[test]
fn a_streaming_source_accepts_many_topics() {
    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: UNREACHABLE.to_string(),
        topics: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        group_id: "g".to_string(),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };
    // Constructing subscribes but never blocks on the broker, so an
    // unreachable one is fine here -- what is under test is that the
    // multi-topic config is accepted at all.
    assert!(
        KafkaSource::new(&config, &Default::default()).is_ok(),
        "a streaming source must accept several topics"
    );
}

#[test]
fn kafka_source_rejects_zero_topics() {
    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: UNREACHABLE.to_string(),
        topics: vec![],
        group_id: "g".to_string(),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };
    let Err(err) = KafkaSource::new(&config, &Default::default()) else {
        panic!("expected construction to reject a zero-topic config");
    };
    assert!(err.to_string().contains("at least one topic"), "{err}");
}

/// Construction doesn't connect -- `rdkafka::ClientConfig::create` builds
/// the client handle and starts its background threads, but the actual
/// broker connection happens lazily on first use. A syntactically valid
/// but unreachable address should still construct cleanly.
#[test]
fn kafka_source_construction_does_not_require_a_reachable_broker() {
    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: UNREACHABLE.to_string(),
        topics: vec!["a-topic".to_string()],
        group_id: "a-group".to_string(),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };
    KafkaSource::new(&config, &Default::default()).expect("construction should not touch the network");
}

/// An operator's own librdkafka properties reach the real client. A
/// bogus *value* for a real property is the check: librdkafka validates
/// property values at construction, so a producer that refuses to build
/// with `compression.type = "not-a-codec"` proves the property was
/// actually applied -- where a well-formed value would build either way
/// and prove nothing.
#[test]
fn operator_properties_reach_the_producer() {
    let mut props = smeltr::plan::topology::KafkaProps::default();
    props.literal.push(("compression.type".into(), "not-a-codec".into()));
    let err = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            stall_after: std::time::Duration::from_secs(30),
            brokers: UNREACHABLE.into(),
            default_topic: "t".into(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 64 * 1024 * 1024,
            props,
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    );
    let err = match err {
        Err(e) => e,
        Ok(_) => panic!("librdkafka should reject an unknown compression codec"),
    };
    assert!(
        err.to_string().contains("compression.type"),
        "the error should name the property: {err}"
    );
}

/// The credential half, and the failure that matters: one this worker cannot
/// obtain is an error naming it, not a silently skipped property. An operator
/// who mistyped the variable and got an unauthenticated connection would have
/// no way to tell that from a broker refusing them.
#[test]
fn a_credential_this_worker_cannot_obtain_is_a_clear_error() {
    let mut props = smeltr::plan::topology::KafkaProps::default();
    props.credentials.push((
        "sasl.password".into(),
        smeltr::plan::topology::SecretRef::WorkerLocal("SMELTR_TEST_DEFINITELY_UNSET_VAR".into()),
    ));
    let err = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            stall_after: std::time::Duration::from_secs(30),
            brokers: UNREACHABLE.into(),
            default_topic: "t".into(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 64 * 1024 * 1024,
            props,
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    );
    let msg = match err {
        Err(e) => e.to_string(),
        Ok(_) => panic!("an unset variable must not be skipped silently"),
    };
    assert!(msg.contains("SMELTR_TEST_DEFINITELY_UNSET_VAR"), "{msg}");
    assert!(msg.contains("sasl.password"), "{msg}");
}

/// The same failure for a leader-stored credential, including the one that
/// only this custody has: the worker holds the name but at a version older
/// than the bundle was built for.
#[test]
fn a_stored_credential_the_leader_has_not_sent_yet_is_a_clear_error() {
    let mut props = smeltr::plan::topology::KafkaProps::default();
    props.credentials.push((
        "sasl.password".into(),
        smeltr::plan::topology::SecretRef::LeaderStored {
            name: "kafka-west-sasl".into(),
            version: 7,
        },
    ));
    let mut stale = smeltr::plan::topology::HeldSecrets::default();
    stale.set_stored([("kafka-west-sasl".to_string(), 6, "old".to_string())]);

    for held in [smeltr::plan::topology::HeldSecrets::default(), stale] {
        let err = KafkaDestination::new(
            &KafkaDestinationConfig {
                payload: smeltr::plan::config::Payload::Record,
                compression: smeltr::plan::topology::Compression::None,
                stall_after: std::time::Duration::from_secs(30),
                brokers: UNREACHABLE.into(),
                default_topic: "t".into(),
                service: smeltr::plan::topology::KafkaService::Kafka,
                tls: None,
                buffer_bytes: 64 * 1024 * 1024,
                props: props.clone(),
                avro: None,
            },
            0,
            kafka_metrics(),
            &held,
        );
        let msg = match err {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a credential this worker does not have must not be skipped silently"),
        };
        assert!(msg.contains("kafka-west-sasl"), "{msg}");
        assert!(msg.contains("sasl.password"), "{msg}");
    }
}

/// Construction asks the broker how to size the producer and goes ahead without an answer: an unreachable broker costs
/// the link timeout and librdkafka's own sizes, not the deployment.
#[test]
fn kafka_destination_construction_does_not_require_a_reachable_broker() {
    let config = KafkaDestinationConfig {
        payload: smeltr::plan::config::Payload::Record,
        compression: smeltr::plan::topology::Compression::None,
        stall_after: std::time::Duration::from_secs(30),
        brokers: UNREACHABLE.to_string(),
        default_topic: "out".to_string(),
        service: smeltr::plan::topology::KafkaService::Kafka,
        tls: None,
        buffer_bytes: 64 * 1024 * 1024,
        props: Default::default(),
        avro: None,
    };
    KafkaDestination::new(&config, 0, kafka_metrics(), &Default::default())
        .expect("construction goes ahead without the broker");
}

/// The real point of this test: `poll_batch`'s deadline math and `Timeout`
/// conversion, exercised against the actual `rdkafka::BaseConsumer` type
/// and a real "connection refused" (nothing listens on `UNREACHABLE`).
/// This caught a genuine bug during development: librdkafka does *not*
/// treat an unreachable broker as a silent timeout -- `poll` returns
/// `Some(Err(KafkaError::MessageConsumption(BrokerTransportFailure)))`,
/// repeatedly, one per (fast, immediately-refused) reconnect attempt. The
/// first version of this code treated any `poll` error as fatal and
/// propagated it as `Err`; this test is what caught that, against the real
/// client type, not a guess about what it would do.
#[test]
fn poll_batch_against_an_unreachable_broker_times_out_empty_not_hung() {
    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: UNREACHABLE.to_string(),
        topics: vec!["a-topic".to_string()],
        group_id: "a-group".to_string(),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };
    let mut source = KafkaSource::new(&config, &Default::default()).unwrap();

    let budget = Duration::from_millis(500);
    let start = Instant::now();
    let batch = source
        .poll_batch(10, 1_000_000, budget)
        .expect("a transient connection error is not an error at this layer");
    let elapsed = start.elapsed();

    assert!(batch.is_empty());
    // Both bounds matter here, not just the upper one: the lower bound
    // confirms the transient-error path actually keeps retrying up to the
    // deadline (a version that bailed out after the *first* error would
    // also pass an upper-bound-only check, without proving the retry loop
    // itself works). The upper bound is the generous "didn't hang forever"
    // half -- not pinning down librdkafka's own poll-loop granularity.
    // A few ms of slack for ordinary scheduling jitter -- this is checking
    // "did the retry loop run for roughly the full budget," not pinning
    // down wall-clock timing to the millisecond.
    assert!(
        elapsed + Duration::from_millis(20) >= budget,
        "poll_batch returned after {elapsed:?}, before its {budget:?} budget -- \
         did it bail out on the first transient error instead of retrying?"
    );
    assert!(
        elapsed < budget * 10,
        "poll_batch took {elapsed:?}, expected roughly {budget:?}"
    );
}

/// `max_rows: 0` is a degenerate case `Source::poll_batch`'s own doc
/// doesn't explicitly rule on, but "asked for nothing, get nothing, don't
/// touch the network" is the only sane reading -- and it matters here
/// specifically because `KafkaSource::poll_batch` has an early return for
/// exactly this case, precisely so it never mis-drains `self.pending` into
/// a zero-capacity batch.
#[test]
fn poll_batch_max_rows_zero_returns_empty_immediately() {
    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: UNREACHABLE.to_string(),
        topics: vec!["a-topic".to_string()],
        group_id: "a-group".to_string(),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };
    let mut source = KafkaSource::new(&config, &Default::default()).unwrap();

    let start = Instant::now();
    let batch = source.poll_batch(0, 1_000_000, Duration::from_secs(30)).unwrap();
    assert!(batch.is_empty());
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "max_rows: 0 should never block on the network"
    );
}

/// Polls `source` repeatedly until at least `want` records have arrived or
/// `overall_budget` elapses. `poll_batch` can legitimately return a
/// partial batch on any single call (that's its whole contract) -- a test
/// that wants a known count needs its own outer loop, the same as a real
/// driver loop's caller would.
fn drain_at_least(source: &mut KafkaSource, want: usize, overall_budget: Duration) -> Vec<smeltr::io::SourceRecord> {
    let deadline = Instant::now() + overall_budget;
    let mut out = Vec::new();
    while out.len() < want && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let mut batch = source.poll_batch(want - out.len(), 10_000_000, remaining).unwrap();
        out.append(&mut batch);
    }
    out
}

/// Records in hand go out once nothing more is queued: a batch short of
/// `max_rows` does not wait out the timeout to fill.
#[test]
fn live_a_partial_batch_returns_without_waiting_out_the_timeout() {
    let Some(brokers) = live_brokers("live_a_partial_batch_returns_without_waiting_out_the_timeout") else {
        return;
    };
    let topic = unique("partial-batch");
    let mut dest = dest_for(&brokers, &topic);
    let rows: Vec<OutgoingRecord<'_>> = ["a", "b", "c"]
        .iter()
        .map(|m| OutgoingRecord::new(m.as_bytes()))
        .collect();
    hand(&mut dest, &rows);
    dest.flush().unwrap();

    let mut source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers,
            topics: vec![topic],
            group_id: unique("group"),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();
    let timeout = Duration::from_secs(20);
    let deadline = Instant::now() + Duration::from_secs(30);
    let (batch, took) = loop {
        let start = Instant::now();
        let batch = source.poll_batch(1_000, 1_000_000, timeout).unwrap();
        if !batch.is_empty() || Instant::now() > deadline {
            break (batch, start.elapsed());
        }
    };
    assert!(!batch.is_empty(), "the three records never arrived");
    assert!(
        took < timeout / 2,
        "a batch of {} took {took:?}: it waited for records that were not coming",
        batch.len()
    );
}

/// The actual point of the `offset + 1` translation in `KafkaSource::commit`
/// (see its own doc): a second consumer in the *same* group should resume
/// exactly where an explicit partial commit left off -- not redeliver
/// everything already committed, not restart from scratch either.
/// A static member that leaves and comes back under its id takes back its partition at once, and the member that
/// stayed keeps its own: a redeploy is not a rebalance. A dynamic member waits for the group to rebalance, which takes
/// at least until the member that stayed next heartbeats.
#[test]
fn live_a_static_member_that_comes_back_takes_back_its_partition_without_a_rebalance() {
    let Some(brokers) =
        live_brokers("live_a_static_member_that_comes_back_takes_back_its_partition_without_a_rebalance")
    else {
        return;
    };
    // Longer than librdkafka's default, so a rebalance takes far longer than a static member's return.
    const HEARTBEAT: Duration = Duration::from_secs(10);
    let topic = unique("static-members");
    make_topic(&brokers, &topic, 2);
    let group = unique("group");
    let member = |id: &str| {
        let mut source = KafkaSource::new(
            &KafkaSourceConfig {
                service: smeltr::plan::topology::KafkaService::Kafka,
                encoding: Encoding::Auto,
                fetch_bytes: 64 * 1024 * 1024,
                brokers: brokers.clone(),
                topics: vec![topic.clone()],
                group_id: group.clone(),
                tls: None,
                start: StartPosition::Earliest,
                stop: None,
                deployed_at: 0,
                member: Some(id.to_string()),
                rack: None,
                props: smeltr::plan::topology::KafkaProps {
                    literal: vec![("heartbeat.interval.ms".to_string(), HEARTBEAT.as_millis().to_string())],
                    credentials: Vec::new(),
                },
                avro: None,
            },
            &Default::default(),
        )
        .unwrap();
        source.block().unwrap();
        source
    };
    let owned = |source: &mut KafkaSource| {
        source.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap();
        source.assigned_partitions()
    };

    let mut first = member("worker-a");
    let mut second = member("worker-b");
    let deadline = Instant::now() + Duration::from_secs(60);
    while !(owned(&mut first).len() == 1 && owned(&mut second).len() == 1) {
        assert!(Instant::now() < deadline, "the group never shared its two partitions");
    }
    let (taken, kept) = (first.assigned_partitions(), second.assigned_partitions());

    drop(first);
    let mut again = member("worker-a");
    let deadline = Instant::now() + HEARTBEAT / 2;
    while owned(&mut again) != taken {
        assert!(
            Instant::now() < deadline,
            "the member that came back waited for a rebalance"
        );
        assert_eq!(owned(&mut second), kept, "the group rebalanced");
    }
    // Long enough for the member that stayed to hear of a rebalance, were there one.
    let settled = Instant::now() + Duration::from_secs(10);
    while Instant::now() < settled {
        assert_eq!(owned(&mut second), kept, "the group rebalanced");
        assert_eq!(owned(&mut again), taken);
    }
}

/// Makes `topic` on the broker with `partitions` partitions, where auto-creation would make one.
fn make_topic(brokers: &str, topic: &str, partitions: i32) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for made in runtime
        .block_on(admin.create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        ))
        .unwrap()
    {
        made.unwrap();
    }
}

#[test]
fn live_commit_resumes_from_committed_offset_not_from_the_start() {
    let Some(brokers) = live_brokers("live_commit_resumes_from_committed_offset_not_from_the_start") else {
        return;
    };
    let topic = unique("commit-resume");
    let group = unique("group");

    let mut dest = dest_for(&brokers, &topic);
    let rows: Vec<OutgoingRecord<'_>> = ["a", "b", "c"]
        .iter()
        .map(|m| OutgoingRecord::new(m.as_bytes()))
        .collect();
    hand(&mut dest, &rows);
    dest.flush().unwrap();

    // First consumer reads all three (offsets 0, 1, 2) but only commits
    // through offset 1 ("b") -- "c" is deliberately left uncommitted, to
    // prove commit() advances exactly as far as told, not "everything
    // polled so far."
    {
        let mut source = KafkaSource::new(
            &KafkaSourceConfig {
                service: smeltr::plan::topology::KafkaService::Kafka,
                encoding: Encoding::Auto,
                fetch_bytes: 64 * 1024 * 1024,
                brokers: brokers.clone(),
                topics: vec![topic.clone()],
                group_id: group.clone(),
                tls: None,
                start: StartPosition::Earliest,
                stop: None,
                deployed_at: 0,
                member: None,
                rack: None,
                props: Default::default(),
                avro: None,
            },
            &Default::default(),
        )
        .unwrap();
        let records = drain_at_least(&mut source, 3, Duration::from_secs(15));
        assert_eq!(records.len(), 3);

        let mut commit = CommitMap::new();
        // topic's partition 0, last processed offset 1 ("b")
        commit.insert(TopicPartition::new(Arc::from(topic.as_str()), 0), 1);
        source.commit(&commit).unwrap();
    }

    // Second consumer, same group id: should resume from the committed
    // offset (2, "c") -- not "a"/"b" again, not the full topic from
    // scratch either.
    {
        let mut source = KafkaSource::new(
            &KafkaSourceConfig {
                service: smeltr::plan::topology::KafkaService::Kafka,
                encoding: Encoding::Auto,
                fetch_bytes: 64 * 1024 * 1024,
                brokers,
                topics: vec![topic],
                group_id: group,
                tls: None,
                start: StartPosition::Earliest,
                stop: None,
                deployed_at: 0,
                member: None,
                rack: None,
                props: Default::default(),
                avro: None,
            },
            &Default::default(),
        )
        .unwrap();
        let records = drain_at_least(&mut source, 1, Duration::from_secs(15));
        assert_eq!(
            records.len(),
            1,
            "expected exactly the one uncommitted record to redeliver"
        );
        assert_eq!(records[0].bytes().unwrap(), b"c");
        assert_eq!(records[0].offset, 2);
    }
}

/// `lag` samples against the *committed* offset, so a durable lag of exactly one exists once one of two produced
/// records is read and committed: the broker's high watermark is 2 (the next offset it will assign), the committed
/// offset is 1 (the next one this group will read), and their difference is what a chart shows.
#[test]
fn live_lag_reports_the_committed_offsets_distance_from_the_watermark() {
    let Some(brokers) = live_brokers("live_lag_reports_the_committed_offsets_distance_from_the_watermark") else {
        return;
    };
    let topic = unique("lag");

    let mut dest = dest_for(&brokers, &topic);
    let rows: Vec<OutgoingRecord<'_>> = ["a", "b"].iter().map(|m| OutgoingRecord::new(m.as_bytes())).collect();
    hand(&mut dest, &rows);
    dest.flush().unwrap();

    let mut source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers,
            topics: vec![topic.clone()],
            group_id: unique("group"),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();

    let records = drain_at_least(&mut source, 1, Duration::from_secs(15));
    let first = records
        .iter()
        .find(|r| r.offset == 0)
        .expect("offset 0 among what was read");
    let mut commit = CommitMap::new();
    commit.insert(TopicPartition::new(Arc::from(topic.as_str()), first.partition), 0);
    source.commit(&commit).unwrap();

    // Lag is measured on a thread of its own, once at start -- before the group assigned anything, so empty -- and
    // then every `LAG_SAMPLE_INTERVAL`. The first sample with the partition in it comes from the second round.
    let deadline = Instant::now() + Duration::from_secs(20);
    let samples = loop {
        let samples = source.lag();
        if !samples.is_empty() {
            break samples;
        }
        assert!(Instant::now() < deadline, "no lag sample within 20s");
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(samples.len(), 1, "one assigned partition, one sample");
    let sample = &samples[0];
    assert_eq!(sample.partition.topic.as_ref(), topic);
    assert_eq!(sample.partition.partition, 0);
    assert_eq!(sample.lag, 1, "one record committed, one still unread");
}

#[test]
fn live_placement_routes_to_a_different_topic() {
    let Some(brokers) = live_brokers("live_placement_routes_to_a_different_topic") else {
        return;
    };
    let default_topic = unique("default-topic");
    let override_topic = unique("override-topic");

    let mut dest = dest_for(&brokers, &default_topic);
    hand(
        &mut dest,
        &[OutgoingRecord::new(b"goes-to-override").with_placement(Some(&override_topic))],
    );
    dest.flush().unwrap();

    let mut source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers,
            topics: vec![override_topic],
            group_id: unique("group"),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();
    let records = drain_at_least(&mut source, 1, Duration::from_secs(15));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].bytes().unwrap(), b"goes-to-override");
}

/// Test-only: produces records at explicit, deterministic timestamps.
/// `Destination`'s public 3-argument `send` (and `KafkaDestination`, which
/// only implements that trait) has no timestamp override, so a test that
/// needs to control *when* a record claims to have happened -- as opposed
/// to "whenever this test happened to run" -- talks to a raw
/// `rdkafka::producer::BaseProducer` directly instead. This is test
/// plumbing only, not a gap in the production `Destination` trait.
fn seed_with_timestamps(brokers: &str, topic: &str, records: &[(i64, &[u8])]) {
    use rdkafka::ClientConfig as RawClientConfig;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use rdkafka::util::Timeout as RawTimeout;

    let producer: BaseProducer = RawClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    for &(ts, payload) in records {
        let mut record = BaseRecord::<(), [u8]>::to(topic).payload(payload).timestamp(ts);
        loop {
            match producer.send(record) {
                Ok(()) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rejected)) => {
                    producer.poll(Duration::from_millis(50));
                    record = rejected;
                }
                Err((e, _)) => panic!("seed_with_timestamps: send failed: {e}"),
            }
        }
    }
    producer.flush(RawTimeout::Never).unwrap();
}

/// The actual backfill use case this whole mechanism exists for:
/// "everything between 4 and 8 AM yesterday" is `start: Timestamp(4am)`,
/// `stop: Some(Timestamp(8am))` -- a half-open `[start, stop)` window. Six
/// records are seeded spanning a fake "yesterday," and only the three
/// whose own timestamp falls inside the window should come back:
/// specifically *not* the one sitting exactly on the stop boundary,
/// proving `stop` really is exclusive, not inclusive.
#[test]
fn live_timestamp_backfill_reads_exactly_the_bounded_window() {
    let Some(brokers) = live_brokers("live_timestamp_backfill_reads_exactly_the_bounded_window") else {
        return;
    };
    let topic = unique("timestamp-backfill");

    let hour = 3_600_000i64;
    let t0 = 1_700_000_000_000i64; // an arbitrary fixed reference point
    let window_start = t0 + 4 * hour;
    let window_stop = t0 + 8 * hour;

    seed_with_timestamps(
        &brokers,
        &topic,
        &[
            (t0 + hour, b"before-window"),
            (window_start, b"at-window-start"), // included: >= start
            (t0 + 6 * hour, b"mid-window"),
            (window_stop - 60_000, b"just-before-stop"),
            (window_stop, b"at-window-stop"), // excluded: stop is exclusive
            (t0 + 10 * hour, b"after-window"),
        ],
    );

    let mut source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers,
            topics: vec![topic],
            group_id: unique("group"),
            tls: None,
            start: StartPosition::Timestamp(window_start),
            stop: Some(StopPosition::Timestamp(window_stop)),
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();

    let records = drain_at_least(&mut source, 3, Duration::from_secs(15));
    let payloads: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(
        payloads,
        vec![
            b"at-window-start".as_slice(),
            b"mid-window".as_slice(),
            b"just-before-stop".as_slice(),
        ],
        "expected exactly the three records inside [start, stop), in order"
    );

    // One more poll should find nothing (the paused partition has nothing
    // left to give), and the source should now report itself exhausted --
    // the signal a one-shot backfill job uses to know when to stop asking
    // and exit, rather than a normal streaming source's "poll forever."
    let extra = source.poll_batch(10, 1_000_000, Duration::from_secs(2)).unwrap();
    assert!(extra.is_empty(), "expected nothing past the stop boundary");
    assert!(
        source.is_exhausted(),
        "expected the bounded read to report itself exhausted once its stop offset is reached"
    );
}

/// A range restarted with the same `group_id` picks up from its last
/// commit, not from `start` again.
#[test]
fn live_bounded_read_resumes_from_committed_offset_on_restart() {
    let Some(brokers) = live_brokers("live_bounded_read_resumes_from_committed_offset_on_restart") else {
        return;
    };
    let topic = unique("resume-backfill");
    let group = unique("group");

    let mut dest = dest_for(&brokers, &topic);
    let rows: Vec<OutgoingRecord<'_>> = ["a", "b", "c", "d"]
        .iter()
        .map(|m| OutgoingRecord::new(m.as_bytes()))
        .collect();
    hand(&mut dest, &rows);
    dest.flush().unwrap();

    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: brokers.clone(),
        topics: vec![topic.clone()],
        group_id: group.clone(),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };

    // First run: reads "a", "b" (offsets 0, 1) and commits through 1,
    // then stops (simulating a job that got killed partway through).
    {
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();
        let records = drain_at_least(&mut source, 2, Duration::from_secs(15));
        assert_eq!(records.len(), 2);
        let mut commit = CommitMap::new();
        commit.insert(TopicPartition::new(Arc::from(topic.as_str()), 0), 1);
        source.commit(&commit).unwrap();
    }

    // Second run, same group_id, same `start` in the config --
    // should resume from the commit (offset 2, "c") rather than replaying
    // "a"/"b" from the configured `start` again.
    {
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();
        let records = drain_at_least(&mut source, 2, Duration::from_secs(15));
        let payloads: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
        assert_eq!(
            payloads,
            vec![b"c".as_slice(), b"d".as_slice()],
            "expected the restarted read to resume from the commit, not replay from start"
        );
    }
}

/// A source restarted after its range is done reads nothing, and is exhausted
/// at the record at its stop.
#[test]
fn live_a_range_restarted_at_its_stop_is_exhausted_once_assigned() {
    let Some(brokers) = live_brokers("live_a_range_restarted_at_its_stop_is_exhausted_once_assigned") else {
        return;
    };
    let topic = unique("already-done-backfill");
    let group = unique("group");

    let stop = 1_700_000_000_000i64;
    seed_with_timestamps(&brokers, &topic, &[(stop - 1_000, b"only-record"), (stop, b"at-stop")]);

    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: brokers.clone(),
        topics: vec![topic.clone()],
        group_id: group.clone(),
        tls: None,
        start: StartPosition::Earliest,
        stop: Some(StopPosition::Timestamp(stop)),
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };

    // First run: consumes the one record and commits past it.
    {
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();
        let records = drain_at_least(&mut source, 1, Duration::from_secs(15));
        assert_eq!(records.len(), 1);
        let mut commit = CommitMap::new();
        commit.insert(TopicPartition::new(Arc::from(topic.as_str()), 0), 0);
        source.commit(&commit).unwrap();
    }

    // Second run: resumes at the committed offset (1), the record at the
    // stop.
    {
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !source.is_exhausted() {
            assert!(Instant::now() < deadline, "a range restarted at its stop never ended");
            let read = source.poll_batch(10, 1_000_000, Duration::from_millis(200)).unwrap();
            assert!(read.is_empty(), "a range restarted at its stop reads nothing");
        }
    }
}

/// A start with no stop reads from that point and stays open: a record
/// that arrives later is read too.
#[test]
fn live_a_read_from_a_time_with_no_stop_stays_open() {
    let Some(brokers) = live_brokers("live_a_read_from_a_time_with_no_stop_stays_open") else {
        return;
    };
    let topic = unique("from-a-time");
    let t0 = 1_700_000_000_000i64;
    seed_with_timestamps(&brokers, &topic, &[(t0, b"before"), (t0 + 2, b"from")]);

    let mut source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: brokers.clone(),
            topics: vec![topic.clone()],
            group_id: unique("group"),
            tls: None,
            start: StartPosition::Timestamp(t0 + 1),
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();
    let records = drain_at_least(&mut source, 1, Duration::from_secs(15));
    assert_eq!(
        records.iter().map(|r| r.bytes().unwrap()).collect::<Vec<_>>(),
        vec![b"from".as_slice()]
    );
    assert!(!source.is_exhausted(), "a read with no stop never ends");

    seed_with_timestamps(&brokers, &topic, &[(t0 + 3, b"later")]);
    let records = drain_at_least(&mut source, 1, Duration::from_secs(15));
    assert_eq!(
        records.iter().map(|r| r.bytes().unwrap()).collect::<Vec<_>>(),
        vec![b"later".as_slice()]
    );
}

/// The producer buffer bound reaches librdkafka under the name smeltr
/// thinks it does. A misspelled property is not an error at construction
/// -- rdkafka would just ignore it -- so the proof is that a value
/// librdkafka's own range check refuses is refused here.
#[test]
fn the_producer_buffer_bound_reaches_librdkafka() {
    let ok = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            stall_after: std::time::Duration::from_secs(30),
            brokers: UNREACHABLE.into(),
            default_topic: "t".into(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 8 * 1024 * 1024,
            props: Default::default(),
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    );
    assert!(ok.is_ok(), "{:?}", ok.err().map(|e| e.to_string()));

    // librdkafka's own minimum for this property is 1 kbyte.
    let err = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            stall_after: std::time::Duration::from_secs(30),
            brokers: UNREACHABLE.into(),
            default_topic: "t".into(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 0,
            props: Default::default(),
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    );
    let err = match err {
        Err(e) => e,
        Ok(_) => panic!("librdkafka should refuse a zero-length producer queue"),
    };
    assert!(err.to_string().contains("queue.buffering.max.kbytes"), "{err}");
}

/// The fetch-queue bound reaches librdkafka under the name smeltr thinks
/// it does. A misspelled property is not an error at construction --
/// rdkafka ignores it -- so the proof is that a value librdkafka's own
/// range check refuses is refused here.
#[test]
fn the_consumer_fetch_bound_reaches_librdkafka() {
    let ok = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 16 * 1024 * 1024,
            brokers: UNREACHABLE.into(),
            topics: vec!["t".into()],
            group_id: "fetch-bound-ok".into(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    );
    assert!(ok.is_ok(), "{:?}", ok.err().map(|e| e.to_string()));

    // librdkafka's own minimum for this property is 1 kbyte.
    let err = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 0,
            brokers: UNREACHABLE.into(),
            topics: vec!["t".into()],
            group_id: "fetch-bound-bad".into(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    );
    let msg = match err {
        Err(e) => e.to_string(),
        Ok(_) => panic!("librdkafka should refuse a zero-length fetch queue"),
    };
    assert!(msg.contains("queued.max.messages.kbytes"), "{msg}");
}

/// A consumer group rebalances only once every member has polled, so a member that stops polling holds every
/// rebalance of its group until its poll interval runs out, and is then evicted. A reader past its in-flight limit --
/// what a destination that is down leaves it -- must still let the group rebalance: a second member joining gets its
/// partition as quickly as it does beside a reader that is polling.
#[test]
fn live_a_reader_at_its_in_flight_limit_still_lets_its_group_rebalance() {
    let Some(brokers) = live_brokers("live_a_reader_at_its_in_flight_limit_still_lets_its_group_rebalance") else {
        return;
    };
    // Two single-partition topics, so a group of two has a partition each to share.
    let topics = [unique("depth-a"), unique("depth-b")];
    for topic in &topics {
        let mut dest = dest_for(&brokers, topic);
        let bodies: Vec<String> = (0..50).map(|i| format!("row-{i}")).collect();
        let rows: Vec<OutgoingRecord<'_>> = bodies.iter().map(|b| OutgoingRecord::new(b.as_bytes())).collect();
        hand(&mut dest, &rows);
        dest.flush().unwrap();
    }

    let mut waited = Vec::new();
    for at_limit in [false, true] {
        let config = KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: brokers.clone(),
            topics: topics.to_vec(),
            group_id: unique("depth-group"),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        };
        let source = KafkaSource::new(&config, &Default::default()).unwrap();
        // Nothing acknowledges what this reader hands over, as when its destination is down.
        let limit = if at_limit { 1 } else { usize::MAX };
        let mut reader = Reader::new(0, Box::new(source), vec![0], limit, kafka_metrics());
        let handoff = Handoff::new();
        let (_reports, acks) = reports();
        let step = |reader: &mut Reader| {
            let polled = reader
                .step(&handoff, &acks, 10, 1 << 20, Duration::from_millis(100))
                .unwrap();
            // Below its limit the reader goes on polling, so what it hands over is taken off the queue.
            while let smeltr::handoff::Taken::Batch(_) = handoff.pop(Duration::ZERO) {}
            polled
        };

        // The first member takes both partitions and reads until it is where the case under test puts it.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let polled = step(&mut reader);
            let there = if at_limit {
                matches!(polled, Polled::Full)
            } else {
                reader.source.assigned_partitions().len() == 2
            };
            if there {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "at_limit={at_limit}: the first member never got there"
            );
        }

        // The second member joins. The first goes on stepping, as a deployed reader loop does.
        let mut second = KafkaSource::new(&config, &Default::default()).unwrap();
        let started = Instant::now();
        let deadline = started + Duration::from_secs(30);
        let joined = loop {
            step(&mut reader);
            second.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap();
            if !second.assigned_partitions().is_empty() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
        };
        waited.push((at_limit, joined, started.elapsed()));
    }
    for (at_limit, joined, elapsed) in &waited {
        assert!(
            joined,
            "at_limit={at_limit}: the second member got no partition within {elapsed:?}; all runs: {waited:?}"
        );
    }
}

/// A blocked source supplies nothing, including from a partition the group assigns it while it is blocked: a
/// partition assigned mid-pause starts unpaused, and the pause has to reach it too.
#[test]
fn live_a_partition_assigned_while_blocked_supplies_nothing() {
    let Some(brokers) = live_brokers("live_a_partition_assigned_while_blocked_supplies_nothing") else {
        return;
    };
    // Two single-partition topics, created empty, so each of two members holds one.
    let topics = [unique("blocked-a"), unique("blocked-b")];
    for topic in &topics {
        let mut dest = dest_for(&brokers, topic);
        hand(&mut dest, &[]);
        dest.flush().unwrap();
        let mut probe = KafkaSource::new(
            &KafkaSourceConfig {
                service: smeltr::plan::topology::KafkaService::Kafka,
                encoding: Encoding::Auto,
                fetch_bytes: 64 * 1024 * 1024,
                brokers: brokers.clone(),
                topics: vec![topic.clone()],
                group_id: unique("probe"),
                tls: None,
                start: StartPosition::Earliest,
                stop: None,
                deployed_at: 0,
                member: None,
                rack: None,
                props: Default::default(),
                avro: None,
            },
            &Default::default(),
        )
        .unwrap();
        // Subscribing to a topic that does not exist yet asks the broker to create it.
        let deadline = Instant::now() + Duration::from_secs(30);
        while probe.assigned_partitions().is_empty() {
            probe.poll_batch(1, 1 << 20, Duration::from_millis(100)).unwrap();
            assert!(Instant::now() < deadline, "{topic} was never created");
        }
    }
    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: brokers.clone(),
        topics: topics.to_vec(),
        group_id: unique("blocked-group"),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };
    let mut first = KafkaSource::new(&config, &Default::default()).unwrap();
    let mut second = KafkaSource::new(&config, &Default::default()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while first.assigned_partitions().len() != 1 || second.assigned_partitions().len() != 1 {
        first.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap();
        second.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap();
        assert!(Instant::now() < deadline, "the two members never split the partitions");
    }

    first.block().unwrap();
    for topic in &topics {
        let mut dest = dest_for(&brokers, topic);
        let bodies: Vec<String> = (0..20).map(|i| format!("row-{i}")).collect();
        let rows: Vec<OutgoingRecord<'_>> = bodies.iter().map(|b| OutgoingRecord::new(b.as_bytes())).collect();
        hand(&mut dest, &rows);
        dest.flush().unwrap();
    }
    // The second member leaves, and its partition moves to the blocked one.
    drop(second);

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut supplied = 0;
    while first.assigned_partitions().len() != 2 {
        supplied += first.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap().len();
        assert!(
            Instant::now() < deadline,
            "the blocked member never took the other partition"
        );
    }
    // Long enough for an unpaused partition to fetch.
    let settle = Instant::now() + Duration::from_secs(5);
    while Instant::now() < settle {
        supplied += first.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap().len();
    }
    assert_eq!(supplied, 0, "a blocked source supplied records");

    first.release().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut after = 0;
    while after < 40 {
        after += first
            .poll_batch(100, 1 << 20, Duration::from_millis(100))
            .unwrap()
            .len();
        assert!(Instant::now() < deadline, "released, it read {after} of 40");
    }
}

/// A partition the group moves to another member is reported revoked by the member that lost it, so the reader can
/// stop any batch read from it before the move from committing there.
#[test]
fn live_a_partition_moved_to_another_member_is_reported_revoked() {
    let Some(brokers) = live_brokers("live_a_partition_moved_to_another_member_is_reported_revoked") else {
        return;
    };
    // Two single-partition topics, so a group of two has a partition each to share.
    let topics = [unique("revoked-a"), unique("revoked-b")];
    for topic in &topics {
        let mut dest = dest_for(&brokers, topic);
        hand(&mut dest, &[OutgoingRecord::new(b"row")]);
        dest.flush().unwrap();
    }
    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: 64 * 1024 * 1024,
        brokers: brokers.clone(),
        topics: topics.to_vec(),
        group_id: unique("revoked-group"),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    };
    let mut first = KafkaSource::new(&config, &Default::default()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while first.assigned_partitions().len() != 2 {
        first.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap();
        assert!(Instant::now() < deadline, "the first member never took both partitions");
    }
    assert!(first.take_revoked().is_empty(), "nothing moved yet");

    let mut second = KafkaSource::new(&config, &Default::default()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut revoked = Vec::new();
    while second.assigned_partitions().is_empty() {
        first.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap();
        second.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap();
        revoked.extend(first.take_revoked());
        assert!(Instant::now() < deadline, "the second member never got a partition");
    }
    let moved = second.assigned_partitions();
    let revoked: Vec<(String, i32)> = revoked.iter().map(|tp| (tp.topic.to_string(), tp.partition)).collect();
    assert_eq!(revoked, moved, "what the first member lost is what the second took");
}

/// A record's headers reach `SourceRecord::headers` once the source is asked to keep them, and not before. A header
/// with no value is left out.
#[test]
fn live_headers_are_kept_once_asked_for() {
    use rdkafka::ClientConfig as RawClientConfig;
    use rdkafka::message::{Header, OwnedHeaders};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use rdkafka::util::Timeout as RawTimeout;

    let Some(brokers) = live_brokers("live_headers_are_kept_once_asked_for") else {
        return;
    };
    let topic = unique("headers");
    let producer: BaseProducer = RawClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    for payload in ["first", "second"] {
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "trace",
                value: Some("t1"),
            })
            .insert(Header::<&str> {
                key: "empty",
                value: None,
            });
        producer
            .send(BaseRecord::<(), str>::to(&topic).payload(payload).headers(headers))
            .unwrap();
    }
    producer.flush(RawTimeout::Never).unwrap();

    let mut source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers,
            topics: vec![topic],
            group_id: unique("group"),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();

    let first = drain_at_least(&mut source, 1, Duration::from_secs(15));
    assert_eq!(first.len(), 1, "one at a time, so the second is read after asking");
    assert!(first[0].headers.is_none(), "not asked for yet");
    source.keep_headers(true);
    let second = drain_at_least(&mut source, 1, Duration::from_secs(15));
    assert_eq!(second[0].headers, Some(vec![("trace".to_string(), b"t1".to_vec())]));
}

/// Writes `count` records of `size` bytes to `topic`, a single-partition topic, each with one header, `i`, holding
/// its offset.
fn seed_with_headers(brokers: &str, topic: &str, count: usize, size: usize) {
    use rdkafka::ClientConfig as RawClientConfig;
    use rdkafka::message::{Header, OwnedHeaders};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use rdkafka::util::Timeout as RawTimeout;

    let producer: BaseProducer = RawClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("queue.buffering.max.kbytes", "1048576")
        .create()
        .unwrap();
    let payload = vec![b'x'; size];
    for i in 0..count {
        let headers = OwnedHeaders::new().insert(Header {
            key: "i",
            value: Some(i.to_string().as_bytes()),
        });
        producer
            .send(BaseRecord::<(), [u8]>::to(topic).payload(&payload).headers(headers))
            .unwrap();
        if i % 1000 == 999 {
            producer.poll(Duration::ZERO);
        }
    }
    producer.flush(RawTimeout::Never).unwrap();
}

/// A source for these header tests: from the start of `topics`, `fetch_bytes` at a time, in a group of its own.
fn header_source_config(brokers: &str, topics: &[String], fetch_bytes: usize) -> KafkaSourceConfig {
    KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes,
        brokers: brokers.to_string(),
        topics: topics.to_vec(),
        group_id: unique("headers-group"),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: Default::default(),
        avro: None,
    }
}

/// Reads `source` until it has `want` records, in batches of at most `batch`.
fn read_exactly(source: &mut KafkaSource, want: usize, batch: usize) -> Vec<smeltr::io::SourceRecord> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut out = Vec::new();
    while out.len() < want {
        assert!(Instant::now() < deadline, "read {} of {want} records", out.len());
        let mut got = source
            .poll_batch(batch.min(want - out.len()), usize::MAX, Duration::from_millis(500))
            .unwrap();
        out.append(&mut got);
    }
    out
}

/// Asserts `records` read from `topic` are each of its offsets in `from..to` exactly once, in order, each carrying
/// its header when `headers` and none otherwise.
fn assert_read(records: &[smeltr::io::SourceRecord], topic: &str, from: i64, to: i64, headers: bool) {
    let mine: Vec<&smeltr::io::SourceRecord> = records.iter().filter(|r| r.topic.as_ref() == topic).collect();
    let offsets: Vec<i64> = mine.iter().map(|r| r.offset).collect();
    assert_eq!(
        offsets,
        (from..to).collect::<Vec<_>>(),
        "{topic}: each offset once, in order"
    );
    for r in mine {
        if headers {
            assert_eq!(
                r.headers,
                Some(vec![("i".to_string(), r.offset.to_string().into_bytes())]),
                "{topic}@{} carries its own header",
                r.offset
            );
        } else {
            assert_eq!(
                r.headers, None,
                "{topic}@{} was read before headers were asked for",
                r.offset
            );
        }
    }
}

/// Lets the client fill the source's inbox before the case under test.
fn let_the_inbox_fill() {
    std::thread::sleep(Duration::from_secs(2));
}

/// Headers asked for mid-read reach every record not read yet, across several partitions, each record exactly once:
/// those already in the inbox are fetched again with theirs.
#[test]
fn live_headers_asked_for_mid_read_reach_every_record_not_read_yet() {
    let Some(brokers) = live_brokers("live_headers_asked_for_mid_read_reach_every_record_not_read_yet") else {
        return;
    };
    let topics: Vec<String> = (0..3).map(|i| unique(&format!("headers-mid-{i}"))).collect();
    for topic in &topics {
        seed_with_headers(&brokers, topic, 300, 100);
    }
    let mut source = KafkaSource::new(
        &header_source_config(&brokers, &topics, 64 * 1024 * 1024),
        &Default::default(),
    )
    .unwrap();

    let before = read_exactly(&mut source, 60, 60);
    let_the_inbox_fill();
    source.keep_headers(true);
    let after = read_exactly(&mut source, 900 - 60, 100);

    let read_before = |topic: &str| before.iter().filter(|r| r.topic.as_ref() == topic).count() as i64;
    for topic in &topics {
        let split = read_before(topic);
        assert_read(&before, topic, 0, split, false);
        assert_read(&after, topic, split, 300, true);
    }
    assert!(
        source
            .poll_batch(10, usize::MAX, Duration::from_secs(1))
            .unwrap()
            .is_empty(),
        "nothing is read twice"
    );
}

/// An inbox full to its cap, its partitions paused, is fetched again when headers are asked for, and reads on to the
/// end.
#[test]
fn live_headers_asked_for_with_a_full_inbox_reach_every_record_not_read_yet() {
    let Some(brokers) = live_brokers("live_headers_asked_for_with_a_full_inbox_reach_every_record_not_read_yet") else {
        return;
    };
    let topic = unique("headers-full");
    // Three MiB, past the two an inbox of the smallest fetch holds before it pauses.
    seed_with_headers(&brokers, &topic, 3000, 1024);
    let mut source = KafkaSource::new(
        &header_source_config(&brokers, std::slice::from_ref(&topic), 4 * 1024 * 1024),
        &Default::default(),
    )
    .unwrap();

    let before = read_exactly(&mut source, 10, 10);
    let_the_inbox_fill();
    source.keep_headers(true);
    let after = read_exactly(&mut source, 2990, 500);

    assert_read(&before, &topic, 0, 10, false);
    assert_read(&after, &topic, 10, 3000, true);
}

/// Headers asked for while the source is blocked reach every record it reads once released.
#[test]
fn live_headers_asked_for_while_blocked_reach_every_record_read_once_released() {
    let Some(brokers) = live_brokers("live_headers_asked_for_while_blocked_reach_every_record_read_once_released")
    else {
        return;
    };
    let topic = unique("headers-blocked");
    seed_with_headers(&brokers, &topic, 500, 100);
    let mut source = KafkaSource::new(
        &header_source_config(&brokers, std::slice::from_ref(&topic), 64 * 1024 * 1024),
        &Default::default(),
    )
    .unwrap();

    let before = read_exactly(&mut source, 10, 10);
    let_the_inbox_fill();
    source.block().unwrap();
    source.keep_headers(true);
    std::thread::sleep(Duration::from_secs(1));
    source.release().unwrap();
    let after = read_exactly(&mut source, 490, 100);

    assert_read(&before, &topic, 0, 10, false);
    assert_read(&after, &topic, 10, 500, true);
}

/// Headers turned on, off and on again: each record is read once, with its header exactly while they are asked for.
#[test]
fn live_headers_turned_on_off_and_on_again_follow_each_switch() {
    let Some(brokers) = live_brokers("live_headers_turned_on_off_and_on_again_follow_each_switch") else {
        return;
    };
    let topic = unique("headers-toggle");
    seed_with_headers(&brokers, &topic, 400, 100);
    let mut source = KafkaSource::new(
        &header_source_config(&brokers, std::slice::from_ref(&topic), 64 * 1024 * 1024),
        &Default::default(),
    )
    .unwrap();

    let first = read_exactly(&mut source, 10, 10);
    let_the_inbox_fill();
    source.keep_headers(true);
    let second = read_exactly(&mut source, 90, 90);
    source.keep_headers(false);
    // Read without headers, from what the inbox already held with them.
    let third = read_exactly(&mut source, 100, 100);
    let_the_inbox_fill();
    source.keep_headers(true);
    let fourth = read_exactly(&mut source, 200, 100);

    assert_read(&first, &topic, 0, 10, false);
    assert_read(&second, &topic, 10, 100, true);
    let third_offsets: Vec<i64> = third.iter().map(|r| r.offset).collect();
    assert_eq!(
        third_offsets,
        (100..200).collect::<Vec<_>>(),
        "each offset once, in order"
    );
    assert_read(&fourth, &topic, 200, 400, true);
}

/// Headers asked for while records are still being written reach every record read after, each record once.
#[test]
fn live_headers_asked_for_while_records_arrive_reach_every_record_read_after() {
    let Some(brokers) = live_brokers("live_headers_asked_for_while_records_arrive_reach_every_record_read_after")
    else {
        return;
    };
    let topic = unique("headers-arriving");
    seed_with_headers(&brokers, &topic, 1, 100);
    let mut source = KafkaSource::new(
        &header_source_config(&brokers, std::slice::from_ref(&topic), 64 * 1024 * 1024),
        &Default::default(),
    )
    .unwrap();
    let before = read_exactly(&mut source, 1, 1);
    let writer = {
        let (brokers, topic) = (brokers.clone(), topic.clone());
        // Offsets 1.. carry `i` = 0.., so the header is told from the offset by one.
        std::thread::spawn(move || {
            for _ in 0..20 {
                seed_with_headers(&brokers, &topic, 100, 100);
                std::thread::sleep(Duration::from_millis(50));
            }
        })
    };
    let mut during = read_exactly(&mut source, 500, 50);
    source.keep_headers(true);
    let after = read_exactly(&mut source, 1500, 50);
    writer.join().unwrap();

    during.insert(0, before.into_iter().next().unwrap());
    let offsets: Vec<i64> = during.iter().chain(&after).map(|r| r.offset).collect();
    assert_eq!(offsets, (0..2001).collect::<Vec<_>>(), "each offset once, in order");
    assert!(
        during.iter().all(|r| r.headers.is_none()),
        "read before headers were asked for"
    );
    for r in &after {
        assert_eq!(
            r.headers,
            Some(vec![("i".to_string(), ((r.offset - 1) % 100).to_string().into_bytes())]),
            "@{} carries its own header",
            r.offset
        );
    }
}

/// Of two sources sharing a client, headers asked for by one are fetched again for that one alone: the other reads
/// on, each record once, without them.
#[test]
fn live_headers_asked_for_by_one_source_on_a_shared_client_leave_the_other_alone() {
    let Some(brokers) = live_brokers("live_headers_asked_for_by_one_source_on_a_shared_client_leave_the_other_alone")
    else {
        return;
    };
    let (a, b) = (unique("headers-shared-a"), unique("headers-shared-b"));
    seed_with_headers(&brokers, &a, 300, 100);
    seed_with_headers(&brokers, &b, 300, 100);
    let clients = Arc::new(smeltr::io::kafka::Clients::default());
    let config = header_source_config(&brokers, std::slice::from_ref(&a), 64 * 1024 * 1024);
    let mut first = KafkaSource::open(&config, &Default::default(), &clients).unwrap();
    let mut second = KafkaSource::open(
        &KafkaSourceConfig {
            topics: vec![b.clone()],
            ..config
        },
        &Default::default(),
        &clients,
    )
    .unwrap();

    let first_before = read_exactly(&mut first, 10, 10);
    let second_before = read_exactly(&mut second, 10, 10);
    let_the_inbox_fill();
    first.keep_headers(true);
    let first_after = read_exactly(&mut first, 290, 100);
    let second_after = read_exactly(&mut second, 290, 100);

    assert_read(&first_before, &a, 0, 10, false);
    assert_read(&first_after, &a, 10, 300, true);
    assert_read(&second_before, &b, 0, 10, false);
    assert_read(&second_after, &b, 10, 300, false);
}

/// A source that names a topic pattern reads every topic whose whole name matches, including one created after it
/// started, each record once, and nothing from a topic the pattern does not match.
#[test]
fn live_a_topic_pattern_reads_every_matching_topic_and_those_created_later() {
    let Some(brokers) = live_brokers("live_a_topic_pattern_reads_every_matching_topic_and_those_created_later") else {
        return;
    };
    let prefix = unique("wild");
    let [a, b, later, other] = ["a", "b", "later", "x"].map(|t| format!("{prefix}-{t}"));
    seed_with_headers(&brokers, &a, 50, 10);
    seed_with_headers(&brokers, &b, 50, 10);
    seed_with_headers(&brokers, &format!("x{prefix}-a"), 50, 10);
    let pattern = format!("^{}-(a|b|later)", regex::escape(&prefix));
    let mut source = KafkaSource::new(
        &header_source_config(&brokers, &[pattern, other.clone()], 64 * 1024 * 1024),
        &Default::default(),
    )
    .unwrap();

    let first = read_exactly(&mut source, 100, 100);
    assert_read(&first, &a, 0, 50, false);
    assert_read(&first, &b, 0, 50, false);

    // Created now, after the source's first listing: read once the next listing finds it.
    seed_with_headers(&brokers, &later, 50, 10);
    seed_with_headers(&brokers, &other, 50, 10);
    let second = read_exactly(&mut source, 100, 100);
    assert_read(&second, &later, 0, 50, false);
    assert_read(&second, &other, 0, 50, false);
    assert!(
        source
            .poll_batch(10, usize::MAX, Duration::from_secs(2))
            .unwrap()
            .is_empty(),
        "nothing more: a topic the pattern does not match whole is not read"
    );
}

/// Two sources whose patterns match different topics share a client and each reads its own.
#[test]
fn live_topic_patterns_on_a_shared_client_each_read_their_own_topics() {
    let Some(brokers) = live_brokers("live_topic_patterns_on_a_shared_client_each_read_their_own_topics") else {
        return;
    };
    let prefix = unique("wild-shared");
    let [a, b] = ["a", "b"].map(|t| format!("{prefix}-{t}"));
    seed_with_headers(&brokers, &a, 50, 10);
    seed_with_headers(&brokers, &b, 50, 10);
    let escaped = regex::escape(&prefix);
    let clients = Arc::new(smeltr::io::kafka::Clients::default());
    let config = header_source_config(&brokers, &[format!("^{escaped}-a")], 64 * 1024 * 1024);
    let mut first = KafkaSource::open(&config, &Default::default(), &clients).unwrap();
    let mut second = KafkaSource::open(
        &KafkaSourceConfig {
            topics: vec![format!("^{escaped}-b")],
            ..config
        },
        &Default::default(),
        &clients,
    )
    .unwrap();

    let from_first = read_exactly(&mut first, 50, 50);
    let from_second = read_exactly(&mut second, 50, 50);
    assert_read(&from_first, &a, 0, 50, false);
    assert_read(&from_second, &b, 0, 50, false);
}

/// Two patterns on a shared client that both match one topic are both read: the source that matched it first reads
/// it, and the other reads the rest of what it matches.
#[test]
fn live_overlapping_topic_patterns_on_a_shared_client_leave_a_shared_topic_to_one() {
    let Some(brokers) = live_brokers("live_overlapping_topic_patterns_on_a_shared_client_leave_a_shared_topic_to_one")
    else {
        return;
    };
    let prefix = unique("wild-overlap");
    let [a, both, b] = ["a", "both", "b"].map(|t| format!("{prefix}-{t}"));
    for topic in [&a, &both, &b] {
        seed_with_headers(&brokers, topic, 50, 10);
    }
    let escaped = regex::escape(&prefix);
    let clients = Arc::new(smeltr::io::kafka::Clients::default());
    let config = header_source_config(&brokers, &[format!("^{escaped}-(a|both)")], 64 * 1024 * 1024);
    let mut first = KafkaSource::open(&config, &Default::default(), &clients).unwrap();
    let mut second = KafkaSource::open(
        &KafkaSourceConfig {
            topics: vec![format!("^{escaped}-(both|b)")],
            ..config
        },
        &Default::default(),
        &clients,
    )
    .unwrap();

    let from_first = read_exactly(&mut first, 100, 100);
    let from_second = read_exactly(&mut second, 50, 50);
    assert_read(&from_first, &a, 0, 50, false);
    assert_read(&from_first, &both, 0, 50, false);
    assert_read(&from_second, &b, 0, 50, false);
    assert!(
        second
            .poll_batch(100, usize::MAX, Duration::from_secs(2))
            .unwrap()
            .is_empty(),
        "the topic the first source reads is not read again by the second"
    );
}

/// The emulator's published connection string, the same on every install.
const EVENT_HUBS_EMULATOR: &str = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;\
SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

/// An `eventHubs` source, loaded from a config file, reads what was sent to its event hub and commits it. The
/// hub and its consumer group are the emulator's fixed ones (`tests/fixtures/event-hubs-emulator.json`), shared by
/// every run, so the test looks for the events this run sent.
#[test]
fn live_an_event_hubs_source_reads_and_commits_through_the_kafka_endpoint() {
    let Some(namespace) = live::event_hubs("live_an_event_hubs_source_reads_and_commits_through_the_kafka_endpoint")
    else {
        return;
    };
    let hub = "smeltr-live";
    let json = serde_json::json!({
        "version": 1,
        "sources": [{"id": "azure-logs", "type": "eventHubs", "namespace": namespace, "eventHubs": [hub],
            "consumerGroup": "smeltr", "connectionString": {"secret": "event-hubs", "version": 1}}],
        "destinations": [{"id": "es", "type": "elasticsearch", "url": "https://es.internal:9200", "defaultIndex": "logs"}],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "azure-logs", "pipeline": "p", "deliver": [{"to": ["es"]}]}]
    });
    let system = smeltr::config_file::load_str(&json.to_string()).unwrap();
    let smeltr::plan::topology::SourceKind::Kafka(config) = &system.topology.sources[0].kind else {
        panic!("an eventHubs source loads as a Kafka source");
    };
    let mut secrets = smeltr::plan::topology::HeldSecrets::default();
    secrets.set_stored([("event-hubs".to_string(), 1, EVENT_HUBS_EMULATOR.to_string())]);

    // Sent with the same settings the source signs in with, the password written out.
    let mut props = config.props.clone();
    props.credentials.clear();
    props.literal.push(("sasl.password".into(), EVENT_HUBS_EMULATOR.into()));
    let mut dest = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            stall_after: Duration::from_secs(30),
            brokers: config.brokers.clone(),
            default_topic: hub.to_string(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 64 * 1024 * 1024,
            props,
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    )
    .unwrap();
    let run = unique("event");
    let sent: Vec<String> = (0..3).map(|i| format!("{run}-{i}")).collect();
    let rows: Vec<OutgoingRecord<'_>> = sent.iter().map(|m| OutgoingRecord::new(m.as_bytes())).collect();
    hand(&mut dest, &rows);
    dest.flush().unwrap();

    let mut source = KafkaSource::new(config, &secrets).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut seen = Vec::new();
    let mut commit = CommitMap::new();
    while seen.len() < sent.len() {
        assert!(Instant::now() < deadline, "read {seen:?} of {sent:?}");
        for r in source.poll_batch(100, 10_000_000, Duration::from_secs(5)).unwrap() {
            let raw = String::from_utf8_lossy(r.bytes().unwrap()).into_owned();
            if raw.starts_with(&run) {
                seen.push(raw);
            }
            commit.insert(TopicPartition::new(Arc::from(hub), r.partition), r.offset);
        }
    }
    seen.sort();
    assert_eq!(seen, sent);
    source.commit(&commit).unwrap();
}

/// Commits `offset` for `topic`'s partition 0 under `config`'s group with a metadata string, and answers the
/// metadata the broker gives back with it.
fn commit_metadata_round_trip(config: &rdkafka::ClientConfig, topic: &str, offset: i64) -> String {
    use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
    use rdkafka::{Offset, TopicPartitionList};
    let consumer: BaseConsumer = config.create().unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Offset(offset)).unwrap();
    tpl.elements_for_topic(topic)[0].set_metadata(TAG);
    consumer.commit(&tpl, CommitMode::Sync).unwrap();
    let mut query = TopicPartitionList::new();
    query.add_partition(topic, 0);
    let committed = consumer.committed_offsets(query, Duration::from_secs(30)).unwrap();
    let e = &committed.elements()[0];
    assert_eq!(e.offset(), Offset::Offset(offset));
    e.metadata().to_string()
}

/// A metadata string as a source commits one: the `start` its offset descends from, and its deployed commit's time.
const TAG: &str = "smeltr-start=ts:1791763200000;at=1791849600";

/// Which `start` a committed offset descends from is the metadata committed with it, so the broker must keep that
/// string and give it back.
#[test]
fn live_a_committed_offset_keeps_its_metadata() {
    let Some(brokers) = live_brokers("live_a_committed_offset_keeps_its_metadata") else {
        return;
    };
    let topic = unique("metadata");
    let mut dest = dest_for(&brokers, &topic);
    hand(&mut dest, &[OutgoingRecord::new(b"one")]);
    dest.flush().unwrap();
    let mut config = rdkafka::ClientConfig::new();
    config
        .set("bootstrap.servers", &brokers)
        .set("group.id", unique("metadata-group"));
    assert_eq!(commit_metadata_round_trip(&config, &topic, 1), TAG);
}

/// The same, through Event Hubs' Kafka endpoint. The emulator's group is shared with every run, so the offset it
/// already holds is committed again rather than moved.
#[test]
fn live_event_hubs_keeps_a_committed_offsets_metadata() {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    let Some(namespace) = live::event_hubs("live_event_hubs_keeps_a_committed_offsets_metadata") else {
        return;
    };
    let hub = "smeltr-live";
    let mut config = rdkafka::ClientConfig::new();
    config
        .set("bootstrap.servers", &namespace)
        .set("group.id", "smeltr-reset")
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "PLAIN")
        .set("sasl.username", "$ConnectionString")
        .set("sasl.password", EVENT_HUBS_EMULATOR);
    let consumer: BaseConsumer = config.create().unwrap();
    let mut query = rdkafka::TopicPartitionList::new();
    query.add_partition(hub, 0);
    let held = match consumer
        .committed_offsets(query, Duration::from_secs(30))
        .unwrap()
        .elements()[0]
        .offset()
    {
        rdkafka::Offset::Offset(v) => v,
        _ => 0,
    };
    drop(consumer);
    assert_eq!(commit_metadata_round_trip(&config, hub, held), TAG);
}

/// What `group` has committed for `topic`'s partition 0: the offset and its metadata.
fn committed_at(config: &rdkafka::ClientConfig, topic: &str) -> (rdkafka::Offset, String) {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    let asker: BaseConsumer = config.create().unwrap();
    let mut query = rdkafka::TopicPartitionList::new();
    query.add_partition(topic, 0);
    let committed = asker.committed_offsets(query, Duration::from_secs(30)).unwrap();
    let e = &committed.elements()[0];
    (e.offset(), e.metadata().to_string())
}

/// Reads everything `source` delivers for `how_long`, committing it, and answers the offsets it read.
fn read_committing(source: &mut KafkaSource, topic: &str, how_long: Duration) -> Vec<i64> {
    let deadline = Instant::now() + how_long;
    let mut offsets = Vec::new();
    while Instant::now() < deadline {
        let batch = source.poll_batch(100, 10_000_000, Duration::from_millis(200)).unwrap();
        let mut commit = CommitMap::new();
        for r in &batch {
            commit.insert(TopicPartition::new(Arc::from(topic), r.partition), r.offset);
            offsets.push(r.offset);
        }
        if !commit.is_empty() {
            source.commit(&commit).unwrap();
        }
    }
    offsets
}

/// A `start` changed to a time, in a newer deployed commit, rereads a partition the group had read to its end from
/// the first record at or after that time, once: a later source on the same commit reads on from what it committed.
#[test]
fn live_a_start_changed_to_a_time_rereads_from_there_once() {
    let Some(brokers) = live_brokers("live_a_start_changed_to_a_time_rereads_from_there_once") else {
        return;
    };
    let topic = unique("start-time");
    let group = unique("start-time-group");
    let mut dest = dest_for(&brokers, &topic);
    hand(
        &mut dest,
        &[
            OutgoingRecord::new(b"a"),
            OutgoingRecord::new(b"b"),
            OutgoingRecord::new(b"c"),
        ],
    );
    dest.flush().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    std::thread::sleep(Duration::from_millis(50));
    hand(&mut dest, &[OutgoingRecord::new(b"d"), OutgoingRecord::new(b"e")]);
    dest.flush().unwrap();

    let config = |start, deployed_at| KafkaSourceConfig {
        start,
        deployed_at,
        member: None,
        rack: None,
        group_id: group.clone(),
        ..header_source_config(&brokers, std::slice::from_ref(&topic), 64 * 1024 * 1024)
    };
    let mut source = KafkaSource::new(&config(StartPosition::Earliest, 1), &Default::default()).unwrap();
    let read = read_committing(&mut source, &topic, Duration::from_secs(10));
    assert_eq!(read, [0, 1, 2, 3, 4], "the group reads to the end before the change");
    drop(source);

    let moved = config(StartPosition::Timestamp(at), 2);
    let mut source = KafkaSource::new(&moved, &Default::default()).unwrap();
    assert_eq!(read_committing(&mut source, &topic, Duration::from_secs(10)), [3, 4]);
    drop(source);
    let mut asker = rdkafka::ClientConfig::new();
    asker.set("bootstrap.servers", &brokers).set("group.id", &group);
    assert_eq!(
        committed_at(&asker, &topic),
        (rdkafka::Offset::Offset(5), format!("smeltr-start=ts:{at};at=2"))
    );

    let mut source = KafkaSource::new(&moved, &Default::default()).unwrap();
    assert!(
        read_committing(&mut source, &topic, Duration::from_secs(5)).is_empty(),
        "moved once"
    );
}

/// Rolling deploys can overlap, so a source on an older deployed commit can take a partition after one on a newer
/// commit has. A newer commit with the start the partition already has claims it at once, so the older one, with
/// another start, moves nothing; the partition moves once, not away and back.
#[test]
fn live_an_older_deploy_does_not_move_a_partition_a_newer_one_holds_at_the_same_start() {
    let Some(brokers) =
        live_brokers("live_an_older_deploy_does_not_move_a_partition_a_newer_one_holds_at_the_same_start")
    else {
        return;
    };
    let topic = unique("start-overlap");
    let group = unique("start-overlap-group");
    let mut dest = dest_for(&brokers, &topic);
    let records: Vec<_> = (0..5).map(|n| n.to_string()).collect();
    let records: Vec<_> = records.iter().map(|r| OutgoingRecord::new(r.as_bytes())).collect();
    hand(&mut dest, &records);
    dest.flush().unwrap();

    let config = |start, deployed_at| KafkaSourceConfig {
        start,
        deployed_at,
        member: None,
        rack: None,
        group_id: group.clone(),
        ..header_source_config(&brokers, std::slice::from_ref(&topic), 64 * 1024 * 1024)
    };
    let read = |start, deployed_at| {
        let mut source = KafkaSource::new(&config(start, deployed_at), &Default::default()).unwrap();
        read_committing(&mut source, &topic, Duration::from_secs(5))
    };
    // Commit 2 reads from the beginning. Commit 4 has the same start. Commit 3, between them, moved to the end.
    assert_eq!(read(StartPosition::Earliest, 2), [0, 1, 2, 3, 4]);
    assert!(read(StartPosition::Earliest, 4).is_empty(), "the same start reads on");
    assert!(
        read(StartPosition::Latest, 3).is_empty(),
        "commit 3 is older than what holds the partition"
    );
    assert!(
        read(StartPosition::Earliest, 4).is_empty(),
        "the partition never left the beginning's line"
    );
}

/// A `start` moved through Event Hubs' Kafka endpoint: to a time, under a newer deployed commit, it skips what was
/// sent before that time and reads what comes after. Its group, `smeltr-reset`, is shared with every run, so the time
/// and the deployed commit are this run's, newer than any earlier run's; it is not `smeltr`, so the source test beside
/// it keeps its partitions.
#[test]
fn live_an_event_hubs_start_moved_to_a_time_skips_what_came_before() {
    let Some(namespace) = live::event_hubs("live_an_event_hubs_start_moved_to_a_time_skips_what_came_before") else {
        return;
    };
    let hub = "smeltr-live";
    let json = serde_json::json!({
        "version": 1,
        "sources": [{"id": "azure-logs", "type": "eventHubs", "namespace": namespace, "eventHubs": [hub],
            "consumerGroup": "smeltr-reset", "connectionString": {"secret": "event-hubs", "version": 1}}],
        "destinations": [{"id": "es", "type": "elasticsearch", "url": "https://es.internal:9200", "defaultIndex": "logs"}],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "azure-logs", "pipeline": "p", "deliver": [{"to": ["es"]}]}]
    });
    let system = smeltr::config_file::load_str(&json.to_string()).unwrap();
    let smeltr::plan::topology::SourceKind::Kafka(config) = &system.topology.sources[0].kind else {
        panic!("an eventHubs source loads as a Kafka source");
    };
    let mut config = config.clone();
    let mut secrets = smeltr::plan::topology::HeldSecrets::default();
    secrets.set_stored([("event-hubs".to_string(), 1, EVENT_HUBS_EMULATOR.to_string())]);
    let mut props = config.props.clone();
    props.credentials.clear();
    props.literal.push(("sasl.password".into(), EVENT_HUBS_EMULATOR.into()));
    let mut dest = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            stall_after: Duration::from_secs(30),
            brokers: config.brokers.clone(),
            default_topic: hub.to_string(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 64 * 1024 * 1024,
            props,
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    )
    .unwrap();
    let run = unique("moved");
    let before = format!("{run}-before");
    hand(&mut dest, &[OutgoingRecord::new(before.as_bytes())]);
    dest.flush().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    config.start = StartPosition::Timestamp(now.as_millis() as i64);
    config.deployed_at = now.as_secs() as i64;

    let mut asker = rdkafka::ClientConfig::new();
    asker
        .set("bootstrap.servers", &config.brokers)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "PLAIN")
        .set("sasl.username", "$ConnectionString")
        .set("sasl.password", EVENT_HUBS_EMULATOR);
    // The group has read both partitions under an older start, so the new one is a move, not a first start.
    {
        use rdkafka::consumer::{BaseConsumer, Consumer};
        let older: BaseConsumer = asker.clone().set("group.id", "smeltr-reset").create().unwrap();
        let mut held = rdkafka::TopicPartitionList::new();
        for p in [0, 1] {
            held.add_partition_offset(hub, p, rdkafka::Offset::Offset(0)).unwrap();
            held.find_partition(hub, p)
                .unwrap()
                .set_metadata("smeltr-start=earliest;at=1");
        }
        older.commit(&held, rdkafka::consumer::CommitMode::Sync).unwrap();
    }

    let mut source = KafkaSource::new(&config, &secrets).unwrap();
    // Every partition is placed at the time, which the move commits at once, before anything more is sent.
    let tag = format!("smeltr-start=ts:{};at={}", now.as_millis(), now.as_secs());
    let placed = |asker: &rdkafka::ClientConfig| {
        use rdkafka::consumer::{BaseConsumer, Consumer};
        let consumer: BaseConsumer = asker.clone().set("group.id", "smeltr-reset").create().unwrap();
        let mut query = rdkafka::TopicPartitionList::new();
        query.add_partition(hub, 0);
        query.add_partition(hub, 1);
        let committed = consumer.committed_offsets(query, Duration::from_secs(30)).unwrap();
        committed.elements().iter().all(|e| e.metadata() == tag)
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut seen = Vec::new();
    while !placed(&asker) {
        assert!(Instant::now() < deadline, "the move was never committed");
        for r in source.poll_batch(100, 10_000_000, Duration::from_millis(500)).unwrap() {
            seen.push(String::from_utf8_lossy(r.bytes().unwrap()).into_owned());
        }
    }
    let after = format!("{run}-after");
    hand(&mut dest, &[OutgoingRecord::new(after.as_bytes())]);
    dest.flush().unwrap();
    seen.retain(|raw| raw.starts_with(&run));
    let mut commit = CommitMap::new();
    while !seen.contains(&after) {
        assert!(Instant::now() < deadline, "read {seen:?}");
        for r in source.poll_batch(100, 10_000_000, Duration::from_secs(1)).unwrap() {
            let raw = String::from_utf8_lossy(r.bytes().unwrap()).into_owned();
            if raw.starts_with(&run) {
                seen.push(raw);
            }
            commit.insert(TopicPartition::new(Arc::from(hub), r.partition), r.offset);
        }
    }
    assert_eq!(seen, [after], "what was sent before the time is skipped");
    source.commit(&commit).unwrap();
}
