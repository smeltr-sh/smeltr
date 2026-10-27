//! A certificate against a *real* broker that serves TLS and demands a client certificate.
//!
//! `config_file`'s tests establish that TLS settings are refused where they disagree with themselves, and
//! `elastic-tls-tests` proves the handshake for the connector whose TLS this code configures directly. Neither can
//! reach what actually breaks here: whether librdkafka accepts the inline PEM properties the TLS settings synthesize,
//! reads a key out of a string rather than a file, and completes the handshake. A build whose librdkafka was
//! compiled without `ssl.certificate.pem` passes every other test in the suite and fails this one.
//!
//! `tests/tls/up.sh` provides the broker. Under `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips
//! -- see `tests/live/mod.rs`.

use crate::live;

use std::sync::Arc;
use std::time::Duration;

use smeltr::format::encoding::Encoding;
use smeltr::io::kafka::{KafkaDestination, KafkaSource, total_partitions};
use smeltr::io::{Destination, OutgoingRecord, OwnedRecord};
use smeltr::plan::topology::{HeldSecrets, KafkaDestinationConfig, KafkaProps, KafkaSourceConfig, StartPosition, Tls};
use std::borrow::Cow;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};

/// Hand `rows` to `dest` as one batch.
fn hand(dest: &mut dyn Destination, rows: &[OutgoingRecord<'_>]) {
    let rows: Vec<OwnedRecord> = rows.iter().map(OwnedRecord::from).collect();
    dest.deliver(0, Cow::Owned(rows)).unwrap();
}

/// The name the certificate is held under.
const CERTIFICATE: &str = "smeltr-tls-client";

/// What `up.sh` wrote, as TLS settings and the secrets a worker would hold for them.
///
/// `None` when the rig is not up, which `live::service` has already decided is a skip or a failure.
fn rig(test_name: &str) -> Option<(String, Tls, HeldSecrets)> {
    let brokers = live::service(test_name, "KAFKA_TLS_BROKERS", "a TLS Kafka broker (`just tls-up`)")?;
    let read = |var: &str| {
        let path = live::service(test_name, var, "a certificate from `just tls-up`")?;
        Some(std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}")))
    };
    let certificate = read("KAFKA_TLS_CLIENT_CERT")?;
    let key = read("KAFKA_TLS_CLIENT_KEY")?;
    let authorities = read("KAFKA_TLS_CA")?;

    let mut secrets = HeldSecrets::default();
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: CERTIFICATE.to_string(),
        version: 1,
        value: key,
        chain: Some(certificate),
    }]);
    let tls = Tls {
        of: "source \"acme\"".into(),
        certificate: Some(CERTIFICATE.to_string()),
        authorities: Some(authorities),
        trust_public: true,
        side: smeltr::io::trust::Side::Client,
    };
    Some((brokers, tls, secrets))
}

fn metrics() -> smeltr::metrics::MetricsHandle {
    smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "tls-test")
}

fn topic() -> String {
    std::env::var("KAFKA_TLS_TOPIC").unwrap_or_else(|_| "smeltr-tls-tests".to_string())
}

/// `security.protocol` is the operator's: the TLS settings say what this client presents and trusts, never
/// whether SASL is also in play. Save-time validation refuses them without it.
fn tls_props() -> KafkaProps {
    KafkaProps {
        literal: vec![("security.protocol".into(), "SSL".into())],
        credentials: Vec::new(),
    }
}

fn source(brokers: &str, tls: Option<Tls>) -> KafkaSourceConfig {
    KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        fetch_bytes: smeltr::config_file::FetchBytes::MIN,
        brokers: brokers.to_string(),
        topics: vec![topic()],
        group_id: "smeltr-tls-tests".to_string(),
        tls,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: tls_props(),
        avro: None,
    }
}

/// Through smeltr's own path, not a hand-built client: `KafkaSource::new` and `total_partitions` each configure
/// their own client, and a regression that dropped the TLS settings from one of them would connect as nobody while a
/// test that assembled its own `ClientConfig` still passed.
///
/// `total_partitions` decides how many readers a source gets, so a source it cannot count is one the deployment
/// mis-sizes.
#[test]
fn a_source_under_a_certificate_connects_through_the_paths_deploy_uses() {
    let name = "a_source_under_a_certificate_connects_through_the_paths_deploy_uses";
    let Some((brokers, tls, secrets)) = rig(name) else {
        return;
    };
    let config = source(&brokers, Some(tls));

    // `up.sh` gave the topic two partitions: a refused connection returns `None`, and an auto-created topic would
    // have one, so the count is what tells a real answer from either.
    assert_eq!(
        total_partitions(&config, &secrets),
        Some(2),
        "counting partitions needs a completed mutual-TLS handshake"
    );
    KafkaSource::new(&config, &secrets).expect("open a source under a certificate");
}

/// The producer is the other call site that configures a client, and the reason both go through one function. A
/// destination builds even when the broker never answers, so only a delivered record proves the handshake.
#[test]
fn a_destination_under_a_certificate_delivers_to_a_broker_that_demands_one() {
    let name = "a_destination_under_a_certificate_delivers_to_a_broker_that_demands_one";
    let Some((brokers, tls, secrets)) = rig(name) else {
        return;
    };
    let config = KafkaDestinationConfig {
        payload: smeltr::plan::config::Payload::Record,
        tls: Some(tls),
        brokers,
        props: tls_props(),
        default_topic: topic(),
        service: smeltr::plan::topology::KafkaService::Kafka,
        compression: smeltr::plan::topology::Compression::None,
        acks: smeltr::plan::topology::Acks::All,
        // `flush` fails once a record sits undelivered this long. Short, so a refused handshake fails fast.
        stall_after: Duration::from_secs(10),
        buffer_bytes: 16 * 1024 * 1024,
        avro: None,
    };
    let mut dest = KafkaDestination::new(&config, 0, metrics(), &secrets).expect("open a destination");
    hand(&mut dest, &[OutgoingRecord::new(b"under-a-certificate")]);
    dest.flush()
        .expect("delivering a record needs a completed mutual-TLS handshake");
}

/// Without this the test above could pass against a broker that asked for no certificate, which is the one way a
/// mutual-TLS suite quietly stops testing what it is named for. The client still trusts the broker's authority, so
/// what it lacks is only the certificate to present.
#[test]
fn a_client_presenting_no_certificate_is_refused() {
    let name = "a_client_presenting_no_certificate_is_refused";
    let Some((brokers, tls, _)) = rig(name) else {
        return;
    };

    let consumer: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "smeltr-tls-tests-anon")
        .set("security.protocol", "SSL")
        .set("ssl.ca.pem", tls.authorities.as_deref().unwrap_or_default())
        .create()
        .expect("build a client that presents nothing");

    assert!(
        consumer.fetch_metadata(None, Duration::from_secs(10)).is_err(),
        "a client presenting no certificate reached a broker that is supposed to demand one"
    );
}

/// TLS settings that present nothing are a real configuration -- a far end asking for no certificate needs only the
/// authorities -- and this broker is not that far end. Through smeltr's own path, so what is refused is the
/// deployment's own way of connecting rather than a hand-built client's.
#[test]
fn trust_alone_is_refused_by_a_broker_that_demands_a_certificate() {
    let name = "trust_alone_is_refused_by_a_broker_that_demands_a_certificate";
    let Some((brokers, tls, _)) = rig(name) else {
        return;
    };
    let trust_only = Tls {
        of: "source \"trust-only\"".into(),
        certificate: None,
        authorities: tls.authorities,
        trust_public: true,
        side: smeltr::io::trust::Side::Client,
    };

    assert_eq!(
        total_partitions(&source(&brokers, Some(trust_only)), &HeldSecrets::default()),
        None,
        "a client presenting no certificate counted partitions on a broker that demands one"
    );
}
