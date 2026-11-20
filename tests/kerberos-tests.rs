//! Kerberos against a *real* KDC and a broker that demands GSSAPI.
//!
//! `io::kafka`'s unit tests establish that this build reports
//! `sasl_gssapi` and that the config surface agrees with it. Neither can
//! reach what actually breaks: whether librdkafka selects a provider,
//! gets a ticket from the keytab without spawning `kinit`, and completes
//! the handshake. A build that failed to link Cyrus passes every other
//! test in the suite and fails here.
//!
//! `tests/kerberos/up.sh` provides the services. Under
//! `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips --
//! see `tests/live/mod.rs`.

use crate::live;

use smeltr::format::encoding::Encoding;
use smeltr::io::kafka::{KafkaSource, total_partitions};
use smeltr::plan::topology::{KafkaProps, KafkaSourceConfig, StartPosition};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use std::time::Duration;

/// Every SASL property a Kerberized source needs, as an operator would
/// write them: passthrough properties on the source, not something smeltr
/// sets. smeltr gets the ticket from the keytab itself.
fn gssapi_props(keytab: &str) -> KafkaProps {
    KafkaProps {
        literal: vec![
            ("security.protocol".into(), "SASL_PLAINTEXT".into()),
            ("sasl.mechanism".into(), "GSSAPI".into()),
            ("sasl.kerberos.service.name".into(), "kafka".into()),
            ("sasl.kerberos.principal".into(), "smeltrclient".into()),
            ("sasl.kerberos.keytab".into(), keytab.into()),
        ],
        credentials: Vec::new(),
    }
}

/// Through smeltr's own path, not a hand-built client: `KafkaSource::new`
/// and `total_partitions` each apply `props` themselves, and a regression
/// that dropped a `sasl.*` key there would authenticate as nobody while a
/// test that assembled its own `ClientConfig` still passed.
///
/// `total_partitions` decides how many readers a source gets, so a
/// Kerberized source it cannot count is one the deployment mis-sizes.
#[test]
fn a_gssapi_source_authenticates_through_the_paths_deploy_uses() {
    let name = "a_gssapi_source_authenticates_through_the_paths_deploy_uses";
    let Some(brokers) = live::service(
        name,
        "KAFKA_GSSAPI_BROKERS",
        "a Kerberized Kafka broker (`just kerberos-up`)",
    ) else {
        return;
    };
    let Some(keytab) = live::service(name, "KAFKA_GSSAPI_KEYTAB", "a client keytab (`just kerberos-up`)") else {
        return;
    };

    let config = KafkaSourceConfig {
        service: smeltr::plan::topology::KafkaService::Kafka,
        encoding: Encoding::Auto,
        // The least a config may ask for. A fetch is half of it, and librdkafka refuses a fetch cap below its
        // message cap.
        fetch_bytes: 4 * 1024 * 1024,
        brokers,
        topics: vec!["smeltr-kerberos-tests".to_string()],
        group_id: "smeltr-kerberos-tests".to_string(),
        tls: None,
        start: StartPosition::Earliest,
        stop: None,
        deployed_at: 0,
        member: None,
        rack: None,
        props: gssapi_props(&keytab),
        avro: None,
    };

    // `up.sh` gave the topic two partitions: an unauthenticated request
    // returns `Some(0)` rather than an error, so a count is what tells a
    // real answer from a refused one.
    assert_eq!(
        total_partitions(&config, &Default::default()),
        Some(2),
        "counting partitions needs an authenticated metadata request"
    );
    KafkaSource::new(&config, &Default::default()).expect("open a Kerberized source");

    // `just check-kerberos` puts a `kinit` that records it ran first on PATH, and starts the cache empty: the image
    // has no `kinit` and no shell, so the ticket must be smeltr's own.
    let Some(ran) = live::service(
        name,
        "SMELTR_KINIT_RAN",
        "the marker a spawned kinit leaves (`just check-kerberos`)",
    ) else {
        return;
    };
    assert!(
        !std::path::Path::new(&ran).exists(),
        "something ran kinit, which the image does not carry"
    );
}

/// The other direction, as far as a plaintext client can take it: the
/// listener must not accept a connection that never authenticates.
/// Without this, a broker that had come up without SASL would make the
/// test above pass for the wrong reason. It does not prove a *wrong*
/// principal is refused; that needs a second keytab the KDC rejects.
#[test]
fn a_client_with_no_credentials_is_refused() {
    let name = "a_client_with_no_credentials_is_refused";
    let Some(brokers) = live::service(
        name,
        "KAFKA_GSSAPI_BROKERS",
        "a Kerberized Kafka broker (`just kerberos-up`)",
    ) else {
        return;
    };

    let consumer: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "smeltr-kerberos-tests-anon")
        .set("security.protocol", "PLAINTEXT")
        .create()
        .expect("build a plaintext consumer");

    assert!(
        consumer.fetch_metadata(None, Duration::from_secs(10)).is_err(),
        "an unauthenticated client reached a broker that is supposed to demand GSSAPI"
    );
}
