//! Whether a test that needs a live Kafka broker or Elasticsearch cluster
//! may skip. Skipping is the default; a skipped test reports `ok`, so
//! `SMELTR_LIVE_TESTS=required` (set by `just check-live`) turns a missing
//! service into a failure.

#![allow(dead_code)]

/// The address of a live Kafka broker, or `None` to skip this test.
pub fn kafka(test_name: &str) -> Option<String> {
    service(test_name, "KAFKA_TEST_BROKERS", "a Kafka broker (`just live-up`)")
}

/// The base URL of a live schema registry answering Confluent's API, or `None` to skip this test.
pub fn schema_registry(test_name: &str) -> Option<String> {
    service(
        test_name,
        "SCHEMA_REGISTRY_TEST_URL",
        "a schema registry (`just live-up`)",
    )
}

/// The base URL of a live Elasticsearch cluster, or `None` to skip.
pub fn elastic(test_name: &str) -> Option<String> {
    service(test_name, "ES_TEST_URL", "an Elasticsearch cluster (`just live-up`)")
}

/// The address of a live S3-compatible service that also answers STS, or `None` to skip this test.
pub fn s3(test_name: &str) -> Option<String> {
    service(test_name, "S3_TEST_ENDPOINT", "an S3 emulator (`just live-up`)")
}

/// The base URL of a live Iceberg REST catalog keeping its tables in the S3 emulator, or `None` to skip this test.
pub fn iceberg(test_name: &str) -> Option<String> {
    service(
        test_name,
        "ICEBERG_TEST_CATALOG",
        "an Iceberg REST catalog (`just live-up`)",
    )
}

/// A live git host with Git LFS: its base URL, and `user:password` for its API. `None` to skip.
pub fn git_host(test_name: &str) -> Option<(String, String)> {
    let what = "a git host (`just live-up`)";
    Some((
        service(test_name, "GIT_TEST_HOST", what)?,
        service(test_name, "GIT_TEST_AUTH", what)?,
    ))
}

/// The address of a live Google Cloud Storage emulator, or `None` to skip this test.
pub fn gcs(test_name: &str) -> Option<String> {
    service(test_name, "GCS_TEST_ENDPOINT", "a GCS emulator (`just live-up`)")
}

/// The address of a live Azure Blob Storage emulator's account, or `None` to skip this test.
pub fn azure(test_name: &str) -> Option<String> {
    service(
        test_name,
        "AZURE_TEST_ENDPOINT",
        "an Azure Blob emulator (`just live-up`)",
    )
}

/// The address of a live Azure Queue Storage emulator's account, or `None` to skip this test.
pub fn azure_queue(test_name: &str) -> Option<String> {
    service(
        test_name,
        "AZURE_QUEUE_TEST_ENDPOINT",
        "an Azure Queue emulator (`just live-up`)",
    )
}

/// The host and port of a live SFTP server, from `SFTP_TEST_HOST` as `host:port`, or `None` to skip this test.
pub fn sftp(test_name: &str) -> Option<(String, u16)> {
    let address = service(test_name, "SFTP_TEST_HOST", "an SFTP server (`just live-up`)")?;
    let (host, port) = address
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("SFTP_TEST_HOST is {address:?}, not host:port"));
    Some((host.to_string(), port.parse().expect("SFTP_TEST_HOST's port")))
}

/// The host and port of a live PostgreSQL, from `POSTGRES_TEST_HOST` as `host:port`, or `None` to skip this test.
pub fn postgres(test_name: &str) -> Option<(String, u16)> {
    let address = service(test_name, "POSTGRES_TEST_HOST", "a PostgreSQL (`just live-up`)")?;
    let (host, port) = address
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("POSTGRES_TEST_HOST is {address:?}, not host:port"));
    Some((host.to_string(), port.parse().expect("POSTGRES_TEST_HOST's port")))
}

/// The host and port of a live MySQL, from `MYSQL_TEST_HOST` as `host:port`, or `None` to skip this test.
pub fn mysql(test_name: &str) -> Option<(String, u16)> {
    host_port(test_name, "MYSQL_TEST_HOST", "a MySQL (`just live-up`)")
}

/// The host and port of a live MariaDB, from `MARIADB_TEST_HOST` as `host:port`, or `None` to skip this test.
pub fn mariadb(test_name: &str) -> Option<(String, u16)> {
    host_port(test_name, "MARIADB_TEST_HOST", "a MariaDB (`just live-up`)")
}

fn host_port(test_name: &str, name: &str, what: &str) -> Option<(String, u16)> {
    let address = service(test_name, name, what)?;
    let (host, port) = address
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("{name} is {address:?}, not host:port"));
    Some((
        host.to_string(),
        port.parse().unwrap_or_else(|_| panic!("{name}'s port")),
    ))
}

/// The address of a live Pub/Sub emulator, or `None` to skip this test.
pub fn pubsub(test_name: &str) -> Option<String> {
    service(test_name, "PUBSUB_TEST_ENDPOINT", "a Pub/Sub emulator (`just live-up`)")
}

/// A live Splunk: its HEC base URL, a token with indexer acknowledgement off and one with it on, its management
/// URL, and `user:password` for searching what landed. `None` to skip.
pub struct Splunk {
    pub hec: String,
    pub token: String,
    pub ack_token: String,
    pub api: String,
    pub auth: String,
}

pub fn splunk(test_name: &str) -> Option<Splunk> {
    let what = "a Splunk with HEC (`just live-up`)";
    Some(Splunk {
        hec: service(test_name, "SPLUNK_TEST_HEC", what)?,
        token: service(test_name, "SPLUNK_TEST_TOKEN", what)?,
        ack_token: service(test_name, "SPLUNK_TEST_ACK_TOKEN", what)?,
        api: service(test_name, "SPLUNK_TEST_API", what)?,
        auth: service(test_name, "SPLUNK_TEST_AUTH", what)?,
    })
}

/// The image of an OpenTelemetry Collector the test runs itself, on the host's network, or `None` to skip this test.
pub fn otel_collector(test_name: &str) -> Option<String> {
    service(
        test_name,
        "OTEL_COLLECTOR_TEST_IMAGE",
        "an OpenTelemetry Collector image and Docker (`just check-live`)",
    )
}

/// The image of Prometheus, which the test runs itself on the host's network, or `None` to skip this test.
pub fn prometheus(test_name: &str) -> Option<String> {
    service(
        test_name,
        "PROMETHEUS_TEST_IMAGE",
        "a Prometheus image and Docker (`just check-live`)",
    )
}

/// The image of Filebeat, which the test runs itself on the host's network, or `None` to skip this test.
pub fn filebeat(test_name: &str) -> Option<String> {
    service(
        test_name,
        "FILEBEAT_TEST_IMAGE",
        "a Filebeat image and Docker (`just check-live`)",
    )
}

/// The image of Fluent Bit, which the test runs itself on the host's network, or `None` to skip this test.
pub fn fluent_bit(test_name: &str) -> Option<String> {
    service(
        test_name,
        "FLUENT_BIT_TEST_IMAGE",
        "a Fluent Bit image and Docker (`just check-live`)",
    )
}

/// The image of OpenTelemetry's `telemetrygen`, which the test runs itself on the host's network, or `None` to skip
/// this test.
pub fn telemetrygen(test_name: &str) -> Option<String> {
    service(
        test_name,
        "TELEMETRYGEN_TEST_IMAGE",
        "a telemetrygen image and Docker (`just check-live`)",
    )
}

/// An image holding util-linux `logger`, which the test runs itself on the host's network, or `None` to skip this
/// test.
pub fn syslog_sender(test_name: &str) -> Option<String> {
    service(
        test_name,
        "SYSLOG_SENDER_TEST_IMAGE",
        "an image with util-linux logger and Docker (`just check-live`)",
    )
}

/// The image of a Splunk universal forwarder, which the test runs itself, or `None` to skip this test.
pub fn universal_forwarder(test_name: &str) -> Option<String> {
    service(
        test_name,
        "S2S_FORWARDER_TEST_IMAGE",
        "a universal forwarder image and Docker (`just check-live`)",
    )
}

/// `$name` from the environment, or `None` -- a failure when live tests
/// are required.
pub fn service(test_name: &str, name: &str, what: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Some(v),
        _ if required() => panic!(
            "{test_name} needs {what} and {name} is not set, but SMELTR_LIVE_TESTS=required. \
             Start it or drop the requirement."
        ),
        _ => {
            eprintln!("skipping {test_name}: set {name} to run it against {what}");
            None
        }
    }
}

/// Whether a missing service is a failure rather than a skip.
pub fn required() -> bool {
    std::env::var("SMELTR_LIVE_TESTS").is_ok_and(|v| v == "required")
}

/// The Kafka endpoint of a live Event Hubs emulator, or `None` to skip this test.
pub fn event_hubs(test_name: &str) -> Option<String> {
    service(
        test_name,
        "EVENT_HUBS_TEST_BROKERS",
        "an Event Hubs emulator (`just live-up`)",
    )
}

/// A real Okta org (`just check-okta`): its address, an API token, and a service app's client id and private key,
/// the key as PEM or as that PEM in base64 on one line. `None` to skip.
pub fn okta(test_name: &str) -> Option<[String; 4]> {
    let what = "an Okta org (`just check-okta`)";
    Some([
        service(test_name, "OKTA_ORG", what)?,
        service(test_name, "OKTA_TOKEN", what)?,
        service(test_name, "OKTA_CLIENT_ID", what)?,
        service(test_name, "OKTA_PRIVATE_KEY", what)?,
    ])
}

/// A real data collection rule and the workspace its table is in (`just check-log-analytics`), with a service principal
/// that may send to the rule and read the workspace.
pub struct LogAnalytics {
    pub endpoint: String,
    pub rule_id: String,
    pub stream: String,
    /// The rule's table, which has a `Tag` and a `Message` column.
    pub table: String,
    /// The workspace's id, a GUID.
    pub workspace: String,
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: String,
}

/// The rule, its workspace and the principal `just check-log-analytics` names, or `None` to skip.
pub fn log_analytics(test_name: &str) -> Option<LogAnalytics> {
    let what = "a data collection rule and its workspace (`just check-log-analytics`)";
    let var = |name: &str| service(test_name, name, what);
    Some(LogAnalytics {
        endpoint: var("LOG_ANALYTICS_TEST_ENDPOINT")?,
        rule_id: var("LOG_ANALYTICS_TEST_RULE_ID")?,
        stream: var("LOG_ANALYTICS_TEST_STREAM")?,
        table: var("LOG_ANALYTICS_TEST_TABLE")?,
        workspace: var("LOG_ANALYTICS_TEST_WORKSPACE")?,
        tenant_id: var("LOG_ANALYTICS_TEST_TENANT_ID")?,
        client_id: var("LOG_ANALYTICS_TEST_CLIENT_ID")?,
        client_secret: var("LOG_ANALYTICS_TEST_CLIENT_SECRET")?,
    })
}

/// The issuer of a live Keycloak's `smeltr` realm (`just live-up`), or `None` to skip this test.
pub fn keycloak(test_name: &str) -> Option<String> {
    service(test_name, "KEYCLOAK_TEST_ISSUER", "a Keycloak (`just live-up`)")
}

/// The directory URL of a live Pebble, Let's Encrypt's test authority (`just live-up`), or `None` to skip this test.
pub fn pebble(test_name: &str) -> Option<String> {
    service(test_name, "PEBBLE_TEST_DIRECTORY", "a Pebble (`just live-up`)")
}

/// A Pebble that validates DNS-01 against challtestsrv, and challtestsrv's DNS and management addresses
/// (`just live-up`), or `None` to skip this test.
pub struct PebbleDns {
    pub directory: String,
    pub dns: std::net::SocketAddr,
    pub management: String,
}

pub fn pebble_dns(test_name: &str) -> Option<PebbleDns> {
    let what = "a Pebble and its challtestsrv (`just live-up`)";
    Some(PebbleDns {
        directory: service(test_name, "PEBBLE_DNS_TEST_DIRECTORY", what)?,
        dns: service(test_name, "CHALLTESTSRV_TEST_DNS", what)?
            .parse()
            .expect("CHALLTESTSRV_TEST_DNS is an address and port"),
        management: service(test_name, "CHALLTESTSRV_TEST_MANAGEMENT", what)?,
    })
}
