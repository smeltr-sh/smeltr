//! The object store destinations against each cloud's real API, emulated as `just live-up` runs it: LocalStack for S3,
//! fake-gcs-server for Google Cloud Storage, Azurite for Azure Blob Storage, and an SFTP server.
//!
//! `tests/objects-tests.rs` covers grouping and object contents against an in-memory store. This file is the contract
//! with each service: the client `build_destination` makes signs and puts through the worker's TLS stack and proxy
//! rules, the objects land under the keys the rows asked for, and on S3 a role is assumed through STS before the put.
//!
//! The object sources are here too: SQS/S3 on LocalStack, Queue/Azure Blob on Azurite's queue service, and a listing
//! of each store and of an SFTP server's directory.
//!
//! Point it at `S3_TEST_ENDPOINT`, `GCS_TEST_ENDPOINT`, `AZURE_TEST_ENDPOINT`, `AZURE_QUEUE_TEST_ENDPOINT` and
//! `SFTP_TEST_HOST`. Under `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips.

use crate::harness;
use crate::live;

use std::borrow::Cow;
use std::io::Read;
use std::time::{Duration, Instant};

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use smeltr::core::column::DataType;
use smeltr::io::parquet::Columns;
use smeltr::io::{Destination, Dispatched, OwnedRecord};
use smeltr::plan::topology::{
    AccessKey, AssumedRole, AzureAccess, AzureCredential, AzureStore, Cloud, Compression, DestinationKind, GcsAccess,
    GcsStore, HeldSecrets, ObjectFormat, ObjectsDestinationConfig, S3Access, S3Store, SecretRef,
};

/// Azurite's account, and a SAS for it signed with the account's published key, good until 2099.
const AZURE_ACCOUNT: &str = "devstoreaccount1";
const AZURE_SAS: &str = "sv=2021-08-06&ss=b&srt=sco&sp=racwdl&se=2099-01-01T00%3A00%3A00Z&spr=https%2Chttp&\
                         sig=ezMwV7FIfnyHN396Y3NDR%2FL%2Bzgrw1cWvrNH0vW9KAC4%3D";
/// Azurite's published account key.
const AZURE_KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
/// A service account key that turns OAuth off, as the emulator takes no token.
const GCS_KEY: &str = r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#;

/// A name no earlier run used.
fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn stored(name: &str) -> SecretRef {
    SecretRef::LeaderStored {
        name: name.to_string(),
        version: 1,
    }
}

fn config(endpoint: &str, bucket: &str) -> ObjectsDestinationConfig {
    on(Cloud::S3(S3Store {
        bucket: bucket.to_string(),
        kms_key_id: None,
        access: S3Access {
            region: Some("us-east-1".to_string()),
            endpoint: Some(endpoint.to_string()),
            auth_endpoint: Some(endpoint.to_string()),
            access_key: Some(AccessKey {
                id: stored("key-id"),
                secret: stored("key"),
            }),
            role: None,
        },
    }))
}

/// The settings every cloud shares, on `cloud`.
fn on(cloud: Cloud) -> ObjectsDestinationConfig {
    ObjectsDestinationConfig {
        cloud,
        prefix: "default/%Y/".to_string(),
        format: ObjectFormat::Ndjson,
        compression: Compression::Gzip,
        storage_class: None,
        tls: None,
        in_flight: 2,
        max_object_bytes: 1024 * 1024,
        buffer_bytes: 4 * 1024 * 1024,
        stall_after: Duration::from_secs(60),
        file_interval: Duration::from_secs(60),
    }
}

fn open(config: ObjectsDestinationConfig) -> Box<dyn Destination> {
    writing(config, None)
}

/// A destination writing `record` for rows no route made, which only Parquet reads.
fn writing(config: ObjectsDestinationConfig, record: Option<&DataType>) -> Box<dyn Destination> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("key-id".to_string(), 1, "test".to_string()),
        ("key".to_string(), 1, "test".to_string()),
        ("gcs-key".to_string(), 1, GCS_KEY.to_string()),
        ("sas".to_string(), 1, AZURE_SAS.to_string()),
        ("sftp-ed25519".to_string(), 1, SFTP_ED25519.to_string()),
    ]);
    let metrics = smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "live");
    let records = record.map(|r| smeltr::plan::topology::Records::from([(None, r.clone())]));
    smeltr::io::build_destination(
        &DestinationKind::Objects(config),
        0,
        metrics,
        &secrets,
        records.as_ref(),
    )
    .expect("built")
}

fn row(value: &str, prefix: Option<&str>) -> OwnedRecord {
    OwnedRecord {
        value: value.as_bytes().to_vec(),
        payload: smeltr::plan::config::Payload::Record,
        placement: prefix.map(str::to_string),
        ..OwnedRecord::default()
    }
}

/// Hands `rows` over as one batch, finishes what holds them, and waits until it lands.
fn deliver(d: &mut dyn Destination, rows: Vec<OwnedRecord>) {
    let (dispatched, refused) = d.deliver(1, Cow::Owned(rows)).expect("delivered");
    assert!(refused.is_empty(), "{refused:?}");
    if dispatched == Dispatched::Complete {
        return;
    }
    // A file is finished at its interval or here.
    d.flush().expect("flushed");
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(c) = d.poll_completions().expect("landed").into_iter().find(|c| c.batch == 1) {
            assert!(c.rejections.is_empty(), "{:?}", c.rejections);
            return;
        }
        assert!(Instant::now() < deadline, "no completion");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Makes `bucket`. The emulator takes any signature, so this one needs none.
fn make_bucket(endpoint: &str, bucket: &str) {
    ureq::put(format!("{endpoint}/{bucket}"))
        .send_empty()
        .expect("bucket made");
}

/// Every key in `bucket`, with its body, unzipped.
fn objects(endpoint: &str, bucket: &str) -> Vec<(String, String)> {
    let listing = ureq::get(format!("{endpoint}/{bucket}?list-type=2"))
        .call()
        .expect("listed")
        .body_mut()
        .read_to_string()
        .unwrap();
    let mut out: Vec<(String, String)> = listing
        .split("<Key>")
        .skip(1)
        .map(|rest| rest[..rest.find("</Key>").unwrap()].to_string())
        .map(|key| {
            let gz = ureq::get(format!("{endpoint}/{bucket}/{key}"))
                .call()
                .expect("read")
                .body_mut()
                .read_to_vec()
                .unwrap();
            let mut text = String::new();
            flate2::read::GzDecoder::new(gz.as_slice())
                .read_to_string(&mut text)
                .unwrap();
            (key, text)
        })
        .collect();
    out.sort();
    out
}

#[test]
fn live_rows_land_as_one_object_per_prefix() {
    let Some(endpoint) = live::s3("live_rows_land_as_one_object_per_prefix") else {
        return;
    };
    let bucket = unique("smeltr");
    make_bucket(&endpoint, &bucket);
    let mut d = open(config(&endpoint, &bucket));

    deliver(
        d.as_mut(),
        vec![
            row(r#"{"n":1}"#, Some("fw/a/")),
            row(r#"{"n":2}"#, None),
            row(r#"{"n":3}"#, Some("fw/a/")),
        ],
    );

    let found = objects(&endpoint, &bucket);
    assert_eq!(found.len(), 2, "{found:?}");
    let (fw, default): (Vec<_>, Vec<_>) = found.iter().partition(|(k, _)| k.starts_with("fw/a/"));
    assert_eq!(fw[0].1, "{\"n\":1}\n{\"n\":3}\n");
    assert!(fw[0].0.ends_with(".ndjson.gz"), "{}", fw[0].0);
    assert!(
        default[0].0.starts_with("default/20"),
        "the year filled in: {}",
        default[0].0
    );
    assert_eq!(default[0].1, "{\"n\":2}\n");
}

/// A Parquet object lands whole, as a file a Parquet reader opens.
#[test]
fn live_a_parquet_object_lands() {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let Some(endpoint) = live::s3("live_a_parquet_object_lands") else {
        return;
    };
    let bucket = unique("smeltr");
    make_bucket(&endpoint, &bucket);
    let plan = harness::compile_ok(vec![harness::code(0, "n: i64; Object.assign($, json(_raw));")]);
    let chunk = harness::cut(&plan, &[r#"{"n":1}"#, r#"{"n":2}"#]);
    let record = harness::record(&plan);
    let mut d = writing(
        ObjectsDestinationConfig {
            format: ObjectFormat::Parquet { native_variants: false },
            ..config(&endpoint, &bucket)
        },
        Some(&record),
    );

    let cut = |row: u32| OwnedRecord {
        payload: smeltr::plan::config::Payload::Record,
        columns: Some(Columns {
            chunk: chunk.clone(),
            row,
        }),
        ..OwnedRecord::default()
    };
    deliver(d.as_mut(), vec![cut(0), cut(1)]);

    let listing = ureq::get(format!("{endpoint}/{bucket}?list-type=2"))
        .call()
        .expect("listed")
        .body_mut()
        .read_to_string()
        .unwrap();
    let key = &listing[listing.find("<Key>").unwrap() + 5..listing.find("</Key>").unwrap()];
    assert!(key.ends_with(".parquet"), "{key}");
    let body = ureq::get(format!("{endpoint}/{bucket}/{key}"))
        .call()
        .expect("read")
        .body_mut()
        .read_to_vec()
        .unwrap();
    let path = std::env::temp_dir().join(format!("{bucket}.parquet"));
    std::fs::write(&path, body).unwrap();
    let file = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
    std::fs::remove_file(&path).unwrap();
    let rows: Vec<String> = file
        .get_row_iter(None)
        .unwrap()
        .map(|r| r.unwrap().to_string())
        .collect();
    assert_eq!(
        rows,
        [
            r#"{_raw: "{"n":1}", n: 1, smeltr: null, rest: null}"#,
            r#"{_raw: "{"n":2}", n: 2, smeltr: null, rest: null}"#
        ]
    );
}

/// An Avro container file lands under a `.avro` key, and a listing that reads Avro reads its rows back: the declared
/// field typed, the rest of the top level kept.
#[test]
fn live_an_avro_object_lands_and_is_read_back() {
    let Some(endpoint) = live::s3("live_an_avro_object_lands_and_is_read_back") else {
        return;
    };
    let bucket = unique("smeltr-avro");
    make_bucket(&endpoint, &bucket);
    let plan = harness::compile_ok(vec![harness::code(0, "n: i64; Object.assign($, json(_raw));")]);
    let chunk = harness::cut(&plan, &[r#"{"n":1}"#, r#"{"n":2}"#]);
    let record = harness::record(&plan);
    let mut d = writing(
        ObjectsDestinationConfig {
            prefix: "in/".to_string(),
            format: ObjectFormat::Avro,
            compression: Compression::Zstd,
            ..config(&endpoint, &bucket)
        },
        Some(&record),
    );
    let cut = |row: u32| OwnedRecord {
        payload: smeltr::plan::config::Payload::Record,
        columns: Some(Columns {
            chunk: chunk.clone(),
            row,
        }),
        ..OwnedRecord::default()
    };
    deliver(d.as_mut(), vec![cut(0), cut(1)]);
    let listing = ureq::get(format!("{endpoint}/{bucket}?list-type=2"))
        .call()
        .expect("listed")
        .body_mut()
        .read_to_string()
        .unwrap();
    let key = &listing[listing.find("<Key>").unwrap() + 5..listing.find("</Key>").unwrap()];
    assert!(key.starts_with("in/") && key.ends_with(".avro"), "{key}");

    let stored = |name: &str| serde_json::json!({ "secret": name, "version": 1 });
    let json = serde_json::json!({
        "version": 1,
        "sources": [{
            "id": "avro", "type": "s3List", "bucket": bucket, "prefix": "in/", "region": "us-east-1",
            "endpoint": endpoint, "accessKeyId": stored("key-id"), "secretAccessKey": stored("key"),
            "format": "avro", "fields": [{ "name": "n", "type": "i64" }],
        }],
    });
    let system = smeltr::config_file::load_str(&json.to_string()).expect("loaded");
    let kind = &system.topology.sources[0].kind;
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("key-id".to_string(), 1, "test".to_string()),
        ("key".to_string(), 1, "test".to_string()),
    ]);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    let mut source = smeltr::io::build_source(kind, &registry, &subjects, &secrets, None).expect("built");
    source.resume(smeltr::io::Progress::default()).expect("resumed");
    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while records.len() < 2 {
        assert!(Instant::now() < deadline, "only {} records", records.len());
        records.extend(
            source
                .poll_batch(100, 1 << 20, Duration::from_millis(500))
                .expect("polled"),
        );
    }
    let meta = kind.meta();
    let rows: Vec<serde_json::Value> = (records.iter())
        .map(|r| serde_json::from_slice(&meta.record_json(r, "")).unwrap())
        .collect();
    assert_eq!(
        rows,
        [
            serde_json::json!({ "n": 1, "_raw": r#"{"n":1}"# }),
            serde_json::json!({ "n": 2, "_raw": r#"{"n":2}"# }),
        ]
    );
}

/// With a role named, the destination asks STS for it with its access key and writes with what comes back.
#[test]
fn live_an_assumed_role_writes() {
    let Some(endpoint) = live::s3("live_an_assumed_role_writes") else {
        return;
    };
    let bucket = unique("smeltr-role");
    make_bucket(&endpoint, &bucket);
    let mut c = config(&endpoint, &bucket);
    let Cloud::S3(s3) = &mut c.cloud else { unreachable!() };
    s3.access.role = Some(AssumedRole {
        arn: "arn:aws:iam::000000000000:role/writer".to_string(),
        external_id: Some("smeltr-live".to_string()),
    });
    let mut d = open(c);

    deliver(d.as_mut(), vec![row(r#"{"n":1}"#, Some("role/"))]);

    let found = objects(&endpoint, &bucket);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].1, "{\"n\":1}\n");
}

/// Every object in `store`, by key, with its body unzipped.
fn read_all(store: &dyn ObjectStore) -> Vec<(String, String)> {
    let runtime = smeltr::io::objects::runtime().unwrap();
    let mut out = runtime.block_on(async {
        let mut out = Vec::new();
        let mut todo = vec![None::<Path>];
        while let Some(at) = todo.pop() {
            let listed = store.list_with_delimiter(at.as_ref()).await.unwrap();
            todo.extend(listed.common_prefixes.into_iter().map(Some));
            for meta in listed.objects {
                let gz = store.get(&meta.location).await.unwrap().bytes().await.unwrap();
                let mut text = String::new();
                flate2::read::GzDecoder::new(gz.as_ref())
                    .read_to_string(&mut text)
                    .unwrap();
                out.push((meta.location.to_string(), text));
            }
        }
        out
    });
    out.sort();
    out
}

/// The rows of `live_rows_land_as_one_object_per_prefix`, and what they must leave in the bucket.
fn two_prefixes(d: &mut dyn Destination, found: impl FnOnce() -> Vec<(String, String)>) {
    deliver(
        d,
        vec![
            row(r#"{"n":1}"#, Some("fw/a/")),
            row(r#"{"n":2}"#, None),
            row(r#"{"n":3}"#, Some("fw/a/")),
        ],
    );
    let found = found();
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(
        found[0].0.starts_with("default/20"),
        "the year filled in: {}",
        found[0].0
    );
    assert_eq!(found[0].1, "{\"n\":2}\n");
    assert!(
        found[1].0.starts_with("fw/a/") && found[1].0.ends_with(".ndjson.gz"),
        "{}",
        found[1].0
    );
    assert_eq!(found[1].1, "{\"n\":1}\n{\"n\":3}\n");
}

#[test]
fn live_gcs_rows_land_as_one_object_per_prefix() {
    let Some(endpoint) = live::gcs("live_gcs_rows_land_as_one_object_per_prefix") else {
        return;
    };
    let bucket = unique("smeltr");
    ureq::post(format!("{endpoint}/storage/v1/b?project=smeltr"))
        .send_json(serde_json::json!({ "name": bucket }))
        .expect("bucket made");
    let mut d = open(on(Cloud::Gcs(GcsStore {
        bucket: bucket.clone(),
        access: GcsAccess {
            endpoint: Some(endpoint.clone()),
            service_account_key: Some(stored("gcs-key")),
        },
    })));
    let reader = object_store::gcp::GoogleCloudStorageBuilder::new()
        .with_service_account_key(GCS_KEY)
        .with_base_url(&endpoint)
        .with_bucket_name(&bucket)
        .with_client_options(object_store::ClientOptions::new().with_allow_http(true))
        .build()
        .unwrap();
    two_prefixes(d.as_mut(), || read_all(&reader));
}

/// Signed with a SAS, and the container made with the same one.
#[test]
fn live_azure_rows_land_as_one_object_per_prefix() {
    let Some(endpoint) = live::azure("live_azure_rows_land_as_one_object_per_prefix") else {
        return;
    };
    let container = unique("smeltr");
    ureq::put(format!("{endpoint}/{container}?restype=container&{AZURE_SAS}"))
        .send_empty()
        .expect("container made");
    let mut d = open(on(Cloud::Azure(AzureStore {
        container: container.clone(),
        access: AzureAccess {
            account: AZURE_ACCOUNT.to_string(),
            endpoint: Some(endpoint.clone()),
            auth_endpoint: None,
            credential: Some(AzureCredential::SasToken(stored("sas"))),
        },
    })));
    let reader = object_store::azure::MicrosoftAzureBuilder::new()
        .with_use_emulator(true)
        .with_container_name(&container)
        .build()
        .unwrap();
    two_prefixes(d.as_mut(), || read_all(&reader));
}

/// Calls SQS's `action` with `body`. The emulator takes any signature, so this one needs none.
fn sqs(endpoint: &str, action: &str, body: serde_json::Value) -> serde_json::Value {
    let text = ureq::post(endpoint)
        .header("X-Amz-Target", format!("AmazonSQS.{action}"))
        .header("Content-Type", "application/x-amz-json-1.0")
        .send(body.to_string())
        .expect("sqs answered")
        .body_mut()
        .read_to_string()
        .unwrap();
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

/// The first lag `source` reports, as (topic, partition, lag): its sampler measures on a thread of its own.
fn first_lag(source: &mut dyn smeltr::io::Source) -> (String, i32, i64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(lag) = source.lag().pop() {
            return (lag.partition.topic.to_string(), lag.partition.partition, lag.lag);
        }
        assert!(Instant::now() < deadline, "no lag reported");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// S3 announces an object on a queue; the source reads it as lines and deletes the message once they are committed.
#[test]
fn live_an_announced_object_is_read_and_its_message_deleted() {
    let Some(endpoint) = live::s3("live_an_announced_object_is_read_and_its_message_deleted") else {
        return;
    };
    let bucket = unique("smeltr-source");
    make_bucket(&endpoint, &bucket);
    let key = "in/a b.log";
    ureq::put(format!("{endpoint}/{bucket}/in/a%20b.log"))
        .send("one\r\ntwo\nthree")
        .expect("object put");
    let queue = sqs(
        &endpoint,
        "CreateQueue",
        serde_json::json!({ "QueueName": unique("smeltr") }),
    )["QueueUrl"]
        .as_str()
        .expect("a queue URL")
        .to_string();
    let event = serde_json::json!({"Records": [{
        "eventName": "ObjectCreated:Put",
        "s3": {"bucket": {"name": bucket}, "object": {"key": key.replace(' ', "+")}}
    }]});
    sqs(
        &endpoint,
        "SendMessage",
        serde_json::json!({ "QueueUrl": queue, "MessageBody": event.to_string() }),
    );

    let config = smeltr::plan::topology::ObjectSourceConfig {
        queue: smeltr::plan::topology::ObjectQueue::Sqs {
            queue_url: queue.clone(),
            access: S3Access {
                region: Some("us-east-1".to_string()),
                endpoint: Some(endpoint.clone()),
                auth_endpoint: None,
                access_key: Some(AccessKey {
                    id: stored("key-id"),
                    secret: stored("key"),
                }),
                role: None,
            },
        },
        tls: None,
        framing: smeltr::plan::topology::Framing::Lines,
        encoding: smeltr::format::encoding::Encoding::Auto,
        avro: None,
        visibility: Duration::from_secs(30),
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("key-id".to_string(), 1, "test".to_string()),
        ("key".to_string(), 1, "test".to_string()),
    ]);
    let kind = smeltr::plan::topology::SourceKind::Objects(config);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    let mut source = smeltr::io::build_source(&kind, &registry, &subjects, &secrets, None).expect("built");
    let name = queue.rsplit('/').next().unwrap();
    assert_eq!(
        first_lag(source.as_mut()),
        (name.to_string(), 0, 1),
        "the message waits"
    );

    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while records.len() < 3 {
        assert!(Instant::now() < deadline, "only {records:?}");
        records.extend(
            source
                .poll_batch(100, 1 << 20, Duration::from_millis(500))
                .expect("polled"),
        );
    }
    let lines: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(lines, [&b"one"[..], b"two", b"three"]);
    assert_eq!(&*records[0].topic, format!("{bucket}/{key}"));

    let last = records.last().unwrap();
    let offsets: smeltr::io::CommitMap = std::iter::once((
        smeltr::io::TopicPartition::new(last.topic.clone(), last.partition),
        last.offset,
    ))
    .collect();
    source.commit(&offsets).expect("committed");
    // Received and not deleted, it would still count, hidden.
    let attributes = sqs(
        &endpoint,
        "GetQueueAttributes",
        serde_json::json!({ "QueueUrl": queue, "AttributeNames": ["All"] }),
    );
    let count = |name: &str| attributes["Attributes"][name].as_str().unwrap_or("?").to_string();
    assert_eq!(
        (
            count("ApproximateNumberOfMessages"),
            count("ApproximateNumberOfMessagesNotVisible")
        ),
        ("0".to_string(), "0".to_string()),
        "the message was deleted: {attributes}"
    );
}

/// A bucket's listing as the queue: the object under the prefix is read, its progress names it, and once committed it
/// is deleted.
#[test]
fn live_a_listed_object_is_read_and_deleted_once_committed() {
    let Some(endpoint) = live::s3("live_a_listed_object_is_read_and_deleted_once_committed") else {
        return;
    };
    let bucket = unique("smeltr-listing");
    make_bucket(&endpoint, &bucket);
    ureq::put(format!("{endpoint}/{bucket}/in/2024-01.log"))
        .send("one\ntwo")
        .expect("object put");
    ureq::put(format!("{endpoint}/{bucket}/other.log"))
        .send("elsewhere")
        .expect("object put");

    let config = smeltr::plan::topology::ObjectSourceConfig {
        queue: smeltr::plan::topology::ObjectQueue::List {
            place: smeltr::plan::topology::ListPlace::S3 {
                bucket: bucket.clone(),
                access: S3Access {
                    region: Some("us-east-1".to_string()),
                    endpoint: Some(endpoint.clone()),
                    auth_endpoint: None,
                    access_key: Some(AccessKey {
                        id: stored("key-id"),
                        secret: stored("key"),
                    }),
                    role: None,
                },
            },
            prefix: "in/2024".to_string(),
            poll_interval: Duration::from_secs(1),
            start: smeltr::plan::topology::ListStart::Everything,
            after_commit: smeltr::plan::topology::AfterCommit::Delete,
        },
        tls: None,
        framing: smeltr::plan::topology::Framing::Lines,
        encoding: smeltr::format::encoding::Encoding::Auto,
        avro: None,
        visibility: Duration::from_secs(30),
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("key-id".to_string(), 1, "test".to_string()),
        ("key".to_string(), 1, "test".to_string()),
    ]);
    let kind = smeltr::plan::topology::SourceKind::Objects(config);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    let mut source = smeltr::io::build_source(&kind, &registry, &subjects, &secrets, None).expect("built");
    source.resume(smeltr::io::Progress::default()).expect("resumed");

    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while records.len() < 2 {
        assert!(Instant::now() < deadline, "only {records:?}");
        records.extend(
            source
                .poll_batch(100, 1 << 20, Duration::from_millis(500))
                .expect("polled"),
        );
    }
    let lines: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(lines, [&b"one"[..], b"two"]);
    assert_eq!(&*records[0].topic, format!("{bucket}/in/2024-01.log"));

    let last = records.last().unwrap();
    let offsets: smeltr::io::CommitMap = std::iter::once((
        smeltr::io::TopicPartition::new(last.topic.clone(), last.partition),
        last.offset,
    ))
    .collect();
    source.commit(&offsets).expect("committed");
    assert_eq!(source.take_progress(), None, "a deleting listing keeps no ledger");
    let listing = ureq::get(format!("{endpoint}/{bucket}?list-type=2"))
        .call()
        .expect("listed")
        .body_mut()
        .read_to_string()
        .unwrap();
    assert!(
        !listing.contains("<Key>in/2024-01.log</Key>") && listing.contains("<Key>other.log</Key>"),
        "the read object was deleted: {listing}"
    );
}

/// `method` on `url` with `headers` and `body`, signed with Azurite's account key: the answer's status, headers and
/// body.
fn azure(method: &str, url: &str, headers: &[(&str, &str)], body: &str) -> (u16, ureq::http::HeaderMap, String) {
    use object_store::azure::{AzureAccessKey, AzureAuthorizer, AzureCredential as Signing};
    let mut request = http::Request::builder()
        .method(method)
        .uri(url)
        .header(http::header::CONTENT_LENGTH, body.len());
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let mut request = request
        .body(object_store::client::HttpRequestBody::from(body.to_string()))
        .unwrap();
    let key = Signing::AccessKey(AzureAccessKey::try_new(AZURE_KEY).unwrap());
    AzureAuthorizer::new(&key, AZURE_ACCOUNT)
        .try_authorize(&mut request)
        .unwrap();
    let agent: ureq::Agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
    let mut call = ureq::http::Request::builder().method(method).uri(url);
    for (name, value) in request.headers() {
        call = call.header(name.as_str(), value.as_bytes());
    }
    let mut response = agent
        .run(call.body(body.to_string()).unwrap())
        .expect("azurite answered");
    let text = response.body_mut().read_to_string().unwrap();
    (response.status().as_u16(), response.headers().clone(), text)
}

/// A queue made fresh on Azurite, its URL.
fn azure_queue(endpoint: &str) -> String {
    let url = format!("{endpoint}/{}", unique("smeltr"));
    let (status, _, body) = azure("PUT", &url, &[], "");
    assert_eq!(status, 201, "queue made: {body}");
    url
}

/// How many messages `queue` holds, hidden ones too, or `None` while it does not exist.
fn azure_count(queue: &str) -> Option<usize> {
    let (status, headers, _) = azure("GET", &format!("{queue}?comp=metadata"), &[], "");
    if status == 404 {
        return None;
    }
    let count = headers.get("x-ms-approximate-messages-count")?;
    count.to_str().ok()?.parse().ok()
}

/// A Queue/Azure Blob source on `queue`, reading the blob service at `blobs` with Azurite's key.
fn azure_source(queue: &str, blobs: &str, visibility: Duration, max_deliveries: u32) -> Box<dyn smeltr::io::Source> {
    let config = smeltr::plan::topology::ObjectSourceConfig {
        queue: smeltr::plan::topology::ObjectQueue::StorageQueue {
            queue_url: queue.to_string(),
            access: AzureAccess {
                account: AZURE_ACCOUNT.to_string(),
                endpoint: Some(blobs.to_string()),
                auth_endpoint: None,
                credential: Some(AzureCredential::AccessKey(stored("azure-key"))),
            },
            max_deliveries,
        },
        tls: None,
        framing: smeltr::plan::topology::Framing::Lines,
        encoding: smeltr::format::encoding::Encoding::Auto,
        avro: None,
        visibility,
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("azure-key".to_string(), 1, AZURE_KEY.to_string())]);
    let kind = smeltr::plan::topology::SourceKind::Objects(config);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    smeltr::io::build_source(&kind, &registry, &subjects, &secrets, None).expect("built")
}

/// An Event Grid `BlobCreated` event for `url`, base64-encoded as Azure's SDKs write a queue message.
fn blob_created(url: &str) -> String {
    let event = serde_json::json!({
        "eventType": "Microsoft.Storage.BlobCreated",
        "subject": "/blobServices/default/containers/c/blobs/b",
        "data": {"api": "PutBlob", "url": url}
    });
    smeltr::format::bytes_text::BytesForm::Base64.text(event.to_string().as_bytes())
}

/// Puts `text` as a message on `queue`.
fn azure_send(queue: &str, text: &str) {
    let body = format!("<QueueMessage><MessageText>{text}</MessageText></QueueMessage>");
    let (status, _, answer) = azure("POST", &format!("{queue}/messages"), &[], &body);
    assert_eq!(status, 201, "message put: {answer}");
}

/// Event Grid announces a blob on a Storage queue; the source reads it as lines, signing with the account key, and
/// deletes the message once they are committed.
#[test]
fn live_an_announced_blob_is_read_and_its_message_deleted() {
    let test = "live_an_announced_blob_is_read_and_its_message_deleted";
    let (Some(blobs), Some(queues)) = (live::azure(test), live::azure_queue(test)) else {
        return;
    };
    let container = unique("smeltr-source");
    let (status, _, body) = azure("PUT", &format!("{blobs}/{container}?restype=container"), &[], "");
    assert_eq!(status, 201, "container made: {body}");
    let blob = format!("{blobs}/{container}/in/a%20b.log");
    let (status, _, body) = azure("PUT", &blob, &[("x-ms-blob-type", "BlockBlob")], "one\r\ntwo\nthree");
    assert_eq!(status, 201, "blob put: {body}");
    let queue = azure_queue(&queues);
    azure_send(&queue, &blob_created(&blob));

    let mut source = azure_source(&queue, &blobs, Duration::from_secs(30), 5);
    let name = queue.rsplit('/').next().unwrap();
    assert_eq!(
        first_lag(source.as_mut()),
        (name.to_string(), 0, 1),
        "the message waits"
    );
    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while records.len() < 3 {
        assert!(Instant::now() < deadline, "only {records:?}");
        records.extend(
            source
                .poll_batch(100, 1 << 20, Duration::from_millis(500))
                .expect("polled"),
        );
    }
    let lines: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(lines, [&b"one"[..], b"two", b"three"]);
    assert_eq!(&*records[0].topic, format!("{container}/in/a b.log"));

    let last = records.last().unwrap();
    let offsets: smeltr::io::CommitMap = std::iter::once((
        smeltr::io::TopicPartition::new(last.topic.clone(), last.partition),
        last.offset,
    ))
    .collect();
    source.commit(&offsets).expect("committed");
    // Received and not deleted, it would still count, hidden.
    assert_eq!(azure_count(&queue), Some(0), "the message was deleted");
}

/// A message naming a blob that is not there comes back after each lapse, and on the receive past `maxDeliveries` it
/// is moved to the poison queue, as it was sent, rather than read again.
#[test]
fn live_a_message_received_too_often_is_moved_to_the_poison_queue() {
    let test = "live_a_message_received_too_often_is_moved_to_the_poison_queue";
    let (Some(blobs), Some(queues)) = (live::azure(test), live::azure_queue(test)) else {
        return;
    };
    let queue = azure_queue(&queues);
    let text = blob_created(&format!("{blobs}/{}/gone.log", unique("smeltr-none")));
    azure_send(&queue, &text);

    let mut source = azure_source(&queue, &blobs, Duration::from_secs(1), 1);
    let poison = format!("{queue}-poison");
    let deadline = Instant::now() + Duration::from_secs(30);
    while azure_count(&poison) != Some(1) {
        assert!(Instant::now() < deadline, "the message was never moved to {poison}");
        let records = source
            .poll_batch(100, 1 << 20, Duration::from_millis(500))
            .expect("polled");
        assert!(records.is_empty(), "{records:?}");
    }
    assert_eq!(azure_count(&queue), Some(0), "the message left its queue");
    let (_, _, peeked) = azure("GET", &format!("{poison}/messages?peekonly=true"), &[], "");
    assert!(peeked.contains(&text), "{peeked}");
}

/// Calls the Pub/Sub emulator's `path` with `method` and `body`. The emulator takes any request unsigned.
fn pubsub(method: &str, url: &str, body: serde_json::Value) -> serde_json::Value {
    let agent: ureq::Agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
    let call = ureq::http::Request::builder()
        .method(method)
        .uri(url)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .unwrap();
    let mut response = agent.run(call).expect("pubsub answered");
    let text = response.body_mut().read_to_string().unwrap();
    assert!(response.status().is_success(), "{method} {url}: {text}");
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

/// Cloud Storage announces an object on a Pub/Sub subscription; the source reads the generation announced, though the
/// object has been written again since, and acknowledges the message once its lines are committed. A notification of
/// any other event is acknowledged unread.
#[test]
fn live_an_announced_gcs_object_is_read_at_its_generation_and_acknowledged() {
    let test = "live_an_announced_gcs_object_is_read_at_its_generation_and_acknowledged";
    let (Some(gcs), Some(endpoint)) = (live::gcs(test), live::pubsub(test)) else {
        return;
    };
    let bucket = unique("smeltr-source");
    ureq::post(format!("{gcs}/storage/v1/b?project=smeltr"))
        .send_json(serde_json::json!({ "name": bucket, "versioning": {"enabled": true} }))
        .expect("bucket made");
    let key = "in/a b.log";
    let put = |body: &str| -> String {
        let mut answer = ureq::post(format!(
            "{gcs}/upload/storage/v1/b/{bucket}/o?uploadType=media&name=in%2Fa%20b.log"
        ))
        .send(body)
        .expect("object put");
        let object: serde_json::Value = answer.body_mut().read_json().unwrap();
        object["generation"].as_str().expect("a generation").to_string()
    };
    let generation = put("one\r\ntwo\nthree");
    put("written again");

    let project = format!("{endpoint}/v1/projects/smeltr");
    let name = unique("smeltr");
    pubsub("PUT", &format!("{project}/topics/{name}"), serde_json::json!({}));
    let subscription = format!("projects/smeltr/subscriptions/{name}");
    pubsub(
        "PUT",
        &format!("{endpoint}/v1/{subscription}"),
        serde_json::json!({ "topic": format!("projects/smeltr/topics/{name}") }),
    );
    let notification = |event: &str| {
        serde_json::json!({
            "data": smeltr::format::bytes_text::BytesForm::Base64.text(b"{}"),
            "attributes": {
                "eventType": event,
                "payloadFormat": "JSON_API_V1",
                "bucketId": bucket,
                "objectId": key,
                "objectGeneration": generation,
            }
        })
    };
    pubsub(
        "POST",
        &format!("{project}/topics/{name}:publish"),
        serde_json::json!({ "messages": [notification("OBJECT_DELETE"), notification("OBJECT_FINALIZE")] }),
    );

    let visibility = Duration::from_secs(2);
    let config = smeltr::plan::topology::ObjectSourceConfig {
        queue: smeltr::plan::topology::ObjectQueue::PubSub {
            subscription: subscription.clone(),
            endpoint: Some(endpoint.clone()),
            access: GcsAccess {
                endpoint: Some(gcs.clone()),
                service_account_key: Some(stored("gcs-key")),
            },
        },
        tls: None,
        framing: smeltr::plan::topology::Framing::Lines,
        encoding: smeltr::format::encoding::Encoding::Auto,
        avro: None,
        visibility,
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("gcs-key".to_string(), 1, GCS_KEY.to_string())]);
    let kind = smeltr::plan::topology::SourceKind::Objects(config);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    let mut source = smeltr::io::build_source(&kind, &registry, &subjects, &secrets, None).expect("built");

    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while records.len() < 3 {
        assert!(Instant::now() < deadline, "only {records:?}");
        records.extend(
            source
                .poll_batch(100, 1 << 20, Duration::from_millis(500))
                .expect("polled"),
        );
    }
    let lines: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(lines, [&b"one"[..], b"two", b"three"]);
    assert_eq!(&*records[0].topic, format!("{bucket}/{key}"));

    let last = records.last().unwrap();
    let offsets: smeltr::io::CommitMap = std::iter::once((
        smeltr::io::TopicPartition::new(last.topic.clone(), last.partition),
        last.offset,
    ))
    .collect();
    source.commit(&offsets).expect("committed");
    drop(source);
    // A message pulled and not acknowledged comes back once its deadline lapses.
    std::thread::sleep(visibility * 2);
    let left = pubsub(
        "POST",
        &format!("{endpoint}/v1/{subscription}:pull"),
        serde_json::json!({ "maxMessages": 10, "returnImmediately": true }),
    );
    assert!(
        left["receivedMessages"].is_null(),
        "both messages were acknowledged: {left}"
    );
}

/// A listing source on `place` under `prefix`, signed with the emulators' keys.
fn listing_source(
    place: smeltr::plan::topology::ListPlace,
    prefix: &str,
    after_commit: smeltr::plan::topology::AfterCommit,
) -> Box<dyn smeltr::io::Source> {
    let config = smeltr::plan::topology::ObjectSourceConfig {
        queue: smeltr::plan::topology::ObjectQueue::List {
            place,
            prefix: prefix.to_string(),
            poll_interval: Duration::from_secs(1),
            start: smeltr::plan::topology::ListStart::Everything,
            after_commit,
        },
        tls: None,
        framing: smeltr::plan::topology::Framing::Lines,
        encoding: smeltr::format::encoding::Encoding::Auto,
        avro: None,
        visibility: Duration::from_secs(30),
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("gcs-key".to_string(), 1, GCS_KEY.to_string()),
        ("azure-key".to_string(), 1, AZURE_KEY.to_string()),
    ]);
    let kind = smeltr::plan::topology::SourceKind::Objects(config);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    let mut source = smeltr::io::build_source(&kind, &registry, &subjects, &secrets, None).expect("built");
    source.resume(smeltr::io::Progress::default()).expect("resumed");
    source
}

/// Reads `source` until it gives exactly `want` lines, checks they came from `topic`, and commits the last.
fn read_and_commit(source: &mut dyn smeltr::io::Source, topic: &str, want: &[&[u8]]) {
    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while records.len() < want.len() {
        assert!(Instant::now() < deadline, "only {records:?}");
        records.extend(
            source
                .poll_batch(100, 1 << 20, Duration::from_millis(500))
                .expect("polled"),
        );
    }
    let lines: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(lines, want);
    assert!(records.iter().all(|r| &*r.topic == topic), "{records:?}");

    let last = records.last().unwrap();
    let offsets: smeltr::io::CommitMap = std::iter::once((
        smeltr::io::TopicPartition::new(last.topic.clone(), last.partition),
        last.offset,
    ))
    .collect();
    source.commit(&offsets).expect("committed");
}

/// Every key in `store`.
fn keys(store: &dyn ObjectStore) -> Vec<String> {
    let runtime = smeltr::io::objects::runtime().unwrap();
    let mut out = runtime.block_on(async {
        let mut out = Vec::new();
        let mut todo = vec![None::<Path>];
        while let Some(at) = todo.pop() {
            let listed = store.list_with_delimiter(at.as_ref()).await.unwrap();
            todo.extend(listed.common_prefixes.into_iter().map(Some));
            out.extend(listed.objects.into_iter().map(|meta| meta.location.to_string()));
        }
        out
    });
    out.sort();
    out
}

/// A Cloud Storage bucket's listing as the queue: the object under the prefix is read and, once committed, deleted;
/// the one outside it is left.
#[test]
fn live_a_listed_gcs_object_is_read_and_deleted_once_committed() {
    let Some(endpoint) = live::gcs("live_a_listed_gcs_object_is_read_and_deleted_once_committed") else {
        return;
    };
    let bucket = unique("smeltr-listing");
    ureq::post(format!("{endpoint}/storage/v1/b?project=smeltr"))
        .send_json(serde_json::json!({ "name": bucket }))
        .expect("bucket made");
    for (name, body) in [("in%2F2024-01.log", "one\ntwo\n"), ("other.log", "elsewhere\n")] {
        ureq::post(format!(
            "{endpoint}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={name}"
        ))
        .send(body)
        .expect("object put");
    }

    let store = GcsStore {
        bucket: bucket.clone(),
        access: GcsAccess {
            endpoint: Some(endpoint.clone()),
            service_account_key: Some(stored("gcs-key")),
        },
    };
    let mut source = listing_source(
        smeltr::plan::topology::ListPlace::Gcs(store),
        "in/2024",
        smeltr::plan::topology::AfterCommit::Delete,
    );
    read_and_commit(source.as_mut(), &format!("{bucket}/in/2024-01.log"), &[b"one", b"two"]);
    assert_eq!(source.take_progress(), None, "a deleting listing keeps no ledger");

    let reader = object_store::gcp::GoogleCloudStorageBuilder::new()
        .with_service_account_key(GCS_KEY)
        .with_base_url(&endpoint)
        .with_bucket_name(&bucket)
        .with_client_options(object_store::ClientOptions::new().with_allow_http(true))
        .build()
        .unwrap();
    assert_eq!(keys(&reader), ["other.log"], "the read object was deleted");
}

/// An Azure container's listing as the queue, signed with the account key: the blob under the prefix is read and,
/// once committed, moved under the done prefix; the one outside it is left.
#[test]
fn live_a_listed_blob_is_read_and_moved_once_committed() {
    let Some(endpoint) = live::azure("live_a_listed_blob_is_read_and_moved_once_committed") else {
        return;
    };
    let container = unique("smeltr-listing");
    let (status, _, body) = azure("PUT", &format!("{endpoint}/{container}?restype=container"), &[], "");
    assert_eq!(status, 201, "container made: {body}");
    for (name, text) in [("in/2024-01.log", "one\ntwo\n"), ("other.log", "elsewhere\n")] {
        let url = format!("{endpoint}/{container}/{name}");
        let (status, _, body) = azure("PUT", &url, &[("x-ms-blob-type", "BlockBlob")], text);
        assert_eq!(status, 201, "blob put: {body}");
    }

    let store = AzureStore {
        container: container.clone(),
        access: AzureAccess {
            account: AZURE_ACCOUNT.to_string(),
            endpoint: Some(endpoint.clone()),
            auth_endpoint: None,
            credential: Some(AzureCredential::AccessKey(stored("azure-key"))),
        },
    };
    let mut source = listing_source(
        smeltr::plan::topology::ListPlace::Azure(store),
        "in/",
        smeltr::plan::topology::AfterCommit::Move {
            prefix: "done/".to_string(),
        },
    );
    read_and_commit(
        source.as_mut(),
        &format!("{container}/in/2024-01.log"),
        &[b"one", b"two"],
    );
    assert_eq!(source.take_progress(), None, "a moving listing keeps no ledger");

    let reader = object_store::azure::MicrosoftAzureBuilder::new()
        .with_use_emulator(true)
        .with_container_name(&container)
        .build()
        .unwrap();
    assert_eq!(
        keys(&reader),
        ["done/2024-01.log", "other.log"],
        "the read blob was moved"
    );
}

/// An append blob read once it stops growing, kept, then appended to: it is read again whole.
#[test]
fn live_an_append_blob_that_grows_is_read_again_whole() {
    let Some(endpoint) = live::azure("live_an_append_blob_that_grows_is_read_again_whole") else {
        return;
    };
    let container = unique("smeltr-listing");
    let (status, _, body) = azure("PUT", &format!("{endpoint}/{container}?restype=container"), &[], "");
    assert_eq!(status, 201, "container made: {body}");
    let url = format!("{endpoint}/{container}/app.log");
    let (status, _, body) = azure("PUT", &url, &[("x-ms-blob-type", "AppendBlob")], "");
    assert_eq!(status, 201, "append blob made: {body}");
    let append = |text: &str| {
        let (status, _, body) = azure("PUT", &format!("{url}?comp=appendblock"), &[], text);
        assert_eq!(status, 201, "block appended: {body}");
    };
    append("one\n");

    let store = AzureStore {
        container: container.clone(),
        access: AzureAccess {
            account: AZURE_ACCOUNT.to_string(),
            endpoint: Some(endpoint.clone()),
            auth_endpoint: None,
            credential: Some(AzureCredential::AccessKey(stored("azure-key"))),
        },
    };
    let mut source = listing_source(
        smeltr::plan::topology::ListPlace::Azure(store),
        "",
        smeltr::plan::topology::AfterCommit::Keep,
    );
    let topic = format!("{container}/app.log");
    read_and_commit(source.as_mut(), &topic, &[b"one"]);
    append("two\n");
    read_and_commit(source.as_mut(), &topic, &[b"one", b"two"]);
}

/// The SFTP server's host keys and the keys `smeltr` signs in with, all made for `just live-up` alone.
const SFTP_HOST_KEY: &str = include_str!("live/sftp/ssh_host_ed25519_key.pub");
const SFTP_RSA_HOST_KEY: &str = include_str!("live/sftp/ssh_host_rsa_key.pub");
const SFTP_ED25519: &str = include_str!("live/sftp/client_ed25519");
const SFTP_RSA: &str = include_str!("live/sftp/client_rsa");
/// A host key the server does not have.
const SFTP_OTHER_HOST_KEY: &str = include_str!("live/sftp/other_host_ed25519.pub");

/// A fresh directory under the user's upload directory on the server at `address`, reached with `credential` and
/// pinned to `host_key`.
fn sftp_store(
    address: &(String, u16),
    credential: smeltr::plan::topology::SftpCredential,
    host_key: &str,
) -> smeltr::plan::topology::SftpStore {
    smeltr::plan::topology::SftpStore {
        directory: unique("upload/smeltr-listing"),
        access: smeltr::plan::topology::SftpAccess {
            host: address.0.clone(),
            port: address.1,
            username: "smeltr".to_string(),
            credential,
            host_key: host_key.to_string(),
        },
    }
}

fn sftp_password() -> smeltr::plan::topology::SftpCredential {
    smeltr::plan::topology::SftpCredential::Password(stored("sftp-password"))
}

/// What the tests hold for the SFTP server, under the names `sftp_password` and the keys refer to.
fn sftp_secrets() -> HeldSecrets {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("sftp-password".to_string(), 1, "smeltr-live-1".to_string()),
        ("sftp-ed25519".to_string(), 1, SFTP_ED25519.to_string()),
        ("sftp-rsa".to_string(), 1, SFTP_RSA.to_string()),
    ]);
    secrets
}

/// `store`'s directory as an object store, signed in with the password and pinned to the server's own key.
fn sftp_writer(store: &smeltr::plan::topology::SftpStore) -> smeltr::io::sftp::Sftp {
    let mut store = store.clone();
    store.access.credential = sftp_password();
    store.access.host_key = SFTP_HOST_KEY.to_string();
    smeltr::io::sftp::Sftp::new(&store, "", false, &sftp_secrets()).expect("sftp store")
}

/// Writes `body` to `key` under `store`'s directory, which it makes when it is not there: a store's directory must
/// be, as a bucket must.
fn sftp_put(store: &smeltr::plan::topology::SftpStore, key: &str, body: &str) {
    let (parent, name) = store.directory.rsplit_once('/').expect("a directory under another");
    let mut upload = store.clone();
    upload.directory = parent.to_string();
    let runtime = smeltr::io::objects::runtime().unwrap();
    runtime
        .block_on(sftp_writer(&upload).put(&Path::from(format!("{name}/{key}")), body.as_bytes().to_vec().into()))
        .expect("file put");
}

/// A listing source on an SFTP directory, holding the SFTP secrets.
fn sftp_source(
    store: smeltr::plan::topology::SftpStore,
    prefix: &str,
    after_commit: smeltr::plan::topology::AfterCommit,
) -> Box<dyn smeltr::io::Source> {
    let config = smeltr::plan::topology::ObjectSourceConfig {
        queue: smeltr::plan::topology::ObjectQueue::List {
            place: smeltr::plan::topology::ListPlace::Sftp(store),
            prefix: prefix.to_string(),
            poll_interval: Duration::from_secs(1),
            start: smeltr::plan::topology::ListStart::Everything,
            after_commit,
        },
        tls: None,
        framing: smeltr::plan::topology::Framing::Lines,
        encoding: smeltr::format::encoding::Encoding::Auto,
        avro: None,
        visibility: Duration::from_secs(30),
    };
    let kind = smeltr::plan::topology::SourceKind::Objects(config);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    let mut source = smeltr::io::build_source(&kind, &registry, &subjects, &sftp_secrets(), None).expect("built");
    source.resume(smeltr::io::Progress::default()).expect("resumed");
    source
}

/// An SFTP directory's listing as the queue, signed in with a password: the file under the prefix is read and, once
/// committed, moved under the done prefix; the one outside it is left.
#[test]
fn live_a_listed_sftp_file_is_read_and_moved_once_committed() {
    let Some(address) = live::sftp("live_a_listed_sftp_file_is_read_and_moved_once_committed") else {
        return;
    };
    let store = sftp_store(&address, sftp_password(), SFTP_HOST_KEY);
    sftp_put(&store, "in/2024-01.log", "one\ntwo\n");
    sftp_put(&store, "other.log", "elsewhere\n");
    let writer = sftp_writer(&store);

    let directory = store.directory.clone();
    let mut source = sftp_source(
        store,
        "in/",
        smeltr::plan::topology::AfterCommit::Move {
            prefix: "done/".to_string(),
        },
    );
    read_and_commit(
        source.as_mut(),
        &format!("{directory}/in/2024-01.log"),
        &[b"one", b"two"],
    );
    assert_eq!(source.take_progress(), None, "a moving listing keeps no ledger");
    assert_eq!(
        keys(&writer),
        ["done/2024-01.log", "other.log"],
        "the read file was moved"
    );
}

/// A file signed in to with `key`, kept, then rewritten: it is read again whole.
fn an_sftp_file_rewritten_is_read_again_whole(test: &str, key: &str) {
    let Some(address) = live::sftp(test) else {
        return;
    };
    let credential = smeltr::plan::topology::SftpCredential::PrivateKey(stored(key));
    let store = sftp_store(&address, credential, SFTP_HOST_KEY);
    sftp_put(&store, "app.log", "one\n");

    let topic = format!("{}/app.log", store.directory);
    let mut source = sftp_source(store.clone(), "", smeltr::plan::topology::AfterCommit::Keep);
    read_and_commit(source.as_mut(), &topic, &[b"one"]);
    sftp_put(&store, "app.log", "one\ntwo");
    read_and_commit(source.as_mut(), &topic, &[b"one", b"two"]);
}

#[test]
fn live_an_sftp_file_signed_in_with_an_ed25519_key_is_read_again_whole_once_rewritten() {
    an_sftp_file_rewritten_is_read_again_whole(
        "live_an_sftp_file_signed_in_with_an_ed25519_key_is_read_again_whole_once_rewritten",
        "sftp-ed25519",
    );
}

#[test]
fn live_an_sftp_file_signed_in_with_an_rsa_key_is_read_again_whole_once_rewritten() {
    an_sftp_file_rewritten_is_read_again_whole(
        "live_an_sftp_file_signed_in_with_an_rsa_key_is_read_again_whole_once_rewritten",
        "sftp-rsa",
    );
}

/// A batch lands as one file per prefix under the directory.
#[test]
fn live_sftp_rows_land_as_one_file_per_prefix() {
    let Some(address) = live::sftp("live_sftp_rows_land_as_one_file_per_prefix") else {
        return;
    };
    let credential = smeltr::plan::topology::SftpCredential::PrivateKey(stored("sftp-ed25519"));
    let store = sftp_store(&address, credential, SFTP_HOST_KEY);
    let reader = sftp_writer(&store);
    // The directory must be there, as a bucket must; the folders under it need not.
    sftp_put(&store, "made", "");
    let runtime = smeltr::io::objects::runtime().unwrap();
    runtime.block_on(reader.delete(&Path::from("made"))).expect("deleted");
    let mut d = open(on(Cloud::Sftp(store.clone())));
    two_prefixes(d.as_mut(), || read_all(&reader));
}

/// A Parquet file goes up in parts and is renamed into place whole once finished.
#[test]
fn live_an_sftp_parquet_file_lands_whole() {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let Some(address) = live::sftp("live_an_sftp_parquet_file_lands_whole") else {
        return;
    };
    let credential = smeltr::plan::topology::SftpCredential::PrivateKey(stored("sftp-ed25519"));
    let store = sftp_store(&address, credential, SFTP_HOST_KEY);
    let reader = sftp_writer(&store);
    sftp_put(&store, "made", "");
    let runtime = smeltr::io::objects::runtime().unwrap();
    runtime.block_on(reader.delete(&Path::from("made"))).expect("deleted");
    let plan = harness::compile_ok(vec![harness::code(0, "n: i64; Object.assign($, json(_raw));")]);
    let chunk = harness::cut(&plan, &[r#"{"n":1}"#, r#"{"n":2}"#]);
    let record = harness::record(&plan);
    let mut d = writing(
        ObjectsDestinationConfig {
            format: ObjectFormat::Parquet { native_variants: false },
            ..on(Cloud::Sftp(store.clone()))
        },
        Some(&record),
    );
    let cut = |row: u32| OwnedRecord {
        payload: smeltr::plan::config::Payload::Record,
        columns: Some(Columns {
            chunk: chunk.clone(),
            row,
        }),
        ..OwnedRecord::default()
    };
    deliver(d.as_mut(), vec![cut(0), cut(1)]);

    let keys: Vec<Path> = runtime.block_on(async {
        use futures_util::TryStreamExt;
        reader
            .list(None)
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .expect("listed")
    });
    assert_eq!(keys.len(), 1, "{keys:?}");
    assert!(keys[0].as_ref().ends_with(".parquet"), "{keys:?}");
    let body = runtime
        .block_on(async { reader.get(&keys[0]).await?.bytes().await })
        .expect("read");
    let file = SerializedFileReader::new(body).unwrap();
    let ns: Vec<String> = file
        .get_row_iter(None)
        .unwrap()
        .map(|r| {
            r.unwrap()
                .get_column_iter()
                .find(|(name, _)| name.as_str() == "n")
                .unwrap()
                .1
                .to_string()
        })
        .collect();
    assert_eq!(ns, ["1", "2"]);
}

/// A server that holds an ed25519 key too shows the RSA key pinned.
#[test]
fn live_an_sftp_server_s_rsa_host_key_can_be_pinned() {
    let Some(address) = live::sftp("live_an_sftp_server_s_rsa_host_key_can_be_pinned") else {
        return;
    };
    let store = sftp_store(&address, sftp_password(), SFTP_RSA_HOST_KEY);
    sftp_put(&store, "app.log", "one\n");

    let topic = format!("{}/app.log", store.directory);
    let mut source = sftp_source(store, "", smeltr::plan::topology::AfterCommit::Keep);
    read_and_commit(source.as_mut(), &topic, &[b"one"]);
}

/// A server whose host key is not the one pinned: the source fails and reads nothing.
#[test]
fn live_an_sftp_server_with_another_host_key_is_refused() {
    let Some(address) = live::sftp("live_an_sftp_server_with_another_host_key_is_refused") else {
        return;
    };
    let store = sftp_store(&address, sftp_password(), SFTP_OTHER_HOST_KEY);
    sftp_put(&store, "app.log", "one\n");

    let mut source = sftp_source(store, "", smeltr::plan::topology::AfterCommit::Keep);
    let deadline = Instant::now() + Duration::from_secs(30);
    let error = loop {
        assert!(Instant::now() < deadline, "no error");
        match source.poll_batch(100, 1 << 20, Duration::from_millis(500)) {
            Ok(records) => assert!(records.is_empty(), "read through a wrong host key: {records:?}"),
            Err(e) => break e.to_string(),
        }
    };
    assert!(error.contains("host key"), "{error}");
}
