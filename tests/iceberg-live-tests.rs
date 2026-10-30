//! The Iceberg destination against a real REST catalog: Apache Iceberg's own REST fixture, keeping its tables in
//! LocalStack's S3, as `just live-up` runs them.
//!
//! `io::iceberg`'s unit tests cover the catalog protocol and the table's files against a fake catalog and an in-memory
//! store. This file is the contract with the reference implementation: the destination creates the table, the catalog
//! takes each commit as a snapshot, and the snapshot's files are in the bucket.
//!
//! Point it at `ICEBERG_TEST_CATALOG` and `S3_TEST_ENDPOINT`. Under `SMELTR_LIVE_TESTS=required` a missing one fails
//! rather than skips.

use crate::harness;
use crate::live;

use std::borrow::Cow;
use std::time::{Duration, Instant};

use smeltr::io::iceberg::manifest::{read_manifest, read_manifest_list};
use smeltr::io::parquet::Columns;
use smeltr::io::{Destination, Dispatched, OwnedRecord};
use smeltr::plan::topology::{
    AccessKey, CatalogSignIn, Compression, DestinationKind, HeldSecrets, IcebergDestinationConfig, S3Access, SecretRef,
};

/// The bucket the fixture's warehouse is in (`tests/live/compose.live.yaml`).
const BUCKET: &str = "iceberg-live";

/// A name no earlier run used.
fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
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

/// Hands `rows` over as batch `batch`, commits, and waits until the commit lands.
fn deliver(d: &mut dyn Destination, batch: u64, rows: Vec<OwnedRecord>) {
    let (dispatched, refused) = d.deliver(batch, Cow::Owned(rows)).expect("delivered");
    assert!(refused.is_empty(), "{refused:?}");
    if dispatched == Dispatched::Complete {
        return;
    }
    d.flush().expect("flushed");
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(c) = d
            .poll_completions()
            .expect("landed")
            .into_iter()
            .find(|c| c.batch == batch)
        {
            assert!(c.rejections.is_empty(), "{:?}", c.rejections);
            return;
        }
        assert!(Instant::now() < deadline, "no completion");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The table's metadata as the catalog answers it.
fn load(catalog: &str, namespace: &str, table: &str) -> serde_json::Value {
    ureq::get(format!("{catalog}/v1/namespaces/{namespace}/tables/{table}"))
        .call()
        .expect("table loaded")
        .body_mut()
        .read_json()
        .unwrap()
}

/// The table's current snapshot.
fn current(metadata: &serde_json::Value) -> &serde_json::Value {
    let id = &metadata["metadata"]["current-snapshot-id"];
    metadata["metadata"]["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| &s["snapshot-id"] == id)
        .expect("the current snapshot")
}

/// The current snapshot's summary.
fn summary(metadata: &serde_json::Value) -> serde_json::Value {
    current(metadata)["summary"].clone()
}

/// The destination creates the table from the route's record, and each commit is the table's next snapshot, its
/// files in the bucket.
#[test]
fn live_rows_are_committed_as_snapshots_of_a_table_it_creates() {
    let Some(catalog) = live::iceberg("live_rows_are_committed_as_snapshots_of_a_table_it_creates") else {
        return;
    };
    let Some(s3) = live::s3("live_rows_are_committed_as_snapshots_of_a_table_it_creates") else {
        return;
    };
    // The fixture keeps its warehouse in a bucket it does not make. LocalStack answers a bucket made already as made.
    ureq::put(format!("{s3}/{BUCKET}")).send_empty().expect("bucket made");
    let namespace = unique("smeltr");
    ureq::post(format!("{catalog}/v1/namespaces"))
        .send_json(serde_json::json!({ "namespace": [namespace] }))
        .expect("namespace made");

    let plan = harness::compile_ok(vec![harness::code(
        0,
        "host: str; n: i64; Object.assign($, json(_raw));",
    )]);
    let record = harness::record(&plan);
    let config = IcebergDestinationConfig {
        catalog: catalog.clone(),
        warehouse: None,
        sign_in: CatalogSignIn::None,
        namespace: vec![namespace.clone()],
        table: "access".to_string(),
        partition_by: vec!["host".to_string()],
        storage: Some(S3Access {
            region: Some("us-east-1".to_string()),
            endpoint: Some(s3.clone()),
            auth_endpoint: None,
            access_key: Some(AccessKey {
                id: stored("key-id"),
                secret: stored("key"),
            }),
            role: None,
        }),
        compression: Compression::Zstd,
        tls: None,
        in_flight: 2,
        max_object_bytes: 1024 * 1024,
        buffer_bytes: 4 * 1024 * 1024,
        stall_after: Duration::from_secs(30),
        file_interval: Duration::from_secs(60),
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("key-id".to_string(), 1, "test".to_string()),
        ("key".to_string(), 1, "test".to_string()),
    ]);
    let metrics = smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "live");
    let records = smeltr::plan::topology::Records::from([(None, record)]);
    let mut d = smeltr::io::build_destination(&DestinationKind::Iceberg(config), 0, metrics, &secrets, Some(&records))
        .expect("built");

    let rows = |texts: &[&str]| {
        let chunk = harness::cut(&plan, texts);
        (0..texts.len() as u32)
            .map(|row| OwnedRecord {
                payload: smeltr::plan::config::Payload::Record,
                columns: Some(Columns {
                    chunk: chunk.clone(),
                    row,
                }),
                ..OwnedRecord::default()
            })
            .collect::<Vec<_>>()
    };
    deliver(
        d.as_mut(),
        1,
        rows(&[
            r#"{"host":"a","n":1}"#,
            r#"{"host":"b","n":2}"#,
            r#"{"host":"a","n":3}"#,
        ]),
    );

    let first = load(&catalog, &namespace, "access");
    let schema_id = &first["metadata"]["current-schema-id"];
    let columns: Vec<&str> = first["metadata"]["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| &s["schema-id"] == schema_id)
        .expect("the current schema")["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(columns.contains(&"host") && columns.contains(&"n"), "{columns:?}");
    let spec = &first["metadata"]["partition-specs"][0]["fields"][0];
    assert_eq!(spec["transform"], "identity", "{spec}");
    assert_eq!(
        first["metadata"]["properties"]["write.parquet.compression-codec"], "zstd",
        "the table says how every writer after this one writes"
    );
    let s = summary(&first);
    assert_eq!(s["added-records"], "3", "{s}");
    assert_eq!(s["added-data-files"], "2", "one file per host: {s}");

    deliver(d.as_mut(), 2, rows(&[r#"{"host":"c","n":4}"#]));

    let second = load(&catalog, &namespace, "access");
    assert_eq!(
        second["metadata"]["snapshots"].as_array().unwrap().len(),
        2,
        "one snapshot per commit"
    );
    let s = summary(&second);
    assert_eq!(s["total-records"], "4", "{s}");
    assert_eq!(s["total-data-files"], "3", "{s}");

    // The files the snapshot's manifests name are the files in the bucket: one listing, and one read per manifest.
    let read = |uri: &str| {
        let key = uri.strip_prefix("s3://").expect("an S3 address");
        ureq::get(format!("{s3}/{key}"))
            .call()
            .expect("read")
            .body_mut()
            .read_to_vec()
            .unwrap()
    };
    let mut named = Vec::new();
    for m in read_manifest_list(&read(current(&second)["manifest-list"].as_str().unwrap())).unwrap() {
        named.extend(
            read_manifest(&read(&m.manifest_path), &m)
                .unwrap()
                .into_iter()
                .map(|e| e.file_path),
        );
    }
    named.sort();
    let listing = ureq::get(format!("{s3}/{BUCKET}?list-type=2&prefix={namespace}/access/data/"))
        .call()
        .expect("listed")
        .body_mut()
        .read_to_string()
        .unwrap();
    let mut keys: Vec<String> = listing
        .split("<Key>")
        .skip(1)
        .map(|k| format!("s3://{BUCKET}/{}", &k[..k.find("</Key>").unwrap()]))
        .collect();
    keys.sort();
    assert_eq!(named, keys);
}
