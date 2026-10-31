//! The Delta destination against LocalStack's S3, as `just live-up` runs it.
//!
//! `io::delta`'s unit tests cover the log, its commits and its checkpoints against an in-memory store. This file is
//! the contract with a real S3 API: a commit is written only where none is, two destinations on one table take turns
//! in its log, and the log's adds name exactly the data files in the bucket.
//!
//! Point it at `S3_TEST_ENDPOINT`. Under `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips.

use crate::harness;
use crate::live;

use std::borrow::Cow;
use std::time::{Duration, Instant};

use smeltr::io::parquet::Columns;
use smeltr::io::{Destination, Dispatched, OwnedRecord};
use smeltr::plan::topology::{
    AccessKey, Compression, DeltaDestinationConfig, DestinationKind, HeldSecrets, S3Access, SecretRef,
};

const BUCKET: &str = "delta-live";

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

/// The keys under `prefix` in the bucket, sorted.
fn keys(s3: &str, prefix: &str) -> Vec<String> {
    let listing = ureq::get(format!("{s3}/{BUCKET}?list-type=2&prefix={prefix}"))
        .call()
        .expect("listed")
        .body_mut()
        .read_to_string()
        .unwrap();
    let mut keys: Vec<String> = listing
        .split("<Key>")
        .skip(1)
        .map(|k| k[..k.find("</Key>").unwrap()].to_string())
        .collect();
    keys.sort();
    keys
}

fn read(s3: &str, key: &str) -> String {
    ureq::get(format!("{s3}/{BUCKET}/{key}"))
        .call()
        .expect("read")
        .body_mut()
        .read_to_string()
        .unwrap()
}

/// Each JSON line of commit `version` of the table at `root`.
fn commit(s3: &str, root: &str, version: u64) -> Vec<serde_json::Value> {
    read(s3, &format!("{root}/_delta_log/{version:020}.json"))
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// The destination creates a plain table from the route's record, and each commit is the log's next entry, its adds
/// naming the files in the bucket under the record's own column names. A second destination on the table takes its
/// turn in the same log, and the commit that reaches the checkpoint interval is checkpointed before the next.
#[test]
fn live_rows_are_committed_to_a_delta_log_it_creates() {
    let Some(s3) = live::s3("live_rows_are_committed_to_a_delta_log_it_creates") else {
        return;
    };
    // LocalStack answers a bucket made already as made.
    ureq::put(format!("{s3}/{BUCKET}")).send_empty().expect("bucket made");
    let root = unique("access");

    let plan = harness::compile_ok(vec![harness::code(
        0,
        "host: str; n: i64; Object.assign($, json(_raw));",
    )]);
    let record = harness::record(&plan);
    let config = DeltaDestinationConfig {
        location: format!("s3://{BUCKET}/{root}"),
        unity_catalog: None,
        partition_by: vec!["host".to_string()],
        storage: S3Access {
            region: Some("us-east-1".to_string()),
            endpoint: Some(s3.clone()),
            auth_endpoint: None,
            access_key: Some(AccessKey {
                id: stored("key-id"),
                secret: stored("key"),
            }),
            role: None,
        },
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
    let build = || {
        smeltr::io::build_destination(
            &DestinationKind::Delta(config.clone()),
            0,
            metrics.clone(),
            &secrets,
            Some(&records),
        )
        .expect("built")
    };
    let mut d = build();
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

    let created = commit(&s3, &root, 0);
    let protocol = &created.iter().find(|a| a.get("protocol").is_some()).unwrap()["protocol"];
    assert_eq!(
        *protocol,
        serde_json::json!({"minReaderVersion": 1, "minWriterVersion": 2})
    );
    let metadata = &created.iter().find(|a| a.get("metaData").is_some()).unwrap()["metaData"];
    assert_eq!(metadata["partitionColumns"], serde_json::json!(["host"]));
    let first = commit(&s3, &root, 1);
    let adds: Vec<&serde_json::Value> = first.iter().filter_map(|a| a.get("add")).collect();
    assert_eq!(adds.len(), 2, "one file per host: {first:?}");
    assert!(
        adds.iter().all(|a| a["path"].as_str().unwrap().starts_with("host=")),
        "{adds:?}"
    );
    let stats: serde_json::Value = serde_json::from_str(adds[0]["stats"].as_str().unwrap()).unwrap();
    assert!(stats["numRecords"].as_u64().unwrap() >= 1, "{stats}");
    let txn = &first.iter().find(|a| a.get("txn").is_some()).unwrap()["txn"];
    assert_eq!(txn["version"], 1, "{txn}");

    // A second destination on the table: each commit lands on the other's, in one log with no gap.
    let mut other = build();
    for batch in 2..12 {
        let d = if batch % 2 == 0 { d.as_mut() } else { other.as_mut() };
        let host = if batch % 2 == 0 { "c" } else { "d" };
        deliver(d, batch, rows(&[&format!(r#"{{"host":"{host}","n":{batch}}}"#)]));
    }
    let log = keys(&s3, &format!("{root}/_delta_log/"));
    let commits: Vec<&String> = log
        .iter()
        .filter(|k| k.ends_with(".json") && !k.contains(".checkpoint."))
        .collect();
    assert_eq!(commits.len(), 12, "commit 0 and eleven of rows: {log:?}");
    // Version 10 reached the checkpoint interval: its writer checkpoints it before its next commit.
    deliver(d.as_mut(), 12, rows(&[r#"{"host":"c","n":12}"#]));
    deliver(other.as_mut(), 13, rows(&[r#"{"host":"d","n":13}"#]));
    let log = keys(&s3, &format!("{root}/_delta_log/"));
    let checkpoint = format!("{root}/_delta_log/00000000000000000010.checkpoint.parquet");
    assert!(log.contains(&checkpoint), "{log:?}");
    let last: serde_json::Value =
        serde_json::from_str(&read(&s3, &format!("{root}/_delta_log/_last_checkpoint"))).unwrap();
    assert_eq!(last["version"], 10, "{last}");

    // The log's adds are the data files in the bucket.
    let mut named = Vec::new();
    for version in 1..=13 {
        for action in commit(&s3, &root, version) {
            if let Some(path) = action["add"]["path"].as_str() {
                named.push(format!("{root}/{path}"));
            }
        }
    }
    named.sort();
    let files: Vec<String> = keys(&s3, &format!("{root}/"))
        .into_iter()
        .filter(|k| !k.contains("/_delta_log/"))
        .collect();
    assert_eq!(named, files);
}

/// A Databricks workspace on 127.0.0.1 whose Unity Catalog places `catalog.web.access` at `location` and vends
/// LocalStack's keys for it, to every caller, until the test ends.
fn workspace(location: String) -> String {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let mut reader = BufReader::new(conn.unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let Some((name, value)) = header.trim_end().split_once(':') else {
                    break;
                };
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap();
                }
            }
            reader.read_exact(&mut vec![0; length]).unwrap();
            let in_an_hour = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + 3_600_000;
            let body = if line.starts_with("POST /oidc/v1/token") {
                serde_json::json!({"access_token": "t", "expires_in": 3600})
            } else if line.starts_with("GET /api/2.1/unity-catalog/tables/catalog.web.access") {
                serde_json::json!({
                    "table_id": "6f1c",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "storage_location": location,
                })
            } else {
                serde_json::json!({
                    "aws_temp_credentials": {"access_key_id": "test", "secret_access_key": "test", "session_token": "x"},
                    "expiration_time": in_an_hour as u64,
                })
            }
            .to_string();
            let mut conn = reader.into_inner();
            write!(
                conn,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    url
}

/// A table reached through Unity Catalog is found at the location it names and written with the credentials it vends:
/// no bucket key is configured.
#[test]
fn live_a_unity_catalog_table_is_written_with_the_credentials_it_vends() {
    let Some(s3) = live::s3("live_a_unity_catalog_table_is_written_with_the_credentials_it_vends") else {
        return;
    };
    ureq::put(format!("{s3}/{BUCKET}")).send_empty().expect("bucket made");
    let root = unique("unity");
    let plan = harness::compile_ok(vec![harness::code(
        0,
        "host: str; n: i64; Object.assign($, json(_raw));",
    )]);
    let record = harness::record(&plan);
    let config = DeltaDestinationConfig {
        location: String::new(),
        unity_catalog: Some(smeltr::plan::topology::UnityCatalogTable {
            workspace: workspace(format!("s3://{BUCKET}/{root}")),
            table: "catalog.web.access".to_string(),
            client_id: "app".to_string(),
            client_secret: stored("client-secret"),
        }),
        partition_by: Vec::new(),
        storage: S3Access {
            region: Some("us-east-1".to_string()),
            endpoint: Some(s3.clone()),
            auth_endpoint: None,
            access_key: None,
            role: None,
        },
        compression: Compression::Zstd,
        tls: None,
        in_flight: 2,
        max_object_bytes: 1024 * 1024,
        buffer_bytes: 4 * 1024 * 1024,
        stall_after: Duration::from_secs(30),
        file_interval: Duration::from_secs(60),
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("client-secret".to_string(), 1, "s3cr3t".to_string())]);
    let metrics = smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "live");
    let records = smeltr::plan::topology::Records::from([(None, record)]);
    let mut d = smeltr::io::build_destination(&DestinationKind::Delta(config), 0, metrics, &secrets, Some(&records))
        .expect("built");
    let chunk = harness::cut(&plan, &[r#"{"host":"a","n":1}"#]);
    let rows = vec![OwnedRecord {
        payload: smeltr::plan::config::Payload::Record,
        columns: Some(Columns { chunk, row: 0 }),
        ..OwnedRecord::default()
    }];
    deliver(d.as_mut(), 1, rows);

    let added: Vec<String> = commit(&s3, &root, 1)
        .iter()
        .filter_map(|a| a.get("add"))
        .map(|add| format!("{root}/{}", add["path"].as_str().unwrap()))
        .collect();
    assert_eq!(added.len(), 1, "{added:?}");
    assert!(keys(&s3, &root).contains(&added[0]), "{added:?}");
}
