//! The S2S receiver against a *real* universal forwarder.
//!
//! `tests/s2s-receiver-tests.rs` covers the protocol against a fake forwarder, which sends what it was written to send
//! and so agrees with the receiver by construction. This file is the contract with the forwarder itself: the forwarder
//! in `tests/live/compose.live.yaml` reads the files this test writes and clones them to three receivers, one for each way a
//! forwarder sends (plain, with acks, compressed with acks), as `tests/fixtures/s2s-forwarder` says. Each receiver
//! must hold every line once, a multiline event and a line longer than the forwarder's ack window whole, and the host, source and sourcetype the forwarder's inputs give
//! them. Each Splunk release can change what a forwarder sends; this check against the newest is what notices.
//!
//! `S2S_TEST_FORWARDER` names the directory under the checkout that the forwarder reads. Under
//! `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips.

use crate::live;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use smeltr::io::line_breaker::LineBreaker;
use smeltr::io::{CommitMap, Source, SourceFields, SourceRecord, TopicPartition, build_source};
use smeltr::plan::topology::{
    HeldSecrets, Receiver, ReceiverConfig, S2sReceiverConfig, SecretRef, SourceKind, SourceMeta,
};

/// The token every group in the fixture's outputs.conf presents.
const TOKEN: &str = "5b6f0c2e-8a41-4d7e-9c3b-1f2a4e6d8c90";

/// Each group in the fixture's outputs.conf, and the port it sends to.
const GROUPS: [(&str, u16); 3] = [("plain", 19981), ("acked", 19982), ("compressed", 19983)];

/// The access lines written: half a megabyte, more than one chunk of a file.
const LINES: usize = 3000;

/// How long the long line is: past the acked groups' windows, three times their `maxQueueSize`, and within what a
/// receiver holds.
const LONG: usize = 600 * 1024;

/// How long a receiver may take to hold what was written. A forwarder that found no receiver waits out a quarantine
/// before it tries again.
const PATIENCE: Duration = Duration::from_secs(240);

/// How long the long line may take once every receiver holds a record. A forwarder stuck on it sends heartbeats 200 ms
/// apart, and the receiver acks it early on the second; the 30 s quiet cut would take longer. A forwarder that sends
/// them outside 100 to 500 ms apart is one the early ack no longer recognizes.
const UNBLOCKED: Duration = Duration::from_secs(10);

/// A receiver for one group, on every interface: the forwarder reaches it from its container through the host's
/// gateway. It breaks `smeltr:multiline` before lines starting `START`.
fn receiver(port: u16) -> Box<dyn Source> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("forwarders".to_string(), 1, TOKEN.to_string())]);
    let line_breaker = LineBreaker(regex::bytes::Regex::new(r"([\r\n]+)START").unwrap());
    let config = ReceiverConfig {
        receiver: Receiver::S2s(S2sReceiverConfig {
            sourcetypes: BTreeMap::from([("smeltr:multiline".to_string(), line_breaker)]),
        }),
        listen: SocketAddr::from(([0, 0, 0, 0], port)),
        tokens: vec![(
            "forwarders".to_string(),
            SecretRef::LeaderStored {
                name: "forwarders".into(),
                version: 1,
            },
        )],
        identity: None,
        max_request_bytes: 1 << 20,
    };
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    build_source(&SourceKind::Receiver(config), &registry, &subjects, &secrets, None).expect("opens")
}

/// Prints what the receivers warn of, such as why a forwarder's connection was closed and the byte it could not read.
/// The test fails on what did not arrive; this says why.
struct Warnings;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warnings {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0 += &format!(" {}={value:?}", field.name());
            }
        }
        if *event.metadata().level() <= tracing::Level::WARN {
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            eprintln!(
                "{} {}:{}",
                event.metadata().level(),
                event.metadata().target(),
                fields.0
            );
        }
    }
}

/// What `source` holds now, and its `__source`, committed.
fn poll(source: &mut Box<dyn Source>) -> Vec<(String, SourceFields)> {
    let polled = source
        .poll_batch(1000, 4 << 20, Duration::from_millis(50))
        .expect("polled");
    commit(source, &polled);
    let fields = SourceMeta::S2S_RECEIVER.objects(&polled, Some("s2s"));
    polled.iter().map(text).zip(fields).collect()
}

/// Commits every record in `records`, which acks them to a forwarder that asked for acks.
fn commit(source: &mut Box<dyn Source>, records: &[SourceRecord]) {
    let mut done = CommitMap::new();
    for r in records {
        let at = TopicPartition::new(r.topic.clone(), r.partition);
        let o = done.entry(at).or_insert(r.offset);
        *o = (*o).max(r.offset);
    }
    source.commit(&done).expect("committed");
}

fn text(record: &SourceRecord) -> String {
    String::from_utf8_lossy(record.bytes().unwrap()).into_owned()
}

/// The files this test wrote, removed when it ends however it ends.
struct Written(Vec<PathBuf>);

impl Drop for Written {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Writes `body` whole, so the forwarder never reads a line half written.
fn write(dir: &Path, name: &str, body: &str, written: &mut Written) {
    let path = dir.join(name);
    written.0.push(path.clone());
    std::fs::write(&path, body).expect("writing a file the forwarder reads");
}

/// Line `n` of the access log.
fn access_line(tag: &str, n: usize) -> String {
    let agent = "smeltr-live ".repeat(8);
    format!(r#"10.0.0.1 - - [11/Oct/2026:03:00:01 +0000] "GET /{tag}/{n} HTTP/1.1" 200 123 "-" "{agent}""#)
}

/// One forwarder, three receivers: what it reads arrives at each, every line once and the multiline event whole, under
/// the host, source and sourcetype its inputs give. The acked receivers commit what they take, which acks it.
#[test]
fn a_universal_forwarder_is_received_plain_acked_and_compressed() {
    let Some(dir) = live::service(
        "a_universal_forwarder_is_received_plain_acked_and_compressed",
        "S2S_TEST_FORWARDER",
        "a universal forwarder (`just live-up`)",
    ) else {
        return;
    };
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(dir);
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::layer::SubscriberExt::with(
        tracing_subscriber::registry(),
        Warnings,
    ));
    std::fs::create_dir_all(&dir).expect("the directory the forwarder reads");
    let mut receivers: Vec<(&str, Box<dyn Source>)> =
        GROUPS.iter().map(|&(group, port)| (group, receiver(port))).collect();

    let tag = format!(
        "s2s-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut written = Written(Vec::new());

    // Every receiver holds the probe before the files are written. A forwarder connects to each group on its own retry,
    // and holds back the groups it has until it has them all: a line held back for 30 s is cut.
    let probe_source = format!("/smeltr/target/s2s-forwarder/access-probe-{tag}.log");
    write(
        &dir,
        &format!("access-probe-{tag}.log"),
        &format!("PROBE {tag}\n"),
        &mut written,
    );
    let mut probed = vec![false; receivers.len()];
    let deadline = Instant::now() + PATIENCE;
    while probed.contains(&false) && Instant::now() < deadline {
        for ((_, source), probed) in receivers.iter_mut().zip(&mut probed) {
            *probed |= poll(source)
                .iter()
                .any(|(_, fields)| fields["source"] == probe_source.as_str());
        }
    }
    let unprobed: Vec<&str> = receivers
        .iter()
        .zip(&probed)
        .filter(|(_, p)| !**p)
        .map(|((g, _), _)| *g)
        .collect();
    assert!(
        unprobed.is_empty(),
        "{unprobed:?}: the forwarder sent no probe within {PATIENCE:?}"
    );

    let access: String = (0..LINES).map(|n| access_line(&tag, n) + "\n").collect();
    write(&dir, &format!("access-{tag}.log"), &access, &mut written);
    let long = format!("LONG {tag} {}", "y".repeat(LONG));
    write(
        &dir,
        &format!("access-long-{tag}.log"),
        &format!("{long}\n"),
        &mut written,
    );
    let event = format!("START {tag}\n  continuation a\n  continuation b");
    write(&dir, &format!("multi-{tag}.log"), &format!("{event}\n"), &mut written);

    // The path the forwarder reads the access log at, in its container.
    let access_source = format!("/smeltr/target/s2s-forwarder/access-{tag}.log");
    let long_source = format!("/smeltr/target/s2s-forwarder/access-long-{tag}.log");
    let ours = |raw: &str, fields: &SourceFields| match fields["source"].as_str() {
        Some(source) if source == access_source || source == long_source => true,
        Some("smeltr_multiline") => raw.contains(&tag),
        _ => false,
    };
    // Each receiver's records from the files written, and their `__source`. The others, the forwarder's own logs, are
    // committed and dropped.
    let mut held: Vec<Vec<(String, SourceFields)>> = receivers.iter().map(|_| Vec::new()).collect();
    // When each receiver held its first record, and the long line.
    let mut first: Vec<Option<Instant>> = receivers.iter().map(|_| None).collect();
    let mut long_at: Vec<Option<Instant>> = receivers.iter().map(|_| None).collect();
    let deadline = Instant::now() + PATIENCE;
    while held.iter().any(|records| records.len() < LINES + 2) && Instant::now() < deadline {
        for (((_, source), held), (first, long_at)) in receivers
            .iter_mut()
            .zip(&mut held)
            .zip(first.iter_mut().zip(&mut long_at))
        {
            held.extend(poll(source).into_iter().filter(|(raw, fields)| ours(raw, fields)));
            if !held.is_empty() {
                first.get_or_insert_with(Instant::now);
            }
            if held.iter().any(|(_, fields)| fields["source"] == long_source.as_str()) {
                long_at.get_or_insert_with(Instant::now);
            }
        }
    }

    // A receiver that held nothing fails below on its missing lines.
    let all_held = first.iter().flatten().max().copied();
    for (((group, _), records), long_at) in receivers.iter().zip(&held).zip(&long_at) {
        let mut lines = BTreeMap::new();
        let mut events = Vec::new();
        let mut longs = Vec::new();
        for (raw, fields) in records {
            if fields["source"] == long_source.as_str() {
                longs.push(raw.len());
                continue;
            }
            if fields["sourcetype"] == "smeltr:multiline" {
                assert_eq!(
                    fields["host"], "s2s-forwarder",
                    "{group}: with no host in its inputs, a file's host is the forwarder's"
                );
                assert_eq!(fields["source"], "smeltr_multiline", "{group}");
                events.push(raw.as_str());
                continue;
            }
            assert_eq!(fields["sourcetype"], "access_combined", "{group}: {raw}");
            assert_eq!(fields["host"], "webhost01", "{group}");
            assert_eq!(fields["index"], "main", "{group}");
            let n: usize = raw
                .split(&format!("/{tag}/"))
                .nth(1)
                .and_then(|rest| rest.split(' ').next())
                .and_then(|n| n.parse().ok())
                .unwrap_or_else(|| panic!("{group}: not a line written: {raw:?}"));
            assert_eq!(*raw, access_line(&tag, n), "{group}: a line is one record, whole");
            *lines.entry(n).or_insert(0) += 1;
        }
        let missing: Vec<usize> = (0..LINES).filter(|n| !lines.contains_key(n)).collect();
        let twice: Vec<(&usize, &i32)> = lines.iter().filter(|(_, c)| **c > 1).collect();
        assert!(
            missing.is_empty(),
            "{group}: {} of {LINES} lines did not arrive within {PATIENCE:?}, from line {:?}",
            missing.len(),
            missing.first()
        );
        assert!(twice.is_empty(), "{group}: lines arrived more than once: {twice:?}");
        assert_eq!(
            events,
            [event.as_str()],
            "{group}: the multiline event arrives once, whole"
        );
        assert_eq!(
            longs,
            [long.len()],
            "{group}: a line longer than the forwarder's window arrives once, whole"
        );
        let took = long_at.unwrap().saturating_duration_since(all_held.unwrap());
        assert!(
            took < UNBLOCKED,
            "{group}: the long line arrived {took:?} after every receiver held a record, so the early ack did not \
             unblock it: did the forwarder's heartbeats while it waits stop coming 100 to 500 ms apart?"
        );
    }
}
