//! `serde_json` stays off the hot path.
//!
//! A `serde_json::Value` tree allocates for every object, string and array it reads, and is thrown away after one
//! read. What reads or writes records, request bodies or captured rows does it with `format::json_scan` and
//! `format::json`. `serde_json` is for what runs once per poll, commit, connection or request to the leader: config,
//! the leader, its API, and the cloud services' own answers.
//!
//! A file under `src/` names `serde_json` only if `ALLOWED` lists it, by its path or a directory holding it, with the
//! reason. Comments, `#[cfg(test)]` items and the fuzzers (`fuzz*.rs`) are not read: tests check against
//! `serde_json` as an oracle.

use std::fs;

use crate::{collect, repository};

/// The files that may name `serde_json`, by path from the checkout or a directory ending in `/`, each with its reason.
/// An entry nothing under it still needs fails the check, so the list only shrinks.
const ALLOWED: &[(&str, &str)] = &[
    // Once per request to the leader, poll, commit or connection.
    ("src/io/stored.rs", "a stored record, read once per request"),
    ("src/plan/topology/stored.rs", "a stored record, read once per request"),
    ("src/worker/capture.rs", "a captured row, at most a capture's bound"),
    ("src/bin/smeltr/", "the leader's API, one request at a time"),
    ("src/bin/smeltr-demo/", "the demo's preview API, one request at a time"),
    ("src/config_file/", "config, read and written as it changes"),
    ("src/leader/", "the leader's state, store and sign-in"),
    ("src/heartbeat.rs", "one message per heartbeat"),
    ("src/releases.rs", "the release fixtures, a test-only module"),
    (
        "src/plan/dictionary.rs",
        "a sample's types, read when a pipeline compiles",
    ),
    (
        "src/plan/tree.rs",
        "a path's quoted keys, read when a pipeline compiles",
    ),
    (
        "src/plan/topology.rs",
        "a stream's route condition, written once per onboarding proposal",
    ),
    (
        "src/worker/child.rs",
        "one frame per message between a worker and its child",
    ),
    ("src/worker/held.rs", "the progress ledger, one line per commit"),
    ("src/worker/mod.rs", "messages to the leader"),
    ("src/io/client_credentials.rs", "a token, once per expiry"),
    ("src/io/discovery.rs", "an OpenID provider's document, once per sign-in"),
    ("src/io/oauth.rs", "a token, once per expiry"),
    ("src/io/pubsub.rs", "Pub/Sub's API, once per poll and ack"),
    ("src/io/sqs.rs", "SQS's API, once per poll and ack"),
    ("src/io/storage_queue.rs", "a storage notification, once per object"),
    ("src/io/listing.rs", "an object's tag, once per object listed"),
    ("src/io/elastic_mapping.rs", "an index template, once per index"),
    ("src/io/elastic_nodes.rs", "a cluster's node list, once per refresh"),
    (
        "src/io/sql_source.rs",
        "a stored position, read once as the source resumes",
    ),
    ("src/io/delta/", "the Delta log, once per commit"),
    ("src/io/iceberg/", "Iceberg's metadata, once per commit"),
    ("src/io/avro.rs", "a route's record as an Avro schema, once per schema"),
    ("src/io/avro/", "a schema registry's answers, once per schema"),
    ("src/schema/json_schema.rs", "a bundle's schema, once per load"),
    ("src/schema/csv.rs", "a file read as a schema, once per read"),
    ("src/schema/ecs.rs", "a file read as a schema, once per read"),
    ("src/schema/ocsf.rs", "a file read as a schema, once per read"),
    (
        "src/schema/standard.rs",
        "the OCSF release carried, parsed for each read of it",
    ),
    ("src/schema.rs", "a field's Elasticsearch mapping, once per load"),
];

/// The lines of `text` that name `serde_json`, 1-based, outside comments and `#[cfg(test)]` items. An item ends at
/// the first line that closes a brace at its attribute's indent, as rustfmt writes it.
fn naming(text: &str) -> Vec<usize> {
    let lines: Vec<&str> = text.lines().collect();
    let mut found = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        if line.trim() == "#[cfg(test)]" {
            let indent = &line[..line.len() - line.trim_start().len()];
            let mut j = i + 1;
            while lines.get(j).is_some_and(|l| l.trim_start().starts_with("#[")) {
                j += 1;
            }
            if lines.get(j).is_some_and(|l| !l.trim_end().ends_with(';')) {
                let close = format!("{indent}}}");
                while lines.get(j).is_some_and(|l| !l.starts_with(&close)) {
                    j += 1;
                }
            }
            i = j + 1;
            continue;
        }
        if code(line).contains("serde_json") {
            found.push(i + 1);
        }
        i += 1;
    }
    found
}

#[test]
fn serde_json_stays_off_the_hot_path() {
    let root = &repository();
    let mut files = Vec::new();
    collect(&root.join("src"), &["rs"], &mut files);
    let mut naming_files = Vec::new();
    let mut found = Vec::new();
    for path in &files {
        let shown = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let fuzzer = path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("fuzz"));
        let Ok(text) = fs::read_to_string(path) else { continue };
        let lines = naming(&text);
        if fuzzer || lines.is_empty() {
            continue;
        }
        if !ALLOWED.iter().any(|(allowed, _)| covers(allowed, &shown)) {
            found.extend(lines.iter().map(|n| format!("  {shown}:{n}")));
        }
        naming_files.push(shown);
    }
    assert!(
        found.is_empty(),
        "serde_json is named where records, request bodies or captured rows are read or written -- use \
         format::json_scan and format::json, or, for what runs once per poll, commit or request to the leader, \
         list the file in ALLOWED with its reason:\n{}",
        found.join("\n")
    );

    let stale: Vec<_> = (ALLOWED.iter())
        .filter(|(allowed, _)| !naming_files.iter().any(|f| covers(allowed, f)))
        .map(|(allowed, _)| format!("  {allowed}"))
        .collect();
    assert!(
        stale.is_empty(),
        "ALLOWED lists what no longer names serde_json -- take it off:\n{}",
        stale.join("\n")
    );
}

/// `line` up to its comment: the first `//` outside a string.
fn code(line: &str) -> &str {
    let mut quoted = false;
    let bytes = line.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'"' if i == 0 || bytes[i - 1] != b'\\' => quoted = !quoted,
            b'/' if !quoted && bytes.get(i + 1) == Some(&b'/') => return &line[..i],
            _ => {}
        }
    }
    line
}

/// Whether `allowed`, a file or a directory ending in `/`, covers `file`.
fn covers(allowed: &str, file: &str) -> bool {
    match allowed.ends_with('/') {
        true => file.starts_with(allowed),
        false => file == allowed,
    }
}

#[test]
fn a_test_item_and_a_comment_name_nothing() {
    let text = "use serde_json::Value;\n// serde_json\n#[cfg(test)]\nuse serde_json::json;\n#[cfg(test)]\n\
                mod tests {\n    use serde_json::json;\n}\nfn f() {\n    #[cfg(test)]\n    #[allow(unused)]\n    \
                fn g() {\n        serde_json::json!({});\n    }\n    serde_json::to_string(&1);\n}\n";
    assert_eq!(naming(text), [1, 15]);
    let text = "#[cfg(test)]\nconst X: u8 = {\n    1\n};\nfn f() { serde_json::to_string(&1); }\n\
                let u = \"http://x\"; serde_json::to_string(&u);\n";
    assert_eq!(naming(text), [5, 6]);
    let text = "#[cfg(test)]\nfn h(\n    a: u8,\n) -> u8 {\n    serde_json::json!(a);\n}\nserde_json::json!(1);\n";
    assert_eq!(naming(text), [7]);
}
