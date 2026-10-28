//! Every name a comment or a document cites in backticks exists.
//!
//! `website-reference-tests` holds the published function reference to the engine on the principle that a reference
//! nobody runs drifts. The same is true of the ~1800 names `src/` comments and `docs/` cite in passing: a rename
//! leaves them behind, and nothing notices. Twelve had already rotted when this test was written, seven of which
//! survived a full manual read of the same files -- prose is not where this class of error is visible.
//!
//! A cited name resolves if it appears anywhere in the tree outside backticks, or names a file. Anything else is
//! either dead or external, and an external one is declared below with its reason.

#![cfg(test)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

mod hot_path;

/// Cited names nothing in this tree defines, and why each is correct anyway: it belongs to another project, a
/// proposal coins it for something not yet built, or prose uses it as a shape rather than a name.
///
/// Each carries its reason, the way `RESERVED_KAFKA_PROPS` does. Adding a real one is a line and a justification;
/// silencing a dead citation by adding it here is visible in review, and a reason reading "not built" next to a name
/// a document uses in the present tense is itself the finding.
const NOT_DEFINED_HERE: &[(&str, &str)] = &[
    (
        "_RJEM_MALLOC_CONF",
        "the environment jemalloc reads its settings from, under tikv-jemalloc-sys's prefix",
    ),
    (
        "CARGO_TARGET_DIR",
        "the environment cargo reads its build directory from",
    ),
    // librdkafka and the rdkafka crate.
    ("RD_KAFKA_EVENT_ERROR", "librdkafka's event type"),
    ("WITH_SASL_CYRUS", "librdkafka build flag"),
    ("_WIN32", "the C preprocessor's, which librdkafka tests for SSPI"),
    ("rd_kafka_new", "librdkafka's client constructor"),
    (
        "rd_avg_rollover",
        "librdkafka's window reset, which makes a stats window a delta",
    ),
    (
        "ThreadedProducer",
        "the rdkafka crate's other producer, which smeltr does not use",
    ),
    ("Offset::Invalid", "the rdkafka crate's sentinel offset"),
    ("sasl_gssapi", "a librdkafka `builtin.features` value"),
    // The operating system's own calls.
    ("System32", "the Windows directory a service starts in"),
    (
        "setsid",
        "the unix call a process leaves its session by, escaping a tree kill",
    ),
    (
        "setpgid",
        "the unix call a process leaves its group by, escaping a tree kill",
    ),
    (
        "waitpid",
        "the unix call that reaps a child, cited for the flag it does not take",
    ),
    (
        "PR_SET_PDEATHSIG",
        "the linux prctl flag that signals a child on its parent's death, cited as a gap not built",
    ),
    (
        "TIME_WAIT",
        "the TCP state a closed listener's port sits in until the OS reclaims it, cited as what SO_REUSEADDR skips",
    ),
    // Other languages and runtimes.
    ("RangeError", "JavaScript's, thrown by the Temporal API"),
    ("RFC3339Nano", "Go's layout constant, cited for contrast"),
    ("epochMilliseconds", "the Temporal API's property"),
    ("fromEpochMilliseconds", "the Temporal API's constructor"),
    ("relativeTo", "the Temporal API's option"),
    ("beforetoggle", "a DOM event"),
    ("to_entries", "jq's builtin, cited as prior art"),
    // Crates and tools smeltr depends on or names.
    (
        "serde_urlencoded",
        "what axum's `Query` decodes with, whose handling of a repeated key shapes the logs query",
    ),
    ("chrono", "the crate smeltr deliberately does not depend on"),
    ("cmake_build", "the rdkafka crate's feature"),
    ("mmdbinspect", "MaxMind's own inspection tool"),
    ("journal_size_limit", "a SQLite pragma"),
    ("Transaction", "rusqlite's, which rolls back as it drops"),
    (
        "reqwest",
        "gix-transport's other HTTP backend, named for contrast with the curl one smeltr uses",
    ),
    (
        "ssl_verify",
        "a field of gix-transport's http::Options, cited for the default a worker's fetch relies on",
    ),
    // Databricks, named by a proposal that has not been built.
    ("_delta_log", "Delta Lake's transaction log directory"),
    ("deltalake", "a crate a proposal would need"),
    ("object_store", "a crate a proposal would need"),
    // Splunk forwarders: their settings and the fields they send over S2S.
    ("httpout", "a Splunk forwarder's outputs.conf stanza"),
    ("_meta", "a Splunk forwarder's inputs.conf setting"),
    ("_hpn", "a field a Splunk forwarder sends"),
    ("_charSet", "a field a Splunk forwarder sends"),
    ("_nfd", "a field a Splunk forwarder sends"),
    ("_linebreaker", "a field a Splunk forwarder sends"),
    (
        "channel_limit",
        "what a Splunk indexer tells a forwarder in its S2S handshake",
    ),
    // Timezone abbreviations, cited as parse inputs.
    ("CDT", "a timezone abbreviation"),
    ("CST", "a timezone abbreviation"),
    ("EDT", "a timezone abbreviation"),
    ("EST", "a timezone abbreviation"),
    ("MDT", "a timezone abbreviation"),
    ("MST", "a timezone abbreviation"),
    ("PDT", "a timezone abbreviation"),
    ("PST", "a timezone abbreviation"),
    // Names a proposal or a review coins for something not yet built.
    ("dispatch_chunked", "named by a proposal, not built"),
    (
        "__brk",
        "a spelling weighed for the loop-break variable and not adopted",
    ),
    // Named by a design that is not built. Each is a finding, parked here so the check can hold the rest of the
    // tree while it is settled.
    (
        "dispatch_chunked",
        "status.md's \"Unroll's output is not bounded at dispatch\" note names this; nothing implements it",
    ),
    (
        "max_dispatch_rows",
        "the bound status.md says `dispatch_chunked` applies; neither exists",
    ),
    (
        "max_dispatch_bytes",
        "the bound status.md says `dispatch_chunked` applies; neither exists",
    ),
    // Prose: a fragment, a placeholder, or a stand-in for a shape.
    ("JanX", "a shape a parse rejects, not a value"),
    ("add_", "a prefix, cited as a naming shape"),
    ("set_", "a prefix, cited as a naming shape"),
    ("gc", "shorthand for garbage collection"),
    ("_no_replacement", "a stand-in for what a decoder does not do"),
];

fn collect(root: &Path, exts: &[&str], out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else { return };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            if p.file_name()
                .is_some_and(|n| n == "node_modules" || n == "target" || n == ".git")
            {
                continue;
            }
            collect(&p, exts, out);
        } else if p.extension().is_some_and(|x| exts.iter().any(|w| x == *w)) {
            out.push(p);
        }
    }
}

/// Every backtick-delimited span, with the 1-based line it starts on. Spans never cross a blank line, so a stray
/// backtick in prose cannot swallow the rest of a file.
fn backticked(text: &str) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut line = 1;
    while i < bytes.len() {
        if bytes[i] == b'\n' {
            line += 1;
            i += 1;
            continue;
        }
        if bytes[i] != b'`' {
            i += 1;
            continue;
        }
        let start = i + 1;
        let mut j = start;
        while j < bytes.len() && bytes[j] != b'`' && bytes[j] != b'\n' {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'`' {
            out.push((text[start..j].to_string(), line));
            i = j + 1;
        } else {
            i = start;
        }
    }
    out
}

/// A citation this test can check: a Rust path, or one identifier. Anything with a space, a bracket or an operator is
/// prose or an expression, and belongs to a reader rather than to this test.
fn is_citation(s: &str) -> bool {
    if s.is_empty() || s.len() > 120 {
        return false;
    }
    s.split("::").all(|seg| {
        !seg.is_empty()
            && seg.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

fn without_backticks(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(a) = rest.find('`') {
        out.push_str(&rest[..a]);
        let after = &rest[a + 1..];
        match after.find(['`', '\n']) {
            Some(b) if after.as_bytes()[b] == b'`' => rest = &after[b + 1..],
            _ => {
                rest = after;
            }
        }
        out.push(' ');
    }
    out.push_str(rest);
    out
}

fn contains_word(haystack: &str, word: &str) -> bool {
    let mut from = 0;
    while let Some(at) = haystack[from..].find(word) {
        let s = from + at;
        let e = s + word.len();
        let before_ok = s == 0 || !is_word_byte(haystack.as_bytes()[s - 1]);
        let after_ok = e == haystack.len() || !is_word_byte(haystack.as_bytes()[e]);
        if before_ok && after_ok {
            return true;
        }
        from = s + 1;
    }
    false
}

/// The checkout this crate sits in, two directories up.
fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[test]
fn every_name_a_comment_cites_exists() {
    let root = &repository();

    // Where a name may be defined: any source anywhere, plus the names of the files themselves.
    let mut defining = Vec::new();
    for (dir, exts) in [
        ("src", &["rs"][..]),
        ("tests", &["rs"][..]),
        ("tools", &["rs"][..]),
        ("gui-frontend/src", &["ts", "svelte"][..]),
        ("website/src", &["ts", "svelte", "json"][..]),
        ("docs", &["md", "py"][..]),
    ] {
        collect(&root.join(dir), exts, &mut defining);
    }
    let mut corpus = String::new();
    let mut stems: Vec<String> = Vec::new();
    for p in &defining {
        if let Ok(t) = fs::read_to_string(p) {
            corpus.push_str(&without_backticks(&t));
            corpus.push('\n');
        }
        if let Some(s) = p.file_stem().and_then(|s| s.to_str()) {
            stems.push(s.to_ascii_lowercase());
        }
    }
    for extra in ["justfile", "Cargo.toml", "CLAUDE.md"] {
        if let Ok(t) = fs::read_to_string(root.join(extra)) {
            corpus.push_str(&without_backticks(&t));
            corpus.push('\n');
        }
        stems.push(extra.to_ascii_lowercase());
    }

    // What cites: comments in `src/`, and the documents.
    let mut citing = Vec::new();
    collect(&root.join("src"), &["rs"], &mut citing);
    collect(&root.join("docs"), &["md"], &mut citing);
    citing.push(root.join("CLAUDE.md"));

    let allowed: Vec<&str> = NOT_DEFINED_HERE.iter().map(|(n, _)| *n).collect();
    let mut dead: BTreeMap<String, String> = BTreeMap::new();

    for p in &citing {
        let Ok(text) = fs::read_to_string(p) else { continue };
        let rel = p.strip_prefix(root).unwrap_or(p).display().to_string();
        for (tok, line) in backticked(&text) {
            if !is_citation(&tok) || allowed.contains(&tok.as_str()) {
                continue;
            }
            let last = tok.rsplit("::").next().unwrap_or(&tok);
            if contains_word(&corpus, last) || stems.contains(&last.to_ascii_lowercase()) {
                continue;
            }
            dead.entry(tok).or_insert(format!("{rel}:{line}"));
        }
    }

    assert!(
        dead.is_empty(),
        "these names are cited in backticks and exist nowhere in the tree. Fix the citation, or -- if nothing here \
         should define it -- add it to NOT_DEFINED_HERE with the reason:\n{}",
        dead.iter().map(|(n, w)| format!("  {n:50} {w}\n")).collect::<String>()
    );
}

/// A conflict left in a file compiles for anything that is not compiled: a markdown table, a TOML comment, a
/// stylesheet. `cargo test` said nothing about one committed in `docs/status.md`, and a reviewer found it.
///
/// The markers are matched at the start of a line, which is where git writes them, so this file may describe them
/// and the check still passes.
#[test]
fn nothing_committed_holds_a_merge_conflict() {
    let root = &repository();
    let mut files = Vec::new();
    for (dir, exts) in [
        ("src", &["rs"][..]),
        ("tests", &["rs"][..]),
        ("tools", &["rs"][..]),
        ("docs", &["md", "py"][..]),
        ("gui-frontend/src", &["ts", "svelte", "css", "html"][..]),
        ("website/src", &["ts", "svelte", "css"][..]),
    ] {
        collect(&root.join(dir), exts, &mut files);
    }
    for name in [
        "README.md",
        "CLAUDE.md",
        "smeltr.example.toml",
        "Cargo.toml",
        "justfile",
    ] {
        let p = root.join(name);
        if p.exists() {
            files.push(p);
        }
    }

    let mut found = Vec::new();
    for path in &files {
        let Ok(text) = fs::read_to_string(path) else { continue };
        for (n, line) in text.lines().enumerate() {
            let marker = line.starts_with("<<<<<<< ")
                || line.starts_with(">>>>>>> ")
                || (line == "=======" && text.contains("\n<<<<<<< "));
            if marker {
                let shown = path.strip_prefix(root).unwrap_or(path).display();
                found.push(format!("  {shown}:{}  {line}", n + 1));
            }
        }
    }

    assert!(
        found.is_empty(),
        "a merge conflict was committed -- resolve it rather than shipping both sides:\n{}",
        found.join("\n")
    );
}
