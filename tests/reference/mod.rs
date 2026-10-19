//! The reference pipelines: one per common log type, each a generator of
//! realistic records and the bundle that normalizes them to ECS. The
//! benchmark log (`just bench-log`) runs every one, so a regression in any
//! common shape shows up there.
//!
//! A bundle is the steps the GUI saves, loaded and compiled the way a
//! worker loads it, so the benchmark runs what a deployment runs. `driver`
//! runs it between a generator and a counter, with no broker: the workload
//! is the transform.
//!
//! This file and the pipeline modules are the workload; `driver` is the half
//! that reaches into the engine. The benchmark log runs head's workload on
//! each side's own driver, so a change to the engine's internals does not
//! stop the reference from building.

#![allow(dead_code)]

pub mod asa;
pub mod cloudtrail;
pub mod fortigate;
pub mod json_app;
pub mod k8s;
pub mod nginx;

mod driver;

use std::path::PathBuf;

// Each test binary over this module uses a different part of the driver.
#[allow(unused_imports)]
pub use driver::{Counter, Generator, Run, run};

/// One reference pipeline.
pub struct Pipeline {
    /// The name the benchmark log files its row under.
    pub name: &'static str,
    /// Record `i` of the corpus, as the source delivers it.
    pub line: fn(u64) -> String,
    pub bundle: fn() -> String,
    /// Panics unless the output is the mapping's: `rows` records in, `docs`
    /// documents out, the first of them in `kept`.
    pub check: fn(rows: u64, docs: u64, kept: &[serde_json::Value]),
}

/// A bundle with one source, one destination and one pipeline of `steps`
/// over `tables`. Both ends are declared so the compiler sees a real
/// topology; `run` substitutes `Generator` and `Counter` for them, so
/// nothing opens a socket.
pub fn bundle(steps: Vec<serde_json::Value>, tables: Vec<serde_json::Value>) -> String {
    serde_json::json!({
        "version": 1,
        "sources": [{
            "id": "in", "type": "kafka", "brokers": "127.0.0.1:1",
            "topics": ["records"], "groupId": "reference", "encoding": "utf-8",
        }],
        "destinations": [{
            "id": "ecs", "type": "kafka", "brokers": "127.0.0.1:1", "defaultTopic": "ecs",
        }],
        "lookupTables": tables,
        "pipelines": [{ "id": "p", "name": "reference", "steps": steps }],
        "routes": [{
            "id": "r", "source": "in", "condition": null, "pipeline": "p",
            "deliver": [{"to": ["ecs"]}],
        }],
    })
    .to_string()
}

/// Where the lookup files live, as a deployed bundle's directory holds
/// them. One per thread, as each test builds and runs its bundle on one, so
/// a test that runs and removes its own does not take another's.
pub fn bundle_dir() -> PathBuf {
    let thread = std::thread::current().id();
    std::env::temp_dir().join(format!("smeltr-reference-{}-{thread:?}", std::process::id()))
}

/// Writes `contents` to the bundle's `lookups/<name>` and returns the path a
/// table names it by. A lookup file resolves against the bundle that
/// declared the table, so `run` loads every table as declared there.
pub fn lookup_file(name: &str, contents: &[u8]) -> String {
    let path = format!("lookups/{name}");
    let at = bundle_dir().join(&path);
    std::fs::create_dir_all(at.parent().unwrap()).expect("the lookups folder");
    std::fs::write(&at, contents).expect("the lookup file");
    path
}

/// `10^9`, the unit vendors publish daily volume in.
pub const GB: f64 = 1e9;

/// How many distinct records the corpus holds before it repeats, at most.
pub const CORPUS: u64 = 50_000;
