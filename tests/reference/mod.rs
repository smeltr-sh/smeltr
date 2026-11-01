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
//! The same generators write the website demo's logs (`demo`), so what the
//! benchmark measures is what the demo shows. `people` is the company both
//! are about; `auth` is a generator the benchmark has no pipeline for.
//!
//! This file and the pipeline modules are the workload; `driver` is the half
//! that reaches into the engine. The benchmark log runs head's workload on
//! each side's own driver, so a change to the engine's internals does not
//! stop the reference from building.

#![allow(dead_code)]

pub mod asa;
pub mod auth;
pub mod cloudtrail;
pub mod demo;
pub mod fortigate;
pub mod json_app;
pub mod k8s;
pub mod nginx;
pub mod people;

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

/// splitmix64. A record is a pure function of its index, so a run is
/// reproducible and two runs of different lengths agree row for row.
pub fn mix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub fn pick<T: Copy>(pool: &[T], r: u64) -> T {
    pool[(r % pool.len() as u64) as usize]
}

pub const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// A UTC instant, broken into the fields a log line writes. The benchmark
/// stamps record `i` with [`Clock::of`], so a record stays a pure function
/// of its index; the demo stamps each with the time it is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clock {
    /// Seconds since the Unix epoch.
    pub unix: i64,
    pub nanos: u32,
    pub year: i64,
    /// 1 to 12.
    pub month: u32,
    /// 1 to 31.
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
}

impl Clock {
    pub fn at(unix: i64, nanos: u32) -> Clock {
        // Howard Hinnant's `civil_from_days`.
        let (days, secs) = (unix.div_euclid(86_400), unix.rem_euclid(86_400));
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
        let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
        Clock {
            unix,
            nanos,
            year: yoe + era * 400 + i64::from(month <= 2),
            month,
            day,
            hour: (secs / 3_600) as u32,
            minute: (secs / 60 % 60) as u32,
            second: (secs % 60) as u32,
        }
    }

    /// The benchmark's instant for record `i`: anywhere in 2026, to the
    /// nanosecond.
    pub fn of(i: u64) -> Clock {
        const START_2026: i64 = 1_767_225_600;
        let r = mix(i ^ 0xC10C_C10C);
        Clock::at(
            START_2026 + (r % (365 * 86_400)) as i64,
            ((r >> 32) % 1_000_000_000) as u32,
        )
    }

    pub fn mon(&self) -> &'static str {
        MONTHS[self.month as usize - 1]
    }

    /// `HH:MM:SS`.
    pub fn hms(&self) -> String {
        format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
    }

    /// `Oct  5 14:20:05`, BSD syslog's: no year, the day padded with a space.
    pub fn syslog(&self) -> String {
        format!("{} {:>2} {}", self.mon(), self.day, self.hms())
    }

    /// `2026-10-05T14:20:05Z`.
    pub fn iso(&self) -> String {
        format!("{}-{:02}-{:02}T{}Z", self.year, self.month, self.day, self.hms())
    }

    /// `2026-10-05T14:20:05.123Z`.
    pub fn iso_millis(&self) -> String {
        format!(
            "{}-{:02}-{:02}T{}.{:03}Z",
            self.year,
            self.month,
            self.day,
            self.hms(),
            self.nanos / 1_000_000
        )
    }

    /// The instant `secs` later.
    pub fn plus(&self, secs: i64) -> Clock {
        Clock::at(self.unix + secs, self.nanos)
    }
}

#[test]
fn a_clock_breaks_an_instant_into_its_civil_fields() {
    let c = Clock::at(1_790_000_000, 5);
    assert_eq!(c.iso(), "2026-09-21T14:13:20Z");
    assert_eq!(c.syslog(), "Sep 21 14:13:20");
    assert_eq!(Clock::at(951_782_400, 0).iso(), "2000-02-29T00:00:00Z");
    assert_eq!(Clock::at(0, 0).iso(), "1970-01-01T00:00:00Z");
}
