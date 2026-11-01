//! The website demo's logs: which generator writes each Kafka topic, at what rate, and in what trouble. The demo's
//! fleet is staged from these (`examples/demo-traffic.rs`, `tools/screenshots`), and its recording
//! (`gui-frontend/src/demo/recorded.json`) holds what they made then. [`fingerprint`] says whether they still make
//! the same.

use serde_json::{Value, json};

use super::{Clock, asa, auth, cloudtrail, fortigate, mix, nginx, people};
use crate::panos;

/// One topic's traffic.
pub struct Stream {
    pub topic: &'static str,
    /// Records a second, before the wave.
    pub rate: f64,
    /// The wave's period in seconds and its phase, so each stream's chart moves on its own.
    pub period: f64,
    pub phase: f64,
    /// Record `i` at an instant, in trouble or not.
    pub record: fn(u64, &Clock, bool) -> String,
    /// Whether the stream is in trouble `t` seconds into the traffic.
    pub trouble: fn(u64) -> bool,
}

pub const STREAMS: &[Stream] = &[
    Stream {
        topic: "syslog.panos",
        rate: 900.0,
        period: 420.0,
        phase: 0.0,
        record: |i, at, _| panos::record(i, at),
        trouble: |_| false,
    },
    Stream {
        topic: "syslog.fortigate",
        rate: 420.0,
        period: 600.0,
        phase: 1.3,
        record: fortigate::record,
        // FGT-SIN's renamed `proto` is the fleet's standing type warning.
        trouble: |_| true,
    },
    Stream {
        topic: "web.nginx.access",
        rate: 260.0,
        period: 300.0,
        phase: 2.1,
        record: nginx::record,
        // A spell of upstream errors every ten minutes.
        trouble: |t| t % 600 < 45,
    },
    Stream {
        topic: "syslog.auth",
        rate: 18.0,
        period: 540.0,
        phase: 0.7,
        record: auth::record,
        // Twenty seconds of password guessing every five minutes.
        trouble: |t| t % 300 < 20,
    },
    Stream {
        // A delivery carries about five events.
        topic: "aws.cloudtrail",
        rate: 7.0,
        period: 480.0,
        phase: 2.8,
        record: |i, at, _| cloudtrail::record(i, at),
        trouble: |_| false,
    },
    Stream {
        topic: "syslog.asa",
        rate: 120.0,
        period: 360.0,
        phase: 1.9,
        record: |i, at, _| asa::record(i, at),
        trouble: |_| false,
    },
];

/// The instant the samples are taken at.
const SAMPLE_AT: i64 = 1_790_518_805;

/// `n` lines a few seconds apart, from `SAMPLE_AT` on, from a stream out of trouble. Each `seed` is a different
/// run of lines.
pub fn sample(s: &Stream, n: u64, seed: u64) -> Vec<String> {
    let mut at = Clock::at(SAMPLE_AT, 0);
    (0..n)
        .map(|k| {
            let r = mix(seed ^ k.wrapping_mul(0x9E37));
            at = Clock::at(at.unix + (r % 4) as i64, ((r >> 8) % 1_000) as u32 * 1_000_000);
            (s.record)(seed.wrapping_mul(1_000_003).wrapping_add(k), &at, false)
        })
        .collect()
}

/// The lookup tables the demo's pipelines join on.
pub fn tables() -> Value {
    let table = |(headers, rows): (Vec<&str>, Vec<Vec<String>>)| json!({"headers": headers, "rows": rows});
    json!({
        "sites": table(panos::sites()),
        "directory": table(people::directory()),
        "apps": table(panos::apps()),
    })
}

/// A digest of what the generators make: the tables, and a run of every stream in and out of trouble. Any change
/// to what a generator writes changes it.
pub fn fingerprint() -> String {
    let at = Clock::at(SAMPLE_AT, 0);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |s: &str| {
        for b in s.bytes().chain(*b"\n") {
            h = (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3);
        }
    };
    eat(&tables().to_string());
    for s in STREAMS {
        eat(s.topic);
        for i in 0..500 {
            eat(&(s.record)(i, &at.plus(i as i64), false));
            eat(&(s.record)(i, &at.plus(i as i64), true));
        }
    }
    format!("{h:016x}")
}
