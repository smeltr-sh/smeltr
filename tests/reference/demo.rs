//! The website demo's logs: which generator writes each Kafka topic, at what rate, and in what trouble. The demo's
//! fleet is staged from these (`examples/demo-traffic.rs`, `tools/screenshots`), and its recording
//! (`gui-frontend/src/demo/recorded.json`) holds what they made then. [`fingerprint`] says whether they still make
//! the same.

use serde_json::{Value, json};

use super::{
    Clock, asa, auth, cloudtrail, elastic_agent, fortigate, mix, netscout, nginx, people, sentinel, telegraf, versa,
};
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
    Stream {
        topic: "syslog.panos.threat",
        rate: 25.0,
        period: 400.0,
        phase: 0.4,
        record: panos::threat::record,
        // Forty seconds of a vulnerability scan against the DMZ every eight minutes.
        trouble: |t| t % 480 < 40,
    },
    Stream {
        topic: "syslog.versa",
        rate: 150.0,
        period: 500.0,
        phase: 2.4,
        record: versa::record,
        // A minute and a half of a lossy internet path every twelve minutes.
        trouble: |t| t % 720 < 90,
    },
    Stream {
        topic: "elastic.agent.linux",
        rate: 60.0,
        period: 450.0,
        phase: 1.1,
        record: elastic_agent::linux,
        // Password guessing at the bastions, with the auth.log stream's.
        trouble: |t| t % 300 < 20,
    },
    Stream {
        topic: "elastic.agent.windows",
        rate: 80.0,
        period: 380.0,
        phase: 3.0,
        record: elastic_agent::windows,
        // Thirty seconds of password spraying every seven minutes.
        trouble: |t| t % 420 < 30,
    },
    Stream {
        // A message carries about two and a half rows.
        topic: "azure.eventhub.sentinel",
        rate: 15.0,
        period: 520.0,
        phase: 0.2,
        record: sentinel::record,
        // The same spraying, as Entra ID sees it.
        trouble: |t| t % 420 < 30,
    },
    Stream {
        topic: "syslog.netscout",
        rate: 40.0,
        period: 340.0,
        phase: 1.6,
        record: netscout::record,
        // A minute of a reflection flood every fifteen minutes.
        trouble: |t| t % 900 < 60,
    },
    Stream {
        topic: "metrics.snmp.interface",
        rate: 10.0,
        period: 600.0,
        phase: 2.6,
        record: telegraf::record,
        // Two minutes of a flapping uplink every ten minutes.
        trouble: |t| t % 600 < 120,
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
