//! PAN-OS THREAT records over syslog: what the firewalls' security profiles caught in the sessions of
//! [`super::record`]. The columns follow PAN-OS's THREAT log format; the signature names and ids are synthetic.

use super::{app, people};
use crate::reference::{Clock, mix, pick};

/// A threat: its content type, name, id, category and severity, and what the profile does with it.
const THREATS: &[(&str, &str, u32, &str, &str, &str)] = &[
    (
        "vulnerability",
        "HTTP Directory Traversal Attempt",
        94_001,
        "code-execution",
        "high",
        "reset-both",
    ),
    (
        "vulnerability",
        "SQL Injection Probe in URI",
        94_017,
        "sql-injection",
        "medium",
        "alert",
    ),
    (
        "vulnerability",
        "Remote Code Execution in Java Deserialization",
        94_102,
        "code-execution",
        "critical",
        "reset-both",
    ),
    (
        "spyware",
        "Generic Command and Control Beacon",
        86_211,
        "spyware",
        "critical",
        "drop",
    ),
    (
        "spyware",
        "Suspicious DNS Query for Known Bad Domain",
        86_340,
        "dns-c2",
        "high",
        "sinkhole",
    ),
    ("virus", "Trojan Dropper Generic", 52_004, "virus", "high", "reset-both"),
    ("file", "Windows Executable (EXE)", 52_020, "file", "low", "alert"),
    (
        "url",
        "Malicious URL Category Match",
        9_999,
        "malware",
        "informational",
        "block-url",
    ),
];

/// Threat `i`, logged at `at`. With `scanning`, one outside address is probing the DMZ for vulnerabilities.
pub fn record(i: u64, at: &Clock, scanning: bool) -> String {
    let a = mix(i ^ 0x0741_7EA7);
    let b = mix(i ^ 0x0741_7EA8);
    let net = a % people::NETWORKS;
    let (site, _) = people::site_of(net);
    let device = format!("pa-{site}-0{}", 1 + (a >> 16) % 2);
    let scan = scanning && (b >> 50).is_multiple_of(2);
    let (kind, name, id, category, severity, action) = if scan {
        THREATS[(b % 3) as usize]
    } else {
        pick(THREATS, b)
    };
    let inbound = scan || kind == "vulnerability";
    let (outside, country) = if scan {
        people::PUBLIC[6]
    } else {
        people::PUBLIC[(a >> 8) as usize % people::PUBLIC.len()]
    };
    let inside = if inbound {
        format!("172.16.{}.{}", 1 + (b >> 8) % 20, 2 + (b >> 16) % 248)
    } else {
        format!("10.{net}.{}.{}", (b >> 24) % 41, 2 + (b >> 32) % 248)
    };
    let (src, dst) = if inbound {
        (outside.to_string(), inside.clone())
    } else {
        (inside.clone(), outside.to_string())
    };
    let (app, port, proto, ..) = match (kind, category) {
        ("spyware", "dns-c2") => super::APPS[2],
        ("url" | "file" | "virus", _) => super::APPS[1],
        _ => app(a >> 20),
    };
    let port = if inbound && port != 53 { 443 } else { port };
    let user = if inbound || (a >> 24).is_multiple_of(4) {
        String::new()
    } else {
        format!("acme\\{}", people::person(a >> 28).login)
    };
    let target = match kind {
        "url" => format!("\"malware-test.example/{}\"", (b >> 40) % 1000),
        "file" | "virus" => format!("\"invoice_{}.exe\"", (b >> 40) % 10_000),
        _ => String::new(),
    };
    let panos = |t: &Clock| format!("{}/{:02}/{:02} {}", t.year, t.month, t.day, t.hms());
    let now = panos(at);
    format!(
        "<14>{syslog} {device}.acme.example 1,{now},0132010{serial:05},THREAT,{kind},2562,{now},{src},{dst},\
         {nat},{dst},{rule},{user},,{app},vsys1,{szone},{dzone},ethernet1/{iif},ethernet1/{oif},log-fwd-siem,,\
         {session},1,{sport},{port},0,0,0x80402000,{proto},{action},{target},{name}({id}),{category},{severity},\
         {direction},{sequence},0x8000000000000000,{scountry},{dcountry},,,0,,,,,,,,,,,,,0,0,0,0,vsys1,{device},,",
        syslog = at.syslog(),
        serial = net * 2 + (a >> 16) % 2,
        nat = if inbound {
            String::new()
        } else {
            format!("198.51.100.{}", 2 + (b >> 24) % 29)
        },
        rule = if inbound {
            "dmz-inbound-web"
        } else {
            "allow-outbound-web"
        },
        szone = if inbound { "untrust" } else { "trust" },
        dzone = if inbound { "dmz" } else { "untrust" },
        iif = if inbound { 1 } else { 2 },
        oif = if inbound { 3 } else { 1 },
        session = 100_000 + (a >> 32) % 900_000,
        sport = 32_768 + (b >> 44) % 28_000,
        direction = if inbound {
            "client-to-server"
        } else {
            "server-to-client"
        },
        sequence = 9_000_000 + (b >> 20) % 1_000_000,
        scountry = if inbound { country } else { "10.0.0.0-10.255.255.255" },
        dcountry = if inbound { "172.16.0.0-172.31.255.255" } else { country },
    )
}

#[test]
fn a_threat_names_its_signature_and_scanning_comes_from_one_address() {
    let at = Clock::at(1_790_000_000, 0);
    for i in 0..300 {
        let line = record(i, &at, false);
        let cols: Vec<&str> = line.split(',').collect();
        assert_eq!(cols[3], "THREAT", "{line}");
        assert!(cols.iter().any(|c| c.ends_with(')') && c.contains('(')), "{line}");
    }
    let scans = (0..300)
        .map(|i| record(i, &at, true))
        .filter(|l| l.contains(",175.16.199.1,"))
        .count();
    assert!(scans > 100, "{scans}");
}
