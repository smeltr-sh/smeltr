//! Versa SD-WAN branch appliances exporting their logs as syslog (Versa's log export function, LEF): each line is
//! an ISO timestamp, the log type, and comma-separated `key=value` pairs. Four types share a flow's identity
//! (`tenantId`, `applianceId`, `flowId`, `flowCookie`, `vsnId`), so they join: the flow's start (`flowIdLog`), the
//! firewall's verdict (`accessLog`), an intrusion signature (`idpLog`), and the SD-WAN path's measured quality
//! (`sdwanB2BSlamLog`).
//!
//! The key names follow Versa's published log types; the values are synthetic.

use super::{Clock, mix, people, pick};

/// Branch appliances: name, the site's network.
const APPLIANCES: &[(&str, u64)] = &[
    ("Branch-NYC-01", 0),
    ("Branch-LON-01", 1),
    ("Branch-FRA-01", 2),
    ("Branch-SIN-01", 3),
    ("Branch-SFO-01", 4),
    ("Branch-DUB-01", 5),
];
const TENANT: (&str, u64) = ("Acme", 2);
const APPS: &[(&str, &str, u16, u8)] = &[
    ("office365", "business", 443, 6),
    ("microsoft-teams", "collaboration", 443, 17),
    ("zoom", "collaboration", 8801, 17),
    ("salesforce", "business", 443, 6),
    ("youtube", "streaming", 443, 6),
    ("dns", "networking", 53, 17),
    ("ssh", "remote-access", 22, 6),
    ("bittorrent", "file-sharing", 6881, 6),
];
const RULES: &[&str] = &["Allow-Business", "Allow-Collab", "Allow-Web", "Deny-P2P"];
const SIGNATURES: &[(&str, u32, &str)] = &[
    ("HTTP Apache Struts2 Remote Code Execution", 1_002_331, "critical"),
    ("SMB EternalBlue Exploit Attempt", 1_003_118, "critical"),
    ("DNS Tunneling Suspicious Long Query", 1_004_210, "medium"),
    ("SSH Brute Force Login Attempt", 1_001_874, "high"),
];
/// The WAN paths a branch measures to the hub.
const PATHS: &[(&str, &str)] = &[("MPLS", "Hub-DC1"), ("Internet", "Hub-DC1"), ("LTE", "Hub-DC2")];

/// Line `i`, logged at `at`. With `degraded`, the internet path is losing packets and adding latency, and its SLA
/// logs say so.
pub fn record(i: u64, at: &Clock, degraded: bool) -> String {
    let a = mix(i);
    let b = mix(i ^ 0x7E25_A000);
    let (appliance, net) = APPLIANCES[(a % APPLIANCES.len() as u64) as usize];
    let appliance_id = 100 + (a % APPLIANCES.len() as u64);
    let flow = 30_000_000 + (b >> 8) % 60_000_000;
    let cookie = 1_790_000_000 + (b >> 40) % 10_000_000;
    let ids = format!(
        "applianceName={appliance}, tenantName={}, flowId={flow}, flowCookie={cookie}, vsnId=0, applianceId={appliance_id}, tenantId={}",
        TENANT.0, TENANT.1
    );
    let (app, family, port, proto) = APPS[((a >> 16) % APPS.len() as u64) as usize];
    let src = format!(
        "10.{}.{}.{}",
        net + 8 * ((a >> 24) % 8),
        (a >> 32) % 41,
        2 + (a >> 40) % 248
    );
    let (dst, _) = people::PUBLIC[((b >> 4) as usize) % people::PUBLIC.len()];
    let sport = 32_768 + (b >> 16) % 28_000;
    let flow_keys = format!(
        "sourceIPv4Address={src}, destinationIPv4Address={dst}, sourcePort={sport}, destinationPort={port}, protocolIdentifier={proto}"
    );
    let stamp = format!("{}-{:02}-{:02}T{}+0000", at.year, at.month, at.day, at.hms());
    match (a >> 56) % 16 {
        _ if degraded && (a >> 52).is_multiple_of(4) => sla(&stamp, &ids, appliance, b, true),
        0..=5 => format!(
            "{stamp} flowIdLog, {ids}, {flow_keys}, fromZone=trust, toZone=untrust, fromCountry=, toCountry={country}, \
             ingressInterfaceName=vni-0/2.0, egressInterfaceName=vni-0/0.0, appId={app}, appFamily={family}, \
             user={user}",
            country = people::PUBLIC[((b >> 4) as usize) % people::PUBLIC.len()].1,
            user = user(a),
        ),
        6..=11 => {
            let deny = app == "bittorrent";
            format!(
                "{stamp} accessLog, {ids}, {flow_keys}, fromZone=trust, toZone=untrust, rule={rule}, action={action}, \
                 appId={app}, sentOctets={sent}, recvdOctets={recvd}, sentPackets={sp}, recvdPackets={rp}, \
                 duration={duration}, user={user}",
                rule = if deny { RULES[3] } else { pick(&RULES[..3], b >> 20) },
                action = if deny { "deny" } else { "allow" },
                sent = if deny { 0 } else { 200 + (b >> 24) % 80_000 },
                recvd = if deny { 0 } else { 300 + (a >> 28) % 900_000 },
                sp = if deny { 1 } else { 2 + (b >> 30) % 300 },
                rp = if deny { 0 } else { 2 + (a >> 34) % 900 },
                duration = if deny { 0 } else { (b >> 36) % 600 },
                user = user(a),
            )
        }
        12..=13 => {
            let (sig, id, severity) = SIGNATURES[((b >> 12) % SIGNATURES.len() as u64) as usize];
            format!(
                "{stamp} idpLog, {ids}, {flow_keys}, fromZone=untrust, toZone=trust, signatureId={id}, \
                 signatureMsg=\"{sig}\", threatSeverity={severity}, idpAction={action}, appId={app}",
                action = if severity == "critical" {
                    "drop-session"
                } else {
                    "alert"
                },
            )
        }
        _ => sla(&stamp, &ids, appliance, b, false),
    }
}

/// One SLA measurement of a WAN path to the hub.
fn sla(stamp: &str, ids: &str, appliance: &str, r: u64, degraded: bool) -> String {
    let (path, hub) = PATHS[(r % PATHS.len() as u64) as usize];
    let bad = degraded || path == "LTE";
    let (delay, jitter, loss) = if bad {
        (60 + (r >> 8) % 140, 8 + (r >> 16) % 30, (r >> 24) % 600)
    } else {
        (8 + (r >> 8) % 30, (r >> 16) % 4, (r >> 24) % 20)
    };
    // Loss in hundredths of a percent, the way back a little less than the way out.
    let rev = loss * 7 / 10;
    format!(
        "{stamp} sdwanB2BSlamLog, {ids}, localSiteName={appliance}, remoteSiteName={hub}, localAccCktName={path}, \
         remoteAccCktName={path}, fwdClass=fc_ef, delay={delay}, fwdDelayVar={jitter}, revDelayVar={jitter}, \
         fwdLossRatio={}.{:02}, revLossRatio={}.{:02}",
        loss / 100,
        loss % 100,
        rev / 100,
        rev % 100,
    )
}

/// The user a flow is attributed to, or none for a device nobody signed in on.
fn user(r: u64) -> String {
    if (r >> 44).is_multiple_of(3) {
        String::new()
    } else {
        format!("{}@acme.example", people::person(r >> 46).login)
    }
}

#[test]
fn every_line_is_a_timestamp_a_type_and_pairs_that_name_the_flow() {
    let at = Clock::at(1_790_000_000, 0);
    let mut types = std::collections::BTreeSet::new();
    for i in 0..400 {
        let line = record(i, &at, false);
        let (head, pairs) = line.split_once(", ").expect("a type, then pairs");
        let (stamp, kind) = head.split_once(' ').expect("a timestamp, then the type");
        assert_eq!(stamp, "2026-09-21T14:13:20+0000");
        types.insert(kind.to_string());
        assert!(pairs.contains("flowId=") && pairs.contains("tenantId=2"), "{line}");
    }
    assert_eq!(types.len(), 4, "{types:?}");
}
