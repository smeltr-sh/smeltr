//! NETSCOUT Arbor Edge Defense (AED) events as CEF over syslog: a BSD syslog envelope around
//! `CEF:0|vendor|product|version|signature|name|severity|extensions`. AED reports the hosts it blocks, the attacks
//! its protection groups mitigate, and the outbound traffic its threat filter catches. The CEF header and the
//! extension keys are ArcSight's; the values are synthetic.

use super::{Clock, mix, people, pick};

const DEVICES: &[&str] = &["aed-nyc-01", "aed-fra-01"];
const VERSION: &str = "6.11.0";
/// What AED reports: the signature id, its name, the CEF severity, and the protection category.
const EVENTS: &[(&str, &str, u8, &str)] = &[
    ("1001", "Blocked Host", 5, "Blocked Hosts"),
    ("1002", "Botnet Prevention", 7, "Botnet Prevention"),
    ("1003", "Invalid Packets", 3, "Invalid Packets"),
    ("1004", "Outbound Threat Filter", 8, "Outbound Threat Filter"),
    ("1005", "TCP SYN Authentication", 4, "TCP SYN Authentication"),
];
/// The protection groups, and the servers in each.
const GROUPS: &[(&str, &str)] = &[
    ("Web Servers", "172.16.1"),
    ("DNS Servers", "172.16.2"),
    ("VPN Gateways", "172.16.3"),
];

/// Event `i`, logged at `at`. With `attacked`, a UDP reflection flood hits the web servers and AED drops it.
pub fn record(i: u64, at: &Clock, attacked: bool) -> String {
    let a = mix(i ^ 0xAED0_0001);
    let b = mix(i ^ 0xAED0_0002);
    let device = pick(DEVICES, a);
    let flood = attacked && (a >> 50).is_multiple_of(2);
    let (group, net) = if flood { GROUPS[0] } else { pick(GROUPS, a >> 4) };
    let server = format!("{net}.{}", 10 + (a >> 8) % 20);
    let (outside, _) = pick(people::PUBLIC, b);
    let rt = at.unix * 1_000 + i64::from(at.nanos / 1_000_000);
    let head = |(sig, name, severity, _): (&str, &str, u8, &str)| {
        format!(
            "<134>{} {device} CEF:0|NETSCOUT|Arbor Edge Defense|{VERSION}|{sig}|{name}|{severity}|",
            at.syslog()
        )
    };
    if flood {
        let bytes = 1_000_000 + (b >> 8) % 9_000_000;
        return format!(
            "{}rt={rt} src={outside} dst={server} spt={sport} dpt=443 proto=UDP act=Drop cnt={packets} in={bytes} \
             cs1Label=Protection Group cs1={group} cs2Label=Attack Category cs2=UDP Reflection Amplification \
             cs3Label=Protection Level cs3=High",
            head(("2001", "DDoS Attack Mitigated", 9, "")),
            sport = pick(&[53, 123, 389, 1900, 11211][..], b >> 40),
            packets = bytes / 1_200,
        );
    }
    let event = pick(EVENTS, a >> 12);
    let outbound = event.0 == "1004";
    let inside = format!(
        "10.{}.{}.{}",
        (b >> 20) % people::NETWORKS,
        (b >> 28) % 41,
        2 + (b >> 36) % 248
    );
    let (src, dst) = if outbound {
        (inside, outside.to_string())
    } else {
        (outside.to_string(), server)
    };
    format!(
        "{}rt={rt} src={src} dst={dst} spt={sport} dpt={dpt} proto={proto} act={act} cnt={cnt} \
         cs1Label=Protection Group cs1={group} cs2Label=Protection Category cs2={category} cs3Label=Protection Level \
         cs3={level}",
        head(event),
        sport = 1_024 + (b >> 44) % 60_000,
        dpt = if outbound {
            443
        } else {
            pick(&[443, 53, 500][..], a >> 20)
        },
        proto = if event.0 == "1003" { "UDP" } else { "TCP" },
        act = if event.0 == "1005" { "Authenticated" } else { "Blocked" },
        cnt = 1 + (a >> 30) % 400,
        group = if outbound { "Outbound" } else { group },
        category = event.3,
        level = pick(&["Low", "Medium", "High"][..], a >> 40),
    )
}

#[test]
fn every_event_is_cef_and_an_attack_is_mitigated_on_the_web_servers() {
    let at = Clock::at(1_790_000_000, 0);
    for i in 0..300 {
        let line = record(i, &at, false);
        let cef = line.split_once(" CEF:").expect("a CEF body").1;
        assert_eq!(cef.split('|').count(), 8, "{line}");
        assert!(cef.contains("rt=1790000000000 "), "{line}");
    }
    let attacks = (0..300)
        .map(|i| record(i, &at, true))
        .filter(|l| l.contains("cs2=UDP Reflection"))
        .count();
    assert!(attacks > 100, "{attacks}");
}
