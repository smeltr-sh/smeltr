//! Router interface counters polled over SNMP by Telegraf's `snmp` input and written by its JSON serializer: one
//! metric per line, `{"fields":{...},"name":"interface","tags":{...},"timestamp":<seconds>}`. The fields are
//! IF-MIB's (`ifTable`, `ifXTable`); the counters grow with the clock, so a run of metrics yields rates. The values
//! are synthetic.

use serde_json::json;

use super::{Clock, mix, pick};

/// Routers: name, management address.
const ROUTERS: &[(&str, &str)] = &[
    ("edge-nyc-01", "10.0.0.1"),
    ("edge-lon-01", "10.1.0.1"),
    ("edge-fra-01", "10.2.0.1"),
    ("core-nyc-01", "10.0.0.2"),
];
/// Interfaces: index, name, description, speed in Mb/s, and mean load in Mb/s.
const INTERFACES: &[(u32, &str, &str, u64, u64)] = &[
    (1, "GigabitEthernet0/0/0", "WAN: MPLS to Hub-DC1", 1_000, 220),
    (2, "GigabitEthernet0/0/1", "WAN: Internet", 1_000, 410),
    (3, "TenGigabitEthernet0/1/0", "LAN: core uplink", 10_000, 1_900),
    (4, "Cellular0/2/0", "WAN: LTE backup", 100, 2),
];

/// Metric `i`, polled at `at`. With `flapping`, the internet uplink of one router is taking errors and discards
/// and now and then reports itself down.
pub fn record(i: u64, at: &Clock, flapping: bool) -> String {
    let a = mix(i ^ 0x07E1_E6AF);
    let (router, address) = pick(ROUTERS, a);
    let r = a % ROUTERS.len() as u64;
    let (index, name, description, speed, load) = pick(INTERFACES, a >> 8);
    let sick = flapping && r == 1 && index == 2;
    let t = at.unix as u64;
    // The counters count from the router's last boot, every forty-five days or so.
    const BOOTS: u64 = 45 * 86_400;
    let uptime = (t + mix(r) % BOOTS) % BOOTS;
    // A counter starts at a value of its own and grows at the interface's mean load.
    let seed = mix(r << 8 | u64::from(index));
    let octets = |share: u64, salt: u64| (mix(seed ^ salt) >> 40) + uptime * load * share / 100 * 125_000;
    let (inbound, outbound) = (octets(100, 1), octets(35, 2));
    let errors = |salt: u64| {
        (mix(seed ^ salt) >> 48)
            + if sick {
                uptime / 2 + (a >> 40) % 50
            } else {
                uptime / 50_000
            }
    };
    let up = !(sick && (a >> 56).is_multiple_of(4));
    json!({
        "fields": {
            "ifIndex": index,
            "ifHCInOctets": inbound,
            "ifHCOutOctets": outbound,
            "ifHCInUcastPkts": inbound / 900,
            "ifHCOutUcastPkts": outbound / 600,
            "ifInErrors": errors(3),
            "ifOutErrors": errors(4) / 4,
            "ifInDiscards": errors(5) / 2,
            "ifOutDiscards": errors(6) / 8,
            "ifOperStatus": if up { 1 } else { 2 },
            "ifAdminStatus": 1,
            "ifHighSpeed": speed,
            "ifMtu": 1_500,
        },
        "name": "interface",
        "tags": {
            "agent_host": address,
            "host": "telegraf-snmp-01",
            "hostname": router,
            "ifName": name,
            "ifDescr": description,
            "ifIndex": index.to_string(),
        },
        "timestamp": t,
    })
    .to_string()
}

#[test]
fn a_counter_grows_with_the_clock_and_flapping_shows_on_one_uplink() {
    let field =
        |line: &str, f: &str| serde_json::from_str::<serde_json::Value>(line).expect("JSON")["fields"][f].clone();
    let (now, later) = (Clock::at(1_790_000_000, 0), Clock::at(1_790_000_060, 0));
    for i in 0..100 {
        let (a, b) = (record(i, &now, false), record(i, &later, false));
        assert!(
            field(&b, "ifHCInOctets").as_u64() > field(&a, "ifHCInOctets").as_u64(),
            "{a}"
        );
    }
    let down = (0..2_000)
        .map(|i| record(i, &now, true))
        .filter(|l| l.contains("\"ifOperStatus\":2"))
        .count();
    assert!(down > 0);
    assert!((0..2_000).all(|i| !record(i, &now, false).contains("\"ifOperStatus\":2")));
}
