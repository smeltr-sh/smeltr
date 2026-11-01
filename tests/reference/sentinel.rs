//! Microsoft Sentinel's tables as a Log Analytics data export sends them to an Event Hub: each message is a JSON
//! object whose `records` array holds rows of the exported tables, each row stamped with its `Type`, `TenantId`
//! and `TimeGenerated`. The tables are SecurityEvent, SigninLogs, CommonSecurityLog and Syslog, with the column
//! names of the Azure-Sentinel repository's sample data; the values are synthetic.

use serde_json::{Value, json};

use super::{Clock, mix, people, pick};

const TENANT: &str = "6c3a8f2e-1b4d-4e5f-9a7b-2d8c0e1f3a5b";
const WINDOWS: &[&str] = &[
    "DC01.acme.example",
    "DC02.acme.example",
    "FS01.acme.example",
    "WS-0417.acme.example",
];
const LINUX: &[&str] = &["sentinel-fwd-01", "sentinel-fwd-02"];
const APPS: &[(&str, &str)] = &[
    ("Office 365 Exchange Online", "00000002-0000-0ff1-ce00-000000000000"),
    ("Microsoft Teams", "1fec8e78-bce4-4aaf-ab1b-5451cc387264"),
    ("Azure Portal", "c44b4083-3bb0-49c1-b47d-974e53cbdf3c"),
    ("Microsoft Office", "d3590ed6-52b3-4102-aeff-aad2292ab01c"),
];
/// A failed sign-in: its result type and description.
const FAILURES: &[(&str, &str)] = &[
    (
        "50126",
        "Invalid username or password or Invalid on-premise username or password.",
    ),
    (
        "50053",
        "Account is locked because user tried to sign in too many times with an incorrect user ID or password.",
    ),
    ("50074", "Strong Authentication is required."),
];

/// Message `i`, exported at `at`: a batch of one to four rows. With `spraying`, one address is trying a
/// password against many accounts, and its failed sign-ins fill the batch.
pub fn record(i: u64, at: &Clock, spraying: bool) -> String {
    let a = mix(i ^ 0x05E4_71E1);
    let rows: Vec<Value> = (0..1 + a % 4)
        .map(|k| {
            let r = mix(a ^ k);
            let at = at.plus(-((r >> 56) as i64 % 30));
            match (r >> 60) % 4 {
                _ if spraying && (r >> 52).is_multiple_of(2) => signin(r, &at, true),
                0 => security_event(r, &at),
                1 => signin(r, &at, false),
                2 => common_security_log(r, &at),
                _ => syslog(r, &at),
            }
        })
        .collect();
    json!({ "records": rows }).to_string()
}

/// The columns every exported row carries.
fn row(table: &str, at: &Clock, columns: Value) -> Value {
    let mut row = json!({"TimeGenerated": at.iso_millis(), "TenantId": TENANT, "Type": table});
    row.as_object_mut()
        .expect("an object")
        .extend(columns.as_object().expect("an object").clone());
    row
}

fn address(r: u64) -> String {
    format!("10.{}.{}.{}", r % people::NETWORKS, (r >> 8) % 41, 2 + (r >> 16) % 248)
}

fn guid(r: u64) -> String {
    let g = mix(r);
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        r >> 32,
        r & 0xffff,
        (g >> 4) & 0xfff,
        0x8000 | ((g >> 20) & 0x3fff),
        g >> 16
    )
}

fn security_event(r: u64, at: &Clock) -> Value {
    let p = people::person(r >> 20);
    let (id, activity, logon) = pick(
        &[
            (4624, "4624 - An account was successfully logged on.", Some(3)),
            (4625, "4625 - An account failed to log on.", Some(3)),
            (4634, "4634 - An account was logged off.", Some(3)),
            (4672, "4672 - Special privileges assigned to new logon.", None),
            (4688, "4688 - A new process has been created.", None),
        ][..],
        r >> 8,
    );
    let mut columns = json!({
        "Computer": pick(WINDOWS, r >> 4), "EventID": id, "Activity": activity, "Channel": "Security",
        "EventSourceName": "Microsoft-Windows-Security-Auditing", "Level": "8", "Task": 12544,
        "SourceSystem": "OpsManager", "Account": format!("ACME\\{}", p.login), "AccountType": "User",
        "TargetUserName": p.login, "TargetDomainName": "ACME", "SubjectUserName": "-",
    });
    if let Some(logon) = logon {
        columns["LogonType"] = json!(logon);
        columns["LogonProcessName"] = json!("NtLmSsp");
        columns["AuthenticationPackageName"] = json!("NTLM");
        columns["IpAddress"] = json!(address(r >> 24));
        columns["IpPort"] = json!(((49_152 + (r >> 40) % 16_000) as u16).to_string());
        columns["WorkstationName"] = json!(format!("WS-{:04}", (r >> 30) % 2_000));
    }
    if id == 4625 {
        columns["Status"] = json!("0xc000006d");
        columns["SubStatus"] = json!("0xc000006a");
    }
    if id == 4688 {
        columns["Process"] = json!(pick(
            &["powershell.exe", "cmd.exe", "svchost.exe", "msiexec.exe"][..],
            r >> 34
        ));
    }
    row("SecurityEvent", at, columns)
}

fn signin(r: u64, at: &Clock, spraying: bool) -> Value {
    let p = people::person(r >> 20);
    let (app, app_id) = pick(APPS, r >> 8);
    let (ip, country) = if spraying {
        people::PUBLIC[11]
    } else {
        pick(people::PUBLIC, r >> 12)
    };
    let failed = spraying || (r >> 36).is_multiple_of(9);
    let (result, description) = if spraying {
        FAILURES[0]
    } else if failed {
        pick(FAILURES, r >> 40)
    } else {
        ("0", "")
    };
    row(
        "SigninLogs",
        at,
        json!({
            "OperationName": "Sign-in activity", "Category": "SignInLogs",
            "ResultType": result, "ResultDescription": description,
            "UserPrincipalName": format!("{}@acme.example", p.login), "UserDisplayName": p.full_name,
            "UserId": guid(r >> 20), "AppDisplayName": app, "AppId": app_id, "IPAddress": ip,
            "ClientAppUsed": if spraying { "Exchange ActiveSync" } else { "Browser" },
            "ConditionalAccessStatus": if failed { "notApplied" } else { "success" },
            "Location": country,
            "LocationDetails": {"countryOrRegion": country},
            "DeviceDetail": {"operatingSystem": pick(&["Windows 10", "MacOs", "Ios", "Android"][..], r >> 44),
                             "browser": pick(&["Edge 120.0.0", "Chrome 121.0.0", "Safari 17.2"][..], r >> 46)},
            "Status": {"errorCode": result.parse::<u32>().expect("a number"), "failureReason": description},
            "RiskLevelDuringSignIn": if spraying { "medium" } else { "none" },
            "CorrelationId": guid(r ^ 0xC0), "Id": guid(r ^ 0x1D),
        }),
    )
}

fn common_security_log(r: u64, at: &Clock) -> Value {
    let (dst, _) = pick(people::PUBLIC, r >> 12);
    let deny = (r >> 30).is_multiple_of(5);
    row(
        "CommonSecurityLog",
        at,
        json!({
            "DeviceVendor": "Check Point", "DeviceProduct": "VPN-1 & FireWall-1", "DeviceVersion": "1.0",
            "DeviceEventClassID": "Log", "Activity": if deny { "Drop" } else { "Accept" },
            "LogSeverity": if deny { "5" } else { "1" }, "DeviceAction": if deny { "Drop" } else { "Accept" },
            "SourceIP": address(r >> 4), "DestinationIP": dst,
            "SourcePort": 32_768 + (r >> 40) % 28_000, "DestinationPort": pick(&[443, 80, 53, 22][..], r >> 34),
            "Protocol": "TCP", "Computer": pick(LINUX, r >> 2),
        }),
    )
}

fn syslog(r: u64, at: &Clock) -> Value {
    let host = pick(LINUX, r >> 2);
    let (facility, severity, process, message) = pick(
        &[
            (
                "authpriv",
                "info",
                "sshd",
                "Accepted publickey for azureuser from 10.0.0.4 port 51522 ssh2",
            ),
            ("daemon", "notice", "systemd", "Started Daily apt download activities."),
            (
                "kern",
                "warning",
                "kernel",
                "TCP: request_sock_TCP: Possible SYN flooding on port 514.",
            ),
            ("syslog", "info", "rsyslogd", "action 'action-8-builtin:omfwd' resumed"),
        ][..],
        r >> 8,
    );
    row(
        "Syslog",
        at,
        json!({
            "Computer": host, "HostName": host, "HostIP": address(r >> 16), "Facility": facility,
            "SeverityLevel": severity, "ProcessName": process, "ProcessID": 300 + (r >> 40) % 30_000,
            "SyslogMessage": message, "SourceSystem": "Linux",
        }),
    )
}

#[test]
fn every_message_is_a_batch_of_typed_rows_and_spraying_fails_sign_ins() {
    let at = Clock::at(1_790_000_000, 0);
    let mut tables = std::collections::BTreeSet::new();
    let mut failures = 0;
    for i in 0..300 {
        let message: Value = serde_json::from_str(&record(i, &at, false)).expect("JSON");
        for row in message["records"].as_array().expect("records") {
            assert_eq!(row["TenantId"], TENANT);
            tables.insert(row["Type"].as_str().expect("a Type").to_string());
        }
        let message: Value = serde_json::from_str(&record(i, &at, true)).expect("JSON");
        failures += message["records"]
            .as_array()
            .expect("records")
            .iter()
            .filter(|r| r["ResultType"] == "50126")
            .count();
    }
    assert_eq!(tables.len(), 4, "{tables:?}");
    assert!(failures > 200, "{failures}");
}
