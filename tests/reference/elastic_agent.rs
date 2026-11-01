//! Elastic Agent's events as its Kafka output sends them: one JSON document per event, before any Elasticsearch
//! ingest pipeline has run. A Linux host's log lines arrive whole in `message`; a Windows host's events arrive as
//! the Windows event log input reads them, structured under `winlog`. The field names are ECS's and the Windows
//! event IDs Microsoft's; the values are synthetic.

use serde_json::{Value, json};

use super::{Clock, auth, mix, people, pick};

const VERSION: &str = "8.15.0";

/// A host's agent: its name, its operating system, and the two ids Elastic Agent and Filebeat give it.
fn agent(host: &str, os: Value, h: u64) -> (Value, Value, Value) {
    let id = uuid(mix(h ^ 0xA9E7));
    let host = json!({
        "hostname": host, "name": host, "architecture": "x86_64",
        "id": format!("{:032x}", u128::from(mix(h)) << 64 | u128::from(mix(h ^ 1))),
        "ip": [format!("10.{}.{}.{}", h % people::NETWORKS, (h >> 8) % 41, 2 + (h >> 16) % 248)],
        "os": os,
    });
    let agent = json!({"type": "filebeat", "version": VERSION, "name": host["name"], "id": id,
                       "ephemeral_id": uuid(mix(h ^ 0xE9E7))});
    let elastic_agent = json!({"id": id, "version": VERSION, "snapshot": false});
    (host, agent, elastic_agent)
}

fn uuid(h: u64) -> String {
    let g = mix(h);
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        h >> 32,
        h & 0xffff,
        (g >> 4) & 0xfff,
        0x8000 | ((g >> 20) & 0x3fff),
        g >> 16
    )
}

/// The envelope every event shares: who sent it, into which data stream.
fn envelope(at: &Clock, dataset: &str, host: Value, agent: Value, elastic_agent: Value, input: &str) -> Value {
    json!({
        "@timestamp": at.iso_millis(),
        "@metadata": {"beat": "filebeat", "type": "_doc", "version": VERSION},
        "agent": agent,
        "data_stream": {"type": "logs", "dataset": dataset, "namespace": "default"},
        "ecs": {"version": "8.0.0"},
        "elastic_agent": elastic_agent,
        "event": {"dataset": dataset},
        "host": host,
        "input": {"type": input},
    })
}

// -- Linux --------------------------------------------------------------------

const LINUX_HOSTS: &[&str] = &["bastion-01", "bastion-02", "app-db-01", "web-01", "web-02", "build-01"];
const SYSLOG: &[&str] = &[
    "systemd[1]: Started Session {n} of User {user}.",
    "systemd[1]: Starting Daily apt upgrade and clean activities...",
    "kernel: [UFW BLOCK] IN=eth0 OUT= SRC={ip} DST=10.0.0.5 PROTO=TCP SPT={port} DPT=23 WINDOW=1024 SYN",
    "CRON[{n}]: (root) CMD (/usr/local/bin/backup.sh >/dev/null 2>&1)",
    "dockerd[{n}]: time=\"{iso}\" level=info msg=\"ignoring event\" container={hex} module=libcontainerd",
    "kernel: Out of memory: Killed process {n} (java) total-vm:8123456kB, anon-rss:4012345kB",
];

/// Event `i` of a Linux host's agent, at `at`. With `guessing`, its auth log is one of a password-guessing run's
/// attempts (`auth::record`).
pub fn linux(i: u64, at: &Clock, guessing: bool) -> String {
    let a = mix(i);
    let k = a % LINUX_HOSTS.len() as u64;
    let name = LINUX_HOSTS[k as usize];
    let os = json!({"type": "linux", "family": "debian", "platform": "ubuntu", "name": "Ubuntu",
                    "version": "22.04.4 LTS (Jammy Jellyfish)", "kernel": "5.15.0-118-generic"});
    let (host, agent, elastic_agent) = agent(name, os, k);
    let auth = guessing || (a >> 8).is_multiple_of(3);
    let (dataset, path, line) = if auth {
        // auth.log's own line, with this host's name in it.
        let line = auth::record(i, at, guessing);
        let line = match line.splitn(5, ' ').collect::<Vec<_>>()[..] {
            [m, d, t, _, rest] => format!("{m} {d} {t} {name} {rest}"),
            _ => line,
        };
        ("system.auth", "/var/log/auth.log", line)
    } else {
        let user = people::person(a >> 20).login;
        let msg = pick(SYSLOG, a >> 12)
            .replace("{n}", &((a >> 24) % 60_000 + 100).to_string())
            .replace("{user}", &user)
            .replace("{ip}", people::PUBLIC[((a >> 32) as usize) % people::PUBLIC.len()].0)
            .replace("{port}", &(1024 + (a >> 40) % 60_000).to_string())
            .replace("{iso}", &at.iso())
            .replace("{hex}", &format!("{:016x}", mix(a)));
        (
            "system.syslog",
            "/var/log/syslog",
            format!("{} {name} {msg}", at.syslog()),
        )
    };
    let mut e = envelope(at, dataset, host, agent, elastic_agent, "log");
    let o = e.as_object_mut().expect("an object");
    o.insert(
        "log".into(),
        json!({"file": {"path": path}, "offset": (i % 10_000_000) * 97}),
    );
    o.insert("message".into(), json!(line));
    e.to_string()
}

// -- Windows ------------------------------------------------------------------

/// The Windows events the demo's hosts write: channel, provider, event ID, and what Windows says it is.
const SECURITY: &str = "Microsoft-Windows-Security-Auditing";
const SYSMON: &str = "Microsoft-Windows-Sysmon";
const PROCESSES: &[(&str, &str)] = &[
    ("C:\\Windows\\System32\\cmd.exe", "cmd.exe /c whoami /all"),
    (
        "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
        "powershell.exe -NoProfile -Command Get-Process",
    ),
    (
        "C:\\Program Files\\Microsoft Office\\root\\Office16\\OUTLOOK.EXE",
        "\"OUTLOOK.EXE\" /recycle",
    ),
    (
        "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
        "\"chrome.exe\" --type=renderer",
    ),
    ("C:\\Windows\\System32\\svchost.exe", "svchost.exe -k netsvcs -p"),
    ("C:\\Users\\Public\\update.exe", "update.exe -silent"),
];

/// A kind of Windows event: the data stream it lands in, where Windows logs it, and what it says.
struct WinEvent {
    dataset: &'static str,
    channel: &'static str,
    provider: &'static str,
    code: u32,
    action: &'static str,
    outcome: &'static str,
    message: &'static str,
}

const LOGON: WinEvent = WinEvent {
    dataset: "system.security",
    channel: "Security",
    provider: SECURITY,
    code: 4624,
    action: "logged-in",
    outcome: "success",
    message: "An account was successfully logged on.",
};
const LOGON_FAILED: WinEvent = WinEvent {
    code: 4625,
    action: "logon-failed",
    outcome: "failure",
    message: "An account failed to log on.",
    ..LOGON
};
const PROCESS_CREATED: WinEvent = WinEvent {
    code: 4688,
    action: "created-process",
    message: "A new process has been created.",
    ..LOGON
};
const SYSMON_PROCESS: WinEvent = WinEvent {
    dataset: "windows.sysmon_operational",
    channel: "Microsoft-Windows-Sysmon/Operational",
    provider: SYSMON,
    code: 1,
    action: "Process Create (rule: ProcessCreate)",
    outcome: "success",
    message: "Process Create:",
};
const SYSMON_NETWORK: WinEvent = WinEvent {
    code: 3,
    action: "Network connection detected (rule: NetworkConnect)",
    message: "Network connection detected:",
    ..SYSMON_PROCESS
};

/// Event `i` of a Windows workstation's agent, at `at`. With `spraying`, it is a failed logon (4625) from one
/// address trying many accounts.
pub fn windows(i: u64, at: &Clock, spraying: bool) -> String {
    let a = mix(i);
    let b = mix(i ^ 0x3D1C_0000);
    let owner = people::person(a % people::PEOPLE as u64);
    let (site, _) = people::site_of(a >> 12);
    let name = format!("WS-{}-{:04}", site.to_uppercase(), a % people::PEOPLE as u64);
    let os = json!({"type": "windows", "family": "windows", "platform": "windows", "name": "Windows 11 Enterprise",
                    "version": "10.0", "build": "22631.4317", "kernel": "10.0.22621.4317 (WinBuild.160101.0800)"});
    let (host, agent, elastic_agent) = agent(&name, os, a % people::PEOPLE as u64);
    let logon_id = format!("0x{:x}", 0x3e7 + (b >> 8) % 0xffffff);
    let record_id = (i % 10_000_000) + 120_000;
    let fqdn = format!("{name}.acme.example");
    let user = owner.login;

    let kind = if spraying { 1 } else { (b >> 56) % 8 };
    let (
        WinEvent {
            dataset,
            channel,
            provider,
            code,
            action,
            outcome,
            message,
        },
        data,
    ) = match kind {
        0 | 2 => (
            LOGON,
            json!({"SubjectUserSid": "S-1-5-18", "SubjectUserName": format!("{name}$"), "SubjectDomainName": "ACME",
                       "TargetUserName": user, "TargetDomainName": "ACME", "TargetLogonId": logon_id,
                       "LogonType": pick(&["2", "3", "7", "10", "11"], b >> 4), "LogonProcessName": "User32",
                       "AuthenticationPackageName": "Negotiate", "WorkstationName": name,
                       "IpAddress": "127.0.0.1", "IpPort": "0", "ElevatedToken": "%%1843"}),
        ),
        1 => {
            let from = if spraying {
                "89.160.20.156"
            } else {
                people::PUBLIC[(b >> 4) as usize % people::PUBLIC.len()].0
            };
            let target = if spraying {
                people::person(b >> 8).login
            } else {
                user.clone()
            };
            (
                LOGON_FAILED,
                json!({"SubjectUserSid": "S-1-0-0", "TargetUserName": target, "TargetDomainName": "ACME",
                           "Status": "0xc000006d", "SubStatus": "0xc000006a", "FailureReason": "%%2313",
                           "LogonType": "3", "LogonProcessName": "NtLmSsp", "AuthenticationPackageName": "NTLM",
                           "WorkstationName": "-", "IpAddress": from, "IpPort": (40_000 + (b >> 20) % 20_000).to_string()}),
            )
        }
        3 | 4 => {
            let (image, cmd) = PROCESSES[(b >> 12) as usize % PROCESSES.len()];
            (
                PROCESS_CREATED,
                json!({"SubjectUserName": user, "SubjectDomainName": "ACME", "SubjectLogonId": logon_id,
                           "NewProcessId": format!("0x{:x}", (b >> 20) % 0xffff), "NewProcessName": image,
                           "TokenElevationType": "%%1938", "ProcessId": format!("0x{:x}", (b >> 36) % 0xffff),
                           "CommandLine": cmd, "ParentProcessName": "C:\\Windows\\explorer.exe",
                           "MandatoryLabel": "S-1-16-8192"}),
            )
        }
        5 | 6 => {
            let (image, cmd) = PROCESSES[(b >> 12) as usize % PROCESSES.len()];
            (
                SYSMON_PROCESS,
                json!({"RuleName": "-", "UtcTime": format!("{} {}.{:03}", &at.iso()[..10], at.hms(), at.nanos / 1_000_000),
                           "ProcessGuid": format!("{{{}}}", uuid(b)), "ProcessId": ((b >> 20) % 30_000).to_string(),
                           "Image": image, "CommandLine": cmd, "CurrentDirectory": format!("C:\\Users\\{user}\\"),
                           "User": format!("ACME\\{user}"), "IntegrityLevel": "Medium",
                           "Hashes": format!("SHA256={:016X}{:016X}{:016X}{:016X}", mix(b), mix(b ^ 1), mix(b ^ 2), mix(b ^ 3)),
                           "ParentImage": "C:\\Windows\\explorer.exe", "ParentCommandLine": "C:\\Windows\\Explorer.EXE"}),
            )
        }
        _ => {
            let (image, _) = PROCESSES[(b >> 12) as usize % PROCESSES.len()];
            let (dst, _) = people::PUBLIC[(b >> 4) as usize % people::PUBLIC.len()];
            (
                SYSMON_NETWORK,
                json!({"RuleName": "-", "ProcessId": ((b >> 20) % 30_000).to_string(), "Image": image,
                           "User": format!("ACME\\{user}"), "Protocol": "tcp", "Initiated": "true",
                           "SourceIp": host["ip"][0], "SourcePort": (49_152 + (b >> 30) % 16_000).to_string(),
                           "DestinationIp": dst, "DestinationPort": pick(&["443", "443", "80", "445", "3389"], b >> 40)}),
            )
        }
    };
    let mut e = envelope(at, dataset, host, agent, elastic_agent, "winlog");
    let o = e.as_object_mut().expect("an object");
    let ev = o["event"].as_object_mut().expect("an object");
    ev.insert("code".into(), json!(code.to_string()));
    ev.insert("provider".into(), json!(provider));
    ev.insert("action".into(), json!(action));
    ev.insert("outcome".into(), json!(outcome));
    ev.insert("kind".into(), json!("event"));
    ev.insert("created".into(), json!(at.plus(1).iso_millis()));
    o.insert(
        "winlog".into(),
        json!({
            "channel": channel, "provider_name": provider, "event_id": code.to_string(), "computer_name": fqdn,
            "record_id": record_id.to_string(), "api": "wineventlog", "opcode": "Info",
            "keywords": [if outcome == "failure" { "Audit Failure" } else { "Audit Success" }],
            "process": {"pid": 4 + (b >> 44) % 900, "thread": {"id": 100 + (b >> 50) % 9_000}},
            "event_data": data,
        }),
    );
    o.insert("log".into(), json!({"level": "information"}));
    o.insert("message".into(), json!(message));
    e.to_string()
}

#[test]
fn every_event_is_a_document_in_a_data_stream() {
    let at = Clock::at(1_790_000_000, 0);
    for i in 0..300 {
        for (line, family) in [
            (linux(i, &at, false), "debian"),
            (windows(i, &at, i % 2 == 0), "windows"),
        ] {
            let doc: Value = serde_json::from_str(&line).expect("JSON");
            assert_eq!(doc["host"]["os"]["family"], family);
            assert_eq!(doc["data_stream"]["type"], "logs");
            assert_eq!(doc["@timestamp"], "2026-09-21T14:13:20.000Z");
        }
    }
    let spray: Value = serde_json::from_str(&windows(7, &at, true)).expect("JSON");
    assert_eq!(spray["winlog"]["event_id"], "4625");
}
