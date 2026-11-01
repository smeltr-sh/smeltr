//! Linux `auth.log` from a few bastion hosts, as rsyslog writes it: sshd's
//! logins, refusals and session ends, and sudo's commands. The logins come
//! from the company's own networks and people (`people`); the refusals from
//! the internet, guessing at common account names.

use super::{Clock, mix, people, pick};

const HOSTS: &[&str] = &["bastion-01", "bastion-02", "app-db-01"];
/// Where the password guessing comes from.
const ATTACKERS: &[&str] = &["89.160.20.156", "175.16.199.60", "67.43.156.44"];
/// The names it guesses.
const PROBES: &[&str] = &["root", "admin", "test", "oracle", "postgres", "ubuntu", "git"];
const COMMANDS: &[&str] = &[
    "/usr/bin/systemctl restart nginx",
    "/usr/bin/journalctl -u smeltr",
    "/bin/cat /var/log/secure",
    "/usr/bin/apt-get upgrade",
    "/usr/bin/docker ps",
];

/// Line `i`, logged at `at`. With `guessing`, someone on the internet is
/// guessing passwords, and every line is one of their attempts.
pub fn record(i: u64, at: &Clock, guessing: bool) -> String {
    let a = mix(i);
    let b = mix(i ^ 0xA07B_10C5);
    let host = pick(HOSTS, a);
    let pid = 1_000 + (a >> 8) % 64_000;
    let port = 30_000 + (b >> 8) % 35_000;
    let login = people::person(b >> 24).login;
    let msg = match (a >> 32) & 0xff {
        _ if guessing => attempt(a, pid, port),
        0..=89 => attempt(a, pid, port),
        90..=178 => {
            let method = pick(&["publickey", "publickey", "password"], b);
            let key = if method == "publickey" {
                format!(
                    ": ED25519 SHA256:{:016x}{:016x}{:011x}",
                    mix(b),
                    mix(b ^ 1),
                    mix(b ^ 2) >> 20
                )
            } else {
                String::new()
            };
            let net = (b >> 40) % people::NETWORKS;
            format!(
                "sshd[{pid}]: Accepted {method} for {login} from 10.{net}.{}.{} port {port} ssh2{key}",
                (b >> 48) % 41,
                2 + (b >> 56) % 248
            )
        }
        179..=224 => format!(
            "sudo:   {login} : TTY=pts/{} ; PWD=/home/{login} ; USER=root ; COMMAND={}",
            (b >> 4) % 5,
            pick(COMMANDS, b >> 12)
        ),
        _ => format!("sshd[{pid}]: pam_unix(sshd:session): session closed for user {login}"),
    };
    format!("{} {host} {msg}", at.syslog())
}

/// One guess: a wrong password, or a name the host does not have.
fn attempt(a: u64, pid: u64, port: u64) -> String {
    let from = pick(ATTACKERS, a >> 16);
    let who = pick(PROBES, a >> 20);
    if (a >> 40) & 1 == 0 {
        let invalid = if who == "root" { "" } else { "invalid user " };
        format!("sshd[{pid}]: Failed password for {invalid}{who} from {from} port {port} ssh2")
    } else {
        format!("sshd[{pid}]: Invalid user {who} from {from} port {port}")
    }
}

#[test]
fn guessing_makes_every_line_an_attempt_from_the_internet() {
    let at = Clock::at(1_790_000_000, 0);
    for i in 0..200 {
        let line = record(i, &at, true);
        assert!(line.starts_with("Sep 21 14:13:20 "), "{line}");
        assert!(
            ATTACKERS.iter().any(|a| line.contains(&format!("from {a} port"))),
            "{line}"
        );
    }
    let calm: Vec<String> = (0..200).map(|i| record(i, &at, false)).collect();
    assert!(calm.iter().any(|l| l.contains("Accepted ")) && calm.iter().any(|l| l.contains("sudo: ")));
}
