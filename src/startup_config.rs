//! The optional TOML startup file, loaded once at the start of `main`. `smeltr` defines the schema; this module finds
//! the file, parses it, and backfills whichever env vars are not already set.
//!
//! Precedence per setting: a set env var wins; otherwise the file's value; otherwise the default below. Reading still
//! goes through the environment, unaware where a value came from, which is what lets a container bake a file in and
//! override one field with `-e`.
//!
//! The reader for each setting lives here too, one function per field of `smeltr.example.toml`, so a default is written
//! once and the variable name is spelled once. A caller that reads `SMELTR_*` itself will drift from the documented
//! default; call the accessor.

use std::num::{NonZeroU16, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// `SMELTR_CONFIG_FILE` wins even when the path does not exist, so an operator who set it gets an error rather than a
/// silent fallback.
fn resolve_config_path(env_value: Option<&str>, default_path_exists: bool, default_filename: &str) -> Option<PathBuf> {
    if let Some(path) = env_value {
        return Some(PathBuf::from(path));
    }
    if default_path_exists {
        return Some(PathBuf::from(default_filename));
    }
    None
}

/// `SMELTR_CONFIG_FILE` if set, else `./<default_filename>` if it exists, else `None`.
pub fn find_config_file(default_filename: &str) -> Option<PathBuf> {
    let env_value = std::env::var("SMELTR_CONFIG_FILE").ok();
    let default_path_exists = Path::new(default_filename).exists();
    resolve_config_path(env_value.as_deref(), default_path_exists, default_filename)
}

/// Parses `path` as TOML into `T`, refusing to start on any failure. `T` must declare `deny_unknown_fields`, or a
/// misspelled key would parse and do nothing. Prints and exits rather than panicking: a typo in the operator's file is
/// not a crash.
pub fn parse_config_file<T: serde::de::DeserializeOwned>(path: &Path) -> T {
    let refuse = |problem: String| -> ! {
        eprintln!(
            "smeltr: refusing to start -- {} is not usable:\n{problem}\n\
             \n\
             This file is optional: every setting in it has a working default. \
             Delete or rename it to start without it, or see smeltr.example.toml \
             for the field list.",
            path.display()
        );
        std::process::exit(1);
    };
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| refuse(e.to_string()));
    toml::from_str(&text).unwrap_or_else(|e| refuse(e.to_string()))
}

/// An env var set to the empty string counts as unset: that is what an unset docker-compose variable expands to.
fn should_backfill(current: Option<&str>) -> bool {
    match current {
        None => true,
        Some(existing) => existing.is_empty(),
    }
}

/// Sets `env_var` to `value` unless it already holds a non-empty value. `None` is a no-op.
///
/// `set_var` is unsafe because it races concurrent readers. Every call happens at the start of `main`, before any other
/// thread exists.
pub fn backfill_env(env_var: &str, value: Option<impl ToString>) {
    let Some(value) = value else { return };
    let current = std::env::var(env_var).ok();
    if should_backfill(current.as_deref()) {
        unsafe { std::env::set_var(env_var, value.to_string()) };
    }
}

/// Adds this host's loopback names to `NO_PROXY` and `no_proxy`, and `own`, the address a leader here listens on,
/// keeping every entry the operator listed in either.
///
/// A request to this host never leaves it, so a proxy can only carry it, and any credential it holds, to a machine
/// where its own address answers. The worker inside a leader calls it at `own`, which need not be loopback. Every
/// HTTP client in smeltr, `ureq` and curl alike, reads these variables, so this one call covers each of them,
/// including any added later. Called at the start of `main`, before any other thread exists.
pub fn never_proxy_this_host(own: Option<std::net::IpAddr>) {
    let listed = ["NO_PROXY", "no_proxy"].map(|var| std::env::var(var).unwrap_or_default());
    let value = with_this_host(&listed.join(","), own);
    for var in ["NO_PROXY", "no_proxy"] {
        unsafe { std::env::set_var(var, &value) };
    }
}

/// `listed`, a comma-separated host list, with every loopback entry and `own` added once. Each client spells a match
/// its own way: `127.` is `ureq`'s prefix and `127.0.0.0/8` curl's range, `[::1]` is how `ureq` sees an IPv6 host
/// and `::1` how curl does.
fn with_this_host(listed: &str, own: Option<std::net::IpAddr>) -> String {
    const LOOPBACK: [&str; 5] = ["localhost", "127.", "127.0.0.0/8", "::1", "[::1]"];
    let own: Vec<String> = match own {
        Some(std::net::IpAddr::V6(v6)) => vec![v6.to_string(), format!("[{v6}]")],
        Some(v4) => vec![v4.to_string()],
        None => Vec::new(),
    };
    let mut entries: Vec<&str> = Vec::new();
    for entry in listed
        .split(',')
        .map(str::trim)
        .chain(LOOPBACK)
        .chain(own.iter().map(String::as_str))
    {
        if !entry.is_empty() && !entries.contains(&entry) {
            entries.push(entry);
        }
    }
    entries.join(",")
}

/// The boolean spellings a setting accepts, case- and space-insensitive. `None` for anything else.
fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "on" | "true" | "yes" | "y" | "t" => Some(true),
        "0" | "off" | "false" | "no" | "n" | "f" => Some(false),
        _ => None,
    }
}

/// A setting that is set but unusable refuses to start. There is no fallback: an absent variable takes its documented
/// default, a present one means what it says or nothing at all.
fn refuse(var: &str, value: &str, expected: &str) -> ! {
    eprintln!(
        "smeltr: refusing to start -- {var} is {value:?}, which is not {expected}.\n\
         \n\
         Unset it to take the default; there is no partial reading of it."
    );
    std::process::exit(1);
}

/// `var` as a boolean, or `default` when it is unset. A value that is not a boolean refuses to start.
fn flag(var: &str, default: bool) -> bool {
    match env(var) {
        None => default,
        Some(v) => parse_bool(&v).unwrap_or_else(|| refuse(var, &v, "1, on, true, yes, 0, off, false, or no")),
    }
}

/// `var` parsed as `T`, or `default()` when it is unset. A value that will not parse refuses to start.
fn parsed<T: std::str::FromStr>(var: &str, expected: &str, default: impl FnOnce() -> T) -> T {
    match env(var) {
        None => default(),
        Some(v) => v.trim().parse().unwrap_or_else(|_| refuse(var, &v, expected)),
    }
}
// -- reading the settings ---------------------------------------------------

// The defaults, in one place. `smeltr.example.toml` shows each of these against its field, and a test below holds the
// two together.
const DEFAULT_GUI_PORT: NonZeroU16 = NonZeroU16::new(443).expect("443 is not zero");
const DEFAULT_GUI_BIND: &str = "127.0.0.1";
const DEFAULT_LEADER_DATA_DIR: &str = "smeltr-leader-data";
const DEFAULT_LEADER_SECRET_KEY: &str = "smeltr-leader-secrets/sealing.key";
const DEFAULT_LEADER_SECRET_VERSIONS: usize = 5;
const DEFAULT_LEADER_LOOKUP_VERSIONS: usize = 5;
const DEFAULT_LEADER_CA_VALIDITY: &str = "3650d";
const DEFAULT_WORKER_STATE_DIR: &str = "smeltr-worker-data";
const DEFAULT_WORKER_HEARTBEAT_INTERVAL_MS: u64 = 1500;
const DEFAULT_WORKER_METRICS_ENDPOINT_PORT: NonZeroU16 = NonZeroU16::new(9464).expect("9464 is not zero");
const DEFAULT_OIDC_REFRESH_INTERVAL_MS: u64 = 5 * 60 * 1000;
/// Where a certificate authority connects for an HTTP-01 challenge (`leader::acme`): port 80, as RFC 8555 fixes it.
const DEFAULT_ACME_HTTP_PORT: NonZeroU16 = NonZeroU16::new(80).expect("80 is not zero");
/// Only when the core count cannot be read at all, which is why it is not in `smeltr.example.toml`: there is no field
/// that sets it.
const FALLBACK_WORKER_THREADS: NonZeroUsize = NonZeroUsize::new(2).expect("2 is not zero");

/// A set, non-empty variable. Empty is unset, matching [`backfill_env`]: that is what an unset docker-compose variable
/// expands to.
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// How much a process says about itself: `error`, `warn`, `info` (the default), `debug` or `trace`. Everything
/// below it is refused at its call site, so a level nobody reads costs nothing to leave in the code.
pub fn log_level() -> tracing::Level {
    let Some(value) = env("SMELTR_LOG_LEVEL") else {
        return tracing::Level::INFO;
    };
    crate::logs::level_for_label(&value).unwrap_or_else(|| {
        refuse(
            "SMELTR_LOG_LEVEL",
            &value,
            &format!("one of {}", crate::logs::LEVEL_LABELS.join(", ")),
        )
    })
}

/// Whether this start regenerates the built-in administrator's password.
///
/// The way back in when nobody can sign in: a forgotten password, an administrator another one disabled, a
/// revocation that left nobody. Holding the leader's host and its data directory is the authorization, which is
/// why it is a startup setting and not a route.
///
/// It acts on every start it is set for, so leaving it set in a unit file or a compose file resets the password
/// and signs everyone out at each restart. The leader says so on the console when it sees one.
pub fn recover_admin() -> bool {
    flag("SMELTR_RECOVER_ADMIN", false)
}

/// The origin a browser reaches this leader at, as scheme, host and port -- `https://leader.example.com`. A
/// leader with more than one name -- a proxy reached under two hostnames, say -- takes a comma-separated list.
///
/// It decides three things a socket cannot: which `Origin` header a request is allowed to carry -- beside the
/// [`LOOPBACK_NAMES`] a leader serving its own TLS on a bind that reaches loopback also answers to -- whether the
/// session cookie is marked `Secure`, and therefore whether the cookie is host-locked. A leader behind a proxy
/// that terminates TLS serves plain HTTP and is still reached over `https`, so the socket is the wrong thing to
/// ask.
///
/// `None` derives it from the TLS setting and the bind address, which is right for a laptop and for the leader's
/// own certificate, and wrong for anything behind a proxy.
pub fn public_origin() -> Option<String> {
    env("SMELTR_PUBLIC_ORIGIN").map(|v| v.trim().to_string())
}

/// The port the GUI and both APIs listen on. The default, 443, is the standard HTTPS port and needs a
/// capability this process may not have: on Linux, binding it unprivileged requires
/// `CAP_NET_BIND_SERVICE` (or root); Windows has no such restriction. A leader that cannot bind it sets
/// `SMELTR_GUI_PORT` to an unprivileged one, typically behind a reverse proxy.
pub fn gui_port() -> u16 {
    parsed("SMELTR_GUI_PORT", "a port between 1 and 65535", || DEFAULT_GUI_PORT).get()
}

/// The address to bind. Loopback by default, which a container's host cannot reach; a deployment sets `0.0.0.0`.
pub fn gui_bind() -> String {
    env("SMELTR_GUI_BIND")
        .map(|b| b.trim().to_string())
        .filter(|b| !b.is_empty())
        .unwrap_or_else(|| DEFAULT_GUI_BIND.to_string())
}

/// Everything the leader owns on disk: `leader.db`, `repos/`, `tls/`.
pub fn leader_data_dir() -> PathBuf {
    env("SMELTR_LEADER_DATA_DIR")
        .unwrap_or_else(|| DEFAULT_LEADER_DATA_DIR.to_string())
        .into()
}

/// The key that seals leader-stored secret values. Outside [`leader_data_dir`] by default and meant to stay there:
/// one archive of the data directory must not carry both the sealed values and the key that opens them.
pub fn leader_secret_key_path() -> PathBuf {
    env("SMELTR_LEADER_SECRET_KEY")
        .unwrap_or_else(|| DEFAULT_LEADER_SECRET_KEY.to_string())
        .into()
}

/// How many versions of each secret the leader keeps. A bundle quotes the version it runs, so this is how far back
/// a rollback can reach; whatever a deployed bundle names is kept regardless, so the fleet is never cut off by it.
/// It is also the blast radius: a stolen backup and key yield this many credentials per secret rather than one.
pub fn leader_secret_versions() -> usize {
    versions_setting("SMELTR_LEADER_SECRET_VERSIONS", DEFAULT_LEADER_SECRET_VERSIONS)
}

/// How many versions of each uploaded lookup file the leader keeps, counted by the name it was uploaded under: how
/// far back a rollback can reach. Whatever a branch or a running deployment names is kept regardless
/// (`leader::lookups::prune`).
pub fn leader_lookup_versions() -> usize {
    versions_setting("SMELTR_LEADER_LOOKUP_VERSIONS", DEFAULT_LEADER_LOOKUP_VERSIONS)
}

/// `name` as how many versions to keep, or `default` when it is unset: a count of at least 1, or `-1` for every
/// version, which reads as `usize::MAX` so keeping the newest `n` keeps them all. Anything else refuses to start.
fn versions_setting(name: &str, default: usize) -> usize {
    match env(name) {
        None => default,
        Some(raw) => match raw.trim().parse::<i64>() {
            Ok(-1) => usize::MAX,
            Ok(n) if n >= 1 => usize::try_from(n).unwrap_or(usize::MAX),
            _ => refuse(name, &raw, "a count of at least 1, or -1 to keep every version"),
        },
    }
}

/// Whether the leader serves HTTPS with its own certificate. On unless the variable is exactly `off`: a deployment that
/// terminates TLS in front of this process turns it off, and nothing else should.
pub fn leader_tls_enabled() -> bool {
    flag("SMELTR_LEADER_TLS", true)
}

/// Extra names to put in the leader's certificate, beyond `localhost`, `127.0.0.1` and every host `public_origin`
/// names. For a name the certificate must carry that is not itself a browser-facing origin -- a fleet-internal
/// DNS name a remote worker reaches this leader by, say. Comma-separated.
pub fn leader_tls_extra_sans() -> Vec<String> {
    env("SMELTR_LEADER_TLS_EXTRA_SAN")
        .map(|extra| split_sans(&extra))
        .unwrap_or_default()
}

/// The bounds on [`leader_ca_validity`]. Under a day an authority expires while an operator is still copying it
/// out; over a century it is one that never expires, which is not a thing to arrive at by mistyping a number.
const MIN_LEADER_CA_VALIDITY: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_LEADER_CA_VALIDITY: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

/// How long the leader's certificate authority lives. Ten years unless a policy says otherwise.
///
/// The authority is the one file an operator copies to every worker, so replacing it is a visit to all of them and
/// the default is long. A shorter one is a deliberate choice to make that visit on a schedule.
///
/// It applies when the authority is created; one already on disk keeps its own period. The certificate the leader
/// signs takes the same period and is reissued at every start, so it is never what expires first.
/// The names a leader answers to on its own machine, whatever else it is called. Its certificate always carries
/// them, since the worker inside the process reaches its leader by one of them, and a bind that reaches loopback
/// accepts a browser's `Origin` under either -- see `resolve_origin` in the `smeltr` binary.
pub const LOOPBACK_NAMES: [&str; 2] = ["localhost", "127.0.0.1"];

/// The names the leader's certificate carries: [`LOOPBACK_NAMES`] always, every host in `origin_hosts` -- the
/// same hosts a browser's `Origin` is checked against -- and whatever [`leader_tls_extra_sans`] adds beyond that.
/// Read wherever one is written or checked, so a certificate is never judged against a different list than the
/// one it was issued for.
pub fn leader_certificate_names(origin_hosts: &[String]) -> Vec<String> {
    let mut names: Vec<String> = LOOPBACK_NAMES.iter().map(|name| name.to_string()).collect();
    for host in origin_hosts.iter().cloned().chain(leader_tls_extra_sans()) {
        if !names.iter().any(|n| n.eq_ignore_ascii_case(&host)) {
            names.push(host);
        }
    }
    names
}

pub fn leader_ca_validity() -> Duration {
    bounded_ca_validity(duration_setting("SMELTR_LEADER_CA_VALIDITY"))
}

/// The port the leader listens on for a certificate authority's HTTP-01 challenge, while an order is open. An
/// authority connects to port 80; another port is for whatever forwards 80 here, and for a test authority told to
/// connect elsewhere.
pub fn acme_http_port() -> u16 {
    parsed("SMELTR_ACME_HTTP_PORT", "a port between 1 and 65535", || {
        DEFAULT_ACME_HTTP_PORT
    })
    .get()
}

/// What [`leader_ca_validity`] answers for a given setting. `None` is unset, which takes the default.
fn bounded_ca_validity(configured: Option<Duration>) -> Duration {
    configured
        .unwrap_or_else(|| parse_duration(DEFAULT_LEADER_CA_VALIDITY).expect("the default is a duration"))
        .clamp(MIN_LEADER_CA_VALIDITY, MAX_LEADER_CA_VALIDITY)
}

/// Whether this process runs a leader. Off by default.
pub fn leader_enabled() -> bool {
    flag("SMELTR_LEADER_ENABLED", false)
}

/// Whether this process runs a worker. Off by default, and independent of [`leader_enabled`]: a process that enables
/// both runs a leader and a worker that reports to it.
pub fn worker_enabled() -> bool {
    flag("SMELTR_WORKER_ENABLED", false)
}

/// The leader a worker reports to. Required of a worker that does not run one itself; a process that runs both
/// takes its own address when this is unset.
pub fn leader_url() -> Option<String> {
    env("SMELTR_LEADER_URL")
}

/// The host in `url`, and the port it reaches, defaulted from the scheme. `None` when `url` names neither a
/// scheme this leader speaks nor a host.
fn url_authority(url: &str) -> Option<(String, u16)> {
    let (scheme, rest) = url.split_once("://")?;
    let default = match scheme.to_ascii_lowercase().as_str() {
        "https" => 443,
        "http" => 80,
        _ => return None,
    };
    let (host, port) = split_authority(rest.split(['/', '?', '#']).next().unwrap_or_default())?;
    Some((host, port.unwrap_or(default)))
}

/// The host in `authority` (`host`, `host:port`, `[v6]`, `[v6]:port`), and the port where it carries one.
///
/// What a certificate is checked against is the host alone: a port is not a name, and no certificate carries one.
pub fn split_authority(authority: &str) -> Option<(String, Option<u16>)> {
    // Credentials are not a host, and an IPv6 literal's own colons are not a port separator.
    let authority = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    let (host, port) = match authority.strip_prefix('[') {
        Some(after) => {
            let (host, rest) = after.split_once(']')?;
            match rest.strip_prefix(':') {
                Some(port) => (host, Some(port.parse::<u16>().ok()?)),
                None => (host, None),
            }
        }
        None => match authority.rsplit_once(':') {
            Some((host, port)) => (host, Some(port.parse::<u16>().ok()?)),
            None => (authority, None),
        },
    };
    (!host.is_empty()).then(|| (host.to_string(), port))
}

/// The name a worker configured with `url` calls its leader by. What a certificate has to serve for that worker to
/// reach it, which is not always what a browser opens the leader under.
pub fn leader_url_host(url: &str) -> Option<String> {
    url_authority(url).map(|(host, _)| host)
}

/// Whether `url` names the leader inside this process: on this leader's own `port`, a loopback host or one of the
/// `names` its certificate is issued for ([`leader_certificate_names`]).
///
/// An operator who writes out the address of the leader they are already running, by any name it answers to, means
/// the same thing as one who left it unset, and has to get the same worker.
pub fn names_this_leader(url: &str, port: u16, names: &[String]) -> bool {
    url_authority(url).is_some_and(|(host, reaching)| {
        reaching == port && (is_loopback(&host) || names.iter().any(|name| name.eq_ignore_ascii_case(&host)))
    })
}

/// Whether `host`, without brackets or port, names this machine: `localhost` or a loopback address.
pub fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost") || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// The leader's certificate authority, by path: the worker's copy of that leader's `tls/ca.pem`. `None` leaves the
/// platform's own roots in place, which is correct only against a leader on plain HTTP or behind a proxy holding a
/// publicly issued certificate.
pub fn leader_ca_path() -> Option<String> {
    env("SMELTR_LEADER_CA")
}

/// Whether a worker trusts the public roots as well as its leader's authority, as an identity's `trustPublic` does.
/// On by default; off, the leader's certificate must chain to an authority the worker holds.
pub fn leader_trust_public() -> bool {
    flag("SMELTR_LEADER_TRUST_PUBLIC", true)
}

/// The group a worker serves when its own configuration names none. A fleet needs somewhere to put a worker, and a
/// first install has exactly one place to put it; an operator who wants more names them. The leader seeds this one
/// into `main`'s first commit (`leader::git::seed_first_commit`), so a worker that names nothing still runs a group
/// that exists.
pub const DEFAULT_WORKER_GROUP: &str = "default";

/// The group a worker serves. Reported on every heartbeat and never persisted, so moving a worker is this value
/// changing and the process restarting.
pub fn worker_group() -> String {
    env("SMELTR_WORKER_GROUP").unwrap_or_else(|| DEFAULT_WORKER_GROUP.to_string())
}

/// The availability zone a worker runs in, handed to each connector that can prefer what sits in its own zone (a Kafka
/// consumer names it as its rack). A fact about the host, so it is set per worker and never in a bundle, which every
/// worker of a group runs alike.
pub fn worker_zone() -> Option<String> {
    env("SMELTR_WORKER_ZONE")
        .map(|zone| zone.trim().to_string())
        .filter(|zone| !zone.is_empty())
}

/// The address every receiver a worker runs listens on, `[::]` by default: every address of the host, which a receiver
/// opens dual-stack. A fact about the host, so it is set per worker and never in a bundle, which every worker of a
/// group runs alike. Read when a worker starts, so one that is not an IP address refuses to start.
pub fn worker_listen_address() -> std::net::IpAddr {
    parsed("SMELTR_WORKER_LISTEN_ADDRESS", "an IP address", || {
        std::net::Ipv6Addr::UNSPECIFIED.into()
    })
}

/// A worker's identity, credential and git clone. Must survive a restart.
pub fn worker_state_dir() -> PathBuf {
    env("SMELTR_WORKER_STATE_DIR")
        .unwrap_or_else(|| DEFAULT_WORKER_STATE_DIR.to_string())
        .into()
}

/// Threads in the transform pool, and nothing else: ingest is one reader per source, egress one thread per
/// destination. Defaults to the core count, which is what transform is bound by -- it is the only CPU-bound stage.
///
/// 0 is the default too, so the example file can show a value that changes nothing: a pool of zero would be a
/// worker that runs no pipeline.
pub fn worker_thread_limit() -> usize {
    let cores = || {
        std::thread::available_parallelism()
            .unwrap_or(FALLBACK_WORKER_THREADS)
            .get()
    };
    match parsed(
        "SMELTR_WORKER_THREAD_LIMIT",
        "a thread count, or 0 for the core count",
        || 0usize,
    ) {
        0 => cores(),
        n => n,
    }
}

/// How long a worker may go without a successful heartbeat before it stops its deployment. `None` (the default, or
/// `-1`, which the example file shows) means never stop on its own: a leader outage must never become a data-plane
/// outage. A startup setting, not one the leader sends down, because the worker must be able to enforce it before its
/// first successful heartbeat -- the exact situation a leader-supplied policy could never cover.
pub fn worker_stop_after_unreachable_secs() -> Option<u64> {
    env("SMELTR_WORKER_STOP_AFTER_UNREACHABLE_SECS").and_then(|v| {
        stop_after_unreachable_secs(&v).unwrap_or_else(|| {
            refuse(
                "SMELTR_WORKER_STOP_AFTER_UNREACHABLE_SECS",
                &v,
                "a count of seconds, or -1 to never stop",
            )
        })
    })
}

/// What [`worker_stop_after_unreachable_secs`] reads `raw` as: `Some(None)` for `-1`, `None` for anything it refuses.
fn stop_after_unreachable_secs(raw: &str) -> Option<Option<u64>> {
    match raw.trim() {
        "-1" => Some(None),
        secs => secs.parse().ok().map(Some),
    }
}

/// How often a worker reports to the leader.
pub fn worker_heartbeat_interval() -> Duration {
    Duration::from_millis(parsed(
        "SMELTR_WORKER_HEARTBEAT_INTERVAL_MS",
        "a count of milliseconds (e.g. '1500')",
        || DEFAULT_WORKER_HEARTBEAT_INTERVAL_MS,
    ))
}

/// How often the leader refreshes each OIDC session's tokens at the provider, and so how long a change to a
/// person's groups there takes to reach this leader.
pub fn oidc_refresh_interval() -> Duration {
    Duration::from_millis(parsed(
        "SMELTR_OIDC_REFRESH_INTERVAL_MS",
        "a count of milliseconds (e.g. '300000')",
        || DEFAULT_OIDC_REFRESH_INTERVAL_MS,
    ))
}

/// Whether a worker serves its metrics for Prometheus to scrape (`worker::metrics_endpoint`). Off unless asked for.
pub fn worker_metrics_endpoint_enabled() -> bool {
    flag("SMELTR_WORKER_METRICS_ENDPOINT_ENABLED", false)
}

/// The port a worker serves its metrics on, at [`worker_listen_address`].
pub fn worker_metrics_endpoint_port() -> u16 {
    parsed(
        "SMELTR_WORKER_METRICS_ENDPOINT_PORT",
        "a port between 1 and 65535",
        || DEFAULT_WORKER_METRICS_ENDPOINT_PORT,
    )
    .get()
}

/// The PEM certificate chain, leaf first, a worker serves its metrics under. With [`worker_metrics_endpoint_tls_key`], the
/// metrics endpoint speaks only TLS.
pub fn worker_metrics_endpoint_tls_cert() -> Option<PathBuf> {
    env("SMELTR_WORKER_METRICS_ENDPOINT_TLS_CERT").map(PathBuf::from)
}

/// The unencrypted PEM private key of [`worker_metrics_endpoint_tls_cert`].
pub fn worker_metrics_endpoint_tls_key() -> Option<PathBuf> {
    env("SMELTR_WORKER_METRICS_ENDPOINT_TLS_KEY").map(PathBuf::from)
}

/// PEM roots a scraper's client certificate must chain to. Set, the metrics endpoint refuses a scraper without one.
pub fn worker_metrics_endpoint_client_ca() -> Option<PathBuf> {
    env("SMELTR_WORKER_METRICS_ENDPOINT_CLIENT_CA").map(PathBuf::from)
}

/// The user a scraper signs in as with basic auth, with [`worker_metrics_endpoint_password`].
pub fn worker_metrics_endpoint_user() -> Option<String> {
    env("SMELTR_WORKER_METRICS_ENDPOINT_USER")
}

/// Where [`worker_metrics_endpoint_password`] is read from.
pub const WORKER_METRICS_ENDPOINT_PASSWORD_ENV: &str = "SMELTR_WORKER_METRICS_ENDPOINT_PASSWORD";

/// The password of [`worker_metrics_endpoint_user`], as an environment variable holds it, so a secret store can set it.
pub fn worker_metrics_endpoint_password() -> Option<String> {
    env(WORKER_METRICS_ENDPOINT_PASSWORD_ENV)
}

/// The enrollment code a worker spends for a credential on its first run. Unused once `worker_state_dir` holds one.
///
/// It carries the authority to verify the leader with as well as the secret, so a worker given one needs nothing
/// else about the leader but its address.
pub fn worker_enrollment_code() -> Option<String> {
    env("SMELTR_WORKER_ENROLLMENT_CODE")
}

/// Split out so the parsing is testable without the environment.
fn split_sans(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// `name` as a duration, or `None` when it is unset. A value that is not a duration refuses to start.
fn duration_setting(name: &str) -> Option<Duration> {
    let raw = env(name)?;
    Some(parse_duration(&raw).unwrap_or_else(|| refuse(name, &raw, "a duration like \"30m\" or \"12h\"")))
}

/// `name` as a size above zero, or `default` when it is unset: `4GB`, `512 MiB`, or a bare count of bytes. Anything
/// else refuses to start.
fn size_setting(name: &str, default: usize) -> usize {
    match env(name) {
        None => default,
        Some(raw) => match crate::config_file::bytes::parse(&raw) {
            Ok(bytes) if bytes > 0 => bytes,
            _ => refuse(name, &raw, "a size above zero like \"4GB\" or \"512 MiB\""),
        },
    }
}

/// The ceiling on columnar memory every reader in a worker shares (`worker::process_memory_budget`).
pub fn memory_budget_bytes() -> usize {
    size_setting("SMELTR_MEMORY_BUDGET_BYTES", crate::worker::MEMORY_BUDGET_BYTES)
}

/// Raw bytes a transform thread runs through its pipelines at once (`cpu_cache::slice_bytes`), unless
/// `SMELTR_SLICE_BYTES` says otherwise.
pub fn slice_bytes() -> usize {
    size_setting("SMELTR_SLICE_BYTES", crate::cpu_cache::slice_bytes())
}

/// The columnar budget, when it is set. A worker sized to its memory then sizes everything else around it.
pub fn stated_memory_budget_bytes() -> Option<usize> {
    env("SMELTR_MEMORY_BUDGET_BYTES").map(|_| memory_budget_bytes())
}

/// The memory a worker sizes its deployments to (`worker::memory::fit`), when it is set.
pub fn worker_memory_bytes() -> Option<usize> {
    env("SMELTR_WORKER_MEMORY").map(|_| size_setting("SMELTR_WORKER_MEMORY", 0))
}

/// The ceiling on columnar memory every preview in a leader shares (`runtime::preview_memory_budget`).
pub fn preview_budget_bytes() -> usize {
    size_setting("SMELTR_PREVIEW_BUDGET_BYTES", crate::runtime::PREVIEW_BUDGET_BYTES)
}

/// How long one `Code` block may spend inside `while` loops per pipeline run ([`crate::plan::exec::DEFAULT_LOOP_BUDGET`]).
/// A bare number is milliseconds, as the name says; `2s` or `500ms` name their unit. Anything else refuses to start.
pub fn loop_budget() -> Duration {
    const NAME: &str = "SMELTR_LOOP_BUDGET_MS";
    match env(NAME) {
        None => crate::plan::exec::DEFAULT_LOOP_BUDGET,
        Some(raw) => match raw.trim().parse::<u64>() {
            Ok(millis) => Duration::from_millis(millis),
            Err(_) => parse_duration(&raw)
                .unwrap_or_else(|| refuse(NAME, &raw, "a count of milliseconds or a duration like \"2s\"")),
        },
    }
}

/// Every setting read only when first needed, read now: a bad value refuses to start rather than ending the process
/// the first time something asks for it.
pub fn check_lazy_settings() {
    memory_budget_bytes();
    worker_memory_bytes();
    preview_budget_bytes();
    loop_budget();
    leader_secret_versions();
    leader_lookup_versions();
    leader_ca_validity();
    acme_http_port();
}

/// A duration written the way an operator writes one: `500ms`, `45s`, `30m`, `12h`, `7d`, or bare seconds.
fn parse_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    let (digits, multiplier) = if let Some(digits) = raw.strip_suffix("ms") {
        (digits, 1)
    } else {
        match raw.chars().last() {
            Some('s') => (&raw[..raw.len() - 1], 1_000),
            Some('m') => (&raw[..raw.len() - 1], 60 * 1_000),
            Some('h') => (&raw[..raw.len() - 1], 60 * 60 * 1_000),
            Some('d') => (&raw[..raw.len() - 1], 24 * 60 * 60 * 1_000),
            _ => (raw, 1_000),
        }
    };
    // Checked, because the unit is a multiplication: `4611686018427387905m` parses as a `u64` and then wraps to
    // 60 seconds, which would be accepted as a perfectly ordinary minimum rather than rejected.
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A process runs no role unless it is told to, so the block an operator copies to bring a worker up has to
    /// carry every setting the defaults do not supply. One missing line makes the paste complete and the process
    /// exit having done nothing, which is not a failure the page can show.
    /// What the operator listed stays, in its order, and each loopback entry is there once, however often this runs.
    #[test]
    fn loopback_joins_the_operator_s_no_proxy_list_once() {
        let all = "localhost,127.,127.0.0.0/8,::1,[::1]";
        assert_eq!(with_this_host("", None), all);
        assert_eq!(
            with_this_host(" .internal, ,localhost", None),
            format!(".internal,{all}")
        );
        assert_eq!(
            with_this_host(&with_this_host(".internal", None), None),
            format!(".internal,{all}")
        );
    }

    /// A leader's own address is never proxied, spelled as each client reads it.
    #[test]
    fn a_leader_s_own_address_joins_the_no_proxy_list() {
        let all = "localhost,127.,127.0.0.0/8,::1,[::1]";
        assert_eq!(with_this_host("", "10.0.0.5".parse().ok()), format!("{all},10.0.0.5"));
        assert_eq!(
            with_this_host("", "fd00::5".parse().ok()),
            format!("{all},fd00::5,[fd00::5]")
        );
        assert_eq!(with_this_host("", "::1".parse().ok()), all);
    }

    #[test]
    fn the_pasted_enrollment_block_names_what_a_worker_cannot_start_without() {
        let page = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("gui-frontend/src/AdminPage.svelte"),
        )
        .expect("the Workers page");
        let block = page
            .split_once("const enrollCommand")
            .expect("the block an operator copies")
            .1
            .split_once("});")
            .map_or_else(|| page.clone(), |(before, _)| before.to_string());

        for setting in [
            "SMELTR_WORKER_ENABLED",
            "SMELTR_LEADER_URL",
            "SMELTR_WORKER_ENROLLMENT_CODE",
        ] {
            assert!(block.contains(setting), "the copied block does not set {setting}");
        }
    }

    #[test]
    fn env_var_wins_even_over_a_default_path_that_exists() {
        assert_eq!(
            resolve_config_path(Some("/etc/smeltr/custom.toml"), true, "smeltr.toml"),
            Some(PathBuf::from("/etc/smeltr/custom.toml"))
        );
    }

    #[test]
    fn env_var_wins_even_when_the_default_path_does_not_exist() {
        assert_eq!(
            resolve_config_path(Some("/etc/smeltr/custom.toml"), false, "smeltr.toml"),
            Some(PathBuf::from("/etc/smeltr/custom.toml"))
        );
    }

    #[test]
    fn no_env_var_falls_back_to_the_default_path_only_if_it_exists() {
        assert_eq!(
            resolve_config_path(None, true, "smeltr.toml"),
            Some(PathBuf::from("smeltr.toml"))
        );
    }

    #[test]
    fn no_env_var_and_no_default_path_is_no_config_file_at_all() {
        assert_eq!(resolve_config_path(None, false, "smeltr.toml"), None);
    }

    #[test]
    fn an_unset_env_var_takes_the_files_value() {
        assert!(should_backfill(None));
    }

    #[test]
    fn a_set_env_var_keeps_its_own_value() {
        assert!(!should_backfill(Some("/real/path")));
    }

    #[test]
    fn an_empty_env_var_counts_as_unset_and_takes_the_files_value() {
        assert!(should_backfill(Some("")));
    }

    #[test]
    fn extra_sans_are_trimmed_and_empties_dropped() {
        assert_eq!(
            split_sans("leader.internal, 10.0.0.5 ,,"),
            vec!["leader.internal".to_string(), "10.0.0.5".to_string()]
        );
    }

    #[test]
    fn a_duration_is_read_the_way_an_operator_writes_one() {
        for (written, seconds) in [
            ("45s", 45),
            ("30m", 30 * 60),
            ("12h", 12 * 60 * 60),
            ("7d", 7 * 24 * 60 * 60),
            ("90", 90),
            ("  2h  ", 2 * 60 * 60),
        ] {
            assert_eq!(
                parse_duration(written),
                Some(Duration::from_secs(seconds)),
                "{written:?}"
            );
        }
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
    }

    #[test]
    fn a_duration_that_is_not_one_is_refused_rather_than_guessed() {
        for written in [
            "",
            "s",
            "ms",
            "h",
            "half an hour",
            "-1",
            "1.5h",
            "12 hours",
            "4611686018427387905m",
        ] {
            assert_eq!(parse_duration(written), None, "{written:?}");
        }
    }

    #[test]
    fn an_unset_authority_validity_is_ten_years() {
        assert_eq!(bounded_ca_validity(None), Duration::from_secs(3650 * 24 * 60 * 60));
    }

    /// A number outside the bounds is brought inside them rather than refused: an operator who wrote one is better
    /// served by an authority that works than by a leader that will not start.
    #[test]
    fn an_authority_validity_outside_the_bounds_is_brought_inside_them() {
        assert_eq!(
            bounded_ca_validity(Some(Duration::from_secs(1))),
            MIN_LEADER_CA_VALIDITY
        );
        assert_eq!(
            bounded_ca_validity(Some(Duration::from_secs(u64::MAX))),
            MAX_LEADER_CA_VALIDITY
        );
        let inside = Duration::from_secs(90 * 24 * 60 * 60);
        assert_eq!(bounded_ca_validity(Some(inside)), inside);
    }

    /// The example file documents every default above, and the pair drifted while those values sat at their call sites.
    /// Built from the constants, so changing one without the other fails here.
    #[test]
    fn the_example_file_shows_the_defaults_this_module_applies() {
        let example = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/smeltr.example.toml"))
            .expect("read smeltr.example.toml")
            // A Windows checkout may end its lines with CRLF.
            .replace("\r\n", "\n");
        for shown in [
            format!("port = {DEFAULT_GUI_PORT}"),
            format!("bind = {DEFAULT_GUI_BIND:?}"),
            format!("data_dir = {DEFAULT_LEADER_DATA_DIR:?}"),
            format!("key = {DEFAULT_LEADER_SECRET_KEY:?}"),
            format!("versions = {DEFAULT_LEADER_SECRET_VERSIONS}"),
            format!("[leader.lookups]\n# versions = {DEFAULT_LEADER_LOOKUP_VERSIONS}"),
            format!("ca_validity = {DEFAULT_LEADER_CA_VALIDITY:?}"),
            format!("acme_http_port = {DEFAULT_ACME_HTTP_PORT}"),
            format!("state_dir = {DEFAULT_WORKER_STATE_DIR:?}"),
            format!("interval_ms = {DEFAULT_WORKER_HEARTBEAT_INTERVAL_MS}"),
            "[worker.metrics_endpoint]\n# enabled = false".to_string(),
            format!("port = {DEFAULT_WORKER_METRICS_ENDPOINT_PORT}"),
            "thread_limit = 0 ".to_string(),
            "stop_after_unreachable_secs = -1 ".to_string(),
        ] {
            assert!(
                example.contains(&shown),
                "smeltr.example.toml no longer shows {shown:?}"
            );
        }
    }

    #[test]
    fn minus_one_never_stops_a_worker_and_other_negatives_are_refused() {
        assert_eq!(stop_after_unreachable_secs("-1"), Some(None));
        assert_eq!(stop_after_unreachable_secs(" 0 "), Some(Some(0)));
        assert_eq!(stop_after_unreachable_secs("300"), Some(Some(300)));
        assert_eq!(stop_after_unreachable_secs("-2"), None);
        assert_eq!(stop_after_unreachable_secs("5m"), None);
    }

    #[test]
    fn every_accepted_spelling_of_a_boolean_reads_as_itself() {
        for yes in ["1", "on", "true", "yes", "y", "t"] {
            assert_eq!(parse_bool(yes), Some(true), "{yes:?}");
        }
        for no in ["0", "off", "false", "no", "n", "f"] {
            assert_eq!(parse_bool(no), Some(false), "{no:?}");
        }
    }

    /// An operator types what their shell or compose file hands over, which is neither trimmed nor cased.
    #[test]
    fn a_boolean_is_read_whatever_its_case_and_surrounding_space() {
        assert_eq!(parse_bool("  TRUE "), Some(true));
        assert_eq!(parse_bool("\tOff\n"), Some(false));
        assert_eq!(parse_bool("YeS"), Some(true));
    }

    /// The point of the whole exercise: a value that is not a boolean is not quietly one.
    #[test]
    fn anything_else_is_not_a_boolean() {
        for other in ["", " ", "2", "-1", "yes please", "enabled", "null", "tt"] {
            assert_eq!(parse_bool(other), None, "{other:?}");
        }
    }

    /// Moving either default so the key sits under the data directory would leave the sealing decorative, and
    /// nothing else would notice.
    #[test]
    fn the_default_secret_key_lives_outside_the_default_data_dir() {
        assert!(!Path::new(DEFAULT_LEADER_SECRET_KEY).starts_with(DEFAULT_LEADER_DATA_DIR));
    }

    /// The bind default has to survive `resolve_listen_addr`'s check that a non-loopback address carries an admin
    /// token; an unconfigured process has none.
    #[test]
    fn the_default_bind_is_loopback() {
        let ip: std::net::IpAddr = DEFAULT_GUI_BIND.parse().expect("a valid address");
        assert!(ip.is_loopback());
    }

    /// Writing out the address of the leader you are already running is the same leader. Read as elsewhere, the
    /// certificate check that keeps this process's own worker able to reach it is skipped.
    #[test]
    fn a_loopback_url_on_this_leader_s_port_is_this_leader() {
        assert!(names_this_leader("https://127.0.0.1:8443", 8443, &[]));
        assert!(names_this_leader("https://localhost:8443/", 8443, &[]));
        assert!(names_this_leader("https://[::1]:8443", 8443, &[]));
        assert!(names_this_leader("https://127.0.0.1", 443, &[]));
        assert!(names_this_leader("http://localhost", 80, &[]));
    }

    /// A name this leader's certificate is issued for, on its port, is this leader under its public name.
    #[test]
    fn a_name_this_leader_s_certificate_serves_on_its_port_is_this_leader() {
        let names = leader_certificate_names(&["smeltr.example".to_string()]);
        assert!(names_this_leader("https://smeltr.example:8443", 8443, &names));
        assert!(names_this_leader("https://SMELTR.example:8443/", 8443, &names));
        assert!(!names_this_leader("https://smeltr.example", 8443, &names));
    }

    /// Another port is another process, and a name that is neither loopback nor this leader's own is another machine
    /// as far as this one can tell. Both are a leader whose certificate this one has no say over.
    #[test]
    fn a_url_naming_anywhere_else_is_another_leader() {
        let names = leader_certificate_names(&["smeltr.example".to_string()]);
        assert!(!names_this_leader("https://127.0.0.1:9443", 8443, &names));
        assert!(!names_this_leader("https://other.example:8443", 8443, &names));
        assert!(!names_this_leader("https://10.0.0.4:8443", 8443, &names));
        assert!(!names_this_leader("not a url", 8443, &names));
    }

    #[test]
    fn the_host_is_what_a_certificate_has_to_serve() {
        assert_eq!(
            leader_url_host("https://localhost:8443/x"),
            Some("localhost".to_string())
        );
        assert_eq!(leader_url_host("https://[::1]:8443"), Some("::1".to_string()));
        assert_eq!(
            leader_url_host("https://smeltr.example"),
            Some("smeltr.example".to_string())
        );
        assert_eq!(leader_url_host("https://"), None);
    }
}
