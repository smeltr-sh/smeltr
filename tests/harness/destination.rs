//! What the destination fuzzers share: the options a seed reached and when they count, the save's verdict on a drawn
//! bundle, a PKI for a service in this process, delivering to a built destination, and the seed driver that credits
//! coverage only once a seed's checks pass.
//!
//! Each fuzzer keeps its own draw, its breaks, the check that what loaded is what was drawn, its fake service and its
//! live half.

use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use smeltr::config_file::{FileConfig, load_str, validation_errors};
use smeltr::io::{Destination, Dispatched, OwnedRecord, Rejection};

use crate::certs;
use crate::harness::fuzz::{self, Coverage, Rng};

/// What one seed reached, credited to the fuzzer's [`Coverage`] only once the seed's checks pass. A broken or
/// mangled draw is checked only for its refusal, so it credits only its break: the options it drew were never
/// loaded, and the break may have taken them out again.
#[derive(Default)]
pub struct Reached {
    names: Vec<String>,
    pub only_breaks: bool,
}

impl Reached {
    pub fn reach(&mut self, name: impl Into<String>) {
        self.names.push(name.into());
    }

    pub fn reached(&self, name: &str) -> bool {
        self.names.iter().any(|n| n == name)
    }

    /// Keeps only `names`: a seed that stops short of delivering checked nothing else it drew.
    pub fn keep_only(&mut self, names: &[&str]) {
        self.names.retain(|n| names.contains(&n.as_str()));
    }

    /// Drops `names`: a seed that stops short of building checked none of them.
    pub fn forget(&mut self, names: &[&str]) {
        self.forget_where(|n| names.contains(&n));
    }

    /// Drops every name `unchecked` says so of.
    pub fn forget_where(&mut self, unchecked: impl Fn(&str) -> bool) {
        self.names.retain(|n| !unchecked(n));
    }

    pub fn credit(self, cov: &mut Coverage) {
        for name in self.names {
            if !self.only_breaks || name.starts_with("broken.") || name == "mangled" {
                cov.reach(name);
            }
        }
    }
}

/// The zones a drawn destination names addresses for. None of them leads anywhere but the drawn `url`.
const ZONES: &[&str] = &["us-east-1a", "us-east-1b", "eu-west-1c"];

/// An address in a zone the worker is not in. https://, so an identity the destination names stays valid on it.
const ELSEWHERE: &str = "https://elsewhere.example.test:9200";

/// Draws `zoneUrls` onto `d`, a destination whose drawn `url` is where rows must go, and returns the zone of the
/// worker that runs it. Either `url` moves to the worker's own zone and `url` becomes an address nothing should
/// reach, or the worker's zone has no address (or the worker has no zone) and `url` stays.
pub fn draw_zones(
    rng: &mut Rng,
    cov: &mut Reached,
    d: &mut serde_json::Map<String, serde_json::Value>,
) -> Option<&'static str> {
    if !rng.chance(3) {
        return None;
    }
    let own = *rng.pick(ZONES);
    let mut zone_urls = serde_json::Map::new();
    for zone in ZONES.iter().filter(|z| **z != own && rng.chance(2)) {
        zone_urls.insert(zone.to_string(), serde_json::json!(ELSEWHERE));
    }
    let zone = match rng.below(3) {
        0 => {
            cov.reach("zoneUrls.own");
            zone_urls.insert(own.to_string(), d["url"].clone());
            d.insert("url".into(), serde_json::json!(ELSEWHERE));
            Some(own)
        }
        1 => {
            cov.reach("zoneUrls.other");
            zone_urls
                .entry(ZONES.iter().find(|z| **z != own).unwrap().to_string())
                .or_insert(serde_json::json!(ELSEWHERE));
            Some(own)
        }
        _ => {
            cov.reach("zoneUrls.noZone");
            zone_urls.entry(own.to_string()).or_insert(serde_json::json!(ELSEWHERE));
            None
        }
    };
    d.insert("zoneUrls".into(), serde_json::Value::Object(zone_urls));
    zone
}

/// Breaks `d`'s `zoneUrls`: a zone with no address, or an address with no zone. The save refuses it on `zoneUrls`.
pub fn break_zones(rng: &mut Rng, d: &mut serde_json::Map<String, serde_json::Value>) -> &'static str {
    let (zone, url) = *rng.pick(&[
        ("us-east-1a", ""),
        ("us-east-1a", "  "),
        ("", ELSEWHERE),
        (" ", ELSEWHERE),
    ]);
    d.insert("zoneUrls".into(), serde_json::json!({ zone: url }));
    "zoneUrls"
}

/// The `zoneUrls` drawn onto `d`, as a loaded destination holds them.
pub fn drawn_zones(d: &serde_json::Map<String, serde_json::Value>) -> std::collections::BTreeMap<String, String> {
    d.get("zoneUrls")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flatten()
        .map(|(zone, url)| (zone.clone(), url.as_str().unwrap().to_string()))
        .collect()
}

/// Runs each of `seeds` with a fresh [`Reached`], credits what a passing seed reached, and fails naming every seed
/// that did not pass or any of `options` no seed reached.
pub fn run(
    seeds: Range<u64>,
    options: impl IntoIterator<Item = &'static str>,
    mut seed: impl FnMut(u64, &mut Reached) -> Result<(), String>,
) {
    let mut cov = Coverage::new(options);
    fuzz::run(seeds, |n| {
        let mut reached = Reached::default();
        let checked = seed(n, &mut reached);
        if checked.is_ok() {
            reached.credit(&mut cov);
        }
        checked
    });
    cov.assert_complete();
}

// -- what the save says -------------------------------------------------------------------------------------------

/// Every refusal of the save, as the field it names and its message. `Err` when the JSON is no bundle.
pub fn refusals(json: &str) -> Result<Vec<(Option<&'static str>, String)>, String> {
    let file: FileConfig = serde_json::from_str(json).map_err(|e| e.to_string())?;
    Ok(validation_errors(file)
        .into_iter()
        .map(|e| (e.field, e.message))
        .collect())
}

/// Whether a bundle carrying a key the save does not know got past it: accepted outright, or loaded.
pub fn unknown_key_taken(json: &str) -> bool {
    refusals(json).is_ok_and(|r| r.is_empty()) || load_str(json).is_ok()
}

/// Whether `json` loads.
pub fn loads(json: &str) -> bool {
    load_str(json).is_ok()
}

/// Whether `json` loads and its topology compiles: a break some destinations show only there.
pub fn compiles(json: &str) -> bool {
    load_str(json).is_ok_and(|s| s.compile_topology().is_ok())
}

/// How the save must refuse a broken draw.
#[derive(Clone, Copy, Debug)]
pub enum Broken {
    /// On this field, or on no one field.
    Field(Option<&'static str>),
    /// As JSON that is no bundle at all.
    Unread,
}

/// The credentials `topology`'s connectors name, by where each lives: what a worker asks for on their behalf. An
/// identity's key is left out, being the identity's rather than the connector's.
pub fn connector_secrets(topology: &smeltr::plan::topology::TopologyConfig) -> Vec<smeltr::plan::topology::SecretRef> {
    (topology.named_secrets.iter())
        .filter(|n| !n.what.contains(": the identity "))
        .map(|n| n.reference.clone())
        .collect()
}

/// Checks the save: a draw broken as `broken` says is refused so and does not pass `loads`, and an unbroken one is
/// accepted, loads with `loaded`, loads the same once saved, and saves the same twice.
pub fn check_save<T: Debug>(
    json: &str,
    broken: Option<Broken>,
    loads: fn(&str) -> bool,
    loaded: impl Fn(&str) -> Result<T, String>,
) -> Result<Option<T>, String> {
    let refused = refusals(json);
    match broken {
        Some(Broken::Unread) => {
            if refused.is_ok() || load_str(json).is_ok() {
                return Err(format!("broken past reading, but read: {refused:?}"));
            }
            return Ok(None);
        }
        Some(Broken::Field(field)) => {
            let refused = refused?;
            if !refused.iter().any(|(at, _)| *at == field) {
                return Err(format!("broken on {field:?}, but refused {refused:?}"));
            }
            if loads(json) {
                return Err("broken, but loads".to_string());
            }
            return Ok(None);
        }
        None => {}
    }
    let refused = refused?;
    if !refused.is_empty() {
        return Err(format!("refused: {refused:?}"));
    }
    let config = loaded(json)?;
    let file: FileConfig = serde_json::from_str(json).unwrap();
    let saved = serde_json::to_string(&file).map_err(|e| format!("does not save: {e}"))?;
    let again = loaded(&saved).map_err(|e| format!("saved, does not load: {e}\n{saved}"))?;
    if format!("{config:?}") != format!("{again:?}") {
        return Err(format!("saved, loads differently:\n  {config:?}\n  {again:?}\n{saved}"));
    }
    let resaved = serde_json::to_string(&serde_json::from_str::<FileConfig>(&saved).unwrap()).unwrap();
    if resaved != saved {
        return Err(format!("a second save differs:\n  {saved}\n  {resaved}"));
    }
    Ok(Some(config))
}

// -- the certificates an identity names ---------------------------------------------------------------------------

/// One authority, the certificate it issued the service in this process, and a client's under it.
pub struct Pki {
    pub authority: certs::Authority,
    pub served: certs::Issued,
    pub client: certs::Issued,
    server_only: Arc<rustls::ServerConfig>,
    mutual: Arc<rustls::ServerConfig>,
}

impl Pki {
    /// Mints the authority as `authority`, the service's certificate as `served` for `names`, and the client's.
    pub fn new(authority: &str, served: &str, names: &[&str]) -> Pki {
        use rustls::pki_types::pem::PemObject;
        certs::install_provider();
        let authority = certs::Authority::new(authority);
        let served = authority.issue(served, names.iter().map(|n| n.to_string()).collect());
        let client = authority.issue("smeltr-fuzz", vec!["smeltr".to_string()]);
        let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(served.key.as_bytes()).expect("a PEM key");
        let server_only =
            rustls::ServerConfig::builder_with_provider(rustls::crypto::aws_lc_rs::default_provider().into())
                .with_safe_default_protocol_versions()
                .expect("default protocol versions")
                .with_no_client_auth()
                .with_single_cert(certs::certificates(&served.certificate), key)
                .expect("the key belongs to the certificate");
        Pki {
            mutual: certs::server_config(&served, &authority),
            server_only: Arc::new(server_only),
            authority,
            served,
            client,
        }
    }

    /// The service presenting its certificate and asking the client for none.
    pub fn server_only(&self) -> Arc<rustls::ServerConfig> {
        self.server_only.clone()
    }

    /// The service presenting its certificate and demanding one under the authority.
    pub fn mutual(&self) -> Arc<rustls::ServerConfig> {
        self.mutual.clone()
    }
}

// -- delivering ---------------------------------------------------------------------------------------------------

/// Hands `batches` to `d` one after another, flushes, waits for every one, and answers what each refused, by batch.
pub fn write(
    d: &mut dyn Destination,
    batches: &[Vec<OwnedRecord>],
    deadline: Duration,
) -> Result<Vec<Vec<Rejection>>, String> {
    let mut refused: Vec<Vec<Rejection>> = (0..batches.len()).map(|_| Vec::new()).collect();
    let mut pending = Vec::new();
    for (batch, rows) in batches.iter().enumerate() {
        let (dispatched, now) = d
            .deliver(batch as u64, Cow::Owned(rows.clone()))
            .map_err(|e| format!("batch {batch} not delivered: {e}"))?;
        refused[batch].extend(now);
        if dispatched == Dispatched::InFlight {
            pending.push(batch as u64);
        }
    }
    d.flush().map_err(|e| format!("not flushed: {e}"))?;
    let until = Instant::now() + deadline;
    while !pending.is_empty() {
        for done in d.poll_completions().map_err(|e| format!("not landed: {e}"))? {
            refused[done.batch as usize].extend(done.rejections);
            pending.retain(|b| *b != done.batch);
        }
        if Instant::now() > until {
            return Err(format!("batches {pending:?} never completed"));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(refused)
}

/// Checks that each batch refused exactly the rows `is_refused` says it had to.
pub fn check_refused<S>(
    sent: &[Vec<S>],
    refused: &[Vec<Rejection>],
    is_refused: impl Fn(&S) -> bool,
) -> Result<(), String> {
    for (batch, rows) in sent.iter().enumerate() {
        let want: Vec<usize> = (rows.iter().enumerate())
            .filter(|(_, s)| is_refused(s))
            .map(|(i, _)| i)
            .collect();
        let mut got: Vec<usize> = refused[batch].iter().map(|r| r.doc).collect();
        got.sort();
        if got != want {
            return Err(format!("batch {batch}: refused rows {got:?}, not {want:?}"));
        }
    }
    Ok(())
}

/// One request's line and headers.
pub struct Head {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
}

impl Head {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Reads one request off `s`, keeping what follows it in `buf`. `None` once the client hangs up.
pub fn read_request(s: &mut impl std::io::Read, buf: &mut Vec<u8>) -> Option<(Head, Vec<u8>)> {
    let mut chunk = [0u8; 16 * 1024];
    let end = loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
        let n = s.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let text = String::from_utf8_lossy(&buf[..end]).to_string();
    let mut lines = text.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let (method, path) = (first.next()?.to_string(), first.next()?.to_string());
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_string(), v.trim().to_string()))
        .collect();
    let head = Head { method, path, headers };
    let len: usize = head.header("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    while buf.len() < end + 4 + len {
        let n = s.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf[end + 4..end + 4 + len].to_vec();
    buf.drain(..end + 4 + len);
    Some((head, body))
}

/// One change to `d` that no generator rule describes: a value of another type, an unknown key, a key dropped. The
/// save may take or refuse it, and must not panic. `true` when it must refuse it: one of `unknown`, a key the kind
/// does not take.
pub fn mangle(
    rng: &mut Rng,
    cov: &mut Reached,
    d: &mut serde_json::Map<String, serde_json::Value>,
    unknown: &[&str],
) -> bool {
    use serde_json::json;
    cov.reach("mangled");
    let keys: Vec<String> = d.keys().cloned().collect();
    let key = rng.pick(&keys).clone();
    match rng.below(4) {
        0 => {
            let junk = rng
                .pick(&[
                    json!(null),
                    json!(-1),
                    json!(1.5),
                    json!(""),
                    json!([]),
                    json!({}),
                    json!(true),
                ])
                .clone();
            d.insert(key, junk);
        }
        1 => {
            d.remove(&key);
        }
        2 => {
            d.insert(rng.pick(unknown).to_string(), json!(1));
            return true;
        }
        _ => {
            d.insert(key, json!(u64::MAX));
        }
    }
    false
}

/// The TLS a drawn destination sets on itself.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ClientTls {
    /// An authority to verify the far end against.
    Trusts,
    /// That, and a certificate to present.
    Presents,
}

/// Stands for the authority's PEM until a fuzzer's bundle puts its PKI's in.
pub const AUTHORITIES: &str = "@authorities";

/// Sets TLS on `d`: the authority as [`AUTHORITIES`], and the certificate `client` to present when `tls` says so.
pub fn set_tls(d: &mut serde_json::Map<String, serde_json::Value>, tls: ClientTls) {
    d.insert("trustedAuthorities".into(), serde_json::json!(AUTHORITIES));
    if tls == ClientTls::Presents {
        d.insert("certificate".into(), serde_json::json!("client"));
    }
}
