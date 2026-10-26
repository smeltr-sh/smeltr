//! The Log Analytics destination's fuzzers (`type: "logAnalytics"`, `io::http_post` over `io::http`, signed in by
//! `io::entra`). Each seed draws one destination as a bundle names it, every option at random, and sometimes breaks
//! one of its settings.
//!
//! Offline, a destination the save accepts loads as its bundle says, loads the same after a save, builds when this
//! worker holds every credential it names and refuses to when it does not, and, signed in as a service principal,
//! delivers drawn batches to a Logs Ingestion API on this host, signing in at a token endpoint beside it. Every token
//! request is checked for the principal, its secret and Azure Monitor's scope, and every ingestion request for its
//! target, its body's type and encoding, a token the endpoint issued, and its body, decompressed, item for item. The
//! service answers each ingestion request at random as Azure Monitor does: a 204 lands every row the request carried, a
//! 429 or a 5xx is sent again whole, a 400 or a 413 is split until the row at fault is alone, and a 401, 403 or 404
//! stalls the destination naming the status. Every row lands exactly once or comes back refused. One the save refuses
//! is refused on the setting that was broken. Any other change to its JSON may be refused, and never panics.
//!
//! Live, the same draws, pointed at a real data collection rule and signed in as a real service principal
//! (`just check-log-analytics`), land in its Log Analytics table and are queried back.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};
use smeltr::config_file::load_str;
use smeltr::io::{Destination, IoError, OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{
    Compression, DestinationKind, EntraSignIn, HeldSecrets, LogAnalyticsDestinationConfig, RequestBody, SecretRef,
};

use crate::harness::destination::{self, Broken, Pki, Reached, connector_secrets, unknown_key_taken};
use crate::harness::fuzz::Rng;
use crate::harness::http::{self, Answer, Answers, Id, Seen, Sent, Service, fnv, gunzip, json_array_rows};
use crate::live;

const SEEDS: u64 = 300;
const LIVE_SEEDS: u64 = 6;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// A worker-local credential no test sets, so a destination naming it cannot build.
const UNSET_VARIABLE: &str = "SMELTR_FUZZ_NEVER_SET";
/// A worker-local credential every test process has: cargo sets it to the package's name.
const SET_VARIABLE: &str = "CARGO_PKG_NAME";

/// What a principal's secret may hold: what Entra issues, and what a form must encode.
const SECRET_VALUES: &[&str] = &["Abc8Q~x.y_z-123", "s3cr&t=1+2", "with space", "ünï"];

/// The streams a rule may name: a custom table's, a built-in table's, and one a path must encode.
const STREAMS: &[&str] = &[
    "Custom-Events_CL",
    "Custom-firewall_CL",
    "Microsoft-Syslog",
    "Custom-a b/c",
];

const TENANT: &str = "72f988bf-86f1-41af-91ab-2d7cd011db47";

/// Every option the generator knows. Each must be drawn into a destination the save accepts and that delivers to a
/// service checking every request, or into the refusal it names, over the seed range.
const OPTIONS: &[&str] = &[
    "endpoint.http",
    "endpoint.https",
    "endpoint.localhost",
    "endpoint.trailingSlash",
    "endpoint.padded",
    "ruleId.upperHex",
    "stream.encoded",
    "signIn.servicePrincipal",
    "signIn.authEndpointSlash",
    "signIn.managedIdentity",
    "signIn.managedIdentityClientId",
    "secret.leaderStored",
    "secret.workerLocal",
    "secret.workerLocalUnset",
    "tls.authorities",
    "inFlight",
    "maxRequestBytes",
    "bufferBytes",
    "stallAfterSecs",
    "batches.several",
    "requests.several",
    "row.padded",
    "row.unfit",
    "token.renewed",
    "answer.204",
    "answer.retry",
    "answer.retryAfter",
    "answer.split",
    "answer.stall",
    "broken.endpoint.blank",
    "broken.endpoint.path",
    "broken.ruleId",
    "broken.stream",
    "broken.signIn.partial",
    "broken.signIn.twoCredentials",
    "broken.signIn.authEndpoint",
    "broken.signIn.managedIdentityClientId",
    "broken.certificate",
    "broken.inFlight",
    "broken.maxRequestBytes",
    "broken.bufferBytes",
    "broken.stallAfterSecs",
    "mangled",
];

// -- certificates -----------------------------------------------------------------------------------------------

/// One authority and the service's certificate under it, minted once for every seed.
fn pki() -> &'static Pki {
    static PKI: OnceLock<Pki> = OnceLock::new();
    PKI.get_or_init(|| Pki::new("smeltr log analytics fuzz", "localhost", &["localhost", "127.0.0.1"]))
}

// -- drawing a destination --------------------------------------------------------------------------------------

/// Where a draw is pointed.
#[derive(Clone, Copy)]
enum Target<'a> {
    /// The services this seed starts on this host, at these ports: the ingestion endpoint, and the token endpoint.
    Offline(u16, u16),
    /// A real data collection rule, signed in to as a real service principal.
    Live(&'a live::LogAnalytics),
}

/// One drawn destination.
struct Draw {
    destination: Map<String, Value>,
    /// Whether the destination trusts the service's authority.
    tls: bool,
    /// The setting broken, if one was: the field the save must refuse it on.
    broken: Option<&'static str>,
}

/// A credential: a value the leader holds, or a variable on the worker, set or not.
fn secret(rng: &mut Rng, cov: &mut Reached, live: bool) -> Value {
    if !live && rng.chance(6) {
        if rng.chance(2) {
            cov.reach("secret.workerLocalUnset");
            return json!(UNSET_VARIABLE);
        }
        cov.reach("secret.workerLocal");
        return json!(SET_VARIABLE);
    }
    cov.reach("secret.leaderStored");
    json!({"secret": "principal", "version": 1 + rng.below(3)})
}

fn draw(rng: &mut Rng, cov: &mut Reached, target: Target, stored: &mut String) -> Draw {
    let mut d = Map::new();
    d.insert("id".into(), json!("logAnalytics"));
    d.insert("type".into(), json!("logAnalytics"));
    let live = matches!(target, Target::Live(_));

    let https = !live && rng.chance(4);
    if https {
        cov.reach("tls.authorities");
        d.insert("authorities".into(), json!(AUTHORITIES));
    }
    let (endpoint, rule, stream) = match target {
        Target::Live(s) => (s.endpoint.clone(), s.rule_id.clone(), s.stream.clone()),
        Target::Offline(port, _) => {
            let host = if https || rng.chance(3) {
                cov.reach("endpoint.localhost");
                "localhost"
            } else {
                "127.0.0.1"
            };
            let scheme = if https { "https" } else { "http" };
            cov.reach(format!("endpoint.{scheme}"));
            let mut endpoint = format!("{scheme}://{host}:{port}");
            if rng.chance(4) {
                cov.reach("endpoint.trailingSlash");
                endpoint.push('/');
            }
            if rng.chance(8) {
                cov.reach("endpoint.padded");
                endpoint = format!(" {endpoint}\t");
            }
            let hex: String = (0..32).map(|_| *rng.pick(&['0', '7', 'a', 'f'])).collect();
            let rule = if rng.chance(4) {
                cov.reach("ruleId.upperHex");
                format!("dcr-{}", hex.to_ascii_uppercase())
            } else {
                format!("dcr-{hex}")
            };
            let stream = rng.pick(STREAMS).to_string();
            if stream.contains(' ') {
                cov.reach("stream.encoded");
            }
            (endpoint, rule, stream)
        }
    };
    d.insert("endpoint".into(), json!(endpoint));
    d.insert("ruleId".into(), json!(rule));
    d.insert("stream".into(), json!(stream));

    match target {
        Target::Live(s) => {
            *stored = s.client_secret.clone();
            cov.reach("signIn.servicePrincipal");
            d.insert("tenantId".into(), json!(s.tenant_id));
            d.insert("clientId".into(), json!(s.client_id));
            d.insert("clientSecret".into(), secret(rng, cov, live));
        }
        Target::Offline(..) if rng.chance(5) => {
            cov.reach("signIn.managedIdentity");
            if rng.chance(2) {
                cov.reach("signIn.managedIdentityClientId");
                d.insert(
                    "managedIdentityClientId".into(),
                    json!("11112222-3333-4444-5555-666677778888"),
                );
            }
        }
        Target::Offline(_, token_port) => {
            cov.reach("signIn.servicePrincipal");
            *stored = rng.pick(SECRET_VALUES).to_string();
            d.insert("tenantId".into(), json!(TENANT));
            d.insert("clientId".into(), json!("smeltr-app"));
            d.insert("clientSecret".into(), secret(rng, cov, live));
            // A principal is sent to the service's token endpoint by authEndpoint.
            let mut auth = format!("http://127.0.0.1:{token_port}");
            if rng.chance(3) {
                cov.reach("signIn.authEndpointSlash");
                auth.push('/');
            }
            d.insert("authEndpoint".into(), json!(auth));
        }
    }

    draw_limits(rng, cov, &mut d);
    if rng.chance(2) {
        cov.reach("stallAfterSecs");
        let low = if live { 30 } else { 1 };
        d.insert("stallAfterSecs".into(), json!(rng.pick(&[low, 5, 30, 120])));
    }
    let mut draw = Draw {
        destination: d,
        tls: https,
        broken: None,
    };
    if !live && rng.chance(3) {
        draw.broken = Some(break_one(rng, cov, &mut draw));
    }
    draw
}

/// Request size, buffer and senders, together, so the senders the buffer affords are never fewer than the start.
fn draw_limits(rng: &mut Rng, cov: &mut Reached, d: &mut Map<String, Value>) {
    let request = if rng.chance(2) {
        cov.reach("maxRequestBytes");
        let request = *rng.pick(&[4 * KIB, 16 * KIB, 256 * KIB, MIB]);
        d.insert("maxRequestBytes".into(), json!(request));
        request
    } else {
        MIB
    };
    let buffer = if rng.chance(2) {
        cov.reach("bufferBytes");
        let buffer = *rng.pick(&[4 * MIB, 16 * MIB, 64 * MIB]);
        d.insert("bufferBytes".into(), json!(buffer));
        buffer
    } else {
        64 * MIB
    };
    let affords = (buffer / request).min(64);
    if rng.chance(2) {
        cov.reach("inFlight");
        let start = *rng.pick(&[1, 2, 4, 16]);
        d.insert("inFlight".into(), json!(start.min(affords)));
    }
}

/// Breaks one setting of `draw`, and answers the field the save must refuse it on.
fn break_one(rng: &mut Rng, cov: &mut Reached, draw: &mut Draw) -> &'static str {
    let d = &mut draw.destination;
    let principal = d.contains_key("clientSecret");
    let mut breaks = vec![
        "endpoint.blank",
        "endpoint.path",
        "ruleId",
        "stream",
        "signIn.partial",
        "signIn.twoCredentials",
        "signIn.managedIdentityClientId",
        "certificate",
        "inFlight",
        "maxRequestBytes",
        "bufferBytes",
        "stallAfterSecs",
    ];
    if principal {
        breaks.push("signIn.authEndpoint");
    }
    let what = *rng.pick(&breaks);
    cov.reach(format!("broken.{what}"));
    match what {
        "endpoint.blank" => {
            d.insert("endpoint".into(), json!(rng.pick(&["", "  "])));
            "endpoint"
        }
        "endpoint.path" => {
            let endpoint = d["endpoint"].as_str().unwrap().trim().trim_end_matches('/').to_string();
            let path = rng.pick(&["/dataCollectionRules/dcr-1", "/x", "/?api-version=1"]);
            d.insert("endpoint".into(), json!(format!("{endpoint}{path}")));
            "endpoint"
        }
        "ruleId" => {
            d.insert(
                "ruleId".into(),
                json!(rng.pick(&[
                    "",
                    "dcr-123",
                    "my-rule",
                    "dcr-0000000000000000000000000000000g",
                    "dcr-00000000000000000000000000000000 0"
                ])),
            );
            "ruleId"
        }
        "stream" => {
            d.insert("stream".into(), json!(rng.pick(&["", " "])));
            "stream"
        }
        "signIn.partial" => {
            let (keep, field) = *rng.pick(&[
                ("tenantId", "clientId"),
                ("clientId", "tenantId"),
                ("clientSecret", "tenantId"),
            ]);
            for key in [
                "tenantId",
                "clientId",
                "clientSecret",
                "authEndpoint",
                "managedIdentityClientId",
            ] {
                d.remove(key);
            }
            let value = match keep {
                "clientSecret" => json!({"secret": "principal", "version": 1}),
                _ => json!("x"),
            };
            d.insert(keep.into(), value);
            field
        }
        "signIn.twoCredentials" => {
            if !principal {
                d.insert("tenantId".into(), json!(TENANT));
                d.insert("clientId".into(), json!("app"));
                d.insert("clientSecret".into(), json!({"secret": "principal", "version": 1}));
            }
            d.insert(
                "managedIdentityClientId".into(),
                json!("11112222-3333-4444-5555-666677778888"),
            );
            "managedIdentityClientId"
        }
        "signIn.authEndpoint" => {
            d.insert(
                "authEndpoint".into(),
                json!(rng.pick(&["https://login.microsoftonline.us/tenant", " "])),
            );
            "authEndpoint"
        }
        "signIn.managedIdentityClientId" => {
            for key in ["tenantId", "clientId", "clientSecret", "authEndpoint"] {
                d.remove(key);
            }
            d.insert("managedIdentityClientId".into(), json!(rng.pick(&["my-identity", " "])));
            "managedIdentityClientId"
        }
        "certificate" => {
            d.insert("certificate".into(), json!(" "));
            "certificate"
        }
        "inFlight" => {
            d.insert("inFlight".into(), json!(rng.pick(&[0, 65])));
            "inFlight"
        }
        "maxRequestBytes" => {
            d.insert(
                "maxRequestBytes".into(),
                json!(rng.pick(&[0, 4 * KIB - 1, MIB + 1, 5 * MIB])),
            );
            "maxRequestBytes"
        }
        "bufferBytes" => {
            d.insert("bufferBytes".into(), json!(rng.pick(&[0, 4 * MIB - 1, 1024 * MIB + 1])));
            "bufferBytes"
        }
        "stallAfterSecs" => {
            d.insert("stallAfterSecs".into(), json!(rng.pick(&[0, 121, 3600])));
            "stallAfterSecs"
        }
        other => unreachable!("{other}"),
    }
}

/// Stands for the authority's PEM until `bundle` has it.
const AUTHORITIES: &str = "@authorities";

/// `draw` as a whole bundle: a Kafka source, one route to the destination, and the authority it trusts.
fn bundle(draw: &Draw) -> Value {
    let mut b = json!({
        "version": 1,
        "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
        "destinations": [Value::Object(draw.destination.clone())],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["logAnalytics"]}]}],
    });
    if let Some(authorities) = b["destinations"][0].get_mut("authorities") {
        *authorities = json!(pki().authority.pem);
    }
    b
}

/// What the worker holds: the principal's secret at the version the destination names. `true` beside it when the
/// destination names a credential this worker does not hold: a variable no worker sets.
fn held(draw: &Draw, stored: &str) -> (HeldSecrets, bool) {
    let mut secrets = HeldSecrets::default();
    match draw.destination.get("clientSecret") {
        Some(Value::Object(o)) => secrets.set_stored([(
            o["secret"].as_str().unwrap().to_string(),
            o["version"].as_i64().unwrap(),
            stored.to_string(),
        )]),
        Some(Value::String(s)) => return (secrets, s == UNSET_VARIABLE),
        _ => {}
    }
    (secrets, false)
}

/// The secret the principal signs in with, as this worker holds it.
fn secret_value(draw: &Draw, stored: &str) -> Option<String> {
    match draw.destination.get("clientSecret")? {
        Value::String(var) => std::env::var(var).ok(),
        _ => Some(stored.to_string()),
    }
}

// -- what the save says ------------------------------------------------------------------------------------------

/// The destination `json` loads as.
fn loaded(json: &str) -> Result<(LogAnalyticsDestinationConfig, Vec<SecretRef>), String> {
    let system = load_str(json).map_err(|e| format!("{e:?}"))?;
    match &system.topology.destinations[0].kind {
        DestinationKind::LogAnalytics(c) => Ok((c.clone(), connector_secrets(&system.topology))),
        other => Err(format!("loaded as {other:?}")),
    }
}

/// Checks the save: a broken draw is refused on its field, and an unbroken one is accepted, loads as drawn, and
/// loads the same once saved.
fn check_save(draw: &Draw, json: &str) -> Result<Option<LogAnalyticsDestinationConfig>, String> {
    let config = destination::check_save(
        json,
        draw.broken.map(|f| Broken::Field(Some(f))),
        destination::loads,
        loaded,
    )?;
    if let Some((c, named)) = &config {
        check_loaded(draw, c, named)?;
    }
    Ok(config.map(|(c, _)| c))
}

/// Where a percent-encoded path segment of `text` is, as the save writes it: every byte but RFC 3986's unreserved
/// ones encoded.
fn encoded(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The request target a stream's rows are POSTed to.
fn target_of(d: &Map<String, Value>) -> String {
    let s = |key: &str| d[key].as_str().unwrap().trim();
    format!(
        "/dataCollectionRules/{}/streams/{}?api-version=2023-01-01",
        encoded(s("ruleId")),
        encoded(s("stream"))
    )
}

/// What `draw` says, against what it loaded as.
fn check_loaded(draw: &Draw, c: &LogAnalyticsDestinationConfig, named: &[SecretRef]) -> Result<(), String> {
    let d = &draw.destination;
    let mut wrong = Vec::new();
    let mut expect = |what: &str, want: String, got: String| {
        if want != got {
            wrong.push(format!("{what}: drawn {want}, loaded {got}"));
        }
    };
    let s = |key: &str| d.get(key).and_then(Value::as_str).map(str::trim);
    let url = format!("{}{}", s("endpoint").unwrap().trim_end_matches('/'), target_of(d));
    let h = &c.http;
    expect("url", url, h.url.clone());
    expect("body", format!("{:?}", RequestBody::JsonArray), format!("{:?}", h.body));
    expect("payload", format!("{:?}", Payload::Record), format!("{:?}", h.payload));
    expect(
        "compression",
        format!("{:?}", Compression::Gzip),
        format!("{:?}", h.compression),
    );
    expect("headers", "[]".to_string(), format!("{:?}", h.headers));
    let reference = d.get("clientSecret").map(|v| match v {
        Value::String(var) => SecretRef::WorkerLocal(var.clone()),
        v => SecretRef::LeaderStored {
            name: v["secret"].as_str().unwrap().to_string(),
            version: v["version"].as_i64().unwrap(),
        },
    });
    let sign_in = match &reference {
        Some(secret) => EntraSignIn::ServicePrincipal {
            authority: s("authEndpoint").map(|a| a.trim_end_matches('/').to_string()),
            tenant_id: s("tenantId").unwrap().to_string(),
            client_id: s("clientId").unwrap().to_string(),
            secret: secret.clone(),
        },
        None => EntraSignIn::ManagedIdentity {
            client_id: s("managedIdentityClientId").map(str::to_string),
        },
    };
    expect("signIn", format!("{sign_in:?}"), format!("{:?}", c.sign_in));
    expect(
        "credentials named",
        format!("{:?}", reference.iter().collect::<Vec<_>>()),
        format!("{:?}", named.iter().collect::<Vec<_>>()),
    );
    expect("tls", format!("{:?}", draw.tls), format!("{:?}", h.tls.is_some()));
    let size = |key: &str, default: usize| d.get(key).map_or(default, |v| v.as_u64().unwrap() as usize);
    let request = size("maxRequestBytes", MIB);
    expect("maxRequestBytes", request.to_string(), h.max_request_bytes.to_string());
    let buffer = size("bufferBytes", 64 * MIB);
    expect("bufferBytes", buffer.to_string(), h.buffer_bytes.to_string());
    let affords = (buffer / request).max(1);
    expect(
        "inFlight",
        size("inFlight", 4.min(affords)).to_string(),
        h.in_flight.to_string(),
    );
    expect(
        "stallAfterSecs",
        size("stallAfterSecs", 30).to_string(),
        h.stall_after.as_secs().to_string(),
    );
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(wrong.join("\n"))
    }
}

// -- rows -------------------------------------------------------------------------------------------------------

/// Text in any script, with what JSON must escape. Live text holds no control characters, which a table's string
/// column is not asked to keep.
fn text(rng: &mut Rng, len: usize, live: bool) -> String {
    (0..len)
        .map(|_| {
            if live {
                *rng.pick(&['a', 'z', ' ', 'é', '"', '\\', '😀', '{'])
            } else {
                *rng.pick(&[
                    'a', 'z', '0', ' ', 'é', '"', '\\', '\n', '\t', '\u{2028}', '😀', '{', '\u{1}',
                ])
            }
        })
        .collect()
}

/// The row `id`, carrying its tag in `Tag`, a column a live table has.
fn row(rng: &mut Rng, cov: &mut Reached, run: &str, id: Id, big: bool, live: bool) -> OwnedRecord {
    let tag = format!("{run}b{}r{}", id.0, id.1);
    let len = if big { 300 + rng.below(1200) } else { rng.below(24) };
    let body = serde_json::to_string(&json!({
        "TimeGenerated": "2026-10-18T00:00:00Z",
        "Tag": tag,
        "Message": text(rng, len, live),
    }))
    .unwrap();
    let value = if !live && rng.chance(4) {
        cov.reach("row.padded");
        format!(
            "{}{body}{}",
            rng.pick(&[" ", "\n", "\t "]),
            rng.pick(&["\n", "\r\n", " "])
        )
    } else {
        body
    };
    OwnedRecord {
        value: value.into_bytes(),
        payload: Payload::Record,
        ..OwnedRecord::default()
    }
}

/// One to three batches. Large rows under a small request size fill several requests.
fn batches_for(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &LogAnalyticsDestinationConfig,
    run: &str,
    poison: bool,
    unfit: bool,
    live: bool,
) -> Vec<Vec<Sent>> {
    let count = 1 + rng.below(3);
    if count > 1 {
        cov.reach("batches.several");
    }
    (0..count)
        .map(|batch| {
            let big = c.http.max_request_bytes <= 16 * KIB && rng.chance(2);
            let rows = if big { 8 + rng.below(24) } else { 1 + rng.below(12) };
            (0..rows)
                .map(|at| {
                    let mut row = row(rng, cov, run, (batch, at), big, live);
                    if unfit && rng.chance(5) {
                        cov.reach("row.unfit");
                        row.unfit = Some("nested too deep".to_string());
                    }
                    let poison = poison && row.unfit.is_none() && rng.chance(5);
                    Sent {
                        item: row.value.trim_ascii().to_vec(),
                        row,
                        poison,
                    }
                })
                .collect()
        })
        .collect()
}

// -- the service ------------------------------------------------------------------------------------------------

/// How the service answers, and what it saw. Each ingestion answer is drawn from the seed and the request's body
/// alone, so a replayed seed gets the same answers however its concurrent requests arrive.
struct Script {
    seed: u64,
    /// Each row's item, by what it is on the wire.
    items: HashMap<Vec<u8>, Id>,
    poison: BTreeSet<Id>,
    /// The status of the refusal a request carrying a poisoned row gets.
    split: u16,
    /// Whether some requests are asked for again, each body once.
    retry: bool,
    retried: BTreeSet<Vec<u8>>,
    /// The status that stalls the destination, given to every ingestion request.
    stall: Option<u16>,
    /// The lifetime of each token the service issues: under a minute, each is renewed before its next use.
    expires_in: u64,
    /// Each token request's form.
    token_requests: Vec<(String, String)>,
    /// Each ingestion request, its rows, and the status it was answered with.
    log: Vec<(Seen, Result<Vec<Id>, String>, u16)>,
    answered: BTreeSet<&'static str>,
}

impl Answers for Script {
    fn answer(&mut self, seen: &Seen, _conn: usize) -> Answer {
        if seen.target.ends_with("/oauth2/v2.0/token") {
            self.token_requests
                .push((seen.target.clone(), String::from_utf8_lossy(&seen.body).into_owned()));
            let token = format!("token-{}", self.token_requests.len());
            let body = json!({"token_type": "Bearer", "access_token": token, "expires_in": self.expires_in});
            return Answer {
                status: 200,
                headers: vec![("Content-Type".into(), "application/json".into())],
                body: body.to_string(),
            };
        }
        let mut rng = Rng::seeded(self.seed ^ fnv(&seen.body));
        let ids = self.rows_of(seen);
        let mut headers = Vec::new();
        let status = match &ids {
            Err(_) => 204,
            Ok(_) if self.stall.is_some() => self.stall.unwrap(),
            Ok(ids) if ids.iter().any(|id| self.poison.contains(id)) => {
                self.answered.insert("answer.split");
                self.split
            }
            Ok(_) if self.retry && rng.chance(3) && self.retried.insert(seen.body.clone()) => {
                self.answered.insert("answer.retry");
                if rng.chance(2) {
                    self.answered.insert("answer.retryAfter");
                    headers.push(("Retry-After".to_string(), "0".to_string()));
                }
                *rng.pick(&[429, 500, 503])
            }
            Ok(_) => {
                self.answered.insert("answer.204");
                204
            }
        };
        self.log.push((seen.clone(), ids, status));
        // Azure Monitor says why it refused in a JSON error.
        let body = if status == 204 {
            String::new()
        } else {
            json!({"error": {"code": "Fuzz", "message": format!("answered {status}")}}).to_string()
        };
        Answer { status, headers, body }
    }
}

impl Script {
    /// The rows `seen` carries, in order, when its body is exactly their items, gzipped, as a JSON array.
    fn rows_of(&self, seen: &Seen) -> Result<Vec<Id>, String> {
        json_array_rows(&gunzip(&seen.body)?, |item| {
            (self.items.get(item).copied())
                .ok_or_else(|| format!("an item no row is: {:?}", String::from_utf8_lossy(item)))
        })
    }
}

// -- delivering -------------------------------------------------------------------------------------------------

fn build(c: &LogAnalyticsDestinationConfig, secrets: &HeldSecrets) -> Result<Box<dyn Destination>, IoError> {
    let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
    smeltr::io::build_destination(&DestinationKind::LogAnalytics(c.clone()), 0, metrics, secrets, None)
}

/// The rows each request carried, by how many requests landed each, and how many requests carried each batch.
type Carried = (BTreeMap<Id, usize>, BTreeMap<usize, usize>);

/// Checks every token request against the principal, and every ingestion request against `c` and the tokens issued.
fn check_requests(
    c: &LogAnalyticsDestinationConfig,
    d: &Map<String, Value>,
    secret: &str,
    sent: &[Vec<Sent>],
    script: &Script,
) -> Result<Carried, String> {
    let want: Vec<(String, String)> = [
        ("grant_type", "client_credentials"),
        ("client_id", "smeltr-app"),
        ("client_secret", secret),
        ("scope", "https://monitor.azure.com/.default"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    for (target, body) in &script.token_requests {
        if *target != format!("/{TENANT}/oauth2/v2.0/token") {
            return Err(format!("a token asked of {target:?}"));
        }
        let form = reqwest::Url::parse(&format!("http://form/?{body}")).map_err(|e| format!("{body:?}: {e}"))?;
        let got: Vec<(String, String)> = form
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        if got != want {
            return Err(format!("a token asked with {got:?}, not {want:?}"));
        }
    }
    let issued = script.token_requests.len();
    let target = target_of(d);
    let mut landed: BTreeMap<Id, usize> = BTreeMap::new();
    let mut requests: BTreeMap<usize, usize> = BTreeMap::new();
    let mut carried: BTreeSet<String> = BTreeSet::new();
    for (seen, ids, status) in &script.log {
        let ids = ids
            .as_ref()
            .map_err(|e| format!("a request the service could not read: {e}"))?;
        let mut wrong = Vec::new();
        if seen.method != "POST" {
            wrong.push(format!("method {}", seen.method));
        }
        if seen.target != target {
            wrong.push(format!("target {:?}, not {target:?}", seen.target));
        }
        if seen.header("content-type") != ["application/json"] {
            wrong.push(format!("content-type {:?}", seen.header("content-type")));
        }
        if seen.header("content-encoding") != ["gzip"] {
            wrong.push(format!("content-encoding {:?}", seen.header("content-encoding")));
        }
        let token = match seen.header("authorization").as_slice() {
            [bearer] => bearer
                .strip_prefix("Bearer token-")
                .and_then(|n| n.parse::<usize>().ok()),
            _ => None,
        };
        match token {
            Some(n) if (1..=issued).contains(&n) => {
                carried.insert(format!("token-{n}"));
            }
            _ => wrong.push(format!(
                "authorization {:?}, no token issued",
                seen.header("authorization")
            )),
        }
        wrong.extend(http::wrong_rows(ids, sent, c.http.max_request_bytes));
        if !wrong.is_empty() {
            return Err(format!("a request carrying {ids:?}: {}", wrong.join("; ")));
        }
        if let Some((b, _)) = ids.first() {
            *requests.entry(*b).or_default() += 1;
        }
        if *status == 204 {
            for id in ids {
                *landed.entry(*id).or_default() += 1;
            }
        }
    }
    // A token kept for its lifetime is asked for once; one renewed before each use, for every request. A token
    // refused is asked for again.
    if script.log.iter().any(|(_, _, status)| *status == 401) {
        return Ok((landed, requests));
    }
    if script.expires_in > 60 && issued > 1 {
        return Err(format!(
            "{issued} tokens asked for, though the first was good for an hour"
        ));
    }
    if script.expires_in <= 60 && carried.len() != script.log.len() {
        return Err(format!(
            "{} requests carried {} tokens, though each was due for renewal at once",
            script.log.len(),
            carried.len()
        ));
    }
    Ok((landed, requests))
}

/// Checks every request against `c`, and that every row landed once or was refused as it had to be.
fn check_delivered(
    c: &LogAnalyticsDestinationConfig,
    d: &Map<String, Value>,
    secret: &str,
    sent: &[Vec<Sent>],
    refused: &[Vec<Rejection>],
    script: &Script,
) -> Result<(), String> {
    let (landed, requests) = check_requests(c, d, secret, sent, script)?;
    http::check_landed(sent, refused, &landed, script.split)?;
    if script.split == 0 && script.log.iter().all(|(_, _, s)| *s == 204) {
        for (batch, rows) in sent.iter().enumerate() {
            let want = http::requests_for(c.http.max_request_bytes, rows);
            let got = requests.get(&batch).copied().unwrap_or(0);
            if got != want {
                return Err(format!("batch {batch} went in {got} requests, not {want}"));
            }
        }
    }
    Ok(())
}

// -- the offline fuzzer -----------------------------------------------------------------------------------------

/// Each seed's destination, against the save, what it builds with, and a service on this host it delivers to.
#[test]
fn log_analytics_destination_fuzz() {
    assert!(
        std::env::var(SET_VARIABLE).is_ok(),
        "{SET_VARIABLE} is set in a test process"
    );
    destination::run(0..SEEDS, OPTIONS.iter().copied(), offline_seed);
}

fn offline_seed(seed: u64, reached: &mut Reached) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let mut stored = String::new();
    // Bound first, so the draw's endpoints can name their ports.
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let tokens = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = |l: &TcpListener| l.local_addr().unwrap().port();
    let mut draw = draw(
        &mut rng,
        reached,
        Target::Offline(port(&listener), port(&tokens)),
        &mut stored,
    );
    let mangled = draw.broken.is_none() && rng.chance(12);
    reached.only_breaks = mangled || draw.broken.is_some();
    let unknown = mangled && destination::mangle(&mut rng, reached, &mut draw.destination, &["unknownSetting"]);
    let json = bundle(&draw).to_string();
    let context = |e: String| format!("{e}\n{json}");
    if mangled {
        if unknown && unknown_key_taken(&json) {
            return Err(context("a key it does not know is taken".to_string()));
        }
        return Ok(());
    }
    let Some(c) = check_save(&draw, &json).map_err(context)? else {
        return Ok(());
    };

    let (secrets, unheld) = held(&draw, &stored);
    let mut d = match (build(&c, &secrets), unheld) {
        (Ok(d), false) => d,
        (Err(e), true) if e.message.contains(UNSET_VARIABLE) => {
            reached.keep_only(&["secret.workerLocalUnset"]);
            return Ok(());
        }
        (Ok(_), true) => return Err(context("built with a secret it does not hold".to_string())),
        (Err(e), _) => return Err(context(format!("does not build: {}", e.message))),
    };
    // A managed identity signs in only on an Azure host: what it loads as and builds is all this host can check.
    let Some(secret) = secret_value(&draw, &stored) else {
        reached.keep_only(&["signIn.managedIdentity", "signIn.managedIdentityClientId"]);
        return Ok(());
    };

    let poison = rng.chance(5);
    let unfit = rng.chance(5);
    let sent = batches_for(&mut rng, reached, &c, &format!("s{seed}"), poison, unfit, false);
    let stall = rng.chance(12).then(|| *rng.pick(&[401, 403, 404]));
    let renew = rng.chance(6);
    if renew {
        reached.reach("token.renewed");
    }
    let script = Script {
        items: (sent.iter().enumerate())
            .flat_map(|(b, rows)| rows.iter().enumerate().map(move |(r, s)| (s.item.clone(), (b, r))))
            .collect(),
        poison: (sent.iter().enumerate())
            .flat_map(|(b, rows)| {
                rows.iter()
                    .enumerate()
                    .filter(|(_, s)| s.poison)
                    .map(move |(r, _)| (b, r))
            })
            .collect(),
        seed,
        split: if poison { *rng.pick(&[400, 413]) } else { 0 },
        // Each costs a backoff, so they are kept rare.
        retry: rng.chance(24),
        retried: BTreeSet::new(),
        stall,
        expires_in: if renew { 30 } else { 3600 },
        token_requests: Vec::new(),
        log: Vec::new(),
        answered: BTreeSet::new(),
    };
    let script = Arc::new(Mutex::new(script));
    let tls = draw.tls.then(|| pki().server_only());
    let service = Service::start(listener, tls, script.clone());
    let token_service = Service::start(tokens, None, script.clone());

    let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
    let delivered = destination::write(d.as_mut(), &batches, Duration::from_secs(60));
    drop(d);
    drop((service, token_service));
    let script = script.lock().unwrap();
    // A batch whose every row is unfit sends nothing to be stalled by.
    let sends = sent.iter().flatten().any(|s| s.row.unfit.is_none());
    if let Some(status) = stall.filter(|_| sends) {
        check_requests(&c, &draw.destination, &secret, &sent, &script).map_err(context)?;
        reached.reach("answer.stall");
        let e = match delivered {
            Ok(_) => return Err(context(format!("answered {status}, and did not stall"))),
            Err(e) => e,
        };
        if !e.contains(&format!("HTTP {status}")) {
            return Err(context(format!("answered {status}, stalled saying {e:?}")));
        }
        if e.contains(&secret) || e.contains("token-") {
            return Err(context(format!("the stall shows a credential: {e}")));
        }
        return Ok(());
    }
    let refused = delivered.map_err(|e| context(format!("stalled: {e}")))?;
    check_delivered(&c, &draw.destination, &secret, &sent, &refused, &script).map_err(context)?;
    for name in &script.answered {
        reached.reach(*name);
    }
    if sent
        .iter()
        .any(|rows| http::requests_for(c.http.max_request_bytes, rows) > 1)
    {
        reached.reach("requests.several");
    }
    Ok(())
}

// -- the live fuzzer --------------------------------------------------------------------------------------------

/// What a live draw reaches: every option a real rule serves, signed in as a real principal. Not TLS, the
/// endpoint's forms or the answers, which are Azure's to give; not the secret a worker holds, which the leader sends.
fn live_options() -> Vec<&'static str> {
    OPTIONS
        .iter()
        .copied()
        .filter(|o| {
            !(o.starts_with("broken.")
                || o.starts_with("answer.")
                || o.starts_with("tls.")
                || o.starts_with("endpoint.")
                || o.starts_with("ruleId.")
                || o.starts_with("stream.")
                || o.starts_with("signIn.managedIdentity")
                || [
                    "mangled",
                    "secret.workerLocal",
                    "secret.workerLocalUnset",
                    "signIn.authEndpointSlash",
                    "row.padded",
                    "token.renewed",
                ]
                .contains(o))
        })
        .collect()
}

/// A name no earlier run used.
fn unique() -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("sf{n:x}")
}

/// Every `Message` the rule's table holds under a tag starting with `run`, by tag. Asked until each of `tags` is
/// there or fifteen minutes pass, since a row is queryable minutes after it is ingested.
fn filed(s: &live::LogAnalytics, run: &str, tags: &BTreeSet<String>) -> Result<BTreeMap<String, Vec<String>>, String> {
    let agent = ureq::Agent::new_with_defaults();
    let token: Value = agent
        .post(format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            s.tenant_id
        ))
        .send_form([
            ("grant_type", "client_credentials"),
            ("client_id", s.client_id.as_str()),
            ("client_secret", s.client_secret.as_str()),
            ("scope", "https://api.loganalytics.io/.default"),
        ])
        .map_err(|e| format!("signing in to query: {e}"))?
        .body_mut()
        .read_json()
        .map_err(|e| format!("reading the token: {e}"))?;
    let bearer = format!("Bearer {}", token["access_token"].as_str().unwrap_or_default());
    let query = format!("{} | where Tag startswith '{run}' | project Tag, Message", s.table);
    let deadline = Instant::now() + Duration::from_secs(15 * 60);
    loop {
        let answer: Value = agent
            .post(format!(
                "https://api.loganalytics.io/v1/workspaces/{}/query",
                s.workspace
            ))
            .header("Authorization", &bearer)
            .send_json(json!({"query": query, "timespan": "P1D"}))
            .map_err(|e| format!("querying the workspace: {e}"))?
            .body_mut()
            .read_json()
            .map_err(|e| format!("reading the query: {e}"))?;
        let mut found: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for row in answer["tables"][0]["rows"].as_array().into_iter().flatten() {
            let tag = row[0].as_str().unwrap_or_default().to_string();
            found
                .entry(tag)
                .or_default()
                .push(row[1].as_str().unwrap_or_default().to_string());
        }
        if tags.iter().all(|t| found.contains_key(t)) || Instant::now() > deadline {
            return Ok(found);
        }
        std::thread::sleep(Duration::from_secs(20));
    }
}

/// Each seed's destination, against the save, and delivering to a real rule, whose table, queried back, holds every
/// row landed exactly once, as it was sent.
#[test]
#[ignore = "writes to a real Log Analytics workspace; `just check-log-analytics` runs it"]
fn live_log_analytics_fuzz() {
    let Some(s) = live::log_analytics("live_log_analytics_fuzz") else {
        return;
    };
    destination::run(0..LIVE_SEEDS, live_options(), |seed, reached| {
        let mut rng = Rng::seeded(seed);
        let run = unique();
        let mut stored = String::new();
        let draw = draw(&mut rng, reached, Target::Live(&s), &mut stored);
        let json = bundle(&draw).to_string();
        let context = |e: String| format!("{e}\n{json}");
        let c = check_save(&draw, &json)
            .map_err(context)?
            .expect("a live draw is never broken");
        let (secrets, _) = held(&draw, &stored);
        let mut d = build(&c, &secrets).map_err(|e| context(format!("does not build: {}", e.message)))?;
        let unfit = rng.chance(4);
        let sent = batches_for(&mut rng, reached, &c, &run, false, unfit, true);
        let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
        let refused = destination::write(d.as_mut(), &batches, Duration::from_secs(120))
            .map_err(|e| context(format!("stalled: {e}")))?;
        destination::check_refused(&sent, &refused, |s| s.row.unfit.is_some()).map_err(context)?;
        let message = |s: &Sent| -> (String, String) {
            let v: Value = serde_json::from_slice(&s.row.value).unwrap();
            (
                v["Tag"].as_str().unwrap().to_string(),
                v["Message"].as_str().unwrap().to_string(),
            )
        };
        let landing: BTreeMap<String, String> = (sent.iter().flatten())
            .filter(|s| s.row.unfit.is_none())
            .map(message)
            .collect();
        let found = filed(&s, &run, &landing.keys().cloned().collect()).map_err(context)?;
        for (tag, msg) in &landing {
            if found.get(tag) != Some(&vec![msg.clone()]) {
                return Err(context(format!(
                    "{tag} filed as {:?}, not once as {msg:?}",
                    found.get(tag)
                )));
            }
        }
        for s in sent.iter().flatten().filter(|s| s.row.unfit.is_some()) {
            let (tag, _) = message(s);
            if found.contains_key(&tag) {
                return Err(context(format!("a refused row was filed: {tag}")));
            }
        }
        Ok(())
    });
}
