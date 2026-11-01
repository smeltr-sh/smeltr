//! The DNS-01 proof: a TXT record the leader publishes through the provider that holds the zone, and takes down
//! when the order is over.
//!
//! ```mermaid
//! flowchart LR
//!   C[the authority's challenge] --> P[publish TXT through the provider]
//!   P --> V[the zone's nameservers all answer it]
//!   V --> R[the authority is told to look]
//!   R --> D[order over: every record removed]
//! ```
//!
//! The authority asks the zone's nameservers, so the record is waited for there, asked of each one directly: a
//! recursive resolver would cache the answer from before the record was published. A provider is anything that
//! can publish and remove a TXT record ([`Provider`]); Cloudflare is the one there is.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use hickory_resolver::config::{NameServerConfig, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::proto::rr::RData;
use hickory_resolver::{Resolver, TokioResolver};

use super::acme::{Prover, Settings};

/// Which provider publishes the records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Cloudflare,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Cloudflare => "cloudflare",
        }
    }

    pub fn parse(text: &str) -> Option<Kind> {
        match text {
            "cloudflare" => Some(Kind::Cloudflare),
            _ => None,
        }
    }
}

/// A TXT record a provider published, with what it takes to remove it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// The zone it is in, as DNS names it.
    pub zone: String,
    /// The provider's id for the zone.
    pub zone_id: String,
    pub name: String,
    pub value: String,
    /// The provider's id for the record.
    pub id: String,
}

/// Where the zone is held: what publishes a TXT record in it and removes it.
#[async_trait]
pub trait Provider: Send + Sync {
    /// Publishes `value` as a TXT record at `name`, beside any already there.
    async fn publish(&self, name: &str, value: &str) -> Result<Record, String>;
    async fn remove(&self, record: &Record) -> Result<(), String>;
}

/// Who is asked whether a record is there.
#[async_trait]
pub trait Nameservers: Send + Sync {
    /// Whether every nameserver of `record.zone` answers `record.value` at `record.name`. An error says which could
    /// not be asked.
    async fn all_answer(&self, record: &Record) -> Result<bool, String>;
}

/// `_acme-challenge.` under the name, a wildcard's included: `*.example.com` is proved at
/// `_acme-challenge.example.com`.
pub fn record_name(name: &str) -> String {
    format!("_acme-challenge.{}", name.strip_prefix("*.").unwrap_or(name))
}

/// The zones that could hold `name`, nearest first: `a.example.com`, then `example.com`. A top-level domain is
/// nobody's zone to publish in.
fn enclosing(name: &str) -> impl Iterator<Item = &str> {
    let name = name.trim_end_matches('.');
    std::iter::successors(Some(name), |n| n.split_once('.').map(|(_, rest)| rest)).filter(|n| n.contains('.'))
}

/// The proofs for an order under `settings`, published through the provider they name.
pub fn publishing(settings: &Settings) -> Result<Publishing, String> {
    let provider: Box<dyn Provider> = match settings.dns_provider {
        Kind::Cloudflare => {
            let token = settings
                .cloudflare_token
                .as_deref()
                .ok_or("DNS-01 through Cloudflare needs an API token")?;
            Box::new(Cloudflare::new(CLOUDFLARE_API, token)?)
        }
    };
    Ok(Publishing::new(provider, Box::new(ZoneNameservers)))
}

// -- where a proof goes -------------------------------------------------------------------------------------------

/// How often the nameservers are asked again, and for how long, before the record is given up on.
const ASK_EVERY: Duration = Duration::from_secs(5);
const VISIBLE_WITHIN: Duration = Duration::from_secs(5 * 60);
/// How often the log says the record is still waited for.
const TELL_EVERY: Duration = Duration::from_secs(60);

/// DNS-01 proofs published through a provider, each waited for on the zone's nameservers. Every record it published
/// is removed when the order is over, however it ended.
pub struct Publishing {
    provider: Box<dyn Provider>,
    nameservers: Box<dyn Nameservers>,
    published: tokio::sync::Mutex<Vec<Record>>,
    ask_every: Duration,
    within: Duration,
}

impl Publishing {
    pub fn new(provider: Box<dyn Provider>, nameservers: Box<dyn Nameservers>) -> Publishing {
        Publishing {
            provider,
            nameservers,
            published: Default::default(),
            ask_every: ASK_EVERY,
            within: VISIBLE_WITHIN,
        }
    }

    /// Asking every `ask_every`, giving up after `within`: a test's own clock.
    #[cfg(test)]
    fn asking(self, ask_every: Duration, within: Duration) -> Publishing {
        Publishing {
            ask_every,
            within,
            ..self
        }
    }

    /// Until every nameserver answers `record`. The last error is kept so a record never seen says why.
    async fn visible(&self, record: &Record) -> Result<(), String> {
        let started = tokio::time::Instant::now();
        let mut told = started;
        loop {
            let asked = self.nameservers.all_answer(record).await;
            if let Ok(true) = asked {
                tracing::info!(target: "leader.acme_dns_visible", record = %record.name, zone = %record.zone, "every nameserver answers the DNS-01 record");
                return Ok(());
            }
            if told.elapsed() >= TELL_EVERY {
                told = tokio::time::Instant::now();
                let answer = match &asked {
                    Err(e) => e.clone(),
                    Ok(_) => "not every nameserver answers it yet".to_string(),
                };
                tracing::info!(target: "leader.acme_dns_waiting", record = %record.name, zone = %record.zone, waited_secs = started.elapsed().as_secs(), %answer, "waiting for the nameservers to answer the DNS-01 record");
            }
            if started.elapsed() >= self.within {
                let why = match asked {
                    Err(e) => format!(": {e}"),
                    _ => String::new(),
                };
                return Err(format!(
                    "the TXT record {} was not on every nameserver of {} within {}s{why}",
                    record.name,
                    record.zone,
                    self.within.as_secs()
                ));
            }
            tokio::time::sleep(self.ask_every).await;
        }
    }
}

#[async_trait]
impl Prover for Publishing {
    async fn tls_alpn(&self, _: &str, _: &[u8]) -> Result<(), String> {
        Err("a DNS-01 order asks for no TLS-ALPN-01 proof".to_string())
    }

    async fn http(&self, _: &str, _: &str) -> Result<(), String> {
        Err("a DNS-01 order asks for no HTTP-01 proof".to_string())
    }

    async fn dns(&self, name: &str, value: &str) -> Result<(), String> {
        if name.parse::<IpAddr>().is_ok() {
            return Err(format!("DNS-01 cannot prove an IP address: {name}"));
        }
        let record = self.provider.publish(&record_name(name), value).await?;
        tracing::info!(target: "leader.acme_dns_published", record = %record.name, zone = %record.zone, "published the DNS-01 record");
        self.published.lock().await.push(record.clone());
        self.visible(&record).await
    }

    async fn done(&self) {
        for record in self.published.lock().await.drain(..) {
            match self.provider.remove(&record).await {
                Ok(()) => {
                    tracing::info!(target: "leader.acme_dns_removed", record = %record.name, "removed the DNS-01 record")
                }
                Err(e) => {
                    tracing::warn!(target: "leader.acme_dns_left", record = %record.name, error = %e, "could not remove a DNS-01 record")
                }
            }
        }
    }
}

// -- the zone's nameservers ---------------------------------------------------------------------------------------

/// The nameservers the zone names for itself, found through this host's resolver and each asked directly.
pub struct ZoneNameservers;

impl ZoneNameservers {
    /// Each nameserver's addresses, by its name.
    async fn of(zone: &str) -> Result<Vec<(String, Vec<IpAddr>)>, String> {
        let system = TokioResolver::builder_tokio()
            .map_err(|e| format!("this host's DNS resolver: {e}"))?
            .build()
            .map_err(|e| format!("this host's DNS resolver: {e}"))?;
        let fqdn = format!("{}.", zone.trim_end_matches('.'));
        let named = system
            .ns_lookup(fqdn.as_str())
            .await
            .map_err(|e| format!("the nameservers of {zone}: {e}"))?;
        let mut found = Vec::new();
        for answer in named.answers() {
            let RData::NS(ns) = &answer.data else { continue };
            let host = ns.0.to_ascii();
            let addresses = system
                .lookup_ip(host.as_str())
                .await
                .map_err(|e| format!("the address of {host}, a nameserver of {zone}: {e}"))?;
            found.push((host, addresses.iter().collect()));
        }
        if found.is_empty() {
            return Err(format!("{zone} names no nameservers"));
        }
        Ok(found)
    }
}

#[async_trait]
impl Nameservers for ZoneNameservers {
    async fn all_answer(&self, record: &Record) -> Result<bool, String> {
        for (host, addresses) in ZoneNameservers::of(&record.zone).await? {
            let at: Vec<_> = addresses.into_iter().map(|ip| SocketAddr::new(ip, 53)).collect();
            if !one_answers(&host, &at, record).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Whether the nameserver `host` answers `record.value`, asked at the first of its `addresses` this host reaches:
/// a host with no IPv6 route reaches none of its IPv6 ones. An error only when none is reached.
async fn one_answers(host: &str, addresses: &[SocketAddr], record: &Record) -> Result<bool, String> {
    let mut unreached = Vec::new();
    for at in addresses {
        match answers(*at, record).await {
            Ok(answered) => return Ok(answered),
            Err(e) => unreached.push(format!("{}: {e}", at.ip())),
        }
    }
    Err(format!("{host}: {}", unreached.join("; ")))
}

/// Whether the nameserver at `at` answers `record.value` among the TXT records at `record.name`. Asked without
/// recursion and without a cache, so the answer is the server's own and current.
pub async fn answers(at: SocketAddr, record: &Record) -> Result<bool, String> {
    let mut server = NameServerConfig::udp_and_tcp(at.ip());
    for connection in &mut server.connections {
        connection.port = at.port();
    }
    let mut options = ResolverOpts::default();
    options.recursion_desired = false;
    options.cache_size = 0;
    options.attempts = 1;
    options.timeout = Duration::from_secs(5);
    let resolver = Resolver::builder_with_config(
        ResolverConfig::from_name_servers(vec![server]),
        TokioRuntimeProvider::default(),
    )
    .with_options(options)
    .build()
    .map_err(|e| e.to_string())?;
    let fqdn = format!("{}.", record.name.trim_end_matches('.'));
    let found = match resolver.txt_lookup(fqdn.as_str()).await {
        Ok(found) => found,
        Err(e) if e.is_no_records_found() || e.is_nx_domain() => return Ok(false),
        Err(e) => return Err(e.to_string()),
    };
    Ok(found.answers().iter().any(|answer| match &answer.data {
        RData::TXT(txt) => txt.txt_data.concat() == record.value.as_bytes(),
        _ => false,
    }))
}

// -- Cloudflare ---------------------------------------------------------------------------------------------------

/// Cloudflare's API.
pub const CLOUDFLARE_API: &str = "https://api.cloudflare.com/client/v4";

/// Cloudflare's codes for a record that is already there, name and content alike: 81058, and 81057 as it answered
/// before.
const IDENTICAL: [i64; 2] = [81057, 81058];

/// How long one call to the provider may take, and the most it may answer.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);
const MOST_ANSWER: usize = 1024 * 1024;

/// Records published in a Cloudflare zone, with an API token that may read the zone and edit its DNS.
pub struct Cloudflare {
    api: String,
    token: String,
    client: reqwest::Client,
}

impl Cloudflare {
    pub fn new(api: &str, token: &str) -> Result<Cloudflare, String> {
        let client = super::outbound::client(api, "Cloudflare", CALL_TIMEOUT, reqwest::redirect::Policy::none())?;
        Ok(Cloudflare {
            api: api.trim_end_matches('/').to_string(),
            token: token.to_string(),
            client,
        })
    }

    /// The `result` of a call, or what Cloudflare said is wrong with it.
    async fn call(&self, request: reqwest::RequestBuilder, what: &str) -> Result<serde_json::Value, String> {
        self.answer(request, what).await.map_err(|(_, e)| e)
    }

    /// As [`Cloudflare::call`], with the codes of what Cloudflare said is wrong.
    async fn answer(
        &self,
        request: reqwest::RequestBuilder,
        what: &str,
    ) -> Result<serde_json::Value, (Vec<i64>, String)> {
        let response = request
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| (Vec::new(), format!("Cloudflare, {what}: {e}")))?;
        let status = response.status();
        let body = super::outbound::read(response, MOST_ANSWER)
            .await
            .map_err(|e| (Vec::new(), format!("Cloudflare, {what}: {e}")))?;
        let answer: serde_json::Value = serde_json::from_slice(&body).map_err(|_| {
            (
                Vec::new(),
                format!("Cloudflare, {what}: answered HTTP {status} with no JSON"),
            )
        })?;
        if status.is_success() && answer["success"] == true {
            return Ok(answer["result"].clone());
        }
        let codes = answer["errors"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|e| e["code"].as_i64())
            .collect();
        let said: Vec<String> = answer["errors"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|e| e["message"].as_str().map(str::to_string))
            .collect();
        Err((
            codes,
            match said.is_empty() {
                true => format!("Cloudflare refused {what}: HTTP {status}"),
                false => format!("Cloudflare refused {what}: {}", said.join("; ")),
            },
        ))
    }

    /// The zone holding `name`, nearest first, as the token sees them.
    async fn zone_of(&self, name: &str) -> Result<(String, String), String> {
        for zone in enclosing(name) {
            let mut url = reqwest::Url::parse(&format!("{}/zones", self.api)).map_err(|e| e.to_string())?;
            url.query_pairs_mut().append_pair("name", zone);
            let request = self.client.get(url);
            let found = self.call(request, &format!("looking up the zone {zone}")).await?;
            if let Some(id) = found
                .as_array()
                .into_iter()
                .flatten()
                .find(|z| z["name"].as_str().is_some_and(|n| n.eq_ignore_ascii_case(zone)))
                .and_then(|z| z["id"].as_str())
            {
                return Ok((zone.to_string(), id.to_string()));
            }
        }
        let name = name.strip_prefix("_acme-challenge.").unwrap_or(name);
        Err(format!(
            "Cloudflare holds no zone for {name} that this token can read. The token needs Zone · Zone · Read and \
             Zone · DNS · Edit on it."
        ))
    }
}

#[async_trait]
impl Provider for Cloudflare {
    async fn publish(&self, name: &str, value: &str) -> Result<Record, String> {
        let (zone, zone_id) = self.zone_of(name).await?;
        let request = self
            .client
            .post(format!("{}/zones/{zone_id}/dns_records", self.api))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::json!({ "type": "TXT", "name": name, "content": value, "ttl": 60 }).to_string());
        let created = match self.answer(request, &format!("adding the TXT record {name}")).await {
            Ok(created) => created,
            Err((codes, _)) if codes.iter().any(|c| IDENTICAL.contains(c)) => {
                return Err(format!(
                    "Cloudflare already holds the TXT record {name} with the value this order needs, added by an \
                     earlier or running order for the same name. When no other order for it is running, delete the \
                     record in Cloudflare and try again."
                ));
            }
            Err((_, e)) => return Err(e),
        };
        let id = created["id"]
            .as_str()
            .ok_or_else(|| format!("Cloudflare added the TXT record {name} but named no id for it"))?;
        Ok(Record {
            zone,
            zone_id,
            name: name.to_string(),
            value: value.to_string(),
            id: id.to_string(),
        })
    }

    async fn remove(&self, record: &Record) -> Result<(), String> {
        let request = self.client.delete(format!(
            "{}/zones/{}/dns_records/{}",
            self.api, record.zone_id, record.id
        ));
        self.call(request, &format!("removing the TXT record {}", record.name))
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fuzz::{Coverage, Rng, run};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// How a stand-in Cloudflare fails, when it does.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Fails {
        Never,
        /// Every call: a token it does not know.
        Token,
        /// Adding a record.
        Adding,
        /// Removing one.
        Removing,
    }

    #[derive(Default)]
    struct Held {
        /// Zone name to id.
        zones: HashMap<String, String>,
        /// Record id to (zone id, name, content).
        records: HashMap<String, (String, String, String)>,
        next: u32,
        fails: Option<Fails>,
        /// Every record ever added, by name and content.
        added: Vec<(String, String)>,
    }

    /// A stand-in for Cloudflare's API on loopback: the three calls a DNS-01 order makes, answered as Cloudflare
    /// answers them.
    struct FakeCloudflare {
        held: Arc<Mutex<Held>>,
        api: String,
    }

    const TOKEN: &str = "the-token";

    impl FakeCloudflare {
        async fn start(zones: &[&str], fails: Fails) -> FakeCloudflare {
            use axum::extract::{Path, Query, State};
            use axum::http::{HeaderMap, StatusCode};
            use axum::routing::{delete, get, post};

            type Shared = Arc<Mutex<Held>>;
            fn refused(status: StatusCode, message: &str) -> (StatusCode, String) {
                refused_as(status, 1, message)
            }
            fn refused_as(status: StatusCode, code: i64, message: &str) -> (StatusCode, String) {
                let body = serde_json::json!({ "success": false, "errors": [{ "code": code, "message": message }], "result": null });
                (status, body.to_string())
            }
            fn ok(result: serde_json::Value) -> (StatusCode, String) {
                (
                    StatusCode::OK,
                    serde_json::json!({ "success": true, "errors": [], "result": result }).to_string(),
                )
            }
            fn authorized(held: &Held, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
                let bearer = headers.get("authorization").and_then(|v| v.to_str().ok());
                if held.fails == Some(Fails::Token) || bearer != Some(&format!("Bearer {TOKEN}")) {
                    return Err(refused(StatusCode::FORBIDDEN, "Authentication error"));
                }
                Ok(())
            }

            let held: Shared = Arc::new(Mutex::new(Held {
                zones: zones
                    .iter()
                    .enumerate()
                    .map(|(i, z)| (z.to_string(), format!("zone{i}")))
                    .collect(),
                fails: Some(fails),
                ..Held::default()
            }));
            let app = axum::Router::new()
                .route(
                    "/zones",
                    get(
                        |State(held): State<Shared>, headers: HeaderMap, Query(q): Query<HashMap<String, String>>| async move {
                            let held = held.lock().unwrap();
                            if let Err(e) = authorized(&held, &headers) {
                                return e;
                            }
                            let name = q.get("name").cloned().unwrap_or_default();
                            let found: Vec<_> = held
                                .zones
                                .get(&name)
                                .map(|id| serde_json::json!({ "id": id, "name": name }))
                                .into_iter()
                                .collect();
                            ok(serde_json::json!(found))
                        },
                    ),
                )
                .route(
                    "/zones/:zone/dns_records",
                    post(
                        |State(held): State<Shared>, headers: HeaderMap, Path(zone): Path<String>, body: String| async move {
                            let mut held = held.lock().unwrap();
                            if let Err(e) = authorized(&held, &headers) {
                                return e;
                            }
                            if held.fails == Some(Fails::Adding) {
                                return refused(StatusCode::BAD_REQUEST, "DNS Validation Error");
                            }
                            let asked: serde_json::Value = serde_json::from_str(&body).unwrap();
                            assert_eq!(asked["type"], "TXT");
                            let name = asked["name"].as_str().unwrap().to_string();
                            let content = asked["content"].as_str().unwrap().to_string();
                            // In the nearest zone that holds it, not one above.
                            let nearest = enclosing(&name).find_map(|z| held.zones.get(z)).unwrap();
                            assert_eq!(*nearest, zone, "{name} added to the wrong zone");
                            if held.records.values().any(|(_, n, c)| *n == name && *c == content) {
                                return refused_as(StatusCode::BAD_REQUEST, 81058, "An identical record already exists.");
                            }
                            held.next += 1;
                            let id = format!("record{}", held.next);
                            held.records.insert(id.clone(), (zone, name.clone(), content.clone()));
                            held.added.push((name, content));
                            ok(serde_json::json!({ "id": id }))
                        },
                    ),
                )
                .route(
                    "/zones/:zone/dns_records/:id",
                    delete(
                        |State(held): State<Shared>, headers: HeaderMap, Path((zone, id)): Path<(String, String)>| async move {
                            let mut held = held.lock().unwrap();
                            if let Err(e) = authorized(&held, &headers) {
                                return e;
                            }
                            if held.fails == Some(Fails::Removing) {
                                return refused(StatusCode::INTERNAL_SERVER_ERROR, "Internal error");
                            }
                            match held.records.get(&id) {
                                Some((z, _, _)) if *z == zone => {
                                    held.records.remove(&id);
                                    ok(serde_json::json!({ "id": id }))
                                }
                                _ => refused(StatusCode::NOT_FOUND, "Record does not exist."),
                            }
                        },
                    ),
                )
                .with_state(held.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let api = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await });
            FakeCloudflare { held, api }
        }

        /// The records Cloudflare holds now, by name and content.
        fn records(&self) -> Vec<(String, String)> {
            let mut held: Vec<_> = self
                .held
                .lock()
                .unwrap()
                .records
                .values()
                .map(|(_, n, c)| (n.clone(), c.clone()))
                .collect();
            held.sort();
            held
        }
    }

    /// The zone's nameservers, reading what the stand-in Cloudflare holds: answering only after `lag` asks, or
    /// never.
    struct FakeNameservers {
        held: Arc<Mutex<Held>>,
        lag: Option<usize>,
        asked: Mutex<usize>,
    }

    #[async_trait]
    impl Nameservers for FakeNameservers {
        async fn all_answer(&self, record: &Record) -> Result<bool, String> {
            let mut asked = self.asked.lock().unwrap();
            *asked += 1;
            let Some(lag) = self.lag else {
                return Err("ns1.test: timed out".to_string());
            };
            if *asked <= lag {
                return Ok(false);
            }
            Ok(self
                .held
                .lock()
                .unwrap()
                .records
                .values()
                .any(|(_, n, c)| *n == record.name && *c == record.value))
        }
    }

    fn publishing(cloudflare: &FakeCloudflare, token: &str, lag: Option<usize>) -> Publishing {
        Publishing::new(
            Box::new(Cloudflare::new(&cloudflare.api, token).unwrap()),
            Box::new(FakeNameservers {
                held: cloudflare.held.clone(),
                lag,
                asked: Mutex::new(0),
            }),
        )
        .asking(Duration::from_millis(1), Duration::from_millis(50))
    }

    #[test]
    fn a_wildcard_is_proved_at_the_name_beneath_it() {
        assert_eq!(record_name("a.example.com"), "_acme-challenge.a.example.com");
        assert_eq!(record_name("*.example.com"), "_acme-challenge.example.com");
    }

    #[test]
    fn the_enclosing_zones_are_tried_nearest_first_and_never_a_top_level_domain() {
        let zones: Vec<_> = enclosing("_acme-challenge.a.example.com.").collect();
        assert_eq!(zones, ["_acme-challenge.a.example.com", "a.example.com", "example.com"]);
        assert_eq!(enclosing("com").count(), 0);
    }

    /// Every way a DNS-01 order's records can go: names in the zone, under it, wildcards, more than one at a
    /// name, an IP, a name in no zone, Cloudflare refusing each call, and records slow to reach the nameservers or
    /// never reaching them. Whatever happened, every record added is gone after, unless removing is what failed.
    #[test]
    fn acme_dns_fuzz() {
        let mut coverage = Coverage::new([
            "apex",
            "subdomain",
            "nested-zone",
            "wildcard",
            "shared-record",
            "ip-refused",
            "no-zone",
            "token-refused",
            "adding-refused",
            "removing-refused",
            "visible-at-once",
            "visible-late",
            "never-visible",
            "proved",
        ]);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        run(0..64, |seed| {
            let mut rng = Rng::seeded(seed);
            let fails = *rng.pick(&[
                Fails::Never,
                Fails::Never,
                Fails::Never,
                Fails::Token,
                Fails::Adding,
                Fails::Removing,
            ]);
            let lag = *rng.pick(&[Some(0), Some(0), Some(3), None]);
            let pool = [
                ("example.com", "apex"),
                ("www.example.com", "subdomain"),
                ("a.b.example.com", "subdomain"),
                ("leader.dept.example.com", "nested-zone"),
                ("*.example.com", "wildcard"),
                ("192.0.2.1", "ip-refused"),
                ("leader.example.org", "no-zone"),
            ];
            let names: Vec<(&str, &str)> = (0..1 + rng.below(3)).map(|_| *rng.pick(&pool)).collect();
            runtime.block_on(async {
                let cloudflare = FakeCloudflare::start(&["example.com", "dept.example.com"], fails).await;
                let token = if fails == Fails::Token { "another-token" } else { TOKEN };
                let publishing = publishing(&cloudflare, token, lag);
                let mut outcome = Ok(());
                let mut proved: Vec<(String, String)> = Vec::new();
                for (i, (name, case)) in names.iter().enumerate() {
                    let value = format!("value-{i}");
                    outcome = publishing.dns(name, &value).await;
                    let Err(e) = &outcome else {
                        coverage.reach(*case);
                        if proved.iter().any(|(n, _)| record_name(n) == record_name(name)) {
                            coverage.reach("shared-record");
                        }
                        proved.push((name.to_string(), value));
                        continue;
                    };
                    let expected = match (*case, fails, lag) {
                        ("ip-refused", _, _) => "cannot prove an IP address",
                        (_, Fails::Token, _) => "Authentication error",
                        ("no-zone", _, _) => "holds no zone for leader.example.org that",
                        (_, Fails::Adding, _) => "DNS Validation Error",
                        (_, _, None) => "ns1.test: timed out",
                        _ => return Err(format!("{name} failed: {e}")),
                    };
                    if !e.contains(expected) {
                        return Err(format!("{name} failed for another reason than {expected:?}: {e}"));
                    }
                    coverage.reach(match expected {
                        "Authentication error" => "token-refused",
                        "DNS Validation Error" => "adding-refused",
                        "ns1.test: timed out" => "never-visible",
                        _ => case,
                    });
                    break;
                }
                // Each record went to the zone that holds its name, as the name it is asked at.
                let added = cloudflare.held.lock().unwrap().added.clone();
                for (name, value) in &proved {
                    if !added.contains(&(record_name(name), value.clone())) {
                        return Err(format!("{name} was proved by no record {added:?}"));
                    }
                }
                if outcome.is_ok() {
                    coverage.reach("proved");
                    coverage.reach(if lag == Some(0) {
                        "visible-at-once"
                    } else {
                        "visible-late"
                    });
                }
                publishing.done().await;
                let left = cloudflare.records();
                if fails == Fails::Removing {
                    if left.len() != added.len() {
                        return Err(format!("removing failed, yet {left:?} is left of {added:?}"));
                    }
                    if !added.is_empty() {
                        coverage.reach("removing-refused");
                    }
                } else if !left.is_empty() {
                    return Err(format!("records left after the order: {left:?}"));
                }
                Ok(())
            })
        });
        coverage.assert_complete();
    }

    /// A record already there with the same value, as one an earlier or a running order left, is refused by
    /// Cloudflare, and the order says which record to delete. The record is not this order's to remove.
    #[test]
    fn a_record_already_there_says_which_to_delete() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let cloudflare = FakeCloudflare::start(&["example.com"], Fails::Never).await;
            let earlier = publishing(&cloudflare, TOKEN, Some(0));
            earlier.dns("www.example.com", "the-value").await.unwrap();
            let again = publishing(&cloudflare, TOKEN, Some(0));
            let e = again.dns("www.example.com", "the-value").await.unwrap_err();
            assert!(
                e.starts_with("Cloudflare already holds the TXT record _acme-challenge.www.example.com with the value"),
                "{e}"
            );
            again.done().await;
            assert_eq!(cloudflare.records().len(), 1, "the earlier order's record stays");
        });
    }

    /// The real nameserver check, against a stand-in nameserver on loopback that answers what it holds, and
    /// refuses a query that asks it to recurse.
    #[test]
    fn a_nameserver_is_asked_directly_for_the_record() {
        use hickory_resolver::proto::op::{Message, ResponseCode};
        use hickory_resolver::proto::rr::rdata::TXT;
        use hickory_resolver::proto::rr::{Name, Record as Answer, RecordType};

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let at = socket.local_addr().unwrap();
            let held: HashMap<&str, Vec<Vec<&str>>> = HashMap::from([
                (
                    "_acme-challenge.a.example.com.",
                    vec![vec!["other"], vec!["the-", "value"]],
                ),
                ("_acme-challenge.b.example.com.", vec![vec!["other"]]),
                ("_acme-challenge.c.example.com.", vec![]),
            ]);
            tokio::spawn(async move {
                let mut buffer = [0u8; 1500];
                loop {
                    let (n, from) = socket.recv_from(&mut buffer).await.unwrap();
                    let asked = Message::from_vec(&buffer[..n]).unwrap();
                    let mut answer = Message::response(asked.metadata.id, asked.metadata.op_code);
                    answer.metadata.authoritative = true;
                    answer.add_queries(asked.queries.clone());
                    let query = &asked.queries[0];
                    let name = query.name().to_ascii();
                    match held.get(name.as_str()) {
                        _ if asked.metadata.recursion_desired => answer.metadata.response_code = ResponseCode::Refused,
                        _ if query.query_type() != RecordType::TXT => {}
                        Some(texts) => {
                            for text in texts {
                                let data = TXT::from_bytes(text.iter().map(|t| t.as_bytes()).collect());
                                answer.add_answer(Answer::from_rdata(
                                    Name::from_ascii(&name).unwrap(),
                                    60,
                                    RData::TXT(data),
                                ));
                            }
                        }
                        None => answer.metadata.response_code = ResponseCode::NXDomain,
                    }
                    socket.send_to(&answer.to_vec().unwrap(), from).await.unwrap();
                }
            });
            let record = |name: &str| Record {
                zone: "example.com".to_string(),
                zone_id: String::new(),
                name: name.to_string(),
                value: "the-value".to_string(),
                id: String::new(),
            };
            // Split into character strings the way a long value is, and among others at the name.
            assert_eq!(answers(at, &record("_acme-challenge.a.example.com")).await, Ok(true));
            assert_eq!(answers(at, &record("_acme-challenge.b.example.com")).await, Ok(false));
            assert_eq!(answers(at, &record("_acme-challenge.c.example.com")).await, Ok(false));
            assert_eq!(answers(at, &record("_acme-challenge.d.example.com")).await, Ok(false));
            // A nameserver answers at any one address this host reaches, as one with no IPv6 route reaches none
            // of its IPv6 ones; an error only when it is reached at none.
            let unreached: SocketAddr = "[2001:db8::1]:53".parse().unwrap();
            let a = record("_acme-challenge.a.example.com");
            assert_eq!(one_answers("ns1.test", &[unreached, at], &a).await, Ok(true));
            let e = one_answers("ns1.test", &[unreached], &a).await.unwrap_err();
            assert!(e.starts_with("ns1.test: 2001:db8::1: "), "{e}");
        });
    }
}
