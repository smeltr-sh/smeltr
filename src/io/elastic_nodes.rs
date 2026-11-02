//! Sniffing: what an Elasticsearch destination's senders learned of the cluster's nodes, shared among them.
//!
//! The list is learned from `url` with `GET _nodes/http` and learned again every [`REFRESH_EVERY`], and sooner once
//! every node it holds has failed. A request goes to the next live node in turn, preferring the worker's own zone's;
//! a node that does not answer is passed over for [`DEAD_FOR`]. With no node to send to, a request goes to `url`, so
//! sniffing never holds a row back that `url` alone would have delivered.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::Value;

use super::IoError;
use super::http::{HttpTransport, Response, Transport};
use crate::logs::Relog;
use crate::plan::topology::Sniff;

/// How often a working list is learned again. The default of Elastic's own clients.
const REFRESH_EVERY: Duration = Duration::from_secs(5 * 60);

/// How long a node that did not answer is passed over.
const DEAD_FOR: Duration = Duration::from_secs(60);

/// The least time between lookups that losing every node starts: requests go to `url` meanwhile, and a lookup per
/// request would weigh on a cluster already in trouble.
const RELEARN_FLOOR: Duration = Duration::from_secs(10);

/// The most one lookup of one address may take: it holds up the request that ran it, and never past that request's
/// deadline.
const LOOKUP_BUDGET: Duration = Duration::from_secs(10);

/// The most a connection to a node may take to open. A node the cluster lists may be one a worker cannot reach, and
/// a request moves on from it to the next node rather than wait out its budget.
pub const CONNECT_WITHIN: Duration = Duration::from_secs(5);

/// Roles that take no request a destination sends. A node holding only these is a dedicated master.
const MASTER_ONLY: &[&str] = &["master", "voting_only"];

/// One node learned of the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    /// Its base address: the scheme of `url`, the host and port it publishes.
    pub url: String,
    /// Whether its zone attribute names the worker's zone.
    pub in_zone: bool,
}

/// The nodes one destination's senders share.
pub struct Nodes {
    url: String,
    sniff: Sniff,
    auth: Option<String>,
    agent: ureq::Agent,
    /// The destination, as its warnings name it.
    subject: String,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    nodes: Vec<Live>,
    /// The next node in turn.
    next: usize,
    /// When a lookup last ran, landed or not.
    looked: Option<Instant>,
    /// A sender is looking up now; the others go on with what is known.
    looking: bool,
    /// Sniffing never worked: said once per change and on the interval, until it does.
    failing: Relog,
    /// A refresh failing while nodes are known: said the same way, cleared by one that lands.
    refreshing: Relog,
}

struct Live {
    node: Node,
    dead_until: Option<Instant>,
}

impl Live {
    fn alive(&self, now: Instant) -> bool {
        self.dead_until.is_none_or(|at| at <= now)
    }
}

impl Nodes {
    /// `url` is the destination's, after the worker took its zone's. `config` is the destination's agent
    /// config, so a lookup presents and trusts what a request does.
    pub fn new(
        url: &str,
        sniff: &Sniff,
        auth: Option<String>,
        config: &ureq::config::Config,
        subject: String,
    ) -> Arc<Nodes> {
        Arc::new(Nodes {
            url: url.trim_end_matches('/').to_string(),
            sniff: sniff.clone(),
            auth,
            agent: config.new_agent(),
            subject,
            state: Mutex::default(),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The address the next request goes to, learning the nodes first when that is due and `deadline` allows.
    pub fn pick(&self, deadline: Instant) -> String {
        self.learn_if_due(deadline);
        let mut state = self.state();
        let now = Instant::now();
        if let Some(why) = state.failing.due() {
            tracing::warn!(
                target: "elasticsearch.sniff_failed",
                subject = %self.subject,
                error = %why,
                "an Elasticsearch destination could not learn the cluster's nodes, so it sends to the URL it was given"
            );
        }
        let alive: Vec<usize> = (0..state.nodes.len()).filter(|&i| state.nodes[i].alive(now)).collect();
        let in_zone: Vec<usize> = alive.iter().copied().filter(|&i| state.nodes[i].node.in_zone).collect();
        let from = if in_zone.is_empty() { alive } else { in_zone };
        if from.is_empty() {
            return self.url.clone();
        }
        let at = from[state.next % from.len()];
        state.next = state.next.wrapping_add(1);
        state.nodes[at].node.url.clone()
    }

    /// Passes over `url` for [`DEAD_FOR`]: a request sent there had no answer.
    fn failed(&self, url: &str) {
        let mut state = self.state();
        let until = Instant::now() + DEAD_FOR;
        for live in state.nodes.iter_mut().filter(|l| l.node.url == url) {
            if live.dead_until.is_none_or(|at| at <= Instant::now()) {
                tracing::debug!(target: "elasticsearch.node_passed_over", subject = %self.subject, node = %url, for_secs = DEAD_FOR.as_secs(), "a node did not answer, so it is passed over for a while");
            }
            live.dead_until = Some(until);
        }
    }

    /// How many addresses a request may try: every node, then `url`.
    fn tries(&self) -> usize {
        self.state().nodes.len() + 1
    }

    /// The nodes known now.
    pub fn known(&self) -> Vec<Node> {
        self.state().nodes.iter().map(|l| l.node.clone()).collect()
    }

    fn learn_if_due(&self, deadline: Instant) {
        let addresses = {
            let mut state = self.state();
            let now = Instant::now();
            // Learned again sooner while it fails, so the warning stands (`logs::Relog`) for as long as it does.
            let every = if state.failing.why().is_some() {
                crate::logs::RELOG_EVERY
            } else {
                REFRESH_EVERY
            };
            let all_dead = !state.nodes.is_empty() && !state.nodes.iter().any(|l| l.alive(now));
            let due = state
                .looked
                .is_none_or(|at| at.elapsed() >= every || (all_dead && at.elapsed() >= RELEARN_FLOOR));
            if !due || state.looking || Instant::now() >= deadline {
                return;
            }
            state.looking = true;
            // `url` first; a node already known answers when `url` names one that is gone.
            std::iter::once(self.url.clone())
                .chain(state.nodes.iter().map(|l| l.node.url.clone()))
                .collect::<Vec<_>>()
        };
        let mut learned = Err(String::new());
        for address in addresses.iter().take(3) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            learned = self.look_up(address, left.min(LOOKUP_BUDGET));
            if learned.is_ok() {
                break;
            }
        }
        let mut state = self.state();
        state.looking = false;
        state.looked = Some(Instant::now());
        match learned {
            Ok(nodes) => {
                state.failing.clear();
                if state.refreshing.clear() {
                    tracing::info!(target: "elasticsearch.refresh_recovered", subject = %self.subject, "an Elasticsearch destination learns the cluster's nodes again");
                }
                let before: Vec<&Node> = state.nodes.iter().map(|l| &l.node).collect();
                if before != nodes.iter().collect::<Vec<_>>() {
                    tracing::info!(
                        target: "elasticsearch.nodes_learned",
                        subject = %self.subject,
                        nodes = nodes.len(),
                        in_zone = nodes.iter().filter(|n| n.in_zone).count(),
                        "an Elasticsearch destination learned the cluster's nodes"
                    );
                }
                // A node learned again is still passed over until its time is out: the cluster lists a node it
                // cannot yet tell is gone.
                let dead_until = |url: &str| {
                    state
                        .nodes
                        .iter()
                        .find(|l| l.node.url == url)
                        .and_then(|l| l.dead_until)
                };
                state.nodes = (nodes.into_iter())
                    .map(|node| Live {
                        dead_until: dead_until(&node.url),
                        node,
                    })
                    .collect();
            }
            // With no node known, sniffing has never worked, which is the admin's to fix. A refresh that fails
            // leaves the nodes already learned taking requests.
            Err(why) if state.nodes.is_empty() => state.failing.fail(why),
            Err(why) => {
                if let Some(error) = state.refreshing.fail_due(why) {
                    tracing::warn!(
                        target: "elasticsearch.refresh_failed",
                        subject = %self.subject,
                        error,
                        "an Elasticsearch destination could not refresh the cluster's nodes, so it sends to those it knew"
                    );
                }
            }
        }
    }

    /// The nodes `address` says the cluster has.
    fn look_up(&self, address: &str, budget: Duration) -> Result<Vec<Node>, String> {
        let mut request = self
            .agent
            .get(format!(
                "{address}/_nodes/http?filter_path=nodes.*.http.publish_address,nodes.*.roles,nodes.*.attributes"
            ))
            .config()
            .timeout_global(Some(budget))
            .timeout_connect(Some(CONNECT_WITHIN))
            // The credential goes with the request, and must not follow a redirect to another host.
            .max_redirects(0)
            .http_status_as_error(false)
            .build();
        if let Some(auth) = &self.auth {
            request = request.header("Authorization", auth);
        }
        let mut response = request.call().map_err(|e| format!("GET _nodes/http failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            .limit(super::http::MAX_ANSWER_BYTES)
            .read_to_string()
            .map_err(|e| format!("GET _nodes/http: HTTP {status}: could not read the answer: {e}"))?;
        match status {
            200 => {}
            401 | 403 => {
                return Err(format!(
                    "GET _nodes/http answered {status}: its credential needs the cluster privilege `monitor`"
                ));
            }
            _ => {
                return Err(format!(
                    "GET _nodes/http answered {status}: {}",
                    super::discovery::said(body.as_bytes())
                ));
            }
        }
        let nodes = parse(&body, scheme(&self.url), &self.sniff)?;
        if nodes.is_empty() {
            return Err("GET _nodes/http named no node that takes requests".to_string());
        }
        Ok(nodes)
    }
}

fn scheme(url: &str) -> &str {
    url.split_once("://").map_or("http", |(scheme, _)| scheme)
}

/// The nodes a `_nodes/http` answer names, but dedicated masters, in address order. A node publishes
/// `host/ip:port` when it knows its name, and is reached by the name, which a certificate is likelier to cover.
fn parse(body: &str, scheme: &str, sniff: &Sniff) -> Result<Vec<Node>, String> {
    let answer: Value =
        serde_json::from_str(body).map_err(|e| format!("GET _nodes/http answered what is not JSON: {e}"))?;
    let mut nodes: Vec<Node> = (answer["nodes"].as_object().into_iter().flatten())
        .filter(|(_, node)| {
            let roles: Vec<&str> = (node["roles"].as_array().into_iter().flatten())
                .filter_map(Value::as_str)
                .collect();
            roles.is_empty() || roles.iter().any(|r| !MASTER_ONLY.contains(r))
        })
        .filter_map(|(_, node)| {
            let published = node["http"]["publish_address"].as_str()?;
            let address = match published.split_once('/') {
                Some((host, ip)) if !host.is_empty() => format!("{host}:{}", ip.rsplit_once(':')?.1),
                Some((_, ip)) => ip.to_string(),
                None => published.to_string(),
            };
            let zone = node["attributes"][sniff.zone_attribute.as_str()]
                .as_str()
                .map(str::trim);
            Some(Node {
                url: format!("{scheme}://{address}"),
                in_zone: zone.is_some() && zone == sniff.zone.as_deref(),
            })
        })
        .collect();
    nodes.sort_by(|a, b| a.url.cmp(&b.url));
    nodes.dedup();
    Ok(nodes)
}

/// A sender's transport over the shared [`Nodes`]: each request goes to the next node, and on to the one after
/// when a node does not answer.
pub struct Sniffing {
    nodes: Arc<Nodes>,
    transport: HttpTransport,
}

impl Sniffing {
    pub fn new(nodes: Arc<Nodes>, transport: HttpTransport) -> Self {
        Sniffing {
            nodes,
            transport: transport.with_connect_within(CONNECT_WITHIN),
        }
    }
}

impl Transport for Sniffing {
    fn post(&mut self, path: &str, body: &[u8], budget: Duration) -> Result<Response, IoError> {
        let deadline = Instant::now() + budget;
        let mut tries = None;
        loop {
            let url = self.nodes.pick(deadline);
            // Counted after the first pick, which may be the one that learns the nodes.
            let tries = tries.get_or_insert_with(|| self.nodes.tries());
            self.transport.rebase(&url);
            let left = deadline.saturating_duration_since(Instant::now());
            match self.transport.post(path, body, left) {
                Err(e) if !e.is_oversized() => {
                    self.nodes.failed(&url);
                    *tries -= 1;
                    if *tries == 0 || deadline <= Instant::now() {
                        return Err(e);
                    }
                }
                answered => return answered,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nodes whose every lookup fails: `url` names a port nothing listens on.
    fn unlearnable() -> Arc<Nodes> {
        let gone = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", gone.local_addr().unwrap());
        drop(gone);
        let config = crate::io::http::agent_config(None, &Default::default()).unwrap();
        Nodes::new(&url, &sniff(None), None, &config, "es".to_string())
    }

    #[test]
    fn a_lookup_that_never_landed_warns() {
        let nodes = unlearnable();
        nodes.learn_if_due(Instant::now() + LOOKUP_BUDGET);
        assert!(nodes.state().failing.why().is_some());
    }

    #[test]
    fn a_failed_refresh_keeps_the_nodes_it_knew_and_does_not_warn() {
        let nodes = unlearnable();
        {
            let mut state = nodes.state();
            state.nodes = vec![Live {
                node: Node {
                    url: "http://es-a.internal:9200".to_string(),
                    in_zone: false,
                },
                dead_until: None,
            }];
            state.looked = Some(Instant::now() - REFRESH_EVERY);
        }
        nodes.learn_if_due(Instant::now() + LOOKUP_BUDGET);
        assert!(nodes.state().failing.why().is_none());
        assert_eq!(nodes.pick(Instant::now()), "http://es-a.internal:9200");
    }

    fn sniff(zone: Option<&str>) -> Sniff {
        Sniff {
            zone_attribute: "zone".to_string(),
            zone: zone.map(str::to_string),
        }
    }

    const ANSWER: &str = r#"{"nodes": {
        "a": {"roles": ["data", "ingest"], "attributes": {"zone": "us-east-1a"},
              "http": {"publish_address": "es-a.internal/10.0.0.1:9200"}},
        "b": {"roles": ["data_hot"], "attributes": {"zone": "us-east-1b"},
              "http": {"publish_address": "10.0.0.2:9200"}},
        "c": {"roles": ["master"], "attributes": {"zone": "us-east-1a"},
              "http": {"publish_address": "10.0.0.3:9200"}},
        "d": {"roles": [], "http": {"publish_address": "/[::1]:9201"}}
    }}"#;

    #[test]
    fn a_node_is_reached_by_the_name_it_publishes_and_a_dedicated_master_is_left_out() {
        let nodes = parse(ANSWER, "https", &sniff(Some("us-east-1a"))).unwrap();
        assert_eq!(
            nodes,
            vec![
                Node {
                    url: "https://10.0.0.2:9200".into(),
                    in_zone: false
                },
                Node {
                    url: "https://[::1]:9201".into(),
                    in_zone: false
                },
                Node {
                    url: "https://es-a.internal:9200".into(),
                    in_zone: true
                },
            ]
        );
    }

    #[test]
    fn a_worker_with_no_zone_has_no_node_in_its_zone() {
        let nodes = parse(ANSWER, "http", &sniff(None)).unwrap();
        assert!(nodes.iter().all(|n| !n.in_zone), "{nodes:?}");
    }

    #[test]
    fn an_answer_that_is_not_json_is_an_error() {
        assert!(parse("<html>", "http", &sniff(None)).is_err());
    }
}
