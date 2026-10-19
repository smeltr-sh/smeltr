//! The Elasticsearch destination against a *real* cluster.
//!
//! `tests/elastic-tests.rs` covers response handling exhaustively against
//! canned bodies, and says why that needs no live cluster. This file is
//! the other direction, and only the part a stub cannot reach: whether a
//! real Elasticsearch **accepts what we send** and whether its **own**
//! error shapes classify the way the canned ones do.
//!
//! A stub answers whatever it was told to answer, so it agrees with our
//! request format by construction. Every assertion here is about the
//! contract with the server: the bulk body is well-formed, `create`
//! really refuses a duplicate, `index` really replaces one, an index
//! override really routes, and a mapping conflict really comes back as a
//! rejection rather than as a transient failure that would retry forever.
//!
//! Point it at `ES_TEST_URL`. Under `SMELTR_LIVE_TESTS=required` a
//! missing one fails rather than skips -- see `tests/live/mod.rs`.

use crate::live;

use std::time::Duration;

use smeltr::io::elastic::{self, Elastic};
use smeltr::io::http::{HttpDestination, HttpTransport};
use smeltr::io::{Destination, IoError, OutgoingRecord, OwnedRecord, Rejection};
use std::borrow::Cow;

/// Hand `rows` over as one batch, posted and answered before this returns.
fn deliver(
    d: &mut HttpDestination<Elastic, HttpTransport>,
    rows: &[OutgoingRecord<'_>],
) -> Result<Vec<Rejection>, IoError> {
    let rows: Vec<OwnedRecord> = rows.iter().map(OwnedRecord::from).collect();
    Ok(d.deliver(0, Cow::Owned(rows))?.1)
}
use smeltr::plan::topology::DocIdPolicy;

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn dest(url: &str, index: &str, doc_id: DocIdPolicy) -> HttpDestination<Elastic, HttpTransport> {
    // No identity: this Elastic cluster is plain http, and what a transport presents is nothing.
    let config = smeltr::io::http::agent_config(None, &Default::default()).expect("an agent with no identity");
    HttpDestination::new(
        elastic::transport(url, None, &config),
        Elastic::new(index).with_doc_id_policy(doc_id),
    )
    .with_stall_after(Duration::from_secs(30))
}

/// Make everything indexed so far searchable. Elasticsearch refreshes on
/// its own schedule, so reading back without this races the refresh
/// rather than testing delivery.
fn refresh(url: &str, index: &str) {
    let _ = ureq::post(format!("{url}/{index}/_refresh"))
        .send_empty()
        .map(|mut r| r.body_mut().read_to_string());
}

/// How many documents `index` holds.
fn count(url: &str, index: &str) -> u64 {
    refresh(url, index);
    let body: serde_json::Value = ureq::get(format!("{url}/{index}/_count"))
        .call()
        .expect("counting documents")
        .body_mut()
        .read_json()
        .expect("a count response");
    body["count"].as_u64().unwrap_or_default()
}

/// The `_source` of one document by id.
fn source(url: &str, index: &str, id: &str) -> serde_json::Value {
    refresh(url, index);
    let body: serde_json::Value = ureq::get(format!("{url}/{index}/_doc/{id}"))
        .call()
        .expect("fetching a document")
        .body_mut()
        .read_json()
        .expect("a get response");
    body["_source"].clone()
}

/// The bulk body reaches a real cluster and the documents are really
/// there afterward. A canned response cannot fail this: it agrees with
/// whatever we sent.
#[test]
fn documents_sent_to_a_real_cluster_are_really_indexed() {
    let Some(url) = live::elastic("documents_sent_to_a_real_cluster_are_really_indexed") else {
        return;
    };
    let index = unique("smeltr-live");
    let mut d = dest(&url, &index, DocIdPolicy::Auto);

    let bodies: Vec<String> = (0..3).map(|i| format!(r#"{{"n":{i},"kind":"live"}}"#)).collect();
    let rows: Vec<OutgoingRecord<'_>> = bodies.iter().map(|b| OutgoingRecord::new(b.as_bytes())).collect();
    deliver(&mut d, &rows).expect("a real cluster accepts the bulk body");

    assert_eq!(count(&url, &index), 3, "every document landed");
}

/// `Dedupe` writes `create`, so a re-send of the same identity is refused
/// by the cluster and the first copy is kept. This is the guarantee that
/// makes an at-least-once redelivery survivable, and it depends entirely
/// on Elasticsearch's own behavior rather than on ours.
#[test]
fn dedupe_keeps_the_first_copy_when_a_batch_is_redelivered() {
    let Some(url) = live::elastic("dedupe_keeps_the_first_copy_when_a_batch_is_redelivered") else {
        return;
    };
    let index = unique("smeltr-dedupe");
    let id = "stable-identity";

    let mut d = dest(&url, &index, DocIdPolicy::Dedupe);
    deliver(
        &mut d,
        &[OutgoingRecord::new(br#"{"attempt":"first"}"#).with_doc_id(Some(id))],
    )
    .unwrap();

    // The same batch again, exactly as a redelivery after a crash between
    // flush and commit would send it.
    let mut again = dest(&url, &index, DocIdPolicy::Dedupe);
    deliver(
        &mut again,
        &[OutgoingRecord::new(br#"{"attempt":"second"}"#).with_doc_id(Some(id))],
    )
    .expect("a version conflict counts as delivered under dedupe");

    assert_eq!(count(&url, &index), 1, "one document, not two");
    assert_eq!(
        source(&url, &index, id)["attempt"],
        serde_json::json!("first"),
        "the first copy is the one kept"
    );
}

/// `Overwrite` writes `index`, so the same identity replaces what was
/// there. The opposite of the above, and the reason the two are separate
/// named policies rather than one flag.
#[test]
fn overwrite_replaces_the_document_with_the_same_identity() {
    let Some(url) = live::elastic("overwrite_replaces_the_document_with_the_same_identity") else {
        return;
    };
    let index = unique("smeltr-overwrite");
    let id = "stable-identity";

    for attempt in ["first", "second"] {
        let mut d = dest(&url, &index, DocIdPolicy::Overwrite);
        let body = format!(r#"{{"attempt":"{attempt}"}}"#);
        deliver(&mut d, &[OutgoingRecord::new(body.as_bytes()).with_doc_id(Some(id))]).unwrap();
    }

    assert_eq!(count(&url, &index), 1, "still one document");
    assert_eq!(
        source(&url, &index, id)["attempt"],
        serde_json::json!("second"),
        "the later copy wins"
    );
}

/// A row's `__destination.index` names the index for this destination, and the
/// cluster really routes it there rather than to the configured default.
#[test]
fn an_index_override_routes_to_a_different_real_index() {
    let Some(url) = live::elastic("an_index_override_routes_to_a_different_real_index") else {
        return;
    };
    let default_index = unique("smeltr-default");
    let other_index = unique("smeltr-override");

    let mut d = dest(&url, &default_index, DocIdPolicy::Auto);
    deliver(
        &mut d,
        &[
            OutgoingRecord::new(br#"{"where":"default"}"#),
            OutgoingRecord::new(br#"{"where":"override"}"#).with_placement(Some(&other_index)),
        ],
    )
    .unwrap();

    assert_eq!(count(&url, &default_index), 1);
    assert_eq!(count(&url, &other_index), 1);
}

/// The one that matters most, and the one a canned response can only
/// assume: a document a real cluster refuses on its own terms.
///
/// The index is created with a strict mapping, so a document carrying an
/// undeclared field is refused with a real `strict_dynamic_mapping_
/// exception`. That has to classify as a *rejection* --- retrying it is a
/// wedged partition, since the cluster will refuse it identically every
/// time --- and the rest of the batch has to land regardless.
#[test]
fn a_document_a_real_cluster_refuses_is_a_rejection_and_the_batch_survives() {
    let name = "a_document_a_real_cluster_refuses_is_a_rejection_and_the_batch_survives";
    let Some(url) = live::elastic(name) else {
        return;
    };
    let index = unique("smeltr-strict");

    ureq::put(format!("{url}/{index}"))
        .send_json(serde_json::json!({
            "mappings": { "dynamic": "strict", "properties": { "known": { "type": "keyword" } } }
        }))
        .expect("creating a strictly-mapped index")
        .body_mut()
        .read_to_string()
        .ok();

    let mut d = dest(&url, &index, DocIdPolicy::Auto);
    // Not an error: one refused document must not fail the hand-over, or
    // the whole batch stays uncommitted behind a document that will never
    // be accepted.
    let rejections = deliver(
        &mut d,
        &[
            OutgoingRecord::new(br#"{"known":"fine"}"#),
            OutgoingRecord::new(br#"{"unmapped":"refused"}"#),
            OutgoingRecord::new(br#"{"known":"also-fine"}"#),
        ],
    )
    .expect("a refused document is not a delivery failure");

    assert_eq!(rejections.len(), 1, "exactly one refusal: {rejections:?}");
    assert_eq!(rejections[0].doc, 1, "attributed to the row that caused it");
    assert!(
        rejections[0].reason.contains("strict_dynamic_mapping_exception") || rejections[0].reason.contains("mapping"),
        "the cluster's own reason is carried through: {}",
        rejections[0].reason
    );

    assert_eq!(count(&url, &index), 2, "the two good documents landed anyway");
}

/// How many bulk requests each node has taken, by the address it publishes.
fn bulks_by_node(url: &str) -> std::collections::BTreeMap<String, u64> {
    let body: serde_json::Value = ureq::get(format!("{url}/_nodes/stats/http"))
        .call()
        .expect("reading the nodes' stats")
        .body_mut()
        .read_json()
        .expect("a stats response");
    let addresses: serde_json::Value = ureq::get(format!("{url}/_nodes/http"))
        .call()
        .expect("reading the nodes")
        .body_mut()
        .read_json()
        .expect("a nodes response");
    (body["nodes"].as_object().into_iter().flatten())
        .map(|(id, node)| {
            let published = addresses["nodes"][id]["http"]["publish_address"]
                .as_str()
                .unwrap_or_default();
            let port = published.rsplit_once(':').map(|(_, p)| p).unwrap_or_default();
            let taken = node["http"]["routes"]["/_bulk"]["requests"]["count"]
                .as_u64()
                .unwrap_or_default();
            (format!("http://localhost:{port}"), taken)
        })
        .collect()
}

/// A destination that sniffs learns the real cluster's two nodes from `_nodes/http`, spreads its bulk requests
/// across them, and keeps to its own zone's node when it names a zone. `tests/live/compose.live.yaml` puts its two nodes in
/// zones `z1` and `z2`, at ports 9200 and 9201.
#[test]
fn sniffing_spreads_requests_across_a_real_clusters_nodes() {
    use smeltr::io::elastic_nodes::{Nodes, Sniffing};
    use smeltr::io::http::{Protocol, Transport};
    use smeltr::plan::topology::Sniff;

    let Some(url) = live::elastic("sniffing_spreads_requests_across_a_real_clusters_nodes") else {
        return;
    };
    let url = url.trim_end_matches('/').to_string();
    let index = unique("smeltr-live-sniff");
    let (first, second) = ("http://localhost:9200".to_string(), "http://localhost:9201".to_string());
    let send = |zone: Option<&str>, requests: usize| {
        let config = smeltr::io::http::agent_config(None, &Default::default()).expect("an agent with no identity");
        let sniff = Sniff {
            zone_attribute: Sniff::DEFAULT_ZONE_ATTRIBUTE.to_string(),
            zone: zone.map(str::to_string),
        };
        let nodes = Nodes::new(&url, &sniff, None, &config, "es".to_string());
        let mut transport = Sniffing::new(nodes.clone(), elastic::transport(&url, None, &config));
        let before = bulks_by_node(&url);
        let body = format!("{{\"index\":{{\"_index\":\"{index}\"}}}}\n{{\"n\":1}}\n");
        for _ in 0..requests {
            let sent = transport.post(Elastic::new(&index).path(), body.as_bytes(), Duration::from_secs(30));
            assert_eq!(sent.map(|r| r.status).map_err(|e| e.to_string()), Ok(200));
        }
        let known: Vec<(String, bool)> = nodes.known().into_iter().map(|n| (n.url, n.in_zone)).collect();
        let after = bulks_by_node(&url);
        let taken = |node: &String| {
            after.get(node).copied().unwrap_or_default() - before.get(node).copied().unwrap_or_default()
        };
        (known, taken(&first), taken(&second))
    };

    // No zone: both nodes, in turn. Other tests send to 9200 too, so only 9201's count is exact.
    let (known, at_first, at_second) = send(None, 6);
    assert_eq!(known, vec![(first.clone(), false), (second.clone(), false)]);
    assert!(at_first >= 3, "9200 took {at_first}");
    assert_eq!(at_second, 3);

    // A worker in z1 sends only to the node in z1.
    let (known, at_first, at_second) = send(Some("z1"), 4);
    assert_eq!(known, vec![(first.clone(), true), (second.clone(), false)]);
    assert!(at_first >= 4, "9200 took {at_first}");
    assert_eq!(at_second, 0);

    assert_eq!(count(&url, &index), 10, "every document landed");
}
