//! The Splunk HEC destination against a *real* Splunk.
//!
//! `tests/splunk-tests.rs` covers the protocol against a stub, which answers whatever it was told to and so agrees
//! with our requests by construction. This file is the contract with Splunk itself: the envelope is accepted, `time`
//! is the time an event is filed under, indexer acknowledgement really confirms, and an event HEC cannot take comes
//! back refused alone while those after it land.
//!
//! Point it at `SPLUNK_TEST_HEC` and the rest of `tests/live/mod.rs`'s `Splunk`. Under `SMELTR_LIVE_TESTS=required`
//! a missing one fails rather than skips.

use crate::live;

use std::time::{Duration, Instant};

use std::borrow::Cow;

use smeltr::format::bytes_text::BytesForm;
use smeltr::io::{Destination, Dispatched, IoError, OwnedRecord, Rejection};
use smeltr::plan::topology::{
    Acknowledgement, DestinationKind, Endpoint, HeldSecrets, SecretRef, SplunkHecDestinationConfig,
};

/// A marker no earlier run wrote, so a search sees this test's events alone.
fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn build(s: &live::Splunk, acknowledgement: Acknowledgement, token: &str) -> Box<dyn Destination> {
    open(config(s, acknowledgement), token)
}

fn config(s: &live::Splunk, acknowledgement: Acknowledgement) -> SplunkHecDestinationConfig {
    SplunkHecDestinationConfig {
        payload: smeltr::plan::config::Payload::Record,
        zone_urls: Default::default(),
        url: s.hec.clone(),
        token: SecretRef::LeaderStored {
            name: "hec".to_string(),
            version: 1,
        },
        index: Some("main".to_string()),
        sourcetype: Some("smeltr:live".to_string()),
        source: None,
        host: None,
        endpoint: Endpoint::Event,
        acknowledgement,
        tls: None,
        in_flight: 2,
        max_request_bytes: 1024 * 1024,
        buffer_bytes: 4 * 1024 * 1024,
        stall_after: Duration::from_secs(60),
    }
}

fn open(config: SplunkHecDestinationConfig, token: &str) -> Box<dyn Destination> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("hec".to_string(), 1, token.to_string())]);
    let metrics = smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "live");
    smeltr::io::build_destination(&DestinationKind::SplunkHec(config), 0, "d", metrics, &secrets, None).expect("built")
}

/// Hands `rows` over as one batch and waits for its outcome.
fn deliver(d: &mut dyn Destination, rows: Vec<OwnedRecord>) -> Result<Vec<Rejection>, IoError> {
    let (dispatched, mut refused) = d.deliver(1, Cow::Owned(rows))?;
    if dispatched == Dispatched::Complete {
        return Ok(refused);
    }
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(c) = d.poll_completions()?.into_iter().find(|c| c.batch == 1) {
            refused.extend(c.rejections);
            return Ok(refused);
        }
        assert!(Instant::now() < deadline, "no completion");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn row(value: &str) -> OwnedRecord {
    OwnedRecord {
        value: value.as_bytes().to_vec(),
        payload: smeltr::plan::config::Payload::Record,
        ..OwnedRecord::default()
    }
}

/// The events Splunk holds for `run`: each one's `n` and the time it is filed under, in epoch seconds. Asked until
/// `expect` are there or a minute passes, since an event is searchable a moment after it is indexed.
fn landed(s: &live::Splunk, run: &str, expect: usize) -> Vec<(String, f64)> {
    let agent = ureq::Agent::new_with_defaults();
    let (user, password) = s.auth.split_once(':').expect("user:password");
    let auth = format!(
        "Basic {}",
        BytesForm::Base64.text(format!("{user}:{password}").as_bytes())
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let body = agent
            .post(format!("{}/services/search/jobs/export", s.api))
            .header("Authorization", &auth)
            .send_form([
                (
                    "search",
                    format!("search index=main run={run} | table n _time").as_str(),
                ),
                ("output_mode", "json"),
                ("earliest_time", "0"),
                ("time_format", "%s.%6N"),
            ])
            .expect("searching Splunk")
            .body_mut()
            .read_to_string()
            .unwrap();
        let found: Vec<(String, f64)> = body
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter_map(|v| {
                let r = v.get("result")?;
                Some((r["n"].as_str()?.to_string(), r["_time"].as_str()?.parse().ok()?))
            })
            .collect();
        if found.len() >= expect || Instant::now() > deadline {
            return found;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn event(run: &str, n: usize) -> String {
    format!(r#"{{"run":"{run}","n":"{n}"}}"#)
}

/// Events land, each filed under the `_time` it carries, to the microsecond, not the time Splunk received it.
#[test]
fn events_land_under_the_time_they_carry() {
    let Some(s) = live::splunk("events_land_under_the_time_they_carry") else {
        return;
    };
    let run = unique("time");
    let mut d = build(&s, Acknowledgement::Receipt, &s.token);
    let a = format!(r#"{{"_time":"2026-08-25T12:00:00.250000Z","run":"{run}","n":"0"}}"#);
    let refused = deliver(&mut *d, vec![row(&a), row(&event(&run, 1))]).expect("delivered");
    assert!(refused.is_empty(), "{refused:?}");

    let mut found = landed(&s, &run, 2);
    found.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(found.len(), 2, "{found:?}");
    assert_eq!(found[0].1, 1_787_659_200.25, "filed under the time it carried");
    assert!(found[1].1 > 1_787_659_200.25, "no time is the time Splunk received it");
}

/// The raw endpoint files each line by the sourcetype it is sent under; Splunk takes the time from the line itself.
#[test]
fn raw_lines_land_parsed_by_their_sourcetype() {
    let Some(s) = live::splunk("raw_lines_land_parsed_by_their_sourcetype") else {
        return;
    };
    let run = unique("raw");
    let config = SplunkHecDestinationConfig {
        payload: smeltr::plan::config::Payload::Record,
        endpoint: Endpoint::Raw,
        sourcetype: Some("_json".to_string()),
        ..config(&s, Acknowledgement::Indexer)
    };
    let mut d = open(config, &s.ack_token);
    let rows = (0..3).map(|n| row(&event(&run, n))).collect();
    assert!(deliver(&mut *d, rows).expect("acknowledged").is_empty());
    assert_eq!(landed(&s, &run, 3).len(), 3);
}

/// Under indexer acknowledgement the batch completes once Splunk says the events are indexed, and they are.
#[test]
fn indexer_acknowledgement_confirms_what_is_indexed() {
    let Some(s) = live::splunk("indexer_acknowledgement_confirms_what_is_indexed") else {
        return;
    };
    let run = unique("ack");
    let mut d = build(&s, Acknowledgement::Indexer, &s.ack_token);
    let events: Vec<String> = (0..20).map(|n| event(&run, n)).collect();
    let rows = events.iter().map(|e| row(e)).collect();
    assert!(deliver(&mut *d, rows).expect("acknowledged").is_empty());
    assert_eq!(landed(&s, &run, 20).len(), 20);
}

/// HEC stops at an event it cannot take. That one is refused alone; the events after it land, once each.
#[test]
fn an_event_splunk_cannot_take_is_refused_alone() {
    let Some(s) = live::splunk("an_event_splunk_cannot_take_is_refused_alone") else {
        return;
    };
    let run = unique("refused");
    let mut d = build(&s, Acknowledgement::Receipt, &s.token);
    let (a, c) = (event(&run, 0), event(&run, 2));
    // An empty record is a blank string event, which HEC refuses.
    let refused = deliver(&mut *d, vec![row(&a), row(""), row(&c)]).expect("a refusal is not a failure");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0].doc, 1);

    let found = landed(&s, &run, 3);
    let count = |n: &str| found.iter().filter(|(x, _)| x == n).count();
    assert_eq!(count("2"), 1, "the event after the refused one lands once: {found:?}");
    assert!(
        (1..=2).contains(&count("0")),
        "the event before it lands, at most twice: {found:?}"
    );
}

/// A receipt destination on a token with acknowledgement on is stopped at its first request, saying what to change.
#[test]
fn receipt_on_a_token_that_acknowledges_says_so() {
    let Some(s) = live::splunk("receipt_on_a_token_that_acknowledges_says_so") else {
        return;
    };
    let mut d = build(&s, Acknowledgement::Receipt, &s.ack_token);
    let e = deliver(&mut *d, vec![row("{}")]).unwrap_err();
    assert!(!e.is_rejection(), "{e}");
    assert!(e.to_string().contains("indexer acknowledgement on"), "{e}");
}

/// An indexer destination on a token with acknowledgement off is stopped before its first event, saying what to change.
#[test]
fn indexer_on_a_token_that_does_not_acknowledge_says_so() {
    let Some(s) = live::splunk("indexer_on_a_token_that_does_not_acknowledge_says_so") else {
        return;
    };
    let mut d = build(&s, Acknowledgement::Indexer, &s.token);
    let e = deliver(&mut *d, vec![row("{}")]).unwrap_err();
    assert!(!e.is_rejection(), "{e}");
    assert!(e.to_string().contains("indexer acknowledgement off"), "{e}");
}

/// Splunk refusing the token is unreachable, not a refusal of the records.
#[test]
fn a_token_splunk_refuses_is_unreachable() {
    let Some(s) = live::splunk("a_token_splunk_refuses_is_unreachable") else {
        return;
    };
    let mut d = build(&s, Acknowledgement::Receipt, "not-a-token");
    let e = deliver(&mut *d, vec![row("{}")]).unwrap_err();
    assert!(!e.is_rejection(), "{e}");
    assert!(e.to_string().contains("code 4"), "{e}");
}
