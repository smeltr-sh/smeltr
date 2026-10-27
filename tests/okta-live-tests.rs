//! Reading a *real* Okta org's System Log page after page, signed in with an API token and as a service app with DPoP.
//!
//! `api_source_tests` checks the wire against a fake org that verifies every signature. This file is the contract with
//! Okta: that it takes the token, the assertion and the proofs, and answers with events.
//!
//! Point it at `OKTA_ORG` with `OKTA_TOKEN`, `OKTA_CLIENT_ID`, `OKTA_PRIVATE_KEY` and, when the app holds several active
//! keys, `OKTA_KEY_ID`; `just check-okta` runs it. The app is an API Services app granted `okta.logs.read` and the
//! Read-only Administrator role. Under `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips.

use crate::live;

use std::time::{Duration, Instant};

use smeltr::io::{Progress, Source, build_source};
use smeltr::plan::topology::{
    Api, ApiSourceConfig, HeldSecrets, ListStart, OktaLogsConfig, OktaSignIn, SecretRef, SourceKind,
};

/// A PEM key, or one in base64 on a line, as an environment variable can hold it.
fn pem(value: &str) -> String {
    if value.contains("-----BEGIN") {
        return value.to_string();
    }
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let (mut bits, mut n) = (0u32, 0);
    for c in value.bytes().filter(|c| !c.is_ascii_whitespace() && *c != b'=') {
        let at = ALPHABET
            .iter()
            .position(|&a| a == c)
            .expect("OKTA_PRIVATE_KEY is neither PEM nor base64");
        bits = bits << 6 | at as u32;
        n += 6;
        if n >= 8 {
            n -= 8;
            out.push((bits >> n) as u8);
        }
    }
    String::from_utf8(out).expect("OKTA_PRIVATE_KEY's base64 holds a PEM key")
}

fn okta(org: &str, sign_in: OktaSignIn, secrets: &HeldSecrets) -> Box<dyn Source> {
    // What `main` installs first. Another test in this process may have installed it already.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let kind = SourceKind::Api(ApiSourceConfig {
        api: Api::OktaLogs(OktaLogsConfig {
            org: org.trim_end_matches('/').to_string(),
            sign_in,
            start: ListStart::Everything,
        }),
        tls: None,
        poll_interval: Duration::from_secs(3600),
    });
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    build_source(&kind, &registry, &subjects, secrets, None).expect("opens")
}

/// Okta's pages read in order. The source resumes at a first page of two events, so the pages it then follows are
/// Okta's own next links, each signed in afresh: three pages hold six distinct events, oldest first. Signing in
/// leaves events of its own, so an org this test has run against has them.
fn read_pages(org: &str, mut source: Box<dyn Source>) {
    source
        .resume(Progress {
            cursor: Some(format!(
                "{}/api/v1/logs?sortOrder=ASCENDING&limit=2",
                org.trim_end_matches('/')
            )),
            ..Progress::default()
        })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut pages: Vec<(String, Vec<serde_json::Value>)> = Vec::new();
    while pages.len() < 3 && Instant::now() < deadline {
        for record in source.poll_batch(10, 1 << 20, Duration::from_secs(1)).unwrap() {
            let event: serde_json::Value = serde_json::from_slice(record.bytes().expect("raw")).expect("JSON");
            assert!(event["uuid"].is_string() && event["eventType"].is_string(), "{event}");
            match pages.last_mut() {
                Some((page, events)) if **page == *record.topic => events.push(event),
                _ => pages.push((record.topic.to_string(), vec![event])),
            }
        }
    }
    assert!(
        pages.len() >= 3,
        "{} pages within a minute: {:?}",
        pages.len(),
        source.connection_error()
    );
    let events: Vec<&serde_json::Value> = pages.iter().take(3).flat_map(|(_, e)| e).collect();
    for (page, events) in &pages[..3] {
        assert_eq!(events.len(), 2, "{page}");
    }
    let uuids: std::collections::HashSet<_> = events.iter().map(|e| e["uuid"].as_str()).collect();
    assert_eq!(uuids.len(), 6, "every event once");
    let published: Vec<_> = events.iter().map(|e| e["published"].as_str().unwrap()).collect();
    assert!(published.is_sorted(), "{published:?}");
}

#[test]
#[ignore = "reads a real Okta org; `just check-okta` runs it"]
fn an_api_token_reads_the_system_log() {
    let Some([org, ..]) = live::okta("an_api_token_reads_the_system_log") else {
        return;
    };
    let sign_in = OktaSignIn::Token(SecretRef::WorkerLocal("OKTA_TOKEN".into()));
    read_pages(&org, okta(&org, sign_in, &HeldSecrets::default()));
}

#[test]
#[ignore = "reads a real Okta org; `just check-okta` runs it"]
fn a_service_app_reads_the_system_log_with_dpop() {
    let Some([org, _, client_id, key]) = live::okta("a_service_app_reads_the_system_log_with_dpop") else {
        return;
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("okta-key".to_string(), 1, pem(&key))]);
    let sign_in = OktaSignIn::ServiceApp {
        client_id,
        private_key: SecretRef::LeaderStored {
            name: "okta-key".into(),
            version: 1,
        },
        // Needed when the app holds several active keys.
        key_id: std::env::var("OKTA_KEY_ID").ok().filter(|k| !k.is_empty()),
    };
    read_pages(&org, okta(&org, sign_in, &secrets));
}
