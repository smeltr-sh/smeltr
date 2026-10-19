//! The provider this process offers, and what it holds to.
//!
//! One provider, installed once at startup, taken from there by everything that opens a connection: the leader's
//! server, a worker's heartbeat, the Elasticsearch transport and the git fetch inside curl. Kafka is not among
//! them -- librdkafka carries its own TLS over its own OpenSSL, which the design document's Cryptography section
//! states and this module cannot reach.
//!
//! Every suite and group in it sits inside the linked module's validation boundary, because `rustls`'s `fips`
//! feature leaves nothing else to offer. There is one set, so nothing selects between sets and no deployment
//! answers for a choice it did not make.

use std::sync::Arc;

use rustls::crypto::CryptoProvider;

/// The provider this process is using.
///
/// `main` installs one before any role is told apart, so that is what a running smeltr takes. Building one here
/// is for a test that calls into this crate without standing a process up, which would otherwise have to install
/// a process-wide default to ask an ordinary question about a certificate.
pub fn in_use() -> Arc<CryptoProvider> {
    match CryptoProvider::get_default() {
        Some(installed) => installed.clone(),
        None => Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    }
}

/// The suites and groups this process offers, for the startup line that records them.
///
/// Read from the installed provider rather than named here, so what is logged is what a peer is given.
pub fn offered() -> (Vec<String>, Vec<String>) {
    let provider = in_use();
    (
        provider
            .cipher_suites
            .iter()
            .map(|suite| format!("{:?}", suite.suite()))
            .collect(),
        provider
            .kx_groups
            .iter()
            .map(|group| format!("{:?}", group.name()))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    /// Everything offered is inside the boundary, which is what `fips()` reports.
    ///
    /// The module performing an operation and its validation covering that operation are two facts, and only the
    /// second is what an audit asks about. This is the second.
    #[test]
    fn the_offer_is_what_the_validation_covers() {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        assert!(
            provider.fips(),
            "a suite or group outside the validation boundary is on offer"
        );
    }

    /// What is left still completes a handshake, and still has somewhere post-quantum to go.
    ///
    /// `TLS13_AES_128_GCM_SHA256` is mandatory for a TLS 1.3 peer, so a narrowed offer meets any of them; the
    /// hybrid group is what a harvest-now-decrypt-later recording does not get.
    #[test]
    fn the_offer_still_reaches_any_peer() {
        let provider = rustls::crypto::aws_lc_rs::default_provider();

        let suites: Vec<_> = provider
            .cipher_suites
            .iter()
            .map(|s| format!("{:?}", s.suite()))
            .collect();
        let groups: Vec<_> = provider.kx_groups.iter().map(|g| format!("{:?}", g.name())).collect();

        assert!(
            suites.iter().any(|name| name == "TLS13_AES_128_GCM_SHA256"),
            "the suite every TLS 1.3 peer must implement is not offered: {suites:?}"
        );
        assert!(
            groups.iter().any(|name| name.contains("MLKEM")),
            "no post-quantum group is offered: {groups:?}"
        );
    }
}
