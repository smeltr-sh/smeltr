//! What this build's cryptography is, asserted rather than assumed.
//!
//! One provider, AWS-LC, linked as the validated module. These hold whatever features a dependency turns on
//! downstream, so a second backend arriving through someone else's default fails here rather than at a handshake.

/// The module is the validated one, in every build.
///
/// A deployment is eligible for a FIPS claim or it is not, and that is settled when the binary is built. Nothing
/// an operator does afterward changes the answer, so nothing has to be rebuilt to ask it.
#[test]
fn the_linked_module_is_the_validated_one() {
    assert!(
        aws_lc_rs::try_fips_mode().is_ok(),
        "this build did not link the validated AWS-LC module: check the `fips` feature on `aws-lc-rs`"
    );
}

/// The provider rustls is given is AWS-LC's, and it is the one the process installs.
///
/// `ureq` and `axum-server` are both built without a provider of their own and take whatever is installed, so an
/// install that did not happen is a panic on a connection rather than a failure here.
#[test]
fn the_installed_provider_is_aws_lc() {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    assert!(!provider.cipher_suites.is_empty());

    // What the provider offers, rather than what it can perform: `fips()` is true only once every suite and group
    // in it is one the module's validation covers. `rustls`'s `fips` feature leaves nothing else in it, so this
    // holds in every build and for every deployment.
    assert!(provider.fips());

    provider
        .install_default()
        .expect("no other provider was installed first");
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
}
