//! What every client smeltr opens verifies its far end against: Mozilla's roots and the host's own store, with an
//! identity's authorities added, or the authorities alone when the identity does not trust public roots.
//!
//! ```mermaid
//! flowchart LR
//!   M[Mozilla's roots] --> P[public]
//!   H[the host's store] --> P
//!   P -->|unless trustPublic is false| R[what a client trusts]
//!   A[the identity's authorities] --> R
//! ```
//!
//! A receiver never takes these: whom it admits is its identity's authorities alone (`io::receiver`). What an identity
//! presents, a client's certificate or a receiver's, is read here too ([`presented`]), so every connector says the
//! same thing about a certificate or a key it cannot use.

use std::sync::{Arc, OnceLock};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::IoError;
use crate::plan::topology::{HeldSecrets, Identity};

/// Mozilla's roots and the host's, read once per process. A host store that will not read, wholly or in part, is
/// logged and left out, so the public roots still verify what Mozilla's alone would.
pub fn public() -> &'static [CertificateDer<'static>] {
    static PUBLIC: OnceLock<Vec<CertificateDer<'static>>> = OnceLock::new();
    PUBLIC.get_or_init(|| {
        let host = rustls_native_certs::load_native_certs();
        for error in &host.errors {
            tracing::warn!(target: "trust", %error, "a certificate in the host's trust store could not be read");
        }
        let mut seen = std::collections::HashSet::new();
        webpki_root_certs::TLS_SERVER_ROOT_CERTS
            .iter()
            .cloned()
            .chain(host.certs)
            .filter(|c| seen.insert(c.as_ref().to_vec()))
            .collect()
    })
}

/// Every root a client under `identity` trusts. Without an identity, the public roots.
pub fn roots(identity: Option<&Identity>) -> Result<Vec<CertificateDer<'static>>, IoError> {
    let mut out = match identity {
        Some(i) if !i.trust_public => Vec::new(),
        _ => public().to_vec(),
    };
    if let Some(identity) = identity
        && let Some(pem) = &identity.authorities
    {
        out.extend(certificates(identity, pem, "what it trusts")?);
    }
    Ok(out)
}

/// `roots` as a ureq client takes them.
pub fn ureq_roots(identity: Option<&Identity>) -> Result<ureq::tls::RootCerts, IoError> {
    let roots = roots(identity)?
        .iter()
        .map(|der| ureq::tls::Certificate::from_der(der.as_ref()).to_owned())
        .collect();
    Ok(ureq::tls::RootCerts::Specific(Arc::new(roots)))
}

/// A ureq client's TLS under no identity: the public roots, and no certificate.
pub fn public_tls() -> ureq::tls::TlsConfig {
    let roots = public()
        .iter()
        .map(|der| ureq::tls::Certificate::from_der(der.as_ref()).to_owned())
        .collect();
    ureq::tls::TlsConfig::builder()
        .root_certs(ureq::tls::RootCerts::Specific(Arc::new(roots)))
        .build()
}

/// `roots`, as the store a rustls client is built on. A host certificate rustls cannot use is skipped, as the
/// platform's own clients skip it; an identity's authorities are checked when its bundle is saved.
pub fn store(identity: Option<&Identity>) -> Result<Arc<rustls::RootCertStore>, IoError> {
    let mut store = rustls::RootCertStore::empty();
    store.add_parsable_certificates(roots(identity)?);
    Ok(Arc::new(store))
}

/// `roots`, as PEM, for a client that reads its roots from a string (librdkafka's `ssl.ca.pem`).
pub fn pem(identity: Option<&Identity>) -> Result<String, IoError> {
    use base64::Engine as _;
    let mut out = String::new();
    for root in roots(identity)? {
        let body = base64::engine::general_purpose::STANDARD.encode(root.as_ref());
        out.push_str("-----BEGIN CERTIFICATE-----\n");
        for line in body.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
            out.push('\n');
        }
        out.push_str("-----END CERTIFICATE-----\n");
    }
    Ok(out)
}

/// The certificate an identity presents, and the key it signs with.
pub struct Presented {
    /// Leaf first, with any intermediates after it.
    pub chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    /// `key` as the credential holds it, for a client that reads PEM.
    pub key_pem: String,
}

/// What `identity` presents, or `None` when it holds no certificate. A credential this worker cannot obtain is an
/// error here, never a connection that silently falls back to presenting nothing.
pub fn presented(identity: &Identity, secrets: &HeldSecrets) -> Result<Option<Presented>, IoError> {
    let Some(certificate) = &identity.certificate else {
        return Ok(None);
    };
    let named = |what: &str| IoError::transient(format!("the identity {:?}: {what}", identity.id));
    let chain = certificates(identity, certificate, "its certificate")?;
    // Save-time validation refuses a certificate with no key, so this is a bundle that never validated.
    let reference =
        (identity.key.as_ref()).ok_or_else(|| named("it presents a certificate and names no private key"))?;
    let key_pem = super::held(
        secrets,
        reference,
        &format!("the identity {:?}", identity.id),
        "private key",
    )?;
    // Every accepted key format is unencrypted PEM: an encrypted one reaches here as a parse failure, and saying so
    // is the difference between a fixable message and a handshake nobody can explain.
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|e| named(&format!("its private key is not an unencrypted PEM private key: {e}")))?;
    Ok(Some(Presented { chain, key, key_pem }))
}

/// Every certificate in `pem`, one of `identity`'s fields, in the order it was written. `what` names the field for
/// an operator reading the failure.
pub fn certificates(identity: &Identity, pem: &str, what: &str) -> Result<Vec<CertificateDer<'static>>, IoError> {
    let named = |why: String| IoError::transient(format!("the identity {:?}: {what} {why}", identity.id));
    let all = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| named(format!("is not a certificate: {e}")))?;
    if all.is_empty() {
        return Err(named("holds no certificate".to_string()));
    }
    Ok(all)
}

/// Every certificate in the PEM file at `path`, in the order it was written. An error names the file.
pub fn certificates_file(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>, String> {
    CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("{}: {e}", path.display()))
}

/// The unencrypted PEM private key in the file at `path`. An error names the file.
pub fn key_file(path: &std::path::Path) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(authorities: Option<&str>, trust_public: bool) -> Identity {
        Identity {
            id: "acme".to_string(),
            certificate: None,
            key: None,
            authorities: authorities.map(str::to_string),
            trust_public,
        }
    }

    fn authority() -> String {
        rcgen::generate_simple_self_signed(vec!["ca.acme.test".to_string()])
            .unwrap()
            .cert
            .pem()
    }

    #[test]
    fn without_an_identity_a_client_trusts_mozillas_roots() {
        let roots = roots(None).unwrap();
        assert!(roots.len() >= webpki_root_certs::TLS_SERVER_ROOT_CERTS.len());
        assert!(
            webpki_root_certs::TLS_SERVER_ROOT_CERTS
                .iter()
                .all(|r| roots.contains(r))
        );
    }

    #[test]
    fn an_identitys_authorities_are_added_to_the_public_roots() {
        let ca = authority();
        let roots = roots(Some(&identity(Some(&ca), true))).unwrap();
        assert_eq!(roots.len(), public().len() + 1);
    }

    #[test]
    fn an_identity_that_does_not_trust_public_roots_trusts_its_authorities_alone() {
        let ca = authority();
        let roots = roots(Some(&identity(Some(&ca), false))).unwrap();
        assert_eq!(roots.len(), 1);
    }

    #[test]
    fn the_pem_reads_back_as_the_same_roots() {
        let ca = authority();
        let id = identity(Some(&ca), true);
        let back = CertificateDer::pem_slice_iter(pem(Some(&id)).unwrap().as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(back, roots(Some(&id)).unwrap());
    }

    #[test]
    fn authorities_that_are_not_certificates_are_refused() {
        let error = roots(Some(&identity(Some("not a certificate"), true))).unwrap_err();
        assert!(error.to_string().contains("holds no certificate"), "{error}");
    }
}
