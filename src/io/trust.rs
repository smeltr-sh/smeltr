//! What every client smeltr opens verifies its far end against: Mozilla's roots and the host's own store, with the
//! connector's authorities added, or the authorities alone when the connector does not trust public roots.
//!
//! ```mermaid
//! flowchart LR
//!   M[Mozilla's roots] --> P[public]
//!   H[the host's store] --> P
//!   P -->|unless trustPublic is false| R[what a client trusts]
//!   A[the connector's authorities] --> R
//! ```
//!
//! A receiver never takes these: whom it admits is its own authorities alone (`io::receiver`). What a connector
//! presents, a client's certificate or a receiver's, is read here too ([`presented`]), so every connector says the
//! same thing about a certificate or a key it cannot use.

use std::sync::{Arc, OnceLock};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::IoError;
use crate::plan::topology::{HeldSecrets, Tls};

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

/// Every root a client speaking `tls` trusts. Speaking none of its own, the public roots.
pub fn roots(tls: Option<&Tls>) -> Result<Vec<CertificateDer<'static>>, IoError> {
    let Some(tls) = tls else {
        return Ok(public().to_vec());
    };
    let authorities = match &tls.authorities {
        Some(pem) => certificates(tls, pem, "what it trusts")?,
        None => Vec::new(),
    };
    Ok(trusting(authorities, tls.trust_public))
}

/// What a client given `authorities` trusts: those, and the public roots too when `trust_public`. A connector's rule,
/// for a client that holds its authorities without one: a worker's, for its leader.
pub fn trusting(authorities: Vec<CertificateDer<'static>>, trust_public: bool) -> Vec<CertificateDer<'static>> {
    let public = if trust_public { public() } else { &[] };
    public.iter().cloned().chain(authorities).collect()
}

/// `roots` as a ureq client takes them.
pub fn ureq_roots(tls: Option<&Tls>) -> Result<ureq::tls::RootCerts, IoError> {
    let roots = roots(tls)?
        .iter()
        .map(|der| ureq::tls::Certificate::from_der(der.as_ref()).to_owned())
        .collect();
    Ok(ureq::tls::RootCerts::Specific(Arc::new(roots)))
}

/// A ureq client's TLS speaking none of its own: the public roots, and no certificate.
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
/// platform's own clients skip it; a connector's authorities are checked when its bundle is saved.
pub fn store(tls: Option<&Tls>) -> Result<Arc<rustls::RootCertStore>, IoError> {
    let mut store = rustls::RootCertStore::empty();
    store.add_parsable_certificates(roots(tls)?);
    Ok(Arc::new(store))
}

/// `roots`, as PEM, for a client that reads its roots from a string (librdkafka's `ssl.ca.pem`).
pub fn pem(tls: Option<&Tls>) -> Result<String, IoError> {
    Ok(pem_of(&roots(tls)?))
}

/// `roots` as PEM, one block each, in order.
pub fn pem_of(roots: &[CertificateDer<'static>]) -> String {
    use base64::Engine as _;
    let mut out = String::new();
    for root in roots {
        let body = base64::engine::general_purpose::STANDARD.encode(root.as_ref());
        out.push_str("-----BEGIN CERTIFICATE-----\n");
        for line in body.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
            out.push('\n');
        }
        out.push_str("-----END CERTIFICATE-----\n");
    }
    out
}

/// `roots` as a PEM file at `path`, for a client that names its roots only by a file: curl, under gix. Written
/// beside and renamed over, so a fetch reading it on another thread never sees half of it.
pub fn write_pem_file(roots: &[CertificateDer<'static>], path: &std::path::Path) -> Result<(), String> {
    crate::leader::tls::write_durably(path, pem_of(roots).as_bytes()).map_err(|e| format!("{}: {e}", path.display()))
}

/// The certificate a connector presents, and the key it signs with.
pub struct Presented {
    /// Leaf first, with any intermediates after it.
    pub chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    /// `chain` as the certificate entry holds it, for a client that reads PEM.
    pub chain_pem: String,
    /// `key` as the certificate entry holds it, for a client that reads PEM.
    pub key_pem: String,
}

/// What `tls` presents, or `None` when it names no certificate. A certificate this worker cannot obtain is an
/// error here, never a connection that silently falls back to presenting nothing.
pub fn presented(tls: &Tls, secrets: &HeldSecrets) -> Result<Option<Presented>, IoError> {
    let Some(name) = &tls.certificate else {
        return Ok(None);
    };
    let named = |what: &str| IoError::transient(format!("{}: {what}", tls.of));
    let (chain_pem, key_pem) = secrets.certificate(name).ok_or_else(|| {
        named(&format!(
            "it presents the certificate {name:?}, which this worker does not have"
        ))
    })?;
    let chain = certificates(tls, &chain_pem, "its certificate")?;
    // Every accepted key format is unencrypted PEM: an encrypted one reaches here as a parse failure, and saying so
    // is the difference between a fixable message and a handshake nobody can explain.
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|e| named(&format!("its private key is not an unencrypted PEM private key: {e}")))?;
    Ok(Some(Presented {
        chain,
        key,
        chain_pem,
        key_pem,
    }))
}

/// Every certificate in `pem`, one of `tls`'s fields, in the order it was written. `what` names the field for an
/// operator reading the failure.
pub fn certificates(tls: &Tls, pem: &str, what: &str) -> Result<Vec<CertificateDer<'static>>, IoError> {
    let named = |why: String| IoError::transient(format!("{}: {what} {why}", tls.of));
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

/// Which side of a TLS handshake a certificate is presented from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// By a connector connecting out.
    Client,
    /// By a receiver, to its senders.
    Server,
}

/// Refuses a presented certificate that cannot authenticate from `side`: Extended Key Usage naming purposes that
/// leave out that side's, or Key Usage naming permissions that do not include the handshake signature either side
/// makes. Read with the same library the connectors build their TLS configs against: a deploy must not accept what a
/// handshake would go on to refuse. Either extension absent entirely rules nothing out -- RFC 5280's own default --
/// so only one naming purposes or permissions and excluding what this needs is refused.
pub fn refuse_unless_usable(pem: &str, side: Side) -> Result<(), String> {
    let leaf = CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .ok_or_else(|| "it holds no certificate".to_string())?
        .map_err(|e| format!("{e}"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref())
        .map_err(|e| format!("that is not a certificate this can read: {e}"))?;
    match (cert.extended_key_usage(), side) {
        (Ok(Some(eku)), Side::Client) if !eku.value.client_auth => {
            return Err(
                "whose Extended Key Usage does not permit client authentication, so a far end enforcing it \
                 refuses this certificate at the handshake -- a connector connecting out presents it as a client \
                 certificate"
                    .to_string(),
            );
        }
        (Ok(Some(eku)), Side::Server) if !eku.value.server_auth => {
            return Err(
                "whose Extended Key Usage does not permit server authentication, so a sender refuses this \
                 certificate at the handshake -- a receiver presents it as a server certificate"
                    .to_string(),
            );
        }
        (Ok(_), _) => {}
        (Err(e), _) => return Err(format!("whose Extended Key Usage could not be read: {e}")),
    }
    // Either end signs the handshake, which is what digitalSignature names permission for: rustls offers only
    // ephemeral key exchange, so no end encrypts with its certificate's key. Present but false is a certificate
    // meant for key exchange, not signing -- the Extended Key Usage names the purpose, this the means, and a far
    // end enforcing KeyUsage checks both.
    match cert.key_usage() {
        Ok(Some(usage)) if !usage.value.digital_signature() => Err(
            "whose Key Usage does not permit a digital signature, so it cannot sign the handshake a far end \
             enforcing it requires"
                .to_string(),
        ),
        Ok(_) => Ok(()),
        Err(e) => Err(format!("whose Key Usage could not be read: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn speaking(authorities: Option<&str>, trust_public: bool) -> Tls {
        Tls {
            of: "source \"in\"".to_string(),
            certificate: None,
            authorities: authorities.map(str::to_string),
            trust_public,
            side: Side::Client,
        }
    }

    fn authority() -> String {
        rcgen::generate_simple_self_signed(vec!["ca.acme.test".to_string()])
            .unwrap()
            .cert
            .pem()
    }

    #[test]
    fn a_client_speaking_no_tls_of_its_own_trusts_mozillas_roots() {
        let roots = roots(None).unwrap();
        assert!(roots.len() >= webpki_root_certs::TLS_SERVER_ROOT_CERTS.len());
        assert!(
            webpki_root_certs::TLS_SERVER_ROOT_CERTS
                .iter()
                .all(|r| roots.contains(r))
        );
    }

    #[test]
    fn a_connectors_authorities_are_added_to_the_public_roots() {
        let ca = authority();
        let roots = roots(Some(&speaking(Some(&ca), true))).unwrap();
        assert_eq!(roots.len(), public().len() + 1);
    }

    #[test]
    fn a_connector_that_does_not_trust_public_roots_trusts_its_authorities_alone() {
        let ca = authority();
        let roots = roots(Some(&speaking(Some(&ca), false))).unwrap();
        assert_eq!(roots.len(), 1);
    }

    #[test]
    fn the_pem_reads_back_as_the_same_roots() {
        let ca = authority();
        let id = speaking(Some(&ca), true);
        let back = CertificateDer::pem_slice_iter(pem(Some(&id)).unwrap().as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(back, roots(Some(&id)).unwrap());
    }

    #[test]
    fn the_pem_file_reads_back_as_the_same_roots() {
        let path = std::env::temp_dir().join(format!("smeltr-trust-pem-file-{}.pem", std::process::id()));
        write_pem_file(public(), &path).unwrap();
        assert_eq!(certificates_file(&path).unwrap(), public());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn authorities_that_are_not_certificates_are_refused() {
        let error = roots(Some(&speaking(Some("not a certificate"), true))).unwrap_err();
        assert!(error.to_string().contains("holds no certificate"), "{error}");
    }
}
