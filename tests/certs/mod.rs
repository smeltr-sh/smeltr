//! A private certificate authority, and the certificates under it that a TLS test needs.
//!
//! Minted rather than checked in: a fixture certificate expires, and a suite that starts failing on a date is worse
//! than no suite. `rcgen` is already how this repository writes the leader's own authority, so there is one answer
//! here to what a certificate is made of.

use std::sync::Arc;

/// An authority and a certificate it issued: the PEM a bundle would carry, and the key a secret would.
pub struct Issued {
    /// The certificate, leaf first. What an identity's `certificate` holds, or a server presents.
    pub certificate: String,
    /// Its private key, unencrypted PEM. What an identity's key secret holds.
    pub key: String,
}

/// A certificate authority that can issue for a name, and hand out the PEM a client must trust to verify it.
pub struct Authority {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    /// The authority's own certificate, in PEM. What an identity's `authorities` holds.
    pub pem: String,
}

impl Authority {
    /// A fresh authority, under a name that says which one it is in a failure.
    pub fn new(common_name: &str) -> Authority {
        let key = rcgen::KeyPair::generate().expect("generate an authority key");
        let mut params = rcgen::CertificateParams::new(Vec::new()).expect("authority parameters");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.distinguished_name = named(common_name);
        let certificate = params.clone().self_signed(&key).expect("self-sign an authority");
        Authority {
            pem: certificate.pem(),
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    /// A certificate for `names`, signed by this authority. A server's or a client's: what differs is who checks it.
    pub fn issue(&self, common_name: &str, names: Vec<String>) -> Issued {
        let key = rcgen::KeyPair::generate().expect("generate a key");
        let mut params = rcgen::CertificateParams::new(names).expect("certificate parameters");
        params.distinguished_name = named(common_name);
        let certificate = params.signed_by(&key, &self.issuer).expect("sign a certificate");
        Issued {
            certificate: certificate.pem(),
            key: key.serialize_pem(),
        }
    }
}

fn named(common_name: &str) -> rcgen::DistinguishedName {
    let mut name = rcgen::DistinguishedName::new();
    name.push(rcgen::DnType::CommonName, common_name);
    name
}

/// PEM to DER, for handing rustls what rcgen wrote.
pub fn certificates(pem: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .expect("PEM certificates")
}

/// A rustls server configuration presenting `served` and demanding a certificate under `trusted`.
///
/// Client authentication is required rather than optional: a test that proves a client certificate is presented has
/// to be one the connection fails without.
pub fn server_config(served: &Issued, trusted: &Authority) -> Arc<rustls::ServerConfig> {
    use rustls::pki_types::pem::PemObject;

    let mut roots = rustls::RootCertStore::empty();
    for root in certificates(&trusted.pem) {
        roots.add(root).expect("a usable authority");
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        roots.into(),
        rustls::crypto::aws_lc_rs::default_provider().into(),
    )
    .build()
    .expect("a client verifier");

    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(served.key.as_bytes()).expect("a PEM private key");
    Arc::new(
        rustls::ServerConfig::builder_with_provider(rustls::crypto::aws_lc_rs::default_provider().into())
            .with_safe_default_protocol_versions()
            .expect("default protocol versions")
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates(&served.certificate), key)
            .expect("the key belongs to the certificate"),
    )
}

/// Install AWS-LC as this process's rustls provider, as `main` does at startup.
///
/// A client built here takes whatever the process installed rather than carrying a provider of its own, so
/// without this it panics on its first connection. Once per process, whichever test connects first.
pub fn install_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("install rustls crypto provider");
    });
}
