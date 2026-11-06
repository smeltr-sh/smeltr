//! Whom a receiver admits by a sender's certificate (RFC 5425, 5): one it names by its fingerprint, whoever issued
//! it, or one that chains to its authorities and, when it names hosts, was issued to one of them. A sender it refuses
//! is refused in the handshake, with an alert.
//!
//! ```mermaid
//! flowchart TD
//!   C[a sender's certificate] --> F{its fingerprint is named}
//!   F -->|yes| A[admitted]
//!   F -->|no| V{it chains to the authorities}
//!   V -->|no| R[refused]
//!   V -->|yes| N{the receiver names hosts}
//!   N -->|no| A
//!   N -->|yes| M{the certificate names one}
//!   M -->|yes| A
//!   M -->|no| R
//! ```

use std::fmt;
use std::sync::Arc;

use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{CertificateError, DigitallySignedStruct, DistinguishedName, Error, RootCertStore, SignatureScheme};

use crate::plan::topology::Senders;

/// A hash a fingerprint is taken with, by the label RFC 4572 gives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hash {
    /// RFC 5425 (4.2.1) has every implementation take it.
    Sha1,
    Sha256,
}

impl Hash {
    fn label(self) -> &'static str {
        match self {
            Hash::Sha1 => "sha-1",
            Hash::Sha256 => "sha-256",
        }
    }

    fn algorithm(self) -> &'static aws_lc_rs::digest::Algorithm {
        match self {
            Hash::Sha1 => &aws_lc_rs::digest::SHA1_FOR_LEGACY_USE_ONLY,
            Hash::Sha256 => &aws_lc_rs::digest::SHA256,
        }
    }
}

/// A certificate's fingerprint: a hash of its DER. Shown as RFC 5425 (4.2.1) writes one, `sha-256:AB:CD:…`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint {
    hash: Hash,
    digest: Vec<u8>,
}

impl Fingerprint {
    /// `der`'s, taken with `hash`.
    pub fn of(hash: Hash, der: &[u8]) -> Fingerprint {
        Fingerprint {
            hash,
            digest: aws_lc_rs::digest::digest(hash.algorithm(), der).as_ref().to_vec(),
        }
    }

    /// One as RFC 5425 (4.2.1) writes it: a label, `sha-1` or `sha-256`, a colon, then the hash's bytes in hex. Its
    /// case, and the colons between bytes, are the writer's choice. `Err` says what is wrong with it.
    pub fn parse(text: &str) -> Result<Fingerprint, String> {
        let (label, hex) = text.trim().split_once(':').ok_or("it has no label, as sha-256:")?;
        let hash = match label.to_ascii_lowercase().as_str() {
            "sha-1" => Hash::Sha1,
            "sha-256" => Hash::Sha256,
            _ => return Err(format!("{label:?} is not sha-1 or sha-256")),
        };
        let hex: String = hex.chars().filter(|&c| c != ':').collect();
        let digest = (0..hex.len())
            .step_by(2)
            .map(|i| {
                hex.get(i..i + 2)
                    .filter(|byte| byte.bytes().all(|b| b.is_ascii_hexdigit()))
            })
            .map(|byte| byte.and_then(|byte| u8::from_str_radix(byte, 16).ok()))
            .collect::<Option<Vec<u8>>>()
            .ok_or("its hash is not hexadecimal bytes")?;
        let length = hash.algorithm().output_len();
        if digest.len() != length {
            return Err(format!(
                "a {} hash is {length} bytes, and it holds {}",
                hash.label(),
                digest.len()
            ));
        }
        Ok(Fingerprint { hash, digest })
    }

    /// Whether it is `der`'s.
    fn is_of(&self, der: &[u8]) -> bool {
        *self == Fingerprint::of(self.hash, der)
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.hash.label())?;
        for byte in &self.digest {
            write!(f, ":{byte:02X}")?;
        }
        Ok(())
    }
}

/// Whether `host`, a name a receiver admits, is a host name, its left-most label `*` where it stands for any one.
pub fn is_host(host: &str) -> bool {
    let host = host.strip_prefix("*.").unwrap_or(host);
    let label = |l: &str| {
        (1..=63).contains(&l.len())
            && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !l.starts_with('-')
            && !l.ends_with('-')
    };
    host.len() <= 253 && host.split('.').all(label)
}

/// Whether `host`, a name a receiver admits, matches `named`, one a sender's certificate holds. Either may begin with
/// `*.`, which stands for any one left-most label (RFC 5425, 5.2), so `*.example.com` matches `a.example.com` and
/// neither `example.com` nor `a.b.example.com`. Case does not count.
fn matches(host: &str, named: &str) -> bool {
    let (host, named) = (host.to_ascii_lowercase(), named.to_ascii_lowercase());
    let within = |domain: &str, name: &str| {
        name.split_once('.')
            .is_some_and(|(label, rest)| !label.is_empty() && label != "*" && rest == domain)
    };
    match (host.strip_prefix("*."), named.strip_prefix("*.")) {
        (Some(domain), None) => within(domain, &named),
        (None, Some(domain)) => within(domain, &host),
        _ => host == named,
    }
}

/// The host names `der` is issued to: its subjectAltName's dNSNames, or, where it has no subjectAltName names at all,
/// its subject's common name, which RFC 6125 (6.4.4) has a client read only then.
fn hosts(der: &[u8]) -> Vec<String> {
    let Ok((_, cert)) = x509_parser::parse_x509_certificate(der) else {
        return Vec::new();
    };
    if let Ok(Some(san)) = cert.subject_alternative_name()
        && !san.value.general_names.is_empty()
    {
        return (san.value.general_names.iter())
            .filter_map(|name| match name {
                x509_parser::extensions::GeneralName::DNSName(n) => Some((*n).to_string()),
                _ => None,
            })
            .collect();
    }
    cert.subject()
        .iter_common_name()
        .filter_map(|cn| cn.as_str().ok().map(str::to_string))
        .collect()
}

/// What checks a sender's certificate: `authorities` for one that chains to them, and `senders` for the rest. `Err`
/// says why it cannot be built.
pub(crate) fn verifier(
    authorities: Option<RootCertStore>,
    senders: &Senders,
    provider: Arc<CryptoProvider>,
) -> Result<Arc<dyn ClientCertVerifier>, String> {
    let chained = authorities
        .map(|roots| {
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                .build()
                .map_err(|e| e.to_string())
        })
        .transpose()?;
    if senders.fingerprints.is_empty()
        && senders.names.is_empty()
        && let Some(chained) = chained
    {
        return Ok(chained);
    }
    Ok(Arc::new(SenderVerifier {
        fingerprints: senders.fingerprints.clone(),
        names: senders.names.clone(),
        chained,
        algorithms: provider.signature_verification_algorithms,
    }))
}

/// A sender's certificate checked as the module says.
#[derive(Debug)]
struct SenderVerifier {
    fingerprints: Vec<Fingerprint>,
    names: Vec<String>,
    chained: Option<Arc<dyn ClientCertVerifier>>,
    algorithms: WebPkiSupportedAlgorithms,
}

/// The alert a refused sender is sent: access_denied.
fn refused() -> Error {
    Error::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
}

impl ClientCertVerifier for SenderVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // A sender named by its fingerprint may be issued by anyone, so the authorities are no hint to it.
        match (&self.chained, self.fingerprints.is_empty()) {
            (Some(chained), true) => chained.root_hint_subjects(),
            _ => &[],
        }
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        if self.fingerprints.iter().any(|f| f.is_of(end_entity)) {
            return Ok(ClientCertVerified::assertion());
        }
        let chained = self.chained.as_ref().ok_or_else(refused)?;
        let verified = chained.verify_client_cert(end_entity, intermediates, now)?;
        let named = || {
            hosts(end_entity)
                .iter()
                .any(|n| self.names.iter().any(|h| matches(h, n)))
        };
        match self.names.is_empty() || named() {
            true => Ok(verified),
            false => Err(refused()),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fingerprint_reads_as_rfc_5425_writes_it_and_shows_so() {
        let der = b"a certificate";
        let sha256 = Fingerprint::of(Hash::Sha256, der);
        let shown = sha256.to_string();
        assert!(shown.starts_with("sha-256:"), "{shown}");
        assert_eq!(shown.len(), "sha-256".len() + 32 * 3);
        assert_eq!(Fingerprint::parse(&shown), Ok(sha256.clone()));
        // Its label in any case, its hex in either, its colons left out.
        let loose = format!(
            "SHA-256:{}",
            shown["sha-256:".len()..].replace(':', "").to_ascii_lowercase()
        );
        assert_eq!(Fingerprint::parse(&loose), Ok(sha256));
        let sha1 = Fingerprint::of(Hash::Sha1, der);
        assert_eq!(sha1.to_string().len(), 65, "RFC 5425 (4.2.1)");
        assert_eq!(Fingerprint::parse(&sha1.to_string()), Ok(sha1));
        assert!(Fingerprint::parse(&shown[..shown.len() - 3]).is_err());
        assert!(Fingerprint::parse("md5:AB").is_err());
        assert!(Fingerprint::parse("ABCD").is_err());
        assert!(Fingerprint::parse("sha-1:zz").is_err());
        assert!(Fingerprint::parse("sha-256:é").is_err());
        assert!(Fingerprint::parse(&format!("sha-1{}", ":+F".repeat(20))).is_err());
    }

    #[test]
    fn a_host_matches_by_its_name_or_one_left_most_wildcard() {
        assert!(matches("a.example.com", "A.Example.com"));
        assert!(matches("*.example.com", "a.example.com"));
        assert!(matches("a.example.com", "*.example.com"));
        assert!(matches("*.example.com", "*.example.com"));
        for name in ["example.com", "a.b.example.com", "a.example.org", "*"] {
            assert!(!matches("*.example.com", name), "{name}");
            assert!(!matches(name, "*.example.com"), "{name}");
        }
        assert!(!matches("a.example.com", "b.example.com"));
    }

    #[test]
    fn a_host_is_labels_its_left_most_maybe_a_wildcard() {
        for host in ["example.com", "*.example.com", "a-b.c", "localhost"] {
            assert!(is_host(host), "{host}");
        }
        for host in ["", "a..b", "-a.b", "a.*.b", "*", "a b", "a.b.", &"a".repeat(64)] {
            assert!(!is_host(host), "{host}");
        }
    }
}
