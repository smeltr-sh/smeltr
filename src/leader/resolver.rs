//! What the leader's socket presents: the certificate in force, and, while an ACME order is being validated, the
//! proof a certificate authority connects for.
//!
//! ```mermaid
//! flowchart LR
//!   H[a handshake] -->|offers acme-tls/1| P[the proof for its server name]
//!   H -->|anything else| S[the certificate in force]
//! ```
//!
//! One resolver for the life of the process. Replacing the certificate swaps what it holds, which reaches every
//! handshake after it and no connection already open -- the same property `axum_server`'s own reload has, kept
//! here because the proof has to live on the same socket: TLS-ALPN-01 (RFC 8737) is the authority connecting to
//! the leader's own port, naming the name it is validating, and asking for `acme-tls/1`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use rustls::pki_types::PrivateKeyDer;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

/// The protocol a validator asks for, and nothing else offers.
pub const ACME_TLS: &[u8] = b"acme-tls/1";

pub struct Resolver {
    served: RwLock<Arc<CertifiedKey>>,
    /// By the name each proves, lower-cased as a server name arrives.
    proofs: Mutex<HashMap<String, Arc<CertifiedKey>>>,
}

impl Resolver {
    /// Resolver for the certificate `chain`, signed with `key`: both PEM.
    pub fn load(chain: &[u8], key: &[u8]) -> Result<Arc<Resolver>, String> {
        Ok(Arc::new(Resolver {
            served: RwLock::new(read(chain, key)?),
            proofs: Mutex::new(HashMap::new()),
        }))
    }

    /// Presents the certificate `chain` from the next handshake on. A pair that cannot be read or does not match
    /// leaves what is presented as it was.
    pub fn reload(&self, chain: &[u8], key: &[u8]) -> Result<(), String> {
        let key = read(chain, key)?;
        *self.served.write().unwrap_or_else(|e| e.into_inner()) = key;
        Ok(())
    }

    /// Answers a validator asking for `name` with a certificate carrying `digest`, the SHA-256 of the challenge's
    /// key authorization, until [`done`](Self::done).
    pub fn prove(&self, name: &str, digest: &[u8]) -> Result<(), String> {
        let proof = proof(name, digest)?;
        self.proofs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(asked_as(name), Arc::new(proof));
        Ok(())
    }

    /// Withdraws every proof. An order is over, however it ended.
    pub fn done(&self) {
        self.proofs.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    /// The socket's TLS, over this process's one provider, offering what browsers and workers speak and what a
    /// validator asks for.
    pub fn server_config(self: &Arc<Self>) -> Result<Arc<rustls::ServerConfig>, String> {
        let mut config = rustls::ServerConfig::builder_with_provider(crate::crypto::in_use())
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("{e}"))?
            .with_no_client_auth()
            .with_cert_resolver(self.clone());
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), ACME_TLS.to_vec()];
        Ok(Arc::new(config))
    }
}

impl std::fmt::Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resolver").finish_non_exhaustive()
    }
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let asks_for_proof = hello.alpn().is_some_and(|mut offered| offered.any(|p| p == ACME_TLS));
        if !asks_for_proof {
            return Some(self.served.read().unwrap_or_else(|e| e.into_inner()).clone());
        }
        // A validator asking for a name nobody is proving gets no certificate at all, which fails its handshake:
        // the right answer to a challenge this leader did not start.
        let name = hello.server_name()?.to_ascii_lowercase();
        self.proofs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&name)
            .cloned()
    }
}

fn read(chain: &[u8], key: &[u8]) -> Result<Arc<CertifiedKey>, String> {
    use rustls::pki_types::pem::PemObject;
    let chain = rustls::pki_types::CertificateDer::pem_slice_iter(chain)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("the certificate is not a PEM certificate: {e}"))?;
    let key =
        PrivateKeyDer::from_pem_slice(key).map_err(|e| format!("the private key is not a PEM private key: {e}"))?;
    CertifiedKey::from_der(chain, key, &crate::crypto::in_use())
        .map(Arc::new)
        .map_err(|e| format!("the certificate: {e}"))
}

/// The server name a validator asks for `name` by: the name itself, lower-cased, or an address in its reverse-DNS
/// form (RFC 8738), since a server name cannot carry an address.
fn asked_as(name: &str) -> String {
    match name.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(v4)) => {
            let o = v4.octets();
            format!("{}.{}.{}.{}.in-addr.arpa", o[3], o[2], o[1], o[0])
        }
        Ok(std::net::IpAddr::V6(v6)) => {
            let nibbles: Vec<String> = v6
                .octets()
                .iter()
                .rev()
                .flat_map(|b| [format!("{:x}", b & 0xf), format!("{:x}", b >> 4)])
                .collect();
            format!("{}.ip6.arpa", nibbles.join("."))
        }
        Err(_) => name.to_ascii_lowercase(),
    }
}

/// A self-signed certificate for `name` carrying `digest` in the `acmeIdentifier` extension, as RFC 8737 has a
/// validator check.
fn proof(name: &str, digest: &[u8]) -> Result<CertifiedKey, String> {
    if digest.len() != 32 {
        return Err("a proof carries a SHA-256 digest".to_string());
    }
    // Lower-cased, as the server name it answers arrives: a DNS name has no case, and a validator comparing bytes
    // must find its own spelling.
    let mut params = rcgen::CertificateParams::new(vec![name.to_ascii_lowercase()])
        .map_err(|e| format!("{name:?} is not a name a certificate can carry: {e}"))?;
    params
        .custom_extensions
        .push(rcgen::CustomExtension::new_acme_identifier(digest));
    let key = rcgen::KeyPair::generate().map_err(|e| format!("a key for the proof: {e}"))?;
    let cert = params
        .self_signed(&key)
        .map_err(|e| format!("the proof for {name:?}: {e}"))?;
    // `CertifiedKey::from_der` reads the certificate back through webpki, which refuses the extension: it is
    // critical, and a client is meant to refuse it, which is the point. The key is loaded on its own.
    let signing_key = crate::crypto::in_use()
        .key_provider
        .load_private_key(PrivateKeyDer::Pkcs8(key.serialize_der().into()))
        .map_err(|e| format!("the proof for {name:?}: {e}"))?;
    Ok(CertifiedKey::new(vec![cert.der().clone()], signing_key))
}

#[cfg(test)]
mod tests {
    use rustls::pki_types::CertificateDer;

    use super::*;

    /// A certificate and key on disk, as `tls::ensure` leaves them.
    fn pair(name: &str) -> (Vec<u8>, Vec<u8>) {
        let signed = rcgen::generate_simple_self_signed(vec![name.to_string()]).unwrap();
        (
            signed.cert.pem().into_bytes(),
            signed.signing_key.serialize_pem().into_bytes(),
        )
    }

    /// Handshakes with `resolver` as `server_name`, offering `alpn`, and gives back the certificate it was shown.
    fn shown(resolver: &Arc<Resolver>, server_name: &str, alpn: &[&[u8]]) -> Result<CertificateDer<'static>, String> {
        let config = resolver.server_config().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut tcp, _) = listener.accept().unwrap();
            let mut conn = rustls::ServerConnection::new(config).unwrap();
            // Until the handshake is over, or the client has gone: a refused handshake ends with the client closing.
            while conn.is_handshaking() {
                if conn.complete_io(&mut tcp).is_err() {
                    break;
                }
            }
            let _ = conn.complete_io(&mut tcp);
        });

        // A client that takes whatever it is shown, so the test can look at it.
        #[derive(Debug)]
        struct Anything(Arc<rustls::crypto::CryptoProvider>);
        impl rustls::client::danger::ServerCertVerifier for Anything {
            fn verify_server_cert(
                &self,
                _: &CertificateDer<'_>,
                _: &[CertificateDer<'_>],
                _: &rustls::pki_types::ServerName<'_>,
                _: &[u8],
                _: rustls::pki_types::UnixTime,
            ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            // Not checked either: rustls reads the certificate through webpki to check a signature, and webpki
            // refuses the proof's critical extension, as RFC 8737 means every ordinary client to.
            fn verify_tls12_signature(
                &self,
                _: &[u8],
                _: &CertificateDer<'_>,
                _: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }
            fn verify_tls13_signature(
                &self,
                _: &[u8],
                _: &CertificateDer<'_>,
                _: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }
            fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
                self.0.signature_verification_algorithms.supported_schemes()
            }
        }
        let provider = crate::crypto::in_use();
        let mut client_config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(Anything(provider)))
            .with_no_client_auth();
        client_config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        let name = rustls::pki_types::ServerName::try_from(server_name.to_string()).unwrap();
        let mut conn = rustls::ClientConnection::new(Arc::new(client_config), name).unwrap();
        let mut tcp = std::net::TcpStream::connect(addr).unwrap();
        let outcome = loop {
            if !conn.is_handshaking() {
                break Ok(());
            }
            match conn.complete_io(&mut tcp) {
                Ok(_) => {}
                Err(e) => break Err(e.to_string()),
            }
        };
        drop(tcp);
        let _ = server.join();
        outcome?;
        let shown = conn
            .peer_certificates()
            .and_then(|certs| certs.first().cloned())
            .ok_or_else(|| "no certificate was shown".to_string())?;
        Ok(shown)
    }

    fn names_in(cert: &CertificateDer<'_>) -> Vec<String> {
        let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref()).unwrap();
        parsed
            .subject_alternative_name()
            .unwrap()
            .map(|san| san.value.general_names.iter().map(|n| format!("{n}")).collect())
            .unwrap_or_default()
    }

    #[test]
    fn a_browser_is_shown_the_certificate_in_force_and_a_reload_moves_it() {
        let (cert, key) = pair("first.test");
        let resolver = Resolver::load(&cert, &key).unwrap();
        let shown_first = shown(&resolver, "first.test", &[b"h2", b"http/1.1"]).unwrap();
        assert!(names_in(&shown_first).iter().any(|n| n.contains("first.test")));

        let (cert, key) = pair("second.test");
        resolver.reload(&cert, &key).unwrap();
        let shown_second = shown(&resolver, "first.test", &[b"http/1.1"]).unwrap();
        assert!(names_in(&shown_second).iter().any(|n| n.contains("second.test")));
        assert_ne!(shown_first, shown_second);
    }

    #[test]
    fn a_validator_is_shown_the_proof_for_the_name_it_asks_for_and_nothing_for_another() {
        let (cert, key) = pair("leader.test");
        let resolver = Resolver::load(&cert, &key).unwrap();
        let digest = [7u8; 32];
        resolver.prove("Leader.Example", &digest).unwrap();

        let proof = shown(&resolver, "leader.example", &[ACME_TLS]).unwrap();
        let (_, parsed) = x509_parser::parse_x509_certificate(proof.as_ref()).unwrap();
        let acme = parsed
            .extensions()
            .iter()
            .find(|e| e.oid.to_id_string() == "1.3.6.1.5.5.7.1.31")
            .expect("the acmeIdentifier extension");
        assert!(acme.critical);
        assert!(acme.value.ends_with(&digest));
        assert!(names_in(&proof).iter().any(|n| n.contains("leader.example")));

        assert!(
            shown(&resolver, "other.example", &[ACME_TLS]).is_err(),
            "a name nobody is proving gets no certificate"
        );
        // A browser mid-order is still shown the certificate in force.
        let browser = shown(&resolver, "leader.example", &[b"h2"]).unwrap();
        assert!(names_in(&browser).iter().any(|n| n.contains("leader.test")));

        resolver.done();
        assert!(shown(&resolver, "leader.example", &[ACME_TLS]).is_err());
    }

    #[test]
    fn an_address_is_proved_under_its_reverse_name() {
        let (cert, key) = pair("leader.test");
        let resolver = Resolver::load(&cert, &key).unwrap();
        resolver.prove("127.0.0.1", &[9u8; 32]).unwrap();
        let proof = shown(&resolver, "1.0.0.127.in-addr.arpa", &[ACME_TLS]).unwrap();
        let (_, parsed) = x509_parser::parse_x509_certificate(proof.as_ref()).unwrap();
        let carries_address = parsed
            .subject_alternative_name()
            .unwrap()
            .expect("a subject alternative name")
            .value
            .general_names
            .iter()
            .any(|n| matches!(n, x509_parser::extensions::GeneralName::IPAddress(a) if *a == [127, 0, 0, 1]));
        assert!(carries_address, "the proof carries the address, not a name");
        assert_eq!(
            asked_as("::1"),
            "1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa"
        );
    }

    #[test]
    fn a_pair_that_does_not_match_is_refused_and_changes_nothing() {
        let (cert, key) = pair("kept.test");
        let resolver = Resolver::load(&cert, &key).unwrap();
        let (other_cert, _) = pair("other.test");
        assert!(resolver.reload(&other_cert, &key).is_err());
        let still = shown(&resolver, "kept.test", &[b"h2"]).unwrap();
        assert!(names_in(&still).iter().any(|n| n.contains("kept.test")));
    }
}
