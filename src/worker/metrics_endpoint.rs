//! The metrics endpoint: a worker's registry served as Prometheus scrapes it, at `GET /metrics` (`metrics::exposition`).
//!
//! The supervisor serves it, because it lives as long as the worker and a runtime is replaced at every deploy. It
//! holds its own series and mirrors its runtime's, so one scrape reads the whole worker.
//!
//! Set from the startup file, since which port a host opens, and the certificate it presents there, are facts about the
//! host. Everything is read and the port bound before the worker says it is starting, so a setting that cannot work
//! refuses to start rather than leaving a worker nobody can scrape. A new certificate therefore takes a restart.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_lc_rs::constant_time::verify_slices_are_equal;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use rustls::pki_types::CertificateDer;

use crate::io::{receiver, trust};
use crate::metrics::exposition;
use crate::plan::topology::{Listen, Transport};
use crate::startup_config as cfg;

/// Where a scraper reads, Prometheus's default.
pub const PATH: &str = "/metrics";

/// The metrics endpoint, bound and ready to serve.
pub struct MetricsEndpoint {
    listener: TcpListener,
    address: SocketAddr,
    tls: Option<rustls::ServerConfig>,
    /// The hash of the basic auth credentials a scraper must send, when one must ([`basic_hash`]).
    basic: Option<String>,
}

impl std::fmt::Debug for MetricsEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsEndpoint")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// The metrics endpoint the startup settings ask for, bound, or `None` when they leave it off. An error says which setting
/// cannot work, for an operator to read.
pub fn open() -> Result<Option<MetricsEndpoint>, String> {
    let settings = Settings {
        address: SocketAddr::new(cfg::worker_listen_address(), cfg::worker_metrics_endpoint_port()),
        tls_cert: cfg::worker_metrics_endpoint_tls_cert(),
        tls_key: cfg::worker_metrics_endpoint_tls_key(),
        client_ca: cfg::worker_metrics_endpoint_client_ca(),
        user: cfg::worker_metrics_endpoint_user(),
        password: cfg::worker_metrics_endpoint_password(),
    };
    if !cfg::worker_metrics_endpoint_enabled() {
        // An operator who set these believes the endpoint is serving, and nothing else would tell him it is not.
        let ignored = settings.given();
        if !ignored.is_empty() {
            tracing::warn!(
                target: "worker.metrics_endpoint_ignored",
                settings = %ignored.join(", "),
                "ignoring the metrics endpoint's settings, since worker.metrics_endpoint.enabled is false"
            );
        }
        return Ok(None);
    }
    settings.bind().map(Some)
}

/// What the startup settings say of the metrics endpoint, once it is on.
struct Settings {
    address: SocketAddr,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    client_ca: Option<PathBuf>,
    user: Option<String>,
    password: Option<String>,
}

impl Settings {
    /// The names of the settings given beyond the address, each of which only an endpoint that is on reads.
    fn given(&self) -> Vec<&'static str> {
        [
            ("tls_cert", self.tls_cert.is_some()),
            ("tls_key", self.tls_key.is_some()),
            ("client_ca", self.client_ca.is_some()),
            ("user", self.user.is_some()),
            ("password", self.password.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, given)| given.then_some(name))
        .collect()
    }

    fn bind(self) -> Result<MetricsEndpoint, String> {
        let tls = match (&self.tls_cert, &self.tls_key) {
            (None, None) if self.client_ca.is_some() => {
                return Err(
                    "SMELTR_WORKER_METRICS_ENDPOINT_CLIENT_CA is set without SMELTR_WORKER_METRICS_ENDPOINT_TLS_CERT and \
                     SMELTR_WORKER_METRICS_ENDPOINT_TLS_KEY: a client certificate is asked for only over TLS"
                        .to_string(),
                );
            }
            (None, None) => None,
            (Some(cert), Some(key)) => Some(tls(cert, key, self.client_ca.as_deref())?),
            _ => {
                return Err(
                    "SMELTR_WORKER_METRICS_ENDPOINT_TLS_CERT and SMELTR_WORKER_METRICS_ENDPOINT_TLS_KEY are set one without the other"
                        .to_string(),
                );
            }
        };
        let basic = match (&self.user, &self.password) {
            (None, None) => None,
            (Some(user), Some(password)) => Some(basic_hash(user, password)),
            _ => {
                return Err(
                    "SMELTR_WORKER_METRICS_ENDPOINT_USER and SMELTR_WORKER_METRICS_ENDPOINT_PASSWORD are set one without the other"
                        .to_string(),
                );
            }
        };
        let address = self.address;
        let socket = receiver::bind(Listen {
            transport: Transport::Tcp,
            address,
        })
        .and_then(|socket| socket.set_nonblocking(true).map(|()| socket))
        .map_err(|e| format!("the metrics endpoint could not listen on {address}: {e}"))?;
        Ok(MetricsEndpoint {
            listener: socket.into(),
            address,
            tls,
            basic,
        })
    }
}

impl MetricsEndpoint {
    /// Serves `scrape`'s answer at [`PATH`] for the life of the process.
    pub fn serve(self, scrape: impl Fn() -> String + Send + Sync + 'static) -> Result<SocketAddr, String> {
        let runtime = receiver::receiver_runtime().map_err(|e| e.to_string())?;
        let tls = self
            .tls
            .map(|tls| axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(tls)));
        let state = Arc::new(Scraped {
            scrape: Box::new(scrape),
            basic: self.basic,
        });
        let router = axum::Router::new()
            .route(PATH, axum::routing::get(answer))
            .with_state(state);
        let bound = self.listener.local_addr().map_err(|e| e.to_string())?;
        // The handle is dropped and the server kept: it serves until the process ends.
        receiver::serve_router(
            runtime,
            self.listener,
            tls,
            router,
            self.address,
            "the metrics endpoint",
        )
        .map_err(|e| e.to_string())?;
        Ok(bound)
    }
}

struct Scraped {
    scrape: Box<dyn Fn() -> String + Send + Sync>,
    basic: Option<String>,
}

async fn answer(axum::extract::State(scraped): axum::extract::State<Arc<Scraped>>, headers: HeaderMap) -> Response {
    if let Some(expected) = &scraped.basic {
        // Hashed before comparing, so the time a comparison takes says nothing of the password's length either.
        let sent = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("basic"))
            .map(|(_, credentials)| crate::leader::auth::hash_secret(credentials.trim()));
        if !sent.is_some_and(|sent| verify_slices_are_equal(sent.as_bytes(), expected.as_bytes()).is_ok()) {
            let mut refused = StatusCode::UNAUTHORIZED.into_response();
            refused.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"smeltr\""),
            );
            return refused;
        }
    }
    // Writing out a large registry takes a while, which the server's own threads should not spend.
    let scraped = scraped.clone();
    match tokio::task::spawn_blocking(move || (scraped.scrape)()).await {
        Ok(text) => ([(header::CONTENT_TYPE, exposition::CONTENT_TYPE)], text).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// The hash of the credentials basic auth sends for `user` and `password`.
fn basic_hash(user: &str, password: &str) -> String {
    use base64::Engine as _;
    crate::leader::auth::hash_secret(&base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}")))
}

/// The TLS the metrics endpoint serves under, from PEM files.
fn tls(cert: &Path, key: &Path, client_ca: Option<&Path>) -> Result<rustls::ServerConfig, String> {
    let certificates = |path: &Path, var: &str| -> Result<Vec<CertificateDer<'static>>, String> {
        let all = trust::certificates_file(path).map_err(|e| format!("{var} is not a PEM certificate file: {e}"))?;
        if all.is_empty() {
            return Err(format!("{var} ({}) holds no certificate", path.display()));
        }
        Ok(all)
    };
    let chain = certificates(cert, "SMELTR_WORKER_METRICS_ENDPOINT_TLS_CERT")?;
    let key = trust::key_file(key)
        .map_err(|e| format!("SMELTR_WORKER_METRICS_ENDPOINT_TLS_KEY is not an unencrypted PEM private key: {e}"))?;
    let authorities = client_ca
        .map(|path| certificates(path, "SMELTR_WORKER_METRICS_ENDPOINT_CLIENT_CA"))
        .transpose()?;
    let mut tls = receiver::serving_tls(chain, key, authorities).map_err(|(what, e)| match what {
        Some(what) => format!("the metrics endpoint's TLS settings, {what}: {e}"),
        None => format!("the metrics endpoint's TLS: {e}"),
    })?;
    tls.alpn_protocols = receiver::http_alpn();
    Ok(tls)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject as _;

    use super::*;

    fn settings() -> Settings {
        Settings {
            address: "127.0.0.1:0".parse().unwrap(),
            tls_cert: None,
            tls_key: None,
            client_ca: None,
            user: None,
            password: None,
        }
    }

    #[test]
    fn the_settings_given_are_named_in_order() {
        assert!(settings().given().is_empty());
        let given = Settings {
            user: Some("prometheus".to_string()),
            password: Some("s3cret".to_string()),
            tls_cert: Some("/etc/smeltr/metrics.pem".into()),
            ..settings()
        };
        assert_eq!(given.given(), ["tls_cert", "user", "password"]);
    }

    fn serving(settings: Settings) -> SocketAddr {
        settings
            .bind()
            .unwrap()
            .serve(|| "# TYPE smeltr_rows_in_total counter\nsmeltr_rows_in_total 3\n".to_string())
            .unwrap()
    }

    /// One HTTP/1.1 request on `stream`, and the whole answer.
    fn get(mut stream: impl Read + Write, path: &str, authorization: Option<&str>) -> std::io::Result<String> {
        let authorization = authorization
            .map(|value| format!("Authorization: {value}\r\n"))
            .unwrap_or_default();
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: localhost\r\n{authorization}Connection: close\r\n\r\n"
        )?;
        let mut answer = String::new();
        stream.read_to_string(&mut answer)?;
        Ok(answer)
    }

    fn plain(at: SocketAddr, path: &str, authorization: Option<&str>) -> String {
        get(std::net::TcpStream::connect(at).unwrap(), path, authorization).unwrap()
    }

    #[test]
    fn a_scrape_reads_the_text_format_at_its_path_and_nothing_else_is_served() {
        let at = serving(settings());
        let answer = plain(at, PATH, None);
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
        assert!(
            answer.contains(&format!("content-type: {}", exposition::CONTENT_TYPE)),
            "{answer}"
        );
        assert!(answer.ends_with("smeltr_rows_in_total 3\n"), "{answer}");
        assert!(plain(at, "/", None).starts_with("HTTP/1.1 404"));
    }

    #[test]
    fn basic_auth_admits_only_its_own_user_and_password() {
        let at = serving(Settings {
            user: Some("prometheus".into()),
            password: Some("s3cret".into()),
            ..settings()
        });
        for refused in [None, Some("Basic cHJvbWV0aGV1czp3cm9uZw=="), Some("Bearer s3cret")] {
            let answer = plain(at, PATH, refused);
            assert!(answer.starts_with("HTTP/1.1 401"), "{refused:?}: {answer}");
            assert!(answer.contains("www-authenticate: Basic realm=\"smeltr\""), "{answer}");
        }
        // prometheus:s3cret, under the scheme spelled either way.
        for admitted in ["Basic cHJvbWV0aGV1czpzM2NyZXQ=", "basic cHJvbWV0aGV1czpzM2NyZXQ="] {
            let answer = plain(at, PATH, Some(admitted));
            assert!(answer.starts_with("HTTP/1.1 200"), "{admitted}: {answer}");
        }
    }

    #[test]
    fn half_a_pair_of_settings_refuses() {
        for (half, says) in [
            (
                Settings {
                    user: Some("prometheus".into()),
                    ..settings()
                },
                "SMELTR_WORKER_METRICS_ENDPOINT_PASSWORD",
            ),
            (
                Settings {
                    tls_key: Some("/key.pem".into()),
                    ..settings()
                },
                "SMELTR_WORKER_METRICS_ENDPOINT_TLS_CERT",
            ),
            (
                Settings {
                    client_ca: Some("/ca.pem".into()),
                    ..settings()
                },
                "SMELTR_WORKER_METRICS_ENDPOINT_CLIENT_CA",
            ),
        ] {
            let refused = half.bind().expect_err(says);
            assert!(refused.contains(says), "{refused}");
        }
    }

    #[test]
    fn a_port_already_held_refuses() {
        let held = TcpListener::bind("127.0.0.1:0").unwrap();
        let refused = Settings {
            address: held.local_addr().unwrap(),
            ..settings()
        }
        .bind()
        .expect_err("the port is held");
        assert!(refused.contains("could not listen"), "{refused}");
    }

    /// Under TLS with a client authority, a scraper presenting no certificate is refused at the handshake, and one
    /// presenting a certificate that chains to it reads.
    #[test]
    fn mtls_admits_only_a_scraper_whose_certificate_chains_to_the_client_authority() {
        let dir = std::env::temp_dir().join(format!("smeltr-metrics-port-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let server_key = rcgen::KeyPair::generate().unwrap();
        let server = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&server_key)
            .unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let ca_issuer = rcgen::Issuer::new(ca_params, &ca_key);
        let client_key = rcgen::KeyPair::generate().unwrap();
        let client = rcgen::CertificateParams::new(vec!["prometheus".to_string()])
            .unwrap()
            .signed_by(&client_key, &ca_issuer)
            .unwrap();
        for (name, pem) in [
            ("cert.pem", server.pem()),
            ("key.pem", server_key.serialize_pem()),
            ("ca.pem", ca.pem()),
        ] {
            std::fs::write(dir.join(name), pem).unwrap();
        }
        let at = serving(Settings {
            tls_cert: Some(dir.join("cert.pem")),
            tls_key: Some(dir.join("key.pem")),
            client_ca: Some(dir.join("ca.pem")),
            ..settings()
        });

        let scrape = |presenting: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>| {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(server.der().clone()).unwrap();
            let builder = rustls::ClientConfig::builder_with_provider(crate::crypto::in_use())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots);
            let config = match presenting {
                Some((cert, key)) => builder.with_client_auth_cert(vec![cert], key).unwrap(),
                None => builder.with_no_client_auth(),
            };
            let connection = rustls::ClientConnection::new(Arc::new(config), "localhost".try_into().unwrap()).unwrap();
            get(
                rustls::StreamOwned::new(connection, std::net::TcpStream::connect(at).unwrap()),
                PATH,
                None,
            )
        };
        assert!(scrape(None).is_err(), "a scraper with no certificate read");
        let key = PrivateKeyDer::from_pem_slice(client_key.serialize_pem().as_bytes()).unwrap();
        let answer = scrape(Some((client.der().clone(), key))).unwrap();
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
