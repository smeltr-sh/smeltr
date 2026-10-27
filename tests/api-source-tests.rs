//! The API sources over a socket: what goes on the wire, how pages are followed, and what each record carries.

use std::io::{BufRead, BufReader, Write};
use std::time::Duration;

use smeltr::io::{CommitMap, Source, TopicPartition, build_source};
use smeltr::plan::topology::{
    Api, ApiRecords, ApiSourceConfig, HeldSecrets, HttpApiConfig, ListStart, OktaLogsConfig, OktaSignIn, SecretRef,
    Since, SourceKind,
};

/// A service on a socket that answers each request with the next of `answers`, a status line ending in CRLF with any
/// headers after it, and a body. Gives back each request's head, lowercased.
fn service(answers: Vec<(&'static str, String)>) -> (String, std::thread::JoinHandle<Vec<String>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let served = std::thread::spawn(move || {
        answers
            .into_iter()
            .map(|(status, body)| {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut head = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    head.push_str(&line.to_ascii_lowercase());
                }
                write!(
                    reader.get_mut(),
                    "HTTP/1.1 {status}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
                head
            })
            .collect()
    });
    (url, served)
}

fn source(url: String, follow_links: bool) -> Box<dyn Source> {
    let kind = SourceKind::Api(ApiSourceConfig {
        api: Api::HttpApi(HttpApiConfig {
            url: format!("{url}/v1/events"),
            method: smeltr::plan::topology::ApiMethod::Get,
            body: None,
            headers: vec![("Accept".into(), "application/json".into())],
            credentials: vec![(
                "X-Api-Key".into(),
                SecretRef::LeaderStored {
                    name: "api".into(),
                    version: 1,
                },
            )],
            records: ApiRecords::Array("/data".into()),
            follow_links,
            since: Some(Since {
                param: "since".into(),
                overlap: Duration::ZERO,
                start: ListStart::Everything,
            }),
            sign_in: None,
        }),
        tls: None,
        poll_interval: Duration::from_secs(3600),
    });
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("api".to_string(), 1, "k3y".to_string())]);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    build_source(&kind, &registry, &subjects, &secrets, None).expect("opens")
}

/// The headers and the credential go on every request; a poll follows the next link, and its records carry the page
/// they came from. Progress is the poll's start, once both pages are delivered.
#[test]
fn a_poll_asks_with_the_credential_follows_the_next_link_and_moves_on_delivery() {
    let (url, served) = service(vec![
        (
            "200 OK\r\nLink: </v1/events?page=2>; rel=\"next\"\r\n",
            r#"{"data":[{"n":1},{"n":2}]}"#.to_string(),
        ),
        ("200 OK\r\n", r#"{"data":[{"n":3}]}"#.to_string()),
    ]);
    let mut s = source(url.clone(), true);
    let mut records = Vec::new();
    while records.len() < 3 {
        records.extend(s.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap());
    }
    let raw: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(raw, [br#"{"n":1}"#.as_slice(), br#"{"n":2}"#, br#"{"n":3}"#]);
    assert_eq!(&*records[0].topic, format!("{url}/v1/events"));
    assert_eq!(&*records[2].topic, format!("{url}/v1/events?page=2"));

    let heads = served.join().unwrap();
    assert!(heads[0].starts_with("get /v1/events http/1.1"), "{}", heads[0]);
    assert!(heads[1].starts_with("get /v1/events?page=2 "), "{}", heads[1]);
    for head in &heads {
        assert!(head.contains("x-api-key: k3y\r\n"), "{head}");
        assert!(head.contains("accept: application/json\r\n"), "{head}");
    }

    assert_eq!(s.take_progress(), None);
    let mut done = CommitMap::new();
    for r in &records {
        done.insert(
            TopicPartition {
                topic: r.topic.clone(),
                partition: r.partition,
            },
            r.offset,
        );
    }
    s.commit(&done).unwrap();
    let cursor = s.take_progress().and_then(|p| p.cursor).expect("progress");
    assert!(cursor.ends_with('Z') && cursor.len() == 24, "{cursor}");
}

/// A refusal is shown as the source's error, and the request asked again later.
#[test]
fn a_refusing_api_is_shown() {
    let (url, served) = service(vec![("401 Unauthorized\r\n", "invalid key".to_string())]);
    let mut s = source(url, false);
    assert!(
        s.poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
    served.join().unwrap();
    let error = s.connection_error().expect("an error");
    assert!(error.contains("refused the credentials (401): invalid key"), "{error}");
}

/// A redirect is not followed: the credential would go wherever it leads.
#[test]
fn a_redirect_is_not_followed() {
    let (url, served) = service(vec![(
        "302 Found\r\nLocation: https://elsewhere.example/\r\n",
        String::new(),
    )]);
    let mut s = source(url, false);
    assert!(
        s.poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
    assert_eq!(served.join().unwrap().len(), 1);
    let error = s.connection_error().expect("an error");
    assert!(error.contains("302 to https://elsewhere.example/"), "{error}");
}

fn okta(org: String) -> Box<dyn Source> {
    let kind = SourceKind::Api(ApiSourceConfig {
        api: Api::OktaLogs(OktaLogsConfig {
            org,
            sign_in: OktaSignIn::Token(SecretRef::LeaderStored {
                name: "okta".into(),
                version: 1,
            }),
            start: ListStart::Everything,
        }),
        tls: None,
        poll_interval: Duration::from_secs(3600),
    });
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("okta".to_string(), 1, "00tok".to_string())]);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    build_source(&kind, &registry, &subjects, &secrets, None).expect("opens")
}

/// Okta is asked in its own order with the token, its next links are followed until a page holds nothing new, and
/// the source resumes at that page's next link.
#[test]
fn okta_is_read_in_order_to_a_page_of_nothing_new_and_resumes_at_its_next_link() {
    let (url, served) = service(vec![
        (
            "200 OK\r\nLink: </api/v1/logs?after=a1&sortOrder=ASCENDING&limit=1000>; rel=\"next\"\r\n",
            r#"[{"uuid":"1"},{"uuid":"2"}]"#.to_string(),
        ),
        (
            "200 OK\r\nLink: </api/v1/logs?after=a2&sortOrder=ASCENDING&limit=1000>; rel=\"next\"\r\n",
            "[]".to_string(),
        ),
    ]);
    let mut s = okta(url.clone());
    let mut records = Vec::new();
    while records.len() < 2 {
        records.extend(s.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap());
    }
    assert!(
        s.poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
    let heads = served.join().unwrap();
    assert!(
        heads[0].starts_with("get /api/v1/logs?sortorder=ascending&limit=1000 "),
        "{}",
        heads[0]
    );
    assert!(heads[1].starts_with("get /api/v1/logs?after=a1&"), "{}", heads[1]);
    for head in &heads {
        assert!(head.contains("authorization: ssws 00tok\r\n"), "{head}");
    }

    let mut done = CommitMap::new();
    for r in &records {
        done.insert(
            TopicPartition {
                topic: r.topic.clone(),
                partition: r.partition,
            },
            r.offset,
        );
    }
    s.commit(&done).unwrap();
    assert_eq!(
        s.take_progress().and_then(|p| p.cursor),
        Some(format!("{url}/api/v1/logs?after=a2&sortOrder=ASCENDING&limit=1000"))
    );
}

/// Okta's rate limit is waited out until the reset it names, not asked again at the first backoff.
#[test]
fn okta_waits_out_its_rate_limit() {
    let reset = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    let (url, served) = service(vec![(
        Box::leak(format!("429 Too Many Requests\r\nX-Rate-Limit-Reset: {reset}\r\n").into_boxed_str()),
        String::new(),
    )]);
    let mut s = okta(url);
    for _ in 0..3 {
        assert!(
            s.poll_batch(100, 1 << 20, Duration::from_millis(200))
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(served.join().unwrap().len(), 1);
    assert!(s.connection_error().unwrap().contains("429"));
}

/// One request as a service sees it: its request line, its headers by lowercase name, and its body.
struct Request {
    line: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Request {
    fn header(&self, name: &str) -> &str {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map_or("", |(_, v)| v.as_str())
    }

    /// A form field of the body.
    fn field(&self, name: &str) -> String {
        self.body
            .split('&')
            .filter_map(|p| p.split_once('='))
            .find(|(k, _)| *k == name)
            .map(|(_, v)| {
                v.replace("%3A", ":")
                    .replace("%2E", ".")
                    .replace("%2D", "-")
                    .replace("%5F", "_")
            })
            .unwrap_or_default()
    }
}

type Handler = Box<dyn FnOnce(&Request) -> (String, String) + Send>;

/// A service that answers each request with the next handler's status line, headers and body.
fn scripted(handlers: Vec<Handler>) -> (String, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let served = std::thread::spawn(move || {
        for handler in handlers {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut headers = Vec::new();
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header == "\r\n" {
                    break;
                }
                let (name, value) = header.trim_end().split_once(':').unwrap();
                headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
            }
            let length = headers
                .iter()
                .find(|(n, _)| n == "content-length")
                .map_or(0, |(_, v)| v.parse().unwrap());
            let mut body = vec![0; length];
            std::io::Read::read_exact(&mut reader, &mut body).unwrap();
            let request = Request {
                line: line.trim_end().to_string(),
                headers,
                body: String::from_utf8(body).unwrap(),
            };
            let (status, body) = handler(&request);
            write!(
                reader.get_mut(),
                "HTTP/1.1 {status}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    (url, served)
}

pub(crate) fn unb64url(text: &str) -> Vec<u8> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = Vec::new();
    let (mut bits, mut n) = (0u32, 0);
    for c in text.bytes() {
        bits = bits << 6 | ALPHABET.iter().position(|&a| a == c).expect("base64url") as u32;
        n += 6;
        if n >= 8 {
            n -= 8;
            out.push((bits >> n) as u8);
        }
    }
    out
}

/// A JWS's header and claims, once its signature verifies under `alg` with `public`.
pub(crate) fn verified(
    jws: &str,
    alg: &'static dyn aws_lc_rs::signature::VerificationAlgorithm,
    public: &[u8],
) -> (serde_json::Value, serde_json::Value) {
    let parts: Vec<&str> = jws.split('.').collect();
    assert_eq!(parts.len(), 3, "{jws}");
    aws_lc_rs::signature::UnparsedPublicKey::new(alg, public)
        .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &unb64url(parts[2]))
        .unwrap_or_else(|_| panic!("{jws} does not verify"));
    (
        serde_json::from_slice(&unb64url(parts[0])).unwrap(),
        serde_json::from_slice(&unb64url(parts[1])).unwrap(),
    )
}

/// A DPoP proof's claims, once it verifies under the key its header names, which must be `jwk` once known.
fn proof(request: &Request, jwk: &std::sync::Mutex<Option<serde_json::Value>>) -> serde_json::Value {
    let jws = request.header("dpop");
    let header: serde_json::Value = serde_json::from_slice(&unb64url(jws.split('.').next().unwrap())).unwrap();
    assert_eq!(header["typ"], "dpop+jwt");
    let mut point = vec![4];
    point.extend(unb64url(header["jwk"]["x"].as_str().unwrap()));
    point.extend(unb64url(header["jwk"]["y"].as_str().unwrap()));
    let (header, claims) = verified(jws, &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED, &point);
    let mut known = jwk.lock().unwrap();
    assert_eq!(
        known.get_or_insert(header["jwk"].clone()),
        &header["jwk"],
        "one DPoP key throughout"
    );
    claims
}

/// A new RSA app key as PEM PKCS#8, and its public key.
pub(crate) fn app_key() -> (String, Vec<u8>) {
    use aws_lc_rs::encoding::{AsDer, Pkcs8V1Der};
    use aws_lc_rs::signature::KeyPair;
    let key = aws_lc_rs::rsa::KeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048).unwrap();
    let der = AsDer::<Pkcs8V1Der>::as_der(&key).unwrap();
    let body = smeltr::format::bytes_text::BytesForm::Base64.text(der.as_ref());
    let lines: Vec<&str> = body
        .as_bytes()
        .chunks(64)
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect();
    let pem = format!(
        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
        lines.join("\n")
    );
    (pem, key.public_key().as_ref().to_vec())
}

/// An Okta service app signs in with its key assertion, takes the nonce Okta asks for, and reads with a DPoP-bound
/// token, every proof bound to the request and the token; a token refused mid-poll is replaced once. The fake org
/// verifies every signature with the public keys alone.
#[test]
fn an_okta_service_app_signs_in_with_dpop_and_replaces_a_refused_token() {
    let (pem, public) = app_key();
    let jwk = std::sync::Arc::new(std::sync::Mutex::new(None));
    let origin = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let ath = |token: &str| {
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, token.as_bytes());
        smeltr::format::bytes_text::BytesForm::Base64
            .text(digest.as_ref())
            .trim_end_matches('=')
            .replace('+', "-")
            .replace('/', "_")
    };
    let token = |n: &'static str,
                 jwk: std::sync::Arc<_>,
                 origin: std::sync::Arc<std::sync::Mutex<String>>,
                 public: Vec<u8>|
     -> Handler {
        Box::new(move |r: &Request| {
            let url = format!("{}/oauth2/v1/token", origin.lock().unwrap());
            assert_eq!(r.line, "POST /oauth2/v1/token HTTP/1.1");
            assert_eq!(r.field("grant_type"), "client_credentials");
            assert_eq!(r.field("scope"), "okta.logs.read");
            assert_eq!(
                r.field("client_assertion_type"),
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
            );
            let (header, claims) = verified(
                &r.field("client_assertion"),
                &aws_lc_rs::signature::RSA_PKCS1_2048_8192_SHA256,
                &public,
            );
            assert_eq!(header["kid"], "k1");
            assert_eq!(claims["iss"], "0oa1app");
            assert_eq!(claims["sub"], "0oa1app");
            assert_eq!(claims["aud"], url.as_str());
            let proof = proof(r, &jwk);
            assert_eq!(proof["htm"], "POST");
            assert_eq!(proof["htu"], url.as_str());
            assert_eq!(proof["nonce"], "n1");
            (
                "200 OK\r\nContent-Type: application/json\r\n".to_string(),
                format!(r#"{{"token_type":"DPoP","expires_in":3600,"access_token":"{n}","scope":"okta.logs.read"}}"#),
            )
        })
    };
    let logs = |n: &'static str,
                page: &'static str,
                next: &'static str,
                nonce: Option<&'static str>,
                jwk: std::sync::Arc<_>,
                origin: std::sync::Arc<std::sync::Mutex<String>>|
     -> Handler {
        Box::new(move |r: &Request| {
            assert_eq!(r.header("authorization"), format!("DPoP {n}"));
            let proof = proof(r, &jwk);
            assert_eq!(proof["htm"], "GET");
            assert_eq!(
                proof["htu"],
                format!("{}/api/v1/logs", origin.lock().unwrap()),
                "no query"
            );
            assert_eq!(proof["ath"], ath(n));
            assert_eq!(proof["nonce"], nonce.map_or(serde_json::Value::Null, Into::into));
            (
                format!(
                    "200 OK\r\nDPoP-Nonce: r-{next}\r\nLink: </api/v1/logs?after={next}&sortOrder=ASCENDING&limit=1000>; \
                     rel=\"next\"\r\n"
                ),
                page.to_string(),
            )
        })
    };
    let (url, served) = scripted(vec![
        Box::new(|r: &Request| {
            assert!(r.line.starts_with("POST /oauth2/v1/token "), "{}", r.line);
            (
                "400 Bad Request\r\nDPoP-Nonce: n1\r\nContent-Type: application/json\r\n".to_string(),
                r#"{"error":"use_dpop_nonce","error_description":"Authorization server requires nonce in DPoP proof."}"#
                    .to_string(),
            )
        }),
        token("at1", jwk.clone(), origin.clone(), public.clone()),
        logs("at1", r#"[{"uuid":"1"}]"#, "a1", None, jwk.clone(), origin.clone()),
        Box::new(|r: &Request| {
            assert!(r.line.starts_with("GET /api/v1/logs?after=a1&"), "{}", r.line);
            (
                "401 Unauthorized\r\nWWW-Authenticate: DPoP error=\"invalid_token\"\r\n".to_string(),
                String::new(),
            )
        }),
        token("at2", jwk.clone(), origin.clone(), public.clone()),
        // The nonce the last page named, though it came on a 200.
        logs("at2", "[]", "a2", Some("r-a1"), jwk.clone(), origin.clone()),
    ]);
    *origin.lock().unwrap() = url.clone();

    let kind = SourceKind::Api(ApiSourceConfig {
        api: Api::OktaLogs(OktaLogsConfig {
            org: url,
            sign_in: OktaSignIn::ServiceApp {
                client_id: "0oa1app".into(),
                private_key: SecretRef::LeaderStored {
                    name: "okta-key".into(),
                    version: 1,
                },
                key_id: Some("k1".into()),
            },
            start: ListStart::Everything,
        }),
        tls: None,
        poll_interval: Duration::from_secs(3600),
    });
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("okta-key".to_string(), 1, pem)]);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    let mut s = build_source(&kind, &registry, &subjects, &secrets, None).expect("opens");

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut records = Vec::new();
    while !served.is_finished() && std::time::Instant::now() < deadline {
        records.extend(s.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap());
    }
    // A failed check in the fake org fails here, with its message.
    served.join().unwrap();
    records.extend(s.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap());
    assert_eq!(records.len(), 1);
    assert_eq!(s.connection_error(), None);
}

/// A 403 from Okta says which admin role reading the System Log takes, since Okta's own answer does not.
#[test]
fn okta_refusing_the_caller_names_the_role_it_needs() {
    let (url, served) = service(vec![(
        "403 Forbidden\r\n",
        r#"{"errorCode":"E0000006","errorSummary":"You do not have permission to perform the requested action"}"#
            .to_string(),
    )]);
    let mut s = okta(url);
    s.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap();
    served.join().unwrap();
    let error = s.connection_error().unwrap();
    assert!(error.contains("Read-only Administrator"), "{error}");
}

/// A sign-in Okta refuses as `invalid_client` says what to check of the app's keys.
#[test]
fn a_refused_service_app_says_what_to_check_of_its_keys() {
    let (pem, _) = app_key();
    let (url, served) = scripted(vec![Box::new(|_: &Request| {
        (
            "401 Unauthorized\r\nContent-Type: application/json\r\n".to_string(),
            r#"{"error":"invalid_client","error_description":"The client_assertion signature is invalid."}"#
                .to_string(),
        )
    })]);
    let kind = SourceKind::Api(ApiSourceConfig {
        api: Api::OktaLogs(OktaLogsConfig {
            org: url,
            sign_in: OktaSignIn::ServiceApp {
                client_id: "0oa1app".into(),
                private_key: SecretRef::LeaderStored {
                    name: "okta-key".into(),
                    version: 1,
                },
                key_id: None,
            },
            start: ListStart::Everything,
        }),
        tls: None,
        poll_interval: Duration::from_secs(3600),
    });
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("okta-key".to_string(), 1, pem)]);
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    let mut s = build_source(&kind, &registry, &subjects, &secrets, None).expect("opens");
    s.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap();
    served.join().unwrap();
    let error = s.connection_error().unwrap();
    assert!(
        error.contains("signature is invalid") && error.contains("keyId"),
        "{error}"
    );
}

/// An HTTP API source signed in as `sign_in`, reading `{url}/v1/events` with the secret and key it names, every
/// `poll_interval`.
fn signed_in(
    url: &str,
    sign_in: smeltr::plan::topology::AppSignIn,
    pem: Option<String>,
    poll_interval: Duration,
) -> Box<dyn Source> {
    let kind = SourceKind::Api(ApiSourceConfig {
        api: Api::HttpApi(HttpApiConfig {
            url: format!("{url}/v1/events"),
            method: smeltr::plan::topology::ApiMethod::Get,
            body: None,
            headers: Vec::new(),
            credentials: Vec::new(),
            records: ApiRecords::Array(String::new()),
            follow_links: false,
            since: None,
            sign_in: Some(sign_in),
        }),
        tls: None,
        poll_interval,
    });
    let mut secrets = HeldSecrets::default();
    let secret = ("app-secret".to_string(), 1, "s3cr&t".to_string());
    secrets.set_stored(std::iter::once(secret).chain(pem.map(|pem| ("app-key".to_string(), 1, pem))));
    let registry = std::sync::Arc::new(smeltr::metrics::Metrics::new());
    let subjects = std::sync::Arc::new(smeltr::metrics::Subjects::default());
    build_source(&kind, &registry, &subjects, &secrets, None).expect("opens")
}

fn app_secret() -> smeltr::plan::topology::AppProof {
    smeltr::plan::topology::AppProof::Secret(SecretRef::LeaderStored {
        name: "app-secret".into(),
        version: 1,
    })
}

/// Polls `s` until `served` has answered every request it scripted, and gives back the records read.
fn drain(s: &mut Box<dyn Source>, served: std::thread::JoinHandle<()>) -> Vec<smeltr::io::SourceRecord> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut records = Vec::new();
    while !served.is_finished() && std::time::Instant::now() < deadline {
        records.extend(s.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap());
    }
    // A failed check in the fake service fails here, with its message.
    served.join().unwrap();
    records
}

fn json_answer(body: String) -> (String, String) {
    ("200 OK\r\nContent-Type: application/json\r\n".to_string(), body)
}

/// An app naming an issuer finds the token endpoint in its discovery document, sends its secret the way the
/// document says the endpoint takes it, and sends the token it gets on every request.
#[test]
fn an_app_finds_its_token_endpoint_through_the_issuer_and_sends_the_token() {
    use smeltr::plan::topology::{AppSignIn, TokenAt};
    let origin = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let (o1, o2) = (origin.clone(), origin.clone());
    let (url, served) = scripted(vec![
        Box::new(move |r: &Request| {
            assert_eq!(r.line, "GET /realm/.well-known/openid-configuration HTTP/1.1");
            let issuer = format!("{}/realm", o1.lock().unwrap());
            json_answer(
                serde_json::json!({
                    "issuer": issuer,
                    "token_endpoint": format!("{issuer}/token"),
                    "token_endpoint_auth_methods_supported": ["private_key_jwt", "client_secret_basic"],
                })
                .to_string(),
            )
        }),
        Box::new(|r: &Request| {
            assert_eq!(r.line, "POST /realm/token HTTP/1.1");
            assert_eq!(r.header("authorization"), "Basic YXBwOnMzY3IlMjZ0");
            assert_eq!(r.field("grant_type"), "client_credentials");
            assert_eq!(r.field("scope"), "read:events");
            assert_eq!(r.field("client_secret"), "", "the secret goes in the header alone");
            assert_eq!(r.header("dpop"), "", "no DPoP proof unless asked for");
            json_answer(r#"{"access_token":"at1","token_type":"Bearer","expires_in":3600}"#.to_string())
        }),
        Box::new(|r: &Request| {
            assert_eq!(r.line, "GET /v1/events HTTP/1.1");
            assert_eq!(r.header("authorization"), "Bearer at1");
            json_answer(r#"[{"n":1}]"#.to_string())
        }),
    ]);
    *o2.lock().unwrap() = url.clone();
    let mut s = signed_in(
        &url,
        AppSignIn {
            client_id: "app".into(),
            token_at: TokenAt::Issuer(format!("{url}/realm")),
            scope: Some("read:events".into()),
            proof: app_secret(),
        },
        None,
        Duration::from_secs(3600),
    );
    assert_eq!(drain(&mut s, served).len(), 1);
    assert_eq!(s.connection_error(), None);
}

/// A token the API refuses after it let it through is replaced, and the request asked again once. A token endpoint
/// given whole takes the secret in the form.
#[test]
fn a_token_refused_after_use_is_replaced_and_the_request_asked_again() {
    use smeltr::plan::topology::{AppSignIn, TokenAt};
    let token = |n: &'static str| -> Handler {
        Box::new(move |r: &Request| {
            assert_eq!(r.line, "POST /connect/token HTTP/1.1");
            assert_eq!(r.header("authorization"), "");
            assert_eq!(r.field("client_id"), "app");
            assert_eq!(r.field("client_secret"), "s3cr%26t");
            json_answer(format!(r#"{{"access_token":"{n}","expires_in":3600}}"#))
        })
    };
    let events = |n: &'static str, status: &'static str| -> Handler {
        Box::new(move |r: &Request| {
            assert_eq!(r.header("authorization"), format!("Bearer {n}"));
            (format!("{status}\r\n"), "[]".to_string())
        })
    };
    let (url, served) = scripted(vec![
        token("at1"),
        events("at1", "200 OK"),
        events("at1", "401 Unauthorized"),
        token("at2"),
        events("at2", "200 OK"),
    ]);
    let mut s = signed_in(
        &url,
        AppSignIn {
            client_id: "app".into(),
            token_at: TokenAt::Url(format!("{url}/connect/token")),
            scope: None,
            proof: app_secret(),
        },
        None,
        // A second poll, to use the token again.
        Duration::from_millis(10),
    );
    // Each scripted request checks the token it carries; a request out of order fails here.
    drain(&mut s, served);
}

/// A token the API refuses as soon as it is given is a refusal to show, not one to ask again. It is dropped, so the
/// next poll signs in again rather than sending it once more: a provider that names no lifetime would otherwise have
/// it sent until the worker restarts.
#[test]
fn a_new_token_refused_is_shown_and_the_next_poll_signs_in_again() {
    use smeltr::plan::topology::{AppSignIn, TokenAt};
    let (url, served) = scripted(vec![
        Box::new(|_: &Request| json_answer(r#"{"access_token":"at1"}"#.to_string())),
        Box::new(|_: &Request| ("401 Unauthorized\r\n".to_string(), "no".to_string())),
        Box::new(|_: &Request| json_answer(r#"{"access_token":"at2"}"#.to_string())),
        Box::new(|r: &Request| {
            assert_eq!(r.header("authorization"), "Bearer at2");
            ("200 OK\r\n".to_string(), "[]".to_string())
        }),
    ]);
    let mut s = signed_in(
        &url,
        AppSignIn {
            client_id: "app".into(),
            token_at: TokenAt::Url(format!("{url}/token")),
            scope: None,
            proof: app_secret(),
        },
        None,
        Duration::from_millis(10),
    );
    s.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap();
    let error = s.connection_error().expect("an error");
    assert!(error.contains("(401): no"), "{error}");
    drain(&mut s, served);
}

/// An app with a private key signs in with an assertion for the token endpoint, and offers no DPoP proof.
#[test]
fn an_app_with_a_key_signs_in_with_an_assertion() {
    use smeltr::plan::topology::{AppProof, AppSignIn, TokenAt};
    let (pem, public) = app_key();
    let origin = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = origin.clone();
    let (url, served) = scripted(vec![
        Box::new(move |r: &Request| {
            assert_eq!(r.header("dpop"), "");
            assert_eq!(r.field("scope"), "");
            let (header, claims) = verified(
                &r.field("client_assertion"),
                &aws_lc_rs::signature::RSA_PKCS1_2048_8192_SHA256,
                &public,
            );
            assert_eq!(header["kid"], serde_json::Value::Null);
            assert_eq!(claims["sub"], "app");
            assert_eq!(claims["aud"], format!("{}/token", seen.lock().unwrap()));
            json_answer(r#"{"access_token":"at1","token_type":"Bearer","expires_in":3600}"#.to_string())
        }),
        Box::new(|r: &Request| {
            assert_eq!(r.header("authorization"), "Bearer at1");
            json_answer("[]".to_string())
        }),
    ]);
    *origin.lock().unwrap() = url.clone();
    let mut s = signed_in(
        &url,
        AppSignIn {
            client_id: "app".into(),
            token_at: TokenAt::Url(format!("{url}/token")),
            scope: None,
            proof: AppProof::Key {
                private_key: SecretRef::LeaderStored {
                    name: "app-key".into(),
                    version: 1,
                },
                key_id: None,
            },
        },
        Some(pem),
        Duration::from_secs(3600),
    );
    drain(&mut s, served);
    assert_eq!(s.connection_error(), None);
}
