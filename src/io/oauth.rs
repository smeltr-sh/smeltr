//! Signing in to an API as an app: OAuth 2.0 client credentials, the app proving itself with an assertion signed by
//! its private key (RFC 7523), for a token sent on every request until shortly before it expires. When the service
//! binds its tokens to a key (DPoP, RFC 9449), each request also carries a proof signed by a key this source made when
//! it opened, which never leaves it.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant S as source
//!   participant T as token endpoint
//!   participant A as API
//!   S->>T: client_credentials, signed assertion, DPoP proof
//!   T-->>S: 400 use_dpop_nonce, DPoP-Nonce
//!   S->>T: the same, the proof carrying the nonce
//!   T-->>S: token, its type, when it expires
//!   S->>A: Authorization: DPoP token, DPoP proof (method, address, token hash)
//!   A-->>S: 401: a nonce named, or the token refused
//!   S->>A: asked again, once, with the nonce or a new token
//! ```

use std::time::{Duration, SystemTime};

use aws_lc_rs::rand::SecureRandom;
use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair, RSA_PKCS1_SHA256, RsaKeyPair};
use base64::Engine as _;

use super::IoError;
use super::api::{Answer, Authorize};

/// How long an assertion is good for. Okta takes at most an hour.
const ASSERTION_LIFE: u64 = 300;

pub(crate) const JWT_BEARER: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// The private key an app proves itself with.
pub struct AppKey {
    key: RsaKeyPair,
    /// Which of the app's keys this is, when the service holds several.
    kid: Option<String>,
}

impl AppKey {
    /// An RSA key written as PEM PKCS#8, `-----BEGIN PRIVATE KEY-----`, as Okta and `openssl genpkey` write one.
    pub fn from_pem(pem: &str, kid: Option<String>) -> Result<AppKey, String> {
        use rustls::pki_types::PrivateKeyDer;
        use rustls::pki_types::pem::PemObject;
        let key = match PrivateKeyDer::from_pem_slice(pem.trim().as_bytes()) {
            Ok(PrivateKeyDer::Pkcs8(der)) => RsaKeyPair::from_pkcs8(der.secret_pkcs8_der())
                .map_err(|e| format!("the private key is not an RSA key of 2048 bits or more: {e}"))?,
            Ok(_) => {
                return Err(
                    "the private key is not PKCS#8 (BEGIN PRIVATE KEY); `openssl pkcs8 -topk8 -nocrypt` \
                            converts one"
                        .to_string(),
                );
            }
            Err(e) => return Err(format!("the private key is not a PEM key: {e}")),
        };
        Ok(AppKey { key, kid })
    }
}

/// An app's sign-in at one token endpoint, and the token it holds.
pub struct SignIn {
    token_url: String,
    client_id: String,
    scope: Option<String>,
    key: AppKey,
    /// Whether the sign-in offers a DPoP proof, so the service may bind the token to this source's key.
    offer_dpop: bool,
    /// This source's DPoP key, made when it opened.
    dpop: EcdsaKeyPair,
    /// `dpop`'s public half, as a JWK.
    jwk: serde_json::Value,
    /// The nonce the token endpoint last named, and the one the API last named: each names its own.
    token_nonce: Option<String>,
    api_nonce: Option<String>,
    token: Option<Token>,
    random: aws_lc_rs::rand::SystemRandom,
}

struct Token {
    granted: super::client_credentials::Token,
    /// Whether it is bound to the DPoP key, so each request proves it holds that key.
    dpop: bool,
    /// Whether no request has used it yet, so a refusal of it is not a refusal of an expired token.
    fresh: bool,
}

impl SignIn {
    /// Signs in at `token_url` as `client_id`, proven by `key`. `scope` is sent only when given. No DPoP proof is
    /// offered until [`SignIn::offering_dpop`].
    /// A sign-in that could not be made gives its key back, so a caller can try again on its next poll.
    pub fn new(
        token_url: String,
        client_id: String,
        scope: Option<String>,
        key: AppKey,
    ) -> Result<SignIn, (String, AppKey)> {
        let Ok(dpop) = EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING) else {
            return Err(("could not make a DPoP key".to_string(), key));
        };
        // The public key is the uncompressed point: 0x04, then x and y.
        let point = dpop.public_key().as_ref();
        let jwk = serde_json::json!({
            "kty": "EC",
            "crv": "P-256",
            "x": b64url(&point[1..33]),
            "y": b64url(&point[33..65]),
        });
        Ok(SignIn {
            token_url,
            client_id,
            scope,
            key,
            offer_dpop: false,
            dpop,
            jwk,
            token_nonce: None,
            api_nonce: None,
            token: None,
            random: aws_lc_rs::rand::SystemRandom::new(),
        })
    }

    /// Offers a DPoP proof with every sign-in. A service that binds tokens then binds this source's to its key, and
    /// one that does not answers a Bearer token. A service that binds every token it is offered a proof for leaves
    /// an API that takes only Bearer tokens unable to take this source's.
    pub fn offering_dpop(mut self) -> SignIn {
        self.offer_dpop = true;
        self
    }

    /// The headers that authorize a GET of `url`, signing in first when no token is held or the one held is about to
    /// expire.
    fn headers(&mut self, agent: &ureq::Agent, timeout: Duration, url: &str) -> Result<Headers, IoError> {
        if self.token.as_ref().is_none_or(|t| t.granted.due()) {
            self.sign_in(agent, timeout)?;
        }
        let token = self.token.as_ref().expect("a token");
        if !token.dpop {
            return Ok(vec![(
                "Authorization".to_string(),
                format!("Bearer {}", token.granted.value),
            )]);
        }
        let value = &token.granted.value;
        let ath = b64url(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, value.as_bytes()).as_ref());
        let authorization = format!("DPoP {value}");
        let proof = self.proof("GET", url, Some(&ath), self.api_nonce.clone())?;
        Ok(vec![
            ("Authorization".to_string(), authorization),
            ("DPoP".to_string(), proof),
        ])
    }

    /// Takes what the API's 401 says. True when the request is worth sending again: the API named a nonce, or it
    /// refused a token that had been used before, which is replaced. A new token refused is a refusal to show.
    fn take_refusal(&mut self, answer: &Answer) -> bool {
        let nonce_asked = answer
            .header("www-authenticate")
            .is_some_and(|v| v.contains("use_dpop_nonce"));
        if nonce_asked && let Some(nonce) = answer.header("dpop-nonce") {
            self.api_nonce = Some(nonce.to_string());
            return true;
        }
        // Dropped either way, so a token refused is never sent again; only one used before is worth asking again now.
        self.token.take().is_some_and(|token| !token.fresh)
    }

    /// Takes what an answer the API let through says: the token held is used, and a nonce it names is the one the
    /// next request carries, since the API may move it on any answer.
    fn take_answer(&mut self, answer: &Answer) {
        if let Some(token) = &mut self.token {
            token.fresh = false;
        }
        if let Some(nonce) = answer.header("dpop-nonce") {
            self.api_nonce = Some(nonce.to_string());
        }
    }

    /// Trades a new assertion for a token. A nonce the endpoint asks for is taken and asked with once.
    fn sign_in(&mut self, agent: &ureq::Agent, timeout: Duration) -> Result<(), IoError> {
        let url = self.token_url.clone();
        for _ in 0..2 {
            let assertion = self.assertion()?;
            let mut request = agent
                .post(&url)
                .config()
                .timeout_global(Some(timeout))
                .http_status_as_error(false)
                .max_redirects(0)
                .build()
                .header("Accept", "application/json");
            if self.offer_dpop {
                request = request.header("DPoP", &self.proof("POST", &url, None, self.token_nonce.clone())?);
            }
            let mut form = vec![
                ("grant_type", "client_credentials"),
                ("client_assertion_type", JWT_BEARER),
                ("client_assertion", assertion.as_str()),
            ];
            if let Some(scope) = &self.scope {
                form.push(("scope", scope.as_str()));
            }
            let mut response = request
                .send_form(form)
                .map_err(|e| IoError::transient(format!("signing in at {url} failed: {e}")))?;
            if let Some(nonce) = response.headers().get("dpop-nonce").and_then(|v| v.to_str().ok()) {
                self.token_nonce = Some(nonce.to_string());
            }
            let (status, body, answer) = super::client_credentials::read_answer(&url, &mut response)?;
            let text = |field: &str| answer.get(field).and_then(|v| v.as_str()).unwrap_or_default();
            if status == 400 && text("error") == "use_dpop_nonce" {
                continue;
            }
            if !(200..300).contains(&status) {
                let why = super::client_credentials::why_refused(&body, &answer);
                // The service names the key it refused, never why; this is what an admin gets wrong.
                let why = match text("error") {
                    "invalid_client" => format!(
                        "{why}. Check that the app holds this key's public half and that it is active; an app with \
                         several active keys needs keyId"
                    ),
                    _ => why,
                };
                return Err(IoError::transient(format!(
                    "{url} refused the sign-in ({status}): {why}"
                )));
            }
            self.token = Some(Token {
                granted: super::client_credentials::granted(&url, &answer)?,
                // The type it came back as is what it is: a service that does not bind tokens says Bearer.
                dpop: text("token_type").eq_ignore_ascii_case("DPoP"),
                fresh: true,
            });
            return Ok(());
        }
        Err(IoError::transient(format!(
            "{url} asked for a new DPoP nonce twice running"
        )))
    }

    /// The app's proof of itself, for the token endpoint.
    fn assertion(&self) -> Result<String, IoError> {
        let now = epoch_secs();
        let mut header = serde_json::json!({"alg": "RS256", "typ": "JWT"});
        if let Some(kid) = &self.key.kid {
            header["kid"] = kid.as_str().into();
        }
        let claims = serde_json::json!({
            "iss": self.client_id,
            "sub": self.client_id,
            "aud": self.token_url,
            "iat": now,
            "exp": now + ASSERTION_LIFE,
            "jti": self.jti()?,
        });
        let input = signing_input(&header, &claims);
        let mut signature = vec![0; self.key.key.public_modulus_len()];
        self.key
            .key
            .sign(&RSA_PKCS1_SHA256, &self.random, input.as_bytes(), &mut signature)
            .map_err(|_| IoError::transient("could not sign the assertion"))?;
        Ok(format!("{input}.{}", b64url(&signature)))
    }

    /// A DPoP proof for one request: `method` of `url`, with the token's hash `ath` when it carries one.
    fn proof(&self, method: &str, url: &str, ath: Option<&str>, nonce: Option<String>) -> Result<String, IoError> {
        let mut htu = reqwest::Url::parse(url).map_err(|e| IoError::transient(format!("{url} is not a URL: {e}")))?;
        htu.set_query(None);
        htu.set_fragment(None);
        let header = serde_json::json!({"typ": "dpop+jwt", "alg": "ES256", "jwk": self.jwk});
        let mut claims = serde_json::json!({
            "jti": self.jti()?,
            "htm": method,
            "htu": htu.as_str(),
            "iat": epoch_secs(),
        });
        if let Some(ath) = ath {
            claims["ath"] = ath.into();
        }
        if let Some(nonce) = nonce {
            claims["nonce"] = nonce.into();
        }
        let input = signing_input(&header, &claims);
        let signature = self
            .dpop
            .sign(&self.random, input.as_bytes())
            .map_err(|_| IoError::transient("could not sign a DPoP proof"))?;
        Ok(format!("{input}.{}", b64url(signature.as_ref())))
    }

    /// A value no other assertion or proof of this source's carries.
    fn jti(&self) -> Result<String, IoError> {
        let mut bytes = [0u8; 16];
        self.random
            .fill(&mut bytes)
            .map_err(|_| IoError::transient("could not draw a random jti"))?;
        Ok(b64url(&bytes))
    }
}

impl Authorize for SignIn {
    fn authorize(&mut self, agent: &ureq::Agent, timeout: Duration, url: &str) -> Result<Headers, IoError> {
        self.headers(agent, timeout, url)
    }

    fn refused(&mut self, answer: &Answer) -> bool {
        self.take_refusal(answer)
    }

    fn used(&mut self, answer: &Answer) {
        self.take_answer(answer)
    }
}

/// Headers to send, by name.
pub type Headers = Vec<(String, String)>;

/// A JWS's header and claims, encoded and joined: what its signature signs.
fn signing_input(header: &serde_json::Value, claims: &serde_json::Value) -> String {
    format!(
        "{}.{}",
        b64url(header.to_string().as_bytes()),
        b64url(claims.to_string().as_bytes())
    )
}

/// URL-safe base64 with no padding, as JOSE writes it.
fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::bytes_text::BytesForm;
    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED, RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};

    /// A new app key, as PEM, and its public key.
    pub(crate) fn app_key() -> (String, Vec<u8>) {
        let key = RsaKeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048).unwrap();
        let der = AsDer::<aws_lc_rs::encoding::Pkcs8V1Der>::as_der(&key).unwrap();
        let body = BytesForm::Base64.text(der.as_ref());
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

    fn unb64url(text: &str) -> Vec<u8> {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = Vec::new();
        let (mut bits, mut n) = (0u32, 0);
        for c in text.bytes() {
            bits = bits << 6 | ALPHABET.iter().position(|&a| a == c).unwrap() as u32;
            n += 6;
            if n >= 8 {
                n -= 8;
                out.push((bits >> n) as u8);
            }
        }
        out
    }

    /// A JWS's header and claims, once its signature is checked by `verify`.
    fn opened(jws: &str, verify: impl Fn(&[u8], &[u8])) -> (serde_json::Value, serde_json::Value) {
        let parts: Vec<&str> = jws.split('.').collect();
        assert_eq!(parts.len(), 3, "{jws}");
        verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &unb64url(parts[2]));
        (
            serde_json::from_slice(&unb64url(parts[0])).unwrap(),
            serde_json::from_slice(&unb64url(parts[1])).unwrap(),
        )
    }

    fn sign_in(kid: Option<&str>) -> (SignIn, Vec<u8>) {
        let (pem, public) = app_key();
        let key = AppKey::from_pem(&pem, kid.map(str::to_string)).unwrap();
        let s = SignIn::new(
            "https://acme.okta.com/oauth2/v1/token".into(),
            "0oa1".into(),
            Some("okta.logs.read".into()),
            key,
        )
        .unwrap_or_else(|(e, _)| panic!("{e}"))
        .offering_dpop();
        (s, public)
    }

    #[test]
    fn base64url_has_no_padding_and_the_url_alphabet() {
        assert_eq!(b64url(&[0xfb, 0xff]), "-_8");
        assert_eq!(unb64url("-_8"), vec![0xfb, 0xff]);
    }

    #[test]
    fn the_assertion_is_signed_by_the_app_key_and_names_the_app_and_the_endpoint() {
        let (s, public) = sign_in(Some("k1"));
        let (header, claims) = opened(&s.assertion().unwrap(), |input, sig| {
            UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, &public)
                .verify(input, sig)
                .expect("signed by the app key");
        });
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["kid"], "k1");
        assert_eq!(claims["iss"], "0oa1");
        assert_eq!(claims["sub"], "0oa1");
        assert_eq!(claims["aud"], "https://acme.okta.com/oauth2/v1/token");
        let iat = claims["iat"].as_u64().unwrap();
        assert_eq!(claims["exp"].as_u64().unwrap(), iat + ASSERTION_LIFE);
        assert_ne!(
            claims["jti"],
            opened(&s.assertion().unwrap(), |_, _| {}).1["jti"],
            "a jti is never reused"
        );
    }

    #[test]
    fn a_proof_is_signed_by_the_key_it_names_and_binds_the_request_and_the_token() {
        let (s, _) = sign_in(None);
        let jws = s
            .proof(
                "GET",
                "https://acme.okta.com/api/v1/logs?after=x#f",
                Some("hash"),
                Some("n1".into()),
            )
            .unwrap();
        let header: serde_json::Value = serde_json::from_slice(&unb64url(jws.split('.').next().unwrap())).unwrap();
        let mut point = vec![4];
        point.extend(unb64url(header["jwk"]["x"].as_str().unwrap()));
        point.extend(unb64url(header["jwk"]["y"].as_str().unwrap()));
        let (header, claims) = opened(&jws, |input, sig| {
            UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, &point)
                .verify(input, sig)
                .expect("signed by the key it names");
        });
        assert_eq!(header["typ"], "dpop+jwt");
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["jwk"]["crv"], "P-256");
        assert_eq!(claims["htm"], "GET");
        assert_eq!(
            claims["htu"], "https://acme.okta.com/api/v1/logs",
            "no query or fragment"
        );
        assert_eq!(claims["ath"], "hash");
        assert_eq!(claims["nonce"], "n1");
    }

    fn holding(s: &mut SignIn, fresh: bool) {
        s.token = Some(Token {
            granted: crate::io::client_credentials::Token {
                value: "t".into(),
                renew_at: Some(std::time::Instant::now() + Duration::from_secs(600)),
            },
            dpop: true,
            fresh,
        });
    }

    fn refusal(headers: &[(&str, &str)]) -> Answer {
        Answer {
            status: 401,
            headers: headers.iter().map(|(n, v)| (n.to_string(), v.to_string())).collect(),
            body: Vec::new(),
        }
    }

    #[test]
    fn a_nonce_the_api_names_is_taken_and_asked_with() {
        let (mut s, _) = sign_in(None);
        holding(&mut s, true);
        let asked = refusal(&[
            ("www-authenticate", r#"DPoP error="use_dpop_nonce""#),
            ("dpop-nonce", "n2"),
        ]);
        assert!(s.refused(&asked));
        assert_eq!(s.api_nonce.as_deref(), Some("n2"));
        assert!(s.token.is_some(), "the token is kept");
    }

    #[test]
    fn a_used_token_refused_is_replaced_and_a_new_one_refused_is_shown() {
        let (mut s, _) = sign_in(None);
        holding(&mut s, false);
        assert!(s.refused(&refusal(&[])), "asked again with a new token");
        assert!(s.token.is_none());
        holding(&mut s, true);
        assert!(!s.refused(&refusal(&[])), "a new token refused is the answer");
    }

    #[test]
    fn a_key_that_is_not_pkcs8_rsa_is_refused_with_how_to_convert_it() {
        let ec = EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING).unwrap();
        let der = ec.private_key().as_der().unwrap();
        let pem = format!(
            "-----BEGIN EC PRIVATE KEY-----\n{}\n-----END EC PRIVATE KEY-----\n",
            BytesForm::Base64.text(der.as_ref())
        );
        let why = AppKey::from_pem(&pem, None).err().unwrap();
        assert!(why.contains("openssl pkcs8"), "{why}");
        let why = AppKey::from_pem("not a key", None).err().unwrap();
        assert!(why.contains("not a PEM key"), "{why}");
    }
}
