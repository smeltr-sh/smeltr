//! An API source signing in as an app: OAuth 2.0 client credentials, proven by a client secret or by an assertion
//! signed with the app's private key, for a bearer token sent on every request until shortly before it expires.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant S as source
//!   participant P as provider
//!   participant A as API
//!   opt the source names an issuer, on its first sign-in
//!     S->>P: GET /.well-known/openid-configuration
//!     P-->>S: token endpoint, how it takes a secret
//!   end
//!   S->>P: token endpoint: client_credentials, the secret or a signed assertion
//!   P-->>S: token, when it expires
//!   S->>A: Authorization: Bearer token
//!   A-->>S: 401 to a token used before: it is replaced, and the request asked again
//! ```
//!
//! The token endpoint is found when the first token is needed, not when the source opens, so a provider out of reach
//! is a failed poll that is asked again, like any other.

use std::time::Duration;

use super::IoError;
use super::api::{Answer, Authorize};
use super::client_credentials::{ClientCredentials, LastSent, TokenAuth};
use super::oauth::{AppKey, SignIn};
use crate::plan::topology::TokenAt;

/// An app's sign-in, before and after its token endpoint is known.
pub struct AppSignIn {
    client_id: String,
    token_at: TokenAt,
    scope: Option<String>,
    /// How the app proves itself, until the sign-in it makes is held.
    proof: Option<Proof>,
    held: Option<Held>,
}

/// What an app proves itself with, its secret values read.
pub enum Proof {
    Secret(String),
    Key(AppKey),
}

enum Held {
    Secret { sign_in: ClientCredentials, last: LastSent },
    Key(SignIn),
}

impl AppSignIn {
    pub fn new(client_id: String, token_at: TokenAt, scope: Option<String>, proof: Proof) -> AppSignIn {
        AppSignIn {
            client_id,
            token_at,
            scope,
            proof: Some(proof),
            held: None,
        }
    }

    /// The sign-in, made the first time it is needed.
    fn held(&mut self, agent: &ureq::Agent, timeout: Duration) -> Result<&mut Held, IoError> {
        if self.held.is_none() {
            let (token_url, discovered) = match &self.token_at {
                TokenAt::Url(url) => (url.clone(), None),
                TokenAt::Issuer(issuer) => {
                    let (url, methods) = super::discovery::token_endpoint(agent, timeout, issuer)
                        .map_err(|e| IoError::transient(format!("finding where {issuer} signs apps in: {e}")))?;
                    (url, Some((issuer, methods)))
                }
            };
            // A token endpoint given whole takes the secret in the form, as most do.
            let auth = match discovered {
                Some((issuer, methods)) if matches!(self.proof, Some(Proof::Secret(_))) => {
                    super::discovery::secret_auth(issuer, methods.as_deref()).map_err(IoError::transient)?
                }
                _ => TokenAuth::Post,
            };
            let Some(proof) = self.proof.take() else {
                return Err(IoError::transient(
                    "the sign-in could not be made, and its proof is gone",
                ));
            };
            let held = match proof {
                Proof::Secret(secret) => {
                    let sign_in = ClientCredentials::new(
                        token_url,
                        self.client_id.clone(),
                        secret,
                        self.scope.clone(),
                        agent.clone(),
                        timeout,
                    )
                    .sending_secret(auth);
                    Held::Secret {
                        sign_in,
                        last: LastSent::default(),
                    }
                }
                Proof::Key(key) => Held::Key(
                    SignIn::new(token_url, self.client_id.clone(), self.scope.clone(), key).map_err(|(e, key)| {
                        self.proof = Some(Proof::Key(key));
                        IoError::transient(e)
                    })?,
                ),
            };
            self.held = Some(held);
        }
        Ok(self.held.as_mut().expect("just made"))
    }
}

impl Authorize for AppSignIn {
    fn authorize(
        &mut self,
        agent: &ureq::Agent,
        timeout: Duration,
        url: &str,
    ) -> Result<Vec<(String, String)>, IoError> {
        match self.held(agent, timeout)? {
            Held::Secret { sign_in, last } => {
                let bearer = sign_in.bearer()?;
                last.sent(&bearer);
                Ok(vec![("Authorization".to_string(), bearer)])
            }
            Held::Key(sign_in) => sign_in.authorize(agent, timeout, url),
        }
    }

    fn refused(&mut self, answer: &Answer) -> bool {
        match &mut self.held {
            // Either way it is dropped, so the next poll signs in again rather than sending a token already refused.
            Some(Held::Secret { sign_in, last }) => {
                sign_in.forget();
                last.worth_replacing()
            }
            Some(Held::Key(sign_in)) => sign_in.refused(answer),
            None => false,
        }
    }

    fn used(&mut self, answer: &Answer) {
        match &mut self.held {
            Some(Held::Secret { last, .. }) => last.used(),
            Some(Held::Key(sign_in)) => sign_in.used(answer),
            None => {}
        }
    }
}
