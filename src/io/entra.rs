//! Signing in to an Azure service with Entra ID, for a bearer token sent on every request until shortly before it
//! expires: as a service principal with its secret, as one of the host's managed identities through Azure's instance
//! metadata service, or, in an Azure Kubernetes Service pod, as the pod's workload identity.
//!
//! ```mermaid
//! flowchart LR
//!   sp[service principal] -- client secret --> token[token endpoint]
//!   pod[pod with a workload identity] -- the pod's token, as an assertion --> token
//!   host[managed identity] -- GET, Metadata: true --> imds[instance metadata service]
//!   token -- token, expires_in --> client
//!   imds -- token, expires_in --> client
//! ```
//!
//! The host's own managed identity is the pod's workload identity wherever Azure's webhook marks the pod as having
//! one, as Azure's own SDKs choose. A named managed identity always signs in at the metadata service, and so does
//! one the host names in `AZURE_CLIENT_ID`, as Event Hubs and blob storage take it.

use std::path::PathBuf;
use std::time::Duration;

use super::IoError;
use super::api::{Answer, Authorize};
use super::client_credentials::{ClientCredentials, LastSent, Token, granted, read_answer, why_refused};
use super::http::percent_encode;
use crate::plan::topology::{EntraSignIn, HeldSecrets};

/// The Azure Instance Metadata Service version a managed identity's token is asked of.
pub const IMDS_API_VERSION: &str = "2018-02-01";

/// Where the host's instance metadata service gives a managed identity its tokens.
const IMDS: &str = "http://169.254.169.254/metadata/identity/oauth2/token";

/// A sign-in to the Azure service `resource` names, made the first time a token is needed.
pub struct Entra {
    how: How,
    resource: String,
    held: Option<Held>,
    last: LastSent,
}

enum How {
    Secret {
        token_url: String,
        client_id: String,
        secret: String,
    },
    Managed {
        client_id: Option<String>,
        imds: String,
    },
    Workload(Workload),
}

enum Held {
    Credentials(ClientCredentials),
    Imds(Option<Token>),
}

impl Entra {
    /// A sign-in as `sign_in` says, for a token to `resource`, such as `https://monitor.azure.com`. `what` names the
    /// connector when the secret it takes is not held.
    pub fn new(sign_in: &EntraSignIn, resource: &str, secrets: &HeldSecrets, what: &str) -> Result<Entra, IoError> {
        let how = match sign_in {
            EntraSignIn::ServicePrincipal {
                authority,
                tenant_id,
                client_id,
                secret,
            } => How::Secret {
                token_url: format!(
                    "{}/{tenant_id}/oauth2/v2.0/token",
                    authority.as_deref().unwrap_or(super::entra_authority(resource))
                ),
                client_id: client_id.clone(),
                secret: super::held(secrets, secret, what, "clientSecret")?,
            },
            EntraSignIn::ManagedIdentity { client_id: None } if in_workload() => {
                How::Workload(Workload::from_env(&|name| std::env::var(name).ok(), resource)?)
            }
            EntraSignIn::ManagedIdentity { client_id } => How::Managed {
                client_id: client_id.clone().or_else(managed_identity_client_id),
                imds: IMDS.to_string(),
            },
        };
        Ok(Entra {
            how,
            resource: resource.trim_end_matches('/').to_string(),
            held: None,
            last: LastSent::default(),
        })
    }

    /// The token held, signing in first when none is or the one held is about to expire.
    fn bearer(&mut self, agent: &ureq::Agent, timeout: Duration) -> Result<String, IoError> {
        let scope = format!("{}/.default", self.resource);
        let held = self.held.get_or_insert_with(|| match &self.how {
            How::Secret {
                token_url,
                client_id,
                secret,
            } => Held::Credentials(ClientCredentials::new(
                token_url.clone(),
                client_id.clone(),
                secret.clone(),
                Some(scope),
                agent.clone(),
                timeout,
            )),
            How::Workload(workload) => Held::Credentials(workload.credentials(scope, agent.clone(), timeout)),
            How::Managed { .. } => Held::Imds(None),
        });
        match held {
            Held::Credentials(credentials) => credentials.bearer(),
            Held::Imds(token) => {
                let How::Managed { client_id, imds } = &self.how else {
                    unreachable!("only a managed identity holds a metadata service token")
                };
                if token.as_ref().is_none_or(Token::due) {
                    *token = Some(imds_token(agent, timeout, imds, &self.resource, client_id.as_deref())?);
                }
                Ok(format!("Bearer {}", token.as_ref().expect("signed in above").value))
            }
        }
    }

    #[cfg(test)]
    fn at_imds(mut self, url: &str) -> Entra {
        if let How::Managed { imds, .. } = &mut self.how {
            *imds = url.to_string();
        }
        self
    }
}

impl Authorize for Entra {
    fn authorize(
        &mut self,
        agent: &ureq::Agent,
        timeout: Duration,
        _url: &str,
    ) -> Result<Vec<(String, String)>, IoError> {
        let bearer = self.bearer(agent, timeout)?;
        self.last.sent(&bearer);
        Ok(vec![("Authorization".to_string(), bearer)])
    }

    /// Either way the token is dropped, so the next request signs in again rather than sending a token already
    /// refused.
    fn refused(&mut self, _answer: &Answer) -> bool {
        match &mut self.held {
            Some(Held::Credentials(credentials)) => credentials.forget(),
            Some(Held::Imds(token)) => *token = None,
            None => return false,
        }
        self.last.worth_replacing()
    }

    fn used(&mut self, _answer: &Answer) {
        self.last.used();
    }
}

/// A managed identity's token for `resource` from the metadata service at `imds`: the host's own, or the one
/// `client_id` names.
fn imds_token(
    agent: &ureq::Agent,
    timeout: Duration,
    imds: &str,
    resource: &str,
    client_id: Option<&str>,
) -> Result<Token, IoError> {
    let mut url = format!(
        "{imds}?api-version={IMDS_API_VERSION}&resource={}",
        percent_encode(resource)
    );
    if let Some(client_id) = client_id {
        url.push_str(&format!("&client_id={}", percent_encode(client_id)));
    }
    let mut response = agent
        .get(&url)
        .config()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .max_redirects(0)
        // The metadata service is on the host's own link; a proxy would carry the request away from it.
        .proxy(None)
        .build()
        .header("Metadata", "true")
        .call()
        .map_err(|e| {
            IoError::transient(format!(
                "asking the host's metadata service for a managed identity's token failed: {e}. A managed \
                 identity signs in only on an Azure host that has one"
            ))
        })?;
    let (status, body, answer) = read_answer(imds, &mut response)?;
    if !(200..300).contains(&status) {
        return Err(IoError::transient(format!(
            "the host's metadata service refused a managed identity's token ({status}): {}",
            why_refused(&body, &answer)
        )));
    }
    granted(imds, &answer)
}

/// The user-assigned managed identity this host names in `AZURE_CLIENT_ID`. The same variable names a workload
/// identity's or an environment's service principal, which are not managed identities: their own variables beside
/// it say so, and then it names none.
pub fn managed_identity_client_id() -> Option<String> {
    let set = |name: &str| std::env::var_os(name).is_some_and(|v| !v.is_empty());
    if set("AZURE_FEDERATED_TOKEN_FILE") || set("AZURE_CLIENT_SECRET") {
        return None;
    }
    std::env::var("AZURE_CLIENT_ID").ok()
}

/// Whether this host is a pod with a workload identity, as Azure's webhook marks one.
pub fn in_workload() -> bool {
    std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some_and(|v| !v.is_empty())
}

/// A pod's workload identity: the pod's Kubernetes token, from the file Azure's webhook names, traded at Entra as an
/// assertion in place of a secret.
pub struct Workload {
    token_url: String,
    client_id: String,
    file: PathBuf,
}

impl Workload {
    /// The workload identity the variables Azure's webhook sets name, read through `env`, signing in where the cloud
    /// `resource` is in does unless the webhook names an authority.
    pub fn from_env(env: &dyn Fn(&str) -> Option<String>, resource: &str) -> Result<Workload, IoError> {
        let var = |name: &str| {
            env(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    IoError::transient(format!(
                        "signing in as the pod's workload identity needs {name}, which Azure's webhook sets on a pod \
                         labeled azure.workload.identity/use=true"
                    ))
                })
        };
        let (file, client_id, tenant) = (
            var("AZURE_FEDERATED_TOKEN_FILE")?,
            var("AZURE_CLIENT_ID")?,
            var("AZURE_TENANT_ID")?,
        );
        let authority = env("AZURE_AUTHORITY_HOST")
            .map(|a| a.trim().trim_end_matches('/').to_string())
            .filter(|a| !a.is_empty())
            .unwrap_or_else(|| super::entra_authority(resource).to_string());
        Ok(Workload {
            token_url: format!("{authority}/{tenant}/oauth2/v2.0/token"),
            client_id,
            file: PathBuf::from(file),
        })
    }

    /// The client id it signs in as.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Where it signs in.
    pub fn token_url(&self) -> &str {
        &self.token_url
    }

    /// Its sign-in for `scope`. Nothing is asked until the first token is needed.
    pub fn credentials(&self, scope: String, agent: ureq::Agent, timeout: Duration) -> ClientCredentials {
        ClientCredentials::new(
            self.token_url.clone(),
            self.client_id.clone(),
            String::new(),
            Some(scope),
            agent,
            timeout,
        )
        .proven_by_assertion_file(self.file.clone())
    }
}

/// The resource an Azure Monitor endpoint at `url` takes tokens for: Azure Monitor in the cloud its host is in.
pub fn monitor_resource(url: &str) -> &'static str {
    match super::entra_authority(url) {
        "https://login.microsoftonline.us" => "https://monitor.azure.us",
        "https://login.chinacloudapi.cn" => "https://monitor.azure.cn",
        _ => "https://monitor.azure.com",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;
    use crate::io::client_credentials::tests::{serve, serve_keeping};
    use crate::plan::topology::SecretRef;

    fn agent() -> ureq::Agent {
        // The environment's proxy would carry a loopback call away.
        ureq::Agent::config_builder().proxy(None).build().new_agent()
    }

    fn answer(status: u16) -> Answer {
        Answer {
            status,
            ..Answer::default()
        }
    }

    /// Each request's target, and whether it carried `Metadata: true`.
    type Seen = Arc<Mutex<Vec<(String, bool)>>>;

    /// A metadata service on 127.0.0.1 that answers each GET with the next of `answers`, and keeps what it saw.
    fn imds(answers: Vec<(u16, String)>) -> (String, Seen) {
        serve_keeping("/metadata/identity/oauth2/token", answers, |asked| {
            let metadata = asked.header("metadata") == Some("true");
            (asked.target, metadata)
        })
    }

    fn secrets() -> HeldSecrets {
        let mut secrets = HeldSecrets::default();
        secrets.set_stored([("sp".to_string(), 1, "s3cr&t".to_string())]);
        secrets
    }

    fn principal(authority: &str) -> EntraSignIn {
        EntraSignIn::ServicePrincipal {
            authority: Some(authority.to_string()),
            tenant_id: "tenant".into(),
            client_id: "app".into(),
            secret: SecretRef::LeaderStored {
                name: "sp".into(),
                version: 1,
            },
        }
    }

    #[test]
    fn a_service_principal_signs_in_for_the_resource_and_keeps_its_token() {
        let (url, seen) = serve(vec![(
            200,
            json!({"access_token": "one", "expires_in": 3600}).to_string(),
        )]);
        let authority = url.trim_end_matches("/token");
        let mut entra = Entra::new(&principal(authority), "https://monitor.azure.com", &secrets(), "test").unwrap();
        let timeout = Duration::from_secs(5);
        let headers = entra.authorize(&agent(), timeout, "https://x").unwrap();
        assert_eq!(headers, vec![("Authorization".to_string(), "Bearer one".to_string())]);
        assert_eq!(entra.authorize(&agent(), timeout, "https://x").unwrap(), headers);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].contains("client_secret=s3cr%26t"), "{}", seen[0]);
        assert!(
            seen[0].contains("scope=https%3A%2F%2Fmonitor.azure.com%2F.default"),
            "{}",
            seen[0]
        );
    }

    #[test]
    fn a_service_principal_whose_secret_is_not_held_is_refused() {
        let err = Entra::new(
            &principal("https://login"),
            "https://monitor.azure.com",
            &HeldSecrets::default(),
            "Log Analytics destination",
        )
        .err()
        .unwrap();
        assert!(
            err.to_string()
                .contains("Log Analytics destination takes its clientSecret"),
            "{err}"
        );
    }

    #[test]
    fn a_managed_identity_asks_the_metadata_service_and_names_the_identity() {
        let (url, seen) = imds(vec![(
            200,
            // The metadata service writes its numbers as strings.
            json!({"access_token": "m1", "expires_in": "86399", "token_type": "Bearer"}).to_string(),
        )]);
        let sign_in = EntraSignIn::ManagedIdentity {
            client_id: Some("1111-2222".into()),
        };
        let mut entra = Entra::new(&sign_in, "https://monitor.azure.com", &secrets(), "test")
            .unwrap()
            .at_imds(&url);
        let timeout = Duration::from_secs(5);
        let headers = entra.authorize(&agent(), timeout, "https://x").unwrap();
        assert_eq!(headers[0].1, "Bearer m1");
        entra.authorize(&agent(), timeout, "https://x").unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "a token with a lifetime is kept");
        assert_eq!(
            seen[0],
            (
                "/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fmonitor.azure.com\
                 &client_id=1111-2222"
                    .to_string(),
                true
            )
        );
    }

    #[test]
    fn a_metadata_service_refusal_says_why() {
        let (url, _) = imds(vec![(
            400,
            json!({"error": "invalid_request", "error_description": "Identity not found"}).to_string(),
        )]);
        let sign_in = EntraSignIn::ManagedIdentity {
            client_id: Some("1111".into()),
        };
        let mut entra = Entra::new(&sign_in, "https://monitor.azure.com", &secrets(), "test")
            .unwrap()
            .at_imds(&url);
        let err = entra
            .authorize(&agent(), Duration::from_secs(5), "https://x")
            .unwrap_err();
        assert!(
            err.to_string().contains("(400): invalid_request: Identity not found"),
            "{err}"
        );
    }

    #[test]
    fn a_token_used_before_and_refused_is_replaced_once() {
        let (url, seen) = serve(vec![
            (200, json!({"access_token": "one", "expires_in": 3600}).to_string()),
            (200, json!({"access_token": "two", "expires_in": 3600}).to_string()),
        ]);
        let authority = url.trim_end_matches("/token");
        let mut entra = Entra::new(&principal(authority), "https://monitor.azure.com", &secrets(), "test").unwrap();
        let timeout = Duration::from_secs(5);
        entra.authorize(&agent(), timeout, "https://x").unwrap();
        entra.used(&answer(204));
        assert!(entra.refused(&answer(401)), "a token used before is worth replacing");
        let headers = entra.authorize(&agent(), timeout, "https://x").unwrap();
        assert_eq!(headers[0].1, "Bearer two");
        assert!(
            !entra.refused(&answer(401)),
            "a new token refused is the refusal to show"
        );
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[test]
    fn a_workload_identity_reads_the_webhooks_variables() {
        let env = |name: &str| {
            Some(
                match name {
                    "AZURE_FEDERATED_TOKEN_FILE" => "/var/run/token",
                    "AZURE_CLIENT_ID" => "pod-app",
                    "AZURE_TENANT_ID" => "t1",
                    _ => return None,
                }
                .to_string(),
            )
        };
        let workload = Workload::from_env(&env, "https://monitor.azure.us").unwrap();
        assert_eq!(workload.client_id(), "pod-app");
        assert_eq!(
            workload.token_url(),
            "https://login.microsoftonline.us/t1/oauth2/v2.0/token"
        );
        let err = Workload::from_env(&|_| None, "https://monitor.azure.com")
            .err()
            .unwrap();
        assert!(err.to_string().contains("needs AZURE_FEDERATED_TOKEN_FILE"), "{err}");
    }

    #[test]
    fn an_ingestion_endpoint_signs_in_to_monitor_in_its_own_cloud() {
        assert_eq!(
            monitor_resource("https://dce-1.eastus-1.ingest.monitor.azure.com"),
            "https://monitor.azure.com"
        );
        assert_eq!(
            monitor_resource("https://dce-1.usgovvirginia-1.ingest.monitor.azure.us/"),
            "https://monitor.azure.us"
        );
        assert_eq!(
            monitor_resource("https://dce-1.chinaeast2-1.ingest.monitor.azure.cn"),
            "https://monitor.azure.cn"
        );
        assert_eq!(monitor_resource("http://127.0.0.1:8080"), "https://monitor.azure.com");
    }
}
