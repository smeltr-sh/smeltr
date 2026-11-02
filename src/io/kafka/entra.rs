//! An Event Hubs client signing in as the Azure Kubernetes Service pod's workload identity: the pod's Kubernetes
//! token, from the file Azure's webhook names, traded at Entra for a token for the namespace.
//!
//! librdkafka cannot make this sign-in itself: its own assertion grant is not the one Entra takes. So a client the
//! loader sent to the host's managed identity is turned, in a pod with a workload identity, to the token callback
//! here, which every Kafka client's context answers.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant K as librdkafka
//!   participant C as client context
//!   participant E as Entra
//!   K->>C: token due, with sasl.oauthbearer.config
//!   C->>C: read AZURE_FEDERATED_TOKEN_FILE
//!   C->>E: client_credentials, AZURE_CLIENT_ID, the file as client_assertion
//!   E-->>C: token, expires_in
//!   C-->>K: token, when it expires
//! ```

use std::error::Error;
use std::ffi::CString;
use std::time::{Instant, SystemTime};

use rdkafka::ClientContext;
use rdkafka::bindings;
use rdkafka::client::{Client, OAuthToken};

use super::KafkaProps;
use crate::io::IoError;
use crate::io::client_credentials::SIGN_IN_TIMEOUT;
use crate::io::entra::Workload;

/// Where Entra signs in when the pod names no other authority.
/// What `sasl.oauthbearer.config` starts with on a client this module signs in, and on no other: an operator's own
/// OAUTHBEARER client never gets the pod's token.
const SCOPE: &str = "smeltr.workload.scope=";

/// The `sasl.oauthbearer.config` that sends a client signing in as the host's managed identity to this module
/// instead, for the same namespace. `None` for every other client, and for one naming which managed identity: that
/// one signs in at the host's metadata service.
pub(super) fn workload_config(props: &KafkaProps) -> Option<String> {
    let literal = |key: &str| props.literal.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
    if literal("sasl.oauthbearer.metadata.authentication.type") != Some("azure_imds") {
        return None;
    }
    let query = literal("sasl.oauthbearer.config")?.strip_prefix("query=")?;
    if query.split('&').any(|pair| pair.starts_with("client_id=")) {
        return None;
    }
    let resource = query.split('&').find_map(|pair| pair.strip_prefix("resource="))?;
    Some(format!("{SCOPE}{resource}/.default"))
}

pub(super) use crate::io::entra::in_workload;

/// A token for the client whose `sasl.oauthbearer.config` is `config`: what every Kafka client's context answers
/// librdkafka with. A client this module does not sign in is refused, as librdkafka would have refused it.
pub(super) fn token(config: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
    let agent = ureq::Agent::new_with_config(crate::io::http::agent_config(None, &Default::default())?);
    Ok(token_from(config, &|name| std::env::var(name).ok(), agent)?)
}

/// Hands `client` its first token now, when this module signs it in. For a client nothing polls for its token: an
/// admin client's own thread serves its admin queue alone, and a throwaway consumer asks one question and is gone.
/// librdkafka holds every connection until a token is set, so without this such a client only times out.
pub(super) fn sign_in_unpolled<C: ClientContext>(client: &Client<C>, config: Option<&str>) {
    if config.is_some_and(|c| c.starts_with(SCOPE))
        && let Err(why) = hand(client, token(config).map_err(|e| e.to_string()))
    {
        tracing::warn!(target: "kafka.sign_in", error = %why, "could not sign in to Kafka as the pod's workload identity");
    }
}

/// Sets `client`'s token, or the reason it has none. `Err` with librdkafka's reason when it refuses the token.
fn hand<C: ClientContext>(client: &Client<C>, token: Result<OAuthToken, String>) -> Result<(), String> {
    let mut why = [0 as std::ffi::c_char; 512];
    let failed = match token {
        Ok(token) => {
            let value = CString::new(token.token).map_err(|e| e.to_string())?;
            let principal = CString::new(token.principal_name).map_err(|e| e.to_string())?;
            // SAFETY: the client is live for the borrow; librdkafka copies every string before it returns, and writes
            // at most `why.len()` bytes into `why`.
            let set = unsafe {
                bindings::rd_kafka_oauthbearer_set_token(
                    client.native_ptr(),
                    value.as_ptr(),
                    token.lifetime_ms,
                    principal.as_ptr(),
                    std::ptr::null_mut(),
                    0,
                    why.as_mut_ptr(),
                    why.len(),
                )
            };
            if set == bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Ok(());
            }
            // SAFETY: librdkafka wrote a terminated string into `why`.
            unsafe { std::ffi::CStr::from_ptr(why.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        }
        Err(why) => why,
    };
    let reason = CString::new(failed.replace('\0', " ")).unwrap_or_default();
    // SAFETY: as above; the reason is copied.
    unsafe { bindings::rd_kafka_oauthbearer_set_token_failure(client.native_ptr(), reason.as_ptr()) };
    Err(failed)
}

/// [`token`], the pod's variables read through `env`.
fn token_from(
    config: Option<&str>,
    env: &dyn Fn(&str) -> Option<String>,
    agent: ureq::Agent,
) -> Result<OAuthToken, IoError> {
    let Some(scope) = config.and_then(|c| c.strip_prefix(SCOPE)) else {
        return Err(IoError::transient(
            "smeltr makes no token for this OAUTHBEARER client: librdkafka must make its own".to_string(),
        ));
    };
    let workload = Workload::from_env(env, scope.trim_end_matches("/.default"))?;
    let mut credentials = workload.credentials(scope.to_string(), agent, SIGN_IN_TIMEOUT);
    let token = credentials.token()?;
    // librdkafka renews at 80% of what it is told; told the time the token is renewed before, it never holds one
    // that has expired.
    let Some(renew_at) = token.renew_at else {
        return Err(IoError::transient(format!(
            "{} answered the sign-in with no expires_in, so the token's lifetime is unknown",
            workload.token_url()
        )));
    };
    let expires = SystemTime::now() + renew_at.saturating_duration_since(Instant::now());
    Ok(OAuthToken {
        token: token.value.clone(),
        principal_name: workload.client_id().to_string(),
        lifetime_ms: expires
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::io::client_credentials::tests::serve;

    fn imds_props(resource: &str) -> KafkaProps {
        KafkaProps {
            literal: vec![
                ("sasl.mechanism".into(), "OAUTHBEARER".into()),
                ("sasl.oauthbearer.method".into(), "oidc".into()),
                (
                    "sasl.oauthbearer.metadata.authentication.type".into(),
                    "azure_imds".into(),
                ),
                (
                    "sasl.oauthbearer.config".into(),
                    format!("query=api-version=2018-02-01&resource={resource}"),
                ),
            ],
            ..KafkaProps::default()
        }
    }

    #[test]
    fn a_managed_identity_sign_in_is_turned_to_the_workload_for_the_same_namespace() {
        assert_eq!(
            workload_config(&imds_props("https://ns.servicebus.windows.net")).as_deref(),
            Some("smeltr.workload.scope=https://ns.servicebus.windows.net/.default")
        );
        let mut principal = imds_props("https://ns.servicebus.windows.net");
        principal
            .literal
            .retain(|(k, _)| k != "sasl.oauthbearer.metadata.authentication.type");
        assert_eq!(workload_config(&principal), None);
    }

    #[test]
    fn a_named_managed_identity_is_not_turned_to_the_workload() {
        assert_eq!(
            workload_config(&imds_props("https://ns.servicebus.windows.net&client_id=c-1")),
            None
        );
    }

    #[test]
    fn a_pod_trades_its_token_file_at_its_authority_for_the_namespaces_token() {
        let (url, seen) = serve(vec![(
            200,
            json!({"access_token": "ns-token", "token_type": "Bearer", "expires_in": 3600}).to_string(),
        )]);
        let authority = url.strip_suffix("/token").unwrap().to_string();
        let file = std::env::temp_dir().join(format!("smeltr-workload-{}", std::process::id()));
        std::fs::write(&file, "pod.jwt\n").unwrap();
        let file_name = file.display().to_string();
        let env = move |name: &str| match name {
            "AZURE_FEDERATED_TOKEN_FILE" => Some(file_name.clone()),
            "AZURE_CLIENT_ID" => Some("app-id".into()),
            "AZURE_TENANT_ID" => Some("tenant-id".into()),
            "AZURE_AUTHORITY_HOST" => Some(format!("{authority}/")),
            _ => None,
        };
        let agent = ureq::Agent::config_builder().proxy(None).build().new_agent();
        let before = SystemTime::now();
        let token = token_from(Some("smeltr.workload.scope=https://ns/.default"), &env, agent).unwrap();
        std::fs::remove_file(&file).unwrap();
        assert_eq!(
            (token.token.as_str(), token.principal_name.as_str()),
            ("ns-token", "app-id")
        );
        let lifetime = Duration::from_millis(token.lifetime_ms as u64);
        let from_now = lifetime - before.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        assert!(
            from_now > Duration::from_secs(3400) && from_now <= Duration::from_secs(3600),
            "{from_now:?}"
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!(
            seen[0].contains("client_id=app-id&client_assertion_type="),
            "{}",
            seen[0]
        );
        assert!(
            seen[0].contains("&client_assertion=pod.jwt&scope=https%3A%2F%2Fns%2F.default"),
            "{}",
            seen[0]
        );
    }

    #[test]
    fn a_sign_in_missing_what_the_webhook_sets_names_it() {
        let agent = || ureq::Agent::config_builder().proxy(None).build().new_agent();
        let env = |name: &str| (name == "AZURE_FEDERATED_TOKEN_FILE").then(|| "/token".to_string());
        let err = token_from(Some("smeltr.workload.scope=https://ns/.default"), &env, agent())
            .err()
            .expect("refused");
        assert!(err.to_string().contains("AZURE_CLIENT_ID"), "{err}");
        let err = token_from(Some("principal=admin"), &env, agent())
            .err()
            .expect("refused");
        assert!(err.to_string().contains("librdkafka must make its own"), "{err}");
    }

    #[test]
    fn a_client_nothing_polls_is_handed_its_token() {
        let mut config = rdkafka::ClientConfig::new();
        config
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "OAUTHBEARER")
            .set("sasl.oauthbearer.config", "smeltr.workload.scope=https://ns/.default");
        let admin: rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext> =
            config.create().expect("builds");
        let in_an_hour = SystemTime::now() + Duration::from_secs(3600);
        let token = OAuthToken {
            token: "eyJ.e30.sig".into(),
            principal_name: "app-id".into(),
            lifetime_ms: in_an_hour.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64,
        };
        assert_eq!(hand(admin.inner(), Ok(token)), Ok(()));
        assert_eq!(hand(admin.inner(), Err("no file".into())), Err("no file".to_string()));
    }
}
