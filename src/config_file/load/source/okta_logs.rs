use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OktaLogsSourceDto {
    /// The org's address, as `https://acme.okta.com`.
    pub org: String,
    /// The credential holding an API token with read access to the System Log. A source names a token or a
    /// service app, never both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SecretRefDto>,
    /// The service app's client id, when the source signs in as a service app.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// The credential holding the service app's private key, a PEM RSA key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key: Option<SecretRefDto>,
    /// Which of the service app's keys `privateKey` is, for an app with several.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
    pub poll_interval_secs: PollIntervalSecs,
    /// `"everything"` (default) reads what Okta lists with no start, its last seven days; `"new"` reads what
    /// happens from the first poll on.
    #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
    pub start: ListStartDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for OktaLogsSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let Self {
            org,
            token,
            client_id,
            private_key,
            key_id,
            tls,
            poll_interval_secs,
            ..
        } = self;
        let what = format!("source {:?}", s.id);
        let parsed = check_api_source(&what, ("org", org), tls, *poll_interval_secs, errors);
        if parsed.is_some_and(|u| u.path() != "/" || u.query().is_some() || u.fragment().is_some()) {
            errors.at("org").push(format!(
                "{what} names the org {org:?}; give its address alone, as https://acme.okta.com"
            ));
        }
        let client_id = client_id.as_deref().filter(|c| !blank(c));
        let service_app = client_id.is_some() || private_key.is_some();
        match (token, service_app) {
            (Some(_), true) => errors.at("token").push(format!(
                "{what} names both a token and a service app; give the token, or clientId and privateKey"
            )),
            (None, false) => errors.at("token").push(format!(
                "{what} names no sign-in; give a token, or a service app's clientId and privateKey"
            )),
            (None, true) => {
                if client_id.is_none() {
                    errors.at("clientId").push(format!(
                        "{what} names a service app's private key and no client id; give its clientId"
                    ));
                }
                if private_key.is_none() {
                    errors.at("privateKey").push(format!(
                        "{what} names a service app's client id and no private key; give the credential \
                             holding it as privateKey"
                    ));
                }
            }
            (Some(_), false) => {}
        }
        if key_id.is_some() && !service_app {
            errors.at("keyId").push(format!(
                "{what} names a keyId with no service app; give it beside clientId and privateKey, or drop it"
            ));
        }
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            org,
            token,
            client_id,
            private_key,
            key_id,
            tls,
            poll_interval_secs,
            start,
        } = self;
        SourceKind::Api(crate::plan::topology::ApiSourceConfig {
            api: crate::plan::topology::Api::OktaLogs(crate::plan::topology::OktaLogsConfig {
                org: org.trim().trim_end_matches('/').to_string(),
                sign_in: match (token, client_id, private_key) {
                    (Some(token), _, _) => OktaSignIn::Token(to_secret_ref(token)),
                    (None, Some(client_id), Some(private_key)) => OktaSignIn::ServiceApp {
                        client_id: client_id.trim().to_string(),
                        private_key: to_secret_ref(private_key),
                        key_id: key_id
                            .as_deref()
                            .map(str::trim)
                            .filter(|k| !k.is_empty())
                            .map(String::from),
                    },
                    _ => unreachable!("required_fields refuses an Okta source with neither a token nor a service app"),
                },
                start: match start {
                    ListStartDto::Everything => ListStart::Everything,
                    ListStartDto::New => ListStart::New,
                },
            }),
            tls: to_tls(tls, of, Side::Client),
            poll_interval: Duration::from_secs(poll_interval_secs.0),
        })
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let Self { token, private_key, .. } = self;
        (token.iter().map(|r| ("token", r)))
            .chain(private_key.iter().map(|r| ("privateKey", r)))
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.token.iter_mut().chain(&mut self.private_key).collect()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        vec![("org", &self.org)]
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::OKTA_LOGS
    }

    fn single_worker(&self) -> bool {
        true
    }
}
