//! A schema registry as Confluent's REST API answers it: a schema by its id, a schema by its subject and version, and
//! a schema registered under a subject. Every call is blocking, on the caller's agent, and bounded by one timeout.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant S as source
//!   participant D as destination
//!   participant R as registry
//!   S->>R: GET /schemas/ids/{id}
//!   R-->>S: the schema, and the subjects and versions it references
//!   S->>R: GET /subjects/{subject}/versions/{version}, once per reference
//!   D->>R: POST /subjects/{subject}/versions
//!   R-->>D: the id, or 409 when the subject's compatibility refuses it
//! ```

use std::time::Duration;

use serde::Deserialize;

use crate::io::IoError;
use crate::io::http::percent_encode;
use crate::plan::topology::{HeldSecrets, Identity, SchemaRegistry};

/// The most one answer may hold. A schema is a few kilobytes; this bounds what a broken server can make a worker hold.
const MAX_ANSWER: u64 = 16 * 1024 * 1024;

/// How long one call may take.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The most schemas one schema may reference, counting what its references reference.
pub const MAX_REFERENCES: usize = 256;

/// What a call that got no schema answers.
#[derive(Debug)]
pub enum Unread {
    /// Asking again may answer: the registry was unreachable, slow, busy, or refused who signed in.
    Transient(IoError),
    /// The registry answered, and will answer the same again.
    Refused(String),
}

impl Unread {
    /// As an error to retry, whichever it is.
    pub fn into_error(self) -> IoError {
        match self {
            Unread::Transient(e) => e,
            Unread::Refused(why) => IoError::transient(why),
        }
    }
}

/// A schema as the registry holds it: its Avro text, and each schema it references by name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Text {
    pub schema: String,
    pub references: Vec<Reference>,
}

/// A schema another references, by the subject and version it is registered under.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Reference {
    pub name: String,
    pub subject: String,
    pub version: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Answered {
    schema: String,
    /// Absent is Avro.
    #[serde(default)]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<Reference>,
}

#[derive(Deserialize)]
struct Registered {
    id: u32,
}

pub struct Registry {
    /// Without a trailing `/`.
    url: String,
    agent: ureq::Agent,
    authorization: Option<String>,
}

impl Registry {
    /// A client of `config`, connecting under `identity`. A password this worker does not hold is an error here,
    /// never a call made without it.
    pub fn new(
        config: &SchemaRegistry,
        identity: Option<&Identity>,
        secrets: &HeldSecrets,
    ) -> Result<Registry, IoError> {
        let authorization = match &config.basic {
            Some((user, password)) => Some(crate::io::http::basic(
                "the schema registry",
                "password",
                user,
                password,
                secrets,
            )?),
            None => None,
        };
        Ok(Registry {
            url: config.url.trim_end_matches('/').to_string(),
            agent: crate::io::http::agent_config(identity, secrets)?.new_agent(),
            authorization,
        })
    }

    /// The schema registered under `id`.
    pub fn schema(&self, id: u32) -> Result<Text, Unread> {
        let what = format!("schema {id}");
        self.text(&what, &format!("{}/schemas/ids/{id}", self.url))
    }

    /// The schema registered under `subject` at `version`.
    pub fn version(&self, subject: &str, version: i64) -> Result<Text, Unread> {
        let what = format!("version {version} of subject {subject:?}");
        let url = format!("{}/subjects/{}/versions/{version}", self.url, percent_encode(subject));
        self.text(&what, &url)
    }

    /// Registers `schema` under `subject`, or finds it already there, and answers its id. `Refused` is a schema the
    /// subject's compatibility setting or the registry's parser refuses.
    pub fn register(&self, subject: &str, schema: &str) -> Result<u32, Unread> {
        let url = format!("{}/subjects/{}/versions", self.url, percent_encode(subject));
        let body = serde_json::json!({ "schema": schema }).to_string();
        let what = format!("registering a schema under subject {subject:?}");
        let (status, answer) = self.call(&url, Some(&body))?;
        match status {
            200 => serde_json::from_slice::<Registered>(&answer)
                .map(|r| r.id)
                .map_err(|e| {
                    Unread::Transient(IoError::transient(format!("{what}: an answer that does not read: {e}")))
                }),
            409 | 422 => Err(Unread::Refused(refused(&what, status, &answer))),
            _ => Err(Unread::Transient(IoError::transient(refused(&what, status, &answer)))),
        }
    }

    fn text(&self, what: &str, url: &str) -> Result<Text, Unread> {
        let (status, answer) = self.call(url, None)?;
        match status {
            200 => {}
            404 | 422 => return Err(Unread::Refused(refused(what, status, &answer))),
            _ => return Err(Unread::Transient(IoError::transient(refused(what, status, &answer)))),
        }
        let answered: Answered = serde_json::from_slice(&answer)
            .map_err(|e| Unread::Refused(format!("the registry answered {what} unreadably: {e}")))?;
        match answered.schema_type.as_deref() {
            None | Some("AVRO") => Ok(Text {
                schema: answered.schema,
                references: answered.references,
            }),
            Some(other) => Err(Unread::Refused(format!("{what} is a {other} schema, not Avro"))),
        }
    }

    /// One call: a GET, or a POST of `body`. Answers the status and the body.
    fn call(&self, url: &str, body: Option<&str>) -> Result<(u16, Vec<u8>), Unread> {
        let sent = match body {
            None => {
                let mut request = (self.agent.get(url).config())
                    .timeout_global(Some(TIMEOUT))
                    .http_status_as_error(false)
                    .build()
                    .header("Accept", "application/vnd.schemaregistry.v1+json");
                if let Some(a) = &self.authorization {
                    request = request.header("Authorization", a);
                }
                request.call()
            }
            Some(body) => {
                // No redirect is followed: ureq would follow a POST's as a GET with no body.
                let mut request = (self.agent.post(url).config())
                    .timeout_global(Some(TIMEOUT))
                    .http_status_as_error(false)
                    .max_redirects(0)
                    .build()
                    .header("Accept", "application/vnd.schemaregistry.v1+json")
                    .header("Content-Type", "application/vnd.schemaregistry.v1+json");
                if let Some(a) = &self.authorization {
                    request = request.header("Authorization", a);
                }
                request.send(body.as_bytes())
            }
        };
        let mut response =
            sent.map_err(|e| Unread::Transient(IoError::transient(format!("the schema registry at {url}: {e}"))))?;
        let status = response.status().as_u16();
        let answer = (response.body_mut().with_config().limit(MAX_ANSWER))
            .read_to_vec()
            .map_err(|e| Unread::Transient(IoError::transient(format!("could not read {url}'s answer: {e}"))))?;
        Ok((status, answer))
    }
}

/// The registry refused `what`: its status, and the message its error answer gives, or the start of its answer.
fn refused(what: &str, status: u16, answer: &[u8]) -> String {
    #[derive(Deserialize)]
    struct Error {
        message: String,
    }
    let why = match serde_json::from_slice::<Error>(answer) {
        Ok(e) => e.message,
        Err(_) => String::from_utf8_lossy(&answer[..answer.len().min(200)]).into_owned(),
    };
    format!("the schema registry refused {what} ({status}): {why}")
}
