//! Databricks Unity Catalog, as the Delta destination reaches an external table through it: the table's storage
//! location, and short-lived credentials for that storage. Everything else is the path table's own log.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant D as destination
//!   participant W as workspace
//!   D->>W: POST /oidc/v1/token, client credentials
//!   W-->>D: token
//!   D->>W: GET /api/2.1/unity-catalog/tables/{catalog.schema.table}
//!   W-->>D: table_id, storage_location
//!   D->>W: POST /api/2.0/unity-catalog/temporary-table-credentials, READ_WRITE
//!   W-->>D: AWS keys, when they expire
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::aws::AwsCredential;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::io::IoError;
use crate::io::client_credentials::{ClientCredentials, TokenAuth};
use crate::io::http::percent_encode;

/// The most one answer may hold.
const MAX_ANSWER: u64 = 1024 * 1024;

/// The scope Unity Catalog's APIs are granted under: a secret Databricks scoped to it grants no wider one, and an
/// unscoped secret grants it too.
const SCOPE: &str = "unity-catalog";

/// A table as Unity Catalog places it.
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    pub id: String,
    /// The table's root, `s3://bucket/path`.
    pub location: String,
}

/// Credentials for a table's storage, as Unity Catalog vends them.
#[derive(Clone, Debug)]
pub struct Grant {
    pub credential: Arc<AwsCredential>,
    pub expires: SystemTime,
    /// The ARN of the S3 access point the credentials are for, when the table's external location is reached
    /// through one.
    pub access_point: Option<String>,
}

/// A blocking client of one workspace's Unity Catalog, signed in to as a service principal.
pub struct UnityCatalog {
    workspace: String,
    sign_in: ClientCredentials,
    agent: ureq::Agent,
    timeout: Duration,
}

impl UnityCatalog {
    /// `workspace` is the workspace's URL, as `https://dbc-1234.cloud.databricks.com`.
    pub fn new(
        workspace: &str,
        client_id: String,
        client_secret: String,
        agent: ureq::Agent,
        timeout: Duration,
    ) -> UnityCatalog {
        let workspace = workspace.trim().trim_end_matches('/').to_string();
        let sign_in = ClientCredentials::new(
            format!("{workspace}/oidc/v1/token"),
            client_id,
            client_secret,
            Some(SCOPE.to_string()),
            agent.clone(),
            timeout,
        )
        // Databricks documents the secret in the Authorization header.
        .sending_secret(TokenAuth::Basic);
        UnityCatalog {
            workspace,
            sign_in,
            agent,
            timeout,
        }
    }

    /// The external Delta table `name`, three-part. Err for any other table: a managed table's storage is vended
    /// read-only, and its commits go through the catalog.
    pub fn table(&mut self, name: &str) -> Result<Table, IoError> {
        #[derive(Deserialize)]
        struct Answer {
            table_id: Option<String>,
            table_type: Option<String>,
            data_source_format: Option<String>,
            storage_location: Option<String>,
        }
        let url = format!(
            "{}/api/2.1/unity-catalog/tables/{}",
            self.workspace,
            percent_encode(name)
        );
        let (status, body) = self.call(&url, None)?;
        if !(200..300).contains(&status) {
            return Err(refused(&format!("the table {name}"), status, &body));
        }
        let answer: Answer = serde_json::from_slice(&body)
            .map_err(|e| IoError::transient(format!("Unity Catalog answered the table {name} unreadably: {e}")))?;
        let refuse = |why: String| Err(IoError::transient(format!("the table {why}")));
        match answer.data_source_format.as_deref() {
            Some("DELTA") => {}
            other => return refuse(format!("is {}, not Delta", other.unwrap_or("of no format"))),
        }
        match answer.table_type.as_deref() {
            Some("EXTERNAL") => {}
            other => {
                return refuse(format!(
                    "is {}; only an external table is written",
                    other.unwrap_or("of no type")
                ));
            }
        }
        let (Some(id), Some(location)) = (answer.table_id, answer.storage_location) else {
            return refuse("has no id or no storage location".to_string());
        };
        Ok(Table { id, location })
    }

    /// Credentials to read and write the storage of the table `id`, and when they expire.
    pub fn credentials(&mut self, id: &str) -> Result<Grant, IoError> {
        #[derive(Deserialize)]
        struct Aws {
            access_key_id: String,
            secret_access_key: String,
            session_token: Option<String>,
            access_point: Option<String>,
        }
        #[derive(Deserialize)]
        struct Answer {
            aws_temp_credentials: Option<Aws>,
            /// Milliseconds since the epoch.
            expiration_time: Option<u64>,
        }
        let url = format!("{}/api/2.0/unity-catalog/temporary-table-credentials", self.workspace);
        let request = json!({"table_id": id, "operation": "READ_WRITE"});
        let (status, body) = self.call(&url, Some(&request))?;
        if !(200..300).contains(&status) {
            return Err(refused("credentials for the table's storage", status, &body));
        }
        let answer: Answer = serde_json::from_slice(&body)
            .map_err(|e| IoError::transient(format!("Unity Catalog answered the credentials unreadably: {e}")))?;
        let (Some(aws), Some(expires)) = (answer.aws_temp_credentials, answer.expiration_time) else {
            return Err(IoError::transient(
                "Unity Catalog vended no AWS credentials, or no expiry, for the table's storage; the table must be in \
                 S3",
            ));
        };
        let credential = AwsCredential {
            key_id: aws.access_key_id,
            secret_key: aws.secret_access_key,
            token: aws.session_token,
        };
        Ok(Grant {
            credential: Arc::new(credential),
            expires: UNIX_EPOCH + Duration::from_millis(expires),
            access_point: aws.access_point.filter(|arn| !arn.is_empty()),
        })
    }

    /// A GET, or a POST of `body`, with the token; a token refused is replaced once.
    fn call(&mut self, url: &str, body: Option<&Value>) -> Result<(u16, Vec<u8>), IoError> {
        let mut renewed = false;
        loop {
            let authorization = self.sign_in.bearer()?;
            let sent = match body {
                None => self
                    .agent
                    .get(url)
                    .config()
                    .timeout_global(Some(self.timeout))
                    .http_status_as_error(false)
                    .max_redirects(0)
                    .build()
                    .header("Accept", "application/json")
                    .header("Authorization", &authorization)
                    .call(),
                Some(body) => self
                    .agent
                    .post(url)
                    .config()
                    .timeout_global(Some(self.timeout))
                    .http_status_as_error(false)
                    .max_redirects(0)
                    .build()
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .header("Authorization", &authorization)
                    .send(body.to_string().as_bytes()),
            };
            let mut response = sent.map_err(|e| IoError::transient(format!("{url} failed: {e}")))?;
            let status = response.status().as_u16();
            let answer = response
                .body_mut()
                .with_config()
                .limit(MAX_ANSWER)
                .read_to_vec()
                .map_err(|e| IoError::transient(format!("could not read {url}'s answer: {e}")))?;
            if status == 401 && !renewed {
                self.sign_in.forget();
                renewed = true;
                continue;
            }
            return Ok((status, answer));
        }
    }
}

/// Unity Catalog refused `what`: its status, and its error's code and message, or the start of its answer.
fn refused(what: &str, status: u16, body: &[u8]) -> IoError {
    let answer: Value = serde_json::from_slice(body).unwrap_or_default();
    let why = match (answer["error_code"].as_str(), answer["message"].as_str()) {
        (Some(code), Some(message)) => format!("{code}: {message}"),
        (None, Some(message)) => message.to_string(),
        _ => String::from_utf8_lossy(&body[..body.len().min(200)]).trim().to_string(),
    };
    IoError::transient(format!("Unity Catalog refused {what} ({status}): {why}"))
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;

    use super::*;

    /// One request as the fake workspace read it: its line, its `Authorization`, and its body.
    #[derive(Clone, Debug)]
    struct Request {
        line: String,
        authorization: Option<String>,
        body: String,
    }

    /// A workspace on 127.0.0.1 that answers each connection with the next of `answers`, in order, and closes it.
    fn serve(answers: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<Request>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        std::thread::spawn(move || {
            for (status, body) in answers {
                // ureq connects before it refuses a header it will not send, and closes having sent nothing.
                let (mut reader, line) = loop {
                    let (conn, _) = listener.accept().unwrap();
                    let mut reader = BufReader::new(conn);
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap() > 0 {
                        break (reader, line);
                    }
                };
                let (mut length, mut authorization) = (0, None);
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let Some((name, value)) = header.trim_end().split_once(':') else {
                        break;
                    };
                    match name.to_ascii_lowercase().as_str() {
                        "content-length" => length = value.trim().parse().unwrap(),
                        "authorization" => authorization = Some(value.trim().to_string()),
                        _ => {}
                    }
                }
                let mut content = vec![0; length];
                reader.read_exact(&mut content).unwrap();
                log.lock().unwrap().push(Request {
                    line: line.trim_end().to_string(),
                    authorization,
                    body: String::from_utf8(content).unwrap(),
                });
                let mut conn = reader.into_inner();
                write!(
                    conn,
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        (url, seen)
    }

    fn unity(url: &str) -> UnityCatalog {
        // The environment's proxy would carry a loopback call away.
        let agent = ureq::Agent::config_builder().proxy(None).build().new_agent();
        UnityCatalog::new(url, "app".into(), "s3cr3t".into(), agent, Duration::from_secs(5))
    }

    fn token(value: &str) -> (u16, String) {
        (200, format!(r#"{{"access_token":"{value}","expires_in":3600}}"#))
    }

    fn table(kind: &str, format: &str) -> (u16, String) {
        let body = json!({
            "table_id": "6f1c",
            "table_type": kind,
            "data_source_format": format,
            "storage_location": "s3://lake/tables/web",
        });
        (200, body.to_string())
    }

    #[test]
    fn an_external_delta_table_is_found_after_signing_in_for_unity_catalog() {
        let (url, seen) = serve(vec![token("t1"), table("EXTERNAL", "DELTA")]);
        let found = unity(&url).table("main.web.access log").unwrap();
        assert_eq!(
            found,
            Table {
                id: "6f1c".into(),
                location: "s3://lake/tables/web".into()
            }
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].line, "POST /oidc/v1/token HTTP/1.1");
        assert!(seen[0].body.contains("scope=unity-catalog"), "{}", seen[0].body);
        assert_eq!(
            seen[1].line,
            "GET /api/2.1/unity-catalog/tables/main.web.access%20log HTTP/1.1"
        );
        assert_eq!(seen[1].authorization.as_deref(), Some("Bearer t1"));
    }

    #[test]
    fn a_table_that_is_not_an_external_delta_table_is_refused() {
        for (kind, format, why) in [
            ("MANAGED", "DELTA", "is MANAGED; only an external table"),
            ("EXTERNAL", "ICEBERG", "is ICEBERG, not Delta"),
        ] {
            let (url, _) = serve(vec![token("t"), table(kind, format)]);
            let error = unity(&url).table("main.web.access").unwrap_err().to_string();
            assert!(error.contains(why), "{error}");
        }
    }

    #[test]
    fn a_refusal_names_unity_catalogs_error() {
        let refusal = json!({"error_code": "PERMISSION_DENIED", "message": "no EXTERNAL USE SCHEMA"});
        let (url, _) = serve(vec![token("t"), (403, refusal.to_string())]);
        let error = unity(&url).table("main.web.access").unwrap_err().to_string();
        assert!(
            error.contains("refused the table main.web.access (403): PERMISSION_DENIED: no EXTERNAL USE SCHEMA"),
            "{error}"
        );
    }

    #[test]
    fn credentials_are_vended_to_read_and_write_and_expire_when_unity_catalog_says() {
        let vended = json!({
            "aws_temp_credentials": {"access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST"},
            "expiration_time": 1_700_000_000_000u64,
            "url": "s3://lake/tables/web",
        });
        let (url, seen) = serve(vec![token("t"), (200, vended.to_string())]);
        let grant = unity(&url).credentials("6f1c").unwrap();
        assert_eq!(
            (
                grant.credential.key_id.as_str(),
                grant.credential.secret_key.as_str(),
                grant.credential.token.as_deref()
            ),
            ("AK", "SK", Some("ST"))
        );
        assert_eq!(grant.expires, UNIX_EPOCH + Duration::from_millis(1_700_000_000_000));
        assert_eq!(grant.access_point, None);
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen[1].line,
            "POST /api/2.0/unity-catalog/temporary-table-credentials HTTP/1.1"
        );
        let body: Value = serde_json::from_str(&seen[1].body).unwrap();
        assert_eq!(body, json!({"table_id": "6f1c", "operation": "READ_WRITE"}));
    }

    #[test]
    fn credentials_for_an_access_point_name_it() {
        let arn = "arn:aws:s3:us-west-2:123456789012:accesspoint/lake-writer";
        let vended = json!({
            "aws_temp_credentials": {
                "access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST", "access_point": arn,
            },
            "expiration_time": 1_700_000_000_000u64,
        });
        let (url, _) = serve(vec![token("t"), (200, vended.to_string())]);
        assert_eq!(
            unity(&url).credentials("6f1c").unwrap().access_point.as_deref(),
            Some(arn)
        );
    }

    #[test]
    fn storage_outside_s3_vends_no_credentials_this_writer_takes() {
        let vended = json!({"azure_user_delegation_sas": {"sas_token": "x"}, "expiration_time": 1});
        let (url, _) = serve(vec![token("t"), (200, vended.to_string())]);
        let error = unity(&url).credentials("6f1c").unwrap_err().to_string();
        assert!(error.contains("the table must be in S3"), "{error}");
    }

    #[test]
    fn a_refused_token_is_replaced_once() {
        let (url, seen) = serve(vec![
            token("old"),
            (401, "{}".into()),
            token("new"),
            table("EXTERNAL", "DELTA"),
        ]);
        unity(&url).table("main.web.access").unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen[3].authorization.as_deref(), Some("Bearer new"));
    }

    /// Random answers from a fake workspace over a seed range. Each answer is generated as the client asks for it,
    /// beside a model of what the client must make of it.
    mod fuzz {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        use object_store::StaticCredentialProvider;
        use serde_json::Map;

        use super::*;
        use crate::fuzz::{Coverage, Rng};
        use crate::io::objects::{self, CatalogGrant};
        use crate::plan::topology::{HeldSecrets, S3Access};

        /// The first of `values` half the time, else any.
        fn mostly<'a, T>(rng: &mut Rng, values: &'a [T]) -> &'a T {
            if rng.chance(2) { &values[0] } else { rng.pick(values) }
        }

        /// The statuses a workspace refuses with, 401 aside.
        const REFUSALS: &[u16] = &[400, 403, 404, 409, 429, 500, 502, 503];

        const TABLE_TYPES: &[&str] = &[
            "EXTERNAL",
            "MANAGED",
            "VIEW",
            "external",
            "EXTERNAL ",
            "STREAMING_TABLE",
        ];

        const FORMATS: &[&str] = &["DELTA", "ICEBERG", "PARQUET", "delta", "DELTASHARING", "CSV"];

        /// Storage locations, by the name coverage reaches them under. The client takes any of them as given.
        const LOCATIONS: &[(&str, &str)] = &[
            ("s3", "s3://lake/tables/web"),
            ("s3a", "s3a://lake/tables/web"),
            ("abfss", "abfss://lake@account.dfs.core.windows.net/tables/web"),
            ("gs", "gs://lake/tables/web"),
            ("no bucket", "s3://"),
            ("one slash", "s3:/lake/tables"),
            ("no scheme", "lake/tables/web"),
            ("unicode", "S3://LAKE/ü/表"),
            ("file", "file:///etc/passwd"),
        ];

        /// Milliseconds since the epoch.
        const EXPIRATIONS: &[(&str, u64)] = &[
            ("now", 1_700_000_000_000),
            ("zero", 0),
            ("past", 1),
            ("far future", 4_102_444_800_000),
            ("i64 max", i64::MAX as u64),
            ("u64 max", u64::MAX),
        ];

        /// Seconds. A token of 60 or under is replaced before every call.
        const LIFETIMES: &[u64] = &[3600, 0, 1, 59, 60, 86_400, 1 << 40, i64::MAX as u64 / 2, u64::MAX];

        /// A partition, a region in it, and the domain an access point there is reached under.
        const REGIONS: &[(&str, &str, &str)] = &[
            ("aws", "us-east-1", "amazonaws.com"),
            ("aws", "eu-west-3", "amazonaws.com"),
            ("aws", "ap-southeast-5", "amazonaws.com"),
            ("aws", "il-central-1", "amazonaws.com"),
            ("aws-us-gov", "us-gov-west-1", "amazonaws.com"),
            ("aws-cn", "cn-north-1", "amazonaws.com.cn"),
            ("aws-cn", "cn-northwest-1", "amazonaws.com.cn"),
            ("aws-iso", "us-iso-east-1", "c2s.ic.gov"),
            ("aws-iso-b", "us-isob-east-1", "sc2s.sgov.gov"),
            ("aws-iso-f", "us-isof-south-1", "csp.hci.ic.gov"),
            ("aws-iso-e", "eu-isoe-west-1", "cloud.adc-e.uk"),
            ("aws-eusc", "eusc-de-east-1", "amazonaws.eu"),
        ];

        const ARN_SHAPES: &[&str] = &[
            "arn well formed",
            "arn multi-region",
            "arn field replaced",
            "arn field dropped",
            "arn character put in",
            "arn cut short",
        ];

        /// What the workspace fuzz must reach. The access point shapes it also draws are the other test's to cover.
        const ANSWERS: &[&str] = &[
            "token 2xx",
            "token 401",
            "token 4xx",
            "token 5xx",
            "table 2xx",
            "table 401",
            "table 4xx",
            "table 5xx",
            "credentials 2xx",
            "credentials 401",
            "credentials 4xx",
            "credentials 5xx",
            "body object",
            "body extra field",
            "body truncated",
            "body garbage",
            "body duplicate field",
            "body oversize",
            "field absent",
            "field null",
            "field wrong type",
            "field empty",
            "field unicode",
            "field escapes",
            "field long",
            "field likely",
            "location s3",
            "location s3a",
            "location abfss",
            "location gs",
            "location no bucket",
            "location one slash",
            "location no scheme",
            "location unicode",
            "location file",
            "expiry now",
            "expiry zero",
            "expiry past",
            "expiry far future",
            "expiry i64 max",
            "expiry u64 max",
            "expiry not a u64",
            "lifetime short",
            "lifetime long",
            "lifetime none",
            "token refused by the client",
            "token missing",
            "token replaced after 401",
            "token refused twice",
            "table taken",
            "table not delta",
            "table not external",
            "table without id or location",
            "table unreadable",
            "credentials taken",
            "credentials through an access point",
            "credentials not for S3",
            "credentials unreadable",
        ];

        fn status(rng: &mut Rng, cov: &mut Coverage, call: &str) -> u16 {
            let status = match rng.below(8) {
                0 => 401,
                1 => *rng.pick(REFUSALS),
                2 => 201,
                _ => 200,
            };
            let class = match status {
                200..300 => "2xx",
                401 => "401",
                400..500 => "4xx",
                _ => "5xx",
            };
            cov.reach(format!("{call} {class}"));
            status
        }

        fn wrong_type(rng: &mut Rng) -> Value {
            let values = [
                json!(0),
                json!(-1),
                json!(true),
                json!([]),
                json!({}),
                json!(1.5),
                json!(["x"]),
            ];
            rng.pick(&values).clone()
        }

        /// One of `likely`, or empty, unicode, escapes, or long.
        fn text(rng: &mut Rng, cov: &mut Coverage, likely: &[&str]) -> String {
            let (name, text) = match rng.below(12) {
                0 => ("empty", String::new()),
                1 => ("unicode", "tablé-表-🦀-\u{202e}".into()),
                2 => ("escapes", "quote \" backslash \\ newline \n nul \u{0}".into()),
                3 => ("long", "x".repeat(rng.below(20_000))),
                _ => ("likely", mostly(rng, likely).to_string()),
            };
            cov.reach(format!("field {name}"));
            text
        }

        /// A field's value: absent, null, of the wrong type, or a string.
        fn field(rng: &mut Rng, cov: &mut Coverage, likely: &[&str]) -> Option<Value> {
            let (name, value) = match rng.below(6) {
                0 => ("absent", None),
                1 => ("null", Some(Value::Null)),
                2 => ("wrong type", Some(wrong_type(rng))),
                _ => return Some(Value::String(text(rng, cov, likely))),
            };
            cov.reach(format!("field {name}"));
            value
        }

        /// How a field of `Option<String>` reads `value`; Err for another type.
        fn optional_text(value: Option<&Value>) -> Result<Option<String>, ()> {
            match value {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(s)) => Ok(Some(s.clone())),
                Some(_) => Err(()),
            }
        }

        /// How a field of `String` reads `value`.
        fn required_text(value: Option<&Value>) -> Result<String, ()> {
            optional_text(value)?.ok_or(())
        }

        /// How a field of `Option<u64>` reads `value`.
        fn optional_millis(value: Option<&Value>) -> Result<Option<u64>, ()> {
            match value {
                None | Some(Value::Null) => Ok(None),
                Some(v) => v.as_u64().map(Some).ok_or(()),
            }
        }

        /// `fields` sent as a body, and whether the body still reads as the object `fields` holds: it may be cut
        /// short, replaced with something that is no such object, or hold one of `known` twice, which a reader
        /// refuses.
        fn body(rng: &mut Rng, cov: &mut Coverage, mut fields: Map<String, Value>, known: &[&str]) -> (String, bool) {
            if rng.chance(3) {
                cov.reach("body extra field");
                let key = rng.pick(&["url", "owner", "properties", "ünï", "", "azure_user_delegation_sas"]);
                let value = rng
                    .pick(&[json!("x"), json!(7), json!({"a": [1, null]}), Value::Null])
                    .clone();
                fields.entry(key.to_string()).or_insert(value);
            }
            let twice = known.iter().find(|k| fields.contains_key(**k));
            let whole = Value::Object(fields.clone()).to_string();
            match (rng.below(10), twice) {
                (0, _) => {
                    cov.reach("body truncated");
                    let mut end = rng.below(whole.len());
                    while !whole.is_char_boundary(end) {
                        end -= 1;
                    }
                    (whole[..end].to_string(), false)
                }
                (1, _) => {
                    cov.reach("body garbage");
                    let garbage = [
                        "",
                        "null",
                        "[]",
                        "[1]",
                        "\"x\"",
                        "7",
                        "<html>bad gateway</html>",
                        "{",
                        "{}}",
                        "\u{feff}{}",
                        r#"{"a":}"#,
                    ];
                    (rng.pick(&garbage).to_string(), false)
                }
                (2, Some(key)) => {
                    cov.reach("body duplicate field");
                    (format!("{{{key:?}:null,{}", &whole[1..]), false)
                }
                _ => {
                    cov.reach("body object");
                    (whole, true)
                }
            }
        }

        /// The body of a refusal.
        fn refusal(rng: &mut Rng, cov: &mut Coverage) -> String {
            match rng.below(6) {
                0 => String::new(),
                1 => {
                    let message = text(rng, cov, &["no USE CATALOG"]);
                    json!({"error_code": "PERMISSION_DENIED", "message": message}).to_string()
                }
                2 => json!({"message": "Table does not exist"}).to_string(),
                3 => json!({"error_code": 5, "message": null}).to_string(),
                4 => "<html>ü".repeat(rng.below(500)),
                _ => "{\"error_code\":\"TEMPORARILY_UNAVAILABLE\"".to_string(),
            }
        }

        /// An access point's ARN: well formed in some partition and region, multi-region, or malformed. Some of the
        /// address and region it is reached at when it is well formed.
        fn access_point(rng: &mut Rng, cov: &mut Coverage) -> (String, Option<(String, String)>) {
            let (partition, region, domain) = *rng.pick(REGIONS);
            cov.reach(format!("partition {partition}"));
            let alphabet = b"abcdefghijklmnopqrstuvwxyz0123456789-";
            let name: String = (0..3 + rng.below(48)).map(|_| *rng.pick(alphabet) as char).collect();
            let account = format!("{:012}", rng.next_u64() % 1_000_000_000_000);
            let arn = format!("arn:{partition}:s3:{region}:{account}:accesspoint/{name}");
            match rng.below(4) {
                0 => {
                    cov.reach("arn multi-region");
                    (format!("arn:{partition}:s3::{account}:accesspoint/{name}.mrap"), None)
                }
                1 => (malformed(rng, cov, &arn), None),
                _ => {
                    cov.reach("arn well formed");
                    let address = format!("https://{name}-{account}.s3-accesspoint.{region}.{domain}");
                    (arn, Some((address, region.to_string())))
                }
            }
        }

        /// `arn` with a field replaced or dropped, a character put in, or cut short.
        fn malformed(rng: &mut Rng, cov: &mut Coverage, arn: &str) -> String {
            let odd = [
                "",
                "S3",
                "s3-outposts",
                "ARN",
                " arn",
                "accesspoint",
                "accesspoint/",
                "accesspoint/a/b",
                "bucket/lake",
                "表",
                "us-east-1 ",
                "ACCESSPOINT/lake",
                "123456789012:extra",
            ];
            let mut fields: Vec<String> = arn.split(':').map(str::to_string).collect();
            let at = rng.below(fields.len());
            match rng.below(4) {
                0 => {
                    cov.reach("arn field replaced");
                    fields[at] = rng.pick(&odd).to_string();
                }
                1 => {
                    cov.reach("arn field dropped");
                    fields.remove(at);
                }
                2 => {
                    cov.reach("arn character put in");
                    let mut cut = rng.below(fields[at].len() + 1);
                    while !fields[at].is_char_boundary(cut) {
                        cut -= 1;
                    }
                    fields[at].insert(
                        cut,
                        *rng.pick(&['#', '?', '@', '/', '%', ' ', '\0', 'é', '.', 'A', '\n']),
                    );
                }
                _ => {
                    cov.reach("arn cut short");
                    let mut cut = rng.below(arn.len());
                    while !arn.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    return arn[..cut].to_string();
                }
            }
            fields.join(":")
        }

        /// A workspace's answers, generated as the client asks, and the model of the client's sign-in.
        struct Workspace {
            answers: Vec<(u16, String)>,
            /// The token held, and whether it outlives the next call.
            held: Option<(String, bool)>,
            /// The token each call to Unity Catalog's API carries, in order.
            bearers: Vec<String>,
            signed_in: usize,
        }

        impl Workspace {
            /// Answers a sign-in. Err is a phrase the error must hold.
            fn sign_in(&mut self, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
                const URL: &str = "/oidc/v1/token";
                self.signed_in += 1;
                let status = status(rng, cov, "token");
                let good = format!("t{}", self.signed_in);
                let mut fields = Map::new();
                let token = match rng.below(4) {
                    0 => field(rng, cov, &[&good]),
                    _ => Some(Value::String(good.clone())),
                };
                if let Some(token) = token {
                    fields.insert("access_token".into(), token);
                }
                let life = match rng.below(6) {
                    0 => None,
                    1 => Some(
                        rng.pick(&[Value::Null, json!("3600"), json!(-1), json!(1.5), json!(1e300)])
                            .clone(),
                    ),
                    _ => Some(json!(mostly(rng, LIFETIMES))),
                };
                if let Some(life) = life {
                    fields.insert("expires_in".into(), life);
                }
                fields.insert("token_type".into(), json!("Bearer"));
                let (mut body, _) = body(rng, cov, fields, &[]);
                if rng.chance(40) {
                    cov.reach("body oversize");
                    body = json!({"access_token": "t".repeat(70 * 1024)}).to_string();
                }
                self.answers.push((status, body.clone()));
                if body.len() > 64 * 1024 {
                    return Err("could not read http://127.0.0.1:".to_string());
                }
                if !(200..300).contains(&status) {
                    return Err(format!("{URL} refused the sign-in ({status})"));
                }
                let answer: Value = serde_json::from_str(&body).unwrap_or_default();
                let token = answer.get("access_token").and_then(Value::as_str).unwrap_or_default();
                if token.is_empty() {
                    cov.reach("token missing");
                    return Err(format!("{URL} answered the sign-in with no access_token"));
                }
                let lasts = match answer.get("expires_in").and_then(Value::as_u64) {
                    None => {
                        cov.reach("lifetime none");
                        true
                    }
                    Some(life) => {
                        cov.reach(if life > 60 { "lifetime long" } else { "lifetime short" });
                        life > 60
                    }
                };
                self.held = Some((token.to_string(), lasts));
                Ok(())
            }

            /// Answers one call to Unity Catalog's API at `path`, signing in as the client must: `answer` makes a 2xx
            /// answer's body and what the call comes to, `refused` the phrase a refusal with a status holds.
            fn call<T>(
                &mut self,
                rng: &mut Rng,
                cov: &mut Coverage,
                (call, path): (&str, &str),
                mut answer: impl FnMut(&mut Rng, &mut Coverage) -> (String, Result<T, String>),
                refused: impl Fn(u16) -> String,
            ) -> Result<T, String> {
                let mut renewed = false;
                loop {
                    let token = match &self.held {
                        Some((token, true)) => token.clone(),
                        _ => {
                            self.sign_in(rng, cov)?;
                            self.held.as_ref().map(|(t, _)| t.clone()).unwrap_or_default()
                        }
                    };
                    // ureq sends a header only when its value is visible ASCII, spaces and tabs.
                    if !token.chars().all(|c| c == '\t' || (' '..='~').contains(&c)) {
                        cov.reach("token refused by the client");
                        return Err(format!("{path} failed"));
                    }
                    self.bearers.push(token);
                    let status = status(rng, cov, call);
                    if (200..300).contains(&status) {
                        let (body, outcome) = answer(rng, cov);
                        self.answers.push((status, body));
                        return outcome;
                    }
                    self.answers.push((status, refusal(rng, cov)));
                    if status == 401 {
                        if renewed {
                            cov.reach("token refused twice");
                        } else {
                            cov.reach("token replaced after 401");
                            self.held = None;
                            renewed = true;
                            continue;
                        }
                    }
                    return Err(refused(status));
                }
            }
        }

        /// A table's answer, and what `table(name)` makes of it.
        fn table_answer(rng: &mut Rng, cov: &mut Coverage, name: &str) -> (String, Result<Table, String>) {
            let (scheme, location) = *rng.pick(LOCATIONS);
            cov.reach(format!("location {scheme}"));
            let mut slots = [
                ("table_id", Some(json!("6f1c"))),
                ("table_type", Some(json!("EXTERNAL"))),
                ("data_source_format", Some(json!("DELTA"))),
                ("storage_location", Some(json!(location))),
            ];
            let locations: Vec<&str> = LOCATIONS.iter().map(|(_, l)| *l).collect();
            for _ in 0..rng.below(4) {
                let at = rng.below(slots.len());
                let likely: &[&str] = match slots[at].0 {
                    "table_type" => TABLE_TYPES,
                    "data_source_format" => FORMATS,
                    "storage_location" => &locations,
                    _ => &["6f1c", "c0ffee"],
                };
                slots[at].1 = field(rng, cov, likely);
            }
            let fields: Map<String, Value> = slots
                .iter()
                .filter_map(|(k, v)| v.clone().map(|v| (k.to_string(), v)))
                .collect();
            let known: Vec<&str> = slots.iter().map(|(k, _)| *k).collect();
            let (body, readable) = body(rng, cov, fields, &known);
            let read = |at: usize| optional_text(slots[at].1.as_ref());
            let (outcome, expected) = match (readable, read(0), read(1), read(2), read(3)) {
                (true, Ok(id), Ok(kind), Ok(format), Ok(location)) => {
                    if format.as_deref() != Some("DELTA") {
                        ("table not delta", Err("not Delta".to_string()))
                    } else if kind.as_deref() != Some("EXTERNAL") {
                        (
                            "table not external",
                            Err("only an external table is written".to_string()),
                        )
                    } else {
                        match (id, location) {
                            (Some(id), Some(location)) => ("table taken", Ok(Table { id, location })),
                            _ => (
                                "table without id or location",
                                Err("the table has no id or no storage location".to_string()),
                            ),
                        }
                    }
                }
                _ => (
                    "table unreadable",
                    Err(format!("Unity Catalog answered the table {name} unreadably")),
                ),
            };
            cov.reach(outcome);
            (body, expected)
        }

        /// What `credentials` vends, as the test compares it.
        #[derive(Debug, PartialEq)]
        struct Vended {
            key: String,
            secret: String,
            token: Option<String>,
            expires: SystemTime,
            access_point: Option<String>,
        }

        /// An answer's access key id, secret access key, session token and access point.
        type AwsFields = (String, String, Option<String>, Option<String>);

        /// A credentials answer, and what `credentials` makes of it.
        fn credentials_answer(rng: &mut Rng, cov: &mut Coverage) -> (String, Result<Vended, String>) {
            let (arn, _) = access_point(rng, cov);
            let mut aws = [
                ("access_key_id", Some(json!("AK"))),
                ("secret_access_key", Some(json!("SK"))),
                ("session_token", Some(json!("ST"))),
                ("access_point", rng.chance(2).then(|| json!(arn))),
            ];
            let mut aws_whole = true;
            let expiry = |rng: &mut Rng, cov: &mut Coverage| {
                let (name, millis) = *mostly(rng, EXPIRATIONS);
                cov.reach(format!("expiry {name}"));
                Some(json!(millis))
            };
            let mut expiration = expiry(rng, cov);
            for _ in 0..rng.below(4) {
                match rng.below(6) {
                    4 => {
                        expiration = match rng.below(4) {
                            0 => None,
                            1 => Some(Value::Null),
                            2 => {
                                cov.reach("expiry not a u64");
                                let odd = [json!("1700000000000"), json!(-1), json!(1.5), json!(1e300)];
                                Some(rng.pick(&odd).clone())
                            }
                            _ => expiry(rng, cov),
                        }
                    }
                    5 => aws_whole = false,
                    at => aws[at].1 = field(rng, cov, &["AK", "ASIA0000", &arn]),
                }
            }
            let mut fields = Map::new();
            let aws_value = match aws_whole {
                true => Some(Value::Object(
                    aws.iter()
                        .filter_map(|(k, v)| v.clone().map(|v| (k.to_string(), v)))
                        .collect(),
                )),
                false => rng
                    .pick(&[None, Some(Value::Null), Some(json!("AK")), Some(json!([]))])
                    .clone(),
            };
            if let Some(value) = &aws_value {
                fields.insert("aws_temp_credentials".into(), value.clone());
            }
            if let Some(value) = &expiration {
                fields.insert("expiration_time".into(), value.clone());
            }
            let (body, readable) = body(rng, cov, fields, &["aws_temp_credentials", "expiration_time"]);
            let read_aws = || -> Result<Option<AwsFields>, ()> {
                match &aws_value {
                    None | Some(Value::Null) => Ok(None),
                    Some(Value::Object(_)) => Ok(Some((
                        required_text(aws[0].1.as_ref())?,
                        required_text(aws[1].1.as_ref())?,
                        optional_text(aws[2].1.as_ref())?,
                        optional_text(aws[3].1.as_ref())?,
                    ))),
                    Some(_) => Err(()),
                }
            };
            let (outcome, expected) = match (readable, read_aws(), optional_millis(expiration.as_ref())) {
                (true, Ok(Some((key, secret, token, access_point))), Ok(Some(millis))) => {
                    let access_point = access_point.filter(|a| !a.is_empty());
                    let outcome = match access_point {
                        Some(_) => "credentials through an access point",
                        None => "credentials taken",
                    };
                    let expires = UNIX_EPOCH + Duration::from_millis(millis);
                    let vended = Vended {
                        key,
                        secret,
                        token,
                        expires,
                        access_point,
                    };
                    (outcome, Ok(vended))
                }
                (true, Ok(_), Ok(_)) => (
                    "credentials not for S3",
                    Err("vended no AWS credentials, or no expiry".to_string()),
                ),
                _ => (
                    "credentials unreadable",
                    Err("Unity Catalog answered the credentials unreadably".to_string()),
                ),
            };
            cov.reach(outcome);
            (body, expected)
        }

        /// Answers shown short, for a failure's message.
        fn shown(answers: &[(u16, String)]) -> Vec<(u16, String)> {
            answers
                .iter()
                .map(|(s, b)| (*s, b.chars().take(300).collect()))
                .collect()
        }

        /// One seed: one to three calls, each for a table or credentials, through one client. Err says what the
        /// client did that the model did not.
        fn check(seed: u64, cov: &mut Coverage) -> Result<(), String> {
            let mut rng = Rng::seeded(seed);
            let names = [
                "main.web.access",
                "main.web.access log",
                "카탈로그.스키마.표",
                "",
                "a/b?c#d%e",
            ];
            let calls: Vec<(bool, String)> = (0..1 + rng.below(3))
                .map(|_| (rng.chance(2), rng.pick(&names).to_string()))
                .collect();
            let mut workspace = Workspace {
                answers: Vec::new(),
                held: None,
                bearers: Vec::new(),
                signed_in: 0,
            };
            let mut expected = Vec::new();
            for (is_table, name) in &calls {
                expected.push(if *is_table {
                    let path = format!("/api/2.1/unity-catalog/tables/{}", percent_encode(name));
                    let refused = |status| format!("Unity Catalog refused the table {name} ({status})");
                    let answer = |rng: &mut Rng, cov: &mut Coverage| table_answer(rng, cov, name);
                    let outcome = workspace.call(&mut rng, cov, ("table", &path), answer, refused);
                    outcome.map(|t| format!("{t:?}"))
                } else {
                    let path = "/api/2.0/unity-catalog/temporary-table-credentials";
                    let refused =
                        |status| format!("Unity Catalog refused credentials for the table's storage ({status})");
                    let outcome = workspace.call(&mut rng, cov, ("credentials", path), credentials_answer, refused);
                    outcome.map(|v| format!("{v:?}"))
                });
            }
            let answers = workspace.answers.clone();
            let (url, seen) = serve(answers.clone());
            let mut unity = unity(&url);
            let fail = |what: String| Err(format!("{what}\ncalls {calls:?}\nanswers {:?}", shown(&answers)));
            for ((is_table, name), expected) in calls.iter().zip(expected) {
                let before = seen.lock().unwrap().len();
                let got = catch_unwind(AssertUnwindSafe(|| {
                    if *is_table {
                        unity.table(name).map(|t| format!("{t:?}"))
                    } else {
                        unity.credentials(name).map(|grant| {
                            let vended = Vended {
                                key: grant.credential.key_id.clone(),
                                secret: grant.credential.secret_key.clone(),
                                token: grant.credential.token.clone(),
                                expires: grant.expires,
                                access_point: grant.access_point,
                            };
                            format!("{vended:?}")
                        })
                    }
                }));
                let Ok(got) = got else {
                    return fail("the call panicked".into());
                };
                match (&got, &expected) {
                    (Ok(got), Ok(expected)) if got == expected => {}
                    (Err(error), Err(phrase)) if error.message.contains(phrase.as_str()) => {}
                    _ => return fail(format!("got {got:?}, expected {expected:?}")),
                }
                let seen = seen.lock().unwrap();
                let api = |r: &Request| !r.line.contains("/oidc/v1/token");
                let first_api = seen[before..].iter().position(api).unwrap_or(seen.len() - before);
                let replaced = seen[before + first_api..].iter().filter(|r| !api(r)).count();
                if replaced > 1 {
                    return fail(format!("the token was replaced {replaced} times in one call"));
                }
            }
            let seen = seen.lock().unwrap();
            if seen.len() != answers.len() {
                return fail(format!("{} requests, not {}", seen.len(), answers.len()));
            }
            let api_calls = seen.iter().filter(|r| !r.line.contains("/oidc/v1/token"));
            for (request, token) in api_calls.zip(&workspace.bearers) {
                let sent = format!("Bearer {token}");
                if request.authorization.as_deref() != Some(sent.trim()) {
                    return fail(format!(
                        "{} carried {:?}, not {sent:?}",
                        request.line, request.authorization
                    ));
                }
            }
            Ok(())
        }

        #[test]
        fn random_answers_are_taken_or_refused_by_name() {
            let mut cov = Coverage::new(ANSWERS.iter().copied());
            crate::fuzz::run(0..1000, |seed| check(seed, &mut cov));
            cov.assert_complete();
        }

        #[test]
        fn an_access_point_is_reached_over_https_in_its_arns_region_or_refused() {
            let partitions = REGIONS.iter().map(|(p, _, _)| format!("partition {p}"));
            let mut cov = Coverage::new(ARN_SHAPES.iter().map(|s| s.to_string()).chain(partitions));
            let runtime = objects::runtime().unwrap();
            let secrets = HeldSecrets::default();
            let access = S3Access {
                region: None,
                endpoint: None,
                auth_endpoint: None,
                access_key: None,
                role: None,
            };
            crate::fuzz::run(0..2000, |seed| {
                let mut rng = Rng::seeded(seed);
                let (arn, well_formed) = access_point(&mut rng, &mut cov);
                let credential = AwsCredential {
                    key_id: "AK".into(),
                    secret_key: "SK".into(),
                    token: None,
                };
                let grant = CatalogGrant {
                    credentials: Arc::new(StaticCredentialProvider::new(credential)),
                    access_point: Some(arn.clone()),
                };
                let stall = Duration::from_secs(1);
                let store = match objects::s3_bucket(&access, "lake", Some(grant), None, stall, &secrets, &runtime) {
                    Err(e) if well_formed.is_some() => return Err(format!("{arn:?} refused: {}", e.message)),
                    Err(e) if !e.message.contains(&format!("{arn:?}")) => {
                        return Err(format!("{arn:?} refused without naming it: {}", e.message));
                    }
                    Err(_) => return Ok(()),
                    Ok(store) => store,
                };
                // The store shows its address and region only in its Debug, which escapes them as the region is
                // escaped below.
                let shown = format!("{store:?}");
                let quoted = |field: &str| {
                    let start = shown.find(&format!("{field}: \""))? + field.len() + 3;
                    let value = &shown[start..];
                    Some(value[..value.find('"')?].to_string())
                };
                let (address, region) = (quoted("bucket_endpoint"), quoted("region"));
                let arns_region = arn
                    .split(':')
                    .nth(3)
                    .map(|r| format!("{r:?}").trim_matches('"').to_string());
                if region != arns_region {
                    return Err(format!("{arn:?} is signed for {region:?}, not its own region"));
                }
                let address = address.unwrap_or_default();
                let host = address.strip_prefix("https://").unwrap_or_default();
                let plain = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.';
                if host.is_empty() || !host.bytes().all(plain) {
                    return Err(format!("{arn:?} is reached at {address:?}"));
                }
                match well_formed {
                    Some((expected, region)) if (&address, arns_region.as_deref()) != (&expected, Some(&*region)) => {
                        Err(format!(
                            "{arn:?} is reached at {address:?} in {arns_region:?}, not {expected:?}"
                        ))
                    }
                    _ => Ok(()),
                }
            });
            cov.assert_complete();
        }
    }
}
