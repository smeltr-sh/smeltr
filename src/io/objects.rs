//! The object store destinations -- S3, Google Cloud Storage, Azure Blob Storage and an SFTP directory -- each the
//! file destination (`io::files`) writing into its store, built by [`destination`]: one file open per prefix, and per
//! record for Parquet, Avro and CSV, across batches, finished at the file interval or at its size, and a batch
//! acknowledged once every file holding its rows is finished. Parquet files are written by `files::ParquetFormat`,
//! Avro by `avro::container` and CSV by `io::csv`; NDJSON, a JSON array, text and bytes by `lines::LineFormat`, framed
//! and then compressed as a stream, as CSV's lines are.
//!
//! ```mermaid
//! flowchart LR
//!   R[rows] --> P[partition: prefix, and record for Parquet, Avro and CSV] --> F{format}
//!   F -->|parquet| Q[Parquet row groups]
//!   F -->|avro| A[Avro blocks]
//!   F -->|csv| V[a header, then a line per row]
//!   V --> L
//!   F -->|ndjson, json, text, bytes| L[lines, then the codec]
//!   Q --> M["multipart upload &lt;prefix&gt;&lt;id&gt;.&lt;ext&gt;"]
//!   A --> M
//!   L --> M
//! ```
//!
//! Two objects never share a key, so identical rows written twice are two objects. A batch handed over again after a
//! failure goes into new files, a duplicate where a file holding it had been finished, as for every at-least-once
//! destination.
//!
//! `object_store` signs the requests and finds the host's credentials. Only [`store`] differs by cloud. Its client is
//! async, so each destination runs a small tokio runtime of its own, which the file destination's writer thread waits
//! on.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsAuthorizer, AwsCredential, AwsCredentialProvider};
use object_store::azure::{AzureAuthorizer, AzureConfigKey, AzureCredentialProvider, MicrosoftAzureBuilder};
use object_store::client::{HttpClient, HttpConnector, HttpRequestBody};
use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, ClientConfigKey, ClientOptions, CredentialProvider, ObjectStore,
};
use tokio_util::bytes::Bytes;

use super::avro::container::AvroFormat;
use super::csv::CsvFormat;
use super::files::{
    CommitError, Committer, DataFile, FileConfig, FileDestination, FileFormat, ParquetFormat, Partition, Partitioner,
};
use super::http::percent_encode;
use super::lines::LineFormat;
use super::parquet::Schema;
use super::{IoError, OwnedRecord};
use crate::core::column::DataType;
use crate::format::json_scan::{Next, Reader};
use crate::plan::topology::{
    AssumedRole, AzureAccess, AzureCredential, AzureStore, Cloud, Compression, GcsAccess, GcsStore, HeldSecrets,
    Identity, ObjectFormat, ObjectsDestinationConfig, Records, S3Access, S3Store,
};

/// The placeholders a configured prefix may hold, each filled from the write time.
const PLACEHOLDERS: [char; 5] = ['Y', 'm', 'd', 'H', 'M'];

/// What S3 takes as an object's storage class.
pub const S3_STORAGE_CLASSES: &[&str] = &[
    "STANDARD",
    "REDUCED_REDUNDANCY",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER",
    "GLACIER_IR",
    "DEEP_ARCHIVE",
    "SNOW",
];

/// What Google Cloud Storage takes as an object's storage class.
pub const GCS_STORAGE_CLASSES: &[&str] = &["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"];

/// What Azure Blob Storage takes as a blob's access tier.
pub const AZURE_ACCESS_TIERS: &[&str] = &["Hot", "Cool", "Cold", "Archive"];

/// The longest key, in bytes, for every cloud. S3 and Google count bytes; Azure counts characters, which a key of
/// this many bytes never exceeds, so one limit serves all three.
pub(crate) const MAX_KEY_BYTES: usize = 1024;

/// What a key adds after its prefix: the id, a dot, and the longest extension.
const KEY_TAIL_BYTES: usize = 2 * ID_BYTES + 1 + "ndjson.zst".len();

/// Random bytes in an object's id: 128 bits, past any chance of two objects meeting.
const ID_BYTES: usize = 16;

/// Why `prefix` cannot be a destination's prefix, if it cannot: a `%` other than the placeholders, or a key S3 or
/// `object_store` would refuse or rewrite.
pub fn check_prefix(prefix: &str) -> Result<(), String> {
    let mut chars = prefix.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.next() {
                Some(p) if PLACEHOLDERS.contains(&p) => {}
                _ => return Err("a % starts one of %Y %m %d %H %M, and nothing else".to_string()),
            }
        }
    }
    check_literal(&expand(prefix, SystemTime::UNIX_EPOCH))
}

/// Why `prefix`, taken as it stands, cannot start an object key.
fn check_literal(prefix: &str) -> Result<(), String> {
    if prefix.starts_with('/') {
        return Err("a key does not start with /".to_string());
    }
    if prefix.len() + KEY_TAIL_BYTES > MAX_KEY_BYTES {
        return Err(format!(
            "a key is at most {MAX_KEY_BYTES} bytes, and this leaves no room for the object's name"
        ));
    }
    // A complete key, as `object_store` would read it: an empty segment, `.` or `..` is refused there, and the
    // object would be written somewhere other than where the prefix says.
    Path::parse(format!("{prefix}x"))
        .map(|_| ())
        .map_err(|e| format!("it is no key prefix: {e}"))
}

/// `prefix` with each placeholder filled from `at`, UTC.
fn expand(prefix: &str, at: SystemTime) -> String {
    if !prefix.contains('%') {
        return prefix.to_string();
    }
    let secs = at
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    let (year, month, day) = crate::ops::civil::civil_from_days(secs.div_euclid(86_400));
    let of_day = secs.rem_euclid(86_400);
    let mut out = String::with_capacity(prefix.len() + 8);
    let mut chars = prefix.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('Y') => out.push_str(&format!("{year:04}")),
            Some('m') => out.push_str(&format!("{month:02}")),
            Some('d') => out.push_str(&format!("{day:02}")),
            Some('H') => out.push_str(&format!("{:02}", of_day / 3600)),
            Some('M') => out.push_str(&format!("{:02}", of_day % 3600 / 60)),
            // Refused at load.
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

/// The prefix `row` goes under: its `__destination.prefix` as it stands, else `template` filled from now.
fn prefix_of(template: &str, row: &OwnedRecord) -> Result<String, String> {
    match &row.placement {
        // Taken as it stands: a `%` in row data is data.
        Some(prefix) => {
            check_literal(prefix).map_err(|why| format!("its __destination.prefix {prefix:?} is refused: {why}"))?;
            Ok(prefix.clone())
        }
        None => Ok(expand(template, SystemTime::now())),
    }
}

/// The name a destination on `cloud` goes by in errors and thread names.
fn cloud_name(cloud: &Cloud) -> &'static str {
    match cloud {
        Cloud::S3(_) => "s3",
        Cloud::Gcs(_) => "gcs",
        Cloud::Azure(_) => "azure blob",
        Cloud::Sftp(_) => "sftp",
    }
}

/// What every object is written with: its content type, and `storage_class`, the bucket's default when `None`.
fn attributes(content_type: &'static str, storage_class: Option<&str>) -> Attributes {
    let mut attributes = Attributes::new();
    attributes.insert(Attribute::ContentType, AttributeValue::from(content_type));
    if let Some(class) = storage_class {
        attributes.insert(Attribute::StorageClass, AttributeValue::from(class.to_string()));
    }
    attributes
}

/// What an object's key ends with.
fn extension(format: &ObjectFormat, compression: Compression) -> String {
    let format = match format {
        ObjectFormat::Parquet => return "parquet".into(),
        ObjectFormat::Avro => return "avro".into(),
        ObjectFormat::Ndjson => "ndjson",
        ObjectFormat::Csv => "csv",
        ObjectFormat::Json => "json",
        ObjectFormat::Text { .. } => "txt",
        ObjectFormat::Bytes { .. } => "raw",
    };
    match compression {
        Compression::None => format.into(),
        Compression::Gzip => format!("{format}.gz"),
        Compression::Snappy => format!("{format}.sz"),
        Compression::Lz4 => format!("{format}.lz4"),
        Compression::Zstd => format!("{format}.zst"),
    }
}

/// An object's content type. Parquet and Avro compress inside the file, so their types never name the codec.
fn content_type(format: &ObjectFormat, compression: Compression) -> &'static str {
    match (format, compression) {
        (ObjectFormat::Parquet, _) => "application/vnd.apache.parquet",
        (ObjectFormat::Avro, _) => "application/avro",
        (_, Compression::Gzip) => "application/gzip",
        (_, Compression::Snappy) => "application/x-snappy-framed",
        (_, Compression::Lz4) => "application/x-lz4",
        (_, Compression::Zstd) => "application/zstd",
        (ObjectFormat::Ndjson, Compression::None) => "application/x-ndjson",
        (ObjectFormat::Json, Compression::None) => "application/json",
        (ObjectFormat::Csv, Compression::None) => "text/csv; charset=utf-8",
        (ObjectFormat::Text { .. }, Compression::None) => "text/plain; charset=utf-8",
        (ObjectFormat::Bytes { .. }, Compression::None) => "application/octet-stream",
    }
}

/// The key of the object `id` under `prefix`.
fn key_of(prefix: &str, id: &str, extension: &str) -> String {
    format!("{prefix}{id}.{extension}")
}

/// An object store destination: the file destination writing `config`'s files into `store`, whose requests run on
/// `runtime`. `records` is what a Parquet, Avro or CSV destination writes, route by route (`CompiledTopology::records`); a row from
/// a route not in it is refused. The line formats write every route's rows alike and do not read it.
pub fn destination(
    config: &ObjectsDestinationConfig,
    records: Option<&Records>,
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<FileDestination, String> {
    let (format, part_of): (Arc<dyn FileFormat>, _) = match &config.format {
        format if format.writes_columns() => {
            let mut types: Vec<&DataType> = Vec::new();
            let mut part_of = HashMap::new();
            for (&route, record) in records.into_iter().flatten() {
                // Routes running one pipeline write one record, and share its files.
                let at = types.iter().position(|t| *t == record).unwrap_or_else(|| {
                    types.push(record);
                    types.len() - 1
                });
                part_of.insert(route, at);
            }
            let schemas = types.into_iter().map(Schema::new).collect::<Result<Vec<_>, _>>()?;
            let format: Arc<dyn FileFormat> = match config.format {
                ObjectFormat::Avro => Arc::new(AvroFormat::new(schemas, config.compression)?),
                ObjectFormat::Csv => Arc::new(CsvFormat::new(schemas, config.compression)),
                _ => Arc::new(ParquetFormat::new(schemas, config.compression)),
            };
            (format, Some(part_of))
        }
        lines => (Arc::new(LineFormat::new(lines.clone(), config.compression)), None),
    };
    let files = FileConfig {
        format,
        partitioner: Arc::new(ObjectsPartitioner {
            prefix: config.prefix.clone(),
            part_of,
        }),
        commit_interval: config.file_interval,
        max_file_bytes: config.max_object_bytes,
        buffer_bytes: config.buffer_bytes,
        stall_after: config.stall_after,
        attributes: attributes(
            content_type(&config.format, config.compression),
            config.storage_class.as_deref(),
        ),
        parts_in_flight: config.in_flight,
    };
    Ok(FileDestination::new(
        cloud_name(&config.cloud),
        files,
        store,
        runtime,
        Box::new(ObjectsCommitter {
            extension: extension(&config.format, config.compression),
        }),
    ))
}

/// A row's partition: the prefix its file goes under, and for Parquet, Avro and CSV its record's part.
struct ObjectsPartitioner {
    /// The configured prefix, before its placeholders are filled.
    prefix: String,
    /// Each route's record, as its index in the destination's schemas: routes writing one record share files. `None`
    /// for the line formats, whose files take every route's rows.
    part_of: Option<HashMap<Option<u32>, usize>>,
}

impl Partitioner for ObjectsPartitioner {
    fn partition(&self, row: &OwnedRecord) -> Result<Partition, String> {
        let prefix = prefix_of(&self.prefix, row)?;
        let part = match &self.part_of {
            None => 0,
            Some(part_of) => *part_of
                .get(&row.route)
                .ok_or("no route that writes here made it, so it has no columns here")?,
        };
        Ok(Partition {
            part,
            path: vec![prefix],
            values: Vec::new(),
        })
    }
}

/// Names every file `<prefix><id>.<extension>`, and has nothing to commit: a finished upload is the object.
struct ObjectsCommitter {
    extension: String,
}

impl Committer for ObjectsCommitter {
    fn file_path(&self, partition: &Partition, id: &str) -> String {
        key_of(&partition.path[0], id, &self.extension)
    }

    fn commit(&mut self, _files: &[DataFile]) -> Result<(), CommitError> {
        Ok(())
    }
}

/// A new object's id: `ID_BYTES` random bytes as lowercase hex.
pub fn object_id() -> String {
    crate::leader::auth::random_bytes(ID_BYTES)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A runtime of its own for a destination's or a source's client I/O, which its threads wait on.
pub fn runtime() -> Result<Arc<tokio::runtime::Runtime>, IoError> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("objects-io")
        .enable_all()
        .build()
        .map(Arc::new)
        .map_err(|e| IoError::transient(format!("object store: could not start its runtime: {e}")))
}

/// The bucket `config` names, signed with what it says to sign with. A credential this worker cannot obtain is an
/// error here, never a client that silently signs with something else.
///
/// Every cloud goes through the one [`Connector`], so the same TLS, and plain HTTP only to an address the bundle
/// names as http://.
pub fn store(
    config: &ObjectsDestinationConfig,
    secrets: &HeldSecrets,
    runtime: &tokio::runtime::Runtime,
) -> Result<Arc<dyn ObjectStore>, IoError> {
    let connector = Connector {
        tls: tls(config.identity.as_deref(), secrets)?,
    };
    let retry = object_store::RetryConfig {
        retry_timeout: config.stall_after,
        ..object_store::RetryConfig::default()
    };
    // Building does no I/O, but `object_store` expects to be inside a runtime when its client is made.
    let _entered = runtime.enter();
    match &config.cloud {
        Cloud::S3(s3) => self::s3(s3, connector, retry, secrets),
        Cloud::Gcs(gcs) => self::gcs(gcs, connector, retry, secrets),
        Cloud::Azure(azure) => self::azure(azure, connector, retry, secrets),
        // Over SSH, so no connector: load refuses an identity on it.
        Cloud::Sftp(sftp) => Ok(Arc::new(super::sftp::Sftp::new(sftp, "", false, secrets)?)),
    }
}

/// Whether `address` is plain HTTP.
fn plain(address: &Option<String>) -> bool {
    address
        .as_deref()
        .is_some_and(|a| a.to_ascii_lowercase().starts_with("http://"))
}

fn s3(
    config: &S3Store,
    connector: Connector,
    retry: object_store::RetryConfig,
    secrets: &HeldSecrets,
) -> Result<Arc<dyn ObjectStore>, IoError> {
    let mut builder = Aws::with_connector(&config.access, connector, retry, secrets)?
        .builder
        .with_bucket_name(&config.bucket);
    if let Some(key) = &config.kms_key_id {
        builder = builder.with_sse_kms_encryption(key);
    }
    let store = builder.build().map_err(|e| IoError::transient(format!("s3: {e}")))?;
    Ok(Arc::new(store))
}

/// The store for the S3 `bucket` a table's files are in, reached as `access` says, each call retried for up to
/// `stall_after`. `grant`, when given, signs every request in place of what `access` names.
pub(crate) fn s3_bucket(
    access: &S3Access,
    bucket: &str,
    grant: Option<CatalogGrant>,
    identity: Option<&Identity>,
    stall_after: Duration,
    secrets: &HeldSecrets,
    runtime: &tokio::runtime::Runtime,
) -> Result<Arc<dyn ObjectStore>, IoError> {
    let connector = Connector {
        tls: tls(identity, secrets)?,
    };
    let retry = object_store::RetryConfig {
        retry_timeout: stall_after,
        ..object_store::RetryConfig::default()
    };
    let _entered = runtime.enter();
    let mut builder = Aws::with_connector(access, connector, retry, secrets)?
        .builder
        .with_bucket_name(bucket);
    let (credentials, access_point) = match grant {
        Some(grant) => (Some(grant.credentials), grant.access_point),
        None => (None, None),
    };
    if let Some(credentials) = credentials {
        builder = builder.with_credentials(credentials);
    }
    if let Some(arn) = access_point {
        let (endpoint, region) =
            access_point_address(&arn).map_err(|e| IoError::transient(format!("s3 bucket {bucket}: {e}")))?;
        // The access point's address names the bucket, so a request's path holds only the key.
        builder = builder
            .with_region(region)
            .with_endpoint(&endpoint)
            .with_config(AmazonS3ConfigKey::S3Endpoint, endpoint)
            .with_virtual_hosted_style_request(true);
    }
    let store = builder
        .build()
        .map_err(|e| IoError::transient(format!("s3 bucket {bucket}: {e}")))?;
    Ok(Arc::new(store))
}

/// What a table's catalog vends for the table's storage.
pub(crate) struct CatalogGrant {
    pub credentials: AwsCredentialProvider,
    /// The ARN of the S3 access point the credentials are for, which every request then goes to.
    pub access_point: Option<String>,
}

/// AWS as one [`S3Access`] reaches it: a store for any bucket, and the signature for any other AWS call, all under
/// one credential provider, so an assumed role is assumed once for every bucket and the queue.
#[derive(Clone)]
pub struct Aws {
    /// Everything a bucket's store takes but the bucket.
    builder: AmazonS3Builder,
    region: String,
    credentials: AwsCredentialProvider,
    connector: Connector,
}

impl Aws {
    /// AWS as `access` and `identity` say to reach it. A credential this worker cannot obtain is an error here.
    pub fn new(
        access: &S3Access,
        identity: Option<&Identity>,
        secrets: &HeldSecrets,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Aws, IoError> {
        let connector = Connector {
            tls: tls(identity, secrets)?,
        };
        let _entered = runtime.enter();
        Aws::with_connector(access, connector, object_store::RetryConfig::default(), secrets)
    }

    fn with_connector(
        access: &S3Access,
        connector: Connector,
        retry: object_store::RetryConfig,
        secrets: &HeldSecrets,
    ) -> Result<Aws, IoError> {
        // With no access key named, the host's settings, credentials among them, and then what the bundle says over
        // them. With one, nothing of the host's but its region: a host's session token belongs to the host's key.
        let host = match &access.access_key {
            None => AmazonS3Builder::from_env(),
            Some(_) => match ["AWS_REGION", "AWS_DEFAULT_REGION"]
                .iter()
                .find_map(|v| std::env::var(v).ok())
            {
                Some(region) => AmazonS3Builder::new().with_region(region),
                None => AmazonS3Builder::new(),
            },
        };
        let mut builder = host
            .with_http_connector(connector.clone())
            .with_retry(retry)
            // The bundle's endpoint decides this; a host's allow-http setting must not.
            .with_allow_http(plain(&access.endpoint));
        if let Some(region) = &access.region {
            builder = builder.with_region(region);
        }
        let region = builder
            .get_config_value(&AmazonS3ConfigKey::Region)
            .unwrap_or_else(|| "us-east-1".to_string());
        // AWS's own addresses are in the region's partition; object_store assumes the commercial one. What the bundle
        // or the host names wins.
        let domain = aws_domain(&region);
        let sts = format!("https://sts.{region}.{domain}");
        if builder.get_config_value(&AmazonS3ConfigKey::StsEndpoint).is_none() {
            builder = builder.with_config(AmazonS3ConfigKey::StsEndpoint, &sts);
        }
        let named = [AmazonS3ConfigKey::Endpoint, AmazonS3ConfigKey::S3Endpoint];
        let endpoint = match &access.endpoint {
            Some(endpoint) => Some(endpoint.clone()),
            None if named.iter().all(|k| builder.get_config_value(k).is_none()) => {
                Some(format!("https://s3.{region}.{domain}"))
            }
            None => None,
        };
        if let Some(endpoint) = endpoint {
            // Over the host's S3-only endpoint variable too, which object_store would otherwise prefer.
            builder = builder
                .with_endpoint(&endpoint)
                .with_config(AmazonS3ConfigKey::S3Endpoint, endpoint);
        }
        if let Some(key) = &access.access_key {
            builder = builder
                .with_access_key_id(super::held(secrets, &key.id, "the destination", "access key id")?)
                .with_secret_access_key(super::held(
                    secrets,
                    &key.secret,
                    "the destination",
                    "secret access key",
                )?);
        }
        // What object_store finds for the settings above. Building needs a bucket, which credentials never depend on.
        let found = builder
            .clone()
            .with_bucket_name("smeltr")
            .build()
            .map_err(|e| IoError::transient(format!("s3: {e}")))?
            .credentials()
            .clone();
        let credentials: AwsCredentialProvider = match &access.role {
            None => found,
            Some(role) => {
                // The base credentials sign the AssumeRole call; everything else is signed with what it returns.
                // The bundle's, else the host's, else the partition's, as set on the builder above.
                let endpoint = access
                    .auth_endpoint
                    .clone()
                    .or_else(|| builder.get_config_value(&AmazonS3ConfigKey::StsEndpoint))
                    .unwrap_or(sts);
                let client = connector
                    .connect(&ClientOptions::new().with_allow_http(plain(&access.auth_endpoint)))
                    .map_err(|e| IoError::transient(format!("s3: {e}")))?;
                Arc::new(AssumeRole {
                    base: found,
                    client,
                    endpoint,
                    region: region.clone(),
                    role: role.clone(),
                    cached: tokio::sync::Mutex::new(None),
                })
            }
        };
        Ok(Aws {
            builder: builder.with_credentials(credentials.clone()),
            region,
            credentials,
            connector,
        })
    }

    /// The store for `bucket`. Call it inside the runtime its requests will run on.
    pub fn bucket(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>, IoError> {
        let store = self
            .builder
            .clone()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| IoError::transient(format!("s3 bucket {bucket}: {e}")))?;
        Ok(Arc::new(store))
    }

    /// The region every request is signed for.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Signs `request` for `service` with the current credentials.
    async fn sign(&self, request: &mut http::Request<HttpRequestBody>, service: &str) -> Result<(), String> {
        let credential = self.credentials.get_credential().await.map_err(|e| e.to_string())?;
        AwsAuthorizer::new(&credential, service, &self.region)
            .try_authorize(request, None)
            .map_err(|e| e.to_string())
    }

    /// This AWS signing for one `service`, such as `sqs`.
    pub fn service(self, service: impl Into<String>) -> AwsService {
        AwsService {
            aws: self,
            service: service.into(),
        }
    }
}

/// AWS bound to the one service a client calls, since an AWS signature names its service.
pub struct AwsService {
    aws: Aws,
    service: String,
}

impl Signing for AwsService {
    const CLOUD: &'static str = "aws";

    fn connector(&self) -> &Connector {
        &self.aws.connector
    }

    async fn sign(&self, request: &mut http::Request<HttpRequestBody>) -> Result<(), String> {
        self.aws.sign(request, &self.service).await
    }
}

/// What signs a client's calls to a cloud beside its stores: the connector the stores use, and a signature from the same
/// credentials.
pub trait Signing: Send + Sync {
    /// The cloud, as an error names it.
    const CLOUD: &'static str;

    /// What connects to the cloud: the stores' TLS and proxy.
    fn connector(&self) -> &Connector;

    /// Signs `request` with the current credentials.
    fn sign(&self, request: &mut http::Request<HttpRequestBody>) -> impl Future<Output = Result<(), String>> + Send;
}

/// What a call answered, at any status.
pub struct Answered {
    pub status: http::StatusCode,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

/// A cloud's calls from one client: each signed, bounded by a timeout, over the same TLS and proxy as the stores.
pub struct Signed<C> {
    cloud: C,
    /// The service, as an error names it.
    name: &'static str,
    client: HttpClient,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl<C: Signing> Signed<C> {
    /// Calls to `address` on `cloud`, run on `runtime`. Plain HTTP only if `address` is http://.
    pub fn new(
        cloud: C,
        name: &'static str,
        address: &str,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Signed<C>, IoError> {
        let client = cloud
            .connector()
            .connect(&ClientOptions::new().with_allow_http(plain(&Some(address.to_string()))))
            .map_err(|e| IoError::transient(format!("{}: {e}", C::CLOUD)))?;
        Ok(Signed {
            cloud,
            name,
            client,
            runtime,
        })
    }

    /// `request` with `body`, signed and sent: what it answered within `timeout`, or why there is none. `what` names
    /// the call in that reason.
    pub fn call(
        &self,
        request: http::request::Builder,
        body: HttpRequestBody,
        what: &str,
        timeout: Duration,
    ) -> Result<Answered, String> {
        let call = async {
            let mut request = request.body(body).map_err(|e| e.to_string())?;
            self.cloud.sign(&mut request).await?;
            let response = self.client.execute(request).await.map_err(|e| e.to_string())?;
            let status = response.status();
            let headers = response.headers().clone();
            let body = response.into_body().bytes().await.map_err(|e| e.to_string())?;
            Ok(Answered { status, headers, body })
        };
        self.runtime
            .block_on(async { tokio::time::timeout(timeout, call).await })
            .unwrap_or_else(|_| Err(format!("{} did not answer {what} within {timeout:?}", self.name)))
    }
}

fn gcs(
    config: &GcsStore,
    connector: Connector,
    retry: object_store::RetryConfig,
    secrets: &HeldSecrets,
) -> Result<Arc<dyn ObjectStore>, IoError> {
    Gcp::with_connector(&config.access, connector, retry, secrets)?.bucket(&config.bucket)
}

/// Google Cloud, reached and signed as its [`GcsAccess`] says: one credential for every bucket's store and for the
/// Pub/Sub calls a source makes.
#[derive(Clone)]
pub struct Gcp {
    /// Everything a bucket's store takes but the bucket.
    builder: GoogleCloudStorageBuilder,
    credentials: GcpCredentialProvider,
    connector: Connector,
    /// The domain Google's services are under: `googleapis.com`, or the sovereign universe the key names.
    universe: String,
}

impl Gcp {
    /// Google Cloud as `access` and `identity` say to reach it. A credential this worker cannot obtain is an error
    /// here.
    pub fn new(
        access: &GcsAccess,
        identity: Option<&Identity>,
        secrets: &HeldSecrets,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Gcp, IoError> {
        let connector = Connector {
            tls: tls(identity, secrets)?,
        };
        let _entered = runtime.enter();
        Gcp::with_connector(access, connector, object_store::RetryConfig::default(), secrets)
    }

    fn with_connector(
        access: &GcsAccess,
        connector: Connector,
        retry: object_store::RetryConfig,
        secrets: &HeldSecrets,
    ) -> Result<Gcp, IoError> {
        // As for S3: the host's settings only when the bundle names no key.
        let key = match &access.service_account_key {
            None => None,
            Some(key) => Some(super::held(secrets, key, "the destination", "service account key")?),
        };
        let universe = key
            .as_deref()
            .and_then(universe)
            .unwrap_or_else(|| GOOGLE_UNIVERSE.to_string());
        // The key's own `gcs_base_url`, which object_store reads, wins over the universe's address as the bundle's
        // endpoint does.
        let named = key.as_deref().and_then(|k| key_text(k, "gcs_base_url")).is_some();
        let mut builder = match key {
            None => GoogleCloudStorageBuilder::from_env(),
            Some(key) => GoogleCloudStorageBuilder::new().with_service_account_key(key),
        }
        .with_http_connector(connector.clone())
        .with_retry(retry)
        .with_client_options(ClientOptions::new().with_allow_http(plain(&access.endpoint)));
        // object_store knows only Google's own universe.
        let storage = access
            .endpoint
            .clone()
            .or_else(|| (universe != GOOGLE_UNIVERSE && !named).then(|| format!("https://storage.{universe}")));
        if let Some(storage) = &storage {
            builder = builder.with_base_url(storage);
        }
        // What object_store finds for the settings above. Building needs a bucket, which credentials never depend on.
        let credentials = builder
            .clone()
            .with_bucket_name("smeltr")
            .build()
            .map_err(|e| IoError::transient(format!("gcs: {e}")))?
            .credentials()
            .clone();
        Ok(Gcp {
            builder: builder.with_credentials(credentials.clone()),
            credentials,
            connector,
            universe,
        })
    }

    /// The address of Google's service `name`, such as `pubsub`, in the universe the key names.
    pub fn service(&self, name: &str) -> String {
        format!("https://{name}.{}", self.universe)
    }

    /// The store for `bucket`. Call it inside the runtime its requests will run on.
    pub fn bucket(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>, IoError> {
        let store = self
            .builder
            .clone()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| IoError::transient(format!("gcs bucket {bucket}: {e}")))?;
        Ok(Arc::new(store))
    }
}

impl Signing for Gcp {
    const CLOUD: &'static str = "gcp";

    fn connector(&self) -> &Connector {
        &self.connector
    }

    /// A service account key's token is scoped to `cloud-platform`, which covers Pub/Sub as well as Cloud Storage. The
    /// host's token carries whatever scopes the host was given.
    async fn sign(&self, request: &mut http::Request<HttpRequestBody>) -> Result<(), String> {
        let credential = self.credentials.get_credential().await.map_err(|e| e.to_string())?;
        // An emulator's key disables OAuth and yields no token, and an empty bearer is not a header Pub/Sub takes.
        if !credential.bearer.is_empty() {
            let value =
                http::HeaderValue::from_str(&format!("Bearer {}", credential.bearer)).map_err(|e| e.to_string())?;
            request.headers_mut().insert(http::header::AUTHORIZATION, value);
        }
        Ok(())
    }
}

/// The domain of Google's own universe, which a key that names none is in.
const GOOGLE_UNIVERSE: &str = "googleapis.com";

/// The universe a service account key names in its `universe_domain`, as a sovereign cloud's keys do. None for a key
/// that names none or names no host name; one that is no JSON is refused when the store is built.
fn universe(key: &str) -> Option<String> {
    let domain = key_text(key, "universe_domain")?;
    let domain = domain.trim().trim_matches('.').to_ascii_lowercase();
    let host = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '.';
    (!domain.is_empty() && domain.chars().all(host)).then_some(domain)
}

/// The text of `key`'s top-level member `field`. None when the key has none, or it is not text.
fn key_text(key: &str, field: &str) -> Option<String> {
    let mut key = Reader::of_str(key);
    if key.value().ok()? != Next::Object {
        return None;
    }
    loop {
        if key.key().ok()?? == field {
            return match key.value().ok()? {
                Next::Str(text) => Some(text.into_owned()),
                _ => None,
            };
        }
        key.skip().ok()?;
    }
}

fn azure(
    config: &AzureStore,
    connector: Connector,
    retry: object_store::RetryConfig,
    secrets: &HeldSecrets,
) -> Result<Arc<dyn ObjectStore>, IoError> {
    Azure::with_connector(&config.access, connector, retry, secrets)?.container(&config.container)
}

/// An Azure storage account, reached and signed as its [`AzureAccess`] says: one credential for every container's
/// store and for the queue calls a source makes.
#[derive(Clone)]
pub struct Azure {
    /// Everything a container's store takes but the container.
    builder: MicrosoftAzureBuilder,
    account: String,
    credentials: AzureCredentialProvider,
    connector: Connector,
}

impl Azure {
    /// The account as `access` and `identity` say to reach it. A credential this worker cannot obtain is an error
    /// here.
    pub fn new(
        access: &AzureAccess,
        identity: Option<&Identity>,
        secrets: &HeldSecrets,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Azure, IoError> {
        let connector = Connector {
            tls: tls(identity, secrets)?,
        };
        let _entered = runtime.enter();
        Azure::with_connector(access, connector, object_store::RetryConfig::default(), secrets)
    }

    fn with_connector(
        access: &AzureAccess,
        connector: Connector,
        retry: object_store::RetryConfig,
        secrets: &HeldSecrets,
    ) -> Result<Azure, IoError> {
        // As for S3: the host's settings only when the bundle names no credential.
        let mut builder = match &access.credential {
            None => MicrosoftAzureBuilder::from_env(),
            Some(AzureCredential::AccessKey(key)) => MicrosoftAzureBuilder::new().with_access_key(super::held(
                secrets,
                key,
                "the destination",
                "access key",
            )?),
            Some(AzureCredential::SasToken(token)) => {
                let token = super::held(secrets, token, "the destination", "SAS token")?;
                let pairs = object_store::azure::split_sas(&token)
                    .map_err(|e| IoError::transient(format!("azure blob: its SAS token: {e}")))?;
                MicrosoftAzureBuilder::new().with_sas_authorization(pairs)
            }
            // The host's metadata service alone, for that identity: none of the host's other settings.
            Some(AzureCredential::ManagedIdentity { client_id }) => {
                MicrosoftAzureBuilder::new().with_client_id(client_id)
            }
            Some(AzureCredential::ClientSecret {
                tenant_id,
                client_id,
                secret,
            }) => MicrosoftAzureBuilder::new().with_client_secret_authorization(
                client_id,
                super::held(secrets, secret, "the destination", "client secret")?,
                tenant_id,
            ),
        }
        .with_account(&access.account)
        .with_use_emulator(false)
        .with_http_connector(connector.clone())
        .with_retry(retry)
        // Every address the bundle names decides this, never the host's settings: a token exchange carries the
        // secret.
        .with_allow_http(plain(&access.endpoint) || plain(&access.auth_endpoint));
        if let Some(endpoint) = &access.endpoint {
            builder = builder.with_endpoint(endpoint.clone());
        }
        // Unnamed, Entra is the one in the cloud the account is in, unless the host's own settings name one.
        let cloud = access.endpoint.as_deref().map(crate::io::entra_authority);
        if let Some(host) = &access.auth_endpoint {
            builder = builder.with_authority_host(host);
        } else if let Some(authority) = cloud
            && builder.get_config_value(&AzureConfigKey::AuthorityHost).is_none()
        {
            builder = builder.with_authority_host(authority);
        }
        // What object_store finds for the settings above. Building needs a container, which credentials never
        // depend on.
        let credentials = builder
            .clone()
            .with_container_name("smeltr")
            .build()
            .map_err(|e| IoError::transient(format!("azure blob: {e}")))?
            .credentials()
            .clone();
        Ok(Azure {
            builder: builder.with_credentials(credentials.clone()),
            account: access.account.clone(),
            credentials,
            connector,
        })
    }

    /// The store for `container`. Call it inside the runtime its requests will run on.
    pub fn container(&self, container: &str) -> Result<Arc<dyn ObjectStore>, IoError> {
        let store = self
            .builder
            .clone()
            .with_container_name(container)
            .build()
            .map_err(|e| IoError::transient(format!("azure blob: {e}")))?;
        Ok(Arc::new(store))
    }
}

impl Signing for Azure {
    const CLOUD: &'static str = "azure";

    fn connector(&self) -> &Connector {
        &self.connector
    }

    /// Signs as a request to this account's storage.
    async fn sign(&self, request: &mut http::Request<HttpRequestBody>) -> Result<(), String> {
        let credential = self.credentials.get_credential().await.map_err(|e| e.to_string())?;
        AzureAuthorizer::new(&credential, &self.account)
            .try_authorize(request)
            .map_err(|e| e.to_string())
    }
}

/// The DNS domain of the AWS partition `region` is in. Any other name, such as an S3-compatible store's own region, is
/// taken as commercial AWS's, and needs its endpoint named.
/// The address and the region of the S3 access point `arn`, `arn:{partition}:s3:{region}:{account}:accesspoint/{name}`.
fn access_point_address(arn: &str) -> Result<(String, String), String> {
    let refused = || format!("{arn:?} is not an S3 access point's ARN");
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let [_, _, "s3", region, account, resource] = parts[..] else {
        return Err(refused());
    };
    let name = resource
        .strip_prefix("accesspoint/")
        .filter(|name| !name.is_empty() && !name.contains('/'))
        .ok_or_else(refused)?;
    // The ARN comes from the catalog's answer and becomes the host every request, and its credentials, go to.
    let lower = |s: &str| {
        s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    };
    let account_ok = account.len() == 12 && account.bytes().all(|b| b.is_ascii_digit());
    if parts[0] != "arn" || !account_ok {
        return Err(refused());
    }
    if region.is_empty() {
        return Err(format!(
            "the access point {arn:?} is multi-region, whose requests take a SigV4A signature, which this writer \
             does not make"
        ));
    }
    if !lower(name) || !lower(region) {
        return Err(refused());
    }
    let domain = aws_domain(region);
    Ok((
        format!("https://{name}-{account}.s3-accesspoint.{region}.{domain}"),
        region.to_string(),
    ))
}

fn aws_domain(region: &str) -> &'static str {
    const PARTITIONS: &[(&str, &str)] = &[
        ("cn-", "amazonaws.com.cn"),
        ("us-iso-", "c2s.ic.gov"),
        ("us-isob-", "sc2s.sgov.gov"),
        ("us-isof-", "csp.hci.ic.gov"),
        ("eu-isoe-", "cloud.adc-e.uk"),
        ("eusc-", "amazonaws.eu"),
    ];
    PARTITIONS
        .iter()
        .find(|(prefix, _)| region.starts_with(prefix))
        .map_or("amazonaws.com", |(_, domain)| domain)
}

/// The client the store and the AssumeRole call go through: this process's TLS, so its FIPS provider and the
/// identity's authorities, and the environment's proxy, which never covers loopback.
#[derive(Clone, Debug)]
pub struct Connector {
    tls: rustls::ClientConfig,
}

impl HttpConnector for Connector {
    /// Plain HTTP only where the caller allows it: the store and the AssumeRole call as the bundle says, which load
    /// allows, whatever address a host's settings name; the host's metadata credentials always.
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let allow_http = options.get_config_value(&ClientConfigKey::AllowHttp).as_deref() == Some("true");
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(self.tls.clone())
            .https_only(!allow_http)
            .build()
            .map_err(|e| object_store::Error::Generic {
                store: "object store",
                source: Box::new(e),
            })?;
        Ok(HttpClient::new(client))
    }
}

/// What the client presents and trusts (`io::trust`). Without an identity, no certificate.
pub(crate) fn tls(identity: Option<&Identity>, secrets: &HeldSecrets) -> Result<rustls::ClientConfig, IoError> {
    let builder = rustls::ClientConfig::builder_with_provider(crate::crypto::in_use())
        .with_safe_default_protocol_versions()
        .map_err(|e| IoError::transient(format!("object store: {e}")))?
        .with_root_certificates(super::trust::store(identity)?);
    let Some(identity) = identity else {
        return Ok(builder.with_no_client_auth());
    };
    let Some(presented) = super::trust::presented(identity, secrets)? else {
        return Ok(builder.with_no_client_auth());
    };
    builder
        .with_client_auth_cert(presented.chain, presented.key)
        .map_err(|e| IoError::transient(format!("the identity {:?}: its certificate and key: {e}", identity.id)))
}

/// How long before they expire storage credentials are renewed, an assumed role's or what a table's catalog vends: a
/// put signed just before expiry must still be valid when it lands, retries included.
pub(crate) const RENEW_BEFORE: Duration = Duration::from_secs(5 * 60);

/// Credentials for a role, assumed through STS with the base credentials and kept until shortly before they expire.
#[derive(Debug)]
struct AssumeRole {
    base: AwsCredentialProvider,
    client: HttpClient,
    /// STS's address.
    endpoint: String,
    region: String,
    role: AssumedRole,
    cached: tokio::sync::Mutex<Option<(Arc<AwsCredential>, SystemTime)>>,
}

#[async_trait::async_trait]
impl CredentialProvider for AssumeRole {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let mut cached = self.cached.lock().await;
        if let Some((credential, expires)) = &*cached
            && SystemTime::now() + RENEW_BEFORE < *expires
        {
            return Ok(credential.clone());
        }
        let (credential, expires) = self.assume().await.map_err(|why| object_store::Error::Generic {
            store: "S3",
            source: format!("assuming {}: {why}", self.role.arn).into(),
        })?;
        let credential = Arc::new(credential);
        *cached = Some((credential.clone(), expires));
        Ok(credential)
    }
}

impl AssumeRole {
    async fn assume(&self) -> Result<(AwsCredential, SystemTime), String> {
        let mut form = format!(
            "Action=AssumeRole&Version=2011-06-15&RoleSessionName=smeltr&DurationSeconds=3600&RoleArn={}",
            percent_encode(&self.role.arn)
        );
        if let Some(external_id) = &self.role.external_id {
            form.push_str(&format!("&ExternalId={}", percent_encode(external_id)));
        }
        let mut request = http::Request::builder()
            .method(http::Method::POST)
            .uri(&self.endpoint)
            .header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(HttpRequestBody::from(form))
            .map_err(|e| e.to_string())?;
        let base = self.base.get_credential().await.map_err(|e| e.to_string())?;
        AwsAuthorizer::new(&base, "sts", &self.region)
            .try_authorize(&mut request, None)
            .map_err(|e| e.to_string())?;
        let response = self.client.execute(request).await.map_err(|e| e.to_string())?;
        let status = response.status();
        let body = response.into_body().bytes().await.map_err(|e| e.to_string())?;
        let body = String::from_utf8_lossy(&body);
        if !status.is_success() {
            let start: String = body.chars().take(400).collect();
            return Err(format!("STS answered {status}: {start}"));
        }
        read_assumed(&body)
    }
}

/// The credentials and their expiry out of an `AssumeRoleResponse`.
fn read_assumed(xml: &str) -> Result<(AwsCredential, SystemTime), String> {
    let field = |name: &str| -> Result<String, String> {
        let open = format!("<{name}>");
        let close = format!("</{name}>");
        let start = xml.find(&open).ok_or_else(|| format!("STS's answer has no {name}"))? + open.len();
        let end = xml[start..]
            .find(&close)
            .ok_or_else(|| format!("STS's answer has no {name}"))?
            + start;
        Ok(xml[start..end].trim().to_string())
    };
    let expiration = field("Expiration")?;
    let micros = crate::ops::timeparse::parse_auto(&expiration)
        .ok_or_else(|| format!("STS's Expiration {expiration:?} is no time"))?;
    let expires = SystemTime::UNIX_EPOCH + Duration::from_micros(micros.max(0) as u64);
    Ok((
        AwsCredential {
            key_id: field("AccessKeyId")?,
            secret_key: field("SecretAccessKey")?,
            token: Some(field("SessionToken")?),
        },
        expires,
    ))
}

#[cfg(test)]
mod tests {
    use object_store::gcp::GoogleConfigKey;
    use serde_json::json;

    use super::*;
    use crate::plan::topology::SecretRef;

    #[test]
    fn a_prefix_fills_its_placeholders_from_the_write_time() {
        // 2026-10-04T15:07:09Z
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_791_126_429);
        assert_eq!(expand("logs/%Y/%m/%d/%H/%M/", at), "logs/2026/10/04/15/07/");
        assert_eq!(expand("plain/", at), "plain/");
    }

    #[test]
    fn a_prefix_is_refused_for_what_would_misplace_its_objects() {
        assert!(check_prefix("%Y/%m/%d/%H/").is_ok());
        assert!(check_prefix("").is_ok());
        assert!(check_prefix("logs-").is_ok());
        for bad in ["/logs/", "a//b/", "a/../b/", "%S/", "100%", &"x".repeat(1000)] {
            assert!(check_prefix(bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    fn a_region_finds_its_partitions_domain() {
        assert_eq!(aws_domain("us-east-1"), "amazonaws.com");
        assert_eq!(aws_domain("us-gov-west-1"), "amazonaws.com");
        assert_eq!(aws_domain("cn-north-1"), "amazonaws.com.cn");
        assert_eq!(aws_domain("us-isob-east-1"), "sc2s.sgov.gov");
        assert_eq!(aws_domain("minio"), "amazonaws.com");
    }

    #[test]
    fn an_access_point_is_reached_at_its_own_address() {
        assert_eq!(
            access_point_address("arn:aws:s3:us-west-2:123456789012:accesspoint/lake-writer"),
            Ok((
                "https://lake-writer-123456789012.s3-accesspoint.us-west-2.amazonaws.com".to_string(),
                "us-west-2".to_string()
            ))
        );
        assert_eq!(
            access_point_address("arn:aws-cn:s3:cn-north-1:123456789012:accesspoint/lake").map(|(at, _)| at),
            Ok("https://lake-123456789012.s3-accesspoint.cn-north-1.amazonaws.com.cn".to_string())
        );
        let multi_region = access_point_address("arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap");
        assert!(multi_region.unwrap_err().contains("SigV4A"));
        for bad in [
            "lake-writer",
            "arn:aws:s3:us-west-2:123456789012:bucket/lake",
            "arn:aws:s3:us-west-2::accesspoint/lake",
            "arn:aws:s3-outposts:us-west-2:123456789012:outpost/op-1/accesspoint/lake",
            "arn:aws:s3:us-west-2:123456789012:accesspoint/",
            "arn:aws:s3:us-west-2:123456789012:accesspoint/evil.example#",
            "arn:aws:s3:us-west-2?:123456789012:accesspoint/lake",
        ] {
            assert!(access_point_address(bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    fn every_key_is_new() {
        let key = || key_of("p/", &object_id(), "ndjson.gz");
        let (one, two) = (key(), key());
        assert_ne!(one, two);
        assert!(one.starts_with("p/") && one.ends_with(".ndjson.gz"), "{one}");
        assert_eq!(one.len(), "p/".len() + 2 * ID_BYTES + ".ndjson.gz".len());
    }

    #[test]
    fn sts_answers_are_read() {
        let xml = "<AssumeRoleResponse><AssumeRoleResult><Credentials>\
            <AccessKeyId>ASIA</AccessKeyId><SecretAccessKey>s/k+</SecretAccessKey>\
            <SessionToken>tok</SessionToken><Expiration>2026-10-04T16:00:00Z</Expiration>\
            </Credentials></AssumeRoleResult></AssumeRoleResponse>";
        let (credential, expires) = read_assumed(xml).unwrap();
        assert_eq!(
            (credential.key_id.as_str(), credential.secret_key.as_str()),
            ("ASIA", "s/k+")
        );
        assert_eq!(credential.token.as_deref(), Some("tok"));
        assert_eq!(expires, SystemTime::UNIX_EPOCH + Duration::from_secs(1_791_129_600));
        assert!(read_assumed("<Error>denied</Error>").is_err());
    }

    /// Secrets that hold `value` as the leader-stored `secret`, a reference to it, and a client for the store.
    fn holding(value: &str) -> (HeldSecrets, SecretRef, Connector) {
        let mut secrets = HeldSecrets::default();
        secrets.set_stored([("secret".to_string(), 1, value.to_string())]);
        let secret = SecretRef::LeaderStored {
            name: "secret".to_string(),
            version: 1,
        };
        let connector = Connector {
            tls: tls(None, &secrets).unwrap(),
        };
        (secrets, secret, connector)
    }

    /// A service principal signs in in the cloud its account is in, unless the bundle names where.
    #[test]
    fn an_azure_account_signs_in_in_its_own_cloud() {
        let runtime = runtime().unwrap();
        let _entered = runtime.enter();
        let authority = |endpoint: Option<&str>, auth_endpoint: Option<&str>| {
            let (secrets, secret, connector) = holding("s");
            let access = AzureAccess {
                account: "acme".to_string(),
                endpoint: endpoint.map(str::to_string),
                auth_endpoint: auth_endpoint.map(str::to_string),
                credential: Some(AzureCredential::ClientSecret {
                    tenant_id: "t".to_string(),
                    client_id: "c".to_string(),
                    secret,
                }),
            };
            let azure = Azure::with_connector(&access, connector, object_store::RetryConfig::default(), &secrets);
            azure.unwrap().builder.get_config_value(&AzureConfigKey::AuthorityHost)
        };
        let gov = Some("https://acme.blob.core.usgovcloudapi.net");
        assert_eq!(
            authority(gov, None).as_deref(),
            Some("https://login.microsoftonline.us")
        );
        assert_eq!(
            authority(Some("https://acme.blob.core.chinacloudapi.cn/"), None).as_deref(),
            Some("https://login.chinacloudapi.cn")
        );
        assert_eq!(
            authority(gov, Some("https://login.example")).as_deref(),
            Some("https://login.example")
        );
        assert_eq!(
            authority(Some("https://acme.blob.core.windows.net"), None).as_deref(),
            Some("https://login.microsoftonline.com")
        );
    }

    /// A key from a sovereign universe reaches Google's services in that universe; an address the bundle or the key
    /// names wins.
    #[test]
    fn a_keys_universe_names_googles_addresses() {
        let runtime = runtime().unwrap();
        let _entered = runtime.enter();
        let gcp = |universe: Option<&str>, endpoint: Option<&str>, base: Option<&str>| {
            let mut key = json!({"private_key": "", "private_key_id": "", "client_email": "", "disable_oauth": true});
            if let Some(universe) = universe {
                key["universe_domain"] = json!(universe);
            }
            if let Some(base) = base {
                key["gcs_base_url"] = json!(base);
            }
            let (secrets, secret, connector) = holding(&key.to_string());
            let access = GcsAccess {
                endpoint: endpoint.map(str::to_string),
                service_account_key: Some(secret),
            };
            let gcp = Gcp::with_connector(&access, connector, object_store::RetryConfig::default(), &secrets).unwrap();
            let storage = gcp.builder.get_config_value(&GoogleConfigKey::BaseUrl);
            (gcp.service("pubsub"), storage)
        };
        assert_eq!(
            gcp(None, None, None),
            ("https://pubsub.googleapis.com".to_string(), None)
        );
        assert_eq!(gcp(Some("googleapis.com"), None, None).1, None);
        assert_eq!(gcp(Some("evil.example/x?"), None, None).1, None);
        // The key's own address is object_store's to take, as before.
        assert_eq!(gcp(Some("s3nsapis.fr"), None, Some("https://gcs.example")).1, None);
        assert_eq!(
            gcp(Some(" S3nsAPIs.fr. "), None, None),
            (
                "https://pubsub.s3nsapis.fr".to_string(),
                Some("https://storage.s3nsapis.fr".to_string())
            )
        );
        assert_eq!(
            gcp(Some("s3nsapis.fr"), Some("https://gcs.example"), None).1,
            Some("https://gcs.example".to_string())
        );
    }

    /// A client speaks plain HTTP only where its caller allows it, whatever address it is handed.
    #[test]
    fn plain_http_needs_a_caller_that_allows_it() {
        let runtime = runtime().unwrap();
        let get = |allow_http| {
            let client = Connector {
                tls: tls(None, &HeldSecrets::default()).unwrap(),
            }
            .connect(&ClientOptions::new().with_allow_http(allow_http))
            .unwrap();
            let request = http::Request::get("http://127.0.0.1:1/")
                .body(HttpRequestBody::empty())
                .unwrap();
            format!("{:?}", runtime.block_on(client.execute(request)).unwrap_err())
        };
        let refused = get(false);
        assert!(refused.contains("BadScheme"), "{refused}");
        let tried = get(true);
        assert!(!tried.contains("BadScheme"), "{tried}");
    }
}
