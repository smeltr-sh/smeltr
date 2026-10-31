//! The Iceberg destination: the file destination writing the table's Parquet files, and the committer that records
//! them as the table's next snapshot.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant D as destination start
//!   participant W as writer thread
//!   participant S as object store
//!   participant C as catalog
//!   D->>C: load the table, or create it from the record
//!   D->>C: add the record's new columns, before any file
//!   W->>S: data files, under data/
//!   W->>S: manifest, the parent's small manifests merged, manifest list
//!   W->>C: add-snapshot, asserting main is still the parent
//!   C-->>W: committed, 409: reload and commit again, or lost: look for it before the next commit
//! ```
//!
//! Only tables in S3, or a store that speaks its API, are written. Storage is signed with what the destination names,
//! else with what the catalog vends, renewed before it expires, else with the host's credentials.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::aws::{AwsCredential, AwsCredentialProvider};
use object_store::client::HttpRequestBody;
use object_store::path::Path;
use object_store::{Attributes, ObjectStore, ObjectStoreExt, PutPayload};

use super::catalog::{Auth, Catalog, CommitFailure, Loaded, Requirement, Signer, StorageCredential, Update};
use super::manifest::{self, DataFileEntry, ManifestFile};
use super::metadata::{
    FIRST_PARTITION_ID, FORMAT_VERSION, MAIN, NestedField, PartitionField, PartitionSpec, Primitive,
    ROW_LINEAGE_VERSION, Schema, Snapshot, SnapshotRef, TableMetadata, Transform, Type,
};
use super::schema::{table_schema, with_field_ids};
use super::transform::{TablePartitioner, literal_of, partition_name, partition_term, result_type};
use crate::core::column::DataType;
use crate::io::IoError;
use crate::io::files::{
    CommitError, Committer, DataFile, FileConfig, FileDestination, Lost, MAX_CONFLICT_RETRIES, ParquetFormat,
    Partition, wait,
};
use crate::io::objects::{self, RENEW_BEFORE, Signing};
use crate::io::parquet;
use crate::io::table::{ROW_GROUP_BYTES, one_record};
use crate::io::vended::Vended;
use crate::plan::topology::{CatalogSignIn, Compression, HeldSecrets, IcebergDestinationConfig, Records, S3Access};

/// The schemes of an S3 location.
const S3_SCHEMES: [&str; 3] = ["s3", "s3a", "s3n"];

/// The keys a catalog vends S3 credentials under (Iceberg's S3FileIOProperties).
const ACCESS_KEY_ID: &str = "s3.access-key-id";
const SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
const SESSION_TOKEN: &str = "s3.session-token";
const EXPIRES_AT_MS: &str = "s3.session-token-expires-at-ms";

/// How long vended credentials that name no expiry are kept before they are asked for again: an STS session's
/// shortest life is 15 minutes.
const UNDATED_LIFE: Duration = Duration::from_secs(15 * 60);

/// The destination `config` names. Its table is loaded, or created from the record when missing, and the record's
/// new columns are added to it before any file is written. Err is transient: the catalog cannot be reached, or the
/// table cannot take the record, which a restart asks again.
pub fn destination(
    config: &IcebergDestinationConfig,
    records: Option<&Records>,
    secrets: &HeldSecrets,
) -> Result<FileDestination, IoError> {
    let refused = |why: String| IoError::transient(format!("iceberg table {}: {why}", config.table));
    let record = one_record(records).map_err(refused)?;
    let runtime = objects::runtime()?;
    let mut catalog = catalog(config, secrets, &runtime)?;
    let (loaded, schema) = open(&mut catalog, config, record)?;
    let location = Location::parse(&loaded.metadata.location).map_err(refused)?;
    let (data, meta) = location.places(&loaded.metadata).map_err(refused)?;
    let (access, credentials) = storage(
        config,
        &loaded.config,
        &loaded.storage_credentials,
        &data,
        &meta,
        secrets,
        &runtime,
    )?;
    let store = objects::s3_bucket(
        &access,
        &location.bucket,
        credentials.map(|credentials| objects::CatalogGrant {
            credentials,
            access_point: None,
        }),
        config.tls.as_ref(),
        config.stall_after,
        secrets,
        &runtime,
    )?;
    assemble(config, record, catalog, loaded.metadata, schema, store, runtime)
}

/// The destination writing `record` into the opened table through `catalog`, its files into `store`.
fn assemble(
    config: &IcebergDestinationConfig,
    record: &DataType,
    catalog: Catalog,
    metadata: TableMetadata,
    schema: Schema,
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<FileDestination, IoError> {
    let refused = |why: String| IoError::transient(format!("iceberg table {}: {why}", config.table));
    let committer =
        IcebergCommitter::new(catalog, config, metadata, schema, store.clone(), runtime.clone()).map_err(refused)?;
    let partitioner = TablePartitioner::new(&committer.spec, &committer.schema).map_err(refused)?;
    let file_schema = parquet::Schema::new(record).map_err(refused)?;
    let written = with_field_ids(file_schema.arrow(), &committer.schema).map_err(refused)?;
    let settings = WriteSettings::of(&committer.metadata.properties, config);
    let format = ParquetFormat::new(vec![file_schema.with_written(written)], settings.options);
    let files = FileConfig {
        format: Arc::new(format),
        partitioner: Arc::new(partitioner),
        commit_interval: config.file_interval,
        max_file_bytes: settings.file_bytes,
        take_bytes: settings.row_group_bytes,
        buffer_bytes: config.buffer_bytes,
        stall_after: config.stall_after,
        attributes: Attributes::new(),
        parts_in_flight: config.in_flight,
    };
    Ok(FileDestination::new(
        "iceberg",
        files,
        store,
        runtime,
        Box::new(committer),
    ))
}

/// A client of the catalog `config` names, signed in as it says. An AWS signature is made on `runtime`.
fn catalog(
    config: &IcebergDestinationConfig,
    secrets: &HeldSecrets,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> Result<Catalog, IoError> {
    let auth = match &config.sign_in {
        CatalogSignIn::None => Auth::None,
        CatalogSignIn::Bearer(token) => {
            Auth::Bearer(crate::io::held(secrets, token, "the iceberg destination", "token")?)
        }
        CatalogSignIn::ClientCredentials {
            token_url,
            client_id,
            client_secret,
            scope,
        } => Auth::ClientCredentials {
            token_url: token_url.clone(),
            client_id: client_id.clone(),
            client_secret: crate::io::held(secrets, client_secret, "the iceberg destination", "client secret")?,
            scope: scope.clone(),
        },
        CatalogSignIn::SigV4 { signing_name } => {
            let mut access = config.storage.clone().unwrap_or_default();
            // A signature for another region than the catalog's is refused, whatever region storage is in.
            if let Some(region) = signing_region(&config.catalog) {
                access.region = Some(region);
            }
            Auth::Signed(Arc::new(AwsSigner {
                service: objects::Aws::new(&access, config.tls.as_ref(), secrets, runtime)?.service(signing_name),
                runtime: runtime.clone(),
                within: config.stall_after / 2,
            }))
        }
    };
    let agent = crate::io::http::agent_config(config.tls.as_ref(), secrets)?.new_agent();
    Ok(Catalog::new(
        config.catalog.clone(),
        config.warehouse.clone(),
        auth,
        agent,
        // A call that times out leaves room in the commit's budget to try again, and to look for a lost one.
        config.stall_after / 2,
    ))
}

/// The region an AWS service's own address names: `eu-west-1` for `https://glue.eu-west-1.amazonaws.com/iceberg`,
/// `us-gov-west-1` for `glue-fips.us-gov-west-1.amazonaws.com`. `None` for any other address.
pub(crate) fn signing_region(url: &str) -> Option<String> {
    let host = reqwest::Url::parse(url.trim()).ok()?.host_str()?.to_ascii_lowercase();
    let labels: Vec<&str> = host.split('.').collect();
    let aws = labels.iter().position(|l| *l == "amazonaws")?;
    let region = labels.get(aws.checked_sub(1)?)?;
    // A region is letters, then a number: `us-east-1`, `us-gov-west-1`.
    (aws >= 2 && region.contains('-') && region.ends_with(|c: char| c.is_ascii_digit())).then(|| region.to_string())
}

/// Signs catalog calls as AWS signs calls to its own services, `glue` or `s3tables`.
struct AwsSigner {
    service: objects::AwsService,
    runtime: Arc<tokio::runtime::Runtime>,
    /// How long a signature may take, credentials fetched from the host or STS included: a catalog call's budget.
    within: Duration,
}

impl Signer for AwsSigner {
    fn sign(&self, method: &str, url: &str, body: &[u8]) -> Result<Vec<(String, String)>, String> {
        let mut request = http::Request::builder()
            .method(method)
            .uri(url)
            .body(HttpRequestBody::from(body.to_vec()))
            .map_err(|e| e.to_string())?;
        // Called from the writer's thread and from a renewal on the runtime's blocking pool, never from a task.
        self.runtime
            .handle()
            // The timer is made on the runtime, which its clock belongs to.
            .block_on(async { tokio::time::timeout(self.within, self.service.sign(&mut request)).await })
            .map_err(|_| format!("no AWS credentials to sign with after {:?}", self.within))??;
        Ok(request
            .headers()
            .iter()
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect())
    }
}

/// The table, created from `record` when missing, with `record`'s new columns added, and its schema for `record`.
fn open(
    catalog: &mut Catalog,
    config: &IcebergDestinationConfig,
    record: &DataType,
) -> Result<(Loaded, Schema), IoError> {
    let loaded = match catalog.load_table(&config.namespace, &config.table)? {
        Some(loaded) => loaded,
        None => create(catalog, config, record)?,
    };
    let table = &loaded.metadata;
    let refused = |why: String| IoError::transient(format!("iceberg table {}: {why}", config.table));
    // A version 1 table upgrades in place, no file rewritten.
    if table.format_version == 1 {
        return Err(refused(format!(
            "it is of format version 1, and versions {FORMAT_VERSION} and {ROW_LINEAGE_VERSION} are written; upgrade \
             it in place in Spark with ALTER TABLE {}.{} SET TBLPROPERTIES ('format-version'='{FORMAT_VERSION}')",
            config.namespace.join("."),
            config.table
        )));
    }
    if !(FORMAT_VERSION..=ROW_LINEAGE_VERSION).contains(&table.format_version) {
        return Err(refused(format!(
            "it is of format version {}, and versions {FORMAT_VERSION} and {ROW_LINEAGE_VERSION} are written",
            table.format_version
        )));
    }
    // An encrypted table's manifests carry key metadata, which this writer neither makes nor keeps.
    let keys = table.other.get("encryption-keys").and_then(|k| k.as_array());
    if table.properties.contains_key("encryption.key-id") || keys.is_some_and(|k| !k.is_empty()) {
        return Err(refused("it is encrypted, and an encrypted table is not written".into()));
    }
    // A version 3 transform of several columns has no source column of its own to partition by.
    let spec = table.spec().into_iter().flat_map(|s| &s.fields);
    if let Some(field) = spec
        .into_iter()
        .find(|f| f.source_ids.as_ref().is_some_and(|ids| ids.len() > 1))
    {
        return Err(refused(format!(
            "its partition field {} is a transform of several columns, and only a transform of one column is written",
            field.name
        )));
    }
    evolve(catalog, config, record, loaded)
}

/// How the table asks its data files written, as its `write.*` properties say; what they leave unsaid is the
/// destination's config, or Iceberg's own default where the config has no say.
#[derive(Debug, PartialEq)]
struct WriteSettings {
    options: parquet::WriteOptions,
    row_group_bytes: usize,
    file_bytes: usize,
}

/// Iceberg's `write.parquet.bloom-filter-fpp.column.*` where a table sets none.
const BLOOM_FILTER_FPP: f64 = 0.01;

const CODEC: &str = "write.parquet.compression-codec";
const BLOOM_FILTER: &str = "write.parquet.bloom-filter-enabled.column.";

impl WriteSettings {
    fn of(properties: &BTreeMap<String, String>, config: &IcebergDestinationConfig) -> WriteSettings {
        let property = |key: &str| properties.get(key).map(String::as_str);
        let bytes = |key: &str| property(key).and_then(|v| v.parse::<usize>().ok()).filter(|&n| n > 0);
        let codec = match property(CODEC).map(str::to_ascii_lowercase).as_deref() {
            None => config.compression,
            Some(name) => codec(name).unwrap_or_else(|| {
                tracing::warn!(target: "iceberg.write", table = config.table, codec = name,
                    "the table asks for a codec this writer has not, and its files take the destination's");
                config.compression
            }),
        };
        let bloom_filters = (properties.iter())
            .filter_map(|(key, value)| Some((key.strip_prefix(BLOOM_FILTER)?, value)))
            .filter(|(_, enabled)| enabled.eq_ignore_ascii_case("true"))
            .map(|(column, _)| {
                let fpp = property(&format!("write.parquet.bloom-filter-fpp.column.{column}"))
                    .and_then(|v| v.parse::<f64>().ok())
                    .filter(|p| *p > 0.0 && *p < 1.0)
                    .unwrap_or(BLOOM_FILTER_FPP);
                (column.split('.').map(str::to_string).collect(), fpp)
            })
            .collect();
        WriteSettings {
            options: parquet::WriteOptions {
                codec,
                level: property("write.parquet.compression-level").and_then(|v| v.parse().ok()),
                bloom_filters,
            },
            row_group_bytes: bytes("write.parquet.row-group-size-bytes").unwrap_or(ROW_GROUP_BYTES),
            file_bytes: bytes("write.target-file-size-bytes").unwrap_or(config.max_object_bytes),
        }
    }
}

/// The name Iceberg gives `codec`: librdkafka's, but for none.
fn codec_name(codec: Compression) -> &'static str {
    match codec {
        Compression::None => "uncompressed",
        codec => codec.as_property(),
    }
}

/// The codec Iceberg names `name`, where this writer has it.
fn codec(name: &str) -> Option<Compression> {
    use Compression as C;
    [C::None, C::Gzip, C::Snappy, C::Lz4, C::Zstd]
        .into_iter()
        .find(|c| codec_name(*c) == name || c.as_property() == name)
}

/// Creates the table with the record as its schema and `partitionBy` as its spec. A table another writer created
/// first is loaded instead.
fn create(catalog: &mut Catalog, config: &IcebergDestinationConfig, record: &DataType) -> Result<Loaded, IoError> {
    let refused = |why: String| IoError::transient(format!("creating the iceberg table {}: {why}", config.table));
    let fresh = table_schema(record, None, 0).map_err(refused)?;
    let spec = spec_of(&config.partition_by, &fresh.schema).map_err(refused)?;
    let properties = BTreeMap::from([
        ("format-version".to_string(), "2".to_string()),
        ("write.format.default".to_string(), "parquet".to_string()),
        // So that every writer after this one writes as it does.
        (CODEC.to_string(), codec_name(config.compression).to_string()),
    ]);
    catalog.create_table(&config.namespace, &config.table, &fresh.schema, &spec, &properties)
}

/// The partition spec `terms` make over `schema`, its fields numbered from [`FIRST_PARTITION_ID`]. Err when a term
/// names no primitive column of `schema`, or its transform cannot be applied to that column.
fn spec_of(terms: &[String], schema: &Schema) -> Result<PartitionSpec, String> {
    let fields = terms
        .iter()
        .zip(FIRST_PARTITION_ID..)
        .map(|(term, field_id)| {
            let (transform, path) = partition_term(term)?;
            let source = column_id(&schema.fields, &path)
                .ok_or_else(|| format!("partitionBy names {}, which the record lacks", path.join(".")))?;
            let Some(Type::Primitive(source_type)) = schema.field(source).map(|f| &f.field_type) else {
                return Err(format!(
                    "partitionBy term {term:?}: {} is no primitive column",
                    path.join(".")
                ));
            };
            result_type(&transform, source_type).map_err(|e| format!("partitionBy term {term:?}: {e}"))?;
            Ok(PartitionField {
                name: partition_name(&transform, &path),
                transform,
                source_id: source,
                source_ids: None,
                field_id,
            })
        })
        .collect::<Result<_, String>>()?;
    Ok(PartitionSpec { spec_id: 0, fields })
}

/// The id of the column at `path` among `fields`, through structs.
fn column_id(fields: &[NestedField], path: &[String]) -> Option<i32> {
    let (first, rest) = path.split_first()?;
    let field = fields.iter().find(|f| &f.name == first)?;
    match (rest.is_empty(), &field.field_type) {
        (true, _) => Some(field.id),
        (false, Type::Struct(s)) => column_id(&s.fields, rest),
        (false, _) => None,
    }
}

/// Adds the record's new columns to the table, so every file is written with ids the table holds. Gives the table and
/// its schema for the record. Err when the table holds a column as another type than the record, or keeps changing.
fn evolve(
    catalog: &mut Catalog,
    config: &IcebergDestinationConfig,
    record: &DataType,
    mut loaded: Loaded,
) -> Result<(Loaded, Schema), IoError> {
    let refused = |why: String| IoError::transient(format!("iceberg table {}: {why}", config.table));
    for _ in 0..=MAX_CONFLICT_RETRIES {
        let table = &loaded.metadata;
        let current = table.schema();
        let mut evolution = table_schema(record, current, table.last_column_id).map_err(refused)?;
        if !evolution.changed {
            return Ok((loaded, evolution.schema));
        }
        // Ids of schemas the table no longer uses stay taken.
        evolution.schema.schema_id = table.schemas.iter().map(|s| s.schema_id).max().map_or(0, |id| id + 1);
        let mut requirements = vec![
            Requirement::AssertTableUuid(table.table_uuid.clone()),
            Requirement::AssertLastAssignedFieldId(table.last_column_id),
        ];
        if let Some(current) = current {
            requirements.push(Requirement::AssertCurrentSchemaId(current.schema_id));
        }
        let updates = [
            Update::AddSchema {
                schema: evolution.schema,
                last_column_id: evolution.last_column_id,
            },
            Update::SetCurrentSchema(-1),
        ];
        match catalog.commit(&config.namespace, &config.table, &requirements, &updates) {
            Ok(committed) => loaded.metadata = committed.metadata,
            // Another writer changed the table, or the change's answer was lost: the table as it is now says which.
            Err(CommitFailure::Conflict | CommitFailure::Unknown(_)) => {
                loaded = catalog
                    .load_table(&config.namespace, &config.table)?
                    .ok_or_else(|| refused("the table was dropped while its columns were added".into()))?;
            }
            Err(CommitFailure::Failed(why)) => return Err(refused(format!("adding the record's columns: {why}"))),
        }
    }
    Err(refused(format!(
        "its schema changed under {} attempts to add the record's columns",
        MAX_CONFLICT_RETRIES + 1
    )))
}

/// The type of each partition field's values, and its transform, in the spec's order. A `void` field's source may
/// be a column since dropped from `schema`, whose type is then taken from the table's `earlier` schemas.
fn partition_types(
    spec: &PartitionSpec,
    schema: &Schema,
    earlier: &[Schema],
) -> Result<Vec<(Primitive, Transform)>, String> {
    spec.fields
        .iter()
        .map(|field| {
            let mut source = schema.field(field.source_id);
            // A void field never reads its source, so dropping that column leaves the field standing.
            if source.is_none() && field.transform == Transform::Void {
                source = earlier.iter().find_map(|s| s.field(field.source_id));
            }
            match source.map(|f| &f.field_type) {
                Some(Type::Primitive(source)) => Ok((result_type(&field.transform, source)?, field.transform.clone())),
                _ => Err(format!("the partition field {} has no primitive source", field.name)),
            }
        })
        .collect()
}

/// Where the table's storage is and what signs for it: what the destination names wins, then what the catalog says
/// for the table, then the host's. Err when the catalog vends credentials but none holds both `data`, where data
/// files go, and `meta`, where manifests go.
fn storage(
    config: &IcebergDestinationConfig,
    table_config: &BTreeMap<String, String>,
    vended: &[StorageCredential],
    data: &Location,
    meta: &Location,
    secrets: &HeldSecrets,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> Result<(S3Access, Option<AwsCredentialProvider>), IoError> {
    let mut access = config.storage.clone().unwrap_or(S3Access {
        region: None,
        endpoint: None,
        auth_endpoint: None,
        access_key: None,
        role: None,
    });
    if access.endpoint.is_none()
        && let Some(endpoint) = table_config.get("s3.endpoint")
    {
        // The catalog names this endpoint, not the admin, so plain HTTP off this host was never chosen.
        if crate::config_file::plain_off_this_host(endpoint) {
            return Err(IoError::transient(format!(
                "the catalog places the table's storage at {endpoint}, plain HTTP off this host"
            )));
        }
        access.endpoint = Some(endpoint.clone());
    }
    if access.region.is_none() {
        // A table is in its catalog's region unless the catalog says otherwise.
        access.region = ["client.region", "s3.region"]
            .iter()
            .find_map(|k| table_config.get(*k).cloned())
            .or_else(|| signing_region(&config.catalog));
    }
    if access.access_key.is_some() || access.role.is_some() {
        return Ok((access, None));
    }
    let (data, meta) = (data.uri(""), meta.uri(""));
    let Some(first) = vended_credential(vended, table_config, &data, &meta)
        .map_err(|why| IoError::transient(format!("iceberg table {}: {why}", config.table)))?
    else {
        return Ok((access, None));
    };
    // The renewing catalog client is its own: a put may need fresh credentials while the committer waits on the
    // catalog.
    let catalog = Mutex::new(catalog(config, secrets, runtime)?);
    let (namespace, table) = (config.namespace.clone(), config.table.clone());
    let renew = move || {
        let mut catalog = catalog.lock().unwrap_or_else(PoisonError::into_inner);
        let found = match catalog.credentials(&namespace, &table).map_err(|e| e.to_string())? {
            Some(vended) if !vended.is_empty() => vended_credential(&vended, &BTreeMap::new(), &data, &meta)?,
            _ => {
                let loaded = catalog
                    .load_table(&namespace, &table)
                    .map_err(|e| e.to_string())?
                    .ok_or("the table is gone")?;
                vended_credential(&loaded.storage_credentials, &loaded.config, &data, &meta)?
            }
        };
        found.ok_or_else(|| "the catalog vended no credentials for the table's storage".to_string())
    };
    let provider = Vended::new("the catalog".to_string(), first, renew);
    Ok((access, Some(Arc::new(provider))))
}

/// The credentials a catalog vended for the table's `data` and `meta` locations, and when they are to be renewed by:
/// those whose prefix is the longest that holds both, whichever of `s3`, `s3a` and `s3n` either names, else the
/// table's config when it vended none. `None` when it vended no keys. Err when it vended credentials and none holds
/// both.
fn vended_credential(
    vended: &[StorageCredential],
    table_config: &BTreeMap<String, String>,
    data: &str,
    meta: &str,
) -> Result<Option<(Arc<AwsCredential>, SystemTime)>, String> {
    let within = |location: &str| format!("{}/", s3_scheme(location).trim_end_matches('/'));
    let (data_within, meta_within) = (within(data), within(meta));
    let holding = vended
        .iter()
        .filter(|c| {
            let prefix = s3_scheme(&c.prefix);
            data_within.starts_with(&prefix) && meta_within.starts_with(&prefix)
        })
        .max_by_key(|c| c.prefix.len());
    let properties = match holding {
        Some(c) => &c.config,
        None if vended.is_empty() => table_config,
        None => {
            return Err(format!(
                "the catalog vended no single credential for both the table's data location {data} and its metadata \
                 location {meta}"
            ));
        }
    };
    let expires = properties
        .get(EXPIRES_AT_MS)
        .and_then(|ms| ms.parse::<u64>().ok())
        .map(|ms| UNIX_EPOCH + Duration::from_millis(ms))
        .unwrap_or_else(|| SystemTime::now() + RENEW_BEFORE + UNDATED_LIFE);
    let (Some(key_id), Some(secret_key)) = (properties.get(ACCESS_KEY_ID), properties.get(SECRET_ACCESS_KEY)) else {
        return Ok(None);
    };
    Ok(Some((
        Arc::new(AwsCredential {
            key_id: key_id.clone(),
            secret_key: secret_key.clone(),
            token: properties.get(SESSION_TOKEN).cloned(),
        }),
        expires,
    )))
}

/// `uri` with an S3 scheme written `s3`.
fn s3_scheme(uri: &str) -> String {
    match uri.split_once("://") {
        Some((scheme, rest)) if S3_SCHEMES.contains(&scheme) => format!("s3://{rest}"),
        _ => uri.to_string(),
    }
}

/// An S3 location: its bucket, and the key it names under it.
#[derive(Clone, Debug, PartialEq)]
struct Location {
    scheme: String,
    bucket: String,
    /// Without a slash at either end. Empty for the bucket's root.
    key: String,
}

impl Location {
    fn parse(uri: &str) -> Result<Location, String> {
        let (scheme, rest) = uri
            .split_once("://")
            .filter(|(scheme, _)| S3_SCHEMES.contains(scheme))
            .ok_or_else(|| {
                format!("its location {uri:?} is not in S3, the one store an iceberg table is written to")
            })?;
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(format!("its location {uri:?} names no bucket"));
        }
        Ok(Location {
            scheme: scheme.to_string(),
            bucket: bucket.to_string(),
            key: key.trim_matches('/').to_string(),
        })
    }

    /// The key `name` under this location.
    fn key(&self, name: &str) -> String {
        match (self.key.is_empty(), name.is_empty()) {
            (true, _) => name.to_string(),
            (false, true) => self.key.clone(),
            (false, false) => format!("{}/{name}", self.key),
        }
    }

    /// The URI of `name` under this location, as the table's metadata names files.
    fn uri(&self, name: &str) -> String {
        format!("{}://{}/{}", self.scheme, self.bucket, self.key(name))
    }

    /// Where the table's data files go, and where its manifests and manifest lists go.
    fn places(&self, table: &TableMetadata) -> Result<(Location, Location), String> {
        Ok((
            self.within(table, "write.data.path", "data")?,
            self.within(table, "write.metadata.path", "metadata")?,
        ))
    }

    /// Where the table keeps one kind of file: the location `property` names, else `default` under the table's.
    fn within(&self, table: &TableMetadata, property: &str, default: &str) -> Result<Location, String> {
        let Some(uri) = table.properties.get(property) else {
            return Ok(Location {
                key: self.key(default),
                ..self.clone()
            });
        };
        let place = Location::parse(uri)?;
        if place.bucket != self.bucket {
            return Err(format!(
                "its {property} {uri:?} is in another bucket than its location's, {}",
                self.bucket
            ));
        }
        Ok(place)
    }

    /// The key under this location's bucket of `uri`, a file of the table.
    fn key_of(&self, uri: &str) -> Result<Path, String> {
        let file = Location::parse(uri)?;
        if file.bucket != self.bucket {
            return Err(format!(
                "{uri:?} is in another bucket than the table's, {}",
                self.bucket
            ));
        }
        Path::parse(&file.key).map_err(|e| format!("{uri:?} is no path: {e}"))
    }
}

/// Records closed files as the table's next snapshot: a manifest of them, and a manifest list of it and the parent's
/// manifests, the small ones merged as the table's settings say.
struct IcebergCommitter {
    catalog: Catalog,
    namespace: Vec<String>,
    table: String,
    /// The table the destination started on. One replaced since is another table, whose ids the files do not carry.
    uuid: String,
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
    /// Bounds each store call.
    stall_after: Duration,
    /// The table's location.
    location: Location,
    /// Where data files go.
    data: Location,
    /// Where manifests and manifest lists go.
    meta: Location,
    /// The table as last loaded or committed.
    metadata: TableMetadata,
    /// The schema files are written with.
    schema: Schema,
    /// The spec files are partitioned by.
    spec: PartitionSpec,
    /// Each spec field's value type and transform.
    partition_types: Vec<(Primitive, Transform)>,
    /// Lost commits, by their snapshot ids.
    lost: Lost<i64>,
}

/// The longest name a data file takes under the table's data location: a partition's digest folder, then the file.
const LONGEST_DATA_NAME: usize = "partition-".len() + 32 + 1 + 32 + ".parquet".len();

/// The longest name a manifest or manifest list takes under the table's metadata location: a snapshot's list, whose
/// snapshot id is never negative.
const LONGEST_META_NAME: usize = "snap-".len() + "9223372036854775807".len() + "-1-".len() + 32 + ".avro".len();

impl Committer for IcebergCommitter {
    fn file_path(&self, partition: &Partition, id: &str) -> String {
        let folder = partition.path.join("/");
        let file = format!("{id}.parquet");
        let key = if folder.is_empty() {
            self.data.key(&file)
        } else {
            self.data.key(&format!("{folder}/{file}"))
        };
        if key.len() <= objects::MAX_KEY_BYTES {
            return key;
        }
        // A reader takes a file's partition from its manifest entry, never from its path. A partition too long to
        // name in a key goes under its digest, its files still together.
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, folder.as_bytes());
        let hex: String = digest.as_ref()[..16].iter().map(|b| format!("{b:02x}")).collect();
        self.data.key(&format!("partition-{hex}/{file}"))
    }

    fn commit(&mut self, files: &[DataFile]) -> Result<(), CommitError> {
        if files.is_empty() {
            return Ok(());
        }
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        if self.lost.same_files(&paths) {
            self.reload().map_err(CommitError::Transient)?;
            if self.landed() {
                return Ok(());
            }
        }
        let snapshot = self.write_snapshot(files).map_err(CommitError::Transient)?;
        let id = snapshot.snapshot_id;
        let requirements = [
            Requirement::AssertTableUuid(self.uuid.clone()),
            Requirement::AssertRefSnapshotId {
                r#ref: MAIN.to_string(),
                snapshot_id: snapshot.parent_snapshot_id,
            },
        ];
        let updates = [
            Update::AddSnapshot(snapshot),
            Update::SetSnapshotRef {
                name: MAIN.to_string(),
                reference: SnapshotRef {
                    snapshot_id: id,
                    kind: "branch".to_string(),
                    ..self.metadata.refs.get(MAIN).cloned().unwrap_or_default()
                },
            },
        ];
        match self
            .catalog
            .commit(&self.namespace, &self.table, &requirements, &updates)
        {
            Ok(committed) => {
                self.metadata = committed.metadata;
                self.lost.clear();
                Ok(())
            }
            Err(CommitFailure::Conflict) => {
                self.reload().map_err(CommitError::Transient)?;
                // The conflict may be a lost commit of these files that landed late.
                if self.landed() {
                    return Ok(());
                }
                Err(CommitError::Conflict)
            }
            Err(CommitFailure::Unknown(why)) => {
                self.lost.note(&paths, id);
                if self.reload().is_ok() && self.landed() {
                    return Ok(());
                }
                Err(CommitError::Unknown(format!(
                    "the commit's answer was lost, and the table is looked at before committing again: {why}"
                )))
            }
            Err(CommitFailure::Failed(why)) => Err(CommitError::Transient(why)),
        }
    }
}

impl IcebergCommitter {
    /// The committer of the table `metadata` describes, whose files are written with `schema`. Err when the table's
    /// spec cannot be followed or its files are not in one S3 bucket.
    fn new(
        catalog: Catalog,
        config: &IcebergDestinationConfig,
        metadata: TableMetadata,
        schema: Schema,
        store: Arc<dyn ObjectStore>,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<IcebergCommitter, String> {
        let spec = metadata
            .spec()
            .cloned()
            .ok_or("the table has no default partition spec")?;
        let partition_types = partition_types(&spec, &schema, &metadata.schemas)?;
        let location = Location::parse(&metadata.location)?;
        let (data, meta) = location.places(&metadata)?;
        // Every file would be refused by S3, and its write retried until the destination stalls.
        for (place, what, longest) in [
            (&data, "data", LONGEST_DATA_NAME),
            (&meta, "manifest", LONGEST_META_NAME),
        ] {
            if place.key("").len() + 1 + longest > objects::MAX_KEY_BYTES {
                return Err(format!(
                    "the table keeps its {what} files under {:?}, which leaves no room for a file's name in the {} \
                     bytes of an S3 key",
                    place.uri(""),
                    objects::MAX_KEY_BYTES
                ));
            }
        }
        Ok(IcebergCommitter {
            catalog,
            namespace: config.namespace.clone(),
            table: config.table.clone(),
            uuid: metadata.table_uuid.clone(),
            store,
            runtime,
            stall_after: config.stall_after,
            location,
            data,
            meta,
            metadata,
            schema,
            spec,
            partition_types,
            lost: Lost::none(),
        })
    }

    /// Whether a lost commit is in the table as last read.
    fn landed(&mut self) -> bool {
        let snapshots = &self.metadata.snapshots;
        self.lost.landed(|id| snapshots.iter().any(|s| s.snapshot_id == *id))
    }

    /// Reads the table as it is now. Err when it is gone or replaced, which no commit gets past.
    fn reload(&mut self) -> Result<(), String> {
        let loaded = self
            .catalog
            .load_table(&self.namespace, &self.table)
            .map_err(|e| e.to_string())?
            .ok_or("the table was dropped")?;
        if loaded.metadata.table_uuid != self.uuid {
            return Err("the table was replaced by another of its name; the destination restarts on it".into());
        }
        self.metadata = loaded.metadata;
        Ok(())
    }

    /// Writes the manifest and manifest list of a snapshot adding `files` to the current one, and gives the snapshot.
    fn write_snapshot(&self, files: &[DataFile]) -> Result<Snapshot, String> {
        let snapshot_id = snapshot_id();
        let parent = self.metadata.current_snapshot();
        let sequence_number = self.metadata.last_sequence_number + 1;
        let entries = files
            .iter()
            .map(|file| self.entry(snapshot_id, file))
            .collect::<Result<Vec<_>, String>>()?;
        let mut manifests = vec![self.put_manifest(&self.spec, snapshot_id, &entries)?];
        if let Some(parent) = parent {
            manifests.extend(self.carried(parent, snapshot_id)?);
        }
        // Taken from the table as loaded for this attempt: a commit made again assigns row ids again.
        let first_row_id = self.metadata.next_row_id();
        let added_rows = first_row_id.map(|first| manifest::assign_row_ids(&mut manifests, first));
        let list = manifest::write_manifest_list(
            self.metadata.format_version,
            snapshot_id,
            parent.map(|p| p.snapshot_id),
            sequence_number,
            &manifests,
        )?;
        let list_name = format!("snap-{snapshot_id}-1-{}.avro", objects::object_id());
        self.put(&self.meta.key(&list_name), list)?;
        Ok(Snapshot {
            snapshot_id,
            parent_snapshot_id: parent.map(|p| p.snapshot_id),
            sequence_number,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64),
            manifest_list: self.meta.uri(&list_name),
            summary: summary(files, &entries, parent),
            schema_id: Some(self.schema.schema_id),
            first_row_id,
            added_rows,
        })
    }

    /// `file`'s manifest entry, its partition values read back from its partition's text.
    fn entry(&self, snapshot_id: i64, file: &DataFile) -> Result<DataFileEntry, String> {
        if file.partition.values.len() != self.partition_types.len() {
            return Err(format!(
                "{} has {} partition values, and the spec {}",
                file.path,
                file.partition.values.len(),
                self.partition_types.len()
            ));
        }
        let partition = self
            .partition_types
            .iter()
            .zip(&file.partition.values)
            .map(|((result, transform), (_, text))| literal_of(result, transform, text))
            .collect::<Result<Vec<_>, String>>()?;
        let stats = file
            .metadata
            .as_deref()
            .map(|metadata| manifest::stats(metadata, &self.schema))
            .unwrap_or_default();
        Ok(DataFileEntry::added(
            snapshot_id,
            format!("{}://{}/{}", self.location.scheme, self.location.bucket, file.path),
            partition,
            file.record_count as i64,
            file.size_bytes as i64,
            stats,
        ))
    }

    /// Writes a manifest of `entries`, partitioned by `spec`, and gives its list entry.
    fn put_manifest(
        &self,
        spec: &PartitionSpec,
        snapshot_id: i64,
        entries: &[DataFileEntry],
    ) -> Result<ManifestFile, String> {
        let types = partition_types(spec, &self.schema, &self.metadata.schemas)?;
        let types: Vec<Primitive> = types.into_iter().map(|(ty, _)| ty).collect();
        let bytes = manifest::write_manifest(self.metadata.format_version, &self.schema, spec, &types, entries)?;
        let length = bytes.len() as u64;
        let name = format!("{}-m0.avro", objects::object_id());
        self.put(&self.meta.key(&name), bytes)?;
        Ok(manifest::summarize(
            self.meta.uri(&name),
            length,
            &self.schema,
            spec,
            snapshot_id,
            entries,
        ))
    }

    /// The parent's manifests as the new snapshot lists them: small ones merged once there are enough of them, each
    /// group into one, and the rest as they are. A group that cannot be merged now stays as it was: merging is upkeep,
    /// and no reason to hold rows back.
    fn carried(&self, parent: &Snapshot, snapshot_id: i64) -> Result<Vec<ManifestFile>, String> {
        let mut list = manifest::read_manifest_list(&self.get(&parent.manifest_list)?)?;
        // A list written before format version 2 may lack counts a version 2 list must carry: they are counted from
        // the manifest itself.
        for m in &mut list {
            let files = [m.added_files_count, m.existing_files_count, m.deleted_files_count];
            let rows = [m.added_rows_count, m.existing_rows_count, m.deleted_rows_count];
            if files.contains(&None) || rows.contains(&None) {
                let entries = manifest::read_manifest(&self.get(&m.manifest_path)?, m)?;
                manifest::fill_counts(m, &entries);
            }
        }
        let property = |key: &str| self.metadata.properties.get(key);
        if property("commit.manifest-merge.enabled").is_some_and(|v| v.eq_ignore_ascii_case("false")) {
            return Ok(list);
        }
        let target = property("commit.manifest.target-size-bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(manifest::TARGET_SIZE_BYTES);
        let min_count = property("commit.manifest.min-count-to-merge")
            .and_then(|v| v.parse().ok())
            .unwrap_or(manifest::MIN_COUNT_TO_MERGE);
        let mut merged_away = vec![false; list.len()];
        let mut carried = Vec::new();
        for group in manifest::merge_plan(&list, target, min_count) {
            if let Ok(merged) = self.merge(&list, &group, snapshot_id) {
                carried.push(merged);
                for i in group {
                    merged_away[i] = true;
                }
            }
        }
        carried.extend(
            list.into_iter()
                .zip(merged_away)
                .filter(|(_, away)| !away)
                .map(|(manifest, _)| manifest),
        );
        Ok(carried)
    }

    /// The manifests `group` names in `list`, rewritten as one.
    fn merge(&self, list: &[ManifestFile], group: &[usize], snapshot_id: i64) -> Result<ManifestFile, String> {
        let spec_id = list[group[0]].partition_spec_id;
        let spec = self
            .metadata
            .partition_specs
            .iter()
            .find(|s| s.spec_id == spec_id)
            .ok_or_else(|| format!("the table has no partition spec {spec_id}"))?;
        let mut entries = Vec::new();
        for &i in group {
            entries.extend(manifest::read_manifest(&self.get(&list[i].manifest_path)?, &list[i])?);
        }
        self.put_manifest(spec, snapshot_id, &manifest::merge(entries, snapshot_id))
    }

    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        let path = Path::parse(key).map_err(|e| format!("{key:?} is no path: {e}"))?;
        wait(
            &self.runtime,
            self.stall_after,
            self.store.put(&path, PutPayload::from(bytes)),
        )
        .map(|_| ())
        .map_err(|e| format!("writing {key}: {e}"))
    }

    fn get(&self, uri: &str) -> Result<Vec<u8>, String> {
        let path = self.location.key_of(uri)?;
        let store = self.store.clone();
        let got = wait(&self.runtime, self.stall_after, async move {
            let got = store.get(&path).await?;
            if got.meta.size > MAX_METADATA_BYTES {
                return Ok::<_, object_store::Error>(Err(got.meta.size));
            }
            got.bytes().await.map(Ok)
        })
        .map_err(|e| format!("reading {uri}: {e}"))?;
        got.map(|bytes| bytes.to_vec())
            .map_err(|size| format!("{uri} holds {size} bytes, more than the {MAX_METADATA_BYTES} a manifest may"))
    }
}

/// The most a manifest or manifest list read from the table may hold. Iceberg's writers roll a manifest at 8 MiB by
/// default, and the whole object is held while it is decoded.
const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;

/// A new snapshot's id: random, positive, as Iceberg's own writers make them.
fn snapshot_id() -> i64 {
    let bytes: [u8; 8] = crate::leader::auth::random_bytes(8)
        .try_into()
        .expect("asked for 8 bytes");
    (i64::from_le_bytes(bytes) & i64::MAX).max(1)
}

/// An append's summary: what it adds, and the table's totals after it where the parent's summary has them.
fn summary(files: &[DataFile], entries: &[DataFileEntry], parent: Option<&Snapshot>) -> BTreeMap<String, String> {
    let added_files = entries.len() as i64;
    let added_records: i64 = entries.iter().map(|e| e.record_count).sum();
    let added_size: i64 = entries.iter().map(|e| e.file_size_in_bytes).sum();
    let partitions = files
        .iter()
        .map(|f| &f.partition.path)
        .collect::<std::collections::HashSet<_>>()
        .len();
    let mut summary = BTreeMap::from([
        ("operation".to_string(), "append".to_string()),
        ("added-data-files".to_string(), added_files.to_string()),
        ("added-records".to_string(), added_records.to_string()),
        ("added-files-size".to_string(), added_size.to_string()),
        ("changed-partition-count".to_string(), partitions.to_string()),
    ]);
    let totals = [
        ("total-data-files", added_files),
        ("total-records", added_records),
        ("total-files-size", added_size),
        ("total-delete-files", 0),
        ("total-position-deletes", 0),
        ("total-equality-deletes", 0),
    ];
    for (key, added) in totals {
        let before = match parent {
            None => Some(0),
            Some(parent) => parent.summary.get(key).and_then(|v| v.parse::<i64>().ok()),
        };
        if let Some(before) = before {
            summary.insert(key.to_string(), (before + added).to_string());
        }
    }
    summary
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    use object_store::memory::InMemory;
    use serde_json::{Value, json};

    use crate::plan::topology::SecretRef;

    use super::*;
    use crate::io::files::Partition;
    use crate::io::iceberg::manifest::Status;
    use crate::io::iceberg::metadata::Literal;
    use crate::plan::topology::{AccessKey, Compression};

    /// What goes wrong with the fake catalog's next commit.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub(super) enum Fault {
        /// The commit lands and its answer is a 503.
        Lost,
        /// The answer is a 503, and the commit lands before the next commit is answered, if its requirements still
        /// hold.
        Late,
        /// The answer is a 503, and the commit never lands.
        Dropped,
        /// The answer is a 409, as when another writer's commit came first, and the commit never lands.
        Refused,
    }

    #[derive(Default)]
    pub(super) struct Fake {
        pub(super) table: Option<TableMetadata>,
        pub(super) faults: Vec<Fault>,
        pub(super) commits: usize,
        /// A `Late` commit, landed before the next commit is answered when its requirements still hold.
        late: Option<Value>,
        /// The `Authorization` header of each call for the table.
        pub(super) signed: Vec<String>,
        /// The body of each call for a token.
        pub(super) token_asks: Vec<String>,
        /// The request line of each call for the catalog's config.
        pub(super) configs: Vec<String>,
    }

    /// A REST catalog on 127.0.0.1 holding one table, under whatever namespace and name it is asked for, in
    /// `s3://bucket/wh/t`. It hands out the token `fake-token` for any client credentials.
    pub(super) fn serve(fake: Arc<Mutex<Fake>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let mut reader = BufReader::new(conn.unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut length = 0;
                let mut authorization = String::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let header = header.trim_end();
                    if header.is_empty() {
                        break;
                    }
                    let lower = header.to_ascii_lowercase();
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                    if lower.starts_with("authorization:") {
                        authorization = header["authorization:".len()..].trim().to_string();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let (status, answer) = {
                    let mut fake = fake.lock().unwrap();
                    if line.starts_with("POST /v1/oauth/tokens ") {
                        fake.token_asks.push(String::from_utf8_lossy(&body).into_owned());
                        (
                            200,
                            json!({"access_token": "fake-token", "token_type": "bearer", "expires_in": 3600}),
                        )
                    } else {
                        if line.contains("/tables") {
                            fake.signed.push(authorization);
                        }
                        if line.starts_with("GET /v1/config") {
                            fake.configs.push(line.trim_end().to_string());
                        }
                        let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                        answer(&mut fake, line.trim_end(), body)
                    }
                };
                let answer = answer.to_string();
                write!(
                    reader.into_inner(),
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{answer}",
                    answer.len()
                )
                .unwrap();
            }
        });
        url
    }

    fn answer(fake: &mut Fake, line: &str, body: Value) -> (u16, Value) {
        let loaded =
            |table: &TableMetadata| json!({"metadata-location": "s3://bucket/wh/t/metadata/v.json", "metadata": table});
        let mut words = line.split(' ');
        let method = words.next().unwrap_or_default();
        let path = words.next().unwrap_or_default();
        let path = path.split('?').next().unwrap_or_default();
        // Any namespace and table name the one table.
        let shape: Vec<&str> = (path.split('/').enumerate())
            .map(|(i, part)| if i == 3 || i == 5 { "*" } else { part })
            .collect();
        match (method, shape.join("/").as_str()) {
            ("GET", "/v1/config") => (200, json!({})),
            ("GET", "/v1/namespaces/*/tables/*") => match &fake.table {
                Some(table) => (200, loaded(table)),
                None => (404, json!({})),
            },
            ("POST", "/v1/namespaces/*/tables") => {
                let schema: Schema = serde_json::from_value(body["schema"].clone()).unwrap();
                let mut spec: PartitionSpec = serde_json::from_value(body["partition-spec"].clone()).unwrap();
                spec.spec_id = 0;
                let table = TableMetadata {
                    next_row_id: None,
                    format_version: 2,
                    table_uuid: "uuid-1".into(),
                    location: "s3://bucket/wh/t".into(),
                    last_sequence_number: 0,
                    last_column_id: max_id(&serde_json::to_value(&schema).unwrap()),
                    current_schema_id: schema.schema_id,
                    schemas: vec![schema],
                    default_spec_id: 0,
                    last_partition_id: spec.fields.iter().map(|f| f.field_id).max().unwrap_or(999),
                    partition_specs: vec![spec],
                    properties: serde_json::from_value(body["properties"].clone()).unwrap(),
                    current_snapshot_id: None,
                    snapshots: Vec::new(),
                    refs: BTreeMap::new(),
                    other: BTreeMap::new(),
                };
                fake.table = Some(table.clone());
                (200, loaded(&table))
            }
            ("POST", "/v1/namespaces/*/tables/*") => {
                if let Some(late) = fake.late.take() {
                    let table = fake.table.as_mut().unwrap();
                    if late["requirements"].as_array().unwrap().iter().all(|r| holds(table, r)) {
                        for update in late["updates"].as_array().unwrap() {
                            apply(table, update);
                        }
                        fake.commits += 1;
                    }
                }
                if fake.faults.first() == Some(&Fault::Late) {
                    fake.faults.remove(0);
                    fake.late = Some(body);
                    return (503, json!({}));
                }
                let table = fake.table.as_mut().unwrap();
                let fault = if fake.faults.is_empty() {
                    None
                } else {
                    Some(fake.faults.remove(0))
                };
                match fault {
                    Some(Fault::Dropped) => return (503, json!({})),
                    Some(Fault::Refused) => return (409, json!({"error": {"message": "another commit came first"}})),
                    _ => {}
                }
                for requirement in body["requirements"].as_array().unwrap() {
                    if !holds(table, requirement) {
                        return (409, json!({"error": {"message": "requirement failed"}}));
                    }
                }
                for update in body["updates"].as_array().unwrap() {
                    apply(table, update);
                }
                fake.commits += 1;
                match fault {
                    Some(Fault::Lost) => (503, json!({})),
                    _ => (200, loaded(table)),
                }
            }
            _ => (404, json!({})),
        }
    }

    fn max_id(value: &Value) -> i32 {
        match value {
            Value::Object(map) => map
                .iter()
                .map(|(k, v)| match (k.as_str(), v.as_i64()) {
                    ("id" | "element-id", Some(id)) => id as i32,
                    _ => max_id(v),
                })
                .max()
                .unwrap_or(0),
            Value::Array(values) => values.iter().map(max_id).max().unwrap_or(0),
            _ => 0,
        }
    }

    fn holds(table: &TableMetadata, requirement: &Value) -> bool {
        match requirement["type"].as_str().unwrap() {
            "assert-table-uuid" => requirement["uuid"] == table.table_uuid.as_str(),
            "assert-ref-snapshot-id" => {
                requirement["snapshot-id"].as_i64() == table.refs.get(MAIN).map(|r| r.snapshot_id)
            }
            "assert-current-schema-id" => requirement["current-schema-id"] == table.current_schema_id,
            "assert-last-assigned-field-id" => requirement["last-assigned-field-id"] == table.last_column_id,
            other => panic!("unknown requirement {other}"),
        }
    }

    fn apply(table: &mut TableMetadata, update: &Value) {
        match update["action"].as_str().unwrap() {
            "add-schema" => {
                table
                    .schemas
                    .push(serde_json::from_value(update["schema"].clone()).unwrap());
                table.last_column_id = update["last-column-id"].as_i64().unwrap() as i32;
            }
            "set-current-schema" => {
                table.current_schema_id = table.schemas.last().unwrap().schema_id;
                assert_eq!(update["schema-id"], -1);
            }
            "add-snapshot" => {
                let snapshot: Snapshot = serde_json::from_value(update["snapshot"].clone()).unwrap();
                assert_eq!(snapshot.sequence_number, table.last_sequence_number + 1);
                table.last_sequence_number = snapshot.sequence_number;
                // As a catalog does: a snapshot takes its row ids from the table's next.
                if let Some(next) = table.next_row_id() {
                    assert_eq!(snapshot.first_row_id, Some(next), "a snapshot's first row id");
                    table.next_row_id = Some(next + snapshot.added_rows.expect("a version 3 snapshot's added rows"));
                }
                table.snapshots.push(snapshot);
            }
            "set-snapshot-ref" => {
                let main: SnapshotRef = serde_json::from_value(update.clone()).unwrap();
                table.current_snapshot_id = Some(main.snapshot_id);
                table.refs.insert(MAIN.into(), main);
            }
            other => panic!("unknown update {other}"),
        }
    }

    fn st(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect(), false)
    }

    #[test]
    fn a_table_says_how_its_files_are_written_and_the_config_says_the_rest() {
        let config = config("http://catalog", &[]);
        let unsaid = WriteSettings::of(&BTreeMap::new(), &config);
        assert_eq!(
            unsaid,
            WriteSettings {
                options: config.compression.into(),
                row_group_bytes: ROW_GROUP_BYTES,
                file_bytes: config.max_object_bytes,
            }
        );
        let said = BTreeMap::from(
            [
                (CODEC, "SNAPPY"),
                ("write.parquet.compression-level", "3"),
                ("write.parquet.row-group-size-bytes", "8388608"),
                ("write.target-file-size-bytes", "268435456"),
                ("write.parquet.bloom-filter-enabled.column.http.host", "true"),
                ("write.parquet.bloom-filter-fpp.column.http.host", "0.05"),
                ("write.parquet.bloom-filter-enabled.column.id", "true"),
                ("write.parquet.bloom-filter-enabled.column.off", "false"),
            ]
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        assert_eq!(
            WriteSettings::of(&said, &config),
            WriteSettings {
                options: parquet::WriteOptions {
                    codec: Compression::Snappy,
                    level: Some(3),
                    bloom_filters: vec![
                        (vec!["http".into(), "host".into()], 0.05),
                        (vec!["id".into()], BLOOM_FILTER_FPP),
                    ],
                },
                row_group_bytes: 8 << 20,
                file_bytes: 256 << 20,
            }
        );
        let brotli = BTreeMap::from([(CODEC.to_string(), "brotli".to_string())]);
        assert_eq!(WriteSettings::of(&brotli, &config).options.codec, config.compression);
    }

    fn config(url: &str, partition_by: &[&str]) -> IcebergDestinationConfig {
        IcebergDestinationConfig {
            catalog: url.to_string(),
            warehouse: None,
            sign_in: CatalogSignIn::None,
            namespace: vec!["ns".into()],
            table: "t".into(),
            partition_by: partition_by.iter().map(|t| t.to_string()).collect(),
            storage: None,
            compression: Compression::Zstd,
            tls: None,
            in_flight: 2,
            max_object_bytes: 1 << 20,
            buffer_bytes: 1 << 24,
            stall_after: Duration::from_secs(10),
            file_interval: Duration::from_secs(60),
        }
    }

    fn client(url: &str) -> Catalog {
        // The environment's proxy would carry a loopback call away.
        let agent = ureq::Agent::config_builder().proxy(None).build().new_agent();
        Catalog::new(url.to_string(), None, Auth::None, agent, Duration::from_secs(10))
    }

    /// A committer on the fake catalog's table, opened for `record`, writing into `store`.
    fn committer(
        url: &str,
        config: &IcebergDestinationConfig,
        record: &DataType,
        store: &Arc<InMemory>,
    ) -> IcebergCommitter {
        let mut catalog = client(url);
        let (loaded, schema) = open(&mut catalog, config, record).unwrap();
        IcebergCommitter::new(
            catalog,
            config,
            loaded.metadata,
            schema,
            store.clone(),
            objects::runtime().unwrap(),
        )
        .unwrap()
    }

    fn file(committer: &IcebergCommitter, values: &[(&str, &str)], rows: u64) -> DataFile {
        let partition = Partition {
            part: 0,
            path: values.iter().map(|(n, v)| format!("{n}={v}")).collect(),
            values: values.iter().map(|(n, v)| (n.to_string(), v.to_string())).collect(),
        };
        DataFile {
            path: committer.file_path(&partition, &objects::object_id()),
            partition,
            size_bytes: 100,
            record_count: rows,
            metadata: None,
        }
    }

    /// The data files the table's current snapshot holds, by path, with their status in the manifest naming them.
    fn live_files(committer: &IcebergCommitter) -> Vec<DataFileEntry> {
        let snapshot = committer.metadata.current_snapshot().unwrap();
        let list = manifest::read_manifest_list(&committer.get(&snapshot.manifest_list).unwrap()).unwrap();
        list.iter()
            .flat_map(|m| manifest::read_manifest(&committer.get(&m.manifest_path).unwrap(), m).unwrap())
            .filter(|e| e.status != Status::Deleted)
            .collect()
    }

    #[test]
    fn a_table_whose_files_could_not_be_named_in_a_key_is_refused() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let config = config(&url, &[]);
        let mut catalog = client(&url);
        let (loaded, schema) = open(&mut catalog, &config, &st(&[("a", DataType::Str)])).unwrap();
        // A location whose key leaves a name exactly the room it needs, or a byte less.
        let at = |property: &str, short: usize| {
            let mut metadata = loaded.metadata.clone();
            // The key is the bucket's own, after `s3://bucket/`; its name follows a `/`.
            let longest = if property == "write.data.path" {
                LONGEST_DATA_NAME
            } else {
                LONGEST_META_NAME
            };
            let key = "k".repeat(objects::MAX_KEY_BYTES - 1 - longest + short);
            metadata
                .properties
                .insert(property.into(), format!("s3://bucket/{key}"));
            let store = Arc::new(InMemory::new());
            IcebergCommitter::new(
                client(&url),
                &config,
                metadata,
                schema.clone(),
                store,
                objects::runtime().unwrap(),
            )
        };
        for property in ["write.data.path", "write.metadata.path"] {
            assert!(at(property, 0).is_ok(), "{property} with a name's room");
            let Err(e) = at(property, 1) else {
                panic!("{property} a byte short of a name's room is refused");
            };
            assert!(e.contains("no room"), "{e}");
        }
    }

    #[test]
    fn a_missing_table_is_created_from_the_record_and_partitioned_as_configured() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let record = st(&[("host", DataType::Str), ("ts", DataType::DateTime)]);
        let committer = committer(
            &url,
            &config(&url, &["host", "day(ts)"]),
            &record,
            &Arc::new(InMemory::new()),
        );

        let table = fake.lock().unwrap().table.clone().unwrap();
        assert_eq!(table.properties.get("format-version").map(String::as_str), Some("2"));
        let spec = table.spec().unwrap();
        let fields: Vec<_> = spec
            .fields
            .iter()
            .map(|f| (f.name.as_str(), &f.transform, f.source_id, f.field_id))
            .collect();
        assert_eq!(
            fields,
            [
                ("host", &Transform::Identity, 1, 1000),
                ("ts_day", &Transform::Day, 2, 1001)
            ]
        );
        assert_eq!(committer.schema, *table.schema().unwrap());
        assert_eq!(
            committer.file_path(
                &file(&committer, &[("host", "a"), ("ts_day", "2026-10-11")], 1).partition,
                "f"
            ),
            "wh/t/data/host=a/ts_day=2026-10-11/f.parquet"
        );
    }

    #[test]
    fn a_record_that_gains_a_field_adds_its_column_before_any_file() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let store = Arc::new(InMemory::new());
        let config = config(&url, &[]);
        committer(&url, &config, &st(&[("a", DataType::Str)]), &store);
        let wider = committer(
            &url,
            &config,
            &st(&[("a", DataType::Str), ("b", DataType::I64)]),
            &store,
        );

        let table = fake.lock().unwrap().table.clone().unwrap();
        assert_eq!(table.schemas.len(), 2);
        assert_eq!(table.last_column_id, 2);
        assert_eq!(wider.schema.schema_id, 1);
        assert_eq!(
            wider
                .schema
                .fields
                .iter()
                .map(|f| (f.id, f.name.as_str()))
                .collect::<Vec<_>>(),
            [(1, "a"), (2, "b")]
        );

        let mut catalog = client(&url);
        let Err(e) = open(&mut catalog, &config, &st(&[("a", DataType::I64)])) else {
            panic!("a column of another type must be refused");
        };
        assert!(e.message.contains('a'), "{e}");
    }

    #[test]
    fn a_table_of_another_format_version_or_encrypted_is_refused() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let config = config(&url, &[]);
        let record = st(&[("a", DataType::Str)]);
        committer(&url, &config, &record, &Arc::new(InMemory::new()));

        fake.lock().unwrap().table.as_mut().unwrap().format_version = 4;
        let Err(e) = open(&mut client(&url), &config, &record) else {
            panic!("a version 4 table must be refused");
        };
        assert!(
            e.message.contains("format version 4, and versions 2 and 3 are written"),
            "{e}"
        );

        fake.lock().unwrap().table.as_mut().unwrap().format_version = 1;
        let Err(e) = open(&mut client(&url), &config, &record) else {
            panic!("a version 1 table must be refused");
        };
        assert!(
            e.message
                .contains("in Spark with ALTER TABLE ns.t SET TBLPROPERTIES ('format-version'='2')"),
            "{e}"
        );

        let mut held = fake.lock().unwrap();
        let table = held.table.as_mut().unwrap();
        table.format_version = 2;
        table.properties.insert("encryption.key-id".into(), "k".into());
        drop(held);
        let Err(e) = open(&mut client(&url), &config, &record) else {
            panic!("an encrypted table must be refused");
        };
        assert!(e.message.contains("encrypted"), "{e}");

        let mut held = fake.lock().unwrap();
        let table = held.table.as_mut().unwrap();
        table.format_version = 3;
        table.properties.clear();
        table.other.insert(
            "encryption-keys".into(),
            json!([{"key-id": "k", "encrypted-key-metadata": "AA=="}]),
        );
        drop(held);
        let Err(e) = open(&mut client(&url), &config, &record) else {
            panic!("a version 3 table with encryption keys must be refused");
        };
        assert!(e.message.contains("encrypted"), "{e}");

        let mut held = fake.lock().unwrap();
        let table = held.table.as_mut().unwrap();
        table.other.clear();
        table.partition_specs[0].fields.push(PartitionField {
            name: "both".into(),
            transform: Transform::Other("zorder".into()),
            source_id: crate::io::iceberg::metadata::NO_SOURCE,
            source_ids: Some(vec![1, 2]),
            field_id: 1000,
        });
        drop(held);
        let Err(e) = open(&mut client(&url), &config, &record) else {
            panic!("a partition field of several columns must be refused");
        };
        assert!(
            e.message
                .contains("partition field both is a transform of several columns"),
            "{e}"
        );
    }

    #[test]
    fn a_version_3_table_takes_each_snapshot_s_row_ids_from_its_next() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let record = st(&[("host", DataType::Str)]);
        let store = Arc::new(InMemory::new());
        let config = config(&url, &[]);
        let mut committer = committer(&url, &config, &record, &store);
        // Upgraded from version 2 after a first snapshot, which keeps no row ids.
        committer.commit(&[file(&committer, &[], 3)]).unwrap();
        {
            let mut fake = fake.lock().unwrap();
            let table = fake.table.as_mut().unwrap();
            table.format_version = 3;
            table.next_row_id = Some(100);
            // So that the third commit merges the second's two manifests.
            table
                .properties
                .insert("commit.manifest.min-count-to-merge".into(), "2".into());
        }
        let mut committer = self::committer(&url, &config, &record, &store);
        committer
            .commit(&[file(&committer, &[], 4), file(&committer, &[], 5)])
            .unwrap();
        committer.commit(&[file(&committer, &[], 6)]).unwrap();

        let table = fake.lock().unwrap().table.clone().unwrap();
        let rows: Vec<_> = table.snapshots.iter().map(|s| (s.first_row_id, s.added_rows)).collect();
        // The second snapshot gives row ids to its own 9 rows and to the first snapshot's 3. The third gives them to
        // its own 6, and to the manifest it merges the second's two into, whose files keep the ones they have.
        assert_eq!(rows, [(None, None), (Some(100), Some(12)), (Some(112), Some(18))]);
        assert_eq!(table.next_row_id, Some(130));
        let list = manifest::read_manifest_list(&committer.get(&table.snapshots[2].manifest_list).unwrap()).unwrap();
        assert_eq!(list.len(), 2, "the second snapshot's manifests are merged");

        // Every live file's rows hold their own row ids, none shared.
        let mut ranges: Vec<(i64, i64)> = live_files(&committer)
            .into_iter()
            .map(|e| {
                let first = e.first_row_id.expect("every live file has a first row id");
                (first, first + e.record_count)
            })
            .collect();
        ranges.sort();
        assert_eq!(ranges, [(100, 104), (104, 109), (109, 112), (112, 118)]);
    }

    #[test]
    fn each_commit_adds_a_snapshot_on_the_last_holding_every_file() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let record = st(&[("host", DataType::Str)]);
        let mut committer = committer(&url, &config(&url, &["host"]), &record, &Arc::new(InMemory::new()));

        let first = [
            file(&committer, &[("host", "a")], 3),
            file(&committer, &[("host", "b")], 4),
        ];
        committer.commit(&first).unwrap();
        let second = [file(&committer, &[("host", "a")], 5)];
        committer.commit(&second).unwrap();

        let table = fake.lock().unwrap().table.clone().unwrap();
        assert_eq!(table.snapshots.len(), 2);
        assert_eq!(
            table.snapshots[1].parent_snapshot_id,
            Some(table.snapshots[0].snapshot_id)
        );
        let summary = &table.snapshots[1].summary;
        assert_eq!(summary.get("total-records").map(String::as_str), Some("12"));
        assert_eq!(summary.get("total-data-files").map(String::as_str), Some("3"));

        let mut live: Vec<_> = live_files(&committer)
            .into_iter()
            .map(|e| {
                (
                    e.partition,
                    e.record_count,
                    e.file_path.starts_with("s3://bucket/wh/t/data/host="),
                )
            })
            .collect();
        live.sort_by_key(|(_, rows, _)| *rows);
        let host = |h: &str| vec![Some(Literal::String(h.into()))];
        assert_eq!(live, [(host("a"), 3, true), (host("b"), 4, true), (host("a"), 5, true)]);
    }

    #[test]
    fn a_commit_keeps_the_retention_of_main() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let record = st(&[("host", DataType::Str)]);
        let mut committer = committer(&url, &config(&url, &[]), &record, &Arc::new(InMemory::new()));
        committer.commit(&[file(&committer, &[], 3)]).unwrap();
        let retention = |main: &mut SnapshotRef| {
            main.min_snapshots_to_keep = Some(5);
            main.max_snapshot_age_ms = Some(3_600_000);
            main.max_ref_age_ms = Some(86_400_000);
        };
        retention(fake.lock().unwrap().table.as_mut().unwrap().refs.get_mut(MAIN).unwrap());
        committer.reload().unwrap();

        committer.commit(&[file(&committer, &[], 4)]).unwrap();
        let table = fake.lock().unwrap().table.clone().unwrap();
        let mut expected = SnapshotRef {
            snapshot_id: table.snapshots[1].snapshot_id,
            kind: "branch".into(),
            ..SnapshotRef::default()
        };
        retention(&mut expected);
        assert_eq!(table.refs[MAIN], expected);
    }

    #[test]
    fn a_commit_that_loses_a_race_is_refused_and_lands_on_the_winner_after() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let store = Arc::new(InMemory::new());
        let config = config(&url, &[]);
        let record = st(&[("a", DataType::Str)]);
        let mut loser = committer(&url, &config, &record, &store);
        let mut winner = committer(&url, &config, &record, &store);
        winner.commit(&[file(&winner, &[], 1)]).unwrap();

        let files = [file(&loser, &[], 2)];
        assert_eq!(loser.commit(&files), Err(CommitError::Conflict));
        loser.commit(&files).unwrap();
        let table = fake.lock().unwrap().table.clone().unwrap();
        assert_eq!(
            table.snapshots[1].parent_snapshot_id,
            Some(table.snapshots[0].snapshot_id)
        );
        let mut rows: Vec<_> = live_files(&loser).iter().map(|e| e.record_count).collect();
        rows.sort();
        assert_eq!(rows, [1, 2]);
    }

    #[test]
    fn a_commit_whose_answer_is_lost_is_found_and_not_made_twice() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let mut committer = committer(
            &url,
            &config(&url, &[]),
            &st(&[("a", DataType::Str)]),
            &Arc::new(InMemory::new()),
        );
        let files = [file(&committer, &[], 1)];
        fake.lock().unwrap().faults.push(Fault::Lost);

        // Looked for at once, and found.
        committer.commit(&files).unwrap();
        let fake = fake.lock().unwrap();
        assert_eq!(fake.commits, 1);
        assert_eq!(fake.table.as_ref().unwrap().snapshots.len(), 1);
    }

    #[test]
    fn a_lost_commit_that_lands_late_is_found_and_not_made_twice() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let mut committer = committer(
            &url,
            &config(&url, &[]),
            &st(&[("a", DataType::Str)]),
            &Arc::new(InMemory::new()),
        );
        let files = [file(&committer, &[], 1)];
        fake.lock().unwrap().faults.push(Fault::Late);

        assert!(matches!(committer.commit(&files), Err(CommitError::Unknown(_))));
        // The retry's own commit meets the late one: a conflict, which the reload shows is these files.
        committer.commit(&files).unwrap();
        let fake = fake.lock().unwrap();
        assert_eq!(fake.commits, 1);
        assert_eq!(fake.table.as_ref().unwrap().snapshots.len(), 1);
    }

    #[test]
    fn lost_commits_of_other_files_are_not_taken_for_these() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let mut committer = committer(
            &url,
            &config(&url, &[]),
            &st(&[("a", DataType::Str)]),
            &Arc::new(InMemory::new()),
        );
        let first = [file(&committer, &[], 1)];
        fake.lock().unwrap().faults.push(Fault::Late);
        assert!(matches!(committer.commit(&first), Err(CommitError::Unknown(_))));

        // The destination gave up on those; these are other files, committed on top of the late one.
        let second = [file(&committer, &[], 2)];
        assert_eq!(committer.commit(&second), Err(CommitError::Conflict));
        committer.commit(&second).unwrap();
        assert_eq!(fake.lock().unwrap().table.as_ref().unwrap().snapshots.len(), 2);
    }

    #[test]
    fn small_manifests_are_merged_once_there_are_enough_of_them() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let store = Arc::new(InMemory::new());
        let config = config(&url, &[]);
        let mut committer = committer(&url, &config, &st(&[("a", DataType::Str)]), &store);
        committer
            .metadata
            .properties
            .insert("commit.manifest.min-count-to-merge".into(), "3".into());
        for rows in 1..=4 {
            let files = [file(&committer, &[], rows)];
            committer.commit(&files).unwrap();
            // The fake keeps no properties of a commit's; the committer reads its own copy.
            committer
                .metadata
                .properties
                .insert("commit.manifest.min-count-to-merge".into(), "3".into());
        }
        let snapshot = committer.metadata.current_snapshot().unwrap().clone();
        let list = manifest::read_manifest_list(&committer.get(&snapshot.manifest_list).unwrap()).unwrap();
        // The fourth commit's own manifest, and the three before it merged into one.
        assert_eq!(list.len(), 2);
        assert_eq!(list[1].existing_files_count, Some(3));
        let mut rows: Vec<_> = live_files(&committer).iter().map(|e| e.record_count).collect();
        rows.sort();
        assert_eq!(rows, [1, 2, 3, 4]);
    }

    #[test]
    fn a_location_is_an_s3_bucket_and_a_key() {
        let at = Location::parse("s3a://bucket/wh/t/").unwrap();
        assert_eq!((at.bucket.as_str(), at.key.as_str()), ("bucket", "wh/t"));
        assert_eq!(at.uri("data/x.parquet"), "s3a://bucket/wh/t/data/x.parquet");
        assert_eq!(
            at.key_of("s3a://bucket/wh/t/metadata/m.avro").unwrap().as_ref(),
            "wh/t/metadata/m.avro"
        );
        assert!(at.key_of("s3://other/wh/t/m.avro").is_err());
        assert!(Location::parse("gs://bucket/t").is_err());
        assert!(Location::parse("s3:///t").is_err());
    }

    #[test]
    fn vended_credentials_are_the_longest_prefix_holding_the_table() {
        let credential = |prefix: &str, key: &str| StorageCredential {
            prefix: prefix.into(),
            config: BTreeMap::from([
                (ACCESS_KEY_ID.to_string(), key.to_string()),
                (SECRET_ACCESS_KEY.to_string(), "s".to_string()),
                (EXPIRES_AT_MS.to_string(), "1000".to_string()),
            ]),
        };
        let vended = [
            credential("s3://bucket/", "short"),
            credential("s3://bucket/wh/t", "long"),
            credential("s3://x/", "x"),
        ];
        let (found, expires) = vended_credential(
            &vended,
            &BTreeMap::new(),
            "s3a://bucket/wh/t/data",
            "s3a://bucket/wh/t/metadata",
        )
        .unwrap()
        .unwrap();
        assert_eq!(found.key_id, "long");
        assert_eq!(expires, UNIX_EPOCH + Duration::from_secs(1));
        // Credentials naming no expiry are asked for again.
        let mut undated = credential("s3://bucket/wh/t/", "undated");
        undated.config.remove(EXPIRES_AT_MS);
        let (_, renew_by) = vended_credential(&[undated], &BTreeMap::new(), "s3://bucket/wh/t", "s3://bucket/wh/t")
            .unwrap()
            .unwrap();
        assert!(renew_by < SystemTime::now() + RENEW_BEFORE + UNDATED_LIFE + Duration::from_secs(1));
        assert_eq!(
            vended_credential(&[], &BTreeMap::new(), "s3://bucket/wh/t/", "s3://bucket/wh/t/"),
            Ok(None)
        );
    }

    #[test]
    fn a_catalog_call_is_signed_for_its_service_and_region() {
        let key = |name: &str| SecretRef::LeaderStored {
            name: name.into(),
            version: 1,
        };
        let mut secrets = HeldSecrets::default();
        secrets.set_stored([
            ("id".to_string(), 1, "AKIDEXAMPLE".to_string()),
            ("secret".to_string(), 1, "wJalrXUtnFEMI".to_string()),
        ]);
        let config = IcebergDestinationConfig {
            sign_in: CatalogSignIn::SigV4 {
                signing_name: "glue".into(),
            },
            storage: Some(S3Access {
                region: Some("us-gov-west-1".into()),
                access_key: Some(AccessKey {
                    id: key("id"),
                    secret: key("secret"),
                }),
                ..S3Access::default()
            }),
            ..config("http://127.0.0.1:1", &[])
        };
        let runtime = objects::runtime().unwrap();
        let catalog = catalog(&config, &secrets, &runtime).unwrap();
        let Auth::Signed(signer) = catalog.auth() else {
            panic!("a SigV4 sign-in signs each call");
        };
        let headers: BTreeMap<_, _> = signer
            .sign(
                "POST",
                "https://glue.us-gov-west-1.amazonaws.com/iceberg/v1/catalogs/123/namespaces",
                b"{}",
            )
            .unwrap()
            .into_iter()
            .collect();
        let authorization = &headers["authorization"];
        assert!(
            authorization.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
                && authorization.contains("/us-gov-west-1/glue/aws4_request"),
            "{authorization}"
        );
        assert_eq!(
            headers["x-amz-content-sha256"], "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "the body is signed"
        );
        assert!(
            !headers.contains_key("host"),
            "the client sends the host it connects to"
        );
    }

    #[test]
    fn a_signature_waits_for_credentials_no_longer_than_a_catalog_call() {
        // STS that takes the call and never answers.
        let sts = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let key = |name: &str| SecretRef::LeaderStored {
            name: name.into(),
            version: 1,
        };
        let mut secrets = HeldSecrets::default();
        secrets.set_stored([
            ("id".to_string(), 1, "AKIDEXAMPLE".to_string()),
            ("secret".to_string(), 1, "wJalrXUtnFEMI".to_string()),
        ]);
        let config = IcebergDestinationConfig {
            sign_in: CatalogSignIn::SigV4 {
                signing_name: "glue".into(),
            },
            storage: Some(S3Access {
                region: Some("us-east-1".into()),
                auth_endpoint: Some(format!("http://{}", sts.local_addr().unwrap())),
                access_key: Some(AccessKey {
                    id: key("id"),
                    secret: key("secret"),
                }),
                role: Some(crate::plan::topology::AssumedRole {
                    arn: "arn:aws:iam::123456789012:role/writer".into(),
                    external_id: None,
                }),
                ..S3Access::default()
            }),
            stall_after: Duration::from_secs(2),
            ..config("http://127.0.0.1:1", &[])
        };
        let runtime = objects::runtime().unwrap();
        let catalog = catalog(&config, &secrets, &runtime).unwrap();
        let Auth::Signed(signer) = catalog.auth() else {
            panic!("a SigV4 sign-in signs each call");
        };
        let started = std::time::Instant::now();
        let e = signer
            .sign("GET", "https://glue.us-east-1.amazonaws.com/iceberg/v1/config", b"")
            .unwrap_err();
        assert!(e.contains("no AWS credentials to sign with after 1s"), "{e}");
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
    }

    #[test]
    fn an_aws_address_names_its_signing_region() {
        for (url, region) in [
            ("https://glue.eu-west-1.amazonaws.com/iceberg", Some("eu-west-1")),
            (
                "https://glue-fips.us-gov-west-1.amazonaws.com/iceberg",
                Some("us-gov-west-1"),
            ),
            ("https://s3tables.us-east-1.amazonaws.com/iceberg", Some("us-east-1")),
            ("https://GLUE.US-EAST-2.AMAZONAWS.COM/", Some("us-east-2")),
            ("https://catalog.example/api", None),
            ("https://amazonaws.com/", None),
            ("https://glue.amazonaws.com/", None),
            ("not a url", None),
        ] {
            assert_eq!(signing_region(url).as_deref(), region, "{url}");
        }
    }

    #[test]
    fn vended_credentials_hold_both_the_data_and_the_metadata_location() {
        let credential = |prefix: &str, key: &str| StorageCredential {
            prefix: prefix.into(),
            config: BTreeMap::from([
                (ACCESS_KEY_ID.to_string(), key.to_string()),
                (SECRET_ACCESS_KEY.to_string(), "s".to_string()),
            ]),
        };
        // Data is written outside the table's location; only the bucket-wide credential holds both.
        let vended = [
            credential("s3://bucket/", "bucket"),
            credential("s3://bucket/wh/t", "table"),
            credential("s3://bucket/lake", "lake"),
        ];
        let (data, meta) = ("s3a://bucket/lake/t", "s3://bucket/wh/t/metadata");
        let (found, _) = vended_credential(&vended, &BTreeMap::new(), data, meta)
            .unwrap()
            .unwrap();
        assert_eq!(found.key_id, "bucket");

        let Err(e) = vended_credential(&vended[1..], &BTreeMap::new(), data, meta) else {
            panic!("credentials holding one location alone must be refused");
        };
        assert!(
            e.contains("no single credential") && e.contains(data) && e.contains(meta),
            "{e}"
        );

        // Credentials the destination names are used whatever the catalog vends.
        let config = IcebergDestinationConfig {
            storage: Some(S3Access {
                access_key: Some(AccessKey {
                    id: SecretRef::WorkerLocal("AWS_ACCESS_KEY_ID".into()),
                    secret: SecretRef::WorkerLocal("AWS_SECRET_ACCESS_KEY".into()),
                }),
                ..S3Access::default()
            }),
            ..config("http://127.0.0.1:1", &[])
        };
        let (data, meta) = (Location::parse(data).unwrap(), Location::parse(meta).unwrap());
        let (_, provider) = storage(
            &config,
            &BTreeMap::new(),
            &vended[1..],
            &data,
            &meta,
            &HeldSecrets::default(),
            &objects::runtime().unwrap(),
        )
        .unwrap();
        assert!(provider.is_none());

        // Storage is in the region the catalog's AWS address names, unless the catalog says otherwise for the table.
        let config = IcebergDestinationConfig {
            catalog: "https://glue.eu-west-1.amazonaws.com/iceberg".into(),
            ..config
        };
        let region = |table_config: &BTreeMap<String, String>| {
            storage(
                &config,
                table_config,
                &[],
                &data,
                &meta,
                &HeldSecrets::default(),
                &objects::runtime().unwrap(),
            )
            .unwrap()
            .0
            .region
        };
        assert_eq!(region(&BTreeMap::new()).as_deref(), Some("eu-west-1"));
        let said = BTreeMap::from([("client.region".to_string(), "eu-central-1".to_string())]);
        assert_eq!(region(&said).as_deref(), Some("eu-central-1"));
    }

    #[test]
    fn a_void_partition_field_outlives_its_source_column() {
        let column = |id: i32, name: &str| NestedField {
            id,
            name: name.into(),
            required: false,
            field_type: Type::Primitive(Primitive::String),
            doc: None,
            other: Default::default(),
        };
        let schema = |schema_id: i32, fields: Vec<NestedField>| Schema {
            kind: Default::default(),
            schema_id,
            identifier_field_ids: Vec::new(),
            fields,
        };
        let earlier = schema(0, vec![column(1, "host"), column(2, "gone")]);
        let current = schema(1, vec![column(1, "host")]);
        let field = |name: &str, transform: Transform, source_id: i32| PartitionField {
            source_ids: None,
            name: name.into(),
            transform,
            source_id,
            field_id: 1000,
        };
        let spec = |fields| PartitionSpec { spec_id: 0, fields };
        let schemas = [earlier, current.clone()];

        let types = partition_types(
            &spec(vec![
                field("host", Transform::Identity, 1),
                field("gone_null", Transform::Void, 2),
            ]),
            &current,
            &schemas,
        )
        .unwrap();
        assert_eq!(
            types,
            [
                (Primitive::String, Transform::Identity),
                (Primitive::String, Transform::Void)
            ]
        );
        // Any other transform reads its source, which must be in the current schema.
        assert!(partition_types(&spec(vec![field("gone", Transform::Identity, 2)]), &current, &schemas).is_err());
    }

    #[test]
    fn a_table_partitioned_void_over_a_dropped_column_takes_commits() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let store = Arc::new(InMemory::new());
        let config = config(&url, &["host"]);
        let before = st(&[("host", DataType::Str), ("gone", DataType::Str)]);
        committer(&url, &config, &before, &store);
        {
            let mut fake = fake.lock().unwrap();
            let table = fake.table.as_mut().unwrap();
            let mut schema = table.schemas[0].clone();
            schema.schema_id = 1;
            let gone = schema.fields.pop().unwrap();
            table.schemas.push(schema);
            table.current_schema_id = 1;
            let mut spec = table.partition_specs[0].clone();
            spec.spec_id = 1;
            spec.fields.push(PartitionField {
                source_ids: None,
                name: "gone_null".into(),
                transform: Transform::Void,
                source_id: gone.id,
                field_id: 1001,
            });
            table.partition_specs.push(spec);
            table.default_spec_id = 1;
            table.last_partition_id = 1001;
        }

        let mut committer = committer(&url, &config, &st(&[("host", DataType::Str)]), &store);
        committer
            .commit(&[file(&committer, &[("host", "a"), ("gone_null", "null")], 3)])
            .unwrap();
        let live: Vec<_> = live_files(&committer).into_iter().map(|e| e.partition).collect();
        assert_eq!(live, [vec![Some(Literal::String("a".into())), None]]);
    }

    #[test]
    fn a_partition_term_its_column_cannot_take_is_refused_before_the_catalog_is_asked() {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone());
        let record = st(&[
            ("host", DataType::Str),
            ("ratio", DataType::F64),
            ("tags", DataType::Array(Box::new(DataType::Str))),
        ]);
        for term in ["day(host)", "bucket(16, ratio)", "tags"] {
            let Err(e) = create(&mut client(&url), &config(&url, &[term]), &record) else {
                panic!("{term} must be refused");
            };
            assert!(e.message.contains(term), "{e}");
        }
        assert!(fake.lock().unwrap().table.is_none());
    }

    #[test]
    fn one_record_is_written_to_a_table() {
        let a = st(&[("a", DataType::Str)]);
        let b = st(&[("b", DataType::Str)]);
        let records: Records = [(Some(1), a.clone()), (Some(2), a.clone())].into_iter().collect();
        assert_eq!(one_record(Some(&records)), Ok(&a));
        let records: Records = [(Some(1), a), (Some(2), b)].into_iter().collect();
        assert!(one_record(Some(&records)).is_err());
        assert!(one_record(None).is_err());
    }
}

#[cfg(test)]
#[path = "fuzz.rs"]
mod fuzz;
