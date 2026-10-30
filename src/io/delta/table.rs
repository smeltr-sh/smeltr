//! The Delta destination: the file destination writing the table's Parquet files, and the committer that adds them
//! as the log's next entry.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant D as destination start
//!   participant W as writer thread
//!   participant S as bucket
//!   D->>S: read the log from its last checkpoint, or create the table from the record
//!   D->>S: add the record's new columns, before any file
//!   W->>S: data files, under the table's root
//!   W->>S: _delta_log/N+1.json, only where none is: this writer's transaction and the files' adds
//!   S-->>W: written, taken: read the commits since and commit again, lost: look for it before the next commit
//!   W->>S: at every delta.checkpointInterval, before the next commit: a checkpoint, on the newest in the log
//! ```
//!
//! Each commit carries this writer's transaction, its application a random id per destination start and its version
//! a count of its commits, so a commit whose answer was lost is found in the log and not made twice. Only tables in
//! S3, or a store that speaks its API, are written.

use std::sync::{Arc, Mutex, PoisonError};

use ::parquet::file::metadata::ParquetMetaData;
use arrow_schema::Schema as ArrowSchema;
use object_store::Attributes;
use serde_json::{Map, Value, json};

use super::checkpoint;
use super::log::{
    Action, DomainMetadata, Format, Log, Metadata, Protocol, Snapshot, Txn, commit_name, line, now_ms, uuid,
};
use super::schema::{self, Mapping, StructField, StructType, Type};
use super::unity::UnityCatalog;
use crate::core::column::DataType;
use crate::io::files::{
    CommitError, Committer, DataFile, FileConfig, FileDestination, Lost, MAX_CONFLICT_RETRIES, ParquetFormat,
    Partition, Partitioner,
};
use crate::io::table::{
    Kind, Literal, ROW_GROUP_BYTES, column_at, column_stats, iso_date, one_record, read, url_encode,
};
use crate::io::vended::Vended;
use crate::io::{IoError, OwnedRecord, objects, parquet};
use crate::ops::civil::MICROS_PER_DAY;
use crate::plan::topology::{DeltaDestinationConfig, HeldSecrets, Records, UnityCatalogTable};

/// The writer features of a table this writer can write. A table needing any other is refused.
const WRITER_FEATURES: [&str; 13] = [
    "appendOnly",
    "invariants",
    "checkConstraints",
    "changeDataFeed",
    "columnMapping",
    "v2Checkpoint",
    "domainMetadata",
    "deletionVectors",
    "vacuumProtocolCheck",
    "timestampNtz",
    // Our files are appended unclustered: they name no clustering provider, and carry stats for the clustering columns.
    "clustering",
    // Each file's rows take fresh ids above the table's high-water mark. An append moves no row, so every id is stable.
    "rowTracking",
    // Columns are widened only at open, and only where the table enables it; earlier type changes are kept.
    "typeWidening",
];

/// The domain that holds a row-tracking table's highest row id.
const ROW_TRACKING: &str = "delta.rowTracking";

/// The schemes of an S3 location.
const S3_SCHEMES: [&str; 2] = ["s3", "s3a"];

/// A string bound in a file's statistics is cut to this many characters, as Delta's own writer cuts it.
const STRING_PREFIX: usize = 32;

/// How many leaf columns take statistics when the table does not say.
const INDEXED_COLUMNS: usize = 32;

/// What a null partition value reads as in a data file's path.
const NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// The destination `config` names. Its table is read, or created from the record when missing, and the record's new
/// columns are added to it before any file is written. Err is transient: the bucket cannot be reached, or the table
/// cannot take the record, which a restart asks again.
pub fn destination(
    config: &DeltaDestinationConfig,
    records: Option<&Records>,
    secrets: &HeldSecrets,
) -> Result<FileDestination, IoError> {
    let (location, grant) = match &config.unity_catalog {
        None => (config.location.clone(), None),
        Some(table) => {
            let (location, grant) = unity_table(config, table, secrets)
                .map_err(|e| IoError::transient(format!("delta table {}: {e}", table.table)))?;
            (location, Some(grant))
        }
    };
    let refused = |why: String| IoError::transient(format!("delta table {location}: {why}"));
    let record = one_record(records).map_err(refused)?;
    let root = Root::parse(&location).map_err(refused)?;
    let runtime = objects::runtime()?;
    let store = objects::s3_bucket(
        &config.storage,
        &root.bucket,
        grant,
        config.tls.as_ref(),
        config.stall_after,
        secrets,
        &runtime,
    )?;
    let log = Log::new(store.clone(), runtime.clone(), config.stall_after, &root.key);
    let unpartitioned_key_bytes = root.key(&format!("{}.parquet", "0".repeat(32))).len();
    let committer = DeltaCommitter::open(log, root, &config.partition_by, record).map_err(refused)?;
    let partitioner = DeltaPartitioner {
        columns: committer.partitions.clone(),
        unpartitioned_key_bytes,
    };
    let file_schema = parquet::Schema::new(record).map_err(refused)?;
    let format = ParquetFormat::new(
        vec![file_schema.with_written(committer.written.clone())],
        config.compression.into(),
    );
    // The protocol names no codec and no row group's size; a table names its files' size.
    let file_bytes = (committer.snapshot.metadata.property("delta.targetFileSize"))
        .and_then(size_bytes)
        .unwrap_or(config.max_object_bytes);
    let files = FileConfig {
        format: Arc::new(format),
        partitioner: Arc::new(partitioner),
        commit_interval: config.file_interval,
        max_file_bytes: file_bytes,
        take_bytes: ROW_GROUP_BYTES,
        buffer_bytes: config.buffer_bytes,
        stall_after: config.stall_after,
        attributes: Attributes::new(),
        parts_in_flight: config.in_flight,
    };
    Ok(FileDestination::new(
        "delta",
        files,
        store,
        runtime,
        Box::new(committer),
    ))
}

/// The root of the Unity Catalog table `table`, and what Unity Catalog vends for its storage, the credentials renewed
/// before they expire. Its client is the provider's own: a put may need fresh credentials at any time.
fn unity_table(
    config: &DeltaDestinationConfig,
    table: &UnityCatalogTable,
    secrets: &HeldSecrets,
) -> Result<(String, objects::CatalogGrant), IoError> {
    let secret = crate::io::held(secrets, &table.client_secret, "the delta destination", "client secret")?;
    // The destination's TLS is for storage: one trusting a private CA would refuse the workspace's certificate.
    let agent = crate::io::http::agent_config(None, secrets)?.new_agent();
    // A sign-in and a call each take at most half a stall, so asking for credentials takes at most one.
    let mut catalog = UnityCatalog::new(
        &table.workspace,
        table.client_id.clone(),
        secret,
        agent,
        config.stall_after / 2,
    );
    let found = catalog.table(&table.table)?;
    let first = catalog.credentials(&found.id)?;
    let access_point = first.access_point.clone();
    let catalog = Mutex::new(catalog);
    let id = found.id;
    let first_access_point = access_point.clone();
    let vended = Vended::new(
        "Unity Catalog".to_string(),
        (first.credential, first.expires),
        move || {
            let mut catalog = catalog.lock().unwrap_or_else(PoisonError::into_inner);
            let grant = catalog.credentials(&id).map_err(|e| e.to_string())?;
            // The store sends every request to the first grant's access point.
            if grant.access_point != first_access_point {
                return Err(format!(
                    "Unity Catalog now vends the table's storage through the access point {:?}, not {:?}, the one this \
                 destination writes through",
                    grant.access_point, first_access_point
                ));
            }
            Ok((grant.credential, grant.expires))
        },
    );
    let grant = objects::CatalogGrant {
        credentials: Arc::new(vended),
        access_point,
    };
    Ok((found.location, grant))
}

/// The table's root: a bucket, and the key under it.
#[derive(Clone, Debug, PartialEq)]
struct Root {
    bucket: String,
    /// Without a slash at either end. Empty for the bucket's root.
    key: String,
}

impl Root {
    fn parse(uri: &str) -> Result<Root, String> {
        let (_, rest) = uri
            .split_once("://")
            .filter(|(scheme, _)| S3_SCHEMES.contains(scheme))
            .ok_or_else(|| format!("its location {uri:?} is not in S3, the one store a delta table is written to"))?;
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(format!("its location {uri:?} names no bucket"));
        }
        Ok(Root {
            bucket: bucket.to_string(),
            key: key.trim_matches('/').to_string(),
        })
    }

    /// The key of `name`, relative to the root.
    fn key(&self, name: &str) -> String {
        if self.key.is_empty() {
            name.to_string()
        } else {
            format!("{}/{name}", self.key)
        }
    }
}

/// A column the table is partitioned by.
#[derive(Clone, Debug, PartialEq)]
struct PartitionColumn {
    name: String,
    physical: String,
    kind: Kind,
}

/// Adds closed files to the table as the log's next commit.
struct DeltaCommitter {
    log: Log,
    root: Root,
    /// The table as last read or committed.
    snapshot: Snapshot,
    /// The table's id when the destination started: one of another id at the location replaced it.
    table_id: String,
    /// The route's record, and the schema the Parquet writer writes it with.
    record: DataType,
    arrow: Arc<ArrowSchema>,
    /// `arrow` as the data files carry it, under the table's physical names.
    written: Arc<ArrowSchema>,
    /// The table's schema as last read.
    schema: StructType,
    partitions: Vec<PartitionColumn>,
    /// The version of this writer's last transaction: each commit it tries carries the next.
    txn_version: i64,
    /// Lost commits, by their transactions' versions.
    lost: Lost<i64>,
    /// The table's version this writer is to checkpoint before its next commit.
    checkpoint_due: bool,
    /// Why the files this destination writes do not fit the table as last read. Each commit reads the log again
    /// until they do.
    unfit: Option<String>,
}

impl Committer for DeltaCommitter {
    fn file_path(&self, partition: &Partition, id: &str) -> String {
        self.root.key(&relative(partition, id))
    }

    fn commit(&mut self, files: &[DataFile]) -> Result<(), CommitError> {
        if files.is_empty() {
            return Ok(());
        }
        if self.checkpoint_due {
            self.checkpoint_due = false;
            match self.checkpoint() {
                Ok(()) => {}
                // A checkpoint is upkeep, and no reason to hold rows back: the log reads without it.
                Err(why) => {
                    tracing::warn!(target: "delta.checkpoint", error = %why, version = self.snapshot.version,
                        "could not checkpoint the table")
                }
            }
        }
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        if self.lost.same_files(&paths) {
            self.advance().map_err(CommitError::Transient)?;
            if self.landed() {
                return Ok(());
            }
        }
        if self.unfit.is_some() {
            self.advance().map_err(CommitError::Transient)?;
        }
        if let Some(why) = &self.unfit {
            return Err(CommitError::Transient(why.clone()));
        }
        self.txn_version += 1;
        let now = now_ms();
        let version = self.snapshot.version + 1;
        let mut high_water_mark = high_water_mark(&self.snapshot).map_err(CommitError::Transient)?;
        let mut commit_info = json!({
            "timestamp": now,
            "operation": "WRITE",
            "operationParameters": {"mode": "Append"},
            "isBlindAppend": true,
            "engineInfo": "smeltr",
        });
        if self.snapshot.metadata.flag("delta.enableRowTracking") == Some(true) {
            commit_info["tags"] = json!({"delta.rowTracking.preserved": "true"});
        }
        let mut lines = vec![
            line("commitInfo", &commit_info),
            line(
                "txn",
                &Txn {
                    app_id: self.snapshot.app_id.clone(),
                    version: self.txn_version,
                    last_updated: Some(now),
                },
            ),
        ];
        for file in files {
            let mut add = self.add(file, now).map_err(CommitError::Transient)?;
            if let Some(high) = &mut high_water_mark {
                add["baseRowId"] = json!(*high + 1);
                add["defaultRowCommitVersion"] = json!(version);
                *high += file.record_count as i64;
            }
            lines.push(line("add", &add));
        }
        if let Some(high) = high_water_mark {
            let domain = DomainMetadata {
                domain: ROW_TRACKING.into(),
                configuration: json!({"rowIdHighWaterMark": high}).to_string(),
                removed: false,
            };
            lines.push(line("domainMetadata", &domain));
        }
        let actions = lines
            .iter()
            .map(|l| Action::parse(l))
            .collect::<Result<Vec<_>, String>>()
            .map_err(CommitError::Transient)?;
        match self.log.create(&commit_name(version), lines.join("\n").into_bytes()) {
            Ok(true) => {
                self.snapshot.committed(actions);
                self.lost.clear();
                self.checkpoint_due = checkpoint::due(&self.snapshot.metadata, version);
                Ok(())
            }
            Ok(false) => {
                // The conflict may be a lost commit of these files that landed late.
                self.advance().map_err(CommitError::Transient)?;
                if self.landed() {
                    return Ok(());
                }
                Err(CommitError::Conflict)
            }
            Err(why) => {
                self.lost.note(&paths, self.txn_version);
                if self.advance().is_ok() && self.landed() {
                    return Ok(());
                }
                Err(CommitError::Unknown(format!(
                    "the commit's answer was lost, and the log is read before committing again: {why}"
                )))
            }
        }
    }
}

impl DeltaCommitter {
    /// The committer of the table whose log is `log`, created from `record` and partitioned by `partition_by` when
    /// missing, the record's new columns added. Err when the table cannot be written.
    fn open(log: Log, root: Root, partition_by: &[String], record: &DataType) -> Result<DeltaCommitter, String> {
        let app_id = uuid();
        let mut snapshot = None;
        let mut failed = None;
        for _ in 0..=MAX_CONFLICT_RETRIES {
            snapshot = Snapshot::load(&log, &app_id)?;
            if snapshot.is_some() {
                break;
            }
            // Taken or lost, the table is read again: whoever made it, it is there.
            failed = log
                .create(&commit_name(0), create(record, partition_by)?.into_bytes())
                .err();
        }
        let mut snapshot = snapshot
            .ok_or_else(|| format!("the table could not be created: {}", failed.clone().unwrap_or_default()))?;
        let mut checkpoint_due = false;
        let mut tries = 0;
        let schema = loop {
            fits(&snapshot)?;
            let current = StructType::parse(&snapshot.metadata.schema_string)?;
            let evolution =
                schema::table_schema(record, Some(&current), mapping(&snapshot.metadata)?, widens(&snapshot))?;
            if !evolution.changed {
                break current;
            }
            if tries > MAX_CONFLICT_RETRIES {
                return Err(format!(
                    "the record's new or wider columns could not be committed: {}",
                    failed.unwrap_or_default()
                ));
            }
            tries += 1;
            let mut metadata = snapshot.metadata.clone();
            metadata.schema_string = evolution.schema.to_json();
            if let Mapping::Name { max_column_id } = evolution.mapping {
                (metadata.configuration).insert("delta.columnMapping.maxColumnId".into(), max_column_id.to_string());
            }
            let version = snapshot.version + 1;
            let body = [
                line(
                    "commitInfo",
                    &json!({"timestamp": now_ms(), "operation": "UPDATE SCHEMA", "engineInfo": "smeltr"}),
                ),
                line("metaData", &metadata),
            ]
            .join("\n");
            // Taken or lost, the log is read again, and the columns are added or widened if they still need it.
            match log.create(&commit_name(version), body.into_bytes()) {
                Ok(true) => {
                    snapshot.committed(vec![Action::Metadata(Box::new(metadata))]);
                    checkpoint_due = checkpoint::due(&snapshot.metadata, version);
                }
                taken_or_lost => {
                    failed = taken_or_lost.err();
                    snapshot.advance(&log)?;
                }
            }
        };
        if let Some(why) = schema::refused_column(&schema) {
            return Err(why);
        }
        let partitions = partition_columns(&snapshot.metadata, &schema)?;
        let arrow = parquet::Schema::new(record)?.arrow().clone();
        let written = schema::physical(&arrow, &schema);
        Ok(DeltaCommitter {
            log,
            root,
            table_id: snapshot.metadata.id.clone(),
            snapshot,
            record: record.clone(),
            arrow,
            written,
            schema,
            partitions,
            txn_version: 0,
            lost: Lost::none(),
            checkpoint_due,
            unfit: None,
        })
    }

    /// Reads the commits since, and notes whether the files this destination writes still fit the table.
    fn advance(&mut self) -> Result<(), String> {
        if self.snapshot.advance(&self.log)? > 0 {
            self.unfit = self.fit().err();
        }
        Ok(())
    }

    /// Takes the table as last read when the files this destination writes fit it: its columns hold the record's
    /// fields under the physical names the files carry, and its partitioning is the one the files are written by.
    /// Columns another writer added, and features this writer supports, change nothing. Err names what does not fit.
    fn fit(&mut self) -> Result<(), String> {
        fits(&self.snapshot)?;
        let metadata = &self.snapshot.metadata;
        if metadata.id != self.table_id {
            return Err("another table replaced the one at its location".to_string());
        }
        let schema = StructType::parse(&metadata.schema_string)?;
        if let Some(why) = schema::refused_column(&schema) {
            return Err(why);
        }
        let evolution = schema::table_schema(&self.record, Some(&schema), mapping(metadata)?, None)?;
        if evolution.changed {
            return Err("another writer dropped a column the record writes".to_string());
        }
        if schema::physical(&self.arrow, &schema) != self.written {
            return Err("another writer changed a column the record writes".to_string());
        }
        if partition_columns(metadata, &schema)? != self.partitions {
            return Err("another writer changed the table's partitioning".to_string());
        }
        self.schema = schema;
        Ok(())
    }

    /// Checkpoints the table at this version, on the newest checkpoint before it, unless another writer has one at it
    /// or after.
    fn checkpoint(&mut self) -> Result<(), String> {
        if !self.snapshot.rebase(&self.log)? {
            return Ok(());
        }
        let written = checkpoint::write(&self.log, &self.snapshot)?;
        self.snapshot.checkpointed(written);
        Ok(())
    }

    /// Whether a lost commit is in the log as last read.
    fn landed(&mut self) -> bool {
        let ours = self.snapshot.our_version();
        self.lost.landed(|version| ours == Some(*version))
    }

    /// `file`'s add action.
    fn add(&self, file: &DataFile, now: i64) -> Result<Value, String> {
        let partition_values: Map<String, Value> = (file.partition.values.iter())
            .map(|(physical, text)| {
                let value = if text.is_empty() { Value::Null } else { json!(text) };
                (physical.clone(), value)
            })
            .collect();
        let relative = file
            .path
            .strip_prefix(&self.root.key(""))
            .ok_or_else(|| format!("{} is not under the table's root", file.path))?
            .trim_start_matches('/');
        let mut add = json!({
            "path": uri_path(relative),
            "partitionValues": partition_values,
            "size": file.size_bytes,
            "modificationTime": now,
            "dataChange": true,
        });
        if let Some(metadata) = file.metadata.as_deref() {
            let clustering = clustering_columns(&self.snapshot)?;
            let stats = stats(
                metadata,
                &self.schema,
                &self.snapshot.metadata,
                &clustering,
                file.record_count,
            );
            add["stats"] = json!(stats.to_string());
        }
        Ok(add)
    }
}

/// Commit 0 of a table of `record`, partitioned by `partition_by`: a plain table, which every Delta reader opens. Its
/// files name their columns as the record does.
fn create(record: &DataType, partition_by: &[String]) -> Result<String, String> {
    let evolution = schema::table_schema(record, None, Mapping::None, None)?;
    for name in partition_by {
        written_partition_column(&evolution.schema, name)?;
    }
    // A table no data file of the record fits is refused before it is made, not after.
    if let Some(why) = schema::refused_column(&evolution.schema) {
        return Err(why);
    }
    parquet::Schema::new(record)?;
    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
        reader_features: None,
        writer_features: None,
    };
    let now = now_ms();
    let metadata = Metadata {
        id: uuid(),
        name: None,
        description: None,
        format: Format {
            provider: "parquet".to_string(),
            options: Default::default(),
        },
        schema_string: evolution.schema.to_json(),
        partition_columns: partition_by.to_vec(),
        created_time: Some(now),
        configuration: Default::default(),
        rest: Map::new(),
    };
    Ok([
        line(
            "commitInfo",
            &json!({"timestamp": now, "operation": "CREATE TABLE", "engineInfo": "smeltr"}),
        ),
        line("protocol", &protocol),
        line("metaData", &metadata),
    ]
    .join("\n"))
}

/// Err names what keeps this writer from writing the table: a protocol or a feature it does not support, or
/// constraints it cannot check.
fn fits(snapshot: &Snapshot) -> Result<(), String> {
    let protocol = &snapshot.protocol;
    let features = protocol.writer_features.as_deref().unwrap_or_default();
    if protocol.min_writer_version > 7 {
        return Err(format!(
            "the table needs writer version {}",
            protocol.min_writer_version
        ));
    }
    if protocol.min_reader_version > 3 {
        return Err(format!(
            "the table needs reader version {}",
            protocol.min_reader_version
        ));
    }
    if let Some(feature) = features.iter().find(|f| !WRITER_FEATURES.contains(&f.as_str())) {
        return Err(format!(
            "the table needs the writer feature {feature}, which this writer does not support"
        ));
    }
    let metadata = &snapshot.metadata;
    if metadata.format.provider != "parquet" {
        return Err(format!(
            "the table's files are {}, not Parquet",
            metadata.format.provider
        ));
    }
    if let Some(key) = metadata
        .configuration
        .keys()
        .find(|k| k.starts_with("delta.constraints."))
    {
        return Err(format!(
            "the table has the check constraint {key}, which this writer does not check"
        ));
    }
    // This writer's checkpoints keep stats as JSON only, which is what the protocol's defaults ask for.
    for (key, default) in [
        ("delta.checkpoint.writeStatsAsJson", "true"),
        ("delta.checkpoint.writeStatsAsStruct", "false"),
    ] {
        let value = metadata.property(key).unwrap_or(default);
        if !value.eq_ignore_ascii_case(default) {
            return Err(format!(
                "the table sets {key} to {value}, and this writer's checkpoints keep stats as JSON only"
            ));
        }
    }
    mapping(metadata).map(|_| ())
}

/// The table's column mapping. Err for mapping by id, which this writer does not write.
fn mapping(metadata: &Metadata) -> Result<Mapping, String> {
    match metadata.property("delta.columnMapping.mode").unwrap_or("none") {
        "none" => Ok(Mapping::None),
        "name" => {
            let max_column_id = metadata
                .property("delta.columnMapping.maxColumnId")
                .and_then(|v| v.parse().ok())
                .ok_or("the table maps columns by name and has no delta.columnMapping.maxColumnId")?;
            Ok(Mapping::Name { max_column_id })
        }
        other => Err(format!("the table maps columns by {other}, which this writer does not")),
    }
}

/// The table's partition columns, each a top-level column of a type this writer writes a partition value of.
fn partition_columns(metadata: &Metadata, schema: &StructType) -> Result<Vec<PartitionColumn>, String> {
    (metadata.partition_columns.iter())
        .map(|name| {
            let (physical, kind) = written_partition_column(schema, name)?;
            Ok(PartitionColumn {
                name: name.clone(),
                physical,
                kind,
            })
        })
        .collect()
}

/// `name`'s physical name and kind, where it is a partition column this writer writes values of.
fn written_partition_column(schema: &StructType, name: &str) -> Result<(String, Kind), String> {
    let (physical, kind) = schema::partition_column(schema, name)?;
    if kind == Kind::Binary {
        return Err(format!(
            "the table is partitioned by {name}, a {kind} column, which this writer does not write"
        ));
    }
    Ok((physical, kind))
}

/// A data file's path under the table's root: Hive-style partition directories, then the file.
fn relative(partition: &Partition, id: &str) -> String {
    let mut name = partition.path.join("/");
    if !name.is_empty() {
        name.push('/');
    }
    name.push_str(id);
    name.push_str(".parquet");
    name
}

/// `path` as an add action names it: a URI path, so a `%` the directory names carry is escaped again.
fn uri_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for &b in path.as_bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'_' | b'~' | b'/' | b'=' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Partitions rows by the table's partition columns: one directory per column, the value in the log as Delta writes
/// it, an empty string for null.
struct DeltaPartitioner {
    columns: Vec<PartitionColumn>,
    /// The length of a data file's key with no partition directories.
    unpartitioned_key_bytes: usize,
}

impl Partitioner for DeltaPartitioner {
    fn partition(&self, row: &OwnedRecord) -> Result<Partition, String> {
        if self.columns.is_empty() {
            return Ok(Partition {
                part: 0,
                path: Vec::new(),
                values: Vec::new(),
            });
        }
        let columns = row
            .columns
            .as_ref()
            .ok_or("the row has no columns, and a table is partitioned by its columns")?;
        let batch = columns.chunk.batch();
        let at = columns.row as usize;
        let mut path = Vec::with_capacity(self.columns.len());
        let mut values = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let names = [column.name.clone()];
            let value = column_at(batch, &names, at)?
                .map(|c| read(&c, at, &column.kind, &names))
                .transpose()?
                .flatten();
            let text = value
                .as_ref()
                .map(|v| partition_text(v, &column.kind))
                .unwrap_or_default();
            let segment = if text.is_empty() {
                NULL_PARTITION.to_string()
            } else {
                url_encode(&text)
            };
            path.push(format!("{}={segment}", url_encode(&column.name)));
            values.push((column.physical.clone(), text));
        }
        let key_bytes = self.unpartitioned_key_bytes + path.iter().map(|p| p.len() + 1).sum::<usize>();
        if key_bytes > objects::MAX_KEY_BYTES {
            return Err(format!(
                "the row's partition values make a key of {key_bytes} bytes, past the {} a key may hold",
                objects::MAX_KEY_BYTES
            ));
        }
        Ok(Partition { part: 0, path, values })
    }
}

/// Bytes `value` says, as Delta writes a size: `104857600`, `100mb`. None for none, or nothing.
fn size_bytes(value: &str) -> Option<usize> {
    let value = value.trim().to_ascii_lowercase();
    let digits = value.find(|c: char| !c.is_ascii_digit()).unwrap_or(value.len());
    let unit = match value[digits..].trim() {
        "" | "b" => 1,
        "k" | "kb" => 1 << 10,
        "m" | "mb" => 1 << 20,
        "g" | "gb" => 1 << 30,
        _ => return None,
    };
    value[..digits]
        .parse::<usize>()
        .ok()?
        .checked_mul(unit)
        .filter(|&n| n > 0)
}

/// A partition value of a `kind` column as the log holds it. A timestamp is in UTC, as ISO 8601 writes it; a time
/// without zone is its wall clock, as the protocol writes it.
fn partition_text(value: &Literal, kind: &Kind) -> String {
    match value {
        Literal::Boolean(b) => b.to_string(),
        Literal::Int(v) => v.to_string(),
        Literal::Long(v) => v.to_string(),
        // As Java writes it, which is how Spark and the kernel read it back.
        Literal::Double(v) if v.is_infinite() => if *v > 0.0 { "Infinity" } else { "-Infinity" }.to_string(),
        Literal::Double(v) if *kind == Kind::Float => (*v as f32).to_string(),
        Literal::Double(v) => v.to_string(),
        Literal::Date(d) => iso_date(*d as i64),
        Literal::Micros(us) if *kind == Kind::Timestamp => wall_clock(*us, 6, ' '),
        Literal::Micros(us) => timestamp(*us, 6),
        Literal::String(s) => s.clone(),
        Literal::Binary(_) => unreachable!("a binary partition column is refused at start"),
    }
}

/// `micros` since the epoch as ISO 8601 in UTC, with `digits` of the second's fraction: 3 or 6.
fn timestamp(micros: i64, digits: u32) -> String {
    format!("{}Z", wall_clock(micros, digits, 'T'))
}

/// `micros` since the epoch as the date and the time of day in UTC, with no zone, `separator` between them.
fn wall_clock(micros: i64, digits: u32, separator: char) -> String {
    let (day, time) = (micros.div_euclid(MICROS_PER_DAY), micros.rem_euclid(MICROS_PER_DAY));
    let secs = time / 1_000_000;
    let fraction = time % 1_000_000 / 10i64.pow(6 - digits);
    format!(
        "{}{separator}{:02}:{:02}:{:02}.{fraction:0width$}",
        iso_date(day),
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        width = digits as usize
    )
}

/// The table's partition columns when it lets a writer widen its columns, as `schema::table_schema` takes them.
fn widens(snapshot: &Snapshot) -> Option<&[String]> {
    let supported = (snapshot.protocol.writer_features.iter().flatten()).any(|f| f == "typeWidening");
    let enabled = snapshot.metadata.flag("delta.enableTypeWidening") == Some(true);
    (supported && enabled).then_some(snapshot.metadata.partition_columns.as_slice())
}

/// The highest row id a row-tracking table has given, -1 when none; None when the table tracks no rows or suspends it.
fn high_water_mark(snapshot: &Snapshot) -> Result<Option<i64>, String> {
    let tracked = (snapshot.protocol.writer_features.iter().flatten()).any(|f| f == "rowTracking");
    if !tracked || snapshot.metadata.flag("delta.rowTrackingSuspended") == Some(true) {
        return Ok(None);
    }
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RowTracking {
        row_id_high_water_mark: i64,
    }
    let domain = snapshot.domain::<RowTracking>(ROW_TRACKING)?;
    Ok(Some(domain.map_or(-1, |r| r.row_id_high_water_mark)))
}

/// The physical paths of the columns the table is clustered by, from its `delta.clustering` domain; none when it is
/// not clustered.
fn clustering_columns(snapshot: &Snapshot) -> Result<Vec<Vec<String>>, String> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Clustering {
        clustering_columns: Vec<Vec<String>>,
    }
    let domain = snapshot.domain::<Clustering>("delta.clustering")?;
    Ok(domain.map_or_else(Vec::new, |c| c.clustering_columns))
}

/// A data file's statistics as an add action carries them: its row count, and each leaf column's null count and
/// bounds, keyed by physical names. A leaf inside an array or a map has none, nor one past the table's indexed columns
/// unless the table is clustered by it, as the protocol asks.
fn stats(
    file: &ParquetMetaData,
    schema: &StructType,
    metadata: &Metadata,
    clustering: &[Vec<String>],
    rows: u64,
) -> Value {
    let indexed = match metadata
        .property("delta.dataSkippingNumIndexedCols")
        .map(str::parse::<i64>)
    {
        Some(Ok(n)) if n < 0 => usize::MAX,
        Some(Ok(n)) => n as usize,
        _ => INDEXED_COLUMNS,
    };
    let named: Option<Vec<String>> = metadata
        .property("delta.dataSkippingStatsColumns")
        .map(|v| v.split(',').map(|c| c.trim().to_string()).collect());
    let (mut nulls, mut mins, mut maxes) = (Map::new(), Map::new(), Map::new());
    let mut leaves = 0usize;
    let descr = file.file_metadata().schema_descr();
    for i in 0..descr.num_columns() {
        let parts = descr.column(i).path().parts().to_vec();
        let Some((names, kind)) = leaf(schema, &parts) else {
            continue;
        };
        // A partition column's value is in the add's partition values, so it takes no stats nor an indexed place.
        if names.len() == 1 && metadata.partition_columns.contains(&names[0]) {
            continue;
        }
        let taken = match &named {
            Some(named) => named
                .iter()
                .any(|n| *n == names.join(".") || names.join(".").starts_with(&format!("{n}."))),
            None => leaves < indexed,
        } || clustering.contains(&parts);
        leaves += 1;
        if !taken {
            continue;
        }
        let stats = column_stats(file, i, kind.as_ref());
        if let Some(n) = stats.nulls {
            insert(&mut nulls, &parts, json!(n));
        }
        let Some((low, high)) = stats.bounds else {
            continue;
        };
        if let Some(low) = kind.as_ref().and_then(|k| bound(k, &low, false)) {
            insert(&mut mins, &parts, low);
        }
        if let Some(high) = kind.as_ref().and_then(|k| bound(k, &high, true)) {
            insert(&mut maxes, &parts, high);
        }
    }
    json!({
        "numRecords": rows,
        "minValues": mins,
        "maxValues": maxes,
        "nullCount": nulls,
    })
}

/// The table column at physical path `parts`: its logical names, and its kind when it takes bounds. None when it is
/// no column of the table's, or is within an array or a map.
fn leaf(schema: &StructType, parts: &[String]) -> Option<(Vec<String>, Option<Kind>)> {
    let mut fields: &[StructField] = &schema.fields;
    let mut names = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        let field = fields.iter().find(|f| f.physical_name() == part)?;
        names.push(field.name.clone());
        match &field.data_type {
            Type::Struct(s) if i + 1 < parts.len() => fields = &s.fields,
            Type::Primitive(p) if i + 1 == parts.len() => {
                let kind = schema::kind(p).filter(|k| !matches!(k, Kind::Boolean | Kind::Binary));
                return Some((names, kind));
            }
            _ => return None,
        }
    }
    None
}

/// A footer bound as the statistics hold it: a timestamp cut to milliseconds, a string to its prefix. None when it
/// cannot be held: a float JSON has no number for, or a string max longer than the prefix.
fn bound(kind: &Kind, bound: &crate::io::table::Bound, max: bool) -> Option<Value> {
    use crate::io::table::Bound as B;
    Some(match (kind, bound) {
        (Kind::Byte | Kind::Short | Kind::Int | Kind::Long, B::Int(v)) => json!(v),
        (Kind::Int | Kind::Long, B::Long(v)) => json!(v),
        (Kind::Float | Kind::Double, B::Float(v)) => serde_json::Number::from_f64(*v as f64).map(Value::Number)?,
        (Kind::Float | Kind::Double, B::Double(v)) => serde_json::Number::from_f64(*v).map(Value::Number)?,
        (Kind::Date, B::Int(d)) => json!(iso_date(*d as i64)),
        (Kind::Timestamp, B::Long(us)) => json!(wall_clock(us.div_euclid(1000).checked_mul(1000)?, 3, 'T')),
        (Kind::Timestamptz, B::Long(us)) => json!(timestamp(us.div_euclid(1000).checked_mul(1000)?, 3)),
        (Kind::String, B::Bytes(b)) => {
            let text = std::str::from_utf8(b).ok()?;
            match text.char_indices().nth(STRING_PREFIX) {
                None => json!(text),
                Some(_) if max => return None,
                Some((cut, _)) => json!(&text[..cut]),
            }
        }
        _ => return None,
    })
}

/// Sets `value` at `path` in `object`, making the objects on the way.
fn insert(object: &mut Map<String, Value>, path: &[String], value: Value) {
    let (last, parents) = path.split_last().expect("a leaf has a name");
    let mut at = object;
    for part in parents {
        at = at
            .entry(part.clone())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("a struct's statistics are an object");
    }
    at.insert(last.clone(), value);
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fmt;
    use std::ops::Range;
    use std::sync::Mutex;
    use std::time::Duration;

    use ::parquet::arrow::ArrowWriter;
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{
        ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType as Arrow, Field, Fields, Schema as ArrowSchema, TimeUnit};
    use futures_util::StreamExt;
    use futures_util::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
        PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use tokio_util::bytes::Bytes;

    use super::*;
    use crate::core::buffer::BufferPool;
    use crate::core::column::{Column, StrBuilder, VarType};
    use crate::format::record::RecordView;
    use crate::io::parquet::{Chunk, Columns};

    /// 2017-11-16T22:31:08 UTC.
    const SPEC_TIME: i64 = 1_510_871_468_000_000;

    /// What goes wrong with the store's next create.
    #[derive(Clone, Copy, Debug)]
    pub(super) enum Fault {
        /// It is written, and its answer is an error.
        Lost,
        /// It is not written, and its answer is an error.
        Dropped,
        /// Its answer is an error, and it is written only when the test lands it.
        Late,
    }

    /// An in-memory bucket whose creates fail as armed.
    #[derive(Debug, Default)]
    pub(super) struct Faulty {
        pub(super) inner: InMemory,
        faults: Mutex<VecDeque<Fault>>,
        pub(super) late: Mutex<Option<(Path, PutPayload)>>,
    }

    impl fmt::Display for Faulty {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Faulty({})", self.inner)
        }
    }

    fn lost_answer() -> object_store::Error {
        object_store::Error::Generic {
            store: "Faulty",
            source: "the connection reset before the answer".into(),
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Faulty {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let fault = if matches!(opts.mode, PutMode::Create) {
                self.faults.lock().unwrap().pop_front()
            } else {
                None
            };
            match fault {
                None => self.inner.put_opts(location, payload, opts).await,
                Some(Fault::Lost) => {
                    self.inner.put_opts(location, payload, opts).await?;
                    Err(lost_answer())
                }
                Some(Fault::Dropped) => Err(lost_answer()),
                Some(Fault::Late) => {
                    *self.late.lock().unwrap() = Some((location.clone(), payload));
                    Err(lost_answer())
                }
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A table at `tbl/` in a bucket of its own.
    pub(super) struct Table {
        pub(super) store: Arc<Faulty>,
        pub(super) runtime: Arc<tokio::runtime::Runtime>,
    }

    impl Table {
        pub(super) fn new() -> Table {
            Table {
                store: Arc::new(Faulty::default()),
                runtime: objects::runtime().unwrap(),
            }
        }

        pub(super) fn log(&self) -> Log {
            Log::new(self.store.clone(), self.runtime.clone(), Duration::from_secs(10), "tbl")
        }

        pub(super) fn open(&self, record: &DataType, partition_by: &[&str]) -> Result<DeltaCommitter, String> {
            let root = Root {
                bucket: "b".into(),
                key: "tbl".into(),
            };
            let partition_by: Vec<String> = partition_by.iter().map(|p| p.to_string()).collect();
            DeltaCommitter::open(self.log(), root, &partition_by, record)
        }

        fn refused(&self, record: &DataType, partition_by: &[&str]) -> String {
            match self.open(record, partition_by) {
                Ok(_) => panic!("the table was opened"),
                Err(why) => why,
            }
        }

        pub(super) fn arm(&self, fault: Fault) {
            self.store.faults.lock().unwrap().push_back(fault);
        }

        /// Writes the late create, where none is.
        fn land(&self) {
            let (path, payload) = self.store.late.lock().unwrap().take().expect("a late create");
            self.runtime
                .block_on(self.store.inner.put_opts(&path, payload, PutMode::Create.into()))
                .unwrap();
        }

        /// Commit `version`'s actions; None when there is none.
        fn commit(&self, version: i64) -> Option<Vec<Value>> {
            let path = Path::from(format!("tbl/_delta_log/{}", commit_name(version)));
            let bytes = self.runtime.block_on(async {
                match self.store.inner.get(&path).await {
                    Ok(got) => Some(got.bytes().await.unwrap()),
                    Err(object_store::Error::NotFound { .. }) => None,
                    Err(e) => panic!("{e}"),
                }
            })?;
            let text = std::str::from_utf8(&bytes).unwrap();
            Some(text.lines().map(|l| serde_json::from_str(l).unwrap()).collect())
        }

        /// Another writer's commit `version` of `actions`.
        fn put(&self, version: i64, actions: &[Value]) {
            let body = actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n");
            assert!(self.log().create(&commit_name(version), body.into_bytes()).unwrap());
        }

        /// The names under `_delta_log/`.
        fn names(&self) -> Vec<String> {
            let prefix = Path::from("tbl/_delta_log");
            self.runtime.block_on(async {
                let mut names = Vec::new();
                let mut listing = self.store.inner.list(Some(&prefix));
                while let Some(meta) = listing.next().await {
                    names.push(meta.unwrap().location.as_ref()["tbl/_delta_log/".len()..].to_string());
                }
                names.sort();
                names
            })
        }
    }

    fn st(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect(), false)
    }

    fn action<'a>(actions: &'a [Value], key: &str) -> &'a Value {
        actions
            .iter()
            .find_map(|a| a.get(key))
            .unwrap_or_else(|| panic!("no {key} in {actions:?}"))
    }

    fn adds(actions: &[Value]) -> Vec<&Value> {
        actions.iter().filter_map(|a| a.get("add")).collect()
    }

    fn physical(schema: &StructType, name: &str) -> String {
        let field = schema.fields.iter().find(|f| f.name == name).unwrap();
        field.physical_name().to_string()
    }

    /// A file of `committer`'s in the partition of `path` and `values`, the values keyed by column name.
    fn file(
        committer: &DeltaCommitter,
        path: &[&str],
        values: &[(&str, &str)],
        metadata: Option<Arc<ParquetMetaData>>,
    ) -> DataFile {
        let partition = Partition {
            part: 0,
            path: path.iter().map(|s| s.to_string()).collect(),
            values: values
                .iter()
                .map(|(n, v)| (physical(&committer.schema, n), v.to_string()))
                .collect(),
        };
        DataFile {
            path: committer.file_path(&partition, &objects::object_id()),
            partition,
            size_bytes: 100,
            record_count: 2,
            metadata,
        }
    }

    /// The footer of a Parquet file of `columns`.
    fn footer(fields: Vec<Field>, columns: Vec<ArrayRef>) -> Arc<ParquetMetaData> {
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        Arc::new(writer.close().unwrap())
    }

    fn metadata(schema: &StructType, configuration: &[(&str, &str)], partition_by: &[&str]) -> Metadata {
        Metadata {
            id: "id".into(),
            name: None,
            description: None,
            format: Format {
                provider: "parquet".into(),
                options: Default::default(),
            },
            schema_string: schema.to_json(),
            partition_columns: partition_by.iter().map(|p| p.to_string()).collect(),
            created_time: None,
            configuration: (configuration.iter())
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            rest: Map::new(),
        }
    }

    fn new_schema(record: &DataType) -> StructType {
        schema::table_schema(record, None, Mapping::Name { max_column_id: 0 }, None)
            .unwrap()
            .schema
    }

    #[test]
    fn a_missing_table_is_created_plain_with_its_partition_columns() {
        let table = Table::new();
        let record = st(&[("host", DataType::Str), ("n", DataType::I64)]);
        let committer = table.open(&record, &["host"]).unwrap();
        assert_eq!(committer.snapshot.version, 0);

        let actions = table.commit(0).unwrap();
        assert_eq!(action(&actions, "commitInfo")["operation"], "CREATE TABLE");
        assert_eq!(
            *action(&actions, "protocol"),
            json!({"minReaderVersion": 1, "minWriterVersion": 2})
        );
        let meta = action(&actions, "metaData");
        assert_eq!(meta["format"]["provider"], "parquet");
        assert_eq!(meta["partitionColumns"], json!(["host"]));
        assert_eq!(meta["configuration"], json!({}));

        let schema = StructType::parse(meta["schemaString"].as_str().unwrap()).unwrap();
        assert_eq!(
            schema.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            ["host", "n"]
        );
        for field in &schema.fields {
            assert!(field.metadata.is_empty(), "{field:?}");
        }
        assert_eq!(committer.schema, schema);
        assert_eq!(committer.partitions.len(), 1);
        assert_eq!(committer.partitions[0].physical, "host");
        assert_eq!(committer.written, committer.arrow);
        assert!(table.commit(1).is_none());
    }

    #[test]
    fn a_table_partitioned_by_a_column_this_writer_cannot_write_is_not_created() {
        let table = Table::new();
        let record = st(&[("raw", DataType::Bytes), ("n", DataType::I64)]);
        let error = table.open(&record, &["raw"]).err().unwrap();
        assert!(error.contains("partitioned by raw"), "{error}");
        assert!(table.commit(0).is_none());
    }

    #[test]
    fn a_record_with_a_new_field_adds_its_column_in_a_metadata_commit_and_an_unchanged_one_commits_nothing() {
        let table = Table::new();
        table.open(&st(&[("a", DataType::Str)]), &[]).unwrap();
        let wider = st(&[("a", DataType::Str), ("b", DataType::I64)]);
        let committer = table.open(&wider, &[]).unwrap();
        assert_eq!(committer.snapshot.version, 1);

        let actions = table.commit(1).unwrap();
        assert_eq!(action(&actions, "commitInfo")["operation"], "UPDATE SCHEMA");
        assert!(adds(&actions).is_empty());
        let meta = action(&actions, "metaData");
        let schema = StructType::parse(meta["schemaString"].as_str().unwrap()).unwrap();
        assert_eq!(
            schema.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(committer.schema, schema);

        let again = table.open(&wider, &[]).unwrap();
        assert_eq!(again.snapshot.version, 1);
        assert!(table.commit(2).is_none());
    }

    /// A table whose commit 0 is this writer's own, changed by `change`.
    fn table_with(record: &DataType, partition_by: &[&str], change: impl Fn(&mut Value)) -> Table {
        let table = Table::new();
        let partition_by: Vec<String> = partition_by.iter().map(|p| p.to_string()).collect();
        let mut actions: Vec<Value> = create(record, &partition_by)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        actions.iter_mut().for_each(&change);
        table.put(0, &actions);
        table
    }

    #[test]
    fn a_table_this_writer_cannot_write_is_refused() {
        let record = st(&[("a", DataType::Str), ("raw", DataType::Bytes)]);
        let protocol = |key: &'static str, features: Value| {
            move |a: &mut Value| {
                if let Some(p) = a.get_mut("protocol") {
                    p[key] = features.clone();
                }
            }
        };
        let configured = |key: &'static str, value: &'static str| {
            move |a: &mut Value| {
                if let Some(m) = a.get_mut("metaData") {
                    m["configuration"][key] = json!(value);
                }
            }
        };

        let why = table_with(&record, &[], protocol("minWriterVersion", json!(8))).refused(&record, &[]);
        assert!(why.contains("needs writer version 8"), "{why}");
        let why = table_with(&record, &[], |a| {
            protocol("minWriterVersion", json!(7))(a);
            protocol("writerFeatures", json!(["v2Checkpoint", "identityColumns"]))(a);
        })
        .refused(&record, &[]);
        assert!(why.contains("writer feature identityColumns"), "{why}");
        // A V2 table is written classic checkpoints, which the protocol always allows.
        table_with(&record, &[], |a| {
            protocol("minReaderVersion", json!(3))(a);
            protocol("minWriterVersion", json!(7))(a);
            protocol("readerFeatures", json!(["v2Checkpoint"]))(a);
            protocol("writerFeatures", json!(["v2Checkpoint"]))(a);
        })
        .open(&record, &[])
        .unwrap();
        let why = table_with(&record, &[], configured("delta.constraints.positive", "a > 0")).refused(&record, &[]);
        assert!(why.contains("check constraint delta.constraints.positive"), "{why}");
        let why = table_with(&record, &[], configured("delta.columnMapping.mode", "id")).refused(&record, &[]);
        assert!(why.contains("maps columns by id"), "{why}");
        let why =
            table_with(&record, &[], configured("delta.checkpoint.writeStatsAsStruct", "true")).refused(&record, &[]);
        assert!(why.contains("writeStatsAsStruct to true"), "{why}");
        let why =
            table_with(&record, &[], configured("delta.checkpoint.writeStatsAsJson", "false")).refused(&record, &[]);
        assert!(why.contains("writeStatsAsJson to false"), "{why}");
        let why = table_with(&record, &[], |a| {
            if let Some(m) = a.get_mut("metaData") {
                m["format"]["provider"] = json!("orc");
            }
        })
        .refused(&record, &[]);
        assert!(why.contains("orc, not Parquet"), "{why}");
        let why = Table::new().refused(&record, &["raw"]);
        assert!(why.contains("partitioned by raw, a binary column"), "{why}");
    }

    #[test]
    fn a_table_that_enables_type_widening_has_a_narrower_column_widened_at_open() {
        let record = st(&[("n", DataType::I64)]);
        let narrow = |widening: &'static str| {
            move |a: &mut Value| {
                if let Some(p) = a.get_mut("protocol") {
                    *p = json!({"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["typeWidening"],
                                "writerFeatures": ["typeWidening"]});
                }
                if let Some(m) = a.get_mut("metaData") {
                    let schema = json!({"type": "struct", "fields": [
                        {"name": "n", "type": "integer", "nullable": true, "metadata": {}}]});
                    m["schemaString"] = json!(schema.to_string());
                    m["configuration"] = json!({"delta.enableTypeWidening": widening});
                }
            }
        };

        let table = table_with(&record, &[], narrow("false"));
        let kept = table.open(&record, &[]).unwrap();
        assert_eq!(kept.written.field(0).data_type(), &arrow_schema::DataType::Int32);
        assert!(
            table.commit(1).is_none(),
            "a table that does not widen is left as it is"
        );

        let table = table_with(&record, &[], narrow("true"));
        let widened = table.open(&record, &[]).unwrap();
        assert_eq!(widened.written.field(0).data_type(), &arrow_schema::DataType::Int64);
        let meta = action(&table.commit(1).unwrap(), "metaData").clone();
        let schema = StructType::parse(meta["schemaString"].as_str().unwrap()).unwrap();
        assert_eq!(schema.fields[0].data_type, Type::Primitive("long".into()));
        assert_eq!(
            schema.fields[0].metadata["delta.typeChanges"],
            json!([{"fromType": "integer", "toType": "long"}])
        );
    }

    /// A row-tracking table whose rows are given up to id 9, and that sets `configuration`.
    fn row_tracking_table(record: &DataType, configuration: Value) -> Table {
        let table = Table::new();
        let mut actions: Vec<Value> = (create(record, &[]).unwrap().lines())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        for action in &mut actions {
            if let Some(p) = action.get_mut("protocol") {
                *p = json!({"minReaderVersion": 1, "minWriterVersion": 7,
                            "writerFeatures": ["domainMetadata", "rowTracking"]});
            }
            if let Some(m) = action.get_mut("metaData") {
                m["configuration"] = configuration.clone();
            }
        }
        actions.push(json!({"domainMetadata": {"domain": ROW_TRACKING,
                                               "configuration": "{\"rowIdHighWaterMark\":9}", "removed": false}}));
        table.put(0, &actions);
        table
    }

    #[test]
    fn a_row_tracking_tables_new_rows_take_fresh_ids_above_its_high_water_mark() {
        let record = st(&[("host", DataType::Str)]);
        let table = row_tracking_table(&record, json!({"delta.enableRowTracking": "true"}));
        let mut committer = table.open(&record, &[]).unwrap();
        let files = [file(&committer, &[], &[], None), file(&committer, &[], &[], None)];
        committer.commit(&files).unwrap();

        let actions = table.commit(1).unwrap();
        let rows: Vec<(&Value, &Value)> = (adds(&actions).into_iter())
            .map(|a| (&a["baseRowId"], &a["defaultRowCommitVersion"]))
            .collect();
        assert_eq!(rows, [(&json!(10), &json!(1)), (&json!(12), &json!(1))]);
        let domain = action(&actions, "domainMetadata");
        assert_eq!(domain["domain"], ROW_TRACKING);
        assert_eq!(domain["configuration"], "{\"rowIdHighWaterMark\":13}");
        assert_eq!(
            action(&actions, "commitInfo")["tags"]["delta.rowTracking.preserved"],
            "true"
        );
    }

    #[test]
    fn a_table_that_suspends_row_tracking_is_given_no_row_ids() {
        let record = st(&[("host", DataType::Str)]);
        let table = row_tracking_table(&record, json!({"delta.rowTrackingSuspended": "true"}));
        let mut committer = table.open(&record, &[]).unwrap();
        committer.commit(&[file(&committer, &[], &[], None)]).unwrap();

        let actions = table.commit(1).unwrap();
        assert_eq!(adds(&actions)[0].get("baseRowId"), None);
        assert!(!actions.iter().any(|a| a.get("domainMetadata").is_some()));
        assert_eq!(action(&actions, "commitInfo").get("tags"), None);
    }

    #[test]
    fn a_table_that_maps_its_columns_by_name_has_them_written_under_their_physical_names() {
        let record = st(&[("host", DataType::Str), ("n", DataType::I64)]);
        let mapped = schema::table_schema(&record, None, Mapping::Name { max_column_id: 0 }, None)
            .unwrap()
            .schema;
        let table = table_with(&record, &["host"], |a| {
            if let Some(p) = a.get_mut("protocol") {
                *p = json!({"minReaderVersion": 2, "minWriterVersion": 5});
            }
            if let Some(m) = a.get_mut("metaData") {
                m["schemaString"] = json!(mapped.to_json());
                m["configuration"] =
                    json!({"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": "2"});
            }
        });

        let committer = table.open(&record, &["host"]).unwrap();

        let names: Vec<&String> = committer.written.fields().iter().map(|f| f.name()).collect();
        assert_eq!(names, [&physical(&mapped, "host"), &physical(&mapped, "n")]);
        assert!(names[0].starts_with("col-"), "{names:?}");
        assert_eq!(committer.partitions[0].physical, physical(&mapped, "host"));
    }

    #[test]
    fn a_commit_adds_its_files_with_this_writers_transaction_as_the_next_version() {
        let table = Table::new();
        let record = st(&[("host", DataType::Str), ("n", DataType::I64), ("msg", DataType::Str)]);
        let mut committer = table.open(&record, &["host", "n"]).unwrap();
        let msg = physical(&committer.schema, "msg");
        let stats = footer(
            vec![Field::new(&msg, Arrow::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["x", "y"]))],
        );
        let first = file(
            &committer,
            &["host=a%2Fb", "n=__HIVE_DEFAULT_PARTITION__"],
            &[("host", "a/b"), ("n", "")],
            Some(stats),
        );
        let second = file(&committer, &["host=c", "n=7"], &[("host", "c"), ("n", "7")], None);
        let id = first.path.rsplit('/').next().unwrap().to_string();
        assert_eq!(first.path, format!("tbl/host=a%2Fb/n=__HIVE_DEFAULT_PARTITION__/{id}"));

        committer.commit(&[first, second]).unwrap();
        assert_eq!(committer.snapshot.version, 1);
        let actions = table.commit(1).unwrap();
        assert_eq!(action(&actions, "commitInfo")["operation"], "WRITE");
        let txn = action(&actions, "txn");
        assert_eq!(txn["appId"], json!(committer.snapshot.app_id));
        assert_eq!(txn["version"], 1);
        let adds = adds(&actions);
        assert_eq!(adds.len(), 2);
        let (host, n) = (physical(&committer.schema, "host"), physical(&committer.schema, "n"));
        assert_eq!(
            adds[0]["path"],
            format!("host=a%252Fb/n=__HIVE_DEFAULT_PARTITION__/{id}")
        );
        assert_eq!(
            adds[0]["partitionValues"],
            json!({host.clone(): "a/b", n.clone(): null})
        );
        assert_eq!(adds[0]["size"], 100);
        assert_eq!(adds[0]["dataChange"], true);
        let stats: Value = serde_json::from_str(adds[0]["stats"].as_str().unwrap()).unwrap();
        assert_eq!(
            stats,
            json!({"numRecords": 2, "minValues": {msg.clone(): "x"}, "maxValues": {msg.clone(): "y"},
                   "nullCount": {msg: 0}})
        );
        assert_eq!(adds[1]["partitionValues"], json!({host: "c", n: "7"}));
        assert!(adds[1].get("stats").is_none());

        let third = file(&committer, &["host=c", "n=7"], &[("host", "c"), ("n", "7")], None);
        committer.commit(&[third]).unwrap();
        assert_eq!(action(&table.commit(2).unwrap(), "txn")["version"], 2);
        assert_eq!(committer.commit(&[]), Ok(()));
        assert!(table.commit(3).is_none());
    }

    fn other_writers_commit() -> Vec<Value> {
        vec![
            json!({"commitInfo": {"operation": "WRITE"}}),
            json!({"txn": {"appId": "other", "version": 1}}),
            json!({"add": {"path": "other.parquet", "partitionValues": {}, "size": 1, "modificationTime": 0,
                           "dataChange": true}}),
        ]
    }

    #[test]
    fn a_commit_that_loses_a_race_is_a_conflict_and_the_next_lands_after_the_winner() {
        let table = Table::new();
        let mut committer = table.open(&st(&[("a", DataType::Str)]), &[]).unwrap();
        table.put(1, &other_writers_commit());
        let files = [file(&committer, &[], &[], None)];

        assert_eq!(committer.commit(&files), Err(CommitError::Conflict));
        assert_eq!(committer.snapshot.version, 1);
        committer.commit(&files).unwrap();
        let actions = table.commit(2).unwrap();
        assert_eq!(adds(&actions)[0]["path"], files[0].path.trim_start_matches("tbl/"));
        assert_eq!(action(&table.commit(1).unwrap(), "txn")["appId"], "other");
    }

    #[test]
    fn a_commit_whose_answer_is_lost_but_landed_is_found_and_not_made_twice() {
        let table = Table::new();
        let mut committer = table.open(&st(&[("a", DataType::Str)]), &[]).unwrap();
        let files = [file(&committer, &[], &[], None)];
        table.arm(Fault::Lost);

        assert_eq!(committer.commit(&files), Ok(()));
        assert_eq!(adds(&table.commit(1).unwrap()).len(), 1);
        assert!(table.commit(2).is_none());

        committer.commit(&[file(&committer, &[], &[], None)]).unwrap();
        assert_eq!(action(&table.commit(2).unwrap(), "txn")["version"], 2);
    }

    #[test]
    fn a_lost_commit_that_lands_late_is_found_by_its_transaction_on_the_next_try() {
        let table = Table::new();
        let mut committer = table.open(&st(&[("a", DataType::Str)]), &[]).unwrap();
        let files = [file(&committer, &[], &[], None)];
        table.arm(Fault::Late);

        assert!(matches!(committer.commit(&files), Err(CommitError::Unknown(_))));
        assert!(table.commit(1).is_none());
        table.land();
        assert_eq!(committer.commit(&files), Ok(()));
        let actions = table.commit(1).unwrap();
        assert_eq!(action(&actions, "txn")["appId"], json!(committer.snapshot.app_id));
        assert_eq!(action(&actions, "txn")["version"], 1);
        assert!(table.commit(2).is_none(), "the files are committed once");

        committer.commit(&[file(&committer, &[], &[], None)]).unwrap();
        assert_eq!(action(&table.commit(2).unwrap(), "txn")["version"], 2);
    }

    #[test]
    fn a_lost_commit_that_did_not_land_is_made_on_the_next_try() {
        let table = Table::new();
        let mut committer = table.open(&st(&[("a", DataType::Str)]), &[]).unwrap();
        let files = [file(&committer, &[], &[], None)];
        table.arm(Fault::Dropped);

        assert!(matches!(committer.commit(&files), Err(CommitError::Unknown(_))));
        assert!(table.commit(1).is_none());
        assert_eq!(committer.commit(&files), Ok(()));
        let actions = table.commit(1).unwrap();
        assert_eq!(adds(&actions)[0]["path"], files[0].path.trim_start_matches("tbl/"));
        assert_eq!(action(&actions, "txn")["version"], 2);
        assert!(table.commit(2).is_none());
    }

    #[test]
    fn other_files_than_a_lost_commits_forget_it() {
        let table = Table::new();
        let mut committer = table.open(&st(&[("a", DataType::Str)]), &[]).unwrap();
        let lost = [file(&committer, &[], &[], None)];
        table.arm(Fault::Late);
        assert!(matches!(committer.commit(&lost), Err(CommitError::Unknown(_))));
        table.land();

        // Were the lost commit still remembered, finding it would take these files as committed.
        let others = [file(&committer, &[], &[], None)];
        assert_eq!(committer.commit(&others), Err(CommitError::Conflict));
        committer.commit(&others).unwrap();
        let actions = table.commit(2).unwrap();
        assert_eq!(adds(&actions)[0]["path"], others[0].path.trim_start_matches("tbl/"));
    }

    #[test]
    fn a_column_another_writer_adds_changes_nothing_and_one_it_drops_holds_commits_back_until_it_is_back() {
        let table = Table::new();
        let mut committer = table.open(&st(&[("a", DataType::Str)]), &[]).unwrap();
        table
            .open(&st(&[("a", DataType::Str), ("b", DataType::I64)]), &[])
            .unwrap();
        let files = [file(&committer, &[], &[], None)];
        assert_eq!(committer.commit(&files), Err(CommitError::Conflict));
        committer.commit(&files).unwrap();
        assert_eq!(adds(&table.commit(2).unwrap()).len(), 1);

        let mut meta = action(&table.commit(1).unwrap(), "metaData").clone();
        let mut schema = StructType::parse(meta["schemaString"].as_str().unwrap()).unwrap();
        let a = schema.fields.remove(0);
        meta["schemaString"] = json!(schema.to_json());
        table.put(3, &[json!({"metaData": meta.clone()})]);
        let files = [file(&committer, &[], &[], None)];
        assert_eq!(committer.commit(&files), Err(CommitError::Conflict));
        let Err(CommitError::Transient(why)) = committer.commit(&files) else {
            panic!("the commit was not held back");
        };
        assert!(why.contains("dropped a column"), "{why}");
        assert!(table.commit(4).is_none());

        schema.fields.insert(0, a);
        meta["schemaString"] = json!(schema.to_json());
        table.put(4, &[json!({"metaData": meta})]);
        committer.commit(&files).unwrap();
        assert_eq!(adds(&table.commit(5).unwrap()).len(), 1);
    }

    #[test]
    fn the_commit_after_a_due_version_writes_a_checkpoint_first() {
        let table = Table::new();
        let record = st(&[("a", DataType::Str)]);
        let first = table.open(&record, &[]).unwrap();
        let mut meta = first.snapshot.metadata.clone();
        meta.configuration.insert("delta.checkpointInterval".into(), "2".into());
        table.put(1, &[json!({"metaData": meta})]);
        let mut committer = table.open(&record, &[]).unwrap();
        let checkpoints = |table: &Table| {
            (table.names().into_iter())
                .filter(|n| n.contains(".checkpoint."))
                .collect::<Vec<_>>()
        };

        committer.commit(&[file(&committer, &[], &[], None)]).unwrap();
        assert!(table.commit(2).is_some());
        assert!(checkpoints(&table).is_empty());
        committer.commit(&[file(&committer, &[], &[], None)]).unwrap();
        let written = checkpoints(&table);
        assert_eq!(written, ["00000000000000000002.checkpoint.parquet"]);
        assert!(table.commit(3).is_some());
        assert!(table.names().contains(&"_last_checkpoint".to_string()));
    }

    pub(super) fn str_col(pool: &mut BufferPool, vals: &[Option<&str>]) -> Column {
        let mut sb = StrBuilder::new(pool, VarType::Str, vals.len());
        sb.reserve(pool, vals.iter().flatten().map(|s| s.len()).sum());
        for v in vals {
            match v {
                Some(s) => sb.push(s.as_bytes()),
                None => sb.push_null(),
            }
        }
        sb.finish()
    }

    #[test]
    fn rows_partition_by_their_values_as_delta_writes_them() {
        let record = st(&[
            ("n", DataType::I64),
            ("host name", DataType::Str),
            ("ok", DataType::Bool),
            ("ts", DataType::DateTime),
        ]);
        let schema = new_schema(&record);
        let by = ["n", "host name", "ok", "ts"];
        let columns = partition_columns(&metadata(&schema, &[], &by), &schema).unwrap();
        let partitioner = DeltaPartitioner {
            columns: columns.clone(),
            unpartitioned_key_bytes: 0,
        };

        let mut pool = BufferPool::new();
        let mut n = Column::alloc(&mut pool, &DataType::I64, 2);
        n.i64_mut().unwrap().values.copy_from_slice(&[34, 0]);
        n.i64_mut().unwrap().validity.set_valid(1, false);
        let host = str_col(&mut pool, &[Some("a b/c%"), None]);
        let mut ok = Column::alloc(&mut pool, &DataType::Bool, 2);
        ok.bool_mut().unwrap().values[0] = 0b01;
        ok.bool_mut().unwrap().validity.set_valid(1, false);
        let mut ts = Column::alloc(&mut pool, &DataType::DateTime, 2);
        ts.datetime_mut()
            .unwrap()
            .values
            .copy_from_slice(&[SPEC_TIME + 123_456, 0]);
        ts.datetime_mut().unwrap().validity.set_valid(1, false);
        let file_schema = crate::io::parquet::Schema::new(&record).unwrap();
        let chunk = {
            let view = RecordView::from_named([("n", &n), ("host name", &host), ("ok", &ok), ("ts", &ts)]);
            Arc::new(Chunk::cut(&file_schema, &[(&view, &[0, 1])], 0, &mut pool))
        };
        for c in [n, host, ok, ts] {
            c.reclaim(&mut pool);
        }
        let row = |row| OwnedRecord {
            columns: Some(Columns {
                chunk: chunk.clone(),
                row,
            }),
            ..Default::default()
        };
        let p = |name| physical(&schema, name);

        let full = partitioner.partition(&row(0)).unwrap();
        assert_eq!(
            full.path,
            [
                "n=34",
                "host+name=a+b%2Fc%25",
                "ok=true",
                "ts=2017-11-16T22%3A31%3A08.123456Z"
            ]
        );
        assert_eq!(
            full.values,
            [
                (p("n"), "34".to_string()),
                (p("host name"), "a b/c%".to_string()),
                (p("ok"), "true".to_string()),
                (p("ts"), "2017-11-16T22:31:08.123456Z".to_string()),
            ]
        );
        let nulls = partitioner.partition(&row(1)).unwrap();
        assert_eq!(
            nulls.path,
            [
                "n=__HIVE_DEFAULT_PARTITION__",
                "host+name=__HIVE_DEFAULT_PARTITION__",
                "ok=__HIVE_DEFAULT_PARTITION__",
                "ts=__HIVE_DEFAULT_PARTITION__"
            ]
        );
        assert!(nulls.values.iter().all(|(_, v)| v.is_empty()), "{:?}", nulls.values);

        let near_full = DeltaPartitioner {
            columns,
            unpartitioned_key_bytes: 1000,
        };
        let why = near_full.partition(&row(0)).unwrap_err();
        assert!(why.contains("past the 1024 a key may hold"), "{why}");

        let unpartitioned = DeltaPartitioner {
            columns: Vec::new(),
            unpartitioned_key_bytes: 0,
        }
        .partition(&OwnedRecord::default())
        .unwrap();
        assert_eq!(unpartitioned.path, Vec::<String>::new());
        assert_eq!(unpartitioned.values, Vec::<(String, String)>::new());
    }

    #[test]
    fn a_size_reads_as_delta_writes_one() {
        assert_eq!(size_bytes("104857600"), Some(104_857_600));
        assert_eq!(size_bytes("100mb"), Some(100 << 20));
        assert_eq!(size_bytes(" 1 GB "), Some(1 << 30));
        for nothing in ["", "0", "mb", "1tb", "-5", "1.5mb"] {
            assert_eq!(size_bytes(nothing), None, "{nothing:?}");
        }
    }

    #[test]
    fn an_add_path_is_a_uri_path() {
        assert_eq!(uri_path("h=a+b%2F c/x.parquet"), "h=a%2Bb%252F%20c/x.parquet");
        assert_eq!(uri_path("Az09.-_~/="), "Az09.-_~/=");
    }

    #[test]
    fn a_timestamp_is_iso_8601_in_utc_to_the_digits_asked() {
        assert_eq!(timestamp(SPEC_TIME + 123_456, 6), "2017-11-16T22:31:08.123456Z");
        assert_eq!(timestamp(SPEC_TIME + 123_456, 3), "2017-11-16T22:31:08.123Z");
        assert_eq!(timestamp(-1, 6), "1969-12-31T23:59:59.999999Z");
        assert_eq!(wall_clock(SPEC_TIME + 123_456, 6, ' '), "2017-11-16 22:31:08.123456");
    }

    #[test]
    fn a_partition_value_is_written_as_its_columns_type() {
        assert_eq!(partition_text(&Literal::Double(0.1f32 as f64), &Kind::Float), "0.1");
        assert_eq!(partition_text(&Literal::Double(0.1), &Kind::Double), "0.1");
        assert_eq!(
            partition_text(&Literal::Double(f64::NEG_INFINITY), &Kind::Float),
            "-Infinity"
        );
        assert_eq!(
            partition_text(&Literal::Micros(SPEC_TIME), &Kind::Timestamp),
            "2017-11-16 22:31:08.000000"
        );
        assert_eq!(
            partition_text(&Literal::Micros(SPEC_TIME), &Kind::Timestamptz),
            "2017-11-16T22:31:08.000000Z"
        );
    }

    /// `{ts, host, msg, n, ok, http: {status}, tags: [str]}` and a footer of two rows of it, by physical names.
    fn stats_file() -> (StructType, Arc<ParquetMetaData>) {
        let record = st(&[
            ("ts", DataType::DateTime),
            ("host", DataType::Str),
            ("msg", DataType::Str),
            ("n", DataType::I64),
            ("ok", DataType::Bool),
            ("http", st(&[("status", DataType::I64)])),
            ("tags", DataType::Array(Box::new(DataType::Str))),
        ]);
        let schema = new_schema(&record);
        let p = |name| physical(&schema, name);
        let Type::Struct(http) = &schema.fields[5].data_type else {
            panic!("http is a struct")
        };
        let status = Field::new(physical(http, "status"), Arrow::Int64, true);
        let element = Arc::new(Field::new("element", Arrow::Utf8, true));
        let mut tags = ListBuilder::new(StringBuilder::new()).with_field(element.clone());
        tags.values().append_value("x");
        tags.append(true);
        tags.append(true);
        let fields = vec![
            Field::new(
                p("ts"),
                Arrow::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(p("host"), Arrow::Utf8, true),
            Field::new(p("msg"), Arrow::Utf8, true),
            Field::new(p("n"), Arrow::Int64, true),
            Field::new(p("ok"), Arrow::Boolean, true),
            Field::new(p("http"), Arrow::Struct(Fields::from(vec![status.clone()])), true),
            Field::new(p("tags"), Arrow::List(element), true),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![SPEC_TIME + 123_456, SPEC_TIME + 999]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["b".to_string(), "a".repeat(40)])),
            Arc::new(StringArray::from(vec!["a".to_string(), "z".repeat(40)])),
            Arc::new(Int64Array::from(vec![Some(5), None])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(StructArray::new(
                Fields::from(vec![status]),
                vec![Arc::new(Int64Array::from(vec![200, 404])) as ArrayRef],
                None,
            )),
            Arc::new(tags.finish()),
        ];
        (schema, footer(fields, columns))
    }

    #[test]
    fn a_files_stats_hold_its_rows_null_counts_and_bounds_by_physical_names() {
        let (schema, file) = stats_file();
        let p = |name| physical(&schema, name);
        let Type::Struct(http) = &schema.fields[5].data_type else {
            panic!("http is a struct")
        };
        let status = physical(http, "status");

        let got = stats(&file, &schema, &metadata(&schema, &[], &[]), &[], 2);
        assert_eq!(
            got,
            json!({
                "numRecords": 2,
                "minValues": {
                    p("ts"): "2017-11-16T22:31:08.000Z",
                    p("host"): "a".repeat(32),
                    p("msg"): "a",
                    p("n"): 5,
                    p("http"): {status.clone(): 200},
                },
                "maxValues": {
                    p("ts"): "2017-11-16T22:31:08.123Z",
                    p("host"): "b",
                    p("n"): 5,
                    p("http"): {status.clone(): 404},
                },
                "nullCount": {
                    p("ts"): 0,
                    p("host"): 0,
                    p("msg"): 0,
                    p("n"): 1,
                    p("ok"): 0,
                    p("http"): {status: 0},
                },
            })
        );
    }

    #[test]
    fn only_the_tables_indexed_columns_take_stats() {
        let (schema, file) = stats_file();
        let p = |name| physical(&schema, name);
        let meta = metadata(&schema, &[("delta.dataSkippingNumIndexedCols", "2")], &[]);
        let got = stats(&file, &schema, &meta, &[], 2);
        assert_eq!(got["nullCount"], json!({p("ts"): 0, p("host"): 0}));
        assert_eq!(
            got["minValues"],
            json!({p("ts"): "2017-11-16T22:31:08.000Z", p("host"): "a".repeat(32)})
        );
    }

    #[test]
    fn a_clustering_column_takes_stats_past_the_indexed_ones() {
        let (schema, file) = stats_file();
        let p = |name| physical(&schema, name);
        let meta = metadata(&schema, &[("delta.dataSkippingNumIndexedCols", "1")], &[]);
        let got = stats(&file, &schema, &meta, &[vec![p("n")]], 2);
        assert_eq!(got["nullCount"], json!({p("ts"): 0, p("n"): 1}));
    }

    #[test]
    fn a_partition_column_takes_no_stats_nor_an_indexed_place() {
        let (schema, file) = stats_file();
        let p = |name| physical(&schema, name);
        let meta = metadata(&schema, &[("delta.dataSkippingNumIndexedCols", "2")], &["host"]);
        let got = stats(&file, &schema, &meta, &[], 2);
        assert_eq!(got["nullCount"], json!({p("ts"): 0, p("msg"): 0}));
    }

    #[test]
    fn a_location_is_an_s3_bucket_and_a_key() {
        let root = |bucket: &str, key: &str| Root {
            bucket: bucket.into(),
            key: key.into(),
        };
        assert_eq!(Root::parse("s3://b/a/t/"), Ok(root("b", "a/t")));
        assert_eq!(Root::parse("s3a://b"), Ok(root("b", "")));
        let why = Root::parse("s3:///t").unwrap_err();
        assert!(why.contains("names no bucket"), "{why}");
        for other in ["gs://b/t", "abfss://b/t", "b/t"] {
            let why = Root::parse(other).unwrap_err();
            assert!(why.contains("is not in S3"), "{why}");
        }
        assert_eq!(root("b", "").key("x"), "x");
        assert_eq!(root("b", "a/t").key("x"), "a/t/x");
    }
}

#[cfg(test)]
#[path = "fuzz.rs"]
mod fuzz;
