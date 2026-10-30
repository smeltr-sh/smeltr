//! The `_delta_log`: its actions, and the table's state read from its last checkpoint and the commits after it.
//!
//! The state held is what a commit and a checkpoint need besides the file list: the protocol, the metadata, each
//! application's last transaction, the domains, and the last checkpoint's files. File actions are held only for
//! the commits since the last checkpoint, which the next checkpoint writes after the last checkpoint's.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use tokio_util::bytes::Bytes;

use super::checkpoint;
use crate::io::files::{Upload, wait};

/// The most a JSON object read whole from the log may hold.
const MAX_JSON_BYTES: u64 = 64 * 1024 * 1024;

/// The most the file actions of the commits since the last checkpoint may hold before they are let go, and the next
/// checkpoint reads them from the commits again.
const MAX_HELD_BYTES: usize = 16 * 1024 * 1024;

/// A table's `_delta_log` in its store. Every call is bounded by `stall_after`, so a store that goes quiet is an error.
pub struct Log {
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
    stall_after: Duration,
    /// The log's key, `<table>/_delta_log`.
    dir: String,
}

/// One entry of the log as a listing gives it.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    /// Its name under `_delta_log/`.
    pub name: String,
    pub size: u64,
    /// Milliseconds since the epoch.
    pub modified: i64,
}

impl Log {
    /// The log of the table whose root is `table`, a key in `store`: empty for the bucket's root.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        runtime: Arc<tokio::runtime::Runtime>,
        stall_after: Duration,
        table: &str,
    ) -> Log {
        let table = table.trim_matches('/');
        Log {
            store,
            runtime,
            stall_after,
            dir: if table.is_empty() {
                "_delta_log".to_string()
            } else {
                format!("{table}/_delta_log")
            },
        }
    }

    fn path(&self, name: &str) -> Result<Path, String> {
        Path::parse(format!("{}/{name}", self.dir)).map_err(|e| format!("_delta_log/{name} is no path: {e}"))
    }

    /// The object `name`, whole; None when there is none. Err when it is larger than a JSON object of the log may be.
    pub fn get(&self, name: &str) -> Result<Option<Bytes>, String> {
        let path = self.path(name)?;
        let store = self.store.clone();
        let got = wait(&self.runtime, self.stall_after, async move {
            let got = match store.get(&path).await {
                Ok(got) => got,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e),
            };
            if got.meta.size > MAX_JSON_BYTES {
                return Ok(Some(Err(got.meta.size)));
            }
            got.bytes().await.map(|b| Some(Ok(b)))
        })
        .map_err(|e| format!("reading _delta_log/{name}: {e}"))?;
        match got {
            None => Ok(None),
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(size)) => Err(format!(
                "_delta_log/{name} holds {size} bytes, more than the {MAX_JSON_BYTES} read whole"
            )),
        }
    }

    /// Calls `each` on every line of `name`, read as it streams in, so a commit of any size is read in the memory of
    /// its longest line. Ok(false) when there is no such object.
    pub fn lines(&self, name: &str, mut each: impl FnMut(&str) -> Result<(), String>) -> Result<bool, String> {
        let path = self.path(name)?;
        let store = self.store.clone();
        let reading = |e: String| format!("reading _delta_log/{name}: {e}");
        let got = wait(&self.runtime, self.stall_after, async move {
            match store.get(&path).await {
                Ok(got) => Ok(Some(got)),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(e),
            }
        })
        .map_err(reading)?;
        let Some(got) = got else {
            return Ok(false);
        };
        let mut stream = got.into_stream();
        let mut line: Vec<u8> = Vec::new();
        let mut take = |line: &mut Vec<u8>| -> Result<(), String> {
            let text = std::str::from_utf8(line).map_err(|e| format!("_delta_log/{name} is no UTF-8: {e}"))?;
            if !text.trim().is_empty() {
                each(text)?;
            }
            line.clear();
            Ok(())
        };
        loop {
            let chunk = wait(&self.runtime, self.stall_after, async {
                stream.next().await.transpose()
            })
            .map_err(reading)?;
            let Some(chunk) = chunk else { break };
            let mut rest = &chunk[..];
            while let Some(end) = rest.iter().position(|&b| b == b'\n') {
                line.extend_from_slice(&rest[..end]);
                take(&mut line)?;
                rest = &rest[end + 1..];
            }
            line.extend_from_slice(rest);
        }
        take(&mut line)?;
        Ok(true)
    }

    /// Bytes `range` of `name`.
    pub fn range(&self, name: &str, range: std::ops::Range<u64>) -> Result<Bytes, String> {
        let path = self.path(name)?;
        wait(&self.runtime, self.stall_after, self.store.get_range(&path, range))
            .map_err(|e| format!("reading _delta_log/{name}: {e}"))
    }

    /// Byte ranges of `name`, fetched together.
    pub fn ranges(&self, name: &str, ranges: &[std::ops::Range<u64>]) -> Result<Vec<Bytes>, String> {
        let path = self.path(name)?;
        wait(&self.runtime, self.stall_after, self.store.get_ranges(&path, ranges))
            .map_err(|e| format!("reading _delta_log/{name}: {e}"))
    }

    /// Writes `name` only where there is none. Ok(false) when there is one; Err when the answer is lost and it may or
    /// may not have been written.
    pub fn create(&self, name: &str, bytes: Vec<u8>) -> Result<bool, String> {
        let path = self.path(name)?;
        let put = self
            .store
            .put_opts(&path, PutPayload::from(bytes), PutMode::Create.into());
        match wait(&self.runtime, self.stall_after, async {
            match put.await {
                Ok(_) => Ok(true),
                Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
                Err(e) => Err(e),
            }
        }) {
            Ok(written) => Ok(written),
            Err(e) => Err(format!("writing _delta_log/{name}: {e}")),
        }
    }

    /// Writes `name`, replacing what is there.
    pub fn put(&self, name: &str, bytes: Vec<u8>) -> Result<(), String> {
        let path = self.path(name)?;
        wait(
            &self.runtime,
            self.stall_after,
            self.store.put(&path, PutPayload::from(bytes)),
        )
        .map(|_| ())
        .map_err(|e| format!("writing _delta_log/{name}: {e}"))
    }

    /// Starts writing `name`, replacing what is there once the upload finishes.
    pub(crate) fn upload(&self, name: &str) -> Result<Upload, String> {
        let path = self.path(name)?;
        // One part uploads while the next is made.
        Upload::new(
            self.store.clone(),
            self.runtime.clone(),
            self.stall_after,
            1,
            Default::default(),
            path.as_ref(),
        )
    }

    pub fn delete(&self, name: &str) -> Result<(), String> {
        let path = self.path(name)?;
        wait(&self.runtime, self.stall_after, async {
            match self.store.delete(&path).await {
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                other => other,
            }
        })
        .map_err(|e| format!("deleting _delta_log/{name}: {e}"))
    }

    /// The log's entries whose names sort after `after`, in name order, until `more` says to stop.
    pub fn list(&self, after: Option<&str>, mut more: impl FnMut(Entry) -> bool) -> Result<(), String> {
        let base = self.dir.clone();
        let prefix = Path::parse(&base).map_err(|e| format!("{base} is no path: {e}"))?;
        let mut stream = match after {
            Some(after) => {
                let offset = self.path(after)?;
                self.store.list_with_offset(Some(&prefix), &offset)
            }
            None => self.store.list(Some(&prefix)),
        };
        let mut ended = false;
        while !ended {
            // A listing comes a page at a time; the entries of one page are taken in order before the next is asked.
            let page = wait(&self.runtime, self.stall_after, async {
                let mut page = Vec::new();
                while page.len() < 1000 {
                    match stream.next().await {
                        Some(meta) => page.push(meta?),
                        None => {
                            ended = true;
                            break;
                        }
                    }
                }
                Ok::<_, object_store::Error>(page)
            })
            .map_err(|e| format!("listing _delta_log: {e}"))?;
            // S3 lists in name order, which `more` relies on.
            for meta in page {
                let Some(name) = meta.location.as_ref().strip_prefix(&format!("{base}/")) else {
                    continue;
                };
                let entry = Entry {
                    name: name.to_string(),
                    size: meta.size,
                    modified: meta.last_modified.timestamp_millis(),
                };
                if !more(entry) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

/// The name of commit `version`.
pub fn commit_name(version: i64) -> String {
    format!("{version:020}.json")
}

/// What a name under `_delta_log/` is.
#[derive(Clone, Debug, PartialEq)]
pub enum Name {
    Commit(i64),
    /// A checkpoint in one file: `n.checkpoint.parquet`, or V2's `n.checkpoint.<uuid>.json|parquet`.
    Checkpoint(i64),
    /// One part of a checkpoint in several, which a table with V2 checkpoints never has.
    Part(i64),
    /// Anything else this writer reads nothing from: checksums, compacted commits, `_last_checkpoint`.
    Other,
}

impl Name {
    pub fn of(name: &str) -> Name {
        let Some((digits, rest)) = name.split_once('.') else {
            return Name::Other;
        };
        let Ok(version) = digits.parse::<i64>() else {
            return Name::Other;
        };
        if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) || name.contains('/') {
            return Name::Other;
        }
        let parts: Vec<&str> = rest.split('.').collect();
        match parts.as_slice() {
            ["json"] => Name::Commit(version),
            ["checkpoint", "parquet"] | ["checkpoint", _, "json" | "parquet"] => Name::Checkpoint(version),
            ["checkpoint", _, _, "parquet"] => Name::Part(version),
            _ => Name::Other,
        }
    }

    pub fn version(&self) -> Option<i64> {
        match self {
            Name::Commit(v) | Name::Checkpoint(v) | Name::Part(v) => Some(*v),
            Name::Other => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reader_features: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_features: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub format: Format,
    pub schema_string: String,
    #[serde(default)]
    pub partition_columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_time: Option<i64>,
    #[serde(default)]
    pub configuration: BTreeMap<String, String>,
    /// Fields this writer does not know, carried as they are.
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

impl Metadata {
    /// The table property `key`.
    pub fn property(&self, key: &str) -> Option<&str> {
        self.configuration.get(key).map(String::as_str)
    }

    /// The true-or-false table property `key`; None when it is unset or neither.
    pub fn flag(&self, key: &str) -> Option<bool> {
        let value = self.property(key)?.trim();
        (value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false"))
            .then(|| value.eq_ignore_ascii_case("true"))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Format {
    pub provider: String,
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

/// An application's last transaction: what this writer counts its commits with.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Txn {
    pub app_id: String,
    pub version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DomainMetadata {
    pub domain: String,
    pub configuration: String,
    pub removed: bool,
}

/// A checkpoint's file of file actions, named under `_delta_log/_sidecars/`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sidecar {
    pub path: String,
    pub size_in_bytes: i64,
    pub modification_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    pub version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<BTreeMap<String, String>>,
}

/// An `add` or a `remove`, as the log has it.
#[derive(Clone, Debug)]
pub struct FileAction {
    pub add: bool,
    /// What makes it the same file as another: its path and its deletion vector's id.
    pub key: String,
    /// When a remove's file was removed, in milliseconds since the epoch.
    pub deleted: Option<i64>,
    pub json: Box<RawValue>,
}

impl FileAction {
    fn parse(add: bool, json: Box<RawValue>) -> Result<FileAction, String> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Fields {
            path: String,
            #[serde(default)]
            deletion_vector: Option<DeletionVector>,
            #[serde(default)]
            deletion_timestamp: Option<i64>,
        }
        let fields: Fields = serde_json::from_str(json.get()).map_err(|e| format!("a file action in the log: {e}"))?;
        Ok(FileAction {
            add,
            key: file_key(&fields.path, fields.deletion_vector.as_ref()),
            deleted: fields.deletion_timestamp,
            json,
        })
    }
}

/// A file action's deletion vector, as far as its id.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeletionVector {
    pub storage_type: String,
    pub path_or_inline_dv: String,
    #[serde(default)]
    pub offset: Option<i32>,
}

/// What a file action names: two actions of one key are of one file, and the later wins.
pub fn file_key(path: &str, dv: Option<&DeletionVector>) -> String {
    match dv {
        None => path.to_string(),
        Some(dv) => match dv.offset {
            Some(offset) => format!("{path}\0{}{}@{offset}", dv.storage_type, dv.path_or_inline_dv),
            None => format!("{path}\0{}{}", dv.storage_type, dv.path_or_inline_dv),
        },
    }
}

/// One line of the log.
#[derive(Clone, Debug)]
pub enum Action {
    Protocol(Protocol),
    Metadata(Box<Metadata>),
    Txn(Txn),
    Domain(DomainMetadata),
    Sidecar(Sidecar),
    CheckpointMetadata(CheckpointMetadata),
    File(FileAction),
    /// `commitInfo`, or an action this writer does not read.
    Other,
}

impl Action {
    pub fn parse(line: &str) -> Result<Action, String> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Line {
            protocol: Option<Protocol>,
            meta_data: Option<Box<Metadata>>,
            txn: Option<Txn>,
            domain_metadata: Option<DomainMetadata>,
            sidecar: Option<Sidecar>,
            checkpoint_metadata: Option<CheckpointMetadata>,
            add: Option<Box<RawValue>>,
            remove: Option<Box<RawValue>>,
        }
        let line: Line = serde_json::from_str(line).map_err(|e| format!("a line of the log: {e}"))?;
        Ok(if let Some(p) = line.protocol {
            Action::Protocol(p)
        } else if let Some(m) = line.meta_data {
            Action::Metadata(m)
        } else if let Some(t) = line.txn {
            Action::Txn(t)
        } else if let Some(d) = line.domain_metadata {
            Action::Domain(d)
        } else if let Some(s) = line.sidecar {
            Action::Sidecar(s)
        } else if let Some(c) = line.checkpoint_metadata {
            Action::CheckpointMetadata(c)
        } else if let Some(add) = line.add {
            Action::File(FileAction::parse(true, add)?)
        } else if let Some(remove) = line.remove {
            Action::File(FileAction::parse(false, remove)?)
        } else {
            Action::Other
        })
    }
}

/// `value` as one line of the log: `{"<key>": value}`.
pub fn line(key: &str, value: &impl Serialize) -> String {
    let mut object = Map::new();
    object.insert(
        key.to_string(),
        serde_json::to_value(value).expect("an action serializes"),
    );
    Value::Object(object).to_string()
}

/// The table's last checkpoint.
#[derive(Clone, Debug, PartialEq)]
pub struct Checkpoint {
    pub version: i64,
    /// Its files under `_delta_log/`: the one, or a multi-part checkpoint's parts in order.
    pub files: Vec<Entry>,
    /// A V2 checkpoint's sidecars, which hold its file actions.
    pub sidecars: Vec<Sidecar>,
}

/// The file actions of the commits since the last checkpoint, in commit order.
#[derive(Clone, Debug, Default)]
pub struct Since {
    /// Every action, unless `spilled`.
    pub actions: Vec<FileAction>,
    held_bytes: usize,
    /// They were too many to hold, and the next checkpoint reads them from the commits again.
    pub spilled: bool,
    /// How many there are.
    pub count: usize,
    /// For each key a commit not this writer's acted on, the place in order of its last action. Every earlier action
    /// on the key, here or in a checkpoint, has lost to that one.
    pub touched: HashMap<String, usize>,
}

impl Since {
    /// Takes one commit's file actions. `ours` when this writer made the commit: then each adds a file of its own, new
    /// to the table, and no earlier action is touched.
    fn take(&mut self, actions: Vec<FileAction>, ours: bool) {
        for action in actions {
            if !ours {
                self.touched.insert(action.key.clone(), self.count);
            }
            self.count += 1;
            if self.spilled {
                continue;
            }
            self.held_bytes += action.json.get().len();
            if self.held_bytes > MAX_HELD_BYTES {
                self.spilled = true;
                self.actions = Vec::new();
            } else {
                self.actions.push(action);
            }
        }
    }
}

/// The table at one version, but for its file list.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub version: i64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    /// By application.
    pub txns: BTreeMap<String, Txn>,
    /// By domain, removed ones left out.
    pub domains: BTreeMap<String, DomainMetadata>,
    pub checkpoint: Option<Checkpoint>,
    pub since: Since,
    /// The application this writer's commits carry.
    pub app_id: String,
}

impl Snapshot {
    /// The configuration of the domain `name`; None when the table has no such domain. Err when it does not read as
    /// a `T`.
    pub fn domain<T: serde::de::DeserializeOwned>(&self, name: &str) -> Result<Option<T>, String> {
        let Some(domain) = self.domains.get(name) else {
            return Ok(None);
        };
        serde_json::from_str(&domain.configuration)
            .map(Some)
            .map_err(|e| format!("the table's domain {name} does not read: {e}"))
    }

    /// The table as the log has it now, its commits read from the last checkpoint on; None when it has no commit.
    /// Commits carrying `app_id`'s transaction are taken as this writer's.
    pub fn load(log: &Log, app_id: &str) -> Result<Option<Snapshot>, String> {
        let hint = last_checkpoint(log);
        let mut found = latest(log, hint)?;
        if hint.is_some() && found.as_ref().is_none_or(|f| f.checkpoint.is_none()) {
            // `_last_checkpoint` names one cleanup has since deleted: the log is read from its start.
            found = latest(log, None)?;
        }
        let Some(found) = found else {
            return Ok(None);
        };
        let mut state = State::default();
        let mut snapshot = match &found.checkpoint {
            None => {
                if found.first != Some(0) {
                    return Err(format!(
                        "the log has no checkpoint and no commit 0: its commits start at {}",
                        found.first.unwrap_or_default()
                    ));
                }
                let mut actions = Vec::new();
                if !log.lines(&commit_name(0), |line| {
                    state.apply(Action::parse(line)?, &mut actions, app_id)
                })? {
                    return Err("the log lists commit 0, which reads as missing".to_string());
                }
                let mut since = Since::default();
                since.take(actions, state.ours);
                state.into_snapshot(0, None, since, app_id)?
            }
            Some((version, files)) => {
                let checkpoint = read_checkpoint(log, *version, files.clone(), &mut state)?;
                if found.first.is_some_and(|first| first != checkpoint.version + 1) {
                    return Err(format!(
                        "the log has no commit {} after its checkpoint at {}",
                        checkpoint.version + 1,
                        checkpoint.version
                    ));
                }
                state.into_snapshot(checkpoint.version, Some(checkpoint), Since::default(), app_id)?
            }
        };
        while snapshot.version < found.latest {
            if !snapshot.read_next(log)? {
                return Err(format!(
                    "the log lists commit {}, which reads as missing",
                    snapshot.version + 1
                ));
            }
        }
        Ok(Some(snapshot))
    }

    /// Reads the commits after this version until there are none. Gives how many there were.
    pub fn advance(&mut self, log: &Log) -> Result<usize, String> {
        let mut read = 0;
        while self.read_next(log)? {
            read += 1;
        }
        Ok(read)
    }

    /// Reads the commit after this version; false when there is none yet.
    fn read_next(&mut self, log: &Log) -> Result<bool, String> {
        let version = self.version + 1;
        let mut state = State {
            txns: std::mem::take(&mut self.txns),
            domains: std::mem::take(&mut self.domains),
            ..State::default()
        };
        let mut actions = Vec::new();
        let read = log.lines(&commit_name(version), |line| {
            state.apply(Action::parse(line)?, &mut actions, &self.app_id)
        });
        self.txns = std::mem::take(&mut state.txns);
        self.domains = std::mem::take(&mut state.domains);
        if !read? {
            return Ok(false);
        }
        if state.sidecars {
            return Err(format!("commit {version} holds checkpoint actions"));
        }
        if let Some(p) = state.protocol {
            self.protocol = p;
        }
        if let Some(m) = state.metadata {
            self.metadata = m;
        }
        self.since.take(actions, state.ours);
        self.version = version;
        Ok(true)
    }

    /// Takes this writer's own commit of `actions` as the table's next version.
    pub fn committed(&mut self, actions: Vec<Action>) {
        let mut files = Vec::new();
        for action in actions {
            match action {
                Action::Protocol(p) => self.protocol = p,
                Action::Metadata(m) => self.metadata = *m,
                Action::Txn(t) => {
                    self.txns.insert(t.app_id.clone(), t);
                }
                Action::Domain(d) if d.removed => {
                    self.domains.remove(&d.domain);
                }
                Action::Domain(d) => {
                    self.domains.insert(d.domain.clone(), d);
                }
                Action::File(f) => files.push(f),
                Action::Sidecar(_) | Action::CheckpointMetadata(_) | Action::Other => {}
            }
        }
        self.since.take(files, true);
        self.version += 1;
    }

    /// This writer's last transaction the table holds.
    pub fn our_version(&self) -> Option<i64> {
        self.txns.get(&self.app_id).map(|t| t.version)
    }

    /// Takes `checkpoint`, written at this version, as the last.
    pub fn checkpointed(&mut self, checkpoint: Checkpoint) {
        self.checkpoint = Some(checkpoint);
        self.since = Since::default();
    }

    /// Reads the table again from the newest checkpoint in the log before this version, when another writer wrote
    /// one after the last this snapshot holds, so the next checkpoint is written from it and not from an older one.
    /// Gives whether this version is still to be checkpointed: false when the log has a checkpoint at it or after.
    pub fn rebase(&mut self, log: &Log) -> Result<bool, String> {
        let after = self.checkpoint.as_ref().map(|c| c.version);
        let Some((version, files)) = newest(checkpoints(log, after, self.version)?) else {
            return Ok(true);
        };
        if version >= self.version {
            return Ok(false);
        }
        let mut state = State::default();
        let checkpoint = read_checkpoint(log, version, files, &mut state)?;
        let mut snapshot = state.into_snapshot(checkpoint.version, Some(checkpoint), Since::default(), &self.app_id)?;
        while snapshot.version < self.version {
            if !snapshot.read_next(log)? {
                return Err(format!(
                    "the log has no commit {}, which it had before",
                    snapshot.version + 1
                ));
            }
        }
        *self = snapshot;
        Ok(true)
    }
}

/// The log's checkpoints after version `after`, through the first after `through`, in version order.
pub fn checkpoints(log: &Log, after: Option<i64>, through: i64) -> Result<Vec<(Name, Entry)>, String> {
    let mut found = Vec::new();
    // The offset is exclusive, and `n` sorts before every name of version n.
    let offset = after.map(|v| format!("{v:020}"));
    log.list(offset.as_deref(), |entry| {
        let name = Name::of(&entry.name);
        let Some(version) = name.version() else {
            // `_last_checkpoint` and `_sidecars/` sort after every version.
            return !entry.name.starts_with('_');
        };
        if after.is_some_and(|a| version <= a) {
            return true;
        }
        if matches!(name, Name::Checkpoint(_) | Name::Part(_)) {
            found.push((name, entry));
        }
        version <= through
    })?;
    Ok(found)
}

/// What reading a checkpoint or a commit gathers.
#[derive(Default)]
struct State {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    txns: BTreeMap<String, Txn>,
    domains: BTreeMap<String, DomainMetadata>,
    sidecar_list: Vec<Sidecar>,
    /// A sidecar or checkpointMetadata action was read.
    sidecars: bool,
    /// A transaction of this writer's was read.
    ours: bool,
}

impl State {
    fn into_snapshot(
        self,
        version: i64,
        checkpoint: Option<Checkpoint>,
        since: Since,
        app_id: &str,
    ) -> Result<Snapshot, String> {
        let (Some(protocol), Some(metadata)) = (self.protocol, self.metadata) else {
            return Err(format!("the table at version {version} has no protocol or no metaData"));
        };
        Ok(Snapshot {
            version,
            protocol,
            metadata,
            txns: self.txns,
            domains: self.domains,
            checkpoint,
            since,
            app_id: app_id.to_string(),
        })
    }

    fn apply(&mut self, action: Action, files: &mut Vec<FileAction>, app_id: &str) -> Result<(), String> {
        match action {
            Action::Protocol(p) => self.protocol = Some(p),
            Action::Metadata(m) => self.metadata = Some(*m),
            Action::Txn(t) => {
                self.ours |= t.app_id == app_id;
                self.txns.insert(t.app_id.clone(), t);
            }
            Action::Domain(d) if d.removed => {
                self.domains.remove(&d.domain);
            }
            Action::Domain(d) => {
                self.domains.insert(d.domain.clone(), d);
            }
            Action::Sidecar(s) => {
                self.sidecars = true;
                self.sidecar_list.push(s);
            }
            Action::CheckpointMetadata(_) => self.sidecars = true,
            Action::File(f) => files.push(f),
            Action::Other => {}
        }
        Ok(())
    }
}

/// The version `_last_checkpoint` names, if it can be read. It is a hint: the listing is the truth.
pub fn last_checkpoint(log: &Log) -> Option<i64> {
    #[derive(Deserialize)]
    struct Last {
        version: i64,
    }
    let bytes = log.get("_last_checkpoint").ok()??;
    serde_json::from_slice::<Last>(&bytes).ok().map(|l| l.version)
}

/// What a listing of the log found.
struct Found {
    /// The newest checkpoint: its version and files.
    checkpoint: Option<(i64, Vec<Entry>)>,
    /// The first commit after it, or at all when there is none.
    first: Option<i64>,
    latest: i64,
}

/// The newest checkpoint and the commits after it, listing from version `from` on. None when nothing is listed.
/// Err when the commits after the checkpoint have a gap.
fn latest(log: &Log, from: Option<i64>) -> Result<Option<Found>, String> {
    let mut checkpoints = Vec::new();
    let mut commits: Vec<i64> = Vec::new();
    // The offset is exclusive, and `n` sorts before every name of version n.
    let after = from.map(|v| format!("{v:020}"));
    log.list(after.as_deref(), |entry| {
        match Name::of(&entry.name) {
            Name::Commit(v) => commits.push(v),
            name @ (Name::Checkpoint(_) | Name::Part(_)) => checkpoints.push((name, entry)),
            // `_last_checkpoint` and `_sidecars/` sort after every version.
            Name::Other if entry.name.starts_with('_') => return false,
            Name::Other => {}
        }
        true
    })?;
    let checkpoint = newest(checkpoints);
    let at = checkpoint.as_ref().map(|(v, _)| *v);
    commits.retain(|&v| at.is_none_or(|c| v > c));
    let Some(latest) = commits.last().copied().or(at) else {
        return Ok(None);
    };
    for pair in commits.windows(2) {
        if pair[1] != pair[0] + 1 {
            return Err(format!("the log has no commit {}, and has {}", pair[0] + 1, pair[1]));
        }
    }
    Ok(Some(Found {
        checkpoint,
        first: commits.first().copied(),
        latest,
    }))
}

/// The newest of the checkpoints `found` a reader can take: one in a file, or one in parts with every part listed.
/// Of two at one version, the one in a file.
fn newest(found: Vec<(Name, Entry)>) -> Option<(i64, Vec<Entry>)> {
    // Each version's parts, by the count of parts their names give.
    let mut parts: BTreeMap<(i64, u32), Vec<(u32, Entry)>> = BTreeMap::new();
    let mut best: Option<(i64, Vec<Entry>)> = None;
    for (name, entry) in found {
        match name {
            Name::Checkpoint(v) if best.as_ref().is_none_or(|(b, _)| *b < v) => {
                best = Some((v, vec![entry]));
            }
            Name::Part(v) => {
                if let Some((part, of)) = part_of(&entry.name) {
                    parts.entry((v, of)).or_default().push((part, entry));
                }
            }
            _ => {}
        }
    }
    for ((v, of), mut listed) in parts {
        if listed.len() == of as usize && best.as_ref().is_none_or(|(b, _)| *b < v) {
            listed.sort_by_key(|(part, _)| *part);
            best = Some((v, listed.into_iter().map(|(_, e)| e).collect()));
        }
    }
    best
}

/// Which part of how many the multi-part checkpoint file `name` is: `n.checkpoint.<part>.<of>.parquet`.
fn part_of(name: &str) -> Option<(u32, u32)> {
    let mut fields = name.split('.').skip(2);
    let part = fields.next()?.parse().ok()?;
    let of = fields.next()?.parse().ok()?;
    Some((part, of))
}

/// Reads the checkpoint at `version` in `files` into `state`, and gives it.
fn read_checkpoint(log: &Log, version: i64, files: Vec<Entry>, state: &mut State) -> Result<Checkpoint, String> {
    let mut ignored = Vec::new();
    for file in &files {
        if file.name.ends_with(".json") {
            log.lines(&file.name, |line| match Action::parse(line)? {
                // A checkpoint's file actions are read when the next is written, not here.
                Action::File(_) => Ok(()),
                action => state.apply(action, &mut ignored, ""),
            })?;
        } else {
            for action in checkpoint::top_level(log, &file.name, file.size)? {
                state.apply(action, &mut ignored, "")?;
            }
        }
    }
    Ok(Checkpoint {
        version,
        files,
        sidecars: std::mem::take(&mut state.sidecar_list),
    })
}

/// Milliseconds since the epoch.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// A random UUID, version 4, as text.
pub fn uuid() -> String {
    let mut b = crate::leader::auth::random_bytes(16);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use serde_json::json;

    use super::*;

    const APP: &str = "smeltr-app";

    fn new_log() -> Log {
        Log::new(
            Arc::new(InMemory::new()),
            crate::io::objects::runtime().unwrap(),
            Duration::from_secs(5),
            "t",
        )
    }

    fn write(log: &Log, name: &str, lines: &[String]) {
        log.put(name, lines.join("\n").into_bytes()).unwrap();
    }

    fn commit(log: &Log, version: i64, lines: &[String]) {
        write(log, &commit_name(version), lines);
    }

    fn protocol(reader: i32) -> String {
        json!({"protocol": {"minReaderVersion": reader, "minWriterVersion": 7,
            "readerFeatures": ["v2Checkpoint"], "writerFeatures": ["v2Checkpoint"]}})
        .to_string()
    }

    fn metadata(id: &str) -> String {
        json!({"metaData": {"id": id, "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[]}", "partitionColumns": [], "configuration": {}}})
        .to_string()
    }

    fn txn(app: &str, version: i64) -> String {
        json!({"txn": {"appId": app, "version": version}}).to_string()
    }

    fn add(path: &str) -> String {
        json!({"add": {"path": path, "partitionValues": {}, "size": 1, "modificationTime": 0, "dataChange": true}})
            .to_string()
    }

    fn remove(path: &str) -> String {
        json!({"remove": {"path": path, "deletionTimestamp": 9, "dataChange": true}}).to_string()
    }

    fn domain(name: &str, removed: bool) -> String {
        json!({"domainMetadata": {"domain": name, "configuration": "{}", "removed": removed}}).to_string()
    }

    fn table(log: &Log) {
        commit(log, 0, &[protocol(3), metadata("first")]);
    }

    fn load(log: &Log) -> Snapshot {
        Snapshot::load(log, APP).unwrap().unwrap()
    }

    fn v2_checkpoint(log: &Log, version: i64) -> String {
        let name = format!("{version:020}.checkpoint.{}.json", uuid());
        write(
            log,
            &name,
            &[
                json!({"checkpointMetadata": {"version": version}}).to_string(),
                protocol(3),
                metadata("checkpointed"),
                txn(APP, 7),
                json!({"sidecar": {"path": "a.parquet", "sizeInBytes": 10, "modificationTime": 1}}).to_string(),
                add("in-checkpoint"),
            ],
        );
        name
    }

    #[test]
    fn names_of_the_log_are_told_apart() {
        let v = "00000000000000000012";
        assert_eq!(Name::of(&format!("{v}.json")), Name::Commit(12));
        assert_eq!(Name::of(&format!("{v}.checkpoint.parquet")), Name::Checkpoint(12));
        let uuid = "3a0d65cd-4056-49b8-937b-95f9e3ee90e5";
        assert_eq!(Name::of(&format!("{v}.checkpoint.{uuid}.json")), Name::Checkpoint(12));
        assert_eq!(
            Name::of(&format!("{v}.checkpoint.{uuid}.parquet")),
            Name::Checkpoint(12)
        );
        assert_eq!(
            Name::of(&format!("{v}.checkpoint.0000000001.0000000003.parquet")),
            Name::Part(12)
        );
        assert_eq!(Name::of(&format!("{v}.crc")), Name::Other);
        assert_eq!(
            Name::of(&format!("{v}.00000000000000000015.compacted.json")),
            Name::Other
        );
        assert_eq!(Name::of("_last_checkpoint"), Name::Other);
        assert_eq!(Name::of("12.json"), Name::Other);
        assert_eq!(Name::of("000000000000000000012.json"), Name::Other);
        assert_eq!(Name::of(&format!("_sidecars/{v}.json")), Name::Other);
        assert_eq!(Name::Commit(12).version(), Some(12));
        assert_eq!(Name::Other.version(), None);
    }

    #[test]
    fn a_version_with_a_sign_is_no_name_of_the_log() {
        assert_eq!(Name::of("+0000000000000000001.json"), Name::Other);
        assert_eq!(Name::of("-0000000000000000001.json"), Name::Other);
    }

    #[test]
    fn each_action_parses_to_its_kind() {
        assert!(matches!(Action::parse(&protocol(3)).unwrap(), Action::Protocol(p) if p.min_reader_version == 3));
        assert!(matches!(Action::parse(&metadata("m")).unwrap(), Action::Metadata(m) if m.id == "m"));
        assert!(matches!(Action::parse(&txn("a", 4)).unwrap(), Action::Txn(t) if t.app_id == "a" && t.version == 4));
        assert!(matches!(Action::parse(&domain("d", true)).unwrap(), Action::Domain(d) if d.removed));
        let sidecar = json!({"sidecar": {"path": "s.parquet", "sizeInBytes": 3, "modificationTime": 4}});
        assert!(matches!(Action::parse(&sidecar.to_string()).unwrap(), Action::Sidecar(s) if s.size_in_bytes == 3));
        let cm = json!({"checkpointMetadata": {"version": 5}});
        assert!(matches!(Action::parse(&cm.to_string()).unwrap(), Action::CheckpointMetadata(c) if c.version == 5));
        let Action::File(a) = Action::parse(&add("p")).unwrap() else {
            panic!()
        };
        assert!(a.add && a.key == "p" && a.deleted.is_none());
        let Action::File(r) = Action::parse(&remove("p")).unwrap() else {
            panic!()
        };
        assert!(!r.add && r.key == "p" && r.deleted == Some(9));
    }

    #[test]
    fn commit_info_and_unknown_actions_are_other() {
        let info = json!({"commitInfo": {"timestamp": 1, "operation": "WRITE"}});
        assert!(matches!(Action::parse(&info.to_string()).unwrap(), Action::Other));
        let cdc = json!({"cdc": {"path": "c", "size": 1, "dataChange": false}});
        assert!(matches!(Action::parse(&cdc.to_string()).unwrap(), Action::Other));
        assert!(Action::parse("not json").is_err());
    }

    #[test]
    fn unknown_metadata_fields_are_carried_and_written_back() {
        let mut value: Value = serde_json::from_str(&metadata("m")).unwrap();
        value["metaData"]["futureField"] = json!({"x": [1, 2]});
        let Action::Metadata(m) = Action::parse(&value.to_string()).unwrap() else {
            panic!()
        };
        assert_eq!(m.rest.get("futureField"), Some(&json!({"x": [1, 2]})));
        let back: Value = serde_json::from_str(&line("metaData", &m)).unwrap();
        assert_eq!(back["metaData"]["futureField"], json!({"x": [1, 2]}));
        assert_eq!(back["metaData"]["id"], "m");
    }

    #[test]
    fn a_file_key_names_the_deletion_vector_and_its_offset() {
        let dv = |offset| DeletionVector {
            storage_type: "u".to_string(),
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset,
        };
        assert_eq!(file_key("p", None), "p");
        assert_eq!(file_key("p", Some(&dv(None))), "p\0uab^-aqEH.-t@S}K{vb[*k^");
        assert_eq!(file_key("p", Some(&dv(Some(4)))), "p\0uab^-aqEH.-t@S}K{vb[*k^@4");
        let line = json!({"add": {"path": "p", "deletionVector":
            {"storageType": "u", "pathOrInlineDv": "x", "offset": 1, "sizeInBytes": 2, "cardinality": 3}}});
        let Action::File(a) = Action::parse(&line.to_string()).unwrap() else {
            panic!()
        };
        assert_eq!(a.key, "p\0ux@1");
    }

    #[test]
    fn lines_skips_blank_lines_and_takes_a_last_line_without_newline() {
        let log = new_log();
        log.put("x.json", b"a\n\n  \nb\r\n\nc".to_vec()).unwrap();
        let mut seen = Vec::new();
        assert!(
            log.lines("x.json", |l| {
                seen.push(l.trim_end().to_string());
                Ok(())
            })
            .unwrap()
        );
        assert_eq!(seen, ["a", "b", "c"]);
        assert!(!log.lines("missing.json", |_| Ok(())).unwrap());
    }

    #[test]
    fn create_writes_only_where_there_is_nothing() {
        let log = new_log();
        assert!(log.create(&commit_name(0), b"one".to_vec()).unwrap());
        assert!(!log.create(&commit_name(0), b"two".to_vec()).unwrap());
        assert_eq!(&log.get(&commit_name(0)).unwrap().unwrap()[..], b"one");
    }

    #[test]
    fn a_table_with_no_log_loads_as_none() {
        assert!(Snapshot::load(&new_log(), APP).unwrap().is_none());
    }

    #[test]
    fn commits_without_a_checkpoint_are_read_from_zero_and_later_ones_replace_protocol_and_metadata() {
        let log = new_log();
        table(&log);
        commit(&log, 1, &[add("a")]);
        commit(&log, 2, &[protocol(4), metadata("second")]);
        let s = load(&log);
        assert_eq!(s.version, 2);
        assert_eq!(s.protocol.min_reader_version, 4);
        assert_eq!(s.metadata.id, "second");
        assert!(s.checkpoint.is_none());
        assert_eq!(s.since.count, 1);
    }

    #[test]
    fn the_latest_transaction_of_each_application_is_kept() {
        let log = new_log();
        commit(&log, 0, &[protocol(3), metadata("m"), txn("a", 1), txn("b", 1)]);
        commit(&log, 1, &[txn("a", 2)]);
        let s = load(&log);
        assert_eq!(s.txns["a"].version, 2);
        assert_eq!(s.txns["b"].version, 1);
    }

    #[test]
    fn a_removed_domain_is_left_out() {
        let log = new_log();
        commit(
            &log,
            0,
            &[protocol(3), metadata("m"), domain("a", false), domain("b", false)],
        );
        commit(&log, 1, &[domain("a", true)]);
        let s = load(&log);
        assert_eq!(s.domains.keys().collect::<Vec<_>>(), ["b"]);
    }

    #[test]
    fn a_gap_in_the_commits_is_an_error() {
        let log = new_log();
        table(&log);
        commit(&log, 1, &[add("a")]);
        commit(&log, 3, &[add("b")]);
        assert!(Snapshot::load(&log, APP).is_err());
    }

    #[test]
    fn commits_that_start_after_zero_without_a_checkpoint_are_an_error() {
        let log = new_log();
        commit(&log, 2, &[protocol(3), metadata("m")]);
        assert!(Snapshot::load(&log, APP).is_err());
    }

    #[test]
    fn a_checkpoint_in_parts_is_taken_only_with_every_part_listed() {
        let log = new_log();
        table(&log);
        commit(&log, 1, &[add("a")]);
        commit(&log, 2, &[add("b")]);
        v2_checkpoint(&log, 2);
        commit(&log, 3, &[add("c")]);
        let part = |i: u32| format!("{:020}.checkpoint.{i:010}.0000000002.parquet", 3);
        log.put(&part(1), Vec::new()).unwrap();
        assert_eq!(load(&log).checkpoint.unwrap().version, 2);

        let mut found = Vec::new();
        log.list(None, |e| {
            found.push((Name::of(&e.name), e));
            true
        })
        .unwrap();
        found.push((
            Name::of(&part(2)),
            Entry {
                name: part(2),
                size: 0,
                modified: 0,
            },
        ));
        let (version, files) = newest(found).unwrap();
        assert_eq!(version, 3);
        assert_eq!(
            files.iter().map(|f| f.name.clone()).collect::<Vec<_>>(),
            [part(1), part(2)]
        );
    }

    fn checkpointed_table(log: &Log) -> String {
        // Commits up to the checkpoint are not read: these would fail to parse.
        for v in 0..=2 {
            commit(log, v, &["not json".to_string()]);
        }
        let name = v2_checkpoint(log, 2);
        commit(log, 3, &[add("x"), txn("other", 1)]);
        commit(log, 4, &[metadata("after")]);
        name
    }

    fn assert_checkpointed(s: &Snapshot, name: &str) {
        assert_eq!(s.version, 4);
        let c = s.checkpoint.as_ref().unwrap();
        assert_eq!((c.version, c.files[0].name.as_str()), (2, name));
        assert_eq!(c.sidecars.len(), 1);
        assert_eq!(c.sidecars[0].path, "a.parquet");
        assert_eq!(s.metadata.id, "after");
        assert_eq!(s.protocol.min_reader_version, 3);
        assert_eq!(s.our_version(), Some(7));
        assert_eq!(s.txns["other"].version, 1);
        // The checkpoint's own add is not among the commits since it.
        assert_eq!(s.since.count, 1);
        assert_eq!(s.since.actions[0].key, "x");
    }

    #[test]
    fn a_v2_checkpoint_and_the_commits_after_it_are_read_without_last_checkpoint() {
        let log = new_log();
        let name = checkpointed_table(&log);
        assert_checkpointed(&load(&log), &name);
    }

    #[test]
    fn a_v2_checkpoint_and_the_commits_after_it_are_read_from_last_checkpoint() {
        let log = new_log();
        let name = checkpointed_table(&log);
        log.put(
            "_last_checkpoint",
            json!({"version": 2, "size": 6}).to_string().into_bytes(),
        )
        .unwrap();
        assert_checkpointed(&load(&log), &name);
    }

    #[test]
    fn a_last_checkpoint_naming_a_deleted_checkpoint_falls_back_to_the_listing() {
        let log = new_log();
        let name = checkpointed_table(&log);
        log.put(
            "_last_checkpoint",
            json!({"version": 3, "size": 6}).to_string().into_bytes(),
        )
        .unwrap();
        assert_checkpointed(&load(&log), &name);
    }

    #[test]
    fn a_checkpoint_with_no_commit_after_it_is_the_table() {
        let log = new_log();
        v2_checkpoint(&log, 5);
        let s = load(&log);
        assert_eq!(s.version, 5);
        assert_eq!(s.since.count, 0);
    }

    #[test]
    fn a_missing_commit_after_the_checkpoint_is_an_error() {
        let log = new_log();
        v2_checkpoint(&log, 2);
        commit(&log, 4, &[add("x")]);
        assert!(Snapshot::load(&log, APP).is_err());
    }

    #[test]
    fn our_commits_touch_nothing_and_others_touch_their_keys_at_their_last_position() {
        let log = new_log();
        commit(&log, 0, &[protocol(3), metadata("m"), add("mine0"), txn(APP, 1)]);
        commit(&log, 1, &[add("a"), remove("b")]);
        commit(&log, 2, &[txn(APP, 2), add("mine2")]);
        commit(&log, 3, &[remove("a"), txn("other", 1)]);
        let s = load(&log);
        assert_eq!(s.since.count, 5);
        assert_eq!(s.since.actions.len(), 5);
        assert!(!s.since.spilled);
        let mut touched: Vec<_> = s.since.touched.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        touched.sort();
        assert_eq!(touched, [("a", 4), ("b", 2)]);
        assert_eq!(s.our_version(), Some(2));
    }

    #[test]
    fn advance_reads_new_commits_until_the_first_missing() {
        let log = new_log();
        table(&log);
        let mut s = load(&log);
        assert_eq!(s.advance(&log).unwrap(), 0);
        commit(&log, 1, &[add("a")]);
        commit(&log, 2, &[metadata("later"), domain("d", false)]);
        commit(&log, 4, &[add("b")]);
        assert_eq!(s.advance(&log).unwrap(), 2);
        assert_eq!(s.version, 2);
        assert_eq!(s.metadata.id, "later");
        assert!(s.domains.contains_key("d"));
        assert_eq!(s.since.touched.get("a"), Some(&0));
    }

    #[test]
    fn a_commit_holding_checkpoint_actions_is_an_error() {
        let log = new_log();
        table(&log);
        let mut s = load(&log);
        commit(&log, 1, &[json!({"checkpointMetadata": {"version": 1}}).to_string()]);
        assert!(s.advance(&log).is_err());
    }

    #[test]
    fn committed_advances_the_version_and_records_the_transaction() {
        let log = new_log();
        table(&log);
        let mut s = load(&log);
        let actions = [txn(APP, 3), add("mine"), metadata("new"), domain("d", false)]
            .iter()
            .map(|l| Action::parse(l).unwrap())
            .collect();
        s.committed(actions);
        assert_eq!(s.version, 1);
        assert_eq!(s.our_version(), Some(3));
        assert_eq!(s.metadata.id, "new");
        assert!(s.domains.contains_key("d"));
        assert_eq!(s.since.count, 1);
        assert!(s.since.touched.is_empty());
    }
}
