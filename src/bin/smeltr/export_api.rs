//! A pipeline's export: a bundle in a zip, holding the pipeline and what it reads (`config_file::export`), each lookup
//! file as its bytes rather than the pointer a commit holds, and the samples and tests it includes. Importing
//! reads it back through the same loader as any bundle.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use axum::extract::{Path as AxPath, Query};
use axum::http::header;
use axum::response::{IntoResponse, Response};

use smeltr::config_file::{
    self, FileConfig, FileSet, LookupSourceDto, TestSummary, list_tests, sample_path, slugify, test_path,
};
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::leader::{blocking, blocking_answer};
use smeltr::startup_config::leader_data_dir;

use crate::config_api::{ConfigQuery, Reading, merged_file};

#[derive(serde::Deserialize)]
pub struct ExportQuery {
    #[serde(flatten)]
    config: ConfigQuery,
    /// Include the routes that run the pipeline, with their sources and destinations.
    #[serde(default)]
    routes: bool,
}

/// Pipeline `id`'s export from `area`, as a zip to download. The zip is written as it is sent, so a large lookup file
/// is never held in memory.
pub async fn export_pipeline(AxPath((area, id)): AxPath<(String, String)>, Query(q): Query<ExportQuery>) -> Response {
    let files = blocking(move || exported(&leader_data_dir(), &area, &id, &q).map(|(_, files)| (id, files)));
    let (id, files) = match files.await {
        Ok(found) => found,
        Err(e) => return Envelope::<()>::refused(e).into_response(),
    };
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::task::spawn_blocking(move || {
        // Once the headers are sent, a failure can only end the body early.
        if let Err(e) = zipped(
            &files,
            std::io::BufWriter::with_capacity(64 * 1024, Sending(tx.clone())),
        ) {
            let _ = tx.blocking_send(Err(e));
        }
    });
    // A fused stream: it answers 'ended' again when asked after its end, because the compression layer asks once more.
    let body = futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|chunk| (chunk, rx)) });
    let body = futures_util::StreamExt::fuse(body);
    let disposition = format!("attachment; filename=\"{}.zip\"", slugify(&id));
    let headers = [
        (header::CONTENT_TYPE, "application/zip".to_string()),
        (header::CONTENT_DISPOSITION, disposition),
    ];
    (headers, axum::body::Body::from_stream(body)).into_response()
}

/// A writer that hands what it is given to the response body, waiting while the body is behind.
struct Sending(tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>);

impl Write for Sending {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .blocking_send(Ok(bytes::Bytes::copy_from_slice(buf)))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "the download was cancelled"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// What pipeline `id`'s export from `area` holds, by kind and id, for a person to see before downloading it.
#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Contents {
    pipelines: Vec<String>,
    lookup_tables: Vec<TableContents>,
    grok_patterns: Vec<String>,
    schemas: Vec<String>,
    samples: Vec<String>,
    tests: Vec<String>,
    routes: Vec<String>,
    sources: Vec<String>,
    destinations: Vec<String>,
    secrets: Vec<String>,
    certificates: Vec<String>,
}

/// A lookup table, and the file it reads with that file's size. An inline table has no file.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TableContents {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes: Option<usize>,
}

pub async fn export_contents(
    AxPath((area, id)): AxPath<(String, String)>,
    Query(q): Query<ExportQuery>,
) -> Envelope<Contents> {
    blocking_answer(move || contents_in(&leader_data_dir(), &area, &id, &q).into()).await
}

fn contents_in(data_dir: &Path, area: &str, id: &str, q: &ExportQuery) -> Result<Contents, Refusal> {
    let (export, files) = exported(data_dir, area, id, q)?;
    let ids = |items: Vec<&String>| items.into_iter().cloned().collect();
    let file = &export.file;
    Ok(Contents {
        pipelines: ids(file.pipelines.iter().map(|p| &p.id).collect()),
        lookup_tables: (file.lookup_tables.iter())
            .map(|t| {
                let path = table_file(&t.source).map(str::to_string);
                TableContents {
                    id: t.id.clone(),
                    bytes: path.as_ref().and_then(|p| files.get(p)).map(|held| held.len()),
                    file: path,
                }
            })
            .collect(),
        grok_patterns: ids(file.grok_patterns.iter().map(|g| &g.id).collect()),
        schemas: ids(file.schemas.iter().map(|s| &s.id).collect()),
        samples: export.samples,
        tests: export.tests,
        routes: ids(file.routes.iter().map(|r| &r.id).collect()),
        sources: ids(file.sources.iter().map(|s| &s.id).collect()),
        destinations: ids(file.destinations.iter().map(|d| &d.id).collect()),
        secrets: export.secrets.into_iter().collect(),
        certificates: export.certificates.into_iter().collect(),
    })
}

/// The file a lookup table reads, relative to its bundle. An inline table reads none.
pub(crate) fn table_file(source: &LookupSourceDto) -> Option<&str> {
    match source {
        LookupSourceDto::Csv { path, .. } | LookupSourceDto::Mmdb { path } => Some(path),
        LookupSourceDto::Inline { .. } => None,
    }
}

/// A file of the export: its bytes, or a stored lookup file mapped where the store keeps it, so that even a large
/// one is not copied into memory.
enum Held {
    Bytes(Vec<u8>),
    Mapped(memmap2::Mmap),
}

impl std::ops::Deref for Held {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Held::Bytes(bytes) => bytes,
            Held::Mapped(map) => map,
        }
    }
}

/// What pipeline `id`'s export from `area` holds, and every file of it by its path in the bundle.
fn exported(
    data_dir: &Path,
    area: &str,
    id: &str,
    q: &ExportQuery,
) -> Result<(config_file::Export, BTreeMap<String, Held>), Refusal> {
    let own = q.config.open_in(data_dir, area)?;
    // `shared/` is read at the same commit, so the two halves of what the group runs are of one moment.
    let shared = match own.group_id() {
        Some(_) => Some(own.sibling(config_file::SHARED_DIR)?),
        None => None,
    };
    // Each scope with its file, `shared/` first, as the group runs them.
    let scopes: Vec<(&Reading, FileConfig)> = shared.iter().chain([&own]).map(|r| (r, r.raw().file)).collect();
    let merged = match own.group_id() {
        Some(group) if scopes.len() == 2 => merged_file(&scopes[0].1, &scopes[1].1, group),
        _ => scopes[scopes.len() - 1].1.clone(),
    };
    let tests: Vec<TestSummary> = (scopes.iter())
        .flat_map(|(scope, _)| list_tests(&mut scope.files()).0)
        .collect();
    let export = config_file::export(&merged, &tests, id, q.routes)
        .ok_or_else(|| Refusal::not_found(format!("no pipeline {id:?}")))?;

    let mut files: BTreeMap<String, Held> = config_file::bundle_files(&export.file)
        .map_err(|e| Refusal::internal(e.to_string()))?
        .into_iter()
        .map(|(path, bytes)| (path, Held::Bytes(bytes)))
        .collect();
    for table in &export.file.lookup_tables {
        let Some(path) = table_file(&table.source) else {
            continue;
        };
        let from = declared_in(&scopes, |file| file.lookup_tables.iter().any(|t| t.id == table.id));
        files.insert(path.to_string(), lookup_bytes(from, path)?);
    }
    let samples = export.samples.iter().map(|s| sample_path(s));
    for path in samples.chain(export.tests.iter().map(|t| test_path(t))) {
        let path = path.map_err(|e| Refusal::internal(e.to_string()))?;
        files.insert(path.clone(), Held::Bytes(read_from(&scopes, &path)?));
    }
    Ok((export, files))
}

/// The scope that declares what `declares` finds. An id is declared once in what a group runs, so the order does not
/// matter.
fn declared_in<'a>(scopes: &[(&'a Reading, FileConfig)], declares: impl Fn(&FileConfig) -> bool) -> &'a Reading {
    let found = scopes.iter().find(|(_, file)| declares(file));
    found.unwrap_or(&scopes[scopes.len() - 1]).0
}

/// The lookup file at `path` in `scope`, mapped where the store holds it.
fn lookup_bytes(scope: &Reading, path: &str) -> Result<Held, Refusal> {
    let file = smeltr::plan::config::LookupFile {
        text: path.to_string(),
        at: scope.at().lookup(path).map_err(|e| Refusal::not_found(e.to_string()))?,
    };
    Ok(Held::Mapped(file.map().map_err(|e| Refusal::internal(e.to_string()))?))
}

/// The file at `path` in the first scope, the group's own first, that holds one.
fn read_from(scopes: &[(&Reading, FileConfig)], path: &str) -> Result<Vec<u8>, Refusal> {
    for (scope, _) in scopes.iter().rev() {
        if let Some(bytes) = scope.files().read(path).map_err(|e| Refusal::internal(e.to_string()))? {
            return Ok(bytes);
        }
    }
    Err(Refusal::internal(format!("{path} is gone")))
}

/// `files` as a zip, each at its path, written to `out` as it is made.
fn zipped(files: &BTreeMap<String, Held>, out: impl Write) -> std::io::Result<()> {
    let mut zip = zip::ZipWriter::new_stream(out);
    let options = zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    for (path, bytes) in files {
        zip.start_file(path.as_str(), options)?;
        zip.write_all(bytes)?;
    }
    zip.finish()?.into_inner().flush()
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use serde_json::json;
    use smeltr::leader::git;
    use smeltr::leader::store::Actor;

    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-export-api-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// `bytes` put in `repo`'s lookup store, as the pointer a commit holds for them.
    fn stored(repo: &Path, bytes: &[u8]) -> Vec<u8> {
        let mut incoming = smeltr::lookup::store::Incoming::new(repo).unwrap();
        std::io::Write::write_all(&mut incoming, bytes).unwrap();
        incoming.finish(None).unwrap().text().into_bytes()
    }

    const SITES: &[u8] = b"ip,site\n10.0.0.1,lab\n";

    /// `main` holding a `shared/` table that a pipeline of group `west` reads, with a test and its sample beside it.
    fn seeded(label: &str) -> std::path::PathBuf {
        let data = temp_dir(label);
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let shared: FileConfig = serde_json::from_value(json!({"version": 1,
            "lookupTables": [{"id": "sites", "type": "csv", "path": "lookups/sites.csv", "delimiter": ",",
                              "headers": ["ip", "site"]}]}))
        .unwrap();
        let west: FileConfig = serde_json::from_value(json!({"version": 1,
            "pipelines": [
                {"id": "audit", "name": "audit", "steps": [
                    {"id": "ip", "type": "eval", "assignments": [{"target": "ip", "expr": "\"10.0.0.1\""}]},
                    {"type": "lookup", "id": "site", "input": "ip", "table": "sites", "matchKind": "exact",
                     "keyColumn": "ip", "outputs": [{"source": "site", "field": "site"}]}]},
                {"id": "other", "name": "other", "steps": []}]}))
        .unwrap();
        let mut edits: Vec<git::FileEdit> = Vec::new();
        for (prefix, file) in [("shared", &shared), ("groups/west", &west)] {
            edits.extend(
                config_file::bundle_files(file)
                    .unwrap()
                    .into_iter()
                    .map(|(path, content)| git::FileEdit {
                        path: format!("{prefix}/{path}"),
                        content: Some(content),
                    }),
            );
        }
        let sample: config_file::SampleDto =
            serde_json::from_value(json!({"id": "s1", "name": "one", "records": [{"_raw": "x"}]})).unwrap();
        let test: config_file::TestDto = serde_json::from_value(json!({"id": "t1", "name": "t", "pipeline": "audit",
            "sample": "s1", "lockedAt": 0, "rowsIn": 1, "expected": [null]}))
        .unwrap();
        edits.extend([
            git::FileEdit {
                path: "shared/lookups/sites.csv".into(),
                content: Some(stored(&repo, SITES)),
            },
            git::FileEdit {
                path: format!("groups/west/{}", sample_path("s1").unwrap()),
                content: Some(config_file::sample_bytes(&sample)),
            },
            git::FileEdit {
                path: format!("groups/west/{}", test_path("t1").unwrap()),
                content: Some(config_file::test_bytes(&test)),
            },
        ]);
        let parent = git::main_head(&repo).unwrap();
        git::commit_edits(&repo, "seed", &parent, None, &edits, "seed", &Actor::system()).unwrap();
        git::open_change(&repo, "seed", "seed", "", &Actor::system()).unwrap();
        git::deploy_change(&data, "seed", &Actor::system()).expect("the seed deploys");
        data
    }

    fn query() -> ExportQuery {
        serde_json::from_value(json!({})).unwrap()
    }

    fn unzipped(bytes: Vec<u8>) -> BTreeMap<String, Vec<u8>> {
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        (0..zip.len())
            .map(|i| {
                let mut file = zip.by_index(i).unwrap();
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).unwrap();
                (file.name().unwrap().to_string(), bytes)
            })
            .collect()
    }

    #[test]
    fn an_export_is_a_bundle_holding_the_lookup_bytes_and_the_tests_samples() {
        let data = seeded("whole");
        let (_, files) = exported(&data, "west", "audit", &query()).unwrap();
        let mut zip = Vec::new();
        zipped(&files, &mut zip).unwrap();
        let files = unzipped(zip);
        assert_eq!(
            files["lookups/sites.csv"], SITES,
            "the bytes, not the pointer a commit holds"
        );
        assert!(files.contains_key("pipelines/audit.json") && !files.contains_key("pipelines/other.json"));
        assert!(files.contains_key("samples/s1.json") && files.contains_key("tests/t1.json"));

        // Written out, it loads as a bundle of its own, the lookup file read from beside it.
        let dir = data.join("unzipped");
        for (path, bytes) in &files {
            std::fs::create_dir_all(dir.join(path).parent().unwrap()).unwrap();
            std::fs::write(dir.join(path), bytes).unwrap();
        }
        let loaded = config_file::load_bundle_dir(&dir).expect("the export loads");
        drop(loaded);
        let _ = std::fs::remove_dir_all(&data);
    }

    #[test]
    fn a_pipeline_the_area_lacks_is_not_found() {
        let data = seeded("missing");
        let refused = exported(&data, "west", "nope", &query()).err().unwrap();
        assert!(refused.message().contains("no pipeline"), "{}", refused.message());
        let _ = std::fs::remove_dir_all(&data);
    }

    #[test]
    fn the_contents_name_what_the_export_holds() {
        let data = seeded("contents");
        let contents = contents_in(&data, "west", "audit", &query()).unwrap();
        assert_eq!(contents.pipelines, ["audit"]);
        let table = &contents.lookup_tables[0];
        assert_eq!(
            (table.id.as_str(), table.file.as_deref(), table.bytes),
            ("sites", Some("lookups/sites.csv"), Some(SITES.len()))
        );
        assert_eq!(
            (contents.samples, contents.tests),
            (vec!["s1".to_string()], vec!["t1".to_string()])
        );
        assert!(contents.routes.is_empty() && contents.secrets.is_empty());
        let _ = std::fs::remove_dir_all(&data);
    }
}
