//! Importing a pipeline's export into an area: the zip is unpacked into a directory of its own and read with the
//! loader that reads every bundle, compared with what the area holds (`config_file::Objects`), and written as one
//! save once every taken id and every credential has an answer.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use axum::extract::{Extension, Multipart, State};
use axum::http::StatusCode;

use smeltr::config_file::{
    self, Credential, FileConfig, FileSet, Keep, Listed, LookupSourceDto, Objects, Part, SampleDto, SecretRefDto,
    TestDto, read_sample, read_test,
};
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::leader::store::Actor;
use smeltr::leader::tree::TreeFiles;
use smeltr::leader::{blocking, blocking_answer};

use crate::config_api::{Addressed, Naming, Touches, Writing, group_of, merged_file, store_lookup_file};

/// The most entries an export may hold: far more than one pipeline's objects, and few enough to unpack at once.
const MAX_ENTRIES: usize = 10_000;

/// What an import answers: each object it holds and how it stands, the credentials it names, and the commit the area
/// was read at, which the answers are sent against.
#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Import {
    listed: Vec<Listed>,
    /// What the sources and destinations that are not the same as one here name.
    credentials: Vec<Credential>,
    #[serde(skip_serializing_if = "Option::is_none")]
    at: Option<String>,
    /// The commit the import was saved in. Absent until it is.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

/// The importer's answers: a side or a new id for each taken id, and the secret or certificate here for each one
/// the import names.
#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Answers {
    #[serde(default)]
    taken: Vec<TakenAnswer>,
    #[serde(default)]
    secrets: BTreeMap<String, String>,
    #[serde(default)]
    certificates: BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TakenAnswer {
    part: Part,
    id: String,
    #[serde(flatten)]
    keep: Keep,
}

/// Import the export in the `file` field into the area and branch the request names. Without an `answers` field it
/// writes nothing and answers what the export holds; with one, it saves, or answers 409 with what still needs one.
pub async fn import(
    State(state): State<smeltr::leader::AppState>,
    Extension(actor): Extension<Actor>,
    at: Addressed,
    mut multipart: Multipart,
) -> Envelope<Import> {
    // Opened first: the zip is unpacked beside the repository, which a first save creates.
    let writing = match blocking(move || at.open()).await {
        Ok(writing) => writing,
        Err(e) => return Envelope::refused(e),
    };
    let unpacking = match Unpacking::new(writing.repo()) {
        Ok(unpacking) => unpacking,
        Err(e) => return Envelope::refused(Refusal::internal(format!("couldn't receive the export: {e}"))),
    };
    let mut answers: Option<Answers> = None;
    let mut received = false;
    loop {
        let mut field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(e) => return Envelope::refused(Refusal::bad_request(format!("malformed upload: {e}"))),
        };
        match field.name() {
            Some("file") => {
                let mut zip = match std::fs::File::create(unpacking.zip()) {
                    Ok(zip) => zip,
                    Err(e) => return Envelope::refused(Refusal::internal(format!("couldn't receive the export: {e}"))),
                };
                // Streamed to disk as it arrives: an export holds its lookup files, which can be large.
                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => {
                            if let Err(e) = tokio::task::block_in_place(|| zip.write_all(&chunk)) {
                                let message = format!("couldn't receive the export: {e}");
                                return Envelope::refused(Refusal::internal(message));
                            }
                        }
                        Ok(None) => break,
                        Err(e) => return Envelope::refused(Refusal::bad_request(format!("couldn't read upload: {e}"))),
                    }
                }
                received = true;
            }
            Some("answers") => match field.bytes().await.map(|b| serde_json::from_slice(&b)) {
                Ok(Ok(parsed)) => answers = Some(parsed),
                Ok(Err(e)) => return Envelope::refused(Refusal::bad_request(format!("malformed answers: {e}"))),
                Err(e) => return Envelope::refused(Refusal::bad_request(format!("couldn't read upload: {e}"))),
            },
            _ => {}
        }
    }
    if !received {
        return Envelope::refused(Refusal::bad_request("no `file` field in this upload"));
    }
    let store = state.store.clone();
    let imported = blocking_answer(move || {
        let secrets = match store.list_secrets() {
            Ok(records) => records,
            Err(e) => return Envelope::refused(Refusal::internal(e.to_string())),
        };
        imported(writing, &unpacking, answers, &secrets, &actor)
    })
    .await;
    smeltr::leader::prune_lookups(&state).await;
    imported
}

/// The export unpacked under `unpacking`, compared with what `writing`'s area holds, and saved when `answers` cover
/// everything that needs one. Blocking.
fn imported(
    mut writing: Writing,
    unpacking: &Unpacking,
    answers: Option<Answers>,
    secrets_here: &[smeltr::leader::store::SecretRecord],
    author: &Actor,
) -> Envelope<Import> {
    let objects = match unpacking.unpack().and_then(|()| unpacking.objects()) {
        Ok(objects) => objects,
        Err(e) => return Envelope::refused(e),
    };
    let here = match here(&writing, &objects) {
        Ok(here) => here,
        Err(e) => return Envelope::refused(e),
    };
    let listing = Import {
        listed: objects.list(&here),
        credentials: objects.unlike(&here).credentials(),
        at: Some(writing.parent.clone()),
        commit: None,
    };
    let Some(answers) = answers else {
        return Envelope::ok(listing);
    };
    let still_needed = |errors: Vec<String>| {
        let errors = errors
            .into_iter()
            .map(smeltr::leader::envelope::ApiError::new)
            .collect();
        Envelope::with(
            Import {
                listed: objects.list(&here),
                credentials: objects.unlike(&here).credentials(),
                at: Some(writing.parent.clone()),
                commit: None,
            },
            StatusCode::CONFLICT,
            errors,
        )
    };

    let keeps = (answers.taken.into_iter()).map(|a| ((a.part, a.id), a.keep)).collect();
    let mut written = match objects.clone().resolve(&here, &keeps) {
        Ok(written) => written,
        Err(errors) => return still_needed(errors),
    };
    let mut unmatched = Vec::new();
    for credential in written.credentials() {
        let (name, to) = (
            &credential.name,
            match credential.certificate {
                false => answers.secrets.get(&credential.name),
                true => answers.certificates.get(&credential.name),
            },
        );
        let Some(record) = to.and_then(|to| {
            secrets_here
                .iter()
                .find(|r| &r.name == to && r.certificate == credential.certificate)
        }) else {
            let kind = if credential.certificate {
                "certificate"
            } else {
                "secret"
            };
            unmatched.push(format!("{kind} {name:?} needs a match here"));
            continue;
        };
        if credential.certificate {
            written.match_certificate(name, &record.name);
        } else {
            written.match_secret(name, &matched(record));
        }
    }
    if !unmatched.is_empty() {
        return still_needed(unmatched);
    }

    let mut staged = Vec::new();
    for table in &mut written.file.lookup_tables {
        match stored(&writing, unpacking, table) {
            Ok(Some(edit)) => staged.push(Ok(edit)),
            Ok(None) => {}
            Err(e) => return Envelope::refused(e),
        }
    }
    for sample in &written.samples {
        staged.push(file_edit(
            config_file::sample_path(&sample.id),
            config_file::sample_bytes(sample),
        ));
    }
    for test in &written.tests {
        staged.push(file_edit(
            config_file::test_path(&test.id),
            config_file::test_bytes(test),
        ));
    }
    let staged: Result<Vec<_>, _> = staged.into_iter().collect();
    if let Err(e) = staged.and_then(|staged| writing.stage(staged)) {
        return Envelope::refused(e);
    }
    let first = objects.root().unwrap_or_default();
    let saved = writing.save(Touches::Every, &format!("import pipeline {first}"), author, |file| {
        upsert(file, written.file)
    });
    match saved.body.commit() {
        Some(commit) => Envelope::ok(Import {
            commit: Some(commit.to_string()),
            ..listing
        }),
        None => Envelope::with(listing, saved.status, saved.errors),
    }
}

/// The reference a match to `record` writes: its latest version when the leader holds it, its name otherwise.
fn matched(record: &smeltr::leader::store::SecretRecord) -> SecretRefDto {
    match record.versions.last() {
        Some(latest) => SecretRefDto::LeaderStored(config_file::LeaderSecretDto {
            secret: record.name.clone(),
            version: latest.version,
        }),
        None => SecretRefDto::WorkerLocal(record.name.clone()),
    }
}

fn file_edit(
    path: Result<String, config_file::BundleError>,
    bytes: Vec<u8>,
) -> Result<smeltr::leader::git::FileEdit, Refusal> {
    let path = path.map_err(|e| Refusal::invalid(e.to_string()))?;
    Ok(smeltr::leader::git::FileEdit {
        path,
        content: Some(bytes),
    })
}

/// Each of `written`'s objects put into `file`, replacing one of its kind and id.
fn upsert(file: &mut FileConfig, written: FileConfig) {
    fn put<T>(into: &mut Vec<T>, items: Vec<T>, id: impl Fn(&T) -> &str) {
        for item in items {
            match into.iter().position(|o| id(o) == id(&item)) {
                Some(i) => into[i] = item,
                None => into.push(item),
            }
        }
    }
    put(&mut file.pipelines, written.pipelines, |o| &o.id);
    put(&mut file.lookup_tables, written.lookup_tables, |o| &o.id);
    put(&mut file.grok_patterns, written.grok_patterns, |o| &o.id);
    put(&mut file.schemas, written.schemas, |o| &o.id);
    put(&mut file.routes, written.routes, |o| &o.id);
    put(&mut file.sources, written.sources, |o| &o.id);
    put(&mut file.destinations, written.destinations, |o| &o.id);
}

/// Puts `table`'s file from the export into the store and points `table` at it, answering the pointer to commit.
/// An inline table has no file.
fn stored(
    writing: &Writing,
    unpacking: &Unpacking,
    table: &mut config_file::LookupTableDto,
) -> Result<Option<smeltr::leader::git::FileEdit>, Refusal> {
    let (path, delimiter) = match &mut table.source {
        LookupSourceDto::Csv { path, delimiter, .. } => {
            let delimiter = config_file::to_byte(delimiter, "delimiter").map_err(Refusal::invalid)?;
            (path, delimiter)
        }
        LookupSourceDto::Mmdb { path } => (path, b','),
        LookupSourceDto::Inline { .. } => return Ok(None),
    };
    let failed = |e: &dyn std::fmt::Display| Refusal::internal(format!("couldn't store lookup file {path:?}: {e}"));
    let mut from = std::fs::File::open(unpacking.dir().join(&*path)).map_err(|e| failed(&e))?;
    let mut incoming = smeltr::lookup::store::Incoming::new(writing.repo()).map_err(|e| failed(&e))?;
    std::io::copy(&mut from, &mut incoming).map_err(|e| failed(&e))?;
    // Named as it was uploaded: the store puts the content hash in front of it again.
    let name = path.rsplit('/').next().unwrap_or(path);
    let name = strip_content_prefix(name);
    let stored = store_lookup_file(
        writing.repo(),
        writing.prefix(),
        name,
        incoming,
        delimiter,
        Naming::ByContent,
    )?;
    *path = stored.path;
    Ok(Some(stored.edit))
}

/// `name` without the content hash prefix the store names an upload with, when it has one.
fn strip_content_prefix(name: &str) -> &str {
    match name.split_once('-') {
        Some((hash, rest)) if hash.len() == 16 && hash.bytes().all(|b| b.is_ascii_hexdigit()) => rest,
        _ => name,
    }
}

/// What `writing`'s area holds of the kinds `imported` holds: what the group runs, `shared/` merged with its own, and
/// the samples and tests of the ids the import holds.
fn here(writing: &Writing, imported: &Objects) -> Result<Objects, Refusal> {
    let (repo, commit) = (writing.repo(), writing.parent.as_str());
    let own = writing.read().file;
    let file = match group_of(writing.prefix()) {
        Some(group) => {
            let shared = config_file::read_raw_from(&mut TreeFiles::new(repo, commit, config_file::SHARED_DIR));
            merged_file(&shared.file, &own, group)
        }
        None => own,
    };
    let scopes = [writing.prefix().to_string(), config_file::SHARED_DIR.to_string()];
    let failed = |e: config_file::BundleError| Refusal::internal(e.to_string());
    let (mut samples, mut tests) = (Vec::<SampleDto>::new(), Vec::<TestDto>::new());
    for scope in scopes
        .iter()
        .take(if group_of(writing.prefix()).is_some() { 2 } else { 1 })
    {
        let mut files = TreeFiles::new(repo, commit, scope.clone());
        for s in &imported.samples {
            if let Some(found) = read_sample(&mut files, &s.id).map_err(failed)? {
                samples.push(found);
            }
        }
        for t in &imported.tests {
            if let Some(found) = read_test(&mut files, &t.id).map_err(failed)? {
                tests.push(found);
            }
        }
    }
    Ok(Objects { file, samples, tests })
}

/// The directory an upload is unpacked into, beside the repository, removed when this is dropped.
struct Unpacking {
    root: PathBuf,
}

impl Unpacking {
    fn new(repo: &Path) -> std::io::Result<Unpacking> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let parent = repo.parent().unwrap_or(repo);
        let root = parent.join(format!(".import-{}-{n}", std::process::id()));
        std::fs::create_dir_all(root.join("bundle"))?;
        Ok(Unpacking { root })
    }

    fn zip(&self) -> PathBuf {
        self.root.join("export.zip")
    }

    fn dir(&self) -> PathBuf {
        self.root.join("bundle")
    }

    /// Unpacks the zip into `dir`. An entry whose name would land outside it, a link, too many entries or more bytes
    /// than the lookup store takes in all are refused.
    fn unpack(&self) -> Result<(), Refusal> {
        let refused = |e: &dyn std::fmt::Display| Refusal::invalid(format!("this file is not an export: {e}"));
        let file = std::fs::File::open(self.zip()).map_err(|e| Refusal::internal(e.to_string()))?;
        let mut zip = zip::ZipArchive::new(file).map_err(|e| refused(&e))?;
        if zip.len() > MAX_ENTRIES {
            return Err(refused(&format!("it holds more than {MAX_ENTRIES} files")));
        }
        let mut left = smeltr::lookup::store::MAX_FILE_BYTES;
        for i in 0..zip.len() {
            let mut entry = zip.by_index(i).map_err(|e| refused(&e))?;
            if entry.is_dir() {
                continue;
            }
            if entry.is_symlink() {
                return Err(refused(&format!(
                    "{} is a link",
                    String::from_utf8_lossy(entry.name_raw())
                )));
            }
            let Some(name) = entry.enclosed_name() else {
                let name = String::from_utf8_lossy(entry.name_raw()).into_owned();
                return Err(refused(&format!("{name} names a place outside the export")));
            };
            let to = self.dir().join(name);
            if let Some(dir) = to.parent() {
                std::fs::create_dir_all(dir).map_err(|e| Refusal::internal(e.to_string()))?;
            }
            let mut out = std::fs::File::create(&to).map_err(|e| Refusal::internal(e.to_string()))?;
            // Bounded by what is left, plus one byte to tell a file that fits from one that does not.
            let copied = std::io::copy(&mut (&mut entry).take(left + 1), &mut out).map_err(|e| refused(&e))?;
            if copied > left {
                return Err(refused(&format!(
                    "it unpacks to more than {} bytes",
                    smeltr::lookup::store::MAX_FILE_BYTES
                )));
            }
            left -= copied;
        }
        Ok(())
    }

    /// What the unpacked export holds, read with the loader that reads every bundle, and the samples and tests beside
    /// it.
    fn objects(&self) -> Result<Objects, Refusal> {
        let refused = |e: &dyn std::fmt::Display| Refusal::invalid(format!("this export can't be read: {e}"));
        // Loaded strictly, so a version or a field this build does not read is refused with the loader's reason; the
        // objects are then read as every other read of a bundle reads them.
        config_file::load_bundle_dir(&self.dir()).map_err(|e| refused(&e))?;
        let dir = self.dir();
        let mut files = config_file::DirFileSet { root: &dir };
        let mut objects = Objects {
            file: config_file::read_raw_from(&mut files).file,
            samples: Vec::new(),
            tests: Vec::new(),
        };
        for path in files
            .json_paths_under(config_file::SAMPLES_DIR)
            .map_err(|e| refused(&e))?
        {
            let bytes = files.read(&path).map_err(|e| refused(&e))?.unwrap_or_default();
            objects
                .samples
                .push(serde_json::from_slice(&bytes).map_err(|e| refused(&format!("{path}: {e}")))?);
        }
        for path in files
            .json_paths_under(config_file::TESTS_DIR)
            .map_err(|e| refused(&e))?
        {
            let bytes = files.read(&path).map_err(|e| refused(&e))?.unwrap_or_default();
            objects
                .tests
                .push(serde_json::from_slice(&bytes).map_err(|e| refused(&format!("{path}: {e}")))?);
        }
        Ok(objects)
    }
}

impl Drop for Unpacking {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use smeltr::leader::git;

    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-import-api-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    const SITES: &[u8] = b"ip,site\n10.0.0.1,lab\n";

    /// A leader whose group `west` holds `common` as the export does, and a `sites` table of its own.
    fn leader(label: &str) -> PathBuf {
        let data = temp_dir(label);
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let west: FileConfig = serde_json::from_value(json!({"version": 1,
            "pipelines": [{"id": "common", "name": "common", "steps": []}],
            "lookupTables": [{"id": "sites", "type": "inline", "headers": ["ip"], "rows": [["x"]]}]}))
        .unwrap();
        let edits: Vec<git::FileEdit> = (config_file::bundle_files(&west).unwrap().into_iter())
            .map(|(path, content)| git::FileEdit {
                path: format!("groups/west/{path}"),
                content: Some(content),
            })
            .collect();
        let parent = git::main_head(&repo).unwrap();
        git::commit_edits(&repo, "work", &parent, None, &edits, "seed", &Actor::system()).unwrap();
        data
    }

    /// An export of `audit`, which calls `common` and reads `sites`, as a zip holding `extra` too.
    fn export_zip(extra: &[(&str, &[u8])]) -> Vec<u8> {
        let file: FileConfig = serde_json::from_value(json!({"version": 1,
            "pipelines": [
                {"id": "audit", "name": "audit", "steps": [
                    {"type": "call", "id": "c", "pipeline": "common"},
                    {"type": "lookup", "id": "site", "input": "_raw", "table": "sites", "matchKind": "exact",
                     "keyColumn": "ip", "outputs": [{"source": "site", "field": "site"}]}]},
                {"id": "common", "name": "common", "steps": []}],
            "lookupTables": [{"id": "sites", "type": "csv", "path": "lookups/848eac143ba4496d-sites.csv",
                              "delimiter": ",", "headers": ["ip", "site"]}]}))
        .unwrap();
        let sample: SampleDto = serde_json::from_value(json!({"id": "s1", "name": "one", "records": []})).unwrap();
        let mut files = config_file::bundle_files(&file).unwrap();
        files.push(("lookups/848eac143ba4496d-sites.csv".into(), SITES.to_vec()));
        files.push((
            config_file::sample_path("s1").unwrap(),
            config_file::sample_bytes(&sample),
        ));
        files.extend(extra.iter().map(|(p, b)| (p.to_string(), b.to_vec())));
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        for (path, bytes) in files {
            zip.start_file(path, zip::write::SimpleFileOptions::default()).unwrap();
            zip.write_all(&bytes).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    fn import_into(data: &Path, zip: &[u8], answers: Option<Answers>) -> Envelope<Import> {
        let writing = Writing::open_in(data, "work", None, "west").unwrap();
        let unpacking = Unpacking::new(writing.repo()).unwrap();
        std::fs::write(unpacking.zip(), zip).unwrap();
        imported(writing, &unpacking, answers, &[], &Actor::system())
    }

    fn standings(answer: &Envelope<Import>) -> Vec<(Part, String, config_file::Standing)> {
        (answer.body.listed.iter())
            .map(|l| (l.part, l.id.clone(), l.standing))
            .collect()
    }

    #[test]
    fn without_answers_an_import_lists_what_it_holds_and_writes_nothing() {
        let data = leader("listing");
        let head = git::branch_tip(&git::repo_path(&data), "work").unwrap();
        let answer = import_into(&data, &export_zip(&[]), None);
        assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.errors);
        use config_file::Standing::*;
        assert_eq!(
            standings(&answer),
            [
                (Part::Pipeline, "audit".into(), New),
                (Part::Pipeline, "common".into(), Same),
                (Part::LookupTable, "sites".into(), Taken),
                (Part::Sample, "s1".into(), New),
            ]
        );
        assert!(answer.body.commit.is_none());
        assert_eq!(git::branch_tip(&git::repo_path(&data), "work").unwrap(), head);
    }

    #[test]
    fn answered_it_saves_once_with_the_lookup_bytes_in_the_store() {
        let data = leader("saved");
        let answers: Answers = serde_json::from_value(json!({
            "taken": [{"part": "lookupTable", "id": "sites", "keep": "renamed", "as": "sites-2"}]}))
        .unwrap();
        let answer = import_into(&data, &export_zip(&[]), Some(answers));
        assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.errors);
        assert!(answer.body.commit.is_some());
        let reading = crate::config_api::Reading::open_in(&data, "work", "west").unwrap();
        let file = reading.raw().file;
        let ids = |v: Vec<&String>| v.into_iter().cloned().collect::<Vec<_>>();
        // Pipelines are read back in their files' order.
        assert_eq!(ids(file.pipelines.iter().map(|p| &p.id).collect()), ["audit", "common"]);
        assert_eq!(
            ids(file.lookup_tables.iter().map(|t| &t.id).collect()),
            ["sites", "sites-2"]
        );
        let LookupSourceDto::Csv { path, .. } = &file.lookup_tables[1].source else {
            panic!("a csv table")
        };
        assert!(path.ends_with("-sites.csv"), "{path}");
        let bytes = smeltr::plan::config::LookupFile {
            text: path.clone(),
            at: reading.at().lookup(path).unwrap(),
        };
        assert_eq!(&*bytes.map().unwrap(), SITES);
        assert!(read_sample(&mut reading.files(), "s1").unwrap().is_some());
        // The renamed table is the one the imported pipeline reads.
        assert!(
            serde_json::to_string(&file.pipelines[0].steps)
                .unwrap()
                .contains("\"table\":\"sites-2\"")
        );
    }

    #[test]
    fn a_taken_id_without_an_answer_is_asked_again() {
        let data = leader("unanswered");
        let answer = import_into(&data, &export_zip(&[]), Some(Answers::default()));
        assert_eq!(answer.status, StatusCode::CONFLICT);
        assert!(answer.body.commit.is_none() && !answer.body.listed.is_empty());
        assert_eq!(
            answer.errors[0].message,
            r#"lookup table "sites" needs an answer: its id is taken here"#
        );
    }

    #[test]
    fn an_entry_named_outside_the_export_is_refused() {
        let data = leader("slip");
        let answer = import_into(&data, &export_zip(&[("../escaped.json", b"{}")]), None);
        assert_eq!(answer.status, StatusCode::UNPROCESSABLE_ENTITY, "{:?}", answer.errors);
        assert!(answer.errors[0].message.contains("names a place outside the export"));
        assert!(!data.join("escaped.json").exists());
    }
}
