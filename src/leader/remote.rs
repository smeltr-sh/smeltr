//! Following a git host's `main`.
//!
//! With a remote set, the host owns `main`. The leader fetches the host's `main`, on a timer and on Sync now, fetches
//! from the host's LFS server any lookup file it names that the store lacks, and follows it (`git::follow`). The fleet
//! moves with it unless that would break a group that runs now; then the fleet stays where it was, so the fix can be
//! approved on top of the host's `main`, and the leader says why ([`Failure`]) until a sync succeeds. Workers never
//! talk to the host: they fetch from the leader as before.
//!
//! Every other branch goes the other way: a save is a local commit at once, and the leader pushes it to the host's
//! branch of the same name, its lookup files to the host's LFS server first, by a lease on what it last pushed
//! ([`push_saves`]). Review and merge are the host's; the leader follows the `main` that comes of them.
//!
//! The remote's credential is a leader setting sealed under the secrets key, and never in a bundle.

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use axum::Json;
use axum::extract::State;

use crate::format::bytes_text::BytesForm;
use crate::lookup::store::{self, Pointer};

use super::envelope::Envelope;
use super::store::{Actor, Store, StoreError};
use super::{AppState, git, run_store, tree};

pub const URL_KEY: &str = "git.remote.url";
pub const USER_KEY: &str = "git.remote.user";
pub const TOKEN_KEY: &str = "git.remote.token";
pub const POLL_KEY: &str = "git.remote.poll_seconds";
pub const APPROVE_KEY: &str = "git.remote.approve_pushes";

/// Where the leader keeps the roots its fetch from the remote trusts, as a PEM file: gix's curl takes them only by
/// path. Written at every start (`io::trust::write_pem_file`), from the public roots, as the LFS client trusts.
pub fn roots_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("public-roots.pem")
}

/// How often the remote is fetched where nobody has said.
pub const DEFAULT_POLL: Duration = Duration::from_secs(60);

/// Its bounds, off aside. Faster than every ten seconds is load on the host for a deploy nobody is waiting on that
/// closely; slower than a day is a timer nobody would notice running.
pub const MIN_POLL: Duration = Duration::from_secs(10);
pub const MAX_POLL: Duration = Duration::from_secs(24 * 60 * 60);

/// The remote this leader follows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Remote {
    pub url: String,
    pub user: String,
    pub token: Option<String>,
    /// `None` is off: the remote is fetched on Sync now only.
    pub poll: Option<Duration>,
    /// Approve squashes a change here and pushes it to the host's `main`. Off, changes are merged on the host.
    pub approve_pushes: bool,
}

impl Remote {
    /// The `Authorization` header every request to the host carries, when there is a token. Basic, since that is
    /// what every git host takes a token as.
    fn authorization(&self) -> Option<String> {
        let token = self.token.as_deref()?;
        Some(format!(
            "Basic {}",
            BytesForm::Base64.text(format!("{}:{token}", self.user).as_bytes())
        ))
    }
}

/// The remote this leader follows, or `None` when there is none.
pub fn configured(store: &dyn Store) -> Result<Option<Remote>, StoreError> {
    let Some(url) = store.setting(URL_KEY)?.filter(|url| !url.is_empty()) else {
        return Ok(None);
    };
    let poll = match store.setting(POLL_KEY)?.map(|s| s.parse::<u64>()) {
        None => Some(DEFAULT_POLL),
        Some(Ok(0)) => None,
        Some(Ok(seconds)) => Some(Duration::from_secs(seconds)),
        Some(Err(e)) => return Err(StoreError(format!("{POLL_KEY}: {e}"))),
    };
    Ok(Some(Remote {
        url,
        user: store.setting(USER_KEY)?.unwrap_or_default(),
        token: store.opened_setting(TOKEN_KEY)?.filter(|token| !token.is_empty()),
        poll,
        approve_pushes: store.setting(APPROVE_KEY)?.as_deref() == Some("true"),
    }))
}

/// Refuse a URL the leader may not fetch from: anything but a whole `https://` or `http://` URL, and `http://` off
/// this host unless `plain`. An admin who names a plain `http://` remote has chosen plain text for it, so what that
/// remote hands back may be plain too; an `https://` remote's answers may not take its token or lookup files onto
/// plain text.
///
/// A backslash is refused, since parsers disagree about whether it ends the host, and so about which host a URL names.
pub fn check_url(url: &str, plain: bool) -> Result<(), String> {
    if url.contains('\\') {
        return Err(format!("{url:?} holds a backslash; give the URL without one"));
    }
    if crate::config_file::web_url(url).is_none() {
        return Err(format!(
            "{url:?} is not a whole http:// or https:// URL. SSH is not supported"
        ));
    }
    if !plain && crate::config_file::plain_off_this_host(url) {
        return Err(format!(
            "{url:?} is plain HTTP, which only this host may be reached over. Use https://"
        ));
    }
    Ok(())
}

/// Whether `url` is plain `http://`.
fn is_plain(url: &str) -> bool {
    url.trim().to_ascii_lowercase().starts_with("http://")
}

// -- a sync -------------------------------------------------------------------

/// Why the last sync did not leave the fleet on the remote's head.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Failure {
    /// The remote's head it was about, when the fetch got that far.
    pub commit: Option<String>,
    pub message: String,
    /// When syncs began failing, unix millis.
    pub since: i64,
}

/// What the last sync found, as the settings page shows it.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncState {
    /// When the remote last answered, unix millis.
    pub synced_at: Option<i64>,
    /// The remote's `main` then. `None` with `synced_at` set is a remote with no `main` yet.
    pub remote_head: Option<String>,
    pub failure: Option<Failure>,
}

/// One sync: fetch the remote's `main`, trusting the PEM file at `roots`, then follow it. Answers the remote's head,
/// `None` when it has no `main`.
///
/// `Err` carries the head when the fetch reached it: a fetch that failed says nothing about the remote's `main`.
pub fn sync(repo: &Path, roots: &Path, remote: &Remote) -> Result<Option<String>, (Option<String>, String)> {
    let head = fetch(repo, roots, remote).map_err(|e| (None, e))?;
    let Some(head) = head else {
        // An empty host is given the leader's `main`, and owns it from then on.
        let main = git::main_head(repo).ok_or((None, "this leader has no main to give the remote".to_string()))?;
        return match push_branch(repo, remote, git::MAIN_BRANCH, Some(&main), None) {
            Ok(git::Pushed::Done) => Ok(Some(main)),
            Ok(git::Pushed::Refused) => Err((
                None,
                "the remote's main appeared while it was being given ours".to_string(),
            )),
            Err(e) => Err((None, e)),
        };
    };
    let refused = |e: String| (Some(head.clone()), e);
    // On every sync, not only the one that moves `main`, and for what runs as well as for `main`: a file the start
    // found damaged and removed comes back here, and the fleet may be held on a commit `main` has left.
    fetch_lookup_files(repo, &head, remote).map_err(refused)?;
    if let Some(running) = git::deployed(repo).filter(|running| *running != head) {
        fetch_lookup_files(repo, &running, remote).map_err(refused)?;
    }
    if git::main_head(repo).as_deref() != Some(head.as_str()) {
        git::follow(repo, &head).map_err(|e| refused(e.0))?;
    }
    // Asked on every sync, not only the one that moved `main`: the fleet stays held until the remote mends it.
    if let Some(why) = git::worse_than_running(repo, &head).map_err(|e| refused(e.0))?
        && git::deployed(repo).as_deref() != Some(head.as_str())
    {
        return Err(refused(format!(
            "{why}, so the fleet stays where it runs. Fix it on a branch and approve that, or release it from History"
        )));
    }
    Ok(Some(head))
}

/// Fetch the remote's `main` into [`git::FETCHED_MAIN_REF`], and answer the commit it names, `None` when it has none.
fn fetch(repo: &Path, roots: &Path, remote: &Remote) -> Result<Option<String>, String> {
    let unreachable = |e: &dyn std::fmt::Display| format!("fetching {} did not work: {e}", remote.url);
    let git = gix::open(repo).map_err(|e| unreachable(&e))?;
    let refspec = format!("+refs/heads/{}:{}", git::MAIN_BRANCH, git::FETCHED_MAIN_REF);
    let host = git
        .remote_at(remote.url.as_str())
        .map_err(|e| unreachable(&e))?
        .with_refspecs([refspec.as_str()], gix::remote::Direction::Fetch)
        .map_err(|e| unreachable(&e))?;
    let mut connection = host
        .connect(gix::remote::Direction::Fetch)
        .map_err(|e| unreachable(&e))?;
    connection.set_transport_options(Box::new(gix::protocol::transport::client::blocking_io::http::Options {
        extra_headers: remote
            .authorization()
            .map(|auth| format!("Authorization: {auth}"))
            .into_iter()
            .collect(),
        ssl_ca_info: Some(roots.to_path_buf()),
        connect_timeout: Some(FETCH_BUDGET),
        low_speed_limit_bytes_per_second: MIN_BYTES_PER_SECOND,
        low_speed_time_seconds: FETCH_BUDGET.as_secs(),
        ..Default::default()
    }));
    // The token above is the one credential, so a refusal is reported rather than answered from whatever credential
    // helper this host's gitconfig names -- the worker's fetch refuses the same cascade for the same reason.
    // Asked for one is the host answering 401, which gix reports as a credential helper that returned nothing.
    let asked = std::cell::Cell::new(false);
    #[allow(clippy::result_large_err)]
    connection.set_credentials(|_action| {
        asked.set(true);
        Ok(None)
    });
    let prepare = connection
        .prepare_fetch(gix::progress::Discard, Default::default())
        .map_err(|e| {
            let check = match (asked.get(), remote.token.is_some()) {
                (false, _) => return unreachable(&with_cause(&e)),
                (true, false) => "Set a token that can read this repository",
                (true, true) => "Check that the token can read this repository",
            };
            unreachable(&format!("401 Unauthorized. {check}, and check the URL"))
        })?;
    // A repository nobody has pushed `main` to yet: a state, not a failure.
    if prepare.ref_map().is_missing_required_mapping() {
        return Ok(None);
    }
    let outcome = prepare
        .receive(gix::progress::Discard, &AtomicBool::new(false))
        .map_err(|e| unreachable(&with_cause(&e)))?;
    Ok(outcome
        .ref_map
        .mappings
        .iter()
        .find_map(|m| m.remote.peeled_id())
        .map(|id| id.to_string()))
}

/// What a failed gix step says, and the cause gix picks for it. gix's own text is its step alone ("Transport handshake
/// failed"), while what an admin can act on, a refused certificate say, is the cause.
fn with_cause(e: &gix::Error) -> String {
    let (step, cause) = (e.error().to_string(), e.probable_cause().to_string());
    if step == cause {
        step
    } else {
        format!("{step}: {cause}")
    }
}

/// How long a fetch, or a request to the LFS server, may take to connect or answer, or run slower than
/// [`MIN_BYTES_PER_SECOND`]. A sync holds every other sync and every settings save behind it, so a host that
/// stops sending must not hold them for good.
const FETCH_BUDGET: Duration = Duration::from_secs(60);
const MIN_BYTES_PER_SECOND: u32 = 1024;

/// How long one lookup file's download may take: the largest file there is, at a slow line's pace.
const DOWNLOAD_BUDGET: Duration = Duration::from_secs(60 * 60);

/// How many files one batch request asks for. GitHub's LFS server takes no more.
const BATCH_SIZE: usize = 100;

/// Download from the host's LFS server every lookup file `commit` names that the store lacks, each checked against its
/// pointer. A file committed as its bytes rather than a pointer is refused (`store::pointer_in`).
fn fetch_lookup_files(repo: &Path, commit: &str, remote: &Remote) -> Result<(), String> {
    let pointers = lookup_pointers(repo, commit)?;
    let missing = store::missing(repo, pointers.keys());
    if missing.is_empty() {
        return Ok(());
    }
    if let Some(big) = missing.iter().find(|p| p.size > store::MAX_FILE_BYTES) {
        return Err(format!(
            "lookup file sha256 {} is {} bytes, past the {} a lookup file may be",
            big.oid,
            big.size,
            store::MAX_FILE_BYTES
        ));
    }
    let endpoint = lfs_endpoint(repo, commit, remote)?;
    let mut hrefs = std::collections::HashMap::new();
    for batch in missing.chunks(BATCH_SIZE) {
        let main = format!("refs/heads/{}", git::MAIN_BRANCH);
        for (oid, actions) in lfs_batch(remote, &endpoint, "download", &main, batch)? {
            hrefs.extend(actions.download.map(|download| (oid, download)));
        }
    }
    // Named as uploads are, before anything arrives, so retention counts a file this leaves in the store. A name whose
    // file never arrives is cleared by the next prune.
    for pointer in &missing {
        super::lookups::record_upload(repo, &pointer.oid, &pointers[*pointer])
            .map_err(|e| format!("could not record lookup file sha256 {}: {e}", pointer.oid))?;
    }
    store::fetch_missing(repo, missing, |pointer| {
        let action = hrefs
            .remove(&pointer.oid)
            .ok_or_else(|| "the remote's LFS server did not offer it".to_string())?;
        check_url(&action.href, is_plain(&remote.url))?;
        let mut request = agent(remote, &action.href, DOWNLOAD_BUDGET, true).get(&action.href);
        for (name, value) in &action.header {
            request = request.header(name, value);
        }
        let response = request.call().map_err(|e| e.to_string())?;
        Ok(response.into_body().into_reader())
    })
}

/// Every lookup file `commit` names, in `shared/` and in every group, with the path it is committed at.
fn lookup_pointers(repo: &Path, commit: &str) -> Result<std::collections::HashMap<Pointer, String>, String> {
    let mut prefixes = vec![crate::config_file::SHARED_DIR.to_string()];
    for group in tree::groups_at(repo, commit).map_err(|e| e.0)? {
        prefixes.extend(crate::config_file::group_subtree(&group));
    }
    tree::lookup_pointers(repo, commit, &prefixes)
}

/// Upload to the host's LFS server every lookup file `commit` names that it lacks, from the leader's store, so a clone
/// of the host with git-lfs gets every file.
fn upload_lookup_files(repo: &Path, commit: &str, branch: &str, remote: &Remote) -> Result<(), String> {
    let pointers: Vec<Pointer> = lookup_pointers(repo, commit)?.into_keys().collect();
    if pointers.is_empty() {
        return Ok(());
    }
    let endpoint = lfs_endpoint(repo, commit, remote)?;
    let pointers: Vec<&Pointer> = pointers.iter().collect();
    for batch in pointers.chunks(BATCH_SIZE) {
        for (oid, actions) in lfs_batch(remote, &endpoint, "upload", &format!("refs/heads/{branch}"), batch)? {
            // No upload asked for is the server holding the file already.
            let Some(upload) = actions.upload else {
                continue;
            };
            let failed = |e: &dyn std::fmt::Display| format!("uploading lookup file sha256 {oid} did not work: {e}");
            let size = batch
                .iter()
                .find(|p| p.oid == oid)
                .ok_or_else(|| failed(&"the remote's LFS server asked for a file it was not offered"))?
                .size;
            let file = std::fs::File::open(store::path_of(repo, &oid))
                .map_err(|e| failed(&format!("the leader's store does not hold it: {e}")))?;
            check_url(&upload.href, is_plain(&remote.url)).map_err(|e| failed(&e))?;
            let mut request = agent(remote, &upload.href, DOWNLOAD_BUDGET, true)
                .put(&upload.href)
                .header("Content-Type", "application/octet-stream");
            for (name, value) in &upload.header {
                request = request.header(name, value);
            }
            request.send(file).map_err(|e| failed(&e))?;
            if let Some(verify) = actions.verify {
                check_url(&verify.href, is_plain(&remote.url)).map_err(|e| failed(&e))?;
                let mut request = agent(remote, &verify.href, FETCH_BUDGET, true)
                    .post(&verify.href)
                    .header("Accept", LFS_JSON)
                    .header("Content-Type", LFS_JSON);
                for (name, value) in &verify.header {
                    request = request.header(name, value);
                }
                request
                    .send_json(serde_json::json!({"oid": oid, "size": size}))
                    .map_err(|e| failed(&e))?;
            }
        }
    }
    Ok(())
}

const LFS_JSON: &str = "application/vnd.git-lfs+json";

/// What the LFS server says to do about one file, each step absent when there is nothing to do.
#[derive(serde::Deserialize, Default)]
struct Actions {
    download: Option<Action>,
    upload: Option<Action>,
    verify: Option<Action>,
}

/// One request the LFS server asks for: where, and the headers that authorize it. The leader's own token is never
/// sent there -- the server says what to send.
#[derive(serde::Deserialize)]
struct Action {
    href: String,
    #[serde(default)]
    header: std::collections::HashMap<String, String>,
}

/// Where `commit` says its lookup files are served: `lfs.url` in its `.lfsconfig`, as git-lfs reads it, or else the
/// remote's URL as a `.git` repository, then `info/lfs`.
fn lfs_endpoint(repo: &Path, commit: &str, remote: &Remote) -> Result<String, String> {
    let config = git::read_blobs(repo, commit, &[".lfsconfig".to_string()])
        .map_err(|e| e.0)?
        .pop()
        .flatten();
    if let Some(config) = config {
        let file =
            gix::config::File::from_bytes_no_includes(&config, gix::config::file::Metadata::api(), Default::default())
                .map_err(|e| format!(".lfsconfig at {commit}: {e}"))?;
        if let Some(url) = file.string("lfs.url") {
            let url = url.to_string();
            check_url(&url, is_plain(&remote.url)).map_err(|e| format!(".lfsconfig at {commit}, lfs.url: {e}"))?;
            return Ok(url.trim_end_matches('/').to_string());
        }
    }
    let base = remote.url.trim_end_matches('/');
    Ok(match base.ends_with(".git") {
        true => format!("{base}/info/lfs"),
        false => format!("{base}.git/info/lfs"),
    })
}

/// How many redirects a batch request follows.
const MAX_REDIRECTS: usize = 5;

/// Ask the LFS server at `endpoint` what to do to `operation` -- `download` or `upload` -- `pointers`, for the branch
/// `reference` names: the batch API every git-lfs server speaks.
///
/// A 307 or 308 is followed here rather than by the agent, which does not send a request's body again: each hop is
/// checked as the remote's URL is, never from `https://` to `http://`, and the token goes only to the remote's origin.
fn lfs_batch(
    remote: &Remote,
    endpoint: &str,
    operation: &str,
    reference: &str,
    pointers: &[&Pointer],
) -> Result<std::collections::HashMap<String, Actions>, String> {
    #[derive(serde::Deserialize)]
    struct Batch {
        objects: Vec<Object>,
    }
    #[derive(serde::Deserialize)]
    struct Object {
        oid: String,
        actions: Option<Actions>,
        error: Option<ObjectError>,
    }
    #[derive(serde::Deserialize)]
    struct ObjectError {
        message: String,
    }

    let batch_url = format!("{endpoint}/objects/batch");
    let failed = |e: &dyn std::fmt::Display| format!("asking {batch_url} for lookup files did not work: {e}");
    let body = serde_json::json!({
        "operation": operation,
        "transfers": ["basic"],
        // A server that grants write per branch decides by it.
        "ref": {"name": reference},
        "objects": pointers.iter().map(|p| serde_json::json!({"oid": p.oid, "size": p.size})).collect::<Vec<_>>(),
        "hash_algo": "sha256",
    });
    let mut url = batch_url.clone();
    let mut response = None;
    for _ in 0..=MAX_REDIRECTS {
        let mut request = agent(remote, &url, FETCH_BUDGET, false)
            .post(&url)
            .header("Accept", LFS_JSON)
            .header("Content-Type", LFS_JSON);
        if let Some(auth) = remote.authorization().filter(|_| origin(&url) == origin(&remote.url)) {
            request = request.header("Authorization", auth);
        }
        let answer = request.send_json(&body).map_err(|e| failed(&e))?;
        if !matches!(answer.status().as_u16(), 307 | 308) {
            response = Some(answer);
            break;
        }
        let location = answer
            .headers()
            .get("location")
            .and_then(|l| l.to_str().ok())
            .ok_or_else(|| failed(&"a redirect without a Location"))?;
        url = redirected(&url, location).map_err(|e| failed(&e))?;
    }
    let batch: Batch = response
        .ok_or_else(|| failed(&format!("more than {MAX_REDIRECTS} redirects")))?
        .body_mut()
        .read_json()
        .map_err(|e| failed(&e))?;
    let mut offered = std::collections::HashMap::new();
    for object in batch.objects {
        if let Some(error) = object.error {
            return Err(format!(
                "lookup file sha256 {}: the remote's LFS server says {}",
                object.oid, error.message
            ));
        }
        offered.insert(object.oid, object.actions.unwrap_or_default());
    }
    Ok(offered)
}

/// Where `location`, answered to a request for `from`, sends it: absolute, or a path on `from`'s origin. Checked as
/// the remote's URL is, and never from `https://` to `http://`.
fn redirected(from: &str, location: &str) -> Result<String, String> {
    let to = match location.starts_with('/') && !location.starts_with("//") {
        true => {
            let (scheme, authority) = from.split_once("://").ok_or("not a URL")?;
            let authority = authority.split('/').next().unwrap_or_default();
            format!("{scheme}://{authority}{location}")
        }
        false => location.to_string(),
    };
    check_url(&to, is_plain(from))?;
    if from.to_ascii_lowercase().starts_with("https://") && !to.to_ascii_lowercase().starts_with("https://") {
        return Err(format!("redirected from https to {to}"));
    }
    Ok(to)
}

/// The scheme, host and port `url` reaches, the port filled in when the scheme implies it.
fn origin(url: &str) -> Option<(String, String, u16)> {
    let uri: ureq::http::Uri = url.parse().ok()?;
    let scheme = uri.scheme_str()?.to_ascii_lowercase();
    let port = uri.port_u16().or(match scheme.as_str() {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    })?;
    Some((scheme, uri.host()?.to_ascii_lowercase(), port))
}

/// An agent for `url`, on behalf of `remote`. With `follow`, one that is `https://` is redirected, to `https://` only;
/// one on plain HTTP is redirected only for a remote the admin gave as plain HTTP, since otherwise where it would go
/// is not this host's to vouch for. Neither reaches this leader's own network unless `remote` is on it
/// ([`Reachable`]).
fn agent(remote: &Remote, url: &str, budget: Duration, follow: bool) -> ureq::Agent {
    let https = url.to_ascii_lowercase().starts_with("https://");
    let tls = crate::io::trust::public_tls();
    let config = ureq::Agent::config_builder()
        .tls_config(tls)
        .https_only(https)
        .max_redirects(if follow && (https || is_plain(&remote.url)) {
            5
        } else {
            0
        })
        .timeout_connect(Some(FETCH_BUDGET))
        .timeout_recv_response(Some(FETCH_BUDGET))
        .timeout_global(Some(budget))
        .build();
    let reachable = Reachable {
        internal: on_internal_network(&remote.url),
    };
    ureq::Agent::with_parts(config, ureq::unversioned::transport::DefaultConnector::new(), reachable)
}

/// The platform's resolver, refusing an internal address -- loopback, a private network, link-local -- to a remote
/// that is not itself on one. The LFS server says where a file is downloaded from, so a compromised host could
/// otherwise have the leader send requests to services only its own network can reach. Checked on the address
/// actually connected to, redirects included. Through a proxy, the proxy is what is resolved here, and what it
/// reaches is its own policy.
#[derive(Debug)]
struct Reachable {
    internal: bool,
}

impl ureq::unversioned::resolver::Resolver for Reachable {
    fn resolve(
        &self,
        uri: &ureq::http::Uri,
        config: &ureq::config::Config,
        timeout: ureq::unversioned::transport::NextTimeout,
    ) -> Result<ureq::unversioned::resolver::ResolvedSocketAddrs, ureq::Error> {
        let addrs = ureq::unversioned::resolver::DefaultResolver::default().resolve(uri, config, timeout)?;
        let proxied = config.proxy().is_some_and(|proxy| {
            uri.host() == Some(proxy.host()) && uri.port_u16().is_none_or(|port| port == proxy.port())
        });
        if self.internal || proxied || !addrs.iter().any(|a| internal(a.ip())) {
            return Ok(addrs);
        }
        Err(ureq::Error::Io(std::io::Error::other(format!(
            "{uri} is on this leader's own network, which the remote is not: nothing is downloaded from there"
        ))))
    }
}

/// Whether `url`'s host resolves to an internal address. A host that does not resolve is not.
fn on_internal_network(url: &str) -> bool {
    use std::net::ToSocketAddrs;
    let Ok(uri) = url.parse::<ureq::http::Uri>() else {
        return false;
    };
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("http") { 80 } else { 443 });
    let resolved = uri
        .host()
        .and_then(|host| (host.trim_matches(['[', ']']), port).to_socket_addrs().ok());
    resolved.is_some_and(|mut addrs| addrs.any(|a| internal(a.ip())))
}

/// Loopback, a private or carrier-grade NAT network, link-local, or unspecified.
fn internal(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || (v4.octets()[0] == 100 && v4.octets()[1] & 0xc0 == 64)
        }
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => internal(v4.into()),
            None => v6.is_loopback() || v6.is_unspecified() || v6.is_unique_local() || v6.is_unicast_link_local(),
        },
    }
}

// -- syncing on a timer and on demand -----------------------------------------

/// The leader's syncs: one at a time, and what the last one found.
#[derive(Default)]
pub struct Syncing {
    /// Held by whatever moves `main`, the fleet or the remote it follows: a sync, a settings save, an Approve, a
    /// rollback.
    pub turn: tokio::sync::Mutex<()>,
    state: std::sync::Mutex<SyncState>,
    /// Wakes the timer when the settings change, so a new interval applies at once.
    pub settings_changed: tokio::sync::Notify,
    /// Held by whatever pushes a branch, and by whatever changes the remote, so no push lands on a remote it was not
    /// meant for.
    pub pushing: tokio::sync::Mutex<()>,
    /// Why each branch's latest save is not on the host, by branch.
    pushes: std::sync::Mutex<std::collections::HashMap<String, PushFailure>>,
}

impl Syncing {
    pub fn state(&self) -> SyncState {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The remote took `head` on its `main` and the leader runs it, as a successful sync would leave things.
    pub fn heard(&self, head: &str) {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = SyncState {
            synced_at: Some(super::warnings::now_millis()),
            remote_head: Some(head.to_string()),
            failure: None,
        };
    }

    pub fn push_failures(&self) -> std::collections::HashMap<String, PushFailure> {
        self.pushes.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Sync now, after any sync already running. Answers what it found; with no remote set, nothing is done.
pub async fn sync_now(state: &AppState) -> Result<SyncState, String> {
    let _turn = state.remote.turn.lock().await;
    let Some(remote) = run_store(state.store.clone(), configured)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Err("no remote is set".to_string());
    };
    let repo = git::repo_path(&state.data_dir);
    let roots = roots_path(&state.data_dir);
    let synced = tokio::task::spawn_blocking(move || sync(&repo, &roots, &remote))
        .await
        .unwrap_or_else(|e| Err((None, format!("internal error: {e}"))));
    if synced.is_ok() {
        // Retention runs where files arrive, as it does after an upload.
        super::prune_lookups(state).await;
    }
    let now = super::warnings::now_millis();
    let mut held = state.remote.state.lock().unwrap_or_else(|e| e.into_inner());
    match synced {
        Ok(head) => {
            *held = SyncState {
                synced_at: Some(now),
                remote_head: head,
                failure: None,
            }
        }
        Err((commit, message)) => {
            // A sync that reached the remote's head has heard from the remote, even if it could not follow it.
            if commit.is_some() {
                held.synced_at = Some(now);
                held.remote_head = commit.clone();
            }
            let since = held.failure.as_ref().map_or(now, |f| f.since);
            held.failure = Some(Failure { commit, message, since });
        }
    }
    Ok(held.clone())
}

/// Sync on the configured interval, for as long as the leader runs. With the timer off or no remote set, it waits
/// for the settings to change.
pub async fn poll(state: AppState) {
    loop {
        let every = run_store(state.store.clone(), configured)
            .await
            .ok()
            .flatten()
            .and_then(|remote| remote.poll);
        let Some(every) = every else {
            state.remote.settings_changed.notified().await;
            continue;
        };
        let _ = sync_now(&state).await;
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = state.remote.settings_changed.notified() => {}
        }
    }
}

// -- pushing branches ---------------------------------------------------------

/// Why a branch's latest save is not on the host.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PushFailure {
    /// The save that did not go out; `None` is the branch's deletion. A newer save is pushed afresh.
    pub tip: Option<String>,
    pub message: String,
    /// The host's branch is not what this leader last pushed, so the lease refused the push. It stays refused until
    /// the host's branch is overwritten ([`overwrite`]) or the branch is saved again.
    pub refused: bool,
    /// Where the host's branch is, when the push was refused.
    pub host_tip: Option<String>,
    /// When the push failed, unix millis.
    pub at: i64,
}

/// How often the leader looks for a save to push. Looking reads the leader's own refs, so it costs the host nothing.
const PUSH_SCAN: Duration = Duration::from_secs(2);

/// Push every save to the host for as long as the leader runs. A push that fails is tried again at the poll
/// interval, so a host that was down gets the save once it is back.
pub async fn push_saves(state: AppState) {
    loop {
        tokio::time::sleep(PUSH_SCAN).await;
        let _pushing = state.remote.pushing.lock().await;
        let Ok(Some(remote)) = run_store(state.store.clone(), configured).await else {
            continue;
        };
        let retry = remote.poll.unwrap_or(DEFAULT_POLL).as_millis() as i64;
        let repo = git::repo_path(&state.data_dir);
        let failures = state.remote.push_failures();
        let now = super::warnings::now_millis();
        if let Ok(Ok(failures)) =
            tokio::task::spawn_blocking(move || push_all(&repo, &remote, &failures, now, retry)).await
        {
            *state.remote.pushes.lock().unwrap_or_else(|e| e.into_inner()) = failures;
        }
    }
}

/// Push every branch whose tip is not what the leader last pushed, and delete on the host each one deleted here.
/// A branch that failed at the same tip waits `retry` millis, or, refused, until it is overwritten or saved again.
/// Answers what is still not on the host.
pub fn push_all(
    repo: &Path,
    remote: &Remote,
    failures: &std::collections::HashMap<String, PushFailure>,
    now: i64,
    retry: i64,
) -> Result<std::collections::HashMap<String, PushFailure>, String> {
    let local = git::branch_tips(repo).map_err(|e| e.0)?;
    let pushed = git::host_branches(repo).map_err(|e| e.0)?;
    let mut branches: Vec<&String> = local.keys().chain(pushed.keys()).collect();
    branches.sort();
    branches.dedup();
    let mut still = std::collections::HashMap::new();
    // `main` is the host's, and comes back by fetching.
    for branch in branches.into_iter().filter(|b| *b != git::MAIN_BRANCH) {
        let (tip, last) = (local.get(branch), pushed.get(branch));
        if tip == last {
            continue;
        }
        if let Some(failed) = failures.get(branch).filter(|f| f.tip.as_ref() == tip)
            && (failed.refused || now - failed.at < retry)
        {
            still.insert(branch.clone(), failed.clone());
            continue;
        }
        let tip = tip.map(String::as_str);
        let failed = |message: String, refused: bool, host_tip: Option<String>| PushFailure {
            tip: tip.map(str::to_string),
            message,
            refused,
            host_tip,
            at: now,
        };
        match push_branch(repo, remote, branch, tip, last.map(String::as_str)) {
            Ok(git::Pushed::Done) => {}
            // A deletion the host's branch moved past leaves that branch to whoever moved it.
            Ok(git::Pushed::Refused) if tip.is_none() => {
                git::record_host_branch(repo, branch, None).map_err(|e| e.0)?;
            }
            // Refused only once the host has said where its branch is: that is what an overwrite leases on.
            Ok(git::Pushed::Refused) => {
                let failure = match git::host_branch(repo, &remote.url, remote.authorization().as_deref(), branch) {
                    Ok(host_tip) => failed(refused_message(branch), true, host_tip),
                    Err(e) => failed(e.0, false, None),
                };
                still.insert(branch.clone(), failure);
            }
            Err(message) => {
                still.insert(branch.clone(), failed(message, false, None));
            }
        }
    }
    Ok(still)
}

fn refused_message(branch: &str) -> String {
    format!("{branch} on the remote has commits this leader did not push")
}

/// Push `tip` to `branch` on the host, or delete it there for `None`, while the host's branch is at `lease`. Lookup
/// files go to the host's LFS server first, since a branch whose files are not there is no use to a clone. Records
/// where the host's branch now is.
fn push_branch(
    repo: &Path,
    remote: &Remote,
    branch: &str,
    tip: Option<&str>,
    lease: Option<&str>,
) -> Result<git::Pushed, String> {
    if let Some(tip) = tip {
        upload_lookup_files(repo, tip, branch, remote)?;
    }
    let pushed =
        git::push(repo, &remote.url, remote.authorization().as_deref(), branch, tip, lease).map_err(|e| e.0)?;
    if pushed == git::Pushed::Done {
        git::record_host_branch(repo, branch, tip).map_err(|e| e.0)?;
    }
    Ok(pushed)
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Overwrite {
    /// The branch's tip the admin was shown and chose to push. A save since is not pushed unseen.
    tip: String,
    /// The host's tip the admin was shown and chose to overwrite; `None` when the host had no such branch.
    host_tip: Option<String>,
}

/// Push a branch the host refused over what the host holds, by a lease on what the admin was shown of both: if either
/// has moved since, this is refused too, and what the admin is shown next is where the host's branch is now.
pub async fn overwrite(
    State(state): State<AppState>,
    axum::extract::Path(branch): axum::extract::Path<String>,
    Json(request): Json<Overwrite>,
) -> Json<Envelope> {
    if branch == git::MAIN_BRANCH {
        return Json(Envelope::failed_one("main is the remote's, and comes back by fetching"));
    }
    let _pushing = state.remote.pushing.lock().await;
    let remote = match run_store(state.store.clone(), configured).await {
        Ok(Some(remote)) => remote,
        Ok(None) => return Json(Envelope::failed_one("no remote is set")),
        Err(e) => return Json(Envelope::failed_one(e)),
    };
    let repo = git::repo_path(&state.data_dir);
    let name = branch.clone();
    let pushed = tokio::task::spawn_blocking(move || {
        if git::branch_tip(&repo, &name).as_deref() != Some(request.tip.as_str()) {
            return Err(format!(
                "{name} was saved again since it was shown: look at it again before overwriting"
            ));
        }
        match push_branch(&repo, &remote, &name, Some(&request.tip), request.host_tip.as_deref())? {
            git::Pushed::Done => Ok(None),
            git::Pushed::Refused => git::host_branch(&repo, &remote.url, remote.authorization().as_deref(), &name)
                .map(|host_tip| Some((request.tip, host_tip)))
                .map_err(|e| e.0),
        }
    })
    .await
    .unwrap_or_else(|e| Err(e.to_string()));
    match pushed {
        Ok(None) => {
            state
                .remote
                .pushes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&branch);
            Json(Envelope::done())
        }
        Ok(Some((tip, host_tip))) => {
            let failed = PushFailure {
                tip: Some(tip),
                message: refused_message(&branch),
                refused: true,
                host_tip,
                at: super::warnings::now_millis(),
            };
            state
                .remote
                .pushes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(branch.clone(), failed);
            Json(Envelope::failed_one(format!(
                "{branch} on the remote has moved again since it was shown: look at it again before overwriting it"
            )))
        }
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

// -- approving --------------------------------------------------------------

/// Approve `branch` onto the host's `main`: squash it as Approve does, push the squash by a lease on the `main` last
/// fetched, and only once the host has taken it, move `main` and the fleet. Answers the deployed commit. A host that
/// refuses, its branch protection or a `main` that moved, leaves everything where it was.
pub fn approve(data_dir: &Path, remote: &Remote, branch: &str, author: &Actor) -> Result<String, String> {
    let tip =
        git::branch_tip(&git::repo_path(data_dir), branch).ok_or_else(|| format!("there is no branch {branch:?}"))?;
    approve_at(data_dir, remote, branch, &tip, author)
}

/// [`approve`] of `branch` at `tip`, the tip the caller judged, refused once the branch has moved from it
/// (`git::squash_change_at`).
pub fn approve_at(data_dir: &Path, remote: &Remote, branch: &str, tip: &str, author: &Actor) -> Result<String, String> {
    if !remote.approve_pushes {
        return Err(format!(
            "this leader follows {}'s main: merge the change there, and the leader follows it",
            remote.url
        ));
    }
    let repo = git::repo_path(data_dir);
    let squash = git::squash_change_at(data_dir, branch, tip, author).map_err(|e| e.0)?;
    // Leased on the `main` the squash is onto: the host takes it only on top of what this leader runs, so nothing on
    // the host's `main` is ever dropped, whether the host moved or the leader did not follow it.
    match push_branch(
        &repo,
        remote,
        git::MAIN_BRANCH,
        Some(&squash.commit),
        Some(&squash.head),
    )? {
        git::Pushed::Done => {}
        git::Pushed::Refused => {
            return Err(
                "the remote's main is not the one this leader runs: sync, bring the change current and approve again"
                    .to_string(),
            );
        }
    }
    // The host has it, so the change is merged whatever happens here; a sync follows it.
    if let Err(e) = git::deploy_squash(data_dir, &squash, author) {
        let _ = git::delete_branch(data_dir, branch, Some(&squash.tip));
        return Err(format!(
            "{branch} is on the remote's main, but this leader did not deploy it ({}); the next sync follows it",
            e.0
        ));
    }
    Ok(squash.commit)
}

// -- settings -----------------------------------------------------------------

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteSettingsDto {
    url: String,
    user: String,
    /// Whether a token is stored. The token itself is never sent back.
    token_set: bool,
    /// 0 is off.
    poll_seconds: u64,
    approve_pushes: bool,
    min_poll_seconds: u64,
    max_poll_seconds: u64,
    sync: SyncState,
}

#[derive(Default, serde::Serialize)]
pub struct RemoteSettings {
    settings: Option<RemoteSettingsDto>,
}

impl RemoteSettings {
    fn answer(state: &AppState, remote: Result<Option<Remote>, String>) -> Json<Envelope<RemoteSettings>> {
        Json(match remote {
            Ok(remote) => Envelope::ok(RemoteSettings {
                settings: Some(RemoteSettingsDto {
                    url: remote.as_ref().map(|r| r.url.clone()).unwrap_or_default(),
                    user: remote.as_ref().map(|r| r.user.clone()).unwrap_or_default(),
                    token_set: remote.as_ref().is_some_and(|r| r.token.is_some()),
                    poll_seconds: remote
                        .as_ref()
                        .map_or(DEFAULT_POLL, |r| r.poll.unwrap_or_default())
                        .as_secs(),
                    approve_pushes: remote.as_ref().is_some_and(|r| r.approve_pushes),
                    min_poll_seconds: MIN_POLL.as_secs(),
                    max_poll_seconds: MAX_POLL.as_secs(),
                    sync: state.remote.state(),
                }),
            }),
            Err(error) => Envelope::failed_one(error),
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutRemoteSettings {
    /// Empty disconnects the remote.
    url: String,
    #[serde(default)]
    user: String,
    /// `None` keeps the stored token while the URL stays the same; empty removes it.
    token: Option<String>,
    poll_seconds: u64,
    #[serde(default)]
    approve_pushes: bool,
}

/// `GET /api/settings/git-remote`.
pub async fn get_settings(State(state): State<AppState>) -> Json<Envelope<RemoteSettings>> {
    let read = run_store(state.store.clone(), configured)
        .await
        .map_err(|e| e.to_string());
    RemoteSettings::answer(&state, read)
}

/// `PUT /api/settings/git-remote`. Recorded with who set it, in the transaction that sets it. The remote's
/// credential is sealed before it is stored.
pub async fn put_settings(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Json(request): Json<PutRemoteSettings>,
) -> Json<Envelope<RemoteSettings>> {
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let url = request.url.trim().to_string();
    if !url.is_empty()
        && let Err(e) = check_url(&url, true)
    {
        return RemoteSettings::answer(&state, Err(e));
    }
    let poll = Duration::from_secs(request.poll_seconds);
    if !poll.is_zero() && !(MIN_POLL..=MAX_POLL).contains(&poll) {
        return RemoteSettings::answer(
            &state,
            Err(format!(
                "the remote is fetched every {} to {} seconds, or 0 for Sync now only",
                MIN_POLL.as_secs(),
                MAX_POLL.as_secs()
            )),
        );
    }
    let event = super::accounts::event_for(&actor, "git-remote-change", &url, "ok");
    // After any sync or push in flight, so what it did is never recorded against a remote it was not about.
    let _turn = state.remote.turn.lock().await;
    let _pushing = state.remote.pushing.lock().await;
    let written = run_store(state.store.clone(), move |s| {
        let was = s.setting(URL_KEY)?.unwrap_or_default();
        let mut entries = vec![
            (URL_KEY, url.clone()),
            (USER_KEY, request.user.trim().to_string()),
            (POLL_KEY, poll.as_secs().to_string()),
            (APPROVE_KEY, request.approve_pushes.to_string()),
        ];
        // A token is for the remote it was saved with: a new URL, or none, forgets it unless a new one is given.
        let token = match request.token {
            None if url != was => Some(String::new()),
            token => token,
        };
        if let Some(token) = token {
            let stored = if token.is_empty() {
                token
            } else {
                s.sealed_setting(TOKEN_KEY, &token)?
            };
            entries.push((TOKEN_KEY, stored));
        }
        s.put_settings(&entries, Some(&event))?;
        Ok((url != was, configured(s)?))
    })
    .await
    .map_err(|e| e.to_string());
    if let Ok((moved, _)) = &written {
        if *moved {
            forget(&state).await;
        }
        state.remote.settings_changed.notify_one();
    }
    RemoteSettings::answer(&state, written.map(|(_, remote)| remote))
}

/// Forget what was found about the remote that was set -- its sync state, its push failures and its branches --
/// once it is replaced or gone. What was found about one remote says nothing about another. The caller holds the
/// remote's turn and its pushing lock.
pub async fn forget(state: &AppState) {
    *state.remote.state.lock().unwrap_or_else(|e| e.into_inner()) = SyncState::default();
    state.remote.pushes.lock().unwrap_or_else(|e| e.into_inner()).clear();
    let repo = git::repo_path(&state.data_dir);
    if let Ok(Err(e)) = tokio::task::spawn_blocking(move || git::forget_host(&repo)).await {
        tracing::warn!(target: "remote.forget", error = %e.0, "the old remote's branches could not be forgotten");
    }
    state.remote.settings_changed.notify_one();
}

/// `POST /api/settings/git-remote/sync`: fetch and follow now, without waiting for the timer.
pub async fn post_sync(State(state): State<AppState>) -> Json<Envelope<RemoteSettings>> {
    if let Err(e) = sync_now(&state).await {
        return RemoteSettings::answer(&state, Err(e));
    }
    get_settings(State(state)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    const TOKEN: &str = "s3cret";

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-remote-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A git host on loopback: `host.git` over smart HTTP behind a Basic token, and an LFS server holding `lfs`.
    struct Host {
        repo: PathBuf,
        url: String,
        /// The scheme and authority it answers at.
        base: String,
        /// How many batch requests `moved/objects/batch` has redirected.
        moved: Arc<AtomicUsize>,
        /// Whether a download is offered at `hop/objects/{oid}`, which redirects to the file.
        hop: Arc<std::sync::atomic::AtomicBool>,
        lfs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    }

    impl Host {
        fn start(name: &str) -> Host {
            Host::serve(name, true)
        }

        /// A host whose repository holds a `main` when `with_main`, and nothing when not. Its LFS server answers at
        /// `host.git/info/lfs` and at `elsewhere/info/lfs`; `moved/objects/batch` redirects to it.
        fn serve(name: &str, with_main: bool) -> Host {
            use axum::routing::{any, get, post};
            let repo = temp_dir(name).join("host.git");
            git::init_bare_repo(&repo).unwrap();
            if with_main {
                git::ensure_main_commit(&repo).unwrap();
            }
            let lfs: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::default();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());

            let authorized = |headers: &axum::http::HeaderMap| {
                headers.get("authorization").and_then(|v| v.to_str().ok())
                    == Some(format!("Basic {}", BytesForm::Base64.text(format!("smeltr:{TOKEN}").as_bytes())).as_str())
            };
            let (served, objects, files, uploads) = (repo.clone(), lfs.clone(), lfs.clone(), lfs.clone());
            let lfs_base = base.clone();
            let hop: Arc<std::sync::atomic::AtomicBool> = Arc::default();
            let hopping = hop.clone();
            let batch = post(
                move |headers: axum::http::HeaderMap, body: axum::body::Bytes| async move {
                    if !authorized(&headers) {
                        return (axum::http::StatusCode::UNAUTHORIZED, String::new());
                    }
                    let asked: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    assert!(
                        asked["ref"]["name"].as_str().unwrap().starts_with("refs/heads/"),
                        "{asked}"
                    );
                    let held = objects.lock().unwrap();
                    let answered: Vec<serde_json::Value> = asked["objects"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|o| {
                            let oid = o["oid"].as_str().unwrap();
                            let at = if hopping.load(Ordering::SeqCst) {
                                "hop/objects"
                            } else {
                                "objects"
                            };
                            let action = serde_json::json!({
                                "href": format!("{lfs_base}/{at}/{oid}"),
                                "header": {"X-Ticket": "t"},
                            });
                            match (asked["operation"].as_str().unwrap(), held.contains_key(oid)) {
                                ("download", true) => serde_json::json!({"oid": oid, "actions": {"download": action}}),
                                ("download", false) => {
                                    serde_json::json!({"oid": oid, "error": {"code": 404, "message": "not found"}})
                                }
                                (_, true) => serde_json::json!({"oid": oid}),
                                (_, false) => serde_json::json!({"oid": oid, "actions": {"upload": action}}),
                            }
                        })
                        .collect();
                    (
                        axum::http::StatusCode::OK,
                        serde_json::json!({"objects": answered}).to_string(),
                    )
                },
            );
            let moved: Arc<AtomicUsize> = Arc::default();
            let redirects = moved.clone();
            let app =
                axum::Router::new()
                    .route("/host.git/info/lfs/objects/batch", batch.clone())
                    .route("/elsewhere/info/lfs/objects/batch", batch)
                    .route(
                        "/moved/objects/batch",
                        // The body is read though unused: a server that answers before reading it closes the
                        // connection with the body unread, and Windows then resets it under the client's read.
                        post(move |_: axum::body::Bytes| async move {
                            redirects.fetch_add(1, Ordering::SeqCst);
                            axum::http::Response::builder()
                                .status(307)
                                .header("Location", "/elsewhere/info/lfs/objects/batch")
                                .body(axum::body::Body::empty())
                                .unwrap()
                        }),
                    )
                    .route(
                        "/hop/objects/:oid",
                        get(|axum::extract::Path(oid): axum::extract::Path<String>| async move {
                            axum::http::Response::builder()
                                .status(302)
                                .header("Location", format!("/objects/{oid}"))
                                .body(axum::body::Body::empty())
                                .unwrap()
                        }),
                    )
                    .route(
                        "/objects/:oid",
                        get(
                            move |axum::extract::Path(oid): axum::extract::Path<String>,
                                  headers: axum::http::HeaderMap| async move {
                                // The server's own header is what a download carries, and never the leader's token.
                                assert!(headers.get("authorization").is_none());
                                assert_eq!(headers.get("x-ticket").unwrap(), "t");
                                files.lock().unwrap().get(&oid).cloned().unwrap_or_default()
                            },
                        )
                        .put(
                            move |axum::extract::Path(oid): axum::extract::Path<String>,
                                  headers: axum::http::HeaderMap,
                                  body: axum::body::Bytes| async move {
                                assert!(headers.get("authorization").is_none());
                                assert_eq!(headers.get("x-ticket").unwrap(), "t");
                                // Git LFS uploads declare their length rather than arriving chunked.
                                assert_eq!(headers.get("content-length").unwrap(), &body.len().to_string());
                                uploads.lock().unwrap().insert(oid, body.to_vec());
                            },
                        ),
                    )
                    .route(
                        "/host.git/*path",
                        any(
                            move |method: axum::http::Method,
                                  axum::extract::Path(path): axum::extract::Path<String>,
                                  axum::extract::RawQuery(query): axum::extract::RawQuery,
                                  headers: axum::http::HeaderMap,
                                  body: axum::body::Bytes| async move {
                                if !authorized(&headers) {
                                    return axum::http::Response::builder()
                                        .status(401)
                                        .header("WWW-Authenticate", "Basic realm=\"host\"")
                                        .body(axum::body::Body::empty())
                                        .unwrap();
                                }
                                let content_type = headers
                                    .get("content-type")
                                    .and_then(|v| v.to_str().ok())
                                    .map(str::to_string);
                                let cgi = git::http_backend_receiving(
                                    &served,
                                    &path,
                                    method.as_str(),
                                    &query.unwrap_or_default(),
                                    content_type.as_deref(),
                                    &body,
                                )
                                .unwrap();
                                let mut response = axum::http::Response::builder().status(cgi.status);
                                for (name, value) in &cgi.headers {
                                    response = response.header(name, value);
                                }
                                response.body(axum::body::Body::from(cgi.body)).unwrap()
                            },
                        ),
                    );
            std::thread::spawn(move || {
                tokio::runtime::Runtime::new().unwrap().block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    axum::serve(listener, app).await.unwrap();
                })
            });
            Host {
                url: format!("{base}/host.git"),
                base,
                moved,
                hop,
                repo,
                lfs,
            }
        }

        fn remote(&self) -> Remote {
            Remote {
                url: self.url.clone(),
                user: "smeltr".to_string(),
                token: Some(TOKEN.to_string()),
                poll: None,
                approve_pushes: false,
            }
        }

        /// Commit `files` on the host's `main` and answer the commit.
        fn commit(&self, files: &[(&str, &[u8])], title: &str) -> String {
            let head = git::main_head(&self.repo).unwrap();
            let edits: Vec<git::FileEdit> = files
                .iter()
                .map(|(path, body)| git::FileEdit {
                    path: path.to_string(),
                    content: Some(body.to_vec()),
                })
                .collect();
            let author = Actor {
                id: "u-ada".to_string(),
                display_name: "Ada".to_string(),
            };
            let git::Committed::At(commit) =
                git::commit_edits(&self.repo, git::MAIN_BRANCH, &head, Some(&head), &edits, title, &author).unwrap()
            else {
                panic!("the host commit did not land")
            };
            git::forget_advertisement();
            commit
        }
    }

    const LOADS: [(&str, &[u8]); 3] = [
        ("groups/standalone/system.json", br#"{"version":1}"#),
        (
            "groups/standalone/sources.json",
            br#"[{"id":"m","type":"metrics","bucketSeconds":10}]"#,
        ),
        (
            "groups/standalone/pipelines/orders.json",
            br#"{"id":"orders","name":"orders","steps":[]}"#,
        ),
    ];

    /// The roots a leader's fetch trusts, written once for every test.
    fn roots() -> &'static Path {
        static ROOTS: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        ROOTS.get_or_init(|| {
            let path = roots_path(&temp_dir("roots"));
            crate::io::trust::write_pem_file(None, &path).unwrap();
            path
        })
    }

    /// A leader repository of its own history, following nothing yet.
    fn leader(name: &str) -> PathBuf {
        let repo = git::repo_path(&temp_dir(name));
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        repo
    }

    #[test]
    fn a_commit_that_loads_is_followed_and_the_fleet_runs_it() {
        let host = Host::start("follows-host");
        let commit = host.commit(&LOADS, "orders");
        let repo = leader("follows");
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(commit.clone())));
        assert_eq!(git::main_head(&repo).as_deref(), Some(commit.as_str()));
        assert_eq!(git::deployed(&repo).as_deref(), Some(commit.as_str()));
        let followed = git::log(&repo, git::MAIN_BRANCH, 1).unwrap().remove(0);
        assert_eq!(followed.author, "Ada", "the host's author line is kept");
        // The host's commit claims an account in its author address, which nobody here checked.
        assert_eq!(followed.author_id, None);
    }

    /// `main` follows the host whatever it holds, so a fix can be approved on top of it; the fleet does not move onto
    /// a group that stops loading, says why on every sync, and moves on with the host's fix.
    #[test]
    fn a_commit_that_breaks_a_group_moves_main_and_holds_the_fleet() {
        let host = Host::start("refused-host");
        let good = host.commit(&LOADS, "orders");
        let repo = leader("refused");
        sync(&repo, roots(), &host.remote()).unwrap();
        let bad = host.commit(&[("groups/standalone/sources.json", b"[{")], "break it");
        for _ in 0..2 {
            let (commit, why) = sync(&repo, roots(), &host.remote()).unwrap_err();
            assert_eq!(commit.as_deref(), Some(bad.as_str()));
            assert!(why.contains("\"standalone\""), "{why}");
            assert_eq!(git::main_head(&repo).as_deref(), Some(bad.as_str()));
            assert_eq!(git::deployed(&repo).as_deref(), Some(good.as_str()));
        }
        let mended = host.commit(&LOADS, "mend it");
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(mended.clone())));
        assert_eq!(git::deployed(&repo).as_deref(), Some(mended.as_str()));
    }

    /// A group already broken where the fleet runs does not stop the host's fix to another from being followed.
    #[test]
    fn a_fix_beside_a_group_already_broken_is_followed() {
        let host = Host::start("beside-host");
        host.commit(&LOADS, "orders");
        let broken = host.commit(&[("groups/other/system.json", b"not json")], "a broken group");
        let repo = leader("beside");
        sync(&repo, roots(), &host.remote()).unwrap_err();
        git::deploy_commit(data_dir(&repo), &broken, &Actor::system()).unwrap();

        let fix = host.commit(
            &[(
                "groups/standalone/pipelines/orders.json",
                br#"{"id":"orders","name":"orders v2","steps":[]}"#,
            )],
            "fix standalone",
        );
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(fix.clone())));
        assert_eq!(git::deployed(&repo).as_deref(), Some(fix.as_str()));
    }

    #[test]
    fn a_force_push_on_the_host_is_followed() {
        let host = Host::start("force-host");
        let first = host.commit(&LOADS, "orders");
        let repo = leader("force");
        sync(&repo, roots(), &host.remote()).unwrap();
        host.commit(
            &[(
                "groups/standalone/pipelines/more.json",
                br#"{"id":"more","name":"more","steps":[]}"#,
            )],
            "more",
        );
        sync(&repo, roots(), &host.remote()).unwrap();
        // The host's `main` goes back to `first`.
        let host_git = gix::open(&host.repo).unwrap();
        let id = host_git.rev_parse_single(first.as_str()).unwrap().detach();
        host_git
            .reference(
                "refs/heads/main",
                id,
                gix::refs::transaction::PreviousValue::Any,
                "force",
            )
            .unwrap();
        git::forget_advertisement();
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(first.clone())));
        assert_eq!(git::main_head(&repo).as_deref(), Some(first.as_str()));
    }

    #[test]
    fn a_held_fleet_stays_where_it_was_put_while_main_follows_the_host() {
        let host = Host::start("held-host");
        let first = host.commit(&LOADS, "orders");
        let repo = leader("held");
        sync(&repo, roots(), &host.remote()).unwrap();
        let second = host.commit(
            &[(
                "groups/standalone/pipelines/more.json",
                br#"{"id":"more","name":"more","steps":[]}"#,
            )],
            "more",
        );
        sync(&repo, roots(), &host.remote()).unwrap();
        git::deploy_commit(repo.parent().unwrap().parent().unwrap(), &first, &Actor::system()).unwrap();

        let third = host.commit(
            &[(
                "groups/standalone/pipelines/again.json",
                br#"{"id":"again","name":"again","steps":[]}"#,
            )],
            "again",
        );
        assert_ne!(second, third);
        sync(&repo, roots(), &host.remote()).unwrap();
        assert_eq!(git::main_head(&repo).as_deref(), Some(third.as_str()));
        assert_eq!(git::deployed(&repo).as_deref(), Some(first.as_str()), "the hold stays");
        assert_eq!(git::hold(&repo).unwrap().unwrap().behind, 2);
    }

    const CITIES: &[u8] = b"ip,city\n10.0.0.1,Oslo\n";

    /// Commit on the host a bundle with a lookup file of [`CITIES`], committed as its pointer, plus `extra`. Answers
    /// the commit and the file's oid.
    fn commit_cities(host: &Host, extra: &[(&str, &[u8])]) -> (String, String) {
        let oid = {
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, CITIES);
            digest.as_ref().iter().map(|b| format!("{b:02x}")).collect::<String>()
        };
        let pointer = Pointer {
            oid: oid.clone(),
            size: CITIES.len() as u64,
        };
        let mut files = LOADS.to_vec();
        let text = pointer.text();
        files.push((
            "groups/standalone/lookup-tables.json",
            br#"[{"id":"cities","type":"csv","delimiter":",","path":"lookups/cities.csv"}]"#,
        ));
        files.push(("groups/standalone/lookups/cities.csv", text.as_bytes()));
        files.extend_from_slice(extra);
        (host.commit(&files, "cities"), oid)
    }

    #[test]
    fn a_lookup_file_the_store_lacks_is_fetched_from_the_host_s_lfs_and_checked() {
        let host = Host::start("lfs-host");
        let bytes = CITIES.to_vec();
        let (commit, oid) = commit_cities(&host, &[]);
        let repo = leader("lfs");

        // Not on the host's LFS server: refused, and the next sync tries again.
        let (_, why) = sync(&repo, roots(), &host.remote()).unwrap_err();
        assert!(why.contains(&oid) && why.contains("not found"), "{why}");
        assert_ne!(git::main_head(&repo).as_deref(), Some(commit.as_str()));

        // Bytes that are not the pointer's: refused, and not kept.
        host.lfs.lock().unwrap().insert(oid.clone(), b"ip,city\n".to_vec());
        let (_, why) = sync(&repo, roots(), &host.remote()).unwrap_err();
        assert!(why.contains(&oid), "{why}");
        assert!(!store::path_of(&repo, &oid).exists());

        host.lfs.lock().unwrap().insert(oid.clone(), bytes.clone());
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(commit.clone())));
        assert_eq!(std::fs::read(store::path_of(&repo, &oid)).unwrap(), bytes);
        assert!(
            store::path_of(&repo, &format!("{oid}.name")).exists(),
            "retention counts it"
        );
    }

    /// A repository's `.lfsconfig` names where its files are served, and a redirect of the batch request is followed
    /// with its body.
    #[test]
    fn lookup_files_come_from_where_the_lfsconfig_and_redirects_send_them() {
        let host = Host::start("lfsconfig-host");
        let lfsconfig = format!("[lfs]\n\turl = {}/moved\n", host.base);
        let (commit, oid) = commit_cities(&host, &[(".lfsconfig", lfsconfig.as_bytes())]);
        host.lfs.lock().unwrap().insert(oid.clone(), CITIES.to_vec());
        let repo = leader("lfsconfig");
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(commit)));
        assert_eq!(std::fs::read(store::path_of(&repo, &oid)).unwrap(), CITIES);
        assert_eq!(host.moved.load(Ordering::SeqCst), 1, "asked where .lfsconfig says");
    }

    #[test]
    fn a_plain_http_remote_s_download_follows_a_redirect() {
        let host = Host::start("hop-host");
        let (commit, oid) = commit_cities(&host, &[]);
        host.lfs.lock().unwrap().insert(oid.clone(), CITIES.to_vec());
        host.hop.store(true, Ordering::SeqCst);
        let repo = leader("hop");
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(commit)));
        assert_eq!(std::fs::read(store::path_of(&repo, &oid)).unwrap(), CITIES);
    }

    #[test]
    fn redirects_keep_to_the_rules_the_remote_s_url_keeps() {
        let from = "https://git.example/team/config.git/info/lfs/objects/batch";
        assert_eq!(
            redirected(from, "/lfs/objects/batch"),
            Ok("https://git.example/lfs/objects/batch".to_string())
        );
        assert!(redirected(from, "http://git.example/lfs").is_err());
        assert!(redirected(from, "//elsewhere.example/lfs").is_err());
        // A remote the admin gave as plain HTTP is followed over plain HTTP.
        let plain = "http://git.example/team/config.git/info/lfs/objects/batch";
        assert_eq!(
            redirected(plain, "/lfs/objects/batch"),
            Ok("http://git.example/lfs/objects/batch".to_string())
        );
        assert!(redirected(plain, "http://git.example/lfs").is_ok());
        assert_eq!(origin("https://Git.Example/a"), origin("https://git.example:443/b"));
        assert_ne!(origin("https://git.example/a"), origin("https://git.example:8443/a"));
    }

    #[test]
    fn an_empty_remote_is_given_the_leader_s_main() {
        let host = Host::serve("empty-host", false);
        let repo = leader("empty");
        let main = git::main_head(&repo).unwrap();
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(main.clone())));
        assert_eq!(git::main_head(&host.repo), Some(main.clone()));
        assert_eq!(git::main_head(&repo), Some(main.clone()));
        // The next sync finds the remote's main where the leader's is, and has nothing to do.
        assert_eq!(sync(&repo, roots(), &host.remote()), Ok(Some(main)));
    }

    /// Commit `files` on the leader's `branch`, from `main` the first time, and answer the commit.
    fn save(repo: &Path, branch: &str, files: &[(&str, &[u8])]) -> String {
        let parent = git::branch_tip(repo, branch).or_else(|| git::main_head(repo)).unwrap();
        let expected = git::branch_tip(repo, branch);
        let edits: Vec<git::FileEdit> = files
            .iter()
            .map(|(path, body)| git::FileEdit {
                path: path.to_string(),
                content: Some(body.to_vec()),
            })
            .collect();
        let git::Committed::At(commit) = git::commit_edits(
            repo,
            branch,
            &parent,
            expected.as_deref(),
            &edits,
            "save",
            &Actor::system(),
        )
        .unwrap() else {
            panic!("the save did not land")
        };
        commit
    }

    /// The leader following `host`, which holds what the leader's `main` holds.
    fn following(host: &Host, name: &str) -> PathBuf {
        let repo = leader(name);
        host.commit(&LOADS, "orders");
        sync(&repo, roots(), &host.remote()).unwrap();
        repo
    }

    #[test]
    fn a_save_is_pushed_to_the_host_s_branch_of_the_same_name_and_again_on_the_next_save() {
        let host = Host::start("push-host");
        let repo = following(&host, "push");
        let first = save(
            &repo,
            "al/fix",
            &[("groups/standalone/system.json", br#"{"version":1,"x":1}"#)],
        );
        assert_eq!(
            push_all(&repo, &host.remote(), &HashMap::new(), 0, 0),
            Ok(HashMap::new())
        );
        assert_eq!(git::branch_tip(&host.repo, "al/fix"), Some(first));

        let second = save(
            &repo,
            "al/fix",
            &[("groups/standalone/system.json", br#"{"version":1,"x":2}"#)],
        );
        assert_eq!(
            push_all(&repo, &host.remote(), &HashMap::new(), 0, 0),
            Ok(HashMap::new())
        );
        assert_eq!(git::branch_tip(&host.repo, "al/fix"), Some(second));

        // Deleted here, deleted there.
        git::delete_branch(repo.parent().unwrap().parent().unwrap(), "al/fix", None).unwrap();
        assert_eq!(
            push_all(&repo, &host.remote(), &HashMap::new(), 0, 0),
            Ok(HashMap::new())
        );
        assert_eq!(git::branch_tip(&host.repo, "al/fix"), None);
        assert!(!git::host_branches(&repo).unwrap().contains_key("al/fix"));
    }

    const MORE: (&str, &[u8]) = (
        "groups/standalone/pipelines/more.json",
        br#"{"id":"more","name":"more","steps":[]}"#,
    );

    /// A leader following `host`, with a change open on `al/more` that adds [`MORE`], and the remote set to approve
    /// onto the host's `main`.
    fn approving(host: &Host, name: &str) -> (PathBuf, Remote) {
        let repo = following(host, name);
        save(&repo, "al/more", &[MORE]);
        git::open_change(&repo, "al/more", "More", "", &Actor::system()).unwrap();
        (
            repo,
            Remote {
                approve_pushes: true,
                ..host.remote()
            },
        )
    }

    fn data_dir(repo: &Path) -> &Path {
        repo.parent().unwrap().parent().unwrap()
    }

    #[test]
    fn approve_pushes_the_change_to_the_host_s_main_then_deploys_it() {
        let host = Host::start("approve-host");
        let (repo, remote) = approving(&host, "approve");
        let held = git::main_head(&repo).unwrap();
        // A rollback holds through an approval with a remote as without one.
        host.commit(
            &[(
                "groups/standalone/pipelines/again.json",
                br#"{"id":"again","name":"again","steps":[]}"#,
            )],
            "again",
        );
        sync(&repo, roots(), &remote).unwrap();
        assert!(matches!(
            git::bring_current(data_dir(&repo), "al/more", &Actor::system(), None).unwrap(),
            git::BringCurrent::Done(_)
        ));
        git::deploy_commit(data_dir(&repo), &held, &Actor::system()).unwrap();

        let merged = approve(data_dir(&repo), &remote, "al/more", &Actor::system()).unwrap();
        assert_eq!(git::main_head(&host.repo), Some(merged.clone()));
        assert_eq!(git::main_head(&repo), Some(merged.clone()));
        assert_eq!(git::deployed(&repo), Some(held));
        assert_eq!(git::host_branches(&repo).unwrap().get(git::MAIN_BRANCH), Some(&merged));
        assert!(!git::branch_exists(&repo, "al/more").unwrap());
        // The next sync finds the host where the approval left it.
        assert_eq!(sync(&repo, roots(), &remote), Ok(Some(merged)));
    }

    #[test]
    fn a_save_made_while_approve_pushes_keeps_its_branch() {
        let host = Host::start("approve-save-host");
        let (repo, remote) = approving(&host, "approve-save");
        let squash = git::squash_change(data_dir(&repo), "al/more", &Actor::system()).unwrap();
        let saved = save(
            &repo,
            "al/more",
            &[(
                "groups/standalone/pipelines/late.json",
                br#"{"id":"late","name":"late","steps":[]}"#,
            )],
        );
        assert_eq!(
            push_branch(
                &repo,
                &remote,
                git::MAIN_BRANCH,
                Some(&squash.commit),
                Some(&squash.head)
            ),
            Ok(git::Pushed::Done)
        );
        git::deploy_squash(data_dir(&repo), &squash, &Actor::system()).unwrap();
        assert_eq!(git::main_head(&repo), Some(squash.commit));
        assert_eq!(git::branch_tip(&repo, "al/more"), Some(saved), "the save is not lost");
    }

    /// A save landing between the judgment and the approval refuses it, rather than pushing what nobody judged.
    #[test]
    fn approve_squashes_the_tip_it_judged() {
        let host = Host::start("approve-tip-host");
        let (repo, remote) = approving(&host, "approve-tip");
        let head = git::main_head(&repo);
        let judged = git::branch_tip(&repo, "al/more").unwrap();
        save(
            &repo,
            "al/more",
            &[(
                "groups/standalone/pipelines/late.json",
                br#"{"id":"late","name":"late","steps":[]}"#,
            )],
        );
        let refused = approve_at(data_dir(&repo), &remote, "al/more", &judged, &Actor::system()).unwrap_err();
        assert!(refused.contains("moved"), "{refused}");
        assert_eq!(git::main_head(&host.repo), head);
        assert_eq!(git::main_head(&repo), head);
        assert!(git::branch_exists(&repo, "al/more").unwrap());
    }

    #[test]
    fn approve_is_refused_unless_the_remote_s_settings_allow_it() {
        let host = Host::start("approve-off-host");
        let (repo, _) = approving(&host, "approve-off");
        let head = git::main_head(&repo);
        let refused = approve(data_dir(&repo), &host.remote(), "al/more", &Actor::system()).unwrap_err();
        assert!(refused.contains("merge the change there"), "{refused}");
        assert_eq!(git::main_head(&repo), head);
        assert_eq!(git::main_head(&host.repo), head);
    }

    #[test]
    fn approve_is_refused_when_the_host_s_main_moved_since_it_was_fetched() {
        let host = Host::start("approve-moved-host");
        let (repo, remote) = approving(&host, "approve-moved");
        let head = git::main_head(&repo);
        let moved = host.commit(
            &[(
                "groups/standalone/pipelines/again.json",
                br#"{"id":"again","name":"again","steps":[]}"#,
            )],
            "again",
        );
        let refused = approve(data_dir(&repo), &remote, "al/more", &Actor::system()).unwrap_err();
        assert!(refused.contains("not the one this leader runs"), "{refused}");
        assert_eq!(git::main_head(&host.repo), Some(moved));
        assert_eq!(git::main_head(&repo), head);
        assert_eq!(git::deployed(&repo), head);
        assert!(git::branch_exists(&repo, "al/more").unwrap());
    }

    #[test]
    fn approve_is_refused_while_the_leader_does_not_follow_the_host_s_main() {
        let host = Host::start("approve-behind-host");
        let (repo, remote) = approving(&host, "approve-behind");
        let head = git::main_head(&repo);
        // A lookup file committed as its bytes is one the leader will not follow.
        let mut files = LOADS.to_vec();
        files.push((
            "groups/standalone/lookup-tables.json",
            br#"[{"id":"cities","type":"csv","delimiter":",","path":"lookups/cities.csv"}]"#,
        ));
        files.push(("groups/standalone/lookups/cities.csv", b"ip,city\n"));
        let bad = host.commit(&files, "plain");
        sync(&repo, roots(), &remote).unwrap_err();
        // The lease is on `bad`, so a squash onto `head` would pass it and drop `bad` from the host.
        let refused = approve(data_dir(&repo), &remote, "al/more", &Actor::system()).unwrap_err();
        assert!(refused.contains("not the one this leader runs"), "{refused}");
        assert_eq!(git::main_head(&host.repo), Some(bad));
        assert_eq!(git::main_head(&repo), head);
    }

    #[test]
    fn a_host_that_protects_main_refuses_approve_and_says_why() {
        let host = Host::start("approve-protected-host");
        let (repo, remote) = approving(&host, "approve-protected");
        let head = git::main_head(&repo);
        let hook = host.repo.join("hooks").join("pre-receive");
        std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
        std::fs::write(&hook, "#!/bin/sh\necho 'main is protected' >&2\nexit 1\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let refused = approve(data_dir(&repo), &remote, "al/more", &Actor::system()).unwrap_err();
        assert!(refused.contains("main is protected"), "{refused}");
        assert_eq!(git::main_head(&host.repo), head);
        assert_eq!(git::main_head(&repo), head);
        assert_eq!(git::deployed(&repo), head);
        assert!(git::branch_exists(&repo, "al/more").unwrap());
    }

    #[test]
    fn a_branch_someone_else_pushed_to_is_refused_until_overwritten() {
        let host = Host::start("refused-push-host");
        let repo = following(&host, "refused-push");
        save(
            &repo,
            "al/fix",
            &[("groups/standalone/system.json", br#"{"version":1,"x":1}"#)],
        );
        push_all(&repo, &host.remote(), &HashMap::new(), 0, 0).unwrap();
        // Someone pushes to the host's branch; then the GUI saves on it.
        let theirs = git::commit_edits(
            &host.repo,
            "al/fix",
            &git::branch_tip(&host.repo, "al/fix").unwrap(),
            git::branch_tip(&host.repo, "al/fix").as_deref(),
            &[git::FileEdit {
                path: "groups/standalone/system.json".to_string(),
                content: Some(br#"{"version":1,"x":"theirs"}"#.to_vec()),
            }],
            "theirs",
            &Actor::system(),
        )
        .unwrap();
        let git::Committed::At(theirs) = theirs else { panic!() };
        let ours = save(
            &repo,
            "al/fix",
            &[("groups/standalone/system.json", br#"{"version":1,"x":2}"#)],
        );

        let failed = push_all(&repo, &host.remote(), &HashMap::new(), 7, 0).unwrap();
        let failed = &failed["al/fix"];
        assert!(failed.refused, "{failed:?}");
        assert_eq!(
            (failed.tip.as_deref(), failed.host_tip.as_deref()),
            (Some(ours.as_str()), Some(theirs.as_str()))
        );
        assert_eq!(
            git::branch_tip(&host.repo, "al/fix"),
            Some(theirs.clone()),
            "the local commit is kept, theirs too"
        );

        // Refused stays refused, however long it waits, until someone chooses.
        let again = push_all(
            &repo,
            &host.remote(),
            &HashMap::from([("al/fix".to_string(), failed.clone())]),
            i64::MAX,
            0,
        );
        assert_eq!(again.unwrap()["al/fix"], *failed);

        assert_eq!(
            push_branch(&repo, &host.remote(), "al/fix", Some(&ours), Some(&theirs)),
            Ok(git::Pushed::Done)
        );
        assert_eq!(git::branch_tip(&host.repo, "al/fix"), Some(ours));
    }

    #[test]
    fn a_host_that_is_down_gets_the_save_once_it_is_back() {
        let host = Host::start("down-host");
        let repo = following(&host, "down");
        let tip = save(
            &repo,
            "al/fix",
            &[("groups/standalone/system.json", br#"{"version":1,"x":1}"#)],
        );
        let mut down = host.remote();
        down.url = "http://127.0.0.1:9/host.git".to_string();
        let failed = push_all(&repo, &down, &HashMap::new(), 1_000, 60_000).unwrap();
        assert!(!failed["al/fix"].refused);
        assert_eq!(failed["al/fix"].tip.as_deref(), Some(tip.as_str()));

        // Not tried again before the retry is due, then pushed once it is.
        assert_eq!(push_all(&repo, &host.remote(), &failed, 2_000, 60_000).unwrap(), failed);
        assert_eq!(
            push_all(&repo, &host.remote(), &failed, 61_000, 60_000),
            Ok(HashMap::new())
        );
        assert_eq!(git::branch_tip(&host.repo, "al/fix"), Some(tip));
    }

    #[test]
    fn a_pushed_branch_s_lookup_files_go_to_the_host_s_lfs_first() {
        let host = Host::start("upload-host");
        let repo = following(&host, "upload");
        // The leader holds the file, uploaded through Smeltr.
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, CITIES);
        let oid: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        let path = store::path_of(&repo, &oid);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, CITIES).unwrap();
        let text = Pointer {
            oid: oid.clone(),
            size: CITIES.len() as u64,
        }
        .text();
        save(
            &repo,
            "al/cities",
            &[
                (
                    "groups/standalone/lookup-tables.json",
                    br#"[{"id":"cities","type":"csv","delimiter":",","path":"lookups/cities.csv"}]"#,
                ),
                ("groups/standalone/lookups/cities.csv", text.as_bytes()),
            ],
        );
        assert_eq!(
            push_all(&repo, &host.remote(), &HashMap::new(), 0, 0),
            Ok(HashMap::new())
        );
        assert_eq!(host.lfs.lock().unwrap().get(&oid).map(Vec::as_slice), Some(CITIES));
    }

    #[test]
    fn a_lookup_committed_as_its_bytes_is_refused() {
        let host = Host::start("plain-host");
        let mut files = LOADS.to_vec();
        files.push((
            "groups/standalone/lookup-tables.json",
            br#"[{"id":"cities","type":"csv","delimiter":",","path":"lookups/cities.csv"}]"#,
        ));
        files.push(("groups/standalone/lookups/cities.csv", b"ip,city\n"));
        host.commit(&files, "plain");
        let repo = leader("plain");
        let before = git::main_head(&repo);
        let (_, why) = sync(&repo, roots(), &host.remote()).unwrap_err();
        assert!(why.contains("git-lfs"), "{why}");
        assert_eq!(git::main_head(&repo), before);
    }

    #[test]
    fn a_wrong_token_is_reported_as_the_remote_refusing_it() {
        let host = Host::start("token-host");
        host.commit(&LOADS, "orders");
        let repo = leader("token");
        let mut remote = host.remote();
        remote.token = Some("wrong".to_string());
        let (commit, why) = sync(&repo, roots(), &remote).unwrap_err();
        assert_eq!(commit, None);
        assert!(
            why.contains(&host.url)
                && why.ends_with("401 Unauthorized. Check that the token can read this repository, and check the URL"),
            "{why}"
        );
        remote.token = None;
        let (_, why) = sync(&repo, roots(), &remote).unwrap_err();
        assert!(
            why.ends_with("401 Unauthorized. Set a token that can read this repository, and check the URL"),
            "{why}"
        );
    }

    #[test]
    fn a_refusal_names_the_status_the_host_answered() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/host.git", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let _ = std::io::Read::read(&mut stream, &mut [0; 4096]);
                let _ = std::io::Write::write_all(&mut stream, b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n");
            }
        });
        let remote = Remote {
            url,
            ..Host::start("forbidden-host").remote()
        };
        let (_, why) = sync(&leader("forbidden"), roots(), &remote).unwrap_err();
        assert!(why.ends_with("403"), "{why}");
    }

    /// The certificate is checked against the roots, so a host no root signed is refused for its certificate, and the
    /// message says so rather than only that the handshake failed.
    #[test]
    fn an_https_host_is_checked_against_the_roots() {
        let signed = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let config = Arc::new(
            rustls::ServerConfig::builder_with_provider(crate::crypto::in_use())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![signed.cert.der().clone()],
                    rustls::pki_types::PrivateKeyDer::Pkcs8(signed.signing_key.serialize_der().into()),
                )
                .unwrap(),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(config.clone()).unwrap(), stream);
                let _ = std::io::Read::read(&mut tls, &mut [0; 1]);
            }
        });
        let repo = leader("https");
        let remote = Remote {
            url: format!("https://localhost:{port}/host.git"),
            ..Host::start("https-host").remote()
        };
        let (_, why) = sync(&repo, roots(), &remote).unwrap_err();
        assert!(why.contains("certificate") && !why.contains("verifier"), "{why}");
    }

    fn state(name: &str) -> AppState {
        let data_dir = temp_dir(name);
        let repo = git::repo_path(&data_dir);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let store =
            crate::leader::sqlite_store::SqliteStore::open(Path::new(":memory:"), &data_dir.join("sealing.key"))
                .unwrap();
        AppState {
            store: Arc::new(store),
            registry: Default::default(),
            captures: Default::default(),
            entitlements: Default::default(),
            verify_limiter: Default::default(),
            expiries: Default::default(),
            throughput: Default::default(),
            deploy_checks: Default::default(),
            origin_hosts: Vec::new(),
            remote: Default::default(),
            directives: Default::default(),
            data_dir,
        }
    }

    fn put(url: &str, token: Option<&str>, poll_seconds: u64) -> Json<PutRemoteSettings> {
        Json(PutRemoteSettings {
            url: url.to_string(),
            user: "smeltr".to_string(),
            token: token.map(str::to_string),
            poll_seconds,
            approve_pushes: false,
        })
    }

    /// An overwrite pushes the commit the admin was shown over the host's commit they were shown, and nothing else;
    /// a host that moved again is shown afresh.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_overwrite_pushes_only_what_the_admin_was_shown() {
        let host = Host::start("overwrite-host");
        let state = state("overwrite");
        assert!(
            put_settings(State(state.clone()), None, put(&host.url, Some(TOKEN), 0))
                .await
                .ok
        );
        let repo = git::repo_path(&state.data_dir);
        let host_main = git::main_head(&host.repo).unwrap();
        let git::Committed::At(theirs) = git::commit_edits(
            &host.repo,
            "al/fix",
            &host_main,
            None,
            &[git::FileEdit {
                path: "theirs.txt".to_string(),
                content: Some(b"theirs".to_vec()),
            }],
            "theirs",
            &Actor::system(),
        )
        .unwrap() else {
            panic!("their commit did not land")
        };
        let ours = save(&repo, "al/fix", &[("ours.txt", b"ours")]);
        let overwrite_with = |tip: &str, host_tip: &str| {
            overwrite(
                State(state.clone()),
                axum::extract::Path("al/fix".to_string()),
                Json(Overwrite {
                    tip: tip.to_string(),
                    host_tip: Some(host_tip.to_string()),
                }),
            )
        };

        let stale_save = overwrite_with(&host_main, &theirs).await;
        assert!(
            stale_save.errors[0].message.contains("saved again"),
            "{:?}",
            stale_save.errors
        );

        let stale_host = overwrite_with(&ours, &host_main).await;
        assert!(!stale_host.ok);
        assert_eq!(
            state.remote.push_failures()["al/fix"].host_tip.as_deref(),
            Some(theirs.as_str())
        );
        assert_eq!(git::branch_tip(&host.repo, "al/fix"), Some(theirs.clone()));

        assert!(overwrite_with(&ours, &theirs).await.ok);
        assert_eq!(git::branch_tip(&host.repo, "al/fix"), Some(ours));
        assert!(state.remote.push_failures().is_empty());
    }

    /// `main` is the host's, so it is never overwritten from here.
    #[tokio::test(flavor = "multi_thread")]
    async fn main_is_never_overwritten() {
        let host = Host::start("main-host");
        let state = state("main-overwrite");
        assert!(
            put_settings(State(state.clone()), None, put(&host.url, Some(TOKEN), 0))
                .await
                .ok
        );
        let body = Json(Overwrite {
            tip: "0".repeat(40),
            host_tip: None,
        });
        let refused = overwrite(
            State(state.clone()),
            axum::extract::Path(git::MAIN_BRANCH.to_string()),
            body,
        )
        .await;
        assert!(refused.errors[0].message.contains("comes back by fetching"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn settings_keep_the_token_to_themselves_and_sync_now_follows_at_once() {
        let host = Host::start("settings-host");
        let state = state("settings");
        let refused = put_settings(
            State(state.clone()),
            None,
            put("ssh://git@git.example.com/c.git", Some(TOKEN), 0),
        )
        .await;
        assert!(!refused.ok && refused.errors[0].message.contains("SSH"));

        let saved = put_settings(State(state.clone()), None, put(&host.url, Some(TOKEN), 0)).await;
        let settings = saved.0.settings.as_ref().unwrap();
        assert!(settings.token_set && settings.poll_seconds == 0);
        assert!(!serde_json::to_string(&saved.0).unwrap().contains(TOKEN));

        // The timer is off, so the host's commit is followed on Sync now, and only then.
        let commit = host.commit(&LOADS, "orders");
        let repo = git::repo_path(&state.data_dir);
        assert_ne!(git::main_head(&repo).as_deref(), Some(commit.as_str()));
        let synced = post_sync(State(state.clone())).await;
        assert_eq!(
            synced.0.body.settings.unwrap().sync.remote_head.as_deref(),
            Some(commit.as_str())
        );
        assert_eq!(git::main_head(&repo).as_deref(), Some(commit.as_str()));

        // A commit that does not load is a warning until a sync succeeds.
        host.commit(&[("groups/standalone/sources.json", b"[{")], "break it");
        assert!(
            post_sync(State(state.clone())).await.ok,
            "a sync that reached the remote answers"
        );
        let warnings = crate::leader::warnings_snapshot(&state).await.unwrap();
        assert!(
            warnings
                .iter()
                .any(|w| w.condition == crate::leader::warnings::Condition::RemoteNotFollowed),
            "{warnings:?}"
        );

        // Mended on the host: the warning clears on the next sync.
        let mended = host.commit(&LOADS, "mend it");
        let synced = post_sync(State(state.clone())).await.0.body.settings.unwrap().sync;
        assert_eq!((synced.remote_head, synced.failure), (Some(mended), None));

        // Saving with no token keeps the stored one for the same URL, and forgets it for another.
        assert!(
            put_settings(State(state.clone()), None, put(&host.url, None, 0))
                .await
                .ok
        );
        assert!(
            get_settings(State(state.clone()))
                .await
                .0
                .body
                .settings
                .unwrap()
                .token_set
        );
        assert!(
            !get_settings(State(state.clone()))
                .await
                .0
                .body
                .settings
                .unwrap()
                .approve_pushes
        );
        let approving = PutRemoteSettings {
            approve_pushes: true,
            ..put(&host.url, None, 0).0
        };
        assert!(put_settings(State(state.clone()), None, Json(approving)).await.ok);
        assert!(
            get_settings(State(state.clone()))
                .await
                .0
                .body
                .settings
                .unwrap()
                .approve_pushes
        );
        let elsewhere = host.url.replace("host.git", "other.git");
        assert!(
            put_settings(State(state.clone()), None, put(&elsewhere, None, 0))
                .await
                .ok
        );
        assert!(
            !get_settings(State(state.clone()))
                .await
                .0
                .body
                .settings
                .unwrap()
                .token_set
        );
        assert!(put_settings(State(state.clone()), None, put("", None, 0)).await.ok);
        assert_eq!(run_store(state.store.clone(), configured).await.unwrap(), None);
        assert_eq!(state.store.opened_setting(TOKEN_KEY).unwrap(), None);
    }

    #[test]
    fn only_http_or_https_is_followed() {
        assert!(check_url("https://git.example.com/team/config.git", false).is_ok());
        assert!(check_url("http://127.0.0.1:3000/team/config.git", false).is_ok());
        assert!(check_url("http://localhost/config.git", false).is_ok());
        assert!(check_url("http://git.example.com/team/config.git", false).is_err());
        assert!(check_url("http://git.example.com/team/config.git", true).is_ok());
        assert!(check_url("ssh://git@git.example.com/team/config.git", false).is_err());
        assert!(check_url("git@git.example.com:team/config.git", false).is_err());
        assert!(check_url("file:///srv/config.git", false).is_err());
        assert!(check_url("https://bot:ghp_x@github.com/team/config.git", false).is_ok());
        assert!(check_url("http://evil.example\\@127.0.0.1/config.git", false).is_err());
        assert!(check_url("http://", true).is_err());
        assert!(check_url("http://?x", true).is_err());
    }

    #[test]
    fn a_remote_off_this_network_cannot_send_a_download_onto_it() {
        let public = Remote {
            url: "https://203.0.113.7/team/config.git".to_string(),
            user: String::new(),
            token: None,
            poll: None,
            approve_pushes: false,
        };
        let refused = agent(&public, "http://127.0.0.1:9/x", FETCH_BUDGET, true)
            .get("http://127.0.0.1:9/x")
            .call();
        let refused = refused.unwrap_err().to_string();
        assert!(refused.contains("own network"), "{refused}");
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "192.168.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "::1",
            "fd00::1",
            "::ffff:10.0.0.1",
        ] {
            assert!(internal(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["203.0.113.7", "8.8.8.8", "2001:db8::1"] {
            assert!(!internal(ip.parse().unwrap()), "{ip}");
        }
    }
}
