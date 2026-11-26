//! git's fetch, served to workers: how a worker gets what the fleet runs (`git::DEPLOYED_REF`) from its leader.
//!
//! The leader speaks git's protocol v2 over smart HTTP itself, so no git runs on it and nothing of git's needs to be
//! installed beside it. Protocol v2 is what gix speaks, and git since 2.26: a client asks what the server offers,
//! lists the refs it wants, then asks for a pack.
//!
//! ```mermaid
//! sequenceDiagram
//!   worker->>leader: GET info/refs?service=git-upload-pack (Git-Protocol: version=2)
//!   leader-->>worker: version 2, ls-refs, fetch
//!   worker->>leader: POST git-upload-pack: command=ls-refs, ref-prefix ...
//!   leader-->>worker: each served ref and its commit
//!   worker->>leader: POST git-upload-pack: command=fetch, want ..., have ..., done
//!   leader-->>worker: acknowledgments, then the pack on band 1
//! ```
//!
//! Every request is answered on its own, as the protocol's statelessness over HTTP asks: a fetch carries every
//! `have` the leader needs to know. The repository is read-only here: nothing is pushed to it.

use crate::sync::Mutex;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::git;
use super::pkt_line::{self, MAX_PAYLOAD, Packet};

/// What the leader answers a git request with.
#[derive(Debug)]
pub struct Answer {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

impl Answer {
    fn text(status: u16, why: &str) -> Answer {
        Answer {
            status,
            content_type: "text/plain; charset=utf-8",
            body: why.as_bytes().to_vec(),
        }
    }

    /// A refusal a git client reports as the server's: an `ERR` packet in place of the answer.
    fn refused(why: &str) -> Answer {
        Answer {
            status: 200,
            content_type: RESULT,
            body: pkt_line::line(format!("ERR {why}\n").as_bytes()),
        }
    }
}

const RESULT: &str = "application/x-git-upload-pack-result";

/// The bytes of one band-1 packet: a pkt-line less the byte that names its band.
const BAND_PAYLOAD: usize = MAX_PAYLOAD - 1;

/// Answer one git request for the repository at `repo`: `path` under the repository's URL, the request's `query`,
/// its `Git-Protocol` header, and its body. `Err` is a failure of the leader's own, which the route reports.
pub fn serve(
    repo: &Path,
    path: &str,
    method: &str,
    query: &str,
    protocol: Option<&str>,
    body: &[u8],
) -> Result<Answer, String> {
    let v2 = protocol.is_some_and(|p| p.split(':').any(|kv| kv.trim() == "version=2"));
    let service = query.split('&').find_map(|kv| kv.strip_prefix("service="));
    let answer = match (method, path.trim_start_matches('/'), service) {
        (_, "info/refs", Some("git-receive-pack")) | (_, "git-receive-pack", _) => Answer::text(
            403,
            "the leader's repository is read-only: a change reaches it through the leader's API, or the git host it \
             follows",
        ),
        // Every smeltr worker speaks v2, so nothing that asks for another version is a worker.
        ("GET", "info/refs", Some("git-upload-pack")) | ("POST", "git-upload-pack", _) if !v2 => Answer::text(
            400,
            "the leader serves git's protocol v2 only, which every smeltr worker speaks",
        ),
        ("GET", "info/refs", Some("git-upload-pack")) => Answer {
            status: 200,
            content_type: "application/x-git-upload-pack-advertisement",
            body: capabilities(),
        },
        ("POST", "git-upload-pack", _) => command(repo, body)?,
        _ => Answer::text(404, "not a git request the leader serves"),
    };
    tracing::trace!(name: "served", target: "git", path, method, status = answer.status, bytes = answer.body.len(), "the leader's repository answered a git request");
    Ok(answer)
}

/// What the leader offers: the two commands a fetch needs. The repository's ids are SHA-1, as the leader's are.
fn capabilities() -> Vec<u8> {
    let mut body: Vec<u8> = ["version 2", "agent=smeltr", "ls-refs", "fetch", "object-format=sha1"]
        .iter()
        .flat_map(|line| pkt_line::line(format!("{line}\n").as_bytes()))
        .collect();
    body.extend_from_slice(b"0000");
    body
}

/// A command request: the command, its capabilities, a delimiter, its arguments, a flush.
fn command(repo: &Path, body: &[u8]) -> Result<Answer, String> {
    let (command, arguments) = match request(body) {
        Ok(parsed) => parsed,
        Err(why) => return Ok(Answer::refused(&why)),
    };
    let answered = match command.as_str() {
        "ls-refs" => ls_refs(repo, &arguments),
        "fetch" => fetch(repo, &arguments),
        other => Err(Refused::Asked(format!("{other} is not a command the leader offers"))),
    };
    match answered {
        Ok(body) => Ok(Answer {
            status: 200,
            content_type: RESULT,
            body,
        }),
        Err(Refused::Asked(why)) => Ok(Answer::refused(&why)),
        Err(Refused::Failed(why)) => Err(why),
    }
}

/// Why a command was not answered: the request asked for something it may not have, or the leader failed.
enum Refused {
    Asked(String),
    Failed(String),
}

/// The command a request names, and its arguments.
fn request(mut body: &[u8]) -> Result<(String, Vec<String>), String> {
    let mut lines = Vec::new();
    let mut delimited = None;
    loop {
        let (packet, rest) = pkt_line::next(body)?;
        body = rest;
        match packet {
            Packet::Data(line) => lines.push(
                String::from_utf8(line.to_vec())
                    .map_err(|_| "a request line that is not UTF-8".to_string())?
                    .trim_end_matches('\n')
                    .to_string(),
            ),
            Packet::Delim if delimited.is_none() => delimited = Some(lines.len()),
            Packet::Flush => break,
            other => return Err(format!("a {other:?} packet where the request has none")),
        }
    }
    let arguments = lines.split_off(delimited.unwrap_or(lines.len()));
    let mut capabilities = lines.into_iter();
    let command = capabilities
        .next()
        .and_then(|line| line.strip_prefix("command=").map(str::to_string))
        .ok_or("a request that names no command")?;
    for capability in capabilities {
        match capability.split_once('=') {
            Some(("agent", _)) => {}
            Some(("object-format", "sha1")) => {}
            _ => return Err(format!("{capability} is not a capability the leader offers")),
        }
    }
    Ok((command, arguments))
}

/// Whether workers are served `name`. `refs/smeltr/` is the leader's own bookkeeping, change markers and the deploy
/// log among it, of which a worker needs only the commit the fleet runs. `refs/remotes/` is what the leader fetched
/// from the git host it follows, and `main` is what came of it.
fn served(name: &str) -> bool {
    match name.strip_prefix("refs/smeltr/") {
        Some(_) => name == git::DEPLOYED_REF,
        None => !name.starts_with("refs/remotes/"),
    }
}

fn open(repo: &Path) -> Result<gix::Repository, Refused> {
    gix::open(repo).map_err(|e| Refused::Failed(format!("opening {}: {e}", repo.display())))
}

/// Each served ref, by full name, with the object it names and what that peels to, `HEAD` first as git lists it.
fn served_refs(repo: &gix::Repository) -> Result<Vec<Served>, Refused> {
    let failed = |e: &dyn std::fmt::Display| Refused::Failed(format!("listing the repository's refs: {e}"));
    let mut refs = Vec::new();
    let head = repo.find_reference("HEAD").ok();
    for reference in head.into_iter().map(Ok).chain(
        repo.references()
            .map_err(|e| failed(&e))?
            .all()
            .map_err(|e| failed(&e))?,
    ) {
        let mut reference = reference.map_err(|e| failed(&e))?;
        let name = reference.name().as_bstr().to_string();
        if !served(&name) {
            continue;
        }
        let symref = match reference.target() {
            gix::refs::TargetRef::Symbolic(target) => Some(target.as_bstr().to_string()),
            gix::refs::TargetRef::Object(_) => None,
        };
        // A branch HEAD names before its first commit points nowhere yet, and is not listed.
        let Ok(id) = reference.peel_to_id() else {
            continue;
        };
        let id = id.detach();
        let object = match &symref {
            Some(_) => id,
            None => reference.target().try_id().map(ToOwned::to_owned).unwrap_or(id),
        };
        refs.push(Served {
            name,
            object,
            peeled: (object != id).then_some(id),
            symref,
        });
    }
    Ok(refs)
}

struct Served {
    name: String,
    /// What the ref names: a commit, or an annotated tag.
    object: gix::ObjectId,
    /// What a tag peels to.
    peeled: Option<gix::ObjectId>,
    /// What `HEAD` names.
    symref: Option<String>,
}

/// How long a ref listing is answered from memory before the repository is read again.
///
/// Every worker asks on every heartbeat, and a fetch that changes nothing is *only* this request -- the client reads
/// the listing, sees it already holds that commit, and stops. The answer is the same for every worker until a ref
/// moves, so a window shorter than the heartbeat makes the cost of a fleet asking independent of its size, and the
/// leader forgets it at once where it moves a ref itself ([`forget_listing`]).
const LISTING_TTL: Duration = Duration::from_secs(1);

/// The last listing. A leader serves one repository, but the path is held so the answer for one can never be given
/// for another.
static LISTING: Mutex<Option<Listing>> = Mutex::new(None);

struct Listing {
    repo: PathBuf,
    arguments: Vec<String>,
    made: Instant,
    body: Vec<u8>,
}

/// Drop the remembered listing, so the next asker is answered from the repository. Called where the leader moves a
/// ref itself; [`LISTING_TTL`] covers every other way one moves.
pub(crate) fn forget_listing() {
    *LISTING.lock() = None;
}

/// `ls-refs`, remembered for [`LISTING_TTL`].
fn ls_refs(repo: &Path, arguments: &[String]) -> Result<Vec<u8>, Refused> {
    if let Some(last) = LISTING.lock().as_ref()
        && last.repo == repo
        && last.arguments == arguments
        && last.made.elapsed() < LISTING_TTL
    {
        return Ok(last.body.clone());
    }
    let body = listing(repo, arguments)?;
    *LISTING.lock() = Some(Listing {
        repo: repo.to_path_buf(),
        arguments: arguments.to_vec(),
        made: Instant::now(),
        body: body.clone(),
    });
    Ok(body)
}

/// The served refs whose names start with one of the `ref-prefix` arguments, or every one without any.
fn listing(repo: &Path, arguments: &[String]) -> Result<Vec<u8>, Refused> {
    let (mut symrefs, mut peel, mut prefixes) = (false, false, Vec::new());
    for argument in arguments {
        match argument.split_once(' ') {
            _ if argument == "symrefs" => symrefs = true,
            _ if argument == "peel" => peel = true,
            Some(("ref-prefix", prefix)) => prefixes.push(prefix),
            _ => {
                return Err(Refused::Asked(format!(
                    "{argument} is not an ls-refs argument the leader takes"
                )));
            }
        }
    }
    let mut body = Vec::new();
    for served in served_refs(&open(repo)?)? {
        if !prefixes.is_empty() && !prefixes.iter().any(|p| served.name.starts_with(p)) {
            continue;
        }
        let mut line = format!("{} {}", served.object, served.name);
        if let (true, Some(target)) = (symrefs, &served.symref) {
            line.push_str(&format!(" symref-target:{target}"));
        }
        if let (true, Some(peeled)) = (peel, served.peeled) {
            line.push_str(&format!(" peeled:{peeled}"));
        }
        line.push('\n');
        body.extend(pkt_line::line(line.as_bytes()));
    }
    body.extend_from_slice(b"0000");
    Ok(body)
}

/// `fetch`: acknowledge the `have`s the leader holds, then send a pack of what the `want`s reach and they do not --
/// at once when the client says it is `done`, or when one of its haves is held, which is base enough.
fn fetch(repo_path: &Path, arguments: &[String]) -> Result<Vec<u8>, Refused> {
    let asked = |why: String| Refused::Asked(why);
    let (mut wants, mut haves, mut done) = (Vec::new(), Vec::new(), false);
    for argument in arguments {
        let id = |hex: &str| {
            gix::ObjectId::from_hex(hex.as_bytes()).map_err(|_| asked(format!("{hex} is not an object id")))
        };
        match argument.split_once(' ') {
            Some(("want", hex)) => wants.push(id(hex)?),
            Some(("have", hex)) => haves.push(id(hex)?),
            _ if argument == "done" => done = true,
            // A pack is never thin and carries no progress, and the repository holds no annotated tags to include,
            // so these ask for nothing that changes it. Its deltas name their bases by offset, as `ofs-delta` allows.
            _ if ["thin-pack", "no-progress", "include-tag", "ofs-delta"].contains(&argument.as_str()) => {}
            _ => return Err(asked(format!("{argument} is not a fetch argument the leader takes"))),
        }
    }
    if wants.is_empty() {
        return Err(asked("a fetch that wants nothing".to_string()));
    }
    let repo = open(repo_path)?;
    let failed = |e: &dyn std::fmt::Display| Refused::Failed(format!("answering a fetch: {e}"));

    // A worker is handed nothing a served ref does not reach: the leader's bookkeeping stays its own.
    let tips: Vec<gix::ObjectId> = served_refs(&repo)?
        .into_iter()
        .map(|r| r.peeled.unwrap_or(r.object))
        .collect();
    if wants.iter().any(|want| !tips.contains(want)) {
        let reached: HashSet<gix::ObjectId> = repo
            .rev_walk(tips)
            .all()
            .map_err(|e| failed(&e))?
            .map(|info| info.map(|info| info.id))
            .collect::<Result<_, _>>()
            .map_err(|e| failed(&e))?;
        if let Some(want) = wants.iter().find(|want| !reached.contains(*want)) {
            return Err(asked(format!("{want} is not a commit the leader serves")));
        }
    }

    let common: Vec<gix::ObjectId> = haves
        .into_iter()
        .filter(|id| repo.find_object(*id).is_ok_and(|o| o.kind == gix::object::Kind::Commit))
        .collect();
    let mut body = Vec::new();
    if !done {
        body.extend(pkt_line::line(b"acknowledgments\n"));
        if common.is_empty() {
            body.extend(pkt_line::line(b"NAK\n"));
            body.extend_from_slice(b"0000");
            return Ok(body);
        }
        for id in &common {
            body.extend(pkt_line::line(format!("ACK {id}\n").as_bytes()));
        }
        body.extend(pkt_line::line(b"ready\n"));
        body.extend_from_slice(b"0001");
    }
    let wants: Vec<String> = wants.iter().map(ToString::to_string).collect();
    let common: Vec<String> = common.iter().map(ToString::to_string).collect();
    let pack = git::pack_reaching(
        repo_path,
        &wants.iter().map(String::as_str).collect::<Vec<_>>(),
        &common.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .map_err(|e| Refused::Failed(e.message))?;
    body.extend(pkt_line::line(b"packfile\n"));
    for chunk in pack.chunks(BAND_PAYLOAD) {
        body.extend(pkt_line::line(&[&[1u8][..], chunk].concat()));
    }
    body.extend_from_slice(b"0000");
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fuzz::{Coverage, Rng, run};
    use crate::leader::store::Actor;
    use std::path::PathBuf;

    const V2: Option<&str> = Some("version=2");

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-upload-pack-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A leader's data directory whose repository holds `main`, deployed.
    fn leader(name: &str) -> PathBuf {
        let data = temp_dir(name);
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let main = commit(&repo, git::MAIN_BRANCH, "groups/g/system.json", b"{\"version\":1}");
        git::deploy_commit(&data, &main, &Actor::system()).unwrap();
        data
    }

    /// Commit `bytes` at `path` on `branch`, from `main` the first time, and answer the commit.
    fn commit(repo: &Path, branch: &str, path: &str, bytes: &[u8]) -> String {
        let tip = git::branch_tip(repo, branch);
        let parent = tip.clone().or_else(|| git::main_head(repo)).unwrap();
        let edit = git::FileEdit {
            path: path.to_string(),
            content: Some(bytes.to_vec()),
        };
        match git::commit_edits(repo, branch, &parent, tip.as_deref(), &[edit], "save", &Actor::system()).unwrap() {
            git::Committed::At(commit) => commit,
            _ => parent,
        }
    }

    /// A command request: `command`, then each of `arguments`.
    fn request_of(command: &str, arguments: &[String]) -> Vec<u8> {
        let mut body = pkt_line::line(format!("command={command}\n").as_bytes());
        body.extend(pkt_line::line(b"agent=git/2.52.0\n"));
        body.extend_from_slice(b"0001");
        for argument in arguments {
            body.extend(pkt_line::line(format!("{argument}\n").as_bytes()));
        }
        body.extend_from_slice(b"0000");
        body
    }

    fn post(repo: &Path, command: &str, arguments: &[String]) -> Answer {
        serve(repo, "git-upload-pack", "POST", "", V2, &request_of(command, arguments)).unwrap()
    }

    /// The refs `ls-refs` lists with no prefix, by name.
    fn listed(repo: &Path) -> Vec<String> {
        let answer = post(repo, "ls-refs", &["symrefs".to_string()]);
        String::from_utf8_lossy(&answer.body)
            .lines()
            .filter_map(|line| line.get(4..)?.split(' ').nth(1).map(str::to_string))
            .collect()
    }

    #[test]
    fn the_server_offers_protocol_v2_and_answers_nothing_older() {
        let data = leader("offers");
        let repo = git::repo_path(&data);
        let offered = serve(&repo, "info/refs", "GET", "service=git-upload-pack", V2, &[]).unwrap();
        assert_eq!(offered.status, 200);
        assert_eq!(offered.content_type, "application/x-git-upload-pack-advertisement");
        let text = String::from_utf8_lossy(&offered.body);
        assert!(text.starts_with("000eversion 2\n"), "{text}");
        assert!(text.contains("ls-refs\n") && text.contains("fetch\n"), "{text}");
        for protocol in [None, Some("version=1")] {
            let refused = serve(&repo, "info/refs", "GET", "service=git-upload-pack", protocol, &[]).unwrap();
            assert_eq!(refused.status, 400, "{protocol:?}");
        }
    }

    /// Read-only is a property of the server, not of which routes happen to exist.
    #[test]
    fn a_push_is_refused_by_the_server_itself() {
        let data = leader("no-push");
        let repo = git::repo_path(&data);
        let asked = serve(&repo, "info/refs", "GET", "service=git-receive-pack", V2, &[]).unwrap();
        assert_eq!(asked.status, 403);
        let pushed = serve(&repo, "git-receive-pack", "POST", "", V2, b"0000").unwrap();
        assert_eq!(pushed.status, 403);
    }

    /// The leader hands every path under `/api/worker/repo/` to this and keeps no list of its own, so this is the
    /// whole guard on that route. A path a worker picks never reads a file.
    #[test]
    fn a_path_a_worker_picks_never_reads_a_file() {
        let data = leader("paths");
        let repo = git::repo_path(&data);
        std::fs::write(data.join("leader.db"), "not-for-any-worker").unwrap();
        for path in ["config", "HEAD", "objects/info/packs", "../../leader.db", "info/refs"] {
            let answer = serve(&repo, path, "GET", "", V2, &[]).unwrap();
            assert_eq!(answer.status, 404, "{path} was served");
        }
    }

    /// Workers are listed the pointer and the branches, and nothing of the leader's bookkeeping beside them.
    #[test]
    fn the_deployed_pointer_is_listed_and_the_leader_s_bookkeeping_is_not() {
        let data = leader("listed");
        let repo = git::repo_path(&data);
        commit(&repo, "al/fix", "groups/g/system.json", b"{\"version\":2}");
        git::open_change(&repo, "al/fix", "Fix", "", &Actor::system()).unwrap();
        let listed = listed(&repo);
        assert!(listed.contains(&git::DEPLOYED_REF.to_string()), "{listed:?}");
        assert!(listed.contains(&"refs/heads/al/fix".to_string()), "{listed:?}");
        assert_eq!(listed.first().map(String::as_str), Some("HEAD"), "{listed:?}");
        assert!(
            listed.iter().all(|name| served(name)),
            "a hidden ref was listed: {listed:?}"
        );
        let prefixed = post(&repo, "ls-refs", &[format!("ref-prefix {}", git::DEPLOYED_REF)]);
        let text = String::from_utf8_lossy(&prefixed.body);
        let deployed = git::deployed(&repo).unwrap();
        assert_eq!(
            text,
            format!(
                "{:04x}{deployed} {}\n0000",
                4 + 41 + git::DEPLOYED_REF.len() + 1,
                git::DEPLOYED_REF
            )
        );
    }

    /// A deploy is a ref moving. The listing's window must not hold a worker back from one the leader made itself.
    #[test]
    fn moving_a_ref_is_listed_at_once() {
        let data = leader("listed-moved");
        let repo = git::repo_path(&data);
        let asked = [format!("ref-prefix {}", git::DEPLOYED_REF)];
        let before = post(&repo, "ls-refs", &asked).body;
        let main = commit(&repo, git::MAIN_BRANCH, "groups/g/system.json", b"{\"version\":3}");
        git::deploy_commit(&data, &main, &Actor::system()).unwrap();
        let after = post(&repo, "ls-refs", &asked).body;
        assert_ne!(after, before);
        assert!(String::from_utf8_lossy(&after).contains(&main));
    }

    /// One leader serves one repository, but a remembered listing for one must never be given for another.
    #[test]
    fn a_listing_is_not_reused_across_repositories() {
        let (one, two) = (leader("listed-one"), leader("listed-two"));
        // Built the same way in the same second, two repositories hold the same commits: give one its own.
        let main = commit(&git::repo_path(&two), git::MAIN_BRANCH, "groups/g/system.json", b"{}");
        git::deploy_commit(&two, &main, &Actor::system()).unwrap();
        let asked = [format!("ref-prefix {}", git::DEPLOYED_REF)];
        let first = post(&git::repo_path(&one), "ls-refs", &asked).body;
        let second = post(&git::repo_path(&two), "ls-refs", &asked).body;
        assert_ne!(first, second);
    }

    /// A client none of whose haves the leader holds is told so, and asked for more before any pack.
    #[test]
    fn a_fetch_with_nothing_in_common_is_answered_nak() {
        let data = leader("nak");
        let repo = git::repo_path(&data);
        let want = format!("want {}", git::deployed(&repo).unwrap());
        let answer = post(&repo, "fetch", &[want, format!("have {}", "1".repeat(40))]);
        let expected = [
            pkt_line::line(b"acknowledgments\n"),
            pkt_line::line(b"NAK\n"),
            b"0000".to_vec(),
        ]
        .concat();
        assert_eq!(
            String::from_utf8_lossy(&answer.body),
            String::from_utf8_lossy(&expected)
        );
    }

    /// A leader on loopback answering `/api/worker/repo/` as the leader's route does, and its URL.
    fn serving(repo: PathBuf) -> String {
        use axum::routing::any;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().route(
            "/api/worker/repo/*path",
            any(
                move |method: axum::http::Method,
                      axum::extract::Path(path): axum::extract::Path<String>,
                      axum::extract::RawQuery(query): axum::extract::RawQuery,
                      headers: axum::http::HeaderMap,
                      body: axum::body::Bytes| async move {
                    let protocol = headers.get("git-protocol").and_then(|v| v.to_str().ok());
                    let answer = serve(
                        &repo,
                        &path,
                        method.as_str(),
                        &query.unwrap_or_default(),
                        protocol,
                        &body,
                    )
                    .unwrap();
                    axum::http::Response::builder()
                        .status(answer.status)
                        .header("content-type", answer.content_type)
                        .body(axum::body::Body::from(answer.body))
                        .unwrap()
                },
            ),
        );
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                axum::serve(listener, app).await.unwrap();
            })
        });
        base
    }

    fn roots() -> &'static Path {
        static ROOTS: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        ROOTS.get_or_init(|| {
            let path = crate::leader::remote::roots_path(&temp_dir("roots"));
            crate::io::trust::write_pem_file(crate::io::trust::public(), &path).unwrap();
            path
        })
    }

    /// git, as a client of the leader: protocol v2, no prompts, no config of this host's.
    fn git(args: &[&str]) -> std::process::Output {
        std::process::Command::new("git")
            .args(["-c", "protocol.version=2"])
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap()
    }

    /// Fetches of what the fleet runs, by a worker (gix) and by git, while the leader moves: saves, deploys,
    /// rollbacks, changes opened. Each fetch lands what is deployed, and at the end both clones hold every object
    /// it needs. Asking for what no served ref reaches is refused, and a request garbled any way is answered, never
    /// failed on.
    #[test]
    fn upload_pack_fuzz() {
        let mut coverage = Coverage::new([
            "save",
            "save-large",
            "deploy",
            "rollback",
            "open-change",
            "worker-clone",
            "worker-fetch-moved",
            "worker-fetch-unmoved",
            "git-clone",
            "git-fetch",
            "want-hidden-refused",
            "mutant-answered",
        ]);
        run(0..10, |seed| {
            let mut rng = Rng::seeded(seed);
            let data = leader(&format!("fuzz-{seed}"));
            let repo = git::repo_path(&data);
            let base = serving(repo.clone());
            let url = format!("{base}/api/worker/repo");
            let (worker, cli) = (data.join("worker"), data.join("cli.git"));
            let mut ran = vec![git::deployed(&repo).unwrap()];
            let mut worker_at = None;
            for _ in 0..20 {
                let branch = *rng.pick(&["al/a", "bo/b"]);
                match rng.below(8) {
                    0 => {
                        let large = rng.chance(4);
                        coverage.reach(if large { "save-large" } else { "save" });
                        let bytes: Vec<u8> = match large {
                            true => (0..128 << 10).map(|_| rng.next_u64() as u8).collect(),
                            false => format!("{{\"n\":{}}}", rng.next_u64()).into_bytes(),
                        };
                        let path = format!("groups/g{}/pipelines/p{}.json", rng.below(3), rng.below(3));
                        commit(&repo, branch, &path, &bytes);
                    }
                    1 | 2 => {
                        coverage.reach("deploy");
                        let path = format!("groups/g{}/system.json", rng.below(3));
                        let main = commit(
                            &repo,
                            git::MAIN_BRANCH,
                            &path,
                            format!("{{\"n\":{}}}", rng.next_u64()).as_bytes(),
                        );
                        if git::deploy_commit(&data, &main, &Actor::system()).is_ok() {
                            ran.push(main);
                        }
                    }
                    3 => {
                        coverage.reach("rollback");
                        let back = rng.pick(&ran).clone();
                        let _ = git::deploy_commit(&data, &back, &Actor::system());
                    }
                    4 => {
                        if git::branch_tip(&repo, branch).is_some() {
                            coverage.reach("open-change");
                            let _ = git::open_change(&repo, branch, "A change", "", &Actor::system());
                        }
                    }
                    5 | 6 => {
                        let deployed = git::deployed(&repo).unwrap();
                        coverage.reach(match &worker_at {
                            None => "worker-clone",
                            Some(at) if *at == deployed => "worker-fetch-unmoved",
                            Some(_) => "worker-fetch-moved",
                        });
                        let fetched = crate::worker::fetch_deployed(&worker, &base, "token", roots())?;
                        if fetched != deployed {
                            return Err(format!("the worker fetched {fetched}, not the deployed {deployed}"));
                        }
                        worker_at = Some(fetched);
                    }
                    _ => {
                        let deployed = git::deployed(&repo).unwrap();
                        let out = match cli.exists() {
                            false => {
                                coverage.reach("git-clone");
                                git(&["clone", "--quiet", "--bare", &url, cli.to_str().unwrap()])
                            }
                            true => {
                                coverage.reach("git-fetch");
                                let spec = format!("+{}:refs/fetched", git::DEPLOYED_REF);
                                git(&["--git-dir", cli.to_str().unwrap(), "fetch", "--quiet", &url, &spec])
                            }
                        };
                        if !out.status.success() {
                            return Err(format!("git: {}", String::from_utf8_lossy(&out.stderr)));
                        }
                        let spec = format!("+{}:refs/fetched", git::DEPLOYED_REF);
                        let out = git(&["--git-dir", cli.to_str().unwrap(), "fetch", "--quiet", &url, &spec]);
                        let at = git(&["--git-dir", cli.to_str().unwrap(), "rev-parse", "refs/fetched"]);
                        if !out.status.success() || String::from_utf8_lossy(&at.stdout).trim() != deployed {
                            return Err(format!("git fetched {:?}, not the deployed {deployed}", at.stdout));
                        }
                    }
                }

                // What only the leader's bookkeeping reaches is not listed or handed out: workers are served the
                // branches, HEAD and the deployed pointer, and nothing else.
                let handle = gix::open(&repo).unwrap();
                let mut tips = Vec::new();
                let mut hidden_refs = Vec::new();
                for reference in handle.references().unwrap().all().unwrap() {
                    let reference = reference.unwrap();
                    let name = reference.name().as_bstr().to_string();
                    let id = reference.into_fully_peeled_id().unwrap().detach();
                    match name.starts_with("refs/heads/") || name == git::DEPLOYED_REF {
                        true => tips.push(id),
                        false => hidden_refs.push((name, id)),
                    }
                }
                let listed = listed(&repo);
                if let Some((name, _)) = hidden_refs.iter().find(|(name, _)| listed.contains(name)) {
                    return Err(format!("{name} was listed"));
                }
                let reached: HashSet<gix::ObjectId> = handle
                    .rev_walk(tips)
                    .all()
                    .unwrap()
                    .map(|info| info.unwrap().id)
                    .collect();
                let hidden = hidden_refs.iter().map(|(_, id)| *id).find(|id| !reached.contains(id));
                if let Some(hidden) = hidden {
                    coverage.reach("want-hidden-refused");
                    let answer = post(&repo, "fetch", &[format!("want {hidden}"), "done".to_string()]);
                    if !String::from_utf8_lossy(&answer.body).contains("ERR ") {
                        return Err(format!("{hidden}, which no served ref reaches, was handed out"));
                    }
                }

                // A request garbled any way is answered: a refusal, or what it still asks for.
                let mut garbled = request_of(
                    "fetch",
                    &[
                        format!("want {}", git::deployed(&repo).unwrap()),
                        format!("have {}", rng.pick(&ran)),
                        "done".to_string(),
                    ],
                );
                let at = rng.below(garbled.len());
                match rng.below(3) {
                    0 => garbled.truncate(at),
                    1 => garbled[at] = rng.next_u64() as u8,
                    _ => {
                        let junk: Vec<u8> = (0..1 + rng.below(6)).map(|_| rng.next_u64() as u8).collect();
                        garbled.splice(at..at, junk);
                    }
                }
                serve(&repo, "git-upload-pack", "POST", "", V2, &garbled)
                    .map_err(|e| format!("a garbled request failed the leader: {e}"))?;
                coverage.reach("mutant-answered");
            }
            for clone in [&worker, &cli] {
                if clone.exists() {
                    let fsck = git(&[
                        "--git-dir",
                        clone.to_str().unwrap(),
                        "fsck",
                        "--strict",
                        "--no-dangling",
                    ]);
                    if !fsck.status.success() {
                        return Err(format!(
                            "{} is not whole: {}",
                            clone.display(),
                            String::from_utf8_lossy(&fsck.stderr)
                        ));
                    }
                }
            }
            Ok(())
        });
        coverage.assert_complete();
    }
}
