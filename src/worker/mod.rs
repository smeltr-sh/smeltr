//! The worker process (design doc, Control plane): enrolls with a leader, heartbeats, and runs the bundle the leader
//! deploys.
//!
//! A deployment is one reader thread per source, [`WorkerConfig::thread_limit`] transform threads draining every
//! reader's lane, and one egress thread per destination. Each stage's size comes from its own limit: a source's shards
//! are divided across workers by the source's own protocol, transform is sized by cores, egress by what a destination
//! accepts.
//!
//! A worker runs nothing until the leader has answered it, after every start: the local git clone is a cache that
//! buys speed, not independence. Once running, it keeps running whether the leader is unreachable or rejects its
//! credential, unless `stop_after_unreachable_secs` says otherwise or an operator asks it to shut down through its
//! leader (`Supervisor::follow_directive`). The exception is a credential the leader no longer knows: a worker holding an enrollment code
//! enrolls again and restarts (`Supervisor::enroll_again`).
//!
//! Every thread in one process belongs to the one group named in `WorkerConfig::group_id`, which is why the
//! leader's answer is one policy for the process rather than one directive per thread.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub mod capture;
pub mod child;
pub mod held;
mod memory;
pub mod metrics_endpoint;

use crate::config_file::{self, LoadedSystem};
use crate::core::buffer::MemoryBudget;
use crate::core::id::SourceId;
use crate::egress::Egress;
use crate::handoff::{Acks, Ready};
use crate::heartbeat::{self, HeartbeatClient, HeartbeatRequest, PartitionRef, ThreadHeartbeat, ThreadKind};
use crate::metrics::{Metrics, MetricsHandle};
use crate::runtime::{DestinationTarget, Lane, Reader, Transform};

/// Everything a worker needs before it can talk to a leader. Plain fields; the binary reads them from the environment.
pub struct WorkerConfig {
    pub leader_url: String,
    /// The certificate authority the leader's own certificate must chain to, copied from the leader's `ca.pem`.
    /// `None` for a leader running plain HTTP (`SMELTR_LEADER_TLS` false) or one behind a proxy holding a
    /// publicly issued certificate.
    pub leader_ca_pem: Option<Vec<u8>>,
    /// Whether the public roots verify the leader too, beside `leader_ca_pem` (`heartbeat::leader_roots`).
    pub leader_trust_public: bool,
    /// Where the leader keeps what it serves TLS with, for a worker running inside that leader's own process.
    ///
    /// Such a leader can replace its certificate without restarting, and a client built once from bytes handed
    /// over at startup would go on verifying against an authority nothing presents any more. Set only when this
    /// worker's leader is this process: anywhere else the directory is not ours to read.
    pub leader_authority_dir: Option<PathBuf>,
    pub group_id: String,
    pub thread_limit: usize,
    /// The bytes this worker sizes each deployment to (`memory::fit`). `None` gives every unset part its default.
    pub memory: Option<usize>,
    /// Identity, credential and git clone. Must survive a restart.
    pub state_dir: PathBuf,
    /// The enrollment code the operator gave this worker, as they pasted it. Spent for a credential on the first run,
    /// and again for a new identity where the leader no longer knows this worker's credential. Read for nothing else
    /// once `state_dir` holds one: an enrollment code left in a unit file after it was spent is no instruction.
    pub enrollment_code: Option<String>,
    pub heartbeat_interval: Duration,
    /// How long this worker may go without a successful heartbeat before it stops its deployment. `None` means
    /// never. Known at startup, unlike a directive from the leader -- which is what lets this worker enforce it
    /// before its first successful heartbeat, the exact situation the setting exists for.
    pub stop_after_unreachable_secs: Option<u64>,
    /// Whether this worker runs inside its leader's own process, where exiting would end the leader too. Such a
    /// worker restarts when asked and is never asked to shut down.
    pub in_leader: bool,
    /// Where this worker serves its metrics for Prometheus, bound already, or `None` to serve them nowhere.
    pub metrics_endpoint: Option<metrics_endpoint::MetricsEndpoint>,
}

/// When a worker holding `pem` can verify no leader at all: the last of its roots to expire.
///
/// A rotation leaves a worker holding the outgoing root beside the incoming one, and reporting the outgoing date
/// would warn about the very step that resolves it.
/// The roots in `pem`, by fingerprint: which authorities this worker would accept.
///
/// A leader rotating its own authority cannot sign under the new one until the fleet holds it, and an expiry date
/// does not say which root a worker has. This does.
fn fingerprints(pem: &[u8]) -> Vec<String> {
    match crate::leader::tls::describe_each(pem, "the leader's authority") {
        Ok(described) => described.into_iter().map(|d| d.fingerprint).collect(),
        Err(_) => Vec::new(),
    }
}

fn expires(pem: &[u8]) -> Option<i64> {
    match crate::leader::tls::describe_each(pem, "the leader's authority") {
        Ok(described) => described.iter().map(|d| d.not_after_millis).max(),
        Err(e) => {
            tracing::warn!(
                target: "worker.authority_unreadable",
                error = %e,
                "could not read when this worker's copy of the leader's authority expires"
            );
            None
        }
    }
}

// -- bootstrap: identity + enrollment ---------------------------------------

fn credential_path(state_dir: &Path) -> PathBuf {
    state_dir.join("credential.token")
}
fn repo_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("repo")
}

/// Stable across restarts and distinct across a fleet; not a secret.
fn generate_worker_id() -> String {
    let hostname = host_name().unwrap_or_else(|| "worker".to_string());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{hostname}-{}-{nanos:x}", std::process::id())
}

/// The name this host goes by, which a sender reaches a receiver under when it listens on every address. `None` where
/// the host will not say.
fn host_name() -> Option<String> {
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        // SAFETY: `buf` is valid for its whole length, which is what `gethostname` is told it may write.
        if unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } != 0 {
            return None;
        }
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        let name = String::from_utf8_lossy(&buf[..end]).into_owned();
        (!name.is_empty()).then_some(name)
    }
    #[cfg(not(unix))]
    {
        std::env::var("COMPUTERNAME").ok().filter(|name| !name.is_empty())
    }
}

fn load_or_create_worker_id(state_dir: &Path) -> String {
    let path = state_dir.join("worker_id");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let trimmed = existing.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    let id = generate_worker_id();
    if let Err(e) = crate::leader::private_file::replace(&path, id.as_bytes()) {
        tracing::warn!(
            target: "worker.id_not_written",
            path = %path.display(),
            error = %e,
            "could not write this worker's id, so it will mint a new one next start and enroll again"
        );
    }
    id
}

/// Whether this worker still needs an enrollment code: true only before its first successful enrollment. The binary
/// refuses to start with a message naming the setting rather than reaching the `expect` in [`run`].
pub fn needs_an_enrollment_code(state_dir: &Path) -> bool {
    load_credential(state_dir).is_none()
}

fn load_credential(state_dir: &Path) -> Option<String> {
    std::fs::read_to_string(credential_path(state_dir))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The id a worker enrolled again under, in place of the one its leader forgot.
fn save_worker_id(state_dir: &Path, worker_id: &str) -> Result<(), String> {
    let path = state_dir.join("worker_id");
    crate::leader::private_file::replace(&path, worker_id.as_bytes()).map_err(|e| format!("{}: {e}", path.display()))
}

/// The other half of what a spent enrollment code bought. Durable, and loud when it is not: the enrollment code is gone
/// either way, so a token that did not reach the disk is a worker that can never enroll and never says why.
fn save_credential(state_dir: &Path, token: &str) -> Result<(), String> {
    let path = credential_path(state_dir);
    crate::leader::private_file::replace(&path, token.as_bytes()).map_err(|e| format!("{}: {e}", path.display()))
}

#[derive(serde::Deserialize)]
pub(crate) struct EnrollResponse {
    ok: bool,
    error: Option<String>,
    /// Whether sending the same enrollment code again could ever succeed. A leader too old to say defaults to yes,
    /// which is how this worker behaved before it was asked.
    #[serde(default = "yes")]
    retry: bool,
    token: Option<String>,
}

fn yes() -> bool {
    true
}

/// Where a worker keeps what an enrollment code pinned. Durable, because an enrollment code is spent once and the trust
/// it established has to outlive it.
///
/// Separate from the copy written for git: that one is whatever authority is in force, including one an operator
/// set with `SMELTR_LEADER_CA`. Writing both to one path would make unsetting that setting keep its bytes forever
/// as though an enrollment code had pinned them.
fn pinned_path(state_dir: &Path) -> PathBuf {
    state_dir.join("pinned-ca.pem")
}

/// Where what git's fetch trusts of the leader is written ([`write_git_roots`]), since git wants a path rather than
/// bytes.
fn git_roots_path(state_dir: &Path) -> PathBuf {
    state_dir.join("leader-ca.pem")
}

/// Write to `path` what git's fetch trusts of the leader: [`heartbeat::leader_roots`], as the heartbeat trusts.
fn write_git_roots(path: &Path, leader_ca_pem: Option<&[u8]>, trust_public: bool) -> Result<(), String> {
    crate::io::trust::write_pem_file(&heartbeat::leader_roots(leader_ca_pem, trust_public)?, path)
}

/// Why a worker got no answer it could use, from the two exchanges it makes before it holds anything: fetching the
/// authority its enrollment code names, and spending the enrollment code.
enum Unresolved {
    /// Nothing reached the leader, or the leader said it could not answer yet. At a first boot that is a startup
    /// race rather than a misconfiguration, and waiting is what resolves it.
    Unreachable(String),
    /// The leader answered, and will answer the same way for as long as it is asked. Somebody has to change
    /// something: the address, the enrollment code, or the worker id.
    Refused(String),
}

/// Which side of [`Unresolved`] a failed request falls on.
///
/// A status code is an answer, so the address reaches something that is running. Only the handful that mean "not
/// yet" are worth waiting on; the rest name the address or the route, which waiting does not fix.
fn unresolved(e: &ureq::Error, leader_url: &str) -> Unresolved {
    if let Some(refused) = heartbeat::certificate_refused(e) {
        return Unresolved::Unreachable(format!(
            "enrolling failed: {}",
            heartbeat::untrusted_certificate(leader_url, refused)
        ));
    }
    match e {
        ureq::Error::StatusCode(408 | 429 | 500 | 502 | 503 | 504) => {
            Unresolved::Unreachable(format!("{leader_url} is not answering yet: {e}"))
        }
        ureq::Error::StatusCode(code) => Unresolved::Refused(format!(
            "{leader_url} answered {code}. A leader answers a worker at /api/authority and /api/enroll, neither \
             of them gated -- check the address, and anything routing to it."
        )),
        // The connection was made and the certificate was not accepted, which is a different thing from a leader
        // that is not there: an operator told "could not reach" goes looking at the network rather than at the
        // authority this worker holds. Waited out rather than fatal, because a leader mid-certificate-replacement
        // answers this way for a moment.
        ureq::Error::Tls(_) | ureq::Error::Rustls(_) | ureq::Error::Pem(_) | ureq::Error::TlsRequired => {
            Unresolved::Unreachable(format!(
                "{leader_url} presented a certificate this worker will not accept: {e}. Check that the authority it \
             holds is the one that leader serves under, and that the address names a name the certificate carries."
            ))
        }
        _ => Unresolved::Unreachable(format!("could not reach {leader_url}: {e}")),
    }
}

impl Unresolved {
    fn message(&self) -> &str {
        match self {
            Unresolved::Unreachable(e) | Unresolved::Refused(e) => e,
        }
    }
}

/// What this worker will verify its leader with, in the order the answers are authoritative.
///
/// An operator who manages the file themselves outranks everything: `SMELTR_LEADER_CA` is how an existing PKI is
/// pointed at, and it is never quietly replaced. Failing that, what an enrollment code pinned on the first run is kept
/// and reused, so the unverified fetch happens once in a worker's life rather than at every start. A worker with
/// neither verifies with the platform's own roots, which is what a leader behind a proxy holding a publicly issued
/// certificate needs.
fn resolve_authority(config: &WorkerConfig) -> Result<Option<Vec<u8>>, Unresolved> {
    let resolved = resolve_authority_from(config)?;
    // An authority is only ever used against a certificate, and only `https` presents one. Holding one and addressing
    // the leader over plain HTTP is a worker that verifies nothing while its operator believes it does, and sends the
    // enrollment code secret it is about to spend, and every heartbeat after it, where anyone can read them.
    if resolved.is_some() && !verifiable(&config.leader_url) {
        return Err(unverifiable(&config.leader_url));
    }
    Ok(resolved)
}

/// Whether a leader at `leader_url` presents a certificate for this worker to verify.
fn verifiable(leader_url: &str) -> bool {
    leader_url.trim_start().to_ascii_lowercase().starts_with("https://")
}

fn unverifiable(leader_url: &str) -> Unresolved {
    Unresolved::Refused(format!(
        "this worker has a certificate authority to verify its leader with, and {leader_url} presents no certificate \
         to verify: the enrollment code secret and this worker's credential would go out in clear text, and the \
         authority would never be used. Address that leader over https, or, if it really does serve plain HTTP, start \
         this worker with neither an authority nor an enrollment code that pins one."
    ))
}

fn resolve_authority_from(config: &WorkerConfig) -> Result<Option<Vec<u8>>, Unresolved> {
    if let Some(configured) = &config.leader_ca_pem {
        return Ok(Some(configured.clone()));
    }

    // Only an enrollment code about to be spent says anything here. One an enrolled worker still carries may name a
    // root its leader has since rotated away from, and what the worker holds is the later word.
    let named = config
        .enrollment_code
        .as_deref()
        .filter(|_| needs_an_enrollment_code(&config.state_dir))
        .map(str::parse::<crate::enrollment_code::EnrollmentCode>)
        .transpose()
        .map_err(Unresolved::Refused)?
        .map(|code| code.authority);

    let path = pinned_path(&config.state_dir);
    if path.exists() {
        let kept = std::fs::read(&path).map_err(|e| Unresolved::Refused(format!("{}: {e}", path.display())))?;
        // An enrollment code names the leader this worker is meant to enroll with, so where the two disagree the
        // enrollment code is the later intent: a worker pointed at a different leader, keeping its state directory,
        // would otherwise pin an authority nobody meant it to keep and fail the handshake rather than say why. An
        // enrollment code naming the platform's own roots disagrees with any kept file, which is a private authority by
        // construction.
        let still_meant = match &named {
            None => true,
            Some(crate::enrollment_code::Authority::Pinned(fingerprint)) => holds(&kept, fingerprint),
            Some(crate::enrollment_code::Authority::System) => false,
        };
        if still_meant {
            return Ok(Some(kept));
        }
        // Removed, not merely passed over. The enrollment code is spent on this run and gone from the next one, so a
        // file left here would be accepted again at the next start -- by which time nothing remains to say it was
        // superseded, and every heartbeat would go out under the authority of a leader this worker has left.
        std::fs::remove_file(&path).map_err(|e| Unresolved::Refused(format!("{}: {e}", path.display())))?;
        tracing::info!(
            target: "worker.authority_replaced",
            path = %path.display(),
            "this enrollment code names a different certificate authority than the one kept here, and is the newer word"
        );
    }

    let Some(crate::enrollment_code::Authority::Pinned(fingerprint)) = named else {
        return Ok(None);
    };
    // Before the fetch rather than after it: that request is the unverified one, and over plain HTTP it is the
    // whole of what an attacker in the way needs to be asked for an authority.
    if !verifiable(&config.leader_url) {
        return Err(unverifiable(&config.leader_url));
    }
    let pem = fetch_pinned_authority(&config.leader_url, &fingerprint)?;
    // Kept before it is used, so the unverified fetch happens once in this worker's life however the rest of this start
    // goes -- and durably, because the enrollment code about to be spent is spent for good. A credential surviving a
    // power loss that this file did not is a worker that comes back holding a token, discards the enrollment code it no
    // longer needs, and falls back to the platform's roots against a leader they will never verify.
    crate::leader::private_file::replace(&pinned_path(&config.state_dir), &pem)
        .map_err(|e| Unresolved::Refused(format!("keep the authority this enrollment code names: {e}")))?;
    tracing::info!(
        target: "worker.authority_pinned",
        fingerprint = %fingerprint,
        "took the certificate authority this enrollment code names, and will accept no other"
    );
    Ok(Some(pem))
}

/// [`resolve_authority`], waiting out a leader that is not up yet.
///
/// A first-boot worker races its leader, which is what the enrollment loop below already allows for; fetching the
/// authority is now the first request and needs the same patience. A refusal that waiting cannot change is still fatal,
/// so an enrollment code naming an authority this leader does not serve stops the worker rather than retrying forever.
fn resolve_authority_waiting(config: &WorkerConfig) -> Option<Vec<u8>> {
    let mut backoff = ENROLL_RETRY_MIN;
    loop {
        match resolve_authority(config) {
            Ok(pem) => return pem,
            Err(refused @ Unresolved::Refused(_)) => {
                tracing::error!(
                    target: "worker.authority_refused",
                    error = %refused.message(),
                    "this worker cannot verify its leader"
                );
                crate::logs::stop();
                std::process::exit(1);
            }
            Err(unreachable) => {
                tracing::warn!(
                    target: "worker.authority_unreachable",
                    error = %unreachable.message(),
                    retry_in = ?backoff,
                    "could not fetch the authority this enrollment code names, and will try again"
                );
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(ENROLL_RETRY_MAX);
            }
        }
    }
}

/// Whether `pem` holds the certificate `fingerprint` names.
fn holds(pem: &[u8], fingerprint: &str) -> bool {
    crate::leader::tls::describe_each(pem, "the authority kept here")
        .is_ok_and(|certs| certs.iter().any(|cert| cert.fingerprint == fingerprint))
}

/// The one certificate an enrollment code pins to, fetched from the leader that minted it.
///
/// The fetch itself is unverified, and has to be: a worker holding only a fingerprint has nothing yet to verify
/// with. What the fingerprint buys is that the answer is checked before anything is done with it -- an attacker in
/// the way can serve whatever they like and the worker will refuse all of it.
///
/// Exactly the matching certificate is kept, never the file it arrived in. A response carrying the real authority
/// followed by one of the attacker's own would otherwise match on the first and leave the worker trusting both.
fn fetch_pinned_authority(leader_url: &str, fingerprint: &str) -> Result<Vec<u8>, Unresolved> {
    let agent = heartbeat::agent_without_verification(crate::heartbeat::REQUEST_BUDGET);
    let mut response = agent
        .get(format!("{}/api/authority", leader_url.trim_end_matches('/')))
        .call()
        .map_err(|e| unresolved(&e, leader_url))?;
    let pem = response
        .body_mut()
        .read_to_vec()
        .map_err(|e| Unresolved::Unreachable(format!("the connection to {leader_url} did not last the answer: {e}")))?;

    let text = String::from_utf8(pem)
        .map_err(|_| Unresolved::Refused(format!("{leader_url} served an authority that is not text")))?;
    match pinned_block(&text, fingerprint) {
        Some(block) => Ok(block.into_bytes()),
        None => Err(Unresolved::Refused(format!(
            "{leader_url} did not offer the certificate authority this enrollment code names. Either it is not the \
             leader the enrollment code was minted on, or its authority has been replaced since -- mint an enrollment \
             code on the leader you mean."
        ))),
    }
}

/// The one certificate in `text` whose fingerprint is `fingerprint`, alone and as it was served.
fn pinned_block(text: &str, fingerprint: &str) -> Option<String> {
    // Block by block over the text, so what is kept is the certificate as it was served rather than one
    // re-encoded from what was parsed out of it.
    certificate_blocks(text).into_iter().find(|block| {
        ureq::tls::parse_pem(block.as_bytes()).any(|item| {
            matches!(item, Ok(ureq::tls::PemItem::Certificate(ref cert))
                if crate::leader::tls::fingerprint(cert.der()) == fingerprint)
        })
    })
}

const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
const END: &str = "-----END CERTIFICATE-----";

/// Each certificate in a PEM file, as its own text.
///
/// The line ending after `END` is whatever the source used -- `rcgen` writes CRLF, per RFC 7468 -- and is kept
/// rather than replaced, since a re-encoded ending is a byte a caller comparing this against the file it came
/// from should never see move. A block with nothing after `END` at all (a file truncated mid-write) still ends
/// in a newline, since what is returned is parsed as its own PEM file next.
fn certificate_blocks(pem: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut rest = pem;
    while let Some(start) = rest.find(BEGIN) {
        let after = &rest[start..];
        let Some(end) = after.find(END) else { break };
        let after_end = end + END.len();
        let line_ending_len = match &after.as_bytes()[after_end..] {
            [b'\r', b'\n', ..] => 2,
            [b'\n', ..] => 1,
            _ => 0,
        };
        let block_end = after_end + line_ending_len;
        let block = if line_ending_len == 0 {
            format!("{}\n", &after[..block_end])
        } else {
            after[..block_end].to_string()
        };
        blocks.push(block);
        rest = &after[block_end..];
    }
    blocks
}

/// `POST /api/enroll`: an enrollment code for a persistent Bearer token. Enrollment happens before a `HeartbeatClient`
/// exists, so it builds its own agent over the same trust and the same budget.
fn enroll(
    leader_url: &str,
    code: &str,
    worker_id: &str,
    leader_ca_pem: Option<&[u8]>,
    trust_public: bool,
) -> Result<String, Unresolved> {
    let agent = heartbeat::agent_trusting(leader_ca_pem, trust_public, crate::heartbeat::REQUEST_BUDGET)
        .map_err(Unresolved::Refused)?;
    let mut response = agent
        .post(format!("{}/api/enroll", leader_url.trim_end_matches('/')))
        .send_json(serde_json::json!({ "code": code, "workerId": worker_id }))
        .map_err(|e| unresolved(&e, leader_url))?;
    let parsed: EnrollResponse = response.body_mut().read_json().map_err(|e| {
        Unresolved::Unreachable(format!(
            "{leader_url} answered something this worker could not read as an enrollment: {e}. A leader answers \
                 /api/enroll with JSON, so anything else is something else at that address."
        ))
    })?;
    if parsed.ok {
        return parsed
            .token
            .ok_or_else(|| Unresolved::Refused("leader returned ok without a token".to_string()));
    }
    let said = parsed.error.unwrap_or_else(|| "enrollment failed".to_string());
    // The leader says which of its refusals a worker can wait out. An expired or spent enrollment code, or an id that
    // has enrolled before, is refused the same way however long it waits: only somebody changing something moves it.
    if parsed.retry {
        Err(Unresolved::Unreachable(said))
    } else {
        Err(Unresolved::Refused(said))
    }
}

// -- git: fetch a group's deployed bundle -----------------------------------
//
// In-process, through `gix`, the way the leader's own reads and writes already are (`leader/git.rs`): nothing here
// spawns git. Only the leader's serving half still does (`git http-backend`), so this is still a real git client
// talking to a real git server.

/// How long a fetch against the leader may take before it counts as stalled. A fetch that changes nothing is
/// answered from the leader's cached ref advertisement, so this bounds a stalled exchange rather than ordinary
/// work. There is no process to kill here, so the budget is spent as curl's own stall detection: a transfer stuck
/// at zero bytes/second for this long is aborted from inside the request rather than by a wall clock this code
/// would otherwise have to enforce itself.
const GIT_BUDGET: Duration = Duration::from_secs(120);

/// A first clone copies the leader's whole repository, so it is given longer than the fetches that follow it.
const GIT_CLONE_BUDGET: Duration = Duration::from_secs(600);

/// Transport options for one request to the leader's git server: the worker's bearer token as a header, and the
/// roots the leader is verified by (`git_roots`, [`write_git_roots`]), the same two things `-c http.extraHeader` and `-c http.sslCAInfo` carried for the
/// git binary. Nothing else reaches the leader without one; the `Authorization` header is git's own convention for
/// `http.extraHeader`, and the leader's smart-HTTP route on `leader/mod.rs` reads it the same way of a real client.
fn git_http_options(
    token: &str,
    git_roots: &Path,
    budget: Duration,
) -> gix::protocol::transport::client::blocking_io::http::Options {
    gix::protocol::transport::client::blocking_io::http::Options {
        extra_headers: [
            format!("Authorization: Bearer {token}"),
            format!("{}: {}", crate::leader::BUILD_HEADER, crate::version::commit()),
        ]
        .into_iter()
        .chain(crate::version::RELEASE.map(|release| format!("{}: {release}", crate::leader::RELEASE_HEADER)))
        .collect(),
        ssl_ca_info: Some(git_roots.to_path_buf()),
        connect_timeout: Some(budget),
        low_speed_limit_bytes_per_second: 1,
        // 0 is curl's own "disabled" -- never what a caller here means by a budget, so a sub-second one still
        // aborts within one second rather than never.
        low_speed_time_seconds: budget.as_secs().max(1),
        ..Default::default()
    }
}

/// Force `name` to point at `commit`, creating it if it does not exist. This worker is the only writer of its own
/// `refs/smeltr/*`, so there is nothing to compare against first -- only a write.
fn set_ref(repo: &gix::Repository, name: &str, commit: &str) -> Result<(), String> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};
    let id = repo
        .rev_parse_single(commit)
        .map_err(|e| format!("{commit:?} does not name an object: {e}"))?;
    repo.edit_reference(RefEdit {
        change: Change::Update {
            log: LogChange::default(),
            expected: PreviousValue::Any,
            new: gix::refs::Target::Object(id.detach()),
        },
        name: name
            .try_into()
            .map_err(|e| format!("{name:?} is not a ref name: {e}"))?,
        deref: false,
    })
    .map_err(|e| format!("{name} did not move: {e}"))?;
    Ok(())
}

/// Fetch what the fleet runs (`leader::git::DEPLOYED_REF`) into `repo_dir`, cloning first if needed, and answer the
/// commit it names. Asking is one request: an unchanged pointer is answered from the ref advertisement alone, with no
/// pack transferred.
///
/// The clone is the leader's whole repository. The cluster is the security boundary, so an enrolled worker is trusted
/// with all of it (design doc, Security).
fn fetch_deployed(repo_dir: &Path, leader_url: &str, token: &str, git_roots: &Path) -> Result<String, String> {
    let budget = if is_bare_clone(repo_dir) {
        GIT_BUDGET
    } else {
        GIT_CLONE_BUDGET
    };
    fetch_deployed_with_budget(repo_dir, leader_url, token, git_roots, budget)
}

/// What a failed fetch of what the fleet runs says. curl's refusal of the leader's certificate says what the
/// worker trusts; anything else is gix's own account.
fn fetch_failed(e: &gix::Error, leader_url: &str) -> String {
    match e.downcast_any_ref::<curl::Error>() {
        Some(curl) if curl.is_peer_failed_verification() => format!(
            "fetching what the fleet runs failed: {}",
            heartbeat::untrusted_certificate(leader_url, curl)
        ),
        _ => format!("fetching what the fleet runs from {leader_url}: {e}"),
    }
}

/// [`fetch_deployed`], with the stall budget exposed rather than chosen from [`GIT_BUDGET`]/[`GIT_CLONE_BUDGET`] --
/// what a test shortens to keep a stalled-fetch test fast.
fn fetch_deployed_with_budget(
    repo_dir: &Path,
    leader_url: &str,
    token: &str,
    git_roots: &Path,
    budget: Duration,
) -> Result<String, String> {
    use crate::leader::git::DEPLOYED_REF;

    if !is_bare_clone(repo_dir) {
        // Anything here is a cache this worker cannot use: a working tree left by a version that checked commits
        // out, or a clone that never finished. Neither is repairable and both are only a cache, so the answer to
        // either is the fresh repository fetched into below.
        if repo_dir.exists() {
            std::fs::remove_dir_all(repo_dir).map_err(|e| format!("clearing {}: {e}", repo_dir.display()))?;
        }
        gix::init_bare(repo_dir).map_err(|e| format!("creating {}: {e}", repo_dir.display()))?;
    }
    let repo = gix::open(repo_dir).map_err(|e| format!("opening {}: {e}", repo_dir.display()))?;

    // The url is built fresh on every call rather than kept in the repository's own remote config, so where the
    // leader keeps its repository is never something a worker remembers wrongly.
    let url = format!("{}/repo", leader_url.trim_end_matches('/'));
    // Straight into `FETCHED_REF`: `refs/smeltr/` is local on a worker, and what it fetched is all it needs to name.
    let refspec = format!("+{DEPLOYED_REF}:{FETCHED_REF}");
    let remote = repo
        .remote_at(url.as_str())
        .map_err(|e| format!("{url}: {e}"))?
        .with_refspecs([refspec.as_str()], gix::remote::Direction::Fetch)
        .map_err(|e| format!("{url}: {e}"))?;

    let mut connection = remote
        .connect(gix::remote::Direction::Fetch)
        .map_err(|e| format!("connecting to {leader_url}: {e}"))?;
    connection.set_transport_options(Box::new(git_http_options(token, git_roots, budget)));
    // The bearer token above is this worker's only credential, so a 401 is not answered by trying harder: without
    // this, gix's default is the same cascade `git credential` runs -- helpers from whatever gitconfig this host
    // has, ending in a terminal prompt -- and nothing here reads a worker's console. `GIT_TERMINAL_PROMPT=0` was
    // the same refusal for the git binary; offering nothing is refusing every stage of the cascade, not only the
    // prompt at its end.
    // `gix_credentials::protocol::Error` is gix's own type, sized for a helper subprocess's failure modes rather
    // than this closure's, which has none.
    #[allow(clippy::result_large_err)]
    connection.set_credentials(|_action| Ok(None));
    connection
        .prepare_fetch(gix::progress::Discard, Default::default())
        .map_err(|e| fetch_failed(&e, leader_url))?
        .receive(gix::progress::Discard, &AtomicBool::new(false))
        .map_err(|e| fetch_failed(&e, leader_url))?;

    let head = repo
        .find_reference(FETCHED_REF)
        .map_err(|e| format!("{leader_url} did not advertise {DEPLOYED_REF}: {e}"))?
        .into_fully_peeled_id()
        .map_err(|e| format!("reading {FETCHED_REF}: {e}"))?
        .to_string();
    Ok(head)
}

/// Where the last fetch landed, held so the objects behind it stay reachable once this worker's next gc runs.
const FETCHED_REF: &str = "refs/smeltr/fetched";

/// The commit the running deployment was built from, held for as long as it runs. It may be older than what was
/// fetched, and once a newer commit is fetched nothing else names it.
const RUNNING_REF: &str = "refs/smeltr/running";

/// A repository this worker made. Bare, so there is no working tree for two tasks to contend over and no checkout
/// for one of them to leave half-written.
fn is_bare_clone(repo_dir: &Path) -> bool {
    repo_dir.join("HEAD").is_file() && repo_dir.join("objects").is_dir() && !repo_dir.join(".git").exists()
}

/// Whether everything `FETCHED_REF` names reads back as what its id says: the commit, and every tree and blob under
/// it. A fetch answers from the objects a clone says it has and sends none of them again, so one that no longer
/// reads would stay that way. A clone that has fetched nothing yet names nothing to read.
fn fetched_reads(repo_dir: &Path) -> Result<(), String> {
    let repo = gix::open(repo_dir).map_err(|e| format!("opening {}: {e}", repo_dir.display()))?;
    let Some(reference) = repo
        .try_find_reference(FETCHED_REF)
        .map_err(|e| format!("{FETCHED_REF} does not read: {e}"))?
    else {
        return Ok(());
    };
    let commit = reference
        .into_fully_peeled_id()
        .map_err(|e| format!("{FETCHED_REF} does not read: {e}"))?
        .detach();
    let read = |id: gix::ObjectId| -> Result<gix::Object<'_>, String> {
        let object = repo.find_object(id).map_err(|e| format!("{id} does not read: {e}"))?;
        // A loose object's bytes are not checked against its id as they are read.
        let hash = gix::objs::compute_hash(id.kind(), object.kind, &object.data)
            .map_err(|e| format!("{id} does not hash: {e}"))?;
        if hash != id {
            return Err(format!("{id} reads back as {hash}"));
        }
        Ok(object)
    };
    let tree = read(commit)?
        .try_into_commit()
        .map_err(|e| format!("{FETCHED_REF} does not name a commit: {e}"))?
        .tree_id()
        .map_err(|e| format!("commit {commit} does not read: {e}"))?
        .detach();
    let mut seen = std::collections::HashSet::new();
    let mut trees = vec![tree];
    while let Some(id) = trees.pop() {
        if !seen.insert(id) {
            continue;
        }
        let tree = read(id)?
            .try_into_tree()
            .map_err(|e| format!("tree {id} is not a tree: {e}"))?;
        for entry in tree.iter() {
            let entry = entry.map_err(|e| format!("tree {id} does not read: {e}"))?;
            let id = entry.oid().to_owned();
            if entry.mode().is_tree() {
                trees.push(id);
            } else if entry.mode().is_blob() && seen.insert(id) {
                read(id)?;
            }
        }
    }
    Ok(())
}

/// Hold `commit` against garbage collection for as long as the deployment built from it runs.
fn hold_running(repo_dir: &Path, commit: &str) {
    let held = gix::open(repo_dir)
        .map_err(|e| e.to_string())
        .and_then(|repo| set_ref(&repo, RUNNING_REF, commit));
    if let Err(e) = held {
        tracing::warn!(
            target: "worker.running_ref_failed",
            subject = %commit,
            error = %e,
            "could not hold the running commit, so a repack could drop what this deployment was built from"
        );
    }
}

// -- running the deployed bundle ---------------------------------------------

/// How long one poll blocks before the thread checks `stop`.
pub const TICK_TIMEOUT: Duration = Duration::from_millis(500);
/// Rows in one batch, whichever of this and [`TICK_MAX_BYTES`] is reached first. A source hands over what is ready
/// rather than wait to fill a batch, so this binds only under load.
pub(crate) const TICK_MAX_ROWS: usize = 4000;

/// Raw bytes in one batch, whatever the row size.
pub const TICK_MAX_BYTES: usize = 4 * 1024 * 1024;

/// The ceiling on columnar memory every reader in this process shares, unless `SMELTR_MEMORY_BUDGET_BYTES` says
/// otherwise. It bounds a batch's own processing, which no queue cap sees (design doc, Runtime stages); exceeding it
/// splits the work, so too low costs wasted attempts, never records. `smeltr_memory_peak_bytes`
/// says how much was needed.
pub(crate) const MEMORY_BUDGET_BYTES: usize = 1024 * 1024 * 1024; //1 GB = 1024^3

/// The one ceiling every reader in this process shares. What is protected is the container's memory, so a per-thread
/// budget would be a number multiplied by a thread count `deploy` chooses.
pub fn process_memory_budget() -> MemoryBudget {
    static BUDGET: std::sync::OnceLock<MemoryBudget> = std::sync::OnceLock::new();
    BUDGET
        .get_or_init(|| MemoryBudget::new(crate::startup_config::memory_budget_bytes()))
        .clone()
}

/// The bytes a worker sizes its deployments to: `worker.runtime.memory` when set, else its container's memory limit.
/// `None` where neither is known, since a host's whole memory is shared with whatever else runs there, and for a worker
/// inside a leader without the setting, since the container's limit is shared with a leader of no fixed size.
pub fn memory_to_fit(in_leader: bool) -> Option<usize> {
    crate::startup_config::worker_memory_bytes().or_else(|| {
        (!in_leader)
            .then(crate::resources::container_memory_limit_bytes)
            .flatten()
            .map(|limit| limit as usize)
    })
}

/// One source's reader thread.
struct ReaderHandle {
    thread_id: String,
    /// Updated by the reader thread, which owns the source; read by the heartbeat.
    partitions: Arc<Mutex<Vec<PartitionRef>>>,
    /// Why this reader's source is blocked, while it is. Published the same way as `partitions`: the reader owns the
    /// source, so it is the only thing that can say.
    held: Arc<Mutex<Option<crate::heartbeat::MetricSampleDto>>>,
    /// This reader's source's most recent connectivity error, while it has one (E4). Published the same way as
    /// `held`, for the same reason: only the reader thread has a `&Source` to ask.
    connection_error: Arc<Mutex<Option<crate::heartbeat::MetricSampleDto>>>,
    stop: Arc<Stop>,
    thread: JoinHandle<()>,
}

/// One thread of the transform pool. It holds no source, so it owns no partitions.
struct PoolHandle {
    thread_id: String,
    stop: Arc<Stop>,
    thread: JoinHandle<()>,
}

/// A deployment thread's stop request, carrying the instant a reader's drain must end by.
#[derive(Default)]
struct Stop(std::sync::OnceLock<Instant>);

impl Stop {
    fn raise(&self, drain_by: Instant) {
        let _ = self.0.set(drain_by);
    }

    /// `None` until raised.
    fn drain_by(&self) -> Option<Instant> {
        self.0.get().copied()
    }
}

struct Deployment {
    /// One per source.
    readers: Vec<ReaderHandle>,
    /// `thread_limit` threads over every reader's lane.
    pool: Vec<PoolHandle>,
    /// One per destination, shared by every transform thread. Dropped last, so nothing is dispatching when a client is
    /// closed.
    egress: Vec<Egress>,
    /// The most recent refusals per destination, shared by every thread that dispatches. What the heartbeat reports
    /// so the leader can name how a destination worded its refusal.
    refusals: Arc<crate::runtime::RefusalRing>,
    /// The author's own names for what this deployment numbers by position. What tells a capture it can be served
    /// by tapping what is already flowing rather than opening a second consumer, and what a heartbeat's and a
    /// metrics source's subject labels resolve against. Every destination the bundle declares, not only the ones
    /// this deployment delivers to: a label naming an unreached one still resolves.
    subjects: Arc<crate::metrics::Subjects>,
    /// What the transform threads see of this deployment's streams (design doc, Observability), drained into each
    /// report.
    streams: Arc<crate::streams::Streams>,
    /// That this deployment's memory ceilings add up to more than the worker's limit, logged again while it runs
    /// ([`Deployment::relog_memory_warning`]). `None` when they fit.
    memory_warning: Option<memory::Overcommit>,
    /// The pools sized to the worker's memory, the destination queues' and the receivers', and where what they have
    /// lent is reported ([`Deployment::note_pools`]).
    pools: [Option<Arc<crate::egress::QueuePool>>; 2],
    registry: Arc<Metrics>,
}

/// One reader, as the heartbeat sees it: an identity and the locks the reader thread publishes through.
struct ReaderFacts {
    thread_id: String,
    partitions: Arc<Mutex<Vec<PartitionRef>>>,
    held: Arc<Mutex<Option<crate::heartbeat::MetricSampleDto>>>,
    connection_error: Arc<Mutex<Option<crate::heartbeat::MetricSampleDto>>>,
}

/// What the heartbeat reports about a running deployment, shared with it rather than borrowed from it.
///
/// The deployment belongs to the thread that starts and stops it, and stopping joins threads that may never
/// return. A heartbeat holding a reference to it would be waiting on that join, which is the one thing this
/// separation exists to prevent. Every live fact here is behind the lock its own writer already published
/// through, so this view stays current without the heartbeat reaching the deployment at all.
struct DeployedFacts {
    readers: Vec<ReaderFacts>,
    pool_thread_ids: Vec<String>,
    egress_ids: Vec<crate::core::id::DestId>,
    refusals: Arc<crate::runtime::RefusalRing>,
    subjects: Arc<crate::metrics::Subjects>,
    /// What the transform threads see of the deployment's streams, drained into each report.
    streams: Arc<crate::streams::Streams>,
}

impl Deployment {
    /// Logs the memory warning again when it is due, for the runtime's report loop to call each pass.
    fn relog_memory_warning(&self) {
        if let Some(over) = &self.memory_warning {
            over.relog_if_due(crate::resources::memory_limit_bytes);
        }
    }

    /// Reports what each pool has lent and what the process's allocations hold, for the runtime's report loop to call
    /// each pass. A pool lent to its limit is memory, not a destination or a sender, setting the pace.
    fn note_pools(&self) {
        let ([destinations, receivers], registry) = (&self.pools, &self.registry);
        if let Some(pool) = destinations {
            registry_gauge(registry, "smeltr_destination_pool_bytes", pool.lent() as f64);
        }
        if let Some(pool) = receivers {
            registry_gauge(registry, "smeltr_receiver_pool_bytes", pool.lent() as f64);
        }
        let allocated = crate::core::buffer::allocated_bytes();
        registry_gauge(registry, "smeltr_memory_allocated_bytes", allocated as f64);
    }

    /// Published once, when the deployment starts. Nothing here is copied per heartbeat.
    fn facts(&self) -> Arc<DeployedFacts> {
        Arc::new(DeployedFacts {
            readers: self
                .readers
                .iter()
                .map(|r| ReaderFacts {
                    thread_id: r.thread_id.clone(),
                    partitions: r.partitions.clone(),
                    held: r.held.clone(),
                    connection_error: r.connection_error.clone(),
                })
                .collect(),
            pool_thread_ids: self.pool.iter().map(|t| t.thread_id.clone()).collect(),
            egress_ids: self.egress.iter().map(|e| e.id()).collect(),
            refusals: self.refusals.clone(),
            subjects: self.subjects.clone(),
            streams: self.streams.clone(),
        })
    }
}

/// Signals stop, waits for the buffers to drain, commit to the source, and the threads to shut down.
fn stop_deployment(mut deployment: Deployment) {
    let drain_by = Instant::now() + DRAIN_TIMEOUT;
    for r in &deployment.readers {
        r.stop.raise(drain_by);
    }
    // Ingest first: it stops polling and stays alive to commit what the pool still lands.
    for r in std::mem::take(&mut deployment.readers) {
        let _ = r.thread.join();
    }
    for t in &deployment.pool {
        t.stop.raise(drain_by);
    }
    for t in std::mem::take(&mut deployment.pool) {
        let _ = t.thread.join();
    }
    // An egress thread that stopped first would fail the dispatches of a thread still winding down.
    deployment.egress.clear();
}

/// Backoff bounds for first-boot enrollment. The common failure is a leader seconds from ready (the all-in-one
/// deployment spawns this worker while still binding its listener).
const ENROLL_RETRY_MIN: Duration = Duration::from_millis(250);
const ENROLL_RETRY_MAX: Duration = Duration::from_secs(10);

/// How long shutdown gives the readers to commit what they already handed over. What is not committed inside it is
/// redelivered.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Backoff bounds between retries of a failing turn, so an outage settles into a slow retry rather than a spin.
const TICK_RETRY_MIN: Duration = Duration::from_millis(250);
const TICK_RETRY_MAX: Duration = Duration::from_secs(15);

/// Ingest: poll the source, hand batches over, commit what came back. Owns the source, so it is the only thing that
/// commits, holds, or reports which partitions this thread has.
#[allow(clippy::too_many_arguments)]
fn run_reader_loop(
    mut reader: Reader,
    lane: Arc<Lane>,
    acks: Acks,
    stop: Arc<Stop>,
    partitions: Arc<Mutex<Vec<PartitionRef>>>,
    held: Arc<Mutex<Option<crate::heartbeat::MetricSampleDto>>>,
    connection_error: Arc<Mutex<Option<crate::heartbeat::MetricSampleDto>>>,
    batch_bytes: usize,
) {
    let mut backoff = TICK_RETRY_MIN;
    // Whether this idle stretch has handed back the reader's free pages: what it polled is freed by other threads into
    // pages only its own collect hands back.
    let mut released = false;
    while stop.drain_by().is_none() {
        // `Polled::Full` has already waited its timeout on the queue or the acknowledgments, so it needs no sleep
        // here.
        match reader.step(&lane.batches, &acks, TICK_MAX_ROWS, batch_bytes, TICK_TIMEOUT) {
            Ok(crate::runtime::Polled::Idle) => {
                backoff = TICK_RETRY_MIN;
                if !released {
                    released = crate::core::buffer::release_if_due();
                }
            }
            Ok(_) => {
                backoff = TICK_RETRY_MIN;
                released = false;
            }
            Err(_) => {
                std::thread::sleep(backoff.min(TICK_RETRY_MAX));
                backoff = (backoff * 2).min(TICK_RETRY_MAX);
            }
        }
        let current = reader
            .source
            .assigned_partitions()
            .into_iter()
            .map(|(topic, partition)| PartitionRef { topic, partition })
            .collect();
        *partitions.lock().unwrap() = current;
        *held.lock().unwrap() = reader.blocked_fact();
        *connection_error.lock().unwrap() = reader.connection_error_fact();
    }

    // Batches already handed over are still being delivered, and their offsets are this thread's to commit.
    let deadline = stop.drain_by().unwrap_or_else(Instant::now);
    while Instant::now() < deadline {
        reader.apply(acks.drain());
        if reader.retire_and_commit().is_err() {
            break;
        }
        if reader.commits.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    reader.report_in_flight();
}

/// One thread of the transform pool: take batches from any lane, run the pipelines, hand the rows to egress, and
/// report what the destinations said to the lane the batch came from.
///
/// Stops at once, with no drain of its own: a deployment stops its readers first, so whatever this thread would
/// land after that is committed by nobody and is redelivered as a duplicate.
fn run_transform_loop(mut transform: Transform, stop: Arc<Stop>) {
    let mut backoff = TICK_RETRY_MIN;
    while stop.drain_by().is_none() {
        match transform.step(TICK_TIMEOUT) {
            Ok(_) => backoff = TICK_RETRY_MIN,
            Err(_) => {
                std::thread::sleep(backoff.min(TICK_RETRY_MAX));
                backoff = (backoff * 2).min(TICK_RETRY_MAX);
            }
        }
    }
}

/// How many batches one lane holds, and so how far ahead its reader may run: one for every transform thread that could
/// be working on this lane at once, plus one of slack so the reader can poll while they do.
///
/// A pool of `threads` behind a queue holding one batch leaves `threads - 1` of them idle whenever there are fewer
/// sources than threads -- which is the common deployment, not the corner. With at least as many sources as threads it
/// comes back to one batch of slack per lane, which is what a single reader and a single transform thread needed.
fn lane_depth(threads: usize, lanes: usize) -> usize {
    1 + threads / lanes.max(1)
}

/// How far ahead of the pool one reader may run: `lane` batches of at most `batch_bytes` each for the transform
/// threads that could be working on it, and `ack` bytes more for the acknowledgment round trip.
struct Depth {
    lane: usize,
    batch_bytes: usize,
    ack: usize,
}

/// Bytes a reader may have outstanding for the acknowledgment round trip: the largest queue among its destinations,
/// as `queue_budget` gives it, so every queue binds before the reader does. The queues are the designed backpressure,
/// each on its own. A destination that acknowledges only when its buffer fills or its interval ends needs its whole
/// queue outstanding to fill it. Bytes, not batches: a source's batches are as small as what arrives within one poll, and such a
/// destination holds many of them.
///
/// Only the destinations the routes deliver to count. The reader also waits on the DLQs, but they carry what the flow
/// refuses, a trickle, so their queues are never the ones that bind, and a DLQ sized small for that trickle would
/// hold every reader to its size. A destination that is both a delivery target and a DLQ counts. No destination is
/// 0, and the reader's batch floor (`runtime::MAX_IN_FLIGHT`) alone binds.
fn reader_depth(
    topology: &crate::plan::topology::CompiledTopology,
    destinations: &[crate::plan::topology::DestinationConfig],
    source: SourceId,
    queue_budget: impl Fn(&crate::plan::topology::DestinationConfig) -> usize,
) -> usize {
    let delivered_to = topology.delivery_destinations_from(source);
    destinations
        .iter()
        .filter(|d| delivered_to.contains(&d.id))
        .map(queue_budget)
        .max()
        .unwrap_or(0)
}

/// What every thread of one deployment shares: where metrics land, the gate the pool waits on, the refusals a
/// destination's most recent wording is kept in, and the captures taking its records.
struct Shared {
    registry: Arc<Metrics>,
    ready: Arc<Ready>,
    refusals: Arc<crate::runtime::RefusalRing>,
    captures: Arc<capture::Captures>,
    subjects: Arc<crate::metrics::Subjects>,
    held: Arc<held::Held>,
    /// What the receivers sized to the worker's memory borrow from above their reserves (`memory::fit`).
    receivers: Option<Arc<crate::egress::QueuePool>>,
    /// The Kafka clients the deployment's Kafka sources share: one for each client configuration.
    kafka: Arc<crate::io::kafka::Clients>,
}

/// A reader thread for `source_id`, and the lane the pool takes its batches from. A single-worker source is read
/// through a [`held::HeldSource`] on the same thread and lane, so it is opened only while the leader holds it here.
fn spawn_reader(
    topology: &crate::plan::topology::CompiledTopology,
    source_id: SourceId,
    source_name: &str,
    kind: &crate::plan::topology::SourceKind,
    shared: &Shared,
    depth: Depth,
    secrets: &crate::plan::topology::HeldSecrets,
) -> Result<(ReaderHandle, Arc<Lane>), String> {
    let source: Box<dyn crate::io::Source> = match kind.single_worker() {
        Some(place) => {
            let key = heartbeat::SourceKey {
                source: source_name.to_string(),
                place,
            };
            let (kind, registry, subjects, secrets, receivers) = (
                kind.clone(),
                shared.registry.clone(),
                shared.subjects.clone(),
                secrets.clone(),
                shared.receivers.clone(),
            );
            Box::new(shared.held.source(
                key,
                Box::new(move || crate::io::build_source(&kind, &registry, &subjects, &secrets, receivers.as_ref())),
            ))
        }
        None => match kind {
            crate::plan::topology::SourceKind::Kafka(config) => Box::new(
                crate::io::kafka::KafkaSource::open(config, secrets, &shared.kafka).map_err(|e| e.to_string())?,
            ),
            _ => crate::io::build_source(
                kind,
                &shared.registry,
                &shared.subjects,
                secrets,
                shared.receivers.as_ref(),
            )
            .map_err(|e| e.to_string())?,
        },
    };
    let destinations: Vec<crate::core::id::DestId> =
        topology.destinations_reachable_from(source_id).into_iter().collect();

    let partitions = Arc::new(Mutex::new(Vec::new()));
    let held = Arc::new(Mutex::new(None));
    let connection_error = Arc::new(Mutex::new(None));
    let stop = Arc::new(Stop::default());
    let thread_id = format!("reader-{source_id}");

    let (reports, acks) = crate::handoff::reports();
    let lane = Arc::new(Lane::new(
        source_id,
        Some(source_name.to_string()),
        topology.meta_for(source_id),
        kind.encoding(),
        destinations.clone(),
        reports,
        shared.ready.clone(),
        depth.lane * depth.batch_bytes,
    ));
    let reader = Reader::new(
        source_id,
        source,
        destinations,
        depth.lane + crate::runtime::MAX_IN_FLIGHT,
        MetricsHandle::new(shared.registry.clone(), &thread_id),
    )
    .with_byte_budget(depth.lane * depth.batch_bytes + depth.ack)
    .with_refusals(shared.refusals.clone())
    .with_headers({
        let read = topology.reads(source_id, "__source.headers");
        let (captures, name) = (shared.captures.clone(), source_name.to_string());
        move || read || captures.watching(&name)
    });
    let handle = std::thread::Builder::new()
        .name(format!("smeltr-{thread_id}"))
        .spawn({
            let (lane, stop, partitions, held, connection_error) = (
                lane.clone(),
                stop.clone(),
                partitions.clone(),
                held.clone(),
                connection_error.clone(),
            );
            let batch_bytes = depth.batch_bytes;
            move || {
                run_reader_loop(
                    reader,
                    lane,
                    acks,
                    stop,
                    partitions,
                    held,
                    connection_error,
                    batch_bytes,
                )
            }
        })
        .map_err(|e| e.to_string())?;

    Ok((
        ReaderHandle {
            thread_id,
            partitions,
            held,
            connection_error,
            stop,
            thread: handle,
        },
        lane,
    ))
}

/// Start every destination, then one reader per source, then the transform pool: `thread_limit` threads draining
/// every reader's lane.
///
/// A deployment starts whole or not at all. A missing destination would have its rows masked off and committed past
/// uncounted; a source whose reader failed to open would be silently unread.
///
/// `topology` arrives compiled because compiling is the one failure the caller must find before it tears the running
/// deployment down -- see [`redeploy`]. It is read-only afterward, and every thread in the pool runs the same plans.
#[allow(clippy::too_many_arguments)]
fn deploy(
    system: &LoadedSystem,
    topology: Arc<crate::plan::topology::CompiledTopology>,
    thread_limit: usize,
    fitted: Option<memory::Fit>,
    captures: &Arc<capture::Captures>,
    secrets: &crate::plan::topology::HeldSecrets,
    registry: &Arc<Metrics>,
    held: &Arc<held::Held>,
) -> Result<Deployment, String> {
    let registry = registry.clone();
    let refusals = Arc::new(crate::runtime::RefusalRing::default());
    // One tallied batch per route per heartbeat: what the leader sees of a stream arrives at the pace it hears
    // everything else.
    let streams = Arc::new(crate::streams::Streams::new(
        topology.routes.len(),
        system.pipeline_ids.clone(),
        crate::startup_config::worker_heartbeat_interval(),
    ));
    let memory = process_memory_budget();
    // A memory refusal is an unwind by design; the default hook would print a panic dump for each one.
    crate::core::buffer::quiet_refusals();

    // One signal across every lane, so a transform thread waits once rather than polling each queue. The egress
    // queues signal it too, when they free room.
    let ready = Arc::new(Ready::new());
    // Egress first, so every transform thread gets a handle onto the same thread. A destination no route delivers to
    // is never connected.
    let mut egress = Vec::new();
    let wanted = delivered(system, &topology);
    // The destinations sized to the worker's memory borrow from one pool above their reserves (`memory::fit`).
    let queue_pool = fitted.map(|f| crate::egress::QueuePool::new(f.pool));
    // And the receivers from one of their own.
    let receiver_pool = fitted.map(|f| crate::egress::QueuePool::new(f.receivers));
    // A destination sized to the worker's memory keeps its default buffer, and so its senders, and its queue borrows
    // up to its ceiling.
    let fitted_destinations = fitted.map(|_| &system.topology.fitted.destinations);
    let queue_budget = |d: &crate::plan::topology::DestinationConfig| match fitted_destinations {
        Some(fitted) if fitted.contains(&d.id) => memory::queue_budget(&d.kind),
        _ => d.kind.queue_bytes(),
    };
    let batch_bytes = fitted.map_or(TICK_MAX_BYTES, |f| f.batch_bytes);
    let borrows = |d: &crate::plan::topology::DestinationConfig| {
        queue_pool
            .as_ref()
            .filter(|_| system.topology.fitted.destinations.contains(&d.id))
    };
    for d in system.topology.destinations.iter().filter(|d| wanted.contains(&d.id)) {
        let handle = MetricsHandle::new(registry.clone(), format!("egress-{}", d.id));
        let client = crate::io::build_destination(&d.kind, d.id, handle.clone(), secrets, topology.records.get(&d.id))
            .map_err(|e| format!("destination {:?} could not be built: {e}", d.name))?;
        let stage = Egress::start(d.id, &d.name, client, d.kind.stall_after(), queue_budget(d), handle)
            .with_pool(ready.clone())
            .with_captures(captures.clone());
        let stage = match borrows(d) {
            Some(pool) => stage.borrowing(pool.clone(), batch_bytes, memory::queue_charge(&d.kind)),
            None => stage,
        };
        // A destination set to block keeps what it refuses and sends it again, since the fix is usually its own.
        egress.push(if d.on_reject == crate::plan::config::Answer::Block {
            stage.holding_refusals()
        } else {
            stage
        });
    }

    registry_gauge(&registry, "smeltr_worker_threads_budget", thread_limit as f64);
    registry_gauge(
        &registry,
        "smeltr_worker_slice_bytes",
        crate::startup_config::slice_bytes() as f64,
    );
    registry_gauge(
        &registry,
        "smeltr_memory_budget_bytes",
        memory.limit().unwrap_or(0) as f64,
    );

    // Steps by `PipelineConfig::id`, not by position in the Vec: `LoadedSystem::step_id_for` and `leader::stats` both
    // look this up by id rather than assume the two agree, and resolving a label through the wrong pipeline's steps
    // would mislabel history with nothing to show for it.
    let subjects = Arc::new(crate::metrics::Subjects {
        sources: system.topology.sources.iter().map(|s| s.name.clone()).collect(),
        destinations: system.topology.destinations.iter().map(|d| d.name.clone()).collect(),
        pipelines: system.pipeline_ids.clone(),
        steps: (0..system.pipeline_ids.len())
            .map(|i| {
                system
                    .pipelines
                    .iter()
                    .find(|p| p.id == i)
                    .map(|p| p.step_ids.clone())
                    .unwrap_or_default()
            })
            .collect(),
    });
    let shared = Shared {
        ready: ready.clone(),
        registry: registry.clone(),
        refusals: refusals.clone(),
        captures: captures.clone(),
        subjects: subjects.clone(),
        held: held.clone(),
        receivers: receiver_pool.clone(),
        kafka: Arc::default(),
    };
    let depth = lane_depth(thread_limit, system.topology.sources.len());
    let mut readers = Vec::new();
    let mut lanes = Vec::new();
    for source_cfg in &system.topology.sources {
        match spawn_reader(
            &topology,
            source_cfg.id,
            &source_cfg.name,
            &source_cfg.kind,
            &shared,
            Depth {
                lane: depth,
                batch_bytes,
                ack: reader_depth(&topology, &system.topology.destinations, source_cfg.id, queue_budget),
            },
            secrets,
        ) {
            Ok((handle, lane)) => {
                readers.push(handle);
                lanes.push(lane);
            }
            Err(e) => {
                stop_deployment(Deployment {
                    readers,
                    pool: Vec::new(),
                    egress,
                    refusals,
                    subjects,
                    streams: streams.clone(),
                    memory_warning: None,
                    pools: [None, None],
                    registry: registry.clone(),
                });
                return Err(format!(
                    "the reader for source {:?} did not start: {e}",
                    source_cfg.name
                ));
            }
        }
    }

    let mut pool = Vec::new();
    // No lanes means nothing authored: a pool over no lanes would spin.
    if !lanes.is_empty() {
        for i in 0..thread_limit {
            let thread_id = format!("transform-{i}");
            let destinations = match pool_targets(system, &wanted, &mut egress) {
                Ok(t) => t,
                Err(e) => {
                    stop_deployment(Deployment {
                        readers,
                        pool,
                        egress,
                        refusals,
                        subjects,
                        streams: streams.clone(),
                        memory_warning: None,
                        pools: [None, None],
                        registry: registry.clone(),
                    });
                    return Err(e);
                }
            };
            let transform = Transform::new(
                lanes.clone(),
                ready.clone(),
                i,
                topology.clone(),
                destinations,
                memory.clone(),
                MetricsHandle::new(registry.clone(), &thread_id),
            )
            .with_captures(captures.clone())
            .with_refusals(refusals.clone())
            .with_streams(streams.clone())
            .with_threads(thread_limit);
            let stop = Arc::new(Stop::default());
            let handle = std::thread::Builder::new().name(format!("smeltr-{thread_id}")).spawn({
                let stop = stop.clone();
                move || run_transform_loop(transform, stop)
            });
            match handle {
                Ok(h) => pool.push(PoolHandle {
                    thread_id,
                    stop,
                    thread: h,
                }),
                Err(e) => {
                    stop_deployment(Deployment {
                        readers,
                        pool,
                        egress,
                        refusals,
                        subjects,
                        streams: streams.clone(),
                        memory_warning: None,
                        pools: [None, None],
                        registry: registry.clone(),
                    });
                    return Err(format!("transform thread {i} did not start: {e}"));
                }
            }
        }
    }

    registry_gauge(&registry, "smeltr_worker_readers", readers.len() as f64);
    registry_gauge(&registry, "smeltr_worker_transform_threads", pool.len() as f64);
    registry_gauge(&registry, "smeltr_worker_egress_threads", egress.len() as f64);

    // A worker is two processes, its supervisor and the runtime running the deployment, as `fit` counts it.
    let mut ceilings: Vec<memory::Part> = ["supervisor", "runtime"]
        .map(|process| memory::Part::worker(format!("{process} process"), process, memory::BASELINE_BYTES))
        .into();
    let mut bounded = Vec::new();
    for s in &system.topology.sources {
        match memory::receiver_reserve(&s.kind).filter(|_| receiver_pool.is_some()) {
            Some(reserve) => ceilings.push(memory::Part::source(s, "reserve", reserve)),
            None => bounded.push(s),
        }
    }
    ceilings.extend(memory::source_parts(bounded));
    ceilings.push(memory::Part::worker(
        "lanes",
        "lanes",
        lanes.len() * depth * batch_bytes,
    ));
    for d in system.topology.destinations.iter().filter(|d| wanted.contains(&d.id)) {
        match borrows(d) {
            Some(_) => {
                ceilings.push(memory::Part::destination(
                    d,
                    "reserve",
                    batch_bytes * memory::queue_charge(&d.kind),
                ));
                let senders = memory::senders_bytes(&d.kind);
                if senders > 0 {
                    ceilings.push(memory::Part::destination(d, "senders", senders));
                }
            }
            None => ceilings.extend(memory::destination_parts(d)),
        }
    }
    if let Some(pool) = queue_pool.as_ref().filter(|p| p.limit() > 0) {
        ceilings.push(memory::Part::worker(
            "destination queue pool",
            "destination pool",
            pool.limit(),
        ));
    }
    if let Some(pool) = receiver_pool.as_ref().filter(|p| p.limit() > 0) {
        ceilings.push(memory::Part::worker("receiver pool", "receiver pool", pool.limit()));
    }
    if let Some(pool) = &queue_pool {
        registry_gauge(&registry, "smeltr_destination_pool_budget_bytes", pool.limit() as f64);
    }
    if let Some(pool) = &receiver_pool {
        registry_gauge(&registry, "smeltr_receiver_pool_budget_bytes", pool.limit() as f64);
    }
    // Only the transform threads spend the columnar budget, and a deployment with no source starts none.
    if let Some(limit) = memory.limit().filter(|_| !pool.is_empty()) {
        ceilings.push(memory::Part::worker("columnar budget", "columnar", limit));
    }
    record_memory_parts(&registry, &ceilings);
    let memory_warning = memory::check(ceilings, crate::resources::memory_limit_bytes());

    Ok(Deployment {
        readers,
        pool,
        egress,
        refusals,
        subjects,
        streams,
        memory_warning,
        pools: [queue_pool, receiver_pool],
        registry,
    })
}

/// Every destination a source of `system` reaches. One no route delivers to is never connected.
fn delivered(
    system: &LoadedSystem,
    topology: &crate::plan::topology::CompiledTopology,
) -> Vec<crate::core::id::DestId> {
    let mut reached: Vec<crate::core::id::DestId> = system
        .topology
        .sources
        .iter()
        .flat_map(|s| topology.destinations_reachable_from(s.id))
        .collect();
    reached.sort_unstable();
    reached.dedup();
    reached
}

/// Every destination this worker delivers to, as handles onto the shared egress threads. One transform thread gets one
/// set: a handle's completions come back to it, and only the thread that handed a batch over knows what it owes.
fn pool_targets(
    system: &LoadedSystem,
    wanted: &[crate::core::id::DestId],
    egress: &mut [Egress],
) -> Result<Vec<DestinationTarget>, String> {
    let mut targets = Vec::new();
    for d in system.topology.destinations.iter().filter(|d| wanted.contains(&d.id)) {
        let Some(e) = egress.iter_mut().find(|e| e.id() == d.id) else {
            return Err(format!("destination {:?} has no egress thread", d.name));
        };
        targets.push(DestinationTarget {
            id: d.id,
            name: d.name.clone(),
            destination: Box::new(e.handle()),
            on_reject: d.on_reject.clone(),
            wants_doc_id: d.kind.wants_doc_id(),
            id_field: d.kind.id_field().map(str::to_string),
            placement: d.kind.placement(),
        });
    }
    Ok(targets)
}

/// The `thread` label the deployment-wide gauges are recorded under. Not a thread; the heartbeat reports the bucket as
/// the worker's own.
const DEPLOY_LABEL: &str = "deploy";

fn registry_gauge(registry: &Arc<Metrics>, name: &'static str, value: f64) {
    MetricsHandle::new(registry.clone(), DEPLOY_LABEL).gauge(name, &[], value);
}

/// Records the most each of `parts` may hold, under [`DEPLOY_LABEL`]. Once, at deploy: a gauge reports its value on
/// every read of the registry (`Metrics::peek_delta`), and a runtime runs one deployment, so its parts never change.
fn record_memory_parts(registry: &Arc<Metrics>, parts: &[memory::Part]) {
    let handle = MetricsHandle::new(registry.clone(), DEPLOY_LABEL);
    for part in parts {
        let owner = match part.owner {
            memory::Owner::Worker => None,
            memory::Owner::Source(id) => Some(("source", id.to_string())),
            memory::Owner::Destination(id) => Some(("destination", id.to_string())),
        };
        let mut labels = vec![("part", part.part)];
        labels.extend(owner.as_ref().map(|(key, id)| (*key, id.as_str())));
        handle.gauge("smeltr_memory_part_bytes", &labels, part.bytes as f64);
    }
}

/// What a deployment can read in the `group` and `shared` folders of a commit: each entry's name and object id, with
/// `samples/` and `tests/` left out. `None` for a folder the commit does not hold.
#[derive(Clone, PartialEq, Eq, Debug)]
struct ContentId {
    group: Option<Vec<(String, gix::ObjectId)>>,
    shared: Option<Vec<(String, gix::ObjectId)>>,
}

/// The entries of the `group` and `shared` subtrees at `commit`, or `None` for one the commit does not hold.
///
/// `samples/` and `tests/` are left out. Only the leader reads them, so a commit that moves nothing else is not a
/// redeploy (design doc, Control plane).
///
/// Absent and unreadable are different answers. A commit that failed to resolve reads as absent, and absent
/// compared equal to a running deployment that had no `shared/` -- so a commit that added one during a passing
/// failure was represented without ever being deployed. A path the commit does not hold answers `None`, and a
/// failure to read the commit at all is the only other case.
fn content_id(repo_dir: &Path, commit: &str, group_id: &str) -> Result<ContentId, String> {
    let repo = gix::open(repo_dir).map_err(|e| format!("opening {}: {e}", repo_dir.display()))?;
    let root = repo
        .rev_parse_single(format!("{commit}^{{tree}}").as_str())
        .map_err(|e| format!("{commit:?} names nothing here: {e}"))?;
    let tree = repo
        .find_tree(root.detach())
        .map_err(|e| format!("reading {commit}: {e}"))?;
    let entries = |path: &str| -> Result<Option<Vec<(String, gix::ObjectId)>>, String> {
        let unreadable = |e: &dyn std::fmt::Display| format!("reading {path:?} of {commit}: {e}");
        let Some(entry) = tree.clone().lookup_entry_by_path(path).map_err(|e| unreadable(&e))? else {
            return Ok(None);
        };
        let object = entry.object().map_err(|e| unreadable(&e))?;
        if object.kind != gix::object::Kind::Tree {
            // Not a folder, so nothing in it to leave out.
            return Ok(Some(vec![(String::new(), entry.object_id())]));
        }
        let folder = object.into_tree();
        let mut kept = Vec::new();
        for child in folder.iter() {
            let child = child.map_err(|e| unreadable(&e))?;
            let name = child.inner.filename.to_string();
            if name != config_file::SAMPLES_DIR && name != config_file::TESTS_DIR {
                kept.push((name, child.inner.oid.to_owned()));
            }
        }
        Ok(Some(kept))
    };
    Ok(ContentId {
        group: match config_file::group_subtree(group_id) {
            Ok(sub) => entries(&sub)?,
            // Not a name any group can have, so not one any commit holds.
            Err(_) => None,
        },
        shared: entries(config_file::SHARED_DIR)?,
    })
}

/// One report per running thread, then one for every other bucket the registry has samples under.
///
/// The deployment-wide gauges are recorded under [`DEPLOY_LABEL`], which names no thread, so they match nothing in
/// `running` -- but the cursor is committed past everything peeked, so a bucket left behind here is lost rather than
/// owed. That bucket reports as the worker's own. Any other leftover carries no kind: a label nothing here knows, which
/// is worth seeing rather than guessing at.
fn thread_reports(
    group_id: &str,
    running: &[(String, ThreadKind, Vec<PartitionRef>)],
    mut by_thread: std::collections::HashMap<String, Vec<crate::heartbeat::MetricSampleDto>>,
) -> Vec<ThreadHeartbeat> {
    let mut out: Vec<ThreadHeartbeat> = running
        .iter()
        .map(|(thread_id, kind, partitions_owned)| ThreadHeartbeat {
            thread_id: thread_id.clone(),
            group_id: group_id.to_string(),
            kind: Some(*kind),
            partitions_owned: partitions_owned.clone(),
            metrics: by_thread.remove(thread_id).unwrap_or_default(),
        })
        .collect();
    // Sorted: the request is compared in tests and read by a human, and a HashMap's order is neither stable nor
    // meaningful.
    let mut rest: Vec<(String, Vec<crate::heartbeat::MetricSampleDto>)> = by_thread.into_iter().collect();
    rest.sort_by(|a, b| a.0.cmp(&b.0));
    out.extend(rest.into_iter().map(|(thread_id, metrics)| ThreadHeartbeat {
        kind: (thread_id == DEPLOY_LABEL).then_some(ThreadKind::Worker),
        thread_id,
        group_id: group_id.to_string(),
        partitions_owned: vec![],
        metrics,
    }));
    out
}

/// Attach facts to the thread that knows them first-hand -- a reader's block, a destination's refusals. The registry
/// never saw these, so the leader can only read a subject from the thread reporting it.
fn attach_owned_facts(
    by_thread: &mut std::collections::HashMap<String, Vec<crate::heartbeat::MetricSampleDto>>,
    owned: impl IntoIterator<Item = (String, Vec<crate::heartbeat::MetricSampleDto>)>,
) {
    for (thread_id, facts) in owned {
        if !facts.is_empty() {
            by_thread.entry(thread_id).or_default().extend(facts);
        }
    }
}

/// Rewrite every `source`/`destination`/`pipeline`/`function` label across `metrics` from its positional id to the
/// author's own name (`Subjects::resolve`). One pass over the assembled report rather than the write sites: a hold or
/// a refusal is labeled the same way a registry sample is (`Reader::blocked_fact`, `RefusalRing::heartbeat_facts`),
/// and this is the one place all of them pass through before the wire.
///
fn resolve_subject_labels(metrics: &mut [crate::heartbeat::MetricSampleDto], subjects: &crate::metrics::Subjects) {
    for sample in metrics {
        subjects.resolve(&mut sample.labels);
    }
}

/// The same grouping [`heartbeat::split_by_thread`] does, for samples that arrived over a pipe rather than from a
/// registry: a runtime's own counters are already DTOs by the time this process sees them.
fn split_dtos_by_thread(
    samples: &[crate::heartbeat::MetricSampleDto],
) -> std::collections::HashMap<String, Vec<crate::heartbeat::MetricSampleDto>> {
    let mut out: std::collections::HashMap<String, Vec<crate::heartbeat::MetricSampleDto>> =
        std::collections::HashMap::new();
    for sample in samples {
        let thread = sample
            .labels
            .iter()
            .find(|(k, _)| k == crate::metrics::THREAD_LABEL)
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        out.entry(thread).or_default().push(sample.clone());
    }
    out
}

/// What a heartbeat says about the worker itself, rather than about the threads it is running.
struct Reporting<'a> {
    worker_id: &'a str,
    group_id: &'a str,
    /// The commit this worker represents, which is the supervisor's answer and not the runtime's.
    commit: Option<&'a str>,
    supervising: Option<heartbeat::Supervising>,
    /// What this process is spending against what it is allowed to run in, sampled fresh for this beat.
    resources: Option<heartbeat::ResourceReport>,
    stop_after_unreachable_secs: Option<u64>,
    /// When the authority this worker verifies its leader with runs out. The copy this worker holds, which is
    /// what its own verification uses, rather than whatever the leader is serving under.
    authority_expires_millis: Option<i64>,
    /// The roots that copy holds. A date says when this worker can verify nothing; this says which authorities it
    /// would accept, which is what a leader rotating its own has to know before it signs under the new one.
    authority_fingerprints: Vec<String>,
    /// Whether the leader may replace that copy. False once an operator has named one with `SMELTR_LEADER_CA`.
    takes_authority: bool,
    /// Whether this is the notice that the worker is stopping. It always carries the report about the worker
    /// itself, since that is all the leader keeps of a worker that has stopped.
    stopping: bool,
    accepts: Vec<heartbeat::DirectiveAction>,
    took: Option<String>,
    /// How often this worker beats, for the leader to hold its sources for long enough.
    heartbeat_interval: Option<Duration>,
}

/// What a beat is about to answer for. The threads named here and what they last said stay in `held`, true until
/// a later report replaces them -- a beat with nothing new still describes them rather than an empty worker.
/// Only the metrics, the logs and the progress are a window; clearing them here keeps the next beat, if nothing has
/// changed, from reporting any twice. The progress is the oldest [`heartbeat::LEDGER_PER_HEARTBEAT`] ledger entries'
/// worth, and the rest waits in `held` for the beats that follow.
fn take_runtime_window(held: &mut child::RuntimeReport) -> child::RuntimeReport {
    let progress = held::take_ledger(
        &mut held.progress,
        heartbeat::LEDGER_PER_HEARTBEAT,
        PROGRESS_BYTES_PER_HEARTBEAT,
    );
    let snapshot = child::RuntimeReport {
        progress,
        ..held.clone()
    };
    held.metrics.clear();
    held.logs.clear();
    held.streams = None;
    snapshot
}

/// How much of a heartbeat's body its progress may take, as JSON: a quarter of what the leader accepts, so a beat
/// carrying it and a full batch of logs stays inside the limit however its entries escape.
const PROGRESS_BYTES_PER_HEARTBEAT: usize = heartbeat::HEARTBEAT_BODY_LIMIT / 4;

/// A window a beat could not deliver, folded back into whatever the runtime has said since `take_runtime_window`
/// took it -- through `followed_by`, so a counter sums both and a gauge or fact keeps the newer, since neither
/// has reached the leader; logs and progress come back oldest first, the order the leader takes them in.
fn untake_runtime_window(held: &mut child::RuntimeReport, unsent: child::RuntimeReport) {
    let since = std::mem::take(held);
    *held = unsent.followed_by(since);
}

/// The stream windows this process has numbered, and the one the leader has not acknowledged. The leader records a
/// window's number once, so the window goes again as it was rather than folded into what came after it: a beat
/// whose answer was lost may have been recorded.
#[derive(Default)]
struct OwedStreams {
    numbered: u64,
    owed: Option<(u64, heartbeat::StreamReport)>,
}

/// What `process`'s log buffer has lost, and what each site has actually said, as counters in its `registry`,
/// labeled with the process so the supervisor's and the runtime's stay two series. Called just before the read that
/// carries them. `logs` keeps the running totals rather than the registry, so it stays underneath it, and each call
/// raises a counter to its total.
///
/// Together they answer the question that decides whether these events ever need folding: one site drowning the
/// rest shows as its own line climbing while `dropped` climbs beside it.
///
/// Written straight to the registry rather than through a `MetricsHandle`, which would stamp a `thread` label and
/// so hand them to `retire_threads_except` to throw away on the next redeploy. The buffer belongs to the process,
/// and so does what it reports about itself. A site is a call site or a library's target, so the label's
/// cardinality is a property of the code rather than of the traffic.
pub(crate) fn record_log_counts(registry: &Metrics, process: &str) {
    let process = ("process", process);
    registry.count_to("smeltr_logs_dropped_total", &[process], crate::logs::dropped());
    registry.count_to("smeltr_logs_unwritable_total", &[process], crate::logs::unwritable());
    registry.count_to("smeltr_logs_undelivered_total", &[process], crate::logs::undelivered());
    registry.count_to("smeltr_logs_unread_total", &[process], crate::logs::unread());
    for (site, written) in crate::logs::written_by_site() {
        registry.count_to(
            "smeltr_logs_written_total",
            &[process, ("site", site.as_str())],
            written,
        );
    }
}

/// What `process` holds in memory, as a gauge in its `registry`, labeled as [`record_log_counts`] labels its own and
/// for the same reason. The runtime's is what its memory parts (`smeltr_memory_part_bytes`) are checked against.
pub(crate) fn record_memory_gauge(registry: &Metrics, process: &str) {
    let bytes = crate::resources::own_memory_bytes();
    registry.gauge("smeltr_process_memory_bytes", &[("process", process)], bytes as f64);
}

/// The request, and the metric samples it carries so the caller can commit the registry's cursor past them once the
/// request has landed, with how many of its logs came from this process and how many from its runtime.
fn build_heartbeat_request(
    about: Reporting<'_>,
    deployed: Option<&child::RuntimeReport>,
    registry: &Arc<Metrics>,
    secrets: &crate::plan::topology::HeldSecrets,
) -> (HeartbeatRequest, Vec<crate::metrics::MetricSample>, usize, usize) {
    let Reporting {
        worker_id,
        group_id,
        commit,
        supervising,
        resources,
        stop_after_unreachable_secs,
        authority_expires_millis,
        authority_fingerprints,
        takes_authority,
        stopping,
        accepts,
        took,
        heartbeat_interval,
    } = about;
    record_log_counts(registry, "supervisor");
    record_memory_gauge(registry, "supervisor");

    // This process's own logs, plus whatever the runtime said about itself. A runtime has no connection to the
    // leader, so its logs reach the Logs screen on this beat or not at all; each carries the boot id of the
    // process that numbered it, so two sequences cannot collide under one key.
    let mut logs = crate::logs::pending_map(heartbeat::LOGS_PER_HEARTBEAT, |event| heartbeat::LogDto::from(event));
    // Only this many are this process's to drop from its own buffer once the leader has them.
    let own_logs = logs.len();
    let mut runtime_logs = 0;
    if let Some(d) = deployed {
        let room = heartbeat::LOGS_PER_HEARTBEAT.saturating_sub(own_logs);
        runtime_logs = d.logs.len().min(room);
        logs.extend(d.logs.iter().take(room).cloned().map(|mut log| {
            log.boot_id.get_or_insert(d.boot_id);
            log
        }));
    }

    // The process's registry, read whether or not anything is deployed: what a capture recorded, and what a
    // deployment that has since stopped left owed, travel either way.
    let peeked = heartbeat::peek_for_heartbeat(registry);
    // Not a thread: a worker with nothing deployed reports so it learns a directive, and counts as zero. It is
    // what a worker says while it has no runtime, including while it still owes the leader what its last one
    // counted -- a report with no threads in it describes a worker running nothing just as an absent one does.
    let bootstrap = || ThreadHeartbeat {
        thread_id: format!("{worker_id}-bootstrap"),
        group_id: group_id.to_string(),
        kind: Some(ThreadKind::Worker),
        partitions_owned: vec![],
        metrics: vec![],
    };
    let threads = match deployed {
        // What the registry holds rides beside it rather than waiting for a deployment to carry it -- there are
        // no source or destination names to resolve against, so those labels stay as they were written.
        None => {
            let mut out = vec![bootstrap()];
            out.extend(thread_reports(group_id, &[], heartbeat::split_by_thread(&peeked)));
            out
        }
        Some(d) => {
            // A reader carries the shards it owns; nothing else holds a source, so nothing else owns any.
            let running: Vec<(String, ThreadKind, Vec<PartitionRef>)> = d
                .readers
                .iter()
                .map(|r| (r.thread_id.clone(), ThreadKind::Reader, r.partitions.clone()))
                .chain(
                    d.pool_thread_ids
                        .iter()
                        .map(|id| (id.clone(), ThreadKind::Transform, vec![])),
                )
                .chain(
                    d.egress_ids
                        .iter()
                        .map(|id| (format!("egress-{id}"), ThreadKind::Egress, vec![])),
                )
                .collect();
            let mut by_thread = heartbeat::split_by_thread(&peeked);
            attach_owned_facts(
                &mut by_thread,
                d.readers
                    .iter()
                    .map(|r| (r.thread_id.clone(), r.held.clone().into_iter().collect())),
            );
            attach_owned_facts(
                &mut by_thread,
                d.readers
                    .iter()
                    .map(|r| (r.thread_id.clone(), r.connection_error.clone().into_iter().collect())),
            );
            attach_owned_facts(
                &mut by_thread,
                d.egress_refusals
                    .iter()
                    .map(|(id, facts)| (format!("egress-{id}"), facts.clone())),
            );
            // Subject identity, resolved once for the whole report (GOAL ONE): every sample landed above still
            // labels its source or destination by the positional id `Reader`/`egress` wrote, which does not
            // survive a config edit that reorders or deletes an unrelated entry.
            for metrics in by_thread.values_mut() {
                resolve_subject_labels(metrics, &d.subjects);
            }
            // What the runtime's own registry recorded, already resolved when it arrived (`absorb`). It reaches
            // the leader through this request because the process that recorded it has no connection of its own.
            for (thread, samples) in split_dtos_by_thread(&d.metrics) {
                by_thread.entry(thread).or_default().extend(samples);
            }
            let mut out = if running.is_empty() || stopping {
                vec![bootstrap()]
            } else {
                Vec::new()
            };
            out.extend(thread_reports(group_id, &running, by_thread));
            out
        }
    };
    (
        HeartbeatRequest {
            heartbeat_interval_ms: heartbeat_interval.map(|i| i.as_millis() as u64),
            single_worker: deployed.map(|d| d.single_worker.clone()).unwrap_or_default(),
            // Already capped, by `take_runtime_window`.
            progress: deployed.map(|d| d.progress.clone()).unwrap_or_default(),
            streams: deployed.and_then(|d| d.streams.clone()),
            // Numbered by the beat that sends it (`Supervisor::streams_window`).
            streams_window: None,
            stopping,
            accepts,
            took,
            authority_fingerprints,
            takes_authority,
            secret_versions: secrets.versions(),
            supervising,
            resources,
            worker_id: worker_id.to_string(),
            // Peeked, like the registry's counters: a send the leader does not answer leaves them queued for the
            // next beat, so a dropped connection costs a round trip rather than the logs it was carrying.
            // This process's own, plus whatever the runtime said about itself. A runtime has no connection to
            // the leader, so its logs reach the Logs screen on the supervisor's beat or not at all; each carries
            // the boot id of the process that numbered it, so two sequences cannot collide under one key.
            logs,
            boot_id: crate::logs::boot_id(),
            group_id: group_id.to_string(),
            stop_after_unreachable_secs,
            // What is running, not what was last offered: a commit this worker fetched but refused never becomes
            // a deployment, and reporting it would tell the leader the fleet had moved when it had not.
            commit: commit.map(str::to_string),
            smeltr_version: crate::version::VERSION.to_string(),
            release: crate::version::RELEASE.map(str::to_string),
            authority_expires_millis,
            threads,
        },
        peeked,
        own_logs,
        runtime_logs,
    )
}

/// Whether a heartbeat body of `bytes` has crossed [`heartbeat::HEARTBEAT_BODY_WARNING`] of
/// [`heartbeat::HEARTBEAT_BODY_LIMIT`].
fn heartbeat_body_is_large(bytes: usize) -> bool {
    bytes as f64 > heartbeat::HEARTBEAT_BODY_LIMIT as f64 * heartbeat::HEARTBEAT_BODY_WARNING
}

/// Warns once a heartbeat's body has grown past [`heartbeat::HEARTBEAT_BODY_WARNING`] of what the leader will
/// accept. Nothing bounds metrics the way [`heartbeat::LOGS_PER_HEARTBEAT`] bounds logs, so this is the operator's
/// notice of a worker heading toward the limit, well before a beat is ever refused.
fn warn_if_heartbeat_body_large(req: &HeartbeatRequest) {
    let Ok(bytes) = serde_json::to_vec(req) else {
        return;
    };
    if heartbeat_body_is_large(bytes.len()) {
        tracing::warn!(
            target: "worker.heartbeat_body_large",
            bytes = bytes.len(),
            limit = heartbeat::HEARTBEAT_BODY_LIMIT,
            "a heartbeat has grown past half of what the leader will accept"
        );
    }
}

/// Check in, then heartbeat forever, redeploying whenever the leader deploys. Never returns.
pub fn run_worker(mut config: WorkerConfig) {
    std::fs::create_dir_all(&config.state_dir).expect("create worker state directory");

    let worker_id = load_or_create_worker_id(&config.state_dir);
    // `logs::start` runs before this process knows its role, so the buffer has been open since startup with
    // nothing to call itself. From here every event it carries says which worker spoke.
    crate::logs::set_worker(&worker_id);
    tracing::info!(
        target: "worker.started",
        subject = %worker_id,
        group = %config.group_id,
        "worker starting"
    );

    // Settled before enrolling: the enrollment code names what to verify the leader with, and enrolling is the first
    // request that has to be verified.
    let leader_ca_pem = resolve_authority_waiting(&config);

    let token = match load_credential(&config.state_dir) {
        Some(t) => t,
        None => {
            // The binary checks `needs_an_enrollment_code` first and refuses to start with a message naming the
            // setting.
            let code = config.enrollment_code.as_deref().expect(
                "no persisted credential in state_dir and no enrollment code provided -- \
                     pass one on this worker's very first run",
            );
            // A first-boot worker whose leader is not up yet is a startup race, not a misconfiguration.
            let mut backoff = ENROLL_RETRY_MIN;
            let t = loop {
                match enroll(
                    &config.leader_url,
                    code,
                    &worker_id,
                    leader_ca_pem.as_deref(),
                    config.leader_trust_public,
                ) {
                    Ok(t) => break t,
                    Err(refused @ Unresolved::Refused(_)) => {
                        tracing::error!(
                            target: "worker.enroll_refused",
                            error = %refused.message(),
                            "this worker cannot enroll with the enrollment code it was given"
                        );
                        crate::logs::stop();
                        std::process::exit(1);
                    }
                    Err(unreachable) => {
                        tracing::warn!(
                            target: "worker.enroll_failed",
                            error = %unreachable.message(),
                            retry_in = ?backoff,
                            "enrollment failed, and will be retried"
                        );
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(ENROLL_RETRY_MAX);
                    }
                }
            };
            if let Err(e) = save_credential(&config.state_dir, &t) {
                // The enrollment code is spent, and this token is the only thing that came back. Starting without it
                // means enrolling again with an enrollment code that no longer works, so stopping here is what leaves
                // an operator something to fix.
                tracing::error!(
                    target: "worker.credential_not_saved",
                    error = %e,
                    "this worker enrolled and could not keep the credential it was given, so it would have to \
                     enroll again with an enrollment code that is now spent"
                );
                crate::logs::stop();
                std::process::exit(1);
            }
            tracing::info!(
                target: "worker.enrolled",
                path = %config.state_dir.display(),
                "enrolled, and the credential is saved"
            );
            t
        }
    };

    // Git's TLS trust config wants a path, not bytes.
    let git_roots = git_roots_path(&config.state_dir);
    write_git_roots(&git_roots, leader_ca_pem.as_deref(), config.leader_trust_public)
        .expect("write what git's fetch trusts of the leader");

    let repo_dir = repo_dir(&config.state_dir);

    // The process's, not a deployment's: a capture in flight when a redeploy lands is one somebody is still watching,
    // and losing it would be losing their records rather than restarting their session.
    let captures = Arc::new(capture::Captures::default());
    // The supervisor's own, for the life of the process. Its heartbeat reports from it, and a capture this process
    // serves reads it and reports into it under its own label, so a worker running nothing still has somewhere to
    // report from. A runtime builds a registry of its own.
    let registry = Arc::new(Metrics::new());
    let metrics_endpoint = config.metrics_endpoint.take();

    let supervisor = Arc::new(Supervisor {
        // The authority this worker resolved for itself, which is not always the one its configuration named: an
        // enrollment code pins one of its own, and a worker inside a leader's process follows what that leader serves
        // under.
        client: Mutex::new(Arc::new(
            HeartbeatClient::new(
                &config.leader_url,
                &token,
                leader_ca_pem.as_deref(),
                config.leader_trust_public,
            )
            .expect("build heartbeat client"),
        )),
        // What the leader would say about its own copy is not what stops this worker: the copy this worker holds
        // is what its verification uses. A leader still serving under a root that is itself running out is the
        // leader's own page to say so.
        authority_expires_millis: Mutex::new(leader_ca_pem.as_deref().and_then(expires)),
        trusting: Mutex::new(leader_ca_pem),
        worker_id,
        token,
        git_roots,
        repo_dir,
        captures,
        registry,
        secrets: Mutex::new(crate::plan::topology::HeldSecrets::default()),
        last_contact: Mutex::new(None),
        target: Mutex::new(None),
        arrived: Condvar::new(),
        runtime: Mutex::new(None),
        streams_owed: Mutex::new(OwedStreams::default()),
        commit: Mutex::new(None),
        supervising: Mutex::new(heartbeat::Supervising {
            host_name: host_name(),
            ..Default::default()
        }),
        resources: Mutex::new(crate::resources::Sampler::new(config.state_dir.clone())),
        invitations: Mutex::new(Invitations::default()),
        outbox: Mutex::new(Vec::new()),
        offline_stop: AtomicBool::new(false),
        mirror_due: AtomicBool::new(false),
        logs_mirror: Mutex::new(None),
        exit: Mutex::new(Exit::Running),
        exit_changed: Condvar::new(),
        enrolled_again_at: Mutex::new(None),
        took: Mutex::new(None),
        restart_due: AtomicBool::new(false),
        holds: Arc::new(Mutex::new(Holds::default())),
        receiver_sockets: Mutex::new(crate::io::receiver::Sockets::default()),
        config,
    });
    // One worker per process, so this is set once. A second would be a test running two in one process, and the
    // first is the one a signal ends.
    let _ = RUNNING.set(supervisor.clone());

    if let Some(port) = metrics_endpoint {
        let scraping = supervisor.clone();
        match port.serve(move || scraping.scrape()) {
            Ok(address) => tracing::info!(
                target: "worker.metrics_endpoint",
                address = %address,
                path = metrics_endpoint::PATH,
                "serving metrics for Prometheus"
            ),
            // Bound already, so what is left to fail is the server's own runtime, which every receiver needs too.
            Err(e) => tracing::error!(
                target: "worker.metrics_endpoint_failed",
                error = %e,
                "this worker could not serve its metrics endpoint"
            ),
        }
    }

    tracing::info!(
        target: "worker.awaiting_first_heartbeat",
        "nothing runs until this worker has checked into the leader with a heartbeat request"
    );

    // Three schedules, because only one of them can stall. Stopping a deployment joins threads that may never
    // return, and a worker whose heartbeat and discovery were behind that join could not be told to run anything
    // else -- which is the state this separation exists to make unreachable.
    let discovering = supervisor.clone();
    std::thread::Builder::new()
        .name("smeltr-discovery".to_string())
        .spawn(move || discovering.discover_forever())
        .expect("spawn the discovery thread");
    let deploying = supervisor.clone();
    std::thread::Builder::new()
        .name("smeltr-deployer".to_string())
        .spawn(move || deploying.deploy_forever())
        .expect("spawn the deployment thread");
    // A fourth, for the one thing a worker sends that is not on anybody's clock: a capture is as big as the
    // leader allowed, and neither schedule above may wait behind it.
    let uploading = supervisor.clone();
    std::thread::Builder::new()
        .name("smeltr-capture-upload".to_string())
        .spawn(move || uploading.upload_forever())
        .expect("spawn the capture upload thread");
    // The heartbeat keeps this thread, which is the one the process was given: a worker that stopped heartbeating
    // has stopped being a worker, whatever else is still running.
    supervisor.heartbeat_forever()
}

/// Fold `incoming` into `base` by session: a session already present has its entry replaced with the incoming
/// one, since `incoming` is always the fresher of the two -- newly finished captures over what the outbox already
/// held, or what arrived while a failed upload was in flight over the copy that failed. A session `incoming`
/// does not name is left exactly where `base` had it.
///
/// A final report already queued is never replaced by a progress report for the same session: a runtime
/// releases its local session as soon as it writes a final report to its own pipe, not once the leader has it
/// (`docs/status.md`), so a repeated invitation can re-admit that session and start it filling again while the
/// original final report is still waiting to go out. Its own next progress report must not erase what the
/// finished capture already caught.
fn merge_owed(base: &mut Vec<crate::heartbeat::CaptureReport>, incoming: Vec<crate::heartbeat::CaptureReport>) {
    for report in incoming {
        match base.iter_mut().find(|held| held.session == report.session) {
            Some(held) if held.finished.is_some() && report.finished.is_none() => {}
            Some(held) => *held = report,
            None => base.push(report),
        }
    }
}

/// Drop what is waiting until the outbox is within its bound. Every path that puts a report there ends here,
/// because one that did not would be the path that grows without limit.
///
/// The oldest progress report goes first, however many finished reports sit behind it: a progress report is one
/// growth tick away from being offered again, where a finished one -- once a live runtime has written it home
/// and released its own copy -- is gone for good if this worker does not keep it. Only once no progress report
/// is left does a finished one become the oldest thing here, and so the one dropped.
fn trim_outbox(outbox: &mut Vec<crate::heartbeat::CaptureReport>) {
    let Some(over) = outbox.len().checked_sub(CAPTURES_WAITING).filter(|n| *n > 0) else {
        return;
    };
    for _ in 0..over {
        let victim = outbox.iter().position(|r| r.finished.is_none()).unwrap_or(0);
        outbox.remove(victim);
    }
    tracing::warn!(
        target: "worker.captures_dropped",
        dropped = over,
        waiting = CAPTURES_WAITING,
        "more captures are waiting for the leader than this worker will hold, so some are dropped"
    );
}

/// The next batch to upload, oldest first, drained rather than copied out from under the lock. What stays behind
/// is what a concurrent `owe_captures` sees and counts against the cap, so a slow batch never leaves the outbox
/// holding more than the reports still queued behind it.
fn next_upload_batch(outbox: &Mutex<Vec<crate::heartbeat::CaptureReport>>) -> Vec<crate::heartbeat::CaptureReport> {
    let mut outbox = outbox.lock().unwrap();
    let take = CAPTURE_UPLOADS_AT_ONCE.min(outbox.len());
    outbox.drain(..take).collect()
}

/// How many capture uploads may be in flight. Each is allowed a budget far past a heartbeat's, so one that spends
/// it must not be what a session behind it waits on.
const CAPTURE_UPLOADS_AT_ONCE: usize = 4;

/// How many finished captures may wait for a leader that is not taking them. Each is bounded by the ceilings the
/// leader set when it invited one, so this bounds the memory a worker spends holding them.
const CAPTURES_WAITING: usize = 32;

/// How long a runtime has to load, compile, open its connectors and say so. Kafka alone gives a client thirty
/// seconds to set itself up, and a bundle may name several, so this is generous: what it bounds is a startup that
/// will never finish, not one that is merely slow.
const STARTUP_BUDGET: Duration = Duration::from_secs(180);

/// How long a cooperative stop is given. It has to cover the readers' drain (`DRAIN_TIMEOUT`), the retries an egress
/// thread makes while it finishes, and a destination's own stall threshold -- a budget shorter than that sequence
/// would force-kill workers that were about to finish.
const GRACEFUL_STOP_BUDGET: Duration = Duration::from_secs(90);

/// How long the operating system is given to make a killed process tree gone, after which its exit is not
/// confirmed and nothing may take its place.
const HARD_STOP_BUDGET: Duration = Duration::from_secs(30);

/// How long a process asked to end waits for its runtime to be confirmed gone. A stop's whole allowance -- the
/// frame that asks, the drain and the kill -- twice over: a signal can land while the deployer is already inside
/// a start or a stop, each bounded the same way, and the stop it asks for comes after.
const EXIT_BUDGET: Duration = Duration::from_secs(
    2 * (child::CONTROL_FRAME_BUDGET.as_secs() + GRACEFUL_STOP_BUDGET.as_secs() + HARD_STOP_BUDGET.as_secs()),
);

/// How long a worker on its way out gives its leader to take the notice that it is stopping: one beat already on
/// its way, and the notice behind it, each allowed its retry. A leader that takes longer is told nothing, and reads
/// the worker as silent.
const NOTICE_BUDGET: Duration = Duration::from_secs(4 * heartbeat::REQUEST_BUDGET.as_secs());

/// How often a worker its leader no longer knows tries its enrollment code again.
const ENROLL_AGAIN_EVERY: Duration = Duration::from_secs(60);

/// What a worker that enrolled again exits with, so it is started again as the new worker. It is sysexits'
/// temporary failure: a supervisor that restarts only a service that failed restarts this one too.
const ENROLLED_AGAIN_EXIT: i32 = 75;

/// The worker this process runs, once it runs one. What a signal ends before the process does.
static RUNNING: std::sync::OnceLock<Arc<Supervisor>> = std::sync::OnceLock::new();

/// Ends this process's deployment before the process ends: the runtime drains, commits and closes its consumers, as it
/// does for a redeploy. Returns once the runtime is confirmed gone, or at once where this process
/// runs no worker or its worker has not yet checked in.
///
/// Without this a worker's exit leaves its runtime to find out from its closed pipe. Where the supervisor is a
/// container's first process that never happens: its exit ends every process in the container, the runtime
/// mid-batch among them, and the consumer group waits out a session timeout for a member that never left.
pub fn stop_before_exit() {
    if let Some(supervisor) = RUNNING.get() {
        supervisor.stop_for_exit();
    }
}

/// How long the deployer waits before coming back to see to what it owes -- a report to absorb, a capture to hand
/// on, a runtime that has ended. Long enough that an idle worker is idle, short enough that none of those waits.
const IDLE_POLL: Duration = Duration::from_millis(250);

/// How long a deployment that failed waits before trying the same target again, and the ceiling that backoff
/// climbs to. A newer target does not wait: it is a different target, and whatever refused the last one says
/// nothing about it.
const REDEPLOY_RETRY_MIN: Duration = Duration::from_secs(1);
const REDEPLOY_RETRY_MAX: Duration = Duration::from_secs(60);

/// How long a target may go on being refused before the refusal is a warning. Long enough for a secret version a commit
/// names to reach this worker, which a rotation races.
const REFUSAL_WARN_AFTER: Duration = Duration::from_secs(30);

/// What a worker is, apart from whatever it happens to be running.
///
/// Three schedules share it. None of them holds a lock across anything external -- a network call, a git command,
/// a thread join -- so what one is waiting on is never what another is waiting for.
struct Supervisor {
    config: WorkerConfig,
    worker_id: String,
    token: String,
    /// Git's TLS trust wants a path, not bytes.
    git_roots: PathBuf,
    repo_dir: PathBuf,
    /// Replaced when the leader this worker lives inside serves TLS under a new authority, so it is read out and
    /// cloned rather than borrowed: an upload holding the client for a whole capture must not be what a heartbeat
    /// waits behind, which is the stall the separate schedules exist to rule out.
    client: Mutex<Arc<HeartbeatClient>>,
    /// The authority this worker verifies its leader with, and when it runs out. Both move together with `client`,
    /// and only for a worker inside the leader's own process: see `WorkerConfig::leader_authority_dir`.
    trusting: Mutex<Option<Vec<u8>>>,
    authority_expires_millis: Mutex<Option<i64>>,
    captures: Arc<capture::Captures>,
    registry: Arc<Metrics>,
    /// The credentials this worker can obtain. Worker-local ones are its own environment and are there from the
    /// start; the leader's arrive on the heartbeat and are read by a deployment deciding whether it can start.
    secrets: Mutex<crate::plan::topology::HeldSecrets>,
    /// When the leader last answered. `None` until it has answered once, which is what gates everything else:
    /// a worker runs nothing until it has checked in, and its clone buys speed rather than independence.
    last_contact: Mutex<Option<Instant>>,
    /// The newest commit discovery has seen. Ordering is by observation and never by commit ancestry, so a
    /// rollback to an older commit is eligible the moment it is seen, like any other newly observed target.
    target: Mutex<Option<Target>>,
    arrived: Condvar,
    /// What the runtime last said about itself, or `None` when nothing is running. A snapshot rather than a set
    /// of locks: the threads it describes belong to another process now.
    runtime: Mutex<Option<child::RuntimeReport>>,
    /// The stream window the leader has not acknowledged, sent again unchanged until it is (`streams_window`).
    streams_owed: Mutex<OwedStreams>,
    /// The commit this worker represents. Not the runtime's to report: a commit whose group and `shared` subtrees
    /// match what is already running advances this without restarting anything.
    commit: Mutex<Option<String>>,
    /// What this supervisor is doing about what it runs, for the heartbeat to carry. Written by the thread whose
    /// business each fact is: discovery says when it last looked and when it last found, and the deployer says
    /// what phase it is in and why the last attempt did not run.
    supervising: Mutex<heartbeat::Supervising>,
    /// What this process is spending against what it is allowed to run in. A rate needs a previous reading, so
    /// one sampler lives for the supervisor's whole life rather than being built fresh each beat.
    resources: Mutex<crate::resources::Sampler>,
    /// What the leader lists, and the invitations waiting for the deployer. With a runtime running, the runtime
    /// decides -- it is what knows which sources it reads, and a second consumer opened here against one would be
    /// two on one source -- except for a session this process still holds, which stays here.
    invitations: Mutex<Invitations>,
    /// Finished captures waiting for the leader. Oldest first.
    outbox: Mutex<Vec<crate::heartbeat::CaptureReport>>,
    /// Set by the heartbeat when the leader has been unreachable past this worker's own limit, and acted on by
    /// the deployer. The stop joins threads, so the thread that noticed is not the thread that does it.
    offline_stop: AtomicBool,
    /// Set by each heartbeat once it has recorded this process's own gauges, and cleared by the deployer when it
    /// hands the runtime what this registry holds ([`Supervisor::mirror_to`]).
    mirror_due: AtomicBool,
    /// What this process has said since it last handed the runtime its logs, open while a runtime runs and closed
    /// while none does, so nothing accumulates for a runtime that does not exist.
    logs_mirror: Mutex<Option<crate::logs::Tap>>,
    /// How far this process is on its way out. The deployer, the heartbeat and the thread ending the process each
    /// move it on in turn, and each waits for the one before.
    exit: Mutex<Exit>,
    exit_changed: Condvar,
    /// When this worker last tried to enroll again, so an enrollment code that will not enroll it is not tried every
    /// beat.
    enrolled_again_at: Mutex<Option<Instant>>,
    /// The last directive this worker took, by id. Every beat names it, so the leader stops repeating it.
    took: Mutex<Option<String>>,
    /// Set by the heartbeat when the leader asks for a restart, and acted on by the deployer, for the reason
    /// `offline_stop` is.
    restart_due: AtomicBool,
    /// The single-worker sources the leader holds for this worker, and what the running runtime was told of them.
    /// Shared with the thread fetching their ledgers.
    holds: Arc<Mutex<Holds>>,
    /// The sockets of the receivers this worker runs, held across the runtimes it starts.
    receiver_sockets: Mutex<crate::io::receiver::Sockets>,
}

/// How long a grant the runtime did not make waits before it is offered again, at first and at most. It doubles
/// with each try, so a ledger that will never read costs one fetch every few minutes rather than one a turn.
const HOLD_RETRY_MIN: Duration = Duration::from_secs(5);
const HOLD_RETRY_MAX: Duration = Duration::from_secs(300);

/// The leader's holds for this worker, set by the heartbeat and handed to the runtime by the deployer. Each grant
/// goes with its ledger, fetched on a thread of its own: a ledger can take far longer than a beat or a deployer
/// turn may.
///
/// ```text
///   heartbeat           deployer turn                              fetch thread
///   listed ------> plan: what the runtime holds vs listed --+--> fetch each ledger
///                         |                                  |        |
///                         | nothing to fetch                 |   fetched (if the epoch still stands)
///                         v                                  v        |
///                    Holds frame (seq) <---------------------+--------+
///                         |
///   report: holding, holds_applied --> a grant missing once applied is offered again, after a backoff
/// ```
#[derive(Default)]
struct Holds {
    /// What the leader's last answer held. An answer that did not come changes nothing, so a holder keeps reading
    /// through an outage.
    listed: Vec<heartbeat::Hold>,
    /// Every source the running runtime was last told it holds, and the frame that last offered it a grant.
    /// Empty for a runtime that has just started.
    told: std::collections::BTreeMap<heartbeat::SourceKey, Offered>,
    /// The number of the last holds frame sent. Never reset, so a report naming the last frame a runtime applied
    /// cannot be read against frames sent to the one before it.
    seq: u64,
    /// Moves when the sources listed change and when a runtime starts. A fetch begun under an earlier epoch is
    /// thrown away when it finishes.
    epoch: u64,
    /// The epoch a ledger fetch is running for.
    fetching: Option<u64>,
    /// Whether the progress waiting for the leader here is past [`held::BACKLOG_FULL`], and what the running
    /// runtime was last told of it: `None` for one that has just started.
    backlogged: bool,
    told_backlogged: Option<bool>,
    /// A frame whose ledgers have been fetched, for the deployer to send.
    fetched: Option<(u64, Vec<(held::Holding, Offer)>)>,
}

/// The frame that last offered a source a grant, and how many offers it has had.
#[derive(Debug, Clone, Copy)]
struct Offered {
    seq: u64,
    at: Instant,
    tries: u32,
}

/// What a holds frame asks of the runtime for one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Offer {
    /// Keep it as it is: reading if it is held, unheld if it is not, since without its ledger it would read
    /// everything again.
    Keep,
    /// Grant it, with its ledger.
    Grant,
}

impl Holds {
    /// What a frame would carry now, or `None` when the runtime has been told all there is: every listed source,
    /// each with what it asks. A source is granted when it is new to this runtime, or when a grant the runtime has
    /// applied did not take and its backoff has run. `holding` and `applied` are the runtime's latest report.
    fn plan(
        &self,
        holding: &[heartbeat::SourceKey],
        applied: u64,
        now: Instant,
    ) -> Option<Vec<(heartbeat::Hold, Offer)>> {
        let listed = |key: &heartbeat::SourceKey| self.listed.iter().any(|h| &h.key == key);
        let mut due = self.listed.len() != self.told.len()
            || self.listed.iter().any(|h| !self.told.contains_key(&h.key))
            || holding.iter().any(|key| !listed(key));
        let planned = self
            .listed
            .iter()
            .map(|hold| {
                let offer = match self.told.get(&hold.key) {
                    _ if holding.contains(&hold.key) => Offer::Keep,
                    None => Offer::Grant,
                    // Not applied yet, or applied and backing off.
                    Some(o) if applied < o.seq || now < o.at + hold_retry(o.tries) => Offer::Keep,
                    Some(_) => Offer::Grant,
                };
                due |= offer == Offer::Grant;
                (hold.clone(), offer)
            })
            .collect();
        due.then_some(planned)
    }

    /// A new runtime holds nothing, so every listed source is granted it again, under a new epoch.
    fn restarted(&mut self) {
        self.told.clear();
        self.epoch += 1;
        self.told_backlogged = None;
    }

    /// Record frame `seq` as sent, with what it asked of each source.
    fn sent(&mut self, seq: u64, frame: &[(held::Holding, Offer)], now: Instant) {
        let before = std::mem::take(&mut self.told);
        for (holding, offer) in frame {
            let key = &holding.hold.key;
            let offered = match (offer, before.get(key)) {
                (Offer::Keep, Some(o)) => *o,
                (Offer::Keep, None) => Offered { seq, at: now, tries: 0 },
                (Offer::Grant, o) => Offered {
                    seq,
                    at: now,
                    tries: o.map_or(0, |o| o.tries) + 1,
                },
            };
            self.told.insert(key.clone(), offered);
        }
    }
}

/// How long a source offered `tries` grants waits before the next.
fn hold_retry(tries: u32) -> Duration {
    HOLD_RETRY_MIN
        .saturating_mul(1 << tries.saturating_sub(1).min(6))
        .min(HOLD_RETRY_MAX)
}

/// A worker's way out, in the order it goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Exit {
    /// Nothing has asked this process to end.
    Running,
    /// Asked. The deployer ends the runtime and starts nothing more.
    Draining,
    /// The runtime is gone, or has been waited on for as long as it will be. The next heartbeat says the worker is
    /// stopping, and is the last.
    Notice,
    /// The leader has been told, or the heartbeat tried. The process may end.
    Told,
}

/// What the leader's last answer listed, and what of it waits for the deployer. One lock, so the deployer reads a
/// queue and a listing from the same answer. The heartbeat lets go of sessions under it, the upload thread marks an
/// answer under it, and the deployer decides under it whether to serve an invitation. An invitation this process
/// serves itself is admitted under it too, so it is never served after the answer that let its session go or after
/// the leader has this worker's answer to it. One forwarded to a runtime is written after the lock is released, and
/// the runtime admits it on its own thread, so it can be served after either; the next listing that leaves its
/// session out ends it there.
#[derive(Default)]
struct Invitations {
    /// Every capture session the leader's last answer listed for this worker.
    listed: HashSet<String>,
    /// Invitations waiting for the deployer to route. Each passed [`Invitations::wants`] when it was queued. An
    /// answer marked since can fail it, so the deployer asks again before it serves one.
    queued: Vec<crate::heartbeat::CaptureInvitation>,
    /// Listed sessions the leader already has this worker's final report for. The leader lists a session until
    /// it has that report, so an answer that crossed the acknowledgment still lists it; this keeps that answer from
    /// serving it again. Every one is in `listed`.
    answered: HashSet<String>,
    /// Set when `listed` moves, and cleared on the deployer's next turn, which tells the running runtime or finds
    /// none running. A runtime started later holds nothing a listing could let go of.
    changed: bool,
}

impl Invitations {
    /// Whether the leader wants `session` served by this worker: listed, and not yet answered.
    fn wants(&self, session: &str) -> bool {
        self.listed.contains(session) && !self.answered.contains(session)
    }
}

/// What the supervisor learned by loading a target it is about to run.
struct Validated {
    named: Vec<crate::plan::topology::NamedSecret>,
    /// Every source the target reads. A cold capture this process opened against one of them ends when the
    /// runtime starts, or it would go on consuming beside the runtime's own reader.
    sources: Vec<String>,
    /// The oid of every lookup file the target names, each now in this host's store.
    lookup_files: std::collections::HashSet<String>,
    /// What every receiver the target reads listens on, which this process holds for each runtime it starts.
    listens: Vec<crate::plan::topology::Listen>,
    /// The same, by the source each is, as the heartbeat reports it once the target runs.
    listening: Vec<heartbeat::Listening>,
}

/// `read`'s answer, with a panic in it answered as an error that says so. The deployer is the only thread that
/// starts anything, so a commit that panics it is refused like any other rather than ending it.
fn unpanicked<T>(read: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(read)).unwrap_or_else(|panic| {
        let said = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        Err(format!("reading the commit panicked: {said}"))
    })
}

/// What came of asking for a runtime.
enum Begun {
    /// It is running, and has said so.
    Started(child::RuntimeHandle),
    /// It will not run, and nothing of the attempt is left behind.
    Refused,
    /// The attempt would not end. Nothing may take its place until it does.
    Unconfirmed(child::RuntimeHandle),
}

/// A commit discovery has seen, and which observation saw it.
#[derive(Clone)]
struct Target {
    /// Monotonic per worker. What makes "newer" a fact about when this worker looked rather than about ancestry.
    generation: u64,
    commit: String,
}

impl Supervisor {
    /// Whether `commit` will run here, and what credentials it names. Loading and compiling are this process's
    /// own work against the object store, so this refuses before anything is torn down.
    fn validate(&self, commit: &str) -> Result<Validated, String> {
        let lookup_files = self.fetch_lookup_files(commit)?;
        let held = self.secrets.lock().unwrap().clone();
        let (system, _, _) = load_target(&self.repo_dir, commit, &self.config.group_id, &held)?;
        let listening: Vec<heartbeat::Listening> = system
            .topology
            .sources
            .iter()
            .filter_map(|s| {
                s.kind.listens().map(|l| heartbeat::Listening {
                    source: s.name.clone(),
                    transport: l.transport,
                    address: l.address,
                })
            })
            .collect();
        Ok(Validated {
            named: system.topology.named_secrets.clone(),
            sources: system.topology.sources.iter().map(|s| s.name.clone()).collect(),
            lookup_files,
            listens: listening
                .iter()
                .map(|l| crate::plan::topology::Listen {
                    transport: l.transport,
                    address: l.address,
                })
                .collect(),
            listening,
        })
    }

    /// Fetch every lookup file `commit` names for this group that this host's store lacks, each checked against its
    /// pointer, and answer the oids it names.
    fn fetch_lookup_files(&self, commit: &str) -> Result<std::collections::HashSet<String>, String> {
        use crate::lookup::store;
        let mut prefixes = vec![crate::config_file::SHARED_DIR.to_string()];
        prefixes.extend(crate::config_file::group_subtree(&self.config.group_id));
        let pointers = crate::leader::tree::lookup_pointers(&self.repo_dir, commit, &prefixes)?;
        let client = self.client.lock().unwrap().clone();
        store::fetch_missing(&self.repo_dir, pointers.keys(), |pointer| {
            client.lookup_file(&pointer.oid).map_err(|e| e.to_string())
        })?;
        Ok(pointers.into_keys().map(|p| p.oid).collect())
    }

    /// Say what the deployer is doing now. The time is when this phase began, so a phase that does not move is
    /// visible as one whose start recedes.
    fn phase(&self, phase: &str) {
        let mut supervising = self.supervising.lock().unwrap();
        if supervising.phase.as_deref() != Some(phase) {
            supervising.phase = Some(phase.to_string());
            supervising.phase_since_ms = Some(now_ms());
        }
    }

    /// Why the last attempt did not run, or `None` once one has. Cut to the length every reported string is.
    fn note_failure(&self, failure: Option<&str>) {
        self.supervising.lock().unwrap().failure =
            failure.map(|f| crate::runtime::truncate(f, heartbeat::MAX_TEXT_LEN));
    }

    /// Whether the leader has ever answered. Until it has, this worker runs nothing.
    fn contacted(&self) -> bool {
        self.last_contact.lock().unwrap().is_some()
    }

    /// Takes the authority the leader is serving now, for a worker running inside that leader's own process.
    ///
    /// Everything that means trusting a leader moves together: the client that verifies a heartbeat, the file git
    /// verifies through, and the date the leader warns on. Nothing counts as taken until all of it is, so a write
    /// that fails is tried again on the next beat rather than leaving git behind under an authority this worker
    /// has already reported adopting.
    fn follow_replaced_authority(&self, dir: &Path) {
        // Asked as one answer. The disk moving while the socket has not followed describes a leader this worker
        // cannot reach, and adopting it would replace trust that works with trust that does not, over a change no
        // client has seen -- and asking that separately from the read lets a replacement land between the two.
        let materials = match crate::leader::tls::current_if_agreed(dir) {
            Ok(materials) => materials,
            Err(crate::leader::tls::Refused::Conflict(_)) => return,
            Err(crate::leader::tls::Refused::Unavailable(e)) => {
                // A torn read while the leader is mid-change, most likely. Said out loud because every other
                // failure here is, and a silent one here looks like a worker that simply never noticed.
                tracing::warn!(
                    target: "worker.authority_unreadable",
                    error = %e,
                    "could not read what this leader is serving TLS with"
                );
                return;
            }
        };
        if self.trusting.lock().unwrap().as_deref() == Some(materials.ca_pem.as_slice()) {
            return;
        }

        let rebuilt = match HeartbeatClient::new(
            &self.config.leader_url,
            &self.token,
            Some(&materials.ca_pem),
            self.config.leader_trust_public,
        ) {
            Ok(rebuilt) => rebuilt,
            Err(e) => {
                tracing::warn!(
                    target: "worker.authority_unusable",
                    error = %e,
                    "the leader's authority changed and this worker cannot use what replaced it"
                );
                return;
            }
        };
        // Git's copy first, because it is the half that cannot be held in memory. Taking the rest without it
        // would leave this worker heartbeating under the new authority and fetching under the old one, which is
        // the failure with nothing to show for it. Beside and renamed rather than in place, because `git fetch`
        // reads this file from another thread and a truncating write is a window holding no usable authority.
        // Nothing is kept here: what replaced the authority is already on disk, and a start reads it back.
        if let Err(e) = write_git_roots(
            &self.git_roots,
            Some(&materials.ca_pem),
            self.config.leader_trust_public,
        ) {
            tracing::warn!(
                target: "worker.authority_not_written",
                error = %e,
                path = %self.git_roots.display(),
                "the leader replaced its authority and git's copy could not be written, so the old one is kept \
                 until it can be"
            );
            return;
        }
        // Together, so no beat reports one authority's expiry while sending under another's.
        let mut expires_millis = self.authority_expires_millis.lock().unwrap();
        let mut trusting = self.trusting.lock().unwrap();
        let mut client = self.client.lock().unwrap();
        *expires_millis = expires(&materials.ca_pem);
        *trusting = Some(materials.ca_pem);
        *client = Arc::new(rebuilt);
        tracing::info!(
            target: "worker.authority_replaced",
            "the leader replaced its certificate authority, and this worker took the new one"
        );
    }

    /// Takes `pem` as the authority this worker verifies its leader by, having been handed it on a beat.
    ///
    /// The leader is rotating its own and this worker does not hold what it is rotating to. Everything that means
    /// trusting a leader moves together, as in [`Self::follow_replaced_authority`], and one thing more: the copy a
    /// restart reads back. Without it this worker verifies the new authority until it stops and the old one
    /// afterward, against a leader that has by then signed under the new one.
    fn take_authority(&self, pem: Vec<u8>) {
        if self.trusting.lock().unwrap().as_deref() == Some(pem.as_slice()) {
            return;
        }
        let rebuilt = match HeartbeatClient::new(
            &self.config.leader_url,
            &self.token,
            Some(&pem),
            self.config.leader_trust_public,
        ) {
            Ok(rebuilt) => rebuilt,
            Err(e) => {
                tracing::warn!(
                    target: "worker.authority_unusable",
                    error = %e,
                    "the leader is rotating its authority and this worker cannot use what it was handed"
                );
                return;
            }
        };
        // The kept copy first. It is the one a restart resolves from, and git's is rebuilt from it at every
        // start -- so a crash between the two loses git's copy and nothing else, while the other order leaves the
        // restart resolving the old authority and writing it back over the new one.
        let kept = pinned_path(&self.config.state_dir);
        if let Err(e) = crate::leader::private_file::replace(&kept, &pem) {
            tracing::warn!(
                target: "worker.authority_not_kept",
                error = %e,
                path = %kept.display(),
                "the leader is rotating its authority and this worker could not keep the new one, so it is not \
                 taken"
            );
            return;
        }
        // Written beside and renamed over, never in place: the discovery thread runs `git fetch` against this
        // same file, and a truncating write hands it an empty or half-written trust store that refuses a leader
        // it should accept.
        if let Err(e) = write_git_roots(&self.git_roots, Some(&pem), self.config.leader_trust_public) {
            tracing::warn!(
                target: "worker.authority_not_written",
                error = %e,
                path = %self.git_roots.display(),
                "the leader is rotating its authority and git's copy could not be written. The next start writes \
                 it from the copy this worker kept"
            );
            return;
        }
        let mut expires_millis = self.authority_expires_millis.lock().unwrap();
        let mut trusting = self.trusting.lock().unwrap();
        let mut client = self.client.lock().unwrap();
        *expires_millis = expires(&pem);
        *trusting = Some(pem);
        *client = Arc::new(rebuilt);
        tracing::info!(
            target: "worker.authority_taken",
            "this worker took the certificate authority its leader is rotating to"
        );
    }

    // -- the heartbeat schedule ------------------------------------------------

    fn heartbeat_forever(&self) -> ! {
        loop {
            // Read before the beat is built, so the notice carries what the drain left the leader owed.
            let stopping = *self.exit.lock().unwrap() >= Exit::Notice;
            // Every beat, rather than only when one fails. A connection already open keeps succeeding across a
            // certificate change, because only a new handshake sees the new one -- while git opens a connection
            // per fetch and is refused by the authority this worker still holds. Waiting for a failed heartbeat
            // would leave such a worker reporting itself healthy and taking no configuration at all.
            if let Some(dir) = self.config.leader_authority_dir.as_deref() {
                self.follow_replaced_authority(dir);
            }

            // What the runtime last said, read under the lock. The deployment it describes is another process,
            // so nothing here can be waiting on one that is stopping.
            let mut sent = self.runtime.lock().unwrap().as_mut().map(take_runtime_window);
            let streams_window = self.streams_window(&mut sent);
            let (mut req, reported, own_logs, runtime_logs) = {
                let runtime = &sent;
                let commit = self.commit.lock().unwrap();
                let supervising = self.supervising.lock().unwrap().clone();
                let resources = self.resources.lock().unwrap().sample();
                let secrets = self.secrets.lock().unwrap();
                build_heartbeat_request(
                    Reporting {
                        worker_id: &self.worker_id,
                        group_id: &self.config.group_id,
                        commit: commit.as_deref(),
                        supervising: Some(supervising),
                        resources: Some(heartbeat::ResourceReport::from(resources)),
                        stop_after_unreachable_secs: self.config.stop_after_unreachable_secs,
                        authority_expires_millis: *self.authority_expires_millis.lock().unwrap(),
                        authority_fingerprints: self
                            .trusting
                            .lock()
                            .unwrap()
                            .as_deref()
                            .map(fingerprints)
                            .unwrap_or_default(),
                        // The leader is this worker's source of trust: it is handed what the leader rotates to,
                        // or, inside the leader's own process, reads it off the leader's disk each beat.
                        takes_authority: self.config.leader_ca_pem.is_none()
                            || self.config.leader_authority_dir.is_some(),
                        stopping,
                        accepts: self.accepts(),
                        took: self.took.lock().unwrap().clone(),
                        heartbeat_interval: Some(self.config.heartbeat_interval),
                    },
                    runtime.as_ref(),
                    &self.registry,
                    &secrets,
                )
            };
            req.streams_window = streams_window.filter(|_| req.streams.is_some());
            // Owed until the leader answers, and kept apart from the window `unanswered` gives back.
            if let Some(sent) = sent.as_mut() {
                sent.streams = None;
            }
            self.mirror_due.store(true, Ordering::Relaxed);
            warn_if_heartbeat_body_large(&req);
            // Cloned out rather than held: a beat that took the lock for the length of its request would be one
            // an upload could not replace the client behind, and the reverse is the stall that matters more.
            let client = self.client.lock().unwrap().clone();
            let answered = client.send(&req);
            if stopping {
                match &answered {
                    Ok(_) => tracing::info!(
                        target: "worker.said_stopping",
                        "told the leader this worker is stopping, so it will not report it silent"
                    ),
                    Err(e) => tracing::warn!(
                        target: "worker.stopping_not_said",
                        error = %e,
                        "could not tell the leader this worker is stopping, so it will report it silent"
                    ),
                }
                // Nothing more is said, whatever the answer: a beat after the notice would take it back.
                self.exit_to(Exit::Told);
                loop {
                    std::thread::park();
                }
            }
            match answered {
                Ok(resp) => {
                    *self.last_contact.lock().unwrap() = Some(Instant::now());
                    self.streams_delivered(req.streams_window);
                    // Committed only on success: a failed send leaves the window's counts owed. Against the
                    // process's registry, so a worker with nothing deployed commits what it still reported.
                    heartbeat::commit_for_heartbeat(&self.registry, &reported);
                    // Dropped only once the leader has them, the same bargain the capture reports make. Only
                    // this process's own: the runtime's came from a buffer it has already emptied.
                    crate::logs::delivered(own_logs);
                    // What the runtime said has now been said onward -- all but the logs that did not fit, which
                    // are still owed and go back ahead of whatever has arrived since.
                    if let Some(mut sent) = sent
                        && sent.logs.len() > runtime_logs
                        && let Some(current) = self.runtime.lock().unwrap().as_mut()
                    {
                        let overflow = child::RuntimeReport {
                            logs: sent.logs.split_off(runtime_logs),
                            boot_id: sent.boot_id,
                            ..Default::default()
                        };
                        untake_runtime_window(current, overflow);
                    }

                    // Whatever this worker's group needs and did not hold. An empty answer is the steady state and
                    // leaves what it has; a rotation is a handful of beats where it is not empty. Nothing redeploys
                    // on arrival -- the bundle's version is what says a deployment wants the new value.
                    self.secrets.lock().unwrap().take(resp.secrets);

                    // The leader is rotating its authority and this worker does not hold what it is rotating to.
                    // Sent only in that window, and only to a worker whose trust the leader is the source of: one
                    // started with `SMELTR_LEADER_CA` is trusting what its operator manages, and that is not the
                    // leader's to replace.
                    if let Some(pem) = resp.authority.filter(|_| self.config.leader_ca_pem.is_none()) {
                        self.take_authority(pem.into_bytes());
                    }
                    self.hand_back_captures();
                    self.follow_captures(resp.captures);
                    self.follow_holds(resp.holds);
                    if let Some(directive) = resp.directive {
                        self.follow_directive(directive);
                    }
                }
                Err(e) if e.certificate_untrusted() => {
                    tracing::warn!(
                        target: "worker.heartbeat_failed",
                        "the heartbeat failed: {}",
                        heartbeat::untrusted_certificate(&self.config.leader_url, &e.reason()),
                    );
                    self.consider_offline_stop();
                    self.unanswered(sent);
                }
                Err(e) => {
                    tracing::warn!(
                        target: "worker.heartbeat_failed",
                        error = %e,
                        "the heartbeat failed, so the leader has not heard from this worker"
                    );
                    if e.credential_unknown() {
                        self.enroll_again();
                    }
                    self.consider_offline_stop();
                    self.unanswered(sent);
                }
            }
            // The interval, or less once the notice is due: a worker on its way out is inside a grace period
            // somebody else set.
            let exit = self.exit.lock().unwrap();
            let _ = self
                .exit_changed
                .wait_timeout_while(exit, self.config.heartbeat_interval, |exit| *exit < Exit::Notice);
        }
    }

    /// The stream window the beat about to carry `sent` sends, and its number: the one still owed, or else what
    /// the runtime saw since the last, under the next number. Streams `sent` took that are not the owed window go
    /// back to the runtime's report for the window after.
    fn streams_window(&self, sent: &mut Option<child::RuntimeReport>) -> Option<u64> {
        let mut streams = self.streams_owed.lock().unwrap();
        let fresh = sent.as_mut().and_then(|s| s.streams.take());
        match (&streams.owed, fresh) {
            (None, Some(fresh)) => {
                streams.numbered += 1;
                streams.owed = Some((streams.numbered, fresh));
            }
            (Some(_), Some(mut fresh)) => {
                if let Some(current) = self.runtime.lock().unwrap().as_mut() {
                    if let Some(later) = current.streams.take() {
                        fresh.absorb(later);
                    }
                    current.streams = Some(fresh);
                }
            }
            (_, None) => {}
        }
        let (seq, report) = streams.owed.as_ref()?;
        sent.as_mut()?.streams = Some(report.clone());
        Some(*seq)
    }

    /// The leader has the stream window numbered `seq`.
    fn streams_delivered(&self, seq: Option<u64>) {
        let mut streams = self.streams_owed.lock().unwrap();
        if seq.is_some() && streams.owed.as_ref().map(|(n, _)| *n) == seq {
            streams.owed = None;
        }
    }

    /// A beat the leader did not answer, or answered with an error status, leaves what it carried owed: it goes back
    /// merged with whatever the runtime has said since -- oldest first for logs and progress, the order the leader
    /// takes them in, and summed for a counter, since neither window has reached the leader.
    fn unanswered(&self, sent: Option<child::RuntimeReport>) {
        if let Some(sent) = sent
            && let Some(current) = self.runtime.lock().unwrap().as_mut()
        {
            untake_runtime_window(current, sent);
        }
    }

    /// What this worker does when its leader asks.
    fn accepts(&self) -> Vec<heartbeat::DirectiveAction> {
        use heartbeat::DirectiveAction::{Restart, Shutdown};
        if self.config.in_leader {
            vec![Restart]
        } else {
            vec![Restart, Shutdown]
        }
    }

    /// Act on what the leader asked, once per id. Taken before it is done, so the leader stops repeating it on the
    /// next beat: a restart is the deployer's to carry out, and a shutdown ends with the notice that says so.
    fn follow_directive(&self, directive: heartbeat::Directive) {
        use heartbeat::DirectiveAction;
        let mut took = self.took.lock().unwrap();
        if took.as_deref() == Some(directive.id.as_str()) {
            return;
        }
        *took = Some(directive.id);
        drop(took);
        match directive.action {
            DirectiveAction::Restart if self.accepts().contains(&DirectiveAction::Restart) => {
                tracing::info!(target: "worker.restart_asked", "the leader asked this worker to restart its deployment");
                self.restart_due.store(true, Ordering::Relaxed);
                self.arrived.notify_all();
            }
            DirectiveAction::Shutdown if self.accepts().contains(&DirectiveAction::Shutdown) => {
                tracing::info!(target: "worker.shutdown_asked", "the leader asked this worker to shut down");
                // Off this thread: the stop waits for the notice, and the notice is this thread's to send.
                let spawned = std::thread::Builder::new().name("smeltr-exit".to_string()).spawn(|| {
                    stop_before_exit();
                    crate::logs::stop();
                    std::process::exit(0);
                });
                if let Err(e) = spawned {
                    tracing::warn!(
                        target: "worker.shutdown_not_started",
                        error = %e,
                        "could not start the thread that shuts this worker down, so it keeps running"
                    );
                }
            }
            action => tracing::warn!(
                target: "worker.directive_unknown",
                action = ?action,
                "the leader asked this worker for something it does not do, so it does nothing"
            ),
        }
    }

    /// Follow the leader's list of captures, which is whole on every answer: let go of every session this process
    /// holds still filling that it leaves out, drop the progress reports the leader would throw away, and keep the
    /// deployer's queue to what the leader still wants. On the heartbeat thread, so a queued invitation for a
    /// session the leader wants nothing more of is purged even while a start or a stop holds the deployer for
    /// minutes. Called after `hand_back_captures`, so a finished session's final is owed before anything is let go.
    /// One queued invitation per session: the leader repeats each on every beat until the capture is over or this
    /// worker has answered, and each copy carries the credentials its source names.
    ///
    /// An invitation this build cannot read is answered at once with a failed report, so the capture shows this worker
    /// as unable to take part rather than waiting on it until its deadline.
    fn follow_captures(&self, invited: Vec<crate::heartbeat::Invited>) {
        let listed: HashSet<String> = invited.iter().map(|i| i.session().to_string()).collect();
        // Held from the let-go to the queue: the deployer decides under this lock, and admits under it when it
        // serves an invitation itself, so this process never admits a session after the answer that let it go.
        let mut held = self.invitations.lock().unwrap();
        self.captures.drop_unlisted(&listed);
        // A progress report the leader would drop goes; a final stays -- it is this worker's answer, and the leader
        // still takes its reason.
        self.outbox
            .lock()
            .unwrap()
            .retain(|r| r.finished.is_some() || listed.contains(&r.session));

        let inv = &mut *held;
        if inv.listed != listed {
            inv.listed = listed;
            inv.changed = true;
        }
        inv.answered.retain(|s| inv.listed.contains(s));
        let queued = std::mem::take(&mut inv.queued);
        inv.queued = queued.into_iter().filter(|i| inv.wants(&i.session)).collect();
        let mut unreadable = Vec::new();
        for invitation in invited {
            match invitation {
                crate::heartbeat::Invited::Readable(invitation) => {
                    if inv.wants(&invitation.session) && !inv.queued.iter().any(|q| q.session == invitation.session) {
                        inv.queued.push(*invitation);
                    }
                }
                crate::heartbeat::Invited::Unreadable { session } if inv.wants(&session) => {
                    unreadable.push(crate::heartbeat::CaptureReport {
                        session,
                        worker_id: self.worker_id.clone(),
                        batches: Vec::new(),
                        // Not the parser's own words: they can quote a value from the entry, and an entry carries
                        // its source's credentials.
                        finished: Some(crate::heartbeat::CaptureEnd::Failed(format!(
                            "this worker's build ({}) cannot read this capture; a newer build can",
                            crate::version::RELEASE.unwrap_or(crate::version::VERSION)
                        ))),
                        sql: None,
                        avro: Vec::new(),
                    });
                }
                crate::heartbeat::Invited::Unreadable { .. } => {}
            }
        }
        drop(held);
        // Owed again on every beat until the leader has it; `owe_captures` keeps one report per session.
        self.owe_captures(unreadable);
    }

    /// Take the leader's holds, which are whole on every answer, for the deployer to hand the runtime.
    /// A change in which sources are held supersedes a ledger fetch in flight.
    fn follow_holds(&self, holds: Vec<heartbeat::Hold>) {
        let mut held = self.holds.lock().unwrap();
        let keys = |list: &[heartbeat::Hold]| list.iter().map(|h| h.key.clone()).collect::<HashSet<_>>();
        if keys(&held.listed) != keys(&holds) {
            held.epoch += 1;
        }
        held.listed = holds;
    }

    /// The leader has this worker's final report for `session`. This process forgets it, and the deployer decides
    /// to serve it nowhere after this while an answer that crossed the acknowledgment still lists it. A copy a
    /// runtime admitted before then keeps filling until an answer leaves the session out: that answer moves the
    /// listing, and the deployer's next turn tells the runtime.
    fn answered(&self, session: &str) {
        let mut held = self.invitations.lock().unwrap();
        if held.listed.contains(session) {
            held.answered.insert(session.to_string());
        }
        self.captures.sent(session);
    }

    /// Move what this worker finished serving, and what every session still filling has caught since it last said,
    /// into the outbox. A finished session stays admitted until the upload thread confirms the leader has the
    /// report -- released early, a leader still short of it repeats the invitation, finds the session gone, and
    /// admits a second driver for what the outbox is already carrying. Once confirmed, [`Supervisor::answered`]
    /// keeps an answer that crossed the confirmation from doing the same. A session still filling stays admitted
    /// regardless; a progress report never closes one.
    fn hand_back_captures(&self) {
        let mut reports = self.captures.finished(&self.worker_id);
        reports.extend(self.captures.progress(&self.worker_id));
        self.owe_captures(reports);
    }

    /// Leave finished captures for the upload thread, newest last.
    ///
    /// A full outbox drops the oldest. A leader unreachable for long enough to fill this has lost the earliest
    /// samples whatever happens, and refusing the newest would keep exactly the ones least worth keeping.
    ///
    /// A session not yet acknowledged is offered again every heartbeat (`Captures::finished`'s own contract,
    /// harmless to the leader since it keeps only the first answer), so its place here is replaced rather than
    /// added to -- otherwise the queue this bounds would fill with repeated copies of one session still waiting
    /// to go out, at the expense of every other session's report.
    fn owe_captures(&self, reports: Vec<crate::heartbeat::CaptureReport>) {
        if reports.is_empty() {
            return;
        }
        let mut outbox = self.outbox.lock().unwrap();
        merge_owed(&mut outbox, reports);
        trim_outbox(&mut outbox);
    }

    /// Hand the leader what this worker has finished capturing, on a schedule of its own.
    ///
    /// A capture is allowed an upload budget three orders of magnitude past a heartbeat interval, because it may
    /// legally carry that much. Spending it on the heartbeat's thread would make a healthy worker read as silent
    /// after three intervals, and spending it on the deployer's would hold up a redeploy behind an upload.
    fn upload_forever(&self) -> ! {
        // Consecutive batches this pass has answered with a failure. A leader rejecting every upload -- a
        // revoked credential, a proxy in the way -- is not a slow endpoint this bound waits out; it is grown the
        // way a redeploy's own retry is, so a rejection this fast does not spend every batch's worth of thread
        // spawns and requests on the interval a slow upload alone would justify.
        let mut failures: u32 = 0;
        loop {
            // The next batch alone, oldest first, rather than the whole outbox: what still waits behind it stays
            // under the cap this worker holds it to for as long as this batch is in flight. Taking everything at
            // once let a slow batch grow the outbox to twice its bound, since newly finished captures kept
            // arriving behind a pass that could spend minutes on four uploads before it looked again.
            let batch = next_upload_batch(&self.outbox);
            if batch.is_empty() {
                failures = 0;
                std::thread::sleep(IDLE_POLL);
                continue;
            }
            // An upload is allowed minutes, and a capture that finished behind a slow one has a deadline of its
            // own that the leader is already counting down -- so this batch's uploads run at once rather than
            // one after another.
            // Cloned out once for the whole batch: an upload runs for minutes, and holding the lock across one
            // would be the heartbeat waiting behind a capture -- the stall the separate schedules rule out.
            let client = self.client.lock().unwrap().clone();
            let mut failed = std::thread::scope(|scope| {
                let mut running = Vec::new();
                let mut unstarted = Vec::new();
                for report in &batch {
                    let client = client.clone();
                    // Fallible: at this host's thread limit a panicking spawn would take this schedule with it,
                    // and a worker whose only uploader has gone keeps every capture it ever finishes.
                    let started = std::thread::Builder::new()
                        .name("smeltr-capture-put".to_string())
                        .spawn_scoped(scope, move || match client.report_capture(report) {
                            Ok(()) => {
                                // Acknowledged: the session is the leader's now. Only once it is finished, though
                                // -- a progress report leaves the session open, and removing it here would stop a
                                // cold capture's own poll loop, which reads `open()` to decide whether to keep
                                // going.
                                if report.finished.is_some() {
                                    self.answered(&report.session);
                                }
                                None
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "worker.capture_handback_failed",
                                    subject = %report.session,
                                    error = %e,
                                    "could not hand a finished capture back, so it is offered again"
                                );
                                Some(report.clone())
                            }
                        });
                    match started {
                        Ok(handle) => running.push(handle),
                        Err(e) => {
                            tracing::warn!(
                                target: "worker.capture_upload_not_started",
                                subject = %report.session,
                                error = %e,
                                "could not start an upload, so this capture is offered again"
                            );
                            unstarted.push(report.clone());
                        }
                    }
                }
                let mut failed: Vec<_> = running
                    .into_iter()
                    .filter_map(|handle| handle.join().ok().flatten())
                    .collect();
                failed.append(&mut unstarted);
                failed
            });
            if failed.is_empty() {
                failures = 0;
                continue;
            }
            // Merged with whatever finished while this batch was in flight, which is where it came from, rather
            // than appended: a session this batch failed to deliver may have been offered again in the meantime
            // with a fuller answer, and concatenating the two would queue the stale one behind it instead of
            // replacing it. Capped again on the way back: a leader refusing every batch would otherwise add one
            // each pass.
            {
                let mut outbox = self.outbox.lock().unwrap();
                let arrived = std::mem::take(&mut *outbox);
                merge_owed(&mut failed, arrived);
                *outbox = failed;
                trim_outbox(&mut outbox);
            }
            failures = failures.saturating_add(1);
            std::thread::sleep(backoff(failures));
        }
    }

    /// Enrolls this worker again as a new one, where its leader no longer knows its credential and it still holds an
    /// enrollment code: an ephemeral worker forgotten while it was away, most often a container restarted in the pod
    /// whose state it kept. The new identity is kept first. Then the runtime ends the way any exit ends it, and the
    /// process exits for whatever supervises it to start it again as the new worker.
    ///
    /// A worker with no enrollment code, or one its enrollment code no longer enrolls, is left as it was -- refused,
    /// which it treats as a leader it cannot reach -- and tries again [`ENROLL_AGAIN_EVERY`] later. A revoked worker is
    /// not unknown, and never comes here.
    fn enroll_again(&self) {
        let Some(code) = self.config.enrollment_code.as_deref() else {
            return;
        };
        {
            let mut tried = self.enrolled_again_at.lock().unwrap();
            if tried.is_some_and(|at| at.elapsed() < ENROLL_AGAIN_EVERY) {
                return;
            }
            *tried = Some(Instant::now());
        }
        let worker_id = generate_worker_id();
        let trusting = self.trusting.lock().unwrap().clone();
        let token = match enroll(
            &self.config.leader_url,
            code,
            &worker_id,
            trusting.as_deref(),
            self.config.leader_trust_public,
        ) {
            Ok(token) => token,
            Err(refused) => {
                tracing::warn!(
                    target: "worker.not_enrolled_again",
                    error = %refused.message(),
                    retry_in = ?ENROLL_AGAIN_EVERY,
                    "the leader no longer knows this worker's credential, and the enrollment code it holds did not \
                     enroll it again"
                );
                return;
            }
        };
        // The id before the credential: the leader answers a credential only for the id it was issued to. A start
        // between the two holds a credential the leader does not know, so it enrolls again under yet another id,
        // and the one enrolled here is never heard from and is forgotten like any ephemeral worker gone.
        if let Err(e) = save_worker_id(&self.config.state_dir, &worker_id)
            .and_then(|()| save_credential(&self.config.state_dir, &token))
        {
            tracing::error!(
                target: "worker.not_kept_enrolled_again",
                error = %e,
                "this worker enrolled again and could not keep what it was given, so it will try again"
            );
            return;
        }
        tracing::info!(
            target: "worker.enrolled_again",
            subject = %worker_id,
            "the leader no longer knew this worker's credential, so it enrolled again as a new worker, and stops \
             to start as it"
        );
        self.exit_to(Exit::Draining);
        self.arrived.notify_all();
        if !self.exit_reaches(Exit::Notice, EXIT_BUDGET) {
            tracing::warn!(
                target: "worker.exit_not_drained",
                budget = ?EXIT_BUDGET,
                "this worker is ending before its runtime was confirmed stopped"
            );
        }
        // No notice: the leader has not heard from the new worker, and has already forgotten the old one.
        crate::logs::stop();
        std::process::exit(ENROLLED_AGAIN_EXIT);
    }

    /// The configured answer to a leader that has gone. Noticing is the heartbeat's; doing it is the deployer's,
    /// because stopping joins threads and this thread may not wait on them.
    fn consider_offline_stop(&self) {
        let Some(limit) = self.config.stop_after_unreachable_secs.map(Duration::from_secs) else {
            return;
        };
        let overdue = self
            .last_contact
            .lock()
            .unwrap()
            .is_some_and(|last| last.elapsed() > limit);
        if overdue && self.commit.lock().unwrap().is_some() {
            self.offline_stop.store(true, Ordering::Relaxed);
        }
    }

    // -- the discovery schedule ------------------------------------------------

    /// Make the clone and lookup store this worker starts with sound before anything reads them. Both are caches of
    /// what the leader holds: what does not read is removed, and fetched again as what was never fetched. Nothing
    /// runs from either until a fetch has succeeded, so removing them costs a worker that cannot reach its leader
    /// nothing it could run.
    fn check_caches(&self) {
        if is_bare_clone(&self.repo_dir)
            && let Err(e) = fetched_reads(&self.repo_dir)
        {
            tracing::warn!(
                target: "worker.clone_damaged",
                path = %self.repo_dir.display(),
                error = %e,
                "this worker's clone does not read, so it is removed and cloned again"
            );
            if let Err(e) = std::fs::remove_dir_all(&self.repo_dir) {
                tracing::warn!(
                    target: "worker.clone_not_removed",
                    path = %self.repo_dir.display(),
                    error = %e,
                    "this worker's clone does not read and could not be removed"
                );
            }
        }
        for (oid, removed) in crate::lookup::store::remove_damaged(&self.repo_dir) {
            match removed {
                Ok(()) => tracing::warn!(
                    target: "worker.lookup_file_damaged",
                    subject = %oid,
                    "a stored lookup file's bytes are not the ones its name says, so it is removed and fetched again"
                ),
                Err(e) => tracing::warn!(
                    target: "worker.lookup_file_damaged",
                    subject = %oid,
                    error = %e,
                    "a stored lookup file's bytes are not the ones its name says, and it could not be removed"
                ),
            }
        }
    }

    fn discover_forever(&self) -> ! {
        self.check_caches();
        // Said once per cause, not once per try: a fetch that keeps failing the same way is one fact.
        let mut failing: Option<String> = None;
        loop {
            // What the leader deploys is this worker's to follow only once it has checked in. After that this schedule is its own:
            // a heartbeat that fails cancels no discovery, where before they were the same turn of one loop.
            if self.contacted() {
                self.supervising.lock().unwrap().attempted_ms = Some(now_ms());
                match fetch_deployed(&self.repo_dir, &self.config.leader_url, &self.token, &self.git_roots) {
                    Ok(head) => {
                        if failing.take().is_some() {
                            tracing::info!(
                                target: "worker.fetch_recovered",
                                "fetched what the leader deploys again"
                            );
                        }
                        // Distinct from having looked: a worker whose fetches all fail keeps trying and learns
                        // nothing, and only one of these two moves while that is true.
                        let mut supervising = self.supervising.lock().unwrap();
                        supervising.discovered_ms = Some(now_ms());
                        supervising.desired_commit = Some(head.clone());
                        drop(supervising);
                        self.publish(head);
                    }
                    Err(e) => {
                        let cause = without_timings(&e);
                        if failing.as_ref() != Some(&cause) {
                            tracing::warn!(
                                target: "worker.fetch_failed",
                                error = %e,
                                "could not fetch what the leader deploys, so this worker keeps running what it has"
                            );
                            failing = Some(cause);
                        }
                    }
                }
            }
            std::thread::sleep(self.config.heartbeat_interval);
        }
    }

    /// Record `commit` as the newest thing seen, and wake the deployer. Only a commit that differs from the last
    /// one observed counts as a new observation: re-reporting the same head every interval would be a stream of
    /// targets that change nothing.
    fn publish(&self, commit: String) {
        let mut target = self.target.lock().unwrap();
        if target.as_ref().is_some_and(|t| t.commit == commit) {
            return;
        }
        let generation = target.as_ref().map_or(1, |t| t.generation + 1);
        *target = Some(Target { generation, commit });
        drop(target);
        self.arrived.notify_all();
    }

    // -- the deployment schedule -----------------------------------------------

    fn deploy_forever(&self) -> ! {
        let mut running: Option<child::RuntimeHandle> = None;
        let mut inbox = Arc::new(child::RuntimeInbox::default());
        // The observation this thread last attempted, and when it may attempt that one again. A newer
        // observation is always a reason to act; the same one only once its retry has come.
        let mut acted: Option<u64> = None;
        let mut retry_at: Option<Instant> = None;
        let mut failures: u32 = 0;
        // When this target was first refused. A commit can name a secret version a moment before the version reaches
        // this worker, so a refusal is only worth a warning once it has lasted.
        let mut refused_since: Option<Instant> = None;
        let mut incarnation: u64 = 0;
        // What the running runtime was built from. A later commit whose group subtree and `shared/` are both
        // unchanged runs the same configuration, so it is a commit to represent rather than one to restart for.
        let mut content: Option<ContentId> = None;
        // The commit the last runtime that started was built from, and what it was built of. A commit that will
        // not start is answered by starting this again, so it does not cost the deployment that was working.
        let mut last_good: Option<(String, ContentId)> = None;
        // A runtime whose exit this worker could not confirm. It may still be holding a source, so nothing takes
        // its place; saying so and going on heartbeating is all that is left until an operator has seen to it.
        let mut blocked = false;
        loop {
            self.absorb(&inbox);
            self.drain_captured(&inbox);
            // Before any invitation is forwarded: a runtime about to be stopped for good is handed nothing more.
            if self.exiting() {
                self.end_for_exit(running, &inbox, blocked);
            }
            let wedged = self.route_invitations(running.as_mut())
                || self.mirror_to(running.as_mut())
                || self.route_holds(running.as_mut())
                || self.route_backlog(running.as_mut());

            // A control pipe that has stopped taking frames: every capture forwarded to it from here on fails
            // the same way. `send_within`'s caller owes the tree a kill, and this is where that is paid -- the
            // tree may still be running, and ending it is what lets a replacement take its place.
            if wedged && let Some(mut handle) = running.take() {
                tracing::warn!(
                    target: "worker.runtime_unresponsive",
                    subject = %handle.commit,
                    "a runtime stopped taking frames on its control pipe, so it is being ended"
                );
                match self.end(&mut handle, &inbox) {
                    Ok(()) => {
                        self.absorb(&inbox);
                        self.drain_captured(&inbox);
                        content = None;
                        self.forget_runtime();
                        failures = failures.saturating_add(1);
                        retry_at = Some(Instant::now() + backoff(failures));
                    }
                    Err(()) => {
                        blocked = true;
                        running = Some(handle);
                    }
                }
            }

            // A runtime that ended on its own. Confirmed dead is retired like any other ending; unconfirmed is
            // not -- a descendant may still be consuming, so it blocks deployment the same as a kill this worker
            // could not confirm, rather than being read as still running.
            if let Some(mut handle) = running.take() {
                match handle.exited() {
                    child::Exit::Running => running = Some(handle),
                    child::Exit::Unconfirmed => {
                        blocked = true;
                        running = Some(handle);
                    }
                    child::Exit::Confirmed => {
                        tracing::warn!(
                            target: "worker.runtime_exited",
                            subject = %handle.commit,
                            "a runtime process ended on its own, so this worker is running nothing"
                        );
                        content = None;
                        blocked = false;
                        self.absorb(&inbox);
                        self.drain_captured(&inbox);
                        self.forget_runtime();
                        failures = failures.saturating_add(1);
                        retry_at = Some(Instant::now() + backoff(failures));
                    }
                }
            }

            if self.take_offline_stop()
                && let Some(mut handle) = running.take()
            {
                tracing::warn!(
                    target: "worker.stopped_unreachable",
                    limit = ?self.config.stop_after_unreachable_secs,
                    "stopping the deployment: the leader has been unreachable for longer than this worker's \
                     stop_after_unreachable_secs"
                );
                match self.end(&mut handle, &inbox) {
                    Ok(()) => {
                        self.absorb(&inbox);
                        self.drain_captured(&inbox);
                        content = None;
                        self.forget_runtime();
                        // The target is worth acting on again, but not until the leader is back: the gate below
                        // is what holds it, rather than this forgetting that it was ever deployed.
                        acted = None;
                        retry_at = None;
                    }
                    Err(()) => {
                        blocked = true;
                        running = Some(handle);
                    }
                }
            }

            // Asked by an operator. The same target starts again below, in a fresh runtime.
            if self.restart_due.swap(false, Ordering::Relaxed) {
                if blocked {
                    tracing::warn!(
                        target: "worker.restart_blocked",
                        "a restart was asked for, but this worker's runtime could not be confirmed dead, so nothing \
                         restarts"
                    );
                }
                if !blocked && let Some(mut handle) = running.take() {
                    tracing::info!(target: "worker.restarting", subject = %handle.commit, "restarting the deployment");
                    self.phase("stopping");
                    match self.end(&mut handle, &inbox) {
                        Ok(()) => {
                            self.absorb(&inbox);
                            self.drain_captured(&inbox);
                            content = None;
                            self.forget_runtime();
                        }
                        Err(()) => {
                            blocked = true;
                            running = Some(handle);
                        }
                    }
                }
                acted = None;
                retry_at = None;
            }

            // Stopped for an outage stays stopped for the outage. Nothing is started while this holds, however
            // new the target, because what the setting is about is not processing without a leader to answer to.
            if self.out_of_contact() {
                self.phase("offline");
                std::thread::sleep(IDLE_POLL);
                continue;
            }

            self.phase(match (blocked, running.is_some()) {
                (true, _) => "blocked",
                (false, true) => "running",
                (false, false) => "idle",
            });

            let Some(target) = self.next_target(acted, retry_at) else {
                continue;
            };
            if acted != Some(target.generation) {
                // A different observation than the one this backoff was counted against: whatever made the
                // last target fail says nothing about this one, so it gets a first attempt rather than
                // inheriting how far the last one had backed off.
                failures = 0;
                refused_since = None;
            }
            acted = Some(target.generation);
            retry_at = None;

            // A newer target is not what unblocks this: what blocks it is a process that may still be consuming,
            // and starting a second one beside it is the thing that must not happen.
            if blocked {
                continue;
            }

            if running.as_ref().is_some_and(|h| h.commit == target.commit) {
                // Already running exactly this. Said again because an unrelated commit may have moved what this
                // worker reports since, and rolling back to what is running is not being behind -- nor is it a
                // failure to go on reporting, or one to go on backing off from.
                *self.commit.lock().unwrap() = Some(target.commit.clone());
                self.note_failure(None);
                failures = 0;
                refused_since = None;
                hold_running(&self.repo_dir, &target.commit);
                continue;
            }

            let arriving = match unpanicked(|| content_id(&self.repo_dir, &target.commit, &self.config.group_id)) {
                Ok(arriving) => arriving,
                Err(why) => {
                    tracing::warn!(
                        target: "worker.commit_unreadable",
                        subject = %target.commit,
                        error = %why,
                        "a commit could not be read, so this worker keeps running what it has"
                    );
                    self.note_failure(Some(&why));
                    failures = failures.saturating_add(1);
                    retry_at = Some(Instant::now() + backoff(failures));
                    continue;
                }
            };
            if running.is_some() && content.as_ref() == Some(&arriving) {
                // Same configuration under a new commit: what this worker represents moves, and nothing restarts.
                *self.commit.lock().unwrap() = Some(target.commit.clone());
                last_good = Some((target.commit.clone(), arriving));
                self.note_failure(None);
                failures = 0;
                refused_since = None;
                hold_running(&self.repo_dir, &target.commit);
                continue;
            }

            // Every refusal comes before anything stops (design doc, Control plane). The runtime loads this
            // commit again in its own process, but that is past the teardown; this is the copy that decides
            // whether there is one, so a commit that will not load never costs a deployment that was working.
            let validated = match unpanicked(|| self.validate(&target.commit)) {
                Ok(validated) => validated,
                Err(why) => {
                    let since = *refused_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= REFUSAL_WARN_AFTER {
                        tracing::warn!(
                            target: "worker.commit_refused",
                            subject = %target.commit,
                            error = %why,
                            "a commit was fetched but will not run, so this worker keeps running what it has"
                        );
                    } else {
                        tracing::info!(
                            target: "worker.commit_refused",
                            subject = %target.commit,
                            error = %why,
                            "a commit was fetched but will not run yet, so this worker keeps running what it has \
                             and tries again"
                        );
                    }
                    self.note_failure(Some(&why));
                    failures = failures.saturating_add(1);
                    retry_at = Some(Instant::now() + backoff(failures));
                    continue;
                }
            };

            // Nothing may start while something might still be consuming, so the old runtime is confirmed dead
            // before the new one is asked to exist.
            if let Some(mut handle) = running.take() {
                self.phase("stopping");
                if self.end(&mut handle, &inbox).is_err() {
                    self.note_failure(Some(
                        "a runtime could not be confirmed dead, so nothing will start in its place",
                    ));
                    blocked = true;
                    running = Some(handle);
                    continue;
                }
            }
            // Everything the runtime that has just stopped still had to say, including what it committed during
            // its drain, before the inbox it said it into is let go.
            self.absorb(&inbox);
            self.drain_captured(&inbox);
            self.forget_runtime();

            // A process asked to end while the old runtime stopped starts nothing it would only stop again.
            if self.exiting() {
                continue;
            }
            incarnation += 1;
            inbox = Arc::new(child::RuntimeInbox::default());
            let mut begun = self.begin(&target, &validated, incarnation, &inbox);
            let mut started = (target.commit.clone(), arriving, validated);
            let mut restored = false;
            if matches!(begun, Begun::Refused)
                && let Some((was, was_content)) = last_good.clone().filter(|(was, _)| *was != target.commit)
                && !self.exiting()
            {
                // Once, and only for a commit that started here before. What the leader is told is still why the
                // target did not start, since the target is what this worker is not running.
                let why = self.supervising.lock().unwrap().failure.clone();
                tracing::warn!(
                    target: "worker.restoring",
                    subject = %was,
                    refused = %target.commit,
                    "a commit would not start, so this worker starts again the one it ran before"
                );
                self.absorb(&inbox);
                self.drain_captured(&inbox);
                self.forget_runtime();
                match unpanicked(|| self.validate(&was)) {
                    Ok(again) => {
                        incarnation += 1;
                        inbox = Arc::new(child::RuntimeInbox::default());
                        let restoring = Target {
                            generation: target.generation,
                            commit: was.clone(),
                        };
                        begun = self.begin(&restoring, &again, incarnation, &inbox);
                        started = (was, was_content, again);
                        restored = true;
                    }
                    Err(e) => tracing::warn!(
                        target: "worker.restore_refused",
                        subject = %was,
                        error = %e,
                        "the commit this worker ran before will not run either, so it is running nothing"
                    ),
                }
                self.note_failure(why.as_deref());
            }
            let (commit, arriving, validated) = started;
            match begun {
                Begun::Started(handle) => {
                    // What this process itself may have opened a capture against, it no longer may. Its sources
                    // are all it needs: this process's own registry labels no subject.
                    self.captures.set_deployed(&Arc::new(crate::metrics::Subjects {
                        sources: validated.sources.clone(),
                        ..Default::default()
                    }));
                    {
                        let mut supervising = self.supervising.lock().unwrap();
                        supervising.incarnation = Some(incarnation);
                        supervising.listens = validated.listening.clone();
                    }
                    // It holds nothing yet. Its first turn tells it what the leader holds here, each ledger
                    // fetched afresh: the last runtime's reports have moved them since.
                    self.holds.lock().unwrap().restarted();
                    running = Some(handle);
                    content = Some(arriving.clone());
                    last_good = Some((commit.clone(), arriving));
                    *self.commit.lock().unwrap() = Some(commit.clone());
                    hold_running(&self.repo_dir, &commit);
                    // What the runtime that started names and nothing else: a version it does not quote is of no
                    // further use here, and a worker that kept everything it had ever been sent would hold a
                    // growing pile of dead credentials.
                    self.secrets.lock().unwrap().keep_only(&validated.named);
                    // The runtime it replaced is gone, so nothing maps a file this one does not name.
                    crate::lookup::store::keep_only(&self.repo_dir, &validated.lookup_files);
                    // Nor listens on an address this one does not.
                    self.receiver_sockets.lock().unwrap().keep_only(&validated.listens);
                    if restored {
                        // The target is not retried on a timer: a commit that would not start fails the same way
                        // next time, and each try would stop the deployment that works. A new target or a restart
                        // is what tries again.
                        failures = failures.saturating_add(1);
                    } else {
                        self.note_failure(None);
                        failures = 0;
                        refused_since = None;
                    }
                }
                Begun::Refused => {
                    running = None;
                    content = None;
                    self.forget_runtime();
                    // Nothing runs, so nothing may look as if it listens: a sender is refused at once rather than
                    // left waiting in a queue nobody takes from. The retry binds again.
                    self.receiver_sockets.lock().unwrap().keep_only(&[]);
                    failures = failures.saturating_add(1);
                    // The same target again once the backoff has run: a broker that was not up and a secret that
                    // had not arrived are both answered by asking again, not by waiting for another deploy.
                    retry_at = Some(Instant::now() + backoff(failures));
                }
                Begun::Unconfirmed(handle) => {
                    content = None;
                    self.forget_runtime();
                    blocked = true;
                    running = Some(handle);
                }
            }
        }
    }

    /// Start a runtime for `target` and wait for it to say the startup steps completed. A process that exists is
    /// not a deployment that runs, so what is waited for is readiness and never liveness.
    fn begin(
        &self,
        target: &Target,
        validated: &Validated,
        incarnation: u64,
        inbox: &Arc<child::RuntimeInbox>,
    ) -> Begun {
        tracing::info!(target: "worker.redeploying", subject = %target.commit, "redeploying to a new commit");
        self.phase("starting");
        let spec = child::StartSpec {
            state_dir: self.config.state_dir.clone(),
            repo_dir: self.repo_dir.clone(),
            commit: target.commit.clone(),
            group_id: self.config.group_id.clone(),
            thread_limit: self.config.thread_limit,
            memory: self.config.memory,
            worker_id: self.worker_id.clone(),
            // Only what this commit quotes. A runtime is not handed credentials its own bundle does not name,
            // and never the pile of every version this worker has been sent since it started.
            secrets: self.secrets.lock().unwrap().deliveries_for(&validated.named),
            incarnation,
        };
        let mut sockets = self.receiver_sockets.lock().unwrap();
        sockets.hold(&validated.listens);
        // Only what this target serves: a socket a runtime adopts stays open for its life.
        let started = child::RuntimeHandle::start(spec, inbox, &sockets, &validated.listens);
        drop(sockets);
        let handle = match started {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(
                    target: "worker.runtime_not_started",
                    subject = %target.commit,
                    error = %e,
                    "could not start a runtime process, so this worker is running nothing"
                );
                self.note_failure(Some(&format!("could not start a runtime process: {e}")));
                return Begun::Refused;
            }
        };
        let deadline = Instant::now() + STARTUP_BUDGET;
        while Instant::now() < deadline {
            if inbox.ready.load(Ordering::Relaxed) {
                return Begun::Started(handle);
            }
            if let Some(reason) = inbox.failed.lock().unwrap().clone() {
                tracing::warn!(
                    target: "worker.commit_refused",
                    subject = %target.commit,
                    error = %reason,
                    "a commit was fetched but will not run, so this worker keeps running what it has"
                );
                self.note_failure(Some(&reason));
                // Ended even though it said so itself: a process that refused its target has still been started,
                // and what it may have spawned is this worker's to be sure of before anything replaces it.
                return self.ended(handle, inbox);
            }
            if inbox.exited.load(Ordering::Relaxed) {
                break;
            }
            // A process asked to end does not wait out a start only to stop what it started.
            if self.exiting() {
                return self.ended(handle, inbox);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        tracing::warn!(
            target: "worker.runtime_not_ready",
            subject = %target.commit,
            budget = ?STARTUP_BUDGET,
            "a runtime did not report itself ready in time, so it is being ended"
        );
        self.note_failure(Some("a runtime did not report itself ready in time"));
        self.ended(handle, inbox)
    }

    /// End an attempt that will not run. A startup that cannot be confirmed dead blocks exactly as a running
    /// deployment that cannot be does: it may still be holding a source, whatever it said about itself.
    fn ended(&self, mut handle: child::RuntimeHandle, inbox: &child::RuntimeInbox) -> Begun {
        match self.end(&mut handle, inbox) {
            Ok(()) => Begun::Refused,
            Err(()) => Begun::Unconfirmed(handle),
        }
    }

    /// End a runtime and confirm it. `Err` is a process the operating system would not confirm dead, which is the
    /// one state this worker cannot deploy out of: it says so and keeps heartbeating.
    fn end(&self, handle: &mut child::RuntimeHandle, inbox: &child::RuntimeInbox) -> Result<(), ()> {
        match handle.stop(GRACEFUL_STOP_BUDGET, HARD_STOP_BUDGET, inbox) {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!(
                    target: "worker.runtime_unkillable",
                    subject = %handle.commit,
                    error = %e,
                    "a runtime could not be confirmed dead, so nothing will be started in its place until it is: \
                     this needs an operator, since a replacement beside a reader that may still be consuming is \
                     two consumers on one source"
                );
                Err(())
            }
        }
    }

    /// Take what the runtime has said since this was last called, and hold it for the heartbeat. Its counts and
    /// its logs are added to whatever the leader has not taken yet; what its threads are doing is replaced, since
    /// that is a description of now.
    fn absorb(&self, inbox: &child::RuntimeInbox) {
        let Some(mut arrived) = inbox.report.lock().unwrap().take() else {
            return;
        };
        // Resolved here, against the names the runtime that wrote them sent alongside. A sample labels its source
        // by position, and a position is only meaningful against the topology it was written under: held until
        // the leader takes it, it may outlive that runtime and be read beside its replacement's names.
        resolve_subject_labels(&mut arrived.metrics, &arrived.subjects);
        // Totals for the metrics endpoint, which reads the whole worker from this process.
        self.registry.mirror(
            arrived
                .metrics
                .iter()
                .cloned()
                .filter_map(heartbeat::MetricSampleDto::into_sample),
        );
        let mut held = self.runtime.lock().unwrap();
        *held = Some(match held.take() {
            Some(previous) => previous.followed_by(arrived),
            None => arrived,
        });
    }

    /// What the metrics endpoint answers a scrape with: this process's registry, its subjects named as the heartbeat
    /// names them, and the runtime's totals it mirrors. A counter both processes write, such as a capture's, reads as
    /// their sum, and a gauge both set as this process's own.
    fn scrape(&self) -> String {
        let mut own = self.registry.snapshot();
        if let Some(held) = self.runtime.lock().unwrap().as_ref() {
            for sample in &mut own {
                held.subjects.resolve(&mut sample.key.labels);
            }
        }
        crate::metrics::exposition::render(own.into_iter().chain(self.registry.snapshot_mirrored()))
    }

    /// Stop reporting a runtime that is no longer there. A stale report would have the leader believe threads are
    /// running for the life of this process, and a stale incarnation would have it believe a dead process exists.
    fn forget_runtime(&self) {
        if let Some(held) = self.runtime.lock().unwrap().as_mut() {
            // Its threads are gone, so nothing is reported about them. What it counted and said before it went is
            // still owed to the leader, and is carried by the beats that follow.
            *held = child::RuntimeReport {
                metrics: std::mem::take(&mut held.metrics),
                logs: std::mem::take(&mut held.logs),
                progress: std::mem::take(&mut held.progress),
                streams: held.streams.take(),
                boot_id: held.boot_id,
                ..Default::default()
            };
        }
        self.registry.forget_mirrored();
        *self.commit.lock().unwrap() = None;
        {
            let mut supervising = self.supervising.lock().unwrap();
            supervising.incarnation = None;
            supervising.listens.clear();
        }
        self.captures.set_deployed(&Arc::default());
    }

    /// Whether this process has been asked to end.
    fn exiting(&self) -> bool {
        *self.exit.lock().unwrap() >= Exit::Draining
    }

    /// Moves this process on its way out, never back.
    fn exit_to(&self, step: Exit) {
        let mut exit = self.exit.lock().unwrap();
        if *exit < step {
            *exit = step;
            self.exit_changed.notify_all();
        }
    }

    /// Waits until this process is at `step` on its way out, for at most `budget`. `false` is the budget spent.
    fn exit_reaches(&self, step: Exit, budget: Duration) -> bool {
        let deadline = Instant::now() + budget;
        let mut exit = self.exit.lock().unwrap();
        while *exit < step {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            exit = self.exit_changed.wait_timeout(exit, left).unwrap().0;
        }
        true
    }

    /// Ends this worker: the deployer ends the runtime, then the heartbeat tells the leader the worker is stopping.
    /// Returns once both are done, or once each has had as long as it gets.
    ///
    /// The notice goes out on the heartbeat's own thread, behind whatever beat it is already sending: a beat that
    /// landed after it would say the worker was back.
    fn stop_for_exit(&self) {
        self.exit_to(Exit::Draining);
        // Woken rather than left to its next poll, so a deployer waiting on a target sees this at once.
        self.arrived.notify_all();
        if !self.exit_reaches(Exit::Notice, EXIT_BUDGET) {
            tracing::warn!(
                target: "worker.exit_not_drained",
                budget = ?EXIT_BUDGET,
                "this worker is ending before its runtime was confirmed stopped"
            );
            self.exit_to(Exit::Notice);
        }
        if !self.exit_reaches(Exit::Told, NOTICE_BUDGET) {
            tracing::warn!(
                target: "worker.exit_not_told",
                budget = ?NOTICE_BUDGET,
                "this worker is ending before its leader was told it is stopping, so the leader will report it silent"
            );
        }
    }

    /// The deployer's half of [`Self::stop_for_exit`]: end `running` the way a redeploy ends it, then start
    /// nothing more. Never returns; the thread that asked ends the process.
    ///
    /// `blocked` is a runtime whose kill already went unconfirmed. Asking it again buys nothing, so it is left for
    /// the process's own end, held here rather than dropped: a drop is one more kill, on the way out of a process
    /// that is ending anyway.
    fn end_for_exit(&self, mut running: Option<child::RuntimeHandle>, inbox: &child::RuntimeInbox, blocked: bool) -> ! {
        if !blocked && let Some(handle) = running.as_mut() {
            self.phase("stopping");
            if self.end(handle, inbox).is_ok() {
                self.absorb(inbox);
                self.drain_captured(inbox);
                self.forget_runtime();
            }
        }
        self.phase("stopped");
        tracing::info!(
            target: "worker.stopped_for_exit",
            "this worker's runtime has stopped, and nothing more will start before the process ends"
        );
        self.exit_to(Exit::Notice);
        loop {
            std::thread::park();
        }
    }

    /// Hand the runtime what this process recorded and said since it last did, once per heartbeat, so a metrics or
    /// logs source there reads the whole worker. The metrics cursor moves, and the logs leave, once the frame is on
    /// the pipe; with no runtime, the metrics wait for one and the logs are not kept.
    ///
    /// `true` is a control pipe that would not take the frame, as for [`Supervisor::route_invitations`].
    fn mirror_to(&self, running: Option<&mut child::RuntimeHandle>) -> bool {
        let mut logs_mirror = self.logs_mirror.lock().unwrap();
        let Some(handle) = running else {
            *logs_mirror = None;
            return false;
        };
        let tap = logs_mirror.get_or_insert_with(crate::logs::tap);
        if !self.mirror_due.swap(false, Ordering::Relaxed) {
            return false;
        }
        let peeked = self.registry.peek_delta(crate::metrics::SUBSCRIBER_MIRROR);
        // As many as a report carries up, and no more than their share of the frame: a frame refused for its size
        // would be refused every time it was built, and the deploy loop would end the runtime over it.
        let mut said = tap.drain(heartbeat::LOGS_PER_HEARTBEAT);
        let mut logs = Vec::with_capacity(said.len());
        let mut bytes = 0;
        for event in &said {
            let log = heartbeat::LogDto::from(event);
            bytes += serde_json::to_vec(&log).map_or(0, |encoded| encoded.len());
            if bytes > child::MIRROR_LOG_BYTES && !logs.is_empty() {
                break;
            }
            logs.push(log);
        }
        tap.put_back(said.split_off(logs.len()));
        match handle.mirror(
            peeked.iter().cloned().map(heartbeat::MetricSampleDto::from).collect(),
            logs,
        ) {
            Ok(()) => {
                self.registry.commit_delta(crate::metrics::SUBSCRIBER_MIRROR, &peeked);
                false
            }
            Err(e) => {
                tap.put_back(said);
                tracing::warn!(
                    target: "worker.not_mirrored",
                    error = %e,
                    "could not hand the runtime this process's metrics and logs, so it is being ended"
                );
                true
            }
        }
    }

    /// Hand each waiting invitation to the runtime, or serve it here when none is running. A worker with nothing
    /// deployed still answers a capture -- that is what lets an operator read a source before anything runs
    /// against it. A running runtime is first told every session the leader lists, when that has changed. A
    /// session this process still holds stays here, and one the leader no longer wants from this worker is not
    /// served. Each is decided under the invitations lock. One served here is admitted under it; one forwarded is
    /// written after it is released, so it can reach the runtime after the leader let its session go, and the next
    /// listing that leaves the session out ends it there.
    ///
    /// `true` is a control pipe that would not take a frame: `send_within`'s own obligation is that its caller
    /// owes the tree a kill, since the writer may be stuck on that frame until the pipe closes. Every other
    /// caller reaches `kill_tree` by dropping the handle or stopping on its own deadline; this one has no handle
    /// to drop, so it says so and leaves ending the runtime to the deploy loop.
    fn route_invitations(&self, mut running: Option<&mut child::RuntimeHandle>) -> bool {
        let (waiting, listing) = {
            let mut held = self.invitations.lock().unwrap();
            let waiting = std::mem::take(&mut held.queued);
            let listing = std::mem::take(&mut held.changed).then(|| {
                let mut sessions: Vec<String> = held.listed.iter().cloned().collect();
                sessions.sort();
                sessions
            });
            (waiting, listing)
        };
        let mut wedged = false;
        // First, so the runtime lets go of what the leader dropped before it is handed anything new, and a pipe that
        // refused this is handed nothing.
        if let (Some(handle), Some(sessions)) = (running.as_deref_mut(), listing)
            && let Err(e) = handle.listed(sessions)
        {
            tracing::warn!(
                target: "worker.capture_listing_not_forwarded",
                error = %e,
                "could not tell the runtime which captures the leader still lists, so it is being ended"
            );
            wedged = true;
        }
        let mut requeue = Vec::new();
        for invitation in waiting {
            // Once one frame has gone unanswered, the deployment it was meant for is on its way out, but this
            // worker still reports its sources deployed until the deploy loop ends it -- serving the rest of the
            // batch now, the way a worker with nothing running would, would mark them covered by a source
            // nothing is reading. Left queued for the round after that deployment is gone.
            if wedged {
                requeue.push(invitation);
                continue;
            }
            let session = invitation.session.clone();
            // Decided under the lock the heartbeat lets go under and the upload thread marks an answer under. Held
            // while this process admits, so what it serves itself is never served after either. A forward is
            // written after it is released and admitted on the runtime's own thread, so it can land after either;
            // the next listing that leaves its session out ends it there.
            let held = self.invitations.lock().unwrap();
            if !held.wants(&session) {
                continue;
            }
            match running.as_deref_mut() {
                Some(handle) => {
                    // Still held here: begun before the runtime started and still filling on a source it does not
                    // read, or finished and waiting for its report to be sent. The leader lists it until it has
                    // that report, and forwarding it would start a second driver under this worker's id.
                    let here = self.captures.holds(&session);
                    drop(held);
                    if here {
                        continue;
                    }
                    if let Err(e) = handle.capture(invitation) {
                        tracing::warn!(
                            target: "worker.capture_not_forwarded",
                            subject = %session,
                            error = %e,
                            "could not hand a capture to the runtime, so it is not being served"
                        );
                        wedged = true;
                    }
                }
                None => {
                    let secrets = self.secrets.lock().unwrap().clone();
                    serve_capture(&self.captures, invitation, &self.registry, &secrets);
                }
            }
        }
        if !requeue.is_empty() {
            let mut held = self.invitations.lock().unwrap();
            let inv = &mut *held;
            for invitation in requeue {
                if inv.wants(&invitation.session) && !inv.queued.iter().any(|q| q.session == invitation.session) {
                    inv.queued.push(invitation);
                }
            }
        }
        wedged
    }

    /// Tell the running runtime which single-worker sources it holds, when that has changed or a grant it did not
    /// make is due again ([`Holds::plan`]). A frame that grants a source waits for a thread of its own to fetch its
    /// ledger, and is sent on the first turn after; one that grants none goes at once.
    ///
    /// `true` is a control pipe that would not take the frame, as for [`Supervisor::route_invitations`].
    fn route_holds(&self, running: Option<&mut child::RuntimeHandle>) -> bool {
        let Some(handle) = running else { return false };
        let (holding, applied) = self
            .runtime
            .lock()
            .unwrap()
            .as_ref()
            .map(|r| (r.holding.clone(), r.holds_applied))
            .unwrap_or_default();
        let now = Instant::now();
        let (seq, frame) = {
            let mut holds = self.holds.lock().unwrap();
            let fetched = holds.fetched.take();
            let frame = match fetched {
                Some((epoch, frame)) if epoch == holds.epoch => frame,
                stale => {
                    // Superseded while it was fetched: what it fetched is not this epoch's to hand over.
                    for (holding, _) in stale.into_iter().flat_map(|(_, frame)| frame) {
                        holding.discard_ledger();
                    }
                    if holds.fetching == Some(holds.epoch) {
                        return false;
                    }
                    let Some(plan) = holds.plan(&holding, applied, now) else {
                        return false;
                    };
                    if plan.iter().any(|(_, offer)| *offer == Offer::Grant) {
                        holds.fetching = Some(holds.epoch);
                        self.fetch_ledgers(holds.epoch, plan);
                        return false;
                    }
                    plan.into_iter()
                        .map(|(hold, offer)| {
                            (
                                held::Holding {
                                    hold,
                                    ledger_file: None,
                                },
                                offer,
                            )
                        })
                        .collect()
                }
            };
            holds.seq += 1;
            (holds.seq, frame)
        };
        let holding: Vec<held::Holding> = frame.iter().map(|(h, _)| h.clone()).collect();
        match handle.holds(seq, holding) {
            Ok(()) => {
                self.holds.lock().unwrap().sent(seq, &frame, now);
                false
            }
            Err(e) => {
                tracing::warn!(
                    target: "worker.holds_not_forwarded",
                    error = %e,
                    "could not tell the runtime which sources the leader holds for it, so it is being ended"
                );
                true
            }
        }
    }

    /// Tell the running runtime whether the progress waiting here for the leader is past its bound, when that has
    /// changed or the runtime has not been told. Held sources there read nothing new while it is, which is what
    /// bounds this backlog through an outage: their commits still arrive, and are kept.
    ///
    /// `true` is a control pipe that would not take the frame, as for [`Supervisor::route_invitations`].
    fn route_backlog(&self, running: Option<&mut child::RuntimeHandle>) -> bool {
        let weight = self
            .runtime
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |r| held::backlog(&r.progress));
        let full = {
            let mut holds = self.holds.lock().unwrap();
            let full = held::backlogged(holds.backlogged, weight);
            if full != holds.backlogged {
                holds.backlogged = full;
                if full {
                    tracing::warn!(
                        target: "worker.progress_backlogged",
                        weight,
                        "more progress is waiting for the leader than this worker keeps, so its held sources read \
                         nothing new until the leader takes it"
                    );
                } else {
                    tracing::info!(
                        target: "worker.progress_drained",
                        weight,
                        "the progress waiting for the leader has drained, so this worker's held sources read again"
                    );
                }
            }
            if holds.told_backlogged == Some(full) {
                return false;
            }
            full
        };
        let Some(handle) = running else { return false };
        match handle.backlogged(full) {
            Ok(()) => {
                self.holds.lock().unwrap().told_backlogged = Some(full);
                false
            }
            Err(e) => {
                tracing::warn!(
                    target: "worker.backlog_not_forwarded",
                    error = %e,
                    "could not tell the runtime whether its progress is backlogged, so it is being ended"
                );
                true
            }
        }
    }

    /// Fetch the ledger of each source `plan` grants, on a thread of its own, and leave the frame for the deployer.
    /// A ledger that could not be fetched leaves its source kept rather than granted, and so offered again after its
    /// backoff. A frame whose epoch has moved on by the time it is ready is thrown away with its files.
    fn fetch_ledgers(&self, epoch: u64, plan: Vec<(heartbeat::Hold, Offer)>) {
        let holds = self.holds.clone();
        let client = self.client.lock().unwrap().clone();
        let dir = self.config.state_dir.join("ledgers");
        let spawned =
            std::thread::Builder::new()
                .name("smeltr-ledger-fetch".to_string())
                .spawn(move || {
                    let frame: Vec<(held::Holding, Offer)> = plan
                    .into_iter()
                    .map(|(hold, offer)| {
                        if offer == Offer::Keep {
                            return (held::Holding { hold, ledger_file: None }, offer);
                        }
                        match fetch_ledger(&client, &dir, &hold.key, epoch) {
                            Ok(file) => (held::Holding { hold, ledger_file: Some(file) }, offer),
                            Err(e) => {
                                tracing::warn!(
                                    target: "worker.ledger_not_fetched",
                                    subject = %hold.key.source,
                                    error = %e,
                                    "could not fetch the ledger of a source the leader holds for this worker, so it \
                                     is not read until it is fetched"
                                );
                                // Offered all the same, so the backoff counts this try.
                                (held::Holding { hold, ledger_file: None }, offer)
                            }
                        }
                    })
                    .collect();
                    let mut held = holds.lock().unwrap();
                    if held.fetching == Some(epoch) {
                        held.fetching = None;
                    }
                    if held.epoch == epoch {
                        held.fetched = Some((epoch, frame));
                    } else {
                        for (holding, _) in frame {
                            holding.discard_ledger();
                        }
                    }
                });
        if let Err(e) = spawned {
            tracing::warn!(
                target: "worker.ledger_fetch_not_started",
                error = %e,
                "could not start the thread that fetches ledgers, so the sources waiting on one are not read yet"
            );
            self.holds.lock().unwrap().fetching = None;
        }
    }

    /// Hand the leader whatever captures a runtime finished. They arrive on the runtime's pipe and leave on the
    /// heartbeat's connection, so this is the one place they cross.
    fn drain_captured(&self, inbox: &child::RuntimeInbox) {
        // The runtime counted these delivered when it wrote them to this process, so from here the outbox is what
        // is holding them: a leader briefly unreachable costs a round trip rather than the records.
        let finished = std::mem::take(&mut *inbox.captured.lock().unwrap());
        self.owe_captures(finished);
    }

    /// The target to act on now, or `None` when there is nothing to do yet.
    ///
    /// A generation this thread has not attempted is always something to do. The one it attempted last is
    /// something to do again only once `retry_at` has come, which is what makes a backoff a wait rather than a
    /// number nothing reads.
    fn next_target(&self, acted: Option<u64>, retry_at: Option<Instant>) -> Option<Target> {
        let ready = |held: &Option<Target>| {
            let due = retry_at.is_some_and(|at| Instant::now() >= at);
            held.as_ref().filter(|t| Some(t.generation) != acted || due).cloned()
        };
        let held = self.target.lock().unwrap();
        if let Some(target) = ready(&held) {
            return Some(target);
        }
        // Woken by a newer target or by the retry falling due, whichever is first, and never for longer than it
        // takes to come back and see to everything else this thread owes.
        let wait = retry_at
            .map(|at| at.saturating_duration_since(Instant::now()))
            .unwrap_or(IDLE_POLL)
            .min(IDLE_POLL);
        let (held, _) = self.arrived.wait_timeout(held, wait).unwrap();
        ready(&held)
    }

    /// Whether a queued offline stop still applies. The heartbeat queues one when contact is overdue and the
    /// deployer acts on it later; a leader that answered in between has ended the outage the stop was for.
    fn take_offline_stop(&self) -> bool {
        self.offline_stop.swap(false, Ordering::Relaxed) && self.out_of_contact()
    }

    /// Whether this worker has been out of contact for longer than its own setting says it may keep running.
    /// Being stopped for an outage lasts as long as the outage: a worker that restarted itself the moment after
    /// would be enforcing nothing.
    fn out_of_contact(&self) -> bool {
        let Some(limit) = self.config.stop_after_unreachable_secs.map(Duration::from_secs) else {
            return false;
        };
        self.last_contact
            .lock()
            .unwrap()
            .is_none_or(|last| last.elapsed() > limit)
    }
}

/// The ledger the leader keeps for `key`, written into `dir` for the runtime to read, which removes it once read.
fn fetch_ledger(
    client: &HeartbeatClient,
    dir: &Path,
    key: &heartbeat::SourceKey,
    epoch: u64,
) -> Result<PathBuf, String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let file = dir.join(ledger_file_name(key, epoch));
    let mut body = client.fetch_ledger(key).map_err(|e| e.to_string())?;
    let mut out = std::fs::File::create(&file).map_err(|e| format!("{}: {e}", file.display()))?;
    std::io::copy(&mut body, &mut out).map_err(|e| format!("{}: {e}", file.display()))?;
    Ok(file)
}

/// The file a source's ledger is fetched into under `epoch`: a digest of its key, since a source's name and place
/// may hold anything a path cannot, and the epoch, so a fetch superseded while it runs never writes over the one
/// that replaced it.
fn ledger_file_name(key: &heartbeat::SourceKey, epoch: u64) -> String {
    let named = format!("{}\0{}", key.source, key.place);
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, named.as_bytes());
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    format!("{hex}-{epoch}.ndjson")
}

/// Wall-clock milliseconds, for facts a leader compares against its own clock. Every deadline in this file is
/// measured on a monotonic clock instead; this is only for saying when something last happened.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Capped exponential backoff with jitter, so a fleet that all failed on one commit does not retry in step.
fn backoff(failures: u32) -> Duration {
    if failures == 0 {
        return REDEPLOY_RETRY_MIN;
    }
    let grown = REDEPLOY_RETRY_MIN.saturating_mul(1u32 << failures.min(6));
    let capped = grown.min(REDEPLOY_RETRY_MAX);
    // A cheap spread rather than a random one: the nanosecond the wait starts on is not something two workers
    // agree about, and a quarter of the delay is enough to break a fleet out of lockstep.
    let spread = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    capped + Duration::from_nanos(spread % (capped.as_nanos() as u64 / 4).max(1))
}

/// Load and compile what `group_id` runs at `commit`. Everything here is this process's own CPU and the object
/// store: nothing is opened, nothing is consumed, and a refusal costs nothing that was running.
fn load_target(
    repo_dir: &Path,
    commit: &str,
    group_id: &str,
    secrets: &crate::plan::topology::HeldSecrets,
) -> Result<(LoadedSystem, Arc<crate::plan::topology::CompiledTopology>, ContentId), String> {
    // Whether the commit holds this group at all, which is what tells an unauthored group from an unreadable one.
    let content = content_id(repo_dir, commit, group_id)?;
    let mut system = match crate::leader::tree::load_group_at(repo_dir, commit, group_id) {
        Ok(s) => s,
        // A commit carries every group, so it holds this one before anybody authors it and after somebody deletes
        // it. Nothing to run is a state, not a failure.
        Err(_) if content.group.is_none() => LoadedSystem::default(),
        Err(e) => return Err(format!("a commit was fetched but does not load: {e}")),
    };
    // What orders this commit's Kafka `start`s after those of the commits before it.
    let deployed_at = committer_time(repo_dir, commit)?;
    // Here rather than with the zone in `start_deployment`, so the supervisor holds the address the runtime binds.
    let listen_address = crate::startup_config::worker_listen_address();
    for source in &mut system.topology.sources {
        match &mut source.kind {
            crate::plan::topology::SourceKind::Kafka(kafka) => kafka.deployed_at = deployed_at,
            crate::plan::topology::SourceKind::Receiver(receiver) => receiver.listen.set_ip(listen_address),
            _ => {}
        }
    }
    let missing = system.topology.missing_secrets(secrets);
    if !missing.is_empty() {
        return Err(format!(
            "it names credentials this host does not have: {}",
            missing.join("; ")
        ));
    }
    let unpaired = system.topology.identity_pairing_errors(secrets);
    if !unpaired.is_empty() {
        return Err(unpaired.join("; "));
    }
    let topology = system
        .compile_topology()
        .map(Arc::new)
        .map_err(|errs| errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "))?;
    Ok((system, topology, content))
}

/// `commit`'s committer time, in unix seconds.
fn committer_time(repo_dir: &Path, commit: &str) -> Result<i64, String> {
    let repo = gix::open(repo_dir).map_err(|e| format!("opening {}: {e}", repo_dir.display()))?;
    let unreadable = |e: &dyn std::fmt::Display| format!("reading {commit}: {e}");
    let id = repo.rev_parse_single(commit).map_err(|e| unreadable(&e))?;
    let commit = id
        .object()
        .map_err(|e| unreadable(&e))?
        .try_into_commit()
        .map_err(|e| unreadable(&e))?;
    let time = commit
        .committer()
        .map_err(|e| unreadable(&e))?
        .time()
        .map_err(|e| unreadable(&e))?;
    Ok(time.seconds)
}

/// Load a commit and start running it, in the process that will run it. Every refusal here happens before
/// anything is consumed: the supervisor keeps whatever was running and retries the latest target.
fn start_deployment(
    spec: &child::StartSpec,
    captures: &Arc<capture::Captures>,
    secrets: &crate::plan::topology::HeldSecrets,
    registry: &Arc<Metrics>,
    held: &Arc<held::Held>,
) -> Result<Deployment, String> {
    let (mut system, topology, _) = load_target(&spec.repo_dir, &spec.commit, &spec.group_id, secrets)?;
    // What differs between the workers of a group, which the bundle they all run cannot say.
    let zone = crate::startup_config::worker_zone();
    for source in &mut system.topology.sources {
        if let crate::plan::topology::SourceKind::Kafka(kafka) = &mut source.kind
            && kafka.service == crate::plan::topology::KafkaService::Kafka
        {
            kafka.member = Some(spec.worker_id.clone());
            kafka.rack = zone.clone();
        }
    }
    for destination in &mut system.topology.destinations {
        destination.kind.take_zone(zone.as_deref());
    }
    let fitted = match spec.memory {
        Some(bytes) => {
            let sources = system.topology.sources.len();
            let delivered = delivered(&system, &topology);
            Some(memory::fit(
                &mut system.topology,
                &delivered,
                sources * lane_depth(spec.thread_limit, sources),
                bytes,
                spec.thread_limit,
                crate::startup_config::stated_memory_budget_bytes(),
            ))
        }
        None => None,
    };
    process_memory_budget().set_limit(fitted.map_or_else(crate::startup_config::memory_budget_bytes, |f| f.columnar));
    deploy(
        &system,
        topology,
        spec.thread_limit,
        fitted,
        captures,
        secrets,
        registry,
        held,
    )
    .map_err(|e| e.to_string())
}

/// Serve one capture invitation. A source this deployment already reads is tapped by the transform threads with
/// no help from here, unless the capture reads the source itself; any other is opened on a thread of its own, which
/// never commits. A destination is
/// tapped by its egress thread, and one nothing here sends to is answered as such: a capture there opens nothing.
fn serve_capture(
    captures: &Arc<capture::Captures>,
    invitation: crate::heartbeat::CaptureInvitation,
    registry: &Arc<Metrics>,
    secrets: &crate::plan::topology::HeldSecrets,
) {
    for admitted in captures.admit(std::slice::from_ref(&invitation)) {
        if let Some(destination) = &admitted.invitation.destination {
            if !captures.sends_to(destination) {
                captures.fail(
                    &admitted,
                    format!("no deployment on this worker sends to destination {destination:?}"),
                );
            }
            continue;
        }
        // A session naming no source was admitted finished, with why. One on a deployed source is served by the
        // deployment's own reader, but for one that reads the source itself.
        if admitted
            .invitation
            .source
            .as_ref()
            .is_none_or(|source| !admitted.invitation.reads_source_itself() && captures.is_deployed(&source.id))
        {
            continue;
        }
        let session = admitted.invitation.session.clone();
        let (captures, thread_captures, registry) = (captures.clone(), captures.clone(), registry.clone());
        // Its own copy: a capture opens a source of its own, and a secured broker answers it no differently than
        // it answers the deployment.
        let secrets = secrets.clone();
        let name = format!("smeltr-capture-{session}");
        let driven = admitted.clone();
        if let Err(e) = std::thread::Builder::new()
            .name(name)
            .spawn(move || capture::run_cold_capture(&thread_captures, &driven, &registry, &secrets))
        {
            tracing::warn!(
                target: "worker.capture_thread_failed",
                subject = %session,
                error = %e,
                "could not start a capture thread"
            );
            // Admitted above, so a repeated invitation for this session would otherwise be ignored as a
            // duplicate forever. Reported rather than silently timing out as a quiet source.
            captures.fail(&admitted, format!("starting a capture thread failed: {e}"));
        }
    }
}

/// A fetch failure with the counts curl puts in it blanked -- how long it waited and how much it got, which differ
/// on every try of the same failure. Every other number, such as a status code, still tells two failures apart.
fn without_timings(error: &str) -> String {
    let words: Vec<&str> = error.split(' ').collect();
    let counts = |next: Option<&&str>| {
        next.is_some_and(|unit| {
            ["ms", "milliseconds", "seconds", "bytes", "out"]
                .iter()
                .any(|u| unit.starts_with(u))
        })
    };
    words
        .iter()
        .enumerate()
        .map(|(i, word)| {
            if !word.is_empty() && word.chars().all(|c| c.is_ascii_digit()) && counts(words.get(i + 1)) {
                "#"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    /// A pinned fetch keeps one certificate, not the file it arrived in. An answer carrying the real authority
    /// beside an attacker's own would otherwise leave the worker trusting both -- after which the attacker is a
    /// leader this worker accepts.
    #[test]
    fn a_pin_keeps_only_the_pinned_certificate_of_what_was_served() {
        let dir = std::env::temp_dir().join(format!("smeltr-pin-blocks-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let authority = |name: &str| {
            let materials = crate::leader::tls::ensure(
                &dir.join(name),
                vec!["localhost".to_string()],
                Duration::from_secs(3600),
                &[],
            )
            .unwrap();
            String::from_utf8(materials.ca_pem).unwrap()
        };
        let real = authority("real");
        let attacker = authority("attacker");
        let wanted = crate::leader::tls::describe(real.as_bytes(), "real")
            .unwrap()
            .fingerprint;

        for served in [format!("{real}{attacker}"), format!("{attacker}{real}")] {
            let kept = pinned_block(&served, &wanted).expect("the pinned certificate was served");
            let described = crate::leader::tls::describe_each(kept.as_bytes(), "kept").unwrap();
            assert_eq!(described.len(), 1, "the attacker's certificate came with it");
            assert_eq!(described[0].fingerprint, wanted);
        }
        assert_eq!(
            pinned_block(&attacker, &wanted),
            None,
            "an answer without the pin kept something"
        );
    }

    use super::*;
    use std::process::Command;
    use std::sync::atomic::AtomicUsize;

    /// A leader that accepts every connection and answers none, on the port this returns.
    ///
    /// This is the stall a budget exists for: the socket is open, so nothing reports a failure on its own, and an
    /// unbounded request would wait on it for as long as the leader stayed up.
    fn silent_leader() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a local port");
        let port = listener.local_addr().expect("an address").port();
        std::thread::spawn(move || {
            // Every accepted connection is held, never answered, until the test ends.
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept() {
                held.push(stream);
            }
        });
        port
    }

    /// `ureq` and `axum-server` take the process's rustls provider, and `main` is what installs it. A test that
    /// opens a TLS connection installs the same one.
    fn install_crypto_provider() {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(|| {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .expect("install rustls crypto provider");
        });
    }

    #[test]
    fn a_fetch_that_stalls_is_aborted_at_its_budget() {
        let port = silent_leader();
        let started = Instant::now();
        let dir = temp_dir("fetch-stalls");
        let refused = fetch_deployed_with_budget(
            &dir,
            &format!("http://127.0.0.1:{port}"),
            "token",
            public_roots(),
            Duration::from_secs(1),
        )
        .expect_err("a fetch that is never answered cannot succeed");
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the budget did not bound the stall: {refused}"
        );
    }

    /// A fetch carries the worker's token as a header, and a stalled fetch's failure names what happened without
    /// repeating it.
    #[test]
    fn a_fetch_aborted_at_its_budget_never_repeats_the_credential_it_carried() {
        let port = silent_leader();
        let token = "s3cret-worker-token";
        let dir = temp_dir("fetch-stalls-credential");
        let refused = fetch_deployed_with_budget(
            &dir,
            &format!("http://127.0.0.1:{port}"),
            token,
            public_roots(),
            Duration::from_secs(1),
        )
        .expect_err("a fetch that is never answered cannot succeed");
        assert!(
            !refused.contains(token),
            "a stalled fetch repeated its credential: {refused}"
        );
    }

    /// A clone whose fetched commit names a blob that no longer reads is found, and one that reads is not.
    #[test]
    fn a_clone_whose_fetched_tree_does_not_read_is_found() {
        let dir = temp_dir("clone-damaged");
        let repo = gix::init_bare(&dir).unwrap();
        assert!(
            fetched_reads(&dir).is_ok(),
            "a clone that has fetched nothing names nothing to read"
        );
        let blob = repo.write_blob(b"pipelines: []\n").unwrap().detach();
        let mut inner = gix::objs::Tree::empty();
        inner.entries.push(gix::objs::tree::Entry {
            mode: gix::objs::tree::EntryKind::Blob.into(),
            filename: "smeltr.yaml".into(),
            oid: blob,
        });
        let inner = repo.write_object(&inner).unwrap().detach();
        let mut root = gix::objs::Tree::empty();
        root.entries.push(gix::objs::tree::Entry {
            mode: gix::objs::tree::EntryKind::Tree.into(),
            filename: "shared".into(),
            oid: inner,
        });
        let root = repo.write_object(&root).unwrap().detach();
        let signature = gix::actor::Signature {
            name: "t".into(),
            email: "t@t".into(),
            time: gix::date::Time::new(0, 0),
        };
        let commit = gix::objs::Commit {
            tree: root,
            parents: Default::default(),
            author: signature.clone(),
            committer: signature,
            encoding: None,
            message: "c".into(),
            extra_headers: Vec::new(),
        };
        let commit = repo.write_object(&commit).unwrap().detach();
        set_ref(&repo, FETCHED_REF, &commit.to_string()).unwrap();
        assert!(fetched_reads(&dir).is_ok());

        let hex = blob.to_string();
        let loose = dir.join("objects").join(&hex[..2]).join(&hex[2..]);
        let mut perms = std::fs::metadata(&loose).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&loose, perms).unwrap();
        // Another blob's object in its place: it decompresses, and only its hash says it is not this one.
        let other = repo.write_blob(b"pipelines: [x]\n").unwrap().detach().to_string();
        std::fs::copy(dir.join("objects").join(&other[..2]).join(&other[2..]), &loose).unwrap();
        let refused = fetched_reads(&dir).expect_err("a blob that reads back as another is found");
        assert!(refused.contains(&hex), "{refused}");
        std::fs::write(&loose, b"not an object").unwrap();
        let refused = fetched_reads(&dir).expect_err("a blob that does not decompress is found");
        assert!(refused.contains(&hex), "{refused}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A real leader, enrolled with and fetched from the way a worker does: `gix` and curl on this side, a real
    /// `git http-backend` CGI process on the leader's, and a lookup file from its store. This is the whole path but
    /// the network hop, which is `127.0.0.1`.
    #[test]
    fn a_worker_s_fetch_lands_the_commit_a_leader_deployed() {
        let data_dir = temp_dir("fetch-e2e");
        std::fs::create_dir_all(&data_dir).unwrap();
        let sealing_dir = temp_dir("fetch-e2e-sealing");
        std::fs::create_dir_all(&sealing_dir).unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let data_dir_for_leader = data_dir.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = crate::leader::sqlite_store::SqliteStore::open(
                    std::path::Path::new(":memory:"),
                    &sealing_dir.join("sealing.key"),
                )
                .unwrap();
                let state = crate::leader::AppState {
                    store: Arc::new(store),
                    registry: Arc::new(crate::leader::registry::Registry::default()),
                    captures: Arc::new(crate::leader::capture::Captures::default()),
                    entitlements: Arc::new(crate::leader::Entitlements::default()),
                    verify_limiter: Arc::new(crate::leader::VerifyLimiter::default()),
                    expiries: Default::default(),
                    throughput: Default::default(),
                    deploy_checks: Default::default(),
                    origin_hosts: Vec::new(),
                    remote: Default::default(),
                    updates: Default::default(),
                    directives: Default::default(),
                    data_dir: data_dir_for_leader,
                };
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                tx.send(addr.port()).unwrap();
                axum::serve(listener, crate::leader::router(state)).await.unwrap();
            });
        });
        let port = rx.recv().expect("leader thread reports its bound port");
        let base = format!("http://127.0.0.1:{port}");

        let code: serde_json::Value = ureq::post(format!("{base}/api/enrollment-codes"))
            .send_json(serde_json::json!({}))
            .expect("minting an enrollment code succeeds")
            .body_mut()
            .read_json()
            .expect("valid JSON");
        assert!(code["ok"].as_bool().unwrap_or(false), "{code}");
        let code = code["code"]["value"].as_str().expect("ok implies an enrollment code");

        let enrolled: serde_json::Value = ureq::post(format!("{base}/api/enroll"))
            .send_json(serde_json::json!({ "code": code, "workerId": "w1" }))
            .expect("enroll request succeeds")
            .body_mut()
            .read_json()
            .expect("valid JSON");
        assert!(enrolled["ok"].as_bool().unwrap_or(false), "{enrolled}");
        let token = enrolled["token"].as_str().expect("ok implies a token");

        let repo = crate::leader::git::repo_path(&data_dir);
        crate::leader::git::init_bare_repo(&repo).unwrap();
        crate::leader::git::ensure_main_commit(&repo).unwrap();
        let parent = crate::leader::git::main_head(&repo).expect("main has a commit");
        let edit = crate::leader::git::FileEdit {
            path: "groups/west/system.json".to_string(),
            content: Some(br#"{"version":1}"#.to_vec()),
        };
        let deployed = match crate::leader::git::commit_edits(
            &repo,
            crate::leader::git::MAIN_BRANCH,
            &parent,
            Some(&parent),
            &[edit],
            "deploy",
            &crate::leader::store::Actor::system(),
        )
        .unwrap()
        {
            crate::leader::git::Committed::At(commit) => commit,
            other => panic!(
                "the commit did not land: {}",
                match other {
                    crate::leader::git::Committed::Unchanged => "nothing changed",
                    _ => "main moved",
                }
            ),
        };

        crate::leader::git::deploy_commit(&data_dir, &deployed, &crate::leader::store::Actor::system()).unwrap();

        let clone_dir = temp_dir("fetch-e2e-clone");
        let head = fetch_deployed(&clone_dir, &base, token, public_roots()).expect("the fetch succeeds");
        assert_eq!(
            head, deployed,
            "the worker's fetch lands the commit the leader deployed"
        );

        // A second fetch against an unmoved pointer is the common case: answered from the ref advertisement alone.
        let unchanged = fetch_deployed(&clone_dir, &base, token, public_roots()).expect("the fetch succeeds again");
        assert_eq!(unchanged, deployed);

        // A worker built from another commit, or one that does not say, is handed nothing.
        for named in [Some("0000000"), None] {
            let mut request = ureq::get(format!("{base}/repo/info/refs?service=git-upload-pack"))
                .header("Authorization", format!("Bearer {token}"));
            if let Some(commit) = named {
                request = request.header(crate::leader::BUILD_HEADER, commit);
            }
            match request.call() {
                Err(ureq::Error::StatusCode(409)) => {}
                other => panic!("a worker naming {named:?} was not refused: {other:?}"),
            }
        }

        // A lookup file travels beside git, asked for by oid and checked on arrival.
        let mut incoming = crate::lookup::store::Incoming::new(&repo).unwrap();
        std::io::Write::write_all(&mut incoming, b"ip,city\n10.0.0.0/8,lan\n").unwrap();
        let pointer = incoming.finish(None).unwrap();
        let client = HeartbeatClient::new(&base, token, None, true).unwrap();
        let mut arriving = crate::lookup::store::Incoming::new(&clone_dir).unwrap();
        std::io::copy(&mut client.lookup_file(&pointer.oid).unwrap(), &mut arriving).unwrap();
        arriving.finish(Some(&pointer)).expect("the file arrives whole");
        assert!(
            client.lookup_file(&"0".repeat(64)).is_err(),
            "a file the store lacks is not found"
        );
        let stranger = HeartbeatClient::new(&base, "not-a-token", None, true).unwrap();
        assert!(
            stranger.lookup_file(&pointer.oid).is_err(),
            "only an enrolled worker is served"
        );
    }

    /// The whole reason a worker's fetch runs on curl rather than `gix-transport`'s own `reqwest` backend
    /// (`Cargo.toml`): a leader signs its own certificate, so a worker verifies it against a specific authority
    /// rather than the public web's. `ssl_ca_info` is what carries that authority into the fetch, the same as
    /// `-c http.sslCAInfo` did for the git binary -- given, the leader's self-signed certificate is accepted;
    /// withheld, it is refused rather than silently trusted.
    #[test]
    fn a_fetch_verifies_the_leader_s_certificate_against_the_pinned_authority() {
        install_crypto_provider();

        let data_dir = temp_dir("fetch-tls-e2e");
        std::fs::create_dir_all(&data_dir).unwrap();
        let tls_dir = data_dir.join("tls");
        // `ensure` twice, the way a real leader's own startup does across a restart, so `socket_took` below can say the
        // certificate this process is about to serve is the one now on disk -- minting an enrollment code checks that.
        let names = vec!["127.0.0.1".to_string()];
        let validity = Duration::from_secs(3600);
        let first = crate::leader::tls::ensure(&tls_dir, names.clone(), validity, &[]).expect("an authority, created");
        let ca_pem = first.ca_pem.clone();
        let materials = crate::leader::tls::ensure(&tls_dir, names, validity, &[]).expect("a certificate, reissued");
        crate::leader::tls::socket_took(&tls_dir, crate::leader::tls::disk_generation(&tls_dir));
        let cert_pem = std::fs::read(&materials.server_cert_path).unwrap();
        let key_pem = std::fs::read(&materials.server_key_path).unwrap();

        let sealing_dir = temp_dir("fetch-tls-e2e-sealing");
        std::fs::create_dir_all(&sealing_dir).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let data_dir_for_leader = data_dir.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = crate::leader::sqlite_store::SqliteStore::open(
                    std::path::Path::new(":memory:"),
                    &sealing_dir.join("sealing.key"),
                )
                .unwrap();
                let state = crate::leader::AppState {
                    store: Arc::new(store),
                    registry: Arc::new(crate::leader::registry::Registry::default()),
                    captures: Arc::new(crate::leader::capture::Captures::default()),
                    entitlements: Arc::new(crate::leader::Entitlements::default()),
                    verify_limiter: Arc::new(crate::leader::VerifyLimiter::default()),
                    expiries: Default::default(),
                    throughput: Default::default(),
                    deploy_checks: Default::default(),
                    origin_hosts: Vec::new(),
                    remote: Default::default(),
                    updates: Default::default(),
                    directives: Default::default(),
                    data_dir: data_dir_for_leader,
                };
                let tls = axum_server::tls_rustls::RustlsConfig::from_pem(cert_pem, key_pem)
                    .await
                    .unwrap();
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                listener.set_nonblocking(true).unwrap();
                let addr = listener.local_addr().unwrap();
                tx.send(addr.port()).unwrap();
                axum_server::tls_rustls::from_tcp_rustls(listener, tls)
                    .unwrap()
                    .serve(crate::leader::router(state).into_make_service())
                    .await
                    .unwrap();
            });
        });
        let port = rx.recv().expect("leader thread reports its bound port");
        let base = format!("https://127.0.0.1:{port}");

        let agent = crate::heartbeat::agent_trusting(Some(&ca_pem), true, crate::heartbeat::REQUEST_BUDGET).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if agent.get(format!("{base}/api/health")).call().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "{base} never answered");
            std::thread::sleep(Duration::from_millis(20));
        }

        let code: serde_json::Value = agent
            .post(format!("{base}/api/enrollment-codes"))
            .send_json(serde_json::json!({}))
            .expect("minting an enrollment code succeeds")
            .body_mut()
            .read_json()
            .expect("valid JSON");
        assert!(code["ok"].as_bool().unwrap_or(false), "{code}");
        let code = code["code"]["value"].as_str().expect("ok implies an enrollment code");

        let enrolled: serde_json::Value = agent
            .post(format!("{base}/api/enroll"))
            .send_json(serde_json::json!({ "code": code, "workerId": "w1" }))
            .expect("enroll request succeeds")
            .body_mut()
            .read_json()
            .expect("valid JSON");
        assert!(enrolled["ok"].as_bool().unwrap_or(false), "{enrolled}");
        let token = enrolled["token"].as_str().expect("ok implies a token");

        let repo = crate::leader::git::repo_path(&data_dir);
        crate::leader::git::init_bare_repo(&repo).unwrap();
        crate::leader::git::ensure_main_commit(&repo).unwrap();

        let ca_path = temp_dir("fetch-tls-e2e-ca").join("ca.pem");
        std::fs::create_dir_all(ca_path.parent().unwrap()).unwrap();
        std::fs::write(&ca_path, &ca_pem).unwrap();

        let clone_dir = temp_dir("fetch-tls-e2e-clone");
        fetch_deployed(&clone_dir, &base, token, &ca_path).expect("the fetch succeeds with the right authority");

        // Without the authority the leader's certificate is verified against, the same self-signed leader is
        // refused rather than trusted anyway: `ssl_verify` is not silently off.
        let untrusted = temp_dir("fetch-tls-e2e-clone-untrusted");
        let refused = fetch_deployed(&untrusted, &base, token, public_roots())
            .expect_err("a self-signed leader is refused without its authority");
        assert!(
            refused.starts_with(&format!(
                "fetching what the fleet runs failed: {base} presented a certificate this worker does not trust ([60] "
            )),
            "the fetch should fail on the TLS handshake and say so: {refused}"
        );

        // Enrolling against the same leader without its authority says the same, not that the leader is not there.
        let refused = match enroll(&base, code, "w2", None, true) {
            Err(e) => e.message().to_string(),
            Ok(_) => panic!("enrolled with a leader it does not trust"),
        };
        assert_eq!(
            refused,
            format!(
                "enrolling failed: {}",
                heartbeat::untrusted_certificate(&base, &"invalid peer certificate: UnknownIssuer")
            )
        );
    }

    /// `ureq` bounds no part of a request on its own, so an agent built without a budget waits on a leader that
    /// accepts and never answers for as long as the socket stays open. `HeartbeatClient` builds its agents with the
    /// same builder as `agent_trusting`, so these cover the heartbeat and the capture report too.
    #[test]
    fn every_agent_that_talks_to_the_leader_gives_up_on_a_silent_one() {
        install_crypto_provider();
        let budget = Duration::from_millis(200);
        let platform =
            heartbeat::agent_trusting(None, true, budget).expect("an agent trusting the platform's own roots");
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("a self-signed certificate");
        let authority = heartbeat::agent_trusting(Some(cert.pem().as_bytes()), true, budget)
            .expect("an agent trusting an authority");
        // The one that verifies nothing included: it is a worker's very first request, made before it holds an
        // authority at all, and the address it goes to is the one nothing has vouched for yet.
        let unverified = heartbeat::agent_without_verification(budget);

        let port = silent_leader();
        // Over TLS the stall is in the handshake. Over plain HTTP it is in the wait for an answer.
        for scheme in ["https", "http"] {
            for (name, agent) in [
                ("platform", &platform),
                ("authority", &authority),
                ("unverified", &unverified),
            ] {
                // On its own thread, so an agent with no budget fails this test rather than hanging it. A thread
                // left waiting ends with the process.
                let (sent, received) = std::sync::mpsc::channel();
                let agent = agent.clone();
                let url = format!("{scheme}://127.0.0.1:{port}/api/health");
                std::thread::spawn(move || {
                    let _ = sent.send(agent.get(url).call().map(|response| response.status()));
                });
                let answered = received
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap_or_else(|_| panic!("{name} over {scheme}: the budget did not bound the stall"));
                // A timeout, and nothing else: a connection refused or dropped also fails fast, and proves no budget.
                assert!(
                    matches!(answered, Err(ureq::Error::Timeout(_))),
                    "{name} over {scheme}: a leader that never answers should end in a timeout: {answered:?}"
                );
            }
        }
    }

    fn sample(metric: &str) -> crate::heartbeat::MetricSampleDto {
        crate::heartbeat::MetricSampleDto {
            metric: metric.to_string(),
            labels: vec![],
            value: crate::heartbeat::MetricValueDto::Counter(1),
        }
    }

    #[test]
    fn a_dlq_does_not_hold_the_reader_at_the_floor() {
        use crate::core::error::ExceptionKind;
        use crate::plan::config::{Answer, OnException};
        let to_kafka = || Answer::Deliver(vec![2]);
        use crate::plan::topology::*;
        let elastic = DestinationConfig {
            id: 1,
            name: "es".into(),
            kind: DestinationKind::Elasticsearch(ElasticDestinationConfig {
                url: "http://es:9200".into(),
                zone_urls: Default::default(),
                sniff: None,
                default_index: "i".into(),
                identity: None,
                auth: None,
                doc_id: DocIdPolicy::default(),
                in_flight: 4,
                max_bulk_bytes: 5 * 1024 * 1024,
                buffer_bytes: 64 * 1024 * 1024,
                stall_after: Duration::from_secs(30),
                id_field: None,
            }),
            on_reject: to_kafka(),
        };
        let kafka_dlq = DestinationConfig {
            id: 2,
            name: "dlq".into(),
            kind: DestinationKind::Kafka(KafkaDestinationConfig {
                payload: crate::plan::config::Payload::Record,
                compression: crate::plan::topology::Compression::None,
                stall_after: Duration::from_secs(30),
                brokers: "k:9092".into(),
                default_topic: "dlq".into(),
                service: crate::plan::topology::KafkaService::Kafka,
                identity: None,
                buffer_bytes: 16_000_000,
                props: KafkaProps {
                    literal: Vec::new(),
                    credentials: Vec::new(),
                },
                avro: None,
            }),
            on_reject: Answer::Block,
        };
        let route = |to: Vec<crate::core::id::DestId>| CompiledRoute {
            id: "r".into(),
            source: 0,
            cond: None,
            pipeline: 0,
            deliver: vec![CompiledDeliverRule {
                when: None,
                to,
                compiled_patterns: Vec::new(),
            }],
            compiled_patterns: Vec::new(),
            on_exception: Default::default(),
        };
        let mut topology = CompiledTopology {
            routes: vec![route(vec![1])],
            pipelines: std::collections::HashMap::new(),
            metas: SourceMetas::new(),
            on_exception: {
                let mut group = OnException::default();
                for kind in [ExceptionKind::Unmatched, ExceptionKind::Unprocessable] {
                    group.answers[kind.index()] = to_kafka();
                }
                group
            },
            dlq_of: std::collections::HashMap::from([(1, vec![2])]),
            records: std::collections::HashMap::new(),
            payloads: std::collections::HashMap::new(),
        };
        let destinations = [elastic, kafka_dlq];

        // The Kafka destination is the unprocessable destination, the unmatched destination and Elasticsearch's DLQ. The reader waits on
        // it for every batch, and its buffer holds four polls, but the flow goes to Elasticsearch and that queue is
        // what sizes the reader.
        assert_eq!(
            reader_depth(&topology, &destinations, 0, |d| d.kind.queue_bytes()),
            64 * 1024 * 1024
        );
        // Deliver to it as well and it is a destination like any other: the largest queue rules, so a destination
        // that acknowledges late can fill before the reader stops, and the smaller queue still binds on its own.
        topology.routes = vec![route(vec![1, 2])];
        assert!(destinations[1].kind.queue_bytes() < 64 * 1024 * 1024);
        assert_eq!(
            reader_depth(&topology, &destinations, 0, |d| d.kind.queue_bytes()),
            64 * 1024 * 1024
        );
        // A source with no routes has no flow to size for: its batch floor alone binds.
        assert_eq!(reader_depth(&topology, &destinations, 5, |d| d.kind.queue_bytes()), 0);
    }

    /// A block and a refusal are known only to the thread holding the source or dispatching to the destination, so
    /// they reach the leader by riding that thread's report rather than the registry.
    #[test]
    fn a_facts_owner_carries_it_onto_its_own_thread() {
        let text = |metric: &str| crate::heartbeat::MetricSampleDto {
            metric: metric.to_string(),
            labels: vec![("source".to_string(), "orders-west".to_string())],
            value: crate::heartbeat::MetricValueDto::Text("es-prod is refusing".to_string()),
        };
        let mut by_thread =
            std::collections::HashMap::from([("reader-0".to_string(), vec![sample("smeltr_rows_polled_total")])]);

        attach_owned_facts(
            &mut by_thread,
            [
                ("reader-0".to_string(), vec![text("smeltr_source_blocked_reason")]),
                // A reader that is not blocked offers nothing, and must not create an entry.
                ("reader-1".to_string(), vec![]),
                ("egress-3".to_string(), vec![text("smeltr_destination_last_refusal")]),
            ],
        );

        let names = |t: &str| {
            by_thread
                .get(t)
                .map(|v| v.iter().map(|s| s.metric.clone()).collect::<Vec<_>>())
                .unwrap_or_default()
        };
        assert_eq!(
            names("reader-0"),
            vec!["smeltr_rows_polled_total", "smeltr_source_blocked_reason"],
            "the hold joins what the registry already had for that thread"
        );
        assert_eq!(names("egress-3"), vec!["smeltr_destination_last_refusal"]);
        assert!(
            !by_thread.contains_key("reader-1"),
            "a reader with no block reports no fact"
        );
    }

    #[test]
    fn a_sample_no_running_thread_claims_still_travels() {
        let by_thread = std::collections::HashMap::from([
            ("0-0".to_string(), vec![sample("smeltr_rows_polled_total")]),
            ("egress-3".to_string(), vec![sample("smeltr_destination_healthy")]),
            ("deploy".to_string(), vec![sample("smeltr_worker_transform_threads")]),
        ]);
        let running = vec![
            (
                "0-0".to_string(),
                ThreadKind::Reader,
                vec![PartitionRef {
                    topic: "t".to_string(),
                    partition: 0,
                }],
            ),
            // Running, but recorded nothing this window.
            ("1-0".to_string(), ThreadKind::Transform, vec![]),
        ];

        let out = thread_reports("g", &running, by_thread);

        assert_eq!(
            out.iter().map(|t| t.thread_id.as_str()).collect::<Vec<_>>(),
            ["0-0", "1-0", "deploy", "egress-3"]
        );
        assert_eq!(out[0].partitions_owned.len(), 1);
        assert_eq!(out[0].kind, Some(ThreadKind::Reader));
        assert!(out[1].metrics.is_empty());
        // The deploy gauges are the worker's own; a bucket nothing knows carries no kind. Neither is a thread.
        assert_eq!(out[2].kind, Some(ThreadKind::Worker));
        assert_eq!(out[3].kind, None);
        assert!(out[3].partitions_owned.is_empty());
        // The cursor is committed past every peeked sample, so every bucket has to be on the request.
        assert_eq!(out.iter().map(|t| t.metrics.len()).sum::<usize>(), 3);
        assert!(out.iter().all(|t| t.group_id == "g"));
    }

    /// Why the registry belongs to the process. A capture runs whether or not anything is deployed, and a
    /// deployment that has stopped leaves counts owed; while the heartbeat read only a deployment's own registry,
    /// both had nowhere to go and were simply dropped.
    #[test]
    fn a_worker_with_nothing_deployed_still_reports_what_the_registry_holds() {
        let registry = Arc::new(Metrics::new());
        MetricsHandle::new(registry.clone(), capture::CAPTURE_LABEL).incr("smeltr_capture_records_total", &[], 4);

        let (req, reported, _, _) = build_heartbeat_request(
            Reporting {
                worker_id: "w1",
                group_id: "g",
                commit: None,
                supervising: None,
                resources: None,
                stop_after_unreachable_secs: None,
                authority_expires_millis: None,
                authority_fingerprints: Vec::new(),
                takes_authority: true,
                stopping: false,
                accepts: Vec::new(),
                took: None,
                heartbeat_interval: None,
            },
            None,
            &registry,
            &Default::default(),
        );

        assert!(
            reported.iter().any(|s| s.key.name == "smeltr_capture_records_total"),
            "peeked, so the count can be committed once the request lands: {reported:?}"
        );
        let from_capture = req
            .threads
            .iter()
            .find(|t| t.thread_id == capture::CAPTURE_LABEL)
            .expect("the capture's own report rides beside the bootstrap one");
        assert_eq!(from_capture.metrics[0].metric, "smeltr_capture_records_total");
        assert!(
            req.threads.iter().any(|t| t.thread_id.ends_with("-bootstrap")),
            "and the worker still reports itself, so it still learns a directive"
        );
    }

    /// A stopping worker's notice is all the leader keeps of it, and the leader keeps only what it says about
    /// itself. So the notice always says it, even from a worker whose runtime would not confirm it had stopped.
    #[test]
    fn the_notice_always_carries_the_worker_s_own_report() {
        let running = child::RuntimeReport {
            pool_thread_ids: vec!["1-0".to_string()],
            ..Default::default()
        };
        let threads = |stopping: bool| -> Vec<String> {
            let (req, _, _, _) = build_heartbeat_request(
                Reporting {
                    worker_id: "w1",
                    group_id: "g",
                    commit: None,
                    supervising: None,
                    resources: None,
                    stop_after_unreachable_secs: None,
                    authority_expires_millis: None,
                    authority_fingerprints: Vec::new(),
                    takes_authority: true,
                    stopping,
                    accepts: Vec::new(),
                    took: None,
                    heartbeat_interval: None,
                },
                Some(&running),
                &Arc::new(Metrics::new()),
                &Default::default(),
            );
            assert_eq!(req.stopping, stopping);
            req.threads.into_iter().map(|t| t.thread_id).collect()
        };

        let (running_on, stopping) = (threads(false), threads(true));
        assert!(!running_on.contains(&"w1-bootstrap".to_string()), "{running_on:?}");
        assert!(stopping.contains(&"w1-bootstrap".to_string()), "{stopping:?}");
        assert!(stopping.contains(&"1-0".to_string()), "{stopping:?}");
    }

    /// The logs buffer is the process's, so what it reports about itself carries no thread -- and a
    /// redeploy, which retires every thread-labeled key the new deployment has no counterpart for, leaves it
    /// alone. `smeltr_logs_written_total` does carry a `site`, which is a call site rather than a thread and so
    /// is nobody's to retire.
    #[test]
    fn what_the_logs_buffer_reports_belongs_to_no_thread() {
        let registry = Arc::new(Metrics::new());
        MetricsHandle::new(registry.clone(), "reader-0").incr("smeltr_rows_in_total", &[], 1);
        build_heartbeat_request(
            Reporting {
                worker_id: "w1",
                group_id: "g",
                commit: None,
                supervising: None,
                resources: None,
                stop_after_unreachable_secs: None,
                authority_expires_millis: None,
                authority_fingerprints: Vec::new(),
                takes_authority: true,
                stopping: false,
                accepts: Vec::new(),
                took: None,
                heartbeat_interval: None,
            },
            None,
            &registry,
            &Default::default(),
        );

        let gauges = |registry: &Metrics| -> Vec<crate::metrics::MetricSample> {
            registry
                .snapshot()
                .into_iter()
                .filter(|s| s.key.name.starts_with("smeltr_logs_"))
                .collect()
        };

        let reported = gauges(&registry);
        assert!(!reported.is_empty(), "the buffer reports itself: {reported:?}");
        assert!(
            reported
                .iter()
                .all(|s| s.key.labels.iter().all(|(k, _)| *k != crate::metrics::THREAD_LABEL)),
            "and none of it carries a thread a redeploy could claim: {reported:?}"
        );

        assert!(
            reported
                .iter()
                .all(|s| s.key.labels.contains(&("process", "supervisor".to_string()))),
            "each names the process whose buffer it is: {reported:?}"
        );

        // What a redeploy does: keep the threads the new deployment named, retire the rest.
        registry.retire_threads_except(&[]);
        assert_eq!(
            gauges(&registry).len(),
            reported.len(),
            "so a redeploy leaves all of it"
        );
    }

    /// GOAL ONE: a heartbeat's `source`/`destination` labels carry the author's own name, not the position
    /// `sources.json`/`destinations.json` happened to assign -- the id that shifts when an unrelated entry is
    /// deleted (`config_file::load::index_by_id`). Resolution runs once over the whole assembled report, so this
    /// covers a registry sample without needing a second test for an owned fact -- both pass through the one loop
    /// in `build_heartbeat_request`.
    #[test]
    fn a_heartbeats_subject_labels_carry_author_names_not_positional_ids() {
        let registry = Arc::new(Metrics::new());
        MetricsHandle::new(registry.clone(), "reader-0").incr("smeltr_rows_polled_total", &[("source", "0")], 5);
        // Past this deployment's one destination: nothing to resolve it to, so it has to survive rather than vanish.
        MetricsHandle::new(registry.clone(), "reader-0").incr("smeltr_batches_dlq_total", &[("destination", "9")], 1);
        MetricsHandle::new(registry.clone(), "transform-0").incr(
            "smeltr_function_calls_total",
            &[("pipeline", "0"), ("function", "2")],
            3,
        );

        let deployment = Some(Deployment {
            readers: Vec::new(),
            pool: Vec::new(),
            egress: Vec::new(),
            refusals: Arc::new(crate::runtime::RefusalRing::default()),
            streams: Arc::new(crate::streams::Streams::new(
                0,
                Vec::new(),
                std::time::Duration::from_secs(1),
            )),
            subjects: Arc::new(crate::metrics::Subjects {
                sources: vec!["orders-west".to_string()],
                destinations: Vec::new(),
                pipelines: vec!["orders-normalize".to_string()],
                steps: vec![vec![
                    "parse".to_string(),
                    "enrich".to_string(),
                    "drop-noise".to_string(),
                ]],
            }),
            memory_warning: None,
            pools: [None, None],
            registry: registry.clone(),
        });

        let (req, _, _, _) = build_heartbeat_request(
            Reporting {
                worker_id: "w1",
                group_id: "g",
                commit: Some("c0ffee"),
                supervising: None,
                resources: None,
                stop_after_unreachable_secs: None,
                authority_expires_millis: None,
                authority_fingerprints: Vec::new(),
                takes_authority: true,
                stopping: false,
                accepts: Vec::new(),
                took: None,
                heartbeat_interval: None,
            },
            deployment.as_ref().map(report_of).as_ref(),
            &registry,
            &Default::default(),
        );
        let metrics: Vec<_> = req.threads.into_iter().flat_map(|t| t.metrics).collect();

        let rows_polled = metrics.iter().find(|m| m.metric == "smeltr_rows_polled_total").unwrap();
        assert!(
            rows_polled
                .labels
                .contains(&("source".to_string(), "orders-west".to_string())),
            "{:?}",
            rows_polled.labels
        );

        let calls = metrics
            .iter()
            .find(|m| m.metric == "smeltr_function_calls_total")
            .unwrap();
        assert!(
            calls
                .labels
                .contains(&("pipeline".to_string(), "orders-normalize".to_string())),
            "a pipeline resolves the same way: {:?}",
            calls.labels
        );
        assert!(
            calls
                .labels
                .contains(&("function".to_string(), "drop-noise".to_string())),
            "and a step resolves through the pipeline it belongs to: {:?}",
            calls.labels
        );

        let dlq = metrics.iter().find(|m| m.metric == "smeltr_batches_dlq_total").unwrap();
        assert!(
            dlq.labels.contains(&("destination".to_string(), "9".to_string())),
            "an id past this deployment's own destinations keeps its numeric value: {:?}",
            dlq.labels
        );
    }

    /// What a runtime would have sent about this deployment, so a heartbeat test reads the same shape the wire
    /// carries rather than the locks the deployment keeps.
    fn report_of(d: &Deployment) -> child::RuntimeReport {
        let facts = d.facts();
        child::RuntimeReport {
            readers: facts
                .readers
                .iter()
                .map(|r| child::ReaderReport {
                    thread_id: r.thread_id.clone(),
                    partitions: r.partitions.lock().unwrap().clone(),
                    held: r.held.lock().unwrap().clone(),
                    connection_error: r.connection_error.lock().unwrap().clone(),
                })
                .collect(),
            pool_thread_ids: facts.pool_thread_ids.clone(),
            egress_ids: facts.egress_ids.clone(),
            egress_refusals: facts
                .egress_ids
                .iter()
                .map(|id| (*id, facts.refusals.heartbeat_facts(*id)))
                .collect(),
            subjects: (*facts.subjects).clone(),
            ..Default::default()
        }
    }

    /// A target that failed is tried again, but not at once. The retry is a time, not a flag: making the target
    /// eligible again without one turns a bounded backoff into a load-compile-spawn loop that never rests.
    #[test]
    fn a_failed_target_waits_for_its_backoff_before_it_is_offered_again() {
        let supervisor = supervisor_for(temp_dir("backoff"));
        supervisor.publish("c0ffee".to_string());

        let first = supervisor
            .next_target(None, None)
            .expect("a target nothing has attempted is offered at once");
        assert_eq!(first.commit, "c0ffee");

        // Attempted and failed: due later, and not before.
        let later = Instant::now() + Duration::from_secs(30);
        let waited = Instant::now();
        assert!(
            supervisor.next_target(Some(first.generation), Some(later)).is_none(),
            "a target was offered again before its backoff had run"
        );
        assert!(
            waited.elapsed() < Duration::from_secs(5),
            "waiting for the retry should not hold this thread from what else it owes"
        );

        // Due now.
        assert!(
            supervisor
                .next_target(Some(first.generation), Some(Instant::now()))
                .is_some(),
            "a target was not offered again once its backoff had run"
        );
    }

    /// A newer observation never waits on the last one's backoff: whatever refused that commit says nothing about
    /// a different one.
    #[test]
    fn a_newer_target_does_not_wait_on_an_older_one_s_backoff() {
        let supervisor = supervisor_for(temp_dir("bypass"));
        supervisor.publish("older".to_string());
        let first = supervisor.next_target(None, None).expect("the first target");

        supervisor.publish("newer".to_string());
        let offered = supervisor
            .next_target(Some(first.generation), Some(Instant::now() + Duration::from_secs(300)))
            .expect("a newly observed target is eligible whatever the last one is waiting out");
        assert_eq!(offered.commit, "newer");
    }

    /// The same head seen again is the same observation. A worker whose discovery answers every interval must not
    /// read that as a stream of new targets, or nothing would ever be a retry.
    #[test]
    fn seeing_the_same_head_again_is_not_a_new_observation() {
        let supervisor = supervisor_for(temp_dir("same-head"));
        supervisor.publish("c0ffee".to_string());
        let first = supervisor.next_target(None, None).expect("the first target");
        supervisor.publish("c0ffee".to_string());
        assert!(
            supervisor.next_target(Some(first.generation), None).is_none(),
            "the same commit was counted as something new to act on"
        );
    }

    /// Stopped for an outage stays stopped for the outage. This is the setting's whole purpose: a worker that
    /// restarted the moment after it stopped would consume through exactly the period it was told not to.
    #[test]
    fn a_leader_heard_from_since_cancels_a_queued_offline_stop() {
        let mut supervisor = supervisor_for(temp_dir("offline-canceled"));
        supervisor.config.stop_after_unreachable_secs = Some(3600);
        supervisor.offline_stop.store(true, Ordering::Relaxed);
        *supervisor.last_contact.lock().unwrap() = Some(Instant::now());
        assert!(
            !supervisor.take_offline_stop(),
            "a stop queued for an outage that has ended still stopped the runtime"
        );
        assert!(
            !supervisor.offline_stop.load(Ordering::Relaxed),
            "a canceled stop was left queued"
        );

        supervisor.offline_stop.store(true, Ordering::Relaxed);
        *supervisor.last_contact.lock().unwrap() = None;
        assert!(supervisor.take_offline_stop(), "a stop during an outage is a stop");
    }

    #[test]
    fn a_worker_out_of_contact_past_its_limit_stays_out() {
        let mut supervisor = supervisor_for(temp_dir("offline"));
        supervisor.config.stop_after_unreachable_secs = Some(0);
        assert!(
            supervisor.out_of_contact(),
            "a worker that has never heard from its leader is out of contact"
        );

        *supervisor.last_contact.lock().unwrap() = Some(Instant::now());
        // Zero seconds: any elapsed time is past it, which is what makes this deterministic rather than a sleep.
        std::thread::sleep(Duration::from_millis(5));
        assert!(supervisor.out_of_contact(), "an outage past the limit is an outage");

        supervisor.config.stop_after_unreachable_secs = None;
        assert!(
            !supervisor.out_of_contact(),
            "a worker with no limit runs indefinitely without a leader"
        );
    }

    /// The leader repeats a directive until a beat names it taken, so each id is acted on once however many
    /// answers carry it.
    #[test]
    fn a_restart_is_taken_once_per_request() {
        let supervisor = supervisor_for(temp_dir("restart-once"));
        let restart = |id: &str| heartbeat::Directive {
            id: id.to_string(),
            action: heartbeat::DirectiveAction::Restart,
        };
        supervisor.follow_directive(restart("a"));
        assert!(supervisor.restart_due.swap(false, Ordering::Relaxed));
        assert_eq!(supervisor.took.lock().unwrap().as_deref(), Some("a"));
        supervisor.follow_directive(restart("a"));
        assert!(
            !supervisor.restart_due.load(Ordering::Relaxed),
            "a repeat is not a second restart"
        );
        supervisor.follow_directive(restart("b"));
        assert!(supervisor.restart_due.load(Ordering::Relaxed), "a new request is");
    }

    /// Exiting would end the leader this worker lives in.
    #[test]
    fn a_worker_inside_the_leader_never_offers_to_shut_down() {
        let mut supervisor = supervisor_for(temp_dir("in-leader"));
        assert!(supervisor.accepts().contains(&heartbeat::DirectiveAction::Shutdown));
        supervisor.config.in_leader = true;
        assert_eq!(supervisor.accepts(), vec![heartbeat::DirectiveAction::Restart]);
    }

    fn supervisor_for(state_dir: PathBuf) -> Supervisor {
        Supervisor {
            client: Mutex::new(Arc::new(
                HeartbeatClient::new("http://127.0.0.1:1", "t", None, true).expect("a client that talks to nothing"),
            )),
            trusting: Mutex::new(None),
            authority_expires_millis: Mutex::new(None),
            worker_id: "w1".to_string(),
            token: "t".to_string(),
            git_roots: PathBuf::new(),
            repo_dir: repo_dir(&state_dir),
            captures: Arc::new(capture::Captures::default()),
            registry: Arc::new(Metrics::new()),
            secrets: Mutex::new(crate::plan::topology::HeldSecrets::default()),
            last_contact: Mutex::new(None),
            target: Mutex::new(None),
            arrived: Condvar::new(),
            runtime: Mutex::new(None),
            streams_owed: Mutex::new(OwedStreams::default()),
            commit: Mutex::new(None),
            supervising: Mutex::new(heartbeat::Supervising::default()),
            resources: Mutex::new(crate::resources::Sampler::new(state_dir.clone())),
            invitations: Mutex::new(Invitations::default()),
            outbox: Mutex::new(Vec::new()),
            offline_stop: AtomicBool::new(false),
            mirror_due: AtomicBool::new(false),
            logs_mirror: Mutex::new(None),
            exit: Mutex::new(Exit::Running),
            exit_changed: Condvar::new(),
            enrolled_again_at: Mutex::new(None),
            took: Mutex::new(None),
            restart_due: AtomicBool::new(false),
            holds: Arc::new(Mutex::new(Holds::default())),
            receiver_sockets: Mutex::new(crate::io::receiver::Sockets::default()),
            config: WorkerConfig {
                leader_url: "http://127.0.0.1:1".to_string(),
                leader_ca_pem: None,
                leader_trust_public: true,
                leader_authority_dir: None,
                group_id: "g".to_string(),
                thread_limit: 1,
                memory: None,
                state_dir,
                enrollment_code: None,
                heartbeat_interval: Duration::from_millis(50),
                stop_after_unreachable_secs: None,
                in_leader: false,
                metrics_endpoint: None,
            },
        }
    }

    /// A capture leaves the heartbeat's thread and the deployer's thread the moment it is finished with, because
    /// uploading one is allowed to take far longer than either schedule may pause for. Both paths put it in the
    /// same place, and neither reaches the leader itself.
    #[test]
    fn a_finished_capture_leaves_the_schedules_that_produced_it() {
        let supervisor = supervisor_for(temp_dir("capture-outbox"));
        let report = |session: &str| crate::heartbeat::CaptureReport {
            session: session.to_string(),
            worker_id: "w1".to_string(),
            batches: vec![],
            finished: Some(crate::heartbeat::CaptureEnd::Bytes),
            sql: None,
            avro: Vec::new(),
        };

        // What a runtime finished, arriving on its pipe.
        let inbox = child::RuntimeInbox::default();
        inbox.captured.lock().unwrap().push(report("from-runtime"));
        supervisor.drain_captured(&inbox);
        assert!(
            inbox.captured.lock().unwrap().is_empty(),
            "the runtime's inbox still holds a capture nothing is going to offer again"
        );

        // And what the supervisor served cold itself.
        supervisor.owe_captures(vec![report("from-supervisor")]);

        let waiting: Vec<String> = supervisor
            .outbox
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.session.clone())
            .collect();
        assert_eq!(waiting, vec!["from-runtime", "from-supervisor"], "oldest first");
    }

    /// `merge_owed` is used both ways this file needs it: fresh reports replacing what the outbox already held,
    /// and what arrived while a failed batch was in flight replacing the stale copy that failed. Either way, a
    /// session the incoming side does not name is left exactly where the base side had it.
    #[test]
    fn merge_owed_replaces_a_named_session_and_leaves_the_rest_where_they_were() {
        let report = |session: &str, finished: crate::heartbeat::CaptureEnd| crate::heartbeat::CaptureReport {
            session: session.to_string(),
            worker_id: "w1".to_string(),
            batches: vec![],
            finished: Some(finished),
            sql: None,
            avro: Vec::new(),
        };
        let mut base = vec![
            report("s1", crate::heartbeat::CaptureEnd::Bytes),
            report("s2", crate::heartbeat::CaptureEnd::Bytes),
        ];

        merge_owed(
            &mut base,
            vec![
                report("s1", crate::heartbeat::CaptureEnd::Matches),
                report("s3", crate::heartbeat::CaptureEnd::Bytes),
            ],
        );

        let sessions: Vec<&str> = base.iter().map(|r| r.session.as_str()).collect();
        assert_eq!(
            sessions,
            vec!["s1", "s2", "s3"],
            "s1 should have kept its place, s2 stayed untouched, s3 was appended"
        );
        assert!(
            matches!(base[0].finished, Some(crate::heartbeat::CaptureEnd::Matches)),
            "the incoming report did not replace the one already held"
        );
    }

    /// A runtime that released its session the moment it wrote a final report can be re-admitted by a repeated
    /// invitation and start filling again -- but its own progress report must not erase the final one still
    /// queued for upload.
    #[test]
    fn merge_owed_never_lets_a_progress_report_replace_a_queued_final_one() {
        let mut base = vec![crate::heartbeat::CaptureReport {
            session: "s1".to_string(),
            worker_id: "w1".to_string(),
            batches: vec![crate::heartbeat::CaptureBatch {
                columns: vec![crate::heartbeat::CaptureColumn {
                    name: "_raw".to_string(),
                    ty: "str".to_string(),
                }],
                rows: vec![crate::heartbeat::CapturedRow {
                    values: vec![serde_json::Value::String("a".to_string())],
                    failed: vec![],
                    worker: None,
                }],
            }],
            finished: Some(crate::heartbeat::CaptureEnd::Matches),
            sql: None,
            avro: Vec::new(),
        }];

        merge_owed(
            &mut base,
            vec![crate::heartbeat::CaptureReport {
                session: "s1".to_string(),
                worker_id: "w1".to_string(),
                batches: vec![],
                finished: None,
                sql: None,
                avro: Vec::new(),
            }],
        );

        assert_eq!(base.len(), 1);
        assert_eq!(base[0].batches.len(), 1, "the queued final report's rows survived");
        assert!(
            matches!(base[0].finished, Some(crate::heartbeat::CaptureEnd::Matches)),
            "a progress report replaced a final one still waiting to go out"
        );
    }

    /// A session offered again before it is sent -- `Captures::finished`'s own contract, since a leader still
    /// short of the report repeats the invitation -- replaces its place in the outbox rather than adding to it.
    /// Otherwise the queue this bounds would fill with repeated copies of the one session slowest to be
    /// acknowledged, at the expense of every other session's report.
    #[test]
    fn a_capture_offered_again_before_it_is_sent_replaces_its_place_in_the_outbox() {
        let supervisor = supervisor_for(temp_dir("capture-outbox-dedup"));
        let report = |session: &str, finished: crate::heartbeat::CaptureEnd| crate::heartbeat::CaptureReport {
            session: session.to_string(),
            worker_id: "w1".to_string(),
            batches: vec![],
            finished: Some(finished),
            sql: None,
            avro: Vec::new(),
        };

        supervisor.owe_captures(vec![report("s1", crate::heartbeat::CaptureEnd::Bytes)]);
        supervisor.owe_captures(vec![report("s2", crate::heartbeat::CaptureEnd::Bytes)]);
        supervisor.owe_captures(vec![report("s1", crate::heartbeat::CaptureEnd::Matches)]);

        let outbox = supervisor.outbox.lock().unwrap();
        assert_eq!(
            outbox.len(),
            2,
            "offering a session again before it was sent grew the outbox instead of replacing its entry"
        );
        let s1 = outbox.iter().find(|r| r.session == "s1").expect("s1 is still queued");
        assert!(
            matches!(s1.finished, Some(crate::heartbeat::CaptureEnd::Matches)),
            "the newer offer did not replace the older one"
        );
    }

    /// A session stays admitted until the leader has its report, not until this worker merely queues it for
    /// upload. Released as soon as it was handed to the outbox, a leader that repeats an unanswered invitation
    /// while the report is still waiting to go out found the session gone and admitted a second driver for what
    /// the outbox was already carrying.
    #[test]
    fn a_session_stays_admitted_until_its_report_is_actually_sent() {
        let supervisor = supervisor_for(temp_dir("capture-stays-admitted"));
        let invitation = crate::heartbeat::CaptureInvitation {
            session: "s1".to_string(),
            group: "g".to_string(),
            destination: None,
            source: Some(crate::config_file::SourceDto {
                id: "orders".to_string(),
                kind: crate::config_file::SourceKindDto::Kafka {
                    schema_registry: None,
                    read: Default::default(),
                    brokers: "b1:9092".to_string(),
                    topics: vec!["orders".to_string()],
                    group_id: "production-group".to_string(),
                    identity: None,
                    start: Default::default(),
                    stop: None,
                    fetch_bytes: Default::default(),
                    encoding: Default::default(),
                    props: Default::default(),
                },
            }),
            identity: None,
            filter: None,
            start: None,
            max_matches: 1,
            max_bytes: 1,
            // Already past, so the session is done the instant `finished()` first asks it whether it is open.
            deadline_secs: 0,
            secrets: vec![],
        };

        let admitted = supervisor.captures.admit(std::slice::from_ref(&invitation));
        assert_eq!(admitted.len(), 1, "the first invitation for a session was not admitted");

        // The heartbeat's own path: `finished()` sees the deadline has already passed and hands the report to
        // the outbox, with nothing from the leader yet acknowledging it.
        supervisor.hand_back_captures();
        assert_eq!(
            supervisor.outbox.lock().unwrap().len(),
            1,
            "a finished session was not offered to the outbox"
        );

        // The leader has not answered, so the same invitation arrives again.
        let readmitted = supervisor.captures.admit(std::slice::from_ref(&invitation));
        assert!(
            readmitted.is_empty(),
            "a session already queued for upload was admitted again, opening a second driver for it"
        );
    }

    /// The leader offers an unanswered invitation on every beat, and the deployer may be minutes from draining the
    /// queue. One session is one queued invitation however many times it arrives.
    #[test]
    fn an_invitation_repeated_while_the_deployer_is_busy_is_queued_once() {
        let supervisor = supervisor_for(temp_dir("invitation-repeat"));

        for _ in 0..50 {
            supervisor.follow_captures(vec![invited("s1")]);
        }
        supervisor.follow_captures(vec![invited("s1"), invited("s2")]);

        let held = supervisor.invitations.lock().unwrap();
        let sessions: Vec<&str> = held.queued.iter().map(|i| i.session.as_str()).collect();
        assert_eq!(sessions, vec!["s1", "s2"], "a repeated invitation was queued again");
    }

    /// The leader lists every capture it still wants on every answer, and wants nothing more of a session it leaves
    /// out: one still filling here is let go, its progress reports go with it, and its invitation leaves the queue.
    /// A finished one stays until its report is sent.
    #[test]
    fn a_capture_the_leader_no_longer_lists_is_let_go_and_unqueued() {
        let supervisor = supervisor_for(temp_dir("capture-unlisted"));
        supervisor.captures.admit(&[
            crate::heartbeat::CaptureInvitation {
                deadline_secs: 60,
                ..capture_invitation("s1")
            },
            // Already past its deadline, so it is finished the first time it is asked whether it is open.
            crate::heartbeat::CaptureInvitation {
                deadline_secs: 0,
                ..capture_invitation("s3")
            },
        ]);
        let report = |session: &str, finished: Option<crate::heartbeat::CaptureEnd>| crate::heartbeat::CaptureReport {
            session: session.to_string(),
            worker_id: "w1".to_string(),
            batches: Vec::new(),
            finished,
            sql: None,
            avro: Vec::new(),
        };
        *supervisor.outbox.lock().unwrap() = vec![
            report("s1", None),
            report("s3", Some(crate::heartbeat::CaptureEnd::Deadline)),
            report("s2", None),
        ];
        supervisor.follow_captures(vec![invited("s1"), invited("s2")]);
        {
            let mut held = supervisor.invitations.lock().unwrap();
            let queued: Vec<&str> = held.queued.iter().map(|i| i.session.as_str()).collect();
            assert_eq!(queued, vec!["s1", "s2"]);
            // As the deployer's turn leaves it, so each answer below must set it on its own.
            held.changed = false;
        }

        supervisor.follow_captures(vec![invited("s2")]);

        assert!(
            !supervisor.captures.holds("s1"),
            "an unlisted session still filling was kept"
        );
        assert!(
            supervisor.captures.holds("s3"),
            "a finished session was let go before its report was sent"
        );
        {
            let mut held = supervisor.invitations.lock().unwrap();
            let queued: Vec<&str> = held.queued.iter().map(|i| i.session.as_str()).collect();
            assert_eq!(queued, vec!["s2"], "an unlisted session's invitation stayed queued");
            assert_eq!(held.listed, HashSet::from(["s2".to_string()]));
            assert!(held.changed, "a runtime would not be told the list moved");
            held.changed = false;
        }
        let owed: Vec<String> = supervisor
            .outbox
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.session.clone())
            .collect();
        assert_eq!(
            owed,
            vec!["s3", "s2"],
            "a progress report for an unlisted session was kept, or a final was dropped"
        );

        supervisor.follow_captures(vec![]);
        let held = supervisor.invitations.lock().unwrap();
        assert!(held.queued.is_empty(), "an empty list left an invitation queued");
        assert!(held.listed.is_empty());
        assert!(held.changed, "a runtime would not be told the list emptied");
    }

    /// A fake runtime that reads its start frame, as a real one does, then leaves at once -- confirmed dead
    /// rather than merely given time, so a race that finds its pipe still open fails the test that uses this
    /// rather than passing it for the wrong reason.
    fn dead_pipe_runtime(inbox: &Arc<child::RuntimeInbox>) -> child::RuntimeHandle {
        let cmd = crate::process::shell("read -r _; exit 0", "set /p _= & exit 0");
        let mut handle = child::RuntimeHandle::start_as(
            cmd,
            child::StartSpec {
                state_dir: std::path::PathBuf::from("/nonexistent"),
                repo_dir: std::path::PathBuf::from("/nonexistent"),
                commit: "c0ffee".to_string(),
                group_id: "g".to_string(),
                thread_limit: 1,
                memory: None,
                worker_id: "w1".to_string(),
                secrets: vec![],
                incarnation: 1,
            },
            inbox,
        )
        .expect("a runtime stands up");
        let dead = (0..500).any(|_| {
            std::thread::sleep(Duration::from_millis(10));
            matches!(handle.exited(), child::Exit::Confirmed)
        });
        assert!(
            dead,
            "the fake runtime did not exit in time for this test to mean anything"
        );
        handle
    }

    fn invited(session: &str) -> crate::heartbeat::Invited {
        crate::heartbeat::Invited::Readable(Box::new(capture_invitation(session)))
    }

    /// An invitation this build cannot read is answered once with a failed report, never queued, and not answered
    /// again once the leader has the answer.
    #[test]
    fn an_invitation_this_build_cannot_read_is_answered_as_failed() {
        let supervisor = supervisor_for(temp_dir("invitation-unreadable"));
        let unreadable = || crate::heartbeat::Invited::Unreadable {
            session: "s2".to_string(),
        };
        for _ in 0..3 {
            supervisor.follow_captures(vec![invited("s1"), unreadable()]);
        }
        let queued: Vec<String> = supervisor
            .invitations
            .lock()
            .unwrap()
            .queued
            .iter()
            .map(|i| i.session.clone())
            .collect();
        assert_eq!(queued, vec!["s1"]);
        let owed = supervisor.outbox.lock().unwrap().clone();
        assert_eq!(
            owed.len(),
            1,
            "one report per session however often it is listed: {owed:?}"
        );
        assert_eq!(owed[0].session, "s2");
        assert!(
            matches!(&owed[0].finished, Some(crate::heartbeat::CaptureEnd::Failed(why)) if why.contains("cannot read"))
        );

        supervisor.outbox.lock().unwrap().clear();
        supervisor.answered("s2");
        supervisor.follow_captures(vec![unreadable()]);
        assert!(
            supervisor.outbox.lock().unwrap().is_empty(),
            "answered again after the leader had it"
        );
    }

    fn capture_invitation(session: &str) -> crate::heartbeat::CaptureInvitation {
        crate::heartbeat::CaptureInvitation {
            session: session.to_string(),
            group: "g".to_string(),
            destination: None,
            source: Some(crate::config_file::SourceDto {
                id: "orders".to_string(),
                kind: crate::config_file::SourceKindDto::Kafka {
                    schema_registry: None,
                    read: Default::default(),
                    brokers: "b1:9092".to_string(),
                    topics: vec!["orders".to_string()],
                    group_id: "production-group".to_string(),
                    identity: None,
                    start: Default::default(),
                    stop: None,
                    fetch_bytes: Default::default(),
                    encoding: Default::default(),
                    props: Default::default(),
                },
            }),
            identity: None,
            filter: None,
            start: None,
            max_matches: 1,
            max_bytes: 1,
            deadline_secs: 1,
            secrets: vec![],
        }
    }

    /// A capture is one more caller `send_within` leaves the same obligation to as `stop`: a control pipe the
    /// runtime is not reading from is a runtime this worker owes a kill, not one whose invitation is dropped with
    /// a warning while the deploy loop goes on believing it is still there.
    #[test]
    fn a_capture_the_control_pipe_will_not_take_reports_the_runtime_rather_than_only_warning() {
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = dead_pipe_runtime(&inbox);

        let supervisor = supervisor_for(temp_dir("dead-pipe-capture"));
        *supervisor.invitations.lock().unwrap() = Invitations {
            listed: ["s1".to_string()].into(),
            queued: vec![capture_invitation("s1")],
            ..Default::default()
        };

        let wedged = supervisor.route_invitations(Some(&mut handle));
        assert!(
            wedged,
            "a control pipe that will not take a frame was only logged, not reported as a runtime that is gone"
        );
    }

    /// The supervisor's metrics and logs go to the runtime once a heartbeat has recorded them, and stay owed until
    /// a frame carrying them is on the pipe. A pipe that will not take one is a runtime this worker owes a kill.
    /// With no runtime, no logs are kept for one.
    #[test]
    fn metrics_and_logs_are_mirrored_once_per_heartbeat_and_stay_owed_until_sent() {
        let _taps = crate::logs::TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = dead_pipe_runtime(&inbox);
        let supervisor = supervisor_for(temp_dir("dead-pipe-mirror"));
        supervisor.registry.incr("smeltr_capture_records_total", &[], 3);

        assert!(
            !supervisor.mirror_to(Some(&mut handle)),
            "a mirror was sent before any heartbeat asked for one"
        );
        // Into every open queue, the supervisor's among them: no subscriber is installed here to carry a
        // `tracing` call there.
        let said = crate::logs::Event {
            timestamp: std::time::SystemTime::now(),
            level: crate::logs::Level::INFO,
            site: "mirror_to.test".into(),
            message: String::new(),
            fields: Vec::new(),
            worker: None,
            thread: None,
            local_only: false,
            seq: 0,
        };
        crate::logs::mirror([said]);

        supervisor.mirror_due.store(true, Ordering::Relaxed);
        assert!(
            supervisor.mirror_to(Some(&mut handle)),
            "a control pipe that will not take the mirror was not reported"
        );
        let owed = supervisor.registry.peek_delta(crate::metrics::SUBSCRIBER_MIRROR);
        assert!(
            owed.iter().any(|s| s.key.name == "smeltr_capture_records_total"),
            "what the failed frame carried is no longer owed: {owed:?}"
        );
        let owed_logs = supervisor
            .logs_mirror
            .lock()
            .unwrap()
            .as_ref()
            .expect("a runtime is running, so the logs mirror is open")
            .drain(usize::MAX);
        assert!(
            owed_logs.iter().any(|e| e.site == "mirror_to.test"),
            "what the failed frame carried is no longer owed: {owed_logs:?}"
        );

        assert!(!supervisor.mirror_to(None));
        assert!(
            supervisor.logs_mirror.lock().unwrap().is_none(),
            "logs are kept for a runtime that is not running"
        );
    }

    /// The rest of a batch behind a wedged frame is left queued rather than served the way a worker with nothing
    /// running serves one: this worker still reports its deployment's sources deployed until the deploy loop
    /// ends it, and serving one now would mark it covered by a source nothing is reading any more.
    #[test]
    fn invitations_behind_a_wedged_frame_are_left_for_the_next_round() {
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = dead_pipe_runtime(&inbox);

        let supervisor = supervisor_for(temp_dir("dead-pipe-capture-batch"));
        supervisor.captures.set_deployed(&Arc::new(crate::metrics::Subjects {
            sources: vec!["orders".to_string()],
            ..Default::default()
        }));
        *supervisor.invitations.lock().unwrap() = Invitations {
            listed: ["s1".to_string(), "s2".to_string()].into(),
            queued: vec![capture_invitation("s1"), capture_invitation("s2")],
            ..Default::default()
        };

        let wedged = supervisor.route_invitations(Some(&mut handle));
        assert!(wedged, "the first invitation's failed send was not reported");

        let queued: Vec<String> = supervisor
            .invitations
            .lock()
            .unwrap()
            .queued
            .iter()
            .map(|i| i.session.clone())
            .collect();
        assert_eq!(
            queued,
            vec!["s2"],
            "the invitation behind the wedged frame was not left for the round that ends the runtime"
        );
    }

    /// A fake runtime that writes every frame it is sent to `file`, one per line, and reads until its pipe closes.
    fn recording_runtime(file: &Path, inbox: &Arc<child::RuntimeInbox>) -> child::RuntimeHandle {
        let file = file.display();
        // PowerShell, because it writes each line as it reads it; findstr held its output until its input ended.
        // One writer, opened once and sharing the file, because the test reads it while it is written: a writer
        // opened per line (Add-Content) is refused whenever that read holds the file.
        let cmd = crate::process::shell(
            &format!("cat > '{file}'"),
            &format!(
                "powershell -NoProfile -Command \"$w = [IO.StreamWriter]::new([IO.FileStream]::new('{file}', 'Create', 'Write', 'ReadWrite')); $w.AutoFlush = $true; while (($l = [Console]::In.ReadLine()) -ne $null) {{ $w.WriteLine($l) }}\""
            ),
        );
        child::RuntimeHandle::start_as(
            cmd,
            child::StartSpec {
                state_dir: std::path::PathBuf::from("/nonexistent"),
                repo_dir: std::path::PathBuf::from("/nonexistent"),
                commit: "c0ffee".to_string(),
                group_id: "g".to_string(),
                thread_limit: 1,
                memory: None,
                worker_id: "w1".to_string(),
                secrets: vec![],
                incarnation: 1,
            },
            inbox,
        )
        .expect("a runtime stands up")
    }

    /// The lines `file` holds once `done` accepts them, waiting up to five seconds for a runtime to write them.
    fn recorded_until(file: &Path, done: impl Fn(&[String]) -> bool) -> Vec<String> {
        let begun = Instant::now();
        loop {
            let lines: Vec<String> = std::fs::read_to_string(file)
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect();
            if done(&lines) {
                return lines;
            }
            assert!(
                begun.elapsed() < Duration::from_secs(5),
                "the runtime was never sent what was expected: {lines:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// A running runtime hears the leader's list ahead of any capture it covers, and hears an empty one too: that is
    /// the list that lets go of everything.
    #[test]
    fn the_runtime_is_told_which_captures_the_leader_still_lists() {
        let dir = temp_dir("runtime-listing");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("frames");
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = recording_runtime(&file, &inbox);
        let supervisor = supervisor_for(temp_dir("runtime-listing-supervisor"));

        supervisor.follow_captures(vec![invited("s1")]);
        assert!(!supervisor.route_invitations(Some(&mut handle)));
        let listing = r#"{"kind":"listed","sessions":["s1"]}"#;
        recorded_until(&file, |lines| {
            lines.iter().position(|l| l == listing).is_some_and(|at| {
                lines[at + 1..]
                    .iter()
                    .any(|l| l.contains(r#""kind":"capture""#) && l.contains(r#""session":"s1""#))
            })
        });

        supervisor.follow_captures(vec![]);
        assert!(!supervisor.route_invitations(Some(&mut handle)));
        recorded_until(&file, |lines| {
            lines.iter().any(|l| l == r#"{"kind":"listed","sessions":[]}"#)
        });
    }

    /// A session this process began before the runtime started stays here, still filling or finished and waiting
    /// for its report to be sent. Forwarded, it would get a second driver under this worker's id.
    #[test]
    fn a_session_this_process_holds_is_not_forwarded_to_the_runtime() {
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = dead_pipe_runtime(&inbox);

        let supervisor = supervisor_for(temp_dir("capture-served-here"));
        let admitted = supervisor.captures.admit(&[
            crate::heartbeat::CaptureInvitation {
                deadline_secs: 60,
                ..capture_invitation("s1")
            },
            // Already past its deadline, so it is finished the first time it is asked whether it is open.
            crate::heartbeat::CaptureInvitation {
                deadline_secs: 0,
                ..capture_invitation("s2")
            },
        ]);
        assert!(
            !supervisor.captures.open(&admitted[1]) && supervisor.captures.holds("s2"),
            "s2 is finished and still held"
        );
        *supervisor.invitations.lock().unwrap() = Invitations {
            listed: ["s1".to_string(), "s2".to_string()].into(),
            queued: vec![capture_invitation("s1"), capture_invitation("s2")],
            ..Default::default()
        };

        assert!(
            !supervisor.route_invitations(Some(&mut handle)),
            "a session this process holds, filling or finished, was forwarded to the runtime"
        );
        assert!(
            supervisor.invitations.lock().unwrap().queued.is_empty(),
            "a session this process holds was queued again"
        );
    }

    /// A listing is one more frame `send_within` leaves its caller owing the tree a kill for.
    #[test]
    fn a_listing_the_control_pipe_will_not_take_reports_the_runtime() {
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = dead_pipe_runtime(&inbox);

        let supervisor = supervisor_for(temp_dir("dead-pipe-listing"));
        *supervisor.invitations.lock().unwrap() = Invitations {
            changed: true,
            ..Default::default()
        };

        assert!(
            supervisor.route_invitations(Some(&mut handle)),
            "a control pipe that would not take a listing was not reported as a runtime that is gone"
        );
    }

    /// The deployer can take an invitation off the queue just before the answer that leaves its session out. By
    /// the time it routes, that answer has let the session go, and admitting it would start a fresh one the leader
    /// wants nothing of.
    #[test]
    fn an_invitation_taken_before_the_answer_that_let_it_go_is_not_served() {
        let supervisor = supervisor_for(temp_dir("capture-taken-then-unlisted"));
        // The deployer's copy of the queue, and the listing as the heartbeat left it after.
        *supervisor.invitations.lock().unwrap() = Invitations {
            queued: vec![capture_invitation("s1")],
            ..Default::default()
        };

        assert!(!supervisor.route_invitations(None));
        assert!(
            !supervisor.captures.holds("s1"),
            "a session the leader let go of was admitted again"
        );
    }

    /// The leader lists a session until it has this worker's final report, so an answer can cross the
    /// acknowledgment and list it once more, and an invitation queued before the acknowledgment can be routed
    /// after it. Neither serves the session again: not here, and not in a runtime.
    #[test]
    fn a_session_the_leader_has_the_answer_to_is_served_nowhere_again() {
        let supervisor = supervisor_for(temp_dir("capture-answered"));
        supervisor.follow_captures(vec![invited("s1")]);
        supervisor.answered("s1");

        assert!(!supervisor.route_invitations(None));
        assert!(
            !supervisor.captures.holds("s1"),
            "an invitation queued before the acknowledgment was served after it"
        );

        supervisor.follow_captures(vec![invited("s1")]);
        assert!(
            supervisor.invitations.lock().unwrap().queued.is_empty(),
            "an answer that crossed the acknowledgment queued the session again"
        );

        supervisor.follow_captures(vec![]);
        assert!(
            supervisor.invitations.lock().unwrap().answered.is_empty(),
            "an answered session was remembered past the last answer that listed it"
        );
    }

    /// With a runtime running, a session the leader has this worker's answer to is not forwarded either: the
    /// runtime would admit it fresh.
    #[test]
    #[cfg(unix)]
    fn a_session_the_leader_has_the_answer_to_is_not_forwarded_to_the_runtime() {
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = dead_pipe_runtime(&inbox);

        let supervisor = supervisor_for(temp_dir("capture-answered-runtime"));
        *supervisor.invitations.lock().unwrap() = Invitations {
            listed: ["s1".to_string()].into(),
            queued: vec![capture_invitation("s1")],
            answered: ["s1".to_string()].into(),
            changed: false,
        };

        assert!(
            !supervisor.route_invitations(Some(&mut handle)),
            "a session the leader has the answer to was forwarded to the runtime"
        );
    }

    /// A leader that stops taking captures must not grow this worker without bound, and what it keeps when it has
    /// to choose is the newest.
    #[test]
    fn an_outbox_the_leader_is_not_draining_keeps_the_newest() {
        let supervisor = supervisor_for(temp_dir("capture-outbox-full"));
        for i in 0..CAPTURES_WAITING + 4 {
            supervisor.owe_captures(vec![crate::heartbeat::CaptureReport {
                session: format!("s{i}"),
                worker_id: "w1".to_string(),
                batches: vec![],
                finished: Some(crate::heartbeat::CaptureEnd::Bytes),
                sql: None,
                avro: Vec::new(),
            }]);
        }
        let outbox = supervisor.outbox.lock().unwrap();
        assert_eq!(outbox.len(), CAPTURES_WAITING, "the outbox grew past what it may hold");
        assert_eq!(outbox[0].session, "s4", "it dropped the newest rather than the oldest");
    }

    /// A finished report is unrecoverable if this worker drops it -- a live runtime already released its own
    /// copy the moment it wrote it home. A progress report is only ever a growth tick away from being offered
    /// again, so it goes first however much older a finished report sitting behind it is.
    #[test]
    fn trim_outbox_drops_progress_reports_before_finished_ones() {
        fn report(session: &str, finished: Option<crate::heartbeat::CaptureEnd>) -> crate::heartbeat::CaptureReport {
            crate::heartbeat::CaptureReport {
                session: session.to_string(),
                worker_id: "w1".to_string(),
                batches: vec![],
                finished,
                sql: None,
                avro: Vec::new(),
            }
        }

        let mut outbox = vec![
            report("f1", Some(crate::heartbeat::CaptureEnd::Bytes)),
            report("f2", Some(crate::heartbeat::CaptureEnd::Bytes)),
        ];
        for i in 0..(CAPTURES_WAITING - 1) {
            outbox.push(report(&format!("p{i}"), None));
        }
        assert_eq!(outbox.len(), CAPTURES_WAITING + 1, "one past the bound");

        trim_outbox(&mut outbox);

        let sessions: Vec<&str> = outbox.iter().map(|r| r.session.as_str()).collect();
        assert_eq!(outbox.len(), CAPTURES_WAITING);
        assert!(
            sessions.contains(&"f1") && sessions.contains(&"f2"),
            "the oldest entries were finished reports and should have survived: {sessions:?}"
        );
        assert!(
            !sessions.contains(&"p0"),
            "the oldest progress report should have been dropped instead: {sessions:?}"
        );
    }

    /// A slow upload must not empty the outbox behind it: what stays is what `owe_captures` still counts against
    /// the cap. Taking the whole outbox for one upload pass let it grow to twice its bound while a batch of four
    /// spent minutes on the reports already drained out from under it.
    #[test]
    fn an_upload_batch_never_takes_more_than_it_is_uploading_at_once() {
        let outbox = Mutex::new(
            (0..CAPTURES_WAITING)
                .map(|i| crate::heartbeat::CaptureReport {
                    session: format!("s{i}"),
                    worker_id: "w1".to_string(),
                    batches: vec![],
                    finished: Some(crate::heartbeat::CaptureEnd::Bytes),
                    sql: None,
                    avro: Vec::new(),
                })
                .collect(),
        );

        let batch = next_upload_batch(&outbox);
        assert_eq!(
            batch.len(),
            CAPTURE_UPLOADS_AT_ONCE,
            "a batch took more than this worker uploads at once"
        );
        assert_eq!(batch[0].session, "s0", "the oldest was not what a batch took first");
        assert_eq!(
            outbox.lock().unwrap().len(),
            CAPTURES_WAITING - CAPTURE_UPLOADS_AT_ONCE,
            "what a batch left behind is what the cap must still count -- taking the whole outbox at once left \
             nothing for a concurrent arrival to be capped against"
        );
    }

    /// A stream window the leader did not acknowledge goes again under its own number, unchanged, while what the
    /// runtime sees meanwhile waits for the window after; acknowledged, the next window takes the next number.
    #[test]
    fn an_unacknowledged_stream_window_goes_again_as_it_was() {
        let supervisor = supervisor_for(temp_dir("streams-owed"));
        let unmatched = |rows| heartbeat::StreamReport {
            parsers: Vec::new(),
            unmatched: vec![heartbeat::UnmatchedStreams {
                source: "hec".to_string(),
                rows,
                keys: Vec::new(),
                keys_dropped: 0,
            }],
        };
        let report = |rows| child::RuntimeReport {
            streams: Some(unmatched(rows)),
            ..Default::default()
        };
        let beat = || {
            let mut sent = supervisor.runtime.lock().unwrap().as_mut().map(take_runtime_window);
            let seq = supervisor.streams_window(&mut sent);
            (seq, sent.and_then(|s| s.streams))
        };
        let inbox = child::RuntimeInbox::default();
        *inbox.report.lock().unwrap() = Some(report(2));
        supervisor.absorb(&inbox);
        assert_eq!(beat(), (Some(1), Some(unmatched(2))));

        // Unanswered; the runtime sees more before the next beat, and more again before the one after.
        *inbox.report.lock().unwrap() = Some(report(3));
        supervisor.absorb(&inbox);
        assert_eq!(beat(), (Some(1), Some(unmatched(2))), "the owed window changed");
        *inbox.report.lock().unwrap() = Some(report(4));
        supervisor.absorb(&inbox);
        supervisor.streams_delivered(Some(1));
        assert_eq!(beat(), (Some(2), Some(unmatched(7))), "what waited was lost");
    }

    /// A beat that lands between two of the runtime's own reports still describes its threads. Only its metrics
    /// and logs are a window a beat spends; taking the whole report, rather than just that window, reported an
    /// empty worker on every beat that did not race a fresh one from the runtime.
    #[test]
    fn a_beat_with_nothing_new_from_the_runtime_still_reports_its_threads() {
        let supervisor = supervisor_for(temp_dir("window-keeps-threads"));
        let inbox = child::RuntimeInbox::default();
        *inbox.report.lock().unwrap() = Some(child::RuntimeReport {
            pool_thread_ids: vec!["pool-0".to_string()],
            ..Default::default()
        });
        supervisor.absorb(&inbox);

        // One beat's worth of window, taken exactly as `heartbeat_forever` takes it.
        let taken = supervisor.runtime.lock().unwrap().as_mut().map(take_runtime_window);
        assert!(taken.is_some(), "a runtime that has reported once has a window to take");

        // A second beat, with nothing new from the runtime since.
        let runtime = supervisor.runtime.lock().unwrap();
        let (req, _, _, _) = build_heartbeat_request(
            Reporting {
                worker_id: "w1",
                group_id: "g",
                commit: Some("c0ffee"),
                supervising: None,
                resources: None,
                stop_after_unreachable_secs: None,
                authority_expires_millis: None,
                authority_fingerprints: Vec::new(),
                takes_authority: true,
                stopping: false,
                accepts: Vec::new(),
                took: None,
                heartbeat_interval: None,
            },
            runtime.as_ref(),
            &supervisor.registry,
            &Default::default(),
        );
        assert!(
            req.threads.iter().any(|t| t.thread_id == "pool-0"),
            "a beat that found nothing new reported an empty worker instead of the runtime's threads: {:?}",
            req.threads.iter().map(|t| &t.thread_id).collect::<Vec<_>>()
        );
    }

    /// What the runtime counted rides the heartbeat beside the thread it counted it on. The extend that carries it
    /// sits after the label resolution and was once lost to a reformat, silently: nothing else fails when a
    /// runtime's counters simply stop appearing.
    #[test]
    fn a_runtime_s_metrics_ride_the_heartbeat_under_their_thread() {
        let supervisor = supervisor_for(temp_dir("metrics-ride"));
        let inbox = child::RuntimeInbox::default();
        *inbox.report.lock().unwrap() = Some(child::RuntimeReport {
            pool_thread_ids: vec!["pool-0".to_string()],
            metrics: vec![crate::heartbeat::MetricSampleDto {
                metric: "smeltr_rows_in_total".to_string(),
                labels: vec![("thread".to_string(), "pool-0".to_string())],
                value: crate::heartbeat::MetricValueDto::Counter(9),
            }],
            ..Default::default()
        });
        supervisor.absorb(&inbox);

        let runtime = supervisor.runtime.lock().unwrap();
        let (req, _, _, taken) = build_heartbeat_request(
            Reporting {
                worker_id: "w1",
                group_id: "g",
                commit: Some("c0ffee"),
                supervising: None,
                resources: None,
                stop_after_unreachable_secs: None,
                authority_expires_millis: None,
                authority_fingerprints: Vec::new(),
                takes_authority: true,
                stopping: false,
                accepts: Vec::new(),
                took: None,
                heartbeat_interval: None,
            },
            runtime.as_ref(),
            &supervisor.registry,
            &Default::default(),
        );
        let pool = req
            .threads
            .iter()
            .find(|t| t.thread_id == "pool-0")
            .expect("the runtime's thread is reported");
        assert!(
            pool.metrics.iter().any(|m| m.metric == "smeltr_rows_in_total"),
            "the runtime's counter did not reach the request: {:?}",
            pool.metrics
        );
        assert_eq!(taken, 0, "no logs were offered, so none were taken");
    }

    /// A scrape reads the whole worker: this process's series, and its runtime's summed over every report. Once the
    /// runtime goes, so does every series only it wrote.
    #[test]
    fn a_scrape_reads_the_runtime_s_totals_across_runtimes_and_its_gauges_until_it_goes() {
        let supervisor = supervisor_for(temp_dir("metrics-scrape"));
        supervisor
            .registry
            .incr("smeltr_capture_records_total", &[("thread", "capture")], 2);
        let report = |rows: u64, depth: f64| child::RuntimeReport {
            pool_thread_ids: vec!["pool-0".to_string()],
            metrics: vec![
                crate::heartbeat::MetricSampleDto {
                    metric: "smeltr_rows_in_total".to_string(),
                    labels: vec![("thread".to_string(), "pool-0".to_string())],
                    value: crate::heartbeat::MetricValueDto::Counter(rows),
                },
                crate::heartbeat::MetricSampleDto {
                    metric: "smeltr_queue_depth".to_string(),
                    labels: vec![("thread".to_string(), "pool-0".to_string())],
                    value: crate::heartbeat::MetricValueDto::Gauge(depth),
                },
            ],
            ..Default::default()
        };
        let inbox = child::RuntimeInbox::default();
        for (rows, depth) in [(9, 4.0), (5, 1.0)] {
            *inbox.report.lock().unwrap() = Some(report(rows, depth));
            supervisor.absorb(&inbox);
        }
        let text = supervisor.scrape();
        assert!(
            text.contains("smeltr_capture_records_total{thread=\"capture\"} 2\n"),
            "{text}"
        );
        assert!(text.contains("smeltr_rows_in_total{thread=\"pool-0\"} 14\n"), "{text}");
        assert!(text.contains("smeltr_queue_depth{thread=\"pool-0\"} 1\n"), "{text}");

        supervisor.forget_runtime();
        let text = supervisor.scrape();
        assert!(!text.contains("smeltr_queue_depth"), "{text}");
        assert!(text.contains("smeltr_rows_in_total{thread=\"pool-0\"} 14\n"), "{text}");
        *inbox.report.lock().unwrap() = Some(report(3, 2.0));
        supervisor.absorb(&inbox);
        assert!(
            supervisor
                .scrape()
                .contains("smeltr_rows_in_total{thread=\"pool-0\"} 17\n"),
            "the next runtime's counts add to the last one's"
        );
        assert!(
            text.contains("smeltr_capture_records_total{thread=\"capture\"} 2\n"),
            "{text}"
        );
    }

    fn source_key(source: &str) -> heartbeat::SourceKey {
        heartbeat::SourceKey {
            source: source.to_string(),
            place: format!("s3://logs/{source}/"),
        }
    }

    fn progress_of(source: &str, entries: usize, cursor: &str) -> heartbeat::SourceProgress {
        heartbeat::SourceProgress {
            key: source_key(source),
            progress: crate::io::Progress {
                cursor: Some(cursor.to_string()),
                added: (0..entries)
                    .map(|i| heartbeat::LedgerEntry {
                        item: format!("{source}-{i}"),
                        entry: String::new(),
                    })
                    .collect(),
                removed: vec![],
            },
        }
    }

    fn ledger_entries(progress: &[heartbeat::SourceProgress]) -> usize {
        progress
            .iter()
            .map(|p| p.progress.added.len() + p.progress.removed.len())
            .sum()
    }

    /// A beat carries the oldest progress up to its cap, and the rest waits for the beats after. What a beat carried
    /// that the leader never answered goes back ahead of everything since, so the leader takes it in order.
    #[test]
    fn a_beat_carries_progress_up_to_its_cap_and_keeps_what_the_leader_has_not_taken() {
        let supervisor = supervisor_for(temp_dir("progress-cap"));
        let cap = heartbeat::LEDGER_PER_HEARTBEAT;
        let inbox = child::RuntimeInbox::default();
        *inbox.report.lock().unwrap() = Some(child::RuntimeReport {
            single_worker: vec![source_key("a"), source_key("b")],
            progress: vec![progress_of("a", cap - 1, "a1"), progress_of("a", 3, "a2")],
            ..Default::default()
        });
        supervisor.absorb(&inbox);

        let sent = supervisor
            .runtime
            .lock()
            .unwrap()
            .as_mut()
            .map(take_runtime_window)
            .unwrap();
        assert_eq!(
            ledger_entries(&sent.progress),
            cap,
            "a beat did not carry up to its cap"
        );
        let (req, _, _, _) = build_heartbeat_request(
            Reporting {
                worker_id: "w1",
                group_id: "g",
                commit: Some("c0ffee"),
                supervising: None,
                resources: None,
                stop_after_unreachable_secs: None,
                authority_expires_millis: None,
                authority_fingerprints: Vec::new(),
                takes_authority: true,
                stopping: false,
                accepts: Vec::new(),
                took: None,
                heartbeat_interval: Some(Duration::from_millis(1500)),
            },
            Some(&sent),
            &supervisor.registry,
            &Default::default(),
        );
        assert_eq!(req.progress, sent.progress);
        assert_eq!(req.single_worker, vec![source_key("a"), source_key("b")]);
        assert_eq!(
            req.heartbeat_interval_ms,
            Some(1500),
            "the leader was not told how often this worker beats"
        );

        // More arrives from the runtime, then the beat fails.
        *inbox.report.lock().unwrap() = Some(child::RuntimeReport {
            progress: vec![progress_of("a", 1, "a3")],
            ..Default::default()
        });
        supervisor.absorb(&inbox);
        if let Some(current) = supervisor.runtime.lock().unwrap().as_mut() {
            untake_runtime_window(current, sent.clone());
        }
        let cursors: Vec<Option<String>> = supervisor
            .runtime
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .progress
            .iter()
            .map(|p| p.progress.cursor.clone())
            .collect();
        assert_eq!(
            cursors,
            [
                Some("a1".to_string()),
                None,
                Some("a2".to_string()),
                Some("a3".to_string())
            ],
            "what a failed beat carried did not go back ahead of what came after it"
        );

        // The beat that lands carries the same again; the next carries only what is left.
        let again = supervisor
            .runtime
            .lock()
            .unwrap()
            .as_mut()
            .map(take_runtime_window)
            .unwrap();
        assert_eq!(again.progress, sent.progress);
        let rest = supervisor
            .runtime
            .lock()
            .unwrap()
            .as_mut()
            .map(take_runtime_window)
            .unwrap();
        assert_eq!(ledger_entries(&rest.progress), 3);
        assert_eq!(rest.progress.last().unwrap().progress.cursor.as_deref(), Some("a3"));
    }

    /// Progress outlives the runtime that read it: the leader has not had it, and nothing else will deliver it.
    #[test]
    fn forgetting_a_runtime_keeps_its_progress_and_forgets_its_sources() {
        let supervisor = supervisor_for(temp_dir("forget-progress"));
        let inbox = child::RuntimeInbox::default();
        *inbox.report.lock().unwrap() = Some(child::RuntimeReport {
            single_worker: vec![source_key("a")],
            progress: vec![progress_of("a", 2, "a1")],
            ..Default::default()
        });
        supervisor.absorb(&inbox);
        supervisor.forget_runtime();
        let held = supervisor.runtime.lock().unwrap().clone().unwrap();
        assert_eq!(held.progress, vec![progress_of("a", 2, "a1")]);
        assert!(
            held.single_worker.is_empty(),
            "a runtime that has gone still offered its sources"
        );
    }

    fn hold_of(source: &str, cursor: &str) -> heartbeat::Hold {
        heartbeat::Hold {
            key: source_key(source),
            cursor: Some(cursor.to_string()),
        }
    }

    /// A source is granted when it is new to the runtime, and again once a grant the runtime applied did not take
    /// and its backoff has run. Where a held source resumes moves with every beat, and changes nothing.
    #[test]
    fn a_grant_is_offered_again_once_the_runtime_applied_it_and_does_not_hold_it() {
        let t0 = Instant::now();
        let mut holds = Holds {
            listed: vec![hold_of("a", "c1")],
            ..Default::default()
        };
        let offers = |plan: Option<Vec<(heartbeat::Hold, Offer)>>| {
            plan.map(|p| p.into_iter().map(|(h, o)| (h.key.source, o)).collect::<Vec<_>>())
        };
        assert_eq!(
            offers(holds.plan(&[], 0, t0)),
            Some(vec![("a".to_string(), Offer::Grant)])
        );
        let frame = |offers: &[(&str, Offer)]| -> Vec<(held::Holding, Offer)> {
            offers
                .iter()
                .map(|(source, offer)| {
                    (
                        held::Holding {
                            hold: hold_of(source, "c1"),
                            ledger_file: None,
                        },
                        *offer,
                    )
                })
                .collect()
        };
        holds.sent(1, &frame(&[("a", Offer::Grant)]), t0);

        assert_eq!(
            offers(holds.plan(&[], 0, t0)),
            None,
            "a grant was judged before the runtime applied it"
        );
        let later = t0 + Duration::from_secs(1);
        assert_eq!(
            offers(holds.plan(&[], 1, later)),
            None,
            "a failed grant was offered inside its backoff"
        );
        let due = t0 + hold_retry(1) + Duration::from_millis(1);
        assert_eq!(
            offers(holds.plan(&[], 1, due)),
            Some(vec![("a".to_string(), Offer::Grant)]),
            "a grant the runtime did not make was never offered again"
        );
        holds.sent(2, &frame(&[("a", Offer::Grant)]), due);
        assert_eq!(holds.told[&source_key("a")].tries, 2);
        assert!(hold_retry(2) > hold_retry(1), "the backoff does not grow");

        assert_eq!(offers(holds.plan(&[source_key("a")], 2, due)), None);
        holds.listed = vec![hold_of("a", "c9")];
        assert_eq!(
            offers(holds.plan(&[source_key("a")], 2, due)),
            None,
            "a moved cursor was taken for a new hold"
        );
        holds.listed.push(hold_of("b", "c1"));
        assert_eq!(
            offers(holds.plan(&[source_key("a")], 2, due)),
            Some(vec![("a".to_string(), Offer::Keep), ("b".to_string(), Offer::Grant)])
        );
        holds.listed.clear();
        assert_eq!(
            offers(holds.plan(&[source_key("a")], 2, due)),
            Some(vec![]),
            "a revoke was missed"
        );
    }

    /// Route the holds until `file` records `frames` holds frames, and answer them.
    fn routed_until(
        supervisor: &Supervisor,
        handle: &mut child::RuntimeHandle,
        file: &Path,
        frames: usize,
    ) -> Vec<String> {
        let begun = Instant::now();
        loop {
            assert!(!supervisor.route_holds(Some(&mut *handle)));
            let lines: Vec<String> = std::fs::read_to_string(file)
                .unwrap_or_default()
                .lines()
                .filter(|l| l.contains(r#""kind":"holds""#))
                .map(str::to_string)
                .collect();
            if lines.len() >= frames {
                return lines;
            }
            assert!(begun.elapsed() < Duration::from_secs(5), "holds never sent: {lines:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The runtime hears the holds when they change, an empty list included. A ledger is fetched off the deployer's
    /// turn, and one that could not be fetched leaves its source kept rather than granted. A fetch superseded by a
    /// newer answer hands nothing over.
    #[test]
    fn the_runtime_is_told_which_sources_it_holds() {
        let dir = temp_dir("runtime-holds");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("frames");
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = recording_runtime(&file, &inbox);
        let supervisor = supervisor_for(temp_dir("runtime-holds-supervisor"));

        // This supervisor's leader is nowhere to fetch a ledger from.
        supervisor.follow_holds(vec![hold_of("a", "c1"), hold_of("b", "c1")]);
        let frames = routed_until(&supervisor, &mut handle, &file, 1);
        let first = &frames[0];
        assert!(first.contains(r#""seq":1"#), "{first}");
        assert!(
            first.contains(r#""source":"a""#) && first.contains(r#""source":"b""#),
            "{first}"
        );
        assert!(
            !first.contains(".ndjson"),
            "a ledger that was never fetched was handed over: {first}"
        );

        // A fetch the next answer supersedes hands nothing over.
        supervisor.follow_holds(vec![hold_of("c", "c1")]);
        assert!(!supervisor.route_holds(Some(&mut handle)));
        assert!(
            supervisor.holds.lock().unwrap().fetching.is_some(),
            "no fetch began for a grant"
        );
        supervisor.follow_holds(vec![hold_of("d", "c1")]);
        let frames = routed_until(&supervisor, &mut handle, &file, 2);
        assert!(frames[1].contains(r#""source":"d""#), "{}", frames[1]);
        assert!(
            !frames[1].contains(r#""source":"c""#),
            "a superseded fetch was handed over: {}",
            frames[1]
        );

        supervisor.follow_holds(vec![]);
        let frames = routed_until(&supervisor, &mut handle, &file, 3);
        assert!(frames[2].ends_with(r#""holds":[]}"#), "{}", frames[2]);
        std::thread::sleep(Duration::from_millis(200));
        assert!(!supervisor.route_holds(Some(&mut handle)));
        assert!(supervisor.holds.lock().unwrap().told.is_empty());
        assert_eq!(
            routed_until(&supervisor, &mut handle, &file, 3).len(),
            3,
            "a frame was sent with nothing to say"
        );
    }

    /// A runtime that starts again is granted each source with its ledger fetched afresh, whatever the leader's
    /// answer said when the source was first granted: the last runtime's reports have grown the ledger since.
    #[test]
    fn a_regrant_after_a_runtime_restart_fetches_the_ledger() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a local port");
        let port = listener.local_addr().expect("an address").port();
        let fetches = Arc::new(AtomicUsize::new(0));
        std::thread::spawn({
            let fetches = fetches.clone();
            move || {
                while let Ok((mut stream, _)) = listener.accept() {
                    use std::io::{Read, Write};
                    let mut buf = vec![0u8; 1 << 16];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    if String::from_utf8_lossy(&buf[..n]).contains("/api/ledger") {
                        fetches.fetch_add(1, Ordering::SeqCst);
                    }
                    let body = "{\"item\":\"a.log\",\"entry\":\"v1\"}\n";
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                }
            }
        });
        let dir = temp_dir("runtime-restart-holds");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("frames");
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = recording_runtime(&file, &inbox);
        let supervisor = supervisor_for(temp_dir("runtime-restart-holds-supervisor"));
        *supervisor.client.lock().unwrap() =
            Arc::new(HeartbeatClient::new(format!("http://127.0.0.1:{port}"), "t", None, true).unwrap());

        supervisor.follow_holds(vec![hold_of("a", "c1")]);
        let frames = routed_until(&supervisor, &mut handle, &file, 1);
        assert!(
            frames[0].contains(".ndjson"),
            "a grant went without its ledger: {}",
            frames[0]
        );
        assert_eq!(fetches.load(Ordering::SeqCst), 1);

        // The same answer, heard again by a runtime that has just started.
        supervisor.holds.lock().unwrap().restarted();
        supervisor.follow_holds(vec![hold_of("a", "c1")]);
        let frames = routed_until(&supervisor, &mut handle, &file, 2);
        assert!(
            frames[1].contains(".ndjson"),
            "a regrant after a restart went without its ledger: {}",
            frames[1]
        );
        assert_eq!(fetches.load(Ordering::SeqCst), 2, "the regrant fetched no ledger");
    }

    /// The runtime hears when the progress waiting here passes its bound and when it has eased, once each, and a
    /// runtime that starts hears it again.
    #[test]
    fn the_runtime_is_told_when_the_progress_waiting_for_the_leader_is_backlogged() {
        let dir = temp_dir("runtime-backlog");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("frames");
        let inbox = Arc::new(child::RuntimeInbox::default());
        let mut handle = recording_runtime(&file, &inbox);
        let supervisor = supervisor_for(temp_dir("runtime-backlog-supervisor"));
        let frames = |count: usize| {
            recorded_until(&file, |lines| {
                lines.iter().filter(|l| l.contains(r#""kind":"backlogged""#)).count() >= count
            })
            .into_iter()
            .filter(|l| l.contains(r#""kind":"backlogged""#))
            .collect::<Vec<_>>()
        };

        assert!(!supervisor.route_backlog(Some(&mut handle)));
        assert_eq!(frames(1), [r#"{"kind":"backlogged","full":false}"#]);
        assert!(!supervisor.route_backlog(Some(&mut handle)));

        *supervisor.runtime.lock().unwrap() = Some(child::RuntimeReport {
            progress: vec![progress_of("a", held::BACKLOG_FULL, "a1")],
            ..Default::default()
        });
        assert!(!supervisor.route_backlog(Some(&mut handle)));
        assert!(!supervisor.route_backlog(Some(&mut handle)));
        assert_eq!(frames(2)[1], r#"{"kind":"backlogged","full":true}"#);

        // Drained to between the marks: still full.
        let entries = held::BACKLOG_EASED + 1;
        supervisor.runtime.lock().unwrap().as_mut().unwrap().progress = vec![progress_of("a", entries, "a1")];
        assert!(!supervisor.route_backlog(Some(&mut handle)));
        supervisor.runtime.lock().unwrap().as_mut().unwrap().progress.clear();
        assert!(!supervisor.route_backlog(Some(&mut handle)));
        assert_eq!(frames(3)[2], r#"{"kind":"backlogged","full":false}"#);

        supervisor.holds.lock().unwrap().restarted();
        assert!(!supervisor.route_backlog(Some(&mut handle)));
        assert_eq!(frames(4).len(), 4, "a runtime that started was not told");
    }

    /// Each source's ledger has a file of its own per epoch, named by nothing an author wrote.
    #[test]
    fn a_ledger_file_is_named_by_its_source_and_epoch_alone() {
        let a = ledger_file_name(&source_key("a"), 1);
        assert_eq!(a, ledger_file_name(&source_key("a"), 1));
        assert_ne!(a, ledger_file_name(&source_key("b"), 1));
        assert_ne!(a, ledger_file_name(&source_key("a"), 2));
        let odd = heartbeat::SourceKey {
            source: "../x".to_string(),
            place: "/etc/passwd".to_string(),
        };
        assert!(
            ledger_file_name(&odd, 1)
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
        );
    }

    /// A leader that answers a beat with an error status has not taken what it carried, so its progress stays
    /// owed, ahead of what came after it.
    #[test]
    fn progress_in_a_beat_the_leader_refuses_stays_pending() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a local port");
        let port = listener.local_addr().expect("an address").port();
        std::thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                use std::io::{Read, Write};
                let mut buf = vec![0u8; 1 << 16];
                let _ = stream.read(&mut buf);
                let _ = stream
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            }
        });
        let supervisor = supervisor_for(temp_dir("progress-503"));
        let inbox = child::RuntimeInbox::default();
        *inbox.report.lock().unwrap() = Some(child::RuntimeReport {
            progress: vec![progress_of("a", 2, "a1")],
            ..Default::default()
        });
        supervisor.absorb(&inbox);

        let sent = supervisor.runtime.lock().unwrap().as_mut().map(take_runtime_window);
        let (req, _, _, _) = build_heartbeat_request(
            Reporting {
                worker_id: "w1",
                group_id: "g",
                commit: Some("c0ffee"),
                supervising: None,
                resources: None,
                stop_after_unreachable_secs: None,
                authority_expires_millis: None,
                authority_fingerprints: Vec::new(),
                takes_authority: true,
                stopping: false,
                accepts: Vec::new(),
                took: None,
                heartbeat_interval: None,
            },
            sent.as_ref(),
            &supervisor.registry,
            &Default::default(),
        );
        assert_eq!(req.progress, vec![progress_of("a", 2, "a1")]);
        let client = HeartbeatClient::new(format!("http://127.0.0.1:{port}"), "t", None, true).unwrap();
        assert!(client.send(&req).is_err(), "a 503 was taken for an answer");

        *inbox.report.lock().unwrap() = Some(child::RuntimeReport {
            progress: vec![progress_of("a", 1, "a2")],
            ..Default::default()
        });
        supervisor.absorb(&inbox);
        supervisor.unanswered(sent);
        assert_eq!(
            supervisor.runtime.lock().unwrap().as_ref().unwrap().progress,
            vec![progress_of("a", 2, "a1"), progress_of("a", 1, "a2")],
            "progress the leader refused was not kept, in order"
        );
    }

    /// The boundary `warn_if_heartbeat_body_large` acts on: nothing bounds metrics the way `LOGS_PER_HEARTBEAT`
    /// bounds logs, so this share of the limit is the only thing standing between silence and an operator's notice.
    #[test]
    fn a_heartbeat_body_past_the_warning_share_is_flagged_and_the_share_itself_is_not() {
        let warn_at = (heartbeat::HEARTBEAT_BODY_LIMIT as f64 * heartbeat::HEARTBEAT_BODY_WARNING) as usize;
        assert!(
            !heartbeat_body_is_large(warn_at),
            "the share itself was already flagged, so the warning fires before its own threshold"
        );
        assert!(
            heartbeat_body_is_large(warn_at + 1),
            "one byte past the warning share was not flagged"
        );
    }

    /// A worker that is running nothing says so -- no threads, no incarnation, no commit -- while still owing
    /// the leader whatever its last runtime counted and said before it went. The threads are gone; what they
    /// recorded is not, and nothing else will ever deliver it.
    #[test]
    fn forgetting_a_runtime_keeps_what_it_still_owes_and_claims_nothing_else() {
        let supervisor = supervisor_for(temp_dir("forget"));
        supervisor.supervising.lock().unwrap().incarnation = Some(4);
        supervisor.supervising.lock().unwrap().listens = vec![heartbeat::Listening {
            source: "in".to_string(),
            transport: crate::plan::topology::Transport::Tcp,
            address: "[::]:8088".parse().unwrap(),
        }];
        *supervisor.commit.lock().unwrap() = Some("c0ffee".to_string());
        let inbox = child::RuntimeInbox::default();
        *inbox.report.lock().unwrap() = Some(child::RuntimeReport {
            pool_thread_ids: vec!["pool-0".to_string()],
            metrics: vec![crate::heartbeat::MetricSampleDto {
                metric: "smeltr_rows_in_total".to_string(),
                labels: vec![],
                value: crate::heartbeat::MetricValueDto::Counter(5),
            }],
            ..Default::default()
        });
        supervisor.absorb(&inbox);

        supervisor.forget_runtime();

        assert_eq!(supervisor.commit.lock().unwrap().clone(), None);
        assert_eq!(
            supervisor.supervising.lock().unwrap().incarnation,
            None,
            "a dead runtime was still being named"
        );
        assert!(
            supervisor.supervising.lock().unwrap().listens.is_empty(),
            "a dead runtime's receivers were still being named"
        );

        let runtime = supervisor.runtime.lock().unwrap();
        let (req, _, _, _) = build_heartbeat_request(
            Reporting {
                worker_id: "w1",
                group_id: "g",
                commit: None,
                supervising: None,
                resources: None,
                stop_after_unreachable_secs: None,
                authority_expires_millis: None,
                authority_fingerprints: Vec::new(),
                takes_authority: true,
                stopping: false,
                accepts: Vec::new(),
                took: None,
                heartbeat_interval: None,
            },
            runtime.as_ref(),
            &supervisor.registry,
            &Default::default(),
        );
        assert!(
            !req.threads.iter().any(|t| t.thread_id == "pool-0"),
            "a thread that has gone was still reported"
        );
        assert!(
            req.threads.iter().any(|t| t.thread_id == "w1-bootstrap"),
            "a worker running nothing still reports, or the leader hears nothing from it at all"
        );
        assert!(
            runtime.as_ref().is_some_and(|r| !r.metrics.is_empty()),
            "what the runtime counted was thrown away with the threads that counted it"
        );
    }

    /// What git's fetch trusts of a leader nobody pinned, written once for every test.
    fn public_roots() -> &'static Path {
        static ROOTS: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        ROOTS.get_or_init(|| {
            let dir = temp_dir("public-roots");
            std::fs::create_dir_all(&dir).unwrap();
            let path = git_roots_path(&dir);
            write_git_roots(&path, None, true).unwrap();
            path
        })
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-worker-test-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn git_ok(args: &[&str], cwd: &Path) {
        let status = Command::new("git").args(args).current_dir(cwd).status().unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    /// A fixture's own `HEAD`, read through `gix` rather than the git binary -- these fixtures are still built with
    /// real git (`git_ok`), but nothing here needs to shell out just to read back what it wrote.
    fn head_of(dir: &Path) -> String {
        gix::open(dir).unwrap().head_id().unwrap().to_string()
    }

    /// A bare clone is what a worker holds: the object store and no working tree. Reading a bundle out of one is
    /// what lets discovery fetch while a deployment loads, since there are no files for either to move.
    #[test]
    fn a_bundle_loads_out_of_a_commit_with_no_working_tree() {
        let source = repo("bare-source");
        let head = head_of(&source);
        let bare = temp_dir("bare-clone");
        git_ok(
            &[
                "clone",
                "--bare",
                "--quiet",
                &source.to_string_lossy(),
                &bare.to_string_lossy(),
            ],
            &source,
        );

        assert!(is_bare_clone(&bare), "a `git clone --bare` is what this worker keeps");
        assert!(
            !bare.join("system.json").exists(),
            "a bare clone has no files checked out"
        );

        let system =
            crate::leader::tree::load_group_at(&bare, &head, "west").expect("the bundle loads from the commit");
        assert_eq!(system.topology.sources.len(), 0);
        // The same commit still answers for its content, which is what decides whether a redeploy is needed.
        let content = content_id(&bare, &head, "west").unwrap();
        assert!(content.group.is_some(), "the group subtree is in the commit");
        assert!(content.shared.is_some(), "`shared/` is in the commit");
    }

    /// A worker upgraded from a version that checked commits out finds a working tree where it now expects an
    /// object store. The clone is only a cache, so the answer is to take it again rather than repair it.
    #[test]
    fn a_working_tree_left_by_an_older_worker_is_not_mistaken_for_a_bare_clone() {
        let checked_out = repo("stale-worktree");
        assert!(
            !is_bare_clone(&checked_out),
            "a clone with a working tree is not one this worker can read commits out of"
        );

        let empty = temp_dir("never-cloned");
        std::fs::create_dir_all(&empty).unwrap();
        assert!(!is_bare_clone(&empty), "an empty directory is not a clone");
    }

    fn write(root: &Path, path: &str, body: &str) {
        let full = root.join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, body).unwrap();
    }

    /// A checkout laid out the way the leader's repository is: two groups and `shared/`.
    fn repo(name: &str) -> PathBuf {
        let dir = temp_dir(name);
        std::fs::create_dir_all(&dir).unwrap();
        git_ok(&["init", "--quiet", "-b", "main"], &dir);
        git_ok(&["config", "user.email", "test@example.com"], &dir);
        git_ok(&["config", "user.name", "test"], &dir);
        write(&dir, "groups/west/system.json", r#"{"version":1}"#);
        write(&dir, "groups/east/system.json", r#"{"version":1}"#);
        write(&dir, "shared/sources.json", "[]");
        git_ok(&["add", "-A"], &dir);
        git_ok(&["commit", "--quiet", "-m", "first"], &dir);
        dir
    }

    fn commit(dir: &Path, message: &str) -> String {
        git_ok(&["add", "-A"], dir);
        git_ok(&["commit", "--quiet", "-m", message], dir);
        head_of(dir)
    }

    /// `main` moves for every group, so this is the common case: nothing to do but remember the commit.
    #[test]
    fn another_group_s_change_leaves_this_group_s_content_alone() {
        let dir = repo("unrelated");
        let before = head_of(&dir);

        write(&dir, "groups/east/sources.json", r#"[{"id":"s"}]"#);
        let after = commit(&dir, "east only");

        assert_ne!(before, after, "the commit moved");
        assert_eq!(
            content_id(&dir, &before, "west").unwrap(),
            content_id(&dir, &after, "west").unwrap(),
            "west runs the same configuration, so nothing should restart"
        );
    }

    /// Only the leader reads samples and tests (design doc, Config plane). The GUI saves a sample to `shared/` unless
    /// told otherwise, and every group draws on `shared/`.
    #[test]
    fn a_sample_or_a_test_leaves_every_group_s_content_alone() {
        let dir = repo("samples");
        let before = head_of(&dir);

        write(
            &dir,
            "shared/samples/orders.json",
            r#"{"id":"orders","name":"orders","records":[]}"#,
        );
        write(
            &dir,
            "groups/west/samples/west.json",
            r#"{"id":"west","name":"west","records":[]}"#,
        );
        write(&dir, "groups/west/tests/west.json", "{}");
        let after = commit(&dir, "samples and a test");

        for group in ["west", "east"] {
            assert_eq!(
                content_id(&dir, &before, group).unwrap(),
                content_id(&dir, &after, group).unwrap(),
                "{group} runs what it ran"
            );
        }
    }

    /// Anything else in a group's folder counts, a file the loader does not name included: a lookup file is found by
    /// the path a table gives, not by a list.
    #[test]
    fn a_file_beside_samples_still_moves_its_group_s_content() {
        let dir = repo("lookups");
        let before = head_of(&dir);

        write(&dir, "groups/west/lookups/users.csv", "name,shell\nroot,/bin/sh\n");
        let after = commit(&dir, "a lookup file");

        assert_ne!(
            content_id(&dir, &before, "west").unwrap(),
            content_id(&dir, &after, "west").unwrap()
        );
    }

    #[test]
    fn a_change_to_this_group_s_own_subtree_moves_its_content() {
        let dir = repo("own");
        let before = head_of(&dir);

        write(&dir, "groups/west/sources.json", r#"[{"id":"s"}]"#);
        let after = commit(&dir, "west");

        assert_ne!(
            content_id(&dir, &before, "west").unwrap(),
            content_id(&dir, &after, "west").unwrap()
        );
    }

    /// A group loads `shared/` merged with its own, so a shared change is its change.
    #[test]
    fn a_shared_change_moves_every_group_s_content() {
        let dir = repo("shared");
        let before = head_of(&dir);

        write(&dir, "shared/sources.json", r#"[{"id":"s"}]"#);
        let after = commit(&dir, "shared");

        for group in ["west", "east"] {
            assert_ne!(
                content_id(&dir, &before, group).unwrap(),
                content_id(&dir, &after, group).unwrap(),
                "{group} reads shared/"
            );
        }
    }

    /// A deleted group and a never-authored one look the same to `content_id`, so the worker's own state is what
    /// tells them apart: something running means the commit removed it, and it has to stop.
    #[test]
    fn removing_a_group_is_distinguishable_from_never_having_authored_it() {
        let dir = repo("removed");
        let authored = content_id(&dir, "HEAD", "west").unwrap();
        assert!(authored.group.is_some());

        std::fs::remove_dir_all(dir.join("groups").join("west")).unwrap();
        let after = commit(&dir, "delete west");

        let removed = content_id(&dir, &after, "west").unwrap();
        assert_eq!(removed.group, None);
        assert_ne!(removed, authored, "the deletion is a change, not a no-op");
        // `shared/` is still there, so this is not "the whole tree went away".
        assert!(removed.shared.is_some());
    }

    /// A group nobody has authored, and an unsafe id, are answers rather than errors -- and they are not each other's.
    #[test]
    fn a_group_the_commit_does_not_hold_has_no_content() {
        let dir = repo("absent");
        let head = head_of(&dir);

        let missing = content_id(&dir, &head, "nobody-authored-this").unwrap();
        assert_eq!(missing.group, None);
        assert!(missing.shared.is_some(), "shared/ is there whether or not the group is");
        assert_ne!(missing, content_id(&dir, &head, "west").unwrap());
    }

    /// A pinned enrollment code says "this leader, verified". Spent over an address that presents no certificate, the
    /// secret and every heartbeat after it go out in clear text, and the authority is never used at all.
    #[test]
    fn an_authority_is_not_spent_against_an_address_that_presents_none() {
        let dir = temp_dir("authority-over-http");
        let refused = resolve_authority(&WorkerConfig {
            leader_ca_pem: Some(b"-----BEGIN CERTIFICATE-----".to_vec()),
            ..worker_config(&dir, "west")
        })
        .expect_err("an authority over plain HTTP");
        assert!(matches!(refused, Unresolved::Refused(_)), "{}", refused.message());
        assert!(refused.message().contains("clear text"), "{}", refused.message());
    }

    /// A leader that really does serve plain HTTP is a supported deployment, and a worker with nothing to verify
    /// with is not claiming otherwise.
    #[test]
    fn a_worker_with_nothing_to_verify_with_reaches_a_plain_http_leader() {
        let dir = temp_dir("nothing-over-http");
        match resolve_authority(&worker_config(&dir, "west")) {
            Ok(resolved) => assert_eq!(resolved, None),
            Err(e) => panic!("{}", e.message()),
        }
    }

    fn worker_config(dir: &Path, group: &str) -> WorkerConfig {
        WorkerConfig {
            leader_url: "http://127.0.0.1:0".into(),
            leader_ca_pem: None,
            leader_trust_public: true,
            leader_authority_dir: None,
            group_id: group.to_string(),
            thread_limit: 1,
            memory: None,
            state_dir: dir.to_path_buf(),
            enrollment_code: None,
            heartbeat_interval: Duration::from_secs(1),
            stop_after_unreachable_secs: None,
            in_leader: false,
            metrics_endpoint: None,
        }
    }

    /// Commits to `dir` group `west` reading HTTP receiver `in`, on `port` under token `t`, into Elasticsearch, and
    /// answers the commit.
    fn receiver_into_elasticsearch(dir: &Path, port: u16) -> String {
        write(
            dir,
            "groups/west/sources.json",
            &format!(
                r#"[{{"id":"in","type":"httpReceiver","port":{port},"tokens":{{"t":{{"secret":"t","version":1}}}}}}]"#
            ),
        );
        write(
            dir,
            "groups/west/destinations.json",
            r#"[{"id":"es","type":"elasticsearch","url":"http://127.0.0.1:1","defaultIndex":"i"}]"#,
        );
        write(
            dir,
            "groups/west/pipelines/p.json",
            r#"{"id":"p","name":"p","steps":[]}"#,
        );
        write(
            dir,
            "groups/west/routes.json",
            r#"[{"id":"r","source":"in","pipeline":"p","deliver":[{"to":["es"]}]}]"#,
        );
        commit(dir, "a receiver into Elasticsearch")
    }

    /// Each part of a deployment sized to the worker's memory reports the most it may hold, named for what it is and,
    /// when it is one source's or destination's, for whose.
    #[test]
    fn a_fitted_deployment_reports_each_memory_part_by_what_it_is_and_whose() {
        let dir = repo("memory-parts");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let head = receiver_into_elasticsearch(&dir, port);
        let spec = child::StartSpec {
            state_dir: dir.clone(),
            repo_dir: dir.clone(),
            commit: head,
            group_id: "west".to_string(),
            thread_limit: 1,
            memory: Some(1024 * 1024 * 1024),
            worker_id: "w1".to_string(),
            secrets: vec![],
            incarnation: 1,
        };
        let mut secrets = crate::plan::topology::HeldSecrets::default();
        secrets.set_stored([("t".to_string(), 1, "T".to_string())]);
        let registry = Arc::new(Metrics::new());
        let deployment = start_deployment(
            &spec,
            &Arc::new(capture::Captures::default()),
            &secrets,
            &registry,
            &Arc::default(),
        )
        .unwrap();

        let parts = |samples: Vec<crate::metrics::MetricSample>| {
            let mut parts: Vec<Vec<(&'static str, String)>> = samples
                .into_iter()
                .filter(|s| s.key.name == "smeltr_memory_part_bytes")
                .map(|s| {
                    assert!(
                        matches!(s.value, crate::metrics::MetricValue::Gauge(g) if g > 0.0),
                        "{s:?}"
                    );
                    let mut labels = s.key.labels;
                    labels.sort();
                    labels
                })
                .collect();
            parts.sort();
            parts
        };
        let label = |pairs: &[(&'static str, &str)]| {
            let mut labels: Vec<(&'static str, String)> = pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
            labels.push(("thread", DEPLOY_LABEL.to_string()));
            labels.sort();
            labels
        };
        // By position, as every sample labels its subject until the heartbeat resolves it to the author's name.
        let (source, destination) = ("0", "0");
        let mut expected = vec![
            label(&[("part", "supervisor")]),
            label(&[("part", "runtime")]),
            label(&[("part", "reserve"), ("source", source)]),
            label(&[("part", "lanes")]),
            label(&[("part", "reserve"), ("destination", destination)]),
            label(&[("part", "senders"), ("destination", destination)]),
            label(&[("part", "destination pool")]),
            label(&[("part", "receiver pool")]),
            label(&[("part", "columnar")]),
        ];
        expected.sort();
        assert_eq!(parts(registry.snapshot()), expected);

        // Recorded once, at deploy, and still on the heartbeat's read after the last one was sent.
        let sent = crate::heartbeat::peek_for_heartbeat(&registry);
        crate::heartbeat::commit_for_heartbeat(&registry, &sent);
        assert_eq!(parts(crate::heartbeat::peek_for_heartbeat(&registry)), expected);
        stop_deployment(deployment);
    }

    /// A target names each receiver it reads by its source, so the heartbeat can say which port is which.
    #[test]
    fn a_target_names_each_receiver_it_listens_for() {
        let dir = repo("listening");
        let head = receiver_into_elasticsearch(&dir, 18088);
        let mut supervisor = supervisor_for(temp_dir("listening-state"));
        supervisor.repo_dir = dir;
        supervisor.config.group_id = "west".to_string();
        supervisor
            .secrets
            .lock()
            .unwrap()
            .set_stored([("t".to_string(), 1, "T".to_string())]);

        let listening = supervisor.validate(&head).unwrap().listening;
        assert_eq!(listening.len(), 1, "{listening:?}");
        assert_eq!(listening[0].source, "in");
        assert_eq!(listening[0].transport, crate::plan::topology::Transport::Tcp);
        assert_eq!(listening[0].address.port(), 18088);
    }

    /// A destination that never answers holds the stop for the readers' drain and no longer: the pool behind them
    /// has nothing left to wait for, since what lands after the readers have gone is committed by nobody.
    #[test]
    fn a_destination_that_never_answers_holds_the_stop_for_one_drain() {
        let dir = repo("unreachable-destination");
        write(&dir, "groups/west/system.json", r#"{"version":1}"#);
        write(
            &dir,
            "groups/west/sources.json",
            r#"[{"id":"m","type":"metrics","bucketSeconds":1}]"#,
        );
        write(
            &dir,
            "groups/west/destinations.json",
            r#"[{"id":"d","type":"kafka","brokers":"127.0.0.1:1","defaultTopic":"t"}]"#,
        );
        write(
            &dir,
            "groups/west/pipelines/p.json",
            r#"{"id":"p","name":"p","steps":[]}"#,
        );
        write(
            &dir,
            "groups/west/routes.json",
            r#"[{"id":"r","source":"m","pipeline":"p","deliver":[{"to":["d"]}]}]"#,
        );
        let head = commit(&dir, "a destination nobody answers for");
        let spec = child::StartSpec {
            state_dir: dir.clone(),
            repo_dir: dir.clone(),
            commit: head,
            group_id: "west".to_string(),
            thread_limit: 1,
            memory: None,
            worker_id: "w1".to_string(),
            secrets: vec![],
            incarnation: 1,
        };
        let deployment = start_deployment(
            &spec,
            &Arc::new(capture::Captures::default()),
            &crate::plan::topology::HeldSecrets::default(),
            &Arc::new(Metrics::new()),
            &Arc::default(),
        )
        .unwrap();
        // Long enough for the metrics source to hand over a batch the destination will never acknowledge.
        std::thread::sleep(Duration::from_secs(3));

        let stopping = Instant::now();
        stop_deployment(deployment);
        let took = stopping.elapsed();
        assert!(
            took < DRAIN_TIMEOUT + Duration::from_secs(5),
            "the stop took {took:?}, more than one drain"
        );
    }

    /// A commit that will not run is refused before anything is torn down, so whatever was running stays. Loading
    /// and compiling happen in the runtime process, before it says it is ready; a refusal is a `Failed` frame and
    /// the supervisor keeps what it has.
    #[test]
    fn a_commit_that_does_not_load_is_refused_rather_than_started() {
        let dir = repo("no-compile");
        let captures = Arc::new(capture::Captures::default());
        let registry = Arc::new(Metrics::new());
        let secrets = crate::plan::topology::HeldSecrets::default();

        // An empty bundle deploys: no sources, no destinations, and so no threads.
        let good = head_of(&dir);
        let spec = |commit: &str| child::StartSpec {
            state_dir: dir.clone(),
            repo_dir: dir.clone(),
            commit: commit.to_string(),
            group_id: "west".to_string(),
            thread_limit: 1,
            memory: None,
            worker_id: "w1".to_string(),
            secrets: vec![],
            incarnation: 1,
        };
        let Ok(deployment) = start_deployment(&spec(&good), &captures, &secrets, &registry, &Arc::default()) else {
            panic!("an empty bundle runs")
        };
        stop_deployment(deployment);

        write(
            &dir,
            "groups/west/pipelines/p.json",
            r#"{"id":"p","steps":[{"type":"nonsense"}]}"#,
        );
        let bad = commit(&dir, "a pipeline that does not load");
        let Err(refused) = start_deployment(&spec(&bad), &captures, &secrets, &registry, &Arc::default()) else {
            panic!("a commit that does not load cannot start")
        };
        assert!(refused.contains("does not load"), "unexpected refusal: {refused}");
    }

    /// A panic while reading a commit is a refusal that says so, and the deployer goes on.
    #[test]
    fn a_commit_that_panics_the_reader_is_refused_and_named() {
        let refused = unpanicked::<()>(|| panic!("an index past the end"));
        assert_eq!(
            refused,
            Err("reading the commit panicked: an index past the end".to_string())
        );
        let owned = unpanicked::<()>(|| panic!("{} past the end", "an index"));
        assert_eq!(
            owned,
            Err("reading the commit panicked: an index past the end".to_string())
        );
    }

    /// A bundle naming a credential this host cannot obtain is refused, and named. The refusal is the runtime's
    /// because the runtime is what reads the bundle, but it happens before a connector is opened.
    #[test]
    fn a_commit_naming_a_credential_this_host_lacks_is_refused_by_name() {
        let dir = repo("missing-secret");
        write(
            &dir,
            "groups/west/destinations.json",
            r#"[{"id":"d","type":"kafka","brokers":"b:9092","defaultTopic":"t",
                 "properties":{"security.protocol":"SASL_SSL"},"credentials":{"sasl.password":{"secret":"pw","version":3}}}]"#,
        );
        let head = commit(&dir, "a destination quoting a stored secret");
        let Err(refused) = start_deployment(
            &child::StartSpec {
                state_dir: dir.clone(),
                repo_dir: dir.clone(),
                commit: head,
                group_id: "west".to_string(),
                thread_limit: 1,
                memory: None,
                worker_id: "w1".to_string(),
                secrets: vec![],
                incarnation: 1,
            },
            &Arc::new(capture::Captures::default()),
            &crate::plan::topology::HeldSecrets::default(),
            &Arc::new(Metrics::new()),
            &Arc::default(),
        ) else {
            panic!("a credential this host does not hold cannot be deployed against")
        };
        assert!(
            refused.contains("pw"),
            "the refusal should name what is missing: {refused}"
        );
    }

    /// The same failure on its next try reads the same, and a different status code does not.
    #[test]
    fn a_fetch_failure_is_the_same_failure_whatever_it_waited() {
        assert_eq!(
            super::without_timings("Failed to connect to leader port 8443 after 2 ms: Couldn't connect"),
            super::without_timings("Failed to connect to leader port 8443 after 3017 ms: Couldn't connect"),
        );
        assert_eq!(
            super::without_timings("Operation timed out after 30001 milliseconds with 0 out of 0 bytes received"),
            super::without_timings("Operation timed out after 30000 milliseconds with 12 out of 40 bytes received"),
        );
        assert_ne!(
            super::without_timings("the requested URL returned error: 409"),
            super::without_timings("the requested URL returned error: 403"),
        );
    }
}
