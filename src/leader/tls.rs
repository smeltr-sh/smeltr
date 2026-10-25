//! The leader's certificate authority.
//!
//! The leader creates an authority on its first start and signs its own server certificate with it. A worker is
//! given the authority's certificate, never the server's. That is the point: the leader can issue itself a new
//! server certificate -- for a name it did not have before, or a key it no longer trusts -- and every worker
//! accepts it already, because what the fleet trusts is the authority.
//!
//! Four files under the leader's `tls/` directory:
//!
//! ```text
//!   ca.pem       the authority's certificate  --  the one file a worker is given
//!   ca-key.pem   the authority's private key  --  owner-only; the fleet's trust rests on it
//!   cert.pem     the server certificate       --  what a client is shown
//!   key.pem      the server's private key     --  owner-only
//! ```
//!
//! The leader can serve a certificate from the certificate list instead. `served.json` names the entry and version,
//! and the certificate and key are read from the list each time the socket loads them; nothing of the entry is
//! copied here but the root a worker is given for it. The leader's own authority stays beneath, untouched, so
//! removing `served.json` is the whole of going back.
//!
//! The authority is written once and kept; losing `ca-key.pem` means a new authority and a visit to every worker.
//! The server certificate is issued at every start instead. Nobody holds a copy of it, so a fresh one costs
//! nothing, a name added to the configuration takes effect on restart, and the path that rotates it is the path
//! that runs every day. It costs one thing: a browser told to accept the certificate rather than the authority
//! asks again after every restart. Trusting `ca.pem` is what makes that once.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use time::OffsetDateTime;

use super::private_file;
use super::store::Store;

/// The authority's certificate. The file an operator copies to every worker.
pub const CA_CERT: &str = "ca.pem";
const CA_KEY: &str = "ca-key.pem";

/// The incoming authority's key while a rotation is open. Promoting it to [`CA_KEY`] is the whole of cutting
/// over: what signs the certificate this leader serves is whatever `ca-key.pem` holds.
const NEXT_CA_KEY: &str = "ca-next-key.pem";

/// An open rotation. Present only between minting the incoming authority and cutting over to it, which are days
/// apart and survive restarts, so the intent lives on disk rather than in this process.
const ROTATION: &str = "rotation.json";
const SERVER_CERT: &str = "cert.pem";
const SERVER_KEY: &str = "key.pem";

/// Which entry of the certificate list is served, where one is ([`Entry`]).
const SERVED: &str = "served.json";

/// Where a certificate was supplied as files, before the list kept certificates. Read only to say it is not served.
const SUPPLIED_BEFORE_THE_LIST: &str = "supplied";

/// The authority's subject, and so the issuer named in every certificate it signs. It is on-disk format: signing
/// for an authority already on disk means naming it exactly as that certificate does.
const AUTHORITY_SUBJECT: &str = "smeltr leader certificate authority";

/// The server certificate's subject. Clients match on the subject alternative names; this is for a human reading
/// the certificate.
const SERVER_SUBJECT: &str = "smeltr leader";

/// How long before an authority expires a rotation stops waiting for the fleet.
///
/// A deadline landing on the incoming root's last moment caps the certificate signed under it at that same
/// instant, so the leader promotes a key and then cannot issue anything that verifies. This is the room to issue
/// one and have it serve -- not a length anybody should be running on, but an authority this close to its expiry
/// is a leader with a larger problem than the wait.
const CUTOVER_MARGIN: Duration = Duration::from_secs(10 * 60);

/// How far a certificate is backdated, against a client whose clock trails the leader's. Without it a certificate
/// minted a second ago can be one that is not valid yet.
const BACKDATE: Duration = Duration::from_secs(60 * 60);

/// What the leader serves, and the one file a worker is given.
#[derive(Debug)]
pub struct Materials {
    /// The authority's certificate, PEM. A worker verifies the leader against this.
    pub ca_pem: Vec<u8>,
    /// The certificate a client is shown, PEM, with any intermediates after it.
    pub served: Vec<u8>,
    /// Set when this call created the authority: the one moment an operator has to be told to copy `ca.pem`.
    pub authority_created: bool,
    /// Whether this is an entry of the certificate list, rather than the leader's own.
    pub supplied: bool,
}

/// A version of an entry of the certificate list, as the leader serves it or rotates to it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub name: String,
    pub version: i64,
    /// The root a worker is given to verify it, PEM. Fixed when the entry is chosen: where the chain leaves its root
    /// out, the platform's roots supplied it, and what workers are handed must not move when those do.
    pub ca: String,
}

/// Prepares `dir` to serve TLS, and reports what a worker needs.
///
/// `sans` are the names the server certificate carries. `validity` is how long a certificate written here lives;
/// an authority already on disk keeps the period it was created with.
///
/// A list entry that cannot be served -- its key no longer opens, or it no longer passes [`check_supplied`] -- does
/// not stop a start. The leader serves its own certificate and [`unserved`] says why, while `served.json` still
/// names the entry: workers keep verifying by its root, and serving it again brings them back. A certificate
/// supplied as files, before the list kept certificates, is one of these.
pub fn ensure(dir: &Path, sans: Vec<String>, validity: Duration, required: &[String]) -> Result<Materials, String> {
    private_file::create_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    recover_rotation(dir)?;
    let own = ensure_own(dir, sans.clone(), validity)?;
    let why = match served(dir)? {
        Some(entry) => match entry_serves(dir, &entry, &sans, required) {
            Ok(()) => return current(dir),
            Err(e) => format!(
                "This leader is serving a \"Leader issued\" certificate instead of {:?}, which cannot be served \
                 because {e}. Choose a certificate under Source.",
                entry.name
            ),
        },
        None if dir.join(SUPPLIED_BEFORE_THE_LIST).exists() => format!(
            "This leader is serving a \"Leader issued\" certificate instead of the one in {}, which is not in the \
             certificate list. Add it under Admin → Certificates, then choose it under Source.",
            dir.join(SUPPLIED_BEFORE_THE_LIST).display()
        ),
        None => return Ok(own),
    };
    tracing::warn!(target: "leader.certificate_unserved", "{why}");
    // Parted until something is served again: everything that reads what is in force waits for that.
    with_generations(dir, |held| {
        held.disk += 1;
        held.unserved = Some(why);
    });
    Ok(Materials { supplied: false, ..own })
}

/// What an admin is told while the list entry this leader serves is not what its socket presents, since the start
/// that found it could not serve it: why, and what to do. Cleared by the next replacement of what is served.
pub fn unserved(dir: &Path) -> Option<String> {
    let held = GENERATIONS.lock().unwrap_or_else(|e| e.into_inner());
    held.as_ref()
        .and_then(|all| all.get(dir))
        .and_then(|held| held.unserved.clone())
}

/// Reads the certificate list for `dir`, which a served [`Entry`] is a version of. Registered by whatever opened
/// the list, before [`ensure`].
pub fn read_list(dir: &Path, list: std::sync::Arc<dyn Store>) {
    LISTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(Default::default)
        .insert(dir.to_path_buf(), list);
}

/// Per directory, the certificate list its entries come from. A process holding several leaders, as a test binary
/// does, holds one each.
static LISTS: std::sync::Mutex<Option<std::collections::HashMap<PathBuf, std::sync::Arc<dyn Store>>>> =
    std::sync::Mutex::new(None);

fn list_of(dir: &Path) -> Result<std::sync::Arc<dyn Store>, String> {
    LISTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|all| all.get(dir).cloned())
        .ok_or_else(|| format!("no certificate list is open for {}", dir.display()))
}

/// The chain `entry` serves, PEM, without its root.
fn entry_chain(dir: &Path, entry: &Entry) -> Result<Vec<u8>, String> {
    let chain = list_of(dir)?
        .certificate_chain(&entry.name, entry.version)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!(
                "version {} of the certificate {:?} is not in the certificate list",
                entry.version, entry.name
            )
        })?;
    let (served, _) = split_root(chain.as_bytes(), entry.ca.as_bytes())?.ok_or_else(|| {
        format!(
            "the certificate {:?} is not signed under the root it was served with",
            entry.name
        )
    })?;
    Ok(served)
}

/// [`entry_chain`], and the private key: both PEM.
fn entry_pair(dir: &Path, entry: &Entry) -> Result<(Vec<u8>, Vec<u8>), String> {
    // The chain first: a version gone from the list is said as that, not as a key that does not open.
    let chain = entry_chain(dir, entry)?;
    let opened = (list_of(dir)?)
        .secrets_for_delivery(&[(entry.name.clone(), entry.version)], &[])
        .map_err(|e| e.to_string())?;
    let key = opened.into_iter().next().ok_or_else(|| {
        format!(
            "the key of version {} of the certificate {:?} does not open",
            entry.version, entry.name
        )
    })?;
    Ok((chain, key.value.into_bytes()))
}

/// Everything serving `entry` has to satisfy, asked of it as the list holds it now.
fn entry_serves(dir: &Path, entry: &Entry, names: &[String], required: &[String]) -> Result<(), String> {
    let (chain, key) = entry_pair(dir, entry)?;
    check_supplied(&chain, &key, entry.ca.as_bytes(), names, required)
}

/// This leader's own authority and a current certificate under it, whatever is supplied on top.
///
/// Withdrawing a supplied certificate calls this first: the certificate beneath has been sitting unused for as
/// long as the supplied one was in force, and one that cannot be minted must be found out while the supplied set
/// is still there to keep serving.
pub fn ensure_own(dir: &Path, sans: Vec<String>, validity: Duration) -> Result<Materials, String> {
    let _replacing = changing();
    own(dir, sans, validity)
}

/// A certificate this leader's authority issues for `names`, as a chain ending at that root and its private key,
/// both PEM. For an entry of the certificate list, which a receiver serves and an identity presents, so it permits
/// both server and client authentication. Never outlives the authority, as the leader's own certificate never does.
pub fn issue(dir: &Path, names: Vec<String>, validity: Duration) -> Result<(String, String), String> {
    let _reading = changing();
    let (ca_pem, ca_key_pem, authority) = authority_held(dir)?;
    let mut params = leaf_params(
        names,
        validity,
        vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth],
        "those names cannot be put in a certificate",
    )?;
    cap_at(&mut params, &authority)?;
    let (leaf, key) = sign(params, &ca_key_pem, "the certificate")?;
    let (_, root) = signing_root_pem(&ca_pem, leaf.as_bytes())?;
    Ok((format!("{}\n{root}", leaf.trim_end()), key))
}

/// The fingerprint of the root [`issue`] signs under now: after a rotation's cutover, the incoming one.
pub fn signing_root_fingerprint(dir: &Path) -> Result<String, String> {
    let _reading = changing();
    Ok(authority_held(dir)?.2.fingerprint)
}

/// The authority's certificate file and private key, both PEM, and the root in that file the key signs as.
fn authority_held(dir: &Path) -> Result<(Vec<u8>, String, Described), String> {
    let ca_cert_path = dir.join(CA_CERT);
    let ca_key_path = dir.join(CA_KEY);
    private_file::check(&ca_key_path, "the certificate authority's private key")?;
    let ca_pem = std::fs::read(&ca_cert_path).map_err(|e| format!("{}: {e}", ca_cert_path.display()))?;
    let ca_key_pem = std::fs::read_to_string(&ca_key_path).map_err(|e| format!("{}: {e}", ca_key_path.display()))?;
    // The root this leader signs with, not the file's first. A rotation leaves two in there, and reading the
    // other one's expiry caps the certificate against a date that has nothing to do with it -- too early while
    // the incoming root outlives the outgoing one, too late the other way round, which is the direction that
    // serves a certificate outliving what vouches for it.
    let authority = root_of_key(&ca_pem, &ca_key_pem)?;
    Ok((ca_pem, ca_key_pem, authority))
}

/// [`ensure_own`] for a caller already holding [`CHANGING`]. Issuing a certificate is replacing what is in force,
/// so it is serialized with every other replacement -- and a cutover, which holds the lock across the promotion
/// that must not be read half-done, calls this rather than taking it twice.
fn own(dir: &Path, sans: Vec<String>, validity: Duration) -> Result<Materials, String> {
    private_file::create_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;

    let ca_cert_path = dir.join(CA_CERT);
    let ca_key_path = dir.join(CA_KEY);

    // Either half missing is no authority: a server certificate signed by a key that is gone cannot be reissued,
    // and a key whose certificate is gone signs for an authority no worker was ever given.
    let authority_created = !(ca_cert_path.exists() && ca_key_path.exists());

    match prepare(dir, sans, validity, authority_created) {
        Ok((ca_pem, served)) => Ok(Materials {
            ca_pem,
            served,
            authority_created,
            supplied: false,
        }),
        Err(e) => {
            if authority_created {
                // An authority written moments ago and never served is one the next start would find in place and
                // so never tell the operator to hand out. Nobody holds it yet, so there is nothing to lose in
                // taking it back, and a leader that starts without announcing its authority is worse.
                let _ = std::fs::remove_file(&ca_key_path);
                let _ = std::fs::remove_file(&ca_cert_path);
            }
            Err(e)
        }
    }
}

/// What is in force now, writing nothing. [`ensure`] is what puts it there and issues a certificate doing so;
/// anything that only wants to look reaches for this, or looking would change what is being looked at.
pub fn current(dir: &Path) -> Result<Materials, String> {
    let _reading = changing();
    current_held(dir)
}

/// [`current`], for a caller already holding [`changing`].
fn current_held(dir: &Path) -> Result<Materials, String> {
    if let Some(entry) = served(dir)?
        // An entry gone from the list is one a start already fell back from: what it serves is all there is.
        && let Some(served) = entry_chain(dir, &entry).map(Some).or_else(|e| match unserved(dir) {
            Some(_) => Ok(None),
            None => Err(e),
        })?
    {
        // The entry's root, and during a rotation the incoming one beside it: what every worker is given is
        // derived, so a rotation's overlap is published by writing its intent and gone with it.
        let ca_pem = match rotation(dir)? {
            Some(state) => with_incoming(entry.ca.as_bytes(), &state)?,
            None => entry.ca.into_bytes(),
        };
        return Ok(Materials {
            ca_pem,
            served,
            authority_created: false,
            supplied: true,
        });
    }
    let read = |name: &str| std::fs::read(dir.join(name)).map_err(|e| format!("{}: {e}", dir.join(name).display()));
    Ok(Materials {
        ca_pem: read(CA_CERT)?,
        served: read(SERVER_CERT)?,
        authority_created: false,
        supplied: false,
    })
}

/// The entry of the certificate list this leader serves, where it serves one.
pub fn served(dir: &Path) -> Result<Option<Entry>, String> {
    let path = dir.join(SERVED);
    match std::fs::read(&path) {
        Ok(raw) => serde_json::from_slice(&raw)
            .map(Some)
            .map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Where a leader keeps what it serves TLS with, under its data directory. Read wherever the material is written
/// or looked at, so no caller invents a second answer.
pub fn dir_of(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("tls")
}

/// Serializes reading the whole of what is in force against replacing it.
///
/// An upload and a withdrawal move whole directories. A reader between those moves can pair one set's authority
/// with another's certificate and conclude that neither vouches for the other. A leader holds its data directory
/// alone (`leader::lock`), so one lock in this process covers every reader and writer of it.
static CHANGING: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Which state of a TLS directory's files is on disk, and which one the socket serving them loaded.
///
/// A replacement changes files under [`CHANGING`]; the socket takes them on a reload that has to be awaited, and
/// so cannot be held under that lock. Nothing that flips on one side of that gap can say which state of the other
/// side it refers to -- so a reader is told the two agree while a replacement is in flight, or a reload that
/// finishes late reports success for material two changes old. A number each says exactly that, and equality is
/// the whole of the question.
///
/// Kept per directory, because that is what the pair describes: these files, and the socket presenting them. One
/// leader serves one directory, so in a running leader there is one entry -- but a process holding several, as a
/// test binary does, has one leader's replacement otherwise telling another's readers that what they hold is
/// stale. The key is the path as its caller spells it, which every path here takes from `dir_of` or passes down
/// unchanged.
///
/// A directory nothing has replaced has no entry and agrees: a leader serving plain HTTP replaces nothing and
/// builds no socket, and needs nothing to notice that it has no socket at all.
static GENERATIONS: std::sync::Mutex<Option<Generations>> = std::sync::Mutex::new(None);

type Generations = std::collections::HashMap<PathBuf, Held>;

/// Per directory: the state its files are in, the state of them the socket loaded, and why the two parted at a
/// start that could not serve the list entry named.
#[derive(Clone, Default)]
struct Held {
    disk: u64,
    socket: u64,
    unserved: Option<String>,
}

/// The state of `dir` on disk, and the state of it the socket loaded.
fn generations(dir: &Path) -> (u64, u64) {
    let held = GENERATIONS.lock().unwrap_or_else(|e| e.into_inner());
    held.as_ref()
        .and_then(|all| all.get(dir))
        .map_or((0, 0), |held| (held.disk, held.socket))
}

fn with_generations<T>(dir: &Path, change: impl FnOnce(&mut Held) -> T) -> T {
    let mut held = GENERATIONS.lock().unwrap_or_else(|e| e.into_inner());
    change(
        held.get_or_insert_with(Default::default)
            .entry(dir.to_path_buf())
            .or_default(),
    )
}

/// The state of `dir` the socket would have to load to agree.
pub fn disk_generation(dir: &Path) -> u64 {
    generations(dir).0
}

/// Whether what this leader presents is what `dir` holds.
///
/// False from the moment a replacement reaches the disk until a reload reports having loaded it. Everything answering
/// "what is this leader's authority" has to ask, because in between the disk describes a certificate no handshake
/// presents: a page that reads as settled, an enrollment code pinning a worker to a root it will never be offered.
pub fn socket_has_what_is_on_disk(dir: &Path) -> bool {
    let (disk, socket) = generations(dir);
    socket == disk
}

/// How to make the socket serve what is on disk, registered by whatever built it.
static RELOAD: std::sync::OnceLock<Reload> = std::sync::OnceLock::new();

/// How to make the socket serving a directory take what that directory holds.
type Reload = Box<dyn Fn(&Path) + Send + Sync>;

/// Says how to make the socket serve what is on disk, for the paths that replace a certificate from outside the
/// layer holding it. The same seam as [`socket_took`], in the other direction.
///
/// It is told which directory was replaced. A leader holds one and can ignore it; a process holding several has
/// no other way to tell which socket it is being asked about.
pub fn on_reload(reload: impl Fn(&Path) + Send + Sync + 'static) {
    let _ = RELOAD.set(Box::new(reload));
}

/// Makes the socket serving `dir` take what is on disk. Does nothing where nothing registered a way to, which is
/// a leader serving plain HTTP and having no socket to reload.
pub fn reload(dir: &Path) {
    if let Some(reload) = RELOAD.get() {
        reload(dir);
    }
}

/// Records that the socket serving `dir` loaded `generation`.
///
/// The generation that was read, never "the disk as it is now": the disk can be replaced again while a reload is
/// awaited, and claiming the latest would report the socket holding material it never saw. A reload that failed
/// says nothing at all, leaving the socket on the older state it still serves.
pub fn socket_took(dir: &Path, generation: u64) {
    with_generations(dir, |held| held.socket = generation);
}

/// Says what this leader presents has been replaced, and asks for the socket to catch up.
///
/// Called by every path that replaces one of these files, while it holds [`CHANGING`]. Unconditional: a start
/// bumps this before any socket exists and the reload is a no-op, which the explicit hand-over after the socket is
/// built then settles. Asking for the reload here is what makes the two numbers meet again -- without it a
/// replacement could leave the leader refusing every authority read with nothing scheduled to put it right.
fn replaced_what_is_served(dir: &Path) {
    with_generations(dir, |held| {
        held.disk += 1;
        held.unserved = None;
    });
    reload(dir);
}

fn changing() -> std::sync::MutexGuard<'static, ()> {
    CHANGING.lock().unwrap_or_else(|e| e.into_inner())
}

/// Whether a certificate of this leader's own is on disk, or a list entry is named to serve.
///
/// Checked without the lock a replacement holds, for a caller deciding whether to look further rather than one
/// asking what is in force.
///
/// `Err` where a name could not be checked -- a permission error, or the directory itself gone missing -- rather than
/// folded into `false`. `exists()` makes that same fold, and a caller reading its `false` as "no certificate" would
/// mint an enrollment code naming no root, or spend one that names none, for a leader a moment's read failure never
/// actually asked.
pub fn has_own_material(dir: &Path) -> Result<bool, String> {
    for name in [CA_CERT, SERVED] {
        let path = dir.join(name);
        if path.try_exists().map_err(|e| format!("{}: {e}", path.display()))? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// What is in force, and the rotation open over it, if any: the pair, read together.
///
/// A cutover promotes a certificate and removes the rotation record describing it inside one hold of the lock a
/// replacement takes. Asked apart, a caller can be handed the certificate from before a cutover and the rotation
/// from after it -- `None` where a moment ago it was `Some` -- and a root read that way is one this leader had
/// already stopped signing under by the time it was named.
pub fn current_and_rotation(dir: &Path) -> Result<(Materials, Option<Rotation>), String> {
    let _reading = changing();
    Ok((current_held(dir)?, rotation(dir)?))
}

/// What is in force, and nothing at all while the socket is not presenting it.
///
/// One answer rather than two. A caller that asks whether the socket agrees and then reads what is in force can be
/// told yes, have a replacement land underneath, and hand back an authority no handshake presents -- which is a
/// worker pinning a root its next request refuses. Both come from inside the lock a replacement takes.
pub fn current_if_agreed(dir: &Path) -> Result<Materials, Refused> {
    let _reading = changing();
    presenting_what_is_read(dir)?;
    current_held(dir).map_err(Refused::Unavailable)
}

/// What the socket should load, as a chain and its private key, and the state of the disk it belongs to.
///
/// The two together, under the lock, because what comes back is what [`socket_took`] will be told the socket has.
/// Read apart, a replacement landing between them pairs one state's number with another's certificate, and the
/// socket is recorded as holding material it never saw.
pub fn to_serve(dir: &Path) -> Result<(u64, Vec<u8>, Vec<u8>), String> {
    let _reading = changing();
    let generation = disk_generation(dir);
    if let Some(entry) = served(dir)?
        && unserved(dir).is_none()
    {
        let (chain, key) = entry_pair(dir, &entry)?;
        return Ok((generation, chain, key));
    }
    let read = |name: &str| std::fs::read(dir.join(name)).map_err(|e| format!("{}: {e}", dir.join(name).display()));
    Ok((generation, read(SERVER_CERT)?, read(SERVER_KEY)?))
}

/// The same condition [`socket_agrees`] asks, answered for somebody reading what is in force rather than moving a
/// rotation: what they would be handed is not what a handshake would show them, and the remedy is the same.
fn presenting_what_is_read(dir: &Path) -> Result<(), Refused> {
    if socket_has_what_is_on_disk(dir) {
        return Ok(());
    }
    Err(Refused::Conflict(
        "this leader is still presenting a certificate other than the one on disk, so this does not describe \
         what a client would be shown. Restart this leader, then ask again."
            .to_string(),
    ))
}

/// Settles a rotation interrupted between any two of its steps.
///
/// A rotation of the leader's own authority writes its next key, then its intent, then the bundle, and cuts over by
/// promoting the key and dropping the intent; one to or from a list entry writes only the intent, which publishes
/// the overlap by itself, and cuts over by writing or removing [`SERVED`]. Every gap in that order leaves a state
/// that can be read off the disk: whether what is in force is what the rotation installs says whether the cutover
/// ran ([`promoted`]), and whether what it installs is still there says whether it still could.
///
/// A start, before anything serves.
fn recover_rotation(dir: &Path) -> Result<(), String> {
    let next_key_path = dir.join(NEXT_CA_KEY);
    let Some(state) = rotation(dir)? else {
        // Written before the intent that claims it, so one left without an intent is from a beginning that died
        // and belongs to an authority no bundle holds: the bundle is written last.
        if next_key_path.exists() {
            tracing::warn!(
                target: "leader.rotation_abandoned",
                "a certificate authority rotation did not get far enough to be resumed, and was dropped"
            );
            remove_file_durably(&next_key_path)?;
        }
        return Ok(());
    };

    // The cutover installs, then drops the intent. What is in force being what it installs already is what says
    // the installing took, whatever else is still lying about.
    if promoted(dir, &state)? {
        // Where a cutover interrupted between the two lands. Asked unconditionally: the root is already gone on
        // the ordinary path, and dropping one that is not there writes nothing.
        match state.installs {
            Installs::NextKey => retire(&dir.join(CA_CERT), &state.outgoing)?,
            Installs::Supplied => retire(&dir.join(CA_CERT), &state.incoming)?,
            Installs::Own => {}
        }
        remove_rotation(dir)?;
        return remove_file_durably(&next_key_path);
    }

    // Not cut over, and nothing left to cut over with. Serving the outgoing root is what this leader has been
    // doing all along, so going back to it alone strands nobody.
    let can_still = match (&state.installs, &state.entry) {
        (Installs::NextKey, _) => next_key_path.exists(),
        (Installs::Supplied, Some(entry)) => entry_pair(dir, entry).is_ok(),
        (Installs::Supplied, None) => false,
        (Installs::Own, _) => dir.join(CA_CERT).exists() && dir.join(CA_KEY).exists(),
    };
    if !can_still {
        tracing::warn!(
            target: "leader.rotation_abandoned",
            incoming = %state.incoming,
            "what this leader was rotating to is gone, so the rotation was dropped"
        );
        retire_overlap(dir, &state)?;
        return remove_rotation(dir);
    }

    // Mid-overlap. Where the leader's own bundle is what workers are given, the bundle is the step after the
    // intent, so this is where a beginning that died between them is finished -- from the copy the intent
    // carries, which is why it carries one.
    if served(dir)?.is_none() {
        let bundle_path = dir.join(CA_CERT);
        let serving = std::fs::read(&bundle_path).map_err(|e| format!("{}: {e}", bundle_path.display()))?;
        if !describe_each(&serving, CA_CERT)?
            .iter()
            .any(|d| d.fingerprint == state.incoming)
        {
            publish_bundle(&bundle_path, &serving, &state)?;
        }
    }
    Ok(())
}

/// Everything [`ensure`] undoes on failure when it was the call that created the authority. Answers the
/// authority's certificate and the one it just signed.
fn prepare(dir: &Path, sans: Vec<String>, validity: Duration, create: bool) -> Result<(Vec<u8>, Vec<u8>), String> {
    // The name the finished certificate is checked against below, and the first a client would be given.
    let name = sans
        .first()
        .cloned()
        .ok_or_else(|| "a certificate carrying no names is one no client would accept".to_string())?;

    // Built before the authority is written, because this is where a name a certificate cannot carry is refused,
    // and an operator who has to fix one should not be left holding an authority they were never told about.
    let mut server = server_params(sans, validity)?;

    let ca_cert_path = dir.join(CA_CERT);
    let ca_key_path = dir.join(CA_KEY);
    if create {
        create_authority(&ca_cert_path, &ca_key_path, validity)?;
    }
    let (ca_pem, ca_key_pem, authority) = authority_held(dir)?;

    cap_at(&mut server, &authority)?;

    issue_server_cert(dir, server, &ca_key_pem)?;

    let server_cert_path = dir.join(SERVER_CERT);
    let served = std::fs::read(&server_cert_path).map_err(|e| format!("{}: {e}", server_cert_path.display()))?;
    verify(&ca_pem, &served, &name)?;
    Ok((ca_pem, served))
}

/// Refuses an authority that does not vouch for the certificate just signed with it.
///
/// Both files existing says nothing about their being a pair. Restored from separate backups, or copied by hand,
/// they leave the leader signing with one authority and handing out another, and every client refusing every
/// connection with nothing to say why. This is the leader finding that out about itself, before serving anything.
fn verify(ca_pem: &[u8], server_cert_pem: &[u8], name: &str) -> Result<(), String> {
    use rustls::client::danger::ServerCertVerifier;

    let unusable = |e: &dyn std::fmt::Display| format!("{CA_CERT} is not a usable certificate authority: {e}");

    // Every root in the file, because the file is a trust store: during a rollover it holds the outgoing root
    // beside the incoming one, and which of them signs what this leader serves is not the file's order to decide.
    let mut roots = rustls::RootCertStore::empty();
    for root in ders(ca_pem, CA_CERT)? {
        roots.add(root).map_err(|e| unusable(&e))?;
    }
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(roots.into(), crate::crypto::in_use())
        .build()
        .map_err(|e| unusable(&e))?;

    let name = rustls::pki_types::ServerName::try_from(name.to_string())
        .map_err(|e| format!("{name:?} is not a name a certificate can be checked against: {e}"))?;
    // A chain is a leaf followed by whatever vouches for it up to, but not including, the root. Handing the
    // verifier only the leaf refuses every certificate issued through an intermediate, which is most of them.
    let chain = ders(server_cert_pem, SERVER_CERT)?;
    let (leaf, intermediates) = chain
        .split_first()
        .ok_or_else(|| format!("{SERVER_CERT} holds no certificate"))?;
    verifier
        .verify_server_cert(leaf, intermediates, &name, &[], rustls::pki_types::UnixTime::now())
        .map_err(|e| {
            format!(
                "the certificate this leader would serve does not verify against {CA_CERT}: {e}. They are not a \
                 pair -- restore both from one backup, or remove both and let the leader write a new \
                 authority."
            )
        })?;
    Ok(())
}

/// Serves `entry` of the certificate list in place of this leader's own certificate.
///
/// For an entry under the root workers already verify by; one under another is reached by
/// [`begin_supplied_rotation`]. `entry.ca` is what a worker must be given to verify it -- the leader's own authority
/// says nothing about somebody else's certificate, so an entry that did not carry its own root would leave every
/// worker unable to verify anything, including the worker inside this process.
///
/// `names` are the names this leader is reached by. A certificate that carries none of them verifies perfectly
/// and is still a leader nobody can open, over the same connection the request arrived on.
pub fn serve(dir: &Path, entry: &Entry, names: &[String], required: &[String]) -> Result<(), String> {
    let _replacing = changing();
    entry_serves(dir, entry, names, required)?;
    write_served(dir, Some(entry))
}

/// Names `entry` as what is served, or with `None` goes back to this leader's own certificate, for a caller holding
/// [`CHANGING`]. One file written whole, so a crash leaves what was served or what replaces it.
fn write_served(dir: &Path, entry: Option<&Entry>) -> Result<(), String> {
    // What is served starts moving here. Everything before it changed nothing a handshake reaches.
    replaced_what_is_served(dir);
    let path = dir.join(SERVED);
    match entry {
        Some(entry) => {
            let json = serde_json::to_vec_pretty(entry).map_err(|e| e.to_string())?;
            write_durably(&path, &json).map_err(|e| format!("{}: {e}", path.display()))
        }
        None => remove_file_durably(&path),
    }
}

/// Everything a list entry has to satisfy to be served. Run when it is chosen, and again at every start and at a
/// cutover: a certificate passes its expiry while it waits, and nothing else would look at it again.
pub fn check_supplied(
    chain: &[u8],
    key: &[u8],
    ca: &[u8],
    names: &[String],
    required: &[String],
) -> Result<(), String> {
    let described = describe(chain, "the certificate")?;

    let now = OffsetDateTime::now_utc().unix_timestamp() * 1000;
    if now >= described.not_after_millis {
        return Err("the certificate has already expired".to_string());
    }
    if now < described.not_before_millis {
        return Err("the certificate is not valid yet".to_string());
    }

    // The authority's own dates, which `verify` does not check: rustls does not read a root's expiry at all. An
    // expired root leaves heartbeats succeeding through rustls while git's OpenSSL refuses the same authority,
    // which is a worker that looks healthy and has stopped taking configuration.
    //
    // The root that signs what would be served, not the first in the file. A rollover bundle holds the outgoing
    // root beside the incoming one, and only the signer's dates bound the certificate under it.
    let authority = signing_root(ca, chain)?;
    if now >= authority.not_after_millis {
        return Err("the authority has already expired, so every worker given it would refuse this leader".to_string());
    }
    // Its start too. A root that is not valid yet fails the same way an expired one does, and in the same place:
    // rustls does not look, and git's OpenSSL does.
    if now < authority.not_before_millis {
        return Err("the authority is not valid yet, so every worker given it would refuse this leader".to_string());
    }
    // Against every certificate needed to build the chain, not the root alone. An intermediate that expires
    // first takes the whole chain with it, while the leaf and the root this page shows both still read as valid.
    let mut earliest = authority.not_after_millis;
    for issuer in describe_each(chain, "the certificate")?.iter().skip(1) {
        if now >= issuer.not_after_millis {
            return Err(format!(
                "an intermediate in this chain ({}) has already expired, so no client will accept the certificate \
                 it vouches for.",
                issuer.subject
            ));
        }
        earliest = earliest.min(issuer.not_after_millis);
    }
    if described.not_after_millis > earliest {
        return Err(
            "the certificate outlives something its chain needs -- the authority that signed it, or an \
             intermediate between them. Clients disagree about such a certificate, so a worker would keep \
             heartbeating while its git fetch failed."
                .to_string(),
        );
    }

    // Asked of the verifier rather than by comparing strings, and of every name this leader is reached at rather
    // than the first. A client matches a name against a certificate by rules a string comparison does not know:
    // `*.example.com` covers `leader.example.com`, and a name differing only in case is the same name. Doing it
    // here the way a browser will do it is the only way to be sure the answer agrees.
    let mut refused = None;
    let reached_at = names.iter().find(|name| match verify(ca, chain, name) {
        Ok(()) => true,
        Err(e) => {
            refused = Some(e);
            false
        }
    });
    // `names` asks for one; these ask for each. A name here is one that breaks with nobody watching if the
    // certificate drops it: the worker inside this process, whose git fetch fails hostname verification at once
    // and whose heartbeats fail on the next connection, and the name this upload itself arrived over, which is
    // the session performing it. A certificate covering `leader.example.com` and nothing else satisfies `names`
    // through `localhost` and takes both with it.
    for name in required {
        verify(ca, chain, name).map_err(|e| {
            format!(
                "this leader is reached at {name:?} by something that cannot be told to use another name -- the \
                 worker inside this process, or the session uploading this -- and this certificate does not serve \
                 it: {e}. Add it to the certificate, or change what reaches this leader by it."
            )
        })?;
    }

    if reached_at.is_none() {
        return Err(format!(
            "no name this leader is reached at ({names:?}) is one this certificate serves. It carries {:?}, and \
             the last refusal was: {}",
            described.names,
            refused.unwrap_or_else(|| "it carries no name at all".to_string())
        ));
    }

    // Last, because it is the one check that reads the private key: rustls refuses a key that does not belong to
    // the certificate, which is the pairing nothing above would catch.
    drop(rustls_config(chain, key)?);
    Ok(())
}

/// Gives back the leader's own authority, by no longer serving the list entry. The authority was never touched, so
/// there is nothing to restore.
pub fn uninstall(dir: &Path) -> Result<(), String> {
    let _replacing = changing();
    write_served(dir, None)
}

// -- rotating the authority workers verify by ------------------------------------

/// A rotation of the authority workers verify this leader by, from the moment the incoming root is published to
/// the moment what is served is signed under it.
///
/// Replacing an authority reaches every worker, and a worker is reached only when it asks. So the incoming root
/// goes out first and is signed under second: for the length of the overlap the authority file of the set in
/// force holds both, every worker that heartbeats takes the pair, and what this leader serves does not move.
/// Cutting over before the fleet has the incoming root is what silently strands a worker -- its heartbeats keep
/// succeeding on a connection already open while its `git fetch` is refused.
///
/// The overlap is the same whatever the cutover installs; [`Installs`] is the one place they differ.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rotation {
    /// The root being moved to. Carried whole because it is what makes every step after the first replayable: a
    /// start that finds this and a bundle without it can put it back.
    pub incoming_pem: String,
    pub incoming: String,
    /// The root still being served, so a caller can say which of the two in the bundle is which without matching
    /// keys to certificates itself.
    pub outgoing: String,
    pub started_millis: i64,
    /// When this leader signs under the incoming root whatever the fleet has reported. An operator rotating the
    /// leader's own authority sets it, because only they know how long their workers take to come round, and
    /// without one a single worker that never returns holds the rotation open forever. A switch of source takes
    /// the served certificate's expiry: the wait costs nothing until then, and past it there is nothing to wait
    /// with.
    pub deadline_millis: i64,
    /// Absent from files written before a cutover could install anything but the next key.
    #[serde(default)]
    pub installs: Installs,
    /// The list entry the cutover serves, for [`Installs::Supplied`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<Entry>,
}

/// What cutting over puts in force. The overlap before it, and the recovery around it, do not depend on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Installs {
    /// `ca-next-key.pem`, promoted to `ca-key.pem`: another authority of this leader's own.
    #[default]
    NextKey,
    /// The list entry [`Rotation::entry`] names.
    Supplied,
    /// This leader's own authority, by no longer serving the list entry over it.
    Own,
}

/// The open rotation, if there is one. `None` is the ordinary state: one authority, signed under.
pub fn rotation(dir: &Path) -> Result<Option<Rotation>, String> {
    let path = dir.join(ROTATION);
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    serde_json::from_slice(&raw).map(Some).map_err(|e| {
        format!(
            "{} is not a rotation this leader can read: {e}. Remove it to abandon the rotation and keep serving \
             the authority it was moving from.",
            path.display()
        )
    })
}

/// Why something was refused, and whether asking again could change the answer.
///
/// The two are different facts for whoever is waiting: a conflict wants an operator to do something else first,
/// and an unreadable file wants nothing from them at all. Collapsing them tells a client watching this leader
/// that a storage failure is a permanent state of its own.
#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    /// The state is what it is. Nothing is wrong with the request.
    Conflict(String),
    /// A file could not be read or parsed.
    Unavailable(String),
}

impl Refused {
    pub fn message(&self) -> &str {
        match self {
            Refused::Conflict(why) | Refused::Unavailable(why) => why,
        }
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

/// Every reason a rotation that would install `installs` refuses to begin, for a caller about to write down that
/// one began.
///
/// One list, asked in two places: the act re-asks it under the lock that makes the answer binding, and a caller
/// asks it first so a refusal leaves no row claiming otherwise. Two copies of this drifted once already.
pub fn rotation_may_begin(dir: &Path, installs: Installs) -> Result<(), Refused> {
    if rotation(dir).map_err(Refused::Unavailable)?.is_some() {
        return Err(Refused::Conflict(
            "this leader is already rotating its authority. Cut that one over, or abandon it, first.".to_string(),
        ));
    }
    let supplied = served(dir).map_err(Refused::Unavailable)?.is_some();
    match installs {
        // A root minted under a supplied certificate would be one nothing served is signed under: the fleet would
        // move to it and verify nothing by it.
        Installs::NextKey if supplied => {
            return Err(Refused::Conflict(
                "this leader serves a certificate an operator supplied, so the authority workers verify \
                 against is not this leader's to rotate. Switch back to the leader's own certificate first."
                    .to_string(),
            ));
        }
        Installs::Own if !supplied => {
            return Err(Refused::Conflict(
                "this leader is already serving its own authority, so there is nothing to withdraw.".to_string(),
            ));
        }
        _ => {}
    }
    // What the socket presents is not what `ca.pem` describes, so the root a rotation would move away from is not
    // the one any worker is verifying by. Minting against the file would publish an overlap the fleet never sees.
    // Going back to the leader's own from an entry a start could not serve is the exception: no worker verifying
    // by that entry's root reaches this leader to be given an overlap, so it goes back at once.
    if !socket_has_what_is_on_disk(dir) && !(installs == Installs::Own && unserved(dir).is_some()) {
        return Err(Refused::Conflict(
            "this leader is presenting a certificate other than the one on disk, so what its workers verify by \
             is not what a rotation would move them off. Restart it first."
                .to_string(),
        ));
    }
    Ok(())
}

/// Mints the incoming authority and publishes it beside the outgoing one, changing nothing about what is served.
///
/// `deadline_millis` is when [`cut_over_if_due`] stops waiting for the fleet.
pub fn begin_rotation(dir: &Path, validity: Duration, deadline_millis: i64, now_ms: i64) -> Result<Rotation, String> {
    let _changing = changing();
    rotation_may_begin(dir, Installs::NextKey).map_err(|e| e.message().to_string())?;

    let ca_cert_path = dir.join(CA_CERT);
    let ca_key_path = dir.join(CA_KEY);
    let serving = std::fs::read(&ca_cert_path).map_err(|e| format!("{}: {e}", ca_cert_path.display()))?;
    let ca_key_pem = std::fs::read_to_string(&ca_key_path).map_err(|e| format!("{}: {e}", ca_key_path.display()))?;
    let outgoing = root_of_key(&serving, &ca_key_pem)?;

    let key = KeyPair::generate().map_err(|e| format!("generate the incoming authority's key: {e}"))?;
    let cert = authority_params(validity)?
        .self_signed(&key)
        .map_err(|e| format!("sign the incoming authority's certificate: {e}"))?;
    let incoming = describe(cert.pem().as_bytes(), "the incoming authority")?;

    // Never past either authority's own expiry, however long an operator said they would wait. Beyond the
    // outgoing one the fleet is being served by an authority that has run out; beyond the incoming one there is
    // nothing left to cut over to. `SMELTR_LEADER_CA_VALIDITY` goes down to a day, so a fortnight's wait is a
    // reachable way to ask for both.
    // Short of either expiry rather than up to it, by [`CUTOVER_MARGIN`].
    let margin = CUTOVER_MARGIN.as_millis() as i64;
    let deadline_millis = deadline_millis
        .min(outgoing.not_after_millis - margin)
        .min(incoming.not_after_millis - margin)
        .max(now_ms);

    let state = Rotation {
        incoming_pem: cert.pem(),
        incoming: incoming.fingerprint.clone(),
        outgoing: outgoing.fingerprint.clone(),
        started_millis: now_ms,
        deadline_millis,
        installs: Installs::NextKey,
        entry: None,
    };

    // Key, then intent, then the bundle every worker is given. Each step is what makes the next one's absence
    // recoverable, and [`recover`] is where each is put right: a key nothing claims is removed, and an intent
    // whose root never reached `ca.pem` puts it there from the copy it carries.
    let next_key_path = dir.join(NEXT_CA_KEY);
    private_file::replace(&next_key_path, key.serialize_pem().as_bytes())
        .map_err(|e| format!("{}: {e}", next_key_path.display()))?;
    if let Err(e) = open(dir, &state, now_ms) {
        // A rotation whose root reached no file is one nothing can finish: promoting its key would leave
        // `ca-key.pem` signing for an authority `ca.pem` does not hold, and every path out of that reads the same
        // way. Nothing was promised to anybody yet, so the whole of it goes back.
        let _ = remove_file_durably(&dir.join(NEXT_CA_KEY));
        return Err(e);
    }
    Ok(state)
}

/// Writes the intent, then publishes the incoming root beside the outgoing one. The two steps every rotation opens
/// with, in the order [`recover_rotation`] relies on: an intent whose root never reached the bundle puts it there
/// from the copy it carries. Where a list entry is served, the intent is the publishing: what workers are given
/// for it is derived ([`current`]).
///
/// Every root this leader has ever rotated away from would otherwise stay in every worker's trust store for
/// good. rustls does not read a trust anchor's expiry, so one of them is a root that goes on vouching for whoever
/// holds its key long after the date says it stopped. A rotation is the one moment a trust store is replaced
/// wholesale, so it is where they go. Never the one still being served.
///
/// Nothing is promised to anybody until the bundle is written, so a failure takes the intent back with it.
fn open(dir: &Path, state: &Rotation, now_ms: i64) -> Result<(), String> {
    write_rotation(dir, state)?;
    if served(dir)?.is_some() {
        return Ok(());
    }
    let bundle_path = dir.join(CA_CERT);
    let published = std::fs::read(&bundle_path)
        .map_err(|e| format!("{}: {e}", bundle_path.display()))
        .and_then(|serving| retire_expired(&serving, &state.outgoing, now_ms))
        .and_then(|kept| publish_bundle(&bundle_path, &kept, state));
    if let Err(e) = published {
        let _ = remove_rotation(dir);
        return Err(e);
    }
    Ok(())
}

/// What a switch of source waits for. Every caller that rotates to a set already signed has the same bound:
/// the served certificate's expiry, short of which nothing is lost by waiting, and the incoming set's, past
/// which there is nothing to wait with. Never before `now_ms`.
fn switch_deadline(
    served: &[u8],
    incoming_chain: &[u8],
    incoming_root: &Described,
    now_ms: i64,
) -> Result<i64, String> {
    let margin = CUTOVER_MARGIN.as_millis() as i64;
    let served = describe(served, "the certificate")?;
    let incoming = describe(incoming_chain, "the incoming certificate")?;
    Ok((served.not_after_millis - margin)
        .min(incoming.not_after_millis - margin)
        .min(incoming_root.not_after_millis - margin)
        .max(now_ms))
}

/// The latest an open rotation can wait: short of every expiry it depends on by [`CUTOVER_MARGIN`], the same
/// bound each kind of rotation began under. [`deadline_may_move`] refuses a deadline past it.
pub fn latest_deadline(dir: &Path, state: &Rotation) -> Result<i64, String> {
    let _reading = changing();
    latest_deadline_held(dir, state)
}

fn latest_deadline_held(dir: &Path, state: &Rotation) -> Result<i64, String> {
    let materials = current_held(dir)?;
    let incoming = describe(state.incoming_pem.as_bytes(), "the incoming authority")?;
    match state.installs {
        Installs::NextKey => {
            let margin = CUTOVER_MARGIN.as_millis() as i64;
            let outgoing = signing_root(&materials.ca_pem, &materials.served)?;
            Ok((outgoing.not_after_millis - margin).min(incoming.not_after_millis - margin))
        }
        Installs::Supplied => switch_deadline(&materials.served, &incoming_chain(dir, state)?, &incoming, i64::MIN),
        Installs::Own => {
            let own_path = dir.join(SERVER_CERT);
            let own = std::fs::read(&own_path).map_err(|e| format!("{}: {e}", own_path.display()))?;
            switch_deadline(&materials.served, &own, &incoming, i64::MIN)
        }
    }
}

/// The chain the open rotation's list entry serves.
fn incoming_chain(dir: &Path, state: &Rotation) -> Result<Vec<u8>, String> {
    let entry = state
        .entry
        .as_ref()
        .ok_or_else(|| "the rotation names no certificate to put in force".to_string())?;
    entry_chain(dir, entry)
}

/// Every reason the open rotation's deadline cannot move to `deadline_millis`, for a caller about to write down
/// that it moved. [`set_deadline`] re-asks it under the lock that makes the answer binding.
///
/// A time already past is refused rather than taken as "now": cutting over at once is its own act, and an
/// operator asks for it by name.
pub fn deadline_may_move(dir: &Path, deadline_millis: i64, now_ms: i64) -> Result<Rotation, Refused> {
    let state = rotation(dir)
        .map_err(Refused::Unavailable)?
        .ok_or_else(|| Refused::Conflict("no rotation is open, so there is no cutover to move.".to_string()))?;
    if deadline_millis <= now_ms {
        return Err(Refused::Conflict(
            "that time has passed. Pick a later one, or cut over now.".to_string(),
        ));
    }
    if deadline_millis > latest_deadline_held(dir, &state).map_err(Refused::Unavailable)? {
        return Err(Refused::Conflict(
            "that is later than this rotation can wait: 10 minutes before a certificate it depends on expires."
                .to_string(),
        ));
    }
    Ok(state)
}

/// Moves the open rotation's deadline, which [`cut_over_if_due`] reads afresh each time it asks.
pub fn set_deadline(dir: &Path, deadline_millis: i64, now_ms: i64) -> Result<Rotation, Refused> {
    let _changing = changing();
    let mut state = deadline_may_move(dir, deadline_millis, now_ms)?;
    state.deadline_millis = deadline_millis;
    write_rotation(dir, &state).map_err(Refused::Unavailable)?;
    Ok(state)
}

/// Splits an uploaded chain into what to serve and the root a worker is given.
///
/// The root is the self-signed authority the chain ends with, where it ends with one; else the one in `authority`
/// that signed it; else the platform's. `None` is a chain none of these vouches for, which the caller words for its
/// own form. A public authority's full chain leaves its root out, which is why the platform is asked at all.
pub fn split_root(chain: &[u8], authority: &[u8]) -> Result<Option<(Vec<u8>, String)>, String> {
    let mut certs = blocks(chain)?;
    if certs.len() > 1
        && let Some(last) = certs.last()
        && self_signed_authority(last.as_bytes())?
    {
        let root = certs.pop().expect("more than one");
        let served = certs.iter().map(|c| format!("{c}\n")).collect::<String>().into_bytes();
        // The root was read from this chain, so it signed it unless the chain is not one: refused here rather than
        // served with a root that vouches for nothing in it.
        signing_root(root.as_bytes(), &served)?;
        return Ok(Some((served, format!("{root}\n"))));
    }
    if !authority.iter().all(u8::is_ascii_whitespace)
        && let Ok((_, pem)) = signing_root_pem(authority, chain)
    {
        return Ok(Some((chain.to_vec(), pem)));
    }
    let public = crate::io::trust::pem(None).map_err(|e| e.to_string())?;
    Ok(signing_root_pem(public.as_bytes(), chain)
        .ok()
        .map(|(_, pem)| (chain.to_vec(), pem)))
}

/// Whether `pem` is an authority that signed itself: the end of a chain that carries its root.
fn self_signed_authority(pem: &[u8]) -> Result<bool, String> {
    let der = der(pem, "the certificate")?;
    let (_, cert) = x509_parser::parse_x509_certificate(der.as_ref())
        .map_err(|e| format!("the certificate is not one this can read: {e}"))?;
    Ok(cert.is_ca() && cert.subject() == cert.issuer() && cert.verify_signature(None).is_ok())
}

/// The root `chain` is signed under, as the one PEM block of `ca` that holds it.
pub(crate) fn signing_root_pem(ca: &[u8], chain: &[u8]) -> Result<(Described, String), String> {
    let root = signing_root(ca, chain)?;
    let mut only = Vec::new();
    for block in blocks(ca)? {
        if describe(block.as_bytes(), CA_CERT)?.fingerprint == root.fingerprint {
            only.push(block);
        }
    }
    let pem = only
        .into_iter()
        .next()
        .ok_or_else(|| format!("{CA_CERT} does not hold the root it was just read from"))?;
    Ok((root, format!("{pem}\n")))
}

/// Whether serving a certificate would change the root workers verify by, which is what decides between serving it
/// at once and rotating to it.
pub fn supplying_changes_root(dir: &Path, chain: &[u8], ca: &[u8]) -> Result<bool, String> {
    let materials = current(dir)?;
    Ok(signing_root(ca, chain)?.fingerprint != signing_root(&materials.ca_pem, &materials.served)?.fingerprint)
}

/// Whether going back from the list entry served to the leader's own would change the root workers verify by. The
/// leader's own authority is on disk: [`ensure_own`] ran first, as going back always has it.
pub fn withdrawing_changes_root(dir: &Path) -> Result<bool, String> {
    let _reading = changing();
    let materials = current_held(dir)?;
    let served = materials.served;
    if !materials.supplied {
        return Ok(false);
    }
    let own_ca_path = dir.join(CA_CERT);
    let own_ca = std::fs::read(&own_ca_path).map_err(|e| format!("{}: {e}", own_ca_path.display()))?;
    let own_key_path = dir.join(CA_KEY);
    let own_key = std::fs::read_to_string(&own_key_path).map_err(|e| format!("{}: {e}", own_key_path.display()))?;
    Ok(root_of_key(&own_ca, &own_key)?.fingerprint != signing_root(&materials.ca_pem, &served)?.fingerprint)
}

/// Takes a list entry in under a rotation: its root goes out to the fleet beside the one in force, and what is
/// served does not move until [`cut_over`] serves it.
///
/// For an entry whose root is not the one in force. One under the same root is a renewal, which [`serve`] serves at
/// once: every worker already verifies by it.
pub fn begin_supplied_rotation(
    dir: &Path,
    entry: &Entry,
    names: &[String],
    required: &[String],
    now_ms: i64,
) -> Result<Rotation, String> {
    let _changing = changing();
    rotation_may_begin(dir, Installs::Supplied).map_err(|e| e.message().to_string())?;
    entry_serves(dir, entry, names, required)?;
    let chain = entry_chain(dir, entry)?;

    let materials = current_held(dir)?;
    let outgoing = signing_root(&materials.ca_pem, &materials.served)?;
    let (incoming, incoming_pem) = signing_root_pem(entry.ca.as_bytes(), &chain)?;
    if incoming.fingerprint == outgoing.fingerprint {
        return Err("this certificate is signed under the authority already in force, so it can be served at once rather than rotated to.".to_string());
    }

    let state = Rotation {
        deadline_millis: switch_deadline(&materials.served, &chain, &incoming, now_ms)?,
        incoming_pem,
        incoming: incoming.fingerprint,
        outgoing: outgoing.fingerprint,
        started_millis: now_ms,
        installs: Installs::Supplied,
        entry: Some(entry.clone()),
    };
    open(dir, &state, now_ms)?;
    Ok(state)
}

/// Goes back from the list entry served to the leader's own authority under a rotation: this leader's own root goes
/// out to the fleet beside the entry's, and the entry keeps serving until [`cut_over`] stops serving it.
///
/// This leader's own authority and a certificate under it are already on disk: the caller minted them with
/// [`ensure_own`] while the entry was still there to keep serving, as going back always has. For an entry signed
/// under the leader's own authority there is nothing to rotate; [`uninstall`] is at once.
pub fn begin_own_rotation(dir: &Path, now_ms: i64) -> Result<Rotation, String> {
    let _changing = changing();
    rotation_may_begin(dir, Installs::Own).map_err(|e| e.message().to_string())?;

    let materials = current_held(dir)?;
    let served = materials.served;
    let outgoing = signing_root(&materials.ca_pem, &served)?;
    let own_cert_path = dir.join(SERVER_CERT);
    let own_served = std::fs::read(&own_cert_path).map_err(|e| format!("{}: {e}", own_cert_path.display()))?;
    let own_ca_path = dir.join(CA_CERT);
    let own_ca = std::fs::read(&own_ca_path).map_err(|e| format!("{}: {e}", own_ca_path.display()))?;
    let (incoming, incoming_pem) = signing_root_pem(&own_ca, &own_served)?;
    if incoming.fingerprint == outgoing.fingerprint {
        return Err("the supplied certificate is signed under this leader's own authority, so it can be withdrawn at once rather than rotated from.".to_string());
    }

    let state = Rotation {
        deadline_millis: switch_deadline(&served, &own_served, &incoming, now_ms)?,
        incoming_pem,
        incoming: incoming.fingerprint,
        outgoing: outgoing.fingerprint,
        started_millis: now_ms,
        installs: Installs::Own,
        entry: None,
    };
    open(dir, &state, now_ms)?;
    Ok(state)
}

/// Signs under the incoming root, having waited as long as it was going to.
///
/// `holders` are the roots each worker the leader can still expect to hear from last reported holding. Cutting
/// over is due once every one of them carries the incoming root, or once the deadline has passed -- an operator
/// who set it has said how long a worker that has not come round may hold the rest of the fleet.
///
/// `None` is a caller that could not find out what the fleet holds. It is not an empty fleet: every worker
/// reads as ready over nothing at all, which is the one answer this must never give by accident. The deadline
/// still governs, because it is what an operator set for exactly the case where waiting is not working.
///
/// Answers whether it cut over, so a caller with a socket to reload knows whether the certificate moved.
pub fn cut_over_if_due(
    dir: &Path,
    sans: Vec<String>,
    validity: Duration,
    holders: Option<&[Vec<String>]>,
    now_ms: i64,
) -> Result<bool, String> {
    let Some(state) = rotation(dir)? else {
        return Ok(false);
    };
    // An operator's certificate arrived over a rotation of this leader's own authority. What it serves is signed
    // by an authority this leader does not hold, so signing under the incoming root would change nothing a client
    // sees and close a rotation whose work never reached the fleet.
    if state.installs == Installs::NextKey && served(dir)?.is_some() {
        return Ok(false);
    }
    // No worker has been handed the incoming root while this holds: `leader::authority_to_hand` withholds the
    // bundle for exactly the same reason, so the deadline would otherwise sign under a root nobody in the fleet
    // could have. Held open rather than cut over, and the aging warning comes back to say so.
    //
    // Scoped: [`cut_over`] takes the same lock, and asks again there where the answer decides something.
    if !{
        let _reading = changing();
        socket_has_what_is_on_disk(dir)
    } {
        return Ok(false);
    }
    let fleet_ready = holders.is_some_and(|holders| holders.iter().all(|held| held.contains(&state.incoming)));
    if !fleet_ready && now_ms < state.deadline_millis {
        return Ok(false);
    }
    cut_over(dir, sans, validity)?;
    tracing::info!(
        target: "leader.authority_cut_over",
        incoming = %state.incoming,
        outgoing = %state.outgoing,
        installs = ?state.installs,
        waited_for_fleet = fleet_ready,
        "this leader now serves a certificate signed under the authority it was rotating to"
    );
    Ok(true)
}

/// Signs under the incoming root now, whatever the fleet holds. The button an operator presses to stop waiting.
///
/// The outgoing root leaves the bundle here, which is what makes a rotation worth running: an operator rotates
/// because the root they are leaving should stop being trusted, and one kept in the bundle is one whoever holds
/// its key can still impersonate this leader with. A worker that has not taken the incoming root by now is
/// reached by nothing this leader can do, and is torn down and enrolled again.
///
/// Each kind has one step nothing comes back from, and [`promoted`] reads whether it ran. Everything after it is
/// asked again by a retry and by [`recover_rotation`], so a cutover interrupted anywhere finishes.
pub fn cut_over(dir: &Path, sans: Vec<String>, validity: Duration) -> Result<(), String> {
    let _changing = changing();
    let Some(state) = rotation(dir)? else {
        return Err("this leader is not rotating its authority, so there is nothing to cut over to.".to_string());
    };

    match state.installs {
        Installs::NextKey => {
            // The key first. It is what signs, so a start interrupted after this reissues under the incoming root
            // by itself. Skipped where it has already run: a cutover whose certificate could not be written is
            // finished from here, and asking again for a key that is now `ca-key.pem` would refuse this rotation
            // forever.
            if !promoted(dir, &state)? {
                parked_under_supplied(dir)?;
                // Asked at the moment the key is promoted, not when the rotation opened. The rename below drops
                // the outgoing key, and an incoming root that can no longer vouch for a certificate leaves this
                // leader with no key that can sign one.
                still_signs(&state, super::warnings::now_millis())?;
                // Re-asked here, under the lock, because that is the only place the answer holds still: a reissue
                // can replace and fail to reload between a caller's check and this one, and a cutover reached
                // that way promotes a root while [`leader::authority_to_hand`] is withholding it from every
                // worker.
                socket_agrees(dir)?;
                let next_key_path = dir.join(NEXT_CA_KEY);
                private_file::check(&next_key_path, "the incoming authority's private key")?;
                rename_durably(&next_key_path, &dir.join(CA_KEY))?;
            }
            // The certificate before the intent. A cutover that promoted the key and then could not write a
            // certificate under it would, with the intent already gone, be a rotation no page could show and no
            // beat could finish. Failing here leaves the outgoing certificate in force, which every worker still
            // verifies, and [`recover`] reads the promoted key as the cutover it was.
            own(dir, sans, validity)?;
            // After the reissue, so a failure to write the certificate leaves a bundle that still vouches for what
            // is being served; before the intent goes, because the intent is the only record of which of the two
            // roots this was. [`recover_rotation`] finishes it from there if nothing comes back here.
            retire(&dir.join(CA_CERT), &state.outgoing)?;
        }
        Installs::Supplied => {
            if !promoted(dir, &state)? {
                socket_agrees(dir)?;
                let entry = incoming_serves(dir, &state)?;
                entry_serves(dir, &entry, &sans, &[]).map_err(|e| {
                    format!("the certificate this leader was switching to cannot be served because {e}. Choose Abandon switch, then choose the certificate again under Source.")
                })?;
                write_served(dir, Some(&entry))?;
            }
            // The outgoing bundle was derived and went with it. What is left to retire is the copy of the incoming
            // root that the leader's own bundle carried through the overlap, where what went out was the leader's
            // own: left there, it would be handed out again the day the leader's own came back into force.
            retire(&dir.join(CA_CERT), &state.incoming)?;
        }
        Installs::Own => {
            if !promoted(dir, &state)? {
                socket_agrees(dir)?;
                // Under the names this leader answers to now, which may have moved since the rotation opened.
                own(dir, sans, validity)?;
                write_served(dir, None)?;
            }
        }
    }
    remove_rotation(dir)?;
    Ok(())
}

/// Refuses to promote the leader's own next key while a list entry is served. What is served is signed by an
/// authority this leader does not hold, so promoting the key would close a rotation whose work never reached the
/// fleet and leave both roots in the bundle with nothing to retire one. [`cut_over_if_due`] waits for the same
/// reason; the rotation resumes once the entry is no longer served.
fn parked_under_supplied(dir: &Path) -> Result<(), String> {
    if served(dir)?.is_some() {
        return Err(
            "this leader serves a certificate an operator supplied, so the rotation of its own authority waits. \
             Withdraw that certificate first."
                .to_string(),
        );
    }
    Ok(())
}

/// The list entry the open rotation serves, refused where it would not serve: gone from the list, or run out while
/// the fleet was being waited for.
fn incoming_serves(dir: &Path, state: &Rotation) -> Result<Entry, String> {
    let entry = state
        .entry
        .clone()
        .ok_or_else(|| "the rotation names no certificate to put in force".to_string())?;
    let chain = entry_chain(dir, &entry).map_err(|e| {
        format!("the certificate this leader was switching to cannot be served because {e}. Choose Abandon switch, then choose the certificate again under Source.")
    })?;
    let now = OffsetDateTime::now_utc().unix_timestamp() * 1000;
    if describe(&chain, "the incoming certificate")?.not_after_millis <= now {
        return Err(
            "the certificate this leader was switching to has expired while the fleet was being waited for. \
             Abandon the switch and choose a current one."
                .to_string(),
        );
    }
    Ok(entry)
}

/// Drops the incoming root and goes back to serving what was already being served.
///
/// A worker that took the pair holds a root for an authority that will never sign anything, which costs it
/// nothing: a trust store is a list, and one entry of it going unused is the ordinary state of every machine.
///
/// `expected_incoming` is the rotation the caller means, and it is checked here rather than trusted. A caller
/// looks, decides and writes an audit row, and a cutover due on the deadline can run in any of those gaps: this
/// is where "the rotation you asked about is not the one on disk" stops being reported as an abandonment that
/// happened.
pub fn abandon_rotation(dir: &Path, expected_incoming: &str) -> Result<(), Refused> {
    let _changing = changing();
    let Some(state) = rotation(dir).map_err(Refused::Unavailable)? else {
        return Err(Refused::Conflict(
            "this leader finished the rotation while the request to abandon it was in flight, so there is \
             nothing to abandon. What it serves is signed under the authority it was rotating to."
                .to_string(),
        ));
    };
    if state.incoming != expected_incoming {
        return Err(Refused::Conflict(
            "this leader is rotating to a different authority than the one this request names, so nothing was \
             abandoned. Look at the rotation again."
                .to_string(),
        ));
    }
    // Past the promotion there is nothing to go back to: the outgoing key was renamed over rather than kept.
    // Dropping the incoming root here would leave `ca-key.pem` signing for an authority no file holds, which is a
    // leader that cannot start.
    if promoted(dir, &state).map_err(Refused::Unavailable)? {
        return Err(Refused::Conflict(
            "this leader already signs under the authority it was rotating to, so the rotation cannot be \
                    abandoned. Cut it over to finish it."
                .to_string(),
        ));
    }
    // The bundle first: while the intent is still on disk, a start interrupted here puts the root back and the
    // rotation simply continues. Removing the intent first would leave a root in the bundle that nothing claims.
    let undo = || -> Result<(), String> {
        retire_overlap(dir, &state)?;
        remove_rotation(dir)?;
        match state.installs {
            Installs::NextKey => remove_file_durably(&dir.join(NEXT_CA_KEY)),
            // The entry stays in the list, and the leader's own authority and certificate stay: they were there
            // before, and a start reissues the certificate anyway.
            Installs::Supplied | Installs::Own => Ok(()),
        }
    };
    undo().map_err(Refused::Unavailable)
}

/// Whether [`cut_over`] would run, for a caller about to write down that it did.
///
/// It refuses on what `cut_over` refuses on and in the same words, so an operator reading the reply and the audit
/// log are told the same thing. A rotation past its promotion is finishable whatever its dates say: the outgoing
/// key is already gone, and the only way out is forward.
pub fn cut_over_is_possible(dir: &Path) -> Result<(), Refused> {
    // Under the lock a replacement takes, so the answer is one state of the disk rather than a reading of each
    // number as it happened to be. [`cut_over`] asks the same questions again where it can act on them.
    let _reading = changing();
    let Some(state) = rotation(dir).map_err(Refused::Unavailable)? else {
        return Err(Refused::Conflict(
            "this leader is not rotating its authority, so there is nothing to cut over to.".to_string(),
        ));
    };
    if promoted(dir, &state).map_err(Refused::Unavailable)? {
        return Ok(());
    }
    socket_agrees(dir).map_err(Refused::Conflict)?;
    match state.installs {
        Installs::NextKey => {
            parked_under_supplied(dir).map_err(Refused::Conflict)?;
            // A cutover fails for good on a key it cannot read, and a rotation that cannot cut over is stuck
            // rather than under way -- which is what decides whether the authority's own expiry is worth warning
            // about.
            private_file::check(&dir.join(NEXT_CA_KEY), "the incoming authority's private key")
                .map_err(Refused::Unavailable)?;
            still_signs(&state, super::warnings::now_millis()).map_err(Refused::Conflict)
        }
        Installs::Supplied => incoming_serves(dir, &state).map(|_| ()).map_err(Refused::Conflict),
        Installs::Own => {
            private_file::check(&dir.join(CA_KEY), "the certificate authority's private key")
                .map_err(Refused::Unavailable)?;
            still_signs(&state, super::warnings::now_millis()).map_err(Refused::Conflict)
        }
    }
}

/// Whether the open rotation can still reach a cutover -- there is one, and nothing standing in its way.
///
/// What tells a rotation under way from one that is stuck, for a warning that would otherwise stay silent on the
/// grounds that an answer is already coming.
pub fn rotation_can_finish(dir: &Path) -> bool {
    matches!(rotation(dir), Ok(Some(_))) && cut_over_is_possible(dir).is_ok()
}

/// Refuses while what this leader presents is not what its files describe.
///
/// A rotation is a promise carried on the heartbeat, and [`leader::authority_to_hand`] withholds the bundle for
/// exactly as long as this holds -- so no worker can have been handed the incoming root, and anything that moved
/// the rotation forward here would move it past a fleet that never saw it.
fn socket_agrees(dir: &Path) -> Result<(), String> {
    if socket_has_what_is_on_disk(dir) {
        return Ok(());
    }
    Err(
        "this leader is presenting a certificate other than the one on disk, so no worker has been handed the \
         authority it is rotating to. Restart it first."
            .to_string(),
    )
}

/// Refuses an incoming root that has run out of the room a certificate needs.
///
/// [`begin_rotation`] caps the deadline short of this same expiry, which settles the ordinary wait. It cannot
/// settle this one: nothing bounds how long a leader stays down past its deadline, and the cutover an operator
/// presses has no deadline at all. Either way the root can be expired by the time the key is promoted -- and
/// [`prepare`] caps what it signs at the issuer's own expiry, so what came out would be a certificate already
/// past its end, refused by the leader's own verify and by every client after it.
fn still_signs(state: &Rotation, now_ms: i64) -> Result<(), String> {
    let incoming = describe(state.incoming_pem.as_bytes(), "the incoming authority")?;
    if incoming.not_after_millis - CUTOVER_MARGIN.as_millis() as i64 <= now_ms {
        return Err(
            "the authority this leader is rotating to has too little life left to sign a certificate that would \
             serve, so cutting over to it would leave this leader unable to issue one at all. Abandon the \
             rotation and start another."
                .to_string(),
        );
    }
    Ok(())
}

/// `pem` without the roots that have expired, keeping `serving` whatever its dates say.
fn retire_expired(pem: &[u8], serving: &str, now_ms: i64) -> Result<Vec<u8>, String> {
    let mut kept = pem.to_vec();
    for root in describe_each(pem, CA_CERT)? {
        if root.fingerprint != serving && root.not_after_millis <= now_ms {
            tracing::info!(
                target: "leader.authority_retired",
                fingerprint = %root.fingerprint,
                "an expired certificate authority was dropped from what this leader hands its workers"
            );
            kept = without(&kept, &root.fingerprint)?;
        }
    }
    Ok(kept)
}

/// Drops `fingerprint` from the bundle at `bundle_path`, so a worker handed it stops verifying this leader by it.
///
/// A fingerprint the file does not hold writes nothing, and so does a file that is not there, which is what lets
/// a caller ask again after an interruption without knowing whether the first ask got there.
fn retire(bundle_path: &Path, fingerprint: &str) -> Result<(), String> {
    let bundle = match std::fs::read(bundle_path) {
        Ok(bundle) => bundle,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("{}: {e}", bundle_path.display())),
    };
    if !describe_each(&bundle, CA_CERT)?
        .iter()
        .any(|root| root.fingerprint == fingerprint)
    {
        return Ok(());
    }
    let kept = without(&bundle, fingerprint)?;
    write_durably(bundle_path, &kept).map_err(|e| format!("{}: {e}", bundle_path.display()))?;
    tracing::info!(
        target: "leader.authority_retired",
        fingerprint = %fingerprint,
        "a certificate authority was dropped from what this leader hands its workers"
    );
    Ok(())
}

/// Drops a rotation's incoming root from the leader's own bundle, where the overlap was published while the
/// leader's own was served. What a list entry is given is derived, and loses the root with the intent.
///
/// A rotation back to the leader's own authority is the one whose incoming root belongs in the leader's own
/// bundle: that root signs it, and was never an overlap there.
fn retire_overlap(dir: &Path, state: &Rotation) -> Result<(), String> {
    if state.installs == Installs::Own {
        return Ok(());
    }
    retire(&dir.join(CA_CERT), &state.incoming)
}

/// The bundle at `bundle_path` as `serving` plus the rotation's incoming root, written whole.
fn publish_bundle(bundle_path: &Path, serving: &[u8], state: &Rotation) -> Result<(), String> {
    let bundle = with_incoming(serving, state)?;
    write_durably(bundle_path, &bundle).map_err(|e| format!("{}: {e}", bundle_path.display()))
}

/// `serving` with the incoming root of `state` after it, unless it carries that root already.
fn with_incoming(serving: &[u8], state: &Rotation) -> Result<Vec<u8>, String> {
    let mut bundle = serving.to_vec();
    if describe_each(serving, CA_CERT)?
        .iter()
        .any(|root| root.fingerprint == state.incoming)
    {
        return Ok(bundle);
    }
    if !bundle.ends_with(b"\n") {
        bundle.push(b'\n');
    }
    bundle.extend_from_slice(state.incoming_pem.as_bytes());
    Ok(bundle)
}

/// Each certificate block of `pem`, exactly as it was written.
fn blocks(pem: &[u8]) -> Result<Vec<String>, String> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let text = std::str::from_utf8(pem).map_err(|e| format!("{CA_CERT} is not text: {e}"))?;

    let mut found = Vec::new();
    let mut rest = text;
    while let Some(begin) = rest.find(BEGIN) {
        let after = &rest[begin..];
        let end = after
            .find(END)
            .ok_or_else(|| format!("{CA_CERT} holds a certificate that never ends"))?
            + END.len();
        found.push(after[..end].to_string());
        rest = &after[end..];
    }
    Ok(found)
}

/// `pem` without the certificate whose fingerprint is `dropping`, the rest kept exactly as they were written.
fn without(pem: &[u8], dropping: &str) -> Result<Vec<u8>, String> {
    let mut kept = String::new();
    for block in blocks(pem)? {
        // Through the same reader everything else uses, so a block this drops is one a client would have read.
        let der = ders(block.as_bytes(), CA_CERT)?;
        let matches = der.first().is_some_and(|der| fingerprint(der.as_ref()) == dropping);
        if !matches {
            kept.push_str(&block);
            kept.push('\n');
        }
    }
    Ok(kept.into_bytes())
}

fn write_rotation(dir: &Path, state: &Rotation) -> Result<(), String> {
    let path = dir.join(ROTATION);
    let json = serde_json::to_vec_pretty(state).map_err(|e| format!("write {ROTATION}: {e}"))?;
    write_durably(&path, &json).map_err(|e| format!("{}: {e}", path.display()))
}

fn remove_rotation(dir: &Path) -> Result<(), String> {
    remove_file_durably(&dir.join(ROTATION))
}

/// Removes `path` and persists its absence. A file already gone is already removed.
fn remove_file_durably(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    }
    warn_unsynced(path, private_file::sync_parent(path));
    Ok(())
}

/// Whether a rotation has got as far as promoting its key, which is the step it cannot be taken back over.
///
/// For a caller deciding whether abandoning is still a thing that can happen, before it writes down that it did.
pub fn cut_over_began(dir: &Path) -> Result<bool, String> {
    match rotation(dir)? {
        Some(state) => promoted(dir, &state),
        None => Ok(false),
    }
}

/// Whether the cutover's one irreversible step has run: what is in force is signed under the incoming root.
///
/// For the leader's own authority that is `ca-key.pem` holding the incoming key -- the outgoing key is renamed
/// over, not kept. For a switch of source it is [`SERVED`] naming what the rotation installs. Either way this is
/// what says which half of a cutover has run, for a retry, an abandonment and a start alike.
fn promoted(dir: &Path, state: &Rotation) -> Result<bool, String> {
    match state.installs {
        Installs::NextKey => {
            let ca_pem =
                std::fs::read(dir.join(CA_CERT)).map_err(|e| format!("{}: {e}", dir.join(CA_CERT).display()))?;
            let key_path = dir.join(CA_KEY);
            let key_pem = std::fs::read_to_string(&key_path).map_err(|e| format!("{}: {e}", key_path.display()))?;
            Ok(root_of_key(&ca_pem, &key_pem)?.fingerprint == state.incoming)
        }
        Installs::Supplied => Ok(served(dir)? == state.entry),
        Installs::Own => Ok(served(dir)?.is_none()),
    }
}

/// The root in `ca_pem` that `key_pem` signs for, of however many the file holds.
///
/// A rotation leaves two in there under one subject, so a signature is what tells them apart and a name is not.
fn root_of_key(ca_pem: &[u8], key_pem: &str) -> Result<Described, String> {
    let key = KeyPair::from_pem(key_pem).map_err(|e| format!("read the authority's private key: {e}"))?;
    for der in ders(ca_pem, CA_CERT)? {
        let Ok((_, candidate)) = x509_parser::parse_x509_certificate(der.as_ref()) else {
            continue;
        };
        if candidate.public_key().subject_public_key.data.as_ref() == key.public_key_raw() {
            return from_der(der.as_ref(), CA_CERT);
        }
    }
    Err(format!(
        "no certificate in {CA_CERT} belongs to the key in {CA_KEY}: they are not a pair, so this leader would \
         sign with an authority it hands nobody. Restore both from one backup, or remove both and let the leader \
         write a new authority."
    ))
}

/// Whether `key` is the private key `chain`'s certificate was issued for.
///
/// The check the leader already makes about its own certificate, offered to whatever else holds an operator's
/// certificate beside a key kept somewhere else -- an outbound identity, whose certificate is committed and whose
/// key is a secret, so the two can be moved apart and nothing else would notice until a handshake failed.
pub fn is_pair(chain: &[u8], key: &[u8]) -> Result<(), String> {
    rustls_config(chain, key).map(|_| ())
}

/// A server configuration over `chain` and `key`, which is where a key that does not belong to its certificate is
/// found out. Built and dropped as a check, and built again by the caller that serves it.
fn rustls_config(chain: &[u8], key: &[u8]) -> Result<rustls::ServerConfig, String> {
    use rustls::pki_types::pem::PemObject;

    let certs = rustls::pki_types::CertificateDer::pem_slice_iter(chain)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("the certificate is not a PEM certificate: {e}"))?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(key)
        .map_err(|e| format!("the private key is not a PEM private key: {e}"))?;

    rustls::ServerConfig::builder_with_provider(crate::crypto::in_use())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("{e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("the private key does not belong to the certificate: {e}"))
}

/// What a certificate says about itself, for somebody deciding whether to act on it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Described {
    /// The subject's common name, or the whole distinguished name when it carries no common name.
    pub subject: String,
    /// SHA-256 over the certificate's DER, lowercase hex. What a worker is told to expect out of band, and the one
    /// short string that says two certificates are the same one.
    pub fingerprint: String,
    /// The names a client may reach this certificate by. Empty on an authority, which is reached by no name.
    pub names: Vec<String>,
    pub not_before_millis: i64,
    pub not_after_millis: i64,
}

/// Every certificate in `pem`, in the order the file holds them.
///
/// A PEM file is a list, and which entry a caller means depends on what it holds: an authority is one certificate,
/// a served chain is a leaf followed by its issuers, and a worker's trust store is as many roots as it was given.
/// Reading only the first would report an outgoing root mid-rotation as though it were the only one a worker had.
///
/// The leader writes most of what it describes and could remember these instead, but not the one that matters: a
/// certificate an operator supplied is one only the certificate itself can answer for.
pub fn describe_each(pem: &[u8], what: &str) -> Result<Vec<Described>, String> {
    ders(pem, what)?
        .iter()
        .map(|der| from_der(der.as_ref(), what))
        .collect()
}

/// The root in `ca_pem` that vouches for `chain`, of however many the file holds.
///
/// A file given to a worker is a trust store, and during a rollover it holds the outgoing root beside the incoming one.
/// Which of them signs what this leader serves is what an enrollment code should name and what bounds the expiry of the
/// certificate under it; the others are there so a worker keeps verifying across the change.
///
/// Answered from the certificates themselves rather than by building a path, so it needs no name to check against
/// and gives the same answer for a caller that only has the files. What it asks of a candidate is what a client
/// asks before it will anchor anything there: this chain's issuer, a certificate authority, and the signature.
pub fn signing_root(ca_pem: &[u8], chain: &[u8]) -> Result<Described, String> {
    // The last certificate in a chain is the one a root signed: a leaf alone, or the highest intermediate.
    let chain = ders(chain, SERVER_CERT)?;
    let topmost = chain
        .last()
        .ok_or_else(|| format!("{SERVER_CERT} holds no certificate"))?;
    let (_, signed) = x509_parser::parse_x509_certificate(topmost.as_ref())
        .map_err(|e| format!("{SERVER_CERT} is not a certificate this can read: {e}"))?;

    // A renewal that keeps its key leaves two roots whose signatures both check out, which is the ordinary way a
    // long-lived authority is extended. The one worth naming is the one a client can still anchor a chain at, so
    // a current root wins over an expired or not-yet-valid one and the file's order decides nothing.
    // Through an overlap both roots are valid and both verify, so "still current" does not narrow it to one
    // either. The one worth naming is the one that keeps a chain anchored the longest, which is also the only
    // choice that does not put a ceiling on the certificate under it sooner than it has to.
    let now = OffsetDateTime::now_utc().unix_timestamp() * 1000;
    let mut current: Option<Described> = None;
    let mut lapsed: Option<Described> = None;
    for root in ders(ca_pem, CA_CERT)? {
        let Ok((_, candidate)) = x509_parser::parse_x509_certificate(root.as_ref()) else {
            continue;
        };
        // Everything a client needs to anchor `chain` here, not the signature alone. Two certificates can share
        // a public key and differ in who they say they are -- a cross-sign, a renewal under a new name -- and
        // both check out by signature. A worker pinned to the one whose subject is not this chain's issuer, or
        // that is not a certificate authority at all, can build no path to it and refuses the leader it was sent
        // to.
        if candidate.subject() != signed.issuer() || !candidate.is_ca() {
            continue;
        }
        if signed.verify_signature(Some(candidate.public_key())).is_err() {
            continue;
        }
        let described = from_der(root.as_ref(), CA_CERT)?;
        let held = if described.not_before_millis <= now && now < described.not_after_millis {
            &mut current
        } else {
            &mut lapsed
        };
        let longer = held
            .as_ref()
            .is_none_or(|best| described.not_after_millis > best.not_after_millis);
        if longer {
            *held = Some(described);
        }
    }
    let signer = current.or(lapsed);
    // None of them is current. Answering with one anyway is what lets the caller say so: an expired authority is
    // a different thing to report from an authority that signed nothing here.
    signer.ok_or_else(|| {
        format!("no certificate in {CA_CERT} signed what this leader would serve. Supply the authority that issued it.")
    })
}

/// The first certificate in `pem`: an authority, or the leaf a chain leads with. For a caller that means one
/// certificate rather than whatever else the file carries behind it.
pub fn describe(pem: &[u8], what: &str) -> Result<Described, String> {
    let der = der(pem, what)?;
    from_der(der.as_ref(), what)
}

fn from_der(der: &[u8], what: &str) -> Result<Described, String> {
    let (_, cert) = x509_parser::parse_x509_certificate(der)
        .map_err(|e| format!("{what} is not a certificate this can read: {e}"))?;

    let subject = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(str::to_string)
        .unwrap_or_else(|| cert.subject().to_string());

    let names = match cert.subject_alternative_name() {
        Ok(Some(san)) => san
            .value
            .general_names
            .iter()
            .filter_map(|name| match name {
                x509_parser::extensions::GeneralName::DNSName(n) => Some((*n).to_string()),
                x509_parser::extensions::GeneralName::IPAddress(bytes) => match bytes.len() {
                    4 => Some(std::net::Ipv4Addr::from(<[u8; 4]>::try_from(*bytes).ok()?).to_string()),
                    16 => Some(std::net::Ipv6Addr::from(<[u8; 16]>::try_from(*bytes).ok()?).to_string()),
                    _ => None,
                },
                _ => None,
            })
            .collect(),
        // A certificate carrying no names, or names this cannot read, is described without them rather than
        // refused: the dates are the reason to read it at all.
        _ => Vec::new(),
    };

    Ok(Described {
        subject,
        fingerprint: fingerprint(der),
        names,
        not_before_millis: cert.validity().not_before.timestamp() * 1000,
        not_after_millis: cert.validity().not_after.timestamp() * 1000,
    })
}

/// SHA-256 over DER, lowercase hex. The form an operator compares by eye and a worker is given out of band.
pub fn fingerprint(der: &[u8]) -> String {
    use std::fmt::Write;

    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, der);
    digest.as_ref().iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// The first certificate in a PEM file, as DER. `what` names the file for whoever has to fix it.
fn ders(pem: &[u8], what: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    let mut out = Vec::new();
    for item in ureq::tls::parse_pem(pem) {
        let item = item.map_err(|e| format!("{what} is not a certificate: {e}"))?;
        match item {
            ureq::tls::PemItem::Certificate(cert) => {
                out.push(rustls::pki_types::CertificateDer::from(cert.der().to_vec()));
            }
            // A combined file holding a certificate and the key that signed it is what an operator reaches for by
            // that name, and this one is stored as it arrived and handed to every worker. Skipping the key would
            // distribute it; refusing says which of the two files was wanted, before anything is written.
            ureq::tls::PemItem::PrivateKey(_) => {
                return Err(format!("{what} holds a private key. Supply the certificate alone"));
            }
            _ => return Err(format!("{what} holds something that is not a certificate")),
        }
    }
    if out.is_empty() {
        return Err(format!("{what} holds no certificate"));
    }
    Ok(out)
}

fn der(pem: &[u8], what: &str) -> Result<rustls::pki_types::CertificateDer<'static>, String> {
    // Through `ders` rather than a parser of its own, so one reading of a PEM file decides what a certificate
    // field may hold. `ders` answers with at least one certificate or an error.
    Ok(ders(pem, what)?.swap_remove(0))
}

fn create_authority(cert_path: &Path, key_path: &Path, validity: Duration) -> Result<(), String> {
    let key = KeyPair::generate().map_err(|e| format!("generate the authority's key: {e}"))?;
    let cert = authority_params(validity)?
        .self_signed(&key)
        .map_err(|e| format!("sign the authority's certificate: {e}"))?;

    // The key stands for the authority: removed first, and written last by a rename. It therefore reaches the
    // disk only after a whole certificate has, and an interrupted write leaves no authority for the next start to
    // find -- rather than a torn `ca.pem` that every start afterward keeps and no worker can read. Nothing
    // re-reads `ca.pem` to check, so this ordering carries it, and both writes persist their directory entry as
    // well as their contents: half a name is half an authority, and a start that finds one mints another.
    let _ = std::fs::remove_file(key_path);
    write_durably(cert_path, cert.pem().as_bytes()).map_err(|e| format!("{}: {e}", cert_path.display()))?;
    private_file::replace(key_path, key.serialize_pem().as_bytes()).map_err(|e| format!("{}: {e}", key_path.display()))
}

/// Writes `path` and waits for the disk to have it. A plain write returns once the kernel has the bytes, which is
/// too early for anything another file's presence is meant to promise.
pub(crate) fn write_durably(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    // Written beside and renamed over, like `private_file::replace` and for the same two reasons: a reader never
    // sees half a certificate, and an interrupted write leaves the previous one rather than part of this one. The
    // mode is the default, because everything this writes is public.
    let mut beside = path.as_os_str().to_owned();
    beside.push(".new");
    let beside = PathBuf::from(beside);
    let _ = std::fs::remove_file(&beside);

    let mut file = std::fs::File::create(&beside)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&beside, path)?;
    // The name as well as the contents. A file whose directory entry is not durable is one the next start may not
    // find, which is the whole of what another file's presence is meant to promise.
    private_file::sync_parent(path)
}

/// Renames `from` to `to` and persists the new name.
///
/// Without the sync a crash can leave the file under neither name: gone from the one it was read under, and not yet
/// durably under the one that would find it.
fn rename_durably(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::rename(from, to).map_err(|e| format!("{}: {e}", to.display()))?;
    // The move is the result. A sync that fails after it leaves the new name in place and only its survival of a
    // crash in doubt, and a caller told the rename failed would go back over a move that already took. What a crash
    // here costs is this change, which the next start reads off the disk as it finds it.
    warn_unsynced(to, private_file::sync_parent(to));
    Ok(())
}

fn warn_unsynced(path: &Path, synced: std::io::Result<()>) {
    if let Err(e) = synced {
        tracing::warn!(
            target: "leader.certificate_move_not_durable",
            path = %path.display(),
            error = %e,
            "a certificate file was moved or removed and that was not made durable, so a crash before the \
             filesystem writes it back undoes it"
        );
    }
}

/// A new server certificate and key under `dir`, signed by the authority whose key is `ca_key_pem`.
fn issue_server_cert(dir: &Path, server: CertificateParams, ca_key_pem: &str) -> Result<(), String> {
    // Only where this certificate is the one presented. Going back from a list entry writes this one while the
    // entry is still served, and the socket goes on presenting it: saying it was replaced would have every reader
    // refuse until a reload that changes nothing has run -- the rotation going back opens next among them, on a
    // leader whose reload waits for the handler that asked.
    if served(dir)?.is_none() {
        replaced_what_is_served(dir);
    }
    let (cert, key) = sign(server, ca_key_pem, "the server certificate")?;
    let key_path = dir.join(SERVER_KEY);
    private_file::replace(&key_path, key.as_bytes()).map_err(|e| format!("{}: {e}", key_path.display()))?;
    let cert_path = dir.join(SERVER_CERT);
    write_durably(&cert_path, cert.as_bytes()).map_err(|e| format!("{}: {e}", cert_path.display()))
}

/// `params` signed by the authority whose key is `ca_key_pem` under a new key: the certificate and that key, PEM.
fn sign(params: CertificateParams, ca_key_pem: &str, what: &str) -> Result<(String, String), String> {
    let ca_key = KeyPair::from_pem(ca_key_pem).map_err(|e| format!("read the authority's private key: {e}"))?;
    // The issuer carries the subject, key usages and key-identifier method to write into the signed certificate;
    // its own validity is never read, so any period builds the same issuer. [`AUTHORITY_SUBJECT`] is what makes
    // rebuilding it from the same constants match the authority on disk, and `verify` is what proves it did.
    let issuer = Issuer::new(authority_params(Duration::ZERO)?, ca_key);
    let key = KeyPair::generate().map_err(|e| format!("generate the key for {what}: {e}"))?;
    let cert = params
        .signed_by(&key, &issuer)
        .map_err(|e| format!("sign {what}: {e}"))?;
    Ok((cert.pem(), key.serialize_pem()))
}

/// Ends `params` no later than `authority` does. Clients disagree about a certificate that outlives its issuer --
/// rustls does not read a root's expiry at all, OpenSSL does -- so one that did would leave a worker's heartbeats
/// succeeding while its `git fetch` failed, which is a fleet that reports healthy and has quietly stopped taking
/// configuration.
fn cap_at(params: &mut CertificateParams, authority: &Described) -> Result<(), String> {
    let ceiling = OffsetDateTime::from_unix_timestamp(authority.not_after_millis / 1000)
        .map_err(|e| format!("{CA_CERT} expires at a time no calendar holds: {e}"))?;
    params.not_after = params.not_after.min(ceiling);
    Ok(())
}

fn authority_params(validity: Duration) -> Result<CertificateParams, String> {
    let mut params = CertificateParams::default();
    // Constrained to zero: this authority signs the leader's own certificate and the ones it issues for the list, and
    // nothing that could sign further.
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.distinguished_name = subject(AUTHORITY_SUBJECT);
    (params.not_before, params.not_after) = window(validity)?;
    Ok(params)
}

fn server_params(sans: Vec<String>, validity: Duration) -> Result<CertificateParams, String> {
    let mut params = leaf_params(
        sans,
        validity,
        vec![ExtendedKeyUsagePurpose::ServerAuth],
        "the leader's certificate names are not usable",
    )?;
    params.distinguished_name = subject(SERVER_SUBJECT);
    Ok(params)
}

/// A certificate for `names` that signs nothing further, valid `validity` from now. `unusable` opens the error for
/// a name a certificate cannot carry.
fn leaf_params(
    names: Vec<String>,
    validity: Duration,
    extended: Vec<ExtendedKeyUsagePurpose>,
    unusable: &str,
) -> Result<CertificateParams, String> {
    let mut params = CertificateParams::new(names).map_err(|e| format!("{unusable}: {e}"))?;
    params.is_ca = IsCa::NoCa;
    // `KeyPair::generate` gives an ECDSA key, which signs and never enciphers, so a key-encipherment bit here
    // would claim a use the key does not have.
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = extended;
    params.use_authority_key_identifier_extension = true;
    (params.not_before, params.not_after) = window(validity)?;
    Ok(params)
}

fn subject(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name
}

/// Backdated by [`BACKDATE`], and `validity` long from now.
fn window(validity: Duration) -> Result<(OffsetDateTime, OffsetDateTime), String> {
    let now = OffsetDateTime::now_utc();
    let ahead = i64::try_from(validity.as_secs())
        .map(time::Duration::seconds)
        .map_err(|_| {
            format!(
                "a validity of {} seconds is longer than any calendar",
                validity.as_secs()
            )
        })?;
    let not_after = now.checked_add(ahead).ok_or_else(|| {
        format!(
            "a validity of {} seconds expires past the end of the calendar",
            validity.as_secs()
        )
    })?;
    Ok((now - time::Duration::seconds(BACKDATE.as_secs() as i64), not_after))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn scratch(tag: &str) -> PathBuf {
        socket_follows();
        let dir = std::env::temp_dir().join(format!("smeltr-tls-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir = dir.join("tls");
        std::fs::create_dir_all(&dir).unwrap();
        listing(&dir);
        dir
    }

    /// Gives `dir` an empty certificate list of its own.
    pub(crate) fn listing(dir: &Path) {
        let key = dir.with_extension("key");
        let _ = std::fs::remove_file(&key);
        let list = crate::leader::sqlite_store::SqliteStore::open(Path::new(":memory:"), &key).expect("a list");
        read_list(dir, std::sync::Arc::new(list));
    }

    /// The certificate list [`listing`] gave `dir`.
    pub(crate) fn list_in(dir: &Path) -> std::sync::Arc<dyn Store> {
        list_of(dir).expect("a list")
    }

    /// `chain` and `key` as the newest version of the entry `supplied` in `dir`'s list, served with the root in `ca`
    /// that signs it, as the upload form chooses it.
    pub(crate) fn listed(dir: &Path, chain: &[u8], key: &[u8], ca: &[u8]) -> Entry {
        let list = list_of(dir).expect("a list");
        let (chain, key) = (std::str::from_utf8(chain).unwrap(), std::str::from_utf8(key).unwrap());
        let version = match list.add_certificate_version("supplied", chain, key) {
            Ok(version) => version,
            Err(_) => {
                list.create_certificate("supplied", "", chain, key).expect("an entry");
                1
            }
        };
        let ca = match split_root(chain.as_bytes(), ca) {
            Ok(Some((_, root))) => root,
            _ => String::from_utf8(ca.to_vec()).unwrap(),
        };
        Entry {
            name: "supplied".to_string(),
            version,
            ca,
        }
    }

    /// Lists `chain` and serves it.
    pub(crate) fn install(
        dir: &Path,
        chain: &[u8],
        key: &[u8],
        ca: &[u8],
        names: &[String],
        required: &[String],
    ) -> Result<(), String> {
        serve(dir, &listed(dir, chain, key, ca), names, required)
    }

    /// Lists `chain` and rotates to it.
    pub(crate) fn rotate_to(
        dir: &Path,
        chain: &[u8],
        key: &[u8],
        ca: &[u8],
        names: &[String],
        required: &[String],
        now_ms: i64,
    ) -> Result<Rotation, String> {
        begin_supplied_rotation(dir, &listed(dir, chain, key, ca), names, required, now_ms)
    }

    /// A stand-in socket that takes every replacement the moment it is made.
    ///
    /// These tests replace certificates constantly and share one process, so without something playing the socket
    /// every test would leave every other one reading a leader whose disk has moved past what it presents. It
    /// runs inside [`replaced_what_is_served`], under [`CHANGING`], which is why no test can catch another
    /// between the two numbers -- the same lock every reader of them takes.
    ///
    /// Registered from `scratch` because every test that writes one of these files starts there.
    fn socket_follows() {
        on_reload(|dir| socket_took(dir, disk_generation(dir)));
    }

    pub(super) fn names() -> Vec<String> {
        vec!["localhost".to_string(), "127.0.0.1".to_string()]
    }

    pub(super) const A_YEAR: Duration = Duration::from_secs(365 * 24 * 60 * 60);
    const A_DECADE: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);

    pub(super) const NOW: i64 = 1_800_000_000_000;
    pub(super) const A_WEEK_OUT: i64 = NOW + 7 * 24 * 60 * 60 * 1000;

    /// What a rotation starts from: an authority in force, signed under.
    fn rotating(tag: &str) -> PathBuf {
        let dir = scratch(tag);
        ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        dir
    }

    fn roots(dir: &Path) -> Vec<String> {
        let pem = std::fs::read(dir.join(CA_CERT)).expect("the authority");
        describe_each(&pem, CA_CERT)
            .expect("readable")
            .into_iter()
            .map(|d| d.fingerprint)
            .collect()
    }

    /// The root this leader signs the certificate it serves under.
    fn signing(dir: &Path) -> String {
        let pem = std::fs::read(dir.join(CA_CERT)).expect("the authority");
        let served = std::fs::read(dir.join(SERVER_CERT)).expect("the certificate");
        signing_root(&pem, &served).expect("a signer").fingerprint
    }

    /// The whole point of the overlap: the incoming root goes out to the fleet while what is served does not move,
    /// so a worker that has not heard yet keeps verifying.
    #[test]
    fn minting_the_incoming_authority_changes_nothing_a_client_sees() {
        let dir = rotating("mint");
        let before = signing(&dir);

        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");

        assert_eq!(state.outgoing, before);
        assert_ne!(state.incoming, before, "the incoming authority is a new one");
        assert_eq!(
            roots(&dir),
            vec![before.clone(), state.incoming.clone()],
            "workers are given both"
        );
        assert_eq!(signing(&dir), before, "what this leader serves moved");

        // And it still verifies, which is what a worker holding only the outgoing root does on its next beat.
        let pem = std::fs::read(dir.join(CA_CERT)).unwrap();
        let served = std::fs::read(dir.join(SERVER_CERT)).unwrap();
        verify(&pem, &served, "localhost").expect("the overlap serves something every worker can still verify");
    }

    /// Cutting over is the promotion of a key and the retirement of a root. A rotation is run to stop trusting
    /// the outgoing one, so it leaves the bundle here -- as an absence from the set each worker is handed whole.
    #[test]
    fn cutting_over_signs_under_the_incoming_root_and_retires_the_outgoing_one() {
        let dir = rotating("cutover");
        let state = begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect("a rotation");

        cut_over(&dir, names(), A_YEAR).expect("a cutover");

        assert_eq!(
            signing(&dir),
            state.incoming,
            "still signing under the root it was rotating from"
        );
        assert_eq!(
            roots(&dir),
            vec![state.incoming.clone()],
            "the root this rotation was run to stop trusting is still handed to every worker"
        );
        assert!(rotation(&dir).unwrap().is_none(), "the rotation is over");
        assert!(
            !dir.join(NEXT_CA_KEY).exists(),
            "the incoming key is `ca-key.pem` now, not a second file"
        );

        let pem = std::fs::read(dir.join(CA_CERT)).unwrap();
        let served = std::fs::read(dir.join(SERVER_CERT)).unwrap();
        verify(&pem, &served, "localhost").expect("what is served verifies against what workers hold");
        ensure(&dir, names(), A_YEAR, &[]).expect("a start after the retirement");
    }

    /// The intent outlives the promotion. A cutover that promoted the key and then could not write a certificate
    /// under it would, with the intent already gone, be a rotation nothing could show and nothing could finish.
    #[test]
    fn a_cutover_that_cannot_issue_a_certificate_leaves_the_rotation_settleable() {
        let dir = rotating("cutover-fails");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");

        // A name no certificate can carry, which is where `prepare` refuses before anything is written.
        let refused = cut_over(&dir, vec![String::new()], A_YEAR).expect_err("a certificate for no name");
        assert!(!refused.is_empty());

        // The promotion stands and the intent is still there, which is the pair `recover` reads as a cutover that
        // ran. A start settles it and issues the certificate that was missing.
        ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(rotation(&dir).unwrap().is_none(), "the rotation was left open");
        assert_eq!(signing(&dir), state.incoming, "the promoted key was rolled back");
    }

    /// A certificate never outlives what signs it, and during an overlap that is not the file's first root.
    #[test]
    fn the_served_certificate_is_bounded_by_the_root_that_signs_it() {
        let dir = scratch("ceiling");
        // A short-lived authority, so a decade-long certificate under the incoming root would be obvious.
        ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect("a rotation");
        cut_over(&dir, names(), A_DECADE).expect("a cutover");

        let pem = std::fs::read(dir.join(CA_CERT)).unwrap();
        let served = std::fs::read(dir.join(SERVER_CERT)).unwrap();
        let signer = signing_root(&pem, &served).expect("a signer");
        let leaf = describe(&served, SERVER_CERT).expect("the certificate");
        assert!(
            leaf.not_after_millis <= signer.not_after_millis,
            "the certificate outlives the authority that signed it"
        );
        // The outgoing root expires in a year and the incoming one in a decade, so a ceiling read off the file's
        // first entry would cap this at the year.
        assert!(
            leaf.not_after_millis > NOW + 400 * 24 * 60 * 60 * 1000,
            "capped against a root that did not sign it"
        );
    }

    /// Abandoning puts back exactly what was being served.
    #[test]
    fn abandoning_a_rotation_leaves_the_authority_in_force_alone() {
        let dir = rotating("abandon");
        let before = roots(&dir);
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");

        abandon_rotation(&dir, &state.incoming).expect("abandoned");

        assert_eq!(roots(&dir), before);
        assert!(rotation(&dir).unwrap().is_none());
        assert!(!dir.join(NEXT_CA_KEY).exists());
        ensure(&dir, names(), A_YEAR, &[]).expect("a start after abandoning");
    }

    /// An operator's certificate is issued by an authority this leader does not hold, so there is nothing here to
    /// rotate and a root minted for it would sign nothing.
    #[test]
    fn a_supplied_certificate_is_not_this_leaders_authority_to_rotate() {
        let dir = scratch("supplied-rotate");
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        let chain = materials.served.clone();
        let key = std::fs::read(dir.join(SERVER_KEY)).unwrap();
        install(&dir, &chain, &key, &materials.ca_pem, &names(), &[]).expect("a supplied set");

        let refused = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect_err("not this leader's to rotate");
        assert!(refused.contains("supplied"), "{refused}");
    }

    /// The deadline is what stops one worker that never comes back from holding the fleet.
    #[test]
    fn cutting_over_waits_for_the_fleet_and_then_stops_waiting() {
        let dir = rotating("due");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        let behind = vec![vec![state.outgoing.clone()]];

        assert!(
            !cut_over_if_due(&dir, names(), A_YEAR, Some(&behind), NOW).unwrap(),
            "cut over under a worker"
        );
        assert!(
            cut_over_if_due(&dir, names(), A_YEAR, Some(&behind), A_WEEK_OUT).unwrap(),
            "a worker that never came back held the rotation past its deadline"
        );

        let dir = rotating("ready");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        let caught_up = vec![vec![state.outgoing.clone(), state.incoming.clone()]];
        assert!(
            cut_over_if_due(&dir, names(), A_YEAR, Some(&caught_up), NOW).unwrap(),
            "the fleet had it"
        );
        assert_eq!(signing(&dir), state.incoming);
    }

    /// A fleet with nobody in it is a fleet that has everything. A leader whose workers have all faded should not
    /// wait out a deadline for them.
    #[test]
    fn a_rotation_with_no_workers_to_wait_for_cuts_over_at_once() {
        let dir = rotating("nofleet");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        assert!(cut_over_if_due(&dir, names(), A_YEAR, Some(&[]), NOW).unwrap());
        assert_eq!(signing(&dir), state.incoming);
    }

    /// Each gap in the order a rotation is written, settled by the next start. What the disk holds says which gap
    /// it was, and none of them leaves a leader that cannot serve.
    #[test]
    fn a_rotation_interrupted_at_any_step_is_settled_by_the_next_start() {
        // Between the key and the intent: an authority nothing claims and no worker was given.
        let dir = rotating("torn-key");
        let before = roots(&dir);
        std::fs::write(dir.join(NEXT_CA_KEY), "not a key, and nothing reads it").unwrap();
        ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(!dir.join(NEXT_CA_KEY).exists(), "a key no rotation claims was kept");
        assert_eq!(roots(&dir), before);

        // Between the intent and the bundle: the root was minted and never published.
        let dir = rotating("torn-bundle");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        let outgoing_only = without(&std::fs::read(dir.join(CA_CERT)).unwrap(), &state.incoming).unwrap();
        write_durably(&dir.join(CA_CERT), &outgoing_only).unwrap();
        ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(
            roots(&dir).contains(&state.incoming),
            "the rotation stalled with nothing published"
        );

        // The incoming key lost while the intent stands: nothing left to cut over with.
        let dir = rotating("lost-key");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        std::fs::remove_file(dir.join(NEXT_CA_KEY)).unwrap();
        ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(
            rotation(&dir).unwrap().is_none(),
            "a rotation that cannot be finished was kept"
        );
        assert!(
            !roots(&dir).contains(&state.incoming),
            "a root nothing can sign under was left published"
        );

        // Between promoting the key and dropping the intent: the cutover already happened.
        let dir = rotating("torn-cutover");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        rename_durably(&dir.join(NEXT_CA_KEY), &dir.join(CA_KEY)).unwrap();
        ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(rotation(&dir).unwrap().is_none(), "a finished rotation was left open");
        assert_eq!(signing(&dir), state.incoming, "the promoted key was rolled back");
        assert_eq!(
            roots(&dir),
            vec![state.incoming],
            "the intent was the last record of which root to retire, and it was dropped without retiring it"
        );
    }

    #[test]
    fn a_first_start_writes_an_authority_and_a_certificate_it_signed() {
        let dir = scratch("first");
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("an authority");

        assert!(materials.authority_created);
        for file in [CA_CERT, CA_KEY, SERVER_CERT, SERVER_KEY] {
            assert!(dir.join(file).exists(), "{file} was not written");
        }
        assert_eq!(materials.ca_pem, std::fs::read(dir.join(CA_CERT)).unwrap());
    }

    #[test]
    fn both_private_keys_belong_to_their_owner_alone() {
        let dir = scratch("modes");
        ensure(&dir, names(), A_YEAR, &[]).expect("an authority");

        for key in [CA_KEY, SERVER_KEY] {
            assert!(
                private_file::check(&dir.join(key), "a private key").is_ok(),
                "{key} is readable by more than its owner"
            );
        }
    }

    /// What makes rotation the leader's own business: the file a worker holds stays put while the certificate
    /// underneath it is replaced.
    #[test]
    fn the_authority_is_kept_across_starts_and_the_certificate_is_not() {
        let dir = scratch("kept");
        let first = ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        let first_cert = first.served.clone();
        let first_key = std::fs::read(dir.join(SERVER_KEY)).unwrap();

        let second = ensure(&dir, names(), A_YEAR, &[]).expect("a certificate, reissued");

        assert!(!second.authority_created);
        assert_eq!(second.ca_pem, first.ca_pem);
        assert_ne!(second.served.clone(), first_cert);
        assert_ne!(std::fs::read(dir.join(SERVER_KEY)).unwrap(), first_key);
    }

    /// Half an authority is none: a certificate whose key is gone can sign nothing, and a key whose certificate is
    /// gone signs for an authority no worker was ever given.
    #[test]
    fn losing_either_half_of_the_authority_writes_a_new_one() {
        for lost in [CA_CERT, CA_KEY] {
            let dir = scratch(&format!("lost-{lost}"));
            let first = ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
            let first_key = std::fs::read(dir.join(CA_KEY)).unwrap();
            std::fs::remove_file(dir.join(lost)).unwrap();

            let second = ensure(&dir, names(), A_YEAR, &[]).expect("a new authority");
            assert!(second.authority_created, "losing {lost} left the authority in place");
            assert_ne!(second.ca_pem, first.ca_pem, "losing {lost} reused the old authority");
            // Both halves or neither. A new certificate beside the old key would have the leader signing with a
            // key the authority it hands out does not belong to, and no worker could verify anything.
            assert_ne!(
                std::fs::read(dir.join(CA_KEY)).unwrap(),
                first_key,
                "losing {lost} kept the old authority's key"
            );
        }
    }

    /// Two files both being present says nothing about their being a pair, and a leader that served a certificate
    /// its authority does not vouch for would be refused by every client with nothing to say why.
    #[test]
    fn an_authority_whose_halves_are_not_a_pair_is_refused() {
        let ours = scratch("pair-ours");
        let theirs = scratch("pair-theirs");
        ensure(&ours, names(), A_YEAR, &[]).expect("one authority");
        ensure(&theirs, names(), A_YEAR, &[]).expect("another, unrelated");

        // What restoring the two files from different backups leaves behind.
        std::fs::copy(theirs.join(CA_CERT), ours.join(CA_CERT)).unwrap();

        let refused = ensure(&ours, names(), A_YEAR, &[]).expect_err("a certificate and a key that are not a pair");
        assert!(refused.contains("not a pair"), "{refused}");
    }

    /// A worker is given a name, not an address it happens to reach. A certificate that verified for any name
    /// would make the authority worth nothing.
    #[test]
    fn the_certificate_does_not_verify_for_a_name_it_does_not_carry() {
        let dir = scratch("wrong-name");
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        let served = materials.served.clone();

        assert!(verify(&materials.ca_pem, &served, "leader.elsewhere").is_err());
    }

    /// A certificate, key and authority triple, as an operator would have from their own PKI. `ensure` makes one
    /// that is genuinely somebody else's when it runs somewhere this leader does not look.
    pub(super) fn elsewhere(tag: &str, names: Vec<String>) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let dir = scratch(tag);
        let materials = ensure(&dir, names, A_YEAR, &[]).expect("an authority of their own");
        (
            materials.served.clone(),
            std::fs::read(dir.join(SERVER_KEY)).unwrap(),
            materials.ca_pem,
        )
    }

    #[test]
    fn a_supplied_certificate_is_what_the_leader_serves_and_hands_out() {
        let dir = scratch("supplied");
        let own = ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        let (chain, key, ca) = elsewhere("supplied-theirs", names());

        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a certificate for the names this leader answers to");

        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start with one supplied");
        assert_eq!(
            materials.ca_pem, ca,
            "a worker is given the authority that vouches for what is served"
        );
        assert_eq!(materials.served.clone(), chain);
        assert!(!materials.authority_created);

        uninstall(&dir).expect("give it back");
        let back = ensure(&dir, names(), A_YEAR, &[]).expect("a start with none supplied");
        assert_eq!(back.ca_pem, own.ca_pem, "the leader's own authority was never touched");
    }

    /// A list entry that can no longer be served does not stop a start. The leader serves its own and says why,
    /// and still names the entry, so workers keep its root until it is served again.
    #[test]
    fn a_start_whose_entry_cannot_be_served_serves_the_leaders_own() {
        let dir = scratch("unserved");
        let own = ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        let (chain, key, ca) = elsewhere("unserved-theirs", names());
        let entry = listed(&dir, &chain, &key, &ca);
        serve(&dir, &entry, &names(), &[]).expect("served");
        list_in(&dir).delete_secret_version(&entry.name, entry.version).unwrap();

        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start with its entry gone");
        assert!(!materials.supplied);
        assert_eq!(materials.ca_pem, own.ca_pem);
        assert!(unserved(&dir).is_some_and(|why| why.contains("supplied")));
        assert_eq!(served(&dir).unwrap(), Some(entry), "the entry was forgotten");
        assert_eq!(
            current(&dir).unwrap().ca_pem,
            own.ca_pem,
            "nothing reads what is served"
        );
        assert!(
            !socket_has_what_is_on_disk(&dir),
            "what workers are given is not what the socket presents"
        );
        let (_, presented, _) = to_serve(&dir).unwrap();
        assert_eq!(presented, std::fs::read(dir.join(SERVER_CERT)).unwrap());

        install(&dir, &chain, &key, &ca, &names(), &[]).expect("served again");
        assert!(unserved(&dir).is_none());
        assert!(current(&dir).unwrap().supplied);
    }

    /// A certificate supplied as files before the list is not served, and the start says where it goes.
    #[test]
    fn a_certificate_supplied_before_the_list_is_not_served() {
        let dir = scratch("before-the-list");
        let own = ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        std::fs::create_dir_all(dir.join(SUPPLIED_BEFORE_THE_LIST)).unwrap();

        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert_eq!(materials.ca_pem, own.ca_pem);
        assert!(unserved(&dir).is_some_and(|why| why.contains("is not in the certificate list")));
        assert!(
            !socket_has_what_is_on_disk(&dir),
            "workers were handed the leader's own root"
        );
    }

    /// The failure that matters: it verifies, and it still leaves a leader nobody can open -- over the connection
    /// the upload arrived on.
    #[test]
    fn a_supplied_certificate_for_names_this_leader_does_not_answer_to_is_refused() {
        let dir = scratch("supplied-wrong-name");
        ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        let (chain, key, ca) = elsewhere("supplied-wrong-name-theirs", vec!["elsewhere.internal".to_string()]);

        let refused = install(&dir, &chain, &key, &ca, &names(), &[]).expect_err("a certificate for another host");
        assert!(refused.contains("elsewhere.internal"), "{refused}");
        assert!(
            served(&dir).unwrap().is_none(),
            "a refused certificate was served anyway"
        );
    }

    #[test]
    fn a_supplied_key_that_belongs_to_another_certificate_is_refused() {
        let dir = scratch("supplied-wrong-key");
        ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        let (chain, _, ca) = elsewhere("supplied-wrong-key-a", names());
        let (_, other_key, _) = elsewhere("supplied-wrong-key-b", names());

        let refused = install(&dir, &chain, &other_key, &ca, &names(), &[]).expect_err("a key from somewhere else");
        assert!(refused.contains("does not belong"), "{refused}");
        assert!(served(&dir).unwrap().is_none());
    }

    #[test]
    fn a_supplied_authority_that_does_not_vouch_for_the_certificate_is_refused() {
        let dir = scratch("supplied-wrong-ca");
        ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        let (chain, key, _) = elsewhere("supplied-wrong-ca-a", names());
        let (_, _, other_ca) = elsewhere("supplied-wrong-ca-b", names());

        let refused = install(&dir, &chain, &key, &other_ca, &names(), &[]).expect_err("an unrelated authority");
        assert!(refused.contains("not signed under the root"), "{refused}");
        assert!(served(&dir).unwrap().is_none());
    }

    /// A trust store is a list, and a rotation is the moment it holds two. Reading only the first would report
    /// the outgoing root as though it were the only one, and warn about the step that resolves it.
    #[test]
    fn a_bundle_describes_every_root_it_holds() {
        let short = ensure(&scratch("bundle-outgoing"), names(), A_YEAR, &[]).expect("an authority");
        let long = ensure(&scratch("bundle-incoming"), names(), A_DECADE, &[]).expect("another authority");

        let mut bundle = short.ca_pem.clone();
        bundle.extend_from_slice(&long.ca_pem);
        let described = describe_each(&bundle, "a worker's roots").expect("both roots");

        assert_eq!(described.len(), 2);
        assert_eq!(describe(&bundle, "a worker's roots").unwrap(), described[0]);
        assert_ne!(described[0].fingerprint, described[1].fingerprint);
        assert!(
            described[1].not_after_millis > described[0].not_after_millis,
            "the incoming root outlives the outgoing one"
        );
    }

    /// A bundle holding no root that signed the certificate is the operator supplying the wrong file, which is a
    /// different thing from supplying more than one.
    #[test]
    fn an_authority_that_signed_none_of_the_chain_is_refused() {
        let dir = scratch("install-unrelated-roots");
        ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        let (chain, key, _) = elsewhere("install-unrelated-roots-served", names());
        let (_, _, first) = elsewhere("install-unrelated-roots-a", names());
        let (_, _, second) = elsewhere("install-unrelated-roots-b", names());

        let mut neither = first.clone();
        neither.extend_from_slice(&second);

        let refused = install(&dir, &chain, &key, &neither, &names(), &[]).expect_err("two unrelated roots");
        assert!(refused.contains("not signed under the root"), "{refused}");
    }

    /// A file with nothing in it is not a certificate that happens to say nothing.
    #[test]
    fn a_file_holding_no_certificate_is_refused() {
        let refused = describe_each(b"not a certificate", "a worker's roots").expect_err("nothing to read");
        assert!(refused.contains("holds no certificate"), "{refused}");
    }

    /// Reading is not writing. `ensure` issues a certificate every time it runs, so a page that showed what was
    /// being served by calling it would replace what it was showing, every time somebody looked.
    #[test]
    fn looking_at_what_is_served_does_not_replace_it() {
        let dir = scratch("current");
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        let served = materials.served.clone();

        for _ in 0..3 {
            let now = current(&dir).expect("what is on disk");
            assert_eq!(now.ca_pem, materials.ca_pem);
            assert_eq!(now.served.clone(), served);
        }
    }

    /// A certificate serving every browser and not the loopback is one this leader can serve and its own worker
    /// cannot use. Accepting it verifies perfectly and stops the fleet inside this process.
    #[test]
    fn a_certificate_the_embedded_worker_cannot_use_is_refused() {
        let dir = scratch("public-only");
        let public_only = vec!["leader.example.com".to_string()];
        let pki = scratch("public-only-pki");
        let theirs = ensure(&pki, public_only.clone(), A_YEAR, &[]).expect("their pki");
        let chain = theirs.served.clone();
        let key = std::fs::read(pki.join(SERVER_KEY)).unwrap();

        // A leader reached at that name and running no worker of its own: nothing wrong with it.
        install(&dir, &chain, &key, &theirs.ca_pem, &public_only, &[]).expect("a leader with no worker inside");

        // The same certificate on a leader hosting a worker that calls it at 127.0.0.1.
        let loopback = vec!["127.0.0.1".to_string()];
        let refused = install(&dir, &chain, &key, &theirs.ca_pem, &public_only, &loopback)
            .expect_err("a worker inside that cannot reach its own leader");
        assert!(refused.contains("cannot be told to use another name"), "{refused}");
        assert!(refused.contains("127.0.0.1"), "{refused}");
    }

    /// `localhost` is always among the names a leader answers to, so one name verifying is satisfied by a
    /// certificate that serves only that. An operator uploading it from `leader.example.com` would be cut off by
    /// the very request doing it, over the connection carrying it.
    #[test]
    fn a_certificate_dropping_the_name_the_upload_arrived_over_is_refused() {
        let dir = scratch("drops-the-uploader");
        let reached_at = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "leader.example.com".to_string(),
        ];
        // A certificate for the loopback names alone, which satisfies `names` through either of them.
        let loopback_only = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        let pki = scratch("drops-the-uploader-pki");
        let theirs = ensure(&pki, loopback_only, A_YEAR, &[]).expect("their pki");
        let chain = theirs.served.clone();
        let key = std::fs::read(pki.join(SERVER_KEY)).unwrap();

        install(&dir, &chain, &key, &theirs.ca_pem, &reached_at, &[]).expect("nothing requires the outside name");

        let uploading_from = vec!["leader.example.com".to_string()];
        let refused = install(&dir, &chain, &key, &theirs.ca_pem, &reached_at, &uploading_from)
            .expect_err("a certificate that ends the session uploading it");
        assert!(refused.contains("leader.example.com"), "{refused}");
    }

    /// A name that cannot be resolved is not the same fact as a name that is not there. Folded into the same
    /// `false` -- which is what a plain `exists()` does with any error, permission denied included -- a leader
    /// that still has a certificate would read as never having had one, for as long as the read keeps failing.
    #[cfg(unix)]
    #[test]
    fn a_material_check_that_cannot_resolve_a_name_is_an_error_not_an_absence() {
        let dir = scratch("has-material-unresolvable");
        std::fs::create_dir_all(&dir).unwrap();
        // A symlink loop: not a name that is missing, but one `try_exists` cannot answer for either way.
        std::os::unix::fs::symlink(dir.join(CA_CERT), dir.join(CA_CERT)).unwrap();

        assert!(
            has_own_material(&dir).is_err(),
            "a name that cannot be resolved read as a plain absence"
        );
    }

    /// A rollover is the moment a trust store holds two roots. The file's order is not what decides which one
    /// signed the certificate, and a worker is given the one that does.
    #[test]
    fn a_rollover_bundle_is_accepted_either_way_round() {
        for (tag, incoming_first) in [("bundle-incoming-first", true), ("bundle-outgoing-first", false)] {
            let dir = scratch(tag);
            ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
            let (chain, key, signing) = elsewhere(&format!("{tag}-signing"), names());
            let (_, _, other) = elsewhere(&format!("{tag}-other"), names());

            let mut bundle = Vec::new();
            if incoming_first {
                bundle.extend_from_slice(&signing);
                bundle.extend_from_slice(&other);
            } else {
                bundle.extend_from_slice(&other);
                bundle.extend_from_slice(&signing);
            }

            install(&dir, &chain, &key, &bundle, &names(), &[])
                .unwrap_or_else(|e| panic!("a bundle was refused with the signing root {tag}: {e}"));

            let found = signing_root(&bundle, &chain).expect("the root that signed it");
            let signed_by = describe(&signing, "the signing root").unwrap();
            assert_eq!(found.fingerprint, signed_by.fingerprint, "{tag} named the wrong root");

            let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start with the bundle supplied");
            assert_eq!(
                describe_each(&materials.ca_pem, CA_CERT).unwrap().len(),
                1,
                "a worker is given a root that signs nothing it is shown"
            );
            assert_eq!(
                describe(&materials.ca_pem, CA_CERT).unwrap().fingerprint,
                signed_by.fingerprint
            );
        }
    }

    /// The promotion is the step a cutover cannot undo, so a cutover that failed after it is finished from there
    /// rather than started again. Asking a second time for a key that is now `ca-key.pem` would refuse forever.
    #[test]
    fn a_cutover_that_failed_after_promoting_can_be_finished() {
        let dir = rotating("retry");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        cut_over(&dir, vec![String::new()], A_YEAR).expect_err("a certificate for no name");
        assert!(!dir.join(NEXT_CA_KEY).exists(), "the key was promoted");

        cut_over(&dir, names(), A_YEAR).expect("the cutover finishes from where it stopped");
        assert_eq!(signing(&dir), state.incoming);
        assert!(rotation(&dir).unwrap().is_none());
    }

    /// And it cannot be abandoned: the outgoing key was renamed over, so there is nothing to go back to, and
    /// dropping the incoming root would leave `ca-key.pem` signing for an authority no file holds.
    #[test]
    fn a_rotation_past_its_promotion_cannot_be_abandoned() {
        let dir = rotating("no-abandon");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        cut_over(&dir, vec![String::new()], A_YEAR).expect_err("a certificate for no name");

        let refused = abandon_rotation(&dir, &state.incoming).expect_err("nothing to go back to");
        assert!(
            refused.message().contains("cut it over") || refused.message().contains("Cut it over"),
            "{refused}"
        );
        // And the leader still starts, which is what abandoning would have cost.
        ensure(&dir, names(), A_YEAR, &[]).expect("a start");
    }

    /// An operator's certificate arriving mid-rotation serves an authority this leader does not hold. Signing
    /// under the incoming root would change nothing a client sees and close a rotation that never reached anyone.
    #[test]
    fn a_rotation_does_not_cut_over_under_a_supplied_certificate() {
        let dir = scratch("supplied-midway");
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        let chain = materials.served.clone();
        let key = std::fs::read(dir.join(SERVER_KEY)).unwrap();
        install(&dir, &chain, &key, &materials.ca_pem, &names(), &[]).expect("a supplied set");

        let ready = vec![vec![state.outgoing.clone(), state.incoming.clone()]];
        assert!(!cut_over_if_due(&dir, names(), A_YEAR, Some(&ready), A_WEEK_OUT).unwrap());
        assert!(
            rotation(&dir).unwrap().is_some(),
            "the rotation was closed against a certificate it never moved"
        );
    }

    /// However long an operator says they will wait. Past the outgoing root the fleet is served by an authority
    /// that has run out; past the incoming one there is nothing left to cut over to.
    #[test]
    fn a_wait_never_outlives_either_authority() {
        let dir = scratch("capped-wait");
        // An authority with a day in it, against a fortnight's wait. Real time, because the dates being compared
        // are the ones `ensure` wrote from the clock.
        ensure(&dir, names(), Duration::from_secs(24 * 60 * 60), &[]).expect("an authority");
        let now_ms = crate::leader::warnings::now_millis();
        let a_fortnight = now_ms + 14 * 24 * 60 * 60 * 1000;
        let state = begin_rotation(&dir, Duration::from_secs(24 * 60 * 60), a_fortnight, now_ms).expect("a rotation");

        let outgoing = describe_each(&std::fs::read(dir.join(CA_CERT)).unwrap(), CA_CERT)
            .unwrap()
            .into_iter()
            .find(|d| d.fingerprint == state.outgoing)
            .expect("the outgoing root");
        assert!(
            state.deadline_millis <= outgoing.not_after_millis,
            "the fleet would be waited for past the authority serving it"
        );
        assert!(state.deadline_millis < a_fortnight, "the wait was taken at face value");
    }

    /// A deadline landing on the incoming root's last moment caps the certificate signed under it at that same
    /// instant, so the leader promotes a key and then cannot issue anything that verifies.
    #[test]
    fn a_deadline_leaves_time_to_issue_a_certificate_under_the_incoming_root() {
        let dir = scratch("margin");
        let a_day = Duration::from_secs(24 * 60 * 60);
        ensure(&dir, names(), a_day, &[]).expect("an authority");
        let now_ms = crate::leader::warnings::now_millis();
        let state = begin_rotation(&dir, a_day, now_ms + 14 * 24 * 60 * 60 * 1000, now_ms).expect("a rotation");

        let incoming = describe(state.incoming_pem.as_bytes(), "the incoming authority").unwrap();
        assert!(
            state.deadline_millis < incoming.not_after_millis,
            "the deadline is the last moment the incoming authority exists"
        );

        // And a cutover at that deadline issues something that verifies, which is the whole of the margin.
        cut_over(&dir, names(), a_day).expect("a cutover");
        let pem = std::fs::read(dir.join(CA_CERT)).unwrap();
        let served = std::fs::read(dir.join(SERVER_CERT)).unwrap();
        verify(&pem, &served, "localhost").expect("a certificate issued under the incoming root verifies");
    }

    /// rustls does not read a trust anchor's expiry, so a root left in every worker's store goes on vouching for
    /// whoever holds its key long after the date says it stopped.
    #[test]
    fn a_rotation_drops_the_roots_that_have_expired() {
        let dir = rotating("retire");
        // A root from some rotation before this leader retired them at cutover, sitting in the bundle with
        // nothing left to claim it. Appended by hand because that is the only way one gets there now.
        let elsewhere = scratch("retire-stale");
        ensure(&elsewhere, names(), A_YEAR, &[]).expect("another authority");
        let stale_pem = std::fs::read(elsewhere.join(CA_CERT)).unwrap();
        let stale = describe(&stale_pem, CA_CERT).unwrap();
        let mut bundle = std::fs::read(dir.join(CA_CERT)).unwrap();
        bundle.extend_from_slice(&stale_pem);
        write_durably(&dir.join(CA_CERT), &bundle).unwrap();

        // Long enough after that it has run out.
        let later = NOW + 400 * 24 * 60 * 60 * 1000;
        let state = begin_rotation(&dir, A_YEAR, later + 1000, later).expect("a rotation");
        let held = roots(&dir);
        assert!(
            !held.contains(&stale.fingerprint),
            "an expired root is still handed to every worker"
        );
        assert!(held.contains(&state.outgoing), "the root being served was dropped");
        assert!(held.contains(&state.incoming));
    }

    /// A rotation cannot be abandoned once its key is promoted, and a caller about to write that down needs to
    /// know before it does.
    #[test]
    fn a_cutover_that_has_begun_says_so_before_anybody_tries_to_abandon_it() {
        let dir = rotating("began");
        assert!(!cut_over_began(&dir).unwrap(), "nothing is rotating");

        let untouched = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        assert!(!cut_over_began(&dir).unwrap(), "the key has not moved yet");
        assert!(
            abandon_rotation(&dir, &untouched.incoming).is_ok(),
            "an untouched rotation is abandonable"
        );

        let promoted = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("another rotation");
        cut_over(&dir, vec![String::new()], A_YEAR).expect_err("a certificate for no name");
        assert!(
            cut_over_began(&dir).unwrap(),
            "the key was promoted and this does not say so"
        );
        assert!(
            abandon_rotation(&dir, &promoted.incoming).is_err(),
            "the two answers disagree"
        );
    }

    /// A rotation whose root reached no file is one nothing can finish: promoting its key would leave
    /// `ca-key.pem` signing for an authority `ca.pem` does not hold. Nothing was promised to anybody, so it goes.
    #[test]
    fn a_mint_that_cannot_publish_its_root_leaves_nothing_behind() {
        let dir = rotating("mint-fails");
        // The name `write_durably` writes beside, taken by a directory: reading `ca.pem` still works, so the key
        // and the intent land, and only the publish cannot.
        let beside = dir.join(format!("{CA_CERT}.new"));
        std::fs::create_dir(&beside).unwrap();
        let refused = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW);
        std::fs::remove_dir(&beside).unwrap();

        assert!(refused.is_err(), "the publish cannot have succeeded onto a directory");
        assert!(
            rotation(&dir).unwrap().is_none(),
            "an intent nothing can finish was kept"
        );
        assert!(!dir.join(NEXT_CA_KEY).exists(), "a key nothing claims was kept");
    }

    /// A cross-sign, or a renewal under a new name: two certificates sharing a public key and differing in who
    /// they say they are. Both check out by signature, and only one is this chain's issuer.
    #[test]
    fn a_root_no_client_could_anchor_the_chain_at_is_not_the_one_named() {
        let key = rcgen::KeyPair::generate().unwrap();

        let mut issuing = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        issuing.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        issuing.distinguished_name = subject("the authority that signed it");
        issuing.not_before = OffsetDateTime::now_utc() - Duration::from_secs(60);
        issuing.not_after = OffsetDateTime::now_utc() + Duration::from_secs(30 * 24 * 60 * 60);
        let signer = issuing.clone().self_signed(&key).unwrap();

        // The same key under another name, outliving it: by signature alone it is the one that would be named.
        let mut elsewhere = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        elsewhere.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        elsewhere.distinguished_name = subject("an authority of another name");
        elsewhere.not_before = OffsetDateTime::now_utc() - Duration::from_secs(60);
        elsewhere.not_after = OffsetDateTime::now_utc() + Duration::from_secs(365 * 24 * 60 * 60);
        let stranger = elsewhere.self_signed(&key).unwrap();

        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut leaf = rcgen::CertificateParams::new(names()).unwrap();
        leaf.not_after = OffsetDateTime::now_utc() + Duration::from_secs(7 * 24 * 60 * 60);
        let issuer = rcgen::Issuer::new(issuing, key);
        let served = leaf.signed_by(&leaf_key, &issuer).unwrap();

        let bundle = format!("{}{}", stranger.pem(), signer.pem());
        let named = signing_root(bundle.as_bytes(), served.pem().as_bytes()).expect("a root");
        let expected = describe(signer.pem().as_bytes(), "the issuing root").unwrap();
        assert_eq!(
            named.fingerprint, expected.fingerprint,
            "named a root a worker pinned to it could build no path to"
        );
    }

    /// The overlap itself: both roots valid, both verifying, and the outgoing one listed first. Naming it would
    /// put a ceiling on the certificate under it sooner than the renewal already lifted.
    #[test]
    fn an_overlap_names_the_root_that_lasts_longest() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut soon = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        soon.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        soon.not_before = OffsetDateTime::now_utc() - Duration::from_secs(60);
        soon.not_after = OffsetDateTime::now_utc() + Duration::from_secs(7 * 24 * 60 * 60);
        let outgoing = soon.self_signed(&key).unwrap();

        let mut later = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        later.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        later.not_before = OffsetDateTime::now_utc() - Duration::from_secs(60);
        later.not_after = OffsetDateTime::now_utc() + Duration::from_secs(365 * 24 * 60 * 60);
        let incoming = later.self_signed(&key).unwrap();

        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut leaf = rcgen::CertificateParams::new(names()).unwrap();
        leaf.not_after = OffsetDateTime::now_utc() + Duration::from_secs(30 * 24 * 60 * 60);
        let mut issuing = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        issuing.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let issuer = rcgen::Issuer::new(issuing, key);
        let served = leaf.signed_by(&leaf_key, &issuer).unwrap();

        // Outgoing first, which is the order that used to decide it.
        let bundle = format!("{}{}", outgoing.pem(), incoming.pem());
        let named = signing_root(bundle.as_bytes(), served.pem().as_bytes()).expect("a root");
        let expected = describe(incoming.pem().as_bytes(), "the renewed root").unwrap();
        assert_eq!(
            named.fingerprint, expected.fingerprint,
            "it named the root expiring first"
        );

        // And the leaf outliving the outgoing root is not what refuses the upload.
        let dir = scratch("overlap-ceiling");
        ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        install(
            &dir,
            served.pem().as_bytes(),
            leaf_key.serialize_pem().as_bytes(),
            bundle.as_bytes(),
            &names(),
            &[],
        )
        .expect("an overlap bundle");
    }

    /// The ordinary way a long-lived authority is extended: a renewal that keeps its key. Both roots verify the
    /// same signature, so the signature alone does not say which one a client can still anchor a chain at.
    #[test]
    fn a_renewal_that_kept_its_key_names_the_root_that_is_still_current() {
        let dir = scratch("renewed-authority");
        let key = rcgen::KeyPair::generate().unwrap();
        let mut expired = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        expired.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        expired.not_before = OffsetDateTime::now_utc() - Duration::from_secs(3 * 365 * 24 * 60 * 60);
        expired.not_after = OffsetDateTime::now_utc() - Duration::from_secs(24 * 60 * 60);
        let outgoing = expired.self_signed(&key).unwrap();

        let mut renewed = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        renewed.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        renewed.not_before = OffsetDateTime::now_utc() - Duration::from_secs(60);
        renewed.not_after = OffsetDateTime::now_utc() + Duration::from_secs(365 * 24 * 60 * 60);
        let incoming = renewed.self_signed(&key).unwrap();

        // Signed by the key both roots carry, so both of them check out against it.
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut leaf = rcgen::CertificateParams::new(names()).unwrap();
        leaf.not_after = OffsetDateTime::now_utc() + Duration::from_secs(30 * 24 * 60 * 60);
        let mut renewed_again = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        renewed_again.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let issuer = rcgen::Issuer::new(renewed_again, key);
        let served = leaf.signed_by(&leaf_key, &issuer).unwrap();

        let bundle = format!("{}{}", outgoing.pem(), incoming.pem());
        let chain = served.pem();

        let named = signing_root(bundle.as_bytes(), chain.as_bytes()).expect("the root still current");
        let expected = describe(incoming.pem().as_bytes(), "the renewed root").unwrap();
        assert_eq!(named.fingerprint, expected.fingerprint, "it named the expired root");

        // And the upload is not refused for the expired one it happened to be listed beside.
        install(
            &dir,
            chain.as_bytes(),
            leaf_key.serialize_pem().as_bytes(),
            bundle.as_bytes(),
            &names(),
            &[],
        )
        .expect("a renewal bundle");
    }

    /// `ca.pem` is stored as it arrived and handed to every worker, so a key that reaches it reaches all of them.
    #[test]
    fn an_authority_carrying_its_own_private_key_is_refused() {
        let dir = scratch("authority-with-key");
        ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        let (chain, key, ca) = elsewhere("authority-with-key-theirs", names());

        // What an operator pastes having reached for the file their tooling calls the CA.
        let ca_and_its_key = [ca.as_slice(), key.as_slice()].concat();

        let refused = check_supplied(&chain, &key, &ca_and_its_key, &names(), &[]).expect_err("a key was accepted");
        assert!(refused.contains("private key"), "{refused}");
    }

    /// The same mistake in the other field: a chain is served to every client that opens a connection.
    #[test]
    fn a_certificate_carrying_its_own_private_key_is_refused() {
        let dir = scratch("chain-with-key");
        ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own");
        let (chain, key, ca) = elsewhere("chain-with-key-theirs", names());

        let chain_and_its_key = [chain.as_slice(), key.as_slice()].concat();

        let refused = check_supplied(&chain_and_its_key, &key, &ca, &names(), &[]).expect_err("a key was accepted");
        assert!(refused.contains("private key"), "{refused}");
    }

    #[test]
    fn a_certificate_describes_itself() {
        let dir = scratch("describe");
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("an authority");

        let authority = describe(&materials.ca_pem, CA_CERT).expect("the authority");
        assert_eq!(authority.subject, AUTHORITY_SUBJECT);
        assert!(authority.names.is_empty(), "an authority is reached by no name");
        assert_eq!(authority.fingerprint.len(), 64, "sha-256 in hex");

        let served = describe(&materials.served.clone(), SERVER_CERT).expect("served");
        assert_eq!(served.subject, SERVER_SUBJECT);
        assert_eq!(served.names, names(), "the names it was issued for, in order");
        assert_ne!(served.fingerprint, authority.fingerprint);
    }

    /// A certificate outliving the authority that signed it is accepted by rustls and refused by OpenSSL, so a
    /// worker would keep heartbeating while its `git fetch` failed. The authority's expiry is the ceiling.
    #[test]
    fn the_certificate_never_outlives_the_authority() {
        let dir = scratch("clamp");
        // An authority with a day to live, then a certificate asked for a year: the ask is what gets cut.
        let day = Duration::from_secs(24 * 60 * 60);
        let first = ensure(&dir, names(), day, &[]).expect("a short-lived authority");
        let authority = describe(&first.ca_pem, CA_CERT).expect("the authority");

        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a certificate asked for a year");
        let served = describe(&materials.served.clone(), SERVER_CERT).expect("served");

        assert_eq!(
            served.not_after_millis, authority.not_after_millis,
            "the certificate was issued past the authority's own expiry"
        );
    }

    #[test]
    fn something_that_is_not_a_certificate_is_refused_by_name() {
        let refused = describe_each(
            b"-----BEGIN CERTIFICATE-----\nnot der\n-----END CERTIFICATE-----\n",
            CA_CERT,
        )
        .expect_err("not a certificate");
        assert!(refused.contains(CA_CERT), "{refused}");
    }

    #[test]
    fn a_certificate_is_valid_for_the_configured_period_and_backdated_before_it() {
        let validity = Duration::from_secs(600);
        let before = OffsetDateTime::now_utc();
        let (not_before, not_after) = window(validity).expect("a window");
        let after = OffsetDateTime::now_utc();

        assert!(
            not_after >= before + validity,
            "the certificate expires before the period is out"
        );
        assert!(
            not_after <= after + validity,
            "the certificate outlasts the period asked for"
        );
        assert!(
            not_before <= before - Duration::from_secs(60),
            "a certificate valid only from now rejects a client running behind"
        );
    }

    #[test]
    fn a_validity_past_the_end_of_the_calendar_is_refused_rather_than_wrapped() {
        assert!(window(Duration::from_secs(u64::MAX)).is_err());
    }

    /// A certificate carries names in ASCII. An operator who configures an internationalized one is told so, here,
    /// rather than by a handshake that fails later against a name they did type.
    ///
    /// And is told once more after fixing it: a refused start that had left an authority behind would be one the
    /// next start finds in place, and so never announces.
    #[test]
    fn a_name_a_certificate_cannot_carry_is_refused_and_leaves_no_authority() {
        let dir = scratch("bad-name");

        let refused = ensure(&dir, vec!["läder.internal".to_string()], A_YEAR, &[]).expect_err("a name outside ASCII");
        assert!(refused.contains("names are not usable"), "{refused}");
        for file in [CA_CERT, CA_KEY] {
            assert!(!dir.join(file).exists(), "the refused start left {file} behind");
        }

        assert!(
            ensure(&dir, names(), A_YEAR, &[])
                .expect("the start after the fix")
                .authority_created,
            "the operator was never told to hand out an authority"
        );
    }

    #[test]
    fn a_certificate_carrying_no_names_at_all_is_refused() {
        let dir = scratch("no-names");
        let refused = ensure(&dir, Vec::new(), A_YEAR, &[]).expect_err("no names");
        assert!(refused.contains("no names"), "{refused}");
    }

    /// An incoming root can run out between the rotation opening and the cutover running: a leader down past its
    /// deadline, an operator pressing the button late. Promotion is the step nothing comes back from, so the
    /// question is asked before it rather than discovered by the certificate that will not verify.
    #[test]
    fn a_cutover_to_an_authority_that_has_run_out_is_refused() {
        let dir = rotating("expired-incoming");
        let outgoing_key = std::fs::read(dir.join(CA_KEY)).expect("the outgoing key");
        // Less life than a certificate signed under it would need, so it is already past signing one that serves.
        let state = begin_rotation(&dir, Duration::from_secs(1), A_WEEK_OUT, NOW).expect("a rotation");

        let refused = cut_over(&dir, names(), A_YEAR).expect_err("an authority that cannot sign");
        assert!(refused.contains("too little life left"), "{refused}");
        assert_eq!(
            std::fs::read(dir.join(CA_KEY)).expect("the outgoing key"),
            outgoing_key,
            "the one key that could still sign was renamed over"
        );
        assert!(dir.join(NEXT_CA_KEY).exists(), "the incoming key was promoted anyway");
        abandon_rotation(&dir, &state.incoming).expect("a refused cutover leaves a rotation that can be abandoned");
    }

    /// Readiness nobody could answer is not readiness. The deadline still decides, because it is what an operator
    /// set for the case where waiting is not working -- and a store that stays unreadable is that case.
    #[test]
    fn a_fleet_that_could_not_be_asked_is_not_a_fleet_that_is_ready() {
        let dir = rotating("unasked");
        let state = begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect("a rotation");

        assert!(
            !cut_over_if_due(&dir, names(), A_YEAR, None, NOW).unwrap(),
            "a fleet nobody could ask about was taken for one that is ready"
        );
        assert!(
            cut_over_if_due(&dir, names(), A_YEAR, None, A_WEEK_OUT + 1).unwrap(),
            "the deadline stopped deciding because readiness could not be read"
        );
        assert_eq!(signing(&dir), state.incoming);
    }

    /// An abandonment answers for the rotation it names. A deadline's cutover can finish that rotation while the
    /// request is in flight, and "abandoned" recorded for it would be the log describing the opposite.
    #[test]
    fn abandoning_a_rotation_that_finished_underneath_is_refused() {
        let dir = rotating("finished-underneath");
        let state = begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect("a rotation");
        cut_over(&dir, names(), A_YEAR).expect("the cutover finishes");

        let refused = abandon_rotation(&dir, &state.incoming).expect_err("nothing was abandoned");
        assert!(refused.message().contains("finished the rotation"), "{refused}");
    }

    /// The preflight a caller uses before writing down that a cutover happened has to refuse on exactly what the
    /// cutover refuses on, or the audit log and the reply describe different events.
    #[test]
    fn what_may_be_cut_over_and_what_cuts_over_agree() {
        let dir = rotating("possible");
        assert!(
            cut_over_is_possible(&dir).is_err(),
            "nothing is rotating and this says it could be cut over"
        );

        let state = begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect("a rotation");
        assert!(cut_over_is_possible(&dir).is_ok(), "an open rotation is cuttable");
        assert!(rotation_can_finish(&dir), "an open rotation reads as stuck");
        cut_over(&dir, names(), A_YEAR).expect("and so it cuts over");
        assert_eq!(signing(&dir), state.incoming);

        // An incoming root with no room left: refused, in the same words, and no longer counted as under way.
        let dir = rotating("possible-expired");
        begin_rotation(&dir, Duration::from_secs(1), A_WEEK_OUT, NOW).expect("a rotation");
        let preflight = cut_over_is_possible(&dir).expect_err("no room to sign");
        let attempted = cut_over(&dir, names(), A_YEAR).expect_err("no room to sign");
        assert_eq!(
            preflight.message(),
            attempted,
            "the preflight and the act give different reasons"
        );
        assert!(
            !rotation_can_finish(&dir),
            "a rotation that cannot cut over reads as under way"
        );
    }

    /// Past the promotion the outgoing key is gone, so finishing is the only way out whatever the dates say.
    #[test]
    fn a_rotation_past_its_promotion_may_always_be_cut_over() {
        let dir = rotating("promoted-possible");
        begin_rotation(&dir, Duration::from_secs(1), A_WEEK_OUT, NOW).expect("a rotation");
        assert!(cut_over_is_possible(&dir).is_err(), "not promoted yet, and out of room");

        // Promote by hand: what an interrupted cutover leaves behind.
        rename_durably(&dir.join(NEXT_CA_KEY), &dir.join(CA_KEY)).expect("promoted");
        assert!(
            cut_over_is_possible(&dir).is_ok(),
            "a rotation nothing can go back from was refused its only way forward"
        );
    }

    /// A cutover fails for good on an incoming key it cannot read, so a rotation missing one is stuck rather than
    /// under way -- and "under way" is what silences the warning that the authority being served is running out.
    #[test]
    fn a_rotation_whose_incoming_key_is_gone_is_stuck_rather_than_under_way() {
        let dir = rotating("key-gone");
        begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect("a rotation");
        assert!(rotation_can_finish(&dir), "an untouched rotation reads as stuck");

        std::fs::remove_file(dir.join(NEXT_CA_KEY)).expect("the incoming key");
        assert!(
            cut_over(&dir, names(), A_YEAR).is_err(),
            "a cutover ran without the key that signs"
        );
        assert!(
            !rotation_can_finish(&dir),
            "a rotation that can never cut over still silences the aging warning"
        );
    }

    /// The list a caller checks before writing down that a rotation began is the list the act refuses on. Kept as
    /// one function because two copies of it drifted.
    #[test]
    fn what_may_begin_a_rotation_and_what_begins_one_agree() {
        let dir = rotating("may-begin");
        rotation_may_begin(&dir, Installs::NextKey).expect("nothing in the way");
        begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect("a rotation");

        let refused = rotation_may_begin(&dir, Installs::NextKey).expect_err("one is already open");
        let attempted = begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect_err("one is already open");
        assert_eq!(
            refused.message(),
            attempted,
            "the preflight and the act give different reasons"
        );
        assert!(matches!(refused, Refused::Conflict(_)), "a conflict read as a failure");
    }

    /// A file that will not read is not a state an operator can resolve, and a client watching this leader has to
    /// be able to tell the two apart. They travel as different answers rather than one string.
    #[test]
    fn a_preflight_tells_an_unreadable_file_from_a_state_that_conflicts() {
        let dir = rotating("refusal-kinds");
        begin_rotation(&dir, A_DECADE, A_WEEK_OUT, NOW).expect("a rotation");
        assert!(
            matches!(rotation_may_begin(&dir, Installs::NextKey), Err(Refused::Conflict(_))),
            "an open rotation is a conflict"
        );

        // A rotation nothing can parse: the request is fine and the leader is not.
        write_durably(&dir.join(ROTATION), b"not a rotation").expect("written");
        assert!(
            matches!(
                rotation_may_begin(&dir, Installs::NextKey),
                Err(Refused::Unavailable(_))
            ),
            "an unreadable rotation read as an operator's problem"
        );
        assert!(
            matches!(cut_over_is_possible(&dir), Err(Refused::Unavailable(_))),
            "an unreadable rotation read as an operator's problem"
        );
    }

    /// Every replacement asks for the reload that brings the two back together. Without it, a write leaves every
    /// answer about this leader's authority refusing, with nothing scheduled to put it right.
    #[test]
    fn a_replacement_asks_for_the_reload_that_settles_it() {
        let dir = scratch("settles");
        ensure(&dir, names(), A_YEAR, &[]).expect("an authority");
        assert!(socket_has_what_is_on_disk(&dir), "a start left the two parted");

        ensure_own(&dir, names(), A_YEAR).expect("a reissue");
        assert!(socket_has_what_is_on_disk(&dir), "a reissue left the two parted");

        // The case a rule once tried to single out: minting the leader's own certificate while a supplied set is
        // in force changes nothing a client sees. Asking for a reload of whatever is in force settles it either
        // way, which is why no rule is needed -- and a withdrawal that fails after this leaves nothing stuck.
        let own = std::fs::read(dir.join(CA_CERT)).expect("the authority");
        let chain = std::fs::read(dir.join(SERVER_CERT)).expect("the certificate");
        let key = std::fs::read(dir.join(SERVER_KEY)).expect("the key");
        install(&dir, &chain, &key, &own, &names(), &[]).expect("supplied in force");
        assert!(socket_has_what_is_on_disk(&dir), "an upload left the two parted");

        ensure_own(&dir, names(), A_YEAR).expect("what a withdrawal mints first");
        assert!(
            socket_has_what_is_on_disk(&dir),
            "a certificate hidden behind a supplied set left every authority read refusing"
        );

        uninstall(&dir).expect("withdrawn");
        assert!(socket_has_what_is_on_disk(&dir), "a withdrawal left the two parted");
    }

    /// What the socket loaded is the state of the disk it read, never the state the disk reached while it was
    /// loading. A replacement landing during a reload is the two parting, and recording the latest instead would
    /// report the socket holding material it never saw.
    ///
    /// Holds the lock throughout, so no replacement elsewhere moves these numbers mid-test.
    #[test]
    fn the_socket_holds_what_it_loaded_and_not_what_arrived_since() {
        let dir = scratch("loaded");
        ensure(&dir, names(), A_YEAR, &[]).expect("an authority");

        // What a reload is handed, and what lands while it is still awaited.
        let (loading, _, _) = to_serve(&dir).expect("what the socket would load");
        ensure_own(&dir, names(), A_YEAR).expect("a replacement during the reload");

        socket_took(&dir, loading);
        assert!(
            !socket_has_what_is_on_disk(&dir),
            "a state that arrived while the socket was loading was reported as the one it holds"
        );

        socket_took(&dir, disk_generation(&dir));
        assert!(socket_has_what_is_on_disk(&dir), "the socket loaded what is on disk");
    }

    // -- switching source ------------------------------------

    /// The root signing what is in force, whichever set that is.
    fn in_force_signing(dir: &Path) -> String {
        let materials = current(dir).expect("what is in force");
        signing_root(&materials.ca_pem, &materials.served)
            .expect("a signer")
            .fingerprint
    }

    /// The roots every worker is handed.
    fn published(dir: &Path) -> Vec<String> {
        describe_each(&current(dir).expect("what is in force").ca_pem, CA_CERT)
            .expect("readable")
            .into_iter()
            .map(|d| d.fingerprint)
            .collect()
    }

    fn what_is_served(dir: &Path) -> Vec<u8> {
        current(dir).expect("what is in force").served
    }

    /// What a cutover to or from a list entry leaves when it dies after serving what it installs.
    pub(super) fn torn_cutover(dir: &Path, state: &Rotation) {
        let _changing = changing();
        write_served(
            dir,
            state.entry.as_ref().filter(|_| state.installs == Installs::Supplied),
        )
        .expect("served");
    }

    /// The entry a rotation installs, gone from the list.
    pub(super) fn lose_entry(dir: &Path, mut state: Rotation) {
        let entry = state.entry.as_mut().expect("a rotation to an entry");
        entry.version += 1000;
        write_rotation(dir, &state).expect("written");
    }

    /// An upload under another root is the leader's own rotation with the uploaded set in the key's place: its
    /// root goes out first, what is served does not move, and the cutover is what installs it.
    #[test]
    fn an_upload_under_another_root_is_published_before_it_is_served() {
        let dir = rotating("switch-up");
        let own = signing(&dir);
        let before = what_is_served(&dir);
        let (chain, key, ca) = elsewhere("switch-up-theirs", names());
        assert!(supplying_changes_root(&dir, &chain, &ca).unwrap());

        let state = rotate_to(&dir, &chain, &key, &ca, &names(), &[], NOW).expect("a rotation");

        assert_eq!(state.installs, Installs::Supplied);
        assert_eq!(state.outgoing, own);
        assert_eq!(state.incoming, describe(&ca, CA_CERT).unwrap().fingerprint);
        assert_eq!(
            published(&dir),
            vec![own.clone(), state.incoming.clone()],
            "workers are given both"
        );
        assert_eq!(what_is_served(&dir), before, "what this leader serves moved");
        assert_eq!(in_force_signing(&dir), own);
        assert!(!current(&dir).unwrap().supplied, "the upload is in force already");
        ensure(&dir, names(), A_YEAR, &[]).expect("a start mid-overlap");
        assert_eq!(
            published(&dir),
            vec![own.clone(), state.incoming.clone()],
            "a start dropped the overlap"
        );

        cut_over(&dir, names(), A_YEAR).expect("a cutover");

        assert_eq!(
            what_is_served(&dir),
            chain,
            "the uploaded certificate is not what is served"
        );
        assert_eq!(in_force_signing(&dir), state.incoming);
        assert_eq!(
            published(&dir),
            vec![state.incoming.clone()],
            "the outgoing root is still handed out"
        );
        assert_eq!(roots(&dir), vec![own], "the leader's own bundle kept the uploaded root");
        assert!(rotation(&dir).unwrap().is_none());
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start after the switch");
        assert!(materials.supplied);
    }

    /// Every worker verifies by the root already, so there is nobody to wait for. A renewal is served the moment
    /// it arrives, as it always was.
    #[test]
    fn an_upload_under_the_root_in_force_is_served_at_once() {
        let dir = rotating("renewal");
        let (chain, key, ca) = elsewhere("renewal-theirs", names());
        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a supplied set");
        let (renewed, renewed_key, _) = {
            // The same authority, issuing again: `ensure` on the directory `elsewhere` left, not wiped first.
            let theirs = std::env::temp_dir()
                .join(format!("smeltr-tls-{}-renewal-theirs", std::process::id()))
                .join("tls");
            let materials = ensure(&theirs, names(), A_YEAR, &[]).expect("a renewal");
            (
                materials.served.clone(),
                std::fs::read(theirs.join(SERVER_KEY)).unwrap(),
                materials.ca_pem,
            )
        };
        assert!(!supplying_changes_root(&dir, &renewed, &ca).unwrap());
        let refused = rotate_to(&dir, &renewed, &renewed_key, &ca, &names(), &[], NOW).expect_err("nothing to rotate");
        assert!(refused.contains("at once"), "{refused}");

        install(&dir, &renewed, &renewed_key, &ca, &names(), &[]).expect("a renewal");
        assert_eq!(what_is_served(&dir), renewed);
        assert!(rotation(&dir).unwrap().is_none());
    }

    /// Going back to the leader's own is the same rotation the other way: its root goes out beside the supplied
    /// one, the supplied certificate keeps serving, and the cutover withdraws it.
    #[test]
    fn a_withdrawal_under_another_root_is_published_before_it_is_served() {
        let dir = rotating("switch-down");
        let own = signing(&dir);
        let (chain, key, ca) = elsewhere("switch-down-theirs", names());
        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a supplied set");
        let theirs = in_force_signing(&dir);

        // What a withdrawal does first: the certificate beneath, minted while the supplied one still serves.
        ensure_own(&dir, names(), A_YEAR).expect("the leader's own, reissued");
        assert!(withdrawing_changes_root(&dir).unwrap());
        let state = begin_own_rotation(&dir, NOW).expect("a rotation");

        assert_eq!(state.installs, Installs::Own);
        assert_eq!(
            (state.outgoing.as_str(), state.incoming.as_str()),
            (theirs.as_str(), own.as_str())
        );
        assert_eq!(
            published(&dir),
            vec![theirs.clone(), own.clone()],
            "workers are given both"
        );
        assert_eq!(what_is_served(&dir), chain, "what this leader serves moved");
        assert!(current(&dir).unwrap().supplied);
        ensure(&dir, names(), A_YEAR, &[]).expect("a start mid-overlap");
        assert_eq!(
            published(&dir),
            vec![theirs, own.clone()],
            "a start dropped the overlap"
        );

        cut_over(&dir, names(), A_YEAR).expect("a cutover");

        assert!(!current(&dir).unwrap().supplied, "the supplied set is still in force");
        assert_eq!(in_force_signing(&dir), own);
        assert_eq!(published(&dir), vec![own], "the outgoing root is still handed out");
        assert!(rotation(&dir).unwrap().is_none());
        assert!(served(&dir).unwrap().is_none());
        ensure(&dir, names(), A_YEAR, &[]).expect("a start after the switch");
    }

    /// The certificate beneath a supplied set is written while that set is what the socket presents, so writing
    /// it replaces nothing the socket would load. Said otherwise, the switch a withdrawal opens next would be
    /// refused for a disagreement a reload could not settle.
    #[test]
    fn minting_the_leaders_own_beneath_a_supplied_set_replaces_nothing_presented() {
        let dir = rotating("beneath");
        let (chain, key, ca) = elsewhere("beneath-theirs", names());
        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a supplied set");
        let before = disk_generation(&dir);
        ensure_own(&dir, names(), A_YEAR).expect("the leader's own");
        assert_eq!(
            disk_generation(&dir),
            before,
            "the socket was told to load what it already presents"
        );
        uninstall(&dir).expect("withdrawn");
        assert_eq!(
            disk_generation(&dir),
            before + 1,
            "the withdrawal is what replaces what is presented"
        );
    }

    /// A supplied set under this leader's own authority is one every worker verifies already.
    #[test]
    fn withdrawing_a_set_under_the_leaders_own_authority_rotates_nothing() {
        let dir = rotating("switch-down-same");
        let materials = current(&dir).unwrap();
        let chain = materials.served.clone();
        let key = std::fs::read(dir.join(SERVER_KEY)).unwrap();
        install(&dir, &chain, &key, &materials.ca_pem, &names(), &[]).expect("a supplied set");
        ensure_own(&dir, names(), A_YEAR).expect("the leader's own");
        assert!(!withdrawing_changes_root(&dir).unwrap());
        let refused = begin_own_rotation(&dir, NOW).expect_err("nothing to rotate");
        assert!(refused.contains("at once"), "{refused}");
    }

    /// From one operator's set to another: the outgoing set leaves with its bundle, and nothing of the leader's
    /// own is touched.
    #[test]
    fn an_upload_over_an_upload_rotates_between_their_roots() {
        let dir = rotating("switch-across");
        let own = roots(&dir);
        let (chain_a, key_a, ca_a) = elsewhere("switch-across-a", names());
        install(&dir, &chain_a, &key_a, &ca_a, &names(), &[]).expect("the first");
        let a = in_force_signing(&dir);
        let (chain_b, key_b, ca_b) = elsewhere("switch-across-b", names());

        let state = rotate_to(&dir, &chain_b, &key_b, &ca_b, &names(), &[], NOW).expect("a rotation");
        assert_eq!(published(&dir), vec![a.clone(), state.incoming.clone()]);
        assert_eq!(what_is_served(&dir), chain_a);

        let ready = vec![vec![a, state.incoming.clone()]];
        assert!(cut_over_if_due(&dir, names(), A_YEAR, Some(&ready), NOW).unwrap());
        assert_eq!(what_is_served(&dir), chain_b);
        assert_eq!(published(&dir), vec![state.incoming]);
        assert_eq!(roots(&dir), own, "the leader's own bundle changed");
    }

    /// An upload under the leader's own root while its key rotation is open parks the rotation: the automatic
    /// cutover waits, and the operator's button refuses, until the set is withdrawn and the rotation resumes.
    #[test]
    fn a_key_rotation_is_parked_while_an_upload_under_its_root_is_in_force() {
        let dir = rotating("parked");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        let own_set = (
            std::fs::read(dir.join(SERVER_CERT)).unwrap(),
            std::fs::read(dir.join(SERVER_KEY)).unwrap(),
            std::fs::read(dir.join(CA_CERT)).unwrap(),
        );
        assert!(!supplying_changes_root(&dir, &own_set.0, &own_set.2).unwrap());
        install(&dir, &own_set.0, &own_set.1, &own_set.2, &names(), &[]).expect("served at once");
        assert_eq!(published(&dir), vec![state.outgoing.clone(), state.incoming.clone()]);

        let ready = vec![vec![state.outgoing.clone(), state.incoming.clone()]];
        assert!(!cut_over_if_due(&dir, names(), A_YEAR, Some(&ready), state.deadline_millis).unwrap());
        let refused = cut_over(&dir, names(), A_YEAR).expect_err("promoted beneath a supplied set");
        assert!(refused.contains("Withdraw"), "{refused}");
        assert!(cut_over_is_possible(&dir).is_err());
        assert!(rotation(&dir).unwrap().is_some(), "the rotation was dropped");

        assert!(!withdrawing_changes_root(&dir).unwrap());
        uninstall(&dir).expect("withdrawn at once");
        assert!(cut_over_if_due(&dir, names(), A_YEAR, Some(&ready), NOW).unwrap());
        assert_eq!(roots(&dir), vec![state.incoming]);
    }

    /// A renewal changes no root, so it is served at once whatever rotation is open -- and the overlap that
    /// rotation published rides along into the renewed set's bundle, where the next beat reads it.
    #[test]
    fn a_renewal_during_a_switch_back_is_served_and_keeps_the_overlap_published() {
        let dir = rotating("renewal-while-open");
        let (chain, key, ca) = elsewhere("renewal-while-open-theirs", names());
        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a supplied set");
        ensure_own(&dir, names(), A_YEAR).expect("the leader's own");
        let state = begin_own_rotation(&dir, NOW).expect("a switch back");
        assert_eq!(published(&dir), vec![state.outgoing.clone(), state.incoming.clone()]);

        let (renewed, renewed_key) = {
            let theirs = std::env::temp_dir()
                .join(format!("smeltr-tls-{}-renewal-while-open-theirs", std::process::id()))
                .join("tls");
            let materials = ensure(&theirs, names(), A_YEAR, &[]).expect("a renewal");
            (
                materials.served.clone(),
                std::fs::read(theirs.join(SERVER_KEY)).unwrap(),
            )
        };
        assert!(!supplying_changes_root(&dir, &renewed, &ca).unwrap());
        install(&dir, &renewed, &renewed_key, &ca, &names(), &[]).expect("a renewal during the switch");
        assert_eq!(what_is_served(&dir), renewed);
        assert_eq!(
            rotation(&dir).unwrap().map(|s| s.incoming),
            Some(state.incoming.clone())
        );
        assert_eq!(published(&dir), vec![state.outgoing.clone(), state.incoming.clone()]);

        let ready = vec![vec![state.outgoing, state.incoming.clone()]];
        assert!(cut_over_if_due(&dir, names(), A_YEAR, Some(&ready), NOW).unwrap());
        assert!(!current(&dir).unwrap().supplied);
        assert_eq!(published(&dir), vec![state.incoming]);
    }

    /// A switch waits the way the leader's own rotation does: for every worker, or until the deadline. The deadline
    /// is the served certificate's own end, short of the margin: nothing is lost by waiting until then.
    #[test]
    fn a_switch_waits_for_the_fleet_until_the_served_certificate_runs_out() {
        let dir = rotating("switch-waits");
        let (chain, key, ca) = elsewhere("switch-waits-theirs", names());
        let state = rotate_to(&dir, &chain, &key, &ca, &names(), &[], NOW).expect("a rotation");
        let served_until = describe(&what_is_served(&dir), SERVER_CERT).unwrap().not_after_millis;
        assert_eq!(state.deadline_millis, served_until - CUTOVER_MARGIN.as_millis() as i64);

        let behind = vec![vec![state.outgoing.clone()]];
        assert!(!cut_over_if_due(&dir, names(), A_YEAR, Some(&behind), NOW).unwrap());
        assert!(
            !cut_over_if_due(&dir, names(), A_YEAR, None, NOW).unwrap(),
            "an unreadable roster read as ready"
        );
        assert!(cut_over_if_due(&dir, names(), A_YEAR, Some(&behind), state.deadline_millis).unwrap());
        assert_eq!(what_is_served(&dir), chain);
    }

    /// The deadline moves anywhere between now and the latest the switch could wait, and the cutover keeps it.
    /// A time already past is refused, not taken as "cut over now".
    #[test]
    fn a_switch_deadline_moves_within_its_bounds() {
        let dir = rotating("switch-moves");
        let (chain, key, ca) = elsewhere("switch-moves-theirs", names());
        let state = rotate_to(&dir, &chain, &key, &ca, &names(), &[], NOW).expect("a rotation");
        let latest = state.deadline_millis;

        assert!(matches!(set_deadline(&dir, NOW, NOW), Err(Refused::Conflict(_))));
        assert!(matches!(set_deadline(&dir, latest + 1, NOW), Err(Refused::Conflict(_))));
        assert_eq!(
            rotation(&dir).unwrap().unwrap().deadline_millis,
            latest,
            "a refusal moved it"
        );

        let sooner = NOW + 60 * 60 * 1000;
        assert_eq!(set_deadline(&dir, sooner, NOW).expect("moved").deadline_millis, sooner);
        let behind = vec![vec![state.outgoing.clone()]];
        assert!(!cut_over_if_due(&dir, names(), A_YEAR, Some(&behind), sooner - 1).unwrap());
        assert!(cut_over_if_due(&dir, names(), A_YEAR, Some(&behind), sooner).unwrap());
        assert!(
            matches!(set_deadline(&dir, sooner + 1, NOW), Err(Refused::Conflict(_))),
            "nothing is open"
        );
    }

    /// One field takes the whole chain: a root at its end is the authority and is not served, and a chain under a
    /// root nothing here holds is no upload at all.
    #[test]
    fn an_uploaded_chain_carries_its_own_root() {
        let (chain, _, ca) = elsewhere("split-theirs", names());
        let mut whole = chain.clone();
        whole.extend_from_slice(&ca);
        let (served, root) = split_root(&whole, b"").unwrap().expect("its own root");
        assert_eq!(
            describe_each(&served, SERVER_CERT).unwrap().len(),
            1,
            "the root was served"
        );
        assert_eq!(
            describe(root.as_bytes(), CA_CERT).unwrap().fingerprint,
            describe(&ca, CA_CERT).unwrap().fingerprint
        );
        assert!(
            split_root(&chain, b"").unwrap().is_none(),
            "no root was found and none was refused"
        );
        let (_, _, stranger) = elsewhere("split-stranger", names());
        let mut wrong = chain.clone();
        wrong.extend_from_slice(&stranger);
        assert!(
            split_root(&wrong, b"").is_err(),
            "a root that signed nothing here was taken"
        );
    }

    /// Abandoning a switch is the same as abandoning a rotation: the incoming root leaves the bundle, what was
    /// staged goes, and what is served never moved.
    #[test]
    fn abandoning_a_switch_leaves_what_is_in_force_alone() {
        let dir = rotating("switch-abandon-up");
        let before = what_is_served(&dir);
        let own = roots(&dir);
        let (chain, key, ca) = elsewhere("switch-abandon-up-theirs", names());
        let state = rotate_to(&dir, &chain, &key, &ca, &names(), &[], NOW).expect("a rotation");
        abandon_rotation(&dir, &state.incoming).expect("abandoned");
        assert_eq!(published(&dir), own);
        assert_eq!(what_is_served(&dir), before);
        assert!(rotation(&dir).unwrap().is_none());
        ensure(&dir, names(), A_YEAR, &[]).expect("a start after abandoning");

        let dir = rotating("switch-abandon-down");
        let (chain, key, ca) = elsewhere("switch-abandon-down-theirs", names());
        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a supplied set");
        ensure_own(&dir, names(), A_YEAR).expect("the leader's own");
        let state = begin_own_rotation(&dir, NOW).expect("a rotation");
        abandon_rotation(&dir, &state.incoming).expect("abandoned");
        assert_eq!(published(&dir), vec![state.outgoing]);
        assert_eq!(what_is_served(&dir), chain);
        assert!(current(&dir).unwrap().supplied);
        assert!(rotation(&dir).unwrap().is_none());
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start after abandoning");
        assert!(materials.supplied);
    }

    /// Past the renames there is no set to go back to, so a switch is as final as a promotion.
    #[test]
    fn a_switch_past_its_installing_cannot_be_abandoned() {
        let dir = rotating("switch-final");
        let (chain, key, ca) = elsewhere("switch-final-theirs", names());
        let state = rotate_to(&dir, &chain, &key, &ca, &names(), &[], NOW).expect("a rotation");
        // The entry is served, and nothing after that ran.
        torn_cutover(&dir, &state);
        assert!(cut_over_began(&dir).unwrap());
        assert!(matches!(
            abandon_rotation(&dir, &state.incoming),
            Err(Refused::Conflict(_))
        ));
        cut_over_is_possible(&dir).expect("finishable from here");
        cut_over(&dir, names(), A_YEAR).expect("finished");
        assert_eq!(
            roots(&dir),
            vec![state.outgoing],
            "the leader's own bundle kept the uploaded root"
        );
    }

    /// Each gap in the order a switch is written, settled by the next start, as the leader's own rotation is.
    #[test]
    fn a_switch_interrupted_at_any_step_is_settled_by_the_next_start() {
        // Between the intent and the bundle: the root was never published.
        let dir = rotating("switch-torn-bundle");
        let (chain, key, ca) = elsewhere("switch-torn-theirs", names());
        let state = rotate_to(&dir, &chain, &key, &ca, &names(), &[], NOW).expect("a rotation");
        let outgoing_only = without(&std::fs::read(dir.join(CA_CERT)).unwrap(), &state.incoming).unwrap();
        write_durably(&dir.join(CA_CERT), &outgoing_only).unwrap();
        ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(
            published(&dir).contains(&state.incoming),
            "the switch stalled with nothing published"
        );

        // The entry gone from the list while the intent stands: nothing left to serve.
        let dir = rotating("switch-lost-entry");
        let state = rotate_to(&dir, &chain, &key, &ca, &names(), &[], NOW).expect("a rotation");
        lose_entry(&dir, state.clone());
        ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(
            rotation(&dir).unwrap().is_none(),
            "a switch that cannot be finished was kept"
        );
        assert!(
            !published(&dir).contains(&state.incoming),
            "a root nothing serves was left published"
        );

        // Between serving the entry and dropping the intent: the switch already happened.
        let dir = rotating("switch-torn-cutover");
        let own = signing(&dir);
        let state = rotate_to(&dir, &chain, &key, &ca, &names(), &[], NOW).expect("a rotation");
        torn_cutover(&dir, &state);
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(materials.supplied);
        assert!(rotation(&dir).unwrap().is_none(), "a finished switch was left open");
        assert_eq!(published(&dir), vec![state.incoming]);
        assert_eq!(roots(&dir), vec![own], "the leader's own bundle kept the uploaded root");

        // The other way: the leader's own lost while a withdrawal waits.
        let dir = rotating("switch-lost-own");
        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a supplied set");
        ensure_own(&dir, names(), A_YEAR).expect("the leader's own");
        let state = begin_own_rotation(&dir, NOW).expect("a rotation");
        std::fs::remove_file(dir.join(CA_KEY)).unwrap();
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(materials.supplied);
        assert!(
            rotation(&dir).unwrap().is_none(),
            "a switch that cannot be finished was kept"
        );
        assert!(!published(&dir).contains(&state.incoming));

        // And a withdrawal interrupted after the entry stopped being served.
        let dir = rotating("switch-torn-withdrawal");
        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a supplied set");
        ensure_own(&dir, names(), A_YEAR).expect("the leader's own");
        let state = begin_own_rotation(&dir, NOW).expect("a rotation");
        torn_cutover(&dir, &state);
        let materials = ensure(&dir, names(), A_YEAR, &[]).expect("a start");
        assert!(!materials.supplied);
        assert!(rotation(&dir).unwrap().is_none(), "a finished switch was left open");
        assert_eq!(published(&dir), vec![state.incoming]);
    }

    /// A rotation written before a cutover could install anything but the next key reads as one that promotes it.
    #[test]
    fn a_rotation_written_before_switches_existed_promotes_the_next_key() {
        let dir = rotating("old-intent");
        let state = begin_rotation(&dir, A_YEAR, A_WEEK_OUT, NOW).expect("a rotation");
        let json = std::fs::read_to_string(dir.join(ROTATION)).unwrap();
        assert!(json.contains("\"installs\": \"nextKey\""), "{json}");
        let mut old: serde_json::Value = serde_json::from_str(&json).unwrap();
        old.as_object_mut().unwrap().remove("installs");
        write_durably(&dir.join(ROTATION), old.to_string().as_bytes()).unwrap();

        let read = rotation(&dir).unwrap().expect("still a rotation");
        assert_eq!(read.installs, Installs::NextKey);
        assert_eq!(read.incoming, state.incoming);
        cut_over(&dir, names(), A_YEAR).expect("a cutover");
        assert_eq!(signing(&dir), state.incoming);
    }

    /// Which rotations may begin depends on what is in force: the leader's own authority is not rotated under an
    /// operator's certificate, and there is nothing to withdraw when none is supplied.
    #[test]
    fn which_rotation_may_begin_depends_on_what_is_in_force() {
        let dir = rotating("may-begin-kinds");
        rotation_may_begin(&dir, Installs::NextKey).expect("the leader's own");
        rotation_may_begin(&dir, Installs::Supplied).expect("an upload");
        assert!(matches!(
            rotation_may_begin(&dir, Installs::Own),
            Err(Refused::Conflict(_))
        ));

        let (chain, key, ca) = elsewhere("may-begin-kinds-theirs", names());
        install(&dir, &chain, &key, &ca, &names(), &[]).expect("a supplied set");
        assert!(matches!(
            rotation_may_begin(&dir, Installs::NextKey),
            Err(Refused::Conflict(_))
        ));
        rotation_may_begin(&dir, Installs::Supplied).expect("another upload");
        rotation_may_begin(&dir, Installs::Own).expect("a withdrawal");
    }
}

/// Every way the certificate in force can change, in every order, with the leader interrupted and restarted
/// between any two steps. After each step what is served verifies against what workers are handed, and after each
/// restart nothing is left that no rotation claims.
#[cfg(test)]
mod certificate_fuzz {
    use super::tests::{
        A_WEEK_OUT, A_YEAR, NOW, elsewhere, install, lose_entry, names, rotate_to, scratch, torn_cutover,
    };
    use super::*;
    use crate::fuzz::{Coverage, Rng, run};

    /// What a fuzz step may draw.
    const STEPS: &[&str] = &[
        "upload-rotates",
        "upload-renewal",
        "withdraw-rotates",
        "withdraw-direct",
        "rotate-own",
        "cutover-nextKey",
        "cutover-supplied",
        "cutover-own",
        "fleet-cutover",
        "deadline-cutover",
        "abandon-nextKey",
        "abandon-supplied",
        "abandon-own",
        "crash-torn-bundle",
        "crash-lost",
        "crash-torn-cutover",
        "crash-stray",
        "restart",
    ];

    /// An operator's PKI, issuing again from the same authority when asked.
    struct Pki {
        dir: PathBuf,
    }

    impl Pki {
        fn new(tag: &str) -> Pki {
            let (_, _, _) = elsewhere(tag, names());
            Pki {
                dir: std::env::temp_dir()
                    .join(format!("smeltr-tls-{}-{tag}", std::process::id()))
                    .join("tls"),
            }
        }

        /// A certificate under this authority, freshly issued.
        fn issue(&self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
            let materials = ensure(&self.dir, names(), A_YEAR, &[]).expect("their authority");
            (
                materials.served.clone(),
                std::fs::read(self.dir.join(SERVER_KEY)).unwrap(),
                materials.ca_pem,
            )
        }
    }

    fn in_force_signing(dir: &Path) -> Result<String, String> {
        let materials = current(dir)?;
        Ok(signing_root(&materials.ca_pem, &materials.served)?.fingerprint)
    }

    fn published(dir: &Path) -> Result<Vec<String>, String> {
        Ok(describe_each(&current(dir)?.ca_pem, CA_CERT)?
            .into_iter()
            .map(|d| d.fingerprint)
            .collect())
    }

    /// What must hold after every step: what is served verifies against what workers are handed, and an open
    /// rotation hands them both roots.
    fn serves(dir: &Path) -> Result<(), String> {
        let materials = current(dir)?;
        verify(&materials.ca_pem, &materials.served, "localhost")?;
        if let Some(state) = rotation(dir)? {
            let bundle = published(dir)?;
            if !bundle.contains(&state.incoming) || !bundle.contains(&state.outgoing) {
                return Err(format!("the overlap is not published: {bundle:?} for {state:?}"));
            }
            if !promoted(dir, &state)? && in_force_signing(dir)? != state.outgoing {
                return Err("what is served is signed under neither side of the rotation".to_string());
            }
        }
        Ok(())
    }

    /// What a start settles: nothing left that no rotation claims, and a rotation that is left is finishable.
    fn settled(dir: &Path) -> Result<(), String> {
        serves(dir)?;
        match rotation(dir)? {
            None => {
                if dir.join(NEXT_CA_KEY).exists() {
                    return Err(format!("{NEXT_CA_KEY} was left behind with no rotation claiming it"));
                }
                let bundle = published(dir)?;
                if bundle.len() != 1 {
                    return Err(format!("no rotation is open and workers are handed {bundle:?}"));
                }
            }
            Some(state) => {
                if promoted(dir, &state)? {
                    return Err("a start left a cutover half-done".to_string());
                }
                // Parked under a supplied set, the leader's own rotation finishes once that set is withdrawn.
                if state.installs == Installs::NextKey && current(dir)?.supplied {
                    return Ok(());
                }
                cut_over_is_possible(dir).map_err(|e| format!("a start left a rotation that cannot finish: {e}"))?;
            }
        }
        Ok(())
    }

    fn restart(dir: &Path) -> Result<(), String> {
        ensure(dir, names(), A_YEAR, &[]).map_err(|e| format!("a start refused: {e}"))?;
        settled(dir)
    }

    fn step(rng: &mut Rng, dir: &Path, pkis: &[Pki], coverage: &mut Coverage) -> Result<String, String> {
        let open = rotation(dir)?;
        let supplied = current(dir)?.supplied;
        let step = *rng.pick(STEPS);
        match step {
            "upload-rotates" | "upload-renewal" => {
                // Sometimes the leader's own certificate, uploaded as a supplied set: the one upload under the
                // root in force that is not a renewal of somebody else's, and the one an open rotation of the
                // leader's own key takes -- so that is what it gets while one is open.
                let parks = open.as_ref().is_some_and(|state| state.installs == Installs::NextKey);
                let (chain, key, ca) = if parks || rng.chance(3) {
                    let own = current(dir)?;
                    let own_cert = dir.join(SERVER_CERT);
                    let own_key = dir.join(SERVER_KEY);
                    let own_ca = dir.join(CA_CERT);
                    if own.supplied || !own_cert.exists() {
                        rng.pick(pkis).issue()
                    } else {
                        (
                            std::fs::read(own_cert).map_err(|e| e.to_string())?,
                            std::fs::read(own_key).map_err(|e| e.to_string())?,
                            std::fs::read(own_ca).map_err(|e| e.to_string())?,
                        )
                    }
                } else {
                    rng.pick(pkis).issue()
                };
                let rotates = supplying_changes_root(dir, &chain, &ca)?;
                if open.is_some() && rotates {
                    let refused = rotate_to(dir, &chain, &key, &ca, &names(), &[], NOW)
                        .err()
                        .ok_or("an upload began a second rotation")?;
                    coverage.reach("refused-while-open");
                    return Ok(format!("upload refused: {refused}"));
                }
                if open.is_some() {
                    // A renewal is served at once whatever is open, and the overlap stays published.
                    install(dir, &chain, &key, &ca, &names(), &[])?;
                    coverage.reach("upload-renewal-while-open");
                    return Ok("upload under the root in force, during a rotation".to_string());
                }
                if rotates {
                    rotate_to(dir, &chain, &key, &ca, &names(), &[], NOW)?;
                    coverage.reach("upload-rotates");
                    Ok("upload under another root".to_string())
                } else {
                    install(dir, &chain, &key, &ca, &names(), &[])?;
                    coverage.reach("upload-renewal");
                    Ok("upload under the root in force".to_string())
                }
            }
            "withdraw-rotates" | "withdraw-direct" => {
                if !supplied {
                    rotation_may_begin(dir, Installs::Own)
                        .err()
                        .ok_or("nothing to withdraw, and allowed")?;
                    return Ok("withdrawal refused: nothing supplied".to_string());
                }
                if open.is_some() {
                    rotation_may_begin(dir, Installs::Own)
                        .err()
                        .ok_or("a withdrawal began a second rotation")?;
                    coverage.reach("refused-while-open");
                    return Ok("withdrawal refused: a rotation is open".to_string());
                }
                ensure_own(dir, names(), A_YEAR)?;
                if withdrawing_changes_root(dir)? {
                    begin_own_rotation(dir, NOW)?;
                    coverage.reach("withdraw-rotates");
                    Ok("withdrawal under another root".to_string())
                } else {
                    uninstall(dir)?;
                    coverage.reach("withdraw-direct");
                    Ok("withdrawal under the root in force".to_string())
                }
            }
            "rotate-own" => {
                let begun = begin_rotation(dir, A_YEAR, A_WEEK_OUT, NOW);
                if open.is_some() || supplied {
                    begun
                        .err()
                        .ok_or("the leader's own rotation began over another, or under a supplied set")?;
                    if open.is_some() {
                        coverage.reach("refused-while-open");
                    }
                    return Ok("the leader's own rotation refused".to_string());
                }
                begun?;
                coverage.reach("rotate-own");
                Ok("the leader's own rotation".to_string())
            }
            "cutover-nextKey" | "cutover-supplied" | "cutover-own" => {
                let Some(state) = open else {
                    cut_over(dir, names(), A_YEAR)
                        .err()
                        .ok_or("a cutover with no rotation")?;
                    return Ok("cutover refused".to_string());
                };
                if state.installs == Installs::NextKey && supplied {
                    cut_over(dir, names(), A_YEAR)
                        .err()
                        .ok_or("the leader's own key was promoted beneath a supplied set")?;
                    cut_over_is_possible(dir)
                        .err()
                        .ok_or("a parked rotation read as one that could cut over")?;
                    coverage.reach("cutover-parked");
                    return Ok("cutover refused: parked under a supplied set".to_string());
                }
                cut_over(dir, names(), A_YEAR)?;
                coverage.reach(format!("cutover-{}", kind(state.installs)));
                if in_force_signing(dir)? != state.incoming {
                    return Err("a cutover did not sign under the incoming root".to_string());
                }
                Ok(format!("cut over to {}", kind(state.installs)))
            }
            "fleet-cutover" | "deadline-cutover" => {
                let Some(state) = open else {
                    return Ok("a beat with nothing to cut over".to_string());
                };
                let (holders, now, what) = if step == "fleet-cutover" {
                    (
                        vec![vec![state.outgoing.clone(), state.incoming.clone()]],
                        NOW,
                        "fleet-cutover",
                    )
                } else {
                    (
                        vec![vec![state.outgoing.clone()]],
                        state.deadline_millis,
                        "deadline-cutover",
                    )
                };
                let behind = vec![vec![state.outgoing.clone()]];
                if cut_over_if_due(dir, names(), A_YEAR, Some(&behind), NOW)? {
                    return Err("a beat cut over under a worker still behind".to_string());
                }
                if state.installs == Installs::NextKey && supplied {
                    // Parked: a beat waits however ready the fleet is and however late the hour.
                    if cut_over_if_due(dir, names(), A_YEAR, Some(&holders), state.deadline_millis)? {
                        return Err("a beat promoted the leader's own key beneath a supplied set".to_string());
                    }
                    return Ok("a beat with a parked rotation".to_string());
                }
                if !cut_over_if_due(dir, names(), A_YEAR, Some(&holders), now)? {
                    return Err(format!("{what} did not cut over"));
                }
                coverage.reach(what);
                Ok(what.to_string())
            }
            "abandon-nextKey" | "abandon-supplied" | "abandon-own" => {
                let Some(state) = open else {
                    return Ok("nothing to abandon".to_string());
                };
                abandon_rotation(dir, &state.incoming).map_err(|e| e.to_string())?;
                coverage.reach(format!("abandon-{}", kind(state.installs)));
                if rotation(dir)?.is_some() || published(dir)?.contains(&state.incoming) {
                    return Err("an abandoned rotation is still there".to_string());
                }
                Ok(format!("abandoned {}", kind(state.installs)))
            }
            "crash-torn-bundle" => {
                let Some(state) = open else {
                    return Ok("nothing to tear".to_string());
                };
                // Under a list entry the overlap is derived from the intent, and there is no bundle to tear.
                if served(dir)?.is_some() {
                    return Ok("nothing to tear: the overlap is derived".to_string());
                }
                retire(&dir.join(CA_CERT), &state.incoming)?;
                coverage.reach(step);
                restart(dir)?;
                if !published(dir)?.contains(&state.incoming) {
                    return Err("a start did not republish the incoming root".to_string());
                }
                Ok("the bundle torn, then a start".to_string())
            }
            "crash-lost" => {
                let Some(state) = open else {
                    return Ok("nothing to lose".to_string());
                };
                match state.installs {
                    Installs::NextKey => remove_file_durably(&dir.join(NEXT_CA_KEY))?,
                    Installs::Supplied => lose_entry(dir, state.clone()),
                    Installs::Own => remove_file_durably(&dir.join(CA_KEY))?,
                }
                coverage.reach(step);
                restart(dir)?;
                if rotation(dir)?.is_some() {
                    return Err("a start kept a rotation with nothing to cut over to".to_string());
                }
                Ok("the incoming side lost, then a start".to_string())
            }
            "crash-torn-cutover" => {
                let Some(state) = open else {
                    return Ok("nothing to tear".to_string());
                };
                match state.installs {
                    Installs::NextKey => rename_durably(&dir.join(NEXT_CA_KEY), &dir.join(CA_KEY))?,
                    Installs::Supplied | Installs::Own => torn_cutover(dir, &state),
                }
                coverage.reach(step);
                restart(dir)?;
                if rotation(dir)?.is_some() || in_force_signing(dir)? != state.incoming {
                    return Err("a start did not finish a cutover past its promotion".to_string());
                }
                Ok("the cutover torn after its promotion, then a start".to_string())
            }
            "crash-stray" => {
                if open.is_some() {
                    return Ok("nothing stray while a rotation is open".to_string());
                }
                write_durably(&dir.join(NEXT_CA_KEY), b"a key nothing claims").map_err(|e| e.to_string())?;
                coverage.reach(step);
                restart(dir)?;
                Ok("a beginning that died before its intent, then a start".to_string())
            }
            "restart" => {
                restart(dir)?;
                coverage.reach(step);
                Ok("a start".to_string())
            }
            _ => unreachable!(),
        }
    }

    fn kind(installs: Installs) -> &'static str {
        match installs {
            Installs::NextKey => "nextKey",
            Installs::Supplied => "supplied",
            Installs::Own => "own",
        }
    }

    #[test]
    fn every_order_of_certificate_changes_serves_and_restarts() {
        // Every step, and the refusal a step meets while a rotation is open, which no step draws by itself.
        let mut coverage = Coverage::new(STEPS.iter().copied().chain([
            "refused-while-open",
            "upload-renewal-while-open",
            "cutover-parked",
        ]));
        let pkis = [Pki::new("fuzz-pki-a"), Pki::new("fuzz-pki-b")];
        run(0..48, |seed| {
            let mut rng = Rng::seeded(seed);
            let dir = scratch(&format!("fuzz-{seed}"));
            restart(&dir)?;
            let mut made = Vec::new();
            for _ in 0..24 {
                let drew = step(&mut rng, &dir, &pkis, &mut coverage)
                    .map_err(|e| format!("after {}:\n  {e}", made.join("\n  ")))?;
                serves(&dir).map_err(|e| format!("after {}:\n  {drew}: {e}", made.join("\n  ")))?;
                made.push(drew);
            }
            restart(&dir).map_err(|e| format!("after {}:\n  {e}", made.join("\n  ")))
        });
        coverage.assert_complete();
    }
}
