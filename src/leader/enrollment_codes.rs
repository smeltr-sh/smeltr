//! Minting, listing and withdrawing enrollment codes.
//!
//! An enrollment code is the one value an operator pastes to bring a worker in. Minting one reads the authority this
//! leader is serving under, so a worker taking it knows what to accept before it has anything else to go on.
//!
//! The leader's address is not minted with it. This process cannot know what address reaches it from somewhere else --
//! a bind of `127.0.0.1` and a proxy both make that a guess -- so the page that shows an enrollment code fills it in
//! from the address the operator reached the leader at, and lets them correct it.

use axum::Json;
use axum::extract::{Path, State};

use crate::enrollment_code::{Authority, EnrollmentCode};
use crate::leader::envelope::Envelope;
use crate::leader::store::{Actor, EnrollmentCodeRecord, NewEnrollmentCode, WorkerKind};
use crate::leader::{AppState, auth, run_store, tls, warnings};

/// What a permanent enrollment code brings in and how long it lasts, where the operator says nothing: the one machine
/// they are walking to, and a day to get there. There is no ceiling; how far an enrollment code reaches is the
/// operator's call.
const DEFAULT_USES: i64 = 1;
const DEFAULT_LIFETIME_HOURS: i64 = 24;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintRequest {
    /// The group the command will name. Pre-fills it and binds nothing: a worker names its own group.
    #[serde(default)]
    group_id: String,
    /// `permanent` or `ephemeral`; `permanent` when absent.
    #[serde(default)]
    kind: Option<String>,
    /// A permanent enrollment code's only. An ephemeral enrollment code has no cap and no expiry.
    #[serde(default)]
    max_uses: Option<i64>,
    #[serde(default)]
    lifetime_hours: Option<i64>,
}

#[derive(Default, serde::Serialize)]
pub struct Minted {
    code: Option<MintedEnrollmentCode>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MintedEnrollmentCode {
    id: String,
    /// The whole pasteable value. Shown once: the leader keeps only its hash.
    value: String,
    group_id: String,
    kind: &'static str,
    /// `None`: no expiry of its own.
    expires_at_millis: Option<i64>,
    /// `None`: no cap on how many workers it brings in.
    max_uses: Option<i64>,
}

/// `POST /api/enrollment-codes`.
///
/// `actor` is what the gate attached. It is optional because the router this handler sits in is composed without a
/// gate by tests and by anything embedding the leader; in a served process the gate always ran, and there is no
/// route to here that skips it.
pub async fn mint(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Json(request): Json<MintRequest>,
) -> Json<Envelope<Minted>> {
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let kind = match request.kind.as_deref() {
        None => WorkerKind::Permanent,
        Some(kind) => match WorkerKind::parse(kind) {
            Some(kind) => kind,
            None => {
                return Json(Envelope::failed_one(format!(
                    "{kind:?} is not a kind of enrollment code: permanent or ephemeral."
                )));
            }
        },
    };
    let (max_uses, lifetime_hours) = match kind {
        WorkerKind::Permanent => {
            let max_uses = request.max_uses.unwrap_or(DEFAULT_USES);
            if max_uses < 1 {
                return Json(Envelope::failed_one(
                    "an enrollment code brings in at least one worker.",
                ));
            }
            let lifetime_hours = request.lifetime_hours.unwrap_or(DEFAULT_LIFETIME_HOURS);
            if lifetime_hours < 1 {
                return Json(Envelope::failed_one("an enrollment code lasts at least an hour."));
            }
            (Some(max_uses), Some(lifetime_hours))
        }
        // An autoscaler spends it for as long as it runs, so a cap or a date would be scale-out stopping one
        // night with nobody watching. Refused rather than ignored, so nobody believes they bounded one.
        WorkerKind::Ephemeral => {
            if request.max_uses.is_some() || request.lifetime_hours.is_some() {
                return Json(Envelope::failed_one(
                    "an ephemeral enrollment code has no cap and no expiry: it brings in workers until it is withdrawn.",
                ));
            }
            (None, None)
        }
    };

    // A group id no worker could run is refused here, where it costs nothing. Accepted, it is copied into a
    // command, spends a use, and buys a credential -- and then every deployment is refused for a name chosen
    // minutes earlier and long forgotten. Empty is the worker naming its own.
    if !request.group_id.is_empty() {
        if let Err(e) = crate::config_file::group_subtree(&request.group_id) {
            return Json(Envelope::failed_one(e));
        }
        // Syntax alone would also accept a group only proposed on a branch, or deleted from `main` since. A worker
        // taking this enrollment code can name no group but the one baked into its command, so one `main` does not yet
        // or no longer serve is exactly the credential no deployment will ever answer for.
        if !super::deployed_groups(&state.data_dir).contains(&request.group_id) {
            return Json(Envelope::failed_one(format!(
                "{:?} is not a deployed worker group. Deploy it, or mint without naming one.",
                request.group_id
            )));
        }
    }

    // An enrollment code names one root, and a rotation is the window where which one it should name has no good
    // answer. The outgoing root is what the leader serves today and will stop serving; the incoming one is what it will
    // serve and does not yet. A worker keeps exactly the certificate its enrollment code names and refuses the rest, so
    // either choice buys a credential the worker cannot then use. The window is bounded and the operator holds both
    // ends of it.
    match tls::rotation(&tls::dir_of(&state.data_dir)) {
        Ok(Some(_)) => {
            return Json(Envelope::failed_one(
                "this leader is rotating its certificate authority, and an enrollment code minted now would name a \
                 root it is about to stop serving. Cut the rotation over, or abandon it, and mint again.",
            ));
        }
        Ok(None) => {}
        Err(e) => return Json(Envelope::failed_one(e)),
    }

    let (authority, authority_expires) = match authority_of(&state) {
        Ok(answered) => answered,
        Err(e) => return Json(Envelope::failed_one(e)),
    };

    let secret = auth::generate_secret();
    // Named independently of what it holds. An id derived from the secret's hash would be a public prefix of it,
    // and an id is shown, logged and put in a URL.
    let id = format!("ec_{}", &auth::generate_secret()[..24]);
    let now_ms = warnings::now_millis();
    // Never past the authority it pins to, whatever kind it is. A worker taking this enrollment code keeps that one
    // certificate and accepts no other, so beyond that date the enrollment code buys a credential for a leader the
    // worker can no longer verify.
    let lasts_until = lifetime_hours.map(|hours| now_ms.saturating_add(hours.saturating_mul(60 * 60 * 1000)));
    let expires_at = match authority_expires {
        Some(root_expires) if root_expires <= now_ms => {
            return Json(Envelope::failed_one(
                "this leader's certificate authority has expired, so an enrollment code minted now would pin a worker \
                 to a certificate it will refuse. Replace the authority first.",
            ));
        }
        Some(root_expires) => Some(lasts_until.map_or(root_expires, |until| until.min(root_expires))),
        None => lasts_until,
    };
    let record = NewEnrollmentCode {
        id: id.clone(),
        code_hash: auth::hash_secret(&secret),
        fingerprint: match &authority {
            Authority::Pinned(fingerprint) => fingerprint.clone(),
            Authority::System => String::new(),
        },
        group_id: request.group_id.clone(),
        minted_by: actor.id.clone(),
        minted_at: now_ms,
        expires_at,
        max_uses,
        kind,
    };

    let event = super::accounts::event_for(&actor, "enrollment-code-minted", &id, "ok");
    let written = run_store(state.store, move |s| s.create_enrollment_code_recorded(&record, &event)).await;
    if let Err(e) = written {
        return Json(Envelope::failed_one(e));
    }

    Json(Envelope::ok(Minted {
        code: Some(MintedEnrollmentCode {
            id,
            value: EnrollmentCode { authority, secret }.to_string(),
            group_id: request.group_id,
            kind: kind.as_str(),
            expires_at_millis: expires_at,
            max_uses,
        }),
    }))
}

/// Whether this leader presents a certificate of its own, which is the only case where the root an enrollment code pins
/// is this leader's to have an opinion about.
///
/// Two questions. Whether this process is serving TLS, because a leader moved behind a proxy keeps the authority
/// it once made and pinning workers to a certificate it no longer presents would strand every one of them --
/// answered outright, with nothing on disk to fail reading. And whether there is an authority at all, because a
/// leader serving no TLS of its own has none to name -- asked with [`tls::has_own_material`] rather than a plain
/// existence check, since an upload's own two renames leave a set of this leader's under a third name for the
/// length of one, and a check that missed it would read a leader mid-upload the same as one that never had a
/// certificate. `Err` where that could not be told, which is not the same fact as either yes or no.
fn serving_its_own(dir: &std::path::Path) -> Result<bool, String> {
    if !crate::startup_config::leader_tls_enabled() {
        return Ok(false);
    }
    tls::has_own_material(dir)
}

/// What this leader is presenting, as everything that decides whether an enrollment code still brings a worker in.
///
/// One value, read once, because every surface that speaks about an enrollment code has to say the same thing about it:
/// the page an operator acts on, the warning that alerts them, the withdrawal a cutover performs, and the enrollment
/// that refuses. Answered apart, those drifted -- each reading its own subset of whether a rotation is open, whether
/// this leader presents a certificate of its own, whether the socket agrees with the disk, and whose certificate it is.
///
/// Read off the disk rather than off what the socket presents. The two part for as long as a reload takes, and in
/// that gap the socket still offers a root a cutover has just retired -- which is a window, not an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Presenting {
    /// A certificate of this leader's own, signed under `signing`, and under `coming` once any open rotation
    /// finishes. The same root whenever none is open.
    Own { signing: String, coming: String },
    /// A certificate an operator supplied, signed under `signing`. An enrollment code naming some other root brings
    /// nobody in here either, but which of their roots one should name is theirs to say, so nothing is withdrawn on it.
    Theirs { signing: String },
    /// No certificate of this leader's own: it serves plain HTTP, or sits behind a proxy holding somebody else's.
    /// An enrollment code that pins a root pins one no handshake here presents.
    NothingOfItsOwn,
    /// The files could not be read, so nothing about any enrollment code is decided on.
    Unreadable,
}

/// What an enrollment code is worth against what this leader presents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// Brings a worker in, now and after any open rotation.
    Good,
    /// Brings a worker in until the cutover, which retires the root it names.
    UntilCutover,
    /// Names a root this leader has left. Brings nobody in.
    Stale,
}

impl Presenting {
    pub fn read(dir: &std::path::Path) -> Presenting {
        match serving_its_own(dir) {
            Ok(false) => return Presenting::NothingOfItsOwn,
            Err(_) => return Presenting::Unreadable,
            Ok(true) => {}
        }
        let Ok((materials, rotation)) = tls::current_and_rotation(dir) else {
            return Presenting::Unreadable;
        };
        let Ok(root) = tls::signing_root(&materials.ca_pem, &materials.served) else {
            return Presenting::Unreadable;
        };
        if materials.supplied {
            return Presenting::Theirs {
                signing: root.fingerprint,
            };
        }
        // A rotation publishes the incoming root beside the one being served and moves nothing else, so this is
        // the only place the two roots differ. Read under the same lock as the certificate: a cutover between the
        // two would remove the rotation record and pair the root it just retired with an answer that says nothing
        // is being replaced.
        let coming = match rotation {
            Some(rotation) => rotation.incoming,
            None => root.fingerprint.clone(),
        };
        Presenting::Own {
            signing: root.fingerprint,
            coming,
        }
    }

    /// What `fingerprint`, as this leader wrote it down when it minted the enrollment code, is worth now.
    pub fn standing_of(&self, fingerprint: &str) -> Standing {
        let Some(may) = self.may_have_named() else {
            // Not the enrollment code's fault, and not something to send an operator to replace an enrollment code
            // over.
            return Standing::Good;
        };
        if !may.contains(&fingerprint) {
            return Standing::Stale;
        }
        match self {
            Presenting::Own { signing, coming } if signing != coming && fingerprint == signing => {
                Standing::UntilCutover
            }
            _ => Standing::Good,
        }
    }

    /// The roots an enrollment code may have been minted against and still bring a worker in.
    ///
    /// One list, which [`Presenting::standing_of`] reads rather than deciding again: the page, the warning, the
    /// withdrawal and the spend all turn on this, and the spend is in SQL's company where `standing_of` cannot be
    /// called. Two answers to one question is how the page came to call an enrollment code good that the spend refused.
    ///
    /// Ordered, and the first is the one an enrollment code minted now names -- see [`Presenting::mints_against`].
    ///
    /// `None` where this leader cannot say. That is a refusal for the caller to make, not permission: an enrollment
    /// code let in against no check at all is the whole hole this closes, and a worker at a first boot racing a leader
    /// still coming up asks again in a moment and gets a real answer.
    pub fn may_have_named(&self) -> Option<Vec<&str>> {
        match self {
            // The root being moved to as well: ordinary minting is refused while a rotation is open, so nothing names
            // it -- except the enrollment code this leader mints for the worker inside its own process, which has to
            // survive whichever way that rotation ends.
            //
            // And naming no root, for the same worker. It is handed the authority out of band and never reads the
            // field, so there is no root it could name that a cutover or an abandonment could not turn against it:
            // whichever of the two this leader keeps, the other is the one the enrollment code would have been minted
            // for. The cost is that an enrollment code minted long ago on a leader with no certificate of its own reads
            // as usable here, on a leader whose certificate it cannot reach. Telling the two apart needs the enrollment
            // code to say which it is, which is a column it does not have.
            Presenting::Own { signing, coming } => Some(vec![signing, coming, ""]),
            // Whether the platform's roots reach an operator's certificate is theirs to know. An enrollment code naming
            // no root reaches this leader where they do, and fails the handshake before it gets here where they do not
            // -- so refusing it here would only turn a working enrollment into a refusal.
            Presenting::Theirs { signing } => Some(vec![signing, ""]),
            // Nothing of this leader's own is presented, so naming no root is the only thing that reaches it.
            Presenting::NothingOfItsOwn => Some(vec![""]),
            Presenting::Unreadable => None,
        }
    }

    /// The root to mint the worker inside this process an enrollment code against.
    ///
    /// Nothing, where this leader has a certificate of its own. That worker is handed the authority out of band and
    /// never reads the field, and naming either root of an open rotation would put the enrollment code at the mercy of
    /// how that rotation ends -- a cutover invalidates one, an abandonment the other, and a refused enrollment stops
    /// this process rather than just that thread.
    ///
    /// The operator's own certificate has no such pair, so an enrollment code minted under one names it.
    ///
    /// Always something [`Presenting::may_have_named`] accepts, which is what
    /// `what_this_leader_mints_against_is_something_it_accepts` holds it to.
    pub fn mints_against(&self) -> &str {
        match self {
            Presenting::Theirs { signing } => signing,
            Presenting::Own { .. } | Presenting::NothingOfItsOwn | Presenting::Unreadable => "",
        }
    }

    /// Whether this leader may withdraw an enrollment code on its own account. Only where the root it measures against
    /// is one it presents itself: between certificates, or holding an operator's, it may be mid-migration, and a
    /// withdrawal is not something an operator can undo.
    pub fn may_withdraw(&self) -> bool {
        matches!(self, Presenting::Own { .. })
    }

    /// Whether a rotation is open, which is the only time an enrollment code can be [`Standing::UntilCutover`].
    pub fn rotating(&self) -> bool {
        matches!(self, Presenting::Own { signing, coming } if signing != coming)
    }
}

/// The authority a worker taking this enrollment code will pin to, and when it expires.
///
/// A leader with no authority of its own is one serving plain HTTP, or one behind a proxy holding a publicly
/// issued certificate. Both are verified by the platform's own roots, and a fingerprint would pin a worker to a
/// certificate the leader never presents.
fn authority_of(state: &AppState) -> Result<(Authority, Option<i64>), String> {
    let dir = tls::dir_of(&state.data_dir);
    if !serving_its_own(&dir)? {
        return Ok((Authority::System, None));
    }
    // A certificate that reached the disk and was refused by the socket leaves the two describing different leaders
    // until a restart. An enrollment code minted from the disk would pin a worker to a root no handshake presents, and
    // that worker refuses the leader before it can say why. Both halves from one set, and only while the socket is
    // presenting it. A certificate that reached the disk and was refused leaves the two describing different leaders,
    // and an enrollment code minted from the disk would pin a worker to a root no handshake presents -- which that
    // worker reads as permanent. Asked as one answer, because an upload swapping whole directories between the question
    // and the read pairs one set's authority with another's certificate.
    let materials = tls::current_if_agreed(&dir).map_err(|e| match e {
        tls::Refused::Conflict(_) => "this leader is still serving the certificate before the last one, which is not \
             what is on disk. An enrollment code minted now would pin a worker to an authority no handshake presents. \
             Restart this leader first."
            .to_string(),
        tls::Refused::Unavailable(why) => why,
    })?;
    // The root that signs what this leader serves, of however many the file holds. An enrollment code names one
    // fingerprint and a worker keeps the certificate matching it; during a rollover, naming the wrong one of two would
    // pin a worker to a root that vouches for nothing this leader presents.
    let root = tls::signing_root(&materials.ca_pem, &materials.served)?;
    Ok((Authority::Pinned(root.fingerprint), Some(root.not_after_millis)))
}

/// Which authority an enrollment code names, and so how long it goes on bringing a worker in.
///
/// Three states rather than a yes and a no: during a rotation an enrollment code naming the outgoing root still works,
/// and an operator told only that it does not match what is coming would withdraw the one thing they still have a
/// window to spend.
#[derive(Debug, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NamesAuthority {
    /// The one this leader serves under, and will still serve under when any open rotation finishes.
    Served,
    /// The one it is rotating away from. This enrollment code brings a worker in until the cutover, which withdraws it.
    UntilCutover,
    /// One it has already stopped serving under. This enrollment code brings nobody in.
    Replaced,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentCodeDto {
    id: String,
    group_id: String,
    /// Who minted it, by the name they sign in with. An account id names nobody to the person reading the page.
    minted_by: String,
    minted_at_millis: i64,
    kind: &'static str,
    /// `None`: no expiry of its own.
    expires_at_millis: Option<i64>,
    /// `None`: no cap on how many workers it brings in.
    max_uses: Option<i64>,
    uses: i64,
    used_by: Vec<String>,
    /// When an operator withdrew it. Set means it brings nobody else in, whatever its other dates say.
    withdrawn_at_millis: Option<i64>,
    names_authority: NamesAuthority,
}

/// What `GET /api/enrollment-codes` answers with, carrying `ok` like every other listing. An empty list and a leader
/// that cannot read its enrollment code table are different answers, and a page handed a bare array cannot tell them
/// apart.
#[derive(Default, serde::Serialize)]
pub struct EnrollmentCodes {
    codes: Vec<EnrollmentCodeDto>,
}

/// `GET /api/enrollment-codes`: what is outstanding, and what brought a worker in.
pub async fn list(State(state): State<AppState>) -> Json<Envelope<EnrollmentCodes>> {
    let presenting = Presenting::read(&tls::dir_of(&state.data_dir));
    let now_ms = warnings::now_millis();
    let listed = match run_store(state.store, move |s| {
        let codes = s.list_enrollment_codes(now_ms)?;
        // Resolved here rather than on the page: the page has no roster, and an account id names nobody.
        let mut named = Vec::with_capacity(codes.len());
        for code in codes {
            let who = s
                .username_of(&code.minted_by)?
                .unwrap_or_else(|| code.minted_by.clone());
            named.push((code, who));
        }
        Ok(named)
    })
    .await
    {
        Ok(listed) => listed,
        // An empty list reads as "nothing outstanding", which is the one answer a leader that cannot read its own
        // enrollment code table must not give.
        Err(e) => {
            return Json(Envelope::failed_one(e));
        }
    };
    Json(Envelope::ok(EnrollmentCodes {
        codes: listed
            .into_iter()
            .map(|(code, minted_by)| dto(code, minted_by, &presenting))
            .collect(),
    }))
}

fn dto(code: EnrollmentCodeRecord, minted_by: String, presenting: &Presenting) -> EnrollmentCodeDto {
    let names_authority = match presenting.standing_of(&code.fingerprint) {
        Standing::Good => NamesAuthority::Served,
        Standing::UntilCutover => NamesAuthority::UntilCutover,
        Standing::Stale => NamesAuthority::Replaced,
    };
    EnrollmentCodeDto {
        id: code.id,
        group_id: code.group_id,
        minted_by,
        minted_at_millis: code.minted_at,
        kind: code.kind.as_str(),
        expires_at_millis: code.expires_at,
        max_uses: code.max_uses,
        uses: code.uses,
        used_by: code.used_by,
        withdrawn_at_millis: code.withdrawn_at,
        names_authority,
    }
}

/// Withdraws every outstanding enrollment code that names a root other than the one this leader now signs under.
///
/// An enrollment code carries its root's fingerprint inside the value an operator pastes, so it cannot be re-pointed.
/// Once a cutover retires the root an enrollment code names, a worker spending it fetches the authority, finds nothing
/// matching, and retries forever against a leader that is answering correctly. Withdrawing it is what turns that into a
/// refusal the operator can read, and a row saying who withdrew it.
///
/// Tidying, not the guarantee. What keeps a stranded enrollment code from bringing a worker in is [`Presenting`], read
/// afresh wherever it matters: the enrollment holds the enrollment code to the root this leader signs under, and the
/// Enrollment codes page reads its standing live. This runs after the files have already moved, so a certificate
/// replaced between the two, or an enrollment code minted across the gap, leaves one outstanding that this never sees
/// -- shown as replaced, refused at enrollment, and missing only its audit row. Closing that gap means holding one lock
/// across the files and the store, which is a change to how this leader locks rather than a change here.
///
/// Called from both places a cutover happens -- the deadline a heartbeat carries past, and the button an operator
/// presses -- because an enrollment code left standing after either one is the same dead enrollment. One function
/// rather than a check at each, so the two cannot come to disagree about which enrollment codes those are.
///
/// Which root that is, is read here rather than taken from the caller. Both callers know the rotation they meant
/// before they act, and between looking and acting that rotation can be abandoned and another begun: a
/// fingerprint carried across that gap names the root to keep, and withdrawing everything else is the exact
/// opposite of this.
///
/// Read off the disk rather than off what the socket presents, because that is what a worker is handed and what
/// the next enrollment code will name -- a cutover the socket has not followed yet is still the answer for both.
///
/// Asked again at every start, because a cutover is not one act: the files change, and then this runs. A process that
/// stops in between comes back to a finished cutover with no rotation left to notice it, and would otherwise leave
/// those enrollment codes outstanding until the next rotation. A store that refused this is the same state, settled the
/// same way. Nothing else is owed on failure -- a cutover that already happened is not undone by a store that could not
/// be written.
///
/// Asks nothing about whether a rotation is open. Beginning one mints a root and publishes it beside the one
/// being served; abandoning one takes it back again. Neither moves what this leader signs under, so the answer
/// here is the same either way -- and a rotation begun in the moment after a cutover cannot make this skip the
/// enrollment codes that cutover just stranded.
///
/// Refuses to decide where an operator supplied the certificate, so no caller has to know it is in that state.
pub async fn withdraw_off_authority(state: &AppState, actor: &Actor) {
    let presenting = Presenting::read(&tls::dir_of(&state.data_dir));
    // Every state where the root to measure against is not one this leader presents itself, held on the value
    // rather than asked for again here.
    if !presenting.may_withdraw() {
        return;
    }
    // The same list the spend holds an enrollment code to, so an enrollment code this leader would accept is never one
    // it withdraws -- including the one it mints for the worker inside its own process, whose refusal stops this
    // leader.
    let Some(may_have_named) = presenting.may_have_named() else {
        return;
    };
    let may_have_named: Vec<String> = may_have_named.into_iter().map(str::to_string).collect();
    let now_ms = warnings::now_millis();
    let actor = actor.clone();
    let withdrawn = run_store(state.store.clone(), move |s| {
        let may: Vec<&str> = may_have_named.iter().map(String::as_str).collect();
        s.withdraw_enrollment_codes_not_naming(&may, now_ms, &|id| {
            super::accounts::event_for(&actor, "enrollment-code-withdrawn", id, "ok")
        })
    })
    .await;
    match withdrawn {
        Ok(ids) if ids.is_empty() => {}
        Ok(ids) => tracing::info!(
            target: "leader.enrollment_codes_withdrawn_on_cutover",
            codes = %ids.join(", "),
            "these enrollment codes named the authority this leader rotated away from, and were withdrawn: a worker \
             given one would pin a root nothing here presents"
        ),
        Err(e) => tracing::warn!(
            target: "leader.enrollment_codes_not_withdrawn",
            error = %e,
            "this leader rotated away from an authority and could not withdraw the enrollment codes naming it, so they \
             are still outstanding and bring nobody in"
        ),
    }
}

/// `DELETE /api/enrollment-codes/:id`: stop it bringing in anybody else.
pub async fn withdraw(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path(id): Path<String>,
) -> Json<Envelope> {
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let event = super::accounts::event_for(&actor, "enrollment-code-withdrawn", &id, "ok");
    // One transaction, like minting: an enrollment code that is gone with nobody recorded as having withdrawn it is the
    // same hole in the audit read from the other end.
    let result = run_store(state.store, move |s| s.withdraw_enrollment_code_recorded(&id, &event)).await;
    match result {
        Ok(true) => Json(Envelope::done()),
        Ok(false) => Json(Envelope::failed_one("that enrollment code is already gone")),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{Presenting, Standing};

    const SIGNING: &str = "the root signed under";
    const COMING: &str = "the root being moved to";
    const LEFT: &str = "a root left behind";

    /// One answer, for every surface that speaks about an enrollment code. Three states because three exist: an
    /// enrollment code naming the root being left works until the cutover, which is neither of the other two.
    #[test]
    fn an_enrollment_code_is_worth_one_of_three_things_against_what_this_leader_presents() {
        let rotating = Presenting::Own {
            signing: SIGNING.to_string(),
            coming: COMING.to_string(),
        };
        assert_eq!(rotating.standing_of(COMING), Standing::Good);
        assert_eq!(rotating.standing_of(SIGNING), Standing::UntilCutover);
        assert_eq!(rotating.standing_of(LEFT), Standing::Stale);

        let settled = Presenting::Own {
            signing: SIGNING.to_string(),
            coming: SIGNING.to_string(),
        };
        assert_eq!(settled.standing_of(SIGNING), Standing::Good);
        assert_eq!(
            settled.standing_of(LEFT),
            Standing::Stale,
            "nothing is being replaced, so nothing has a window"
        );
        assert!(!settled.rotating());
        assert!(rotating.rotating(), "the two roots differ and nothing else says so");
    }

    /// What this leader mints an enrollment code against is something it will accept, in every state it can answer for.
    /// An enrollment code it refuses the moment it is spent is one whose refusal stops the process the worker runs in.
    #[test]
    fn what_this_leader_mints_against_is_something_it_accepts() {
        for presenting in [
            Presenting::Own {
                signing: SIGNING.to_string(),
                coming: COMING.to_string(),
            },
            Presenting::Own {
                signing: SIGNING.to_string(),
                coming: SIGNING.to_string(),
            },
            Presenting::Theirs {
                signing: SIGNING.to_string(),
            },
            Presenting::NothingOfItsOwn,
        ] {
            let mints_against = presenting.mints_against();
            assert!(
                presenting
                    .may_have_named()
                    .is_some_and(|may| may.contains(&mints_against)),
                "{presenting:?} mints against {mints_against:?} and would refuse it"
            );
            assert_eq!(presenting.standing_of(mints_against), Standing::Good, "{presenting:?}");
        }
    }

    /// The worker inside this process is minted an enrollment code naming no root, and a rotation can end either way
    /// while that enrollment code is unspent. Whichever way it ends, the enrollment code has to survive: the refusal is
    /// terminal, and the thread that takes it stops this leader rather than just itself.
    #[test]
    fn the_enrollment_code_for_the_worker_in_this_process_survives_either_end_of_a_rotation() {
        let rotating = Presenting::Own {
            signing: SIGNING.to_string(),
            coming: COMING.to_string(),
        };
        let minted = rotating.mints_against().to_string();

        for ended in [
            // Cut over: the root being moved to is the one signed under now.
            Presenting::Own {
                signing: COMING.to_string(),
                coming: COMING.to_string(),
            },
            // Abandoned: the root being moved from is kept.
            Presenting::Own {
                signing: SIGNING.to_string(),
                coming: SIGNING.to_string(),
            },
        ] {
            assert_eq!(ended.standing_of(&minted), Standing::Good, "{ended:?}");
        }
    }

    /// The three states in which the root to measure against is not one this leader is presenting. They are not
    /// one state: what an enrollment code is worth differs between them, and so does whether this leader may act on it.
    #[test]
    fn what_is_not_this_leaders_to_judge_is_told_apart_from_what_is() {
        // Behind a proxy, or on plain HTTP. Any root an enrollment code pins is one no handshake here presents -- but
        // this leader may be mid-migration, and a withdrawal is not something an operator can undo.
        let none = Presenting::NothingOfItsOwn;
        assert_eq!(none.standing_of(SIGNING), Standing::Stale);
        assert_eq!(none.may_have_named(), Some(vec![""]));
        assert!(!none.may_withdraw());

        // An operator's certificate. An enrollment code naming another root brings nobody in here either, so enrollment
        // holds it to theirs -- but which of their roots one should name is theirs to say.
        let theirs = Presenting::Theirs {
            signing: SIGNING.to_string(),
        };
        assert_eq!(theirs.standing_of(SIGNING), Standing::Good);
        assert_eq!(theirs.standing_of(LEFT), Standing::Stale);
        assert_eq!(
            theirs.may_have_named(),
            Some(vec![SIGNING, ""]),
            "an enrollment code naming no root reaches an operator's certificate wherever the platform's roots do, and \
             fails the handshake before it gets here where they do not"
        );
        assert!(!theirs.may_withdraw());

        // Nothing read. What an enrollment code is worth is undecided, so a page does not send an operator to replace
        // one over a fault that is not the enrollment code's -- but spending one is an act, and there is nothing to
        // hold it to.
        let unreadable = Presenting::Unreadable;
        assert_eq!(unreadable.standing_of(LEFT), Standing::Good);
        assert_eq!(
            unreadable.may_have_named(),
            None,
            "an enrollment would be let in against no check at all"
        );
        assert!(!unreadable.may_withdraw());

        assert!(
            Presenting::Own {
                signing: SIGNING.to_string(),
                coming: SIGNING.to_string(),
            }
            .may_withdraw()
        );
    }

    fn source(file: &str) -> String {
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(file))
            .unwrap_or_else(|e| panic!("{file}: {e}"))
    }

    /// The deadline's cutover is covered by
    /// `a_cutover_withdraws_the_enrollment_codes_that_named_the_authority_it_retired`, which drives a running leader.
    /// The operator's button is in the binary, behind a password and a socket, and there is no harness that reaches it
    /// -- so what holds that path to this one is read here instead.
    ///
    /// It is the same kind of check as `warnings::tests::the_page_labels_every_condition`: a contract across a
    /// boundary no compiler crosses, kept by reading the other side.
    #[test]
    fn the_button_that_cuts_over_withdraws_the_enrollment_codes_too() {
        let handler = source("src/bin/smeltr/certificates_api.rs");
        let cut_over = handler
            .split("async fn cut_over(")
            .nth(1)
            .expect("a handler that cuts over");
        let body = cut_over.split("\nasync fn ").next().unwrap();
        assert!(
            body.contains("tls::cut_over("),
            "this no longer reads the handler that cuts over"
        );
        assert!(
            body.contains("enrollment_codes::withdraw_off_authority("),
            "the button cuts over without withdrawing the enrollment codes that named the authority it retired, which \
             the deadline does"
        );
    }

    /// A cutover changes the files and then withdraws the enrollment codes, and a process that stops in between comes
    /// back to a finished cutover with no rotation left to notice it. The start is where that is settled, and the start
    /// is in the binary too.
    #[test]
    fn a_start_settles_a_cutover_whose_withdrawal_never_ran() {
        assert!(
            source("src/bin/smeltr/main.rs").contains("enrollment_codes::withdraw_off_authority("),
            "a leader starts without settling the enrollment codes a cutover it did not finish left outstanding"
        );
    }
}
