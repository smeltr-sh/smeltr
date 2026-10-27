//! The warning catalog (design doc, Control plane): a fixed set of named conditions, each about one object, each
//! derived fresh from `Registry::snapshot()`, the worker roster, and which groups the fleet runs, on every call.
//! There is no warning table --
//! [`derive`] is the whole mechanism, and a condition absent from its result is not true, not closed or acknowledged.
//!
//! A warning needs a start time this module cannot read off any one heartbeat: nothing on the wire says how long a
//! fact has held. [`crate::leader::registry::Registry::observe_facts`] is the leader's only memory of that -- when
//! each currently-true fact was first seen -- and it is not itself a warning: no severity, no sentence, nothing to
//! acknowledge, and an entry is forgotten the instant its fact is.

use crate::handoff::BlockCause;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use serde::Serialize;

use crate::heartbeat::{MetricValueDto, ThreadKind};
use crate::leader::access::{Access, Level};
use crate::leader::deploy_checks::{FailingTest, Findings};
use crate::leader::registry::{FactKey, ThreadHealth};
use crate::leader::series::{DRIFT_WINDOW, DeclaredField, LoggedSite, RegistryStore, Tier};
use crate::leader::store::{KeyStanding, Unopenable, WorkerCredentialRecord};
use crate::leader::throughput;

/// *Failed*: the thing is not doing its job. *Warning*: it is working and something is off. (design doc, Control
/// plane). *Info*: a fact worth knowing that asks nothing of anyone, which only a caller that asks for it is given
/// (`leader::catalog`), so no view that judges health can count one as trouble.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Severity {
    Failed,
    Warning,
    Info,
}

/// The kind of object a warning's subject is (design doc, Control plane: "this source, this destination, this
/// worker, this group"). A fleet or group view rolls warnings up by subject; that rollup is a query the caller runs,
/// not a fifth
/// kind of warning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SubjectKind {
    Source,
    Destination,
    Worker,
    Group,
    /// This leader itself. The one subject nothing reports on, because it is what the reports arrive at.
    Leader,
    /// An outstanding enrollment code, by its id.
    EnrollmentCode,
    /// A pipeline, by the id its author gave it.
    Pipeline,
    /// A leader-stored secret, by name.
    Secret,
    /// A certificate in the leader's list, by name.
    Certificate,
}

/// Which catalog entry raised this warning. The catalog is fixed (design doc, Control plane), so a reader can
/// switch on it and a new entry is a compile error rather than a blank label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Condition {
    WorkerSilent,
    WorkerNeverHeartbeated,
    SourceBlocked,
    SourceRefused,
    SourceWaiting,
    SourceUnreachable,
    DestinationRefusing,
    DestinationUnreachable,
    LagClimbing,
    SourceRowsBelow,
    SourceRowsAbove,
    GroupNotDeployed,
    WorkerNotDeployingItsTarget,
    WorkerOnAnotherBuild,
    WorkerOnAnotherRelease,
    WorkerMemoryHigh,
    AuthorityExpiring,
    AuthorityAging,
    EnrollmentCodeNamesOutgoingAuthority,
    CertificateExpiring,
    WorkerLoggedWarnings,
    DeployHeld,
    RemoteNotFollowed,
    NewerRelease,
    DeployDoesNotLoad,
    DeployFailsTests,
    DeclaredFieldFailing,
    DeclaredFieldGone,
    StreamUnrouted,
    SecretsKeyUnusable,
    SecretNeedsValue,
    AcmeRenewalFailing,
    LeaderCertificateRunningOut,
    IssuedCertificateNotRenewed,
}

impl Condition {
    /// The whole catalog, so the page's copy of it can be checked against this one. The reader that switches on
    /// a condition is in another language, where a missing entry is a blank label rather than a compile error.
    pub const ALL: &'static [Condition] = &[
        Condition::WorkerSilent,
        Condition::WorkerNeverHeartbeated,
        Condition::SourceBlocked,
        Condition::SourceRefused,
        Condition::SourceWaiting,
        Condition::SourceUnreachable,
        Condition::DestinationRefusing,
        Condition::DestinationUnreachable,
        Condition::LagClimbing,
        Condition::SourceRowsBelow,
        Condition::SourceRowsAbove,
        Condition::GroupNotDeployed,
        Condition::WorkerNotDeployingItsTarget,
        Condition::WorkerOnAnotherBuild,
        Condition::WorkerOnAnotherRelease,
        Condition::WorkerMemoryHigh,
        Condition::AuthorityExpiring,
        Condition::AuthorityAging,
        Condition::CertificateExpiring,
        Condition::EnrollmentCodeNamesOutgoingAuthority,
        Condition::WorkerLoggedWarnings,
        Condition::DeployHeld,
        Condition::RemoteNotFollowed,
        Condition::NewerRelease,
        Condition::DeployDoesNotLoad,
        Condition::DeployFailsTests,
        Condition::DeclaredFieldFailing,
        Condition::DeclaredFieldGone,
        Condition::StreamUnrouted,
        Condition::SecretsKeyUnusable,
        Condition::SecretNeedsValue,
        Condition::AcmeRenewalFailing,
        Condition::LeaderCertificateRunningOut,
        Condition::IssuedCertificateNotRenewed,
    ];

    /// What this is called on the wire. The match is what makes a new entry a compile error here.
    pub fn name(self) -> &'static str {
        match self {
            Condition::WorkerSilent => "workerSilent",
            Condition::WorkerNeverHeartbeated => "workerNeverHeartbeated",
            Condition::SourceBlocked => "sourceBlocked",
            Condition::SourceRefused => "sourceRefused",
            Condition::SourceWaiting => "sourceWaiting",
            Condition::SourceUnreachable => "sourceUnreachable",
            Condition::DestinationRefusing => "destinationRefusing",
            Condition::DestinationUnreachable => "destinationUnreachable",
            Condition::LagClimbing => "lagClimbing",
            Condition::SourceRowsBelow => "sourceRowsBelow",
            Condition::SourceRowsAbove => "sourceRowsAbove",
            Condition::GroupNotDeployed => "groupNotDeployed",
            Condition::WorkerNotDeployingItsTarget => "workerNotDeployingItsTarget",
            Condition::WorkerOnAnotherBuild => "workerOnAnotherBuild",
            Condition::WorkerOnAnotherRelease => "workerOnAnotherRelease",
            Condition::WorkerMemoryHigh => "workerMemoryHigh",
            Condition::AuthorityExpiring => "authorityExpiring",
            Condition::AuthorityAging => "authorityAging",
            Condition::CertificateExpiring => "certificateExpiring",
            Condition::EnrollmentCodeNamesOutgoingAuthority => "enrollmentCodeNamesOutgoingAuthority",
            Condition::WorkerLoggedWarnings => "workerLoggedWarnings",
            Condition::DeployHeld => "deployHeld",
            Condition::RemoteNotFollowed => "remoteNotFollowed",
            Condition::NewerRelease => "newerRelease",
            Condition::DeployDoesNotLoad => "deployDoesNotLoad",
            Condition::DeployFailsTests => "deployFailsTests",
            Condition::DeclaredFieldFailing => "declaredFieldFailing",
            Condition::DeclaredFieldGone => "declaredFieldGone",
            Condition::StreamUnrouted => "streamUnrouted",
            Condition::SecretsKeyUnusable => "secretsKeyUnusable",
            Condition::SecretNeedsValue => "secretNeedsValue",
            Condition::AcmeRenewalFailing => "acmeRenewalFailing",
            Condition::LeaderCertificateRunningOut => "leaderCertificateRunningOut",
            Condition::IssuedCertificateNotRenewed => "issuedCertificateNotRenewed",
        }
    }
}

/// Where an operator manages what this leader serves and the authority it signs under, as a message names it. One
/// name in every message, so a renamed page is renamed everywhere.
const LEADER_CERTIFICATE_TAB: &str = "Admin → System → Leader certificate";

/// Where certificates are kept and replaced, as a message names it.
const CERTIFICATE_LIST: &str = "Admin → Certificates";

/// How long before an authority expires this starts saying so. Long enough to copy a new one to every worker at
/// a normal pace, which is the work it is warning about.
const AUTHORITY_NOTICE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// The last part of an authority's life, after which replacing it is work worth starting.
///
/// Proportional, because how long is long enough is proportional: a rotation waits for every worker to come round,
/// and a fleet that heartbeats every few seconds is done in minutes while one behind a weekly maintenance window
/// is not. A tenth of a decade is a year, which is the pace this is for.
const AUTHORITY_LAST_PART: f64 = 0.1;

/// How long before a certificate good from `not_before_millis` to `not_after_millis` runs out it is said to be
/// running out: the last part of its life. The GUI marks one by the same rule (`time.ts`).
pub fn notice_millis(not_before_millis: i64, not_after_millis: i64) -> i64 {
    ((not_after_millis - not_before_millis) as f64 * AUTHORITY_LAST_PART) as i64
}

/// A named condition about one object: what the design calls a warning. [`derive`] recomputes the whole list on
/// every call; nothing here is stored, and there is nothing to acknowledge.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Warning {
    /// The catalog entry. Its name is what a reader scans; the message says what to do about it.
    pub condition: Condition,
    pub subject_kind: SubjectKind,
    /// Unique only within `group` (design doc, Control plane: "a group is the addressing unit") -- a worker's own id
    /// is the
    /// exception, since credentials are issued fleet-wide.
    pub subject_id: String,
    /// Which group this is about. `None` when there is none to report: an enrolled worker that has never
    /// heartbeated has never said which group it belongs to.
    pub group: Option<String>,
    pub severity: Severity,
    /// Epoch milliseconds since this warning's condition began. `None` when that cannot be known: an enrolled
    /// worker that has never heartbeated may have just enrolled, or have been quiet since before the leader's last
    /// restart -- the registry cannot tell the two apart (design doc, Control plane).
    pub since_millis: Option<i64>,
    /// Names the object and the fix. Stands alone -- a reader sees this sentence with no other context.
    pub message: String,
    /// The log site a [`Condition::WorkerLoggedWarnings`] is about. One worker raises that condition once per site,
    /// so condition and subject alone do not name one warning; with this they do. `None` on every other condition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    /// The scope whose bundle declares the subject: `shared`, or the group. Set only on a pipeline, which the page
    /// links to its editor, and only where the deployed commit says.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

impl Warning {
    /// Whether `access` may see this warning: viewer in the group it names. One that names no group is about the
    /// leader itself -- its authority, its licence, its remote, a worker not yet placed in a group -- which only an
    /// administrator can act on.
    pub fn visible_to(&self, access: &Access) -> bool {
        match &self.group {
            Some(group) => access.allows(group, Level::Viewer),
            None => access.is_administrator(),
        }
    }
}

const SOURCE_BLOCKED: &str = "source_blocked";
const SOURCE_REFUSED: &str = "source_refused";
const SOURCE_WAITING: &str = "source_waiting";
const SOURCE_UNREACHABLE: &str = "source_unreachable";
const DESTINATION_REFUSING: &str = "destination_refusing";
const DESTINATION_UNREACHABLE: &str = "destination_unreachable";
const SOURCE_ROWS_BELOW: &str = "source_rows_below";
const SOURCE_ROWS_ABOVE: &str = "source_rows_above";
const GROUP_NOT_DEPLOYED: &str = "group_not_deployed";
const DEPLOY_DOES_NOT_LOAD: &str = "deploy_does_not_load";
const DEPLOY_FAILS_TESTS: &str = "deploy_fails_tests";

/// A source blocked on `cause`: its condition, its severity, and its fact-clock entry -- each cause its own, so a
/// source whose block changes cause is dated from the change.
fn block_condition(cause: BlockCause) -> (Condition, Severity, &'static str) {
    match cause {
        BlockCause::Held => (Condition::SourceBlocked, Severity::Failed, SOURCE_BLOCKED),
        BlockCause::Refused => (Condition::SourceRefused, Severity::Failed, SOURCE_REFUSED),
        // It clears on its own, and the destination it waits on has a warning of its own.
        BlockCause::Waiting => (Condition::SourceWaiting, Severity::Warning, SOURCE_WAITING),
    }
}

/// The wire name `Reader::blocked_fact` gives a blocked source's reason.
const SOURCE_BLOCKED_REASON: &str = "smeltr_source_blocked_reason";
/// The wire name `Reader::connection_error_fact` gives a source's most recent client-connectivity error (E4).
const SOURCE_CONNECTION_ERROR: &str = "smeltr_source_connection_error";
/// The wire name `RefusalRing::heartbeat_facts` gives one refusal, worded as the destination gave it.
const DESTINATION_LAST_REFUSAL: &str = "smeltr_destination_last_refusal";
/// The wire name `egress::note_stall`/`clear_stall` give a destination's own connectivity: a gauge, 1.0 while it is
/// accepting writes and 0.0 from the moment a send fails until one next succeeds -- the destination-side mirror of
/// `SOURCE_CONNECTION_ERROR` (E4), though this one carries no reason text, only whether.
const DESTINATION_HEALTHY: &str = "smeltr_destination_healthy";
/// The wire name `Reader::lag` gauges, per source, topic and partition (E6). Only a source kind that overrides
/// `Source::lag` ever reports it, so a source that cannot is simply absent from this metric rather than reporting
/// zero.
const SOURCE_LAG: &str = "smeltr_source_lag";
/// Messages a queue-backed source has finished, which is what drains its lag.
const MESSAGES_FINISHED: &str = "smeltr_source_messages_finished_total";
/// The wire name `egress::note_kept` gauges a destination's refused rows with, kept to send again.
const DESTINATION_HELD_ROWS: &str = "smeltr_destination_held_rows";
/// A fact last evidenced longer ago than this no longer counts as current: long enough that one delayed heartbeat --
/// `HeartbeatClient::send` already retries a failed one once -- does not flap a warning, short enough that a real
/// outage is caught within a few intervals.
const STALE_AFTER_INTERVALS: u32 = 3;

/// How old a worker's last heartbeat may be before what it said stops counting as current. Shared with
/// `stats::queue_fills`, which marks a quiet worker's queue stale for the same reason a warning stops being
/// evidenced: nobody has said it is still true.
pub(super) fn stale_after_ms(heartbeat_interval: Duration) -> i64 {
    (heartbeat_interval * STALE_AFTER_INTERVALS).as_millis() as i64
}

/// Consecutive rising lag samples [`lag_climbing`] requires before it raises, read at [`Tier::TenSeconds`] --
/// `io::kafka::LAG_SAMPLE_INTERVAL` paces a real source's lag reporting to at most one fresh value every ten
/// seconds, so one bucket is ordinarily one sample. A restart's lag is one upward step -- the broker kept moving
/// while the reader was gone, and the very next sample falls as it starts working the backlog off. Four samples is
/// three of those steps in a row, at least thirty seconds of a backlog that kept getting worse after the jump --
/// well past where ordinary catch-up would have turned the trend back down.
const LAG_CLIMB_SAMPLES: usize = 4;

/// How far behind a climbing partition must be before [`lag_climbing`] raises, in time rather than records: a
/// thousand records is days of a quiet source and a millisecond of a busy one.
const LAG_WARN_BEHIND: Duration = Duration::from_secs(60);

/// How far back [`recent_logs`] reads a worker's logs for something to look at. Arbitrary, like
/// [`STALE_AFTER_INTERVALS`]'s window: long enough that a poll landing between two heartbeats still catches
/// a warning logged just before it, short enough that a warning clears soon after the worker stops logging it.
const WORKER_LOG_WINDOW_MS: i64 = crate::logs::WARNING_FADES_AFTER.as_millis() as i64;

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(super) fn label<'a>(labels: &'a [(String, String)], key: &str) -> Option<&'a str> {
    labels.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// Workers whose copy of the leader's authority runs out soon, or already has.
///
/// About the copy each worker holds rather than the file on the leader, because that is what its verification
/// uses: one given an authority long ago is the one that goes quiet while the leader looks healthy. Expiry is
/// worth saying early -- replacing an authority means reaching every worker, which is the slowest thing an
/// operator has to do here.
///
/// It goes quiet unevenly, which is why this is a warning rather than something to notice later: rustls does not
/// read a root's expiry at all and OpenSSL does, so a worker keeps heartbeating while its `git fetch` stops, and
/// the fleet reports healthy having quietly stopped taking configuration.
fn authority_expiring(snapshot: &[ThreadHealth], credentials: &[WorkerCredentialRecord], now_ms: i64) -> Vec<Warning> {
    // A revoked worker keeps its last snapshot in the registry and can never heartbeat again, so nothing it could
    // be told to do would clear this. Telling an operator to go and update a worker they decommissioned is a
    // warning that stays on the page forever.
    let revoked: HashSet<&str> = credentials
        .iter()
        .filter(|c| c.revoked)
        .map(|c| c.worker_id.as_str())
        .collect();

    let notice = AUTHORITY_NOTICE.as_millis() as i64;
    // What this worker said most recently, not the least of what it ever said. The registry never evicts, so a
    // thread a deployment removed keeps its last report forever -- and with it the authority that worker held
    // before it was given a new one. Taking the minimum lets that retired entry outvote every live one, and the
    // warning it raises can never be cleared by the worker it names.
    let mut latest: HashMap<&str, (u64, Option<i64>, Option<String>)> = HashMap::new();
    for t in snapshot {
        if revoked.contains(t.worker_id.as_str()) {
            continue;
        }
        // Reports carrying no expiry are held with the rest. Skipping them here would leave the most recent thing
        // this worker said out of the comparison, so a retired thread's older `Some` would go on raising a warning
        // the worker's own live reports say nothing about and cannot clear.
        let held = latest.entry(&t.worker_id).or_insert((u64::MAX, None, None));
        if t.last_seen_millis_ago <= held.0 {
            *held = (
                t.last_seen_millis_ago,
                t.authority_expires_millis,
                Some(t.report.group_id.clone()).filter(|g| !g.is_empty()),
            );
        }
    }
    // A worker whose most recent report names no expiry is one saying nothing about the authority it holds.
    let soonest: HashMap<&str, (i64, Option<String>)> = latest
        .into_iter()
        .filter_map(|(id, (_, expires, group))| expires.map(|expires| (id, (expires, group))))
        .collect();

    let mut out: Vec<Warning> = soonest
        .into_iter()
        .filter(|(_, (expires, _))| *expires - now_ms <= notice)
        .map(|(worker_id, (expires, group))| {
            let expired = expires <= now_ms;
            Warning {
                scope: None,
                condition: Condition::AuthorityExpiring,
                subject_kind: SubjectKind::Worker,
                site: None,
                subject_id: worker_id.to_string(),
                group,
                severity: if expired { Severity::Failed } else { Severity::Warning },
                // Exactly when the condition began, for once: an expiry is a date, not something first noticed.
                since_millis: Some(expires - notice),
                message: if expired {
                    format!(
                        "worker {worker_id} holds a certificate authority that has expired; it can no longer fetch \
                         configuration from the leader. Copy the leader's current ca.pem to it"
                    )
                } else {
                    let days = (expires - now_ms) / (24 * 60 * 60 * 1000);
                    format!(
                        "worker {worker_id} holds a certificate authority that expires in {days} days; copy the \
                         leader's current ca.pem to it before then"
                    )
                },
            }
        })
        .collect();
    out.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    out
}

/// Which side of a connector's TLS a certificate sits on. The two run out differently, so they are said differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Half {
    /// The chain this worker presents. Out of date, the connector fails at the handshake.
    Presented,
    /// An authority a far end is verified against. Out of date, a far end chaining only to it cannot be checked;
    /// one chaining to another root the connector still trusts is unaffected.
    Trusted,
}

/// One certificate a deployed connector's TLS carries, and the window it is good for.
///
/// The leader reads these off the commit the fleet runs, with the chain a presented certificate's list entry holds
/// now: a bundle nobody deploys cannot stop anything working. A group mid-rollout runs more than one commit at
/// once, and each carries its own answer -- a worker on one never has another commit's roots in its trust store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployedCertificate {
    pub group: String,
    /// The commit this certificate was read off. Two commits are never one trust store: a worker still on the
    /// older one has only its own roots to verify against.
    pub commit: String,
    /// The connector whose TLS carries it.
    pub subject_kind: SubjectKind,
    pub subject_id: String,
    pub half: Half,
    pub not_before_millis: i64,
    pub not_after_millis: i64,
}

/// A deployed connector's certificates running out, in time to replace them.
///
/// A date rather than something observed, like the two authority entries: the leader reads it off the bundle, so
/// the warning knows exactly when it began and needs nothing remembered. Notice is a tenth of that certificate's
/// own life.
///
/// One warning per connector per half, because a connector holds more than one certificate and they do not fail
/// together. A presented chain is only as good as its soonest part. A trust store holds several roots on purpose --
/// during a far end's own rollover the outgoing one expires sooner and matters to nobody -- so trust runs out when
/// the last of them does, not the first.
fn certificate_expiring(deployed: &[DeployedCertificate], now_ms: i64) -> Vec<Warning> {
    // Commit is part of the key: two commits are two separate trust stores, one per worker generation, and merging
    // them would let a fresh root on one hide an exhausted one on the other. Kind is part of it because a source
    // and a destination may share an id.
    let mut decisive: HashMap<(&str, &str, SubjectKind, &str, Half), &DeployedCertificate> = HashMap::new();
    for certificate in deployed {
        let key = (
            certificate.group.as_str(),
            certificate.commit.as_str(),
            certificate.subject_kind,
            certificate.subject_id.as_str(),
            certificate.half,
        );
        let held = decisive.entry(key).or_insert(certificate);
        let decides = match certificate.half {
            Half::Presented => certificate.not_after_millis < held.not_after_millis,
            // A trust store's roots are alternatives, not a chain -- any one of them currently valid is enough to
            // verify a far end. A pre-staged rollover root, not valid yet, must never outrank one already good:
            // outranking it here is what reported a connector failed while its current root still worked.
            Half::Trusted => {
                let held_valid = now_ms >= held.not_before_millis;
                let candidate_valid = now_ms >= certificate.not_before_millis;
                match (candidate_valid, held_valid) {
                    (true, false) => true,
                    (false, true) => false,
                    _ => certificate.not_after_millis > held.not_after_millis,
                }
            }
        };
        if decides {
            *held = certificate;
        }
    }

    // A presented chain is only as good as its soonest-expiring member for a countdown, but for validity it is
    // only as good as its latest-starting one: every member has to hold right now, unlike a trust store's roots,
    // which are alternatives. `decisive` above tracks the first; this tracks the second, since the two can name
    // different members of the same chain.
    let mut presented_starts: HashMap<(&str, &str, SubjectKind, &str), i64> = HashMap::new();
    for certificate in deployed.iter().filter(|c| c.half == Half::Presented) {
        let key = (
            certificate.group.as_str(),
            certificate.commit.as_str(),
            certificate.subject_kind,
            certificate.subject_id.as_str(),
        );
        let start = presented_starts.entry(key).or_insert(certificate.not_before_millis);
        *start = (*start).max(certificate.not_before_millis);
    }

    let mut out: Vec<Warning> = decisive
        .into_values()
        .filter_map(|certificate| {
            let notice = notice_millis(certificate.not_before_millis, certificate.not_after_millis);
            let remaining = certificate.not_after_millis - now_ms;
            // Not yet valid is a current failure, not a countdown, so it is reported whatever the notice window
            // says about a `not_after` still years off: a TLS peer rejects it today regardless of when it expires.
            let not_yet_valid = match certificate.half {
                // The decisive member's own start is not enough: a chain's soonest-expiring member can be valid
                // today while a differently-chosen intermediate is not, and path validation rejects the chain
                // either way.
                Half::Presented => {
                    let key = (
                        certificate.group.as_str(),
                        certificate.commit.as_str(),
                        certificate.subject_kind,
                        certificate.subject_id.as_str(),
                    );
                    presented_starts.get(&key).is_some_and(|&start| now_ms < start)
                }
                Half::Trusted => now_ms < certificate.not_before_millis,
            };
            if !not_yet_valid && remaining > notice {
                return None;
            }
            let expired = remaining <= 0;
            let days = remaining / (24 * 60 * 60 * 1000);
            let what = match certificate.subject_kind {
                SubjectKind::Source => "source",
                _ => "destination",
            };
            let id = &certificate.subject_id;
            Some(Warning {
                scope: None,
                condition: Condition::CertificateExpiring,
                subject_kind: certificate.subject_kind,
                site: None,
                subject_id: id.clone(),
                group: Some(certificate.group.clone()),
                severity: if not_yet_valid || expired {
                    Severity::Failed
                } else {
                    Severity::Warning
                },
                // Clamped to the certificate's own start: one shorter-lived than the notice window would otherwise
                // read as having warned before it existed.
                since_millis: Some(certificate.not_after_millis - notice),
                message: match (certificate.half, not_yet_valid, expired) {
                    (Half::Presented, true, _) => format!(
                        "{what} {id:?} uses a certificate that is not valid yet, so it fails at the handshake \
                         until it is. Check its start date in {CERTIFICATE_LIST}"
                    ),
                    (Half::Presented, _, true) => format!(
                        "{what} {id:?} uses a certificate that has expired, so it fails at the handshake. \
                         Replace it in {CERTIFICATE_LIST}"
                    ),
                    (Half::Presented, false, false) => format!(
                        "{what} {id:?} uses a certificate that expires in {days} days; replace it in \
                         {CERTIFICATE_LIST} before then"
                    ),
                    (Half::Trusted, true, _) => format!(
                        "the authority {what} {id:?} trusts is not valid yet, so nothing can be verified against it \
                         until it is. Check its start date in the {what}'s authorities"
                    ),
                    (Half::Trusted, _, true) => format!(
                        "every authority {what} {id:?} trusts has expired, so nothing can be verified against any \
                         of them. Add their replacements to the {what}'s authorities"
                    ),
                    (Half::Trusted, false, false) => format!(
                        "the last authority {what} {id:?} trusts expires in {days} days, and the {what} stops \
                         verifying then. Add its replacement to the {what}'s authorities"
                    ),
                },
            })
        })
        .collect();
    // Deterministic: a map's order is not, and a poll that reordered its own answer reads as the fleet changing.
    out.sort_by(|a, b| (&a.group, &a.subject_id, &a.message).cmp(&(&b.group, &b.subject_id, &b.message)));
    // An ordinary rollout has both commits still carrying the same certificate for a connector neither is
    // changing, and each commit's own decisive entry says the same thing about it -- keeping the two apart above
    // is what a differing pair needs, but an identical pair is one fact, not two.
    out.dedup();
    out
}

/// This leader's own authority running out, in time to do something about it.
///
/// Where this leader is with replacing its authority, for a warning that has to name a remedy an operator can
/// carry out.
#[derive(Debug)]
pub enum Rotating {
    /// None open. Starting one is the remedy.
    None,
    /// One open and able to finish. The answer is already coming.
    UnderWay,
    /// One open that cannot finish, and why. Starting another is refused until it is abandoned.
    Stuck(String),
    /// This leader could not read whether one is open, and why not. Not "none open": starting one is refused on
    /// the same fault, so offering it sends an operator to a page that turns them away.
    Unreadable(String),
}

/// About the root this leader signs under, which is the one that stops working: the file may hold an outgoing root
/// beside it that expires sooner and matters to nobody. Separate from [`authority_expiring`], which is about the
/// copy a worker holds -- a worker left behind by a replacement this warning is what prompts.
///
/// Silent while a rotation is under way, because the answer is already coming and a warning nobody can clear is
/// one an operator learns to ignore. A rotation that cannot finish is not an answer coming, so it says so -- and
/// says what to do about it, since starting another is refused while this one is open.
fn authority_aging(signing: Option<&crate::leader::tls::Described>, rotating: &Rotating, now_ms: i64) -> Vec<Warning> {
    let Some(authority) = signing.filter(|_| !matches!(rotating, Rotating::UnderWay)) else {
        return Vec::new();
    };
    let notice = notice_millis(authority.not_before_millis, authority.not_after_millis);
    let remaining = authority.not_after_millis - now_ms;
    if remaining > notice {
        return Vec::new();
    }
    let expired = remaining <= 0;
    let days = remaining / (24 * 60 * 60 * 1000);
    vec![Warning {
        scope: None,
        condition: Condition::AuthorityAging,
        subject_kind: SubjectKind::Leader,
        site: None,
        subject_id: authority.fingerprint.clone(),
        group: None,
        severity: if expired { Severity::Failed } else { Severity::Warning },
        // A date, not something first noticed, like the other authority entry.
        since_millis: Some(authority.not_after_millis - notice),
        message: match (expired, rotating) {
            // The remedy an operator can actually carry out. Starting a rotation is refused while one is open, so
            // naming that would send them to a page that turns them away.
            (_, Rotating::Stuck(why) | Rotating::Unreadable(why)) => format!(
                "this leader's certificate authority {} and the rotation meant to replace it cannot finish: {why} \
                 Put that right, or abandon the rotation in {LEADER_CERTIFICATE_TAB} and start another",
                if expired {
                    "has expired".to_string()
                } else {
                    format!("expires in {days} days")
                }
            ),
            (true, _) => format!(
                "this leader's certificate authority has expired, and no worker given it can verify this leader any \
                 more. Rotate it in {LEADER_CERTIFICATE_TAB}"
            ),
            (false, _) => format!(
                "this leader's certificate authority expires in {days} days. Rotate it in {LEADER_CERTIFICATE_TAB}: \
                 every worker has to take the new one before this leader signs under it"
            ),
        },
    }]
}

/// The certificate this leader serves, as its warnings read it.
pub struct Serving {
    pub certificate: crate::leader::tls::Described,
    pub source: crate::leader::acme::Source,
    /// The entry in the certificate list it is a copy of, for an uploaded one.
    pub entry: Option<String>,
    /// What the last order left, for one ACME renews.
    pub acme: crate::leader::acme::State,
}

/// The certificate this leader serves running out. One ACME renews is said once a renewal is due and the last try
/// failed, which is two thirds of the way through its life rather than the last tenth: the leader has been trying,
/// and the operator has most of a third to put it right. Any other is said in its last tenth, by [`notice_millis`],
/// and so is an ACME one ACME has stopped trying to renew.
pub fn leader_certificate(serving: &Serving, now_ms: i64) -> Vec<Warning> {
    let certificate = &serving.certificate;
    let remaining = certificate.not_after_millis - now_ms;
    let expired = remaining <= 0;
    let when = if expired {
        "has expired".to_string()
    } else {
        format!("expires in {} days", remaining / (24 * 60 * 60 * 1000))
    };
    let acme = &serving.acme;
    let failing = match (
        acme.last_error.as_deref(),
        acme.renew_after_millis,
        acme.last_attempt_millis,
    ) {
        // Tried since it fell due: an error from before then, such as a Renew now that failed early, is not a
        // renewal failing.
        (Some(error), Some(due), Some(tried)) if tried >= due => Some((error, due)),
        _ => None,
    };
    let (condition, since_millis, message) = match (serving.source, failing) {
        (crate::leader::acme::Source::Acme, Some((error, due))) => {
            let error = one_line(error);
            // The error last, so a long one clipped costs nothing and nothing after it reads as the authority's.
            let message = if expired {
                format!(
                    "this leader could not renew its certificate, and the certificate has expired. Check the ACME \
                     settings in {LEADER_CERTIFICATE_TAB}. Last error: {error}"
                )
            } else {
                format!(
                    "this leader could not renew its certificate. It tries again every hour, and the certificate \
                     {when}. Check the ACME settings in {LEADER_CERTIFICATE_TAB}. Last error: {error}"
                )
            };
            (Condition::AcmeRenewalFailing, due, message)
        }
        (source, _) => {
            // An ACME certificate reaches its last tenth unrenewed and without a failure only where ACME stopped
            // trying: turned off, or its names no longer orderable.
            let notice = notice_millis(certificate.not_before_millis, certificate.not_after_millis);
            if remaining > notice {
                return Vec::new();
            }
            let message = match (source, &serving.entry) {
                (crate::leader::acme::Source::Acme, _) => format!(
                    "this leader's certificate {when}, and ACME is not renewing it. Check the ACME settings in {LEADER_CERTIFICATE_TAB}"
                ),
                (crate::leader::acme::Source::Upload, Some(entry)) => format!(
                    "this leader's certificate {entry:?} {when}. Replace it in {CERTIFICATE_LIST}, and the leader \
                     serves the new version"
                ),
                _ => {
                    format!("this leader's certificate {when}. Issue a new one in {LEADER_CERTIFICATE_TAB}")
                }
            };
            (
                Condition::LeaderCertificateRunningOut,
                certificate.not_after_millis - notice,
                message,
            )
        }
    };
    vec![Warning {
        scope: None,
        condition,
        subject_kind: SubjectKind::Leader,
        site: None,
        subject_id: certificate
            .names
            .first()
            .cloned()
            .unwrap_or_else(|| certificate.subject.clone()),
        group: None,
        severity: if expired { Severity::Failed } else { Severity::Warning },
        since_millis: Some(since_millis),
        message,
    }]
}

/// An authority's error as the end of a row: its first line, without a full stop, as no row ends in one, and short
/// enough to leave the row readable.
fn one_line(error: &str) -> String {
    const LONGEST: usize = 200;
    let line = error.lines().next().unwrap_or_default().trim().trim_end_matches('.');
    match line.char_indices().nth(LONGEST) {
        // At the last word that fits, so a clipped error does not end mid-word.
        Some((cut, _)) => format!(
            "{}…",
            line[..cut].rsplit_once(' ').map_or(&line[..cut], |(kept, _)| kept)
        ),
        None => line.to_string(),
    }
}

/// The outstanding enrollment codes that name a root this rotation is about to retire.
///
/// An enrollment code carries its root's fingerprint inside the value an operator pastes, so it cannot be re-pointed to
/// the incoming one: the only remedy is to spend it before the cutover, or mint another after. Said while the rotation
/// is open because that is the whole window in which either is possible -- the cutover withdraws these enrollment
/// codes, and afterward there is nothing left to warn about.
///
/// Only the enrollment codes a cutover is about to strand, which is what [`enrollment_codes::Standing::UntilCutover`]
/// means and the only state with a remedy. One naming a root already left behind brings nobody in now, and telling an
/// operator it has until the cutover to spend it would be a window that does not exist; the Enrollment codes page says
/// that one is replaced, off the same answer.
fn enrollment_code_names_outgoing_authority(
    codes: &[crate::leader::store::UsableEnrollmentCode],
    presenting: &crate::leader::enrollment_codes::Presenting,
) -> Vec<Warning> {
    use crate::leader::enrollment_codes::Standing;
    codes
        .iter()
        .filter(|code| presenting.standing_of(&code.fingerprint) == Standing::UntilCutover)
        .map(|code| Warning {
            scope: None,
            condition: Condition::EnrollmentCodeNamesOutgoingAuthority,
            subject_kind: SubjectKind::EnrollmentCode,
            site: None,
            subject_id: code.id.clone(),
            group: (!code.group_id.is_empty()).then(|| code.group_id.clone()),
            severity: Severity::Warning,
            // The rotation's own start, which the Leader certificate tab already shows against the rotation itself.
            // Repeating it on every enrollment code it dooms would be one date said many times and acted on once.
            since_millis: None,
            message:
                "this enrollment code names the certificate authority this leader is rotating away from, so it is \
                 withdrawn when the rotation finishes and brings nobody in after that. Spend it before then, or mint \
                 another once the rotation is over -- minting is refused while one is open."
                    .to_string(),
        })
        .collect()
}

/// A series subject key (`registry::subject_key`'s output, opaque everywhere else) decoded back into labels, for a
/// message that has to name what the key only encodes.
fn subject_labels(subject_key: &str) -> Option<Vec<(String, String)>> {
    serde_json::from_str(subject_key).ok()
}

/// A fact-clock subject naming both the group and the object within it, so two groups' facts about equally-named
/// objects (design doc, Control plane: "a group is the addressing unit") are tracked -- and timed -- independently.
/// Opaque:
/// built here, compared only as a [`FactKey`], never decoded.
fn scoped_subject(group: &str, object_id: &str) -> String {
    serde_json::to_string(&(group, object_id)).unwrap_or_else(|_| format!("{group}\u{0}{object_id}"))
}

/// Every source currently blocked, keyed by (group, source id, cause), with the reason its reader gave -- already a
/// complete sentence naming the fix (`runtime::block_reason`, `runtime::undeliverable`) -- and the workers blocked on
/// that cause. A source id is unique only within its group plus `shared/`, never globally, so the group is part of the
/// key: two groups running a source of the same id are two different facts, and a `shared/` source blocked in one
/// group is not blocked in another that also runs it. A source that spans workers can be blocked on a different
/// cause on each, and each cause is its own fact, since each is cleared its own way. When more than one worker
/// gives the same cause, the lexicographically first `(worker, thread)` gives the reason: an arbitrary but
/// deterministic tie-break, since the registry gives no other order to prefer.
fn blocked_sources(snapshot: &[ThreadHealth]) -> HashMap<(String, String, BlockCause), (String, HashSet<String>)> {
    let mut threads: Vec<&ThreadHealth> = snapshot.iter().collect();
    threads.sort_by(|a, b| (&a.worker_id, &a.report.thread_id).cmp(&(&b.worker_id, &b.report.thread_id)));

    let mut out: HashMap<_, (String, HashSet<String>)> = HashMap::new();
    for t in threads {
        for m in &t.report.metrics {
            if m.metric != SOURCE_BLOCKED_REASON {
                continue;
            }
            let Some(source_id) = label(&m.labels, "source") else {
                continue;
            };
            let MetricValueDto::Text(reason) = &m.value else {
                continue;
            };
            // A reader that names no cause, or one this build does not know, is blocked as it always was reported.
            let cause = label(&m.labels, "cause")
                .and_then(BlockCause::named)
                .unwrap_or(BlockCause::Held);
            out.entry((t.report.group_id.clone(), source_id.to_string(), cause))
                .or_insert_with(|| (reason.clone(), HashSet::new()))
                .1
                .insert(t.worker_id.clone());
        }
    }
    out
}

/// The workers of `expected`, in `group`, holding rows a capture of `source` or at `destination` is shown before any
/// other: a reader whose block keeps records, or an egress thread that keeps rows its destination refused, by a report
/// no older than `within`. A capture waits for each to report before its total ends it.
///
/// The registry keeps a thread's last report after the thread is gone, so an older one says nothing of now.
///
/// A source blocked on a destination holds nothing a capture of the source sees: its rows wait at the destination.
pub fn holding_for_capture(
    snapshot: &[ThreadHealth],
    group: &str,
    expected: &HashSet<String>,
    within: Duration,
    source: Option<&str>,
    destination: Option<&str>,
) -> HashSet<String> {
    let holds = |m: &crate::heartbeat::MetricSampleDto| match (&m.value, source, destination) {
        (MetricValueDto::Text(_), Some(source), _) if m.metric == SOURCE_BLOCKED_REASON => {
            label(&m.labels, "source") == Some(source)
                && label(&m.labels, "cause")
                    .and_then(BlockCause::named)
                    .is_none_or(|c| c == BlockCause::Held)
        }
        (MetricValueDto::Gauge(rows), _, Some(destination)) if m.metric == DESTINATION_HELD_ROWS => {
            label(&m.labels, "destination") == Some(destination) && *rows > 0.0
        }
        _ => false,
    };
    snapshot
        .iter()
        .filter(|t| t.report.group_id == group && expected.contains(&t.worker_id))
        .filter(|t| Duration::from_millis(t.last_seen_millis_ago) <= within)
        .filter(|t| t.report.metrics.iter().any(holds))
        .map(|t| t.worker_id.clone())
        .collect()
}

/// Every source whose consumer currently reports a client-connectivity error (E4), keyed by (group, source id),
/// with the error text as the source kind worded it. Same shape as [`blocked_sources`], including the group-scoped key
/// and the tie-break: this is a present-or-absent fact, not a history, so a source that has recovered since simply
/// stops reporting it.
fn unreachable_sources(snapshot: &[ThreadHealth]) -> HashMap<(String, String), String> {
    let mut threads: Vec<&ThreadHealth> = snapshot.iter().collect();
    threads.sort_by(|a, b| (&a.worker_id, &a.report.thread_id).cmp(&(&b.worker_id, &b.report.thread_id)));

    let mut out = HashMap::new();
    for t in threads {
        for m in &t.report.metrics {
            if m.metric != SOURCE_CONNECTION_ERROR {
                continue;
            }
            let Some(source_id) = label(&m.labels, "source") else {
                continue;
            };
            let MetricValueDto::Text(reason) = &m.value else {
                continue;
            };
            out.entry((t.report.group_id.clone(), source_id.to_string()))
                .or_insert_with(|| reason.clone());
        }
    }
    out
}

/// Every destination whose freshest report says it cannot currently be sent to at all -- `DESTINATION_HEALTHY` read
/// 0 last -- keyed by (group, destination id). Distinct from [`refusing_destinations`]: that is the destination
/// examining a record and rejecting it, this is failing to reach it in the first place. Unlike the present-or-absent
/// text facts above, this gauge is reported every heartbeat whether healthy or not, so the set is built from its
/// value rather than its mere presence -- a destination that has recovered reports 1.0 and simply leaves the set.
///
/// Threads past `stale_after` are excluded, the same bound [`running_deployments`] applies: the registry never
/// evicts a thread's row, so a worker that redeploys without this destination, moves groups, or simply stops --
/// distinct from `worker_silent`, which is about the *worker* -- leaves its last-reported 0.0 behind forever.
/// Without this, that frozen row would keep raising the warning and keep its fact-clock entry alive past the last
/// heartbeat that could possibly still mean it.
///
/// `report.kind` must still say `Egress`: unlike the text facts above, this gauge lives in the metrics registry,
/// and `worker::thread_reports` carries a registry's owed samples forward under their last thread id even once
/// that id is no longer running (`RuntimeReport::followed_by`) -- with no kind, since nothing running claims it.
/// Such a leftover arrives timestamped by a perfectly current heartbeat, so staleness alone would not catch it.
fn unreachable_destinations(snapshot: &[ThreadHealth], heartbeat_interval: Duration) -> HashSet<(String, String)> {
    let stale_after = stale_after_ms(heartbeat_interval);
    let mut out = HashSet::new();
    for t in snapshot {
        if t.last_seen_millis_ago as i64 > stale_after || t.report.kind != Some(ThreadKind::Egress) {
            continue;
        }
        for m in &t.report.metrics {
            if m.metric != DESTINATION_HEALTHY {
                continue;
            }
            let (Some(dest_id), MetricValueDto::Gauge(healthy)) = (label(&m.labels, "destination"), &m.value) else {
                continue;
            };
            if *healthy == 0.0 {
                out.insert((t.report.group_id.clone(), dest_id.to_string()));
            }
        }
    }
    out
}

/// Every destination currently refusing writes, keyed by (group, destination id), with how the cluster worded its
/// freshest refusal -- freshest across every thread of that group that reported one, since a destination's egress
/// thread differs per worker. Group-scoped for the same reason [`blocked_sources`] is: a destination id is unique only
/// within its group plus `shared/`. A refusal older than [`stale_after_ms`] is history: `RefusalRing` keeps up to
/// twenty of them forever, so only a recent one still means "refusing now" rather than "refused once".
fn refusing_destinations(
    snapshot: &[ThreadHealth],
    now_ms: i64,
    heartbeat_interval: Duration,
) -> HashMap<(String, String), String> {
    let stale_after = stale_after_ms(heartbeat_interval);
    let mut freshest: HashMap<(String, String), (i64, String)> = HashMap::new();

    for t in snapshot {
        for m in &t.report.metrics {
            if m.metric != DESTINATION_LAST_REFUSAL {
                continue;
            }
            let (Some(dest_id), Some(at)) = (label(&m.labels, "destination"), label(&m.labels, "at_millis")) else {
                continue;
            };
            let Ok(at_millis) = at.parse::<i64>() else { continue };
            if now_ms - at_millis > stale_after {
                continue;
            }
            let MetricValueDto::Text(message) = &m.value else {
                continue;
            };
            let key = (t.report.group_id.clone(), dest_id.to_string());
            freshest
                .entry(key)
                .and_modify(|(best_at, best_message)| {
                    if at_millis > *best_at {
                        *best_at = at_millis;
                        *best_message = message.clone();
                    }
                })
                .or_insert((at_millis, message.clone()));
        }
    }
    freshest.into_iter().map(|(key, (_, message))| (key, message)).collect()
}

/// Every worker in `group` with a thread currently reporting `metric` labeled `label_key` == `value` -- the
/// workers a fact is true for right now. The numerator [`spread_clause`] reads.
fn workers_reporting(
    snapshot: &[ThreadHealth],
    group: &str,
    metric: &str,
    label_key: &str,
    value: &str,
) -> HashSet<String> {
    snapshot
        .iter()
        .filter(|t| t.report.group_id == group)
        .filter(|t| {
            t.report
                .metrics
                .iter()
                .any(|m| m.metric == metric && label(&m.labels, label_key) == Some(value))
        })
        .map(|t| t.worker_id.clone())
        .collect()
}

/// Every worker in `group` with a thread reporting *any* metric labeled `label_key` == `value` -- what the object
/// is running on, whether or not a fact about it currently holds. A worker a fact is true for is always included
/// here too, since the fact's own metric carries the same label; this is the denominator [`spread_clause`] reads
/// against. Approximate in one direction only: a reader or egress thread reports many metrics under ordinary
/// operation (E4's own connectivity check among them), so a worker genuinely running the object and reporting
/// nothing about it at all in one heartbeat window is not expected.
fn workers_running(snapshot: &[ThreadHealth], group: &str, label_key: &str, value: &str) -> HashSet<String> {
    snapshot
        .iter()
        .filter(|t| t.report.group_id == group)
        .filter(|t| {
            t.report
                .metrics
                .iter()
                .any(|m| label(&m.labels, label_key) == Some(value))
        })
        .map(|t| t.worker_id.clone())
        .collect()
}

/// How widespread a source or destination fact is within its group (design brief, part C): every worker running the
/// object currently shows it, or only some do -- the pattern that says whether the cause is shared (the broker, the
/// destination) or sits on particular hosts.
fn spread_clause(reporting: usize, running: usize) -> String {
    let running = running.max(reporting).max(1);
    let (noun, verb) = if running == 1 {
        ("worker", "shows")
    } else {
        ("workers", "show")
    };
    if reporting >= running {
        format!("all {running} {noun} in this group {verb} it")
    } else {
        format!(
            "{reporting} of {running} {noun} in this group {verb} it, which points at those hosts rather than \
             something group-wide"
        )
    }
}

/// Every worker currently heartbeating, with the group it reports. A worker belongs to one group for the life of
/// its process, so its threads agree; the last one seen wins over the first, which matters only if that ever stops
/// being true.
fn worker_groups(snapshot: &[ThreadHealth]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for t in snapshot {
        out.insert(t.worker_id.clone(), t.report.group_id.clone());
    }
    out
}

/// Every worker heartbeating for a group the deployed commit holds no subtree for, keyed by worker id, with the group it
/// reports.
fn groups_not_deployed(snapshot: &[ThreadHealth], deployed_groups: &HashSet<String>) -> HashMap<String, String> {
    worker_groups(snapshot)
        .into_iter()
        .filter(|(_, group_id)| !deployed_groups.contains(group_id))
        .collect()
}

/// Every `(group, commit)` a worker is actually running right now, for [`super::deployed_certificates`].
///
/// A thread a heartbeat no longer reports keeps its row -- `record_heartbeat` has no sweep for one -- so a worker
/// that moved to a different group leaves its old group's row behind, its `commit` still refreshed by every later
/// heartbeat but its `group_id` frozen at the last one that thread actually ran under. Left in, that row would
/// invent a deployment nobody runs. Excluded the same way `worker_silent` excludes a row nothing recent backs: it
/// stops being fed a fresh `last_seen` the moment its thread drops out of a heartbeat, so it goes stale within a
/// few intervals like any other worker that stopped reporting -- and a revoked worker's is never current at all.
pub fn running_deployments(
    snapshot: &[ThreadHealth],
    credentials: &[WorkerCredentialRecord],
    heartbeat_interval: Duration,
) -> HashSet<(String, String)> {
    let revoked: HashSet<&str> = credentials
        .iter()
        .filter(|c| c.revoked)
        .map(|c| c.worker_id.as_str())
        .collect();
    let stale_after = stale_after_ms(heartbeat_interval);
    snapshot
        .iter()
        .filter(|t| !revoked.contains(t.worker_id.as_str()) && (t.last_seen_millis_ago as i64) <= stale_after)
        .filter_map(|t| t.commit.clone().map(|commit| (t.report.group_id.clone(), commit)))
        .collect()
}

/// Every fact a catalog entry needing a start time currently reads as true, as keys for
/// [`crate::leader::registry::Registry::observe_facts`]. Call this on every heartbeat (design doc, Control plane:
/// "recomputed
/// ... on every heartbeat"), so a warning's start time reflects when its fact began rather than when it was next
/// polled. `deployed_groups` is every group the deployed commit holds a subtree for; what is wrong with that
/// commit adds [`deploy_facts`].
pub fn currently_active_facts(
    snapshot: &[ThreadHealth],
    now_ms: i64,
    heartbeat_interval: Duration,
    deployed_groups: &HashSet<String>,
) -> HashSet<FactKey> {
    let mut active = HashSet::new();
    active.extend(
        blocked_sources(snapshot)
            .into_keys()
            .map(|(group, id, cause)| FactKey::new(block_condition(cause).2, scoped_subject(&group, &id))),
    );
    active.extend(
        unreachable_sources(snapshot)
            .into_keys()
            .map(|(group, id)| FactKey::new(SOURCE_UNREACHABLE, scoped_subject(&group, &id))),
    );
    active.extend(
        refusing_destinations(snapshot, now_ms, heartbeat_interval)
            .into_keys()
            .map(|(group, id)| FactKey::new(DESTINATION_REFUSING, scoped_subject(&group, &id))),
    );
    active.extend(
        unreachable_destinations(snapshot, heartbeat_interval)
            .into_iter()
            .map(|(group, id)| FactKey::new(DESTINATION_UNREACHABLE, scoped_subject(&group, &id))),
    );
    active.extend(
        groups_not_deployed(snapshot, deployed_groups)
            .into_keys()
            .map(|worker_id| FactKey::new(GROUP_NOT_DEPLOYED, worker_id)),
    );
    // Active from the first beat that names a commit the worker is not running, which is what makes "since when"
    // mean the deployment started rather than the warning did.
    active.extend(workers_behind_target(snapshot).map(|(worker_id, desired)| not_deploying_key(&worker_id, &desired)));
    active
}

/// The facts [`deploy_checked`] times, as [`currently_active_facts`] gives the rest. While the deployed commit's tests
/// are still running, every failing-test fact the clock holds is kept as it is: whether it still holds is not known
/// yet, and forgetting it would restart its clock once the run lands -- after every deploy and every restart.
pub fn deploy_facts(checks: &Findings, registry: &crate::leader::registry::Registry) -> Vec<FactKey> {
    let mut out: Vec<FactKey> = checks.not_loading.iter().map(|n| does_not_load_key(&n.group)).collect();
    out.extend(checks.failing.iter().map(|t| fails_tests_key(&t.scope, &t.id)));
    if checks.tests_pending() {
        out.extend(registry.facts_of(DEPLOY_FAILS_TESTS));
    }
    out
}

/// Keyed by group alone, so a fix that does not fix it carries on the same clock.
fn does_not_load_key(group: &str) -> FactKey {
    FactKey::new(DEPLOY_DOES_NOT_LOAD, group)
}

/// Keyed by test alone, so a deploy that leaves it failing carries on the same clock.
fn fails_tests_key(scope: &str, test: &str) -> FactKey {
    FactKey::new(DEPLOY_FAILS_TESTS, scoped_subject(scope, test))
}

/// Every worker whose supervisor has named a commit it is not running, read from that worker's freshest row.
///
/// The registry keeps the last row for every thread id a worker has ever reported, and a deployment that removes
/// a thread leaves its row behind saying whatever was true when it stopped reporting. Reading every row would let
/// one of those keep this fact alive after the worker itself had caught up, so only the newest speaks.
fn workers_behind_target(snapshot: &[ThreadHealth]) -> impl Iterator<Item = (String, String)> {
    freshest_per_worker(snapshot)
        .into_iter()
        .filter(|(_, t)| t.stopped_at_millis.is_none())
        .filter_map(|(worker_id, t)| {
            let supervising = t.supervising.as_ref()?;
            let desired = supervising.desired_commit.as_deref()?;
            not_running_as_found(t.commit.as_deref(), desired, supervising.failure.as_deref())
                .then(|| (worker_id.to_string(), desired.to_string()))
        })
        .collect::<Vec<_>>()
        .into_iter()
}

/// Whether a worker is not running `desired` as it found it: on another commit, or on it but refusing to restart
/// it, which a new version of a certificate it presents asks for. A worker refusing keeps running the old version
/// until it expires, so it is as stuck as one that never deployed.
fn not_running_as_found(running: Option<&str>, desired: &str, failure: Option<&str>) -> bool {
    running != Some(desired) || failure.is_some()
}

/// The clock this warning ages against, which is a worker's wait for one particular commit rather than its wait
/// in general. Keyed by worker alone, a target the worker has since been pointed away from would hand its start
/// time to the next one, and a replacement commit would be a failure the instant it was found -- with none of the
/// startup it is entitled to.
pub fn not_deploying_key(worker_id: &str, desired: &str) -> FactKey {
    FactKey::new(NOT_DEPLOYING, scoped_subject(worker_id, desired))
}

/// Every worker whose last word was that it is stopping. Its silence since is a stop it announced, and what that
/// last report says about deploying describes a worker that let go of its deployment on purpose.
fn stopped_on_notice(snapshot: &[ThreadHealth]) -> HashSet<&str> {
    freshest_per_worker(snapshot)
        .into_iter()
        .filter(|(_, t)| t.stopped_at_millis.is_some())
        .map(|(worker_id, _)| worker_id)
        .collect()
}

/// One row per worker: the one from its most recent heartbeat. Every thread of a worker reports in the same
/// request, so the smallest age is that request, and rows older than it belong to threads that have gone.
fn freshest_per_worker(snapshot: &[ThreadHealth]) -> HashMap<&str, &ThreadHealth> {
    let mut newest: HashMap<&str, &ThreadHealth> = HashMap::new();
    for t in snapshot {
        newest
            .entry(t.worker_id.as_str())
            .and_modify(|held| {
                if t.last_seen_millis_ago < held.last_seen_millis_ago {
                    *held = t;
                }
            })
            .or_insert(t);
    }
    newest
}

/// Every worker whose last heartbeat names a build the leader's is not compatible with (`version::compatible`). The
/// leader hands such a worker no deployment (`git_http`), so it keeps running whatever it had until one of the two
/// is upgraded or rolled back.
fn workers_on_another_build(
    snapshot: &[ThreadHealth],
    leader_commit: &str,
    leader_release: Option<&str>,
) -> Vec<Warning> {
    let mut out: Vec<Warning> = freshest_per_worker(snapshot)
        .into_iter()
        .filter(|(_, t)| t.stopped_at_millis.is_none())
        .filter(|(_, t)| {
            !crate::version::compatible(
                leader_commit,
                leader_release,
                crate::version::commit_of(&t.smeltr_version),
                t.release.as_deref(),
            )
        })
        .map(|(worker_id, t)| Warning {
            scope: None,
            condition: Condition::WorkerOnAnotherBuild,
            subject_kind: SubjectKind::Worker,
            site: None,
            subject_id: worker_id.to_string(),
            group: Some(t.report.group_id.clone()),
            severity: Severity::Warning,
            since_millis: None,
            message: format!(
                "worker {worker_id} is on {}, and this leader on {}. It keeps running what it has and takes nothing \
                 new until both are on one major release, or on one commit",
                crate::leader::build_name(crate::version::commit_of(&t.smeltr_version), t.release.as_deref()),
                crate::leader::build_name(leader_commit, leader_release),
            ),
        })
        .collect();
    out.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    out
}

/// Every worker on another release than the leader's that it is still compatible with: it takes deployments, and is
/// one upgrade from matching. Info, since nothing is wrong; [`workers_on_another_build`] covers the pairs that are not
/// compatible.
fn workers_on_another_release(
    snapshot: &[ThreadHealth],
    leader_commit: &str,
    leader_release: Option<&str>,
) -> Vec<Warning> {
    let Some(leader_release) = leader_release else {
        return Vec::new();
    };
    let mut out: Vec<Warning> = freshest_per_worker(snapshot)
        .into_iter()
        .filter(|(_, t)| t.stopped_at_millis.is_none())
        .filter_map(|(worker_id, t)| {
            let release = t.release.as_deref().filter(|r| *r != leader_release)?;
            let commit = crate::version::commit_of(&t.smeltr_version);
            crate::version::compatible(leader_commit, Some(leader_release), commit, Some(release)).then(|| Warning {
                scope: None,
                condition: Condition::WorkerOnAnotherRelease,
                subject_kind: SubjectKind::Worker,
                site: None,
                subject_id: worker_id.to_string(),
                group: Some(t.report.group_id.clone()),
                severity: Severity::Info,
                since_millis: None,
                message: format!("worker {worker_id} is on {release}, and this leader on {leader_release}"),
            })
        })
        .collect();
    out.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    out
}

/// The share of its memory limit, in percent, at which a worker is raised. Far enough below the limit that there is
/// time to act before the kernel kills it, and above what a worker steadily holding its buffers sits at.
const MEMORY_HIGH_PERCENT: u64 = 90;

/// Every worker whose last heartbeat reports resident memory at or above [`MEMORY_HIGH_PERCENT`] of its limit. One
/// that reported no limit has nothing to measure against, and one that has stopped or gone silent is reporting a
/// reading too old to act on -- [`worker_silent`] says what is wrong with that one.
fn workers_memory_high(snapshot: &[ThreadHealth], heartbeat_interval: Duration) -> Vec<Warning> {
    let stale_after = stale_after_ms(heartbeat_interval);
    let mut out: Vec<Warning> = freshest_per_worker(snapshot)
        .into_iter()
        .filter(|(_, t)| t.stopped_at_millis.is_none() && t.last_seen_millis_ago as i64 <= stale_after)
        .filter_map(|(worker_id, t)| {
            let resources = t.resources?;
            let limit = resources.memory_limit_bytes.filter(|l| *l > 0)?;
            let used = resources.memory_used_bytes;
            // Widened, so a limit near u64::MAX cannot overflow the comparison.
            (u128::from(used) * 100 >= u128::from(limit) * u128::from(MEMORY_HIGH_PERCENT)).then(|| Warning {
                scope: None,
                condition: Condition::WorkerMemoryHigh,
                subject_kind: SubjectKind::Worker,
                site: None,
                subject_id: worker_id.to_string(),
                group: Some(t.report.group_id.clone()),
                severity: Severity::Warning,
                since_millis: None,
                message: format!(
                    "worker {worker_id} is using {}% of its memory ({} of {}); give it more memory or run less on it \
                     before it runs out",
                    u128::from(used) * 100 / u128::from(limit),
                    humanize_bytes(used),
                    humanize_bytes(limit),
                ),
            })
        })
        .collect();
    out.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    out
}

/// A byte count in the largest binary unit that keeps it at one or more, to one decimal place.
fn humanize_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// A worker the registry has heard from before, but not recently, and that did not say it was stopping.
/// Leader-side by construction: a worker that has gone silent cannot be the one saying so. What it said before it
/// went is the one thing read here: a stop it announced is a stop, not a failure.
fn worker_silent(
    snapshot: &[ThreadHealth],
    credentials: &[WorkerCredentialRecord],
    now_ms: i64,
    heartbeat_interval: Duration,
) -> Vec<Warning> {
    let revoked: HashSet<&str> = credentials
        .iter()
        .filter(|c| c.revoked)
        .map(|c| c.worker_id.as_str())
        .collect();
    let stale_after = stale_after_ms(heartbeat_interval);
    let groups = worker_groups(snapshot);
    let stopped = stopped_on_notice(snapshot);

    // A worker's threads all land in one heartbeat request, so their ages agree; the minimum is the freshest
    // evidence this worker is still there, from any of them.
    let mut freshest_ago: HashMap<&str, u64> = HashMap::new();
    for t in snapshot {
        freshest_ago
            .entry(t.worker_id.as_str())
            .and_modify(|ago| *ago = (*ago).min(t.last_seen_millis_ago))
            .or_insert(t.last_seen_millis_ago);
    }

    let mut out: Vec<Warning> = freshest_ago
        .into_iter()
        .filter(|(worker_id, ago)| {
            !revoked.contains(worker_id) && !stopped.contains(worker_id) && *ago as i64 > stale_after
        })
        .map(|(worker_id, ago)| Warning {
            scope: None,
            condition: Condition::WorkerSilent,
            subject_kind: SubjectKind::Worker,
            site: None,
            subject_id: worker_id.to_string(),
            group: groups.get(worker_id).cloned(),
            severity: Severity::Failed,
            since_millis: Some(now_ms - ago as i64),
            message: format!(
                "worker {worker_id} has stopped sending heartbeats; check the process is running and can reach the \
                 leader"
            ),
        })
        .collect();
    out.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    out
}

/// A credential that has never produced a thread in the registry. The registry rebuilds from nothing on every
/// leader restart, so this also matches a worker that was heartbeating before the last one -- indistinguishable by
/// design (design doc, Control plane), which is why it carries no start time.
fn enrolled_never_heartbeating(snapshot: &[ThreadHealth], credentials: &[WorkerCredentialRecord]) -> Vec<Warning> {
    let known: HashSet<&str> = snapshot.iter().map(|t| t.worker_id.as_str()).collect();
    let mut out: Vec<Warning> = credentials
        .iter()
        .filter(|c| !c.revoked && !known.contains(c.worker_id.as_str()))
        .map(|c| Warning {
            scope: None,
            condition: Condition::WorkerNeverHeartbeated,
            subject_kind: SubjectKind::Worker,
            site: None,
            subject_id: c.worker_id.clone(),
            // Never heartbeated, so it has never said which group it belongs to.
            group: None,
            severity: Severity::Warning,
            since_millis: None,
            message: format!(
                "worker {} enrolled but has never sent a heartbeat; check it was started with this leader's URL and \
                 credential",
                c.worker_id
            ),
        })
        .collect();
    out.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    out
}

/// A source blocked, under the condition its cause names: records held until the config changes, rows a destination
/// refused, or a destination it waits on. The message is the reader's own reason, unchanged: it already names the fix
/// (`Reader::blocked_fact`).
fn source_blocked(snapshot: &[ThreadHealth], starts: &HashMap<FactKey, i64>) -> Vec<Warning> {
    let mut out: Vec<Warning> = blocked_sources(snapshot)
        .into_iter()
        .map(|((group, source_id, cause), (reason, blocked))| {
            let (condition, severity, entry) = block_condition(cause);
            let since_millis = starts
                .get(&FactKey::new(entry, scoped_subject(&group, &source_id)))
                .copied();
            let reporting = blocked.len();
            let running = workers_running(snapshot, &group, "source", &source_id).len();
            Warning {
                scope: None,
                condition,
                subject_kind: SubjectKind::Source,
                site: None,
                subject_id: source_id,
                group: Some(group),
                severity,
                since_millis,
                message: format!("{reason}; {}", spread_clause(reporting, running)),
            }
        })
        .collect();
    out.sort_by(|a, b| {
        (
            a.group.as_deref().unwrap_or(""),
            a.subject_id.as_str(),
            a.condition.name(),
        )
            .cmp(&(
                b.group.as_deref().unwrap_or(""),
                b.subject_id.as_str(),
                b.condition.name(),
            ))
    });
    out
}

/// A source whose consumer cannot reach its broker (E4): distinct from `SourceBlocked`, which is a downstream
/// destination refusing -- this is the source's own connection. `Failed`, not `Warning`: a source that cannot be
/// read is not delivering data at all, the same standing `SourceBlocked` gets. The fix suggestion is generic (the
/// source kind's own reason is quoted first) because the underlying causes share one remedy: check what the source
/// is configured to reach, and whether the network between here and it agrees.
fn source_unreachable(snapshot: &[ThreadHealth], starts: &HashMap<FactKey, i64>) -> Vec<Warning> {
    let mut out: Vec<Warning> = unreachable_sources(snapshot)
        .into_iter()
        .map(|((group, source_id), reason)| {
            let since_millis = starts
                .get(&FactKey::new(SOURCE_UNREACHABLE, scoped_subject(&group, &source_id)))
                .copied();
            let reporting = workers_reporting(snapshot, &group, SOURCE_CONNECTION_ERROR, "source", &source_id).len();
            let running = workers_running(snapshot, &group, "source", &source_id).len();
            Warning {
                scope: None,
                condition: Condition::SourceUnreachable,
                subject_kind: SubjectKind::Source,
                site: None,
                subject_id: source_id.clone(),
                group: Some(group),
                severity: Severity::Failed,
                since_millis,
                message: format!(
                    "source {source_id} cannot reach its broker: {reason}; check the broker address is correct, \
                     that it resolves, and that nothing between here and it (a firewall, a security group) is \
                     blocking the connection; {}",
                    spread_clause(reporting, running)
                ),
            }
        })
        .collect();
    out.sort_by(|a, b| {
        (a.group.as_deref().unwrap_or(""), a.subject_id.as_str())
            .cmp(&(b.group.as_deref().unwrap_or(""), b.subject_id.as_str()))
    });
    out
}

/// Every worker in `group` whose freshest refusal of `dest_id` is still within `stale_after` -- the same freshness
/// test [`refusing_destinations`] applies, but per worker rather than fleet-wide: `DESTINATION_LAST_REFUSAL` is a
/// ring of history and stays present long after a refusal has stopped being current, so counting its bare presence
/// (as [`workers_reporting`] does for the present-or-absent facts) would count a worker that recovered days ago.
fn workers_refusing(
    snapshot: &[ThreadHealth],
    group: &str,
    dest_id: &str,
    now_ms: i64,
    stale_after: i64,
) -> HashSet<String> {
    snapshot
        .iter()
        .filter(|t| t.report.group_id == group)
        .filter(|t| {
            t.report.metrics.iter().any(|m| {
                m.metric == DESTINATION_LAST_REFUSAL
                    && label(&m.labels, "destination") == Some(dest_id)
                    && label(&m.labels, "at_millis")
                        .and_then(|s| s.parse::<i64>().ok())
                        .is_some_and(|at| now_ms - at <= stale_after)
            })
        })
        .map(|t| t.worker_id.clone())
        .collect()
}

/// A destination whose most recent refusal is still fresh -- refusing now, not merely refused at some point in its
/// kept history (`RefusalRing`).
fn destination_refusing(
    snapshot: &[ThreadHealth],
    starts: &HashMap<FactKey, i64>,
    now_ms: i64,
    heartbeat_interval: Duration,
) -> Vec<Warning> {
    let stale_after = stale_after_ms(heartbeat_interval);
    let mut out: Vec<Warning> = refusing_destinations(snapshot, now_ms, heartbeat_interval)
        .into_iter()
        .map(|((group, dest_id), cluster_message)| {
            let since_millis = starts
                .get(&FactKey::new(DESTINATION_REFUSING, scoped_subject(&group, &dest_id)))
                .copied();
            let reporting = workers_refusing(snapshot, &group, &dest_id, now_ms, stale_after).len();
            let running = workers_running(snapshot, &group, "destination", &dest_id).len();
            Warning {
                scope: None,
                condition: Condition::DestinationRefusing,
                subject_kind: SubjectKind::Destination,
                site: None,
                subject_id: dest_id.clone(),
                group: Some(group),
                severity: Severity::Failed,
                since_millis,
                message: format!(
                    "destination {dest_id} is refusing records: {cluster_message}; fix the destination's schema or \
                     the pipeline that shapes the record; {}",
                    spread_clause(reporting, running)
                ),
            }
        })
        .collect();
    out.sort_by(|a, b| {
        (a.group.as_deref().unwrap_or(""), a.subject_id.as_str())
            .cmp(&(b.group.as_deref().unwrap_or(""), b.subject_id.as_str()))
    });
    out
}

/// Every worker in `group` whose freshest report of `dest_id`'s `DESTINATION_HEALTHY` gauge is 0 -- unlike
/// [`workers_refusing`], this reads the gauge's value rather than a ring's freshness, since a healthy egress thread
/// keeps reporting the same metric at 1.0 rather than falling silent. `stale_after` and the `Egress` check exclude
/// a thread's row the same two ways [`unreachable_destinations`] does: a frozen row, or a leftover carried forward
/// under a thread id nothing currently runs, is not this worker's current say.
fn workers_destination_unhealthy(
    snapshot: &[ThreadHealth],
    group: &str,
    dest_id: &str,
    stale_after: i64,
) -> HashSet<String> {
    snapshot
        .iter()
        .filter(|t| {
            t.report.group_id == group
                && (t.last_seen_millis_ago as i64) <= stale_after
                && t.report.kind == Some(ThreadKind::Egress)
        })
        .filter(|t| {
            t.report.metrics.iter().any(|m| {
                m.metric == DESTINATION_HEALTHY
                    && label(&m.labels, "destination") == Some(dest_id)
                    && matches!(m.value, MetricValueDto::Gauge(v) if v == 0.0)
            })
        })
        .map(|t| t.worker_id.clone())
        .collect()
}

/// Every worker in `group` currently running an egress thread for `dest_id` at all -- the denominator
/// [`destination_unreachable`]'s spread clause reads, so "1 of 2" means two workers *currently* run this
/// destination, not two that once did. Bespoke rather than the shared `workers_running`, which every other
/// condition's spread clause uses unfiltered: those read text facts a dead thread cannot go on reporting, where
/// this reads a registry gauge that can (see `unreachable_destinations`).
fn workers_running_destination(
    snapshot: &[ThreadHealth],
    group: &str,
    dest_id: &str,
    stale_after: i64,
) -> HashSet<String> {
    snapshot
        .iter()
        .filter(|t| {
            t.report.group_id == group
                && (t.last_seen_millis_ago as i64) <= stale_after
                && t.report.kind == Some(ThreadKind::Egress)
        })
        .filter(|t| {
            t.report
                .metrics
                .iter()
                .any(|m| label(&m.labels, "destination") == Some(dest_id))
        })
        .map(|t| t.worker_id.clone())
        .collect()
}

/// A destination no worker running it can currently reach at all: distinct from [`destination_refusing`], which is
/// the destination examining a record and rejecting it -- this is failing the connection before any record is sent,
/// `egress`'s own mirror of `SourceUnreachable`. `Failed`, the same standing `DestinationRefusing` gets: a
/// destination nothing can reach is not receiving anything.
///
/// Quotes no reason of its own: `DESTINATION_HEALTHY` is a bare gauge, not a text fact like
/// `SOURCE_CONNECTION_ERROR` -- `egress::note_stall` sees the specific error but only logs it, and
/// [`fold_logged_details`] tells the latest of those on this warning.
fn destination_unreachable(
    snapshot: &[ThreadHealth],
    starts: &HashMap<FactKey, i64>,
    heartbeat_interval: Duration,
) -> Vec<Warning> {
    let stale_after = stale_after_ms(heartbeat_interval);
    let mut out: Vec<Warning> = unreachable_destinations(snapshot, heartbeat_interval)
        .into_iter()
        .map(|(group, dest_id)| {
            let since_millis = starts
                .get(&FactKey::new(DESTINATION_UNREACHABLE, scoped_subject(&group, &dest_id)))
                .copied();
            let reporting = workers_destination_unhealthy(snapshot, &group, &dest_id, stale_after).len();
            let running = workers_running_destination(snapshot, &group, &dest_id, stale_after).len();
            Warning {
                scope: None,
                condition: Condition::DestinationUnreachable,
                subject_kind: SubjectKind::Destination,
                site: None,
                subject_id: dest_id.clone(),
                group: Some(group),
                severity: Severity::Failed,
                since_millis,
                message: format!(
                    "destination {dest_id} cannot be reached: its egress connection has failed; check the broker \
                     or cluster is up, reachable from the worker, and that its credentials are still valid; {}",
                    spread_clause(reporting, running)
                ),
            }
        })
        .collect();
    out.sort_by(|a, b| {
        (a.group.as_deref().unwrap_or(""), a.subject_id.as_str())
            .cmp(&(b.group.as_deref().unwrap_or(""), b.subject_id.as_str()))
    });
    out
}

/// A worker heartbeating for a group the deployed commit holds no subtree for. The worker itself is fine -- it is running the
/// empty deployment a commit with no subtree for its group always means (`leader::tree::load_group_at`) -- so this
/// is the silence that condition leaves: nothing else says the group was never approved.
fn group_not_deployed(
    snapshot: &[ThreadHealth],
    deployed_groups: &HashSet<String>,
    starts: &HashMap<FactKey, i64>,
) -> Vec<Warning> {
    let mut out: Vec<Warning> = groups_not_deployed(snapshot, deployed_groups)
        .into_iter()
        .map(|(worker_id, group_id)| {
            let since_millis = starts
                .get(&FactKey::new(GROUP_NOT_DEPLOYED, worker_id.clone()))
                .copied();
            Warning { scope: None,
                condition: Condition::GroupNotDeployed,
                subject_kind: SubjectKind::Worker,
                site: None,
                subject_id: worker_id,
                group: Some(group_id.clone()),
                severity: Severity::Warning,
                since_millis,
                message: format!(
                    "worker is heartbeating for group {group_id:?}, but what is deployed holds no configuration for it; \
                     configure and deploy a change that adds groups/{group_id}/system.json, or move the worker to \
                     a group that exists"
                ),
            }
        })
        .collect();
    out.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    out
}

/// The longest run of strictly rising values at the end of `points` (oldest first): its length, and the bucket it
/// starts at. A flat or falling last step is a run of one -- just the last point, nothing before it agrees with.
fn trailing_climb(points: &[(i64, f64)]) -> (usize, i64) {
    let mut start = points.len() - 1;
    while start > 0 && points[start].1 > points[start - 1].1 {
        start -= 1;
    }
    (points.len() - start, points[start].0)
}

/// A delay of at least [`LAG_WARN_BEHIND`] in the largest whole unit that reads naturally.
fn humanize_secs(secs: u64) -> String {
    match secs {
        ..120 => format!("{secs} s"),
        120..7_200 => format!("{} min", secs / 60),
        7_200..172_800 => format!("{} h", secs / 3_600),
        _ => format!("{} days", secs / 86_400),
    }
}

/// `metric`, a count each source reports, per ten-second bucket, keyed by (group, source). A store that cannot answer
/// reads as nothing counted, which leaves a climbing partition to warn rather than go quiet.
fn per_source(store: &RegistryStore, metric: &str) -> HashMap<(String, String), Vec<(i64, f64)>> {
    let mut out: HashMap<(String, String), Vec<(i64, f64)>> = HashMap::new();
    for (key, points) in store.series_since(metric, Tier::TenSeconds, 0).unwrap_or_default() {
        let Some(labels) = subject_labels(&key) else { continue };
        if let (Some(group), Some(source)) = (label(&labels, "group"), label(&labels, "source")) {
            out.entry((group.to_string(), source.to_string()))
                .or_default()
                .extend(points);
        }
    }
    out
}

/// Every source reading outside its normal (`throughput`) that nothing else explains. A source held by a destination,
/// one that cannot reach what it reads, and one whose worker has gone silent each read less than usual, and each
/// already has a warning that names the cause. A source no current worker runs is not judged: what it read is
/// history.
fn outside_normal<'a>(
    readings: &'a [throughput::Reading],
    snapshot: &[ThreadHealth],
    recovering: &HashSet<(String, String)>,
    heartbeat_interval: Duration,
) -> Vec<(&'a throughput::Reading, throughput::Outside)> {
    let blocked: HashSet<(String, String)> = blocked_sources(snapshot)
        .into_keys()
        .map(|(group, id, _)| (group, id))
        .chain(unreachable_sources(snapshot).into_keys())
        .chain(recovering.iter().cloned())
        .collect();
    let stale_after = stale_after_ms(heartbeat_interval);
    readings
        .iter()
        .filter(|r| !blocked.contains(&(r.group.clone(), r.source.clone())))
        .filter(|r| {
            let running: Vec<&ThreadHealth> = snapshot
                .iter()
                .filter(|t| t.report.group_id == r.group)
                .filter(|t| {
                    t.report
                        .metrics
                        .iter()
                        .any(|m| label(&m.labels, "source") == Some(&r.source))
                })
                .collect();
            !running.is_empty() && running.iter().all(|t| t.last_seen_millis_ago as i64 <= stale_after)
        })
        .filter_map(|r| r.outside().map(|side| (r, side)))
        .collect()
}

/// What stops a source reading, so that the window after one ends still holds the dip it made and the catch-up after.
const STOPS_READING: &[&str] = &[SOURCE_BLOCKED, SOURCE_REFUSED, SOURCE_WAITING, SOURCE_UNREACHABLE];

/// Every (group, source) one of [`STOPS_READING`] ended for within the last [`throughput::WINDOW`]: its window is
/// still the stop's, not its normal's. None when the store cannot say.
pub fn recovering_sources(store: Option<&RegistryStore>, now_ms: i64) -> HashSet<(String, String)> {
    let Some(store) = store else {
        return HashSet::new();
    };
    match store.facts_ended_since(STOPS_READING, now_ms - throughput::WINDOW.as_millis() as i64) {
        Ok(subjects) => subjects
            .iter()
            .filter_map(|subject| serde_json::from_str::<(String, String)>(subject).ok())
            .collect(),
        Err(e) => {
            tracing::warn!(
                target: "throughput.recovery_unreadable",
                error = %e,
                "could not read which sources just recovered, so one may warn on the dip it is recovering from"
            );
            HashSet::new()
        }
    }
}

fn outside_key(reading: &throughput::Reading, side: throughput::Outside) -> FactKey {
    let fact = match side {
        throughput::Outside::Below => SOURCE_ROWS_BELOW,
        throughput::Outside::Above => SOURCE_ROWS_ABOVE,
    };
    FactKey::new(fact, scoped_subject(&reading.group, &reading.source))
}

/// The facts [`outside_normal_warnings`] times, as [`currently_active_facts`] gives the rest.
pub fn outside_normal_facts(
    readings: &[throughput::Reading],
    snapshot: &[ThreadHealth],
    recovering: &HashSet<(String, String)>,
    heartbeat_interval: Duration,
) -> Vec<FactKey> {
    outside_normal(readings, snapshot, recovering, heartbeat_interval)
        .into_iter()
        .map(|(r, side)| outside_key(r, side))
        .collect()
}

/// A source reading fewer rows than it normally does at this time of day, or more. A warning either way: it is
/// reading, and how much is off.
pub fn outside_normal_warnings(
    readings: &[throughput::Reading],
    snapshot: &[ThreadHealth],
    recovering: &HashSet<(String, String)>,
    starts: &HashMap<FactKey, i64>,
    heartbeat_interval: Duration,
) -> Vec<Warning> {
    outside_normal(readings, snapshot, recovering, heartbeat_interval)
        .into_iter()
        .map(|(r, side)| {
            let condition = match side {
                throughput::Outside::Below => Condition::SourceRowsBelow,
                throughput::Outside::Above => Condition::SourceRowsAbove,
            };
            Warning {
                scope: None,
                condition,
                subject_kind: SubjectKind::Source,
                site: None,
                subject_id: r.source.clone(),
                group: Some(r.group.clone()),
                severity: Severity::Warning,
                since_millis: starts.get(&outside_key(r, side)).copied(),
                message: format!(
                    "source {}: {} rows in {} minutes, normal at this time of day is {} to {}",
                    r.source,
                    throughput::grouped(r.rows),
                    throughput::WINDOW.as_secs() / 60,
                    throughput::grouped(r.band.low),
                    throughput::grouped(r.band.high),
                ),
            }
        })
        .collect()
}

/// When one warning about a source or destination held, for a chart to mark.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WarningSpan {
    pub condition: Condition,
    pub severity: Severity,
    pub started_millis: i64,
    /// `None` while it still holds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_millis: Option<i64>,
}

/// The warnings a source's chart marks, by the fact each is kept under.
const SOURCE_SPANS: &[(&str, Condition, Severity)] = &[
    (SOURCE_BLOCKED, Condition::SourceBlocked, Severity::Failed),
    (SOURCE_REFUSED, Condition::SourceRefused, Severity::Failed),
    (SOURCE_WAITING, Condition::SourceWaiting, Severity::Warning),
    (SOURCE_UNREACHABLE, Condition::SourceUnreachable, Severity::Failed),
    (SOURCE_ROWS_BELOW, Condition::SourceRowsBelow, Severity::Warning),
    (SOURCE_ROWS_ABOVE, Condition::SourceRowsAbove, Severity::Warning),
];

/// The warnings a destination's chart marks, by the fact each is kept under.
const DESTINATION_SPANS: &[(&str, Condition, Severity)] = &[
    (DESTINATION_REFUSING, Condition::DestinationRefusing, Severity::Failed),
    (
        DESTINATION_UNREACHABLE,
        Condition::DestinationUnreachable,
        Severity::Failed,
    ),
];

/// Every warning about `group`'s source or destination `id` that held at any time since `since_ms`, oldest first.
pub fn spans(
    store: &RegistryStore,
    kind: SubjectKind,
    group: &str,
    id: &str,
    since_ms: i64,
) -> Result<Vec<WarningSpan>, crate::leader::store::StoreError> {
    let table = match kind {
        SubjectKind::Source => SOURCE_SPANS,
        SubjectKind::Destination => DESTINATION_SPANS,
        _ => return Ok(Vec::new()),
    };
    let entries: Vec<&str> = table.iter().map(|(entry, _, _)| *entry).collect();
    Ok(store
        .fact_spans(&entries, &scoped_subject(group, id), since_ms)?
        .into_iter()
        .filter_map(|(entry, started_millis, ended_millis)| {
            let (_, condition, severity) = table.iter().find(|(e, _, _)| *e == entry)?;
            Some(WarningSpan {
                condition: *condition,
                severity: *severity,
                started_millis,
                ended_millis,
            })
        })
        .collect())
}

/// Every (group, source, topic, partition) whose lag has strictly risen for [`LAG_CLIMB_SAMPLES`] straight samples
/// (E6) -- a trend, which a single heartbeat cannot show, so this reads the series rather than `snapshot`. `None`
/// `store` (no durable registry behind this leader; see [`crate::leader::registry::Registry::series`]) answers with
/// nothing: there is no history yet to judge a trend against.
///
/// Not a fact-clock entry: unlike `source_blocked`/`destination_refusing`, the series already carries real timestamps,
/// so `since_millis` is the run's own first bucket rather than something a separate clock has to remember.
fn lag_climbing(store: Option<&RegistryStore>) -> Vec<Warning> {
    let Some(store) = store else { return Vec::new() };
    let series = match store.series_since(SOURCE_LAG, Tier::TenSeconds, 0) {
        Ok(series) => series,
        Err(e) => {
            tracing::warn!(
                target: "warnings.lag_series_unreadable",
                error = %e,
                "could not read the lag series, so no lag warnings are derived this pass"
            );
            return Vec::new();
        }
    };

    let mut out: Vec<Warning> = Vec::new();
    // Counted over the pass rather than reported per subject: an unparseable key is a shape this reader does not
    // know, and the store holds every other key of that shape too.
    let (mut unreadable, mut incomplete) = (0usize, 0usize);
    // A count says how much; one key says what shape, which is the difference between a format this reader has
    // not learned and a row that is corrupt.
    let (mut unreadable_key, mut incomplete_key) = (None, None);
    // Lag is drained at the rate of what it counts: a queue's messages, which it counts as finished, else records,
    // which are rows read.
    let read = per_source(store, super::ROWS_IN_METRIC);
    let finished = per_source(store, MESSAGES_FINISHED);
    // Rows read are counted per source, lag per partition, so a partition's share of the reading is taken as even.
    let mut partitions: HashMap<(String, String), usize> = HashMap::new();
    for labels in series.iter().filter_map(|(key, _)| subject_labels(key)) {
        if let (Some(group), Some(source)) = (label(&labels, "group"), label(&labels, "source")) {
            *partitions.entry((group.to_string(), source.to_string())).or_default() += 1;
        }
    }
    for (subject_key, points) in series {
        if points.len() < LAG_CLIMB_SAMPLES {
            continue; // too few samples to judge a trend
        }
        let (run_len, since_millis) = trailing_climb(&points);
        if run_len < LAG_CLIMB_SAMPLES {
            continue; // rose, but not for long enough in a row
        }
        let Some(labels) = subject_labels(&subject_key) else {
            unreadable += 1;
            unreadable_key.get_or_insert_with(|| subject_key.clone());
            continue;
        };
        // `registry::subject_key` folds the reporting group into every key it builds, and no release has stored a
        // row without it, so it is required rather than merely preferred.
        let (Some(group), Some(source_id), Some(topic), Some(partition)) = (
            label(&labels, "group"),
            label(&labels, "source"),
            label(&labels, "topic"),
            label(&labels, "partition"),
        ) else {
            incomplete += 1;
            incomplete_key.get_or_insert_with(|| subject_key.clone());
            continue;
        };
        let first_lag = points[points.len() - run_len].1 as i64;
        let last_lag = points[points.len() - 1].1 as i64;
        let key = (group.to_string(), source_id.to_string());
        let span_ms = points[points.len() - 1].0 + Tier::TenSeconds.bucket_ms() - since_millis;
        let queue = finished.contains_key(&key);
        let drained = (if queue { &finished } else { &read })
            .get(&key)
            .map_or(0.0, |buckets| {
                buckets
                    .iter()
                    .filter(|(at, _)| *at >= since_millis)
                    .map(|(_, n)| n)
                    .sum()
            });
        let per_second = drained / (span_ms as f64 / 1000.0) / partitions.get(&key).copied().unwrap_or(1) as f64;
        // Nothing read while lag climbs is a reader that has stopped, as far behind as it gets.
        let behind = if per_second > 0.0 {
            last_lag as f64 / per_second
        } else {
            f64::INFINITY
        };
        if behind < LAG_WARN_BEHIND.as_secs_f64() {
            continue; // climbing, but by less than a minute of what this source reads
        }
        out.push(Warning {
            scope: None,
            condition: Condition::LagClimbing,
            subject_kind: SubjectKind::Source,
            site: None,
            subject_id: source_id.to_string(),
            group: Some(group.to_string()),
            severity: Severity::Warning,
            since_millis: Some(since_millis),
            message: format!(
                "source {source_id} {} in group {group:?} has lag climbing, {first_lag} to {last_lag} over the last \
                 {run_len} samples, {}; add reader capacity or reduce what is published to it",
                if queue {
                    format!("queue {topic}")
                } else {
                    format!("partition {partition} of topic {topic}")
                },
                if behind.is_finite() {
                    format!("about {} behind", humanize_secs(behind as u64))
                } else {
                    format!("with nothing {} meanwhile", if queue { "finished" } else { "read" })
                }
            ),
        });
    }
    if unreadable > 0 {
        tracing::warn!(
            target: "warnings.lag_subjects_unreadable",
            subjects = unreadable,
            exemplar = unreadable_key.as_deref(),
            "lag series subjects were dropped because their keys could not be parsed"
        );
    }
    if incomplete > 0 {
        tracing::warn!(
            target: "warnings.lag_subjects_incomplete",
            subjects = incomplete,
            exemplar = incomplete_key.as_deref(),
            "lag series subjects were dropped for naming no group, source, topic or partition"
        );
    }
    out.sort_by(|a, b| {
        (a.group.as_deref().unwrap_or(""), a.subject_id.as_str(), a.since_millis).cmp(&(
            b.group.as_deref().unwrap_or(""),
            b.subject_id.as_str(),
            b.since_millis,
        ))
    });
    out
}

/// A log site whose warning repeats a catalog warning: its latest detail is told on that warning, and it raises
/// no warning of its own, so one fault reads as one warning.
struct Fold {
    site: &'static str,
    condition: Condition,
    /// The field that says what went wrong. A row without it is told by its sentence.
    detail: &'static str,
    /// Whether the catalog warning's subject is the row's `subject` field or the worker that logged it.
    by: FoldBy,
}

enum FoldBy {
    /// A source or destination, by its author's name, within the group the row was delivered under.
    Subject,
    Worker,
}

const FOLDS: &[Fold] = &[
    Fold {
        site: "egress.destination_stalled",
        condition: Condition::DestinationUnreachable,
        detail: "error",
        by: FoldBy::Subject,
    },
    Fold {
        site: "worker.commit_refused",
        condition: Condition::WorkerNotDeployingItsTarget,
        detail: "error",
        by: FoldBy::Worker,
    },
];

/// Log sites whose warning a catalog warning already gives in full, reason and all, read off the heartbeat. They
/// raise nothing of their own and tell nothing on it: their `subject` is the positional id, which no catalog
/// warning is named by.
const REPEATED: &[&str] = &["runtime.source_blocked", "runtime.rows_rejected"];

/// One worker's latest row at one site, its fields read out of their JSON.
struct Logged {
    row: LoggedSite,
    fields: Vec<(String, String)>,
}

impl Logged {
    fn field(&self, key: &str) -> Option<&str> {
        self.fields.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }
}

/// Every worker's latest row at `warn` or `error` per site in the last [`WORKER_LOG_WINDOW_MS`], read once for
/// [`worker_logged_warnings`] and [`fold_logged_details`] both. A revoked worker's logs never speak for it, the same
/// exclusion `worker_silent` and `enrolled_never_heartbeating` apply.
fn recent_logs(store: Option<&RegistryStore>, credentials: &[WorkerCredentialRecord], now_ms: i64) -> Vec<Logged> {
    let Some(store) = store else { return Vec::new() };
    let revoked: HashSet<&str> = credentials
        .iter()
        .filter(|c| c.revoked)
        .map(|c| c.worker_id.as_str())
        .collect();

    let levels = ["warn".to_string(), "error".to_string()];
    // One query, not a page walk: no cap to pick means no cap to pick wrong -- whatever the table holds, past
    // `LOGS_KEPT` or not, this reads the window once and groups as it goes.
    let rows = match store.latest_log_by_site(&levels, now_ms - WORKER_LOG_WINDOW_MS, now_ms) {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                target: "warnings.worker_logs_unreadable",
                error = %e,
                "could not read recent logs, so no worker-logged-warnings warnings are derived this pass"
            );
            return Vec::new();
        }
    };
    rows.into_iter()
        .filter(|row| !revoked.contains(row.worker_id.as_str()))
        .map(|row| {
            let fields = serde_json::from_str::<Vec<(String, crate::heartbeat::FieldValueDto)>>(&row.fields_json)
                .unwrap_or_default()
                .into_iter()
                .map(|(key, value)| (key, crate::logs::render_value(&value.into())))
                .collect();
            Logged { row, fields }
        })
        .collect()
}

/// A worker that has logged at `warn` or `error` at one site in the last [`WORKER_LOG_WINDOW_MS`] -- not broken,
/// but worth a look, so [`Severity::Warning`] rather than `Failed`, an error as much as a warning: what a log line
/// says is worth reading, and the faults that stop work have catalog entries of their own. Reads the log store
/// rather than `snapshot`: a heartbeat carries a thread's current metrics, not whether its runtime has logged
/// anything.
///
/// One warning per worker and site, carrying that site's latest sentence: several rows at one site are one thing
/// to read, and two sites are two. A site in [`FOLDS`] or [`REPEATED`] raises none -- its catalog warning says it
/// already. Not a fact-clock entry, for the reason [`lag_climbing`]'s doc comment gives -- a stored log already
/// carries its own timestamp, so `since_millis` is the earliest row at that site in the window, and the warning
/// clears a window after the latest.
fn worker_logged_warnings(logged: &[Logged]) -> Vec<Warning> {
    // `logged` is per subject as well; a site's warning is its latest row about any subject, since its earliest.
    let mut by_site: BTreeMap<(&str, &str), (&Logged, i64)> = BTreeMap::new();
    for l in logged
        .iter()
        .filter(|l| !FOLDS.iter().any(|f| f.site == l.row.site) && !REPEATED.contains(&l.row.site.as_str()))
    {
        by_site
            .entry((l.row.worker_id.as_str(), l.row.site.as_str()))
            .and_modify(|(latest, first)| {
                *first = (*first).min(l.row.first_ms);
                if l.row.at_ms > latest.row.at_ms {
                    *latest = l;
                }
            })
            .or_insert((l, l.row.first_ms));
    }
    let mut out: Vec<Warning> = by_site
        .into_values()
        .map(|(l, first_ms)| {
            let row = &l.row;
            let what = if row.level == "error" { "an error" } else { "a warning" };
            let fields = crate::logs::render_fields(l.fields.iter().map(|(k, v)| (k.as_str(), v.clone())));
            Warning {
                scope: None,
                condition: Condition::WorkerLoggedWarnings,
                subject_kind: SubjectKind::Worker,
                subject_id: row.worker_id.clone(),
                site: Some(row.site.clone()),
                group: row.group_id.clone(),
                severity: Severity::Warning,
                since_millis: Some(first_ms),
                message: format!(
                    "worker {} logged {what} at {}: {}{fields}",
                    row.worker_id, row.site, row.message
                ),
            }
        })
        .collect();
    out.sort_by(|a, b| (&a.subject_id, &a.site).cmp(&(&b.subject_id, &b.site)));
    out
}

/// Tell each catalog warning a [`FOLDS`] site repeats the latest detail any worker logged there about its subject.
/// Severity, spread and start time stay the catalog's: the log adds the words, not the judgement.
fn fold_logged_details(out: &mut [Warning], logged: &[Logged]) {
    for warning in out.iter_mut() {
        let Some(fold) = FOLDS.iter().find(|f| f.condition == warning.condition) else {
            continue;
        };
        let latest = logged
            .iter()
            .filter(|l| l.row.site == fold.site)
            .filter(|l| match fold.by {
                FoldBy::Subject => {
                    l.row.group_id == warning.group && l.field("subject") == Some(warning.subject_id.as_str())
                }
                FoldBy::Worker => l.row.worker_id == warning.subject_id,
            })
            .max_by_key(|l| l.row.at_ms);
        if let Some(l) = latest {
            let detail = l.field(fold.detail).unwrap_or(&l.row.message);
            warning.message = format!(
                "{}. Last reported by worker {}: {detail}",
                warning.message, l.row.worker_id
            );
        }
    }
}

/// A worker whose supervisor has found a commit it is not running. Every deployment passes through this state for
/// as long as it takes to start, so what raises it is staying there: a heartbeat proves the supervisor is healthy
/// and says nothing at all about whether the deployment it is trying to make ever happened.
///
/// This is what tells a worker that refused a commit from one that is simply up to date. Before a worker said what
/// it was looking for, the two read identically.
/// One worker's side of the mismatch, read from its freshest row.
struct BehindTarget<'a> {
    desired: &'a str,
    /// What it is running instead, which is nothing at all for a worker whose first deployment never ran.
    running: Option<&'a str>,
    failure: Option<&'a str>,
    group: &'a str,
}

fn worker_not_deploying_its_target(
    snapshot: &[ThreadHealth],
    starts: &HashMap<FactKey, i64>,
    now_ms: i64,
) -> Vec<Warning> {
    // The group comes from the same row the rest of this does. A worker keeps its rows for every thread id it has
    // ever reported, so one that restarted into another group still has rows naming the old one, and reading the
    // group from all of them would file the failure under whichever the snapshot happened to hold last.
    let behind: HashMap<&str, BehindTarget<'_>> = freshest_per_worker(snapshot)
        .into_iter()
        .filter(|(_, t)| t.stopped_at_millis.is_none())
        .filter_map(|(worker_id, t)| {
            let supervising = t.supervising.as_ref()?;
            let desired = supervising.desired_commit.as_deref()?;
            not_running_as_found(t.commit.as_deref(), desired, supervising.failure.as_deref()).then_some((
                worker_id,
                BehindTarget {
                    desired,
                    running: t.commit.as_deref(),
                    failure: supervising.failure.as_deref(),
                    group: t.report.group_id.as_str(),
                },
            ))
        })
        .collect();

    let mut out: Vec<Warning> = behind
        .into_iter()
        .filter_map(|(worker_id, behind)| {
            let BehindTarget {
                desired,
                running,
                failure,
                group,
            } = behind;
            // A deployment takes as long as its connectors do, and a worker gives a runtime three minutes to say
            // it has started. Judged against a few heartbeats, most slow-but-fine deployments would read as failed
            // for the whole of their startup; this is that budget with room to spare.
            let since = starts.get(&not_deploying_key(worker_id, desired)).copied();
            if since.is_none_or(|start| now_ms - start < NOT_DEPLOYING_AFTER_MS) {
                return None;
            }
            Some(Warning {
                scope: None,
                condition: Condition::WorkerNotDeployingItsTarget,
                subject_kind: SubjectKind::Worker,
                site: None,
                subject_id: worker_id.to_string(),
                group: Some(group.to_string()),
                severity: Severity::Failed,
                since_millis: since,
                // A worker whose first deployment never ran has nothing to fall back to, so what reassures an
                // operator about every other failure here -- that the old bundle is still processing -- is the
                // opposite of true for this one.
                message: match (failure, running) {
                    (Some(why), Some(running)) if running == desired => {
                        format!(
                            "worker {worker_id} could not restart {desired} with the certificates it holds now: {why}. It keeps \
                             running {desired} unchanged"
                        )
                    }
                    (Some(why), Some(_)) => {
                        format!("worker {worker_id} has not deployed {desired}: {why}. It is still running what it had")
                    }
                    (Some(why), None) => {
                        format!("worker {worker_id} has not deployed {desired}: {why}. It is not processing")
                    }
                    (None, _) => format!("worker {worker_id} has found {desired} and is not running it; read its logs"),
                },
            })
        })
        .collect();
    out.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    out
}

/// The catalog entry name [`worker_not_deploying_its_target`] keeps its start time under.
pub const NOT_DEPLOYING: &str = "workerNotDeployingItsTarget";

/// How long a worker may be behind the commit it found before that is a warning. Longer than the startup budget a
/// worker gives a runtime, so a deployment that is merely slow is never reported as one that failed.
const NOT_DEPLOYING_AFTER_MS: i64 = 5 * 60 * 1000;

/// What the leader knows when it derives the warnings: the fleet's reports and what the leader holds itself.
#[derive(Clone, Copy)]
pub struct Facts<'a> {
    pub snapshot: &'a [ThreadHealth],
    pub credentials: &'a [WorkerCredentialRecord],
    pub starts: &'a HashMap<FactKey, i64>,
    pub heartbeat_interval: Duration,
    pub series_store: Option<&'a RegistryStore>,
    pub deployed_groups: &'a HashSet<String>,
    pub signing: Option<&'a crate::leader::tls::Described>,
    pub rotating: &'a Rotating,
    pub codes: &'a [crate::leader::store::UsableEnrollmentCode],
    pub presenting: &'a crate::leader::enrollment_codes::Presenting,
    pub deployed_certificates: &'a [DeployedCertificate],
    pub held: Option<&'a crate::leader::git::Hold>,
    pub remote: Option<&'a crate::leader::remote::Failure>,
    pub newer: Option<&'a crate::leader::updates::Newer>,
}

/// The fixed catalog (design doc, Control plane), read fresh from `snapshot` and `credentials` on every call.
/// `starts` is the fact clock's answer (`Registry::observe_facts`) for whichever facts need one; a warning with no
/// entry there carries no start time. `series_store` is the durable registry's series half
/// (`crate::leader::registry::Registry::series`), which only [`lag_climbing`] and
/// [`recent_logs`] need; `deployed_groups` is every group the deployed commit holds a subtree for, which
/// only [`group_not_deployed`] needs -- the rest read `snapshot` alone.
pub fn derive(facts: &Facts, now_ms: i64) -> Vec<Warning> {
    let Facts {
        snapshot,
        credentials,
        starts,
        heartbeat_interval,
        series_store,
        deployed_groups,
        signing,
        rotating,
        codes,
        presenting,
        deployed_certificates,
        held,
        remote,
        newer,
    } = *facts;
    let mut out = worker_silent(snapshot, credentials, now_ms, heartbeat_interval);
    out.extend(enrolled_never_heartbeating(snapshot, credentials));
    out.extend(source_blocked(snapshot, starts));
    out.extend(source_unreachable(snapshot, starts));
    out.extend(destination_refusing(snapshot, starts, now_ms, heartbeat_interval));
    out.extend(destination_unreachable(snapshot, starts, heartbeat_interval));
    out.extend(lag_climbing(series_store));
    let logged = recent_logs(series_store, credentials, now_ms);
    out.extend(worker_logged_warnings(&logged));
    out.extend(group_not_deployed(snapshot, deployed_groups, starts));
    out.extend(worker_not_deploying_its_target(snapshot, starts, now_ms));
    out.extend(workers_on_another_build(
        snapshot,
        crate::version::commit(),
        crate::version::RELEASE,
    ));
    out.extend(workers_on_another_release(
        snapshot,
        crate::version::commit(),
        crate::version::RELEASE,
    ));
    out.extend(workers_memory_high(snapshot, heartbeat_interval));
    out.extend(authority_expiring(snapshot, credentials, now_ms));
    out.extend(authority_aging(signing, rotating, now_ms));
    out.extend(enrollment_code_names_outgoing_authority(codes, presenting));
    out.extend(certificate_expiring(deployed_certificates, now_ms));
    out.extend(held.map(deploy_held));
    out.extend(remote.map(remote_not_followed));
    out.extend(newer.zip(crate::version::RELEASE).map(|(n, own)| newer_release(n, own)));
    out.extend(declared_fields_drifted(series_store, now_ms));
    out.extend(streams_unrouted(series_store, now_ms));
    fold_logged_details(&mut out, &logged);
    out
}

/// The fleet held behind `main`'s head by a rollback. Raised for as long as the hold lasts, because a fleet that does
/// not take what is deployed after it looks, to anyone who did not do the rollback, like a deploy that failed.
fn deploy_held(held: &crate::leader::git::Hold) -> Warning {
    let short = super::git::short;
    Warning {
        scope: None,
        condition: Condition::DeployHeld,
        subject_kind: SubjectKind::Leader,
        site: None,
        subject_id: held.commit.clone(),
        group: None,
        severity: Severity::Warning,
        since_millis: Some(held.since * 1000),
        message: format!(
            "the fleet is held at {}, deployed there by {}, and does not run the {} commit{} on main since, up to {}. \
             Release it on the History page to deploy main's head",
            short(&held.commit),
            held.by.display_name,
            held.behind,
            if held.behind == 1 { "" } else { "s" },
            short(&held.head),
        ),
    }
}

/// The fleet does not run the head of the remote the leader follows, and why. Raised until a sync succeeds: a remote
/// commit the fleet does not run is one a person merged on the host and expects to be running.
fn remote_not_followed(failure: &crate::leader::remote::Failure) -> Warning {
    Warning {
        scope: None,
        condition: Condition::RemoteNotFollowed,
        subject_kind: SubjectKind::Leader,
        site: None,
        subject_id: failure.commit.clone().unwrap_or_default(),
        group: None,
        severity: Severity::Warning,
        since_millis: Some(failure.since),
        message: format!("{}. The fleet keeps running what it runs", failure.message),
    }
}

/// A published release newer than this leader's own (`leader::updates`). Info: running an older release is no fault.
fn newer_release(newer: &crate::leader::updates::Newer, own: &str) -> Warning {
    Warning {
        scope: None,
        condition: Condition::NewerRelease,
        subject_kind: SubjectKind::Leader,
        site: None,
        subject_id: "leader".to_string(),
        group: None,
        severity: Severity::Info,
        since_millis: Some(newer.since),
        message: format!("smeltr {} is out, and this leader runs {own}", newer.release),
    }
}

/// A certificate this leader issued: its name, how it is issued, and its newest chain described.
pub type IssuedChain = (String, crate::leader::store::Issued, Vec<crate::leader::tls::Described>);

/// Why a certificate this leader issued is due for its next version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Due {
    /// Two thirds of the way through its newest version's life, as an ordered one is, since the time it holds.
    Aged(i64),
    /// Ending at a root other than the one the leader signs under, which a rotation's cutover leaves it doing.
    OtherRoot,
    /// Its newest chain cannot be read, so nothing says it is not due.
    Unreadable,
}

/// Whether the certificate whose newest chain is `chain`, described, is due for its next version under `signing`.
pub fn issued_due(chain: &[crate::leader::tls::Described], signing: &str, now_ms: i64) -> Option<Due> {
    let (Some(leaf), Some(root)) = (chain.first(), chain.last()) else {
        return Some(Due::Unreadable);
    };
    let renew_after = leaf.not_before_millis + (leaf.not_after_millis - leaf.not_before_millis) * 2 / 3;
    if root.fingerprint != signing {
        Some(Due::OtherRoot)
    } else if now_ms >= renew_after {
        Some(Due::Aged(renew_after))
    } else {
        None
    }
}

/// How long a certificate may stay due before it is a warning. The leader looks for one due every hour, so a day is
/// a renewal that failed, not one that has yet to run.
const RENEWAL_GRACE_MILLIS: i64 = 24 * 60 * 60 * 1000;

/// The certificates this leader issued that it should have renewed and has not, by name and newest chain described.
/// Only the aged ones: after a cutover every one is due at once and is renewed within the hour, and a failure there
/// is in the log, as one here is.
pub fn issued_certificates_not_renewed(issued: &[IssuedChain], signing: &str, now_ms: i64) -> Vec<Warning> {
    let mut out = Vec::new();
    for (name, _, chain) in issued {
        let Some(Due::Aged(since)) = issued_due(chain, signing, now_ms) else {
            continue;
        };
        if now_ms < since + RENEWAL_GRACE_MILLIS {
            continue;
        }
        let remaining = chain[0].not_after_millis - now_ms;
        let when = if remaining <= 0 {
            "has expired".to_string()
        } else {
            format!("expires in {} days", remaining / (24 * 60 * 60 * 1000))
        };
        out.push(Warning {
            scope: None,
            condition: Condition::IssuedCertificateNotRenewed,
            subject_kind: SubjectKind::Certificate,
            site: None,
            subject_id: name.clone(),
            group: None,
            severity: if remaining <= 0 {
                Severity::Failed
            } else {
                Severity::Warning
            },
            since_millis: Some(since),
            message: format!(
                "{when}, and this leader has not renewed it. The leader's log says why: Status → Logs \
                 (leader.certificate_renewal_failed)"
            ),
        });
    }
    out
}

/// What the key's failure is about, as the Issues page names it.
const KEY_SUBJECT: &str = "secrets encryption key";

/// The facts the secrets encryption key's warnings start from, so each says how long it has held.
pub fn encryption_key_facts(standing: &KeyStanding) -> HashSet<FactKey> {
    if standing.broken() {
        return HashSet::from([FactKey::new("secretsKeyUnusable", KEY_SUBJECT)]);
    }
    standing
        .unopenable
        .iter()
        .map(|u| FactKey::new("secretNeedsValue", unopenable_id(u)))
        .collect()
}

/// A key that is missing or wrong, as one failure: nothing sealed can be stored or changed until it is fixed. Once
/// it holds again, each value it cannot open is a warning of its own, which clears when that value is entered again.
/// A warning rather than a failure, because a running worker still holds what it was given.
/// `issued` names the certificates this leader issued, which are renewed rather than replaced.
pub fn encryption_key_warnings(
    standing: &KeyStanding,
    kept: &std::collections::BTreeMap<String, crate::leader::store::Kept>,
    starts: &HashMap<FactKey, i64>,
) -> Vec<Warning> {
    let leader = |condition, subject_id: String, severity, message| Warning {
        scope: None,
        condition,
        subject_kind: SubjectKind::Leader,
        site: None,
        since_millis: starts.get(&FactKey::new(condition.name(), subject_id.clone())).copied(),
        subject_id,
        group: None,
        severity,
        message,
    };
    if let Some(problem) = standing.problem() {
        return vec![leader(
            Condition::SecretsKeyUnusable,
            KEY_SUBJECT.to_string(),
            Severity::Failed,
            problem,
        )];
    }
    standing
        .unopenable
        .iter()
        .map(|u| {
            let (subject_kind, message) = match u {
                Unopenable::Secret(_) => (
                    SubjectKind::Secret,
                    "encrypted with a key this leader no longer has. Store a new version".to_string(),
                ),
                Unopenable::Certificate(name) => (
                    SubjectKind::Certificate,
                    match kept.get(name) {
                        Some(crate::leader::store::Kept::Issued(_)) => {
                            "encrypted with a key this leader no longer has. The leader issues a new version within \
                             the hour, and workers take it on their next heartbeat"
                                .to_string()
                        }
                        Some(crate::leader::store::Kept::Acme) => format!(
                            "encrypted with a key this leader no longer has. Order it again in {LEADER_CERTIFICATE_TAB}"
                        ),
                        None => {
                            format!("encrypted with a key this leader no longer has. Replace it in {CERTIFICATE_LIST}")
                        }
                    },
                ),
                Unopenable::Setting(key) => (
                    SubjectKind::Leader,
                    format!(
                        "encrypted with a key this leader no longer has. Enter it again under {}",
                        sealed_setting_place(key).1
                    ),
                ),
            };
            Warning {
                subject_kind,
                ..leader(
                    Condition::SecretNeedsValue,
                    unopenable_id(u),
                    Severity::Warning,
                    message,
                )
            }
        })
        .collect()
}

fn unopenable_id(u: &Unopenable) -> String {
    match u {
        Unopenable::Secret(name) | Unopenable::Certificate(name) => name.clone(),
        Unopenable::Setting(key) => sealed_setting_place(key).0.to_string(),
    }
}

/// What a sealed leader setting is called, and where it is entered.
pub(crate) fn sealed_setting_place(key: &str) -> (&str, &'static str) {
    match key {
        crate::leader::remote::TOKEN_KEY => ("Git remote token", "System, Git remote"),
        crate::leader::oidc::CLIENT_SECRET_KEY => ("Single sign-on client secret", "Accounts, Single sign-on"),
        crate::leader::model::API_KEY_KEY => ("AI agents API key", "System, AI agents"),
        crate::leader::acme::EAB_HMAC_KEY => ("ACME EAB HMAC key", "System, Certificates"),
        crate::leader::acme::CLOUDFLARE_TOKEN_KEY => ("Cloudflare API token", "System, Certificates"),
        other => (other, "System"),
    }
}

/// What is wrong with the deployed commit, as warnings: [`derive`]'s catalog takes these beside its own.
pub fn deploy_checked(checks: &Findings, starts: &HashMap<FactKey, i64>) -> Vec<Warning> {
    let mut out = deploy_does_not_load(checks, starts);
    out.extend(deploy_fails_tests(checks, starts));
    out
}

/// A group the deployed commit holds that does not load. Its workers refuse it and keep running what they had, and a
/// worker that starts now runs nothing, so it is a failure however many workers are up to say so.
fn deploy_does_not_load(checks: &Findings, starts: &HashMap<FactKey, i64>) -> Vec<Warning> {
    let short = super::git::short(&checks.commit);
    checks
        .not_loading
        .iter()
        .map(|n| Warning {
            scope: None,
            condition: Condition::DeployDoesNotLoad,
            subject_kind: SubjectKind::Group,
            site: None,
            subject_id: n.group.clone(),
            group: Some(n.group.clone()),
            severity: Severity::Failed,
            since_millis: starts.get(&does_not_load_key(&n.group)).copied(),
            message: format!(
                "group {:?} does not load at the deployed commit {short}: {}. Running workers keep their old config, \
                 but a restarted worker stops processing. Deploy a fix, or roll back on the History page",
                n.group, n.why
            ),
        })
        .collect()
}

/// Production no longer matching what a Parser step declares (design doc, Deferred: drift is the leader's to
/// propose), one warning per pipeline of a group and per kind: a declared field whose values failed to coerce within
/// the drift window, and one its step's rows have gone the window without while the step read them. A field is
/// summed over every source and route of the group that ran its step, so it is gone only once none of them carries
/// it.
fn declared_fields_drifted(store: Option<&RegistryStore>, now_ms: i64) -> Vec<Warning> {
    let Some(store) = store else { return Vec::new() };
    let fields = match store.declared_fields() {
        Ok(fields) => fields,
        Err(e) => {
            tracing::warn!(
                target: "warnings.declared_fields_unreadable",
                error = %e,
                "could not read the declared fields production saw, so no drift warnings are derived this pass"
            );
            return Vec::new();
        }
    };
    let window = DRIFT_WINDOW.as_millis() as i64;
    // By group, pipeline, step and field, in the store's order, so each warning names its fields as they arrived.
    let current = |f: &DeclaredField| f.failed_last_ms.is_some_and(|at| now_ms - at <= window);
    let mut summed: Vec<DeclaredField> = Vec::new();
    let mut at: HashMap<(String, String, String, String), usize> = HashMap::new();
    for mut f in fields {
        let key = (f.group_id.clone(), f.pipeline.clone(), f.step.clone(), f.field.clone());
        if !current(&f) {
            f.failing_since_ms = None;
        }
        match at.get(&key) {
            None => {
                at.insert(key, summed.len());
                summed.push(f);
            }
            Some(&i) => {
                let s = &mut summed[i];
                s.read_last_ms = s.read_last_ms.max(f.read_last_ms);
                s.present_last_ms = s.present_last_ms.max(f.present_last_ms);
                s.failed_last_ms = s.failed_last_ms.max(f.failed_last_ms);
                s.failing_since_ms = s.failing_since_ms.into_iter().chain(f.failing_since_ms).min();
            }
        }
    }
    // Failing first, then gone; within each, by group and pipeline: the fields each names, and since when.
    type Named = (Vec<String>, Option<i64>);
    let mut drifted: BTreeMap<(bool, &str, &str), Named> = BTreeMap::new();
    for f in &summed {
        let mut raise = |gone: bool, since: Option<i64>| {
            let (named, earliest) = drifted.entry((gone, &f.group_id, &f.pipeline)).or_default();
            named.push(format!("{} in step {}", f.field, f.step));
            *earliest = earliest.take().into_iter().chain(since).min();
        };
        if f.failed_last_ms.is_some_and(|at| now_ms - at <= window) {
            raise(false, f.failing_since_ms);
        }
        let Some(read) = f.read_last_ms.filter(|read| now_ms - read <= window) else {
            continue;
        };
        if let Some(present) = f.present_last_ms.filter(|at| read - at > window) {
            raise(true, Some(present));
        }
    }
    drifted
        .into_iter()
        .map(|((gone, group, pipeline), (named, since))| {
            let many = named.len() > 1;
            let named = named.join(", ");
            Warning {
                scope: None,
                condition: if gone {
                    Condition::DeclaredFieldGone
                } else {
                    Condition::DeclaredFieldFailing
                },
                subject_kind: SubjectKind::Pipeline,
                site: None,
                subject_id: pipeline.to_string(),
                group: Some(group.to_string()),
                severity: Severity::Warning,
                since_millis: since,
                message: if gone {
                    format!("pipeline {pipeline} in group {group:?}: no row has held {named} in the last hour.")
                } else {
                    format!(
                        "pipeline {pipeline} in group {group:?}: {named} {} failed to read as {} declared type in the \
                         last hour.",
                        if many { "have" } else { "has" },
                        if many { "their" } else { "its" },
                    )
                },
            }
        })
        .collect()
}

/// Rows a source received that no route took, within the drift window: one warning per source of a group, naming
/// the streams they came under. It clears once a window passes with none, which is how a route that takes them shows,
/// however its condition is written.
fn streams_unrouted(store: Option<&RegistryStore>, now_ms: i64) -> Vec<Warning> {
    let Some(store) = store else { return Vec::new() };
    let sources = match store.unrouted_since(now_ms - DRIFT_WINDOW.as_millis() as i64) {
        Ok(sources) => sources,
        Err(e) => {
            tracing::warn!(
                target: "warnings.streams_unreadable",
                error = %e,
                "could not read the streams production saw, so no unrouted-stream warnings are derived this pass"
            );
            return Vec::new();
        }
    };
    sources
        .into_iter()
        .map(|s| Warning {
            scope: None,
            condition: Condition::StreamUnrouted,
            subject_kind: SubjectKind::Source,
            site: None,
            subject_id: s.source.clone(),
            group: Some(s.group_id.clone()),
            severity: Severity::Warning,
            since_millis: s.since_ms,
            message: format!(
                "source {} in group {:?}: no route took {} in the last hour.",
                s.source,
                s.group_id,
                untaken(&s.keys)
            ),
        })
        .collect()
}

/// What a `StreamUnrouted` warning says no route took: its named streams, and the rows filed under none.
fn untaken(keys: &[String]) -> String {
    let named: Vec<&str> = keys.iter().filter(|k| !k.is_empty()).map(String::as_str).collect();
    let unnamed = named.len() < keys.len() || keys.is_empty();
    let streams = match named.len() {
        0 => None,
        1 => Some(format!("stream {}", named[0])),
        _ => Some(format!("streams {}", named.join(", "))),
    };
    match (streams, unnamed) {
        (Some(s), false) => s,
        (Some(s), true) => format!("{s} and rows under no stream"),
        (None, _) => "its rows".to_string(),
    }
}

/// The tests that fail at the deployed commit, one warning per subject: the pipeline they run, so the map shows it
/// there, or for an end-to-end test, which names no pipeline, its group. A test in `shared/` belongs to no one group.
fn deploy_fails_tests(checks: &Findings, starts: &HashMap<FactKey, i64>) -> Vec<Warning> {
    let short = super::git::short(&checks.tests_at);
    // While a deploy's own tests run, what is shown is what the commit before it found.
    let tested = if checks.tests_pending() {
        format!("{short}, the last deployed commit tested")
    } else {
        format!("the deployed commit {short}")
    };
    // By scope, then pipeline: `None` is an end-to-end test, about its group.
    let mut by_subject: std::collections::BTreeMap<(&str, Option<&str>), Vec<&FailingTest>> = Default::default();
    for t in &checks.failing {
        by_subject.entry((&t.scope, t.pipeline.as_deref())).or_default().push(t);
    }
    by_subject
        .into_iter()
        .map(|((scope, pipeline), tests)| {
            let (subject_kind, subject_id) = match pipeline {
                Some(pipeline) => (SubjectKind::Pipeline, pipeline.to_string()),
                None => (SubjectKind::Group, scope.to_string()),
            };
            let each: Vec<String> = tests.iter().map(|t| format!("{:?}: {}", t.name, t.why)).collect();
            Warning {
                scope: None,
                condition: Condition::DeployFailsTests,
                subject_kind,
                site: None,
                subject_id,
                group: (scope != crate::config_file::SHARED_DIR).then(|| scope.to_string()),
                severity: Severity::Warning,
                since_millis: tests
                    .iter()
                    .filter_map(|t| starts.get(&fails_tests_key(&t.scope, &t.id)).copied())
                    .min(),
                message: format!(
                    "{} at {}: {}. Fix what it tests, or lock it again if the new output is \
                     right",
                    if tests.len() == 1 {
                        "a test fails".to_string()
                    } else {
                        format!("{} tests fail", tests.len())
                    },
                    tested,
                    each.join("; "),
                ),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    /// The catalog as it goes over the wire. `gui-frontend/src/api.ts` and `website/src/lib/fleet-demo.ts`
    /// declare these same names and switch on them, so a variant added or renamed here without them is a label
    /// they cannot produce. Nothing across that boundary is checked by a compiler; this is what stands in.
    #[test]
    fn every_condition_keeps_the_name_its_clients_know() {
        let wire = |c: Condition| serde_json::to_string(&c).unwrap();
        assert_eq!(wire(Condition::CertificateExpiring), "\"certificateExpiring\"");
        assert_eq!(wire(Condition::WorkerSilent), "\"workerSilent\"");
        assert_eq!(wire(Condition::WorkerNeverHeartbeated), "\"workerNeverHeartbeated\"");
        assert_eq!(wire(Condition::SourceBlocked), "\"sourceBlocked\"");
        assert_eq!(wire(Condition::SourceUnreachable), "\"sourceUnreachable\"");
        assert_eq!(wire(Condition::DestinationRefusing), "\"destinationRefusing\"");
        assert_eq!(wire(Condition::DestinationUnreachable), "\"destinationUnreachable\"");
        assert_eq!(wire(Condition::LagClimbing), "\"lagClimbing\"");
        assert_eq!(wire(Condition::GroupNotDeployed), "\"groupNotDeployed\"");
        assert_eq!(
            wire(Condition::WorkerNotDeployingItsTarget),
            format!("\"{NOT_DEPLOYING}\""),
            "the fact clock's key and the condition's wire name are one name"
        );
        assert_eq!(wire(Condition::WorkerLoggedWarnings), "\"workerLoggedWarnings\"");
    }

    use std::path::Path;

    use super::*;
    use crate::heartbeat::{MetricSampleDto, ThreadHeartbeat, ThreadKind};

    const NOW: i64 = 1_000_000_000;

    /// The root every fixture below is rotating away from, and the one an enrollment code has to name to be worth
    /// saying.
    const OUTGOING: &str = "the outgoing root";

    /// A leader rotating away from [`OUTGOING`], which is what makes an enrollment code naming it worth saying anything
    /// about.
    fn under_way() -> crate::leader::enrollment_codes::Presenting {
        crate::leader::enrollment_codes::Presenting::Own {
            signing: OUTGOING.to_string(),
            coming: "the root being moved to".to_string(),
        }
    }

    /// The same leader with nothing being replaced.
    fn settled() -> crate::leader::enrollment_codes::Presenting {
        crate::leader::enrollment_codes::Presenting::Own {
            signing: OUTGOING.to_string(),
            coming: OUTGOING.to_string(),
        }
    }

    /// The page switches on these names in another language, where an entry it has never heard of is a blank
    /// label rather than a compile error. This is that compile error.
    #[test]
    fn the_page_labels_every_condition() {
        let api = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("gui-frontend/src/api.ts"))
            .expect("the typed client");
        for condition in Condition::ALL {
            let name = condition.name();
            assert_eq!(
                serde_json::to_string(condition).unwrap(),
                format!("\"{name}\""),
                "{condition:?} is serialized as something other than what it calls itself"
            );
            assert!(
                api.contains(&format!("| '{name}'")),
                "WarningCondition does not list {name}"
            );
            assert!(
                api.contains(&format!("case '{name}':")),
                "conditionLabel has no label for {name}"
            );
        }
    }
    /// A group that does not load is a failure about that group; failing tests are one warning per pipeline they
    /// run, an end-to-end test's about its group, and a `shared/` test's about no group.
    #[test]
    fn what_is_wrong_with_the_deployed_commit_is_said_once_per_subject() {
        use crate::leader::deploy_checks::{FailingTest, Findings, NotLoading};
        let failing = |scope: &str, id: &str, pipeline: Option<&str>| FailingTest {
            scope: scope.to_string(),
            id: id.to_string(),
            name: id.to_string(),
            pipeline: pipeline.map(str::to_string),
            why: "it differs".to_string(),
        };
        let checks = Findings {
            commit: "0123456789abcdef".to_string(),
            tests_at: "0123456789abcdef".to_string(),
            not_loading: vec![NotLoading {
                group: "west".to_string(),
                why: "bad manifest".to_string(),
            }],
            failing: vec![
                failing("east", "a", Some("orders")),
                failing("east", "b", Some("orders")),
                failing("east", "routes", None),
                failing("shared", "c", Some("enrich")),
            ],
        };
        let registry = crate::leader::registry::Registry::default();
        let starts: HashMap<FactKey, i64> = deploy_facts(&checks, &registry).into_iter().zip(1..).collect();
        let warnings = deploy_checked(&checks, &starts);
        let seen: Vec<(Condition, SubjectKind, &str, Option<&str>, Severity)> = warnings
            .iter()
            .map(|w| {
                (
                    w.condition,
                    w.subject_kind,
                    w.subject_id.as_str(),
                    w.group.as_deref(),
                    w.severity,
                )
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                (
                    Condition::DeployDoesNotLoad,
                    SubjectKind::Group,
                    "west",
                    Some("west"),
                    Severity::Failed
                ),
                (
                    Condition::DeployFailsTests,
                    SubjectKind::Group,
                    "east",
                    Some("east"),
                    Severity::Warning
                ),
                (
                    Condition::DeployFailsTests,
                    SubjectKind::Pipeline,
                    "orders",
                    Some("east"),
                    Severity::Warning
                ),
                (
                    Condition::DeployFailsTests,
                    SubjectKind::Pipeline,
                    "enrich",
                    None,
                    Severity::Warning
                ),
            ]
        );
        assert!(warnings[0].message.contains("01234567"), "{}", warnings[0].message);
        assert!(
            warnings[2].message.starts_with("2 tests fail"),
            "{}",
            warnings[2].message
        );
        assert_eq!(warnings[2].since_millis, Some(2), "the earliest of the two");
    }

    /// While a deploy's tests run, a failing test's start time is kept rather than forgotten.
    #[test]
    fn a_failing_tests_clock_survives_the_run_after_a_deploy() {
        use crate::leader::deploy_checks::Findings;
        let registry = crate::leader::registry::Registry::default();
        let key = fails_tests_key("east", "t");
        registry.observe_facts(&HashSet::from([key.clone()]), 1_000);
        let pending = Findings {
            commit: "b".to_string(),
            tests_at: "a".to_string(),
            ..Default::default()
        };
        let active: HashSet<FactKey> = deploy_facts(&pending, &registry).into_iter().collect();
        assert_eq!(registry.observe_facts(&active, 9_000).get(&key), Some(&1_000));
        let settled = Findings {
            tests_at: "b".to_string(),
            ..pending
        };
        assert!(
            deploy_facts(&settled, &registry).is_empty(),
            "once they land, it holds only if it still fails"
        );
    }

    const INTERVAL: Duration = Duration::from_secs(2);

    /// A worker reporting the commit it runs and the commit its supervisor has found.
    fn behind(worker_id: &str, running: Option<&str>, desired: &str, failure: Option<&str>) -> ThreadHealth {
        let mut health = thread_health(worker_id, "t0", 10, vec![]);
        health.commit = running.map(str::to_string);
        health.supervising = Some(crate::heartbeat::Supervising {
            desired_commit: Some(desired.to_string()),
            failure: failure.map(str::to_string),
            ..Default::default()
        });
        health
    }

    /// Being behind is what every deployment is while it starts, so the fact has to have been true for a while
    /// before it is a warning. The fact clock is what remembers since when.
    #[test]
    fn a_worker_that_has_just_found_a_commit_is_not_yet_a_warning() {
        let snapshot = vec![behind("w1", Some("older"), "newer", None)];
        let active = currently_active_facts(&snapshot, NOW, INTERVAL, &deployed());
        assert!(
            active.contains(&not_deploying_key("w1", "newer")),
            "the fact is true from the first beat that names it"
        );

        let just_now = HashMap::from([(not_deploying_key("w1", "newer"), NOW)]);
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &just_now,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert!(
            !warnings
                .iter()
                .any(|w| w.condition == Condition::WorkerNotDeployingItsTarget),
            "a deployment that has only just started is not a failure"
        );
    }

    /// A worker on the commit it found that refused to restart onto a new version of a certificate it presents keeps
    /// presenting the old one until it expires. Nothing else says so: the expiry warning reads the newest version.
    #[test]
    fn a_worker_refusing_to_restart_what_it_runs_is_a_warning() {
        let refusing =
            "source \"in\" presents a certificate whose Extended Key Usage does not permit client authentication";
        let snapshot = vec![behind("w1", Some("newer"), "newer", Some(refusing))];
        assert!(
            currently_active_facts(&snapshot, NOW, INTERVAL, &deployed()).contains(&not_deploying_key("w1", "newer"))
        );
        let stuck = HashMap::from([(not_deploying_key("w1", "newer"), NOW - 600_000)]);
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &stuck,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let raised = (warnings.iter())
            .find(|w| w.condition == Condition::WorkerNotDeployingItsTarget)
            .expect("a worker refusing to restart is a warning");
        assert_eq!(
            raised.message,
            format!(
                "worker w1 could not restart newer with the certificates it holds now: {refusing}. It keeps running newer unchanged"
            )
        );

        let running = vec![behind("w1", Some("newer"), "newer", None)];
        assert!(
            !currently_active_facts(&running, NOW, INTERVAL, &deployed()).contains(&not_deploying_key("w1", "newer"))
        );
    }

    /// The distinction the leader could not draw before a worker said what it was looking for: this one refused a
    /// commit and kept running the one before it, which read exactly like being up to date.
    #[test]
    fn a_worker_stuck_on_a_commit_it_will_not_run_is_a_warning_naming_why() {
        let snapshot = vec![behind(
            "w1",
            Some("older"),
            "newer",
            Some("it names a credential this host lacks"),
        )];
        let stuck = HashMap::from([(not_deploying_key("w1", "newer"), NOW - 600_000)]);
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &stuck,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let raised = warnings
            .iter()
            .find(|w| w.condition == Condition::WorkerNotDeployingItsTarget)
            .expect("a worker that is not running what it found is a warning");
        assert_eq!(raised.subject_id, "w1");
        assert!(
            raised.message.contains("newer"),
            "it names the commit: {}",
            raised.message
        );
        assert!(
            raised.message.contains("credential"),
            "it says why, when the worker said: {}",
            raised.message
        );
    }

    /// Being behind one commit says nothing about the next. A worker that spent its whole grace failing to deploy
    /// one commit, and is then pointed at another, is starting afresh -- and a clock carried over from the commit
    /// it was moved off would make the replacement a failure the moment discovery found it.
    #[test]
    fn a_worker_pointed_at_a_different_commit_starts_its_grace_again() {
        let stuck_on_first = vec![behind("w1", None, "first", Some("it will not compile"))];
        let long_ago = HashMap::from([(not_deploying_key("w1", "first"), NOW - 600_000)]);
        assert!(
            derive(
                &Facts {
                    snapshot: &stuck_on_first,
                    credentials: &[],
                    starts: &long_ago,
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .iter()
            .any(|w| w.condition == Condition::WorkerNotDeployingItsTarget),
            "a worker past its grace on the commit it is stuck behind is a warning"
        );

        // Discovery has since found another commit, and this worker is at the very start of deploying it.
        let now_on_second = vec![behind("w1", None, "second", None)];
        assert!(
            derive(
                &Facts {
                    snapshot: &now_on_second,
                    credentials: &[],
                    starts: &long_ago,
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .iter()
            .all(|w| w.condition != Condition::WorkerNotDeployingItsTarget),
            "the replacement commit inherited the grace the one before it had already spent"
        );
    }

    /// A worker whose very first deployment never ran has no older bundle behind it. Telling an operator it is
    /// "still running what it had" sends them looking for data that was never being processed.
    #[test]
    fn a_worker_that_has_never_deployed_is_not_said_to_be_running_what_it_had() {
        let snapshot = vec![behind(
            "w1",
            None,
            "first",
            Some("it names a credential this host lacks"),
        )];
        let stuck = HashMap::from([(not_deploying_key("w1", "first"), NOW - 600_000)]);
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &stuck,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let raised = warnings
            .iter()
            .find(|w| w.condition == Condition::WorkerNotDeployingItsTarget)
            .expect("a worker that never deployed what it found is still a warning");
        assert!(
            !raised.message.contains("still running what it had"),
            "it claimed an idle worker was still processing: {}",
            raised.message
        );
        assert!(
            raised.message.contains("not processing"),
            "it does not say what is actually true of it: {}",
            raised.message
        );
    }

    /// A worker running exactly what it found raises nothing, however long it has been doing so.
    #[test]
    fn a_worker_running_the_commit_it_found_is_not_a_warning() {
        let snapshot = vec![behind("w1", Some("same"), "same", None)];
        let long_ago = HashMap::from([(not_deploying_key("w1", "same"), NOW - 600_000)]);
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &long_ago,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert!(
            !warnings
                .iter()
                .any(|w| w.condition == Condition::WorkerNotDeployingItsTarget),
            "a worker that is up to date is not behind"
        );
        assert!(
            currently_active_facts(&snapshot, NOW, INTERVAL, &deployed())
                .iter()
                .all(|k| *k != not_deploying_key("w1", "same"))
        );
    }

    fn thread_health(
        worker_id: &str,
        thread_id: &str,
        last_seen_millis_ago: u64,
        metrics: Vec<MetricSampleDto>,
    ) -> ThreadHealth {
        thread_health_in("g", worker_id, thread_id, last_seen_millis_ago, metrics)
    }

    /// Like [`thread_health`], but in a named group -- for a test that must tell two groups apart.
    fn thread_health_in(
        group_id: &str,
        worker_id: &str,
        thread_id: &str,
        last_seen_millis_ago: u64,
        metrics: Vec<MetricSampleDto>,
    ) -> ThreadHealth {
        ThreadHealth {
            worker_id: worker_id.to_string(),
            last_seen_millis_ago,
            stop_after_unreachable_secs: None,
            commit: None,
            smeltr_version: crate::version::VERSION.to_string(),
            release: None,
            supervising: None,
            resources: None,
            authority_expires_millis: None,
            stopped_at_millis: None,
            report: ThreadHeartbeat {
                thread_id: thread_id.to_string(),
                group_id: group_id.to_string(),
                kind: Some(ThreadKind::Reader),
                partitions_owned: vec![],
                metrics,
            },
        }
    }

    /// Like [`thread_health`], but tagged as the egress thread it needs to be to count toward `DESTINATION_HEALTHY`
    /// ([`unreachable_destinations`], [`workers_destination_unhealthy`], [`workers_running_destination`] all require it).
    fn egress_thread_health(
        worker_id: &str,
        thread_id: &str,
        last_seen_millis_ago: u64,
        metrics: Vec<MetricSampleDto>,
    ) -> ThreadHealth {
        let mut health = thread_health(worker_id, thread_id, last_seen_millis_ago, metrics);
        health.report.kind = Some(ThreadKind::Egress);
        health
    }

    const DAY: i64 = 24 * 60 * 60 * 1000;

    fn holding_authority_until(worker_id: &str, expires: i64) -> ThreadHealth {
        ThreadHealth {
            authority_expires_millis: Some(expires),
            ..thread_health(worker_id, "t0", 0, vec![])
        }
    }

    fn authority(life_days: i64, remaining_days: i64) -> crate::leader::tls::Described {
        let day = DAY;
        crate::leader::tls::Described {
            subject: "smeltr leader certificate authority".to_string(),
            fingerprint: "ab".repeat(32),
            names: Vec::new(),
            not_before_millis: NOW + remaining_days * day - life_days * day,
            not_after_millis: NOW + remaining_days * day,
        }
    }

    // -- the certificate this leader serves -----------------------------------

    /// What this leader serves: a 45-day certificate with `remaining_days` left, from `source`.
    fn serving(source: crate::leader::acme::Source, remaining_days: i64) -> Serving {
        let not_after_millis = NOW + remaining_days * DAY;
        Serving {
            certificate: crate::leader::tls::Described {
                subject: "smeltr.example.com".to_string(),
                fingerprint: "cd".repeat(32),
                names: vec!["smeltr.example.com".to_string()],
                not_before_millis: not_after_millis - 45 * DAY,
                not_after_millis,
            },
            source,
            entry: None,
            acme: crate::leader::acme::State {
                renew_after_millis: Some(not_after_millis - 15 * DAY),
                ..Default::default()
            },
        }
    }

    /// The last try, at `NOW`, failed with `error`.
    fn failing(mut serving: Serving, error: &str) -> Serving {
        serving.acme.last_error = Some(error.to_string());
        serving.acme.last_attempt_millis = Some(NOW);
        serving
    }

    /// Said once a renewal is due and the last try failed, dated from when it fell due: the leader has been trying
    /// since then.
    #[test]
    fn a_failing_acme_renewal_is_said_from_when_it_fell_due() {
        let due = failing(
            serving(crate::leader::acme::Source::Acme, 12),
            "the authority refused the order.",
        );
        let said = leader_certificate(&due, NOW);
        assert_eq!(said.len(), 1);
        assert_eq!(said[0].condition, Condition::AcmeRenewalFailing);
        assert_eq!(said[0].severity, Severity::Warning);
        assert_eq!(said[0].since_millis, due.acme.renew_after_millis);
        assert_eq!(said[0].subject_id, "smeltr.example.com");
        assert!(
            said[0].message.ends_with("Last error: the authority refused the order"),
            "{}",
            said[0].message
        );
        assert!(said[0].message.contains("expires in 12 days"), "{}", said[0].message);
    }

    /// A renewal not yet due, or one that has not failed, is nothing to say.
    #[test]
    fn an_acme_certificate_renewing_on_time_is_not_said() {
        let early = failing(serving(crate::leader::acme::Source::Acme, 20), "the authority is down");
        assert!(leader_certificate(&early, NOW).is_empty(), "failed before it was due");
        let quiet = serving(crate::leader::acme::Source::Acme, 10);
        assert!(leader_certificate(&quiet, NOW).is_empty(), "due, and nothing failed");
    }

    /// An error from before the renewal fell due, such as a Renew now that failed early, is not the renewal
    /// failing: nothing has tried since.
    #[test]
    fn an_error_from_before_the_renewal_fell_due_is_not_said() {
        let mut stale = failing(serving(crate::leader::acme::Source::Acme, 12), "the authority is down");
        stale.acme.last_attempt_millis = stale.acme.renew_after_millis.map(|due| due - 1);
        assert!(leader_certificate(&stale, NOW).is_empty());
    }

    /// ACME turned off, or its names no longer orderable, tries nothing and records no failure: the certificate
    /// still has to be said before it runs out.
    #[test]
    fn an_acme_certificate_nothing_renews_is_said_in_its_last_tenth() {
        let said = leader_certificate(&serving(crate::leader::acme::Source::Acme, 4), NOW);
        assert_eq!(said[0].condition, Condition::LeaderCertificateRunningOut);
        assert!(
            said[0]
                .message
                .starts_with("this leader's certificate expires in 4 days, and ACME is not renewing it"),
            "{}",
            said[0].message
        );
    }

    #[test]
    fn an_acme_certificate_that_ran_out_is_a_failure() {
        let expired = failing(serving(crate::leader::acme::Source::Acme, -1), "the authority is down");
        let said = leader_certificate(&expired, NOW);
        assert_eq!(said[0].severity, Severity::Failed);
        assert!(
            said[0]
                .message
                .starts_with("this leader could not renew its certificate, and the certificate has expired"),
            "{}",
            said[0].message
        );
    }

    /// An uploaded certificate is said in its last tenth, by the entry it is a copy of, where it is replaced.
    #[test]
    fn an_uploaded_certificate_is_said_in_its_last_tenth() {
        let mut uploaded = serving(crate::leader::acme::Source::Upload, 5);
        uploaded.entry = Some("web".to_string());
        assert!(
            leader_certificate(&uploaded, NOW).is_empty(),
            "45 days, 5 left: not in its last tenth yet"
        );

        let mut uploaded = serving(crate::leader::acme::Source::Upload, 4);
        uploaded.entry = Some("web".to_string());
        let said = leader_certificate(&uploaded, NOW);
        assert_eq!(said[0].condition, Condition::LeaderCertificateRunningOut);
        assert_eq!(
            said[0].since_millis,
            Some(uploaded.certificate.not_after_millis - 45 * DAY / 10)
        );
        assert!(
            said[0]
                .message
                .starts_with("this leader's certificate \"web\" expires in 4 days. Replace it")
        );
    }

    #[test]
    fn the_leaders_own_certificate_is_issued_again_where_it_was_issued() {
        let said = leader_certificate(&serving(crate::leader::acme::Source::Own, 0), NOW + 1);
        assert_eq!(said[0].severity, Severity::Failed);
        assert_eq!(
            said[0].message,
            "this leader's certificate has expired. Issue a new one in Admin → System → Leader certificate"
        );
    }

    /// An authority's error goes inside a sentence, so it must not end the sentence or swamp the row.
    #[test]
    fn an_authority_error_is_one_clause() {
        assert_eq!(one_line("refused.\ndetail"), "refused");
        let long = "word ".repeat(60);
        let clipped = one_line(&long);
        assert!(clipped.chars().count() <= 201, "{clipped}");
        assert!(clipped.ends_with("word…"), "{clipped}");
    }

    // -- a deployed connector's certificates -----------------------------------

    /// One certificate a source carries: how long it is good for, and how much of that is left. Every caller
    /// not testing a split rollout wants the same commit, so two certificates it built are one trust store unless
    /// `on_commit` says otherwise.
    fn carried(source: &str, half: Half, life_days: i64, remaining_days: i64) -> DeployedCertificate {
        on_commit(source, half, life_days, remaining_days, "c0")
    }

    fn on_commit(source: &str, half: Half, life_days: i64, remaining_days: i64, commit: &str) -> DeployedCertificate {
        DeployedCertificate {
            group: "g".to_string(),
            commit: commit.to_string(),
            subject_kind: SubjectKind::Source,
            subject_id: source.to_string(),
            half,
            not_before_millis: NOW + remaining_days * DAY - life_days * DAY,
            not_after_millis: NOW + remaining_days * DAY,
        }
    }

    #[test]
    fn a_certificate_with_most_of_its_life_left_is_not_work_yet() {
        let year = carried("acme", Half::Presented, 365, 200);
        assert!(certificate_expiring(&[year], NOW).is_empty());
    }

    #[test]
    fn the_certificate_a_source_presents_is_worth_saying_before_it_goes() {
        let said = certificate_expiring(&[carried("acme", Half::Presented, 365, 20)], NOW);
        assert_eq!(said.len(), 1);
        assert_eq!(said[0].condition, Condition::CertificateExpiring);
        assert_eq!(said[0].subject_kind, SubjectKind::Source);
        assert_eq!(said[0].subject_id, "acme");
        assert_eq!(said[0].group.as_deref(), Some("g"));
        assert_eq!(said[0].severity, Severity::Warning);
        assert!(said[0].message.contains("expires in 20 days"), "{}", said[0].message);
    }

    /// A source and a destination may share an id, and each is its own connector with its own certificate.
    #[test]
    fn a_source_and_a_destination_sharing_an_id_are_warned_about_apart() {
        let source = carried("kafka", Half::Presented, 365, 5);
        let destination = DeployedCertificate {
            subject_kind: SubjectKind::Destination,
            ..carried("kafka", Half::Presented, 365, 20)
        };
        let said = certificate_expiring(&[source, destination], NOW);
        let messages: Vec<&str> = said.iter().map(|w| w.message.as_str()).collect();
        assert_eq!(said.len(), 2, "{messages:?}");
        assert!(
            messages
                .iter()
                .any(|m| m.starts_with("source \"kafka\"") && m.contains("in 5 days")),
            "{messages:?}"
        );
        assert!(
            messages
                .iter()
                .any(|m| m.starts_with("destination \"kafka\"") && m.contains("in 20 days")),
            "{messages:?}"
        );
    }

    /// Working and something is off, until the day it is not working at all.
    #[test]
    fn a_certificate_that_has_run_out_is_a_failure_rather_than_a_warning() {
        let said = certificate_expiring(&[carried("acme", Half::Presented, 365, -1)], NOW);
        assert_eq!(said.len(), 1);
        assert_eq!(said[0].severity, Severity::Failed);
        assert!(
            said[0].message.contains("fails at the handshake"),
            "{}",
            said[0].message
        );
    }

    /// A certificate that has not started yet is a current failure, not a countdown: a TLS peer rejects it today
    /// whatever the notice window says about a `not_after` a year off, which is why this is not caught by the
    /// ordinary "expires soon" check at all.
    #[test]
    fn a_certificate_not_valid_yet_is_a_failure_however_far_off_it_expires() {
        let said = certificate_expiring(&[carried("acme", Half::Presented, 365, 400)], NOW);
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0].severity, Severity::Failed);
        assert!(said[0].message.contains("not valid yet"), "{}", said[0].message);
    }

    /// The decisive member for a countdown is the soonest-expiring one, but that is not necessarily the same
    /// member whose start matters: a leaf good today beside an intermediate not valid yet is a chain path
    /// validation rejects regardless of which member a min-expiry selection happened to keep.
    #[test]
    fn a_presented_chain_is_not_yet_valid_when_any_member_is_even_if_the_soonest_expiring_one_is_fine() {
        let leaf = carried("acme", Half::Presented, 3650, 20);
        let intermediate = carried("acme", Half::Presented, 3650, 4000);
        assert!(
            NOW >= leaf.not_before_millis && NOW < leaf.not_after_millis,
            "the fixture's leaf must actually be valid now"
        );
        assert!(
            NOW < intermediate.not_before_millis,
            "the fixture's intermediate must actually not be valid yet"
        );
        assert!(
            intermediate.not_after_millis > leaf.not_after_millis,
            "the intermediate must not be the decisive member on its own expiry"
        );

        let said = certificate_expiring(&[leaf, intermediate], NOW);
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0].severity, Severity::Failed);
        assert!(said[0].message.contains("not valid yet"), "{}", said[0].message);
    }

    #[test]
    fn an_authority_not_valid_yet_is_a_failure_however_far_off_it_expires() {
        let said = certificate_expiring(&[carried("acme", Half::Trusted, 365, 400)], NOW);
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0].severity, Severity::Failed);
        assert!(said[0].message.contains("not valid yet"), "{}", said[0].message);
    }

    /// A trust store's roots are alternatives, not a chain: staging tomorrow's rollover root today, ahead of the
    /// swap, must not report the source failed while the root actually in use still verifies fine. Picking the
    /// largest `not_after` among candidates -- without asking whether one is even valid yet -- is exactly the bug
    /// that would have outranked the good root with the pre-staged one.
    #[test]
    fn a_pre_staged_rollover_root_does_not_fail_a_source_a_current_root_still_verifies() {
        let current = carried("acme", Half::Trusted, 3650, 3600);
        let pre_staged = carried("acme", Half::Trusted, 3650, 4000);
        assert!(
            now_ms_is_within(&current, NOW),
            "the fixture must actually be valid now"
        );
        assert!(
            NOW < pre_staged.not_before_millis,
            "the fixture must actually not be valid yet"
        );

        let said = certificate_expiring(&[current, pre_staged], NOW);
        assert!(
            said.is_empty(),
            "a root the fleet can still verify against was outranked by one not valid yet: {said:?}"
        );
    }

    fn now_ms_is_within(certificate: &DeployedCertificate, now_ms: i64) -> bool {
        certificate.not_before_millis <= now_ms && now_ms < certificate.not_after_millis
    }

    /// A date, so the warning knows exactly when it began rather than when it was first looked for: notice
    /// opened a tenth of this certificate's life before it runs out.
    #[test]
    fn a_certificate_warning_begins_when_its_notice_did() {
        let year = carried("acme", Half::Presented, 365, 20);
        let notice = (365 * DAY) / 10;
        let said = certificate_expiring(std::slice::from_ref(&year), NOW);
        assert_eq!(said[0].since_millis, Some(year.not_after_millis - notice));
    }

    /// A chain is only as good as its soonest part: an intermediate that has run out breaks it whatever the leaf
    /// says about itself.
    #[test]
    fn a_presented_chain_runs_out_with_its_soonest_part() {
        let said = certificate_expiring(
            &[
                carried("acme", Half::Presented, 3650, 2000),
                carried("acme", Half::Presented, 365, 10),
            ],
            NOW,
        );
        assert_eq!(said.len(), 1, "one source, one thing to say about what it presents");
        assert!(said[0].message.contains("expires in 10 days"), "{}", said[0].message);
    }

    /// The other way round, and the reason the two halves are counted apart: a trust store holds several roots on
    /// purpose, and during a far end's own rollover the outgoing one expires sooner and matters to nobody.
    #[test]
    fn trust_runs_out_with_the_last_authority_rather_than_the_first() {
        let rolling_over = [
            carried("acme", Half::Trusted, 3650, 5),
            carried("acme", Half::Trusted, 3650, 2000),
        ];
        assert!(
            certificate_expiring(&rolling_over, NOW).is_empty(),
            "an outgoing root beside an incoming one is not trust running out"
        );

        let said = certificate_expiring(
            &[
                carried("acme", Half::Trusted, 3650, 5),
                carried("acme", Half::Trusted, 3650, 20),
            ],
            NOW,
        );
        assert_eq!(said.len(), 1);
        assert!(said[0].message.contains("last authority"), "{}", said[0].message);
    }

    /// A group mid-rollout runs two commits, and the same source in each is never one trust store: a worker
    /// on the old commit has only its own roots, so a fresh root the new commit added must not hide the old
    /// commit's own running out.
    #[test]
    fn a_commits_own_authorities_are_never_hidden_by_anothers() {
        let said = certificate_expiring(
            &[
                on_commit("acme", Half::Trusted, 3650, 5, "old"),
                on_commit("acme", Half::Trusted, 3650, 2000, "new"),
            ],
            NOW,
        );
        assert_eq!(
            said.len(),
            1,
            "the old commit's own root is a warning; the new commit's is nowhere near expiring: {said:?}"
        );
        assert!(said[0].message.contains("expires in 5 days"), "{}", said[0].message);
    }

    /// An ordinary rollout has both commits still carrying the same certificate for a source neither is
    /// rotating: two decisive entries, one per commit, saying the exact same thing. That is one warning, not a
    /// duplicate in the ribbon.
    #[test]
    fn two_commits_carrying_the_same_untouched_certificate_are_one_warning() {
        let said = certificate_expiring(
            &[
                on_commit("acme", Half::Trusted, 30, 2, "old"),
                on_commit("acme", Half::Trusted, 30, 2, "new"),
            ],
            NOW,
        );
        assert_eq!(said.len(), 1, "{said:?}");
    }

    /// Each half fails on its own and is said on its own.
    #[test]
    fn the_two_halves_of_one_source_are_counted_apart() {
        let said = certificate_expiring(
            &[
                carried("acme", Half::Presented, 365, 10),
                carried("acme", Half::Trusted, 365, 10),
            ],
            NOW,
        );
        assert_eq!(said.len(), 2);
        assert!(said.iter().all(|w| w.subject_id == "acme"));
    }

    /// `shared/` declares one source and every group runs its own deployment of it, so each group is
    /// told separately -- which is what the fleet view rolls up by.
    #[test]
    fn a_source_two_groups_deploy_is_said_once_for_each() {
        let mut west = carried("corp-root", Half::Presented, 365, 10);
        west.group = "west".to_string();
        let mut east = carried("corp-root", Half::Presented, 365, 10);
        east.group = "east".to_string();
        let said = certificate_expiring(&[west, east], NOW);
        assert_eq!(said.len(), 2);
        assert_eq!(
            said.iter().map(|w| w.group.clone().unwrap()).collect::<Vec<_>>(),
            vec!["east".to_string(), "west".to_string()],
            "and in an order a poll cannot change under a reader"
        );
    }

    /// Proportional, because how long is long enough is proportional: a rotation waits for every worker to come
    /// round, and what that costs is a property of the fleet rather than of the calendar.
    #[test]
    fn the_leaders_own_authority_is_worth_saying_in_the_last_part_of_its_life() {
        let decade = authority(3650, 400);
        assert!(
            authority_aging(Some(&decade), &Rotating::None, NOW).is_empty(),
            "a decade-long authority with over a year left is not work yet"
        );

        let decade = authority(3650, 300);
        let said = authority_aging(Some(&decade), &Rotating::None, NOW);
        assert_eq!(said.len(), 1, "a tenth of a decade left and nothing said");
        assert_eq!(said[0].severity, Severity::Warning);
        assert_eq!(said[0].subject_kind, SubjectKind::Leader);
    }

    /// The answer is already under way, and a warning nobody can clear is one an operator learns to ignore.
    #[test]
    fn an_authority_already_being_rotated_is_not_worth_saying() {
        let expiring = authority(3650, 10);
        assert!(!authority_aging(Some(&expiring), &Rotating::None, NOW).is_empty());
        assert!(authority_aging(Some(&expiring), &Rotating::UnderWay, NOW).is_empty());
    }

    /// Past the date it is not a warning about work to start, it is a leader no worker given that authority can
    /// reach.
    #[test]
    fn an_expired_authority_is_a_failure_rather_than_a_warning() {
        let gone = authority(3650, -1);
        let said = authority_aging(Some(&gone), &Rotating::None, NOW);
        assert_eq!(said[0].severity, Severity::Failed);
        assert!(said[0].message.contains("has expired"), "{}", said[0].message);
    }

    /// The registry never evicts, so a thread a deployment removed keeps reporting the authority that worker held
    /// before it was re-provisioned. A warning drawn from it could never be cleared.
    #[test]
    fn a_retired_thread_does_not_outvote_what_a_worker_says_now() {
        let stale = ThreadHealth {
            authority_expires_millis: Some(NOW + DAY),
            last_seen_millis_ago: 900_000,
            ..thread_health("w1", "retired", 900_000, vec![])
        };
        let live = ThreadHealth {
            authority_expires_millis: Some(NOW + 400 * DAY),
            last_seen_millis_ago: 200,
            ..thread_health("w1", "running", 200, vec![])
        };
        assert!(
            authority_expiring(&[stale, live], &[], NOW).is_empty(),
            "the retired thread's old authority raised a warning the worker cannot clear"
        );
    }

    /// The same retired thread against a worker that now reports no authority at all -- one moved onto plain HTTP,
    /// or behind a proxy. Its live silence is the most recent thing it has said, and has to win.
    #[test]
    fn a_retired_thread_does_not_outvote_a_worker_that_now_reports_none() {
        let stale = ThreadHealth {
            authority_expires_millis: Some(NOW + DAY),
            last_seen_millis_ago: 900_000,
            ..thread_health("w1", "retired", 900_000, vec![])
        };
        let live = ThreadHealth {
            last_seen_millis_ago: 200,
            ..thread_health("w1", "running", 200, vec![])
        };
        assert!(
            authority_expiring(&[stale, live], &[], NOW).is_empty(),
            "a retired thread's old authority raised a warning the worker no longer reports on"
        );
    }

    /// A decommissioned worker can never heartbeat again, so nothing would ever clear this.
    #[test]
    fn a_revoked_worker_is_not_told_to_update_its_authority() {
        let snapshot = vec![holding_authority_until("w1", NOW + 10 * DAY)];
        let revoked = [WorkerCredentialRecord {
            worker_id: "w1".to_string(),
            token_hash: String::new(),
            revoked: true,
            used: true,
            kind: crate::leader::store::WorkerKind::Permanent,
            enrolled_at: None,
        }];
        assert_eq!(
            authority_expiring(&snapshot, &[], NOW).len(),
            1,
            "warned while enrolled"
        );
        assert!(authority_expiring(&snapshot, &revoked, NOW).is_empty());
    }

    #[test]
    fn an_authority_with_plenty_of_life_left_is_not_worth_saying() {
        let snapshot = vec![holding_authority_until("w1", NOW + 365 * DAY)];
        assert!(authority_expiring(&snapshot, &[], NOW).is_empty());
    }

    #[test]
    fn an_authority_running_out_is_a_warning_naming_the_worker_and_the_days() {
        let snapshot = vec![holding_authority_until("w1", NOW + 10 * DAY)];
        let out = authority_expiring(&snapshot, &[], NOW);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].condition, Condition::AuthorityExpiring);
        assert_eq!(out[0].subject_id, "w1");
        assert_eq!(out[0].severity, Severity::Warning);
        assert!(out[0].message.contains("10 days"), "{}", out[0].message);
        assert!(out[0].message.contains("ca.pem"), "{}", out[0].message);
    }

    /// Past the date it is no longer advice: that worker has stopped fetching configuration and the leader will
    /// not have noticed, because its heartbeats still arrive.
    #[test]
    fn an_authority_already_expired_has_failed_rather_than_warned() {
        let snapshot = vec![holding_authority_until("w1", NOW - DAY)];
        let out = authority_expiring(&snapshot, &[], NOW);

        assert_eq!(out[0].severity, Severity::Failed);
        assert!(out[0].message.contains("expired"), "{}", out[0].message);
    }

    /// One worker, one warning, whatever it is running: the authority is the process's, not each thread's.
    #[test]
    fn a_worker_with_several_threads_is_warned_about_once() {
        let snapshot = vec![
            ThreadHealth {
                authority_expires_millis: Some(NOW + 5 * DAY),
                ..thread_health("w1", "t0", 0, vec![])
            },
            ThreadHealth {
                authority_expires_millis: Some(NOW + 5 * DAY),
                ..thread_health("w1", "t1", 0, vec![])
            },
        ];
        assert_eq!(authority_expiring(&snapshot, &[], NOW).len(), 1);
    }

    /// A worker on plain HTTP, and one older than the field, report nothing. Silence is not an expiry.
    #[test]
    fn a_worker_that_reports_no_authority_raises_nothing() {
        let snapshot = vec![thread_health("w1", "t0", 0, vec![])];
        assert!(authority_expiring(&snapshot, &[], NOW).is_empty());
    }

    fn blocked_metric(source_id: &str, reason: &str) -> MetricSampleDto {
        MetricSampleDto {
            metric: SOURCE_BLOCKED_REASON.to_string(),
            labels: vec![("source".to_string(), source_id.to_string())],
            value: MetricValueDto::Text(reason.to_string()),
        }
    }

    fn unreachable_metric(source_id: &str, reason: &str) -> MetricSampleDto {
        MetricSampleDto {
            metric: SOURCE_CONNECTION_ERROR.to_string(),
            labels: vec![("source".to_string(), source_id.to_string())],
            value: MetricValueDto::Text(reason.to_string()),
        }
    }

    fn refusal_metric(dest_id: &str, at_millis: i64, message: &str) -> MetricSampleDto {
        MetricSampleDto {
            metric: DESTINATION_LAST_REFUSAL.to_string(),
            labels: vec![
                ("destination".to_string(), dest_id.to_string()),
                ("at_millis".to_string(), at_millis.to_string()),
            ],
            value: MetricValueDto::Text(message.to_string()),
        }
    }

    fn destination_healthy_metric(dest_id: &str, healthy: f64) -> MetricSampleDto {
        MetricSampleDto {
            metric: DESTINATION_HEALTHY.to_string(),
            labels: vec![("destination".to_string(), dest_id.to_string())],
            value: MetricValueDto::Gauge(healthy),
        }
    }

    fn credential(worker_id: &str, revoked: bool) -> WorkerCredentialRecord {
        WorkerCredentialRecord {
            worker_id: worker_id.to_string(),
            token_hash: "h".to_string(),
            revoked,
            used: true,
            kind: crate::leader::store::WorkerKind::Permanent,
            enrolled_at: None,
        }
    }

    // -- outside_normal_warnings ------------------------------------------------

    fn rows_metric(source_id: &str) -> MetricSampleDto {
        MetricSampleDto {
            metric: "smeltr_rows_polled_total".to_string(),
            labels: vec![("source".to_string(), source_id.to_string())],
            value: MetricValueDto::Counter(0),
        }
    }

    fn reading_of(source: &str, rows: f64) -> throughput::Reading {
        throughput::Reading {
            group: "g".to_string(),
            source: source.to_string(),
            rows,
            ended_ms: NOW,
            band: throughput::Band {
                low: 1000.0,
                high: 5000.0,
            },
        }
    }

    fn outside(readings: &[throughput::Reading], snapshot: &[ThreadHealth]) -> Vec<(Condition, String)> {
        outside_normal_warnings(readings, snapshot, &HashSet::new(), &HashMap::new(), INTERVAL)
            .into_iter()
            .map(|w| (w.condition, w.message))
            .collect()
    }

    #[test]
    fn a_source_outside_its_normal_warns_on_the_side_it_is_on() {
        let snapshot = [thread_health_in(
            "g",
            "w1",
            "t0",
            0,
            vec![rows_metric("low"), rows_metric("high"), rows_metric("ok")],
        )];
        let readings = [
            reading_of("high", 9000.0),
            reading_of("low", 1800.0 / 3.0),
            reading_of("ok", 2000.0),
        ];
        let warned = outside(&readings, &snapshot);
        assert_eq!(
            warned,
            [
                (
                    Condition::SourceRowsAbove,
                    "source high: 9,000 rows in 30 minutes, normal at this time of day is 1,000 to 5,000".to_string()
                ),
                (
                    Condition::SourceRowsBelow,
                    "source low: 600 rows in 30 minutes, normal at this time of day is 1,000 to 5,000".to_string()
                ),
            ]
        );
        let w = &outside_normal_warnings(&readings, &snapshot, &HashSet::new(), &HashMap::new(), INTERVAL)[0];
        assert_eq!(
            (w.severity, w.subject_kind, w.group.as_deref()),
            (Severity::Warning, SubjectKind::Source, Some("g"))
        );
    }

    #[test]
    fn a_source_whose_low_reading_has_another_cause_does_not_also_warn() {
        let readings = [reading_of("s", 0.0)];
        for metrics in [
            vec![rows_metric("s"), blocked_metric("s", "held")],
            vec![rows_metric("s"), unreachable_metric("s", "no route to host")],
        ] {
            assert!(outside(&readings, &[thread_health_in("g", "w1", "t0", 0, metrics)]).is_empty());
        }
        // One of its workers silent.
        let silent = stale_after_ms(INTERVAL) as u64 + 1;
        let snapshot = [
            thread_health_in("g", "w1", "t0", 0, vec![rows_metric("s")]),
            thread_health_in("g", "w2", "t0", silent, vec![rows_metric("s")]),
        ];
        assert!(outside(&readings, &snapshot).is_empty());
        // No worker runs it any more.
        assert!(
            outside(
                &readings,
                &[thread_health_in("g", "w1", "t0", 0, vec![rows_metric("other")])]
            )
            .is_empty()
        );
    }

    #[test]
    fn a_source_whose_stop_ended_within_the_window_does_not_warn_on_its_dip() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        let held = || vec![(SOURCE_BLOCKED.to_string(), scoped_subject("g", "s"))];
        store.sync_fact_clock(&[], &held(), NOW - 20 * 60_000).unwrap();
        store.sync_fact_clock(&held(), &[], NOW - 10 * 60_000).unwrap();

        let recovering = recovering_sources(Some(&store), NOW);
        assert_eq!(recovering, HashSet::from([("g".to_string(), "s".to_string())]));
        let snapshot = [thread_health_in("g", "w1", "t0", 0, vec![rows_metric("s")])];
        let readings = [reading_of("s", 0.0)];
        assert!(outside_normal_warnings(&readings, &snapshot, &recovering, &HashMap::new(), INTERVAL).is_empty());

        let window = throughput::WINDOW.as_millis() as i64;
        assert!(
            recovering_sources(Some(&store), NOW - 10 * 60_000 + window + 1).is_empty(),
            "a window after the stop ended, its dip has left the window"
        );
    }

    #[test]
    fn a_sources_warnings_come_back_as_spans_oldest_first_and_the_one_still_held_open() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        let fact = |entry: &str, id: &str| vec![(entry.to_string(), scoped_subject("g", id))];
        store
            .sync_fact_clock(&[], &fact(SOURCE_BLOCKED, "s"), NOW - 30 * 60_000)
            .unwrap();
        store
            .sync_fact_clock(&fact(SOURCE_BLOCKED, "s"), &[], NOW - 20 * 60_000)
            .unwrap();
        store
            .sync_fact_clock(&[], &fact(SOURCE_ROWS_BELOW, "s"), NOW - 10 * 60_000)
            .unwrap();
        store
            .sync_fact_clock(&[], &fact(SOURCE_BLOCKED, "other"), NOW - 10 * 60_000)
            .unwrap();

        let got = spans(&store, SubjectKind::Source, "g", "s", NOW - 60 * 60_000).unwrap();
        assert_eq!(
            got,
            vec![
                WarningSpan {
                    condition: Condition::SourceBlocked,
                    severity: Severity::Failed,
                    started_millis: NOW - 30 * 60_000,
                    ended_millis: Some(NOW - 20 * 60_000),
                },
                WarningSpan {
                    condition: Condition::SourceRowsBelow,
                    severity: Severity::Warning,
                    started_millis: NOW - 10 * 60_000,
                    ended_millis: None,
                },
            ]
        );
        assert_eq!(
            spans(&store, SubjectKind::Source, "g", "s", NOW - 15 * 60_000)
                .unwrap()
                .len(),
            1,
            "a span that ended before the window is left out"
        );
        assert!(spans(&store, SubjectKind::Destination, "g", "s", 0).unwrap().is_empty());
    }

    #[test]
    fn a_source_outside_its_normal_is_dated_from_when_it_left_it() {
        let snapshot = [thread_health_in("g", "w1", "t0", 0, vec![rows_metric("s")])];
        let readings = [reading_of("s", 0.0)];
        let facts = outside_normal_facts(&readings, &snapshot, &HashSet::new(), INTERVAL);
        let starts: HashMap<FactKey, i64> = facts.into_iter().map(|k| (k, NOW - 60_000)).collect();
        let w = &outside_normal_warnings(&readings, &snapshot, &HashSet::new(), &starts, INTERVAL)[0];
        assert_eq!(w.since_millis, Some(NOW - 60_000));
    }

    // -- running_deployments ---------------------------------------------------

    fn thread_on_commit(group: &str, worker_id: &str, commit: &str, last_seen_millis_ago: u64) -> ThreadHealth {
        ThreadHealth {
            commit: Some(commit.to_string()),
            ..thread_health_in(group, worker_id, "t0", last_seen_millis_ago, vec![])
        }
    }

    #[test]
    fn a_worker_reporting_a_commit_is_a_running_deployment() {
        let snapshot = [thread_on_commit("g", "w1", "c1", 0)];
        assert_eq!(
            running_deployments(&snapshot, &[], INTERVAL),
            HashSet::from([("g".to_string(), "c1".to_string())])
        );
    }

    /// A row `record_heartbeat` refreshed for a worker's *current* group, not the stale one an old thread's row
    /// is still frozen under, goes stale itself the moment that thread drops out of a heartbeat -- the same
    /// staleness window `worker_silent` already uses.
    #[test]
    fn a_stale_row_left_behind_by_a_worker_that_moved_groups_is_not_running() {
        let stale_after = stale_after_ms(INTERVAL) as u64;
        let snapshot = [
            // The row a worker's old group left behind: still carries a commit, but nothing has refreshed
            // `last_seen` on it since the worker stopped running that thread.
            thread_on_commit("west", "w1", "c9", stale_after + 1),
            // What the worker is actually running now.
            thread_on_commit("east", "w1", "c9", 0),
        ];
        assert_eq!(
            running_deployments(&snapshot, &[], INTERVAL),
            HashSet::from([("east".to_string(), "c9".to_string())]),
            "the stale row from the group the worker left must not invent a deployment nobody runs"
        );
    }

    #[test]
    fn a_revoked_workers_commit_is_never_a_running_deployment() {
        let snapshot = [thread_on_commit("g", "w1", "c1", 0)];
        assert!(running_deployments(&snapshot, &[credential("w1", true)], INTERVAL).is_empty());
    }

    /// `main` holding a subtree for every group `thread_health` ever reports ("g"), the ordinary case every test
    /// but the ones about [`Condition::GroupNotDeployed`] itself wants.
    fn deployed() -> HashSet<String> {
        HashSet::from(["g".to_string()])
    }

    /// The key `registry::subject_key` would produce for `smeltr_source_lag { group, source, topic, partition }` --
    /// labels sorted alphabetically, `thread` already dropped.
    fn lag_subject_key(group: &str, source: &str, topic: &str, partition: &str) -> String {
        serde_json::to_string(&[
            ("group", group),
            ("partition", partition),
            ("source", source),
            ("topic", topic),
        ])
        .unwrap()
    }

    /// An in-memory registry with one partition's lag gauge seeded at consecutive ten-second buckets, oldest
    /// first -- one value per bucket, the shape [`lag_climbing`] reads. Group "g", the ordinary case every test but
    /// the ones about two groups wants.
    fn store_with_lag(values: &[f64]) -> RegistryStore {
        store_with_lag_in("g", values)
    }

    /// Like [`store_with_lag`], but in a named group -- for a test that must tell two groups' lag apart.
    fn store_with_lag_in(group: &str, values: &[f64]) -> RegistryStore {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        let key = lag_subject_key(group, "7", "orders", "0");
        for (i, v) in values.iter().enumerate() {
            store.record_gauge_max(SOURCE_LAG, &key, i as i64 * 10_000, *v).unwrap();
        }
        store
    }

    // -- declared fields drifted ------------------------------------------------

    /// One window of source `source` through step `parse` of pipeline `normalize`, whose declared field `ts` was
    /// present in `present` of its 10 rows and failed in `failed`.
    fn declared_window(source: &str, present: u64, failed: u64) -> crate::heartbeat::StreamReport {
        use crate::heartbeat::*;
        StreamReport {
            parsers: vec![ParserShape {
                source: source.into(),
                route: "to-es".into(),
                pipeline: "normalize".into(),
                step: "parse".into(),
                rows: 10,
                fields: vec![FieldShape {
                    name: "ts".into(),
                    declared: true,
                    suggested: "datetime".into(),
                    present,
                    failed,
                    ..Default::default()
                }],
                fields_dropped: 0,
            }],
            unmatched: Vec::new(),
        }
    }

    const HOUR: i64 = 60 * 60 * 1000;

    #[test]
    fn a_declared_field_failing_within_the_window_warns_from_when_its_failures_began() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        for (source, failed, at) in [("a", 1, 1_000), ("a", 2, 5_000), ("b", 0, 6_000)] {
            store
                .record_streams("prod", &declared_window(source, 10, failed), None, true, at)
                .unwrap();
        }
        let warnings = declared_fields_drifted(Some(&store), 5_000 + HOUR);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        let w = &warnings[0];
        assert_eq!(
            (
                w.condition,
                w.subject_kind,
                w.subject_id.as_str(),
                w.group.as_deref(),
                w.since_millis
            ),
            (
                Condition::DeclaredFieldFailing,
                SubjectKind::Pipeline,
                "normalize",
                Some("prod"),
                Some(1_000)
            )
        );
        assert_eq!(
            w.message,
            "pipeline normalize in group \"prod\": ts in step parse has failed to read as its declared type in the last \
             hour."
        );
        assert!(
            declared_fields_drifted(Some(&store), 5_001 + HOUR).is_empty(),
            "a failure older than the window is past"
        );
    }

    // -- streams unrouted ------------------------------------------------------

    /// Rows of `source` that no route took, `rows` under each of `keys`, as a worker reports them.
    fn untaken_window(source: &str, keys: &[(&str, u64)]) -> crate::heartbeat::StreamReport {
        use crate::heartbeat::*;
        StreamReport {
            parsers: Vec::new(),
            unmatched: vec![UnmatchedStreams {
                source: source.into(),
                rows: keys.iter().map(|(_, rows)| rows).sum(),
                keys: keys
                    .iter()
                    .map(|(key, rows)| StreamKey {
                        key: (*key).into(),
                        rows: *rows,
                    })
                    .collect(),
                keys_dropped: 0,
            }],
        }
    }

    #[test]
    fn a_stream_is_unrouted_while_rows_no_route_took_arrive_under_it_within_the_window() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        let beat = |keys: &[(&str, u64)], at: i64| {
            store
                .record_streams("prod", &untaken_window("hec", keys), None, true, at)
                .unwrap();
        };
        beat(&[("okta:system", 3), ("pan:threat", 2)], 1_000);
        beat(&[("pan:threat", 4)], 2_000 + HOUR);
        let warnings = streams_unrouted(Some(&store), 2_000 + HOUR);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        let w = &warnings[0];
        assert_eq!(
            (w.condition, w.subject_kind, w.subject_id.as_str(), w.group.as_deref()),
            (Condition::StreamUnrouted, SubjectKind::Source, "hec", Some("prod"))
        );
        assert_eq!(
            w.message, "source hec in group \"prod\": no route took stream pan:threat in the last hour.",
            "okta:system came an hour before, and is taken now"
        );
        assert_eq!(
            w.since_millis,
            Some(2_000 + HOUR),
            "an hour with none ended the run that began at 1_000"
        );
        beat(&[("pan:threat", 1)], 3_000 + HOUR);
        assert_eq!(
            streams_unrouted(Some(&store), 3_000 + HOUR)[0].since_millis,
            Some(2_000 + HOUR),
            "rows within the hour carry the run on"
        );
        assert!(
            streams_unrouted(Some(&store), 3_001 + 2 * HOUR).is_empty(),
            "a window with none clears it, whatever route took them"
        );
    }

    #[test]
    fn an_unrouted_source_names_its_streams_and_the_rows_filed_under_none() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        let said = |keys: &[(&str, u64)]| {
            store
                .record_streams("prod", &untaken_window("in", keys), None, true, 1_000)
                .unwrap();
            streams_unrouted(Some(&store), 1_000).remove(0).message
        };
        assert_eq!(
            said(&[("", 2)]),
            "source in in group \"prod\": no route took its rows in the last hour."
        );
        assert_eq!(
            said(&[("a", 1), ("b", 1)]),
            "source in in group \"prod\": no route took streams a, b and rows under no stream in the last hour."
        );
    }

    #[test]
    fn a_declared_field_is_gone_once_no_source_of_its_step_has_carried_it_for_the_window() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        let beat = |source: &str, present: u64, at: i64| {
            store
                .record_streams("prod", &declared_window(source, present, 0), None, true, at)
                .unwrap();
        };
        beat("a", 10, 1_000);
        beat("b", 10, 2_000);
        beat("a", 0, 1_500 + HOUR);
        beat("b", 0, 2_000 + HOUR);
        let now = 2_000 + HOUR;
        assert!(
            declared_fields_drifted(Some(&store), now).is_empty(),
            "b carried it within the hour"
        );
        beat("a", 0, 4_000 + HOUR);
        let warnings = declared_fields_drifted(Some(&store), 4_000 + HOUR);
        assert_eq!(
            warnings
                .iter()
                .map(|w| (w.condition, w.since_millis))
                .collect::<Vec<_>>(),
            [(Condition::DeclaredFieldGone, Some(2_000))]
        );
        assert_eq!(
            warnings[0].message,
            "pipeline normalize in group \"prod\": no row has held ts in step parse in the last hour."
        );
        assert!(
            declared_fields_drifted(Some(&store), 5_000 + 2 * HOUR).is_empty(),
            "a step that stopped reading has nothing to say of its fields"
        );
    }

    // -- worker logged warnings ------------------------------------------------

    /// A log line at `at_ms`, for seeding [`RegistryStore::record_logs`].
    fn log_event(seq: u64, at_ms: i64, level: &str, message: &str) -> crate::heartbeat::LogDto {
        crate::heartbeat::LogDto {
            seq,
            timestamp_ms: at_ms,
            level: level.to_string(),
            site: "warnings_test".to_string(),
            message: message.to_string(),
            thread: None,
            boot_id: None,
            fields: Vec::new(),
        }
    }

    fn log_warnings(store: &RegistryStore) -> Vec<Warning> {
        log_warnings_with(store, &[])
    }

    fn log_warnings_with(store: &RegistryStore, credentials: &[WorkerCredentialRecord]) -> Vec<Warning> {
        derive(
            &Facts {
                snapshot: &[],
                credentials,
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: Some(store),
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        )
    }

    #[test]
    fn a_warn_log_in_the_window_raises() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[log_event(0, NOW - 60_000, "warn", "a source is blocked")],
            )
            .unwrap();

        let warnings = log_warnings(&store);
        assert_eq!(warnings.len(), 1);
        let w = &warnings[0];
        assert_eq!(w.condition, Condition::WorkerLoggedWarnings);
        assert_eq!(w.subject_kind, SubjectKind::Worker);
        assert_eq!(w.subject_id, "w1");
        assert_eq!(w.group.as_deref(), Some("g"));
        assert_eq!(w.severity, Severity::Warning);
        assert_eq!(w.since_millis, Some(NOW - 60_000));
        assert_eq!(w.site.as_deref(), Some("warnings_test"));
        assert!(w.message.contains("w1"), "{}", w.message);
        assert!(w.message.contains("a source is blocked"), "{}", w.message);
    }

    #[test]
    fn info_and_debug_logs_do_not_raise() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[
                    log_event(0, NOW - 60_000, "info", "worker starting"),
                    log_event(1, NOW - 30_000, "debug", "polled a batch"),
                ],
            )
            .unwrap();
        assert!(log_warnings(&store).is_empty());
    }

    #[test]
    fn a_warn_log_outside_the_window_does_not_raise() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[log_event(0, NOW - WORKER_LOG_WINDOW_MS - 1, "warn", "too long ago")],
            )
            .unwrap();
        assert!(log_warnings(&store).is_empty());
    }

    #[test]
    fn since_millis_is_the_earliest_qualifying_log() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[
                    log_event(0, NOW - 200_000, "warn", "first"),
                    log_event(1, NOW - 100_000, "error", "second"),
                    log_event(2, NOW - 10_000, "warn", "third"),
                ],
            )
            .unwrap();
        let warnings = log_warnings(&store);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].since_millis, Some(NOW - 200_000));
    }

    #[test]
    fn a_revoked_workers_logs_do_not_raise() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs("w1", 1, Some("g"), &[log_event(0, NOW - 60_000, "error", "boom")])
            .unwrap();
        assert!(
            log_warnings_with(&store, &[credential("w1", true)]).is_empty(),
            "revocation is deliberate, not a failure"
        );
    }

    #[test]
    fn two_workers_logging_warnings_each_raise_their_own() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs("w1", 1, Some("g"), &[log_event(0, NOW - 60_000, "warn", "a")])
            .unwrap();
        // w2 logs only an error. An error is as much a reason to look as a warning, so it raises the same condition.
        store
            .record_logs("w2", 1, Some("g"), &[log_event(0, NOW - 30_000, "error", "b")])
            .unwrap();

        let mut warnings = log_warnings(&store);
        warnings.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
        assert_eq!(warnings.len(), 2);
        assert_eq!(warnings[0].subject_id, "w1");
        assert_eq!(warnings[1].subject_id, "w2");
    }

    /// A log line at `site`, carrying `fields` as string values.
    fn site_event(
        seq: u64,
        at_ms: i64,
        site: &str,
        message: &str,
        fields: &[(&str, &str)],
    ) -> crate::heartbeat::LogDto {
        crate::heartbeat::LogDto {
            site: site.to_string(),
            fields: fields
                .iter()
                .map(|(k, v)| (k.to_string(), crate::heartbeat::FieldValueDto::Str(v.to_string())))
                .collect(),
            ..log_event(seq, at_ms, "warn", message)
        }
    }

    #[test]
    fn each_site_a_worker_logs_at_is_its_own_warning_carrying_its_latest_sentence() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[
                    site_event(0, NOW - 200_000, "worker.fetch_failed", "first fetch failed", &[]),
                    site_event(1, NOW - 100_000, "worker.heartbeat_failed", "a heartbeat failed", &[]),
                    site_event(
                        2,
                        NOW - 10_000,
                        "worker.fetch_failed",
                        "fetch failed again",
                        &[("error", "refused")],
                    ),
                ],
            )
            .unwrap();

        let warnings = log_warnings(&store);
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        let fetch = warnings
            .iter()
            .find(|w| w.site.as_deref() == Some("worker.fetch_failed"))
            .unwrap();
        assert_eq!(
            fetch.since_millis,
            Some(NOW - 200_000),
            "since the site's earliest row in the window"
        );
        assert!(fetch.message.contains("fetch failed again"), "{}", fetch.message);
        assert!(!fetch.message.contains("first fetch failed"), "{}", fetch.message);
        assert!(fetch.message.contains("[error=refused]"), "{}", fetch.message);
        let heartbeat = warnings
            .iter()
            .find(|w| w.site.as_deref() == Some("worker.heartbeat_failed"))
            .unwrap();
        assert_eq!(heartbeat.since_millis, Some(NOW - 100_000));
    }

    #[test]
    fn a_site_clears_a_window_after_its_last_log_while_a_fresher_one_stays() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[
                    site_event(0, NOW - WORKER_LOG_WINDOW_MS - 1, "worker.fetch_failed", "old", &[]),
                    site_event(1, NOW - 1_000, "worker.heartbeat_failed", "fresh", &[]),
                ],
            )
            .unwrap();
        let warnings = log_warnings(&store);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert_eq!(warnings[0].site.as_deref(), Some("worker.heartbeat_failed"));
    }

    #[test]
    fn a_stalled_destination_log_is_told_on_destination_unreachable_rather_than_raised_alone() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[
                    site_event(
                        0,
                        NOW - 60_000,
                        "egress.destination_stalled",
                        "a destination is stalled",
                        &[("subject", "search"), ("error", "connection refused")],
                    ),
                    site_event(
                        1,
                        NOW - 30_000,
                        "egress.destination_stalled",
                        "a destination is stalled",
                        &[("subject", "search"), ("error", "503 unavailable")],
                    ),
                ],
            )
            .unwrap();
        let snapshot = vec![egress_thread_health(
            "w1",
            "egress-0",
            0,
            vec![destination_healthy_metric("search", 0.0)],
        )];
        let starts: HashMap<FactKey, i64> = [(
            FactKey::new(DESTINATION_UNREACHABLE, scoped_subject("g", "search")),
            NOW - 90_000,
        )]
        .into();
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[credential("w1", false)],
                starts: &starts,
                heartbeat_interval: INTERVAL,
                series_store: Some(&store),
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );

        assert!(
            warnings.iter().all(|w| w.condition != Condition::WorkerLoggedWarnings),
            "{warnings:?}"
        );
        let unreachable = warnings
            .iter()
            .find(|w| w.condition == Condition::DestinationUnreachable)
            .unwrap();
        assert!(
            unreachable
                .message
                .ends_with(". Last reported by worker w1: 503 unavailable"),
            "the latest row's error: {}",
            unreachable.message
        );
        assert_eq!(unreachable.severity, Severity::Failed);
        assert_eq!(
            unreachable.since_millis,
            Some(NOW - 90_000),
            "the catalog's own start time"
        );
        assert!(
            unreachable.message.contains("all 1 worker in this group"),
            "the spread stays: {}",
            unreachable.message
        );
    }

    /// A catalog warning as its own function would raise it, for [`fold_logged_details`] to tell a log on.
    fn catalog(condition: Condition, subject_kind: SubjectKind, subject_id: &str, group: &str) -> Warning {
        Warning {
            scope: None,
            condition,
            subject_kind,
            subject_id: subject_id.to_string(),
            site: None,
            group: Some(group.to_string()),
            severity: Severity::Failed,
            since_millis: Some(NOW - 90_000),
            message: "it is wrong".to_string(),
        }
    }

    /// A viewer sees the warnings of the groups they hold. One about the leader itself names no group, and only an
    /// administrator sees it.
    #[test]
    fn a_warning_is_seen_by_whoever_holds_its_group() {
        let payments = Access::Areas(BTreeMap::from([("payments".to_string(), Level::Viewer)]));
        let theirs = catalog(Condition::SourceBlocked, SubjectKind::Source, "orders", "payments");
        let elsewhere = catalog(Condition::SourceBlocked, SubjectKind::Source, "orders", "search");
        let leader = Warning {
            group: None,
            ..catalog(Condition::RemoteNotFollowed, SubjectKind::Leader, "leader", "payments")
        };
        assert!(theirs.visible_to(&payments));
        assert!(!elsewhere.visible_to(&payments));
        assert!(!leader.visible_to(&payments));
        assert!(
            [theirs, elsewhere, leader]
                .iter()
                .all(|w| w.visible_to(&Access::Administrator))
        );
    }

    #[test]
    fn each_folded_site_tells_its_detail_on_the_catalog_warning_it_repeats() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[
                    site_event(
                        0,
                        NOW - 50_000,
                        "egress.destination_stalled",
                        "a destination is stalled",
                        &[("subject", "es-prod"), ("error", "connection refused")],
                    ),
                    // No `error` field: the sentence stands in for it.
                    site_event(
                        1,
                        NOW - 30_000,
                        "worker.commit_refused",
                        "a commit will not run",
                        &[("subject", "abc123")],
                    ),
                    // Repeated sites tell nothing, though their subject matches by accident.
                    site_event(
                        2,
                        NOW - 20_000,
                        "runtime.rows_rejected",
                        "a destination refused rows",
                        &[("subject", "es-prod"), ("reason", "mapper_parsing_exception")],
                    ),
                    site_event(
                        3,
                        NOW - 10_000,
                        "runtime.source_blocked",
                        "a source is blocked",
                        &[("subject", "orders"), ("why", "es-prod refuses")],
                    ),
                ],
            )
            .unwrap();
        let logged = recent_logs(Some(&store), &[], NOW);
        let mut out = vec![
            catalog(
                Condition::DestinationUnreachable,
                SubjectKind::Destination,
                "es-prod",
                "g",
            ),
            catalog(Condition::WorkerNotDeployingItsTarget, SubjectKind::Worker, "w1", "g"),
            // The same destination name in another group is another destination.
            catalog(
                Condition::DestinationUnreachable,
                SubjectKind::Destination,
                "es-prod",
                "other",
            ),
            catalog(Condition::DestinationRefusing, SubjectKind::Destination, "es-prod", "g"),
            catalog(Condition::SourceBlocked, SubjectKind::Source, "orders", "g"),
        ];
        fold_logged_details(&mut out, &logged);

        let told: Vec<&str> = out.iter().map(|w| w.message.as_str()).collect();
        assert_eq!(
            told,
            vec![
                "it is wrong. Last reported by worker w1: connection refused",
                "it is wrong. Last reported by worker w1: a commit will not run",
                "it is wrong",
                "it is wrong",
                "it is wrong",
            ]
        );
        assert!(
            out.iter()
                .all(|w| w.severity == Severity::Failed && w.since_millis == Some(NOW - 90_000))
        );
        assert!(
            worker_logged_warnings(&logged).is_empty(),
            "a folded or repeated site raises nothing alone"
        );
    }

    #[test]
    fn a_fold_tells_the_latest_report_across_workers() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        for (worker, at, error) in [("w1", NOW - 20_000, "newer"), ("w2", NOW - 60_000, "older")] {
            store
                .record_logs(
                    worker,
                    1,
                    Some("g"),
                    &[site_event(
                        0,
                        at,
                        "egress.destination_stalled",
                        "stalled",
                        &[("subject", "search"), ("error", error)],
                    )],
                )
                .unwrap();
        }
        let mut out = vec![catalog(
            Condition::DestinationUnreachable,
            SubjectKind::Destination,
            "search",
            "g",
        )];
        fold_logged_details(&mut out, &recent_logs(Some(&store), &[], NOW));
        assert_eq!(out[0].message, "it is wrong. Last reported by worker w1: newer");
    }

    /// One worker stalled on two destinations tells each its own error, and a site it logs about two subjects is
    /// still one warning, the latest of them since the earliest.
    #[test]
    fn one_workers_site_about_two_subjects_folds_onto_each_and_warns_once() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[
                    site_event(
                        0,
                        NOW - 50_000,
                        "egress.destination_stalled",
                        "stalled",
                        &[("subject", "search"), ("error", "search refused")],
                    ),
                    site_event(
                        1,
                        NOW - 20_000,
                        "egress.destination_stalled",
                        "stalled",
                        &[("subject", "archive"), ("error", "archive refused")],
                    ),
                    site_event(2, NOW - 40_000, "kafka.link", "first", &[("subject", "a")]),
                    site_event(3, NOW - 10_000, "kafka.link", "second", &[("subject", "b")]),
                ],
            )
            .unwrap();
        let logged = recent_logs(Some(&store), &[], NOW);
        let mut out = vec![
            catalog(
                Condition::DestinationUnreachable,
                SubjectKind::Destination,
                "search",
                "g",
            ),
            catalog(
                Condition::DestinationUnreachable,
                SubjectKind::Destination,
                "archive",
                "g",
            ),
        ];
        fold_logged_details(&mut out, &logged);
        let told: Vec<&str> = out.iter().map(|w| w.message.as_str()).collect();
        assert_eq!(
            told,
            vec![
                "it is wrong. Last reported by worker w1: search refused",
                "it is wrong. Last reported by worker w1: archive refused",
            ]
        );

        let alone = worker_logged_warnings(&logged);
        assert_eq!(alone.len(), 1, "{alone:?}");
        assert!(alone[0].message.contains("second"), "{}", alone[0].message);
        assert_eq!(alone[0].since_millis, Some(NOW - 40_000));
    }

    #[test]
    fn a_revoked_workers_log_is_not_folded() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        store
            .record_logs(
                "w1",
                1,
                Some("g"),
                &[site_event(
                    0,
                    NOW - 20_000,
                    "egress.destination_stalled",
                    "stalled",
                    &[("subject", "search"), ("error", "refused")],
                )],
            )
            .unwrap();
        let mut out = vec![catalog(
            Condition::DestinationUnreachable,
            SubjectKind::Destination,
            "search",
            "g",
        )];
        fold_logged_details(&mut out, &recent_logs(Some(&store), &[credential("w1", true)], NOW));
        assert_eq!(out[0].message, "it is wrong");
    }

    #[test]
    fn nothing_wrong_is_an_empty_catalog() {
        let healthy = vec![thread_health("w1", "reader-1", 0, vec![])];
        let warnings = derive(
            &Facts {
                snapshot: &healthy,
                credentials: &[credential("w1", false)],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn worker_silent_raises_past_the_threshold_and_is_absent_within_it() {
        let quiet = vec![thread_health("w1", "t0", 10_000, vec![])];
        let warnings = derive(
            &Facts {
                snapshot: &quiet,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].subject_kind, SubjectKind::Worker);
        assert_eq!(warnings[0].subject_id, "w1");
        assert_eq!(warnings[0].severity, Severity::Failed);
        assert_eq!(warnings[0].since_millis, Some(NOW - 10_000));
        assert!(warnings[0].message.contains("w1"));

        let fresh = vec![thread_health("w1", "t0", 500, vec![])];
        assert!(
            derive(
                &Facts {
                    snapshot: &fresh,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty(),
            "within the threshold, silence is absent -- this is the point of derived, never stored"
        );
    }

    #[test]
    fn worker_silent_excludes_a_revoked_worker() {
        let quiet = vec![thread_health("w1", "t0", 10_000, vec![])];
        let warnings = derive(
            &Facts {
                snapshot: &quiet,
                credentials: &[credential("w1", true)],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert!(warnings.is_empty(), "revocation is deliberate, not a failure");
    }

    #[test]
    fn a_worker_on_another_build_is_raised_until_it_matches() {
        let on = |worker_id: &str, version: &str, release: Option<&str>| {
            let mut t = thread_health(worker_id, "reader-0", 0, vec![]);
            t.smeltr_version = version.to_string();
            t.release = release.map(str::to_string);
            t
        };
        let raised = workers_on_another_build(
            &[
                on("w1", "2026.1.1+main.0000000", None),
                on("w2", "2026.1.1+main.1a2b3c4", None),
                on("w3", "", None),
            ],
            "1a2b3c4",
            None,
        );
        assert_eq!(
            raised.iter().map(|w| w.subject_id.as_str()).collect::<Vec<_>>(),
            ["w1", "w3"],
            "the date and branch do not count, and a worker that names no build is on another one"
        );
        assert!(
            raised[0].message.contains("0000000") && raised[0].message.contains("1a2b3c4"),
            "{raised:?}"
        );

        let raised = workers_on_another_build(
            &[
                on("w1", "2026.1.1+main.0000000", Some("1.2.0")),
                on("w2", "2026.1.1+main.2222222", Some("2.0.0")),
                on("w3", "2026.1.1+main.3333333", None),
            ],
            "1a2b3c4",
            Some("1.3.1"),
        );
        assert_eq!(
            raised.iter().map(|w| w.subject_id.as_str()).collect::<Vec<_>>(),
            ["w2", "w3"],
            "a release of the leader's major is on its build, and another major or a dev build is not"
        );
        assert!(
            raised[0].message.contains("2.0.0") && raised[0].message.contains("1.3.1"),
            "{raised:?}"
        );

        let mut stopped = on("w1", "2026.1.1+main.0000000", None);
        stopped.stopped_at_millis = Some(NOW);
        assert!(
            workers_on_another_build(&[stopped], "1a2b3c4", None).is_empty(),
            "a stopped worker is not deploying"
        );
    }

    #[test]
    fn a_worker_on_another_release_it_is_compatible_with_is_info() {
        let on = |worker_id: &str, commit: &str, release: Option<&str>| {
            let mut t = thread_health(worker_id, "reader-0", 0, vec![]);
            t.smeltr_version = format!("2026.1.1+main.{commit}");
            t.release = release.map(str::to_string);
            t
        };
        let fleet = [
            on("w1", "1111111", Some("1.2.0")),
            on("w2", "2222222", Some("1.3.1")),
            on("w3", "3333333", Some("2.0.0")),
            on("w4", "4444444", None),
            on("w5", "5555555", Some("1.4.0-rc1")),
            on("w6", "6666666", Some("1.4.0")),
        ];
        let raised = workers_on_another_release(&fleet, "2222222", Some("1.3.1"));
        assert_eq!(
            raised.iter().map(|w| w.subject_id.as_str()).collect::<Vec<_>>(),
            ["w1", "w6"],
            "older or newer within the major; not the leader's own, another major, a dev build or a pre-release"
        );
        assert!(raised.iter().all(|w| w.severity == Severity::Info), "{raised:?}");
        assert_eq!(raised[0].message, "worker w1 is on 1.2.0, and this leader on 1.3.1");

        assert!(
            workers_on_another_release(&fleet, "2222222", None).is_empty(),
            "a leader that is no release has none to compare"
        );
        let mut stopped = on("w1", "1111111", Some("1.2.0"));
        stopped.stopped_at_millis = Some(NOW);
        assert!(
            workers_on_another_release(&[stopped], "2222222", Some("1.3.1")).is_empty(),
            "a stopped worker runs nothing"
        );
    }

    #[test]
    fn a_newer_release_is_info_about_this_leader_since_it_was_first_seen() {
        let newer = crate::leader::updates::Newer {
            release: "1.3.0".into(),
            url: "https://github.com/smeltr-sh/smeltr/releases/tag/v1.3.0".into(),
            since: 42,
        };
        let w = newer_release(&newer, "1.2.0");
        assert_eq!(
            (w.severity, w.subject_kind, w.since_millis),
            (Severity::Info, SubjectKind::Leader, Some(42))
        );
        assert_eq!(w.group, None, "for administrators only");
        assert_eq!(w.message, "smeltr 1.3.0 is out, and this leader runs 1.2.0");
    }

    #[test]
    fn a_worker_near_its_memory_limit_is_raised() {
        let using = |worker_id: &str, used: u64, limit: Option<u64>| {
            let mut t = thread_health(worker_id, "reader-0", 0, vec![]);
            t.resources = Some(crate::heartbeat::ResourceReport {
                memory_used_bytes: used,
                memory_limit_bytes: limit,
                memory_limited: true,
                ..Default::default()
            });
            t
        };
        const GIB: u64 = 1 << 30;
        let raised = workers_memory_high(
            &[
                using("w1", 9 * GIB, Some(10 * GIB)),
                using("w2", 89 * GIB / 10, Some(10 * GIB)),
                using("w3", GIB, None),
            ],
            INTERVAL,
        );
        assert_eq!(
            raised.iter().map(|w| w.subject_id.as_str()).collect::<Vec<_>>(),
            ["w1"],
            "90% is raised, 89% is not, and no limit is nothing to measure against"
        );
        assert_eq!(raised[0].severity, Severity::Warning);
        assert_eq!(raised[0].group.as_deref(), Some("g"));
        assert!(
            raised[0].message.contains("90%")
                && raised[0].message.contains("9.0 GiB")
                && raised[0].message.contains("10.0 GiB"),
            "{raised:?}"
        );

        let mut stopped = using("w1", GIB, Some(GIB));
        stopped.stopped_at_millis = Some(NOW);
        assert!(
            workers_memory_high(&[stopped], INTERVAL).is_empty(),
            "a stopped worker is not using memory"
        );

        let silent = using("w1", GIB, Some(GIB));
        let silent = ThreadHealth {
            last_seen_millis_ago: stale_after_ms(INTERVAL) as u64 + 1,
            ..silent
        };
        assert!(
            workers_memory_high(&[silent], INTERVAL).is_empty(),
            "a silent worker's last reading is too old to act on"
        );
    }

    /// A worker that said it was stopping and then went quiet did what it said. Its silence is not a failure, and
    /// what its last report says about deploying -- nothing running, a commit found -- is a deployment let go of
    /// on purpose.
    #[test]
    fn a_worker_that_stopped_on_notice_is_neither_silent_nor_behind() {
        let mut stopped = behind("w1", None, "newer", None);
        stopped.last_seen_millis_ago = 60_000;
        stopped.stopped_at_millis = Some(NOW - 60_000);
        let long_ago = HashMap::from([(not_deploying_key("w1", "newer"), NOW - 60 * 60 * 1000)]);
        let derived = |snapshot: &[ThreadHealth]| {
            derive(
                &Facts {
                    snapshot,
                    credentials: &[credential("w1", false)],
                    starts: &long_ago,
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None,
                },
                NOW,
            )
        };

        assert!(derived(std::slice::from_ref(&stopped)).is_empty());
        assert!(
            !currently_active_facts(std::slice::from_ref(&stopped), NOW, INTERVAL, &deployed())
                .contains(&not_deploying_key("w1", "newer")),
            "the clock would age a failure that is not one"
        );

        let failed = ThreadHealth {
            stopped_at_millis: None,
            ..stopped
        };
        let conditions: Vec<Condition> = derived(&[failed]).into_iter().map(|w| w.condition).collect();
        assert_eq!(
            conditions,
            [Condition::WorkerSilent, Condition::WorkerNotDeployingItsTarget],
            "the same worker gone without a word"
        );
    }

    #[test]
    fn enrolled_never_heartbeating_raises_and_clears_once_a_thread_appears() {
        let creds = vec![credential("w2", false)];
        let warnings = derive(
            &Facts {
                snapshot: &[],
                credentials: &creds,
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].subject_kind, SubjectKind::Worker);
        assert_eq!(warnings[0].subject_id, "w2");
        assert_eq!(warnings[0].severity, Severity::Warning);
        assert_eq!(
            warnings[0].since_millis, None,
            "the registry cannot know when this started"
        );

        let now_reporting = vec![thread_health("w2", "t0", 0, vec![])];
        assert!(
            derive(
                &Facts {
                    snapshot: &now_reporting,
                    credentials: &creds,
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty(),
            "absent the moment a thread shows up, even an empty bootstrap one"
        );
    }

    #[test]
    fn enrolled_never_heartbeating_excludes_a_revoked_worker() {
        let warnings = derive(
            &Facts {
                snapshot: &[],
                credentials: &[credential("w3", true)],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn source_held_raises_from_the_reason_text_and_clears_when_it_stops() {
        let held = vec![thread_health(
            "w1",
            "reader-3",
            0,
            vec![blocked_metric(
                "7",
                "destination 2 refused a record and is set to block: bad schema. fix the schema, then it resumes.",
            )],
        )];
        let warnings = derive(
            &Facts {
                snapshot: &held,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 1);
        let w = &warnings[0];
        assert_eq!(w.subject_kind, SubjectKind::Source);
        assert_eq!(w.subject_id, "7");
        assert_eq!(w.severity, Severity::Failed);
        assert!(w.message.contains("destination 2"), "{}", w.message);
        assert!(w.message.contains("fix the schema"), "{}", w.message);

        let resumed = vec![thread_health("w1", "reader-3", 0, vec![])];
        assert!(
            derive(
                &Facts {
                    snapshot: &resumed,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty(),
            "the fact clears the moment the reader stops reporting a reason"
        );
    }

    /// A source blocked on a different cause on each worker raises each cause, each counting its own workers: one
    /// worker waiting does not hide another held.
    #[test]
    fn a_source_blocked_on_two_causes_across_workers_raises_both() {
        let caused = |cause: &str| {
            let mut m = blocked_metric("7", cause);
            m.labels.push(("cause".to_string(), cause.to_string()));
            m
        };
        let health = vec![
            thread_health("w1", "reader-3", 0, vec![caused("waiting")]),
            thread_health("w2", "reader-3", 0, vec![caused("held")]),
        ];
        let w = derive(
            &Facts {
                snapshot: &health,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let got: Vec<_> = w.iter().map(|w| (w.condition, w.message.as_str())).collect();
        assert_eq!(got.len(), 2, "{w:?}");
        assert!(
            got.iter()
                .any(|(c, m)| *c == Condition::SourceBlocked && m.starts_with("held; ")),
            "{got:?}"
        );
        assert!(
            got.iter()
                .any(|(c, m)| *c == Condition::SourceWaiting && m.starts_with("waiting; ")),
            "{got:?}"
        );
    }

    /// Each cause a reader names is its own condition, so a source waiting on a destination reads apart from one a
    /// config change has to clear. A reader that names none is blocked as before.
    #[test]
    fn a_blocked_source_s_condition_is_its_cause() {
        let caused = |cause: Option<&str>| {
            let mut m = blocked_metric("7", "why");
            if let Some(cause) = cause {
                m.labels.push(("cause".to_string(), cause.to_string()));
            }
            let health = vec![thread_health("w1", "reader-3", 0, vec![m])];
            let w = derive(
                &Facts {
                    snapshot: &health,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None,
                },
                NOW,
            );
            assert_eq!(w.len(), 1, "{w:?}");
            (w[0].condition, w[0].severity)
        };
        assert_eq!(caused(None), (Condition::SourceBlocked, Severity::Failed));
        assert_eq!(caused(Some("held")), (Condition::SourceBlocked, Severity::Failed));
        assert_eq!(caused(Some("refused")), (Condition::SourceRefused, Severity::Failed));
        assert_eq!(caused(Some("waiting")), (Condition::SourceWaiting, Severity::Warning));
        assert_eq!(
            caused(Some("a cause from a newer build")),
            (Condition::SourceBlocked, Severity::Failed)
        );
    }

    #[test]
    fn source_helds_since_time_comes_from_the_fact_clock() {
        let held = vec![thread_health("w1", "reader-3", 0, vec![blocked_metric("7", "blocked")])];
        let mut starts = HashMap::new();
        starts.insert(FactKey::new(SOURCE_BLOCKED, scoped_subject("g", "7")), NOW - 240_000);
        let warnings = derive(
            &Facts {
                snapshot: &held,
                credentials: &[],
                starts: &starts,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings[0].since_millis, Some(NOW - 240_000));

        // No entry in the clock (as if this were the fact's first heartbeat) leaves it unknown rather than "now".
        let warnings = derive(
            &Facts {
                snapshot: &held,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings[0].since_millis, None);
    }

    /// The defect E4 exists to fix, at the layer an operator actually reads: which source, and why, in words that
    /// name the fix -- and the recovery half, since a fact that never clears is worse than one that never fired.
    #[test]
    fn source_unreachable_raises_from_the_reason_text_and_clears_when_it_recovers() {
        let unreachable = vec![thread_health(
            "w1",
            "reader-3",
            0,
            vec![unreachable_metric(
                "7",
                "Global error: BrokerTransportFailure: kafka-nowhere.invalid:9092/bootstrap: Failed to resolve \
                 'kafka-nowhere.invalid:9092': Name or service not known",
            )],
        )];
        let warnings = derive(
            &Facts {
                snapshot: &unreachable,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 1);
        let w = &warnings[0];
        assert_eq!(w.condition, Condition::SourceUnreachable);
        assert_eq!(w.subject_kind, SubjectKind::Source);
        assert_eq!(w.subject_id, "7");
        assert_eq!(w.severity, Severity::Failed);
        assert!(w.message.contains("kafka-nowhere.invalid"), "{}", w.message);
        assert!(w.message.contains("firewall"), "{}", w.message);

        // Recovered: the reader stopped reporting the metric (`Reader::connection_error_fact` cleared it), same as
        // `SourceBlocked` clearing once a reader stops reporting a block reason.
        let recovered = vec![thread_health("w1", "reader-3", 0, vec![])];
        assert!(
            derive(
                &Facts {
                    snapshot: &recovered,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty(),
            "the fact clears the moment the reader stops reporting a reason"
        );
    }

    /// A capture waits for the workers holding rows it is shown: a reader whose block keeps records, for a capture of
    /// its source, and an egress thread keeping refused rows, for one at its destination. A source blocked on a
    /// destination holds nothing there, and a worker not expected, in another group, or heard from only by a thread
    /// long gone, is not waited for.
    #[test]
    fn a_capture_waits_for_the_workers_holding_its_rows() {
        const WITHIN: Duration = Duration::from_secs(30);
        let caused = |source: &str, cause: BlockCause| MetricSampleDto {
            labels: vec![
                ("source".to_string(), source.to_string()),
                ("cause".to_string(), cause.name().to_string()),
            ],
            ..blocked_metric(source, "blocked")
        };
        let kept = |destination: &str, rows: f64| MetricSampleDto {
            metric: DESTINATION_HELD_ROWS.to_string(),
            labels: vec![("destination".to_string(), destination.to_string())],
            value: MetricValueDto::Gauge(rows),
        };
        let snapshot = vec![
            thread_health("held", "reader-0", 0, vec![caused("orders", BlockCause::Held)]),
            thread_health("unnamed", "reader-0", 0, vec![blocked_metric("orders", "blocked")]),
            thread_health("refused", "reader-0", 0, vec![caused("orders", BlockCause::Refused)]),
            thread_health("other", "reader-0", 0, vec![caused("audit", BlockCause::Held)]),
            thread_health("keeps", "egress-0", 0, vec![kept("search", 3.0)]),
            thread_health("drained", "egress-0", 0, vec![kept("search", 0.0)]),
            thread_health("unexpected", "reader-0", 0, vec![caused("orders", BlockCause::Held)]),
            thread_health_in(
                "h",
                "elsewhere",
                "reader-0",
                0,
                vec![caused("orders", BlockCause::Held)],
            ),
            thread_health("gone", "reader-1", 60_000, vec![caused("orders", BlockCause::Held)]),
        ];
        let expected: HashSet<String> = [
            "held",
            "unnamed",
            "refused",
            "other",
            "keeps",
            "drained",
            "elsewhere",
            "gone",
        ]
        .map(String::from)
        .into();
        let mut source: Vec<String> = holding_for_capture(&snapshot, "g", &expected, WITHIN, Some("orders"), None)
            .into_iter()
            .collect();
        source.sort();
        assert_eq!(source, ["held", "unnamed"]);
        let destination = holding_for_capture(&snapshot, "g", &expected, WITHIN, None, Some("search"));
        assert_eq!(destination, HashSet::from(["keeps".to_string()]));
    }

    /// An idle source -- polling and finding nothing, `SOURCE_BLOCKED_REASON`'s absence already proves this is not
    /// `SourceBlocked` -- must not read as `SourceUnreachable` either: the whole point is telling the two apart.
    #[test]
    fn an_idle_source_raises_no_unreachable_warning() {
        let idle = vec![thread_health(
            "w1",
            "reader-3",
            0,
            vec![MetricSampleDto {
                metric: "smeltr_tick_idle_polls_total".to_string(),
                labels: vec![("source".to_string(), "7".to_string())],
                value: MetricValueDto::Counter(40),
            }],
        )];
        assert!(
            derive(
                &Facts {
                    snapshot: &idle,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty()
        );
    }

    #[test]
    fn source_unreachables_since_time_comes_from_the_fact_clock() {
        let unreachable = vec![thread_health(
            "w1",
            "reader-3",
            0,
            vec![unreachable_metric("7", "down")],
        )];
        let mut starts = HashMap::new();
        starts.insert(FactKey::new(SOURCE_UNREACHABLE, scoped_subject("g", "7")), NOW - 60_000);
        let warnings = derive(
            &Facts {
                snapshot: &unreachable,
                credentials: &[],
                starts: &starts,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings[0].since_millis, Some(NOW - 60_000));

        let warnings = derive(
            &Facts {
                snapshot: &unreachable,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings[0].since_millis, None);
    }

    /// A source can be blocked by a destination and unreachable at once -- two different facts about the same
    /// subject, each its own warning, neither masking the other.
    #[test]
    fn source_held_and_source_unreachable_are_independent_warnings_on_the_same_source() {
        let both = vec![thread_health(
            "w1",
            "reader-3",
            0,
            vec![blocked_metric("7", "blocked"), unreachable_metric("7", "down")],
        )];
        let warnings = derive(
            &Facts {
                snapshot: &both,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 2);
        assert!(warnings.iter().any(|w| w.condition == Condition::SourceBlocked));
        assert!(warnings.iter().any(|w| w.condition == Condition::SourceUnreachable));
    }

    /// The defect a destination that has lost its connection leaves, at the layer an operator reads: which
    /// destination, and the recovery half, since a fact that never clears is worse than one that never fired.
    #[test]
    fn destination_unreachable_raises_when_unhealthy_and_clears_when_it_recovers() {
        let down = vec![egress_thread_health(
            "w1",
            "egress-2",
            0,
            vec![destination_healthy_metric("2", 0.0)],
        )];
        let warnings = derive(
            &Facts {
                snapshot: &down,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 1);
        let w = &warnings[0];
        assert_eq!(w.condition, Condition::DestinationUnreachable);
        assert_eq!(w.subject_kind, SubjectKind::Destination);
        assert_eq!(w.subject_id, "2");
        assert_eq!(w.group.as_deref(), Some("g"));
        assert_eq!(w.severity, Severity::Failed);
        assert!(w.message.contains("destination 2 cannot be reached"), "{}", w.message);

        // Recovered: the egress thread reports 1.0 again, same as `clear_stall` does on the next success.
        let recovered = vec![egress_thread_health(
            "w1",
            "egress-2",
            0,
            vec![destination_healthy_metric("2", 1.0)],
        )];
        assert!(
            derive(
                &Facts {
                    snapshot: &recovered,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty(),
            "the fact clears the moment the gauge reports healthy again"
        );
    }

    /// The registry never evicts: a thread that stops heartbeating (moved groups, redeployed without this
    /// destination, or simply stopped) leaves its last-reported 0.0 behind forever. Past `stale_after` that row is
    /// not this worker's current say, the same bound `running_deployments` applies -- without it, a warning would
    /// outlive every worker that could ever clear it.
    #[test]
    fn a_stale_threads_reported_unhealthy_destination_does_not_raise() {
        let stale_ago = stale_after_ms(INTERVAL) as u64 + 1;
        let frozen = vec![egress_thread_health(
            "w1",
            "egress-2",
            stale_ago,
            vec![destination_healthy_metric("2", 0.0)],
        )];
        // The worker itself going silent is its own, correct warning (`WorkerSilent`); what must not also appear
        // is a `DestinationUnreachable` extrapolated from a row nothing has refreshed in just as long.
        assert!(
            !derive(
                &Facts {
                    snapshot: &frozen,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .iter()
            .any(|w| w.condition == Condition::DestinationUnreachable),
            "a frozen row from a thread that has gone silent is not a current fact"
        );
    }

    /// A redeploy that drops a destination leaves the registry's last gauge for its old thread id owed;
    /// `worker::thread_reports` carries it forward under a perfectly current heartbeat, but with no kind, since
    /// nothing running claims that thread id any more (`RuntimeReport::followed_by`). Staleness alone would not
    /// catch this -- the row's age is zero -- so this must not raise either.
    #[test]
    fn an_orphaned_leftover_gauge_with_no_kind_does_not_raise() {
        let mut orphan = thread_health("w1", "egress-2", 0, vec![destination_healthy_metric("2", 0.0)]);
        orphan.report.kind = None;
        assert!(
            !derive(
                &Facts {
                    snapshot: &[orphan],
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .iter()
            .any(|w| w.condition == Condition::DestinationUnreachable),
            "a leftover carried under a thread id nothing runs is not this worker's current say"
        );
    }

    #[test]
    fn destination_unreachables_since_time_comes_from_the_fact_clock() {
        let down = vec![egress_thread_health(
            "w1",
            "egress-2",
            0,
            vec![destination_healthy_metric("2", 0.0)],
        )];
        let mut starts = HashMap::new();
        starts.insert(
            FactKey::new(DESTINATION_UNREACHABLE, scoped_subject("g", "2")),
            NOW - 90_000,
        );
        let warnings = derive(
            &Facts {
                snapshot: &down,
                credentials: &[],
                starts: &starts,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings[0].since_millis, Some(NOW - 90_000));

        let warnings = derive(
            &Facts {
                snapshot: &down,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings[0].since_millis, None);
    }

    /// Two workers sharing one destination, only one of them cut off from it: the message has to say so rather than
    /// reading as the broker itself being down.
    #[test]
    fn destination_unreachables_message_names_a_subset_of_the_groups_workers() {
        let snapshot = vec![
            egress_thread_health("w1", "egress-2", 0, vec![destination_healthy_metric("2", 0.0)]),
            egress_thread_health("w2", "egress-2", 0, vec![destination_healthy_metric("2", 1.0)]),
        ];
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let w = warnings
            .iter()
            .find(|w| w.condition == Condition::DestinationUnreachable)
            .unwrap();
        assert!(w.message.contains("1 of 2 workers"), "{}", w.message);
    }

    /// A worker gone stale must not inflate the denominator either: two workers on paper, one of them not
    /// currently anything, reads as the one live worker's own complete outage -- not a partial one that points
    /// at particular hosts over the broker itself.
    #[test]
    fn a_stale_worker_does_not_inflate_the_spread_denominator() {
        let stale_ago = stale_after_ms(INTERVAL) as u64 + 1;
        let snapshot = vec![
            egress_thread_health("w1", "egress-2", 0, vec![destination_healthy_metric("2", 0.0)]),
            egress_thread_health("w2", "egress-2", stale_ago, vec![destination_healthy_metric("2", 1.0)]),
        ];
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let w = warnings
            .iter()
            .find(|w| w.condition == Condition::DestinationUnreachable)
            .unwrap();
        assert!(w.message.contains("all 1 worker"), "{}", w.message);
    }

    /// A destination can be refusing records and unreachable at once -- two different facts about the same
    /// subject, each its own warning, neither masking the other.
    #[test]
    fn destination_refusing_and_destination_unreachable_are_independent_warnings_on_the_same_destination() {
        let both = vec![egress_thread_health(
            "w1",
            "egress-2",
            0,
            vec![
                refusal_metric("2", NOW - 100, "429"),
                destination_healthy_metric("2", 0.0),
            ],
        )];
        let warnings = derive(
            &Facts {
                snapshot: &both,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 2);
        assert!(warnings.iter().any(|w| w.condition == Condition::DestinationRefusing));
        assert!(
            warnings
                .iter()
                .any(|w| w.condition == Condition::DestinationUnreachable)
        );
    }

    #[test]
    fn destination_refusing_raises_when_fresh_and_is_absent_once_stale() {
        let fresh = vec![thread_health(
            "w1",
            "egress-2",
            0,
            vec![refusal_metric("2", NOW - 500, "429 es_rejected_execution_exception")],
        )];
        let warnings = derive(
            &Facts {
                snapshot: &fresh,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].subject_kind, SubjectKind::Destination);
        assert_eq!(warnings[0].subject_id, "2");
        assert_eq!(warnings[0].severity, Severity::Failed);
        assert!(warnings[0].message.contains("429 es_rejected_execution_exception"));

        let stale_at = NOW - stale_after_ms(INTERVAL) - 1;
        let old = vec![thread_health(
            "w1",
            "egress-2",
            0,
            vec![refusal_metric("2", stale_at, "429 ...")],
        )];
        assert!(
            derive(
                &Facts {
                    snapshot: &old,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty(),
            "the ring keeps history; a warning is not a log of what once happened"
        );
    }

    #[test]
    fn destination_refusing_prefers_the_freshest_of_several_threads() {
        let snapshot = vec![
            thread_health(
                "w1",
                "egress-2",
                0,
                vec![refusal_metric("2", NOW - 5_000, "old wording")],
            ),
            thread_health("w2", "egress-2", 0, vec![refusal_metric("2", NOW - 100, "new wording")]),
        ];
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(
            warnings.len(),
            1,
            "one destination is one warning, however many workers dispatch to it"
        );
        assert!(warnings[0].message.contains("new wording"));
    }

    #[test]
    fn lag_climbing_stays_silent_on_too_few_samples_to_judge() {
        // Three rises, one short of LAG_CLIMB_SAMPLES.
        let store = store_with_lag(&[100.0, 200.0, 300.0]);
        assert!(lag_climbing(Some(&store)).is_empty());
    }

    #[test]
    fn lag_climbing_raises_once_it_climbs_for_enough_straight_samples() {
        let store = store_with_lag(&[100.0, 200.0, 300.0, 400.0]);
        let warnings = lag_climbing(Some(&store));
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].condition, Condition::LagClimbing);
        assert_eq!(warnings[0].subject_kind, SubjectKind::Source);
        assert_eq!(warnings[0].subject_id, "7");
        assert_eq!(warnings[0].group.as_deref(), Some("g"));
        assert_eq!(warnings[0].severity, Severity::Warning);
        assert_eq!(
            warnings[0].since_millis,
            Some(0),
            "the run started at the very first sample"
        );
    }

    /// Rows source 7 in group "g" read in each of `buckets` ten-second buckets, from 0.
    fn read_rows(store: &RegistryStore, per_bucket: f64, buckets: i64) {
        let key = serde_json::to_string(&[("group", "g"), ("source", "7")]).unwrap();
        for i in 0..buckets {
            store
                .record_delta(super::super::ROWS_IN_METRIC, &key, i * 10_000, per_bucket)
                .unwrap();
        }
    }

    /// The false alarm this gate is for: 35 records a second, lag rising 24 to 33, a quarter of a second behind.
    #[test]
    fn lag_climbing_stays_silent_on_a_climb_worth_under_a_minute_of_reading() {
        let store = store_with_lag(&[24.0, 27.0, 30.0, 33.0]);
        read_rows(&store, 350.0, 4);
        assert!(lag_climbing(Some(&store)).is_empty());
    }

    #[test]
    fn lag_climbing_raises_once_the_climb_is_over_a_minute_of_reading() {
        // One record a second read, 400 behind.
        let store = store_with_lag(&[100.0, 200.0, 300.0, 400.0]);
        read_rows(&store, 10.0, 4);
        let warnings = lag_climbing(Some(&store));
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].message.contains("about 6 min behind"),
            "{}",
            warnings[0].message
        );
    }

    /// Messages source 7 in group "g" finished in each of `buckets` ten-second buckets, from 0.
    fn finish_messages(store: &RegistryStore, per_bucket: f64, buckets: i64) {
        let key = serde_json::to_string(&[("group", "g"), ("source", "7")]).unwrap();
        for i in 0..buckets {
            store
                .record_delta(MESSAGES_FINISHED, &key, i * 10_000, per_bucket)
                .unwrap();
        }
    }

    /// A queue's lag counts messages, each an object of many rows: it drains at the messages it finishes, not at the
    /// rows it reads, which would hide its climb.
    #[test]
    fn a_queues_climb_is_judged_by_the_messages_it_finishes() {
        let store = store_with_lag(&[100.0, 200.0, 300.0, 400.0]);
        read_rows(&store, 100_000.0, 4);
        finish_messages(&store, 10.0, 4);
        let warnings = lag_climbing(Some(&store));
        assert_eq!(warnings.len(), 1, "one message a second, 400 behind");
        let msg = &warnings[0].message;
        assert!(msg.contains("source 7 queue orders"), "{msg}");
        assert!(msg.contains("about 6 min behind"), "{msg}");

        let store = store_with_lag(&[24.0, 27.0, 30.0, 33.0]);
        finish_messages(&store, 350.0, 4);
        assert!(
            lag_climbing(Some(&store)).is_empty(),
            "35 messages a second, a second behind"
        );

        // Stuck on its first object: many rows read, no message finished yet.
        let store = store_with_lag(&[24.0, 27.0, 30.0, 33.0]);
        read_rows(&store, 100_000.0, 4);
        finish_messages(&store, 0.0, 4);
        let warnings = lag_climbing(Some(&store));
        assert_eq!(
            warnings.len(),
            1,
            "a queue is judged by messages before it finishes one"
        );
        assert!(
            warnings[0].message.contains("with nothing finished meanwhile"),
            "{}",
            warnings[0].message
        );
    }

    #[test]
    fn lag_climbing_is_absent_once_a_sample_no_longer_rises() {
        let flat = store_with_lag(&[100.0, 200.0, 300.0, 300.0]);
        assert!(lag_climbing(Some(&flat)).is_empty(), "holding steady is not climbing");

        let falls = store_with_lag(&[100.0, 200.0, 300.0, 250.0]);
        assert!(
            lag_climbing(Some(&falls)).is_empty(),
            "catching up clears it immediately"
        );
    }

    #[test]
    fn lag_climbings_since_time_is_the_runs_true_start_not_just_the_trailing_window() {
        // Six straight rises: LAG_CLIMB_SAMPLES only requires four, but the start is the whole run's.
        let store = store_with_lag(&[10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
        let warnings = lag_climbing(Some(&store));
        assert_eq!(warnings[0].since_millis, Some(0));
    }

    #[test]
    fn lag_climbing_only_counts_the_run_since_the_last_dip() {
        // Dips at bucket 30_000 (300 -> 250), then rises for four straight samples from there.
        let store = store_with_lag(&[100.0, 200.0, 300.0, 250.0, 260.0, 270.0, 280.0]);
        let warnings = lag_climbing(Some(&store));
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].since_millis, Some(30_000), "the dip restarts the run");
    }

    #[test]
    fn lag_climbing_judges_each_partition_on_its_own() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        let climbing = lag_subject_key("g", "7", "orders", "0");
        let steady = lag_subject_key("g", "7", "orders", "1");
        for (i, v) in [100.0, 200.0, 300.0, 400.0].into_iter().enumerate() {
            store
                .record_gauge_max(SOURCE_LAG, &climbing, i as i64 * 10_000, v)
                .unwrap();
        }
        for (i, v) in [50.0, 50.0, 50.0, 50.0].into_iter().enumerate() {
            store
                .record_gauge_max(SOURCE_LAG, &steady, i as i64 * 10_000, v)
                .unwrap();
        }

        let warnings = lag_climbing(Some(&store));
        assert_eq!(warnings.len(), 1, "only the climbing partition raises");
        assert!(warnings[0].message.contains("partition 0"), "{}", warnings[0].message);
    }

    #[test]
    fn lag_climbings_sentence_names_the_source_the_partition_the_group_and_the_fix() {
        let store = store_with_lag(&[100.0, 200.0, 300.0, 400.0]);
        let warnings = lag_climbing(Some(&store));
        let msg = &warnings[0].message;
        assert!(msg.contains("source 7"), "{msg}");
        assert!(msg.contains("partition 0"), "{msg}");
        assert!(msg.contains("orders"), "{msg}");
        assert!(msg.contains("\"g\""), "{msg}");
        assert!(msg.contains("reader capacity") || msg.contains("reduce"), "{msg}");
    }

    /// The defect this fixes: two groups running a source of the same id used to share one lag series, so a climb
    /// in one was indistinguishable from a climb in the other.
    #[test]
    fn two_groups_reporting_lag_for_the_same_source_id_are_two_series_and_two_warnings() {
        let g1 = store_with_lag_in("g1", &[100.0, 200.0, 300.0, 400.0]);
        // Same subject id ("7", "orders", "0"), a different group: independent series, so seed it directly rather
        // than reopening a store `store_with_lag_in` already committed to one group.
        let key = lag_subject_key("g2", "7", "orders", "0");
        for (i, v) in [50.0, 40.0, 30.0, 20.0].into_iter().enumerate() {
            g1.record_gauge_max(SOURCE_LAG, &key, i as i64 * 10_000, v).unwrap();
        }

        let warnings = lag_climbing(Some(&g1));
        assert_eq!(
            warnings.len(),
            1,
            "g2's lag is falling, not climbing -- only g1's series raises: {warnings:?}"
        );
        assert_eq!(warnings[0].group.as_deref(), Some("g1"));
    }

    #[test]
    fn derive_folds_lag_climbing_into_the_catalog() {
        let store = store_with_lag(&[100.0, 200.0, 300.0, 400.0]);
        let warnings = derive(
            &Facts {
                snapshot: &[],
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: Some(&store),
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].condition, Condition::LagClimbing);
    }

    #[test]
    fn group_not_deployed_raises_for_a_worker_whose_group_main_holds_no_subtree_for() {
        let heartbeating = vec![thread_health("w1", "t0", 0, vec![])];
        let warnings = derive(
            &Facts {
                snapshot: &heartbeating,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &HashSet::new(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings.len(), 1);
        let w = &warnings[0];
        assert_eq!(w.condition, Condition::GroupNotDeployed);
        assert_eq!(w.subject_kind, SubjectKind::Worker);
        assert_eq!(w.subject_id, "w1");
        assert_eq!(
            w.group.as_deref(),
            Some("g"),
            "the group the worker reported, not the worker itself"
        );
        assert_eq!(w.severity, Severity::Warning);
        assert!(w.message.contains("\"g\""), "{}", w.message);
    }

    #[test]
    fn group_not_deployed_is_absent_once_main_holds_the_groups_subtree() {
        let heartbeating = vec![thread_health("w1", "t0", 0, vec![])];
        assert!(
            derive(
                &Facts {
                    snapshot: &heartbeating,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty(),
            "the worker is fine -- the group it reports is what main lacks"
        );
    }

    #[test]
    fn group_not_deployed_since_time_comes_from_the_fact_clock() {
        let heartbeating = vec![thread_health("w1", "t0", 0, vec![])];
        let mut starts = HashMap::new();
        starts.insert(FactKey::new(GROUP_NOT_DEPLOYED, "w1"), NOW - 60_000);
        let warnings = derive(
            &Facts {
                snapshot: &heartbeating,
                credentials: &[],
                starts: &starts,
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &HashSet::new(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        assert_eq!(warnings[0].since_millis, Some(NOW - 60_000));
    }

    #[test]
    fn currently_active_facts_includes_group_not_deployed() {
        let heartbeating = vec![thread_health("w1", "t0", 0, vec![])];
        let active = currently_active_facts(&heartbeating, NOW, INTERVAL, &HashSet::new());
        assert!(active.contains(&FactKey::new(GROUP_NOT_DEPLOYED, "w1")));

        let active = currently_active_facts(&heartbeating, NOW, INTERVAL, &deployed());
        assert!(
            !active.contains(&FactKey::new(GROUP_NOT_DEPLOYED, "w1")),
            "a deployed group's fact clears the moment main holds it, same as source_blocked and destination_refusing"
        );
    }

    #[test]
    fn a_warning_serializes_to_the_wire_spelling() {
        let w = Warning {
            scope: None,
            condition: Condition::SourceBlocked,
            subject_kind: SubjectKind::Source,
            site: None,
            subject_id: "7".to_string(),
            group: Some("g".to_string()),
            severity: Severity::Failed,
            since_millis: Some(1234),
            message: "destination 2 refused a record".to_string(),
        };
        let json = serde_json::to_string(&w).unwrap();
        assert_eq!(
            json,
            r#"{"condition":"sourceBlocked","subjectKind":"source","subjectId":"7","group":"g","severity":"failed","sinceMillis":1234,"message":"destination 2 refused a record"}"#
        );

        let no_group_or_start = Warning {
            group: None,
            since_millis: None,
            ..w
        };
        let json = serde_json::to_string(&no_group_or_start).unwrap();
        assert!(json.contains(r#""group":null"#), "{json}");
        assert!(json.contains(r#""sinceMillis":null"#), "{json}");
    }

    // -- group scoping (defect fix: a source or destination id is unique only within its group) ------------------

    /// Two groups running a source of the same id must not collapse into one warning: source ids are unique only
    /// within a group plus `shared/`, and `or_insert_with` used to keep whichever worker's report sorted first.
    #[test]
    fn two_groups_running_a_source_of_the_same_id_are_two_separate_warnings() {
        let snapshot = vec![
            thread_health_in("g1", "w1", "reader-3", 0, vec![blocked_metric("7", "blocked in g1")]),
            thread_health_in("g2", "w2", "reader-3", 0, vec![blocked_metric("7", "blocked in g2")]),
        ];
        let deployed = HashSet::from(["g1".to_string(), "g2".to_string()]);
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed,
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let held: Vec<&Warning> = warnings
            .iter()
            .filter(|w| w.condition == Condition::SourceBlocked)
            .collect();

        assert_eq!(held.len(), 2, "one source id, two groups, two facts: {held:?}");
        assert!(
            held.iter()
                .any(|w| w.group.as_deref() == Some("g1") && w.message.contains("blocked in g1"))
        );
        assert!(
            held.iter()
                .any(|w| w.group.as_deref() == Some("g2") && w.message.contains("blocked in g2"))
        );
    }

    /// A `shared/` source runs in every group that draws on it; a worker reports the group it serves, not where the
    /// config was authored, so the same source id blocked in one group is not blocked in another that also runs it.
    #[test]
    fn a_shared_source_held_in_one_group_does_not_raise_in_another() {
        let snapshot = vec![
            thread_health_in("g1", "w1", "reader-3", 0, vec![blocked_metric("shared-src", "blocked")]),
            thread_health_in("g2", "w2", "reader-3", 0, vec![]), // g2 runs it too, but it is fine there
        ];
        let deployed = HashSet::from(["g1".to_string(), "g2".to_string()]);
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed,
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let held: Vec<&Warning> = warnings
            .iter()
            .filter(|w| w.condition == Condition::SourceBlocked)
            .collect();

        assert_eq!(held.len(), 1, "only the group it is actually blocked in: {held:?}");
        assert_eq!(held[0].group.as_deref(), Some("g1"));
    }

    /// Only some of a group's workers currently unreachable points at those hosts, not the broker -- the message
    /// has to say so.
    #[test]
    fn source_unreachables_message_names_a_subset_of_the_groups_workers() {
        fn idle_poll(source_id: &str) -> MetricSampleDto {
            MetricSampleDto {
                metric: "smeltr_tick_idle_polls_total".to_string(),
                labels: vec![("source".to_string(), source_id.to_string())],
                value: MetricValueDto::Counter(3),
            }
        }
        let snapshot = vec![
            thread_health("w1", "reader-3", 0, vec![unreachable_metric("7", "down")]),
            thread_health("w2", "reader-3", 0, vec![idle_poll("7")]),
            thread_health("w3", "reader-3", 0, vec![idle_poll("7")]),
        ];
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let w = warnings
            .iter()
            .find(|w| w.condition == Condition::SourceUnreachable)
            .unwrap();
        assert!(w.message.contains("1 of 3 workers"), "{}", w.message);
    }

    /// Every one of a group's workers reporting the same fact says so plainly too, rather than leaving "how many"
    /// for the reader to count.
    #[test]
    fn destination_refusings_message_says_when_every_worker_reports_it() {
        let snapshot = vec![
            thread_health("w1", "egress-2", 0, vec![refusal_metric("2", NOW - 100, "429")]),
            thread_health("w2", "egress-2", 0, vec![refusal_metric("2", NOW - 100, "429")]),
        ];
        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );
        let w = warnings
            .iter()
            .find(|w| w.condition == Condition::DestinationRefusing)
            .unwrap();
        assert!(w.message.contains("all 2 workers"), "{}", w.message);
    }

    /// The constraint that keeps a capture out of the fleet's alarms. A cold capture opens a throwaway consumer
    /// against a source nothing is deployed to read, on a session with its own deadline -- so its trouble is the
    /// author's to see in the capture panel, not a condition about a production source. That holds today because
    /// the catalog reads four metric names and all four are written by `Reader`, which a cold capture bypasses.
    /// Nothing enforces that but this test: give the cold path a fact channel and the catalog starts raising
    /// warnings about sources no bundle names.
    #[test]
    fn a_captures_own_samples_raise_nothing_in_the_catalog() {
        let from_capture = MetricSampleDto {
            metric: "smeltr_capture_records_total".to_string(),
            labels: vec![("source".to_string(), "orders-west".to_string())],
            value: MetricValueDto::Counter(40),
        };
        let snapshot = vec![thread_health("w1", "capture", 0, vec![from_capture])];

        let warnings = derive(
            &Facts {
                snapshot: &snapshot,
                credentials: &[],
                starts: &HashMap::new(),
                heartbeat_interval: INTERVAL,
                series_store: None,
                deployed_groups: &deployed(),
                signing: None,
                rotating: &Rotating::None,
                codes: &[],
                presenting: &settled(),
                deployed_certificates: &[],
                held: None,
                remote: None,
                newer: None,
            },
            NOW,
        );

        assert!(
            warnings.is_empty(),
            "a capture's own counters are not facts about a production source: {warnings:?}"
        );

        // The control, so the emptiness above means something: the same thread carrying a name the catalog does
        // read raises against the source it names. What keeps a capture quiet is which names it writes, nothing else.
        let as_a_fact = vec![thread_health(
            "w1",
            "capture",
            0,
            vec![unreachable_metric("orders-west", "no brokers")],
        )];
        assert!(
            !derive(
                &Facts {
                    snapshot: &as_a_fact,
                    credentials: &[],
                    starts: &HashMap::new(),
                    heartbeat_interval: INTERVAL,
                    series_store: None,
                    deployed_groups: &deployed(),
                    signing: None,
                    rotating: &Rotating::None,
                    codes: &[],
                    presenting: &settled(),
                    deployed_certificates: &[],
                    held: None,
                    remote: None,
                    newer: None
                },
                NOW
            )
            .is_empty(),
            "the harness raises when the name is one the catalog reads"
        );
    }

    fn code(id: &str, fingerprint: &str) -> crate::leader::store::UsableEnrollmentCode {
        crate::leader::store::UsableEnrollmentCode {
            id: id.to_string(),
            group_id: String::new(),
            fingerprint: fingerprint.to_string(),
        }
    }

    /// An enrollment code names its root inside the value an operator pastes, so a rotation dooms the ones naming the
    /// root it is leaving. Said while it is open, which is the whole window in which anything can be done about it:
    /// spent before the cutover, or minted again after.
    #[test]
    fn an_enrollment_code_naming_the_outgoing_root_is_said_while_the_rotation_is_open() {
        let doomed = code("ec_outgoing", OUTGOING);
        let fine = code("ec_incoming", "the root being moved to");
        // From before some earlier authority change: it brings nobody in already, and this rotation is not what
        // an operator should be told to act on about it. The Enrollment codes page calls this one replaced.
        let already_dead = code("ec_older", "a root left behind long ago");

        let said = enrollment_code_names_outgoing_authority(
            &[doomed.clone(), fine.clone(), already_dead.clone()],
            &under_way(),
        );
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0].subject_id, "ec_outgoing");
        assert_eq!(said[0].subject_kind, SubjectKind::EnrollmentCode);
        assert!(
            said[0].message.contains("withdrawn when the rotation finishes"),
            "the operator is not told what becomes of it: {}",
            said[0].message
        );

        assert!(
            enrollment_code_names_outgoing_authority(&[doomed, fine, already_dead], &settled()).is_empty(),
            "an enrollment code is only doomed by a rotation, and none is open"
        );
    }

    /// Every enrollment code reads as doomed against a root this leader could not read, and a fleet of warnings naming
    /// enrollment codes that are fine is how an operator learns to ignore the one that is not.
    #[test]
    fn a_rotation_this_leader_cannot_read_dooms_no_enrollment_code() {
        let any = code("ec_any", OUTGOING);
        assert!(
            enrollment_code_names_outgoing_authority(&[any], &crate::leader::enrollment_codes::Presenting::Unreadable)
                .is_empty()
        );
    }

    /// A rotation that cannot finish un-silences this warning, and the remedy it names has to be one the leader
    /// will accept: starting another rotation is refused while this one is open.
    #[test]
    fn a_stuck_rotation_is_told_what_it_can_actually_do() {
        let dying = authority(3650, 10);
        let said = authority_aging(
            Some(&dying),
            &Rotating::Stuck("the incoming authority's private key is missing.".to_string()),
            NOW,
        );
        assert_eq!(said.len(), 1, "a stuck rotation said nothing");
        assert!(
            said[0].message.contains("abandon the rotation"),
            "the remedy is one the leader refuses: {}",
            said[0].message
        );
        assert!(
            said[0].message.contains("private key is missing"),
            "the operator is not told what is stuck: {}",
            said[0].message
        );
        assert!(
            authority_aging(Some(&dying), &Rotating::UnderWay, NOW).is_empty(),
            "a rotation that can still finish is an answer already coming"
        );
    }

    fn standing(key: crate::leader::store::KeyState, unopenable: Vec<Unopenable>) -> KeyStanding {
        KeyStanding {
            path: std::path::PathBuf::from("/var/lib/smeltr-keys/sealing.key"),
            key,
            unopenable,
        }
    }

    /// Issue 21: a leader whose key is gone says so once, as a failure about itself, and names the fix.
    #[test]
    fn a_missing_encryption_key_is_one_failure_that_names_the_fix() {
        let broken = standing(
            crate::leader::store::KeyState::Missing,
            vec![
                Unopenable::Secret("es-auth".to_string()),
                Unopenable::Setting(crate::leader::remote::TOKEN_KEY.to_string()),
            ],
        );
        let facts = encryption_key_facts(&broken);
        let starts: HashMap<FactKey, i64> = facts.iter().map(|f| (f.clone(), NOW)).collect();
        let said = encryption_key_warnings(&broken, &Default::default(), &starts);
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0].condition, Condition::SecretsKeyUnusable);
        assert_eq!(said[0].severity, Severity::Failed);
        assert_eq!(said[0].subject_kind, SubjectKind::Leader);
        assert_eq!(said[0].since_millis, Some(NOW));
        assert!(
            said[0]
                .message
                .contains("is missing, so the 2 stored secrets cannot be opened"),
            "{}",
            said[0].message
        );
        assert!(
            said[0].message.contains("Secrets, Encryption key"),
            "{}",
            said[0].message
        );
    }

    /// After a regeneration each value to enter again is its own warning, named where it is entered.
    #[test]
    fn each_value_a_regenerated_key_cannot_open_is_its_own_warning() {
        let held = standing(
            crate::leader::store::KeyState::Held {
                key_id: "5be1a09c44d7f312".to_string(),
            },
            vec![
                Unopenable::Secret("es-auth".to_string()),
                Unopenable::Setting(crate::leader::oidc::CLIENT_SECRET_KEY.to_string()),
                Unopenable::Certificate("ingest-tls".to_string()),
                Unopenable::Certificate("receiver-tls".to_string()),
                Unopenable::Certificate("acme".to_string()),
            ],
        );
        let issued = crate::leader::store::Kept::Issued(crate::leader::store::Issued {
            names: vec!["receiver.example.com".to_string()],
            validity_days: 365,
        });
        let kept = [
            ("receiver-tls".to_string(), issued),
            ("acme".to_string(), crate::leader::store::Kept::Acme),
        ]
        .into_iter()
        .collect();
        let said = encryption_key_warnings(&held, &kept, &HashMap::new());
        assert_eq!(said.len(), 5);
        assert!(
            said.iter()
                .all(|w| w.condition == Condition::SecretNeedsValue && w.severity == Severity::Warning)
        );
        assert_eq!(
            (said[0].subject_kind, said[0].subject_id.as_str()),
            (SubjectKind::Secret, "es-auth")
        );
        assert!(said[0].message.ends_with("Store a new version"));
        assert_eq!(
            (said[1].subject_kind, said[1].subject_id.as_str()),
            (SubjectKind::Leader, "Single sign-on client secret")
        );
        assert!(
            said[1]
                .message
                .ends_with("Enter it again under Accounts, Single sign-on"),
            "{}",
            said[1].message
        );
        assert_eq!(
            (said[2].subject_kind, said[2].subject_id.as_str()),
            (SubjectKind::Certificate, "ingest-tls")
        );
        assert!(
            said[2].message.ends_with("Replace it in Admin → Certificates"),
            "{}",
            said[2].message
        );
        assert!(
            said[3]
                .message
                .contains("The leader issues a new version within the hour"),
            "{}",
            said[3].message
        );
        assert!(
            said[4]
                .message
                .ends_with("Order it again in Admin → System → Leader certificate"),
            "{}",
            said[4].message
        );
        assert!(said.iter().all(|w| w.group.is_none()), "an administrator's to act on");

        assert!(
            encryption_key_warnings(
                &standing(crate::leader::store::KeyState::Absent, vec![]),
                &Default::default(),
                &HashMap::new()
            )
            .is_empty()
        );
    }
}
