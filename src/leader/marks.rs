//! What an editor said about a source, as marks: lower the warning on its rows that holds to info; reset its history,
//! so its normal is learned again from now; say a past period of it was normal or an outage (`throughput`); pick the
//! preset it is judged at, and the one its senders are; and say which of its fields name its senders, and lower a
//! sender's warning to info (`senders`).
//!
//! A lowered warning stays on the Issues page for everyone, marked as info, and the views that judge health leave
//! it out. It is named by when it began, so it stays lowered until it ends, and the next one is a warning again.
//! Nothing is learned from the time it held.
//!
//! A source whose history was reset learns its normal only from what it read after the reset, so it has none, and
//! does not warn, until it has [`super::throughput::MIN_DAYS`] days of that.
//!
//! A period said to be normal is learned from though it warned, and one said to be an outage is not learned from though
//! it did not warn. The count widths and the gap learn from the same periods.
//!
//! All are kept in one setting, with who said each.

use axum::Json;
use axum::extract::{Path, State};
use serde::{Deserialize, Serialize};

use super::AppState;
use super::envelope::{Empty, Envelope, Refusal};
use super::senders::Sender;
use super::series::YEAR_MS;
use super::store::{Actor, Store};
use super::throughput::{self, Learning, Preset};
use super::warnings::{self, Condition, SubjectKind};
use crate::heartbeat::SenderFields;

/// Where the marks are kept, as JSON.
pub const MARKS_KEY: &str = "warnings.source_rows_marks";

/// Every mark that still says something.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Marks {
    #[serde(default)]
    pub lowered: Vec<Lowered>,
    #[serde(default)]
    pub reset: Vec<Reset>,
    #[serde(default)]
    pub periods: Vec<Period>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<SourceFields>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub presets: Vec<Picked>,
}

/// A source's rows warning that began at `since_millis`, or one of its sender's, lowered to info by `by`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lowered {
    pub group: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<Sender>,
    pub since_millis: i64,
    pub by: String,
}

/// One source's sender fields as an editor changed them: `edits` as its group's workers are sent them, and which fields
/// stay watched once new ones are not watched.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceFields {
    /// Absent from the body of [`put_sender_fields`], which names it in its path.
    #[serde(default)]
    pub group: String,
    #[serde(flatten)]
    pub edits: SenderFields,
    /// Once an editor stops watching new fields, the fields that stay watched. Any other field is new, except one an
    /// editor added; a new field is counted but not watched until an editor checks it. `None` while new fields are
    /// watched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locked: Option<Vec<String>>,
}

impl SourceFields {
    /// Whether `field` is watched: an editor did not uncheck it, and it is not new.
    pub fn watches(&self, field: &str) -> bool {
        !self.unchecked(field) && !self.is_new(field)
    }

    /// Whether `field` came after an editor stopped watching new fields, and nobody checked or added it since.
    pub fn is_new(&self, field: &str) -> bool {
        let named = |fields: &[String]| fields.iter().any(|f| f == field);
        self.locked
            .as_deref()
            .is_some_and(|kept| !named(kept) && !named(&self.edits.added) && !self.unchecked(field))
    }

    fn unchecked(&self, field: &str) -> bool {
        self.edits.unchecked.iter().any(|f| f == field)
    }
}

/// The preset `by` picked for a source, or for its `senders`. Only one other than the default is kept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Picked {
    pub group: String,
    pub source: String,
    pub senders: bool,
    pub preset: Preset,
    pub by: String,
}

/// The preset `group`'s `source`, or its `senders`, is judged at, as `presets` were picked.
pub fn picked(presets: &[Picked], group: &str, source: &str, senders: bool) -> Preset {
    presets
        .iter()
        .find(|p| p.group == group && p.source == source && p.senders == senders)
        .map_or(unpicked(senders), |p| p.preset)
}

/// The preset of a source, or of its `senders`, until an editor picks another.
fn unpicked(senders: bool) -> Preset {
    if senders { Preset::SENDERS } else { Preset::ROWS }
}

/// A source whose normal `by` said to learn again from `from_millis` on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reset {
    pub group: String,
    pub source: String,
    pub from_millis: i64,
    pub by: String,
}

/// A past period of a source an editor said was normal or an outage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Period {
    pub group: String,
    pub source: String,
    pub from_millis: i64,
    pub to_millis: i64,
    pub was: Was,
    pub by: String,
}

/// What an editor said a past period of a source was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Was {
    Normal,
    Outage,
}

impl Marks {
    /// The marks as stored. None when they cannot be read, which is logged: every warning is then a warning, and
    /// every normal learned from all it can be.
    pub fn read(store: &Store) -> Marks {
        let text = match store.setting(MARKS_KEY) {
            Ok(text) => text,
            Err(e) => {
                tracing::warn!(name: "marks_unreadable", target: "status", error = %e, "could not read which rows warnings were lowered or had their history reset, so none is");
                return Marks::default();
            }
        };
        text.and_then(|text| {
            serde_json::from_str(&text)
                .inspect_err(|e| {
                    tracing::warn!(name: "marks_unreadable", target: "status", error = %e, "the stored marks on rows warnings are not readable, so none is applied");
                })
                .ok()
        })
        .unwrap_or_default()
    }

    /// Who lowered `group`'s `source`'s rows warning, or its `sender`'s, that began at `since_millis`, if anyone did.
    pub fn lowered_by(
        &self,
        group: &str,
        source: &str,
        sender: Option<&Sender>,
        since_millis: Option<i64>,
    ) -> Option<&str> {
        self.lowered
            .iter()
            .find(|l| {
                l.group == group
                    && l.source == source
                    && l.sender.as_ref() == sender
                    && Some(l.since_millis) == since_millis
            })
            .map(|l| l.by.as_str())
    }

    /// What an editor changed of the sender fields of each of `group`'s sources.
    pub fn sender_fields(&self, group: &str) -> Vec<SenderFields> {
        self.fields
            .iter()
            .filter(|f| f.group == group)
            .map(|f| f.edits.clone())
            .collect()
    }
}

/// When `group`'s `source`'s rows warning that holds began, or its `sender`'s, if one holds.
fn holding_since(
    state: &AppState,
    group: &str,
    source: &str,
    sender: Option<&Sender>,
    now_ms: i64,
) -> Result<Option<i64>, Refusal> {
    let Some(store) = state.registry.series() else {
        return Ok(None);
    };
    let spans = match sender {
        Some(sender) => warnings::sender_spans(store, group, source, sender, now_ms)?,
        None => warnings::spans(store, SubjectKind::Source, group, source, now_ms)?,
    };
    Ok(spans
        .into_iter()
        .find(|s| {
            s.ended_millis.is_none() && matches!(s.condition, Condition::SourceRowsBelow | Condition::SendersBelow)
        })
        .map(|s| s.started_millis))
}

/// Held while the marks are read, changed and written, so two marks made at once are both kept.
static CHANGING: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// One source an editor marks, or one of its senders, by the route that marks it.
struct Marking {
    group: String,
    source: String,
    sender: Option<Sender>,
    /// The audit's name for what was done.
    action: &'static str,
}

impl Marking {
    /// Makes `change` to the stored marks, given when the source's rows warning that holds began, and writes them
    /// with who made it, leaving out every lowered warning that no longer holds. Off the async runtime.
    async fn write(
        self,
        state: AppState,
        actor: Option<axum::Extension<Actor>>,
        change: impl FnOnce(&mut Marks, &Marking, Option<i64>, &Actor, i64) -> Result<(), Refusal> + Send + 'static,
    ) -> Envelope {
        let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
        let written = super::blocking(move || -> Result<(), Refusal> {
            let _changing = CHANGING.lock().unwrap_or_else(|e| e.into_inner());
            let now_ms = warnings::now_millis();
            let mut marks = Marks::read(&state.store);
            let since = holding_since(&state, &self.group, &self.source, self.sender.as_ref(), now_ms)?;
            change(&mut marks, &self, since, &actor, now_ms)?;
            let mut kept = Vec::new();
            for l in marks.lowered {
                if holding_since(&state, &l.group, &l.source, l.sender.as_ref(), now_ms)? == Some(l.since_millis) {
                    kept.push(l);
                }
            }
            marks.lowered = kept;
            let target = match &self.sender {
                Some(sender) => format!("{}/{}/{}={}", self.group, self.source, sender.field, sender.value),
                None => format!("{}/{}", self.group, self.source),
            };
            let event = super::accounts::event_for(&actor, self.action, &target, "ok");
            let text = serde_json::to_string(&marks).expect("marks always serialize");
            Ok(state.store.put_settings(&[(MARKS_KEY, text)], Some(&event))?)
        })
        .await;
        match written {
            Ok(()) => Envelope::done(),
            Err(refusal) => Envelope::refused(refusal),
        }
    }
}

/// `POST /api/areas/:area/sources/:id/lower`: lowers the rows warning that holds for the source to info until it
/// ends. Refused when none holds.
pub async fn lower(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        sender: None,
        action: "source-rows-lower",
    };
    marking.write(state, actor, lowered).await
}

/// `POST /api/areas/:area/sources/:id/senders/lower`: lowers the warning that holds for one sender of the source to
/// info until it ends. Refused when none holds.
pub async fn lower_sender(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
    Json(sender): Json<Sender>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        sender: Some(sender),
        action: "sender-rows-lower",
    };
    marking.write(state, actor, lowered).await
}

/// Lowers the warning about what `m` marks that began at `since` to info, by `actor`.
fn lowered(marks: &mut Marks, m: &Marking, since: Option<i64>, actor: &Actor, _: i64) -> Result<(), Refusal> {
    let Some(since) = since else {
        let what = match &m.sender {
            Some(sender) => format!("sender {} {} of source {}", sender.field, sender.value, m.source),
            None => format!("source {}", m.source),
        };
        return Err(Refusal::not_found(format!(
            "{what} in {} has no warning to lower",
            m.group
        )));
    };
    if marks
        .lowered_by(&m.group, &m.source, m.sender.as_ref(), Some(since))
        .is_none()
    {
        marks.lowered.push(Lowered {
            group: m.group.clone(),
            source: m.source.clone(),
            sender: m.sender.clone(),
            since_millis: since,
            by: actor.display_name.clone(),
        });
    }
    Ok(())
}

/// The most fields an editor may add to one source, and stop watching or keep watched.
const ADDED_FIELDS: usize = 32;
const LISTED_FIELDS: usize = 256;

/// `PUT /api/areas/:area/sources/:id/senders/fields`: the source's sender fields an editor unchecked, added and,
/// once new fields are not watched, kept watched, whole. Its workers count them from their next heartbeat. Refused
/// for an added field that is not a field name.
pub async fn put_sender_fields(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
    Json(fields): Json<SourceFields>,
) -> Envelope<Empty> {
    let SourceFields { edits, locked, .. } = fields;
    let marking = Marking {
        group,
        source,
        sender: None,
        action: "sender-fields",
    };
    marking
        .write(state, actor, move |marks, m, _, _, _| {
            if edits.added.len() > ADDED_FIELDS {
                return Err(Refusal::invalid(format!(
                    "source {} in {}: at most {ADDED_FIELDS} fields can be added",
                    m.source, m.group
                )));
            }
            if edits.unchecked.len() > LISTED_FIELDS {
                return Err(Refusal::invalid(format!(
                    "source {} in {}: at most {LISTED_FIELDS} fields can be left unwatched",
                    m.source, m.group
                )));
            }
            if locked.as_ref().is_some_and(|kept| kept.len() > LISTED_FIELDS) {
                return Err(Refusal::invalid(format!(
                    "source {} in {}: at most {LISTED_FIELDS} fields can be kept watched",
                    m.source, m.group
                )));
            }
            let longest = crate::heartbeat::STREAM_NAME_LEN;
            if edits
                .added
                .iter()
                .chain(&edits.unchecked)
                .chain(locked.iter().flatten())
                .any(|name| name.len() > longest)
            {
                return Err(Refusal::invalid(format!(
                    "source {} in {}: a field's name is at most {longest} bytes",
                    m.source, m.group
                )));
            }
            for name in &edits.added {
                crate::plan::tree::parse_name(name).map_err(|e| {
                    Refusal::invalid(format!(
                        "source {} in {}: {name:?} is not a field name: {e}",
                        m.source, m.group
                    ))
                })?;
            }
            marks
                .fields
                .retain(|f| (&f.group, &f.edits.source) != (&m.group, &m.source));
            if !edits.unchecked.is_empty() || !edits.added.is_empty() || locked.is_some() {
                marks.fields.push(SourceFields {
                    group: m.group.clone(),
                    edits: SenderFields {
                        source: m.source.clone(),
                        ..edits
                    },
                    locked,
                });
            }
            Ok(())
        })
        .await
}

/// `POST /api/areas/:area/sources/:id/reset-history`: resets the source's history, so its normal is learned again
/// from now.
pub async fn reset_history(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        sender: None,
        action: "source-rows-reset-history",
    };
    marking
        .write(state, actor, |marks, m, _, actor, now_ms| {
            // Only the last reset says anything: it forgets more than every one before it.
            marks.reset.retain(|r| (&r.group, &r.source) != (&m.group, &m.source));
            marks.reset.push(Reset {
                group: m.group.clone(),
                source: m.source.clone(),
                from_millis: now_ms,
                by: actor.display_name.clone(),
            });
            Ok(())
        })
        .await
}

/// A preset to pick.
#[derive(Debug, Clone, Deserialize)]
pub struct Picking {
    pub preset: Preset,
}

/// `PUT /api/areas/:area/sources/:id/preset`: picks the preset the source is judged at.
pub async fn put_preset(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
    Json(picking): Json<Picking>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        sender: None,
        action: "source-rows-preset",
    };
    marking.write(state, actor, pick(picking.preset, false)).await
}

/// `PUT /api/areas/:area/sources/:id/senders/preset`: picks the preset the source's senders are judged at.
pub async fn put_senders_preset(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
    Json(picking): Json<Picking>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        sender: None,
        action: "sender-rows-preset",
    };
    marking.write(state, actor, pick(picking.preset, true)).await
}

/// Picks `preset` for what is marked, or for its `senders`, in place of the one picked before.
fn pick(
    preset: Preset,
    senders: bool,
) -> impl FnOnce(&mut Marks, &Marking, Option<i64>, &Actor, i64) -> Result<(), Refusal> + Send + 'static {
    move |marks, m, _, actor, _| {
        marks
            .presets
            .retain(|p| (&p.group, &p.source, p.senders) != (&m.group, &m.source, senders));
        if preset != unpicked(senders) {
            marks.presets.push(Picked {
                group: m.group.clone(),
                source: m.source.clone(),
                senders,
                preset,
                by: actor.display_name.clone(),
            });
        }
        Ok(())
    }
}

/// A past period of a source, and what to mark it.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Marked {
    pub from_millis: i64,
    pub to_millis: i64,
    pub was: Was,
}

/// `POST /api/areas/:area/sources/:id/mark`: says a past period of the source was normal or an outage, in place of
/// what was said about that same period. Refused for a period that is empty or not over.
pub async fn mark(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
    Json(marked): Json<Marked>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        sender: None,
        action: "source-rows-mark",
    };
    marking
        .write(state, actor, move |marks, m, _, actor, now_ms| {
            if marked.from_millis >= marked.to_millis || marked.to_millis > now_ms {
                return Err(Refusal::invalid(format!(
                    "source {} in {}: a period to mark must end after it begins, and must be over",
                    m.source, m.group
                )));
            }
            // A period past a year is past what any normal learns from.
            marks
                .periods
                .retain(|p| p.to_millis >= now_ms - YEAR_MS && !p.is(&m.group, &m.source, &marked));
            marks.periods.push(Period {
                group: m.group.clone(),
                source: m.source.clone(),
                from_millis: marked.from_millis,
                to_millis: marked.to_millis,
                was: marked.was,
                by: actor.display_name.clone(),
            });
            Ok(())
        })
        .await
}

impl Period {
    /// Whether this is `group`'s `source`'s period `marked` names.
    fn is(&self, group: &str, source: &str, marked: &Marked) -> bool {
        (
            self.group.as_str(),
            self.source.as_str(),
            self.from_millis,
            self.to_millis,
        ) == (group, source, marked.from_millis, marked.to_millis)
    }
}

/// `GET /api/areas/:area/sources/:id/past`: the source's warnings and quiet spells of the past year, for an editor to
/// mark ([`throughput::past`]).
pub async fn past(
    State(state): State<AppState>,
    Path((group, source)): Path<(String, String)>,
) -> Envelope<throughput::Past> {
    super::blocking_answer(move || {
        let learning = Learning::read(&state.store);
        let Some(store) = state.registry.series() else {
            return Envelope::ok(throughput::Past {
                preset: learning.preset(&group, &source),
                ..throughput::Past::default()
            });
        };
        throughput::past(store, &group, &source, warnings::now_millis(), &learning).into()
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lowered_warning_is_named_by_when_it_began() {
        let marks = Marks {
            lowered: vec![Lowered {
                group: "g".into(),
                source: "s".into(),
                sender: None,
                since_millis: 5,
                by: "John".into(),
            }],
            ..Marks::default()
        };
        assert_eq!(marks.lowered_by("g", "s", None, Some(5)), Some("John"));
        assert_eq!(
            marks.lowered_by("g", "s", None, Some(6)),
            None,
            "the next one is a warning again"
        );
        assert_eq!(marks.lowered_by("g", "other", None, Some(5)), None);
        assert_eq!(marks.lowered_by("g", "s", None, None), None);
        let fw = Sender {
            field: "devname".into(),
            value: "fw-01".into(),
        };
        assert_eq!(
            marks.lowered_by("g", "s", Some(&fw), Some(5)),
            None,
            "a sender's is its own"
        );
    }

    #[test]
    fn a_groups_workers_are_sent_only_its_own_sources_fields() {
        let edited = |group: &str, source: &str| SourceFields {
            group: group.into(),
            edits: SenderFields {
                source: source.into(),
                unchecked: vec!["action".into()],
                added: Vec::new(),
            },
            locked: None,
        };
        let marks = Marks {
            fields: vec![edited("g", "s"), edited("other", "t")],
            ..Marks::default()
        };
        assert_eq!(marks.sender_fields("g"), [edited("g", "s").edits]);
    }

    /// Once an editor stops watching new fields, a field they neither kept nor added is new, and not watched; one
    /// they unchecked stays unchecked.
    #[test]
    fn a_field_that_comes_after_new_ones_stop_being_watched_is_new() {
        let fields = SourceFields {
            group: "g".into(),
            edits: SenderFields {
                source: "s".into(),
                unchecked: vec!["action".into()],
                added: vec!["policyid".into()],
            },
            locked: Some(vec!["devname".into()]),
        };
        assert!(fields.watches("devname") && fields.watches("policyid"));
        assert!(!fields.watches("action") && !fields.is_new("action"));
        assert!(!fields.watches("user") && fields.is_new("user"));
        let open = SourceFields { locked: None, ..fields };
        assert!(open.watches("user") && !open.is_new("user"));
    }

    /// A preset picked replaces the one picked before, and the default is not kept. A source's and its senders' are
    /// each their own.
    #[test]
    fn a_picked_preset_replaces_the_one_before_and_the_default_is_not_kept() {
        let mut marks = Marks::default();
        let m = Marking {
            group: "g".into(),
            source: "s".into(),
            sender: None,
            action: "source-rows-preset",
        };
        let mut at = |preset, senders| pick(preset, senders)(&mut marks, &m, None, &Actor::system(), 0).unwrap();
        at(Preset::Fast, false);
        at(Preset::Quiet, false);
        at(Preset::Sensitive, true);
        assert_eq!(picked(&marks.presets, "g", "s", false), Preset::Quiet);
        assert_eq!(picked(&marks.presets, "g", "s", true), Preset::Sensitive);
        assert_eq!(picked(&marks.presets, "g", "other", false), Preset::ROWS);
        assert_eq!(picked(&marks.presets, "g", "other", true), Preset::SENDERS);
        let mut at = |preset, senders| pick(preset, senders)(&mut marks, &m, None, &Actor::system(), 0).unwrap();
        at(Preset::ROWS, false);
        at(Preset::SENDERS, true);
        assert_eq!(marks.presets, []);
    }

    #[test]
    fn marks_read_back_as_written_and_unreadable_ones_as_none() {
        let dir = std::env::temp_dir().join(format!("smeltr-marks-{}", std::process::id()));
        let store = crate::leader::store::Store::open(std::path::Path::new(":memory:"), &dir.join("k")).unwrap();
        assert_eq!(Marks::read(&store), Marks::default());
        let marks = Marks {
            reset: vec![Reset {
                group: "g".into(),
                source: "s".into(),
                from_millis: 7,
                by: "John".into(),
            }],
            periods: vec![Period {
                group: "g".into(),
                source: "s".into(),
                from_millis: 3,
                to_millis: 5,
                was: Was::Outage,
                by: "John".into(),
            }],
            ..Marks::default()
        };
        store
            .put_settings(&[(MARKS_KEY, serde_json::to_string(&marks).unwrap())], None)
            .unwrap();
        assert_eq!(Marks::read(&store), marks);
        store.put_settings(&[(MARKS_KEY, "not json".into())], None).unwrap();
        assert_eq!(Marks::read(&store), Marks::default());
    }
}
