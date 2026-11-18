//! What an editor said about a source, as marks: answer what one of its below-normal warnings, or one of its
//! sender's, was (`throughput`, `senders`); reset its history, so its normal is learned again from now; pick the preset
//! it is judged at, and the one its senders are; and say which of its fields name its senders.
//!
//! An answer names a warning by when it began ([`Mark`]), so every page that shows the warning answers the same one.
//! A warning nobody answered stays a warning, and nothing is learned from the time it held. One answered Outage is
//! info, and stays on the Issues page for everyone until it ends; nothing is learned from it either. One answered
//! Normal is info too, and is learned whole, by the count widths and the gap alike, so it ends once its level is. A
//! source's warning answered Normal is learned by its senders too.
//!
//! A source whose history was reset learns its normal only from what it read after the reset, so it has none, and
//! does not warn, until it has [`super::throughput::JUDGED_AFTER_MS`] of that.
//!
//! All are kept in one setting, with who said each.

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};

use super::AppState;
use super::envelope::{Empty, Envelope, Refusal};
use super::senders::Sender;
use super::series::YEAR_MS;
use super::store::{Actor, Store};
use super::throughput::{self, Preset, Presets};
use super::warnings::{self, Condition, SubjectKind};
use crate::heartbeat::SenderFields;

/// Where the marks are kept, as JSON.
pub const MARKS_KEY: &str = "warnings.source_rows_marks";

/// Every mark that still says something.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Marks {
    /// What an editor answered each below-normal warning was.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub answers: Vec<Mark>,
    /// The warnings a leader before answers lowered to info, each read as answered Outage ([`Marks::read`]).
    #[serde(default, skip_serializing)]
    pub(super) lowered: Vec<Mark>,
    #[serde(default)]
    pub reset: Vec<Reset>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<SourceFields>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub presets: Vec<Picked>,
}

/// What `by` answered a source's rows warning that began at `since_millis`, or one of its sender's, was.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mark {
    pub group: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<Sender>,
    pub since_millis: i64,
    /// Absent from a warning lowered before answers, which was an outage.
    #[serde(default)]
    pub was: Was,
    pub by: String,
}

/// The answer to `group`'s `source`'s rows warning, or its `sender`'s, that began at `since_millis`, among `answers`.
pub(super) fn answer<'a>(
    answers: &'a [Mark],
    group: &str,
    source: &str,
    sender: Option<&Sender>,
    since_millis: Option<i64>,
) -> Option<&'a Mark> {
    answers.iter().find(|m| {
        m.group == group && m.source == source && m.sender.as_ref() == sender && Some(m.since_millis) == since_millis
    })
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

/// The presets `by` last picked for a source and its senders. Only those other than the default are kept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Picked {
    pub group: String,
    pub source: String,
    #[serde(flatten)]
    pub presets: Presets,
    pub by: String,
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

/// What an editor answered a below-normal warning was.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Was {
    Normal,
    #[default]
    Outage,
}

impl Marks {
    /// The marks as stored, a warning lowered before answers as answered Outage. None when they cannot be read, which
    /// is logged: every warning is then a warning, and every normal learned from all it can be.
    pub fn read(store: &Store) -> Marks {
        let text = match store.setting(MARKS_KEY) {
            Ok(text) => text,
            Err(e) => {
                tracing::warn!(name: "marks_unreadable", target: "status", error = %e, "could not read which rows warnings were answered or had their history reset, so none is");
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
        .map(|mut marks: Marks| {
            let lowered = std::mem::take(&mut marks.lowered);
            marks.answers.extend(lowered);
            marks
        })
        .unwrap_or_default()
    }

    /// The answer to `group`'s `source`'s rows warning, or its `sender`'s, that began at `since_millis`, if an editor
    /// gave one.
    pub fn answer(
        &self,
        group: &str,
        source: &str,
        sender: Option<&Sender>,
        since_millis: Option<i64>,
    ) -> Option<&Mark> {
        answer(&self.answers, group, source, sender, since_millis)
    }

    /// Whether an editor answered `group`'s `source`'s below-normal warning, or its `sender`'s, that began at
    /// `since_millis` was normal.
    pub fn said_normal(&self, group: &str, source: &str, sender: Option<&Sender>, since_millis: i64) -> bool {
        self.answer(group, source, sender, Some(since_millis))
            .is_some_and(|m| m.was == Was::Normal)
    }

    /// When `group`'s `source` was last said to learn again from, if it was.
    pub fn reset_from(&self, group: &str, source: &str) -> Option<i64> {
        self.reset
            .iter()
            .filter(|r| r.group == group && r.source == source)
            .map(|r| r.from_millis)
            .max()
    }

    /// The presets `group`'s `source`, and its senders, are judged at.
    pub fn presets_of(&self, group: &str, source: &str) -> Presets {
        self.presets
            .iter()
            .find(|p| p.group == group && p.source == source)
            .map_or_else(Presets::default, |p| p.presets)
    }

    /// Whether `field` of `group`'s `source` is not watched, as an editor left it ([`SourceFields::watches`]).
    pub fn unchecked(&self, group: &str, source: &str, field: &str) -> bool {
        self.fields
            .iter()
            .any(|f| f.group == group && f.edits.source == source && !f.watches(field))
    }

    /// Picks `picking` for `group`'s `source`, by `by`, keeping the preset it does not name.
    fn pick(&mut self, group: &str, source: &str, picking: Picking, by: &str) {
        let was = self.presets_of(group, source);
        let presets = Presets {
            rows: picking.rows.unwrap_or(was.rows),
            senders: picking.senders.unwrap_or(was.senders),
        };
        self.presets
            .retain(|p| (p.group.as_str(), p.source.as_str()) != (group, source));
        if presets != Presets::default() {
            self.presets.push(Picked {
                group: group.into(),
                source: source.into(),
                presets,
                by: by.into(),
            });
        }
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

/// `group`'s `source`'s below-normal warnings, or its `sender`'s, that held at or after `since_ms`.
fn below(
    state: &AppState,
    group: &str,
    source: &str,
    sender: Option<&Sender>,
    since_ms: i64,
) -> Result<Vec<warnings::WarningSpan>, Refusal> {
    let Some(store) = state.registry.series() else {
        return Ok(Vec::new());
    };
    let spans = match sender {
        Some(sender) => warnings::sender_spans(store, group, source, sender, since_ms)?,
        None => warnings::spans(store, SubjectKind::Source, group, source, since_ms)?,
    };
    Ok(spans
        .into_iter()
        .filter(|s| matches!(s.condition, Condition::SourceRowsBelow | Condition::SendersBelow))
        .collect())
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
    /// What it names, as a refusal says it.
    fn what(&self) -> String {
        match &self.sender {
            Some(sender) => format!("sender {} {} of source {}", sender.field, sender.value, self.source),
            None => format!("source {}", self.source),
        }
    }

    /// Makes `change` to the stored marks and writes them with who made it. Off the async runtime.
    async fn write(
        self,
        state: AppState,
        actor: Option<axum::Extension<Actor>>,
        change: impl FnOnce(&mut Marks, &Marking, &Actor, i64) -> Result<(), Refusal> + Send + 'static,
    ) -> Envelope {
        let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
        let written = super::blocking(move || -> Result<(), Refusal> {
            let _changing = CHANGING.lock().unwrap_or_else(|e| e.into_inner());
            let now_ms = warnings::now_millis();
            let mut marks = Marks::read(&state.store);
            change(&mut marks, &self, &actor, now_ms)?;
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
        .write(state, actor, move |marks, m, _, _| {
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
        .write(state, actor, |marks, m, actor, now_ms| {
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

/// The preset to pick for a source, for its senders, or for both.
#[derive(Debug, Clone, Deserialize)]
pub struct Picking {
    pub rows: Option<Preset>,
    pub senders: Option<Preset>,
}

/// `PUT /api/areas/:area/sources/:id/preset`: picks the preset the source is judged at, the one its senders are, or
/// both. The one not named stays as it was.
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
        action: "source-preset",
    };
    marking
        .write(state, actor, move |marks, m, actor, _| {
            marks.pick(&m.group, &m.source, picking, &actor.display_name);
            Ok(())
        })
        .await
}

/// A below-normal warning of a source, by when it began, and what an editor answered it was.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Marked {
    pub since_millis: i64,
    pub was: Was,
}

/// A below-normal warning of one sender of a source, and what an editor answered it was.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderMarked {
    #[serde(flatten)]
    pub sender: Sender,
    #[serde(flatten)]
    pub marked: Marked,
}

/// `POST /api/areas/:area/sources/:id/mark`: answers what one of the source's below-normal warnings of the past year
/// was, in place of any answer before. Refused when none began then.
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
    marking.write_mark(state, actor, marked).await
}

/// `POST /api/areas/:area/sources/:id/senders/mark`: [`mark`] for one sender of the source.
pub async fn mark_sender(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
    Json(SenderMarked { sender, marked }): Json<SenderMarked>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        sender: Some(sender),
        action: "sender-rows-mark",
    };
    marking.write_mark(state, actor, marked).await
}

impl Marking {
    /// Writes `marked`, once it names one of the warnings of the past year of what this marks.
    async fn write_mark(self, state: AppState, actor: Option<axum::Extension<Actor>>, marked: Marked) -> Envelope {
        let read = state.clone();
        self.write(state, actor, move |marks, m, actor, now_ms| {
            let began = below(&read, &m.group, &m.source, m.sender.as_ref(), now_ms - YEAR_MS)?
                .iter()
                .any(|s| s.started_millis == marked.since_millis);
            if !began {
                return Err(Refusal::not_found(format!(
                    "{} in {} has no warning of the past year that began then",
                    m.what(),
                    m.group
                )));
            }
            marks.said(m, &marked, actor, now_ms);
            Ok(())
        })
        .await
    }
}

impl Marks {
    /// Answers the warning `marked` names of what `m` names, by `actor`, in place of any answer before.
    fn said(&mut self, m: &Marking, marked: &Marked, actor: &Actor, now_ms: i64) {
        // A warning past a year is past what any normal learns from, and long over.
        self.answers.retain(|n| {
            n.since_millis >= now_ms - YEAR_MS
                && (&n.group, &n.source, &n.sender, n.since_millis)
                    != (&m.group, &m.source, &m.sender, marked.since_millis)
        });
        self.answers.push(Mark {
            group: m.group.clone(),
            source: m.source.clone(),
            sender: m.sender.clone(),
            since_millis: marked.since_millis,
            was: marked.was,
            by: actor.display_name.clone(),
        });
    }
}

/// `GET /api/areas/:area/sources/:id/past`: the source's warnings and quiet spells of the past year, for an editor to
/// mark ([`throughput::past`]).
pub async fn past(
    State(state): State<AppState>,
    Path((group, source)): Path<(String, String)>,
) -> Envelope<throughput::Past> {
    past_of(state, group, source, None).await
}

/// `GET /api/areas/:area/sources/:id/senders/past?field=&value=`: [`past`] for one sender of the source.
pub async fn past_sender(
    State(state): State<AppState>,
    Path((group, source)): Path<(String, String)>,
    Query(sender): Query<Sender>,
) -> Envelope<throughput::Past> {
    past_of(state, group, source, Some(sender)).await
}

/// What [`chart`] and [`chart_sender`] are asked: a span, and for a sender, which.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartQuery {
    pub from_millis: i64,
    pub to_millis: i64,
}

/// `GET /api/areas/:area/sources/:id/chart?fromMillis=&toMillis=`: the source's rows against its range over a span,
/// as History draws it around a warning ([`throughput::chart`]).
pub async fn chart(
    State(state): State<AppState>,
    Path((group, source)): Path<(String, String)>,
    Query(asked): Query<ChartQuery>,
) -> Envelope<throughput::Chart> {
    chart_of(state, group, source, None, asked).await
}

/// `GET /api/areas/:area/sources/:id/senders/chart?field=&value=&fromMillis=&toMillis=`: [`chart`] for one sender of
/// the source.
pub async fn chart_sender(
    State(state): State<AppState>,
    Path((group, source)): Path<(String, String)>,
    Query(sender): Query<Sender>,
    Query(asked): Query<ChartQuery>,
) -> Envelope<throughput::Chart> {
    chart_of(state, group, source, Some(sender), asked).await
}

async fn chart_of(
    state: AppState,
    group: String,
    source: String,
    sender: Option<Sender>,
    asked: ChartQuery,
) -> Envelope<throughput::Chart> {
    super::blocking_answer(move || {
        let Some(store) = state.registry.series() else {
            return Envelope::ok(throughput::Chart::default());
        };
        let learning = Marks::read(&state.store);
        let now_ms = warnings::now_millis();
        throughput::chart(
            store,
            &group,
            &source,
            sender.as_ref(),
            asked.from_millis,
            asked.to_millis,
            now_ms,
            &learning,
        )
        .into()
    })
    .await
}

async fn past_of(state: AppState, group: String, source: String, sender: Option<Sender>) -> Envelope<throughput::Past> {
    super::blocking_answer(move || {
        let learning = Marks::read(&state.store);
        let Some(store) = state.registry.series() else {
            return Envelope::ok(throughput::Past {
                preset: learning.presets_of(&group, &source).of(sender.as_ref()),
                ..throughput::Past::default()
            });
        };
        let answer = || -> Result<throughput::Past, Refusal> {
            let now_ms = warnings::now_millis();
            let mut past = throughput::past(store, &group, &source, sender.as_ref(), now_ms, &learning)?;
            // At Off the gap never warns, so there is no limit to show.
            if sender.is_none() && learning.presets_of(&group, &source).rows.warns() {
                past.quiet_limit_millis = state
                    .throughput
                    .learned()
                    .and_then(|l| l.quiet_limit_ms(&group, &source));
            }
            Ok(past)
        };
        answer().into()
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An answer names one warning of one subject, by when it began, and takes the place of the answer before it.
    #[test]
    fn an_answer_names_one_warning_and_replaces_the_one_before() {
        let fw = Sender {
            field: "devname".into(),
            value: "fw-01".into(),
        };
        let of = |sender: Option<Sender>| Marking {
            group: "g".into(),
            source: "s".into(),
            sender,
            action: "test",
        };
        let marked = |since_millis, was| Marked { since_millis, was };
        let now = YEAR_MS;
        let mut marks = Marks::default();
        let john = Actor::system();
        let was =
            |marks: &Marks, sender: Option<&Sender>, since| marks.answer("g", "s", sender, Some(since)).map(|m| m.was);
        marks.said(&of(None), &marked(10, Was::Normal), &john, now);
        marks.said(&of(Some(fw.clone())), &marked(30, Was::Outage), &john, now);
        assert_eq!(was(&marks, None, 10), Some(Was::Normal));
        assert_eq!(was(&marks, None, 11), None, "the next one is not answered");
        assert_eq!(was(&marks, Some(&fw), 10), None, "a sender's warnings are its own");
        assert_eq!(was(&marks, Some(&fw), 30), Some(Was::Outage));
        assert_eq!(was(&marks, None, 30), None);
        assert_eq!(marks.answer("g", "s", None, None), None);
        marks.said(&of(None), &marked(10, Was::Outage), &john, now);
        assert_eq!(was(&marks, None, 10), Some(Was::Outage));
        assert_eq!(marks.answers.len(), 2, "answered twice, kept once");
        marks.said(&of(None), &marked(20, Was::Normal), &john, now + YEAR_MS - 15);
        assert_eq!(
            marks.answers.iter().map(|m| m.since_millis).collect::<Vec<_>>(),
            [20],
            "answers to warnings past a year are dropped"
        );
    }

    /// A leader before answers kept the warnings it lowered to info apart. Each reads as answered Outage, and is
    /// written back as one.
    #[test]
    fn a_warning_lowered_before_answers_reads_as_answered_outage() {
        let dir = std::env::temp_dir().join(format!("smeltr-marks-lowered-{}", std::process::id()));
        let store = crate::leader::store::Store::open(std::path::Path::new(":memory:"), &dir.join("k")).unwrap();
        let before = r#"{"lowered":[{"group":"g","source":"s","sinceMillis":5,"by":"John"}],"reset":[]}"#;
        store.put_settings(&[(MARKS_KEY, before.into())], None).unwrap();
        let marks = Marks::read(&store);
        let answer = marks.answer("g", "s", None, Some(5)).unwrap();
        assert_eq!((answer.was, answer.by.as_str()), (Was::Outage, "John"));
        let text = serde_json::to_string(&marks).unwrap();
        assert!(!text.contains("lowered"), "{text}");
        store.put_settings(&[(MARKS_KEY, text)], None).unwrap();
        assert_eq!(Marks::read(&store), marks);
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

    /// A preset picked replaces the one picked before and keeps the one it does not name, and a source at the defaults
    /// keeps no mark.
    #[test]
    fn a_picked_preset_replaces_the_one_before_and_the_default_is_not_kept() {
        let mut marks = Marks::default();
        let pick = |rows, senders| Picking { rows, senders };
        marks.pick("g", "s", pick(Some(Preset::Fast), None), "John");
        marks.pick("g", "s", pick(Some(Preset::Quiet), None), "John");
        marks.pick("g", "s", pick(None, Some(Preset::Sensitive)), "John");
        let expected = Presets {
            rows: Preset::Quiet,
            senders: Preset::Sensitive,
        };
        assert_eq!(marks.presets_of("g", "s"), expected);
        assert_eq!(marks.presets_of("g", "other"), Presets::default());
        let back = Presets::default();
        marks.pick("g", "s", pick(Some(back.rows), Some(back.senders)), "John");
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
            answers: vec![Mark {
                group: "g".into(),
                source: "s".into(),
                sender: None,
                since_millis: 3,
                was: Was::Normal,
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
