//! Moving objects from one area to another, the way an export and an import move them between leaders: the mover
//! sees each object the move would take or break, ticks what goes, answers each id the other area holds already, and
//! the move is one commit. Without `go` it writes nothing and answers what the ticks would do.

use std::collections::BTreeSet;

use axum::Json;
use axum::extract::Extension;
use axum::http::StatusCode;

use smeltr::config_file::{self, Keep, MoveRow, Moving, Objects, Part, PartId, Standing, describe};
use smeltr::leader::envelope::{ApiError, Envelope, Refusal};
use smeltr::leader::store::Actor;

use crate::config_api::{Addressed, NotWritten, Writing};
use crate::import_api::{self, TakenAnswer, scope_objects};

/// What the mover asks: the object, where it goes, and once the mover has chosen, what goes with it and the answer
/// for each taken id.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MoveAsk {
    pub(crate) part: Part,
    pub(crate) id: String,
    /// `shared`, or a group id.
    pub(crate) to: String,
    /// What goes. Absent, the least the move takes.
    #[serde(default)]
    pub(crate) ticked: Option<Vec<PartId>>,
    #[serde(default)]
    pub(crate) taken: Vec<TakenAnswer>,
    /// Writes the move. Without it, nothing is written.
    #[serde(default)]
    pub(crate) go: bool,
}

/// Each object the move lists, what the ticks leave broken, the commit both areas were read at, and once written,
/// the commit the move made.
#[derive(Default, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveAnswer {
    pub(crate) rows: Vec<MoveRow>,
    /// Why the ticks cannot move as they are. Empty when they can.
    exceptions: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

pub async fn move_objects(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<smeltr::leader::access::Access>,
    at: Addressed,
    Json(ask): Json<MoveAsk>,
) -> Envelope<MoveAnswer> {
    // The gate judged the area moved from, which the path names. The one moved to is in the body.
    let need = crate::policy::Need::Area {
        area: ask.to.clone(),
        level: smeltr::leader::access::Level::Editor,
    };
    if !need.met_by(&access) {
        return Envelope::refused(Refusal::from(need));
    }
    smeltr::leader::blocking_answer(move || {
        let opened = at
            .open()
            .and_then(|from| Ok((from, Writing::open(&at.branch, at.base.as_deref(), &ask.to)?)));
        match opened {
            Ok((from, to)) => move_between(from, to, ask, &actor),
            Err(e) => not_written(e),
        }
    })
    .await
}

fn not_written(e: NotWritten) -> Envelope<MoveAnswer> {
    Envelope::refused(e.refusal)
}

/// [`move_objects`] once both areas are open. Blocking.
pub(crate) fn move_between(mut from: Writing, mut to: Writing, ask: MoveAsk, author: &Actor) -> Envelope<MoveAnswer> {
    let what = format!("{} {:?}", describe(ask.part), ask.id);
    if to.area() == from.area() {
        return Envelope::refused(Refusal::invalid(format!("{what} is in {} already", to.area())));
    }
    if let Err(e) = from.can_move_to(&to) {
        return not_written(e);
    }
    let here = match all_objects(&from) {
        Ok(here) => here,
        Err(e) => return Envelope::refused(e),
    };
    let asked = (ask.part, ask.id.clone());
    if !here.ids().contains(&asked) {
        return Envelope::refused(Refusal::not_found(format!("{} has no {what}", from.area())));
    }
    // Out of `shared/`, the group moved to sees what moves where it is now: compared with the group's own alone.
    let found = match from.area() == config_file::SHARED_DIR {
        true => scope_of(&to, &here).map(|there| (there, BTreeSet::new())),
        false => import_api::here(&to, &here),
    };
    let (there, in_shared) = match found {
        Ok(found) => found,
        Err(e) => return Envelope::refused(e),
    };

    let (from_area, to_area) = (from.area().to_string(), to.area().to_string());
    let moving = Moving::new(&here, &there, asked, &from_area, &to_area);
    let ticked: Option<BTreeSet<(Part, String)>> =
        (ask.ticked.as_ref()).map(|t| t.iter().map(|o| (o.part, o.id.clone())).collect());
    let (mut rows, mut exceptions) = moving.list(ticked.as_ref());
    for r in &mut rows {
        r.listed.in_shared = in_shared.contains(&(r.listed.part, r.listed.id.clone()));
    }
    let going: BTreeSet<(Part, String)> = (rows.iter())
        .filter(|r| r.ticked)
        .map(|r| (r.listed.part, r.listed.id.clone()))
        .collect();

    let mut edits = None;
    if exceptions.is_empty() {
        exceptions.extend(
            (ask.taken.iter())
                .filter(|a| a.keep == Keep::Imported && in_shared.contains(&(a.part, a.id.clone())))
                .map(|a| {
                    format!(
                        "{} {:?} is in shared, so only a move into shared can replace it",
                        describe(a.part),
                        a.id
                    )
                }),
        );
    }
    // An id taken there is asked in the dialog, so a dry run says nothing of one not answered yet, and checks nothing
    // until every one is.
    let unanswered: Vec<String> = (rows.iter())
        .filter(|r| r.ticked && r.listed.standing == Standing::Taken)
        .filter(|r| !(ask.taken.iter()).any(|a| a.part == r.listed.part && a.id == r.listed.id))
        .map(|r| {
            format!(
                "{} {:?} needs a Keep choice: its id is taken in {to_area}",
                describe(r.listed.part),
                r.listed.id
            )
        })
        .collect();
    if !unanswered.is_empty() {
        if ask.go {
            exceptions.extend(unanswered);
        }
    } else if exceptions.is_empty() {
        let keeps = (ask.taken.iter())
            .map(|a| ((a.part, a.id.clone()), a.keep.clone()))
            .collect();
        match moving.going(&going).resolve(&there, &keeps) {
            Err(unanswered) => exceptions.extend(unanswered),
            Ok(written) => match from.move_edits(&mut to, &going, written) {
                Ok(e) => edits = Some(e),
                Err(refused) => exceptions.extend(refused.errors.into_iter().map(|e| e.message)),
            },
        }
    }

    let answer = MoveAnswer {
        rows,
        exceptions,
        at: from.parent.clone(),
        commit: None,
    };
    if !ask.go {
        return Envelope::ok(answer);
    }
    let Some(edits) = edits else {
        let errors = answer.exceptions.iter().map(ApiError::new).collect();
        return Envelope::with(answer, StatusCode::CONFLICT, errors);
    };
    let message = format!("move {} {} to {to_area}", describe(ask.part), ask.id);
    let saved = from.commit_move(edits, &message, author);
    match saved.body.commit() {
        Some(commit) => Envelope::ok(MoveAnswer {
            commit: Some(commit.to_string()),
            ..answer
        }),
        None => Envelope::with(answer, saved.status, saved.errors),
    }
}

/// Every object `writing`'s area holds, its samples and tests among them.
fn all_objects(writing: &Writing) -> Result<Objects, Refusal> {
    let mut files = writing.files();
    let samples = config_file::list_samples(&mut files).0;
    let tests = config_file::list_tests(&mut files).0;
    let samples: Vec<&str> = samples.iter().map(|s| s.id.as_str()).collect();
    let tests: Vec<&str> = tests.iter().map(|t| t.id.as_str()).collect();
    scope_objects(writing, writing.prefix(), writing.read().file, &samples, &tests)
}

/// What `to`'s own area holds of the samples and tests `here` holds, beside its bundle.
fn scope_of(to: &Writing, here: &Objects) -> Result<Objects, Refusal> {
    let samples: Vec<&str> = here.samples.iter().map(|s| s.id.as_str()).collect();
    let tests: Vec<&str> = here.tests.iter().map(|t| t.id.as_str()).collect();
    scope_objects(to, to.prefix(), to.read().file, &samples, &tests)
}
