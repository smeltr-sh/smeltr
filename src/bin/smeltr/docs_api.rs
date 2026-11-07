//! The user docs this build shipped with (`leader::docs`), for the AI agent to read. They are public: anyone signed
//! in may read them.

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use smeltr::leader::docs;
use smeltr::leader::envelope::Envelope;

use crate::gate::refuse;

#[derive(Deserialize)]
pub struct ListQuery {
    q: Option<String>,
}

#[derive(Serialize)]
struct Listed {
    path: &'static str,
    title: &'static str,
    headings: Vec<Heading>,
}

#[derive(Serialize)]
struct Heading {
    id: &'static str,
    level: u8,
    heading: &'static str,
}

/// `GET /api/docs`: every page with its headings as `pages`, or with `q` the sections that hold every word of it as
/// `hits`.
pub async fn list(Query(query): Query<ListQuery>) -> Response {
    if let Some(q) = query.q {
        return Envelope::ok(serde_json::json!({ "hits": docs::search(&q) })).into_response();
    }
    let pages: Vec<Listed> = docs::pages()
        .iter()
        .map(|page| Listed {
            path: &page.path,
            title: &page.title,
            headings: headings(page),
        })
        .collect();
    Envelope::ok(serde_json::json!({ "pages": pages })).into_response()
}

/// A page's headings, after its opening.
fn headings(page: &'static docs::Page) -> Vec<Heading> {
    page.sections[1..]
        .iter()
        .map(|s| Heading {
            id: &s.id,
            level: s.level,
            heading: &s.heading,
        })
        .collect()
}

#[derive(Deserialize)]
pub struct PageQuery {
    path: String,
    section: Option<String>,
}

#[derive(Serialize)]
struct Read {
    path: String,
    text: String,
    /// Without a section: the page's headings, each read by its id.
    #[serde(skip_serializing_if = "Option::is_none")]
    headings: Option<Vec<Heading>>,
}

/// `GET /api/docs/page`: one page's opening and its headings, or with `section` one heading's text and what is under
/// it.
pub async fn page(Query(query): Query<PageQuery>) -> Response {
    let Some(text) = docs::read(&query.path, query.section.as_deref()) else {
        return refuse(StatusCode::NOT_FOUND, "no such page or section");
    };
    let headings = query
        .section
        .is_none()
        .then(|| docs::page(&query.path).map(headings).unwrap_or_default());
    Envelope::ok(Read {
        path: query.path,
        text,
        headings,
    })
    .into_response()
}
