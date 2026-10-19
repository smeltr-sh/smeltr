//! The user docs this build shipped with (`leader::docs`), for the AI agent to read. They are public: anyone signed
//! in may read them.

use axum::Json;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use smeltr::leader::docs;

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

/// `GET /api/docs`: every page with its headings, or with `q` the sections that hold every word of it.
pub async fn list(Query(query): Query<ListQuery>) -> Response {
    if let Some(q) = query.q {
        return Json(docs::search(&q)).into_response();
    }
    let pages: Vec<Listed> = docs::pages()
        .iter()
        .map(|page| Listed {
            path: &page.path,
            title: &page.title,
            headings: page.sections[1..]
                .iter()
                .map(|s| Heading {
                    id: &s.id,
                    level: s.level,
                    heading: &s.heading,
                })
                .collect(),
        })
        .collect();
    Json(pages).into_response()
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
}

/// `GET /api/docs/page`: one page's text, or with `section` one heading's and what is under it.
pub async fn page(Query(query): Query<PageQuery>) -> Response {
    match docs::read(&query.path, query.section.as_deref()) {
        Some(text) => Json(Read { path: query.path, text }).into_response(),
        None => refuse(StatusCode::NOT_FOUND, "no such page or section"),
    }
}
