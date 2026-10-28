//! The leader's schema catalog routes, over `smeltr::leader::schema_catalog`.

use axum::Json;
use axum::extract::Query;

use smeltr::leader::envelope::Envelope;
use smeltr::leader::schema_catalog::{
    self as catalog, Catalog, Exported, Matches, ObjectOut, ObjectRequest, SearchRequest,
};

pub async fn builtin() -> Json<Envelope<Catalog>> {
    Json(Envelope::ok(catalog::catalog()))
}

pub async fn object(Json(req): Json<ObjectRequest>) -> Json<Envelope<ObjectOut>> {
    Json(catalog::object(&req).map_or_else(Envelope::failed_one, Envelope::ok))
}

pub async fn search(Json(req): Json<SearchRequest>) -> Json<Envelope<Matches>> {
    Json(Envelope::ok(catalog::search(&req)))
}

#[derive(serde::Deserialize)]
pub struct ExportQuery {
    id: String,
}

pub async fn export(Query(q): Query<ExportQuery>) -> Json<Envelope<Exported>> {
    Json(catalog::export(&q.id).map_or_else(Envelope::failed_one, Envelope::ok))
}
