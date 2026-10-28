//! The leader's schema catalog routes, over `smeltr::leader::schema_catalog`.

use axum::Json;

use smeltr::leader::envelope::Envelope;
use smeltr::leader::schema_catalog::{self as catalog, Matches, ObjectOut, ObjectRequest, SearchRequest};

pub async fn object(Json(req): Json<ObjectRequest>) -> Json<Envelope<ObjectOut>> {
    Json(catalog::object(&req).map_or_else(Envelope::failed_one, Envelope::ok))
}

pub async fn search(Json(req): Json<SearchRequest>) -> Json<Envelope<Matches>> {
    Json(Envelope::ok(catalog::search(&req)))
}
