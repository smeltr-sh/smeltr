//! The leader's schema catalog routes, over `smeltr::leader::schema_catalog`.

use axum::Json;

use smeltr::leader::blocking;
use smeltr::leader::envelope::Envelope;
use smeltr::leader::schema_catalog::{self as catalog, Matches, ObjectOut, ObjectRequest, SearchRequest};

pub async fn object(Json(req): Json<ObjectRequest>) -> Json<Envelope<ObjectOut>> {
    Json(
        blocking(move || catalog::object(&req))
            .await
            .map_or_else(Envelope::failed_one, Envelope::ok),
    )
}

pub async fn search(Json(req): Json<SearchRequest>) -> Json<Envelope<Matches>> {
    let found = blocking(move || Ok::<_, String>(catalog::search(&req))).await;
    Json(found.map_or_else(Envelope::failed_one, Envelope::ok))
}
