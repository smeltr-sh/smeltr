//! The leader's schema routes, over `smeltr::leader::schema_catalog`: a file in a standard's format in, and a schema
//! out in an export format.

use axum::Json;

use smeltr::leader::blocking;
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::leader::schema_catalog::{self as catalog, ExportRequest, ExportedSchema, ReadRequest, ReadSchema, Standards};

pub async fn read(Json(req): Json<ReadRequest>) -> Envelope<ReadSchema> {
    blocking(move || catalog::read(&req).map_err(Refusal::invalid))
        .await
        .into()
}

pub async fn export(Json(req): Json<ExportRequest>) -> Envelope<ExportedSchema> {
    blocking(move || catalog::export(&req).map_err(Refusal::invalid))
        .await
        .into()
}

pub async fn standards() -> Envelope<Standards> {
    blocking(|| Ok::<_, Refusal>(catalog::standards())).await.into()
}
