//! The leader's schema routes, over `smeltr::leader::schema_catalog`: a JSON Schema in, and a schema out in an
//! export format.

use axum::Json;

use smeltr::leader::blocking;
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::leader::schema_catalog::{self as catalog, ExportRequest, ExportedSchema, FromJsonSchema, ReadSchema};

pub async fn from_json_schema(Json(req): Json<FromJsonSchema>) -> Envelope<ReadSchema> {
    blocking(move || catalog::from_json_schema(&req).map_err(Refusal::invalid))
        .await
        .into()
}

pub async fn export(Json(req): Json<ExportRequest>) -> Envelope<ExportedSchema> {
    blocking(move || catalog::export(&req).map_err(Refusal::invalid))
        .await
        .into()
}
