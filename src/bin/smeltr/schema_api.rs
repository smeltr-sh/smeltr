//! The leader's schema routes, over `smeltr::leader::schema_catalog`: a JSON Schema in, and a schema out as one.

use axum::Json;

use smeltr::config_file::SchemaDto;
use smeltr::leader::blocking;
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::leader::schema_catalog::{self as catalog, FromJsonSchema, ReadSchema, WrittenSchema};

pub async fn from_json_schema(Json(req): Json<FromJsonSchema>) -> Envelope<ReadSchema> {
    blocking(move || catalog::from_json_schema(&req).map_err(Refusal::invalid))
        .await
        .into()
}

pub async fn to_json_schema(Json(schema): Json<SchemaDto>) -> Envelope<WrittenSchema> {
    blocking(move || catalog::to_json_schema(&schema).map_err(Refusal::invalid))
        .await
        .into()
}
