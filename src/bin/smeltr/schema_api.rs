//! The leader's schema routes, over `smeltr::leader::schema_catalog`: a JSON Schema in, and a schema out as one.

use axum::Json;

use smeltr::config_file::SchemaDto;
use smeltr::leader::blocking;
use smeltr::leader::envelope::Envelope;
use smeltr::leader::schema_catalog::{self as catalog, FromJsonSchema, ReadSchema, WrittenSchema};

pub async fn from_json_schema(Json(req): Json<FromJsonSchema>) -> Json<Envelope<ReadSchema>> {
    Json(
        blocking(move || catalog::from_json_schema(&req))
            .await
            .map_or_else(Envelope::failed_one, Envelope::ok),
    )
}

pub async fn to_json_schema(Json(schema): Json<SchemaDto>) -> Json<Envelope<WrittenSchema>> {
    Json(
        blocking(move || catalog::to_json_schema(&schema))
            .await
            .map_or_else(Envelope::failed_one, Envelope::ok),
    )
}
