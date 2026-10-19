//! Iceberg tables through a REST catalog: the file destination's Parquet files, recorded in the table by a committer
//! that writes a manifest and a manifest list (Avro) and asks the catalog to make them the table's next snapshot.
//!
//! ```mermaid
//! flowchart LR
//!   R[row] -->|TablePartitioner<br/>the table's spec| F[Parquet file<br/>field ids from the table's schema]
//!   F -->|DataFile, footer stats| M[manifest<br/>Avro]
//!   M --> L[manifest list<br/>the parent's manifests, small ones merged]
//!   L -->|add-snapshot, asserting the parent| C[catalog commit]
//!   C -->|409| L
//! ```
//!
//! The table's schema is the route's record: a missing table is created from it, a field the table lacks is added as
//! an optional column, and a type the table holds otherwise is refused (`schema`). The table's partition spec is
//! followed (`transform`). Only format version 2 is written.

pub mod catalog;
pub mod manifest;
pub mod metadata;
pub mod schema;
pub mod table;
pub mod transform;
