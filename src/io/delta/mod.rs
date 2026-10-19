//! The Delta destination: Parquet files under a Delta table's root, each commit the next entry of its `_delta_log`,
//! written only where none is.

pub mod checkpoint;
pub mod log;
pub mod schema;
pub mod table;
pub mod unity;
