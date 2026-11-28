//! The Delta destination: Parquet files under a Delta table's root, each commit the next entry of its `_delta_log`,
//! written only where none is.

pub(crate) mod checkpoint;
pub(crate) mod default;
pub(crate) mod identity;
pub mod log;
pub(crate) mod schema;
pub(crate) mod table;
pub(crate) mod unity;
