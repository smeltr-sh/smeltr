//! Sources, destinations, and routes — the layer above a single compiled `Plan` that decides *which* pipeline a batch
//! of raw records runs through, and where each pipeline's output goes. See the design doc's "Sources, destinations,
//! routes" section for the full reasoning; this module is that section's config schema plus route compiling/evaluation.
//!
//! `SourceKind`/`DestinationKind` are config *shape* only in this pass — `io::kafka`'s actual runtime implementation
//! (behind the `Source`/ `Destination` traits) is a deliberately separate, later step. Nothing here depends on it, and
//! nothing here will need to change once it lands.

mod fields;
mod read;
mod routes;
mod settings;
mod stored;

pub use fields::*;
pub use routes::*;
pub use settings::*;
