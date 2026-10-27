pub mod commit;
pub mod config_file;
pub mod core;
pub mod cpu_cache;
pub mod crypto;
pub mod egress;
pub mod enrollment_code;
pub mod expr;
pub mod format;
#[cfg(test)]
pub mod fuzz;
pub mod handoff;
pub mod heartbeat;
pub mod io;
pub mod leader;
pub mod logs;
pub mod lookup;
pub mod metrics;
pub mod ops;
pub mod paths;
pub mod plan;
pub mod process;
#[cfg(test)]
mod releases;
pub mod resources;
pub mod runtime;
pub mod schema;
pub mod startup_config;
pub mod streams;
pub mod version;
pub mod worker;

// In the library, not the binary, so the tests and the profiling suites run on the allocator that ships.
#[global_allocator]
static ALLOCATOR: core::buffer::Counting = core::buffer::Counting;
