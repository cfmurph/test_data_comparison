//! Data source adapters.
//!
//! Each adapter reads from a concrete source and returns a [`dc_core::Dataset`].
//!
//! # Adding your own adapter
//!
//! 1. Create a new module under `src/` implementing an `async fn load(…) -> Result<Dataset>`.
//! 2. Add a variant to [`SourceSpec`] in `dc_cli` (or call the module directly from your crate).
//! 3. Optionally gate it behind a Cargo feature.
//!
//! The `sources` crate is intentionally kept thin: it contains no comparison
//! logic and no CLI concerns.

pub mod db;
pub mod file;

pub use file::FileAdapter;

use dc_core::Dataset;
use anyhow::Result;

/// Load a dataset from a file source (synchronous).
pub fn load_file(adapter: &FileAdapter) -> Result<Dataset> {
    file::load(adapter)
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
pub async fn load_db(adapter: &db::DbAdapter) -> Result<Dataset> {
    db::load(adapter).await
}
