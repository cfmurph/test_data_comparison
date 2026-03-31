pub mod db;
pub mod file;

use crate::types::{DataSource, Dataset};
use anyhow::Result;

/// Load a [`Dataset`] from any supported [`DataSource`].
pub async fn load(source: &DataSource) -> Result<Dataset> {
    match source {
        DataSource::File(fs) => file::load(fs),
        DataSource::Database(db) => db::load(db).await,
    }
}
