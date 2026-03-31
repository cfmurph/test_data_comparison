//! TOML-based configuration for repeatable comparison runs.
//!
//! A config file lets you express all comparison parameters declaratively,
//! encode them in version control, and reuse them across environments.
//!
//! ```toml
//! [left]
//! type  = "file"
//! path  = "export_a.csv"
//!
//! [right]
//! type  = "file"
//! path  = "export_b.csv"
//!
//! [compare]
//! keys             = ["id"]
//! ignore_columns   = ["updated_at", "created_at"]
//! max_diffs        = 100
//! output_format    = "table"   # table | json | summary
//! output_file      = "diff.json"
//!
//! [compare.column_mappings]
//! amount_usd = "amount"
//!
//! [compare.column_options.price]
//! comparator = "numeric_tolerance"
//! epsilon    = 0.01
//!
//! [compare.column_options.name]
//! comparator = "case_insensitive"
//!
//! [[compare.column_options.description]]
//! comparator = "trim"
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level config file structure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompareConfig {
    pub left: Option<SourceConfig>,
    pub right: Option<SourceConfig>,
    #[serde(default)]
    pub compare: CompareSection,
}

impl CompareConfig {
    pub fn from_toml(s: &str) -> anyhow::Result<Self> {
        let cfg: Self = toml::from_str(s)?;
        Ok(cfg)
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Cannot read config file {path}: {e}"))?;
        Self::from_toml(&content)
    }
}

/// Describes one data source in the config file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// `"file"` or `"database"`
    #[serde(rename = "type")]
    pub source_type: String,

    // ── file fields ──────────────────────────────────────────────────────────
    pub path: Option<String>,
    /// `"csv"` | `"tsv"` | `"json"` – detected from extension if absent
    pub format: Option<String>,

    // ── database fields ──────────────────────────────────────────────────────
    pub connection_string: Option<String>,
    pub query: Option<String>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompareSection {
    #[serde(default)]
    pub keys: Vec<String>,

    #[serde(default)]
    pub ignore_columns: Vec<String>,

    #[serde(default)]
    pub column_mappings: HashMap<String, String>,

    pub max_diffs: Option<usize>,

    /// Default comparator for all columns not listed in `column_options`.
    /// Values: `"strict"` | `"case_insensitive"` | `"trim"` | `"numeric_tolerance"`
    #[serde(default = "default_comparator_name")]
    pub default_comparator: String,

    /// Per-column comparator overrides.
    #[serde(default)]
    pub column_options: HashMap<String, ColumnOptions>,

    /// `"table"` | `"json"` | `"summary"`
    #[serde(default = "default_output_format")]
    pub output_format: String,

    /// Write output to this file path (instead of / in addition to stdout).
    pub output_file: Option<String>,
}

fn default_comparator_name() -> String {
    "strict".to_string()
}

fn default_output_format() -> String {
    "table".to_string()
}

/// Per-column comparison options.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnOptions {
    /// `"strict"` | `"case_insensitive"` | `"trim"` | `"numeric_tolerance"`
    pub comparator: Option<String>,
    /// Used by `numeric_tolerance` comparator.
    pub epsilon: Option<f64>,
}
