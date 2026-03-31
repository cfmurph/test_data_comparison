//! File-based data source adapter.
//!
//! Supports CSV, TSV, and JSON (array of objects or single object).

use dc_core::{types::Row, Dataset, Value};
use anyhow::{Context, Result};
use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use std::fs;

/// Configuration for a file-based data source.
#[derive(Debug, Clone)]
pub struct FileAdapter {
    pub path: String,
    pub format: FileFormat,
}

impl FileAdapter {
    pub fn new(path: impl Into<String>) -> Self {
        let path = path.into();
        let format = FileFormat::detect(&path);
        Self { path, format }
    }

    pub fn with_format(mut self, format: FileFormat) -> Self {
        self.format = format;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Tsv,
    Json,
}

impl FileFormat {
    pub fn detect(path: &str) -> Self {
        let lower = path.to_lowercase();
        if lower.ends_with(".json") {
            FileFormat::Json
        } else if lower.ends_with(".tsv") || lower.ends_with(".tab") {
            FileFormat::Tsv
        } else {
            FileFormat::Csv
        }
    }
}

pub fn load(adapter: &FileAdapter) -> Result<Dataset> {
    match adapter.format {
        FileFormat::Csv => load_delimited(adapter, b','),
        FileFormat::Tsv => load_delimited(adapter, b'\t'),
        FileFormat::Json => load_json(adapter),
    }
}

fn load_delimited(adapter: &FileAdapter, delimiter: u8) -> Result<Dataset> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(true)
        .from_path(&adapter.path)
        .with_context(|| format!("Cannot open file: {}", adapter.path))?;

    let headers: Vec<String> = rdr
        .headers()
        .with_context(|| format!("Cannot read headers from: {}", adapter.path))?
        .iter()
        .map(str::to_owned)
        .collect();

    let mut dataset = Dataset::new(&adapter.path);
    dataset.columns = headers.clone();

    for result in rdr.records() {
        let record =
            result.with_context(|| format!("Error reading record from: {}", adapter.path))?;
        let mut row: Row = IndexMap::new();
        for (i, col) in headers.iter().enumerate() {
            let raw = record.get(i).unwrap_or("").trim();
            row.insert(col.clone(), Value::from_str_smart(raw));
        }
        dataset.rows.push(row);
    }

    Ok(dataset)
}

fn load_json(adapter: &FileAdapter) -> Result<Dataset> {
    let content = fs::read_to_string(&adapter.path)
        .with_context(|| format!("Cannot open file: {}", adapter.path))?;

    let parsed: JsonValue = serde_json::from_str(&content)
        .with_context(|| format!("Invalid JSON: {}", adapter.path))?;

    let array = match &parsed {
        JsonValue::Array(arr) => arr.clone(),
        JsonValue::Object(_) => vec![parsed.clone()],
        _ => anyhow::bail!("JSON must contain an array of objects or a single object"),
    };

    let mut dataset = Dataset::new(&adapter.path);

    for item in &array {
        if let JsonValue::Object(map) = item {
            for key in map.keys() {
                if !dataset.columns.contains(key) {
                    dataset.columns.push(key.clone());
                }
            }
        }
    }

    for item in &array {
        if let JsonValue::Object(map) = item {
            let mut row: Row = IndexMap::new();
            for col in &dataset.columns {
                let val = map
                    .get(col)
                    .map(json_val_to_value)
                    .unwrap_or(Value::Null);
                row.insert(col.clone(), val);
            }
            dataset.rows.push(row);
        }
    }

    Ok(dataset)
}

fn json_val_to_value(v: &JsonValue) -> Value {
    match v {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        JsonValue::String(s) => Value::from_str_smart(s),
        other => Value::Text(other.to_string()),
    }
}
