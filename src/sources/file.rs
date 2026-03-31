use crate::types::{Dataset, FileFormat, FileSource, Row, Value};
use anyhow::{Context, Result};
use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use std::fs;

pub fn load(src: &FileSource) -> Result<Dataset> {
    match src.format {
        FileFormat::Csv => load_delimited(src, b','),
        FileFormat::Tsv => load_delimited(src, b'\t'),
        FileFormat::Json => load_json(src),
    }
}

fn load_delimited(src: &FileSource, delimiter: u8) -> Result<Dataset> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(true)
        .from_path(&src.path)
        .with_context(|| format!("Cannot open file: {}", src.path))?;

    let headers: Vec<String> = rdr
        .headers()
        .with_context(|| format!("Cannot read headers from: {}", src.path))?
        .iter()
        .map(str::to_owned)
        .collect();

    let mut dataset = Dataset::new(&src.path);
    dataset.columns = headers.clone();

    for result in rdr.records() {
        let record = result.with_context(|| format!("Error reading record from: {}", src.path))?;
        let mut row: Row = IndexMap::new();
        for (i, col) in headers.iter().enumerate() {
            let raw = record.get(i).unwrap_or("").trim();
            row.insert(col.clone(), Value::from_str_smart(raw));
        }
        dataset.rows.push(row);
    }

    Ok(dataset)
}

fn load_json(src: &FileSource) -> Result<Dataset> {
    let content =
        fs::read_to_string(&src.path).with_context(|| format!("Cannot open file: {}", src.path))?;

    let parsed: JsonValue =
        serde_json::from_str(&content).with_context(|| format!("Invalid JSON: {}", src.path))?;

    let array = match &parsed {
        JsonValue::Array(arr) => arr.clone(),
        // Accept a single object as a one-row dataset
        JsonValue::Object(_) => vec![parsed.clone()],
        _ => anyhow::bail!("JSON file must contain an array of objects or a single object"),
    };

    let mut dataset = Dataset::new(&src.path);

    // Collect all column names in insertion order
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
        let mut row: Row = IndexMap::new();
        if let JsonValue::Object(map) = item {
            for col in &dataset.columns {
                let val = map
                    .get(col)
                    .map(json_value_to_value)
                    .unwrap_or(Value::Null);
                row.insert(col.clone(), val);
            }
        }
        dataset.rows.push(row);
    }

    Ok(dataset)
}

fn json_value_to_value(v: &JsonValue) -> Value {
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
