use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A single comparable value in a dataset cell.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(String),
}

impl Value {
    /// Parse a raw string into the most specific type.
    pub fn from_str_smart(s: &str) -> Self {
        let trimmed = s.trim();
        if trimmed.is_empty()
            || trimmed.eq_ignore_ascii_case("null")
            || trimmed.eq_ignore_ascii_case("nil")
        {
            return Value::Null;
        }
        if trimmed.eq_ignore_ascii_case("true") {
            return Value::Bool(true);
        }
        if trimmed.eq_ignore_ascii_case("false") {
            return Value::Bool(false);
        }
        if let Ok(i) = trimmed.parse::<i64>() {
            return Value::Integer(i);
        }
        if let Ok(f) = trimmed.parse::<f64>() {
            return Value::Float(f);
        }
        Value::Text(s.to_owned())
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Loose equality: integer and float are equal when numerically identical.
    pub fn loose_numeric_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), Value::Integer(b)) => *a == (*b as f64),
            _ => self == other,
        }
    }

    /// Return the float representation if numeric.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Text(s) => write!(f, "{s}"),
        }
    }
}

/// An ordered map of column name → value representing one row.
pub type Row = IndexMap<String, Value>;

/// A full dataset: column names and rows.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Dataset {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

impl Dataset {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
