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
    /// Attempt to parse a raw string into the most specific type.
    pub fn from_str_smart(s: &str) -> Self {
        if s.is_empty() || s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("nil") {
            return Value::Null;
        }
        if s.eq_ignore_ascii_case("true") {
            return Value::Bool(true);
        }
        if s.eq_ignore_ascii_case("false") {
            return Value::Bool(false);
        }
        if let Ok(i) = s.parse::<i64>() {
            return Value::Integer(i);
        }
        if let Ok(f) = s.parse::<f64>() {
            return Value::Float(f);
        }
        Value::Text(s.to_owned())
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Loose equality: compares integers and floats cross-type.
    pub fn loose_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), Value::Integer(b)) => *a == (*b as f64),
            _ => self == other,
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

/// A full dataset: an ordered list of column names and rows.
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
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Describes where to load data from.
#[derive(Debug, Clone)]
pub enum DataSource {
    /// CSV / TSV / JSON file path.
    File(FileSource),
    /// Database connection.
    Database(DbSource),
}

#[derive(Debug, Clone)]
pub struct FileSource {
    pub path: String,
    pub format: FileFormat,
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
        } else if lower.ends_with(".tsv") {
            FileFormat::Tsv
        } else {
            FileFormat::Csv
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbSource {
    /// Full connection string, e.g. `sqlite://db.sqlite3`,
    /// `postgres://user:pass@host/db`, `mysql://user:pass@host/db`
    pub connection_string: String,
    /// SQL query to execute.
    pub query: String,
    /// Human-readable label for reporting.
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbDriver {
    Sqlite,
    Postgres,
    Mysql,
}

impl DbDriver {
    pub fn detect(conn: &str) -> anyhow::Result<Self> {
        if conn.starts_with("sqlite://") || conn.starts_with("sqlite3://") {
            Ok(DbDriver::Sqlite)
        } else if conn.starts_with("postgres://") || conn.starts_with("postgresql://") {
            Ok(DbDriver::Postgres)
        } else if conn.starts_with("mysql://") || conn.starts_with("mariadb://") {
            Ok(DbDriver::Mysql)
        } else {
            anyhow::bail!("Unsupported connection string: {conn}")
        }
    }
}
