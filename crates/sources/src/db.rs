//! Database source adapters gated by Cargo features.
//!
//! Enable with: `--features sqlite`, `--features postgres`, `--features mysql`
//! (all three are on by default).

use dc_core::{types::Row, Dataset, Value};
use anyhow::{Context, Result};
use indexmap::IndexMap;

/// Configuration for a database source.
#[derive(Debug, Clone)]
pub struct DbAdapter {
    /// Full connection string.
    /// e.g. `sqlite://./db.sqlite3`, `postgres://user:pw@host/db`, `mysql://…`
    pub connection_string: String,
    /// SQL query to run.
    pub query: String,
    /// Human-readable label used in reports.
    pub label: String,
}

impl DbAdapter {
    pub fn new(
        connection_string: impl Into<String>,
        query: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        Self {
            connection_string: connection_string.into(),
            query: query.into(),
            label: label.into(),
        }
    }
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
            anyhow::bail!("Unsupported connection string prefix: {conn}")
        }
    }
}

pub async fn load(adapter: &DbAdapter) -> Result<Dataset> {
    let driver = DbDriver::detect(&adapter.connection_string)?;
    match driver {
        DbDriver::Sqlite => load_sqlite(adapter).await,
        DbDriver::Postgres => load_postgres(adapter).await,
        DbDriver::Mysql => load_mysql(adapter).await,
    }
}

// ── SQLite ────────────────────────────────────────────────────────────────────

#[cfg(feature = "sqlite")]
async fn load_sqlite(adapter: &DbAdapter) -> Result<Dataset> {
    use sqlx::{Column, Row as SqlxRow, TypeInfo};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
    use std::str::FromStr;

    let opts = SqliteConnectOptions::from_str(&adapter.connection_string)
        .with_context(|| format!("Invalid SQLite URL: {}", adapter.connection_string))?
        .create_if_missing(true);

    let pool = SqlitePool::connect_with(opts)
        .await
        .with_context(|| format!("Cannot open SQLite: {}", adapter.connection_string))?;

    let rows = sqlx::query(&adapter.query)
        .fetch_all(&pool)
        .await
        .with_context(|| format!("Query failed: {}", adapter.query))?;

    let mut dataset = Dataset::new(&adapter.label);
    if let Some(first) = rows.first() {
        dataset.columns = first.columns().iter().map(|c| c.name().to_string()).collect();
    }

    for row in &rows {
        let mut map: Row = IndexMap::new();
        for col in row.columns() {
            let name = col.name().to_string();
            let type_name = col.type_info().name().to_lowercase();
            map.insert(name, sqlite_val(row, col.ordinal(), &type_name));
        }
        dataset.rows.push(map);
    }
    pool.close().await;
    Ok(dataset)
}

#[cfg(not(feature = "sqlite"))]
async fn load_sqlite(adapter: &DbAdapter) -> Result<Dataset> {
    anyhow::bail!("SQLite support not compiled in. Enable the `sqlite` feature.")
}

#[cfg(feature = "sqlite")]
fn sqlite_val(row: &sqlx::sqlite::SqliteRow, idx: usize, type_name: &str) -> Value {
    use sqlx::Row as SqlxRow;
    match type_name {
        "boolean" | "bool" => row.try_get::<Option<bool>, _>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        "integer" | "int" | "int2" | "int4" | "int8" | "bigint" | "smallint" | "tinyint" => {
            row.try_get::<Option<i64>, _>(idx)
                .ok()
                .flatten()
                .map(Value::Integer)
                .unwrap_or(Value::Null)
        }
        "real" | "float" | "double" | "numeric" | "decimal" => {
            row.try_get::<Option<f64>, _>(idx)
                .ok()
                .flatten()
                .map(Value::Float)
                .unwrap_or(Value::Null)
        }
        _ => match row.try_get::<Option<String>, _>(idx) {
            Ok(Some(s)) => Value::from_str_smart(&s),
            Ok(None) => Value::Null,
            Err(_) => row
                .try_get::<Option<i64>, _>(idx)
                .ok()
                .flatten()
                .map(Value::Integer)
                .unwrap_or_else(|| {
                    row.try_get::<Option<f64>, _>(idx)
                        .ok()
                        .flatten()
                        .map(Value::Float)
                        .unwrap_or(Value::Null)
                }),
        },
    }
}

// ── PostgreSQL ────────────────────────────────────────────────────────────────

#[cfg(feature = "postgres")]
async fn load_postgres(adapter: &DbAdapter) -> Result<Dataset> {
    use sqlx::{Column, PgPool, Row as SqlxRow, TypeInfo};

    let pool = PgPool::connect(&adapter.connection_string)
        .await
        .with_context(|| format!("Cannot connect to PostgreSQL: {}", adapter.connection_string))?;

    let rows = sqlx::query(&adapter.query)
        .fetch_all(&pool)
        .await
        .with_context(|| format!("Query failed: {}", adapter.query))?;

    let mut dataset = Dataset::new(&adapter.label);
    if let Some(first) = rows.first() {
        dataset.columns = first.columns().iter().map(|c| c.name().to_string()).collect();
    }

    for row in &rows {
        let mut map: Row = IndexMap::new();
        for col in row.columns() {
            let name = col.name().to_string();
            let type_name = col.type_info().name().to_lowercase();
            map.insert(name, pg_val(row, col.ordinal(), &type_name));
        }
        dataset.rows.push(map);
    }
    pool.close().await;
    Ok(dataset)
}

#[cfg(not(feature = "postgres"))]
async fn load_postgres(adapter: &DbAdapter) -> Result<Dataset> {
    anyhow::bail!("PostgreSQL support not compiled in. Enable the `postgres` feature.")
}

#[cfg(feature = "postgres")]
fn pg_val(row: &sqlx::postgres::PgRow, idx: usize, type_name: &str) -> Value {
    use sqlx::Row as SqlxRow;
    match type_name {
        "bool" => row.try_get::<Option<bool>, _>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        "int2" | "int4" | "int8" | "smallint" | "integer" | "bigint" | "serial" | "bigserial" => {
            row.try_get::<Option<i64>, _>(idx)
                .ok()
                .flatten()
                .map(Value::Integer)
                .or_else(|| {
                    row.try_get::<Option<i32>, _>(idx)
                        .ok()
                        .flatten()
                        .map(|i| Value::Integer(i as i64))
                })
                .unwrap_or(Value::Null)
        }
        "float4" | "float8" | "real" | "double precision" | "numeric" | "decimal" => {
            row.try_get::<Option<f64>, _>(idx)
                .ok()
                .flatten()
                .map(Value::Float)
                .unwrap_or(Value::Null)
        }
        _ => row.try_get::<Option<String>, _>(idx)
            .ok()
            .flatten()
            .map(|s| Value::from_str_smart(&s))
            .unwrap_or(Value::Null),
    }
}

// ── MySQL / MariaDB ───────────────────────────────────────────────────────────

#[cfg(feature = "mysql")]
async fn load_mysql(adapter: &DbAdapter) -> Result<Dataset> {
    use sqlx::{Column, MySqlPool, Row as SqlxRow, TypeInfo};

    let pool = MySqlPool::connect(&adapter.connection_string)
        .await
        .with_context(|| format!("Cannot connect to MySQL: {}", adapter.connection_string))?;

    let rows = sqlx::query(&adapter.query)
        .fetch_all(&pool)
        .await
        .with_context(|| format!("Query failed: {}", adapter.query))?;

    let mut dataset = Dataset::new(&adapter.label);
    if let Some(first) = rows.first() {
        dataset.columns = first.columns().iter().map(|c| c.name().to_string()).collect();
    }

    for row in &rows {
        let mut map: Row = IndexMap::new();
        for col in row.columns() {
            let name = col.name().to_string();
            let type_name = col.type_info().name().to_lowercase();
            map.insert(name, mysql_val(row, col.ordinal(), &type_name));
        }
        dataset.rows.push(map);
    }
    pool.close().await;
    Ok(dataset)
}

#[cfg(not(feature = "mysql"))]
async fn load_mysql(adapter: &DbAdapter) -> Result<Dataset> {
    anyhow::bail!("MySQL support not compiled in. Enable the `mysql` feature.")
}

#[cfg(feature = "mysql")]
fn mysql_val(row: &sqlx::mysql::MySqlRow, idx: usize, type_name: &str) -> Value {
    use sqlx::Row as SqlxRow;
    match type_name {
        "tinyint(1)" | "boolean" | "bool" => row.try_get::<Option<bool>, _>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => {
            row.try_get::<Option<i64>, _>(idx)
                .ok()
                .flatten()
                .map(Value::Integer)
                .unwrap_or(Value::Null)
        }
        "float" | "double" | "decimal" | "numeric" => row.try_get::<Option<f64>, _>(idx)
            .ok()
            .flatten()
            .map(Value::Float)
            .unwrap_or(Value::Null),
        _ => row.try_get::<Option<String>, _>(idx)
            .ok()
            .flatten()
            .map(|s| Value::from_str_smart(&s))
            .unwrap_or(Value::Null),
    }
}
