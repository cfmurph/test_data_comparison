use crate::types::{Dataset, DbDriver, DbSource, Row, Value};
use anyhow::{Context, Result};
use indexmap::IndexMap;
use sqlx::{Column, Row as SqlxRow, TypeInfo};

pub async fn load(src: &DbSource) -> Result<Dataset> {
    let driver = DbDriver::detect(&src.connection_string).context("Detecting database driver")?;
    match driver {
        DbDriver::Sqlite => load_sqlite(src).await,
        DbDriver::Postgres => load_postgres(src).await,
        DbDriver::Mysql => load_mysql(src).await,
    }
}

// ── SQLite ────────────────────────────────────────────────────────────────────

async fn load_sqlite(src: &DbSource) -> Result<Dataset> {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
    use std::str::FromStr;

    let opts = SqliteConnectOptions::from_str(&src.connection_string)
        .with_context(|| format!("Invalid SQLite connection string: {}", src.connection_string))?
        .create_if_missing(true);

    let pool = SqlitePool::connect_with(opts)
        .await
        .with_context(|| format!("Cannot open SQLite database: {}", src.connection_string))?;

    let rows = sqlx::query(&src.query)
        .fetch_all(&pool)
        .await
        .with_context(|| format!("Cannot execute query: {}", src.query))?;

    let mut dataset = Dataset::new(&src.label);

    if let Some(first) = rows.first() {
        dataset.columns = first
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
    }

    for row in &rows {
        let mut map: Row = IndexMap::new();
        for col in row.columns() {
            let name = col.name().to_string();
            let type_name = col.type_info().name().to_lowercase();
            let val = sqlite_col_to_value(row, col.ordinal(), &type_name);
            map.insert(name, val);
        }
        dataset.rows.push(map);
    }

    pool.close().await;
    Ok(dataset)
}

fn sqlite_col_to_value(row: &sqlx::sqlite::SqliteRow, idx: usize, type_name: &str) -> Value {
    match type_name {
        "boolean" | "bool" => match row.try_get::<Option<bool>, _>(idx) {
            Ok(Some(b)) => Value::Bool(b),
            Ok(None) => Value::Null,
            Err(_) => Value::Null,
        },
        "integer" | "int" | "int2" | "int4" | "int8" | "bigint" | "smallint" | "tinyint" => {
            match row.try_get::<Option<i64>, _>(idx) {
                Ok(Some(i)) => Value::Integer(i),
                Ok(None) => Value::Null,
                Err(_) => Value::Null,
            }
        }
        "real" | "float" | "double" | "numeric" | "decimal" => {
            match row.try_get::<Option<f64>, _>(idx) {
                Ok(Some(f)) => Value::Float(f),
                Ok(None) => Value::Null,
                Err(_) => Value::Null,
            }
        }
        _ => match row.try_get::<Option<String>, _>(idx) {
            Ok(Some(s)) => Value::from_str_smart(&s),
            Ok(None) => Value::Null,
            Err(_) => {
                // Try integer fallback (SQLite is weakly typed)
                match row.try_get::<Option<i64>, _>(idx) {
                    Ok(Some(i)) => Value::Integer(i),
                    _ => match row.try_get::<Option<f64>, _>(idx) {
                        Ok(Some(f)) => Value::Float(f),
                        _ => Value::Null,
                    },
                }
            }
        },
    }
}

// ── PostgreSQL ────────────────────────────────────────────────────────────────

async fn load_postgres(src: &DbSource) -> Result<Dataset> {
    use sqlx::PgPool;

    let pool = PgPool::connect(&src.connection_string)
        .await
        .with_context(|| format!("Cannot connect to PostgreSQL: {}", src.connection_string))?;

    let rows = sqlx::query(&src.query)
        .fetch_all(&pool)
        .await
        .with_context(|| format!("Cannot execute query: {}", src.query))?;

    let mut dataset = Dataset::new(&src.label);

    if let Some(first) = rows.first() {
        dataset.columns = first
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
    }

    for row in &rows {
        let mut map: Row = IndexMap::new();
        for col in row.columns() {
            let name = col.name().to_string();
            let type_name = col.type_info().name().to_lowercase();
            let val = pg_col_to_value(row, col.ordinal(), &type_name);
            map.insert(name, val);
        }
        dataset.rows.push(map);
    }

    pool.close().await;
    Ok(dataset)
}

fn pg_col_to_value(row: &sqlx::postgres::PgRow, idx: usize, type_name: &str) -> Value {
    match type_name {
        "bool" => match row.try_get::<Option<bool>, _>(idx) {
            Ok(Some(b)) => Value::Bool(b),
            Ok(None) => Value::Null,
            Err(_) => Value::Null,
        },
        "int2" | "int4" | "int8" | "smallint" | "integer" | "bigint" | "serial" | "bigserial" => {
            match row.try_get::<Option<i64>, _>(idx) {
                Ok(Some(i)) => Value::Integer(i),
                Ok(None) => Value::Null,
                Err(_) => match row.try_get::<Option<i32>, _>(idx) {
                    Ok(Some(i)) => Value::Integer(i as i64),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                },
            }
        }
        "float4" | "float8" | "real" | "double precision" | "numeric" | "decimal" => {
            match row.try_get::<Option<f64>, _>(idx) {
                Ok(Some(f)) => Value::Float(f),
                Ok(None) => Value::Null,
                Err(_) => Value::Null,
            }
        }
        _ => match row.try_get::<Option<String>, _>(idx) {
            Ok(Some(s)) => Value::from_str_smart(&s),
            Ok(None) => Value::Null,
            Err(_) => Value::Null,
        },
    }
}

// ── MySQL / MariaDB ───────────────────────────────────────────────────────────

async fn load_mysql(src: &DbSource) -> Result<Dataset> {
    use sqlx::MySqlPool;

    let pool = MySqlPool::connect(&src.connection_string)
        .await
        .with_context(|| format!("Cannot connect to MySQL: {}", src.connection_string))?;

    let rows = sqlx::query(&src.query)
        .fetch_all(&pool)
        .await
        .with_context(|| format!("Cannot execute query: {}", src.query))?;

    let mut dataset = Dataset::new(&src.label);

    if let Some(first) = rows.first() {
        dataset.columns = first
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
    }

    for row in &rows {
        let mut map: Row = IndexMap::new();
        for col in row.columns() {
            let name = col.name().to_string();
            let type_name = col.type_info().name().to_lowercase();
            let val = mysql_col_to_value(row, col.ordinal(), &type_name);
            map.insert(name, val);
        }
        dataset.rows.push(map);
    }

    pool.close().await;
    Ok(dataset)
}

fn mysql_col_to_value(row: &sqlx::mysql::MySqlRow, idx: usize, type_name: &str) -> Value {
    match type_name {
        "tinyint(1)" | "boolean" | "bool" => match row.try_get::<Option<bool>, _>(idx) {
            Ok(Some(b)) => Value::Bool(b),
            Ok(None) => Value::Null,
            Err(_) => Value::Null,
        },
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => {
            match row.try_get::<Option<i64>, _>(idx) {
                Ok(Some(i)) => Value::Integer(i),
                Ok(None) => Value::Null,
                Err(_) => Value::Null,
            }
        }
        "float" | "double" | "decimal" | "numeric" => {
            match row.try_get::<Option<f64>, _>(idx) {
                Ok(Some(f)) => Value::Float(f),
                Ok(None) => Value::Null,
                Err(_) => Value::Null,
            }
        }
        _ => match row.try_get::<Option<String>, _>(idx) {
            Ok(Some(s)) => Value::from_str_smart(&s),
            Ok(None) => Value::Null,
            Err(_) => Value::Null,
        },
    }
}
