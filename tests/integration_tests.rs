use data_comparator::{
    compare::{compare, CompareOptions},
    compare::result::DiffKind,
    sources::load,
    types::{DataSource, DbSource, FileFormat, FileSource},
};

// ── Helper: fixture path ──────────────────────────────────────────────────────

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn file_source(name: &str) -> DataSource {
    let path = fixture(name);
    DataSource::File(FileSource {
        format: FileFormat::detect(&path),
        path,
    })
}

// ── File-to-file tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_tsv_identical_files() {
    let left = load(&file_source("identical_a.tsv")).await.unwrap();
    let right = load(&file_source("identical_b.tsv")).await.unwrap();
    let report = compare(&left, &right, &CompareOptions::default());

    assert!(!report.has_differences(), "Expected no differences");
    assert_eq!(report.matching_rows, 3);
    assert!(report.row_diffs.is_empty());
}

#[tokio::test]
async fn test_csv_key_based_diff() {
    let left = load(&file_source("employees_a.csv")).await.unwrap();
    let right = load(&file_source("employees_b.csv")).await.unwrap();

    let opts = CompareOptions {
        key_columns: vec!["id".to_string()],
        ..Default::default()
    };
    let report = compare(&left, &right, &opts);

    // Row id=2 has salary change: 65000 -> 67500
    // Row id=4 is left-only (Dave)
    // Row id=6 is right-only (Frank)
    assert!(report.has_differences());
    assert_eq!(report.left_only_rows(), 1, "Dave should be left-only");
    assert_eq!(report.right_only_rows(), 1, "Frank should be right-only");
    assert_eq!(report.modified_rows(), 1, "Bob's salary changed");

    let modified = report
        .row_diffs
        .iter()
        .find(|r| r.kind == DiffKind::Modified)
        .unwrap();
    assert_eq!(modified.column_diffs.len(), 1);
    assert_eq!(modified.column_diffs[0].column, "salary");
    assert_eq!(modified.column_diffs[0].left.to_string(), "65000");
    assert_eq!(modified.column_diffs[0].right.to_string(), "67500");
}

#[tokio::test]
async fn test_json_key_based_diff() {
    let left = load(&file_source("products_a.json")).await.unwrap();
    let right = load(&file_source("products_b.json")).await.unwrap();

    let opts = CompareOptions {
        key_columns: vec!["sku".to_string()],
        ..Default::default()
    };
    let report = compare(&left, &right, &opts);

    // P002 price changed, P003 in_stock changed
    // P004 is left-only, P005 is right-only
    assert!(report.has_differences());
    assert_eq!(report.left_only_rows(), 1);
    assert_eq!(report.right_only_rows(), 1);
    assert_eq!(report.modified_rows(), 2);
}

#[tokio::test]
async fn test_positional_diff() {
    let left = load(&file_source("employees_a.csv")).await.unwrap();
    let right = load(&file_source("employees_b.csv")).await.unwrap();

    // Both have 5 rows; positional comparison finds row-level differences.
    let report = compare(&left, &right, &CompareOptions::default());

    assert!(report.has_differences());
    // Positional: same row count → no left-only / right-only
    assert_eq!(report.left_only_rows(), 0);
    assert_eq!(report.right_only_rows(), 0);
}

#[tokio::test]
async fn test_ignore_case_option() {
    use data_comparator::types::{Dataset, Row, Value};
    use indexmap::IndexMap;

    let mut left = Dataset::new("left");
    left.columns = vec!["key".to_string(), "val".to_string()];
    let mut r: Row = IndexMap::new();
    r.insert("key".to_string(), Value::Integer(1));
    r.insert("val".to_string(), Value::Text("Hello".to_string()));
    left.rows.push(r);

    let mut right = Dataset::new("right");
    right.columns = vec!["key".to_string(), "val".to_string()];
    let mut r2: Row = IndexMap::new();
    r2.insert("key".to_string(), Value::Integer(1));
    r2.insert("val".to_string(), Value::Text("hello".to_string()));
    right.rows.push(r2);

    let strict = compare(&left, &right, &CompareOptions::default());
    assert!(strict.has_differences(), "strict: Hello != hello");

    let loose = compare(
        &left,
        &right,
        &CompareOptions {
            ignore_case: true,
            ..Default::default()
        },
    );
    assert!(!loose.has_differences(), "ignore_case: Hello == hello");
}

#[tokio::test]
async fn test_loose_numeric_option() {
    use data_comparator::types::{Dataset, Row, Value};
    use indexmap::IndexMap;

    let mut left = Dataset::new("left");
    left.columns = vec!["id".to_string(), "score".to_string()];
    let mut r: Row = IndexMap::new();
    r.insert("id".to_string(), Value::Integer(1));
    r.insert("score".to_string(), Value::Integer(100));
    left.rows.push(r);

    let mut right = Dataset::new("right");
    right.columns = vec!["id".to_string(), "score".to_string()];
    let mut r2: Row = IndexMap::new();
    r2.insert("id".to_string(), Value::Integer(1));
    r2.insert("score".to_string(), Value::Float(100.0));
    right.rows.push(r2);

    let strict = compare(&left, &right, &CompareOptions::default());
    assert!(strict.has_differences(), "strict: 100 (int) != 100.0 (float)");

    let loose = compare(
        &left,
        &right,
        &CompareOptions {
            loose_numeric: true,
            ..Default::default()
        },
    );
    assert!(!loose.has_differences(), "loose: 100 == 100.0");
}

// ── SQLite db-to-db tests (via sqlx) ─────────────────────────────────────────

async fn make_sqlite_db(url: &str, rows: &[(i64, &str, i64)]) {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
    use std::str::FromStr;
    let opts = SqliteConnectOptions::from_str(url)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(opts).await.unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS employees \
         (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for (id, name, salary) in rows {
        sqlx::query(
            "INSERT OR REPLACE INTO employees (id, name, salary) VALUES (?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(salary)
        .execute(&pool)
        .await
        .unwrap();
    }
    pool.close().await;
}

#[tokio::test]
async fn test_db_to_db_identical() {
    let dir = tempfile::tempdir().unwrap();
    let url_a = format!("sqlite://{}", dir.path().join("a.db").to_str().unwrap());
    let url_b = format!("sqlite://{}", dir.path().join("b.db").to_str().unwrap());

    let rows: Vec<(i64, &str, i64)> = vec![(1, "Alice", 90000), (2, "Bob", 65000)];
    make_sqlite_db(&url_a, &rows).await;
    make_sqlite_db(&url_b, &rows).await;

    let query = "SELECT * FROM employees ORDER BY id".to_string();
    let left = load(&DataSource::Database(DbSource {
        connection_string: url_a,
        query: query.clone(),
        label: "db_a".to_string(),
    }))
    .await
    .unwrap();

    let right = load(&DataSource::Database(DbSource {
        connection_string: url_b,
        query,
        label: "db_b".to_string(),
    }))
    .await
    .unwrap();

    let report = compare(&left, &right, &CompareOptions::default());
    assert!(!report.has_differences());
}

#[tokio::test]
async fn test_db_to_db_with_diffs() {
    let dir = tempfile::tempdir().unwrap();
    let url_a = format!("sqlite://{}", dir.path().join("a.db").to_str().unwrap());
    let url_b = format!("sqlite://{}", dir.path().join("b.db").to_str().unwrap());

    make_sqlite_db(&url_a, &[(1, "Alice", 90000), (2, "Bob", 65000)]).await;
    make_sqlite_db(&url_b, &[(1, "Alice", 92000), (3, "Carol", 80000)]).await;

    let query = "SELECT * FROM employees ORDER BY id".to_string();
    let left = load(&DataSource::Database(DbSource {
        connection_string: url_a,
        query: query.clone(),
        label: "db_a".to_string(),
    }))
    .await
    .unwrap();

    let right = load(&DataSource::Database(DbSource {
        connection_string: url_b,
        query,
        label: "db_b".to_string(),
    }))
    .await
    .unwrap();

    let opts = CompareOptions {
        key_columns: vec!["id".to_string()],
        ..Default::default()
    };
    let report = compare(&left, &right, &opts);

    assert!(report.has_differences());
    assert_eq!(report.modified_rows(), 1); // Alice salary diff
    assert_eq!(report.left_only_rows(), 1); // Bob not in b
    assert_eq!(report.right_only_rows(), 1); // Carol not in a
}

// ── db-to-file test ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_db_to_file() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("test.db").to_str().unwrap());

    make_sqlite_db(
        &url,
        &[
            (1, "Alice", 90000),
            (2, "Bob", 65000),
            (3, "Carol", 95000),
            (4, "Dave", 58000),
            (5, "Eve", 70000),
        ],
    )
    .await;

    let db_ds = load(&DataSource::Database(DbSource {
        connection_string: url,
        query: "SELECT id, name, salary FROM employees ORDER BY id".to_string(),
        label: "test_db".to_string(),
    }))
    .await
    .unwrap();

    // employees_a.csv has id, name, department, salary
    let file_ds = load(&file_source("employees_a.csv")).await.unwrap();

    let opts = CompareOptions {
        key_columns: vec!["id".to_string()],
        ..Default::default()
    };
    let report = compare(&db_ds, &file_ds, &opts);

    // DB has no "department" column → right_only_columns
    assert!(report.right_only_columns.contains(&"department".to_string()));
    // The salary / name values should match
    assert_eq!(report.modified_rows(), 0);
    assert_eq!(report.left_only_rows(), 0);
    assert_eq!(report.right_only_rows(), 0);
}
