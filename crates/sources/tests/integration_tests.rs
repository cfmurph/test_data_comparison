use dc_core::{
    compare::{compare, CompareOptions, DiffKind},
    Dataset, Row, Value,
};
use dc_sources::{
    db::DbAdapter,
    file::FileAdapter,
    load_db, load_file,
};
use indexmap::IndexMap;

// ── Fixture helper ────────────────────────────────────────────────────────────

fn fixture(name: &str) -> String {
    format!(
        "{}/tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn file_adapter(name: &str) -> FileAdapter {
    FileAdapter::new(fixture(name))
}

// ── File-to-file: identical TSV ───────────────────────────────────────────────

#[tokio::test]
async fn test_tsv_identical() {
    let left = load_file(&file_adapter("identical_a.tsv")).unwrap();
    let right = load_file(&file_adapter("identical_b.tsv")).unwrap();
    let report = compare(&left, &right, &CompareOptions::new());
    assert!(!report.has_differences());
    assert_eq!(report.matching_rows, 3);
}

// ── File-to-file: CSV key-based diff ─────────────────────────────────────────

#[tokio::test]
async fn test_csv_key_based_diff() {
    let left = load_file(&file_adapter("employees_a.csv")).unwrap();
    let right = load_file(&file_adapter("employees_b.csv")).unwrap();
    let mut opts = CompareOptions::new();
    opts.key_columns = vec!["id".to_string()];
    let report = compare(&left, &right, &opts);

    assert!(report.has_differences());
    assert_eq!(report.left_only_rows(), 1, "Dave should be left-only");
    assert_eq!(report.right_only_rows(), 1, "Frank should be right-only");
    assert_eq!(report.modified_rows(), 1, "Bob's salary changed");

    let modified = report.row_diffs.iter().find(|r| r.kind == DiffKind::Modified).unwrap();
    assert_eq!(modified.column_diffs.len(), 1);
    assert_eq!(modified.column_diffs[0].column, "salary");
    assert_eq!(modified.column_diffs[0].left.to_string(), "65000");
    assert_eq!(modified.column_diffs[0].right.to_string(), "67500");
}

// ── File-to-file: JSON key-based diff ────────────────────────────────────────

#[tokio::test]
async fn test_json_key_based_diff() {
    let left = load_file(&file_adapter("products_a.json")).unwrap();
    let right = load_file(&file_adapter("products_b.json")).unwrap();
    let mut opts = CompareOptions::new();
    opts.key_columns = vec!["sku".to_string()];
    let report = compare(&left, &right, &opts);

    assert!(report.has_differences());
    assert_eq!(report.left_only_rows(), 1);
    assert_eq!(report.right_only_rows(), 1);
    assert_eq!(report.modified_rows(), 2);
}

// ── File-to-file: positional diff ────────────────────────────────────────────

#[tokio::test]
async fn test_positional_diff() {
    let left = load_file(&file_adapter("employees_a.csv")).unwrap();
    let right = load_file(&file_adapter("employees_b.csv")).unwrap();
    let report = compare(&left, &right, &CompareOptions::new());
    assert!(report.has_differences());
    assert_eq!(report.left_only_rows(), 0);
    assert_eq!(report.right_only_rows(), 0);
}

// ── ValueComparator: ignore_case ──────────────────────────────────────────────

#[tokio::test]
async fn test_ignore_case() {
    use dc_core::value_cmp::CaseInsensitiveComparator;
    use std::sync::Arc;

    let mut left = Dataset::new("left");
    left.columns = vec!["k".into(), "v".into()];
    let mut r: Row = IndexMap::new();
    r.insert("k".into(), Value::Integer(1));
    r.insert("v".into(), Value::Text("Hello".into()));
    left.rows.push(r);

    let mut right = Dataset::new("right");
    right.columns = vec!["k".into(), "v".into()];
    let mut r2: Row = IndexMap::new();
    r2.insert("k".into(), Value::Integer(1));
    r2.insert("v".into(), Value::Text("hello".into()));
    right.rows.push(r2);

    let strict = compare(&left, &right, &CompareOptions::new());
    assert!(strict.has_differences());

    let mut opts = CompareOptions::new();
    opts.default_comparator = Arc::new(CaseInsensitiveComparator);
    assert!(!compare(&left, &right, &opts).has_differences());
}

// ── ValueComparator: numeric_tolerance ───────────────────────────────────────

#[tokio::test]
async fn test_numeric_tolerance() {
    use dc_core::value_cmp::NumericToleranceComparator;
    use std::sync::Arc;

    let mut left = Dataset::new("l");
    left.columns = vec!["id".into(), "score".into()];
    let mut r: Row = IndexMap::new();
    r.insert("id".into(), Value::Integer(1));
    r.insert("score".into(), Value::Integer(100));
    left.rows.push(r);

    let mut right = Dataset::new("r");
    right.columns = vec!["id".into(), "score".into()];
    let mut r2: Row = IndexMap::new();
    r2.insert("id".into(), Value::Integer(1));
    r2.insert("score".into(), Value::Float(100.0));
    right.rows.push(r2);

    assert!(compare(&left, &right, &CompareOptions::new()).has_differences());

    let mut opts = CompareOptions::new();
    opts.default_comparator = Arc::new(NumericToleranceComparator::default());
    assert!(!compare(&left, &right, &opts).has_differences());
}

// ── ValueComparator: per-column override ─────────────────────────────────────

#[tokio::test]
async fn test_per_column_comparator() {
    use dc_core::value_cmp::{CaseInsensitiveComparator, StrictComparator};
    use std::sync::Arc;

    let mut left = Dataset::new("l");
    left.columns = vec!["id".into(), "name".into(), "code".into()];
    let mut r: Row = IndexMap::new();
    r.insert("id".into(), Value::Integer(1));
    r.insert("name".into(), Value::Text("Alice".into()));
    r.insert("code".into(), Value::Text("ABC".into()));
    left.rows.push(r);

    let mut right = Dataset::new("r");
    right.columns = vec!["id".into(), "name".into(), "code".into()];
    let mut r2: Row = IndexMap::new();
    r2.insert("id".into(), Value::Integer(1));
    r2.insert("name".into(), Value::Text("alice".into())); // case diff
    r2.insert("code".into(), Value::Text("xyz".into())); // real diff
    right.rows.push(r2);

    let mut opts = CompareOptions::new();
    // name: ignore case → equal; code: strict → differs
    opts.column_comparators.insert("name".into(), Arc::new(CaseInsensitiveComparator));
    opts.column_comparators.insert("code".into(), Arc::new(StrictComparator));

    let report = compare(&left, &right, &opts);
    assert!(report.has_differences());
    let diff = &report.row_diffs[0];
    assert_eq!(diff.column_diffs.len(), 1);
    assert_eq!(diff.column_diffs[0].column, "code");
}

// ── CompareOptions: column_mappings ──────────────────────────────────────────

#[tokio::test]
async fn test_column_mapping() {
    let mut left = Dataset::new("l");
    left.columns = vec!["id".into(), "amount_usd".into()];
    let mut r: Row = IndexMap::new();
    r.insert("id".into(), Value::Integer(1));
    r.insert("amount_usd".into(), Value::Float(99.99));
    left.rows.push(r);

    let mut right = Dataset::new("r");
    right.columns = vec!["id".into(), "amount".into()];
    let mut r2: Row = IndexMap::new();
    r2.insert("id".into(), Value::Integer(1));
    r2.insert("amount".into(), Value::Float(99.99));
    right.rows.push(r2);

    let mut opts = CompareOptions::new();
    opts.column_mappings.insert("amount_usd".into(), "amount".into());
    let report = compare(&left, &right, &opts);
    assert!(!report.has_differences(), "mapped columns should match");
}

// ── CompareOptions: max_diffs ─────────────────────────────────────────────────

#[tokio::test]
async fn test_max_diffs() {
    let left = load_file(&file_adapter("employees_a.csv")).unwrap();
    let right = load_file(&file_adapter("employees_b.csv")).unwrap();
    let mut opts = CompareOptions::new();
    opts.key_columns = vec!["id".to_string()];
    opts.max_diffs = Some(1);
    let report = compare(&left, &right, &opts);
    assert!(report.row_diffs.len() <= 1);
}

// ── CompareOptions: ignore_columns ───────────────────────────────────────────

#[tokio::test]
async fn test_ignore_columns() {
    let left = load_file(&file_adapter("employees_a.csv")).unwrap();
    let right = load_file(&file_adapter("employees_b.csv")).unwrap();
    let mut opts = CompareOptions::new();
    opts.key_columns = vec!["id".to_string()];
    // ignore salary → Bob no longer modified; Dave still left-only
    opts.ignore_columns = vec!["salary".to_string()];
    let report = compare(&left, &right, &opts);
    assert_eq!(report.modified_rows(), 0, "salary ignored → no modifications");
}

// ── column_diff_counts ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_column_diff_counts() {
    let left = load_file(&file_adapter("employees_a.csv")).unwrap();
    let right = load_file(&file_adapter("employees_b.csv")).unwrap();
    let mut opts = CompareOptions::new();
    opts.key_columns = vec!["id".to_string()];
    let report = compare(&left, &right, &opts);
    // Bob's salary changed → salary should have a count of 1
    assert_eq!(report.column_diff_counts.get("salary").copied().unwrap_or(0), 1);
}

// ── SQLite: db-to-db identical ────────────────────────────────────────────────

async fn make_db(url: &str, rows: &[(i64, &str, i64)]) {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
    use std::str::FromStr;
    let opts = SqliteConnectOptions::from_str(url).unwrap().create_if_missing(true);
    let pool = SqlitePool::connect_with(opts).await.unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS emp (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for (id, name, salary) in rows {
        sqlx::query("INSERT OR REPLACE INTO emp (id,name,salary) VALUES(?,?,?)")
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
    let url_a = format!("sqlite://{}", dir.path().join("a.db").display());
    let url_b = format!("sqlite://{}", dir.path().join("b.db").display());
    let rows: &[(i64, &str, i64)] = &[(1, "Alice", 90000), (2, "Bob", 65000)];
    make_db(&url_a, rows).await;
    make_db(&url_b, rows).await;

    let q = "SELECT * FROM emp ORDER BY id";
    let left = load_db(&DbAdapter::new(&url_a, q, "db_a")).await.unwrap();
    let right = load_db(&DbAdapter::new(&url_b, q, "db_b")).await.unwrap();
    assert!(!compare(&left, &right, &CompareOptions::new()).has_differences());
}

// ── SQLite: db-to-db with diffs ───────────────────────────────────────────────

#[tokio::test]
async fn test_db_to_db_diffs() {
    let dir = tempfile::tempdir().unwrap();
    let url_a = format!("sqlite://{}", dir.path().join("a.db").display());
    let url_b = format!("sqlite://{}", dir.path().join("b.db").display());
    make_db(&url_a, &[(1, "Alice", 90000), (2, "Bob", 65000)]).await;
    make_db(&url_b, &[(1, "Alice", 92000), (3, "Carol", 80000)]).await;

    let q = "SELECT * FROM emp ORDER BY id";
    let left = load_db(&DbAdapter::new(&url_a, q, "a")).await.unwrap();
    let right = load_db(&DbAdapter::new(&url_b, q, "b")).await.unwrap();
    let mut opts = CompareOptions::new();
    opts.key_columns = vec!["id".to_string()];
    let report = compare(&left, &right, &opts);
    assert_eq!(report.modified_rows(), 1);
    assert_eq!(report.left_only_rows(), 1);
    assert_eq!(report.right_only_rows(), 1);
}

// ── SQLite: db-to-file ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_db_to_file() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("t.db").display());
    make_db(
        &url,
        &[(1,"Alice",90000),(2,"Bob",65000),(3,"Carol",95000),(4,"Dave",58000),(5,"Eve",70000)],
    )
    .await;

    let db_ds = load_db(&DbAdapter::new(
        &url,
        "SELECT id, name, salary FROM emp ORDER BY id",
        "db",
    ))
    .await
    .unwrap();

    let file_ds = load_file(&file_adapter("employees_a.csv")).unwrap();
    let mut opts = CompareOptions::new();
    opts.key_columns = vec!["id".to_string()];
    let report = compare(&db_ds, &file_ds, &opts);
    assert!(report.right_only_columns.contains(&"department".to_string()));
    assert_eq!(report.modified_rows(), 0);
}

// ── TOML config round-trip ────────────────────────────────────────────────────

#[test]
fn test_config_round_trip() {
    use dc_core::config::CompareConfig;

    let toml = r#"
[left]
type = "file"
path = "left.csv"

[right]
type = "file"
path = "right.csv"

[compare]
keys = ["id"]
ignore_columns = ["updated_at"]
max_diffs = 50
output_format = "json"

[compare.column_mappings]
amount_usd = "amount"

[compare.column_options.price]
comparator = "numeric_tolerance"
epsilon = 0.01
"#;

    let cfg = CompareConfig::from_toml(toml).unwrap();
    assert_eq!(cfg.compare.keys, vec!["id"]);
    assert_eq!(cfg.compare.ignore_columns, vec!["updated_at"]);
    assert_eq!(cfg.compare.max_diffs, Some(50));
    assert_eq!(cfg.compare.output_format, "json");
    assert_eq!(
        cfg.compare.column_mappings.get("amount_usd").unwrap(),
        "amount"
    );
    let price_opts = cfg.compare.column_options.get("price").unwrap();
    assert_eq!(price_opts.comparator.as_deref(), Some("numeric_tolerance"));
    assert!((price_opts.epsilon.unwrap() - 0.01).abs() < 1e-9);
}
