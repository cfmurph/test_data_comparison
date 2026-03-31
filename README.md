# data-comparator

A Rust CLI tool that compares datasets across multiple source types:

| Mode | Left | Right |
|------|------|-------|
| `file-to-file` | CSV / TSV / JSON file | CSV / TSV / JSON file |
| `db-to-db` | SQL query result | SQL query result |
| `db-to-file` | SQL query result | CSV / TSV / JSON file |

Supported databases: **SQLite**, **PostgreSQL**, **MySQL / MariaDB**

---

## Build

```bash
cargo build --release
# binary at: target/release/data-comparator
```

---

## Usage

### file-to-file

```bash
data-comparator file-to-file left.csv right.csv
data-comparator file-to-file left.csv right.csv --key id
data-comparator file-to-file left.json right.json --key sku --format json
data-comparator file-to-file left.tsv right.tsv --ignore-case --trim
```

### db-to-db

```bash
# SQLite
data-comparator db-to-db \
  "sqlite://production.db" "SELECT * FROM orders" \
  "sqlite://staging.db"    "SELECT * FROM orders" \
  --key order_id

# PostgreSQL
data-comparator db-to-db \
  "postgres://user:pass@host/prod"    "SELECT id, amount FROM payments" \
  "postgres://user:pass@host/staging" "SELECT id, amount FROM payments" \
  --key id

# MySQL
data-comparator db-to-db \
  "mysql://user:pass@host/db_a" "SELECT * FROM customers" \
  "mysql://user:pass@host/db_b" "SELECT * FROM customers" \
  --key customer_id
```

### db-to-file

```bash
data-comparator db-to-file \
  "sqlite://app.db" "SELECT id, name, salary FROM employees ORDER BY id" \
  expected_employees.csv \
  --key id
```

---

## Options

| Flag | Description |
|------|-------------|
| `--key COLUMN` | Join key column(s); repeatable. Omit for positional comparison. |
| `--format table\|json\|summary` | Output format (default: `table`) |
| `--ignore-case` | Case-insensitive string comparison |
| `--trim` | Strip whitespace before comparing |
| `--loose-numeric` | Treat `100` (int) and `100.0` (float) as equal |
| `--ignore-col COLUMN` | Exclude a column from comparison; repeatable |
| `--fail-on-diff` | Exit with code 1 if any differences are found (useful in CI) |

---

## Output formats

### `table` (default)

```
Comparing  employees_a.csv (5 rows)  vs  employees_b.csv (5 rows)
  Matching rows:         3
  Left-only rows:        1
  Right-only rows:       1
  Modified rows:         1

✖ Datasets differ.

  ~ MODIFIED id=2
    +--------+-------+-------+
    | Column | Left  | Right |
    +========================+
    | salary | 65000 | 67500 |
    +--------+-------+-------+

  − LEFT ONLY id=4
  + RIGHT ONLY id=6
```

### `summary`

One-paragraph stats block, no row detail.

### `json`

Machine-readable JSON report containing the full diff tree.

---

## Architecture

```
src/
├── lib.rs              – module declarations
├── main.rs             – CLI (clap) + async entry point
├── types.rs            – Value, Dataset, DataSource, FileSource, DbSource
├── sources/
│   ├── mod.rs          – unified load() dispatcher
│   ├── file.rs         – CSV / TSV / JSON loading
│   └── db.rs           – SQLite / PostgreSQL / MySQL loading (sqlx)
├── compare/
│   ├── mod.rs
│   ├── engine.rs       – positional & key-based comparison logic
│   └── result.rs       – ComparisonReport, RowDiff, ColumnDiff types
└── output/
    ├── mod.rs
    └── formatter.rs    – table / json / summary rendering
```

---

## Running tests

```bash
cargo test
```

Tests cover:

- Identical TSV files → no differences
- CSV key-based diff (modified row, left-only, right-only)
- JSON key-based diff
- Positional comparison
- `--ignore-case` option
- `--loose-numeric` option  
- SQLite db-to-db identical
- SQLite db-to-db with differences
- SQLite db-to-file comparison
