# data-comparator

A modular Rust toolkit for comparing datasets across files and databases.

---

## Workspace layout

```
crates/
├── core/      – types, comparison engine, traits, TOML config model (no I/O)
├── sources/   – data source adapters (file, SQLite, PostgreSQL, MySQL)
└── cli/       – binary: CLI parser, reporter implementations
examples/      – sample TOML config files
```

---

## Features

| Capability | Details |
|---|---|
| **File sources** | CSV, TSV, JSON (array of objects) |
| **Database sources** | SQLite, PostgreSQL, MySQL / MariaDB (feature-gated) |
| **Comparison modes** | Key-based (join) or positional (by index) |
| **Pluggable comparators** | `ValueComparator` trait; per-column overrides |
| **Built-in comparators** | Strict, CaseInsensitive, Trim, NumericTolerance |
| **Column mappings** | Compare `amount_usd` ↔ `amount` across rename |
| **Column ignoring** | Exclude audit columns (`updated_at`, etc.) |
| **Diff capping** | `--max-diffs N` / `max_diffs` in config |
| **Output reporters** | `Reporter` trait; stdout, file, or tee (both) |
| **Output formats** | table (colored), json, summary |
| **TOML config** | Full comparison described in a versioned file |
| **CI integration** | `--fail-on-diff` exits 1 on any difference |

---

## Build

```bash
cargo build --release
# binary: target/release/data-comparator

# Build without PostgreSQL / MySQL (SQLite only):
cargo build --release --no-default-features --features sqlite
```

---

## CLI usage

### file-to-file

```bash
data-comparator file-to-file left.csv right.csv
data-comparator file-to-file left.csv right.csv --key id --key region
data-comparator file-to-file left.json right.json --key sku --format json
data-comparator file-to-file left.tsv right.tsv --ignore-case --trim
data-comparator file-to-file a.csv b.csv --map-col amount_usd:amount --key id
data-comparator file-to-file a.csv b.csv --key id --output diff.json --format json
data-comparator file-to-file a.csv b.csv --key id --max-diffs 10 --fail-on-diff
```

### db-to-db

```bash
# SQLite
data-comparator db-to-db \
  "sqlite://prod.db"    "SELECT * FROM orders ORDER BY id" \
  "sqlite://staging.db" "SELECT * FROM orders ORDER BY id" \
  --key id

# PostgreSQL
data-comparator db-to-db \
  "postgres://user:pw@prod/db"    "SELECT id, amount FROM payments" \
  "postgres://user:pw@staging/db" "SELECT id, amount FROM payments" \
  --key id --epsilon 0.01

# MySQL
data-comparator db-to-db \
  "mysql://user:pw@host/db_a" "SELECT * FROM customers" \
  "mysql://user:pw@host/db_b" "SELECT * FROM customers" \
  --key customer_id
```

### db-to-file

```bash
data-comparator db-to-file \
  "sqlite://app.db" "SELECT id, name, salary FROM employees ORDER BY id" \
  expected_employees.csv \
  --key id
```

### Config-file driven (recommended for repeatable runs)

```bash
data-comparator run --config compare.toml
```

---

## TOML config format

```toml
[left]
type = "file"          # or "database"
path = "left.csv"

[right]
type              = "database"
connection_string = "postgres://user:pw@host/db"
query             = "SELECT id, name, amount FROM orders"
label             = "production"

[compare]
keys           = ["id"]
ignore_columns = ["updated_at", "created_at"]
max_diffs      = 100
output_format  = "table"    # table | json | summary
output_file    = "diff.json"

[compare.column_mappings]
amount_usd = "amount"       # left column → right column

[compare.column_options.amount]
comparator = "numeric_tolerance"
epsilon    = 0.001

[compare.column_options.name]
comparator = "case_insensitive"
```

See `examples/` for full sample configs.

---

## CLI options reference

| Flag | Description |
|---|---|
| `--key COLUMN` | Join key column(s) (repeatable) |
| `--format table\|json\|summary` | Output format (default: `table`) |
| `--output FILE` | Write output to file (also prints to stdout) |
| `--ignore-case` | Case-insensitive string comparison |
| `--trim` | Strip whitespace before comparing |
| `--loose-numeric` | Accept int/float cross-type equality |
| `--epsilon N` | Numeric tolerance (implies `--loose-numeric`) |
| `--ignore-col COLUMN` | Exclude column (repeatable) |
| `--map-col LEFT:RIGHT` | Rename left column to match right (repeatable) |
| `--max-diffs N` | Stop after N row diffs |
| `--fail-on-diff` | Exit 1 if any differences found |

---

## Extending the library

### Custom comparator

```rust
use dc_core::value_cmp::ValueComparator;
use dc_core::Value;

struct FuzzyDateComparator;

impl ValueComparator for FuzzyDateComparator {
    fn equals(&self, left: &Value, right: &Value) -> bool {
        // parse and compare dates with tolerance
        todo!()
    }
    fn name(&self) -> &str { "fuzzy_date" }
}

// Register per-column:
opts.column_comparators.insert("created_at".into(), Arc::new(FuzzyDateComparator));
```

### Custom reporter

```rust
use dc_core::reporter::Reporter;
use dc_core::compare::ComparisonReport;

struct SlackReporter { webhook_url: String }

impl Reporter for SlackReporter {
    fn report(&self, report: &ComparisonReport) -> anyhow::Result<()> {
        // POST to Slack, S3, email, etc.
        todo!()
    }
    fn name(&self) -> &str { "slack" }
}
```

### Custom data source adapter

Add a module in `dc_sources/src/` implementing:

```rust
pub async fn load(adapter: &MyAdapter) -> anyhow::Result<dc_core::Dataset> {
    // read from HTTP API, Parquet file, Arrow IPC, etc.
    todo!()
}
```

Gate it with a Cargo feature and expose it via `dc_sources::lib`.

---

## Database feature flags

```toml
# In your Cargo.toml:
dc_sources = { path = "...", default-features = false, features = ["sqlite"] }
```

| Feature | Default | Enables |
|---|---|---|
| `sqlite` | ✓ | SQLite via sqlx |
| `postgres` | ✓ | PostgreSQL via sqlx |
| `mysql` | ✓ | MySQL / MariaDB via sqlx |

---

## Running tests

```bash
cargo test
```

15 integration tests covering:

- Identical TSV files → no differences
- CSV key-based diff (modified, left-only, right-only rows)
- JSON key-based diff
- Positional comparison
- `CaseInsensitiveComparator`
- `NumericToleranceComparator`
- Per-column comparator overrides
- Column mappings (`amount_usd` ↔ `amount`)
- `ignore_columns`
- `max_diffs` cap
- `column_diff_counts` per-column statistics
- SQLite db-to-db identical
- SQLite db-to-db with differences
- SQLite db-to-file cross-source
- TOML config round-trip
