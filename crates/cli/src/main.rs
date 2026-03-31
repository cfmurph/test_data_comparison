mod reporter;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dc_core::{
    compare::{compare, CompareOptions},
    config::{CompareConfig, CompareSection, SourceConfig},
    value_cmp::{
        CaseInsensitiveComparator, NumericToleranceComparator, StrictComparator, TrimComparator,
    },
};
use dc_sources::{
    db::DbAdapter,
    file::{FileAdapter, FileFormat},
};
use reporter::{FileReporter, OutputFormat, StdoutReporter, TeeReporter};
use std::sync::Arc;

// ── CLI definition ────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "data-comparator",
    version,
    about = "Compare datasets across files and databases",
    long_about = r#"
Compare datasets from any supported source:

  file-to-file  – CSV / TSV / JSON vs CSV / TSV / JSON
  db-to-db      – SQL query vs SQL query  (sqlite:// / postgres:// / mysql://)
  db-to-file    – SQL query result vs file
  run           – Load all settings from a TOML config file

Examples:
  data-comparator file-to-file left.csv right.csv --key id
  data-comparator db-to-db "sqlite://a.db" "SELECT * FROM t" \
                            "sqlite://b.db" "SELECT * FROM t" --key id
  data-comparator run --config compare.toml
"#
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compare two files (CSV / TSV / JSON)
    FileToFile {
        left: String,
        right: String,
        #[command(flatten)]
        opts: SharedOpts,
    },
    /// Compare two database queries
    DbToDb {
        left_conn: String,
        left_query: String,
        right_conn: String,
        right_query: String,
        #[command(flatten)]
        opts: SharedOpts,
    },
    /// Compare a database query result against a file
    DbToFile {
        db_conn: String,
        db_query: String,
        file: String,
        #[command(flatten)]
        opts: SharedOpts,
    },
    /// Load comparison settings from a TOML config file
    Run {
        /// Path to the TOML config file
        #[arg(short, long)]
        config: String,
    },
}

#[derive(clap::Args, Debug, Clone)]
struct SharedOpts {
    /// Column(s) to use as join key (repeatable)
    #[arg(short, long = "key", value_name = "COLUMN")]
    key_columns: Vec<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    format: CliFormat,

    /// Write output to a file (in addition to stdout)
    #[arg(long, value_name = "FILE")]
    output: Option<String>,

    /// Ignore case when comparing strings
    #[arg(long)]
    ignore_case: bool,

    /// Trim whitespace before comparing strings
    #[arg(long)]
    trim: bool,

    /// Compare integers and floats as equal when numerically identical
    #[arg(long)]
    loose_numeric: bool,

    /// Float tolerance for numeric columns (implies --loose-numeric)
    #[arg(long, value_name = "EPSILON")]
    epsilon: Option<f64>,

    /// Columns to exclude from comparison (repeatable)
    #[arg(long = "ignore-col", value_name = "COLUMN")]
    ignore_columns: Vec<String>,

    /// Rename a LEFT column to match a RIGHT column: LEFT:RIGHT (repeatable)
    #[arg(long = "map-col", value_name = "LEFT:RIGHT")]
    column_mappings: Vec<String>,

    /// Stop after recording this many row diffs
    #[arg(long, value_name = "N")]
    max_diffs: Option<usize>,

    /// Exit code 1 if any differences found (CI use)
    #[arg(long)]
    fail_on_diff: bool,
}

#[derive(ValueEnum, Clone, Debug)]
enum CliFormat {
    Table,
    Json,
    Summary,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::FileToFile { left, right, opts } => {
            let left_ds = dc_sources::load_file(&FileAdapter::new(&left))
                .with_context(|| format!("Loading left file: {left}"))?;
            let right_ds = dc_sources::load_file(&FileAdapter::new(&right))
                .with_context(|| format!("Loading right file: {right}"))?;
            let cmp_opts = build_compare_opts(&opts);
            let fail = opts.fail_on_diff;
            let reporter = build_reporter(&opts);
            let report = compare(&left_ds, &right_ds, &cmp_opts);
            reporter.report(&report)?;
            if fail && report.has_differences() {
                std::process::exit(1);
            }
        }

        Commands::DbToDb {
            left_conn,
            left_query,
            right_conn,
            right_query,
            opts,
        } => {
            let left_ds = dc_sources::load_db(&DbAdapter::new(&left_conn, &left_query, &left_conn))
                .await
                .with_context(|| format!("Loading left DB: {left_conn}"))?;
            let right_ds =
                dc_sources::load_db(&DbAdapter::new(&right_conn, &right_query, &right_conn))
                    .await
                    .with_context(|| format!("Loading right DB: {right_conn}"))?;
            let cmp_opts = build_compare_opts(&opts);
            let fail = opts.fail_on_diff;
            let reporter = build_reporter(&opts);
            let report = compare(&left_ds, &right_ds, &cmp_opts);
            reporter.report(&report)?;
            if fail && report.has_differences() {
                std::process::exit(1);
            }
        }

        Commands::DbToFile {
            db_conn,
            db_query,
            file,
            opts,
        } => {
            let left_ds = dc_sources::load_db(&DbAdapter::new(&db_conn, &db_query, &db_conn))
                .await
                .with_context(|| format!("Loading DB: {db_conn}"))?;
            let right_ds = dc_sources::load_file(&FileAdapter::new(&file))
                .with_context(|| format!("Loading file: {file}"))?;
            let cmp_opts = build_compare_opts(&opts);
            let fail = opts.fail_on_diff;
            let reporter = build_reporter(&opts);
            let report = compare(&left_ds, &right_ds, &cmp_opts);
            reporter.report(&report)?;
            if fail && report.has_differences() {
                std::process::exit(1);
            }
        }

        Commands::Run { config } => {
            run_from_config(&config).await?;
        }
    }

    Ok(())
}

// ── Config-file driven run ────────────────────────────────────────────────────

async fn run_from_config(config_path: &str) -> Result<()> {
    let cfg = CompareConfig::from_file(config_path)
        .with_context(|| format!("Loading config: {config_path}"))?;

    let left_ds = load_from_source_config(
        cfg.left
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Config missing [left] section"))?,
    )
    .await?;

    let right_ds = load_from_source_config(
        cfg.right
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Config missing [right] section"))?,
    )
    .await?;

    let cmp_opts = build_compare_opts_from_config(&cfg.compare);

    let report = compare(&left_ds, &right_ds, &cmp_opts);

    let fmt = OutputFormat::from_str(&cfg.compare.output_format);
    let reporter: Box<dyn dc_core::reporter::Reporter> = match &cfg.compare.output_file {
        Some(path) => Box::new(TeeReporter {
            stdout: StdoutReporter { format: fmt.clone() },
            file: FileReporter { path: path.clone(), format: fmt },
        }),
        None => Box::new(StdoutReporter { format: fmt }),
    };

    reporter.report(&report)?;
    Ok(())
}

async fn load_from_source_config(src: &SourceConfig) -> Result<dc_core::Dataset> {
    match src.source_type.to_lowercase().as_str() {
        "file" => {
            let path = src
                .path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("File source missing `path`"))?;
            let mut adapter = FileAdapter::new(path);
            if let Some(fmt) = &src.format {
                adapter = adapter.with_format(match fmt.to_lowercase().as_str() {
                    "json" => FileFormat::Json,
                    "tsv" => FileFormat::Tsv,
                    _ => FileFormat::Csv,
                });
            }
            dc_sources::load_file(&adapter)
        }
        "database" | "db" => {
            let conn = src
                .connection_string
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("DB source missing `connection_string`"))?;
            let query = src
                .query
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("DB source missing `query`"))?;
            let label = src.label.as_deref().unwrap_or(conn);
            dc_sources::load_db(&DbAdapter::new(conn, query, label)).await
        }
        other => anyhow::bail!("Unknown source type: {other}"),
    }
}

// ── Option builders ───────────────────────────────────────────────────────────

fn build_compare_opts(opts: &SharedOpts) -> CompareOptions {
    let mut cmp = CompareOptions::new();
    cmp.key_columns = opts.key_columns.clone();
    cmp.ignore_columns = opts.ignore_columns.clone();
    cmp.max_diffs = opts.max_diffs;

    for mapping in &opts.column_mappings {
        if let Some((l, r)) = mapping.split_once(':') {
            cmp.column_mappings.insert(l.to_string(), r.to_string());
        }
    }

    cmp.default_comparator = if let Some(eps) = opts.epsilon {
        Arc::new(NumericToleranceComparator { epsilon: eps })
    } else if opts.loose_numeric {
        Arc::new(NumericToleranceComparator::default())
    } else if opts.ignore_case && opts.trim {
        // both: trim first then lower
        Arc::new(ComboComparator { ignore_case: true, trim: true })
    } else if opts.ignore_case {
        Arc::new(CaseInsensitiveComparator)
    } else if opts.trim {
        Arc::new(TrimComparator)
    } else {
        Arc::new(StrictComparator)
    };

    cmp
}

fn build_compare_opts_from_config(sec: &CompareSection) -> CompareOptions {
    let mut cmp = CompareOptions::new();
    cmp.key_columns = sec.keys.clone();
    cmp.ignore_columns = sec.ignore_columns.clone();
    cmp.column_mappings = sec.column_mappings.clone();
    cmp.max_diffs = sec.max_diffs;

    cmp.default_comparator = comparator_from_name(&sec.default_comparator, None);

    for (col, col_opts) in &sec.column_options {
        if let Some(name) = &col_opts.comparator {
            cmp.column_comparators
                .insert(col.clone(), comparator_from_name(name, col_opts.epsilon));
        }
    }

    cmp
}

fn comparator_from_name(
    name: &str,
    epsilon: Option<f64>,
) -> Arc<dyn dc_core::value_cmp::ValueComparator> {
    match name.to_lowercase().as_str() {
        "case_insensitive" | "ignore_case" => Arc::new(CaseInsensitiveComparator),
        "trim" => Arc::new(TrimComparator),
        "numeric_tolerance" | "loose_numeric" => Arc::new(NumericToleranceComparator {
            epsilon: epsilon.unwrap_or(1e-9),
        }),
        _ => Arc::new(StrictComparator),
    }
}

fn build_reporter(opts: &SharedOpts) -> Box<dyn dc_core::reporter::Reporter> {
    let fmt = match opts.format {
        CliFormat::Table => OutputFormat::Table,
        CliFormat::Json => OutputFormat::Json,
        CliFormat::Summary => OutputFormat::Summary,
    };

    match &opts.output {
        Some(path) => Box::new(TeeReporter {
            stdout: StdoutReporter { format: fmt.clone() },
            file: FileReporter { path: path.clone(), format: fmt },
        }),
        None => Box::new(StdoutReporter { format: fmt }),
    }
}

// ── ComboComparator (trim + ignore_case) ──────────────────────────────────────

struct ComboComparator {
    ignore_case: bool,
    trim: bool,
}

impl dc_core::value_cmp::ValueComparator for ComboComparator {
    fn equals(&self, left: &dc_core::Value, right: &dc_core::Value) -> bool {
        use dc_core::Value;
        match (left, right) {
            (Value::Text(a), Value::Text(b)) => {
                let mut a = a.clone();
                let mut b = b.clone();
                if self.trim {
                    a = a.trim().to_owned();
                    b = b.trim().to_owned();
                }
                if self.ignore_case {
                    a = a.to_lowercase();
                    b = b.to_lowercase();
                }
                a == b
            }
            _ => left == right,
        }
    }
    fn name(&self) -> &str {
        "combo"
    }
}
