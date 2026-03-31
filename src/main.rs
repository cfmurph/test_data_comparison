use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use data_comparator::{
    compare::{compare, CompareOptions},
    output::{format_report, OutputFormat},
    sources::load,
    types::{DataSource, DbSource, FileFormat, FileSource},
};

// ── CLI definition ────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "data-comparator",
    about = "Compare data between files and databases",
    version,
    long_about = r#"
data-comparator compares datasets from multiple sources:

  file-to-file   Compare two CSV / TSV / JSON files
  db-to-db       Compare results of two SQL queries (SQLite / PostgreSQL / MySQL)
  db-to-file     Compare a SQL query result against a file

Supported file formats: .csv  .tsv  .json
Supported DB schemes  : sqlite://  postgres://  mysql://

Examples:
  data-comparator file-to-file left.csv right.csv --key id
  data-comparator db-to-db "sqlite://a.db" "SELECT * FROM t" \
                            "sqlite://b.db" "SELECT * FROM t" --key id
  data-comparator db-to-file "sqlite://a.db" "SELECT * FROM t" out.csv --key id
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
        /// Path to the LEFT file
        left: String,
        /// Path to the RIGHT file
        right: String,
        #[command(flatten)]
        opts: SharedOpts,
    },

    /// Compare two database queries
    DbToDb {
        /// Connection string for the LEFT database (e.g. sqlite://left.db)
        left_conn: String,
        /// SQL query for the LEFT database
        left_query: String,
        /// Connection string for the RIGHT database
        right_conn: String,
        /// SQL query for the RIGHT database
        right_query: String,
        #[command(flatten)]
        opts: SharedOpts,
    },

    /// Compare a database query against a file
    DbToFile {
        /// Connection string for the database (e.g. sqlite://db.sqlite3)
        db_conn: String,
        /// SQL query to execute
        db_query: String,
        /// Path to the file (CSV / TSV / JSON)
        file: String,
        #[command(flatten)]
        opts: SharedOpts,
    },
}

#[derive(clap::Args, Debug, Clone)]
struct SharedOpts {
    /// Column(s) to use as a join key (may be repeated).  When omitted, rows
    /// are compared positionally.
    #[arg(short, long = "key", value_name = "COLUMN")]
    key_columns: Vec<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    format: CliFormat,

    /// Ignore case when comparing strings
    #[arg(long)]
    ignore_case: bool,

    /// Trim leading/trailing whitespace before comparing
    #[arg(long)]
    trim: bool,

    /// Compare integers and floats as equal when numerically identical
    #[arg(long)]
    loose_numeric: bool,

    /// Column(s) to exclude from comparison (may be repeated)
    #[arg(long = "ignore-col", value_name = "COLUMN")]
    ignore_columns: Vec<String>,

    /// Exit with code 1 if any differences are found (useful for CI)
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
            let left_src = DataSource::File(FileSource {
                format: FileFormat::detect(&left),
                path: left,
            });
            let right_src = DataSource::File(FileSource {
                format: FileFormat::detect(&right),
                path: right,
            });
            run(left_src, right_src, opts).await
        }

        Commands::DbToDb {
            left_conn,
            left_query,
            right_conn,
            right_query,
            opts,
        } => {
            let left_src = DataSource::Database(DbSource {
                connection_string: left_conn.clone(),
                query: left_query,
                label: left_conn,
            });
            let right_src = DataSource::Database(DbSource {
                connection_string: right_conn.clone(),
                query: right_query,
                label: right_conn,
            });
            run(left_src, right_src, opts).await
        }

        Commands::DbToFile {
            db_conn,
            db_query,
            file,
            opts,
        } => {
            let left_src = DataSource::Database(DbSource {
                connection_string: db_conn.clone(),
                query: db_query,
                label: db_conn,
            });
            let right_src = DataSource::File(FileSource {
                format: FileFormat::detect(&file),
                path: file,
            });
            run(left_src, right_src, opts).await
        }
    }
}

async fn run(left: DataSource, right: DataSource, opts: SharedOpts) -> Result<()> {
    let left_ds = load(&left).await.context("Loading left data source")?;
    let right_ds = load(&right).await.context("Loading right data source")?;

    let cmp_opts = CompareOptions {
        key_columns: opts.key_columns,
        loose_numeric: opts.loose_numeric,
        ignore_case: opts.ignore_case,
        trim_whitespace: opts.trim,
        ignore_columns: opts.ignore_columns,
    };

    let report = compare(&left_ds, &right_ds, &cmp_opts);

    let out_fmt = match opts.format {
        CliFormat::Table => OutputFormat::Table,
        CliFormat::Json => OutputFormat::Json,
        CliFormat::Summary => OutputFormat::Summary,
    };

    print!("{}", format_report(&report, &out_fmt));

    if opts.fail_on_diff && report.has_differences() {
        std::process::exit(1);
    }

    Ok(())
}
