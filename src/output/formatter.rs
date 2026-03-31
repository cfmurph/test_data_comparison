use crate::compare::result::{ComparisonReport, DiffKind};
use colored::*;
use comfy_table::{Cell, CellAlignment, Color, ContentArrangement, Table};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human-readable table + summary (default).
    Table,
    /// JSON report suitable for programmatic use.
    Json,
    /// Condensed one-line-per-diff summary.
    Summary,
}

pub fn format_report(report: &ComparisonReport, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Table => format_table(report),
        OutputFormat::Json => format_json(report),
        OutputFormat::Summary => format_summary(report),
    }
}

// ── Summary ──────────────────────────────────────────────────────────────────

fn format_summary(report: &ComparisonReport) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "Comparing  {} ({} rows)  vs  {} ({} rows)\n",
        report.left_name.bold(),
        report.left_row_count,
        report.right_name.bold(),
        report.right_row_count,
    ));

    if !report.left_only_columns.is_empty() {
        out.push_str(&format!(
            "  Columns only in LEFT:  {}\n",
            report.left_only_columns.join(", ").yellow()
        ));
    }
    if !report.right_only_columns.is_empty() {
        out.push_str(&format!(
            "  Columns only in RIGHT: {}\n",
            report.right_only_columns.join(", ").yellow()
        ));
    }

    out.push_str(&format!(
        "  Matching rows:         {}\n",
        report.matching_rows.to_string().green()
    ));
    out.push_str(&format!(
        "  Left-only rows:        {}\n",
        report.left_only_rows().to_string().red()
    ));
    out.push_str(&format!(
        "  Right-only rows:       {}\n",
        report.right_only_rows().to_string().red()
    ));
    out.push_str(&format!(
        "  Modified rows:         {}\n",
        report.modified_rows().to_string().yellow()
    ));

    if !report.has_differences() {
        out.push_str(&format!("\n{}\n", "✔ Datasets are identical.".green().bold()));
    } else {
        out.push_str(&format!("\n{}\n", "✖ Datasets differ.".red().bold()));
    }

    out
}

// ── Table ────────────────────────────────────────────────────────────────────

fn format_table(report: &ComparisonReport) -> String {
    let mut out = format_summary(report);

    if report.row_diffs.is_empty() {
        return out;
    }

    out.push('\n');

    for diff in &report.row_diffs {
        match diff.kind {
            DiffKind::LeftOnly => {
                let key_str = key_label(&diff.key, diff.left_index);
                out.push_str(&format!(
                    "  {} {}\n",
                    "− LEFT ONLY".red().bold(),
                    key_str
                ));
            }
            DiffKind::RightOnly => {
                let key_str = key_label(&diff.key, diff.right_index);
                out.push_str(&format!(
                    "  {} {}\n",
                    "+ RIGHT ONLY".green().bold(),
                    key_str
                ));
            }
            DiffKind::Modified => {
                let key_str = key_label(&diff.key, diff.left_index);
                out.push_str(&format!(
                    "  {} {}\n",
                    "~ MODIFIED".yellow().bold(),
                    key_str
                ));

                let mut table = Table::new();
                table.set_content_arrangement(ContentArrangement::Dynamic);
                table.set_header(vec![
                    Cell::new("Column").fg(Color::Cyan),
                    Cell::new("Left").fg(Color::Red),
                    Cell::new("Right").fg(Color::Green),
                ]);

                for cd in &diff.column_diffs {
                    table.add_row(vec![
                        Cell::new(&cd.column),
                        Cell::new(cd.left.to_string()).fg(Color::Red).set_alignment(CellAlignment::Left),
                        Cell::new(cd.right.to_string()).fg(Color::Green).set_alignment(CellAlignment::Left),
                    ]);
                }

                for line in table.to_string().lines() {
                    out.push_str(&format!("    {line}\n"));
                }
                out.push('\n');
            }
        }
    }

    out
}

fn key_label(key: &[(String, crate::types::Value)], fallback_idx: Option<usize>) -> String {
    if key.is_empty() {
        format!("(row {})", fallback_idx.map(|i| i + 1).unwrap_or(0))
    } else {
        key.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

// ── JSON ─────────────────────────────────────────────────────────────────────

fn format_json(report: &ComparisonReport) -> String {
    serde_json::to_string_pretty(report).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}
