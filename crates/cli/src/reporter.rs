//! Built-in [`Reporter`] implementations.

use dc_core::{compare::ComparisonReport, reporter::Reporter};
use anyhow::Result;
use colored::*;
use comfy_table::{Cell, CellAlignment, Color, ContentArrangement, Table};

// ── Output format ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Summary,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => OutputFormat::Json,
            "summary" => OutputFormat::Summary,
            _ => OutputFormat::Table,
        }
    }
}

// ── Stdout reporter ───────────────────────────────────────────────────────────

pub struct StdoutReporter {
    pub format: OutputFormat,
}

impl Reporter for StdoutReporter {
    fn report(&self, report: &ComparisonReport) -> Result<()> {
        print!("{}", render(report, &self.format));
        Ok(())
    }
    fn name(&self) -> &str {
        "stdout"
    }
}

// ── File reporter ─────────────────────────────────────────────────────────────

pub struct FileReporter {
    pub path: String,
    pub format: OutputFormat,
}

impl Reporter for FileReporter {
    fn report(&self, report: &ComparisonReport) -> Result<()> {
        let content = render(report, &self.format);
        std::fs::write(&self.path, &content)
            .map_err(|e| anyhow::anyhow!("Cannot write output file {}: {e}", self.path))?;
        Ok(())
    }
    fn name(&self) -> &str {
        "file"
    }
}

// ── Tee reporter (stdout + file) ──────────────────────────────────────────────

pub struct TeeReporter {
    pub stdout: StdoutReporter,
    pub file: FileReporter,
}

impl Reporter for TeeReporter {
    fn report(&self, report: &ComparisonReport) -> Result<()> {
        self.stdout.report(report)?;
        self.file.report(report)
    }
    fn name(&self) -> &str {
        "tee"
    }
}

// ── Rendering ─────────────────────────────────────────────────────────────────

pub fn render(report: &ComparisonReport, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Table => render_table(report),
        OutputFormat::Json => render_json(report),
        OutputFormat::Summary => render_summary(report),
    }
}

fn render_summary(report: &ComparisonReport) -> String {
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

    if !report.column_diff_counts.is_empty() {
        out.push_str("  Diffs by column:\n");
        let mut counts: Vec<(&String, &usize)> = report.column_diff_counts.iter().collect();
        counts.sort_by(|a, b| b.1.cmp(a.1));
        for (col, count) in counts {
            out.push_str(&format!("    {:30} {}\n", col, count.to_string().yellow()));
        }
    }

    if !report.has_differences() {
        out.push_str(&format!("\n{}\n", "✔ Datasets are identical.".green().bold()));
    } else {
        out.push_str(&format!("\n{}\n", "✖ Datasets differ.".red().bold()));
    }

    out
}

fn render_table(report: &ComparisonReport) -> String {
    let mut out = render_summary(report);

    if report.row_diffs.is_empty() {
        return out;
    }

    out.push('\n');

    for diff in &report.row_diffs {
        match diff.kind {
            dc_core::compare::DiffKind::LeftOnly => {
                out.push_str(&format!(
                    "  {} {}\n",
                    "− LEFT ONLY".red().bold(),
                    key_label(&diff.key, diff.left_index)
                ));
            }
            dc_core::compare::DiffKind::RightOnly => {
                out.push_str(&format!(
                    "  {} {}\n",
                    "+ RIGHT ONLY".green().bold(),
                    key_label(&diff.key, diff.right_index)
                ));
            }
            dc_core::compare::DiffKind::Modified => {
                out.push_str(&format!(
                    "  {} {}\n",
                    "~ MODIFIED".yellow().bold(),
                    key_label(&diff.key, diff.left_index)
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
                        Cell::new(cd.left.to_string())
                            .fg(Color::Red)
                            .set_alignment(CellAlignment::Left),
                        Cell::new(cd.right.to_string())
                            .fg(Color::Green)
                            .set_alignment(CellAlignment::Left),
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

fn key_label(key: &[(String, dc_core::Value)], fallback_idx: Option<usize>) -> String {
    if key.is_empty() {
        format!("(row {})", fallback_idx.map(|i| i + 1).unwrap_or(0))
    } else {
        key.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn render_json(report: &ComparisonReport) -> String {
    serde_json::to_string_pretty(report).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}
