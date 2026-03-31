use crate::types::{Dataset, Row, Value};
use indexmap::IndexMap;

use super::result::{ColumnDiff, ComparisonReport, DiffKind, RowDiff};

/// Options controlling comparison behaviour.
#[derive(Debug, Clone, Default)]
pub struct CompareOptions {
    /// Column(s) to use as a join key. When empty, rows are compared
    /// positionally (by index).
    pub key_columns: Vec<String>,
    /// If true, numeric types are compared loosely (int vs float).
    pub loose_numeric: bool,
    /// If true, string comparison is case-insensitive.
    pub ignore_case: bool,
    /// If true, leading/trailing whitespace is stripped before comparison.
    pub trim_whitespace: bool,
    /// Columns to exclude from comparison (still used as keys if listed in
    /// `key_columns`).
    pub ignore_columns: Vec<String>,
}

/// Run a comparison between two datasets and return a structured report.
pub fn compare(left: &Dataset, right: &Dataset, opts: &CompareOptions) -> ComparisonReport {
    let left_col_set: std::collections::HashSet<&str> =
        left.columns.iter().map(String::as_str).collect();
    let right_col_set: std::collections::HashSet<&str> =
        right.columns.iter().map(String::as_str).collect();

    let left_only_columns: Vec<String> = left
        .columns
        .iter()
        .filter(|c| !right_col_set.contains(c.as_str()))
        .cloned()
        .collect();

    let right_only_columns: Vec<String> = right
        .columns
        .iter()
        .filter(|c| !left_col_set.contains(c.as_str()))
        .cloned()
        .collect();

    let common_columns: Vec<String> = left
        .columns
        .iter()
        .filter(|c| right_col_set.contains(c.as_str()))
        .filter(|c| !opts.ignore_columns.contains(c))
        .cloned()
        .collect();

    let row_diffs = if opts.key_columns.is_empty() {
        compare_positional(&left.rows, &right.rows, &common_columns, opts)
    } else {
        compare_keyed(&left.rows, &right.rows, &common_columns, opts)
    };

    let matching_rows = if opts.key_columns.is_empty() {
        let compared = left.rows.len().min(right.rows.len());
        let modified: usize = row_diffs.iter().filter(|r| r.kind == DiffKind::Modified).count();
        compared - modified
    } else {
        let total_left = left.rows.len();
        let left_only = row_diffs.iter().filter(|r| r.kind == DiffKind::LeftOnly).count();
        let modified = row_diffs.iter().filter(|r| r.kind == DiffKind::Modified).count();
        total_left - left_only - modified
    };

    ComparisonReport {
        left_name: left.name.clone(),
        right_name: right.name.clone(),
        left_row_count: left.rows.len(),
        right_row_count: right.rows.len(),
        left_only_columns,
        right_only_columns,
        common_columns,
        row_diffs,
        matching_rows,
    }
}

// ── Positional comparison ─────────────────────────────────────────────────────

fn compare_positional(
    left: &[Row],
    right: &[Row],
    common_columns: &[String],
    opts: &CompareOptions,
) -> Vec<RowDiff> {
    let mut diffs = Vec::new();
    let max_len = left.len().max(right.len());

    for i in 0..max_len {
        match (left.get(i), right.get(i)) {
            (Some(lr), Some(rr)) => {
                let col_diffs = diff_row(lr, rr, common_columns, opts);
                if !col_diffs.is_empty() {
                    diffs.push(RowDiff {
                        kind: DiffKind::Modified,
                        left_index: Some(i),
                        right_index: Some(i),
                        key: vec![],
                        column_diffs: col_diffs,
                    });
                }
            }
            (Some(_), None) => {
                diffs.push(RowDiff {
                    kind: DiffKind::LeftOnly,
                    left_index: Some(i),
                    right_index: None,
                    key: vec![],
                    column_diffs: vec![],
                });
            }
            (None, Some(_)) => {
                diffs.push(RowDiff {
                    kind: DiffKind::RightOnly,
                    left_index: None,
                    right_index: Some(i),
                    key: vec![],
                    column_diffs: vec![],
                });
            }
            (None, None) => unreachable!(),
        }
    }

    diffs
}

// ── Key-based comparison ──────────────────────────────────────────────────────

fn row_key(row: &Row, key_columns: &[String]) -> String {
    key_columns
        .iter()
        .map(|k| {
            row.get(k)
                .map(|v| v.to_string())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join("\x00")
}

fn compare_keyed(
    left: &[Row],
    right: &[Row],
    common_columns: &[String],
    opts: &CompareOptions,
) -> Vec<RowDiff> {
    // Index right rows by key
    let mut right_map: IndexMap<String, (usize, &Row)> = IndexMap::new();
    for (i, row) in right.iter().enumerate() {
        let key = row_key(row, &opts.key_columns);
        right_map.insert(key, (i, row));
    }

    let mut diffs = Vec::new();
    let mut matched_right_keys = std::collections::HashSet::new();

    for (left_idx, left_row) in left.iter().enumerate() {
        let key = row_key(left_row, &opts.key_columns);
        let key_values: Vec<(String, Value)> = opts
            .key_columns
            .iter()
            .map(|k| {
                (
                    k.clone(),
                    left_row.get(k).cloned().unwrap_or(Value::Null),
                )
            })
            .collect();

        if let Some((right_idx, right_row)) = right_map.get(&key) {
            matched_right_keys.insert(key.clone());
            let col_diffs = diff_row(left_row, right_row, common_columns, opts);
            if !col_diffs.is_empty() {
                diffs.push(RowDiff {
                    kind: DiffKind::Modified,
                    left_index: Some(left_idx),
                    right_index: Some(*right_idx),
                    key: key_values,
                    column_diffs: col_diffs,
                });
            }
        } else {
            diffs.push(RowDiff {
                kind: DiffKind::LeftOnly,
                left_index: Some(left_idx),
                right_index: None,
                key: key_values,
                column_diffs: vec![],
            });
        }
    }

    // Right-only rows
    for (key, (right_idx, right_row)) in &right_map {
        if !matched_right_keys.contains(key) {
            let key_values: Vec<(String, Value)> = opts
                .key_columns
                .iter()
                .map(|k| {
                    (
                        k.clone(),
                        right_row.get(k).cloned().unwrap_or(Value::Null),
                    )
                })
                .collect();
            diffs.push(RowDiff {
                kind: DiffKind::RightOnly,
                left_index: None,
                right_index: Some(*right_idx),
                key: key_values,
                column_diffs: vec![],
            });
        }
    }

    diffs
}

// ── Cell-level diffing ────────────────────────────────────────────────────────

fn normalise(v: &Value, opts: &CompareOptions) -> Value {
    match v {
        Value::Text(s) => {
            let mut s = s.clone();
            if opts.trim_whitespace {
                s = s.trim().to_owned();
            }
            if opts.ignore_case {
                s = s.to_lowercase();
            }
            Value::Text(s)
        }
        other => other.clone(),
    }
}

fn values_equal(a: &Value, b: &Value, opts: &CompareOptions) -> bool {
    let na = normalise(a, opts);
    let nb = normalise(b, opts);
    if opts.loose_numeric {
        na.loose_eq(&nb)
    } else {
        na == nb
    }
}

fn diff_row(
    left: &Row,
    right: &Row,
    common_columns: &[String],
    opts: &CompareOptions,
) -> Vec<ColumnDiff> {
    let mut diffs = Vec::new();
    for col in common_columns {
        let lv = left.get(col).cloned().unwrap_or(Value::Null);
        let rv = right.get(col).cloned().unwrap_or(Value::Null);
        if !values_equal(&lv, &rv, opts) {
            diffs.push(ColumnDiff {
                column: col.clone(),
                left: lv,
                right: rv,
            });
        }
    }
    diffs
}
