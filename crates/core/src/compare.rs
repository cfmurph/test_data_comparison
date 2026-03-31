use crate::types::{Dataset, Row, Value};
use crate::value_cmp::{StrictComparator, ValueComparator};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;

pub use result::{ColumnDiff, ComparisonReport, DiffKind, RowDiff};

mod result {
    use crate::types::Value;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ColumnDiff {
        pub column: String,
        pub left: Value,
        pub right: Value,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub enum DiffKind {
        LeftOnly,
        RightOnly,
        Modified,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RowDiff {
        pub kind: DiffKind,
        pub left_index: Option<usize>,
        pub right_index: Option<usize>,
        /// Key values used to match this row (empty in positional mode).
        pub key: Vec<(String, Value)>,
        /// Cell-level diffs (populated only for `Modified` rows).
        pub column_diffs: Vec<ColumnDiff>,
    }

    /// Top-level comparison result.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ComparisonReport {
        pub left_name: String,
        pub right_name: String,
        pub left_row_count: usize,
        pub right_row_count: usize,
        /// Columns present in left but not right.
        pub left_only_columns: Vec<String>,
        /// Columns present in right but not left.
        pub right_only_columns: Vec<String>,
        /// Columns present in both (and not ignored).
        pub common_columns: Vec<String>,
        /// Every row-level difference.
        pub row_diffs: Vec<RowDiff>,
        /// Rows that matched with no differences.
        pub matching_rows: usize,
        /// Per-column diff counts (column → number of differing rows).
        pub column_diff_counts: std::collections::HashMap<String, usize>,
    }

    impl ComparisonReport {
        pub fn has_differences(&self) -> bool {
            !self.row_diffs.is_empty()
                || !self.left_only_columns.is_empty()
                || !self.right_only_columns.is_empty()
        }

        pub fn left_only_rows(&self) -> usize {
            self.row_diffs
                .iter()
                .filter(|r| r.kind == DiffKind::LeftOnly)
                .count()
        }

        pub fn right_only_rows(&self) -> usize {
            self.row_diffs
                .iter()
                .filter(|r| r.kind == DiffKind::RightOnly)
                .count()
        }

        pub fn modified_rows(&self) -> usize {
            self.row_diffs
                .iter()
                .filter(|r| r.kind == DiffKind::Modified)
                .count()
        }
    }
}

// ── CompareOptions ────────────────────────────────────────────────────────────

/// All options that control a comparison run.
pub struct CompareOptions {
    /// Columns to join on. Empty → positional mode.
    pub key_columns: Vec<String>,

    /// Columns to exclude from value comparison (keys still work).
    pub ignore_columns: Vec<String>,

    /// Rename left column → right column before comparing.
    /// e.g. `{"amount_usd" → "amount"}` lets mismatched names be compared.
    pub column_mappings: HashMap<String, String>,

    /// Stop recording individual diffs after this many row diffs.
    /// `None` = unlimited.
    pub max_diffs: Option<usize>,

    /// Default comparator applied to every column that has no column-specific
    /// override (see `column_comparators`).
    pub default_comparator: Arc<dyn ValueComparator>,

    /// Per-column comparator overrides.
    /// Key: left-side column name (after mapping).
    pub column_comparators: HashMap<String, Arc<dyn ValueComparator>>,
}

impl std::fmt::Debug for CompareOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompareOptions")
            .field("key_columns", &self.key_columns)
            .field("ignore_columns", &self.ignore_columns)
            .field("column_mappings", &self.column_mappings)
            .field("max_diffs", &self.max_diffs)
            .field("default_comparator", &self.default_comparator.name())
            .field(
                "column_comparators",
                &self
                    .column_comparators
                    .keys()
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl CompareOptions {
    pub fn new() -> Self {
        Self {
            key_columns: Vec::new(),
            ignore_columns: Vec::new(),
            column_mappings: HashMap::new(),
            max_diffs: None,
            default_comparator: Arc::new(StrictComparator),
            column_comparators: HashMap::new(),
        }
    }
}

impl Default for CompareOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl CompareOptions {
    fn comparator_for(&self, col: &str) -> &dyn ValueComparator {
        self.column_comparators
            .get(col)
            .map(|c| c.as_ref())
            .unwrap_or_else(|| self.default_comparator.as_ref())
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn compare(left: &Dataset, right: &Dataset, opts: &CompareOptions) -> ComparisonReport {
    let left_col_set: std::collections::HashSet<&str> =
        left.columns.iter().map(String::as_str).collect();
    let right_col_set: std::collections::HashSet<&str> =
        right.columns.iter().map(String::as_str).collect();

    let left_only_columns: Vec<String> = left
        .columns
        .iter()
        .filter(|c| !right_col_set.contains(effective_right(c, &opts.column_mappings).as_str()))
        .cloned()
        .collect();

    let right_only_columns: Vec<String> = right
        .columns
        .iter()
        .filter(|c| {
            !left_col_set.contains(c.as_str())
                && !opts.column_mappings.values().any(|v| v == *c)
        })
        .cloned()
        .collect();

    // Build the set of (left_col, right_col) pairs to compare.
    let common_pairs: Vec<(String, String)> = left
        .columns
        .iter()
        .filter(|c| !opts.ignore_columns.contains(c))
        .filter_map(|lc| {
            let rc = effective_right(lc, &opts.column_mappings);
            if right_col_set.contains(rc.as_str()) {
                Some((lc.clone(), rc))
            } else {
                None
            }
        })
        .collect();

    let common_columns: Vec<String> = common_pairs.iter().map(|(l, _)| l.clone()).collect();

    let row_diffs = if opts.key_columns.is_empty() {
        compare_positional(&left.rows, &right.rows, &common_pairs, opts)
    } else {
        compare_keyed(&left.rows, &right.rows, &common_pairs, opts)
    };

    // Count diffs per column
    let mut column_diff_counts: HashMap<String, usize> = HashMap::new();
    for rd in &row_diffs {
        for cd in &rd.column_diffs {
            *column_diff_counts.entry(cd.column.clone()).or_insert(0) += 1;
        }
    }

    let matching_rows = {
        let modified = row_diffs.iter().filter(|r| r.kind == DiffKind::Modified).count();
        let left_only = row_diffs.iter().filter(|r| r.kind == DiffKind::LeftOnly).count();
        if opts.key_columns.is_empty() {
            let compared = left.rows.len().min(right.rows.len());
            compared - modified
        } else {
            left.rows.len().saturating_sub(left_only + modified)
        }
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
        column_diff_counts,
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Return the right-side column name for a left-side column, applying any mapping.
fn effective_right(left_col: &str, mappings: &HashMap<String, String>) -> String {
    mappings
        .get(left_col)
        .cloned()
        .unwrap_or_else(|| left_col.to_owned())
}

fn row_key(row: &Row, key_columns: &[String], mappings: &HashMap<String, String>) -> String {
    key_columns
        .iter()
        .map(|k| {
            let effective = effective_right(k, mappings);
            row.get(&effective)
                .or_else(|| row.get(k))
                .map(|v| v.to_string())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join("\x00")
}

// ── Positional ────────────────────────────────────────────────────────────────

fn compare_positional(
    left: &[Row],
    right: &[Row],
    pairs: &[(String, String)],
    opts: &CompareOptions,
) -> Vec<RowDiff> {
    let mut diffs = Vec::new();
    let max_len = left.len().max(right.len());
    let limit = opts.max_diffs.unwrap_or(usize::MAX);

    for i in 0..max_len {
        if diffs.len() >= limit {
            break;
        }
        match (left.get(i), right.get(i)) {
            (Some(lr), Some(rr)) => {
                let col_diffs = diff_cells(lr, rr, pairs, opts);
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
            (Some(_), None) => diffs.push(RowDiff {
                kind: DiffKind::LeftOnly,
                left_index: Some(i),
                right_index: None,
                key: vec![],
                column_diffs: vec![],
            }),
            (None, Some(_)) => diffs.push(RowDiff {
                kind: DiffKind::RightOnly,
                left_index: None,
                right_index: Some(i),
                key: vec![],
                column_diffs: vec![],
            }),
            (None, None) => unreachable!(),
        }
    }
    diffs
}

// ── Key-based ─────────────────────────────────────────────────────────────────

fn compare_keyed(
    left: &[Row],
    right: &[Row],
    pairs: &[(String, String)],
    opts: &CompareOptions,
) -> Vec<RowDiff> {
    // Index right rows by key (using right-side column names)
    let mut right_map: IndexMap<String, (usize, &Row)> = IndexMap::new();
    for (i, row) in right.iter().enumerate() {
        // For the right side use the right-column names directly
        let key = right_row_key(row, &opts.key_columns, &opts.column_mappings);
        right_map.insert(key, (i, row));
    }

    let mut diffs = Vec::new();
    let mut matched_keys = std::collections::HashSet::new();
    let limit = opts.max_diffs.unwrap_or(usize::MAX);

    for (left_idx, left_row) in left.iter().enumerate() {
        if diffs.len() >= limit {
            break;
        }
        let key = row_key(left_row, &opts.key_columns, &opts.column_mappings);
        let key_values: Vec<(String, Value)> = opts
            .key_columns
            .iter()
            .map(|k| (k.clone(), left_row.get(k).cloned().unwrap_or(Value::Null)))
            .collect();

        if let Some((right_idx, right_row)) = right_map.get(&key) {
            matched_keys.insert(key.clone());
            let col_diffs = diff_cells(left_row, right_row, pairs, opts);
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
        if diffs.len() >= limit {
            break;
        }
        if !matched_keys.contains(key) {
            let key_values: Vec<(String, Value)> = opts
                .key_columns
                .iter()
                .map(|k| {
                    let effective = effective_right(k, &opts.column_mappings);
                    (
                        k.clone(),
                        right_row
                            .get(&effective)
                            .or_else(|| right_row.get(k))
                            .cloned()
                            .unwrap_or(Value::Null),
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

/// Build a join key from a RIGHT-side row (using right-column names directly).
fn right_row_key(
    row: &Row,
    key_columns: &[String],
    mappings: &HashMap<String, String>,
) -> String {
    key_columns
        .iter()
        .map(|k| {
            let effective = effective_right(k, mappings);
            row.get(&effective)
                .or_else(|| row.get(k))
                .map(|v| v.to_string())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join("\x00")
}

// ── Cell diff ─────────────────────────────────────────────────────────────────

fn diff_cells(
    left: &Row,
    right: &Row,
    pairs: &[(String, String)],
    opts: &CompareOptions,
) -> Vec<ColumnDiff> {
    let mut diffs = Vec::new();
    for (left_col, right_col) in pairs {
        let lv = left.get(left_col).cloned().unwrap_or(Value::Null);
        let rv = right.get(right_col).cloned().unwrap_or(Value::Null);
        let cmp = opts.comparator_for(left_col);
        if !cmp.equals(&lv, &rv) {
            diffs.push(ColumnDiff {
                column: left_col.clone(),
                left: lv,
                right: rv,
            });
        }
    }
    diffs
}
