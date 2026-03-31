use crate::types::Value;
use serde::{Deserialize, Serialize};

/// A single cell difference within a matched row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDiff {
    pub column: String,
    pub left: Value,
    pub right: Value,
}

/// Classification of a row-level difference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffKind {
    /// Row exists only in the left dataset.
    LeftOnly,
    /// Row exists only in the right dataset.
    RightOnly,
    /// Row exists in both but has cell-level differences.
    Modified,
}

/// All differences found for one logical row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowDiff {
    pub kind: DiffKind,
    /// Position (0-indexed) in the respective dataset.
    pub left_index: Option<usize>,
    pub right_index: Option<usize>,
    /// Key value(s) used to match this row (empty when using positional mode).
    pub key: Vec<(String, Value)>,
    /// Cell-level differences (only populated for `Modified` rows).
    pub column_diffs: Vec<ColumnDiff>,
}

/// Top-level comparison report.
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
    /// Columns present in both.
    pub common_columns: Vec<String>,
    pub row_diffs: Vec<RowDiff>,
    /// Total matching rows with no differences.
    pub matching_rows: usize,
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
