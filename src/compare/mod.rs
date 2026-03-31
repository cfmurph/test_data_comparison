pub mod engine;
pub mod result;

pub use engine::{compare, CompareOptions};
pub use result::{ColumnDiff, ComparisonReport, DiffKind, RowDiff};
