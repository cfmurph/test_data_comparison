//! Pluggable value-comparison strategies.
//!
//! Implement [`ValueComparator`] to define custom equality rules for any
//! column. The comparison engine calls the registered comparator (or the
//! default one) for every cell pair.

use crate::types::Value;

/// Trait that decides whether two [`Value`]s should be considered equal.
///
/// Implement this to build custom strategies: fuzzy dates, currency rounding,
/// regex normalisation, domain-specific enumerations, etc.
pub trait ValueComparator: Send + Sync {
    /// Return `true` if `left` and `right` should be treated as equal.
    fn equals(&self, left: &Value, right: &Value) -> bool;

    /// Human-readable name used in diagnostics.
    fn name(&self) -> &str;
}

// ── Built-in comparators ──────────────────────────────────────────────────────

/// Strict equality (default).
#[derive(Debug, Default)]
pub struct StrictComparator;

impl ValueComparator for StrictComparator {
    fn equals(&self, left: &Value, right: &Value) -> bool {
        left == right
    }
    fn name(&self) -> &str {
        "strict"
    }
}

/// Case-insensitive string comparison; other types fall back to strict.
#[derive(Debug, Default)]
pub struct CaseInsensitiveComparator;

impl ValueComparator for CaseInsensitiveComparator {
    fn equals(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Text(a), Value::Text(b)) => a.to_lowercase() == b.to_lowercase(),
            _ => left == right,
        }
    }
    fn name(&self) -> &str {
        "case_insensitive"
    }
}

/// Loose numeric comparison: integer and float are equal when numerically
/// identical, and floats within `epsilon` of each other are equal.
#[derive(Debug)]
pub struct NumericToleranceComparator {
    pub epsilon: f64,
}

impl Default for NumericToleranceComparator {
    fn default() -> Self {
        Self { epsilon: 1e-9 }
    }
}

impl ValueComparator for NumericToleranceComparator {
    fn equals(&self, left: &Value, right: &Value) -> bool {
        if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
            (a - b).abs() <= self.epsilon
        } else {
            left == right
        }
    }
    fn name(&self) -> &str {
        "numeric_tolerance"
    }
}

/// Whitespace-trimming comparator: strips leading/trailing whitespace from
/// strings before comparing.
#[derive(Debug, Default)]
pub struct TrimComparator;

impl ValueComparator for TrimComparator {
    fn equals(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Text(a), Value::Text(b)) => a.trim() == b.trim(),
            _ => left == right,
        }
    }
    fn name(&self) -> &str {
        "trim"
    }
}

/// Chains two comparators: both must agree that values are equal.
pub struct AndComparator<A: ValueComparator, B: ValueComparator> {
    pub a: A,
    pub b: B,
}

impl<A: ValueComparator, B: ValueComparator> ValueComparator for AndComparator<A, B> {
    fn equals(&self, left: &Value, right: &Value) -> bool {
        self.a.equals(left, right) && self.b.equals(left, right)
    }
    fn name(&self) -> &str {
        "and"
    }
}

/// Chains two comparators: either may say values are equal.
pub struct OrComparator<A: ValueComparator, B: ValueComparator> {
    pub a: A,
    pub b: B,
}

impl<A: ValueComparator, B: ValueComparator> ValueComparator for OrComparator<A, B> {
    fn equals(&self, left: &Value, right: &Value) -> bool {
        self.a.equals(left, right) || self.b.equals(left, right)
    }
    fn name(&self) -> &str {
        "or"
    }
}
