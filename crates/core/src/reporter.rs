//! The [`Reporter`] trait lets you plug in any output backend.
//!
//! Built-in implementations live in `dc_cli`; external crates can provide
//! their own (Slack, S3, email, etc.) without touching core.

use crate::compare::ComparisonReport;

/// Receive a completed comparison report and render / emit it somewhere.
pub trait Reporter: Send + Sync {
    /// Process the report. May write to stdout, a file, a remote service, etc.
    fn report(&self, report: &ComparisonReport) -> anyhow::Result<()>;

    /// Human-readable name used in diagnostics.
    fn name(&self) -> &str;
}
