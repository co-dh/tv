//! Backend trait for data operations on parquet files and in-memory DataFrames.
//!
//! # Backends
//! - `Polars`: Native polars streaming engine for parquet files
//! - `Memory`: In-memory DataFrame operations (for ls, ps, csv, etc.)

mod polars;
mod memory;

pub use polars::Polars;
pub use memory::Memory;

use anyhow::Result;
use ::polars::prelude::DataFrame;

/// Backend interface for data operations.
/// All methods take a path (ignored by Memory backend).
pub trait Backend: Send + Sync {
    /// Get column names from data source.
    fn cols(&self, path: &str) -> Result<Vec<String>>;

    /// Get schema as (column_name, type_string) pairs.
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>>;

    /// Compute frequency counts for a column, sorted descending.
    /// Returns DataFrame with [col, Cnt] columns.
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame>;

    /// Filter rows using SQL WHERE clause syntax.
    /// Returns DataFrame with matching rows.
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame>;
}
