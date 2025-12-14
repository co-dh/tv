//! Backend trait for data operations.
//!
//! # Backends
//! - `Polars`: Streaming engine for parquet files
//! - `Memory`: In-memory DataFrame (ls, ps, csv)
//! - `Gz`: Streaming gzipped CSV with memory limits

pub mod polars;
mod memory;
pub mod gz;

pub use polars::Polars;
pub use memory::Memory;
pub use gz::Gz;

use anyhow::Result;
use ::polars::prelude::DataFrame;
use std::path::Path;

/// Backend interface for data operations.
/// All methods take a path (ignored by Memory backend).
pub trait Backend: Send + Sync {
    /// Get column names from data source.
    fn cols(&self, path: &str) -> Result<Vec<String>>;

    /// Get schema as (column_name, type_string) pairs.
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>>;

    /// Get metadata: (row_count, column_names)
    fn metadata(&self, path: &str) -> Result<(usize, Vec<String>)>;

    /// Fetch rows for viewport (offset, limit)
    fn fetch_rows(&self, path: &str, offset: usize, limit: usize) -> Result<DataFrame>;

    /// Get distinct values for a column
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>>;

    /// Save dataframe to parquet
    fn save(&self, df: &DataFrame, path: &Path) -> Result<()>;

    /// Compute frequency counts for a column, sorted descending.
    /// Returns DataFrame with [col, Cnt] columns.
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame>;

    /// Filter rows using SQL WHERE clause syntax.
    /// Returns DataFrame with matching rows.
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame>;
}
