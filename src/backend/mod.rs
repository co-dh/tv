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

use anyhow::{anyhow, Result};
use ::polars::prelude::*;
use std::path::Path;

// ── Common DataFrame ops (used by Memory & Gz) ──────────────────────────────

/// Filter DataFrame using SQL WHERE clause
pub fn df_filter(df: &DataFrame, w: &str) -> Result<DataFrame> {
    let mut ctx = ::polars::sql::SQLContext::new();
    ctx.register("df", df.clone().lazy());
    ctx.execute(&format!("SELECT * FROM df WHERE {}", w))?
        .collect().map_err(|e| anyhow!("{}", e))
}

/// Sort DataFrame and take top N rows
pub fn df_sort_head(df: &DataFrame, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
    df.clone().lazy()
        .sort([col], SortMultipleOptions::default().with_order_descending(desc))
        .limit(limit as u32).collect().map_err(|e| anyhow!("{}", e))
}

/// Get distinct values (exclude null)
pub fn df_distinct(df: &DataFrame, col: &str) -> Result<Vec<String>> {
    let c = df.column(col).map_err(|e| anyhow!("{}", e))?;
    let uniq = c.unique().map_err(|e| anyhow!("{}", e))?;
    Ok((0..uniq.len()).filter_map(|i| uniq.get(i).ok().map(|v| v.to_string())).filter(|v| v != "null").collect())
}

/// Save DataFrame to parquet
pub fn df_save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df).map_err(|e| anyhow!("Parquet write: {}", e))?;
    Ok(())
}

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

    /// Sort by column and return top N rows (efficient for TUI viewport).
    fn sort_head(&self, path: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame>;
}
