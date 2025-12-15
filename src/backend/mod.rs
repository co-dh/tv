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

use crate::state::ViewState;
use anyhow::{anyhow, Result};
use ::polars::prelude::*;
use std::path::Path;
use std::sync::mpsc::Receiver;

/// Result of loading a file: ViewState + optional background loader
pub struct LoadResult {
    pub view: ViewState,
    pub bg_loader: Option<Receiver<gz::GzChunk>>,
}

// ── Common DataFrame ops (used by Memory & Gz) ──────────────────────────────

/// Filter DataFrame using SQL WHERE clause, limit to avoid OOM
pub fn df_filter(df: &DataFrame, w: &str, limit: usize) -> Result<DataFrame> {
    let mut ctx = ::polars::sql::SQLContext::new();
    ctx.register("df", df.clone().lazy());
    ctx.execute(&format!("SELECT * FROM df WHERE {} LIMIT {}", w, limit))?
        .collect().map_err(|e| anyhow!("{}", e))
}

/// Execute SQL on LazyFrame (common helper for all SQL ops)
pub fn sql(lf: LazyFrame, query: &str) -> Result<DataFrame> {
    let mut ctx = ::polars::sql::SQLContext::new();
    ctx.register("df", lf);
    ctx.execute(query)?.collect().map_err(|e| anyhow!("{}", e))
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

    /// Fetch rows for viewport (offset, limit), with optional WHERE clause
    fn fetch_rows(&self, path: &str, offset: usize, limit: usize) -> Result<DataFrame>;

    /// Fetch rows with WHERE clause for filtered views
    fn fetch_where(&self, path: &str, where_clause: &str, offset: usize, limit: usize) -> Result<DataFrame>;

    /// Count rows matching WHERE clause (for filtered view total count)
    fn count_where(&self, path: &str, where_clause: &str) -> Result<usize>;

    /// Get distinct values for a column
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>>;

    /// Save dataframe (parquet or csv based on extension)
    fn save(&self, df: &DataFrame, path: &Path) -> Result<()> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("csv") => {
                polars::save_csv(df, path)
            }
            _ => df_save(df, path),
        }
    }

    /// Compute frequency counts for a column, sorted descending.
    /// Returns DataFrame with [col, Cnt] columns.
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame>;

    /// Compute frequency with WHERE clause for filtered views
    fn freq_where(&self, path: &str, col: &str, where_clause: &str) -> Result<DataFrame>;

    /// Filter rows using SQL WHERE clause syntax, limit results.
    /// Returns DataFrame with up to `limit` matching rows.
    fn filter(&self, path: &str, where_clause: &str, limit: usize) -> Result<DataFrame>;

    /// Sort by column and return top N rows (efficient for TUI viewport).
    fn sort_head(&self, path: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame>;

    /// Load file into ViewState (default: not supported)
    fn load(&self, _path: &str, _id: usize) -> Result<LoadResult> {
        Err(anyhow!("Load not supported by this backend"))
    }
}
