//! Backend trait for data operations on parquet files and in-memory DataFrames.
//!
//! # Backends
//! - `Polars`: Native polars streaming engine (default, fastest for most ops)
//! - `DuckApi`: DuckDB via native Rust crate with Arrow transfer
//! - `DuckCli`: DuckDB via CLI subprocess (fallback if crate issues)
//! - `Memory`: In-memory DataFrame operations (for ls, ps, csv, etc.)
//!
//! # Usage
//! Views own their backend via `ViewState::backend()`. File-based views use
//! Polars/DuckApi/DuckCli, while in-memory views use Memory backend.

mod polars;
mod duckapi;
mod duckcli;
mod memory;

pub use polars::Polars;
pub use duckapi::DuckApi;
pub use duckcli::DuckCli;
pub use memory::Memory;

use anyhow::Result;
use ::polars::prelude::DataFrame;

/// Backend type for file-based operations (parquet files).
/// Memory backend is created directly, not via this enum.
#[derive(Clone, Copy, Default, PartialEq)]
pub enum BackendType { #[default] Polars, DuckApi, DuckCli }

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

/// Create backend instance by type (for file-based backends).
pub fn get(t: BackendType) -> Box<dyn Backend> {
    match t {
        BackendType::Polars => Box::new(Polars),
        BackendType::DuckApi => Box::new(DuckApi),
        BackendType::DuckCli => Box::new(DuckCli),
    }
}
