//! Backend trait for data operations
//! Each backend in its own file: polars, duckapi, duckcli, memory

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

/// Backend type selection (for parquet files)
#[derive(Clone, Copy, Default, PartialEq)]
pub enum BackendType { #[default] Polars, DuckApi, DuckCli }

/// Backend interface for data operations
pub trait Backend: Send + Sync {
    /// Get column names from parquet file
    fn cols(&self, path: &str) -> Result<Vec<String>>;
    /// Get schema (column name, type) from parquet file
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>>;
    /// Frequency count from parquet file
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame>;
    /// Filter parquet file by SQL WHERE clause
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame>;
    /// Frequency count from in-memory DataFrame
    fn freq_df(&self, df: &DataFrame, col: &str, keys: &[String]) -> Result<DataFrame>;
}

/// Get backend instance by type
pub fn get(t: BackendType) -> Box<dyn Backend> {
    match t {
        BackendType::Polars => Box::new(Polars),
        BackendType::DuckApi => Box::new(DuckApi),
        BackendType::DuckCli => Box::new(DuckCli),
    }
}
