//! Polars backend - native streaming engine for parquet files.
//! Default backend. Uses LazyFrame with streaming engine for memory efficiency.
use super::Backend;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Polars streaming backend. Zero-copy parquet access via LazyFrame.
pub struct Polars;

impl Backend for Polars {
    /// Read column names from parquet file metadata (no data loaded).
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        Ok(self.schema(path)?.into_iter().map(|(n, _)| n).collect())
    }

    /// Read schema from parquet metadata. Returns polars dtype strings.
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let file = std::fs::File::open(path)?;
        let schema = ParquetReader::new(file).schema()?;
        Ok(schema.iter().map(|(n, f)| (n.to_string(), format!("{:?}", f.dtype()))).collect())
    }

    /// Frequency count using streaming engine. Memory-efficient for large files.
    fn freq(&self, path: &str, name: &str) -> Result<DataFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?
            .group_by([col(name)])
            .agg([len().alias("Cnt")])
            .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
            .collect_with_engine(Engine::Streaming)
            .map_err(|e| anyhow!("{}", e))
    }

    /// Filter using polars SQL context with lazy evaluation.
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?);
        ctx.execute(&format!("SELECT * FROM df WHERE {}", where_clause))?
            .collect()
            .map_err(|e| anyhow!("{}", e))
    }
}
