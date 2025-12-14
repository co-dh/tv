//! Polars backend - streaming engine for parquet files
use super::Backend;
use anyhow::{anyhow, Result};
use polars::prelude::*;

pub struct Polars;

impl Backend for Polars {
    /// Get column names from parquet file
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        Ok(self.schema(path)?.into_iter().map(|(n, _)| n).collect())
    }

    /// Get schema (column name, type) from parquet file
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let file = std::fs::File::open(path)?;
        let schema = ParquetReader::new(file).schema()?;
        Ok(schema.iter().map(|(n, f)| (n.to_string(), format!("{:?}", f.dtype()))).collect())
    }

    /// Freq from parquet using streaming engine
    fn freq(&self, path: &str, name: &str) -> Result<DataFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?
            .group_by([col(name)])
            .agg([len().alias("Cnt")])
            .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
            .collect_with_engine(Engine::Streaming)
            .map_err(|e| anyhow!("{}", e))
    }

    /// Filter parquet using SQL WHERE clause
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?);
        ctx.execute(&format!("SELECT * FROM df WHERE {}", where_clause))?
            .collect()
            .map_err(|e| anyhow!("{}", e))
    }

    /// Freq from in-memory DataFrame (delegates to memory backend logic)
    fn freq_df(&self, df: &DataFrame, col: &str, keys: &[String]) -> Result<DataFrame> {
        super::Memory.freq_df(df, col, keys)
    }
}
