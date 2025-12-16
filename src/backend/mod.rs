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

// ── SQL execution ───────────────────────────────────────────────────────────

/// Execute SQL on LazyFrame with streaming engine
pub fn sql(lf: LazyFrame, query: &str) -> Result<DataFrame> {
    let mut ctx = ::polars::sql::SQLContext::new();
    ctx.register("df", lf);
    ctx.execute(query)?.collect_with_engine(Engine::Streaming).map_err(|e| anyhow!("{}", e))
}

/// Save DataFrame to parquet
pub fn df_save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df).map_err(|e| anyhow!("Parquet write: {}", e))?;
    Ok(())
}

/// Backend interface for data operations.
/// All methods take a path (ignored by Memory/Gz backends).
/// Only lf() is required - all other ops use SQL defaults.
pub trait Backend: Send + Sync {
    /// Get LazyFrame for SQL operations (path ignored by Memory/Gz)
    fn lf(&self, path: &str) -> Result<LazyFrame>;

    /// Get metadata: (row_count, column_names) - default via SQL COUNT(*)
    fn metadata(&self, path: &str) -> Result<(usize, Vec<String>)> {
        let r = sql(self.lf(path)?, "SELECT COUNT(*) as cnt FROM df")?;
        let cnt = r.column("cnt")?.get(0)?.try_extract::<u64>().unwrap_or(0) as usize;
        Ok((cnt, self.cols(path)?))
    }

    /// Get column names - default via lf().collect_schema()
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter_names().map(|s| s.to_string()).collect())
    }

    /// Get schema as (name, type) pairs - default via lf().collect_schema()
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

    /// Fetch rows - default uses fetch_sel
    fn fetch_rows(&self, path: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        self.fetch_sel(path, &[], "TRUE", offset, limit)
    }

    /// Fetch rows with WHERE clause (all columns)
    fn fetch_where(&self, path: &str, w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        self.fetch_sel(path, &[], w, offset, limit)
    }

    /// Fetch selected columns with WHERE clause
    fn fetch_sel(&self, path: &str, cols: &[String], w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        let sel = if cols.is_empty() { "*".into() } else { cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(",") };
        sql(self.lf(path)?, &format!("SELECT {} FROM df WHERE {} LIMIT {} OFFSET {}", sel, w, limit, offset))
    }

    /// Count rows matching WHERE clause
    fn count_where(&self, path: &str, w: &str) -> Result<usize> {
        let r = sql(self.lf(path)?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

    /// Get distinct values for a column
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>> {
        let df = sql(self.lf(path)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }

    /// Save dataframe
    fn save(&self, df: &DataFrame, path: &Path) -> Result<()> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("csv") => polars::save_csv(df, path),
            _ => df_save(df, path),
        }
    }

    /// Frequency counts - default uses freq_where
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame> {
        self.freq_where(path, col, "TRUE")
    }

    /// Frequency with WHERE clause
    fn freq_where(&self, path: &str, col: &str, w: &str) -> Result<DataFrame> {
        sql(self.lf(path)?, &format!("SELECT \"{}\", COUNT(*) as Cnt FROM df WHERE {} GROUP BY \"{}\" ORDER BY Cnt DESC", col, w, col))
    }

    /// Filter rows
    fn filter(&self, path: &str, w: &str, limit: usize) -> Result<DataFrame> {
        self.fetch_where(path, w, 0, limit)
    }

    /// Sort and take top N
    fn sort_head(&self, path: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        let ord = if desc { "DESC" } else { "ASC" };
        sql(self.lf(path)?, &format!("SELECT * FROM df ORDER BY \"{}\" {} LIMIT {}", col, ord, limit))
    }

    /// Load file into ViewState (default: not supported)
    fn load(&self, _path: &str, _id: usize) -> Result<LoadResult> {
        Err(anyhow!("Load not supported by this backend"))
    }
}
