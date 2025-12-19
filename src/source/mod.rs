//! Source trait for data operations.
//!
//! # Sources
//! - `Polars`: Streaming engine for parquet files
//! - `Memory`: In-memory DataFrame (ls, ps, csv)
//! - `Gz`: Streaming gzipped CSV with memory limits

pub mod polars;
mod memory;
pub mod gz;

pub use polars::Polars;
pub use memory::Memory;

use crate::state::ViewState;
use crate::utils::is_numeric;
use anyhow::{anyhow, Result};
use ::polars::prelude::*;
use std::path::Path;
use std::sync::mpsc::Receiver;

/// Create Column from name and data (shorthand for Series::new().into())
#[macro_export]
macro_rules! ser {
    ($name:expr, $data:expr) => { Series::new($name.into(), $data).into() };
}

/// Get column names as Vec<String> from DataFrame
#[must_use]
pub fn df_cols(df: &DataFrame) -> Vec<String> {
    df.get_column_names().iter().map(|s| s.to_string()).collect()
}

/// Result of loading a file: ViewState + optional background loader
pub struct LoadResult {
    pub view: ViewState,
    pub bg_loader: Option<Receiver<gz::GzChunk>>,
}

// ── SQL/PRQL execution ──────────────────────────────────────────────────────

/// Execute SQL on LazyFrame with streaming engine
pub fn sql(lf: LazyFrame, query: &str) -> Result<DataFrame> {
    sql_lazy(lf, query)?.collect_with_engine(Engine::Streaming).map_err(|e| anyhow!("{}", e))
}

/// Execute SQL returning LazyFrame (for chained operations)
pub fn sql_lazy(lf: LazyFrame, query: &str) -> Result<LazyFrame> {
    let mut ctx = ::polars::sql::SQLContext::new();
    ctx.register("df", lf);
    ctx.execute(query).map_err(|e| anyhow!("{}", e))
}

/// Compile PRQL to SQL
pub fn prql_to_sql(prql: &str) -> Result<String> {
    prqlc::compile(prql, &prqlc::Options::default().no_signature())
        .map_err(|e| anyhow!("PRQL: {}", e.inner[0].reason))
}

/// Execute PRQL on LazyFrame (compiles to SQL, then executes)
pub fn prql(lf: LazyFrame, query: &str) -> Result<DataFrame> {
    sql(lf, &prql_to_sql(query)?)
}

/// Save DataFrame to parquet
pub fn df_save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df).map_err(|e| anyhow!("Parquet write: {}", e))?;
    Ok(())
}

/// Source interface for data operations.
/// All methods take a path (ignored by Memory/Gz sources).
/// Only lf() is required - all other ops use SQL defaults.
pub trait Source: Send + Sync {
    /// Get LazyFrame for SQL operations (path ignored by Memory/Gz)
    fn lf(&self, path: &str) -> Result<LazyFrame>;

    /// Get schema Arc (shared by cols/schema)
    fn get_schema(&self, path: &str) -> Result<std::sync::Arc<Schema>> {
        self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))
    }

    /// Get metadata: (row_count, column_names) - via SQL COUNT (prqlc count has bugs)
    fn metadata(&self, path: &str) -> Result<(usize, Vec<String>)> {
        let r = sql(self.lf(path)?, "SELECT COUNT(*) as cnt FROM df")?;
        Ok((r.column("cnt")?.get(0)?.try_extract::<u64>().unwrap_or(0) as usize, self.cols(path)?))
    }

    /// Get column names via get_schema()
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        Ok(self.get_schema(path)?.iter_names().map(|s| s.to_string()).collect())
    }

    /// Get schema as (name, type) pairs via get_schema()
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        Ok(self.get_schema(path)?.iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
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

    /// Frequency with WHERE clause, supports multiple GROUP BY columns
    fn freq_where(&self, path: &str, grp_cols: &[String], w: &str) -> Result<DataFrame> {
        let grp_sel = grp_cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");
        let grp_by = grp_sel.clone();
        sql(self.lf(path)?, &format!("SELECT {}, COUNT(*) as Cnt FROM df WHERE {} GROUP BY {} ORDER BY Cnt DESC", grp_sel, w, grp_by))
    }

    /// Frequency with aggregates (min/max/sum) for selected columns, supports multiple GROUP BY
    fn freq_agg(&self, path: &str, grp_cols: &[String], w: &str, sel_cols: &[String]) -> Result<DataFrame> {
        let base = if w != "TRUE" { format!("WHERE {}", w) } else { String::new() };
        let grp_sel = grp_cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");
        let grp_by = grp_sel.clone();
        let cnt_q = format!("SELECT {}, COUNT(*) as Cnt FROM df {} GROUP BY {} ORDER BY Cnt DESC", grp_sel, base, grp_by);
        let mut result = sql(self.lf(path)?, &cnt_q)?;

        // Only aggregate selected columns (filter to numeric)
        let schema = self.lf(path)?.collect_schema()?;
        let num_cols: Vec<&String> = sel_cols.iter()
            .filter(|c| schema.get(c.as_str()).map(|dt| is_numeric(dt)).unwrap_or(false))
            .collect();

        // Join columns for multi-column GROUP BY
        let join_cols: Vec<Expr> = grp_cols.iter().map(|c| col(c)).collect();

        // Process each column separately to save memory
        for c in num_cols {
            let agg_q = format!(
                "SELECT {}, MIN(\"{}\") as {}_min, MAX(\"{}\") as {}_max, SUM(CAST(\"{}\" AS DOUBLE)) as {}_sum FROM df {} GROUP BY {}",
                grp_sel, c, c, c, c, c, c, base, grp_by
            );
            if let Ok(agg_df) = sql(self.lf(path)?, &agg_q) {
                result = result.lazy()
                    .join(agg_df.lazy(), join_cols.clone(), join_cols.clone(), JoinArgs::new(JoinType::Left))
                    .collect()?;
            }
        }
        Ok(result)
    }

    /// Filter rows
    fn filter(&self, path: &str, w: &str, limit: usize) -> Result<DataFrame> {
        self.fetch_where(path, w, 0, limit)
    }

    /// Sort and take top N (PRQL: sort + take)
    fn sort_head(&self, path: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        let sort = if desc { format!("-`{}`", col) } else { format!("`{}`", col) };
        prql(self.lf(path)?, &format!("from df | sort {{{}}} | take {}", sort, limit))
    }

    /// Load file into ViewState (default: not supported)
    fn load(&self, _path: &str, _id: usize) -> Result<LoadResult> {
        Err(anyhow!("Load not supported by this source"))
    }
}

/// Test helper: create DataFrame from columns (use in tests only)
#[cfg(test)]
#[macro_export]
macro_rules! test_df {
    ($($name:literal => $data:expr),+ $(,)?) => {
        DataFrame::new(vec![$(Column::new($name.into(), $data),)+]).unwrap()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prql_to_sql() {
        let sql = prql_to_sql("from df | take 10").unwrap();
        assert!(sql.contains("LIMIT") && sql.contains("10"), "Expected LIMIT 10: {}", sql);
    }

    #[test]
    fn test_prql_filter() {
        let sql = prql_to_sql("from df | filter x > 5").unwrap();
        assert!(sql.contains("WHERE"), "Expected WHERE: {}", sql);
    }

    #[test]
    fn test_prql_select() {
        let sql = prql_to_sql("from df | select {a, b}").unwrap();
        assert!(sql.contains("SELECT"), "Expected SELECT: {}", sql);
    }

    // Note: prqlc 0.13.10 has a bug where `count this` and `count *` panic
    // with "called `Option::unwrap()` on a `None` value" in names.rs:211.
    // We use SQL for count queries until this is fixed.

    #[test]
    fn test_prql_sort_take() {
        // PRQL uses braces for sort columns, - for descending
        let sql = prql_to_sql("from df | sort {-x} | take 10").unwrap();
        assert!(sql.contains("ORDER BY") && sql.contains("DESC"), "Expected ORDER BY DESC: {}", sql);
    }
}
