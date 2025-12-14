//! Backend trait for data operations (polars vs duckdb)
use anyhow::Result;
use polars::prelude::*;

/// Backend interface for parquet operations
pub trait Backend: Send + Sync {
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame>;
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame>;
}

// === Polars Backend ===
pub struct Polars;

impl Backend for Polars {
    fn freq(&self, path: &str, name: &str) -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        LazyFrame::scan_parquet(PlPath::new(path), args)?
            .group_by([col(name)])
            .agg([len().alias("Cnt")])
            .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
            .collect_with_engine(Engine::Streaming)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", LazyFrame::scan_parquet(PlPath::new(path), args)?);
        ctx.execute(&format!("SELECT * FROM df WHERE {}", where_clause))?
            .collect()
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

// === DuckDB Backend ===
pub struct DuckDb;

impl DuckDb {
    fn query(&self, sql: &str) -> Result<DataFrame> {
        use duckdb::Connection;
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query([])?;

        // Get column names
        let names: Vec<String> = rows.as_ref()
            .map(|r| r.column_names().iter().map(|s| s.to_string()).collect())
            .unwrap_or_default();
        let ncols = names.len();

        // Collect values
        let mut cols: Vec<Vec<String>> = vec![vec![]; ncols];
        while let Some(row) = rows.next()? {
            for i in 0..ncols {
                let val: String = row.get::<_, Option<String>>(i)
                    .unwrap_or(None)
                    .unwrap_or_else(|| "null".to_string());
                cols[i].push(val);
            }
        }

        // Build DataFrame, infer types
        let series: Vec<Column> = names.iter().zip(cols.iter())
            .map(|(name, vals)| {
                let s = Series::new(name.into(), vals);
                if let Ok(i) = s.cast(&DataType::Int64) {
                    if !i.is_null().any() { return i.into(); }
                }
                if let Ok(f) = s.cast(&DataType::Float64) {
                    if !f.is_null().any() { return f.into(); }
                }
                s.into()
            })
            .collect();

        DataFrame::new(series).map_err(|e| anyhow::anyhow!("{}", e))
    }
}

impl Backend for DuckDb {
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame> {
        self.query(&format!(
            "SELECT \"{col}\", COUNT(*)::INTEGER as Cnt FROM '{path}' GROUP BY \"{col}\" ORDER BY Cnt DESC"
        ))
    }

    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        self.query(&format!("SELECT * FROM '{}' WHERE {}", path, where_clause))
    }
}

/// Get backend based on flag
pub fn get(use_duckdb: bool) -> Box<dyn Backend> {
    if use_duckdb { Box::new(DuckDb) } else { Box::new(Polars) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_backend() {
        let b = DuckDb;
        let df = b.query("SELECT 1 as a, 2 as b").unwrap();
        assert_eq!(df.height(), 1);
    }
}
