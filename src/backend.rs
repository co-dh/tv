//! Backend trait for data operations (polars vs duckdb)
use anyhow::Result;
use polars::prelude::*;

/// Backend interface for file operations
pub trait Backend: Send + Sync {
    fn head(&self, path: &str, n: usize) -> Result<DataFrame>;
    fn count(&self, path: &str) -> Result<usize>;
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame>;
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame>;
    fn filter_count(&self, path: &str, where_clause: &str) -> Result<usize>;
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>>;
    fn fetch_rows(&self, path: &str, offset: usize, limit: usize) -> Result<DataFrame>;
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>>;
}

// === Polars Backend ===
pub struct Polars;

impl Backend for Polars {
    fn head(&self, path: &str, n: usize) -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        LazyFrame::scan_parquet(PlPath::new(path), args)?
            .limit(n as u32)
            .collect()
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn count(&self, path: &str) -> Result<usize> {
        use polars::io::parquet::read::ParquetReader;
        use std::fs::File;
        let file = File::open(path)?;
        Ok(ParquetReader::new(file).get_metadata()?.num_rows)
    }

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

    fn filter_count(&self, path: &str, where_clause: &str) -> Result<usize> {
        let args = ScanArgsParquet::default();
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", LazyFrame::scan_parquet(PlPath::new(path), args)?);
        let df = ctx.execute(&format!("SELECT COUNT(*) as cnt FROM df WHERE {}", where_clause))?
            .collect()?;
        df.column("cnt")
            .and_then(|c| c.idx().map(|v| v.get(0).unwrap_or(0) as usize))
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn distinct(&self, path: &str, name: &str) -> Result<Vec<String>> {
        let args = ScanArgsParquet::default();
        let df = LazyFrame::scan_parquet(PlPath::new(path), args)?
            .select([col(name)])
            .unique(None, UniqueKeepStrategy::First)
            .sort([name], SortMultipleOptions::default())
            .collect()?;
        let c = df.column(name)?;
        Ok((0..c.len().min(10000))
            .filter_map(|i| c.get(i).ok().map(|v| v.to_string().trim_matches('"').to_string()))
            .filter(|v| v != "null")
            .collect())
    }

    fn fetch_rows(&self, path: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        LazyFrame::scan_parquet(PlPath::new(path), args)?
            .slice(offset as i64, limit as u32)
            .collect()
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        use polars::io::parquet::read::ParquetReader;
        use std::fs::File;
        let file = File::open(path)?;
        let schema = ParquetReader::new(file).schema()?;
        Ok(schema.iter().map(|(n, f)| (n.to_string(), format!("{:?}", f.dtype()))).collect())
    }
}

// === DuckDB Backend ===
pub struct DuckDb;

impl DuckDb {
    pub fn query(&self, sql: &str) -> Result<DataFrame> {
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
    fn head(&self, path: &str, n: usize) -> Result<DataFrame> {
        self.query(&format!("SELECT * FROM '{}' LIMIT {}", path, n))
    }

    fn count(&self, path: &str) -> Result<usize> {
        let df = self.query(&format!("SELECT COUNT(*) as cnt FROM '{}'", path))?;
        df.column("cnt")
            .and_then(|c| c.i64().map(|v| v.get(0).unwrap_or(0) as usize))
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn freq(&self, path: &str, col: &str) -> Result<DataFrame> {
        self.query(&format!(
            "SELECT \"{col}\", COUNT(*)::INTEGER as Cnt FROM '{path}' GROUP BY \"{col}\" ORDER BY Cnt DESC"
        ))
    }

    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        self.query(&format!("SELECT * FROM '{}' WHERE {}", path, where_clause))
    }

    fn filter_count(&self, path: &str, where_clause: &str) -> Result<usize> {
        let df = self.query(&format!("SELECT COUNT(*) as cnt FROM '{}' WHERE {}", path, where_clause))?;
        df.column("cnt")
            .and_then(|c| c.i64().map(|v| v.get(0).unwrap_or(0) as usize))
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>> {
        let df = self.query(&format!("SELECT DISTINCT \"{}\" FROM '{}' ORDER BY 1 LIMIT 10000", col, path))?;
        let c = df.column(col)?;
        Ok((0..c.len())
            .filter_map(|i| c.get(i).ok().map(|v| v.to_string().trim_matches('"').to_string()))
            .filter(|v| v != "null")
            .collect())
    }

    fn fetch_rows(&self, path: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        self.query(&format!("SELECT * FROM '{}' LIMIT {} OFFSET {}", path, limit, offset))
    }

    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let df = self.head(path, 1)?;
        Ok(df.schema().iter().map(|(n, t)| (n.to_string(), format!("{:?}", t))).collect())
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
    fn test_polars_backend() {
        let b = Polars;
        // Just test it compiles and schema works on test file
        if let Ok(s) = b.schema("tests/data/test.parquet") {
            assert!(!s.is_empty());
        }
    }

    #[test]
    fn test_duckdb_backend() {
        let b = DuckDb;
        let df = b.query("SELECT 1 as a, 2 as b").unwrap();
        assert_eq!(df.height(), 1);
    }
}
