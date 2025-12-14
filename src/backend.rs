//! Backend trait for data operations (polars, duckdb api, duckdb cli)
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Backend type selection
#[derive(Clone, Copy, Default, PartialEq)]
pub enum BackendType { #[default] Polars, DuckApi, DuckCli }

/// Backend interface for parquet operations
pub trait Backend: Send + Sync {
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame>;
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame>;
}

// === Polars Backend (streaming engine) ===
pub struct Polars;

impl Backend for Polars {
    fn freq(&self, path: &str, name: &str) -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        LazyFrame::scan_parquet(PlPath::new(path), args)?
            .group_by([col(name)])
            .agg([len().alias("Cnt")])
            .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
            .collect_with_engine(Engine::Streaming)
            .map_err(|e| anyhow!("{}", e))
    }

    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", LazyFrame::scan_parquet(PlPath::new(path), args)?);
        ctx.execute(&format!("SELECT * FROM df WHERE {}", where_clause))?
            .collect()
            .map_err(|e| anyhow!("{}", e))
    }
}

// === DuckDB API Backend (native crate) ===
pub struct DuckApi;

impl DuckApi {
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

        DataFrame::new(series).map_err(|e| anyhow!("{}", e))
    }
}

impl Backend for DuckApi {
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame> {
        self.query(&format!(
            "SELECT \"{col}\", COUNT(*)::INTEGER as Cnt FROM '{path}' GROUP BY \"{col}\" ORDER BY Cnt DESC"
        ))
    }

    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        self.query(&format!("SELECT * FROM '{}' WHERE {}", path, where_clause))
    }
}

// === DuckDB CLI Backend (shell out to duckdb) ===
pub struct DuckCli;

impl DuckCli {
    /// Run duckdb CLI and parse CSV output to DataFrame
    fn query(&self, sql: &str) -> Result<DataFrame> {
        use std::process::Command;
        let out = Command::new("duckdb")
            .args(["-csv", "-c", sql])
            .output()
            .map_err(|e| anyhow!("duckdb cli: {}", e))?;
        if !out.status.success() {
            return Err(anyhow!("duckdb: {}", String::from_utf8_lossy(&out.stderr)));
        }
        // Parse CSV output
        let csv = String::from_utf8_lossy(&out.stdout);
        let cursor = std::io::Cursor::new(csv.as_bytes());
        CsvReader::new(cursor)
            .finish()
            .map_err(|e| anyhow!("csv parse: {}", e))
    }
}

impl Backend for DuckCli {
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame> {
        self.query(&format!(
            "SELECT \"{col}\", COUNT(*)::INTEGER as Cnt FROM '{path}' GROUP BY \"{col}\" ORDER BY Cnt DESC"
        ))
    }

    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        self.query(&format!("SELECT * FROM '{}' WHERE {}", path, where_clause))
    }
}

/// Get backend based on type
pub fn get(t: BackendType) -> Box<dyn Backend> {
    match t {
        BackendType::Polars => Box::new(Polars),
        BackendType::DuckApi => Box::new(DuckApi),
        BackendType::DuckCli => Box::new(DuckCli),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckapi_backend() {
        let b = DuckApi;
        let df = b.query("SELECT 1 as a, 2 as b").unwrap();
        assert_eq!(df.height(), 1);
    }

    #[test]
    fn test_duckcli_backend() {
        // Only run if duckdb is installed
        if std::process::Command::new("duckdb").arg("--version").output().is_ok() {
            let b = DuckCli;
            let df = b.query("SELECT 1 as a, 2 as b").unwrap();
            assert_eq!(df.height(), 1);
        }
    }
}
