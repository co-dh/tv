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
        // Use query_arrow for efficient Arrow-based transfer
        let rbs: Vec<duckdb::arrow::record_batch::RecordBatch> = stmt.query_arrow([])?.collect();
        if rbs.is_empty() { return Ok(DataFrame::empty()); }
        // Convert Arrow RecordBatches to polars DataFrame
        let mut dfs = Vec::new();
        for rb in rbs {
            let schema = rb.schema();
            let mut cols: Vec<Column> = Vec::new();
            for (i, field) in schema.fields().iter().enumerate() {
                let arr = rb.column(i);
                let s = arrow_to_series(field.name(), arr)?;
                cols.push(s.into());
            }
            dfs.push(DataFrame::new(cols)?);
        }
        // Vertical concat
        let mut result = dfs.remove(0);
        for df in dfs { result.vstack_mut(&df)?; }
        Ok(result)
    }
}

/// Convert Arrow array to polars Series
fn arrow_to_series(name: &str, arr: &duckdb::arrow::array::ArrayRef) -> Result<Series> {
    use duckdb::arrow::array::*;
    use duckdb::arrow::datatypes::DataType as ArrowDT;
    let name = PlSmallStr::from(name);
    match arr.data_type() {
        ArrowDT::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Series::new(name, a.values().as_ref()))
        }
        ArrowDT::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(Series::new(name, a.values().as_ref()))
        }
        ArrowDT::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(Series::new(name, a.values().as_ref()))
        }
        ArrowDT::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            let vals: Vec<&str> = (0..a.len()).map(|i| a.value(i)).collect();
            Ok(Series::new(name, vals))
        }
        ArrowDT::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let vals: Vec<&str> = (0..a.len()).map(|i| a.value(i)).collect();
            Ok(Series::new(name, vals))
        }
        _ => {
            // Fallback: convert to string
            let vals: Vec<String> = (0..arr.len()).map(|_| format!("{:?}", arr.as_ref())).collect();
            Ok(Series::new(name, vals))
        }
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
