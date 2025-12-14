//! Connector abstraction for different data sources
//! Each connector has different capabilities (head, count, stats)
use anyhow::Result;
use polars::prelude::*;

/// Capabilities that a connector may support
#[derive(Debug, Clone, Default)]
pub struct Caps {
    pub head: bool,   // fetch first N rows
    pub count: bool,  // get total row count
    pub stats: bool,  // column statistics (min, max, distinct)
    pub filter: bool, // server-side filtering
    pub full: bool,   // full in-memory access
}

impl Caps {
    pub fn memory() -> Self { Self { head: true, count: true, stats: true, filter: true, full: true } }
    pub fn sql() -> Self { Self { head: true, count: true, stats: true, filter: true, full: false } }
    pub fn stream() -> Self { Self { head: true, count: false, stats: false, filter: false, full: false } }
}

/// Column statistics
#[derive(Debug, Clone)]
pub struct ColStats {
    pub dtype: String,
    pub count: usize,
    pub null_count: usize,
    pub distinct: Option<usize>,
    pub min: Option<String>,
    pub max: Option<String>,
}

/// Data source connector trait
pub trait Connector: Send {
    /// Get connector capabilities
    fn caps(&self) -> Caps;
    /// Connector name/description
    fn name(&self) -> &str;
    /// Get schema (column names and types)
    fn schema(&self) -> Result<Schema>;
    /// Fetch first N rows
    fn head(&self, n: usize) -> Result<DataFrame>;
    /// Get total row count (if supported)
    fn count(&self) -> Result<Option<usize>>;
    /// Get column statistics (if supported)
    fn col_stats(&self, col: &str) -> Result<Option<ColStats>>;
    /// Execute filter query (if supported)
    fn filter(&self, expr: &str) -> Result<Option<DataFrame>>;
}

/// In-memory DataFrame connector (full capabilities)
pub struct MemConn {
    name: String,
    df: DataFrame,
}

impl MemConn {
    pub fn new(name: String, df: DataFrame) -> Self { Self { name, df } }
    pub fn df(&self) -> &DataFrame { &self.df }
    pub fn df_mut(&mut self) -> &mut DataFrame { &mut self.df }
}

impl Connector for MemConn {
    fn caps(&self) -> Caps { Caps::memory() }
    fn name(&self) -> &str { &self.name }

    fn schema(&self) -> Result<Schema> {
        Ok(self.df.schema().as_ref().clone())
    }

    fn head(&self, n: usize) -> Result<DataFrame> {
        Ok(self.df.head(Some(n)))
    }

    fn count(&self) -> Result<Option<usize>> {
        Ok(Some(self.df.height()))
    }

    fn col_stats(&self, col: &str) -> Result<Option<ColStats>> {
        let s = self.df.column(col).map_err(|e| anyhow::anyhow!("{}", e))?;
        let dtype = format!("{:?}", s.dtype());
        let count = s.len();
        let null_count = s.null_count();
        // distinct count (expensive for large data)
        let distinct = s.n_unique().ok();
        // min/max as strings
        let min = s.min_reduce().ok().map(|v| format!("{:?}", v.value()));
        let max = s.max_reduce().ok().map(|v| format!("{:?}", v.value()));
        Ok(Some(ColStats { dtype, count, null_count, distinct, min, max }))
    }

    fn filter(&self, _expr: &str) -> Result<Option<DataFrame>> {
        // Use PRQL/polars filter - delegate to existing prql module
        Ok(None) // TODO: integrate with prql module
    }
}

/// DuckDB SQL connector (via CLI or server)
pub struct DuckConn {
    name: String,
    source: String, // file path or table name
    schema: Option<Schema>,
}

impl DuckConn {
    pub fn new(name: String, source: String) -> Self {
        Self { name, source, schema: None }
    }

    /// Execute DuckDB query and return DataFrame
    fn query(&self, sql: &str) -> Result<DataFrame> {
        use std::io::Cursor;
        use std::process::{Command, Stdio};
        let out = Command::new("duckdb")
            .args(["-csv", "-c", sql])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| anyhow::anyhow!("duckdb: {}", e))?;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            return Err(anyhow::anyhow!("duckdb: {}", err));
        }
        if out.stdout.is_empty() {
            return Err(anyhow::anyhow!("Empty result"));
        }
        CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(500))
            .into_reader_with_file_handle(Cursor::new(out.stdout))
            .finish()
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

impl Connector for DuckConn {
    fn caps(&self) -> Caps { Caps::sql() }
    fn name(&self) -> &str { &self.name }

    fn schema(&self) -> Result<Schema> {
        let df = self.head(1)?;
        Ok(df.schema().as_ref().clone())
    }

    fn head(&self, n: usize) -> Result<DataFrame> {
        let sql = format!("SELECT * FROM '{}' LIMIT {}", self.source, n);
        self.query(&sql)
    }

    fn count(&self) -> Result<Option<usize>> {
        let sql = format!("SELECT COUNT(*) as cnt FROM '{}'", self.source);
        let df = self.query(&sql)?;
        let cnt = df.column("cnt")
            .and_then(|s| s.i64().map(|c| c.get(0).unwrap_or(0) as usize))
            .unwrap_or(0);
        Ok(Some(cnt))
    }

    fn col_stats(&self, col: &str) -> Result<Option<ColStats>> {
        // Get basic stats via SQL (separate queries for type and aggregates)
        let type_sql = format!("SELECT typeof(\"{}\") as dtype FROM '{}' LIMIT 1", col, self.source);
        let dtype = self.query(&type_sql).ok()
            .and_then(|df| df.column("dtype").ok().and_then(|s| s.str().ok().and_then(|s| s.get(0)).map(|s| s.to_string())))
            .unwrap_or_default();

        let sql = format!(
            "SELECT COUNT(*) as cnt, \
             COUNT(*) - COUNT(\"{col}\") as nulls, \
             COUNT(DISTINCT \"{col}\") as distinct, \
             MIN(\"{col}\")::VARCHAR as min_val, \
             MAX(\"{col}\")::VARCHAR as max_val \
             FROM '{}'",
            self.source, col = col
        );
        let df = self.query(&sql)?;
        let get_str = |c: &str| df.column(c).ok()
            .and_then(|s| s.str().ok())
            .and_then(|s| s.get(0))
            .map(|s| s.to_string());
        let get_usize = |c: &str| df.column(c).ok()
            .and_then(|s| s.i64().ok())
            .and_then(|s| s.get(0))
            .map(|v| v as usize);

        Ok(Some(ColStats {
            dtype,
            count: get_usize("cnt").unwrap_or(0),
            null_count: get_usize("nulls").unwrap_or(0),
            distinct: get_usize("distinct"),
            min: get_str("min_val"),
            max: get_str("max_val"),
        }))
    }

    fn filter(&self, expr: &str) -> Result<Option<DataFrame>> {
        // SQL WHERE clause
        let sql = format!("SELECT * FROM '{}' WHERE {}", self.source, expr);
        self.query(&sql).map(Some)
    }
}

/// Streaming gz connector (head only)
pub struct GzConn {
    name: String,
    path: String,
    preview: DataFrame,
}

impl GzConn {
    pub fn new(name: String, path: String, preview: DataFrame) -> Self {
        Self { name, path, preview }
    }
}

impl Connector for GzConn {
    fn caps(&self) -> Caps { Caps::stream() }
    fn name(&self) -> &str { &self.name }

    fn schema(&self) -> Result<Schema> {
        Ok(self.preview.schema().as_ref().clone())
    }

    fn head(&self, n: usize) -> Result<DataFrame> {
        Ok(self.preview.head(Some(n)))
    }

    fn count(&self) -> Result<Option<usize>> {
        Ok(None) // Unknown for streaming
    }

    fn col_stats(&self, _col: &str) -> Result<Option<ColStats>> {
        Ok(None) // Not supported
    }

    fn filter(&self, _expr: &str) -> Result<Option<DataFrame>> {
        Ok(None) // Not supported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_connector() {
        let df = df!("a" => [1, 2, 3], "b" => ["x", "y", "z"]).unwrap();
        let conn = MemConn::new("test".into(), df);
        assert!(conn.caps().full);
        assert_eq!(conn.count().unwrap(), Some(3));
        let h = conn.head(2).unwrap();
        assert_eq!(h.height(), 2);
        let stats = conn.col_stats("a").unwrap().unwrap();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.distinct, Some(3));
    }

    #[test]
    fn test_duck_connector() {
        // Skip if duckdb not installed
        if std::process::Command::new("duckdb").arg("--version").output().is_err() { return; }
        let conn = DuckConn::new("test".into(), ":memory:".into());
        // Test simple query
        let df = conn.query("SELECT 1 as a, 2 as b").unwrap();
        assert_eq!(df.height(), 1);
    }

    #[test]
    fn test_duck_parquet() {
        if std::process::Command::new("duckdb").arg("--version").output().is_err() { return; }
        let pq = "tests/data/5_001.parquet";
        if !std::path::Path::new(pq).exists() { return; }
        let conn = DuckConn::new("test".into(), pq.into());
        // Test count
        let cnt = conn.count().unwrap().unwrap();
        assert!(cnt > 0);
        // Test head
        let df = conn.head(10).unwrap();
        assert_eq!(df.height(), 10);
        // Test stats (first column)
        let cols = conn.schema().unwrap();
        let first = cols.iter().next().map(|(n, _)| n.to_string());
        if let Some(name) = first {
            let stats = conn.col_stats(&name).unwrap();
            assert!(stats.is_some());
        }
    }
}
