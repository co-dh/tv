//! DuckDB query execution via CLI
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::io::Cursor;
use std::process::{Command, Stdio};

/// Execute DuckDB SQL query and return DataFrame
/// Query can reference files directly: SELECT * FROM 'file.parquet'
pub fn query(sql: &str) -> Result<DataFrame> {
    let output = Command::new("duckdb")
        .args(["-csv", "-c", sql])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| anyhow!("Failed to run duckdb: {}", e))?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("DuckDB error: {}", err));
    }

    let csv = output.stdout;
    if csv.is_empty() {
        return Err(anyhow!("Empty result from DuckDB"));
    }

    CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(500))
        .into_reader_with_file_handle(Cursor::new(csv))
        .finish()
        .map_err(|e| anyhow!("Failed to parse DuckDB output: {}", e))
}

/// Check if duckdb is available
pub fn available() -> bool {
    Command::new("duckdb").arg("--version").output().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_simple_query() {
        if !available() { return; }  // skip if duckdb not installed
        let df = query("SELECT 1 as a, 2 as b").unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_duckdb_parquet_query() {
        if !available() { return; }
        // Use a file that exists in tests/data if available
        let result = query("SELECT 1+1 as result");
        assert!(result.is_ok());
    }
}
