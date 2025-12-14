//! DuckDB CLI backend - shell out to duckdb command
use super::Backend;
use anyhow::{anyhow, Result};
use polars::prelude::*;

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
        let csv = String::from_utf8_lossy(&out.stdout);
        CsvReader::new(std::io::Cursor::new(csv.as_bytes()))
            .finish()
            .map_err(|e| anyhow!("csv parse: {}", e))
    }
}

impl Backend for DuckCli {
    /// Freq from parquet using duckdb CLI
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame> {
        self.query(&format!(
            "SELECT \"{col}\", COUNT(*)::INTEGER as Cnt FROM '{path}' GROUP BY \"{col}\" ORDER BY Cnt DESC"
        ))
    }

    /// Filter parquet using duckdb CLI
    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        self.query(&format!("SELECT * FROM '{}' WHERE {}", path, where_clause))
    }

    /// Freq from in-memory DataFrame (delegates to memory backend)
    fn freq_df(&self, df: &DataFrame, col: &str, keys: &[String]) -> Result<DataFrame> {
        super::Memory.freq_df(df, col, keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckcli_query() {
        // Only run if duckdb is installed
        if std::process::Command::new("duckdb").arg("--version").output().is_ok() {
            let df = DuckCli.query("SELECT 1 as a, 2 as b").unwrap();
            assert_eq!(df.height(), 1);
        }
    }
}
