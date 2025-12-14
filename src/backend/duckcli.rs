//! DuckDB CLI backend - shell out to duckdb command.
//! Fallback when native duckdb crate has issues. Requires duckdb in PATH.
//! Slower than DuckApi due to CSV serialization overhead.
use super::Backend;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// DuckDB CLI backend. Spawns duckdb process, parses CSV output.
pub struct DuckCli;

impl DuckCli {
    /// Execute SQL via duckdb CLI, parse CSV output to DataFrame.
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
    /// Get column names from parquet using duckdb CLI
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        Ok(self.schema(path)?.into_iter().map(|(n, _)| n).collect())
    }

    /// Get schema (column name, type) from parquet using duckdb CLI
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let df = self.query(&format!("DESCRIBE SELECT * FROM '{}'", path))?;
        let names = df.column("column_name")?;
        let types = df.column("column_type")?;
        Ok((0..names.len()).filter_map(|i| {
            let n = names.get(i).ok()?.to_string().trim_matches('"').to_string();
            let t = types.get(i).ok()?.to_string().trim_matches('"').to_string();
            Some((n, t))
        }).collect())
    }

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
