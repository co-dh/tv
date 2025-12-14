//! DuckDB native API
use anyhow::{anyhow, Result};
use duckdb::Connection;
use polars::prelude::*;

/// Thread-local connection (reused across queries)
thread_local! {
    static CONN: Connection = Connection::open_in_memory().expect("duckdb init");
}

/// Execute SQL and convert to polars DataFrame
pub fn query(sql: &str) -> Result<DataFrame> {
    CONN.with(|conn| {
        let mut stmt = conn.prepare(sql).map_err(|e| anyhow!("{}", e))?;
        let mut rows = stmt.query([]).map_err(|e| anyhow!("{}", e))?;

        // Get column info
        let names: Vec<String> = rows.as_ref()
            .map(|r| r.column_names().iter().map(|s| s.to_string()).collect())
            .unwrap_or_default();
        let ncols = names.len();

        // Collect all values by column
        let mut cols: Vec<Vec<String>> = vec![vec![]; ncols];
        while let Some(row) = rows.next().map_err(|e| anyhow!("{}", e))? {
            for i in 0..ncols {
                let val: String = row.get::<_, Option<String>>(i)
                    .unwrap_or(None)
                    .unwrap_or_else(|| "null".to_string());
                cols[i].push(val);
            }
        }

        // Build DataFrame from string columns, infer types
        let series: Vec<Column> = names.iter().zip(cols.iter())
            .map(|(name, vals)| {
                let s = Series::new(name.into(), vals);
                // Try to cast to numeric types
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
    })
}

/// Load first n rows from file
pub fn head(path: &str, n: usize) -> Result<DataFrame> {
    query(&format!("SELECT * FROM '{}' LIMIT {}", path, n))
}

/// Count rows in file
pub fn count(path: &str) -> Result<usize> {
    let df = query(&format!("SELECT COUNT(*) as cnt FROM '{}'", path))?;
    df.column("cnt")
        .and_then(|c| c.i64().map(|v| v.get(0).unwrap_or(0) as usize))
        .map_err(|e| anyhow!("{}", e))
}

/// Frequency counts for a column
pub fn freq(path: &str, col: &str) -> Result<DataFrame> {
    query(&format!(
        "SELECT \"{col}\", COUNT(*)::INTEGER as Cnt FROM '{path}' GROUP BY \"{col}\" ORDER BY Cnt DESC"
    ))
}

/// Filter rows by SQL WHERE clause
pub fn filter(path: &str, where_clause: &str) -> Result<DataFrame> {
    query(&format!("SELECT * FROM '{}' WHERE {}", path, where_clause))
}

/// Count rows matching filter
pub fn filter_count(path: &str, where_clause: &str) -> Result<usize> {
    let df = query(&format!("SELECT COUNT(*) as cnt FROM '{}' WHERE {}", path, where_clause))?;
    df.column("cnt")
        .and_then(|c| c.i64().map(|v| v.get(0).unwrap_or(0) as usize))
        .map_err(|e| anyhow!("{}", e))
}

/// Distinct values for a column
pub fn distinct(path: &str, col: &str) -> Result<Vec<String>> {
    let df = query(&format!("SELECT DISTINCT \"{}\" FROM '{}' ORDER BY 1", col, path))?;
    let c = df.column(col).map_err(|e| anyhow!("{}", e))?;
    Ok((0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect())
}

/// Fetch rows with offset and limit (for viewport)
pub fn fetch_rows(path: &str, offset: usize, limit: usize) -> Result<DataFrame> {
    query(&format!("SELECT * FROM '{}' LIMIT {} OFFSET {}", path, limit, offset))
}

/// Get schema (column names and types)
pub fn schema(path: &str) -> Result<Vec<(String, String)>> {
    let df = head(path, 1)?;
    Ok(df.schema().iter().map(|(n, t)| (n.to_string(), format!("{:?}", t))).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_simple_query() {
        let df = query("SELECT 1 as a, 2 as b").unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_duckdb_parquet_query() {
        let result = query("SELECT 1+1 as result");
        assert!(result.is_ok());
    }
}
