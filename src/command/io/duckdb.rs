//! DuckDB native API
use anyhow::{anyhow, Result};
use duckdb::Connection;
use polars::prelude::*;

// Thread-local connection (reused across queries)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_simple_query() {
        let df = query("SELECT 1 as a, 2 as b").unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 2);
    }
}
