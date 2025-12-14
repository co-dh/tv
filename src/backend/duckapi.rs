//! DuckDB API backend - native crate with Arrow transfer
use super::Backend;
use anyhow::Result;
use polars::prelude::*;

pub struct DuckApi;

impl DuckApi {
    /// Execute SQL via DuckDB, return polars DataFrame (Arrow transfer)
    pub fn query(&self, sql: &str) -> Result<DataFrame> {
        use duckdb::Connection;
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare(sql)?;
        let rbs: Vec<duckdb::arrow::record_batch::RecordBatch> = stmt.query_arrow([])?.collect();
        if rbs.is_empty() { return Ok(DataFrame::empty()); }
        // Convert Arrow RecordBatches to polars DataFrame
        let mut dfs = Vec::new();
        for rb in rbs {
            let schema = rb.schema();
            let mut cols: Vec<Column> = Vec::new();
            for (i, field) in schema.fields().iter().enumerate() {
                let s = arrow_to_series(field.name(), rb.column(i))?;
                cols.push(s.into());
            }
            dfs.push(DataFrame::new(cols)?);
        }
        // Vertical concat all batches
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
            // Fallback: string representation
            let vals: Vec<String> = (0..arr.len()).map(|_| format!("{:?}", arr.as_ref())).collect();
            Ok(Series::new(name, vals))
        }
    }
}

impl Backend for DuckApi {
    /// Get column names from parquet using DuckDB
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        Ok(self.schema(path)?.into_iter().map(|(n, _)| n).collect())
    }

    /// Get schema (column name, type) from parquet using DuckDB
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

    /// Freq from parquet using DuckDB SQL
    fn freq(&self, path: &str, col: &str) -> Result<DataFrame> {
        self.query(&format!(
            "SELECT \"{col}\", COUNT(*)::INTEGER as Cnt FROM '{path}' GROUP BY \"{col}\" ORDER BY Cnt DESC"
        ))
    }

    /// Filter parquet using DuckDB SQL
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
    fn test_duckapi_query() {
        let df = DuckApi.query("SELECT 1 as a, 2 as b").unwrap();
        assert_eq!(df.height(), 1);
    }
}
