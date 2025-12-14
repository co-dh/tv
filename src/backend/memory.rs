//! Memory backend - in-memory DataFrame operations
use super::Backend;
use anyhow::{anyhow, Result};
use polars::prelude::*;

pub struct Memory;

impl Backend for Memory {
    /// Not supported for memory backend (no file path)
    fn cols(&self, _path: &str) -> Result<Vec<String>> {
        Err(anyhow!("Memory backend does not support file operations"))
    }

    /// Not supported for memory backend (no file path)
    fn schema(&self, _path: &str) -> Result<Vec<(String, String)>> {
        Err(anyhow!("Memory backend does not support file operations"))
    }

    /// Not supported for memory backend (no file path)
    fn freq(&self, _path: &str, _col: &str) -> Result<DataFrame> {
        Err(anyhow!("Memory backend does not support file operations"))
    }

    /// Not supported for memory backend (no file path)
    fn filter(&self, _path: &str, _where_clause: &str) -> Result<DataFrame> {
        Err(anyhow!("Memory backend does not support file operations"))
    }

    /// Freq from in-memory DataFrame
    fn freq_df(&self, df: &DataFrame, name: &str, keys: &[String]) -> Result<DataFrame> {
        if keys.is_empty() {
            // Simple value_counts on single column
            let series = df.column(name)?.as_materialized_series();
            series.value_counts(true, false, "Cnt".into(), false)
                .map_err(|e| anyhow!("{}", e))
        } else {
            // Group by key columns + target column
            let mut group_cols: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
            group_cols.push(col(name));
            df.clone().lazy()
                .group_by(group_cols)
                .agg([len().alias("Cnt")])
                .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
                .collect()
                .map_err(|e| anyhow!("{}", e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_freq_simple() {
        let df = DataFrame::new(vec![
            Column::new("cat".into(), vec!["a", "b", "a", "a", "b"]),
        ]).unwrap();
        let freq = Memory.freq_df(&df, "cat", &[]).unwrap();
        assert_eq!(freq.height(), 2);
        // "a" has 3, "b" has 2
        let cnt = freq.column("Cnt").unwrap();
        assert_eq!(cnt.get(0).unwrap().try_extract::<u32>().unwrap(), 3);
    }

    #[test]
    fn test_memory_freq_keyed() {
        let df = DataFrame::new(vec![
            Column::new("grp".into(), vec!["x", "x", "y", "y"]),
            Column::new("cat".into(), vec!["a", "a", "a", "b"]),
        ]).unwrap();
        let freq = Memory.freq_df(&df, "cat", &["grp".to_string()]).unwrap();
        assert_eq!(freq.height(), 3); // (x,a)=2, (y,a)=1, (y,b)=1
    }
}
