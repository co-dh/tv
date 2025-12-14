//! Memory backend - in-memory DataFrame operations.
//! Used for OS commands (ls, ps, tcp), CSV files, and filtered results.
//! Path parameter is ignored - data comes from stored DataFrame reference.
use super::Backend;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::path::Path;

/// Memory backend: tuple struct (df, keys).
/// - `'a` = lifetime parameter, ensures struct doesn't outlive the DataFrame it borrows
/// - Tuple struct fields accessed via .0, .1 (like array indexing)
/// - `pub` on fields allows construction: Memory(&df, vec![])
pub struct Memory<'a>(pub &'a DataFrame, pub Vec<String>);

/// `impl Backend for Memory<'_>` - implement trait for Memory with any lifetime
/// `'_` = elided lifetime, compiler infers it
impl Backend for Memory<'_> {
    fn cols(&self, _: &str) -> Result<Vec<String>> {
        Ok(self.0.get_column_names().iter().map(|s| s.to_string()).collect())
    }

    fn schema(&self, _: &str) -> Result<Vec<(String, String)>> {
        Ok(self.0.schema().iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

    fn metadata(&self, _: &str) -> Result<(usize, Vec<String>)> {
        let df = self.0;
        Ok((df.height(), df.get_column_names().iter().map(|s| s.to_string()).collect()))
    }

    fn fetch_rows(&self, _: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        Ok(self.0.slice(offset as i64, limit))
    }

    fn distinct(&self, _: &str, col: &str) -> Result<Vec<String>> {
        let c = self.0.column(col).map_err(|e| anyhow!("{}", e))?;
        let uniq = c.unique().map_err(|e| anyhow!("{}", e))?;
        let vals: Vec<String> = (0..uniq.len())
            .filter_map(|i| uniq.get(i).ok().map(|v| v.to_string()))
            .filter(|v| v != "null")
            .collect();
        Ok(vals)
    }

    fn save(&self, df: &DataFrame, path: &Path) -> Result<()> {
        let mut df = df.clone();
        ParquetWriter::new(std::fs::File::create(path)?)
            .finish(&mut df)
            .map_err(|e| anyhow!("Failed to write Parquet: {}", e))?;
        Ok(())
    }

    fn freq(&self, _: &str, c: &str) -> Result<DataFrame> {
        let (df, keys) = (self.0, &self.1);
        if keys.is_empty() {
            df.column(c)?.as_materialized_series()
                .value_counts(true, false, "Cnt".into(), false)
                .map_err(|e| anyhow!("{}", e))
        } else {
            let mut g: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
            g.push(col(c));
            df.clone().lazy()
                .group_by(g).agg([len().alias("Cnt")])
                .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
                .collect()
                .map_err(|e| anyhow!("{}", e))
        }
    }

    fn filter(&self, _: &str, w: &str) -> Result<DataFrame> {
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", self.0.clone().lazy());
        ctx.execute(&format!("SELECT * FROM df WHERE {}", w))?
            .collect()
            .map_err(|e| anyhow!("{}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_cols() {
        let df = DataFrame::new(vec![
            Column::new("a".into(), vec![1, 2, 3]),
            Column::new("b".into(), vec!["x", "y", "z"]),
        ]).unwrap();
        assert_eq!(Memory(&df, vec![]).cols("").unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn test_memory_metadata() {
        let df = DataFrame::new(vec![
            Column::new("a".into(), vec![1, 2, 3]),
        ]).unwrap();
        let (rows, cols) = Memory(&df, vec![]).metadata("").unwrap();
        assert_eq!(rows, 3);
        assert_eq!(cols, vec!["a"]);
    }

    #[test]
    fn test_memory_fetch_rows() {
        let df = DataFrame::new(vec![
            Column::new("a".into(), (0..100).collect::<Vec<i32>>()),
        ]).unwrap();
        let slice = Memory(&df, vec![]).fetch_rows("", 10, 5).unwrap();
        assert_eq!(slice.height(), 5);
        assert_eq!(slice.column("a").unwrap().get(0).unwrap().try_extract::<i32>().unwrap(), 10);
    }

    #[test]
    fn test_memory_distinct() {
        let df = DataFrame::new(vec![
            Column::new("cat".into(), vec!["a", "b", "a", "c", "b"]),
        ]).unwrap();
        let vals = Memory(&df, vec![]).distinct("", "cat").unwrap();
        assert_eq!(vals.len(), 3);
    }

    #[test]
    fn test_memory_freq_simple() {
        let df = DataFrame::new(vec![
            Column::new("cat".into(), vec!["a", "b", "a", "a", "b"]),
        ]).unwrap();
        let freq = Memory(&df, vec![]).freq("", "cat").unwrap();
        assert_eq!(freq.height(), 2);
        assert_eq!(freq.column("Cnt").unwrap().get(0).unwrap().try_extract::<u32>().unwrap(), 3);
    }

    #[test]
    fn test_memory_freq_keyed() {
        let df = DataFrame::new(vec![
            Column::new("grp".into(), vec!["x", "x", "y", "y"]),
            Column::new("cat".into(), vec!["a", "a", "a", "b"]),
        ]).unwrap();
        let freq = Memory(&df, vec!["grp".into()]).freq("", "cat").unwrap();
        assert_eq!(freq.height(), 3); // (x,a)=2, (y,a)=1, (y,b)=1
    }

    #[test]
    fn test_memory_filter() {
        let df = DataFrame::new(vec![
            Column::new("a".into(), vec![1, 2, 3, 4, 5]),
        ]).unwrap();
        assert_eq!(Memory(&df, vec![]).filter("", "a > 3").unwrap().height(), 2);
    }
}
