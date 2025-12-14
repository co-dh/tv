//! Memory backend - in-memory DataFrame operations.
//! Used for OS commands (ls, ps, tcp), CSV files, and filtered results.
//! Path parameter is ignored - data comes from stored DataFrame reference.
use super::{Backend, df_filter, df_sort_head, df_distinct};
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Memory backend: tuple struct (df, keys).
/// - `'a` = lifetime parameter, ensures struct doesn't outlive the DataFrame it borrows
/// - Tuple struct fields accessed via .0, .1 (like array indexing)
/// - `pub` on fields allows construction: Memory(&df, vec![])
pub struct Memory<'a>(pub &'a DataFrame, pub Vec<String>);

/// `impl Backend for Memory<'_>` - implement trait for Memory with any lifetime
/// `'_` = elided lifetime, compiler infers it
impl Backend for Memory<'_> {
    /// Column names from in-memory DataFrame
    fn cols(&self, _: &str) -> Result<Vec<String>> {
        Ok(self.0.get_column_names().iter().map(|s| s.to_string()).collect())
    }

    /// Schema from in-memory DataFrame
    fn schema(&self, _: &str) -> Result<Vec<(String, String)>> {
        Ok(self.0.schema().iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

    /// Row count and columns from in-memory DataFrame
    fn metadata(&self, _: &str) -> Result<(usize, Vec<String>)> {
        let df = self.0;
        Ok((df.height(), df.get_column_names().iter().map(|s| s.to_string()).collect()))
    }

    /// Slice in-memory DataFrame for viewport
    fn fetch_rows(&self, _: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        Ok(self.0.slice(offset as i64, limit))
    }

    /// Distinct values via common helper
    fn distinct(&self, _: &str, col: &str) -> Result<Vec<String>> { df_distinct(self.0, col) }

    /// Frequency count - simple value_counts or keyed group_by
    fn freq(&self, _: &str, c: &str) -> Result<DataFrame> {
        let (df, keys) = (self.0, &self.1);
        if keys.is_empty() {  // simple: value_counts on single column
            df.column(c)?.as_materialized_series()
                .value_counts(true, false, "Cnt".into(), false)
                .map_err(|e| anyhow!("{}", e))
        } else {  // keyed: group_by [keys..., col] then count
            let mut g: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
            g.push(col(c));
            df.clone().lazy()
                .group_by(g).agg([len().alias("Cnt")])
                .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
                .collect()
                .map_err(|e| anyhow!("{}", e))
        }
    }

    /// Filter via common SQL helper
    fn filter(&self, _: &str, w: &str) -> Result<DataFrame> { df_filter(self.0, w) }
    /// Sort and limit via common helper
    fn sort_head(&self, _: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> { df_sort_head(self.0, col, desc, limit) }
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

    #[test]
    fn test_memory_sort_head() {
        let df = DataFrame::new(vec![
            Column::new("a".into(), vec![3, 1, 4, 1, 5, 9, 2, 6]),
        ]).unwrap();
        // Sort ascending, take 3
        let r = Memory(&df, vec![]).sort_head("", "a", false, 3).unwrap();
        assert_eq!(r.height(), 3);
        assert_eq!(r.column("a").unwrap().get(0).unwrap().try_extract::<i32>().unwrap(), 1);
        assert_eq!(r.column("a").unwrap().get(2).unwrap().try_extract::<i32>().unwrap(), 2);
        // Sort descending, take 2
        let r = Memory(&df, vec![]).sort_head("", "a", true, 2).unwrap();
        assert_eq!(r.height(), 2);
        assert_eq!(r.column("a").unwrap().get(0).unwrap().try_extract::<i32>().unwrap(), 9);
        assert_eq!(r.column("a").unwrap().get(1).unwrap().try_extract::<i32>().unwrap(), 6);
    }
}
