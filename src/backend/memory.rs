//! Memory backend - in-memory DataFrame operations.
//! Used for OS commands (ls, ps, tcp), CSV files, and filtered results.
//! Path parameter is ignored - data comes from stored DataFrame reference.
use super::{Backend, df_filter, df_sort_head, df_distinct, df_cols, df_schema, df_metadata, df_fetch, df_freq, df_count_where, df_fetch_where, df_freq_where};
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
    fn cols(&self, _: &str) -> Result<Vec<String>> { Ok(df_cols(self.0)) }
    fn schema(&self, _: &str) -> Result<Vec<(String, String)>> { Ok(df_schema(self.0)) }
    fn metadata(&self, _: &str) -> Result<(usize, Vec<String>)> { Ok(df_metadata(self.0)) }
    fn fetch_rows(&self, _: &str, offset: usize, limit: usize) -> Result<DataFrame> { Ok(df_fetch(self.0, offset, limit)) }
    fn distinct(&self, _: &str, col: &str) -> Result<Vec<String>> { df_distinct(self.0, col) }
    fn filter(&self, _: &str, w: &str, limit: usize) -> Result<DataFrame> { df_filter(self.0, w, limit) }
    fn sort_head(&self, _: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> { df_sort_head(self.0, col, desc, limit) }
    fn fetch_where(&self, _: &str, w: &str, offset: usize, limit: usize) -> Result<DataFrame> { df_fetch_where(self.0, w, offset, limit) }
    fn count_where(&self, _: &str, w: &str) -> Result<usize> { df_count_where(self.0, w) }
    fn freq_where(&self, _: &str, col: &str, w: &str) -> Result<DataFrame> { df_freq_where(self.0, col, w) }

    /// Frequency count - simple value_counts or keyed group_by
    fn freq(&self, _: &str, c: &str) -> Result<DataFrame> {
        let (df, keys) = (self.0, &self.1);
        if keys.is_empty() { return df_freq(df, c); }
        // keyed: group_by [keys..., col] then count
        let mut g: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
        g.push(col(c));
        df.clone().lazy()
            .group_by(g).agg([len().alias("Cnt")])
            .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
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
        assert_eq!(Memory(&df, vec![]).filter("", "a > 3", 1000).unwrap().height(), 2);
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
