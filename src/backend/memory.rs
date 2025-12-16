//! Memory backend - in-memory DataFrame operations.
//! Used for OS commands (ls, ps, tcp), CSV files, and filtered results.
//! Path parameter is ignored - data comes from stored DataFrame reference.
use super::Backend;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Memory backend: tuple struct (df, keys).
/// - `'a` = lifetime parameter, ensures struct doesn't outlive the DataFrame it borrows
/// - Tuple struct fields accessed via .0, .1 (like array indexing)
/// - `pub` on fields allows construction: Memory(&df, vec![])
pub struct Memory<'a>(pub &'a DataFrame, pub Vec<String>);

/// `impl Backend for Memory<'_>` - implement trait for Memory with any lifetime
/// `'_` = elided lifetime, compiler infers it
/// Only lf() and keyed freq need custom impl - all else uses trait defaults.
impl Backend for Memory<'_> {
    /// LazyFrame from in-memory DataFrame (SQL operations use this)
    fn lf(&self, _: &str) -> Result<LazyFrame> { Ok(self.0.clone().lazy()) }

    /// Frequency count - keyed group_by (simple case uses trait default via SQL)
    fn freq(&self, p: &str, c: &str) -> Result<DataFrame> {
        if self.1.is_empty() { return self.freq_where(p, c, "TRUE"); }
        // keyed: group_by [keys..., col] then count
        let mut g: Vec<Expr> = self.1.iter().map(|k| col(k)).collect();
        g.push(col(c));
        self.0.clone().lazy()
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
