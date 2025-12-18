//! Memory backend - in-memory DataFrame operations.
//! Used for OS commands (ls, ps, tcp), CSV files, and filtered results.
//! Path parameter is ignored - data comes from stored DataFrame reference.
use super::Backend;
use anyhow::Result;
use polars::prelude::*;

/// Memory backend wrapping a DataFrame reference.
/// All trait defaults work via lf() - no custom overrides needed.
pub struct Memory<'a>(pub &'a DataFrame);

impl Backend for Memory<'_> {
    fn lf(&self, _: &str) -> Result<LazyFrame> { Ok(self.0.clone().lazy()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_df;

    #[test]
    fn test_memory_cols() {
        let df = test_df!("a" => vec![1, 2, 3], "b" => vec!["x", "y", "z"]);
        assert_eq!(Memory(&df).cols("").unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn test_memory_metadata() {
        let df = test_df!("a" => vec![1, 2, 3]);
        let (rows, cols) = Memory(&df).metadata("").unwrap();
        assert_eq!(rows, 3);
        assert_eq!(cols, vec!["a"]);
    }

    #[test]
    fn test_memory_fetch_rows() {
        let df = test_df!("a" => (0..100).collect::<Vec<i32>>());
        let slice = Memory(&df).fetch_rows("", 10, 5).unwrap();
        assert_eq!(slice.height(), 5);
        assert_eq!(slice.column("a").unwrap().get(0).unwrap().try_extract::<i32>().unwrap(), 10);
    }

    #[test]
    fn test_memory_distinct() {
        let df = test_df!("cat" => vec!["a", "b", "a", "c", "b"]);
        assert_eq!(Memory(&df).distinct("", "cat").unwrap().len(), 3);
    }

    #[test]
    fn test_memory_freq() {
        let df = test_df!("cat" => vec!["a", "b", "a", "a", "b"]);
        let freq = Memory(&df).freq_where("", "cat", "TRUE").unwrap();
        assert_eq!(freq.height(), 2);
        assert_eq!(freq.column("Cnt").unwrap().get(0).unwrap().try_extract::<u32>().unwrap(), 3);
    }

    #[test]
    fn test_memory_filter() {
        let df = test_df!("a" => vec![1, 2, 3, 4, 5]);
        assert_eq!(Memory(&df).filter("", "a > 3", 1000).unwrap().height(), 2);
    }

    #[test]
    fn test_memory_sort_head() {
        let df = test_df!("a" => vec![3, 1, 4, 1, 5, 9, 2, 6]);
        let r = Memory(&df).sort_head("", "a", false, 3).unwrap();
        assert_eq!(r.height(), 3);
        assert_eq!(r.column("a").unwrap().get(0).unwrap().try_extract::<i32>().unwrap(), 1);
        let r = Memory(&df).sort_head("", "a", true, 2).unwrap();
        assert_eq!(r.height(), 2);
        assert_eq!(r.column("a").unwrap().get(0).unwrap().try_extract::<i32>().unwrap(), 9);
    }
}
