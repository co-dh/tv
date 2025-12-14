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
impl Backend for Memory<'_> {
    fn cols(&self, _: &str) -> Result<Vec<String>> {
        let df = self.0;  // .0 = first field (DataFrame ref)
        // iter() -> map() -> collect(): functional chain to transform column names
        Ok(df.get_column_names().iter().map(|s| s.to_string()).collect())
    }

    fn schema(&self, _: &str) -> Result<Vec<(String, String)>> {
        let df = self.0;
        // format!("{:?}", dt) = Debug format, shows type like "Int64" or "Utf8"
        Ok(df.schema().iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

    fn freq(&self, _: &str, c: &str) -> Result<DataFrame> {
        let (df, keys) = (self.0, &self.1);  // destructure: .0 = DataFrame, .1 = key columns
        if keys.is_empty() {
            // Simple value_counts on single column
            // ? = early return on error (like try/catch but compile-time checked)
            df.column(c)?.as_materialized_series()
                .value_counts(true, false, "Cnt".into(), false)
                .map_err(|e| anyhow!("{}", e))  // convert polars error to anyhow
        } else {
            // Grouped freq: group by keys + target column
            // col(k) = polars expression for column reference
            let mut g: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
            g.push(col(c));
            // .lazy() = convert to LazyFrame for query optimization
            // .clone() needed because lazy() consumes self
            df.clone().lazy()
                .group_by(g).agg([len().alias("Cnt")])
                .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
                .collect()  // execute lazy query
                .map_err(|e| anyhow!("{}", e))
        }
    }

    fn filter(&self, _: &str, w: &str) -> Result<DataFrame> {
        let df = self.0;
        // SQLContext allows SQL queries on DataFrames
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", df.clone().lazy());  // register as table "df"
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
