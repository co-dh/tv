use crate::app::AppContext;
use crate::command::Command;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Delete columns
pub struct DelCol { pub col_names: Vec<String> }

impl Command for DelCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        {
            let v = app.req_mut()?;
            // Count how many deleted cols are before separator
            let sep_adjust = if let Some(sep) = v.col_separator {
                let all: Vec<String> = v.dataframe.get_column_names().iter().map(|s| s.to_string()).collect();
                self.col_names.iter().filter(|c| all.iter().position(|n| n == *c).map(|i| i < sep).unwrap_or(false)).count()
            } else { 0 };
            for c in &self.col_names { v.dataframe = v.dataframe.drop(c)?; }
            if let Some(sep) = v.col_separator { v.col_separator = Some(sep.saturating_sub(sep_adjust)); }
            if v.state.cc >= v.cols() && v.cols() > 0 { v.state.cc = v.cols() - 1; }
        }
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }
    fn to_str(&self) -> String { format!("del_col {}", self.col_names.join(",")) }
}

/// Filter rows using PRQL filter syntax
pub struct Filter { pub expr: String }

impl Command for Filter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Block filter while gz is still loading
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let (filtered, filename) = {
            let v = app.req()?;
            let where_clause = crate::prql::filter_to_sql(&self.expr)?;
            let path = v.path().to_string();
            (v.backend().filter(&path, &where_clause)?, v.filename.clone())
        };
        let id = app.next_id();
        app.stack.push(crate::state::ViewState::new(id, self.expr.clone(), filtered, filename));
        Ok(())
    }
    fn to_str(&self) -> String { format!("filter {}", self.expr) }
}

/// Select columns
pub struct Select { pub col_names: Vec<String> }

impl Command for Select {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.dataframe = v.dataframe.select(&self.col_names)?;
        v.state.cc = 0;
        Ok(())
    }
    fn to_str(&self) -> String { format!("sel {}", self.col_names.join(",")) }
}

/// Sort by column (limit to 10k rows for large files)
pub struct Sort { pub col_name: String, pub descending: bool }

const SORT_LIMIT: usize = 10_000;

impl Command for Sort {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let sorted = {
            let v = app.req()?;
            let path = v.path().to_string();
            v.backend().sort_head(&path, &self.col_name, self.descending, SORT_LIMIT)?
        };
        let v = app.req_mut()?;
        v.dataframe = sorted;
        v.sort_col = Some(self.col_name.clone());
        v.sort_desc = self.descending;
        // Clear parquet lazy state - now in memory
        v.disk_rows = None;
        v.parquet_path = None;
        v.state.top();  // reset to top after sort
        Ok(())
    }
    fn to_str(&self) -> String { format!("{} {}", if self.descending { "sort_desc" } else { "sort_asc" }, self.col_name) }
}

/// Rename column
pub struct RenameCol { pub old_name: String, pub new_name: String }

impl Command for RenameCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.dataframe.rename(&self.old_name, self.new_name.as_str().into())?;
        v.state.col_widths.clear(); // force width recalc
        Ok(())
    }
    fn to_str(&self) -> String { format!("rename {} {}", self.old_name, self.new_name) }
}

/// Aggregate by column
pub struct Agg { pub col: String, pub func: String }

impl Command for Agg {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (agg_df, filename) = {
            let v = app.req()?;
            let grouped = v.dataframe.clone().lazy().group_by([col(&self.col)]);
            let result = match self.func.as_str() {
                "count" => grouped.agg([col("*").count().alias("count")]),
                "sum" => grouped.agg([col("*").sum()]),
                "mean" => grouped.agg([col("*").mean()]),
                "min" => grouped.agg([col("*").min()]),
                "max" => grouped.agg([col("*").max()]),
                "std" => grouped.agg([col("*").std(1)]),
                _ => return Err(anyhow::anyhow!("Unknown aggregation: {}", self.func)),
            };
            (result.collect()?, v.filename.clone())
        };
        let id = app.next_id();
        app.stack.push(crate::state::ViewState::new(id, format!("{}:{}", self.func, self.col), agg_df, filename));
        Ok(())
    }
    fn to_str(&self) -> String { format!("agg {} {}", self.col, self.func) }
    fn record(&self) -> bool { false }
}

/// Filter by values (SQL IN clause) - used by frequency view
pub struct FilterIn { pub col: String, pub values: Vec<String>, pub filename: Option<String> }

impl Command for FilterIn {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = app.req()?.dataframe.clone();
        let is_str = matches!(df.column(&self.col)?.dtype(), DataType::String);
        let vals = self.values.iter().map(|v| if is_str { format!("'{}'", v) } else { v.clone() }).collect::<Vec<_>>().join(",");
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", df.lazy());
        let filtered = ctx.execute(&format!("SELECT * FROM df WHERE {} IN ({})", self.col, vals))?.collect()?;
        let id = app.next_id();
        let name = if self.values.len() == 1 { format!("{}={}", self.col, self.values[0]) }
                   else { format!("{}∈{{{}}}", self.col, self.values.len()) };
        app.stack.push(crate::state::ViewState::new(id, name, filtered, self.filename.clone()));
        Ok(())
    }
    fn to_str(&self) -> String { format!("filter_in {} {:?}", self.col, self.values) }
    fn record(&self) -> bool { false }
}

/// Move columns to front as key columns (with separator)
pub struct Xkey { pub col_names: Vec<String> }

impl Command for Xkey {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        let all: Vec<String> = v.dataframe.get_column_names().iter().map(|s| s.to_string()).collect();
        let rest: Vec<String> = all.iter().filter(|c| !self.col_names.contains(c)).cloned().collect();
        let mut order = self.col_names.clone();
        order.extend(rest);
        v.dataframe = v.dataframe.select(&order)?;
        v.selected_cols.clear();
        for i in 0..self.col_names.len() { v.selected_cols.insert(i); }
        v.state.cc = 0;
        v.state.col_widths.clear();
        v.col_separator = Some(self.col_names.len());
        Ok(())
    }
    fn to_str(&self) -> String { format!("xkey {}", self.col_names.join(",")) }
}

/// Take first n rows (PRQL take)
pub struct Take { pub n: usize }

impl Command for Take {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.dataframe = v.dataframe.head(Some(self.n));
        Ok(())
    }
    fn to_str(&self) -> String { format!("take {}", self.n) }
}

/// Convert TAQ integer column to Time type (HHMMSSNNNNNNNN format)
pub struct ToTime { pub col_name: String }

impl Command for ToTime {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use crate::command::io::convert::{is_taq_time, taq_to_ns};
        let v = app.req_mut()?;
        let c = v.dataframe.column(&self.col_name)?;
        if !c.dtype().is_integer() { return Err(anyhow!("Column must be integer type")); }
        let i64_s = c.cast(&DataType::Int64)?;
        let i64_ca = i64_s.i64()?;
        // Check first non-null value is TAQ format
        let first = i64_ca.into_iter().flatten().next().ok_or_else(|| anyhow!("Column is empty"))?;
        if !is_taq_time(first) { return Err(anyhow!("Value {} doesn't look like TAQ time (HHMMSSNNNNNNNN)", first)); }
        // Convert to nanoseconds since midnight, then to Time
        let ns: Vec<Option<i64>> = i64_ca.into_iter().map(|v| v.map(taq_to_ns)).collect();
        let time_s = Series::new(self.col_name.as_str().into(), ns).cast(&DataType::Time)?;
        v.dataframe.replace(&self.col_name, time_s)?;
        v.state.col_widths.clear();
        Ok(())
    }
    fn to_str(&self) -> String { format!("to_time {}", self.col_name) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn df() -> DataFrame {
        df! { "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"] }.unwrap()
    }

    fn filt(e: &str) -> usize {
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", df().lazy());
        ctx.execute(&format!("SELECT * FROM df WHERE {}", e)).unwrap().collect().unwrap().height()
    }

    #[test]
    fn test_str_exact() { assert_eq!(filt("name = 'apple'"), 1); }

    #[test]
    fn test_str_contains() { assert_eq!(filt("name LIKE '%apple%'"), 2); }  // apple, pineapple

    #[test]
    fn test_str_ends() { assert_eq!(filt("name LIKE '%rry'"), 2); }  // cherry, blueberry

    #[test]
    fn test_str_starts() { assert_eq!(filt("name LIKE 'b%'"), 2); }  // banana, blueberry

    #[test]
    fn test_in_single() { assert_eq!(filt("name IN ('apple')"), 1); }

    #[test]
    fn test_in_multiple() { assert_eq!(filt("name IN ('apple','banana')"), 2); }

    #[test]
    fn test_in_multi_match() { assert_eq!(filt("name IN ('apple','banana','cherry')"), 3); }

    // Datetime filter tests using SQL syntax
    #[test]
    fn test_datetime_range() {
        let dates = ["2025-01-15", "2025-02-20", "2025-02-28"];
        let d = df! {
            "dt" => dates.iter().map(|s| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
            }).collect::<Vec<_>>()
        }.unwrap();

        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", d.lazy());
        let result = ctx.execute("SELECT * FROM df WHERE dt >= '2025-02-01' AND dt < '2025-03-01'")
            .unwrap().collect().unwrap();
        assert_eq!(result.height(), 2);  // 2025-02-20, 2025-02-28
    }

    #[test]
    fn test_datetime_year_range() {
        let dates = ["2024-12-31", "2025-01-15", "2025-06-20", "2026-01-01"];
        let d = df! {
            "dt" => dates.iter().map(|s| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
            }).collect::<Vec<_>>()
        }.unwrap();

        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", d.lazy());
        let result = ctx.execute("SELECT * FROM df WHERE dt >= '2025-01-01' AND dt < '2026-01-01'")
            .unwrap().collect().unwrap();
        assert_eq!(result.height(), 2);  // 2025-01-15, 2025-06-20
    }
}
