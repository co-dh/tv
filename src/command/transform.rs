use crate::app::AppContext;
use crate::state::ViewSource;
use crate::table::{df_to_table, table_to_df};
use crate::command::Command;
use crate::pure;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Delete columns
pub struct DelCol { pub col_names: Vec<String> }

impl Command for DelCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        {
            let v = app.req_mut()?;
            // Pure: count how many deleted cols are before separator
            let sep_adjust = v.col_separator.map(|sep| {
                pure::count_before_sep(&v.data.col_names(), &self.col_names, sep)
            }).unwrap_or(0);
            // Convert to DataFrame, drop columns, convert back
            let mut df = table_to_df(v.data.as_ref());
            for c in &self.col_names { df = df.drop(c)?; }
            v.data = df_to_table(df);
            if let Some(sep) = v.col_separator { v.col_separator = Some(sep.saturating_sub(sep_adjust)); }
            if v.state.cc >= v.cols() && v.cols() > 0 { v.state.cc = v.cols() - 1; }
        }
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }
    fn to_str(&self) -> String { format!("del_col {}", self.col_names.join(",")) }
}

/// Filter rows using SQL WHERE syntax
pub struct Filter { pub expr: String }

impl Command for Filter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let id = app.next_id();
        let v = app.req()?;
        let path = v.path().to_string();
        let parent_prql = v.prql.clone();
        // Pure: chain filters with AND
        let combined = pure::combine_filters(v.filter.as_deref(), &self.expr);
        let name = pure::filter_name(&v.name, &self.expr);
        // Lazy for parquet (disk), materialized for in-memory
        if v.source.is_parquet() {
            let count = v.backend().count_where(&path, &combined)?;
            let cols = v.cols.clone();
            app.stack.push(crate::state::ViewState::new_filtered(id, name, path, cols, combined, count, &parent_prql, &self.expr));
        } else {
            use crate::source::{Source, Memory};
            let df = table_to_df(v.data.as_ref());
            let filtered = Memory(&df).filter("", &combined, FILTER_LIMIT)?;
            app.stack.push(crate::state::ViewState::new(id, name, df_to_table(filtered), v.filename.clone()));
        }
        Ok(())
    }
    fn to_str(&self) -> String { format!("filter {}", self.expr) }
}

/// Select columns
pub struct Select { pub col_names: Vec<String> }

impl Command for Select {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.cols = self.col_names.clone();
        v.cache.fetch = None;
        if !v.source.is_parquet() {
            let df = table_to_df(v.data.as_ref()).select(&self.col_names)?;
            v.data = df_to_table(df);
        }
        v.state.cc = 0;
        Ok(())
    }
    fn to_str(&self) -> String { format!("sel {}", self.col_names.join(",")) }
}

/// Sort by column (limit to 10k rows for large files)
pub struct Sort { pub col_name: String, pub descending: bool }

const SORT_LIMIT: usize = 10_000;
const FILTER_LIMIT: usize = 10_000;

impl Command for Sort {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let sorted = {
            let v = app.req()?;
            let path = v.path().to_string();
            v.backend().sort_head(&path, &self.col_name, self.descending, SORT_LIMIT)?
        };
        let v = app.req_mut()?;
        v.data = df_to_table(sorted);
        v.sort_col = Some(self.col_name.clone());
        v.sort_desc = self.descending;
        // Clear parquet lazy state - now in memory
        v.source = ViewSource::Memory;
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
        let mut df = table_to_df(v.data.as_ref());
        df.rename(&self.old_name, self.new_name.as_str().into())?;
        v.data = df_to_table(df);
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
            let df = table_to_df(v.data.as_ref());
            let grouped = df.lazy().group_by([col(&self.col)]);
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
        app.stack.push(crate::state::ViewState::new(id, format!("{}:{}", self.func, self.col), df_to_table(agg_df), filename));
        Ok(())
    }
    fn to_str(&self) -> String { format!("agg {} {}", self.col, self.func) }
    fn record(&self) -> bool { false }
}

/// Filter by IN clause - creates lazy filtered view for parquet
pub struct FilterIn { pub col: String, pub values: Vec<String> }

impl Command for FilterIn {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let id = app.next_id();
        let v = app.req()?;
        let path = v.path().to_string();
        let is_pq = v.source.is_parquet();
        // Get schema to check if column is string type
        let schema = if is_pq {
            v.backend().schema(&path)?
        } else {
            use crate::source::{Source, Memory};
            let df = table_to_df(v.data.as_ref());
            Memory(&df).schema("")?
        };
        // Pure: check if column is string type
        let is_str = schema.iter().find(|(n, _)| n == &self.col)
            .map(|(_, t)| pure::is_string_type(t)).unwrap_or(true);
        // Pure: build IN clause and name
        let new_clause = pure::in_clause(&self.col, &self.values, is_str);
        let name = pure::filter_in_name(&self.col, &self.values);
        // Lazy filtered view for parquet: combine with existing filter
        if is_pq {
            let combined = pure::combine_filters(v.filter.as_deref(), &new_clause);
            let count = v.backend().count_where(&path, &combined)?;
            let cols = v.cols.clone();
            let parent_prql = v.prql.clone();
            app.stack.push(crate::state::ViewState::new_filtered(id, name, path, cols, combined, count, &parent_prql, &new_clause));
        } else {
            use crate::source::{Source, Memory};
            let df = table_to_df(v.data.as_ref());
            let filtered = Memory(&df).filter("", &new_clause, FILTER_LIMIT)?;
            let filename = v.filename.clone();
            app.stack.push(crate::state::ViewState::new(id, name, df_to_table(filtered), filename));
        }
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
        // Use cols field if set, else data cols
        let all = if v.cols.is_empty() { v.data.col_names() } else { v.cols.clone() };
        // Pure: reorder columns with keys first
        let order = pure::reorder_cols(&all, &self.col_names);
        // Update cols with new order, clear cache
        v.cols = order.clone();
        v.cache.fetch = None;
        // For in-memory, also reorder data
        if !v.source.is_parquet() {
            let df = table_to_df(v.data.as_ref()).select(&order)?;
            v.data = df_to_table(df);
        }
        v.selected_cols.clear();
        v.selected_cols.extend(0..self.col_names.len());
        v.state.cc = 0;
        v.state.col_widths.clear();
        v.col_separator = if self.col_names.is_empty() { None } else { Some(self.col_names.len()) };
        Ok(())
    }
    fn to_str(&self) -> String { format!("xkey {}", self.col_names.join(",")) }
}

/// Take first n rows (PRQL take)
pub struct Take { pub n: usize }

impl Command for Take {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        let df = table_to_df(v.data.as_ref()).head(Some(self.n));
        v.data = df_to_table(df);
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
        let mut df = table_to_df(v.data.as_ref());
        let c = df.column(&self.col_name)?;
        if !c.dtype().is_integer() { return Err(anyhow!("Column must be integer type")); }
        let i64_s = c.cast(&DataType::Int64)?;
        let i64_ca = i64_s.i64()?;
        // Check first non-null value is TAQ format
        let first = i64_ca.into_iter().flatten().next().ok_or_else(|| anyhow!("Column is empty"))?;
        if !is_taq_time(first) { return Err(anyhow!("Value {} doesn't look like TAQ time (HHMMSSNNNNNNNN)", first)); }
        // Convert to nanoseconds since midnight, then to Time
        let ns: Vec<Option<i64>> = i64_ca.into_iter().map(|v| v.map(taq_to_ns)).collect();
        let time_s = Series::new(self.col_name.as_str().into(), ns).cast(&DataType::Time)?;
        df.replace(&self.col_name, time_s)?;
        v.data = df_to_table(df);
        v.state.col_widths.clear();
        Ok(())
    }
    fn to_str(&self) -> String { format!("to_time {}", self.col_name) }
}

/// Cast column to new type
pub struct Cast { pub col_name: String, pub dtype: String }

impl Command for Cast {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        let dt = match self.dtype.as_str() {
            "String" => DataType::String,
            "Int64" => DataType::Int64,
            "Float64" => DataType::Float64,
            "Boolean" => DataType::Boolean,
            _ => return Err(anyhow!("Unknown type: {}", self.dtype)),
        };
        let mut df = table_to_df(v.data.as_ref());
        let c = df.column(&self.col_name)?;
        let new_col = c.cast(&dt)?;
        df.with_column(new_col)?;
        v.data = df_to_table(df);
        v.state.col_widths.clear();
        Ok(())
    }
    fn to_str(&self) -> String { format!("cast {} {}", self.col_name, self.dtype) }
}

/// Derive (copy) a column
pub struct Derive { pub col_name: String }

impl Command for Derive {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        let mut df = table_to_df(v.data.as_ref());
        let c = df.column(&self.col_name)?.clone();
        let new_name = format!("{}_copy", self.col_name);
        let new_col = c.as_materialized_series().clone().with_name(new_name.into());
        df.with_column(new_col)?;
        v.data = df_to_table(df);
        v.state.col_widths.clear();
        Ok(())
    }
    fn to_str(&self) -> String { format!("derive {}", self.col_name) }
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
