//! Transform commands - filter, sort, select, etc.
//! All operations are lazy PRQL that get appended to the view's prql chain.

use crate::app::AppContext;
use crate::data::table::{Table, ColType};
use crate::command::Command;
use crate::util::pure;
use anyhow::{anyhow, Result};

/// Get schema as (name, type) pairs from Table
fn table_schema(t: &dyn Table) -> Vec<(String, String)> {
    (0..t.cols()).map(|c| {
        let typ = match t.col_type(c) {
            ColType::Int => "Int",
            ColType::Float => "Float",
            ColType::Bool => "Bool",
            _ => "Str",
        };
        (t.col_name(c).unwrap_or_default(), typ.to_string())
    }).collect()
}

/// Delete columns (via PRQL select excluding deleted cols)
pub struct DelCol { pub col_names: Vec<String> }

impl Command for DelCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        let v = app.req_mut()?;
        // Get remaining columns (exclude deleted ones)
        let remaining: Vec<String> = v.data.col_names().into_iter()
            .filter(|c| !self.col_names.contains(c))
            .collect();
        // Pure: count how many deleted cols are before separator
        let sep_adjust = v.col_separator.map(|sep| {
            pure::count_before_sep(&v.data.col_names(), &self.col_names, sep)
        }).unwrap_or(0);
        // Append select to PRQL chain
        let sel = remaining.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
        v.prql = format!("{} | select {{{}}}", v.prql, sel);
        if let Some(sep) = v.col_separator { v.col_separator = Some(sep.saturating_sub(sep_adjust)); }
        if v.state.cc >= remaining.len() && !remaining.is_empty() { v.state.cc = remaining.len() - 1; }
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }
    fn to_str(&self) -> String { format!("del_col {}", self.col_names.join(",")) }
}

/// Filter rows using SQL WHERE syntax - lazy, appends to PRQL
pub struct Filter { pub expr: String }

impl Command for Filter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let id = app.next_id();
        let v = app.req()?;
        let prql = format!("{} | filter {}", v.prql, pure::to_prql_filter(&self.expr));
        let name = pure::filter_name(&v.name, &self.expr);
        let mut nv = v.clone();
        nv.id = id;
        nv.name = name;
        nv.prql = prql;
        app.stack.push(nv);
        Ok(())
    }
    fn to_str(&self) -> String { format!("filter {}", self.expr) }
}


/// Select columns (lazy - appends to PRQL chain)
/// Select columns - supports column list or raw PRQL select expression
pub struct Select { pub col_names: Vec<String> }

impl Command for Select {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        // Check if first col starts with '{' - raw PRQL select
        let raw = self.col_names.first().map(|s| s.starts_with('{')).unwrap_or(false);
        if raw {
            // Raw PRQL: select {col1, col2, ...}
            let expr = self.col_names.join(",");
            v.prql = format!("{} | select {}", v.prql, expr);
        } else {
            // Column names: select col1, col2
            let sel = self.col_names.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
            v.prql = format!("{} | select {{{}}}", v.prql, sel);
        }
        v.state.cc = 0;
        Ok(())
    }
    fn to_str(&self) -> String { format!("sel {}", self.col_names.join(",")) }
}

/// Sort by column - lazy, replaces consecutive sorts in PRQL
pub struct Sort { pub col_name: String, pub descending: bool }

impl Command for Sort {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.prql = pure::append_sort(&v.prql, &self.col_name, self.descending);
        v.state.top();
        Ok(())
    }
    fn to_str(&self) -> String { format!("{} {}", if self.descending { "sort_desc" } else { "sort_asc" }, self.col_name) }
}

/// Rename column - lazy, appends derive+select to PRQL
pub struct RenameCol { pub old_name: String, pub new_name: String }

impl Command for RenameCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        // Get cols from data, replace old with new
        let cols: Vec<String> = v.data.col_names().into_iter()
            .map(|c| if c == self.old_name { self.new_name.clone() } else { c })
            .collect();
        // PRQL rename: derive new = old | select (all with new name)
        v.prql = format!("{} | derive {{{} = `{}`}}", v.prql, self.new_name, self.old_name);
        let sel = cols.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
        v.prql = format!("{} | select {{{}}}", v.prql, sel);
        v.state.col_widths.clear();
        Ok(())
    }
    fn to_str(&self) -> String { format!("rename {} {}", self.old_name, self.new_name) }
}

/// Aggregate by column - lazy, appends to PRQL
pub struct Agg { pub col: String, pub func: String }

impl Command for Agg {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let id = app.next_id();
        let v = app.req()?;
        let parent_prql = v.prql.clone();
        // Build PRQL aggregation
        let agg_expr = match self.func.as_str() {
            "count" => format!("count = count `{}`", self.col),
            "sum" => format!("sum = sum `{}`", self.col),
            "mean" => format!("mean = average `{}`", self.col),
            "min" => format!("min = min `{}`", self.col),
            "max" => format!("max = max `{}`", self.col),
            _ => return Err(anyhow!("Unknown aggregation: {}", self.func)),
        };
        let prql = format!("{} | group {{`{}`}} (aggregate {{{}}})", parent_prql, self.col, agg_expr);
        // Create new view with aggregation
        let mut new_view = crate::state::ViewState::new(
            id, format!("{}:{}", self.func, self.col),
            Box::new(crate::data::table::SimpleTable::empty()),
            v.path.clone()
        );
        new_view.prql = prql;
        app.stack.push(new_view);
        Ok(())
    }
    fn to_str(&self) -> String { format!("agg {} {}", self.col, self.func) }
    fn record(&self) -> bool { false }
}

/// Filter by IN clause - lazy, appends to PRQL
pub struct FilterIn { pub col: String, pub values: Vec<String> }

impl Command for FilterIn {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let id = app.next_id();
        let v = app.req()?;
        // Get schema to check if column is string type
        let schema = table_schema(v.data.as_ref());
        let is_str = schema.iter().find(|(n, _)| n == &self.col)
            .map(|(_, t)| pure::is_string_type(t)).unwrap_or(true);
        let clause = pure::in_clause(&self.col, &self.values, is_str);
        let prql = format!("{} | filter {}", v.prql, pure::to_prql_filter(&clause));
        let mut nv = v.clone();
        nv.id = id;
        nv.name = pure::filter_in_name(&self.col, &self.values);
        nv.prql = prql;
        app.stack.push(nv);
        Ok(())
    }
    fn to_str(&self) -> String { format!("filter_in {} {:?}", self.col, self.values) }
    fn record(&self) -> bool { false }
}

/// Move columns to front as key columns (display only, no PRQL change)
pub struct Xkey { pub col_names: Vec<String> }

impl Command for Xkey {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        let cols = v.data.col_names();
        // Build display order: key cols first, then rest
        let order: Vec<usize> = pure::reorder_cols(&cols, &self.col_names)
            .iter().filter_map(|name| cols.iter().position(|c| c == name)).collect();
        v.col_order = if self.col_names.is_empty() { None } else { Some(order) };
        v.col_separator = if self.col_names.is_empty() { None } else { Some(self.col_names.len()) };
        v.selected_cols.clear();
        v.selected_cols.extend(0..self.col_names.len());
        v.state.cc = 0;
        v.state.col_widths.clear();
        Ok(())
    }
    fn to_str(&self) -> String { format!("xkey {}", self.col_names.join(",")) }
}

/// Take first n rows (PRQL take)
pub struct Take { pub n: usize }

impl Command for Take {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.prql = format!("{} | take {}", v.prql, self.n);
        Ok(())
    }
    fn to_str(&self) -> String { format!("take {}", self.n) }
}

/// Convert TAQ integer column to Time type - placeholder (needs plugin)
pub struct ToTime { pub col_name: String }

impl Command for ToTime {
    fn exec(&mut self, _app: &mut AppContext) -> Result<()> {
        // TODO: implement via plugin
        Err(anyhow!("ToTime not yet implemented without polars"))
    }
    fn to_str(&self) -> String { format!("to_time {}", self.col_name) }
}

/// Derive (copy) a column - lazy PRQL
/// Derive - raw PRQL derive or column copy
/// If arg starts with '{', treat as raw PRQL derive expression
/// Otherwise, create a copy of the column
pub struct Derive { pub col_name: String }

impl Command for Derive {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        if self.col_name.starts_with('{') {
            // Raw PRQL: derive {name = expr, ...}
            v.prql = format!("{} | derive {}", v.prql, self.col_name);
        } else {
            // Column copy: derive col_copy = col
            let new_name = format!("{}_copy", self.col_name);
            v.prql = format!("{} | derive {{{} = `{}`}}", v.prql, new_name, self.col_name);
        }
        v.state.col_widths.clear();
        Ok(())
    }
    fn to_str(&self) -> String { format!("derive {}", self.col_name) }
}
