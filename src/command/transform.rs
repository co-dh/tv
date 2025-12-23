//! Transform commands - filter, sort, select, etc.
//! All operations are lazy PRQL that get appended to the view's prql chain.

use crate::app::AppContext;
use crate::data::table::{Table, ColType};
use crate::command::Command;
use crate::util::pure;
use anyhow::{anyhow, Result};

/// Get schema as (name, type) pairs from Table
fn table_schema(t: &dyn Table) -> Vec<(String, ColType)> {
    (0..t.cols()).map(|c| (t.col_name(c).unwrap_or_default(), t.col_type(c))).collect()
}

/// Delete columns (hide from display, remove from key_cols if present)
pub struct DelCol { pub cols: Vec<String> }

impl Command for DelCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.cols.len();
        let v = app.req_mut()?;
        // Add to deleted_cols
        v.deleted_cols.extend(self.cols.clone());
        // Remove from key_cols if present
        v.key_cols.retain(|k| !self.cols.contains(k));
        // Adjust cursor if needed
        let visible = v.display_cols().len();
        if v.state.cc >= visible && visible > 0 { v.state.cc = visible - 1; }
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }
    fn to_str(&self) -> String { format!("del_col {}", self.cols.join(",")) }
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

/// Aggregate by key columns - lazy, appends to PRQL
/// funcs: list of (func_name, col_name) pairs
pub struct Agg { pub keys: Vec<String>, pub funcs: Vec<(String, String)> }

impl Command for Agg {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if self.keys.is_empty() { return Err(anyhow!("No key columns")); }
        if self.funcs.is_empty() { return Err(anyhow!("No aggregations")); }
        let id = app.next_id();
        let v = app.req()?;
        let parent_prql = v.prql.clone();
        // Build PRQL aggregation expressions
        let agg_exprs: Vec<String> = self.funcs.iter().map(|(func, col)| {
            match func.as_str() {
                "count" => format!("{}_cnt = count `{}`", col, col),
                "sum" => format!("{}_sum = sum `{}`", col, col),
                "mean" => format!("{}_avg = average `{}`", col, col),
                "min" => format!("{}_min = min `{}`", col, col),
                "max" => format!("{}_max = max `{}`", col, col),
                "std" => format!("{}_std = stddev `{}`", col, col),
                _ => format!("{} = {} `{}`", col, func, col),
            }
        }).collect();
        let keys_str = self.keys.iter().map(|k| format!("`{}`", k)).collect::<Vec<_>>().join(", ");
        let prql = format!("{} | group {{{}}} (aggregate {{{}}})", parent_prql, keys_str, agg_exprs.join(", "));
        // Create new view with aggregation - PRQL-like name
        let aggs_short: Vec<String> = self.funcs.iter().map(|(f, c)| format!("{} {}", f, c)).collect();
        let name = format!("group {{{}}} (aggregate {{{}}})", keys_str, aggs_short.join(", "));
        let mut nv = crate::state::ViewState::build(id, name).prql(&prql);
        if let Some(p) = &v.path { nv = nv.path(p); }
        app.stack.push(nv);
        Ok(())
    }
    fn to_str(&self) -> String { format!("agg {:?}", self.funcs) }
    fn record(&self) -> bool { false }
}

/// Filter by IN clause - lazy, appends to PRQL
pub struct FilterIn { pub col: String, pub values: Vec<String> }

impl Command for FilterIn {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let id = app.next_id();
        let v = app.req()?;
        // Check if column is string type
        let schema = table_schema(v.data.as_ref());
        let is_str = schema.iter().find(|(n, _)| n == &self.col)
            .map(|(_, t)| matches!(t, ColType::Str)).unwrap_or(true);
        let clause = pure::in_clause(&self.col, &self.values, is_str);
        let filter_expr = pure::to_prql_filter(&clause);
        let prql = format!("{} | filter {}", v.prql, filter_expr);
        let mut nv = v.clone();
        nv.id = id;
        nv.name = format!("filter {}", filter_expr);  // tab shows command
        nv.prql = prql;
        app.stack.push(nv);
        Ok(())
    }
    fn to_str(&self) -> String { format!("filter_in {} {:?}", self.col, self.values) }
    fn record(&self) -> bool { false }
}

/// Set key columns (display first, no PRQL change)
pub struct Xkey { pub keys: Vec<String> }

impl Command for Xkey {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.key_cols = self.keys.clone();
        v.selected_cols.clear();
        v.selected_cols.extend(0..self.keys.len());
        v.state.cc = self.keys.len().saturating_sub(1);  // cursor on last key
        v.state.col_widths.clear();
        Ok(())
    }
    fn to_str(&self) -> String { format!("xkey {}", self.keys.join(",")) }
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
