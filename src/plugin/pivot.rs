//! Pivot table plugin - reshape data with row keys, pivot column, and aggregation

use crate::app::AppContext;
use crate::source::df_cols;
use crate::command::Command;
use crate::picker;
use crate::plugin::Plugin;
use crate::state::ViewState;
use crate::ser;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use polars::lazy::frame::pivot;

pub struct PivotPlugin;

impl Plugin for PivotPlugin {
    fn name(&self) -> &str { "pivot" }
    fn tab(&self) -> &str { "table" }
    fn matches(&self, name: &str) -> bool { name.starts_with("Pivot:") }
    fn handle(&self, _cmd: &str, _app: &mut AppContext) -> Option<Box<dyn Command>> { None }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "pivot" if arg.is_empty() => Some(Box::new(PivotPick)),
            "pivot" => {
                // pivot pivot_col value_col [agg]
                let parts: Vec<&str> = arg.split_whitespace().collect();
                if parts.len() >= 2 {
                    let agg = parts.get(2).map(|s| s.to_string());
                    Some(Box::new(Pivot { pivot_col: parts[0].into(), value_col: parts[1].into(), agg }))
                } else { None }
            }
            _ => None,
        }
    }
}

/// Interactive pivot: pick pivot column and value column via fzf
pub struct PivotPick;

impl Command for PivotPick {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (cols, keys, df, parent_id, parent_name, parent_prql) = {
            let v = app.req()?;
            let cols = df_cols(&v.dataframe);
            let keys: Vec<String> = v.col_separator.map(|sep| cols[..sep].to_vec()).unwrap_or_default();
            if keys.is_empty() { return Err(anyhow!("Set xkey columns first (!)")); }
            (cols, keys, v.dataframe.clone(), v.id, v.name.clone(), v.prql.clone())
        };

        // Available columns for pivot (exclude key columns)
        let available: Vec<String> = cols.iter().filter(|c| !keys.contains(c)).cloned().collect();
        if available.len() < 2 { return Err(anyhow!("Need at least 2 non-key columns")); }

        // Pick pivot column (values become headers)
        let pivot_col = picker::fzf(available.clone(), "Pivot column: ")?.ok_or_else(|| anyhow!("No pivot column selected"))?;

        // Pick value column (to aggregate)
        let value_opts: Vec<String> = available.iter().filter(|c| *c != &pivot_col).cloned().collect();
        let value_col = picker::fzf(value_opts, "Value column: ")?.ok_or_else(|| anyhow!("No value column selected"))?;

        // Create placeholder and run in background
        let placeholder = placeholder_pivot(&keys, &pivot_col)?;
        let id = app.next_id();
        let name = format!("Pivot:{}", pivot_col);
        let v = ViewState::new_pivot(id, name, placeholder, parent_id, parent_name, &parent_prql);
        app.stack.push(v);

        // Background pivot computation
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok(r) = do_pivot(&df, &keys, &pivot_col, &value_col, None) {
                let _ = tx.send(r);
            }
        });
        app.bg_meta = Some((parent_id, rx));
        Ok(())
    }
    fn to_str(&self) -> String { "pivot".into() }
}

/// Create placeholder DataFrame for pivot (shows "..." while computing)
fn placeholder_pivot(keys: &[String], pivot_col: &str) -> Result<DataFrame> {
    let mut cols: Vec<Column> = keys.iter().map(|k| ser!(k.as_str(), &["..."])).collect();
    cols.push(ser!(pivot_col, &["..."]));
    Ok(DataFrame::new(cols)?)
}

/// Pivot with specified columns
pub struct Pivot {
    pub pivot_col: String,
    pub value_col: String,
    pub agg: Option<String>,
}

impl Command for Pivot {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (keys, df, parent_id, parent_name, parent_prql) = {
            let v = app.req()?;
            let cols = df_cols(&v.dataframe);
            let keys: Vec<String> = v.col_separator.map(|sep| cols[..sep].to_vec()).unwrap_or_default();
            if keys.is_empty() { return Err(anyhow!("Set xkey columns first (!)")); }
            (keys, v.dataframe.clone(), v.id, v.name.clone(), v.prql.clone())
        };

        // Create placeholder and run in background
        let placeholder = placeholder_pivot(&keys, &self.pivot_col)?;
        let id = app.next_id();
        let name = format!("Pivot:{}", self.pivot_col);
        let v = ViewState::new_pivot(id, name, placeholder, parent_id, parent_name, &parent_prql);
        app.stack.push(v);

        let pivot_col = self.pivot_col.clone();
        let value_col = self.value_col.clone();
        let agg = self.agg.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok(r) = do_pivot(&df, &keys, &pivot_col, &value_col, agg.as_deref()) {
                let _ = tx.send(r);
            }
        });
        app.bg_meta = Some((parent_id, rx));
        Ok(())
    }
    fn to_str(&self) -> String { format!("pivot {} {}", self.pivot_col, self.value_col) }
}

/// Execute pivot operation (eager - needs materialized df)
fn do_pivot(df: &DataFrame, keys: &[String], pivot_col: &str, value_col: &str, agg: Option<&str>) -> Result<DataFrame> {
    // on = pivot column (values become new column headers)
    // index = row keys
    // values = value column to aggregate
    let on = [pivot_col];
    let index: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let values = [value_col];

    // Aggregation: use element() to refer to values in pivot context
    let agg_expr = match agg.unwrap_or("count") {
        "sum" => element().sum(),
        "mean" | "avg" => element().mean(),
        "min" => element().min(),
        "max" => element().max(),
        "first" => element().first(),
        "last" => element().last(),
        _ => element().len(),  // count
    };

    pivot::pivot(df, on, Some(index), Some(values), true, Some(agg_expr), None)
        .map_err(|e| anyhow!("Pivot failed: {}", e))
}
