//! Freq view plugin - frequency/value counts table
//! Combines: view detection, command handling, Frequency command

use crate::app::AppContext;
use crate::table::{df_to_table, table_to_df};
use crate::utils::unquote;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::FilterIn;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;

pub struct FreqPlugin;

impl Plugin for FreqPlugin {
    fn name(&self) -> &str { "freq" }
    fn tab(&self) -> &str { "freq" }

    fn matches(&self, name: &str) -> bool {
        name.starts_with("Freq:")
    }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" | "filter_parent" => {
                // Extract column name and selected values from freq view
                let info = app.view().and_then(|v| {
                    let col = v.parent.as_ref().and_then(|p| p.freq_col.clone())?;
                    let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                        else { v.selected_rows.iter().copied().collect() };
                    let vals: Vec<String> = rows.iter()
                        .map(|&r| v.data.cell(r, 0).format(10))
                        .filter(|s| !s.is_empty() && s != "null")
                        .map(|s| unquote(&s))
                        .collect();
                    Some((col, vals))
                });
                info.map(|(col, values)| Box::new(FreqEnter { col, values }) as Box<dyn Command>)
            }
            _ => None,
        }
    }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "freq" | "frequency" if !arg.is_empty() => {
                let cols: Vec<String> = arg.split(',').map(|s| s.trim().to_string()).collect();
                Some(Box::new(Frequency { col_names: cols }))
            }
            "freq_enter" | "filter_parent" => Some(Box::new(FreqEnterCmd)),
            _ => None,
        }
    }
}

// === Commands ===

/// Frequency table command - shows value counts grouped by columns
pub struct Frequency {
    pub col_names: Vec<String>,  // GROUP BY columns (key cols or current col)
}

impl Command for Frequency {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Block freq while gz is still loading
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }

        // Extract all data from view first (release borrow before mutations)
        let (parent_id, parent_rows, parent_name, path, key_cols, filter, sel_cols, freq_df, is_parquet, df_clone) = {
            let view = app.req()?;
            // Selected cols for aggregation: explicit selection or current column
            let sel: Vec<String> = if view.selected_cols.is_empty() {
                view.col_name(view.state.cc).into_iter().filter(|c| !self.col_names.contains(c)).collect()
            } else {
                view.selected_cols.iter()
                    .filter_map(|&i| view.col_name(i))
                    .filter(|c| !self.col_names.contains(c))
                    .collect()
            };
            let is_pq = view.source.is_parquet();
            let p = view.path().to_string();
            let w = view.filter.as_deref().unwrap_or("TRUE");
            // Use Memory source for in-memory tables, Polars for parquet
            let (cols, df, df_c) = if is_pq {
                let src = view.backend();
                let c = src.cols(&p)?;
                let d = src.freq_where(&p, &self.col_names, w)?;
                (c, d, None)
            } else {
                let mem_df = table_to_df(view.data.as_ref());
                use crate::source::{Source, Memory};
                let src = Memory(&mem_df);
                let c = view.data.col_names();
                let d = src.freq_where("", &self.col_names, w)?;
                let df_c = if !sel.is_empty() { Some(mem_df) } else { None };
                (c, d, df_c)
            };
            for c in &self.col_names {
                if !cols.contains(c) { return Err(anyhow!("Column '{}' not found", c)); }
            }
            (view.id, view.rows(), view.name.clone(), p, view.key_cols(), view.filter.clone(), sel, df, is_pq, df_c)
        };

        let result = add_freq_cols(freq_df)?;
        let id = app.next_id();
        let name = format!("Freq:{}", self.col_names.join(","));
        let freq_col = self.col_names.first().cloned().unwrap_or_default();
        // Get parent prql before pushing
        let parent_prql = app.req()?.prql.clone();
        let mut new_view = ViewState::new_freq(
            id, name, df_to_table(result), parent_id, parent_rows, parent_name, freq_col, &parent_prql, &self.col_names,
        );
        if !key_cols.is_empty() { new_view.col_separator = Some(key_cols.len()); }
        app.stack.push(new_view);

        // Compute aggregates for selected columns (uses SQL source)
        if !sel_cols.is_empty() {
            let w = filter.as_deref().unwrap_or("TRUE");
            use crate::source::{Source, Polars, Memory};
            let agg_result = if is_parquet {
                Polars.freq_agg(&path, &self.col_names, w, &sel_cols)
            } else if let Some(ref df) = df_clone {
                Memory(df).freq_agg(&path, &self.col_names, w, &sel_cols)
            } else {
                return Ok(());
            };
            if let Ok(agg_df) = agg_result {
                if let Ok(full_df) = add_freq_cols(agg_df) {
                    if let Some(v) = app.stack.cur_mut() {
                        if v.id == id {
                            v.data = df_to_table(full_df);
                            v.state.col_widths.clear();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn to_str(&self) -> String { format!("freq {}", self.col_names.join(",")) }
}

/// Freq Enter: pop freq view and filter parent by selected values
pub struct FreqEnter { pub col: String, pub values: Vec<String> }

impl Command for FreqEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        if !self.values.is_empty() {
            let _ = CommandExecutor::exec(app, Box::new(FilterIn { col: self.col.clone(), values: self.values.clone() }));
            // Move cursor to filter column
            if let Some(v) = app.view_mut() {
                if let Some(i) = v.data.col_names().iter().position(|c| c == &self.col) { v.state.cc = i; }
            }
        }
        Ok(())
    }
    fn to_str(&self) -> String { "freq_enter".to_string() }
    fn record(&self) -> bool { false }
}

/// Freq Enter command (parseable) - extracts col/values from current view at exec time
pub struct FreqEnterCmd;

impl Command for FreqEnterCmd {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Extract col and values from current freq view
        let (col, values) = app.view().and_then(|v| {
            let col = v.parent.as_ref().and_then(|p| p.freq_col.clone())?;
            let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                else { v.selected_rows.iter().copied().collect() };
            let vals: Vec<String> = rows.iter()
                .map(|&r| v.data.cell(r, 0).format(10))
                .filter(|s| !s.is_empty() && s != "null")
                .map(|s| unquote(&s))
                .collect();
            Some((col, vals))
        }).ok_or_else(|| anyhow!("Not a freq view"))?;
        // Delegate to FreqEnter
        FreqEnter { col, values }.exec(app)
    }
    fn to_str(&self) -> String { "freq_enter".to_string() }
}

// === Helpers ===

fn add_freq_cols(mut df: DataFrame) -> Result<DataFrame> {
    let cnt_col = df.column("Cnt")?.as_materialized_series();
    let total: i64 = cnt_col.sum().unwrap_or(0);
    // Handle u32 (polars) or i32/i64 (duckdb) count types
    let counts: Vec<i64> = if let Ok(ca) = cnt_col.u32() {
        ca.into_iter().map(|v| v.unwrap_or(0) as i64).collect()
    } else if let Ok(ca) = cnt_col.i32() {
        ca.into_iter().map(|v| v.unwrap_or(0) as i64).collect()
    } else if let Ok(ca) = cnt_col.i64() {
        ca.into_iter().map(|v| v.unwrap_or(0)).collect()
    } else {
        vec![0; df.height()]
    };
    let pcts: Vec<f64> = counts.iter().map(|&c| 100.0 * c as f64 / total as f64).collect();
    let bars: Vec<String> = pcts.iter().map(|&p| "#".repeat(p.floor() as usize)).collect();
    df.with_column(Series::new("Pct".into(), pcts))?;
    df.with_column(Series::new("Bar".into(), bars))?;
    Ok(df)
}
