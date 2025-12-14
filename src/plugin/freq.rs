//! Freq view plugin - frequency/value counts table
//! Combines: view detection, command handling, Frequency command

use crate::app::AppContext;
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
            "enter" => {
                // Filter parent by selected value(s)
                let info = app.view().and_then(|view| {
                    let freq_col = view.freq_col.clone()?;
                    let rows: Vec<usize> = if view.selected_rows.is_empty() {
                        vec![view.state.cr]
                    } else {
                        view.selected_rows.iter().copied().collect()
                    };
                    let values: Vec<String> = rows.iter()
                        .filter_map(|&r| view.dataframe.get_columns()[0].get(r).ok()
                            .map(|v| v.to_string().trim_matches('"').to_string()))
                        .collect();
                    Some((freq_col, values, view.filename.clone()))
                });

                info.map(|(col, values, filename)| {
                    Box::new(FreqEnter { col, values, filename }) as Box<dyn Command>
                })
            }
            _ => None,
        }
    }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "freq" | "frequency" if !arg.is_empty() => Some(Box::new(Frequency { col_name: arg.to_string() })),
            _ => None,
        }
    }
}

// === Commands ===

/// Frequency table command - shows value counts for a column
pub struct Frequency {
    pub col_name: String,
}

impl Command for Frequency {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Block freq while gz is still loading
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let view = app.req()?;
        let parent_id = view.id;
        let parent_rows = view.rows();
        let parent_name = view.name.clone();
        let path = view.path().to_string();
        let key_cols = view.key_cols();

        // Use view's backend (file or memory)
        let col_names = view.backend().cols(&path)?;
        if !col_names.contains(&self.col_name) {
            return Err(anyhow!("Column '{}' not found", self.col_name));
        }
        let result = add_freq_cols(view.backend().freq(&path, &self.col_name)?)?;

        let id = app.next_id();
        let mut new_view = ViewState::new_freq(
            id, format!("Freq:{}", self.col_name), result,
            parent_id, parent_rows, parent_name, self.col_name.clone(),
        );
        if !key_cols.is_empty() { new_view.col_separator = Some(key_cols.len()); }
        app.stack.push(new_view);
        Ok(())
    }

    fn to_str(&self) -> String { format!("freq {}", self.col_name) }
}

/// Freq Enter: pop view and filter parent by selected values
pub struct FreqEnter {
    pub col: String,
    pub values: Vec<String>,
    pub filename: Option<String>,
}

impl Command for FreqEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));

        if !self.values.is_empty() {
            let _ = CommandExecutor::exec(app, Box::new(FilterIn {
                col: self.col.clone(),
                values: self.values.clone(),
                filename: self.filename.clone(),
            }));
            if let Some(v) = app.view_mut() {
                if let Some(idx) = v.dataframe.get_column_names().iter().position(|c| c.as_str() == self.col) {
                    v.state.cc = idx;
                }
            }
        }
        Ok(())
    }
    fn to_str(&self) -> String { "freq_enter".to_string() }
    fn record(&self) -> bool { false }
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
