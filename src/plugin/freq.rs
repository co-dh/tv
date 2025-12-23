//! Freq view plugin - frequency/value counts table
//! Lazy PRQL view: GROUP BY + COUNT with Pct/Bar computed via SQL

use crate::app::AppContext;
use crate::utils::unquote;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::FilterIn;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::{anyhow, Result};

pub struct FreqPlugin;

impl Plugin for FreqPlugin {
    fn name(&self) -> &str { "freq" }
    fn tab(&self) -> &str { "freq" }

    fn matches(&self, name: &str) -> bool { name.starts_with("freq ") }

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
pub struct Frequency { pub col_names: Vec<String> }

impl Command for Frequency {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }

        // Get view info before mutation
        let (parent_id, parent_rows, parent_name, path, key_cols, parent_prql) = {
            let v = app.req()?;
            (v.id, v.rows(), v.name.clone(), v.path.clone(), v.key_cols.clone(), v.prql.clone())
        };

        // Build PRQL using freq function from funcs.prql
        let cols = self.col_names.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
        let prql = format!("{} | freq {{{}}}", parent_prql, cols);

        // Create lazy freq view
        let id = app.next_id();
        let name = format!("freq {}", self.col_names.join(" "));
        let freq_col = self.col_names.first().cloned().unwrap_or_default();
        let mut nv = ViewState::build(id, name)
            .prql(&prql)
            .parent(parent_id, parent_rows, parent_name, Some(freq_col));
        if let Some(p) = path { nv = nv.path(p); }
        nv.key_cols = key_cols;
        app.stack.push(nv);
        Ok(())
    }

    fn to_str(&self) -> String { format!("freq {}", self.col_names.join(",")) }
}

/// Freq Enter: pop freq view and filter parent by selected values
pub struct FreqEnter { pub col: String, pub values: Vec<String> }

impl Command for FreqEnter {
    /// Pop freq view, filter parent by selected values, focus on filtered column
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        if !self.values.is_empty() {
            let _ = CommandExecutor::exec(app, Box::new(FilterIn { col: self.col.clone(), values: self.values.clone() }));
            // Focus cursor on the filtered column in parent view
            if let Some(v) = app.view_mut() {
                if let Some(i) = v.data.col_names().iter().position(|c| c == &self.col) { v.state.cc = i; }
            }
        }
        Ok(())
    }
    fn to_str(&self) -> String { "freq_enter".to_string() }
    fn record(&self) -> bool { false }
}

/// Freq Enter command (parseable) - extracts col/values at exec time
pub struct FreqEnterCmd;

impl Command for FreqEnterCmd {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
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
        FreqEnter { col, values }.exec(app)
    }
    fn to_str(&self) -> String { "freq_enter".to_string() }
}
