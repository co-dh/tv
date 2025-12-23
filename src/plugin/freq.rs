//! Freq view plugin - frequency/value counts table
//! Lazy PRQL view: GROUP BY + COUNT with Pct/Bar computed via SQL

use crate::app::AppContext;
use crate::utils::unquote;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::state::ViewState;
use crate::util::pure::{qcol, qcols};
use anyhow::{anyhow, Result};

pub struct FreqPlugin;

impl Plugin for FreqPlugin {
    fn name(&self) -> &str { "freq" }
    fn tab(&self) -> &str { "freq" }

    fn matches(&self, name: &str) -> bool { name.starts_with("freq ") }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" | "filter_parent" => {
                // Extract column names and values from freq view
                let col_vals = app.view().and_then(|v| {
                    let cols = v.parent.as_ref().map(|p| p.freq_cols.clone())?;
                    if cols.is_empty() { return None; }
                    let row = v.state.cr;
                    let cv: Vec<(String, String)> = cols.iter().enumerate()
                        .map(|(i, c)| (c.clone(), unquote(&v.data.cell(row, i).format(10))))
                        .filter(|(_, v)| !v.is_empty() && v != "null")
                        .collect();
                    if cv.is_empty() { None } else { Some(cv) }
                });
                col_vals.map(|cv| Box::new(FreqEnter { col_vals: cv }) as Box<dyn Command>)
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
        let (parent_id, parent_rows, parent_name, path, parent_prql) = {
            let v = app.req()?;
            (v.id, v.rows(), v.name.clone(), v.path.clone(), v.prql.clone())
        };
        let cols = qcols(&self.col_names);
        let prql = format!("{}|freq{{{}}}", parent_prql, cols);
        let id = app.next_id();
        let name = format!("freq {}", self.col_names.join(","));
        let mut nv = ViewState::build(id, name)
            .prql(&prql)
            .parent(parent_id, parent_rows, parent_name, self.col_names.clone());
        if let Some(p) = path { nv = nv.path(p); }
        // Set freq columns as key_cols for | separator display
        nv.key_cols = self.col_names.clone();
        app.stack.push(nv);
        Ok(())
    }
    fn to_str(&self) -> String { format!("freq {}", self.col_names.join(",")) }
}

/// Freq Enter: pop freq view and filter parent by column values
pub struct FreqEnter { pub col_vals: Vec<(String, String)> }

impl Command for FreqEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        if !self.col_vals.is_empty() {
            // Build compound filter: col1 = val1 AND col2 = val2 ...
            let conditions: Vec<String> = self.col_vals.iter()
                .map(|(c, v)| format!("{}=='{}'", qcol(c), v.replace('\'', "''")))
                .collect();
            let filter = conditions.join(" && ");
            let _ = CommandExecutor::exec(app, Box::new(crate::command::transform::Filter { expr: filter }));
            // Focus cursor on the first filtered column
            if let Some((col, _)) = self.col_vals.first() {
                if let Some(v) = app.view_mut() {
                    if let Some(i) = v.data.col_names().iter().position(|c| c == col) { v.state.cc = i; }
                }
            }
        }
        Ok(())
    }
    fn to_str(&self) -> String { "freq_enter".to_string() }
    fn record(&self) -> bool { false }
}
