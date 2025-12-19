//! Correlation matrix plugin (stub - needs polars)

use crate::app::AppContext;
use crate::utils::unquote;
use crate::command::Command;
use crate::plugin::Plugin;
use anyhow::{anyhow, Result};

pub struct CorrPlugin;

impl Plugin for CorrPlugin {
    fn name(&self) -> &str { "corr" }
    fn tab(&self) -> &str { "corr" }
    fn matches(&self, name: &str) -> bool { name == "correlation" }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        if cmd != "enter" { return None; }
        // Get column name from current row
        let col_name = app.view().and_then(|v| {
            let col_idx = v.data.col_names().iter().position(|c| c == "column")?;
            let s = v.data.cell(v.state.cr, col_idx).format(10);
            if s.is_empty() || s == "null" { None } else { Some(unquote(&s)) }
        })?;
        Some(Box::new(CorrEnter { col_name }))
    }

    fn parse(&self, cmd: &str, _arg: &str) -> Option<Box<dyn Command>> {
        if cmd != "corr" { return None; }
        Some(Box::new(Correlation { selected_cols: vec![] }))
    }
}

/// Correlation matrix for numeric columns (stub)
pub struct Correlation { pub selected_cols: Vec<usize> }

impl Command for Correlation {
    fn exec(&mut self, _app: &mut AppContext) -> Result<()> {
        Err(anyhow!("Correlation not yet implemented without polars"))
    }
    fn to_str(&self) -> String { "corr".to_string() }
}

/// Corr Enter - jump to column
pub struct CorrEnter { pub col_name: String }

impl Command for CorrEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = crate::command::executor::CommandExecutor::exec(
            app, Box::new(crate::command::view::Pop)
        );
        if let Some(v) = app.view_mut() {
            if let Some(i) = v.data.col_names().iter().position(|c| c == &self.col_name) {
                v.state.cc = i;
            }
        }
        Ok(())
    }
    fn to_str(&self) -> String { "corr_enter".to_string() }
    fn record(&self) -> bool { false }
}
