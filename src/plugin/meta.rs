//! Meta view plugin - data profile/metadata statistics (stub)

use crate::app::AppContext;
use crate::utils::unquote;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::Xkey;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use anyhow::{anyhow, Result};

pub struct MetaPlugin;

impl Plugin for MetaPlugin {
    fn name(&self) -> &str { "meta" }
    fn tab(&self) -> &str { "meta" }
    fn matches(&self, name: &str) -> bool { name == "metadata" }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        let col_names = sel_cols(app, cmd == "delete")?;
        match cmd {
            "enter" => Some(Box::new(MetaEnter { col_names })),
            "delete" => Some(Box::new(MetaDelete { col_names })),
            _ => None,
        }
    }

    fn parse(&self, cmd: &str, _arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "meta" | "metadata" => Some(Box::new(Metadata)),
            "meta_enter" => Some(Box::new(MetaEnterCmd)),
            _ => None,
        }
    }
}

/// Get selected column names from meta view
fn sel_cols(app: &AppContext, allow_all: bool) -> Option<Vec<String>> {
    let v = app.view()?;
    let name_idx = v.data.col_names().iter().position(|c| c == "column")?;
    let rows: Vec<usize> = if v.selected_rows.is_empty() {
        if allow_all { (0..v.data.rows()).collect() } else { vec![v.state.cr] }
    } else { v.selected_rows.iter().copied().collect() };
    Some(rows.iter().filter_map(|&r| {
        let s = v.data.cell(r, name_idx).format(10);
        if s.is_empty() || s == "null" { None } else { Some(unquote(&s)) }
    }).collect())
}

/// Metadata command - show column stats (stub - needs plugin)
pub struct Metadata;

impl Command for Metadata {
    fn exec(&mut self, _app: &mut AppContext) -> Result<()> {
        // TODO: implement via plugin
        Err(anyhow!("Metadata not yet implemented without polars"))
    }
    fn to_str(&self) -> String { "meta".to_string() }
}

/// Meta Enter: pop meta view and select column in parent
pub struct MetaEnter { pub col_names: Vec<String> }

impl Command for MetaEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        CommandExecutor::exec(app, Box::new(Pop))?;
        if !self.col_names.is_empty() {
            CommandExecutor::exec(app, Box::new(Xkey { col_names: self.col_names.clone() }))?;
        }
        Ok(())
    }
    fn to_str(&self) -> String { "meta_enter".to_string() }
    fn record(&self) -> bool { false }
}

/// Meta Delete: pop and delete columns
pub struct MetaDelete { pub col_names: Vec<String> }

impl Command for MetaDelete {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        CommandExecutor::exec(app, Box::new(Pop))?;
        if !self.col_names.is_empty() {
            CommandExecutor::exec(app, Box::new(crate::command::transform::DelCol { col_names: self.col_names.clone() }))?;
        }
        Ok(())
    }
    fn to_str(&self) -> String { "meta_delete".to_string() }
    fn record(&self) -> bool { false }
}

/// Meta Enter command (parseable)
pub struct MetaEnterCmd;

impl Command for MetaEnterCmd {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let cols = sel_cols(app, false).unwrap_or_default();
        MetaEnter { col_names: cols }.exec(app)
    }
    fn to_str(&self) -> String { "meta_enter".to_string() }
}
