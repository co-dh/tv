//! Pivot table plugin (stub - needs polars)

use crate::app::AppContext;
use crate::command::Command;
use crate::plugin::Plugin;
use anyhow::{anyhow, Result};

pub struct PivotPlugin;

impl Plugin for PivotPlugin {
    fn name(&self) -> &str { "pivot" }
    fn tab(&self) -> &str { "table" }
    fn matches(&self, name: &str) -> bool { name.starts_with("Pivot:") }
    fn handle(&self, _cmd: &str, _app: &mut AppContext) -> Option<Box<dyn Command>> { None }

    fn parse(&self, cmd: &str, _arg: &str) -> Option<Box<dyn Command>> {
        if cmd != "pivot" { return None; }
        Some(Box::new(Pivot))
    }
}

/// Pivot table command (stub)
pub struct Pivot;

impl Command for Pivot {
    fn exec(&mut self, _app: &mut AppContext) -> Result<()> {
        Err(anyhow!("Pivot not yet implemented without polars"))
    }
    fn to_str(&self) -> String { "pivot".to_string() }
}
