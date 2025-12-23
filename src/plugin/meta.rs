//! Meta view plugin - data profile/metadata statistics

use crate::app::AppContext;
use crate::utils::unquote;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::Xkey;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::data::table::{Cell, ColType, SimpleTable};
use crate::data::backend;
use crate::state;
use crate::util::pure::qcol;
use anyhow::{anyhow, Result};

pub struct MetaPlugin;

impl Plugin for MetaPlugin {
    fn name(&self) -> &str { "meta" }
    fn tab(&self) -> &str { "meta" }
    fn matches(&self, name: &str) -> bool { name == "meta" }

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

/// Get selected column names from meta view (sorted by parent column order)
fn sel_cols(app: &AppContext, allow_all: bool) -> Option<Vec<String>> {
    let v = app.view()?;
    let name_idx = v.data.col_names().iter().position(|c| c == "column")?;
    let mut rows: Vec<usize> = if v.selected_rows.is_empty() {
        if allow_all { (0..v.data.rows()).collect() } else { vec![v.state.cr] }
    } else { v.selected_rows.iter().copied().collect() };
    rows.sort();
    Some(rows.iter().filter_map(|&r| {
        let s = v.data.cell(r, name_idx).format(10);
        if s.is_empty() || s == "null" { None } else { Some(unquote(&s)) }
    }).collect())
}

/// Metadata command - show column stats via backend
pub struct Metadata;

impl Command for Metadata {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (parent_rows, parent_name, path) = {
            let v = app.req()?;
            (v.rows(), v.name.clone(), v.path.clone().unwrap_or_default())
        };

        let meta = compute_meta(&path)?;

        let id = app.next_id();
        let view = state::ViewState::build(id, "meta")
            .data(meta)
            .parent(0, parent_rows, &parent_name, None)
            .register();
        app.stack.push(view);
        Ok(())
    }
    fn to_str(&self) -> String { "meta".to_string() }
}

/// Compute column metadata via PRQL - returns BoxTable
fn compute_meta(path: &str) -> Result<crate::data::table::BoxTable> {
    let cols = backend::query(&crate::state::take_chunk("from df", 0), path)
        .ok_or_else(|| anyhow!("empty schema"))?
        .col_names();

    let rows: Vec<Vec<Cell>> = cols.iter().filter_map(|col| {
        let t = backend::query(&format!("from df|meta this.{}", qcol(col)), path)?;
        if t.rows() == 0 { return None; }
        let cnt = t.cell(0, 0);
        let distinct = t.cell(0, 1);
        let total = t.cell(0, 2);
        let mn = t.cell(0, 3);
        let mx = t.cell(0, 4);
        let tot = match total { Cell::Int(n) => n, _ => 1 };
        let cnt_val = match cnt { Cell::Int(n) => n, _ => 0 };
        let null_pct = if tot > 0 { format!("{:.1}", 100.0 * (tot - cnt_val) as f64 / tot as f64) } else { "0".into() };
        Some(vec![Cell::Str(col.to_string()), cnt, distinct, Cell::Str(null_pct), mn, mx])
    }).collect();

    let names = vec!["column", "count", "distinct", "null%", "min", "max"]
        .into_iter().map(String::from).collect();
    let types = vec![ColType::Str, ColType::Int, ColType::Int, ColType::Str, ColType::Str, ColType::Str];
    Ok(Box::new(SimpleTable::new(names, types, rows)))
}

/// Meta Enter: pop meta view and select column in parent
pub struct MetaEnter { pub col_names: Vec<String> }

impl Command for MetaEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        CommandExecutor::exec(app, Box::new(Pop))?;
        if !self.col_names.is_empty() {
            CommandExecutor::exec(app, Box::new(Xkey { keys: self.col_names.clone() }))?;
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
            CommandExecutor::exec(app, Box::new(crate::command::transform::DelCol { cols: self.col_names.clone() }))?;
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
