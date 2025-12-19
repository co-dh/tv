//! Meta view plugin - data profile/metadata statistics

use crate::app::AppContext;
use crate::utils::unquote;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::Xkey;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::table::{Cell, ColType, SimpleTable, Table};
use crate::{dynload, state};
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

/// Metadata command - show column stats via plugin SQL
pub struct Metadata;

impl Command for Metadata {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Get view info before mutation
        let (parent_rows, parent_name, path, parent_prql) = {
            let v = app.req()?;
            (v.rows(), v.name.clone(), v.path().to_string(), v.prql.clone())
        };

        // Get column metadata via plugin SQL
        let plugin = dynload::get().ok_or_else(|| anyhow!("plugin not loaded"))?;
        let meta = compute_meta(plugin, &path)?;

        let id = app.next_id();
        let view = state::ViewState::new_meta(id, meta, 0, parent_rows, &parent_name, &parent_prql);
        app.stack.push(view);
        Ok(())
    }
    fn to_str(&self) -> String { "meta".to_string() }
}

/// Compute column metadata via SQL - returns BoxTable
fn compute_meta(plugin: &'static dynload::Plugin, path: &str) -> Result<crate::table::BoxTable> {
    // Get column names
    let cols = plugin.schema(path);
    if cols.is_empty() { return Err(anyhow!("empty schema")); }

    // Build SQL for each column's stats: count, distinct, null%, min, max
    let mut rows: Vec<Vec<Cell>> = Vec::new();
    for col in &cols {
        let q = format!(
            "SELECT count(\"{col}\") as cnt, count(distinct \"{col}\") as d, \
             count(*) as total, min(\"{col}\") as mn, max(\"{col}\") as mx \
             FROM df",
            col = col
        );
        if let Some(t) = plugin.query(&q, path) {
            if t.rows() > 0 {
                let cnt = t.cell(0, 0);    // count non-null
                let distinct = t.cell(0, 1);  // distinct
                let total = t.cell(0, 2);  // total rows
                let mn = t.cell(0, 3);     // min
                let mx = t.cell(0, 4);     // max

                // Compute null%
                let tot = match &total { Cell::Int(n) => *n, _ => 1 };
                let cnt_val = match &cnt { Cell::Int(n) => *n, _ => 0 };
                let null_pct = if tot > 0 { format!("{:.1}", 100.0 * (tot - cnt_val) as f64 / tot as f64) } else { "0".into() };

                rows.push(vec![
                    Cell::Str(col.clone()),
                    cnt, distinct,
                    Cell::Str(null_pct),
                    mn, mx,
                ]);
            }
        }
    }

    // Build table
    let names = vec!["column", "count", "distinct", "null%", "min", "max"]
        .into_iter().map(String::from).collect();
    let types = vec![
        ColType::Str, ColType::Int, ColType::Int,
        ColType::Str, ColType::Str, ColType::Str,
    ];
    Ok(Box::new(SimpleTable::new(names, types, rows)))
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
