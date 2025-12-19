//! Freq view plugin - frequency/value counts table
//! Shows value distribution grouped by one or more columns.

use crate::app::AppContext;
use crate::data::table::{Cell, ColType, SimpleTable, BoxTable};
use crate::data::dynload;
use crate::utils::unquote;
use crate::util::pure;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::FilterIn;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::{anyhow, Result};

/// Add Pct and Bar columns to freq result
fn add_pct_bar(t: BoxTable) -> BoxTable {
    let cnt_idx = t.col_names().iter().position(|n| n == "Cnt").unwrap_or(0);
    let total: i64 = (0..t.rows()).filter_map(|r| match t.cell(r, cnt_idx) {
        Cell::Int(n) => Some(n), _ => None
    }).sum();

    let mut names = t.col_names();
    let mut types: Vec<ColType> = (0..t.cols()).map(|c| t.col_type(c)).collect();
    names.push("Pct".into());
    names.push("Bar".into());
    types.push(ColType::Float);
    types.push(ColType::Str);

    let data: Vec<Vec<Cell>> = (0..t.rows()).map(|r| {
        let mut row: Vec<Cell> = (0..t.cols()).map(|c| t.cell(r, c)).collect();
        let cnt = match t.cell(r, cnt_idx) { Cell::Int(n) => n, _ => 0 };
        let pct = if total > 0 { 100.0 * cnt as f64 / total as f64 } else { 0.0 };
        row.push(Cell::Float(pct));
        row.push(Cell::Str("#".repeat(pct.floor() as usize)));
        row
    }).collect();

    Box::new(SimpleTable::new(names, types, data))
}

pub struct FreqPlugin;

impl Plugin for FreqPlugin {
    fn name(&self) -> &str { "freq" }
    fn tab(&self) -> &str { "freq" }

    fn matches(&self, name: &str) -> bool { name.starts_with("Freq:") }

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
            (v.id, v.rows(), v.name.clone(), v.path.clone().unwrap_or_default(),
             v.key_cols(), v.prql.clone())
        };

        // Build PRQL for freq: group by cols, count, sort desc (using parent prql as base)
        let plugin = dynload::get_for(&path).ok_or_else(|| anyhow!("plugin not loaded"))?;
        let grp_cols = self.col_names.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
        let prql = format!("{} | group {{{}}} (aggregate {{Cnt = count this}}) | sort {{-Cnt}}", parent_prql, grp_cols);
        let sql = pure::compile_prql(&prql).ok_or_else(|| anyhow!("prql compile failed"))?;
        let t = plugin.query(&sql, &path).ok_or_else(|| anyhow!("freq query failed"))?;
        let result = add_pct_bar(dynload::to_box_table(&t));

        // Create freq view
        let id = app.next_id();
        let name = format!("Freq:{}", self.col_names.join(","));
        let freq_col = self.col_names.first().cloned().unwrap_or_default();
        let mut new_view = ViewState::new_freq(
            id, name, result, parent_id, parent_rows, parent_name, freq_col, &parent_prql, &self.col_names,
        );
        if !key_cols.is_empty() { new_view.col_separator = Some(key_cols.len()); }
        app.stack.push(new_view);
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
