// Navigation and display commands
use super::Command;
use crate::app::AppContext;
use anyhow::Result;

/// Unified row navigation: goto +n/-n/0/max
pub struct Goto { pub arg: String }
const BIG: isize = 1_000_000_000;
impl Command for Goto {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let a = self.arg.trim();
        // Parse: 0->-BIG(top), max->BIG(bot), +n/-n as-is
        let n = match a { "0" => -BIG, "max" => BIG, _ => a.trim_start_matches('+').parse().unwrap_or(0) };
        app.nav_row(n);
        Ok(())
    }
    fn to_str(&self) -> String { format!("goto {}", self.arg) }
    fn record(&self) -> bool { false }
}

/// Unified column navigation: gotocol +1/-1/0/max
pub struct GotoCol { pub arg: String }
impl Command for GotoCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let a = self.arg.trim();
        // Parse: 0->-BIG(left), max->BIG(right), +n/-n as-is
        let n = match a { "0" => -BIG, "max" => BIG, _ => a.trim_start_matches('+').parse().unwrap_or(0) };
        app.nav_col(n);
        Ok(())
    }
    fn to_str(&self) -> String { format!("goto_col {}", self.arg) }
    fn record(&self) -> bool { false }
}

/// Toggle info box: 0=off, 1=help, 2=help+prql
pub struct ToggleInfo;
impl Command for ToggleInfo {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        app.info_mode = (app.info_mode + 1) % 3;  // cycle 0→1→2→0
        Ok(())
    }
    fn to_str(&self) -> String { "toggle_info".into() }
    fn record(&self) -> bool { false }
}

/// Adjust decimal places: decimals +1/-1
pub struct Decimals { pub delta: isize }
impl Command for Decimals {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if self.delta > 0 {
            app.float_decimals = (app.float_decimals + self.delta as usize).min(17);
        } else {
            app.float_decimals = app.float_decimals.saturating_sub((-self.delta) as usize);
        }
        if let Some(v) = app.view_mut() { v.state.col_widths.clear(); }
        app.msg(format!("Float decimals: {}", app.float_decimals));
        Ok(())
    }
    fn to_str(&self) -> String { format!("decimals {}", self.delta) }
    fn record(&self) -> bool { false }
}

/// Toggle selection on current row/column
pub struct ToggleSel;
impl Command for ToggleSel {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let msg = if let Some(v) = app.view_mut() {
            if v.is_row_sel() {
                let cr = v.state.cr;
                if v.selected_rows.contains(&cr) { v.selected_rows.remove(&cr); } else { v.selected_rows.insert(cr); }
                format!("{} row(s) selected", v.selected_rows.len())
            } else {
                let cc = v.state.cc;
                if v.selected_cols.contains(&cc) { v.selected_cols.remove(&cc); } else { v.selected_cols.insert(cc); }
                format!("{} column(s) selected", v.selected_cols.len())
            }
        } else { "No view".into() };
        app.msg(msg);
        Ok(())
    }
    fn to_str(&self) -> String { "toggle_sel".into() }
    fn record(&self) -> bool { false }
}

/// Clear all selections
pub struct ClearSel;
impl Command for ClearSel {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if let Some(view) = app.view_mut() {
            view.selected_cols.clear();
            view.selected_rows.clear();
        }
        Ok(())
    }
    fn to_str(&self) -> String { "clear_sel".into() }
    fn record(&self) -> bool { false }
}

/// Select all rows (Meta/Freq view) or columns (table view)
pub struct SelAll;
impl Command for SelAll {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let msg = if let Some(v) = app.view_mut() {
            if v.is_row_sel() {
                v.selected_rows.extend(0..v.rows());
                format!("Selected all {} row(s)", v.selected_rows.len())
            } else {
                v.selected_cols.extend(0..v.cols());
                format!("Selected all {} column(s)", v.selected_cols.len())
            }
        } else { "No view".into() };
        app.msg(msg);
        Ok(())
    }
    fn to_str(&self) -> String { "sel_all".into() }
    fn record(&self) -> bool { false }
}

/// Select rows matching SQL WHERE expression (stub - needs plugin)
pub struct SelRows { pub expr: String }
impl Command for SelRows {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // TODO: implement via plugin for SQL filtering
        app.msg("SelRows not yet implemented without polars");
        Ok(())
    }
    fn to_str(&self) -> String { format!("sel_rows {}", self.expr) }
    fn record(&self) -> bool { false }
}
