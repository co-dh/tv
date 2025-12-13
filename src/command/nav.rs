// Navigation and display commands
use super::Command;
use crate::app::AppContext;
use anyhow::Result;

/// Unified row navigation: goto +n/-n/0/max
pub struct Goto { pub arg: String }
impl Command for Goto {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let a = self.arg.trim();
        match a {
            "0" => app.nav_row(isize::MIN),   // top
            "max" => app.nav_row(isize::MAX), // bottom
            _ => {
                // +n/-n or just n
                if let Ok(n) = a.trim_start_matches('+').parse::<isize>() {
                    app.nav_row(n);
                }
            }
        }
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
        match a {
            "0" => if let Some(v) = app.view_mut() { v.state.cc = 0; },
            "max" => if let Some(v) = app.view_mut() { v.state.cc = v.cols().saturating_sub(1); },
            _ => {
                if let Ok(n) = a.trim_start_matches('+').parse::<isize>() {
                    app.nav_col(n);
                }
            }
        }
        Ok(())
    }
    fn to_str(&self) -> String { format!("gotocol {}", self.arg) }
    fn record(&self) -> bool { false }
}

/// Toggle info box display
pub struct ToggleInfo;
impl Command for ToggleInfo {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        app.show_info = !app.show_info;
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
        let msg = if let Some(view) = app.view_mut() {
            let is_meta = view.name == "metadata";
            let is_freq = view.name.starts_with("Freq:");
            if is_meta || is_freq {
                let cr = view.state.cr;
                if view.selected_rows.contains(&cr) {
                    view.selected_rows.remove(&cr);
                    format!("Deselected row ({} selected)", view.selected_rows.len())
                } else {
                    view.selected_rows.insert(cr);
                    format!("Selected row ({} selected)", view.selected_rows.len())
                }
            } else {
                let cc = view.state.cc;
                if view.selected_cols.contains(&cc) {
                    view.selected_cols.remove(&cc);
                    format!("Deselected column ({} selected)", view.selected_cols.len())
                } else {
                    view.selected_cols.insert(cc);
                    format!("Selected column ({} selected)", view.selected_cols.len())
                }
            }
        } else {
            "No view".into()
        };
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
        let msg = if let Some(view) = app.view_mut() {
            let is_meta = view.name == "metadata";
            let is_freq = view.name.starts_with("Freq:");
            if is_meta || is_freq {
                for i in 0..view.dataframe.height() { view.selected_rows.insert(i); }
                format!("Selected all {} row(s)", view.selected_rows.len())
            } else {
                for i in 0..view.dataframe.width() { view.selected_cols.insert(i); }
                format!("Selected all {} column(s)", view.selected_cols.len())
            }
        } else {
            "No view".into()
        };
        app.msg(msg);
        Ok(())
    }
    fn to_str(&self) -> String { "sel_all".into() }
    fn record(&self) -> bool { false }
}

/// Select rows matching filter expression (like filter but selects instead)
pub struct SelRows { pub expr: String }
impl Command for SelRows {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use polars::prelude::*;
        let view = app.req()?;
        let df = &view.dataframe;

        // Compile PRQL filter to SQL WHERE clause
        let where_clause = crate::prql::filter_to_sql(&self.expr)?;

        // Find matching row indices
        let mut ctx = polars::sql::SQLContext::new();
        let with_idx = df.clone().lazy().with_row_index("__idx__", None);
        ctx.register("df", with_idx);
        let matches: Vec<usize> = ctx.execute(&format!("SELECT __idx__ FROM df WHERE {}", where_clause))
            .and_then(|lf| lf.collect())
            .map(|result| {
                result.column("__idx__").ok()
                    .and_then(|c| c.idx().ok())
                    .map(|idx| idx.into_iter().filter_map(|v| v.map(|i| i as usize)).collect())
                    .unwrap_or_default()
            })
            .unwrap_or_default();

        let count = matches.len();
        if let Some(view) = app.view_mut() {
            for idx in matches { view.selected_rows.insert(idx); }
        }
        app.msg(format!("Selected {} row(s)", count));
        Ok(())
    }
    fn to_str(&self) -> String { format!("sel_rows {}", self.expr) }
    fn record(&self) -> bool { false }
}
