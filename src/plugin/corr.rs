//! Correlation matrix plugin - calculate and display correlation matrix

use crate::app::AppContext;
use crate::backend::is_numeric;
use crate::command::Command;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;

pub struct CorrPlugin;

impl Plugin for CorrPlugin {
    fn name(&self) -> &str { "corr" }
    fn tab(&self) -> &str { "corr" }
    fn matches(&self, name: &str) -> bool { name == "correlation" }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        if cmd != "enter" { return None; }
        // Get column name from current row (first column is row label)
        let col_name = app.view().and_then(|v| {
            v.dataframe.column("column").ok()?.get(v.state.cr).ok()
                .map(|v| v.to_string().trim_matches('"').to_string())
        })?;
        Some(Box::new(CorrEnter { col_name }))
    }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        if cmd != "corr" { return None; }
        // Parse selected columns (comma-separated indices or empty for all)
        let selected_cols = if arg.is_empty() { vec![] } else {
            arg.split(',').filter_map(|s| s.trim().parse().ok()).collect()
        };
        Some(Box::new(Correlation { selected_cols }))
    }
}

/// Correlation matrix for numeric columns
pub struct Correlation {
    pub selected_cols: Vec<usize>,
}

impl Command for Correlation {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let df = &view.dataframe;
        let all_col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();

        // Get columns to correlate: selected columns (if any and numeric) or all numeric
        let numeric_cols: Vec<String> = if self.selected_cols.len() >= 2 {
            self.selected_cols.iter()
                .filter_map(|&idx| {
                    if idx < df.width() {
                        let col = &df.get_columns()[idx];
                        if is_numeric(col.dtype()) { Some(all_col_names[idx].clone()) } else { None }
                    } else { None }
                })
                .collect()
        } else {
            df.get_columns().iter()
                .filter(|col| is_numeric(col.dtype()))
                .map(|col| col.name().to_string())
                .collect()
        };

        if numeric_cols.is_empty() { return Err(anyhow!("No numeric columns found")); }
        if numeric_cols.len() < 2 { return Err(anyhow!("Need at least 2 numeric columns")); }

        // Build correlation matrix using polars
        let n = numeric_cols.len();
        let chunks: Vec<Float64Chunked> = numeric_cols.iter()
            .map(|c| df.column(c).unwrap().as_materialized_series().cast(&DataType::Float64).unwrap().f64().unwrap().clone())
            .collect();

        let mut columns: Vec<Column> = vec![Series::new("column".into(), numeric_cols.clone()).into()];
        for (i, col_name) in numeric_cols.iter().enumerate() {
            let corrs: Vec<f64> = (0..n).map(|j| {
                if i == j { 1.0 } else { polars_ops::chunked_array::cov::pearson_corr(&chunks[i], &chunks[j]).unwrap_or(f64::NAN) }
            }).collect();
            columns.push(Series::new(col_name.clone().into(), corrs).into());
        }
        let corr_df = DataFrame::new(columns)?;

        let id = app.next_id();
        app.stack.push(ViewState::new(id, "correlation".into(), corr_df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "corr".into() }
}


/// Corr Enter: pop view and go to column in parent
pub struct CorrEnter { pub col_name: String }

impl Command for CorrEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        app.stack.pop();
        if let Some(view) = app.view_mut() {
            let idx = view.dataframe.get_column_names().iter()
                .position(|n| n.as_str() == self.col_name);
            if let Some(i) = idx {
                view.state.cc = i;
                view.state.visible();
            }
        }
        Ok(())
    }
    fn to_str(&self) -> String { format!("goto_col {}", self.col_name) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correlation_basic() {
        let mut app = AppContext::new();
        let df = df! {
            "a" => &[1.0, 2.0, 3.0],
            "b" => &[2.0, 4.0, 6.0],
            "c" => &["x", "y", "z"],
        }.unwrap();

        let id = app.next_id();
        app.stack.push(ViewState::new(id, "test".into(), df, None));
        Correlation { selected_cols: vec![] }.exec(&mut app).unwrap();
        let corr = app.view().unwrap();
        assert_eq!(corr.name, "correlation");
        assert_eq!(corr.dataframe.width(), 3);  // column + a + b (c is not numeric)
    }
}
