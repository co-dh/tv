use crate::app::AppContext;
use crate::command::Command;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Correlation matrix for numeric columns
pub struct Correlation {
    /// Selected column indices (if empty, use all numeric columns)
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
                        if matches!(col.dtype(),
                            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                            DataType::Float32 | DataType::Float64
                        ) {
                            Some(all_col_names[idx].clone())
                        } else { None }
                    } else { None }
                })
                .collect()
        } else {
            df.get_columns()
                .iter()
                .filter(|col| matches!(col.dtype(),
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                    DataType::Float32 | DataType::Float64
                ))
                .map(|col| col.name().to_string())
                .collect()
        };

        if numeric_cols.is_empty() { return Err(anyhow!("No numeric columns found")); }
        if numeric_cols.len() < 2 { return Err(anyhow!("Need at least 2 numeric columns for correlation")); }

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

/// Pop view from stack
pub struct Pop;

impl Command for Pop {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        app.stack.pop();
        app.message.clear();
        Ok(())
    }
    fn to_str(&self) -> String { "pop".into() }
}

/// Swap top two views
pub struct Swap;

impl Command for Swap {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.stack.len() >= 2 {
            app.stack.swap();
            Ok(())
        } else {
            Err(anyhow!("Need at least 2 views to swap"))
        }
    }
    fn to_str(&self) -> String { "swap".into() }
    fn record(&self) -> bool { false }
}

/// Duplicate current view
pub struct Dup;

impl Command for Dup {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let mut new_view = view.clone();
        new_view.name = format!("{} (copy)", view.name);
        new_view.id = app.next_id();
        app.stack.push(new_view);
        Ok(())
    }
    fn to_str(&self) -> String { "dup".into() }
    fn record(&self) -> bool { false }
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
