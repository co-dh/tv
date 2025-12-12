use crate::app::AppContext;
use crate::command::Command;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Frequency table command - shows value counts for a column
pub struct Frequency {
    pub col_name: String,
}

impl Command for Frequency {
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
        let current_view = app
            .current_view()
            .ok_or_else(|| anyhow!("No table loaded"))?;

        // Check if column exists
        let found = current_view.dataframe.get_column_names()
            .iter()
            .any(|c| c.as_str() == self.col_name.as_str());
        if !found {
            return Err(anyhow!("Column '{}' not found", self.col_name));
        }

        // Get the column and compute value counts (sorted descending)
        let col = current_view.dataframe.column(&self.col_name)?;
        let series = col.as_materialized_series();
        let value_counts = series.value_counts(true, false, "Cnt".into(), false)?;

        // Calculate total count for percentage
        let cnt_col = value_counts.column("Cnt")?.as_materialized_series();
        let total: u32 = cnt_col.sum().unwrap_or(0);

        // Calculate percentage and bar
        let counts: Vec<u32> = cnt_col
            .u32()
            .map(|ca| ca.into_iter().map(|v| v.unwrap_or(0)).collect())
            .unwrap_or_default();

        let pcts: Vec<f64> = counts.iter().map(|&c| 100.0 * c as f64 / total as f64).collect();
        let bars: Vec<String> = pcts.iter().map(|&p| "#".repeat(p.floor() as usize)).collect();

        // Add Pct and Bar columns
        let pct_series = Series::new("Pct".into(), pcts);
        let bar_series = Series::new("Bar".into(), bars);

        let mut result = value_counts.clone();
        result.with_column(pct_series)?;
        result.with_column(bar_series)?;

        // Create new view with frequency table
        let view_name = format!("Freq:{}", self.col_name);
        let new_view = ViewState::new(
            view_name,
            result,
            None,
        );

        app.stack.push(new_view);
        app.set_message(format!("Frequency table for '{}'", self.col_name));
        Ok(())
    }

    fn to_command_string(&self) -> String {
        format!("freq {}", self.col_name)
    }

    fn should_record(&self) -> bool {
        false // View commands don't modify data
    }
}

/// Metadata view command - shows column types and statistics
pub struct Metadata;

impl Command for Metadata {
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
        let current_view = app
            .current_view()
            .ok_or_else(|| anyhow!("No table loaded"))?;

        let df = &current_view.dataframe;

        // Build metadata dataframe
        let col_names: Vec<String> = df.get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let col_types: Vec<String> = df.dtypes()
            .iter()
            .map(|dt| format!("{:?}", dt))
            .collect();

        let null_counts: Vec<u32> = df.get_columns()
            .iter()
            .map(|col| col.as_materialized_series().null_count() as u32)
            .collect();

        let metadata_df = DataFrame::new(vec![
            Series::new("column".into(), col_names).into(),
            Series::new("type".into(), col_types).into(),
            Series::new("nulls".into(), null_counts).into(),
        ])?;

        let new_view = ViewState::new(
            String::from("metadata"),
            metadata_df,
            None,
        );

        app.stack.push(new_view);
        app.set_message("Metadata view".to_string());
        Ok(())
    }

    fn to_command_string(&self) -> String {
        String::from("meta")
    }

    fn should_record(&self) -> bool {
        false
    }
}
