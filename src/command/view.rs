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

        // Get the column and compute value counts
        let col = current_view.dataframe.column(&self.col_name)?;
        let series = col.as_materialized_series();
        let value_counts = series.value_counts(true, false, "count".into(), false)?;

        // Create new view with frequency table
        let view_name = format!("freq:{}", self.col_name);
        let new_view = ViewState::new(
            view_name,
            value_counts,
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
