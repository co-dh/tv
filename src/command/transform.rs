use crate::app::AppContext;
use crate::command::Command;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Delete column command
pub struct DelCol {
    pub col_name: String,
}

impl Command for DelCol {
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app
            .current_view_mut()
            .ok_or_else(|| anyhow!("No table loaded"))?;

        // Check if column exists
        let found = view.dataframe.get_column_names()
            .iter()
            .any(|c| c.as_str() == self.col_name.as_str());
        if !found {
            return Err(anyhow!("Column '{}' not found", self.col_name));
        }

        // Drop the column
        view.dataframe = view.dataframe.drop(&self.col_name)?;

        // Adjust cursor if needed
        let max_cols = view.col_count();
        if max_cols > 0 && view.state.cc >= max_cols {
            view.state.cc = max_cols - 1;
        }

        app.set_message(format!("Deleted column '{}'", self.col_name));
        Ok(())
    }

    fn to_command_string(&self) -> String {
        format!("delcol {}", self.col_name)
    }
}

/// Filter rows command
pub struct Filter {
    pub expression: String,
}

impl Command for Filter {
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app
            .current_view_mut()
            .ok_or_else(|| anyhow!("No table loaded"))?;

        // Parse the filter expression
        // For MVP, we support simple filters like "col>value", "col<value", "col==value"
        let parts: Vec<&str> = if self.expression.contains(">=") {
            self.expression.splitn(2, ">=").collect()
        } else if self.expression.contains("<=") {
            self.expression.splitn(2, "<=").collect()
        } else if self.expression.contains("==") {
            self.expression.splitn(2, "==").collect()
        } else if self.expression.contains('>') {
            self.expression.splitn(2, '>').collect()
        } else if self.expression.contains('<') {
            self.expression.splitn(2, '<').collect()
        } else {
            return Err(anyhow!("Invalid filter expression. Use: col>value, col<value, col==value, col>=value, or col<=value"));
        };

        if parts.len() != 2 {
            return Err(anyhow!("Invalid filter expression"));
        }

        let col_name = parts[0].trim();
        let value_str = parts[1].trim();

        // Get the column
        let col = view
            .dataframe
            .column(col_name)
            .map_err(|_| anyhow!("Column '{}' not found", col_name))?;
        let series = col.as_materialized_series();

        // Create filter mask based on column type and operator
        let mask = match series.dtype() {
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                let value = value_str
                    .parse::<i64>()
                    .map_err(|_| anyhow!("Invalid integer value"))?;
                self.create_int_mask(series, value)?
            }
            DataType::Float64 | DataType::Float32 => {
                let value = value_str
                    .parse::<f64>()
                    .map_err(|_| anyhow!("Invalid float value"))?;
                self.create_float_mask(series, value)?
            }
            DataType::String => self.create_string_mask(series, value_str)?,
            _ => {
                return Err(anyhow!(
                    "Filtering not supported for column type: {:?}",
                    series.dtype()
                ))
            }
        };

        // Apply filter
        view.dataframe = view.dataframe.filter(&mask)?;

        // Reset cursor to top
        view.state.goto_top();

        let row_count = view.row_count();
        app.set_message(format!("Filtered: {} ({} rows)", self.expression, row_count));
        Ok(())
    }

    fn to_command_string(&self) -> String {
        format!("filter {}", self.expression)
    }
}

impl Filter {
    fn create_int_mask(&self, col: &Series, value: i64) -> Result<ChunkedArray<BooleanType>> {
        let col_i64 = col.cast(&DataType::Int64)?;
        let col_i64 = col_i64.i64()?;

        let mask = if self.expression.contains(">=") {
            col_i64.gt_eq(value)
        } else if self.expression.contains("<=") {
            col_i64.lt_eq(value)
        } else if self.expression.contains("==") {
            col_i64.equal(value)
        } else if self.expression.contains('>') {
            col_i64.gt(value)
        } else {
            col_i64.lt(value)
        };

        Ok(mask)
    }

    fn create_float_mask(&self, col: &Series, value: f64) -> Result<ChunkedArray<BooleanType>> {
        let col_f64 = col.cast(&DataType::Float64)?;
        let col_f64 = col_f64.f64()?;

        let mask = if self.expression.contains(">=") {
            col_f64.gt_eq(value)
        } else if self.expression.contains("<=") {
            col_f64.lt_eq(value)
        } else if self.expression.contains("==") {
            col_f64.equal(value)
        } else if self.expression.contains('>') {
            col_f64.gt(value)
        } else {
            col_f64.lt(value)
        };

        Ok(mask)
    }

    fn create_string_mask(&self, col: &Series, value: &str) -> Result<ChunkedArray<BooleanType>> {
        let col_str = col.str()?;

        let mask = if self.expression.contains("==") {
            col_str.equal(value)
        } else {
            return Err(anyhow!(
                "Only == operator supported for string columns"
            ));
        };

        Ok(mask)
    }
}

/// Select columns command
pub struct Select {
    pub col_names: Vec<String>,
}

impl Command for Select {
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app
            .current_view_mut()
            .ok_or_else(|| anyhow!("No table loaded"))?;

        // Check if all columns exist
        let df_cols = view.dataframe.get_column_names();
        for col_name in &self.col_names {
            let found = df_cols.iter().any(|c| c.as_str() == col_name.as_str());
            if !found {
                return Err(anyhow!("Column '{}' not found", col_name));
            }
        }

        // Select columns
        let col_refs: Vec<&str> = self.col_names.iter().map(|s| s.as_str()).collect();
        view.dataframe = view.dataframe.select(col_refs)?;

        // Reset cursor
        view.state.cc = 0;

        app.set_message(format!("Selected {} columns", self.col_names.len()));
        Ok(())
    }

    fn to_command_string(&self) -> String {
        format!("sel {}", self.col_names.join(","))
    }
}
