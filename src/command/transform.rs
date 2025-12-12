use crate::app::AppContext;
use crate::command::Command;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Delete column command
pub struct DelCol {
    pub col_name: String,
}

impl Command for DelCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req_mut()?;

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
        let max_cols = view.cols();
        if max_cols > 0 && view.state.cc >= max_cols {
            view.state.cc = max_cols - 1;
        }

        app.msg(format!("Deleted column '{}'", self.col_name));
        Ok(())
    }

    fn to_str(&self) -> String { format!("delcol {}", self.col_name) }
}

/// Filter rows command
pub struct Filter {
    pub expression: String,
}

impl Command for Filter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let df = &view.dataframe;

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
        let col = df
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

        // Apply filter to create new dataframe
        let filtered_df = df.filter(&mask)?;
        let filename = view.filename.clone();

        // Push new view onto stack
        let id = app.next_id();
        app.msg(format!("Filtered: {} ({} rows)", self.expression, filtered_df.height()));
        app.stack.push(crate::state::ViewState::new(id, self.expression.clone(), filtered_df, filename));
        Ok(())
    }

    fn to_str(&self) -> String { format!("filter {}", self.expression) }
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

        if !self.expression.contains("==") {
            return Err(anyhow!("Only == operator supported for string columns"));
        }

        // Helper to create mask from predicate
        let make_mask = |pred: fn(&str, &str) -> bool, pattern: &str| -> ChunkedArray<BooleanType> {
            let bools: Vec<bool> = col_str.into_iter()
                .map(|opt| opt.map(|s| pred(s, pattern)).unwrap_or(false))
                .collect();
            ChunkedArray::from_slice("mask".into(), &bools)
        };

        // Support glob patterns: *pattern (ends with), pattern* (begins with), *pattern* (contains)
        let mask = if value.starts_with('*') && value.ends_with('*') && value.len() > 2 {
            make_mask(|s, p| s.contains(p), &value[1..value.len()-1])
        } else if value.starts_with('*') && value.len() > 1 {
            make_mask(|s, p| s.ends_with(p), &value[1..])
        } else if value.ends_with('*') && value.len() > 1 {
            make_mask(|s, p| s.starts_with(p), &value[..value.len()-1])
        } else {
            col_str.equal(value)
        };

        Ok(mask)
    }
}

/// Select columns command
pub struct Select {
    pub col_names: Vec<String>,
}

impl Command for Select {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req_mut()?;

        // Check if all columns exist
        let df_cols = view.dataframe.get_column_names();
        for col_name in &self.col_names {
            let found = df_cols.iter().any(|c| c.as_str() == col_name.as_str());
            if !found {
                return Err(anyhow!("Column '{}' not found", col_name));
            }
        }

        // Select columns
        view.dataframe = view.dataframe.select(self.col_names.iter().map(|s| s.as_str()).collect::<Vec<&str>>())?;

        // Reset cursor
        view.state.cc = 0;

        app.msg(format!("Selected {} columns", self.col_names.len()));
        Ok(())
    }

    fn to_str(&self) -> String { format!("sel {}", self.col_names.join(",")) }
}

/// Sort by column command
pub struct Sort {
    pub col_name: String,
    pub descending: bool,
}

impl Command for Sort {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req_mut()?;

        // Check if column exists
        let found = view.dataframe.get_column_names()
            .iter()
            .any(|c| c.as_str() == self.col_name.as_str());
        if !found {
            return Err(anyhow!("Column '{}' not found", self.col_name));
        }

        // Sort the dataframe
        view.dataframe = view.dataframe.sort(
            [&self.col_name],
            SortMultipleOptions::default().with_order_descending(self.descending)
        )?;

        app.msg(format!("Sorted by {} ({})", self.col_name, if self.descending { "desc" } else { "asc" }));
        Ok(())
    }

    fn to_str(&self) -> String {
        format!("{} {}", if self.descending { "sort_desc" } else { "sort_asc" }, self.col_name)
    }
}

/// Rename column command
pub struct RenameCol {
    pub old_name: String,
    pub new_name: String,
}

impl Command for RenameCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req_mut()?;

        // Check if old column exists
        let found = view.dataframe.get_column_names()
            .iter()
            .any(|c| c.as_str() == self.old_name.as_str());
        if !found {
            return Err(anyhow!("Column '{}' not found", self.old_name));
        }

        // Rename the column
        view.dataframe.rename(&self.old_name, self.new_name.as_str().into())?;

        app.msg(format!("Renamed '{}' to '{}'", self.old_name, self.new_name));
        Ok(())
    }

    fn to_str(&self) -> String { format!("rename {} {}", self.old_name, self.new_name) }
}

/// Delete all-null columns command
pub struct DelNull;

impl Command for DelNull {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req_mut()?;
        let null_cols: Vec<String> = view.dataframe.get_columns()
            .iter()
            .filter(|col| col.as_materialized_series().null_count() == view.dataframe.height())
            .map(|col| col.name().to_string())
            .collect();

        if null_cols.is_empty() {
            app.msg("No all-null columns found".to_string());
            return Ok(());
        }

        let count = null_cols.len();
        for col_name in null_cols {
            let _ = view.dataframe.drop_in_place(&col_name);
        }

        // Adjust cursor if needed
        let max_cols = view.cols();
        if max_cols > 0 && view.state.cc >= max_cols {
            view.state.cc = max_cols - 1;
        }
        app.msg(format!("Deleted {} all-null column(s)", count));
        Ok(())
    }

    fn to_str(&self) -> String { "delnull".into() }
}

/// Delete single-value columns
pub struct DelSingle;

impl Command for DelSingle {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req_mut()?;

        let single_cols: Vec<String> = view.dataframe.get_columns()
            .iter()
            .filter(|col| {
                let series = col.as_materialized_series();
                if let Ok(n_unique) = series.n_unique() {
                    let null_count = series.null_count();
                    if null_count > 0 && null_count < series.len() {
                        n_unique <= 2
                    } else {
                        n_unique == 1
                    }
                } else {
                    false
                }
            })
            .map(|col| col.name().to_string())
            .collect();

        if single_cols.is_empty() {
            app.msg("No single-value columns found".to_string());
            return Ok(());
        }

        let count = single_cols.len();
        for col_name in single_cols {
            let _ = view.dataframe.drop_in_place(&col_name);
        }

        // Adjust cursor if needed
        let max_cols = view.cols();
        if max_cols > 0 && view.state.cc >= max_cols {
            view.state.cc = max_cols - 1;
        }
        app.msg(format!("Deleted {} single-value column(s)", count));
        Ok(())
    }

    fn to_str(&self) -> String { "del1".into() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_string_df() -> DataFrame {
        df! {
            "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"]
        }.unwrap()
    }

    #[test]
    fn test_string_filter_exact_match() {
        let df = make_string_df();
        let filter = Filter { expression: "name==apple".to_string() };
        let col = df.column("name").unwrap();
        let mask = filter.create_string_mask(col.as_materialized_series(), "apple").unwrap();
        let result = df.filter(&mask).unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn test_string_filter_contains() {
        let df = make_string_df();
        let filter = Filter { expression: "name==*apple*".to_string() };
        let col = df.column("name").unwrap();
        let mask = filter.create_string_mask(col.as_materialized_series(), "*apple*").unwrap();
        let result = df.filter(&mask).unwrap();
        // "apple" and "pineapple" both contain "apple"
        assert_eq!(result.height(), 2);
    }

    #[test]
    fn test_string_filter_ends_with() {
        let df = make_string_df();
        let filter = Filter { expression: "name==*rry".to_string() };
        let col = df.column("name").unwrap();
        let mask = filter.create_string_mask(col.as_materialized_series(), "*rry").unwrap();
        let result = df.filter(&mask).unwrap();
        // "cherry", "blueberry" end with "rry"
        assert_eq!(result.height(), 2);
    }

    #[test]
    fn test_string_filter_starts_with() {
        let df = make_string_df();
        let filter = Filter { expression: "name==b*".to_string() };
        let col = df.column("name").unwrap();
        let mask = filter.create_string_mask(col.as_materialized_series(), "b*").unwrap();
        let result = df.filter(&mask).unwrap();
        // "banana", "blueberry" start with "b"
        assert_eq!(result.height(), 2);
    }
}
