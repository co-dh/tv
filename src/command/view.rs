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
        let current_view = app.require_view()?;
        let parent_id = current_view.id;

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
        let id = app.next_id();
        let new_view = ViewState::new_frequency(
            id,
            view_name,
            result,
            parent_id,
            self.col_name.clone(),
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

/// Metadata view command - shows column types and statistics (data profile)
pub struct Metadata;

impl Command for Metadata {
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
        let current_view = app.require_view()?;
        let parent_id = current_view.id;
        let df = &current_view.dataframe;
        let total_rows = df.height() as f64;

        // Build metadata dataframe with data profiling
        let col_names: Vec<String> = df.get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let col_types: Vec<String> = df.dtypes()
            .iter()
            .map(|dt| format!("{:?}", dt))
            .collect();

        let mut null_pcts: Vec<f64> = Vec::new();
        let mut distincts: Vec<u32> = Vec::new();
        let mut mins: Vec<String> = Vec::new();
        let mut maxs: Vec<String> = Vec::new();
        let mut medians: Vec<String> = Vec::new();
        let mut sigmas: Vec<String> = Vec::new();

        for col in df.get_columns() {
            let series = col.as_materialized_series();

            // Null percentage
            let null_count = series.null_count() as f64;
            null_pcts.push(100.0 * null_count / total_rows);

            // Distinct count
            let unique_count = series.n_unique().unwrap_or(0) as u32;
            distincts.push(unique_count);

            // Min/Max - format value cleanly without type info
            let min_str = format_scalar_value(series.min_reduce().ok());
            let max_str = format_scalar_value(series.max_reduce().ok());
            mins.push(min_str);
            maxs.push(max_str);

            // Median and Sigma for numeric columns
            let is_numeric = matches!(
                series.dtype(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                DataType::Float32 | DataType::Float64
            );

            if is_numeric {
                let median = series.median().map(|v| format!("{:.2}", v)).unwrap_or("-".to_string());
                let sigma = series.std(1).map(|v| format!("{:.2}", v)).unwrap_or("-".to_string());
                medians.push(median);
                sigmas.push(sigma);
            } else {
                medians.push("-".to_string());
                sigmas.push("-".to_string());
            }
        }

        let metadata_df = DataFrame::new(vec![
            Series::new("column".into(), col_names).into(),
            Series::new("type".into(), col_types).into(),
            Series::new("null%".into(), null_pcts).into(),
            Series::new("distinct".into(), distincts).into(),
            Series::new("min".into(), mins).into(),
            Series::new("max".into(), maxs).into(),
            Series::new("median".into(), medians).into(),
            Series::new("sigma".into(), sigmas).into(),
        ])?;

        let id = app.next_id();
        let mut new_view = ViewState::new(
            id,
            String::from("metadata"),
            metadata_df,
            None,
        );
        new_view.parent_id = Some(parent_id);

        app.stack.push(new_view);
        app.set_message("Data profile".to_string());
        Ok(())
    }

    fn to_command_string(&self) -> String {
        String::from("meta")
    }

    fn should_record(&self) -> bool {
        false
    }
}

/// Format a scalar value cleanly without type information
fn format_scalar_value(scalar: Option<polars::prelude::Scalar>) -> String {
    match scalar {
        Some(s) => {
            let av = s.value();
            match av {
                AnyValue::Null => "-".to_string(),
                AnyValue::Int8(v) => v.to_string(),
                AnyValue::Int16(v) => v.to_string(),
                AnyValue::Int32(v) => v.to_string(),
                AnyValue::Int64(v) => v.to_string(),
                AnyValue::UInt8(v) => v.to_string(),
                AnyValue::UInt16(v) => v.to_string(),
                AnyValue::UInt32(v) => v.to_string(),
                AnyValue::UInt64(v) => v.to_string(),
                AnyValue::Float32(v) => format!("{:.2}", v),
                AnyValue::Float64(v) => format!("{:.2}", v),
                AnyValue::String(v) => v.to_string(),
                AnyValue::Boolean(v) => v.to_string(),
                _ => format!("{:?}", av),
            }
        }
        None => "-".to_string(),
    }
}

/// Correlation matrix for numeric columns
pub struct Correlation {
    /// Selected column indices (if empty, use all numeric columns)
    pub selected_cols: Vec<usize>,
}

impl Command for Correlation {
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
        let current_view = app.require_view()?;

        let df = &current_view.dataframe;
        let all_col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();

        // Get columns to correlate: selected columns (if any and numeric) or all numeric
        let numeric_cols: Vec<String> = if self.selected_cols.len() >= 2 {
            // Use selected columns, but only numeric ones
            self.selected_cols.iter()
                .filter_map(|&idx| {
                    if idx < df.width() {
                        let col = &df.get_columns()[idx];
                        if matches!(
                            col.dtype(),
                            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                            DataType::Float32 | DataType::Float64
                        ) {
                            Some(all_col_names[idx].clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            // Use all numeric columns
            df.get_columns()
                .iter()
                .filter(|col| {
                    matches!(
                        col.dtype(),
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                        DataType::Float32 | DataType::Float64
                    )
                })
                .map(|col| col.name().to_string())
                .collect()
        };

        if numeric_cols.is_empty() {
            return Err(anyhow!("No numeric columns found"));
        }
        if numeric_cols.len() < 2 {
            return Err(anyhow!("Need at least 2 numeric columns for correlation"));
        }

        // Build correlation matrix
        let n = numeric_cols.len();
        let mut corr_data: Vec<Vec<f64>> = vec![vec![0.0; n]; n];

        // Convert columns to f64 for correlation calculation
        let mut float_cols: Vec<Vec<f64>> = Vec::new();
        for col_name in &numeric_cols {
            let col = df.column(col_name)?;
            let series = col.as_materialized_series();
            let f64_series = series.cast(&DataType::Float64)?;
            let values: Vec<f64> = f64_series.f64()?
                .into_iter()
                .map(|v| v.unwrap_or(f64::NAN))
                .collect();
            float_cols.push(values);
        }

        // Calculate correlations
        for i in 0..n {
            for j in 0..n {
                if i == j {
                    corr_data[i][j] = 1.0;
                } else if j > i {
                    let corr = pearson_correlation(&float_cols[i], &float_cols[j]);
                    corr_data[i][j] = corr;
                    corr_data[j][i] = corr;
                }
            }
        }

        // Build DataFrame with column names as first column
        let mut columns: Vec<Column> = vec![
            Series::new("column".into(), numeric_cols.clone()).into()
        ];

        for (i, col_name) in numeric_cols.iter().enumerate() {
            let values: Vec<f64> = corr_data.iter().map(|row| row[i]).collect();
            columns.push(Series::new(col_name.clone().into(), values).into());
        }

        let corr_df = DataFrame::new(columns)?;

        let id = app.next_id();
        let new_view = ViewState::new(
            id,
            String::from("correlation"),
            corr_df,
            None,
        );

        app.stack.push(new_view);
        app.set_message(format!("Correlation matrix ({} columns)", n));
        Ok(())
    }

    fn to_command_string(&self) -> String {
        String::from("corr")
    }

    fn should_record(&self) -> bool {
        false
    }
}

/// Calculate Pearson correlation coefficient
fn pearson_correlation(x: &[f64], y: &[f64]) -> f64 {
    let n = x.len();
    if n == 0 || n != y.len() {
        return f64::NAN;
    }

    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;
    let mut sum_y2 = 0.0;
    let mut count = 0.0;

    for i in 0..n {
        let xi = x[i];
        let yi = y[i];
        if xi.is_nan() || yi.is_nan() {
            continue;
        }
        sum_x += xi;
        sum_y += yi;
        sum_xy += xi * yi;
        sum_x2 += xi * xi;
        sum_y2 += yi * yi;
        count += 1.0;
    }

    if count < 2.0 {
        return f64::NAN;
    }

    let numerator = count * sum_xy - sum_x * sum_y;
    let denominator = ((count * sum_x2 - sum_x * sum_x) * (count * sum_y2 - sum_y * sum_y)).sqrt();

    if denominator == 0.0 {
        f64::NAN
    } else {
        numerator / denominator
    }
}
