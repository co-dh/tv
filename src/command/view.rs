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
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let parent_id = view.id;

        // Check if column exists
        let found = view.dataframe.get_column_names()
            .iter()
            .any(|c| c.as_str() == self.col_name.as_str());
        if !found {
            return Err(anyhow!("Column '{}' not found", self.col_name));
        }

        // Get the column and compute value counts (sorted descending)
        let col = view.dataframe.column(&self.col_name)?;
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
        let id = app.next_id();
        app.stack.push(ViewState::new_freq(
            id,
            format!("Freq:{}", self.col_name),
            result,
            parent_id,
            self.col_name.clone(),
        ));
        Ok(())
    }

    fn to_str(&self) -> String { format!("freq {}", self.col_name) }
    fn record(&self) -> bool { false }  // view cmd
}

/// Metadata view command - shows column types and statistics (data profile)
pub struct Metadata;

impl Command for Metadata {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let parent_id = view.id;
        let df = &view.dataframe;
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
        Ok(())
    }

    fn to_str(&self) -> String { "meta".into() }
    fn record(&self) -> bool { false }
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
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;

        let df = &view.dataframe;
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
        let new_view = ViewState::new(
            id,
            String::from("correlation"),
            corr_df,
            None,
        );

        app.stack.push(new_view);
        Ok(())
    }

    fn to_str(&self) -> String { "corr".into() }
    fn record(&self) -> bool { false }
}

