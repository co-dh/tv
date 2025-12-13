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
        let parent_rows = view.dataframe.height();
        let parent_name = view.name.clone();
        let col_names: Vec<String> = view.dataframe.get_column_names().iter().map(|s| s.to_string()).collect();

        // Check if column exists
        if !col_names.contains(&self.col_name) {
            return Err(anyhow!("Column '{}' not found", self.col_name));
        }

        // Get key columns (if any)
        let key_cols: Vec<String> = view.col_separator.map(|sep| {
            col_names[..sep].iter().filter(|c| *c != &self.col_name).cloned().collect()
        }).unwrap_or_default();

        // Group by key columns + target column, or just value_counts
        let result = if key_cols.is_empty() {
            // Simple value_counts
            let col = view.dataframe.column(&self.col_name)?;
            let series = col.as_materialized_series();
            let value_counts = series.value_counts(true, false, "Cnt".into(), false)?;
            add_freq_cols(value_counts)?
        } else {
            // Group by key columns + target column
            let mut group_cols: Vec<&str> = key_cols.iter().map(|s| s.as_str()).collect();
            group_cols.push(&self.col_name);
            let grouped = view.dataframe.clone().lazy()
                .group_by(group_cols.iter().map(|&s| col(s)).collect::<Vec<_>>())
                .agg([len().alias("Cnt")])
                .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
                .collect()?;
            add_freq_cols(grouped)?
        };

        // Create new view with frequency table
        let id = app.next_id();
        let mut new_view = ViewState::new_freq(
            id,
            format!("Freq:{}", self.col_name),
            result,
            parent_id,
            parent_rows,
            parent_name,
            self.col_name.clone(),
        );
        // Set separator after key columns in freq view
        if !key_cols.is_empty() {
            new_view.col_separator = Some(key_cols.len());
        }
        app.stack.push(new_view);
        Ok(())
    }

    fn to_str(&self) -> String { format!("freq {}", self.col_name) }
}

/// Add Pct and Bar columns to frequency dataframe
fn add_freq_cols(mut df: DataFrame) -> Result<DataFrame> {
    let cnt_col = df.column("Cnt")?.as_materialized_series();
    let total: u32 = cnt_col.sum().unwrap_or(0);
    let counts: Vec<u32> = cnt_col.u32()
        .map(|ca| ca.into_iter().map(|v| v.unwrap_or(0)).collect())
        .unwrap_or_default();
    let pcts: Vec<f64> = counts.iter().map(|&c| 100.0 * c as f64 / total as f64).collect();
    let bars: Vec<String> = pcts.iter().map(|&p| "#".repeat(p.floor() as usize)).collect();
    df.with_column(Series::new("Pct".into(), pcts))?;
    df.with_column(Series::new("Bar".into(), bars))?;
    Ok(df)
}

/// Metadata view command - shows column types and statistics (data profile)
/// Uses cache if available, otherwise computes stats in background
pub struct Metadata;

const BG_THRESHOLD: usize = 10_000;  // compute in background if rows > this

impl Command for Metadata {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Extract values before mutable borrow
        let (parent_id, parent_col, parent_rows, parent_name, cached, df, col_sep) = {
            let view = app.req()?;
            (view.id, view.state.cc, view.dataframe.height(), view.name.clone(),
             view.meta_cache.clone(), view.dataframe.clone(), view.col_separator)
        };

        // Get key columns if any
        let col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        let key_cols: Vec<String> = col_sep.map(|sep| col_names[..sep].to_vec()).unwrap_or_default();

        // Check for cached meta (only use if no key columns - grouped stats aren't cached)
        if key_cols.is_empty() {
            if let Some(cached_df) = cached {
                let id = app.next_id();
                let mut new_view = ViewState::new_child(id, "metadata".into(), cached_df, parent_id, parent_rows, parent_name.clone());
                new_view.state.cr = parent_col;
                app.stack.push(new_view);
                return Ok(());
            }
        }

        // Small datasets: compute synchronously; large: use background
        if parent_rows <= BG_THRESHOLD {
            let meta_df = if key_cols.is_empty() {
                compute_meta_stats(&df)?
            } else {
                compute_meta_stats_grouped(&df, &key_cols)?
            };
            // Cache in parent (only for non-grouped)
            if key_cols.is_empty() {
                if let Some(parent) = app.stack.find_mut(parent_id) {
                    parent.meta_cache = Some(meta_df.clone());
                }
            }
            let id = app.next_id();
            let mut new_view = ViewState::new_child(id, "metadata".into(), meta_df, parent_id, parent_rows, parent_name);
            new_view.state.cr = parent_col;
            if !key_cols.is_empty() {
                new_view.col_separator = Some(key_cols.len());
            }
            app.stack.push(new_view);
        } else {
            // Large dataset: show placeholder, compute in background
            let col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
            let col_types: Vec<String> = df.dtypes().iter().map(|dt| format!("{:?}", dt)).collect();
            let n = col_names.len();

            let placeholder_df = DataFrame::new(vec![
                Series::new("column".into(), col_names).into(),
                Series::new("type".into(), col_types).into(),
                Series::new("null%".into(), vec!["...".to_string(); n]).into(),
                Series::new("distinct".into(), vec!["...".to_string(); n]).into(),
                Series::new("min".into(), vec!["...".to_string(); n]).into(),
                Series::new("max".into(), vec!["...".to_string(); n]).into(),
                Series::new("median".into(), vec!["...".to_string(); n]).into(),
                Series::new("sigma".into(), vec!["...".to_string(); n]).into(),
            ])?;

            let id = app.next_id();
            let mut new_view = ViewState::new_child(id, "metadata".into(), placeholder_df, parent_id, parent_rows, parent_name);
            new_view.state.cr = parent_col;
            app.stack.push(new_view);

            // Spawn background thread
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                if let Ok(meta_df) = compute_meta_stats(&df) {
                    let _ = tx.send(meta_df);
                }
            });
            app.bg_meta = Some((parent_id, rx));
        }

        Ok(())
    }

    fn to_str(&self) -> String { "meta".into() }
}

/// Compute full metadata stats (runs in background)
fn compute_meta_stats(df: &DataFrame) -> Result<DataFrame> {
    let col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let dtypes = df.dtypes();
    let col_types: Vec<String> = dtypes.iter().map(|dt| format!("{:?}", dt)).collect();
    let total_rows = df.height() as f64;

    let is_numeric: Vec<bool> = dtypes.iter().map(|dt| matches!(dt,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float32 | DataType::Float64
    )).collect();

    let lazy = df.clone().lazy();

    // Build and execute expressions
    let null_exprs: Vec<_> = col_names.iter().map(|c| col(c).null_count().alias(c)).collect();
    let min_exprs: Vec<_> = col_names.iter().map(|c| col(c).min().alias(c)).collect();
    let max_exprs: Vec<_> = col_names.iter().map(|c| col(c).max().alias(c)).collect();
    let distinct_exprs: Vec<_> = col_names.iter().map(|c| col(c).n_unique().alias(c)).collect();

    let numeric_cols: Vec<&String> = col_names.iter().zip(&is_numeric)
        .filter(|(_, &is_num)| is_num).map(|(c, _)| c).collect();
    let mean_exprs: Vec<_> = numeric_cols.iter().map(|c| col(*c).mean().alias(*c)).collect();
    let std_exprs: Vec<_> = numeric_cols.iter().map(|c| col(*c).std(1).alias(*c)).collect();

    let null_df = lazy.clone().select(null_exprs).collect()?;
    let min_df = lazy.clone().select(min_exprs).collect()?;
    let max_df = lazy.clone().select(max_exprs).collect()?;
    let distinct_df = lazy.clone().select(distinct_exprs).collect()?;
    let mean_df = if !mean_exprs.is_empty() { Some(lazy.clone().select(mean_exprs).collect()?) } else { None };
    let std_df = if !std_exprs.is_empty() { Some(lazy.select(std_exprs).collect()?) } else { None };

    let mut null_pcts = Vec::new();
    let mut mins = Vec::new();
    let mut maxs = Vec::new();
    let mut medians = Vec::new();
    let mut sigmas = Vec::new();
    let mut distincts = Vec::new();

    for (i, name) in col_names.iter().enumerate() {
        let null_count = null_df.column(name).ok()
            .and_then(|c| c.get(0).ok())
            .map(|v| v.try_extract::<u32>().unwrap_or(0) as f64)
            .unwrap_or(0.0);
        null_pcts.push(format!("{:.1}", 100.0 * null_count / total_rows));

        mins.push(min_df.column(name).ok()
            .and_then(|c| c.get(0).ok())
            .map(|v| format_anyvalue(&v))
            .unwrap_or_default());

        maxs.push(max_df.column(name).ok()
            .and_then(|c| c.get(0).ok())
            .map(|v| format_anyvalue(&v))
            .unwrap_or_default());

        distincts.push(distinct_df.column(name).ok()
            .and_then(|c| c.get(0).ok())
            .map(|v| format!("{}", v.try_extract::<u32>().unwrap_or(0)))
            .unwrap_or_default());

        if is_numeric[i] {
            medians.push(mean_df.as_ref()
                .and_then(|df| df.column(name).ok())
                .and_then(|c| c.get(0).ok())
                .map(|v| format_anyvalue(&v))
                .unwrap_or_default());
            sigmas.push(std_df.as_ref()
                .and_then(|df| df.column(name).ok())
                .and_then(|c| c.get(0).ok())
                .map(|v| format_anyvalue(&v))
                .unwrap_or_default());
        } else {
            medians.push(String::new());
            sigmas.push(String::new());
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("column".into(), col_names).into(),
        Series::new("type".into(), col_types).into(),
        Series::new("null%".into(), null_pcts).into(),
        Series::new("distinct".into(), distincts).into(),
        Series::new("min".into(), mins).into(),
        Series::new("max".into(), maxs).into(),
        Series::new("median".into(), medians).into(),
        Series::new("sigma".into(), sigmas).into(),
    ])?)
}

/// Compute metadata stats grouped by key columns
fn compute_meta_stats_grouped(df: &DataFrame, key_cols: &[String]) -> Result<DataFrame> {
    let all_cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let non_key_cols: Vec<&String> = all_cols.iter().filter(|c| !key_cols.contains(c)).collect();

    // Get unique combinations of key values
    let key_exprs: Vec<_> = key_cols.iter().map(|c| col(c)).collect();
    let unique_keys = df.clone().lazy()
        .select(key_exprs.clone())
        .unique(None, UniqueKeepStrategy::First)
        .sort(key_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(), SortMultipleOptions::default())
        .collect()?;

    // Build result vectors
    let mut result_cols: Vec<Column> = Vec::new();

    // Add key columns to result
    for key_col in key_cols {
        let mut key_vals: Vec<String> = Vec::new();
        for row in 0..unique_keys.height() {
            for _ in &non_key_cols {
                let v = unique_keys.column(key_col)?.get(row)?;
                key_vals.push(format_anyvalue(&v));
            }
        }
        result_cols.push(Series::new(key_col.as_str().into(), key_vals).into());
    }

    // Add stats columns
    let mut col_names_out: Vec<String> = Vec::new();
    let mut types_out: Vec<String> = Vec::new();
    let mut null_pcts: Vec<String> = Vec::new();
    let mut distincts: Vec<String> = Vec::new();
    let mut mins: Vec<String> = Vec::new();
    let mut maxs: Vec<String> = Vec::new();
    let mut medians: Vec<String> = Vec::new();
    let mut sigmas: Vec<String> = Vec::new();

    for row in 0..unique_keys.height() {
        // Build filter for this key combination
        let mut filter_expr = lit(true);
        for key_col in key_cols {
            let v = unique_keys.column(key_col)?.get(row)?;
            let scalar = Scalar::new(unique_keys.column(key_col)?.dtype().clone(), v.into_static());
            filter_expr = filter_expr.and(col(key_col).eq(lit(scalar)));
        }

        // Filter df to this key group
        let group_df = df.clone().lazy().filter(filter_expr).collect()?;
        let group_rows = group_df.height() as f64;

        // Compute stats for each non-key column
        for &col_name in &non_key_cols {
            col_names_out.push(col_name.clone());

            let dtype = group_df.column(col_name)?.dtype().clone();
            types_out.push(format!("{:?}", dtype));

            let is_numeric = matches!(dtype,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                DataType::Float32 | DataType::Float64);

            // Null count
            let null_count = group_df.column(col_name)?.null_count() as f64;
            null_pcts.push(format!("{:.1}", 100.0 * null_count / group_rows));

            // Distinct
            let distinct = group_df.column(col_name)?.n_unique()? as u32;
            distincts.push(format!("{}", distinct));

            // Min/Max
            let lazy = group_df.clone().lazy();
            let min_df = lazy.clone().select([col(col_name).min()]).collect()?;
            let max_df = lazy.clone().select([col(col_name).max()]).collect()?;
            mins.push(min_df.column(col_name).ok().and_then(|c| c.get(0).ok())
                .map(|v| format_anyvalue(&v)).unwrap_or_default());
            maxs.push(max_df.column(col_name).ok().and_then(|c| c.get(0).ok())
                .map(|v| format_anyvalue(&v)).unwrap_or_default());

            // Mean/Std (numeric only)
            if is_numeric {
                let mean_df = lazy.clone().select([col(col_name).mean()]).collect()?;
                let std_df = lazy.select([col(col_name).std(1)]).collect()?;
                medians.push(mean_df.column(col_name).ok().and_then(|c| c.get(0).ok())
                    .map(|v| format_anyvalue(&v)).unwrap_or_default());
                sigmas.push(std_df.column(col_name).ok().and_then(|c| c.get(0).ok())
                    .map(|v| format_anyvalue(&v)).unwrap_or_default());
            } else {
                medians.push(String::new());
                sigmas.push(String::new());
            }
        }
    }

    result_cols.push(Series::new("column".into(), col_names_out).into());
    result_cols.push(Series::new("type".into(), types_out).into());
    result_cols.push(Series::new("null%".into(), null_pcts).into());
    result_cols.push(Series::new("distinct".into(), distincts).into());
    result_cols.push(Series::new("min".into(), mins).into());
    result_cols.push(Series::new("max".into(), maxs).into());
    result_cols.push(Series::new("median".into(), medians).into());
    result_cols.push(Series::new("sigma".into(), sigmas).into());

    Ok(DataFrame::new(result_cols)?)
}

/// Format AnyValue for display
fn format_anyvalue(v: &AnyValue) -> String {
    match v {
        AnyValue::Null => String::new(),
        AnyValue::Float64(f) => format!("{:.2}", f),
        AnyValue::Float32(f) => format!("{:.2}", f),
        _ => {
            let s = v.to_string();
            if s == "null" { String::new() } else { s.trim_matches('"').to_string() }
        }
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

/// List directory
pub struct Ls { pub dir: std::path::PathBuf }

impl Command for Ls {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::ls(&self.dir)?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, format!("ls:{}", self.dir.display()), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { format!("ls {}", self.dir.display()) }
}

/// List directory recursively
pub struct Lr { pub dir: std::path::PathBuf }

impl Command for Lr {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::lr(&self.dir)?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, format!("lr:{}", self.dir.display()), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { format!("lr {}", self.dir.display()) }
}

/// Process list
pub struct Ps;

impl Command for Ps {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::ps()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "ps".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "ps".into() }
}

/// Disk usage
pub struct Df;

impl Command for Df {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::df()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "df".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "df".into() }
}

/// Mount points
pub struct Mounts;

impl Command for Mounts {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::mounts()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "mounts".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "mounts".into() }
}

/// TCP connections
pub struct Tcp;

impl Command for Tcp {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::tcp()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "tcp".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "tcp".into() }
}

/// UDP connections
pub struct Udp;

impl Command for Udp {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::udp()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "udp".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "udp".into() }
}

/// Block devices
pub struct Lsblk;

impl Command for Lsblk {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::lsblk()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "lsblk".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "lsblk".into() }
}

/// Logged in users
pub struct Who;

impl Command for Who {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::who()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "who".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "who".into() }
}

/// Open files
pub struct Lsof { pub pid: Option<i32> }

impl Command for Lsof {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::lsof(self.pid)?;
        let id = app.next_id();
        let name = self.pid.map(|p| format!("lsof:{}", p)).unwrap_or("lsof".into());
        app.stack.push(ViewState::new(id, name, df, None));
        Ok(())
    }
    fn to_str(&self) -> String { self.pid.map(|p| format!("lsof {}", p)).unwrap_or("lsof".into()) }
}

/// Environment variables
pub struct Env;

impl Command for Env {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::env()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "env".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "env".into() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_focuses_parent_column() {
        let mut app = AppContext::new();
        let df = df! {
            "a" => &[1, 2, 3],
            "b" => &[4, 5, 6],
            "c" => &[7, 8, 9],
            "d" => &[10, 11, 12],
        }.unwrap();

        let id = app.next_id();
        app.stack.push(ViewState::new(id, "test".into(), df, None));

        // Set parent cursor to column 2 (column "c")
        app.view_mut().unwrap().state.cc = 2;

        // Execute Metadata command
        Metadata.exec(&mut app).unwrap();

        // Meta view should focus on row 2 (corresponding to column "c")
        let meta_view = app.view().unwrap();
        assert_eq!(meta_view.name, "metadata");
        assert_eq!(meta_view.state.cr, 2);
    }

    #[test]
    fn test_meta_null_percent_format() {
        // Test that compute_meta_stats produces null% values matchable with get_str()
        let df = df! {
            "all_null" => &[None::<i32>, None, None],
            "some_null" => &[Some(1), None, Some(3)],
            "no_null" => &[1, 2, 3],
        }.unwrap();

        let meta_df = compute_meta_stats(&df).unwrap();
        let null_col = meta_df.column("null%").unwrap();
        let s = null_col.as_materialized_series();

        // all_null should be "100.0"
        let v0 = s.get(0).unwrap();
        assert_eq!(v0.get_str().unwrap(), "100.0", "all_null should be 100.0%");

        // no_null should be "0.0"
        let v2 = s.get(2).unwrap();
        assert_eq!(v2.get_str().unwrap(), "0.0", "no_null should be 0.0%");
    }

    #[test]
    fn test_meta_distinct_format() {
        // Test that compute_meta_stats produces distinct values matchable with get_str()
        let df = df! {
            "single_val" => &[1, 1, 1],
            "two_vals" => &[1, 2, 1],
            "all_diff" => &[1, 2, 3],
        }.unwrap();

        let meta_df = compute_meta_stats(&df).unwrap();
        let distinct_col = meta_df.column("distinct").unwrap();
        let s = distinct_col.as_materialized_series();

        // single_val should have distinct = "1"
        let v0 = s.get(0).unwrap();
        assert_eq!(v0.get_str().unwrap(), "1", "single_val should have 1 distinct");

        // two_vals should have distinct = "2"
        let v1 = s.get(1).unwrap();
        assert_eq!(v1.get_str().unwrap(), "2", "two_vals should have 2 distinct");

        // all_diff should have distinct = "3"
        let v2 = s.get(2).unwrap();
        assert_eq!(v2.get_str().unwrap(), "3", "all_diff should have 3 distinct");
    }
}

