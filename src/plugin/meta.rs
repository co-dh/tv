//! Meta view plugin - data profile/metadata statistics
//! Combines: view detection, command handling, Metadata command

use crate::app::AppContext;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::Xkey;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::Result;
use polars::prelude::*;

pub struct MetaPlugin;

impl Plugin for MetaPlugin {
    fn name(&self) -> &str { "meta" }
    fn tab(&self) -> &str { "meta" }

    fn matches(&self, name: &str) -> bool {
        name == "metadata"
    }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" => {
                let col_names: Vec<String> = app.view().map(|v| {
                    let rows: Vec<usize> = if v.selected_rows.is_empty() {
                        vec![v.state.cr]
                    } else {
                        let mut r: Vec<_> = v.selected_rows.iter().copied().collect();
                        r.sort();
                        r
                    };
                    rows.iter()
                        .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok()
                            .map(|v| v.to_string().trim_matches('"').to_string()))
                        .collect()
                }).unwrap_or_default();
                if col_names.is_empty() { return None; }
                Some(Box::new(MetaEnter { col_names }))
            }
            "delete" => {
                let col_names: Vec<String> = app.view().map(|v| {
                    let rows: Vec<usize> = if v.selected_rows.is_empty() {
                        vec![v.state.cr]
                    } else {
                        let mut r: Vec<_> = v.selected_rows.iter().copied().collect();
                        r.sort_by(|a, b| b.cmp(a));
                        r
                    };
                    rows.iter()
                        .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok()
                            .map(|v| v.to_string().trim_matches('"').to_string()))
                        .collect()
                }).unwrap_or_default();
                if col_names.is_empty() { return None; }
                Some(Box::new(MetaDelete { col_names }))
            }
            _ => None,
        }
    }

    fn parse(&self, cmd: &str, _arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "meta" | "metadata" => Some(Box::new(Metadata)),
            _ => None,
        }
    }

    fn commands(&self) -> Vec<(&str, &str)> {
        vec![("meta", "Show metadata/data profile")]
    }
}

// === Commands ===

const BG_THRESHOLD: usize = 10_000;

/// Metadata command - creates meta view
pub struct Metadata;

impl Command for Metadata {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (parent_id, parent_col, parent_rows, parent_name, cached, df, col_sep) = {
            let view = app.req()?;
            (view.id, view.state.cc, view.dataframe.height(), view.name.clone(),
             view.meta_cache.clone(), view.dataframe.clone(), view.col_separator)
        };

        let col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        let key_cols: Vec<String> = col_sep.map(|sep| col_names[..sep].to_vec()).unwrap_or_default();

        // Check cache (only for non-grouped)
        if key_cols.is_empty() {
            if let Some(cached_df) = cached {
                let id = app.next_id();
                let mut new_view = ViewState::new_child(id, "metadata".into(), cached_df, parent_id, parent_rows, parent_name.clone());
                new_view.state.cr = parent_col;
                app.stack.push(new_view);
                return Ok(());
            }
        }

        if parent_rows <= BG_THRESHOLD {
            let meta_df = if key_cols.is_empty() {
                compute_meta_stats(&df)?
            } else {
                compute_meta_stats_grouped(&df, &key_cols)?
            };
            if key_cols.is_empty() {
                if let Some(parent) = app.stack.find_mut(parent_id) {
                    parent.meta_cache = Some(meta_df.clone());
                }
            }
            let id = app.next_id();
            let mut new_view = ViewState::new_child(id, "metadata".into(), meta_df, parent_id, parent_rows, parent_name);
            new_view.state.cr = parent_col;
            if !key_cols.is_empty() { new_view.col_separator = Some(key_cols.len()); }
            app.stack.push(new_view);
        } else {
            // Large dataset: placeholder, compute in background
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

/// Meta Enter: pop and focus/xkey columns
pub struct MetaEnter { pub col_names: Vec<String> }

impl Command for MetaEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        if self.col_names.len() == 1 {
            if let Some(v) = app.view_mut() {
                if let Some(idx) = v.dataframe.get_column_names().iter().position(|c| c.as_str() == self.col_names[0]) {
                    v.state.cc = idx;
                }
            }
        } else {
            let _ = CommandExecutor::exec(app, Box::new(Xkey { col_names: self.col_names.clone() }));
        }
        Ok(())
    }
    fn to_str(&self) -> String { "meta_enter".to_string() }
    fn record(&self) -> bool { false }
}

/// Meta Delete: delete columns from parent
pub struct MetaDelete { pub col_names: Vec<String> }

impl Command for MetaDelete {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        let parent_id = app.view().and_then(|v| v.parent_id);

        if let Some(pid) = parent_id {
            if let Some(parent) = app.stack.find_mut(pid) {
                if let Some(sep) = parent.col_separator {
                    let all: Vec<String> = parent.dataframe.get_column_names().iter().map(|s| s.to_string()).collect();
                    let adj = self.col_names.iter()
                        .filter(|c| all.iter().position(|n| n == *c).map(|i| i < sep).unwrap_or(false))
                        .count();
                    parent.col_separator = Some(sep.saturating_sub(adj));
                }
                for c in &self.col_names { let _ = parent.dataframe.drop_in_place(c); }
            }
        }

        let _ = CommandExecutor::exec(app, Box::new(Pop));
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }
    fn to_str(&self) -> String { format!("meta_delete {}", self.col_names.join(",")) }
    fn record(&self) -> bool { false }
}

// === Stats computation ===

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
    let null_exprs: Vec<_> = col_names.iter().map(|c| col(c).null_count().alias(c)).collect();
    let min_exprs: Vec<_> = col_names.iter().map(|c| col(c).min().alias(c)).collect();
    let max_exprs: Vec<_> = col_names.iter().map(|c| col(c).max().alias(c)).collect();
    let distinct_exprs: Vec<_> = col_names.iter().map(|c| col(c).n_unique().alias(c)).collect();

    let numeric_cols: Vec<&String> = col_names.iter().zip(&is_numeric).filter(|(_, &is_num)| is_num).map(|(c, _)| c).collect();
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

        mins.push(min_df.column(name).ok().and_then(|c| c.get(0).ok()).map(|v| format_anyvalue(&v)).unwrap_or_default());
        maxs.push(max_df.column(name).ok().and_then(|c| c.get(0).ok()).map(|v| format_anyvalue(&v)).unwrap_or_default());
        distincts.push(distinct_df.column(name).ok().and_then(|c| c.get(0).ok()).map(|v| format!("{}", v.try_extract::<u32>().unwrap_or(0))).unwrap_or_default());

        if is_numeric[i] {
            medians.push(mean_df.as_ref().and_then(|df| df.column(name).ok()).and_then(|c| c.get(0).ok()).map(|v| format_anyvalue(&v)).unwrap_or_default());
            sigmas.push(std_df.as_ref().and_then(|df| df.column(name).ok()).and_then(|c| c.get(0).ok()).map(|v| format_anyvalue(&v)).unwrap_or_default());
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

fn compute_meta_stats_grouped(df: &DataFrame, key_cols: &[String]) -> Result<DataFrame> {
    let all_cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let non_key_cols: Vec<&String> = all_cols.iter().filter(|c| !key_cols.contains(c)).collect();

    let key_exprs: Vec<_> = key_cols.iter().map(|c| col(c)).collect();
    let unique_keys = df.clone().lazy()
        .select(key_exprs.clone())
        .unique(None, UniqueKeepStrategy::First)
        .sort(key_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(), SortMultipleOptions::default())
        .collect()?;

    let mut result_cols: Vec<Column> = Vec::new();

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

    let mut col_names_out = Vec::new();
    let mut types_out = Vec::new();
    let mut null_pcts = Vec::new();
    let mut distincts = Vec::new();
    let mut mins = Vec::new();
    let mut maxs = Vec::new();
    let mut medians = Vec::new();
    let mut sigmas = Vec::new();

    for row in 0..unique_keys.height() {
        let mut filter_expr = lit(true);
        for key_col in key_cols {
            let v = unique_keys.column(key_col)?.get(row)?;
            let scalar = Scalar::new(unique_keys.column(key_col)?.dtype().clone(), v.into_static());
            filter_expr = filter_expr.and(col(key_col).eq(lit(scalar)));
        }

        let group_df = df.clone().lazy().filter(filter_expr).collect()?;
        let group_rows = group_df.height() as f64;

        for &col_name in &non_key_cols {
            col_names_out.push(col_name.clone());
            let dtype = group_df.column(col_name)?.dtype().clone();
            types_out.push(format!("{:?}", dtype));

            let is_numeric = matches!(dtype,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                DataType::Float32 | DataType::Float64);

            let null_count = group_df.column(col_name)?.null_count() as f64;
            null_pcts.push(format!("{:.1}", 100.0 * null_count / group_rows));

            let distinct = group_df.column(col_name)?.n_unique()? as u32;
            distincts.push(format!("{}", distinct));

            let lazy = group_df.clone().lazy();
            let min_df = lazy.clone().select([col(col_name).min()]).collect()?;
            let max_df = lazy.clone().select([col(col_name).max()]).collect()?;
            mins.push(min_df.column(col_name).ok().and_then(|c| c.get(0).ok()).map(|v| format_anyvalue(&v)).unwrap_or_default());
            maxs.push(max_df.column(col_name).ok().and_then(|c| c.get(0).ok()).map(|v| format_anyvalue(&v)).unwrap_or_default());

            if is_numeric {
                let mean_df = lazy.clone().select([col(col_name).mean()]).collect()?;
                let std_df = lazy.select([col(col_name).std(1)]).collect()?;
                medians.push(mean_df.column(col_name).ok().and_then(|c| c.get(0).ok()).map(|v| format_anyvalue(&v)).unwrap_or_default());
                sigmas.push(std_df.column(col_name).ok().and_then(|c| c.get(0).ok()).map(|v| format_anyvalue(&v)).unwrap_or_default());
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
