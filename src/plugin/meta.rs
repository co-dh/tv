//! Meta view plugin - data profile/metadata statistics

use crate::app::AppContext;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::Xkey;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;

pub struct MetaPlugin;

impl Plugin for MetaPlugin {
    fn name(&self) -> &str { "meta" }
    fn tab(&self) -> &str { "meta" }
    fn matches(&self, name: &str) -> bool { name == "metadata" }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        let col_names = get_selected_col_names(app, cmd == "delete")?;
        match cmd {
            "enter" => Some(Box::new(MetaEnter { col_names })),
            "delete" => Some(Box::new(MetaDelete { col_names })),
            _ => None,
        }
    }

    fn parse(&self, cmd: &str, _arg: &str) -> Option<Box<dyn Command>> {
        match cmd { "meta" | "metadata" => Some(Box::new(Metadata)), _ => None }
    }
}

/// Get column names from selected rows (first column values)
fn get_selected_col_names(app: &AppContext, reverse: bool) -> Option<Vec<String>> {
    app.view().and_then(|v| {
        let mut rows: Vec<usize> = if v.selected_rows.is_empty() {
            vec![v.state.cr]
        } else {
            v.selected_rows.iter().copied().collect()
        };
        if reverse { rows.sort_by(|a, b| b.cmp(a)); } else { rows.sort(); }
        let names: Vec<String> = rows.iter()
            .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok()
                .map(|v| v.to_string().trim_matches('"').to_string()))
            .collect();
        if names.is_empty() { None } else { Some(names) }
    })
}

// === Commands ===

const BG_THRESHOLD: usize = 10_000;

pub struct Metadata;

impl Command for Metadata {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Block meta while gz is still loading
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let (parent_id, parent_col, parent_rows, parent_name, cached, df, col_sep, parquet) = {
            let view = app.req()?;
            (view.id, view.state.cc, view.rows(), view.name.clone(),
             view.meta_cache.clone(), view.dataframe.clone(), view.col_separator, view.parquet_path.clone())
        };

        // For lazy parquet, get column names from disk
        let col_names: Vec<String> = if let Some(ref path) = parquet {
            crate::command::io::parquet::schema(std::path::Path::new(path))?
                .into_iter().map(|(name, _)| name).collect()
        } else {
            df.get_column_names().iter().map(|s| s.to_string()).collect()
        };
        let key_cols: Vec<String> = col_sep.map(|sep| col_names[..sep].to_vec()).unwrap_or_default();

        // Check cache (only for non-grouped)
        if key_cols.is_empty() {
            if let Some(cached_df) = cached {
                let id = app.next_id();
                let mut v = ViewState::new_child(id, "metadata".into(), cached_df, parent_id, parent_rows, parent_name);
                v.state.cr = parent_col;
                app.stack.push(v);
                return Ok(());
            }
        }

        // Lazy parquet: always background compute from disk
        if let Some(path) = parquet {
            let dtypes = crate::command::io::parquet::schema(std::path::Path::new(&path))?;
            let n = col_names.len();
            let placeholder = DataFrame::new(vec![
                Series::new("column".into(), col_names).into(),
                Series::new("type".into(), dtypes.iter().map(|(_, dt)| dt.clone()).collect::<Vec<_>>()).into(),
                Series::new("null%".into(), vec!["..."; n]).into(),
                Series::new("distinct".into(), vec!["..."; n]).into(),
                Series::new("min".into(), vec!["..."; n]).into(),
                Series::new("max".into(), vec!["..."; n]).into(),
                Series::new("median".into(), vec!["..."; n]).into(),
                Series::new("sigma".into(), vec!["..."; n]).into(),
            ])?;
            let id = app.next_id();
            let mut v = ViewState::new_child(id, "metadata".into(), placeholder, parent_id, parent_rows, parent_name);
            v.state.cr = parent_col;
            app.stack.push(v);

            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || { if let Ok(r) = compute_stats_from_parquet(&path) { let _ = tx.send(r); } });
            app.bg_meta = Some((parent_id, rx));
        } else if parent_rows <= BG_THRESHOLD {
            let meta_df = if key_cols.is_empty() { compute_stats(&df)? } else { compute_stats_grouped(&df, &key_cols)? };
            if key_cols.is_empty() {
                if let Some(parent) = app.stack.find_mut(parent_id) { parent.meta_cache = Some(meta_df.clone()); }
            }
            let id = app.next_id();
            let mut v = ViewState::new_child(id, "metadata".into(), meta_df, parent_id, parent_rows, parent_name);
            v.state.cr = parent_col;
            if !key_cols.is_empty() { v.col_separator = Some(key_cols.len()); }
            app.stack.push(v);
        } else {
            // Large dataset: placeholder + background compute
            let n = col_names.len();
            let placeholder = DataFrame::new(vec![
                Series::new("column".into(), col_names).into(),
                Series::new("type".into(), df.dtypes().iter().map(|dt| format!("{:?}", dt)).collect::<Vec<_>>()).into(),
                Series::new("null%".into(), vec!["..."; n]).into(),
                Series::new("distinct".into(), vec!["..."; n]).into(),
                Series::new("min".into(), vec!["..."; n]).into(),
                Series::new("max".into(), vec!["..."; n]).into(),
                Series::new("median".into(), vec!["..."; n]).into(),
                Series::new("sigma".into(), vec!["..."; n]).into(),
            ])?;

            let id = app.next_id();
            let mut v = ViewState::new_child(id, "metadata".into(), placeholder, parent_id, parent_rows, parent_name);
            v.state.cr = parent_col;
            app.stack.push(v);

            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || { if let Ok(r) = compute_stats(&df) { let _ = tx.send(r); } });
            app.bg_meta = Some((parent_id, rx));
        }
        Ok(())
    }
    fn to_str(&self) -> String { "meta".into() }
}

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
    fn to_str(&self) -> String { "meta_enter".into() }
    fn record(&self) -> bool { false }
}

pub struct MetaDelete { pub col_names: Vec<String> }

impl Command for MetaDelete {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        if let Some(pid) = app.view().and_then(|v| v.parent_id) {
            if let Some(parent) = app.stack.find_mut(pid) {
                if let Some(sep) = parent.col_separator {
                    let all: Vec<String> = parent.dataframe.get_column_names().iter().map(|s| s.to_string()).collect();
                    let adj = self.col_names.iter().filter(|c| all.iter().position(|n| n == *c).map(|i| i < sep).unwrap_or(false)).count();
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

fn compute_stats(df: &DataFrame) -> Result<DataFrame> {
    let cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let dtypes = df.dtypes();
    let n = df.height() as f64;
    let lazy = df.clone().lazy();

    let is_num: Vec<bool> = dtypes.iter().map(|dt| matches!(dt,
        DataType::Int8|DataType::Int16|DataType::Int32|DataType::Int64|
        DataType::UInt8|DataType::UInt16|DataType::UInt32|DataType::UInt64|
        DataType::Float32|DataType::Float64)).collect();

    let null_df = lazy.clone().select(cols.iter().map(|c| col(c).null_count().alias(c)).collect::<Vec<_>>()).collect()?;
    let min_df = lazy.clone().select(cols.iter().map(|c| col(c).min().alias(c)).collect::<Vec<_>>()).collect()?;
    let max_df = lazy.clone().select(cols.iter().map(|c| col(c).max().alias(c)).collect::<Vec<_>>()).collect()?;
    let dist_df = lazy.clone().select(cols.iter().map(|c| col(c).n_unique().alias(c)).collect::<Vec<_>>()).collect()?;

    let num_cols: Vec<&String> = cols.iter().zip(&is_num).filter(|(_, &b)| b).map(|(c, _)| c).collect();
    let mean_df = if !num_cols.is_empty() { Some(lazy.clone().select(num_cols.iter().map(|c| col(*c).mean().alias(*c)).collect::<Vec<_>>()).collect()?) } else { None };
    let std_df = if !num_cols.is_empty() { Some(lazy.select(num_cols.iter().map(|c| col(*c).std(1).alias(*c)).collect::<Vec<_>>()).collect()?) } else { None };

    let (mut nulls, mut mins, mut maxs, mut dists, mut meds, mut sigs) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (i, c) in cols.iter().enumerate() {
        nulls.push(format!("{:.1}", 100.0 * get_f64(&null_df, c) / n));
        mins.push(get_str(&min_df, c));
        maxs.push(get_str(&max_df, c));
        dists.push(format!("{}", get_u32(&dist_df, c)));
        if is_num[i] {
            meds.push(mean_df.as_ref().map(|df| get_str(df, c)).unwrap_or_default());
            sigs.push(std_df.as_ref().map(|df| get_str(df, c)).unwrap_or_default());
        } else { meds.push(String::new()); sigs.push(String::new()); }
    }

    Ok(DataFrame::new(vec![
        Series::new("column".into(), cols).into(),
        Series::new("type".into(), dtypes.iter().map(|dt| format!("{:?}", dt)).collect::<Vec<_>>()).into(),
        Series::new("null%".into(), nulls).into(), Series::new("distinct".into(), dists).into(),
        Series::new("min".into(), mins).into(), Series::new("max".into(), maxs).into(),
        Series::new("median".into(), meds).into(), Series::new("sigma".into(), sigs).into(),
    ])?)
}

fn compute_stats_grouped(df: &DataFrame, keys: &[String]) -> Result<DataFrame> {
    let all: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let non_keys: Vec<&String> = all.iter().filter(|c| !keys.contains(c)).collect();

    let unique = df.clone().lazy()
        .select(keys.iter().map(|c| col(c)).collect::<Vec<_>>())
        .unique(None, UniqueKeepStrategy::First)
        .sort(keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(), SortMultipleOptions::default())
        .collect()?;

    let mut result: Vec<Column> = Vec::new();
    for k in keys {
        let mut vals = Vec::new();
        for r in 0..unique.height() {
            for _ in &non_keys {
                vals.push(unique.column(k).ok().and_then(|c| c.get(r).ok()).map(|v| fmt(&v)).unwrap_or_default());
            }
        }
        result.push(Series::new(k.as_str().into(), vals).into());
    }

    let (mut names, mut types, mut nulls, mut dists, mut mins, mut maxs, mut meds, mut sigs) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);

    for r in 0..unique.height() {
        let filter = keys.iter().fold(lit(true), |acc, k| {
            let v = unique.column(k).unwrap().get(r).unwrap();
            acc.and(col(k).eq(lit(Scalar::new(unique.column(k).unwrap().dtype().clone(), v.into_static()))))
        });
        let grp = df.clone().lazy().filter(filter).collect()?;
        let n = grp.height() as f64;

        for &c in &non_keys {
            names.push(c.clone());
            let dt = grp.column(c)?.dtype().clone();
            types.push(format!("{:?}", dt));
            nulls.push(format!("{:.1}", 100.0 * grp.column(c)?.null_count() as f64 / n));
            dists.push(format!("{}", grp.column(c)?.n_unique()?));

            let lz = grp.clone().lazy();
            mins.push(lz.clone().select([col(c).min()]).collect().ok().map(|df| get_str(&df, c)).unwrap_or_default());
            maxs.push(lz.clone().select([col(c).max()]).collect().ok().map(|df| get_str(&df, c)).unwrap_or_default());

            let is_num = matches!(dt, DataType::Int8|DataType::Int16|DataType::Int32|DataType::Int64|
                DataType::UInt8|DataType::UInt16|DataType::UInt32|DataType::UInt64|DataType::Float32|DataType::Float64);
            if is_num {
                meds.push(lz.clone().select([col(c).mean()]).collect().ok().map(|df| get_str(&df, c)).unwrap_or_default());
                sigs.push(lz.select([col(c).std(1)]).collect().ok().map(|df| get_str(&df, c)).unwrap_or_default());
            } else { meds.push(String::new()); sigs.push(String::new()); }
        }
    }

    result.extend([
        Series::new("column".into(), names).into(), Series::new("type".into(), types).into(),
        Series::new("null%".into(), nulls).into(), Series::new("distinct".into(), dists).into(),
        Series::new("min".into(), mins).into(), Series::new("max".into(), maxs).into(),
        Series::new("median".into(), meds).into(), Series::new("sigma".into(), sigs).into(),
    ]);
    Ok(DataFrame::new(result)?)
}

/// Compute stats from parquet file on disk (lazy)
fn compute_stats_from_parquet(path: &str) -> Result<DataFrame> {
    use polars::prelude::ScanArgsParquet;
    let args = ScanArgsParquet::default();
    let lazy = LazyFrame::scan_parquet(path, args).map_err(|e| anyhow!("{}", e))?;
    let schema = crate::command::io::parquet::schema(std::path::Path::new(path))?;
    let (rows, _) = crate::command::io::parquet::metadata(std::path::Path::new(path))?;
    let n = rows as f64;

    let cols: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();
    let dtypes: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();

    let is_num: Vec<bool> = dtypes.iter().map(|dt| matches!(dt.as_str(),
        "Int8"|"Int16"|"Int32"|"Int64"|"UInt8"|"UInt16"|"UInt32"|"UInt64"|"Float32"|"Float64")).collect();

    let null_df = lazy.clone().select(cols.iter().map(|c| col(c).null_count().alias(c)).collect::<Vec<_>>()).collect()?;
    let min_df = lazy.clone().select(cols.iter().map(|c| col(c).min().alias(c)).collect::<Vec<_>>()).collect()?;
    let max_df = lazy.clone().select(cols.iter().map(|c| col(c).max().alias(c)).collect::<Vec<_>>()).collect()?;
    let dist_df = lazy.clone().select(cols.iter().map(|c| col(c).n_unique().alias(c)).collect::<Vec<_>>()).collect()?;

    let num_cols: Vec<&String> = cols.iter().zip(&is_num).filter(|(_, &b)| b).map(|(c, _)| c).collect();
    let mean_df = if !num_cols.is_empty() { Some(lazy.clone().select(num_cols.iter().map(|c| col(*c).mean().alias(*c)).collect::<Vec<_>>()).collect()?) } else { None };
    let std_df = if !num_cols.is_empty() { Some(lazy.select(num_cols.iter().map(|c| col(*c).std(1).alias(*c)).collect::<Vec<_>>()).collect()?) } else { None };

    let (mut nulls, mut mins, mut maxs, mut dists, mut meds, mut sigs) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (i, c) in cols.iter().enumerate() {
        nulls.push(format!("{:.1}", 100.0 * get_f64(&null_df, c) / n));
        mins.push(get_str(&min_df, c));
        maxs.push(get_str(&max_df, c));
        dists.push(format!("{}", get_u32(&dist_df, c)));
        if is_num[i] {
            meds.push(mean_df.as_ref().map(|df| get_str(df, c)).unwrap_or_default());
            sigs.push(std_df.as_ref().map(|df| get_str(df, c)).unwrap_or_default());
        } else { meds.push(String::new()); sigs.push(String::new()); }
    }

    Ok(DataFrame::new(vec![
        Series::new("column".into(), cols).into(),
        Series::new("type".into(), dtypes).into(),
        Series::new("null%".into(), nulls).into(), Series::new("distinct".into(), dists).into(),
        Series::new("min".into(), mins).into(), Series::new("max".into(), maxs).into(),
        Series::new("median".into(), meds).into(), Series::new("sigma".into(), sigs).into(),
    ])?)
}

fn get_f64(df: &DataFrame, c: &str) -> f64 { df.column(c).ok().and_then(|c| c.get(0).ok()).and_then(|v| v.try_extract::<u32>().ok()).unwrap_or(0) as f64 }
fn get_u32(df: &DataFrame, c: &str) -> u32 { df.column(c).ok().and_then(|c| c.get(0).ok()).and_then(|v| v.try_extract::<u32>().ok()).unwrap_or(0) }
fn get_str(df: &DataFrame, c: &str) -> String { df.column(c).ok().and_then(|c| c.get(0).ok()).map(|v| fmt(&v)).unwrap_or_default() }
fn fmt(v: &AnyValue) -> String {
    match v {
        AnyValue::Null => String::new(),
        AnyValue::Float64(f) => format!("{:.2}", f),
        AnyValue::Float32(f) => format!("{:.2}", f),
        _ => { let s = v.to_string(); if s == "null" { String::new() } else { s.trim_matches('"').to_string() } }
    }
}
