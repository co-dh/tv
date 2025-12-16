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
        let col_names = sel_cols(app, cmd == "delete")?;
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
fn sel_cols(app: &AppContext, reverse: bool) -> Option<Vec<String>> {
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

/// Row count threshold for background computation (>10k runs in background thread)
const BG_THRESHOLD: usize = 10_000;

/// Metadata command - shows column statistics (null%, distinct, min, max, median, sigma)
pub struct Metadata;

impl Command for Metadata {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Block meta while gz is still loading
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let (parent_id, parent_col, parent_rows, parent_name, cached, df, col_sep, pq_path, col_names, schema) = {
            let view = app.req()?;
            let path = view.path().to_string();
            let cols = view.backend().cols(&path)?;
            let schema = view.backend().schema(&path)?;
            (view.id, view.state.cc, view.rows(), view.name.clone(),
             view.meta_cache.clone(), view.dataframe.clone(), view.col_separator, view.parquet_path.clone(), cols, schema)
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
        if let Some(path) = pq_path {
            let types: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();
            let placeholder = placeholder_df(col_names, types)?;
            let id = app.next_id();
            let mut v = ViewState::new_child(id, "metadata".into(), placeholder, parent_id, parent_rows, parent_name);
            v.state.cr = parent_col;
            app.stack.push(v);

            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || { if let Ok(r) = pq_stats(&path) { let _ = tx.send(r); } });
            app.bg_meta = Some((parent_id, rx));
        } else if parent_rows <= BG_THRESHOLD {
            let meta_df = if key_cols.is_empty() { compute_stats(&df)? } else { grp_stats(&df, &key_cols)? };
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
            let types: Vec<String> = df.dtypes().iter().map(|dt| format!("{:?}", dt)).collect();
            let placeholder = placeholder_df(col_names, types)?;

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

/// Enter from meta view - pops meta, moves cursor to selected column (or xkey if multi-select)
pub struct MetaEnter { pub col_names: Vec<String> }

impl Command for MetaEnter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));  // pop meta view
        if self.col_names.len() == 1 {  // single col: move cursor
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

/// Delete columns from parent table (selected rows in meta view = columns to delete)
pub struct MetaDelete { pub col_names: Vec<String> }

impl Command for MetaDelete {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        if let Some(pid) = app.view().and_then(|v| v.parent_id) {  // find parent table
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

/// Build stats DataFrame from column vectors
fn stats_df(cols: Vec<String>, types: Vec<String>, nulls: Vec<String>, dists: Vec<String>,
            mins: Vec<String>, maxs: Vec<String>, meds: Vec<String>, sigs: Vec<String>) -> Result<DataFrame> {
    Ok(DataFrame::new(vec![
        Series::new("column".into(), cols).into(), Series::new("type".into(), types).into(),
        Series::new("null%".into(), nulls).into(), Series::new("distinct".into(), dists).into(),
        Series::new("min".into(), mins).into(), Series::new("max".into(), maxs).into(),
        Series::new("median".into(), meds).into(), Series::new("sigma".into(), sigs).into(),
    ])?)
}

/// Build placeholder stats DataFrame (with "..." for pending values)
fn placeholder_df(cols: Vec<String>, types: Vec<String>) -> Result<DataFrame> {
    let n = cols.len();
    stats_df(cols, types, vec!["...".into(); n], vec!["...".into(); n],
             vec!["...".into(); n], vec!["...".into(); n], vec!["...".into(); n], vec!["...".into(); n])
}

/// Compute column statistics for in-memory DataFrame
/// Returns: column, type, null%, distinct, min, max, median (mean for numeric), sigma (std dev)
fn compute_stats(df: &DataFrame) -> Result<DataFrame> {
    let cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let dtypes = df.dtypes();
    let n = df.height() as f64;  // row count for null% calculation
    let lazy = df.clone().lazy();  // lazy for efficient batch aggregations

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

    let types: Vec<String> = dtypes.iter().map(|dt| format!("{:?}", dt)).collect();
    stats_df(cols, types, nulls, dists, mins, maxs, meds, sigs)
}

/// Compute grouped column statistics - stats per unique key combination
/// Used when xkey columns are set; shows stats for each group separately
fn grp_stats(df: &DataFrame, keys: &[String]) -> Result<DataFrame> {
    let all: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let non_keys: Vec<&String> = all.iter().filter(|c| !keys.contains(c)).collect();  // columns to analyze

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

/// Compute stats from parquet (streaming, column-by-column to avoid OOM)
fn pq_stats(path: &str) -> Result<DataFrame> {
    use crate::backend::{Backend, Polars};
    use polars::prelude::{ScanArgsParquet, PlPath, Engine};
    let schema = Polars.schema(path)?;
    let (rows, _) = Polars.metadata(path)?;
    let n = rows as f64;

    let cols: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();
    let dtypes: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();
    let (mut nulls, mut mins, mut maxs, mut dists, mut meds, mut sigs) = (vec![], vec![], vec![], vec![], vec![], vec![]);

    // Process each column separately (streaming) to avoid OOM
    for (i, c) in cols.iter().enumerate() {
        let lf = || LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default()).ok();
        let is_num = matches!(dtypes[i].as_str(), "Int8"|"Int16"|"Int32"|"Int64"|"UInt8"|"UInt16"|"UInt32"|"UInt64"|"Float32"|"Float64");

        // null count (streaming)
        let nc = lf().and_then(|l| l.select([col(c).null_count()]).collect_with_engine(Engine::Streaming).ok())
            .and_then(|df| df.column(c).ok()?.get(0).ok()?.try_extract::<u32>().ok()).unwrap_or(0);
        nulls.push(format!("{:.1}", 100.0 * nc as f64 / n));

        // min/max (streaming)
        mins.push(lf().and_then(|l| l.select([col(c).min()]).collect_with_engine(Engine::Streaming).ok()).map(|df| get_str(&df, c)).unwrap_or_default());
        maxs.push(lf().and_then(|l| l.select([col(c).max()]).collect_with_engine(Engine::Streaming).ok()).map(|df| get_str(&df, c)).unwrap_or_default());

        // distinct (streaming)
        let dc = lf().and_then(|l| l.select([col(c).n_unique()]).collect_with_engine(Engine::Streaming).ok())
            .and_then(|df| df.column(c).ok()?.get(0).ok()?.try_extract::<u32>().ok()).unwrap_or(0);
        dists.push(format!("{}", dc));

        // mean/std for numeric (streaming)
        if is_num {
            meds.push(lf().and_then(|l| l.select([col(c).mean()]).collect_with_engine(Engine::Streaming).ok()).map(|df| get_str(&df, c)).unwrap_or_default());
            sigs.push(lf().and_then(|l| l.select([col(c).std(1)]).collect_with_engine(Engine::Streaming).ok()).map(|df| get_str(&df, c)).unwrap_or_default());
        } else { meds.push(String::new()); sigs.push(String::new()); }
    }

    stats_df(cols, dtypes, nulls, dists, mins, maxs, meds, sigs)
}

/// Extract f64 from first row of column (for single-row stats DF)
fn get_f64(df: &DataFrame, c: &str) -> f64 { df.column(c).ok().and_then(|c| c.get(0).ok()).and_then(|v| v.try_extract::<u32>().ok()).unwrap_or(0) as f64 }
/// Extract u32 from first row of column
fn get_u32(df: &DataFrame, c: &str) -> u32 { df.column(c).ok().and_then(|c| c.get(0).ok()).and_then(|v| v.try_extract::<u32>().ok()).unwrap_or(0) }
/// Extract string from first row of column
fn get_str(df: &DataFrame, c: &str) -> String { df.column(c).ok().and_then(|c| c.get(0).ok()).map(|v| fmt(&v)).unwrap_or_default() }
/// Format AnyValue to string, trimming quotes
fn fmt(v: &AnyValue) -> String {
    match v {
        AnyValue::Null => String::new(),
        AnyValue::Float64(f) => format!("{:.2}", f),
        AnyValue::Float32(f) => format!("{:.2}", f),
        _ => { let s = v.to_string(); if s == "null" { String::new() } else { s.trim_matches('"').to_string() } }
    }
}
