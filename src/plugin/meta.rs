//! Meta view plugin - data profile/metadata statistics

use crate::app::AppContext;
use crate::source::df_cols;
use crate::utils::{is_numeric, unquote};
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::transform::Xkey;
use crate::command::view::Pop;
use crate::plugin::Plugin;
use crate::state::ViewState;
use crate::ser;
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
                .map(|v| unquote(&v.to_string())))
            .collect();
        if names.is_empty() { None } else { Some(names) }
    })
}

// === Commands ===

/// Push meta view onto stack
fn push_meta(app: &mut AppContext, df: DataFrame, pid: usize, prows: usize, pname: String, pcol: usize, sep: Option<usize>) {
    let id = app.next_id();
    let mut v = ViewState::new_meta(id, df, pid, prows, pname);
    v.state.cr = pcol;
    if let Some(s) = sep { v.col_separator = Some(s); }
    app.stack.push(v);
}

/// Metadata command - shows column statistics (null%, distinct, min, max, median, sigma)
pub struct Metadata;

impl Command for Metadata {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Block meta while gz is still loading
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let (parent_id, parent_col, parent_rows, parent_name, cached, df, col_sep, pq_path, col_names, schema) = {
            let view = app.req()?;
            let path = view.path().to_string();
            let cols = view.source().cols(&path)?;
            let schema = view.source().schema(&path)?;
            (view.id, view.state.cc, view.rows(), view.name.clone(),
             view.meta_cache.clone(), view.dataframe.clone(), view.col_separator, view.parquet_path.clone(), cols, schema)
        };
        let key_cols: Vec<String> = col_sep.map(|sep| col_names[..sep].to_vec()).unwrap_or_default();

        // Check cache (only for non-grouped)
        if key_cols.is_empty() {
            if let Some(cached_df) = cached {
                push_meta(app, cached_df, parent_id, parent_rows, parent_name, parent_col, None);
                return Ok(());
            }
        }

        // Grouped stats need in-memory df
        if !key_cols.is_empty() {
            let types: Vec<String> = df.dtypes().iter().map(|dt| format!("{:?}", dt)).collect();
            let placeholder = placeholder_df(col_names, types)?;
            push_meta(app, placeholder, parent_id, parent_rows, parent_name, parent_col, Some(key_cols.len()));
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || { if let Ok(r) = grp_stats(&df, &key_cols) { let _ = tx.send(r); } });
            app.bg_meta = Some((parent_id, rx));
            return Ok(());
        }

        // Non-grouped: unified LazyFrame path (parquet or in-memory)
        let types: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();
        let placeholder = placeholder_df(col_names, types)?;
        push_meta(app, placeholder, parent_id, parent_rows, parent_name, parent_col, None);
        let (tx, rx) = std::sync::mpsc::channel();
        if let Some(path) = pq_path {
            std::thread::spawn(move || { if let Ok(r) = lf_stats_path(&path) { let _ = tx.send(r); } });
        } else {
            std::thread::spawn(move || { if let Ok(r) = lf_stats(&df) { let _ = tx.send(r); } });
        }
        app.bg_meta = Some((parent_id, rx));
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
                // Use col_names for parquet, dataframe for memory
                let cols = if v.col_names.is_empty() { df_cols(&v.dataframe) } else { v.col_names.clone() };
                if let Some(idx) = cols.iter().position(|c| c == &self.col_names[0]) {
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
        if let Some(pid) = app.view().and_then(|v| v.parent_id) {
            if let Some(parent) = app.stack.find_mut(pid) {
                // Adjust col_separator if deleting key columns
                if let Some(sep) = parent.col_separator {
                    let cols = if parent.col_names.is_empty() { df_cols(&parent.dataframe) } else { parent.col_names.clone() };
                    let adj = self.col_names.iter().filter(|c| cols.iter().position(|x| x == *c).map(|i| i < sep).unwrap_or(false)).count();
                    parent.col_separator = Some(sep.saturating_sub(adj));
                }
                // Init col_names from df if empty, then remove deleted columns
                if parent.col_names.is_empty() { parent.col_names = df_cols(&parent.dataframe); }
                parent.col_names.retain(|c| !self.col_names.contains(c));
                // Clear cache to force re-fetch with new column list
                parent.fetch_cache = None;
                // For in-memory views, also drop from dataframe
                if parent.parquet_path.is_none() {
                    for c in &self.col_names { let _ = parent.dataframe.drop_in_place(c); }
                }
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

use crate::source::sql;

/// Build stats DataFrame from column vectors
fn stats_df(cols: Vec<String>, types: Vec<String>, nulls: Vec<String>, dists: Vec<String>,
            mins: Vec<String>, maxs: Vec<String>, meds: Vec<String>, sigs: Vec<String>) -> Result<DataFrame> {
    Ok(DataFrame::new(vec![
        ser!("column", cols), ser!("type", types), ser!("null%", nulls), ser!("distinct", dists),
        ser!("min", mins), ser!("max", maxs), ser!("median", meds), ser!("sigma", sigs),
    ])?)
}

/// Build placeholder stats DataFrame (with "..." for pending values)
fn placeholder_df(cols: Vec<String>, types: Vec<String>) -> Result<DataFrame> {
    let n = cols.len();
    stats_df(cols, types, vec!["...".into(); n], vec!["...".into(); n],
             vec!["...".into(); n], vec!["...".into(); n], vec!["...".into(); n], vec!["...".into(); n])
}

/// Column stats result from SQL query
struct ColStats { nulls: f64, distinct: u32, min: String, max: String, mean: String, std: String }

/// Compute stats for one column using SQL (works for both in-memory and lazy parquet)
fn col_stats(lf: LazyFrame, col: &str, n: f64, is_num: bool) -> ColStats {
    // SQL query - skip AVG/STDDEV for non-numeric columns (causes SQL error)
    let q = if is_num {
        format!(r#"SELECT COUNT(*) - COUNT("{}") as nulls, COUNT(DISTINCT "{}") as dist,
            MIN("{}") as min, MAX("{}") as max, AVG("{}") as mean, STDDEV("{}") as std FROM df"#,
            col, col, col, col, col, col)
    } else {
        format!(r#"SELECT COUNT(*) - COUNT("{}") as nulls, COUNT(DISTINCT "{}") as dist,
            MIN("{}") as min, MAX("{}") as max FROM df"#, col, col, col, col)
    };
    let df = sql(lf, &q).ok();
    let get = |c: &str| df.as_ref().and_then(|d| d.column(c).ok()?.get(0).ok()).map(|v| fmt(&v)).unwrap_or_default();
    let nulls = df.as_ref().and_then(|d| d.column("nulls").ok()?.get(0).ok()?.try_extract::<u32>().ok()).unwrap_or(0) as f64;
    let distinct = df.as_ref().and_then(|d| d.column("dist").ok()?.get(0).ok()?.try_extract::<u32>().ok()).unwrap_or(0);
    ColStats {
        nulls: 100.0 * nulls / n, distinct,
        min: get("min"), max: get("max"),
        mean: if is_num { get("mean") } else { String::new() },
        std: if is_num { get("std") } else { String::new() },
    }
}


/// Check if type string is numeric (for parquet schema)
fn is_numeric_str(s: &str) -> bool {
    matches!(s, "Int8"|"Int16"|"Int32"|"Int64"|"UInt8"|"UInt16"|"UInt32"|"UInt64"|"Float32"|"Float64")
}

/// Compute stats from in-memory DataFrame via LazyFrame + SQL
fn lf_stats(df: &DataFrame) -> Result<DataFrame> {
    let cols = df_cols(df);
    let dtypes = df.dtypes();
    let types: Vec<String> = dtypes.iter().map(|dt| format!("{:?}", dt)).collect();
    let n = df.height() as f64;
    let (mut nulls, mut mins, mut maxs, mut dists, mut meds, mut sigs) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (i, c) in cols.iter().enumerate() {
        let s = col_stats(df.clone().lazy(), c, n, is_numeric(&dtypes[i]));
        nulls.push(format!("{:.1}", s.nulls)); dists.push(format!("{}", s.distinct));
        mins.push(s.min); maxs.push(s.max); meds.push(s.mean); sigs.push(s.std);
    }
    stats_df(cols, types, nulls, dists, mins, maxs, meds, sigs)
}

/// Compute grouped column statistics - stats per unique key combination
/// Used when xkey columns are set; shows stats for each group separately
fn grp_stats(df: &DataFrame, keys: &[String]) -> Result<DataFrame> {
    let all = df_cols(df);
    let non_keys: Vec<&String> = all.iter().filter(|c| !keys.contains(c)).collect();

    // Get unique key combinations
    let unique = df.clone().lazy()
        .select(keys.iter().map(|c| col(c)).collect::<Vec<_>>())
        .unique(None, UniqueKeepStrategy::First)
        .sort(keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(), SortMultipleOptions::default())
        .collect()?;

    // Build key columns (repeated for each non-key column)
    let mut result: Vec<Column> = Vec::new();
    for k in keys {
        let mut vals = Vec::new();
        for r in 0..unique.height() {
            for _ in &non_keys { vals.push(unique.column(k).ok().and_then(|c| c.get(r).ok()).map(|v| fmt(&v)).unwrap_or_default()); }
        }
        result.push(ser!(k.as_str(), vals));
    }

    // Compute stats per group per column using SQL
    let (mut names, mut types, mut nulls, mut dists, mut mins, mut maxs, mut meds, mut sigs) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);

    for r in 0..unique.height() {
        // Build filter for this key combo
        let filter = keys.iter().fold(lit(true), |acc, k| {
            let v = unique.column(k).unwrap().get(r).unwrap();
            acc.and(col(k).eq(lit(Scalar::new(unique.column(k).unwrap().dtype().clone(), v.into_static()))))
        });
        let grp = df.clone().lazy().filter(filter).collect()?;
        let n = grp.height() as f64;

        for &c in &non_keys {
            let dt = grp.column(c)?.dtype().clone();
            let s = col_stats(grp.clone().lazy(), c, n, is_numeric(&dt));
            names.push(c.clone()); types.push(format!("{:?}", dt));
            nulls.push(format!("{:.1}", s.nulls)); dists.push(format!("{}", s.distinct));
            mins.push(s.min); maxs.push(s.max); meds.push(s.mean); sigs.push(s.std);
        }
    }

    result.extend([
        ser!("column", names), ser!("type", types), ser!("null%", nulls), ser!("distinct", dists),
        ser!("min", mins), ser!("max", maxs), ser!("median", meds), ser!("sigma", sigs),
    ]);
    Ok(DataFrame::new(result)?)
}

/// Compute stats from parquet path via LazyFrame + SQL (column-by-column to avoid OOM)
fn lf_stats_path(path: &str) -> Result<DataFrame> {
    use crate::source::{Source, Polars};
    use polars::prelude::{ScanArgsParquet, PlPath};
    use std::io::Write;
    let t0 = std::time::Instant::now();
    let _ = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/tv.debug.log")
        .and_then(|mut f| writeln!(f, "[lf_stats_path] START {}", path));

    let schema = Polars.schema(path)?;
    let (rows, _) = Polars.metadata(path)?;
    let cols: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();
    let types: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();
    let n = rows as f64;
    let (mut nulls, mut mins, mut maxs, mut dists, mut meds, mut sigs) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (i, c) in cols.iter().enumerate() {
        let lf = LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?;
        let s = col_stats(lf, c, n, is_numeric_str(&types[i]));
        nulls.push(format!("{:.1}", s.nulls)); dists.push(format!("{}", s.distinct));
        mins.push(s.min); maxs.push(s.max); meds.push(s.mean); sigs.push(s.std);
    }
    let _ = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/tv.debug.log")
        .and_then(|mut f| writeln!(f, "[lf_stats_path] DONE {:.2}s {} cols", t0.elapsed().as_secs_f64(), cols.len()));
    stats_df(cols, types, nulls, dists, mins, maxs, meds, sigs)
}

/// Format AnyValue to string, trimming quotes
fn fmt(v: &AnyValue) -> String {
    match v {
        AnyValue::Null => String::new(),
        AnyValue::Float64(f) => format!("{:.2}", f),
        AnyValue::Float32(f) => format!("{:.2}", f),
        _ => { let s = v.to_string(); if s == "null" { String::new() } else { unquote(&s) } }
    }
}
