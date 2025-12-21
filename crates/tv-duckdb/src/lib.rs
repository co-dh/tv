//! DuckDB CLI plugin - uses duckdb command for fast parquet queries

use std::ffi::c_char;
use std::process::Command;
use std::sync::OnceLock;
use tv_plugin_api::*;
use tv_plugin_api::prql::QueryCache;

/// Debug log to ~/.tv/debug.log
fn dbg(msg: &str) {
    use std::io::Write;
    if let Some(home) = std::env::var_os("HOME") {
        let path = std::path::Path::new(&home).join(".tv/debug.log");
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
            let now = chrono::Local::now().format("%H:%M:%S%.3f");
            let _ = writeln!(f, "[{}] DUCK {}", now, msg);
        }
    }
}

/// Parsed row from CSV output
#[derive(Clone)]
struct Row(Vec<String>);

/// Simple table from duckdb CSV output
struct DuckTable {
    cols: Vec<String>,
    rows: Vec<Row>,
}


static CACHE: OnceLock<QueryCache<DuckTable>> = OnceLock::new();
fn cache() -> &'static QueryCache<DuckTable> {
    CACHE.get_or_init(|| QueryCache::new(100))
}

/// Execute SQL via duckdb CLI, parse CSV output
fn exec_duckdb(path: &str, sql: &str) -> Option<DuckTable> {
    use std::process::Stdio;
    // Replace 'df' with direct parquet file reference
    let pq = format!("'{}'", path);
    // Add trailing space to sql to simplify replacement, then trim
    let sql = format!("{} ", sql)
        .replace(" df ", &format!(" {} ", pq))
        .replace("(df)", &format!("({})", pq))
        .replace("(df ", &format!("({} ", pq));
    let sql = sql.trim();

    let output = Command::new("duckdb")
        .args(["-csv", "-c", &sql])
        .stdin(Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        dbg(&format!("ERR {}", err));
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_csv(&stdout)
}

/// Parse CSV output into DuckTable
fn parse_csv(csv: &str) -> Option<DuckTable> {
    let mut lines = csv.lines();
    let header = lines.next()?;
    let cols: Vec<String> = header.split(',').map(|s| s.trim().to_string()).collect();

    let rows: Vec<Row> = lines
        .filter(|l| !l.is_empty())
        .map(|l| Row(parse_csv_row(l)))
        .collect();

    Some(DuckTable { cols, rows })
}

/// Parse a CSV row (handles quoted fields)
fn parse_csv_row(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;

    for ch in line.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(field.trim().to_string());
                field = String::new();
            }
            _ => field.push(ch),
        }
    }
    fields.push(field.trim().to_string());
    fields
}

// === Plugin interface ===

#[unsafe(no_mangle)]
pub extern "C" fn tv_plugin_init() -> PluginVtable {
    PluginVtable {
        version: PLUGIN_API_VERSION,
        name: c"duckdb".as_ptr(),
        query: tv_query,
        result_free: tv_result_free,
        result_rows: tv_result_rows,
        result_cols: tv_result_cols,
        col_name: tv_col_name,
        col_type: tv_col_type,
        cell: tv_cell,
        str_free: tv_str_free,
        save: tv_save,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_query(prql_ptr: *const c_char, path_ptr: *const c_char) -> TableHandle {
    if prql_ptr.is_null() || path_ptr.is_null() { return std::ptr::null_mut(); }
    let prql = unsafe { from_c_str(prql_ptr) };
    let path = unsafe { from_c_str(path_ptr) };
    dbg(&format!("QUERY prql={} path={}", &prql[..prql.len().min(60)], path));

    cache().get_or_exec(&path, &prql, |sql| {
        dbg(&format!("EXEC sql={}", &sql[..sql.len().min(80)]));
        exec_duckdb(&path, sql)
    }).unwrap_or(std::ptr::null()) as TableHandle
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_result_free(_h: TableHandle) {
    // Cache owns table
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_result_rows(h: TableHandle) -> usize {
    if h.is_null() { 0 } else { unsafe { (*(h as *const DuckTable)).rows.len() } }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_result_cols(h: TableHandle) -> usize {
    if h.is_null() { 0 } else { unsafe { (*(h as *const DuckTable)).cols.len() } }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_col_name(h: TableHandle, idx: usize) -> *mut c_char {
    if h.is_null() { return std::ptr::null_mut(); }
    let t = unsafe { &*(h as *const DuckTable) };
    t.cols.get(idx).map(|s| to_c_str(s)).unwrap_or(std::ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_col_type(h: TableHandle, _idx: usize) -> u8 {
    if h.is_null() { return 0; }
    0 // All strings from CSV - type detection would need schema query
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_cell(h: TableHandle, row: usize, col: usize) -> CCell {
    if h.is_null() { return CCell::default(); }
    let t = unsafe { &*(h as *const DuckTable) };
    let val = t.rows.get(row).and_then(|r| r.0.get(col)).map(|s| s.as_str()).unwrap_or("");

    // Try to detect type
    if val.is_empty() || val == "NULL" {
        CCell::default()
    } else if let Ok(i) = val.parse::<i64>() {
        CCell { typ: CellType::Int, i, f: 0.0, s: std::ptr::null_mut() }
    } else if let Ok(f) = val.parse::<f64>() {
        CCell { typ: CellType::Float, i: 0, f, s: std::ptr::null_mut() }
    } else {
        CCell { typ: CellType::Str, i: 0, f: 0.0, s: to_c_str(val) }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_str_free(p: *mut c_char) {
    unsafe { free_c_str(p); }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_save(_prql: *const c_char, _path_in: *const c_char, _path_out: *const c_char) -> u8 {
    1 // Not implemented
}
