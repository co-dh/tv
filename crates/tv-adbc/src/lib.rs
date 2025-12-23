//! ADBC plugin for tabv - external database connectivity via Arrow
//! Supports any ADBC driver: PostgreSQL, SQLite, DuckDB, Snowflake, etc.
//!
//! Path format: adbc:driver://connection_string?table=tablename
//! Examples:
//!   adbc:sqlite:///path/to/file.db?table=users
//!   adbc:postgresql://localhost/mydb?table=orders
//!   adbc:duckdb:///data.duckdb?table=sales
//!   adbc:duckdb:///path/to/file.parquet  # direct file via DuckDB
//!   adbc:duckdb:///path/to/file.csv
//!   adbc:duckdb:///path/to/file.json


use std::ffi::{c_char, c_void};
use std::sync::OnceLock;
use tv_plugin_api::*;
use tv_plugin_api::prql::QueryCache;
use adbc_core::{Driver, Database, Connection, Statement};
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_driver_manager::ManagedDriver;
use arrow_array::*;
use arrow_schema::*;

// Query cache: (path, prql) -> QueryResult
static CACHE: OnceLock<QueryCache<QueryResult>> = OnceLock::new();
fn cache() -> &'static QueryCache<QueryResult> {
    CACHE.get_or_init(|| QueryCache::new(100))
}

/// Query result with schema and data
struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    rows: usize,
}

impl QueryResult {
    fn cols(&self) -> usize { self.schema.fields().len() }

    /// Get cell value at (row, col)
    fn cell(&self, row: usize, col: usize) -> CCell {
        let mut offset = 0;
        for batch in &self.batches {
            if row < offset + batch.num_rows() {
                return cell_from_batch(batch, row - offset, col);
            }
            offset += batch.num_rows();
        }
        CCell::default()
    }

    /// Column type: 0=str, 1=int, 2=float
    fn col_type(&self, col: usize) -> u8 {
        match self.schema.field(col).data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => 1,
            DataType::Float16 | DataType::Float32 | DataType::Float64 => 2,
            _ => 0,
        }
    }
}

/// Extract cell from Arrow batch
fn cell_from_batch(batch: &RecordBatch, row: usize, col: usize) -> CCell {
    let arr = batch.column(col);
    if arr.is_null(row) { return CCell::default(); }

    match arr.data_type() {
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            CCell { typ: CellType::Bool, i: a.value(row) as i64, f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::Int8 => {
            let a = arr.as_any().downcast_ref::<Int8Array>().unwrap();
            CCell { typ: CellType::Int, i: a.value(row) as i64, f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::Int16 => {
            let a = arr.as_any().downcast_ref::<Int16Array>().unwrap();
            CCell { typ: CellType::Int, i: a.value(row) as i64, f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            CCell { typ: CellType::Int, i: a.value(row) as i64, f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            CCell { typ: CellType::Int, i: a.value(row), f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::UInt8 => {
            let a = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
            CCell { typ: CellType::Int, i: a.value(row) as i64, f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::UInt16 => {
            let a = arr.as_any().downcast_ref::<UInt16Array>().unwrap();
            CCell { typ: CellType::Int, i: a.value(row) as i64, f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::UInt32 => {
            let a = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
            CCell { typ: CellType::Int, i: a.value(row) as i64, f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::UInt64 => {
            let a = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
            CCell { typ: CellType::Int, i: a.value(row) as i64, f: 0.0, s: std::ptr::null_mut() }
        }
        DataType::Float32 => {
            let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            CCell { typ: CellType::Float, i: 0, f: a.value(row) as f64, s: std::ptr::null_mut() }
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            CCell { typ: CellType::Float, i: 0, f: a.value(row), s: std::ptr::null_mut() }
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            CCell { typ: CellType::Str, i: 0, f: 0.0, s: to_c_str(a.value(row)) }
        }
        DataType::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            CCell { typ: CellType::Str, i: 0, f: 0.0, s: to_c_str(a.value(row)) }
        }
        _ => {
            // Fallback: format as string
            CCell { typ: CellType::Str, i: 0, f: 0.0, s: to_c_str("?") }
        }
    }
}

/// Get result from handle (cache owns the data)
fn get_result<F, T>(h: *mut c_void, f: F) -> T
where F: FnOnce(&QueryResult) -> T, T: Default {
    if h.is_null() { return T::default(); }
    let r = unsafe { &*(h as *const QueryResult) };
    f(r)
}

/// Parse ADBC path: adbc:driver://conn_string?table=name
/// Returns (driver_name, conn_string, table_name)
/// For parquet files via DuckDB: table_name is "read_parquet('path')"
fn parse_path(path: &str) -> Option<(String, String, String)> {
    // Strip adbc: prefix
    let path = path.strip_prefix("adbc:").unwrap_or(path);

    // Extract driver name (before ://)
    let (driver, rest) = path.split_once("://")?;

    // DuckDB direct file: adbc:duckdb:///path/to/file.{parquet,csv,json}
    // PRQL quotes path → DuckDB reads quoted path as file
    let exts = [".parquet", ".csv", ".json"];
    if driver == "duckdb" && exts.iter().any(|e| rest.ends_with(e)) {
        let driver_lib = "adbc_driver_duckdb".to_string();
        let conn_str = "duckdb://:memory:".to_string();
        return Some((driver_lib, conn_str, rest.to_string()));
    }

    // Extract table from ?table= param
    let (conn, table) = if let Some(idx) = rest.find("?table=") {
        let conn = &rest[..idx];
        let table = rest[idx + 7..].split('&').next().unwrap_or("");
        (conn, table)
    } else {
        (rest, "")
    };

    // Build driver library name
    let driver_lib = format!("adbc_driver_{}", driver);

    // Reconstruct connection string for the driver
    let conn_str = format!("{}://{}", driver, conn);

    Some((driver_lib, conn_str, table.to_string()))
}

/// Execute query via ADBC driver
fn query_adbc(driver_name: &str, conn_str: &str, sql: &str) -> Result<QueryResult, String> {
    // Resolve driver library path and entrypoint
    let (lib_path, entrypoint): (&str, Option<&[u8]>) = if driver_name == "adbc_driver_duckdb" {
        let paths = ["/usr/lib/libduckdb.so", "/usr/local/lib/libduckdb.so", "libduckdb.so"];
        let lib = paths.iter().find(|p| std::path::Path::new(p).exists())
            .ok_or("libduckdb.so not found")?;
        (*lib, Some(b"duckdb_adbc_init"))
    } else if driver_name == "adbc_driver_sqlite" {
        let paths = ["/usr/local/lib/libadbc_driver_sqlite.so", "/usr/lib/libadbc_driver_sqlite.so"];
        let lib = paths.iter().find(|p| std::path::Path::new(p).exists())
            .ok_or("libadbc_driver_sqlite.so not found")?;
        (*lib, Some(b"AdbcDriverInit"))
    } else {
        (driver_name, None)
    };

    // Load driver dynamically
    let mut driver = ManagedDriver::load_dynamic_from_filename(lib_path, entrypoint, AdbcVersion::V110)
        .map_err(|e| format!("Failed to load driver {}: {}", lib_path, e))?;

    // Create database - driver-specific options
    let opts: Vec<(OptionDatabase, OptionValue)> = if driver_name == "adbc_driver_duckdb" {
        let db_path = conn_str.strip_prefix("duckdb://").unwrap_or(conn_str);
        let db_path = if db_path == ":memory:" { "" } else { db_path };
        vec![(OptionDatabase::Other("path".into()), OptionValue::String(db_path.into()))]
    } else if driver_name == "adbc_driver_sqlite" {
        let db_path = conn_str.strip_prefix("sqlite://").unwrap_or(conn_str);
        vec![(OptionDatabase::Uri, OptionValue::String(db_path.into()))]
    } else {
        vec![(OptionDatabase::Uri, OptionValue::String(conn_str.into()))]
    };
    let database = driver.new_database_with_opts(opts)
        .map_err(|e| format!("Failed to create database: {}", e))?;

    // Connect
    let mut connection = database.new_connection()
        .map_err(|e| format!("Failed to connect: {}", e))?;

    // Create statement and execute
    let mut statement = connection.new_statement()
        .map_err(|e| format!("Failed to create statement: {}", e))?;

    statement.set_sql_query(sql)
        .map_err(|e| format!("Failed to set query: {}", e))?;

    let output = statement.execute()
        .map_err(|e| format!("Failed to execute: {}", e))?;

    // Collect results
    let schema = output.schema();
    let batches: Vec<RecordBatch> = output
        .map(|r| r.map_err(|e| format!("Batch error: {}", e)))
        .collect::<Result<Vec<_>, _>>()?;
    let rows = batches.iter().map(|b| b.num_rows()).sum();

    Ok(QueryResult { schema, batches, rows })
}

// === Plugin exports ===

/// Replace "df" with actual table name in SQL
fn replace_table(sql: &str, table: &str) -> String {
    sql.replace("\"df\"", &format!("\"{}\"", table))
       .replace(" df ", &format!(" \"{}\" ", table))
       .replace(" df\n", &format!(" \"{}\"\n", table))
       .replace("FROM df", &format!("FROM \"{}\"", table))
}

/// Query external database via ADBC (cached)
#[no_mangle]
pub extern "C" fn tv_query(prql_ptr: *const c_char, path_ptr: *const c_char) -> *mut c_void {
    if prql_ptr.is_null() || path_ptr.is_null() { return std::ptr::null_mut(); }
    let prql = unsafe { from_c_str(prql_ptr) };
    let path = unsafe { from_c_str(path_ptr) };

    // Parse path to get driver info
    let (driver, conn_str, table) = match parse_path(&path) {
        Some(p) => p,
        None => return std::ptr::null_mut(),
    };
    if table.is_empty() { return std::ptr::null_mut(); }

    cache().get_or_exec(&path, &prql, |sql| {
        dbg("ADBC", &format!("EXEC [{}] {}", driver.strip_prefix("adbc_driver_").unwrap_or(&driver),
            prql.lines().next().unwrap_or(&prql)));
        let sql = replace_table(sql, &table);
        match query_adbc(&driver, &conn_str, &sql) {
            Ok(r) => {
                dbg("ADBC", &format!("ok: {} rows", r.rows));
                Some(r)
            }
            Err(e) => {
                dbg("ADBC", &format!("err: {}", e));
                None
            }
        }
    }).unwrap_or(std::ptr::null()) as *mut c_void
}

/// No-op: cache owns the QueryResult
#[no_mangle]
pub extern "C" fn tv_result_free(_h: *mut c_void) {}

#[no_mangle]
pub extern "C" fn tv_result_rows(h: *mut c_void) -> usize {
    get_result(h, |r| r.rows)
}

#[no_mangle]
pub extern "C" fn tv_result_cols(h: *mut c_void) -> usize {
    get_result(h, |r| r.cols())
}

#[no_mangle]
pub extern "C" fn tv_col_name(h: *mut c_void, idx: usize) -> *mut c_char {
    let name = get_result(h, |r| r.schema.field(idx).name().clone());
    if name.is_empty() { std::ptr::null_mut() } else { to_c_str(&name) }
}

#[no_mangle]
pub extern "C" fn tv_col_type(h: *mut c_void, idx: usize) -> u8 {
    get_result(h, |r| r.col_type(idx))
}

#[no_mangle]
pub extern "C" fn tv_cell(h: *mut c_void, row: usize, col: usize) -> CCell {
    get_result(h, |r| r.cell(row, col))
}

#[no_mangle]
pub extern "C" fn tv_str_free(p: *mut c_char) {
    unsafe { free_c_str(p); }
}

/// Save query result to file (DuckDB COPY TO)
#[no_mangle]
pub extern "C" fn tv_save(prql_ptr: *const c_char, path_in_ptr: *const c_char, path_out_ptr: *const c_char) -> u8 {
    let prql = unsafe { from_c_str(prql_ptr) };
    let path_in = unsafe { from_c_str(path_in_ptr) };
    let path_out = unsafe { from_c_str(path_out_ptr) };

    // Only DuckDB supports COPY TO
    let (driver, conn_str, table) = match parse_path(&path_in) {
        Some(p) if p.0 == "adbc_driver_duckdb" => p,
        _ => return 1,
    };

    // Compile PRQL to SQL
    let sql = match prql::compile(&prql) {
        Some(s) => s.replace("\"df\"", &format!("\"{}\"", table))
                    .replace(" df ", &format!(" \"{}\" ", table))
                    .replace(" df\n", &format!(" \"{}\"\n", table))
                    .replace("FROM df", &format!("FROM \"{}\"", table)),
        None => return 1,
    };

    // Build COPY TO query
    let fmt = if path_out.ends_with(".parquet") || path_out.ends_with(".pq") {
        "PARQUET"
    } else {
        "CSV"
    };
    let copy_sql = format!("COPY ({}) TO '{}' (FORMAT {})", sql.trim_end_matches(';'), path_out, fmt);

    // Execute via DuckDB
    match query_adbc(&driver, &conn_str, &copy_sql) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

/// Plugin init - returns vtable
#[no_mangle]
pub extern "C" fn tv_plugin_init() -> PluginVtable {
    PluginVtable {
        version: PLUGIN_API_VERSION,
        name: b"adbc\0".as_ptr() as *const c_char,
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
