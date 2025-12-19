//! Polars backend plugin - executes SQL queries

use polars::prelude::*;
use std::ffi::c_char;
use std::sync::Mutex;
use tv_plugin_api::*;

/// Cache: (path, sql) -> DataFrame
static CACHE: Mutex<Option<CacheEntry>> = Mutex::new(None);

struct CacheEntry {
    path: String,
    sql: String,
    df: Box<DataFrame>,
}

static NAME: &[u8] = b"polars\0";

/// Plugin entry point
#[unsafe(no_mangle)]
pub extern "C" fn tv_plugin_init() -> PluginVtable {
    PluginVtable {
        version: PLUGIN_API_VERSION,
        name: NAME.as_ptr() as *const c_char,
        query: tv_query,
        result_free: tv_result_free,
        result_rows: tv_result_rows,
        result_cols: tv_result_cols,
        col_name: tv_col_name,
        col_type: tv_col_type,
        cell: tv_cell,
        str_free: tv_str_free,
    }
}

/// Execute SQL on file (cached)
#[unsafe(no_mangle)]
pub extern "C" fn tv_query(sql: *const c_char, path: *const c_char) -> TableHandle {
    if sql.is_null() || path.is_null() { return std::ptr::null_mut(); }
    let sql = unsafe { from_c_str(sql) };
    let path = unsafe { from_c_str(path) };

    // Check cache
    if let Ok(guard) = CACHE.lock() {
        if let Some(ref e) = *guard {
            if e.path == path && e.sql == sql {
                return e.df.as_ref() as *const DataFrame as TableHandle;
            }
        }
    }

    // Execute query
    let lf = if path.ends_with(".parquet") {
        LazyFrame::scan_parquet(PlPath::new(&path), Default::default()).ok()
    } else if path.ends_with(".csv") {
        CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(path.clone().into()))
            .and_then(|r| r.finish())
            .map(|df| df.lazy())
            .ok()
    } else { None };

    let df = lf.and_then(|lf| exec_sql(lf, &sql).ok());
    if let Some(df) = df {
        // Store in cache
        if let Ok(mut guard) = CACHE.lock() {
            let entry = CacheEntry { path, sql, df: Box::new(df) };
            let ptr = entry.df.as_ref() as *const DataFrame as TableHandle;
            *guard = Some(entry);
            return ptr;
        }
    }
    std::ptr::null_mut()
}

fn exec_sql(lf: LazyFrame, sql: &str) -> PolarsResult<DataFrame> {
    let mut ctx = polars::sql::SQLContext::new();
    ctx.register("df", lf);
    ctx.execute(sql)?.collect()
}

/// No-op: cache owns the DataFrame
#[unsafe(no_mangle)]
pub extern "C" fn tv_result_free(_h: TableHandle) {
    // Cache owns df, don't free
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_result_rows(h: TableHandle) -> usize {
    if h.is_null() { 0 } else { unsafe { (*(h as *const DataFrame)).height() } }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_result_cols(h: TableHandle) -> usize {
    if h.is_null() { 0 } else { unsafe { (*(h as *const DataFrame)).width() } }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_col_name(h: TableHandle, idx: usize) -> *mut c_char {
    if h.is_null() { return std::ptr::null_mut(); }
    let df = unsafe { &*(h as *const DataFrame) };
    df.get_column_names().get(idx).map(|n| to_c_str(n)).unwrap_or(std::ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_col_type(h: TableHandle, idx: usize) -> u8 {
    if h.is_null() { return 0; }
    let df = unsafe { &*(h as *const DataFrame) };
    df.get_columns().get(idx).map(|c| match c.dtype() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => 1,
        DataType::Float32 | DataType::Float64 => 2,
        _ => 0,
    }).unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_cell(h: TableHandle, row: usize, col: usize) -> CCell {
    if h.is_null() { return CCell::default(); }
    let df = unsafe { &*(h as *const DataFrame) };
    df.get_columns().get(col)
        .and_then(|c| c.get(row).ok())
        .map(|v| match v {
            AnyValue::Null => CCell::default(),
            AnyValue::Boolean(b) => CCell { typ: CellType::Bool, i: b as i64, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::Int8(n) => CCell { typ: CellType::Int, i: n as i64, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::Int16(n) => CCell { typ: CellType::Int, i: n as i64, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::Int32(n) => CCell { typ: CellType::Int, i: n as i64, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::Int64(n) => CCell { typ: CellType::Int, i: n, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::UInt8(n) => CCell { typ: CellType::Int, i: n as i64, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::UInt16(n) => CCell { typ: CellType::Int, i: n as i64, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::UInt32(n) => CCell { typ: CellType::Int, i: n as i64, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::UInt64(n) => CCell { typ: CellType::Int, i: n as i64, f: 0.0, s: std::ptr::null_mut() },
            AnyValue::Float32(f) => CCell { typ: CellType::Float, i: 0, f: f as f64, s: std::ptr::null_mut() },
            AnyValue::Float64(f) => CCell { typ: CellType::Float, i: 0, f, s: std::ptr::null_mut() },
            AnyValue::String(s) => CCell { typ: CellType::Str, i: 0, f: 0.0, s: to_c_str(s) },
            AnyValue::StringOwned(s) => CCell { typ: CellType::Str, i: 0, f: 0.0, s: to_c_str(s.as_str()) },
            _ => CCell { typ: CellType::Str, i: 0, f: 0.0, s: to_c_str(&v.to_string()) },
        })
        .unwrap_or_default()
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_str_free(p: *mut c_char) {
    unsafe { free_c_str(p); }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_parquet_time_roundtrip() {
        let pq = "../../tmp/tv_pq_time_test.parquet";
        fs::create_dir_all("../../tmp").ok();

        let ns: Vec<i64> = vec![3600_000_000_000, 7200_000_000_000, 10800_000_000_000];
        let time_series = Series::new("event_time".into(), ns)
            .cast(&DataType::Time).unwrap();
        let mut df = DataFrame::new(vec![time_series.into()]).unwrap();

        ParquetWriter::new(fs::File::create(pq).unwrap())
            .finish(&mut df).unwrap();

        let loaded = ParquetReader::new(fs::File::open(pq).unwrap())
            .finish().unwrap();

        assert!(matches!(loaded.column("event_time").unwrap().dtype(), DataType::Time),
            "Time col should remain Time: {:?}", loaded.column("event_time").unwrap().dtype());
        fs::remove_file(pq).ok();
    }

    #[test]
    fn test_csv_query() {
        let csv = "../../tmp/tv_csv_test.csv";
        fs::create_dir_all("../../tmp").ok();
        fs::write(csv, "a,b\n1,x\n2,y\n3,z").unwrap();

        let sql = std::ffi::CString::new("SELECT * FROM df").unwrap();
        let path = std::ffi::CString::new(csv).unwrap();
        let h = tv_query(sql.as_ptr(), path.as_ptr());
        assert!(!h.is_null(), "Query should return result");
        assert_eq!(tv_result_rows(h), 3);
        assert_eq!(tv_result_cols(h), 2);
        tv_result_free(h);
        fs::remove_file(csv).ok();
    }
}
