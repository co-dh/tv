//! Test plugin loading

use std::ffi::CString;
use libloading::{Library, Symbol};
use tv_plugin_api::*;

#[test]
fn test_polars_plugin_query() {
    // Load plugin
    let lib = unsafe { Library::new("target/release/libtv_polars.so") }.expect("load plugin");
    let init: Symbol<extern "C" fn() -> PluginVtable> = unsafe { lib.get(b"tv_plugin_init") }.expect("get init");
    let vt = init();

    assert_eq!(vt.version, PLUGIN_API_VERSION);

    // Query parquet file
    let sql = CString::new("SELECT * FROM df LIMIT 5").unwrap();
    let path = CString::new("tests/data/sample.parquet").unwrap();
    let h = (vt.query)(sql.as_ptr(), path.as_ptr());
    assert!(!h.is_null(), "query should return table");

    let rows = (vt.result_rows)(h);
    let cols = (vt.result_cols)(h);
    assert!(rows <= 5, "should have <= 5 rows");
    assert!(cols > 0, "should have columns");

    // Get first column name
    let name_ptr = (vt.col_name)(h, 0);
    assert!(!name_ptr.is_null());
    let name = unsafe { std::ffi::CStr::from_ptr(name_ptr).to_string_lossy().into_owned() };
    (vt.str_free)(name_ptr);
    assert!(!name.is_empty());

    // Get a cell
    let _cell = (vt.cell)(h, 0, 0);
    // Just check it doesn't crash

    (vt.result_free)(h);
}

#[test]
fn test_plugin_csv_query() {
    // Load plugin
    let lib = unsafe { Library::new("target/release/libtv_polars.so") }.expect("load plugin");
    let init: Symbol<extern "C" fn() -> PluginVtable> = unsafe { lib.get(b"tv_plugin_init") }.expect("get init");
    let vt = init();

    // Query CSV file
    let sql = CString::new("SELECT * FROM df LIMIT 3").unwrap();
    let path = CString::new("tests/data/sample.csv").unwrap();
    let h = (vt.query)(sql.as_ptr(), path.as_ptr());
    assert!(!h.is_null(), "CSV query should return table");

    let rows = (vt.result_rows)(h);
    assert!(rows <= 3, "should have <= 3 rows");

    (vt.result_free)(h);
}

#[test]
fn test_plugin_freq_query() {
    // Load plugin
    let lib = unsafe { Library::new("target/release/libtv_polars.so") }.expect("load plugin");
    let init: Symbol<extern "C" fn() -> PluginVtable> = unsafe { lib.get(b"tv_plugin_init") }.expect("get init");
    let vt = init();

    // Freq query - GROUP BY
    let sql = CString::new("SELECT cat_city, count(*) as Cnt FROM df GROUP BY cat_city ORDER BY Cnt DESC LIMIT 10").unwrap();
    let path = CString::new("tests/data/sample.parquet").unwrap();
    let h = (vt.query)(sql.as_ptr(), path.as_ptr());
    assert!(!h.is_null(), "freq query should return table");

    let rows = (vt.result_rows)(h);
    let cols = (vt.result_cols)(h);
    assert!(rows > 0, "should have rows");
    assert_eq!(cols, 2, "should have 2 cols: cat_city, Cnt");

    (vt.result_free)(h);
}
