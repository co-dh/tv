//! Plugin API for tv backends
//! Simple interface: query(sql, path) -> table

use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr;

pub use lru::LruCache;

pub const PLUGIN_API_VERSION: u32 = 1;

/// Cell type tag
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CellType { Null = 0, Bool = 1, Int = 2, Float = 3, Str = 4 }

/// Cell value (tagged union)
#[repr(C)]
pub struct CCell {
    pub typ: CellType,
    pub i: i64,           // Bool (0/1), Int
    pub f: f64,           // Float
    pub s: *mut c_char,   // Str (caller frees)
}

impl Default for CCell {
    fn default() -> Self { Self { typ: CellType::Null, i: 0, f: 0.0, s: ptr::null_mut() } }
}

/// Opaque table handle
pub type TableHandle = *mut c_void;

/// Plugin vtable - C ABI interface for data backends
///
/// Design: query() returns opaque handle to result kept in plugin memory.
/// Accessor functions (table_rows, cell, etc.) extract data from handle on demand.
/// This avoids copying large results across FFI boundary - only fetch what's needed.
#[repr(C)]
pub struct PluginVtable {
    pub version: u32,
    pub name: *const c_char,
    /// Execute SQL on source, return handle to result (null on error).
    /// Result stays in plugin memory until result_free() is called.
    /// sql: SQL query (table name is "df")
    /// path: file path or "memory:id" for registered tables
    pub query: extern "C" fn(sql: *const c_char, path: *const c_char) -> TableHandle,
    /// Release result handle and free associated memory
    pub result_free: extern "C" fn(TableHandle),
    /// Number of rows in query result
    pub result_rows: extern "C" fn(TableHandle) -> usize,
    /// Number of columns in query result
    pub result_cols: extern "C" fn(TableHandle) -> usize,
    /// Get column name by index (caller must free with str_free)
    pub col_name: extern "C" fn(TableHandle, usize) -> *mut c_char,
    /// Get column type: 0=str, 1=int, 2=float
    pub col_type: extern "C" fn(TableHandle, usize) -> u8,
    /// Get cell value at (row, col) in result
    pub cell: extern "C" fn(TableHandle, usize, usize) -> CCell,
    /// Free string returned by col_name or cell
    pub str_free: extern "C" fn(*mut c_char),
    /// Save query result to file (parquet/csv). Returns 0 on success, 1 on error.
    /// sql: SQL query, path_in: source file, path_out: destination file
    pub save: extern "C" fn(sql: *const c_char, path_in: *const c_char, path_out: *const c_char) -> u8,
}

// === C string helpers ===

#[inline]
pub fn to_c_str(s: &str) -> *mut c_char {
    CString::new(s).map(|c| c.into_raw()).unwrap_or(ptr::null_mut())
}

#[inline]
pub unsafe fn from_c_str(p: *const c_char) -> String {
    if p.is_null() { String::new() }
    else { unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() } }
}

#[inline]
pub unsafe fn free_c_str(p: *mut c_char) {
    if !p.is_null() { unsafe { drop(CString::from_raw(p)); } }
}
