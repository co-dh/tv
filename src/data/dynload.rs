//! Plugin loader - loads backend .so and wraps in Rust-friendly API
//! Supports multiple plugins: polars for files, sqlite for memory tables.

use super::table::{Cell, ColType, Table, BoxTable};
use libloading::{Library, Symbol};
use std::ffi::{CString, CStr, c_char};
use std::sync::OnceLock;
use tv_plugin_api::*;

static POLARS: OnceLock<Plugin> = OnceLock::new();
static SQLITE: OnceLock<Plugin> = OnceLock::new();

/// Plugin wrapper - unified for all backends
pub struct Plugin {
    _lib: Library,
    vt: PluginVtable,
    // Optional register for memory tables (sqlite)
    register_fn: Option<extern "C" fn(usize, *const *const c_char, *const u8, *const *const CCell, usize, usize)>,
}

unsafe impl Send for Plugin {}
unsafe impl Sync for Plugin {}

impl Plugin {
    /// Load plugin from .so path
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let lib = unsafe { Library::new(path) }?;
        let init: Symbol<extern "C" fn() -> PluginVtable> = unsafe { lib.get(b"tv_plugin_init") }?;
        let vt = init();
        if vt.version != PLUGIN_API_VERSION {
            anyhow::bail!("plugin version {} != {}", vt.version, PLUGIN_API_VERSION);
        }
        // Try to load optional register symbol
        let register_fn = unsafe { lib.get::<extern "C" fn(usize, *const *const c_char, *const u8, *const *const CCell, usize, usize)>(b"tv_register") }
            .ok().map(|s| *s);
        Ok(Self { _lib: lib, vt, register_fn })
    }

    /// Check if plugin supports memory table registration
    pub fn can_register(&self) -> bool { self.register_fn.is_some() }

    /// Execute SQL query on file, return table
    pub fn query(&self, sql: &str, path: &str) -> Option<PluginTable<'_>> {
        let sql = CString::new(sql).ok()?;
        let path = CString::new(path).ok()?;
        let h = (self.vt.query)(sql.as_ptr(), path.as_ptr());
        if h.is_null() { None } else { Some(PluginTable { h, vt: &self.vt }) }
    }

    /// Register memory table (sqlite only)
    fn register(&self, id: usize, names: &[*const c_char], types: &[u8], rows: &[*const CCell], n_rows: usize, n_cols: usize) {
        if let Some(f) = self.register_fn {
            f(id, names.as_ptr(), types.as_ptr(), rows.as_ptr(), n_rows, n_cols);
        }
    }
}

/// Load polars plugin (for file paths)
pub fn load_polars(path: &str) -> anyhow::Result<()> {
    let p = Plugin::load(path)?;
    POLARS.set(p).map_err(|_| anyhow::anyhow!("polars plugin already loaded"))?;
    Ok(())
}

/// Load sqlite plugin (for memory:id paths)
pub fn load_sqlite(path: &str) -> anyhow::Result<()> {
    let p = Plugin::load(path)?;
    if !p.can_register() {
        anyhow::bail!("sqlite plugin missing register/unregister symbols");
    }
    SQLITE.set(p).map_err(|_| anyhow::anyhow!("sqlite plugin already loaded"))?;
    Ok(())
}

/// Get polars plugin
pub fn get() -> Option<&'static Plugin> { POLARS.get() }

/// Get sqlite plugin
pub fn get_sqlite() -> Option<&'static Plugin> { SQLITE.get() }

/// Get plugin for path (routes memory:/source: to sqlite, files to polars)
pub fn get_for(path: &str) -> Option<&'static Plugin> {
    if path.starts_with("memory:") || path.starts_with("source:") { SQLITE.get() } else { POLARS.get() }
}

/// Register in-memory table with sqlite plugin for querying
/// Returns path "memory:id" for later queries
pub fn register_table(id: usize, t: &dyn Table) -> Option<String> {
    let p = SQLITE.get()?;
    let cols = t.cols();
    let rows = t.rows();

    // Convert names to C strings
    let names: Vec<CString> = (0..cols).map(|c| CString::new(t.col_name(c).unwrap_or_default()).unwrap()).collect();
    let name_ptrs: Vec<*const c_char> = names.iter().map(|s| s.as_ptr()).collect();

    // Convert types (0=str, 1=int, 2=float)
    let types: Vec<u8> = (0..cols).map(|c| match t.col_type(c) {
        ColType::Int => 1, ColType::Float => 2, _ => 0
    }).collect();

    // Convert cells to CCell format
    let mut cell_data: Vec<Vec<CCell>> = Vec::with_capacity(rows);
    let mut strings: Vec<Vec<CString>> = Vec::with_capacity(rows); // keep alive
    for r in 0..rows {
        let mut row_cells = Vec::with_capacity(cols);
        let mut row_strs = Vec::new();
        for c in 0..cols {
            let ccell = match t.cell(r, c) {
                Cell::Null => CCell::default(),
                Cell::Bool(b) => CCell { typ: CellType::Bool, i: b as i64, f: 0.0, s: std::ptr::null_mut() },
                Cell::Int(n) => CCell { typ: CellType::Int, i: n, f: 0.0, s: std::ptr::null_mut() },
                Cell::Float(f) => CCell { typ: CellType::Float, i: 0, f, s: std::ptr::null_mut() },
                Cell::Str(s) | Cell::Date(s) | Cell::Time(s) | Cell::DateTime(s) => {
                    let cs = CString::new(s).unwrap_or_default();
                    let ptr = cs.as_ptr() as *mut c_char;
                    row_strs.push(cs);
                    CCell { typ: CellType::Str, i: 0, f: 0.0, s: ptr }
                }
            };
            row_cells.push(ccell);
        }
        cell_data.push(row_cells);
        strings.push(row_strs);
    }

    // Create row pointers
    let row_ptrs: Vec<*const CCell> = cell_data.iter().map(|r| r.as_ptr()).collect();

    // Register with sqlite
    p.register(id, &name_ptrs, &types, &row_ptrs, rows, cols);
    Some(format!("memory:{}", id))
}

/// Table from plugin query result
pub struct PluginTable<'a> {
    h: TableHandle,
    vt: &'a PluginVtable,
}

unsafe impl Send for PluginTable<'_> {}
unsafe impl Sync for PluginTable<'_> {}

impl Drop for PluginTable<'_> {
    fn drop(&mut self) { (self.vt.result_free)(self.h); }
}

impl Table for PluginTable<'_> {
    fn rows(&self) -> usize { (self.vt.result_rows)(self.h) }
    fn cols(&self) -> usize { (self.vt.result_cols)(self.h) }

    fn col_name(&self, idx: usize) -> Option<String> {
        let p = (self.vt.col_name)(self.h, idx);
        if p.is_null() { None }
        else {
            let s = unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() };
            (self.vt.str_free)(p);
            Some(s)
        }
    }

    fn col_names(&self) -> Vec<String> {
        (0..self.cols()).filter_map(|i| self.col_name(i)).collect()
    }

    fn col_type(&self, idx: usize) -> ColType {
        match (self.vt.col_type)(self.h, idx) {
            1 => ColType::Int,
            2 => ColType::Float,
            _ => ColType::Str,
        }
    }

    fn cell(&self, row: usize, col: usize) -> Cell {
        let c = (self.vt.cell)(self.h, row, col);
        match c.typ {
            CellType::Null => Cell::Null,
            CellType::Bool => Cell::Bool(c.i != 0),
            CellType::Int => Cell::Int(c.i),
            CellType::Float => Cell::Float(c.f),
            CellType::Str => {
                if c.s.is_null() { Cell::Str(String::new()) }
                else {
                    let s = unsafe { CStr::from_ptr(c.s).to_string_lossy().into_owned() };
                    (self.vt.str_free)(c.s);
                    Cell::Str(s)
                }
            }
        }
    }

    fn col_width(&self, idx: usize, sample: usize) -> usize {
        let header = self.col_name(idx).map(|s| s.len()).unwrap_or(0);
        let max_data = (0..sample.min(self.rows()))
            .map(|r| self.cell(r, idx).format(3).len())
            .max().unwrap_or(0);
        header.max(max_data).max(3)
    }
}

/// Convert PluginTable to BoxTable (clones data)
pub fn to_box_table(pt: &PluginTable) -> BoxTable {
    use super::table::SimpleTable;
    let names = pt.col_names();
    let types = (0..pt.cols()).map(|i| pt.col_type(i)).collect();
    let data: Vec<Vec<Cell>> = (0..pt.rows())
        .map(|r| (0..pt.cols()).map(|c| pt.cell(r, c)).collect())
        .collect();
    Box::new(SimpleTable::new(names, types, data))
}
