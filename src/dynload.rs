//! Plugin loader - loads backend .so and wraps in Rust-friendly API
//! Supports multiple plugins: polars for files, sqlite for memory tables.

use crate::table::{Cell, ColType, Table, BoxTable};
use libloading::{Library, Symbol};
use std::ffi::{CString, CStr, c_char};
use std::sync::OnceLock;
use tv_plugin_api::*;

static POLARS: OnceLock<Plugin> = OnceLock::new();
static SQLITE: OnceLock<SqlitePlugin> = OnceLock::new();

/// Sqlite plugin with register/unregister for memory tables
pub struct SqlitePlugin {
    plugin: Plugin,
    register: extern "C" fn(usize, *const *const c_char, *const u8, *const *const CCell, usize, usize),
    unregister: extern "C" fn(usize),
}

/// Load polars plugin (for file paths)
pub fn load_polars(path: &str) -> anyhow::Result<()> {
    let p = Plugin::load(path)?;
    POLARS.set(p).map_err(|_| anyhow::anyhow!("polars plugin already loaded"))?;
    Ok(())
}

/// Load sqlite plugin (for memory:id paths)
pub fn load_sqlite(path: &str) -> anyhow::Result<()> {
    let p = SqlitePlugin::load(path)?;
    SQLITE.set(p).map_err(|_| anyhow::anyhow!("sqlite plugin already loaded"))?;
    Ok(())
}

unsafe impl Send for SqlitePlugin {}
unsafe impl Sync for SqlitePlugin {}

impl SqlitePlugin {
    /// Load sqlite plugin with register/unregister symbols
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let lib = unsafe { Library::new(path) }?;
        let init: Symbol<extern "C" fn() -> PluginVtable> = unsafe { lib.get(b"tv_plugin_init") }?;
        let reg: Symbol<extern "C" fn(usize, *const *const c_char, *const u8, *const *const CCell, usize, usize)> =
            unsafe { lib.get(b"tv_register") }?;
        let unreg: Symbol<extern "C" fn(usize)> = unsafe { lib.get(b"tv_unregister") }?;
        let vt = init();
        if vt.version != PLUGIN_API_VERSION {
            anyhow::bail!("plugin version {} != {}", vt.version, PLUGIN_API_VERSION);
        }
        // Keep function pointers before moving lib
        let register = *reg;
        let unregister = *unreg;
        Ok(Self { plugin: Plugin { _lib: lib, vt }, register, unregister })
    }

    /// Query (delegate to inner plugin)
    pub fn query(&self, sql: &str, path: &str) -> Option<PluginTable<'_>> { self.plugin.query(sql, path) }
    pub fn fetch(&self, path: &str, offset: usize, limit: usize) -> Option<PluginTable<'_>> { self.plugin.fetch(path, offset, limit) }
    pub fn count(&self, path: &str) -> usize { self.plugin.count(path) }
}

/// Load plugin from path (legacy - loads polars)
pub fn load(path: &str) -> anyhow::Result<()> { load_polars(path) }

/// Get polars plugin
pub fn get() -> Option<&'static Plugin> { POLARS.get() }

/// Get sqlite plugin
pub fn get_sqlite() -> Option<&'static SqlitePlugin> { SQLITE.get() }

/// Get plugin for path (routes memory: to sqlite, files to polars)
pub fn get_for(path: &str) -> Option<&'static Plugin> {
    if path.starts_with("memory:") { SQLITE.get().map(|s| &s.plugin) } else { POLARS.get() }
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
    (p.register)(id, name_ptrs.as_ptr(), types.as_ptr(), row_ptrs.as_ptr(), rows, cols);
    Some(format!("memory:{}", id))
}

/// Unregister table from sqlite plugin
pub fn unregister_table(id: usize) {
    if let Some(p) = SQLITE.get() { (p.unregister)(id); }
}

/// Plugin wrapper
pub struct Plugin {
    _lib: Library,  // prevent unloading
    vt: PluginVtable,
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
        Ok(Self { _lib: lib, vt })
    }

    /// Execute SQL query on file, return table
    pub fn query(&self, sql: &str, path: &str) -> Option<PluginTable<'_>> {
        let sql = CString::new(sql).ok()?;
        let path = CString::new(path).ok()?;
        let h = (self.vt.query)(sql.as_ptr(), path.as_ptr());
        if h.is_null() { None }
        else { Some(PluginTable { h, vt: &self.vt }) }
    }

    /// Query helper - SELECT * with LIMIT/OFFSET
    pub fn fetch(&self, path: &str, offset: usize, limit: usize) -> Option<PluginTable<'_>> {
        self.query(&format!("SELECT * FROM df LIMIT {} OFFSET {}", limit, offset), path)
    }

    /// Query helper - SELECT * WHERE with LIMIT/OFFSET
    pub fn fetch_where(&self, path: &str, filter: &str, offset: usize, limit: usize) -> Option<PluginTable<'_>> {
        self.query(&format!("SELECT * FROM df WHERE {} LIMIT {} OFFSET {}", filter, limit, offset), path)
    }

    /// Query helper - COUNT(*)
    pub fn count(&self, path: &str) -> usize {
        self.query("SELECT count(*) as cnt FROM df", path)
            .and_then(|t| if t.rows() > 0 { Some(t.cell(0, 0)) } else { None })
            .and_then(|c| if let Cell::Int(n) = c { Some(n as usize) } else { None })
            .unwrap_or(0)
    }

    /// Query helper - COUNT(*) WHERE
    pub fn count_where(&self, path: &str, filter: &str) -> usize {
        self.query(&format!("SELECT count(*) as cnt FROM df WHERE {}", filter), path)
            .and_then(|t| if t.rows() > 0 { Some(t.cell(0, 0)) } else { None })
            .and_then(|c| if let Cell::Int(n) = c { Some(n as usize) } else { None })
            .unwrap_or(0)
    }

    /// Query helper - frequency table
    pub fn freq(&self, path: &str, cols: &str, filter: &str) -> Option<PluginTable<'_>> {
        let w = if filter.is_empty() { "TRUE" } else { filter };
        self.query(&format!(
            "SELECT {}, count(*) as Cnt FROM df WHERE {} GROUP BY {} ORDER BY Cnt DESC",
            cols, w, cols
        ), path)
    }

    /// Get schema (column names)
    pub fn schema(&self, path: &str) -> Vec<String> {
        self.query("SELECT * FROM df LIMIT 0", path)
            .map(|t| t.col_names())
            .unwrap_or_default()
    }

    /// Get distinct values for column (for hints)
    pub fn distinct(&self, path: &str, col: &str) -> Option<Vec<String>> {
        let t = self.query(&format!("SELECT DISTINCT \"{}\" FROM df LIMIT 500", col), path)?;
        Some((0..t.rows()).map(|r| t.cell(r, 0).format(10)).collect())
    }
}

/// Table from plugin query result
pub struct PluginTable<'a> {
    h: TableHandle,
    vt: &'a PluginVtable,
}

// SAFETY: Plugin handles are thread-safe (single-threaded access in TUI)
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
    use crate::table::SimpleTable;
    let names = pt.col_names();
    let types = (0..pt.cols()).map(|i| pt.col_type(i)).collect();
    let data: Vec<Vec<Cell>> = (0..pt.rows())
        .map(|r| (0..pt.cols()).map(|c| pt.cell(r, c)).collect())
        .collect();
    Box::new(SimpleTable::new(names, types, data))
}
