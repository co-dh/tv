//! Plugin loader - loads backend .so and wraps in Rust-friendly API
//! Supports multiple plugins: polars for files, sqlite for memory tables.

use crate::table::{Cell, ColType, Table, BoxTable};
use libloading::{Library, Symbol};
use std::ffi::{CString, CStr};
use std::sync::OnceLock;
use tv_plugin_api::*;

static POLARS: OnceLock<Plugin> = OnceLock::new();
static SQLITE: OnceLock<Plugin> = OnceLock::new();

/// Load polars plugin (for file paths)
pub fn load_polars(path: &str) -> anyhow::Result<()> {
    let p = Plugin::load(path)?;
    POLARS.set(p).map_err(|_| anyhow::anyhow!("polars plugin already loaded"))?;
    Ok(())
}

/// Load sqlite plugin (for memory:id paths)
pub fn load_sqlite(path: &str) -> anyhow::Result<()> {
    let p = Plugin::load(path)?;
    SQLITE.set(p).map_err(|_| anyhow::anyhow!("sqlite plugin already loaded"))?;
    Ok(())
}

/// Load plugin from path (legacy - loads polars)
pub fn load(path: &str) -> anyhow::Result<()> { load_polars(path) }

/// Get polars plugin
pub fn get() -> Option<&'static Plugin> { POLARS.get() }

/// Get plugin for path (routes memory: to sqlite, files to polars)
pub fn get_for(path: &str) -> Option<&'static Plugin> {
    if path.starts_with("memory:") { SQLITE.get() } else { POLARS.get() }
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
