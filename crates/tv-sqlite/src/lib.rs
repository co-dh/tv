//! SQLite backend plugin - executes SQL on in-memory tables via virtual tables
//! Supports: "memory:id" for registered tables, "source:type:args" for system sources.

mod source;

/// Debug log to ~/.tv/debug.log
fn dbg(msg: &str) {
    use std::io::Write;
    let Some(home) = dirs::home_dir() else { return };
    let log = home.join(".tv").join("debug.log");
    let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(log) else { return };
    let ts = chrono::Local::now().format("%H:%M:%S%.3f");
    let _ = writeln!(f, "[{}] {}", ts, msg);
}

use rusqlite::ffi;
use rusqlite::vtab::{read_only_module, Context, CreateVTab, IndexInfo, VTab, VTabConfig, VTabConnection, VTabCursor, VTabKind, Values};
use rusqlite::{Connection, Error as SqlError, Result as SqlResult};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_char;
use std::marker::PhantomData;
use std::os::raw::c_int;
use std::sync::{Arc, Mutex, OnceLock};
use tv_plugin_api::*;

static NAME: &[u8] = b"sqlite\0";

// ── Table Registry ───────────────────────────────────────────────────────────

/// Cell value (mirrors tv Cell for internal use)
#[derive(Clone, Debug)]
pub enum Cell { Null, Bool(bool), Int(i64), Float(f64), Str(String) }

/// Column type
#[derive(Clone, Copy, Debug)]
pub enum ColType { Str, Int, Float, Bool }

/// Simple in-memory table
#[derive(Clone)]
pub struct SimpleTable {
    pub names: Vec<String>,
    pub types: Vec<ColType>,
    pub data: Vec<Vec<Cell>>,
}

impl SimpleTable {
    pub fn new(names: Vec<String>, types: Vec<ColType>, data: Vec<Vec<Cell>>) -> Self {
        Self { names, types, data }
    }
    pub fn rows(&self) -> usize { self.data.len() }
    pub fn cols(&self) -> usize { self.names.len() }
}

/// Global table registry: id -> SimpleTable
static REGISTRY: OnceLock<Mutex<HashMap<usize, Arc<SimpleTable>>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<usize, Arc<SimpleTable>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

// ── Source Cache ─────────────────────────────────────────────────────────────

/// Cached source table with timestamp
struct CachedSource { table: Arc<SimpleTable>, ts: std::time::Instant }

/// Cache for source:... paths
static SOURCE_CACHE: OnceLock<Mutex<HashMap<String, CachedSource>>> = OnceLock::new();

fn source_cache() -> &'static Mutex<HashMap<String, CachedSource>> {
    SOURCE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// TTL for source caches (seconds): static sources get longer cache
fn source_ttl(path: &str) -> u64 {
    match path {
        p if p.starts_with("source:pacman") => 600,   // 10 min (slow, static)
        p if p.starts_with("source:cargo") => 300,    // 5 min (slow, static)
        p if p.starts_with("source:systemctl") => 60, // 1 min
        p if p.starts_with("source:ls") => 10,        // 10 sec (dir may change)
        p if p.starts_with("source:lr") => 30,        // 30 sec
        _ => 5,                                        // 5 sec for dynamic (ps, tcp, etc.)
    }
}

/// Get cached source table or generate new one
fn get_source(path: &str) -> Option<Arc<SimpleTable>> {
    let mut cache = source_cache().lock().ok()?;
    let ttl = std::time::Duration::from_secs(source_ttl(path));

    // Return cached if fresh
    if let Some(entry) = cache.get(path) {
        if entry.ts.elapsed() < ttl {
            dbg(&format!("SRC CACHE HIT {} {}x{}", path, entry.table.rows(), entry.table.cols()));
            return Some(entry.table.clone());
        }
    }

    // Generate new and cache
    dbg(&format!("SRC CACHE MISS {}", path));
    let table = Arc::new(source::query(path)?);
    cache.insert(path.to_string(), CachedSource { table: table.clone(), ts: std::time::Instant::now() });
    Some(table)
}

// ── Query Result Cache ───────────────────────────────────────────────────────

/// Cached query result: (path, sql) -> result table
struct QueryCacheEntry { path: String, sql: String, result: Box<SimpleTable> }

/// Single-entry query cache (like polars plugin)
static QUERY_CACHE: OnceLock<Mutex<Option<QueryCacheEntry>>> = OnceLock::new();

fn query_cache() -> &'static Mutex<Option<QueryCacheEntry>> {
    QUERY_CACHE.get_or_init(|| Mutex::new(None))
}

/// Register table for "memory:id" path access
#[unsafe(no_mangle)]
pub extern "C" fn tv_register(id: usize, names: *const *const c_char, types: *const u8,
                               data: *const *const CCell, rows: usize, cols: usize) {
    if names.is_null() || types.is_null() || data.is_null() { return; }

    let names: Vec<String> = (0..cols).map(|i| unsafe { from_c_str(*names.add(i)) }).collect();
    let types: Vec<ColType> = (0..cols).map(|i| unsafe {
        match *types.add(i) { 1 => ColType::Int, 2 => ColType::Float, _ => ColType::Str }
    }).collect();
    let data: Vec<Vec<Cell>> = (0..rows).map(|r| {
        (0..cols).map(|c| unsafe {
            let cell = &*(*data.add(r)).add(c);
            match cell.typ {
                CellType::Null => Cell::Null,
                CellType::Bool => Cell::Bool(cell.i != 0),
                CellType::Int => Cell::Int(cell.i),
                CellType::Float => Cell::Float(cell.f),
                CellType::Str => Cell::Str(from_c_str(cell.s)),
            }
        }).collect()
    }).collect();

    registry().lock().unwrap().insert(id, Arc::new(SimpleTable::new(names, types, data)));
}

/// Unregister table
#[unsafe(no_mangle)]
pub extern "C" fn tv_unregister(id: usize) {
    registry().lock().unwrap().remove(&id);
}

/// Get table from registry
fn get_registered(id: usize) -> Option<Arc<SimpleTable>> {
    registry().lock().unwrap().get(&id).cloned()
}

/// Parse "memory:id" path
fn parse_path(path: &str) -> Option<usize> {
    path.strip_prefix("memory:").and_then(|s| s.parse().ok())
}

// ── Thread-local table for virtual table access ──────────────────────────────

thread_local! {
    static CURRENT: RefCell<Option<Arc<SimpleTable>>> = const { RefCell::new(None) };
}

fn set_current(t: Arc<SimpleTable>) { CURRENT.with(|c| *c.borrow_mut() = Some(t)); }
fn get_current() -> Option<Arc<SimpleTable>> { CURRENT.with(|c| c.borrow().clone()) }
fn clear_current() { CURRENT.with(|c| *c.borrow_mut() = None); }

// ── Plugin Vtable ────────────────────────────────────────────────────────────

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

/// Execute SQL on table (memory:id or source:type:args) - cached
#[unsafe(no_mangle)]
pub extern "C" fn tv_query(sql: *const c_char, path: *const c_char) -> TableHandle {
    if sql.is_null() || path.is_null() { return std::ptr::null_mut(); }
    let sql = unsafe { from_c_str(sql) };
    let path = unsafe { from_c_str(path) };
    dbg(&format!("QUERY path={} sql={}", path, sql));

    // Check query cache first
    if let Ok(guard) = query_cache().lock() {
        if let Some(ref e) = *guard {
            if e.path == path && e.sql == sql {
                dbg(&format!("QCACHE HIT {}x{}", e.result.rows(), e.result.cols()));
                return e.result.as_ref() as *const SimpleTable as TableHandle;
            }
        }
    }
    dbg("QCACHE MISS");

    // Get source table
    let table: Arc<SimpleTable> = if path.starts_with("source:") {
        match get_source(&path) { Some(t) => t, None => return std::ptr::null_mut() }
    } else {
        let id = match parse_path(&path) { Some(id) => id, None => return std::ptr::null_mut() };
        match get_registered(id) { Some(t) => t, None => return std::ptr::null_mut() }
    };
    dbg(&format!("SOURCE {}x{}", table.rows(), table.cols()));

    // Execute SQL and cache result
    let result = match exec_sql(&table, &sql) { Ok(r) => r, Err(e) => { dbg(&format!("SQL ERR {}", e)); return std::ptr::null_mut(); } };
    dbg(&format!("RESULT {}x{}", result.rows(), result.cols()));
    if let Ok(mut guard) = query_cache().lock() {
        let entry = QueryCacheEntry { path, sql, result: Box::new(result) };
        let ptr = entry.result.as_ref() as *const SimpleTable as TableHandle;
        *guard = Some(entry);
        return ptr;
    }
    std::ptr::null_mut()
}

/// No-op: query cache owns the result
#[unsafe(no_mangle)]
pub extern "C" fn tv_result_free(_h: TableHandle) {}

#[unsafe(no_mangle)]
pub extern "C" fn tv_result_rows(h: TableHandle) -> usize {
    if h.is_null() { 0 } else { unsafe { (*(h as *const SimpleTable)).rows() } }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_result_cols(h: TableHandle) -> usize {
    if h.is_null() { 0 } else { unsafe { (*(h as *const SimpleTable)).cols() } }
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_col_name(h: TableHandle, idx: usize) -> *mut c_char {
    if h.is_null() { return std::ptr::null_mut(); }
    let t = unsafe { &*(h as *const SimpleTable) };
    t.names.get(idx).map(|n| to_c_str(n)).unwrap_or(std::ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_col_type(h: TableHandle, idx: usize) -> u8 {
    if h.is_null() { return 0; }
    let t = unsafe { &*(h as *const SimpleTable) };
    t.types.get(idx).map(|t| match t {
        ColType::Int => 1,
        ColType::Float => 2,
        _ => 0,
    }).unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_cell(h: TableHandle, row: usize, col: usize) -> CCell {
    if h.is_null() { return CCell::default(); }
    let t = unsafe { &*(h as *const SimpleTable) };
    t.data.get(row).and_then(|r| r.get(col)).map(|c| match c {
        Cell::Null => CCell::default(),
        Cell::Bool(b) => CCell { typ: CellType::Bool, i: *b as i64, f: 0.0, s: std::ptr::null_mut() },
        Cell::Int(n) => CCell { typ: CellType::Int, i: *n, f: 0.0, s: std::ptr::null_mut() },
        Cell::Float(f) => CCell { typ: CellType::Float, i: 0, f: *f, s: std::ptr::null_mut() },
        Cell::Str(s) => CCell { typ: CellType::Str, i: 0, f: 0.0, s: to_c_str(s) },
    }).unwrap_or_default()
}

#[unsafe(no_mangle)]
pub extern "C" fn tv_str_free(p: *mut c_char) {
    unsafe { free_c_str(p); }
}

// ── Virtual Table Implementation ─────────────────────────────────────────────

/// Execute SQL on SimpleTable using virtual table
fn exec_sql(table: &SimpleTable, sql: &str) -> Result<SimpleTable, SqlError> {
    let conn = Connection::open_in_memory()?;
    conn.create_module("tv", read_only_module::<TableVTab>(), None)?;

    set_current(Arc::new(table.clone()));
    conn.execute_batch("CREATE VIRTUAL TABLE df USING tv")?;

    let mut stmt = conn.prepare(sql)?;
    let col_count = stmt.column_count();
    let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

    let mut rows_data: Vec<Vec<Cell>> = Vec::new();
    let mut col_types: Vec<ColType> = vec![ColType::Str; col_count];
    let mut type_inferred = false;

    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let mut cells = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let cell = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => Cell::Null,
                rusqlite::types::ValueRef::Integer(n) => {
                    if !type_inferred { col_types[i] = ColType::Int; }
                    Cell::Int(n)
                }
                rusqlite::types::ValueRef::Real(f) => {
                    if !type_inferred { col_types[i] = ColType::Float; }
                    Cell::Float(f)
                }
                rusqlite::types::ValueRef::Text(s) => Cell::Str(String::from_utf8_lossy(s).into()),
                rusqlite::types::ValueRef::Blob(b) => Cell::Str(format!("<blob:{}>", b.len())),
            };
            cells.push(cell);
        }
        rows_data.push(cells);
        type_inferred = true;
    }

    clear_current();
    Ok(SimpleTable::new(col_names, col_types, rows_data))
}

/// Virtual table wrapper
#[repr(C)]
struct TableVTab {
    base: ffi::sqlite3_vtab,
}

unsafe impl<'vtab> VTab<'vtab> for TableVTab {
    type Aux = ();
    type Cursor = TableCursor<'vtab>;

    fn connect(db: &mut VTabConnection, _aux: Option<&()>, _args: &[&[u8]]) -> SqlResult<(String, Self)> {
        let t = get_current().ok_or_else(|| SqlError::ModuleError("No table set".into()))?;
        let cols: Vec<String> = t.names.iter().enumerate().map(|(i, name)| {
            let typ = match t.types.get(i).unwrap_or(&ColType::Str) {
                ColType::Int => "INTEGER",
                ColType::Float => "REAL",
                ColType::Bool => "INTEGER",
                ColType::Str => "TEXT",
            };
            format!("\"{}\" {}", name, typ)
        }).collect();
        let schema = format!("CREATE TABLE x({})", cols.join(", "));
        let _ = db.config(VTabConfig::Innocuous);
        Ok((schema, TableVTab { base: ffi::sqlite3_vtab::default() }))
    }

    fn best_index(&self, info: &mut IndexInfo) -> SqlResult<()> {
        info.set_estimated_cost(1000.0);
        info.set_estimated_rows(get_current().map(|t| t.rows() as i64).unwrap_or(1000));
        Ok(())
    }

    fn open(&mut self) -> SqlResult<Self::Cursor> {
        Ok(TableCursor { base: ffi::sqlite3_vtab_cursor::default(), row: 0, _marker: PhantomData })
    }
}

impl CreateVTab<'_> for TableVTab {
    const KIND: VTabKind = VTabKind::Default;
}

/// Cursor for iterating rows
#[repr(C)]
struct TableCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    row: usize,
    _marker: PhantomData<&'vtab ()>,
}

unsafe impl VTabCursor for TableCursor<'_> {
    fn filter(&mut self, _idx_num: c_int, _idx_str: Option<&str>, _args: &Values<'_>) -> SqlResult<()> {
        self.row = 0;
        Ok(())
    }

    fn next(&mut self) -> SqlResult<()> {
        self.row += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        get_current().map(|t| self.row >= t.rows()).unwrap_or(true)
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> SqlResult<()> {
        let t = get_current().ok_or_else(|| SqlError::ModuleError("No table".into()))?;
        match t.data.get(self.row).and_then(|r| r.get(col as usize)) {
            Some(Cell::Null) | None => ctx.set_result(&rusqlite::types::Null),
            Some(Cell::Bool(b)) => ctx.set_result(&(*b as i32)),
            Some(Cell::Int(n)) => ctx.set_result(n),
            Some(Cell::Float(f)) => ctx.set_result(f),
            Some(Cell::Str(s)) => ctx.set_result(&s.as_str()),
        }
    }

    fn rowid(&self) -> SqlResult<i64> { Ok(self.row as i64) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_table() -> SimpleTable {
        SimpleTable::new(
            vec!["name".into(), "age".into()],
            vec![ColType::Str, ColType::Int],
            vec![
                vec![Cell::Str("alice".into()), Cell::Int(30)],
                vec![Cell::Str("bob".into()), Cell::Int(25)],
            ],
        )
    }

    #[test]
    fn test_select_all() {
        let t = test_table();
        let r = exec_sql(&t, "SELECT * FROM df").unwrap();
        assert_eq!(r.rows(), 2);
        assert_eq!(r.cols(), 2);
    }

    #[test]
    fn test_where() {
        let t = test_table();
        let r = exec_sql(&t, "SELECT * FROM df WHERE age > 26").unwrap();
        assert_eq!(r.rows(), 1);
    }

    #[test]
    fn test_count() {
        let t = test_table();
        let r = exec_sql(&t, "SELECT COUNT(*) as cnt FROM df").unwrap();
        assert_eq!(r.rows(), 1);
        match &r.data[0][0] { Cell::Int(n) => assert_eq!(*n, 2), _ => panic!() }
    }

    #[test]
    fn test_registry() {
        let t = test_table();
        registry().lock().unwrap().insert(99, Arc::new(t));
        let r = get_registered(99).unwrap();
        assert_eq!(r.rows(), 2);
        registry().lock().unwrap().remove(&99);
    }
}
