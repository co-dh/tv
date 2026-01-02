//! DuckDB backend via ADBC - direct integration (no plugin)
//! Uses persistent driver/database to avoid reloading libduckdb.so

use super::source;

use super::table::{Cell, ColType, Table};
use std::sync::{Mutex, OnceLock};
use std::collections::HashMap;
use std::time::SystemTime;
use lru::LruCache;

/// Log to ~/.tv/debug.log (on cache miss)
fn dbg(msg: &str) {
    use std::io::Write;
    let Some(home) = std::env::var_os("HOME") else { return };
    let log = std::path::Path::new(&home).join(".tv/debug.log");
    let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(log) else { return };
    let secs = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_secs() % 86400).unwrap_or(0);
    let _ = writeln!(f, "{:02}:{:02}:{:02} {}", secs / 3600, (secs / 60) % 60, secs % 60, msg);
}
use adbc_core::{Driver, Database, Connection, Statement};
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_driver_manager::{ManagedDriver, ManagedDatabase};
use arrow_array::*;
use arrow_schema::*;

// Persistent DuckDB database (loaded once)
static DUCK_DB: OnceLock<Option<Mutex<ManagedDatabase>>> = OnceLock::new();

// Query cache: (path, prql) -> QueryResult
static CACHE: Mutex<Option<LruCache<String, QueryResult>>> = Mutex::new(None);

// Memory table registry: id -> SQL (CREATE+INSERT)
static MEM_TABLES: Mutex<Option<HashMap<usize, String>>> = Mutex::new(None);

fn cache_get(key: &str) -> Option<QueryResult> {
    CACHE.lock().ok()?.as_mut()?.get(key).cloned()
}

fn cache_put(key: &str, r: QueryResult) {
    let mut guard = CACHE.lock().unwrap();
    let cache = guard.get_or_insert_with(|| LruCache::new(std::num::NonZeroUsize::new(100).unwrap()));
    cache.put(key.to_string(), r);
}

/// Query result with schema and data
#[derive(Clone)]
pub struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

impl QueryResult {
    fn cols(&self) -> usize { self.schema.fields().len() }

    fn cell(&self, row: usize, col: usize) -> Cell {
        let mut offset = 0;
        for batch in &self.batches {
            if row < offset + batch.num_rows() {
                return cell_from_batch(batch, row - offset, col);
            }
            offset += batch.num_rows();
        }
        Cell::Null
    }

    fn col_type(&self, col: usize) -> ColType {
        match self.schema.field(col).data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => ColType::Int,
            DataType::Float16 | DataType::Float32 | DataType::Float64 => ColType::Float,
            DataType::Boolean => ColType::Bool,
            _ => ColType::Str,
        }
    }

    fn col_name(&self, idx: usize) -> Option<String> {
        self.schema.fields().get(idx).map(|f| f.name().clone())
    }
}

/// Extract cell from Arrow batch
fn cell_from_batch(batch: &RecordBatch, row: usize, col: usize) -> Cell {
    let arr = batch.column(col);
    if arr.is_null(row) { return Cell::Null; }

    match arr.data_type() {
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            Cell::Bool(a.value(row))
        }
        DataType::Int8 => {
            let a = arr.as_any().downcast_ref::<Int8Array>().unwrap();
            Cell::Int(a.value(row) as i64)
        }
        DataType::Int16 => {
            let a = arr.as_any().downcast_ref::<Int16Array>().unwrap();
            Cell::Int(a.value(row) as i64)
        }
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            Cell::Int(a.value(row) as i64)
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            Cell::Int(a.value(row))
        }
        DataType::UInt8 => {
            let a = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
            Cell::Int(a.value(row) as i64)
        }
        DataType::UInt16 => {
            let a = arr.as_any().downcast_ref::<UInt16Array>().unwrap();
            Cell::Int(a.value(row) as i64)
        }
        DataType::UInt32 => {
            let a = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
            Cell::Int(a.value(row) as i64)
        }
        DataType::UInt64 => {
            let a = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
            Cell::Int(a.value(row) as i64)
        }
        DataType::Float32 => {
            let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            Cell::Float(a.value(row) as f64)
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            Cell::Float(a.value(row))
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            Cell::Str(a.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Cell::Str(a.value(row).to_string())
        }
        _ => Cell::Str("?".into())
    }
}

/// Source path type
enum PathType {
    File { path: String },
    Source { path: String },
    Mem { id: usize },
}

/// Parse path to determine type
fn parse_path(path: &str) -> Option<PathType> {
    if let Some(id_str) = path.strip_prefix("mem:") {
        return id_str.parse().ok().map(|id| PathType::Mem { id });
    }
    if path.starts_with("source:") {
        return Some(PathType::Source { path: path.into() });
    }
    let exts = [".parquet", ".csv", ".json", ".gz"];
    if exts.iter().any(|e| path.ends_with(e)) {
        return Some(PathType::File { path: path.into() });
    }
    None
}

/// PRQL function definitions (freq, cnt, stats, etc.)
const PRQL_FUNCS: &str = r#"
let freq = func c tbl <relation> -> (from tbl | group {c} (aggregate {Cnt = count this}) | sort {-Cnt} | derive {Pct = s"ROUND({Cnt} * 100.0 / SUM({Cnt}) OVER(), 1)", Bar = s"REPEAT('#', CAST({Cnt} * 20.0 / MAX({Cnt}) OVER() AS INT))"})
let uniq = func c tbl <relation> -> (from tbl | group {c} (take 1) | select {c})
let cnt = func tbl <relation> -> (from tbl | aggregate {n = count this})
let stats = func c tbl <relation> -> (from tbl | aggregate {n = count this, min = min c, max = max c, avg = average c, std = stddev c})
let cntdist = func c tbl <relation> -> (from tbl | aggregate {n = count this, dist = count_distinct c})
let meta = func c tbl <relation> -> (from tbl | aggregate {cnt = s"COUNT({c})", dist = count_distinct c, total = count this, min = min c, max = max c})
"#;

/// Compile PRQL to SQL via CLI (prepends function definitions)
pub fn compile_prql(prql: &str) -> Option<String> {
    use std::process::{Command, Stdio};
    use std::io::Write;
    let full = format!("{}\n{}", PRQL_FUNCS, prql);
    let mut child = Command::new("prqlc")
        .args(["compile", "--hide-signature-comment", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn().ok()?;
    child.stdin.take()?.write_all(full.as_bytes()).ok()?;
    let out = child.wait_with_output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Find libduckdb.so path (env DUCKDB_LIB overrides)
fn find_duckdb_lib() -> Option<&'static str> {
    static LIB: OnceLock<Option<String>> = OnceLock::new();
    LIB.get_or_init(|| {
        if let Ok(p) = std::env::var("DUCKDB_LIB") {
            if std::path::Path::new(&p).exists() { return Some(p); }
        }
        let paths = ["/usr/lib/libduckdb.so", "/usr/local/lib/libduckdb.so", "libduckdb.so"];
        paths.iter().find(|p| std::path::Path::new(p).exists()).map(|s| s.to_string())
    }).as_deref()
}

/// Init persistent DuckDB database (called once)
fn init_db() -> Option<Mutex<ManagedDatabase>> {
    let lib = find_duckdb_lib()?;
    dbg(&format!("ADBC init lib={}", lib));
    let mut driver = ManagedDriver::load_dynamic_from_filename(lib, Some(b"duckdb_adbc_init"), AdbcVersion::V110).ok()?;
    let opts = vec![(OptionDatabase::Other("path".into()), OptionValue::String("".into()))];
    let db = driver.new_database_with_opts(opts).ok()?;
    Some(Mutex::new(db))
}

/// Get persistent DuckDB database
fn get_db() -> Option<&'static Mutex<ManagedDatabase>> {
    DUCK_DB.get_or_init(|| init_db()).as_ref()
}

/// Execute SQL via DuckDB (persistent connection)
fn query_duckdb(sql: &str) -> Result<QueryResult, String> {
    let db = get_db().ok_or("DuckDB not available")?.lock().map_err(|e| format!("lock: {}", e))?;
    let mut conn = db.new_connection().map_err(|e| format!("conn: {}", e))?;
    let mut stmt = conn.new_statement().map_err(|e| format!("stmt: {}", e))?;

    stmt.set_sql_query(sql).map_err(|e| format!("sql: {}", e))?;
    let output = stmt.execute().map_err(|e| format!("exec: {}", e))?;

    let schema = output.schema();
    let batches: Vec<RecordBatch> = output.map(|r| r.map_err(|e| format!("batch: {}", e)))
        .collect::<Result<Vec<_>, _>>()?;
    let row_count = batches.iter().map(|b| b.num_rows()).sum();

    Ok(QueryResult { schema, batches, row_count })
}

/// Replace "df" with table expression in SQL
fn replace_table(sql: &str, table: &str) -> String {
    let is_func = table.contains('(');
    let t = if is_func { table.to_string() } else { format!("\"{}\"", table) };
    sql.replace("\"df\"", &t)
       .replace(" df ", &format!(" {} ", t))
       .replace(" df\n", &format!(" {}\n", t))
       .replace("FROM df", &format!("FROM {}", t))
}

/// Generate DuckDB table expression for file
fn file_table_expr(path: &str) -> String {
    if path.ends_with(".parquet") { format!("read_parquet('{}')", path) }
    else if path.ends_with(".csv") || path.ends_with(".csv.gz") { format!("read_csv('{}')", path) }
    else if path.ends_with(".json") { format!("read_json('{}')", path) }
    else { format!("'{}'", path) }
}

// === Public API ===

/// Execute PRQL query on path, return table (cached)
pub fn query(prql: &str, path: &str) -> Option<Box<dyn Table + Send + Sync>> {
    let key = format!("{}:{}", path, prql);

    // Check cache
    if let Some(r) = cache_get(&key) {
        return Some(Box::new(ResultTable(r)));
    }

    // Log cache miss
    dbg(&format!("EXEC path={} prql={}", path, &prql[..prql.len().min(60)]));

    let result = match parse_path(path)? {
        PathType::Mem { id } => {
            let sql = MEM_TABLES.lock().ok()?.as_ref()?.get(&id).cloned()?;
            let prql_sql = compile_prql(prql)?;
            let full_sql = format!("{};{}", sql, prql_sql);
            query_duckdb(&full_sql).ok()?
        }
        PathType::Source { path: src_path } => {
            let src = source::query(&src_path)?;
            let prql_sql = compile_prql(prql)?;
            let full_sql = format!("{};{}", src.sql, prql_sql);
            query_duckdb(&full_sql).ok()?
        }
        PathType::File { path: file_path } => {
            let sql = compile_prql(prql)?;
            let table_expr = file_table_expr(&file_path);
            let sql = replace_table(&sql, &table_expr);
            query_duckdb(&sql).ok()?
        }
    };

    cache_put(&key, result.clone());
    Some(Box::new(ResultTable(result)))
}

/// Save query result to file
pub fn save(prql: &str, path_in: &str, path_out: &str) -> bool {
    let sql = match compile_prql(prql) { Some(s) => s, None => return false };
    let fmt = if path_out.ends_with(".parquet") || path_out.ends_with(".pq") { "PARQUET" } else { "CSV" };

    let copy_sql = match parse_path(path_in) {
        Some(PathType::File { path }) => {
            let table_expr = file_table_expr(&path);
            let sql = replace_table(&sql, &table_expr);
            format!("COPY ({}) TO '{}' (FORMAT {})", sql.trim_end_matches(';'), path_out, fmt)
        }
        Some(PathType::Source { path: src_path }) => {
            let src = match source::query(&src_path) { Some(s) => s, None => return false };
            format!("{}; COPY ({}) TO '{}' (FORMAT {})", src.sql, sql.trim_end_matches(';'), path_out, fmt)
        }
        _ => return false,
    };

    query_duckdb(&copy_sql).is_ok()
}

/// Register table for in-memory querying, returns path "mem:id"
pub fn register_table(id: usize, data: &dyn Table) -> Option<String> {
    let sql = table_to_sql(data);
    let mut guard = MEM_TABLES.lock().ok()?;
    let map = guard.get_or_insert_with(HashMap::new);
    map.insert(id, sql);
    Some(format!("mem:{}", id))
}

/// Unregister memory table (frees SQL)
pub fn unregister_table(id: usize) {
    if let Ok(mut guard) = MEM_TABLES.lock() {
        if let Some(map) = guard.as_mut() { map.remove(&id); }
    }
}

/// Convert Table to DuckDB SQL (CREATE+INSERT)
fn table_to_sql(t: &dyn Table) -> String {
    let cols = t.col_names();
    let types: Vec<&str> = (0..t.cols()).map(|i| match t.col_type(i) {
        ColType::Int => "BIGINT",
        ColType::Float => "DOUBLE",
        ColType::Bool => "BOOLEAN",
        _ => "VARCHAR",
    }).collect();

    let schema: Vec<String> = cols.iter().zip(&types)
        .map(|(n, ty)| format!("\"{}\" {}", n, ty))
        .collect();
    let create = format!("CREATE OR REPLACE TABLE df({})", schema.join(","));

    if t.rows() == 0 { return create; }
    let rows: Vec<String> = (0..t.rows()).map(|r| {
        let vals: Vec<String> = (0..t.cols()).map(|c| {
            match t.cell(r, c) {
                Cell::Null => "NULL".into(),
                Cell::Bool(b) => if b { "TRUE" } else { "FALSE" }.into(),
                Cell::Int(n) => n.to_string(),
                Cell::Float(f) => f.to_string(),
                Cell::Str(s) | Cell::Date(s) | Cell::Time(s) | Cell::DateTime(s) =>
                    format!("'{}'", s.replace('\'', "''")),
            }
        }).collect();
        format!("({})", vals.join(","))
    }).collect();

    format!("{};INSERT INTO df VALUES{}", create, rows.join(","))
}

/// Wrapper to make QueryResult implement Table
struct ResultTable(QueryResult);

impl Table for ResultTable {
    fn rows(&self) -> usize { self.0.row_count }
    fn cols(&self) -> usize { self.0.cols() }
    fn col_name(&self, idx: usize) -> Option<String> { self.0.col_name(idx) }
    fn col_names(&self) -> Vec<String> { (0..self.cols()).filter_map(|i| self.col_name(i)).collect() }
    fn col_type(&self, idx: usize) -> ColType { self.0.col_type(idx) }
    fn cell(&self, row: usize, col: usize) -> Cell { self.0.cell(row, col) }
    fn col_width(&self, idx: usize, sample: usize) -> usize {
        let header = self.col_name(idx).map(|s| s.len()).unwrap_or(0);
        let max_data = (0..sample.min(self.rows()))
            .map(|r| self.cell(r, idx).format(3).len())
            .max().unwrap_or(0);
        header.max(max_data).max(3)
    }
}

// Make ResultTable Send+Sync safe
unsafe impl Send for ResultTable {}
unsafe impl Sync for ResultTable {}
