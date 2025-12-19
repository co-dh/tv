//! SQLite virtual table for SimpleTable - zero-copy SQL queries on in-memory tables.
//! Uses rusqlite vtab feature to expose SimpleTable as SQLite virtual table.

use crate::table::{Cell, ColType, SimpleTable, Table};
use anyhow::Result;
use rusqlite::ffi;
use rusqlite::vtab::{read_only_module, Context, CreateVTab, IndexInfo, VTab, VTabConfig, VTabConnection, VTabCursor, VTabKind, Values};
use rusqlite::{Connection, Error as SqlError, Result as SqlResult};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::os::raw::c_int;
use std::sync::Arc;

// Thread-local table storage for virtual table access
thread_local! {
    static CURRENT_TABLE: RefCell<Option<Arc<SimpleTable>>> = const { RefCell::new(None) };
}

fn set_table(t: Arc<SimpleTable>) { CURRENT_TABLE.with(|c| *c.borrow_mut() = Some(t)); }
fn get_table() -> Option<Arc<SimpleTable>> { CURRENT_TABLE.with(|c| c.borrow().clone()) }
fn clear_table() { CURRENT_TABLE.with(|c| *c.borrow_mut() = None); }

// ── Virtual Table Implementation ─────────────────────────────────────────────

/// Virtual table wrapper for SimpleTable
#[repr(C)]
struct TableVTab {
    base: ffi::sqlite3_vtab,
}

unsafe impl<'vtab> VTab<'vtab> for TableVTab {
    type Aux = ();
    type Cursor = TableCursor<'vtab>;

    fn connect(db: &mut VTabConnection, _aux: Option<&()>, _args: &[&[u8]]) -> SqlResult<(String, Self)> {
        let t = get_table().ok_or_else(|| SqlError::ModuleError("No table set".into()))?;
        // Build schema from column names/types
        let cols: Vec<String> = t.names.iter().enumerate().map(|(i, name)| {
            let typ = match t.types.get(i).unwrap_or(&ColType::Str) {
                ColType::Int => "INTEGER",
                ColType::Float => "REAL",
                ColType::Bool => "INTEGER",
                _ => "TEXT",
            };
            format!("\"{}\" {}", name, typ)
        }).collect();
        let schema = format!("CREATE TABLE x({})", cols.join(", "));
        let _ = db.config(VTabConfig::Innocuous);
        Ok((schema, TableVTab { base: ffi::sqlite3_vtab::default() }))
    }

    fn best_index(&self, info: &mut IndexInfo) -> SqlResult<()> {
        info.set_estimated_cost(1000.0);
        info.set_estimated_rows(get_table().map(|t| t.rows() as i64).unwrap_or(1000));
        Ok(())
    }

    fn open(&mut self) -> SqlResult<Self::Cursor> {
        Ok(TableCursor { base: ffi::sqlite3_vtab_cursor::default(), row: 0, _marker: PhantomData })
    }
}

impl CreateVTab<'_> for TableVTab {
    const KIND: VTabKind = VTabKind::Default;
}

/// Cursor for iterating over SimpleTable rows
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
        get_table().map(|t| self.row >= t.rows()).unwrap_or(true)
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> SqlResult<()> {
        let t = get_table().ok_or_else(|| SqlError::ModuleError("No table".into()))?;
        match t.cell(self.row, col as usize) {
            Cell::Null => ctx.set_result(&rusqlite::types::Null),
            Cell::Bool(b) => ctx.set_result(&(b as i32)),
            Cell::Int(n) => ctx.set_result(&n),
            Cell::Float(f) => ctx.set_result(&f),
            Cell::Str(s) => ctx.set_result(&s.as_str()),
            Cell::Date(s) | Cell::Time(s) | Cell::DateTime(s) => ctx.set_result(&s.as_str()),
        }
    }

    fn rowid(&self) -> SqlResult<i64> { Ok(self.row as i64) }
}

// ── SQL Execution ────────────────────────────────────────────────────────────

/// Execute SQL on SimpleTable, return result as SimpleTable
pub fn sql(table: &SimpleTable, query: &str) -> Result<SimpleTable> {
    let conn = Connection::open_in_memory()?;
    conn.create_module("tv", read_only_module::<TableVTab>(), None)?;

    set_table(Arc::new(table.clone()));
    conn.execute_batch("CREATE VIRTUAL TABLE df USING tv")?;

    let mut stmt = conn.prepare(query)?;
    let col_count = stmt.column_count();
    let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

    // Collect rows and infer types from first row
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

    clear_table();
    Ok(SimpleTable::new(col_names, col_types, rows_data))
}

/// Execute SQL with WHERE clause
pub fn sql_where(table: &SimpleTable, filter: &str) -> Result<SimpleTable> {
    sql(table, &format!("SELECT * FROM df WHERE {}", filter))
}

/// Count rows matching WHERE
pub fn count_where(table: &SimpleTable, filter: &str) -> Result<usize> {
    let r = sql(table, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", filter))?;
    match r.cell(0, 0) { Cell::Int(n) => Ok(n as usize), _ => Ok(0) }
}

/// Frequency count (GROUP BY)
pub fn freq(table: &SimpleTable, cols: &[String], filter: &str) -> Result<SimpleTable> {
    let grp = cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");
    let w = if filter.is_empty() || filter == "TRUE" { String::new() } else { format!("WHERE {}", filter) };
    sql(table, &format!("SELECT {}, COUNT(*) as Cnt FROM df {} GROUP BY {} ORDER BY Cnt DESC", grp, w, grp))
}

/// Sort and limit
pub fn sort_head(table: &SimpleTable, col: &str, desc: bool, limit: usize) -> Result<SimpleTable> {
    let order = if desc { "DESC" } else { "ASC" };
    sql(table, &format!("SELECT * FROM df ORDER BY \"{}\" {} LIMIT {}", col, order, limit))
}

/// Fetch rows with offset/limit
pub fn fetch(table: &SimpleTable, offset: usize, limit: usize) -> Result<SimpleTable> {
    sql(table, &format!("SELECT * FROM df LIMIT {} OFFSET {}", limit, offset))
}

/// Fetch with WHERE + offset/limit
pub fn fetch_where(table: &SimpleTable, filter: &str, offset: usize, limit: usize) -> Result<SimpleTable> {
    sql(table, &format!("SELECT * FROM df WHERE {} LIMIT {} OFFSET {}", filter, limit, offset))
}

/// Filter with limit
pub fn filter(table: &SimpleTable, expr: &str, limit: usize) -> Result<SimpleTable> {
    sql(table, &format!("SELECT * FROM df WHERE {} LIMIT {}", expr, limit))
}

/// Select specific columns
pub fn select(table: &SimpleTable, cols: &[String]) -> Result<SimpleTable> {
    let sel = cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");
    sql(table, &format!("SELECT {} FROM df", sel))
}

/// Get distinct values for a column
pub fn distinct(table: &SimpleTable, col: &str) -> Result<Vec<String>> {
    let r = sql(table, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
    Ok((0..r.rows()).map(|i| r.cell(i, 0).format(10)).collect())
}

/// Get schema (column name, type pairs)
pub fn schema(table: &SimpleTable) -> Vec<(String, String)> {
    table.names.iter().zip(table.types.iter())
        .map(|(n, t)| (n.clone(), format!("{:?}", t)))
        .collect()
}

/// Compile PRQL to SQL (wrapper for source::prql_to_sql)
pub fn prql_to_sql(prql: &str) -> Result<String> {
    crate::source::prql_to_sql(prql)
}

/// Execute PRQL on table (compile to SQL, then execute)
pub fn prql(table: &SimpleTable, query: &str) -> Result<SimpleTable> {
    let sql_query = prql_to_sql(query)?;
    sql(table, &sql_query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Col;

    fn test_table() -> SimpleTable {
        SimpleTable::from_cols(vec![
            Col::str("name", vec!["alice".into(), "bob".into(), "carol".into()]),
            Col::int("age", vec![30, 25, 35]),
            Col::float("score", vec![85.5, 92.0, 78.5]),
        ])
    }

    #[test]
    fn test_select_all() {
        let t = test_table();
        let r = sql(&t, "SELECT * FROM df").unwrap();
        assert_eq!(r.rows(), 3);
        assert_eq!(r.cols(), 3);
    }

    #[test]
    fn test_where_clause() {
        let t = test_table();
        let r = sql(&t, "SELECT * FROM df WHERE age > 28").unwrap();
        assert_eq!(r.rows(), 2); // alice(30) and carol(35)
    }

    #[test]
    fn test_count() {
        let t = test_table();
        let n = count_where(&t, "score > 80").unwrap();
        assert_eq!(n, 2); // alice(85.5) and bob(92.0)
    }

    #[test]
    fn test_freq() {
        let t = SimpleTable::from_cols(vec![
            Col::str("city", vec!["NYC".into(), "LA".into(), "NYC".into(), "NYC".into()]),
        ]);
        let r = freq(&t, &["city".into()], "TRUE").unwrap();
        assert_eq!(r.rows(), 2);
        assert_eq!(r.cell(0, 0).format(0), "NYC"); // NYC first (count 3)
    }

    #[test]
    fn test_sort_head() {
        let t = test_table();
        let r = sort_head(&t, "age", true, 2).unwrap();
        assert_eq!(r.rows(), 2);
        assert_eq!(r.cell(0, 0).format(0), "carol"); // carol(35) first
    }

    #[test]
    fn test_fetch_where() {
        let t = test_table();
        let r = fetch_where(&t, "age > 20", 1, 1).unwrap();
        assert_eq!(r.rows(), 1); // skip first, take 1
    }

    #[test]
    fn test_distinct() {
        let t = SimpleTable::from_cols(vec![
            Col::str("x", vec!["a".into(), "b".into(), "a".into(), "c".into()]),
        ]);
        let vals = distinct(&t, "x").unwrap();
        assert_eq!(vals, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_select_cols() {
        let t = test_table();
        let r = select(&t, &["name".into(), "age".into()]).unwrap();
        assert_eq!(r.cols(), 2);
        assert_eq!(r.col_names(), vec!["name", "age"]);
    }

    #[test]
    fn test_prql_filter() {
        let t = test_table();
        let r = prql(&t, "from df | filter age > 28").unwrap();
        assert_eq!(r.rows(), 2); // alice(30) and carol(35)
    }

    #[test]
    fn test_prql_chained() {
        let t = test_table();
        let r = prql(&t, "from df | filter age > 20 | filter score > 80").unwrap();
        assert_eq!(r.rows(), 2); // alice and bob (score > 80, age > 20)
    }
}
