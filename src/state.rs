use crate::table::{BoxTable, ColStats, SimpleTable, Cell};
use std::collections::HashSet;

/// Reserved rows in viewport (header + footer_header + status)
pub const RESERVED_ROWS: usize = 3;

/// Convert SQL filter to PRQL filter
/// - = to == (preserving !=, >=, <=)
/// - ~= 'pat' to s"col LIKE '%pat%'" (PRQL raw SQL)
fn sql_to_prql_filter(sql: &str) -> String {
    // First handle ~= (contains pattern match) - convert to PRQL s-string with SQL LIKE
    if let Some((col, pat)) = sql.split_once(" ~= ") {
        let col = col.trim();
        let pat = pat.trim().trim_matches('\'').trim_matches('"');
        return format!("s\"{} LIKE '%{}%'\"", col, pat);
    }
    // Replace standalone = with ==, but preserve !=, >=, <=
    let mut r = String::with_capacity(sql.len() + 10);
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut i = 0;
    while i < n {
        let c = chars[i];
        if c == '=' {
            let prev = if i > 0 { chars[i - 1] } else { ' ' };
            if prev == '!' || prev == '<' || prev == '>' {
                r.push(c);  // keep as-is (part of !=, <=, >=)
            } else {
                r.push_str("==");  // replace = with ==
            }
        } else {
            r.push(c);
        }
        i += 1;
    }
    r
}

// ── Grouped structs to reduce ViewState field count ─────────────────────────

/// Data source: where the view's data comes from
#[derive(Clone, Debug)]
pub enum ViewSource {
    Memory,                                         // CSV loaded in memory
    Gz { path: String, partial: bool },             // gzipped CSV (may be partial)
    Parquet { path: String, rows: usize, cols: Vec<String> },  // lazy parquet on disk
}

impl Default for ViewSource {
    fn default() -> Self { Self::Memory }
}

impl ViewSource {
    /// Get path for display (gz or parquet path)
    pub fn path(&self) -> Option<&str> {
        match self {
            Self::Memory => None,
            Self::Gz { path, .. } | Self::Parquet { path, .. } => Some(path),
        }
    }
    /// Is this a parquet source?
    pub fn is_parquet(&self) -> bool { matches!(self, Self::Parquet { .. }) }
    /// Is this a gz source?
    pub fn is_gz(&self) -> bool { matches!(self, Self::Gz { .. }) }
    /// Get parquet col names
    pub fn cols(&self) -> &[String] {
        match self { Self::Parquet { cols, .. } => cols, _ => &[] }
    }
    /// Get disk row count (parquet only)
    pub fn disk_rows(&self) -> Option<usize> {
        match self { Self::Parquet { rows, .. } => Some(*rows), _ => None }
    }
}

/// Parent view info (for derived views like meta/freq)
#[derive(Clone, Debug, Default)]
pub struct ParentInfo {
    pub id: usize,
    pub rows: usize,
    pub name: String,
    pub freq_col: Option<String>,  // only for freq views
}

/// Cached data to avoid recomputation
#[derive(Clone, Debug, Default)]
pub struct ViewCache {
    pub stats: Option<(usize, String)>,   // (col_idx, stats string)
    pub meta: Option<SimpleTable>,         // metadata stats for this view
    pub fetch: Option<(usize, usize)>,     // (start, end) row range in data
}

// ── Newtypes for type safety ────────────────────────────────────────────────

/// Row index newtype - prevents mixing with column indices
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row(pub usize);

impl Row {
    #[inline] pub fn get(self) -> usize { self.0 }
    #[inline] pub fn saturating_sub(self, n: usize) -> Self { Self(self.0.saturating_sub(n)) }
}

impl std::ops::Add<usize> for Row {
    type Output = Self;
    fn add(self, n: usize) -> Self { Self(self.0 + n) }
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

/// Column index newtype - prevents mixing with row indices
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Col(pub usize);

impl Col {
    #[inline] pub fn get(self) -> usize { self.0 }
    #[inline] pub fn saturating_sub(self, n: usize) -> Self { Self(self.0.saturating_sub(n)) }
}

impl std::ops::Add<usize> for Col {
    type Output = Self;
    fn add(self, n: usize) -> Self { Self(self.0 + n) }
}

impl std::fmt::Display for Col {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

/// View kind for type-safe dispatch
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ViewKind {
    Table,       // normal data table
    Meta,        // metadata view
    Freq,        // frequency distribution
    Corr,        // correlation matrix
    Folder,      // file browser (ls/lr)
    Pivot,       // pivot table
}

impl std::fmt::Display for ViewKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::Meta => write!(f, "meta"),
            Self::Freq => write!(f, "freq"),
            Self::Corr => write!(f, "corr"),
            Self::Folder => write!(f, "folder"),
            Self::Pivot => write!(f, "pivot"),
        }
    }
}

/// Table cursor/viewport state
#[derive(Clone, Debug, Default)]
pub struct TableState {
    pub r0: usize,              // first visible row
    pub cr: usize,              // cursor row
    pub cc: usize,              // cursor col
    pub viewport: (u16, u16),   // (rows, cols)
    pub col_widths: Vec<u16>,   // cached widths
    pub widths_row: usize,      // row where widths calc'd
}

impl TableState {

    /// Need width recalc if moved >1 page
    #[must_use]
    pub fn need_widths(&self) -> bool {
        self.col_widths.is_empty() || self.cr.abs_diff(self.widths_row) > self.viewport.0.saturating_sub(2) as usize
    }

    /// Move cursor down n rows, scroll if needed
    pub fn down(&mut self, n: usize, max: usize) {
        if max == 0 { return; }
        self.cr = (self.cr + n).min(max - 1);
        let vis = self.visible_rows();
        if self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis - 1); }
    }

    /// Number of visible data rows (viewport minus reserved rows)
    #[inline]
    #[must_use]
    pub fn visible_rows(&self) -> usize {
        (self.viewport.0 as usize).saturating_sub(RESERVED_ROWS)
    }

    /// Move cursor up n rows, scroll if needed
    pub fn up(&mut self, n: usize) {
        self.cr = self.cr.saturating_sub(n);
        if self.cr < self.r0 { self.r0 = self.cr; }
    }

    /// Move cursor right n columns
    pub fn right(&mut self, n: usize, max: usize) {
        if max > 0 { self.cc = (self.cc + n).min(max - 1); }
    }

    /// Move cursor left n columns
    pub fn left(&mut self, n: usize) { self.cc = self.cc.saturating_sub(n); }
    /// Jump to first row
    pub fn top(&mut self) { self.cr = 0; self.r0 = 0; }

    /// Ensure cursor is visible in viewport
    pub fn visible(&mut self) {
        let vis = self.visible_rows();
        if self.cr < self.r0 { self.r0 = self.cr; }
        else if self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis.saturating_sub(1)); }
    }

    /// Center cursor on screen only if not already visible
    pub fn center_if_needed(&mut self) {
        let vis = self.visible_rows();
        if self.cr < self.r0 || self.cr >= self.r0 + vis {
            self.r0 = self.cr.saturating_sub(vis / 2);
        }
    }
}

/// View state (28 fields → 16 via grouping)
pub struct ViewState {
    pub id: usize,
    pub name: String,
    pub kind: ViewKind,              // view type for dispatch
    pub prql: String,                // PRQL query that produces this view
    pub data: BoxTable,              // table data (SimpleTable for system, PluginTable for files)
    pub state: TableState,
    pub history: Vec<String>,
    pub filename: Option<String>,    // display filename
    pub show_row_numbers: bool,
    pub source: ViewSource,          // data source (memory/gz/parquet)
    pub parent: Option<ParentInfo>,  // parent view info (meta/freq)
    pub cache: ViewCache,            // cached computations
    pub selected_cols: HashSet<usize>,
    pub selected_rows: HashSet<usize>,
    pub col_separator: Option<usize>,  // draw separator bar after key columns
    pub filter: Option<String>,      // SQL WHERE clause
    pub sort_col: Option<String>,    // sort column
    pub sort_desc: bool,             // sort descending
    pub cols: Vec<String>,           // current column list (for parquet views after select/xkey)
}

/// Manual Clone - copies BoxTable into new SimpleTable
impl Clone for ViewState {
    fn clone(&self) -> Self {
        let t = self.data.as_ref();
        let data: BoxTable = Box::new(SimpleTable::new(
            t.col_names(),
            (0..t.cols()).map(|c| t.col_type(c)).collect(),
            (0..t.rows()).map(|r| (0..t.cols()).map(|c| t.cell(r, c)).collect()).collect()
        ));
        Self {
            id: self.id, name: self.name.clone(), kind: self.kind, prql: self.prql.clone(), data,
            state: self.state.clone(), history: self.history.clone(), filename: self.filename.clone(),
            show_row_numbers: self.show_row_numbers, source: self.source.clone(), parent: self.parent.clone(),
            cache: self.cache.clone(), selected_cols: self.selected_cols.clone(), selected_rows: self.selected_rows.clone(),
            col_separator: self.col_separator, filter: self.filter.clone(), sort_col: self.sort_col.clone(),
            sort_desc: self.sort_desc, cols: self.cols.clone(),
        }
    }
}

impl ViewState {
    /// Get data path (source path or filename for in-memory CSV)
    #[must_use]
    pub fn path(&self) -> &str {
        self.source.path()
            .or(self.filename.as_deref())
            .unwrap_or("")
    }

    /// Get key columns (columns before separator)
    #[must_use]
    pub fn key_cols(&self) -> Vec<String> {
        let Some(sep) = self.col_separator else { return vec![] };
        let names = self.col_names();
        names.into_iter().take(sep).collect()
    }

    /// Get column names (from cols field, source, or data)
    #[must_use]
    pub fn col_names(&self) -> Vec<String> {
        if !self.cols.is_empty() { return self.cols.clone(); }
        let src = self.source.cols();
        if !src.is_empty() { src.to_vec() }
        else { self.data.col_names() }
    }

    /// Base view with default values
    fn base(id: usize, name: impl Into<String>, kind: ViewKind, prql: impl Into<String>, data: BoxTable) -> Self {
        Self {
            id, name: name.into(), kind, prql: prql.into(), data,
            state: TableState::default(), history: Vec::new(), filename: None, show_row_numbers: false,
            source: ViewSource::Memory, parent: None, cache: ViewCache::default(),
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), col_separator: None,
            filter: None, sort_col: None, sort_desc: false, cols: Vec::new(),
        }
    }

    /// Create empty table
    fn empty_table() -> BoxTable { Box::new(SimpleTable::empty()) }

    /// Create standard in-memory view (CSV, filtered results, etc.)
    pub fn new(id: usize, name: impl Into<String>, data: BoxTable, filename: Option<String>) -> Self {
        let prql = "from df".to_string();  // PRQL uses df, path passed separately
        Self { filename, ..Self::base(id, name, ViewKind::Table, prql, data) }
    }

    /// Create lazy parquet view (no in-memory data, all ops go to disk)
    pub fn new_parquet(id: usize, name: impl Into<String>, path: impl Into<String>, rows: usize, c: Vec<String>) -> Self {
        let p = path.into();
        let prql = "from df".to_string();  // PRQL uses df, path passed separately
        let src = ViewSource::Parquet { path: p.clone(), rows, cols: c.clone() };
        Self { filename: Some(p), source: src, cols: c, ..Self::base(id, name, ViewKind::Table, prql, Self::empty_table()) }
    }

    /// Create gzipped CSV view (may be partial if memory limit hit)
    pub fn new_gz(id: usize, name: impl Into<String>, data: BoxTable, filename: Option<String>, gz: impl Into<String>, partial: bool) -> Self {
        let p = gz.into();
        let prql = "from df".to_string();  // PRQL uses df, path passed separately
        let src = ViewSource::Gz { path: p, partial };
        Self { filename, source: src, ..Self::base(id, name, ViewKind::Table, prql, data) }
    }

    /// Create metadata view with parent info
    pub fn new_meta(id: usize, data: BoxTable, pid: usize, prows: usize, pname: impl Into<String>, parent_prql: &str) -> Self {
        let prql = format!("{} | meta", parent_prql);
        let parent = ParentInfo { id: pid, rows: prows, name: pname.into(), freq_col: None };
        Self { parent: Some(parent), ..Self::base(id, "metadata", ViewKind::Meta, prql, data) }
    }

    /// Create freq view with parent info
    pub fn new_freq(id: usize, name: impl Into<String>, data: BoxTable, pid: usize, prows: usize, pname: impl Into<String>, col: impl Into<String>, parent_prql: &str, grp_cols: &[String]) -> Self {
        let grp = grp_cols.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
        let prql = format!("{} | group {{{}}} (aggregate {{Cnt = count this}})", parent_prql, grp);
        let parent = ParentInfo { id: pid, rows: prows, name: pname.into(), freq_col: Some(col.into()) };
        Self { parent: Some(parent), ..Self::base(id, name, ViewKind::Freq, prql, data) }
    }

    /// Create pivot view with parent info
    pub fn new_pivot(id: usize, name: impl Into<String>, data: BoxTable, pid: usize, pname: impl Into<String>, parent_prql: &str) -> Self {
        let prql = format!("{} | pivot", parent_prql);
        let parent = ParentInfo { id: pid, rows: 0, name: pname.into(), freq_col: None };
        Self { parent: Some(parent), ..Self::base(id, name, ViewKind::Pivot, prql, data) }
    }

    /// Create correlation view
    pub fn new_corr(id: usize, data: BoxTable, parent_prql: &str) -> Self {
        let prql = format!("{} | corr", parent_prql);
        Self::base(id, "correlation", ViewKind::Corr, prql, data)
    }

    /// Create folder view (ls/lr) - registers data with sqlite for querying
    pub fn new_folder(id: usize, name: impl Into<String>, data: BoxTable) -> Self {
        let n = name.into();
        let prql = "from df".to_string();
        // Register with sqlite for SQL queries
        let path = crate::dynload::register_table(id, data.as_ref());
        let mut v = Self::base(id, n, ViewKind::Folder, prql, data);
        v.filename = path;  // "memory:id" for plugin routing
        v
    }

    /// Create filtered parquet view (lazy - all ops go to disk with WHERE)
    pub fn new_filtered(id: usize, name: impl Into<String>, path: impl Into<String>, c: Vec<String>, flt: impl Into<String>, count: usize, parent_prql: &str, filter_expr: &str) -> Self {
        let p = path.into();
        // Convert filter to PRQL syntax (= to ==, ~= to s"LIKE")
        let prql_expr = sql_to_prql_filter(filter_expr);
        let prql = format!("{} | filter {}", parent_prql, prql_expr);
        let src = ViewSource::Parquet { path: p.clone(), rows: count, cols: c.clone() };
        Self { filename: Some(p), source: src, filter: Some(flt.into()), cols: c, ..Self::base(id, name, ViewKind::Table, prql, Self::empty_table()) }
    }

    /// Add command to history
    pub fn add_hist(&mut self, cmd: impl Into<String>) { self.history.push(cmd.into()); }

    /// Row count: disk_rows for parquet, else data height
    #[inline]
    #[must_use]
    pub fn rows(&self) -> usize { self.source.disk_rows().unwrap_or_else(|| self.data.rows()) }

    /// Column count
    #[inline]
    #[must_use]
    pub fn cols(&self) -> usize {
        if !self.cols.is_empty() { return self.cols.len(); }
        let src = self.source.cols();
        if src.is_empty() { self.data.cols() } else { src.len() }
    }

    /// Get column name by index
    #[must_use]
    pub fn col_name(&self, idx: usize) -> Option<String> {
        if !self.cols.is_empty() { return self.cols.get(idx).cloned(); }
        let src = self.source.cols();
        if !src.is_empty() { src.get(idx).cloned() }
        else { self.data.col_name(idx) }
    }

    /// Check if view uses row selection (meta/freq) vs column selection (table)
    #[inline]
    #[must_use]
    pub fn is_row_sel(&self) -> bool { matches!(self.kind, ViewKind::Meta | ViewKind::Freq) }

    /// Get cell value
    #[must_use]
    pub fn cell(&self, row: usize, col: usize) -> Cell { self.data.cell(row, col) }

    /// Get column statistics (for status bar) - computes on demand
    #[must_use]
    pub fn col_stats(&self, col_idx: usize) -> ColStats {
        let n = self.data.rows();
        if n == 0 { return ColStats::default(); }
        let name = self.data.col_name(col_idx).unwrap_or_default();
        let typ = self.data.col_type(col_idx);
        // Compute basic stats from data
        let mut nulls = 0usize;
        let mut vals: Vec<String> = Vec::with_capacity(n.min(1000));
        let mut nums: Vec<f64> = Vec::new();
        for r in 0..n.min(1000) {
            match self.data.cell(r, col_idx) {
                Cell::Null => nulls += 1,
                Cell::Int(i) => { nums.push(i as f64); vals.push(i.to_string()); }
                Cell::Float(f) => { nums.push(f); vals.push(format!("{:.2}", f)); }
                Cell::Str(s) => vals.push(s),
                c => vals.push(c.format(2)),
            }
        }
        let null_pct = (nulls as f64 / n as f64) * 100.0;
        let distinct = {
            let mut u = vals.clone();
            u.sort();
            u.dedup();
            u.len()
        };
        if !nums.is_empty() {
            nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let min = nums.first().copied().unwrap_or(0.0);
            let max = nums.last().copied().unwrap_or(0.0);
            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
            let var = nums.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / nums.len() as f64;
            let sigma = var.sqrt();
            ColStats { name, dtype: format!("{:?}", typ), null_pct, distinct, min: format!("{:.2}", min), max: format!("{:.2}", max), median: format!("{:.2}", mean), sigma: format!("{:.2}", sigma) }
        } else {
            let mode = vals.first().cloned().unwrap_or_default();
            ColStats { name, dtype: format!("{:?}", typ), null_pct, distinct, min: String::new(), max: mode, median: String::new(), sigma: String::new() }
        }
    }
}

impl std::fmt::Display for ViewState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}x{}]", self.name, self.rows(), self.cols())
    }
}

/// View stack - manages multiple views like browser tabs
#[derive(Default)]
pub struct StateStack { stack: Vec<ViewState> }

impl StateStack {
    /// Push new view on top (inherits viewport from current view)
    pub fn push(&mut self, mut v: ViewState) {
        if let Some(cur) = self.stack.last() { v.state.viewport = cur.state.viewport; }
        self.stack.push(v);
    }
    /// Pop top view (allows returning to empty state)
    pub fn pop(&mut self) -> Option<ViewState> { self.stack.pop() }
    /// Current view reference
    #[inline]
    #[must_use]
    pub fn cur(&self) -> Option<&ViewState> { self.stack.last() }
    /// Current view mutable reference
    #[inline]
    pub fn cur_mut(&mut self) -> Option<&mut ViewState> { self.stack.last_mut() }
    /// Stack depth
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize { self.stack.len() }
    /// Is empty
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool { self.stack.is_empty() }
    /// Has any view
    #[inline]
    #[must_use]
    pub fn has_view(&self) -> bool { !self.stack.is_empty() }
    /// Find view by id
    pub fn find_mut(&mut self, id: usize) -> Option<&mut ViewState> { self.stack.iter_mut().find(|v| v.id == id) }

    /// Swap top two views
    pub fn swap(&mut self) {
        let n = self.stack.len();
        if n >= 2 { self.stack.swap(n - 1, n - 2); }
    }

    /// Get names of all views in stack
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.stack.iter().map(|v| v.name.clone()).collect()
    }
}

impl std::ops::Index<usize> for StateStack {
    type Output = ViewState;
    fn index(&self, idx: usize) -> &Self::Output { &self.stack[idx] }
}

impl<'a> IntoIterator for &'a StateStack {
    type Item = &'a ViewState;
    type IntoIter = std::slice::Iter<'a, ViewState>;
    fn into_iter(self) -> Self::IntoIter { self.stack.iter() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty() -> BoxTable { Box::new(SimpleTable::empty()) }

    #[test]
    fn test_center_if_needed_visible_row_unchanged() {
        let mut state = TableState::default();
        state.viewport = (20, 80);
        state.r0 = 0;
        state.cr = 5;
        state.center_if_needed();
        assert_eq!(state.r0, 0, "r0 should not change when cursor is visible");
    }

    #[test]
    fn test_center_if_needed_above_visible_centers() {
        let mut state = TableState::default();
        state.viewport = (20, 80);
        state.r0 = 100;
        state.cr = 50;
        state.center_if_needed();
        assert_eq!(state.r0, 42, "r0 should center cursor when above visible area");
    }

    #[test]
    fn test_center_if_needed_below_visible_centers() {
        let mut state = TableState::default();
        state.viewport = (20, 80);
        state.r0 = 0;
        state.cr = 50;
        state.center_if_needed();
        assert_eq!(state.r0, 42, "r0 should center cursor when below visible area");
    }

    #[test]
    fn test_center_if_needed_at_boundary() {
        let mut state = TableState::default();
        state.viewport = (20, 80);
        state.r0 = 0;
        state.cr = 16;
        state.center_if_needed();
        assert_eq!(state.r0, 0, "r0 should not change when cursor is at last visible row");
    }

    #[test]
    fn test_stack_names() {
        let mut stack = StateStack::default();
        stack.push(ViewState::new(0, "view1", empty(), None));
        stack.push(ViewState::new(1, "view2", empty(), None));
        stack.push(ViewState::new(2, "view3", empty(), None));
        let names = stack.names();
        assert_eq!(names, vec!["view1", "view2", "view3"]);
    }

    #[test]
    fn test_prql_from_file() {
        let v = ViewState::new(0, "test", empty(), Some("data.csv".into()));
        assert_eq!(v.prql, "from df"); // df = data frame, path passed separately to plugin
    }

    #[test]
    fn test_prql_parquet() {
        let v = ViewState::new_parquet(0, "test", "data.parquet", 100, vec!["a".into(), "b".into()]);
        assert_eq!(v.prql, "from df"); // df = data frame, path passed separately
    }

    #[test]
    fn test_prql_meta() {
        let v = ViewState::new_meta(0, empty(), 1, 100, "parent", r#"from "data.csv""#);
        assert_eq!(v.prql, r#"from "data.csv" | meta"#);
    }

    #[test]
    fn test_prql_freq() {
        let v = ViewState::new_freq(0, "freq", empty(), 1, 100, "parent", "col", r#"from "data.csv""#, &["col".into()]);
        assert_eq!(v.prql, r#"from "data.csv" | group {`col`} (aggregate {Cnt = count this})"#);
    }

    #[test]
    fn test_prql_filtered() {
        let v = ViewState::new_filtered(0, "filtered", "data.parquet", vec!["a".into()], "x > 5", 10, r#"from "data.parquet""#, "x > 5");
        assert_eq!(v.prql, r#"from "data.parquet" | filter x > 5"#);
    }

    #[test]
    fn test_prql_folder() {
        let v = ViewState::new_folder(0, "ls", empty());
        assert_eq!(v.prql, "from df"); // folder now uses sqlite, path is "memory:id"
    }

    #[test]
    fn test_prql_chained_filter() {
        // First filter
        let v1 = ViewState::new_filtered(0, "f1", "data.parquet", vec![], "x > 5", 10, r#"from "data.parquet""#, "x > 5");
        assert_eq!(v1.prql, r#"from "data.parquet" | filter x > 5"#);
        // Second filter on top (chained)
        let v2 = ViewState::new_filtered(1, "f2", "data.parquet", vec![], "x > 5 AND y < 10", 5, &v1.prql, "y < 10");
        assert_eq!(v2.prql, r#"from "data.parquet" | filter x > 5 | filter y < 10"#);
    }

    #[test]
    fn test_prql_freq_on_filtered() {
        let v1 = ViewState::new_filtered(0, "f1", "data.parquet", vec![], "x > 5", 10, r#"from "data.parquet""#, "x > 5");
        let v2 = ViewState::new_freq(1, "freq", empty(), 0, 10, "f1", "col", &v1.prql, &["col".into()]);
        assert_eq!(v2.prql, r#"from "data.parquet" | filter x > 5 | group {`col`} (aggregate {Cnt = count this})"#);
    }

    #[test]
    fn test_prql_meta_on_filtered() {
        let v1 = ViewState::new_filtered(0, "f1", "data.parquet", vec![], "x > 5", 10, r#"from "data.parquet""#, "x > 5");
        let v2 = ViewState::new_meta(1, empty(), 0, 10, "f1", &v1.prql);
        assert_eq!(v2.prql, r#"from "data.parquet" | filter x > 5 | meta"#);
    }

    #[test]
    fn test_prql_corr_on_csv() {
        let v1 = ViewState::new(0, "data", empty(), Some("data.csv".into()));
        let v2 = ViewState::new_corr(1, empty(), &v1.prql);
        assert_eq!(v2.prql, "from df | corr"); // corr inherits parent's from df
    }

    #[test]
    fn test_prql_pivot() {
        let v1 = ViewState::new(0, "data", empty(), Some("data.csv".into()));
        let v2 = ViewState::new_pivot(1, "pivot", empty(), 0, "data", &v1.prql);
        assert_eq!(v2.prql, "from df | pivot"); // pivot inherits parent's from df
    }

    #[test]
    fn test_prql_multi_col_freq() {
        let v = ViewState::new_freq(0, "freq", empty(), 1, 100, "parent", "a", r#"from "data.csv""#, &["a".into(), "b".into()]);
        assert_eq!(v.prql, r#"from "data.csv" | group {`a`, `b`} (aggregate {Cnt = count this})"#);
    }
}
