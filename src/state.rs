use crate::data::table::{BoxTable, ColStats, SimpleTable, Cell};
use std::collections::HashSet;

/// Reserved rows in viewport (header + footer_header + status)
pub const RESERVED_ROWS: usize = 3;

/// Parent view info (for derived views like meta/freq)
#[derive(Clone, Debug, Default)]
pub struct ParentInfo {
    pub id: usize,
    pub rows: usize,
    pub name: String,
    pub freq_col: Option<String>,
}

/// View kind for dispatch
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewKind { Table, Meta, Freq, Corr, Folder, Pivot }

impl std::fmt::Display for ViewKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            Self::Table => "table", Self::Meta => "meta", Self::Freq => "freq",
            Self::Corr => "corr", Self::Folder => "folder", Self::Pivot => "pivot",
        })
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

    /// Number of visible data rows
    #[inline] #[must_use]
    pub fn visible_rows(&self) -> usize { (self.viewport.0 as usize).saturating_sub(RESERVED_ROWS) }

    /// Move cursor down
    pub fn down(&mut self, n: usize, max: usize) {
        if max == 0 { return; }
        self.cr = (self.cr + n).min(max - 1);
        let vis = self.visible_rows();
        if self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis - 1); }
    }

    /// Move cursor up
    pub fn up(&mut self, n: usize) {
        self.cr = self.cr.saturating_sub(n);
        if self.cr < self.r0 { self.r0 = self.cr; }
    }

    /// Move cursor right
    pub fn right(&mut self, n: usize, max: usize) { if max > 0 { self.cc = (self.cc + n).min(max - 1); } }
    /// Move cursor left
    pub fn left(&mut self, n: usize) { self.cc = self.cc.saturating_sub(n); }
    /// Jump to first row
    pub fn top(&mut self) { self.cr = 0; self.r0 = 0; }

    /// Ensure cursor is visible
    pub fn visible(&mut self) {
        let vis = self.visible_rows();
        if self.cr < self.r0 { self.r0 = self.cr; }
        else if self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis.saturating_sub(1)); }
    }

    /// Center cursor if not visible
    pub fn center_if_needed(&mut self) {
        let vis = self.visible_rows();
        if self.cr < self.r0 || self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis / 2); }
    }
}

/// Minimal view state - everything else derived from prql/path
pub struct ViewState {
    pub id: usize,
    pub name: String,
    pub kind: ViewKind,
    pub prql: String,                    // query chain
    pub path: Option<String>,            // query path (file, memory:id, source:...)
    pub data: BoxTable,                  // current viewport data
    pub state: TableState,
    pub parent: Option<ParentInfo>,
    pub selected_cols: HashSet<usize>,
    pub selected_rows: HashSet<usize>,
    pub col_separator: Option<usize>,
    pub col_order: Option<Vec<usize>>,   // display column order (xkey)
    pub history: Vec<String>,
    pub partial: bool,                   // gz truncated flag
}

/// Manual Clone - copies BoxTable
impl Clone for ViewState {
    fn clone(&self) -> Self {
        let t = self.data.as_ref();
        let data: BoxTable = Box::new(SimpleTable::new(
            t.col_names(),
            (0..t.cols()).map(|c| t.col_type(c)).collect(),
            (0..t.rows()).map(|r| (0..t.cols()).map(|c| t.cell(r, c)).collect()).collect()
        ));
        Self {
            id: self.id, name: self.name.clone(), kind: self.kind,
            prql: self.prql.clone(), path: self.path.clone(), data,
            state: self.state.clone(), parent: self.parent.clone(),
            selected_cols: self.selected_cols.clone(), selected_rows: self.selected_rows.clone(),
            col_separator: self.col_separator, col_order: self.col_order.clone(),
            history: self.history.clone(), partial: self.partial,
        }
    }
}

impl ViewState {
    /// Base view with defaults
    fn base(id: usize, name: impl Into<String>, kind: ViewKind, prql: String, data: BoxTable) -> Self {
        Self {
            id, name: name.into(), kind, prql, path: None, data,
            state: TableState::default(), parent: None,
            selected_cols: HashSet::new(), selected_rows: HashSet::new(),
            col_separator: None, col_order: None, history: Vec::new(), partial: false,
        }
    }

    fn empty_table() -> BoxTable { Box::new(SimpleTable::empty()) }

    /// Standard view (CSV, etc.)
    pub fn new(id: usize, name: impl Into<String>, data: BoxTable, path: Option<String>) -> Self {
        Self { path, ..Self::base(id, name, ViewKind::Table, "from df".into(), data) }
    }

    /// Lazy parquet view
    pub fn new_parquet(id: usize, name: impl Into<String>, path: impl Into<String>) -> Self {
        let p = path.into();
        Self { path: Some(p), ..Self::base(id, name, ViewKind::Table, "from df".into(), Self::empty_table()) }
    }

    /// Gzipped CSV view
    pub fn new_gz(id: usize, name: impl Into<String>, data: BoxTable, path: Option<String>, partial: bool) -> Self {
        Self { path, partial, ..Self::base(id, name, ViewKind::Table, "from df".into(), data) }
    }

    /// Metadata view
    pub fn new_meta(id: usize, data: BoxTable, pid: usize, prows: usize, pname: impl Into<String>, parent_prql: &str) -> Self {
        let prql = format!("{} | meta", parent_prql);
        let parent = ParentInfo { id: pid, rows: prows, name: pname.into(), freq_col: None };
        Self { parent: Some(parent), ..Self::base(id, "metadata", ViewKind::Meta, prql, data) }
    }

    /// Freq view
    pub fn new_freq(id: usize, name: impl Into<String>, data: BoxTable, pid: usize, prows: usize, pname: impl Into<String>, col: impl Into<String>, parent_prql: &str, grp_cols: &[String]) -> Self {
        let grp = grp_cols.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
        let prql = format!("{} | group {{{}}} (aggregate {{Cnt = count this}})", parent_prql, grp);
        let parent = ParentInfo { id: pid, rows: prows, name: pname.into(), freq_col: Some(col.into()) };
        Self { parent: Some(parent), ..Self::base(id, name, ViewKind::Freq, prql, data) }
    }

    /// Pivot view
    pub fn new_pivot(id: usize, name: impl Into<String>, data: BoxTable, pid: usize, pname: impl Into<String>, parent_prql: &str) -> Self {
        let prql = format!("{} | pivot", parent_prql);
        let parent = ParentInfo { id: pid, rows: 0, name: pname.into(), freq_col: None };
        Self { parent: Some(parent), ..Self::base(id, name, ViewKind::Pivot, prql, data) }
    }

    /// Correlation view
    pub fn new_corr(id: usize, data: BoxTable, parent_prql: &str) -> Self {
        Self::base(id, "correlation", ViewKind::Corr, format!("{} | corr", parent_prql), data)
    }

    /// Memory view (folder, system) - registers with sqlite
    pub fn new_memory(id: usize, name: impl Into<String>, kind: ViewKind, data: BoxTable) -> Self {
        let path = crate::data::dynload::register_table(id, data.as_ref());
        Self { path, ..Self::base(id, name, kind, "from df".into(), data) }
    }

    /// Source view (source:ps, source:ls:/path) - lazy via sqlite plugin
    pub fn new_source(id: usize, name: impl Into<String>, kind: ViewKind, source_path: impl Into<String>) -> Self {
        Self { path: Some(source_path.into()), ..Self::base(id, name, kind, "from df".into(), Self::empty_table()) }
    }

    /// Add to history
    pub fn add_hist(&mut self, cmd: impl Into<String>) { self.history.push(cmd.into()); }

    /// Row count from data
    #[inline] #[must_use]
    pub fn rows(&self) -> usize { self.data.rows() }

    /// Column count from data
    #[inline] #[must_use]
    pub fn cols(&self) -> usize { self.data.cols() }

    /// Column names from data
    #[must_use]
    pub fn col_names(&self) -> Vec<String> { self.data.col_names() }

    /// Column name by index
    #[must_use]
    pub fn col_name(&self, idx: usize) -> Option<String> { self.data.col_name(idx) }

    /// Key columns (before separator)
    #[must_use]
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| self.col_names().into_iter().take(sep).collect()).unwrap_or_default()
    }

    /// Row selection mode (meta/freq)
    #[inline] #[must_use]
    pub fn is_row_sel(&self) -> bool { matches!(self.kind, ViewKind::Meta | ViewKind::Freq) }

    /// Get column indices in display order (respects col_order if set)
    #[must_use]
    pub fn display_cols(&self) -> Vec<usize> {
        self.col_order.clone().unwrap_or_else(|| (0..self.data.cols()).collect())
    }

    /// Map display column index to data column index
    #[inline] #[must_use]
    pub fn data_col(&self, display_idx: usize) -> usize {
        self.col_order.as_ref().and_then(|o| o.get(display_idx).copied()).unwrap_or(display_idx)
    }

    /// Get cell
    #[must_use]
    pub fn cell(&self, row: usize, col: usize) -> Cell { self.data.cell(row, col) }

    /// Column statistics
    #[must_use]
    pub fn col_stats(&self, col_idx: usize) -> ColStats {
        let n = self.data.rows();
        if n == 0 { return ColStats::default(); }
        let name = self.data.col_name(col_idx).unwrap_or_default();
        let typ = self.data.col_type(col_idx);
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
        let distinct = { let mut u = vals.clone(); u.sort(); u.dedup(); u.len() };
        if !nums.is_empty() {
            nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let min = nums.first().copied().unwrap_or(0.0);
            let max = nums.last().copied().unwrap_or(0.0);
            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
            let var = nums.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / nums.len() as f64;
            ColStats { name, dtype: format!("{:?}", typ), null_pct, distinct, min: format!("{:.2}", min), max: format!("{:.2}", max), median: format!("{:.2}", mean), sigma: format!("{:.2}", var.sqrt()) }
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

/// View stack
#[derive(Default)]
pub struct StateStack { stack: Vec<ViewState> }

impl StateStack {
    pub fn push(&mut self, mut v: ViewState) {
        if let Some(cur) = self.stack.last() { v.state.viewport = cur.state.viewport; }
        self.stack.push(v);
    }
    pub fn pop(&mut self) -> Option<ViewState> { self.stack.pop() }
    #[inline] #[must_use] pub fn cur(&self) -> Option<&ViewState> { self.stack.last() }
    #[inline] pub fn cur_mut(&mut self) -> Option<&mut ViewState> { self.stack.last_mut() }
    #[inline] #[must_use] pub fn len(&self) -> usize { self.stack.len() }
    #[inline] #[must_use] pub fn is_empty(&self) -> bool { self.stack.is_empty() }
    #[inline] #[must_use] pub fn has_view(&self) -> bool { !self.stack.is_empty() }
    pub fn find_mut(&mut self, id: usize) -> Option<&mut ViewState> { self.stack.iter_mut().find(|v| v.id == id) }
    pub fn swap(&mut self) { let n = self.stack.len(); if n >= 2 { self.stack.swap(n - 1, n - 2); } }
    #[must_use] pub fn names(&self) -> Vec<String> { self.stack.iter().map(|v| v.name.clone()).collect() }
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
    fn test_center_if_needed() {
        let mut s = TableState { viewport: (20, 80), r0: 100, cr: 50, ..Default::default() };
        s.center_if_needed();
        assert_eq!(s.r0, 42);
    }

    #[test]
    fn test_prql_freq() {
        let v = ViewState::new_freq(0, "freq", empty(), 1, 100, "parent", "col", "from df", &["col".into()]);
        assert!(v.prql.contains("group"));
    }
}
