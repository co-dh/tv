use crate::data::table::{BoxTable, ColStats, SimpleTable, Cell};
use crate::data::backend;
use std::collections::HashSet;

/// Reserved rows in viewport (header + footer_header + status + tabs)
pub const RESERVED_ROWS: usize = 4;

/// Chunk size for data fetching (aligns queries for cache hits)
pub const CHUNK: usize = 1000;

/// Build PRQL take query for chunk at offset (1-based range for PRQL)
pub fn take_chunk(prql: &str, offset: usize) -> String {
    let start = (offset / CHUNK) * CHUNK;
    let end = start + CHUNK;
    format!("{}|take {}..{}", prql, start + 1, end + 1)
}

/// Parent view info (for derived views like meta/freq)
#[derive(Clone, Debug, Default)]
pub struct ParentInfo {
    pub id: usize,
    pub rows: usize,
    pub name: String,
    pub freq_col: Option<String>,
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
    pub prql: String,                    // query chain
    pub path: Option<String>,            // query path (file, memory:id, source:...)
    pub data: BoxTable,                  // current viewport data
    pub state: TableState,
    pub parent: Option<ParentInfo>,
    pub selected_cols: HashSet<usize>,
    pub selected_rows: HashSet<usize>,
    pub key_cols: Vec<String>,           // xkey columns (display first)
    pub deleted_cols: Vec<String>,       // deleted columns (hidden)
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
            id: self.id, name: self.name.clone(),
            prql: self.prql.clone(), path: self.path.clone(), data,
            state: self.state.clone(), parent: self.parent.clone(),
            selected_cols: self.selected_cols.clone(), selected_rows: self.selected_rows.clone(),
            key_cols: self.key_cols.clone(), deleted_cols: self.deleted_cols.clone(), partial: self.partial,
        }
    }
}

impl ViewState {
    fn empty() -> BoxTable { Box::new(SimpleTable::empty()) }

    /// Builder: start with id + name, chain setters
    pub fn build(id: usize, name: impl Into<String>) -> Self {
        Self {
            id, name: name.into(), prql: "from df".into(),
            path: None, data: Self::empty(), state: TableState::default(),
            parent: None, selected_cols: HashSet::new(), selected_rows: HashSet::new(),
            key_cols: Vec::new(), deleted_cols: Vec::new(), partial: false,
        }
    }

    // Builder methods
    pub fn prql(mut self, p: impl Into<String>) -> Self { self.prql = p.into(); self }
    pub fn data(mut self, d: BoxTable) -> Self { self.data = d; self }
    pub fn partial(mut self) -> Self { self.partial = true; self }
    /// Set path for querying
    pub fn path(mut self, p: impl Into<String>) -> Self { self.path = Some(p.into()); self }
    /// Set parent info
    pub fn parent(mut self, id: usize, rows: usize, name: impl Into<String>, freq_col: Option<String>) -> Self {
        self.parent = Some(ParentInfo { id, rows, name: name.into(), freq_col }); self
    }
    /// Register data for in-memory querying, set path to mem:id
    pub fn register(mut self) -> Self {
        self.path = backend::register_table(self.id, self.data.as_ref()); self
    }

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

    /// Number of key columns (for separator position)
    #[inline] #[must_use]
    pub fn col_separator(&self) -> usize { self.key_cols.len() }

    /// Row selection mode (meta/freq) - detected by name prefix
    #[inline] #[must_use]
    pub fn is_row_sel(&self) -> bool { self.name == "meta" || self.name.starts_with("freq ") }

    /// Display name with del/xkey suffix (for tabs)
    #[must_use]
    pub fn display_name(&self) -> String {
        use crate::util::pure::qcols;
        let mut s = self.name.clone();
        if !self.deleted_cols.is_empty() { s = format!("{}|del{{{}}}", s, self.deleted_cols.join(",")); }
        // Skip xkey for freq (column already in name)
        if !self.key_cols.is_empty() && !self.name.starts_with("freq ") {
            s = format!("{}|xkey{{{}}}", s, qcols(&self.key_cols));
        }
        s
    }

    /// Get column indices in display order: key_cols first, then rest minus deleted
    #[must_use]
    pub fn display_cols(&self) -> Vec<usize> {
        let cols = self.data.col_names();
        // Key columns first (by name → index)
        let mut order: Vec<usize> = self.key_cols.iter()
            .filter_map(|k| cols.iter().position(|c| c == k))
            .collect();
        // Then remaining columns (not key, not deleted)
        for (i, name) in cols.iter().enumerate() {
            if !self.key_cols.contains(name) && !self.deleted_cols.contains(name) {
                order.push(i);
            }
        }
        order
    }

    /// Map display column index to data column index
    #[inline] #[must_use]
    pub fn data_col(&self, display_idx: usize) -> usize {
        self.display_cols().get(display_idx).copied().unwrap_or(display_idx)
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

    /// Total rows via backend (or in-memory fallback)
    #[must_use]
    pub fn total_rows(&self) -> usize {
        self.path.as_ref().and_then(|p| {
            let q = format!("{}|cnt", self.prql);
            let t = backend::query(&q, p)?;
            match t.cell(0, 0) { Cell::Int(n) => Some(n as usize), _ => None }
        }).unwrap_or_else(|| self.rows())
    }

    /// Select rows matching PRQL filter, return matching row indices (0-based)
    pub fn sel_rows(&self, expr: &str) -> Vec<usize> {
        self.path.as_ref().and_then(|p| {
            let q = format!("{}|derive{{_row=row_number this}}|filter {}|select{{_row}}", self.prql, expr);
            let t = backend::query(&q, p)?;
            Some((0..t.rows()).filter_map(|r| {
                match t.cell(r, 0) { Cell::Int(n) => Some((n - 1) as usize), _ => None }
            }).collect())
        }).unwrap_or_default()
    }

    /// Column stats via backend (or in-memory fallback)
    #[must_use]
    pub fn col_stats_plugin(&self, col_idx: usize) -> ColStats {
        use crate::data::table::ColType;
        use crate::util::pure::qcol;
        self.path.as_ref().and_then(|p| {
            let schema = backend::query(&take_chunk(&self.prql, self.state.r0), p)?;
            let col_name = schema.col_name(col_idx)?;
            let col_type = schema.col_type(col_idx);
            let is_num = matches!(col_type, ColType::Int | ColType::Float);
            let c = qcol(&col_name);
            let q = if is_num {
                format!("{}|stats this.{}", self.prql, c)
            } else {
                format!("{}|cntdist this.{}", self.prql, c)
            };
            let t = backend::query(&q, p)?;
            if is_num && t.cols() >= 5 {
                let min = match t.cell(0, 1) { Cell::Float(f) => format!("{:.2}", f), Cell::Int(i) => i.to_string(), _ => String::new() };
                let max = match t.cell(0, 2) { Cell::Float(f) => format!("{:.2}", f), Cell::Int(i) => i.to_string(), _ => String::new() };
                let avg = match t.cell(0, 3) { Cell::Float(f) => format!("{:.2}", f), _ => String::new() };
                let std = match t.cell(0, 4) { Cell::Float(f) => format!("{:.2}", f), _ => String::new() };
                Some(ColStats { name: col_name, dtype: format!("{:?}", col_type), null_pct: 0.0, distinct: 0, min, max, median: avg, sigma: std })
            } else if !is_num && t.cols() >= 2 {
                let dist = match t.cell(0, 1) { Cell::Int(i) => i as usize, _ => 0 };
                Some(ColStats { name: col_name, dtype: format!("{:?}", col_type), null_pct: 0.0, distinct: dist, ..Default::default() })
            } else { None }
        }).unwrap_or_else(|| self.col_stats(col_idx))
    }
}

impl std::fmt::Display for ViewState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}x{}]", self.name, self.rows(), self.cols())
    }
}

/// Free memory table when view dropped
impl Drop for ViewState {
    fn drop(&mut self) {
        backend::unregister_table(self.id);
    }
}

/// View stack
#[derive(Default)]
pub struct StateStack {
    stack: Vec<ViewState>,
    viewport: (u16, u16),  // cached viewport for new views when stack is empty
}

impl StateStack {
    /// Set viewport (call when terminal resizes)
    pub fn set_viewport(&mut self, rows: u16, cols: u16) { self.viewport = (rows, cols); }

    pub fn push(&mut self, mut v: ViewState) {
        // Inherit viewport from current view, or use cached viewport if stack empty
        v.state.viewport = self.stack.last().map(|c| c.state.viewport).unwrap_or(self.viewport);
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
    #[must_use] pub fn names(&self) -> Vec<String> { self.stack.iter().map(|v| v.display_name()).collect() }
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
        let v = ViewState::build(0, "freq col")
            .prql("from df|group{col}(aggregate{Cnt=count this})|sort{-Cnt}")
            .data(empty())
            .parent(1, 100, "parent", Some("col".into()));
        assert!(v.prql.contains("group"));
        assert!(v.is_row_sel());
    }

    #[test]
    fn test_display_name_basic() {
        let v = ViewState::build(0, "view").data(empty());
        assert_eq!(v.display_name(), "view");
    }

    #[test]
    fn test_display_name_xkey() {
        let mut v = ViewState::build(0, "view").data(empty());
        v.key_cols = vec!["a".into(), "b".into()];
        assert_eq!(v.display_name(), "view|xkey{a,b}");
    }

    #[test]
    fn test_display_name_del() {
        let mut v = ViewState::build(0, "view").data(empty());
        v.deleted_cols = vec!["x".into()];
        assert_eq!(v.display_name(), "view|del{x}");
    }

    #[test]
    fn test_display_name_both() {
        let mut v = ViewState::build(0, "view").data(empty());
        v.deleted_cols = vec!["x".into()];
        v.key_cols = vec!["a".into()];
        assert_eq!(v.display_name(), "view|del{x}|xkey{a}");
    }

    #[test]
    fn test_display_name_reserved() {
        let mut v = ViewState::build(0, "view").data(empty());
        v.key_cols = vec!["date".into(), "time".into()];
        assert_eq!(v.display_name(), "view|xkey{`date`,`time`}");
    }

    #[test]
    fn test_display_name_freq_no_xkey() {
        // Freq views should not show xkey suffix (column already in name)
        let mut v = ViewState::build(0, "freq a").data(empty());
        v.key_cols = vec!["a".into()];
        assert_eq!(v.display_name(), "freq a");
    }
}
