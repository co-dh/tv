use crate::source::{Source, Gz, Memory, Polars};
use polars::prelude::*;
use std::collections::HashSet;

/// Table cursor/viewport state
#[derive(Clone, Debug)]
pub struct TableState {
    pub r0: usize,              // first visible row
    pub cr: usize,              // cursor row
    pub cc: usize,              // cursor col
    pub viewport: (u16, u16),   // (rows, cols)
    pub col_widths: Vec<u16>,   // cached widths
    pub widths_row: usize,      // row where widths calc'd
}

impl TableState {
    /// Create new state with cursor at origin
    pub fn new() -> Self {
        Self { r0: 0, cr: 0, cc: 0, viewport: (0, 0), col_widths: Vec::new(), widths_row: 0 }
    }

    /// Need width recalc if moved >1 page
    #[must_use]
    pub fn need_widths(&self) -> bool {
        self.col_widths.is_empty() || self.cr.abs_diff(self.widths_row) > self.viewport.0.saturating_sub(2) as usize
    }

    /// Move cursor down n rows, scroll if needed
    pub fn down(&mut self, n: usize, max: usize) {
        if max == 0 { return; }
        self.cr = (self.cr + n).min(max - 1);
        let vis = (self.viewport.0 as usize).saturating_sub(3);  // header + footer_header + status
        if self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis - 1); }
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
        let vis = (self.viewport.0 as usize).saturating_sub(3);
        if self.cr < self.r0 { self.r0 = self.cr; }
        else if self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis.saturating_sub(1)); }
    }

    /// Center cursor on screen only if not already visible
    pub fn center_if_needed(&mut self) {
        let vis = (self.viewport.0 as usize).saturating_sub(3);
        // Only center if cursor is outside visible area
        if self.cr < self.r0 || self.cr >= self.r0 + vis {
            let half = vis / 2;
            self.r0 = self.cr.saturating_sub(half);
        }
    }
}

/// View state
#[derive(Clone)]
pub struct ViewState {
    pub id: usize,
    pub name: String,
    pub dataframe: DataFrame,
    pub state: TableState,
    pub history: Vec<String>,
    pub filename: Option<String>,
    pub show_row_numbers: bool,
    pub parent_id: Option<usize>,    // for freq tables
    pub parent_rows: Option<usize>,  // parent table row count (for meta/freq status)
    pub parent_name: Option<String>, // parent table name (for meta/freq status)
    pub freq_col: Option<String>,    // freq column name
    pub selected_cols: HashSet<usize>,
    pub selected_rows: HashSet<usize>,
    pub gz_source: Option<String>,  // original .csv.gz path for streaming save
    pub stats_cache: Option<(usize, String)>,  // (col_idx, stats) cache
    pub col_separator: Option<usize>,  // draw separator bar after this column index
    pub meta_cache: Option<DataFrame>, // cached metadata stats for this view
    pub partial: bool,  // gz file not fully loaded (hit memory limit)
    pub disk_rows: Option<usize>,  // total rows on disk (for large parquet files)
    pub parquet_path: Option<String>,  // lazy parquet source (no in-memory df)
    pub col_names: Vec<String>,  // cached column names (for parquet views)
    pub sort_col: Option<String>,  // sort column for lazy parquet
    pub sort_desc: bool,  // sort descending
    pub filter_clause: Option<String>,  // SQL WHERE clause for filtered parquet views
    pub fetch_cache: Option<(usize, usize)>,  // (start, end) row range cached in dataframe
}

impl ViewState {
    /// Get source for this view (Polars for parquet, Gz for gzip, Memory for in-memory)
    pub fn source(&self) -> Box<dyn Source + '_> {
        if self.parquet_path.is_some() {
            Box::new(Polars)
        } else if self.gz_source.is_some() {
            Box::new(Gz { df: &self.dataframe, partial: self.partial })
        } else {
            Box::new(Memory(&self.dataframe))
        }
    }

    /// Get data path (parquet file or empty for in-memory)
    #[must_use]
    pub fn path(&self) -> &str {
        self.parquet_path.as_deref().unwrap_or("")
    }

    /// Get key columns (columns before separator)
    #[must_use]
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

    /// Base view with default values (all Options None, all flags false)
    fn base(id: usize, name: impl Into<String>, df: DataFrame) -> Self {
        let name = name.into();
        Self {
            id, name, dataframe: df, state: TableState::new(), history: Vec::new(),
            filename: None, show_row_numbers: false, parent_id: None, parent_rows: None, parent_name: None, freq_col: None,
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: None,
            stats_cache: None, col_separator: None, meta_cache: None, partial: false, disk_rows: None, parquet_path: None, col_names: Vec::new(),
            sort_col: None, sort_desc: false, filter_clause: None, fetch_cache: None,
        }
    }

    /// Create standard in-memory view (CSV, filtered results, etc.)
    pub fn new(id: usize, name: impl Into<String>, df: DataFrame, filename: Option<String>) -> Self {
        Self { filename, ..Self::base(id, name, df) }
    }

    /// Create lazy parquet view (no in-memory dataframe, all ops go to disk)
    pub fn new_parquet(id: usize, name: impl Into<String>, path: impl Into<String>, rows: usize, cols: Vec<String>) -> Self {
        let path = path.into();
        Self { filename: Some(path.clone()), disk_rows: Some(rows), parquet_path: Some(path), col_names: cols, ..Self::base(id, name, DataFrame::empty()) }
    }

    /// Create gzipped CSV view (may be partial if memory limit hit)
    pub fn new_gz(id: usize, name: impl Into<String>, df: DataFrame, filename: Option<String>, gz: impl Into<String>, partial: bool) -> Self {
        Self { filename, gz_source: Some(gz.into()), partial, ..Self::base(id, name, df) }
    }

    /// Create child view (freq/meta) with parent info
    pub fn new_child(id: usize, name: impl Into<String>, df: DataFrame, pid: usize, prows: usize, pname: impl Into<String>) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname.into()), ..Self::base(id, name, df) }
    }

    /// Create freq view with parent info
    pub fn new_freq(id: usize, name: impl Into<String>, df: DataFrame, pid: usize, prows: usize, pname: impl Into<String>, col: impl Into<String>) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname.into()), freq_col: Some(col.into()), ..Self::base(id, name, df) }
    }

    /// Create filtered parquet view (lazy - all ops go to disk with WHERE)
    pub fn new_filtered(id: usize, name: impl Into<String>, path: impl Into<String>, cols: Vec<String>, filter: impl Into<String>, count: usize) -> Self {
        let path = path.into();
        Self { filename: Some(path.clone()), disk_rows: Some(count), parquet_path: Some(path), col_names: cols, filter_clause: Some(filter.into()), ..Self::base(id, name, DataFrame::empty()) }
    }

    /// Add command to history
    pub fn add_hist(&mut self, cmd: impl Into<String>) { self.history.push(cmd.into()); }
    /// Row count: disk_rows for parquet, else dataframe height
    #[must_use]
    pub fn rows(&self) -> usize { self.disk_rows.unwrap_or_else(|| self.dataframe.height()) }
    /// Column count: from col_names for parquet, else dataframe width
    #[must_use]
    pub fn cols(&self) -> usize { if self.col_names.is_empty() { self.dataframe.width() } else { self.col_names.len() } }
    /// Get column name by index (works for both parquet and in-memory views)
    #[must_use]
    pub fn col_name(&self, idx: usize) -> Option<String> {
        if !self.col_names.is_empty() { self.col_names.get(idx).cloned() }
        else { self.dataframe.get_column_names().get(idx).map(|s| s.to_string()) }
    }
    /// Check if view uses row selection (meta/freq) vs column selection (table)
    #[must_use]
    pub fn is_row_sel(&self) -> bool { self.name == "metadata" || self.name.starts_with("Freq:") }
}

/// View stack - manages multiple views like browser tabs
pub struct StateStack { stack: Vec<ViewState> }

impl StateStack {
    /// Empty stack
    pub fn new() -> Self { Self { stack: Vec::new() } }
    /// Push new view on top (inherits viewport from current view)
    pub fn push(&mut self, mut v: ViewState) {
        if let Some(cur) = self.stack.last() { v.state.viewport = cur.state.viewport; }
        self.stack.push(v);
    }
    /// Pop top view (allows returning to empty state)
    pub fn pop(&mut self) -> Option<ViewState> { self.stack.pop() }
    /// Current view reference
    #[must_use]
    pub fn cur(&self) -> Option<&ViewState> { self.stack.last() }
    /// Current view mutable reference
    pub fn cur_mut(&mut self) -> Option<&mut ViewState> { self.stack.last_mut() }
    /// Stack depth
    #[must_use]
    pub fn len(&self) -> usize { self.stack.len() }
    /// Has any view
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_center_if_needed_visible_row_unchanged() {
        // If cursor is already visible, r0 should not change
        let mut state = TableState::new();
        state.viewport = (20, 80);  // 20 rows, 18 visible (minus 2 for header/status)
        state.r0 = 0;
        state.cr = 5;  // row 5 is visible (0-17)

        state.center_if_needed();
        assert_eq!(state.r0, 0, "r0 should not change when cursor is visible");
    }

    #[test]
    fn test_center_if_needed_above_visible_centers() {
        // If cursor is above visible area, center it
        let mut state = TableState::new();
        state.viewport = (20, 80);  // 17 visible rows (20 - 3 reserved)
        state.r0 = 100;
        state.cr = 50;  // row 50 is above visible area (100-116)

        state.center_if_needed();
        // Should center: r0 = cr - half = 50 - 8 = 42
        assert_eq!(state.r0, 42, "r0 should center cursor when above visible area");
    }

    #[test]
    fn test_center_if_needed_below_visible_centers() {
        // If cursor is below visible area, center it
        let mut state = TableState::new();
        state.viewport = (20, 80);  // 17 visible rows (20 - 3 reserved)
        state.r0 = 0;
        state.cr = 50;  // row 50 is below visible area (0-16)

        state.center_if_needed();
        // Should center: r0 = cr - half = 50 - 8 = 42
        assert_eq!(state.r0, 42, "r0 should center cursor when below visible area");
    }

    #[test]
    fn test_center_if_needed_at_boundary() {
        // Cursor at exact boundary of visible area
        let mut state = TableState::new();
        state.viewport = (20, 80);  // 17 visible rows (20 - 3 reserved)
        state.r0 = 0;
        state.cr = 16;  // last visible row (0-16)

        state.center_if_needed();
        assert_eq!(state.r0, 0, "r0 should not change when cursor is at last visible row");
    }

    #[test]
    fn test_stack_names() {
        use polars::prelude::*;
        let df = DataFrame::default();
        let mut stack = StateStack::new();
        stack.push(ViewState::new(0, "view1", df.clone(), None));
        stack.push(ViewState::new(1, "view2", df.clone(), None));
        stack.push(ViewState::new(2, "view3", df, None));

        let names = stack.names();
        assert_eq!(names, vec!["view1", "view2", "view3"]);
    }
}
