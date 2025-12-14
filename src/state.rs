use crate::backend::{Backend, Gz, Memory, Polars};
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
    pub fn new() -> Self {
        Self { r0: 0, cr: 0, cc: 0, viewport: (0, 0), col_widths: Vec::new(), widths_row: 0 }
    }

    /// Need width recalc if moved >1 page
    pub fn need_widths(&self) -> bool {
        self.col_widths.is_empty() || self.cr.abs_diff(self.widths_row) > self.viewport.0.saturating_sub(2) as usize
    }

    pub fn cur_col(&self, df: &DataFrame) -> Option<String> {
        df.get_column_names().get(self.cc).map(|s| s.to_string())
    }

    pub fn down(&mut self, n: usize, max: usize) {
        if max == 0 { return; }
        self.cr = (self.cr + n).min(max - 1);
        let vis = (self.viewport.0 as usize).saturating_sub(2);
        if self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis - 1); }
    }

    pub fn up(&mut self, n: usize) {
        self.cr = self.cr.saturating_sub(n);
        if self.cr < self.r0 { self.r0 = self.cr; }
    }

    pub fn right(&mut self, n: usize, max: usize) {
        if max > 0 { self.cc = (self.cc + n).min(max - 1); }
    }

    pub fn left(&mut self, n: usize) { self.cc = self.cc.saturating_sub(n); }
    pub fn top(&mut self) { self.cr = 0; self.r0 = 0; }

    pub fn bot(&mut self, max: usize) {
        if max == 0 { return; }
        self.cr = max - 1;
        self.r0 = self.cr.saturating_sub((self.viewport.0 as usize).saturating_sub(3));
    }

    pub fn visible(&mut self) {
        let vis = (self.viewport.0 as usize).saturating_sub(2);
        if self.cr < self.r0 { self.r0 = self.cr; }
        else if self.cr >= self.r0 + vis { self.r0 = self.cr.saturating_sub(vis.saturating_sub(1)); }
    }

    /// Center cursor on screen only if not already visible
    pub fn center_if_needed(&mut self) {
        let vis = (self.viewport.0 as usize).saturating_sub(2);
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
}

impl ViewState {
    /// Get backend for this view (Polars for parquet, Gz for gzip, Memory for in-memory)
    pub fn backend(&self) -> Box<dyn Backend + '_> {
        if self.parquet_path.is_some() {
            Box::new(Polars)
        } else if self.gz_source.is_some() {
            Box::new(Gz { df: &self.dataframe, partial: self.partial })
        } else {
            Box::new(Memory(&self.dataframe, self.key_cols()))
        }
    }

    /// Get data path (parquet file or empty for in-memory)
    pub fn path(&self) -> &str {
        self.parquet_path.as_deref().unwrap_or("")
    }

    /// Get key columns (columns before separator)
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }
}

impl ViewState {
    pub fn new(id: usize, name: String, df: DataFrame, filename: Option<String>) -> Self {
        Self {
            id, name, dataframe: df, state: TableState::new(), history: Vec::new(),
            filename, show_row_numbers: false, parent_id: None, parent_rows: None, parent_name: None, freq_col: None,
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: None,
            stats_cache: None, col_separator: None, meta_cache: None, partial: false, disk_rows: None, parquet_path: None, col_names: Vec::new(),
            sort_col: None, sort_desc: false,
        }
    }

    /// Create lazy parquet view (no in-memory dataframe, all ops go to disk)
    pub fn new_parquet(id: usize, name: String, path: String, total_rows: usize, cols: Vec<String>) -> Self {
        Self {
            id, name, dataframe: DataFrame::empty(), state: TableState::new(), history: Vec::new(),
            filename: Some(path.clone()), show_row_numbers: false, parent_id: None, parent_rows: None, parent_name: None, freq_col: None,
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: None,
            stats_cache: None, col_separator: None, meta_cache: None, partial: false, disk_rows: Some(total_rows), parquet_path: Some(path), col_names: cols,
            sort_col: None, sort_desc: false,
        }
    }

    pub fn new_gz(id: usize, name: String, df: DataFrame, filename: Option<String>, gz: String, partial: bool) -> Self {
        Self {
            id, name, dataframe: df, state: TableState::new(), history: Vec::new(),
            filename, show_row_numbers: false, parent_id: None, parent_rows: None, parent_name: None, freq_col: None,
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: Some(gz),
            stats_cache: None, col_separator: None, meta_cache: None, partial, disk_rows: None, parquet_path: None, col_names: Vec::new(),
            sort_col: None, sort_desc: false,
        }
    }

    /// Create child view (freq/meta) with parent info
    pub fn new_child(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String) -> Self {
        Self {
            id, name, dataframe: df, state: TableState::new(), history: Vec::new(),
            filename: None, show_row_numbers: false, parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), freq_col: None,
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: None,
            stats_cache: None, col_separator: None, meta_cache: None, partial: false, disk_rows: None, parquet_path: None, col_names: Vec::new(),
            sort_col: None, sort_desc: false,
        }
    }

    /// Create freq view with parent info
    pub fn new_freq(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String, col: String) -> Self {
        Self {
            id, name, dataframe: df, state: TableState::new(), history: Vec::new(),
            filename: None, show_row_numbers: false, parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), freq_col: Some(col),
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: None,
            stats_cache: None, col_separator: None, meta_cache: None, partial: false, disk_rows: None, parquet_path: None, col_names: Vec::new(),
            sort_col: None, sort_desc: false,
        }
    }

    pub fn add_hist(&mut self, cmd: String) { self.history.push(cmd); }
    /// Row count: from disk_rows for parquet, else dataframe height
    pub fn rows(&self) -> usize { self.disk_rows.unwrap_or_else(|| self.dataframe.height()) }
    /// Column count: from col_names for parquet, else dataframe width
    pub fn cols(&self) -> usize { if self.col_names.is_empty() { self.dataframe.width() } else { self.col_names.len() } }
    /// Get column name by index (works for both parquet and in-memory views)
    pub fn col_name(&self, idx: usize) -> Option<String> {
        if !self.col_names.is_empty() { self.col_names.get(idx).cloned() }
        else { self.dataframe.get_column_names().get(idx).map(|s| s.to_string()) }
    }
    /// Check if view uses row selection (meta/freq) vs column selection (table)
    pub fn is_row_sel(&self) -> bool { self.name == "metadata" || self.name.starts_with("Freq:") }
}

/// View stack
pub struct StateStack { stack: Vec<ViewState> }

impl StateStack {
    pub fn new() -> Self { Self { stack: Vec::new() } }
    pub fn init(v: ViewState) -> Self { Self { stack: vec![v] } }
    pub fn push(&mut self, v: ViewState) { self.stack.push(v); }
    pub fn pop(&mut self) -> Option<ViewState> { if self.stack.len() > 1 { self.stack.pop() } else { None } }
    pub fn cur(&self) -> Option<&ViewState> { self.stack.last() }
    pub fn cur_mut(&mut self) -> Option<&mut ViewState> { self.stack.last_mut() }
    pub fn len(&self) -> usize { self.stack.len() }
    pub fn has_view(&self) -> bool { !self.stack.is_empty() }
    pub fn find_mut(&mut self, id: usize) -> Option<&mut ViewState> { self.stack.iter_mut().find(|v| v.id == id) }

    pub fn swap(&mut self) {
        let n = self.stack.len();
        if n >= 2 { self.stack.swap(n - 1, n - 2); }
    }

    /// Get names of all views in stack
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
        state.viewport = (20, 80);  // 18 visible rows
        state.r0 = 100;
        state.cr = 50;  // row 50 is above visible area (100-117)

        state.center_if_needed();
        // Should center: r0 = cr - half = 50 - 9 = 41
        assert_eq!(state.r0, 41, "r0 should center cursor when above visible area");
    }

    #[test]
    fn test_center_if_needed_below_visible_centers() {
        // If cursor is below visible area, center it
        let mut state = TableState::new();
        state.viewport = (20, 80);  // 18 visible rows
        state.r0 = 0;
        state.cr = 50;  // row 50 is below visible area (0-17)

        state.center_if_needed();
        // Should center: r0 = cr - half = 50 - 9 = 41
        assert_eq!(state.r0, 41, "r0 should center cursor when below visible area");
    }

    #[test]
    fn test_center_if_needed_at_boundary() {
        // Cursor at exact boundary of visible area
        let mut state = TableState::new();
        state.viewport = (20, 80);  // 18 visible rows
        state.r0 = 0;
        state.cr = 17;  // last visible row (0-17)

        state.center_if_needed();
        assert_eq!(state.r0, 0, "r0 should not change when cursor is at last visible row");
    }

    #[test]
    fn test_stack_names() {
        use polars::prelude::*;
        let df = DataFrame::default();
        let mut stack = StateStack::new();
        stack.push(ViewState::new(0, "view1".into(), df.clone(), None));
        stack.push(ViewState::new(1, "view2".into(), df.clone(), None));
        stack.push(ViewState::new(2, "view3".into(), df, None));

        let names = stack.names();
        assert_eq!(names, vec!["view1", "view2", "view3"]);
    }
}
