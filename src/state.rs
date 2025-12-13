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
    pub parent_id: Option<usize>,   // for freq tables
    pub freq_col: Option<String>,   // freq column name
    pub selected_cols: HashSet<usize>,
    pub selected_rows: HashSet<usize>,
    pub gz_source: Option<String>,  // original .csv.gz path for streaming save
    pub stats_cache: Option<(usize, String)>,  // (col_idx, stats) cache
    pub col_separator: Option<usize>,  // draw separator bar after this column index
}

impl ViewState {
    pub fn new(id: usize, name: String, df: DataFrame, filename: Option<String>) -> Self {
        Self {
            id, name, dataframe: df, state: TableState::new(), history: Vec::new(),
            filename, show_row_numbers: false, parent_id: None, freq_col: None,
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: None,
            stats_cache: None, col_separator: None,
        }
    }

    pub fn new_gz(id: usize, name: String, df: DataFrame, filename: Option<String>, gz: String) -> Self {
        Self {
            id, name, dataframe: df, state: TableState::new(), history: Vec::new(),
            filename, show_row_numbers: false, parent_id: None, freq_col: None,
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: Some(gz),
            stats_cache: None, col_separator: None,
        }
    }

    pub fn new_freq(id: usize, name: String, df: DataFrame, pid: usize, col: String) -> Self {
        Self {
            id, name, dataframe: df, state: TableState::new(), history: Vec::new(),
            filename: None, show_row_numbers: false, parent_id: Some(pid), freq_col: Some(col),
            selected_cols: HashSet::new(), selected_rows: HashSet::new(), gz_source: None,
            stats_cache: None, col_separator: None,
        }
    }

    pub fn add_hist(&mut self, cmd: String) { self.history.push(cmd); }
    pub fn rows(&self) -> usize { self.dataframe.height() }
    pub fn cols(&self) -> usize { self.dataframe.width() }
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
}
