use polars::prelude::*;

/// Display cursor and viewport state for a table view
#[derive(Clone, Debug)]
pub struct TableState {
    /// First visible row (viewport top)
    pub r0: usize,
    /// Current row cursor
    pub cr: usize,
    /// Current column cursor
    pub cc: usize,
    /// Terminal dimensions (rows, cols)
    pub viewport: (u16, u16),
    /// Cached column widths
    pub col_widths: Vec<u16>,
    /// Row position where widths were calculated
    pub widths_calc_row: usize,
}

impl TableState {
    pub fn new() -> Self {
        Self {
            r0: 0,
            cr: 0,
            cc: 0,
            viewport: (0, 0),
            col_widths: Vec::new(),
            widths_calc_row: 0,
        }
    }

    /// Check if column widths need recalculation
    pub fn needs_width_recalc(&self) -> bool {
        let page_size = self.viewport.0.saturating_sub(2) as usize;
        // Recalculate if we've moved more than 1 page from where we last calculated
        self.col_widths.is_empty() || self.cr.abs_diff(self.widths_calc_row) > page_size
    }

    /// Get the current column name from the DataFrame
    pub fn current_column(&self, df: &DataFrame) -> Option<String> {
        df.get_column_names()
            .get(self.cc)
            .map(|s| s.to_string())
    }

    /// Move cursor down by n rows, adjusting viewport if needed
    pub fn move_down(&mut self, n: usize, max_rows: usize) {
        if max_rows == 0 {
            return;
        }
        self.cr = (self.cr + n).min(max_rows - 1);

        // Adjust viewport if cursor goes below visible area
        let visible_rows = (self.viewport.0 as usize).saturating_sub(2); // -2 for status bar
        if self.cr >= self.r0 + visible_rows {
            self.r0 = self.cr.saturating_sub(visible_rows - 1);
        }
    }

    /// Move cursor up by n rows, adjusting viewport if needed
    pub fn move_up(&mut self, n: usize) {
        self.cr = self.cr.saturating_sub(n);

        // Adjust viewport if cursor goes above visible area
        if self.cr < self.r0 {
            self.r0 = self.cr;
        }
    }

    /// Move cursor right by n columns (just update cc, renderer handles visibility)
    pub fn move_right(&mut self, n: usize, max_cols: usize) {
        if max_cols == 0 {
            return;
        }
        self.cc = (self.cc + n).min(max_cols - 1);
    }

    /// Move cursor left by n columns (just update cc, renderer handles visibility)
    pub fn move_left(&mut self, n: usize) {
        self.cc = self.cc.saturating_sub(n);
    }

    /// Jump to top of table
    pub fn goto_top(&mut self) {
        self.cr = 0;
        self.r0 = 0;
    }

    /// Jump to bottom of table
    pub fn goto_bottom(&mut self, max_rows: usize) {
        if max_rows == 0 {
            return;
        }
        self.cr = max_rows - 1;
        let visible_rows = (self.viewport.0 as usize).saturating_sub(2);
        self.r0 = self.cr.saturating_sub(visible_rows.saturating_sub(1));
    }

    /// Page down
    pub fn page_down(&mut self, max_rows: usize) {
        let page_size = (self.viewport.0 as usize).saturating_sub(2);
        self.move_down(page_size, max_rows);
    }

    /// Page up
    pub fn page_up(&mut self) {
        let page_size = (self.viewport.0 as usize).saturating_sub(2);
        self.move_up(page_size);
    }
}

/// Complete table view with history
#[derive(Clone)]
pub struct ViewState {
    /// Unique numeric ID for this view
    pub id: usize,
    /// View identifier (e.g., "main", "freq:col_name")
    pub name: String,
    /// Table data
    pub dataframe: DataFrame,
    /// Display state
    pub state: TableState,
    /// Command history for this view
    pub history: Vec<String>,
    /// Source filename if applicable
    pub filename: Option<String>,
    /// Whether to show row numbers
    pub show_row_numbers: bool,
    /// Parent view ID (for frequency tables)
    pub parent_id: Option<usize>,
    /// Column name used for frequency (for filtering parent)
    pub freq_col: Option<String>,
}

impl ViewState {
    pub fn new(id: usize, name: String, dataframe: DataFrame, filename: Option<String>) -> Self {
        Self {
            id,
            name,
            dataframe,
            state: TableState::new(),
            history: Vec::new(),
            filename,
            show_row_numbers: false,
            parent_id: None,
            freq_col: None,
        }
    }

    pub fn new_frequency(id: usize, name: String, dataframe: DataFrame, parent_id: usize, freq_col: String) -> Self {
        Self {
            id,
            name,
            dataframe,
            state: TableState::new(),
            history: Vec::new(),
            filename: None,
            show_row_numbers: false,
            parent_id: Some(parent_id),
            freq_col: Some(freq_col),
        }
    }

    /// Add a command to history
    pub fn add_to_history(&mut self, cmd: String) {
        self.history.push(cmd);
    }

    /// Get history as a display string
    #[allow(dead_code)]
    pub fn history_string(&self) -> String {
        self.history.join(" | ")
    }

    /// Get number of rows
    pub fn row_count(&self) -> usize {
        self.dataframe.height()
    }

    /// Get number of columns
    pub fn col_count(&self) -> usize {
        self.dataframe.width()
    }
}

/// Stack of table views for navigation
pub struct StateStack {
    stack: Vec<ViewState>,
}

impl StateStack {
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// Create with an initial view
    pub fn with_initial(view: ViewState) -> Self {
        Self { stack: vec![view] }
    }

    /// Push a new view onto the stack
    pub fn push(&mut self, view: ViewState) {
        self.stack.push(view);
    }

    /// Pop the top view (returns None if only one view remains)
    pub fn pop(&mut self) -> Option<ViewState> {
        if self.stack.len() > 1 {
            self.stack.pop()
        } else {
            None
        }
    }

    /// Get reference to current view
    pub fn current(&self) -> Option<&ViewState> {
        self.stack.last()
    }

    /// Get mutable reference to current view
    pub fn current_mut(&mut self) -> Option<&mut ViewState> {
        self.stack.last_mut()
    }

    /// Get stack size
    pub fn len(&self) -> usize {
        self.stack.len()
    }

    /// Check if stack is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }

    /// Check if we have a current view
    pub fn has_view(&self) -> bool {
        !self.stack.is_empty()
    }

    /// Find a view by ID
    pub fn find_by_id(&self, id: usize) -> Option<&ViewState> {
        self.stack.iter().find(|v| v.id == id)
    }

    /// Find a view by ID (mutable)
    pub fn find_by_id_mut(&mut self, id: usize) -> Option<&mut ViewState> {
        self.stack.iter_mut().find(|v| v.id == id)
    }
}
