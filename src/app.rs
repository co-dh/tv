use crate::state::{StateStack, ViewState};
use anyhow::{anyhow, Result};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

/// Search state for n/N navigation
#[derive(Clone, Default)]
pub struct SearchState {
    /// Column name being searched
    pub col_name: Option<String>,
    /// Search value
    pub value: Option<String>,
    /// Regex pattern for regex search
    pub regex: Option<String>,
}

/// Application context holding all state
pub struct AppContext {
    /// Stack of table views
    pub stack: StateStack,
    /// Path to command history file
    pub history_file: PathBuf,
    /// Message to display in status bar
    pub message: String,
    /// Counter for generating view IDs
    next_view_id: usize,
    /// Current search state for n/N
    pub search: SearchState,
    /// Bookmarked row indices for current view
    pub bookmarks: Vec<usize>,
    /// Show info box (toggle with I)
    pub show_info: bool,
}

impl AppContext {
    /// Create new app context with empty state
    pub fn new() -> Self {
        Self {
            stack: StateStack::new(),
            history_file: PathBuf::from("commands.txt"),
            message: String::from("Press L to load a file, q to quit"),
            next_view_id: 0,
            search: SearchState::default(),
            bookmarks: Vec::new(),
            show_info: true,
        }
    }

    /// Get next view ID
    pub fn next_id(&mut self) -> usize {
        let id = self.next_view_id;
        self.next_view_id += 1;
        id
    }

    /// Create with an initial view
    #[allow(dead_code)]
    pub fn with_view(view: ViewState) -> Self {
        Self {
            stack: StateStack::with_initial(view),
            history_file: PathBuf::from("commands.txt"),
            message: String::new(),
            next_view_id: 1,
            search: SearchState::default(),
            bookmarks: Vec::new(),
            show_info: true,
        }
    }

    /// Check if we have a current view
    pub fn has_view(&self) -> bool {
        self.stack.has_view()
    }

    /// Get current view
    pub fn current_view(&self) -> Option<&ViewState> {
        self.stack.current()
    }

    /// Get mutable reference to current view
    pub fn current_view_mut(&mut self) -> Option<&mut ViewState> {
        self.stack.current_mut()
    }

    /// Get current view or error if none
    pub fn require_view(&self) -> Result<&ViewState> {
        self.current_view().ok_or_else(|| anyhow!("No table loaded"))
    }

    /// Get mutable current view or error if none
    pub fn require_view_mut(&mut self) -> Result<&mut ViewState> {
        self.current_view_mut().ok_or_else(|| anyhow!("No table loaded"))
    }

    /// Record a command to history file
    pub fn record_command(&mut self, cmd: &str) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.history_file)?;

        writeln!(file, "{}", cmd)?;
        Ok(())
    }

    /// Set status message
    pub fn set_message(&mut self, msg: String) {
        self.message = msg;
    }

    /// Set error message
    pub fn set_error(&mut self, e: impl std::fmt::Display) {
        self.message = format!("Error: {}", e);
    }

    /// Set no table loaded message
    pub fn no_table(&mut self) {
        self.message = "No table loaded".to_string();
    }

    /// Update viewport size for current view
    pub fn update_viewport(&mut self, rows: u16, cols: u16) {
        if let Some(view) = self.stack.current_mut() {
            view.state.viewport = (rows, cols);
        }
    }

    /// Navigate rows: positive = down, negative = up
    /// isize::MIN = top, isize::MAX = bottom
    pub fn nav_row(&mut self, delta: isize) {
        if let Some(view) = self.current_view_mut() {
            let max_rows = view.row_count();
            match delta {
                isize::MIN => view.state.goto_top(),
                isize::MAX => view.state.goto_bottom(max_rows),
                d if d < 0 => view.state.move_up((-d) as usize),
                d => view.state.move_down(d as usize, max_rows),
            }
        }
    }

    /// Navigate columns: positive = right, negative = left
    pub fn nav_col(&mut self, delta: isize) {
        if let Some(view) = self.current_view_mut() {
            let max_cols = view.col_count();
            if delta < 0 {
                view.state.move_left((-delta) as usize);
            } else {
                view.state.move_right(delta as usize, max_cols);
            }
        }
    }

    /// Get page size for navigation
    pub fn page_size(&self) -> isize {
        self.current_view()
            .map(|v| (v.state.viewport.0 as isize).saturating_sub(2))
            .unwrap_or(10)
    }
}
