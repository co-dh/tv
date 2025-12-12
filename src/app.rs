use crate::state::{StateStack, ViewState};
use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

/// Application context holding all state
pub struct AppContext {
    /// Stack of table views
    pub stack: StateStack,
    /// Path to command history file
    pub history_file: PathBuf,
    /// Message to display in status bar
    pub message: String,
}

impl AppContext {
    /// Create new app context with empty state
    pub fn new() -> Self {
        Self {
            stack: StateStack::new(),
            history_file: PathBuf::from("commands.txt"),
            message: String::from("Press L to load a file, q to quit"),
        }
    }

    /// Create with an initial view
    pub fn with_view(view: ViewState) -> Self {
        Self {
            stack: StateStack::with_initial(view),
            history_file: PathBuf::from("commands.txt"),
            message: String::new(),
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

    /// Update viewport size for current view
    pub fn update_viewport(&mut self, rows: u16, cols: u16) {
        if let Some(view) = self.stack.current_mut() {
            view.state.viewport = (rows, cols);
        }
    }
}
