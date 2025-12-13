use crate::keymap::KeyMap;
use crate::state::{StateStack, ViewState};
use crate::theme::Theme;
use anyhow::{anyhow, Result};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

/// Search state for n/N
#[derive(Clone, Default)]
pub struct SearchState {
    pub col_name: Option<String>,  // column being searched
    pub value: Option<String>,     // search value (SQL WHERE)
}

/// App context
pub struct AppContext {
    pub stack: StateStack,         // view stack
    pub history_file: PathBuf,     // cmd history file
    pub message: String,           // status bar msg
    next_id: usize,                // view id counter
    pub search: SearchState,       // search state
    pub bookmarks: Vec<usize>,     // bookmarked rows
    pub show_info: bool,           // toggle info box
    pub float_decimals: usize,     // decimal places for floats
    pub keymap: KeyMap,            // key bindings
    pub theme: Theme,              // color theme
}

impl AppContext {
    pub fn new() -> Self {
        let keymap = KeyMap::load(std::path::Path::new("cfg/key.csv")).unwrap_or_default();
        let theme = Theme::load_active();
        Self {
            stack: StateStack::new(),
            history_file: PathBuf::from("commands.txt"),
            message: String::new(),
            next_id: 0,
            search: SearchState::default(),
            bookmarks: Vec::new(),
            show_info: true,
            float_decimals: 3,
            keymap,
            theme,
        }
    }

    pub fn next_id(&mut self) -> usize { let i = self.next_id; self.next_id += 1; i }
    pub fn has_view(&self) -> bool { self.stack.has_view() }
    pub fn view(&self) -> Option<&ViewState> { self.stack.cur() }
    pub fn view_mut(&mut self) -> Option<&mut ViewState> { self.stack.cur_mut() }
    pub fn req(&self) -> Result<&ViewState> { self.view().ok_or_else(|| anyhow!("No table loaded")) }
    pub fn req_mut(&mut self) -> Result<&mut ViewState> { self.view_mut().ok_or_else(|| anyhow!("No table loaded")) }

    pub fn record(&mut self, cmd: &str) -> Result<()> {
        writeln!(OpenOptions::new().create(true).append(true).open(&self.history_file)?, "{}", cmd)?;
        Ok(())
    }

    pub fn msg(&mut self, s: String) { self.message = s; }
    pub fn err(&mut self, e: impl std::fmt::Display) { self.message = format!("Error: {}", e); }
    pub fn no_table(&mut self) { self.message = "No table loaded".into(); }

    pub fn viewport(&mut self, rows: u16, cols: u16) {
        if let Some(v) = self.stack.cur_mut() { v.state.viewport = (rows, cols); }
    }

    /// Navigate rows: +down, -up, MIN=top, MAX=bot
    pub fn nav_row(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = v.rows();
            match d {
                isize::MIN => v.state.top(),
                isize::MAX => v.state.bot(n),
                _ if d < 0 => v.state.up((-d) as usize),
                _ => v.state.down(d as usize, n),
            }
        }
    }

    /// Navigate cols: +right, -left
    pub fn nav_col(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = v.cols();
            if d < 0 { v.state.left((-d) as usize); }
            else { v.state.right(d as usize, n); }
        }
    }

    pub fn page(&self) -> isize {
        self.view().map(|v| (v.state.viewport.0 as isize).saturating_sub(2)).unwrap_or(10)
    }
}
