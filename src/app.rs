//! Application context - global state, view stack, background tasks

use crate::error::TvError;
use crate::keymap::KeyMap;
use crate::plugin::Registry;
use crate::state::{StateStack, ViewState};
use crate::theme::Theme;
use crate::render::Renderer;
use anyhow::Result;
use ratatui::crossterm::event::{self, Event, KeyEvent};
use ratatui::DefaultTerminal;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;

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
    pub info_mode: u8,             // info box: 0=off, 1=help, 2=help+prql
    pub float_decimals: usize,     // decimal places for floats
    pub keymap: KeyMap,            // key bindings
    pub theme: Theme,              // color theme
    pub plugins: Registry,         // plugin registry
    pub bg_saver: Option<Receiver<String>>,      // background save status
    pub raw_save: bool,            // --raw: skip type detection on save
    pub needs_redraw: bool,  // force full redraw (after leaving alternate screen)
    pub needs_center: bool,  // center cursor after viewport update (for search)
}

impl Default for AppContext {
    /// Create app context with default settings, keymap, theme, plugins
    fn default() -> Self {
        let history_file = dirs::home_dir()
            .map(|h| h.join(".tv").join("history"))
            .unwrap_or_else(|| PathBuf::from("history"));
        if let Some(dir) = history_file.parent() { let _ = std::fs::create_dir_all(dir); }
        Self {
            stack: StateStack::default(),
            history_file,
            message: String::new(),
            next_id: 0,
            search: SearchState::default(),
            bookmarks: Vec::new(),
            info_mode: 1,  // start with help visible
            float_decimals: 3,
            keymap: KeyMap::default(),
            theme: Theme::load_active(),
            plugins: Registry::new(std::path::Path::new("cfg/plugins.csv")),
            bg_saver: None,
            raw_save: false,
            needs_redraw: false,
            needs_center: false,
        }
    }
}

impl AppContext {

    /// Check if background loading is in progress (stub - will use plugin)
    pub fn is_loading(&self) -> bool { false }

    /// Check for background save status updates
    pub fn check_bg_saver(&mut self) {
        use std::sync::mpsc::TryRecvError;
        let Some(rx) = &self.bg_saver else { return };
        loop {
            match rx.try_recv() {
                Ok(msg) => self.message = msg,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => { self.bg_saver = None; break; }
            }
        }
    }

    /// Get next unique view ID (auto-increment)
    pub fn next_id(&mut self) -> usize { let i = self.next_id; self.next_id += 1; i }
    /// Check if any view exists
    pub fn has_view(&self) -> bool { self.stack.has_view() }
    /// Get current view (top of stack)
    pub fn view(&self) -> Option<&ViewState> { self.stack.cur() }
    /// Get mutable current view
    pub fn view_mut(&mut self) -> Option<&mut ViewState> { self.stack.cur_mut() }
    /// Get current view or error if none
    pub fn req(&self) -> Result<&ViewState> { self.view().ok_or_else(|| TvError::NoTable.into()) }
    /// Get mutable current view or error if none
    pub fn req_mut(&mut self) -> Result<&mut ViewState> { self.view_mut().ok_or_else(|| TvError::NoTable.into()) }

    /// Append command to history file
    pub fn record(&mut self, cmd: &str) -> Result<()> {
        writeln!(OpenOptions::new().create(true).append(true).open(&self.history_file)?, "{}", cmd)?;
        Ok(())
    }

    /// Set status message
    pub fn msg(&mut self, s: impl Into<String>) { self.message = s.into(); }
    /// Set error message
    pub fn err(&mut self, e: impl std::fmt::Display) { self.message = format!("Error: {}", e); }
    /// Set "no table" message
    pub fn no_table(&mut self) { self.message = "No table loaded".into(); }

    /// Update viewport size (terminal rows/cols)
    pub fn viewport(&mut self, rows: u16, cols: u16) {
        if let Some(v) = self.stack.cur_mut() { v.state.viewport = (rows, cols); }
    }

    /// Navigate rows: +down, -up (large values clamp to bounds)
    pub fn nav_row(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = v.rows();
            if d < 0 { v.state.up((-d) as usize); } else { v.state.down(d as usize, n); }
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

    /// Get page size for page up/down (viewport - header - footer_header - status)
    pub fn page(&self) -> isize {
        self.view().map(|v| (v.state.viewport.0 as isize).saturating_sub(3)).unwrap_or(10)
    }

    // ── Elm Architecture: run/draw/handle_events ─────────────────────────────

    /// Main event loop (Elm Architecture)
    pub fn run(&mut self, tui: &mut DefaultTerminal, on_key: impl Fn(&mut Self, KeyEvent) -> Result<bool>) -> Result<()> {
        let size = tui.size()?;
        self.viewport(size.height, size.width);

        loop {
            // Update: check background tasks
            self.update();

            // Handle redraw/center flags
            if self.needs_redraw {
                tui.clear()?;
                let size = tui.size()?;
                self.viewport(size.height, size.width);
                self.needs_redraw = false;
            }
            if self.needs_center {
                if let Some(v) = self.view_mut() { v.state.center_if_needed(); }
                self.needs_center = false;
            }

            // Draw
            self.draw(tui)?;

            // Handle events (poll with timeout for background tasks)
            if !self.handle_events(&on_key)? {
                break;
            }
        }
        Ok(())
    }

    /// Draw current state to terminal
    fn draw(&mut self, tui: &mut DefaultTerminal) -> Result<()> {
        tui.draw(|frame| Renderer::render(frame, self))?;
        Ok(())
    }

    /// Poll and handle events, return false to quit
    fn handle_events(&mut self, on_key: &impl Fn(&mut Self, KeyEvent) -> Result<bool>) -> Result<bool> {
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                return on_key(self, key);
            }
        }
        Ok(true)
    }

    /// Update: process background tasks
    fn update(&mut self) {
        self.check_bg_saver();
    }
}
