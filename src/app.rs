//! Application context - global state, view stack, background tasks

use crate::error::TvError;
use crate::input::keymap::KeyMap;
use crate::plugin::Registry;
use crate::state::{StateStack, ViewState};
use crate::util::theme::Theme;
use crate::render::Renderer;
use anyhow::Result;
use ratatui::backend::Backend;
use ratatui::crossterm::event::{self, Event, KeyEvent};
use ratatui::Terminal;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;

/// App context
pub struct AppContext {
    pub stack: StateStack,         // view stack
    pub history_file: PathBuf,     // cmd history file
    pub message: String,           // status bar msg
    next_id: usize,                // view id counter
    pub info_mode: u8,             // info box: 0=off, 1=help, 2=help+prql, 3=commands, 4=debug
    pub float_decimals: usize,     // decimal places for floats
    pub keymap: KeyMap,            // key bindings
    pub theme: Theme,              // color theme
    pub plugins: Registry,         // plugin registry
    pub bg_saver: Option<Receiver<String>>,      // background save status
    pub raw_save: bool,            // --raw: skip type detection on save
    pub backend: Option<String>,   // --backend: sqlite uses sqlite plugin, else ADBC
    pub needs_redraw: bool,  // redraw on next frame
    pub needs_clear: bool,   // force full clear (after fzf/bat)
    pub test_input: Vec<String>,   // pre-extracted input for prompts (test mode)
}

impl Default for AppContext {
    /// Create app context with default settings, keymap, theme, plugins
    fn default() -> Self {
        let history_file = std::env::var("HOME").ok()
            .map(|h| PathBuf::from(h).join(".tv").join("history"))
            .unwrap_or_else(|| PathBuf::from("history"));
        if let Some(dir) = history_file.parent() { let _ = std::fs::create_dir_all(dir); }
        Self {
            stack: StateStack::default(),
            history_file,
            message: String::new(),
            next_id: 0,
            info_mode: 1,  // start with help visible
            float_decimals: 3,
            keymap: KeyMap::default(),
            theme: Theme::load_active(),
            plugins: Registry::new(std::path::Path::new("cfg/plugins.csv")),
            bg_saver: None,
            raw_save: false,
            backend: None,
            needs_redraw: false,
            needs_clear: false,
            test_input: Vec::new(),
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
        self.stack.set_viewport(rows, cols);  // cache for new views
        if let Some(v) = self.stack.cur_mut() { v.state.viewport = (rows, cols); }
    }

    /// Navigate rows: +down, -up (large values clamp to bounds)
    pub fn nav_row(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = Self::total_rows(v);
            if d < 0 { v.state.up((-d) as usize); } else { v.state.down(d as usize, n); }
        }
    }

    /// Get total rows via PRQL count query (plugin caches it)
    fn total_rows(v: &ViewState) -> usize {
        use crate::data::dynload;
        use crate::data::table::{Table, Cell};
        v.path.as_ref()
            .and_then(|p| dynload::get_for(p).map(|plugin| (p, plugin)))
            .and_then(|(p, plugin)| {
                let prql = format!("{} | cnt", v.prql);
                plugin.query(&prql, p)
            })
            .and_then(|t| match t.cell(0, 0) { Cell::Int(n) => Some(n as usize), _ => None })
            .unwrap_or_else(|| v.rows())
    }

    /// Navigate cols: +right, -left
    pub fn nav_col(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = v.cols();
            if d < 0 { v.state.left((-d) as usize); }
            else { v.state.right(d as usize, n); }
        }
    }

    /// Get page size for page up/down (viewport - reserved rows)
    pub fn page(&self) -> isize {
        use crate::state::RESERVED_ROWS;
        self.view().map(|v| (v.state.viewport.0 as isize).saturating_sub(RESERVED_ROWS as isize)).unwrap_or(10)
    }

    // ── Elm Architecture: run/draw/handle_events ─────────────────────────────

    /// Main event loop - only redraws when state changes
    pub fn run<B: Backend>(&mut self, tui: &mut Terminal<B>, on_key: impl Fn(&mut Self, KeyEvent) -> Result<bool>) -> Result<()>
    where B::Error: Send + Sync + 'static {  // bounds required for anyhow::Error conversion
        let size = tui.size()?;
        self.viewport(size.height, size.width);
        self.needs_redraw = true;  // initial draw

        loop {
            self.tick(tui)?;
            // Poll for events
            if event::poll(std::time::Duration::from_millis(100))? {
                match event::read()? {
                    Event::Key(key) => {
                        if !on_key(self, key)? { break; }
                        self.needs_redraw = true;
                    }
                    Event::Resize(w, h) => {
                        self.viewport(h, w);
                        self.needs_redraw = true;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Run with list of key events (for testing)
    pub fn run_keys<B: Backend>(&mut self, tui: &mut Terminal<B>, keys: &[KeyEvent], on_key: impl Fn(&mut Self, KeyEvent) -> Result<bool>) -> Result<()>
    where B::Error: Send + Sync + 'static {  // bounds required for anyhow::Error conversion
        let size = tui.size()?;
        self.viewport(size.height, size.width);
        self.needs_redraw = true;
        for key in keys {
            self.tick(tui)?;
            if let Err(e) = on_key(self, *key) { self.message = e.to_string(); }
            self.needs_redraw = true;
        }
        self.tick(tui)?;  // final render
        Ok(())
    }

    /// One tick: update + draw if needed
    fn tick<B: Backend>(&mut self, tui: &mut Terminal<B>) -> Result<()>
    where B::Error: Send + Sync + 'static {
        self.update();
        if self.needs_redraw || self.needs_clear {
            if self.needs_clear {
                tui.clear()?;
                self.needs_clear = false;
            }
            tui.draw(|frame| Renderer::render(frame, self))?;
            self.needs_redraw = false;
        }
        Ok(())
    }

    /// Update: process background tasks
    fn update(&mut self) {
        let had_msg = !self.message.is_empty();
        self.check_bg_saver();
        if !self.message.is_empty() && self.message != "No table loaded" && !had_msg {
            self.needs_redraw = true;  // new message from background task
        }
    }
}
