//! Command handler - routes keymap commands to execution
//! TODO: Extract handle_cmd and on_key from main.rs

use anyhow::Result;
use ratatui::crossterm::event::KeyEvent;
use crate::app::AppContext;

/// Handle key event - lookup in keymap and execute
pub fn on_key(_app: &mut AppContext, _key: KeyEvent) -> Result<bool> {
    // TODO: Move from main.rs
    Ok(true)
}

/// Handle keymap command string
pub fn handle_cmd(_app: &mut AppContext, _cmd: &str) -> Result<bool> {
    // TODO: Move from main.rs
    Ok(true)
}
