//! Command parser - converts command strings to Command objects
//! TODO: Extract parse() from main.rs

use crate::app::AppContext;
use crate::command::Command;

/// Parse command string into Command object
pub fn parse(_line: &str, _app: &mut AppContext) -> Option<Box<dyn Command>> {
    // TODO: Move from main.rs
    None
}
