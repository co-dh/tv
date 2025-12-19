//! Input handling: keymap, command parsing, prompts

pub mod keymap;
pub mod keyhandler;
pub mod handler;
pub mod parser;
pub mod prompt;

pub use handler::{on_key, handle_cmd, cur_tab, run, fetch_lazy, find_match};
pub use parser::parse;
pub use prompt::is_plain_value;
