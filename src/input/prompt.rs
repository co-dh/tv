//! Interactive prompts - search, filter, file dialogs
//! TODO: Extract do_search, do_filter, prompt from main.rs

use anyhow::Result;
use crate::app::AppContext;

/// Search with fzf picker
pub fn do_search(_app: &mut AppContext) -> Result<()> {
    // TODO: Move from main.rs
    Ok(())
}

/// Filter with fzf picker
pub fn do_filter(_app: &mut AppContext) -> Result<()> {
    // TODO: Move from main.rs
    Ok(())
}

/// Generic prompt using fzf
pub fn prompt(_app: &mut AppContext, _prompt: &str) -> Result<Option<String>> {
    // TODO: Move from main.rs
    Ok(None)
}
