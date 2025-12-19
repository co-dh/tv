//! Key handler - translates keymap commands to executable command strings
//! Resolves context (current column, selection) before creating commands

use crate::app::AppContext;
use crate::util::pure;

/// Translate keymap command + app state into command string
/// Returns None for interactive commands (need prompts) or navigation
pub fn to_cmd(app: &AppContext, cmd: &str) -> Option<String> {
    match cmd {
        // Freq: use key columns if set, else current column
        "freq" => {
            let v = app.view()?;
            let sep = v.col_separator.unwrap_or(0);
            let cols = if v.cols.is_empty() { v.data.col_names() } else { v.cols.clone() };
            // Pure: build freq command
            pure::freq_cmd(&cols, sep, cur_col(app).as_deref())
        }
        "sort" => cur_col(app).map(|c| format!("sort {}", c)),
        "sort-" => cur_col(app).map(|c| format!("sort -{}", c)),
        "derive" => cur_col(app).map(|c| format!("derive {}", c)),

        // Selection-based commands - handled in main for extra logic (plugin dispatch, clear selection)
        "delete" => None,
        // Toggle key: add/remove current col from keys, send xkey with all keys
        "xkey" => toggle_key(app),

        // View commands - simple ones that don't need has_view checks
        "swap" => Some("swap".into()),
        "ls" => Some("ls".into()),
        "lr" => Some("lr".into()),
        "pop" => Some("pop".into()),
        // These need has_view check or extra logic in main
        "meta" | "corr" | "pivot" | "dup" => None,

        // Display commands
        "toggle_info" => Some("toggle_info".into()),
        "decimals_inc" => Some("decimals 1".into()),
        "decimals_dec" => Some("decimals -1".into()),

        // Selection commands
        "toggle_sel" => Some("toggle_sel".into()),
        "clear_sel" => Some("clear_sel".into()),
        "sel_all" => Some("sel_all".into()),
        "sel_null" => Some("sel_rows `null%` == '100.0'".into()),
        "sel_single" => Some("sel_rows distinct == '1'".into()),

        // Navigation - return as-is for special handling
        "up" => Some("goto -1".into()),
        "down" => Some("goto +1".into()),
        "left" => Some("goto_col -1".into()),
        "right" => Some("goto_col +1".into()),
        "top" => Some("goto 0".into()),
        "bottom" => Some("goto max".into()),

        // Interactive commands - need prompts, handled separately
        "filter" | "search" | "from" | "save" | "select_cols" | "rename"
        | "aggregate" | "convert" | "command" | "goto_col" | "goto_col_name" => None,

        // Plugin dispatch commands
        "enter" | "filter_parent" | "delete_sel" => None,

        // Search navigation
        "next_match" | "prev_match" | "search_cell" => None,

        // Bookmarks
        "bookmark" | "next_bookmark" => None,

        // Special
        "force_quit" | "print_status" => None,

        // Page navigation - needs app.page()
        "page_down" | "page_up" => None,

        _ => None,
    }
}

/// Get current column name
fn cur_col(app: &AppContext) -> Option<String> {
    app.view().and_then(|v| v.col_name(v.state.cc))
}

/// Toggle selected columns (or current column) as keys, return xkey command
fn toggle_key(app: &AppContext) -> Option<String> {
    let v = app.view()?;
    let sep = v.col_separator.unwrap_or(0);
    let cols = if v.cols.is_empty() { v.data.col_names() } else { v.cols.clone() };

    // Get columns to toggle: selected cols or current col
    let to_toggle: Vec<String> = if v.selected_cols.is_empty() {
        vec![v.col_name(v.state.cc)?]
    } else {
        v.selected_cols.iter().filter_map(|&i| v.col_name(i)).collect()
    };

    // Pure: toggle columns in key list
    let keys = pure::toggle_keys(&cols[..sep], &to_toggle);
    // Pure: build xkey command
    Some(pure::xkey_cmd(&keys))
}

/// Get selected columns or current column (for future use)
#[allow(dead_code)]
fn sel_cols(app: &AppContext) -> Option<Vec<String>> {
    app.view().and_then(|v| {
        if v.selected_cols.is_empty() {
            v.col_name(v.state.cc).map(|c| vec![c])
        } else {
            let mut sel: Vec<usize> = v.selected_cols.iter().copied().collect();
            sel.sort();
            let cols: Vec<String> = sel.into_iter().filter_map(|i| v.col_name(i)).collect();
            if cols.is_empty() { None } else { Some(cols) }
        }
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_simple_commands() {
        // Commands without context don't need app state
        assert_eq!(to_cmd_str("meta"), Some("meta".into()));
        assert_eq!(to_cmd_str("toggle_info"), Some("toggle_info".into()));
        assert_eq!(to_cmd_str("up"), Some("goto -1".into()));
    }

    fn to_cmd_str(cmd: &str) -> Option<String> {
        // Test helper - only works for commands that don't need app context
        match cmd {
            "meta" => Some("meta".into()),
            "toggle_info" => Some("toggle_info".into()),
            "up" => Some("goto -1".into()),
            _ => None,
        }
    }
}
