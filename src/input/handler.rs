//! Command handler - routes keymap commands to execution

use anyhow::Result;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crate::app::AppContext;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::io::{From, Save};
use crate::command::transform::{Agg, DelCol, RenameCol, Select, Xkey};
use crate::command::view::Pop;
use crate::data::dynload;
use crate::input::keyhandler;
use crate::input::parser::parse;
use crate::input::prompt::{do_search, do_filter, do_command_picker, do_goto_col, prompt};
use crate::plugin::corr::Correlation;
use crate::state::ViewKind;
use crate::util::picker;

/// Convert KeyEvent to Kakoune-style key name for keymap lookup
pub fn key_str(key: &KeyEvent) -> String {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) if ctrl => format!("<c-{}>", c.to_ascii_lowercase()),
        KeyCode::Char('\\') => "<backslash>".into(),
        KeyCode::Char(' ') => "<space>".into(),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::Enter => "<ret>".into(),
        KeyCode::Esc => "<esc>".into(),
        KeyCode::Up => "<up>".into(),
        KeyCode::Down => "<down>".into(),
        KeyCode::Left => "<left>".into(),
        KeyCode::Right => "<right>".into(),
        KeyCode::Home => "<home>".into(),
        KeyCode::End => "<end>".into(),
        KeyCode::PageUp => "<pageup>".into(),
        KeyCode::PageDown => "<pagedown>".into(),
        KeyCode::Tab => "<tab>".into(),
        KeyCode::BackTab => "<s-tab>".into(),
        KeyCode::Delete => "<del>".into(),
        KeyCode::Backspace => "<backspace>".into(),
        _ => "?".into(),
    }
}

/// Get current keymap tab based on view type
pub fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| match v.kind {
        ViewKind::Folder => "folder",
        ViewKind::Freq => "freq",
        ViewKind::Meta => "meta",
        ViewKind::Corr => "corr",
        ViewKind::Pivot => "pivot",
        ViewKind::Table => "table",
    }).unwrap_or("table")
}

/// Execute command through executor with error handling
pub fn run(app: &mut AppContext, cmd: Box<dyn Command>) {
    if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
}

/// Dispatch action to view-specific plugin handler
pub fn dispatch(app: &mut AppContext, action: &str) -> bool {
    let name = match app.view() { Some(v) => v.name.clone(), None => return false };
    // mem::take to avoid borrow conflict: plugins.handle needs &mut app
    let plugins = std::mem::take(&mut app.plugins);
    let cmd = plugins.handle(&name, action, app);
    app.plugins = plugins;
    if let Some(cmd) = cmd { run(app, cmd); true } else { false }
}

/// Fetch data for lazy views (parquet files or source: paths)
pub fn fetch_lazy(view: &mut crate::state::ViewState) {
    let path = match &view.path {
        Some(p) if p.ends_with(".parquet") || p.ends_with(".pq") || p.starts_with("source:") => p.clone(),
        _ => return,
    };
    let offset = view.state.r0;
    if let Some(plugin) = dynload::get_for(&path) {
        // Use PRQL chain with take range (1-based)
        let (s, e) = (offset + 1, offset + 51);
        let prql = format!("{} | take {}..{}", view.prql, s, e);
        if let Some(sql) = crate::util::pure::compile_prql(&prql) {
            if let Some(t) = plugin.query(&sql, &path) {
                view.data = dynload::to_box_table(&t);
            }
        }
    }
}

/// Navigate to next/prev search match
pub fn find_match(app: &mut AppContext, forward: bool) {
    if let Some(expr) = app.search.value.clone() {
        if let Some(view) = app.view_mut() {
            let m = crate::input::prompt::find(view.data.as_ref(), &expr);
            let cur = view.state.cr;
            let pos = if forward { m.iter().find(|&&i| i > cur) } else { m.iter().rev().find(|&&i| i < cur) };
            if let Some(&p) = pos { view.state.cr = p; view.state.visible(); }
            else { app.msg("No more matches"); }
        }
    } else { app.msg("No search active"); }
}

/// Process key event, return false to quit
pub fn on_key(app: &mut AppContext, key: KeyEvent) -> Result<bool> {
    let ks = key_str(&key);
    let tab = cur_tab(app);
    if let Some(cmd) = app.keymap.get_command(tab, &ks).map(|s| s.to_string()) {
        return handle_cmd(app, &cmd);
    }
    Ok(true)
}

/// Handle keymap command, return false to quit
pub fn handle_cmd(app: &mut AppContext, cmd: &str) -> Result<bool> {
    // Try keyhandler for commands it can resolve (with context)
    if let Some(cmd_str) = keyhandler::to_cmd(app, cmd) {
        if let Some(c) = parse(&cmd_str, app) {
            run(app, c);
            // Clear selection after xkey
            if cmd == "xkey" { if let Some(v) = app.view_mut() { v.selected_cols.clear(); } }
            return Ok(true);
        }
    }
    // Handle commands that need special logic or prompts
    match cmd {
        "quit" => {
            if !app.has_view() { return Ok(false); }
            run(app, Box::new(Pop));
        }
        "force_quit" => return Ok(false),
        "print_status" => { print_status(app); return Ok(false); }
        "select_cols" => {
            if !app.has_view() { app.no_table(); }
            else if let Some(cols) = prompt(app, "Select columns: ")? {
                run(app, Box::new(Select { col_names: cols.split(',').map(|s| s.trim().to_string()).collect() }));
            }
        }
        "from" => {
            if let Some(file_path) = prompt(app, "Load file: ")? { run(app, Box::new(From { file_path })); }
        }
        "save" => {
            if !app.has_view() { app.no_table(); }
            else if let Some(file_path) = prompt(app, "Save to: ")? {
                let path = std::path::Path::new(&file_path);
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        if let Some(ans) = prompt(app, &format!("Create dir '{}'? (y/n): ", parent.display()))? {
                            if ans.to_lowercase() == "y" {
                                if let Err(e) = std::fs::create_dir_all(parent) {
                                    app.err(anyhow::anyhow!("Failed to create dir: {}", e));
                                    return Ok(true);
                                }
                            } else { return Ok(true); }
                        }
                    }
                }
                run(app, Box::new(Save { file_path }));
            }
        }
        "search" => do_search(app)?,
        "filter" => do_filter(app)?,
        "next_match" => find_match(app, true),
        "prev_match" => find_match(app, false),
        "search_cell" => {
            if let Some(expr) = app.view().and_then(|v| {
                let col_name = v.col_name(v.state.cc)?;
                let cell = v.data.cell(v.state.cr, v.state.cc);
                let is_str = v.data.col_type(v.state.cc) == crate::data::table::ColType::Str;
                let val = unquote(&cell.format(10));
                Some(if is_str { format!("{} = '{}'", col_name, val) } else { format!("{} = {}", col_name, val) })
            }) {
                app.search.col_name = None;
                app.search.value = Some(expr.clone());
                app.msg(format!("Search: {}", expr));
            }
        }
        "meta" => {
            if app.has_view() {
                if let Some(c) = parse("meta", app) { run(app, c); }
            }
        }
        "corr" => {
            if app.has_view() {
                run(app, Box::new(Correlation));
                if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
            }
        }
        "rename" => {
            if let Some(old_name) = app.view().and_then(|v| v.col_name(v.state.cc)) {
                if let Some(new_name) = prompt(app, &format!("Rename '{}' to: ", old_name))? {
                    run(app, Box::new(RenameCol { old_name, new_name }));
                }
            }
        }
        "convert" => { app.msg("Use derive for type conversion (PRQL syntax)"); }
        "aggregate" => {
            if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
                let result = picker::fzf(vec!["count".into(), "sum".into(), "mean".into(), "min".into(), "max".into(), "std".into()], "Aggregate: ");
                app.needs_clear = true;
                if let Ok(Some(func)) = result { run(app, Box::new(Agg { col, func })); }
            }
        }
        "delete" => {
            if !dispatch(app, "delete") {
                let col_names: Vec<String> = app.view().map(|v| {
                    if v.selected_cols.is_empty() {
                        v.col_name(v.state.cc).into_iter().collect()
                    } else {
                        let mut sel: Vec<usize> = v.selected_cols.iter().copied().collect();
                        sel.sort_by(|a, b| b.cmp(a));
                        sel.iter().filter_map(|&i| v.col_name(i)).collect()
                    }
                }).unwrap_or_default();
                if !col_names.is_empty() {
                    run(app, Box::new(DelCol { col_names }));
                    if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
                }
            }
        }
        "xkey" => {
            let col_names: Vec<String> = app.view().map(|v| {
                if v.selected_cols.is_empty() {
                    v.col_name(v.state.cc).into_iter().collect()
                } else {
                    let mut sel: Vec<usize> = v.selected_cols.iter().copied().collect();
                    sel.sort();
                    sel.into_iter().filter_map(|i| v.col_name(i)).collect()
                }
            }).unwrap_or_default();
            if !col_names.is_empty() {
                run(app, Box::new(Xkey { col_names }));
                if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
            }
        }
        "command" => do_command_picker(app)?,
        "goto_col" | "goto_col_name" => do_goto_col(app)?,
        "bookmark" => {
            if let Some(view) = app.view() {
                let cr = view.state.cr;
                if let Some(pos) = app.bookmarks.iter().position(|&r| r == cr) {
                    app.bookmarks.remove(pos);
                    app.msg(format!("Removed bookmark at row {}", cr));
                } else {
                    app.bookmarks.push(cr);
                    app.bookmarks.sort();
                    app.msg(format!("Bookmarked row {} ({} total)", cr, app.bookmarks.len()));
                }
            }
        }
        "next_bookmark" => {
            if app.bookmarks.is_empty() { app.msg("No bookmarks"); }
            else if let Some(row) = app.bookmarks.iter()
                .find(|&&r| r > app.view().map(|v| v.state.cr).unwrap_or(0)).copied()
                .or_else(|| app.bookmarks.first().copied())
            {
                if let Some(v) = app.view_mut() { v.state.cr = row; v.state.visible(); }
                app.msg(format!("Bookmark: row {}", row));
            }
        }
        "enter" | "filter_parent" | "delete_sel" => { dispatch(app, cmd); }
        _ => {}
    }
    Ok(true)
}

/// Strip quotes from string values
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else { s.to_string() }
}

/// Print status line info (for key testing)
fn print_status(app: &mut AppContext) {
    use std::fs;
    fn mem_mb() -> usize {
        fs::read_to_string("/proc/self/status").ok()
            .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
            .map(|kb| kb / 1024).unwrap_or(0)
    }
    if let Some(view) = app.view_mut() {
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let df_rows = view.data.rows();
        let keys = view.col_separator.unwrap_or(0);
        let sel = view.selected_cols.len();
        let sel_cols: Vec<usize> = view.selected_cols.iter().copied().collect();
        println!("STATUS: view={} rows={} df={} col={} col_name={} keys={} sel={} sel_cols={:?} mem={}MB",
            view.name, view.rows(), df_rows, view.state.cc, col_name, keys, sel, sel_cols, mem_mb());
    }
}
