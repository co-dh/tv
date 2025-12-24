//! Command handler - routes keymap commands to execution

use anyhow::Result;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crate::app::AppContext;
use crate::command::Command;
use crate::command::executor::CommandExecutor;
use crate::command::io::{From, Save};
use crate::command::transform::{Agg, DelCol, RenameCol, Select, Xkey};
use crate::command::view::Pop;
use crate::input::keyhandler;
use crate::input::parser::parse;
use crate::input::prompt::{do_filter, do_command_picker, do_goto_col, prompt};
use crate::plugin::corr::Correlation;
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
        KeyCode::Backspace => "<bs>".into(),
        _ => "?".into(),
    }
}

/// Get current keymap tab based on view name prefix
pub fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        let p = v.name.split(&[':', ' '][..]).next().unwrap_or("");
        match p { "folder" => "folder", "freq" => "freq", "meta" => "meta", "corr" => "corr", _ => "table" }
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
        "select_cols" => {
            if !app.has_view() { app.no_table(); }
            else if !app.test_input.is_empty() {
                // Test mode: parse comma-separated column names
                let cols = app.test_input.remove(0);
                run(app, Box::new(Select { col_names: cols.split(',').map(|s| s.trim().to_string()).collect() }));
            } else if let Some(v) = app.view() {
                let cols = v.data.col_names();
                let result = picker::fzf_multi(cols, "Select columns: ");
                app.needs_clear = true;
                if let Ok(selected) = result {
                    if !selected.is_empty() {
                        run(app, Box::new(Select { col_names: selected }));
                    }
                }
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
        "filter" => do_filter(app)?,
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
            // Get key cols and columns to aggregate (selected or current)
            let info = app.view().map(|v| {
                let cols: Vec<String> = if v.selected_cols.is_empty() {
                    v.col_name(v.data_col(v.state.cc)).into_iter().collect()
                } else {
                    v.selected_cols.iter().filter_map(|&i| v.col_name(v.data_col(i))).collect()
                };
                (v.key_cols.clone(), cols)
            });
            if let Some((keys, cols)) = info {
                if keys.is_empty() {
                    app.msg("Set key columns first with ! (xkey)");
                } else if cols.is_empty() {
                    app.msg("No columns to aggregate");
                } else {
                    // Show PRQL: group {keys} (aggregate {funcs cols}), hint for multi-select
                    let prompt = format!("group {{{}}} (agg {{? {}}}) [Tab=multi]: ", keys.join(","), cols.join(","));
                    // Test mode: use test_input, else fzf
                    let funcs: Vec<String> = if !app.test_input.is_empty() {
                        app.test_input.remove(0).split(',').map(|s| s.trim().to_string()).collect()
                    } else {
                        let result = picker::fzf_multi(vec!["count".into(), "sum".into(), "mean".into(), "min".into(), "max".into(), "std".into()], &prompt);
                        app.needs_clear = true;
                        result.unwrap_or_default()
                    };
                    if !funcs.is_empty() {
                        // Apply each func to each col
                        let agg_funcs: Vec<(String, String)> = funcs.iter()
                            .flat_map(|f| cols.iter().map(move |c| (f.clone(), c.clone())))
                            .collect();
                        run(app, Box::new(Agg { keys, funcs: agg_funcs }));
                        if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
                    }
                }
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
                    run(app, Box::new(DelCol { cols: col_names }));
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
                run(app, Box::new(Xkey { keys: col_names }));
                if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
            }
        }
        "command" => do_command_picker(app)?,
        "goto_col" | "goto_col_name" => do_goto_col(app)?,
        "enter" | "filter_parent" | "delete_sel" | "parent" => { dispatch(app, cmd); }
        _ => {}
    }
    Ok(true)
}
