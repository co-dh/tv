//! Interactive prompts - search, filter, file dialogs

use anyhow::Result;
use ratatui::crossterm::event::{self, Event, KeyCode};
use ratatui::crossterm::{cursor, execute, style::Print, terminal};
use std::io::{self, Write};
use crate::app::AppContext;
use crate::command::executor::CommandExecutor;
use crate::command::transform::Filter;
use crate::data::dynload;
use crate::data::table::Table;
use crate::input::handler::{dispatch, run};
use crate::input::parser::parse;
use crate::util::{picker, pure, theme};

/// Search with fzf (/)
pub fn do_search(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let file = v.path.as_deref();
        Some((hints(v.data.as_ref(), &col_name, v.state.cr, file), col_name, v.name.starts_with("ls")))
    });
    if let Some((hint_list, col_name, is_folder)) = info {
        let expr_opt = picker::fzf(hint_list, "Search> ");
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            let prql_mode = theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false);
            let expr = if !prql_mode && is_plain_value(&expr) {
                format!("{} LIKE '%{}%'", col_name, expr)
            } else { expr.to_string() };
            let matches = app.view().map(|v| find(v.data.as_ref(), &expr)).unwrap_or_default();
            app.search.col_name = None;
            app.search.value = Some(expr.clone());
            let found = if let Some(view) = app.view_mut() {
                if let Some(&pos) = matches.first() {
                    view.state.cr = pos;
                    app.needs_center = true;
                    true
                } else { app.msg(format!("Not found: {}", expr)); false }
            } else { false };
            if found && is_folder { dispatch(app, "enter"); }
        }
    }
    Ok(())
}

/// Filter with fzf (\) - multi-select support
pub fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.data.col_type(v.state.cc) == crate::data::table::ColType::Str;
        let file = v.path.as_deref();
        let header = v.data.col_names().join(" | ");
        Some((hints(v.data.as_ref(), &col_name, v.state.cr, file), col_name, is_str, header))
    });
    if let Some((hint_list, col_name, is_str, header)) = info {
        let expr_opt = picker::fzf_filter(hint_list, &col_name, is_str, Some(&header));
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

/// Command picker (:)
pub fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

/// Jump to column by name (@)
pub fn do_goto_col(app: &mut AppContext) -> Result<()> {
    if let Some(view) = app.view() {
        let col_names = view.data.col_names();
        let result = picker::fzf(col_names.clone(), "Column: ");
        app.needs_redraw = true;
        if let Ok(Some(selected)) = result {
            if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                if let Some(v) = app.view_mut() {
                    v.state.cc = idx;
                    app.msg(format!("Column: {}", selected));
                }
            }
        }
    }
    Ok(())
}

/// Prompt user for input, None if cancelled
pub fn prompt(_app: &mut AppContext, prompt_str: &str) -> Result<Option<String>> {
    let (_cols, rows) = terminal::size()?;
    execute!(
        io::stdout(),
        cursor::MoveTo(0, rows - 1),
        terminal::Clear(terminal::ClearType::CurrentLine),
        Print(prompt_str),
        cursor::Show
    )?;
    io::stdout().flush()?;

    let mut input = String::new();
    loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(Some(input));
                }
                KeyCode::Esc => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Backspace => {
                    input.pop();
                    execute!(
                        io::stdout(),
                        cursor::MoveTo(0, rows - 1),
                        terminal::Clear(terminal::ClearType::CurrentLine),
                        Print(prompt_str),
                        Print(&input)
                    )?;
                    io::stdout().flush()?;
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    execute!(io::stdout(), Print(c))?;
                    io::stdout().flush()?;
                }
                _ => {}
            }
        }
    }
}

/// Generate filter hints from table data via PRQL distinct
pub fn hints(table: &dyn Table, col_name: &str, _row: usize, file: Option<&str>) -> Vec<String> {
    let mut items = Vec::new();

    // Try PRQL distinct for parquet files
    if let Some(path) = file.filter(|f| f.ends_with(".parquet")) {
        let prql = format!("from df | select {{`{}`}} | group {{`{}`}} () | take 500", col_name, col_name);
        if let Some(sql) = pure::compile_prql(&prql) {
            if let Some(plugin) = dynload::get() {
                if let Some(t) = plugin.query(&sql, path) {
                    for r in 0..t.rows() {
                        let v = t.cell(r, 0).format(10);
                        if v != "null" { items.push(unquote(&v)); }
                    }
                }
            }
        }
    } else {
        // Get distinct values from in-memory table
        let col_idx = table.col_names().iter().position(|c| c == col_name);
        if let Some(idx) = col_idx {
            let mut seen = std::collections::HashSet::new();
            for r in 0..table.rows().min(1000) {
                let v = table.cell(r, idx).format(10);
                if v != "null" && seen.insert(v.clone()) { items.push(unquote(&v)); }
            }
        }
    }

    items.sort();
    items
}

/// Check if expression is a plain value (simple identifier or literal)
pub fn is_plain_value(expr: &str) -> bool {
    let e = expr.trim();
    if e.is_empty() || e.contains(' ') { return false; }
    if (e.starts_with('\'') && e.ends_with('\'')) || (e.starts_with('"') && e.ends_with('"')) { return true; }
    e.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-')
}

/// Find rows matching expression (stub - returns empty for now)
pub fn find(_table: &dyn Table, _expr: &str) -> Vec<usize> {
    // TODO: implement via plugin for SQL filtering
    Vec::new()
}

/// Strip quotes from string values
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else { s.to_string() }
}
