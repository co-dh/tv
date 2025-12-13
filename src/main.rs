mod app;
mod command;
mod keymap;
mod os;
mod picker;
mod render;
mod state;

use anyhow::Result;
use app::AppContext;
use command::executor::CommandExecutor;
use command::io::{Load, Save};
use command::transform::{Agg, DelCol, Filter, FilterIn, RenameCol, Select, Sort};
use command::view::{Correlation, Df, Dup, Env, Frequency, Lr, Ls, Lsblk, Lsof, Metadata, Mounts, Pop, Ps, Swap, Tcp, Udp, Who};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::{cursor, execute, style::Print, terminal};
use render::{Renderer, Terminal};
use std::fs;
use std::io::{self, Write};

fn main() -> Result<()> {
    // Get command line args
    let args: Vec<String> = std::env::args().collect();

    // Check for -c argument (inline script)
    if let Some(idx) = args.iter().position(|a| a == "-c") {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv -c '<commands>'");
            std::process::exit(1);
        }
        return run_commands(&args[idx + 1]);
    }

    // Check for --script argument
    if let Some(idx) = args.iter().position(|a| a == "--script") {
        // Script mode: run commands from file and print result
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --script <script_file>");
            std::process::exit(1);
        }
        return run_script(&args[idx + 1]);
    }

    // Initialize terminal
    let _terminal = Terminal::init()?;

    // Create app context
    let mut app = if args.len() > 1 {
        // Load file from CLI argument
        let mut temp_app = AppContext::new();
        match CommandExecutor::exec(&mut temp_app, Box::new(Load { file_path: args[1].clone() })) {
            Ok(_) => temp_app,
            Err(e) => {
                Terminal::restore()?;
                eprintln!("Error loading file: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        AppContext::new()
    };

    // Update viewport - Terminal::size() returns (cols, rows)
    app.viewport(Terminal::size()?.1, Terminal::size()?.0);

    // Main event loop
    loop {
        // Render
        Renderer::render(&mut app)?;

        // Handle events
        if let Event::Key(key) = event::read()? {
            if !on_key(&mut app, key)? {
                break;
            }
        }
    }

    Terminal::restore()?;
    Ok(())
}

/// Run commands from inline string (-c option)
fn run_commands(commands: &str) -> Result<()> {
    let mut app = AppContext::new();
    app.viewport(50, 120);

    for cmd_str in commands.split('|') {
        let cmd_str = cmd_str.trim();
        if cmd_str.is_empty() || cmd_str == "quit" {
            continue;
        }

        if let Some(cmd) = parse(cmd_str, &app) {
            if let Err(e) = CommandExecutor::exec(&mut app, cmd) {
                eprintln!("Error executing '{}': {}", cmd_str, e);
            }
        } else {
            eprintln!("Unknown command: {}", cmd_str);
        }
    }

    print(&app);
    Ok(())
}

/// Run commands from a script file and print result
fn run_script(script_path: &str) -> Result<()> {
    let mut app = AppContext::new();

    // Set a reasonable viewport for printing
    app.viewport(50, 120);

    'outer: for line in fs::read_to_string(script_path)?.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Support pipe-separated commands on a single line
        for cmd_str in line.split('|') {
            let cmd_str = cmd_str.trim();
            if cmd_str.is_empty() {
                continue;
            }

            if cmd_str == "quit" {
                break 'outer;
            }

            // Parse and execute command
            if let Some(cmd) = parse(cmd_str, &app) {
                if let Err(e) = CommandExecutor::exec(&mut app, cmd) {
                    eprintln!("Error executing '{}': {}", cmd_str, e);
                }
            } else {
                eprintln!("Unknown command: {}", cmd_str);
            }
        }
    }

    // Print the final state
    print(&app);

    Ok(())
}

/// Print table to stdout (for script mode)
fn print(app: &AppContext) {
    if let Some(view) = app.view() {
        println!("=== {} ({} rows) ===", view.name, view.dataframe.height());
        println!("{}", view.dataframe);
    } else {
        println!("No table loaded");
    }
}

/// Parse a text command into a Command object
fn parse(line: &str, _app: &AppContext) -> Option<Box<dyn command::Command>> {
    let parts: Vec<&str> = line.splitn(2, ' ').collect();
    let arg = parts.get(1).map(|s| s.trim()).unwrap_or("");

    match parts[0].to_lowercase().as_str() {
        "load" => Some(Box::new(Load { file_path: arg.to_string() })),
        "save" => Some(Box::new(Save { file_path: arg.to_string() })),
        "ls" => Some(Box::new(Ls { dir: std::path::PathBuf::from(if arg.is_empty() { "." } else { arg }) })),
        "lr" => Some(Box::new(Lr { dir: std::path::PathBuf::from(if arg.is_empty() { "." } else { arg }) })),
        "ps" => Some(Box::new(Ps)),
        "df" => Some(Box::new(Df)),
        "mounts" => Some(Box::new(Mounts)),
        "tcp" => Some(Box::new(Tcp)),
        "udp" => Some(Box::new(Udp)),
        "lsblk" => Some(Box::new(Lsblk)),
        "who" => Some(Box::new(Who)),
        "lsof" => Some(Box::new(Lsof { pid: if arg.is_empty() { None } else { arg.parse().ok() } })),
        "env" => Some(Box::new(Env)),
        "freq" | "frequency" => Some(Box::new(Frequency { col_name: arg.to_string() })),
        "meta" | "metadata" => Some(Box::new(Metadata)),
        "corr" | "correlation" => Some(Box::new(Correlation { selected_cols: vec![] })),
        "delcol" => Some(Box::new(DelCol { col_names: arg.split(',').map(|s| s.trim().to_string()).collect() })),
        "filter" => Some(Box::new(Filter { expr: arg.to_string() })),
        "select" | "sel" => Some(Box::new(Select {
            col_names: arg.split(',').map(|s| s.trim().to_string()).collect()
        })),
        "sort" => Some(Box::new(Sort { col_name: arg.to_string(), descending: false })),
        "sortdesc" => Some(Box::new(Sort { col_name: arg.to_string(), descending: true })),
        "rename" => {
            let rename_parts: Vec<&str> = arg.splitn(2, ' ').collect();
            if rename_parts.len() == 2 {
                Some(Box::new(RenameCol {
                    old_name: rename_parts[0].to_string(),
                    new_name: rename_parts[1].to_string(),
                }))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Handle keyboard input
/// Returns false to exit the application
fn on_key(app: &mut AppContext, key: KeyEvent) -> Result<bool> {
    match key.code {
        KeyCode::Char('q') => {
            // Quit if no view or only one view
            if !app.has_view() || app.stack.len() == 1 {
                return Ok(false);
            }
            let _ = CommandExecutor::exec(app, Box::new(Pop));
        }
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            // Ctrl+C to force quit
            return Ok(false);
        }
        KeyCode::Up => app.nav_row(-1),
        KeyCode::Down => app.nav_row(1),
        KeyCode::Left => app.nav_col(-1),
        KeyCode::Right => app.nav_col(1),
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => app.nav_row(app.page()),
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => app.nav_row(-app.page()),
        KeyCode::Char('g') => app.nav_row(isize::MIN),
        KeyCode::Char('G') => app.nav_row(isize::MAX),
        KeyCode::PageUp => app.nav_row(-app.page()),
        KeyCode::PageDown => app.nav_row(app.page()),
        KeyCode::Home => app.nav_row(isize::MIN),
        KeyCode::End => app.nav_row(isize::MAX),
        KeyCode::Char('I') => {
            // I: Toggle info box
            app.show_info = !app.show_info;
        }
        KeyCode::Char('.') => {
            app.float_decimals = (app.float_decimals + 1).min(17);
            if let Some(v) = app.view_mut() { v.state.col_widths.clear(); }
            app.msg(format!("Float decimals: {}", app.float_decimals));
        }
        KeyCode::Char(',') => {
            app.float_decimals = app.float_decimals.saturating_sub(1);
            if let Some(v) = app.view_mut() { v.state.col_widths.clear(); }
            app.msg(format!("Float decimals: {}", app.float_decimals));
        }
        KeyCode::Char('L') => {
            // L: Load file
            if let Some(file_path) = prompt(app, "Load file: ")? {
                if let Err(e) = CommandExecutor::exec(app, Box::new(Load { file_path })) {
                    app.err(e);
                }
            }
        }
        KeyCode::Char('S') => {
            // S: Save file
            if !app.has_view() {
                app.no_table();
            } else if let Some(file_path) = prompt(app, "Save to: ")? {
                if let Err(e) = CommandExecutor::exec(app, Box::new(Save { file_path })) {
                    app.err(e);
                }
            }
        }
        KeyCode::Char('D') => {
            // D: Delete column(s) - in metadata view, delete from parent; otherwise delete current
            // Extract needed data first to avoid borrow issues
            let delete_info: Option<(bool, Option<usize>, Vec<String>)> = app.view().map(|view| {
                let is_meta = view.name == "metadata";
                let parent_id = view.parent_id;

                if is_meta {
                    let rows_to_delete: Vec<usize> = if view.selected_rows.is_empty() {
                        vec![view.state.cr]
                    } else {
                        let mut rows: Vec<usize> = view.selected_rows.iter().copied().collect();
                        rows.sort_by(|a, b| b.cmp(a));
                        rows
                    };

                    let col_names: Vec<String> = rows_to_delete.iter()
                        .filter_map(|&row| {
                            view.dataframe.get_columns()[0]
                                .get(row)
                                .ok()
                                .map(|v| unquote(&v.to_string()))
                        })
                        .collect();
                    (true, parent_id, col_names)
                } else {
                    let cols_to_delete: Vec<String> = if view.selected_cols.is_empty() {
                        view.state.cur_col(&view.dataframe)
                            .map(|c| vec![c.to_string()])
                            .unwrap_or_default()
                    } else {
                        let col_names: Vec<String> = view.dataframe.get_column_names()
                            .iter()
                            .map(|s| s.to_string())
                            .collect();
                        let mut selected: Vec<usize> = view.selected_cols.iter().copied().collect();
                        selected.sort_by(|a, b| b.cmp(a));
                        selected.iter()
                            .filter_map(|&idx| col_names.get(idx).cloned())
                            .collect()
                    };
                    (false, None, cols_to_delete)
                }
            });

            if let Some((is_meta, parent_id, col_names)) = delete_info {
                if is_meta {
                    if let Some(pid) = parent_id {
                        if let Some(parent) = app.stack.find_mut(pid) {
                            for c in &col_names { let _ = parent.dataframe.drop_in_place(c); }
                        }
                    }
                    let _ = CommandExecutor::exec(app, Box::new(Pop));
                } else if !col_names.is_empty() {
                    let _ = CommandExecutor::exec(app, Box::new(DelCol { col_names: col_names }));
                    if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
                }
            }
        }
        KeyCode::Char('/') => {
            // /: Search with SQL WHERE expression
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    let items = hints(&view.dataframe, &col_name, view.state.cr);

                    if let Ok(Some(expr)) = picker::input(items, "Search> ") {
                        let matches = find(&view.dataframe, &expr);
                        app.search.col_name = None;
                        app.search.value = Some(expr.clone());

                        if let Some(view) = app.view_mut() {
                            if let Some(&pos) = matches.first() {
                                view.state.cr = pos;
                                view.state.visible();
                                app.msg(format!("Found {} match(es)", matches.len()));
                            } else {
                                app.msg(format!("Not found: {}", expr));
                            }
                        }
                    }
                }
            }
        }
        KeyCode::Char('\\') => {
            // \: Filter rows with SQL WHERE expression
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    let items = hints(&view.dataframe, &col_name, view.state.cr);

                    if let Ok(Some(expr)) = picker::input(items, "WHERE> ") {
                        if let Err(e) = CommandExecutor::exec(app, Box::new(Filter { expr })) {
                            app.err(e);
                        }
                    }
                }
            } else {
                app.no_table();
            }
        }
        KeyCode::Char('n') => {
            // n: Find next match
            if let Some(expr) = app.search.value.clone() {
                if let Some(view) = app.view_mut() {
                    let matches = find(&view.dataframe, &expr);
                    let cur = view.state.cr;
                    if let Some(&pos) = matches.iter().find(|&&i| i > cur) {
                        view.state.cr = pos;
                        view.state.visible();
                    } else {
                        app.msg("No more matches".to_string());
                    }
                }
            } else {
                app.msg("No search active".to_string());
            }
        }
        KeyCode::Char('N') => {
            // N: Find previous match
            if let Some(expr) = app.search.value.clone() {
                if let Some(view) = app.view_mut() {
                    let matches = find(&view.dataframe, &expr);
                    let cur = view.state.cr;
                    if let Some(&pos) = matches.iter().rev().find(|&&i| i < cur) {
                        view.state.cr = pos;
                        view.state.visible();
                    } else {
                        app.msg("No more matches".to_string());
                    }
                }
            } else {
                app.msg("No search active".to_string());
            }
        }
        KeyCode::Char('*') => {
            // *: Search for current cell value (creates SQL expression)
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    let col = &view.dataframe.get_columns()[view.state.cc];
                    if let Ok(value) = col.get(view.state.cr) {
                        let is_str = matches!(col.dtype(), polars::prelude::DataType::String);
                        let val = unquote(&value.to_string());
                        let expr = if is_str { format!("{} = '{}'", col_name, val) } else { format!("{} = {}", col_name, val) };
                        app.search.col_name = None;
                        app.search.value = Some(expr.clone());
                        app.msg(format!("Search: {}", expr));
                    }
                }
            }
        }
        KeyCode::Char('s') => {
            // s: Select columns
            if !app.has_view() {
                app.no_table();
            } else if let Some(cols_str) = prompt(app, "Select columns (comma-separated): ")? {
                if let Err(e) = CommandExecutor::exec(app, Box::new(Select {
                    col_names: cols_str.split(',').map(|s| s.trim().to_string()).collect()
                })) {
                    app.err(e);
                }
            }
        }
        KeyCode::Char('F') => {
            // F: Frequency table for current column
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    if let Err(e) = CommandExecutor::exec(app, Box::new(Frequency { col_name })) {
                        app.err(e);
                    }
                }
            }
        }
        KeyCode::Char('M') => {
            // M: Metadata view
            if app.has_view() {
                if let Err(e) = CommandExecutor::exec(app, Box::new(Metadata)) {
                    app.err(e);
                }
            }
        }
        KeyCode::Char('[') => {
            // [: Sort ascending by current column
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    if let Err(e) = CommandExecutor::exec(app, Box::new(Sort { col_name, descending: false })) {
                        app.err(e);
                    }
                }
            }
        }
        KeyCode::Char(']') => {
            // ]: Sort descending by current column
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    if let Err(e) = CommandExecutor::exec(app, Box::new(Sort { col_name, descending: true })) {
                        app.err(e);
                    }
                }
            }
        }
        KeyCode::Char('^') => {
            // ^: Rename current column
            if let Some(view) = app.view() {
                if let Some(old_name) = view.state.cur_col(&view.dataframe) {
                    if let Some(new_name) = prompt(app, &format!("Rename '{}' to: ", old_name))? {
                        if let Err(e) = CommandExecutor::exec(app, Box::new(RenameCol { old_name, new_name })) {
                            app.err(e);
                        }
                    }
                }
            }
        }
        KeyCode::Enter => {
            // Enter: Pop freq view, filter parent with selected value(s)
            let filter_info: Option<(String, Vec<String>, Option<String>)> = app.view().and_then(|view| {
                if let (Some(_), Some(freq_col)) = (view.parent_id, view.freq_col.clone()) {
                    let rows: Vec<usize> = if view.selected_rows.is_empty() { vec![view.state.cr] }
                                           else { view.selected_rows.iter().copied().collect() };
                    let values: Vec<String> = rows.iter()
                        .filter_map(|&r| view.dataframe.get_columns()[0].get(r).ok().map(|v| unquote(&v.to_string())))
                        .collect();
                    Some((freq_col, values, view.filename.clone()))
                } else { None }
            });
            if let Some((freq_col, values, filename)) = filter_info {
                let _ = CommandExecutor::exec(app, Box::new(Pop));  // Pop freq view first
                if !values.is_empty() {
                    let _ = CommandExecutor::exec(app, Box::new(FilterIn { col: freq_col.clone(), values, filename }));
                    // Focus on the freq column in filtered view
                    if let Some(v) = app.view_mut() {
                        if let Some(idx) = v.dataframe.get_column_names().iter().position(|c| c.as_str() == freq_col) {
                            v.state.cc = idx;
                        }
                    }
                }
            }
        }
        KeyCode::Char('c') => {
            // c: Copy current column
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    let new_name = format!("{}_copy", col_name);
                    let col = view.dataframe.column(&col_name).ok().cloned();
                    if let Some(c) = col {
                        if let Some(view) = app.view_mut() {
                            let new_col = c.as_materialized_series().clone().with_name(new_name.clone().into());
                            if let Err(e) = view.dataframe.with_column(new_col) {
                                app.err(e);
                            } else {
                                
                            }
                        }
                    }
                }
            }
        }
        KeyCode::Char('$') => {
            // $: Type convert column
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    let types = vec![
                        "String".to_string(),
                        "Int64".to_string(),
                        "Float64".to_string(),
                        "Boolean".to_string(),
                    ];
                    if let Ok(Some(selected)) = picker::pick(types, "Convert to: ") {
                        if let Some(view) = app.view_mut() {
                            let result = match selected.as_str() {
                                "String" => view.dataframe.column(&col_name)
                                    .and_then(|c| c.cast(&polars::prelude::DataType::String)),
                                "Int64" => view.dataframe.column(&col_name)
                                    .and_then(|c| c.cast(&polars::prelude::DataType::Int64)),
                                "Float64" => view.dataframe.column(&col_name)
                                    .and_then(|c| c.cast(&polars::prelude::DataType::Float64)),
                                "Boolean" => view.dataframe.column(&col_name)
                                    .and_then(|c| c.cast(&polars::prelude::DataType::Boolean)),
                                _ => Err(polars::prelude::PolarsError::ComputeError("Unknown type".into())),
                            };
                            match result {
                                Ok(new_col) => {
                                    if let Err(e) = view.dataframe.with_column(new_col) {
                                        app.err(e);
                                    } else {
                                        
                                    }
                                }
                                Err(e) => app.err(e),
                            }
                        }
                    }
                }
            }
        }
        KeyCode::Char('b') => {
            // b: Aggregate by current column
            if let Some(view) = app.view() {
                if let Some(col) = view.state.cur_col(&view.dataframe) {
                    if let Ok(Some(func)) = picker::pick(vec![
                        "count".into(), "sum".into(), "mean".into(), "min".into(), "max".into(), "std".into(),
                    ], "Aggregate: ") {
                        if let Err(e) = CommandExecutor::exec(app, Box::new(Agg { col, func })) {
                            app.err(e);
                        }
                    }
                }
            }
        }
        KeyCode::Char('T') => {
            // T: Duplicate current view
            if app.has_view() {
                let _ = CommandExecutor::exec(app, Box::new(Dup));
            }
        }
        KeyCode::Char('W') => {
            // W: Swap top two views
            if let Err(e) = CommandExecutor::exec(app, Box::new(Swap)) {
                app.err(e);
            }
        }
        KeyCode::Char('l') => {
            // l: Directory listing
            let dir = std::env::current_dir().unwrap_or_default();
            if let Err(e) = CommandExecutor::exec(app, Box::new(Ls { dir })) {
                app.err(e);
            }
        }
        KeyCode::Char('r') => {
            // r: Recursive directory listing
            let dir = std::env::current_dir().unwrap_or_default();
            if let Err(e) = CommandExecutor::exec(app, Box::new(Lr { dir })) {
                app.err(e);
            }
        }
        KeyCode::Char('C') => {
            // C: Correlation matrix (uses selected columns if >= 2, otherwise all numeric)
            if app.has_view() {
                if let Err(e) = CommandExecutor::exec(app, Box::new(Correlation {
                    selected_cols: app.view().map(|v| v.selected_cols.iter().copied().collect()).unwrap_or_default()
                })) {
                    app.err(e);
                } else if let Some(view) = app.view_mut() {
                    view.selected_cols.clear();
                }
            }
        }
        KeyCode::Char(':') => {
            // :: Jump to row number
            if let Some(input) = prompt(app, "Go to row: ")? {
                if let Ok(row) = input.parse::<usize>() {
                    if let Some(view) = app.view_mut() {
                        let max_rows = view.rows();
                        if row < max_rows {
                            view.state.cr = row;
                            view.state.visible();
                            app.msg(format!("Row {}", row));
                        } else {
                            app.msg(format!("Row {} out of range (max {})", row, max_rows - 1));
                        }
                    }
                } else {
                    app.msg("Invalid row number".to_string());
                }
            }
        }
        KeyCode::Char('@') => {
            // @: Jump to column by name
            if let Some(view) = app.view() {
                let col_names: Vec<String> = view.dataframe.get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                if let Ok(Some(selected)) = picker::pick(col_names.clone(), "Column: ") {
                    if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                        if let Some(view) = app.view_mut() {
                            view.state.cc = idx;
                            app.msg(format!("Column: {}", selected));
                        }
                    }
                }
            }
        }
        KeyCode::Char('m') => {
            // m: Toggle bookmark on current row
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
        KeyCode::Char('\'') => {
            // ': Jump to next bookmark
            if app.bookmarks.is_empty() {
                app.msg("No bookmarks".to_string());
            } else if let Some(row) = app.bookmarks.iter()
                .find(|&&r| r > app.view().map(|v| v.state.cr).unwrap_or(0)).copied()
                .or_else(|| app.bookmarks.first().copied())
            {
                if let Some(view) = app.view_mut() {
                    view.state.cr = row;
                    view.state.visible();
                }
                app.msg(format!("Bookmark: row {}", row));
            }
        }
        KeyCode::Char(' ') => {
            // Space: Toggle selection (rows in Meta/Freq views, columns otherwise)
            let msg: Option<String> = if let Some(view) = app.view_mut() {
                let is_meta = view.name == "metadata";
                let is_freq = view.name.starts_with("Freq:");

                if is_meta || is_freq {
                    let cr = view.state.cr;
                    if view.selected_rows.contains(&cr) {
                        view.selected_rows.remove(&cr);
                        Some(format!("Deselected row ({} selected)", view.selected_rows.len()))
                    } else {
                        view.selected_rows.insert(cr);
                        Some(format!("Selected row ({} selected)", view.selected_rows.len()))
                    }
                } else {
                    let cc = view.state.cc;
                    if view.selected_cols.contains(&cc) {
                        view.selected_cols.remove(&cc);
                        Some(format!("Deselected column ({} selected)", view.selected_cols.len()))
                    } else {
                        view.selected_cols.insert(cc);
                        Some(format!("Selected column ({} selected)", view.selected_cols.len()))
                    }
                }
            } else {
                None
            };
            if let Some(m) = msg {
                app.msg(m);
            }
        }
        KeyCode::Esc => {
            // Esc: Clear selection
            if let Some(view) = app.view_mut() {
                if !view.selected_cols.is_empty() || !view.selected_rows.is_empty() {
                    view.selected_cols.clear();
                    view.selected_rows.clear();
                    
                }
            }
        }
        KeyCode::Char('0') => {
            // 0: In Meta view, select rows with 100% null; otherwise select all-null columns
            // First pass: collect indices
            let selection: Option<(bool, Vec<usize>)> = app.view().map(|view| {
                let is_meta = view.name == "metadata";
                if is_meta {
                    let df = &view.dataframe;
                    let null_rows: Vec<usize> = df.column("null%").ok().map(|null_col| {
                        let series = null_col.as_materialized_series();
                        (0..series.len())
                            .filter(|&row_idx| {
                                series.get(row_idx).ok()
                                    .map(|v| {
                                        let s = v.to_string();
                                        s == "100" || s == "100.0"
                                    })
                                    .unwrap_or(false)
                            })
                            .collect()
                    }).unwrap_or_default();
                    (true, null_rows)
                } else {
                    let df = &view.dataframe;
                    let total_rows = df.height();
                    let null_cols: Vec<usize> = df.get_columns().iter().enumerate()
                        .filter(|(_, col)| col.as_materialized_series().null_count() == total_rows)
                        .map(|(idx, _)| idx)
                        .collect();
                    (false, null_cols)
                }
            });

            if let Some((is_rows, indices)) = selection {
                if indices.is_empty() {
                    app.msg("No all-null columns found".to_string());
                } else if let Some(view) = app.view_mut() {
                    let count = indices.len();
                    if is_rows {
                        for idx in indices {
                            view.selected_rows.insert(idx);
                        }
                        app.msg(format!("Selected {} row(s) with 100% null", count));
                    } else {
                        for idx in indices {
                            view.selected_cols.insert(idx);
                        }
                        app.msg(format!("Selected {} all-null column(s)", count));
                    }
                }
            }
        }
        KeyCode::Char('1') => {
            // 1: In Meta view, select rows with 1 distinct value; otherwise select single-value columns
            let selection: Option<(bool, Vec<usize>)> = app.view().map(|view| {
                let is_meta = view.name == "metadata";
                if is_meta {
                    let df = &view.dataframe;
                    let single_rows: Vec<usize> = df.column("distinct").ok().map(|distinct_col| {
                        let series = distinct_col.as_materialized_series();
                        (0..series.len())
                            .filter(|&row_idx| {
                                series.get(row_idx).ok()
                                    .map(|v| v.to_string() == "1")
                                    .unwrap_or(false)
                            })
                            .collect()
                    }).unwrap_or_default();
                    (true, single_rows)
                } else {
                    let df = &view.dataframe;
                    let single_cols: Vec<usize> = df.get_columns().iter().enumerate()
                        .filter(|(_, col)| {
                            let series = col.as_materialized_series();
                            series.n_unique().ok().map(|n_unique| {
                                let null_count = series.null_count();
                                if null_count > 0 && null_count < series.len() {
                                    n_unique <= 2
                                } else {
                                    n_unique == 1
                                }
                            }).unwrap_or(false)
                        })
                        .map(|(idx, _)| idx)
                        .collect();
                    (false, single_cols)
                }
            });

            if let Some((is_rows, indices)) = selection {
                if indices.is_empty() {
                    app.msg("No single-value columns found".to_string());
                } else if let Some(view) = app.view_mut() {
                    let count = indices.len();
                    if is_rows {
                        for idx in indices {
                            view.selected_rows.insert(idx);
                        }
                        app.msg(format!("Selected {} row(s) with 1 distinct value", count));
                    } else {
                        for idx in indices {
                            view.selected_cols.insert(idx);
                        }
                        app.msg(format!("Selected {} single-value column(s)", count));
                    }
                }
            }
        }
        _ => {}
    }

    Ok(true)
}

/// Strip quotes from polars string values
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

/// Get SQL hints for search/filter: LIKE patterns (%) + unique values with quotes
fn hints(df: &polars::prelude::DataFrame, col_name: &str, row: usize) -> Vec<String> {
    let mut items = Vec::new();
    if let Ok(col) = df.column(col_name) {
        let is_str = matches!(col.dtype(), polars::prelude::DataType::String);
        if is_str {
            if let Ok(val) = col.get(row) {
                let v = unquote(&val.to_string());
                if v.len() >= 2 {
                    items.push(format!("{} LIKE '{}%'", col_name, &v[..2]));
                    items.push(format!("{} LIKE '%{}'", col_name, &v[v.len()-2..]));
                }
            }
        }
        if let Ok(uniq) = col.unique() {
            for i in 0..uniq.len() {
                if let Ok(v) = uniq.get(i) {
                    let val = unquote(&v.to_string());
                    if is_str {
                        items.push(format!("{} = '{}'", col_name, val));
                    } else {
                        items.push(format!("{} = {}", col_name, val));
                    }
                }
            }
        }
    }
    items
}

/// Find rows matching SQL WHERE expression, returns row indices
fn find(df: &polars::prelude::DataFrame, expr: &str) -> Vec<usize> {
    use polars::prelude::*;
    let mut ctx = polars::sql::SQLContext::new();
    let with_idx = df.clone().lazy().with_row_index("__idx__", None);
    ctx.register("df", with_idx);
    ctx.execute(&format!("SELECT __idx__ FROM df WHERE {}", expr))
        .and_then(|lf| lf.collect())
        .map(|result| {
            result.column("__idx__").ok()
                .and_then(|c| c.idx().ok())
                .map(|idx| idx.into_iter().filter_map(|v| v.map(|i| i as usize)).collect())
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

/// Prompt user for input
/// Returns None if user cancels (Esc)
fn prompt(app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
    // Render current screen first
    Renderer::render(app)?;

    // Show prompt at bottom
    let (_cols, rows) = terminal::size()?;
    execute!(
        io::stdout(),
        cursor::MoveTo(0, rows - 1),
        terminal::Clear(terminal::ClearType::CurrentLine),
        Print(prompt),
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
                    // Re-render prompt
                    execute!(
                        io::stdout(),
                        cursor::MoveTo(0, rows - 1),
                        terminal::Clear(terminal::ClearType::CurrentLine),
                        Print(prompt),
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

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    fn make_test_df() -> DataFrame {
        df! {
            "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"]
        }.unwrap()
    }

    #[test]
    fn test_find_exact() {
        let df = make_test_df();
        assert_eq!(find(&df, "name = 'banana'"), vec![1]);
        assert_eq!(find(&df, "name = 'notfound'"), vec![]);
    }

    #[test]
    fn test_find_like() {
        let df = make_test_df();
        assert_eq!(find(&df, "name LIKE 'b%'"), vec![1, 5]);  // banana, blueberry
        assert_eq!(find(&df, "name LIKE '%rry'"), vec![2, 5]);  // cherry, blueberry
        assert_eq!(find(&df, "name LIKE '%apple%'"), vec![0, 3]);  // apple, pineapple
    }
}
