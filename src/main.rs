mod app;
mod command;
mod picker;
mod render;
mod state;

use anyhow::Result;
use app::AppContext;
use command::executor::CommandExecutor;
use command::io::{Load, Save};
use command::transform::{DelCol, DelNull, DelSingle, Filter, RenameCol, Select, Sort};
use command::view::{Correlation, Frequency, Metadata};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::{cursor, execute, style::Print, terminal};
use render::{Renderer, Terminal};
use std::fs;
use std::io::{self, Write};

fn main() -> Result<()> {
    // Get command line args
    let args: Vec<String> = std::env::args().collect();

    // Check for -c argument (inline script)
    let cmd_mode = args.iter().position(|a| a == "-c");
    if let Some(idx) = cmd_mode {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv -c '<commands>'");
            std::process::exit(1);
        }
        let commands = &args[idx + 1];
        return run_commands(commands);
    }

    // Check for --script argument
    let script_mode = args.iter().position(|a| a == "--script");

    if let Some(idx) = script_mode {
        // Script mode: run commands from file and print result
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --script <script_file>");
            std::process::exit(1);
        }
        let script_path = &args[idx + 1];
        return run_script(script_path);
    }

    // Initialize terminal
    let _terminal = Terminal::init()?;

    // Create app context
    let mut app = if args.len() > 1 {
        // Load file from CLI argument
        let file_path = &args[1];
        let cmd = Box::new(Load { file_path: file_path.clone() });
        let mut temp_app = AppContext::new();

        match CommandExecutor::execute(&mut temp_app, cmd) {
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
    let (cols, rows) = Terminal::size()?;
    app.update_viewport(rows, cols);

    // Main event loop
    loop {
        // Render
        Renderer::render(&mut app)?;

        // Handle events
        if let Event::Key(key) = event::read()? {
            if !handle_key(&mut app, key)? {
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
    app.update_viewport(50, 120);

    for cmd_str in commands.split('|') {
        let cmd_str = cmd_str.trim();
        if cmd_str.is_empty() || cmd_str == "quit" {
            continue;
        }

        if let Some(cmd) = parse_command(cmd_str, &app) {
            if let Err(e) = CommandExecutor::execute(&mut app, cmd) {
                eprintln!("Error executing '{}': {}", cmd_str, e);
            }
        } else {
            eprintln!("Unknown command: {}", cmd_str);
        }
    }

    print_table(&app);
    Ok(())
}

/// Run commands from a script file and print result
fn run_script(script_path: &str) -> Result<()> {
    let script = fs::read_to_string(script_path)?;
    let mut app = AppContext::new();

    // Set a reasonable viewport for printing
    app.update_viewport(50, 120);

    'outer: for line in script.lines() {
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
            if let Some(cmd) = parse_command(cmd_str, &app) {
                if let Err(e) = CommandExecutor::execute(&mut app, cmd) {
                    eprintln!("Error executing '{}': {}", cmd_str, e);
                }
            } else {
                eprintln!("Unknown command: {}", cmd_str);
            }
        }
    }

    // Print the final state
    print_table(&app);

    Ok(())
}

/// Print table to stdout (for script mode)
fn print_table(app: &AppContext) {
    if let Some(view) = app.current_view() {
        let df = &view.dataframe;
        println!("=== {} ({} rows) ===", view.name, df.height());
        println!("{}", df);
    } else {
        println!("No table loaded");
    }
}

/// Parse a text command into a Command object
fn parse_command(line: &str, _app: &AppContext) -> Option<Box<dyn command::Command>> {
    let parts: Vec<&str> = line.splitn(2, ' ').collect();
    let cmd = parts[0].to_lowercase();
    let arg = parts.get(1).map(|s| s.trim()).unwrap_or("");

    match cmd.as_str() {
        "load" => Some(Box::new(Load { file_path: arg.to_string() })),
        "save" => Some(Box::new(Save { file_path: arg.to_string() })),
        "freq" | "frequency" => Some(Box::new(Frequency { col_name: arg.to_string() })),
        "meta" | "metadata" => Some(Box::new(Metadata)),
        "corr" | "correlation" => Some(Box::new(Correlation { selected_cols: vec![] })),
        "delcol" => Some(Box::new(DelCol { col_name: arg.to_string() })),
        "delnull" => Some(Box::new(DelNull)),
        "del1" => Some(Box::new(DelSingle)),
        "filter" => Some(Box::new(Filter { expression: arg.to_string() })),
        "select" | "sel" => {
            let col_names: Vec<String> = arg.split(',').map(|s| s.trim().to_string()).collect();
            Some(Box::new(Select { col_names }))
        }
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
fn handle_key(app: &mut AppContext, key: KeyEvent) -> Result<bool> {
    match key.code {
        KeyCode::Char('q') => {
            // Quit if no view or only one view
            if !app.has_view() || app.stack.len() == 1 {
                return Ok(false);
            }
            // Otherwise pop from stack and clear message
            app.stack.pop();
            app.message.clear();
        }
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            // Ctrl+C to force quit
            return Ok(false);
        }
        KeyCode::Up => {
            if let Some(view) = app.current_view_mut() {
                view.state.move_up(1);
            }
        }
        KeyCode::Down => {
            if let Some(view) = app.current_view_mut() {
                let max_rows = view.row_count();
                view.state.move_down(1, max_rows);
            }
        }
        KeyCode::Left => {
            if let Some(view) = app.current_view_mut() {
                view.state.move_left(1);
            }
        }
        KeyCode::Right => {
            if let Some(view) = app.current_view_mut() {
                let max_cols = view.col_count();
                view.state.move_right(1, max_cols);
            }
        }
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            // Ctrl+D: Page down
            if let Some(view) = app.current_view_mut() {
                let max_rows = view.row_count();
                view.state.page_down(max_rows);
            }
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            // Ctrl+U: Page up
            if let Some(view) = app.current_view_mut() {
                view.state.page_up();
            }
        }
        KeyCode::Char('g') => {
            // g: Go to top
            if let Some(view) = app.current_view_mut() {
                view.state.goto_top();
            }
        }
        KeyCode::Char('G') => {
            // G: Go to bottom
            if let Some(view) = app.current_view_mut() {
                let max_rows = view.row_count();
                view.state.goto_bottom(max_rows);
            }
        }
        KeyCode::PageUp => {
            // Page Up
            if let Some(view) = app.current_view_mut() {
                view.state.page_up();
            }
        }
        KeyCode::PageDown => {
            // Page Down
            if let Some(view) = app.current_view_mut() {
                let max_rows = view.row_count();
                view.state.page_down(max_rows);
            }
        }
        KeyCode::Home => {
            // Home: Go to top
            if let Some(view) = app.current_view_mut() {
                view.state.goto_top();
            }
        }
        KeyCode::End => {
            // End: Go to bottom
            if let Some(view) = app.current_view_mut() {
                let max_rows = view.row_count();
                view.state.goto_bottom(max_rows);
            }
        }
        KeyCode::Char('L') => {
            // L: Load file
            if let Some(file_path) = prompt_input(app, "Load file: ")? {
                let cmd = Box::new(Load { file_path });
                if let Err(e) = CommandExecutor::execute(app, cmd) {
                    app.set_message(format!("Error: {}", e));
                }
            }
        }
        KeyCode::Char('S') => {
            // S: Save file
            if !app.has_view() {
                app.set_message("No table loaded".to_string());
            } else if let Some(file_path) = prompt_input(app, "Save to: ")? {
                let cmd = Box::new(Save { file_path });
                if let Err(e) = CommandExecutor::execute(app, cmd) {
                    app.set_message(format!("Error: {}", e));
                }
            }
        }
        KeyCode::Char('D') => {
            // D: Delete column(s) - in metadata view, delete from parent; otherwise delete current
            // Extract needed data first to avoid borrow issues
            let delete_info: Option<(bool, Option<usize>, Vec<String>)> = app.current_view().map(|view| {
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
                                .map(|v| {
                                    let s = v.to_string();
                                    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                                        s[1..s.len()-1].to_string()
                                    } else {
                                        s
                                    }
                                })
                        })
                        .collect();
                    (true, parent_id, col_names)
                } else {
                    let cols_to_delete: Vec<String> = if view.selected_cols.is_empty() {
                        view.state.current_column(&view.dataframe)
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
                        if let Some(parent) = app.stack.find_by_id_mut(pid) {
                            let mut deleted = 0;
                            for col_name in &col_names {
                                if parent.dataframe.drop_in_place(col_name).is_ok() {
                                    deleted += 1;
                                }
                            }
                            app.stack.pop();
                            app.set_message(format!("Deleted {} column(s)", deleted));
                        }
                    }
                } else if col_names.is_empty() {
                    app.set_message("No columns to delete".to_string());
                } else {
                    let mut deleted = 0;
                    for col_name in &col_names {
                        let cmd = Box::new(DelCol { col_name: col_name.clone() });
                        if CommandExecutor::execute(app, cmd).is_ok() {
                            deleted += 1;
                        }
                    }
                    if let Some(view) = app.current_view_mut() {
                        view.selected_cols.clear();
                    }
                    app.set_message(format!("Deleted {} column(s)", deleted));
                }
            }
        }
        KeyCode::Char('/') => {
            // /: Search column values with skim
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    // Get unique values for current column
                    let col = view.dataframe.column(&col_name).ok();
                    if let Some(c) = col {
                        let unique = c.unique().ok();
                        if let Some(u) = unique {
                            let items: Vec<String> = (0..u.len())
                                .filter_map(|i| u.get(i).ok().map(|v| {
                                    // Strip quotes from string values
                                    let s = v.to_string();
                                    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                                        s[1..s.len()-1].to_string()
                                    } else {
                                        s
                                    }
                                }))
                                .collect();

                            if let Ok(Some(selected)) = picker::pick(items, &format!("{}> ", col_name)) {
                                // Store search state for n/N
                                app.search.col_name = Some(col_name.clone());
                                app.search.value = Some(selected.clone());
                                app.search.regex = None; // Clear regex search

                                // Find first occurrence
                                if let Some(view) = app.current_view_mut() {
                                    if let Some(pos) = find_value(&view.dataframe, &col_name, &selected, 0, true) {
                                        view.state.cr = pos;
                                        view.state.ensure_visible();
                                        app.set_message(format!("Found: {}={}", col_name, selected));
                                    } else {
                                        app.set_message(format!("Not found: {}={}", col_name, selected));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        KeyCode::Char('\\') => {
            // \: Filter rows with expression (using skim with column values as hints)
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    // Get unique values as hints
                    let items: Vec<String> = view.dataframe.column(&col_name)
                        .ok()
                        .and_then(|c| c.unique().ok())
                        .map(|u| {
                            (0..u.len())
                                .filter_map(|i| u.get(i).ok().map(|v| {
                                    let s = v.to_string();
                                    // Strip quotes and format as filter expression
                                    let val = if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                                        &s[1..s.len()-1]
                                    } else {
                                        &s
                                    };
                                    format!("{}=={}", col_name, val)
                                }))
                                .collect()
                        })
                        .unwrap_or_default();

                    if let Ok(Some(expression)) = picker::input_with_hints(items, "Filter> ") {
                        let cmd = Box::new(Filter { expression });
                        if let Err(e) = CommandExecutor::execute(app, cmd) {
                            app.set_message(format!("Error: {}", e));
                        }
                    }
                }
            } else {
                app.set_message("No table loaded".to_string());
            }
        }
        KeyCode::Char('n') => {
            // n: Find next occurrence (value or regex)
            if let Some(col_name) = app.search.col_name.clone() {
                if let Some(pattern) = app.search.regex.clone() {
                    // Regex search
                    if let Ok(re) = regex::Regex::new(&pattern) {
                        if let Some(view) = app.current_view_mut() {
                            let start = view.state.cr + 1;
                            if let Some(pos) = find_regex(&view.dataframe, &col_name, &re, start, true) {
                                view.state.cr = pos;
                                view.state.ensure_visible();
                                app.set_message(format!("Regex match: /{}/", pattern));
                            } else {
                                app.set_message("No more matches".to_string());
                            }
                        }
                    }
                } else if let Some(value) = app.search.value.clone() {
                    // Value search
                    if let Some(view) = app.current_view_mut() {
                        let start = view.state.cr + 1;
                        if let Some(pos) = find_value(&view.dataframe, &col_name, &value, start, true) {
                            view.state.cr = pos;
                            view.state.ensure_visible();
                            app.set_message(format!("Found: {}={}", col_name, value));
                        } else {
                            app.set_message("No more matches".to_string());
                        }
                    }
                } else {
                    app.set_message("No search active".to_string());
                }
            } else {
                app.set_message("No search active".to_string());
            }
        }
        KeyCode::Char('N') => {
            // N: Find previous occurrence (value or regex)
            if let Some(col_name) = app.search.col_name.clone() {
                if let Some(pattern) = app.search.regex.clone() {
                    // Regex search
                    if let Ok(re) = regex::Regex::new(&pattern) {
                        if let Some(view) = app.current_view_mut() {
                            let start = view.state.cr.saturating_sub(1);
                            if let Some(pos) = find_regex(&view.dataframe, &col_name, &re, start, false) {
                                view.state.cr = pos;
                                view.state.ensure_visible();
                                app.set_message(format!("Regex match: /{}/", pattern));
                            } else {
                                app.set_message("No more matches".to_string());
                            }
                        }
                    }
                } else if let Some(value) = app.search.value.clone() {
                    // Value search
                    if let Some(view) = app.current_view_mut() {
                        let start = view.state.cr.saturating_sub(1);
                        if let Some(pos) = find_value(&view.dataframe, &col_name, &value, start, false) {
                            view.state.cr = pos;
                            view.state.ensure_visible();
                            app.set_message(format!("Found: {}={}", col_name, value));
                        } else {
                            app.set_message("No more matches".to_string());
                        }
                    }
                } else {
                    app.set_message("No search active".to_string());
                }
            } else {
                app.set_message("No search active".to_string());
            }
        }
        KeyCode::Char('*') => {
            // *: Search for current cell value
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    let cr = view.state.cr;
                    let col_idx = view.state.cc;
                    if let Ok(value) = view.dataframe.get_columns()[col_idx].get(cr) {
                        let value_str = value.to_string();
                        app.search.col_name = Some(col_name.clone());
                        app.search.value = Some(value_str.clone());
                        app.search.regex = None; // Clear regex search
                        app.set_message(format!("Search: {}={}", col_name, value_str));
                    }
                }
            }
        }
        KeyCode::Char('s') => {
            // s: Select columns
            if !app.has_view() {
                app.set_message("No table loaded".to_string());
            } else if let Some(cols_str) = prompt_input(app, "Select columns (comma-separated): ")? {
                let col_names: Vec<String> = cols_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                let cmd = Box::new(Select { col_names });
                if let Err(e) = CommandExecutor::execute(app, cmd) {
                    app.set_message(format!("Error: {}", e));
                }
            }
        }
        KeyCode::Char('F') => {
            // F: Frequency table for current column
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    let cmd = Box::new(Frequency { col_name });
                    if let Err(e) = CommandExecutor::execute(app, cmd) {
                        app.set_message(format!("Error: {}", e));
                    }
                }
            }
        }
        KeyCode::Char('M') => {
            // M: Metadata view
            if app.has_view() {
                let cmd = Box::new(Metadata);
                if let Err(e) = CommandExecutor::execute(app, cmd) {
                    app.set_message(format!("Error: {}", e));
                }
            }
        }
        KeyCode::Char('[') => {
            // [: Sort ascending by current column
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    let cmd = Box::new(Sort {
                        col_name,
                        descending: false,
                    });
                    if let Err(e) = CommandExecutor::execute(app, cmd) {
                        app.set_message(format!("Error: {}", e));
                    }
                }
            }
        }
        KeyCode::Char(']') => {
            // ]: Sort descending by current column
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    let cmd = Box::new(Sort {
                        col_name,
                        descending: true,
                    });
                    if let Err(e) = CommandExecutor::execute(app, cmd) {
                        app.set_message(format!("Error: {}", e));
                    }
                }
            }
        }
        KeyCode::Char('^') => {
            // ^: Rename current column
            if let Some(view) = app.current_view() {
                if let Some(old_name) = view.state.current_column(&view.dataframe) {
                    if let Some(new_name) = prompt_input(app, &format!("Rename '{}' to: ", old_name))? {
                        let cmd = Box::new(RenameCol { old_name, new_name });
                        if let Err(e) = CommandExecutor::execute(app, cmd) {
                            app.set_message(format!("Error: {}", e));
                        }
                    }
                }
            }
        }
        KeyCode::Enter => {
            // Enter: Filter parent table from frequency view (supports multiple selections)
            // Extract needed info first
            let filter_info: Option<(usize, String, Vec<String>, Option<String>)> = app.current_view().and_then(|view| {
                if let (Some(parent_id), Some(freq_col)) = (view.parent_id, view.freq_col.clone()) {
                    let rows_to_use: Vec<usize> = if view.selected_rows.is_empty() {
                        vec![view.state.cr]
                    } else {
                        view.selected_rows.iter().copied().collect()
                    };

                    let values: Vec<String> = rows_to_use.iter()
                        .filter_map(|&row| {
                            view.dataframe.get_columns()[0]
                                .get(row)
                                .ok()
                                .map(|v| {
                                    let s = v.to_string();
                                    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                                        s[1..s.len()-1].to_string()
                                    } else {
                                        s
                                    }
                                })
                        })
                        .collect();

                    if let Some(parent) = app.stack.find_by_id(parent_id) {
                        Some((parent_id, freq_col, values, parent.filename.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            });

            if let Some((parent_id, freq_col, values, parent_filename)) = filter_info {
                if values.is_empty() {
                    app.set_message("No values to filter".to_string());
                } else if let Some(parent) = app.stack.find_by_id(parent_id) {
                    let parent_df = parent.dataframe.clone();

                    match filter_by_values(&parent_df, &freq_col, &values) {
                        Ok(filtered_df) => {
                            let row_count = filtered_df.height();
                            let id = app.next_id();
                            let view_name = if values.len() == 1 {
                                format!("{}={}", freq_col, values[0])
                            } else {
                                format!("{}∈{{{}}}", freq_col, values.len())
                            };
                            let new_view = state::ViewState::new(
                                id,
                                view_name,
                                filtered_df,
                                parent_filename,
                            );
                            app.stack.push(new_view);
                            app.set_message(format!("Filtered: {} rows", row_count));
                        }
                        Err(e) => app.set_message(format!("Error: {}", e)),
                    }
                }
            }
        }
        KeyCode::Char('c') => {
            // c: Copy current column
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    let new_name = format!("{}_copy", col_name);
                    let col = view.dataframe.column(&col_name).ok().cloned();
                    if let Some(c) = col {
                        if let Some(view) = app.current_view_mut() {
                            let new_col = c.as_materialized_series().clone().with_name(new_name.clone().into());
                            if let Err(e) = view.dataframe.with_column(new_col) {
                                app.set_message(format!("Error: {}", e));
                            } else {
                                app.set_message(format!("Copied column '{}' to '{}'", col_name, new_name));
                            }
                        }
                    }
                }
            }
        }
        KeyCode::Char('$') => {
            // $: Type convert column
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    let types = vec![
                        "String".to_string(),
                        "Int64".to_string(),
                        "Float64".to_string(),
                        "Boolean".to_string(),
                    ];
                    if let Ok(Some(selected)) = picker::pick(types, "Convert to: ") {
                        if let Some(view) = app.current_view_mut() {
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
                                        app.set_message(format!("Error: {}", e));
                                    } else {
                                        app.set_message(format!("Converted '{}' to {}", col_name, selected));
                                    }
                                }
                                Err(e) => app.set_message(format!("Error: {}", e)),
                            }
                        }
                    }
                }
            }
        }
        KeyCode::Char('b') => {
            // b: Aggregate by current column
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    let agg_funcs = vec![
                        "count".to_string(),
                        "sum".to_string(),
                        "mean".to_string(),
                        "min".to_string(),
                        "max".to_string(),
                        "std".to_string(),
                    ];
                    if let Ok(Some(agg_fn)) = picker::pick(agg_funcs, "Aggregate: ") {
                        let df = view.dataframe.clone();
                        let filename = view.filename.clone();
                        let result = aggregate_by(&df, &col_name, &agg_fn);
                        match result {
                            Ok(agg_df) => {
                                let id = app.next_id();
                                let new_view = state::ViewState::new(
                                    id,
                                    format!("{}:{}", agg_fn, col_name),
                                    agg_df,
                                    filename,
                                );
                                app.stack.push(new_view);
                                app.set_message(format!("{} by '{}'", agg_fn, col_name));
                            }
                            Err(e) => app.set_message(format!("Error: {}", e)),
                        }
                    }
                }
            }
        }
        KeyCode::Char('T') => {
            // T: Duplicate current view on stack
            if let Some(view) = app.current_view() {
                let mut new_view = view.clone();
                let old_name = view.name.clone();
                new_view.id = app.next_id();
                new_view.name = format!("{} (copy)", old_name);
                app.stack.push(new_view);
                app.set_message("Duplicated view".to_string());
            }
        }
        KeyCode::Char('W') => {
            // W: Swap with previous view on stack
            if app.stack.len() >= 2 {
                app.stack.swap_top();
                app.set_message("Swapped views".to_string());
            } else {
                app.set_message("Need at least 2 views to swap".to_string());
            }
        }
        KeyCode::Char('l') => {
            // l: Directory listing
            let dir = std::env::current_dir().unwrap_or_default();
            match list_directory(&dir) {
                Ok(df) => {
                    let id = app.next_id();
                    let new_view = state::ViewState::new(
                        id,
                        format!("ls:{}", dir.display()),
                        df,
                        None,
                    );
                    app.stack.push(new_view);
                    app.set_message(format!("Directory: {}", dir.display()));
                }
                Err(e) => app.set_message(format!("Error: {}", e)),
            }
        }
        KeyCode::Char('C') => {
            // C: Correlation matrix (uses selected columns if >= 2, otherwise all numeric)
            let selected: Vec<usize> = app.current_view()
                .map(|v| v.selected_cols.iter().copied().collect())
                .unwrap_or_default();
            if app.has_view() {
                let cmd = Box::new(Correlation { selected_cols: selected });
                if let Err(e) = CommandExecutor::execute(app, cmd) {
                    app.set_message(format!("Error: {}", e));
                } else {
                    if let Some(view) = app.current_view_mut() {
                        view.selected_cols.clear();
                    }
                }
            }
        }
        KeyCode::Char('?') => {
            // ?: Regex search in current column
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    if let Some(pattern) = prompt_input(app, "Regex: ")? {
                        match regex::Regex::new(&pattern) {
                            Ok(re) => {
                                app.search.col_name = Some(col_name.clone());
                                app.search.regex = Some(pattern.clone());
                                app.search.value = None;

                                // Find first match
                                if let Some(view) = app.current_view_mut() {
                                    if let Some(pos) = find_regex(&view.dataframe, &col_name, &re, 0, true) {
                                        view.state.cr = pos;
                                        view.state.ensure_visible();
                                        app.set_message(format!("Regex match: /{}/", pattern));
                                    } else {
                                        app.set_message(format!("No match: /{}/", pattern));
                                    }
                                }
                            }
                            Err(e) => app.set_message(format!("Invalid regex: {}", e)),
                        }
                    }
                }
            }
        }
        KeyCode::Char('|') => {
            // |: Regex filter on current column
            let col_info = app.current_view().map(|view| {
                (
                    view.state.current_column(&view.dataframe),
                    view.dataframe.clone(),
                    view.filename.clone(),
                )
            });
            if let Some((Some(col_name), df, filename)) = col_info {
                if let Some(pattern) = prompt_input(app, "Regex filter: ")? {
                    match regex::Regex::new(&pattern) {
                        Ok(re) => {
                            // Filter rows matching regex
                            match filter_by_regex(&df, &col_name, &re) {
                                Ok(filtered_df) => {
                                    let row_count = filtered_df.height();
                                    let id = app.next_id();
                                    let new_view = state::ViewState::new(
                                        id,
                                        format!("{}~/{}/", col_name, pattern),
                                        filtered_df,
                                        filename,
                                    );
                                    app.stack.push(new_view);
                                    app.set_message(format!("Regex filter: {} rows", row_count));
                                }
                                Err(e) => app.set_message(format!("Error: {}", e)),
                            }
                        }
                        Err(e) => app.set_message(format!("Invalid regex: {}", e)),
                    }
                }
            } else {
                app.set_message("No table loaded".to_string());
            }
        }
        KeyCode::Char(':') => {
            // :: Jump to row number
            if let Some(input) = prompt_input(app, "Go to row: ")? {
                if let Ok(row) = input.parse::<usize>() {
                    if let Some(view) = app.current_view_mut() {
                        let max_rows = view.row_count();
                        if row < max_rows {
                            view.state.cr = row;
                            view.state.ensure_visible();
                            app.set_message(format!("Row {}", row));
                        } else {
                            app.set_message(format!("Row {} out of range (max {})", row, max_rows - 1));
                        }
                    }
                } else {
                    app.set_message("Invalid row number".to_string());
                }
            }
        }
        KeyCode::Char('@') => {
            // @: Jump to column by name
            if let Some(view) = app.current_view() {
                let col_names: Vec<String> = view.dataframe.get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                if let Ok(Some(selected)) = picker::pick(col_names.clone(), "Column: ") {
                    if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                        if let Some(view) = app.current_view_mut() {
                            view.state.cc = idx;
                            app.set_message(format!("Column: {}", selected));
                        }
                    }
                }
            }
        }
        KeyCode::Char('m') => {
            // m: Toggle bookmark on current row
            if let Some(view) = app.current_view() {
                let cr = view.state.cr;
                if let Some(pos) = app.bookmarks.iter().position(|&r| r == cr) {
                    app.bookmarks.remove(pos);
                    app.set_message(format!("Removed bookmark at row {}", cr));
                } else {
                    app.bookmarks.push(cr);
                    app.bookmarks.sort();
                    app.set_message(format!("Bookmarked row {} ({} total)", cr, app.bookmarks.len()));
                }
            }
        }
        KeyCode::Char('\'') => {
            // ': Jump to next bookmark
            if app.bookmarks.is_empty() {
                app.set_message("No bookmarks".to_string());
            } else {
                let cr = app.current_view().map(|v| v.state.cr).unwrap_or(0);
                // Find next bookmark after current row
                let next = app.bookmarks.iter().find(|&&r| r > cr).copied()
                    .or_else(|| app.bookmarks.first().copied());
                if let Some(row) = next {
                    if let Some(view) = app.current_view_mut() {
                        view.state.cr = row;
                        view.state.ensure_visible();
                    }
                    app.set_message(format!("Bookmark: row {}", row));
                }
            }
        }
        KeyCode::Char(' ') => {
            // Space: Toggle selection (rows in Meta/Freq views, columns otherwise)
            let msg: Option<String> = if let Some(view) = app.current_view_mut() {
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
                app.set_message(m);
            }
        }
        KeyCode::Esc => {
            // Esc: Clear selection
            if let Some(view) = app.current_view_mut() {
                if !view.selected_cols.is_empty() || !view.selected_rows.is_empty() {
                    view.selected_cols.clear();
                    view.selected_rows.clear();
                    app.set_message("Selection cleared".to_string());
                }
            }
        }
        KeyCode::Char('0') => {
            // 0: In Meta view, select rows with 100% null; otherwise select all-null columns
            // First pass: collect indices
            let selection: Option<(bool, Vec<usize>)> = app.current_view().map(|view| {
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
                    app.set_message("No all-null columns found".to_string());
                } else if let Some(view) = app.current_view_mut() {
                    let count = indices.len();
                    if is_rows {
                        for idx in indices {
                            view.selected_rows.insert(idx);
                        }
                        app.set_message(format!("Selected {} row(s) with 100% null", count));
                    } else {
                        for idx in indices {
                            view.selected_cols.insert(idx);
                        }
                        app.set_message(format!("Selected {} all-null column(s)", count));
                    }
                }
            }
        }
        KeyCode::Char('1') => {
            // 1: In Meta view, select rows with 1 distinct value; otherwise select single-value columns
            let selection: Option<(bool, Vec<usize>)> = app.current_view().map(|view| {
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
                    app.set_message("No single-value columns found".to_string());
                } else if let Some(view) = app.current_view_mut() {
                    let count = indices.len();
                    if is_rows {
                        for idx in indices {
                            view.selected_rows.insert(idx);
                        }
                        app.set_message(format!("Selected {} row(s) with 1 distinct value", count));
                    } else {
                        for idx in indices {
                            view.selected_cols.insert(idx);
                        }
                        app.set_message(format!("Selected {} single-value column(s)", count));
                    }
                }
            }
        }
        _ => {}
    }

    Ok(true)
}

/// Find a value in a column, returns row index
fn find_value(df: &polars::prelude::DataFrame, col_name: &str, value: &str, start: usize, forward: bool) -> Option<usize> {
    let col = df.column(col_name).ok()?;
    let len = col.len();

    // Helper to strip quotes from string values
    let strip_quotes = |s: String| -> String {
        if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
            s[1..s.len()-1].to_string()
        } else {
            s
        }
    };

    if forward {
        for i in start..len {
            if let Ok(v) = col.get(i) {
                if strip_quotes(v.to_string()) == value {
                    return Some(i);
                }
            }
        }
    } else {
        for i in (0..=start).rev() {
            if let Ok(v) = col.get(i) {
                if strip_quotes(v.to_string()) == value {
                    return Some(i);
                }
            }
        }
    }
    None
}

/// Find a regex match in a column, returns row index
fn find_regex(df: &polars::prelude::DataFrame, col_name: &str, re: &regex::Regex, start: usize, forward: bool) -> Option<usize> {
    let col = df.column(col_name).ok()?;
    let len = col.len();

    if forward {
        for i in start..len {
            if let Ok(v) = col.get(i) {
                let s = v.to_string();
                // Strip quotes for string values
                let text = if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                    &s[1..s.len()-1]
                } else {
                    &s
                };
                if re.is_match(text) {
                    return Some(i);
                }
            }
        }
    } else {
        for i in (0..=start).rev() {
            if let Ok(v) = col.get(i) {
                let s = v.to_string();
                let text = if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                    &s[1..s.len()-1]
                } else {
                    &s
                };
                if re.is_match(text) {
                    return Some(i);
                }
            }
        }
    }
    None
}

/// Filter DataFrame by regex match on a column
fn filter_by_regex(df: &polars::prelude::DataFrame, col_name: &str, re: &regex::Regex) -> anyhow::Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    let col = df.column(col_name)?;
    let len = col.len();

    let mut mask: Vec<bool> = Vec::with_capacity(len);
    for i in 0..len {
        let matches = col.get(i)
            .map(|v| {
                let s = v.to_string();
                let text = if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                    s[1..s.len()-1].to_string()
                } else {
                    s
                };
                re.is_match(&text)
            })
            .unwrap_or(false);
        mask.push(matches);
    }

    let bool_mask = BooleanChunked::from_slice("mask".into(), &mask);
    Ok(df.filter(&bool_mask)?)
}

/// Filter DataFrame by matching any of the given values in a column
fn filter_by_values(df: &polars::prelude::DataFrame, col_name: &str, values: &[String]) -> anyhow::Result<polars::prelude::DataFrame> {
    use polars::prelude::*;
    use std::collections::HashSet;

    let value_set: HashSet<&str> = values.iter().map(|s| s.as_str()).collect();
    let col = df.column(col_name)?;
    let len = col.len();

    let mut mask: Vec<bool> = Vec::with_capacity(len);
    for i in 0..len {
        let matches = col.get(i)
            .map(|v| {
                let s = v.to_string();
                let text = if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                    &s[1..s.len()-1]
                } else {
                    &s
                };
                value_set.contains(text)
            })
            .unwrap_or(false);
        mask.push(matches);
    }

    let bool_mask = BooleanChunked::from_slice("mask".into(), &mask);
    Ok(df.filter(&bool_mask)?)
}

/// Aggregate dataframe by column
fn aggregate_by(df: &polars::prelude::DataFrame, col_name: &str, agg_fn: &str) -> anyhow::Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    let lazy = df.clone().lazy();
    let grouped = lazy.group_by([col(col_name)]);

    let result = match agg_fn {
        "count" => grouped.agg([col("*").count().alias("count")]),
        "sum" => grouped.agg([col("*").sum()]),
        "mean" => grouped.agg([col("*").mean()]),
        "min" => grouped.agg([col("*").min()]),
        "max" => grouped.agg([col("*").max()]),
        "std" => grouped.agg([col("*").std(1)]),
        _ => return Err(anyhow::anyhow!("Unknown aggregation function")),
    };

    Ok(result.collect()?)
}

/// List directory contents as a DataFrame
fn list_directory(dir: &std::path::Path) -> anyhow::Result<polars::prelude::DataFrame> {
    use polars::prelude::*;
    use std::os::unix::fs::MetadataExt;

    let mut names: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    let mut modified: Vec<String> = Vec::new();
    let mut is_dir: Vec<bool> = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        let name = entry.file_name().to_string_lossy().to_string();

        names.push(name);
        sizes.push(meta.size());
        is_dir.push(meta.is_dir());

        // Format modification time
        let mtime = meta.mtime();
        let datetime = chrono::DateTime::from_timestamp(mtime, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_default();
        modified.push(datetime);
    }

    let df = DataFrame::new(vec![
        Series::new("name".into(), names).into(),
        Series::new("size".into(), sizes).into(),
        Series::new("modified".into(), modified).into(),
        Series::new("dir".into(), is_dir).into(),
    ])?;

    Ok(df)
}

/// Prompt user for input
/// Returns None if user cancels (Esc)
fn prompt_input(app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
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
