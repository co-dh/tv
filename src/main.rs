mod app;
mod command;
mod keymap;
mod os;
mod picker;
mod prql;
mod render;
mod state;
mod theme;
mod view;

use anyhow::Result;
use app::AppContext;
use command::executor::CommandExecutor;
use command::io::{Load, Save};
use command::transform::{Agg, DelCol, Filter, RenameCol, Select, Sort, Take, Xkey};
use command::view::{Correlation, Df, Dup, Env, Frequency, Lr, Ls, Lsblk, Lsof, Metadata, Mounts, Pop, Ps, Swap, Tcp, Udp, Who};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::{cursor, execute, style::Print, terminal};
use render::Renderer;
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

    // Check for --raw flag (skip type detection on save)
    let raw_save = args.iter().any(|a| a == "--raw");

    // Initialize ratatui terminal
    let mut tui = render::init()?;

    // Get file path (first non-flag argument after program name)
    let file_arg = args.iter().skip(1).find(|a| !a.starts_with('-'));

    // Create app context
    let mut app = if let Some(path) = file_arg {
        // Load file from CLI argument
        let mut temp_app = AppContext::new();
        temp_app.raw_save = raw_save;
        match CommandExecutor::exec(&mut temp_app, Box::new(Load { file_path: path.clone() })) {
            Ok(_) => temp_app,
            Err(e) => {
                render::restore()?;
                eprintln!("Error loading file: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        let mut temp_app = AppContext::new();
        temp_app.raw_save = raw_save;
        temp_app
    };

    // Update viewport
    let size = tui.size()?;
    app.viewport(size.height, size.width);

    // Main event loop
    loop {
        // Check background tasks
        app.merge_bg_data();
        app.check_bg_saver();
        app.check_bg_meta();

        // Render with ratatui diff-based update
        tui.draw(|frame| Renderer::render(frame, &mut app))?;

        // Poll for events with timeout (allows background data merge)
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if !on_key(&mut app, key)? {
                    break;
                }
            }
        }
    }

    render::restore()?;
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
        "load" | "from" => Some(Box::new(Load { file_path: arg.to_string() })),
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
        "sort" => {
            // PRQL style: "sort col" (asc) or "sort -col" (desc)
            let (col, desc) = prql::parse_sort(arg);
            Some(Box::new(Sort { col_name: col, descending: desc }))
        }
        "sortdesc" => Some(Box::new(Sort { col_name: arg.to_string(), descending: true })),
        "take" => arg.parse().ok().map(|n| Box::new(Take { n }) as Box<dyn command::Command>),
        "xkey" => Some(Box::new(Xkey { col_names: arg.split(',').map(|s| s.trim().to_string()).collect() })),
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
                let path = std::path::Path::new(&file_path);
                // Check if parent directory exists
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        let msg = format!("Create dir '{}'? (y/n): ", parent.display());
                        if let Some(ans) = prompt(app, &msg)? {
                            if ans.to_lowercase() == "y" {
                                if let Err(e) = std::fs::create_dir_all(parent) {
                                    app.err(anyhow::anyhow!("Failed to create dir: {}", e));
                                    return Ok(true);
                                }
                            } else {
                                return Ok(true);  // User declined
                            }
                        }
                    }
                }
                if let Err(e) = CommandExecutor::exec(app, Box::new(Save { file_path })) {
                    app.err(e);
                }
            }
        }
        KeyCode::Char('D') => {
            // D: Delete - dispatch to view-specific handler, fallback to table delete
            let kind = app.view().map(|v| view::ViewKind::from_name(&v.name));
            if let Some(k) = kind {
                if let Some(cmd) = view::handler::dispatch(k, "delete", app) {
                    let _ = CommandExecutor::exec(app, cmd);
                } else {
                    // Default table delete
                    let col_names: Vec<String> = app.view().map(|view| {
                        if view.selected_cols.is_empty() {
                            view.state.cur_col(&view.dataframe).map(|c| vec![c]).unwrap_or_default()
                        } else {
                            let names: Vec<String> = view.dataframe.get_column_names().iter().map(|s| s.to_string()).collect();
                            let mut sel: Vec<usize> = view.selected_cols.iter().copied().collect();
                            sel.sort_by(|a, b| b.cmp(a));
                            sel.iter().filter_map(|&i| names.get(i).cloned()).collect()
                        }
                    }).unwrap_or_default();
                    if !col_names.is_empty() {
                        let _ = CommandExecutor::exec(app, Box::new(DelCol { col_names }));
                        if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
                    }
                }
            }
        }
        KeyCode::Char('/') => {
            // /: Search with SQL WHERE expression
            if let Some(view) = app.view() {
                if let Some(col_name) = view.state.cur_col(&view.dataframe) {
                    let expr_opt = picker::fzf_edit(
                        hints(&view.dataframe, &col_name, view.state.cr), "Search> ");

                    if let Ok(Some(expr)) = expr_opt {
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
                    let expr_opt = picker::fzf_edit(
                        hints(&view.dataframe, &col_name, view.state.cr), "WHERE> ");

                    if let Ok(Some(expr)) = expr_opt {
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
            // Dispatch Enter to view-specific handler
            let kind = app.view().map(|v| view::ViewKind::from_name(&v.name));
            if let Some(k) = kind {
                if let Some(cmd) = view::handler::dispatch(k, "enter", app) {
                    let _ = CommandExecutor::exec(app, cmd);
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
                    if let Ok(Some(selected)) = picker::fzf(types, "Convert to: ") {
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
                    if let Ok(Some(func)) = picker::fzf(vec![
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
            // :: Command picker
            let cmd_list: Vec<String> = vec![
                "from <file>", "save <file>",
                "ls [dir]", "lr [dir]",
                "ps", "df", "mounts", "tcp", "udp", "lsblk", "who", "lsof [pid]", "env",
                "filter <expr>", "freq <col>", "meta", "corr",
                "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
            ].iter().map(|s| s.to_string()).collect();
            if let Ok(Some(selected)) = picker::fzf_edit(cmd_list, ": ") {
                let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
                if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
                    if let Err(e) = CommandExecutor::exec(app, cmd) {
                        app.err(e);
                    }
                } else {
                    app.msg(format!("Unknown command: {}", selected));
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
                if let Ok(Some(selected)) = picker::fzf(col_names.clone(), "Column: ") {
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

///// Get SQL hints for search/filter: LIKE patterns (%) + unique values with quotes
/// Generate PRQL filter hints for fzf picker
fn hints(df: &polars::prelude::DataFrame, col_name: &str, row: usize) -> Vec<String> {
    use polars::prelude::DataType;
    let mut items = Vec::new();
    if let Ok(col) = df.column(col_name) {
        let dtype = col.dtype();
        let is_str = matches!(dtype, DataType::String);
        let is_datetime = matches!(dtype, DataType::Date | DataType::Datetime(_, _) | DataType::Time);

        if is_str {
            // PRQL text functions: starts_with, ends_with, contains
            if let Ok(val) = col.get(row) {
                let v = unquote(&val.to_string());
                if v.len() >= 2 {
                    items.push(format!("({} | text.starts_with '{}')", col_name, &v[..2]));
                    items.push(format!("({} | text.ends_with '{}')", col_name, &v[v.len()-2..]));
                    items.push(format!("({} | text.contains '{}')", col_name, &v[..v.len().min(4)]));
                }
            }
        } else if is_datetime {
            // PRQL date: @2008-01-10 syntax
            if let Ok(val) = col.get(row) {
                let v = val.to_string();
                if let Some((year, rest)) = v.split_once('-') {
                    if let Ok(y) = year.parse::<i32>() {
                        // Year range
                        items.push(format!("{} >= @{}-01-01 && {} < @{}-01-01", col_name, y, col_name, y + 1));
                        // Month range
                        if let Some((month, _)) = rest.split_once('-') {
                            if let Ok(m) = month.parse::<u32>() {
                                let (next_y, next_m) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
                                items.push(format!("{} >= @{}-{:02}-01 && {} < @{}-{:02}-01",
                                    col_name, y, m, col_name, next_y, next_m));
                            }
                        }
                    }
                }
            }
        } else {
            // Numeric: comparison hints
            if let Ok(val) = col.get(row) {
                let v = val.to_string();
                if v != "null" {
                    items.push(format!("{} > {}", col_name, v));
                    items.push(format!("{} < {}", col_name, v));
                    items.push(format!("{} >= {}", col_name, v));
                    items.push(format!("{} <= {}", col_name, v));
                    items.push(format!("{} >= {} && {} <= {}", col_name, v, col_name, v));  // between
                }
            }
        }

        // Unique values as exact match hints
        if let Ok(uniq) = col.unique() {
            for i in 0..uniq.len().min(10) {
                if let Ok(v) = uniq.get(i) {
                    let val = unquote(&v.to_string());
                    if val == "null" { continue; }
                    if is_str {
                        // SQL = for strings (passed through to polars)
                        items.push(format!("{} = '{}'", col_name, val));
                    } else if is_datetime {
                        // Format as @YYYY-MM-DD
                        items.push(format!("{} == @{}", col_name, val));
                    } else {
                        items.push(format!("{} == {}", col_name, val));
                    }
                }
            }
        }
    }
    items
}

/// Find rows matching PRQL filter expression, returns row indices
fn find(df: &polars::prelude::DataFrame, expr: &str) -> Vec<usize> {
    use polars::prelude::*;
    // Compile PRQL filter to SQL WHERE clause
    let where_clause = match prql::filter_to_sql(expr) {
        Ok(w) => w,
        Err(_) => return vec![],
    };
    let mut ctx = polars::sql::SQLContext::new();
    let with_idx = df.clone().lazy().with_row_index("__idx__", None);
    ctx.register("df", with_idx);
    ctx.execute(&format!("SELECT __idx__ FROM df WHERE {}", where_clause))
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
fn prompt(_app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
    // Show prompt at bottom (screen already rendered by main loop)
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
        // SQL = for string equality (passed through)
        assert_eq!(find(&df, "name = 'banana'"), vec![1]);
        assert_eq!(find(&df, "name = 'notfound'"), Vec::<usize>::new());
    }

    #[test]
    fn test_find_like() {
        let df = make_test_df();
        // SQL LIKE patterns (passed through)
        assert_eq!(find(&df, "name LIKE 'b%'"), vec![1, 5]);  // banana, blueberry
        assert_eq!(find(&df, "name LIKE '%rry'"), vec![2, 5]);  // cherry, blueberry
        assert_eq!(find(&df, "name LIKE '%apple%'"), vec![0, 3]);  // apple, pineapple
    }

    #[test]
    fn test_find_prql_text() {
        let df = make_test_df();
        // PRQL text functions compiled to SQL LIKE
        assert_eq!(find(&df, "(name | text.starts_with 'b')"), vec![1, 5]);  // banana, blueberry
        assert_eq!(find(&df, "(name | text.ends_with 'rry')"), vec![2, 5]);  // cherry, blueberry
        assert_eq!(find(&df, "(name | text.contains 'apple')"), vec![0, 3]);  // apple, pineapple
    }

    #[test]
    fn test_hints_string() {
        let df = df! { "name" => &["apple", "banana"] }.unwrap();
        let h = hints(&df, "name", 0);
        // PRQL text function hints
        assert!(h.iter().any(|s| s.contains("text.starts_with")), "Should have starts_with hint");
        assert!(h.iter().any(|s| s.contains("text.ends_with")), "Should have ends_with hint");
        assert!(h.iter().any(|s| s.contains("text.contains")), "Should have contains hint");
        assert!(h.iter().any(|s| s.contains("= 'apple'")), "Should have exact match");
    }

    #[test]
    fn test_hints_datetime_prql() {
        let dates = ["2025-01-15", "2025-02-20"];
        let df = df! {
            "dt" => dates.iter().map(|s| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
            }).collect::<Vec<_>>()
        }.unwrap();

        let h = hints(&df, "dt", 0);

        // PRQL date hints use @ prefix and &&
        assert!(h.iter().any(|s| s.contains("@2025-01-01") && s.contains("@2026-01-01")),
            "Should have PRQL year range hint with @");
        assert!(h.iter().any(|s| s.contains("&&")), "Should use && for AND");
        // PRQL exact match: == @YYYY-MM-DD
        assert!(h.iter().any(|s| s.contains("== @2025-01-15")),
            "Should have PRQL datetime exact match");
    }

    #[test]
    fn test_hints_numeric() {
        let df = df! { "val" => &[1, 2, 3] }.unwrap();
        let h = hints(&df, "val", 0);
        // PRQL uses == for equality
        assert!(h.iter().any(|s| s.contains("== 1") && !s.contains("'")),
            "Numeric hints should use == without quotes");
    }

    #[test]
    fn test_hints_numeric_comparisons() {
        let df = df! { "val" => &[10, 20, 30] }.unwrap();
        let h = hints(&df, "val", 1);  // row 1 has value 20
        assert!(h.iter().any(|s| s == "val > 20"), "Should have > hint");
        assert!(h.iter().any(|s| s == "val < 20"), "Should have < hint");
        assert!(h.iter().any(|s| s == "val >= 20"), "Should have >= hint");
        assert!(h.iter().any(|s| s == "val <= 20"), "Should have <= hint");
        // PRQL uses && instead of BETWEEN
        assert!(h.iter().any(|s| s == "val >= 20 && val <= 20"), "Should have range hint with &&");
    }
}
