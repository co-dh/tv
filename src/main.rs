mod app;
mod backend;
mod command;
mod keymap;
mod os;
mod picker;
mod plugin;
mod prql;
mod render;
mod state;
mod theme;

use anyhow::Result;
use app::AppContext;
use backend::Backend;
use command::executor::CommandExecutor;
use command::io::{From, Save};
use command::nav::{Goto, GotoCol, ToggleInfo, Decimals, ToggleSel, ClearSel, SelAll, SelRows};
use command::transform::{Agg, Cast, DelCol, Derive, Filter, RenameCol, Select, Sort, Take, ToTime, Xkey};
use command::view::{Dup, Pop, Swap};
use plugin::corr::Correlation;
use plugin::freq::Frequency;
use plugin::meta::Metadata;
use plugin::folder::Ls;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::{cursor, execute, style::Print, terminal};
use render::Renderer;
use std::fs;
use std::io::{self, Write};

/// Entry point: parse args, run TUI or batch mode
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Parse flags first (before early returns)
    let raw_save = args.iter().any(|a| a == "--raw");

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
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --script <script_file>");
            std::process::exit(1);
        }
        return run_script(&args[idx + 1]);
    }

    // Check for --keys argument (key replay mode with immutable keymap)
    if let Some(idx) = args.iter().position(|a| a == "--keys") {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --keys 'F<ret>' file.parquet");
            std::process::exit(1);
        }
        let file = args.get(idx + 2).map(|s| s.as_str());
        return run_keys(&args[idx + 1], file);
    }

    // Initialize ratatui terminal
    let mut tui = render::init()?;

    // Get file path (first non-flag argument after program name)
    let file_arg = args.iter().skip(1).find(|a| !a.starts_with('-'));

    // Create app context
    let mut app = if let Some(path) = file_arg {
        // Load file from CLI argument
        let mut temp_app = AppContext::new();
        temp_app.raw_save = raw_save;
        match CommandExecutor::exec(&mut temp_app, Box::new(From { file_path: path.clone() })) {
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

        // Force full redraw if needed (after bat/less/fzf return)
        if app.needs_redraw {
            tui.clear()?;
            // Update viewport in case terminal size changed
            let size = tui.size()?;
            app.viewport(size.height, size.width);
            app.needs_redraw = false;
        }
        // Center cursor if needed (after search, with fresh viewport)
        if app.needs_center {
            if let Some(view) = app.view_mut() {
                view.state.center_if_needed();
            }
            app.needs_center = false;
        }
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

/// Run commands from iterator (pipe/batch mode)
fn run_batch<I: Iterator<Item = String>>(lines: I) -> Result<()> {
    let mut app = AppContext::new();
    app.viewport(50, 120);
    'outer: for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        for cmd_str in line.split('|').map(str::trim) {
            if cmd_str.is_empty() { continue; }
            if cmd_str == "quit" { break 'outer; }
            if let Some(cmd) = parse(cmd_str, &mut app) {
                if let Err(e) = CommandExecutor::exec(&mut app, cmd) {
                    eprintln!("Error executing '{}': {}", cmd_str, e);
                }
            } else {
                eprintln!("Unknown command: {}", cmd_str);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

/// Run inline commands (-c "from x | filter y")
fn run_commands(commands: &str) -> Result<()> {
    run_batch(std::iter::once(commands.to_string()))
}

/// Run script file (--script path)
fn run_script(script_path: &str) -> Result<()> {
    run_batch(fs::read_to_string(script_path)?.lines().map(String::from))
}

/// Parse Kakoune-style key sequence: "F<ret><down>" → ["F", "<ret>", "<down>"]
/// Any <...> is a special key. Use <lt>/<gt> for literal angle brackets.
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

/// Input mode for key player state machine
#[derive(Clone, PartialEq)]
enum InputMode { None, Search, Filter, Load, Save, Command, Goto, GotoCol, Select, Rename }

/// Run key replay mode (--keys "F<ret>" file) - state machine with text input
fn run_keys(keys: &str, file: Option<&str>) -> Result<()> {
    let mut app = AppContext::new();
    app.viewport(50, 120);
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
    }
    let mut mode = InputMode::None;
    let mut buf = String::new();

    for key in parse_keys(keys) {
        if mode != InputMode::None {
            // Text input mode - <lt> → <, <gt> → >, <space> → space
            if key == "<ret>" {
                exec_input(&mut app, &mode, &buf);
                mode = InputMode::None;
                buf.clear();
            } else if key == "<esc>" {
                mode = InputMode::None;
                buf.clear();
            } else if key == "<backspace>" {
                buf.pop();
            } else if key == "<lt>" {
                buf.push('<');
            } else if key == "<gt>" {
                buf.push('>');
            } else if key == "<space>" {
                buf.push(' ');
            } else if !key.starts_with('<') {
                buf.push_str(&key);
            }
        } else {
            // Normal mode - check for input-triggering keys
            let tab = cur_tab(&app);
            let cmd = app.keymap.get_command(tab, &key).map(|s| s.to_string());
            if let Some(cmd) = cmd {
                mode = match cmd.as_str() {
                    "search" => InputMode::Search,
                    "filter" => InputMode::Filter,
                    "from" => InputMode::Load,
                    "save" => InputMode::Save,
                    "command" => InputMode::Command,
                    "goto_row" => InputMode::Goto,
                    "goto_col" => InputMode::GotoCol,
                    "select_cols" => InputMode::Select,
                    "rename" => InputMode::Rename,
                    _ => { let _ = handle_cmd(&mut app, &cmd); InputMode::None }
                };
            } else {
                eprintln!("No binding for key '{}' in tab '{}'", key, tab);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

/// Execute input mode command with accumulated text
fn exec_input(app: &mut AppContext, mode: &InputMode, text: &str) {
    match mode {
        InputMode::Search => {
            app.search.col_name = None;
            app.search.value = Some(text.to_string());
            find_match(app, true);
        }
        InputMode::Filter => {
            run(app, Box::new(Filter { expr: text.to_string() }));
        }
        InputMode::Load => {
            run(app, Box::new(From { file_path: text.to_string() }));
        }
        InputMode::Save => {
            run(app, Box::new(Save { file_path: text.to_string() }));
        }
        InputMode::Command => {
            if let Some(cmd) = parse(text, app) {
                let _ = CommandExecutor::exec(app, cmd);
            }
        }
        InputMode::Goto => {
            run(app, Box::new(Goto { arg: text.to_string() }));
        }
        InputMode::GotoCol => {
            run(app, Box::new(GotoCol { arg: text.to_string() }));
        }
        InputMode::Select => {
            run(app, Box::new(Select { col_names: text.split(',').map(|s| s.trim().to_string()).collect() }));
        }
        InputMode::Rename => {
            let old = app.view().and_then(|v| v.col_name(v.state.cc));
            if let Some(old_name) = old {
                run(app, Box::new(RenameCol { old_name, new_name: text.to_string() }));
            }
        }
        InputMode::None => {}
    }
}


/// Block until background save completes
fn wait_bg_save(app: &mut AppContext) {
    if let Some(rx) = app.bg_saver.take() {
        for msg in rx { eprintln!("{}", msg); }
    }
}

/// Block until background meta completes
fn wait_bg_meta(app: &mut AppContext) {
    if let Some((pid, rx)) = app.bg_meta.take() {
        if let Ok(df) = rx.recv() {
            if let Some(v) = app.view_mut() {
                if v.name == "metadata" && v.parent_id == Some(pid) { v.dataframe = df; }
            }
        }
    }
}

/// Fetch visible rows for lazy parquet view (simulates render)
fn fetch_lazy(view: &mut state::ViewState) {
    if let Some(ref path) = view.parquet_path {
        let df = if let Some(ref w) = view.filter_clause {
            backend::Polars.fetch_where(path, w, 0, 50)
        } else {
            backend::Polars.fetch_rows(path, 0, 50)
        };
        if let Ok(df) = df { view.dataframe = df; }
    }
}

/// Print current view to stdout (batch mode)
fn print(app: &mut AppContext) {
    if let Some(view) = app.view_mut() {
        println!("=== {} ({} rows) ===", view.name, view.rows());
        fetch_lazy(view);
        println!("{}", view.dataframe);
    } else {
        println!("No table loaded");
    }
}

/// Get process memory usage in MB from /proc/self/status
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

/// Print status line info (for key testing) - fetches lazy data first
fn print_status(app: &mut AppContext) {
    if let Some(view) = app.view_mut() {
        fetch_lazy(view);  // simulate render fetch
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let disk = view.disk_rows.map(|n| n.to_string()).unwrap_or("-".into());
        let df = view.dataframe.height();
        println!("STATUS: view={} rows={} disk={} df={} col={} col_name={} mem={}MB",
            view.name, view.rows(), disk, df, view.state.cc, col_name, mem_mb());
    }
}

/// Parse command string into Command object
fn parse(line: &str, app: &mut AppContext) -> Option<Box<dyn command::Command>> {
    let parts: Vec<&str> = line.splitn(2, ' ').collect();
    let cmd = parts[0].to_lowercase();
    let arg = parts.get(1).map(|s| s.trim()).unwrap_or("");

    // Core commands (not in plugins)
    match cmd.as_str() {
        "load" | "from" => return Some(Box::new(From { file_path: arg.to_string() })),
        "save" => return Some(Box::new(Save { file_path: arg.to_string() })),
        "corr" | "correlation" => return Some(Box::new(Correlation { selected_cols: vec![] })),
        "del_col" | "delcol" => return Some(Box::new(DelCol { col_names: arg.split(',').map(|s| s.trim().to_string()).collect() })),
        "filter" => return Some(Box::new(Filter { expr: arg.to_string() })),
        "select" | "sel" => return Some(Box::new(Select {
            col_names: arg.split(',').map(|s| s.trim().to_string()).collect()
        })),
        "sort" => {
            let (col, desc) = prql::parse_sort(arg);
            return Some(Box::new(Sort { col_name: col, descending: desc }));
        }
        "sort_desc" | "sortdesc" => return Some(Box::new(Sort { col_name: arg.to_string(), descending: true })),
        "take" => return arg.parse().ok().map(|n| Box::new(Take { n }) as Box<dyn command::Command>),
        "to_time" => return Some(Box::new(ToTime { col_name: arg.to_string() })),
        "xkey" => return Some(Box::new(Xkey { col_names: arg.split(',').map(|s| s.trim().to_string()).collect() })),
        "rename" => {
            let rename_parts: Vec<&str> = arg.splitn(2, ' ').collect();
            if rename_parts.len() == 2 {
                return Some(Box::new(RenameCol {
                    old_name: rename_parts[0].to_string(),
                    new_name: rename_parts[1].to_string(),
                }));
            }
            return None;
        }
        "goto" => return Some(Box::new(Goto { arg: arg.to_string() })),
        "goto_col" | "gotocol" => return Some(Box::new(GotoCol { arg: arg.to_string() })),
        "toggle_info" => return Some(Box::new(ToggleInfo)),
        "decimals" => return arg.parse().ok().map(|d| Box::new(Decimals { delta: d }) as Box<dyn command::Command>),
        "toggle_sel" => return Some(Box::new(ToggleSel)),
        "clear_sel" => return Some(Box::new(ClearSel)),
        "sel_all" => return Some(Box::new(SelAll)),
        "sel_rows" => return Some(Box::new(SelRows { expr: arg.to_string() })),
        "pop" => return Some(Box::new(Pop)),
        _ => {}
    }

    // Try plugin commands (parse method)
    if let Some(c) = app.plugins.parse(&cmd, arg) { return Some(c); }

    // Try plugin handle for context-dependent commands (enter, delete_sel, etc.)
    if let Some(name) = app.view().map(|v| v.name.clone()) {
        let plugins = std::mem::take(&mut app.plugins);
        let result = plugins.handle(&name, &cmd, app);
        app.plugins = plugins;
        if result.is_some() { return result; }
    }
    None
}

/// Execute command on current column
fn on_col<F>(app: &mut AppContext, f: F) where F: FnOnce(String) -> Box<dyn command::Command> {
    if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
        if let Err(e) = CommandExecutor::exec(app, f(col)) { app.err(e); }
    }
}

/// Dispatch action to view-specific plugin handler
fn dispatch(app: &mut AppContext, action: &str) -> bool {
    let name = match app.view() { Some(v) => v.name.clone(), None => return false };
    // mem::take to avoid borrow conflict: plugins.handle needs &mut app
    let plugins = std::mem::take(&mut app.plugins);
    let cmd = plugins.handle(&name, action, app);
    app.plugins = plugins;
    if let Some(cmd) = cmd { run(app, cmd); true } else { false }
}

/// Execute command through executor with error handling
fn run(app: &mut AppContext, cmd: Box<dyn command::Command>) {
    if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
}

/// Navigate to next/prev search match
fn find_match(app: &mut AppContext, forward: bool) {
    if let Some(expr) = app.search.value.clone() {
        if let Some(view) = app.view_mut() {
            let m = find(&view.dataframe, &expr);
            let cur = view.state.cr;
            let pos = if forward { m.iter().find(|&&i| i > cur) } else { m.iter().rev().find(|&&i| i < cur) };
            if let Some(&p) = pos { view.state.cr = p; view.state.visible(); }
            else { app.msg("No more matches".into()); }
        }
    } else { app.msg("No search active".into()); }
}

/// Convert KeyEvent to Kakoune-style key name for keymap lookup
fn key_str(key: &KeyEvent) -> String {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) if ctrl => format!("<c-{}>", c.to_ascii_lowercase()),
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
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

/// Process key event, return false to quit
fn on_key(app: &mut AppContext, key: KeyEvent) -> Result<bool> {
    // Look up command from keymap
    let ks = key_str(&key);
    let tab = cur_tab(app);
    if let Some(cmd) = app.keymap.get_command(tab, &ks).map(|s| s.to_string()) {
        return handle_cmd(app, &cmd);
    }
    // Fallback for unmapped keys
    Ok(true)
}

/// Handle keymap command, return false to quit
fn handle_cmd(app: &mut AppContext, cmd: &str) -> Result<bool> {
    match cmd {
        // Exit commands
        "quit" => {
            if !app.has_view() || app.stack.len() == 1 { return Ok(false); }
            run(app, Box::new(Pop));
        }
        "force_quit" => return Ok(false),
        "print_status" => { print_status(app); return Ok(false); }
        // Navigation
        "up" => run(app, Box::new(Goto { arg: "-1".into() })),
        "down" => run(app, Box::new(Goto { arg: "+1".into() })),
        "left" => run(app, Box::new(GotoCol { arg: "-1".into() })),
        "right" => run(app, Box::new(GotoCol { arg: "+1".into() })),
        "page_down" => run(app, Box::new(Goto { arg: app.page().to_string() })),
        "page_up" => run(app, Box::new(Goto { arg: (-app.page()).to_string() })),
        "top" => run(app, Box::new(Goto { arg: "0".into() })),
        "bottom" => run(app, Box::new(Goto { arg: "max".into() })),
        // Display
        "toggle_info" => run(app, Box::new(ToggleInfo)),
        "decimals_inc" => run(app, Box::new(Decimals { delta: 1 })),
        "decimals_dec" => run(app, Box::new(Decimals { delta: -1 })),
        // Selection
        "toggle_sel" => run(app, Box::new(ToggleSel)),
        "clear_sel" => run(app, Box::new(ClearSel)),
        "sel_all" => run(app, Box::new(SelAll)),
        "select_cols" => {
            if !app.has_view() { app.no_table(); }
            else if let Some(cols) = prompt(app, "Select columns: ")? {
                run(app, Box::new(Select { col_names: cols.split(',').map(|s| s.trim().to_string()).collect() }));
            }
        }
        // File I/O
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
        // Search
        "search" => do_search(app)?,
        "filter" => do_filter(app)?,
        "next_match" => find_match(app, true),
        "prev_match" => find_match(app, false),
        "search_cell" => {
            if let Some(view) = app.view() {
                if let Some(col_name) = view.col_name(view.state.cc) {
                    if let Some(col) = view.dataframe.get_columns().get(view.state.cc) {
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
        }
        // Column operations
        "freq" => on_col(app, |c| Box::new(Frequency { col_name: c })),
        "meta" => if app.has_view() { run(app, Box::new(Metadata)); },
        "corr" => {
            if app.has_view() {
                run(app, Box::new(Correlation {
                    selected_cols: app.view().map(|v| v.selected_cols.iter().copied().collect()).unwrap_or_default()
                }));
                if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
            }
        }
        "sort" => on_col(app, |c| Box::new(Sort { col_name: c, descending: false })),
        "sort-" => on_col(app, |c| Box::new(Sort { col_name: c, descending: true })),
        "rename" => {
            if let Some(old_name) = app.view().and_then(|v| v.col_name(v.state.cc)) {
                if let Some(new_name) = prompt(app, &format!("Rename '{}' to: ", old_name))? {
                    run(app, Box::new(RenameCol { old_name, new_name }));
                }
            }
        }
        "derive" => on_col(app, |col| Box::new(Derive { col_name: col })),
        "convert" => do_convert(app)?,
        "aggregate" => {
            if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
                let result = picker::fzf(vec!["count".into(), "sum".into(), "mean".into(), "min".into(), "max".into(), "std".into()], "Aggregate: ");
                app.needs_redraw = true;
                if let Ok(Some(func)) = result { run(app, Box::new(Agg { col, func })); }
            }
        }
        "delete" => {
            // Dispatch to plugin first, fallback to table column delete
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
        // View management
        "dup" => if app.has_view() { run(app, Box::new(Dup)); },
        "swap" => run(app, Box::new(Swap)),
        "ls" => run(app, Box::new(Ls { dir: std::env::current_dir().unwrap_or_default(), recursive: false })),
        "lr" => run(app, Box::new(Ls { dir: std::env::current_dir().unwrap_or_default(), recursive: true })),
        // UI
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
            if app.bookmarks.is_empty() { app.msg("No bookmarks".into()); }
            else if let Some(row) = app.bookmarks.iter()
                .find(|&&r| r > app.view().map(|v| v.state.cr).unwrap_or(0)).copied()
                .or_else(|| app.bookmarks.first().copied())
            {
                if let Some(v) = app.view_mut() { v.state.cr = row; v.state.visible(); }
                app.msg(format!("Bookmark: row {}", row));
            }
        }
        // Plugin dispatch (Enter, etc.)
        "enter" | "filter_parent" | "delete_sel" => { dispatch(app, cmd); }
        // Meta view: select null/single-value columns
        "sel_null" => { run(app, Box::new(SelRows { expr: "`null%` == '100.0'".into() })); }
        "sel_single" => { run(app, Box::new(SelRows { expr: "distinct == '1'".into() })); }
        _ => {}
    }
    Ok(true)
}

/// Search with fzf (/)
fn do_search(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, v.name.starts_with("ls")))
    });
    if let Some((hint_list, col_name, is_folder)) = info {
        let expr_opt = picker::fzf_edit(hint_list, "Search> ");
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            let prql_mode = theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false);
            let expr = if !prql_mode && is_plain_value(&expr) {
                format!("{} LIKE '%{}%'", col_name, expr)
            } else { expr };
            let matches = app.view().map(|v| find(&v.dataframe, &expr)).unwrap_or_default();
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

/// Filter with fzf (\)
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.dataframe.column(&col_name).ok()
            .map(|c| matches!(c.dtype(), polars::prelude::DataType::String)).unwrap_or(false);
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, is_str))
    });
    if let Some((hint_list, col_name, is_str)) = info {
        let expr_opt = picker::fzf_edit(hint_list, "WHERE> ");
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            // If plain value selected, construct equality expression (quote column for reserved words)
            let expr = if is_plain_value(&expr) {
                if is_str { format!("\"{}\" = '{}'", col_name, expr) }
                else { format!("\"{}\" = {}", col_name, expr) }
            } else { expr };
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

/// Type conversion ($)
fn do_convert(app: &mut AppContext) -> Result<()> {
    let col_name = app.view().and_then(|v| v.col_name(v.state.cc));
    if let Some(col_name) = col_name {
        let types = vec!["String".into(), "Int64".into(), "Float64".into(), "Boolean".into()];
        let result = picker::fzf(types, "Convert to: ");
        app.needs_redraw = true;
        if let Ok(Some(dtype)) = result {
            run(app, Box::new(Cast { col_name, dtype }));
        }
    }
    Ok(())
}

/// Command picker (:)
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "df", "mounts", "tcp", "udp", "lsblk", "who", "lsof [pid]", "env",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf_edit(cmd_list, ": ");
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
fn do_goto_col(app: &mut AppContext) -> Result<()> {
    if let Some(view) = app.view() {
        let col_names: Vec<String> = view.dataframe.get_column_names().iter().map(|s| s.to_string()).collect();
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

/// Strip quotes from polars string values
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

/// Generate filter hints: distinct values first, PRQL hints if cfg prql_hints=true
fn hints(df: &polars::prelude::DataFrame, col_name: &str, row: usize, file: Option<&str>) -> Vec<String> {
    use polars::prelude::DataType;
    let mut items = Vec::new();
    let Ok(col) = df.column(col_name) else { return items };
    let dtype = col.dtype();
    let is_str = matches!(dtype, DataType::String);
    let is_datetime = matches!(dtype, DataType::Date | DataType::Datetime(_, _) | DataType::Time);

    // Distinct values: from disk for parquet, else from memory
    if let Some(path) = file.filter(|f| f.ends_with(".parquet")) {
        if let Ok(vals) = backend::Polars.distinct(path, col_name) {
            items.extend(vals.into_iter().map(|v| unquote(&v)).filter(|v| v != "null"));
        }
    } else if let Ok(uniq) = col.unique() {
        for i in 0..uniq.len() {
            if let Ok(v) = uniq.get(i) {
                let val = unquote(&v.to_string());
                if val == "null" { continue; }
                items.push(val);
            }
        }
    }

    // Sort distinct values
    items.sort();

    // PRQL hints only if enabled in cfg/config.csv
    if theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false) {
        items.extend(prql_hints(col, col_name, row, is_str, is_datetime));
    }
    items
}

/// Generate PRQL filter hints (text patterns, date ranges, comparisons)
fn prql_hints(col: &polars::prelude::Column, col_name: &str, row: usize, is_str: bool, is_datetime: bool) -> Vec<String> {
    let mut items = Vec::new();
    if is_str {
        if let Ok(val) = col.get(row) {
            let v = unquote(&val.to_string());
            if v.len() >= 2 {
                items.push(format!("({} | text.starts_with '{}')", col_name, &v[..2]));
                items.push(format!("({} | text.ends_with '{}')", col_name, &v[v.len()-2..]));
                items.push(format!("({} | text.contains '{}')", col_name, &v[..v.len().min(4)]));
            }
        }
    } else if is_datetime {
        if let Ok(val) = col.get(row) {
            let v = val.to_string();
            if let Some((year, rest)) = v.split_once('-') {
                if let Ok(y) = year.parse::<i32>() {
                    items.push(format!("{} >= @{}-01-01 && {} < @{}-01-01", col_name, y, col_name, y + 1));
                    if let Some((month, _)) = rest.split_once('-') {
                        if let Ok(m) = month.parse::<u32>() {
                            let (ny, nm) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
                            items.push(format!("{} >= @{}-{:02}-01 && {} < @{}-{:02}-01", col_name, y, m, col_name, ny, nm));
                        }
                    }
                }
            }
        }
    } else if let Ok(val) = col.get(row) {
        let v = val.to_string();
        if v != "null" {
            items.push(format!("{} > {}", col_name, v));
            items.push(format!("{} < {}", col_name, v));
            items.push(format!("{} >= {} && {} <= {}", col_name, v, col_name, v));
        }
    }
    items
}

/// Check if expression is a plain value (no operators)
fn is_plain_value(expr: &str) -> bool {
    let e = expr.trim();
    !e.contains('=') && !e.contains('>') && !e.contains('<') && !e.contains('~')
        && !e.contains("&&") && !e.contains("||") && !e.contains(" AND ")
        && !e.contains(" OR ") && !e.contains(" LIKE ")
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

/// Prompt user for input, None if cancelled
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
        let h = hints(&df, "name", 0, None);
        // Default: distinct values only (no PRQL hints unless cfg enabled)
        assert!(h.iter().any(|s| s == "apple"), "Should have distinct value apple");
        assert!(h.iter().any(|s| s == "banana"), "Should have distinct value banana");
    }

    #[test]
    fn test_hints_datetime() {
        let dates = ["2025-01-15", "2025-02-20"];
        let df = df! {
            "dt" => dates.iter().map(|s| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
            }).collect::<Vec<_>>()
        }.unwrap();
        let h = hints(&df, "dt", 0, None);
        // Default: distinct date values
        assert!(h.iter().any(|s| s.contains("2025-01-15")), "Should have distinct date value");
    }

    #[test]
    fn test_hints_numeric() {
        let df = df! { "val" => &[1, 2, 3] }.unwrap();
        let h = hints(&df, "val", 0, None);
        // Default: distinct values
        assert!(h.iter().any(|s| s == "1"), "Should have distinct value 1");
        assert!(h.iter().any(|s| s == "2"), "Should have distinct value 2");
    }

    #[test]
    fn test_hints_no_limit() {
        let df = df! { "val" => (0..600).collect::<Vec<i32>>() }.unwrap();
        let h = hints(&df, "val", 0, None);
        // All distinct values returned
        assert_eq!(h.len(), 600, "All distinct values should be returned");
    }
}
