mod app;
mod dynload;
mod error;
mod command;
mod keyhandler;
mod keymap;
mod picker;
mod plugin;
mod pure;
mod render;
mod state;
mod table;
mod theme;
mod utils;

use anyhow::Result;
use app::AppContext;
use command::executor::CommandExecutor;
use command::io::{From, Save};
use command::nav::{Goto, GotoCol, ToggleInfo, Decimals, ToggleSel, ClearSel, SelAll, SelRows};
use command::transform::{Agg, DelCol, Derive, Filter, RenameCol, Select, Sort, Take, ToTime, Xkey};
use command::view::{Dup, Pop, Swap};
use plugin::corr::Correlation;
use plugin::meta::Metadata;
use plugin::folder::Ls;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::crossterm::{cursor, execute, style::Print, terminal};
use std::fs;
use std::io::{self, Write};

/// Try to load plugins from standard locations
fn load_plugin() {
    let exe = std::env::current_exe().ok();
    let exe_dir = exe.as_ref().and_then(|p| p.parent());
    let home_lib = dirs::home_dir().map(|h| h.join(".local/lib/tv"));

    // Load polars plugin (for file paths)
    for p in [
        exe_dir.map(|d| d.join("libtv_polars.so")),
        exe_dir.map(|d| d.join("plugins/libtv_polars.so")),
        home_lib.as_ref().map(|d| d.join("libtv_polars.so")),
    ].into_iter().flatten() {
        if p.exists() {
            if let Err(e) = dynload::load_polars(p.to_str().unwrap_or("")) {
                eprintln!("Warning: failed to load polars plugin: {}", e);
            }
            break;
        }
    }

    // Load sqlite plugin (for memory:id paths)
    for p in [
        exe_dir.map(|d| d.join("libtv_sqlite.so")),
        exe_dir.map(|d| d.join("plugins/libtv_sqlite.so")),
        home_lib.as_ref().map(|d| d.join("libtv_sqlite.so")),
    ].into_iter().flatten() {
        if p.exists() {
            if let Err(e) = dynload::load_sqlite(p.to_str().unwrap_or("")) {
                eprintln!("Warning: failed to load sqlite plugin: {}", e);
            }
            break;
        }
    }
}

/// Entry point: parse args, run TUI or batch mode
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Try to load polars plugin (optional - falls back to built-in)
    load_plugin();

    // Parse flags first (before early returns)
    let raw_save = args.iter().any(|a| a == "--raw");

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

    // Check for -c argument (inline commands)
    if let Some(idx) = args.iter().position(|a| a == "-c") {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv -c 'from data.csv filter x > 5'");
            std::process::exit(1);
        }
        let file = args.get(idx + 2).map(|s| s.as_str());
        return run_cmds(&args[idx + 1], file);
    }

    // Initialize ratatui terminal
    let mut tui = render::init()?;

    // Get file path (first non-flag argument after program name)
    let file_arg = args.iter().skip(1).find(|a| !a.starts_with('-'));

    // Create app context
    let mut app = if let Some(path) = file_arg {
        // Load file from CLI argument
        let mut temp_app = AppContext::default();
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
        let mut temp_app = AppContext::default();
        temp_app.raw_save = raw_save;
        temp_app
    };

    // Elm Architecture: run loop with on_key handler
    app.run(&mut tui, on_key)?;

    render::restore()?;
    Ok(())
}

/// Run commands from iterator (pipe/batch mode)
fn run_batch<I: Iterator<Item = String>>(lines: I) -> Result<()> {
    let mut app = AppContext::default();
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

/// Run script file (--script path)
fn run_script(script_path: &str) -> Result<()> {
    run_batch(fs::read_to_string(script_path)?.lines().map(String::from))
}

/// Run inline commands (-c "cmd1 cmd2")
fn run_cmds(cmds: &str, file: Option<&str>) -> Result<()> {
    let mut app = AppContext::default();
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
    }
    app.viewport(50, 120);
    // Split by newline or space (respecting quoted strings)
    for cmd_str in split_cmds(cmds) {
        if cmd_str.is_empty() || cmd_str.starts_with('#') { continue; }
        if cmd_str == "quit" { break; }
        if let Some(cmd) = parse(&cmd_str, &mut app) {
            if let Err(e) = CommandExecutor::exec(&mut app, cmd) {
                eprintln!("Error: {}", e);
            }
        } else {
            eprintln!("Unknown: {}", cmd_str);
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

/// Split command string by newline or space, respecting quoted args
fn split_cmds(s: &str) -> Vec<String> {
    let mut cmds = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    let mut quote_char = ' ';
    for c in s.chars() {
        if in_quote {
            cur.push(c);
            if c == quote_char { in_quote = false; }
        } else if c == '\'' || c == '"' {
            cur.push(c);
            in_quote = true;
            quote_char = c;
        } else if c == '\n' {
            if !cur.trim().is_empty() { cmds.push(cur.trim().to_string()); }
            cur.clear();
        } else {
            cur.push(c);
        }
    }
    if !cur.trim().is_empty() { cmds.push(cur.trim().to_string()); }
    cmds
}

/// Parse Kakoune-style key sequence: "F<ret><down>" → ["F", "<ret>", "<down>"]
/// Any <...> is a special key. Use <lt>/<gt> for literal angle brackets.
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            for ch in chars.by_ref() {
                key.push(ch);
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
    let mut app = AppContext::default();
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
        wait_bg(&mut app);  // wait for file load to complete
    }
    app.viewport(50, 120);  // set after load so first view gets viewport
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
            // Convert plain value to SQL expression (same as do_filter)
            let expr = if is_plain_value(text) {
                if let Some(v) = app.view() {
                    let col = v.col_name(v.state.cc).unwrap_or_default();
                    let is_str = v.data.col_type(v.state.cc) == table::ColType::Str;
                    if is_str { format!("\"{}\" = '{}'", col, text) } else { format!("\"{}\" = {}", col, text) }
                } else { text.to_string() }
            } else { text.to_string() };
            run(app, Box::new(Filter { expr }));
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

/// Block until background meta completes (stub - meta now via plugin)
fn wait_bg_meta(_app: &mut AppContext) {
    // Background meta removed - computed via plugin on demand
}

/// Fetch visible rows for lazy parquet view (via plugin)
fn fetch_lazy(view: &mut state::ViewState) {
    if let state::ViewSource::Parquet { ref path, .. } = view.source {
        let offset = view.state.r0;
        if let Some(plugin) = dynload::get() {
            let w = view.filter.as_deref().unwrap_or("TRUE");
            if let Some(t) = plugin.fetch_where(path, w, offset, 50) {
                view.data = dynload::to_box_table(&t);
            }
        }
    }
}

/// Compile PRQL to SQL
fn compile_prql(prql: &str) -> Option<String> {
    if prql.is_empty() { return None; }
    let opts = prqlc::Options::default().no_format();
    prqlc::compile(prql, &opts).ok()
}

/// Print view data - compiles PRQL to SQL and executes via plugin
fn print(app: &mut AppContext) {
    if let Some(view) = app.view_mut() {
        // Compile and execute PRQL if present
        let path = view.path().to_string();
        let prql = &view.prql;
        if !prql.is_empty() && !path.is_empty() {
            if let Some(sql) = compile_prql(prql) {
                if let Some(plugin) = dynload::get() {
                    if let Some(t) = plugin.query(&sql, &path) {
                        use table::Table;
                        println!("=== {} ({} rows) ===", view.name, t.rows());
                        println!("{}", t.col_names().join(","));
                        for r in 0..t.rows().min(10) {
                            let row: Vec<String> = (0..t.cols()).map(|c| t.cell(r, c).format(10)).collect();
                            println!("{}", row.join(","));
                        }
                        return;
                    }
                }
            }
        }
        // Fallback: show cached data
        println!("=== {} ({} rows) ===", view.name, view.rows());
        fetch_lazy(view);
        let cols = view.data.col_names();
        println!("{}", cols.join(","));
        let r0 = view.state.r0;
        let n = view.data.rows().min(r0 + 10);
        for r in r0..n {
            let row: Vec<String> = (0..cols.len()).map(|c| view.data.cell(r, c).format(10)).collect();
            println!("{}", row.join(","));
        }
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
    // Wait for background tasks to complete
    wait_bg(app);
    if let Some(view) = app.view_mut() {
        fetch_lazy(view);  // simulate render fetch
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let disk = view.source.disk_rows().map(|n| n.to_string()).unwrap_or("-".into());
        let df_rows = view.data.rows();
        let keys = view.col_separator.unwrap_or(0);
        let sel = view.selected_cols.len();
        // Debug: check which cols would show as selected in render
        let sel_cols: Vec<usize> = view.selected_cols.iter().copied().collect();
        println!("STATUS: view={} rows={} disk={} df={} col={} col_name={} keys={} sel={} sel_cols={:?} mem={}MB",
            view.name, view.rows(), disk, df_rows, view.state.cc, col_name, keys, sel, sel_cols, mem_mb());
    }
}

/// Wait for all background tasks to complete (stub - now via plugin)
fn wait_bg(app: &mut AppContext) {
    // Background tasks removed - data fetched via plugin on demand
    app.check_bg_saver();
}

/// Parse command string into Command object
fn parse(line: &str, app: &mut AppContext) -> Option<Box<dyn command::Command>> {
    let (cmd, arg) = line.split_once(' ').map(|(c, a)| (c, a.trim())).unwrap_or((line, ""));
    let cmd = cmd.to_lowercase();

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
            let (col, desc) = if let Some(c) = arg.strip_prefix('-') { (c, true) } else { (arg, false) };
            return Some(Box::new(Sort { col_name: col.to_string(), descending: desc }));
        }
        "sort_desc" | "sortdesc" => return Some(Box::new(Sort { col_name: arg.to_string(), descending: true })),
        "take" => return arg.parse().ok().map(|n| Box::new(Take { n }) as Box<dyn command::Command>),
        "to_time" => return Some(Box::new(ToTime { col_name: arg.to_string() })),
        "derive" => return Some(Box::new(Derive { col_name: arg.to_string() })),
        "xkey" => {
            let cols: Vec<String> = arg.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
            return Some(Box::new(Xkey { col_names: cols }));
        }
        "rename" => {
            let (old, new) = arg.split_once(' ')?;
            return Some(Box::new(RenameCol { old_name: old.into(), new_name: new.into() }));
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
        "swap" => return Some(Box::new(Swap)),
        "dup" => return Some(Box::new(Dup)),
        "ls" => {
            let dir = if arg.is_empty() { std::env::current_dir().unwrap_or_default() } else { std::path::PathBuf::from(arg) };
            return Some(Box::new(Ls { dir, recursive: false }));
        }
        "lr" => {
            let dir = if arg.is_empty() { std::env::current_dir().unwrap_or_default() } else { std::path::PathBuf::from(arg) };
            return Some(Box::new(Ls { dir, recursive: true }));
        }
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
            let m = find(view.data.as_ref(), &expr);
            let cur = view.state.cr;
            let pos = if forward { m.iter().find(|&&i| i > cur) } else { m.iter().rev().find(|&&i| i < cur) };
            if let Some(&p) = pos { view.state.cr = p; view.state.visible(); }
            else { app.msg("No more matches"); }
        }
    } else { app.msg("No search active"); }
}

/// Convert KeyEvent to Kakoune-style key name for keymap lookup
fn key_str(key: &KeyEvent) -> String {
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
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| match v.kind {
        state::ViewKind::Folder => "folder",
        state::ViewKind::Freq => "freq",
        state::ViewKind::Meta => "meta",
        state::ViewKind::Corr => "corr",
        state::ViewKind::Pivot => "pivot",
        state::ViewKind::Table => "table",
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
    // Try keyhandler for commands it can resolve (with context)
    if let Some(cmd_str) = keyhandler::to_cmd(app, cmd) {
        if let Some(c) = parse(&cmd_str, app) {
            run(app, c);
            // Clear selection after xkey (Xkey auto-selects key cols, would toggle off on next !)
            if cmd == "xkey" { if let Some(v) = app.view_mut() { v.selected_cols.clear(); } }
            return Ok(true);
        }
    }
    // Handle commands that need special logic or prompts
    match cmd {
        // Exit commands
        "quit" => {
            if !app.has_view() { return Ok(false); }  // already empty → exit
            run(app, Box::new(Pop));  // pop view, may return to empty state
        }
        "force_quit" => return Ok(false),
        "print_status" => { print_status(app); return Ok(false); }
        // Page navigation (needs app.page())
        "page_down" => run(app, Box::new(Goto { arg: app.page().to_string() })),
        "page_up" => run(app, Box::new(Goto { arg: (-app.page()).to_string() })),
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
            if let Some(expr) = app.view().and_then(|v| {
                let col_name = v.col_name(v.state.cc)?;
                let cell = v.data.cell(v.state.cr, v.state.cc);
                let is_str = v.data.col_type(v.state.cc) == table::ColType::Str;
                let val = unquote(&cell.format(10));
                Some(if is_str { format!("{} = '{}'", col_name, val) } else { format!("{} = {}", col_name, val) })
            }) {
                app.search.col_name = None;
                app.search.value = Some(expr.clone());
                app.msg(format!("Search: {}", expr));
            }
        }
        // Column operations (freq, sort, derive handled by keyhandler)
        "meta" => if app.has_view() { run(app, Box::new(Metadata)); },
        "corr" => {
            if app.has_view() {
                run(app, Box::new(Correlation {
                    selected_cols: app.view().map(|v| v.selected_cols.iter().copied().collect()).unwrap_or_default()
                }));
                if let Some(v) = app.view_mut() { v.selected_cols.clear(); }
            }
        }
        "pivot" => if app.has_view() { run(app, Box::new(crate::plugin::pivot::Pivot)); },
        "rename" => {
            if let Some(old_name) = app.view().and_then(|v| v.col_name(v.state.cc)) {
                if let Some(new_name) = prompt(app, &format!("Rename '{}' to: ", old_name))? {
                    run(app, Box::new(RenameCol { old_name, new_name }));
                }
            }
        }
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
        // View management (ls, lr, swap handled by keyhandler)
        "dup" => if app.has_view() { run(app, Box::new(Dup)); },
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
            if app.bookmarks.is_empty() { app.msg("No bookmarks"); }
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
        // sel_null, sel_single handled by keyhandler
        _ => {}
    }
    Ok(true)
}

/// Search with fzf (/)
fn do_search(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let file = v.filename.as_deref();
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
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.data.col_type(v.state.cc) == table::ColType::Str;
        let file = v.filename.as_deref();
        let header = v.data.col_names().join(" | ");
        Some((hints(v.data.as_ref(), &col_name, v.state.cr, file), col_name, is_str, header))
    });
    if let Some((hint_list, col_name, is_str, header)) = info {
        let expr_opt = picker::fzf_filter(hint_list, "WHERE> ", &col_name, is_str, Some(&header));
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

/// Type conversion ($) - stub, use PRQL derive for casting
fn do_convert(app: &mut AppContext) -> Result<()> {
    app.msg("Use derive for type conversion (PRQL syntax)");
    Ok(())
}

/// Command picker (:)
fn do_command_picker(app: &mut AppContext) -> Result<()> {
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
fn do_goto_col(app: &mut AppContext) -> Result<()> {
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

/// Strip quotes from polars string values
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

/// Generate filter hints from table data (stub - uses Table trait)
fn hints(table: &dyn table::Table, col_name: &str, _row: usize, file: Option<&str>) -> Vec<String> {
    let mut items = Vec::new();

    // Try to get hints from plugin for parquet files
    if let Some(path) = file.filter(|f| f.ends_with(".parquet")) {
        if let Some(plugin) = dynload::get() {
            if let Some(vals) = plugin.distinct(path, col_name) {
                items.extend(vals.into_iter().filter(|v| v != "null"));
            }
        }
    } else {
        // Get distinct values from in-memory table
        let col_idx = table.col_names().iter().position(|c| c == col_name);
        if let Some(idx) = col_idx {
            let mut seen = std::collections::HashSet::new();
            for r in 0..table.rows().min(1000) {  // sample first 1000 rows
                let v = table.cell(r, idx).format(10);
                if v != "null" && seen.insert(v.clone()) {
                    items.push(unquote(&v));
                }
            }
        }
    }

    items.sort();
    items
}

/// Check if expression is a plain value (simple identifier or literal)
fn is_plain_value(expr: &str) -> bool {
    let e = expr.trim();
    // Empty or has spaces (likely SQL) → not plain
    if e.is_empty() || e.contains(' ') { return false; }
    // Quoted string literal → plain
    if (e.starts_with('\'') && e.ends_with('\'')) || (e.starts_with('"') && e.ends_with('"')) { return true; }
    // Alphanumeric/underscore (identifier or number) → plain
    e.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-')
}

/// Find rows matching expression (stub - returns empty for now)
fn find(_table: &dyn table::Table, _expr: &str) -> Vec<usize> {
    // TODO: implement via plugin for SQL filtering
    Vec::new()
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

    #[test]
    fn test_key_str_backslash() {
        use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let key = KeyEvent::new(KeyCode::Char('\\'), KeyModifiers::NONE);
        assert_eq!(key_str(&key), "<backslash>", "backslash should map to <backslash>");
    }

    #[test]
    fn test_is_plain_value() {
        assert!(is_plain_value("foo"));
        assert!(is_plain_value("123"));
        assert!(is_plain_value("'quoted'"));
        assert!(!is_plain_value("a > b"));
        assert!(!is_plain_value("col = 5"));
    }
}
