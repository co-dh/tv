mod app;
mod command;
mod data;
mod error;
mod input;
mod plugin;
mod render;
mod state;
mod util;
mod utils;

use data::dynload;
use input::{on_key, parse, handle_cmd, cur_tab, is_plain_value, fetch_lazy};
use util::pure;

use anyhow::Result;
use app::AppContext;
use command::executor::CommandExecutor;
use command::io::{From, Save};
use command::nav::Goto;
use command::transform::{Filter, RenameCol, Select};
use std::fs;

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

    // Load sqlite plugin (essential for memory:id paths)
    let sqlite_paths: Vec<_> = [
        exe_dir.map(|d| d.join("libtv_sqlite.so")),
        exe_dir.map(|d| d.join("plugins/libtv_sqlite.so")),
        home_lib.as_ref().map(|d| d.join("libtv_sqlite.so")),
    ].into_iter().flatten().collect();

    let mut loaded = false;
    for p in &sqlite_paths {
        if p.exists() {
            if let Err(e) = dynload::load_sqlite(p.to_str().unwrap_or("")) {
                eprintln!("Error: failed to load sqlite plugin from {:?}: {}", p, e);
                std::process::exit(1);
            }
            loaded = true;
            break;
        }
    }
    if !loaded {
        eprintln!("Error: sqlite plugin not found. Searched:");
        for p in &sqlite_paths { eprintln!("  {:?}", p); }
        std::process::exit(1);
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
        wait_bg(&mut app);
    }
    app.viewport(50, 120);
    let mut mode = InputMode::None;
    let mut buf = String::new();

    for key in parse_keys(keys) {
        if mode != InputMode::None {
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
            input::find_match(app, true);
        }
        InputMode::Filter => {
            let expr = if is_plain_value(text) {
                if let Some(v) = app.view() {
                    let col = v.col_name(v.state.cc).unwrap_or_default();
                    let is_str = v.data.col_type(v.state.cc) == data::table::ColType::Str;
                    if is_str { format!("\"{}\" = '{}'", col, text) } else { format!("\"{}\" = {}", col, text) }
                } else { text.to_string() }
            } else { text.to_string() };
            input::run(app, Box::new(Filter { expr }));
        }
        InputMode::Load => {
            input::run(app, Box::new(From { file_path: text.to_string() }));
        }
        InputMode::Save => {
            input::run(app, Box::new(Save { file_path: text.to_string() }));
        }
        InputMode::Command => {
            if let Some(cmd) = parse(text, app) {
                let _ = CommandExecutor::exec(app, cmd);
            }
        }
        InputMode::Goto => {
            input::run(app, Box::new(Goto { arg: text.to_string() }));
        }
        InputMode::GotoCol => {
            input::run(app, Box::new(command::nav::GotoCol { arg: text.to_string() }));
        }
        InputMode::Select => {
            input::run(app, Box::new(Select { col_names: text.split(',').map(|s| s.trim().to_string()).collect() }));
        }
        InputMode::Rename => {
            let old = app.view().and_then(|v| v.col_name(v.state.cc));
            if let Some(old_name) = old {
                input::run(app, Box::new(RenameCol { old_name, new_name: text.to_string() }));
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
fn wait_bg_meta(_app: &mut AppContext) {}

/// Wait for all background tasks to complete (stub - now via plugin)
fn wait_bg(app: &mut AppContext) {
    app.check_bg_saver();
}

/// Print view data - compiles PRQL to SQL and executes via plugin
fn print(app: &mut AppContext) {
    if let Some(view) = app.view_mut() {
        let path = view.path.clone().unwrap_or_default();
        let prql = &view.prql;
        if !prql.is_empty() && !path.is_empty() {
            if let Some(sql) = pure::compile_prql(prql) {
                if let Some(plugin) = dynload::get_for(&path) {
                    if let Some(t) = plugin.query(&sql, &path) {
                        use data::table::Table;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_str_backslash() {
        use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let key = KeyEvent::new(KeyCode::Char('\\'), KeyModifiers::NONE);
        assert_eq!(input::handler::key_str(&key), "<backslash>", "backslash should map to <backslash>");
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
