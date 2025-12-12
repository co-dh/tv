mod app;
mod command;
mod render;
mod state;

use anyhow::Result;
use app::AppContext;
use command::executor::CommandExecutor;
use command::io::{Load, Save};
use command::transform::{DelCol, Filter, RenameCol, Select, Sort};
use command::view::{Frequency, Metadata};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::{cursor, execute, style::Print, terminal};
use render::{Renderer, Terminal};
use std::fs;
use std::io::{self, Write};

fn main() -> Result<()> {
    // Get command line args
    let args: Vec<String> = std::env::args().collect();

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

/// Run commands from a script file and print result
fn run_script(script_path: &str) -> Result<()> {
    let script = fs::read_to_string(script_path)?;
    let mut app = AppContext::new();

    // Set a reasonable viewport for printing
    app.update_viewport(50, 120);

    for line in script.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line == "quit" {
            break;
        }

        // Parse and execute command
        if let Some(cmd) = parse_command(line, &app) {
            if let Err(e) = CommandExecutor::execute(&mut app, cmd) {
                eprintln!("Error executing '{}': {}", line, e);
            }
        } else {
            eprintln!("Unknown command: {}", line);
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
        "delcol" => Some(Box::new(DelCol { col_name: arg.to_string() })),
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
            // D: Delete current column
            if let Some(view) = app.current_view() {
                if let Some(col_name) = view.state.current_column(&view.dataframe) {
                    let cmd = Box::new(DelCol {
                        col_name: col_name.to_string(),
                    });
                    if let Err(e) = CommandExecutor::execute(app, cmd) {
                        app.set_message(format!("Error: {}", e));
                    }
                }
            }
        }
        KeyCode::Char('/') => {
            // /: Filter rows
            if !app.has_view() {
                app.set_message("No table loaded".to_string());
            } else if let Some(expression) = prompt_input(app, "Filter expression (e.g., col>10): ")? {
                let cmd = Box::new(Filter { expression });
                if let Err(e) = CommandExecutor::execute(app, cmd) {
                    app.set_message(format!("Error: {}", e));
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
            // Enter: Filter parent table from frequency view
            if let Some(view) = app.current_view() {
                if let (Some(parent_id), Some(freq_col)) = (view.parent_id, view.freq_col.clone()) {
                    // Get the value from first column at current row
                    let cr = view.state.cr;
                    let value = view.dataframe.get_columns()[0]
                        .get(cr)
                        .ok()
                        .map(|v| v.to_string())
                        .unwrap_or_default();

                    // Find parent view and filter
                    if let Some(parent) = app.stack.find_by_id(parent_id) {
                        let parent_df = parent.dataframe.clone();
                        let parent_filename = parent.filename.clone();

                        // Create filter expression
                        let filter_expr = format!("{}=={}", freq_col, value);
                        let cmd = Box::new(Filter { expression: filter_expr });

                        // Push a temporary view with parent data, then filter
                        let id = app.next_id();
                        let filtered_view = state::ViewState::new(
                            id,
                            format!("{}={}", freq_col, value),
                            parent_df,
                            parent_filename,
                        );
                        app.stack.push(filtered_view);

                        // Now apply the filter
                        if let Err(e) = CommandExecutor::execute(app, cmd) {
                            app.set_message(format!("Error: {}", e));
                            app.stack.pop(); // Remove the failed view
                        }
                    }
                }
            }
        }
        _ => {}
    }

    Ok(true)
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
