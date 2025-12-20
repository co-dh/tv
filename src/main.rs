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
use input::on_key;

use anyhow::Result;
use app::AppContext;
use command::executor::CommandExecutor;
use command::io::From;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};


/// Entry point: parse args, run TUI or key replay mode
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Load plugins from standard locations
    dynload::load_plugins();

    // Parse flags first (before early returns)
    let raw_save = args.iter().any(|a| a == "--raw");
    let file_arg = args.iter().skip(1).find(|a| !a.starts_with('-')).cloned();

    // Check for --keys argument (key replay mode for testing)
    if let Some(idx) = args.iter().position(|a| a == "--keys") {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --keys 'F<ret>' file.parquet");
            std::process::exit(1);
        }
        let file = args.get(idx + 2).map(|s| s.as_str());
        let (keys, test_input) = extract_prompts(&parse_keys(&args[idx + 1]));
        let key_events: Vec<KeyEvent> = keys.iter().map(|s| str_to_key(s)).collect();
        let mut app = make_app(file, raw_save);
        app.test_input = test_input;
        let (w, h) = (200, 80);
        let backend = TestBackend::new(w, h);
        let mut term = ratatui::Terminal::new(backend)?;
        app.run_keys(&mut term, &key_events, on_key)?;
        // Output buffer as string
        for line in term.backend().buffer().content.chunks(w as usize) {
            let s: String = line.iter().map(|c| c.symbol()).collect();
            println!("{}", s.trim_end());
        }
        return Ok(());
    }

    // Initialize ratatui terminal and run TUI
    let mut tui = render::init()?;
    let mut app = make_app(file_arg.as_deref(), raw_save);
    app.run(&mut tui, on_key)?;
    render::restore()?;
    Ok(())
}

/// Create app context and load file if provided
fn make_app(file: Option<&str>, raw_save: bool) -> AppContext {
    let mut app = AppContext::default();
    app.raw_save = raw_save;
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
    }
    app
}

/// Extract prompt inputs from key sequence for --keys mode
/// ":pacman<ret>" → keys=[":"], test_input=["pacman"]
/// Convert key name to char for prompt text
fn key_to_char(s: &str) -> &str {
    match s {
        "<lt>" => "<",
        "<gt>" => ">",
        "<space>" => " ",
        "<backslash>" => "\\",
        _ => s,
    }
}

fn extract_prompts(keys: &[String]) -> (Vec<String>, Vec<String>) {
    // Keys that trigger prompts: : ^ \ s (command picker, rename, filter, select)
    let prompt_keys = [":", "^", "<backslash>", "s"];
    let mut out_keys = Vec::new();
    let mut test_input = Vec::new();
    let mut i = 0;
    while i < keys.len() {
        if prompt_keys.contains(&keys[i].as_str()) {
            out_keys.push(keys[i].clone());
            i += 1;
            // Collect until <ret>, convert special keys to chars
            let mut cmd = String::new();
            while i < keys.len() && keys[i] != "<ret>" {
                cmd.push_str(key_to_char(&keys[i]));
                i += 1;
            }
            if i < keys.len() { i += 1; }  // skip <ret>
            if !cmd.is_empty() { test_input.push(cmd); }
        } else {
            out_keys.push(keys[i].clone());
            i += 1;
        }
    }
    (out_keys, test_input)
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

/// Convert key string to KeyEvent
fn str_to_key(s: &str) -> KeyEvent {
    let (code, mods) = match s {
        "<ret>" => (KeyCode::Enter, KeyModifiers::NONE),
        "<esc>" => (KeyCode::Esc, KeyModifiers::NONE),
        "<up>" => (KeyCode::Up, KeyModifiers::NONE),
        "<down>" => (KeyCode::Down, KeyModifiers::NONE),
        "<left>" => (KeyCode::Left, KeyModifiers::NONE),
        "<right>" => (KeyCode::Right, KeyModifiers::NONE),
        "<home>" => (KeyCode::Home, KeyModifiers::NONE),
        "<end>" => (KeyCode::End, KeyModifiers::NONE),
        "<pageup>" => (KeyCode::PageUp, KeyModifiers::NONE),
        "<pagedown>" => (KeyCode::PageDown, KeyModifiers::NONE),
        "<tab>" => (KeyCode::Tab, KeyModifiers::NONE),
        "<s-tab>" => (KeyCode::BackTab, KeyModifiers::NONE),
        "<del>" => (KeyCode::Delete, KeyModifiers::NONE),
        "<backspace>" => (KeyCode::Backspace, KeyModifiers::NONE),
        "<space>" => (KeyCode::Char(' '), KeyModifiers::NONE),
        "<backslash>" => (KeyCode::Char('\\'), KeyModifiers::NONE),
        "<lt>" => (KeyCode::Char('<'), KeyModifiers::NONE),
        "<gt>" => (KeyCode::Char('>'), KeyModifiers::NONE),
        s if s.starts_with("<c-") && s.ends_with('>') => {
            let c = s.chars().nth(3).unwrap_or('?');
            (KeyCode::Char(c), KeyModifiers::CONTROL)
        }
        s if s.len() == 1 => (KeyCode::Char(s.chars().next().unwrap()), KeyModifiers::NONE),
        _ => (KeyCode::Null, KeyModifiers::NONE),
    };
    KeyEvent::new(code, mods)
}

#[cfg(test)]
mod tests {
    use super::*;
    use input::prompt::is_plain_value;

    #[test]
    fn test_key_str_backslash() {
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

    #[test]
    fn test_str_to_key() {
        assert_eq!(str_to_key("<ret>").code, KeyCode::Enter);
        assert_eq!(str_to_key("<esc>").code, KeyCode::Esc);
        assert_eq!(str_to_key("a").code, KeyCode::Char('a'));
        assert_eq!(str_to_key("<c-x>").code, KeyCode::Char('x'));
        assert!(str_to_key("<c-x>").modifiers.contains(KeyModifiers::CONTROL));
    }

    #[test]
    fn test_extract_prompts() {
        let keys = parse_keys(":pacman<ret>");
        let (out, input) = extract_prompts(&keys);
        assert_eq!(out, vec![":"]);
        assert_eq!(input, vec!["pacman"]);
    }

    #[test]
    fn test_extract_prompts_multiple() {
        let keys = parse_keys(":ps<ret>F<ret>:systemctl<ret>");
        let (out, input) = extract_prompts(&keys);
        assert_eq!(out, vec![":", "F", "<ret>", ":"]);
        assert_eq!(input, vec!["ps", "systemctl"]);
    }
}

    #[test]
    fn test_extract_prompts_select() {
        let keys = parse_keys("sname,city<ret>");
        let (out, input) = extract_prompts(&keys);
        assert_eq!(out, vec!["s"]);
        assert_eq!(input, vec!["name,city"]);
    }
