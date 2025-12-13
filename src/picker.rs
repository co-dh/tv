use anyhow::Result;
use crossterm::{cursor, execute, terminal};
use std::io::Write;
use std::process::{Command, Stdio};

/// Use external fzf - returns selected item
pub fn fzf(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    terminal::disable_raw_mode()?;
    execute!(std::io::stdout(), cursor::Show)?;

    let mut child = Command::new("fzf")
        .args(["--prompt", prompt, "--layout=reverse", "--height=50%"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(items.join("\n").as_bytes())?;
    }

    let output = child.wait_with_output()?;

    terminal::enable_raw_mode()?;
    execute!(std::io::stdout(), cursor::Hide)?;

    if output.status.success() {
        Ok(Some(String::from_utf8_lossy(&output.stdout).trim().to_string()))
    } else {
        Ok(None)
    }
}

/// fzf with edit - select then edit before submit
pub fn fzf_edit(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    match fzf(items, prompt)? {
        Some(selected) => edit_line(prompt, &selected),
        None => Ok(None),
    }
}

/// Run fzf in filter mode (non-interactive, for testing)
#[cfg(test)]
fn fzf_filter(items: Vec<String>, query: &str) -> Result<Option<String>> {
    let mut child = Command::new("fzf")
        .args(["--filter", query])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(items.join("\n").as_bytes())?;
    }

    let output = child.wait_with_output()?;
    Ok(output.stdout.split(|&b| b == b'\n')
        .next()
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).to_string()))
}

/// Simple line editor with pre-filled text
fn edit_line(prompt: &str, initial: &str) -> Result<Option<String>> {
    use crossterm::event::{read, Event, KeyCode, KeyModifiers};

    let mut line = initial.to_string();
    let mut pos = line.len();
    let mut stdout = std::io::stdout();

    terminal::disable_raw_mode()?;
    execute!(stdout, cursor::Show)?;

    let (_, h) = terminal::size()?;
    execute!(stdout, cursor::MoveTo(0, h - 1), terminal::Clear(terminal::ClearType::CurrentLine))?;
    print!("{}{}", prompt, line);
    stdout.flush()?;

    terminal::enable_raw_mode()?;

    loop {
        if let Event::Key(key) = read()? {
            match key.code {
                KeyCode::Enter => {
                    execute!(stdout, cursor::Hide)?;
                    return Ok(Some(line));
                }
                KeyCode::Esc => {
                    execute!(stdout, cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Backspace if pos > 0 => { pos -= 1; line.remove(pos); }
                KeyCode::Delete if pos < line.len() => { line.remove(pos); }
                KeyCode::Left if pos > 0 => { pos -= 1; }
                KeyCode::Right if pos < line.len() => { pos += 1; }
                KeyCode::Home => { pos = 0; }
                KeyCode::End => { pos = line.len(); }
                KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => { pos = 0; }
                KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => { pos = line.len(); }
                KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => { line.clear(); pos = 0; }
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    execute!(stdout, cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Char(c) => { line.insert(pos, c); pos += 1; }
                _ => {}
            }
            execute!(stdout, cursor::MoveTo(0, h - 1), terminal::Clear(terminal::ClearType::CurrentLine))?;
            print!("{}{}", prompt, line);
            execute!(stdout, cursor::MoveTo((prompt.len() + pos) as u16, h - 1))?;
            stdout.flush()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fzf_filter_exact_match() {
        let items = vec!["apple".into(), "banana".into(), "cherry".into()];
        let result = fzf_filter(items, "banana").unwrap();
        assert_eq!(result, Some("banana".to_string()));
    }

    #[test]
    fn test_fzf_filter_partial_match() {
        let items = vec!["apple".into(), "pineapple".into(), "grape".into()];
        let result = fzf_filter(items, "apple").unwrap();
        // fzf returns first match
        assert!(result.is_some());
        assert!(result.unwrap().contains("apple"));
    }

    #[test]
    fn test_fzf_filter_no_match() {
        let items = vec!["apple".into(), "banana".into()];
        let result = fzf_filter(items, "xyz").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_fzf_filter_empty_items() {
        let items: Vec<String> = vec![];
        let result = fzf_filter(items, "test").unwrap();
        assert_eq!(result, None);
    }
}
