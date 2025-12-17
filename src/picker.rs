use anyhow::Result;
use ratatui::crossterm::{cursor, execute, terminal};
use std::io::Write;
use std::process::{Command, Stdio};

/// Simple fzf - single select, returns selection or typed query
pub fn fzf(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    let (sels, query) = fzf_multi(items, prompt)?;
    if let Some(s) = sels.into_iter().next() { Ok(Some(s)) }
    else if !query.is_empty() { Ok(Some(query)) }
    else { Ok(None) }
}

/// Use external fzf with multi-select - returns (selections, query)
/// --print-query: line1=query, rest=selections (tab to select multiple)
pub fn fzf_multi(items: Vec<String>, prompt: &str) -> Result<(Vec<String>, String)> {
    fzf_multi_header(items, prompt, None)
}

/// fzf with optional header line (for showing column context)
pub fn fzf_multi_header(items: Vec<String>, prompt: &str, header: Option<&str>) -> Result<(Vec<String>, String)> {
    terminal::disable_raw_mode()?;
    execute!(std::io::stdout(), cursor::Show)?;

    let _ = header;  // reserved for future use
    let mut child = Command::new("fzf")
        .args(["--prompt", prompt, "--layout=reverse", "--height=50%", "--no-sort", "--print-query", "--multi"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(items.join("\n").as_bytes())?;
    }

    let output = child.wait_with_output()?;

    terminal::enable_raw_mode()?;
    execute!(std::io::stdout(), cursor::Hide)?;

    let text = String::from_utf8_lossy(&output.stdout);
    let mut lines = text.lines();
    let query = lines.next().unwrap_or("").trim().to_string();
    let sels: Vec<String> = lines.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    Ok((sels, query))
}

/// fzf for filter - returns SQL expression based on selection
/// - 1 item from hints → col = 'value'
/// - N items from hints → col IN ('a', 'b')
/// - else → raw SQL
pub fn fzf_filter(hints: Vec<String>, prompt: &str, col: &str, is_str: bool, header: Option<&str>) -> Result<Option<String>> {
    let (sels, query) = fzf_multi_header(hints.clone(), prompt, header)?;
    // Check how many selections are from hints
    let from_hints: Vec<&String> = sels.iter().filter(|s| hints.contains(s)).collect();
    let expr = if from_hints.len() == 1 {
        // Single hint selected → equality
        let v = &from_hints[0];
        if is_str { format!("\"{}\" = '{}'", col, v) } else { format!("\"{}\" = {}", col, v) }
    } else if from_hints.len() > 1 {
        // Multiple hints → IN clause
        let vals: Vec<String> = from_hints.iter().map(|v| {
            if is_str { format!("'{}'", v) } else { v.to_string() }
        }).collect();
        format!("\"{}\" IN ({})", col, vals.join(", "))
    } else if !query.is_empty() {
        // No hint match, use query as raw SQL
        query
    } else {
        return Ok(None);  // Esc pressed
    };
    Ok(Some(expr))
}

/// Run fzf in filter mode (non-interactive, for testing)
#[cfg(test)]
fn fzf_test_filter(items: Vec<String>, query: &str) -> Result<Option<String>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fzf_filter_exact_match() {
        let items = vec!["apple".into(), "banana".into(), "cherry".into()];
        let result = fzf_test_filter(items, "banana").unwrap();
        assert_eq!(result, Some("banana".to_string()));
    }

    #[test]
    fn test_fzf_filter_partial_match() {
        let items = vec!["apple".into(), "pineapple".into(), "grape".into()];
        let result = fzf_test_filter(items, "apple").unwrap();
        // fzf returns first match
        assert!(result.is_some());
        assert!(result.unwrap().contains("apple"));
    }

    #[test]
    fn test_fzf_filter_no_match() {
        let items = vec!["apple".into(), "banana".into()];
        let result = fzf_test_filter(items, "xyz").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_fzf_filter_empty_items() {
        let items: Vec<String> = vec![];
        let result = fzf_test_filter(items, "test").unwrap();
        assert_eq!(result, None);
    }
}
