use anyhow::Result;
use ratatui::crossterm::{cursor, execute, terminal};
use std::io::Write;
use std::process::{Command, Stdio};

/// Simple fzf - single select, returns selection or typed query
pub fn fzf(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    fzf_with(items, prompt, None)
}

/// Simple fzf with optional pre-filled query (for testing)
pub fn fzf_with(items: Vec<String>, prompt: &str, pre_query: Option<&str>) -> Result<Option<String>> {
    let (sels, query) = fzf_multi_header(items, prompt, None, pre_query)?;
    if let Some(s) = sels.into_iter().next() { Ok(Some(s)) }
    else if !query.is_empty() { Ok(Some(query)) }
    else { Ok(None) }
}

/// Use external fzf with multi-select - returns (selections, query)
/// --print-query: line1=query, rest=selections (tab to select multiple)
pub fn fzf_multi(items: Vec<String>, prompt: &str) -> Result<(Vec<String>, String)> {
    fzf_multi_header(items, prompt, None, None)
}

/// fzf with optional header and pre-filled query (--filter mode for testing)
pub fn fzf_multi_header(items: Vec<String>, prompt: &str, header: Option<&str>, pre_query: Option<&str>) -> Result<(Vec<String>, String)> {
    let _ = header;  // reserved for future use
    let test_mode = pre_query.is_some();

    // Skip terminal ops in test mode (no TTY)
    if !test_mode {
        terminal::disable_raw_mode()?;
        execute!(std::io::stdout(), cursor::Show)?;
    }

    let mut args = vec!["--prompt", prompt, "--layout=reverse", "--height=50%", "--no-sort", "--print-query", "--multi", "-e"];
    let q;  // keep owned string alive
    if let Some(query) = pre_query { q = query.to_string(); args.extend(["--filter", &q]); }
    let mut child = Command::new("fzf")
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(if test_mode { Stdio::null() } else { Stdio::inherit() })
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(items.join("\n").as_bytes())?;
    }

    let output = child.wait_with_output()?;

    if !test_mode {
        terminal::enable_raw_mode()?;
        execute!(std::io::stdout(), cursor::Hide)?;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let mut lines = text.lines();
    let query = lines.next().unwrap_or("").trim().to_string();
    let sels: Vec<String> = lines.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    Ok((sels, query))
}

/// fzf for filter - returns PRQL filter expression
/// - 1 item from hints → `col` == 'value'
/// - N items from hints → `col` == 'a' || `col` == 'b'
/// - else → raw PRQL expression
/// Examples shown: col == 'x', col > 5, s"col LIKE '%pat%'"
pub fn fzf_filter(hints: Vec<String>, col: &str, is_str: bool, header: Option<&str>, pre_query: Option<&str>) -> Result<Option<String>> {
    // Build prompt with PRQL examples
    let prompt = format!("PRQL: `{}` == 'x' | > 5 | s\"LIKE '%'\" > ", col);
    let (sels, query) = fzf_multi_header(hints.clone(), &prompt, header, pre_query)?;
    // Check how many selections are from hints
    let from_hints: Vec<&String> = sels.iter().filter(|s| hints.contains(s)).collect();
    let expr = if from_hints.len() == 1 {
        // Single hint → PRQL equality
        let v = &from_hints[0];
        if is_str { format!("`{}` == '{}'", col, v) } else { format!("`{}` == {}", col, v) }
    } else if from_hints.len() > 1 {
        // Multiple hints → PRQL OR chain
        let clauses: Vec<String> = from_hints.iter().map(|v| {
            if is_str { format!("`{}` == '{}'", col, v) } else { format!("`{}` == {}", col, v) }
        }).collect();
        format!("({})", clauses.join(" || "))
    } else if !query.is_empty() {
        // Raw PRQL expression
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
