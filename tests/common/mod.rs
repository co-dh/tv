//! Common test utilities

use std::process::Command;

/// Run tv with --keys mode and return rendered buffer
pub fn run_keys(keys: &str, file: &str) -> String {
    let mut cmd = Command::new("./target/release/tv");
    cmd.arg("--keys").arg(keys);
    if !file.is_empty() { cmd.arg(file); }
    let output = cmd.output().expect("failed to execute tv");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("{}{}", stdout, stderr)
}

/// Extract tab line (second-to-last line) from buffer output
pub fn tab_line(output: &str) -> &str {
    let lines: Vec<&str> = output.lines().collect();
    lines.get(lines.len().saturating_sub(2)).copied().unwrap_or("")
}
