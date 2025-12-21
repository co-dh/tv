//! Common test utilities

use std::process::Command;

/// Run tabv with --keys mode and return rendered buffer
pub fn run_keys(keys: &str, file: &str) -> String {
    let mut cmd = Command::new("./target/release/tabv");
    cmd.arg("--keys").arg(keys);
    if !file.is_empty() { cmd.arg(file); }
    let output = cmd.output().expect("failed to execute tv");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("{}{}", stdout, stderr)
}

/// Extract tab line (2nd-to-last) and status line (last)
pub fn footer(output: &str) -> (&str, &str) {
    let mut it = output.lines().rev();
    let status = it.next().expect("output missing status line");
    let tab = it.next().expect("output missing tab line");
    (tab, status)
}

/// Extract header line (first line)
pub fn header(output: &str) -> &str {
    output.lines().next().expect("output missing header")
}
