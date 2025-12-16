//! Common test utilities shared across test modules.

use std::process::Command;

/// Run tv with --keys mode (interactive key replay) and return output
pub fn run_keys(keys: &str, file: &str) -> String {
    let mut cmd = Command::new("./target/release/tv");
    cmd.arg("--keys").arg(keys);
    if !file.is_empty() { cmd.arg(file); }
    let output = cmd.output().expect("failed to execute tv");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("{}{}", stdout, stderr)
}
