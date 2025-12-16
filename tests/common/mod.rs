//! Common test utilities shared across test modules.

use std::process::Command;

/// Run tv with --keys mode (interactive key replay) and return output
pub fn run_keys(keys: &str, file: &str) -> String {
    let output = Command::new("./target/release/tv")
        .arg("--keys").arg(keys).arg(file)
        .output().expect("failed to execute tv");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("{}{}", stdout, stderr)
}
