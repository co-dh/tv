//! Common test utilities shared across test modules.

use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_ID: AtomicUsize = AtomicUsize::new(0);

/// Generate unique test ID for parallel test isolation
pub fn unique_id() -> usize {
    TEST_ID.fetch_add(1, Ordering::SeqCst)
}

/// Run tv with --keys mode (interactive key replay) and return output
pub fn run_keys(keys: &str, file: &str) -> String {
    let output = Command::new("./target/release/tv")
        .arg("--keys").arg(keys).arg(file)
        .output().expect("failed to execute tv");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("{}{}", stdout, stderr)
}
