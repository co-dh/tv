//! Common test utilities shared across test modules.

use std::fs;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_ID: AtomicUsize = AtomicUsize::new(0);

/// Generate unique test ID for parallel test isolation
pub fn unique_id() -> usize {
    TEST_ID.fetch_add(1, Ordering::SeqCst)
}

/// Run tv with --script mode and return output
pub fn run_script(script: &str, id: usize) -> String {
    let script_path = format!("/tmp/tv_test_script_{}.txt", id);
    fs::write(&script_path, script).unwrap();

    let output = Command::new("./target/release/tv")
        .arg("--script")
        .arg(&script_path)
        .output()
        .expect("failed to execute tv");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("{}{}", stdout, stderr)
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

/// Create test CSV with basic a,b columns
pub fn setup_test_csv(id: usize) -> String {
    let path = format!("/tmp/tv_test_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,y\n3,x\n4,z\n5,x\n").unwrap();
    path
}
