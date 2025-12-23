//! Large parquet tests - all 1.parquet tests in one file with sequential lock
//! This prevents OOM from parallel execution on large files
mod common;
use common::{run_keys, footer};
use std::sync::Mutex;

// Global lock ensures sequential execution of large parquet tests
static LOCK: Mutex<()> = Mutex::new(());

/// Acquire lock, ignoring poison from previous panics
fn lock() -> std::sync::MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Extract key columns from header (before |)
fn keys_from_header(out: &str) -> Vec<String> {
    let hdr = out.lines().next().unwrap_or("");
    if let Some(idx) = hdr.find('|') {
        hdr[..idx].split_whitespace()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    } else { vec![] }
}

// === Key toggle tests ===

#[test]
fn test_key_toggle_property_add() {
    let _lock = lock();
    let out = run_keys("!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert_eq!(keys, vec!["Time"], "After ! on col 0: {:?}", keys);
}

#[test]
fn test_key_toggle_property_roundtrip() {
    let _lock = lock();
    let out = run_keys("!!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert!(keys.is_empty(), "After !! should have no keys: {:?}", keys);
}

#[test]
fn test_key_toggle_property_multiple() {
    let _lock = lock();
    let out = run_keys("!l!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert_eq!(keys.len(), 2, "Should have 2 keys: {:?}", keys);
    assert!(keys.contains(&"Time".to_string()), "Should have Time: {:?}", keys);
    assert!(keys.contains(&"Exchange".to_string()), "Should have Exchange: {:?}", keys);
}

#[test]
fn test_key_toggle_property_three() {
    let _lock = lock();
    let out = run_keys("!l!l!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert_eq!(keys.len(), 3, "Should have 3 keys: {:?}", keys);
}

#[test]
fn test_key_toggle_property_remove_middle() {
    let _lock = lock();
    let out = run_keys("!l!l!h!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert_eq!(keys.len(), 2, "Should have 2 keys after removing middle: {:?}", keys);
    assert!(keys.contains(&"Time".to_string()), "Should have Time: {:?}", keys);
    assert!(keys.contains(&"Symbol".to_string()), "Should have Symbol: {:?}", keys);
    assert!(!keys.contains(&"Exchange".to_string()), "Should NOT have Exchange: {:?}", keys);
}

// === Large parquet workflow tests ===

#[test]
fn test_large_parquet_freq_enter_single() {
    let _lock = lock();
    // After freq-enter, cursor lands on filtered column (Exchange)
    // F on Exchange after filter shows 1 row (all P)
    let out = run_keys("<right>F<ret>F", "tests/data/nyse/1.parquet");
    let (_, status) = footer(&out);
    assert!(status.ends_with("0/1"), "Filtered freq should show 1 row: {}", status);
}

#[test]
fn test_large_parquet_filter_not_10k() {
    let _lock = lock();
    let out = run_keys("<right>F<ret><ret>", "tests/data/nyse/1.parquet");
    let (_, status) = footer(&out);
    assert!(status.contains("94874100") || status.contains("94,874,100"), "Should show 94M rows: {}", status);
}

#[test]
fn test_large_parquet_filtered_freq_symbol() {
    let _lock = lock();
    // After freq-enter, cursor at Exchange (col 1), <right> goes to Symbol (col 2)
    let out = run_keys("<right>F<ret><right>F", "tests/data/nyse/1.parquet");
    let (_, status) = footer(&out);
    assert!(status.ends_with("0/11,342"), "Symbol freq should have 11,342 rows: {}", status);
}

#[test]
fn test_large_parquet_freq_enter_memory() {
    let _lock = lock();
    // Freq-enter should not load all data into memory
    let out = run_keys("<right>F<ret>", "tests/data/nyse/1.parquet");
    let (_, status) = footer(&out);
    // Status format: "...stats XXMB row/total"
    let mem: usize = status.split("MB ").next()
        .and_then(|s| s.split_whitespace().last())
        .and_then(|s| s.parse().ok()).unwrap_or(9999);
    assert!(mem < 1000, "Memory should be < 1GB, got {}MB: {}", mem, status);
}
