//! Key play tests (interactive mode simulation)
mod common;
use common::run_keys;

// === CSV (memory backend) key tests ===

#[test]
fn test_keys_csv_freq() {
    let output = run_keys("F", "tests/data/basic.csv");
    assert!(output.contains("Freq:a"), "F should show freq view: {}", output);
}

#[test]
fn test_keys_csv_freq_enter() {
    let output = run_keys("Right,F,Enter", "tests/data/basic.csv");
    assert!(output.contains("b=x") || output.contains("(3 rows)"), "F+Enter should filter: {}", output);
}

#[test]
fn test_keys_csv_sort_asc() {
    let output = run_keys("[", "tests/data/unsorted.csv");
    assert!(output.contains("│ 1"), "[ should sort asc, first=1: {}", output);
}

#[test]
fn test_keys_csv_sort_desc() {
    let output = run_keys("]", "tests/data/unsorted.csv");
    assert!(output.contains("│ 3"), "] should sort desc, first=3: {}", output);
}

#[test]
fn test_keys_csv_meta() {
    let output = run_keys("M", "tests/data/basic.csv");
    assert!(output.contains("metadata"), "M should show meta: {}", output);
}

// === Parquet (disk backend) key tests ===

#[test]
fn test_keys_parquet_freq() {
    let output = run_keys("F", "sample.parquet");
    assert!(output.contains("Freq:id"), "F should show freq view: {}", output);
}

#[test]
fn test_keys_parquet_freq_enter() {
    let output = run_keys("F,Enter", "sample.parquet");
    assert!(output.contains("(1 row"), "F+Enter should filter to 1 row: {}", output);
}

#[test]
fn test_keys_parquet_sort_asc() {
    let output = run_keys("Right,[", "sample.parquet");
    assert!(output.contains("┆ 18  ┆"), "[ on age should sort asc: {}", output);
}

#[test]
fn test_keys_parquet_sort_desc() {
    let output = run_keys("Right,]", "sample.parquet");
    assert!(output.contains("┆ 73  ┆"), "] on age should sort desc: {}", output);
}

#[test]
fn test_keys_parquet_meta() {
    let output = run_keys("M", "sample.parquet");
    assert!(output.contains("metadata"), "M should show meta: {}", output);
}
