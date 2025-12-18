//! Key play tests (interactive mode simulation)
//! Key names follow Kakoune style: F<ret><down> (no commas)
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
    let output = run_keys("<right>F<ret>", "tests/data/basic.csv");
    assert!(output.contains("b=x") || output.contains("(3 rows)"), "F<ret> should filter: {}", output);
}

#[test]
fn test_keys_csv_sort_asc() {
    let output = run_keys("[", "tests/data/unsorted.csv");
    // First data row should start with 1 (sorted ascending)
    assert!(output.lines().nth(2).map(|l| l.starts_with("1,")).unwrap_or(false),
        "[ should sort asc, first=1: {}", output);
}

#[test]
fn test_keys_csv_sort_desc() {
    let output = run_keys("]", "tests/data/unsorted.csv");
    // First data row should start with 3 (sorted descending)
    assert!(output.lines().nth(2).map(|l| l.starts_with("3,")).unwrap_or(false),
        "] should sort desc, first=3: {}", output);
}

#[test]
fn test_keys_csv_meta() {
    let output = run_keys("M", "tests/data/basic.csv");
    assert!(output.contains("metadata"), "M should show meta: {}", output);
}

#[test]
fn test_keys_meta_filter() {
    // Meta view, then filter with \ - should filter columns
    let output = run_keys("M<backslash>a<ret>", "tests/data/basic.csv");
    assert!(output.contains("(1 row"), "\\ in meta should filter to 1 row (col a): {}", output);
}

#[test]
fn test_keys_folder_filter() {
    // Folder view, then filter with \ - should filter files
    let output = run_keys(":ls<ret><backslash>Cargo<ret>", ".");
    assert!(output.contains("Cargo"), "\\ in folder should filter to Cargo files: {}", output);
    assert!(!output.contains("No binding"), "backslash should have binding in folder: {}", output);
}

#[test]
fn test_keys_freq_filter() {
    // Freq on b, then filter with \ - should work in any view
    let output = run_keys("<right>F<backslash>x<ret>", "tests/data/basic.csv");
    assert!(output.contains("(1 row"), "\\ should filter freq to 1 row (x): {}", output);
    assert!(output.contains("\"x\""), "Filtered result should contain x: {}", output);
}

// === Parquet (disk backend) key tests ===

#[test]
fn test_keys_parquet_freq() {
    let output = run_keys("F", "tests/data/sample.parquet");
    assert!(output.contains("Freq:id"), "F should show freq view: {}", output);
}

#[test]
fn test_keys_parquet_freq_enter() {
    let output = run_keys("F<ret>", "tests/data/sample.parquet");
    assert!(output.contains("(1 row"), "F<ret> should filter to 1 row: {}", output);
}

#[test]
fn test_keys_parquet_sort_asc() {
    let output = run_keys("<right>[", "tests/data/sample.parquet");
    // First data row should have age=18 (sorted ascending)
    assert!(output.contains(",18,"), "[ on age should sort asc: {}", output);
}

#[test]
fn test_keys_parquet_sort_desc() {
    let output = run_keys("<right>]", "tests/data/sample.parquet");
    // First data row should have age=80 (sorted descending, clipped max)
    assert!(output.contains(",80,"), "] on age should sort desc: {}", output);
}

#[test]
fn test_keys_parquet_meta() {
    let output = run_keys("M", "tests/data/sample.parquet");
    assert!(output.contains("metadata"), "M should show meta: {}", output);
}
