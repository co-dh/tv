//! Key play tests (interactive mode simulation)
//! Key names follow Kakoune style: F<ret><down> (no commas)
mod common;
use common::{run_keys, tab_line};

// === CSV (memory backend) key tests ===

#[test]
fn test_freq_a_in_tab_line() {
    // F on column a creates freq view, tabs should show "freq a"
    let output = run_keys("F", "tests/data/basic.csv");
    assert!(tab_line(&output).contains("│ freq a"), "tabs: {}", tab_line(&output));
}

#[test]
fn test_freq_enter_filters_parent_shows_filter_command() {
    // F on column b creates freq, Enter selects first value (x), filters parent to 3 rows
    let output = run_keys("<right>F<ret>", "tests/data/basic.csv");
    assert!(tab_line(&output).contains("filter `b` == 'x'"), "tab: {}", tab_line(&output));
    assert!(output.contains("0/3"), "should show 3 rows: {}", output);
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

#[test]
fn test_page_down_scrolls() {
    // After page down, view should scroll (not show row 1)
    let without = run_keys("", "tests/data/sample.parquet");
    let with_pgdn = run_keys("<c-d>", "tests/data/sample.parquet");
    // First data row should be different after page down
    let first_row = |s: &str| s.lines().nth(2).map(|l| l.to_string());
    let r1 = first_row(&without);
    let r2 = first_row(&with_pgdn);
    assert_ne!(r1, r2, "Page down should scroll view: before={:?} after={:?}", r1, r2);
}
