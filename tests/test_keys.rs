//! Key play tests (interactive mode simulation)
//! Key names follow Kakoune style: F<ret><down> (no commas)
mod common;
use common::{run_keys, footer};

// === CSV (memory backend) key tests ===

#[test]
fn test_freq_a_in_tab_line() {
    // F on column a creates freq view, tabs should show "freq a"
    let output = run_keys("F", "tests/data/basic.csv");
    let (tab, _) = footer(&output);
    assert!(tab.contains("| freq a"), "tabs: {}", tab);
}

#[test]
fn test_freq_enter_filters_parent_shows_filter_command() {
    // F on column b creates freq, Enter selects first value (x), filters parent to 3 rows
    let output = run_keys("<right>F<ret>", "tests/data/basic.csv");
    let (tab, status) = footer(&output);
    assert!(tab.contains("filter `b` == 'x'"), "tab: {}", tab);
    assert!(status.contains("0/3"), "status: {}", status);
}

#[test]
fn test_sort_asc_orders_first_row_smallest() {
    // [ sorts ascending, first data row should have a=1
    let output = run_keys("[", "tests/data/unsorted.csv");
    let first_data = output.lines().nth(1).unwrap_or("");
    assert!(first_data.contains(" 1 "), "[ should sort asc, first=1: {}", first_data);
}

#[test]
fn test_sort_desc_orders_first_row_largest() {
    // ] sorts descending, first data row should have a=3
    let output = run_keys("]", "tests/data/unsorted.csv");
    let first_data = output.lines().nth(1).unwrap_or("");
    assert!(first_data.contains(" 3 "), "] should sort desc, first=3: {}", first_data);
}

#[test]
fn test_meta_shows_column_stats() {
    // M opens meta view showing column statistics
    let output = run_keys("M", "tests/data/basic.csv");
    let (tab, _) = footer(&output);
    assert!(tab.contains("| meta"), "tab: {}", tab);
    assert!(output.contains("column"), "should show column stats: {}", output);
}

#[test]
#[ignore]  // requires tty for fzf
fn test_meta_filter_narrows_columns() {
    // Meta view, then filter with \ - should filter columns
    let output = run_keys("M<backslash>a<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.contains("0/1"), "should filter to 1 row (col a): {}", status);
}

#[test]
#[ignore]  // requires tty for fzf
fn test_keys_folder_filter() {
    // Folder view, then filter with \ - should filter files
    let output = run_keys(":ls<ret><backslash>Cargo<ret>", ".");
    assert!(output.contains("Cargo"), "\\ in folder should filter to Cargo files: {}", output);
    assert!(!output.contains("No binding"), "backslash should have binding in folder: {}", output);
}

#[test]
#[ignore]  // requires tty for fzf
fn test_keys_freq_filter() {
    // Freq on b, then filter with \ - should filter freq view to 1 row
    let output = run_keys("<right>F<backslash>x<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/1"), "should filter freq to 1 row (x): {}", status);
}

// === Parquet (disk backend) key tests ===

#[test]
fn test_keys_parquet_freq() {
    let output = run_keys("F", "tests/data/sample.parquet");
    let (tab, _) = footer(&output);
    assert!(tab.contains("| freq id"), "F should show freq view: {}", tab);
}

#[test]
fn test_keys_parquet_freq_enter() {
    let output = run_keys("F<ret>", "tests/data/sample.parquet");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/1"), "F<ret> should filter to 1 row: {}", status);
}

#[test]
fn test_keys_parquet_sort_asc() {
    // [ on age sorts ascending, first row should have age=18
    let output = run_keys("<right>[", "tests/data/sample.parquet");
    let first = output.lines().nth(1).unwrap_or("");
    assert!(first.contains(" 14  18 2,023 "), "[ on age should sort asc: {}", first);
}

#[test]
fn test_keys_parquet_sort_desc() {
    // ] on age sorts descending, first row should have age=80
    let output = run_keys("<right>]", "tests/data/sample.parquet");
    let first = output.lines().nth(1).unwrap_or("");
    // First row should have id containing 180 and age=80
    assert!(first.contains("180") && first.contains(" 80 "), "] on age should sort desc: {}", first);
}

#[test]
fn test_keys_parquet_meta() {
    let output = run_keys("M", "tests/data/sample.parquet");
    let (tab, _) = footer(&output);
    assert!(tab.contains("| meta"), "M should show meta: {}", tab);
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
