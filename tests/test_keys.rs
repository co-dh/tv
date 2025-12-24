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
    assert!(tab.contains("filter b=='x'"), "tab: {}", tab);
    assert!(status.contains("0/3"), "status: {}", status);
}

#[test]
fn test_delete_twice_different_columns() {
    // Bug: DD deletes same column twice instead of two different columns
    // sample.parquet: id, age, year, ...
    // First D deletes id, second D should delete age (now at cursor), not id again
    let output = run_keys("DD", "tests/data/sample.parquet");
    let hdr = output.lines().next().unwrap_or("");
    // Check column names as words (split by whitespace)
    let cols: Vec<&str> = hdr.split_whitespace().collect();
    assert!(!cols.contains(&"id"), "id should be deleted: {}", hdr);
    assert!(!cols.contains(&"age"), "age should be deleted: {}", hdr);
    assert!(cols.contains(&"year"), "year should remain: {}", hdr);
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
    // [ on age sorts ascending, first row should have age=18 (min age in dataset)
    let output = run_keys("<right>[", "tests/data/sample.parquet");
    let first = output.lines().nth(1).unwrap_or("");
    // Check age=18 is present (multiple rows have age=18, order may vary by backend)
    assert!(first.contains(" 18 "), "[ on age should sort asc, age=18 expected: {}", first);
}

#[test]
fn test_keys_parquet_sort_desc() {
    // ] on age sorts descending, first row should have age=80 (max age in dataset)
    let output = run_keys("<right>]", "tests/data/sample.parquet");
    let first = output.lines().nth(1).unwrap_or("");
    // Check age=80 is present
    assert!(first.contains(" 80 "), "] on age should sort desc, age=80 expected: {}", first);
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

#[test]
fn test_last_col_visible() {
    // Navigate to last column - should still show data, not blank screen
    // ps has many columns, navigating to last should still render
    let output = run_keys(":ps<ret>llllllllllllllllllll", "tests/data/basic.csv");
    // Header line (line 0) should have the last column name visible
    let hdr = output.lines().next().unwrap_or("");
    // Data should be visible - at least first data row shouldn't be empty
    let data = output.lines().nth(1).unwrap_or("");
    let non_ws = data.chars().filter(|c| !c.is_whitespace()).count();
    assert!(non_ws > 0, "Last col should show data, got blank. Header: '{}', Data: '{}'", hdr, data);
}
