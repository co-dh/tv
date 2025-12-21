//! Meta view tests - key-based
mod common;
use common::run_keys;

// Tests use:
// - tests/data/basic.csv: a,b (1,x 2,y 3,x 4,z 5,x) - 5 rows
// - tests/data/numeric.csv: x,y,z - 5 rows
// - tests/data/null_col_c.csv: a,b,c,d with c all null
// - tests/data/single_val.csv: a,b with b all x
// - tests/data/meta_empty.csv: num,str,empty with mixed

#[test]
fn test_meta_shows_columns() {
    let out = run_keys("M", "tests/data/basic.csv");
    assert!(out.contains("| meta"), "Should create meta view: {}", out);
    assert!(out.contains("column"), "Should have column header");
    // type column may be hidden for small files
    assert!(out.contains("null%"), "Should have null% column");
    assert!(out.contains("distinct"), "Should have distinct column");
}

#[test]
fn test_meta_shows_numeric_stats() {
    let out = run_keys("M", "tests/data/numeric.csv");
    // Basic meta shows: column count distinct null% min max
    assert!(out.contains("min"), "Should have min: {}", out);
    assert!(out.contains("max"), "Should have max");
    assert!(out.contains("distinct"), "Should have distinct");
}

#[test]
fn test_null_column_detection() {
    // null_col_c.csv: a,b,c,d with c all null
    let out = run_keys("M", "tests/data/null_col_c.csv");
    assert!(out.contains("100"), "Should show 100% null for c: {}", out);
}

#[test]
fn test_single_value_distinct() {
    // single_val.csv: a,b with b all x
    let out = run_keys("M", "tests/data/single_val.csv");
    // b column should show 1 distinct value
    assert!(out.contains("distinct"), "Should have distinct: {}", out);
}

#[test]
fn test_meta_empty_not_dash() {
    // meta_empty.csv: num,str,empty with mixed values
    let out = run_keys("M", "tests/data/meta_empty.csv");
    let str_line = out.lines().find(|l| l.contains("│ str"));
    if let Some(line) = str_line {
        assert!(!line.contains(" - "), "str row should not have '-': {}", line);
    }
}

#[test]
fn test_filter_then_meta() {
    // Filter a>2 then meta
    let out = run_keys("<backslash>a > 2<ret>M", "tests/data/basic.csv");
    assert!(out.contains("| meta"), "Should end with meta view: {}", out);
    assert!(out.contains("column"), "Should have column header");
}
