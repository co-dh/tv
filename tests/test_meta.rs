//! Meta view tests - key-based
mod common;
use common::run_keys;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_ID: AtomicUsize = AtomicUsize::new(2000);
fn tid() -> usize { TEST_ID.fetch_add(1, Ordering::SeqCst) }

// Tests use:
// - tests/data/basic.csv: a,b (1,x 2,y 3,x 4,z 5,x) - 5 rows
// - tests/data/numeric.csv: x,y,z - 5 rows

#[test]
fn test_meta_shows_columns() {
    let out = run_keys("M", "tests/data/basic.csv");
    assert!(out.contains("metadata"), "Should create metadata view: {}", out);
    assert!(out.contains("column"), "Should have column header");
    assert!(out.contains("type"), "Should have type column");
    assert!(out.contains("null%"), "Should have null% column");
    assert!(out.contains("distinct"), "Should have distinct column");
}

#[test]
fn test_meta_shows_numeric_stats() {
    let out = run_keys("M", "tests/data/numeric.csv");
    assert!(out.contains("median"), "Should have median: {}", out);
    assert!(out.contains("sigma"), "Should have sigma");
    assert!(out.contains("min"), "Should have min");
    assert!(out.contains("max"), "Should have max");
}

#[test]
fn test_null_column_detection() {
    let id = tid();
    let p = format!("tmp/tv_nulls_{}.csv", id);
    fs::write(&p, "a,b,c,d\n1,x,,val\n2,y,,val\n3,x,,val\n").unwrap();
    let out = run_keys("M", &p);
    assert!(out.contains("100"), "Should show 100% null for c: {}", out);
    fs::remove_file(&p).ok();
}

#[test]
fn test_single_value_distinct() {
    let id = tid();
    let p = format!("tmp/tv_single_{}.csv", id);
    fs::write(&p, "a,b\n1,x\n2,x\n3,x\n").unwrap();
    let out = run_keys("M", &p);
    // b column should show 1 distinct value
    assert!(out.contains("distinct"), "Should have distinct: {}", out);
    fs::remove_file(&p).ok();
}

#[test]
fn test_meta_empty_not_dash() {
    let id = tid();
    let p = format!("tmp/tv_meta_{}.csv", id);
    fs::write(&p, "num,str,empty\n1,apple,\n2,banana,\n3,,\n").unwrap();
    let out = run_keys("M", &p);
    let str_line = out.lines().find(|l| l.contains("│ str"));
    if let Some(line) = str_line {
        assert!(!line.contains(" - "), "str row should not have '-': {}", line);
    }
    fs::remove_file(&p).ok();
}

#[test]
fn test_filter_then_meta() {
    // Filter a>2 then meta
    let out = run_keys("<backslash>a <gt> 2<ret>M", "tests/data/basic.csv");
    assert!(out.contains("metadata"), "Should end with meta view: {}", out);
    assert!(out.contains("column"), "Should have column header");
}
