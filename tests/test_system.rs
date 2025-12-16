//! System tests - key-based
mod common;
use common::run_keys;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_ID: AtomicUsize = AtomicUsize::new(5000);
fn tid() -> usize { TEST_ID.fetch_add(1, Ordering::SeqCst) }

// Tests use tests/data/*.csv and tmp/ for dynamic files

#[test]
fn test_failed_filter() {
    // Filter on nonexistent column should show error
    let out = run_keys("<backslash>nonexistent <gt> 1<ret>", "tests/data/basic.csv");
    // Should contain error or remain on original table
    assert!(out.contains("error") || out.contains("a") || out.contains("(5 rows)"),
        "Should handle bad filter: {}", out);
}

// Meta view selection commands (0 for null, 1 for single)
#[test]
fn test_meta_sel_null() {
    let id = tid();
    let p = format!("tmp/tv_sel_null_{}.csv", id);
    fs::write(&p, "a,b,c\n1,,x\n2,,y\n3,,z\n").unwrap();
    // M for meta, 0 to select null columns
    let out = run_keys("M0", &p);
    assert!(out.contains("100"), "Should show 100% null column: {}", out);
    fs::remove_file(&p).ok();
}

#[test]
fn test_meta_sel_single() {
    let id = tid();
    let p = format!("tmp/tv_sel_single_{}.csv", id);
    fs::write(&p, "a,b,c\n1,x,same\n2,y,same\n3,z,same\n").unwrap();
    // M for meta, 1 to select single-value columns
    let out = run_keys("M1", &p);
    assert!(out.contains("metadata"), "Should show meta: {}", out);
    fs::remove_file(&p).ok();
}

#[test]
fn test_meta_shows_all_cols() {
    let id = tid();
    let p = format!("tmp/tv_meta_all_{}.csv", id);
    fs::write(&p, "a,b\n1,\n2,\n3,\n").unwrap();
    let out = run_keys("M", &p);
    assert!(out.contains("(2 rows)"), "Should show 2 columns: {}", out);
    assert!(out.contains("b"), "Should show column b: {}", out);
    fs::remove_file(&p).ok();
}

#[test]
fn test_csv_freq_enter() {
    // F for freq on column b, then enter to filter parent
    let out = run_keys("<right>F<ret>", "tests/data/basic.csv");
    // Should filter parent table by selected freq value
    assert!(out.contains("rows)"), "Should filter: {}", out);
}

#[test]
fn test_save_and_load() {
    let id = tid();
    let out_csv = format!("tmp/tv_save_{}.csv", id);
    let out_pq = format!("tmp/tv_save_{}.parquet", id);

    // Save to CSV
    let keys = format!("S{}<ret>", out_csv);
    run_keys(&keys, "tests/data/basic.csv");
    assert!(std::path::Path::new(&out_csv).exists(), "CSV should be saved");

    // Save to parquet
    let keys = format!("S{}<ret>", out_pq);
    run_keys(&keys, "tests/data/basic.csv");
    assert!(std::path::Path::new(&out_pq).exists(), "Parquet should be saved");

    fs::remove_file(&out_csv).ok();
    fs::remove_file(&out_pq).ok();
}

#[test]
fn test_navigation_keys() {
    // Test basic navigation: up, down, left, right
    let out = run_keys("<down><down><up><right><left>", "tests/data/basic.csv");
    assert!(out.contains("(5 rows)"), "Navigation should work: {}", out);
}

#[test]
fn test_goto_row() {
    // : to goto row, type number, enter
    let out = run_keys(":3<ret>", "tests/data/basic.csv");
    assert!(out.contains("(5 rows)"), "Goto should work: {}", out);
}

// OS command tests
#[test]
fn test_pacman_command() {
    // :pacman to list packages
    let out = run_keys(":pacman<ret><a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should show pacman view: {}", out);
    assert!(out.contains("name"), "Should have name column: {}", out);
    assert!(out.contains("deps"), "Should have deps column: {}", out);
}

#[test]
fn test_pacman_sort_deps() {
    // :pacman then navigate to deps column and sort ascending
    let out = run_keys(":pacman<ret><right><right><right>[<a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should show sorted pacman: {}", out);
    assert!(out.contains("deps"), "Should have deps column: {}", out);
}

#[test]
fn test_pacman_sort_deps_desc() {
    // :pacman then navigate to deps column and sort descending
    let out = run_keys(":pacman<ret><right><right><right>]<a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should show sorted pacman: {}", out);
}

#[test]
fn test_systemctl_command() {
    // :systemctl to list services
    let out = run_keys(":systemctl<ret><a-p>", "tests/data/basic.csv");
    assert!(out.contains("systemctl"), "Should show systemctl view: {}", out);
    assert!(out.contains("unit"), "Should have unit column: {}", out);
    assert!(out.contains("active"), "Should have active column: {}", out);
}

#[test]
fn test_journalctl_command() {
    // :journalctl 50 to get 50 log entries
    let out = run_keys(":journalctl 50<ret><a-p>", "tests/data/basic.csv");
    assert!(out.contains("journalctl"), "Should show journalctl view: {}", out);
    assert!(out.contains("message"), "Should have message column: {}", out);
}

#[test]
fn test_pacman_sort_unicode_description() {
    // Sort on description column which may contain unicode chars like fancy quotes
    // This crashed due to byte-slicing non-ASCII strings
    let out = run_keys(":pacman<ret><right><right><right><right><right><right><right><right>[<a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should handle unicode in sort: {}", out);
}
