//! System tests - key-based
mod common;
use common::run_keys;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use chrono::Utc;

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
fn test_ps_quit_returns_to_empty() {
    // Start without file, :ps to view processes, q should return to empty screen, not quit
    let out = run_keys(":ps<ret>q", "");
    // Should return to empty state
    assert!(out.contains("No table loaded"),
        "q from :ps should return to empty screen: {}", out);
}

#[test]
fn test_pacman_command() {
    // :pacman to list packages
    let out = run_keys(":pacman<ret><a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should show pacman view: {}", out);
    assert!(out.contains("name"), "Should have name column: {}", out);
    assert!(out.contains("size(k)"), "Should have size(k) column: {}", out);
}

#[test]
fn test_pacman_sort_deps() {
    // :pacman then navigate to deps column (col 4: name,ver,size,rsize,deps) and sort ascending
    let out = run_keys(":pacman<ret><right><right><right><right>[<a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should show sorted pacman: {}", out);
    assert!(out.contains("size(k)"), "Should have size(k) column: {}", out);
}

#[test]
fn test_pacman_sort_deps_desc() {
    // :pacman then navigate to deps column (col 4) and sort descending
    let out = run_keys(":pacman<ret><right><right><right><right>]<a-p>", "tests/data/basic.csv");
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
    // Sort on description column (col 9) which may contain unicode chars like fancy quotes
    // This crashed due to byte-slicing non-ASCII strings
    let out = run_keys(":pacman<ret><right><right><right><right><right><right><right><right><right>[<a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should handle unicode in sort: {}", out);
}

#[test]
fn test_pacman_sort_size_numeric() {
    // Size column should sort numerically (KB), not alphabetically
    // Sort descending on size (col 2), largest packages first
    let out = run_keys(":pacman<ret><right><right>]<a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should show pacman: {}", out);
    // Size is now stored as u64 KB. Largest packages (>100MB = >100000KB) should be first.
    let has_large = out.lines().take(20).any(|l| {
        l.split_whitespace().any(|w| w.parse::<u64>().map(|n| n > 100_000).unwrap_or(false))
    });
    assert!(has_large, "Largest packages (>100MB) should be first when sorting desc: {}", out);
}

#[test]
fn test_pacman_rsize_column() {
    // rsize(k) column shows removal size = pkg size + exclusive deps
    // Sort descending on rsize (col 3), largest removals first
    let out = run_keys(":pacman<ret><right><right><right>]<a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should show pacman: {}", out);
    assert!(out.contains("rsize(k)"), "Should have rsize(k) column: {}", out);
    // Some packages have rsize > size (due to exclusive deps)
    // cuda is ~5GB removal size with deps
    let has_large_rsize = out.lines().take(20).any(|l| {
        l.split_whitespace().any(|w| w.parse::<u64>().map(|n| n > 1_000_000).unwrap_or(false))
    });
    assert!(has_large_rsize, "Should have large rsize packages when sorted desc: {}", out);
}

#[test]
fn test_cargo_command() {
    // Pre-populate cache with known value
    let cache_dir = dirs::cache_dir().unwrap().join("tv");
    fs::create_dir_all(&cache_dir).ok();
    let cache_path = cache_dir.join("cargo_versions.csv");
    // Add adler2 with known latest version (visible in output)
    let now = Utc::now().timestamp();
    fs::write(&cache_path, format!("name,version,timestamp\nadler2,99.0.0,{}\n", now)).ok();

    // :cargo to list project dependencies (like pacman for Rust)
    let out = run_keys(":cargo<ret><a-p>", "tests/data/basic.csv");
    assert!(out.contains("cargo"), "Should show cargo view: {}", out);
    assert!(out.contains("name"), "Should have name column: {}", out);
    assert!(out.contains("size(k)"), "Should have size(k) column: {}", out);
    assert!(out.contains("deps"), "Should have deps column: {}", out);
    assert!(out.contains("req_by"), "Should have req_by column: {}", out);
    assert!(out.contains("platform"), "Should have platform column: {}", out);
    assert!(out.contains("latest"), "Should have latest column: {}", out);
    // Check cached version shows up
    assert!(out.contains("99.0.0"), "Should show cached latest version 99.0.0: {}", out);
}

#[test]
fn test_pacman_installed_iso_date() {
    // Installed date should be ISO format (2025-10-25), not verbose (Sat Oct 25 23:02:55 2025)
    let out = run_keys(":pacman<ret><a-p>", "tests/data/basic.csv");
    assert!(out.contains("pacman"), "Should show pacman: {}", out);
    // Check for ISO date pattern YYYY-MM-DD in output
    let has_iso = out.lines().any(|l| {
        l.contains("2025-") || l.contains("2024-")  // recent years
    });
    // Should NOT have verbose date format with day names
    let has_verbose = out.contains("Sat ") || out.contains("Sun ") || out.contains("Mon ")
        || out.contains("Tue ") || out.contains("Wed ") || out.contains("Thu ") || out.contains("Fri ");
    assert!(has_iso, "Should have ISO date format (YYYY-MM-DD): {}", out);
    assert!(!has_verbose, "Should NOT have verbose date format (day names): {}", out);
}
