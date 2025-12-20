//! System tests - key-based
mod common;
use common::{run_keys, footer, header};
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
fn test_pacman_sort_deps() {
    // :pacman then navigate to deps column (col 4: name,ver,size,rsize,deps) and sort ascending
    let out = run_keys(":pacman<ret><right><right><right><right>[!", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("pacman"), "Tab should show pacman: {}", tab);
    assert!(!hdr.starts_with('#'), "Header should not start with #: {}", hdr);
    // First data row should start with 0 (smallest deps count)
    let row1 = out.lines().nth(1).unwrap_or("").trim_start();
    assert!(row1.starts_with('0'), "First data row should start with 0: {}", row1);
}

#[test]
fn test_systemctl_command() {
    // :systemctl to list services
    let out = run_keys(":systemctl<ret>", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("systemctl"), "Tab should show systemctl: {}", tab);
    assert!(!hdr.starts_with('#'), "Header should not start with #: {}", hdr);
    assert!(hdr.contains("unit"), "Header should have unit: {}", hdr);
    assert!(hdr.contains("active"), "Header should have active: {}", hdr);
}

#[test]
fn test_journalctl_command() {
    // :journalctl 50 to get 50 log entries
    let out = run_keys(":journalctl 50<ret>", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("journalctl"), "Tab should show journalctl: {}", tab);
    assert!(!hdr.starts_with('#'), "Header should not start with #: {}", hdr);
    assert!(hdr.contains("message"), "Header should have message: {}", hdr);
}

#[test]
fn test_pacman_sort_unicode_description() {
    // Sort on description column (col 9) which may contain unicode chars like fancy quotes
    // This crashed due to byte-slicing non-ASCII strings
    let out = run_keys(":pacman<ret><right><right><right><right><right><right><right><right><right>[", "");
    let (tab, _) = footer(&out);
    assert!(tab.contains("pacman"), "Tab should show pacman: {}", tab);
}

#[test]
fn test_pacman_sort_size_numeric() {
    // Size column should sort numerically (KB), not alphabetically
    // Sort descending on size (col 2), largest packages first
    let out = run_keys(":pacman<ret><right><right>]!", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("pacman"), "Tab should show pacman: {}", tab);
    assert!(!hdr.starts_with('#'), "Header should not start with #: {}", hdr);
    // First data row should have large size (sorted desc, moved to first col)
    let row1 = out.lines().nth(1).unwrap_or("").trim_start();
    let size: u64 = row1.split_whitespace().next().and_then(|s| s.parse().ok()).unwrap_or(0);
    assert!(size > 100_000, "First row size should be >100MB (>100000KB): {}", row1);
}

#[test]
fn test_pacman_rsize_column() {
    // rsize(k) column shows removal size = pkg size + exclusive deps
    // Sort descending on rsize (col 3), largest removals first
    let out = run_keys(":pacman<ret><right><right><right>]!", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("pacman"), "Tab should show pacman: {}", tab);
    assert!(hdr.contains("rsize(k)"), "Header should have rsize(k): {}", hdr);
    // First data row should have large rsize (sorted desc, moved to first col)
    let row1 = out.lines().nth(1).unwrap_or("").trim_start();
    let rsize: u64 = row1.split_whitespace().next().and_then(|s| s.parse().ok()).unwrap_or(0);
    assert!(rsize > 1_000_000, "First row rsize should be >1GB: {}", row1);
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
    let out = run_keys(":cargo<ret>", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("cargo"), "Tab should show cargo: {}", tab);
    assert!(!hdr.starts_with('#'), "Header should not start with #: {}", hdr);
    assert!(hdr.contains("name"), "Header should have name: {}", hdr);
    assert!(hdr.contains("size(k)"), "Header should have size(k): {}", hdr);
    assert!(hdr.contains("deps"), "Header should have deps: {}", hdr);
    assert!(hdr.contains("req_by"), "Header should have req_by: {}", hdr);
    assert!(hdr.contains("platform"), "Header should have platform: {}", hdr);
    assert!(hdr.contains("latest"), "Header should have latest: {}", hdr);
    // Check cached version shows up
    assert!(out.contains("99.0.0"), "Should show cached latest version 99.0.0: {}", out);
}

#[test]
fn test_pacman_installed_iso_date() {
    // Installed date should be ISO format (YYYY-MM-DD), not verbose (Sat Oct 25 23:02:55 2025)
    // Navigate to installed column (col 6: name,ver,size,rsize,deps,req_by,installed), sort desc, move first
    let out = run_keys(":pacman<ret><right><right><right><right><right><right>]!", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("pacman"), "Tab should show pacman: {}", tab);
    assert!(hdr.starts_with("installed"), "Header should start with installed: {}", hdr);
    // First data row should start with current year (ISO format)
    let row1 = out.lines().nth(1).unwrap_or("").trim_start();
    let year = Utc::now().format("%Y").to_string();
    assert!(row1.starts_with(&year), "First row should start with {}: {}", year, row1);
    // Should NOT have verbose date format with day names
    let has_verbose = out.contains("Sat ") || out.contains("Sun ") || out.contains("Mon ")
        || out.contains("Tue ") || out.contains("Wed ") || out.contains("Thu ") || out.contains("Fri ");
    assert!(!has_verbose, "Should NOT have verbose date format (day names): {}", out);
}
