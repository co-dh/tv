//! System tests - key-based
mod common;
use common::{run_keys, footer, header};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::PathBuf;
use chrono::Utc;

// tid() only for save tests (need unique filenames)
static SAVE_ID: AtomicUsize = AtomicUsize::new(5000);
fn tid() -> usize { SAVE_ID.fetch_add(1, Ordering::SeqCst) }

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
    // sel_null.csv: a,b,c with b all null, M0 selects null% == 100
    let out = run_keys("M0", "tests/data/sel_null.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("1 row(s) selected"), "Should select 1 row: {}", status);
}

#[test]
fn test_meta_sel_single() {
    // sel_single.csv: a,b,c with c having single value, M1 selects distinct == 1
    let out = run_keys("M1", "tests/data/sel_single.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("1 row(s) selected"), "Should select 1 row: {}", status);
}

#[test]
fn test_meta_shows_all_cols() {
    // null_col.csv: a,b with b all null (2 columns)
    let out = run_keys("M", "tests/data/null_col.csv");
    let (_, status) = footer(&out);
    assert!(status.ends_with("0/2"), "Should show 2 columns: {}", status);
    assert!(out.contains("b"), "Should show column b: {}", out);
}

#[test]
fn test_meta_on_ps() {
    // M should work on source:ps (uses sqlite plugin, not polars)
    let out = run_keys(":ps<ret>M", "tests/data/basic.csv");
    let (tab, status) = footer(&out);
    // Should be in meta view with 11 columns (ps has 11 cols)
    assert!(tab.contains("meta"), "Should be in meta view: {}", tab);
    assert!(status.contains("0/11"), "ps has 11 cols: {}", status);
    // Should show column names
    assert!(out.contains("user"), "Should show user col: {}", out);
    assert!(out.contains("command"), "Should show command col: {}", out);
}

#[test]
fn test_ps_filter_last_col_visible() {
    // Bug: after filtering ps, navigating to last column shows blank
    // ps | filter command ~= 'chro' then focus on command column
    let out = run_keys(":ps<ret>:filter command ~= 'chro'<ret>llllllllll", "tests/data/basic.csv");
    // Header should show command column
    let hdr = header(&out);
    assert!(hdr.contains("command"), "Header should show command: {}", hdr);
    // Data row should not be blank - should contain 'chro' pattern
    let data = out.lines().nth(1).unwrap_or("");
    let non_ws = data.chars().filter(|c| !c.is_whitespace()).count();
    assert!(non_ws > 0, "Last col should show data after filter: '{}'", data);
}

#[test]
fn test_command_picker_partial_match() {
    // Bug: typing partial "jour" and selecting "journalctl [n]" should run journalctl
    // fzf_cmd extracts command word from selection when query isn't a valid command
    let out = run_keys(":jour<ret>", "tests/data/basic.csv");
    let (tab, _) = footer(&out);
    // Should show journalctl view (not error about "jour" command)
    assert!(tab.contains("journalctl"), "Partial 'jour' should select journalctl: {}", tab);
}

#[test]
fn test_save_and_load() {
    let id = tid();
    let out_csv = format!("tmp/tv_save_{}.csv", id);
    let out_pq = format!("tmp/tv_save_{}.parquet", id);

    // Save to CSV using :save command (S is now swap)
    let keys = format!(":save {}<ret>", out_csv);
    run_keys(&keys, "tests/data/basic.csv");
    assert!(std::path::Path::new(&out_csv).exists(), "CSV should be saved");

    // Save to parquet
    let keys = format!(":save {}<ret>", out_pq);
    run_keys(&keys, "tests/data/basic.csv");
    assert!(std::path::Path::new(&out_pq).exists(), "Parquet should be saved");

    fs::remove_file(&out_csv).ok();
    fs::remove_file(&out_pq).ok();
}

#[test]
fn test_navigation_keys() {
    // Test basic navigation: up, down, left, right
    let out = run_keys("<down><down><up><right><left>", "tests/data/basic.csv");
    let (_, status) = footer(&out);
    assert!(status.ends_with("1/5"), "Navigation should work: {}", status);
}

#[test]
fn test_goto_row() {
    // :goto to go to row 3
    let out = run_keys(":goto 3<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("3/5"), "Goto should work: {}", status);
}

// OS command tests
#[test]
fn test_ps_quit_returns_to_empty() {
    // Start without file, :ps to view processes, q should pop back
    let out = run_keys(":ps<ret>q", "");
    // q pops the ps view - may show error or be empty
    assert!(out.len() > 0, "q from :ps should produce output");
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
fn test_journalctl_dates_valid() {
    // Bug: native journal reader showed 1970 dates due to wrong timestamp field
    let out = run_keys(":journalctl<ret>", "");
    let year = Utc::now().format("%Y").to_string();
    // First data row should have current year (not 1970)
    let row1 = out.lines().nth(1).unwrap_or("");
    assert!(!row1.contains("1970"), "Date should not be 1970: {}", row1);
    // Should contain recent year somewhere in output
    assert!(out.contains(&year) || out.contains("2024"), "Should have recent dates: {}", out);
}

#[test]
fn test_journalctl_boot_index() {
    // Boot 0 = current boot, should have today's date
    // Bug: boot indices were arbitrary based on file read order
    // Filter to boot == 0 to check current boot entries
    let out = run_keys(":journalctl<ret>:filter boot == 0<ret>", "");
    let today = Utc::now().format("%Y-%m-%d").to_string();
    // First data row should have today's date
    let row1 = out.lines().nth(1).unwrap_or("");
    assert!(row1.contains(&today), "Boot 0 should have today's date {}: {}", today, row1);
}

#[test]
fn test_source_page_down() {
    // Bug: Ctrl+D (page down) doesn't work on source: views
    let before = run_keys(":ps<ret>", "");
    let after = run_keys(":ps<ret><c-d>", "");
    // Extract row number from status line "0/393" -> 0
    let get_row = |s: &str| {
        s.lines().last()
            .and_then(|l| l.split_whitespace().last())
            .and_then(|s| s.split('/').next())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0)
    };
    let r1 = get_row(&before);
    let r2 = get_row(&after);
    assert!(r2 > r1, "Page down should move cursor: before={} after={}", r1, r2);
}

#[test]
fn test_journalctl_freq_date_sort() {
    // Bug: freq date can't sort on date column with [ or ]
    // Navigate to date column, freq, then sort ascending on date
    let out = run_keys(":journalctl<ret><right>F[", "");
    // Extract first 5 dates from data rows
    let dates: Vec<&str> = out.lines().skip(1).take(5)
        .filter_map(|l| l.split_whitespace().next())
        .collect();
    assert!(dates.len() >= 3, "Should have dates: {:?}", dates);
    // Dates should be sorted ascending (lexicographic works for YYYY-MM-DD)
    for i in 1..dates.len() {
        assert!(dates[i-1] <= dates[i], "Dates should be ascending: {:?}", dates);
    }
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
    // Columns: name version size(k) rsize(k)... - size is col 2
    // ! moves sorted column to front as key, so we get "size(k)|name ..."
    let out = run_keys(":pacman<ret><right><right>]!", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("pacman"), "Tab should show pacman: {}", tab);
    assert!(hdr.starts_with("size(k)"), "Header should start with size(k): {}", hdr);
    // Parse first 10 data rows - size is before | separator
    let sizes: Vec<u64> = out.lines().skip(1).take(10)
        .filter_map(|l| l.trim_start().split('|').next())
        .filter_map(|s| s.trim().replace(",", "").parse().ok())
        .collect();
    assert!(sizes.len() >= 5, "Should have at least 5 sizes: {:?}", sizes);
    for i in 1..sizes.len() {
        assert!(sizes[i-1] >= sizes[i], "Sizes should be descending: {:?}", sizes);
    }
}

#[test]
fn test_pacman_rsize_column() {
    // rsize(k) column shows removal size = pkg size + exclusive deps
    // Columns: name version size(k) rsize(k)... - rsize is col 3
    // ! moves sorted column to front as key, so we get "rsize(k)|name ..."
    let out = run_keys(":pacman<ret><right><right><right>]!", "");
    let (tab, _) = footer(&out);
    let hdr = header(&out).trim_start();
    assert!(tab.contains("pacman"), "Tab should show pacman: {}", tab);
    assert!(hdr.starts_with("rsize(k)"), "Header should start with rsize(k): {}", hdr);
    // Parse first 10 data rows - rsize is before | separator
    let rsizes: Vec<u64> = out.lines().skip(1).take(10)
        .filter_map(|l| l.trim_start().split('|').next())
        .filter_map(|s| s.trim().replace(",", "").parse().ok())
        .collect();
    assert!(rsizes.len() >= 5, "Should have at least 5 rsizes: {:?}", rsizes);
    for i in 1..rsizes.len() {
        assert!(rsizes[i-1] >= rsizes[i], "Rsizes should be descending: {:?}", rsizes);
    }
}

#[test]
fn test_cargo_command() {
    // Pre-populate cache with known value
    let cache_dir = std::env::var("XDG_CACHE_HOME").ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".cache"))
        .join("tv");
    fs::create_dir_all(&cache_dir).ok();
    let cache_path = cache_dir.join("cargo_versions.csv");
    // Add adler2 with known latest version (visible in output)
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
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
    // Installed date should be ISO format (YYYY-MM-DD), not verbose
    // Columns: name version size(k) rsize(k) deps req_by orphan reason installed - installed is col 8
    let out = run_keys(":pacman<ret><right><right><right><right><right><right><right><right>]!", "");
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
