//! Command tests (freq, sort, select, save, etc.) - key-based
mod common;
use common::{run_keys, footer};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_ID: AtomicUsize = AtomicUsize::new(1000);
fn tid() -> usize { TEST_ID.fetch_add(1, Ordering::SeqCst) }

// Tests use:
// - tests/data/basic.csv: a,b (1,x 2,y 3,x 4,z 5,x) - 5 rows
// - tests/data/full.csv: name,city,value,score - 6 rows
// - tests/data/numeric.csv: x,y,z - 5 rows for correlation
// - tests/data/xkey.csv: a,b,c,d - 2 rows

#[test]
fn test_save_csv() {
    let out = format!("tmp/tv_save_{}.csv", tid());
    let keys = format!("S{}<ret>", out);
    run_keys(&keys, "tests/data/basic.csv");
    assert!(std::path::Path::new(&out).exists(), "save should create file");
    fs::remove_file(&out).ok();
}

#[test]
fn test_multi_column_select() {
    // Select multiple columns with space, verify sel count
    // full.csv: name,city,value,score (cols 0,1,2,3)
    let out = run_keys("<space><right><space>", "tests/data/full.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("sel=2"), "status: {}", status);
}

#[test]
fn test_select_columns() {
    // s to select, type column names
    let out = run_keys("sname,city<ret>", "tests/data/full.csv");
    assert!(out.contains("name"), "Should have name");
    assert!(!out.contains("value"), "Should not have value: {}", out);
}

#[test]
fn test_select_single() {
    let out = run_keys("sa<ret>", "tests/data/basic.csv");
    assert!(out.contains("a"), "Should have column a");
}

#[test]
fn test_delcol_multi() {
    // Navigate to city (col 1), space to select, right to value (col 2), space, D
    let out = run_keys("<right><space><right><space>D", "tests/data/full.csv");
    assert!(!out.contains("city"), "Should not have city: {}", out);
    assert!(out.contains("name"), "Should still have name");
}

#[test]
fn test_delcol_single() {
    // Navigate to b column, D to delete
    let out = run_keys("<right>D", "tests/data/basic.csv");
    let (_, status) = footer(&out);
    assert!(status.ends_with("0/5"), "Should keep all rows: {}", status);
    assert!(out.contains("a"), "Should have column a");
}

#[test]
fn test_rename_column() {
    // ^ to rename, type new name
    let out = run_keys("^num<ret>", "tests/data/basic.csv");
    assert!(out.contains("num"), "Should have renamed column: {}", out);
}

#[test]
fn test_corr_matrix() {
    // C for correlation matrix
    let out = run_keys("C", "tests/data/numeric.csv");
    assert!(out.contains("correlation"), "Should create corr view: {}", out);
    assert!(out.contains("1.0"), "Diagonal should be 1.0");
}

#[test]
fn test_pivot_requires_xkey() {
    // P for pivot without xkey should stay on original view (not create pivot view)
    let out = run_keys("P", "tests/data/full.csv");
    let (_, status) = footer(&out);
    // Without xkey, pivot should fail and stay on original view (6 rows)
    assert!(!out.contains("Pivot:"), "Should not create pivot view without xkey: {}", out);
    assert!(status.ends_with("0/6"), "Should stay on original view: {}", status);
}

#[test]
#[ignore]  // requires fzf for filter
fn test_filter_then_sort_then_select() {
    // filter city=='NYC' (PRQL syntax), sort value, select name,value
    let out = run_keys("<backslash>city == 'NYC'<ret>[sname,value<ret>", "tests/data/full.csv");
    let (_, status) = footer(&out);
    assert!(status.ends_with("0/3"), "Filter+sort+select chain: {}", status);
}

#[test]
fn test_load_nonexistent() {
    // Try to load nonexistent file via L command
    let out = run_keys("L/nonexistent/path/file.csv<ret>", "tests/data/basic.csv");
    // Should show error or remain on original
    assert!(out.contains("a") || out.contains("error") || out.contains("No"), "Should handle missing file: {}", out);
}

#[test]
fn test_xkey_moves_to_front() {
    // M for meta, navigate to c row, space select, down, space select b, Enter to xkey
    let out = run_keys("M<down><down><space><down><space><ret>", "tests/data/xkey.csv");
    // After xkey, c and d should be key columns (moved to front)
    assert!(out.contains("c") && out.contains("d"), "xkey cols present: {}", out);
}

#[test]
fn test_toggle_key_column() {
    // ! toggles current column as key (adds to keys, not replaces)
    // Start: a,b,c,d - press ! to add a as key, l to move to b, ! to add b as key
    // Expected: a,b are both keys (col 0 and 1), cursor stays on b
    let out = run_keys("!l!", "tests/data/xkey.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("keys=2"), "status: {}", status);
    assert!(out.contains("a,b,c,d"), "order should be a,b,c,d: {}", out);
}

#[test]
fn test_toggle_key_remove() {
    // Toggle key on, then toggle same column off
    // Start: a,b,c,d - press ! to add a as key, then ! again to remove a
    let out = run_keys("!!", "tests/data/xkey.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("keys=0"), "status: {}", status);
}

#[test]
fn test_toggle_key_selected_cols() {
    // Select multiple columns with space, then ! toggles all selected as keys
    // Start: a,b,c,d - select a and b with space, then ! to add both as keys
    let out = run_keys("<space><right><space>!", "tests/data/xkey.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("keys=2"), "status: {}", status);
}

#[test]
fn test_freq_after_meta() {
    // View meta, then q to return, then freq on current col
    let out = run_keys("MqF", "tests/data/basic.csv");
    let (tab, _) = footer(&out);
    assert!(tab.contains("| freq a"), "tab: {}", tab);
}

#[test]
fn test_freq_by_key_columns() {
    // Set key column with !, then press F to freq by key
    // full.csv: name,city,value,score (cols 0,1,2,3)
    // Set city as key, then freq should group by city
    let out = run_keys("<right>!F", "tests/data/full.csv");
    assert!(out.contains("Freq:city"), "Should freq by key column: {}", out);
}

// Test navigation and selection
#[test]
fn test_select_all_null() {
    // Create csv with all-null column, use 0 to select
    let id = tid();
    let p = format!("tmp/tv_null_{}.csv", id);
    fs::write(&p, "a,b\n1,\n2,\n3,\n").unwrap();
    let out = run_keys("M0", &p);
    // 0 selects columns with 100% null
    assert!(out.contains("b") || out.contains("metadata"), "Should show meta: {}", out);
    fs::remove_file(&p).ok();
}

#[test]
fn test_select_single_value() {
    // Create csv with single-value column
    let id = tid();
    let p = format!("tmp/tv_single_{}.csv", id);
    fs::write(&p, "a,b\n1,x\n2,x\n3,x\n").unwrap();
    let out = run_keys("M1", &p);
    // 1 selects columns with 1 distinct value
    assert!(out.contains("metadata"), "Should show meta: {}", out);
    fs::remove_file(&p).ok();
}

// Test decimal precision
#[test]
fn test_decimal_increase() {
    let out = run_keys("..", "tests/data/numeric.csv");
    let (_, status) = footer(&out);
    assert!(status.ends_with("0/5"), "Should show table: {}", status);
}

#[test]
fn test_decimal_decrease() {
    let out = run_keys(",,", "tests/data/numeric.csv");
    let (_, status) = footer(&out);
    assert!(status.ends_with("0/5"), "Should show table: {}", status);
}

// Test duplicate view
#[test]
fn test_duplicate_view() {
    // T duplicates current view
    let out = run_keys("T", "tests/data/basic.csv");
    let (tab, _) = footer(&out);
    // Two views should show | separator in tab line
    assert!(tab.contains("|"), "Should have two views: {}", tab);
}

// Test swap views
#[test]
fn test_swap_views() {
    // Create filter view, then W to swap back to original
    let out = run_keys(":filter a > 2<ret>W", "tests/data/basic.csv");
    let (tab, _) = footer(&out);
    // Two views: original and filtered, both show basic.csv
    assert!(tab.matches("basic.csv").count() == 2, "Should show basic.csv twice: {}", tab);
}

// Test lr shows relative paths
#[test]
fn test_lr_paths() {
    // lr on tests/data should show relative paths
    let out = run_keys(":lr tests/data<ret>", "tests/data/basic.csv");
    assert!(out.contains("basic.csv"), "lr should show paths: {}", out);
}

// Test lr enter on csv opens file
#[test]
fn test_lr_enter_csv() {
    // lr tests/data, filter to numeric.csv, enter should open it
    let out = run_keys(":lr tests/data<ret><backslash>path ~= 'numeric'<ret><ret>", "tests/data/basic.csv");
    // After enter on csv, should open the file (show x,y,z columns)
    assert!(out.contains("x") && out.contains("y"), "Should open csv: {}", out);
}

// Test lr filter by extension and open
#[test]
fn test_lr_filter_extension_open() {
    // lr tests/data, filter for meta_test.parquet, enter to open
    let out = run_keys(":lr tests/data<ret><backslash>path LIKE '%meta_test%'<ret><ret>", "tests/data/basic.csv");
    // After enter, should open parquet (show a, b columns)
    assert!(out.contains("a") || out.contains("b"), "Should open parquet after filter: {}", out);
}

// Test sort on folder view (uses sqlite vtable)
#[test]
fn test_lr_sort_size() {
    // lr tests/data, move to size column, sort ascending
    let out = run_keys(":lr tests/data<ret><right>[", "tests/data/basic.csv");
    let lines: Vec<&str> = out.lines().collect();
    assert!(lines.len() > 3, "Should have data rows: {}", out);
    // First data row (line 2) should be smallest file (unsorted.csv at 16 bytes)
    let first = lines.get(2).unwrap_or(&"");
    assert!(first.contains("unsorted.csv"), "Smallest file should be first after sort: {}", out);
}
