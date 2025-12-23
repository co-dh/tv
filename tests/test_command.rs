//! Command tests (freq, sort, select, save, etc.) - key-based
mod common;
use common::{run_keys, footer, header};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

// tid() only for save test (needs unique filename)
static SAVE_ID: AtomicUsize = AtomicUsize::new(1000);
fn tid() -> usize { SAVE_ID.fetch_add(1, Ordering::SeqCst) }

// Tests use:
// - tests/data/basic.csv: a,b (1,x 2,y 3,x 4,z 5,x) - 5 rows
// - tests/data/full.csv: name,city,value,score - 6 rows
// - tests/data/numeric.csv: x,y,z - 5 rows for correlation
// - tests/data/xkey.csv: a,b,c,d - 2 rows

#[test]
fn test_save_csv() {
    let out = format!("tmp/tv_save_{}.csv", tid());
    let keys = format!(":save {}<ret>", out);
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
    let hdr = header(&out);
    assert!(!hdr.contains("city"), "Header should not have city: {}", hdr);
    assert!(hdr.contains("name"), "Header should have name: {}", hdr);
}

#[test]
fn test_delcol_single() {
    // Navigate to b column, D to delete
    let out = run_keys("<right>D", "tests/data/basic.csv");
    let hdr = header(&out);
    assert!(hdr.contains("a"), "Should have column a: {}", hdr);
    assert!(!hdr.contains("b"), "Should not have column b: {}", hdr);
}

#[test]
fn test_rename_column() {
    // ^ to rename column a to num
    let out = run_keys("^num<ret>", "tests/data/basic.csv");
    let hdr = header(&out);
    assert!(hdr.contains("num"), "Header should have num: {}", hdr);
    assert!(!hdr.contains("a"), "Header should not have a: {}", hdr);
}

#[test]
#[ignore]  // requires polars plugin
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
fn test_filter_then_sort_then_select() {
    // filter city=='NYC' (PRQL syntax), sort value, select name,value
    let out = run_keys("<backslash>city == 'NYC'<ret>[sname,value<ret>", "tests/data/full.csv");
    let (_, status) = footer(&out);
    assert!(status.ends_with("0/3"), "Filter+sort+select chain: {}", status);
}

#[test]
#[ignore]  // loading is lazy, error shows in render not in From command
fn test_load_no_plugin() {
    // Try to load file with unsupported extension
    let out = run_keys(":from test.xyz<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("no plugin"), "Status should show error: {}", status);
}

#[test]
fn test_meta_select_rows_xkey_parent() {
    // M for meta, select rows for c and d columns, Enter to xkey them in parent
    let out = run_keys("M<down><down><space><down><space><ret>", "tests/data/xkey.csv");
    // Parent table should show c,d as key columns (before |)
    let hdr: String = header(&out).chars().filter(|c| !c.is_whitespace()).collect();
    assert!(hdr.starts_with("cd|"), "Header should start with cd|: {}", hdr);
}

#[test]
fn test_toggle_key_column() {
    // ! toggles current column as key (adds to keys, not replaces)
    // Start: a,b,c,d - press ! to add a as key, l to move to b, ! to add b as key
    // Expected: a,b are both keys (col 0 and 1), cursor stays on b
    let out = run_keys("!l!", "tests/data/xkey.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("keys=2"), "status: {}", status);
    // Header should have columns in order: a,b as keys | c,d as non-keys
    let h: String = out.lines().next().unwrap_or("").chars().filter(|c| !c.is_whitespace()).collect();
    assert!(h.starts_with("ab|cd"), "header: {}", h);
}

#[test]
fn test_toggle_key_remove() {
    // Toggle key on, then toggle same column off
    // Start: a,b,c,d - press ! to add a as key, then ! again to remove a
    let out = run_keys("!!", "tests/data/xkey.csv");
    let (_, status) = footer(&out);
    // When keys=0, status shows [sel=0] without keys
    assert!(!status.contains("keys="), "should have no keys: {}", status);
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
    // After ! on city: city moves to pos 0 (key), cursor moves to 0
    // Freq groups by city only - cursor is on city after !
    let out = run_keys("<right>!F", "tests/data/full.csv");
    let (tab, _) = footer(&out);
    let hdr = header(&out);
    assert!(tab.contains("freq city"), "Tab should show freq city: {}", tab);
    assert!(!hdr.contains("name"), "Header should not contain name: {}", hdr);
}

// Test decimal precision (floats.csv: a=1.123456, b=2.654321)
#[test]
fn test_decimal_increase() {
    // Default is 3 decimals (1.123), after . should be 4 (1.1235)
    let out = run_keys(".", "tests/data/floats.csv");
    assert!(out.contains("1.1235"), "Should show 4 decimals: {}", out);
}

#[test]
fn test_decimal_decrease() {
    // Default is 3 decimals (1.123), after , should be 2 (1.12)
    let out = run_keys(",", "tests/data/floats.csv");
    assert!(out.contains("1.12"), "Should show 2 decimals: {}", out);
    assert!(!out.contains("1.123"), "Should not show 3 decimals: {}", out);
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
    // Create filter view, then S to swap back to original
    let out = run_keys(":filter a > 2<ret>S", "tests/data/basic.csv");
    let (tab, _) = footer(&out);
    // Two views: original and filtered (filter shows command)
    assert!(tab.contains("basic"), "Should show original: {}", tab);
    assert!(tab.contains("filter a > 2"), "Should show filter: {}", tab);
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
    let out = run_keys(":lr tests/data<ret><backslash>path ~= 'meta_test'<ret><ret>", "tests/data/basic.csv");
    // After enter, should open parquet (show a, b columns)
    assert!(out.contains("a") || out.contains("b"), "Should open parquet after filter: {}", out);
}

// Test sort on folder view (uses sqlite vtable)
#[test]
fn test_lr_sort_size() {
    // lr tests/data, move to size column, sort ascending
    let out = run_keys(":lr tests/data<ret><right>[", "tests/data/basic.csv");
    let lines: Vec<&str> = out.lines().collect();
    assert!(lines.len() > 2, "Should have data rows: {}", out);
    // First data row (line 1, after header line 0) should be smallest file
    let first = lines.get(1).unwrap_or(&"");
    assert!(first.contains("null_col.csv"), "Smallest file should be first after sort: {}", out);
}

/// Extract key columns from header (columns before |)
fn keys_from_header(out: &str) -> Vec<String> {
    let hdr = header(out);
    if let Some(i) = hdr.find('|') {
        hdr[..i].split_whitespace().map(String::from).collect()
    } else {
        vec![]
    }
}

// Property: toggling key on column X adds X to keys (before |)
#[test]
fn test_key_toggle_property_add() {
    // Start with no keys, toggle col 0 (Time)
    let out = run_keys("!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert_eq!(keys, vec!["Time"], "After ! on col 0: {:?}", keys);
}

// Property: toggling key twice returns to no keys
#[test]
fn test_key_toggle_property_roundtrip() {
    // Toggle on then off
    let out = run_keys("!!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert!(keys.is_empty(), "After !! should have no keys: {:?}", keys);
}

// Property: toggling multiple columns adds all to keys
#[test]
fn test_key_toggle_property_multiple() {
    // Toggle col 0, move right, toggle col 1
    let out = run_keys("!l!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert_eq!(keys.len(), 2, "Should have 2 keys: {:?}", keys);
    assert!(keys.contains(&"Time".to_string()), "Should have Time: {:?}", keys);
    assert!(keys.contains(&"Exchange".to_string()), "Should have Exchange: {:?}", keys);
}

// Property: toggling 3 columns adds all 3 to keys
#[test]
fn test_key_toggle_property_three() {
    // Toggle col 0, move right, toggle col 1, move right, toggle col 2
    let out = run_keys("!l!l!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert_eq!(keys.len(), 3, "Should have 3 keys: {:?}", keys);
}

// Property: toggle removes from middle of keys
#[test]
fn test_key_toggle_property_remove_middle() {
    // Add 3 keys, then remove the middle one
    // Toggle col 0,1,2 then go back to col 1 and toggle off
    let out = run_keys("!l!l!h!", "tests/data/nyse/1.parquet");
    let keys = keys_from_header(&out);
    assert_eq!(keys.len(), 2, "Should have 2 keys after removing middle: {:?}", keys);
    assert!(keys.contains(&"Time".to_string()), "Should have Time: {:?}", keys);
    assert!(keys.contains(&"Symbol".to_string()), "Should have Symbol: {:?}", keys);
    assert!(!keys.contains(&"Exchange".to_string()), "Should NOT have Exchange: {:?}", keys);
}

// Test numeric columns are right-aligned (shorter numbers have leading spaces)
// Covers: i8_val, u8_val, i16_val, u16_val, i32_val, u32_val, u64_val, f16_val, f32_val, etc.
#[test]
fn test_numeric_right_align() {
    // sample.parquet has various numeric columns
    let out = run_keys("", "tests/data/sample.parquet");
    let lines: Vec<&str> = out.lines().collect();
    assert!(lines.len() >= 3, "Need at least 3 lines");
    let hdr = lines[0];

    // Test multiple numeric columns - each should be right-aligned
    for col_name in ["quantity", "i8_val", "u8_val", "i16_val", "f32_val", "score"] {
        let col_pos = match hdr.find(col_name) {
            Some(p) => p,
            None => continue, // skip if column not visible
        };
        let col_width = col_name.len();
        let col_end = col_pos + col_width;

        // Check if values are right-aligned (shorter numbers have leading spaces)
        let mut found_aligned = false;
        for line in lines.iter().skip(1).take(20) {
            if line.len() > col_end {
                let val = &line[col_pos..col_end.min(line.len())];
                let trimmed = val.trim();
                // Value shorter than column width should have leading space
                if !trimmed.is_empty() && trimmed.len() < col_width && val.starts_with(' ') {
                    found_aligned = true;
                    break;
                }
            }
        }
        assert!(found_aligned, "{} should be right-aligned: header={:?}, rows={:?}",
                col_name, hdr, lines.get(1..5));
    }
}

// Test aggregate without key columns shows error
#[test]
fn test_aggregate_requires_key() {
    // Press b (aggregate) without any key columns - should show error
    let out = run_keys("b", "tests/data/basic.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("key") || status.contains("xkey"),
            "Should show key error: {}", status);
}

// Test aggregate with multiple selected columns
#[test]
fn test_aggregate_multi_col() {
    // full.csv: name,city,value,score
    // Set city as key, select value and score, aggregate with sum
    // After !: display is city | name, value, score (cursor on city = col 0)
    // Move right twice to value (col 2), space to select
    // Move right to score (col 3), space to select
    // Press b, fzf gets "sum" from test_input
    let out = run_keys("<right>!<right><right><space><right><space>bsum<ret>", "tests/data/full.csv");
    let hdr = header(&out);
    // Should have both value_sum and score_sum in header
    assert!(hdr.contains("value_sum"), "Should have value_sum: {}", hdr);
    assert!(hdr.contains("score_sum"), "Should have score_sum: {}", hdr);
}

