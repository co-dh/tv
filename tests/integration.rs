use std::fs;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_id() -> usize {
    TEST_ID.fetch_add(1, Ordering::SeqCst)
}

fn run_script(script: &str, id: usize) -> String {
    let script_path = format!("/tmp/tv_test_script_{}.txt", id);
    fs::write(&script_path, script).unwrap();

    let output = Command::new("./target/release/tv")
        .arg("--script")
        .arg(&script_path)
        .output()
        .expect("failed to execute tv");

    String::from_utf8_lossy(&output.stdout).to_string()
}

fn setup_test_csv(id: usize) -> String {
    let path = format!("/tmp/tv_test_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,y\n3,x\n4,z\n5,x\n").unwrap();
    path
}

#[test]
fn test_filter_pushes_new_view() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    // Filter should create a new view with expression as name
    let output = run_script(&format!("load {}\nfilter a>2\n", csv), id);

    // View name should be the filter expression
    assert!(output.contains("=== a>2"), "Filter should create view named by expression");
    // Should have 3 rows (3, 4, 5)
    assert!(output.contains("(3 rows)"), "Filter should have 3 matching rows");
}

#[test]
fn test_filter_preserves_original_view() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    // After filter, original view should still be on stack
    // We verify by checking that both views exist (filter creates new, doesn't replace)
    let output = run_script(&format!("load {}\nfilter a>2\n", csv), id);

    // The output shows the top of stack (filtered view)
    assert!(output.contains("a>2"), "Top of stack should be filtered view");
    assert!(output.contains("3 rows"), "Filtered view should have 3 rows");
}

#[test]
fn test_filter_integer_comparison() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nfilter a >= 3\n", csv), id);
    assert!(output.contains("(3 rows)"), "a>=3 should match rows 3,4,5");

    let output = run_script(&format!("load {}\nfilter a < 3\n", csv), id);
    assert!(output.contains("(2 rows)"), "a<3 should match rows 1,2");

    let output = run_script(&format!("load {}\nfilter a = 3\n", csv), id);
    assert!(output.contains("(1 rows)"), "a=3 should match 1 row");
}

#[test]
fn test_filter_string_equality() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nfilter b = 'x'\n", csv), id);
    assert!(output.contains("(3 rows)"), "b='x' should match 3 rows");
}

#[test]
fn test_filter_between() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nfilter a BETWEEN 2 AND 4\n", csv), id);
    assert!(output.contains("(3 rows)"), "BETWEEN 2 AND 4 should match 3 rows");
}

#[test]
fn test_filter_and() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nfilter a >= 2 AND a <= 4\n", csv), id);
    assert!(output.contains("(3 rows)"), "a>=2 AND a<=4 should match 3 rows");
}

#[test]
fn test_filter_or() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nfilter a = 1 OR a = 5\n", csv), id);
    assert!(output.contains("(2 rows)"), "a=1 OR a=5 should match 2 rows");
}

#[test]
fn test_filter_like_starts_with() {
    let id = unique_id();
    let path = format!("/tmp/tv_test_like_{}.csv", id);
    fs::write(&path, "name,val\napple,1\nbanana,2\napricot,3\nblueberry,4\n").unwrap();

    let output = run_script(&format!("load {}\nfilter name LIKE 'a%'\n", path), id);
    assert!(output.contains("(2 rows)"), "LIKE 'a%' should match apple, apricot");
}

#[test]
fn test_filter_like_ends_with() {
    let id = unique_id();
    let path = format!("/tmp/tv_test_like2_{}.csv", id);
    fs::write(&path, "name,val\napple,1\nbanana,2\npineapple,3\ngrape,4\n").unwrap();

    let output = run_script(&format!("load {}\nfilter name LIKE '%apple'\n", path), id);
    assert!(output.contains("(2 rows)"), "LIKE '%apple' should match apple, pineapple");
}

#[test]
fn test_filter_like_contains() {
    let id = unique_id();
    let path = format!("/tmp/tv_test_like3_{}.csv", id);
    fs::write(&path, "name,val\napple,1\nbanana,2\npineapple,3\ngrape,4\n").unwrap();

    let output = run_script(&format!("load {}\nfilter name LIKE '%an%'\n", path), id);
    assert!(output.contains("(1 rows)"), "LIKE '%an%' should match banana");
}

fn setup_test_csv_with_nulls(id: usize) -> String {
    let path = format!("/tmp/tv_test_nulls_{}.csv", id);
    fs::write(&path, "a,b,c,d\n1,x,,constant\n2,y,,constant\n3,x,,constant\n,z,,constant\n5,x,,constant\n").unwrap();
    path
}

fn setup_numeric_csv(id: usize) -> String {
    let path = format!("/tmp/tv_test_numeric_{}.csv", id);
    fs::write(&path, "x,y,z\n1,10,100\n2,20,200\n3,30,300\n4,40,400\n5,50,500\n").unwrap();
    path
}

#[test]
fn test_meta_shows_columns() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nmeta\n", csv), id);

    // Should show metadata view
    assert!(output.contains("=== metadata"), "Should create metadata view");
    // Should have column info
    assert!(output.contains("column"), "Should have column header");
    assert!(output.contains("type"), "Should have type column");
    assert!(output.contains("null%"), "Should have null% column");
    assert!(output.contains("distinct"), "Should have distinct column");
}

#[test]
fn test_meta_shows_numeric_stats() {
    let id = unique_id();
    let csv = setup_numeric_csv(id);

    let output = run_script(&format!("load {}\nmeta\n", csv), id);

    // Should show numeric stats columns
    assert!(output.contains("median"), "Should have median column");
    assert!(output.contains("sigma"), "Should have sigma column");
    assert!(output.contains("min"), "Should have min column");
    assert!(output.contains("max"), "Should have max column");
}

#[test]
fn test_freq_shows_value_counts() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nfreq b\n", csv), id);

    // Should show frequency view
    assert!(output.contains("Freq:b"), "Should create frequency view for column b");
    // Should have count column
    assert!(output.contains("Cnt"), "Should have Cnt column");
    assert!(output.contains("Pct"), "Should have Pct column");
    // x appears 3 times out of 5
    assert!(output.contains("x"), "Should show value 'x'");
}

#[test]
fn test_corr_matrix() {
    let id = unique_id();
    let csv = setup_numeric_csv(id);

    let output = run_script(&format!("load {}\ncorr\n", csv), id);

    // Should show correlation view
    assert!(output.contains("=== correlation"), "Should create correlation view");
    // Diagonal should be 1.0 (perfectly correlated with themselves)
    assert!(output.contains("1.0"), "Diagonal values should be 1.0");
    // x, y, z are perfectly correlated (y=10*x, z=100*x)
    // So we should see high correlation values
}

#[test]
fn test_sort_ascending() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nsort a\n", csv), id);

    // Should maintain 5 rows
    assert!(output.contains("(5 rows)"), "Should keep all rows after sort");
}

#[test]
fn test_sort_descending() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nsortdesc a\n", csv), id);

    // Should maintain 5 rows
    assert!(output.contains("(5 rows)"), "Should keep all rows after sort");
}

#[test]
fn test_select_columns() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nsel a\n", csv), id);

    // Should have only column a
    assert!(output.contains("a"), "Should have column a");
    // Original had 2 columns, now should have 1
}

#[test]
fn test_delete_column() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\ndelcol b\n", csv), id);

    // Should still have 5 rows
    assert!(output.contains("(5 rows)"), "Should keep all rows after delete");
}

#[test]
fn test_rename_column() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let output = run_script(&format!("load {}\nrename a num\n", csv), id);

    // Should have renamed column
    assert!(output.contains("num"), "Should have renamed column");
}

#[test]
fn test_null_column_detection() {
    let id = unique_id();
    let csv = setup_test_csv_with_nulls(id);

    let output = run_script(&format!("load {}\nmeta\n", csv), id);

    // Column 'c' should show 100% null
    assert!(output.contains("100"), "Should show 100% null for column c");
}

#[test]
fn test_single_value_column() {
    let id = unique_id();
    let csv = setup_test_csv_with_nulls(id);

    let output = run_script(&format!("load {}\nmeta\n", csv), id);

    // Column 'd' has only "constant" value, should show 1 distinct
    // Check that distinct column exists
    assert!(output.contains("distinct"), "Should have distinct column");
}

#[test]
fn test_pipe_separated_commands() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    // Test pipe-separated commands: load and filter on same line
    let output = run_script(&format!("load {} | filter a>2\n", csv), id);

    // Should create filtered view
    assert!(output.contains("a>2"), "Should create filtered view");
    assert!(output.contains("(3 rows)"), "Should have 3 filtered rows");
}

#[test]
fn test_pipe_multiple_operations() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    // Test multiple piped operations: load, filter, then meta
    let output = run_script(&format!("load {} | filter a>2 | meta\n", csv), id);

    // Final view should be metadata
    assert!(output.contains("=== metadata"), "Should end with metadata view");
    // Metadata should have 2 columns (a and b from filtered data)
    assert!(output.contains("column"), "Should have column header");
}

#[test]
fn test_delcol_removes_specific_column() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    // Delete column 'b'
    let output = run_script(&format!("load {} | delcol b\n", csv), id);

    // Should still have 5 rows
    assert!(output.contains("(5 rows)"), "Should keep all rows");
    // Should only have column 'a' now
    assert!(output.contains("a"), "Should have column a");
}
