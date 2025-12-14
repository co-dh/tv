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

#[test]
fn test_filter_in_single_value() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    // Filter using IN clause (what freq Enter does)
    let output = run_script(&format!("load {} | filter b IN ('x')\n", csv), id);
    assert!(output.contains("(3 rows)"), "b IN ('x') should match 3 rows");
}

#[test]
fn test_filter_in_multiple_values() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    // Filter with multiple values (freq multi-select)
    let output = run_script(&format!("load {} | filter b IN ('x','y')\n", csv), id);
    assert!(output.contains("(4 rows)"), "b IN ('x','y') should match 4 rows");
}

#[test]
fn test_freq_then_filter_workflow() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    // Simulate freq->Enter workflow: freq shows counts, filter applies to parent
    // First verify freq works
    let freq_output = run_script(&format!("load {} | freq b\n", csv), id);
    assert!(freq_output.contains("Freq:b"), "Should create freq view");
    assert!(freq_output.contains("x"), "Should show value x");

    // Then verify filtering works on parent data
    let filter_output = run_script(&format!("load {} | filter b IN ('x')\n", csv), id);
    assert!(filter_output.contains("(3 rows)"), "Should filter to 3 rows where b='x'");
    assert!(filter_output.contains("b"), "Filtered view should have column b");
}

// === Additional filter tests (from test_string_filter.sh) ===

fn setup_strings_csv(id: usize) -> String {
    let path = format!("/tmp/tv_test_strings_{}.csv", id);
    fs::write(&path, "name,value\napple,10\nbanana,20\ncherry,30\npineapple,40\ngrape,50\nblueberry,60\n").unwrap();
    path
}

#[test]
fn test_filter_numeric_gt() {
    let id = unique_id();
    let csv = setup_strings_csv(id);
    let output = run_script(&format!("load {} | filter value > 30\n", csv), id);
    assert!(output.contains("(3 rows)"), "value > 30 should match 3 rows");
}

#[test]
fn test_filter_numeric_gte() {
    let id = unique_id();
    let csv = setup_strings_csv(id);
    let output = run_script(&format!("load {} | filter value >= 30\n", csv), id);
    assert!(output.contains("(4 rows)"), "value >= 30 should match 4 rows");
}

#[test]
fn test_filter_numeric_lt() {
    let id = unique_id();
    let csv = setup_strings_csv(id);
    let output = run_script(&format!("load {} | filter value < 30\n", csv), id);
    assert!(output.contains("(2 rows)"), "value < 30 should match 2 rows");
}

#[test]
fn test_filter_numeric_lte() {
    let id = unique_id();
    let csv = setup_strings_csv(id);
    let output = run_script(&format!("load {} | filter value <= 30\n", csv), id);
    assert!(output.contains("(3 rows)"), "value <= 30 should match 3 rows");
}

#[test]
fn test_filter_combined_string_and_numeric() {
    let id = unique_id();
    let csv = setup_strings_csv(id);
    let output = run_script(&format!("load {} | filter name LIKE 'b%' AND value > 30\n", csv), id);
    assert!(output.contains("(1 rows)"), "name LIKE 'b%' AND value > 30 should match 1 row (blueberry)");
}

// === Command tests (from test_commands.sh) ===

fn setup_full_csv(id: usize) -> String {
    let path = format!("/tmp/tv_test_full_{}.csv", id);
    fs::write(&path, "name,city,value,score\nAlice,NYC,100,85\nBob,LA,200,90\nCarol,NYC,150,75\nDave,Chicago,300,95\nEve,LA,250,80\nFrank,NYC,175,\n").unwrap();
    path
}

#[test]
fn test_save_command() {
    let id = unique_id();
    let csv = setup_full_csv(id);
    let out_path = format!("/tmp/tv_test_out_{}.csv", id);
    run_script(&format!("load {} | save {}\n", csv, out_path), id);
    assert!(std::path::Path::new(&out_path).exists(), "save should create output file");
}

#[test]
fn test_freq_city() {
    let id = unique_id();
    let csv = setup_full_csv(id);
    let output = run_script(&format!("load {} | freq city\n", csv), id);
    assert!(output.contains("(3 rows)"), "freq city should have 3 unique values");
}

#[test]
fn test_filter_numeric_value() {
    let id = unique_id();
    let csv = setup_full_csv(id);
    let output = run_script(&format!("load {} | filter value > 200\n", csv), id);
    assert!(output.contains("(2 rows)"), "value > 200 should match 2 rows");
}

#[test]
fn test_filter_string_city() {
    let id = unique_id();
    let csv = setup_full_csv(id);
    let output = run_script(&format!("load {} | filter city = 'NYC'\n", csv), id);
    assert!(output.contains("(3 rows)"), "city = 'NYC' should match 3 rows");
}

#[test]
fn test_select_removes_columns() {
    let id = unique_id();
    let csv = setup_full_csv(id);
    let output = run_script(&format!("load {} | sel name,city\n", csv), id);
    assert!(output.contains("name"), "Should have column name");
    assert!(!output.contains("value"), "Should not have column value");
}

#[test]
fn test_delcol_multi() {
    let id = unique_id();
    let csv = setup_full_csv(id);
    let output = run_script(&format!("load {} | delcol city,score\n", csv), id);
    assert!(!output.contains("city"), "Should not have column city");
    assert!(!output.contains("score"), "Should not have column score");
    assert!(output.contains("name"), "Should still have column name");
}

#[test]
fn test_pipe_chain() {
    let id = unique_id();
    let csv = setup_full_csv(id);
    let output = run_script(&format!("load {} | filter city = 'NYC' | sort value | sel name,value\n", csv), id);
    assert!(output.contains("(3 rows)"), "Pipe chain should work");
}

// === ls tests (from test_ls.sh) ===

#[test]
fn test_ls_shows_directory() {
    let id = unique_id();
    let dir = format!("/tmp/tv_test_dir_{}", id);
    fs::create_dir_all(format!("{}/subdir", dir)).unwrap();
    fs::write(format!("{}/file.txt", dir), "test").unwrap();

    let output = run_script(&format!("ls {}\n", dir), id);
    assert!(output.contains("subdir"), "ls should show subdir");
    assert!(output.contains("file.txt"), "ls should show file");
}

#[test]
fn test_ls_dir_column_x_for_dirs() {
    let id = unique_id();
    let dir = format!("/tmp/tv_test_dir2_{}", id);
    fs::create_dir_all(format!("{}/subdir", dir)).unwrap();
    fs::write(format!("{}/file.txt", dir), "test").unwrap();

    let output = run_script(&format!("ls {}\n", dir), id);
    // subdir line should contain 'x' in dir column
    for line in output.lines() {
        if line.contains("subdir") {
            assert!(line.contains("x"), "dir column should be 'x' for directories");
        }
    }
}

#[test]
fn test_ls_recursive() {
    let id = unique_id();
    let dir = format!("/tmp/tv_test_dir3_{}", id);
    fs::create_dir_all(format!("{}/subdir", dir)).unwrap();
    fs::write(format!("{}/subdir/nested.txt", dir), "test").unwrap();

    let output = run_script(&format!("ls -r {}\n", dir), id);
    assert!(output.contains("subdir"), "ls -r should show subdir");
    assert!(output.contains("nested.txt"), "ls -r should show nested file");
}

#[test]
fn test_ls_parent_dir_entry() {
    let id = unique_id();
    let dir = format!("/tmp/tv_test_parent_{}", id);
    fs::create_dir_all(&dir).unwrap();

    let output = run_script(&format!("ls {}\n", dir), id);
    // First row should be ".." with parent path
    assert!(output.contains(".."), "ls should show .. entry for parent");
    assert!(output.contains("/tmp"), ".. should point to parent directory");
}

#[test]
fn test_ls_sorted_by_name() {
    let id = unique_id();
    let dir = format!("/tmp/tv_test_sort_{}", id);
    fs::create_dir_all(&dir).unwrap();
    // Create files in non-alphabetical order
    fs::write(format!("{}/zebra.txt", dir), "z").unwrap();
    fs::write(format!("{}/apple.txt", dir), "a").unwrap();
    fs::write(format!("{}/mango.txt", dir), "m").unwrap();

    let output = run_script(&format!("ls {}\n", dir), id);
    // Find positions of each file in output
    let apple_pos = output.find("apple.txt").unwrap();
    let mango_pos = output.find("mango.txt").unwrap();
    let zebra_pos = output.find("zebra.txt").unwrap();
    // Should be sorted alphabetically
    assert!(apple_pos < mango_pos, "apple should come before mango");
    assert!(mango_pos < zebra_pos, "mango should come before zebra");
}

#[test]
fn test_load_ragged_csv_truncates() {
    let id = unique_id();
    let path = format!("/tmp/tv_ragged_csv_{}.csv", id);
    // Create csv with inconsistent columns - should truncate ragged lines
    fs::write(&path, "a,b,c\n1,2\n3,4,5,6\n").unwrap();

    let output = run_script(&format!("from {}\n", path), id);
    // Should load with truncated ragged lines
    assert!(output.contains("(2 rows)"),
        "Ragged csv should load with truncation, got: {}", output);
}

#[test]
fn test_load_nonexistent_file_error() {
    let id = unique_id();
    let output = run_script("from /nonexistent/path/file.csv\n", id);
    // Should show "No table loaded" since file doesn't exist
    assert!(output.contains("No table loaded"),
        "Loading nonexistent file should result in no table");
}

#[test]
fn test_from_and_load_equivalent() {
    // Both "from" and "load" commands should work identically
    let id = unique_id();
    let path = format!("/tmp/tv_test_from_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,y\n").unwrap();

    let from_output = run_script(&format!("from {}\n", path), id + 1000);
    let load_output = run_script(&format!("load {}\n", path), id + 1001);

    assert!(from_output.contains("(2 rows)"), "'from' command should load 2 rows");
    assert!(load_output.contains("(2 rows)"), "'load' command should load 2 rows");
    assert_eq!(from_output, load_output, "'from' and 'load' should produce identical output");
}

#[test]
fn test_duckdb_sql_query() {
    // Test DuckDB SQL query via sql: prefix
    let id = unique_id();
    let output = run_script("from sql:SELECT 1 as a, 'hello' as b\n", id);
    assert!(output.contains("(1 rows)"), "DuckDB query should return 1 row");
    assert!(output.contains("hello"), "DuckDB query should contain 'hello'");
}

#[test]
fn test_duckdb_parquet_query() {
    // Test DuckDB query on parquet file
    let id = unique_id();
    let path = format!("/tmp/tv_duckdb_test_{}.parquet", id);
    // First create a parquet file
    run_script(&format!("from sql:SELECT 1 as x, 2 as y UNION SELECT 3, 4 | save {}\n", path), id);
    // Then query it via DuckDB
    let output = run_script(&format!("from sql:SELECT SUM(x) as total FROM read_parquet('{}')\n", path), id + 1);
    assert!(output.contains("total"), "DuckDB parquet query should have 'total' column");
    let _ = std::fs::remove_file(path);
}

// === meta tests (from test_meta.sh) ===

#[test]
fn test_meta_empty_not_dash() {
    let id = unique_id();
    let path = format!("/tmp/tv_test_meta_{}.csv", id);
    fs::write(&path, "num,str,empty\n1,apple,\n2,banana,\n3,,\n").unwrap();

    let output = run_script(&format!("load {} | meta\n", path), id);
    // String columns should have empty median/sigma, not "-"
    // Check that there's no " - " pattern (dash surrounded by spaces)
    let str_line = output.lines().find(|l| l.contains("│ str"));
    if let Some(line) = str_line {
        assert!(!line.contains(" - "), "str row should not contain '-' for empty values");
    }
}

// === System command tests ===

#[test]
fn test_ps_command() {
    let id = unique_id();
    let output = run_script("ps\n", id);
    assert!(output.contains("=== ps"), "ps should show view name");
    assert!(output.contains("pid"), "ps should have pid column");
    assert!(output.contains("user"), "ps should have user column");
    assert!(output.contains("command"), "ps should have command column");
}

#[test]
fn test_df_command() {
    let id = unique_id();
    let output = run_script("df\n", id);
    assert!(output.contains("=== df"), "df should show view name");
    assert!(output.contains("filesystem"), "df should have filesystem column");
    assert!(output.contains("mount"), "df should have mount column");
    assert!(output.contains("total"), "df should have total column");
}

#[test]
fn test_mounts_command() {
    let id = unique_id();
    let output = run_script("mounts\n", id);
    assert!(output.contains("=== mounts"), "mounts should show view name");
    assert!(output.contains("device"), "mounts should have device column");
    assert!(output.contains("type"), "mounts should have type column");
}

#[test]
fn test_tcp_command() {
    let id = unique_id();
    let output = run_script("tcp\n", id);
    assert!(output.contains("=== tcp"), "tcp should show view name");
    assert!(output.contains("local_addr"), "tcp should have local_addr column");
    assert!(output.contains("local_port"), "tcp should have local_port column");
    assert!(output.contains("state"), "tcp should have state column");
}

#[test]
fn test_udp_command() {
    let id = unique_id();
    let output = run_script("udp\n", id);
    assert!(output.contains("=== udp"), "udp should show view name");
    assert!(output.contains("local_addr"), "udp should have local_addr column");
    assert!(output.contains("local_port"), "udp should have local_port column");
}

#[test]
fn test_lsblk_command() {
    let id = unique_id();
    let output = run_script("lsblk\n", id);
    assert!(output.contains("=== lsblk"), "lsblk should show view name");
    assert!(output.contains("name"), "lsblk should have name column");
    assert!(output.contains("size"), "lsblk should have size column");
}

#[test]
fn test_who_command() {
    let id = unique_id();
    let output = run_script("who\n", id);
    assert!(output.contains("=== who"), "who should show view name");
    assert!(output.contains("user"), "who should have user column");
    assert!(output.contains("tty"), "who should have tty column");
}

#[test]
fn test_lsof_command() {
    let id = unique_id();
    // lsof for current process (pid 1 always exists)
    let output = run_script("lsof 1\n", id);
    assert!(output.contains("=== lsof:1"), "lsof should show view name with pid");
    assert!(output.contains("pid"), "lsof should have pid column");
    assert!(output.contains("fd"), "lsof should have fd column");
    assert!(output.contains("path"), "lsof should have path column");
}

#[test]
fn test_env_command() {
    let id = unique_id();
    let output = run_script("env\n", id);
    assert!(output.contains("=== env"), "env should show view name");
    assert!(output.contains("name"), "env should have name column");
    assert!(output.contains("value"), "env should have value column");
    assert!(output.contains("PATH"), "env should contain PATH variable");
}

// Datetime filter tests using SQL syntax
fn setup_datetime_csv(id: usize) -> String {
    let path = format!("/tmp/tv_datetime_test_{}.csv", id);
    fs::write(&path, "date,value\n2025-01-15,100\n2025-02-20,200\n2025-02-28,300\n2025-03-10,400\n").unwrap();
    path
}

#[test]
fn test_datetime_range_filter() {
    let id = unique_id();
    let csv = setup_datetime_csv(id);
    let output = run_script(&format!("load {}\nfilter date >= '2025-02-01' AND date < '2025-03-01'\n", csv), id);
    assert!(output.contains("(2 rows)"), "Datetime range should return 2 rows");
}

#[test]
fn test_datetime_year_filter() {
    let id = unique_id();
    let csv = setup_datetime_csv(id);
    let output = run_script(&format!("load {}\nfilter date >= '2025-01-01' AND date < '2026-01-01'\n", csv), id);
    assert!(output.contains("(4 rows)"), "Datetime year range should return all 4 rows");
}

// Gzip tests with delimiter detection

fn setup_gz_pipe(id: usize) -> String {
    let path = format!("/tmp/tv_test_{}.gz", id);
    let data = "a|b|c\n1|2|3\n4|5|6\n";
    Command::new("sh").arg("-c").arg(format!("echo -n '{}' | gzip > {}", data, path)).output().unwrap();
    path
}

fn setup_gz_tab(id: usize) -> String {
    let path = format!("/tmp/tv_test_tab_{}.gz", id);
    let data = "a\tb\tc\n1\t2\t3\n4\t5\t6\n";
    Command::new("sh").arg("-c").arg(format!("printf '{}' | gzip > {}", data, path)).output().unwrap();
    path
}

fn setup_gz_comma(id: usize) -> String {
    let path = format!("/tmp/tv_test_comma_{}.gz", id);
    let data = "a,b,c\n1,2,3\n4,5,6\n";
    Command::new("sh").arg("-c").arg(format!("echo -n '{}' | gzip > {}", data, path)).output().unwrap();
    path
}

#[test]
fn test_gz_pipe_separated() {
    let id = unique_id();
    let gz = setup_gz_pipe(id);
    let output = run_script(&format!("from {}\n", gz), id);
    assert!(output.contains("(2 rows)"), "Pipe-separated gz should have 2 rows");
    assert!(output.contains("┆ b"), "Should detect pipe separator and have column b");
    assert!(output.contains("┆ c"), "Should have column c");
}

#[test]
fn test_gz_tab_separated() {
    let id = unique_id();
    let gz = setup_gz_tab(id);
    let output = run_script(&format!("from {}\n", gz), id);
    assert!(output.contains("(2 rows)"), "Tab-separated gz should have 2 rows");
    assert!(output.contains("┆ b"), "Should detect tab separator");
}

#[test]
fn test_gz_comma_separated() {
    let id = unique_id();
    let gz = setup_gz_comma(id);
    let output = run_script(&format!("from {}\n", gz), id);
    assert!(output.contains("(2 rows)"), "Comma-separated gz should have 2 rows");
    assert!(output.contains("┆ b"), "Should detect comma separator");
}

// Epoch timestamp conversion tests

fn setup_epoch_csv(id: usize, col_name: &str, epoch_val: i64) -> String {
    let path = format!("/tmp/tv_epoch_test_{}.csv", id);
    fs::write(&path, format!("id,{},value\n1,{},100\n2,{},200\n", col_name, epoch_val, epoch_val + 86400000)).unwrap();
    path
}

#[test]
fn test_epoch_ms_conversion() {
    let id = unique_id();
    let csv = setup_epoch_csv(id, "timestamp", 1702483200000);  // ms
    let output = run_script(&format!("from {}\n", csv), id);
    assert!(output.contains("datetime[ms]"), "Should convert ms epoch to datetime");
    assert!(output.contains("2023-12-13"), "Should show correct date");
}

#[test]
fn test_epoch_sec_conversion() {
    let id = unique_id();
    let csv = setup_epoch_csv(id, "created_at", 1702483200);  // sec
    let output = run_script(&format!("from {}\n", csv), id);
    assert!(output.contains("datetime"), "Should convert sec epoch to datetime");
    assert!(output.contains("2023-12-13"), "Should show correct date");
}

#[test]
fn test_epoch_us_conversion() {
    let id = unique_id();
    let csv = setup_epoch_csv(id, "event_time", 1702483200000000);  // us
    let output = run_script(&format!("from {}\n", csv), id);
    assert!(output.contains("datetime[μs]"), "Should convert us epoch to datetime");
    assert!(output.contains("2023-12-13"), "Should show correct date");
}

#[test]
fn test_taq_time_conversion() {
    let id = unique_id();
    let path = format!("/tmp/tv_taq_test_{}.csv", id);
    // TAQ format: HHMMSSNNNNNNNN (03:59:00.085993578)
    fs::write(&path, "Time,Value\n035900085993578,100\n143000000000000,200\n").unwrap();
    let output = run_script(&format!("from {}\n", path), id);
    assert!(output.contains("time"), "Should convert TAQ format to time type");
    assert!(output.contains("03:59:00"), "Should show correct time");
}

#[test]
fn test_gz_taq_save_parquet() {
    use std::process::Command;
    let id = unique_id();
    // Create CSV with TAQ time format
    let csv_path = format!("/tmp/tv_taq_gz_{}.csv", id);
    let gz_path = format!("/tmp/tv_taq_gz_{}.csv.gz", id);
    let out_path = format!("/tmp/tv_taq_out_{}.parquet", id);
    // Streaming save creates {prefix}_001.parquet
    let chunk_path = format!("/tmp/tv_taq_out_{}_001.parquet", id);
    fs::write(&csv_path, "Time|Value\n035900085993578|100\n143000000000000|200\n120000000000000|300\n").unwrap();
    // Gzip the file
    Command::new("gzip").arg("-f").arg(&csv_path).output().unwrap();
    // Load gz and save to parquet (streaming creates chunked files)
    run_script(&format!("from {} | save {}\n", gz_path, out_path), id);
    // Load parquet chunk and verify time conversion
    let output = run_script(&format!("from {}\n", chunk_path), id);
    assert!(output.contains("time"), "Parquet should have time type");
    assert!(output.contains("03:59:00"), "Should preserve TAQ time in parquet");
}

#[test]
fn test_parquet_load_int_to_time() {
    use polars::prelude::*;
    let id = unique_id();
    // Create parquet with integer columns having time-like names
    let pq_path = format!("/tmp/tv_pq_int_time_{}.parquet", id);
    let df = df! {
        "event_time" => &[035900085993578i64, 143000000000000, 120000000000000],  // TAQ format
        "created_at" => &[1702483200i64, 1702483260, 1702483320],  // epoch seconds
        "value" => &[100i64, 200, 300],
    }.unwrap();
    ParquetWriter::new(std::fs::File::create(&pq_path).unwrap())
        .finish(&mut df.clone()).unwrap();

    // Load parquet - should convert int columns with time-like names
    let output = run_script(&format!("from {}\nmeta\n", pq_path), id);
    // event_time should be Time type (TAQ format)
    assert!(output.contains("Time"), "event_time should be converted to Time type");
    // created_at should be Datetime (epoch seconds)
    assert!(output.contains("Datetime"), "created_at should be converted to Datetime type");
}

// =============================================================================
// Forth-style User Functions (cfg/funcs.4th)
// =============================================================================

#[test]
fn test_forth_func_sel_null() {
    // Test sel_null function selects rows with 100% null columns (doesn't filter)
    let id = unique_id();
    let path = format!("/tmp/tv_forth_null_{}.csv", id);
    fs::write(&path, "a,b,c\n1,,x\n2,,y\n3,,z\n").unwrap();

    // sel_null uses sel_rows to select matching rows, keeps all 3 rows visible
    let output = run_script(&format!("from {}\nmeta\nsel_null\n", path), id);
    assert!(output.contains("(3 rows)"), "sel_null should keep all rows, just select matching");
    assert!(output.contains("100.0"), "Should still show the 100% null column");
}

#[test]
fn test_forth_func_sel_single() {
    // Test sel_single function selects rows with single-value columns (doesn't filter)
    let id = unique_id();
    let path = format!("/tmp/tv_forth_single_{}.csv", id);
    fs::write(&path, "a,b,c\n1,x,same\n2,y,same\n3,z,same\n").unwrap();

    // sel_single uses sel_rows to select matching rows
    let output = run_script(&format!("from {}\nmeta\nsel_single\n", path), id);
    assert!(output.contains("(3 rows)"), "sel_single should keep all rows");
}

#[test]
fn test_sel_rows_command() {
    // Test sel_rows command directly - selects matching rows without filtering
    let id = unique_id();
    let path = format!("/tmp/tv_sel_rows_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,y\n3,x\n").unwrap();

    let output = run_script(&format!("from {}\nsel_rows b == 'x'\n", path), id);
    // Should keep all 3 rows (selection is visual only, not reflected in output)
    assert!(output.contains("(3 rows)"), "sel_rows should not filter rows");
}

#[test]
fn test_forth_func_expansion() {
    // Test that functions expand correctly
    let id = unique_id();
    let path = format!("/tmp/tv_forth_expand_{}.csv", id);
    fs::write(&path, "a,b\n1,\n2,\n3,\n").unwrap();

    // sel_null expands to: sel_rows `null%` == '100.0'
    let output = run_script(&format!("from {}\nmeta\nsel_null\n", path), id);
    // All rows kept (sel_rows doesn't filter), b column with 100% null shown
    assert!(output.contains("(2 rows)"), "Should show all 2 columns in meta");
    assert!(output.contains("b"), "Should show column b (100% null)");
}

#[test]
fn test_forth_comment_ignored() {
    // Verify that comments ( ... ) are ignored in funcs.4th parsing
    // The funcs.4th has comments like "( Syntax: : name body ; )"
    // This should NOT create a function called "Syntax:"
    let id = unique_id();
    let path = format!("/tmp/tv_forth_comment_{}.csv", id);
    fs::write(&path, "Syntax,val\na,1\nb,2\n").unwrap();

    // If comments were parsed as functions, "Syntax" would be replaced
    let output = run_script(&format!("from {}\n", path), id);
    assert!(output.contains("Syntax"), "Column 'Syntax' should not be replaced by function expansion");
}

#[test]
fn test_sel_all_command() {
    // Test sel_all command selects all rows in meta view
    let id = unique_id();
    let path = format!("/tmp/tv_sel_all_{}.csv", id);
    fs::write(&path, "a,b,c\n1,2,3\n4,5,6\n").unwrap();

    // sel_all in meta view selects all rows
    let output = run_script(&format!("from {}\nmeta\nsel_all\n", path), id);
    assert!(output.contains("(3 rows)"), "sel_all should keep all 3 column rows");
}

// =============================================================================
// Meta View Enter (xkey on selected columns)
// =============================================================================

#[test]
fn test_xkey_moves_columns_to_front() {
    // Test xkey command moves columns to front
    let id = unique_id();
    let path = format!("/tmp/tv_xkey_{}.csv", id);
    fs::write(&path, "a,b,c,d\n1,2,3,4\n5,6,7,8\n").unwrap();

    let output = run_script(&format!("from {}\nxkey c,b\n", path), id);
    // Columns should be reordered: c,b,a,d (xkey cols first)
    assert!(output.contains("c") && output.contains("b"), "xkey columns should be present");
}

// =============================================================================
// Underscore Command Naming
// =============================================================================

#[test]
fn test_del_col_underscore() {
    // Test del_col command (underscore style)
    let id = unique_id();
    let path = format!("/tmp/tv_delcol_{}.csv", id);
    fs::write(&path, "a,b,c\n1,2,3\n4,5,6\n").unwrap();

    let output = run_script(&format!("from {}\ndel_col b\n", path), id);
    assert!(!output.contains("│ b"), "Column b should be deleted");
    assert!(output.contains("a") && output.contains("c"), "Columns a,c should remain");
}

#[test]
fn test_goto_col_underscore() {
    // Test goto_col command (underscore style)
    let id = unique_id();
    let path = format!("/tmp/tv_gotocol_{}.csv", id);
    fs::write(&path, "a,b,c\n1,2,3\n").unwrap();

    // goto_col should work (no error)
    let output = run_script(&format!("from {}\ngoto_col 2\n", path), id);
    assert!(output.contains("(1 rows)"), "Should load successfully");
}

#[test]
fn test_sort_desc_underscore() {
    // Test sort_desc command (underscore style)
    let id = unique_id();
    let path = format!("/tmp/tv_sortdesc_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n3,y\n2,z\n").unwrap();

    let output = run_script(&format!("from {}\nsort_desc a\n", path), id);
    // Should sort descending: 3, 2, 1
    assert!(output.contains("3"), "Should have value 3");
}

// =============================================================================
// Freq with Key Columns
// =============================================================================

#[test]
fn test_freq_with_key_columns() {
    // Test freq groups by key columns when xkey is set
    let id = unique_id();
    let path = format!("/tmp/tv_freq_key_{}.csv", id);
    fs::write(&path, "sym,date,price\nA,2024-01-01,100\nA,2024-01-01,100\nA,2024-01-02,101\nB,2024-01-01,200\n").unwrap();

    // Set sym as key column, then freq on price
    let output = run_script(&format!("from {}\nxkey sym\nfreq price\n", path), id);
    // Freq should group by sym + price
    assert!(output.contains("Freq:price"), "Should show freq view");
    assert!(output.contains("sym"), "Should include key column sym in freq output");
}

#[test]
fn test_freq_without_key_columns() {
    // Test freq without key columns (simple value_counts)
    let id = unique_id();
    let path = format!("/tmp/tv_freq_nokey_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,x\n3,y\n").unwrap();

    let output = run_script(&format!("from {}\nfreq b\n", path), id);
    assert!(output.contains("Freq:b"), "Should show freq view");
    assert!(output.contains("Cnt"), "Should have count column");
}

// =============================================================================
// Meta with Key Columns
// =============================================================================

#[test]
fn test_meta_with_key_columns() {
    // Test meta groups by key columns when xkey is set
    let id = unique_id();
    let path = format!("/tmp/tv_meta_key_{}.csv", id);
    fs::write(&path, "sym,price,volume\nA,100,1000\nA,101,2000\nB,200,3000\nB,201,4000\n").unwrap();

    // Set sym as key column, then run meta
    let output = run_script(&format!("from {}\nxkey sym\nmeta\n", path), id);
    // Meta should group by sym, showing stats for price and volume per sym
    assert!(output.contains("metadata"), "Should show metadata view");
    assert!(output.contains("sym"), "Should include key column sym");
    assert!(output.contains("price"), "Should show stats for price");
    assert!(output.contains("volume"), "Should show stats for volume");
}

#[test]
fn test_meta_without_key_columns() {
    // Test meta without key columns (standard per-column stats)
    let id = unique_id();
    let path = format!("/tmp/tv_meta_nokey_{}.csv", id);
    fs::write(&path, "a,b,c\n1,2,3\n4,5,6\n").unwrap();

    let output = run_script(&format!("from {}\nmeta\n", path), id);
    assert!(output.contains("metadata"), "Should show metadata view");
    assert!(output.contains("(3 rows)"), "Should have one row per column");
}

#[test]
fn test_parquet_time_roundtrip() {
    use polars::prelude::*;
    let id = unique_id();
    let pq_path = format!("/tmp/tv_pq_time_rt_{}.parquet", id);
    
    // Create Time column directly
    let ns: Vec<i64> = vec![3600_000_000_000, 7200_000_000_000, 10800_000_000_000];
    let time_series = Series::new("event_time".into(), ns)
        .cast(&DataType::Time).unwrap();
    let mut df = DataFrame::new(vec![time_series.into()]).unwrap();
    
    println!("Before save: {:?}", df.column("event_time").unwrap().dtype());
    
    // Save to parquet
    ParquetWriter::new(std::fs::File::create(&pq_path).unwrap())
        .finish(&mut df).unwrap();
    
    // Load back raw
    let loaded = ParquetReader::new(std::fs::File::open(&pq_path).unwrap())
        .finish().unwrap();
    println!("After load: {:?}", loaded.column("event_time").unwrap().dtype());
    
    // Should be Time type
    assert!(matches!(loaded.column("event_time").unwrap().dtype(), DataType::Time),
        "Time column should remain Time after parquet roundtrip, got {:?}",
        loaded.column("event_time").unwrap().dtype());
}

#[test]
fn test_folder_open_csv_stack() {
    // When opening a csv from folder view, stack should have 2 views
    // Pop should return to folder, not quit
    let id = unique_id();
    let dir = format!("/tmp/tv_folder_test_{}", id);
    fs::create_dir_all(&dir).unwrap();
    fs::write(format!("{}/test.csv", dir), "a,b\n1,2\n3,4\n").unwrap();

    // Test with pop - should return to folder
    let script_path = format!("/tmp/tv_test_script_{}.txt", id);
    let script = format!("ls {}\nfrom {}/test.csv\npop\n", dir, dir);
    fs::write(&script_path, &script).unwrap();

    let output = Command::new("./target/release/tv")
        .arg("--script")
        .arg(&script_path)
        .output()
        .expect("failed to execute tv");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // After pop, should be back at ls view (not quit)
    assert!(stdout.contains(&format!("=== ls:{}", dir)),
        "After pop from csv, should return to folder view.\nstdout: {}\nstderr: {}", stdout, stderr);
}
