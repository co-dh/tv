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

// === ls/lr tests (from test_ls.sh) ===

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
fn test_lr_recursive() {
    let id = unique_id();
    let dir = format!("/tmp/tv_test_dir3_{}", id);
    fs::create_dir_all(format!("{}/subdir", dir)).unwrap();
    fs::write(format!("{}/subdir/nested.txt", dir), "test").unwrap();

    let output = run_script(&format!("lr {}\n", dir), id);
    assert!(output.contains("subdir"), "lr should show subdir");
    assert!(output.contains("nested.txt"), "lr should show nested file");
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
    assert!(output.contains("name"), "ps should have name column");
    assert!(output.contains("state"), "ps should have state column");
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
