//! Command tests (freq, sort, select, save, etc.)
mod common;
use common::{unique_id, run_script, setup_test_csv};
use std::fs;

fn setup_full_csv(id: usize) -> String {
    let path = format!("/tmp/tv_test_full_{}.csv", id);
    fs::write(&path, "name,city,value,score\nAlice,NYC,100,85\nBob,LA,200,90\nCarol,NYC,150,75\nDave,Chicago,300,95\nEve,LA,250,80\nFrank,NYC,175,\n").unwrap();
    path
}

fn setup_numeric_csv(id: usize) -> String {
    let path = format!("/tmp/tv_test_numeric_{}.csv", id);
    fs::write(&path, "x,y,z\n1,10,100\n2,20,200\n3,30,300\n4,40,400\n5,50,500\n").unwrap();
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
fn test_freq_shows_value_counts() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\nfreq b\n", csv), id);

    assert!(output.contains("Freq:b"), "Should create frequency view for column b");
    assert!(output.contains("Cnt"), "Should have Cnt column");
    assert!(output.contains("Pct"), "Should have Pct column");
    assert!(output.contains("x"), "Should show value 'x'");
}

#[test]
fn test_freq_then_filter_workflow() {
    let id = unique_id();
    let csv = setup_test_csv(id);

    let freq_output = run_script(&format!("load {} | freq b\n", csv), id);
    assert!(freq_output.contains("Freq:b"), "Should create freq view");
    assert!(freq_output.contains("x"), "Should show value x");

    let filter_output = run_script(&format!("load {} | filter b IN ('x')\n", csv), id);
    assert!(filter_output.contains("(3 rows)"), "Should filter to 3 rows where b='x'");
    assert!(filter_output.contains("b"), "Filtered view should have column b");
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
fn test_select_columns() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\nsel a\n", csv), id);
    assert!(output.contains("a"), "Should have column a");
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
fn test_delcol_removes_specific_column() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {} | delcol b\n", csv), id);
    assert!(output.contains("(5 rows)"), "Should keep all rows");
    assert!(output.contains("a"), "Should have column a");
}

#[test]
fn test_delete_column() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\ndelcol b\n", csv), id);
    assert!(output.contains("(5 rows)"), "Should keep all rows after delete");
}

#[test]
fn test_rename_column() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\nrename a num\n", csv), id);
    assert!(output.contains("num"), "Should have renamed column");
}

#[test]
fn test_sort_ascending() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\nsort a\n", csv), id);
    assert!(output.contains("(5 rows)"), "Should keep all rows after sort");
}

#[test]
fn test_sort_descending() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\nsortdesc a\n", csv), id);
    assert!(output.contains("(5 rows)"), "Should keep all rows after sort");
}

#[test]
fn test_corr_matrix() {
    let id = unique_id();
    let csv = setup_numeric_csv(id);
    let output = run_script(&format!("load {}\ncorr\n", csv), id);
    assert!(output.contains("=== correlation"), "Should create correlation view");
    assert!(output.contains("1.0"), "Diagonal values should be 1.0");
}

#[test]
fn test_pipe_chain() {
    let id = unique_id();
    let csv = setup_full_csv(id);
    let output = run_script(&format!("load {} | filter city = 'NYC' | sort value | sel name,value\n", csv), id);
    assert!(output.contains("(3 rows)"), "Pipe chain should work");
}

#[test]
fn test_load_nonexistent_file_error() {
    let id = unique_id();
    let output = run_script("from /nonexistent/path/file.csv\n", id);
    assert!(output.contains("No table loaded"), "Loading nonexistent file should result in no table");
}

#[test]
fn test_from_and_load_equivalent() {
    let id = unique_id();
    let path = format!("/tmp/tv_test_from_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,y\n").unwrap();

    let from_output = run_script(&format!("from {}\n", path), id + 1000);
    let load_output = run_script(&format!("load {}\n", path), id + 1001);

    assert!(from_output.contains("(2 rows)"), "'from' command should load 2 rows");
    assert!(load_output.contains("(2 rows)"), "'load' command should load 2 rows");
    assert_eq!(from_output, load_output, "'from' and 'load' should produce identical output");
}

// Underscore command naming tests
#[test]
fn test_del_col_underscore() {
    let id = unique_id();
    let path = format!("/tmp/tv_delcol_{}.csv", id);
    fs::write(&path, "a,b,c\n1,2,3\n4,5,6\n").unwrap();
    let output = run_script(&format!("from {}\ndel_col b\n", path), id);
    assert!(!output.contains("│ b"), "Column b should be deleted");
    assert!(output.contains("a") && output.contains("c"), "Columns a,c should remain");
}

#[test]
fn test_goto_col_underscore() {
    let id = unique_id();
    let path = format!("/tmp/tv_gotocol_{}.csv", id);
    fs::write(&path, "a,b,c\n1,2,3\n").unwrap();
    let output = run_script(&format!("from {}\ngoto_col 2\n", path), id);
    assert!(output.contains("(1 rows)"), "Should load successfully");
}

#[test]
fn test_sort_desc_underscore() {
    let id = unique_id();
    let path = format!("/tmp/tv_sortdesc_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n3,y\n2,z\n").unwrap();
    let output = run_script(&format!("from {}\nsort_desc a\n", path), id);
    assert!(output.contains("3"), "Should have value 3");
}

#[test]
fn test_xkey_moves_columns_to_front() {
    let id = unique_id();
    let path = format!("/tmp/tv_xkey_{}.csv", id);
    fs::write(&path, "a,b,c,d\n1,2,3,4\n5,6,7,8\n").unwrap();
    let output = run_script(&format!("from {}\nxkey c,b\n", path), id);
    assert!(output.contains("c") && output.contains("b"), "xkey columns should be present");
}

#[test]
fn test_freq_with_key_columns() {
    let id = unique_id();
    let path = format!("/tmp/tv_freq_key_{}.csv", id);
    fs::write(&path, "sym,date,price\nA,2024-01-01,100\nA,2024-01-01,100\nA,2024-01-02,101\nB,2024-01-01,200\n").unwrap();
    let output = run_script(&format!("from {}\nxkey sym\nfreq price\n", path), id);
    assert!(output.contains("Freq:price"), "Should show freq view");
    assert!(output.contains("sym"), "Should include key column sym in freq output");
}

#[test]
fn test_freq_without_key_columns() {
    let id = unique_id();
    let path = format!("/tmp/tv_freq_nokey_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,x\n3,y\n").unwrap();
    let output = run_script(&format!("from {}\nfreq b\n", path), id);
    assert!(output.contains("Freq:b"), "Should show freq view");
    assert!(output.contains("Cnt"), "Should have count column");
}

#[test]
fn test_meta_with_key_columns() {
    let id = unique_id();
    let path = format!("/tmp/tv_meta_key_{}.csv", id);
    fs::write(&path, "sym,price,volume\nA,100,1000\nA,101,2000\nB,200,3000\nB,201,4000\n").unwrap();
    let output = run_script(&format!("from {}\nxkey sym\nmeta\n", path), id);
    assert!(output.contains("metadata"), "Should show metadata view");
    assert!(output.contains("sym"), "Should include key column sym");
    assert!(output.contains("price"), "Should show stats for price");
}

#[test]
fn test_meta_without_key_columns() {
    let id = unique_id();
    let path = format!("/tmp/tv_meta_nokey_{}.csv", id);
    fs::write(&path, "a,b,c\n1,2,3\n4,5,6\n").unwrap();
    let output = run_script(&format!("from {}\nmeta\n", path), id);
    assert!(output.contains("metadata"), "Should show metadata view");
    assert!(output.contains("(3 rows)"), "Should have one row per column");
}
