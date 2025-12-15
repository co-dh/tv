//! System command tests (ps, forth funcs, history)
mod common;
use common::{unique_id, run_script, setup_test_csv};
use std::fs;

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
fn test_failed_command_recorded_in_history() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let hist = dirs::home_dir().unwrap().join(".tv/history.4th");

    let before = std::fs::read_to_string(&hist).unwrap_or_default();
    let before_lines = before.lines().count();

    let output = run_script(&format!("load {}\nfilter nonexistent > 1\n", csv), id);
    assert!(output.contains("Error") || output.contains("error"), "Filter should fail: {}", output);

    let after = std::fs::read_to_string(&hist).unwrap_or_default();
    let after_lines = after.lines().count();
    assert!(after_lines > before_lines, "Failed command should be recorded in history");
    assert!(after.contains("filter nonexistent"), "History should contain the failed filter command");
}

// Forth-style User Functions
#[test]
fn test_forth_func_sel_null() {
    let id = unique_id();
    let path = format!("/tmp/tv_forth_null_{}.csv", id);
    fs::write(&path, "a,b,c\n1,,x\n2,,y\n3,,z\n").unwrap();

    let output = run_script(&format!("from {}\nmeta\nsel_null\n", path), id);
    assert!(output.contains("(3 rows)"), "sel_null should keep all rows, just select matching");
    assert!(output.contains("100.0"), "Should still show the 100% null column");
}

#[test]
fn test_forth_func_sel_single() {
    let id = unique_id();
    let path = format!("/tmp/tv_forth_single_{}.csv", id);
    fs::write(&path, "a,b,c\n1,x,same\n2,y,same\n3,z,same\n").unwrap();
    let output = run_script(&format!("from {}\nmeta\nsel_single\n", path), id);
    assert!(output.contains("(3 rows)"), "sel_single should keep all rows");
}

#[test]
fn test_sel_rows_command() {
    let id = unique_id();
    let path = format!("/tmp/tv_sel_rows_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,y\n3,x\n").unwrap();
    let output = run_script(&format!("from {}\nsel_rows b == 'x'\n", path), id);
    assert!(output.contains("(3 rows)"), "sel_rows should not filter rows");
}

#[test]
fn test_forth_func_expansion() {
    let id = unique_id();
    let path = format!("/tmp/tv_forth_expand_{}.csv", id);
    fs::write(&path, "a,b\n1,\n2,\n3,\n").unwrap();
    let output = run_script(&format!("from {}\nmeta\nsel_null\n", path), id);
    assert!(output.contains("(2 rows)"), "Should show all 2 columns in meta");
    assert!(output.contains("b"), "Should show column b (100% null)");
}

#[test]
fn test_forth_comment_ignored() {
    let id = unique_id();
    let path = format!("/tmp/tv_forth_comment_{}.csv", id);
    fs::write(&path, "Syntax,val\na,1\nb,2\n").unwrap();
    let output = run_script(&format!("from {}\n", path), id);
    assert!(output.contains("Syntax"), "Column 'Syntax' should not be replaced by function expansion");
}

#[test]
fn test_sel_all_command() {
    let id = unique_id();
    let path = format!("/tmp/tv_sel_all_{}.csv", id);
    fs::write(&path, "a,b,c\n1,2,3\n4,5,6\n").unwrap();
    let output = run_script(&format!("from {}\nmeta\nsel_all\n", path), id);
    assert!(output.contains("(3 rows)"), "sel_all should keep all 3 column rows");
}

#[test]
fn test_csv_freq_enter() {
    let id = unique_id();
    let path = format!("/tmp/tv_csv_freq_enter_{}.csv", id);
    fs::write(&path, "a,b\n1,x\n2,y\n3,x\n4,z\n5,x\n").unwrap();
    let output = run_script(&format!("from {}\nfreq b\nenter\n", path), id);
    assert!(output.contains("b=x") || output.contains("(3 rows)"), "Should filter to x values: {}", output);
    fs::remove_file(&path).ok();
}
