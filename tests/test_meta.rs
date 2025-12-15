//! Meta view tests
mod common;
use common::{unique_id, run_script, setup_test_csv};
use std::fs;

fn setup_numeric_csv(id: usize) -> String {
    let path = format!("/tmp/tv_test_numeric_{}.csv", id);
    fs::write(&path, "x,y,z\n1,10,100\n2,20,200\n3,30,300\n4,40,400\n5,50,500\n").unwrap();
    path
}

fn setup_test_csv_with_nulls(id: usize) -> String {
    let path = format!("/tmp/tv_test_nulls_{}.csv", id);
    fs::write(&path, "a,b,c,d\n1,x,,constant\n2,y,,constant\n3,x,,constant\n,z,,constant\n5,x,,constant\n").unwrap();
    path
}

#[test]
fn test_meta_shows_columns() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\nmeta\n", csv), id);

    assert!(output.contains("=== metadata"), "Should create metadata view");
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

    assert!(output.contains("median"), "Should have median column");
    assert!(output.contains("sigma"), "Should have sigma column");
    assert!(output.contains("min"), "Should have min column");
    assert!(output.contains("max"), "Should have max column");
}

#[test]
fn test_null_column_detection() {
    let id = unique_id();
    let csv = setup_test_csv_with_nulls(id);
    let output = run_script(&format!("load {}\nmeta\n", csv), id);
    assert!(output.contains("100"), "Should show 100% null for column c");
}

#[test]
fn test_single_value_column() {
    let id = unique_id();
    let csv = setup_test_csv_with_nulls(id);
    let output = run_script(&format!("load {}\nmeta\n", csv), id);
    assert!(output.contains("distinct"), "Should have distinct column");
}

#[test]
fn test_meta_empty_not_dash() {
    let id = unique_id();
    let path = format!("/tmp/tv_test_meta_{}.csv", id);
    fs::write(&path, "num,str,empty\n1,apple,\n2,banana,\n3,,\n").unwrap();

    let output = run_script(&format!("load {} | meta\n", path), id);
    let str_line = output.lines().find(|l| l.contains("│ str"));
    if let Some(line) = str_line {
        assert!(!line.contains(" - "), "str row should not contain '-' for empty values");
    }
}

#[test]
fn test_pipe_multiple_operations() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {} | filter a>2 | meta\n", csv), id);
    assert!(output.contains("=== metadata"), "Should end with metadata view");
    assert!(output.contains("column"), "Should have column header");
}
