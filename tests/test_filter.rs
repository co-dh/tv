//! Filter command tests
mod common;
use common::{unique_id, run_script, setup_test_csv};
use std::fs;

#[test]
fn test_filter_pushes_new_view() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\nfilter a>2\n", csv), id);
    assert!(output.contains("=== a>2"), "Filter should create view named by expression");
    assert!(output.contains("(3 rows)"), "Filter should have 3 matching rows");
}

#[test]
fn test_filter_preserves_original_view() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {}\nfilter a>2\n", csv), id);
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
fn test_filter_reserved_word_column() {
    let id = unique_id();
    let csv = format!("tmp/reserved_{}.csv", id);
    std::fs::write(&csv, "USER,value\nroot,1\nadmin,2\nroot,3\n").unwrap();
    let output = run_script(&format!("load {} | filter \"USER\" = 'root'\n", csv), id);
    assert!(output.contains("(2 rows)"), "Quoted USER='root' should match 2 rows, got: {}", output);
    std::fs::remove_file(&csv).ok();
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

#[test]
fn test_filter_in_single_value() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {} | filter b IN ('x')\n", csv), id);
    assert!(output.contains("(3 rows)"), "b IN ('x') should match 3 rows");
}

#[test]
fn test_filter_in_multiple_values() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {} | filter b IN ('x','y')\n", csv), id);
    assert!(output.contains("(4 rows)"), "b IN ('x','y') should match 4 rows");
}

#[test]
fn test_pipe_separated_commands() {
    let id = unique_id();
    let csv = setup_test_csv(id);
    let output = run_script(&format!("load {} | filter a>2\n", csv), id);
    assert!(output.contains("a>2"), "Should create filtered view");
    assert!(output.contains("(3 rows)"), "Should have 3 filtered rows");
}

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
