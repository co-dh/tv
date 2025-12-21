//! Filter command tests (key-based)
//! Uses <backslash> to enter filter mode, type PRQL expression, <ret> to execute
//! PRQL syntax: == for equality, && for AND, || for OR
mod common;
use common::{run_keys, footer};

// Basic filter tests using tests/data/basic.csv (a,b: 1,x 2,y 3,x 4,z 5,x)

#[test]
fn test_filter_integer_gt() {
    let output = run_keys("<backslash>a > 2<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/3"), "a>2 should match 3 rows: {}", status);
}

#[test]
fn test_filter_integer_gte() {
    let output = run_keys("<backslash>a >= 3<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/3"), "a>=3 should match 3 rows: {}", status);
}

#[test]
fn test_filter_integer_lt() {
    let output = run_keys("<backslash>a <lt> 3<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/2"), "a<3 should match 2 rows: {}", status);
}

#[test]
fn test_filter_integer_eq() {
    let output = run_keys("<backslash>a == 3<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/1"), "a==3 should match 1 row: {}", status);
}

#[test]
fn test_filter_string_eq() {
    let output = run_keys("<backslash>b == 'x'<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/3"), "b=='x' should match 3 rows: {}", status);
}

#[test]
fn test_filter_between() {
    // PRQL: use && for AND
    let output = run_keys("<backslash>a >= 2 && a <lt>= 4<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/3"), "BETWEEN 2 AND 4 should match 3 rows: {}", status);
}

#[test]
fn test_filter_and() {
    let output = run_keys("<backslash>a >= 2 && a <lt>= 4<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/3"), "a>=2 && a<=4 should match 3 rows: {}", status);
}

#[test]
fn test_filter_or() {
    let output = run_keys("<backslash>a == 1 || a == 5<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/2"), "a==1 || a==5 should match 2 rows: {}", status);
}

#[test]
fn test_filter_in_single() {
    let output = run_keys("<backslash>b == 'x'<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/3"), "b=='x' should match 3 rows: {}", status);
}

#[test]
fn test_filter_in_multiple() {
    // PRQL: use || for multiple conditions
    let output = run_keys("<backslash>b == 'x' || b == 'y'<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/4"), "b=='x' || b=='y' should match 4 rows: {}", status);
}

// String filter tests using tests/data/strings.csv (name,value: apple,10 ... blueberry,60)
// PRQL uses s"..." for SQL syntax like LIKE

#[test]
fn test_filter_like_starts_with() {
    // PRQL: s"..." for SQL LIKE syntax
    let output = run_keys("<backslash>s\"name LIKE 'a%'\"<ret>", "tests/data/strings.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/1"), "LIKE 'a%' should match apple: {}", status);
}

#[test]
fn test_filter_like_ends_with() {
    let output = run_keys("<backslash>s\"name LIKE '%apple'\"<ret>", "tests/data/strings.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/2"), "LIKE '%apple' should match 2 rows: {}", status);
}

#[test]
fn test_filter_like_contains() {
    let output = run_keys("<backslash>s\"name LIKE '%an%'\"<ret>", "tests/data/strings.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/1"), "LIKE '%an%' should match banana: {}", status);
}

#[test]
fn test_filter_numeric_gt() {
    let output = run_keys("<backslash>value > 30<ret>", "tests/data/strings.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/3"), "value > 30 should match 3 rows: {}", status);
}

#[test]
fn test_filter_numeric_gte() {
    let output = run_keys("<backslash>value >= 30<ret>", "tests/data/strings.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/4"), "value >= 30 should match 4 rows: {}", status);
}

#[test]
fn test_filter_numeric_lt() {
    let output = run_keys("<backslash>value <lt> 30<ret>", "tests/data/strings.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/2"), "value < 30 should match 2 rows: {}", status);
}

#[test]
fn test_filter_numeric_lte() {
    let output = run_keys("<backslash>value <lt>= 30<ret>", "tests/data/strings.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/3"), "value <= 30 should match 3 rows: {}", status);
}

#[test]
fn test_filter_combined() {
    // PRQL: && for AND, s"..." for SQL LIKE
    let output = run_keys("<backslash>s\"name LIKE 'b%'\" && value > 30<ret>", "tests/data/strings.csv");
    let (_, status) = footer(&output);
    assert!(status.ends_with("0/1"), "name LIKE 'b%' && value > 30 should match blueberry: {}", status);
}
