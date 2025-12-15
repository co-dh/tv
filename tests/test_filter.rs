//! Filter command tests (key-based)
//! Uses <backslash> to enter filter mode, type expression, <ret> to execute
mod common;
use common::run_keys;

// Basic filter tests using tests/data/basic.csv (a,b: 1,x 2,y 3,x 4,z 5,x)

#[test]
fn test_filter_integer_gt() {
    let output = run_keys("<backslash>a <gt> 2<ret>", "tests/data/basic.csv");
    assert!(output.contains("(3 rows)"), "a>2 should match 3 rows: {}", output);
}

#[test]
fn test_filter_integer_gte() {
    let output = run_keys("<backslash>a <gt>= 3<ret>", "tests/data/basic.csv");
    assert!(output.contains("(3 rows)"), "a>=3 should match 3 rows");
}

#[test]
fn test_filter_integer_lt() {
    let output = run_keys("<backslash>a <lt> 3<ret>", "tests/data/basic.csv");
    assert!(output.contains("(2 rows)"), "a<3 should match 2 rows");
}

#[test]
fn test_filter_integer_eq() {
    let output = run_keys("<backslash>a = 3<ret>", "tests/data/basic.csv");
    assert!(output.contains("(1 row"), "a=3 should match 1 row");
}

#[test]
fn test_filter_string_eq() {
    let output = run_keys("<backslash>b = 'x'<ret>", "tests/data/basic.csv");
    assert!(output.contains("(3 rows)"), "b='x' should match 3 rows");
}

#[test]
fn test_filter_between() {
    let output = run_keys("<backslash>a BETWEEN 2 AND 4<ret>", "tests/data/basic.csv");
    assert!(output.contains("(3 rows)"), "BETWEEN 2 AND 4 should match 3 rows");
}

#[test]
fn test_filter_and() {
    let output = run_keys("<backslash>a <gt>= 2 AND a <lt>= 4<ret>", "tests/data/basic.csv");
    assert!(output.contains("(3 rows)"), "a>=2 AND a<=4 should match 3 rows");
}

#[test]
fn test_filter_or() {
    let output = run_keys("<backslash>a = 1 OR a = 5<ret>", "tests/data/basic.csv");
    assert!(output.contains("(2 rows)"), "a=1 OR a=5 should match 2 rows");
}

#[test]
fn test_filter_in_single() {
    let output = run_keys("<backslash>b IN ('x')<ret>", "tests/data/basic.csv");
    assert!(output.contains("(3 rows)"), "b IN ('x') should match 3 rows");
}

#[test]
fn test_filter_in_multiple() {
    let output = run_keys("<backslash>b IN ('x','y')<ret>", "tests/data/basic.csv");
    assert!(output.contains("(4 rows)"), "b IN ('x','y') should match 4 rows");
}

// String filter tests using tests/data/strings.csv (name,value: apple,10 ... blueberry,60)

#[test]
fn test_filter_like_starts_with() {
    let output = run_keys("<backslash>name LIKE 'a%'<ret>", "tests/data/strings.csv");
    assert!(output.contains("(1 row"), "LIKE 'a%' should match apple: {}", output);
}

#[test]
fn test_filter_like_ends_with() {
    let output = run_keys("<backslash>name LIKE '%apple'<ret>", "tests/data/strings.csv");
    assert!(output.contains("(2 rows)"), "LIKE '%apple' should match apple, pineapple");
}

#[test]
fn test_filter_like_contains() {
    let output = run_keys("<backslash>name LIKE '%an%'<ret>", "tests/data/strings.csv");
    assert!(output.contains("(1 row"), "LIKE '%an%' should match banana");
}

#[test]
fn test_filter_numeric_gt() {
    let output = run_keys("<backslash>value <gt> 30<ret>", "tests/data/strings.csv");
    assert!(output.contains("(3 rows)"), "value > 30 should match 3 rows");
}

#[test]
fn test_filter_numeric_gte() {
    let output = run_keys("<backslash>value <gt>= 30<ret>", "tests/data/strings.csv");
    assert!(output.contains("(4 rows)"), "value >= 30 should match 4 rows");
}

#[test]
fn test_filter_numeric_lt() {
    let output = run_keys("<backslash>value <lt> 30<ret>", "tests/data/strings.csv");
    assert!(output.contains("(2 rows)"), "value < 30 should match 2 rows");
}

#[test]
fn test_filter_numeric_lte() {
    let output = run_keys("<backslash>value <lt>= 30<ret>", "tests/data/strings.csv");
    assert!(output.contains("(3 rows)"), "value <= 30 should match 3 rows");
}

#[test]
fn test_filter_combined() {
    let output = run_keys("<backslash>name LIKE 'b%' AND value <gt> 30<ret>", "tests/data/strings.csv");
    assert!(output.contains("(1 row"), "name LIKE 'b%' AND value > 30 should match blueberry");
}
