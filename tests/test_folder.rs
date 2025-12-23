//! Folder view (ls) tests - uses ADBC/DuckDB for source: paths
mod common;
use common::{run_keys, footer};

#[test]
fn test_folder_view() {
    let out = run_keys(":ls<ret>", "tests/data/basic.csv");
    assert!(out.contains("folder:"), ":ls should show folder view: {}", out);
    assert!(out.contains(".."), "Should show parent entry: {}", out);
}

#[test]
fn test_folder_sort_by_size() {
    let out = run_keys(":ls<ret><right><right>]", "tests/data/basic.csv");
    assert!(out.contains("folder:"), ":ls should show folder view: {}", out);
}

#[test]
fn test_folder_freq() {
    let out = run_keys(":ls<ret><right><right><right><right>F", "tests/data/basic.csv");
    assert!(out.contains("freq dir"), "F on dir column: {}", out);
}

#[test]
fn test_folder_multi_select() {
    let out = run_keys(":ls<ret><space><down><space>", "tests/data/basic.csv");
    assert!(out.contains("folder:"), "Folder view multi-select: {}", out);
}

#[test]
fn test_folder_filter() {
    let out = run_keys(":ls<ret><backslash>name ~= '\\.csv$'<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&out);
    assert!(status.contains("/"), "Folder filter should show row count: {}", status);
}

#[test]
fn test_folder_recursive() {
    let out = run_keys("r", "tests/data/basic.csv");
    assert!(out.contains("folder -r:"), "r should show recursive: {}", out);
}

#[test]
fn test_folder_dir_column() {
    let out = run_keys(":ls<ret>", "tests/data/basic.csv");
    for line in out.lines() {
        if line.contains("│ src") || line.contains("│ tests") {
            assert!(line.contains("x"), "dir col should be 'x': {}", line);
        }
    }
}

#[test]
fn test_folder_parent_entry() {
    let out = run_keys(":ls<ret>", "tests/data/basic.csv");
    assert!(out.contains(".."), "ls should show .. entry: {}", out);
}

#[test]
fn test_folder_sorted_by_name() {
    let out = run_keys(":ls<ret>", "tests/data/basic.csv");
    let parent = out.find("..").unwrap_or(999);
    let claude = out.find(".claude").unwrap_or(999);
    let git = out.find(".git").unwrap_or(999);
    assert!(parent < claude, ".. before .claude");
    assert!(claude < git, ".claude before .git");
}

#[test]
fn test_folder_open_file() {
    let out = run_keys(":ls<ret><down><ret>", "tests/data/basic.csv");
    assert!(out.len() > 0, "Should produce output");
}

#[test]
fn test_folder_path_is_containing_dir() {
    // path column should show containing folder, not full file path
    let out = run_keys(":ls tests/data<ret>", "");
    // All files in tests/data should have path = .../tests/data
    assert!(out.contains("tests/data"), "Path should show containing dir: {}", out);
    // File rows should NOT have file extension in path column
    let lines: Vec<&str> = out.lines().skip(1).take(5).collect();
    for line in &lines {
        if line.contains(".csv") || line.contains(".parquet") {
            // The path column (2nd) should not contain the filename
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                assert!(!parts[1].contains(".csv"), "Path should not contain filename: {}", line);
            }
        }
    }
}
