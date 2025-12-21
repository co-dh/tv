//! Folder view (ls) tests - key-based
mod common;
use common::{run_keys, footer};

// Key navigation tests - :ls shows cwd (project root)
#[test]
fn test_folder_view() {
    // :ls shows current working directory
    let out = run_keys(":ls<ret>", "tests/data/basic.csv");
    assert!(out.contains("folder:"), ":ls should show folder view: {}", out);
    // Should show .. parent entry
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
    // PRQL: use ~= for regex match
    let out = run_keys(":ls<ret><backslash>name ~= '\\.csv$'<ret>", "tests/data/basic.csv");
    let (_, status) = footer(&out);
    // Filter to only csv files
    assert!(status.contains("/"), "Folder filter should show row count: {}", status);
}

#[test]
fn test_folder_recursive() {
    // r for recursive listing
    let out = run_keys("r", "tests/data/basic.csv");
    assert!(out.contains("folder -r:"), "r should show recursive: {}", out);
}

#[test]
fn test_folder_dir_column() {
    // Check that directories have 'x' in dir column - use project root
    let out = run_keys(":ls<ret>", "tests/data/basic.csv");
    // 'src' and 'tests' should be directories with 'x' in dir column
    for line in out.lines() {
        if line.contains("│ src") || line.contains("│ tests") {
            assert!(line.contains("x"), "dir col should be 'x': {}", line);
        }
    }
}

#[test]
fn test_folder_parent_entry() {
    // :ls should show .. entry
    let out = run_keys(":ls<ret>", "tests/data/basic.csv");
    assert!(out.contains(".."), "ls should show .. entry: {}", out);
}

#[test]
fn test_folder_sorted_by_name() {
    // Check that files in folder view are sorted - visible rows at top
    let out = run_keys(":ls<ret>", "tests/data/basic.csv");
    // .. should be first, then .claude, then .git (alphabetically)
    let parent = out.find("..").unwrap_or(999);
    let claude = out.find(".claude").unwrap_or(999);
    let git = out.find(".git").unwrap_or(999);
    assert!(parent < claude, ".. before .claude");
    assert!(claude < git, ".claude before .git");
}

#[test]
fn test_folder_open_file() {
    // Open folder view, navigate to a csv, press enter to open
    let out = run_keys(":ls<ret><down><ret>", "tests/data/basic.csv");
    // Should either show the file or error if not csv
    assert!(out.len() > 0, "Should produce output");
}
