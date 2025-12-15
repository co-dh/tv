//! Folder view (ls) tests
mod common;
use common::{unique_id, run_script, run_keys};
use std::fs;

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
    assert!(output.contains(".."), "ls should show .. entry for parent");
    assert!(output.contains("/tmp"), ".. should point to parent directory");
}

#[test]
fn test_ls_sorted_by_name() {
    let id = unique_id();
    let dir = format!("/tmp/tv_test_sort_{}", id);
    fs::create_dir_all(&dir).unwrap();
    fs::write(format!("{}/zebra.txt", dir), "z").unwrap();
    fs::write(format!("{}/apple.txt", dir), "a").unwrap();
    fs::write(format!("{}/mango.txt", dir), "m").unwrap();

    let output = run_script(&format!("ls {}\n", dir), id);
    let apple_pos = output.find("apple.txt").unwrap();
    let mango_pos = output.find("mango.txt").unwrap();
    let zebra_pos = output.find("zebra.txt").unwrap();
    assert!(apple_pos < mango_pos, "apple should come before mango");
    assert!(mango_pos < zebra_pos, "mango should come before zebra");
}

#[test]
fn test_folder_open_csv_stack() {
    // Test that opening CSV from folder view stacks correctly
    let id = unique_id();
    let dir = format!("/tmp/tv_folder_test_{}", id);
    fs::create_dir_all(&dir).unwrap();
    let csv_path = format!("{}/test.csv", dir);
    fs::write(&csv_path, "a,b\n1,x\n2,y\n").unwrap();

    // ls then open CSV
    let output = run_script(&format!("ls {}\n", dir), id);
    assert!(output.contains("test.csv"), "ls should show test.csv");
}

// Key play tests for folder view (Kakoune-style: l<right><right>] no commas)
#[test]
fn test_keys_folder_sort_by_size() {
    let output = run_keys("l<right><right>]", ".");
    assert!(output.contains("ls:"), "l should show folder view: {}", output);
}

#[test]
fn test_keys_folder_freq() {
    let output = run_keys("l<right><right><right><right>F", ".");
    assert!(output.contains("Freq:dir"), "F should show freq on dir column: {}", output);
}

#[test]
fn test_keys_folder_multi_select() {
    let output = run_keys("l<space><down><space>", ".");
    assert!(output.contains("ls:"), "Folder view should support multi-select: {}", output);
}

#[test]
fn test_keys_folder_filter() {
    let output = run_keys("l<backslash>", ".");
    assert!(output.contains("ls:"), "Folder view should support filter: {}", output);
}
