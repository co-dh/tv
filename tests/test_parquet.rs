//! Parquet backend tests
mod common;
use common::{unique_id, run_script};
use std::fs;
use polars::prelude::*;

#[test]
fn test_parquet_filter_uses_disk() {
    let id = unique_id();
    let path = format!("/tmp/tv_pq_filter_{}.parquet", id);
    let n = 200_000usize;
    let df = df! {
        "symbol" => (0..n).map(|i| if i < n/2 { "A" } else { "B" }).collect::<Vec<&str>>()
    }.unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df.clone()).unwrap();

    let output = run_script(&format!("from {}\nfilter \"symbol\" = 'B'\n", path), id);
    assert!(output.contains("rows") && !output.contains("(0 rows)") && !output.contains("(0)"),
        "Filter should find B rows from disk (not 0), got: {}", output);
}

#[test]
fn test_parquet_freq_enter_uses_disk() {
    let id = unique_id();
    let path = format!("/tmp/tv_pq_freq_enter_{}.parquet", id);
    let n = 200_000usize;
    let df = df! {
        "sym" => (0..n).map(|i| if i < n/2 { "A" } else { "B" }).collect::<Vec<&str>>()
    }.unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df.clone()).unwrap();

    let output = run_script(&format!("from {}\nfreq sym\nenter\n", path), id);
    assert!(output.contains("rows") && !output.contains("(0 rows)") && !output.contains("(0)"),
        "Freq enter should find B rows from disk (not 0), got: {}", output);
    fs::remove_file(&path).ok();
}

#[test]
fn test_parquet_sort_ascending() {
    let id = unique_id();
    let csv_path = format!("/tmp/tv_parquet_sort_{}.csv", id);
    let parquet_path = format!("/tmp/tv_parquet_sort_{}.parquet", id);

    fs::write(&csv_path, "name,value\ncharlie,3\nalice,1\nbob,2\n").unwrap();
    run_script(&format!("from {}\nsave {}\n", csv_path, parquet_path), id);
    let output = run_script(&format!("from {}\nsort name\n", parquet_path), id + 1);

    let alice_pos = output.find("alice").expect("Should contain alice");
    let bob_pos = output.find("bob").expect("Should contain bob");
    let charlie_pos = output.find("charlie").expect("Should contain charlie");

    assert!(alice_pos < bob_pos, "alice should come before bob");
    assert!(bob_pos < charlie_pos, "bob should come before charlie");

    let _ = fs::remove_file(csv_path);
    let _ = fs::remove_file(parquet_path);
}

#[test]
fn test_parquet_sort_descending() {
    let id = unique_id();
    let csv_path = format!("/tmp/tv_parquet_sortd_{}.csv", id);
    let parquet_path = format!("/tmp/tv_parquet_sortd_{}.parquet", id);

    fs::write(&csv_path, "name,value\ncharlie,3\nalice,1\nbob,2\n").unwrap();
    run_script(&format!("from {}\nsave {}\n", csv_path, parquet_path), id);
    let output = run_script(&format!("from {}\nsort -name\n", parquet_path), id + 1);

    let alice_pos = output.find("alice").expect("Should contain alice");
    let bob_pos = output.find("bob").expect("Should contain bob");
    let charlie_pos = output.find("charlie").expect("Should contain charlie");

    assert!(charlie_pos < bob_pos, "charlie should come before bob");
    assert!(bob_pos < alice_pos, "bob should come before alice");

    let _ = fs::remove_file(csv_path);
    let _ = fs::remove_file(parquet_path);
}

#[test]
fn test_parquet_meta() {
    let id = unique_id();
    let path = format!("/tmp/tv_pq_meta_{}.parquet", id);
    let df = df! { "a" => &[1i64, 2, 3], "b" => &["x", "y", "z"] }.unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap()).finish(&mut df.clone()).unwrap();
    let output = run_script(&format!("from {}\nmeta\n", path), id);
    assert!(output.contains("metadata"), "Should show metadata view");
    assert!(output.contains("a") && output.contains("b"), "Should list columns");
    fs::remove_file(&path).ok();
}

#[test]
fn test_parquet_time_roundtrip() {
    let id = unique_id();
    let pq_path = format!("/tmp/tv_pq_time_rt_{}.parquet", id);

    let ns: Vec<i64> = vec![3600_000_000_000, 7200_000_000_000, 10800_000_000_000];
    let time_series = Series::new("event_time".into(), ns)
        .cast(&DataType::Time).unwrap();
    let mut df = DataFrame::new(vec![time_series.into()]).unwrap();

    ParquetWriter::new(std::fs::File::create(&pq_path).unwrap())
        .finish(&mut df).unwrap();

    let loaded = ParquetReader::new(std::fs::File::open(&pq_path).unwrap())
        .finish().unwrap();

    assert!(matches!(loaded.column("event_time").unwrap().dtype(), DataType::Time),
        "Time column should remain Time after parquet roundtrip, got {:?}",
        loaded.column("event_time").unwrap().dtype());
}

/// Filtered parquet view shows correct row count from disk via SQL count(*)
#[test]
fn test_parquet_filtered_count() {
    let id = unique_id();
    let path = format!("/tmp/tv_pq_filt_cnt_{}.parquet", id);
    let n = 100_000usize;
    // 40% A, 60% B
    let df = df! {
        "sym" => (0..n).map(|i| if i < n * 4 / 10 { "A" } else { "B" }).collect::<Vec<&str>>()
    }.unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df.clone()).unwrap();

    // Filter for B rows - should show 60,000 rows
    let output = run_script(&format!("from {}\nfilter \"sym\" = 'B'\n", path), id);
    assert!(output.contains("60,000") || output.contains("60000"),
        "Filtered view should show 60,000 rows, got: {}", output);
    fs::remove_file(&path).ok();
}

/// Filtered parquet view freq runs against disk (not memory)
#[test]
fn test_parquet_filtered_freq() {
    let id = unique_id();
    let path = format!("/tmp/tv_pq_filt_freq_{}.parquet", id);
    let n = 100_000usize;
    // sym: 50% A, 50% B. cat: 25% each of X, Y, Z, W
    let df = df! {
        "sym" => (0..n).map(|i| if i < n/2 { "A" } else { "B" }).collect::<Vec<&str>>(),
        "cat" => (0..n).map(|i| match i % 4 { 0 => "X", 1 => "Y", 2 => "Z", _ => "W" }).collect::<Vec<&str>>()
    }.unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df.clone()).unwrap();

    // Filter for A, then freq on cat - should show ~12,500 each
    let output = run_script(&format!("from {}\nfilter \"sym\" = 'A'\nfreq cat\n", path), id);
    // Each cat value should appear ~12,500 times in filtered view
    assert!(output.contains("12") && output.contains("Freq:cat"),
        "Filtered freq should show ~12,500 counts, got: {}", output);
    fs::remove_file(&path).ok();
}
