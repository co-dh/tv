//! Parquet backend tests
mod common;
use common::{unique_id, run_script};
use std::fs;
use polars::prelude::*;

#[test]
fn test_parquet_filter_uses_disk() {
    let id = unique_id();
    let output = run_script("from tests/data/freq_test.parquet\nfilter \"sym\" = 'B'\n", id);
    assert!(output.contains("rows") && !output.contains("(0 rows)") && !output.contains("(0)"),
        "Filter should find B rows from disk (not 0), got: {}", output);
}

#[test]
fn test_parquet_freq_enter_uses_disk() {
    let id = unique_id();
    let output = run_script("from tests/data/freq_test.parquet\nfreq sym\nenter\n", id);
    assert!(output.contains("rows") && !output.contains("(0 rows)") && !output.contains("(0)"),
        "Freq enter should find B rows from disk (not 0), got: {}", output);
}

#[test]
fn test_parquet_sort_ascending() {
    let id = unique_id();
    let output = run_script("from tests/data/sort_test.parquet\nsort name\n", id);
    let alice_pos = output.find("alice").expect("Should contain alice");
    let bob_pos = output.find("bob").expect("Should contain bob");
    let charlie_pos = output.find("charlie").expect("Should contain charlie");
    assert!(alice_pos < bob_pos, "alice should come before bob");
    assert!(bob_pos < charlie_pos, "bob should come before charlie");
}

#[test]
fn test_parquet_sort_descending() {
    let id = unique_id();
    let output = run_script("from tests/data/sort_test.parquet\nsort -name\n", id);
    let alice_pos = output.find("alice").expect("Should contain alice");
    let bob_pos = output.find("bob").expect("Should contain bob");
    let charlie_pos = output.find("charlie").expect("Should contain charlie");
    assert!(charlie_pos < bob_pos, "charlie should come before bob");
    assert!(bob_pos < alice_pos, "bob should come before alice");
}

#[test]
fn test_parquet_meta() {
    let id = unique_id();
    let output = run_script("from tests/data/meta_test.parquet\nmeta\n", id);
    assert!(output.contains("metadata"), "Should show metadata view");
    assert!(output.contains("a") && output.contains("b"), "Should list columns");
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
    fs::remove_file(&pq_path).ok();
}

/// Filtered parquet view shows correct row count from disk via SQL count(*)
#[test]
fn test_parquet_filtered_count() {
    let id = unique_id();
    // filtered_test.parquet: 100k rows, 40% A, 60% B
    let output = run_script("from tests/data/filtered_test.parquet\nfilter \"sym\" = 'B'\n", id);
    assert!(output.contains("60,000") || output.contains("60000"),
        "Filtered view should show 60,000 rows, got: {}", output);
}

/// Filtered parquet view freq runs against disk (not memory)
#[test]
fn test_parquet_filtered_freq() {
    let id = unique_id();
    // filtered_test.parquet: 100k rows, sym 40%A/60%B, cat 25% each X,Y,Z,W
    // Filter for A (40k), freq on cat should show 10k each
    let output = run_script("from tests/data/filtered_test.parquet\nfilter \"sym\" = 'A'\nfreq cat\n", id);
    assert!(output.contains("Freq:cat") && output.contains("10"),
        "Filtered freq should show ~10,000 counts, got: {}", output);
}
