//! Parquet backend tests - key-based
mod common;
use common::{run_keys};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use polars::prelude::*;

static TEST_ID: AtomicUsize = AtomicUsize::new(4000);
fn tid() -> usize { TEST_ID.fetch_add(1, Ordering::SeqCst) }

// Test data files:
// - tests/data/freq_test.parquet: sym column with A/B values
// - tests/data/sort_test.parquet: name column with alice/bob/charlie
// - tests/data/meta_test.parquet: columns a, b
// - tests/data/filtered_test.parquet: 100k rows, sym 40%A/60%B, cat X/Y/Z/W

#[test]
fn test_parquet_filter() {
    // Filter sym = 'B' using backslash
    let out = run_keys("<backslash>sym = 'B'<ret>", "tests/data/freq_test.parquet");
    assert!(out.contains("rows") && !out.contains("(0 rows)"),
        "Filter should find B rows: {}", out);
}

#[test]
fn test_parquet_freq_enter() {
    // F for freq on sym, then enter to filter
    let out = run_keys("F<ret>", "tests/data/freq_test.parquet");
    assert!(out.contains("rows") && !out.contains("(0 rows)"),
        "Freq enter should filter: {}", out);
}

#[test]
fn test_parquet_sort_ascending() {
    // [ for sort ascending on name column
    let out = run_keys("[", "tests/data/sort_test.parquet");
    let alice = out.find("alice").expect("Should have alice");
    let bob = out.find("bob").expect("Should have bob");
    let charlie = out.find("charlie").expect("Should have charlie");
    assert!(alice < bob, "alice before bob");
    assert!(bob < charlie, "bob before charlie");
}

#[test]
fn test_parquet_sort_descending() {
    // ] for sort descending
    let out = run_keys("]", "tests/data/sort_test.parquet");
    let alice = out.find("alice").expect("Should have alice");
    let bob = out.find("bob").expect("Should have bob");
    let charlie = out.find("charlie").expect("Should have charlie");
    assert!(charlie < bob, "charlie before bob");
    assert!(bob < alice, "bob before alice");
}

#[test]
fn test_parquet_meta() {
    let out = run_keys("M", "tests/data/meta_test.parquet");
    assert!(out.contains("metadata"), "Should show metadata: {}", out);
    assert!(out.contains("a") && out.contains("b"), "Should list columns");
}

#[test]
fn test_parquet_time_roundtrip() {
    let id = tid();
    let pq = format!("tmp/tv_pq_time_{}.parquet", id);

    let ns: Vec<i64> = vec![3600_000_000_000, 7200_000_000_000, 10800_000_000_000];
    let time_series = Series::new("event_time".into(), ns)
        .cast(&DataType::Time).unwrap();
    let mut df = DataFrame::new(vec![time_series.into()]).unwrap();

    ParquetWriter::new(std::fs::File::create(&pq).unwrap())
        .finish(&mut df).unwrap();

    let loaded = ParquetReader::new(std::fs::File::open(&pq).unwrap())
        .finish().unwrap();

    assert!(matches!(loaded.column("event_time").unwrap().dtype(), DataType::Time),
        "Time col should remain Time: {:?}", loaded.column("event_time").unwrap().dtype());
    fs::remove_file(&pq).ok();
}

#[test]
fn test_parquet_filtered_count() {
    // Filter for B (60% of 100k = 60k rows)
    let out = run_keys("<backslash>sym = 'B'<ret>", "tests/data/filtered_test.parquet");
    assert!(out.contains("60,000") || out.contains("60000"),
        "Filtered should show 60,000 rows: {}", out);
}

#[test]
fn test_parquet_filtered_freq() {
    // Filter for A (40k rows), then freq on cat (should show ~10k each)
    let out = run_keys("<backslash>sym = 'A'<ret><right>F", "tests/data/filtered_test.parquet");
    assert!(out.contains("Freq:") && out.contains("10"),
        "Filtered freq should show ~10k counts: {}", out);
}

// === Chained filter tests ===

#[test]
fn test_parquet_chained_filter_count() {
    // Filter sym='A' (40k), then filter cat='X' (10k) - should query disk
    let out = run_keys("<backslash>sym = 'A'<ret><backslash>cat = 'X'<ret>", "tests/data/filtered_test.parquet");
    assert!(out.contains("10000") || out.contains("10,000"),
        "Chained filter should show 10,000 rows: {}", out);
}

#[test]
fn test_parquet_chained_filter_name() {
    // Chained filter view name should show both filters
    let out = run_keys("<backslash>sym = 'A'<ret><backslash>cat = 'X'<ret>", "tests/data/filtered_test.parquet");
    assert!(out.contains("& sym") && out.contains("& cat"),
        "View name should show chained filters: {}", out);
}

#[test]
fn test_parquet_filtered_freq_on_disk() {
    // Freq on filtered view should query disk, not memory
    // Filter for A (40k), freq on cat shows 4 values with ~10k each
    let out = run_keys("<backslash>sym = 'A'<ret><right>F", "tests/data/filtered_test.parquet");
    assert!(out.contains("(4 rows)"), "Filtered freq should have 4 cat values: {}", out);
    assert!(out.contains("10000") || out.contains("10,000"),
        "Each cat should have ~10k count: {}", out);
}

#[test]
fn test_parquet_filter_shows_total_rows() {
    // Filtered view should show total matching rows in header
    let out = run_keys("<backslash>sym = 'B'<ret>", "tests/data/filtered_test.parquet");
    assert!(out.contains("60000") || out.contains("60,000"),
        "Filter B should show 60,000 total rows: {}", out);
}

// === Freq Enter should create lazy filtered view ===

#[test]
fn test_parquet_freq_enter_shows_total() {
    // Freq on sym (sorted desc: B=60k, A=40k), Enter on B should show 60k rows
    let out = run_keys("F<ret>", "tests/data/filtered_test.parquet");
    // Should show 60,000 rows for sym=B, NOT 10,000
    assert!(out.contains("60000") || out.contains("60,000"),
        "Freq enter should show 60,000 total rows (not 10k): {}", out);
    assert!(!out.contains("(10000 rows)") && !out.contains("(10,000 rows)"),
        "Should NOT be limited to 10k: {}", out);
}

#[test]
fn test_parquet_freq_enter_then_freq() {
    // Freq on sym, Enter on B (60k), then Freq on cat should show 4 values with 15k each
    let out = run_keys("F<ret><right>F", "tests/data/filtered_test.parquet");
    assert!(out.contains("(4 rows)"), "Freq on cat should have 4 values: {}", out);
    assert!(out.contains("15000") || out.contains("15,000"),
        "Each cat should have ~15k count (25% of 60k): {}", out);
}

// === Large parquet workflow tests (1.parquet ~300M rows) ===

#[test]
fn test_large_parquet_freq_enter_single() {
    // Freq Exchange (18 vals), Enter filters by P, freq Exchange shows 1 row
    let out = run_keys("<right>F<ret><right>F<a-p>", "tests/data/nyse/1.parquet");
    assert!(out.contains("rows=1"), "Filtered freq should show 1 row: {}", out);
}

#[test]
fn test_large_parquet_filter_not_10k() {
    // Filter by Exchange=P should show 94M rows, not 10k
    let out = run_keys("<right>F<ret><ret><a-p>", "tests/data/nyse/1.parquet");
    assert!(!out.contains("rows=10000"), "Should NOT be limited to 10k: {}", out);
    assert!(out.contains("94874100"), "Should show 94M rows: {}", out);
}

#[test]
fn test_large_parquet_filtered_freq_symbol() {
    // Filter Exchange=P, then freq Symbol should have >1000 unique values
    let out = run_keys("<right>F<ret><ret><right><right>F<a-p>", "tests/data/nyse/1.parquet");
    assert!(out.contains("rows=11342") || out.contains("(11342 rows)") || out.contains("11,342"),
        "Symbol freq should have 11342 rows (way more than 4): {}", out);
}
