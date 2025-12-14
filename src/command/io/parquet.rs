//! Parquet loading/saving helpers
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::path::Path;

/// Convert &Path to PlPath for scan_parquet
fn to_plpath(path: &Path) -> PlPath { PlPath::Local(path.into()) }

/// Get parquet metadata (row count, column count) without loading data
pub fn metadata(path: &Path) -> Result<(usize, usize)> {
    let file = std::fs::File::open(path).map_err(|e| anyhow!("Open: {}", e))?;
    let mut reader = ParquetReader::new(file);
    let rows = reader.get_metadata().map_err(|e| anyhow!("Metadata: {}", e))?.num_rows;
    let cols = reader.schema().map_err(|e| anyhow!("Schema: {}", e))?.len();
    Ok((rows, cols))
}

/// Load parquet file (lazy with limit). Returns (df, total_rows_on_disk)
pub fn load(path: &Path, limit: u32) -> Result<(DataFrame, usize)> {
    // Get total row count from parquet metadata (reads only footer, not data)
    let file = std::fs::File::open(path).map_err(|e| anyhow!("Open: {}", e))?;
    let total = ParquetReader::new(file).get_metadata()
        .map(|m| m.num_rows).unwrap_or(0);
    // Lazy load with limit
    let args = ScanArgsParquet::default();
    let df = LazyFrame::scan_parquet(to_plpath(path), args)
        .map_err(|e| anyhow!("Failed to scan parquet: {}", e))?
        .limit(limit).collect()
        .map_err(|e| anyhow!("Failed to read Parquet: {}", e))?;
    Ok((df, total))
}

/// Save dataframe to parquet
pub fn save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df)
        .map_err(|e| anyhow!("Failed to write Parquet: {}", e))?;
    Ok(())
}

/// Filter parquet file on disk and return result (polars lazy)
pub fn filter(path: &Path, expr: Expr) -> Result<DataFrame> {
    let args = ScanArgsParquet::default();
    LazyFrame::scan_parquet(to_plpath(path), args)
        .map_err(|e| anyhow!("Scan: {}", e))?
        .filter(expr)
        .collect()
        .map_err(|e| anyhow!("Filter: {}", e))
}

/// Count rows matching filter from parquet file on disk (polars lazy)
pub fn filter_count(path: &Path, expr: Expr) -> Result<usize> {
    let args = ScanArgsParquet::default();
    let df = LazyFrame::scan_parquet(to_plpath(path), args)
        .map_err(|e| anyhow!("Scan: {}", e))?
        .filter(expr)
        .select([len()])
        .collect()
        .map_err(|e| anyhow!("Filter: {}", e))?;
    Ok(df.column("len").ok()
        .and_then(|c| c.u32().ok())
        .and_then(|c| c.get(0))
        .unwrap_or(0) as usize)
}

/// Get frequency counts for a column from parquet file on disk (streaming)
pub fn freq_from_disk(path: &Path, name: &str) -> Result<DataFrame> {
    let args = ScanArgsParquet::default();
    LazyFrame::scan_parquet(to_plpath(path), args)
        .map_err(|e| anyhow!("Scan: {}", e))?
        .group_by([col(name)])
        .agg([len().alias("Cnt")])
        .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
        .collect_with_engine(Engine::Streaming)
        .map_err(|e| anyhow!("Freq: {}", e))
}

/// Fetch window of rows from parquet (for rendering visible viewport)
pub fn fetch_rows(path: &Path, offset: usize, limit: usize) -> Result<DataFrame> {
    let args = ScanArgsParquet::default();
    LazyFrame::scan_parquet(to_plpath(path), args)
        .map_err(|e| anyhow!("Scan: {}", e))?
        .slice(offset as i64, limit as u32)
        .collect()
        .map_err(|e| anyhow!("Fetch: {}", e))
}

/// Get schema (column names and types) from parquet without loading data
pub fn schema(path: &Path) -> Result<Vec<(String, String)>> {
    let file = std::fs::File::open(path).map_err(|e| anyhow!("Open: {}", e))?;
    let schema = ParquetReader::new(file).schema()
        .map_err(|e| anyhow!("Schema: {}", e))?;
    Ok(schema.iter().map(|(n, f)| (n.to_string(), format!("{:?}", f.dtype()))).collect())
}

/// Get distinct values for a column from parquet file on disk (lazy, no full load)
pub fn distinct(path: &Path, name: &str) -> Result<Vec<String>> {
    let args = ScanArgsParquet::default();
    let df = LazyFrame::scan_parquet(to_plpath(path), args)
        .map_err(|e| anyhow!("Scan: {}", e))?
        .select([col(name)])
        .unique(None, UniqueKeepStrategy::First)
        .sort([name], SortMultipleOptions::default())
        .collect()
        .map_err(|e| anyhow!("Distinct: {}", e))?;
    let vals: Vec<String> = df.column(name)
        .map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect())
        .unwrap_or_default();
    Ok(vals)
}

/// Load parquet files matching glob pattern (preview only)
pub fn load_glob(pattern: &str, limit: u32) -> Result<DataFrame> {
    let args = ScanArgsParquet::default();
    let lf = LazyFrame::scan_parquet(PlPath::new(pattern), args)
        .map_err(|e| anyhow!("Failed to scan parquet: {}", e))?;
    lf.limit(limit).collect()
        .map_err(|e| {
            if e.to_string().contains("mismatch") {
                find_schema_mismatch(pattern).unwrap_or_else(|| anyhow!("{}", e))
            } else { anyhow!("{}", e) }
        })
}

/// Find which parquet file has schema mismatch by comparing all files
fn find_schema_mismatch(pattern: &str) -> Option<anyhow::Error> {
    let output = std::process::Command::new("sh")
        .args(["-c", &format!("ls -1 {} 2>/dev/null", pattern)])
        .output().ok()?;
    let paths: Vec<&str> = std::str::from_utf8(&output.stdout).ok()?
        .lines().filter(|l| !l.is_empty()).collect();
    if paths.len() < 2 { return None; }

    let first = paths[0];
    let base_schema = ParquetReader::new(std::fs::File::open(first).ok()?).schema().ok()?;

    for path in &paths[1..] {
        if let Ok(file) = std::fs::File::open(path) {
            if let Ok(schema) = ParquetReader::new(file).schema() {
                for (name, dtype) in schema.iter() {
                    let name_str = name.as_str();
                    if let Some((_, base_dtype)) = base_schema.iter().find(|(n, _)| n.as_str() == name_str) {
                        if dtype != base_dtype {
                            return Some(anyhow!("Schema mismatch for '{}': {} has {:?}, {} has {:?}",
                                name_str, first, base_dtype.dtype(), path, dtype.dtype()));
                        }
                    }
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_from_disk() {
        let tmp = std::env::temp_dir().join("test_filter_disk.parquet");
        // Create parquet: 1000 rows, id 0-999, only rows 500-999 have val > 0
        let df = DataFrame::new(vec![
            Column::new("id".into(), (0..1000).collect::<Vec<i32>>()),
            Column::new("val".into(), (0..1000).map(|i| if i >= 500 { 1 } else { 0 }).collect::<Vec<i32>>()),
        ]).unwrap();
        save(&df, &tmp).unwrap();

        // Load only first 100 rows - all have val=0, none match val=1
        let (df, _) = load(&tmp, 100).unwrap();
        let sum: i32 = df.column("val").unwrap().as_materialized_series().i32().unwrap().sum().unwrap_or(0);
        assert!(sum == 0, "First 100 rows all have val=0");

        // Filter from disk should find 500 rows with val=1
        let count = filter_count(&tmp, col("val").eq(lit(1))).unwrap();
        assert_eq!(count, 500, "Should find 500 rows with val=1 from disk");

        // Filter result should return actual rows from disk
        let filtered = filter(&tmp, col("val").eq(lit(1))).unwrap();
        assert_eq!(filtered.height(), 500, "Filter should return 500 rows from disk");

        let _ = std::fs::remove_file(tmp);
    }


    #[test]
    fn test_freq_from_disk() {
        let tmp = std::env::temp_dir().join("test_freq_disk.parquet");
        // Create parquet: 1000 rows, category A (first 100), B (next 400), C (last 500)
        let df = DataFrame::new(vec![
            Column::new("cat".into(), (0..1000).map(|i| {
                if i < 100 { "A" } else if i < 500 { "B" } else { "C" }
            }).collect::<Vec<&str>>()),
        ]).unwrap();
        save(&df, &tmp).unwrap();

        // Load only first 50 rows - all category A
        let (df, _) = load(&tmp, 50).unwrap();
        let in_mem_cats = df.column("cat").unwrap().unique().unwrap().len();
        assert_eq!(in_mem_cats, 1, "First 50 rows only have category A");

        // Freq from disk should see all 3 categories with correct counts
        let freq = freq_from_disk(&tmp, "cat").unwrap();
        assert_eq!(freq.height(), 3, "Should have 3 categories from disk");

        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn test_distinct_from_disk() {
        let tmp = std::env::temp_dir().join("test_distinct.parquet");
        // Create parquet with 1000 rows, column has 10 distinct values (0-9)
        let df = DataFrame::new(vec![
            Column::new("id".into(), (0..1000).map(|i| i % 10).collect::<Vec<i32>>()),
        ]).unwrap();
        save(&df, &tmp).unwrap();

        // Load only 5 rows - in memory we might only see a few distinct values
        let (df, _) = load(&tmp, 5).unwrap();
        let in_mem_unique = df.column("id").unwrap().unique().unwrap().len();

        // Get distinct from disk - should see all 10 values
        let disk_unique = distinct(&tmp, "id").unwrap();
        assert_eq!(disk_unique.len(), 10, "Should get all 10 distinct values from disk");
        assert!(in_mem_unique <= 5, "In-memory should have at most 5 unique (loaded 5 rows)");

        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn test_lazy_load_with_limit() {
        let tmp = std::env::temp_dir().join("test_lazy_load.parquet");
        // Create parquet with 1000 rows
        let df = DataFrame::new(vec![
            Column::new("id".into(), (0..1000).collect::<Vec<i32>>()),
            Column::new("val".into(), (0..1000).map(|i| i as f64 * 0.1).collect::<Vec<f64>>()),
        ]).unwrap();
        save(&df, &tmp).unwrap();

        // Load with limit=100: df has 100, disk_rows = 1000
        let (df, total) = load(&tmp, 100).unwrap();
        assert_eq!(df.height(), 100);
        assert_eq!(df.width(), 2);
        assert_eq!(total, 1000);

        // Load with limit=500: df has 500, disk_rows = 1000
        let (df, total) = load(&tmp, 500).unwrap();
        assert_eq!(df.height(), 500);
        assert_eq!(total, 1000);

        // Load with limit > file rows: df has 1000, disk_rows = 1000
        let (df, total) = load(&tmp, 2000).unwrap();
        assert_eq!(df.height(), 1000);
        assert_eq!(total, 1000);

        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn test_lazy_parquet_viewport_fetch() {
        // Test fetch_rows for lazy parquet - fetch only visible window
        let tmp = std::env::temp_dir().join("test_lazy_viewport.parquet");
        // Create parquet: 1000 rows with known values
        let df = DataFrame::new(vec![
            Column::new("id".into(), (0..1000).collect::<Vec<i32>>()),
            Column::new("val".into(), (0..1000).map(|i| i * 10).collect::<Vec<i32>>()),
        ]).unwrap();
        save(&df, &tmp).unwrap();

        // Lazy load: just metadata, no rows loaded
        let (rows, cols) = metadata(&tmp).unwrap();
        assert_eq!(rows, 1000, "Metadata should report 1000 rows");
        assert_eq!(cols, 2, "Metadata should report 2 columns");

        // Fetch rows 100-149 (50 rows)
        let window = fetch_rows(&tmp, 100, 50).unwrap();
        assert_eq!(window.height(), 50, "Should fetch exactly 50 rows");
        // First row should have id=100
        let first_id = window.column("id").unwrap().get(0).unwrap();
        assert_eq!(first_id.to_string(), "100", "First row should have id=100");

        // Fetch rows at end (950-999, 50 rows)
        let end_window = fetch_rows(&tmp, 950, 100).unwrap();
        assert_eq!(end_window.height(), 50, "Should only get 50 rows at end");
        let last_id = end_window.column("id").unwrap().get(49).unwrap();
        assert_eq!(last_id.to_string(), "999", "Last row should have id=999");

        let _ = std::fs::remove_file(tmp);
    }
}
