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

/// Save dataframe to parquet
pub fn save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df)
        .map_err(|e| anyhow!("Failed to write Parquet: {}", e))?;
    Ok(())
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
    fn test_freq_from_disk() {
        let tmp = std::env::temp_dir().join("test_freq_disk.parquet");
        // Create parquet: 1000 rows, category A (first 100), B (next 400), C (last 500)
        let df = DataFrame::new(vec![
            Column::new("cat".into(), (0..1000).map(|i| {
                if i < 100 { "A" } else if i < 500 { "B" } else { "C" }
            }).collect::<Vec<&str>>()),
        ]).unwrap();
        save(&df, &tmp).unwrap();

        // Freq from disk should see all 3 categories
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

        // Get distinct from disk - should see all 10 values
        let disk_unique = distinct(&tmp, "id").unwrap();
        assert_eq!(disk_unique.len(), 10, "Should get all 10 distinct values from disk");

        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn test_lazy_parquet_viewport_fetch() {
        let tmp = std::env::temp_dir().join("test_lazy_viewport.parquet");
        // Create parquet: 1000 rows with known values
        let df = DataFrame::new(vec![
            Column::new("id".into(), (0..1000).collect::<Vec<i32>>()),
            Column::new("val".into(), (0..1000).map(|i| i * 10).collect::<Vec<i32>>()),
        ]).unwrap();
        save(&df, &tmp).unwrap();

        // Fetch rows 100-149 (50 rows)
        let window = fetch_rows(&tmp, 100, 50).unwrap();
        assert_eq!(window.height(), 50, "Should fetch exactly 50 rows");
        // First row should have id=100
        let first_id = window.column("id").unwrap().get(0).unwrap();
        assert_eq!(first_id.to_string(), "100", "First row should have id=100");

        let _ = std::fs::remove_file(tmp);
    }
}
