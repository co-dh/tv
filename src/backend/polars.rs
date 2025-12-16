//! Polars backend - native streaming engine for parquet files.
//! All parquet operations (load, save, fetch, freq, filter, distinct, etc.)
use super::{Backend, LoadResult};
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::path::Path;

/// Polars streaming backend. Zero-copy parquet access via LazyFrame.
pub struct Polars;

impl Backend for Polars {
    /// LazyFrame from parquet scan
    fn lf(&self, path: &str) -> Result<LazyFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default()).map_err(|e| anyhow!("Scan: {}", e))
    }

    /// Load CSV, parquet, or glob pattern into ViewState
    fn load(&self, path: &str, id: usize) -> Result<LoadResult> {
        const MAX_PREVIEW: u32 = 100_000;
        // Glob pattern
        if path.contains('*') || path.contains('?') {
            let df = load_glob(path, MAX_PREVIEW)?;
            if df.height() == 0 { return Err(anyhow!("No data found")); }
            return Ok(LoadResult { view: ViewState::new(id, path.into(), df, None), bg_loader: None });
        }
        let p = Path::new(path);
        if !p.exists() { return Err(anyhow!("File not found: {}", path)); }
        match p.extension().and_then(|s| s.to_str()) {
            Some("csv") => {
                let df = load_csv(p)?;
                if df.height() == 0 { return Err(anyhow!("File is empty")); }
                Ok(LoadResult { view: ViewState::new(id, path.into(), df, Some(path.into())), bg_loader: None })
            }
            Some("parquet") => {
                let (rows, cols) = self.metadata(path)?;
                if rows == 0 { return Err(anyhow!("File is empty")); }
                Ok(LoadResult { view: ViewState::new_parquet(id, path.into(), path.into(), rows, cols), bg_loader: None })
            }
            Some(ext) => Err(anyhow!("Unsupported: {}", ext)),
            None => Err(anyhow!("Unknown file type")),
        }
    }
}

// ── CSV operations ──────────────────────────────────────────────────────────

/// Detect separator by counting occurrences in header line
pub fn detect_sep(line: &str) -> u8 {
    let seps = [(b'|', line.matches('|').count()),
                (b'\t', line.matches('\t').count()),
                (b',', line.matches(',').count()),
                (b';', line.matches(';').count())];
    seps.into_iter().max_by_key(|&(_, n)| n).map(|(c, _)| c).unwrap_or(b',')
}

/// Load CSV file with auto-detected separator
pub fn load_csv(path: &Path) -> Result<DataFrame> {
    // Read first line to detect separator
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut header = String::new();
    std::io::BufRead::read_line(&mut reader, &mut header)?;
    let sep = detect_sep(&header);

    CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(100))
        .map_parse_options(|o| o.with_separator(sep))
        .try_into_reader_with_file_path(Some(path.to_path_buf()))?
        .finish()
        .map_err(|e| anyhow!("Failed to read CSV: {}", e))
}

/// Write CSV file
pub fn save_csv(df: &DataFrame, path: &Path) -> Result<()> {
    CsvWriter::new(&mut std::fs::File::create(path)?)
        .finish(&mut df.clone())
        .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
    Ok(())
}

/// Parse CSV buffer with separator (used by gz streaming)
pub fn parse_csv_buf(buf: Vec<u8>, sep: u8, schema_len: usize) -> Result<DataFrame> {
    CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(schema_len))
        .map_parse_options(|o| o.with_separator(sep))
        .into_reader_with_file_handle(std::io::Cursor::new(buf))
        .finish()
        .map_err(|e| anyhow!("Failed to parse: {}", e))
}

// ── Parquet operations ──────────────────────────────────────────────────────

/// Load parquet files matching glob pattern (preview only)
pub fn load_glob(pattern: &str, limit: u32) -> Result<DataFrame> {
    let lf = LazyFrame::scan_parquet(PlPath::new(pattern), ScanArgsParquet::default())
        .map_err(|e| anyhow!("Failed to scan parquet: {}", e))?;
    lf.limit(limit).collect()
        .map_err(|e| {
            if e.to_string().contains("mismatch") {
                schema_diff(pattern).unwrap_or_else(|| anyhow!("{}", e))
            } else { anyhow!("{}", e) }
        })
}

/// Find which parquet file has schema mismatch by comparing all files
fn schema_diff(pattern: &str) -> Option<anyhow::Error> {
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
    fn test_distinct_from_disk() {
        let tmp = std::env::temp_dir().join("test_distinct.parquet");
        let df = DataFrame::new(vec![
            Column::new("id".into(), (0..1000).map(|i| i % 10).collect::<Vec<i32>>()),
        ]).unwrap();
        Polars.save(&df, &tmp).unwrap();
        let disk_unique = Polars.distinct(tmp.to_str().unwrap(), "id").unwrap();
        assert_eq!(disk_unique.len(), 10, "Should get all 10 distinct values from disk");
        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn test_lazy_parquet_viewport_fetch() {
        let tmp = std::env::temp_dir().join("test_lazy_viewport.parquet");
        let df = DataFrame::new(vec![
            Column::new("id".into(), (0..1000).collect::<Vec<i32>>()),
            Column::new("val".into(), (0..1000).map(|i| i * 10).collect::<Vec<i32>>()),
        ]).unwrap();
        Polars.save(&df, &tmp).unwrap();
        let window = Polars.fetch_rows(tmp.to_str().unwrap(), 100, 50).unwrap();
        assert_eq!(window.height(), 50, "Should fetch exactly 50 rows");
        let first_id = window.column("id").unwrap().get(0).unwrap();
        assert_eq!(first_id.to_string(), "100", "First row should have id=100");
        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn test_parquet_sort_head() {
        let tmp = std::env::temp_dir().join("test_sort_head.parquet");
        let df = DataFrame::new(vec![
            Column::new("id".into(), vec![5, 2, 8, 1, 9, 3]),
        ]).unwrap();
        Polars.save(&df, &tmp).unwrap();
        // Sort ascending, take 3
        let r = Polars.sort_head(tmp.to_str().unwrap(), "id", false, 3).unwrap();
        assert_eq!(r.height(), 3);
        assert_eq!(r.column("id").unwrap().get(0).unwrap().try_extract::<i32>().unwrap(), 1);
        assert_eq!(r.column("id").unwrap().get(2).unwrap().try_extract::<i32>().unwrap(), 3);
        // Sort descending, take 2
        let r = Polars.sort_head(tmp.to_str().unwrap(), "id", true, 2).unwrap();
        assert_eq!(r.height(), 2);
        assert_eq!(r.column("id").unwrap().get(0).unwrap().try_extract::<i32>().unwrap(), 9);
        assert_eq!(r.column("id").unwrap().get(1).unwrap().try_extract::<i32>().unwrap(), 8);
        let _ = std::fs::remove_file(tmp);
    }
}
