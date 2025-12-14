//! Polars backend - native streaming engine for parquet files.
//! All parquet operations (load, save, fetch, freq, filter, distinct, etc.)
use super::Backend;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::path::Path;

/// Polars streaming backend. Zero-copy parquet access via LazyFrame.
pub struct Polars;

impl Backend for Polars {
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        Ok(self.schema(path)?.into_iter().map(|(n, _)| n).collect())
    }

    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let file = std::fs::File::open(path)?;
        let schema = ParquetReader::new(file).schema()?;
        Ok(schema.iter().map(|(n, f)| (n.to_string(), format!("{:?}", f.dtype()))).collect())
    }

    fn metadata(&self, path: &str) -> Result<(usize, Vec<String>)> {
        let file = std::fs::File::open(path).map_err(|e| anyhow!("Open: {}", e))?;
        let mut reader = ParquetReader::new(file);
        let rows = reader.get_metadata().map_err(|e| anyhow!("Metadata: {}", e))?.num_rows;
        let cols = reader.schema().map_err(|e| anyhow!("Schema: {}", e))?
            .iter_names().map(|s| s.to_string()).collect();
        Ok((rows, cols))
    }

    fn fetch_rows(&self, path: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())
            .map_err(|e| anyhow!("Scan: {}", e))?
            .slice(offset as i64, limit as u32)
            .collect()
            .map_err(|e| anyhow!("Fetch: {}", e))
    }

    fn distinct(&self, path: &str, name: &str) -> Result<Vec<String>> {
        let df = LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())
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

    fn save(&self, df: &DataFrame, path: &Path) -> Result<()> {
        let mut df = df.clone();
        ParquetWriter::new(std::fs::File::create(path)?)
            .finish(&mut df)
            .map_err(|e| anyhow!("Failed to write Parquet: {}", e))?;
        Ok(())
    }

    fn freq(&self, path: &str, name: &str) -> Result<DataFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?
            .group_by([col(name)])
            .agg([len().alias("Cnt")])
            .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
            .collect_with_engine(Engine::Streaming)
            .map_err(|e| anyhow!("{}", e))
    }

    fn filter(&self, path: &str, where_clause: &str) -> Result<DataFrame> {
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?);
        ctx.execute(&format!("SELECT * FROM df WHERE {}", where_clause))?
            .collect()
            .map_err(|e| anyhow!("{}", e))
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
}
