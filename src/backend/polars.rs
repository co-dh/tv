//! Polars backend - native streaming engine for parquet files.
//! All parquet operations (load, save, fetch, freq, filter, distinct, etc.)
use super::{Backend, LoadResult};
use crate::command::io::convert::convert_epoch_cols;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::path::Path;

/// Polars streaming backend. Zero-copy parquet access via LazyFrame.
pub struct Polars;

impl Polars {
    /// Create LazyFrame from parquet path
    fn lf(&self, path: &str) -> Result<LazyFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default()).map_err(|e| anyhow!("Scan: {}", e))
    }
}

impl Backend for Polars {
    /// Column names from parquet schema
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        Ok(self.schema(path)?.into_iter().map(|(n, _)| n).collect())
    }

    /// Schema as (name, type) pairs from parquet metadata
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let file = std::fs::File::open(path)?;
        let schema = ParquetReader::new(file).schema()?;
        Ok(schema.iter().map(|(n, f)| (n.to_string(), format!("{:?}", f.dtype()))).collect())
    }

    /// Row count and column names from parquet metadata (no data scan)
    fn metadata(&self, path: &str) -> Result<(usize, Vec<String>)> {
        let file = std::fs::File::open(path).map_err(|e| anyhow!("Open: {}", e))?;
        let mut reader = ParquetReader::new(file);
        let rows = reader.get_metadata().map_err(|e| anyhow!("Metadata: {}", e))?.num_rows;
        let cols = reader.schema().map_err(|e| anyhow!("Schema: {}", e))?
            .iter_names().map(|s| s.to_string()).collect();
        Ok((rows, cols))
    }

    /// Fetch row window via LazyFrame slice (streaming, no full scan)
    fn fetch_rows(&self, path: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())
            .map_err(|e| anyhow!("Scan: {}", e))?
            .slice(offset as i64, limit as u32)
            .collect()
            .map_err(|e| anyhow!("Fetch: {}", e))
    }

    /// Fetch rows with WHERE clause (streaming)
    fn fetch_where(&self, path: &str, w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        super::sql(self.lf(path)?, &format!("SELECT * FROM df WHERE {} LIMIT {} OFFSET {}", w, limit, offset))
    }

    /// Count rows matching WHERE clause (streaming)
    fn count_where(&self, path: &str, w: &str) -> Result<usize> {
        let r = super::sql(self.lf(path)?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

    /// Distinct values via LazyFrame unique (streaming)
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

    /// Frequency count via streaming group_by (handles large files)
    fn freq(&self, path: &str, name: &str) -> Result<DataFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?
            .group_by([col(name)])
            .agg([len().alias("Cnt")])
            .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
            .collect_with_engine(Engine::Streaming)
            .map_err(|e| anyhow!("{}", e))
    }

    /// Frequency count with WHERE clause (streaming)
    fn freq_where(&self, path: &str, col: &str, w: &str) -> Result<DataFrame> {
        super::sql(self.lf(path)?, &format!("SELECT \"{}\", COUNT(*) as Cnt FROM df WHERE {} GROUP BY \"{}\" ORDER BY Cnt DESC", col, w, col))
    }

    /// Filter via SQL WHERE with LIMIT (streaming)
    fn filter(&self, path: &str, w: &str, limit: usize) -> Result<DataFrame> {
        super::sql(self.lf(path)?, &format!("SELECT * FROM df WHERE {} LIMIT {}", w, limit))
    }

    /// Sort and limit via LazyFrame (streaming to avoid OOM on large files)
    fn sort_head(&self, path: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?
            .sort([col], SortMultipleOptions::default().with_order_descending(desc))
            .limit(limit as u32)
            .collect_with_engine(Engine::Streaming)
            .map_err(|e| anyhow!("{}", e))
    }

    /// Load CSV, parquet, or glob pattern into ViewState
    fn load(&self, path: &str, id: usize) -> Result<LoadResult> {
        const MAX_PREVIEW: u32 = 100_000;
        // Glob pattern
        if path.contains('*') || path.contains('?') {
            let df = load_glob(path, MAX_PREVIEW)?;
            if df.height() == 0 { return Err(anyhow!("No data found")); }
            let df = convert_epoch_cols(df);
            return Ok(LoadResult { view: ViewState::new(id, path.into(), df, None), bg_loader: None });
        }
        let p = Path::new(path);
        if !p.exists() { return Err(anyhow!("File not found: {}", path)); }
        match p.extension().and_then(|s| s.to_str()) {
            Some("csv") => {
                let df = convert_epoch_cols(load_csv(p)?);
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
