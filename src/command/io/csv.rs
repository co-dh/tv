//! CSV loading helpers
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::path::Path;

/// Detect separator by counting occurrences in header line
pub fn detect_sep(line: &str) -> u8 {
    let seps = [(b'|', line.matches('|').count()),
                (b'\t', line.matches('\t').count()),
                (b',', line.matches(',').count()),
                (b';', line.matches(';').count())];
    seps.into_iter().max_by_key(|&(_, n)| n).map(|(c, _)| c).unwrap_or(b',')
}

/// Parse CSV buffer with separator
pub fn parse_buf(buf: Vec<u8>, sep: u8, schema_len: usize) -> Result<DataFrame> {
    CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(schema_len))
        .map_parse_options(|o| o.with_separator(sep))
        .into_reader_with_file_handle(std::io::Cursor::new(buf))
        .finish()
        .map_err(|e| anyhow!("Failed to parse: {}", e))
}

/// Load CSV file
pub fn load(path: &Path) -> Result<DataFrame> {
    CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(100))
        .map_parse_options(|o| o.with_truncate_ragged_lines(true))
        .try_into_reader_with_file_path(Some(path.to_path_buf()))?
        .finish()
        .map_err(|e| anyhow!("Failed to read CSV: {}", e))
}

/// Write CSV file
pub fn save(df: &DataFrame, path: &Path) -> Result<()> {
    CsvWriter::new(&mut std::fs::File::create(path)?)
        .finish(&mut df.clone())
        .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_sep() {
        assert_eq!(detect_sep("a,b,c,d"), b',');
        assert_eq!(detect_sep("a|b|c|d"), b'|');
        assert_eq!(detect_sep("a\tb\tc\td"), b'\t');
        assert_eq!(detect_sep("a;b;c;d"), b';');
    }
}
