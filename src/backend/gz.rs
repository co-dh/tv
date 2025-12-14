//! Gz backend - streaming gzipped CSV with memory limits.
//! Refuses expensive operations on partial (memory-limited) data.
use super::{Backend, df_filter, df_sort_head, df_distinct, df_save};
use crate::command::io::convert::{convert_epoch_cols, apply_schema, convert_types};
use super::polars::{detect_sep, parse_csv_buf};
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;

const MIN_ROWS: usize = 1_000;
const CHUNK_ROWS: usize = 100_000;
const FIRST_CHUNK_ROWS: usize = 100_000;

/// Gz backend: wraps DataFrame loaded from .csv.gz
/// - Refuses expensive ops (freq, filter) on partial data
/// - partial = true means file hit memory limit, not fully loaded
pub struct Gz<'a> {
    pub df: &'a DataFrame,
    pub partial: bool,
}

impl Backend for Gz<'_> {
    fn cols(&self, _: &str) -> Result<Vec<String>> {
        Ok(self.df.get_column_names().iter().map(|s| s.to_string()).collect())
    }

    fn schema(&self, _: &str) -> Result<Vec<(String, String)>> {
        Ok(self.df.schema().iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

    fn metadata(&self, _: &str) -> Result<(usize, Vec<String>)> {
        Ok((self.df.height(), self.df.get_column_names().iter().map(|s| s.to_string()).collect()))
    }

    fn fetch_rows(&self, _: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        Ok(self.df.slice(offset as i64, limit))
    }

    fn distinct(&self, _: &str, col: &str) -> Result<Vec<String>> {
        if self.partial { return Err(anyhow!("File not fully loaded (memory limit)")); }
        df_distinct(self.df, col)
    }

    fn save(&self, df: &DataFrame, path: &Path) -> Result<()> { df_save(df, path) }

    fn freq(&self, _: &str, col: &str) -> Result<DataFrame> {
        if self.partial { return Err(anyhow!("File not fully loaded (memory limit)")); }
        self.df.column(col)?.as_materialized_series()
            .value_counts(true, false, "Cnt".into(), false)
            .map_err(|e| anyhow!("{}", e))
    }

    fn filter(&self, _: &str, w: &str) -> Result<DataFrame> {
        if self.partial { return Err(anyhow!("File not fully loaded (memory limit)")); }
        df_filter(self.df, w)
    }

    fn sort_head(&self, _: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> { df_sort_head(self.df, col, desc, limit) }
}

// ── Streaming load ──────────────────────────────────────────────────────────

/// Streaming chunk: Some(df) = more data, None = EOF (fully loaded)
pub type GzChunk = Option<DataFrame>;

/// Load gz file with streaming: returns initial chunk + background loader
pub fn load_streaming(path: &Path, mem_limit: u64) -> Result<(DataFrame, Option<Receiver<GzChunk>>)> {
    let mut child = Command::new("zcat")
        .arg(path).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| anyhow!("Failed to spawn zcat: {}", e))?;

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("No stdout"))?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, stdout);

    // Read header
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let sep = detect_sep(&header);
    let header_bytes = header.as_bytes().to_vec();

    // Read first chunk
    let mut buf = header.clone().into_bytes();
    let (mut lines, mut total_bytes) = (0usize, 0u64);
    while lines < MIN_ROWS.max(CHUNK_ROWS) {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(n) => { buf.extend_from_slice(line.as_bytes()); total_bytes += n as u64; lines += 1; }
            Err(_) => break,
        }
    }
    if lines == 0 { return Err(anyhow!("Empty file")); }

    let raw_df = parse_csv_buf(buf, sep, 500)?;
    let schema = Arc::clone(raw_df.schema());
    let df = convert_epoch_cols(raw_df);

    // EOF or mem limit reached - no background loading
    if lines < CHUNK_ROWS || total_bytes * 2 > mem_limit {
        let _ = child.wait();
        return Ok((df, None));
    }

    // Background streaming
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || stream_chunks(reader, header_bytes, sep, mem_limit, total_bytes, tx, child, schema));
    Ok((df, Some(rx)))
}

fn stream_chunks(
    mut reader: BufReader<ChildStdout>, header: Vec<u8>, sep: u8,
    mem_limit: u64, mut total_bytes: u64, tx: Sender<GzChunk>, mut child: Child, schema: Arc<Schema>,
) {
    loop {
        let mut buf = header.clone();
        let mut lines = 0usize;
        while lines < CHUNK_ROWS {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(n) => { buf.extend_from_slice(line.as_bytes()); total_bytes += n as u64; lines += 1; }
                Err(_) => break,
            }
        }
        if lines == 0 { let _ = tx.send(None); break; }
        let df = CsvReadOptions::default()
            .with_has_header(true).with_schema(Some(schema.clone()))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(std::io::Cursor::new(buf)).finish();
        if let Ok(df) = df { if tx.send(Some(convert_epoch_cols(df))).is_err() { break; } }
        if total_bytes * 2 > mem_limit { break; }  // partial - don't send None
    }
    let _ = child.kill();
    let _ = child.wait();
}

// ── Streaming save ──────────────────────────────────────────────────────────

fn commify(n: usize) -> String {
    let s = n.to_string();
    let mut r = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { r.push(','); }
        r.push(c);
    }
    r.chars().rev().collect()
}

/// Stream gz file to parquet (background)
pub fn stream_to_parquet(gz_path: &str, out_path: &Path, raw: bool) -> Receiver<String> {
    let (gz, out) = (gz_path.to_string(), out_path.to_path_buf());
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || { if let Err(e) = stream_save(&gz, &out, raw, &tx) { let _ = tx.send(format!("Error: {}", e)); } });
    rx
}

fn stream_save(gz_path: &str, out_path: &Path, raw: bool, tx: &Sender<String>) -> Result<()> {
    let mut child = Command::new("zcat")
        .arg(gz_path).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| anyhow!("zcat: {}", e))?;

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("No stdout"))?;
    let mut reader = BufReader::with_capacity(64 * 1024 * 1024, stdout);

    let mut header = String::new();
    reader.read_line(&mut header)?;
    let sep = detect_sep(&header);
    let col_count = header.split(sep as char).count();
    let header_bytes = header.as_bytes().to_vec();

    let mut read_chunk = |n: usize| -> Result<(Vec<u8>, usize)> {
        let mut buf = header_bytes.clone();
        let mut lines = 0usize;
        while lines < n {
            let mut line = String::new();
            if reader.read_line(&mut line)? == 0 { break; }
            buf.extend_from_slice(line.as_bytes());
            lines += 1;
        }
        Ok((buf, lines))
    };

    let str_schema = Arc::new(Schema::from_iter((0..col_count).map(|i| {
        Field::new(header.split(sep as char).nth(i).unwrap_or("").trim().into(), DataType::String)
    })));
    let parse = |buf: Vec<u8>| -> Result<DataFrame> {
        CsvReadOptions::default().with_has_header(true).with_schema(Some(str_schema.clone()))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(std::io::Cursor::new(buf)).finish()
            .map_err(|e| anyhow!("Parse: {}", e))
    };

    let (buf, lines) = read_chunk(FIRST_CHUNK_ROWS)?;
    if lines == 0 { return Err(anyhow!("Empty file")); }
    let first_df = if raw { parse(buf)? } else { convert_types(parse(buf)?) };
    let schema = first_df.schema().to_owned();

    let file = std::fs::File::create(out_path)?;
    let mut writer = ParquetWriter::new(file).batched(&schema)?;
    let mut total_rows = first_df.height();
    let mut first_df = first_df;
    first_df.rechunk_mut();
    writer.write_batch(&first_df)?;
    let _ = tx.send(format!("Written {} rows", commify(total_rows)));

    loop {
        let (buf, lines) = read_chunk(CHUNK_ROWS)?;
        if lines == 0 { break; }
        let df = parse(buf)?;
        let mut df = if raw { df } else { let (d, e) = apply_schema(df, &schema); if let Some(e) = e { let _ = tx.send(format!("Warning: {}", e)); } d };
        df.rechunk_mut();
        writer.write_batch(&df)?;
        total_rows += df.height();
        let _ = tx.send(format!("Written {} rows", commify(total_rows)));
    }

    writer.finish()?;
    let _ = child.wait();
    let _ = tx.send(format!("Done: {} rows", commify(total_rows)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gz_to_parquet() {
        let tmp = std::env::temp_dir();
        let csv_path = tmp.join("test_gz.csv");
        let gz_path = tmp.join("test_gz.csv.gz");
        let out_path = tmp.join("test_gz.parquet");

        std::fs::write(&csv_path, "a,b,c\n1,2.5,x\n3,4.5,y\n").unwrap();
        let _ = std::fs::remove_file(&gz_path);
        std::process::Command::new("gzip").arg("-k").arg(&csv_path).status().unwrap();

        let (tx, rx) = mpsc::channel();
        let _ = stream_save(gz_path.to_str().unwrap(), &out_path, false, &tx);
        let msgs: Vec<_> = rx.iter().collect();
        assert!(msgs.last().unwrap().contains("Done"));

        let df = ParquetReader::new(std::fs::File::open(&out_path).unwrap()).finish().unwrap();
        assert_eq!(df.height(), 2);

        let _ = std::fs::remove_file(csv_path);
        let _ = std::fs::remove_file(gz_path);
        let _ = std::fs::remove_file(out_path);
    }
}
