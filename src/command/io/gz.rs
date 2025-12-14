//! Gzipped CSV streaming helpers
use super::convert::{convert_epoch_cols, apply_schema, convert_types};
use super::csv::{detect_sep, parse_buf};
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::sync::Arc;

const MIN_ROWS: usize = 1_000;      // min rows before showing table
const CHUNK_ROWS: usize = 100_000;  // rows per background chunk
const FIRST_CHUNK_ROWS: usize = 100_000;

fn commify(n: usize) -> String {
    let s = n.to_string();
    let mut r = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { r.push(','); }
        r.push(c);
    }
    r.chars().rev().collect()
}

/// Streaming chunk: Some(df) = more data, None = EOF reached (file fully loaded)
pub type GzChunk = Option<DataFrame>;

/// Load gz file with streaming: returns initial chunk + background loader
pub fn load_streaming(path: &Path, mem_limit: u64) -> Result<(DataFrame, Option<Receiver<GzChunk>>)> {
    let mut child = Command::new("zcat")
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| anyhow!("Failed to spawn zcat: {}", e))?;

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("No stdout"))?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, stdout);

    // Read header
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let sep = detect_sep(&header);
    let header_bytes = header.as_bytes().to_vec();

    // Read first chunk (at least MIN_ROWS)
    let mut buf = header.clone().into_bytes();
    let mut lines = 0usize;
    let mut total_bytes = 0u64;
    while lines < MIN_ROWS.max(CHUNK_ROWS) {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(n) => {
                buf.extend_from_slice(line.as_bytes());
                total_bytes += n as u64;
                lines += 1;
            }
            Err(_) => break,
        }
    }

    if lines == 0 { return Err(anyhow!("Empty file")); }

    // Parse and capture raw schema BEFORE epoch conversion
    let raw_df = parse_buf(buf, sep, 500)?;
    let schema = Arc::clone(raw_df.schema()); // Clone Arc for thread
    let df = convert_epoch_cols(raw_df);

    // If EOF or already at mem limit, no background loading
    if lines < CHUNK_ROWS || total_bytes * 2 > mem_limit {
        let _ = child.wait();
        return Ok((df, None));
    }

    // Continue loading in background with same schema
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        stream_chunks(reader, header_bytes, sep, mem_limit, total_bytes, tx, child, schema.clone());
    });

    Ok((df, Some(rx)))
}

/// Background: stream remaining chunks until mem limit
fn stream_chunks(
    mut reader: BufReader<ChildStdout>,
    header: Vec<u8>,
    sep: u8,
    mem_limit: u64,
    mut total_bytes: u64,
    tx: Sender<GzChunk>,
    mut child: Child,
    schema: Arc<Schema>,
) {
    loop {
        let mut buf = header.clone();
        let mut lines = 0usize;
        while lines < CHUNK_ROWS {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(line.as_bytes());
                    total_bytes += n as u64;
                    lines += 1;
                }
                Err(_) => break,
            }
        }
        if lines == 0 {
            // EOF reached - send None to signal file fully loaded
            let _ = tx.send(None);
            break;
        }
        // Parse with fixed schema from first chunk, then convert epochs
        let df = CsvReadOptions::default()
            .with_has_header(true)
            .with_schema(Some(schema.clone()))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(std::io::Cursor::new(buf))
            .finish();
        if let Ok(df) = df {
            if tx.send(Some(convert_epoch_cols(df))).is_err() { break; }
        }
        // mem_limit hit - don't send None, just close channel (partial = true)
        if total_bytes * 2 > mem_limit { break; }
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// Stream gz file to parquet (runs in background)
pub fn stream_to_parquet(gz_path: &str, out_path: &Path, raw: bool) -> Receiver<String> {
    let gz = gz_path.to_string();
    let out = out_path.to_path_buf();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || { stream_worker(&gz, &out, raw, tx); });
    rx
}

fn stream_worker(gz_path: &str, out_path: &Path, raw: bool, tx: Sender<String>) {
    if let Err(e) = stream_impl(gz_path, out_path, raw, &tx) {
        let _ = tx.send(format!("Save error: {}", e));
    }
}

fn stream_impl(gz_path: &str, out_path: &Path, raw: bool, tx: &Sender<String>) -> Result<()> {
    let mut child = Command::new("zcat")
        .arg(gz_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| anyhow!("Failed to spawn zcat: {}", e))?;

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("No stdout"))?;
    let mut reader = BufReader::with_capacity(64 * 1024 * 1024, stdout);

    // Read header and detect separator
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let sep = detect_sep(&header);
    let col_count = header.split(sep as char).count();
    let header_bytes = header.as_bytes().to_vec();

    // Helper to read N lines
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

    // String schema for parsing
    let str_schema = Arc::new(Schema::from_iter((0..col_count).map(|i| {
        let name = header.split(sep as char).nth(i).unwrap_or("").trim();
        Field::new(name.into(), DataType::String)
    })));
    let parse = |buf: Vec<u8>| -> Result<DataFrame> {
        CsvReadOptions::default()
            .with_has_header(true)
            .with_schema(Some(str_schema.clone()))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(std::io::Cursor::new(buf))
            .finish()
            .map_err(|e| anyhow!("Failed to parse chunk: {}", e))
    };

    // Read first chunk to detect schema
    let (buf, lines) = read_chunk(FIRST_CHUNK_ROWS)?;
    if lines == 0 { return Err(anyhow!("Empty file")); }
    let first_df = parse(buf)?;
    let first_df = if raw { first_df } else { convert_types(first_df) };
    let schema = first_df.schema().to_owned();

    // Create batched writer for streaming
    let file = std::fs::File::create(out_path).map_err(|e| anyhow!("Create file: {}", e))?;
    let mut writer = ParquetWriter::new(file).batched(&schema)
        .map_err(|e| anyhow!("Create writer: {}", e))?;

    // Write first chunk
    let mut total_rows = first_df.height();
    let mut first_df = first_df;
    first_df.rechunk_mut();
    writer.write_batch(&first_df).map_err(|e| anyhow!("Write batch: {}", e))?;
    let _ = tx.send(format!("Written {} rows", commify(total_rows)));

    // Stream remaining chunks directly to parquet
    loop {
        let (buf, lines) = read_chunk(CHUNK_ROWS)?;
        if lines == 0 { break; }

        let df = parse(buf)?;
        let mut df = if raw { df } else {
            let (d, err) = apply_schema(df, &schema);
            if let Some(e) = err { let _ = tx.send(format!("Warning: {}", e)); }
            d
        };
        df.rechunk_mut();
        writer.write_batch(&df).map_err(|e| anyhow!("Write batch: {}", e))?;
        total_rows += df.height();
        let _ = tx.send(format!("Written {} rows", commify(total_rows)));
    }

    // Finalize parquet file
    writer.finish().map_err(|e| anyhow!("Finish: {}", e))?;
    let _ = child.wait();
    let _ = tx.send(format!("Done: {} rows", commify(total_rows)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_gz_to_single_parquet() {
        let tmp = std::env::temp_dir();
        let csv_path = tmp.join("test_single.csv");
        let gz_path = tmp.join("test_single.csv.gz");
        let out_path = tmp.join("test_single.parquet");
        let chunk_path = tmp.join("test_single_001.parquet");  // should NOT exist

        let csv = "National_BBO_Ind,price,volume\nO,10.5,100\nE,11,200\nA,12.5,300\n";
        let mut f = std::fs::File::create(&csv_path).unwrap();
        f.write_all(csv.as_bytes()).unwrap();

        let _ = std::fs::remove_file(&gz_path);
        let _ = std::fs::remove_file(&out_path);
        let _ = std::fs::remove_file(&chunk_path);
        let status = std::process::Command::new("gzip").arg("-k").arg(&csv_path).status().unwrap();
        assert!(status.success());

        let (tx, rx) = mpsc::channel();
        stream_worker(gz_path.to_str().unwrap(), &out_path, false, tx);

        let msgs: Vec<_> = rx.iter().collect();
        assert!(!msgs.is_empty());
        assert!(msgs.last().unwrap().contains("Done"), "Last msg: {:?}", msgs.last());

        // Single file should exist, chunk file should NOT
        assert!(out_path.exists(), "Single parquet file should exist");
        assert!(!chunk_path.exists(), "Chunk file should NOT exist");

        let df = ParquetReader::new(std::fs::File::open(&out_path).unwrap()).finish().unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.column("National_BBO_Ind").unwrap().dtype(), &DataType::String);
        assert_eq!(df.column("price").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("volume").unwrap().dtype(), &DataType::Int64);

        let _ = std::fs::remove_file(csv_path);
        let _ = std::fs::remove_file(gz_path);
        let _ = std::fs::remove_file(out_path);
    }
}
