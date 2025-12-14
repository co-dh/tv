//! Gzipped CSV streaming helpers
use super::convert::{apply_schema, convert_epoch_cols, convert_types};
use super::csv::{detect_sep, parse_buf};
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::sync::Arc;

const MAX_PREVIEW_ROWS: usize = 100_000;
const BG_CHUNK_ROWS: usize = 100_000;
const CHUNK_ROWS: usize = 1_000_000;
const FIRST_CHUNK_ROWS: usize = 100_000;

/// Load gz file with streaming: returns preview + background loader channel
pub fn load_streaming(path: &Path, mem_limit: u64) -> Result<(DataFrame, Option<Receiver<DataFrame>>)> {
    let mut child = Command::new("zcat")
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| anyhow!("Failed to spawn zcat: {}", e))?;

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("No stdout"))?;
    let mut reader = BufReader::new(stdout);

    // Read header to detect delimiter
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let sep = detect_sep(&header);
    let header_bytes = header.as_bytes().to_vec();

    // Read preview rows
    let mut buf = header.into_bytes();
    for _ in 0..MAX_PREVIEW_ROWS {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            let _ = child.wait();
            return Ok((parse_buf(buf, sep, 500)?, None));
        }
        buf.extend_from_slice(line.as_bytes());
    }

    let df = parse_buf(buf, sep, 500)?;

    // Spawn background loader
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        bg_stream_csv(reader, header_bytes, sep, mem_limit, tx, child);
    });

    Ok((df, Some(rx)))
}

/// Background thread: stream CSV chunks until memory limit
fn bg_stream_csv(
    mut reader: BufReader<ChildStdout>,
    header: Vec<u8>,
    sep: u8,
    mem_limit: u64,
    tx: Sender<DataFrame>,
    mut child: Child,
) {
    let mut total_bytes = 0u64;
    loop {
        let mut chunk_buf = header.clone();
        let mut lines = 0usize;
        while lines < BG_CHUNK_ROWS {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(n) => {
                    chunk_buf.extend_from_slice(line.as_bytes());
                    total_bytes += n as u64;
                    lines += 1;
                }
                Err(_) => break,
            }
        }
        if lines == 0 { break; }
        if let Ok(df) = parse_buf(chunk_buf, sep, 100) {
            let df = convert_epoch_cols(df);
            if tx.send(df).is_err() { break; }
        }
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

    // Write chunks immediately (true streaming)
    let stem = out_path.file_stem().and_then(|s| s.to_str()).unwrap_or("out");
    let parent = out_path.parent().unwrap_or(Path::new("."));
    let mut chunk_num = 1usize;
    let mut total_rows = 0usize;

    // Write first chunk
    let chunk_path = parent.join(format!("{}_{:03}.parquet", stem, chunk_num));
    write_chunk(&first_df, &chunk_path)?;
    total_rows += first_df.height();
    chunk_num += 1;
    let _ = tx.send(format!("Written {} rows ({} chunks)", total_rows, chunk_num - 1));

    // Stream remaining chunks
    loop {
        let (buf, lines) = read_chunk(CHUNK_ROWS)?;
        if lines == 0 { break; }

        let df = parse(buf)?;
        let df = if raw { df } else {
            let (df, err) = apply_schema(df, &schema);
            if let Some(e) = err { let _ = tx.send(format!("Warning: {}", e)); }
            df
        };

        let chunk_path = parent.join(format!("{}_{:03}.parquet", stem, chunk_num));
        write_chunk(&df, &chunk_path)?;
        total_rows += df.height();
        chunk_num += 1;
        let _ = tx.send(format!("Written {} rows ({} chunks)", total_rows, chunk_num - 1));
    }

    let _ = child.wait();
    let _ = tx.send(format!("Done: {} rows in {} chunks", total_rows, chunk_num - 1));
    Ok(())
}

fn write_chunk(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df)
        .map_err(|e| anyhow!("Write error: {}", e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_stream_gz_to_parquet() {
        let tmp = std::env::temp_dir();
        let csv_path = tmp.join("test_bbo.csv");
        let gz_path = tmp.join("test_bbo.csv.gz");
        let out_path = tmp.join("test_bbo.parquet");
        let chunk_path = tmp.join("test_bbo_001.parquet");

        let csv = "National_BBO_Ind,price,volume\nO,10.5,100\nE,11,200\nA,12.5,300\n";
        let mut f = std::fs::File::create(&csv_path).unwrap();
        f.write_all(csv.as_bytes()).unwrap();

        let _ = std::fs::remove_file(&gz_path);
        let _ = std::fs::remove_file(&chunk_path);
        let status = std::process::Command::new("gzip").arg("-k").arg(&csv_path).status().unwrap();
        assert!(status.success());

        let (tx, rx) = mpsc::channel();
        stream_worker(gz_path.to_str().unwrap(), &out_path, false, tx);

        let msgs: Vec<_> = rx.iter().collect();
        assert!(!msgs.is_empty());
        assert!(msgs.last().unwrap().contains("Done"));
        assert!(chunk_path.exists());

        let df = ParquetReader::new(std::fs::File::open(&chunk_path).unwrap()).finish().unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.column("National_BBO_Ind").unwrap().dtype(), &DataType::String);
        assert_eq!(df.column("price").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("volume").unwrap().dtype(), &DataType::Int64);

        let _ = std::fs::remove_file(csv_path);
        let _ = std::fs::remove_file(gz_path);
        let _ = std::fs::remove_file(chunk_path);
    }
}
