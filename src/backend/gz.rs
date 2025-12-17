//! Gz backend - streaming gzipped CSV with memory limits.
//! Refuses expensive operations on partial (memory-limited) data.
use super::{Backend, LoadResult, sql, commify};
use crate::command::io::convert::{convert_epoch_cols, apply_schema, convert_types};
use crate::state::ViewState;
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

/// Error message for partial load operations
const PARTIAL_ERR: &str = "File not fully loaded (memory limit)";

impl Gz<'_> {
    /// Check that file is fully loaded, error if partial
    fn require_complete(&self) -> Result<()> {
        if self.partial { Err(anyhow!(PARTIAL_ERR)) } else { Ok(()) }
    }
}

/// Gz backend impl - uses SQL via lf() but blocks expensive ops when partial
impl Backend for Gz<'_> {
    /// LazyFrame from in-memory DataFrame
    fn lf(&self, _: &str) -> Result<LazyFrame> { Ok(self.df.clone().lazy()) }

    /// Sort and take top N - allowed on partial data
    fn sort_head(&self, _: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        let ord = if desc { "DESC" } else { "ASC" };
        sql(self.lf("")?, &format!("SELECT * FROM df ORDER BY \"{}\" {} LIMIT {}", col, ord, limit))
    }

    /// Distinct - blocked if partial
    fn distinct(&self, p: &str, col: &str) -> Result<Vec<String>> {
        self.require_complete()?;
        let df = sql(self.lf(p)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }
    /// Frequency count - blocked if partial
    fn freq(&self, p: &str, col: &str) -> Result<DataFrame> { self.require_complete()?; self.freq_where(p, col, "TRUE") }
    /// Filter - blocked if partial
    fn filter(&self, p: &str, w: &str, limit: usize) -> Result<DataFrame> { self.require_complete()?; self.fetch_where(p, w, 0, limit) }
    /// Fetch where - blocked if partial
    fn fetch_where(&self, _: &str, w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        self.require_complete()?;
        sql(self.lf("")?, &format!("SELECT * FROM df WHERE {} LIMIT {} OFFSET {}", w, limit, offset))
    }
    /// Count where - blocked if partial
    fn count_where(&self, _: &str, w: &str) -> Result<usize> {
        self.require_complete()?;
        let r = sql(self.lf("")?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }
    /// Freq where - blocked if partial
    fn freq_where(&self, _: &str, col: &str, w: &str) -> Result<DataFrame> {
        self.require_complete()?;
        sql(self.lf("")?, &format!("SELECT \"{}\", COUNT(*) as Cnt FROM df WHERE {} GROUP BY \"{}\" ORDER BY Cnt DESC", col, w, col))
    }
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

/// Load gz file into ViewState with streaming background loader
pub fn load(path: &str, id: usize) -> Result<LoadResult> {
    let p = Path::new(path);
    if !p.exists() { return Err(anyhow!("File not found: {}", path)); }
    // Calculate memory limit from config
    let mem_pct: u64 = crate::theme::load_config_value("gz_mem_pct").and_then(|s| s.parse().ok()).unwrap_or(10);
    let mem_limit = crate::plugin::system::mem_total() * mem_pct / 100;
    let (df, bg_rx) = load_streaming(p, mem_limit)?;
    let df = convert_epoch_cols(df);
    if df.height() == 0 { return Err(anyhow!("File is empty")); }
    let partial = bg_rx.is_some();
    Ok(LoadResult {
        view: ViewState::new_gz(id, path.into(), df, Some(path.into()), path.into(), partial),
        bg_loader: bg_rx,
    })
}

/// Background thread: read chunks until EOF or memory limit
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

/// Stream gz file to parquet (background)
pub fn stream_to_parquet(gz_path: &str, out_path: &Path, raw: bool) -> Receiver<String> {
    let (gz, out) = (gz_path.to_string(), out_path.to_path_buf());
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || { if let Err(e) = stream_save(&gz, &out, raw, &tx) { let _ = tx.send(format!("Error: {}", e)); } });
    rx
}

/// Stream gz→parquet conversion with progress updates via channel
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
    let _ = tx.send(format!("Written {} rows", commify(&total_rows.to_string())));

    loop {
        let (buf, lines) = read_chunk(CHUNK_ROWS)?;
        if lines == 0 { break; }
        let df = parse(buf)?;
        let mut df = if raw { df } else { let (d, e) = apply_schema(df, &schema); if let Some(e) = e { let _ = tx.send(format!("Warning: {}", e)); } d };
        df.rechunk_mut();
        writer.write_batch(&df)?;
        total_rows += df.height();
        let _ = tx.send(format!("Written {} rows", commify(&total_rows.to_string())));
    }

    writer.finish()?;
    let _ = child.wait();
    let _ = tx.send(format!("Done: {} rows", commify(&total_rows.to_string())));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gz_to_parquet() {
        use std::time::Duration;
        let tmp = std::env::temp_dir();
        let csv_path = tmp.join("test_gz.csv");
        let gz_path = tmp.join("test_gz.csv.gz");
        let out_path = tmp.join("test_gz.parquet");

        // Cleanup before test
        let _ = std::fs::remove_file(&csv_path);
        let _ = std::fs::remove_file(&gz_path);
        let _ = std::fs::remove_file(&out_path);

        std::fs::write(&csv_path, "a,b,c\n1,2.5,x\n3,4.5,y\n").unwrap();
        std::process::Command::new("gzip").arg("-k").arg(&csv_path).status().unwrap();

        let (tx, rx) = mpsc::channel();
        let _ = stream_save(gz_path.to_str().unwrap(), &out_path, false, &tx);
        // Collect with timeout to avoid hang
        let mut msgs = vec![];
        while let Ok(msg) = rx.recv_timeout(Duration::from_secs(5)) { msgs.push(msg); }
        assert!(msgs.last().map(|s| s.contains("Done")).unwrap_or(false));

        let df = ParquetReader::new(std::fs::File::open(&out_path).unwrap()).finish().unwrap();
        assert_eq!(df.height(), 2);

        let _ = std::fs::remove_file(csv_path);
        let _ = std::fs::remove_file(gz_path);
        let _ = std::fs::remove_file(out_path);
    }
}
